use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use crate::tracking::backoff::{new_backoff, next_delay, reset_backoff, BackoffState};
use crate::tracking::codec::{decode_server_msg, encode_client_msg, stamp_lat_lng, stamp_lat_lngs};
use crate::tracking::errors::{
    error_from_wire, is_auth_error, is_fatal_resume_error, new_error, Error,
};
use crate::tracking::queue::OfflineQueue;
use crate::tracking::rate::{can_accept_publish, next_publish_allowed_at};
use crate::tracking::url::build_ws_url;
use crate::tracking::v2::tracking_client::TrackingClient;
use crate::tracking::v2::{
    client_msg, server_msg, ClientMsg, Command, CommandAck, CommandAckStatus, Event, LatLng,
    LocationAdd, LocationBatch, Relocate, Resume, ServerMsg, Subscribe, TrackStart, TrackStop,
};

/// WebSocket subprotocol.
pub const SUBPROTOCOL: &str = "tracking.v2.proto";
/// Hard cap for Publish calls (points per second).
pub const MAX_PUBLISH_HZ: u32 = 50;
/// Minimum gap between accepted points.
pub const MIN_PUBLISH_INTERVAL: Duration = Duration::from_millis(1000 / MAX_PUBLISH_HZ as u64);
/// Max opaque event payload.
pub const MAX_EVENT_BYTES: usize = 4 * 1024;
/// Max opaque event rate.
pub const MAX_EVENT_HZ: u32 = 1;
/// Minimum gap between events.
pub const MIN_EVENT_INTERVAL: Duration = Duration::from_secs(1);

/// Edge protocol selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    /// Binary protobuf on `/v2/tracking/ws` (default).
    #[default]
    Ws,
    /// gRPC bidi session for mesh/agents.
    Grpc,
}

/// Connection state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Dialing / waiting for Hello.
    Connecting,
    /// Session open.
    Open,
    /// Reconnecting after drop.
    Reconnecting,
    /// Closed.
    Closed,
}

impl ConnectionState {
    /// Wire/string form matching JS/Go.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Open => "open",
            Self::Reconnecting => "reconnecting",
            Self::Closed => "closed",
        }
    }
}

/// Publisher device credentials.
#[derive(Debug, Clone)]
pub struct DeviceAuth {
    /// Device UID / client id.
    pub client_id: String,
    /// Device secret.
    pub client_secret: String,
}

/// Listener JWT credentials.
#[derive(Debug, Clone)]
pub struct ListenerAuth {
    /// Access token.
    pub access_token: String,
}

/// Fresh credentials after AUTH / UNAUTHORIZED.
pub type RefreshAuthFn = Arc<
    dyn Fn() -> Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<(Option<DeviceAuth>, Option<ListenerAuth>), String>,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

/// Opens a session against a Pickpoint tracking endpoint.
#[derive(Clone)]
pub struct Config {
    /// WS: `ws://host:3100`, `wss://…`, or `host:3100`. gRPC: `host:port`.
    pub endpoint: String,
    /// Transport (default WS).
    pub transport: Transport,
    /// Device publisher auth.
    pub device: Option<DeviceAuth>,
    /// Listener auth.
    pub listener: Option<ListenerAuth>,
    /// WS path (default `/v2/tracking/ws`).
    pub ws_path: String,
    /// Disable auto-reconnect (WS only).
    pub disable_reconnect: bool,
    /// Reconnect min delay.
    pub reconnect_min_delay: Duration,
    /// Reconnect max delay.
    pub reconnect_max_delay: Duration,
    /// Max reconnect attempts (`0` = unlimited).
    pub reconnect_max_attempts: u32,
    /// Refresh credentials on AUTH.
    pub refresh_auth: Option<RefreshAuthFn>,
    /// Offline queue size (default 10_000).
    pub max_queue_size: usize,
    /// Hello timeout (default 10s).
    pub hello_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            transport: Transport::Ws,
            device: None,
            listener: None,
            ws_path: String::new(),
            disable_reconnect: false,
            reconnect_min_delay: Duration::ZERO,
            reconnect_max_delay: Duration::ZERO,
            reconnect_max_attempts: 0,
            refresh_auth: None,
            max_queue_size: 10_000,
            hello_timeout: Duration::from_secs(10),
        }
    }
}

enum Outbound {
    Msg(ClientMsg),
    Close,
}

struct PendingStart {
    tx: oneshot::Sender<Result<String, Error>>,
}

struct PendingStop {
    tx: oneshot::Sender<Result<(), Error>>,
}

struct PendingResume {
    tx: oneshot::Sender<Result<u64, Error>>,
}

struct Inner {
    cfg: Config,
    state: ConnectionState,
    track_uid: String,
    client_seq: u64,
    last_acked_seq: u64,
    queue: OfflineQueue,
    backoff: BackoffState,
    next_publish_at: Instant,
    next_event_at: Instant,
    subscriptions: HashSet<String>,
    intentional: bool,
    dial_gen: u64,
    out_tx: Option<mpsc::UnboundedSender<Outbound>>,
    start_wait: Option<PendingStart>,
    stop_wait: Option<PendingStop>,
    resume_wait: Option<PendingResume>,
}

/// Tracking session (device or listener).
#[derive(Clone)]
pub struct Client {
    inner: Arc<Mutex<Inner>>,
    recv_tx: mpsc::Sender<ServerMsg>,
    recv_rx: Arc<Mutex<mpsc::Receiver<ServerMsg>>>,
    cmd_tx: mpsc::Sender<Command>,
    cmd_rx: Arc<Mutex<mpsc::Receiver<Command>>>,
}

/// Connect opens a tracking session (WS binary protobuf by default).
pub async fn connect(cfg: Config) -> Result<Client, Error> {
    if cfg.endpoint.is_empty() {
        return Err(new_error(
            crate::tracking::v2::ErrorCode::Invalid,
            "Endpoint is required",
        ));
    }
    if cfg.device.is_none() && cfg.listener.is_none() {
        return Err(new_error(
            crate::tracking::v2::ErrorCode::Invalid,
            "Device or Listener auth is required",
        ));
    }
    let mut cfg = cfg;
    if cfg.hello_timeout.is_zero() {
        cfg.hello_timeout = Duration::from_secs(10);
    }

    let (recv_tx, recv_rx) = mpsc::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);

    let client = Client {
        inner: Arc::new(Mutex::new(Inner {
            backoff: new_backoff(
                cfg.reconnect_min_delay,
                cfg.reconnect_max_delay,
                cfg.reconnect_max_attempts,
            ),
            queue: OfflineQueue::new(cfg.max_queue_size),
            cfg,
            state: ConnectionState::Connecting,
            track_uid: String::new(),
            client_seq: 0,
            last_acked_seq: 0,
            next_publish_at: Instant::now(),
            next_event_at: Instant::now(),
            subscriptions: HashSet::new(),
            intentional: false,
            dial_gen: 0,
            out_tx: None,
            start_wait: None,
            stop_wait: None,
            resume_wait: None,
        })),
        recv_tx,
        recv_rx: Arc::new(Mutex::new(recv_rx)),
        cmd_tx,
        cmd_rx: Arc::new(Mutex::new(cmd_rx)),
    };

    let transport = {
        let g = client.inner.lock().await;
        g.cfg.transport
    };
    match transport {
        Transport::Grpc => {
            client.connect_grpc().await?;
            let mut g = client.inner.lock().await;
            g.state = ConnectionState::Open;
        }
        Transport::Ws => {
            client.dial(false).await?;
        }
    }
    Ok(client)
}

impl Client {
    /// Connection state.
    pub async fn state(&self) -> ConnectionState {
        self.inner.lock().await.state
    }

    /// Active track UID, if any.
    pub async fn track_uid(&self) -> String {
        self.inner.lock().await.track_uid.clone()
    }

    /// Last assigned publish sequence.
    pub async fn client_seq(&self) -> u64 {
        self.inner.lock().await.client_seq
    }

    /// Highest server-acked client sequence.
    pub async fn last_acked_seq(&self) -> u64 {
        self.inner.lock().await.last_acked_seq
    }

    async fn dial(&self, send_resume: bool) -> Result<(), Error> {
        self.dial_boxed(send_resume).await
    }

    fn dial_boxed(
        &self,
        send_resume: bool,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'static>> {
        let this = self.clone();
        Box::pin(async move { this.dial_inner(send_resume).await })
    }

    async fn dial_inner(&self, send_resume: bool) -> Result<(), Error> {
        let (cfg, gen) = {
            let mut g = self.inner.lock().await;
            g.dial_gen += 1;
            if matches!(
                g.state,
                ConnectionState::Open | ConnectionState::Reconnecting
            ) {
                g.state = ConnectionState::Reconnecting;
            } else {
                g.state = ConnectionState::Connecting;
            }
            (g.cfg.clone(), g.dial_gen)
        };

        let url = build_ws_url(&cfg)
            .map_err(|e| new_error(crate::tracking::v2::ErrorCode::Invalid, e))?;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = url.as_str().into_client_request().map_err(|e| {
            new_error(
                crate::tracking::v2::ErrorCode::Invalid,
                format!("ws request: {e}"),
            )
        })?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            http::HeaderValue::from_static(SUBPROTOCOL),
        );

        let (ws, _) = connect_async(request).await.map_err(|e| {
            new_error(
                crate::tracking::v2::ErrorCode::TryAgain,
                format!("ws dial: {e}"),
            )
        })?;

        let (mut write, mut read) = ws.split();

        // Hello
        let hello_timeout = cfg.hello_timeout;
        let first = tokio::time::timeout(hello_timeout, read.next())
            .await
            .map_err(|_| new_error(crate::tracking::v2::ErrorCode::TryAgain, "hello timeout"))?
            .ok_or_else(|| {
                new_error(
                    crate::tracking::v2::ErrorCode::TryAgain,
                    "connection closed before hello",
                )
            })?
            .map_err(|e| {
                new_error(
                    crate::tracking::v2::ErrorCode::TryAgain,
                    format!("ws read: {e}"),
                )
            })?;

        let data = match first {
            Message::Binary(b) => b,
            other => {
                return Err(new_error(
                    crate::tracking::v2::ErrorCode::Invalid,
                    format!("expected binary hello, got {other:?}"),
                ));
            }
        };
        let msg = decode_server_msg(&data).map_err(|e| {
            new_error(
                crate::tracking::v2::ErrorCode::Invalid,
                format!("decode hello: {e}"),
            )
        })?;

        match msg.body {
            Some(server_msg::Body::Hello(_)) => {}
            Some(server_msg::Body::Relocate(rel)) => {
                return self.handle_relocate(rel, send_resume).await;
            }
            Some(server_msg::Body::Error(err)) => {
                return Err(error_from_wire(Some(&err)));
            }
            _ => {
                return Err(new_error(
                    crate::tracking::v2::ErrorCode::Invalid,
                    "expected hello",
                ));
            }
        }

        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        {
            let mut g = self.inner.lock().await;
            if gen != g.dial_gen || g.intentional {
                return Err(new_error(
                    crate::tracking::v2::ErrorCode::Invalid,
                    "dial superseded",
                ));
            }
            g.out_tx = Some(out_tx);
            g.state = ConnectionState::Open;
            reset_backoff(&mut g.backoff);
        }

        let this = self.clone();
        tokio::spawn(async move {
            while let Some(out) = out_rx.recv().await {
                match out {
                    Outbound::Msg(msg) => {
                        let Ok(buf) = encode_client_msg(&msg) else {
                            continue;
                        };
                        if write.send(Message::Binary(buf.into())).await.is_err() {
                            break;
                        }
                    }
                    Outbound::Close => {
                        let _ = write.close().await;
                        break;
                    }
                }
            }
        });

        let this_read = self.clone();
        tokio::spawn(async move {
            while let Some(frame) = read.next().await {
                match frame {
                    Ok(Message::Binary(b)) => {
                        if let Ok(msg) = decode_server_msg(&b) {
                            this_read.dispatch(msg).await;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
            this_read.on_socket_closed(gen);
        });

        if send_resume {
            self.send_resume_and_wait().await?;
        }
        self.resubscribe().await;
        let _ = this;
        Ok(())
    }

    async fn handle_relocate(&self, rel: Relocate, mut send_resume: bool) -> Result<(), Error> {
        if !rel.endpoint.is_empty() {
            let mut g = self.inner.lock().await;
            g.cfg.endpoint = rel.endpoint;
        }
        if rel.retry_after_ms > 0 {
            tokio::time::sleep(Duration::from_millis(rel.retry_after_ms as u64)).await;
        }
        let (send_resume, intentional) = {
            let g = self.inner.lock().await;
            if !g.track_uid.is_empty() {
                send_resume = true;
            }
            (send_resume, g.intentional)
        };
        if intentional {
            return Err(new_error(crate::tracking::v2::ErrorCode::Invalid, "closed"));
        }
        self.dial_boxed(send_resume).await
    }

    async fn connect_grpc(&self) -> Result<(), Error> {
        let cfg = self.inner.lock().await.cfg.clone();
        let channel = Channel::from_shared(format!("http://{}", cfg.endpoint))
            .map_err(|e| {
                new_error(
                    crate::tracking::v2::ErrorCode::Invalid,
                    format!("grpc endpoint: {e}"),
                )
            })?
            .connect()
            .await
            .map_err(|e| {
                new_error(
                    crate::tracking::v2::ErrorCode::TryAgain,
                    format!("grpc dial: {e}"),
                )
            })?;

        let mut client = TrackingClient::new(channel);
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Outbound>();
        let outbound = tokio_stream::wrappers::UnboundedReceiverStream::new(out_rx).filter_map(
            |o| async move {
                match o {
                    Outbound::Msg(m) => Some(m),
                    Outbound::Close => None,
                }
            },
        );

        let mut request = Request::new(outbound);
        let md = request.metadata_mut();
        if let Some(d) = &cfg.device {
            md.insert(
                "x-client-id",
                MetadataValue::try_from(d.client_id.as_str()).map_err(|_| {
                    new_error(crate::tracking::v2::ErrorCode::Invalid, "bad client-id")
                })?,
            );
            md.insert(
                "x-client-secret",
                MetadataValue::try_from(d.client_secret.as_str()).map_err(|_| {
                    new_error(crate::tracking::v2::ErrorCode::Invalid, "bad client-secret")
                })?,
            );
        } else if let Some(l) = &cfg.listener {
            let v = format!("Bearer {}", l.access_token);
            md.insert(
                "authorization",
                MetadataValue::try_from(v.as_str()).map_err(|_| {
                    new_error(crate::tracking::v2::ErrorCode::Invalid, "bad access-token")
                })?,
            );
        }

        let mut stream = client
            .session(request)
            .await
            .map_err(|e| {
                new_error(
                    crate::tracking::v2::ErrorCode::TryAgain,
                    format!("grpc session: {e}"),
                )
            })?
            .into_inner();

        {
            let mut g = self.inner.lock().await;
            g.out_tx = Some(out_tx);
        }

        let this = self.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = stream.message().await {
                this.dispatch(msg).await;
            }
        });
        Ok(())
    }

    async fn dispatch(&self, msg: ServerMsg) {
        match msg.body.clone() {
            Some(server_msg::Body::Relocate(rel)) => {
                let this = self.clone();
                tokio::spawn(async move {
                    let _ = this.handle_relocate(rel, true).await;
                });
                return;
            }
            Some(server_msg::Body::ResumeOk(ok)) => {
                let wait = {
                    let mut g = self.inner.lock().await;
                    if !ok.track_uid.is_empty() {
                        g.track_uid = ok.track_uid.clone();
                    }
                    g.last_acked_seq = ok.last_acked_seq;
                    if g.client_seq < g.last_acked_seq {
                        g.client_seq = g.last_acked_seq;
                    }
                    let ack = g.last_acked_seq;
                    g.queue.ack_through(ack);
                    g.resume_wait.take()
                };
                self.flush_queue().await;
                if let Some(w) = wait {
                    let _ = w.tx.send(Ok(ok.last_acked_seq));
                }
            }
            Some(server_msg::Body::TrackStarted(ts)) => {
                let wait = {
                    let mut g = self.inner.lock().await;
                    g.track_uid = ts.track_uid.clone();
                    g.client_seq = 0;
                    g.last_acked_seq = 0;
                    g.queue.clear();
                    g.start_wait.take()
                };
                if let Some(w) = wait {
                    let _ = w.tx.send(Ok(ts.track_uid));
                }
            }
            Some(server_msg::Body::TrackStopped(ts)) => {
                let wait = {
                    let mut g = self.inner.lock().await;
                    if g.track_uid == ts.track_uid {
                        g.track_uid.clear();
                        g.queue.clear();
                    }
                    g.stop_wait.take()
                };
                if let Some(w) = wait {
                    let _ = w.tx.send(Ok(()));
                }
            }
            Some(server_msg::Body::LocationAdded(loc)) => {
                let mut g = self.inner.lock().await;
                if loc.client_seq > g.last_acked_seq {
                    g.last_acked_seq = loc.client_seq;
                }
                g.queue.ack_through(loc.client_seq);
            }
            Some(server_msg::Body::Command(cmd)) => {
                let _ = self.cmd_tx.try_send(cmd);
                return;
            }
            Some(server_msg::Body::Error(err)) => {
                let e = error_from_wire(Some(&err));
                {
                    let mut g = self.inner.lock().await;
                    if let Some(w) = g.resume_wait.take() {
                        if is_fatal_resume_error(e.code) {
                            g.track_uid.clear();
                            g.queue.clear();
                        }
                        let _ = w.tx.send(Err(e.clone()));
                    }
                    if let Some(w) = g.start_wait.take() {
                        let _ = w.tx.send(Err(e.clone()));
                    }
                    if let Some(w) = g.stop_wait.take() {
                        let _ = w.tx.send(Err(e.clone()));
                    }
                }
                if is_auth_error(e.code) {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_auth_error().await;
                    });
                }
            }
            _ => {}
        }
        let _ = self.recv_tx.try_send(msg);
    }

    async fn handle_auth_error(&self) {
        let refresh = self.inner.lock().await.cfg.refresh_auth.clone();
        let Some(refresh) = refresh else {
            let mut g = self.inner.lock().await;
            g.intentional = true;
            g.state = ConnectionState::Closed;
            if let Some(tx) = g.out_tx.take() {
                let _ = tx.send(Outbound::Close);
            }
            return;
        };
        let result = refresh().await;
        match result {
            Ok((device, listener)) => {
                let send_resume = {
                    let mut g = self.inner.lock().await;
                    if let Some(d) = device {
                        g.cfg.device = Some(d);
                        g.cfg.listener = None;
                    }
                    if let Some(l) = listener {
                        g.cfg.listener = Some(l);
                        g.cfg.device = None;
                    }
                    g.dial_gen += 1;
                    if let Some(tx) = g.out_tx.take() {
                        let _ = tx.send(Outbound::Close);
                    }
                    let send_resume = !g.track_uid.is_empty();
                    let intentional = g.intentional;
                    (send_resume, intentional)
                };
                if send_resume.1 {
                    return;
                }
                let _ = self.dial_boxed(send_resume.0).await;
            }
            Err(_) => {
                let mut g = self.inner.lock().await;
                g.intentional = true;
                g.state = ConnectionState::Closed;
            }
        }
    }

    fn on_socket_closed(&self, gen: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            let send_resume;
            let delay;
            {
                let mut g = this.inner.lock().await;
                if gen != g.dial_gen {
                    return;
                }
                g.out_tx = None;
                if g.intentional {
                    g.state = ConnectionState::Closed;
                    return;
                }
                if g.cfg.disable_reconnect || g.cfg.transport == Transport::Grpc {
                    g.state = ConnectionState::Closed;
                    this.reject_pending_locked(
                        &mut g,
                        new_error(
                            crate::tracking::v2::ErrorCode::TryAgain,
                            "connection closed",
                        ),
                    );
                    return;
                }
                g.state = ConnectionState::Reconnecting;
                send_resume = !g.track_uid.is_empty();
                let r: f64 = rand::thread_rng().gen();
                match next_delay(&mut g.backoff, r) {
                    Some(d) => delay = d,
                    None => {
                        g.state = ConnectionState::Closed;
                        this.reject_pending_locked(
                            &mut g,
                            new_error(
                                crate::tracking::v2::ErrorCode::TryAgain,
                                "reconnect attempts exhausted",
                            ),
                        );
                        return;
                    }
                }
            }
            tokio::time::sleep(delay).await;
            if this.inner.lock().await.intentional {
                return;
            }
            if this.dial_boxed(send_resume).await.is_err() {
                let gen = this.inner.lock().await.dial_gen;
                this.on_socket_closed(gen);
            }
        });
    }

    fn reject_pending_locked(&self, g: &mut Inner, err: Error) {
        if let Some(w) = g.start_wait.take() {
            let _ = w.tx.send(Err(err.clone()));
        }
        if let Some(w) = g.stop_wait.take() {
            let _ = w.tx.send(Err(err.clone()));
        }
        if let Some(w) = g.resume_wait.take() {
            let _ = w.tx.send(Err(err));
        }
    }

    async fn resubscribe(&self) {
        let subs: Vec<String> = {
            let g = self.inner.lock().await;
            g.subscriptions.iter().cloned().collect()
        };
        for d in subs {
            let _ = self
                .send(ClientMsg {
                    body: Some(client_msg::Body::Subscribe(Subscribe {
                        device_uid: d,
                        include_events: None,
                        min_location_interval_ms: 0,
                    })),
                })
                .await;
        }
    }

    async fn send_resume_and_wait(&self) -> Result<(), Error> {
        let (uid, seq, rx) = {
            let mut g = self.inner.lock().await;
            if g.track_uid.is_empty() {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            g.resume_wait = Some(PendingResume { tx });
            (g.track_uid.clone(), g.client_seq, rx)
        };
        self.send(ClientMsg {
            body: Some(client_msg::Body::Resume(Resume {
                track_uid: uid,
                last_client_seq: seq,
            })),
        })
        .await?;
        match rx.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(new_error(
                crate::tracking::v2::ErrorCode::TryAgain,
                "resume cancelled",
            )),
        }
    }

    async fn flush_queue(&self) {
        let (uid, pending, open) = {
            let g = self.inner.lock().await;
            (
                g.track_uid.clone(),
                g.queue.peek_all(),
                g.state == ConnectionState::Open && g.out_tx.is_some(),
            )
        };
        if uid.is_empty() || !open || pending.is_empty() {
            return;
        }
        let last = pending.last().map(|p| p.seq).unwrap_or(0);
        let points: Vec<LatLng> = pending.into_iter().map(|p| p.point).collect();
        let _ = self
            .send(ClientMsg {
                body: Some(client_msg::Body::LocationBatch(LocationBatch {
                    track_uid: uid,
                    client_seq: last,
                    points: stamp_lat_lngs(points),
                })),
            })
            .await;
    }

    /// Next server message (LocationAdded, Subscribed, …). Commands go to [`Self::recv_command`].
    pub async fn recv(&self) -> Result<ServerMsg, Error> {
        let mut rx = self.recv_rx.lock().await;
        rx.recv().await.ok_or_else(|| {
            new_error(
                crate::tracking::v2::ErrorCode::Invalid,
                "receive channel closed",
            )
        })
    }

    /// Next server→device Command inject.
    pub async fn recv_command(&self) -> Option<Command> {
        let mut rx = self.cmd_rx.lock().await;
        rx.recv().await
    }

    /// Acknowledge a received Command.
    pub async fn ack_command(
        &self,
        command_id: impl Into<String>,
        status: CommandAckStatus,
        message: Option<String>,
    ) -> Result<(), Error> {
        self.send(ClientMsg {
            body: Some(client_msg::Body::CommandAck(CommandAck {
                command_id: command_id.into(),
                status: status as i32,
                message,
            })),
        })
        .await
    }

    /// Write a client message on the session.
    pub async fn send(&self, msg: ClientMsg) -> Result<(), Error> {
        let g = self.inner.lock().await;
        if g.state == ConnectionState::Closed && g.intentional {
            return Err(new_error(crate::tracking::v2::ErrorCode::Invalid, "closed"));
        }
        let Some(tx) = &g.out_tx else {
            return Err(new_error(
                crate::tracking::v2::ErrorCode::TryAgain,
                "socket not open",
            ));
        };
        tx.send(Outbound::Msg(msg))
            .map_err(|_| new_error(crate::tracking::v2::ErrorCode::TryAgain, "socket not open"))
    }

    /// Start a track; waits for `track_started`.
    pub async fn start_track(
        &self,
        loc: Option<LatLng>,
        route: Vec<LatLng>,
    ) -> Result<String, Error> {
        self.start_track_meta(loc, route, Vec::new()).await
    }

    /// Start a track with opaque metadata (≤4 KiB).
    pub async fn start_track_meta(
        &self,
        loc: Option<LatLng>,
        route: Vec<LatLng>,
        metadata: Vec<u8>,
    ) -> Result<String, Error> {
        let (tx, rx) = oneshot::channel();
        {
            let mut g = self.inner.lock().await;
            g.start_wait = Some(PendingStart { tx });
        }
        let location = loc.map(stamp_lat_lng);
        if let Err(e) = self
            .send(ClientMsg {
                body: Some(client_msg::Body::TrackStart(TrackStart {
                    location,
                    route: stamp_lat_lngs(route),
                    metadata,
                })),
            })
            .await
        {
            let mut g = self.inner.lock().await;
            g.start_wait = None;
            return Err(e);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(new_error(
                crate::tracking::v2::ErrorCode::TryAgain,
                "start cancelled",
            )),
        }
    }

    /// Manual resume; auto-reconnect also resumes.
    pub async fn resume(&self, track_uid: String, last_client_seq: u64) -> Result<u64, Error> {
        let (tx, rx) = oneshot::channel();
        {
            let mut g = self.inner.lock().await;
            g.track_uid = track_uid.clone();
            g.client_seq = last_client_seq;
            g.resume_wait = Some(PendingResume { tx });
        }
        if let Err(e) = self
            .send(ClientMsg {
                body: Some(client_msg::Body::Resume(Resume {
                    track_uid,
                    last_client_seq,
                })),
            })
            .await
        {
            let mut g = self.inner.lock().await;
            g.resume_wait = None;
            return Err(e);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(new_error(
                crate::tracking::v2::ErrorCode::TryAgain,
                "resume cancelled",
            )),
        }
    }

    /// Publish a location on the active track (managed clientSeq).
    /// Returns `(seq, accepted)`; when over rate limit, `accepted` is false.
    pub async fn publish(&self, point: LatLng) -> (u64, bool) {
        let (seq, uid, pt, open) = {
            let mut g = self.inner.lock().await;
            if g.track_uid.is_empty() {
                return (0, false);
            }
            let now = Instant::now();
            if !can_accept_publish(g.next_publish_at, now, 1) {
                return (g.client_seq, false);
            }
            g.next_publish_at = next_publish_allowed_at(g.next_publish_at, now, 1);
            g.client_seq += 1;
            let seq = g.client_seq;
            let uid = g.track_uid.clone();
            let pt = stamp_lat_lng(point);
            g.queue.enqueue(seq, pt);
            let open = g.state == ConnectionState::Open && g.out_tx.is_some();
            (seq, uid, pt, open)
        };
        if open {
            let _ = self
                .send(ClientMsg {
                    body: Some(client_msg::Body::LocationAdd(LocationAdd {
                        track_uid: uid,
                        client_seq: seq,
                        point: Some(pt),
                    })),
                })
                .await;
        }
        (seq, true)
    }

    /// Stop the active (or given) track.
    pub async fn stop_track(&self, track_uid: Option<String>) -> Result<(), Error> {
        let track_uid = match track_uid {
            Some(u) if !u.is_empty() => u,
            _ => {
                let u = self.track_uid().await;
                if u.is_empty() {
                    return Err(new_error(
                        crate::tracking::v2::ErrorCode::Invalid,
                        "no active track",
                    ));
                }
                u
            }
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut g = self.inner.lock().await;
            g.stop_wait = Some(PendingStop { tx });
        }
        if let Err(e) = self
            .send(ClientMsg {
                body: Some(client_msg::Body::TrackStop(TrackStop { track_uid })),
            })
            .await
        {
            let mut g = self.inner.lock().await;
            g.stop_wait = None;
            return Err(e);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(new_error(
                crate::tracking::v2::ErrorCode::TryAgain,
                "stop cancelled",
            )),
        }
    }

    /// Fan-out opaque payload (≤4 KiB, ≤1 Hz). Returns `(accepted, error)`.
    pub async fn send_event(&self, payload: Vec<u8>) -> Result<bool, Error> {
        if payload.len() > MAX_EVENT_BYTES {
            return Err(new_error(
                crate::tracking::v2::ErrorCode::Invalid,
                "event payload exceeds 4 KiB",
            ));
        }
        let (uid, open) = {
            let mut g = self.inner.lock().await;
            if g.track_uid.is_empty() {
                return Err(new_error(
                    crate::tracking::v2::ErrorCode::Invalid,
                    "start_track() before send_event()",
                ));
            }
            let now = Instant::now();
            if now < g.next_event_at {
                return Ok(false);
            }
            g.next_event_at = now + MIN_EVENT_INTERVAL;
            (
                g.track_uid.clone(),
                g.state == ConnectionState::Open && g.out_tx.is_some(),
            )
        };
        if !open {
            return Ok(true);
        }
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.send(ClientMsg {
            body: Some(client_msg::Body::Event(Event {
                track_uid: uid,
                payload,
                timestamp_ms: Some(ts),
            })),
        })
        .await?;
        Ok(true)
    }

    /// Subscribe to a device (listener).
    pub async fn subscribe(&self, device_uid: impl Into<String>) -> Result<(), Error> {
        let device_uid = device_uid.into();
        {
            let mut g = self.inner.lock().await;
            g.subscriptions.insert(device_uid.clone());
        }
        self.send(ClientMsg {
            body: Some(client_msg::Body::Subscribe(Subscribe {
                device_uid,
                include_events: None,
                min_location_interval_ms: 0,
            })),
        })
        .await
    }

    /// End the session.
    pub async fn close(&self) -> Result<(), Error> {
        let tx = {
            let mut g = self.inner.lock().await;
            g.intentional = true;
            g.state = ConnectionState::Closed;
            self.reject_pending_locked(
                &mut g,
                new_error(crate::tracking::v2::ErrorCode::Invalid, "client closed"),
            );
            g.out_tx.take()
        };
        if let Some(tx) = tx {
            let _ = tx.send(Outbound::Close);
        }
        Ok(())
    }
}
