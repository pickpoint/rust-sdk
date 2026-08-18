use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::tracking::backoff::{new_backoff, next_delay, reset_backoff, BackoffState};
use crate::tracking::cmd::{ClientCmd, ServerEvt};
use crate::tracking::codec::{
    decode_server_evt, encode_client_cmd, encode_loc_frames, stamp_lat_lng, strip_live_time,
};
use crate::tracking::errors::{
    error_from_evt, is_auth_error, is_fatal_resume_error, is_retry_resume_error, new_error, Error,
};
use crate::tracking::filter::NoiseFilter;
use crate::tracking::queue::OfflineQueue;
use crate::tracking::rate::{can_accept_publish, next_publish_allowed_at};
use crate::tracking::types::{
    Command, CommandAckStatus, ErrorCode, LatLng, Relocate, PROTOCOL_VERSION,
};
use crate::tracking::url::build_ws_url;

/// WebSocket subprotocol.
pub const SUBPROTOCOL: &str = crate::tracking::codec::SUBPROTOCOL;
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

/// WebSocket `tracking.v2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    /// Binary frames on `/v2/ws` (default).
    #[default]
    Ws,
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
    /// Host, e.g. `wss://tracking.pickpoint.io` (SDK appends `/v2/ws`).
    pub endpoint: String,
    /// Always WebSocket (`tracking.v2`).
    pub transport: Transport,
    /// Device publisher auth.
    pub device: Option<DeviceAuth>,
    /// Listener auth.
    pub listener: Option<ListenerAuth>,
    /// WS path (default `/v2/ws` when empty).
    pub ws_path: String,
    /// Disable auto-reconnect.
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
    /// Listener: subscribe these device UIDs after Hello (and again after reconnect).
    pub subscribe: Vec<String>,
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
            subscribe: Vec::new(),
        }
    }
}

enum Outbound {
    Bin(Vec<u8>),
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
    last_assigned_seq: u64,
    last_acked_seq: u64,
    queue: OfflineQueue,
    filter: NoiseFilter,
    /// Bound after TrackStarted / ResumeOk on this socket.
    session_ready: bool,
    backoff: BackoffState,
    next_publish_at: Instant,
    next_event_at: Instant,
    subscriptions: HashSet<String>,
    sub_by_device: HashMap<String, u8>,
    device_by_sub: HashMap<u8, String>,
    intentional: bool,
    dial_gen: u64,
    out_tx: Option<mpsc::UnboundedSender<Outbound>>,
    start_wait: Option<PendingStart>,
    stop_wait: Option<PendingStop>,
    resume_wait: Option<PendingResume>,
    /// TrackStart sent, waiting for TrackStarted (auto-start or explicit).
    starting: bool,
}

/// Tracking session (device or listener).
#[derive(Clone)]
pub struct Client {
    inner: Arc<Mutex<Inner>>,
    recv_tx: mpsc::Sender<ServerEvt>,
    recv_rx: Arc<Mutex<mpsc::Receiver<ServerEvt>>>,
    cmd_tx: mpsc::Sender<Command>,
    cmd_rx: Arc<Mutex<mpsc::Receiver<Command>>>,
}

/// Connect opens a tracking session (binary `tracking.v2`).
pub async fn connect(cfg: Config) -> Result<Client, Error> {
    if cfg.endpoint.is_empty() {
        return Err(new_error(ErrorCode::Invalid, "Endpoint is required"));
    }
    if cfg.device.is_none() && cfg.listener.is_none() {
        return Err(new_error(
            ErrorCode::Invalid,
            "Device or Listener auth is required",
        ));
    }
    let mut cfg = cfg;
    if cfg.hello_timeout.is_zero() {
        cfg.hello_timeout = Duration::from_secs(10);
    }

    let (recv_tx, recv_rx) = mpsc::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let subscriptions: HashSet<String> = cfg.subscribe.iter().cloned().collect();

    let client = Client {
        inner: Arc::new(Mutex::new(Inner {
            backoff: new_backoff(
                cfg.reconnect_min_delay,
                cfg.reconnect_max_delay,
                cfg.reconnect_max_attempts,
            ),
            queue: OfflineQueue::new(cfg.max_queue_size),
            filter: NoiseFilter::new(),
            cfg,
            state: ConnectionState::Connecting,
            track_uid: String::new(),
            last_assigned_seq: 0,
            last_acked_seq: 0,
            session_ready: false,
            next_publish_at: Instant::now(),
            next_event_at: Instant::now(),
            subscriptions,
            sub_by_device: HashMap::new(),
            device_by_sub: HashMap::new(),
            intentional: false,
            dial_gen: 0,
            out_tx: None,
            start_wait: None,
            stop_wait: None,
            resume_wait: None,
            starting: false,
        })),
        recv_tx,
        recv_rx: Arc::new(Mutex::new(recv_rx)),
        cmd_tx,
        cmd_rx: Arc::new(Mutex::new(cmd_rx)),
    };

    client.dial(false).await?;
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
        self.inner.lock().await.last_assigned_seq
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
            g.session_ready = false;
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

        let url = build_ws_url(&cfg).map_err(|e| new_error(ErrorCode::Invalid, e))?;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = url.as_str().into_client_request().map_err(|e| {
            new_error(ErrorCode::Invalid, format!("ws request: {e}"))
        })?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            http::HeaderValue::from_static(crate::tracking::codec::SUBPROTOCOL),
        );

        let (ws, _) = connect_async(request)
            .await
            .map_err(|e| new_error(ErrorCode::TryAgain, format!("ws dial: {e}")))?;

        let (mut write, mut read) = ws.split();

        let hello_timeout = cfg.hello_timeout;
        let first = tokio::time::timeout(hello_timeout, read.next())
            .await
            .map_err(|_| new_error(ErrorCode::TryAgain, "hello timeout"))?
            .ok_or_else(|| new_error(ErrorCode::TryAgain, "connection closed before hello"))?
            .map_err(|e| new_error(ErrorCode::TryAgain, format!("ws read: {e}")))?;

        let data = match first {
            Message::Binary(b) => b,
            other => {
                return Err(new_error(
                    ErrorCode::Invalid,
                    format!("expected binary hello, got {other:?}"),
                ));
            }
        };
        let msg = decode_server_evt(&data)
            .map_err(|e| new_error(ErrorCode::Invalid, format!("decode hello: {e}")))?;

        match msg {
            Some(ServerEvt::Hello { version, .. }) => {
                if version != PROTOCOL_VERSION {
                    return Err(new_error(
                        ErrorCode::Invalid,
                        format!("unsupported hello version {version}"),
                    ));
                }
            }
            Some(ServerEvt::Relocate {
                endpoint,
                retry_after_ms,
            }) => {
                return self
                    .handle_relocate(
                        Relocate {
                            endpoint,
                            retry_after_ms,
                        },
                        send_resume,
                    )
                    .await;
            }
            Some(evt @ ServerEvt::Error { .. }) => {
                return Err(error_from_evt(&evt));
            }
            _ => {
                return Err(new_error(ErrorCode::Invalid, "expected hello"));
            }
        }

        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        {
            let mut g = self.inner.lock().await;
            if gen != g.dial_gen || g.intentional {
                return Err(new_error(ErrorCode::Invalid, "dial superseded"));
            }
            g.out_tx = Some(out_tx);
            g.state = ConnectionState::Open;
            reset_backoff(&mut g.backoff);
        }

        tokio::spawn(async move {
            while let Some(out) = out_rx.recv().await {
                match out {
                    Outbound::Bin(buf) => {
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
                    Ok(Message::Binary(b)) => match decode_server_evt(&b) {
                        Ok(Some(msg)) => this_read.dispatch(msg).await,
                        Ok(None) => {} // unknown server type: ignore
                        Err(_) => {}
                    },
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
            return Err(new_error(ErrorCode::Invalid, "closed"));
        }
        self.dial_boxed(send_resume).await
    }

    async fn dispatch(&self, msg: ServerEvt) {
        match &msg {
            ServerEvt::Relocate {
                endpoint,
                retry_after_ms,
            } => {
                let rel = Relocate {
                    endpoint: endpoint.clone(),
                    retry_after_ms: *retry_after_ms,
                };
                let this = self.clone();
                tokio::spawn(async move {
                    let _ = this.handle_relocate(rel, true).await;
                });
                return;
            }
            ServerEvt::ResumeOk {
                track_uid,
                last_acked_seq,
            } => {
                let wait = {
                    let mut g = self.inner.lock().await;
                    if !track_uid.is_empty() {
                        g.track_uid = track_uid.clone();
                    }
                    g.last_acked_seq = *last_acked_seq;
                    if g.last_assigned_seq < g.last_acked_seq {
                        g.last_assigned_seq = g.last_acked_seq;
                    }
                    g.queue.ack_through(*last_acked_seq);
                    g.session_ready = true;
                    g.resume_wait.take()
                };
                self.flush_queue().await;
                if let Some(w) = wait {
                    let _ = w.tx.send(Ok(*last_acked_seq));
                }
            }
            ServerEvt::TrackStarted { track_uid, .. } => {
                let wait = {
                    let mut g = self.inner.lock().await;
                    g.track_uid = track_uid.clone();
                    g.last_assigned_seq = 0;
                    g.last_acked_seq = 0;
                    g.session_ready = true;
                    g.starting = false;
                    g.start_wait.take()
                };
                self.flush_queue().await;
                if let Some(w) = wait {
                    let _ = w.tx.send(Ok(track_uid.clone()));
                }
            }
            ServerEvt::TrackStopped { track_uid } => {
                let wait = {
                    let mut g = self.inner.lock().await;
                    if g.track_uid == *track_uid {
                        g.track_uid.clear();
                        g.queue.clear();
                        g.session_ready = false;
                        g.filter.reset();
                    }
                    g.stop_wait.take()
                };
                if let Some(w) = wait {
                    let _ = w.tx.send(Ok(()));
                }
            }
            ServerEvt::Ack { seq } => {
                {
                    let mut g = self.inner.lock().await;
                    if *seq > g.last_acked_seq {
                        g.last_acked_seq = *seq;
                    }
                    g.queue.ack_through(*seq);
                }
                self.flush_queue().await;
                return;
            }
            ServerEvt::Command {
                command_id,
                payload,
                timestamp_ms,
            } => {
                let _ = self.cmd_tx.try_send(Command {
                    command_id: command_id.clone(),
                    payload: payload.clone(),
                    timestamp_ms: *timestamp_ms,
                });
                return;
            }
            ServerEvt::Error { code, .. } => {
                let e = error_from_evt(&msg);
                {
                    let mut g = self.inner.lock().await;
                    if let Some(w) = g.resume_wait.take() {
                        if is_fatal_resume_error(*code) {
                            g.track_uid.clear();
                            g.queue.clear();
                            g.session_ready = false;
                            g.filter.reset();
                        }
                        let _ = w.tx.send(Err(e.clone()));
                    }
                    if let Some(w) = g.start_wait.take() {
                        g.starting = false;
                        let _ = w.tx.send(Err(e.clone()));
                    }
                    if let Some(w) = g.stop_wait.take() {
                        let _ = w.tx.send(Err(e.clone()));
                    }
                    if *code == ErrorCode::TrackNotFound && g.resume_wait.is_none() {
                        g.track_uid.clear();
                        g.queue.clear();
                        g.session_ready = false;
                        g.starting = false;
                    }
                }
                if is_auth_error(*code) {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_auth_error().await;
                    });
                }
            }
            ServerEvt::Subscribed { sub, device_uid, .. } => {
                let mut g = self.inner.lock().await;
                g.sub_by_device.insert(device_uid.clone(), *sub);
                g.device_by_sub.insert(*sub, device_uid.clone());
            }
            ServerEvt::LocationAdded { sub, .. }
            | ServerEvt::EventAdded { sub, .. }
            | ServerEvt::DevicePresence { sub, .. } => {
                let uid = {
                    let g = self.inner.lock().await;
                    g.device_by_sub.get(sub).cloned().unwrap_or_default()
                };
                let mut stamped = msg.clone();
                match &mut stamped {
                    ServerEvt::LocationAdded { device_uid, .. }
                    | ServerEvt::EventAdded { device_uid, .. }
                    | ServerEvt::DevicePresence { device_uid, .. } => {
                        if device_uid.is_empty() {
                            *device_uid = uid;
                        }
                    }
                    _ => {}
                }
                let _ = self.recv_tx.try_send(stamped);
                return;
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
                    g.session_ready = false;
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
                g.session_ready = false;
                g.queue.mark_unsent();
                g.sub_by_device.clear();
                g.device_by_sub.clear();
                if g.intentional {
                    g.state = ConnectionState::Closed;
                    return;
                }
                if g.cfg.disable_reconnect {
                    g.state = ConnectionState::Closed;
                    this.reject_pending_locked(
                        &mut g,
                        new_error(ErrorCode::TryAgain, "connection closed"),
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
                            new_error(ErrorCode::TryAgain, "reconnect attempts exhausted"),
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
            let mut g = self.inner.lock().await;
            g.sub_by_device.clear();
            g.device_by_sub.clear();
            g.subscriptions.iter().cloned().collect()
        };
        for d in subs {
            let _ = self
                .send_cmd(ClientCmd::Subscribe {
                    device_uid: d,
                    include_events: true,
                    min_location_interval_ms: 0,
                })
                .await;
        }
    }

    async fn send_resume_and_wait(&self) -> Result<(), Error> {
        loop {
            let (uid, seq, rx) = {
                let mut g = self.inner.lock().await;
                if g.track_uid.is_empty() {
                    return Ok(());
                }
                let (tx, rx) = oneshot::channel();
                g.resume_wait = Some(PendingResume { tx });
                (g.track_uid.clone(), g.last_assigned_seq, rx)
            };
            self.send_cmd(ClientCmd::Resume {
                track_uid: uid,
                last_client_seq: seq,
            })
            .await?;
            match rx.await {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) if is_retry_resume_error(e.code) => {
                    let ms = e.retry_after_ms.unwrap_or(200) as u64;
                    tokio::time::sleep(Duration::from_millis(ms.max(50))).await;
                    if self.inner.lock().await.intentional {
                        return Err(new_error(ErrorCode::Invalid, "closed"));
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(new_error(ErrorCode::TryAgain, "resume cancelled"));
                }
            }
        }
    }

    async fn flush_queue(&self) {
        loop {
            let frames = {
                let mut g = self.inner.lock().await;
                if !g.session_ready || g.state != ConnectionState::Open || g.out_tx.is_none() {
                    return;
                }
                let remaining = g.queue.window_remaining();
                if remaining == 0 {
                    return;
                }
                let mut points: Vec<LatLng> = g
                    .queue
                    .unsent_inflight()
                    .into_iter()
                    .map(|p| p.point)
                    .collect();
                let mut last_seq = g
                    .queue
                    .unsent_inflight()
                    .last()
                    .map(|p| p.seq)
                    .unwrap_or(0);
                if points.is_empty() {
                    let take = g.queue.staging_len().min(100 * remaining);
                    if take == 0 {
                        return;
                    }
                    let next_seq = g.last_assigned_seq;
                    let assigned = g.queue.assign_from_staging(take, next_seq);
                    if let Some(last) = assigned.last() {
                        g.last_assigned_seq = last.seq;
                        last_seq = last.seq;
                    }
                    points = assigned.into_iter().map(|p| p.point).collect();
                } else {
                    last_seq = g
                        .queue
                        .unsent_inflight()
                        .last()
                        .map(|p| p.seq)
                        .unwrap_or(last_seq);
                }
                if points.is_empty() {
                    return;
                }
                let encoded = encode_loc_frames(last_seq, &points);
                let mut out = Vec::new();
                for frame in encoded {
                    if g.queue.window_full() {
                        break;
                    }
                    // seq of this frame is bytes 1..5 LE
                    let seq = if frame.len() >= 5 {
                        u32::from_le_bytes(frame[1..5].try_into().unwrap()) as u64
                    } else {
                        last_seq
                    };
                    g.queue.record_frame(seq);
                    out.push(frame);
                    if out.len() >= remaining {
                        break;
                    }
                }
                out
            };
            if frames.is_empty() {
                return;
            }
            for frame in frames {
                if self.send_bin(frame).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Next server message (LocationAdded, Subscribed, …). Commands go to [`Self::recv_command`].
    pub async fn recv(&self) -> Result<ServerEvt, Error> {
        let mut rx = self.recv_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| new_error(ErrorCode::Invalid, "receive channel closed"))
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
        self.send_cmd(ClientCmd::CommandAck {
            command_id: command_id.into(),
            status,
            message,
        })
        .await
    }

    async fn send_bin(&self, buf: Vec<u8>) -> Result<(), Error> {
        let g = self.inner.lock().await;
        if g.state == ConnectionState::Closed && g.intentional {
            return Err(new_error(ErrorCode::Invalid, "closed"));
        }
        let Some(tx) = &g.out_tx else {
            return Err(new_error(ErrorCode::TryAgain, "socket not open"));
        };
        tx.send(Outbound::Bin(buf))
            .map_err(|_| new_error(ErrorCode::TryAgain, "socket not open"))
    }

    async fn send_cmd(&self, cmd: ClientCmd) -> Result<(), Error> {
        self.send_bin(encode_client_cmd(&cmd)).await
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
            g.starting = true;
            g.queue.clear();
            g.last_assigned_seq = 0;
            g.last_acked_seq = 0;
            g.session_ready = false;
            g.filter.reset();
            if let Some(ref p) = loc {
                g.filter.seed(stamp_lat_lng(p.clone()));
            }
        }
        let location = loc.map(stamp_lat_lng);
        if let Err(e) = self
            .send_cmd(ClientCmd::TrackStart {
                location,
                route: crate::tracking::codec::stamp_lat_lngs(route),
                metadata,
            })
            .await
        {
            let mut g = self.inner.lock().await;
            g.start_wait = None;
            g.starting = false;
            return Err(e);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(new_error(ErrorCode::TryAgain, "start cancelled")),
        }
    }

    /// Manual resume; auto-reconnect also resumes.
    pub async fn resume(&self, track_uid: String, last_client_seq: u64) -> Result<u64, Error> {
        loop {
            let rx = {
                let mut g = self.inner.lock().await;
                g.track_uid = track_uid.clone();
                g.last_assigned_seq = last_client_seq;
                let (tx, rx) = oneshot::channel();
                g.resume_wait = Some(PendingResume { tx });
                rx
            };
            if let Err(e) = self
                .send_cmd(ClientCmd::Resume {
                    track_uid: track_uid.clone(),
                    last_client_seq,
                })
                .await
            {
                let mut g = self.inner.lock().await;
                g.resume_wait = None;
                return Err(e);
            }
            match rx.await {
                Ok(Ok(seq)) => return Ok(seq),
                Ok(Err(e)) if is_retry_resume_error(e.code) => {
                    let ms = e.retry_after_ms.unwrap_or(200) as u64;
                    tokio::time::sleep(Duration::from_millis(ms.max(50))).await;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(new_error(ErrorCode::TryAgain, "resume cancelled")),
            }
        }
    }

    /// Publish a GPS sample.
    ///
    /// If there is no live track, this sends `TrackStart` (the point is the start
    /// location). Further samples are filtered and staged until `TrackStarted`.
    /// Returns `(seq, accepted)`.
    pub async fn publish(&self, point: LatLng) -> (u64, bool) {
        let start = {
            let mut g = self.inner.lock().await;
            if g.track_uid.is_empty() && !g.starting {
                g.starting = true;
                g.queue.clear();
                g.last_assigned_seq = 0;
                g.last_acked_seq = 0;
                g.session_ready = false;
                g.filter.reset();
                g.filter.seed(stamp_lat_lng(point.clone()));
                Some(stamp_lat_lng(point.clone()))
            } else {
                None
            }
        };
        if let Some(location) = start {
            if let Err(_) = self
                .send_cmd(ClientCmd::TrackStart {
                    location: Some(location),
                    route: vec![],
                    metadata: Vec::new(),
                })
                .await
            {
                let mut g = self.inner.lock().await;
                g.starting = false;
                return (0, false);
            }
            return (0, true);
        }
        let work = {
            let mut g = self.inner.lock().await;
            let now = Instant::now();
            if !can_accept_publish(g.next_publish_at, now, 1) {
                return (g.last_assigned_seq, false);
            }
            g.next_publish_at = next_publish_allowed_at(g.next_publish_at, now, 1);
            let pt = stamp_lat_lng(point);
            let Some(emitted) = g.filter.push(pt) else {
                return (g.last_assigned_seq, true);
            };
            let open = g.session_ready
                && g.state == ConnectionState::Open
                && g.out_tx.is_some()
                && !g.queue.window_full();
            if !open {
                g.queue.push_staging(emitted);
                return (g.last_assigned_seq, true);
            }
            g.last_assigned_seq += 1;
            let seq = g.last_assigned_seq;
            g.queue.enqueue(seq, emitted.clone());
            g.queue.record_frame(seq);
            Some((seq, strip_live_time(emitted)))
        };
        if let Some((seq, pt)) = work {
            let _ = self
                .send_cmd(ClientCmd::LocationAdd {
                    track_uid: String::new(),
                    client_seq: seq,
                    point: pt,
                })
                .await;
            (seq, true)
        } else {
            (0, true)
        }
    }

    /// Stop the active (or given) track. Idle (no track) is a client no-op.
    pub async fn stop_track(&self, track_uid: Option<String>) -> Result<(), Error> {
        let local = self.track_uid().await;
        let track_uid = match track_uid {
            Some(u) if !u.is_empty() => u,
            _ => {
                if local.is_empty() {
                    return Ok(());
                }
                local
            }
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut g = self.inner.lock().await;
            g.stop_wait = Some(PendingStop { tx });
        }
        if let Err(e) = self
            .send_cmd(ClientCmd::TrackStop { track_uid })
            .await
        {
            let mut g = self.inner.lock().await;
            g.stop_wait = None;
            return Err(e);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(new_error(ErrorCode::TryAgain, "stop cancelled")),
        }
    }

    /// Fan-out opaque payload (≤4 KiB, ≤1 Hz). Returns `(accepted, error)`.
    pub async fn send_event(&self, payload: Vec<u8>) -> Result<bool, Error> {
        if payload.len() > MAX_EVENT_BYTES {
            return Err(new_error(
                ErrorCode::Invalid,
                "event payload exceeds 4 KiB",
            ));
        }
        let open = {
            let mut g = self.inner.lock().await;
            if g.track_uid.is_empty() || !g.session_ready {
                return Err(new_error(
                    ErrorCode::Invalid,
                    "start_track() before send_event()",
                ));
            }
            let now = Instant::now();
            if now < g.next_event_at {
                return Ok(false);
            }
            g.next_event_at = now + MIN_EVENT_INTERVAL;
            g.state == ConnectionState::Open && g.out_tx.is_some()
        };
        if !open {
            return Ok(true);
        }
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.send_cmd(ClientCmd::Event {
            track_uid: String::new(),
            payload,
            timestamp_ms: Some(ts),
        })
        .await?;
        Ok(true)
    }

    /// Subscribe to a device (listener). Include events unless the app opted out
    /// by using a future options API; this method always sets the include-events flag.
    pub async fn subscribe(&self, device_uid: impl Into<String>) -> Result<(), Error> {
        let device_uid = device_uid.into();
        {
            let mut g = self.inner.lock().await;
            g.subscriptions.insert(device_uid.clone());
        }
        self.send_cmd(ClientCmd::Subscribe {
            device_uid,
            include_events: true,
            min_location_interval_ms: 0,
        })
        .await
    }

    /// Unsubscribe by the `sub` handle from `Subscribed` (looked up by device uid).
    pub async fn unsubscribe(&self, device_uid: impl Into<String>) -> Result<(), Error> {
        let device_uid = device_uid.into();
        let sub = {
            let mut g = self.inner.lock().await;
            g.subscriptions.remove(&device_uid);
            g.sub_by_device.remove(&device_uid)
        };
        if let Some(sub) = sub {
            self.send_cmd(ClientCmd::Unsubscribe { sub }).await
        } else {
            Ok(())
        }
    }

    /// End the session. Sends `TrackStop` if a track is live, then hangs up.
    /// Do not Resume afterwards.
    pub async fn close(&self) -> Result<(), Error> {
        let uid = {
            let g = self.inner.lock().await;
            g.track_uid.clone()
        };
        if !uid.is_empty() {
            let _ = self.send_cmd(ClientCmd::TrackStop { track_uid: uid }).await;
        }
        let tx = {
            let mut g = self.inner.lock().await;
            g.intentional = true;
            g.state = ConnectionState::Closed;
            g.session_ready = false;
            self.reject_pending_locked(&mut g, new_error(ErrorCode::Invalid, "client closed"));
            g.out_tx.take()
        };
        if let Some(tx) = tx {
            let _ = tx.send(Outbound::Close);
        }
        Ok(())
    }
}
