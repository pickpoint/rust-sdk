//! Mock tracking WebSocket server (binary `tracking.v2` frames).

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use pickpoint::tracking::{
    decode_client_cmd, encode_server_evt, ClientCmd, Relocate, ServerEvt, SUBPROTOCOL,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tokio_tungstenite::{accept_hdr_async, tungstenite::handshake::server::Request as WsRequest};

pub const MOCK_TRACK_UID: &str = "11111111-1111-1111-1111-111111111111";
pub const MOCK_DEVICE_UID: &str = "22222222-2222-2222-2222-222222222222";
pub const MOCK_NODE_ID: &str = "33333333-3333-3333-3333-333333333333";

pub type OnMsg = Arc<dyn Fn(ClientCmd, MockConn) + Send + Sync>;
pub type BeforeHello = Arc<dyn Fn(usize, MockConn) + Send + Sync>;

#[derive(Clone, Default)]
pub struct MockOpts {
    pub auto: bool,
    pub on_msg: Option<OnMsg>,
    pub before_hello: Option<BeforeHello>,
    pub relocate_on_connect: Option<Relocate>,
}

#[derive(Clone)]
pub struct MockConn {
    inner: Arc<Mutex<MockConnInner>>,
}

struct MockConnInner {
    messages: Vec<ClientCmd>,
    write: Option<
        futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, WsMessage>,
    >,
}

impl MockConn {
    pub async fn send(&self, msg: &ServerEvt) {
        let buf = encode_server_evt(msg);
        let mut g = self.inner.lock().await;
        if let Some(w) = g.write.as_mut() {
            let _ = w.send(WsMessage::Binary(buf.into())).await;
        }
    }

    pub async fn close(&self) {
        let mut g = self.inner.lock().await;
        if let Some(mut w) = g.write.take() {
            let _ = w.close().await;
        }
    }

    pub async fn messages(&self) -> Vec<ClientCmd> {
        self.inner.lock().await.messages.clone()
    }
}

pub struct MockServer {
    pub url: String,
    connections: Arc<Mutex<Vec<MockConn>>>,
    _join: tokio::task::JoinHandle<()>,
}

impl MockServer {
    pub async fn start(auto: bool, on_msg: Option<OnMsg>) -> Self {
        Self::start_opts(MockOpts {
            auto,
            on_msg,
            ..Default::default()
        })
        .await
    }

    pub async fn start_opts(opts: MockOpts) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connections: Arc<Mutex<Vec<MockConn>>> = Arc::new(Mutex::new(Vec::new()));
        let conns = connections.clone();
        let join = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let conns = conns.clone();
                let opts = opts.clone();
                tokio::spawn(async move {
                    handle_conn(stream, conns, opts).await;
                });
            }
        });
        Self {
            url: format!("ws://{addr}"),
            connections,
            _join: join,
        }
    }

    pub async fn wait_conn(&self, timeout: Duration) -> MockConn {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            let g = self.connections.lock().await;
            if let Some(c) = g.first() {
                return c.clone();
            }
            drop(g);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("wait_conn timeout");
    }

    pub async fn conn_count(&self) -> usize {
        self.connections.lock().await.len()
    }

    pub async fn wait_msg<F>(&self, mut pred: F, timeout: Duration) -> ClientCmd
    where
        F: FnMut(&ClientCmd) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            let g = self.connections.lock().await;
            for c in g.iter() {
                let msgs = c.messages().await;
                for m in msgs {
                    if pred(&m) {
                        return m;
                    }
                }
            }
            drop(g);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("wait_msg timeout");
    }

    pub async fn all_messages(&self) -> Vec<ClientCmd> {
        let g = self.connections.lock().await;
        let mut out = Vec::new();
        for c in g.iter() {
            out.extend(c.messages().await);
        }
        out
    }
}

fn hello_evt() -> ServerEvt {
    ServerEvt::Hello {
        version: 2,
        node_id: MOCK_NODE_ID.into(),
        shard: 0,
    }
}

async fn handle_conn(stream: TcpStream, conns: Arc<Mutex<Vec<MockConn>>>, opts: MockOpts) {
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Response};

    #[allow(clippy::result_large_err)]
    let callback = |req: &WsRequest, mut response: Response| -> Result<Response, ErrorResponse> {
        let protos = req
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if protos.split(',').any(|p| p.trim() == SUBPROTOCOL) {
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
        }
        Ok(response)
    };

    let ws = match accept_hdr_async(stream, callback).await {
        Ok(ws) => ws,
        Err(_) => return,
    };

    let (write, mut read) = ws.split();
    let conn = MockConn {
        inner: Arc::new(Mutex::new(MockConnInner {
            messages: Vec::new(),
            write: Some(write),
        })),
    };

    let idx = {
        let mut g = conns.lock().await;
        g.push(conn.clone());
        g.len()
    };

    if let Some(before) = &opts.before_hello {
        let before = before.clone();
        let c = conn.clone();
        let _ = tokio::task::spawn_blocking(move || before(idx, c)).await;
    }

    if let Some(rel) = &opts.relocate_on_connect {
        if idx == 1 {
            conn.send(&ServerEvt::Relocate {
                endpoint: rel.endpoint.clone(),
                retry_after_ms: rel.retry_after_ms,
            })
            .await;
        } else {
            conn.send(&hello_evt()).await;
        }
    } else {
        conn.send(&hello_evt()).await;
    }

    while let Some(frame) = read.next().await {
        let Ok(WsMessage::Binary(b)) = frame else {
            break;
        };
        let Ok(msg) = decode_client_cmd(&b) else {
            continue;
        };
        {
            let mut g = conn.inner.lock().await;
            g.messages.push(msg.clone());
        }
        if let Some(on_msg) = &opts.on_msg {
            on_msg(msg.clone(), conn.clone());
        }
        if !opts.auto {
            continue;
        }
        match msg {
            ClientCmd::TrackStart { .. } => {
                conn.send(&ServerEvt::TrackStarted {
                    track_uid: MOCK_TRACK_UID.into(),
                    metadata: Vec::new(),
                })
                .await;
            }
            ClientCmd::TrackStop { track_uid } => {
                conn.send(&ServerEvt::TrackStopped {
                    track_uid: if track_uid.is_empty() {
                        MOCK_TRACK_UID.into()
                    } else {
                        track_uid
                    },
                })
                .await;
            }
            ClientCmd::Resume { track_uid, .. } => {
                conn.send(&ServerEvt::ResumeOk {
                    track_uid,
                    last_acked_seq: 0,
                })
                .await;
            }
            ClientCmd::LocationAdd { client_seq, .. } => {
                conn.send(&ServerEvt::Ack { seq: client_seq }).await;
            }
            ClientCmd::LocationBatch { client_seq, .. } => {
                conn.send(&ServerEvt::Ack { seq: client_seq }).await;
            }
            ClientCmd::Subscribe { device_uid, .. } => {
                conn.send(&ServerEvt::Subscribed {
                    sub: 1,
                    device_uid,
                    track_uid: MOCK_TRACK_UID.into(),
                    last_location: None,
                    route: Vec::new(),
                    estimated_distance: 0.0,
                    estimated_duration: 0.0,
                    start_location_name: String::new(),
                    end_location_name: String::new(),
                    metadata: Vec::new(),
                    online: false,
                    last_seen_ms: None,
                })
                .await;
            }
            _ => {}
        }
    }
}

pub fn server_error(code: pickpoint::tracking::ErrorCode, message: &str) -> ServerEvt {
    ServerEvt::Error {
        code,
        message: message.into(),
        track_uid: None,
        retry_after_ms: None,
    }
}

pub async fn wait_for<F>(mut pred: F, timeout: Duration)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("wait_for timeout");
}
