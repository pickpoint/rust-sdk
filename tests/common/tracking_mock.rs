//! Mock tracking WebSocket server (Go `mock_server_test.go` parity).

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use pickpoint::tracking::v2::{
    client_msg, server_msg, ClientMsg, Error as WireError, ErrorCode, Hello, LocationAdded,
    Relocate, ResumeOk, ServerMsg, Subscribed, TrackStarted, TrackStopped,
};
use pickpoint::tracking::SUBPROTOCOL;
use prost::Message;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tokio_tungstenite::{accept_hdr_async, tungstenite::handshake::server::Request as WsRequest};

pub type OnMsg = Arc<dyn Fn(ClientMsg, MockConn) + Send + Sync>;
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
    messages: Vec<ClientMsg>,
    write: Option<
        futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, WsMessage>,
    >,
}

impl MockConn {
    pub async fn send(&self, msg: &ServerMsg) {
        let buf = msg.encode_to_vec();
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

    pub async fn messages(&self) -> Vec<ClientMsg> {
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

    pub async fn wait_msg<F>(&self, mut pred: F, timeout: Duration) -> ClientMsg
    where
        F: FnMut(&ClientMsg) -> bool,
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

    pub async fn all_messages(&self) -> Vec<ClientMsg> {
        let g = self.connections.lock().await;
        let mut out = Vec::new();
        for c in g.iter() {
            out.extend(c.messages().await);
        }
        out
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
            conn.send(&ServerMsg {
                body: Some(server_msg::Body::Relocate(rel.clone())),
            })
            .await;
        } else {
            conn.send(&ServerMsg {
                body: Some(server_msg::Body::Hello(Hello {
                    node_id: "mock-1".into(),
                    shard: 0,
                })),
            })
            .await;
        }
    } else {
        conn.send(&ServerMsg {
            body: Some(server_msg::Body::Hello(Hello {
                node_id: "mock-1".into(),
                shard: 0,
            })),
        })
        .await;
    }

    while let Some(frame) = read.next().await {
        let Ok(WsMessage::Binary(b)) = frame else {
            break;
        };
        let Ok(msg) = ClientMsg::decode(&b[..]) else {
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
        match msg.body {
            Some(client_msg::Body::TrackStart(_)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::TrackStarted(TrackStarted {
                        track_uid: "track-mock-1".into(),
                        metadata: Vec::new(),
                    })),
                })
                .await;
            }
            Some(client_msg::Body::TrackStop(s)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::TrackStopped(TrackStopped {
                        track_uid: s.track_uid,
                    })),
                })
                .await;
            }
            Some(client_msg::Body::Resume(r)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::ResumeOk(ResumeOk {
                        track_uid: r.track_uid,
                        last_acked_seq: 0,
                    })),
                })
                .await;
            }
            Some(client_msg::Body::LocationAdd(a)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::LocationAdded(LocationAdded {
                        track_uid: a.track_uid,
                        client_seq: a.client_seq,
                        point: a.point,
                        device_uid: "dev-1".into(),
                    })),
                })
                .await;
            }
            Some(client_msg::Body::LocationBatch(b)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::LocationAdded(LocationAdded {
                        track_uid: b.track_uid,
                        client_seq: b.client_seq,
                        point: None,
                        device_uid: "dev-1".into(),
                    })),
                })
                .await;
            }
            Some(client_msg::Body::Subscribe(s)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::Subscribed(Subscribed {
                        device_uid: s.device_uid,
                        track_uid: "track-mock-1".into(),
                        last_location: None,
                        route: Vec::new(),
                        estimated_distance: 0.0,
                        estimated_duration: 0.0,
                        start_location_name: String::new(),
                        end_location_name: String::new(),
                        metadata: Vec::new(),
                        online: false,
                        last_seen_ms: None,
                    })),
                })
                .await;
            }
            Some(client_msg::Body::Ping(_)) => {
                conn.send(&ServerMsg {
                    body: Some(server_msg::Body::Pong(Default::default())),
                })
                .await;
            }
            _ => {}
        }
    }
}

pub fn server_error(code: ErrorCode, message: &str) -> ServerMsg {
    ServerMsg {
        body: Some(server_msg::Body::Error(WireError {
            code: code as i32,
            message: message.into(),
            track_uid: None,
            retry_after_ms: None,
        })),
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
