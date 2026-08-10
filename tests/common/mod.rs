//! Shared test helpers.

#![allow(dead_code)]

pub mod tracking_mock;

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::Request;
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use tokio::net::TcpListener;

/// Tiny Axum server with a dynamic handler (for stateful auth/retry tests).
pub struct DynServer {
    pub base_url: String,
    _join: tokio::task::JoinHandle<()>,
}

impl DynServer {
    pub async fn start<F, Fut>(handler: F) -> Self
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Response> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let app = Router::new().fallback(any({
            let handler = handler.clone();
            move |req: Request| {
                let handler = handler.clone();
                async move { handler(req).await }
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        Self {
            base_url: format!("http://{addr}"),
            _join: join,
        }
    }
}

pub fn json_response(status: u16, body: impl AsRef<[u8]>) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(Bytes::copy_from_slice(
            body.as_ref(),
        )))
        .unwrap()
}

pub async fn read_body(req: Request) -> Bytes {
    axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default()
}

pub async fn wait_until<F>(mut pred: F, timeout: std::time::Duration)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    panic!("wait_until timeout");
}
