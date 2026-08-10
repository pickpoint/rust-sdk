use std::sync::Arc;

use serde_json::Value;

use crate::error::Result;
use crate::transport::{OnClientError, RequestOpts, Transport};

/// Routing service (Valhalla proxies under `/v2/route*`).
pub struct RoutingApi {
    transport: Arc<Transport>,
}

impl RoutingApi {
    pub(crate) fn new(transport: Arc<Transport>) -> Self {
        Self { transport }
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::POST,
                path,
                query: Vec::new(),
                body: Some(body),
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        if raw.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Compute a route.
    pub async fn route(&self, body: Value) -> Result<Value> {
        self.post("/v2/route", body).await
    }

    /// Optimized multi-stop route.
    pub async fn optimized(&self, body: Value) -> Result<Value> {
        self.post("/v2/route/optimized", body).await
    }

    /// Time/distance matrix.
    pub async fn matrix(&self, body: Value) -> Result<Value> {
        self.post("/v2/route/matrix", body).await
    }

    /// Snap / locate.
    pub async fn locate(&self, body: Value) -> Result<Value> {
        self.post("/v2/route/locate", body).await
    }

    /// Elevation along a path.
    pub async fn elevation(&self, body: Value) -> Result<Value> {
        self.post("/v2/route/elevation", body).await
    }
}
