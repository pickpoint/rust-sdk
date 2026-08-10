use std::sync::Arc;

use serde_json::Value;

use crate::error::Result;
use crate::geocoding::Query;
use crate::transport::{OnClientError, RequestOpts, Transport};

/// Address search service (`GET /v2/address/search`).
pub struct AddressApi {
    transport: Arc<Transport>,
}

impl AddressApi {
    pub(crate) fn new(transport: Arc<Transport>) -> Self {
        Self { transport }
    }

    /// Address autocomplete / place search (Photon).
    pub async fn search(&self, q: Query) -> Result<Value> {
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::GET,
                path: "/v2/address/search",
                query: q.into_iter().filter(|(_, v)| !v.is_empty()).collect(),
                body: None,
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }
}
