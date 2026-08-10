use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::{self, StreamExt};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::transport::{OnClientError, RequestOpts, Transport};

/// Loose query-string map for geocode / address endpoints.
pub type Query = HashMap<String, String>;

/// Helper to build a [`Query`] from string pairs.
pub fn query(pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Query {
    pairs
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
}

/// Geocoding service (`/v2/geocode/*`, `/v2/address/lookup`).
pub struct GeocodingApi {
    transport: Arc<Transport>,
    concurrency: usize,
}

impl GeocodingApi {
    pub(crate) fn new(transport: Arc<Transport>, concurrency: usize) -> Self {
        Self {
            transport,
            concurrency,
        }
    }

    /// Forward geocode. On non-auth 4xx returns an empty array (batch-friendly).
    pub async fn forward(&self, q: Query) -> Result<Vec<Value>> {
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::GET,
                path: "/v2/geocode/forward",
                query: query_pairs(&q),
                body: None,
                on_client_error: OnClientError::Empty,
                empty: Some(b"[]"),
            })
            .await?;
        decode_json_array(&raw)
    }

    /// Reverse geocode. On non-auth 4xx returns `None`.
    pub async fn reverse(&self, q: Query) -> Result<Option<Value>> {
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::GET,
                path: "/v2/geocode/reverse",
                query: query_pairs(&q),
                body: None,
                on_client_error: OnClientError::Empty,
                empty: Some(b"null"),
            })
            .await?;
        if raw.is_empty() || raw == b"null" {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    /// Lookup OSM ids (`GET /v2/address/lookup`).
    pub async fn lookup(&self, q: Query) -> Result<Vec<Value>> {
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::GET,
                path: "/v2/address/lookup",
                query: query_pairs(&q),
                body: None,
                on_client_error: OnClientError::Empty,
                empty: Some(b"[]"),
            })
            .await?;
        decode_json_array(&raw)
    }

    /// Batch forward with at most `concurrency` in flight.
    pub async fn forward_batch(&self, qs: Vec<Query>) -> Result<Vec<Vec<Value>>> {
        run_batch(self.concurrency, qs, |q| self.forward(q)).await
    }

    /// Batch reverse.
    pub async fn reverse_batch(&self, qs: Vec<Query>) -> Result<Vec<Option<Value>>> {
        run_batch(self.concurrency, qs, |q| self.reverse(q)).await
    }

    /// Batch lookup.
    pub async fn lookup_batch(&self, qs: Vec<Query>) -> Result<Vec<Vec<Value>>> {
        run_batch(self.concurrency, qs, |q| self.lookup(q)).await
    }
}

fn query_pairs(q: &Query) -> Vec<(String, String)> {
    q.iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn decode_json_array(raw: &[u8]) -> Result<Vec<Value>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    if let Ok(out) = serde_json::from_slice::<Vec<Value>>(raw) {
        return Ok(out);
    }
    let one: Value = serde_json::from_slice(raw)?;
    if one.is_null() {
        return Ok(Vec::new());
    }
    Ok(vec![one])
}

async fn run_batch<T, F, Fut>(concurrency: usize, inputs: Vec<Query>, fn_: F) -> Result<Vec<T>>
where
    T: Send + 'static,
    F: Fn(Query) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T>> + Send,
{
    let concurrency = concurrency.max(1);
    let n = inputs.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let mut stream = stream::iter(inputs.into_iter().enumerate())
        .map(|(i, q)| {
            let fut = fn_(q);
            async move { (i, fut.await) }
        })
        .buffer_unordered(concurrency);

    let mut out: Vec<Option<T>> = (0..n).map(|_| None).collect();
    let mut first_err: Option<Error> = None;
    while let Some((i, res)) = stream.next().await {
        match res {
            Ok(v) => out[i] = Some(v),
            Err(e) => {
                first_err = Some(e);
                // Stop polling; dropping `stream` cancels remaining work (Go cancel parity).
                break;
            }
        }
    }
    drop(stream);
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(out.into_iter().map(|v| v.expect("filled")).collect())
}
