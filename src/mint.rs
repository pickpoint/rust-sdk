use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};

use crate::config::{Config, DEFAULT_BASE_URL, DEFAULT_TIMEOUT};
use crate::error::{ApiError, Error, Result};
use crate::transport::trim_slash;

/// Response from `POST /v2/client-tokens`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    #[serde(default)]
    pub expires_in: i64,
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// Mint a client-token pair with a secret API key (server-side).
/// Pass empty `scopes` to grant all client-tokenable permissions on the key.
pub async fn mint_client_tokens(
    cfg: &Config,
    scopes: &[String],
    ttl_sec: Option<i64>,
) -> Result<TokenPair> {
    let api_key = cfg
        .api_key
        .as_ref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::invalid_config("mint_client_tokens requires api_key"))?;

    let base = trim_slash(cfg.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL));
    let http = if let Some(c) = &cfg.http_client {
        c.clone()
    } else {
        HttpClient::builder()
            .timeout(cfg.timeout.unwrap_or(DEFAULT_TIMEOUT))
            .build()
            .map_err(Error::Http)?
    };

    let mut payload = serde_json::json!({ "scopes": scopes });
    if let Some(ttl) = ttl_sec.filter(|t| *t > 0) {
        payload["ttlSec"] = serde_json::json!(ttl);
    }

    let res = http
        .post(format!("{base}/v2/client-tokens"))
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("x-api-key", api_key)
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            Error::Api(ApiError::new(
                0,
                "NETWORK",
                format!("mint client tokens network error: {e}"),
                Vec::new(),
            ))
        })?;

    let status = res.status().as_u16();
    let raw = res.bytes().await.unwrap_or_default().to_vec();
    if !(200..300).contains(&status) {
        return Err(Error::Api(ApiError::new(
            status,
            "CLIENT_ERROR",
            format!("mint client tokens failed ({status})"),
            raw,
        )));
    }
    Ok(serde_json::from_slice(&raw)?)
}
