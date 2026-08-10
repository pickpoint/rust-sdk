use std::sync::Arc;

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;
use crate::transport::{OnClientError, RequestOpts, Transport};

/// Public-api device row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    #[serde(default)]
    pub id: i64,
    pub uid: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tracks_count: i64,
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub secret: String,
    #[serde(default)]
    pub metadata: Option<String>,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub last_location: Option<Value>,
}

/// Create/update body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInput {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
}

/// `GET /v2/devices` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceListResult {
    #[serde(default)]
    pub data: Vec<Device>,
    #[serde(default)]
    pub total: i64,
}

/// Optional list filters.
#[derive(Debug, Clone, Default)]
pub struct DeviceListQuery {
    pub skip: Option<i64>,
    pub take: Option<i64>,
    pub search: Option<String>,
    pub idle: bool,
}

/// `POST /v2/devices/{uid}/command` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceCommandResult {
    #[serde(default)]
    pub delivered: u64,
}

/// Devices service (`/v2/devices*`).
pub struct DevicesApi {
    transport: Arc<Transport>,
}

impl DevicesApi {
    pub(crate) fn new(transport: Arc<Transport>) -> Self {
        Self { transport }
    }

    /// List devices.
    pub async fn list(&self, q: DeviceListQuery) -> Result<DeviceListResult> {
        let mut query = Vec::new();
        if let Some(skip) = q.skip.filter(|v| *v > 0) {
            query.push(("skip".into(), skip.to_string()));
        }
        if let Some(take) = q.take.filter(|v| *v > 0) {
            query.push(("take".into(), take.to_string()));
        }
        if let Some(search) = q.search.filter(|s| !s.is_empty()) {
            query.push(("search".into(), search));
        }
        if q.idle {
            query.push(("idle".into(), "1".into()));
        }
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::GET,
                path: "/v2/devices",
                query,
                body: None,
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Get a device by UID.
    pub async fn get(&self, uid: &str) -> Result<Device> {
        let path = format!("/v2/devices/{}", urlencoding_path(uid));
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::GET,
                path: &path,
                query: Vec::new(),
                body: None,
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Create a device.
    pub async fn create(&self, input: DeviceInput) -> Result<Device> {
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::POST,
                path: "/v2/devices",
                query: Vec::new(),
                body: Some(serde_json::to_value(input)?),
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Update a device.
    pub async fn update(&self, uid: &str, input: DeviceInput) -> Result<Device> {
        let path = format!("/v2/devices/{}", urlencoding_path(uid));
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::PATCH,
                path: &path,
                query: Vec::new(),
                body: Some(serde_json::to_value(input)?),
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Delete a device.
    pub async fn delete(&self, uid: &str) -> Result<()> {
        let path = format!("/v2/devices/{}", urlencoding_path(uid));
        self.transport
            .do_request(RequestOpts {
                method: reqwest::Method::DELETE,
                path: &path,
                query: Vec::new(),
                body: None,
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(())
    }

    /// Inject opaque bytes into an online device session (SDK base64-encodes).
    pub async fn command(&self, uid: &str, payload: &[u8]) -> Result<DeviceCommandResult> {
        let path = format!("/v2/devices/{}/command", urlencoding_path(uid));
        let body = serde_json::json!({
            "payload": base64::engine::general_purpose::STANDARD.encode(payload),
        });
        let raw = self
            .transport
            .do_request(RequestOpts {
                method: reqwest::Method::POST,
                path: &path,
                query: Vec::new(),
                body: Some(body),
                on_client_error: OnClientError::Throw,
                empty: None,
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }
}

fn urlencoding_path(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}
