use std::sync::Arc;
use std::time::Duration;

use reqwest::Client as HttpClient;
use serde_json::Value;

use crate::address::AddressApi;
use crate::auth::resolve_auth;
use crate::config::{
    Config, DEFAULT_BASE_URL, DEFAULT_CONCURRENCY, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE,
    DEFAULT_TIMEOUT, MAX_CONCURRENCY, MIN_RETRY_BASE,
};
use crate::devices::{
    Device, DeviceCommandResult, DeviceInput, DeviceListQuery, DeviceListResult, DevicesApi,
};
use crate::error::Result;
use crate::geocoding::{GeocodingApi, Query};
use crate::routing::RoutingApi;
use crate::transport::{trim_slash, Transport};

/// Unified public-api client (geocoding, address, routing, devices).
///
/// Tracking (WebSocket / gRPC) lives in [`crate::tracking`].
#[derive(Clone)]
pub struct Client {
    transport: Arc<Transport>,
    concurrency: usize,
    geocoding: Arc<GeocodingApi>,
    address: Arc<AddressApi>,
    routing: Arc<RoutingApi>,
    devices: Arc<DevicesApi>,
}

impl Client {
    /// Build a client. Provide exactly one of `api_key`, `client_auth`, `access_token`.
    pub async fn new(cfg: Config) -> Result<Self> {
        let base = trim_slash(cfg.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL));
        let timeout = cfg.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let http = if let Some(c) = &cfg.http_client {
            c.clone()
        } else {
            HttpClient::builder()
                .timeout(timeout)
                .build()
                .map_err(crate::error::Error::Http)?
        };

        let max_retries = cfg.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let mut retry_base = cfg.retry_base.unwrap_or(DEFAULT_RETRY_BASE);
        if retry_base < MIN_RETRY_BASE {
            retry_base = MIN_RETRY_BASE;
        }
        let mut concurrency = cfg.concurrency.unwrap_or(DEFAULT_CONCURRENCY);
        if concurrency == 0 {
            concurrency = DEFAULT_CONCURRENCY;
        }
        if concurrency > MAX_CONCURRENCY {
            concurrency = MAX_CONCURRENCY;
        }

        let auth = resolve_auth(&cfg, &base, http.clone()).await?;
        let transport = Arc::new(Transport {
            base_url: base,
            http,
            auth,
            max_retries,
            retry_base,
        });

        let geocoding = Arc::new(GeocodingApi::new(transport.clone(), concurrency));
        let address = Arc::new(AddressApi::new(transport.clone()));
        let routing = Arc::new(RoutingApi::new(transport.clone()));
        let devices = Arc::new(DevicesApi::new(transport.clone()));

        Ok(Self {
            transport,
            concurrency,
            geocoding,
            address,
            routing,
            devices,
        })
    }

    /// Parallel geocode batch concurrency.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Retry backoff base duration.
    pub fn retry_base(&self) -> Duration {
        self.transport.retry_base
    }

    /// Forward geocode shortcut.
    pub async fn forward(&self, q: Query) -> Result<Vec<Value>> {
        self.geocoding.forward(q).await
    }

    /// Reverse geocode shortcut.
    pub async fn reverse(&self, q: Query) -> Result<Option<Value>> {
        self.geocoding.reverse(q).await
    }

    /// Place lookup shortcut.
    pub async fn lookup(&self, q: Query) -> Result<Vec<Value>> {
        self.geocoding.lookup(q).await
    }

    /// Batch forward geocode.
    pub async fn forward_batch(&self, qs: Vec<Query>) -> Result<Vec<Vec<Value>>> {
        self.geocoding.forward_batch(qs).await
    }

    /// Batch reverse geocode.
    pub async fn reverse_batch(&self, qs: Vec<Query>) -> Result<Vec<Option<Value>>> {
        self.geocoding.reverse_batch(qs).await
    }

    /// Batch lookup.
    pub async fn lookup_batch(&self, qs: Vec<Query>) -> Result<Vec<Vec<Value>>> {
        self.geocoding.lookup_batch(qs).await
    }

    /// Namespaced geocoding API.
    pub fn geocoding(&self) -> &GeocodingApi {
        &self.geocoding
    }

    /// Address search shortcut.
    pub async fn search(&self, q: Query) -> Result<Value> {
        self.address.search(q).await
    }

    /// Namespaced address API.
    pub fn address(&self) -> &AddressApi {
        &self.address
    }

    /// Route shortcut.
    pub async fn route(&self, body: Value) -> Result<Value> {
        self.routing.route(body).await
    }

    /// Optimized multi-stop route shortcut.
    pub async fn optimized_route(&self, body: Value) -> Result<Value> {
        self.routing.optimized(body).await
    }

    /// Time/distance matrix shortcut.
    pub async fn matrix(&self, body: Value) -> Result<Value> {
        self.routing.matrix(body).await
    }

    /// Locate shortcut.
    pub async fn locate(&self, body: Value) -> Result<Value> {
        self.routing.locate(body).await
    }

    /// Elevation shortcut.
    pub async fn elevation(&self, body: Value) -> Result<Value> {
        self.routing.elevation(body).await
    }

    /// Namespaced routing API.
    pub fn routing(&self) -> &RoutingApi {
        &self.routing
    }

    /// Namespaced devices API.
    pub fn devices(&self) -> &DevicesApi {
        &self.devices
    }

    /// List devices shortcut.
    pub async fn list_devices(&self, q: DeviceListQuery) -> Result<DeviceListResult> {
        self.devices.list(q).await
    }

    /// Get device shortcut.
    pub async fn get_device(&self, uid: &str) -> Result<Device> {
        self.devices.get(uid).await
    }

    /// Create device shortcut.
    pub async fn create_device(&self, input: DeviceInput) -> Result<Device> {
        self.devices.create(input).await
    }

    /// Update device shortcut.
    pub async fn update_device(&self, uid: &str, input: DeviceInput) -> Result<Device> {
        self.devices.update(uid, input).await
    }

    /// Delete device shortcut.
    pub async fn delete_device(&self, uid: &str) -> Result<()> {
        self.devices.delete(uid).await
    }

    /// Device command shortcut.
    pub async fn device_command(&self, uid: &str, payload: &[u8]) -> Result<DeviceCommandResult> {
        self.devices.command(uid, payload).await
    }
}
