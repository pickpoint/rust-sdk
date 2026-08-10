use std::time::Duration;

use reqwest::Client as HttpClient;

/// Default public API base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.pickpoint.io";
/// Default max retries for 5xx / network errors.
pub const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default exponential backoff base.
pub const DEFAULT_RETRY_BASE: Duration = Duration::from_secs(1);
/// Minimum retry base.
pub const MIN_RETRY_BASE: Duration = Duration::from_millis(200);
/// Default per-attempt timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Max parallel geocode batch requests.
pub const MAX_CONCURRENCY: usize = 20;
/// Default parallel geocode batch requests.
pub const DEFAULT_CONCURRENCY: usize = 20;
/// Refresh client-auth when this fraction of access TTL has elapsed.
pub(crate) const CLIENT_AUTH_REFRESH_AT: f64 = 0.5;

/// Pair from `POST /v2/client-tokens` (via your backend).
/// `expires_at` is unix epoch milliseconds.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientAuth {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

/// Configures the public-api client.
/// Provide exactly one of `api_key`, `client_auth`, or `access_token`.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Secret key (`x-api-key`). Prefer for server-side use.
    pub api_key: Option<String>,
    /// Short-lived SPA pair. SDK refreshes at ~50% TTL and on 401.
    pub client_auth: Option<ClientAuth>,
    /// Static Bearer (not refreshable). Prefer [`ClientAuth`].
    pub access_token: Option<String>,
    /// Defaults to [`DEFAULT_BASE_URL`].
    pub base_url: Option<String>,
    /// Overrides the default HTTP client.
    pub http_client: Option<HttpClient>,
    /// Retries after 5xx / network errors. Default 3.
    pub max_retries: Option<u32>,
    /// Exponential backoff base. Default 1s; min [`MIN_RETRY_BASE`].
    pub retry_base: Option<Duration>,
    /// Per-attempt timeout. Default 30s. Ignored if `http_client` is set.
    pub timeout: Option<Duration>,
    /// Caps parallel geocode batch requests. Default and max: [`MAX_CONCURRENCY`].
    pub concurrency: Option<usize>,
}

impl Config {
    pub fn with_api_key(api_key: impl Into<String>) -> Self {
        Self {
            api_key: Some(api_key.into()),
            ..Default::default()
        }
    }

    pub fn with_client_auth(auth: ClientAuth) -> Self {
        Self {
            client_auth: Some(auth),
            ..Default::default()
        }
    }

    pub fn with_access_token(token: impl Into<String>) -> Self {
        Self {
            access_token: Some(token.into()),
            ..Default::default()
        }
    }

    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = Some(n);
        self
    }

    pub fn retry_base(mut self, d: Duration) -> Self {
        self.retry_base = Some(d);
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }
}
