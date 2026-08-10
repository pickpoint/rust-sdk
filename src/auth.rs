use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::Client as HttpClient;
use tokio::sync::{Mutex, Notify};

use crate::config::{ClientAuth, Config, CLIENT_AUTH_REFRESH_AT};
use crate::error::{ApiError, Error, Result};

#[derive(Clone)]
pub(crate) enum AuthKind {
    ApiKey(String),
    Bearer(Arc<dyn TokenSession>),
}

pub(crate) trait TokenSession: Send + Sync {
    fn token(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + '_>>;
    fn refresh_after_unauthorized(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>>;
}

pub(crate) async fn resolve_auth(
    cfg: &Config,
    base_url: &str,
    http: HttpClient,
) -> Result<AuthKind> {
    let mut n = 0u8;
    if cfg.api_key.as_ref().is_some_and(|s| !s.is_empty()) {
        n += 1;
    }
    if cfg.client_auth.is_some() {
        n += 1;
    }
    if cfg.access_token.as_ref().is_some_and(|s| !s.is_empty()) {
        n += 1;
    }
    if n > 1 {
        return Err(Error::invalid_config(
            "provide only one of: api_key | client_auth | access_token",
        ));
    }
    if n == 0 {
        return Err(Error::invalid_config(
            "auth required: api_key, client_auth, or access_token",
        ));
    }
    if let Some(key) = &cfg.api_key {
        if !key.is_empty() {
            return Ok(AuthKind::ApiKey(key.clone()));
        }
    }
    if let Some(auth) = &cfg.client_auth {
        let session = ClientAuthSession::new(auth.clone(), base_url.to_string(), http)?;
        return Ok(AuthKind::Bearer(Arc::new(session)));
    }
    Ok(AuthKind::Bearer(Arc::new(StaticSession(
        cfg.access_token.clone().unwrap_or_default(),
    ))))
}

impl AuthKind {
    pub(crate) async fn apply(&self, headers: &mut HeaderMap) -> Result<()> {
        headers.insert("Accept", HeaderValue::from_static("application/json"));
        match self {
            AuthKind::ApiKey(key) => {
                headers.insert(
                    "x-api-key",
                    HeaderValue::from_str(key).map_err(|_| {
                        Error::invalid_config("api_key contains invalid header characters")
                    })?,
                );
            }
            AuthKind::Bearer(session) => {
                let tok = session.token().await?;
                let value = format!("Bearer {tok}");
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&value).map_err(|_| {
                        Error::api(0, "INVALID_TOKEN", "access token is not a valid header", [])
                    })?,
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn refresh_after_unauthorized(&self) -> bool {
        match self {
            AuthKind::ApiKey(_) => false,
            AuthKind::Bearer(session) => session.refresh_after_unauthorized().await,
        }
    }

    pub(crate) fn is_bearer(&self) -> bool {
        matches!(self, AuthKind::Bearer(_))
    }
}

struct StaticSession(String);

impl TokenSession for StaticSession {
    fn token(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + '_>> {
        Box::pin(async { Ok(self.0.clone()) })
    }

    fn refresh_after_unauthorized(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async { false })
    }
}

struct ClientAuthSession {
    inner: Mutex<ClientAuthInner>,
    notify: Notify,
    base_url: String,
    http: HttpClient,
}

struct ClientAuthInner {
    access_token: String,
    refresh_token: String,
    expires_at: i64,
    issued_at: Instant,
    refreshing: bool,
}

impl ClientAuthSession {
    fn new(initial: ClientAuth, base_url: String, http: HttpClient) -> Result<Self> {
        if initial.access_token.is_empty()
            || initial.refresh_token.is_empty()
            || initial.expires_at == 0
        {
            return Err(Error::invalid_config(
                "client_auth requires access_token, refresh_token, and expires_at (unix ms)",
            ));
        }
        Ok(Self {
            inner: Mutex::new(ClientAuthInner {
                access_token: initial.access_token,
                refresh_token: initial.refresh_token,
                expires_at: initial.expires_at,
                issued_at: Instant::now(),
                refreshing: false,
            }),
            notify: Notify::new(),
            base_url,
            http,
        })
    }

    fn needs_proactive_refresh(inner: &ClientAuthInner) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let issued_ms = now_ms
            - Instant::now()
                .saturating_duration_since(inner.issued_at)
                .as_millis() as i64;
        let ttl_ms = inner.expires_at - issued_ms;
        if ttl_ms <= 0 {
            return now_ms >= inner.expires_at - 30_000;
        }
        let refresh_after = Duration::from_millis((ttl_ms as f64 * CLIENT_AUTH_REFRESH_AT) as u64);
        Instant::now() >= inner.issued_at + refresh_after
    }

    async fn refresh(&self) -> Result<()> {
        loop {
            let mut guard = self.inner.lock().await;
            if !guard.refreshing {
                guard.refreshing = true;
                let refresh_tok = guard.refresh_token.clone();
                drop(guard);
                let result = self.do_refresh(&refresh_tok).await;
                {
                    let mut guard = self.inner.lock().await;
                    guard.refreshing = false;
                }
                self.notify.notify_waiters();
                return result;
            }
            drop(guard);
            self.notify.notified().await;
            let guard = self.inner.lock().await;
            if !guard.refreshing {
                return Ok(());
            }
        }
    }

    async fn do_refresh(&self, refresh_tok: &str) -> Result<()> {
        let body = serde_json::json!({ "refreshToken": refresh_tok });
        let res = self
            .http
            .post(format!("{}/v2/client-tokens/refresh", self.base_url))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                Error::Api(ApiError::new(
                    0,
                    "REFRESH_FAILED",
                    format!("client token refresh network error: {e}"),
                    Vec::new(),
                ))
            })?;
        let status = res.status().as_u16();
        let raw = res.bytes().await.unwrap_or_default().to_vec();
        if !(200..300).contains(&status) {
            return Err(Error::Api(ApiError::new(
                status,
                "REFRESH_FAILED",
                format!("client token refresh failed ({status})"),
                raw,
            )));
        }
        let pair: ClientAuth = serde_json::from_slice(&raw).map_err(|e| {
            Error::Api(ApiError::new(
                0,
                "INVALID_TOKEN",
                format!("refresh returned invalid JSON: {e}"),
                raw.clone(),
            ))
        })?;
        if pair.access_token.is_empty() || pair.refresh_token.is_empty() || pair.expires_at == 0 {
            return Err(Error::Api(ApiError::new(
                0,
                "INVALID_TOKEN",
                "refresh returned invalid clientAuth pair",
                raw,
            )));
        }
        let mut guard = self.inner.lock().await;
        guard.access_token = pair.access_token;
        guard.refresh_token = pair.refresh_token;
        guard.expires_at = pair.expires_at;
        guard.issued_at = Instant::now();
        Ok(())
    }
}

impl TokenSession for ClientAuthSession {
    fn token(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + '_>> {
        Box::pin(async {
            {
                let guard = self.inner.lock().await;
                if Self::needs_proactive_refresh(&guard) {
                    drop(guard);
                    self.refresh().await?;
                }
            }
            let guard = self.inner.lock().await;
            Ok(guard.access_token.clone())
        })
    }

    fn refresh_after_unauthorized(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async { self.refresh().await.is_ok() })
    }
}
