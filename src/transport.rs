use std::time::Duration;

use rand::Rng;
use reqwest::header::{HeaderMap, CONTENT_TYPE};
use reqwest::Client as HttpClient;
use serde_json::Value;

use crate::auth::AuthKind;
use crate::config::{DEFAULT_RETRY_BASE, MIN_RETRY_BASE};
use crate::error::{ApiError, Error, Result};

#[derive(Clone, Copy)]
pub(crate) enum OnClientError {
    Throw,
    Empty,
}

pub(crate) struct RequestOpts<'a> {
    pub method: reqwest::Method,
    pub path: &'a str,
    pub query: Vec<(String, String)>,
    pub body: Option<Value>,
    pub on_client_error: OnClientError,
    pub empty: Option<&'static [u8]>,
}

pub(crate) struct Transport {
    pub base_url: String,
    pub http: HttpClient,
    pub auth: AuthKind,
    pub max_retries: u32,
    pub retry_base: Duration,
}

impl Transport {
    pub async fn do_request(&self, opts: RequestOpts<'_>) -> Result<Vec<u8>> {
        let mut attempt = 0u32;
        let mut auth_retried = false;

        loop {
            let mut url = format!("{}{}", self.base_url, opts.path);
            if !opts.query.is_empty() {
                let mut ser = url::form_urlencoded::Serializer::new(String::new());
                for (k, v) in &opts.query {
                    if !v.is_empty() {
                        ser.append_pair(k, v);
                    }
                }
                let q = ser.finish();
                if !q.is_empty() {
                    url.push('?');
                    url.push_str(&q);
                }
            }

            let mut headers = HeaderMap::new();
            self.auth.apply(&mut headers).await?;
            if opts.body.is_some() {
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
            }

            let mut builder = self
                .http
                .request(opts.method.clone(), &url)
                .headers(headers);
            if let Some(body) = &opts.body {
                builder = builder.json(body);
            }

            let send_result = builder.send().await;
            let res = match send_result {
                Ok(r) => r,
                Err(e) => {
                    if attempt >= self.max_retries {
                        return Err(Error::Api(ApiError::new(
                            0,
                            "NETWORK",
                            format!("network error: {e}"),
                            Vec::new(),
                        )));
                    }
                    sleep_backoff(self.retry_base, attempt).await;
                    attempt += 1;
                    continue;
                }
            };

            let status = res.status().as_u16();
            let raw = res.bytes().await.unwrap_or_default().to_vec();

            match status {
                401 => {
                    if !auth_retried
                        && self.auth.is_bearer()
                        && self.auth.refresh_after_unauthorized().await
                    {
                        auth_retried = true;
                        continue;
                    }
                    return Err(Error::Api(ApiError::new(
                        status,
                        "API_AUTH",
                        "auth failed (401)",
                        raw,
                    )));
                }
                402 | 403 => {
                    return Err(Error::Api(ApiError::new(
                        status,
                        "API_AUTH",
                        "auth failed",
                        raw,
                    )));
                }
                204 => return Ok(Vec::new()),
                409 => {
                    return Err(Error::Api(ApiError::new(
                        409,
                        "CONFLICT",
                        message_from_body(&raw, 409),
                        raw,
                    )));
                }
                400 | 404..=499 => {
                    if matches!(opts.on_client_error, OnClientError::Empty) {
                        return Ok(opts.empty.unwrap_or_default().to_vec());
                    }
                    let code = if status == 404 {
                        "NOT_FOUND"
                    } else {
                        "CLIENT_ERROR"
                    };
                    return Err(Error::Api(ApiError::new(
                        status,
                        code,
                        message_from_body(&raw, status),
                        raw,
                    )));
                }
                500..=599 => {
                    if attempt >= self.max_retries {
                        return Err(Error::Api(ApiError::new(
                            status,
                            "SERVER_ERROR",
                            "server error after retries",
                            raw,
                        )));
                    }
                    sleep_backoff(self.retry_base, attempt).await;
                    attempt += 1;
                    continue;
                }
                200..=299 => return Ok(raw),
                _ => {
                    if (400..500).contains(&status)
                        && matches!(opts.on_client_error, OnClientError::Empty)
                    {
                        return Ok(opts.empty.unwrap_or_default().to_vec());
                    }
                    return Err(Error::Api(ApiError::new(
                        status,
                        "CLIENT_ERROR",
                        message_from_body(&raw, status),
                        raw,
                    )));
                }
            }
        }
    }
}

fn message_from_body(raw: &[u8], status: u16) -> String {
    #[derive(serde::Deserialize)]
    struct Msg {
        message: Option<String>,
        error: Option<String>,
    }
    if let Ok(m) = serde_json::from_slice::<Msg>(raw) {
        if let Some(message) = m.message.filter(|s| !s.is_empty()) {
            return message;
        }
        if let Some(error) = m.error.filter(|s| !s.is_empty()) {
            return error;
        }
    }
    http_status_text(status)
}

fn http_status_text(status: u16) -> String {
    http::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("unknown")
        .to_string()
}

async fn sleep_backoff(base: Duration, attempt: u32) {
    let base = if base.is_zero() {
        DEFAULT_RETRY_BASE
    } else {
        base.max(MIN_RETRY_BASE)
    };
    let max = base
        .checked_mul(1u32 << attempt.min(16))
        .unwrap_or(Duration::from_secs(60));
    let jitter = rand::thread_rng().gen_range(0..=max.as_millis() as u64);
    tokio::time::sleep(Duration::from_millis(jitter)).await;
}

pub(crate) fn trim_slash(s: &str) -> String {
    s.trim_end_matches('/').to_string()
}
