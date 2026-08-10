use std::fmt;

/// Auth failed (401 / 402 / 403 / refresh failed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthError;

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("pickpoint: auth failed")
    }
}

impl std::error::Error for AuthError {}

/// Resource not found (404).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotFoundError;

impl fmt::Display for NotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("pickpoint: not found")
    }
}

impl std::error::Error for NotFoundError {}

/// Conflict (409), e.g. device offline for command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictError;

impl fmt::Display for ConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("pickpoint: conflict")
    }
}

impl std::error::Error for ConflictError {}

/// Invalid client configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidConfigError;

impl fmt::Display for InvalidConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("pickpoint: invalid config")
    }
}

impl std::error::Error for InvalidConfigError {}

/// Non-2xx public-api response (or transport failure after retries).
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub body: Vec<u8>,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(
                f,
                "pickpoint: request failed (status={} code={})",
                self.status, self.code
            )
        } else {
            write!(
                f,
                "pickpoint: {} (status={} code={})",
                self.message, self.status, self.code
            )
        }
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    pub fn new(
        status: u16,
        code: impl Into<String>,
        message: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            body: body.into(),
        }
    }

    pub fn is_auth(&self) -> bool {
        matches!(self.code.as_str(), "API_AUTH" | "REFRESH_FAILED")
    }

    pub fn is_not_found(&self) -> bool {
        self.code == "NOT_FOUND"
    }

    pub fn is_conflict(&self) -> bool {
        self.code == "CONFLICT"
    }

    pub fn is_invalid_config(&self) -> bool {
        self.code == "INVALID_CONFIG"
    }
}

/// SDK error type for the HTTP public API.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Api(#[from] ApiError),

    #[error(transparent)]
    Auth(#[from] AuthError),

    #[error(transparent)]
    NotFound(#[from] NotFoundError),

    #[error(transparent)]
    Conflict(#[from] ConflictError),

    #[error(transparent)]
    InvalidConfig(#[from] InvalidConfigError),

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("pickpoint: invalid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("pickpoint: batch cancelled")]
    BatchCancelled,
}

impl Error {
    pub fn api(
        status: u16,
        code: impl Into<String>,
        message: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        Self::Api(ApiError::new(status, code, message, body))
    }

    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::Api(ApiError::new(0, "INVALID_CONFIG", message, Vec::new()))
    }

    /// Returns true if this is an auth failure (including wrapped [`ApiError`]).
    pub fn is_auth(&self) -> bool {
        match self {
            Self::Auth(_) => true,
            Self::Api(e) => e.is_auth(),
            _ => false,
        }
    }

    pub fn is_not_found(&self) -> bool {
        match self {
            Self::NotFound(_) => true,
            Self::Api(e) => e.is_not_found(),
            _ => false,
        }
    }

    pub fn is_conflict(&self) -> bool {
        match self {
            Self::Conflict(_) => true,
            Self::Api(e) => e.is_conflict(),
            _ => false,
        }
    }

    pub fn is_invalid_config(&self) -> bool {
        match self {
            Self::InvalidConfig(_) => true,
            Self::Api(e) => e.is_invalid_config(),
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
