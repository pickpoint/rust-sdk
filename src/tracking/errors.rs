use std::fmt;

use crate::tracking::cmd::ServerEvt;
use crate::tracking::types::ErrorCode;

/// Typed tracking protocol / SDK error.
#[derive(Debug, Clone)]
pub struct Error {
    /// Wire error code.
    pub code: ErrorCode,
    /// Human-readable message.
    pub message: String,
    pub track_uid: Option<String>,
    pub retry_after_ms: Option<u32>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "tracking: {:?}", self.code)
        } else {
            write!(f, "tracking: {} ({:?})", self.message, self.code)
        }
    }
}

impl std::error::Error for Error {}

/// Build an [`Error`] from a wire code + message.
pub fn new_error(code: ErrorCode, message: impl Into<String>) -> Error {
    Error {
        code,
        message: message.into(),
        track_uid: None,
        retry_after_ms: None,
    }
}

pub(crate) fn error_from_evt(evt: &ServerEvt) -> Error {
    match evt {
        ServerEvt::Error {
            code,
            message,
            track_uid,
            retry_after_ms,
        } => Error {
            code: *code,
            message: message.clone(),
            track_uid: track_uid.clone(),
            retry_after_ms: *retry_after_ms,
        },
        _ => new_error(ErrorCode::Invalid, "unknown error"),
    }
}

/// Fatal resume errors that clear the local track.
/// FENCED / TRY_AGAIN retry Resume. UNAUTHORIZED is a role error.
pub fn is_fatal_resume_error(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::TrackNotFound | ErrorCode::Auth)
}

/// Retry Resume (do not TrackStart).
pub fn is_retry_resume_error(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::Fenced | ErrorCode::TryAgain)
}

/// Auth-related wire codes.
pub fn is_auth_error(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::Auth | ErrorCode::Unauthorized)
}
