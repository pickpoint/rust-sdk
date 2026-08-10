use std::fmt;

use crate::tracking::v2::{Error as WireError, ErrorCode};

/// Typed tracking protocol / SDK error.
#[derive(Debug, Clone)]
pub struct Error {
    /// Wire error code.
    pub code: ErrorCode,
    /// Human-readable message.
    pub message: String,
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
    }
}

pub(crate) fn error_from_wire(err: Option<&WireError>) -> Error {
    match err {
        Some(e) => {
            let code = ErrorCode::try_from(e.code).unwrap_or(ErrorCode::Invalid);
            new_error(code, e.message.clone())
        }
        None => new_error(ErrorCode::Invalid, "unknown error"),
    }
}

/// Fatal resume errors that clear the local track.
pub fn is_fatal_resume_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::TrackNotFound | ErrorCode::Fenced | ErrorCode::Auth | ErrorCode::Unauthorized
    )
}

/// Auth-related wire codes.
pub fn is_auth_error(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::Auth | ErrorCode::Unauthorized)
}
