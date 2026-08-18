//! Native types for the tracking session plane.

/// Protocol version in `Hello.version` and WS subprotocol `tracking.v2`.
pub const PROTOCOL_VERSION: u8 = 2;

/// GPS sample. Heading and speed are filter-only and never appear on the wire.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatLng {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: Option<f64>,
    pub accuracy: Option<f64>,
    /// Local GPS heading (degrees). Filter only; not on the wire.
    pub heading: Option<f64>,
    /// Local speed (m/s). Filter only; not on the wire.
    pub speed: Option<f64>,
    pub timestamp_ms: Option<i64>,
}

impl LatLng {
    pub fn new(latitude: f64, longitude: f64) -> Self {
        Self {
            latitude,
            longitude,
            altitude: None,
            accuracy: None,
            heading: None,
            speed: None,
            timestamp_ms: None,
        }
    }
}

/// Wire `Error.code` (u8). 0 is unused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    Auth = 1,
    TrackNotFound = 2,
    Fenced = 3,
    TryAgain = 4,
    Invalid = 5,
    Unauthorized = 6,
}

impl ErrorCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Self::Auth,
            2 => Self::TrackNotFound,
            3 => Self::Fenced,
            4 => Self::TryAgain,
            5 => Self::Invalid,
            6 => Self::Unauthorized,
            _ => return None,
        })
    }
}

/// Wire `CommandAck.status` (u8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandAckStatus {
    Unspecified = 0,
    Ok = 1,
    Rejected = 2,
    Failed = 3,
}

impl CommandAckStatus {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Ok,
            2 => Self::Rejected,
            3 => Self::Failed,
            _ => Self::Unspecified,
        }
    }
}

/// Why a track row was closed. HTTP only — not on the WS frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishReason {
    ClientStop,
    Superseded,
    Idle,
}

impl FinishReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClientStop => "client_stop",
            Self::Superseded => "superseded",
            Self::Idle => "idle",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "client_stop" => Some(Self::ClientStop),
            "superseded" => Some(Self::Superseded),
            "idle" => Some(Self::Idle),
            _ => None,
        }
    }
}

/// Relocate payload (also used by the test mock).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Relocate {
    pub endpoint: String,
    pub retry_after_ms: u32,
}

/// Device command inject (`0x8A`).
#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    pub command_id: String,
    pub payload: Vec<u8>,
    pub timestamp_ms: Option<i64>,
}
