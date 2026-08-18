//! Realtime tracking client (binary WebSocket, `tracking.v2`).

mod backoff;
mod client;
mod cmd;
mod codec;
mod errors;
mod filter;
mod queue;
mod rate;
mod types;
mod url;

pub use backoff::{new_backoff, next_delay, reset_backoff, BackoffState};
pub use client::{
    connect, Client, Config, ConnectionState, DeviceAuth, ListenerAuth, RefreshAuthFn, Transport,
    MAX_EVENT_BYTES, MAX_EVENT_HZ, MAX_PUBLISH_HZ, MIN_EVENT_INTERVAL, MIN_PUBLISH_INTERVAL,
    SUBPROTOCOL,
};
pub use cmd::{ClientCmd, ServerEvt};
pub use codec::{
    client_resume, decode_client_cmd, decode_server_evt, decode_server_msg, encode_client_cmd,
    encode_client_msg, encode_loc_frames, encode_server_evt, stamp_lat_lng, DecodeError,
    DEFAULT_WS_PATH,
};
pub use errors::{is_auth_error, is_fatal_resume_error, is_retry_resume_error, new_error, Error};
pub use filter::NoiseFilter;
pub use queue::{OfflineQueue, QueuedPoint, MAX_UNACKED_FRAMES};
pub use rate::{can_accept_publish, next_publish_allowed_at, MIN_PUBLISH_INTERVAL_MS};
pub use types::{
    Command, CommandAckStatus, ErrorCode, FinishReason, LatLng, Relocate, PROTOCOL_VERSION,
};
pub use url::build_ws_url;
