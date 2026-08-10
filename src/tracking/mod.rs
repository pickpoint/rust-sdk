//! Realtime tracking client (binary WebSocket by default, gRPC supported).

mod backoff;
mod client;
mod codec;
mod errors;
mod queue;
mod rate;
mod url;

/// Generated `tracking.v2` protobuf stubs.
pub mod v2 {
    #![allow(missing_docs)]
    include!("v2/tracking.v2.rs");
}

pub use backoff::{new_backoff, next_delay, reset_backoff, BackoffState};
pub use client::{
    connect, Client, Config, ConnectionState, DeviceAuth, ListenerAuth, RefreshAuthFn, Transport,
    MAX_EVENT_BYTES, MAX_EVENT_HZ, MAX_PUBLISH_HZ, MIN_EVENT_INTERVAL, MIN_PUBLISH_INTERVAL,
    SUBPROTOCOL,
};
pub use codec::{client_resume, decode_server_msg, encode_client_msg, stamp_lat_lng};
pub use errors::{is_auth_error, is_fatal_resume_error, new_error, Error};
pub use queue::{OfflineQueue, QueuedPoint};
pub use rate::{can_accept_publish, next_publish_allowed_at, MIN_PUBLISH_INTERVAL_MS};
pub use url::build_ws_url;
