//! Official Rust SDK for [Pickpoint](https://pickpoint.io).
//!
//! - HTTP public API: geocoding, address search, routing, devices, client-tokens
//! - Realtime tracking: [`tracking`] (WebSocket by default, gRPC supported)

#![allow(missing_docs)]

mod address;
mod auth;
mod client;
mod config;
mod devices;
mod error;
mod geocoding;
mod mint;
mod routing;
mod transport;

pub mod tracking;

pub use client::Client;
pub use config::{
    ClientAuth, Config, DEFAULT_BASE_URL, DEFAULT_CONCURRENCY, DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BASE, DEFAULT_TIMEOUT, MAX_CONCURRENCY, MIN_RETRY_BASE,
};
pub use devices::{Device, DeviceCommandResult, DeviceInput, DeviceListQuery, DeviceListResult};
pub use error::{
    ApiError, AuthError, ConflictError, Error, InvalidConfigError, NotFoundError, Result,
};
pub use geocoding::{query, Query};
pub use mint::{mint_client_tokens, TokenPair};

pub use address::AddressApi;
pub use devices::DevicesApi;
pub use geocoding::GeocodingApi;
pub use routing::RoutingApi;
