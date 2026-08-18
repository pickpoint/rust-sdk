//! Transport-agnostic commands / events.

use crate::tracking::types::{CommandAckStatus, ErrorCode, LatLng};

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCmd {
    Resume {
        track_uid: String,
        last_client_seq: u64,
    },
    TrackStart {
        location: Option<LatLng>,
        route: Vec<LatLng>,
        metadata: Vec<u8>,
    },
    /// Empty body on the wire. Idle stop is a no-op.
    TrackStop {
        track_uid: String,
    },
    LocationAdd {
        track_uid: String,
        client_seq: u64,
        point: LatLng,
    },
    LocationBatch {
        track_uid: String,
        client_seq: u64,
        points: Vec<LatLng>,
    },
    Subscribe {
        device_uid: String,
        include_events: bool,
        min_location_interval_ms: u32,
    },
    Unsubscribe {
        sub: u8,
    },
    Event {
        track_uid: String,
        payload: Vec<u8>,
        timestamp_ms: Option<i64>,
    },
    CommandAck {
        command_id: String,
        status: CommandAckStatus,
        message: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServerEvt {
    Hello {
        version: u8,
        node_id: String,
        shard: u32,
    },
    Relocate {
        endpoint: String,
        retry_after_ms: u32,
    },
    ResumeOk {
        track_uid: String,
        last_acked_seq: u64,
    },
    TrackStarted {
        track_uid: String,
        metadata: Vec<u8>,
    },
    TrackStopped {
        track_uid: String,
    },
    /// Device ingest receipt. Never fan-out to listeners.
    Ack {
        seq: u64,
    },
    /// Listener live point (`0x86 Loc`).
    LocationAdded {
        device_uid: String,
        track_uid: String,
        point: LatLng,
        client_seq: u64,
        sub: u8,
    },
    Subscribed {
        sub: u8,
        device_uid: String,
        track_uid: String,
        last_location: Option<LatLng>,
        route: Vec<LatLng>,
        estimated_distance: f64,
        estimated_duration: f64,
        start_location_name: String,
        end_location_name: String,
        metadata: Vec<u8>,
        online: bool,
        last_seen_ms: Option<i64>,
    },
    Error {
        code: ErrorCode,
        message: String,
        track_uid: Option<String>,
        retry_after_ms: Option<u32>,
    },
    EventAdded {
        device_uid: String,
        track_uid: String,
        payload: Vec<u8>,
        timestamp_ms: Option<i64>,
        sub: u8,
    },
    Command {
        command_id: String,
        payload: Vec<u8>,
        timestamp_ms: Option<i64>,
    },
    DevicePresence {
        device_uid: String,
        online: bool,
        last_seen_ms: Option<i64>,
        sub: u8,
    },
}

impl ClientCmd {
    pub fn is_resume(&self) -> bool {
        matches!(self, Self::Resume { .. })
    }

    pub fn is_track_start(&self) -> bool {
        matches!(self, Self::TrackStart { .. })
    }

    pub fn is_loc(&self) -> bool {
        matches!(self, Self::LocationAdd { .. } | Self::LocationBatch { .. })
    }
}
