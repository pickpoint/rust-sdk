use prost::Message;

use crate::tracking::v2::{client_msg, ClientMsg, LatLng, Resume, ServerMsg};

/// Set `timestamp_ms` to now when omitted.
pub fn stamp_lat_lng(mut p: LatLng) -> LatLng {
    if p.timestamp_ms.is_none() {
        p.timestamp_ms = Some(chrono_now_ms());
    }
    p
}

/// Stamp a slice of points.
pub fn stamp_lat_lngs(points: Vec<LatLng>) -> Vec<LatLng> {
    points.into_iter().map(stamp_lat_lng).collect()
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Marshal a [`ClientMsg`] to binary protobuf.
pub fn encode_client_msg(msg: &ClientMsg) -> Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf)?;
    Ok(buf)
}

/// Unmarshal a [`ServerMsg`] from binary protobuf.
pub fn decode_server_msg(data: &[u8]) -> Result<ServerMsg, prost::DecodeError> {
    ServerMsg::decode(data)
}

/// Build a resume [`ClientMsg`] (for golden / wire tests).
pub fn client_resume(track_uid: impl Into<String>, last_client_seq: u64) -> ClientMsg {
    ClientMsg {
        body: Some(client_msg::Body::Resume(Resume {
            track_uid: track_uid.into(),
            last_client_seq,
        })),
    }
}
