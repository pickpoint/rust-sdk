//! Binary `tracking.v2` codec (little-endian). One WS binary frame = one message.
//! Logic copied from the tracking-service wire crate; this crate does not depend on it.

use uuid::Uuid;

use crate::tracking::cmd::{ClientCmd, ServerEvt};
use crate::tracking::types::{CommandAckStatus, ErrorCode, LatLng, PROTOCOL_VERSION};

pub const SUBPROTOCOL: &str = "tracking.v2";
pub const DEFAULT_WS_PATH: &str = "/v2/ws";

pub const MAX_STRING: usize = 4096;
pub const MAX_LOC_POINTS: u8 = 100;

pub const C_RESUME: u8 = 0x01;
pub const C_TRACK_START: u8 = 0x02;
pub const C_TRACK_STOP: u8 = 0x03;
pub const C_LOC: u8 = 0x04;
pub const C_SUBSCRIBE: u8 = 0x05;
pub const C_UNSUBSCRIBE: u8 = 0x06;
pub const C_EVENT: u8 = 0x07;
pub const C_COMMAND_ACK: u8 = 0x08;

pub const S_HELLO: u8 = 0x80;
pub const S_RELOCATE: u8 = 0x81;
pub const S_RESUME_OK: u8 = 0x82;
pub const S_TRACK_STARTED: u8 = 0x83;
pub const S_TRACK_STOPPED: u8 = 0x84;
pub const S_ACK: u8 = 0x85;
pub const S_LOC: u8 = 0x86;
pub const S_SUBSCRIBED: u8 = 0x87;
pub const S_ERROR: u8 = 0x88;
pub const S_EVENT_ADDED: u8 = 0x89;
pub const S_COMMAND: u8 = 0x8A;
pub const S_PRESENCE: u8 = 0x8B;

const PF_ALT: u8 = 1 << 0;
const PF_ACC: u8 = 1 << 1;
const PF_TIME: u8 = 1 << 4;

const LAT_MIN: i32 = -90_000_000;
const LAT_MAX: i32 = 90_000_000;
const LON_MIN: i32 = -180_000_000;
const LON_MAX: i32 = 180_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("truncated frame")]
    Truncated,
    #[error("invalid frame")]
    Invalid,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("empty loc")]
    Empty,
    #[error("intra-frame delta overflows i16")]
    DeltaOverflow,
}

#[allow(dead_code)]
pub enum ClientDecode {
    Cmd(ClientCmd),
    /// Unknown client type: session stays up. The SDK never sends these.
    Unknown(u8),
}

struct R<'a>(&'a [u8]);

impl<'a> R<'a> {
    fn need(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.need(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.need(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }

    fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_le_bytes(self.need(2)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.need(8)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.need(8)?.try_into().unwrap()))
    }

    fn uuid(&mut self) -> Result<String, DecodeError> {
        let b: [u8; 16] = self.need(16)?.try_into().unwrap();
        Ok(Uuid::from_bytes(b).to_string())
    }

    fn uuid_opt(&mut self) -> Result<Option<String>, DecodeError> {
        let b: [u8; 16] = self.need(16)?.try_into().unwrap();
        if b.iter().all(|x| *x == 0) {
            Ok(None)
        } else {
            Ok(Some(Uuid::from_bytes(b).to_string()))
        }
    }

    fn str(&mut self) -> Result<String, DecodeError> {
        let n = self.u16()? as usize;
        if n > MAX_STRING {
            return Err(DecodeError::Invalid);
        }
        let raw = self.need(n)?;
        String::from_utf8(raw.to_vec()).map_err(|_| DecodeError::Invalid)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let n = self.u16()? as usize;
        if n > MAX_STRING {
            return Err(DecodeError::Invalid);
        }
        Ok(self.need(n)?.to_vec())
    }
}

fn put_u8(w: &mut Vec<u8>, v: u8) {
    w.push(v);
}
fn put_u16(w: &mut Vec<u8>, v: u16) {
    w.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(w: &mut Vec<u8>, v: u32) {
    w.extend_from_slice(&v.to_le_bytes());
}
fn put_i16(w: &mut Vec<u8>, v: i16) {
    w.extend_from_slice(&v.to_le_bytes());
}
fn put_i32(w: &mut Vec<u8>, v: i32) {
    w.extend_from_slice(&v.to_le_bytes());
}
fn put_i64(w: &mut Vec<u8>, v: i64) {
    w.extend_from_slice(&v.to_le_bytes());
}
fn put_f64(w: &mut Vec<u8>, v: f64) {
    w.extend_from_slice(&v.to_le_bytes());
}

fn uid_bytes(s: &str) -> [u8; 16] {
    if s.is_empty() {
        return [0; 16];
    }
    match Uuid::parse_str(s) {
        Ok(u) => *u.as_bytes(),
        Err(_) => [0; 16],
    }
}

fn put_uuid(w: &mut Vec<u8>, s: &str) {
    w.extend_from_slice(&uid_bytes(s));
}

fn put_str(w: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let n = bytes.len().min(MAX_STRING);
    put_u16(w, n as u16);
    w.extend_from_slice(&bytes[..n]);
}

fn put_bytes(w: &mut Vec<u8>, b: &[u8]) {
    let n = b.len().min(MAX_STRING);
    put_u16(w, n as u16);
    w.extend_from_slice(&b[..n]);
}

pub fn deg_to_micro(d: f64) -> i32 {
    (d * 1_000_000.0).round() as i32
}

pub fn micro_to_deg(m: i32) -> f64 {
    m as f64 / 1_000_000.0
}

fn check_coord(lat: i32, lon: i32) -> Result<(), DecodeError> {
    if !(LAT_MIN..=LAT_MAX).contains(&lat) || !(LON_MIN..=LON_MAX).contains(&lon) {
        return Err(DecodeError::Invalid);
    }
    Ok(())
}

/// True when `next - prev` fits in an i16 microdegree delta.
pub fn micro_delta_fits(prev_lat: i32, prev_lon: i32, lat: i32, lon: i32) -> bool {
    let dlat = lat as i64 - prev_lat as i64;
    let dlon = lon as i64 - prev_lon as i64;
    dlat >= i16::MIN as i64
        && dlat <= i16::MAX as i64
        && dlon >= i16::MIN as i64
        && dlon <= i16::MAX as i64
}

fn write_point(
    w: &mut Vec<u8>,
    p: &LatLng,
    prev: Option<(i32, i32)>,
) -> Result<(i32, i32), EncodeError> {
    let lat = deg_to_micro(p.latitude);
    let lon = deg_to_micro(p.longitude);
    let mut flags = 0u8;
    if p.altitude.is_some() {
        flags |= PF_ALT;
    }
    if p.accuracy.is_some() {
        flags |= PF_ACC;
    }
    if p.timestamp_ms.is_some() {
        flags |= PF_TIME;
    }
    put_u8(w, flags);
    if let Some((plat, plon)) = prev {
        if !micro_delta_fits(plat, plon, lat, lon) {
            return Err(EncodeError::DeltaOverflow);
        }
        put_i16(w, (lat - plat) as i16);
        put_i16(w, (lon - plon) as i16);
    } else {
        put_i32(w, lat);
        put_i32(w, lon);
    }
    if let Some(alt) = p.altitude {
        put_i32(w, (alt * 1000.0).round() as i32);
    }
    if let Some(acc) = p.accuracy {
        let cm = (acc * 100.0).round().clamp(0.0, u16::MAX as f64) as u16;
        put_u16(w, cm);
    }
    if let Some(t) = p.timestamp_ms {
        put_i64(w, t);
    }
    Ok((lat, lon))
}

fn write_abs(w: &mut Vec<u8>, p: &LatLng) {
    write_point(w, p, None).expect("absolute point cannot overflow i16 delta");
}

/// Split `points` into Loc frames (count 1…100, i16 Δ must fit).
/// `last_seq` is the seq of the **last** point (same as the Loc seq field).
pub fn encode_loc_frames(last_seq: u64, points: &[LatLng]) -> Vec<Vec<u8>> {
    if points.is_empty() {
        return Vec::new();
    }
    let n = points.len() as u64;
    let first_seq = last_seq + 1 - n;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < points.len() {
        let start = i;
        let mut prev = (
            deg_to_micro(points[i].latitude),
            deg_to_micro(points[i].longitude),
        );
        i += 1;
        while i < points.len() && (i - start) < MAX_LOC_POINTS as usize {
            let lat = deg_to_micro(points[i].latitude);
            let lon = deg_to_micro(points[i].longitude);
            if !micro_delta_fits(prev.0, prev.1, lat, lon) {
                break;
            }
            prev = (lat, lon);
            i += 1;
        }
        let chunk = &points[start..i];
        let seq = first_seq + i as u64 - 1;
        out.push(encode_loc_frame(seq, chunk));
    }
    out
}

fn encode_loc_frame(seq: u64, points: &[LatLng]) -> Vec<u8> {
    let mut w = Vec::new();
    put_u8(&mut w, C_LOC);
    put_u32(&mut w, seq as u32);
    put_u8(&mut w, points.len() as u8);
    let mut prev = None;
    for p in points {
        prev = Some(write_point(&mut w, p, prev).expect("chunk already checked"));
    }
    w
}

fn read_point(r: &mut R<'_>, prev: Option<(i32, i32)>) -> Result<(LatLng, i32, i32), DecodeError> {
    let flags = r.u8()?;
    let (lat, lon) = if let Some((plat, plon)) = prev {
        let lat = plat.saturating_add(r.i16()? as i32);
        let lon = plon.saturating_add(r.i16()? as i32);
        (lat, lon)
    } else {
        (r.i32()?, r.i32()?)
    };
    check_coord(lat, lon)?;
    let mut p = LatLng::new(micro_to_deg(lat), micro_to_deg(lon));
    if flags & PF_ALT != 0 {
        p.altitude = Some(r.i32()? as f64 / 1000.0);
    }
    if flags & PF_ACC != 0 {
        p.accuracy = Some(r.u16()? as f64 / 100.0);
    }
    if flags & PF_TIME != 0 {
        p.timestamp_ms = Some(r.i64()?);
    }
    Ok((p, lat, lon))
}

fn write_route_abs(w: &mut Vec<u8>, route: &[LatLng]) {
    put_u16(w, route.len().min(u16::MAX as usize) as u16);
    for p in route.iter().take(u16::MAX as usize) {
        put_i32(w, deg_to_micro(p.latitude));
        put_i32(w, deg_to_micro(p.longitude));
    }
}

fn read_route_abs(r: &mut R<'_>) -> Result<Vec<LatLng>, DecodeError> {
    let n = r.u16()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let lat = r.i32()?;
        let lon = r.i32()?;
        check_coord(lat, lon)?;
        out.push(LatLng::new(micro_to_deg(lat), micro_to_deg(lon)));
    }
    Ok(out)
}

pub fn decode_client(bytes: &[u8]) -> Result<ClientDecode, DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::Truncated);
    }
    let mut r = R(bytes);
    let typ = r.u8()?;
    let cmd = match typ {
        C_RESUME => ClientCmd::Resume {
            track_uid: r.uuid()?,
            last_client_seq: r.u32()? as u64,
        },
        C_TRACK_START => {
            let flags = r.u8()?;
            let location = if flags & 1 != 0 {
                Some(read_point(&mut r, None)?.0)
            } else {
                None
            };
            let route = read_route_abs(&mut r)?;
            let metadata = r.bytes()?;
            ClientCmd::TrackStart {
                location,
                route,
                metadata,
            }
        }
        C_TRACK_STOP => ClientCmd::TrackStop {
            track_uid: String::new(),
        },
        C_LOC => {
            let seq = r.u32()? as u64;
            let count = r.u8()?;
            if count == 0 || count > MAX_LOC_POINTS {
                return Err(DecodeError::Invalid);
            }
            let mut points = Vec::with_capacity(count as usize);
            let mut prev = None;
            for _ in 0..count {
                let (p, lat, lon) = read_point(&mut r, prev)?;
                points.push(p);
                prev = Some((lat, lon));
            }
            if points.len() == 1 {
                ClientCmd::LocationAdd {
                    track_uid: String::new(),
                    client_seq: seq,
                    point: points.pop().unwrap(),
                }
            } else {
                ClientCmd::LocationBatch {
                    track_uid: String::new(),
                    client_seq: seq,
                    points,
                }
            }
        }
        C_SUBSCRIBE => {
            let device_uid = r.uuid()?;
            let flags = r.u8()?;
            let min_interval = r.u16()? as u32;
            ClientCmd::Subscribe {
                device_uid,
                include_events: flags & 1 != 0,
                min_location_interval_ms: min_interval,
            }
        }
        C_UNSUBSCRIBE => ClientCmd::Unsubscribe { sub: r.u8()? },
        C_EVENT => {
            let payload = r.bytes()?;
            let timestamp_ms = r.i64()?;
            ClientCmd::Event {
                track_uid: String::new(),
                payload,
                timestamp_ms: if timestamp_ms == 0 {
                    None
                } else {
                    Some(timestamp_ms)
                },
            }
        }
        C_COMMAND_ACK => {
            let command_id = r.uuid()?;
            let status = CommandAckStatus::from_u8(r.u8()?);
            let message = r.str()?;
            ClientCmd::CommandAck {
                command_id,
                status,
                message: if message.is_empty() {
                    None
                } else {
                    Some(message)
                },
            }
        }
        0x00 | 0x7F | 0xFF => return Err(DecodeError::Invalid),
        t if (0x01..=0x7E).contains(&t) => return Ok(ClientDecode::Unknown(t)),
        _ => return Err(DecodeError::Invalid),
    };
    Ok(ClientDecode::Cmd(cmd))
}

pub fn decode_client_cmd(bytes: &[u8]) -> Result<ClientCmd, DecodeError> {
    match decode_client(bytes)? {
        ClientDecode::Cmd(c) => Ok(c),
        ClientDecode::Unknown(_) => Err(DecodeError::Invalid),
    }
}

pub fn encode_client_cmd(cmd: &ClientCmd) -> Vec<u8> {
    let mut w = Vec::new();
    match cmd {
        ClientCmd::Resume {
            track_uid,
            last_client_seq,
        } => {
            put_u8(&mut w, C_RESUME);
            put_uuid(&mut w, track_uid);
            put_u32(&mut w, *last_client_seq as u32);
        }
        ClientCmd::TrackStart {
            location,
            route,
            metadata,
        } => {
            put_u8(&mut w, C_TRACK_START);
            let mut flags = 0u8;
            if location.is_some() {
                flags |= 1;
            }
            put_u8(&mut w, flags);
            if let Some(p) = location {
                write_abs(&mut w, p);
            }
            write_route_abs(&mut w, route);
            put_bytes(&mut w, metadata);
        }
        ClientCmd::TrackStop { .. } => {
            put_u8(&mut w, C_TRACK_STOP);
        }
        ClientCmd::LocationAdd {
            client_seq, point, ..
        } => {
            put_u8(&mut w, C_LOC);
            put_u32(&mut w, *client_seq as u32);
            put_u8(&mut w, 1);
            write_abs(&mut w, point);
        }
        ClientCmd::LocationBatch {
            client_seq, points, ..
        } => {
            let frames = encode_loc_frames(*client_seq, points);
            if let Some(first) = frames.into_iter().next() {
                w = first;
            }
        }
        ClientCmd::Subscribe {
            device_uid,
            include_events,
            min_location_interval_ms,
        } => {
            put_u8(&mut w, C_SUBSCRIBE);
            put_uuid(&mut w, device_uid);
            put_u8(&mut w, u8::from(*include_events));
            put_u16(
                &mut w,
                (*min_location_interval_ms).min(u16::MAX as u32) as u16,
            );
        }
        ClientCmd::Unsubscribe { sub } => {
            put_u8(&mut w, C_UNSUBSCRIBE);
            put_u8(&mut w, *sub);
        }
        ClientCmd::Event {
            payload,
            timestamp_ms,
            ..
        } => {
            put_u8(&mut w, C_EVENT);
            put_bytes(&mut w, payload);
            put_i64(&mut w, timestamp_ms.unwrap_or(0));
        }
        ClientCmd::CommandAck {
            command_id,
            status,
            message,
        } => {
            put_u8(&mut w, C_COMMAND_ACK);
            put_uuid(&mut w, command_id);
            put_u8(&mut w, *status as u8);
            put_str(&mut w, message.as_deref().unwrap_or(""));
        }
    }
    w
}

/// Marshal a client command (same bytes as [`encode_client_cmd`]).
pub fn encode_client_msg(cmd: &ClientCmd) -> Vec<u8> {
    encode_client_cmd(cmd)
}

pub fn encode_server_evt(evt: &ServerEvt) -> Vec<u8> {
    let mut w = Vec::new();
    match evt {
        ServerEvt::Hello {
            version,
            node_id,
            shard,
        } => {
            put_u8(&mut w, S_HELLO);
            put_u8(&mut w, *version);
            put_u16(&mut w, *shard as u16);
            put_uuid(&mut w, node_id);
        }
        ServerEvt::Relocate {
            endpoint,
            retry_after_ms,
        } => {
            put_u8(&mut w, S_RELOCATE);
            put_u32(&mut w, *retry_after_ms);
            put_str(&mut w, endpoint);
        }
        ServerEvt::ResumeOk {
            track_uid,
            last_acked_seq,
        } => {
            put_u8(&mut w, S_RESUME_OK);
            put_uuid(&mut w, track_uid);
            put_u32(&mut w, *last_acked_seq as u32);
        }
        ServerEvt::TrackStarted {
            track_uid,
            metadata,
        } => {
            put_u8(&mut w, S_TRACK_STARTED);
            put_uuid(&mut w, track_uid);
            put_bytes(&mut w, metadata);
        }
        ServerEvt::TrackStopped { track_uid } => {
            put_u8(&mut w, S_TRACK_STOPPED);
            put_uuid(&mut w, track_uid);
        }
        ServerEvt::Ack { seq } => {
            put_u8(&mut w, S_ACK);
            put_u32(&mut w, *seq as u32);
        }
        ServerEvt::LocationAdded {
            point,
            client_seq,
            sub,
            ..
        } => {
            put_u8(&mut w, S_LOC);
            put_u8(&mut w, *sub);
            put_u32(&mut w, *client_seq as u32);
            write_abs(&mut w, point);
        }
        ServerEvt::Subscribed {
            sub,
            device_uid,
            track_uid,
            last_location,
            route,
            estimated_distance,
            estimated_duration,
            start_location_name,
            end_location_name,
            metadata,
            online,
            last_seen_ms,
        } => {
            put_u8(&mut w, S_SUBSCRIBED);
            put_u8(&mut w, *sub);
            put_uuid(&mut w, device_uid);
            put_uuid(&mut w, track_uid);
            put_u8(&mut w, u8::from(*online));
            let mut flags = 0u8;
            if last_location.is_some() {
                flags |= 1;
            }
            if last_seen_ms.is_some() {
                flags |= 2;
            }
            if !route.is_empty() {
                flags |= 4;
            }
            put_u8(&mut w, flags);
            if let Some(p) = last_location {
                write_abs(&mut w, p);
            }
            if let Some(t) = last_seen_ms {
                put_i64(&mut w, *t);
            }
            if flags & 4 != 0 {
                write_route_abs(&mut w, route);
            }
            put_f64(&mut w, *estimated_distance);
            put_f64(&mut w, *estimated_duration);
            put_str(&mut w, start_location_name);
            put_str(&mut w, end_location_name);
            put_bytes(&mut w, metadata);
        }
        ServerEvt::Error {
            code,
            message,
            track_uid,
            retry_after_ms,
        } => {
            put_u8(&mut w, S_ERROR);
            put_u8(&mut w, *code as u8);
            put_u32(&mut w, retry_after_ms.unwrap_or(0));
            put_uuid(&mut w, track_uid.as_deref().unwrap_or(""));
            put_str(&mut w, message);
        }
        ServerEvt::EventAdded {
            payload,
            timestamp_ms,
            sub,
            ..
        } => {
            put_u8(&mut w, S_EVENT_ADDED);
            put_u8(&mut w, *sub);
            put_bytes(&mut w, payload);
            put_i64(&mut w, timestamp_ms.unwrap_or(0));
        }
        ServerEvt::Command {
            command_id,
            payload,
            timestamp_ms,
        } => {
            put_u8(&mut w, S_COMMAND);
            put_uuid(&mut w, command_id);
            put_bytes(&mut w, payload);
            put_i64(&mut w, timestamp_ms.unwrap_or(0));
        }
        ServerEvt::DevicePresence {
            online,
            last_seen_ms,
            sub,
            ..
        } => {
            put_u8(&mut w, S_PRESENCE);
            put_u8(&mut w, *sub);
            put_u8(&mut w, u8::from(*online));
            put_i64(&mut w, last_seen_ms.unwrap_or(0));
        }
    }
    w
}

/// `Ok(None)` = unknown server type, ignore (forward-compat).
pub fn decode_server_evt(bytes: &[u8]) -> Result<Option<ServerEvt>, DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::Truncated);
    }
    let mut r = R(bytes);
    let typ = r.u8()?;
    let evt = match typ {
        S_HELLO => {
            let version = r.u8()?;
            let shard = r.u16()? as u32;
            let node_id = r.uuid()?;
            ServerEvt::Hello {
                version,
                node_id,
                shard,
            }
        }
        S_RELOCATE => ServerEvt::Relocate {
            retry_after_ms: r.u32()?,
            endpoint: r.str()?,
        },
        S_RESUME_OK => ServerEvt::ResumeOk {
            track_uid: r.uuid()?,
            last_acked_seq: r.u32()? as u64,
        },
        S_TRACK_STARTED => ServerEvt::TrackStarted {
            track_uid: r.uuid()?,
            metadata: r.bytes()?,
        },
        S_TRACK_STOPPED => ServerEvt::TrackStopped {
            track_uid: r.uuid()?,
        },
        S_ACK => ServerEvt::Ack {
            seq: r.u32()? as u64,
        },
        S_LOC => {
            let sub = r.u8()?;
            let seq = r.u32()? as u64;
            let (point, _, _) = read_point(&mut r, None)?;
            ServerEvt::LocationAdded {
                device_uid: String::new(),
                track_uid: String::new(),
                point,
                client_seq: seq,
                sub,
            }
        }
        S_SUBSCRIBED => {
            let sub = r.u8()?;
            let device_uid = r.uuid()?;
            let track_uid = r.uuid_opt()?.unwrap_or_default();
            let online = r.u8()? != 0;
            let flags = r.u8()?;
            let last_location = if flags & 1 != 0 {
                Some(read_point(&mut r, None)?.0)
            } else {
                None
            };
            let last_seen_ms = if flags & 2 != 0 {
                Some(r.i64()?)
            } else {
                None
            };
            let route = if flags & 4 != 0 {
                read_route_abs(&mut r)?
            } else {
                Vec::new()
            };
            let estimated_distance = r.f64()?;
            let estimated_duration = r.f64()?;
            let start_location_name = r.str()?;
            let end_location_name = r.str()?;
            let metadata = r.bytes()?;
            ServerEvt::Subscribed {
                sub,
                device_uid,
                track_uid,
                last_location,
                route,
                estimated_distance,
                estimated_duration,
                start_location_name,
                end_location_name,
                metadata,
                online,
                last_seen_ms,
            }
        }
        S_ERROR => {
            let code = ErrorCode::from_u8(r.u8()?).ok_or(DecodeError::Invalid)?;
            let retry = r.u32()?;
            let track_uid = r.uuid_opt()?;
            let message = r.str()?;
            ServerEvt::Error {
                code,
                message,
                track_uid,
                retry_after_ms: if retry == 0 { None } else { Some(retry) },
            }
        }
        S_EVENT_ADDED => {
            let sub = r.u8()?;
            let payload = r.bytes()?;
            let t = r.i64()?;
            ServerEvt::EventAdded {
                device_uid: String::new(),
                track_uid: String::new(),
                payload,
                timestamp_ms: if t == 0 { None } else { Some(t) },
                sub,
            }
        }
        S_COMMAND => ServerEvt::Command {
            command_id: r.uuid()?,
            payload: r.bytes()?,
            timestamp_ms: {
                let t = r.i64()?;
                if t == 0 {
                    None
                } else {
                    Some(t)
                }
            },
        },
        S_PRESENCE => {
            let sub = r.u8()?;
            let online = r.u8()? != 0;
            let t = r.i64()?;
            ServerEvt::DevicePresence {
                device_uid: String::new(),
                online,
                last_seen_ms: if t == 0 { None } else { Some(t) },
                sub,
            }
        }
        0x00 | 0x7F | 0xFF | 0x8C => return Err(DecodeError::Invalid),
        t if (0x80..=0xFE).contains(&t) => return Ok(None),
        _ => return Err(DecodeError::Invalid),
    };
    Ok(Some(evt))
}

/// Decode a server frame; unknown types yield `DecodeError::Invalid`.
pub fn decode_server_msg(data: &[u8]) -> Result<ServerEvt, DecodeError> {
    decode_server_evt(data)?.ok_or(DecodeError::Invalid)
}

/// Set `timestamp_ms` to now when omitted (capture time for Staging).
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

/// Live ~1 Hz: omit bit 4 so the server stamps ingest time.
pub fn strip_live_time(mut p: LatLng) -> LatLng {
    p.timestamp_ms = None;
    p
}

/// Build a resume command (for golden / wire tests).
pub fn client_resume(track_uid: impl Into<String>, last_client_seq: u64) -> ClientCmd {
    ClientCmd::Resume {
        track_uid: track_uid.into(),
        last_client_seq,
    }
}

#[allow(dead_code)]
pub const fn protocol_version() -> u8 {
    PROTOCOL_VERSION
}
