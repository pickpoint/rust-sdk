use std::time::Instant;

use crate::tracking::client::{MAX_PUBLISH_HZ, MIN_PUBLISH_INTERVAL};

/// Millisecond form of [`MIN_PUBLISH_INTERVAL`] (JS parity).
pub const MIN_PUBLISH_INTERVAL_MS: u64 = 1000 / MAX_PUBLISH_HZ as u64;

/// Whether `point_count` points can be accepted at `now`.
pub fn can_accept_publish(next_allowed_at: Instant, now: Instant, point_count: i32) -> bool {
    if point_count <= 0 {
        return true;
    }
    now >= next_allowed_at
}

/// Advance the gate after accepting `point_count` points at `now`.
pub fn next_publish_allowed_at(
    next_allowed_at: Instant,
    now: Instant,
    point_count: i32,
) -> Instant {
    let start = if next_allowed_at > now {
        next_allowed_at
    } else {
        now
    };
    let count = point_count.max(0) as u32;
    start + MIN_PUBLISH_INTERVAL * count
}
