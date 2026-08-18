//! Device GPS filter ([filter.md](../../../pickpoint-proto/spec/filter.md)).
//! Heading and speed stay local; they are never written to the wire.

use crate::tracking::types::LatLng;

const HEARTBEAT_MS: i64 = 1000;
const HEADING_JUMP_DEG: f64 = 25.0;
const MIN_MOVE_M: f64 = 2.0;
const MOTION_EPS_MS: f64 = 0.5;

/// ~1 Hz heartbeat + vertices. At ~1 Hz incoming this is pass-through.
#[derive(Debug, Clone, Default)]
pub struct NoiseFilter {
    last_emitted: Option<LatLng>,
    candidate: Option<LatLng>,
    last_emit_at: Option<i64>,
}

impl NoiseFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed after `TrackStart` so the start point is not re-emitted as “first”.
    pub fn seed(&mut self, point: LatLng) {
        let t = point.timestamp_ms.unwrap_or(0);
        self.last_emitted = Some(point);
        self.candidate = None;
        self.last_emit_at = Some(t);
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Push a sample. `None` means hold as candidate / drop as collinear junk.
    pub fn push(&mut self, current: LatLng) -> Option<LatLng> {
        let now = current.timestamp_ms.unwrap_or_else(now_ms);
        let mut current = current;
        current.timestamp_ms = Some(now);

        if self.last_emitted.is_none() {
            return Some(self.emit(current, now));
        }

        if self.should_emit(&current, now) {
            return Some(self.emit(current, now));
        }

        self.candidate = Some(current);
        None
    }

    fn should_emit(&self, current: &LatLng, now: i64) -> bool {
        let Some(last) = self.last_emitted.as_ref() else {
            return true;
        };
        if now.saturating_sub(self.last_emit_at.unwrap_or(now)) >= HEARTBEAT_MS {
            return true;
        }

        let acc = current.accuracy.unwrap_or(0.0);
        let dist = haversine_m(last, current);
        if dist >= MIN_MOVE_M.max(2.0 * acc) {
            return true;
        }

        if let Some(cand) = self.candidate.as_ref() {
            let speed = current.speed.unwrap_or(0.0);
            let eps = MIN_MOVE_M.max(acc).max(0.5 * speed);
            if perpendicular_m(last, current, cand) >= eps {
                return true;
            }
        }

        if let (Some(h0), Some(h1)) = (last.heading, current.heading) {
            if heading_delta_deg(h0, h1) >= HEADING_JUMP_DEG {
                return true;
            }
        }

        if let (Some(s0), Some(s1)) = (last.speed, current.speed) {
            let moving0 = s0 > MOTION_EPS_MS;
            let moving1 = s1 > MOTION_EPS_MS;
            if moving0 != moving1 {
                return true;
            }
        }

        false
    }

    fn emit(&mut self, point: LatLng, now: i64) -> LatLng {
        self.last_emitted = Some(point.clone());
        self.candidate = None;
        self.last_emit_at = Some(now);
        point
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn haversine_m(a: &LatLng, b: &LatLng) -> f64 {
    const R: f64 = 6_371_000.0;
    let dlat = (b.latitude - a.latitude).to_radians();
    let dlon = (b.longitude - a.longitude).to_radians();
    let la1 = a.latitude.to_radians();
    let la2 = b.latitude.to_radians();
    let h = (dlat / 2.0).sin().powi(2) + la1.cos() * la2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * h.sqrt().asin()
}

/// Perpendicular distance from `p` to the line `a → b` (metres).
fn perpendicular_m(a: &LatLng, b: &LatLng, p: &LatLng) -> f64 {
    let (ax, ay) = project_m(a, a);
    let (bx, by) = project_m(a, b);
    let (px, py) = project_m(a, p);
    let dx = bx - ax;
    let dy = by - ay;
    let len2 = dx * dx + dy * dy;
    if len2 < 1e-6 {
        return haversine_m(a, p);
    }
    let t = ((px - ax) * dx + (py - ay) * dy) / len2;
    let qx = ax + t * dx;
    let qy = ay + t * dy;
    ((px - qx).powi(2) + (py - qy).powi(2)).sqrt()
}

fn project_m(origin: &LatLng, p: &LatLng) -> (f64, f64) {
    const M_PER_DEG_LAT: f64 = 111_320.0;
    let lat0 = origin.latitude.to_radians();
    let x = (p.longitude - origin.longitude) * M_PER_DEG_LAT * lat0.cos();
    let y = (p.latitude - origin.latitude) * M_PER_DEG_LAT;
    (x, y)
}

fn heading_delta_deg(a: f64, b: f64) -> f64 {
    let d = (b - a).rem_euclid(360.0);
    if d > 180.0 {
        360.0 - d
    } else {
        d
    }
}

/// Collapse one middle collinear sample. Returns whether a point was removed.
pub fn collapse_one_collinear(points: &mut Vec<LatLng>) -> bool {
    if points.len() < 3 {
        return false;
    }
    for i in 1..points.len() - 1 {
        let acc = points[i].accuracy.unwrap_or(0.0);
        let eps = MIN_MOVE_M.max(acc);
        if perpendicular_m(&points[i - 1], &points[i + 1], &points[i]) < eps {
            points.remove(i);
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_50hz_collinear_heartbeat() {
        let mut f = NoiseFilter::new();
        let mut emitted = 0;
        // 2 s of 50 Hz, ~0.05 m/step north, accuracy 5 m → move floor is 10 m.
        for i in 0..100 {
            let t = i * 20;
            let lat = 55.0 + (i as f64) * 0.05 / 111_320.0;
            let p = LatLng {
                latitude: lat,
                longitude: 37.0,
                accuracy: Some(5.0),
                heading: Some(0.0),
                timestamp_ms: Some(t),
                ..Default::default()
            };
            if f.push(p).is_some() {
                emitted += 1;
            }
        }
        assert!(
            (2..=4).contains(&emitted),
            "expected ~1 Hz heartbeat, got {emitted} emits from 100 samples"
        );
    }
}
