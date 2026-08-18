use crate::tracking::filter::collapse_one_collinear;
use crate::tracking::types::LatLng;

/// Default cap across Staging + InFlight.
pub const DEFAULT_MAX_QUEUE: usize = 10_000;
/// Max unacked Loc frames (send window).
pub const MAX_UNACKED_FRAMES: usize = 8;

/// Point waiting for `Ack` (already assigned a seq).
#[derive(Debug, Clone)]
pub struct QueuedPoint {
    pub seq: u64,
    pub point: LatLng,
    pub sent: bool,
}

/// Staging (no seq) + InFlight (seq, waiting for Ack).
#[derive(Debug, Default)]
pub struct OfflineQueue {
    max_size: usize,
    staging: Vec<LatLng>,
    inflight: Vec<QueuedPoint>,
    sent_frame_seqs: Vec<u64>,
}

impl OfflineQueue {
    /// Create a buffer with `max_size` capacity (default 10_000).
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size: if max_size == 0 {
                DEFAULT_MAX_QUEUE
            } else {
                max_size
            },
            staging: Vec::new(),
            inflight: Vec::new(),
            sent_frame_seqs: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.staging.len() + self.inflight.len()
    }

    pub fn staging_len(&self) -> usize {
        self.staging.len()
    }

    pub fn last_assigned_seq(&self) -> u64 {
        self.inflight.last().map(|p| p.seq).unwrap_or(0)
    }

    /// Filtered point that cannot be sent yet (socket down or window full).
    pub fn push_staging(&mut self, point: LatLng) {
        self.staging.push(point);
        self.enforce_cap();
    }

    /// Assign seqs to the next `n` staging points and append them to InFlight as unsent.
    pub fn assign_from_staging(&mut self, n: usize, mut next_seq: u64) -> Vec<QueuedPoint> {
        let take = n.min(self.staging.len());
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            let point = self.staging.remove(0);
            next_seq += 1;
            let q = QueuedPoint {
                seq: next_seq,
                point,
                sent: false,
            };
            self.inflight.push(q.clone());
            out.push(q);
        }
        out
    }

    /// Enqueue an already-numbered InFlight point (tests / live assign).
    pub fn enqueue(&mut self, seq: u64, point: LatLng) -> usize {
        self.inflight.push(QueuedPoint {
            seq,
            point,
            sent: true,
        });
        let before = self.size();
        self.enforce_cap();
        before.saturating_sub(self.size()).min(1)
    }

    pub fn push_inflight_unsent(&mut self, seq: u64, point: LatLng) {
        self.inflight.push(QueuedPoint {
            seq,
            point,
            sent: false,
        });
        self.enforce_cap();
    }

    pub fn mark_unsent(&mut self) {
        for p in &mut self.inflight {
            p.sent = false;
        }
        self.sent_frame_seqs.clear();
    }

    pub fn window_full(&self) -> bool {
        self.sent_frame_seqs.len() >= MAX_UNACKED_FRAMES
    }

    pub fn window_remaining(&self) -> usize {
        MAX_UNACKED_FRAMES.saturating_sub(self.sent_frame_seqs.len())
    }

    pub fn record_frame(&mut self, last_seq: u64) {
        self.sent_frame_seqs.push(last_seq);
        for p in &mut self.inflight {
            if p.seq <= last_seq {
                p.sent = true;
            }
        }
    }

    /// Drop InFlight points with seq <= ack (inclusive).
    pub fn ack_through(&mut self, ack: u64) {
        self.inflight.retain(|p| p.seq > ack);
        self.sent_frame_seqs.retain(|s| *s > ack);
    }

    pub fn peek_all(&self) -> Vec<QueuedPoint> {
        self.inflight.clone()
    }

    pub fn peek_staging(&self) -> Vec<LatLng> {
        self.staging.clone()
    }

    pub fn unsent_inflight(&self) -> Vec<QueuedPoint> {
        self.inflight.iter().filter(|p| !p.sent).cloned().collect()
    }

    pub fn clear(&mut self) {
        self.staging.clear();
        self.inflight.clear();
        self.sent_frame_seqs.clear();
    }

    fn enforce_cap(&mut self) {
        while self.size() > self.max_size {
            if collapse_one_collinear(&mut self.staging) {
                continue;
            }
            if !self.staging.is_empty() {
                // Keep newest: drop oldest staging.
                self.staging.remove(0);
                continue;
            }
            if !self.inflight.is_empty() {
                // Keep newest: drop oldest InFlight.
                self.inflight.remove(0);
            } else {
                break;
            }
        }
    }
}
