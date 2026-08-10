use crate::tracking::v2::LatLng;

/// Offline location pending flush after resume.
#[derive(Debug, Clone)]
pub struct QueuedPoint {
    /// Client sequence.
    pub seq: u64,
    /// Point payload.
    pub point: LatLng,
}

/// Bounded offline queue keyed by clientSeq. Drop-oldest on overflow.
#[derive(Debug, Default)]
pub struct OfflineQueue {
    max_size: usize,
    items: Vec<QueuedPoint>,
}

impl OfflineQueue {
    /// Create a queue with `max_size` capacity (default 10_000).
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size: if max_size == 0 { 10_000 } else { max_size },
            items: Vec::new(),
        }
    }

    /// Current size.
    pub fn size(&self) -> usize {
        self.items.len()
    }

    /// Enqueue a point; drops oldest on overflow. Returns dropped count.
    pub fn enqueue(&mut self, seq: u64, point: LatLng) -> usize {
        self.items.push(QueuedPoint { seq, point });
        if self.items.len() > self.max_size {
            let dropped = self.items.len() - self.max_size;
            self.items.drain(0..dropped);
            dropped
        } else {
            0
        }
    }

    /// Drop points with seq <= ack (inclusive).
    pub fn ack_through(&mut self, ack: u64) {
        self.items.retain(|p| p.seq > ack);
    }

    /// Snapshot of pending points.
    pub fn peek_all(&self) -> Vec<QueuedPoint> {
        self.items.clone()
    }

    /// Clear the queue.
    pub fn clear(&mut self) {
        self.items.clear();
    }
}
