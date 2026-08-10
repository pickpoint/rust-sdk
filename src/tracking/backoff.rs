use std::time::Duration;

/// Tracks full-jitter exponential reconnect delays.
#[derive(Debug, Clone)]
pub struct BackoffState {
    /// Current attempt counter.
    pub attempt: u32,
    /// Minimum delay.
    pub min_delay: Duration,
    /// Maximum delay.
    pub max_delay: Duration,
    /// Max attempts (`0` = unlimited).
    pub max_attempts: u32,
}

/// Build backoff from reconnect options (defaults: 500ms … 30s).
pub fn new_backoff(min_delay: Duration, max_delay: Duration, max_attempts: u32) -> BackoffState {
    BackoffState {
        attempt: 0,
        min_delay: if min_delay.is_zero() {
            Duration::from_millis(500)
        } else {
            min_delay
        },
        max_delay: if max_delay.is_zero() {
            Duration::from_secs(30)
        } else {
            max_delay
        },
        max_attempts,
    }
}

/// Next sleep duration, or `None` when attempts are exhausted.
/// `random` in `[0, 1)`.
pub fn next_delay(state: &mut BackoffState, random: f64) -> Option<Duration> {
    if state.max_attempts > 0 && state.attempt >= state.max_attempts {
        return None;
    }
    let min_ms = state.min_delay.as_millis() as f64;
    let max_ms = state.max_delay.as_millis() as f64;
    let mut exp = min_ms * 2f64.powi(state.attempt as i32);
    if exp > max_ms {
        exp = max_ms;
    }
    state.attempt += 1;
    let r = random.clamp(0.0, 1.0);
    Some(Duration::from_millis((r * exp).floor() as u64))
}

/// Clear the attempt counter.
pub fn reset_backoff(state: &mut BackoffState) {
    state.attempt = 0;
}
