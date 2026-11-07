//! Clocks (chapter 03, "Time and clocks").
//!
//! Every component reads time through [`Clock`], never `Instant::now()`
//! directly, so deterministic simulation can swap in a virtual clock.
//! Durations use the monotonic clock; wall-clock time is for logs, audit
//! events, catalog timestamps and statistics buckets only.

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// A source of monotonic and wall-clock time.
pub trait Clock: Send + Sync + 'static {
    /// Monotonic time, for timeouts, leases and cooldowns.
    fn now(&self) -> Instant;
    /// Wall-clock time (UTC), for people and records only.
    fn wall(&self) -> SystemTime;
}

/// The real clocks of the operating system.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A clock that moves only when told to, for tests.
#[derive(Debug)]
pub struct ManualClock {
    state: Mutex<(Instant, SystemTime)>,
}

impl ManualClock {
    /// A clock starting at the current real time.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new((Instant::now(), SystemTime::now())),
        }
    }

    /// Moves both clocks forward by `d`.
    pub fn advance(&self, d: Duration) {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.0 += d;
        s.1 += d;
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }
    fn wall(&self) -> SystemTime {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_advanced() {
        let c = ManualClock::new();
        let t0 = c.now();
        assert_eq!(c.now(), t0);
        c.advance(Duration::from_secs(5));
        assert_eq!(c.now() - t0, Duration::from_secs(5));
    }
}
