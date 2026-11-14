//! Virtual clocks: [`SimClock`] implements [`Clock`] on tokio's paused
//! timer, one per simulated node, with injectable skew.
//!
//! Inside a simulation tokio's time is paused and jumps to the next timer
//! whenever every task waits, so `tokio::time::sleep` and [`SimClock::now`]
//! move together and a run of hours takes milliseconds. Wall time starts at
//! a fixed epoch, so nothing a run records depends on the real date.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sprig_common::time::Clock;

/// Wall time at the start of every simulation: 2026-01-01T00:00:00Z.
pub const SIM_EPOCH_SECS: u64 = 1_767_225_600;

#[derive(Debug, Clone, Copy)]
struct Skew {
    /// Forward jumps of the monotonic and wall clocks, summed.
    jumped: Duration,
    /// Extra wall-clock offset in milliseconds, positive or negative.
    wall_offset_ms: i64,
    /// Rate error in parts per million: +100 runs 0.01% fast.
    rate_ppm: i64,
    /// Elapsed (true) time at which the rate last changed, and the skewed
    /// elapsed time then, so a rate change never makes time jump.
    rate_base: (Duration, Duration),
}

/// A node's clock in a simulation.
#[derive(Debug)]
pub struct SimClock {
    origin: tokio::time::Instant,
    skew: Mutex<Skew>,
}

impl SimClock {
    /// A clock starting now on tokio's timer. Call inside the runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            skew: Mutex::new(Skew {
                jumped: Duration::ZERO,
                wall_offset_ms: 0,
                rate_ppm: 0,
                rate_base: (Duration::ZERO, Duration::ZERO),
            }),
        }
    }

    fn skew(&self) -> std::sync::MutexGuard<'_, Skew> {
        self.skew.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// True time since the clock started, without skew.
    #[must_use]
    pub fn true_elapsed(&self) -> Duration {
        tokio::time::Instant::now() - self.origin
    }

    fn skewed(s: &Skew, true_elapsed: Duration) -> Duration {
        let (base_true, base_skewed) = s.rate_base;
        let since = true_elapsed.saturating_sub(base_true);
        let nanos = i128::try_from(since.as_nanos()).unwrap_or(i128::MAX);
        let adj = nanos * i128::from(s.rate_ppm) / 1_000_000;
        let since = u64::try_from((nanos + adj).max(0)).unwrap_or(u64::MAX);
        base_skewed + Duration::from_nanos(since)
    }

    /// Elapsed time as this node sees it: rate skew and jumps applied.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        let s = *self.skew();
        Self::skewed(&s, self.true_elapsed()) + s.jumped
    }

    /// Moves both clocks forward by `d`, as after a VM resume or an NTP step.
    pub fn jump(&self, d: Duration) {
        self.skew().jumped += d;
    }

    /// Sets the wall clock's offset from true time, in milliseconds. The
    /// monotonic clock is unaffected: it never runs backwards.
    pub fn set_wall_offset_ms(&self, ms: i64) {
        self.skew().wall_offset_ms = ms;
    }

    /// Sets the rate error in parts per million (to test lease
    /// arithmetic under rate skew).
    pub fn set_rate_ppm(&self, ppm: i64) {
        let now = self.true_elapsed();
        let mut s = self.skew();
        let skewed = Self::skewed(&s, now);
        s.rate_base = (now, skewed);
        s.rate_ppm = ppm;
    }

    /// Removes every skew. Jumps already taken stay: time can't go back.
    pub fn reset_skew(&self) {
        self.set_rate_ppm(0);
        self.set_wall_offset_ms(0);
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        self.origin.into_std() + self.elapsed()
    }

    fn wall(&self) -> SystemTime {
        let offset = self.skew().wall_offset_ms;
        let t = UNIX_EPOCH + Duration::from_secs(SIM_EPOCH_SECS) + self.elapsed();
        let off = Duration::from_millis(offset.unsigned_abs());
        if offset >= 0 { t + off } else { t - off }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn follows_paused_time_and_skews() {
        let c = SimClock::new();
        let t0 = c.now();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(c.now() - t0, Duration::from_secs(10));
        assert_eq!(
            c.wall(),
            UNIX_EPOCH + Duration::from_secs(SIM_EPOCH_SECS + 10)
        );

        c.set_rate_ppm(100_000); // 10% fast
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(c.now() - t0, Duration::from_secs(21));

        c.jump(Duration::from_secs(5));
        assert_eq!(c.now() - t0, Duration::from_secs(26));

        c.set_wall_offset_ms(-2_000);
        assert_eq!(
            c.wall(),
            UNIX_EPOCH + Duration::from_secs(SIM_EPOCH_SECS + 24)
        );
        c.reset_skew();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(c.now() - t0, Duration::from_secs(27));
    }
}
