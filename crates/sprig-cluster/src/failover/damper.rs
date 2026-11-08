//! The failover-flapping damper (chapter 25, "Damping failover flapping";
//! chapter 47's thrash catalogue).
//!
//! ```text
//! may_fail_over(c):
//! 1. sources = independent views that the writer is down: the DC leader's probes, the
//!    agent (lease released or mysqld down), and gateways (a majority of those that used
//!    c in the last minute report connect failures)
//!    need >= failover.cluster_detect_sources of them; a probe timeout alone is "slow",
//!    never "down"
//! 2. now - last automatic failover < failover.min_interval_minutes -> only a hard
//!    failure (writer lease gone AND the host unreachable) proceeds; otherwise suppress
//! ```

use std::time::{Duration, Instant};

/// Independent views that a writer is down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct DownSources {
    /// The DC leader's probes were refused (not timed out) for
    /// `failover.cluster_detect_seconds`.
    pub probes_refused: bool,
    /// The DC leader's probes only timed out: "slow", which counts as no
    /// source.
    pub probes_timed_out: bool,
    /// The agent released the writer lease or reports mysqld down.
    pub agent: bool,
    /// A majority of the gateways that used the cluster in the last minute
    /// report connect failures.
    pub gateways: bool,
    /// The writer lease is gone and the host is unreachable.
    pub hard: bool,
}

impl DownSources {
    /// How many independent sources say "down".
    #[must_use]
    pub fn count(&self) -> u32 {
        u32::from(self.probes_refused) + u32::from(self.agent) + u32::from(self.gateways)
    }
}

/// What the damper allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Damp {
    /// Fail over.
    Proceed,
    /// Not enough sources agree yet: wait (the writer may be slow, not
    /// down).
    Wait(String),
    /// Within the minimum interval and not a hard failure: suppress, alert
    /// `FailoverSuppressed`, count
    /// `sprig_controller_failovers_suppressed_total`.
    Suppress(String),
}

/// One cluster's damper.
#[derive(Debug, Clone)]
pub struct FlapDamper {
    /// `failover.cluster_detect_sources`.
    pub need_sources: u32,
    /// `failover.min_interval_minutes`.
    pub min_interval: Duration,
    last: Option<Instant>,
}

impl FlapDamper {
    /// A damper with no failover yet.
    #[must_use]
    pub fn new(need_sources: u32, min_interval: Duration) -> Self {
        Self {
            need_sources,
            min_interval,
            last: None,
        }
    }

    /// The book's defaults: 2 sources, 30 minutes.
    #[must_use]
    pub fn defaults() -> Self {
        Self::new(2, Duration::from_secs(30 * 60))
    }

    /// Rules 1 and 2.
    #[must_use]
    pub fn may_fail_over(&self, s: &DownSources, now: Instant) -> Damp {
        let n = s.count();
        if n < self.need_sources && !s.hard {
            let why = if s.probes_timed_out && n == 0 {
                "probes time out: the writer is slow, not down".to_owned()
            } else {
                format!("{n} of {} sources say the writer is down", self.need_sources)
            };
            return Damp::Wait(why);
        }
        if let Some(last) = self.last {
            let since = now.saturating_duration_since(last);
            if since < self.min_interval && !s.hard {
                return Damp::Suppress(format!(
                    "the last automatic failover was {} s ago, within failover.min_interval_minutes",
                    since.as_secs()
                ));
            }
        }
        Damp::Proceed
    }

    /// Records an automatic failover at `now`.
    pub fn record(&mut self, now: Instant) {
        self.last = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timeout_alone_never_fails_over() {
        let d = FlapDamper::defaults();
        let s = DownSources {
            probes_timed_out: true,
            ..DownSources::default()
        };
        assert!(matches!(d.may_fail_over(&s, Instant::now()), Damp::Wait(w) if w.contains("slow")));
    }

    #[test]
    fn two_sources_proceed_then_the_interval_suppresses() {
        let mut d = FlapDamper::defaults();
        let t0 = Instant::now();
        let s = DownSources {
            probes_refused: true,
            agent: true,
            ..DownSources::default()
        };
        assert_eq!(d.may_fail_over(&s, t0), Damp::Proceed);
        d.record(t0);
        let t1 = t0 + Duration::from_secs(600);
        assert!(matches!(d.may_fail_over(&s, t1), Damp::Suppress(_)));
        let hard = DownSources { hard: true, ..s };
        assert_eq!(d.may_fail_over(&hard, t1), Damp::Proceed, "a hard failure proceeds");
        let t2 = t0 + Duration::from_secs(31 * 60);
        assert_eq!(d.may_fail_over(&s, t2), Damp::Proceed);
    }

    #[test]
    fn a_writer_stalling_every_ten_minutes_fails_over_once_per_interval() {
        // Chapter 25's flapping scenario: a 4 s stall every 10 minutes, seen
        // by probes and gateways; two hours give at most 4 failovers.
        let mut d = FlapDamper::defaults();
        let t0 = Instant::now();
        let s = DownSources {
            probes_refused: true,
            gateways: true,
            ..DownSources::default()
        };
        let mut failovers = 0;
        for i in 0..12u64 {
            let t = t0 + Duration::from_secs(i * 600);
            if d.may_fail_over(&s, t) == Damp::Proceed {
                failovers += 1;
                d.record(t);
            }
        }
        assert_eq!(failovers, 4, "one per 30 minutes over 2 hours");
    }
}
