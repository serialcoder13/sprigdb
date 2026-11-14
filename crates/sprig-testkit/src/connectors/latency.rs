//! Latency percentiles for the latency gate: the gateway adds no more than
//! 0.5 ms at p50 inside a data center, measured against direct MariaDB.

use std::fmt;
use std::time::Duration;

/// Percentiles of one workload on one endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// Number of samples.
    pub n: usize,
    /// Median.
    pub p50: Duration,
    /// 99th percentile.
    pub p99: Duration,
    /// Slowest sample.
    pub max: Duration,
}

/// The nearest-rank percentile of sorted samples, in per mille (500 is the
/// median, 990 the 99th percentile).
///
/// # Panics
/// If `sorted` is empty or `per_mille` is above 1000.
#[must_use]
pub fn percentile(sorted: &[Duration], per_mille: usize) -> Duration {
    assert!(!sorted.is_empty(), "no samples");
    assert!(per_mille <= 1000, "per_mille {per_mille} > 1000");
    // Rank ceil(p * n), 1-based; rank 0 (p = 0) is the minimum.
    let rank = (per_mille * sorted.len()).div_ceil(1000).max(1);
    sorted[rank - 1]
}

/// Sorts `samples` and summarizes them.
///
/// # Panics
/// If `samples` is empty.
#[must_use]
pub fn summarize(samples: &mut [Duration]) -> Summary {
    samples.sort_unstable();
    Summary {
        n: samples.len(),
        p50: percentile(samples, 500),
        p99: percentile(samples, 990),
        max: samples[samples.len() - 1],
    }
}

/// Milliseconds, for reports and gates.
#[must_use]
pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// `target - baseline` in milliseconds (negative when the target is faster).
#[must_use]
pub fn delta_ms(target: Duration, baseline: Duration) -> f64 {
    ms(target) - ms(baseline)
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "p50 {:.3} ms  p99 {:.3} ms  max {:.3} ms  (n={})",
            ms(self.p50),
            ms(self.p99),
            ms(self.max),
            self.n
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn us(v: &[u64]) -> Vec<Duration> {
        v.iter().map(|&x| Duration::from_micros(x)).collect()
    }

    #[test]
    fn nearest_rank() {
        let mut s = us(&(1..=100).collect::<Vec<_>>());
        let sum = summarize(&mut s);
        assert_eq!(sum.p50, Duration::from_micros(50));
        assert_eq!(sum.p99, Duration::from_micros(99));
        assert_eq!(sum.max, Duration::from_micros(100));
        assert_eq!(percentile(&s, 0), Duration::from_micros(1));
        assert_eq!(percentile(&s, 1000), Duration::from_micros(100));
    }

    #[test]
    fn unsorted_and_small() {
        let mut s = us(&[30, 10, 20]);
        let sum = summarize(&mut s);
        assert_eq!(sum.p50, Duration::from_micros(20));
        assert_eq!(sum.p99, Duration::from_micros(30));
        let one = summarize(&mut us(&[7]));
        assert_eq!(
            (one.p50, one.p99),
            (Duration::from_micros(7), Duration::from_micros(7))
        );
    }

    #[test]
    fn deltas() {
        let d = delta_ms(Duration::from_micros(700), Duration::from_micros(250));
        assert!((d - 0.45).abs() < 1e-9, "{d}");
        assert!(delta_ms(Duration::from_micros(100), Duration::from_micros(200)) < 0.0);
    }
}
