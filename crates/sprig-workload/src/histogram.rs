//! The sparse log latency histogram (chapter 12, "Latency histogram").
//!
//! Bucket `i` (0 to 95) counts latencies up to 10 µs × 2^(i/4); bucket 95
//! also counts anything slower (from about 141 s). Each bucket is 19% wider
//! than the one before, so a percentile read as the upper bound of its
//! bucket is at most 19% high and never optimistic.
//!
//! In memory the histogram keeps a dense window of counts from its lowest to
//! its highest non-empty bucket (most fingerprints span a few dozen
//! buckets). On disk (`workload_stats.latency_hist`) it is a sequence of
//! (index `u8`, count LEB128) pairs for the non-empty buckets, ascending.

use std::fmt;

/// Number of buckets.
pub const BUCKETS: usize = 96;

/// The upper bound of bucket `i`, in microseconds: 10 × 2^(i/4), rounded
/// up.
#[must_use]
pub fn upper_bound_us(i: usize) -> u64 {
    // 2^(i/4) = 2^(i div 4) × 2^((i mod 4)/4).
    const FRAC: [f64; 4] = [
        1.0,
        1.189_207_115_002_721,
        std::f64::consts::SQRT_2,
        1.681_792_830_507_429,
    ];
    let i = i.min(BUCKETS - 1);
    let v = 10.0 * FRAC[i % 4] * f64::from(1u32 << (i / 4));
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let r = v.ceil() as u64;
    r
}

/// The bucket of a latency: the smallest `i` whose upper bound is at least
/// `us`, at most 95.
#[must_use]
pub fn bucket_of(us: u64) -> usize {
    if us <= 10 {
        return 0;
    }
    // Start from the power of two below and walk at most a few steps.
    let ratio = us.div_ceil(10);
    let log2 = 63 - u64::leading_zeros(ratio.max(1)) as usize;
    let mut i = (log2 * 4).saturating_sub(4).min(BUCKETS - 1);
    while i < BUCKETS - 1 && upper_bound_us(i) < us {
        i += 1;
    }
    i
}

/// A latency histogram.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Histogram {
    /// Bucket of `counts[0]`.
    lo: u8,
    /// Counts of buckets `lo ..= lo + counts.len() - 1`.
    counts: Vec<u64>,
}

impl fmt::Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl Histogram {
    /// An empty histogram.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            lo: 0,
            counts: Vec::new(),
        }
    }

    /// Counts one latency.
    #[inline]
    pub fn record(&mut self, us: u64) {
        self.add_bucket(bucket_of(us), 1);
    }

    /// Adds `n` to bucket `i`.
    pub fn add_bucket(&mut self, i: usize, n: u64) {
        let i = i.min(BUCKETS - 1);
        if self.counts.is_empty() {
            self.lo = u8::try_from(i).unwrap_or(0);
            self.counts.push(n);
            return;
        }
        let lo = usize::from(self.lo);
        if i < lo {
            let grow = lo - i;
            self.counts.splice(0..0, std::iter::repeat_n(0, grow));
            self.lo = u8::try_from(i).unwrap_or(0);
            self.counts[0] += n;
        } else if i - lo >= self.counts.len() {
            self.counts.resize(i - lo + 1, 0);
            self.counts[i - lo] += n;
        } else {
            self.counts[i - lo] += n;
        }
    }

    /// The non-empty buckets, ascending: (index, count).
    pub fn iter(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        let lo = usize::from(self.lo);
        self.counts
            .iter()
            .enumerate()
            .filter(|(_, c)| **c > 0)
            .map(move |(k, c)| (lo + k, *c))
    }

    /// The number of latencies counted.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Whether nothing was counted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.counts.iter().all(|c| *c == 0)
    }

    /// Adds another histogram (associative and commutative).
    pub fn merge(&mut self, other: &Histogram) {
        for (i, c) in other.iter() {
            self.add_bucket(i, c);
        }
    }

    /// The `q` quantile (0 < q ≤ 1) in microseconds: the upper bound of the
    /// bucket where the cumulative count reaches `q` of the total. `None`
    /// when empty.
    #[must_use]
    pub fn quantile_us(&self, q: f64) -> Option<u64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let need = ((q.clamp(0.0, 1.0) * total as f64).ceil() as u64).max(1);
        let mut acc = 0u64;
        for (i, c) in self.iter() {
            acc += c;
            if acc >= need {
                return Some(upper_bound_us(i));
            }
        }
        self.iter().last().map(|(i, _)| upper_bound_us(i))
    }

    /// p50, p95 and p99 in microseconds.
    #[must_use]
    pub fn p50_p95_p99(&self) -> Option<(u64, u64, u64)> {
        Some((
            self.quantile_us(0.50)?,
            self.quantile_us(0.95)?,
            self.quantile_us(0.99)?,
        ))
    }

    /// Bytes this histogram holds in memory, for the recorder's cap.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<u64>()
    }

    /// The stored form: (index `u8`, count LEB128) pairs.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.counts.len() * 3);
        for (i, c) in self.iter() {
            out.push(u8::try_from(i).unwrap_or(u8::MAX));
            let mut v = c;
            loop {
                let b = u8::try_from(v & 0x7f).unwrap_or(0);
                v >>= 7;
                if v == 0 {
                    out.push(b);
                    break;
                }
                out.push(b | 0x80);
            }
        }
        out
    }

    /// Reads the stored form. `None` for a malformed encoding (an index past
    /// 95, indexes not ascending, or a truncated or overlong count).
    #[must_use]
    pub fn decode(mut bytes: &[u8]) -> Option<Self> {
        let mut h = Self::new();
        let mut last: Option<usize> = None;
        while let Some((&i, rest)) = bytes.split_first() {
            let i = usize::from(i);
            if i >= BUCKETS || last.is_some_and(|l| i <= l) {
                return None;
            }
            bytes = rest;
            let mut v = 0u64;
            let mut shift = 0u32;
            loop {
                let (&b, rest) = bytes.split_first()?;
                bytes = rest;
                if shift > 63 {
                    return None;
                }
                v |= u64::from(b & 0x7f).checked_shl(shift)?;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            h.add_bucket(i, v);
            last = Some(i);
        }
        Some(h)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn bounds() {
        assert_eq!(upper_bound_us(0), 10);
        assert_eq!(upper_bound_us(4), 20);
        assert_eq!(bucket_of(0), 0);
        assert_eq!(bucket_of(10), 0);
        assert_eq!(bucket_of(11), 1);
        assert_eq!(bucket_of(20), 4);
        assert_eq!(bucket_of(u64::MAX), 95);
        // Bucket 95 starts at about 141 s.
        assert!((140_000_000..142_000_000).contains(&upper_bound_us(95)));
        for i in 1..BUCKETS {
            assert!(upper_bound_us(i) > upper_bound_us(i - 1));
            assert_eq!(bucket_of(upper_bound_us(i)), i);
            assert_eq!(bucket_of(upper_bound_us(i - 1) + 1), i);
        }
    }

    #[test]
    fn round_trip_and_quantiles() {
        let mut h = Histogram::new();
        for us in [5, 100, 100, 250, 1_000, 50_000] {
            h.record(us);
        }
        assert_eq!(h.count(), 6);
        assert_eq!(Histogram::decode(&h.encode()), Some(h.clone()));
        assert_eq!(h.quantile_us(0.5), Some(upper_bound_us(bucket_of(100))));
        assert_eq!(h.quantile_us(1.0), Some(upper_bound_us(bucket_of(50_000))));
        assert_eq!(Histogram::new().quantile_us(0.95), None);
        assert_eq!(Histogram::decode(&[96, 1]), None);
        assert_eq!(Histogram::decode(&[3, 1, 3, 1]), None);
        assert_eq!(Histogram::decode(&[3, 0x80]), None);
    }

    proptest! {
        #[test]
        fn merge_is_associative_and_commutative(
            a in proptest::collection::vec(0u64..200_000_000, 0..40),
            b in proptest::collection::vec(0u64..200_000_000, 0..40),
            c in proptest::collection::vec(0u64..200_000_000, 0..40),
        ) {
            let h = |v: &[u64]| { let mut h = Histogram::new(); for x in v { h.record(*x); } h };
            let (ha, hb, hc) = (h(&a), h(&b), h(&c));
            let mut ab_c = ha.clone(); ab_c.merge(&hb); ab_c.merge(&hc);
            let mut bc = hb.clone(); bc.merge(&hc);
            let mut a_bc = ha.clone(); a_bc.merge(&bc);
            let mut cba = hc.clone(); cba.merge(&hb); cba.merge(&ha);
            prop_assert_eq!(&ab_c, &a_bc);
            prop_assert_eq!(ab_c.encode(), cba.encode());
            prop_assert_eq!(Histogram::decode(&ab_c.encode()).map(|x| x.encode()), Some(ab_c.encode()));
        }

        #[test]
        fn p95_within_19_percent_and_never_optimistic(v in proptest::collection::vec(11u64..100_000_000, 1..300)) {
            let mut h = Histogram::new();
            for x in &v { h.record(*x); }
            let mut sorted = v.clone();
            sorted.sort_unstable();
            for q in [0.5, 0.95, 0.99] {
                #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let k = ((q * sorted.len() as f64).ceil() as usize).max(1) - 1;
                let exact = sorted[k];
                let est = h.quantile_us(q).unwrap();
                prop_assert!(est >= exact, "q{} est {} < exact {}", q, est, exact);
                #[allow(clippy::cast_precision_loss)]
                let err = (est as f64 - exact as f64) / exact as f64;
                #[allow(clippy::cast_precision_loss)]
                let slack = 1.0 / exact as f64;
                prop_assert!(err <= 0.19 + 1e-9 + slack, "q{} err {}", q, err);
            }
        }

        #[test]
        fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let _ = Histogram::decode(&bytes);
        }
    }
}
