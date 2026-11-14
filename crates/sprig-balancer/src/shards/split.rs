//! Hash histograms and split points.
//!
//! A histogram covers one shard's range `[lo, hi)` with buckets of bytes
//! (from index dives) or writes (from gateway sketches), and names heavy
//! hashes. [`choose_split`] picks the children: a hash carrying half the
//! weight or more is isolated in a shard of its own; otherwise the points
//! are the weighted quantiles `i/k`, interpolated within buckets, moved
//! off a heavy hash's bucket to the hash's edge, and a child under 10% of
//! the weight is refused unless it isolates a hot key.

use serde::{Deserialize, Serialize};

/// One bucket `[lo, next lo)` of a histogram.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    /// First hash of the bucket.
    pub lo: u64,
    /// Bytes in it.
    pub bytes: f64,
    /// Writes in it.
    pub writes: f64,
}

/// A shard's histogram over its range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HashHistogram {
    /// The shard's first hash.
    pub lo: u64,
    /// One past its last; `None` is 2^64.
    pub hi: Option<u64>,
    /// Buckets by `lo`; the first starts at `lo`.
    pub buckets: Vec<Bucket>,
    /// Heavy hashes and their weight (in the histogram's weight).
    pub heavy: Vec<(u64, f64)>,
}

/// Which weight to split by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weight {
    /// Bytes: a size split.
    Bytes,
    /// Writes: a load split.
    Writes,
}

/// The children of a split.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SplitPlan {
    /// The split points: the children are `[lo, p1), [p1, p2), ..., [pn, hi)`.
    pub points: Vec<u64>,
    /// The hash isolated in a child of its own, if one dominated.
    pub hot_key: Option<u64>,
    /// Each child's share of the weight, in order.
    pub shares: Vec<f64>,
}

/// Why no split is possible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoSplit {
    /// The range is one hash (a hot key that can't split further).
    SingleHash,
    /// A child would hold under 10% of the weight.
    Unbalanced,
}

fn hi128(hi: Option<u64>) -> u128 {
    hi.map_or(1u128 << 64, u128::from)
}

impl HashHistogram {
    /// An empty histogram of `n` equal buckets over `[lo, hi)`.
    #[must_use]
    pub fn equal(lo: u64, hi: Option<u64>, n: u32) -> Self {
        let n = u128::from(n.max(1));
        let width = (hi128(hi) - u128::from(lo)) / n;
        let buckets = (0..n)
            .map(|i| Bucket {
                lo: u64::try_from(u128::from(lo) + i * width.max(1)).unwrap_or(u64::MAX),
                bytes: 0.0,
                writes: 0.0,
            })
            .collect::<Vec<_>>();
        let mut h = Self {
            lo,
            hi,
            buckets,
            heavy: Vec::new(),
        };
        h.buckets.dedup_by_key(|b| b.lo);
        h
    }

    /// One past the last hash of bucket `i`.
    #[must_use]
    pub fn bucket_hi(&self, i: usize) -> u128 {
        self.buckets
            .get(i + 1)
            .map_or_else(|| hi128(self.hi), |b| u128::from(b.lo))
    }

    /// The bucket holding `h`.
    #[must_use]
    pub fn bucket_of(&self, h: u64) -> usize {
        self.buckets
            .partition_point(|b| b.lo <= h)
            .saturating_sub(1)
    }

    fn w(b: &Bucket, weight: Weight) -> f64 {
        match weight {
            Weight::Bytes => b.bytes,
            Weight::Writes => b.writes,
        }
        .max(0.0)
    }

    /// The total weight.
    #[must_use]
    pub fn total(&self, weight: Weight) -> f64 {
        self.buckets.iter().map(|b| Self::w(b, weight)).sum()
    }

    /// The weight of `[a, b)`, interpolated within buckets.
    #[must_use]
    pub fn mass(&self, a: u128, b: u128, weight: Weight) -> f64 {
        let mut m = 0.0;
        for (i, bk) in self.buckets.iter().enumerate() {
            let (lo, hi) = (u128::from(bk.lo), self.bucket_hi(i));
            let (x, y) = (lo.max(a), hi.min(b));
            if y > x && hi > lo {
                #[allow(clippy::cast_precision_loss)]
                let f = (y - x) as f64 / (hi - lo) as f64;
                m += f * Self::w(bk, weight);
            }
        }
        m
    }

    /// The hash at which the cumulative weight reaches `target`.
    #[must_use]
    fn quantile(&self, target: f64, weight: Weight) -> u128 {
        let mut cum = 0.0;
        for (i, bk) in self.buckets.iter().enumerate() {
            let w = Self::w(bk, weight);
            if cum + w >= target && w > 0.0 {
                let (lo, hi) = (u128::from(bk.lo), self.bucket_hi(i));
                let f = ((target - cum) / w).clamp(0.0, 1.0);
                #[allow(
                    clippy::cast_precision_loss,
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss
                )]
                let off = ((hi - lo) as f64 * f) as u128;
                return (lo + off).min(hi).max(lo);
            }
            cum += w;
        }
        hi128(self.hi) - 1
    }
}

/// `choose_split`: up to `ways` children by `weight`.
///
/// # Errors
/// [`NoSplit`] when the range is a single hash or a child would hold under
/// 10% of the weight without isolating a hot key.
pub fn choose_split(h: &HashHistogram, weight: Weight, ways: u32) -> Result<SplitPlan, NoSplit> {
    let (lo, hi) = (u128::from(h.lo), hi128(h.hi));
    if hi - lo <= 1 {
        return Err(NoSplit::SingleHash);
    }
    let total = h.total(weight);
    let ways = ways.max(2);
    // One hash dominates: isolate it.
    if total > 0.0
        && let Some(&(hot, w)) = h
            .heavy
            .iter()
            .filter(|(x, _)| u128::from(*x) >= lo && u128::from(*x) < hi)
            .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
        && w >= total / 2.0
    {
        let mut points = Vec::new();
        if u128::from(hot) > lo {
            points.push(hot);
        }
        if u128::from(hot) + 1 < hi {
            points.push(hot + 1);
        }
        let shares = shares(h, &points, weight, total);
        return Ok(SplitPlan {
            points,
            hot_key: Some(hot),
            shares,
        });
    }
    let mut points: Vec<u64> = Vec::new();
    for i in 1..ways {
        let p = if total > 0.0 {
            h.quantile(total * f64::from(i) / f64::from(ways), weight)
        } else {
            lo + (hi - lo) * u128::from(i) / u128::from(ways)
        };
        let mut p = u64::try_from(p).unwrap_or(u64::MAX);
        // Keep a heavy hash's rows together, at its edge.
        for &(hh, _) in &h.heavy {
            if h.bucket_of(hh) == h.bucket_of(p) && p != hh {
                let left = h.mass(lo, u128::from(hh), weight);
                let target = total * f64::from(i) / f64::from(ways);
                p = if (left - target).abs()
                    <= (h.mass(lo, u128::from(hh) + 1, weight) - target).abs()
                    || hh == u64::MAX
                {
                    hh
                } else {
                    hh + 1
                };
            }
        }
        if u128::from(p) > lo && u128::from(p) < hi {
            points.push(p);
        }
    }
    points.sort_unstable();
    points.dedup();
    if points.is_empty() {
        return Err(NoSplit::Unbalanced);
    }
    let shares = shares(h, &points, weight, total);
    if total > 0.0 && shares.iter().any(|s| *s < 0.1) {
        return Err(NoSplit::Unbalanced);
    }
    Ok(SplitPlan {
        points,
        hot_key: None,
        shares,
    })
}

fn shares(h: &HashHistogram, points: &[u64], weight: Weight, total: f64) -> Vec<f64> {
    let mut edges: Vec<u128> = vec![u128::from(h.lo)];
    edges.extend(points.iter().map(|p| u128::from(*p)));
    edges.push(hi128(h.hi));
    edges
        .windows(2)
        .map(|w| {
            if total > 0.0 {
                let mut m = h.mass(w[0], w[1], weight);
                // A heavy hash's weight sits on its own hash.
                for &(hh, hw) in &h.heavy {
                    let x = u128::from(hh);
                    let bucket = h.bucket_of(hh);
                    let (blo, bhi) = (u128::from(h.buckets[bucket].lo), h.bucket_hi(bucket));
                    #[allow(clippy::cast_precision_loss)]
                    let spread = hw * (w[1].min(bhi).saturating_sub(w[0].max(blo))) as f64
                        / (bhi - blo).max(1) as f64;
                    m -= spread.min(m);
                    if x >= w[0] && x < w[1] {
                        m += hw;
                    }
                }
                m / total
            } else {
                #[allow(clippy::cast_precision_loss)]
                let f = (w[1] - w[0]) as f64 / (hi128(h.hi) - u128::from(h.lo)) as f64;
                f
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn flat(lo: u64, hi: Option<u64>, n: u32, bytes: f64) -> HashHistogram {
        let mut h = HashHistogram::equal(lo, hi, n);
        for b in &mut h.buckets {
            b.bytes = bytes;
        }
        h
    }

    fn partitions(h: &HashHistogram, p: &SplitPlan) {
        // Children partition the parent exactly: increasing points strictly
        // inside the range.
        let mut prev = u128::from(h.lo);
        for x in &p.points {
            assert!(u128::from(*x) > prev, "{p:?}");
            prev = u128::from(*x);
        }
        assert!(prev < hi128(h.hi));
        assert_eq!(p.shares.len(), p.points.len() + 1);
    }

    #[test]
    fn a_flat_histogram_splits_evenly() {
        let h = flat(0, None, 64, 1.0);
        let p = choose_split(&h, Weight::Bytes, 2).unwrap();
        partitions(&h, &p);
        assert_eq!(p.points, vec![1 << 63]);
        let p = choose_split(&h, Weight::Bytes, 3).unwrap();
        partitions(&h, &p);
        assert!(
            p.shares.iter().all(|s| (s - 1.0 / 3.0).abs() < 0.02),
            "{p:?}"
        );
        // A shard [0x8000.., 0xc000..) in thirds.
        let h = flat(0x8000_0000_0000_0000, Some(0xc000_0000_0000_0000), 64, 1.0);
        let p = choose_split(&h, Weight::Bytes, 3).unwrap();
        partitions(&h, &p);
        assert_eq!(p.points[0] >> 48, 0x9555);
        assert_eq!(p.points[1] >> 48, 0xaaaa);
    }

    #[test]
    fn a_skewed_histogram_splits_by_weight() {
        let mut h = flat(0, None, 64, 1.0);
        for b in &mut h.buckets[..8] {
            b.bytes = 20.0; // 160 of 216 in the first eighth
        }
        let p = choose_split(&h, Weight::Bytes, 2).unwrap();
        partitions(&h, &p);
        assert!(p.points[0] < 1 << 61, "{p:?}");
        assert!(p.shares.iter().all(|s| (s - 0.5).abs() < 0.05), "{p:?}");
    }

    #[test]
    fn one_heavy_hash_is_isolated() {
        let mut h = flat(0, None, 64, 1.0);
        let hot = 0x313c_a5e2_98b7_eb08_u64;
        let i = h.bucket_of(hot);
        h.buckets[i].bytes += 100.0;
        h.heavy = vec![(hot, 100.0)];
        let p = choose_split(&h, Weight::Bytes, 2).unwrap();
        partitions(&h, &p);
        assert_eq!(p.points, vec![hot, hot + 1]);
        assert_eq!(p.hot_key, Some(hot));
        assert!(p.shares[1] > 0.5);
        // A heavy hash at the range's start drops the empty left child.
        let mut h = flat(hot, None, 64, 1.0);
        h.buckets[0].bytes += 1000.0;
        h.heavy = vec![(hot, 1000.0)];
        let p = choose_split(&h, Weight::Bytes, 2).unwrap();
        assert_eq!(p.points, vec![hot + 1]);
    }

    #[test]
    fn two_heavy_hashes_and_a_heavy_hash_on_an_edge_stay_whole() {
        let mut h = flat(0, None, 64, 1.0);
        let a = (1u64 << 63) + 12_345; // in the bucket the median falls in
        let b = 3u64 << 62;
        let i = h.bucket_of(a);
        h.buckets[i].bytes += 30.0;
        let i = h.bucket_of(b);
        h.buckets[i].bytes += 30.0;
        h.heavy = vec![(a, 30.0), (b, 30.0)];
        let p = choose_split(&h, Weight::Bytes, 2).unwrap();
        partitions(&h, &p);
        assert!(p.points[0] == a || p.points[0] == a + 1, "{p:?}");
        assert_eq!(p.hot_key, None);
        // A heavy hash exactly on a bucket edge.
        let mut h = flat(0, None, 64, 1.0);
        let e = h.buckets[32].lo;
        h.buckets[32].bytes += 20.0;
        h.heavy = vec![(e, 20.0)];
        let p = choose_split(&h, Weight::Bytes, 2).unwrap();
        partitions(&h, &p);
        assert!(p.points[0] == e || p.points[0] == e + 1, "{p:?}");
    }

    #[test]
    fn single_hashes_and_lopsided_children_are_refused() {
        let h = flat(5, Some(6), 1, 1.0);
        assert_eq!(choose_split(&h, Weight::Bytes, 2), Err(NoSplit::SingleHash));
        // Everything in one bucket of a two-bucket histogram.
        let mut h = HashHistogram::equal(0, Some(2), 2);
        h.buckets[0].bytes = 100.0;
        assert_eq!(choose_split(&h, Weight::Bytes, 2), Err(NoSplit::Unbalanced));
        // Empty: equal widths.
        let h = HashHistogram::equal(0, None, 64);
        let p = choose_split(&h, Weight::Bytes, 4).unwrap();
        assert_eq!(p.points, vec![1 << 62, 1 << 63, 3 << 62]);
    }
}
