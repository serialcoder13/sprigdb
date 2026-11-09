//! Write sketches (chapter 20, "Write sketches" and "Load histogram").
//!
//! Each gateway keeps, per shard it writes to and per minute, the shard's
//! write count, 64 equal-width hash buckets of the writes whose routing
//! hash it knows, and a Space-Saving top-16 of those hashes
//! ([`WriteSketch`]). Sketches merge: the same shard and minute from
//! several gateways, or several minutes of one shard, add up bucket by
//! bucket, and the top-16 lists merge with Space-Saving's error bounds.
//!
//! - **Gateway side:** [`WriteSketches`] accumulates by minute and hands
//!   the completed minutes of the last [`REPORT_MINUTES`] to the load
//!   report ([`crate::load::LoadReport::sketches`]), at most
//!   [`MAX_SKETCHES_PER_MINUTE`] shards a minute (the most written), with
//!   every cluster's write total a minute
//!   ([`crate::load::LoadReport::cluster_writes`]).
//! - **Controller side:** [`SketchCollector`] takes each (gateway, minute)
//!   once, keeps per-minute totals for [`TOTALS_RETAIN_MINUTES`] and merged
//!   sketches for [`SKETCH_RETAIN_MINUTES`], and answers what the
//!   triggers need: a shard's share of its cluster's writes per minute and
//!   its write histogram over a window.
//!
//! **What counts as a write** *(choice)*: every row the router routes by
//! its key counts once, with its hash (each row of a multi-row `INSERT`,
//! each key value of an `UPDATE` or `DELETE` by key); a write the router
//! can't key (a scatter `UPDATE`, a reference-table write, a statement on
//! unsharded tables) counts once per shard it runs on, or once on the
//! session's cluster, without a hash. Buckets and the top-16 hold hashed
//! writes only; `writes` holds both, and so do the cluster totals.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::ids::ClusterId;

use super::split::{Bucket, HashHistogram};
use crate::load::LoadReport;
use crate::model::ShardKey;

/// Equal-width hash buckets per shard and minute.
pub const BUCKETS: u32 = 64;
/// Hashes the Space-Saving summary tracks.
pub const TOP_K: usize = 16;
/// Completed minutes in each gateway report: a report a controller misses
/// is covered by the next ones.
pub const REPORT_MINUTES: u64 = 3;
/// Shards per minute in a report (the most written).
pub const MAX_SKETCHES_PER_MINUTE: usize = 64;
/// Shards per minute a gateway keeps in memory; writes to more count only
/// toward their cluster's total.
pub const MAX_SHARDS_PER_MINUTE: usize = 4096;
/// How long after a minute ends the controller treats it as complete
/// (gateways write their report within `LOAD_PERIOD`, 5 s).
pub const SETTLE_MS: u64 = 45_000;
/// Minutes of per-shard and per-cluster totals the controller keeps (the
/// longest `sharding.split_sustain_minutes` that can fire).
pub const TOTALS_RETAIN_MINUTES: u64 = 240;
/// Minutes of merged sketches the controller keeps (the longest window a
/// load histogram covers).
pub const SKETCH_RETAIN_MINUTES: u64 = 60;

const MINUTE_MS: u64 = 60_000;

/// A hash's `(count, error)` on each side of a merge.
type Sides = (Option<(u64, u64)>, Option<(u64, u64)>);

/// The minute (since the Unix epoch) of `ms`.
#[must_use]
pub const fn minute_of(ms: u64) -> u64 {
    ms / MINUTE_MS
}

/// A shard's hash range `[lo, hi)`; `hi` `None` is 2^64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashRange {
    /// First hash.
    pub lo: u64,
    /// One past the last; `None` is 2^64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hi: Option<u64>,
}

impl HashRange {
    /// `[lo, hi)`.
    #[must_use]
    pub const fn new(lo: u64, hi: Option<u64>) -> Self {
        Self { lo, hi }
    }

    fn width(self) -> u128 {
        self.hi
            .map_or(1u128 << 64, u128::from)
            .saturating_sub(u128::from(self.lo))
    }

    /// Buckets over the range: 64, fewer for a range of fewer hashes, 0
    /// for an empty one.
    #[must_use]
    pub fn buckets(self) -> usize {
        usize::try_from(self.width().min(u128::from(BUCKETS))).unwrap_or(0)
    }

    fn bucket_width(self) -> u128 {
        self.width() / u128::try_from(self.buckets().max(1)).unwrap_or(1)
    }

    /// The bucket of hash `h`; `None` outside the range. Buckets are
    /// `width / n` hashes wide; the last also takes the remainder.
    #[must_use]
    pub fn bucket_of(self, h: u64) -> Option<usize> {
        let n = self.buckets();
        if n == 0 || h < self.lo || self.hi.is_some_and(|hi| h >= hi) {
            return None;
        }
        let i = u128::from(h - self.lo) / self.bucket_width();
        Some(usize::try_from(i).unwrap_or(usize::MAX).min(n - 1))
    }

    /// The first hash of bucket `i`.
    #[must_use]
    pub fn bucket_lo(self, i: usize) -> u64 {
        let i = u128::try_from(i).unwrap_or(0);
        u64::try_from(u128::from(self.lo) + i * self.bucket_width()).unwrap_or(u64::MAX)
    }
}

/// A Space-Saving summary of the [`TOP_K`] most frequent hashes: entries
/// `(hash, count, error)`, where `count - error <= true count <= count`,
/// and every hash written more than `N / TOP_K` times of `N` is listed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SpaceSaving(Vec<(u64, u64, u64)>);

impl SpaceSaving {
    /// Whether nothing was offered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The entries `(hash, count, error)`, unordered.
    #[must_use]
    pub fn entries(&self) -> &[(u64, u64, u64)] {
        &self.0
    }

    /// The most a hash not listed can have been written.
    #[must_use]
    pub fn floor(&self) -> u64 {
        if self.0.len() < TOP_K {
            0
        } else {
            self.0.iter().map(|e| e.1).min().unwrap_or(0)
        }
    }

    /// Counts `n` writes of `h`.
    pub fn offer(&mut self, h: u64, n: u64) {
        if let Some(e) = self.0.iter_mut().find(|e| e.0 == h) {
            e.1 += n;
            return;
        }
        if self.0.len() < TOP_K {
            self.0.push((h, n, 0));
            return;
        }
        // Replace the smallest count (ties: the largest hash, so the
        // choice is deterministic).
        if let Some(e) = self
            .0
            .iter_mut()
            .min_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
        {
            let m = e.1;
            *e = (h, m + n, m);
        }
    }

    /// Adds `o`'s writes (the mergeable-summaries rule: a hash missing
    /// from one side counts that side's floor as both count and error, so
    /// the bounds hold for the union).
    pub fn merge(&mut self, o: &Self) {
        let (fa, fb) = (self.floor(), o.floor());
        let mut all: BTreeMap<u64, Sides> = BTreeMap::new();
        for &(h, c, e) in &self.0 {
            all.entry(h).or_default().0 = Some((c, e));
        }
        for &(h, c, e) in &o.0 {
            all.entry(h).or_default().1 = Some((c, e));
        }
        let mut v: Vec<(u64, u64, u64)> = all
            .into_iter()
            .map(|(h, (a, b))| {
                let (ca, ea) = a.unwrap_or((fa, fa));
                let (cb, eb) = b.unwrap_or((fb, fb));
                (h, ca + cb, ea + eb)
            })
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v.truncate(TOP_K);
        self.0 = v;
    }

    /// Hashes with the writes they certainly had (`count - error`), most
    /// first, zeros left out.
    #[must_use]
    pub fn guaranteed(&self) -> Vec<(u64, u64)> {
        let mut v: Vec<(u64, u64)> = self
            .0
            .iter()
            .map(|&(h, c, e)| (h, c.saturating_sub(e)))
            .filter(|x| x.1 > 0)
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v
    }
}

/// One shard's writes in one minute (or merged over several), from one
/// gateway or merged over several.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteSketch {
    /// The shard.
    pub shard: ShardKey,
    /// Its cluster.
    pub cluster: ClusterId,
    /// The minute since the Unix epoch (of the earliest minute merged).
    pub minute: u64,
    /// Writes, hashed or not.
    pub writes: u64,
    /// The shard's range, once a hashed write was counted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<HashRange>,
    /// Hashed writes per bucket of `range` ([`HashRange::bucket_of`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<u64>,
    /// The most written hashes.
    #[serde(default, skip_serializing_if = "SpaceSaving::is_empty")]
    pub top: SpaceSaving,
}

impl WriteSketch {
    /// An empty sketch.
    #[must_use]
    pub fn new(shard: ShardKey, cluster: ClusterId, minute: u64) -> Self {
        Self {
            shard,
            cluster,
            minute,
            writes: 0,
            range: None,
            buckets: Vec::new(),
            top: SpaceSaving::default(),
        }
    }

    /// Counts one write; with `keyed`, the shard's range and the row's
    /// routing hash. A hash outside the range (or a range other than the
    /// one already counted) counts as an unhashed write.
    pub fn record(&mut self, keyed: Option<(HashRange, u64)>) {
        self.writes += 1;
        let Some((range, h)) = keyed else { return };
        if self.range.is_none() {
            self.range = Some(range);
            self.buckets = vec![0; range.buckets()];
        }
        if self.range != Some(range) {
            return;
        }
        if let Some(b) = range.bucket_of(h).and_then(|i| self.buckets.get_mut(i)) {
            *b += 1;
            self.top.offer(h, 1);
        }
    }

    /// Hashed writes.
    #[must_use]
    pub fn hashed(&self) -> u64 {
        self.buckets.iter().sum()
    }

    /// Adds `o` (the same shard; another gateway or another minute).
    pub fn merge(&mut self, o: &Self) {
        self.writes += o.writes;
        self.minute = self.minute.min(o.minute);
        match (self.range, o.range) {
            (None, Some(_)) => {
                self.range = o.range;
                self.buckets.clone_from(&o.buckets);
                self.top = o.top.clone();
            }
            (Some(a), Some(b)) if a == b && self.buckets.len() == o.buckets.len() => {
                for (x, y) in self.buckets.iter_mut().zip(&o.buckets) {
                    *x += y;
                }
                self.top.merge(&o.top);
            }
            _ => {}
        }
    }

    /// The load histogram (chapter 20's `write_histogram`): buckets of
    /// writes over the range, heavy hashes with the writes they certainly
    /// had. `None` without hashed writes.
    #[must_use]
    pub fn histogram(&self) -> Option<HashHistogram> {
        let range = self.range?;
        if self.hashed() == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        let buckets = self
            .buckets
            .iter()
            .enumerate()
            .map(|(i, w)| Bucket {
                lo: range.bucket_lo(i),
                bytes: 0.0,
                writes: *w as f64,
            })
            .collect();
        #[allow(clippy::cast_precision_loss)]
        let heavy = self
            .top
            .guaranteed()
            .into_iter()
            .filter(|(h, _)| range.bucket_of(*h).is_some())
            .map(|(h, w)| (h, w as f64))
            .collect();
        Some(HashHistogram {
            lo: range.lo,
            hi: range.hi,
            buckets,
            heavy,
        })
    }
}

/// One cluster's writes in one minute, as one gateway sent them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterWrites {
    /// The cluster.
    pub cluster: ClusterId,
    /// The minute since the Unix epoch.
    pub minute: u64,
    /// Writes (module docs).
    pub writes: u64,
}

#[derive(Debug, Default)]
struct Minutes {
    shards: BTreeMap<(u64, ShardKey), WriteSketch>,
    per_minute: BTreeMap<u64, usize>,
    clusters: BTreeMap<(u64, ClusterId), u64>,
}

impl Minutes {
    fn prune(&mut self, before: u64) {
        self.shards.retain(|(m, _), _| *m >= before);
        self.per_minute.retain(|m, _| *m >= before);
        self.clusters.retain(|(m, _), _| *m >= before);
    }
}

/// The gateway side: sketches by minute (module docs).
#[derive(Debug, Default)]
pub struct WriteSketches {
    inner: Mutex<Minutes>,
}

impl WriteSketches {
    /// Empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts writes at `at_ms`: per shard, its cluster and, when the
    /// router keyed the row, the shard's range and the row's hash.
    pub fn record<'a>(
        &self,
        at_ms: u64,
        writes: impl IntoIterator<Item = (ShardKey, &'a ClusterId, Option<(HashRange, u64)>)>,
    ) {
        let m = minute_of(at_ms);
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if g.per_minute
            .keys()
            .next_back()
            .is_some_and(|last| *last < m)
        {
            g.prune(m.saturating_sub(REPORT_MINUTES));
        }
        let g = &mut *g;
        for (shard, cluster, keyed) in writes {
            *g.clusters.entry((m, cluster.clone())).or_default() += 1;
            let n = g.per_minute.entry(m).or_default();
            match g.shards.entry((m, shard)) {
                std::collections::btree_map::Entry::Occupied(mut e) => e.get_mut().record(keyed),
                std::collections::btree_map::Entry::Vacant(e) => {
                    if *n < MAX_SHARDS_PER_MINUTE {
                        *n += 1;
                        e.insert(WriteSketch::new(shard, cluster.clone(), m))
                            .record(keyed);
                    }
                }
            }
        }
    }

    /// Counts `n` writes on `cluster` outside any shard (unsharded tables).
    pub fn record_cluster(&self, at_ms: u64, cluster: &ClusterId, n: u64) {
        let m = minute_of(at_ms);
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        *g.clusters.entry((m, cluster.clone())).or_default() += n;
        g.per_minute.entry(m).or_default();
    }

    /// The completed minutes of the last [`REPORT_MINUTES`] before
    /// `now_ms`: the most written [`MAX_SKETCHES_PER_MINUTE`] shards of
    /// each, and every cluster's total. Older minutes are dropped.
    #[must_use]
    pub fn report(&self, now_ms: u64) -> (Vec<WriteSketch>, Vec<ClusterWrites>) {
        let now = minute_of(now_ms);
        let from = now.saturating_sub(REPORT_MINUTES);
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        g.prune(from);
        let mut sketches = Vec::new();
        for m in from..now {
            let mut v: Vec<&WriteSketch> = g
                .shards
                .range((m, ShardKey::new(0, 0, 0))..)
                .take_while(|((x, _), _)| *x == m)
                .map(|(_, s)| s)
                .collect();
            v.sort_by(|a, b| b.writes.cmp(&a.writes).then(a.shard.cmp(&b.shard)));
            sketches.extend(v.into_iter().take(MAX_SKETCHES_PER_MINUTE).cloned());
        }
        let clusters = g
            .clusters
            .iter()
            .filter(|((m, _), _)| *m < now)
            .map(|((m, c), w)| ClusterWrites {
                cluster: c.clone(),
                minute: *m,
                writes: *w,
            })
            .collect();
        (sketches, clusters)
    }

    /// The newest completed minute a report at `now_ms` would carry, if
    /// any write was counted in the reported minutes.
    #[must_use]
    pub fn newest_reported(&self, now_ms: u64) -> Option<u64> {
        let now = minute_of(now_ms);
        let g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        g.per_minute
            .keys()
            .rev()
            .find(|m| **m < now && **m >= now.saturating_sub(REPORT_MINUTES))
            .copied()
    }
}

/// The controller side: every gateway's sketches, by shard and minute
/// (module docs).
#[derive(Debug, Default)]
pub struct SketchCollector {
    /// (minute, gateway) already taken: a minute is final once reported.
    seen: BTreeSet<(u64, String)>,
    totals: BTreeMap<ShardKey, BTreeMap<u64, u64>>,
    sketches: BTreeMap<ShardKey, BTreeMap<u64, WriteSketch>>,
    clusters: BTreeMap<ClusterId, BTreeMap<u64, u64>>,
    cluster_of: BTreeMap<ShardKey, ClusterId>,
    /// The first minute any report covered.
    first: Option<u64>,
}

impl SketchCollector {
    /// Empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the minutes of `r` not taken from its gateway before.
    pub fn ingest(&mut self, r: &LoadReport) {
        let minutes: BTreeSet<u64> = r
            .sketches
            .iter()
            .map(|s| s.minute)
            .chain(r.cluster_writes.iter().map(|c| c.minute))
            .collect();
        let floor = minute_of(r.at_ms).saturating_sub(TOTALS_RETAIN_MINUTES);
        let fresh: BTreeSet<u64> = minutes
            .into_iter()
            .filter(|m| *m >= floor && self.seen.insert((*m, r.gateway.clone())))
            .collect();
        if let Some(m) = fresh.first() {
            self.first = Some(self.first.map_or(*m, |f| f.min(*m)));
        }
        for s in r.sketches.iter().filter(|s| fresh.contains(&s.minute)) {
            *self
                .totals
                .entry(s.shard)
                .or_default()
                .entry(s.minute)
                .or_default() += s.writes;
            self.cluster_of.insert(s.shard, s.cluster.clone());
            self.sketches
                .entry(s.shard)
                .or_default()
                .entry(s.minute)
                .and_modify(|x| x.merge(s))
                .or_insert_with(|| s.clone());
        }
        for c in r
            .cluster_writes
            .iter()
            .filter(|c| fresh.contains(&c.minute))
        {
            *self
                .clusters
                .entry(c.cluster.clone())
                .or_default()
                .entry(c.minute)
                .or_default() += c.writes;
        }
    }

    /// Drops what is older than the retention at `now_ms`.
    pub fn prune(&mut self, now_ms: u64) {
        let now = minute_of(now_ms);
        let totals = now.saturating_sub(TOTALS_RETAIN_MINUTES);
        let sketches = now.saturating_sub(SKETCH_RETAIN_MINUTES);
        self.seen.retain(|(m, _)| *m >= totals);
        for v in self.totals.values_mut() {
            v.retain(|m, _| *m >= totals);
        }
        self.totals.retain(|_, v| !v.is_empty());
        for v in self.sketches.values_mut() {
            v.retain(|m, _| *m >= sketches);
        }
        self.sketches.retain(|_, v| !v.is_empty());
        for v in self.clusters.values_mut() {
            v.retain(|m, _| *m >= totals);
        }
        self.clusters.retain(|_, v| !v.is_empty());
        self.cluster_of.retain(|k, _| self.totals.contains_key(k));
        if let Some(f) = self.first.as_mut() {
            *f = (*f).max(totals);
        }
    }

    /// The last minute treated as complete at `now_ms` ([`SETTLE_MS`]).
    #[must_use]
    pub fn settled(now_ms: u64) -> Option<u64> {
        minute_of(now_ms.saturating_sub(SETTLE_MS)).checked_sub(1)
    }

    /// The cluster a shard's writes went to.
    #[must_use]
    pub fn cluster_of(&self, shard: &ShardKey) -> Option<&ClusterId> {
        self.cluster_of.get(shard)
    }

    /// `shard`'s share of its cluster's writes per minute, oldest first:
    /// every complete minute since the collector's first data, after
    /// `since_ms`'s minute when given. A minute without writes on the
    /// cluster is 0.
    #[must_use]
    pub fn write_shares(&self, shard: &ShardKey, since_ms: Option<u64>, now_ms: u64) -> Vec<f64> {
        let (Some(first), Some(last)) = (self.first, Self::settled(now_ms)) else {
            return Vec::new();
        };
        let Some(cluster) = self.cluster_of.get(shard) else {
            return Vec::new();
        };
        let start = since_ms.map_or(first, |s| first.max(minute_of(s) + 1));
        if start > last {
            return Vec::new();
        }
        let empty = BTreeMap::new();
        let mine = self.totals.get(shard).unwrap_or(&empty);
        let all = self.clusters.get(cluster).unwrap_or(&empty);
        (start..=last)
            .map(|m| {
                let c = all.get(&m).copied().unwrap_or(0);
                let w = mine.get(&m).copied().unwrap_or(0).min(c);
                #[allow(clippy::cast_precision_loss)]
                let share = if c == 0 { 0.0 } else { w as f64 / c as f64 };
                share
            })
            .collect()
    }

    /// `shard`'s load histogram over the complete minutes of the last
    /// `window` (at most [`SKETCH_RETAIN_MINUTES`]): the sum of every
    /// gateway's sketches, heavy hashes from the merged top-16. `None`
    /// without hashed writes, or when the sketches' range isn't `range`.
    #[must_use]
    pub fn write_histogram(
        &self,
        shard: &ShardKey,
        range: HashRange,
        window: Duration,
        now_ms: u64,
    ) -> Option<HashHistogram> {
        let last = Self::settled(now_ms)?;
        let minutes = (window.as_secs() / 60).max(1);
        let from = (last + 1).saturating_sub(minutes);
        let mut merged: Option<WriteSketch> = None;
        for s in self.sketches.get(shard)?.range(from..=last).map(|(_, s)| s) {
            match merged.as_mut() {
                Some(x) => x.merge(s),
                None => merged = Some(s.clone()),
            }
        }
        let merged = merged?;
        if merged.range != Some(range) {
            return None;
        }
        merged.histogram()
    }

    /// `cluster`'s writes per second over the complete minutes of the last
    /// `window`, as the gateways counted them.
    #[must_use]
    pub fn cluster_writes_per_sec(
        &self,
        cluster: &ClusterId,
        window: Duration,
        now_ms: u64,
    ) -> f64 {
        let Some(last) = Self::settled(now_ms) else {
            return 0.0;
        };
        let minutes = (window.as_secs() / 60).max(1);
        let from = (last + 1).saturating_sub(minutes);
        let sum: u64 = self
            .clusters
            .get(cluster)
            .map_or(0, |v| v.range(from..=last).map(|(_, w)| *w).sum());
        #[allow(clippy::cast_precision_loss)]
        let r = sum as f64 / (minutes * 60) as f64;
        r
    }
}

#[cfg(test)]
mod tests;
