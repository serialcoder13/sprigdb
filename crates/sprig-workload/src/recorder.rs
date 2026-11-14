//! The recorder: in-memory aggregation per (tenant, fingerprint).
//!
//! The gateway calls [`Recorder::plan`] before sending a statement and
//! [`Recorder::record`] after its result completes. `record` costs one hash,
//! one lock of one of 64 shards and a few additions; first sights, bucket
//! changes and evictions take slower paths.
//!
//! Each entry holds the counters of one `workload_stats` row for the
//! current bucket (`workload.bucket_minutes`). When a record arrives for a
//! later bucket, the entry's finished bucket moves to the **closed outbox**
//! and the entry starts again. The flusher ([`crate::flush`]) writes
//! [`Recorder::snapshot`] (cumulative values of open buckets, written with
//! `REPLACE`) and drains [`Recorder::drain_closed`] and
//! [`Recorder::drain_new`] (first sightings for `INSERT IGNORE INTO
//! workload_fingerprints`).
//!
//! **Caps.**
//!
//! - `workload.max_memory_mb` for the whole recorder: past it, the coldest
//!   entries (least recently recorded) are evicted into the closed outbox
//!   until memory is back under 90% of the cap. A fingerprint that returns
//!   in the same bucket starts a new `part`, so no counts are overwritten.
//! - A tenant's entries may use at most `max_tenant_memory_pct` of that
//!   cap; past it, that tenant's coldest entries are evicted, so one tenant
//!   can't push the others out.
//! - `workload.max_fingerprints_per_tenant`: once a tenant has that many
//!   fingerprints on this gateway, new ones are counted under the reserved
//!   "other" fingerprint of their kind.
//! - The closed outbox counts toward memory; when the flusher falls behind
//!   past the cap, the oldest closed rows are dropped and counted.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use sprig_common::ids::{DbId, TenantId};
use sprig_common::time::Clock;

use crate::fingerprint::{Caller, Fingerprint, FingerprintId, FpFlags, FpKind, FrontEnd, Reserved};
use crate::histogram::Histogram;
use crate::shape::Shape;

/// The recorder's settings: a snapshot of the `workload.*` keys, resolved by
/// the gateway.
#[derive(Debug, Clone, PartialEq)]
pub struct RecorderConfig {
    /// `workload.bucket_minutes` (60).
    pub bucket_minutes: u32,
    /// `workload.max_memory_mb` (128), in bytes.
    pub max_memory_bytes: usize,
    /// The share of `max_memory_bytes` one tenant's entries may use, in
    /// percent (25).
    pub max_tenant_memory_pct: u8,
    /// `workload.max_fingerprints_per_tenant` (10000).
    pub max_fingerprints_per_tenant: u32,
    /// `workload.watch_p95_ms` (50).
    pub watch_p95_ms: u64,
    /// `workload.examined_sample_rate` (0.02).
    pub examined_sample_rate: f64,
    /// `workload.full_probe_ms` (100).
    pub full_probe_ms: u64,
    /// `workload.capture_samples` (on).
    pub capture_samples: bool,
    /// `workload.sample_rate` (0.01).
    pub sample_rate: f64,
    /// `workload.samples_per_bucket` (3): a watched fingerprint's first
    /// executions in a bucket are captured until this many were asked for,
    /// whatever the rate *(choice: a floor, as probes have one, so a
    /// fingerprint that runs 50 times a day still gets samples to replay)*.
    pub samples_per_bucket: u32,
    /// `auto_index.scan_min_examined` (1,000,000): a probed run that
    /// examined at least this many rows...
    pub scan_min_examined: u64,
    /// `auto_index.scan_max_returned_ratio` (0.01): ...and returned (or
    /// affected) at most this share of them is a scan run.
    pub scan_max_returned_ratio: f64,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            bucket_minutes: 60,
            max_memory_bytes: 128 << 20,
            max_tenant_memory_pct: 25,
            max_fingerprints_per_tenant: 10_000,
            watch_p95_ms: 50,
            examined_sample_rate: 0.02,
            full_probe_ms: 100,
            capture_samples: true,
            sample_rate: 0.01,
            samples_per_bucket: 3,
            scan_min_examined: 1_000_000,
            scan_max_returned_ratio: 0.01,
        }
    }
}

/// What one execution did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecSummary {
    /// Time from sending the statement to the end of its result.
    pub latency_us: u64,
    /// It ended in an error.
    pub error: bool,
    /// Rows sent to the client.
    pub rows_returned: u64,
    /// Rows changed.
    pub rows_affected: u64,
    /// Shards it ran on.
    pub shards: u32,
    /// Clusters it ran on.
    pub clusters: u32,
    /// It was sent to every shard of its group.
    pub scatter: bool,
    /// The longest `IN` list (0 to take the fingerprint's).
    pub in_list_max: u32,
    /// Rows examined, when a `Handler_read_*` probe ran.
    pub examined: Option<u64>,
    /// `xxh3_64(gateway id, connection id)`; for stateless calls the
    /// authenticated user plus the client address.
    pub session: u64,
}

/// What [`Recorder::plan`] asks the gateway to do around one execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProbePlan {
    /// Read `Handler_read_*` before and after.
    pub examined: bool,
    /// Capture the parameter values.
    pub capture_params: bool,
}

/// Up to 8 distinct session hashes (`workload_stats.scan_sessions`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionSet {
    ids: Vec<u64>,
}

impl SessionSet {
    /// The most hashes kept.
    pub const MAX: usize = 8;

    /// Adds a hash if there is room.
    pub fn insert(&mut self, s: u64) {
        if self.ids.len() < Self::MAX && !self.ids.contains(&s) {
            self.ids.push(s);
        }
    }

    /// The union, still capped at 8.
    pub fn merge(&mut self, other: &SessionSet) {
        for s in &other.ids {
            self.insert(*s);
        }
    }

    /// How many distinct sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// The hashes.
    #[must_use]
    pub fn as_slice(&self) -> &[u64] {
        &self.ids
    }

    /// The stored form: 8 bytes little-endian per hash.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.ids.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    /// Reads the stored form; trailing partial hashes are ignored.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        let mut s = Self::default();
        for c in bytes.chunks_exact(8) {
            let mut b = [0u8; 8];
            b.copy_from_slice(c);
            s.insert(u64::from_le_bytes(b));
        }
        s
    }
}

/// The counters of one `workload_stats` row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stats {
    /// Executions.
    pub exec_count: u64,
    /// Executions that ended in an error.
    pub error_count: u64,
    /// Sum of latencies.
    pub total_us: u64,
    /// Longest latency.
    pub max_us: u64,
    /// Latency histogram.
    pub latency_hist: Histogram,
    /// Rows sent.
    pub rows_returned: u64,
    /// Rows changed.
    pub rows_affected: u64,
    /// Probed executions.
    pub examined_samples: u64,
    /// Rows examined over the probed executions.
    pub rows_examined: u64,
    /// Rows returned by the probed executions.
    pub returned_sampled: u64,
    /// Sum of shards per execution.
    pub shards_sum: u64,
    /// Most shards of one execution.
    pub shards_max: u32,
    /// Executions sent to every shard.
    pub scatter_count: u64,
    /// Executions on more than one cluster.
    pub cross_cluster: u64,
    /// Longest `IN` list.
    pub in_list_max: u32,
    /// Probed runs that were large scans.
    pub scan_runs: u64,
    /// Up to 8 sessions of those runs.
    pub scan_sessions: SessionSet,
}

impl Stats {
    /// Adds another row's counters (the rollup's merge): sums, maxima,
    /// merged histograms and session sets.
    pub fn merge(&mut self, o: &Stats) {
        self.exec_count += o.exec_count;
        self.error_count += o.error_count;
        self.total_us += o.total_us;
        self.max_us = self.max_us.max(o.max_us);
        self.latency_hist.merge(&o.latency_hist);
        self.rows_returned += o.rows_returned;
        self.rows_affected += o.rows_affected;
        self.examined_samples += o.examined_samples;
        self.rows_examined += o.rows_examined;
        self.returned_sampled += o.returned_sampled;
        self.shards_sum += o.shards_sum;
        self.shards_max = self.shards_max.max(o.shards_max);
        self.scatter_count += o.scatter_count;
        self.cross_cluster += o.cross_cluster;
        self.in_list_max = self.in_list_max.max(o.in_list_max);
        self.scan_runs += o.scan_runs;
        self.scan_sessions.merge(&o.scan_sessions);
    }

    /// Mean latency in microseconds.
    #[must_use]
    pub fn mean_us(&self) -> u64 {
        self.total_us.checked_div(self.exec_count).unwrap_or(0)
    }
}

/// What every execution of a fingerprint shares, kept for the first-sight
/// row.
#[derive(Debug, Clone)]
pub struct FpMeta {
    /// The id.
    pub id: FingerprintId,
    /// The kind.
    pub kind: FpKind,
    /// The normalized text.
    pub text: Arc<str>,
    /// The shape.
    pub shape: Arc<Shape>,
    /// The front end of its first sighting.
    pub front_end: FrontEnd,
    /// The logic function, if any.
    pub caller: Option<Caller>,
    /// The logical database part of the id.
    pub db_id: DbId,
    /// Flags.
    pub flags: FpFlags,
}

impl FpMeta {
    fn of(fp: &Fingerprint) -> Self {
        Self {
            id: fp.id,
            kind: fp.kind,
            text: Arc::clone(&fp.text),
            shape: Arc::clone(&fp.shape),
            front_end: fp.front_end,
            caller: fp.caller,
            db_id: fp.db_id,
            flags: fp.flags,
        }
    }

    fn other(fp: &Fingerprint) -> Self {
        Self {
            id: Reserved::Other.id(fp.kind),
            kind: fp.kind,
            text: Arc::from(Reserved::Other.text()),
            shape: Arc::new(Shape::empty(crate::shape::StmtType::Other)),
            front_end: fp.front_end,
            caller: None,
            db_id: DbId(0),
            flags: FpFlags {
                other: true,
                ..FpFlags::default()
            },
        }
    }

    fn heap_bytes(&self) -> usize {
        // The text and shape are shared with the fingerprinter's cache;
        // count them once here as an estimate.
        self.text.len() + 256 + self.shape.predicates.len() * 64 + self.shape.tables.len() * 64
    }
}

/// A first sighting, for `INSERT IGNORE INTO workload_fingerprints`.
#[derive(Debug, Clone)]
pub struct NewFingerprint {
    /// The tenant.
    pub tenant: TenantId,
    /// What to insert.
    pub meta: Arc<FpMeta>,
    /// When it was first seen on this gateway.
    pub first_seen: SystemTime,
}

/// One `workload_stats` row for the flusher: this gateway's cumulative
/// values for (fingerprint, bucket, part).
#[derive(Debug, Clone)]
pub struct FlushRow {
    /// The tenant.
    pub tenant: TenantId,
    /// The fingerprint.
    pub fingerprint: FingerprintId,
    /// The bucket's start, UTC, in seconds since the epoch.
    pub bucket_start: u64,
    /// The part (0, then 1, 2, ... after evictions within the bucket).
    pub part: u16,
    /// The counters.
    pub stats: Stats,
    /// When it was last recorded.
    pub last_seen: SystemTime,
}

/// One fingerprint's totals on this gateway since it was first counted or
/// since the tenant's last reset: what `performance_schema`'s
/// `events_statements_summary_by_digest` shows on port 3306 (the MySQL
/// 8.4 port). Unlike [`Stats`], it doesn't start again with
/// each bucket; it goes when the entry is evicted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Digest {
    /// Executions.
    pub exec_count: u64,
    /// Executions that ended in an error.
    pub error_count: u64,
    /// Sum of latencies.
    pub total_us: u64,
    /// Shortest latency.
    pub min_us: u64,
    /// Longest latency.
    pub max_us: u64,
    /// Latency histogram, for the quantiles.
    pub latency_hist: Histogram,
    /// Rows sent.
    pub rows_returned: u64,
    /// Rows changed.
    pub rows_affected: u64,
    /// Rows examined over the probed executions.
    pub rows_examined: u64,
    /// First counted (since the last reset).
    pub first_seen: Option<SystemTime>,
    /// Last counted.
    pub last_seen: Option<SystemTime>,
}

impl Digest {
    fn add(&mut self, ex: &ExecSummary, wall: SystemTime) {
        self.min_us = if self.exec_count == 0 {
            ex.latency_us
        } else {
            self.min_us.min(ex.latency_us)
        };
        self.exec_count += 1;
        self.error_count += u64::from(ex.error);
        self.total_us = self.total_us.saturating_add(ex.latency_us);
        self.max_us = self.max_us.max(ex.latency_us);
        self.latency_hist.record(ex.latency_us);
        self.rows_returned = self.rows_returned.saturating_add(ex.rows_returned);
        self.rows_affected = self.rows_affected.saturating_add(ex.rows_affected);
        if let Some(x) = ex.examined {
            self.rows_examined = self.rows_examined.saturating_add(x);
        }
        self.first_seen.get_or_insert(wall);
        self.last_seen = Some(wall);
    }
}

/// A fingerprint's [`Digest`] with what names it.
#[derive(Debug, Clone)]
pub struct DigestRow {
    /// The fingerprint.
    pub id: FingerprintId,
    /// Its normalized text.
    pub text: Arc<str>,
    /// Its logical database (`DbId(0)` for none).
    pub db_id: DbId,
    /// The totals.
    pub digest: Digest,
}

/// Counters for the workload metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecorderMetrics {
    /// `sprig_workload_fingerprints_active`.
    pub fingerprints_active: u64,
    /// `sprig_workload_memory_bytes`.
    pub memory_bytes: u64,
    /// `sprig_workload_other_total`.
    pub other_total: u64,
    /// Entries evicted under the memory caps.
    pub evicted_total: u64,
    /// Closed rows dropped because the flusher fell behind past the cap.
    pub dropped_rows_total: u64,
    /// `sprig_workload_probes_total`: plans that asked for a probe.
    pub probes_total: u64,
    /// `sprig_workload_samples_total`: plans that asked for parameters.
    pub samples_total: u64,
}

fn row_bytes(r: &FlushRow) -> usize {
    r.stats.latency_hist.heap_bytes() + std::mem::size_of::<FlushRow>()
}

/// Bytes counted for one entry besides its histogram and meta.
const ENTRY_BYTES: usize = std::mem::size_of::<Entry>() + 64;

#[derive(Debug)]
struct Entry {
    meta: Arc<FpMeta>,
    bucket_start: u64,
    part: u16,
    stats: Stats,
    /// p95 of the previous bucket, for watching.
    prev_p95_us: u64,
    /// Automatic indexing or the top-N asked for it.
    watched: bool,
    last_seen_secs: u64,
    /// Bytes this entry was counted for.
    bytes: usize,
    first_seen: SystemTime,
    /// Parameter captures asked for: (bucket, count).
    samples_in_bucket: (u64, u32),
    /// Totals since the entry began or the tenant's last reset.
    digest: Digest,
}

impl Entry {
    fn heap_bytes(&self) -> usize {
        ENTRY_BYTES
            + self.stats.latency_hist.heap_bytes()
            + self.digest.latency_hist.heap_bytes()
            + self.meta.heap_bytes()
    }

    fn hist_bytes(&self) -> usize {
        self.stats.latency_hist.heap_bytes() + self.digest.latency_hist.heap_bytes()
    }
}

#[derive(Debug, Default)]
struct TenantState {
    /// Fingerprint ids this gateway has counted for the tenant.
    known: HashSet<u64>,
    /// Watched by request (automatic indexing, top-N).
    watched: HashSet<u64>,
    /// Bytes of the tenant's entries.
    bytes: usize,
    /// (fingerprint, bucket) → next part, after evictions.
    parts: HashMap<u64, (u64, u16)>,
}

type Key = (TenantId, u64);

/// One shard, alone on its cache lines so threads locking neighbouring
/// shards don't contend.
#[repr(align(128))]
struct Padded(Mutex<HashMap<Key, Entry>>);

impl std::ops::Deref for Padded {
    type Target = Mutex<HashMap<Key, Entry>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Number of entry shards (a power of two).
const SHARDS: usize = 64;

/// Per-gateway workload recorder.
pub struct Recorder {
    /// The settings; replaced whole when a `workload.*` key changes
    /// ([`Recorder::set_config`]).
    cfg: arc_swap::ArcSwap<RecorderConfig>,
    clock: Arc<dyn Clock>,
    shards: Box<[Padded]>,
    tenants: Mutex<HashMap<TenantId, TenantState>>,
    closed: Mutex<VecDeque<FlushRow>>,
    new: Mutex<Vec<NewFingerprint>>,
    hasher: std::hash::RandomState,
    memory: AtomicUsize,
    /// Bytes of the closed outbox (also in `memory`).
    closed_bytes: AtomicUsize,
    entries: AtomicUsize,
    evicting: AtomicBool,
    other_total: AtomicU64,
    evicted_total: AtomicU64,
    dropped_rows_total: AtomicU64,
    probes_total: AtomicU64,
    samples_total: AtomicU64,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder")
            .field("cfg", &self.cfg)
            .field("entries", &self.entries.load(Ordering::Relaxed))
            .field("memory", &self.memory.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding a recorder lock leaves plain counters behind;
    // keep going with them rather than poisoning the hot path.
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A fast 64-bit mix (splitmix64's finalizer), for sampling decisions.
pub(crate) fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Whether `x` (a well-mixed value) falls under `rate`.
fn under(x: u64, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let limit = (rate * u64::MAX as f64) as u64;
    x < limit
}

impl Recorder {
    /// A recorder with `cfg`, reading time from `clock`.
    #[must_use]
    pub fn new(cfg: RecorderConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            cfg: arc_swap::ArcSwap::from_pointee(cfg),
            clock,
            shards: (0..SHARDS)
                .map(|_| Padded(Mutex::new(HashMap::new())))
                .collect(),
            tenants: Mutex::new(HashMap::new()),
            closed: Mutex::new(VecDeque::new()),
            new: Mutex::new(Vec::new()),
            hasher: std::hash::RandomState::new(),
            memory: AtomicUsize::new(0),
            closed_bytes: AtomicUsize::new(0),
            entries: AtomicUsize::new(0),
            evicting: AtomicBool::new(false),
            other_total: AtomicU64::new(0),
            evicted_total: AtomicU64::new(0),
            dropped_rows_total: AtomicU64::new(0),
            probes_total: AtomicU64::new(0),
            samples_total: AtomicU64::new(0),
        }
    }

    /// The settings now.
    #[must_use]
    pub fn config(&self) -> Arc<RecorderConfig> {
        self.cfg.load_full()
    }

    /// Replaces the settings (the `workload.*` keys apply live): the next
    /// statement plans and records with them. A new `bucket_minutes`
    /// applies from the next bucket boundary it computes; entries already
    /// open keep their bucket.
    pub fn set_config(&self, cfg: RecorderConfig) {
        if *self.cfg.load_full() != cfg {
            self.cfg.store(Arc::new(cfg));
        }
    }

    fn shard(&self, key: &Key) -> &Mutex<HashMap<Key, Entry>> {
        &self.shards[usize::try_from(self.hasher.hash_one(key)).unwrap_or(0) & (SHARDS - 1)]
    }

    fn bucket_secs(&self) -> u64 {
        u64::from(self.cfg.load().bucket_minutes.max(1)) * 60
    }

    /// (bucket start, now) in seconds since the epoch.
    fn now(&self) -> (u64, u64, SystemTime) {
        let wall = self.clock.wall();
        let secs = wall.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let b = self.bucket_secs();
        (secs - secs % b, secs, wall)
    }

    fn tenant_cap(&self) -> usize {
        self.cfg.load().max_memory_bytes / 100
            * usize::from(self.cfg.load().max_tenant_memory_pct.min(100))
    }

    /// Asks for probes and parameter capture around one execution. Only
    /// watched fingerprints are probed: p95 above `workload.watch_p95_ms`
    /// in this or the previous bucket, or watched by request
    /// ([`Recorder::watch`]). The caller must not probe statements of
    /// client multi-statement packets.
    pub fn plan(&self, tenant: TenantId, fp: &Fingerprint) -> ProbePlan {
        let key = (tenant, fp.id.0);
        let (bucket, ..) = self.now();
        let mut m = lock(self.shard(&key));
        let Some(e) = m.get_mut(&key) else {
            return ProbePlan::default();
        };
        if e.samples_in_bucket.0 != bucket {
            e.samples_in_bucket = (bucket, 0);
        }
        let cur = if e.bucket_start == bucket {
            &e.stats
        } else {
            &Stats::default()
        };
        let watch_us = self.cfg.load().watch_p95_ms.saturating_mul(1000);
        let p95 = cur.latency_hist.quantile_us(0.95).unwrap_or(0);
        let watched = e.watched || p95 > watch_us || e.prev_p95_us > watch_us;
        if !watched {
            return ProbePlan::default();
        }
        let roll = mix(fp.id.0 ^ cur.exec_count.rotate_left(17) ^ tenant.0);
        let full = cur.mean_us() >= self.cfg.load().full_probe_ms.saturating_mul(1000)
            && cur.exec_count > 0;
        let examined =
            full || cur.examined_samples == 0 || under(roll, self.cfg.load().examined_sample_rate);
        let capture_params = self.cfg.load().capture_samples
            && !fp.sensitive
            && Reserved::of(fp.id).is_none()
            && (e.samples_in_bucket.1 < self.cfg.load().samples_per_bucket
                || under(mix(roll), self.cfg.load().sample_rate));
        if capture_params {
            e.samples_in_bucket.1 = e.samples_in_bucket.1.saturating_add(1);
        }
        drop(m);
        if examined {
            self.probes_total.fetch_add(1, Ordering::Relaxed);
        }
        if capture_params {
            self.samples_total.fetch_add(1, Ordering::Relaxed);
        }
        ProbePlan {
            examined,
            capture_params,
        }
    }

    /// Marks a fingerprint as watched (automatic indexing asked, or it is
    /// among the tenant's top `auto_index.top_n` by total time), or unmarks it.
    pub fn watch(&self, tenant: TenantId, id: FingerprintId, on: bool) {
        {
            let mut t = lock(&self.tenants);
            let st = t.entry(tenant).or_default();
            if on {
                st.watched.insert(id.0);
            } else {
                st.watched.remove(&id.0);
            }
        }
        let key = (tenant, id.0);
        if let Some(e) = lock(self.shard(&key)).get_mut(&key) {
            e.watched = on;
        }
    }

    /// Counts one execution.
    pub fn record(&self, tenant: TenantId, fp: &Fingerprint, ex: &ExecSummary) {
        let (bucket, secs, wall) = self.now();
        let key = (tenant, fp.id.0);
        {
            let mut m = lock(self.shard(&key));
            if let Some(e) = m.get_mut(&key) {
                if e.bucket_start != bucket {
                    self.roll(tenant, e, bucket);
                }
                let before = e.hist_bytes();
                self.add(e, fp, ex, secs);
                e.digest.add(ex, wall);
                let grew = e.hist_bytes().saturating_sub(before);
                if grew > 0 {
                    e.bytes += grew;
                    self.memory.fetch_add(grew, Ordering::Relaxed);
                }
                return;
            }
        }
        self.first_record(tenant, fp, ex, bucket, secs, wall);
    }

    fn add(&self, e: &mut Entry, fp: &Fingerprint, ex: &ExecSummary, secs: u64) {
        let s = &mut e.stats;
        s.exec_count += 1;
        s.error_count += u64::from(ex.error);
        s.total_us = s.total_us.saturating_add(ex.latency_us);
        s.max_us = s.max_us.max(ex.latency_us);
        s.latency_hist.record(ex.latency_us);
        s.rows_returned = s.rows_returned.saturating_add(ex.rows_returned);
        s.rows_affected = s.rows_affected.saturating_add(ex.rows_affected);
        s.shards_sum += u64::from(ex.shards);
        s.shards_max = s.shards_max.max(ex.shards);
        s.scatter_count += u64::from(ex.scatter);
        s.cross_cluster += u64::from(ex.clusters > 1);
        s.in_list_max = s.in_list_max.max(ex.in_list_max.max(fp.in_list_max));
        if let Some(x) = ex.examined {
            s.examined_samples += 1;
            s.rows_examined = s.rows_examined.saturating_add(x);
            s.returned_sampled = s.returned_sampled.saturating_add(ex.rows_returned);
            #[allow(clippy::cast_precision_loss)]
            let small = (ex.rows_returned.max(ex.rows_affected) as f64)
                <= self.cfg.load().scan_max_returned_ratio * x as f64;
            if x >= self.cfg.load().scan_min_examined && small {
                s.scan_runs += 1;
                s.scan_sessions.insert(ex.session);
            }
        }
        e.last_seen_secs = secs;
    }

    /// Moves an entry's finished bucket to the closed outbox.
    fn roll(&self, tenant: TenantId, e: &mut Entry, bucket: u64) {
        let stats = std::mem::take(&mut e.stats);
        e.prev_p95_us = if e.bucket_start + self.bucket_secs() == bucket {
            stats.latency_hist.quantile_us(0.95).unwrap_or(0)
        } else {
            0
        };
        let freed = stats.latency_hist.heap_bytes();
        e.bytes = e.bytes.saturating_sub(freed);
        self.sub_memory(freed);
        if stats.exec_count > 0 {
            self.close(FlushRow {
                tenant,
                fingerprint: e.meta.id,
                bucket_start: e.bucket_start,
                part: e.part,
                stats,
                last_seen: UNIX_EPOCH + std::time::Duration::from_secs(e.last_seen_secs),
            });
        }
        e.bucket_start = bucket;
        e.part = 0;
    }

    fn close(&self, row: FlushRow) {
        let bytes = row_bytes(&row);
        lock(&self.closed).push_back(row);
        self.memory.fetch_add(bytes, Ordering::Relaxed);
        self.closed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn sub_memory(&self, bytes: usize) {
        let _ = self
            .memory
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |m| {
                Some(m.saturating_sub(bytes))
            });
    }

    fn sub_closed(&self, bytes: usize) {
        self.sub_memory(bytes);
        let _ = self
            .closed_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |m| {
                Some(m.saturating_sub(bytes))
            });
    }

    #[allow(clippy::too_many_arguments)]
    fn first_record(
        &self,
        tenant: TenantId,
        fp: &Fingerprint,
        ex: &ExecSummary,
        bucket: u64,
        secs: u64,
        wall: SystemTime,
    ) {
        // Decide under the tenant lock: known, new, or "other".
        let (meta, watched, part, is_new, tenant_bytes) = {
            let mut t = lock(&self.tenants);
            let st = t.entry(tenant).or_default();
            let known = st.known.contains(&fp.id.0);
            let reserved = Reserved::of(fp.id).is_some();
            let cap =
                usize::try_from(self.cfg.load().max_fingerprints_per_tenant).unwrap_or(usize::MAX);
            let over = !known && !reserved && st.known.len() >= cap;
            let meta = if over {
                self.other_total.fetch_add(1, Ordering::Relaxed);
                FpMeta::other(fp)
            } else {
                FpMeta::of(fp)
            };
            let id = meta.id.0;
            let is_new = st.known.insert(id);
            let part = match st.parts.get(&id) {
                Some((b, p)) if *b == bucket => *p,
                _ => 0,
            };
            (
                Arc::new(meta),
                st.watched.contains(&id),
                part,
                is_new,
                st.bytes,
            )
        };
        let key = (tenant, meta.id.0);
        let mut e = Entry {
            meta: Arc::clone(&meta),
            bucket_start: bucket,
            part,
            stats: Stats::default(),
            prev_p95_us: 0,
            watched,
            last_seen_secs: secs,
            bytes: 0,
            first_seen: wall,
            samples_in_bucket: (bucket, 0),
            digest: Digest::default(),
        };
        {
            let mut m = lock(self.shard(&key));
            if let Some(existing) = m.get_mut(&key) {
                // Another thread inserted it (or this is "other").
                if existing.bucket_start != bucket {
                    self.roll(tenant, existing, bucket);
                }
                self.add(existing, fp, ex, secs);
                existing.digest.add(ex, wall);
                return;
            }
            self.add(&mut e, fp, ex, secs);
            e.digest.add(ex, wall);
            e.bytes = e.heap_bytes();
            let bytes = e.bytes;
            m.insert(key, e);
            self.memory.fetch_add(bytes, Ordering::Relaxed);
            self.entries.fetch_add(1, Ordering::Relaxed);
            lock(&self.tenants).entry(tenant).or_default().bytes += bytes;
        }
        if is_new {
            lock(&self.new).push(NewFingerprint {
                tenant,
                meta,
                first_seen: wall,
            });
        }
        self.enforce_caps(tenant, tenant_bytes);
    }

    /// Evicts when over the gateway or the tenant cap.
    fn enforce_caps(&self, tenant: TenantId, tenant_bytes: usize) {
        let total = self.memory.load(Ordering::Relaxed);
        let over_total = total > self.cfg.load().max_memory_bytes;
        let over_tenant = tenant_bytes > self.tenant_cap();
        if !(over_total || over_tenant) {
            return;
        }
        if self
            .evicting
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if over_tenant {
            self.evict(Some(tenant), self.tenant_cap() / 10 * 9);
        }
        if self.memory.load(Ordering::Relaxed) > self.cfg.load().max_memory_bytes {
            // The flusher's backlog may hold at most a quarter of the cap;
            // past that its oldest rows are dropped (and counted) before
            // live entries are evicted.
            self.trim_closed(self.cfg.load().max_memory_bytes / 4);
            self.evict(None, self.cfg.load().max_memory_bytes / 10 * 9);
            self.trim_closed(self.cfg.load().max_memory_bytes / 4);
        }
        self.evicting.store(false, Ordering::Release);
    }

    /// Evicts the coldest entries (of one tenant, or of all) until the
    /// counted bytes (the tenant's, or the total) are at most `target`.
    fn evict(&self, only: Option<TenantId>, target: usize) {
        // Collect (last seen, key) of candidates.
        let mut cands: Vec<(u64, Key)> = Vec::new();
        for s in &*self.shards {
            let m = lock(s);
            cands.extend(
                m.iter()
                    .filter(|(k, _)| only.is_none_or(|t| k.0 == t))
                    .map(|(k, e)| (e.last_seen_secs, *k)),
            );
        }
        cands.sort_unstable_by_key(|c| c.0);
        let current = |rec: &Recorder| match only {
            Some(t) => lock(&rec.tenants).get(&t).map_or(0, |s| s.bytes),
            None => rec.memory.load(Ordering::Relaxed),
        };
        for (_, key) in cands {
            if current(self) <= target {
                break;
            }
            let Some(e) = lock(self.shard(&key)).remove(&key) else {
                continue;
            };
            self.entries.fetch_sub(1, Ordering::Relaxed);
            self.sub_memory(e.bytes);
            self.evicted_total.fetch_add(1, Ordering::Relaxed);
            {
                let mut t = lock(&self.tenants);
                let st = t.entry(key.0).or_default();
                st.bytes = st.bytes.saturating_sub(e.bytes);
                st.parts
                    .insert(key.1, (e.bucket_start, e.part.saturating_add(1)));
            }
            if e.stats.exec_count > 0 {
                self.close(FlushRow {
                    tenant: key.0,
                    fingerprint: e.meta.id,
                    bucket_start: e.bucket_start,
                    part: e.part,
                    stats: e.stats,
                    last_seen: UNIX_EPOCH + std::time::Duration::from_secs(e.last_seen_secs),
                });
            }
        }
    }

    /// Drops the oldest closed rows while the outbox holds more than
    /// `max` bytes.
    fn trim_closed(&self, max: usize) {
        let mut c = lock(&self.closed);
        while self.closed_bytes.load(Ordering::Relaxed) > max {
            let Some(row) = c.pop_front() else { break };
            self.sub_closed(row_bytes(&row));
            self.dropped_rows_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The open buckets' cumulative values, for the periodic flush
    /// (`REPLACE` under this gateway's `(gateway_id, part)`). Entries whose
    /// bucket has ended are rolled into the closed outbox first, so a call
    /// at a bucket's end plus jitter also closes it.
    pub fn snapshot(&self) -> Vec<FlushRow> {
        let (bucket, ..) = self.now();
        let mut rows = Vec::new();
        for s in &*self.shards {
            let mut m = lock(s);
            for (k, e) in m.iter_mut() {
                if e.bucket_start != bucket {
                    self.roll(k.0, e, bucket);
                    continue;
                }
                if e.stats.exec_count > 0 {
                    rows.push(FlushRow {
                        tenant: k.0,
                        fingerprint: e.meta.id,
                        bucket_start: e.bucket_start,
                        part: e.part,
                        stats: e.stats.clone(),
                        last_seen: UNIX_EPOCH + std::time::Duration::from_secs(e.last_seen_secs),
                    });
                }
            }
        }
        rows
    }

    /// Takes the closed rows (finished buckets and evicted entries): the
    /// flusher writes each once, then they are gone from memory.
    pub fn drain_closed(&self) -> Vec<FlushRow> {
        let rows: Vec<FlushRow> = lock(&self.closed).drain(..).collect();
        self.sub_closed(rows.iter().map(row_bytes).sum());
        rows
    }

    /// Puts rows back at the front of the closed outbox after a failed
    /// flush, to retry them later.
    pub fn requeue_closed(&self, rows: Vec<FlushRow>) {
        let mut bytes = 0;
        {
            let mut c = lock(&self.closed);
            for r in rows.into_iter().rev() {
                bytes += row_bytes(&r);
                c.push_front(r);
            }
        }
        self.memory.fetch_add(bytes, Ordering::Relaxed);
        self.closed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.trim_closed(self.cfg.load().max_memory_bytes / 4);
    }

    /// Takes the first sightings since the last call.
    pub fn drain_new(&self) -> Vec<NewFingerprint> {
        std::mem::take(&mut *lock(&self.new))
    }

    /// The current entry of one fingerprint: its open bucket's counters.
    #[must_use]
    pub fn stats(&self, tenant: TenantId, id: FingerprintId) -> Option<Stats> {
        let key = (tenant, id.0);
        lock(self.shard(&key)).get(&key).map(|e| e.stats.clone())
    }

    /// When a fingerprint was first seen by this recorder.
    #[must_use]
    pub fn first_seen(&self, tenant: TenantId, id: FingerprintId) -> Option<SystemTime> {
        let key = (tenant, id.0);
        lock(self.shard(&key)).get(&key).map(|e| e.first_seen)
    }

    /// Forgets a tenant (it moved away or was deleted). Its open entries go
    /// to the closed outbox first.
    pub fn forget_tenant(&self, tenant: TenantId) {
        for s in &*self.shards {
            let removed: Vec<(Key, Entry)> = {
                let mut m = lock(s);
                let keys: Vec<Key> = m.keys().filter(|k| k.0 == tenant).copied().collect();
                keys.into_iter()
                    .filter_map(|k| m.remove(&k).map(|e| (k, e)))
                    .collect()
            };
            for (k, e) in removed {
                self.entries.fetch_sub(1, Ordering::Relaxed);
                self.sub_memory(e.bytes);
                if e.stats.exec_count > 0 {
                    self.close(FlushRow {
                        tenant: k.0,
                        fingerprint: e.meta.id,
                        bucket_start: e.bucket_start,
                        part: e.part,
                        stats: e.stats,
                        last_seen: UNIX_EPOCH + std::time::Duration::from_secs(e.last_seen_secs),
                    });
                }
            }
        }
        lock(&self.tenants).remove(&tenant);
    }

    /// A tenant's open buckets as window statistics (no samples): the
    /// in-memory view of [`crate::window::FpWindowStats`]; the workload
    /// store ([`crate::store::WorkloadStore`]) serves merged windows.
    #[must_use]
    pub fn window(&self, tenant: TenantId) -> Vec<crate::window::FpWindowStats> {
        let mut out = Vec::new();
        for s in &*self.shards {
            let m = lock(s);
            for (k, e) in m.iter().filter(|(k, _)| k.0 == tenant) {
                out.push(crate::window::FpWindowStats {
                    tenant: k.0,
                    id: e.meta.id,
                    kind: e.meta.kind,
                    text: Arc::clone(&e.meta.text),
                    shape: Arc::clone(&e.meta.shape),
                    caller: e.meta.caller,
                    stats: e.stats.clone(),
                    samples: Vec::new(),
                });
            }
        }
        out.sort_by_key(|w| w.id);
        out
    }

    /// A tenant's digests on this gateway (see [`Digest`]), executed at
    /// least once since the last reset.
    #[must_use]
    pub fn digests(&self, tenant: TenantId) -> Vec<DigestRow> {
        let mut out = Vec::new();
        for s in &*self.shards {
            let m = lock(s);
            for (_, e) in m.iter().filter(|(k, _)| k.0 == tenant) {
                if e.digest.exec_count > 0 {
                    out.push(DigestRow {
                        id: e.meta.id,
                        text: Arc::clone(&e.meta.text),
                        db_id: e.meta.db_id,
                        digest: e.digest.clone(),
                    });
                }
            }
        }
        out.sort_by_key(|d| d.id);
        out
    }

    /// Starts a tenant's digests again (`TRUNCATE` of the digest summary
    /// on 3306); the workload statistics are untouched.
    pub fn reset_digests(&self, tenant: TenantId) {
        for s in &*self.shards {
            let mut m = lock(s);
            for (_, e) in m.iter_mut().filter(|(k, _)| k.0 == tenant) {
                let freed = e.digest.latency_hist.heap_bytes();
                e.digest = Digest::default();
                e.bytes = e.bytes.saturating_sub(freed);
                self.sub_memory(freed);
            }
        }
    }

    /// The metrics.
    #[must_use]
    pub fn metrics(&self) -> RecorderMetrics {
        RecorderMetrics {
            fingerprints_active: self.entries.load(Ordering::Relaxed) as u64,
            memory_bytes: self.memory.load(Ordering::Relaxed) as u64,
            other_total: self.other_total.load(Ordering::Relaxed),
            evicted_total: self.evicted_total.load(Ordering::Relaxed),
            dropped_rows_total: self.dropped_rows_total.load(Ordering::Relaxed),
            probes_total: self.probes_total.load(Ordering::Relaxed),
            samples_total: self.samples_total.load(Ordering::Relaxed),
        }
    }

    /// Bytes counted for one tenant's entries.
    #[must_use]
    pub fn tenant_bytes(&self, tenant: TenantId) -> usize {
        lock(&self.tenants).get(&tenant).map_or(0, |s| s.bytes)
    }
}

#[cfg(test)]
mod tests;
