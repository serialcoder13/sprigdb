//! The sink runtime: one task reads its source, prepares records,
//! spreads them over ordered lanes by key, cuts batches, delivers each
//! lane's batches in order with retries, splits out poison records,
//! dead-letters them, pauses and probes a failing destination, and settles
//! the source up to the lowest position not yet done.
//!
//! ```text
//! task(source, target, dead letters, control):
//!  1. target.open → an exactly-once target's own position wins: seek after it
//!  2. loop:
//!     a. read while in-flight bytes < sink.max_inflight_bytes (back-pressure)
//!     b. prepare: WHERE, columns, TRANSFORM (0, 1 or n records; an error
//!        dead-letters the record)
//!     c. lane := xxh3(key) mod sink.lanes (one lane under EXACTLY ONCE)
//!     d. cut a lane's batch at batch_max_records, batch_max_bytes or
//!        batch_max_wait_ms; a lane has one batch in flight, lanes run in
//!        parallel (each on its own tokio task)
//!     e. outcomes: ok → done; destination-wide retry → back off (full
//!        jitter, Retry-After honoured), count towards pausing; one-record
//!        retry → resend that record, up to record_max_attempts; whole-batch
//!        permanent failure → halve until the poison record is alone;
//!        permanent → dead letter (written before the record counts as done)
//!     f. settle the lowest position not done, per partition or shard (queue
//!        messages one by one), after checking the task's epoch
//!  3. paused by failures: one lane probes every probe_interval; success
//!     resumes every lane. Paused by a user, dropped or fenced: stop.
//! ```
//!
//! Every wait uses tokio's clock, so the simulation (`tests/sim.rs`) runs
//! it on a paused clock with seeded randomness.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::model::{Batch, BatchOutcome, DestPosition, Outcome, Position, SinkRecord, TaskEpoch};

/// A cancellation flag the worker sets when the task must stop.
pub mod tokio_util_cancel {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::Notify;

    /// Set once; waiters wake.
    #[derive(Debug, Clone, Default)]
    pub struct CancelFlag(Arc<(AtomicBool, Notify)>);

    impl CancelFlag {
        /// A flag not set.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Sets it.
        pub fn cancel(&self) {
            self.0.0.store(true, Ordering::SeqCst);
            self.0.1.notify_waiters();
        }

        /// Whether it is set.
        #[must_use]
        pub fn is_cancelled(&self) -> bool {
            self.0.0.load(Ordering::SeqCst)
        }

        /// Waits until it is set.
        pub async fn cancelled(&self) {
            loop {
                let n = self.0.1.notified();
                if self.is_cancelled() {
                    return;
                }
                n.await;
            }
        }
    }
}

pub use tokio_util_cancel::CancelFlag as Cancel;

/// What a task reads.
#[async_trait]
pub trait SourceReader: Send {
    /// Up to `max_records` records or `max_bytes` bytes (at least one
    /// record when any is there), waiting at most `wait` for the first.
    /// Empty when nothing arrived in time.
    ///
    /// # Errors
    /// 39223 when the position fell behind the source's retention; the
    /// backend's (retried by the caller).
    async fn next_batch(
        &mut self,
        max_records: usize,
        max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>>;

    /// Settles delivered records: for a partition or shard, the last
    /// position of its delivered prefix; for queue messages, each one.
    ///
    /// # Errors
    /// The backend's (the next settle retries).
    async fn settle(&mut self, done: &[Position]) -> Result<()>;

    /// Rejects `rec` through the source itself, when the sink has no
    /// dead-letter target and the source has its own (a queue nacks
    /// without requeue, so its dead-letter exchange applies). `false`
    /// when the source has none.
    ///
    /// # Errors
    /// The backend's.
    async fn reject(&mut self, _rec: &SinkRecord, _reason: &str) -> Result<bool> {
        Ok(false)
    }

    /// Resumes after `pos` (an exactly-once target's own position).
    ///
    /// # Errors
    /// The backend's.
    async fn seek_after(&mut self, pos: &Position) -> Result<()>;
}

/// The task as a target sees it.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// The tenant.
    pub tenant: u64,
    /// The sink.
    pub sink: u64,
    /// The task number.
    pub task_no: u32,
    /// The task epoch.
    pub epoch: TaskEpoch,
}

impl TaskInfo {
    /// `<tenant>/<sink id>/<task no>`: the key of exactly-once positions in
    /// destinations.
    #[must_use]
    pub fn sink_key(&self) -> String {
        format!("{}/{}/{}", self.tenant, self.sink, self.task_no)
    }
}

/// A destination. `deliver` takes `&self`, so
/// lanes deliver in parallel; a target keeps its connections inside.
#[async_trait]
pub trait Target: Send + Sync {
    /// `webhook`, `kafka`, `s3`, `database`, `script` (metric labels).
    fn kind(&self) -> &'static str;

    /// Called once per task start: an exactly-once target reads its own
    /// positions, one per partition or shard (they win over SprigDB's),
    /// and fences older writers.
    ///
    /// # Errors
    /// A destination-wide failure (the task retries the start).
    async fn open(&mut self, _task: &TaskInfo) -> Result<Vec<DestPosition>> {
        Ok(Vec::new())
    }

    /// Delivers one batch. `Err` is a destination-wide failure, retried
    /// with backoff; 39227 (fenced) stops the task.
    ///
    /// # Errors
    /// See above.
    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome>;

    /// Whether `deliver` writes `b.ends` with the data, in one destination
    /// transaction (exactly once).
    fn exactly_once(&self) -> bool {
        false
    }
}

/// Why a record was dead-lettered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadInfo {
    /// The reason.
    pub reason: String,
    /// `permanent`, `retries_exhausted`, `too_large`, `transform`.
    pub class: &'static str,
    /// Attempts made.
    pub attempts: u32,
}

/// Where dead letters go (a stream `<sink>_dead`, or another stream or
/// queue named by `DEAD LETTER TO`).
#[async_trait]
pub trait DeadLetters: Send + Sync {
    /// Writes one dead letter, with its original key, value and headers
    /// plus the `sprig.dl.*` headers.
    ///
    /// # Errors
    /// The backend's: the record isn't marked done, and the write is
    /// retried.
    async fn write(&self, rec: &SinkRecord, info: &DeadInfo) -> Result<()>;
}

/// What the catalog says the task should do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directive {
    /// Keep delivering.
    Run,
    /// A user paused the sink: stop reading, finish in-flight batches,
    /// wait.
    Pause,
    /// The sink is being dropped, failed or was reset: stop.
    Stop,
}

/// What the task reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Destination-wide failures started.
    Failing(String),
    /// Failures lasted `sink.pause_after_minutes`: paused, probing (39222).
    PausedByFailure(String),
    /// A delivery succeeded after failing or pausing.
    Recovered,
    /// The source lost the position (39223): the sink fails until reset.
    SourceLost(String),
    /// Records delivered since the last report.
    Delivered(u64),
}

/// The catalog and lease side of a task.
#[async_trait]
pub trait Control: Send + Sync {
    /// The sink's directive now (read from the catalog).
    ///
    /// # Errors
    /// The catalog's (the task keeps its last directive).
    async fn poll(&self) -> Result<Directive>;

    /// Records a state change.
    ///
    /// # Errors
    /// The catalog's (logged).
    async fn report(&self, ev: Event) -> Result<()>;

    /// 39227 when the task's epoch moved on (checked before every settle).
    ///
    /// # Errors
    /// See above.
    async fn check_epoch(&self) -> Result<()>;
}

/// The preparation step: `WHERE`, columns, `TRANSFORM`. `Ok(vec![])`
/// drops the record; `Err(reason)` fails it permanently (dead letter).
#[async_trait::async_trait]
pub trait Prepare: Send + Sync {
    /// Prepares one record.
    ///
    /// # Errors
    /// The reason the record fails permanently.
    fn prepare(&self, rec: SinkRecord) -> std::result::Result<Vec<SinkRecord>, String>;

    /// `TRANSFORM`, after [`Prepare::prepare`] kept the record.
    ///
    /// # Errors
    /// The reason the record fails permanently.
    async fn transform(
        &self,
        recs: Vec<SinkRecord>,
    ) -> std::result::Result<Vec<SinkRecord>, String> {
        Ok(recs)
    }
}

/// Passes every record unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct PassAll;

impl Prepare for PassAll {
    fn prepare(&self, rec: SinkRecord) -> std::result::Result<Vec<SinkRecord>, String> {
        Ok(vec![rec])
    }
}

/// The settings one task runs with.
#[derive(Debug, Clone)]
pub struct RunSettings {
    /// `sink.lanes`.
    pub lanes: u16,
    /// `sink.batch_max_records`.
    pub batch_max_records: usize,
    /// `sink.batch_max_bytes`.
    pub batch_max_bytes: u64,
    /// `sink.batch_max_wait_ms`.
    pub batch_max_wait: Duration,
    /// `sink.max_inflight_bytes`.
    pub max_inflight_bytes: u64,
    /// `sink.retry_initial_ms`.
    pub retry_initial: Duration,
    /// `sink.retry_max_ms`.
    pub retry_max: Duration,
    /// `sink.record_max_attempts`.
    pub record_max_attempts: u32,
    /// `sink.pause_after_minutes`.
    pub pause_after: Duration,
    /// `sink.auto_resume`.
    pub auto_resume: bool,
    /// `sink.probe_interval_seconds`.
    pub probe_interval: Duration,
    /// `sink.http_timeout_ms` (a bound on any one delivery).
    pub delivery_timeout: Duration,
    /// `sink.tenant_bytes_per_sec` (0 = none).
    pub tenant_bytes_per_sec: u64,
    /// How often positions are settled at most.
    pub settle_every: Duration,
    /// How often the catalog's directive is read.
    pub poll_every: Duration,
    /// Under `EXACTLY ONCE`: one lane, and `b.end` on every batch.
    pub exactly_once: bool,
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            lanes: 8,
            batch_max_records: 500,
            batch_max_bytes: 4 << 20,
            batch_max_wait: Duration::from_millis(1000),
            max_inflight_bytes: 64 << 20,
            retry_initial: Duration::from_millis(500),
            retry_max: Duration::from_millis(60_000),
            record_max_attempts: 10,
            pause_after: Duration::from_secs(15 * 60),
            auto_resume: true,
            probe_interval: Duration::from_secs(60),
            delivery_timeout: Duration::from_millis(30_000),
            tenant_bytes_per_sec: 0,
            settle_every: Duration::from_millis(200),
            poll_every: Duration::from_secs(1),
            exactly_once: false,
        }
    }
}

/// Seeded randomness for jitter: `SplitMix64`.
#[derive(Debug)]
pub struct Rng(Mutex<u64>);

impl Rng {
    /// Seeded.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(Mutex::new(seed))
    }

    /// From the OS generator.
    #[must_use]
    pub fn from_os() -> Self {
        let mut b = [0u8; 8];
        let _ = getrandom::fill(&mut b);
        Self::new(u64::from_le_bytes(b))
    }

    /// The next value.
    pub fn next_u64(&self) -> u64 {
        let mut s = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n]`.
    pub fn below_incl(&self, n: u64) -> u64 {
        if n == u64::MAX {
            return self.next_u64();
        }
        self.next_u64() % (n + 1)
    }
}

/// The backoff: `base = min(retry_max, retry_initial · 2^(n−1))`,
/// `wait = random(0, base)` (full jitter), and a destination's hint raises
/// it to `max(wait, min(hint, retry_max))`. `n` counts from 1.
#[must_use]
pub fn backoff(
    n: u32,
    hint: Option<Duration>,
    initial: Duration,
    max: Duration,
    rng: &Rng,
) -> Duration {
    let max_ms = u64::try_from(max.as_millis()).unwrap_or(u64::MAX);
    let init_ms = u64::try_from(initial.as_millis()).unwrap_or(u64::MAX);
    let shift = n.saturating_sub(1).min(40);
    let base = init_ms.saturating_mul(1u64 << shift).min(max_ms);
    let mut wait = rng.below_incl(base);
    if let Some(h) = hint {
        let h = u64::try_from(h.as_millis()).unwrap_or(u64::MAX).min(max_ms);
        wait = wait.max(h);
    }
    Duration::from_millis(wait)
}

/// The lane of a key: xxh3 modulo the lane count; keyless records go by
/// their idempotency key.
#[must_use]
pub fn lane_of(rec: &SinkRecord, lanes: u16) -> u16 {
    if lanes <= 1 {
        return 0;
    }
    let h = match &rec.key {
        Some(k) => twox_hash::XxHash3_64::oneshot(k),
        None => twox_hash::XxHash3_64::oneshot(rec.idem_key.as_bytes()),
    };
    u16::try_from(h % u64::from(lanes)).unwrap_or(0)
}

/// Tracks read positions and settles each unit's delivered prefix.
#[derive(Debug, Default)]
pub struct Settler {
    units: HashMap<u64, BTreeMap<u64, (Position, bool)>>,
    loose: Vec<Position>,
}

impl Settler {
    /// A record was read.
    pub fn read(&mut self, p: &Position) {
        if let Some((u, s)) = p.unit_seq() {
            self.units
                .entry(u)
                .or_default()
                .insert(s, (p.clone(), false));
        }
    }

    /// A record is done (delivered or dead-lettered).
    pub fn done(&mut self, p: &Position) {
        match p.unit_seq() {
            Some((u, s)) => {
                if let Some(e) = self.units.get_mut(&u).and_then(|m| m.get_mut(&s)) {
                    e.1 = true;
                }
            }
            None => self.loose.push(p.clone()),
        }
    }

    /// Positions that may be settled now: each unit's last position of
    /// its done prefix (dropped from tracking), and loose ones.
    pub fn drain(&mut self) -> Vec<Position> {
        let mut out = std::mem::take(&mut self.loose);
        for m in self.units.values_mut() {
            let mut last = None;
            while let Some(e) = m.first_entry() {
                if !e.get().1 {
                    break;
                }
                last = Some(e.remove().0);
            }
            if let Some(p) = last {
                out.push(p);
            }
        }
        out
    }

    /// Records read and not yet settled.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.units.values().map(BTreeMap::len).sum::<usize>() + self.loose.len()
    }
}

/// How a task ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskEnd {
    /// The worker cancelled it.
    Cancelled,
    /// The sink is dropped, failed or reset.
    Stopped,
    /// The epoch moved on (39227).
    Fenced,
}

/// State the lanes share.
struct Shared {
    settings: RunSettings,
    rng: Rng,
    target: Arc<dyn Target>,
    dead: Arc<dyn DeadLetters>,
    control: Arc<dyn Control>,
    /// When destination-wide failures started.
    failing_since: Mutex<Option<Instant>>,
    /// Paused by failures (probing).
    paused: AtomicBool,
    /// A lane is the prober.
    probing: AtomicBool,
    resumed: Notify,
    stop: AtomicBool,
    delivered: AtomicU64,
    /// Records the source should reject (queue sources without a
    /// dead-letter target), returned to the reader's owner.
    rejects: Mutex<Vec<(SinkRecord, String)>>,
    reject_via_source: bool,
    quota: Option<Arc<Quota>>,
}

impl Shared {
    fn failing_for(&self, now: Instant) -> Duration {
        let mut f = self
            .failing_since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let since = *f.get_or_insert(now);
        now.saturating_duration_since(since)
    }

    fn mark_ok(&self) -> bool {
        let was = self
            .failing_since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .is_some();
        let paused = self.paused.swap(false, Ordering::SeqCst);
        if paused {
            self.resumed.notify_waiters();
        }
        was || paused
    }
}

/// A per-tenant delivery quota: a token bucket in bytes. `per_sec` is the
/// tenant's budget across every worker (`sink.tenant_bytes_per_sec`); the
/// bucket fills at this worker's share of it, which [`share_quotas`] sets
/// from what each worker asked for in the last window.
#[derive(Debug)]
pub struct Quota {
    per_sec: u64,
    /// This worker's share, bytes a second.
    local: AtomicU64,
    /// Bytes asked for since the last report.
    demand: AtomicU64,
    state: Mutex<(f64, Instant)>,
}

impl Quota {
    /// `per_sec` bytes a second; until shares are known, this worker may
    /// use all of it.
    #[must_use]
    pub fn new(per_sec: u64) -> Self {
        #[allow(clippy::cast_precision_loss)]
        let full = per_sec as f64;
        Self {
            per_sec,
            local: AtomicU64::new(per_sec),
            demand: AtomicU64::new(0),
            state: Mutex::new((full, Instant::now())),
        }
    }

    /// This worker's share now.
    #[must_use]
    pub fn local(&self) -> u64 {
        self.local.load(Ordering::Relaxed)
    }

    /// Sets this worker's share (at least 1 byte a second).
    pub fn set_local(&self, per_sec: u64) {
        self.local.store(per_sec.max(1), Ordering::Relaxed);
    }

    /// Waits until `bytes` may be sent.
    pub async fn take(&self, bytes: u64) {
        self.demand.fetch_add(bytes, Ordering::Relaxed);
        #[allow(clippy::cast_precision_loss)]
        let need = bytes as f64;
        loop {
            #[allow(clippy::cast_precision_loss)]
            let rate = self.local() as f64;
            let wait = {
                let mut s = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let now = Instant::now();
                let refill = now.saturating_duration_since(s.1).as_secs_f64() * rate;
                s.0 = (s.0 + refill).min(rate.max(need));
                s.1 = now;
                if s.0 >= need {
                    s.0 -= need;
                    return;
                }
                Duration::from_secs_f64(((need - s.0) / rate).min(1.0))
            };
            tokio::time::sleep(wait).await;
        }
    }
}

static QUOTAS: std::sync::LazyLock<Mutex<HashMap<u64, Arc<Quota>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// The process's quota bucket of `tenant` (shared by its tasks on this
/// worker), or `None` when `per_sec` is 0.
#[must_use]
pub fn tenant_quota(tenant: u64, per_sec: u64) -> Option<Arc<Quota>> {
    if per_sec == 0 {
        return None;
    }
    let mut m = QUOTAS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let q = m
        .entry(tenant)
        .or_insert_with(|| Arc::new(Quota::new(per_sec)));
    if q.per_sec != per_sec {
        *q = Arc::new(Quota::new(per_sec));
    }
    Some(Arc::clone(q))
}

/// How often workers report and re-split each tenant's budget.
pub const QUOTA_WINDOW: Duration = Duration::from_secs(2);
/// A report older than this is a worker that stopped.
const QUOTA_STALE_MS: u64 = 3 * 2_000 + 1_000;

/// One worker's share of `per_sec` from every worker's demand: each gets
/// a tenth of an even split as a floor (so a worker that starts asking
/// isn't starved for a window), the rest by demand; the shares add up to
/// `per_sec`.
#[must_use]
pub fn split(per_sec: u64, mine: u64, all: &[u64]) -> u64 {
    let n = all.len().max(1) as u64;
    let floor = (per_sec / n / 10).max(1);
    let total: u128 = all.iter().map(|d| u128::from(*d + floor)).sum();
    if total == 0 {
        return per_sec / n;
    }
    u64::try_from(u128::from(per_sec) * u128::from(mine + floor) / total).unwrap_or(per_sec)
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

/// Shares every tenant's `sink.tenant_bytes_per_sec` among the workers
/// delivering for it: every
/// [`QUOTA_WINDOW`] each worker writes what it asked for into
/// `/sprig/dc/<dc>/sink-quota/<tenant>/<worker>`, reads the others'
/// fresh reports, and fills its bucket at its [`split`] of the budget. A
/// worker that stops reporting drops out after about three windows.
/// Started once per process.
pub fn ensure_quota_sharing(dc: Arc<dyn sprig_meta::MetaStore>, worker: String) {
    static STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    tokio::spawn(async move {
        let mut idle: HashMap<u64, u32> = HashMap::new();
        loop {
            tokio::time::sleep(QUOTA_WINDOW).await;
            let quotas: Vec<(u64, Arc<Quota>)> = QUOTAS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|(t, q)| (*t, Arc::clone(q)))
                .collect();
            for (tenant, q) in quotas {
                let asked = q.demand.swap(0, Ordering::Relaxed) / QUOTA_WINDOW.as_secs().max(1);
                let quiet = idle.entry(tenant).or_default();
                *quiet = if asked == 0 { *quiet + 1 } else { 0 };
                if let Err(e) =
                    share_once(dc.as_ref(), &worker, tenant, &q, asked, *quiet > 30).await
                {
                    tracing::debug!(tenant, error = %e, "sink quota: sharing failed; keeping the last share");
                }
            }
        }
    });
}

async fn share_once(
    dc: &dyn sprig_meta::MetaStore,
    worker: &str,
    tenant: u64,
    q: &Quota,
    asked: u64,
    forget: bool,
) -> Result<()> {
    use sprig_meta::{Op, Txn};
    let prefix = dc.tier().key(&format!("sink-quota/{tenant}/"))?;
    let mine = dc.tier().key(&format!("sink-quota/{tenant}/{worker}"))?;
    if forget {
        // Quiet for a minute: leave the split to the others.
        dc.txn(Txn {
            when: Vec::new(),
            then: vec![Op::Delete(mine)],
            otherwise: Vec::new(),
        })
        .await?;
        return Ok(());
    }
    let now = now_ms();
    dc.txn(Txn {
        when: Vec::new(),
        then: vec![Op::Put {
            key: mine.clone(),
            value: serde_json::to_vec(&serde_json::json!({"demand": asked, "at": now}))
                .unwrap_or_default(),
            lease: None,
        }],
        otherwise: Vec::new(),
    })
    .await?;
    let page = dc
        .list(
            &prefix,
            sprig_meta::ListOpts {
                limit: 10_000,
                ..sprig_meta::ListOpts::default()
            },
        )
        .await?;
    let mut all = Vec::new();
    for kv in &page.kvs {
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(&kv.value) else {
            continue;
        };
        let at = v.get("at").and_then(serde_json::Value::as_u64).unwrap_or(0);
        if now.saturating_sub(at) > QUOTA_STALE_MS && kv.key != mine {
            continue;
        }
        all.push(
            v.get("demand")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        );
    }
    let share = split(q.per_sec, asked, &all);
    q.set_local(share);
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!("sprig_sink_quota_share_bytes_per_sec").set(share as f64);
    Ok(())
}

/// What one lane batch ended with.
struct LaneDone {
    lane: u16,
    bytes: u64,
    done: Vec<Position>,
    stop: Option<StopWhy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopWhy {
    Fenced,
    Stopped,
}

/// Classifies a delivery error: 39227 stops; anything else is a
/// destination-wide retry with the error's retry hint.
fn fenced(e: &SprigError) -> bool {
    e.kind == sprig_common::ErrorKind::Sprig(SprigCode::SinkFenced)
}

/// The hint a delivery error carries, written by targets as
/// `retry_after_ms=<n>` in the message.
#[must_use]
pub fn hint_of(msg: &str) -> Option<Duration> {
    let i = msg.find("retry_after_ms=")?;
    let rest = &msg[i + "retry_after_ms=".len()..];
    let n: String = rest.chars().take_while(char::is_ascii_digit).collect();
    n.parse().ok().map(Duration::from_millis)
}

/// Delivers one lane batch to the end (see the module docs, step e).
#[allow(clippy::too_many_lines)] // the outcome table, case by case
async fn run_lane(sh: Arc<Shared>, lane: u16, records: Vec<SinkRecord>) -> LaneDone {
    let bytes: u64 = records.iter().map(SinkRecord::size).sum();
    let kind = sh.target.kind();
    let mut done = Vec::with_capacity(records.len());
    let mut work: VecDeque<Vec<SinkRecord>> = VecDeque::from([records]);
    let mut attempts: HashMap<String, u32> = HashMap::new();
    let mut dest_failures = 0u32;
    let mut batch_attempt = 0u32;
    let s = &sh.settings;
    let finish = |done: Vec<Position>, stop| LaneDone {
        lane,
        bytes,
        done,
        stop,
    };
    while let Some(mut recs) = work.pop_front() {
        loop {
            if sh.stop.load(Ordering::SeqCst) {
                return finish(done, Some(StopWhy::Stopped));
            }
            // While paused by failures, only the prober sends.
            let mut prober = false;
            if sh.paused.load(Ordering::SeqCst) {
                if sh.probing.swap(true, Ordering::SeqCst) {
                    let n = sh.resumed.notified();
                    if sh.paused.load(Ordering::SeqCst) && !sh.stop.load(Ordering::SeqCst) {
                        tokio::select! {
                            () = n => {}
                            () = tokio::time::sleep(s.probe_interval) => {}
                        }
                    }
                    continue;
                }
                prober = true;
                if !s.auto_resume {
                    // Without auto-resume, a person resumes the sink.
                    sh.probing.store(false, Ordering::SeqCst);
                    let n = sh.resumed.notified();
                    tokio::select! {
                        () = n => {}
                        () = tokio::time::sleep(s.probe_interval) => {}
                    }
                    continue;
                }
                tokio::time::sleep(s.probe_interval).await;
            }
            if let Some(q) = &sh.quota {
                q.take(recs.iter().map(SinkRecord::size).sum()).await;
            }
            batch_attempt += 1;
            let mut b = Batch::new(recs.clone(), lane);
            b.attempt = batch_attempt;
            if s.exactly_once {
                b.ends = crate::model::unit_ends(&recs);
            }
            let started = Instant::now();
            let res = match tokio::time::timeout(s.delivery_timeout, sh.target.deliver(&b)).await {
                Ok(r) => r,
                Err(_) => Err(crate::model::delivery_failed(format!(
                    "no answer within {} ms",
                    s.delivery_timeout.as_millis()
                ))),
            };
            metrics::histogram!("sprig_sink_delivery_seconds", "target" => kind)
                .record(started.elapsed().as_secs_f64());
            if prober {
                sh.probing.store(false, Ordering::SeqCst);
            }
            // Destination-wide failure.
            let dest_retry = match &res {
                Err(e) if fenced(e) => {
                    sh.stop.store(true, Ordering::SeqCst);
                    return finish(done, Some(StopWhy::Fenced));
                }
                Err(e) => Some((hint_of(&e.message), e.message.clone())),
                Ok(BatchOutcome::All(Outcome::Retry { after, reason })) => {
                    Some((*after, reason.clone()))
                }
                _ => None,
            };
            if let Some((hint, reason)) = dest_retry {
                dest_failures += 1;
                metrics::counter!("sprig_sink_batches_total", "target" => kind, "result" => "retry")
                    .increment(1);
                metrics::counter!("sprig_sink_retries_total", "target" => kind, "class" => "destination")
                    .increment(1);
                let now = Instant::now();
                let first = sh
                    .failing_since
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_none();
                let failing_for = sh.failing_for(now);
                if first {
                    let _ = sh.control.report(Event::Failing(reason.clone())).await;
                }
                if failing_for >= s.pause_after && !sh.paused.swap(true, Ordering::SeqCst) {
                    metrics::gauge!("sprig_sink_paused", "reason" => "failures").set(1.0);
                    tracing::warn!(lane, %reason, "sink paused after failures (39222)");
                    let _ = sh
                        .control
                        .report(Event::PausedByFailure(reason.clone()))
                        .await;
                }
                if !sh.paused.load(Ordering::SeqCst) {
                    let wait = backoff(dest_failures, hint, s.retry_initial, s.retry_max, &sh.rng);
                    tokio::time::sleep(wait).await;
                }
                continue;
            }
            let Ok(outcome) = res else {
                continue;
            };
            if sh.mark_ok() {
                metrics::gauge!("sprig_sink_paused", "reason" => "failures").set(0.0);
                let _ = sh.control.report(Event::Recovered).await;
            }
            dest_failures = 0;
            match outcome {
                BatchOutcome::All(Outcome::Ok) => {
                    metrics::counter!("sprig_sink_batches_total", "target" => kind, "result" => "ok")
                        .increment(1);
                    count_delivered(&sh, &recs);
                    done.extend(recs.iter().map(|r| r.pos.clone()));
                    break;
                }
                BatchOutcome::All(Outcome::Retry { .. }) => unreachable_retry(),
                BatchOutcome::All(Outcome::Fail { .. }) | BatchOutcome::TooLarge
                    if recs.len() > 1 =>
                {
                    metrics::counter!("sprig_sink_batches_total", "target" => kind, "result" => "split")
                        .increment(1);
                    let second = recs.split_off(recs.len() / 2);
                    work.push_front(second);
                    work.push_front(recs);
                    break;
                }
                BatchOutcome::All(Outcome::Fail { reason }) => {
                    metrics::counter!("sprig_sink_batches_total", "target" => kind, "result" => "fail")
                        .increment(1);
                    let r = &recs[0];
                    let n = attempts.get(&r.idem_key).copied().unwrap_or(0) + 1;
                    if !dead_letter(&sh, r, &reason, "permanent", n).await {
                        return finish(done, Some(StopWhy::Stopped));
                    }
                    done.push(r.pos.clone());
                    break;
                }
                BatchOutcome::TooLarge => {
                    let r = &recs[0];
                    let reason = "ER_SPRIG_SINK_RECORD_TOO_LARGE: the destination refused a single record as too large";
                    if !dead_letter(&sh, r, reason, "too_large", 1).await {
                        return finish(done, Some(StopWhy::Stopped));
                    }
                    done.push(r.pos.clone());
                    break;
                }
                BatchOutcome::Each(outs) => {
                    let mut keep = Vec::new();
                    let mut hint: Option<Duration> = None;
                    let mut max_n = 0;
                    let mut any_ok = false;
                    for (r, o) in
                        recs.iter()
                            .zip(outs.iter().chain(std::iter::repeat(&Outcome::Retry {
                                after: None,
                                reason: "no result for the record".into(),
                            })))
                    {
                        match o {
                            Outcome::Ok => {
                                any_ok = true;
                                count_delivered(&sh, std::slice::from_ref(r));
                                done.push(r.pos.clone());
                            }
                            Outcome::Fail { reason } => {
                                let n = attempts.get(&r.idem_key).copied().unwrap_or(0) + 1;
                                if !dead_letter(&sh, r, reason, "permanent", n).await {
                                    return finish(done, Some(StopWhy::Stopped));
                                }
                                done.push(r.pos.clone());
                            }
                            Outcome::Retry { after, reason } => {
                                let n = attempts.entry(r.idem_key.clone()).or_insert(0);
                                *n += 1;
                                metrics::counter!("sprig_sink_retries_total", "target" => kind, "class" => "record")
                                    .increment(1);
                                if *n >= s.record_max_attempts {
                                    let n = *n;
                                    if !dead_letter(&sh, r, reason, "retries_exhausted", n).await {
                                        return finish(done, Some(StopWhy::Stopped));
                                    }
                                    done.push(r.pos.clone());
                                } else {
                                    max_n = max_n.max(*n);
                                    hint = hint.max(*after);
                                    keep.push(r.clone());
                                }
                            }
                        }
                    }
                    let _ = any_ok;
                    if keep.is_empty() {
                        metrics::counter!("sprig_sink_batches_total", "target" => kind, "result" => "ok")
                            .increment(1);
                        break;
                    }
                    let wait = backoff(max_n, hint, s.retry_initial, s.retry_max, &sh.rng);
                    tokio::time::sleep(wait).await;
                    recs = keep;
                }
            }
        }
    }
    finish(done, None)
}

fn unreachable_retry() {
    // Handled above as a destination-wide retry.
}

fn count_delivered(sh: &Shared, recs: &[SinkRecord]) {
    let kind = sh.target.kind();
    let n = recs.len() as u64;
    let bytes: u64 = recs.iter().map(SinkRecord::size).sum();
    sh.delivered.fetch_add(n, Ordering::Relaxed);
    metrics::counter!("sprig_sink_records_delivered_total", "target" => kind).increment(n);
    metrics::counter!("sprig_sink_bytes_delivered_total", "target" => kind).increment(bytes);
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0);
    for r in recs {
        #[allow(clippy::cast_precision_loss)]
        let e2e = (now_ms - r.ts_ms).max(0) as f64 / 1000.0;
        metrics::histogram!("sprig_sink_end_to_end_seconds", "target" => kind).record(e2e);
    }
}

/// Writes a dead letter, retrying the write until it lands or the task
/// stops. `false` when the task stopped first.
async fn dead_letter(
    sh: &Shared,
    r: &SinkRecord,
    reason: &str,
    class: &'static str,
    n: u32,
) -> bool {
    let info = DeadInfo {
        reason: reason.to_owned(),
        class,
        attempts: n,
    };
    metrics::counter!("sprig_sink_dead_lettered_total", "target" => sh.target.kind(), "reason" => class)
        .increment(1);
    if sh.reject_via_source {
        sh.rejects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((r.clone(), reason.to_owned()));
        return true;
    }
    let mut tries = 0u32;
    loop {
        match sh.dead.write(r, &info).await {
            Ok(()) => return true,
            Err(e) => {
                tries += 1;
                tracing::warn!(error = %e, key = %r.idem_key, "dead letter not written; retrying");
                if sh.stop.load(Ordering::SeqCst) {
                    return false;
                }
                let wait = backoff(
                    tries,
                    None,
                    sh.settings.retry_initial,
                    sh.settings.retry_max,
                    &sh.rng,
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// One lane's buffer.
#[derive(Default)]
struct Lane {
    buf: VecDeque<SinkRecord>,
    bytes: u64,
    first_at: Option<Instant>,
    busy: bool,
}

/// What a task is made of.
pub struct TaskParts {
    /// The task.
    pub info: TaskInfo,
    /// Its source.
    pub source: Box<dyn SourceReader>,
    /// Its destination.
    pub target: Box<dyn Target>,
    /// Its dead letters.
    pub dead: Arc<dyn DeadLetters>,
    /// The catalog and lease side.
    pub control: Arc<dyn Control>,
    /// `WHERE`, columns and `TRANSFORM`.
    pub prepare: Arc<dyn Prepare>,
    /// Its settings.
    pub settings: RunSettings,
    /// The jitter's seed (`None`: the OS generator).
    pub seed: Option<u64>,
    /// Dead letters go back through the source (queue sources without
    /// `DEAD LETTER TO`).
    pub reject_via_source: bool,
}

/// Runs one task until it is cancelled, stopped or fenced.
///
/// # Errors
/// A source error that isn't retryable (39223 after it is reported), or
/// the target's `open` failing for good.
#[allow(clippy::too_many_lines)] // the task loop, step by step
pub async fn run_task(parts: TaskParts, cancel: Cancel) -> Result<TaskEnd> {
    let TaskParts {
        info,
        mut source,
        mut target,
        dead,
        control,
        prepare,
        mut settings,
        seed,
        reject_via_source,
    } = parts;
    if target.exactly_once() {
        settings.exactly_once = true;
    }
    if settings.exactly_once {
        settings.lanes = 1;
    }
    let lanes_n = settings.lanes.max(1);
    // 1. Open; an exactly-once target's position wins.
    let mut open_tries = 0u32;
    let rng0 = seed.map_or_else(Rng::from_os, Rng::new);
    loop {
        if cancel.is_cancelled() {
            return Ok(TaskEnd::Cancelled);
        }
        match target.open(&info).await {
            Ok(ps) => {
                for p in &ps {
                    source.seek_after(&p.pos).await?;
                }
                // Mirror the destination's positions into the source's
                // (stream offsets, feed checkpoints), for lag and Studio.
                if !ps.is_empty() && control.check_epoch().await.is_ok() {
                    let mirror: Vec<Position> = ps.into_iter().map(|p| p.pos).collect();
                    if let Err(e) = source.settle(&mirror).await {
                        tracing::warn!(error = %e, "destination positions not mirrored yet");
                    }
                }
                break;
            }
            Err(e) if fenced(&e) => return Ok(TaskEnd::Fenced),
            Err(e) => {
                open_tries += 1;
                tracing::warn!(error = %e, sink = info.sink, task = info.task_no, "sink target didn't open; retrying");
                let wait = backoff(
                    open_tries,
                    hint_of(&e.message),
                    settings.retry_initial,
                    settings.retry_max,
                    &rng0,
                );
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = cancel.cancelled() => return Ok(TaskEnd::Cancelled),
                }
            }
        }
    }
    let quota = tenant_quota(info.tenant, settings.tenant_bytes_per_sec);
    let sh = Arc::new(Shared {
        rng: rng0,
        target: Arc::from(target),
        dead,
        control: Arc::clone(&control),
        failing_since: Mutex::new(None),
        paused: AtomicBool::new(false),
        probing: AtomicBool::new(false),
        resumed: Notify::new(),
        stop: AtomicBool::new(false),
        delivered: AtomicU64::new(0),
        rejects: Mutex::new(Vec::new()),
        reject_via_source,
        quota,
        settings,
    });
    let s = sh.settings.clone();
    let mut lanes: Vec<Lane> = (0..lanes_n).map(|_| Lane::default()).collect();
    let mut joins: JoinSet<LaneDone> = JoinSet::new();
    let mut settler = Settler::default();
    let mut inflight: u64 = 0;
    let mut last_settle = Instant::now();
    // The first iteration polls before it reads: the
    // controller restarts the tasks of a sink paused by row-level security
    // every few seconds, and such a task must stop before it delivers
    // anything, not after its first second.
    let mut last_poll = Instant::now()
        .checked_sub(s.poll_every)
        .unwrap_or_else(Instant::now);
    let mut directive = Directive::Run;
    let mut read_failures = 0u32;
    let mut end: Option<TaskEnd> = None;
    let mut reported = 0u64;
    let mut arrived: VecDeque<std::result::Result<LaneDone, tokio::task::JoinError>> =
        VecDeque::new();
    let stop_all = |sh: &Shared| {
        sh.stop.store(true, Ordering::SeqCst);
        sh.resumed.notify_waiters();
    };
    loop {
        // Finished lane batches.
        while let Some(r) = arrived.pop_front().or_else(|| joins.try_join_next()) {
            match r {
                Ok(d) => {
                    for p in &d.done {
                        settler.done(p);
                    }
                    inflight = inflight.saturating_sub(d.bytes);
                    lanes[usize::from(d.lane)].busy = false;
                    match d.stop {
                        Some(StopWhy::Fenced) => end = end.or(Some(TaskEnd::Fenced)),
                        Some(StopWhy::Stopped) => end = end.or(Some(TaskEnd::Stopped)),
                        None => {}
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "sink lane panicked");
                    end = end.or(Some(TaskEnd::Stopped));
                }
            }
        }
        // Rejects through the source (queue dead-lettering).
        let rejects: Vec<(SinkRecord, String)> = std::mem::take(
            &mut *sh
                .rejects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for (r, why) in rejects {
            if let Err(e) = source.reject(&r, &why).await {
                tracing::warn!(error = %e, "queue reject failed; the message comes back");
            }
        }
        if cancel.is_cancelled() && end.is_none() {
            end = Some(TaskEnd::Cancelled);
        }
        if let Some(e) = &end {
            stop_all(&sh);
            // Settle what is done, unless fenced (a newer task owns it).
            if *e != TaskEnd::Fenced && control.check_epoch().await.is_ok() {
                let ready = settler.drain();
                if !ready.is_empty() {
                    let _ = source.settle(&ready).await;
                }
            }
            joins.abort_all();
            while joins.join_next().await.is_some() {}
            return Ok(e.clone());
        }
        // Settle.
        let now = Instant::now();
        if now.duration_since(last_settle) >= s.settle_every
            || (joins.is_empty() && settler.pending() > 0)
        {
            let ready = settler.drain();
            if !ready.is_empty() {
                match control.check_epoch().await {
                    Ok(()) => {
                        if let Err(e) = source.settle(&ready).await {
                            tracing::warn!(error = %e, "sink settle failed; records may be delivered again");
                        }
                    }
                    Err(e) if fenced(&e) => {
                        end = Some(TaskEnd::Fenced);
                        continue;
                    }
                    Err(e) => tracing::warn!(error = %e, "epoch check failed; settle skipped"),
                }
            }
            last_settle = now;
            let d = sh.delivered.load(Ordering::Relaxed);
            if d > reported {
                let _ = control.report(Event::Delivered(d - reported)).await;
                reported = d;
            }
        }
        // The catalog's directive.
        if now.duration_since(last_poll) >= s.poll_every {
            last_poll = now;
            match control.poll().await {
                Ok(Directive::Stop) => {
                    end = Some(TaskEnd::Stopped);
                    continue;
                }
                Ok(d) => directive = d,
                Err(e) => tracing::debug!(error = %e, "sink directive not read"),
            }
            if directive == Directive::Run && sh.paused.load(Ordering::SeqCst) && !s.auto_resume {
                // A person resumed a sink paused by failures.
                sh.paused.store(false, Ordering::SeqCst);
                sh.resumed.notify_waiters();
            }
        }
        // Cut batches.
        let mut next_deadline: Option<Instant> = None;
        for (i, lane) in lanes.iter_mut().enumerate() {
            if lane.busy || lane.buf.is_empty() {
                continue;
            }
            let due = lane.first_at.map(|t| t + s.batch_max_wait);
            let full = lane.buf.len() >= s.batch_max_records || lane.bytes >= s.batch_max_bytes;
            if full || due.is_some_and(|d| d <= now) || directive == Directive::Pause {
                let mut recs = Vec::new();
                let mut bytes = 0u64;
                while let Some(r) = lane.buf.front() {
                    let sz = r.size();
                    if !recs.is_empty()
                        && (recs.len() >= s.batch_max_records || bytes + sz > s.batch_max_bytes)
                    {
                        break;
                    }
                    bytes += sz;
                    if let Some(r) = lane.buf.pop_front() {
                        recs.push(r);
                    }
                }
                lane.bytes = lane.bytes.saturating_sub(bytes);
                lane.first_at = if lane.buf.is_empty() { None } else { Some(now) };
                lane.busy = true;
                let lane_no = u16::try_from(i).unwrap_or(0);
                joins.spawn(run_lane(Arc::clone(&sh), lane_no, recs));
            } else if let Some(d) = due {
                next_deadline = Some(next_deadline.map_or(d, |n: Instant| n.min(d)));
            }
        }
        // Read, or wait.
        let can_read = directive == Directive::Run
            && !sh.paused.load(Ordering::SeqCst)
            && inflight < s.max_inflight_bytes;
        #[allow(clippy::cast_precision_loss)] // a gauge
        metrics::gauge!("sprig_sink_inflight_bytes").set(inflight as f64);
        if can_read {
            let wait = next_deadline
                .map_or(Duration::from_millis(100), |d| {
                    d.saturating_duration_since(now)
                })
                .min(Duration::from_millis(100));
            let room =
                usize::try_from((s.max_inflight_bytes - inflight).min(s.batch_max_bytes * 4))
                    .unwrap_or(usize::MAX);
            let r = tokio::select! {
                r = source.next_batch(s.batch_max_records.max(1) * usize::from(lanes_n).min(4), room as u64, wait) => Some(r),
                () = cancel.cancelled() => None,
            };
            let Some(r) = r else {
                continue;
            };
            match r {
                Ok(recs) => {
                    read_failures = 0;
                    for rec in recs {
                        let pos = rec.pos.clone();
                        settler.read(&pos);
                        let prepared = match prepare.prepare(rec.clone()) {
                            Ok(out) if !out.is_empty() => prepare.transform(out).await,
                            other => other,
                        };
                        match prepared {
                            Ok(out) if out.is_empty() => settler.done(&pos),
                            Ok(out) => {
                                // A record that fans out settles when its
                                // last part is done: its parts share its
                                // position, so the first done marks it;
                                // parts go to one lane, in order.
                                let lane = lane_of(&rec, lanes_n);
                                let l = &mut lanes[usize::from(lane)];
                                for o in out {
                                    let sz = o.size();
                                    inflight += sz;
                                    l.bytes += sz;
                                    l.buf.push_back(o);
                                }
                                if l.first_at.is_none() {
                                    l.first_at = Some(Instant::now());
                                }
                            }
                            Err(reason) => {
                                metrics::counter!("sprig_sink_transform_failures_total")
                                    .increment(1);
                                if dead_letter(&sh, &rec, &reason, "transform", 1).await {
                                    settler.done(&pos);
                                }
                            }
                        }
                    }
                }
                Err(e) if e.kind == sprig_common::ErrorKind::Sprig(SprigCode::SinkSourceLost) => {
                    let _ = control.report(Event::SourceLost(e.message.clone())).await;
                    end = Some(TaskEnd::Stopped);
                }
                Err(e) => {
                    read_failures += 1;
                    tracing::warn!(error = %e, "sink source read failed; retrying");
                    let wait = backoff(read_failures, None, s.retry_initial, s.retry_max, &sh.rng);
                    tokio::select! {
                        () = tokio::time::sleep(wait) => {}
                        () = cancel.cancelled() => {}
                    }
                }
            }
        } else {
            let wait = next_deadline
                .map_or(Duration::from_millis(100), |d| {
                    d.saturating_duration_since(now)
                })
                .clamp(Duration::from_millis(1), Duration::from_millis(100));
            tokio::select! {
                r = joins.join_next(), if !joins.is_empty() => {
                    if let Some(r) = r {
                        arrived.push_back(r);
                    }
                }
                () = tokio::time::sleep(wait) => {}
                () = cancel.cancelled() => {}
            }
        }
    }
}

#[cfg(test)]
mod quota_tests {
    #[test]
    fn quota_split_follows_demand_and_adds_up() {
        // One worker alone gets everything.
        assert_eq!(super::split(1000, 50, &[50]), 1000);
        // Two busy workers split evenly; a busy and an idle one about 10:1.
        assert_eq!(super::split(1000, 500, &[500, 500]), 500);
        let busy = super::split(1000, 900, &[900, 0]);
        let idle = super::split(1000, 0, &[900, 0]);
        assert!(busy > 9 * idle && busy + idle <= 1000, "{busy} {idle}");
        // Never more than the budget in all.
        let d = [10_u64, 2000, 0, 77];
        let sum: u64 = d.iter().map(|m| super::split(1000, *m, &d)).sum();
        assert!((990..=1000).contains(&sum), "{sum}");
    }

    /// Two workers' buckets with shares from one window deliver at most
    /// the budget between them.
    #[tokio::test(start_paused = true)]
    async fn two_workers_share_one_budget() {
        let a = super::Quota::new(10_000);
        let b = super::Quota::new(10_000);
        a.set_local(super::split(10_000, 8_000, &[8_000, 8_000]));
        b.set_local(super::split(10_000, 8_000, &[8_000, 8_000]));
        let start = tokio::time::Instant::now();
        let fa = async {
            for _ in 0..50 {
                a.take(1_000).await;
            }
        };
        let fb = async {
            for _ in 0..50 {
                b.take(1_000).await;
            }
        };
        tokio::join!(fa, fb);
        // 100,000 bytes at 10,000 a second in all (each starts with one
        // second of its share): about 9 s, never under 8.
        let took = start.elapsed().as_secs_f64();
        assert!(took >= 8.0, "{took}");
    }
}

#[cfg(test)]
mod settler_tests {
    use super::Settler;
    use crate::model::Position;

    fn at(stream: u64, partition: u32, offset: u64) -> Position {
        Position::Stream {
            stream,
            partition,
            offset,
            epoch: 1,
        }
    }

    #[test]
    fn fan_in_members_settle_apart() {
        // Two member instances of a fan-in task (slots 0 and
        // 1) with records at the same partition and offset. Member B's
        // record done must not settle member A's, still in flight.
        let (a, b) = (
            crate::source::FANIN_SLOT_FLAG,
            crate::source::FANIN_SLOT_FLAG | 1,
        );
        let mut s = Settler::default();
        s.read(&at(a, 0, 7));
        s.read(&at(b, 0, 7));
        s.done(&at(b, 0, 7));
        assert_eq!(s.drain(), vec![at(b, 0, 7)], "only B's");
        assert_eq!(s.pending(), 1, "A's still tracked");
        s.done(&at(a, 0, 7));
        assert_eq!(s.drain(), vec![at(a, 0, 7)]);
        // A plain stream keeps the partition as its unit.
        assert_eq!(at(42, 3, 9).unit_seq(), Some((3, 9)));
    }
}
