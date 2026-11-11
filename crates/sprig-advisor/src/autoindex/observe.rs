//! Observation, usage judgement, retirement and dropping (chapter 13 build
//! step 11, "Observing and dropping"; decision O1), and the reconciler that
//! keeps the store and the shards' real indexes in step ([`drift`]).
//!
//! ```text
//! observing, daily (watch_every):     # for auto_index.observe_days after the build
//!   targets' p95 since the build vs the baseline, runs-weighted:
//!     improvement < min_improvement_pct                       → retire(no_gain)
//!   a fingerprint on t whose plan uses x slower than
//!     max_regression_pct                                      → retire(regression)
//!   write fingerprints on t slower than max_regression_pct and
//!     measured benefit < 2 × measured write cost              → retire(write_cost)
//!   period over                                               → active
//! observing and active, every usage_every (auto_index.eval_interval_minutes):
//!   reads := runs of read fingerprints on t since the build or the last judgement
//!   reads >= usage_sample_reads → judge (usage::judge): the EXPLAIN of each
//!     read fingerprint's sample says whether it uses x
//!     served < min_usage_ratio of the reads and saved <= write cost → retire(unused)
//!     else the next sample starts now
//! observing and active, every pass:
//!   a user or migrated index equals or extends x on every shard → drop(superseded)
//! retire(x, reason):
//!   instant IGNORED toggle: ALTER INDEX x IGNORED on each shard; retiring
//!   while retiring: a fingerprint x served regresses → NOT IGNORED; active
//!   usage_sample_reads reads on t since it was ignored, none regressed → dropping
//!   dropping: DROP INDEX IF EXISTS x on each shard; dropped
//! failed: whatever a failed rollout left is dropped the same way
//! ```
//!
//! Nothing here counts days of disuse (decision O1): an index on a quiet
//! table is never judged on too little evidence, and an index on a busy one
//! is judged quickly. [`user`] applies the same rule to user and migrated
//! indexes, under `auto_index.retire_user_indexes`.
//!
//! Workload numbers come through [`WorkloadSource`] (chapter 12's store):
//! `None` means "unknown", and nothing is judged on it — an index is never
//! dropped because the workload store is missing.
//!
//! **Usage** comes from `EXPLAIN` of a bound sample of each of the table's
//! most-run read fingerprints since the sample began: per fingerprint, not
//! per row, which the ratio needs. (`INDEX_STATISTICS` counts rows read per
//! index and needs `userstat = ON`, which this chapter never sets.)
//!
//! **Automatic indexes** are acted on only through
//! [`node::auto_ddl`](super::rollout::node::auto_ddl), which refuses any
//! other name; [`user`] has its own statements and never touches a
//! primary key, a `UNIQUE` index, an index backing a foreign key or an
//! automatic index.

pub mod drift;
#[cfg(test)]
mod tests;
pub mod usage;
pub mod user;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_cluster::sql::literal;
use sprig_common::ids::{Fingerprint, ObjectId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

use super::model::{IndexDef, KeyPart, Origin};
use super::policy::ObserveRules;
use super::rollout::node::{Node, NodeIndex, drop_sql, ignore_sql, ms};
use super::rollout::{ClusterView, RolloutSettings, ShardTable, unix_ms};
use super::store::{
    AutoIndexRow, AutoIndexStore, DecisionLog, DropReason, IndexDecision, IndexState, StateChange,
    origin_of,
};

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// One fingerprint's latency over a period.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FpLatency {
    /// Runs.
    pub runs: u64,
    /// p95, ms.
    pub p95_ms: f64,
    /// Mean, ms.
    pub mean_ms: f64,
    /// Its plan uses the index, when known (sampled `EXPLAIN`).
    pub uses_index: Option<bool>,
    /// An `INSERT`, `UPDATE`, `DELETE` or `REPLACE`.
    pub write: bool,
    /// Rows a write run changes, on average (0 when unknown or a read).
    #[serde(default)]
    pub affected_per_run: f64,
    /// The columns an `UPDATE` sets, lower case; `None` for inserts,
    /// deletes and updates whose columns aren't known (they change every
    /// index).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated: Option<Vec<String>>,
}

impl FpLatency {
    /// A read's latency.
    #[must_use]
    pub fn read(runs: u64, p95_ms: f64, mean_ms: f64) -> Self {
        Self {
            runs,
            p95_ms,
            mean_ms,
            uses_index: None,
            write: false,
            affected_per_run: 0.0,
            updated: None,
        }
    }
}

/// A sampled statement on the table, bound and in physical names, for the
/// usage `EXPLAIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSample {
    /// Its fingerprint.
    pub fingerprint: Fingerprint,
    /// The default database to run it in.
    pub schema: String,
    /// The statement.
    pub sql: String,
}

/// Chapter 12's workload store, as observation reads it. `None` means the
/// numbers aren't known.
#[async_trait::async_trait]
pub trait WorkloadSource: Send + Sync {
    /// Latency of every fingerprint on `object` from `since` to `until`.
    ///
    /// # Errors
    /// The store couldn't be read.
    async fn latency(
        &self,
        tenant: TenantId,
        object: ObjectId,
        since: SystemTime,
        until: SystemTime,
    ) -> Result<Option<BTreeMap<Fingerprint, FpLatency>>>;

    /// The `n` fingerprints on `object` with the most total time since
    /// `since` (the last usage check, else the build), each with one bound
    /// sample.
    ///
    /// # Errors
    /// The store couldn't be read.
    async fn samples(
        &self,
        tenant: TenantId,
        object: ObjectId,
        n: usize,
        since: SystemTime,
    ) -> Result<Option<Vec<UsageSample>>>;

    /// The fingerprints of `fps` the workload store still holds (a
    /// periodic job's fingerprint stays until retention forgets it).
    /// `None`: unknown, and every remembered fingerprint counts.
    ///
    /// # Errors
    /// The store couldn't be read.
    async fn alive(
        &self,
        _tenant: TenantId,
        _fps: &[Fingerprint],
    ) -> Result<Option<std::collections::BTreeSet<Fingerprint>>> {
        Ok(None)
    }

    /// The objects read from `since` to `until`: the tables whose user
    /// indexes are judged (decision O1). `None`: unknown.
    ///
    /// # Errors
    /// The store couldn't be read.
    async fn objects_read(
        &self,
        _tenant: TenantId,
        _since: SystemTime,
        _until: SystemTime,
    ) -> Result<Option<std::collections::BTreeSet<ObjectId>>> {
        Ok(None)
    }
}

/// No workload store yet: everything unknown.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoWorkload;

#[async_trait::async_trait]
impl WorkloadSource for NoWorkload {
    async fn latency(
        &self,
        _tenant: TenantId,
        _object: ObjectId,
        _since: SystemTime,
        _until: SystemTime,
    ) -> Result<Option<BTreeMap<Fingerprint, FpLatency>>> {
        Ok(None)
    }
    async fn samples(
        &self,
        _tenant: TenantId,
        _object: ObjectId,
        _n: usize,
        _since: SystemTime,
    ) -> Result<Option<Vec<UsageSample>>> {
        Ok(None)
    }
}

/// Observation settings besides [`ObserveRules`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ObserveSettings {
    /// The watch's period (the chapter's daily).
    pub watch_every: Duration,
    /// How often a table's reads are counted for the usage judgement:
    /// `auto_index.eval_interval_minutes` (60). A cadence only; nothing is
    /// decided by it.
    pub usage_every: Duration,
    /// Runs a fingerprint needs before its latency counts.
    pub min_runs: u64,
    /// Most-run read fingerprints explained for usage.
    pub usage_samples: usize,
}

impl Default for ObserveSettings {
    fn default() -> Self {
        Self {
            watch_every: Duration::from_secs(24 * 3600),
            usage_every: Duration::from_secs(3600),
            min_runs: 5,
            usage_samples: 20,
        }
    }
}

/// What the watch concluded.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WatchVerdict {
    /// Keep watching.
    Continue,
    /// The period is over without a reason to retire: `active`.
    Activate,
    /// Retire it.
    Retire(DropReason),
}

/// The watch's measures, kept in `observed.watch`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WatchMeasures {
    /// Runs-weighted p95 improvement of the targets, percent.
    pub improvement_pct: Option<f64>,
    /// The worst slowdown of a fingerprint using the index, percent.
    pub worst_regression_pct: Option<f64>,
    /// The fingerprint of that slowdown.
    pub worst_fp: Option<String>,
    /// Measured benefit, ms per hour.
    pub benefit_ms_h: Option<f64>,
    /// Measured write cost, ms per hour.
    pub write_cost_ms_h: Option<f64>,
    /// The worst write slowdown, percent.
    pub worst_write_pct: Option<f64>,
}

/// Judges one watch check (pure).
///
/// `baseline` holds the fingerprints measured before the build (by 16-hex
/// id), `targets` the candidate's primaries, `now` what was measured over
/// `hours` since the build; `period_over` whether `observe_days` passed.
#[must_use]
pub fn judge_watch(
    baseline: &BTreeMap<String, super::store::Baseline>,
    targets: &[Fingerprint],
    now: &BTreeMap<Fingerprint, FpLatency>,
    hours: f64,
    rules: &ObserveRules,
    min_runs: u64,
    period_over: bool,
) -> (WatchVerdict, WatchMeasures) {
    let mut m = WatchMeasures::default();
    let hours = hours.max(1.0 / 60.0);
    let base = |f: &Fingerprint| baseline.get(&f.to_string());
    // Targets.
    let (mut before, mut after, mut benefit) = (0.0, 0.0, 0.0);
    for f in targets {
        if let (Some(b), Some(n)) = (base(f), now.get(f))
            && n.runs >= min_runs
            && b.p95_ms > 0.0
        {
            #[allow(clippy::cast_precision_loss)] // runs are far below 2^52
            let w = n.runs as f64;
            before += w * b.p95_ms;
            after += w * n.p95_ms;
            benefit += w / hours * (b.mean_ms - n.mean_ms);
        }
    }
    if before > 0.0 {
        m.improvement_pct = Some(100.0 * (1.0 - after / before));
        m.benefit_ms_h = Some(benefit);
    }
    // Regressions of readers using the index, and write costs.
    let mut write_cost = 0.0;
    for (f, n) in now {
        let Some(b) = base(f) else { continue };
        if n.runs < min_runs || b.p95_ms <= 0.0 || targets.contains(f) {
            continue;
        }
        let slower = 100.0 * (n.p95_ms / b.p95_ms - 1.0);
        if n.write {
            #[allow(clippy::cast_precision_loss)]
            let per_h = n.runs as f64 / hours;
            write_cost += per_h * (n.mean_ms - b.mean_ms).max(0.0);
            if m.worst_write_pct.is_none_or(|w| slower > w) {
                m.worst_write_pct = Some(slower);
            }
        } else if n.uses_index == Some(true) && m.worst_regression_pct.is_none_or(|w| slower > w) {
            m.worst_regression_pct = Some(slower);
            m.worst_fp = Some(f.to_string());
        }
    }
    m.write_cost_ms_h = Some(write_cost);
    if m.worst_regression_pct
        .is_some_and(|w| w > rules.max_regression_pct)
    {
        return (WatchVerdict::Retire(DropReason::Regression), m);
    }
    if m.worst_write_pct
        .is_some_and(|w| w > rules.max_regression_pct)
        && m.benefit_ms_h.unwrap_or(0.0) < 2.0 * write_cost
    {
        return (WatchVerdict::Retire(DropReason::WriteCost), m);
    }
    if m.improvement_pct
        .is_some_and(|i| i < rules.min_improvement_pct)
    {
        return (WatchVerdict::Retire(DropReason::NoGain), m);
    }
    if period_over {
        return (WatchVerdict::Activate, m);
    }
    (WatchVerdict::Continue, m)
}

/// Whether `user` equals or extends `ours`: our parts are a prefix of its
/// parts (columns, prefix lengths at least ours, directions).
#[must_use]
pub fn supersedes(user: &[KeyPart], ours: &[KeyPart]) -> bool {
    user.len() >= ours.len()
        && ours.iter().zip(user).all(|(o, u)| {
            o.column == u.column
                && o.desc == u.desc
                && match (o.prefix, u.prefix) {
                    (_, None) => true,
                    (None, Some(_)) => false,
                    (Some(a), Some(b)) => b >= a,
                }
        })
}

/// The user or migrated index on a shard that supersedes `def`, if any.
#[must_use]
pub fn superseding<'a>(
    indexes: &'a BTreeMap<String, NodeIndex>,
    recorded: &BTreeMap<String, Origin>,
    def: &IndexDef,
) -> Option<&'a NodeIndex> {
    indexes.values().find(|i| {
        let origin = origin_of(&i.name, recorded.get(&i.name).copied());
        matches!(origin, Origin::User | Origin::Migrated)
            && !i.ignored
            && supersedes(&i.parts, &def.parts)
    })
}

/// Everything one observation step needs besides the row.
pub struct ObserveCx<'a> {
    /// The tenant.
    pub tenant: TenantId,
    /// The cluster.
    pub view: &'a ClusterView,
    /// The object's shards.
    pub shards: &'a [ShardTable],
    /// The object's rules.
    pub rules: &'a ObserveRules,
    /// Cadence and thresholds.
    pub settings: &'a ObserveSettings,
    /// Lock waits and retries for retire and drop statements.
    pub ddl: &'a RolloutSettings,
    /// Chapter 12's numbers.
    pub workload: &'a dyn WorkloadSource,
    /// Decisions.
    pub decisions: &'a dyn DecisionLog,
    /// Now.
    pub now: SystemTime,
    /// Chapter 52's gate for removing an index; `None`: never held.
    pub removals: Option<&'a dyn RemovalGate>,
}

/// Chapter 52's gate before an index is removed: an automatic index's
/// retirement or drop, or a user or migrated index's removal (decision
/// O1). Under `approve` it holds the removal until someone approves it;
/// the observer proposes it again on its next judgement, so an approved
/// removal goes ahead only if the evidence still asks for it.
#[async_trait::async_trait]
pub trait RemovalGate: Send + Sync {
    /// Whether to remove now.
    async fn removal(&self, r: &Removal<'_>) -> RemovalVerdict;
    /// The removal a [`RemovalVerdict::Go`] ticket let through started
    /// (`ok`), or failed.
    async fn removed(&self, ticket: &str, ok: bool, detail: &str);
}

/// One proposed removal.
#[derive(Debug, Clone, PartialEq)]
pub struct Removal<'a> {
    /// The table.
    pub object: ObjectId,
    /// The index.
    pub index: &'a str,
    /// A user or migrated index (else an automatic one).
    pub user: bool,
    /// Why: `unused`, `no_gain`, `regression`, `write_cost`, `budget`,
    /// `superseded`.
    pub reason: &'a str,
    /// Its columns, in order.
    pub columns: Vec<String>,
    /// The measurements behind it.
    pub evidence: serde_json::Value,
}

/// What the gate said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovalVerdict {
    /// Remove now; report with the ticket, when there is one.
    Go(Option<String>),
    /// Held (the reason names the item).
    Hold(String),
}

/// Asks `cx`'s gate.
pub(crate) async fn gate_removal(cx: &ObserveCx<'_>, r: &Removal<'_>) -> RemovalVerdict {
    match cx.removals {
        Some(g) => g.removal(r).await,
        None => RemovalVerdict::Go(None),
    }
}

/// Reports a removal a ticket let through.
pub(crate) async fn removal_done(cx: &ObserveCx<'_>, ticket: Option<&str>, ok: bool, detail: &str) {
    if let (Some(g), Some(t)) = (cx.removals, ticket) {
        g.removed(t, ok, detail).await;
    }
}

/// What one observation step did.
#[derive(Debug, Clone, PartialEq)]
pub enum ObserveStep {
    /// Nothing to do now.
    Idle,
    /// Measured; still watching (or in use).
    Checked,
    /// `observing → active`.
    Activated,
    /// `→ retiring` (`IGNORED` on every shard).
    Retiring(DropReason),
    /// `retiring → active`: the targets regressed while it was ignored.
    Restored,
    /// `→ dropped`.
    Dropped(Option<DropReason>),
    /// A statement failed; try again later.
    Failed(String),
    /// The removal waits for an approval (chapter 52); the reason names
    /// the item.
    Held(String),
}

/// `observed.<key>` as `T`.
fn part<T: serde::de::DeserializeOwned + Default>(o: Option<&serde_json::Value>, key: &str) -> T {
    o.and_then(|o| o.get(key))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default()
}

fn with_part<T: Serialize>(o: Option<&serde_json::Value>, key: &str, v: &T) -> serde_json::Value {
    let mut out = match o {
        Some(serde_json::Value::Object(m)) => serde_json::Value::Object(m.clone()),
        _ => serde_json::json!({}),
    };
    if let (Some(m), Ok(v)) = (out.as_object_mut(), serde_json::to_value(v)) {
        m.insert(key.into(), v);
    }
    out
}

/// `observed.watch`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WatchState {
    /// The last check, Unix ms.
    #[serde(default)]
    pub last_check_ms: u64,
    /// Its measures.
    #[serde(default)]
    pub measures: WatchMeasures,
}

/// `observed.usage`: the usage judgement's state (decision O1).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageState {
    /// The last count of reads, Unix ms.
    #[serde(default)]
    pub last_check_ms: u64,
    /// Where the current sample of reads began (the build or the last
    /// judgement), Unix ms; 0 until the first count.
    #[serde(default)]
    pub since_ms: u64,
    /// Reads on the table since `since_ms` at the last count.
    #[serde(default)]
    pub reads: u64,
    /// How usage was told: `explain`, or `unknown` (no workload numbers or
    /// samples).
    #[serde(default)]
    pub method: String,
    /// The last judgement's measures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<usage::UsageMeasures>,
    /// The last judgement's verdict: `keep`, `remove`, `unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
    /// Fingerprints the index served, with their savings: periodic jobs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remembered: BTreeMap<String, usage::Remembered>,
}

/// `observed.retire`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RetireState {
    /// Ignored since, Unix ms.
    #[serde(default)]
    pub since_ms: u64,
    /// Targets' runs-weighted p95 before it was ignored, ms (rows retired
    /// before decision O1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_before_ms: Option<f64>,
    /// The fingerprints it served, as they ran before it was ignored.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub before: BTreeMap<String, usage::Before>,
    /// Reads on the table since it was ignored, at the last count.
    #[serde(default)]
    pub reads: u64,
}

/// When the index started serving: the end of its rollout, else its last
/// state change.
#[must_use]
pub fn built_at(row: &AutoIndexRow) -> SystemTime {
    super::rollout::Progress::of(row.observed.as_ref())
        .built_at_ms
        .map_or(row.state_changed_at, |ms| {
            std::time::UNIX_EPOCH + Duration::from_millis(ms)
        })
}

fn elapsed(from: SystemTime, to: SystemTime) -> Duration {
    to.duration_since(from).unwrap_or_default()
}

/// One observation step for one row: whatever its state needs now.
///
/// # Errors
/// Catalog errors. Statement failures on the shards are
/// [`ObserveStep::Failed`].
pub async fn step<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
) -> Result<ObserveStep> {
    let Some(def) = row.def() else {
        return Ok(ObserveStep::Idle);
    };
    match row.state {
        IndexState::Observing | IndexState::Active => serving(store, q, cx, row, &def).await,
        IndexState::Retiring => retiring(store, q, cx, row, &def).await,
        IndexState::Dropping => {
            drop_all(store, q, cx, row, IndexState::Dropping, row.drop_reason).await
        }
        IndexState::Failed => drop_all(store, q, cx, row, IndexState::Failed, None).await,
        _ => Ok(ObserveStep::Idle),
    }
}

#[allow(clippy::too_many_lines)] // superseded, the watch, usage
async fn serving<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    def: &IndexDef,
) -> Result<ObserveStep> {
    let now_ms = unix_ms(cx.now);
    // Superseded by a user index on every shard.
    let recorded = store.origins(q, row.object).await?;
    let mut on_all = !cx.shards.is_empty();
    for t in cx.shards {
        match shard_indexes(cx, t).await {
            Ok(ix) => {
                if superseding(&ix, &recorded, def).is_none() {
                    on_all = false;
                    break;
                }
            }
            Err(e) => return Ok(ObserveStep::Failed(e)),
        }
    }
    if on_all {
        // The user's index serves whatever ours did: no watch needed.
        return retire(store, q, cx, row, def, DropReason::Superseded, None).await;
    }
    let mut checked = false;
    // The watch.
    if row.state == IndexState::Observing {
        let mut ws: WatchState = part(row.observed.as_ref(), "watch");
        if now_ms.saturating_sub(ws.last_check_ms) >= ms(cx.settings.watch_every) {
            let since = built_at(row);
            let hours = elapsed(since, cx.now).as_secs_f64() / 3600.0;
            let period_over = elapsed(since, cx.now)
                >= Duration::from_secs(u64::from(cx.rules.observe_days) * 86_400);
            let lat = cx
                .workload
                .latency(cx.tenant, row.object, since, cx.now)
                .await?;
            let targets = targets(store, q, row).await?;
            let (verdict, measures) = match &lat {
                Some(l) => judge_watch(
                    &row.baseline,
                    &targets,
                    l,
                    hours,
                    cx.rules,
                    cx.settings.min_runs,
                    period_over,
                ),
                // Unknown numbers: nothing to retire for; the period still ends.
                None if period_over => (WatchVerdict::Activate, WatchMeasures::default()),
                None => (WatchVerdict::Continue, WatchMeasures::default()),
            };
            ws.last_check_ms = now_ms;
            ws.measures = measures;
            let observed = with_part(row.observed.as_ref(), "watch", &ws);
            store.set_observed(q, row.index_id, &observed).await?;
            checked = true;
            match verdict {
                WatchVerdict::Continue => {}
                WatchVerdict::Activate => {
                    let ok = store
                        .set_state(
                            q,
                            row.index_id,
                            &change(IndexState::Observing, IndexState::Active, cx.now, None),
                        )
                        .await?;
                    if ok {
                        log(
                            cx,
                            row,
                            "index.activate",
                            "succeeded",
                            "observed",
                            format!(
                                "{} kept after {} days of observation",
                                row.name, cx.rules.observe_days
                            ),
                            serde_json::to_value(&ws.measures).unwrap_or_default(),
                        )
                        .await;
                        return Ok(ObserveStep::Activated);
                    }
                    return Ok(ObserveStep::Idle);
                }
                WatchVerdict::Retire(reason) => {
                    let mut fresh = row.clone();
                    fresh.observed = Some(observed);
                    let before = lat
                        .as_ref()
                        .map(|l| usage::before_of(targets.iter().copied(), l));
                    return retire(store, q, cx, &fresh, def, reason, before).await;
                }
            }
        }
    }
    // Usage (decision O1): judged by reads, never by days.
    let mut us: UsageState = part(row.observed.as_ref(), "usage");
    if now_ms.saturating_sub(us.last_check_ms) >= ms(cx.settings.usage_every) {
        let columns: Vec<String> = def.parts.iter().map(|p| p.column.clone()).collect();
        let judged = match check_usage(
            cx,
            row.object,
            &def.name(),
            &columns,
            Some(&row.baseline),
            &mut us,
            built_at(row),
        )
        .await
        {
            Ok(j) => j,
            Err(e) => return Ok(ObserveStep::Failed(e)),
        };
        // Re-read: the watch may have just written `observed`.
        let current = store
            .index(q, row.index_id)
            .await?
            .map_or_else(|| row.observed.clone(), |r| r.observed);
        let mut retire_now: Option<BTreeMap<String, usage::Before>> = None;
        if let Some(j) = &judged {
            if j.measures.served > 0 {
                store.touch_used(q, row.index_id, cx.now).await?;
            }
            match j.verdict {
                usage::UsageVerdict::NotYet => {}
                usage::UsageVerdict::Remove => {
                    retire_now = Some(j.before());
                    us.verdict = Some("remove".into());
                }
                v @ (usage::UsageVerdict::Keep | usage::UsageVerdict::Unknown) => {
                    j.next_sample(&mut us, now_ms);
                    us.verdict = Some(
                        if v == usage::UsageVerdict::Keep {
                            "keep"
                        } else {
                            "unknown"
                        }
                        .into(),
                    );
                }
            }
        }
        let observed = with_part(current.as_ref(), "usage", &us);
        store.set_observed(q, row.index_id, &observed).await?;
        checked = true;
        if let Some(before) = retire_now {
            let mut fresh = row.clone();
            fresh.observed = Some(observed);
            return retire(store, q, cx, &fresh, def, DropReason::Unused, Some(before)).await;
        }
    }
    Ok(if checked {
        ObserveStep::Checked
    } else {
        ObserveStep::Idle
    })
}

/// One usage judgement's inputs and result.
#[derive(Debug, Clone)]
pub(crate) struct Judged {
    /// The verdict.
    pub verdict: usage::UsageVerdict,
    /// What it measured.
    pub measures: usage::UsageMeasures,
    /// The sample's reads with their use of the index.
    pub reads: Vec<usage::ReadUse>,
    /// The sample's latencies.
    pub lat: BTreeMap<Fingerprint, FpLatency>,
    /// The fingerprints still in the workload, when known.
    pub alive: Option<std::collections::BTreeSet<Fingerprint>>,
}

impl Judged {
    /// The served fingerprints as they ran: what "regressed while ignored"
    /// compares with.
    pub(crate) fn before(&self) -> BTreeMap<String, usage::Before> {
        usage::before_of(
            self.reads
                .iter()
                .filter(|r| r.uses == Some(true))
                .map(|r| r.fp),
            &self.lat,
        )
    }

    /// Kept: the next sample starts now, and the served fingerprints are
    /// remembered.
    pub(crate) fn next_sample(&self, us: &mut UsageState, now_ms: u64) {
        usage::remember(&mut us.remembered, &self.reads, self.alive.as_ref(), now_ms);
        us.since_ms = now_ms;
        us.reads = 0;
    }
}

/// Counts the reads on `object` since `us.since_ms` (else `fallback`) and,
/// once there are `usage_sample_reads`, judges index `name` over
/// `columns`: `None` when the workload can't tell (nothing is decided),
/// else the verdict ([`usage::UsageVerdict::NotYet`] below the count).
/// `baseline` holds an automatic index's measurements before its build.
///
/// # Errors
/// A statement or the store failed: try again later.
pub(crate) async fn check_usage(
    cx: &ObserveCx<'_>,
    object: ObjectId,
    name: &str,
    columns: &[String],
    baseline: Option<&BTreeMap<String, super::store::Baseline>>,
    us: &mut UsageState,
    fallback: SystemTime,
) -> std::result::Result<Option<Judged>, String> {
    let now_ms = unix_ms(cx.now);
    us.last_check_ms = now_ms;
    if us.since_ms == 0 {
        us.since_ms = unix_ms(fallback).min(now_ms);
    }
    let since = std::time::UNIX_EPOCH + Duration::from_millis(us.since_ms);
    let Some(lat) = cx
        .workload
        .latency(cx.tenant, object, since, cx.now)
        .await
        .map_err(|e| e.to_string())?
    else {
        us.method = "unknown".into();
        return Ok(None);
    };
    us.reads = usage::reads_in(&lat);
    let rules = usage::UsageRules {
        sample_reads: cx.rules.usage_sample_reads,
        min_usage_ratio: cx.rules.min_usage_ratio,
        maintenance_cost_ms: cx.rules.maintenance_cost_ms,
    };
    if us.reads < rules.sample_reads.max(1) {
        return Ok(Some(Judged {
            verdict: usage::UsageVerdict::NotYet,
            measures: usage::UsageMeasures {
                reads: us.reads,
                ..usage::UsageMeasures::default()
            },
            reads: Vec::new(),
            lat,
            alive: None,
        }));
    }
    let Some(samples) = cx
        .workload
        .samples(cx.tenant, object, cx.settings.usage_samples, since)
        .await
        .map_err(|e| e.to_string())?
    else {
        us.method = "unknown".into();
        return Ok(None);
    };
    us.method = "explain".into();
    let plans = explain_samples(cx, &samples).await?;
    let table_rows = table_rows(cx).await?;
    let reads = read_uses(&lat, &plans, name, baseline, table_rows);
    let remembered: Vec<Fingerprint> = us
        .remembered
        .keys()
        .filter_map(|k| k.parse().ok())
        .collect();
    let alive = if remembered.is_empty() {
        None
    } else {
        cx.workload
            .alive(cx.tenant, &remembered)
            .await
            .map_err(|e| e.to_string())?
    };
    let (verdict, measures) = usage::judge(
        &reads,
        usage::entry_changes(&lat, columns),
        &us.remembered,
        alive.as_ref(),
        &rules,
    );
    tracing::info!(
        index = %name, object = object.0, reads = measures.reads, served = measures.served,
        unknown = measures.unknown, ratio = measures.ratio, saved_ms = measures.saved_ms,
        periodic_ms = measures.periodic_saved_ms, write_cost_ms = measures.write_cost_ms,
        ?verdict, "index usage judged"
    );
    us.last = Some(measures.clone());
    Ok(Some(Judged {
        verdict,
        measures,
        reads,
        lat,
        alive,
    }))
}

/// Each read fingerprint of `lat` with whether its plan reads `name` and
/// what a run saves through it.
pub(crate) fn read_uses(
    lat: &BTreeMap<Fingerprint, FpLatency>,
    plans: &BTreeMap<Fingerprint, super::replay::PlanRun>,
    name: &str,
    baseline: Option<&BTreeMap<String, super::store::Baseline>>,
    table_rows: f64,
) -> Vec<usage::ReadUse> {
    lat.iter()
        .filter(|(_, l)| !l.write)
        .map(|(fp, l)| {
            let plan = plans.get(fp);
            let uses = plan.map(|p| p.uses_index(name));
            let saved = match (uses, plan) {
                (Some(true), Some(p)) => {
                    let rows_with: f64 = p
                        .tables
                        .iter()
                        .filter(|t| t.keys.iter().any(|k| k.eq_ignore_ascii_case(name)))
                        .map(|t| t.rows_estimate)
                        .sum();
                    let base = baseline
                        .and_then(|b| b.get(&fp.to_string()))
                        .map(|b| b.mean_ms);
                    usage::saved_per_run(l.mean_ms, base, rows_with, table_rows)
                }
                _ => 0.0,
            };
            usage::ReadUse {
                fp: *fp,
                runs: l.runs,
                uses,
                saved_ms_per_run: saved,
            }
        })
        .collect()
}

/// `EXPLAIN FORMAT=JSON` of each sample on the first shard's writer, by
/// fingerprint. Samples that fail to explain are left out (their reads
/// count as unknown).
///
/// # Errors
/// The writer is unreachable.
pub(crate) async fn explain_samples(
    cx: &ObserveCx<'_>,
    samples: &[UsageSample],
) -> std::result::Result<BTreeMap<Fingerprint, super::replay::PlanRun>, String> {
    let mut out = BTreeMap::new();
    if samples.is_empty() {
        return Ok(out);
    }
    let writer = cx.view.nodes.writer().ok_or("no writer")?;
    let mut c = mysql_async::Conn::new(writer.opts.clone())
        .await
        .map_err(|e| e.to_string())?;
    for s in samples {
        if out.contains_key(&s.fingerprint) {
            continue;
        }
        if c.query_drop(format!("USE {}", quote(&s.schema)))
            .await
            .is_err()
        {
            continue;
        }
        let json: std::result::Result<Option<String>, _> = c
            .query_first(format!("EXPLAIN FORMAT=JSON {}", s.sql))
            .await;
        match json {
            Ok(Some(j)) => match super::replay::parse_plan(&j) {
                Ok(p) => {
                    out.insert(s.fingerprint, p);
                }
                Err(e) => {
                    tracing::info!(error = %e, fingerprint = %s.fingerprint, "usage plan unparsed");
                }
            },
            Ok(None) => {}
            Err(e) => {
                tracing::info!(error = %e, fingerprint = %s.fingerprint, "usage EXPLAIN failed");
            }
        }
    }
    let _ = c.disconnect().await;
    Ok(out)
}

/// The table's rows over its shards, from `information_schema.TABLES`.
///
/// # Errors
/// The writer is unreachable.
pub(crate) async fn table_rows(cx: &ObserveCx<'_>) -> std::result::Result<f64, String> {
    let w = cx.view.nodes.writer().ok_or("no writer")?;
    let mut n = Node::connect(w, PROBE_TIMEOUT)
        .await
        .map_err(|e| e.to_string())?;
    let mut rows = 0.0;
    for t in cx.shards {
        let v = n
            .scalar(&format!(
                "SELECT IFNULL(TABLE_ROWS, 0) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await
            .map_err(|e| e.to_string())?;
        rows += v.and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    }
    n.close().await;
    Ok(rows)
}

/// The candidate's primary fingerprints, else every baseline fingerprint.
async fn targets<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    row: &AutoIndexRow,
) -> Result<Vec<Fingerprint>> {
    let from_candidate = store
        .candidate(q, row.candidate_id)
        .await?
        .map(|c| {
            c.fingerprints
                .iter()
                .filter(|(_, r)| *r == super::model::Role::Primary)
                .map(|(f, _)| *f)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !from_candidate.is_empty() {
        return Ok(from_candidate);
    }
    Ok(row
        .baseline
        .keys()
        .filter_map(|k| k.parse::<Fingerprint>().ok())
        .collect())
}

fn targets_p95(targets: &[Fingerprint], l: &BTreeMap<Fingerprint, FpLatency>) -> Option<f64> {
    let (mut w, mut s) = (0.0, 0.0);
    for f in targets {
        if let Some(n) = l.get(f) {
            #[allow(clippy::cast_precision_loss)]
            let r = n.runs as f64;
            w += r;
            s += r * n.p95_ms;
        }
    }
    (w > 0.0).then(|| s / w)
}

async fn shard_indexes(
    cx: &ObserveCx<'_>,
    t: &ShardTable,
) -> std::result::Result<BTreeMap<String, NodeIndex>, String> {
    let w = cx.view.nodes.writer().ok_or("no writer")?;
    let mut n = Node::connect(w, PROBE_TIMEOUT)
        .await
        .map_err(|e| e.to_string())?;
    let r = n.indexes(t).await.map_err(|e| e.to_string());
    n.close().await;
    r
}

fn change(from: IndexState, to: IndexState, at: SystemTime, r: Option<DropReason>) -> StateChange {
    StateChange {
        from,
        to,
        at,
        drop_reason: r,
    }
}

/// Retires `row`: through `IGNORED` when the cluster toggles it instantly,
/// else dropped at once. A superseded index is dropped at once too: the
/// user's index serves whatever it did. `before` holds the fingerprints it
/// served as they ran (else its targets' latency over the last
/// `watch_every` is read): what "regressed while ignored" compares with.
/// Chapter 52's gate is asked first.
#[allow(clippy::too_many_lines)] // the gate, the toggle and the drop
async fn retire<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    def: &IndexDef,
    reason: DropReason,
    before: Option<BTreeMap<String, usage::Before>>,
) -> Result<ObserveStep> {
    let name = def.name();
    let proposal = Removal {
        object: row.object,
        index: &name,
        user: false,
        reason: reason.as_str(),
        columns: def.parts.iter().map(|p| p.column.clone()).collect(),
        evidence: serde_json::json!({
            "reason": reason.as_str(),
            "usage": row.observed.as_ref().and_then(|o| o.get("usage")),
            "watch": row.observed.as_ref().and_then(|o| o.get("watch")),
        }),
    };
    let ticket = match gate_removal(cx, &proposal).await {
        RemovalVerdict::Go(t) => t,
        RemovalVerdict::Hold(why) => return Ok(ObserveStep::Held(why)),
    };
    if cx.view.instant_ignored_toggle && reason != DropReason::Superseded {
        let before = if let Some(b) = before {
            b
        } else {
            let from = cx
                .now
                .checked_sub(cx.settings.watch_every)
                .unwrap_or(cx.now);
            let targets = targets(store, q, row).await?;
            cx.workload
                .latency(cx.tenant, row.object, from, cx.now)
                .await?
                .map(|l| usage::before_of(targets.iter().copied(), &l))
                .unwrap_or_default()
        };
        for t in cx.shards {
            if let Err(e) = run_on_writer(cx, t, &ignore_sql(t, &name, true)?).await {
                removal_done(cx, ticket.as_deref(), false, &e).await;
                return Ok(ObserveStep::Failed(e));
            }
        }
        let rs = RetireState {
            since_ms: unix_ms(cx.now),
            p95_before_ms: None,
            before,
            reads: 0,
        };
        store
            .set_observed(
                q,
                row.index_id,
                &with_part(row.observed.as_ref(), "retire", &rs),
            )
            .await?;
        let ok = store
            .set_state(
                q,
                row.index_id,
                &change(row.state, IndexState::Retiring, cx.now, Some(reason)),
            )
            .await?;
        if ok {
            log(
                cx,
                row,
                "index.retire",
                "scheduled",
                reason.as_str(),
                format!(
                    "{} ignored ({}); dropped after {} more reads on the table unless a query \
                     it served gets slower",
                    row.name,
                    reason.as_str(),
                    cx.rules.usage_sample_reads
                ),
                serde_json::json!({"observed": row.observed}),
            )
            .await;
        }
        removal_done(
            cx,
            ticket.as_deref(),
            ok,
            &format!(
                "{} ignored; dropped after {} more reads unless a query it served gets slower",
                row.name, cx.rules.usage_sample_reads
            ),
        )
        .await;
        return Ok(ObserveStep::Retiring(reason));
    }
    let ok = store
        .set_state(
            q,
            row.index_id,
            &change(row.state, IndexState::Dropping, cx.now, Some(reason)),
        )
        .await?;
    removal_done(cx, ticket.as_deref(), ok, &format!("{} dropping", row.name)).await;
    if !ok {
        return Ok(ObserveStep::Idle);
    }
    log(
        cx,
        row,
        "index.retire",
        "scheduled",
        reason.as_str(),
        format!("{} dropped ({})", row.name, reason.as_str()),
        serde_json::json!({"observed": row.observed}),
    )
    .await;
    drop_all(store, q, cx, row, IndexState::Dropping, Some(reason)).await
}

/// What the watch of an `IGNORED` index concluded (decision O1).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IgnoredWatch {
    /// Unknown numbers, or fewer reads than `usage_sample_reads`: keep
    /// waiting (the reads so far).
    Wait(u64),
    /// A fingerprint it served got slower: make it visible again.
    Regressed(usage::Regressed),
    /// Enough reads and nothing slower: drop it.
    Drop(u64),
}

/// Watches an ignored index over the reads since `rs.since_ms`.
///
/// # Errors
/// The store couldn't be read.
pub(crate) async fn watch_ignored(
    cx: &ObserveCx<'_>,
    object: ObjectId,
    rs: &RetireState,
    legacy_targets: &[Fingerprint],
) -> Result<IgnoredWatch> {
    let since = std::time::UNIX_EPOCH + Duration::from_millis(rs.since_ms);
    let Some(l) = cx
        .workload
        .latency(cx.tenant, object, since, cx.now)
        .await?
    else {
        return Ok(IgnoredWatch::Wait(rs.reads));
    };
    if let Some(r) = usage::regressed(
        &rs.before,
        &l,
        cx.rules.max_regression_pct,
        cx.settings.min_runs,
    ) {
        return Ok(IgnoredWatch::Regressed(r));
    }
    // Rows retired before decision O1 kept only their targets' p95.
    if let Some(before) = rs.p95_before_ms.filter(|b| *b > 0.0)
        && let Some(now) = targets_p95(legacy_targets, &l)
        && 100.0 * (now / before - 1.0) > cx.rules.max_regression_pct
        && now - before > usage::NOISE_FLOOR_MS
    {
        return Ok(IgnoredWatch::Regressed(usage::Regressed {
            fp: "targets".into(),
            before_ms: before,
            now_ms: now,
        }));
    }
    let reads = usage::reads_in(&l);
    if reads >= cx.rules.usage_sample_reads.max(1) {
        Ok(IgnoredWatch::Drop(reads))
    } else {
        Ok(IgnoredWatch::Wait(reads))
    }
}

async fn retiring<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    def: &IndexDef,
) -> Result<ObserveStep> {
    let mut rs: RetireState = part(row.observed.as_ref(), "retire");
    let targets = if rs.p95_before_ms.is_some() {
        targets(store, q, row).await?
    } else {
        Vec::new()
    };
    match watch_ignored(cx, row.object, &rs, &targets).await? {
        IgnoredWatch::Wait(reads) => {
            if reads != rs.reads {
                rs.reads = reads;
                store
                    .set_observed(
                        q,
                        row.index_id,
                        &with_part(row.observed.as_ref(), "retire", &rs),
                    )
                    .await?;
            }
            Ok(ObserveStep::Idle)
        }
        IgnoredWatch::Regressed(r) => {
            for t in cx.shards {
                if let Err(e) = run_on_writer(cx, t, &ignore_sql(t, &def.name(), false)?).await {
                    return Ok(ObserveStep::Failed(e));
                }
            }
            // A new usage sample starts now.
            let mut us: UsageState = part(row.observed.as_ref(), "usage");
            us.since_ms = unix_ms(cx.now);
            us.reads = 0;
            store
                .set_observed(
                    q,
                    row.index_id,
                    &with_part(row.observed.as_ref(), "usage", &us),
                )
                .await?;
            if store
                .set_state(
                    q,
                    row.index_id,
                    &change(IndexState::Retiring, IndexState::Active, cx.now, None),
                )
                .await?
            {
                log(
                    cx,
                    row,
                    "index.restore",
                    "succeeded",
                    "regressed_while_ignored",
                    format!(
                        "{}: fingerprint {} ran {:.1} ms (p95) against {:.1} ms while ignored; kept",
                        row.name, r.fp, r.now_ms, r.before_ms
                    ),
                    serde_json::json!({"fingerprint": r.fp, "before_ms": r.before_ms, "now_ms": r.now_ms}),
                )
                .await;
            }
            Ok(ObserveStep::Restored)
        }
        IgnoredWatch::Drop(reads) => {
            rs.reads = reads;
            store
                .set_observed(
                    q,
                    row.index_id,
                    &with_part(row.observed.as_ref(), "retire", &rs),
                )
                .await?;
            if !store
                .set_state(
                    q,
                    row.index_id,
                    &change(
                        IndexState::Retiring,
                        IndexState::Dropping,
                        cx.now,
                        row.drop_reason,
                    ),
                )
                .await?
            {
                return Ok(ObserveStep::Idle);
            }
            drop_all(store, q, cx, row, IndexState::Dropping, row.drop_reason).await
        }
    }
}

/// Drops the index on every shard, then `from → (dropping →) dropped`.
async fn drop_all<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    from: IndexState,
    reason: Option<DropReason>,
) -> Result<ObserveStep> {
    if from == IndexState::Failed {
        // `failed` is terminal: drop what the rollout left on the shards and
        // keep the row as history.
        if cx
            .shards
            .iter()
            .all(|t| row.shards.get(&t.shard) == Some(&super::store::ShardBuild::Dropped))
        {
            return Ok(ObserveStep::Idle);
        }
        for t in cx.shards {
            if let Err(e) = run_on_writer(cx, t, &drop_sql(t, &row.name)?).await {
                return Ok(ObserveStep::Failed(e));
            }
            store
                .set_shard(q, row.index_id, &t.shard, super::store::ShardBuild::Dropped)
                .await?;
        }
        return Ok(ObserveStep::Checked);
    }
    for t in cx.shards {
        if let Err(e) = run_on_writer(cx, t, &drop_sql(t, &row.name)?).await {
            return Ok(ObserveStep::Failed(e));
        }
        store
            .set_shard(q, row.index_id, &t.shard, super::store::ShardBuild::Dropped)
            .await?;
    }
    let ok = store
        .set_state(
            q,
            row.index_id,
            &change(from, IndexState::Dropped, cx.now, reason),
        )
        .await?;
    if ok {
        metrics::counter!(
            "sprig_advisor_autoindex_drops_total",
            "reason" => reason.map_or("failed_rollout", DropReason::as_str)
        )
        .increment(1);
        log(
            cx,
            row,
            "index.drop",
            "succeeded",
            reason.map_or("failed_rollout", DropReason::as_str),
            format!("{} dropped on {} shard(s)", row.name, cx.shards.len()),
            serde_json::json!({}),
        )
        .await;
    }
    Ok(ObserveStep::Dropped(reason))
}

/// Runs one index statement on the shard's writer with a short lock wait,
/// retried on lock waits. On Galera the session's default (TOI) applies:
/// dropping and toggling `IGNORED` are metadata-only.
pub(crate) async fn run_on_writer(
    cx: &ObserveCx<'_>,
    _t: &ShardTable,
    sql: &str,
) -> std::result::Result<(), String> {
    let w = cx.view.nodes.writer().ok_or("no writer")?;
    run_ddl(w, sql, cx.ddl).await
}

/// Runs `sql` on `ep` with `lock_wait_timeout`, retrying 1205.
///
/// # Errors
/// The node is unreachable or the statement failed, as text.
pub async fn run_ddl(
    ep: &sprig_cluster::sql::NodeEndpoint,
    sql: &str,
    s: &RolloutSettings,
) -> std::result::Result<(), String> {
    let mut n = Node::connect(ep, s.max_build)
        .await
        .map_err(|e| e.to_string())?;
    let r = async {
        n.exec(&format!(
            "SET SESSION lock_wait_timeout = {}",
            s.ddl_lock_wait.as_secs().max(1)
        ))
        .await
        .map_err(|e| e.to_string())?;
        let mut tries = 0;
        loop {
            tries += 1;
            match n.exec(sql).await {
                Ok(()) => return Ok(()),
                Err(e) if e.errno == Some(1205) && tries < s.lock_retries => {
                    tracing::info!(tries, "index DDL waited for its lock; retrying");
                    tokio::time::sleep(s.lock_retry_pause.min(Duration::from_secs(5))).await;
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    .await;
    n.close().await;
    r
}

async fn log(
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    kind: &'static str,
    outcome: &'static str,
    reason: &str,
    summary: String,
    evidence: serde_json::Value,
) {
    let d = IndexDecision {
        tenant: cx.tenant,
        kind,
        subject_type: "index",
        subject_id: format!("{}/{}", row.object.0, row.name),
        mode: "applied",
        outcome,
        reason_code: reason.to_owned(),
        summary,
        evidence,
        alternatives: None,
        at: cx.now,
    };
    if let Err(e) = cx.decisions.record(&d).await {
        tracing::warn!(error = %e, "decision not recorded");
    }
}

/// `Internal` for a row whose name isn't an automatic index (never acted
/// on).
///
/// # Errors
/// As described.
pub fn check_auto(row: &AutoIndexRow) -> Result<()> {
    if sprig_common::names::parse_auto_index(&row.name).is_none() {
        return Err(SprigError::internal(format!(
            "auto_indexes row {} names {}, not an automatic index",
            row.index_id, row.name
        )));
    }
    Ok(())
}

/// Retires `row` now for `reason`, as observation would: the retirement
/// selection asks for when a table is over a tightened budget or a better
/// automatic index replaces it (chapter 13, "Selection under budgets"),
/// which evaluation reports and the loop applies. Only `observing` and
/// `active` rows move; anything else is [`ObserveStep::Idle`].
///
/// # Errors
/// Catalog errors; `Internal` for a row that isn't an automatic index.
pub async fn retire_now<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    reason: DropReason,
) -> Result<ObserveStep> {
    check_auto(row)?;
    let Some(def) = row.def() else {
        return Ok(ObserveStep::Idle);
    };
    if !matches!(row.state, IndexState::Observing | IndexState::Active) {
        return Ok(ObserveStep::Idle);
    }
    retire(store, q, cx, row, &def, reason, None).await
}
