//! Observation, usage tracking, retirement and dropping (chapter 13 build
//! step 11, "Observing and dropping"), and the reconciler that keeps the
//! store and the shards' real indexes in step ([`drift`]).
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
//! observing and active, every usage_check_hours:
//!   used := INDEX_STATISTICS.ROWS_READ for x grew on some node (userstat on),
//!           else the EXPLAIN of some top fingerprint on t uses x
//!   used → last_used_at := now
//!   now − (last_used_at, else the build) > unused_drop_days   → retire(unused)
//! observing and active, every pass:
//!   a user or migrated index equals or extends x on every shard → retire(superseded)
//! retire(x, reason):
//!   instant IGNORED toggle: ALTER INDEX x IGNORED on each shard; retiring
//!     for ignore_before_drop_hours: targets regress → NOT IGNORED; active
//!   then dropping: DROP INDEX IF EXISTS x on each shard; dropped
//! failed: whatever a failed rollout left is dropped the same way
//! ```
//!
//! Workload numbers come through [`WorkloadSource`] (chapter 12's store,
//! not built yet): `None` means "unknown", and nothing is judged on it —
//! an index is never dropped as unused because the workload store is
//! missing.
//!
//! **Usage on 11.8.** `information_schema.INDEX_STATISTICS` exists but
//! stays empty unless `userstat = ON`, which is off by default and a server
//! variable this chapter never changes; so the usual path is `EXPLAIN`
//! sampling of the table's top fingerprints. Both are implemented; the
//! statistics are read when `@@userstat` is on.
//!
//! **Only automatic indexes** are ever touched: every statement goes
//! through [`node::auto_ddl`](super::rollout::node::auto_ddl), which refuses
//! any other name, and only rows of `auto_indexes` are acted on. User and
//! migrated indexes are only read, to find the ones that supersede ours.

pub mod drift;
#[cfg(test)]
mod tests;

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
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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
    /// `auto_index.usage_check_hours` (6).
    pub usage_every: Duration,
    /// Runs a fingerprint needs before its latency counts.
    pub min_runs: u64,
    /// Top fingerprints explained for usage.
    pub usage_samples: usize,
}

impl Default for ObserveSettings {
    fn default() -> Self {
        Self {
            watch_every: Duration::from_secs(24 * 3600),
            usage_every: Duration::from_secs(6 * 3600),
            min_runs: 5,
            usage_samples: 10,
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

/// `observed.usage`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageState {
    /// The last check, Unix ms.
    #[serde(default)]
    pub last_check_ms: u64,
    /// `INDEX_STATISTICS.ROWS_READ` summed over nodes and shards at the
    /// last check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_read: Option<u64>,
    /// How the last check decided: `index_statistics`, `explain`, `unknown`.
    #[serde(default)]
    pub method: String,
}

/// `observed.retire`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RetireState {
    /// Ignored since, Unix ms.
    #[serde(default)]
    pub since_ms: u64,
    /// Targets' runs-weighted p95 before it was ignored, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_before_ms: Option<f64>,
}

/// When the index started serving: the end of its rollout, else its last
/// state change.
fn built_at(row: &AutoIndexRow) -> SystemTime {
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
                    let p95 = lat.as_ref().and_then(|l| targets_p95(&targets, l));
                    return retire(store, q, cx, &fresh, def, reason, p95).await;
                }
            }
        }
    }
    // Usage.
    let mut us: UsageState = part(row.observed.as_ref(), "usage");
    if now_ms.saturating_sub(us.last_check_ms) >= ms(cx.settings.usage_every) {
        let used = match usage(cx, row, def, &mut us).await {
            Ok(u) => u,
            Err(e) => return Ok(ObserveStep::Failed(e)),
        };
        us.last_check_ms = now_ms;
        let observed = row.observed.clone();
        // Re-read: the watch may have just written `observed`.
        let current = store
            .index(q, row.index_id)
            .await?
            .map_or(observed, |r| r.observed);
        store
            .set_observed(q, row.index_id, &with_part(current.as_ref(), "usage", &us))
            .await?;
        checked = true;
        let mut last = row.last_used_at.unwrap_or_else(|| built_at(row));
        if used == Some(true) {
            store.touch_used(q, row.index_id, cx.now).await?;
            last = cx.now;
        }
        let unused_for = elapsed(last, cx.now);
        if used.is_some()
            && unused_for > Duration::from_secs(u64::from(cx.rules.unused_drop_days) * 86_400)
        {
            let mut fresh = row.clone();
            fresh.observed = store.index(q, row.index_id).await?.and_then(|r| r.observed);
            return retire(store, q, cx, &fresh, def, DropReason::Unused, None).await;
        }
    }
    Ok(if checked {
        ObserveStep::Checked
    } else {
        ObserveStep::Idle
    })
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

/// Whether the index was used since the last check: `Some(true)`,
/// `Some(false)`, or `None` when it can't be known.
async fn usage(
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    def: &IndexDef,
    us: &mut UsageState,
) -> std::result::Result<Option<bool>, String> {
    let name = def.name();
    let Some(writer) = cx.view.nodes.writer() else {
        return Err("no writer".into());
    };
    let mut w = Node::connect(writer, PROBE_TIMEOUT)
        .await
        .map_err(|e| e.to_string())?;
    let userstat = w
        .scalar("SELECT @@userstat")
        .await
        .ok()
        .flatten()
        .as_deref()
        == Some("1");
    w.close().await;
    if userstat {
        let mut total = 0u64;
        for ep in cx.view.nodes.all() {
            let mut n = Node::connect(ep, PROBE_TIMEOUT)
                .await
                .map_err(|e| e.to_string())?;
            for t in cx.shards {
                let v = n
                    .scalar(&format!(
                        "SELECT IFNULL(SUM(ROWS_READ), 0) FROM information_schema.INDEX_STATISTICS \
                         WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND INDEX_NAME = {}",
                        literal(&t.schema),
                        literal(&t.table),
                        literal(&name)
                    ))
                    .await
                    .map_err(|e| e.to_string())?;
                total += v.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            }
            n.close().await;
        }
        // Counters reset on restart: any change counts as use.
        let used = us.rows_read.is_some_and(|before| total != before)
            || (us.rows_read.is_none() && total > 0);
        us.rows_read = Some(total);
        us.method = "index_statistics".into();
        return Ok(Some(used));
    }
    let since = if us.last_check_ms > 0 {
        std::time::UNIX_EPOCH + Duration::from_millis(us.last_check_ms)
    } else {
        built_at(row)
    };
    let Some(samples) = cx
        .workload
        .samples(cx.tenant, row.object, cx.settings.usage_samples, since)
        .await
        .map_err(|e| e.to_string())?
    else {
        us.method = "unknown".into();
        return Ok(None);
    };
    us.method = "explain".into();
    let mut c = mysql_async::Conn::new(writer.opts.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut used = false;
    for s in &samples {
        if c.query_drop(format!("USE {}", quote(&s.schema)))
            .await
            .is_err()
        {
            continue;
        }
        match super::candidates::explain::explain_uses(&mut c, &s.sql, &name).await {
            Ok(o) if o.uses => {
                used = true;
                break;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::info!(error = %e, fingerprint = %s.fingerprint, "usage EXPLAIN failed");
            }
        }
    }
    let _ = c.disconnect().await;
    tracing::info!(index = %name, samples = samples.len(), used, "auto_index usage by EXPLAIN");
    Ok(Some(used))
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

/// Retires `row`: through `IGNORED` when the cluster toggles it instantly
/// and `ignore_before_drop_hours > 0`, else dropped at once.
async fn retire<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    def: &IndexDef,
    reason: DropReason,
    p95_before: Option<f64>,
) -> Result<ObserveStep> {
    let name = def.name();
    if cx.view.instant_ignored_toggle && cx.rules.ignore_before_drop_hours > 0 {
        // The targets' latency while it served: what "regressed while
        // ignored" compares with.
        let p95_before = if let Some(p) = p95_before {
            Some(p)
        } else {
            let from = cx
                .now
                .checked_sub(cx.settings.watch_every)
                .unwrap_or(cx.now);
            let targets = targets(store, q, row).await?;
            cx.workload
                .latency(cx.tenant, row.object, from, cx.now)
                .await?
                .and_then(|l| targets_p95(&targets, &l))
        };
        for t in cx.shards {
            if let Err(e) = run_on_writer(cx, t, &ignore_sql(t, &name, true)?).await {
                return Ok(ObserveStep::Failed(e));
            }
        }
        let rs = RetireState {
            since_ms: unix_ms(cx.now),
            p95_before_ms: p95_before,
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
                    "{} ignored ({}); dropped after {} h unless the workload regresses",
                    row.name,
                    reason.as_str(),
                    cx.rules.ignore_before_drop_hours
                ),
                serde_json::json!({"observed": row.observed}),
            )
            .await;
        }
        return Ok(ObserveStep::Retiring(reason));
    }
    let ok = store
        .set_state(
            q,
            row.index_id,
            &change(row.state, IndexState::Dropping, cx.now, Some(reason)),
        )
        .await?;
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

async fn retiring<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    row: &AutoIndexRow,
    def: &IndexDef,
) -> Result<ObserveStep> {
    let rs: RetireState = part(row.observed.as_ref(), "retire");
    let since = std::time::UNIX_EPOCH + Duration::from_millis(rs.since_ms);
    // Regressed while ignored: bring it back.
    if let Some(before) = rs.p95_before_ms.filter(|b| *b > 0.0) {
        let lat = cx
            .workload
            .latency(cx.tenant, row.object, since, cx.now)
            .await?;
        let targets = targets(store, q, row).await?;
        if let Some(l) = lat
            && let Some(now) = targets_p95(&targets, &l)
            && 100.0 * (now / before - 1.0) > cx.rules.max_regression_pct
        {
            for t in cx.shards {
                if let Err(e) = run_on_writer(cx, t, &ignore_sql(t, &def.name(), false)?).await {
                    return Ok(ObserveStep::Failed(e));
                }
            }
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
                        "{}: targets {now:.1} ms against {before:.1} ms while ignored; kept",
                        row.name
                    ),
                    serde_json::json!({}),
                )
                .await;
            }
            return Ok(ObserveStep::Restored);
        }
    }
    let wait = Duration::from_secs(u64::from(cx.rules.ignore_before_drop_hours) * 3600);
    if elapsed(since, cx.now) < wait {
        return Ok(ObserveStep::Idle);
    }
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
pub(crate) async fn run_ddl(
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
