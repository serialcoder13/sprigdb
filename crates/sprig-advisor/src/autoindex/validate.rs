//! Validation of a candidate index (chapter 13, build steps 7, 8 and 9;
//! "Validation harness").
//!
//! Three validators, chosen by the cluster's `derived.validation`
//! (chapter 09) through [`method_for`]:
//!
//! - [`replica::ReplicaValidator`] (`replica`): builds the candidate on one
//!   replica with `sql_log_bin = 0`, replays there, drops it;
//! - [`galera::RsuValidator`] (`galera_rsu`): builds it on one Galera node
//!   with `wsrep_OSU_method = RSU`, waits for the node to resync, replays,
//!   drops it the same way;
//! - [`canary::CanaryValidator`] (`estimate_canary`): single nodes and
//!   external clusters with neither: `EXPLAIN`-checked estimates, then a
//!   real build on the canary shard, observed for `auto_index.canary_hours`.
//!
//! Each returns a [`ValidationReport`]: the verdict with its evidence (the
//! `auto_index_validations` row), timings, and what was checked to prove
//! the node is as it was. Infrastructure failures (lost node, lag, a
//! degraded cluster, user DDL) are verdicts too, `aborted`, after the
//! validator has cleaned up; `Err` is kept for plans that make no sense.
//!
//! **Taking the node out of service** goes through [`NodeMarker`], which
//! the controller implements against chapter 06's read routing (and, on
//! Galera, the writer weight). The validators call it; they don't route.
//!
//! **One at a time.** A validator never runs two validations on one node
//! at once, and the RSU validator refuses to start while any other node of
//! the cluster is not `Synced`. Scheduling one validation per cluster is
//! the worker's job (the `auto_index_validate` job holds the cluster's
//! build slot, `auto_index.max_builds_per_cluster`).

pub mod canary;
pub mod galera;
pub(crate) mod node;
pub mod replica;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_cluster::profile::ValidationMethod;
use sprig_cluster::sql::{NodeEndpoint, ProbeSession};
use sprig_common::ids::{Fingerprint, NodeId};
use sprig_common::{Result, SprigError};

use super::model::{Candidate, KeyPart, PartSource};
use super::replay::{
    self, ReplayGuard, ReplaySettings, ReplayStatement, ReplayTarget, Verdict, VerdictKind,
};

/// `auto_index_validations.method`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// Build on a replica with the binary log off.
    Replica,
    /// Build on a Galera node in rolling schema upgrade mode.
    GaleraRsu,
    /// Estimates, then a canary build.
    EstimateCanary,
}

impl Method {
    /// The column text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Method::Replica => "replica",
            Method::GaleraRsu => "galera_rsu",
            Method::EstimateCanary => "estimate_canary",
        }
    }
}

/// The method for a cluster's `derived.validation`, falling back to
/// estimates and a canary when it has none (single node, external without
/// a usable replica, missing privileges) or when parameter samples are off.
#[must_use]
pub fn method_for(validation: Option<ValidationMethod>, samples_available: bool) -> Method {
    match (validation, samples_available) {
        (Some(ValidationMethod::ReplicaBuild), true) => Method::Replica,
        (Some(ValidationMethod::GaleraRsu), true) => Method::GaleraRsu,
        _ => Method::EstimateCanary,
    }
}

/// The candidate's table, in physical names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexTarget {
    /// The shard schema (`t42_d1_g3_s17`).
    pub schema: String,
    /// The table.
    pub table: String,
    /// Its primary-key columns.
    pub pk: Vec<String>,
}

/// The cost model's prediction for one target fingerprint (chapter 13,
/// "Cost model"), which estimate-and-canary validation checks.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Prediction {
    /// The fingerprint.
    pub fingerprint: Fingerprint,
    /// `t_f`, its mean time now, ms.
    pub t_before_ms: f64,
    /// `t̂_f`, its predicted time with the index, ms.
    pub t_after_ms: f64,
    /// `x_f`, rows it reads per run now, as the model assumed.
    pub rows_assumed: f64,
}

impl Prediction {
    /// `100 × (1 − t̂ / t)`.
    #[must_use]
    pub fn improvement_pct(&self) -> f64 {
        if self.t_before_ms > 0.0 {
            100.0 * (1.0 - self.t_after_ms / self.t_before_ms)
        } else {
            0.0
        }
    }
}

/// Settings of one validation (resolved `auto_index.*` keys).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ValidationSettings {
    /// Replay and verdict.
    pub replay: ReplaySettings,
    /// `auto_index.ddl_lock_wait_seconds` (5).
    pub ddl_lock_wait: Duration,
    /// `auto_index.abort_replica_lag_seconds` (300).
    pub abort_replica_lag: Duration,
    /// `auto_index.max_build_hours` (6): the longest a build may run.
    pub max_build: Duration,
    /// Run `ANALYZE LOCAL ... PERSISTENT` for the new index after the build
    /// (chapter 13, replica build step 2). The statistics rows are put back
    /// as they were afterwards.
    pub analyze_after_build: bool,
    /// Galera: the least time to wait for `Synced` after an RSU statement;
    /// the wait is `max(this, 2 × the statement's time)`.
    pub resync_floor: Duration,
    /// Galera: the fewest nodes the cluster must keep.
    pub min_cluster_size: u32,
    /// How often the node is checked during a build.
    pub poll: Duration,
    /// `auto_index.estimate_margin` (1.5).
    pub estimate_margin: f64,
    /// `auto_index.canary_hours` (24).
    pub canary_hours: u32,
    /// The `EXPLAIN` row estimate must be at least this share of the rows
    /// the cost model assumed.
    pub explain_rows_tolerance: f64,
    /// Estimate-and-canary: replay A/B on the canary right after its build.
    pub canary_replay: bool,
    /// `auto_index.max_cover_columns` (3), for the covering retry.
    pub max_cover_columns: usize,
}

impl Default for ValidationSettings {
    fn default() -> Self {
        Self {
            replay: ReplaySettings::default(),
            ddl_lock_wait: Duration::from_secs(5),
            abort_replica_lag: Duration::from_secs(300),
            max_build: Duration::from_secs(6 * 3600),
            analyze_after_build: true,
            resync_floor: Duration::from_secs(30),
            min_cluster_size: 3,
            poll: Duration::from_millis(500),
            estimate_margin: 1.5,
            canary_hours: 24,
            explain_rows_tolerance: 0.5,
            canary_replay: true,
            max_cover_columns: 3,
        }
    }
}

/// Everything one validation needs.
#[derive(Debug, Clone)]
pub struct ValidationPlan {
    /// `auto_index_candidates.candidate_id`.
    pub candidate_id: u64,
    /// The candidate: its definition names the index.
    pub candidate: Candidate,
    /// Its table on the node, in physical names.
    pub target: IndexTarget,
    /// Sampled statements: the candidate's target fingerprints and the
    /// regression fingerprints on the table (ordered by total time).
    pub statements: Vec<ReplayStatement>,
    /// The cost model's predictions for the targets (estimate and canary).
    pub predictions: Vec<Prediction>,
    /// Columns each fingerprint reads on the table, for the covering retry
    /// (from the fingerprint's shape).
    pub columns_read: BTreeMap<Fingerprint, Vec<String>>,
    /// Keep the index on the node when accepted (chapter 13 step 4: when
    /// the rollout starts within 6 hours). Off: always drop.
    pub keep_if_accepted: bool,
    /// Settings.
    pub settings: ValidationSettings,
}

impl ValidationPlan {
    /// The index name, `sprig_auto_<8 hex>`.
    #[must_use]
    pub fn index_name(&self) -> String {
        self.candidate.def.name()
    }

    pub(crate) fn replay_target(&self) -> ReplayTarget {
        ReplayTarget {
            schema: self.target.schema.clone(),
            table: self.target.table.clone(),
            index: self.index_name(),
            pk: self.target.pk.clone(),
        }
    }

    fn check(&self) -> Result<()> {
        if self.candidate.def.parts.is_empty() {
            return Err(SprigError::internal("a candidate index without key parts"));
        }
        if self.target.schema.is_empty() || self.target.table.is_empty() {
            return Err(SprigError::internal("a validation plan without a table"));
        }
        Ok(())
    }
}

/// What the validator checked to show the node is as it was.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeCheck {
    /// Every check passed.
    pub restored: bool,
    /// What was checked, and anything that wasn't right.
    pub notes: Vec<String>,
}

impl NodeCheck {
    fn ok(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }
    fn bad(&mut self, note: impl Into<String>) {
        self.restored = false;
        let note = note.into();
        tracing::error!(note = %note, "validation left the node changed");
        self.notes.push(format!("NOT RESTORED: {note}"));
    }
}

/// The canary of estimate-and-canary validation, still observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanaryState {
    /// Observe until (Unix ms); [`canary::judge`] decides afterwards.
    pub observe_until_ms: u64,
}

/// The result of one validation: the `auto_index_validations` row and more.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationReport {
    /// `candidate_id`.
    pub candidate_id: u64,
    /// `method`.
    pub method: Method,
    /// `node`.
    pub node: String,
    /// The index name.
    pub index: String,
    /// The canonical definition.
    pub definition: String,
    /// `started_at`, Unix ms.
    pub started_at_ms: u64,
    /// `finished_at`, Unix ms.
    pub finished_at_ms: u64,
    /// The build, ms (`build_seconds` rounds up).
    pub build_ms: Option<u64>,
    /// `ANALYZE` after the build, ms.
    pub analyze_ms: Option<u64>,
    /// Galera: waiting for `Synced` after the build, ms.
    pub resync_ms: Option<u64>,
    /// The replay, ms.
    pub replay_ms: Option<u64>,
    /// The drop, ms (with its resync on Galera).
    pub drop_ms: Option<u64>,
    /// The verdict and its evidence (`statements`, `noisy`,
    /// `target_p95_*`, `worst_*`, `verdict`, `reason`, `details`).
    pub verdict: Verdict,
    /// The index was left in place (accepted with `keep_if_accepted`, or
    /// the canary).
    pub index_kept: bool,
    /// What was checked after cleaning up.
    pub node_check: NodeCheck,
    /// Estimate and canary: the canary under observation.
    pub canary: Option<CanaryState>,
}

impl ValidationReport {
    /// `auto_index_validations.build_seconds`.
    #[must_use]
    pub fn build_seconds(&self) -> Option<u64> {
        self.build_ms.map(|ms| ms.div_ceil(1000))
    }

    /// `auto_index_validations.details`: per statement, sample ids and
    /// timings, never values.
    #[must_use]
    pub fn details_json(&self) -> serde_json::Value {
        serde_json::json!({
            "statements": self.verdict.details,
            "fingerprints": self.verdict.fingerprints,
            "timings_ms": {
                "build": self.build_ms, "analyze": self.analyze_ms, "resync": self.resync_ms,
                "replay": self.replay_ms, "drop": self.drop_ms,
            },
            "index_kept": self.index_kept,
            "node_check": self.node_check,
            "covering_hint": self.verdict.covering_hint,
        })
    }

    /// The whole validation, wall clock, ms.
    #[must_use]
    pub fn elapsed_ms(&self) -> u64 {
        self.finished_at_ms.saturating_sub(self.started_at_ms)
    }
}

/// A validator (chapter 13, "Interfaces"). The chapter's signature takes
/// `(&Candidate, &ValidationPlan)`; here the plan carries the candidate.
#[async_trait::async_trait]
pub trait Validator: Send + Sync {
    /// Validates one candidate.
    ///
    /// # Errors
    /// Only for a plan that makes no sense; everything that goes wrong on
    /// the node is an `aborted` verdict.
    async fn validate(&self, plan: &ValidationPlan) -> Result<ValidationReport>;
}

/// Takes a node out of service while it validates (chapter 06 stops
/// routing reads there; on Galera the writer weight goes to 0).
#[async_trait::async_trait]
pub trait NodeMarker: Send + Sync {
    /// Marks `node` validating.
    ///
    /// # Errors
    /// The mark couldn't be published; the validation doesn't start.
    async fn mark_validating(&self, node: &NodeId) -> Result<()>;
    /// Unmarks it.
    ///
    /// # Errors
    /// The mark couldn't be removed.
    async fn unmark(&self, node: &NodeId) -> Result<()>;
}

/// A marker that does nothing (tests, tools).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMarker;

#[async_trait::async_trait]
impl NodeMarker for NoMarker {
    async fn mark_validating(&self, _node: &NodeId) -> Result<()> {
        Ok(())
    }
    async fn unmark(&self, _node: &NodeId) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

pub(crate) fn ms_since(t: std::time::Instant) -> u64 {
    u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// A report shell, filled in by the validator.
pub(crate) fn report(plan: &ValidationPlan, method: Method, node: &NodeId) -> ValidationReport {
    ValidationReport {
        candidate_id: plan.candidate_id,
        method,
        node: node.to_string(),
        index: plan.index_name(),
        definition: plan.candidate.def.canonical(),
        started_at_ms: now_ms(),
        finished_at_ms: 0,
        build_ms: None,
        analyze_ms: None,
        resync_ms: None,
        replay_ms: None,
        drop_ms: None,
        verdict: replay::decide(&[], &plan.settings.replay.thresholds),
        index_kept: false,
        node_check: NodeCheck {
            restored: true,
            notes: Vec::new(),
        },
        canary: None,
    }
}

/// Sets the verdict to aborted, keeping whatever evidence there is.
pub(crate) fn abort(r: &mut ValidationReport, reason: impl Into<String>) {
    let reason: String = reason.into();
    tracing::warn!(candidate = r.candidate_id, node = %r.node, reason = %reason, "validation aborted");
    r.verdict.kind = VerdictKind::Aborted;
    r.verdict.reason = reason.chars().take(128).collect();
}

/// Opens a replay session on `ep`, replays the plan's statements under
/// `guard`, and decides. The verdict is `aborted` if the guard or the
/// connection stopped the replay.
pub(crate) async fn replay_on(
    ep: &NodeEndpoint,
    plan: &ValidationPlan,
    guard: &mut dyn ReplayGuard,
    r: &mut ValidationReport,
) {
    let s = &plan.settings.replay;
    let started = std::time::Instant::now();
    // The round trip allows the statement limit plus a margin.
    let mut conn = match ProbeSession::connect(ep, s.timeout + Duration::from_secs(30)).await {
        Ok(c) => c,
        Err(e) => {
            abort(r, format!("replay connection: {e}"));
            return;
        }
    };
    if let Err(e) = replay::prepare_session(&mut conn, s).await {
        abort(r, format!("replay session: {e}"));
        conn.close().await;
        return;
    }
    let statements = replay::select_statements(&plan.statements, s);
    let result = replay::replay(&mut conn, guard, &plan.replay_target(), &statements, s).await;
    conn.close().await;
    r.replay_ms = Some(ms_since(started));
    r.verdict = replay::decide(&result.statements, &s.thresholds);
    if let Some(why) = result.aborted {
        abort(r, why);
    }
}

/// The covering variant of a candidate for the statement in `hint`: its
/// parts plus the columns that fingerprint reads, without the primary key
/// and columns already in the key, at most `max_cover_columns` of them.
/// `None` when nothing would be added or too much would.
#[must_use]
pub fn covering_variant(plan: &ValidationPlan, fingerprint: Fingerprint) -> Option<Candidate> {
    let read = plan.columns_read.get(&fingerprint)?;
    let mut c = plan.candidate.clone();
    let mut added = 0;
    for col in read {
        let col = col.to_ascii_lowercase();
        if c.def.parts.iter().any(|p| p.column == col)
            || plan.target.pk.iter().any(|k| k.eq_ignore_ascii_case(&col))
        {
            continue;
        }
        c.def.parts.push(KeyPart::col(&col));
        c.evidence.parts.push(PartSource::Cover);
        added += 1;
    }
    if added == 0 || added > plan.settings.max_cover_columns {
        return None;
    }
    c.covering = true;
    c.evidence
        .notes
        .push(format!("covering retry for {fingerprint}"));
    Some(c)
}

/// Validates, and on a rejection whose regressing statement read the index
/// with many primary-key lookups, validates once more with the covering
/// variant (chapter 13, "Covering retry"; worked example 3). Returns one
/// report, or two when the retry ran (the retry last).
///
/// # Errors
/// As [`Validator::validate`].
pub async fn validate_with_covering_retry(
    v: &dyn Validator,
    plan: &ValidationPlan,
) -> Result<Vec<ValidationReport>> {
    let first = v.validate(plan).await?;
    let retry = (first.verdict.kind == VerdictKind::Rejected)
        .then_some(first.verdict.covering_hint.as_ref())
        .flatten()
        .and_then(|h| covering_variant(plan, h.fingerprint));
    let Some(candidate) = retry else {
        return Ok(vec![first]);
    };
    let mut p2 = plan.clone();
    p2.candidate = candidate;
    let second = v.validate(&p2).await?;
    Ok(vec![first, second])
}

#[cfg(test)]
mod tests;
