//! The `auto_index` loop (chapter 13 "Flows", chapter 23's DC-leader loop
//! table): evaluate → validate → roll out → observe, per tenant, paced.
//!
//! ```text
//! auto_index (DC leader), key = tenant homed here and active:
//!   pass(tenant):
//!     1. every eval_every: CandidateSource::evaluate (steps 2-5), which writes
//!        its own `validating` rows; the retirements it returns are applied
//!     2. each `validating` row: ValidationHook::validate                (steps 7-9 hook)
//!        accepted → `rolling_out` (mode on, a build method exists)
//!                   or `recommended` (mode recommend, advice, no method)
//!        rejected → `rejected`
//!     3. each `rolling_out` row: rollout::driver::advance  — one shard build at
//!        a time per tenant, one per cluster slot across the DC
//!     4. each serving, retiring, dropping or failed row: observe::step
//!     5. every drift_every, each object with live rows: observe::drift
//!   requeue after the soonest of: a rollout's next try, the next observation,
//!   the next evaluation (bounded to [min_requeue, max_requeue])
//! ```
//!
//! Objects whose `auto_index.mode` is `off` are skipped entirely: nothing
//! is built, observed or dropped for them (chapter 13: existing indexes
//! are kept as they are).
//!
//! **Builds run in the controller in M0.4.** Chapter 13 makes them worker
//! jobs (`auto_index_build`), but the worker role has no job runtime yet.
//! The loop runs them itself, on the DC leader only, and keeps chapter 23's
//! slot rule: a build starts only while fewer than
//! `auto_index.max_builds_per_cluster` builds hold the cluster, counting
//! this process's builds, `sprig_auto_*` builds running on the writer (a
//! deposed leader's, say) and the scheduler's `build`-class jobs. A deposed
//! leader's reconcile is dropped, and its build killed with it
//! ([`KillGuard`](sprig_advisor::autoindex::rollout::node::KillGuard)).
//!
//! **Hooks.** [`CandidateSource`] is trigger evaluation (steps 2-5,
//! `autoindex_env::EvalShim`); [`ValidationHook`] runs a validator for a
//! `validating` row (`autoindex_validate::SampleValidation`);
//! [`WorkloadSource`] is chapter 12's store as the observer reads it
//! (`autoindex_workload::ObserveSource`); [`ShardLocks`] and [`CopySwapHook`] are chapter 10's.
//! [`JobShardLocks`] implements the shard lock from the job list: no build
//! while a copy job runs for the tenant (chapter 23, rule 3b).

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use mysql_async::Conn;
use sprig_advisor::autoindex::model::AutoIndexId;
use sprig_advisor::autoindex::observe::{
    self, ObserveCx, ObserveSettings, ObserveStep, WorkloadSource, drift,
};
use sprig_advisor::autoindex::policy::{Mode, ObserveRules};
use sprig_advisor::autoindex::rollout::driver::{RolloutCx, Step, advance};
use sprig_advisor::autoindex::rollout::node::Node;
use sprig_advisor::autoindex::rollout::{
    BuildMethod, ClusterView, CopySwapHook, LocalSlots, RolloutSettings, ShardLock, ShardLocks,
    ShardTable, choose_method,
};
use sprig_advisor::autoindex::store::{
    AUTOINDEX_MODULE, AutoIndexRow, AutoIndexStore, DecisionLog, DropReason, IndexDecision,
    IndexState, ShardBuild, StateChange,
};
use sprig_advisor::autoindex::validate::{NodeMarker, ValidationSettings};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId, ObjectId, TenantId};
use sprig_common::time::Clock;
use sprig_meta::MetaStore;

use crate::dispatch::{JobClass, list_jobs};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};
use crate::tenants::BootstrapHooks;
use crate::tree::{Directory, DirectoryEntry, TenantState};
use sprig_catalog::{CatalogEndpoint, Module};

/// What an evaluation asks the loop to do besides the `validating` rows
/// it wrote itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evaluated {
    /// Automatic indexes selection wants retired now (a table over a
    /// tightened budget, a replaced index), as `(object, name, reason)`.
    pub retire: Vec<(ObjectId, String, DropReason)>,
}

/// Trigger evaluation and selection (chapter 13 steps 2-5,
/// `sprig_advisor::autoindex::evaluate`). It writes its own rows: routed
/// candidates as `validating` (recommend-routed ones carry
/// `observed = {"route": "recommend"}`), with its re-proposal rules (a
/// dropped or failed row of the same name is reused; a rejected one blocks
/// for `recreate_block_days`). The loop only picks up `validating` rows and
/// drives them on, and applies the retirements returned here.
#[async_trait::async_trait]
pub trait CandidateSource: Send + Sync {
    /// Evaluates `tenant` at `now`.
    ///
    /// # Errors
    /// The evaluation failed; the pass carries on with the other steps.
    async fn evaluate(&self, tenant: TenantId, now: SystemTime) -> Result<Evaluated>;
}

/// No evaluation yet.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoCandidates;

#[async_trait::async_trait]
impl CandidateSource for NoCandidates {
    async fn evaluate(&self, _tenant: TenantId, _now: SystemTime) -> Result<Evaluated> {
        Ok(Evaluated::default())
    }
}

/// The verdict of a validation the loop acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validated {
    /// Accepted; `validation_id` names the `auto_index_validations` row.
    Accepted {
        /// The validation row.
        validation_id: Option<u64>,
    },
    /// Rejected.
    Rejected {
        /// Why.
        reason: String,
    },
    /// Not decided yet (running, inconclusive, no node now): ask again.
    Pending,
}

/// Validation of a `validating` row (chapter 13 steps 7-9): builds the
/// validation plan (samples from chapter 12) and runs the cluster's
/// validator.
#[async_trait::async_trait]
pub trait ValidationHook: Send + Sync {
    /// Validates `row` on `object`'s cluster.
    ///
    /// # Errors
    /// The validation couldn't run; asked again next pass.
    async fn validate(
        &self,
        tenant: TenantId,
        row: &AutoIndexRow,
        object: &ObjectEnv,
    ) -> Result<Validated>;
}

/// No validation yet: rows wait in `validating`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoValidation;

#[async_trait::async_trait]
impl ValidationHook for NoValidation {
    async fn validate(
        &self,
        _tenant: TenantId,
        _row: &AutoIndexRow,
        _object: &ObjectEnv,
    ) -> Result<Validated> {
        Ok(Validated::Pending)
    }
}

/// One object as the loop needs it: its cluster, shards and resolved
/// settings.
#[derive(Debug, Clone)]
pub struct ObjectEnv {
    /// The object.
    pub object: ObjectId,
    /// Its cluster.
    pub view: ClusterView,
    /// Its shards.
    pub shards: Vec<ShardTable>,
    /// `auto_index.mode`, resolved at the object.
    pub mode: Mode,
    /// Observe and drop rules, resolved at the object.
    pub rules: ObserveRules,
    /// Rollout settings: the object's keys, the cluster's shared limits.
    pub rollout: RolloutSettings,
    /// Observation cadence.
    pub observe: ObserveSettings,
    /// Validation settings, resolved at the object (the replay keys, the
    /// thresholds, lock waits and the estimate margin).
    pub validation: ValidationSettings,
}

/// Where the loop finds catalogs, objects and clusters.
#[async_trait::async_trait]
pub trait AutoIndexEnv: Send + Sync {
    /// A connection to `tenant`'s catalog and its automatic-indexing tables.
    ///
    /// # Errors
    /// The catalog is unreachable.
    async fn catalog(&self, tenant: TenantId) -> Result<(Conn, AutoIndexStore)>;

    /// The objects of `tenant` named in `objects`, resolved; missing ones
    /// (dropped tables) are left out.
    ///
    /// # Errors
    /// The catalog or the cluster registry couldn't be read.
    async fn objects(
        &self,
        tenant: TenantId,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<BTreeMap<ObjectId, ObjectEnv>>;

    /// Build jobs of the scheduler holding `cluster`'s build slots.
    ///
    /// # Errors
    /// The job list couldn't be read.
    async fn external_builds(&self, cluster: &ClusterId) -> Result<u32>;
}

/// The loop's hooks.
#[derive(Clone)]
pub struct AutoIndexHooks {
    /// Step 2.
    pub candidates: Arc<dyn CandidateSource>,
    /// Steps 7-9.
    pub validation: Arc<dyn ValidationHook>,
    /// Chapter 12's store, for observation.
    pub workload: Arc<dyn WorkloadSource>,
    /// Chapter 10's shard locks.
    pub locks: Arc<dyn ShardLocks>,
    /// Chapter 10's copy-and-swap (M0.6).
    pub mover: Option<Arc<dyn CopySwapHook>>,
    /// Takes Galera nodes out of service during rolling builds.
    pub marker: Arc<dyn NodeMarker>,
    /// Decisions.
    pub decisions: Arc<dyn DecisionLog>,
}

impl fmt::Debug for AutoIndexHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutoIndexHooks")
            .field("mover", &self.mover.is_some())
            .finish_non_exhaustive()
    }
}

/// How often each part of a pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cadence {
    /// Trigger evaluation (`auto_index.eval_interval_minutes`, 60).
    pub eval_every: Duration,
    /// The drift reconciler.
    pub drift_every: Duration,
    /// The shortest requeue.
    pub min_requeue: Duration,
    /// The longest requeue.
    pub max_requeue: Duration,
}

impl Default for Cadence {
    fn default() -> Self {
        Self {
            eval_every: Duration::from_secs(3600),
            drift_every: Duration::from_secs(3600),
            min_requeue: Duration::from_secs(5),
            max_requeue: Duration::from_secs(600),
        }
    }
}

/// What a tenant's last passes did, in memory (lost on a leader change:
/// the next pass evaluates and reconciles at once).
#[derive(Debug, Clone, Default)]
pub struct Memory {
    /// The last evaluation.
    pub last_eval: Option<SystemTime>,
    /// The last drift reconcile.
    pub last_drift: Option<SystemTime>,
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PassReport {
    /// Whether trigger evaluation ran.
    pub evaluated: bool,
    /// Retirements evaluation asked for, applied, by row.
    pub retired: Vec<(AutoIndexId, ObserveStep)>,
    /// Validation outcomes, by row.
    pub validated: Vec<(AutoIndexId, IndexState)>,
    /// Rollout steps, by row.
    pub rollout: Vec<(AutoIndexId, Step)>,
    /// Observation steps, by row.
    pub observed: Vec<(AutoIndexId, ObserveStep)>,
    /// Drift reconciles, by object.
    pub drift: Vec<(ObjectId, drift::DriftReport)>,
    /// Errors that didn't stop the pass.
    pub errors: Vec<String>,
    /// When to come back.
    pub next_in: Duration,
}

const SERVING: [IndexState; 5] = [
    IndexState::Observing,
    IndexState::Active,
    IndexState::Retiring,
    IndexState::Dropping,
    IndexState::Failed,
];

/// One pass over one tenant.
///
/// # Errors
/// The catalog is unreachable; every other failure is in
/// [`PassReport::errors`].
#[allow(clippy::too_many_lines)] // the five steps of chapter 13's flow
pub async fn tenant_pass(
    env: &dyn AutoIndexEnv,
    hooks: &AutoIndexHooks,
    slots: &LocalSlots,
    tenant: TenantId,
    now: SystemTime,
    memory: &mut Memory,
    cadence: &Cadence,
) -> Result<PassReport> {
    let mut rep = PassReport {
        next_in: cadence.max_requeue,
        ..PassReport::default()
    };
    let (mut q, store) = env.catalog(tenant).await?;
    let soon = |rep: &mut PassReport, d: Duration| {
        rep.next_in = rep.next_in.min(d.max(cadence.min_requeue));
    };

    // 1. Evaluation: it writes its own `validating` rows.
    let eval_due = memory
        .last_eval
        // A clock that went back (a test clock reset) makes it due too.
        .is_none_or(|t| now < t || now.duration_since(t).unwrap_or_default() >= cadence.eval_every);
    let mut retire = Vec::new();
    if eval_due {
        memory.last_eval = Some(now);
        rep.evaluated = true;
        match hooks.candidates.evaluate(tenant, now).await {
            Ok(e) => retire = e.retire,
            Err(e) => rep.errors.push(format!("evaluate: {e}")),
        }
    }
    let next_eval = memory.last_eval.map_or(Duration::ZERO, |t| {
        cadence
            .eval_every
            .saturating_sub(now.duration_since(t).unwrap_or_default())
    });
    soon(&mut rep, next_eval);

    // Rows to work on, and their objects.
    let mut states = vec![IndexState::Validating, IndexState::RollingOut];
    states.extend(SERVING);
    let rows = store.indexes_in(&mut q, &states).await?;
    let objects: BTreeSet<ObjectId> = rows
        .iter()
        .map(|r| r.object)
        .chain(retire.iter().map(|(o, _, _)| *o))
        .collect();
    let envs = match env.objects(tenant, &objects).await {
        Ok(e) => e,
        Err(e) => {
            rep.errors.push(format!("objects: {e}"));
            let _ = q.disconnect().await;
            return Ok(rep);
        }
    };

    // 2. Validation.
    for r in rows.iter().filter(|r| r.state == IndexState::Validating) {
        let Some(o) = envs.get(&r.object) else {
            continue;
        };
        if o.mode == Mode::Off {
            continue;
        }
        match hooks.validation.validate(tenant, r, o).await {
            Ok(Validated::Pending) => soon(&mut rep, o.rollout.retry_after),
            Ok(v) => match route(&store, &mut q, hooks, tenant, r, o, &v, now).await {
                Ok(s) => {
                    rep.validated.push((r.index_id, s));
                    soon(&mut rep, Duration::ZERO);
                }
                Err(e) => rep.errors.push(format!("{}: route: {e}", r.name)),
            },
            Err(e) => rep.errors.push(format!("{}: validate: {e}", r.name)),
        }
    }

    // 3. Rollout, re-reading the rows validation just moved.
    let rolling = store.indexes_in(&mut q, &[IndexState::RollingOut]).await?;
    for r in &rolling {
        let Some(o) = envs.get(&r.object) else {
            continue;
        };
        if o.mode == Mode::Off {
            continue;
        }
        let bytes = table_bytes(o).await;
        let external = env.external_builds(&o.view.cluster).await.unwrap_or(0);
        let cx = RolloutCx {
            tenant,
            view: &o.view,
            shards: &o.shards,
            table_bytes: bytes,
            settings: &o.rollout,
            auto_window: None,
            slots,
            external_builds: external,
            locks: &*hooks.locks,
            mover: hooks.mover.as_deref(),
            marker: &*hooks.marker,
            decisions: &*hooks.decisions,
            now,
        };
        match advance(&store, &mut q, &cx, r).await {
            Ok(step) => {
                match &step {
                    Step::Wait { retry_after, .. } | Step::Retry { retry_after, .. } => {
                        soon(&mut rep, *retry_after);
                    }
                    Step::ShardBuilt { next_after, .. } => soon(&mut rep, *next_after),
                    Step::Observing { .. } => soon(&mut rep, Duration::ZERO),
                    Step::NotRollingOut | Step::Failed { .. } => {}
                }
                rep.rollout.push((r.index_id, step));
            }
            Err(e) => rep.errors.push(format!("{}: rollout: {e}", r.name)),
        }
    }

    // 4. Retirements selection asked for, then observation.
    for (object, name, reason) in &retire {
        let Some(o) = envs.get(object) else { continue };
        if o.mode == Mode::Off {
            continue;
        }
        let Some(r) = store.index_by_name(&mut q, *object, name).await? else {
            continue;
        };
        let cx = ObserveCx {
            tenant,
            view: &o.view,
            shards: &o.shards,
            rules: &o.rules,
            settings: &o.observe,
            ddl: &o.rollout,
            workload: &*hooks.workload,
            decisions: &*hooks.decisions,
            now,
        };
        match observe::retire_now(&store, &mut q, &cx, &r, *reason).await {
            Ok(st) => rep.retired.push((r.index_id, st)),
            Err(e) => rep.errors.push(format!("{name}: retire: {e}")),
        }
    }
    let serving = store.indexes_in(&mut q, &SERVING).await?;
    for r in &serving {
        let Some(o) = envs.get(&r.object) else {
            continue;
        };
        if o.mode == Mode::Off {
            continue;
        }
        let cx = ObserveCx {
            tenant,
            view: &o.view,
            shards: &o.shards,
            rules: &o.rules,
            settings: &o.observe,
            ddl: &o.rollout,
            workload: &*hooks.workload,
            decisions: &*hooks.decisions,
            now,
        };
        match observe::step(&store, &mut q, &cx, r).await {
            Ok(s) => {
                if let ObserveStep::Failed(e) = &s {
                    rep.errors.push(format!("{}: observe: {e}", r.name));
                }
                rep.observed.push((r.index_id, s));
            }
            Err(e) => rep.errors.push(format!("{}: observe: {e}", r.name)),
        }
        soon(&mut rep, o.observe.usage_every.min(o.observe.watch_every));
        if r.state == IndexState::Retiring {
            soon(&mut rep, Duration::from_secs(3600));
        }
    }

    // 5. Drift.
    let drift_due = memory.last_drift.is_none_or(|t| {
        now < t || now.duration_since(t).unwrap_or_default() >= cadence.drift_every
    });
    if drift_due {
        memory.last_drift = Some(now);
        for (object, o) in &envs {
            if o.mode == Mode::Off {
                continue;
            }
            let cx = ObserveCx {
                tenant,
                view: &o.view,
                shards: &o.shards,
                rules: &o.rules,
                settings: &o.observe,
                ddl: &o.rollout,
                workload: &*hooks.workload,
                decisions: &*hooks.decisions,
                now,
            };
            match drift::reconcile_object(&store, &mut q, &cx, *object).await {
                Ok(d) => rep.drift.push((*object, d)),
                Err(e) => rep.errors.push(format!("drift {}: {e}", object.0)),
            }
        }
    }
    soon(&mut rep, cadence.drift_every);
    let _ = q.disconnect().await;
    Ok(rep)
}

/// Moves a validated row: accepted → `rolling_out` or `recommended`;
/// rejected → `rejected`.
#[allow(clippy::too_many_arguments, clippy::many_single_char_names)]
async fn route(
    store: &AutoIndexStore,
    q: &mut Conn,
    hooks: &AutoIndexHooks,
    tenant: TenantId,
    r: &AutoIndexRow,
    o: &ObjectEnv,
    v: &Validated,
    now: SystemTime,
) -> Result<IndexState> {
    let advise = r
        .observed
        .as_ref()
        .and_then(|o| o.get("route"))
        .and_then(serde_json::Value::as_str)
        == Some("recommend");
    let (to, reason) = match v {
        Validated::Rejected { reason } => (IndexState::Rejected, reason.clone()),
        Validated::Accepted { .. } => {
            let bytes = table_bytes(o).await;
            let method = choose_method(&o.view, bytes, &o.rollout.galera, hooks.mover.is_some());
            if o.mode == Mode::Recommend || advise || r.kind.is_advice() {
                (IndexState::Recommended, "recommend".to_owned())
            } else if let BuildMethod::Recommend(why) = method {
                (IndexState::Recommended, why)
            } else {
                (IndexState::RollingOut, "accepted".to_owned())
            }
        }
        Validated::Pending => return Ok(r.state),
    };
    if to == IndexState::RollingOut {
        for t in &o.shards {
            store
                .set_shard(q, r.index_id, &t.shard, ShardBuild::Pending)
                .await?;
        }
    }
    let moved = store
        .set_state(
            q,
            r.index_id,
            &StateChange {
                from: IndexState::Validating,
                to,
                at: now,
                drop_reason: None,
            },
        )
        .await?;
    if moved {
        let d = IndexDecision {
            tenant,
            kind: match to {
                IndexState::Rejected => "index.reject",
                IndexState::Recommended => "index.recommend",
                _ => "index.create",
            },
            subject_type: "index",
            subject_id: format!("{}/{}", r.object.0, r.name),
            mode: if to == IndexState::Recommended {
                "recommended"
            } else {
                "applied"
            },
            outcome: if to == IndexState::Rejected {
                "rejected"
            } else {
                "scheduled"
            },
            reason_code: reason.chars().take(48).collect(),
            summary: format!("{}: {} ({reason})", r.name, to.as_str()),
            evidence: serde_json::json!({"validation": format!("{v:?}")}),
            alternatives: None,
            at: now,
        };
        if let Err(e) = hooks.decisions.record(&d).await {
            tracing::warn!(error = %e, "decision not recorded");
        }
    }
    Ok(to)
}

/// The largest shard's data plus indexes, from the writer; `u64::MAX`
/// when unknown, so an unknown size never chooses a Galera TOI build.
async fn table_bytes(o: &ObjectEnv) -> u64 {
    let Some(w) = o.view.nodes.writer() else {
        return u64::MAX;
    };
    let Ok(mut n) = Node::connect(w, Duration::from_secs(10)).await else {
        return u64::MAX;
    };
    let mut max = 0;
    for t in &o.shards {
        if let Ok(b) = n.table_bytes(t).await {
            max = max.max(b);
        } else {
            max = u64::MAX;
            break;
        }
    }
    n.close().await;
    max
}

/// Chapter 10's shard lock from the DC's job list: a tenant's shard is
/// "moving" while any copy-class job of the tenant isn't final (R0 applies
/// it per tenant, as dispatch does).
#[derive(Clone)]
pub struct JobShardLocks {
    dc: Arc<dyn MetaStore>,
}

impl fmt::Debug for JobShardLocks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobShardLocks").finish_non_exhaustive()
    }
}

impl JobShardLocks {
    /// Locks read from `dc`'s `jobs/`.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// `build`-class jobs owned (assigned or running) on `cluster`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn builds_on(&self, cluster: &ClusterId) -> Result<u32> {
        Ok(u32::try_from(
            list_jobs(&*self.dc)
                .await?
                .iter()
                .filter(|j| {
                    let j = j.job();
                    j.class == JobClass::Build && j.state.is_owned() && j.clusters.contains(cluster)
                })
                .count(),
        )
        .unwrap_or(u32::MAX))
    }
}

#[async_trait::async_trait]
impl ShardLocks for JobShardLocks {
    async fn state(&self, tenant: TenantId, _shard: &str) -> Result<ShardLock> {
        let moving = list_jobs(&*self.dc).await?.into_iter().find(|j| {
            let j = j.job();
            j.class == JobClass::Copy && j.tenant_id == Some(tenant) && !j.state.is_final()
        });
        Ok(match moving {
            Some(j) => ShardLock::Moving(format!("{} job {}", j.job().kind, j.job().job_id)),
            None => ShardLock::Free,
        })
    }
}

/// The `auto_index` loop.
pub struct AutoIndexLoop {
    env: Arc<dyn AutoIndexEnv>,
    hooks: AutoIndexHooks,
    directory: Directory,
    dc: DcId,
    clock: Arc<dyn Clock>,
    cadence: Cadence,
    slots: LocalSlots,
    memory: Mutex<HashMap<TenantId, Memory>>,
}

impl fmt::Debug for AutoIndexLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutoIndexLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl AutoIndexLoop {
    /// The loop over `env`.
    #[must_use]
    pub fn new(
        env: Arc<dyn AutoIndexEnv>,
        hooks: AutoIndexHooks,
        directory: Directory,
        dc: DcId,
        clock: Arc<dyn Clock>,
        cadence: Cadence,
    ) -> Self {
        Self {
            env,
            hooks,
            directory,
            dc,
            clock,
            cadence,
            slots: LocalSlots::new(),
            memory: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for AutoIndexLoop {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "auto_index"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<TenantId> {
        // Periodic: each pass says when to come back.
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        Ok(self
            .directory
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| e.home_dc == self.dc && e.state == TenantState::Active)
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, cx: &LoopCx, tenant: &TenantId) -> Result<Outcome> {
        cx.leader()?;
        let Some(entry) = self.directory.get(*tenant).await? else {
            self.memory
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(tenant);
            return Ok(Outcome::Done);
        };
        let e = entry.body();
        if e.home_dc != self.dc || e.state != TenantState::Active {
            return Ok(Outcome::Done);
        }
        let mut mem = self
            .memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(tenant)
            .cloned()
            .unwrap_or_default();
        let now = self.clock.wall();
        let rep = tenant_pass(
            &*self.env,
            &self.hooks,
            &self.slots,
            *tenant,
            now,
            &mut mem,
            &self.cadence,
        )
        .await?;
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(*tenant, mem);
        for err in &rep.errors {
            tracing::warn!(tenant = %tenant, error = %err, "auto_index pass");
        }
        Ok(Outcome::RequeueAfter(rep.next_in))
    }

    fn workers(&self) -> usize {
        // Tenants in parallel; builds are still limited per cluster.
        4
    }
}

/// Bootstrap hooks that add the `autoindex` catalog module to `H`'s, so
/// every new tenant catalog gets chapter 13's tables with its core tables.
#[derive(Debug, Clone, Copy, Default)]
pub struct WithAutoIndex<H>(pub H);

#[async_trait::async_trait]
impl<H: BootstrapHooks> BootstrapHooks for WithAutoIndex<H> {
    fn modules(&self) -> Vec<Module> {
        let mut m = self.0.modules();
        if !m.iter().any(|x| x.name == AUTOINDEX_MODULE.name) {
            m.push(AUTOINDEX_MODULE);
        }
        m
    }

    async fn catalog_ready(
        &self,
        tenant: &DirectoryEntry,
        catalog: &CatalogEndpoint,
    ) -> Result<()> {
        self.0.catalog_ready(tenant, catalog).await
    }

    async fn dropping(
        &self,
        tenant: &DirectoryEntry,
        catalog: Option<&CatalogEndpoint>,
    ) -> Result<()> {
        self.0.dropping(tenant, catalog).await
    }
}
