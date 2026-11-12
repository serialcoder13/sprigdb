//! One evaluation of one tenant (chapter 13, "Trigger evaluation" to
//! "Selection under budgets", and the start of "Flows"): the entry point
//! the controller's loop calls every `auto_index.eval_interval_minutes`.
//!
//! ```text
//! evaluate(tenant):
//!   tenant mode off                                  → nothing
//!   W := the workload window (window_hours, tenant scope), Studio left out
//!   q := qualify(W)                                  (qualify module)
//!   watch every qualified fingerprint (chapter 12)
//!   for each object o with a qualified fingerprint:
//!     F      := every fingerprint of W reading or writing o, as FpInput
//!     stats  := the table's statistics; missing or stale columns are
//!               analyzed first when the throttle admits it
//!     cands  := merge(generate(f) for the qualified f on o)
//!     each candidate: covered by an existing index (user, system,
//!       migrated, usable automatic) → covered; an automatic row with its
//!       name still live → covered (exists:<state>); recreate block →
//!       rejected; a failed cost gate → rejected
//!     select under o's budget (cost::select)
//!     each chosen: route(mode, advise, recommend_validate, validation node);
//!       validate-then-roll-out is held to max_new_per_day (day_cap)
//!     write auto_index_candidates (every candidate, its status and
//!       evidence) and, for routed ones, an auto_indexes row in
//!       `validating` (the validation queue) or `recommended`
//!     record decisions with evidence: selectivities, never values
//! ```
//!
//! Every input is injected: the clock, the workload ([`WorkloadSource`]),
//! the catalog's objects ([`ObjectDirectory`]), statistics
//! ([`StatsSource`]), the catalog tables ([`EvalStore`]) and the decision
//! log, so an evaluation is deterministic for tests.
//!
//! **Choices** (also in the report):
//!
//! - **The validation queue** is the `auto_indexes` rows in `validating`,
//!   which the controller's `auto_index` loop validates
//!   ([`Evaluator::evaluate_at`] is its `CandidateSource`; it writes the
//!   rows itself and the loop picks them up). A row whose accepted index
//!   must stop at `recommended` (recommend mode, or the fast trigger on a
//!   large table) carries `observed = {"route": "recommend"}`, the loop's
//!   convention; the route is also in the candidate's evidence
//!   (`route:<...>`). [`Queued`] carries what a validation plan needs
//!   besides samples.
//! - **A definition with an old row.** `auto_indexes` allows one row per
//!   name and table, and dropped rows are kept a year. Re-proposing a
//!   definition whose row is `dropped`, `rejected` or `failed` reuses that
//!   row ([`EvalStore::put_index`]); its history stays in the decision
//!   log. A `rejected` row blocks re-proposal for `recreate_block_days`,
//!   like a harmful drop.
//! - **Fingerprints without probes** (no rows examined yet) get no
//!   candidate this round (`no_probes`): the cost model needs `x_f`, and
//!   they are watched from now on.
//! - **`window_hours`** is read at the tenant: one window feeds every
//!   object, and the cost model's `W` is that window.
//! - **`max_new_per_day`** counts rows created in the last 24 hours that
//!   aren't `recommended` or `rejected`, and applies only to the route
//!   that builds (validate, then roll out).
//! - **An automatic index with no row** (a stray the reconciler will
//!   drop) doesn't count as covering.

pub mod mem;

#[cfg(test)]
mod live_tests;
#[cfg(test)]
pub(crate) mod synth;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use sprig_catalog::Store;
use sprig_common::Result;
use sprig_common::ids::{DbId, Fingerprint, ObjectId, TenantId};
use sprig_common::names::quote;
use sprig_common::time::Clock;
use sprig_settings::SettingCtx;
use sprig_workload::shape::PredOp;
use sprig_workload::window::{FpWindowStats, SampleValue, SampledLiterals, SampledSlot};

use super::candidates::{FpInput, Generator, Literal, Skipped, covered_by, merge};
use super::cost::{AutoOnTable, CostModel, Estimate, Model, Pooled, TableContext, select};
use super::model::{Candidate, IndexInfo, IndexKind, Origin, Role};
use super::policy::{
    Damped, Dampers, DayCaps, Mode, ObjectPolicy, Route, SettingSource, record_churn, route,
};
use super::qualify::{
    NotQualified, Qualification, Qualified, QualifyInput, WorkloadSource, qualify, window_of,
};
use super::settings as k;
use super::stats::{AnalyzeLimits, AnalyzePlan, AnalyzeThrottle, Param, StatsReader, TableStats};
use super::store::{
    AutoIndexRow, AutoIndexStore, Baseline, CandidateRow, CandidateStatus, DecisionLog, Definition,
    DropReason, IndexDecision, IndexState, Model as DataModel, StoredKind,
};
use super::validate::{IndexTarget, Prediction};
use crate::sql::{AT, errno, map_err, micros, row_count, to_json};

/// What the catalog knows about one object.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    /// Its logical database (settings resolve object > database > tenant).
    pub db: DbId,
    /// Data size over all shards, bytes, when known (the fast trigger's
    /// `scan_recommend_above_gb`); else the statistics' `DATA_LENGTH`.
    pub data_bytes: Option<u64>,
    /// Its cluster has a node to validate on (a replica or a Galera
    /// node): `recommend` mode validates only then, and without one the
    /// cost model applies `estimate_margin`.
    pub validation_node: bool,
    /// The cluster's measured build rate, bytes per second.
    pub build_rate_bytes_per_sec: Option<f64>,
}

/// The tenant's catalog objects (chapter 08), as the controller's
/// snapshot knows them.
pub trait ObjectDirectory: Send + Sync {
    /// `object`'s description, `None` for an unknown object.
    fn meta(&self, object: ObjectId) -> Option<ObjectMeta>;

    /// Chapter 33's views over a key-value namespace (`<ns>__hash`,
    /// `<ns>__set`): the element table a view object reads; `None` for
    /// other objects (R6 round 5).
    fn kv_view(&self, object: ObjectId) -> Option<super::kv::KvTarget> {
        let _ = object;
        None
    }
}

impl ObjectDirectory for BTreeMap<ObjectId, ObjectMeta> {
    fn meta(&self, object: ObjectId) -> Option<ObjectMeta> {
        self.get(&object).cloned()
    }
}

/// Where table statistics come from: a node holding a shard of the
/// object (chapter 13, "Statistics").
#[async_trait::async_trait]
pub trait StatsSource: Send + Sync {
    /// The statistics of `object`'s table, `None` when it has none (the
    /// table doesn't exist there).
    ///
    /// # Errors
    /// The node's.
    async fn read(&self, object: ObjectId) -> Result<Option<TableStats>>;

    /// Runs `plan` (`ANALYZE LOCAL ... PERSISTENT FOR`), already admitted
    /// by the throttle.
    ///
    /// # Errors
    /// The node's.
    async fn analyze(&self, object: ObjectId, plan: &AnalyzePlan) -> Result<()>;

    /// Makes `f`'s sampled values on table `alias` comparable with
    /// `stats`' histogram bounds (text by `WEIGHT_STRING()` when the
    /// bounds were reweighed).
    ///
    /// # Errors
    /// The node's.
    async fn prepare(&self, alias: &str, stats: &TableStats, f: &mut FpInput) -> Result<()>;
}

/// Statistics from one node: each object's physical table there.
#[derive(Clone)]
pub struct NodeStats {
    /// The node.
    pub pool: Pool,
    /// Each object's (schema, table) on that node.
    pub tables: BTreeMap<ObjectId, (String, String)>,
    /// Reader options.
    pub reader: StatsReader,
}

impl std::fmt::Debug for NodeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStats")
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl StatsSource for NodeStats {
    async fn read(&self, object: ObjectId) -> Result<Option<TableStats>> {
        let Some((schema, table)) = self.tables.get(&object) else {
            return Ok(None);
        };
        let mut c = self.pool.get_conn().await.map_err(map_err)?;
        let t = self.reader.read(&mut c, schema, table).await?;
        Ok((!t.columns.is_empty()).then_some(t))
    }

    async fn analyze(&self, _object: ObjectId, plan: &AnalyzePlan) -> Result<()> {
        let mut c = self.pool.get_conn().await.map_err(map_err)?;
        super::stats::analyze(&mut c, plan).await
    }

    async fn prepare(&self, alias: &str, stats: &TableStats, f: &mut FpInput) -> Result<()> {
        if !self.reader.weigh_text || f.samples.is_empty() {
            return Ok(());
        }
        let mut c = self.pool.get_conn().await.map_err(map_err)?;
        super::candidates::weigh_samples(&mut c, alias, stats, f).await
    }
}

/// The catalog tables an evaluation reads and writes.
#[async_trait::async_trait]
pub trait EvalStore: Send + Sync {
    /// Reserves the next evaluation run id.
    ///
    /// # Errors
    /// The catalog's.
    async fn next_run_id(&self) -> Result<u64>;

    /// Every `auto_indexes` row of `object`, dropped ones included.
    ///
    /// # Errors
    /// The catalog's.
    async fn indexes_on(&self, object: ObjectId) -> Result<Vec<AutoIndexRow>>;

    /// Candidates decided at or after `since` (cooldown).
    ///
    /// # Errors
    /// The catalog's.
    async fn decided_since(&self, since: SystemTime) -> Result<Vec<CandidateRow>>;

    /// Index rows created at or after `since` (`max_new_per_day`).
    ///
    /// # Errors
    /// The catalog's.
    async fn created_since(&self, since: SystemTime) -> Result<Vec<AutoIndexRow>>;

    /// Inserts a candidate; returns its id.
    ///
    /// # Errors
    /// The catalog's.
    async fn insert_candidate(&self, r: &CandidateRow) -> Result<u64>;

    /// Inserts index row `r`, or reuses the row of the same name on the
    /// same object when it is `dropped`, `rejected` or `failed`. `None`
    /// when a live row of that name exists (another evaluation won).
    ///
    /// # Errors
    /// The catalog's.
    async fn put_index(&self, r: &AutoIndexRow) -> Result<Option<super::model::AutoIndexId>>;
}

/// [`EvalStore`] on a tenant catalog.
#[derive(Clone)]
pub struct CatalogStore {
    /// Connections to the catalog.
    pub pool: Pool,
    /// The core tables (run counter).
    pub core: Store,
    /// The automatic-indexing tables.
    pub tables: AutoIndexStore,
}

impl std::fmt::Debug for CatalogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogStore")
            .field("schema", &self.tables.schema())
            .finish_non_exhaustive()
    }
}

impl CatalogStore {
    /// The store of catalog schema `schema` on `pool`.
    #[must_use]
    pub fn new(pool: Pool, schema: &str) -> Self {
        let core = Store::new(schema.to_owned());
        let tables = AutoIndexStore::of(&core);
        Self { pool, core, tables }
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        self.pool.get_conn().await.map_err(map_err)
    }
}

#[async_trait::async_trait]
impl EvalStore for CatalogStore {
    async fn next_run_id(&self) -> Result<u64> {
        let mut c = self.conn().await?;
        self.tables.next_run_id(&self.core, &mut c).await
    }

    async fn indexes_on(&self, object: ObjectId) -> Result<Vec<AutoIndexRow>> {
        let mut c = self.conn().await?;
        self.tables.indexes_on(&mut c, object).await
    }

    async fn decided_since(&self, since: SystemTime) -> Result<Vec<CandidateRow>> {
        let mut c = self.conn().await?;
        self.tables.decided_since(&mut c, since).await
    }

    async fn created_since(&self, since: SystemTime) -> Result<Vec<AutoIndexRow>> {
        let mut c = self.conn().await?;
        self.tables.created_since(&mut c, since).await
    }

    async fn insert_candidate(&self, r: &CandidateRow) -> Result<u64> {
        let mut c = self.conn().await?;
        self.tables.insert_candidate(&mut c, r).await
    }

    async fn put_index(&self, r: &AutoIndexRow) -> Result<Option<super::model::AutoIndexId>> {
        let mut c = self.conn().await?;
        match self.tables.insert_index(&mut c, r).await {
            Ok(id) => return Ok(Some(id)),
            Err(e) if errno(&e) == Some(1062) => {}
            Err(e) => return Err(e),
        }
        // Reuse a terminal row of the same name.
        let sql = format!(
            "UPDATE {}.auto_indexes SET state = ?, prior_state = NULL, candidate_id = ?, \
             validation_id = NULL, decision_id = ?, est_benefit_ms_h = ?, est_write_ms_h = ?, \
             est_bytes = ?, actual_bytes = NULL, shards = ?, baseline = ?, observed = ?, \
             last_used_at = NULL, drop_reason = NULL, created_at = {AT}, \
             state_changed_at = {AT} \
             WHERE object_id = ? AND name = ? AND state IN ('dropped','rejected','failed')",
            quote(self.tables.schema())
        );
        let p: Vec<mysql_async::Value> = vec![
            r.state.as_str().into(),
            r.candidate_id.into(),
            r.decision_id.into(),
            r.est_benefit_ms_h.into(),
            r.est_write_ms_h.into(),
            r.est_bytes.into(),
            to_json(&r.shards)?.into(),
            to_json(&r.baseline)?.into(),
            match &r.observed {
                Some(o) => to_json(o)?.into(),
                None => mysql_async::Value::NULL,
            },
            micros(r.created_at).into(),
            micros(r.state_changed_at).into(),
            r.object.0.into(),
            r.name.as_str().into(),
        ];
        c.exec_drop(sql, p).await.map_err(map_err)?;
        if row_count(&mut c).await? != 1 {
            return Ok(None);
        }
        Ok(self
            .tables
            .index_by_name(&mut c, r.object, &r.name)
            .await?
            .map(|row| row.index_id))
    }
}

/// `SampleValue` → [`Param`]: numbers and booleans as numbers, strings and
/// temporal literals as text; anything else (hex, bits, an unbound
/// placeholder) as `NULL`, which no histogram reads.
#[must_use]
pub fn param(v: &SampleValue) -> Param {
    use sprig_workload::fingerprint::LiteralKind as K;
    match v.kind {
        K::Number => v.as_f64().map_or(Param::Null, Param::Num),
        K::Bool => {
            let t = String::from_utf8_lossy(&v.text);
            Param::Num(if t.trim().eq_ignore_ascii_case("true") {
                1.0
            } else {
                0.0
            })
        }
        K::String | K::Temporal => v.string_value(false).map_or(Param::Null, |b| {
            Param::Text(String::from_utf8_lossy(&b).into_owned())
        }),
        K::Hex | K::Bit | K::Param => Param::Null,
    }
}

/// One sample's literal list.
#[must_use]
pub fn literals(s: &SampledLiterals) -> Vec<Literal> {
    s.slots
        .iter()
        .map(|slot| match slot {
            SampledSlot::One(v) => Literal::Value(param(v)),
            SampledSlot::List(vs) => Literal::List(vs.iter().map(param).collect()),
        })
        .collect()
}

/// Chapter 12's window statistics as the generator and the cost model
/// read them.
#[must_use]
pub fn fp_input(w: &FpWindowStats, fast_only: bool) -> FpInput {
    FpInput {
        id: w.id,
        shape: Arc::clone(&w.shape),
        runs: w.runs(),
        mean_ms: w.mean_ms(),
        p95_ms: w.p95_ms(),
        examined_per_run: w.examined_per_run().unwrap_or(0.0),
        returned_per_run: w.returned_per_run(),
        affected_per_run: w.affected_per_run(),
        limit: w.sampled_limit(),
        in_list_max: w.in_list_max(),
        samples: w.samples.iter().map(literals).collect(),
        fast_only,
    }
}

/// One candidate's fate in this evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateOutcome {
    /// `auto_index_candidates.candidate_id`.
    pub candidate_id: u64,
    /// The candidate.
    pub candidate: Candidate,
    /// Its status.
    pub status: CandidateStatus,
    /// `covered_by:<index>`, `below_min_net`, `budget`, ...
    pub reason: Option<String>,
    /// The cost model's estimate, when it got that far.
    pub estimate: Option<Estimate>,
}

/// A chosen candidate routed to validation or a recommendation: what the
/// rollout loop needs to build its validation plan (samples apart, which
/// it reads from the workload store).
#[derive(Debug, Clone, PartialEq)]
pub struct Queued {
    /// The object.
    pub object: ObjectId,
    /// `auto_index_candidates.candidate_id`.
    pub candidate_id: u64,
    /// The `auto_indexes` row (`validating` or `recommended`).
    pub index_id: super::model::AutoIndexId,
    /// Where it goes: validate then roll out, validate then recommend, or
    /// recommend.
    pub route: Route,
    /// The candidate.
    pub candidate: Candidate,
    /// Its estimate.
    pub estimate: Estimate,
    /// Predictions for its primary fingerprints (estimate-and-canary
    /// validation checks them).
    pub predictions: Vec<Prediction>,
    /// Columns each served fingerprint reads on the table (the covering
    /// retry).
    pub columns_read: BTreeMap<Fingerprint, Vec<String>>,
    /// Other fingerprints on the table, by total time, at most
    /// `auto_index.regression_fingerprints`: replayed for regressions.
    pub regression_fps: Vec<Fingerprint>,
    /// The active automatic index it replaces, retired as `replaced` once
    /// it is active.
    pub replaces: Option<String>,
    /// The table in physical names, on the node statistics came from.
    pub target: IndexTarget,
    /// Per served fingerprint (16 hex): runs, p95 and mean before the
    /// index, as in `auto_indexes.baseline`.
    pub baseline: BTreeMap<String, Baseline>,
    /// Only the fast trigger on a table above `scan_recommend_above_gb`
    /// qualified it: it stops at a recommendation whatever the mode.
    pub advise: bool,
}

/// What a `validating` row's validation plan needs besides samples,
/// kept in `auto_indexes.observed.plan` so the loop's validation step can
/// build the plan in any later pass (and after a leader change): the
/// predictions estimate-and-canary validation checks, the columns each
/// served fingerprint reads (the covering retry), the regression
/// fingerprints and the table in physical names. Fingerprints are 16 hex
/// digits, as in `auto_indexes.baseline`.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlanInputs {
    /// Predictions for the primary fingerprints.
    #[serde(default)]
    pub predictions: Vec<Prediction>,
    /// Columns each served fingerprint reads on the table.
    #[serde(default)]
    pub columns_read: BTreeMap<String, Vec<String>>,
    /// Other fingerprints on the table, by total time.
    #[serde(default)]
    pub regression_fps: Vec<String>,
    /// The table, in physical names.
    pub target: Option<IndexTarget>,
}

impl PlanInputs {
    /// The plan inputs of a queued candidate.
    #[must_use]
    pub fn new(
        predictions: &[Prediction],
        columns_read: &BTreeMap<Fingerprint, Vec<String>>,
        regression_fps: &[Fingerprint],
        target: &IndexTarget,
    ) -> Self {
        Self {
            predictions: predictions.to_vec(),
            columns_read: columns_read
                .iter()
                .map(|(f, c)| (f.to_string(), c.clone()))
                .collect(),
            regression_fps: regression_fps.iter().map(ToString::to_string).collect(),
            target: Some(target.clone()),
        }
    }

    /// The plan inputs kept in a row's `observed`, if any.
    #[must_use]
    pub fn of(observed: Option<&serde_json::Value>) -> Option<Self> {
        observed
            .and_then(|o| o.get("plan"))
            .and_then(|p| serde_json::from_value(p.clone()).ok())
    }

    /// `columns_read` keyed by fingerprint.
    #[must_use]
    pub fn columns_by_fp(&self) -> BTreeMap<Fingerprint, Vec<String>> {
        self.columns_read
            .iter()
            .filter_map(|(f, c)| f.parse().ok().map(|f| (f, c.clone())))
            .collect()
    }

    /// `regression_fps` parsed.
    #[must_use]
    pub fn regression(&self) -> Vec<Fingerprint> {
        self.regression_fps
            .iter()
            .filter_map(|f| f.parse().ok())
            .collect()
    }
}

/// One object's part of an evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct TableOutcome {
    /// The object.
    pub object: ObjectId,
    /// Every candidate, with its status.
    pub candidates: Vec<CandidateOutcome>,
    /// The routed ones.
    pub queued: Vec<Queued>,
    /// An automatic index to retire now (the table is over a tightened
    /// budget): the observe loop moves it to `retiring`.
    pub retire: Option<(String, DropReason)>,
    /// Qualified fingerprints that got no candidate here, with the reason
    /// (`function:LOWER`, `no_probes`, `no_statistics`, ...).
    pub skipped: Vec<Skipped>,
    /// Statistics collected before generation.
    pub analyzed: Option<AnalyzePlan>,
    /// Statistics wanted but held back by the throttle.
    pub analyze_deferred: Option<AnalyzePlan>,
    /// The table couldn't be evaluated; the others were.
    pub error: Option<String>,
}

impl TableOutcome {
    /// An empty outcome for `object`.
    #[must_use]
    pub fn new(object: ObjectId) -> Self {
        Self {
            object,
            candidates: Vec::new(),
            queued: Vec::new(),
            retire: None,
            skipped: Vec::new(),
            analyzed: None,
            analyze_deferred: None,
            error: None,
        }
    }
}

/// The result of one evaluation of one tenant.
#[derive(Debug, Clone)]
pub struct Evaluation {
    /// The tenant.
    pub tenant: TenantId,
    /// When.
    pub at: SystemTime,
    /// The tenant's mode.
    pub mode: Mode,
    /// `auto_index_candidates.run_id`, when anything qualified.
    pub run_id: Option<u64>,
    /// Qualified (fingerprint, object) pairs.
    pub qualified: Vec<Qualified>,
    /// The others, with reasons.
    pub not_qualified: Vec<NotQualified>,
    /// Per object with a qualified fingerprint.
    pub tables: Vec<TableOutcome>,
}

impl Evaluation {
    /// Every routed candidate, in table order.
    pub fn queued(&self) -> impl Iterator<Item = &Queued> + '_ {
        self.tables.iter().flat_map(|t| t.queued.iter())
    }
}

/// What one evaluation reads and writes.
pub struct EvalCx<'a> {
    /// The tenant.
    pub tenant: TenantId,
    /// Settings.
    pub settings: &'a dyn SettingSource,
    /// The tenant's settings context (dc, tenant, ancestry; no database
    /// or object).
    pub ctx: SettingCtx<'a>,
    /// The workload store.
    pub workload: &'a dyn WorkloadSource,
    /// The catalog's objects.
    pub objects: &'a dyn ObjectDirectory,
    /// Table statistics.
    pub stats: &'a dyn StatsSource,
    /// The catalog tables.
    pub store: &'a dyn EvalStore,
    /// The decision log.
    pub decisions: &'a dyn DecisionLog,
}

#[derive(Clone)]
struct DynClock(Arc<dyn Clock>);

impl Clock for DynClock {
    fn now(&self) -> Instant {
        self.0.now()
    }
    fn wall(&self) -> SystemTime {
        self.0.wall()
    }
}

/// Runs evaluations: holds the clock, the analyze throttle and the pacing
/// state, shared across tenants and runs.
pub struct Evaluator {
    clock: Arc<dyn Clock>,
    throttle: AnalyzeThrottle<DynClock>,
    limits: AnalyzeLimits,
    last: Mutex<HashMap<TenantId, Instant>>,
    /// Skips already recorded, so a table that keeps getting no candidate
    /// is recorded once per window, not once per evaluation.
    skips: Mutex<HashMap<SkipKey, SystemTime>>,
}

/// A recorded skip: tenant, fingerprint, object and reason.
type SkipKey = (TenantId, Fingerprint, ObjectId, String);

impl std::fmt::Debug for Evaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Evaluator")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

/// A candidate's status and reason, before it is written.
struct Fate {
    candidate: Candidate,
    status: CandidateStatus,
    reason: Option<String>,
    estimate: Option<Estimate>,
    route: Option<Route>,
    replaces: Option<String>,
    advise: bool,
}

impl Evaluator {
    /// An evaluator reading time from `clock`, with the default analyze
    /// throttle.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self::with_limits(clock, AnalyzeLimits::default())
    }

    /// An evaluator with analyze `limits`.
    #[must_use]
    pub fn with_limits(clock: Arc<dyn Clock>, limits: AnalyzeLimits) -> Self {
        Self {
            throttle: AnalyzeThrottle::new(DynClock(Arc::clone(&clock)), limits),
            clock,
            limits,
            last: Mutex::new(HashMap::new()),
            skips: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `tenant` is due: never evaluated by this evaluator, or last
    /// started at least `interval` ago.
    #[must_use]
    pub fn due(&self, tenant: TenantId, interval: Duration) -> bool {
        let now = self.clock.now();
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            // A clock that went back (a test clock reset) makes it due.
            .is_none_or(|t| now < *t || now.saturating_duration_since(*t) >= interval)
    }

    /// Whether skip `s` wasn't recorded within `window` of `now` (and
    /// remembers it as recorded now).
    fn skip_is_new(
        &self,
        tenant: TenantId,
        s: &Skipped,
        now: SystemTime,
        window: Duration,
    ) -> bool {
        let Some(object) = s.object else {
            return false;
        };
        let mut m = self.skips.lock().unwrap_or_else(PoisonError::into_inner);
        m.retain(|_, at| now.duration_since(*at).unwrap_or_default() < window);
        let key = (tenant, s.fp, object, s.reason.clone());
        if m.contains_key(&key) {
            return false;
        }
        m.insert(key, now);
        true
    }

    /// Runs [`evaluate`](Self::evaluate) when the tenant is due under
    /// `auto_index.eval_interval_minutes` (resolved at the tenant);
    /// `None` when it isn't. The pace counts from each start, so a failing
    /// evaluation is retried at the next interval, not at once.
    ///
    /// # Errors
    /// As [`evaluate`](Self::evaluate).
    pub async fn evaluate_if_due(&self, cx: &EvalCx<'_>) -> Result<Option<Evaluation>> {
        let r = cx.settings.resolve(k::EVAL_INTERVAL_MINUTES.key, &cx.ctx)?;
        let minutes = r
            .value
            .as_f64()
            .unwrap_or(f64::from(k::EVAL_INTERVAL_MINUTES.default))
            .max(1.0);
        let interval = Duration::from_secs_f64(minutes * 60.0);
        if !self.due(cx.tenant, interval) {
            return Ok(None);
        }
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cx.tenant, self.clock.now());
        self.evaluate(cx).await.map(Some)
    }

    /// One evaluation of `cx.tenant` now (the evaluator's clock).
    ///
    /// # Errors
    /// As [`evaluate_at`](Self::evaluate_at).
    pub async fn evaluate(&self, cx: &EvalCx<'_>) -> Result<Evaluation> {
        self.evaluate_at(cx, self.clock.wall()).await
    }

    /// One evaluation of `cx.tenant` at `now`: the entry point of the
    /// controller's `auto_index` loop, which paces it itself. It writes
    /// the candidate rows and the `validating` (or `recommended`) index
    /// rows; the loop's validation step picks the `validating` rows up.
    ///
    /// # Errors
    /// Settings, the workload store or the catalog failing before any
    /// table is evaluated. A table whose statistics or rows fail is
    /// reported in its [`TableOutcome::error`]; the others go on.
    #[allow(clippy::too_many_lines)] // the chapter's flow, in one place
    pub async fn evaluate_at(&self, cx: &EvalCx<'_>, now: SystemTime) -> Result<Evaluation> {
        let tctx = SettingCtx {
            db: None,
            object: None,
            ..cx.ctx
        };
        let tenant_policy = ObjectPolicy::resolve(cx.settings, &tctx)?;
        metrics::counter!("sprig_advisor_autoindex_runs_total").increment(1);
        let mut ev = Evaluation {
            tenant: cx.tenant,
            at: now,
            mode: tenant_policy.mode,
            run_id: None,
            qualified: Vec::new(),
            not_qualified: Vec::new(),
            tables: Vec::new(),
        };
        if tenant_policy.mode == Mode::Off {
            return Ok(ev);
        }
        let window_hours = tenant_policy.trigger.window_hours;
        let from = now
            .checked_sub(Duration::from_secs(u64::from(window_hours) * 3600))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut entries = cx.workload.window(cx.tenant, from, now).await?;
        kv_views(cx, &mut entries);

        // Every object the window references, with its settings.
        let mut objects: BTreeSet<ObjectId> = BTreeSet::new();
        for e in &entries {
            objects.extend(e.window.shape.object_ids());
        }
        // A fingerprint an index now serves is judged on its runs since
        // that index was built, not the whole window: the slow runs from
        // before the build would otherwise propose redundant variants
        // beside it (M0.4, cooldown_hours = 0).
        let mut cut: BTreeMap<Fingerprint, SystemTime> = BTreeMap::new();
        for o in &objects {
            for r in cx.store.indexes_on(*o).await? {
                if !matches!(
                    r.state,
                    IndexState::Observing | IndexState::Active | IndexState::Frozen
                ) {
                    continue;
                }
                let built = super::observe::built_at(&r);
                if built <= from {
                    continue;
                }
                for fp in r
                    .baseline
                    .keys()
                    .filter_map(|k| k.parse::<Fingerprint>().ok())
                {
                    let t = cut.entry(fp).or_insert(built);
                    *t = (*t).max(built);
                }
            }
        }
        if !cut.is_empty() {
            entries = since_served(cx, entries, &cut, now).await?;
            kv_views(cx, &mut entries);
        }
        let mut metas: BTreeMap<ObjectId, ObjectMeta> = BTreeMap::new();
        let mut policies: BTreeMap<ObjectId, ObjectPolicy> = BTreeMap::new();
        for o in &objects {
            let Some(m) = cx.objects.meta(*o) else {
                continue;
            };
            let octx = tctx.with_db(m.db).with_object(*o);
            policies.insert(*o, ObjectPolicy::resolve(cx.settings, &octx)?);
            metas.insert(*o, m);
        }
        let include = |w: &FpWindowStats| {
            w.shape
                .main_table()
                .and_then(|t| t.object_id)
                .and_then(|o| policies.get(&o))
                .map_or(tenant_policy.trigger.include_studio, |p| {
                    p.trigger.include_studio
                })
        };
        let (w, excluded) = window_of(entries, &include);
        let cooldown_h = policies
            .values()
            .map(|p| p.trigger.cooldown_hours)
            .chain([tenant_policy.trigger.cooldown_hours])
            .max()
            .unwrap_or(0);
        let since = now
            .checked_sub(Duration::from_secs(u64::from(cooldown_h) * 3600))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let decided = cx.store.decided_since(since).await?;
        let policy_of = |o: ObjectId| policies.get(&o);
        let size_of = |o: ObjectId| metas.get(&o).and_then(|m| m.data_bytes);
        let q = qualify(
            w,
            &excluded,
            &QualifyInput {
                tenant_mode: tenant_policy.mode,
                policy: &policy_of,
                data_bytes: &size_of,
                decided: &decided,
                now,
            },
        );
        let watched: BTreeSet<Fingerprint> = q.qualified.iter().map(|x| x.fp).collect();
        for fp in &watched {
            cx.workload.watch(cx.tenant, *fp);
        }
        ev.qualified.clone_from(&q.qualified);
        ev.not_qualified.clone_from(&q.not_qualified);
        // Fingerprints that fired a trigger but that no index can serve
        // (every predicate unsargable): recorded as skipped candidates, so
        // `sprigctl explain query` says why (`function:LOWER`, ...).
        let unsargable: Vec<Skipped> = q
            .not_qualified
            .iter()
            .filter_map(|n| match (&n.why, n.object) {
                (super::qualify::Miss::Unsargable { .. }, Some(o)) => {
                    Some(skip(n.fp, o, &n.why.code()))
                }
                _ => None,
            })
            .collect();
        let window = Duration::from_secs(u64::from(window_hours) * 3600);
        let fresh_unsargable: Vec<Skipped> = unsargable
            .into_iter()
            .filter(|s| self.skip_is_new(cx.tenant, s, now, window))
            .collect();
        if q.qualified.is_empty() && fresh_unsargable.is_empty() {
            return Ok(ev);
        }
        let run_id = cx.store.next_run_id().await?;
        ev.run_id = Some(run_id);
        for sk in &fresh_unsargable {
            let Some(object) = sk.object else { continue };
            let mode = policies.get(&object).map_or(tenant_policy.mode, |p| p.mode);
            record_skip(cx, run_id, now, mode, sk, None).await?;
        }
        if q.qualified.is_empty() {
            return Ok(ev);
        }

        // Daily caps: rows that became (or may become) indexes in the
        // last 24 hours.
        let day_ago = now
            .checked_sub(Duration::from_secs(86_400))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut created: Vec<(DbId, ObjectId)> = cx
            .store
            .created_since(day_ago)
            .await?
            .into_iter()
            .filter(|r| !matches!(r.state, IndexState::Recommended | IndexState::Rejected))
            .map(|r| {
                let db = metas.get(&r.object).map_or_else(
                    || cx.objects.meta(r.object).map_or(DbId(0), |m| m.db),
                    |m| m.db,
                );
                (db, r.object)
            })
            .collect();
        let objs: Vec<(DbId, ObjectId)> = q
            .objects()
            .into_iter()
            .filter_map(|o| metas.get(&o).map(|m| (m.db, o)))
            .collect();
        let caps = DayCaps::resolve(cx.settings, &tctx, &objs)?;

        for object in q.objects() {
            let (Some(meta), Some(policy)) = (metas.get(&object), policies.get(&object)) else {
                continue;
            };
            let t = Table {
                cx,
                q: &q,
                object,
                meta,
                policy,
                window_hours,
                run_id,
                now,
                decided: &decided,
                caps: &caps,
            };
            let outcome = match self.table(&t, &mut created).await {
                Ok(o) => {
                    let window = Duration::from_secs(u64::from(window_hours) * 3600);
                    for sk in o
                        .skipped
                        .iter()
                        .filter(|sk| self.skip_is_new(cx.tenant, sk, now, window))
                    {
                        let trigger = q.on(object).find(|x| x.fp == sk.fp).map(trigger_json);
                        record_skip(cx, run_id, now, policy.mode, sk, trigger).await?;
                    }
                    o
                }
                Err(e) => {
                    tracing::warn!(tenant = %cx.tenant, object = %object, error = %e,
                        "automatic indexing: table not evaluated");
                    TableOutcome {
                        error: Some(e.to_string()),
                        ..TableOutcome::new(object)
                    }
                }
            };
            ev.tables.push(outcome);
        }
        Ok(ev)
    }

    /// Statistics for `t.object`, analyzed first when they are missing
    /// or stale and the throttle admits it.
    async fn stats(
        &self,
        t: &Table<'_, '_>,
        quals: &[&FpInput],
        out: &mut TableOutcome,
    ) -> Result<Option<TableStats>> {
        let Some(stats) = t.cx.stats.read(t.object).await? else {
            return Ok(None);
        };
        let (cols, hist) = wanted_columns(quals, t.object);
        let c: Vec<&str> = cols.iter().map(String::as_str).collect();
        let h: Vec<&str> = hist.iter().map(String::as_str).collect();
        let Some(plan) = super::stats::plan_analyze(&stats, &c, &h, &self.limits) else {
            return Ok(Some(stats));
        };
        if !self.throttle.admit(t.cx.tenant.0, &plan) {
            out.analyze_deferred = Some(plan);
            return Ok(Some(stats));
        }
        t.cx.stats.analyze(t.object, &plan).await?;
        out.analyzed = Some(plan);
        t.cx.stats.read(t.object).await
    }

    #[allow(clippy::too_many_lines)] // one table's pass, in order
    async fn table(
        &self,
        t: &Table<'_, '_>,
        created: &mut Vec<(DbId, ObjectId)>,
    ) -> Result<TableOutcome> {
        let cx = t.cx;
        let object = t.object;
        let policy = t.policy;
        let mut out = TableOutcome::new(object);
        let quals: BTreeMap<Fingerprint, &Qualified> = t.q.on(object).map(|x| (x.fp, x)).collect();

        // Every fingerprint of W on the table, as the cost model reads it.
        // Reads without probes are left out (their x_f is unknown); a
        // qualified one waits for its probes.
        let mut inputs: Vec<FpInput> = Vec::new();
        for (id, w) in &t.q.windows {
            if !w.shape.object_ids().contains(&object) {
                continue;
            }
            let probed = w.stats.examined_samples > 0;
            let write = w.shape.writes.is_some();
            if let Some(qf) = quals.get(id) {
                if !probed && !write {
                    out.skipped.push(skip(*id, object, "no_probes"));
                    continue;
                }
                inputs.push(fp_input(w, qf.fast_only()));
            } else if probed || write {
                inputs.push(fp_input(w, false));
            }
        }
        let targets: Vec<&FpInput> = inputs
            .iter()
            .filter(|f| quals.contains_key(&f.id))
            .collect();
        if targets.is_empty() {
            return Ok(out);
        }
        let Some(stats) = self.stats(t, &targets, &mut out).await? else {
            for f in &targets {
                out.skipped.push(skip(f.id, object, "no_statistics"));
            }
            return Ok(out);
        };
        for f in &mut inputs {
            let shape = Arc::clone(&f.shape);
            for tr in shape
                .tables
                .iter()
                .filter(|tr| tr.object_id == Some(object) && !tr.derived)
            {
                cx.stats.prepare(&tr.alias, &stats, f).await?;
            }
        }
        let advise_by_stats = |qf: &Qualified| {
            qf.advise
                || (t.meta.data_bytes.is_none()
                    && super::qualify::advise_for(qf.trigger, Some(stats.data_bytes), policy))
        };

        // Existing indexes: automatic ones count when usable.
        let rows = cx.store.indexes_on(object).await?;
        let mut existing: Vec<IndexInfo> = stats.indexes.clone();
        for e in existing.iter_mut().filter(|e| e.origin == Origin::Auto) {
            match rows.iter().find(|r| r.name == e.name) {
                Some(r) => {
                    e.usable = matches!(
                        r.state,
                        IndexState::Observing | IndexState::Active | IndexState::Frozen
                    );
                    e.ignored |= r.state == IndexState::Retiring;
                }
                None => e.usable = false,
            }
        }

        // Generate and merge.
        let generator = Generator::new(policy.generation);
        let mut cands: Vec<Candidate> = Vec::new();
        for f in inputs.iter().filter(|f| quals.contains_key(&f.id)) {
            let g = generator.generate_all(f, &|o| (o == object).then_some(&stats));
            cands.extend(g.candidates.into_iter().filter(|c| c.def.object == object));
            out.skipped
                .extend(g.skipped.into_iter().filter(|s| s.object == Some(object)));
        }
        let merged = merge_beside_covering(cands);

        // Coverage, existing rows, dampers, cost gates.
        let mut params = policy.cost_for(t.meta.build_rate_bytes_per_sec, t.meta.validation_node);
        params.window_hours = f64::from(t.window_hours);
        let ctx = TableContext::new(object, &stats, &inputs, params);
        let dampers = Dampers::of(policy);
        // Cooldown is per object, as in qualification.
        let decided_here: Vec<CandidateRow> = t
            .decided
            .iter()
            .filter(|c| c.object == object)
            .cloned()
            .collect();
        let block = Duration::from_secs(u64::from(policy.observe.recreate_block_days) * 86_400);
        let mut fates: Vec<Fate> = Vec::new();
        let mut pool: Vec<Pooled> = Vec::new();
        let fate = |c: Candidate, status, reason: Option<String>, est| Fate {
            candidate: c,
            status,
            reason,
            estimate: est,
            route: None,
            replaces: None,
            advise: false,
        };
        for c in merged {
            if let Some(e) = covered_by(&c, &existing) {
                let r = format!("covered_by:{}", e.name);
                fates.push(fate(c, CandidateStatus::Covered, Some(r), None));
                continue;
            }
            let name = c.def.name();
            if let Some(r) = rows.iter().find(|r| r.name == name) {
                let recent = t
                    .now
                    .duration_since(r.state_changed_at)
                    .map_or(true, |a| a < block);
                if !r.state.is_terminal() {
                    let reason = format!("exists:{}", r.state);
                    fates.push(fate(c, CandidateStatus::Covered, Some(reason), None));
                    continue;
                }
                if r.state == IndexState::Rejected && recent {
                    let reason = "recreate_blocked:rejected".to_owned();
                    fates.push(fate(c, CandidateStatus::Rejected, Some(reason), None));
                    continue;
                }
            }
            let fps: Vec<Fingerprint> = c.serves.iter().map(|(f, _)| *f).collect();
            match dampers.check(&c.def, &fps, &rows, &decided_here, t.now) {
                Damped::Allow => {}
                d @ Damped::RecreateBlocked(_) => {
                    fates.push(fate(c, CandidateStatus::Rejected, d.reason(), None));
                    continue;
                }
                d @ Damped::Cooldown => {
                    fates.push(fate(c, CandidateStatus::Deferred, d.reason(), None));
                    continue;
                }
            }
            let est = Model.estimate(&c, &ctx);
            if let Some(g) = est.failed.first() {
                let r = g.reason();
                fates.push(fate(c, CandidateStatus::Rejected, Some(r), Some(est)));
                continue;
            }
            let fast_only = c
                .serves
                .iter()
                .all(|(f, _)| quals.get(f).is_some_and(|q| q.fast_only()));
            pool.push(Pooled {
                candidate: c,
                estimate: est,
                fast_only,
            });
        }

        // Selection under the table's budget.
        let auto: Vec<AutoOnTable> = rows
            .iter()
            .filter(|r| r.state.counts_for_budget())
            .map(on_table)
            .collect();
        let estimates: BTreeMap<String, Estimate> = pool
            .iter()
            .map(|p| (p.candidate.def.canonical(), p.estimate.clone()))
            .collect();
        let sel = select(pool, &ctx, &auto, policy.budget);
        out.retire.clone_from(&sel.retire);
        for ch in sel.chosen {
            let c = ch.candidate;
            let advise = c
                .primaries()
                .all(|f| quals.get(&f).is_some_and(|q| advise_by_stats(q)));
            // An ANN index changes results: `auto_index.vector_ann` decides
            // (chapter 13, "Other models"; R6 round 3).
            let mode = if c.def.kind == IndexKind::Vector {
                match (policy.mode, policy.vector_ann) {
                    (Mode::Off, _) | (_, Mode::Off) => Mode::Off,
                    (Mode::On, Mode::On) => Mode::On,
                    _ => Mode::Recommend,
                }
            } else {
                policy.mode
            };
            let r = route(
                mode,
                advise,
                policy.recommend_validate,
                t.meta.validation_node,
            );
            if r == Route::Skip {
                continue;
            }
            if r == Route::ValidateThenRollOut && !t.caps.admits(created, t.meta.db, object) {
                let f = fate(
                    c,
                    CandidateStatus::Deferred,
                    Some("day_cap".into()),
                    Some(ch.estimate),
                );
                fates.push(f);
                continue;
            }
            if r == Route::ValidateThenRollOut {
                created.push((t.meta.db, object));
            }
            let new_status = if r == Route::Recommend {
                CandidateStatus::Recommended
            } else {
                CandidateStatus::Selected
            };
            fates.push(Fate {
                candidate: c,
                status: new_status,
                reason: None,
                estimate: Some(ch.estimate),
                route: Some(r),
                replaces: ch.replaces,
                advise,
            });
        }
        for (c, reason) in sel.deferred {
            let est = estimates.get(&c.def.canonical()).cloned();
            let new_status = if reason.starts_with("covered_by:") {
                CandidateStatus::Covered
            } else {
                CandidateStatus::Deferred
            };
            fates.push(fate(c, new_status, Some(reason), est));
        }

        let target = IndexTarget {
            schema: stats.schema.clone(),
            table: stats.table.clone(),
            pk: stats.primary_key.clone(),
        };
        let regression_n = cx
            .settings
            .resolve(
                k::REGRESSION_FINGERPRINTS.key,
                &t.cx.ctx.with_db(t.meta.db).with_object(object),
            )
            .ok()
            .and_then(|r| r.value.as_f64())
            .map_or(k::REGRESSION_FINGERPRINTS.default as usize, usize_of);
        self.write(
            t,
            &mut out,
            fates,
            &Written {
                ctx: &ctx,
                rows: &rows,
                quals: &quals,
                target,
                regression_n,
            },
        )
        .await?;
        if let Some((name, reason)) = &out.retire {
            let d = IndexDecision {
                tenant: cx.tenant,
                kind: "index.retire",
                subject_type: "index",
                subject_id: format!("{object}/{name}"),
                mode: mode_text(policy.mode),
                outcome: "scheduled",
                reason_code: reason.to_string(),
                summary: format!("retire {name}: the table is over its automatic-index budget"),
                evidence: serde_json::json!({ "settings": policy.evidence() }),
                alternatives: None,
                at: t.now,
            };
            cx.decisions.record(&d).await?;
        }
        Ok(out)
    }

    /// Writes the candidate rows, the routed index rows and the decisions.
    #[allow(clippy::too_many_lines)]
    async fn write(
        &self,
        t: &Table<'_, '_>,
        out: &mut TableOutcome,
        fates: Vec<Fate>,
        w: &Written<'_, '_>,
    ) -> Result<()> {
        let cx = t.cx;
        let policy = t.policy;
        for mut f in fates {
            if let Some(r) = f.route {
                f.candidate
                    .evidence
                    .notes
                    .push(format!("route:{}", route_text(r)));
            }
            let triggers: Vec<serde_json::Value> = f
                .candidate
                .serves
                .iter()
                .filter_map(|(fp, _)| w.quals.get(fp).map(|q| trigger_json(q)))
                .collect();
            let row = CandidateRow {
                candidate_id: 0,
                run_id: t.run_id,
                object: t.object,
                def_hash: f.candidate.def.def_hash(),
                kind: StoredKind::from(f.candidate.def.kind),
                definition: Definition::of(&f.candidate.def),
                covering: f.candidate.covering,
                fingerprints: f.candidate.serves.clone(),
                status: f.status,
                reason: f.reason.clone(),
                benefit_ms_h: f.estimate.as_ref().map(|e| e.benefit_ms_h),
                write_ms_h: f.estimate.as_ref().map(|e| e.write_ms_h),
                est_bytes: f.estimate.as_ref().map(|e| u64_of(e.bytes)),
                build_seconds: f
                    .estimate
                    .as_ref()
                    .map(|e| u32::try_from(u64_of(e.build_seconds.ceil())).unwrap_or(u32::MAX)),
                evidence: f.candidate.evidence.clone(),
                created_at: t.now,
            };
            let candidate_id = cx.store.insert_candidate(&row).await?;
            metrics::counter!("sprig_advisor_autoindex_candidates_total",
                "status" => f.status.as_str())
            .increment(1);
            let name = f.candidate.def.name();
            let evidence = serde_json::json!({
                "object": t.object.0,
                "candidate_id": candidate_id,
                "definition": f.candidate.def.canonical(),
                "covering": f.candidate.covering,
                "serves": super::model::fingerprints_json(&f.candidate.serves),
                "selectivity": f.candidate.evidence.selectivity,
                "sel_source": f.candidate.evidence.sel_source,
                "entries_per_run": f.candidate.evidence.entries_per_run,
                "estimate": f.estimate.as_ref().map(estimate_json),
                "triggers": triggers,
                "route": f.route.map(route_text),
                "replaces": f.replaces,
                "settings": policy.evidence(),
            });
            let (kind, outcome, mode) = match (f.status, f.route) {
                (CandidateStatus::Selected, Some(Route::ValidateThenRollOut)) => {
                    ("index.create", "proposed", "applied")
                }
                (CandidateStatus::Selected | CandidateStatus::Recommended, _) => {
                    ("index.recommend", "proposed", "recommended")
                }
                (CandidateStatus::Rejected, _) => {
                    ("index.reject", "rejected", mode_text(policy.mode))
                }
                (CandidateStatus::Deferred, _) => {
                    ("index.defer", "deferred", mode_text(policy.mode))
                }
                _ => ("", "", ""),
            };
            let decision_id = if kind.is_empty() {
                None
            } else {
                let d = IndexDecision {
                    tenant: cx.tenant,
                    kind,
                    subject_type: "index",
                    subject_id: format!("{}/{name}", t.object),
                    mode,
                    outcome,
                    reason_code: f.reason.clone().unwrap_or_else(|| {
                        first_trigger(&f.candidate, w.quals).unwrap_or_else(|| "selected".into())
                    }),
                    summary: format!(
                        "{} {} on object {}: {}",
                        kind.trim_start_matches("index."),
                        f.candidate.def.canonical(),
                        t.object,
                        f.reason.as_deref().unwrap_or(f.status.as_str())
                    ),
                    evidence,
                    alternatives: None,
                    at: t.now,
                };
                cx.decisions.record(&d).await?
            };
            let mut outcome_row = CandidateOutcome {
                candidate_id,
                candidate: f.candidate.clone(),
                status: f.status,
                reason: f.reason.clone(),
                estimate: f.estimate.clone(),
            };
            let (Some(r), Some(est)) = (f.route, f.estimate.clone()) else {
                out.candidates.push(outcome_row);
                continue;
            };
            let Some(state) = r.first_state() else {
                out.candidates.push(outcome_row);
                continue;
            };
            let _ = record_churn(&f.candidate.def, w.rows, t.now);
            let baseline: BTreeMap<String, Baseline> = f
                .candidate
                .serves
                .iter()
                .filter_map(|(fp, _)| {
                    w.ctx.reads.get(fp).map(|(i, _)| {
                        (
                            fp.to_string(),
                            Baseline {
                                runs: i.runs,
                                p95_ms: i.p95_ms,
                                mean_ms: i.mean_ms,
                            },
                        )
                    })
                })
                .collect();
            let predictions: Vec<Prediction> = est
                .per_fp
                .iter()
                .filter(|e| e.role == Role::Primary)
                .map(|e| Prediction {
                    fingerprint: e.fp,
                    t_before_ms: e.t_ms,
                    t_after_ms: e.t_hat_ms,
                    rows_assumed: w
                        .ctx
                        .reads
                        .get(&e.fp)
                        .map_or(e.entries, |(i, _)| i.examined_per_run),
                })
                .collect();
            let mut others: Vec<(f64, Fingerprint)> = w
                .ctx
                .reads
                .iter()
                .filter(|(fp, _)| !f.candidate.serves_fp(**fp))
                .map(|(fp, (i, _))| (runs_f(i.runs) * i.mean_ms, *fp))
                .collect();
            others.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
            let regression_fps: Vec<Fingerprint> = others
                .into_iter()
                .take(w.regression_n)
                .map(|(_, f)| f)
                .collect();
            // Columns read by the served and the regression fingerprints:
            // the covering retry extends the candidate by those of the
            // statement that regressed, which is usually not one it serves
            // (worked example 3's F6).
            let columns_read: BTreeMap<Fingerprint, Vec<String>> = f
                .candidate
                .serves
                .iter()
                .map(|(fp, _)| *fp)
                .chain(regression_fps.iter().copied())
                .filter_map(|fp| {
                    w.ctx
                        .reads
                        .get(&fp)
                        .map(|(_, acc)| (fp, acc.reads.iter().cloned().collect()))
                })
                .collect();
            let plan = PlanInputs::new(&predictions, &columns_read, &regression_fps, &w.target);
            let mut observed = serde_json::json!({ "plan": plan });
            if r == Route::ValidateThenRecommend {
                observed["route"] = serde_json::json!("recommend");
            }
            let index = AutoIndexRow {
                index_id: super::model::AutoIndexId(0),
                object: t.object,
                name: name.clone(),
                model: match f.candidate.def.kind {
                    IndexKind::Vector => DataModel::Vector,
                    IndexKind::Spatial | IndexKind::S2 => DataModel::Geo,
                    _ if super::kv::is_kv(&f.candidate) => DataModel::Kv,
                    _ if super::doc::is_doc_candidate(&f.candidate) => DataModel::Document,
                    _ => DataModel::Sql,
                },
                kind: StoredKind::from(f.candidate.def.kind),
                definition: Definition::of(&f.candidate.def),
                state,
                prior_state: None,
                candidate_id,
                validation_id: None,
                decision_id,
                est_benefit_ms_h: est.benefit_ms_h,
                est_write_ms_h: est.write_ms_h,
                est_bytes: u64_of(est.bytes),
                actual_bytes: None,
                shards: BTreeMap::new(),
                baseline: baseline.clone(),
                // The loop's validation step reads this: `route` says an
                // accepted index stops at `recommended` (recommend mode, or
                // the fast trigger on a large table); `plan` is what its
                // validation plan needs besides samples ([`PlanInputs`]).
                observed: Some(observed),
                last_used_at: None,
                drop_reason: None,
                created_at: t.now,
                state_changed_at: t.now,
            };
            let Some(index_id) = cx.store.put_index(&index).await? else {
                outcome_row.status = CandidateStatus::Covered;
                outcome_row.reason = Some("exists:concurrent".into());
                out.candidates.push(outcome_row);
                continue;
            };
            out.queued.push(Queued {
                object: t.object,
                candidate_id,
                index_id,
                route: r,
                candidate: f.candidate,
                estimate: est,
                predictions,
                columns_read,
                regression_fps,
                replaces: f.replaces,
                target: w.target.clone(),
                baseline,
                advise: f.advise,
            });
            out.candidates.push(outcome_row);
        }
        Ok(())
    }
}

/// One table's evaluation inputs.
struct Table<'a, 'c> {
    cx: &'a EvalCx<'c>,
    q: &'a Qualification,
    object: ObjectId,
    meta: &'a ObjectMeta,
    policy: &'a ObjectPolicy,
    window_hours: u32,
    run_id: u64,
    now: SystemTime,
    decided: &'a [CandidateRow],
    caps: &'a DayCaps,
}

/// What writing needs from the table's pass.
struct Written<'a, 'b> {
    ctx: &'a TableContext<'b>,
    rows: &'a [AutoIndexRow],
    quals: &'a BTreeMap<Fingerprint, &'a Qualified>,
    target: IndexTarget,
    regression_n: usize,
}

/// Records why a fingerprint got no candidate on a table: a
/// [`CandidateRow::skipped`] row (what `sprigctl explain query` reads) and
/// a decision.
/// `entries` with each fingerprint of `cut` replaced by its runs since
/// its time there (the build of the index serving it).
///
/// # Errors
/// The workload store's.
/// Moves shapes that read chapter 33's views onto the element tables
/// (`super::kv::translate`, R6 round 5).
fn kv_views(cx: &EvalCx<'_>, entries: &mut [super::qualify::SourcedWindow]) {
    for e in entries.iter_mut() {
        if let Some(s) = super::kv::translate(&e.window.shape, &|o| cx.objects.kv_view(o)) {
            e.window.shape = Arc::new(s);
        }
    }
}

async fn since_served(
    cx: &EvalCx<'_>,
    entries: Vec<super::qualify::SourcedWindow>,
    cut: &BTreeMap<Fingerprint, SystemTime>,
    now: SystemTime,
) -> Result<Vec<super::qualify::SourcedWindow>> {
    let mut out: Vec<super::qualify::SourcedWindow> = entries
        .into_iter()
        .filter(|e| !cut.contains_key(&e.window.id))
        .collect();
    let times: BTreeSet<SystemTime> = cut.values().copied().collect();
    for t in times {
        let later = cx.workload.window(cx.tenant, t, now).await?;
        out.extend(
            later
                .into_iter()
                .filter(|e| cut.get(&e.window.id) == Some(&t)),
        );
    }
    Ok(out)
}

async fn record_skip(
    cx: &EvalCx<'_>,
    run_id: u64,
    now: SystemTime,
    mode: Mode,
    s: &Skipped,
    trigger: Option<serde_json::Value>,
) -> Result<()> {
    let Some(object) = s.object else {
        return Ok(());
    };
    let notes = if s.alias.is_empty() {
        Vec::new()
    } else {
        vec![format!("alias:{}", s.alias)]
    };
    let row = CandidateRow::skipped(run_id, object, s.fp, &s.reason, notes, now);
    cx.store.insert_candidate(&row).await?;
    metrics::counter!("sprig_advisor_autoindex_candidates_total", "status" => "skipped")
        .increment(1);
    let d = IndexDecision {
        tenant: cx.tenant,
        kind: "index.reject",
        subject_type: "fingerprint",
        subject_id: s.fp.to_string(),
        mode: mode_text(mode),
        outcome: "no_candidate",
        reason_code: s.reason.clone(),
        summary: format!("no candidate on object {object}: {}", s.reason),
        evidence: serde_json::json!({
            "object": object.0,
            "trigger": trigger,
        }),
        alternatives: None,
        at: now,
    };
    cx.decisions.record(&d).await?;
    Ok(())
}

/// Chapter 13's merge, with each covering variant costed beside its base
/// candidate, not instead of it ("Covering columns": `emit c' := c ++
/// need ... costed beside c`). `candidates::merge` alone would fold a base
/// into its own covering variant (the base's parts are a prefix of it), so
/// worked example 1's B `(customer_id, created_at)` would only be priced
/// with `status` and `total` added. Base candidates merge among
/// themselves; covering variants are kept as generated, once each.
#[must_use]
pub fn merge_beside_covering(cands: Vec<Candidate>) -> Vec<Candidate> {
    use super::model::PartSource;
    let (cover, base): (Vec<Candidate>, Vec<Candidate>) = cands
        .into_iter()
        .partition(|c| c.evidence.parts.contains(&PartSource::Cover));
    let mut out = merge(base);
    for c in merge(cover) {
        if !out.iter().any(|d| d.def == c.def) {
            out.push(c);
        }
    }
    out
}

fn skip(fp: Fingerprint, object: ObjectId, reason: &str) -> Skipped {
    Skipped {
        fp,
        alias: String::new(),
        object: Some(object),
        reason: reason.to_owned(),
    }
}

/// The columns statistics are wanted for (predicate and join columns of
/// the qualified fingerprints on `object`), and those that want a
/// histogram (ranges, `IN` lists, prefixes, and equalities with samples).
fn wanted_columns(quals: &[&FpInput], object: ObjectId) -> (Vec<String>, Vec<String>) {
    let mut cols: BTreeSet<String> = BTreeSet::new();
    let mut hist: BTreeSet<String> = BTreeSet::new();
    for f in quals {
        let aliases: Vec<&str> = f
            .shape
            .tables
            .iter()
            .filter(|t| t.object_id == Some(object))
            .map(|t| t.alias.as_str())
            .collect();
        let on = |t: &Option<String>| t.as_deref().is_some_and(|a| aliases.contains(&a));
        for p in f.shape.predicates.iter().filter(|p| on(&p.column.table)) {
            if !p.op.is_sargable() {
                continue;
            }
            let c = p.column.col.to_ascii_lowercase();
            let wants = match p.op {
                PredOp::Range { .. } | PredOp::In { .. } | PredOp::Prefix => true,
                PredOp::Eq => !f.samples.is_empty(),
                _ => false,
            };
            if wants {
                hist.insert(c.clone());
            }
            cols.insert(c);
        }
        for j in &f.shape.joins {
            for c in [&j.l, &j.r] {
                if on(&c.table) {
                    cols.insert(c.col.to_ascii_lowercase());
                }
            }
        }
    }
    (cols.into_iter().collect(), hist.into_iter().collect())
}

/// An automatic index row as selection sees it.
fn on_table(r: &AutoIndexRow) -> AutoOnTable {
    #[allow(clippy::cast_precision_loss)] // bytes far below 2^52
    let bytes = r.actual_bytes.unwrap_or(r.est_bytes) as f64;
    let measured = r
        .observed
        .as_ref()
        .and_then(|o| o.get("value_ms_h"))
        .and_then(serde_json::Value::as_f64);
    AutoOnTable {
        name: r.name.clone(),
        info: IndexInfo {
            name: r.name.clone(),
            parts: r.definition.parts.clone(),
            unique: false,
            origin: Origin::Auto,
            ignored: r.state == IndexState::Retiring,
            usable: matches!(r.state, IndexState::Observing | IndexState::Active),
        },
        bytes,
        value_ms_h: measured.unwrap_or(r.est_benefit_ms_h - r.est_write_ms_h),
        active: r.state == IndexState::Active,
    }
}

fn trigger_json(q: &Qualified) -> serde_json::Value {
    serde_json::json!({
        "fp": q.fp.to_string(),
        "trigger": q.trigger.as_str(),
        "fired": q.fired.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
        "advise": q.advise,
        "rank": q.rank,
        "runs": q.signals.runs,
        "p95_ms": q.signals.p95_ms,
        "total_ms": q.signals.total_ms,
        "examined_per_returned": q.signals.examined_per_returned,
        "scan_runs": q.signals.scan_runs,
        "scan_sessions": q.signals.scan_sessions,
    })
}

fn estimate_json(e: &Estimate) -> serde_json::Value {
    serde_json::json!({
        "benefit_ms_h": e.benefit_ms_h,
        "write_ms_h": e.write_ms_h,
        "bytes": e.bytes,
        "build_seconds": e.build_seconds,
        "per_fp": e.per_fp.iter().map(|f| serde_json::json!({
            "fp": f.fp.to_string(),
            "role": match f.role { Role::Primary => "primary", Role::Merged => "merged" },
            "per_hour": f.per_hour,
            "t_ms": f.t_ms,
            "t_hat_ms": f.t_hat_ms,
            "entries": f.entries,
        })).collect::<Vec<_>>(),
        "failed": e.failed.iter().map(super::cost::Gate::reason).collect::<Vec<_>>(),
    })
}

fn first_trigger(c: &Candidate, quals: &BTreeMap<Fingerprint, &Qualified>) -> Option<String> {
    c.primaries()
        .find_map(|f| quals.get(&f).map(|q| q.trigger.as_str().to_owned()))
}

/// The route's name in evidence.
#[must_use]
pub const fn route_text(r: Route) -> &'static str {
    match r {
        Route::Skip => "skip",
        Route::ValidateThenRollOut => "validate_then_roll_out",
        Route::ValidateThenRecommend => "validate_then_recommend",
        Route::Recommend => "recommend",
    }
}

const fn mode_text(m: Mode) -> &'static str {
    match m {
        Mode::On => "applied",
        Mode::Recommend | Mode::Approve | Mode::Off => "recommended",
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn u64_of(x: f64) -> u64 {
    if x.is_finite() && x > 0.0 {
        x.min(u64::MAX as f64) as u64
    } else {
        0
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn usize_of(x: f64) -> usize {
    if x.is_finite() && x > 0.0 {
        x.min(1e9) as usize
    } else {
        0
    }
}

#[allow(clippy::cast_precision_loss)]
fn runs_f(n: u64) -> f64 {
    n as f64
}
