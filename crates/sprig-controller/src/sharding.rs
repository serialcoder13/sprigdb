//! The sharding loop: per tenant, every
//! `sharding.check_interval_seconds`,
//!
//! 1. before the tenant's first split (its default-group schemas together
//!    above `sharding.split_size_gb`), run the advisor and apply its first
//!    design: table groups, `_sprig_rk`, active groups;
//! 2. for each active group, measure its shards, evaluate the split and
//!    merge triggers, choose split points from index-dive histograms, admit
//!    proposals against running jobs (limits, locks, backoff) and submit
//!    hash-range mover jobs;
//! 3. follow finished jobs: decision outcomes, failure backoff, shard ages
//!    for the merge cooldown.
//!
//! Every proposal and advisor run is a decision in the tenant's log.

mod graphs;
mod ops;
mod rekey;
pub use graphs::{GraphIndexRow, GraphPartition, GraphRow, GraphShardRow, GraphView};
pub use ops::{GroupView, ManualJob, ShardRow, design_from_json};
pub use rekey::RekeyRequest;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use mysql_async::{Conn, Pool};
use serde_json::json;
use sprig_advisor::sharding::apply::{ApplyCtx, apply_first_design};
use sprig_advisor::sharding::design::{self, Cause, Evidence, Verdict};
use sprig_advisor::sharding::{AdvisorMode, AdvisorSettings, input, runs};
use sprig_balancer::decisions::{DecisionLog, Mode, NewDecision, Outcome};
use sprig_balancer::shards::{
    self, Admission, Backoff, GroupStat, Proposal, Reason, RunningJob, ShardJob, ShardStat,
    ShardingMode, ShardingSettings, Weight,
};
use sprig_common::ids::{ClusterId, DbId, GroupId, JobId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_mover::{JobState, JobStore, MetaMover, Mover};
use sprig_shard::shardmap::RangeState;
use sprig_shard::{Placement, ShardMapStore, TenantMaps, shard_schema};

/// Connections to clusters by id.
#[async_trait::async_trait]
pub trait ClusterPools: Send + Sync {
    /// A pool of `sprig_ctl` connections to `cluster`'s writer.
    ///
    /// # Errors
    /// Unknown or unreachable clusters.
    async fn pool(&self, cluster: &ClusterId) -> Result<Pool>;
}

/// The settings the loop needs for one tenant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TenantSettings {
    /// `sharding.*`.
    pub sharding: ShardingSettings,
    /// `advisor.*`.
    pub advisor: AdvisorSettings,
    /// `mover.cutover_max_hold_ms`.
    pub max_hold: Duration,
    /// `mover.txn_grace_ms`.
    pub txn_grace: Duration,
    /// `mover.hold_grace_ms`.
    pub hold_grace: Duration,
    /// `mover.copy_chunk_rows`.
    pub chunk_rows: usize,
}

impl Default for TenantSettings {
    fn default() -> Self {
        let m = sprig_mover::MoverSettings::default();
        Self {
            sharding: ShardingSettings::default(),
            advisor: AdvisorSettings::default(),
            max_hold: m.cutover_max_hold,
            txn_grace: m.txn_grace,
            hold_grace: m.hold_grace,
            chunk_rows: m.copy_chunk_rows,
        }
    }
}

/// Where a tenant's settings come from.
pub trait SettingsSource: Send + Sync {
    /// The settings of `tenant`, now.
    fn settings(&self, tenant: TenantId) -> TenantSettings;

    /// `sharding.min_shards` for database `db` of `tenant` (the key has
    /// database scope). The default is the tenant's value.
    fn min_shards(&self, tenant: TenantId, _db: DbId) -> u32 {
        self.settings(tenant).sharding.min_shards
    }
}

/// When a shard was born, for the merge cooldown: the commit of the job
/// that made it, else its group's creation. A presplit shard (made with its group by the advisor's apply
/// or by `CREATE GRAPH`) has no job; before, it counted as old, and the
/// loop merged a presplit 4-shard group within minutes.
#[must_use]
pub fn shard_birth(job_born: Option<u64>, group_created_ms: Option<u64>) -> Option<u64> {
    job_born.or(group_created_ms)
}

/// The `min_shards` a group's triggers use: the group's own value
/// (`table_groups.min_shards`, which overrides the setting), else the
/// largest `sharding.min_shards` among its databases.
#[must_use]
pub fn group_min_shards(own: Option<u32>, per_db: impl IntoIterator<Item = u32>) -> Option<u32> {
    own.or_else(|| per_db.into_iter().max())
}

/// Write load per shard, from the gateways' write sketches: what load splits need. Without one, `write_shares`
/// stay empty and only size splits, `min_shards` and merges fire.
pub trait WriteLoad: Send + Sync {
    /// The shard's share of its cluster's writes per minute, oldest first,
    /// for minutes after `since_ms` (its last cutover) only.
    fn write_shares(
        &self,
        tenant: TenantId,
        group: GroupId,
        shard: ShardId,
        since_ms: Option<u64>,
    ) -> Vec<f64>;
    /// Whether placement's load model puts `cluster` above its high
    /// watermark.
    fn cluster_hot(&self, cluster: &ClusterId) -> bool;
    /// The shard's write histogram over `[lo, hi)` for the last `window`,
    /// with heavy hashes; `None` without enough data.
    fn write_histogram(
        &self,
        tenant: TenantId,
        group: GroupId,
        shard: ShardId,
        lo: u64,
        hi: Option<u64>,
        window: Duration,
    ) -> Option<shards::HashHistogram>;
}

/// Fixed settings (tests; a controller without a settings store).
#[derive(Debug, Clone, Default)]
pub struct FixedSettings(pub Arc<Mutex<TenantSettings>>);

impl SettingsSource for FixedSettings {
    fn settings(&self, _tenant: TenantId) -> TenantSettings {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What one tick did, for tests and logs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickReport {
    /// An advisor run: its id and verdict.
    pub advisor: Option<(u64, &'static str)>,
    /// The first design was applied: the regroup's hold, ms.
    pub applied_hold_ms: Option<u64>,
    /// Jobs submitted: kind, reason, job id.
    pub submitted: Vec<(String, &'static str, JobId)>,
    /// Proposals that wait, with what blocks them.
    pub waiting: Vec<(String, &'static str)>,
}

#[derive(Debug, Default)]
struct TenantState {
    /// When the first split became due (for `advisor.max_wait_hours`).
    first_due: Option<Instant>,
    /// The last advisor run (runs are at most hourly while waiting).
    last_run: Option<Instant>,
    /// Waits already logged, by subject and blocker.
    logged_waits: BTreeSet<(String, &'static str)>,
}

/// A `group_members` row: object, routing, key columns, parent, parent FK.
type MemberRow = (u64, String, Option<String>, Option<u64>, Option<String>);

/// The loop.
pub struct ShardingLoop {
    jobs: JobStore,
    mover: MetaMover,
    maps: ShardMapStore,
    tenant_maps: TenantMaps,
    pools: Arc<dyn ClusterPools>,
    decisions: Arc<dyn DecisionLog>,
    settings: Arc<dyn SettingsSource>,
    actor: String,
    dc: String,
    backoff: Mutex<Backoff>,
    state: Mutex<HashMap<TenantId, TenantState>>,
    /// Finished jobs already followed.
    followed: Mutex<BTreeSet<JobId>>,
    /// How often a waiting advisor re-runs.
    pub advisor_retry: Duration,
    /// The global tier's directory, for the tenant's state (a tenant moving
    /// between DCs gets no shard work); `None` in tests without one.
    directory: Option<Arc<dyn sprig_meta::MetaStore>>,
    /// The gateways' write sketches; `None`: no load splits.
    write_load: Option<Arc<dyn WriteLoad>>,
    /// Publishes the advisor's catalog changes to gateways.
    publish: Option<Arc<dyn sprig_advisor::sharding::apply::Publish>>,
    /// Tenants whose table groups this loop published since it started.
    published: Mutex<BTreeSet<TenantId>>,
    /// `sharding.mode = approve` holds jobs for approval.
    pub approvals: std::sync::OnceLock<Arc<crate::approvals::ControllerApprovals>>,
}

impl std::fmt::Debug for ShardingLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardingLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

fn now_ms() -> u64 {
    u64::try_from(now_us() / 1000).unwrap_or(0)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ShardingLoop {
    /// A loop over the DC tier `dc` (jobs, maps), reaching clusters through
    /// `pools`, logging to `decisions`.
    #[must_use]
    pub fn new(
        dc: Arc<dyn sprig_meta::MetaStore>,
        clock: Arc<dyn sprig_common::time::Clock>,
        pools: Arc<dyn ClusterPools>,
        decisions: Arc<dyn DecisionLog>,
        settings: Arc<dyn SettingsSource>,
        actor: &str,
    ) -> Self {
        let jobs = JobStore::new(dc.clone(), clock);
        let dc_name = match dc.tier() {
            sprig_meta::Tier::Dc(d) => d.to_string(),
            sprig_meta::Tier::Global => "global".to_owned(),
        };
        Self {
            mover: MetaMover::new(jobs.clone()),
            jobs,
            maps: ShardMapStore::new(dc.clone()),
            tenant_maps: TenantMaps::new(dc),
            pools,
            decisions,
            settings,
            actor: actor.to_owned(),
            dc: dc_name,
            backoff: Mutex::new(Backoff::default()),
            state: Mutex::new(HashMap::new()),
            followed: Mutex::new(BTreeSet::new()),
            advisor_retry: Duration::from_secs(3600),
            directory: None,
            write_load: None,
            publish: None,
            published: Mutex::new(BTreeSet::new()),
            approvals: std::sync::OnceLock::new(),
        }
    }

    /// The approval gate for a split, merge or move proposal: `Ok` to go
    /// ahead (with the ticket to report the submission with, when an item
    /// tracks it), `Err(why)` to hold. Only under `approve` and
    /// `recommend` with an approvals hub.
    async fn approval_gate(
        &self,
        tenant: TenantId,
        p: &shards::Proposal,
        subject: &str,
        mode: ShardingMode,
    ) -> std::result::Result<Option<sprig_approvals::Ticket>, String> {
        use sprig_approvals::{Action, Kind, Proposal, Verdict};
        let Some(a) = self.approvals.get() else {
            return Ok(None);
        };
        if !matches!(mode, ShardingMode::Approve | ShardingMode::Recommend) {
            return Ok(None);
        }
        let kind = match p.job.kind() {
            "split" => Kind::ShardSplit,
            "merge" => Kind::ShardMerge,
            _ => Kind::ShardMove,
        };
        let policy = a.policy(kind, Some(tenant), None, None, None).await;
        let score = p
            .evidence
            .get("size_bytes")
            .or_else(|| p.evidence.get("bytes"))
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(1.0);
        // The item's identity leaves out a split's points: they are chosen
        // from index-dive samples and differ from one proposal to the next,
        // which made a fresh proposal supersede the approved item instead
        // of running it. They go in the evidence.
        let mut job = p.job.clone();
        let mut evidence = p.evidence.clone();
        if let shards::ShardJob::Split { points, .. } = &mut job {
            if !points.is_empty()
                && let serde_json::Value::Object(o) = &mut evidence
            {
                o.insert("split_points".into(), serde_json::json!(points));
            }
            points.clear();
        }
        let gp = Proposal {
            kind,
            subject: a.subject(tenant).await,
            target: format!("t{tenant}/g{}/{subject}", p.group),
            action: Action {
                text: format!("sprigctl shard {} {subject}", p.job.kind()),
                machine: serde_json::to_value(&job).unwrap_or_default(),
            },
            evidence,
            score,
            summary: format!("{} of {subject} ({:?})", p.job.kind(), p.reason),
            emergency: false,
        };
        match a.gate(&gp, &policy).await {
            Verdict::Proceed(t) => Ok(t.id.is_some().then_some(t)),
            Verdict::Wait(id) => Err(format!("held for approval as {id}")),
            Verdict::Advice(id) => Err(format!("recommended as {id}")),
            Verdict::Suppressed(id) => Err(format!("ignored ({id})")),
            Verdict::Off => Err("approvals unavailable".into()),
        }
    }

    /// Reports a shard job an approval let through.
    async fn approval_done(
        &self,
        ticket: Option<&sprig_approvals::Ticket>,
        ok: bool,
        detail: &str,
    ) {
        if let (Some(t), Some(a)) = (ticket, self.approvals.get()) {
            a.finished(t, ok, detail).await;
        }
    }

    /// Publishes the advisor's catalog changes with `p`.
    pub fn set_publish(&mut self, p: Arc<dyn sprig_advisor::sharding::apply::Publish>) {
        self.publish = Some(p);
    }

    /// The same loop with load splits fed by `w`.
    #[must_use]
    pub fn with_write_load(mut self, w: Arc<dyn WriteLoad>) -> Self {
        self.write_load = Some(w);
        self
    }

    /// The same loop reading tenants' states from the directory in
    /// `global` (a tenant `migrating` between DCs blocks shard work).
    #[must_use]
    pub fn with_directory(mut self, global: Arc<dyn sprig_meta::MetaStore>) -> Self {
        self.directory = Some(global);
        self
    }

    /// The holder text of the advisor's tenant lock (see
    /// [`JobStore::lock_tenant`]).
    fn advisor_holder(tenant: TenantId) -> String {
        format!("advisor:t{tenant}")
    }

    /// Whether `tenant` is `active` in the directory (always, without one).
    ///
    /// # Errors
    /// The directory's errors.
    pub async fn tenant_active(&self, tenant: TenantId) -> Result<bool> {
        let Some(g) = &self.directory else {
            return Ok(true);
        };
        Ok(sprig_shard::home::read_home(&**g, tenant)
            .await?
            .is_some_and(|e| e.state == "active"))
    }

    /// Runs ticks for `tenants()` until `stop`.
    pub async fn run(
        &self,
        tenants: Arc<dyn Fn() -> Vec<TenantId> + Send + Sync>,
        stop: CancellationToken,
    ) {
        loop {
            let mut interval = Duration::from_secs(60);
            for t in tenants() {
                interval = interval.min(self.settings.settings(t).sharding.check_interval);
                if let Err(e) = self.tick(t).await {
                    tracing::warn!(tenant = %t, error = %e, "sharding tick failed");
                }
            }
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
        }
    }

    async fn record(&self, tenant: TenantId, d: &NewDecision) -> u64 {
        match self.decisions.record(tenant, d).await {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(tenant = %tenant, error = %e, "decision not recorded");
                0
            }
        }
    }

    /// Follows finished jobs of `tenant`: outcomes, backoff, shard ages.
    async fn follow(
        &self,
        tenant: TenantId,
    ) -> Result<(Vec<RunningJob>, BTreeMap<(u32, u32), u64>)> {
        let mut running = Vec::new();
        let mut born: BTreeMap<(u32, u32), u64> = BTreeMap::new();
        for (rec, _) in self.jobs.all().await? {
            let r = RunningJob::of(&rec);
            // A committed job only keeps its retired sources fenced
            // (`sharding.retire_hours`): its new shards serve and may split
            // or merge again, and it copies nothing more.
            if !rec.state.is_terminal() && !rec.state.is_committed() {
                running.push(r);
                continue;
            }
            if rec.tenant_id != tenant {
                continue;
            }
            let Some(g) = rec.spec.group() else { continue };
            if rec.state.is_committed() {
                for t in &r.targets {
                    let e = born.entry((g, *t)).or_insert(rec.updated_ms);
                    *e = (*e).min(rec.updated_ms);
                }
            }
            if !rec.state.is_terminal() || !lock(&self.followed).insert(rec.job_id) {
                continue;
            }
            let ok = rec.state == JobState::Done;
            if !ok {
                lock(&self.backoff).failed(tenant, g, &r.sources, now_ms());
            }
            if let Some(d) = rec.decision_id {
                let o = if ok {
                    Outcome::Succeeded
                } else {
                    Outcome::Failed
                };
                let _ = self.decisions.update_outcome(tenant, d, o, now_us()).await;
            }
            metrics::counter!(
                "sprig_balancer_shard_jobs_total",
                "kind" => rec.kind.as_str(),
                "outcome" => if ok { "done" } else { "rolled_back" }
            )
            .increment(1);
        }
        // Running jobs' targets also count as born now (they aren't in the map yet).
        Ok((running, born))
    }

    async fn conn(&self, cluster: &ClusterId) -> Result<Conn> {
        let pool = self.pools.pool(cluster).await?;
        pool.get_conn().await.map_err(|e| {
            SprigError::new(sprig_common::SprigCode::BackendUnavailable, e.to_string())
        })
    }

    /// One evaluation of `tenant`.
    ///
    /// # Errors
    /// Store and cluster errors; the next tick retries.
    pub async fn tick(&self, tenant: TenantId) -> Result<TickReport> {
        let ts = self.settings.settings(tenant);
        let mut report = TickReport::default();
        if ts.sharding.mode == ShardingMode::Off {
            return Ok(report);
        }
        if !self.tenant_active(tenant).await? {
            // Moving between DCs: no shard work until it ends.
            report.waiting.push((format!("t{tenant}"), "tenant_move"));
            return Ok(report);
        }
        let (tm, _) = self.tenant_maps.load(tenant).await?;
        let home = Placement::of(&tm)?.cluster;
        let mut c = self.conn(&home).await?;
        let (mut running, born) = self.follow(tenant).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        sprig_shard::groups::migrate(&mut c, &cat).await?;
        // `created_at` is UTC (every writer uses UTC_TIMESTAMP): as epoch
        // milliseconds whatever the session's time zone.
        let groups: Vec<(u32, String, Option<u32>, Option<u64>)> =
            mysql_async::prelude::Queryable::query(
                &mut c,
                format!(
                    "SELECT group_id, state, min_shards, \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at) DIV 1000 \
                     FROM `{cat}`.table_groups WHERE kind = 'sharded' ORDER BY group_id"
                ),
            )
            .await
            .map_err(|e| SprigError::internal(format!("table groups: {e}")))?;
        if groups.is_empty() {
            self.first_design(tenant, &ts, &home, &mut c, &mut report, false)
                .await?;
            return Ok(report);
        }
        if groups.iter().any(|g| g.1 == "preparing") {
            let holder = Self::advisor_holder(tenant);
            if let Some(h) = self.jobs.lock_tenant(tenant, &holder).await? {
                report.waiting.push((format!("t{tenant}"), "tenant_job"));
                tracing::info!(tenant = %tenant, holder = %h, "first design waits for the tenant's job");
                return Ok(report);
            }
            let r = self
                .resume_design(tenant, &ts, &home, &mut c, &mut report)
                .await;
            self.jobs.unlock_tenant(tenant, &holder).await?;
            r?;
            return Ok(report);
        }
        // Groups applied before this loop started (or by an older binary)
        // are published once, so every gateway routes them.
        if let Some(p) = &self.publish
            && !lock(&self.published).contains(&tenant)
        {
            p.catalog_changed(tenant).await?;
            lock(&self.published).insert(tenant);
        }
        // Online re-keys: finish the catalog of a committed
        // one, undo one that ended without a cutover.
        if groups.iter().any(|g| g.1 == "rekeying") {
            self.rekey_follow(tenant).await?;
        }
        // An apply that ended before releasing the advisor's lock (a crash
        // between the two) would keep every split out: release it.
        self.jobs
            .unlock_tenant(tenant, &Self::advisor_holder(tenant))
            .await?;
        for (g, state, min_shards, created_ms) in groups {
            if state != "active" {
                continue;
            }
            self.group_tick(
                tenant,
                GroupId(g),
                (min_shards, created_ms),
                &ts,
                &born,
                &mut running,
                &mut report,
            )
            .await?;
        }
        Ok(report)
    }

    /// What the key-value rule needs beyond the input: each unit's
    /// database's `sharding.min_shards`, and whether its shard (the
    /// database's default group, shard 1) is write-hot by the load-split
    /// rule.
    fn kv_rule_inputs(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
        asm: &mut input::Assembled,
    ) {
        asm.input.split_write_share = ts.sharding.split_write_share;
        let sustain = usize::try_from(ts.sharding.sustain.as_secs() / 60)
            .unwrap_or(usize::MAX)
            .max(1);
        let hot = self
            .write_load
            .as_ref()
            .is_some_and(|w| w.cluster_hot(home));
        for u in &mut asm.input.kv {
            u.min_shards = self.settings.min_shards(tenant, DbId(u.db));
            let at = asm
                .located
                .iter()
                .find(|l| l.object == u.key)
                .and_then(|l| l.schema.parse::<SchemaName>().ok());
            u.write_hot = match (at, self.write_load.as_ref()) {
                (Some(SchemaName::Shard { group, shard, .. }), Some(w)) if hot => {
                    let v = w.write_shares(tenant, group, shard, None);
                    v.len() >= sustain
                        && v[v.len() - sustain..]
                            .iter()
                            .all(|x| *x >= ts.sharding.split_write_share)
                }
                _ => false,
            };
        }
    }

    /// Step 1: the advisor's first design when the first split is due.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the advisor's run, step by step
    async fn first_design(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
        c: &mut Conn,
        report: &mut TickReport,
        manual: bool,
    ) -> Result<()> {
        let now = SystemTime::now();
        // The whole input only when the first split can be
        // due; most tenants are far below the split size.
        if !manual
            && ts.sharding.min_shards <= 1
            && input::first_split_bound(c, tenant).await? <= ts.sharding.split_bytes
        {
            return Ok(());
        }
        let mut asm =
            input::assemble(c, tenant, now, ts.advisor.window, ts.sharding.split_bytes).await?;
        self.kv_rule_inputs(tenant, ts, home, &mut asm);
        let total: f64 = asm.input.tables.iter().map(|t| t.size_bytes).sum();
        let due = total > ts.sharding.split_bytes || ts.sharding.min_shards > 1;
        if !due && !manual {
            return Ok(());
        }
        if let Some(h) = self.jobs.tenant_lock_holder(tenant).await?
            && h != Self::advisor_holder(tenant)
        {
            // A whole-tenant job (a move, a copy-and-swap) runs: the first
            // design waits for it (mutual exclusion).
            report.waiting.push((format!("t{tenant}"), "tenant_job"));
            return Ok(());
        }
        let (waited, recent) = {
            let mut st = lock(&self.state);
            let e = st.entry(tenant).or_default();
            let first = *e.first_due.get_or_insert_with(Instant::now);
            (
                first.elapsed(),
                e.last_run.is_some_and(|r| r.elapsed() < self.advisor_retry),
            )
        };
        if recent && !manual {
            return Ok(());
        }
        lock(&self.state).entry(tenant).or_default().last_run = Some(Instant::now());
        let cause = if manual {
            Cause::Manual
        } else if total > ts.sharding.split_bytes {
            Cause::FirstSplit
        } else {
            Cause::MinShards
        };
        let s = ts.advisor;
        // A trigger's table and the tables its body
        // names land in one group or stay; `settle` holds split couples
        // and finds the design again.
        let found = sprig_advisor::sharding::code::settle(&mut asm.input, |input| {
            if s.mode == AdvisorMode::Off {
                let (d, evidence) = design::structural(input, &s);
                let score = design::score(input, &d, &s);
                design::SearchResult {
                    best: d,
                    score,
                    path: Vec::new(),
                    alternatives: Vec::new(),
                    evidence,
                }
            } else {
                design::search(input, &s)
            }
        });
        let runner = found.alternatives.first();
        let confidence = design::confidence(&asm.input, &found.best, runner.map(|(d, _)| d), &s);
        let observed = asm
            .first_seen
            .and_then(|f| now.duration_since(f).ok())
            .unwrap_or_default();
        let ev = Evidence {
            observed,
            units: asm.input.units_sampled(),
            confidence,
            waited,
        };
        let verdict = design::decide(cause, &ev, &s);
        let cat = SchemaName::catalog(tenant).to_string();
        runs::migrate(c, &cat).await?;
        let window_start = now.checked_sub(s.window).unwrap_or(SystemTime::UNIX_EPOCH);
        let us = |t: SystemTime| {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(0))
        };
        let evidence = json!({
            "notes": found.evidence,
            "moves": found.path,
            "score": found.score,
            "observed_hours": observed.as_secs_f64() / 3600.0,
            "waited_hours": waited.as_secs_f64() / 3600.0,
            "low_confidence": verdict == Verdict::ApplyLowConfidence,
            "data_bytes": total,
        });
        let run = runs::insert(
            c,
            &cat,
            &runs::RunRecord {
                cause: cause.as_str(),
                mode: match s.mode {
                    AdvisorMode::On => "on",
                    // `advisor_runs.mode` has no `approve`: held runs are
                    // recommendations until approved.
                    AdvisorMode::Recommend | AdvisorMode::Approve => "recommend",
                    AdvisorMode::Off => "off",
                },
                window_us: (us(window_start), us(now)),
                schema_version: asm.schema_version,
                units: ev.units,
                best: found.score.total,
                runner_up: runner.map(|(_, sc)| sc.total),
                confidence,
                verdict: verdict.as_str(),
                design: found.best.to_json(&asm.input),
                alternatives: json!(
                    found
                        .alternatives
                        .iter()
                        .map(|(d, sc)| json!({"design": d.to_json(&asm.input), "score": sc}))
                        .collect::<Vec<_>>()
                ),
                evidence: evidence.clone(),
            },
        )
        .await?;
        report.advisor = Some((run, verdict.as_str()));
        let applying = matches!(verdict, Verdict::Apply | Verdict::ApplyLowConfidence);
        let d = self
            .record(
                tenant,
                &NewDecision {
                    decided_at_us: now_us(),
                    component: "advisor".into(),
                    kind: "group.design".into(),
                    subject_type: "tenant".into(),
                    subject_id: format!("t{tenant}"),
                    mode: if applying {
                        Mode::Applied
                    } else {
                        Mode::Recommended
                    },
                    outcome: if applying {
                        Outcome::Running
                    } else {
                        Outcome::Proposed
                    },
                    reason_code: cause.as_str().into(),
                    summary: format!(
                        "advisor run {run}: {} groups, score {:.3}, confidence {:.2}, verdict {}",
                        found.best.groups.len(),
                        found.score.total,
                        confidence,
                        verdict.as_str()
                    ),
                    evidence,
                    alternatives: None,
                    job_id: None,
                    parent_id: None,
                    actor: self.actor.clone(),
                    dc_id: self.dc.clone(),
                },
            )
            .await;
        if d > 0 {
            runs::set_decision(c, &cat, run, d).await?;
        }
        if verdict == Verdict::Recommend
            && let Some(a) = self.approvals.get()
        {
            // The run's design is the proposal; the advisor
            // runs again every `advisor_retry`, so an approved design is
            // applied from a fresh run that still finds it.
            use sprig_approvals::{Action, Kind, Proposal, Verdict as Gate};
            let policy = a
                .policy(Kind::AdvisorDesign, Some(tenant), None, None, None)
                .await;
            let design = found.best.to_json(&asm.input);
            let p = Proposal {
                kind: Kind::AdvisorDesign,
                subject: a.subject(tenant).await,
                target: format!("t{tenant}/design"),
                action: Action {
                    text: format!("sprigctl sharding advisor apply --tenant {tenant} --run {run}"),
                    machine: design,
                },
                evidence: json!({
                    "run": run,
                    "groups": found.best.groups.len(),
                    "score": found.score.total,
                    "confidence": confidence,
                }),
                score: found.score.total.max(0.0),
                summary: format!(
                    "sharding design: {} table groups (score {:.3}, confidence {:.2})",
                    found.best.groups.len(),
                    found.score.total,
                    confidence
                ),
                emergency: false,
            };
            // Under `recommend` the run is advice; an approval in the
            // inbox still applies it.
            if matches!(s.mode, AdvisorMode::Approve | AdvisorMode::Recommend)
                && let Gate::Proceed(t) = a.gate(&p, &policy).await
            {
                let r = self
                    .apply_run(tenant, ts, home, c, run, d, &found.best, &asm, report)
                    .await;
                a.finished(
                    &t,
                    r.is_ok(),
                    &r.as_ref()
                        .map_or_else(|e| e.message.clone(), |()| format!("run {run} applied")),
                )
                .await;
                return r;
            }
        }
        if !applying {
            return Ok(());
        }
        self.apply_run(tenant, ts, home, c, run, d, &found.best, &asm, report)
            .await
    }

    /// Applies run `run`'s `design` (decision `d`, 0 for none) under the
    /// tenant's lock (see the comments inside).
    #[allow(clippy::too_many_arguments)]
    async fn apply_run(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
        c: &mut Conn,
        run: u64,
        d: u64,
        design: &sprig_advisor::sharding::design::Design,
        asm: &input::Assembled,
        report: &mut TickReport,
    ) -> Result<()> {
        let cat = SchemaName::catalog(tenant).to_string();
        // The groups' maps are created under the tenant's lock, so no
        // whole-tenant move runs beside the apply or starts after it
        // without seeing them.
        let holder = Self::advisor_holder(tenant);
        let blocked = match self.jobs.lock_tenant(tenant, &holder).await? {
            Some(h) => Some(format!("job {h} holds the tenant")),
            None if !self.tenant_active(tenant).await? => {
                self.jobs.unlock_tenant(tenant, &holder).await?;
                Some("the tenant is moving between data centers".to_owned())
            }
            None => None,
        };
        if let Some(why) = blocked {
            runs::set_status(c, &cat, run, "superseded", None, Some(&why)).await?;
            if d > 0 {
                let _ = self
                    .decisions
                    .update_outcome(tenant, d, Outcome::Rejected, now_us())
                    .await;
            }
            lock(&self.state).entry(tenant).or_default().last_run = None;
            report.waiting.push((format!("t{tenant}"), "tenant_job"));
            return Ok(());
        }
        runs::set_status(c, &cat, run, "applying", None, None).await?;
        let ctx = ApplyCtx {
            tenant,
            cluster: home.clone(),
            max_hold: ts.max_hold,
            txn_grace: ts.txn_grace,
            hold_grace: ts.hold_grace,
            chunk_rows: ts.chunk_rows,
            fill_settle: Duration::from_secs(1),
            publish: self.publish.clone(),
        };
        let applied = apply_first_design(
            c,
            &ctx,
            run,
            design,
            &asm.input,
            &asm.located,
            &self.tenant_maps,
            &self.maps,
        )
        .await;
        self.jobs.unlock_tenant(tenant, &holder).await?;
        match applied {
            Ok(a) => {
                runs::set_status(c, &cat, run, "applied", Some(&a.plan), None).await?;
                if d > 0 {
                    let _ = self
                        .decisions
                        .update_outcome(tenant, d, Outcome::Succeeded, now_us())
                        .await;
                }
                report.applied_hold_ms = Some(a.hold_ms);
                tracing::info!(tenant = %tenant, run, hold_ms = a.hold_ms, rows = a.backfilled, "first design applied");
            }
            Err(e) => {
                tracing::warn!(tenant = %tenant, run, error = %e, "first design not applied; retrying");
                runs::set_status(c, &cat, run, "failed", None, Some(&e.to_string())).await?;
                if d > 0 {
                    let _ = self
                        .decisions
                        .update_outcome(tenant, d, Outcome::Failed, now_us())
                        .await;
                }
                // The next tick re-runs at once and resumes what exists.
                lock(&self.state).entry(tenant).or_default().last_run = None;
                return Err(e);
            }
        }
        Ok(())
    }

    /// A first design whose apply stopped (an error, a new leader): its
    /// groups are `preparing`; rebuild the design from the catalog and run
    /// the apply again, which skips what is done.
    async fn resume_design(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
        c: &mut Conn,
        report: &mut TickReport,
    ) -> Result<()> {
        use mysql_async::prelude::Queryable as _;
        use sprig_advisor::sharding::design::{Design, GroupDesign, Member, Routing};
        let cat = SchemaName::catalog(tenant).to_string();
        let err = |e: mysql_async::Error| SprigError::internal(format!("resume: {e}"));
        let groups: Vec<(u32, String, String, Option<u64>)> = c
            .query(format!(
                "SELECT group_id, name, CAST(key_spec AS CHAR), advisor_run_id FROM \
                 `{cat}`.table_groups WHERE state = 'preparing' AND origin = 'advisor' \
                 ORDER BY group_id"
            ))
            .await
            .map_err(err)?;
        let Some(run) = groups.iter().find_map(|g| g.3) else {
            return Ok(());
        };
        let mut design = Design::default();
        for (gid, name, spec, _) in &groups {
            let v: serde_json::Value = serde_json::from_str(spec)
                .map_err(|e| SprigError::internal(format!("key_spec: {e}")))?;
            let key = sprig_shard::KeySpec::from_json(&v)?;
            let rows: Vec<MemberRow> = c
                .exec(
                    format!(
                        "SELECT object_id, routing, CAST(key_columns AS CHAR), parent_object_id, \
                         parent_fk FROM `{cat}`.group_members WHERE group_id = ? ORDER BY object_id"
                    ),
                    (gid,),
                )
                .await
                .map_err(err)?;
            let members = rows
                .into_iter()
                .map(|(o, r, cols, parent, fk)| Member {
                    object: o,
                    routing: if r == "parent" {
                        Routing::Parent {
                            parent: parent.unwrap_or(0),
                            fk: fk.unwrap_or_default(),
                        }
                    } else {
                        Routing::Columns {
                            columns: cols
                                .and_then(|x| serde_json::from_str(&x).ok())
                                .unwrap_or_default(),
                        }
                    },
                })
                .collect();
            design.groups.push(GroupDesign {
                name: name.clone(),
                key: key.parts.clone(),
                members,
            });
        }
        let asm = input::assemble(
            c,
            tenant,
            SystemTime::now(),
            ts.advisor.window,
            ts.sharding.split_bytes,
        )
        .await?;
        let ctx = ApplyCtx {
            tenant,
            cluster: home.clone(),
            max_hold: ts.max_hold,
            txn_grace: ts.txn_grace,
            hold_grace: ts.hold_grace,
            chunk_rows: ts.chunk_rows,
            fill_settle: Duration::from_secs(1),
            publish: self.publish.clone(),
        };
        runs::set_status(c, &cat, run, "applying", None, None).await?;
        let a = apply_first_design(
            c,
            &ctx,
            run,
            &design,
            &asm.input,
            &asm.located,
            &self.tenant_maps,
            &self.maps,
        )
        .await?;
        runs::set_status(c, &cat, run, "applied", Some(&a.plan), None).await?;
        report.applied_hold_ms = Some(a.hold_ms);
        tracing::info!(tenant = %tenant, run, rows = a.backfilled, "first design applied (resumed)");
        Ok(())
    }

    /// Step 2 for one group.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn group_tick(
        &self,
        tenant: TenantId,
        group: GroupId,
        (min_shards, created_ms): (Option<u32>, Option<u64>),
        ts: &TenantSettings,
        born: &BTreeMap<(u32, u32), u64>,
        running: &mut Vec<RunningJob>,
        report: &mut TickReport,
    ) -> Result<()> {
        let (map, _) = self.maps.load(tenant, group).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let home = Placement::of(&self.tenant_maps.load(tenant).await?.0)?.cluster;
        let mut cc = self.conn(&home).await?;
        let dbs: Vec<u32> = mysql_async::prelude::Queryable::exec(
            &mut cc,
            format!(
                "SELECT DISTINCT db_id FROM `{cat}`.group_members WHERE group_id = ? ORDER BY db_id"
            ),
            (group.0,),
        )
        .await
        .map_err(|e| SprigError::internal(format!("members: {e}")))?;
        // Multi-shard DDL jobs hold their shards: a split,
        // merge or move of them waits (the `group-schema` lock as built).
        let ddl_rows = sprig_catalog::store::Store::for_tenant(tenant)
            .settings_of(&mut cc, sprig_catalog::store::SettingScope::Tenant)
            .await?;
        drop(cc);
        let dbs: Vec<DbId> = dbs.into_iter().map(DbId).collect();
        let min_shards = group_min_shards(
            min_shards,
            dbs.iter().map(|d| self.settings.min_shards(tenant, *d)),
        );
        let schemas = |s: ShardId| -> Vec<String> {
            dbs.iter()
                .map(|d| shard_schema(tenant, *d, group, s))
                .collect()
        };
        let busy_shards: BTreeSet<u32> = running
            .iter()
            .filter(|r| r.tenant == tenant && r.group == Some(group.0))
            .flat_map(|r| r.sources.iter().chain(&r.targets).copied())
            .collect();
        let mut stats = Vec::new();
        let mut conns: HashMap<ClusterId, Conn> = HashMap::new();
        for (i, r) in map.ranges.iter().enumerate() {
            if !conns.contains_key(&r.cluster) {
                conns.insert(r.cluster.clone(), self.conn(&r.cluster).await?);
            }
            let c = conns
                .get_mut(&r.cluster)
                .ok_or_else(|| SprigError::internal("conn"))?;
            let size = shards::stats::shard_size(c, &schemas(r.shard)).await?;
            metrics::histogram!("sprig_balancer_shard_size_bytes").record(size);
            stats.push(ShardStat {
                shard: r.shard.0,
                lo: r.lo,
                hi: map.hi(i),
                cluster: r.cluster.clone(),
                size_bytes: size,
                write_shares: self.write_load.as_ref().map_or_else(Vec::new, |w| {
                    w.write_shares(
                        tenant,
                        group,
                        r.shard,
                        born.get(&(group.0, r.shard.0)).copied(),
                    )
                }),
                cluster_hot: self
                    .write_load
                    .as_ref()
                    .is_some_and(|w| w.cluster_hot(&r.cluster)),
                born_ms: shard_birth(born.get(&(group.0, r.shard.0)).copied(), created_ms),
                busy: r.state == RangeState::Held || busy_shards.contains(&r.shard.0),
            });
        }
        let gs = GroupStat {
            tenant,
            group: group.0,
            min_shards,
            shards: stats.clone(),
        };
        for mut p in shards::evaluate(&gs, &ts.sharding, now_ms()) {
            let subject = format!("t{tenant}/g{}/s{:?}", group.0, p.job.sources());
            let split_of = match &p.job {
                ShardJob::Split { shard, points } if points.is_empty() => Some(*shard),
                _ => None,
            };
            if let Some(shard) = split_of {
                let st = stats.iter().find(|s| s.shard == shard).cloned();
                let Some(st) = st else { continue };
                let c = conns
                    .get_mut(&st.cluster)
                    .ok_or_else(|| SprigError::internal("conn"))?;
                let ways = if p.reason == Reason::Size {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let k = (st.size_bytes / (ts.sharding.split_bytes / 2.0)).ceil() as u32;
                    k.clamp(2, ts.sharding.max_split_ways)
                } else {
                    2
                };
                let by_writes = if p.reason == Reason::Load {
                    self.write_load.as_ref().and_then(|w| {
                        w.write_histogram(
                            tenant,
                            group,
                            ShardId(shard),
                            st.lo,
                            st.hi,
                            ts.sharding.sustain,
                        )
                    })
                } else {
                    None
                };
                let weight = if by_writes.is_some() {
                    Weight::Writes
                } else {
                    Weight::Bytes
                };
                let h = if let Some(h) = by_writes {
                    h
                } else {
                    match shards::stats::size_histogram(
                        c,
                        &schemas(ShardId(shard)),
                        st.lo,
                        st.hi,
                        st.size_bytes,
                    )
                    .await
                    {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!(error = %e, "histogram dives failed: equal widths");
                            shards::HashHistogram::equal(st.lo, st.hi, 64)
                        }
                    }
                };
                match shards::choose_split(&h, weight, ways) {
                    Ok(plan) => {
                        if let (Some(o), Some(hot)) = (p.evidence.as_object_mut(), plan.hot_key) {
                            o.insert("hot_key".into(), json!(format!("{hot:#018x}")));
                        }
                        if let Some(o) = p.evidence.as_object_mut() {
                            o.insert("shares".into(), json!(plan.shares));
                            o.insert("ways".into(), json!(ways));
                        }
                        if plan.hot_key.is_some() {
                            p.reason = Reason::HotKey;
                        }
                        if let ShardJob::Split { points, .. } = &mut p.job {
                            *points = plan.points;
                        }
                    }
                    Err(why) => {
                        let key = (subject.clone(), "refused");
                        if lock(&self.state)
                            .entry(tenant)
                            .or_default()
                            .logged_waits
                            .insert(key)
                        {
                            let d = shards::decision(
                                &p,
                                Mode::Applied,
                                Outcome::Rejected,
                                format!("no split of shard {shard}: {why:?}"),
                                None,
                                now_us(),
                                &self.actor,
                                &self.dc,
                            );
                            self.record(tenant, &d).await;
                        }
                        continue;
                    }
                }
            }
            let cluster = stats
                .iter()
                .find(|s| p.job.sources().contains(&s.shard))
                .map_or_else(|| home.clone(), |s| s.cluster.clone());
            let ddl_jobs = sprig_catalog::ddl::jobs_on_shards(
                &ddl_rows,
                &p.job
                    .sources()
                    .iter()
                    .flat_map(|x| {
                        dbs.iter()
                            .map(|d| SchemaName::shard(tenant, *d, group, ShardId(*x)))
                    })
                    .collect::<Vec<_>>(),
            );
            let adm = if ddl_jobs.is_empty() {
                shards::admit(
                    &p,
                    running,
                    &ts.sharding,
                    &cluster,
                    &lock(&self.backoff),
                    now_ms(),
                )
            } else {
                Admission::Wait("ddl")
            };
            // Under `recommend` an approval in the inbox runs the
            // job. The gate is asked once the job would be admitted as under
            // `on` (so an approval isn't spent while a slot or lock is
            // busy); approved, it goes through the same submission.
            let mut ticket: Option<sprig_approvals::Ticket> = None;
            let mut gated = false;
            let adm = if matches!(adm, Admission::Recommend) && ddl_jobs.is_empty() {
                let mut on = ts.sharding;
                on.mode = ShardingMode::On;
                let would =
                    shards::admit(&p, running, &on, &cluster, &lock(&self.backoff), now_ms());
                if matches!(would, Admission::Submit) {
                    gated = true;
                    match self
                        .approval_gate(tenant, &p, &subject, ts.sharding.mode)
                        .await
                    {
                        Ok(t) => {
                            ticket = t;
                            Admission::Submit
                        }
                        Err(_) => adm,
                    }
                } else {
                    adm
                }
            } else {
                adm
            };
            match adm {
                Admission::Wait(why) => {
                    metrics::counter!("sprig_balancer_jobs_waiting_total", "blocked_by" => why)
                        .increment(1);
                    report.waiting.push((subject.clone(), why));
                    let fresh = lock(&self.state)
                        .entry(tenant)
                        .or_default()
                        .logged_waits
                        .insert((subject.clone(), why));
                    if fresh {
                        let d = shards::decision(
                            &p,
                            Mode::Applied,
                            Outcome::Proposed,
                            format!("{} of {subject} waits: {why}", p.job.kind()),
                            None,
                            now_us(),
                            &self.actor,
                            &self.dc,
                        );
                        self.record(tenant, &d).await;
                    }
                }
                Admission::Recommend => {
                    // Advice in the approvals inbox too (an
                    // approval runs it once it would be admitted, above).
                    if !gated {
                        let _ = self
                            .approval_gate(tenant, &p, &subject, ts.sharding.mode)
                            .await;
                    }
                    let key = (subject.clone(), "recommend");
                    if lock(&self.state)
                        .entry(tenant)
                        .or_default()
                        .logged_waits
                        .insert(key)
                    {
                        let d = shards::decision(
                            &p,
                            Mode::Recommended,
                            Outcome::Proposed,
                            format!("recommend {} of {subject}", p.job.kind()),
                            None,
                            now_us(),
                            &self.actor,
                            &self.dc,
                        );
                        self.record(tenant, &d).await;
                    }
                }
                Admission::Submit => {
                    if ts.sharding.mode == ShardingMode::Approve {
                        match self
                            .approval_gate(tenant, &p, &subject, ts.sharding.mode)
                            .await
                        {
                            Ok(t) => ticket = t,
                            Err(why) => {
                                report.waiting.push((subject.clone(), "approval"));
                                tracing::debug!(tenant = %tenant, subject = %subject, why, "shard job held");
                                continue;
                            }
                        }
                    }
                    let Ok(id) = self
                        .submit(tenant, &map, &dbs, &p, ts, running, report)
                        .await?
                    else {
                        self.approval_done(ticket.as_ref(), false, "the job couldn't be submitted")
                            .await;
                        continue;
                    };
                    // The other side of the DDL exclusion and of a move
                    // between DCs: a schema change writes its intent row, and a
                    // move its directory state, before checking for shard jobs;
                    // this job checks for them after taking its locks. One of
                    // the two always sees the other.
                    if let Some(why) = self
                        .late_conflict(tenant, group, &p.job.sources(), &dbs, &home)
                        .await?
                    {
                        tracing::info!(tenant = %tenant, job = %id.0, blocked_by = why, "shard job withdrawn after a late conflict");
                        if let Err(e) = self.mover.cancel(&id, &format!("withdrawn: {why}")).await {
                            tracing::warn!(tenant = %tenant, job = %id.0, error = %e, "withdrawing a shard job failed");
                        }
                        report.submitted.retain(|x| x.2 != id);
                        report.waiting.push((subject.clone(), why));
                        metrics::counter!("sprig_balancer_jobs_waiting_total", "blocked_by" => why)
                            .increment(1);
                        self.approval_done(
                            ticket.as_ref(),
                            false,
                            &format!("job {} withdrawn: {why}", id.0),
                        )
                        .await;
                    } else {
                        self.approval_done(
                            ticket.as_ref(),
                            true,
                            &format!("job {} submitted", id.0),
                        )
                        .await;
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn submit(
        &self,
        tenant: TenantId,
        map: &sprig_shard::ShardMap,
        dbs: &[DbId],
        p: &Proposal,
        ts: &TenantSettings,
        running: &mut Vec<RunningJob>,
        report: &mut TickReport,
    ) -> Result<std::result::Result<JobId, SprigError>> {
        let next = shards::next_shard_id(map, running);
        let retire = u64::try_from(ts.sharding.retire.as_millis()).unwrap_or(u64::MAX);
        let mut spec = shards::build_spec(p, map, dbs, next, retire)?;
        let summary = format!(
            "{} shard(s) {:?} of group {} ({}) into {:?}",
            p.job.kind(),
            p.job.sources(),
            p.group,
            p.reason.code(),
            spec.targets.iter().map(|t| t.shard).collect::<Vec<_>>()
        );
        let d = self
            .record(
                tenant,
                &shards::decision(
                    p,
                    Mode::Applied,
                    Outcome::Proposed,
                    summary,
                    None,
                    now_us(),
                    &self.actor,
                    &self.dc,
                ),
            )
            .await;
        if d > 0 {
            spec = spec.with_decision(d);
        }
        let mine = RunningJob {
            tenant,
            group: spec.group(),
            sources: spec.sources.iter().map(|s| s.shard).collect(),
            targets: spec.targets.iter().map(|t| t.shard).collect(),
            clusters: spec
                .sources
                .iter()
                .map(|s| s.cluster.clone())
                .chain(spec.targets.iter().map(|t| t.cluster.clone()))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        };
        match self.mover.submit(spec).await {
            Ok(id) => {
                if d > 0 {
                    let _ = self
                        .decisions
                        .update_outcome(tenant, d, Outcome::Scheduled, now_us())
                        .await;
                }
                tracing::info!(tenant = %tenant, job = %id.0, kind = p.job.kind(), reason = p.reason.code(), "shard job submitted");
                report
                    .submitted
                    .push((p.job.kind().to_owned(), p.reason.code(), id));
                running.push(mine);
                Ok(Ok(id))
            }
            Err(e) => {
                tracing::warn!(tenant = %tenant, error = %e, "shard job refused by the mover");
                if d > 0 {
                    let _ = self
                        .decisions
                        .update_outcome(tenant, d, Outcome::Rejected, now_us())
                        .await;
                }
                Ok(Err(e))
            }
        }
    }

    /// After a shard job took its locks: the conflict that appeared
    /// meanwhile, if any (`ddl`: a schema change holds a source shard;
    /// `tenant_move`: the tenant started moving between DCs).
    async fn late_conflict(
        &self,
        tenant: TenantId,
        group: GroupId,
        sources: &[u32],
        dbs: &[DbId],
        home: &ClusterId,
    ) -> Result<Option<&'static str>> {
        if !self.tenant_active(tenant).await? {
            return Ok(Some("tenant_move"));
        }
        let schemas: Vec<SchemaName> = sources
            .iter()
            .flat_map(|x| {
                dbs.iter()
                    .map(move |d| SchemaName::shard(tenant, *d, group, ShardId(*x)))
            })
            .collect();
        let mut cc = self.conn(home).await?;
        let rows = sprig_catalog::store::Store::for_tenant(tenant)
            .settings_of(&mut cc, sprig_catalog::store::SettingScope::Tenant)
            .await?;
        Ok((!sprig_catalog::ddl::jobs_on_shards(&rows, &schemas).is_empty()).then_some("ddl"))
    }
}

#[cfg(test)]
mod birth_tests {
    use super::*;

    /// A presplit 4-shard group (no job made its shards) on one
    /// cluster, small and quiet, ten minutes after its creation.
    fn presplit(created_ms: Option<u64>, min_shards: Option<u32>) -> GroupStat {
        let quarter = 1u64 << 62;
        GroupStat {
            tenant: TenantId(42),
            group: 7,
            min_shards,
            shards: (0..4u32)
                .map(|i| ShardStat {
                    shard: i + 1,
                    lo: u64::from(i) * quarter,
                    hi: (i < 3).then(|| u64::from(i + 1) * quarter),
                    cluster: ClusterId::new("dc-a-c1").unwrap(),
                    size_bytes: 1024.0,
                    write_shares: Vec::new(),
                    cluster_hot: false,
                    born_ms: shard_birth(None, created_ms),
                    busy: false,
                })
                .collect(),
        }
    }

    fn merges(g: &GroupStat, now: u64) -> usize {
        let s = ShardingSettings::default(); // 24 h merge cooldown
        shards::evaluate(g, &s, now)
            .iter()
            .filter(|p| p.reason == Reason::Merge)
            .count()
    }

    #[test]
    fn a_presplit_group_is_not_merged_within_the_cooldown() {
        let created = 1_800_000_000_000;
        let now = created + 10 * 60 * 1000;
        // Before: no birth time, so the shards counted as old and merged.
        assert_eq!(merges(&presplit(None, None), now), 2);
        // Born with the group: nothing merges within 24 hours.
        assert_eq!(merges(&presplit(Some(created), None), now), 0);
        // After the cooldown, merges may go on (the setting allows 1 shard).
        let later = created + 25 * 3600 * 1000;
        assert_eq!(merges(&presplit(Some(created), None), later), 2);
    }

    #[test]
    fn min_shards_at_database_scope_is_never_merged_below() {
        let created = 1_800_000_000_000;
        let later = created + 25 * 3600 * 1000;
        // `sharding.min_shards` 4 on one of the group's databases.
        let min = group_min_shards(None, [1, 4]);
        assert_eq!(min, Some(4));
        assert_eq!(merges(&presplit(Some(created), min), later), 0);
        // The group's own value overrides the setting.
        assert_eq!(group_min_shards(Some(2), [4]), Some(2));
        assert_eq!(merges(&presplit(Some(created), Some(2)), later), 2);
    }
}
