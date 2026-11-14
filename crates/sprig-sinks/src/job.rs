//! One sink task in a worker (chapter 51, "Record pipeline", "Task
//! placement"; build steps 5 and 15): the `sink_task` job body.
//!
//! ```text
//! run_sink_task(tenant, sink, task):
//!   def := sinks row; gone, dropping or failed → end
//!   epoch := claim sink_tasks(sink, task) (bumped on every claim)
//!   open the connections: tenant credentials opened from sink_secrets with
//!     the tenant's sink key; platform credentials from files, after the grant
//!   destinations := sink.destination_allowlist (platform: the operator's hosts)
//!   target := webhook | kafka | s3 | database | script
//!   source := the stream's log from consumer group __sprig_sink_<id>
//!   runtime::run_task(...) until cancelled, fenced, dropped, or the
//!     definition changes (the controller starts the job again)
//! ```
//!
//! The job runtime's lease decides which worker runs a task; the epoch in
//! `sink_tasks` fences one that lost it (every settle checks it).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;

use crate::filter::SinkPrepare;
use crate::model::{
    ConnKind, ConnRef, Guarantee, SinkDef, SinkId, SinkState, SourceSpec, TargetKind, TaskEpoch,
    TaskNo, TaskState, err,
};
use crate::policy::{Destinations, Residency, Unpinned, parse_allowlist};
use crate::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, RunSettings, Target, TaskEnd,
    TaskInfo, TaskParts, run_task,
};
use crate::secrets::SinkKeyring;
use crate::store::SinksStore;
use crate::target::{Conn, MemSecrets, Opts, TargetCtx};

/// What a sink task needs from its worker.
#[async_trait]
pub trait SinkEnv: Send + Sync {
    /// The tenant catalog's pool and schema.
    async fn catalog(&self, tenant: TenantId) -> Result<(Pool, String)>;
    /// A pool (as `sprig_ctl`) on the tenant's cluster, for its shards.
    async fn shard_pool(&self, tenant: TenantId) -> Result<Pool>;
    /// The DC tier.
    fn dc(&self) -> Arc<dyn MetaStore>;
    /// The global tier (platform connections).
    fn global(&self) -> Option<Arc<dyn MetaStore>>;
    /// The sink credential keyring.
    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>>;
    /// The tenant's residency policy.
    fn residency(&self, _tenant: TenantId) -> Box<dyn Residency> {
        Box::new(Unpinned)
    }
    /// The tenant's residency policy as the directory holds it now (its
    /// own and its ancestors' policies); by default [`SinkEnv::residency`].
    ///
    /// # Errors
    /// The directory's.
    async fn residency_of(&self, tenant: TenantId) -> Result<Box<dyn Residency>> {
        Ok(self.residency(tenant))
    }
    /// `sink.platform_secret_dir`.
    fn secret_dir(&self) -> PathBuf {
        PathBuf::from("/etc/sprigdb/platform-secrets")
    }
    /// This worker's id.
    fn worker_id(&self) -> String;
    /// The worker's live settings (etcd scopes) and its DC: global
    /// `sink.*` values under the catalog's rows. `None`: the defaults.
    fn settings(&self) -> Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)> {
        None
    }
    /// The DC's gateways, for statements as the sink's owner (queue
    /// sources, the script `db` handle; R9, r9queue). `None`: none here.
    fn peer(&self) -> Option<Arc<dyn crate::peer::SinkPeer>> {
        None
    }
    /// Chapter 29 (decision RL31): whether row-level security policies on
    /// any of tables `objects` now bind user `owner` (the sink would export
    /// rows its owner can't see). By default `false`: workers without the
    /// tenant catalog rely on the gateway's pause (RL25).
    ///
    /// # Errors
    /// The catalog's.
    async fn row_policies_bind(
        &self,
        _tenant: TenantId,
        _owner: u32,
        _objects: &[u64],
    ) -> Result<bool> {
        Ok(false)
    }
    /// Chapter 51 (39229, authfix2): why the sink's owner `owner` can no
    /// longer read `sources` (dropped, locked, or `SELECT` lost), or
    /// `None` while it can. By default `None`: workers without the tenant
    /// catalog leave it to the gateway's check at grant changes.
    ///
    /// # Errors
    /// The catalog's.
    async fn owner_lost(
        &self,
        _tenant: TenantId,
        _owner: u32,
        _sources: &[OwnerRead],
    ) -> Result<Option<String>> {
        Ok(None)
    }
}

/// The start of the pause reason row-level security gives a sink (RL25,
/// RL31): a running task stops at once, not after its buffered records.
pub const RLS_PAUSE: &str = "ER_SPRIG_RLS_UNSUPPORTED";

/// The start of the pause reason when the sink's owner can no longer read
/// its source (chapter 51, "Owner lost access"; authfix2): the sink
/// resumes by itself once the owner can again.
pub const OWNER_PAUSE: &str = "39229 ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST";

/// What a sink reads, for its owner's `SELECT` (39229): by database and
/// name, or a table by object id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerRead {
    /// A table, stream or queue of database `db` (a change topic `t.changes`
    /// reads as table `t`).
    Named {
        /// The database.
        db: u32,
        /// The name.
        name: String,
    },
    /// A table (`CHANGES OF t`).
    Object(u64),
}

/// The sources `def`'s owner must be able to read (chapter 51, "Owner
/// lost access"). Fan-in sinks read other tenants' instances under
/// `SPRIG_FAN_IN` and are left out.
#[must_use]
pub fn owner_reads(def: &SinkDef) -> Vec<OwnerRead> {
    match &def.source {
        SourceSpec::Stream { name, .. } => vec![OwnerRead::Named {
            db: def.db_id,
            name: name.strip_suffix(".changes").unwrap_or(name).to_owned(),
        }],
        SourceSpec::Queue { name, .. } => vec![OwnerRead::Named {
            db: def.db_id,
            name: name.clone(),
        }],
        SourceSpec::Changes { objects } => objects
            .iter()
            .map(|(id, _)| OwnerRead::Object(*id))
            .collect(),
        _ => Vec::new(),
    }
}

/// The pause reason of an owner that lost access.
#[must_use]
pub fn owner_pause_reason(why: &str) -> String {
    format!("{OWNER_PAUSE}: {why}; the sink resumes once the owner can read its source again")
}

/// Who a sink's statements run as on a gateway: its owner, in its
/// tenant (by name) and database.
///
/// # Errors
/// The directory's (the tenant's name).
async fn run_as(
    env: &dyn SinkEnv,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
) -> Result<crate::peer::RunAs> {
    let global = env.global().ok_or_else(|| {
        SprigError::new(
            SprigCode::SinkOptionInvalid,
            "this worker has no global tier to name the sink's tenant",
        )
    })?;
    Ok(crate::peer::RunAs {
        tenant: tenant.0,
        tenant_name: crate::peer::tenant_name(global.as_ref(), tenant.0).await?,
        user_id: def.owner_user_id,
        database: opts.get("__db_name").unwrap_or("").to_owned(),
    })
}

/// The script `db` handle of a script sink in this worker (R9, r9queue):
/// reads through a gateway as the sink's owner. `None` for other targets
/// or without gateways.
async fn script_db(
    env: &dyn SinkEnv,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
) -> Result<Option<Arc<dyn crate::target::script::ReadOnlyDb>>> {
    if def.target.kind() != TargetKind::Script {
        return Ok(None);
    }
    let Some(peer) = env.peer() else {
        return Ok(None);
    };
    let who = run_as(env, tenant, def, opts).await?;
    Ok(Some(Arc::new(crate::peer::PeerReadOnly::new(peer, who))))
}

/// `opts` with every `sink.*` key set at the global scope of `etcd` (in
/// `dc`) added where the catalog and the sink's own options don't set it:
/// object > database > tenant > `WITH` > global > default.
#[must_use]
pub fn over_global(
    mut opts: Opts,
    etcd: &sprig_settings::MemSettings<'_>,
    dc: &sprig_common::ids::DcId,
) -> Opts {
    let ctx = sprig_settings::SettingCtx::process(dc);
    for def in etcd.registry().iter() {
        let Some(short) = def.key().strip_prefix("sink.") else {
            continue;
        };
        if opts.0.contains_key(short) {
            continue;
        }
        let Ok(r) = etcd.resolve(def.key(), &ctx) else {
            continue;
        };
        if r.source == sprig_settings::ScopeKind::Default {
            continue;
        }
        let text = match r.value {
            sprig_settings::Value::Bool(b) => if b { "on" } else { "off" }.to_owned(),
            sprig_settings::Value::Int(i) => i.to_string(),
            sprig_settings::Value::Float(f) => f.to_string(),
            sprig_settings::Value::Str(t) => t,
        };
        opts.0.insert(short.to_owned(), text);
    }
    opts
}

/// The partitions task `task_no` reads, of a stream with `partitions`
/// partitions at `per_task` partitions a task (one under `EXACTLY ONCE`).
#[must_use]
pub fn task_partitions(partitions: u32, per_task: u32, task_no: u32) -> Vec<u32> {
    let k = per_task.max(1);
    (task_no * k..((task_no + 1) * k).min(partitions)).collect()
}

/// How many tasks a sink has.
#[must_use]
pub fn task_count(def: &SinkDef, per_task: u32) -> u32 {
    match &def.source {
        SourceSpec::Stream { partitions, .. } => {
            let k = if def.guarantee == Guarantee::ExactlyOnce {
                1
            } else {
                per_task.max(1)
            };
            partitions.div_ceil(k).max(1)
        }
        _ => 1,
    }
}

fn unquote(v: &str) -> String {
    serde_json::from_str::<Value>(v)
        .ok()
        .and_then(|j| match j {
            Value::String(s) => Some(s),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(if b { "on".into() } else { "off".into() }),
            _ => None,
        })
        .unwrap_or_else(|| v.trim().to_owned())
}

/// `sink.*` from the catalog's settings rows (tenant < database < the
/// sink's own scope), then the sink's `WITH (...)` values, keys without
/// `sink.`.
///
/// # Errors
/// The backend's.
pub async fn resolve_opts<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    def: &SinkDef,
) -> Result<Opts> {
    let rows: Vec<(String, String, Option<String>)> = q
        .exec(
            format!(
                "SELECT scope_kind, setting_key, CAST(value AS CHAR) FROM {}.settings \
                 WHERE setting_key LIKE 'sink.%' AND ((scope_kind = 'object' AND scope_id = ?) \
                 OR (scope_kind = 'database' AND scope_id = ?) OR scope_kind = 'tenant')",
                sprig_common::names::quote(catalog)
            ),
            (def.id.0, def.db_id),
        )
        .await
        .map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
    let rank = |k: &str| match k {
        "object" => 0,
        "database" => 1,
        _ => 2,
    };
    let mut best: BTreeMap<String, (u8, String)> = BTreeMap::new();
    for (kind, key, value) in rows {
        let Some(v) = value else { continue };
        let r = rank(&kind);
        let k = key.strip_prefix("sink.").unwrap_or(&key).to_owned();
        if best.get(&k).is_none_or(|(br, _)| r < *br) {
            best.insert(k, (r, unquote(&v)));
        }
    }
    let mut m: BTreeMap<String, String> = best.into_iter().map(|(k, (_, v))| (k, v)).collect();
    for (k, v) in &def.options {
        m.insert(k.clone(), v.clone());
    }
    Ok(Opts(m))
}

/// The runtime settings from resolved options.
///
/// # Errors
/// 39218 for a bad number.
pub fn run_settings(o: &Opts, def: &SinkDef) -> Result<RunSettings> {
    let ms = |k: &str, d: u64| o.u64(k, d).map(Duration::from_millis);
    Ok(RunSettings {
        lanes: u16::try_from(o.u64("lanes", 8)?).unwrap_or(8),
        batch_max_records: usize::try_from(o.u64("batch_max_records", 500)?).unwrap_or(500),
        batch_max_bytes: o.u64("batch_max_bytes", 4 << 20)?,
        batch_max_wait: ms("batch_max_wait_ms", 1000)?,
        max_inflight_bytes: o.u64("max_inflight_bytes", 64 << 20)?,
        retry_initial: ms("retry_initial_ms", 500)?,
        retry_max: ms("retry_max_ms", 60_000)?,
        record_max_attempts: u32::try_from(o.u64("record_max_attempts", 10)?).unwrap_or(10),
        pause_after: Duration::from_secs(o.u64("pause_after_minutes", 15)? * 60),
        auto_resume: o.flag("auto_resume", true),
        probe_interval: Duration::from_secs(o.u64("probe_interval_seconds", 60)?),
        delivery_timeout: ms("http_timeout_ms", 30_000)?.max(Duration::from_secs(1))
            + Duration::from_secs(5),
        tenant_bytes_per_sec: o.u64("tenant_bytes_per_sec", 0)?,
        settle_every: Duration::from_millis(200),
        poll_every: Duration::from_secs(1),
        exactly_once: def.guarantee == Guarantee::ExactlyOnce,
    })
}

/// Dead letters into `sink_dead_letters`.
struct CatalogDead {
    pool: Pool,
    store: SinksStore,
    sink: SinkId,
    /// `DEAD LETTER TO STREAM d`: the stream, its outbox schema and a
    /// pool on its cluster.
    stream: Option<(Arc<sprig_streams::StreamDef>, String, Pool)>,
}

impl CatalogDead {
    /// The dead-letter stream of `def` (`DEAD LETTER TO STREAM d`), if any.
    async fn target(
        env: &dyn SinkEnv,
        tenant: TenantId,
        pool: &Pool,
        schema: &str,
        def: &SinkDef,
    ) -> Result<Option<(Arc<sprig_streams::StreamDef>, String, Pool)>> {
        let Some(crate::model::DeadLetter::Stream { id, .. }) = &def.dead_letter else {
            return Ok(None);
        };
        let mut c = pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
        let d = sprig_streams::StreamsStore::new(schema)
            .get(&mut c, sprig_streams::StreamId(*id))
            .await?
            .ok_or_else(|| {
                err(
                    SprigCode::SinkSourceLost,
                    format!("dead-letter stream {id} is gone"),
                )
            })?;
        let slot = d.outbox.first().ok_or_else(|| {
            err(
                SprigCode::SinkOptionInvalid,
                "the dead-letter stream has no outbox",
            )
        })?;
        let out_schema = slot.schema(tenant, d.db).to_string();
        Ok(Some((
            Arc::new(d),
            out_schema,
            env.shard_pool(tenant).await?,
        )))
    }
}

#[async_trait]
impl DeadLetters for CatalogDead {
    async fn write(&self, rec: &crate::model::SinkRecord, info: &DeadInfo) -> Result<()> {
        // The stream first: a crash between the two writes a duplicate
        // dead letter, never loses one.
        if let Some((d, schema, pool)) = &self.stream {
            let mut h: Vec<(String, Option<Vec<u8>>)> = rec
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect();
            for (k, v) in [
                ("sprig.dl.reason", info.reason.clone()),
                ("sprig.dl.class", info.class.to_owned()),
                ("sprig.dl.attempts", info.attempts.to_string()),
                ("sprig.dl.sink", self.sink.0.to_string()),
                ("sprig.dl.idempotency_key", rec.idem_key.clone()),
                ("sprig.dl.position", rec.pos.text()),
            ] {
                h.push((k.to_owned(), Some(v.into_bytes())));
            }
            let p = sprig_streams::publish::Publish {
                key: rec.key.clone(),
                value: rec
                    .value
                    .clone()
                    .or_else(|| rec.after.as_ref().map(|a| a.to_string().into_bytes())),
                headers: sprig_streams::publish::headers_json(&h),
                partition: None,
                ts_ms: None,
            };
            if let Some(q) = sprig_streams::publish::outbox_insert(d, Some(schema), &[p]) {
                let mut c = pool.get_conn().await.map_err(|e| {
                    let mut x = SprigError::new(
                        SprigCode::CatalogUnavailable,
                        "dead-letter stream unavailable",
                    )
                    .with_source(e);
                    x.retryable = true;
                    x
                })?;
                c.query_drop(q).await.map_err(|e| {
                    let mut x = SprigError::new(
                        SprigCode::CatalogUnavailable,
                        "dead-letter stream write failed",
                    )
                    .with_source(e);
                    x.retryable = true;
                    x
                })?;
                metrics::counter!("sprig_sink_dead_letters_total", "to" => "stream").increment(1);
            }
        }
        let mut c = self.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
        self.store.insert_dead(&mut c, self.sink, rec, info).await
    }
}

/// The catalog side of a task: directives, state reports, the epoch.
struct CatalogControl {
    pool: Pool,
    store: SinksStore,
    sink: SinkId,
    task: TaskNo,
    epoch: TaskEpoch,
    definition_ver: u64,
    delivered: AtomicU64,
    /// Chapter 29 (RL31): a change sink's owner and tables, checked before
    /// each batch (every poll).
    rls: Option<(Arc<dyn SinkEnv>, TenantId, u32, Vec<u64>)>,
    /// R9 (r9last): a grouped fan-in task's check of its sink's task
    /// count (see [`Regroup`]).
    regroup: Option<Regroup>,
    /// Chapter 51 (39229, authfix2): the owner's access to the source,
    /// checked at most every second before a batch.
    owner: Option<OwnerCheck>,
}

/// A task's check of its sink owner's access (see
/// [`SinkEnv::owner_lost`]).
struct OwnerCheck {
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    owner: u32,
    sources: Vec<OwnerRead>,
    next: std::sync::Mutex<Option<std::time::Instant>>,
}

impl OwnerCheck {
    fn new(env: &Arc<dyn SinkEnv>, tenant: TenantId, def: &SinkDef) -> Option<Self> {
        let sources = owner_reads(def);
        (!sources.is_empty()).then(|| Self {
            env: Arc::clone(env),
            tenant,
            owner: def.owner_user_id,
            sources,
            next: std::sync::Mutex::new(None),
        })
    }

    /// Why the owner lost access, checked now when `now` or once a second
    /// has passed since the last check.
    async fn lost(&self, now: bool) -> Result<Option<String>> {
        let t = std::time::Instant::now();
        {
            let mut n = self
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !now && n.is_some_and(|at| t < at) {
                return Ok(None);
            }
            *n = Some(t + Duration::from_secs(1));
        }
        self.env
            .owner_lost(self.tenant, self.owner, &self.sources)
            .await
    }
}

/// A fan-in task started as one of `of` tasks: it stops once the sink's
/// index entry has another count (the controller regrouped the sink), so
/// the new tasks start after it ends. The entry is read at most every 5 s.
struct Regroup {
    dc: Arc<dyn sprig_meta::MetaStore>,
    tenant: u64,
    sink: u64,
    of: u32,
    next: std::sync::Mutex<Option<std::time::Instant>>,
}

impl Regroup {
    /// Whether the task should stop now (see the type's docs).
    async fn moved(&self) -> bool {
        let now = std::time::Instant::now();
        {
            let mut n = self
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if n.is_some_and(|t| now < t) {
                return false;
            }
            *n = Some(now + Duration::from_secs(5));
        }
        match crate::meta::index_get(&self.dc, self.tenant, self.sink).await {
            Ok(Some(ix)) => ix.tasks != self.of,
            // Gone: the catalog state stops the task.
            Ok(None) | Err(_) => false,
        }
    }
}

impl CatalogControl {
    async fn conn(&self) -> Result<mysql_async::Conn> {
        self.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })
    }
}

/// The directive that stops a task, from its sink's catalog state: gone,
/// redefined, failed, dropping, or paused by row-level security (RL25,
/// RL31: a running task stops at once, its buffered records undelivered).
fn stop_directive(
    state: Option<&(SinkState, Option<String>, u64)>,
    definition_ver: u64,
) -> Option<Directive> {
    match state {
        None => Some(Directive::Stop),
        Some((_, _, v)) if *v != definition_ver => Some(Directive::Stop),
        Some((SinkState::Failed | SinkState::Dropping, _, _)) => Some(Directive::Stop),
        Some((SinkState::Paused, r, _))
            if r.as_deref().is_some_and(|r| r.starts_with(RLS_PAUSE)) =>
        {
            Some(Directive::Stop)
        }
        _ => None,
    }
}

impl CatalogControl {
    /// RL31: whether policies now bind the sink's owner on its tables; if
    /// so the sink is paused (state `paused`, reason 39366), as the
    /// gateway's policy statements do.
    async fn rls_bound(&self, c: &mut mysql_async::Conn) -> Result<bool> {
        let Some((env, tenant, owner, objects)) = &self.rls else {
            return Ok(false);
        };
        if !env.row_policies_bind(*tenant, *owner, objects).await? {
            return Ok(false);
        }
        self.store
            .set_state(
                c,
                self.sink,
                SinkState::Paused,
                Some(&format!(
                    "{RLS_PAUSE}: row-level security on a source table binds the sink's owner; \
                     the sink would export rows the owner can't see"
                )),
            )
            .await?;
        tracing::warn!(tenant = %tenant, sink = self.sink.0, "sink paused by its worker: its owner is bound by row-level security");
        Ok(true)
    }
}

impl CatalogControl {
    /// Chapter 51 (39229, authfix2): a sink paused because its owner lost
    /// access resumes once the owner can read its source again
    /// (`Some(Run)`), else stays paused (`Some(Stop)`: the controller
    /// starts the task again a few seconds later, which checks again). A
    /// running sink whose owner lost access is paused (`Some(Stop)`).
    async fn owner_directive(
        &self,
        c: &mut mysql_async::Conn,
        state: Option<&(SinkState, Option<String>, u64)>,
    ) -> Result<Option<Directive>> {
        let Some(check) = &self.owner else {
            return Ok(None);
        };
        let paused = matches!(
            state,
            Some((SinkState::Paused, Some(r), v))
                if r.starts_with(OWNER_PAUSE) && *v == self.definition_ver
        );
        if paused {
            if check.lost(true).await?.is_some() {
                return Ok(Some(Directive::Stop));
            }
            self.store
                .set_state(c, self.sink, SinkState::Running, None)
                .await?;
            tracing::info!(tenant = %check.tenant, sink = self.sink.0, "sink resumed: its owner can read its source again");
            return Ok(Some(Directive::Run));
        }
        if !matches!(state, Some((SinkState::Running | SinkState::Failing, _, _))) {
            return Ok(None);
        }
        let Some(why) = check.lost(false).await? else {
            return Ok(None);
        };
        let reason = owner_pause_reason(&why);
        self.store
            .set_state(c, self.sink, SinkState::Paused, Some(&reason))
            .await?;
        self.store
            .report_task(
                c,
                self.sink,
                self.task,
                self.epoch,
                TaskState::Paused,
                Some(&reason),
                0,
            )
            .await?;
        tracing::warn!(tenant = %check.tenant, sink = self.sink.0, reason = %why, code = 39229, "SprigSinkPaused: the sink's owner lost access to its source");
        Ok(Some(Directive::Stop))
    }
}

#[async_trait]
impl Control for CatalogControl {
    async fn poll(&self) -> Result<Directive> {
        let mut c = self.conn().await?;
        let state = self.store.state_of(&mut c, self.sink).await?;
        // Chapter 51 (39229): the owner's access, before anything runs.
        if let Some(d) = self.owner_directive(&mut c, state.as_ref()).await? {
            return Ok(d);
        }
        if let Some(d) = stop_directive(state.as_ref(), self.definition_ver) {
            return Ok(d);
        }
        // RL31: row-level security now binds the owner (checked each poll,
        // so before each batch is cut).
        if self.rls_bound(&mut c).await? {
            return Ok(Directive::Stop);
        }
        if let Some(g) = &self.regroup
            && g.moved().await
        {
            tracing::info!(
                sink = self.sink.0,
                task = self.task.0,
                of = g.of,
                "fan-in sink regrouped: task stops"
            );
            return Ok(Directive::Stop);
        }
        Ok(match state {
            Some((SinkState::Paused, r, _))
                if r.as_deref().is_some_and(|r| r.starts_with("user")) =>
            {
                Directive::Pause
            }
            _ => Directive::Run,
        })
    }

    async fn report(&self, ev: Event) -> Result<()> {
        let mut c = self.conn().await?;
        match ev {
            Event::Delivered(n) => {
                // At most every few seconds (the runtime batches reports).
                let total = self.delivered.fetch_add(n, Ordering::Relaxed) + n;
                let _ = total;
                self.store
                    .report_task(
                        &mut c,
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Running,
                        None,
                        n,
                    )
                    .await?;
            }
            Event::Failing(why) => {
                self.store
                    .set_state(&mut c, self.sink, SinkState::Failing, Some(&why))
                    .await?;
                self.store
                    .report_task(
                        &mut c,
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Retrying,
                        Some(&why),
                        0,
                    )
                    .await?;
            }
            Event::PausedByFailure(why) => {
                tracing::warn!(sink = self.sink.0, reason = %why, "SprigSinkPaused: paused after failures (39222)");
                self.store
                    .set_state(
                        &mut c,
                        self.sink,
                        SinkState::Paused,
                        Some(&format!("failures (39222): {why}")),
                    )
                    .await?;
                self.store
                    .report_task(
                        &mut c,
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Paused,
                        Some(&why),
                        0,
                    )
                    .await?;
            }
            Event::Recovered => {
                self.store
                    .set_state(&mut c, self.sink, SinkState::Running, None)
                    .await?;
                self.store
                    .report_task(
                        &mut c,
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Running,
                        None,
                        0,
                    )
                    .await?;
            }
            Event::SourceLost(why) => {
                self.store
                    .set_state(
                        &mut c,
                        self.sink,
                        SinkState::Failed,
                        Some(&format!("39223: {why}")),
                    )
                    .await?;
                self.store
                    .report_task(
                        &mut c,
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Failed,
                        Some(&why),
                        0,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_epoch(&self) -> Result<()> {
        let mut c = self.conn().await?;
        match self.store.task_epoch(&mut c, self.sink, self.task).await? {
            Some(e) if e == self.epoch => Ok(()),
            _ => Err(err(
                SprigCode::SinkFenced,
                format!(
                    "task {} of sink {} has a newer epoch",
                    self.task.0, self.sink.0
                ),
            )),
        }
    }
}

/// Opens one connection: options, credentials, destination rules (also
/// chapter 52's approval webhooks through a tenant's connection).
///
/// # Errors
/// 39210 for a connection that is gone; 39216 for credentials that don't
/// open; 39232 for a platform connection not granted to the tenant.
pub async fn open_conn(
    env: &dyn SinkEnv,
    tenant: TenantId,
    catalog: &mut mysql_async::Conn,
    store: &SinksStore,
    r: &ConnRef,
    opts: &Opts,
) -> Result<Conn> {
    let allow = parse_allowlist(opts.get("destination_allowlist").unwrap_or("[]"))?;
    let metadata_allowed = opts.get("platform_identity") == Some("full");
    match r {
        ConnRef::Tenant { id, name } => {
            let def = store.connection(catalog, *id).await?.ok_or_else(|| {
                err(
                    SprigCode::SinkNotFound,
                    format!("connection {name} is gone"),
                )
            })?;
            let mut secrets = MemSecrets::default();
            let sealed = store
                .sealed_secrets(catalog, *id, opts.u64("credential_previous_hours", 24)?)
                .await?;
            if !sealed.is_empty() {
                let keys = env.keyring().ok_or_else(|| {
                    err(
                        SprigCode::SinkCredentialsInvalid,
                        "this worker has no sink credential key file",
                    )
                })?;
                for (n, v, kv, s, previous) in sealed {
                    let value = crate::secrets::open(keys.as_ref(), tenant, id.0, &n, v, kv, &s)?;
                    if previous {
                        secrets.set_previous(&n, value);
                    } else {
                        secrets.set(&n, value);
                    }
                }
            }
            Ok(Conn {
                name: def.name.clone(),
                kind: def.kind,
                options: def.options.clone(),
                secrets: Arc::new(secrets),
                dest: Arc::new(Destinations {
                    allow,
                    residency: env.residency_of(tenant).await?,
                    platform_hosts: None,
                    metadata_allowed,
                }),
            })
        }
        ConnRef::Platform { name } => {
            let global = env.global().ok_or_else(|| {
                err(
                    SprigCode::SinkPlatformConnectionNotGranted,
                    "no global tier here",
                )
            })?;
            let p = crate::meta::platform_get(&global, name)
                .await?
                .ok_or_else(|| {
                    err(
                        SprigCode::SinkNotFound,
                        format!("no platform connection {name}"),
                    )
                })?;
            if !crate::meta::platform_granted(&global, name, tenant.0, &[]).await? {
                metrics::counter!("sprig_sink_platform_connection_opens_total", "result" => "not_granted")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkPlatformConnectionNotGranted,
                    format!("platform connection {name} isn't granted to this tenant"),
                ));
            }
            let dir = env.secret_dir();
            let mut secrets = MemSecrets::default();
            for (n, reference) in &p.secrets {
                match crate::meta::read_secret_file(&dir, reference) {
                    Ok(v) => secrets.set(n, v),
                    Err(e) => {
                        metrics::counter!("sprig_sink_platform_connection_opens_total", "result" => "secret_unavailable")
                            .increment(1);
                        tracing::error!(connection = %name, error = %e, "platform secret unavailable (39234)");
                        return Err(e);
                    }
                }
            }
            for (n, reference) in &p.previous {
                if let Ok(v) = crate::meta::read_secret_file(&dir, reference) {
                    secrets.set_previous(n, v);
                }
            }
            metrics::counter!("sprig_sink_platform_connection_opens_total", "result" => "ok")
                .increment(1);
            let mut options = p.options.clone();
            options.insert("__platform_tags".into(), Value::Object(p.tags.clone()));
            Ok(Conn {
                name: p.name.clone(),
                kind: p.kind,
                options,
                secrets: Arc::new(secrets),
                dest: Arc::new(Destinations {
                    allow: Vec::new(),
                    residency: env.residency_of(tenant).await?,
                    platform_hosts: Some(p.hosts.clone()),
                    metadata_allowed,
                }),
            })
        }
    }
}

/// The sink's `TRANSFORM` function, compiled at task start (its active
/// version then; a later deploy reaches the next task start).
///
/// # Errors
/// 39219 for a function that is gone or can't be a transform.
pub(crate) async fn load_transform(
    pool: &Pool,
    schema: &str,
    def: &SinkDef,
    opts: &Opts,
) -> Result<Option<Arc<crate::transform::Transform>>> {
    let Some(f) = &def.transform else {
        return Ok(None);
    };
    let mut c = pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })?;
    let budget = Duration::from_millis(opts.u64("transform_budget_ms", 50)?);
    // `transform_version = n` pins a version (0 or absent: the active
    // one, followed at each task start); `EXACTLY ONCE` needs it
    // `DETERMINISTIC` (R9, r9fwd).
    let version = opts.u64("transform_version", 0)?;
    crate::transform::Transform::load_at(
        &mut c,
        schema,
        def.db_id,
        f,
        (version > 0).then_some(version),
        def.guarantee == Guarantee::ExactlyOnce,
        budget,
    )
    .await
    .map(Some)
}

/// Builds the target of a sink.
///
/// # Errors
/// The target's.
pub async fn build_target(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    build_target_with(ctx, None).await
}

/// [`build_target`] with a script sink's `db` handle.
///
/// # Errors
/// The target's.
pub async fn build_target_with(
    ctx: &TargetCtx,
    db: Option<Arc<dyn crate::target::script::ReadOnlyDb>>,
) -> Result<Box<dyn Target>> {
    match ctx.sink.target.kind() {
        TargetKind::Webhook => crate::target::webhook::build(ctx),
        TargetKind::Kafka => crate::target::kafka::build(ctx),
        TargetKind::S3 => crate::target::s3::build(ctx),
        TargetKind::Database => crate::target::db::build(ctx).await,
        TargetKind::Script => crate::target::script::build(ctx, db),
    }
}

/// Runs task `task_no` of sink `sink` until it ends.
///
/// # Errors
/// What stops the task for good (the job then fails and the controller
/// starts it again).
pub async fn run_sink_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    cancel: Cancel,
) -> Result<TaskEnd> {
    run_sink_task_of(env, tenant, sink, task_no, 1, cancel).await
}

/// [`run_sink_task`] for task `task_no` of `of` (R9, r9last): a fan-in
/// sink's task reads the member instances [`crate::meta::fanin_task_of`]
/// gives it, and stops when the sink's index entry has another count.
///
/// # Errors
/// As [`run_sink_task`].
#[allow(clippy::too_many_lines)]
pub async fn run_sink_task_of(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    of: u32,
    cancel: Cancel,
) -> Result<TaskEnd> {
    let (cat_pool, cat_schema) = env.catalog(tenant).await?;
    let store = SinksStore::new(cat_schema.clone());
    let mut c = cat_pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })?;
    let Some(def) = store.sink(&mut c, sink).await? else {
        return Ok(TaskEnd::Stopped);
    };
    if matches!(def.state, SinkState::Failed | SinkState::Dropping) {
        return Ok(TaskEnd::Stopped);
    }
    let def = Arc::new(def);
    // `sink.tenant_bytes_per_sec` is one budget across workers (R9).
    crate::runtime::ensure_quota_sharing(env.dc(), env.worker_id());
    let opts = resolve_opts(&mut c, &cat_schema, &def).await?;
    let opts = match env.settings() {
        Some((etcd, dc)) => over_global(opts, &etcd, &dc),
        None => opts,
    };
    crate::policy::check_enabled(
        opts.flag("enabled", true),
        opts.get("allowed_kinds")
            .unwrap_or("webhook,kafka,s3,database,script"),
        def.target.kind(),
    )?;
    let per_task = u32::try_from(opts.u64("partitions_per_task", 4)?).unwrap_or(4);
    if let SourceSpec::FanIn { .. } | SourceSpec::FanInQueue { .. } | SourceSpec::Changes { .. } =
        &def.source
    {
        return run_fan_in_task(env, tenant, sink, task_no, of, cancel, c, store, def, opts).await;
    }
    if let SourceSpec::Queue { .. } = &def.source {
        return run_queue_task(env, tenant, sink, task_no, cancel, c, store, def, opts).await;
    }
    let (stream, partitions_n) = match &def.source {
        SourceSpec::Stream { id, partitions, .. } => (*id, *partitions),
        other => {
            return Err(err(
                SprigCode::SinkOptionInvalid,
                format!("{} sources don't run in workers yet", other.kind()),
            ));
        }
    };
    let k = if def.guarantee == Guarantee::ExactlyOnce {
        1
    } else {
        per_task
    };
    let parts = task_partitions(partitions_n, k, task_no.0);
    // `START AT LATEST` (the default) means from the sink's creation, not
    // from its task's first start: the task starts seconds later under load,
    // and records published in between were skipped (r9fanin; the dead
    // letter step of `fan_in_sink_end_to_end` lost all three).
    let start = instance_start(
        def.start_at,
        0,
        fan_in_created_ms(sink_created_at(&mut c, &cat_schema, sink).await?, false),
    );
    let epoch = store
        .claim_task(
            &mut c,
            sink,
            task_no,
            &json!({"partitions": parts}),
            &env.worker_id(),
        )
        .await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &mut c, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    if let Some(cn) = &conn
        && !def.target.kind().accepts(cn.kind)
    {
        return Err(err(
            SprigCode::SinkOptionInvalid,
            format!(
                "a {} target can't use a {} connection",
                def.target.kind(),
                cn.kind
            ),
        ));
    }
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    // The stream's log shard.
    let shard = env.shard_pool(tenant).await?;
    let (log_schema, change_source) =
        stream_log_schema(&cat_pool, &cat_schema, tenant, stream).await?;
    let source = crate::source::StreamSource::open(
        shard,
        cat_pool.clone(),
        cat_schema.clone(),
        log_schema,
        sprig_streams::StreamId(stream),
        parts,
        def.group(),
        start,
        sink,
    )
    .await?;
    // Chapter 29 (RL44): a change topic's records reach a sink whose owner
    // the table's policies bind only as the owner may see them, filtered
    // batch by batch through the topic's SQL read as the owner.
    let source: Box<dyn crate::runtime::SourceReader> = match change_source {
        Some(object) => Box::new(crate::rls_filter::FilteredChanges {
            inner: Box::new(source),
            env: Arc::clone(&env),
            tenant,
            owner: def.owner_user_id,
            object,
            topic: crate::rls_filter::topic_sql(&cat_pool, &cat_schema, stream).await?,
            run_as: run_as(env.as_ref(), tenant, &def, &opts).await.ok(),
            peer: env.peer(),
            seed: sink.0,
        }),
        None => Box::new(source),
    };
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(&cat_pool, &cat_schema, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        // RL44: `FROM STREAM t.changes` is filtered for its owner (above)
        // instead of paused when t's policies bind the owner (RL36).
        rls: None,
        regroup: None,
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = Arc::new(CatalogDead {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        stream: CatalogDead::target(env.as_ref(), tenant, &cat_pool, &cat_schema, &def).await?,
    });
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, epoch = epoch.0, "sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source,
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            reject_via_source: false,
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "sink task ended");
    end
}

/// Lists and opens a fan-in sink's member instances (chapter 51,
/// "Fan-in sinks"): the DC's `inherit-instances/` index, members below the
/// sink's tenant (and `UNDER t`), members whose residency sets
/// `deny_remote_access` left out.
struct EnvFanIn {
    env: Arc<dyn SinkEnv>,
    tenant: u64,
    def: (u64, u64),
    under: Option<u64>,
    cat_pool: Pool,
    cat_schema: String,
    group: String,
    start: crate::model::StartAt,
    sink: SinkId,
    /// When the sink was created (ms): instances made later start at
    /// their beginning, so the records that made them aren't missed.
    sink_created_ms: u64,
    /// This task's group: (task, of) (R9, r9last).
    task_group: (u32, u32),
}

#[async_trait]
impl crate::source::FanInOpener for EnvFanIn {
    async fn members(&self) -> Result<Vec<(crate::source::FanInMember, Vec<u32>)>> {
        let dc = self.env.dc();
        let prefix = dc.tier().key("inherit-instances/")?;
        let page = dc
            .list(
                &prefix,
                sprig_meta::ListOpts {
                    limit: 1_000_000,
                    ..sprig_meta::ListOpts::default()
                },
            )
            .await?;
        let mut out = Vec::new();
        for kv in &page.kvs {
            let Ok(v) = serde_json::from_slice::<Value>(&kv.value) else {
                continue;
            };
            let def = (
                v.pointer("/def/tenant").and_then(Value::as_u64),
                v.pointer("/def/object").and_then(Value::as_u64),
            );
            if def != (Some(self.def.0), Some(self.def.1))
                || v.get("kind").and_then(Value::as_str) != Some("stream")
            {
                continue;
            }
            let (Some(member), Some(stream)) = (
                v.get("tenant").and_then(Value::as_u64),
                v.get("instance_id").and_then(Value::as_u64),
            ) else {
                continue;
            };
            if crate::meta::fanin_task_of(member, self.task_group.1) != self.task_group.0 {
                continue;
            }
            let Some(global) = self.env.global() else {
                continue;
            };
            let Some(e) = global
                .get(
                    &global.tier().key(&format!("tenants/{member}"))?,
                    sprig_meta::Consistency::Serializable,
                )
                .await?
                .and_then(|kv| serde_json::from_slice::<Value>(&kv.value).ok())
            else {
                continue;
            };
            let path: Vec<u64> = e
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .split('/')
                .filter_map(|x| x.parse().ok())
                .collect();
            if member != self.tenant && !path.contains(&self.tenant) {
                continue;
            }
            if self.under.is_some_and(|u| !path.contains(&u)) {
                continue;
            }
            if e.pointer("/residency/deny_remote_access")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
                    .increment(1);
                continue;
            }
            let name = e
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            out.push((
                crate::source::FanInMember {
                    tenant: member,
                    name,
                    stream,
                },
                Vec::new(),
            ));
        }
        Ok(out)
    }

    async fn open(
        &self,
        m: &crate::source::FanInMember,
        _partitions: Vec<u32>,
    ) -> Result<Box<dyn crate::runtime::SourceReader>> {
        let t = TenantId(m.tenant);
        let (mpool, mschema) = self.env.catalog(t).await?;
        let (log_schema, partitions, created_ms) =
            instance_stream(&mpool, &mschema, t, m.stream).await?;
        let shard = self.env.shard_pool(t).await?;
        let start = instance_start(self.start, created_ms, self.sink_created_ms);
        crate::source::StreamSource::open_instance(
            shard,
            self.cat_pool.clone(),
            self.cat_schema.clone(),
            log_schema,
            m.tenant,
            sprig_streams::StreamId(m.stream),
            (0..partitions).collect(),
            self.group.clone(),
            start,
            self.sink,
        )
        .await
        .map(|s| Box::new(s) as Box<dyn crate::runtime::SourceReader>)
    }
}

/// A queue sink's `ordering` option: `queue` (strict queue order, chapter
/// 51) or `key` (the default: one routing key's messages in order, lanes
/// in parallel).
///
/// # Errors
/// 39218 for another value.
pub fn queue_ordering(opts: &Opts) -> Result<bool> {
    match opts.get("ordering").map(str::to_ascii_lowercase).as_deref() {
        None | Some("key" | "") => Ok(false),
        Some("queue") => Ok(true),
        Some(o) => Err(crate::model::bad_option(format!(
            "ordering = '{o}' isn't known for a queue sink: use 'queue' or 'key'"
        ))),
    }
}

/// When sink `sink` was made (ms since the Unix epoch), from its catalog
/// row.
async fn sink_created_at(
    c: &mut mysql_async::Conn,
    cat_schema: &str,
    sink: SinkId,
) -> Result<Option<i64>> {
    c.exec_first(
        format!(
            "SELECT CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) FROM {}.sinks \
             WHERE sink_id = ?",
            sprig_common::names::quote(cat_schema)
        ),
        (sink.0,),
    )
    .await
    .map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })
}

/// A fan-in sink's creation time as its sources compare it: milliseconds
/// for stream instances, whole seconds for change topics (`changes`).
#[must_use]
pub fn fan_in_created_ms(created_ms: Option<i64>, changes: bool) -> u64 {
    let ms = u64::try_from(created_ms.unwrap_or(0)).unwrap_or(0);
    if changes { ms / 1000 * 1000 } else { ms }
}

/// Where a fan-in sink starts reading a member's stream instance made at
/// `instance_created_ms`, for a sink made at `sink_created_ms` (both in
/// milliseconds). An instance made after the sink is read from its start;
/// `LATEST` means from the sink's creation (not from the task's first
/// start, which comes a moment later).
#[must_use]
pub fn instance_start(
    start: crate::model::StartAt,
    instance_created_ms: u64,
    sink_created_ms: u64,
) -> crate::model::StartAt {
    if instance_created_ms > sink_created_ms {
        crate::model::StartAt::Earliest
    } else if start == crate::model::StartAt::Latest && sink_created_ms > 0 {
        crate::model::StartAt::Timestamp {
            ms: i64::try_from(sink_created_ms).unwrap_or(i64::MAX),
        }
    } else {
        start
    }
}

/// `CHANGES OF t, ...`: the tables' change topics (`t.changes`, chapter
/// 45) of the sink's own tenant, read as one source.
struct EnvChanges {
    env: Arc<dyn SinkEnv>,
    tenant: u64,
    objects: Vec<u64>,
    cat_pool: Pool,
    cat_schema: String,
    group: String,
    start: crate::model::StartAt,
    sink: SinkId,
    sink_created_ms: u64,
    /// Chapter 29 (RLS round 5, RL50): the sink's owner, whose view of
    /// each change topic the records are filtered to while the table's
    /// policies bind it.
    owner: u32,
    /// The owner on a gateway (the filter's reads).
    run_as: Option<crate::peer::RunAs>,
}

#[async_trait]
impl crate::source::FanInOpener for EnvChanges {
    async fn members(&self) -> Result<Vec<(crate::source::FanInMember, Vec<u32>)>> {
        let mut c = self.cat_pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
        let st = sprig_streams::StreamsStore::new(&self.cat_schema)
            .load(&mut c)
            .await?;
        let mut out = Vec::new();
        for o in &self.objects {
            let Some(s) = st.all().find(|s| {
                s.kind == sprig_streams::model::StreamKind::Changes
                    && s.source_object.is_some_and(|x| x.0 == *o)
            }) else {
                return Err(err(
                    SprigCode::SinkSourceLost,
                    format!(
                        "table object {o} has no change topic (ALTER TABLE ... SPRIG SET \
                         (stream.change_topic = on))"
                    ),
                ));
            };
            out.push((
                crate::source::FanInMember {
                    tenant: self.tenant,
                    name: String::new(),
                    stream: s.id.0,
                },
                Vec::new(),
            ));
        }
        Ok(out)
    }

    async fn open(
        &self,
        m: &crate::source::FanInMember,
        _partitions: Vec<u32>,
    ) -> Result<Box<dyn crate::runtime::SourceReader>> {
        let t = TenantId(self.tenant);
        let (log_schema, partitions, _) =
            instance_stream(&self.cat_pool, &self.cat_schema, t, m.stream).await?;
        let shard = self.env.shard_pool(t).await?;
        let start = if self.start == crate::model::StartAt::Latest && self.sink_created_ms > 0 {
            crate::model::StartAt::Timestamp {
                ms: i64::try_from(self.sink_created_ms).unwrap_or(i64::MAX),
            }
        } else {
            self.start
        };
        let src = crate::source::StreamSource::open(
            shard,
            self.cat_pool.clone(),
            self.cat_schema.clone(),
            log_schema,
            sprig_streams::StreamId(m.stream),
            (0..partitions).collect(),
            self.group.clone(),
            start,
            self.sink,
        )
        .await?;
        // Chapter 29 (RLS round 5, RL50): each table's change topic is
        // filtered batch by batch for the owner while the table's policies
        // bind it, as `FROM STREAM t.changes` is (RL44); before, the sink
        // was paused (RL14, RL25, RL31).
        let (topic, object) =
            crate::rls_filter::topic_of(&self.cat_pool, &self.cat_schema, m.stream).await?;
        let object = object.ok_or_else(|| {
            err(
                SprigCode::SinkSourceLost,
                format!("stream {} isn't a change topic", m.stream),
            )
        })?;
        Ok(Box::new(crate::rls_filter::FilteredChanges {
            inner: Box::new(src),
            env: Arc::clone(&self.env),
            tenant: t,
            owner: self.owner,
            object,
            topic,
            run_as: self.run_as.clone(),
            peer: self.env.peer(),
            seed: self.sink.0,
        }))
    }
}

/// A fan-in sink's task (one task reads every member instance in reach,
/// [`EnvFanIn`]), or a `CHANGES OF` sink's ([`EnvChanges`]).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_fan_in_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    of: u32,
    cancel: Cancel,
    mut c: mysql_async::Conn,
    store: SinksStore,
    def: Arc<SinkDef>,
    opts: Opts,
) -> Result<TaskEnd> {
    let (cat_pool, cat_schema) = env.catalog(tenant).await?;
    let created = sink_created_at(&mut c, &cat_schema, sink).await?;
    // R9 (r9last): a fan-in queue task takes its members' messages under
    // a lease tag, as a queue task does (r9tests), and gives back a dead
    // predecessor's in each member it opens.
    let me = env.worker_id();
    let (my_lease, release) = if matches!(def.source, SourceSpec::FanInQueue { .. }) {
        let my_lease = worker_lease(env.dc().as_ref(), &me).await;
        let prev = store.task_claim(&mut c, sink, task_no).await?;
        let prev_reg = match &prev {
            Some((_, Some(w), _)) => Some(worker_lease(env.dc().as_ref(), w).await),
            _ => None,
        };
        (
            my_lease,
            dead_lease_tag(sink, task_no, prev.as_ref(), prev_reg),
        )
    } else {
        (None, None)
    };
    let epoch = store
        .claim_task(
            &mut c,
            sink,
            task_no,
            &json!({
                "source": def.source.kind().as_str(),
                "multi": def.source.name(),
                "of": of.max(1),
                "worker_lease": my_lease,
            }),
            &me,
        )
        .await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &mut c, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    let refresh = Duration::from_secs(opts.u64("fanin_refresh_seconds", 30)?.max(1));
    // Stream instances compare in milliseconds (their records and their
    // creation carry ms): whole seconds made an instance created earlier in
    // the sink's creation second look newer than the sink, and it was read
    // from its start (r9fanin). Change topics' records carry their commit
    // time in whole seconds, so a record committed in the sink's creation
    // second counts there.
    let sink_created_ms = fan_in_created_ms(created, false);
    let sink_created_s_ms = fan_in_created_ms(created, true);
    let source = match &def.source {
        SourceSpec::FanIn {
            def_tenant,
            def_object,
            under,
            ..
        } => {
            crate::source::FanInSource::open(
                Box::new(EnvFanIn {
                    env: Arc::clone(&env),
                    tenant: tenant.0,
                    def: (*def_tenant, *def_object),
                    under: *under,
                    cat_pool: cat_pool.clone(),
                    cat_schema: cat_schema.clone(),
                    group: def.group(),
                    start: def.start_at,
                    sink,
                    sink_created_ms,
                    task_group: (task_no.0, of.max(1)),
                }),
                refresh,
            )
            .await?
        }
        SourceSpec::FanInQueue {
            def_tenant,
            def_object,
            under,
            db,
            name,
        } => {
            let peer = env.peer().ok_or_else(|| {
                err(
                    SprigCode::SinkOptionInvalid,
                    "queue sources need the DC's gateways, and this worker reaches none",
                )
            })?;
            crate::source::FanInSource::open(
                Box::new(EnvFanInQueue {
                    env: Arc::clone(&env),
                    peer,
                    run_as: run_as(env.as_ref(), tenant, &def, &opts).await?,
                    tenant: tenant.0,
                    def: (*def_tenant, *def_object),
                    under: *under,
                    db: db.clone(),
                    queue: name.clone(),
                    lease_ms: opts.u64("queue_lease_ms", 300_000)?.max(1000),
                    seed: sink.0,
                    task_group: (task_no.0, of.max(1)),
                    tag: lease_tag(sink, task_no, epoch),
                    release: release.clone(),
                }),
                refresh,
            )
            .await?
        }
        SourceSpec::Changes { objects } => {
            crate::source::FanInSource::open_changes(Box::new(EnvChanges {
                env: Arc::clone(&env),
                tenant: tenant.0,
                objects: objects.iter().map(|o| o.0).collect(),
                cat_pool: cat_pool.clone(),
                cat_schema: cat_schema.clone(),
                group: def.group(),
                start: def.start_at,
                sink,
                sink_created_ms: sink_created_s_ms,
                owner: def.owner_user_id,
                run_as: run_as(env.as_ref(), tenant, &def, &opts).await.ok(),
            }))
            .await?
        }
        _ => return Ok(TaskEnd::Stopped),
    };
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(&cat_pool, &cat_schema, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        // RL50: a `CHANGES OF` sink is filtered for its owner (EnvChanges)
        // instead of paused when its tables' policies bind the owner (RL31).
        rls: None,
        regroup: match &def.source {
            SourceSpec::FanIn { .. } | SourceSpec::FanInQueue { .. } => Some(Regroup {
                dc: env.dc(),
                tenant: tenant.0,
                sink: sink.0,
                of: of.max(1),
                next: std::sync::Mutex::new(None),
            }),
            _ => None,
        },
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = Arc::new(CatalogDead {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        stream: CatalogDead::target(env.as_ref(), tenant, &cat_pool, &cat_schema, &def).await?,
    });
    let reject_via_source =
        matches!(def.source, SourceSpec::FanInQueue { .. }) && def.dead_letter.is_none();
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, members = source.members().len(), "fan-in sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source: Box::new(source),
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            reject_via_source,
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "fan-in sink task ended");
    end
}

/// Lists and opens a fan-in queue sink's member instances (chapter 51,
/// "Fan-in sinks"; R9, r9queue): the DC's `inherit-instances/` index of
/// the queue definition, members below the sink's tenant (and `UNDER t`),
/// members denying remote access left out; each read through a gateway
/// as the sink's owner (`FanInGet`).
struct EnvFanInQueue {
    env: Arc<dyn SinkEnv>,
    peer: Arc<dyn crate::peer::SinkPeer>,
    run_as: crate::peer::RunAs,
    tenant: u64,
    def: (u64, u64),
    under: Option<u64>,
    db: String,
    queue: String,
    lease_ms: u64,
    seed: u64,
    /// This task's group: (task, of) (R9, r9last).
    task_group: (u32, u32),
    /// This task's lease tag, and a dead predecessor's to give back in
    /// each member (R9, r9last).
    tag: String,
    release: Option<String>,
}

#[async_trait]
impl crate::source::FanInOpener for EnvFanInQueue {
    async fn members(&self) -> Result<Vec<(crate::source::FanInMember, Vec<u32>)>> {
        let dc = self.env.dc();
        let prefix = dc.tier().key("inherit-instances/")?;
        let page = dc
            .list(
                &prefix,
                sprig_meta::ListOpts {
                    limit: 1_000_000,
                    ..sprig_meta::ListOpts::default()
                },
            )
            .await?;
        let Some(global) = self.env.global() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for kv in &page.kvs {
            let Ok(v) = serde_json::from_slice::<Value>(&kv.value) else {
                continue;
            };
            let def = (
                v.pointer("/def/tenant").and_then(Value::as_u64),
                v.pointer("/def/object").and_then(Value::as_u64),
            );
            if def != (Some(self.def.0), Some(self.def.1))
                || v.get("kind").and_then(Value::as_str) != Some("queue")
            {
                continue;
            }
            let (Some(member), Some(queue)) = (
                v.get("tenant").and_then(Value::as_u64),
                v.get("instance_id").and_then(Value::as_u64),
            ) else {
                continue;
            };
            if crate::meta::fanin_task_of(member, self.task_group.1) != self.task_group.0 {
                continue;
            }
            let Some(e) = global
                .get(
                    &global.tier().key(&format!("tenants/{member}"))?,
                    sprig_meta::Consistency::Serializable,
                )
                .await?
                .and_then(|kv| serde_json::from_slice::<Value>(&kv.value).ok())
            else {
                continue;
            };
            let path: Vec<u64> = e
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .split('/')
                .filter_map(|x| x.parse().ok())
                .collect();
            if member == self.tenant || !path.contains(&self.tenant) {
                continue;
            }
            if self.under.is_some_and(|u| !path.contains(&u)) {
                continue;
            }
            if e.pointer("/residency/deny_remote_access")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
                    .increment(1);
                continue;
            }
            let name = e
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            out.push((
                crate::source::FanInMember {
                    tenant: member,
                    name,
                    stream: queue,
                },
                Vec::new(),
            ));
        }
        Ok(out)
    }

    async fn open(
        &self,
        m: &crate::source::FanInMember,
        _partitions: Vec<u32>,
    ) -> Result<Box<dyn crate::runtime::SourceReader>> {
        Ok(Box::new(
            crate::source::QueueSource::new(
                Arc::clone(&self.peer),
                self.run_as.clone(),
                crate::source::QueueAt::Member {
                    def: self.def,
                    member: m.tenant,
                    db: self.db.clone(),
                },
                self.queue.clone(),
                self.lease_ms,
                self.seed ^ m.tenant,
            )
            .with_lease_tag(self.tag.clone(), self.release.clone()),
        ))
    }
}

/// The lease tag a queue sink's task takes its messages under (R9,
/// r9tests): `mq_get`'s fifth argument, one per task claim.
#[must_use]
pub fn lease_tag(sink: SinkId, task_no: TaskNo, epoch: TaskEpoch) -> String {
    format!("sink/{}/{}/{}", sink.0, task_no.0, epoch.0)
}

/// The lease of worker `worker`'s registration (`workers/<id>` in the DC
/// tier), which identifies the process now running under that id; `None`
/// when it isn't registered. An error reads as `Some(-1)`: never taken
/// for a process that is gone.
async fn worker_lease(dc: &dyn MetaStore, worker: &str) -> Option<i64> {
    let Ok(key) = dc.tier().key(&format!("workers/{worker}")) else {
        return Some(-1);
    };
    match dc.get(&key, sprig_meta::Consistency::Linearizable).await {
        Ok(kv) => kv.map(|kv| kv.lease.map_or(0, |l| l.0)),
        Err(e) => {
            tracing::debug!(worker, error = %e, "can't read a worker's registration");
            Some(-1)
        }
    }
}

/// The lease tag of a queue task's predecessor whose worker process is
/// gone (R9, r9tests), so the new task gives its messages back at once:
/// the predecessor recorded its worker's registration lease when it
/// claimed the task, and that worker's registration is now missing
/// (`reg` is `Some(None)`) or bound to another lease (a new process under
/// the same id). `None` when there is no predecessor, it predates lease
/// tags, or its process may still run: its messages then return after
/// their lease, as before.
#[must_use]
pub fn dead_lease_tag(
    sink: SinkId,
    task_no: TaskNo,
    prev: Option<&(TaskEpoch, Option<String>, Value)>,
    reg: Option<Option<i64>>,
) -> Option<String> {
    let (epoch, _, part) = prev?;
    let had = part.get("worker_lease").and_then(Value::as_i64)?;
    let now = reg?;
    if had <= 0 || now == Some(had) || now == Some(-1) {
        return None;
    }
    Some(lease_tag(sink, task_no, *epoch))
}

/// A queue sink's task (R9, r9queue): the sink's own durable queue,
/// consumed through a gateway as its owner (see `source::QueueSource`).
#[allow(clippy::too_many_arguments)]
async fn run_queue_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    cancel: Cancel,
    mut c: mysql_async::Conn,
    store: SinksStore,
    def: Arc<SinkDef>,
    opts: Opts,
) -> Result<TaskEnd> {
    let (cat_pool, cat_schema) = env.catalog(tenant).await?;
    let SourceSpec::Queue { id, name } = &def.source else {
        return Ok(TaskEnd::Stopped);
    };
    let peer = env.peer().ok_or_else(|| {
        err(
            SprigCode::SinkOptionInvalid,
            "queue sources need the DC's gateways, and this worker reaches none",
        )
    })?;
    let who = run_as(env.as_ref(), tenant, &def, &opts).await?;
    // R9 (r9tests): the task's predecessor, and whether its worker process
    // is gone (its etcd registration expired, or a new process took the
    // worker's id): its leased messages are then given back at once.
    let me = env.worker_id();
    let my_lease = worker_lease(env.dc().as_ref(), &me).await;
    let prev = store.task_claim(&mut c, sink, task_no).await?;
    let prev_reg = match &prev {
        Some((_, Some(w), _)) => Some(worker_lease(env.dc().as_ref(), w).await),
        _ => None,
    };
    let release = dead_lease_tag(sink, task_no, prev.as_ref(), prev_reg);
    let epoch = store
        .claim_task(
            &mut c,
            sink,
            task_no,
            &json!({
                "source": "queue",
                "queue": name,
                "worker_lease": my_lease,
            }),
            &me,
        )
        .await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &mut c, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    let source = crate::source::QueueSource::new(
        peer,
        who.clone(),
        crate::source::QueueAt::Own {
            id: *id,
            db: who.database.clone(),
        },
        name.clone(),
        opts.u64("queue_lease_ms", 300_000)?.max(1000),
        sink.0,
    );
    // `ordering = queue` (r9fanin): queue order, one lane, and earlier
    // tasks' leases waited out before reading on.
    let queue_order = queue_ordering(&opts)?;
    if queue_order {
        settings.lanes = 1;
    }
    let source = source
        .with_queue_order(queue_order)
        .with_lease_tag(lease_tag(sink, task_no, epoch), release);
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(&cat_pool, &cat_schema, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        rls: None,
        regroup: None,
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = Arc::new(CatalogDead {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        stream: CatalogDead::target(env.as_ref(), tenant, &cat_pool, &cat_schema, &def).await?,
    });
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, queue = %name, "queue sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source: Box::new(source),
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            // No DEAD LETTER TO: the queue's own dead-lettering (a reject
            // without requeue), as chapter 51 says.
            reject_via_source: def.dead_letter.is_none(),
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "queue sink task ended");
    end
}

/// A member's instance stream: its log shard schema, partitions and
/// creation time (ms).
async fn instance_stream(
    pool: &Pool,
    catalog: &str,
    tenant: TenantId,
    stream: u64,
) -> Result<(String, u32, u64)> {
    let mut c = pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })?;
    let def = sprig_streams::StreamsStore::new(catalog)
        .get(&mut c, sprig_streams::StreamId(stream))
        .await?
        .ok_or_else(|| {
            err(
                SprigCode::SinkSourceLost,
                format!("instance stream {stream} of tenant {tenant} is gone"),
            )
        })?;
    let log = def
        .log
        .first()
        .ok_or_else(|| err(SprigCode::SinkSourceLost, "the instance has no log shard"))?;
    Ok((
        log.slot.schema(tenant, def.db).to_string(),
        def.partitions,
        def.created_at_ms,
    ))
}

/// The log shard schema of stream `stream` (its first log shard).
/// A stream's log schema, and its table's object id when it is a change
/// topic.
pub(crate) async fn stream_log_schema(
    pool: &Pool,
    catalog: &str,
    tenant: TenantId,
    stream: u64,
) -> Result<(String, Option<u64>)> {
    let mut c = pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })?;
    let def = sprig_streams::StreamsStore::new(catalog)
        .get(&mut c, sprig_streams::StreamId(stream))
        .await?
        .ok_or_else(|| {
            err(
                SprigCode::SinkSourceLost,
                format!("stream {stream} is gone"),
            )
        })?;
    let log = def
        .log
        .first()
        .ok_or_else(|| err(SprigCode::SinkSourceLost, "the stream has no log shard"))?;
    let source = (def.kind == sprig_streams::model::StreamKind::Changes)
        .then(|| def.source_object.map(|o| o.0))
        .flatten();
    Ok((log.slot.schema(tenant, def.db).to_string(), source))
}

/// Kinds of connection each target needs (for messages).
#[must_use]
pub fn needs(kind: TargetKind) -> &'static [ConnKind] {
    match kind {
        TargetKind::Webhook => &[ConnKind::Webhook],
        TargetKind::Kafka => &[ConnKind::Kafka],
        TargetKind::S3 => &[ConnKind::S3],
        TargetKind::Database => &[ConnKind::Mysql, ConnKind::Postgresql],
        TargetKind::Script => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_reads_name_each_source() {
        let mut d =
            crate::probe::stand_in(4, "s", crate::model::TargetSpec::Webhook { path: None });
        d.source = SourceSpec::Stream {
            id: 9,
            name: "orders.changes".into(),
            partitions: 1,
        };
        assert_eq!(
            owner_reads(&d),
            vec![OwnerRead::Named {
                db: 4,
                name: "orders".into()
            }]
        );
        d.source = SourceSpec::Queue {
            id: 3,
            name: "jobs".into(),
        };
        assert_eq!(
            owner_reads(&d),
            vec![OwnerRead::Named {
                db: 4,
                name: "jobs".into()
            }]
        );
        d.source = SourceSpec::Changes {
            objects: vec![(7, "a".into()), (8, "b".into())],
        };
        assert_eq!(
            owner_reads(&d),
            vec![OwnerRead::Object(7), OwnerRead::Object(8)]
        );
        assert!(owner_pause_reason("x").starts_with("39229 ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST: x"));
    }

    #[test]
    fn a_sink_paused_by_row_level_security_stops_its_task() {
        let st = |s: SinkState, r: Option<&str>| (s, r.map(str::to_owned), 3u64);
        // Before RL31 a running task kept delivering (`Run`).
        assert_eq!(
            stop_directive(
                Some(&st(
                    SinkState::Paused,
                    Some(
                        "ER_SPRIG_RLS_UNSUPPORTED: row-level security on 'docs' binds the sink's owner"
                    )
                )),
                3
            ),
            Some(Directive::Stop)
        );
        assert_eq!(
            stop_directive(Some(&st(SinkState::Paused, Some("user"))), 3),
            None
        );
        assert_eq!(stop_directive(Some(&st(SinkState::Running, None)), 3), None);
        assert_eq!(
            stop_directive(Some(&st(SinkState::Running, None)), 4),
            Some(Directive::Stop)
        );
        assert_eq!(stop_directive(None, 3), Some(Directive::Stop));
    }

    #[test]
    fn tasks_split_partitions() {
        assert_eq!(task_partitions(12, 4, 0), vec![0, 1, 2, 3]);
        assert_eq!(task_partitions(10, 4, 2), vec![8, 9]);
        assert!(task_partitions(4, 4, 1).is_empty());
        assert_eq!(unquote("\"[\\\"a\\\"]\""), "[\"a\"]");
        assert_eq!(unquote("12"), "12");
    }

    /// r9fanin: a member's instance made earlier in the second the fan-in
    /// sink was made in is read from the sink's creation under `LATEST`,
    /// not from its start (whole seconds made it look newer than the sink,
    /// and `fan_in_sink_end_to_end` got 5 records from before the sink).
    #[test]
    fn instance_made_in_the_sinks_second_starts_at_the_sink() {
        use crate::model::StartAt;
        let sink = fan_in_created_ms(Some(1_794_000_000_900), false);
        assert_eq!(
            instance_start(StartAt::Latest, 1_794_000_000_300, sink),
            StartAt::Timestamp {
                ms: 1_794_000_000_900
            }
        );
        // An instance made after the sink: from its start.
        assert_eq!(
            instance_start(StartAt::Latest, 1_794_000_000_950, sink),
            StartAt::Earliest
        );
        assert_eq!(
            instance_start(StartAt::Earliest, 1_794_000_000_300, sink),
            StartAt::Earliest
        );
        // `ordering` of queue sinks (r9fanin).
        let o = |v: &str| {
            Opts(
                [("ordering".to_owned(), v.to_owned())]
                    .into_iter()
                    .collect(),
            )
        };
        assert!(queue_ordering(&o("queue")).unwrap());
        assert!(!queue_ordering(&o("key")).unwrap());
        assert!(!queue_ordering(&Opts::default()).unwrap());
        assert!(queue_ordering(&o("partition")).is_err());
    }

    #[test]
    fn a_dead_predecessors_lease_tag() {
        // R9 (r9tests): give back a predecessor's leases only when its
        // worker process is gone.
        let (s, t) = (SinkId(7), TaskNo(0));
        let prev = |lease: Value| {
            (
                TaskEpoch(3),
                Some("wkr-1".to_owned()),
                json!({"source": "queue", "queue": "q", "worker_lease": lease}),
            )
        };
        let p = prev(json!(11));
        assert_eq!(lease_tag(s, t, TaskEpoch(3)), "sink/7/0/3");
        // Registration gone, or a new process under the same id.
        assert_eq!(
            dead_lease_tag(s, t, Some(&p), Some(None)).as_deref(),
            Some("sink/7/0/3")
        );
        assert_eq!(
            dead_lease_tag(s, t, Some(&p), Some(Some(12))).as_deref(),
            Some("sink/7/0/3")
        );
        // The same process still runs, or etcd couldn't say.
        assert_eq!(dead_lease_tag(s, t, Some(&p), Some(Some(11))), None);
        assert_eq!(dead_lease_tag(s, t, Some(&p), Some(Some(-1))), None);
        assert_eq!(dead_lease_tag(s, t, Some(&p), None), None);
        // No predecessor, or one from before lease tags.
        assert_eq!(dead_lease_tag(s, t, None, Some(None)), None);
        let old = (
            TaskEpoch(3),
            Some("wkr-1".to_owned()),
            json!({"source": "queue"}),
        );
        assert_eq!(dead_lease_tag(s, t, Some(&old), Some(None)), None);
        assert_eq!(
            dead_lease_tag(s, t, Some(&prev(Value::Null)), Some(None)),
            None
        );
        // Change topics keep whole seconds.
        assert_eq!(
            fan_in_created_ms(Some(1_794_000_000_900), true),
            1_794_000_000_000
        );
    }
}
