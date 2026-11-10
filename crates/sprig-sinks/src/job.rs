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
}

#[async_trait]
impl DeadLetters for CatalogDead {
    async fn write(&self, rec: &crate::model::SinkRecord, info: &DeadInfo) -> Result<()> {
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
}

impl CatalogControl {
    async fn conn(&self) -> Result<mysql_async::Conn> {
        self.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })
    }
}

#[async_trait]
impl Control for CatalogControl {
    async fn poll(&self) -> Result<Directive> {
        let mut c = self.conn().await?;
        Ok(match self.store.state_of(&mut c, self.sink).await? {
            None => Directive::Stop,
            Some((_, _, v)) if v != self.definition_ver => Directive::Stop,
            Some((SinkState::Failed | SinkState::Dropping, _, _)) => Directive::Stop,
            Some((SinkState::Paused, r, _))
                if r.as_deref().is_some_and(|r| r.starts_with("user")) =>
            {
                Directive::Pause
            }
            Some(_) => Directive::Run,
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

/// Opens one connection: options, credentials, destination rules.
async fn open_conn(
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
                    residency: env.residency(tenant),
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
                    residency: env.residency(tenant),
                    platform_hosts: Some(p.hosts.clone()),
                    metadata_allowed,
                }),
            })
        }
    }
}

/// Builds the target of a sink.
///
/// # Errors
/// The target's.
pub async fn build_target(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    match ctx.sink.target.kind() {
        TargetKind::Webhook => crate::target::webhook::build(ctx),
        TargetKind::Kafka => crate::target::kafka::build(ctx),
        TargetKind::S3 => crate::target::s3::build(ctx),
        TargetKind::Database => crate::target::db::build(ctx).await,
        TargetKind::Script => crate::target::script::build(ctx, None),
    }
}

/// Runs task `task_no` of sink `sink` until it ends.
///
/// # Errors
/// What stops the task for good (the job then fails and the controller
/// starts it again).
#[allow(clippy::too_many_lines)]
pub async fn run_sink_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
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
    let target = build_target(&ctx).await?;
    // The stream's log shard.
    let shard = env.shard_pool(tenant).await?;
    let log_schema = stream_log_schema(&cat_pool, &cat_schema, tenant, stream).await?;
    let source = crate::source::StreamSource::open(
        shard,
        cat_pool.clone(),
        cat_schema.clone(),
        log_schema,
        sprig_streams::StreamId(stream),
        parts,
        def.group(),
        def.start_at,
        sink,
    )
    .await?;
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
    };
    let control = Arc::new(CatalogControl {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
    });
    let dead = Arc::new(CatalogDead {
        pool: cat_pool.clone(),
        store: store.clone(),
        sink,
    });
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, epoch = epoch.0, "sink task started");
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
            reject_via_source: false,
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "sink task ended");
    end
}

/// The log shard schema of stream `stream` (its first log shard).
async fn stream_log_schema(
    pool: &Pool,
    catalog: &str,
    tenant: TenantId,
    stream: u64,
) -> Result<String> {
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
    Ok(log.slot.schema(tenant, def.db).to_string())
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
    fn tasks_split_partitions() {
        assert_eq!(task_partitions(12, 4, 0), vec![0, 1, 2, 3]);
        assert_eq!(task_partitions(10, 4, 2), vec![8, 9]);
        assert!(task_partitions(4, 4, 1).is_empty());
        assert_eq!(unquote("\"[\\\"a\\\"]\""), "[\"a\"]");
        assert_eq!(unquote("12"), "12");
    }
}
