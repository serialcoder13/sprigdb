//! The database targets (chapter 51, "Database target"; build step 13):
//! MySQL and MariaDB through `mysql_async`, PostgreSQL through `sqlx`.
//!
//! ```text
//! deliver(batch b):
//!   plan: each record → upsert | delete | soft delete | append row, in order
//!         (change records: after image, before image for deletes; an update
//!          that changes the primary key is a delete of the old key and an
//!          upsert of the new one; stream and queue records: the JSON object
//!          in the value)
//!   schema: every table and column the plan needs, by schema_policy
//!           (strict → 39224; add_columns → ALTER TABLE ADD; create → CREATE)
//!   one destination transaction:
//!     exactly once, streams and changes: lock the task's offset rows, skip
//!       records at or below the stored position of their partition or shard
//!     exactly once, queues: skip keys already in __sprig_sink_seen
//!     multi-row statements of at most db.batch_rows rows, guarded by
//!       _sprig_ver so a row never moves backwards
//!     exactly once: write b.ends (WHERE epoch <= task epoch; none → 39227),
//!       insert the queue keys seen
//!   commit
//! ```
//!
//! Failures: lost connections, timeouts, lock waits and deadlocks, and
//! server errors that aren't about a row are destination-wide (the runtime
//! backs off); a row the destination refuses (a constraint, a value that
//! doesn't fit) fails the whole batch, which the runtime halves until the
//! failing row is alone and dead-letters it.
//!
//! **Versions** (`_sprig_ver`, 16 bytes, compared as bytes): a stream
//! record's is its offset, big-endian, in the last 8 bytes; a change
//! record's is its commit time (ms, big-endian) then its order in the
//! shard; a queue message's is its time (ms) then the first 8 bytes of
//! SHA-256 of its message id (queues have no order, so the newest message
//! wins).

mod mysql;
mod pg;
pub mod plan;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use sprig_common::{Result, SprigCode};

use crate::model::{
    Batch, BatchOutcome, ConnKind, DestPosition, Guarantee, Outcome, Position, SourceKind,
    TargetSpec, TaskEpoch, bad_option, delivery_failed, err,
};
use crate::runtime::{Target, TaskInfo};
use crate::target::TargetCtx;

pub use plan::{
    Col, DeleteMode, Dialect, Mode, Op, OpKind, SchemaPolicy, Stmt, TableInfo, version_of,
};

/// A failure, classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbErr {
    /// Destination-wide: retry the batch with backoff.
    Retry(String),
    /// A row the destination refuses: the batch fails and is halved.
    Row(String),
    /// The table doesn't fit the records (39224).
    Schema(String),
    /// A newer task owns the position (39227).
    Fenced(String),
}

impl DbErr {
    /// The destination's message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            DbErr::Retry(m) | DbErr::Row(m) | DbErr::Schema(m) | DbErr::Fenced(m) => m,
        }
    }
}

/// The result type of backend calls.
pub type DbResult<T> = std::result::Result<T, DbErr>;

/// A bound value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Param {
    /// `NULL`.
    Null,
    /// Text (PostgreSQL casts it to the column's type).
    Text(String),
    /// Bytes (versions, positions, binary columns).
    Bytes(Vec<u8>),
    /// A whole number.
    Int(i64),
}

/// One destination transaction.
#[async_trait]
pub trait Tx: Send {
    /// Runs a statement; rows affected (found rows on MySQL).
    async fn exec(&mut self, sql: &str, params: Vec<Param>) -> DbResult<u64>;
    /// Reads offset rows: `(sink_key, position, epoch)`.
    async fn offsets(
        &mut self,
        sql: &str,
        params: Vec<Param>,
    ) -> DbResult<Vec<(String, Vec<u8>, u64)>>;
    /// Reads one text column.
    async fn strings(&mut self, sql: &str, params: Vec<Param>) -> DbResult<Vec<String>>;
    /// Commits.
    async fn commit(self: Box<Self>) -> DbResult<()>;
    /// Rolls back (best effort).
    async fn rollback(self: Box<Self>);
}

/// A destination server.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Its dialect.
    fn dialect(&self) -> Dialect;
    /// Starts a transaction.
    async fn begin(&self) -> DbResult<Box<dyn Tx>>;
    /// A table's columns and primary key; `None` when it doesn't exist.
    async fn table(&self, name: &str) -> DbResult<Option<TableInfo>>;
    /// Runs DDL or a maintenance statement outside a transaction.
    async fn exec(&self, sql: &str) -> DbResult<()>;
}

/// The options a database sink runs with.
#[derive(Debug, Clone)]
pub struct DbOpts {
    /// `mode`.
    pub mode: Mode,
    /// `delete_mode`.
    pub delete_mode: DeleteMode,
    /// `schema_policy`.
    pub schema_policy: SchemaPolicy,
    /// `db.batch_rows`.
    pub batch_rows: usize,
    /// `column_map`: source column → destination column.
    pub column_map: BTreeMap<String, String>,
    /// `TABLE '...'`.
    pub table: String,
    /// `primary_key`: columns for `create` when records don't say.
    pub primary_key: Vec<String>,
    /// `db.seen_retention_hours`.
    pub seen_retention: Duration,
    /// The logical database (`{db}`).
    pub db_name: String,
    /// The source's name (`{source}`, and `{table}` without a table
    /// header).
    pub source_name: String,
}

impl DbOpts {
    /// From a target's context.
    ///
    /// # Errors
    /// 39218 for an unknown mode or a bad `column_map`.
    pub fn from_ctx(ctx: &TargetCtx) -> Result<Self> {
        let TargetSpec::Database { table } = &ctx.sink.target else {
            return Err(bad_option("not a database sink"));
        };
        let o = &ctx.opts;
        let mode = Mode::parse(&o.text("mode", "upsert"))?;
        let delete_mode = DeleteMode::parse(&o.text("delete_mode", "delete"))?;
        let schema_policy = SchemaPolicy::parse(&o.text("schema_policy", "strict"))?;
        let column_map = match o.get("column_map") {
            None => BTreeMap::new(),
            Some(t) => {
                let v: Value = serde_json::from_str(t)
                    .map_err(|e| bad_option(format!("column_map must be a JSON object: {e}")))?;
                let Value::Object(m) = v else {
                    return Err(bad_option("column_map must be a JSON object"));
                };
                m.into_iter()
                    .map(|(k, v)| match v {
                        Value::String(s) => Ok((k, s)),
                        _ => Err(bad_option("column_map values must be column names")),
                    })
                    .collect::<Result<_>>()?
            }
        };
        let batch_rows =
            usize::try_from(o.u64("db.batch_rows", 500)?.clamp(1, 10_000)).unwrap_or(500);
        let primary_key = o
            .get("primary_key")
            .map(|s| {
                s.split(',')
                    .map(|c| c.trim().to_owned())
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            mode,
            delete_mode,
            schema_policy,
            batch_rows,
            column_map,
            table: table.clone(),
            primary_key,
            seen_retention: Duration::from_secs(o.u64("db.seen_retention_hours", 168)? * 3600),
            db_name: ctx.db_name.clone(),
            source_name: ctx.sink.source.name(),
        })
    }
}

/// The destination-side tables of exactly-once delivery.
pub const OFFSETS: &str = "__sprig_sink_offsets";
/// Queue sources' seen keys.
pub const SEEN: &str = "__sprig_sink_seen";

/// A database target over a backend.
pub struct DbTarget {
    backend: Arc<dyn Backend>,
    opts: DbOpts,
    exactly_once: bool,
    queue_source: bool,
    task: Mutex<Option<TaskInfo>>,
    tables: Mutex<HashMap<String, Arc<TableInfo>>>,
    last_purge: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for DbTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbTarget")
            .field("dialect", &self.backend.dialect())
            .field("opts", &self.opts)
            .field("exactly_once", &self.exactly_once)
            .finish_non_exhaustive()
    }
}

/// `TEST CONNECTION` on a MySQL, MariaDB or PostgreSQL connection (R9,
/// r9tests): open the checked address, log in, run `SELECT 1`; with
/// `table`, also read that table's columns. Nothing is written.
pub async fn probe(
    ctx: &TargetCtx,
    table: Option<&str>,
) -> Vec<(&'static str, std::result::Result<String, String>)> {
    let mut out = Vec::new();
    let conn = match ctx.conn() {
        Ok(c) => c,
        Err(e) => {
            out.push(("options", Err(e.to_string())));
            return out;
        }
    };
    let backend: Arc<dyn Backend> = match conn.kind {
        ConnKind::Mysql => match mysql::MyBackend::connect(conn, ctx).await {
            Ok(b) => Arc::new(b),
            Err(e) => {
                out.push(("options", Err(e.to_string())));
                return out;
            }
        },
        ConnKind::Postgresql => match pg::PgBackend::connect(conn, ctx).await {
            Ok(b) => Arc::new(b),
            Err(e) => {
                out.push(("options", Err(e.to_string())));
                return out;
            }
        },
        other => {
            out.push((
                "options",
                Err(format!("not a database connection: {other}")),
            ));
            return out;
        }
    };
    out.push(("options", Ok(format!("{} connection", conn.kind))));
    match backend.exec("SELECT 1").await {
        Ok(()) => out.push(("query", Ok("logged in; SELECT 1 answered".into()))),
        Err(e) => {
            out.push(("query", Err(e.message().to_owned())));
            return out;
        }
    }
    if let Some(t) = table {
        out.push((
            "table",
            match backend.table(t).await {
                Ok(Some(info)) => Ok(format!("{t}: {} column(s)", info.cols.len())),
                Ok(None) => Err(format!("{t} doesn't exist")),
                Err(e) => Err(e.message().to_owned()),
            },
        ));
    }
    out
}

/// `TEST CONNECTION c WRITE TO 'table'`'s write step (R9, r9last): `INSERT
/// INTO table SELECT * FROM table WHERE 1 = 0` in a transaction rolled
/// back, so the server checks the `INSERT` (and `SELECT`) right on the
/// table and no row is written.
pub async fn write_probe(
    ctx: &TargetCtx,
    table: &str,
) -> Vec<(&'static str, std::result::Result<String, String>)> {
    let conn = match ctx.conn() {
        Ok(c) => c,
        Err(e) => return vec![("write", Err(e.to_string()))],
    };
    let backend: Arc<dyn Backend> = match conn.kind {
        ConnKind::Mysql => match mysql::MyBackend::connect(conn, ctx).await {
            Ok(b) => Arc::new(b),
            Err(e) => return vec![("write", Err(e.to_string()))],
        },
        ConnKind::Postgresql => match pg::PgBackend::connect(conn, ctx).await {
            Ok(b) => Arc::new(b),
            Err(e) => return vec![("write", Err(e.to_string()))],
        },
        other => return vec![("write", Err(format!("not a database connection: {other}")))],
    };
    let t = plan::table_ident(backend.dialect(), table);
    let mut tx = match backend.begin().await {
        Ok(tx) => tx,
        Err(e) => return vec![("write", Err(e.message().to_owned()))],
    };
    let r = tx
        .exec(
            &format!("INSERT INTO {t} SELECT * FROM {t} WHERE 1 = 0"),
            Vec::new(),
        )
        .await;
    tx.rollback().await;
    vec![
        (
            "write",
            r.map(|_| format!("INSERT on {table} allowed (checked with no row, rolled back)"))
                .map_err(|e| e.message().to_owned()),
        ),
        ("clean_up", Ok("nothing written".into())),
    ]
}

/// Builds the database target of `ctx` (`MYSQL` or `POSTGRESQL`
/// connection).
///
/// # Errors
/// 39218 for bad options; 39216 for missing credentials; 39213 or 39214
/// for a destination the rules refuse.
pub async fn build(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    let conn = ctx.conn()?;
    let backend: Arc<dyn Backend> = match conn.kind {
        ConnKind::Mysql => Arc::new(mysql::MyBackend::connect(conn, ctx).await?),
        ConnKind::Postgresql => Arc::new(pg::PgBackend::connect(conn, ctx).await?),
        other => {
            return Err(bad_option(format!(
                "a database sink needs a mysql or postgresql connection, not {other}"
            )));
        }
    };
    Ok(Box::new(DbTarget::new(backend, ctx)?))
}

impl DbTarget {
    /// A target over `backend`.
    ///
    /// # Errors
    /// 39218 for bad options.
    pub fn new(backend: Arc<dyn Backend>, ctx: &TargetCtx) -> Result<Self> {
        Ok(Self {
            backend,
            opts: DbOpts::from_ctx(ctx)?,
            exactly_once: ctx.sink.guarantee == Guarantee::ExactlyOnce,
            queue_source: seen_keyed(&ctx.sink.source),
            task: Mutex::new(Some(ctx.task.clone())),
            tables: Mutex::new(HashMap::new()),
            last_purge: Mutex::new(None),
        })
    }

    fn dialect(&self) -> Dialect {
        self.backend.dialect()
    }

    fn task(&self) -> Option<TaskInfo> {
        self.task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn cached(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    async fn ensure_eo_tables(&self) -> DbResult<()> {
        let d = self.dialect();
        self.backend.exec(&plan::offsets_ddl(d)).await?;
        if self.queue_source {
            self.backend.exec(&plan::seen_ddl(d)).await?;
        }
        Ok(())
    }

    /// Makes every table and column the plan needs exist, by the schema
    /// policy, and returns their descriptions.
    async fn ensure_tables(&self, ops: &[Op]) -> DbResult<HashMap<String, Arc<TableInfo>>> {
        let mut out: HashMap<String, Arc<TableInfo>> = HashMap::new();
        let d = self.dialect();
        let mut wanted: BTreeMap<String, Vec<&Op>> = BTreeMap::new();
        for op in ops {
            wanted.entry(op.table.clone()).or_default().push(op);
        }
        for (name, ops) in wanted {
            let mut info = match self.cached(&name) {
                Some(i) => Some(i),
                None => self.backend.table(&name).await?.map(Arc::new),
            };
            let needed = plan::needed_columns(&ops, &self.opts);
            if info.is_none() {
                if self.opts.schema_policy != SchemaPolicy::Create {
                    return Err(DbErr::Schema(format!(
                        "table {name} doesn't exist (schema_policy = {})",
                        self.opts.schema_policy.as_str()
                    )));
                }
                let pk = plan::create_key(&ops, &self.opts);
                let sql = plan::create_table_sql(d, &name, &needed, &pk, &self.opts)
                    .map_err(DbErr::Schema)?;
                self.backend.exec(&sql).await?;
                info = self.backend.table(&name).await?.map(Arc::new);
            }
            let Some(mut t) = info else {
                return Err(DbErr::Schema(format!(
                    "table {name} wasn't found after creating it"
                )));
            };
            let missing: Vec<&(String, Value)> =
                needed.iter().filter(|(c, _)| t.col(c).is_none()).collect();
            if !missing.is_empty() {
                let only_sprig = missing.iter().all(|(c, _)| c.starts_with("_sprig_"));
                if self.opts.schema_policy == SchemaPolicy::Strict && !only_sprig {
                    let names: Vec<&str> = missing.iter().map(|(c, _)| c.as_str()).collect();
                    return Err(DbErr::Schema(format!(
                        "table {name} has no column {} (schema_policy = strict)",
                        names.join(", ")
                    )));
                }
                if self.opts.schema_policy == SchemaPolicy::Strict {
                    let names: Vec<&str> = missing.iter().map(|(c, _)| c.as_str()).collect();
                    return Err(DbErr::Schema(format!(
                        "table {name} lacks the sink's column {} (schema_policy = strict; add it, or use add_columns)",
                        names.join(", ")
                    )));
                }
                for (c, sample) in missing {
                    let sql = plan::add_column_sql(d, &name, c, sample);
                    if let Err(e) = self.backend.exec(&sql).await {
                        // Another lane may have added it meanwhile.
                        tracing::debug!(error = e.message(), "add column");
                    }
                }
                t = self
                    .backend
                    .table(&name)
                    .await?
                    .map(Arc::new)
                    .ok_or_else(|| DbErr::Schema(format!("table {name} disappeared")))?;
                if let Some((c, _)) = needed.iter().find(|(c, _)| t.col(c).is_none()) {
                    return Err(DbErr::Schema(format!(
                        "column {c} couldn't be added to {name}"
                    )));
                }
            }
            if self.opts.mode == Mode::Upsert && t.pk.is_empty() {
                return Err(DbErr::Schema(format!(
                    "table {name} has no primary key, which upsert mode needs"
                )));
            }
            self.tables
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(name.clone(), Arc::clone(&t));
            out.insert(name, t);
        }
        Ok(out)
    }

    fn forget_tables(&self) {
        self.tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    async fn purge_seen(&self, task: &TaskInfo) {
        let due = {
            let mut l = self
                .last_purge
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let due = l.is_none_or(|t| t.elapsed() >= Duration::from_secs(3600));
            if due {
                *l = Some(Instant::now());
            }
            due
        };
        if due {
            let sql =
                plan::purge_seen_sql(self.dialect(), &seen_key(task), self.opts.seen_retention);
            if let Err(e) = self.backend.exec(&sql).await {
                tracing::warn!(error = e.message(), "seen keys not purged");
            }
        }
    }

    #[allow(clippy::too_many_lines)] // the transaction, step by step
    async fn deliver_inner(&self, b: &Batch) -> DbResult<()> {
        let d = self.dialect();
        let task = self.task();
        let mut ops = plan::plan(&b.records, &self.opts, d)?;
        let tables = match self.ensure_tables(&ops).await {
            Ok(t) => t,
            Err(e) => {
                self.forget_tables();
                return Err(e);
            }
        };
        let mut tx = self.backend.begin().await?;
        let r: DbResult<()> = async {
            let eo = self.exactly_once && task.is_some();
            let mut skip: HashSet<usize> = HashSet::new();
            let mut stored: HashMap<String, (Vec<u8>, u64)> = HashMap::new();
            if let (true, Some(t)) = (eo, &task) {
                if self.queue_source {
                    let keys: Vec<String> = b.records.iter().map(|r| r.idem_key.clone()).collect();
                    if !keys.is_empty() {
                        let (sql, params) = plan::seen_lock_sql(d, &seen_key(t), &keys);
                        let seen: HashSet<String> =
                            tx.strings(&sql, params).await?.into_iter().collect();
                        for (i, r) in b.records.iter().enumerate() {
                            if seen.contains(&r.idem_key) {
                                skip.insert(i);
                            }
                        }
                    }
                } else {
                    let units: Vec<String> = units_of(&b.records)
                        .into_iter()
                        .map(|u| unit_key(t, u))
                        .collect();
                    if !units.is_empty() {
                        let (sql, params) = plan::offsets_lock_sql(d, &units);
                        for (k, pos, epoch) in tx.offsets(&sql, params).await? {
                            if epoch > t.epoch.0 {
                                return Err(DbErr::Fenced(format!(
                                    "task epoch {} is older than the destination's {epoch} for {k}",
                                    t.epoch.0
                                )));
                            }
                            stored.insert(k, (pos, epoch));
                        }
                        for (i, r) in b.records.iter().enumerate() {
                            let Some((u, seq)) = r.pos.unit_seq() else {
                                continue;
                            };
                            let Some((pos, _)) = stored.get(&unit_key(t, u)) else {
                                continue;
                            };
                            if Position::decode(pos)
                                .and_then(|p| p.unit_seq())
                                .is_some_and(|(_, done)| seq <= done)
                            {
                                skip.insert(i);
                            }
                        }
                    }
                }
            }
            ops.retain(|o| !skip.contains(&o.rec));
            for st in plan::statements(d, &ops, &tables, &self.opts)? {
                tx.exec(&st.sql, st.params).await?;
            }
            if let (true, Some(t)) = (eo, &task) {
                if self.queue_source {
                    let fresh: Vec<String> = b
                        .records
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !skip.contains(i))
                        .map(|(_, r)| r.idem_key.clone())
                        .collect();
                    if !fresh.is_empty() {
                        let (sql, params) = plan::seen_insert_sql(d, &seen_key(t), &fresh);
                        tx.exec(&sql, params).await?;
                    }
                } else {
                    for end in &b.ends {
                        let Some((u, _)) = end.unit_seq() else {
                            continue;
                        };
                        let k = unit_key(t, u);
                        let epoch = i64::try_from(t.epoch.0).unwrap_or(i64::MAX);
                        let n = if stored.contains_key(&k) {
                            let (sql, params) = plan::offsets_update_sql(d, &k, end, epoch);
                            tx.exec(&sql, params).await?
                        } else {
                            let (sql, params) = plan::offsets_insert_sql(d, &k, end, epoch);
                            tx.exec(&sql, params).await?
                        };
                        if n == 0 {
                            return Err(DbErr::Fenced(format!("a newer task owns {k}")));
                        }
                    }
                }
            }
            Ok(())
        }
        .await;
        match r {
            Ok(()) => tx.commit().await?,
            Err(e) => {
                tx.rollback().await;
                return Err(e);
            }
        }
        if self.exactly_once
            && self.queue_source
            && let Some(t) = &task
        {
            self.purge_seen(t).await;
        }
        Ok(())
    }
}

/// `<tenant>/<sink>/<task>/<unit>`.
fn unit_key(t: &TaskInfo, unit: u64) -> String {
    format!("{}/{unit}", t.sink_key())
}

/// Whether exactly once into this target goes by `__sprig_sink_seen`
/// (each record's idempotency key, kept for `db.seen_retention_hours`)
/// rather than by positions per partition: queues (no order to keep a
/// position in), and fan-in sources (R9, r9tests), whose records carry
/// the member in a stable idempotency key but whose positions are a
/// task's own slots, which a destination row can't name across restarts.
#[must_use]
pub fn seen_keyed(source: &crate::model::SourceSpec) -> bool {
    use crate::model::SourceSpec as S;
    source.kind() == SourceKind::Queue || matches!(source, S::FanIn { .. } | S::FanInQueue { .. })
}

/// `<tenant>/<sink>`: queue consumers of one sink compete, so seen keys
/// are the sink's, not a task's.
fn seen_key(t: &TaskInfo) -> String {
    format!("{}/{}", t.tenant, t.sink)
}

fn units_of(records: &[crate::model::SinkRecord]) -> Vec<u64> {
    let mut u: Vec<u64> = records
        .iter()
        .filter_map(|r| r.pos.unit_seq().map(|x| x.0))
        .collect();
    u.sort_unstable();
    u.dedup();
    u
}

#[async_trait]
impl Target for DbTarget {
    fn kind(&self) -> &'static str {
        "database"
    }

    async fn open(&mut self, task: &TaskInfo) -> Result<Vec<DestPosition>> {
        *self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task.clone());
        if !self.exactly_once {
            return Ok(Vec::new());
        }
        self.ensure_eo_tables().await.map_err(to_error)?;
        if self.queue_source {
            return Ok(Vec::new());
        }
        let d = self.dialect();
        let epoch = i64::try_from(task.epoch.0).unwrap_or(i64::MAX);
        let prefix = format!("{}/", task.sink_key());
        let mut tx = self.backend.begin().await.map_err(to_error)?;
        let r: DbResult<Vec<(String, Vec<u8>, u64)>> = async {
            // Fence older writers: they find a newer epoch on their rows.
            let (sql, params) = plan::offsets_fence_sql(d, &prefix, epoch);
            tx.exec(&sql, params).await?;
            let (sql, params) = plan::offsets_prefix_sql(d, &prefix);
            tx.offsets(&sql, params).await
        }
        .await;
        let rows = match r {
            Ok(rows) => {
                tx.commit().await.map_err(to_error)?;
                rows
            }
            Err(e) => {
                tx.rollback().await;
                return Err(to_error(e));
            }
        };
        let mut out = Vec::new();
        for (k, pos, e) in rows {
            if e > task.epoch.0 {
                return Err(to_error(DbErr::Fenced(format!(
                    "{k} is held by epoch {e}, newer than {}",
                    task.epoch.0
                ))));
            }
            if let Some(p) = Position::decode(&pos) {
                out.push(DestPosition {
                    pos: p,
                    epoch: TaskEpoch(e),
                });
            }
        }
        Ok(out)
    }

    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome> {
        match self.deliver_inner(b).await {
            Ok(()) => Ok(BatchOutcome::All(Outcome::Ok)),
            Err(DbErr::Row(reason)) => Ok(BatchOutcome::All(Outcome::Fail { reason })),
            Err(e) => Err(to_error(e)),
        }
    }

    fn exactly_once(&self) -> bool {
        self.exactly_once
    }
}

fn to_error(e: DbErr) -> sprig_common::SprigError {
    match e {
        DbErr::Retry(m) | DbErr::Row(m) => delivery_failed(m),
        DbErr::Schema(m) => {
            let mut x = err(SprigCode::SinkSchemaMismatch, m);
            x.retryable = true;
            x
        }
        DbErr::Fenced(m) => err(SprigCode::SinkFenced, m),
    }
}

#[cfg(test)]
mod tests;
