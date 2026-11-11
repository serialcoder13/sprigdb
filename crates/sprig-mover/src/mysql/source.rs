//! [`SourceIo`] on a MariaDB cluster's writer (chapter 10, "Preparing",
//! "The watermark copy", "Cutover"; chapter 09, "Fencing primitives").

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Params, Pool, Row as MyRow};
use sprig_binlog::GtidPos;
use sprig_cluster::fence::{
    self, FenceMethod, FenceOptions, FenceReceipt, FenceRequest, FenceScope, RenamedTable,
    fenced_name,
};
use sprig_common::ids::{JobId, TenantId};
use sprig_common::names::{INTERNAL_OBJECT_PREFIX, quote};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OnceCell;

use super::sql::{self, qualified};
use super::stream::MysqlStream;
use super::{ClusterAccess, db_err};
use crate::io::{ChangeStream, Counter, Drained, Fenced, Load, PreparedDrain, SourceIo};
use crate::model::{ColumnMeta, Row, RowDigest, TableMeta, Value, Watermark};

/// The names lookup of chapter 07 step 12, as
/// `sprig_sql::names::stored::setup_sql` creates it (idempotent).
const NAMES_SETUP: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_names` (`name` VARCHAR(80) NOT NULL PRIMARY KEY, \
     `logical` VARCHAR(255) NOT NULL, `account` VARCHAR(400) NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
     COLLATE=utf8mb4_bin",
    "CREATE OR REPLACE SQL SECURITY DEFINER VIEW `__sprig_sys`.`__sprig_names_v` AS SELECT `name`, `logical`, \
     `account` FROM `__sprig_sys`.`__sprig_names` WHERE LEFT(`name`, LOCATE('_', `name`)) = \
     LEFT(SESSION_USER(), LOCATE('_', SESSION_USER()))",
    "GRANT SELECT ON `__sprig_sys`.`__sprig_names_v` TO PUBLIC",
];

/// A view's statements for the target: `USE`, then its `CREATE OR
/// REPLACE`. A view reading chapter 07's names lookup needs the lookup on
/// the target before MariaDB accepts it (the gateway writes its rows
/// there), so the first such view brings [`NAMES_SETUP`].
fn push_view(out: &mut Vec<String>, schema: &str, create: &str) {
    if create.contains("__sprig_names_v") && !out.iter().any(|x| x == NAMES_SETUP[0]) {
        out.extend(NAMES_SETUP.iter().map(|x| (*x).to_owned()));
    }
    out.push(format!("USE {}", quote(schema)));
    out.push(create.replacen("CREATE ", "CREATE OR REPLACE ", 1));
}

/// Session settings of the source pool: values as stored, times in UTC.
const SESSION: &[&str] = &["SET NAMES binary", "SET SESSION time_zone = '+00:00'"];

/// The source side of one job.
pub struct MysqlSource {
    access: ClusterAccess,
    pool: Pool,
    job: JobId,
    tenant: TenantId,
    spec_schemas: Vec<String>,
    spec_tables: Option<Vec<String>>,
    server_id: u32,
    swap: bool,
    schemas: OnceCell<Vec<String>>,
    sys_ready: OnceCell<()>,
    /// The tables last listed, `(schema, table)`: a fence's planned receipt
    /// and counters map fenced names back with them.
    known: Mutex<Vec<(String, String)>>,
    /// Chapter 18's resolver, which finishes the tenant's prepared
    /// branches at cutover; `None` leaves them to block the fence.
    drain: Option<Arc<dyn PreparedDrain>>,
    /// A re-key's source (chapter 16): digests leave `_sprig_rk` out.
    rekey: bool,
}

impl std::fmt::Debug for MysqlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSource")
            .field("cluster", &self.access.cluster)
            .field("job", &self.job)
            .finish_non_exhaustive()
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::NULL => String::new(),
        other => format!("{other:?}"),
    }
}

fn digest_of(t: &TableMeta, r: MyRow) -> RowDigest {
    let vals: Vec<Value> = r.unwrap();
    let n = t.pk.len();
    let key_values: Vec<Value> = vals.iter().take(n).cloned().collect();
    let hash = match vals.get(n) {
        Some(Value::UInt(u)) => *u,
        Some(Value::Int(i)) => u64::from_ne_bytes(i.to_ne_bytes()),
        Some(Value::Bytes(b)) => String::from_utf8_lossy(b).parse().unwrap_or(0),
        _ => 0,
    };
    RowDigest {
        key: TableMeta::key_of_values(&key_values),
        key_values,
        hash,
    }
}

/// Internal tables that move with their shard: chapter 41's
/// exactly-once markers of after-commit triggers (`__sprig_ldone`), which
/// must stay in the same schema as the writes they commit with, and
/// chapter 50's queue tables (messages, owner state, delayed messages and
/// the queue outbox), and chapter 42's cross-tenant outbox
/// (`__sprig_xo`), whose rows commit with the shard's writes; and the
/// queue forward ledger (`__sprig_qf`, R9 r9fwd), which commits with the
/// owners' enqueues.
pub const MOVED_INTERNAL: &[&str] = &[
    "__sprig_ldone",
    "__sprig_qm",
    "__sprig_qs",
    "__sprig_qd",
    "__sprig_qo",
    "__sprig_qf",
    "__sprig_xo",
];

/// Prefixes of internal tables that move with their shard like user
/// tables: chapter 45's stream outboxes (`__sprig_eo_<stream>`), stream
/// logs (`__sprig_sl_<stream>`) and their checkpoints (`__sprig_sl_ckpt`),
/// whose rows commit with the shard's writes and the sequencer's position;
/// and chapter 43's native graph tables: nodes (`__sprig_gn_<graph>`),
/// edges (`__sprig_ge_`), label rows (`__sprig_gl_`) and mirrors
/// (`__sprig_gm_`), routed by `_sprig_rk` like a group's tables, and the id
/// counter (`__sprig_gq_`, copied whole with its default-group schema),
/// and the uniqueness claims (`__sprig_gu_`, routed by the claim's hash);
/// and chapter 33's key-value element tables (`__sprig_ke_<ns>`,
/// `__sprig_kz_<ns>`), grouped with their key table on `_sprig_dk`, every
/// row carrying its key's `_sprig_rk`; and the engines' tables in each
/// logical database's default-group schema, which commit with the
/// database's writes: chapter 31's document engine (`__sprig_doc_`:
/// retryable-write records and transaction outcomes, search index
/// definitions, vector side tables), chapter 40's scripts
/// (`__sprig_redis_`), chapter 35's models (`__sprig_dim_`) and chapter
/// 42's broadcast logs and retry lists (`__sprig_ib_`, `__sprig_ibr_`).
/// Chapter 42's materialized copies (`__sprig_ih_`) stay behind: a target
/// without them reads through and seeds them again.
pub const MOVED_INTERNAL_PREFIXES: &[&str] = &[
    "__sprig_doc_",
    "__sprig_redis_",
    "__sprig_dim_",
    "__sprig_ib_",
    "__sprig_ibr_",
    "__sprig_ke_",
    "__sprig_kz_",
    "__sprig_eo_",
    "__sprig_sl_",
    "__sprig_gn_",
    "__sprig_ge_",
    "__sprig_gl_",
    "__sprig_gm_",
    "__sprig_gq_",
    "__sprig_gu_",
];

/// `SET NAMES` for a stored program's `character_set_client` and
/// `collation_connection` (`SHOW CREATE` columns 3 and 4), so the target
/// parses its string literals as the source did; the copy session is
/// otherwise `SET NAMES binary`, which turns them into binary strings
/// (and `'x' COLLATE utf8mb4_bin` into error 1253).
fn set_names(charset: Option<String>, collation: Option<String>) -> Vec<String> {
    let ok = |x: &str| !x.is_empty() && x.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    match (charset, collation) {
        (Some(c), Some(k)) if ok(&c) && ok(&k) && k.starts_with(&c) => {
            vec![format!("SET NAMES {c} COLLATE {k}")]
        }
        (Some(c), _) if ok(&c) => vec![format!("SET NAMES {c}")],
        _ => Vec::new(),
    }
}

/// `body` between its `SET NAMES` and the copy session's own.
fn in_names(out: &mut Vec<String>, names: Vec<String>, body: String) {
    let back = !names.is_empty();
    out.extend(names);
    out.push(body);
    if back {
        out.push("SET NAMES binary".to_owned());
    }
}

/// Whether a table name is one of the mover's or a fence's internal names
/// (the tables of [`MOVED_INTERNAL`] are copied like any other).
fn internal(name: &str) -> bool {
    name.starts_with(INTERNAL_OBJECT_PREFIX)
        && !MOVED_INTERNAL.contains(&name)
        && !MOVED_INTERNAL_PREFIXES.iter().any(|p| name.starts_with(p))
}

impl MysqlSource {
    /// The source of `job` on `access`.
    #[must_use]
    pub fn new(
        access: ClusterAccess,
        job: JobId,
        tenant: TenantId,
        spec_schemas: Vec<String>,
        spec_tables: Option<Vec<String>>,
        server_id: u32,
        swap: bool,
    ) -> Self {
        let pool = access.pool(SESSION, 4);
        Self {
            access,
            pool,
            job,
            tenant,
            spec_schemas,
            spec_tables,
            server_id,
            swap,
            schemas: OnceCell::new(),
            sys_ready: OnceCell::new(),
            known: Mutex::new(Vec::new()),
            drain: None,
            rekey: false,
        }
    }

    /// Finishes the tenant's prepared distributed-transaction branches
    /// through `drain` at cutover (chapter 18's `drain_prepared`).
    #[must_use]
    pub fn with_drain(mut self, drain: Option<Arc<dyn PreparedDrain>>) -> Self {
        self.drain = drain;
        self
    }

    /// The source of a re-key (chapter 16): verification digests leave
    /// `_sprig_rk` out, since the re-key replaces it.
    #[must_use]
    pub fn with_rekey(mut self, on: bool) -> Self {
        self.rekey = on;
        self
    }

    /// The cluster.
    #[must_use]
    pub fn access(&self) -> &ClusterAccess {
        &self.access
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        self.pool
            .get_conn()
            .await
            .map_err(|e| db_err(&format!("connect to {}", self.access.writer.addr()), &e))
    }

    async fn rows(&self, q: &str, params: Vec<Value>) -> Result<Vec<MyRow>> {
        let mut c = self.conn().await?;
        let p = if params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(params)
        };
        c.exec(q, p).await.map_err(|e| db_err("source read", &e))
    }

    fn known(&self) -> Vec<(String, String)> {
        self.known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn table_names(&self, schema: &str) -> Result<Vec<String>> {
        let rows = self
            .rows(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED') ORDER BY TABLE_NAME",
                vec![Value::Bytes(schema.as_bytes().to_vec())],
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| r.get::<String, _>(0))
            .filter(|n| !internal(n))
            .filter(|n| self.spec_tables.as_ref().is_none_or(|ts| ts.contains(n)))
            .collect())
    }

    async fn table(&self, schema: &str, name: &str) -> Result<TableMeta> {
        let p = || {
            vec![
                Value::Bytes(schema.as_bytes().to_vec()),
                Value::Bytes(name.as_bytes().to_vec()),
            ]
        };
        let cols = self
            .rows(
                "SELECT COLUMN_NAME, DATA_TYPE, EXTRA FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                p(),
            )
            .await?;
        let columns: Vec<ColumnMeta> = cols
            .into_iter()
            .map(|r| {
                let v: Vec<Value> = r.unwrap();
                let name = v.first().map(text).unwrap_or_default();
                let ty = v.get(1).map(text).unwrap_or_default().to_ascii_lowercase();
                let extra = v.get(2).map(text).unwrap_or_default().to_ascii_uppercase();
                ColumnMeta {
                    name,
                    generated: extra.contains("GENERATED"),
                    numeric: matches!(ty.as_str(), "enum" | "set" | "bit"),
                }
            })
            .collect();
        let keys = self
            .rows(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX",
                p(),
            )
            .await?;
        let pk = keys
            .into_iter()
            .filter_map(|r| r.get::<String, _>(0))
            .filter_map(|k| columns.iter().position(|c| c.name.eq_ignore_ascii_case(&k)))
            .collect();
        let create = self
            .rows(
                &format!("SHOW CREATE TABLE {}", qualified(schema, name)),
                Vec::new(),
            )
            .await?;
        let create_sql = create
            .into_iter()
            .next()
            .and_then(|r| r.get::<String, _>(1))
            .unwrap_or_default();
        Ok(TableMeta {
            schema: schema.to_owned(),
            name: name.to_owned(),
            columns,
            pk,
            create_sql,
        })
    }

    async fn ensure_sys(&self) -> Result<()> {
        self.sys_ready
            .get_or_try_init(|| async {
                let mut c = self.conn().await?;
                c.query_drop("CREATE DATABASE IF NOT EXISTS `__sprig_sys`")
                    .await
                    .map_err(|e| db_err("create __sprig_sys", &e))?;
                c.query_drop(sql::WATERMARK_DDL)
                    .await
                    .map_err(|e| db_err("create the watermark table", &e))?;
                Ok::<(), SprigError>(())
            })
            .await?;
        Ok(())
    }

    fn fence_request(&self, schemas: Vec<String>, deadline: Duration) -> FenceRequest {
        FenceRequest {
            tenant: self.tenant,
            schemas,
            tables: self.spec_tables.clone(),
            scope: FenceScope::Writes,
            method: FenceMethod::Rename,
            deadline: Instant::now() + deadline,
        }
    }

    /// Statements that create schema `s`'s events (RL12, r2harden): as the
    /// source has them (`as_source`, the cutover's; `CREATE OR REPLACE`),
    /// or disabled (a copy's start). Each runs under the event's own
    /// `sql_mode`, time zone and character set.
    async fn event_statements(&self, s: &str, as_source: bool) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let names = self
            .rows(
                "SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?",
                vec![Value::Bytes(s.as_bytes().to_vec())],
            )
            .await?;
        for n in names.into_iter().filter_map(|r| r.get::<String, _>(0)) {
            let rows = self
                .rows(
                    &format!("SHOW CREATE EVENT {}", qualified(s, &n)),
                    Vec::new(),
                )
                .await?;
            let Some(r) = rows.into_iter().next() else {
                continue;
            };
            // Event, sql_mode, time_zone, Create Event, character_set_client,
            // collation_connection, Database Collation.
            let mode: Option<String> = r.get(1);
            let tz: Option<String> = r.get(2);
            let Some(body) = r.get::<String, _>(3) else {
                continue;
            };
            let body = if as_source {
                body.replacen("CREATE ", "CREATE OR REPLACE ", 1)
            } else {
                body.replacen(" ENABLE DO ", " DISABLE DO ", 1)
            };
            out.push(format!("USE {}", quote(s)));
            out.push(format!(
                "SET SESSION sql_mode = '{}'",
                mode.unwrap_or_default().replace('\'', "")
            ));
            out.push(format!(
                "SET SESSION time_zone = '{}'",
                tz.unwrap_or_else(|| "SYSTEM".to_owned()).replace('\'', "")
            ));
            if !as_source {
                out.push(format!("DROP EVENT IF EXISTS {}", quote(&n)));
            }
            in_names(&mut out, set_names(r.get(4), r.get(5)), body);
            if !as_source {
                out.push(format!("ALTER EVENT {} DISABLE", quote(&n)));
            }
            out.push("SET SESSION time_zone = '+00:00'".to_owned());
        }
        Ok(out)
    }

    /// Creation statements of the schemas' views and routines, and the
    /// tenant's backend accounts with their grants (until chapter 29's
    /// reconciler creates them on the target).
    #[allow(clippy::too_many_lines)]
    async fn object_statements(&self, schemas: &[String]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for s in schemas {
            let r = self
                .rows(&format!("SHOW CREATE DATABASE {}", quote(s)), Vec::new())
                .await?;
            if let Some(create) = r.into_iter().next().and_then(|r| r.get::<String, _>(1)) {
                out.push(create.replacen("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1));
            }
        }
        if self.swap {
            return Ok(out);
        }
        for s in schemas {
            // Chapter 18: every shard schema anchors the decisions of the
            // coordinated transactions that start on it, so each new shard
            // schema (a move, split, merge, re-key or standby, on any
            // cluster) gets `__sprig_dtx` before it serves (G31, decision
            // bugfix-2). Its rows aren't copied.
            if matches!(
                s.parse::<sprig_common::names::SchemaName>(),
                Ok(sprig_common::names::SchemaName::Shard { .. })
            ) {
                out.push(format!("USE {}", quote(s)));
                out.push(sprig_catalog::ddl::dtx_table_sql(s));
            }
            let p = vec![Value::Bytes(s.as_bytes().to_vec())];
            let views = self
                .rows(
                    "SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?",
                    p.clone(),
                )
                .await?;
            for v in views.into_iter().filter_map(|r| r.get::<String, _>(0)) {
                let r = self
                    .rows(
                        &format!("SHOW CREATE VIEW {}", qualified(s, &v)),
                        Vec::new(),
                    )
                    .await?;
                if let Some(c) = r.into_iter().next().and_then(|r| r.get::<String, _>(1)) {
                    push_view(&mut out, s, &c);
                }
            }
            let routines = self
                .rows(
                    "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES \
                     WHERE ROUTINE_SCHEMA = ?",
                    p,
                )
                .await?;
            for r in routines {
                let (Some(name), Some(kind)) = (r.get::<String, _>(0), r.get::<String, _>(1))
                else {
                    continue;
                };
                let rows = self
                    .rows(
                        &format!("SHOW CREATE {kind} {}", qualified(s, &name)),
                        Vec::new(),
                    )
                    .await?;
                if let Some(row) = rows.into_iter().next() {
                    let mode: Option<String> = row.get(1);
                    let body: Option<String> = row.get(2);
                    if let Some(body) = body {
                        out.push(format!("USE {}", quote(s)));
                        out.push(format!(
                            "SET SESSION sql_mode = '{}'",
                            mode.unwrap_or_default().replace('\'', "")
                        ));
                        out.push(format!("DROP {kind} IF EXISTS {}", quote(&name)));
                        // The routine keeps the character set it was
                        // created with: its literals are parsed in it.
                        in_names(&mut out, set_names(row.get(3), row.get(4)), body);
                    }
                }
            }
            // Chapter 10, prepare step 3 (RL12): events, disabled until the
            // cutover (or a standby's promotion) enables them.
            out.extend(self.event_statements(s, false).await?);
        }
        // The tenant's backend accounts and their grants.
        let like = format!("t{}\\_%", self.tenant);
        let accounts = self
            .rows(
                "SELECT DISTINCT User, Host FROM mysql.db WHERE User LIKE ?",
                vec![Value::Bytes(like.into_bytes())],
            )
            .await
            .unwrap_or_default();
        for a in accounts {
            let (Some(u), Some(h)) = (a.get::<String, _>(0), a.get::<String, _>(1)) else {
                continue;
            };
            if h.is_empty() {
                continue; // roles: chapter 29's reconciler
            }
            let who = format!("'{}'@'{}'", u.replace('\'', ""), h.replace('\'', ""));
            if let Ok(r) = self
                .rows(&format!("SHOW CREATE USER {who}"), Vec::new())
                .await
                && let Some(c) = r.into_iter().next().and_then(|r| r.get::<String, _>(0))
            {
                out.push(c.replacen("CREATE USER", "CREATE USER IF NOT EXISTS", 1));
            }
            if let Ok(r) = self
                .rows(&format!("SHOW GRANTS FOR {who}"), Vec::new())
                .await
            {
                out.extend(r.into_iter().filter_map(|r| r.get::<String, _>(0)));
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl SourceIo for MysqlSource {
    async fn schemas(&self) -> Result<Vec<String>> {
        let s = self
            .schemas
            .get_or_try_init(|| async {
                if !self.spec_schemas.is_empty() {
                    return Ok::<_, SprigError>(self.spec_schemas.clone());
                }
                let rows = self.rows("SHOW DATABASES", Vec::new()).await?;
                let names: Vec<String> = rows.into_iter().filter_map(|r| r.get(0)).collect();
                Ok(sprig_shard::tenant_schemas(
                    self.tenant,
                    names.iter().map(String::as_str),
                ))
            })
            .await?;
        if s.is_empty() {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!(
                    "tenant {} has no schemas on cluster {}",
                    self.tenant, self.access.cluster
                ),
            ));
        }
        Ok(s.clone())
    }

    async fn tables(&self) -> Result<Vec<TableMeta>> {
        let mut out = Vec::new();
        for s in self.schemas().await? {
            for t in self.table_names(&s).await? {
                out.push(self.table(&s, &t).await?);
            }
        }
        // Remember every name seen, so a fence planned after a table was
        // renamed still knows it.
        let mut k = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        for t in &out {
            let id = (t.schema.clone(), t.name.clone());
            if !k.contains(&id) {
                k.push(id);
            }
        }
        Ok(out)
    }

    async fn objects(&self) -> Result<Vec<String>> {
        let schemas = self.schemas().await?;
        self.object_statements(&schemas).await
    }

    async fn triggers(&self) -> Result<Vec<String>> {
        if self.swap {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for s in self.schemas().await? {
            let names = self
                .rows(
                    "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?",
                    vec![Value::Bytes(s.as_bytes().to_vec())],
                )
                .await?;
            for n in names.into_iter().filter_map(|r| r.get::<String, _>(0)) {
                let rows = self
                    .rows(
                        &format!("SHOW CREATE TRIGGER {}", qualified(&s, &n)),
                        Vec::new(),
                    )
                    .await?;
                if let Some(r) = rows.into_iter().next() {
                    let mode: Option<String> = r.get(1);
                    if let Some(body) = r.get::<String, _>(2) {
                        out.push(format!("USE {}", quote(&s)));
                        out.push(format!(
                            "SET SESSION sql_mode = '{}'",
                            mode.unwrap_or_default().replace('\'', "")
                        ));
                        in_names(&mut out, set_names(r.get(3), r.get(4)), body);
                    }
                }
            }
            // Step 6's `ALTER EVENT ... ENABLE` (RL12): each event as the
            // source has it, enabled or not, replacing the disabled copy.
            out.extend(self.event_statements(&s, true).await?);
        }
        Ok(out)
    }

    async fn binlog_pos(&self) -> Result<GtidPos> {
        let mut c = self.conn().await?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| db_err("read @@gtid_binlog_pos", &e))?;
        let p: GtidPos = p.unwrap_or_default().parse()?;
        Ok(self.access.trim(p))
    }

    async fn open_stream(
        &self,
        from: &GtidPos,
        tables: Arc<Vec<Arc<TableMeta>>>,
    ) -> Result<Box<dyn ChangeStream>> {
        let schemas = self.schemas().await?;
        // On Galera the job's position keeps only the cluster's domain. A
        // node's own domains (writes made with wsrep off) must still be in
        // the connect state, or the node would send their whole history:
        // start them where the node is now; they never carry tenant rows.
        let mut from = from.clone();
        if let Some(d) = self.access.domain {
            let mut c = self.conn().await?;
            let p: Option<String> = c
                .query_first("SELECT @@gtid_binlog_pos")
                .await
                .map_err(|e| db_err("read @@gtid_binlog_pos", &e))?;
            let now: GtidPos = p.unwrap_or_default().parse()?;
            for g in now.iter().filter(|g| g.domain != d) {
                from.update(*g);
            }
        }
        let s = MysqlStream::connect(
            &self.access.repl,
            &from,
            self.server_id,
            self.job,
            schemas,
            &tables,
            self.access.domain,
        )
        .await?;
        Ok(Box::new(s))
    }

    async fn write_watermark(&self, wm: &Watermark) -> Result<()> {
        self.ensure_sys().await?;
        let mut c = self.conn().await?;
        c.exec_drop(
            sql::WATERMARK_UPSERT,
            Params::Positional(vec![
                Value::Bytes(wm.job.to_bytes().to_vec()),
                Value::UInt(u64::from(wm.stream)),
                Value::Bytes(wm.kind.as_str().as_bytes().to_vec()),
                Value::UInt(wm.seq),
            ]),
        )
        .await
        .map_err(|e| db_err("write a watermark", &e))
    }

    async fn read_chunk(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let (q, p) = sql::chunk(t, after, limit);
        Ok(self
            .rows(&q, p)
            .await?
            .into_iter()
            .map(MyRow::unwrap)
            .collect())
    }

    async fn digests(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        limit: usize,
    ) -> Result<Vec<RowDigest>> {
        let bare;
        let t = if self.rekey {
            bare = crate::route::ShardRoute::without_rk(t);
            &bare
        } else {
            t
        };
        let (q, p) = sql::digests(t, after, limit);
        Ok(self
            .rows(&q, p)
            .await?
            .into_iter()
            .map(|r| digest_of(t, r))
            .collect())
    }

    async fn read_range(&self, t: &TableMeta, after: Option<&Row>, last: &Row) -> Result<Vec<Row>> {
        let (q, p) = sql::range(t, &t.name, &sql::select_list(t), after, last);
        Ok(self
            .rows(&q, p)
            .await?
            .into_iter()
            .map(MyRow::unwrap)
            .collect())
    }

    async fn plan_fence(&self) -> Result<serde_json::Value> {
        let schemas = self.schemas().await?;
        let renamed: Vec<RenamedTable> = self
            .known()
            .into_iter()
            .filter(|(s, _)| schemas.contains(s))
            .map(|(schema, from)| RenamedTable {
                to: fenced_name(&from),
                schema,
                from,
            })
            .collect();
        let r = FenceReceipt {
            tenant: self.tenant,
            method: FenceMethod::Rename,
            scope: FenceScope::Writes,
            writer: self.access.writer.node.clone(),
            revoked: Vec::new(),
            renamed,
            killed: Vec::new(),
            writer_pos: fence::GtidPos::default(),
            fenced_at_unix: None,
        };
        serde_json::to_value(r).map_err(|e| SprigError::internal(e.to_string()))
    }

    async fn fence(&self, deadline: Duration) -> Result<Fenced> {
        let schemas = self.schemas().await?;
        let req = self.fence_request(schemas, deadline);
        let r = fence::fence(
            &req,
            &self.access.writer,
            &self.access.nodes,
            &FenceOptions::default(),
        )
        .await?;
        let pos: GtidPos = r.writer_pos.0.parse()?;
        let receipt = serde_json::to_value(&r).map_err(|e| SprigError::internal(e.to_string()))?;
        Ok(Fenced {
            receipt,
            pos: self.access.trim(pos),
        })
    }

    async fn fence_reads(&self, _receipt: &serde_json::Value) -> Result<()> {
        Ok(()) // a rename fence blocks reads too
    }

    async fn drain_prepared(&self, within: Duration) -> Result<Drained> {
        match &self.drain {
            Some(d) => d.drain(&self.access.cluster, self.tenant, within).await,
            None => Ok(Drained::default()),
        }
    }

    async fn unfence(&self, receipt: &serde_json::Value) -> Result<()> {
        if receipt.is_null() {
            return self.unfence_lost().await;
        }
        let r: FenceReceipt = serde_json::from_value(receipt.clone())
            .map_err(|e| SprigError::internal(format!("bad fence receipt: {e}")))?;
        fence::unfence(&r, &self.access.writer, &FenceOptions::default()).await
    }

    async fn unfence_lost(&self) -> Result<()> {
        let planned = self.plan_fence().await?;
        self.unfence(&planned).await
    }

    async fn counters(&self) -> Result<Vec<Counter>> {
        let known = self.known();
        let mut out = Vec::new();
        for s in self.schemas().await? {
            let rows = self
                .rows(
                    "SELECT TABLE_NAME, AUTO_INCREMENT FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = ? AND AUTO_INCREMENT IS NOT NULL",
                    vec![Value::Bytes(s.as_bytes().to_vec())],
                )
                .await?;
            for r in rows {
                let (Some(name), Some(next)) = (r.get::<String, _>(0), r.get::<u64, _>(1)) else {
                    continue;
                };
                // A fenced table: its original name.
                let table = known
                    .iter()
                    .find(|(ks, kt)| *ks == s && fenced_name(kt) == name)
                    .map_or(name, |(_, kt)| kt.clone());
                if internal(&table) {
                    continue;
                }
                out.push(Counter {
                    schema: s.clone(),
                    table,
                    next,
                });
            }
        }
        Ok(out)
    }

    async fn load(&self) -> Result<Load> {
        let mut c = self.conn().await?;
        let r: Option<(String, u64)> = c
            .query_first("SHOW GLOBAL STATUS LIKE 'Threads_running'")
            .await
            .map_err(|e| db_err("read Threads_running", &e))?;
        Ok(Load {
            threads_running: r.map_or(0, |(_, v)| v),
            ..Load::default()
        })
    }

    async fn drop_schemas(&self) -> Result<()> {
        let mut c = self.conn().await?;
        if self.swap {
            for (s, t) in self.known() {
                let old = format!("{INTERNAL_OBJECT_PREFIX}old_{}", &fenced_name(&t)[10..]);
                c.query_drop(format!("DROP TABLE IF EXISTS {}", qualified(&s, &old)))
                    .await
                    .map_err(|e| db_err("drop the swapped-out table", &e))?;
            }
        } else {
            for s in self.schemas().await? {
                tracing::info!(schema = %s, cluster = %self.access.cluster, "dropping the retained source");
                c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&s)))
                    .await
                    .map_err(|e| db_err("drop the retained source", &e))?;
            }
        }
        let _ = c
            .exec_drop(
                "DELETE FROM `__sprig_sys`.`__sprig_watermark` WHERE job_id = ?",
                (self.job.to_bytes().to_vec(),),
            )
            .await;
        Ok(())
    }

    async fn purge_tenant(&self) -> Result<()> {
        let mut c = self.conn().await?;
        // Accounts and name rows only: the schemas went above, and any
        // other schema of the tenant here isn't this job's to judge.
        let known = sprig_cluster::purge::Known::default();
        let p = sprig_cluster::purge::purge(&mut c, self.tenant, &known).await?;
        if let Some(mark) = &p.foreign {
            tracing::warn!(tenant = %self.tenant, cluster = %self.access.cluster, mark,
                "another tenant's catalog with this id is here; its accounts were left alone");
        } else {
            tracing::info!(tenant = %self.tenant, cluster = %self.access.cluster,
                accounts = p.drop_accounts.len(), names = p.delete_names.len(),
                "deleted tenant purged from the retained source's cluster");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::internal;

    /// Engine tables in a database's schema move with it (R5 round 6: a
    /// tenant move left MongoDB databases without `__sprig_doc_sessions`,
    /// and their next write failed with 1146); the mover's own tables and
    /// fences don't.
    #[test]
    fn engine_tables_move_with_their_schema() {
        for t in [
            "__sprig_doc_sessions",
            "__sprig_doc_search_ix",
            "__sprig_doc_meta",
            "__sprig_doc_vx_00ff00ff00ff00ff",
            "__sprig_redis_scripts",
            "__sprig_dim_models",
            "__sprig_ib_41",
            "__sprig_ibr_41",
            "__sprig_ke_cart",
            "__sprig_qf",
            "orders",
        ] {
            assert!(!internal(t), "{t} must move");
        }
        for t in [
            "__sprig_f_orders",
            "__sprig_cs_orders",
            "__sprig_mover_pos",
            "__sprig_ih_7",
            "__sprig_ih_state",
            "__sprig_dtx",
        ] {
            assert!(internal(t), "{t} must stay");
        }
    }
}
