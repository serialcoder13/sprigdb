//! [`TargetIo`] on a MariaDB cluster's writer: the applier, position rows
//! with fencing tokens, verification reads, and the cutover's target steps.
//! A copy-and-swap target writes a shadow table in the source's schema and
//! swaps it in by rename.

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Pool, Row as MyRow, TxOpts};
use sprig_binlog::GtidPos;
use sprig_cluster::fence::fenced_name;
use sprig_common::ids::JobId;
use sprig_common::names::{INTERNAL_OBJECT_PREFIX, quote};
use sprig_common::{Result, SprigCode, SprigError};

use super::source::MysqlSource;
use super::sql::{self, qualified};
use super::{ClusterAccess, db_err, errno};
use crate::api::DdlTransform;
use crate::io::{Counter, Load, SourceIo, TargetIo};
use crate::model::{ApplyOp, CopyState, PosRow, Row, RowDigest, TableMeta, Value};
use crate::route::ShardRoute;

/// Errors a re-run DDL statement raises when it already took effect:
/// exists (1007, 1050, 1060, 1061, 1304, 1359), doesn't exist (1008, 1051,
/// 1091, 1146, 1305, 1360), duplicate foreign key (1826).
const ALREADY_APPLIED: &[u16] = &[
    1007, 1008, 1050, 1051, 1060, 1061, 1091, 1146, 1304, 1305, 1359, 1360, 1826,
    // An event that exists already, or not at all (stream DDL).
    1537, 1539,
];

/// The target side of one job.
pub struct MysqlTarget {
    access: ClusterAccess,
    pool: Pool,
    job: JobId,
    swap: Option<DdlTransform>,
    source: Arc<MysqlSource>,
    /// A hash-range job's routing: target schemas are new
    /// shard schemas, rows go by `_sprig_rk`.
    route: Option<Arc<ShardRoute>>,
    /// A job standby's tenant: its copy stays read-only for the tenant's
    /// accounts ([`super::standby`]).
    standby: Option<sprig_common::ids::TenantId>,
}

impl std::fmt::Debug for MysqlTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlTarget")
            .field("cluster", &self.access.cluster)
            .field("job", &self.job)
            .finish_non_exhaustive()
    }
}

fn lease_lost() -> SprigError {
    SprigError::new(
        SprigCode::JobLeaseLost,
        "the target position row has a newer owner's token",
    )
}

fn shadow_name(table: &str) -> String {
    format!("{INTERNAL_OBJECT_PREFIX}cs_{}", &fenced_name(table)[10..])
}

fn old_name(table: &str) -> String {
    format!("{INTERNAL_OBJECT_PREFIX}old_{}", &fenced_name(table)[10..])
}

impl MysqlTarget {
    /// The target of `job` on `access`; `swap` for a copy-and-swap.
    #[must_use]
    pub fn new(
        access: ClusterAccess,
        job: JobId,
        swap: Option<DdlTransform>,
        source: Arc<MysqlSource>,
    ) -> Self {
        let pool = access.pool(sql::APPLIER_SESSION, 4);
        Self {
            access,
            pool,
            job,
            swap,
            source,
            route: None,
            standby: None,
        }
    }

    /// The target of a job standby (`Continuous`) of `tenant`: its copy is
    /// read-only for the tenant's accounts until the promotion.
    #[must_use]
    pub fn with_standby(mut self, tenant: sprig_common::ids::TenantId) -> Self {
        self.standby = Some(tenant);
        self
    }

    /// Routes rows of a hash-range job (split, merge, shard move) into
    /// its new shard schemas.
    #[must_use]
    pub fn with_route(mut self, route: Arc<ShardRoute>) -> Self {
        self.route = Some(route);
        self
    }

    /// The target's name for `t`: its shadow in a copy-and-swap.
    fn name_of<'a>(&self, t: &'a TableMeta) -> std::borrow::Cow<'a, str> {
        match &self.swap {
            Some(d) if d.schema == t.schema && d.table == t.name => shadow_name(&t.name).into(),
            _ => std::borrow::Cow::Borrowed(t.name.as_str()),
        }
    }

    async fn conn(&self) -> Result<Conn> {
        self.pool
            .get_conn()
            .await
            .map_err(|e| db_err(&format!("connect to {}", self.access.writer.addr()), &e))
    }

    fn job_bytes(&self) -> Value {
        Value::Bytes(self.job.to_bytes().to_vec())
    }

    async fn exec(
        c: &mut mysql_async::Transaction<'_>,
        q: &str,
        p: Vec<Value>,
        what: &str,
    ) -> Result<u64> {
        let params = if p.is_empty() {
            Params::Empty
        } else {
            Params::Positional(p)
        };
        c.exec_drop(q, params).await.map_err(|e| db_err(what, &e))?;
        Ok(c.affected_rows())
    }

    /// Moves the position in an open transaction, guarded by the token.
    async fn set_pos(
        &self,
        tx: &mut mysql_async::Transaction<'_>,
        token: u64,
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()> {
        let copy = serde_json::to_string(copy).map_err(|e| SprigError::internal(e.to_string()))?;
        let n = Self::exec(
            tx,
            "UPDATE `__sprig_sys`.`__sprig_mover_pos` SET gtid_pos = ?, copy_state = ?, \
             updated_at = NOW(6) WHERE job_id = ? AND stream = 0 AND target = 0 AND token = ?",
            vec![
                Value::Bytes(pos.to_string().into_bytes()),
                Value::Bytes(copy.into_bytes()),
                self.job_bytes(),
                Value::UInt(token),
            ],
            "move the position",
        )
        .await?;
        if n == 0 {
            return Err(lease_lost());
        }
        Ok(())
    }

    async fn run_ops(&self, tx: &mut mysql_async::Transaction<'_>, ops: &[ApplyOp]) -> Result<()> {
        let routed;
        let ops = match &self.route {
            Some(r) => {
                routed = r.route_ops(ops)?;
                routed.as_slice()
            }
            None => ops,
        };
        for op in ops {
            let stmts = match op {
                ApplyOp::Upsert { table, rows } => sql::upserts(table, &self.name_of(table), rows),
                ApplyOp::Delete { table, keys } => sql::deletes(table, &self.name_of(table), keys),
            };
            for (q, p) in stmts {
                Self::exec(tx, &q, p, "apply").await?;
            }
        }
        Ok(())
    }

    /// The schemas this job writes on the target: the source's names for a
    /// move, the new shard schemas for a hash-range job.
    async fn job_schemas(&self) -> Result<Vec<String>> {
        match &self.route {
            Some(r) => Ok(r.target_schemas()),
            None => self.source.schemas().await,
        }
    }

    /// A re-key's target digests of source table `t`'s key range (see
    /// `ShardRoute::rekey_digests`).
    #[allow(clippy::many_single_char_names)] // the module's names: route, table, rows
    async fn rekey_digests(
        &self,
        r: &Arc<ShardRoute>,
        t: &TableMeta,
        after: Option<&Row>,
        last: &Row,
    ) -> Result<Vec<RowDigest>> {
        let Some(rk) = r.rekey.as_ref() else {
            return Ok(Vec::new());
        };
        let cols = rk.columns_of(t)?;
        let bare = ShardRoute::without_rk(t);
        let mut all = Vec::new();
        let num = |v: Option<&Value>| match v {
            Some(Value::UInt(u)) => Some(*u),
            Some(Value::Int(i)) => Some(u64::from_ne_bytes(i.to_ne_bytes())),
            Some(Value::Bytes(b)) => String::from_utf8_lossy(b).parse().ok(),
            _ => None,
        };
        for schema in r.targets_of(&t.schema) {
            let tt = ShardRoute::retarget(&bare, &schema);
            let mut exprs = format!(
                "{}, {}, {}",
                sql::digest_list(&tt),
                quote(crate::route::RK_COLUMN),
                quote(crate::route::RK_PREV_COLUMN)
            );
            for &i in &cols {
                exprs.push_str(", ");
                exprs.push_str(&quote(&t.columns[i].name));
            }
            let (q, p) = sql::range(&tt, &tt.name, &exprs, after, last);
            let mut c = self.conn().await?;
            let rows: Vec<MyRow> = c
                .exec(q, Params::Positional(p))
                .await
                .map_err(|e| db_err("target digests", &e))?;
            let n = t.pk.len();
            for row in rows {
                let vals: Vec<Value> = row.unwrap();
                let key_values: Vec<Value> = vals.iter().take(n).cloned().collect();
                let d = RowDigest {
                    key: TableMeta::key_of_values(&key_values),
                    key_values,
                    hash: num(vals.get(n)).unwrap_or(0),
                };
                let want =
                    sprig_shard::rk::hash_values(&rk.key, &vals[(n + 3).min(vals.len())..]).ok();
                all.push((
                    schema.clone(),
                    d,
                    num(vals.get(n + 1)),
                    num(vals.get(n + 2)),
                    want,
                ));
            }
        }
        Ok(r.rekey_digests(t, all))
    }

    async fn exists(&self, c: &mut Conn, schema: &str, table: &str) -> Result<bool> {
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (schema.as_bytes().to_vec(), table.as_bytes().to_vec()),
            )
            .await
            .map_err(|e| db_err("look up a table", &e))?;
        Ok(n.unwrap_or(0) > 0)
    }
}

#[async_trait::async_trait]
#[allow(clippy::many_single_char_names, clippy::too_many_lines)]
impl TargetIo for MysqlTarget {
    async fn prepare(&self, tables: &[TableMeta], objects: &[String]) -> Result<()> {
        let mut c = self.conn().await?;
        for q in [
            "CREATE DATABASE IF NOT EXISTS `__sprig_sys`",
            sql::POS_DDL,
            sql::XA_DDL,
        ] {
            c.query_drop(q)
                .await
                .map_err(|e| db_err("create the mover's tables", &e))?;
        }
        if let Some(d) = &self.swap {
            let shadow = shadow_name(&d.table);
            if !self.exists(&mut c, &d.schema, &shadow).await? {
                c.query_drop(format!(
                    "CREATE TABLE {} LIKE {}",
                    qualified(&d.schema, &shadow),
                    qualified(&d.schema, &d.table)
                ))
                .await
                .map_err(|e| db_err("create the shadow table", &e))?;
                c.query_drop(format!(
                    "ALTER TABLE {} {}",
                    qualified(&d.schema, &shadow),
                    d.alter
                ))
                .await
                .map_err(|e| {
                    SprigError::new(
                        SprigCode::MoveUnsupported,
                        format!("the copy-and-swap DDL failed on the shadow: {e}"),
                    )
                })?;
            }
            return Ok(());
        }
        let rewritten;
        let objects = match &self.route {
            Some(r) => {
                rewritten = r.rewrite_statements(objects);
                rewritten.as_slice()
            }
            None => objects,
        };
        for q in objects.iter().filter(|q| q.starts_with("CREATE DATABASE")) {
            c.query_drop(q.as_str())
                .await
                .map_err(|e| db_err("create a schema", &e))?;
        }
        for t in tables {
            let schemas = match &self.route {
                Some(r) => r.targets_of(&t.schema),
                None => vec![t.schema.clone()],
            };
            for schema in schemas {
                c.query_drop(format!("CREATE DATABASE IF NOT EXISTS {}", quote(&schema)))
                    .await
                    .map_err(|e| db_err("create a schema", &e))?;
                c.query_drop(format!("USE {}", quote(&schema)))
                    .await
                    .map_err(|e| db_err("use a schema", &e))?;
                let create = t
                    .create_sql
                    .replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1);
                c.query_drop(create)
                    .await
                    .map_err(|e| db_err("create a table", &e))?;
                if self.route.as_ref().is_some_and(|r| r.rekey.is_some()) {
                    // Which source shard each row came from (verification).
                    c.query_drop(format!(
                        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} BIGINT UNSIGNED NULL INVISIBLE",
                        quote(&t.name),
                        crate::route::RK_PREV_COLUMN
                    ))
                    .await
                    .map_err(|e| db_err("add the re-key's column", &e))?;
                }
            }
        }
        // Views may need other views first: retry until nothing changes.
        let mut pending: Vec<&String> = objects
            .iter()
            .filter(|q| !q.starts_with("CREATE DATABASE"))
            .collect();
        for _ in 0..4 {
            let mut failed = Vec::new();
            let mut last_use = None;
            for q in pending {
                if q.starts_with("USE ")
                    || q.starts_with("SET SESSION")
                    || q.starts_with("SET NAMES")
                {
                    let _ = c.query_drop(q.as_str()).await;
                    if q.starts_with("USE ") {
                        last_use = Some(q);
                    }
                    continue;
                }
                if let Err(e) = c.query_drop(q.as_str()).await {
                    tracing::debug!(error = %e, "object statement failed; retrying later");
                    if let Some(u) = last_use {
                        failed.push(u);
                    }
                    failed.push(q);
                }
            }
            if failed.is_empty() {
                pending = failed;
                break;
            }
            pending = failed;
        }
        if !pending.is_empty() {
            tracing::warn!(
                left = pending.len(),
                "some views, routines or grants couldn't be created on the target"
            );
        }
        // A standby's copy is read-only for the tenant's accounts until its
        // promotion: the grants just created lose their
        // write privileges, recorded for the promotion to restore.
        if let Some(tenant) = self.standby {
            let (revoked, killed) = super::standby::fence(&mut c, tenant).await?;
            tracing::info!(job = %self.job.0, %tenant, revoked, killed, "standby copy read-only for the tenant's accounts");
        }
        Ok(())
    }

    async fn reserve(&self, job: JobId, token: u64, init: &PosRow) -> Result<PosRow> {
        let mut c = self.conn().await?;
        for q in [
            "CREATE DATABASE IF NOT EXISTS `__sprig_sys`",
            sql::POS_DDL,
            sql::XA_DDL,
        ] {
            c.query_drop(q)
                .await
                .map_err(|e| db_err("create the mover's tables", &e))?;
        }
        let has_pos: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM `__sprig_sys`.`__sprig_mover_pos` WHERE job_id = ?",
                (job.to_bytes().to_vec(),),
            )
            .await
            .map_err(|e| db_err("read the position", &e))?;
        let resuming = has_pos.unwrap_or(0) > 0;
        if self.swap.is_none() {
            // Never write into schemas the target already had: a move's target
            // schemas are new.
            if !resuming {
                for s in self.job_schemas().await? {
                    let n: Option<u64> = c
                        .exec_first(
                            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
                            (s.as_bytes().to_vec(),),
                        )
                        .await
                        .map_err(|e| db_err("look up a schema", &e))?;
                    if n.unwrap_or(0) > 0 {
                        return Err(SprigError::new(
                            SprigCode::MoveTargetIncompatible,
                            format!(
                                "cluster {} already has schema {s}; drop it or move elsewhere",
                                self.access.cluster
                            ),
                        ));
                    }
                }
            }
        }
        drop(c);
        self.claim(job, token, init).await
    }

    async fn load_pos(&self, job: JobId) -> Result<Option<PosRow>> {
        let mut c = self.conn().await?;
        let r: Option<(u64, String, String)> = c
            .exec_first(
                "SELECT token, gtid_pos, copy_state FROM `__sprig_sys`.`__sprig_mover_pos` \
                 WHERE job_id = ? AND stream = 0 AND target = 0",
                (job.to_bytes().to_vec(),),
            )
            .await
            .map_err(|e| db_err("read the position", &e))?;
        r.map(|(token, pos, copy)| {
            Ok(PosRow {
                token,
                gtid_pos: pos.parse()?,
                copy: serde_json::from_str(&copy)
                    .map_err(|e| SprigError::internal(format!("bad copy state: {e}")))?,
            })
        })
        .transpose()
    }

    async fn claim(&self, job: JobId, token: u64, init: &PosRow) -> Result<PosRow> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| db_err("begin", &e))?;
        let cur: Option<u64> = tx
            .exec_first(
                "SELECT token FROM `__sprig_sys`.`__sprig_mover_pos` WHERE job_id = ? \
                 AND stream = 0 AND target = 0 FOR UPDATE",
                (job.to_bytes().to_vec(),),
            )
            .await
            .map_err(|e| db_err("read the position", &e))?;
        match cur {
            Some(t) if t > token => return Err(lease_lost()),
            Some(_) => {
                Self::exec(
                    &mut tx,
                    "UPDATE `__sprig_sys`.`__sprig_mover_pos` SET token = ? WHERE job_id = ? \
                     AND stream = 0 AND target = 0",
                    vec![Value::UInt(token), self.job_bytes()],
                    "raise the token",
                )
                .await?;
            }
            None => {
                let copy = serde_json::to_string(&init.copy)
                    .map_err(|e| SprigError::internal(e.to_string()))?;
                Self::exec(
                    &mut tx,
                    "INSERT INTO `__sprig_sys`.`__sprig_mover_pos` (job_id, stream, target, \
                     token, gtid_pos, copy_state, updated_at) VALUES (?, 0, 0, ?, ?, ?, NOW(6))",
                    vec![
                        self.job_bytes(),
                        Value::UInt(token),
                        Value::Bytes(init.gtid_pos.to_string().into_bytes()),
                        Value::Bytes(copy.into_bytes()),
                    ],
                    "insert the position",
                )
                .await?;
            }
        }
        tx.commit().await.map_err(|e| db_err("commit", &e))?;
        drop(c);
        self.load_pos(job).await?.ok_or_else(lease_lost)
    }

    async fn apply(
        &self,
        _job: JobId,
        token: u64,
        ops: &[ApplyOp],
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| db_err("begin", &e))?;
        let r = async {
            self.run_ops(&mut tx, ops).await?;
            self.set_pos(&mut tx, token, pos, copy).await
        }
        .await;
        match r {
            Ok(()) => tx.commit().await.map_err(|e| db_err("commit", &e)),
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    async fn apply_ddl(
        &self,
        job: JobId,
        token: u64,
        db: &str,
        stmt: &str,
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()> {
        if let Some(d) = &self.swap
            && !stmt.contains(&format!("{INTERNAL_OBJECT_PREFIX}cs_"))
            && super::sql::ddl_touches(db, stmt, std::slice::from_ref(&d.schema))
            && super::sql::names_word(stmt, &d.table)
        {
            return Err(SprigError::new(
                SprigCode::MoveUnsupported,
                format!("DDL on {}.{} during its copy-and-swap", d.schema, d.table),
            ));
        }
        // Triggers and events aren't created by the
        // stream (they would fire on the applied rows, or run on the copy);
        // the cutover creates them from the source, and a standby records
        // them for its promotion. Drops still apply.
        let program = super::sql::program_ddl(stmt);
        if matches!(
            program,
            Some(super::sql::ProgramDdl::CreateTrigger | super::sql::ProgramDdl::CreateEvent)
        ) {
            tracing::info!(%db, "trigger or event DDL in the stream: created at the cutover, not now");
        } else if self.swap.is_none() {
            let mut c = self.conn().await?;
            let runs: Vec<(String, String)> = match &self.route {
                // DDL on a split or merged shard runs on each target, its
                // schema name rewritten (the group's own DDL is a separate
                // job that waits for movers; this covers what slips by).
                Some(r) => {
                    let stmts = vec![format!("USE {}", quote(db)), stmt.to_owned()];
                    r.rewrite_statements(&stmts)
                        .chunks(2)
                        .filter_map(|p| match p {
                            [u, q] => Some((
                                u.trim_start_matches("USE ").trim_matches('`').to_owned(),
                                q.clone(),
                            )),
                            _ => None,
                        })
                        .collect()
                }
                None => vec![(db.to_owned(), stmt.to_owned())],
            };
            for (db, stmt) in runs {
                if !db.is_empty() {
                    // The database may be the one the statement creates.
                    let _ = c.query_drop(format!("USE {}", quote(&db))).await;
                }
                if let Err(e) = c.query_drop(stmt).await {
                    match errno(&e) {
                        Some(n) if ALREADY_APPLIED.contains(&n) => {
                            tracing::info!(errno = n, "DDL already applied");
                        }
                        _ => return Err(db_err("apply DDL", &e)),
                    }
                }
            }
        }
        self.apply(job, token, &[], pos, copy).await
    }

    async fn digests(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        last: &Row,
    ) -> Result<Vec<RowDigest>> {
        if let Some(r) = self.route.as_ref().filter(|r| r.rekey.is_some()) {
            return self.rekey_digests(r, t, after, last).await;
        }
        if let Some(r) = &self.route {
            let r = r.clone();
            let mut all = Vec::new();
            let rk = ShardRoute::rk_column(t).map(|i| quote(&t.columns[i].name));
            for schema in r.targets_of(&t.schema) {
                let tt = ShardRoute::retarget(t, &schema);
                let exprs = match &rk {
                    Some(k) => format!("{}, {k}", sql::digest_list(&tt)),
                    None => sql::digest_list(&tt),
                };
                let (q, p) = sql::range(&tt, &tt.name, &exprs, after, last);
                let mut c = self.conn().await?;
                let rows: Vec<MyRow> = c
                    .exec(q, Params::Positional(p))
                    .await
                    .map_err(|e| db_err("target digests", &e))?;
                let n = t.pk.len();
                for row in rows {
                    let vals: Vec<Value> = row.unwrap();
                    let key_values: Vec<Value> = vals.iter().take(n).cloned().collect();
                    let num = |v: Option<&Value>| match v {
                        Some(Value::UInt(u)) => Some(*u),
                        Some(Value::Int(i)) => Some(u64::from_ne_bytes(i.to_ne_bytes())),
                        Some(Value::Bytes(b)) => String::from_utf8_lossy(b).parse().ok(),
                        _ => None,
                    };
                    let d = RowDigest {
                        key: TableMeta::key_of_values(&key_values),
                        key_values,
                        hash: num(vals.get(n)).unwrap_or(0),
                    };
                    all.push((schema.clone(), d, num(vals.get(n + 1))));
                }
            }
            return Ok(r.merge_digests(t, all));
        }
        let name = self.name_of(t);
        let (q, p) = sql::range(t, &name, &sql::digest_list(t), after, last);
        let mut c = self.conn().await?;
        let rows: Vec<MyRow> = c
            .exec(q, Params::Positional(p))
            .await
            .map_err(|e| db_err("target digests", &e))?;
        let n = t.pk.len();
        Ok(rows
            .into_iter()
            .map(|r| {
                let vals: Vec<Value> = r.unwrap();
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
            })
            .collect())
    }

    async fn delete_keys(&self, job: JobId, token: u64, t: &TableMeta, keys: &[Row]) -> Result<()> {
        let Some(cur) = self.load_pos(job).await? else {
            return Err(lease_lost());
        };
        let op = ApplyOp::Delete {
            table: Arc::new(t.clone()),
            keys: keys.to_vec(),
        };
        self.apply(job, token, &[op], &cur.gtid_pos, &cur.copy)
            .await
    }

    async fn raise_counters(&self, counters: &[Counter]) -> Result<()> {
        let mut c = self.conn().await?;
        let routed: Vec<Counter> = match &self.route {
            Some(r) => counters
                .iter()
                .flat_map(|k| {
                    r.targets_of(&k.schema).into_iter().map(|s| Counter {
                        schema: s,
                        table: k.table.clone(),
                        next: k.next,
                    })
                })
                .collect(),
            None => counters.to_vec(),
        };
        for k in &routed {
            let name = match &self.swap {
                Some(d) if d.schema == k.schema && d.table == k.table => shadow_name(&k.table),
                Some(_) => continue,
                None => k.table.clone(),
            };
            let cur: Option<Option<u64>> = c
                .exec_first(
                    "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                     AND TABLE_NAME = ?",
                    (k.schema.as_bytes().to_vec(), name.as_bytes().to_vec()),
                )
                .await
                .map_err(|e| db_err("read a counter", &e))?;
            if cur.flatten().is_some_and(|v| v < k.next) {
                c.query_drop(format!(
                    "ALTER TABLE {} AUTO_INCREMENT = {}",
                    qualified(&k.schema, &name),
                    k.next
                ))
                .await
                .map_err(|e| db_err("raise a counter", &e))?;
            }
        }
        Ok(())
    }

    async fn finalize(&self, triggers: &[String]) -> Result<GtidPos> {
        let mut c = self.conn().await?;
        if let Some(d) = &self.swap {
            // The commit point of a copy-and-swap.
            let q = format!(
                "RENAME TABLE {} TO {}, {} TO {}",
                qualified(&d.schema, &shadow_name(&d.table)),
                qualified(&d.schema, &d.table),
                qualified(&d.schema, &fenced_name(&d.table)),
                qualified(&d.schema, &old_name(&d.table)),
            );
            if !self.swapped().await? {
                c.query_drop(q)
                    .await
                    .map_err(|e| db_err("swap the shadow in", &e))?;
            }
        } else {
            let rewritten;
            let triggers = match &self.route {
                Some(r) => {
                    rewritten = r.rewrite_statements(triggers);
                    rewritten.as_slice()
                }
                None => triggers,
            };
            for q in triggers {
                // An event of a split or merged group would run on
                // every shard; only whole-tenant moves carry events.
                if self.route.is_some()
                    && super::sql::program_ddl(q) == Some(super::sql::ProgramDdl::CreateEvent)
                {
                    continue;
                }
                if let Err(e) = c.query_drop(q.as_str()).await {
                    match errno(&e) {
                        Some(1359) => {}
                        _ => return Err(db_err("create a trigger", &e)),
                    }
                }
            }
        }
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| db_err("read @@gtid_binlog_pos", &e))?;
        let p: GtidPos = p.unwrap_or_default().parse()?;
        Ok(self.access.trim(p))
    }

    async fn swapped(&self) -> Result<bool> {
        let Some(d) = &self.swap else {
            return Ok(false);
        };
        let mut c = self.conn().await?;
        Ok(!self
            .exists(&mut c, &d.schema, &shadow_name(&d.table))
            .await?
            && self.exists(&mut c, &d.schema, &old_name(&d.table)).await?)
    }

    async fn unfinalize(&self, _triggers: &[String]) -> Result<()> {
        if self.swap.is_some() {
            return Ok(());
        }
        let mut c = self.conn().await?;
        for s in self.job_schemas().await? {
            let names: Vec<String> = c
                .exec(
                    "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?",
                    (s.as_bytes().to_vec(),),
                )
                .await
                .map_err(|e| db_err("list triggers", &e))?;
            for n in names {
                c.query_drop(format!("DROP TRIGGER IF EXISTS {}", qualified(&s, &n)))
                    .await
                    .map_err(|e| db_err("drop a trigger", &e))?;
            }
            // Events go back to disabled, as `prepare` made them.
            let events: Vec<String> = c
                .exec(
                    "SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ? \
                     AND STATUS = 'ENABLED'",
                    (s.as_bytes().to_vec(),),
                )
                .await
                .map_err(|e| db_err("list events", &e))?;
            for n in events {
                c.query_drop(format!("ALTER EVENT {} DISABLE", qualified(&s, &n)))
                    .await
                    .map_err(|e| db_err("disable an event", &e))?;
            }
        }
        Ok(())
    }

    async fn record_cutover_statements(&self, stmts: &[String]) -> Result<()> {
        let Some(tenant) = self.standby else {
            return Ok(());
        };
        let mut c = self.conn().await?;
        if super::standby::record_objects(&mut c, tenant, stmts).await? {
            tracing::info!(%tenant, statements = stmts.len(), "standby's triggers and events recorded for its promotion");
        }
        Ok(())
    }

    async fn reset(&self, job: JobId, keep_schemas: bool) -> Result<()> {
        let mut c = self.conn().await?;
        if let Some(d) = &self.swap {
            c.query_drop(format!(
                "DROP TABLE IF EXISTS {}",
                qualified(&d.schema, &shadow_name(&d.table))
            ))
            .await
            .map_err(|e| db_err("drop the shadow", &e))?;
        } else {
            // Only schemas this job created: the position row proves it
            // (prepare refused schemas that existed before).
            let ours: Option<u64> = c
                .exec_first(
                    "SELECT COUNT(*) FROM `__sprig_sys`.`__sprig_mover_pos` WHERE job_id = ?",
                    (job.to_bytes().to_vec(),),
                )
                .await
                .unwrap_or(Some(0));
            if ours.unwrap_or(0) > 0 {
                for s in self.job_schemas().await? {
                    c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&s)))
                        .await
                        .map_err(|e| db_err("drop a target schema", &e))?;
                }
            }
        }
        let _ = keep_schemas; // a restarted copy recreates the schemas in preparing
        if let Some(tenant) = self.standby {
            let mut c = self.conn().await?;
            super::standby::forget(&mut c, tenant).await?;
        }
        self.forget(job).await
    }

    async fn forget(&self, job: JobId) -> Result<()> {
        let mut c = self.conn().await?;
        for q in [
            "DELETE FROM `__sprig_sys`.`__sprig_mover_pos` WHERE job_id = ?",
            "DELETE FROM `__sprig_sys`.`__sprig_mover_xa` WHERE job_id = ?",
        ] {
            match c.exec_drop(q, (job.to_bytes().to_vec(),)).await {
                Ok(()) => {}
                Err(e) if errno(&e) == Some(1146) => {}
                Err(e) => return Err(db_err("forget the job", &e)),
            }
        }
        Ok(())
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
}
