//! The coordinator's and resolver's seams on `mysql_async`: branch
//! connections, the admin and a static backend, for the controller's
//! resolver, `sprigctl txn` and the live tests. The gateway implements
//! [`BranchConn`] on its own backend connections instead.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row};
use sprig_common::ids::{ClusterId, DbId, GroupId, TenantId};

use crate::backend::{BranchConn, ClusterAdmin, ClusterCaps, Ctl, DbError, DbResult, TxnBackend};
use crate::decision::{DTX_TABLE, Decision, DecisionRow, decode_participants};
use crate::xid::{AnchorRef, Dtid, FORMAT_ID, Xid, hex};

/// Lock-wait timeout of admin inserts: a race insert that meets an open
/// transaction's row reports "unknown" quickly and retries next round.
pub const ADMIN_LOCK_WAIT_S: u32 = 2;

/// Maps a `mysql_async` error.
#[must_use]
pub fn db_error(e: &mysql_async::Error) -> DbError {
    match e {
        mysql_async::Error::Server(s) => DbError::new(s.code, s.message.clone()),
        other => DbError::lost(other.to_string()),
    }
}

/// A branch on a dedicated `mysql_async` connection. Dropping it closes
/// the connection, which detaches a prepared branch (spike-xa).
#[derive(Debug)]
pub struct MyConn {
    conn: Option<Conn>,
    thread: u64,
    /// Sprig 2PC: the held branch's record (writes, savepoints, the first
    /// statement that can't replay).
    redo: Option<Recording>,
}

/// A held branch's record on a `mysql_async` connection.
#[derive(Debug, Default)]
struct Recording {
    timestamp_us: u64,
    stmts: Vec<crate::redo::RedoStmt>,
    savepoints: Vec<(String, usize)>,
    unsafe_why: Option<String>,
}

/// Whether `sql` writes (what a held branch records).
fn writes(sql: &str) -> bool {
    let w = sql
        .trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(w.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE")
}

impl MyConn {
    /// Connects.
    ///
    /// # Errors
    /// When the connection fails.
    pub async fn connect(opts: Opts) -> DbResult<Self> {
        let conn = Conn::new(opts).await.map_err(|e| db_error(&e))?;
        let thread = u64::from(conn.id());
        Ok(Self {
            conn: Some(conn),
            thread,
            redo: None,
        })
    }

    /// The raw connection, for the caller's own statements.
    ///
    /// # Errors
    /// When the connection broke earlier.
    pub fn raw(&mut self) -> DbResult<&mut Conn> {
        self.conn
            .as_mut()
            .ok_or_else(|| DbError::lost("connection closed"))
    }

    /// Runs one statement of the caller's.
    ///
    /// # Errors
    /// The server's error, or a lost connection.
    pub async fn exec(&mut self, sql: &str) -> DbResult<()> {
        let r = self.raw()?.query_drop(sql).await;
        self.check(r)?;
        self.record(sql);
        Ok(())
    }

    /// A write the caller ran on [`Self::raw`] succeeded: a held branch
    /// records it (with the connection's affected rows), as [`Self::exec`]
    /// does. `sql` must carry its values in the text (as the graph
    /// store's statements do).
    pub fn note_write(&mut self, sql: &str) {
        self.record(sql);
    }

    /// A held branch records a write that succeeded.
    fn record(&mut self, sql: &str) {
        let (Some(rec), Some(c)) = (self.redo.as_mut(), self.conn.as_ref()) else {
            return;
        };
        if !writes(sql) {
            return;
        }
        if rec.unsafe_why.is_none()
            && let Some(why) = crate::redo::unsafe_reason(sql)
        {
            rec.unsafe_why = Some(why.to_owned());
        }
        let update = sql.trim_start().to_ascii_uppercase().starts_with("UPDATE");
        rec.stmts.push(crate::redo::RedoStmt {
            sql: sql.to_owned(),
            // UPDATE counts depend on CLIENT_FOUND_ROWS: not checked.
            affected: if update { u64::MAX } else { c.affected_rows() },
            insert_id: c.last_insert_id().unwrap_or(0),
            vars: Vec::new(),
            charset: None,
            timestamp_us: Some(rec.timestamp_us),
        });
    }

    /// Runs a query of the caller's.
    ///
    /// # Errors
    /// The server's error, or a lost connection.
    pub async fn query(&mut self, sql: &str) -> DbResult<Vec<Row>> {
        let r = self.raw()?.query(sql).await;
        self.check(r)
    }

    /// Runs a prepared statement of the caller's with parameters.
    ///
    /// # Errors
    /// The server's error, or a lost connection.
    pub async fn exec_params(
        &mut self,
        sql: &str,
        params: mysql_async::Params,
    ) -> DbResult<Vec<Row>> {
        let r = self.raw()?.exec(sql, params).await;
        self.check(r)
    }

    fn check<T>(&mut self, r: Result<T, mysql_async::Error>) -> DbResult<T> {
        r.map_err(|e| {
            let d = db_error(&e);
            if d.is_lost() {
                self.conn = None;
            }
            d
        })
    }
}

#[async_trait::async_trait]
impl BranchConn for MyConn {
    fn thread_id(&self) -> u64 {
        self.thread
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        if ctl.is_empty() {
            return Ok(());
        }
        // One round trip: MariaDB stops at the first failing statement.
        let sql: Vec<String> = ctl.iter().map(Ctl::sql).collect();
        let r = self.raw()?.query_drop(sql.join("; ")).await;
        self.check(r)?;
        if let Some(rec) = self.redo.as_mut() {
            for c in ctl {
                match c {
                    Ctl::Savepoint(n) => {
                        rec.savepoints.retain(|(x, _)| x != n);
                        rec.savepoints.push((n.clone(), rec.stmts.len()));
                    }
                    Ctl::RollbackTo(n) => {
                        if let Some(&(_, at)) = rec.savepoints.iter().find(|(x, _)| x == n) {
                            rec.stmts.truncate(at);
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn start_redo(&mut self, timestamp_us: u64) -> bool {
        self.redo = Some(Recording {
            timestamp_us,
            ..Recording::default()
        });
        true
    }

    fn take_redo(&mut self) -> Result<Vec<crate::redo::RedoStmt>, String> {
        let Some(rec) = self.redo.take() else {
            return Ok(Vec::new());
        };
        match rec.unsafe_why {
            Some(why) => Err(why),
            None => Ok(rec.stmts),
        }
    }
}

/// A cluster as the static backend knows it.
#[derive(Debug, Clone)]
pub struct MyCluster {
    /// The writer's connection options for branches (the session's backend
    /// account).
    pub opts: Opts,
    /// The admin's options (`sprig_ctl`); usually the same server.
    pub admin_opts: Opts,
    /// The capability profile.
    pub caps: ClusterCaps,
}

/// The longest the admin waits for a pooled connection.
pub const ADMIN_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(20);

/// Where an admin gets its pools when clusters come and go (the
/// controller's cluster registry): a pool of `sprig_ctl` connections to a
/// cluster's writer. The source owns the pools, so a writer change can
/// hand out a new one.
#[async_trait::async_trait]
pub trait PoolSource: Send + Sync {
    /// The pool for `cluster`'s writer.
    ///
    /// # Errors
    /// An unknown cluster, no writer, or a credential that doesn't
    /// resolve.
    async fn pool(&self, cluster: &ClusterId) -> DbResult<Pool>;
}

enum Pools {
    Fixed(BTreeMap<ClusterId, Pool>),
    Source(Arc<dyn PoolSource>),
}

/// The admin on pooled `mysql_async` connections: one small pool per
/// cluster, fixed ([`MyAdmin::new`]) or from a [`PoolSource`]
/// ([`MyAdmin::over`]).
pub struct MyAdmin {
    pools: Pools,
    /// Schemas known to have their redo table (Sprig 2PC).
    redo_ready: std::sync::Mutex<std::collections::HashSet<(ClusterId, String)>>,
}

impl std::fmt::Debug for MyAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.pools {
            Pools::Fixed(p) => f
                .debug_struct("MyAdmin")
                .field("clusters", &p.keys().collect::<Vec<_>>())
                .finish(),
            Pools::Source(_) => f.debug_struct("MyAdmin").finish_non_exhaustive(),
        }
    }
}

fn pool_for(opts: &Opts) -> Pool {
    let constraints = PoolConstraints::new(0, 4).unwrap_or_default();
    let pool_opts = PoolOpts::default()
        .with_constraints(constraints)
        .with_inactive_connection_ttl(Duration::from_secs(60))
        .with_ttl_check_interval(Duration::from_secs(30));
    Pool::new(OptsBuilder::from_opts(opts.clone()).pool_opts(pool_opts))
}

fn q(s: &str) -> String {
    s.replace('`', "``")
}

fn decision_of(row: &Row, dtid_col: Option<usize>, off: usize) -> Option<DecisionRow> {
    let outcome: String = row.get(off)?;
    let decider: Option<Vec<u8>> = row.get(off + 1)?;
    let coordinator: String = row.get(off + 2)?;
    let participants: Option<Vec<u8>> = row.get(off + 3)?;
    let age: Option<i64> = row.get(off + 4)?;
    let dtid = match dtid_col {
        Some(i) => {
            let b: Vec<u8> = row.get(i)?;
            Dtid::from_bytes(b.try_into().ok()?)
        }
        None => Dtid::from_bytes([0; 16]),
    };
    Some(DecisionRow {
        dtid,
        outcome: Decision::parse(&outcome)?,
        decider: decider.and_then(|b| AnchorRef::from_bytes(&b)),
        coordinator,
        participants: participants
            .map(|b| decode_participants(&b))
            .unwrap_or_default(),
        age_seconds: age.and_then(|a| u64::try_from(a).ok()),
    })
}

impl MyAdmin {
    /// An admin over `clusters`.
    #[must_use]
    pub fn new(clusters: &BTreeMap<ClusterId, MyCluster>) -> Self {
        Self {
            pools: Pools::Fixed(
                clusters
                    .iter()
                    .map(|(k, c)| (k.clone(), pool_for(&c.admin_opts)))
                    .collect(),
            ),
            redo_ready: std::sync::Mutex::default(),
        }
    }

    /// An admin over the pools `source` hands out.
    #[must_use]
    pub fn over(source: Arc<dyn PoolSource>) -> Self {
        Self {
            pools: Pools::Source(source),
            redo_ready: std::sync::Mutex::default(),
        }
    }

    async fn conn(&self, cluster: &ClusterId) -> DbResult<Conn> {
        let p = match &self.pools {
            Pools::Fixed(m) => m
                .get(cluster)
                .cloned()
                .ok_or_else(|| DbError::new(0, format!("unknown cluster {cluster}")))?,
            Pools::Source(s) => s.pool(cluster).await?,
        };
        match tokio::time::timeout(ADMIN_ACQUIRE_TIMEOUT, p.get_conn()).await {
            Ok(r) => r.map_err(|e| db_error(&e)),
            Err(_) => Err(DbError::lost(format!(
                "no admin connection to {cluster} within {} s",
                ADMIN_ACQUIRE_TIMEOUT.as_secs()
            ))),
        }
    }

    /// The schemas on `cluster` named `t<tenant>_d<db>_g<group>_s<n>`, any
    /// `n` (`SHOW DATABASES LIKE`, which opens no tables).
    ///
    /// # Errors
    /// The server's error, or a lost connection.
    pub async fn group_schemas(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        db: DbId,
        group: GroupId,
    ) -> DbResult<Vec<String>> {
        let mut c = self.conn(cluster).await?;
        let names: Vec<String> = c
            .query(format!(
                "SHOW DATABASES LIKE 't{tenant}\\_d{db}\\_g{group}\\_s%'"
            ))
            .await
            .map_err(|e| db_error(&e))?;
        Ok(crate::locate::group_schemas_of(tenant, db, group, names))
    }

    /// Closes the pools (a fixed admin; a source closes its own).
    pub async fn close(self) {
        if let Pools::Fixed(m) = self.pools {
            for (_, p) in m {
                let _ = p.disconnect().await;
            }
        }
    }
}

#[async_trait::async_trait]
impl ClusterAdmin for MyAdmin {
    async fn xa_recover(&self, cluster: &ClusterId) -> DbResult<Vec<Xid>> {
        let mut c = self.conn(cluster).await?;
        let rows: Vec<Row> = c
            .query("XA RECOVER FORMAT='SQL'")
            .await
            .map_err(|e| db_error(&e))?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let fmt: i64 = r.get(0)?;
                if fmt != i64::from(FORMAT_ID) {
                    return None;
                }
                let data: Vec<u8> = r.get(3)?;
                Xid::parse_sql(std::str::from_utf8(&data).ok()?)
            })
            .collect())
    }

    async fn xa_finish(&self, cluster: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        let verb = if commit { "COMMIT" } else { "ROLLBACK" };
        c.query_drop(format!("XA {verb} {}", xid.sql()))
            .await
            .map_err(|e| db_error(&e))
    }

    async fn kill(&self, cluster: &ClusterId, thread: u64) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        match c.query_drop(format!("KILL CONNECTION {thread}")).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let d = db_error(&e);
                // 1094: unknown thread, already gone.
                if d.errno == 1094 { Ok(()) } else { Err(d) }
            }
        }
    }

    async fn insert_decision(
        &self,
        cluster: &ClusterId,
        schema: &str,
        row: &DecisionRow,
    ) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        c.query_drop(format!(
            "SET SESSION innodb_lock_wait_timeout = {ADMIN_LOCK_WAIT_S}; {}",
            crate::backend::insert_decision_sql(schema, row)
        ))
        .await
        .map_err(|e| db_error(&e))
    }

    async fn read_decision(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        let mut c = self.conn(cluster).await?;
        let rows: Vec<Row> = c
            .query(format!(
                "SELECT outcome, decider, coordinator, participants, \
                 TIMESTAMPDIFF(SECOND, created_at, NOW(6)) FROM `{}`.`{DTX_TABLE}` \
                 WHERE dtid = X'{}'",
                q(schema),
                hex(&dtid.to_bytes())
            ))
            .await
            .map_err(|e| db_error(&e))?;
        Ok(rows
            .first()
            .and_then(|r| decision_of(r, None, 0))
            .map(|mut d| {
                d.dtid = dtid;
                d
            }))
    }

    async fn settle_pending(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
        outcome: Decision,
    ) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        c.query_drop(format!(
            "UPDATE `{}`.`{DTX_TABLE}` SET outcome = '{}' WHERE dtid = X'{}' AND outcome = 'pending'",
            q(schema),
            outcome.as_str(),
            hex(&dtid.to_bytes())
        ))
        .await
        .map_err(|e| db_error(&e))
    }

    async fn decision_rows(&self, cluster: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
        let mut c = self.conn(cluster).await?;
        let schemas: Vec<String> = c
            .query(format!(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = '{DTX_TABLE}'"
            ))
            .await
            .map_err(|e| db_error(&e))?;
        let mut out = Vec::new();
        for s in schemas {
            let rows: Vec<Row> = c
                .query(format!(
                    "SELECT dtid, outcome, decider, coordinator, participants, \
                     TIMESTAMPDIFF(SECOND, created_at, NOW(6)) FROM `{}`.`{DTX_TABLE}`",
                    q(&s)
                ))
                .await
                .map_err(|e| db_error(&e))?;
            out.extend(
                rows.iter()
                    .filter_map(|r| decision_of(r, Some(0), 1))
                    .map(|d| (s.clone(), d)),
            );
        }
        Ok(out)
    }

    async fn delete_decision(&self, cluster: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        c.query_drop(format!(
            "DELETE FROM `{}`.`{DTX_TABLE}` WHERE dtid = X'{}'",
            q(schema),
            hex(&dtid.to_bytes())
        ))
        .await
        .map_err(|e| db_error(&e))
    }

    async fn ensure_redo(&self, cluster: &ClusterId, schema: &str) -> DbResult<()> {
        let key = (cluster.clone(), schema.to_owned());
        if self
            .redo_ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&key)
        {
            return Ok(());
        }
        let mut c = self.conn(cluster).await?;
        c.query_drop(crate::redo::redo_table_sql(schema))
            .await
            .map_err(|e| db_error(&e))?;
        self.redo_ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key);
        Ok(())
    }

    async fn insert_redo(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
        gtrid: &crate::xid::Gtrid,
        stmts: &[crate::redo::RedoStmt],
    ) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        let sql = crate::redo::insert_sql(schema, dtid, gtrid, stmts);
        match c.query_drop(&sql).await {
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => {
                c.query_drop(crate::redo::redo_table_sql(schema))
                    .await
                    .map_err(|e| db_error(&e))?;
                c.query_drop(&sql).await.map_err(|e| db_error(&e))
            }
            r => r.map_err(|e| db_error(&e)),
        }
    }

    async fn redo_branches(&self, cluster: &ClusterId) -> DbResult<Vec<crate::redo::RedoBranch>> {
        use crate::redo::{MARK_SEQ, REDO_TABLE, RedoBranch, RedoMark};
        let mut c = self.conn(cluster).await?;
        let schemas: Vec<String> = c
            .query(format!(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = '{REDO_TABLE}'"
            ))
            .await
            .map_err(|e| db_error(&e))?;
        let mut out = Vec::new();
        for s in schemas {
            let rows: Vec<(Vec<u8>, Vec<u8>, i64, Option<String>, Option<i64>)> = c
                .query(format!(
                    "SELECT dtid, MIN(gtrid), SUM(seq < {MARK_SEQ}),                      MAX(IF(seq = {MARK_SEQ}, CAST(ctx AS CHAR), NULL)),                      TIMESTAMPDIFF(SECOND, MIN(created_at), NOW(6))                      FROM `{}`.`{REDO_TABLE}` GROUP BY dtid",
                    q(&s)
                ))
                .await
                .map_err(|e| db_error(&e))?;
            for (d, g, n, mark, age) in rows {
                let (Ok(d), Some(gtrid)) = (
                    <[u8; 16]>::try_from(d.as_slice()),
                    crate::xid::Gtrid::from_bytes(&g),
                ) else {
                    continue;
                };
                out.push(RedoBranch {
                    schema: s.clone(),
                    dtid: Dtid::from_bytes(d),
                    gtrid,
                    age_seconds: age.and_then(|a| u64::try_from(a).ok()).unwrap_or(0),
                    statements: usize::try_from(n).unwrap_or(0),
                    mark: match mark.as_deref() {
                        Some("done") => Some(RedoMark::Done),
                        Some(_) => Some(RedoMark::Replayed),
                        None => None,
                    },
                });
            }
        }
        Ok(out)
    }

    #[allow(clippy::many_single_char_names)] // short names: c, d, g, n, r
    async fn replay_redo(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<crate::redo::Replay> {
        use crate::redo::{MARK_SEQ, REDO_TABLE, RedoMark, RedoStmt, Replay};
        let mut c = self.conn(cluster).await?;
        let d = hex(&dtid.to_bytes());
        let r = async {
            c.query_drop(format!(
                "SET SESSION innodb_lock_wait_timeout = {ADMIN_LOCK_WAIT_S}"
            ))
            .await?;
            c.query_drop("START TRANSACTION").await?;
            // The marker first: a holder still finishing holds it (this
            // waits, then finds it), or committed it (a duplicate).
            let rows: Vec<Vec<u8>> = c
                .query(format!(
                    "SELECT gtrid FROM `{}`.`{REDO_TABLE}` WHERE dtid = X'{d}' LIMIT 1",
                    q(schema)
                ))
                .await?;
            let Some(g) = rows.first().and_then(|g| crate::xid::Gtrid::from_bytes(g)) else {
                c.query_drop("ROLLBACK").await?;
                return Ok(Replay::Gone);
            };
            match c
                .query_drop(crate::redo::mark_sql(schema, dtid, &g, RedoMark::Replayed))
                .await
            {
                Err(mysql_async::Error::Server(e)) if e.code == 1062 => {
                    c.query_drop("ROLLBACK").await?;
                    return Ok(Replay::Gone);
                }
                r => r?,
            }
            let stmts: Vec<(u64, Vec<u8>, Vec<u8>)> = c
                .query(format!(
                    "SELECT seq, ctx, stmt FROM `{}`.`{REDO_TABLE}` WHERE dtid = X'{d}'                      AND seq < {MARK_SEQ} ORDER BY seq FOR UPDATE",
                    q(schema)
                ))
                .await?;
            if stmts.is_empty() {
                c.query_drop("ROLLBACK").await?;
                return Ok(Replay::Gone);
            }
            for (seq, ctx, stmt) in stmts {
                let r = RedoStmt::from_ctx(
                    String::from_utf8_lossy(&stmt).into_owned(),
                    &String::from_utf8_lossy(&ctx),
                );
                if let Some(cs) = &r.charset
                    && cs.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    c.query_drop(format!("SET NAMES {cs}")).await?;
                }
                c.query_drop(r.replay_sql()).await?;
                let n = c.affected_rows();
                if r.affected != u64::MAX && n != r.affected {
                    c.query_drop("ROLLBACK").await?;
                    return Ok(Replay::Hazard(format!(
                        "statement {seq} changed {n} rows on replay, {} when it ran",
                        r.affected
                    )));
                }
            }
            c.query_drop(format!(
                "DELETE FROM `{}`.`{REDO_TABLE}` WHERE dtid = X'{d}' AND seq < {MARK_SEQ}",
                q(schema)
            ))
            .await?;
            c.query_drop("COMMIT").await?;
            Ok::<Replay, mysql_async::Error>(Replay::Applied)
        }
        .await;
        match r {
            Ok(x) => {
                let _ = c.query_drop("SET NAMES utf8mb4").await;
                Ok(x)
            }
            Err(e) => {
                let _ = c.query_drop("ROLLBACK").await;
                Err(db_error(&e))
            }
        }
    }

    async fn delete_redo(&self, cluster: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        let mut c = self.conn(cluster).await?;
        match c.query_drop(crate::redo::delete_sql(schema, dtid)).await {
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => Ok(()),
            r => r.map_err(|e| db_error(&e)),
        }
    }
}

/// A backend over fixed clusters: dedicated branch connections and pooled
/// admin connections.
#[derive(Debug, Clone)]
pub struct MyBackend {
    clusters: Arc<BTreeMap<ClusterId, MyCluster>>,
    admin: Arc<MyAdmin>,
}

impl MyBackend {
    /// A backend over `clusters`.
    #[must_use]
    pub fn new(clusters: BTreeMap<ClusterId, MyCluster>) -> Self {
        let admin = Arc::new(MyAdmin::new(&clusters));
        Self {
            clusters: Arc::new(clusters),
            admin,
        }
    }

    /// The admin, typed.
    #[must_use]
    pub fn my_admin(&self) -> Arc<MyAdmin> {
        self.admin.clone()
    }
}

#[async_trait::async_trait]
impl TxnBackend for MyBackend {
    type Conn = MyConn;

    fn caps(&self, cluster: &ClusterId) -> ClusterCaps {
        self.clusters
            .get(cluster)
            .map(|c| c.caps)
            .unwrap_or_default()
    }

    async fn checkout(&self, cluster: &ClusterId) -> DbResult<MyConn> {
        let c = self
            .clusters
            .get(cluster)
            .ok_or_else(|| DbError::new(0, format!("unknown cluster {cluster}")))?;
        MyConn::connect(c.opts.clone()).await
    }

    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
    }
}
