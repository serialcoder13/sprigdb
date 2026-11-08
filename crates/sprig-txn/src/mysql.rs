//! The coordinator's and resolver's seams on `mysql_async`: branch
//! connections, the admin and a static backend, for the controller's
//! resolver, `sprigctl txn` and the live tests. The gateway implements
//! [`BranchConn`] on its own backend connections instead.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row};
use sprig_common::ids::ClusterId;

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
        self.check(r)
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
        self.check(r)
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

/// The admin on pooled `mysql_async` connections, one small pool per
/// cluster.
pub struct MyAdmin {
    pools: BTreeMap<ClusterId, Pool>,
}

impl std::fmt::Debug for MyAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MyAdmin")
            .field("clusters", &self.pools.keys().collect::<Vec<_>>())
            .finish()
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
            pools: clusters
                .iter()
                .map(|(k, c)| (k.clone(), pool_for(&c.admin_opts)))
                .collect(),
        }
    }

    async fn conn(&self, cluster: &ClusterId) -> DbResult<Conn> {
        let p = self
            .pools
            .get(cluster)
            .ok_or_else(|| DbError::new(0, format!("unknown cluster {cluster}")))?;
        p.get_conn().await.map_err(|e| db_error(&e))
    }

    /// Closes the pools.
    pub async fn close(self) {
        for (_, p) in self.pools {
            let _ = p.disconnect().await;
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
