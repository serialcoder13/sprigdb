//! Bolt transactions over a graph's shards (chapter 43, "Writes and
//! transactions"; chapter 18).
//!
//! - **Writes** run on chapter 18's coordinator: a [`TxnCoordinator`] whose
//!   branches are [`GraphBranch`]es, pooled `sprig_ctl` connections of each
//!   shard's cluster (the gateway's Bolt pool), at `bolt.isolation`. One
//!   cluster commits locally; several commit with XA two-phase commit and a
//!   last resource, as SQL sessions do. A branch whose transaction is still
//!   open (or prepared) when it is dropped closes its connection instead of
//!   returning it to the pool, so a prepared branch detaches for the
//!   resolver.
//! - **Reads** run on [`ReadConns`]: one read-only transaction per cluster,
//!   on a replica after the session's causal wait or on the writer.
//! - **Positions.** After a branch that wrote commits, its `@@last_gtid`
//!   joins the session's causal target (bookmarks).
//! - **Shard maps.** Writes record the epoch of every shard they touch; a
//!   commit whose shards changed epoch in the meantime (a split, merge or
//!   move cut over) rolls back with 39424, which Bolt drivers retry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use async_trait::async_trait;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::maria::{GraphShards, ShardConns, ShardLoc};
use sprig_txn::mysql::{MyAdmin, PoolSource, db_error};
use sprig_txn::{
    BranchConn, ClusterAdmin, ClusterCaps, Ctl, DbError, DbResult, DtidGen, Intent, ShardRef,
    TxnBackend, TxnConfig, TxnCoordinator,
};

use super::GatewayGraphs;
use crate::tenant::TenantServices;

/// The coordinator's per-gateway pieces for Bolt.
pub(crate) struct CoordServices {
    dtids: Arc<DtidGen>,
    coordinator_id: String,
    admin: OnceLock<Arc<MyAdmin>>,
}

impl CoordServices {
    pub(crate) fn new() -> Self {
        let incarnation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0))
            ^ (u64::from(std::process::id()) << 40)
            ^ 0xB017;
        Self {
            dtids: Arc::new(DtidGen::new(incarnation)),
            coordinator_id: format!("gw-bolt-{incarnation:016x}"),
            admin: OnceLock::new(),
        }
    }
}

/// `sprig_ctl` pools for the admin (decision rows, finishing prepared
/// branches).
struct CtlPools(Arc<TenantServices>);

#[async_trait]
impl PoolSource for CtlPools {
    async fn pool(&self, cluster: &ClusterId) -> DbResult<mysql_async::Pool> {
        self.0
            .locator
            .ctl_pool(cluster)
            .await
            .map_err(|e| DbError::lost(e.to_string()))
    }
}

/// Commit positions (`cluster`, `@@last_gtid`) of branches that wrote.
pub(crate) type Positions = Arc<Mutex<Vec<(String, String)>>>;

/// One branch: a pooled `sprig_ctl` connection of a cluster.
pub(crate) struct GraphBranch {
    conn: Option<Conn>,
    thread: u64,
    cluster: String,
    /// A transaction (plain or XA) is open or prepared on it.
    open: bool,
    /// The statement layer wrote through it.
    pub(crate) wrote: bool,
    positions: Positions,
    /// Sprig 2PC: a held branch's record of its writes (R8 round 5).
    redo: Option<Redo>,
}

/// What a held branch wrote, for its redo rows (chapter 18, "Sprig 2PC").
#[derive(Debug, Default)]
struct Redo {
    timestamp_us: u64,
    stmts: Vec<sprig_txn::RedoStmt>,
    unsafe_why: Option<String>,
}

impl std::fmt::Debug for GraphBranch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphBranch")
            .field("cluster", &self.cluster)
            .field("thread", &self.thread)
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

impl GraphBranch {
    /// The connection, for the store's statements.
    ///
    /// # Errors
    /// 39001-class when the connection broke earlier.
    pub(crate) fn raw(&mut self) -> Result<&mut Conn> {
        self.conn
            .as_mut()
            .ok_or_else(|| SprigError::new(SprigCode::BackendLost, "lost the backend connection"))
    }

    /// A held branch records a write the store ran (its values in the
    /// text), with the connection's affected-row count.
    pub(crate) fn record(&mut self, sql: &str) {
        let (Some(r), Some(c)) = (self.redo.as_mut(), self.conn.as_ref()) else {
            return;
        };
        if r.unsafe_why.is_none()
            && let Some(why) = sprig_txn::redo::unsafe_reason(sql)
        {
            r.unsafe_why = Some(why.to_owned());
        }
        let update = sql.trim_start().to_ascii_uppercase().starts_with("UPDATE");
        r.stmts.push(sprig_txn::RedoStmt {
            sql: sql.to_owned(),
            // UPDATE counts depend on CLIENT_FOUND_ROWS: not checked.
            affected: if update { u64::MAX } else { c.affected_rows() },
            insert_id: 0,
            vars: Vec::new(),
            charset: None,
            timestamp_us: Some(r.timestamp_us),
        });
    }
}

impl Drop for GraphBranch {
    fn drop(&mut self) {
        // An open or prepared transaction must not go back to the pool:
        // closing the connection rolls it back, or detaches a prepared XA
        // branch for the resolver.
        if self.open
            && let Some(c) = self.conn.take()
            && let Ok(h) = tokio::runtime::Handle::try_current()
        {
            h.spawn(async move {
                let _ = c.disconnect().await;
            });
        }
    }
}

#[async_trait]
impl BranchConn for GraphBranch {
    fn thread_id(&self) -> u64 {
        self.thread
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        if ctl.is_empty() {
            return Ok(());
        }
        let sql: Vec<String> = ctl.iter().map(Ctl::sql).collect();
        let Some(c) = self.conn.as_mut() else {
            return Err(DbError::lost("connection closed"));
        };
        if let Err(e) = c.query_drop(sql.join("; ")).await {
            let d = db_error(&e);
            if d.is_lost() {
                self.conn = None;
            }
            return Err(d);
        }
        let mut committed = false;
        for x in ctl {
            match x {
                Ctl::Begin { .. } | Ctl::XaStart(_) => self.open = true,
                Ctl::Commit | Ctl::XaCommitOnePhase(_) | Ctl::XaCommit(_) => {
                    self.open = false;
                    committed = true;
                }
                Ctl::Rollback | Ctl::XaRollback(_) => {
                    self.open = false;
                    self.wrote = false;
                }
                _ => {}
            }
        }
        if committed && self.wrote {
            self.wrote = false;
            if let Some(c) = self.conn.as_mut()
                && let Ok(Some(g)) = c.query_first::<String, _>("SELECT @@last_gtid").await
                && !g.is_empty()
            {
                self.positions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((self.cluster.clone(), g));
            }
        }
        Ok(())
    }

    fn start_redo(&mut self, timestamp_us: u64) -> bool {
        self.redo = Some(Redo {
            timestamp_us,
            ..Redo::default()
        });
        true
    }

    fn take_redo(&mut self) -> std::result::Result<Vec<sprig_txn::RedoStmt>, String> {
        let Some(r) = self.redo.take() else {
            return Ok(Vec::new());
        };
        match r.unsafe_why {
            Some(why) => Err(why),
            None => Ok(r.stmts),
        }
    }
}

/// One Bolt session's branch factory.
pub(crate) struct GraphBackend {
    graphs: Arc<GatewayGraphs>,
    caps: Mutex<HashMap<ClusterId, ClusterCaps>>,
    /// `READ COMMITTED` or `REPEATABLE READ`.
    isolation: Mutex<&'static str>,
    /// Chapter 18's `txn.*` settings for the next coordinator (round 5:
    /// `txn.non_xa_policy = sprig_2pc` holds branches on clusters without
    /// XA).
    config: Mutex<Option<TxnConfig>>,
    /// `innodb_lock_wait_timeout` of branches opened from now on, in
    /// seconds: the Bolt transaction's `tx_timeout`, rounded up (none: the
    /// server's default, which the pool's connection reset restores).
    lock_wait: Mutex<Option<u64>>,
    pub(crate) positions: Positions,
}

impl std::fmt::Debug for GraphBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphBackend").finish_non_exhaustive()
    }
}

impl GraphBackend {
    pub(crate) fn new(graphs: Arc<GatewayGraphs>) -> Self {
        Self {
            graphs,
            caps: Mutex::new(HashMap::new()),
            isolation: Mutex::new("READ COMMITTED"),
            config: Mutex::new(None),
            lock_wait: Mutex::new(None),
            positions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The isolation level of branches opened from now on.
    pub(crate) fn set_isolation(&self, level: &'static str) {
        *self
            .isolation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = level;
    }

    /// The lock wait of branches opened from now on: `tx_timeout` rounded
    /// up to whole seconds (MariaDB's unit), at least 1.
    pub(crate) fn set_lock_wait(&self, timeout: Option<std::time::Duration>) {
        *self
            .lock_wait
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = timeout.map(|t| {
            t.as_millis()
                .div_ceil(1000)
                .max(1)
                .try_into()
                .unwrap_or(u64::MAX)
        });
    }

    /// Learns `cluster`'s capability profile (chapter 09's `derived.xa`).
    async fn ensure_caps(&self, cluster: &ClusterId) {
        if self
            .caps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(cluster)
        {
            return;
        }
        let xa = self
            .graphs
            .svc
            .locator
            .cluster_xa(cluster)
            .await
            .unwrap_or(false);
        self.caps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), ClusterCaps { xa });
    }

    /// The `txn.*` settings of coordinators made from now on.
    pub(crate) fn set_config(&self, mut cfg: TxnConfig) {
        cfg.coordinator_id
            .clone_from(&self.graphs.coord.coordinator_id);
        *self.config.lock().unwrap_or_else(PoisonError::into_inner) = Some(cfg);
    }

    /// A coordinator for one transaction of `tenant`.
    pub(crate) fn coordinator(self: &Arc<Self>, tenant: TenantId) -> TxnCoordinator<Self> {
        let s = &self.graphs.coord;
        let cfg = self
            .config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| TxnConfig {
                coordinator_id: s.coordinator_id.clone(),
                ..TxnConfig::default()
            });
        TxnCoordinator::new(Arc::clone(self), Arc::clone(&s.dtids), tenant, cfg)
    }

    /// Commit positions gathered since the last call.
    pub(crate) fn take_positions(&self) -> Vec<(String, String)> {
        std::mem::take(
            &mut *self
                .positions
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }
}

#[async_trait]
impl TxnBackend for GraphBackend {
    type Conn = GraphBranch;

    fn caps(&self, cluster: &ClusterId) -> ClusterCaps {
        self.caps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .copied()
            .unwrap_or_default()
    }

    async fn checkout(&self, cluster: &ClusterId) -> DbResult<GraphBranch> {
        let pool = self
            .graphs
            .session_pool(cluster)
            .await
            .map_err(|e| DbError::lost(e.to_string()))?;
        let mut c = pool.get_conn().await.map_err(|e| db_error(&e))?;
        let level = *self
            .isolation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        c.query_drop(format!("SET SESSION TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .map_err(|e| db_error(&e))?;
        let lock_wait = *self
            .lock_wait
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(secs) = lock_wait {
            c.query_drop(format!("SET SESSION innodb_lock_wait_timeout = {secs}"))
                .await
                .map_err(|e| db_error(&e))?;
        }
        let thread = u64::from(c.id());
        Ok(GraphBranch {
            conn: Some(c),
            thread,
            cluster: cluster.to_string(),
            open: false,
            wrote: false,
            positions: Arc::clone(&self.positions),
            redo: None,
        })
    }

    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        let a = self.graphs.coord.admin.get_or_init(|| {
            Arc::new(MyAdmin::over(Arc::new(CtlPools(Arc::clone(
                &self.graphs.svc,
            )))))
        });
        Arc::clone(a) as Arc<dyn ClusterAdmin>
    }
}

/// A shard a write touched: its cluster, shard id and epoch.
pub(crate) type Touched = (String, u32, u64);

/// The coordinator's branches as a statement's connections.
pub(crate) struct CoordConns<'a> {
    pub(crate) coord: &'a mut TxnCoordinator<GraphBackend>,
    pub(crate) backend: Arc<GraphBackend>,
    pub(crate) db: DbId,
    pub(crate) group: GroupId,
    /// Shards written so far (epoch check at commit).
    pub(crate) touched: &'a mut Vec<Touched>,
    /// An auto-commit statement's graph: `CALL { } IN TRANSACTIONS`
    /// commits its batches here (R8 round 8); `None` in an explicit
    /// transaction.
    pub(crate) batches: Option<(Arc<crate::graphs::GatewayGraphs>, crate::graphs::GraphRef)>,
}

fn cluster_id(s: &str) -> Result<ClusterId> {
    ClusterId::new(s).map_err(|e| SprigError::internal(format!("cluster id {s}: {e}")))
}

#[async_trait]
impl ShardConns for CoordConns<'_> {
    async fn conn(&mut self, at: &ShardLoc, write: bool) -> Result<&mut Conn> {
        let cluster = cluster_id(&at.cluster)?;
        self.backend.ensure_caps(&cluster).await;
        if write {
            let t = (at.cluster.clone(), at.shard, at.epoch);
            if !self.touched.contains(&t) {
                self.touched.push(t);
            }
        }
        let shard = ShardRef {
            db: self.db,
            group: self.group,
            shard: ShardId(at.shard),
            hash_point: at.lo,
            schema: at.schema.clone(),
        };
        let intent = if write { Intent::Write } else { Intent::Read };
        let b = self.coord.branch(&cluster, &shard, intent).await?;
        let bc = b.conn_mut();
        if write {
            bc.wrote = true;
        }
        bc.raw()
    }

    fn records(&self) -> bool {
        true
    }

    fn wrote(&mut self, at: &ShardLoc, sql: &str) {
        let Ok(cluster) = cluster_id(&at.cluster) else {
            return;
        };
        for (_, b) in self.coord.conns_mut(&[cluster]) {
            b.record(sql);
        }
    }

    fn batches(&self) -> bool {
        self.batches.is_some()
    }

    /// A batch commits as the whole statement does: the written shards'
    /// epochs checked against the map now, then chapter 18's commit (Sprig
    /// 2PC across clusters); the coordinator then starts afresh.
    async fn end_batch(&mut self, ok: bool) -> Result<()> {
        let Some((graphs, g)) = &self.batches else {
            return Err(sprig_graph::store::not_implicit());
        };
        if ok {
            let checked = if g.shards.group != 0 && !self.touched.is_empty() {
                match graphs.refresh(g).await {
                    Ok(now) => check_epochs(self.touched, &now.shards),
                    Err(e) => Err(e),
                }
            } else {
                Ok(())
            };
            if let Err(e) = checked {
                let _ = self.coord.rollback().await;
                self.touched.clear();
                return Err(e);
            }
            self.touched.clear();
            self.coord.commit().await.map(drop)
        } else {
            self.touched.clear();
            self.coord.rollback().await
        }
    }
}

/// Read-only transactions, one per cluster (a replica after the causal
/// wait, or the writer).
#[derive(Default)]
pub(crate) struct ReadConns {
    pub(crate) conns: HashMap<String, Conn>,
    /// An auto-commit read: `CALL { } IN TRANSACTIONS` may run (its
    /// batches have nothing to commit).
    pub(crate) implicit: bool,
}

impl std::fmt::Debug for ReadConns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadConns")
            .field("clusters", &self.conns.keys().collect::<Vec<_>>())
            .field("implicit", &self.implicit)
            .finish()
    }
}

impl ReadConns {
    /// Ends every transaction (`COMMIT` of a read-only transaction); the
    /// connections go back to their pools.
    pub(crate) async fn end(self) {
        for (_, mut c) in self.conns {
            let _ = c.query_drop("COMMIT").await;
        }
    }

    /// Clusters of `layout` without a connection yet.
    pub(crate) fn missing(&self, layout: &GraphShards) -> Vec<String> {
        let mut v: Vec<String> = layout
            .shards
            .iter()
            .map(|s| s.cluster.clone())
            .filter(|c| !self.conns.contains_key(c))
            .collect();
        v.sort();
        v.dedup();
        v
    }
}

#[async_trait]
impl ShardConns for ReadConns {
    async fn conn(&mut self, at: &ShardLoc, write: bool) -> Result<&mut Conn> {
        if write {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "a write in a read transaction (mode: \"r\")",
            ));
        }
        self.conns
            .get_mut(&at.cluster)
            .ok_or_else(|| SprigError::internal(format!("no read connection to {}", at.cluster)))
    }

    fn batches(&self) -> bool {
        self.implicit
    }

    async fn end_batch(&mut self, _ok: bool) -> Result<()> {
        Ok(())
    }
}

/// Whether the shards `touched` still have the epochs they were written
/// at in `layout` (the map now).
///
/// # Errors
/// 39424 naming the first shard that changed.
pub(crate) fn check_epochs(touched: &[Touched], now: &GraphShards) -> Result<()> {
    for (cluster, shard, epoch) in touched {
        let same = now
            .shards
            .iter()
            .any(|s| s.shard == *shard && s.epoch == *epoch && &s.cluster == cluster);
        if !same {
            return Err(SprigError::new(
                SprigCode::ShardMapChanged,
                format!(
                    "shard {shard} of the graph was split, merged or moved during the transaction; it was rolled back, retry it"
                ),
            ));
        }
    }
    Ok(())
}
