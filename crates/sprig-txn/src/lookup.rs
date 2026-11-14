//! Lookup sessions on a distributed transaction: `DistTxn`
//! implements `sprig_shard::lookup::LookupSessions`, so lookup rows
//! written in **Main** commit atomically with the data, through the
//! coordinator (XA across clusters, last-resource commit with Galera).
//!
//! - **Pre**: one plain transaction per cluster on its own connection,
//!   with `lookup.lock_wait_seconds`, committed before Main.
//! - **Main**: the coordinator's branch on the target's cluster; every
//!   statement goes through the write rule.
//! - **Post**: buffered, run in their own short transaction after Main
//!   commits; failures only leave orphans for the orphan scan.
//!
//! Each shard's statements go to the cluster's writer, through one
//! connection per cluster: on Galera one shard is never written through
//! two nodes (owner checks rely on it).
//!
//! `DistTxn` is generic over the coordinator's backend: `mysql_async`
//! connections ([`MyBackend`]) in tests and jobs, the gateway's own
//! backend sessions in the router, which keeps one `DistTxn` per session
//! and reaches the coordinator through it (`Deref`). Its connections
//! implement [`DistConn`].

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use mysql_async::{Params, Row};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::lookup::{LookupSessions, Target};

use crate::backend::{BranchConn, DbError, DbResult, TxnBackend};
use crate::coordinator::{CommitOutcome, Intent, ShardRef, TxnCoordinator};
use crate::mysql::{MyBackend, MyConn};

/// Maps a lookup target to the shard it anchors (database, group, shard,
/// hash point), from the caller's shard map.
pub type AnchorOf = Arc<dyn Fn(&Target) -> ShardRef + Send + Sync>;

/// A connection lookup statements run on: a branch's (Main), or a plain
/// one of the same backend (Pre, Post, reads).
#[async_trait::async_trait]
pub trait DistConn: BranchConn {
    /// Runs one statement without parameters.
    ///
    /// # Errors
    /// The server's error, or a lost connection.
    async fn run(&mut self, sql: &str) -> DbResult<()>;

    /// Runs one statement with positional parameters and returns its rows.
    ///
    /// # Errors
    /// The server's error, or a lost connection.
    async fn query(&mut self, sql: &str, params: Params) -> DbResult<Vec<Row>>;
}

#[async_trait::async_trait]
impl DistConn for MyConn {
    async fn run(&mut self, sql: &str) -> DbResult<()> {
        self.exec(sql).await
    }

    async fn query(&mut self, sql: &str, params: Params) -> DbResult<Vec<Row>> {
        self.exec_params(sql, params).await
    }
}

fn sql_err(e: &DbError) -> SprigError {
    if e.is_lost() {
        SprigError::new(SprigCode::BackendLost, "lost the backend connection")
    } else {
        SprigError::backend(e.errno, *b"HY000", e.message.clone())
    }
}

/// A distributed transaction with lookup Pre, Main and Post sessions.
pub struct DistTxn<B: TxnBackend = MyBackend> {
    txn: TxnCoordinator<B>,
    backend: Arc<B>,
    anchor_of: AnchorOf,
    lock_wait_seconds: u64,
    pre: BTreeMap<ClusterId, B::Conn>,
    post: Vec<(Target, String, Params)>,
    /// Post statements that failed in the last commit (orphans left for
    /// the scan).
    pub post_failures: u64,
}

impl<B: TxnBackend> std::fmt::Debug for DistTxn<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistTxn")
            .field("txn", &self.txn)
            .field("pre", &self.pre.keys().collect::<Vec<_>>())
            .field("post", &self.post.len())
            .finish_non_exhaustive()
    }
}

impl<B: TxnBackend> Deref for DistTxn<B> {
    type Target = TxnCoordinator<B>;
    fn deref(&self) -> &TxnCoordinator<B> {
        &self.txn
    }
}

impl<B: TxnBackend> DerefMut for DistTxn<B> {
    fn deref_mut(&mut self) -> &mut TxnCoordinator<B> {
        &mut self.txn
    }
}

impl<B: TxnBackend> DistTxn<B>
where
    B::Conn: DistConn,
{
    /// Wraps a coordinator (with `BEGIN` already run, or not: Main opens
    /// branches on first use). `lock_wait_seconds` is
    /// `lookup.lock_wait_seconds`.
    pub fn new(
        txn: TxnCoordinator<B>,
        backend: Arc<B>,
        anchor_of: AnchorOf,
        lock_wait_seconds: u64,
    ) -> Self {
        Self {
            txn,
            backend,
            anchor_of,
            lock_wait_seconds,
            pre: BTreeMap::new(),
            post: Vec::new(),
            post_failures: 0,
        }
    }

    /// The coordinator, for the transaction's other statements.
    pub fn coordinator(&mut self) -> &mut TxnCoordinator<B> {
        &mut self.txn
    }

    /// Replaces how targets map to anchors (the caller's shard maps moved).
    pub fn set_anchor_of(&mut self, anchor_of: AnchorOf) {
        self.anchor_of = anchor_of;
    }

    /// Whether lookup work (Pre or Post) is pending.
    #[must_use]
    pub fn has_lookup_work(&self) -> bool {
        !self.pre.is_empty() || !self.post.is_empty()
    }

    /// Commits every Pre, then Main through the coordinator, then Post.
    /// The `DistTxn` is ready for the next transaction afterwards.
    ///
    /// # Errors
    /// A failing Pre commit (everything is rolled back), or the
    /// coordinator's commit error (39443, 39441, ...).
    pub async fn commit(&mut self) -> Result<(CommitOutcome, u64)> {
        let pre = std::mem::take(&mut self.pre);
        let mut it = pre.into_iter();
        while let Some((_, mut c)) = it.next() {
            if let Err(e) = c.run("COMMIT").await {
                for (_, mut r) in it.by_ref() {
                    let _ = r.run("ROLLBACK").await;
                }
                self.post.clear();
                self.txn.rollback().await?;
                return Err(sql_err(&e));
            }
        }
        let outcome = match self.txn.commit().await {
            Ok(o) => o,
            Err(e) => {
                self.post.clear();
                return Err(e);
            }
        };
        let mut failures = 0;
        for (t, sql, p) in std::mem::take(&mut self.post) {
            let r = async {
                let mut c = self.backend.checkout(&t.cluster).await?;
                // Its own transaction: the session may run with
                // `autocommit = 0`.
                c.run("START TRANSACTION").await?;
                c.query(&sql, p).await?;
                c.run("COMMIT").await
            }
            .await;
            if let Err(e) = r {
                tracing::warn!(error = %e, "lookup Post statement failed; the orphan scan removes it");
                failures += 1;
            }
        }
        self.post_failures = failures;
        Ok((outcome, failures))
    }

    /// Rolls everything back; Post statements are dropped.
    ///
    /// # Errors
    /// None today.
    pub async fn rollback(&mut self) -> Result<()> {
        for (_, mut c) in std::mem::take(&mut self.pre) {
            let _ = c.run("ROLLBACK").await;
        }
        self.post.clear();
        self.txn.rollback().await
    }

    /// Reads lookup rows on `t` as this transaction sees them: through
    /// its Pre session on that cluster when one is open (its own claims
    /// are uncommitted there), else on a fresh connection (committed
    /// rows).
    ///
    /// # Errors
    /// The server's error.
    pub async fn read(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        if let Some(c) = self.pre.get_mut(&t.cluster) {
            return c.query(sql, params).await.map_err(|e| sql_err(&e));
        }
        let mut c = self
            .backend
            .checkout(&t.cluster)
            .await
            .map_err(|e| sql_err(&e))?;
        c.query(sql, params).await.map_err(|e| sql_err(&e))
    }
}

#[async_trait::async_trait]
impl<B: TxnBackend> LookupSessions for DistTxn<B>
where
    B::Conn: DistConn + Sync,
{
    async fn pre(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        if !self.pre.contains_key(&t.cluster) {
            let mut c = self
                .backend
                .checkout(&t.cluster)
                .await
                .map_err(|e| sql_err(&e))?;
            c.run(&format!(
                "SET SESSION innodb_lock_wait_timeout = {}",
                self.lock_wait_seconds.max(1)
            ))
            .await
            .map_err(|e| sql_err(&e))?;
            c.run("START TRANSACTION").await.map_err(|e| sql_err(&e))?;
            self.pre.insert(t.cluster.clone(), c);
        }
        let c = self
            .pre
            .get_mut(&t.cluster)
            .ok_or_else(|| SprigError::internal("pre session vanished"))?;
        c.query(sql, params).await.map_err(|e| sql_err(&e))
    }

    async fn main(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        let shard = (self.anchor_of)(t);
        let b = self.txn.branch(&t.cluster, &shard, Intent::Write).await?;
        match b.conn_mut().query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                self.txn.statement_failed(&t.cluster, &e).await?;
                Err(sql_err(&e))
            }
        }
    }

    fn post(&mut self, t: &Target, sql: String, params: Params) {
        self.post.push((t.clone(), sql, params));
    }
}
