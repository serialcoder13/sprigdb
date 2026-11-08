//! Chapter 19's lookup sessions on a distributed transaction: `DistTxn`
//! implements `sprig_shard::lookup::LookupSessions`, so lookup rows
//! written in **Main** commit atomically with the data, through the
//! coordinator (XA across clusters, last-resource commit with Galera).
//!
//! - **Pre**: one plain transaction per cluster on its own connection,
//!   with `lookup.lock_wait_seconds`, committed before Main.
//! - **Main**: the coordinator's branch on the target's cluster; every
//!   statement goes through the write rule.
//! - **Post**: buffered, run in autocommit after Main commits; failures
//!   only leave orphans for chapter 19's scan.
//!
//! Each shard's statements go to the cluster's writer, through one
//! connection per cluster: on Galera one shard is never written through
//! two nodes (chapter 19 relies on it for owner checks).

use std::collections::BTreeMap;
use std::sync::Arc;

use mysql_async::{Params, Row};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::lookup::{LookupSessions, Target};

use crate::backend::{DbError, TxnBackend};
use crate::coordinator::{CommitOutcome, Intent, ShardRef, TxnCoordinator};
use crate::mysql::{MyBackend, MyConn};

/// Maps a lookup target to the shard it anchors (database, group, shard,
/// hash point), from the caller's shard map.
pub type AnchorOf = Arc<dyn Fn(&Target) -> ShardRef + Send + Sync>;

fn sql_err(e: &DbError) -> SprigError {
    if e.is_lost() {
        SprigError::new(SprigCode::BackendLost, "lost the backend connection")
    } else {
        SprigError::backend(e.errno, *b"HY000", e.message.clone())
    }
}

/// A distributed transaction with chapter 19's Pre, Main and Post.
pub struct DistTxn {
    txn: TxnCoordinator<MyBackend>,
    backend: Arc<MyBackend>,
    anchor_of: AnchorOf,
    lock_wait_seconds: u64,
    pre: BTreeMap<ClusterId, MyConn>,
    post: Vec<(Target, String, Params)>,
    /// Post statements that failed in the last commit (orphans left for
    /// the scan).
    pub post_failures: u64,
}

impl std::fmt::Debug for DistTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistTxn")
            .field("txn", &self.txn)
            .field("pre", &self.pre.keys().collect::<Vec<_>>())
            .field("post", &self.post.len())
            .finish_non_exhaustive()
    }
}

impl DistTxn {
    /// Wraps a coordinator (with `BEGIN` already run, or not: Main opens
    /// branches on first use). `lock_wait_seconds` is
    /// `lookup.lock_wait_seconds`.
    pub fn new(
        txn: TxnCoordinator<MyBackend>,
        backend: Arc<MyBackend>,
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
    pub fn coordinator(&mut self) -> &mut TxnCoordinator<MyBackend> {
        &mut self.txn
    }

    /// Commits every Pre, then Main through the coordinator, then Post.
    ///
    /// # Errors
    /// A failing Pre commit (everything is rolled back), or the
    /// coordinator's commit error (39443, 39441, ...).
    pub async fn commit(mut self) -> Result<(CommitOutcome, u64)> {
        let pre = std::mem::take(&mut self.pre);
        let mut it = pre.into_iter();
        while let Some((_, mut c)) = it.next() {
            if let Err(e) = c.exec("COMMIT").await {
                for (_, mut r) in it.by_ref() {
                    let _ = r.exec("ROLLBACK").await;
                }
                self.txn.rollback().await?;
                return Err(sql_err(&e));
            }
        }
        let outcome = self.txn.commit().await?;
        let mut failures = 0;
        for (t, sql, p) in std::mem::take(&mut self.post) {
            let r = async {
                let mut c = self.backend.checkout(&t.cluster).await?;
                c.exec_params(&sql, p).await
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

    /// Rolls everything back.
    ///
    /// # Errors
    /// None today.
    pub async fn rollback(mut self) -> Result<()> {
        for (_, mut c) in std::mem::take(&mut self.pre) {
            let _ = c.exec("ROLLBACK").await;
        }
        self.txn.rollback().await
    }
}

#[async_trait::async_trait]
impl LookupSessions for DistTxn {
    async fn pre(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        if !self.pre.contains_key(&t.cluster) {
            let mut c = self
                .backend
                .checkout(&t.cluster)
                .await
                .map_err(|e| sql_err(&e))?;
            c.exec(&format!(
                "SET SESSION innodb_lock_wait_timeout = {}; BEGIN",
                self.lock_wait_seconds.max(1)
            ))
            .await
            .map_err(|e| sql_err(&e))?;
            self.pre.insert(t.cluster.clone(), c);
        }
        let c = self
            .pre
            .get_mut(&t.cluster)
            .ok_or_else(|| SprigError::internal("pre session vanished"))?;
        c.exec_params(sql, params).await.map_err(|e| sql_err(&e))
    }

    async fn main(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        let shard = (self.anchor_of)(t);
        let b = self.txn.branch(&t.cluster, &shard, Intent::Write).await?;
        match b.conn_mut().exec_params(sql, params).await {
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
