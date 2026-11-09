//! The router's [`ShardBackend`] in the gateway: the session's backend
//! connections, one per cluster, and chapter 18's coordinator.
//!
//! ```text
//! a coordinator transaction is open       → the cluster's branch (opened on first use:
//!                                           Intent::Write for writes and locking reads);
//!                                           every ERR goes to statement_failed
//! the session's own cluster               → the session's BackendSession
//! another cluster, outside a transaction  → the session's BackendSession for that
//!                                           cluster (state replayed, accounts ensured)
//! a transaction the coordinator doesn't
//!   know (autocommit = 0, client XA)     → own cluster only, else cross_cluster
//! ```
//!
//! A read outside a transaction opens one [`GwStream`] per shard: a new
//! `BackendSession` with the session's state and causal position, so the
//! shards run in parallel and stream (chapter 17's build step 3).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId};
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::client::{PacketKind, RespPacket};
use sprig_proto_mysql::resultset::decode_text_row;
use sprig_router::{Budget, ShardOk, ShardOutcome, ShardRows, ShardStream, Target};
use sprig_shard::KeyValueOwned;
use sprig_shard::RoutingHash;
use sprig_shard::lookup::{ClaimResult, LookupReader, OwnerRef};
use sprig_txn::lookup::DistTxn;
use sprig_txn::{BeginOptions, DbError, Intent, ShardRef, StmtMark};

use super::txn::{GwTxn, ensure_accounts, session_on};
use crate::backend::{BackendSession, StmtClass, StmtInfo};
use crate::exec::relay::{backend_error, protocol_error};

/// Chapter 18's shard reference for a target.
pub(super) fn shard_ref(t: &Target) -> ShardRef {
    ShardRef {
        db: DbId(t.at.db),
        group: GroupId(t.at.group),
        shard: ShardId(t.shard),
        hash_point: t.at.lo,
        schema: t.at.schema.clone(),
    }
}

/// The coordinator's view of a backend error.
pub(super) fn db_error(e: &SprigError) -> Option<DbError> {
    match e.kind {
        ErrorKind::Backend { errno, .. } => Some(DbError::new(errno, e.message.clone())),
        ErrorKind::Sprig(sprig_common::SprigCode::BackendLost) => {
            Some(DbError::lost(e.message.clone()))
        }
        _ => None,
    }
}

fn cluster_id(c: &str) -> Result<ClusterId> {
    ClusterId::new(c).map_err(|e| SprigError::internal(format!("cluster id {c}: {e}")))
}

/// Runs `sql` on `bs` and collects the whole answer.
pub(super) async fn run_on(
    bs: &mut BackendSession,
    sql: &str,
    budget: &mut Budget,
) -> Result<ShardOutcome> {
    let info = StmtInfo::classify(sql.as_bytes());
    bs.query(&info, sql.as_bytes()).await?;
    let caps = bs.backend_caps().unwrap_or_default();
    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut want = 0usize;
    let mut rows = Vec::new();
    let mut outcome: Option<ShardOutcome> = None;
    let mut err: Option<SprigError> = None;
    let mut over: Option<SprigError> = None;
    while let Some(p) = bs.next().await? {
        match p.kind {
            PacketKind::ColumnCount(n) => {
                want = usize::try_from(n).unwrap_or(0);
                columns.clear();
            }
            PacketKind::ColumnDef => {
                columns.push(
                    ColumnDef::decode(&p.payload, caps)
                        .map_err(|e| protocol_error(&format!("column definition: {e}")))?,
                );
            }
            PacketKind::ColumnsEof => {}
            PacketKind::Row => {
                if over.is_some() {
                    continue;
                }
                if let Err(e) = budget.charge(p.payload.len() as u64 + 16 * want as u64) {
                    over = Some(e);
                    continue;
                }
                let vals = decode_text_row(&p.payload, want)
                    .map_err(|e| protocol_error(&format!("text row: {e}")))?;
                rows.push(
                    vals.into_iter()
                        .map(|v| v.map(|b| p.payload.slice_ref(b)))
                        .collect(),
                );
            }
            PacketKind::End(t) => {
                outcome = Some(ShardOutcome::Rows(ShardRows {
                    columns: std::mem::take(&mut columns),
                    rows: std::mem::take(&mut rows),
                    warnings: t.warnings(),
                }));
            }
            PacketKind::Ok(ok) => {
                outcome = Some(ShardOutcome::Ok(ShardOk {
                    affected: ok.affected_rows,
                    last_insert_id: ok.last_insert_id,
                    warnings: ok.warnings,
                    info: String::from_utf8_lossy(&ok.info).into_owned(),
                }));
            }
            PacketKind::Err(e) => err = Some(backend_error(&e)),
            _ => return Err(protocol_error("a packet the router doesn't expect")),
        }
    }
    bs.finish().await?;
    if let Some(e) = err.or(over) {
        return Err(e);
    }
    outcome.ok_or_else(|| protocol_error("a response without an end"))
}

/// The session's connections as the router's backend.
pub(super) struct GwBackend<'a> {
    pub main: &'a mut BackendSession,
    pub main_cluster: ClusterId,
    pub others: &'a mut HashMap<ClusterId, BackendSession>,
    pub coord: &'a mut DistTxn<GwTxn>,
    pub factory: Arc<GwTxn>,
    /// The main connection is in a transaction the coordinator doesn't
    /// own.
    pub legacy_txn: bool,
    /// `router.max_parallel_shards`.
    pub parallel: usize,
    /// A multi-shard write opened an implicit coordinator transaction.
    pub implicit: bool,
    /// A multi-shard write's statement savepoint.
    pub mark: Option<StmtMark>,
    /// The plan's INSERTs write values from chapter 19's ID blocks: their
    /// OK packets' insert ids aren't `LAST_INSERT_ID()`.
    pub explicit_ids: bool,
}

impl GwBackend<'_> {
    async fn other(&mut self, cluster: &ClusterId) -> Result<&mut BackendSession> {
        if !self.others.contains_key(cluster) {
            let cat = self
                .factory
                .cat
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            ensure_accounts(&self.factory.svc, cat.as_ref(), cluster).await?;
            let bs = session_on(
                &self.factory.backends,
                &self.factory.target,
                &self.factory.opts,
                &self.main_cluster,
                cluster,
                self.main.vars(),
            );
            self.others.insert(cluster.clone(), bs);
        }
        let main_vars = self.main.vars().clone();
        let bs = self
            .others
            .get_mut(cluster)
            .ok_or_else(|| SprigError::internal("no session for the cluster"))?;
        sync_vars(bs, &main_vars);
        Ok(bs)
    }
}

/// Gives `bs` the session's state (its own default schema kept).
pub(super) fn sync_vars(bs: &mut BackendSession, main: &crate::backend::state::SessionVars) {
    let schema = bs.vars().schema.clone();
    let mut v = main.clone();
    v.schema = schema;
    if *bs.vars() != v {
        *bs.vars_mut() = v;
    }
}

#[async_trait::async_trait]
impl sprig_router::ShardBackend for GwBackend<'_> {
    async fn run(&mut self, t: &Target, sql: &str, budget: &mut Budget) -> Result<ShardOutcome> {
        let cluster = cluster_id(&t.cluster)?;
        if self.coord.in_txn() {
            let intent = if StmtInfo::classify(sql.as_bytes()).class == StmtClass::Read {
                Intent::Read
            } else {
                Intent::Write
            };
            let branch = self.coord.branch(&cluster, &shard_ref(t), intent).await?;
            if self.explicit_ids {
                branch.conn_mut().explicit_ids_next_statement();
            }
            let r = run_on(branch.conn_mut(), sql, budget).await;
            if let Err(e) = &r
                && let Some(db) = db_error(e)
            {
                self.coord.statement_failed(&cluster, &db).await?;
            }
            return r;
        }
        if cluster == self.main_cluster {
            if self.explicit_ids {
                self.main.explicit_ids_next_statement();
            }
            return run_on(self.main, sql, budget).await;
        }
        if self.legacy_txn {
            return Err(sprig_router::Rejection::new(
                sprig_router::Reason::CrossCluster,
                format!(
                    "shards on cluster {cluster} inside a transaction the gateway doesn't coordinate"
                ),
            )
            .error());
        }
        let explicit_ids = self.explicit_ids;
        let bs = self.other(&cluster).await?;
        if explicit_ids {
            bs.explicit_ids_next_statement();
        }
        run_on(bs, sql, budget).await
    }

    fn in_transaction(&self) -> bool {
        self.coord.in_txn() || self.legacy_txn
    }

    async fn begin_write(&mut self, targets: &[Target], budget: &mut Budget) -> Result<()> {
        let mut clusters: Vec<ClusterId> = Vec::new();
        for t in targets {
            let c = cluster_id(&t.cluster)?;
            if !clusters.contains(&c) {
                clusters.push(c);
            }
        }
        if self.legacy_txn {
            if clusters.iter().any(|c| *c != self.main_cluster) {
                return Err(sprig_router::Rejection::new(
                    sprig_router::Reason::CrossCluster,
                    "a write on another cluster inside a transaction the gateway doesn't coordinate",
                )
                .error());
            }
            return run_on(self.main, "SAVEPOINT __sprig_stmt", budget)
                .await
                .map(|_| ());
        }
        self.coord.check_writes(&clusters)?;
        if self.coord.in_txn() {
            self.mark = Some(self.coord.statement_savepoint(&clusters).await?);
        } else {
            self.coord.begin(BeginOptions::default())?;
            self.implicit = true;
        }
        Ok(())
    }

    async fn end_write(
        &mut self,
        _targets: &[Target],
        ok: bool,
        budget: &mut Budget,
    ) -> Result<()> {
        if self.legacy_txn {
            let sql = if ok {
                "RELEASE SAVEPOINT __sprig_stmt"
            } else {
                "ROLLBACK TO SAVEPOINT __sprig_stmt"
            };
            return run_on(self.main, sql, budget).await.map(|_| ());
        }
        if self.implicit {
            self.implicit = false;
            return if ok {
                self.coord.commit().await.map(|_| ())
            } else {
                self.coord.rollback().await
            };
        }
        if let Some(mark) = self.mark.take()
            && !ok
            && self.coord.in_txn()
        {
            return self.coord.rollback_statement(mark).await;
        }
        Ok(())
    }

    async fn open_stream(&mut self, cluster: &str) -> Result<Option<Box<dyn ShardStream>>> {
        if self.coord.in_txn() || self.legacy_txn {
            return Ok(None);
        }
        let cluster = cluster_id(cluster)?;
        let causal = if cluster == self.main_cluster {
            self.main.causal().clone()
        } else {
            if !self.others.contains_key(&cluster) {
                let _ = self.other(&cluster).await?;
            }
            self.others
                .get(&cluster)
                .map(|b| b.causal().clone())
                .unwrap_or_default()
        };
        let mut bs = session_on(
            &self.factory.backends,
            &self.factory.target,
            &self.factory.opts,
            &self.main_cluster,
            &cluster,
            self.main.vars(),
        );
        bs.set_causal(causal);
        Ok(Some(Box::new(GwStream {
            bs,
            want: 0,
            pending: None,
            warnings: 0,
            done: false,
        })))
    }

    fn max_parallel(&self) -> usize {
        self.parallel
    }

    async fn lookup_claim(&mut self, c: &sprig_router::plan::Claim) -> Result<()> {
        let owner = OwnerRef {
            pk: c.owner_pk.clone(),
            rk: RoutingHash(c.owner_rk),
        };
        let r = sprig_shard::lookup::claim(
            &mut *self.coord,
            &*c.lookup.placement,
            &c.lookup.index,
            &c.value,
            &owner,
        )
        .await?;
        let result = match r {
            ClaimResult::Inserted => "inserted",
            ClaimResult::Own => "own",
            ClaimResult::Reclaimed => "reclaimed",
            ClaimResult::ConflictCounted => "duplicate",
        };
        metrics::counter!("sprig_lookup_claims_total", "result" => result).increment(1);
        Ok(())
    }

    async fn lookup_release(
        &mut self,
        l: &sprig_router::topology::LookupRef,
        value: Vec<KeyValueOwned>,
        owner_pk: Vec<KeyValueOwned>,
    ) -> Result<()> {
        sprig_shard::lookup::release(
            &mut *self.coord,
            &*l.placement,
            &l.index,
            &value,
            &OwnerRef {
                pk: owner_pk,
                rk: RoutingHash(0),
            },
        )
    }

    async fn lookup_owners(&mut self, p: &sprig_router::plan::LookupProbe) -> Result<Vec<u64>> {
        let reader = TxnReader(tokio::sync::Mutex::new(&mut *self.coord));
        let rks =
            sprig_shard::lookup::route(&reader, &*p.lookup.placement, &p.lookup.index, &p.values)
                .await?;
        Ok(rks.into_iter().map(|h| h.0).collect())
    }
}

/// Lookup rows as the session's transaction sees them (its own Pre
/// sessions' uncommitted claims included; chapter 19).
struct TxnReader<'a>(tokio::sync::Mutex<&'a mut DistTxn<GwTxn>>);

#[async_trait::async_trait]
impl LookupReader for TxnReader<'_> {
    async fn read(
        &self,
        t: &sprig_shard::lookup::Target,
        sql: &str,
        params: mysql_async::Params,
    ) -> Result<Vec<mysql_async::Row>> {
        self.0.lock().await.read(t, sql, params).await
    }
}

/// One shard statement on a connection of its own.
pub(super) struct GwStream {
    bs: BackendSession,
    want: usize,
    pending: Option<RespPacket>,
    warnings: u16,
    done: bool,
}

#[async_trait::async_trait]
impl ShardStream for GwStream {
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        let info = StmtInfo::classify(sql.as_bytes());
        self.bs.query(&info, sql.as_bytes()).await?;
        let caps = self.bs.backend_caps().unwrap_or_default();
        let mut columns = Vec::new();
        loop {
            let Some(p) = self.bs.next().await? else {
                self.bs.finish().await?;
                self.done = true;
                return Err(protocol_error("a response without an end"));
            };
            match p.kind {
                PacketKind::ColumnCount(n) => self.want = usize::try_from(n).unwrap_or(0),
                PacketKind::ColumnDef => columns.push(
                    ColumnDef::decode(&p.payload, caps)
                        .map_err(|e| protocol_error(&format!("column definition: {e}")))?,
                ),
                PacketKind::ColumnsEof => {}
                PacketKind::Err(e) => {
                    self.bs.finish().await?;
                    self.done = true;
                    return Err(backend_error(&e));
                }
                PacketKind::Ok(_) => {
                    self.bs.finish().await?;
                    self.done = true;
                    return Ok(None);
                }
                _ => {
                    self.pending = Some(p);
                    return Ok(Some(columns));
                }
            }
        }
    }

    async fn next_row(&mut self) -> Result<Option<sprig_router::exec::Row>> {
        if self.done {
            return Ok(None);
        }
        let p = match self.pending.take() {
            Some(p) => Some(p),
            None => self.bs.next().await?,
        };
        let Some(p) = p else {
            self.bs.finish().await?;
            self.done = true;
            return Ok(None);
        };
        match p.kind {
            PacketKind::Row => {
                let vals = decode_text_row(&p.payload, self.want)
                    .map_err(|e| protocol_error(&format!("text row: {e}")))?;
                Ok(Some(
                    vals.into_iter()
                        .map(|v| v.map(|b| p.payload.slice_ref(b)))
                        .collect::<Vec<Option<Bytes>>>(),
                ))
            }
            PacketKind::End(t) => {
                self.warnings = t.warnings();
                self.bs.finish().await?;
                self.done = true;
                Ok(None)
            }
            PacketKind::Err(e) => {
                self.bs.finish().await?;
                self.done = true;
                Err(backend_error(&e))
            }
            _ => Err(protocol_error("a packet the router doesn't expect")),
        }
    }

    fn warnings(&self) -> u16 {
        self.warnings
    }
}
