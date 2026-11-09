//! The transaction coordinator (chapter 18, "Algorithms"): one per
//! session, in the gateway.
//!
//! A transaction keeps one branch per cluster. The first branch is the
//! **home**; its first shard anchors the decision. Writes go through the
//! write rule, which enforces `txn.non_xa_policy`. Commit is local when one
//! cluster wrote, and XA two-phase commit with a last resource otherwise:
//! every other writer prepares, the last resource commits with the
//! decision row in its own transaction (the commit point), and phase 2
//! commits the prepared branches.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};

use crate::backend::{BranchConn, ClusterAdmin, Ctl, DbError, DbResult, TxnBackend, errno};
use crate::decision::{Decision, DecisionRow, Participant, ParticipantKind};
use crate::race::{Race, RaceAt, decide_by_race};
use crate::settings::{NonXaPolicy, TxnConfig};
use crate::xid::{AnchorRef, Dtid, DtidGen, Gtrid, Xid, bqual};

/// How a branch is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchKind {
    /// `START TRANSACTION`: commits by itself, never prepares.
    Local,
    /// `XA START`: can prepare.
    Xa,
    /// R3, Sprig 2PC: a `START TRANSACTION` on a cluster without XA whose
    /// writes are recorded; it prepares by writing them as redo rows
    /// ([`crate::redo`]) and stays open with its locks.
    Held,
}

/// How a transaction committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnMode {
    /// Nothing written, or no branch at all.
    Local,
    /// One cluster wrote: one local (or one-phase XA) commit.
    SingleWriter,
    /// XA two-phase commit with a last resource.
    Xa2pc,
    /// R3.
    Sprig2pc,
    /// Two or more writers without XA under `best_effort`.
    BestEffort,
}

impl TxnMode {
    /// The metric label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            TxnMode::Local => "local",
            TxnMode::SingleWriter => "single_writer",
            TxnMode::Xa2pc => "xa_2pc",
            TxnMode::Sprig2pc => "sprig_2pc",
            TxnMode::BestEffort => "best_effort",
        }
    }
}

/// Why the router needs a branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    /// A read (never decides the mode).
    Read,
    /// A write or locking read that must be atomic with the rest.
    Write,
}

/// A shard the router is about to touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRef {
    /// Logical database.
    pub db: DbId,
    /// Table group.
    pub group: GroupId,
    /// The shard.
    pub shard: ShardId,
    /// Start of the shard's hash range.
    pub hash_point: u64,
    /// The physical shard schema.
    pub schema: String,
}

impl ShardRef {
    /// The shard as a decision anchor.
    #[must_use]
    pub fn anchor(&self) -> AnchorRef {
        AnchorRef {
            db: self.db,
            group: self.group,
            hash_point: self.hash_point,
        }
    }
}

/// Options of an explicit `BEGIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BeginOptions {
    /// `START TRANSACTION READ ONLY`.
    pub read_only: bool,
    /// `WITH CONSISTENT SNAPSHOT`.
    pub snapshot: bool,
}

/// Where a branch is in XA terms (Local branches use Active and Done).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XaPhase {
    /// Statements may run.
    Active,
    /// `XA PREPARE` succeeded.
    Prepared,
    /// Finished, or its connection is gone.
    Done,
}

/// One branch: every shard the transaction touches on one cluster,
/// through one pinned connection.
#[derive(Debug)]
pub struct Branch<C> {
    cluster: ClusterId,
    kind: BranchKind,
    conn: C,
    xid: Option<Xid>,
    shards: Vec<ShardId>,
    anchor: ShardRef,
    wrote: bool,
    phase: XaPhase,
    thread_id: u64,
    /// The lock-wait timeout was lowered (two or more branches).
    lowered: bool,
    /// Savepoints set logically but not yet on this connection.
    pending_sps: Vec<String>,
}

impl<C: BranchConn> Branch<C> {
    /// The cluster.
    #[must_use]
    pub fn cluster(&self) -> &ClusterId {
        &self.cluster
    }

    /// The kind.
    #[must_use]
    pub fn kind(&self) -> BranchKind {
        self.kind
    }

    /// The connection, for the router's own statements.
    pub fn conn_mut(&mut self) -> &mut C {
        &mut self.conn
    }

    /// Whether a write went through this branch.
    #[must_use]
    pub fn wrote(&self) -> bool {
        self.wrote
    }

    /// The branch's xid (XA branches).
    #[must_use]
    pub fn xid(&self) -> Option<&Xid> {
        self.xid.as_ref()
    }

    /// Shards touched.
    #[must_use]
    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }
}

/// Where the coordinator is, for failpoints (crash tests stop the
/// process at each).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Point {
    /// A branch was opened.
    BranchOpened,
    /// About to prepare.
    BeforePrepare,
    /// Every prepare succeeded.
    Prepared,
    /// The home's `pending` row is in.
    PendingRecorded,
    /// About to commit the last resource.
    BeforeCommitPoint,
    /// The last resource committed: the decision is commit.
    CommitPoint,
    /// One prepared branch committed in phase 2.
    Phase2Branch,
    /// Commit returned.
    Finished,
}

/// Failpoints and observation of the coordinator's steps.
#[async_trait::async_trait]
pub trait TxnHooks: Send + Sync {
    /// Called at `p`; a crash test may never return.
    async fn at(&self, dtid: Option<Dtid>, p: Point);
}

/// What commit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOutcome {
    /// The mode used.
    pub mode: TxnMode,
    /// The dtid, for distributed transactions.
    pub dtid: Option<Dtid>,
    /// Prepared branches left to the resolver (phase 2 outran
    /// `txn.phase2_wait_ms` or failed).
    pub stragglers: usize,
}

/// A statement savepoint (chapter 18, "Savepoints and statement
/// atomicity").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StmtMark {
    branches: usize,
    last_resource: Option<usize>,
    wrote: Vec<bool>,
    set_on: Vec<bool>,
}

#[derive(Debug, Clone)]
struct SpMark {
    name: String,
    branches: usize,
    last_resource: Option<usize>,
    best_effort: bool,
    wrote: Vec<bool>,
}

/// The statement savepoint's name.
pub const STMT_SAVEPOINT: &str = "__sprig_stmt";

/// The coordinator of one session.
pub struct TxnCoordinator<B: TxnBackend> {
    backend: Arc<B>,
    admin: Arc<dyn ClusterAdmin>,
    dtids: Arc<DtidGen>,
    tenant: TenantId,
    cfg: TxnConfig,
    hooks: Option<Arc<dyn TxnHooks>>,
    clock: Arc<dyn Clock>,
    explicit: Option<BeginOptions>,
    dtid: Option<Dtid>,
    start_ms: u64,
    branches: Vec<Branch<B::Conn>>,
    last_resource: Option<usize>,
    best_effort: bool,
    savepoints: Vec<SpMark>,
}

impl<B: TxnBackend> std::fmt::Debug for TxnCoordinator<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCoordinator")
            .field("tenant", &self.tenant)
            .field("dtid", &self.dtid)
            .field("branches", &self.branches.len())
            .finish_non_exhaustive()
    }
}

fn dtx_rolled_back(why: &str) -> SprigError {
    SprigError::new(
        SprigCode::DtxRolledBack,
        format!("distributed transaction rolled back ({why}); retry it"),
    )
}

/// 1213, 1205 and 39443: what a failed transaction returns.
fn rollback_error(e: &DbError) -> SprigError {
    match e.errno {
        errno::DEADLOCK => SprigError::standard(
            errno::DEADLOCK,
            *b"40001",
            "Deadlock found when trying to get lock; try restarting transaction",
        ),
        errno::LOCK_WAIT_TIMEOUT => SprigError::standard(
            errno::LOCK_WAIT_TIMEOUT,
            *b"HY000",
            "Lock wait timeout exceeded; try restarting transaction",
        ),
        _ => dtx_rolled_back(&format!("backend error {}", e.errno)),
    }
}

fn backend_error(e: &DbError) -> SprigError {
    if e.is_lost() {
        SprigError::new(SprigCode::BackendLost, "lost the backend connection")
    } else {
        SprigError::backend(e.errno, *b"HY000", e.message.clone())
    }
}

fn wall_us(clock: &dyn Clock) -> u64 {
    clock
        .wall()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0))
}

fn wall_ms(clock: &dyn Clock) -> u64 {
    clock
        .wall()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0))
}

impl<B: TxnBackend> TxnCoordinator<B> {
    /// A coordinator for one session of `tenant`.
    pub fn new(backend: Arc<B>, dtids: Arc<DtidGen>, tenant: TenantId, cfg: TxnConfig) -> Self {
        let admin = backend.admin();
        Self {
            backend,
            admin,
            dtids,
            tenant,
            cfg,
            hooks: None,
            clock: Arc::new(SystemClock),
            explicit: None,
            dtid: None,
            start_ms: 0,
            branches: Vec::new(),
            last_resource: None,
            best_effort: false,
            savepoints: Vec::new(),
        }
    }

    /// Installs failpoints.
    pub fn set_hooks(&mut self, hooks: Arc<dyn TxnHooks>) {
        self.hooks = Some(hooks);
    }

    /// Replaces the clock (the gtrid's start time).
    pub fn set_clock(&mut self, clock: Arc<dyn Clock>) {
        self.clock = clock;
    }

    /// Replaces the configuration (between transactions).
    pub fn set_config(&mut self, cfg: TxnConfig) {
        self.cfg = cfg;
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &TxnConfig {
        &self.cfg
    }

    async fn hook(&self, p: Point) {
        if let Some(h) = &self.hooks {
            h.at(self.dtid, p).await;
        }
    }

    /// Whether a transaction is open (explicit, or with branches).
    #[must_use]
    pub fn in_txn(&self) -> bool {
        self.explicit.is_some() || !self.branches.is_empty()
    }

    /// The dtid, once an XA branch opened.
    #[must_use]
    pub fn dtid(&self) -> Option<Dtid> {
        self.dtid
    }

    /// The open branches, home first.
    #[must_use]
    pub fn branches(&self) -> &[Branch<B::Conn>] {
        &self.branches
    }

    /// Whether the transaction is best effort.
    #[must_use]
    pub fn is_best_effort(&self) -> bool {
        self.best_effort
    }

    /// `BEGIN` / `START TRANSACTION`. Branches open lazily on first use.
    ///
    /// # Errors
    /// None today; an open transaction is committed first by the caller,
    /// as MySQL does.
    pub fn begin(&mut self, opts: BeginOptions) -> Result<()> {
        self.explicit = Some(opts);
        Ok(())
    }

    fn find(&self, cluster: &ClusterId) -> Option<usize> {
        self.branches.iter().position(|b| &b.cluster == cluster)
    }

    /// Checks the write rule for a set of clusters before anything runs
    /// (multi-shard autocommit statements; chapter 18, "Best effort and
    /// autocommit statements"): under `reject` a set with two clusters
    /// without XA fails with 39440 before any shard executes.
    ///
    /// # Errors
    /// 39440, or 39444 when there are more clusters than
    /// `txn.max_branches`.
    pub fn check_writes(&self, clusters: &[ClusterId]) -> Result<()> {
        let mut all: Vec<&ClusterId> = self.branches.iter().map(|b| &b.cluster).collect();
        for c in clusters {
            if !all.contains(&c) {
                all.push(c);
            }
        }
        if all.len() > self.cfg.max_branches {
            return Err(self.too_many());
        }
        let mut non_xa: Vec<&ClusterId> = self
            .branches
            .iter()
            .filter(|b| b.wrote && b.kind == BranchKind::Local)
            .map(|b| &b.cluster)
            .collect();
        for c in clusters {
            if !self.backend.caps(c).xa && !non_xa.contains(&c) {
                non_xa.push(c);
            }
        }
        if non_xa.len() > 1 && self.cfg.policy == NonXaPolicy::Reject {
            return Err(self.rejected(non_xa[0], non_xa[1]));
        }
        Ok(())
    }

    fn too_many(&self) -> SprigError {
        SprigError::new(
            SprigCode::TooManyBranches,
            format!(
                "the transaction would touch more than txn.max_branches = {} clusters",
                self.cfg.max_branches
            ),
        )
    }

    fn rejected(&self, a: &ClusterId, b: &ClusterId) -> SprigError {
        metrics::counter!("sprig_txn_rejected_writes_total").increment(1);
        SprigError::new(
            SprigCode::CrossClusterWriteRejected,
            format!(
                "the write would make clusters {a} and {b}, neither with XA, write in one \
                 transaction; txn.non_xa_policy is {}",
                self.cfg.policy.as_str()
            ),
        )
    }

    /// The branch on `cluster`, opened on first use (chapter 18, "Opening
    /// a branch"). With [`Intent::Write`] the write rule runs first, so a
    /// rejected write opens nothing and runs nothing.
    ///
    /// # Errors
    /// 39440 (write rule), 39444 (`txn.max_branches`), or the backend's
    /// error when the branch can't be opened.
    #[allow(clippy::too_many_lines, clippy::missing_panics_doc)] // the book's steps, in order
    pub async fn branch(
        &mut self,
        cluster: &ClusterId,
        shard: &ShardRef,
        intent: Intent,
    ) -> Result<&mut Branch<B::Conn>> {
        if let Some(i) = self.find(cluster) {
            if intent == Intent::Write {
                self.note_write_at(i)?;
            }
            let b = &mut self.branches[i];
            if !b.shards.contains(&shard.shard) {
                b.shards.push(shard.shard);
            }
            if !b.pending_sps.is_empty() {
                let ctl: Vec<Ctl> = b.pending_sps.drain(..).map(Ctl::Savepoint).collect();
                if let Err(e) = b.conn.control(&ctl).await {
                    return Err(self.fail_on(i, &e).await);
                }
            }
            return Ok(&mut self.branches[i]);
        }
        let caps = self.backend.caps(cluster);
        if intent == Intent::Write
            && !caps.xa
            && let Some(j) = self.last_resource
            && self.cfg.policy == NonXaPolicy::Reject
        {
            return Err(self.rejected(&self.branches[j].cluster, cluster));
        }
        if self.branches.len() >= self.cfg.max_branches {
            return Err(self.too_many());
        }
        let mut conn = self
            .backend
            .checkout(cluster)
            .await
            .map_err(|e| backend_error(&e))?;
        let home = self.branches.is_empty();
        let mut kind = if caps.xa && !(home && self.cfg.tenant_all_xa) {
            BranchKind::Xa
        } else {
            BranchKind::Local
        };
        // Sprig 2PC (R3): a branch on a cluster without XA records its
        // writes, so it can prepare as a held branch if it isn't the last
        // resource. Its clock is fixed, so NOW() replays the same.
        let mut held_ts = None;
        if kind == BranchKind::Local && !caps.xa && self.cfg.policy == NonXaPolicy::Sprig2pc {
            let ts = wall_us(&*self.clock);
            if conn.start_redo(ts) {
                kind = BranchKind::Held;
                held_ts = Some(ts);
            } else if intent == Intent::Write
                && let Some(j) = self.last_resource
            {
                return Err(self.rejected(&self.branches[j].cluster, cluster));
            }
        }
        if matches!(kind, BranchKind::Xa | BranchKind::Held) && self.dtid.is_none() {
            self.dtid = Some(self.dtids.next());
            self.start_ms = wall_ms(&*self.clock);
        }
        let mut ctl = Vec::new();
        let distributed = !self.branches.is_empty();
        let cap = self.cfg.lock_wait_cap();
        if distributed {
            ctl.push(Ctl::LockWaitTimeout(cap));
        }
        let anchor = shard.clone();
        let xid = if kind == BranchKind::Xa {
            let home_anchor = self
                .branches
                .first()
                .map_or_else(|| shard.anchor(), |b| b.anchor.anchor());
            Some(Xid {
                gtrid: Gtrid {
                    tenant: self.tenant,
                    dtid: self.dtid.expect("dtid set above"),
                    start_ms: self.start_ms,
                    home: home_anchor,
                },
                bqual: bqual(cluster, shard.shard),
            })
        } else {
            None
        };
        if let Some(ts) = held_ts {
            ctl.push(Ctl::SetTimestamp(Some(ts)));
        }
        if let Some(x) = &xid {
            ctl.push(Ctl::XaStart(x.clone()));
        } else {
            let o = self.explicit.unwrap_or_default();
            ctl.push(Ctl::Begin {
                read_only: o.read_only,
                snapshot: o.snapshot,
            });
        }
        conn.control(&ctl).await.map_err(|e| backend_error(&e))?;
        // Becoming distributed: lower the home's lock-wait timeout too.
        if self.branches.len() == 1 && !self.branches[0].lowered {
            let b = &mut self.branches[0];
            let mut ctl: Vec<Ctl> = b.pending_sps.drain(..).map(Ctl::Savepoint).collect();
            ctl.push(Ctl::LockWaitTimeout(cap));
            if let Err(e) = b.conn.control(&ctl).await {
                drop(conn);
                return Err(self.fail_on(0, &e).await);
            }
            b.lowered = true;
        }
        let thread_id = conn.thread_id();
        self.branches.push(Branch {
            cluster: cluster.clone(),
            kind,
            conn,
            xid,
            shards: vec![shard.shard],
            anchor,
            wrote: false,
            phase: XaPhase::Active,
            thread_id,
            lowered: distributed,
            pending_sps: Vec::new(),
        });
        let i = self.branches.len() - 1;
        if intent == Intent::Write {
            self.note_write_at(i)?;
        }
        self.hook(Point::BranchOpened).await;
        Ok(&mut self.branches[i])
    }

    /// The write rule for an open branch (chapter 18, "The write rule").
    ///
    /// # Errors
    /// 39440 under `reject` when a second cluster without XA would write.
    pub fn note_write(&mut self, cluster: &ClusterId) -> Result<()> {
        match self.find(cluster) {
            Some(i) => self.note_write_at(i),
            None => Err(SprigError::internal(format!("no branch on {cluster}"))),
        }
    }

    fn note_write_at(&mut self, i: usize) -> Result<()> {
        if self.branches[i].wrote {
            return Ok(());
        }
        let b = &self.branches[i];
        // Sprig 2PC: the first held writer is the last resource; later
        // ones prepare with redo rows.
        if b.kind == BranchKind::Held && self.last_resource.is_none() {
            self.last_resource = Some(i);
        }
        // A Local branch can't prepare, whatever its cluster supports.
        if b.kind == BranchKind::Local {
            match self.last_resource {
                None => self.last_resource = Some(i),
                Some(j) if j == i => {}
                Some(j) => match self.cfg.policy {
                    NonXaPolicy::BestEffort => self.best_effort = true,
                    NonXaPolicy::Reject | NonXaPolicy::Sprig2pc => {
                        return Err(self.rejected(&self.branches[j].cluster, &b.cluster));
                    }
                },
            }
        }
        self.branches[i].wrote = true;
        Ok(())
    }

    /// The connections of open branches on `clusters`, for statements run
    /// in parallel.
    pub fn conns_mut(&mut self, clusters: &[ClusterId]) -> Vec<(&ClusterId, &mut B::Conn)> {
        self.branches
            .iter_mut()
            .filter(|b| clusters.contains(&b.cluster))
            .map(|b| (&b.cluster, &mut b.conn))
            .collect()
    }

    /// Reports a backend error a statement got on `cluster`'s branch.
    /// Deadlocks, lock-wait timeouts with two or more branches, lost
    /// connections and XA state errors roll the whole transaction back
    /// and return what to send; anything else leaves it open.
    ///
    /// # Errors
    /// 1213, 1205 or 39443 when the transaction was rolled back.
    pub async fn statement_failed(&mut self, cluster: &ClusterId, err: &DbError) -> Result<()> {
        let Some(i) = self.find(cluster) else {
            return Ok(());
        };
        let whole = match err.errno {
            errno::DEADLOCK | errno::XAER_RMFAIL | errno::XAER_NOTA => true,
            errno::LOCK_WAIT_TIMEOUT => self.branches.len() > 1,
            _ => err.is_lost(),
        };
        if !whole {
            return Ok(());
        }
        Err(self.fail_on(i, err).await)
    }

    /// Rolls everything back after `err` on branch `i`; the error to send.
    async fn fail_on(&mut self, i: usize, err: &DbError) -> SprigError {
        let single = self.branches.len() == 1;
        if err.is_lost() || err.errno == errno::DEADLOCK {
            // InnoDB (or the dead connection) already rolled it back.
            self.branches[i].phase = XaPhase::Done;
        }
        let reason = match err.errno {
            errno::DEADLOCK => "deadlock",
            errno::LOCK_WAIT_TIMEOUT => "lock_wait",
            _ => "backend",
        };
        self.abort_all(reason).await;
        if single && err.is_lost() {
            // One branch: the backend's error, as in R0.
            return backend_error(err);
        }
        rollback_error(err)
    }

    // ------------------------------------------------------------------
    // Savepoints.

    /// `SAVEPOINT name`: set lazily, before each branch's next statement.
    ///
    /// # Errors
    /// None today.
    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        self.savepoints.retain(|s| s.name != name);
        self.savepoints.push(SpMark {
            name: name.to_owned(),
            branches: self.branches.len(),
            last_resource: self.last_resource,
            best_effort: self.best_effort,
            wrote: self.branches.iter().map(|b| b.wrote).collect(),
        });
        for b in &mut self.branches {
            b.pending_sps.retain(|s| s != name);
            b.pending_sps.push(name.to_owned());
        }
        Ok(())
    }

    /// `ROLLBACK TO SAVEPOINT name`.
    ///
    /// # Errors
    /// 1305 when there is no such savepoint; the backend's error (and a
    /// rolled-back transaction) when a branch fails.
    pub async fn rollback_to(&mut self, name: &str) -> Result<()> {
        let Some(pos) = self.savepoints.iter().position(|s| s.name == name) else {
            return Err(SprigError::standard(
                1305,
                *b"42000",
                format!("SAVEPOINT {name} does not exist"),
            ));
        };
        let mark = self.savepoints[pos].clone();
        self.savepoints.truncate(pos + 1);
        self.undo_to(
            mark.branches,
            mark.last_resource,
            mark.best_effort,
            &mark.wrote,
            name,
        )
        .await
    }

    /// `RELEASE SAVEPOINT name`: drops it and later ones. Physical
    /// savepoints stay on the connections, where they are harmless.
    ///
    /// # Errors
    /// 1305 when there is no such savepoint.
    pub fn release(&mut self, name: &str) -> Result<()> {
        let Some(pos) = self.savepoints.iter().position(|s| s.name == name) else {
            return Err(SprigError::standard(
                1305,
                *b"42000",
                format!("SAVEPOINT {name} does not exist"),
            ));
        };
        let dropped: Vec<String> = self.savepoints.drain(pos..).map(|s| s.name).collect();
        for b in &mut self.branches {
            b.pending_sps.retain(|s| !dropped.contains(s));
        }
        Ok(())
    }

    /// Before a multi-branch write in an explicit transaction: `SAVEPOINT
    /// __sprig_stmt` on each touched existing branch, so the statement can
    /// be undone alone.
    ///
    /// # Errors
    /// The backend's error (the transaction is then rolled back).
    pub async fn statement_savepoint(&mut self, clusters: &[ClusterId]) -> Result<StmtMark> {
        let mut set_on = vec![false; self.branches.len()];
        let mut failed = None;
        {
            let futs = self
                .branches
                .iter_mut()
                .enumerate()
                .filter(|(_, b)| clusters.contains(&b.cluster) && b.phase == XaPhase::Active)
                .map(|(i, b)| async move {
                    let mut ctl: Vec<Ctl> = b.pending_sps.drain(..).map(Ctl::Savepoint).collect();
                    ctl.push(Ctl::Savepoint(STMT_SAVEPOINT.to_owned()));
                    (i, b.conn.control(&ctl).await)
                });
            for (i, r) in join_all(futs).await {
                match r {
                    Ok(()) => set_on[i] = true,
                    Err(e) => failed = Some((i, e)),
                }
            }
        }
        if let Some((i, e)) = failed {
            return Err(self.fail_on(i, &e).await);
        }
        Ok(StmtMark {
            branches: self.branches.len(),
            last_resource: self.last_resource,
            wrote: self.branches.iter().map(|b| b.wrote).collect(),
            set_on,
        })
    }

    /// Undoes a failed multi-branch statement: `ROLLBACK TO SAVEPOINT
    /// __sprig_stmt` where it was set, branches the statement opened are
    /// rolled back whole and dropped; the transaction continues.
    ///
    /// # Errors
    /// The backend's error (the transaction is then rolled back).
    pub async fn rollback_statement(&mut self, mark: StmtMark) -> Result<()> {
        let mut failed = None;
        {
            let futs = self
                .branches
                .iter_mut()
                .take(mark.branches)
                .enumerate()
                .filter(|(i, b)| {
                    mark.set_on.get(*i).copied().unwrap_or(false) && b.phase == XaPhase::Active
                })
                .map(|(i, b)| async move {
                    (
                        i,
                        b.conn
                            .control(&[Ctl::RollbackTo(STMT_SAVEPOINT.to_owned())])
                            .await,
                    )
                });
            for (i, r) in join_all(futs).await {
                if let Err(e) = r {
                    failed = Some((i, e));
                }
            }
        }
        if let Some((i, e)) = failed {
            return Err(self.fail_on(i, &e).await);
        }
        for (i, w) in mark.wrote.iter().enumerate() {
            if mark.set_on.get(i).copied().unwrap_or(false)
                && let Some(b) = self.branches.get_mut(i)
            {
                b.wrote = *w;
            }
        }
        self.last_resource = mark.last_resource;
        self.drop_after(mark.branches).await;
        Ok(())
    }

    async fn undo_to(
        &mut self,
        keep: usize,
        last_resource: Option<usize>,
        best_effort: bool,
        wrote: &[bool],
        name: &str,
    ) -> Result<()> {
        let mut failed = None;
        {
            let futs = self
                .branches
                .iter_mut()
                .take(keep)
                .enumerate()
                .filter(|(_, b)| b.phase == XaPhase::Active)
                .map(|(i, b)| async move {
                    if let Some(p) = b.pending_sps.iter().position(|s| s == name) {
                        // Never set here: nothing ran since; drop later ones.
                        b.pending_sps.truncate(p + 1);
                        return (i, Ok(()));
                    }
                    b.pending_sps.clear();
                    (i, b.conn.control(&[Ctl::RollbackTo(name.to_owned())]).await)
                });
            for (i, r) in join_all(futs).await {
                if let Err(e) = r {
                    failed = Some((i, e));
                }
            }
        }
        if let Some((i, e)) = failed {
            return Err(self.fail_on(i, &e).await);
        }
        for (i, w) in wrote.iter().enumerate() {
            if let Some(b) = self.branches.get_mut(i) {
                b.wrote = *w;
            }
        }
        self.last_resource = last_resource;
        self.best_effort = best_effort;
        self.drop_after(keep).await;
        Ok(())
    }

    /// Rolls back and drops branches from index `keep` on. Dropping the
    /// home resets the dtid.
    async fn drop_after(&mut self, keep: usize) {
        if self.branches.len() <= keep {
            return;
        }
        let restore = self.cfg.session_lock_wait_timeout_s;
        let mut gone: Vec<Branch<B::Conn>> = self.branches.drain(keep..).collect();
        join_all(gone.iter_mut().map(|b| end_branch(b, false, restore))).await;
        if let Some(l) = self.last_resource
            && l >= keep
        {
            self.last_resource = None;
        }
        if self.branches.is_empty() {
            self.dtid = None;
            self.last_resource = None;
            self.best_effort = false;
        }
    }

    // ------------------------------------------------------------------
    // Rollback.

    /// `ROLLBACK`: every branch rolls back.
    ///
    /// # Errors
    /// None: failures leave prepared branches to the resolver.
    pub async fn rollback(&mut self) -> Result<()> {
        self.abort_all("client").await;
        Ok(())
    }

    /// Rolls back every branch and ends the transaction. Prepared branches
    /// whose connection fails are finished through the admin, or left to
    /// the resolver; only called when the last resource can't commit.
    async fn abort_all(&mut self, reason: &'static str) {
        let admin = self.admin.clone();
        let restore = self.cfg.session_lock_wait_timeout_s;
        let dtid = self.dtid;
        let gtrid = self.gtrid(dtid.unwrap_or(sprig_common::ids::TxnId(0)));
        join_all(self.branches.iter_mut().map(|b| {
            let admin = admin.clone();
            async move {
                if b.phase == XaPhase::Prepared {
                    finish_prepared(&*admin, b, false, dtid, &gtrid).await;
                } else {
                    end_branch(b, false, restore).await;
                }
            }
        }))
        .await;
        if !self.branches.is_empty() || self.explicit.is_some() {
            metrics::counter!("sprig_txn_rollbacks_total", "reason" => reason).increment(1);
        }
        self.reset();
    }

    fn reset(&mut self) {
        self.branches.clear();
        self.explicit = None;
        self.dtid = None;
        self.last_resource = None;
        self.best_effort = false;
        self.savepoints.clear();
    }

    // ------------------------------------------------------------------
    // Commit.

    /// `COMMIT` (chapter 18, "Commit").
    ///
    /// # Errors
    /// 39443 (rolled back), 1213 or 1205 (the cause of a rollback), 39441
    /// (outcome unknown: the gateway closes the client connection), 39442
    /// (best effort committed only some clusters), or the last resource's
    /// error.
    pub async fn commit(&mut self) -> Result<CommitOutcome> {
        let started = tokio::time::Instant::now();
        let r = self.commit_inner().await;
        self.reset();
        if let Ok(o) = &r {
            metrics::counter!("sprig_txn_commits_total", "mode" => o.mode.label()).increment(1);
            metrics::histogram!("sprig_txn_commit_duration_seconds", "mode" => o.mode.label())
                .record(started.elapsed().as_secs_f64());
        }
        self.hook(Point::Finished).await;
        r
    }

    #[allow(clippy::too_many_lines)] // the book's commit, step by step
    async fn commit_inner(&mut self) -> Result<CommitOutcome> {
        #[allow(clippy::cast_precision_loss)]
        metrics::histogram!("sprig_txn_branch_count").record(self.branches.len() as f64);
        let writers: Vec<usize> = (0..self.branches.len())
            .filter(|&i| self.branches[i].wrote)
            .collect();
        let restore = self.cfg.session_lock_wait_timeout_s;
        if writers.is_empty() {
            join_all(
                self.branches
                    .iter_mut()
                    .map(|b| end_branch(b, true, restore)),
            )
            .await;
            return Ok(CommitOutcome {
                mode: TxnMode::Local,
                dtid: None,
                stragglers: 0,
            });
        }
        if writers.len() == 1 {
            let w = writers[0];
            self.end_readers(|i| i != w).await;
            let b = &mut self.branches[w];
            return match commit_one(b, restore).await {
                Ok(()) => Ok(CommitOutcome {
                    mode: TxnMode::SingleWriter,
                    dtid: None,
                    stragglers: 0,
                }),
                Err(e) if e.is_lost() => {
                    metrics::counter!("sprig_txn_unknown_outcomes_total").increment(1);
                    Err(SprigError::new(
                        SprigCode::CommitOutcomeUnknown,
                        "lost the backend connection during COMMIT; the outcome is unknown",
                    ))
                }
                Err(e) => Err(rollback_error_or_backend(&e)),
            };
        }
        let l = self.last_resource.unwrap_or(0);
        // Best effort: writers that can't prepare, besides the last
        // resource, commit one at a time after the commit point.
        let extra: Vec<usize> = if self.best_effort {
            writers
                .iter()
                .copied()
                .filter(|&i| i != l && self.branches[i].kind != BranchKind::Xa)
                .collect()
        } else {
            Vec::new()
        };
        let prepared: Vec<usize> = writers
            .iter()
            .copied()
            .filter(|&i| i != l && !extra.contains(&i))
            .collect();
        if self.best_effort && prepared.is_empty() {
            // Nothing to prepare, so no decision to record.
            return self.commit_best_effort(&writers).await;
        }
        self.end_readers(|i| i != l).await;
        self.hook(Point::BeforePrepare).await;
        let dtid = self.dtid.expect("XA and held branches have a dtid");
        let gtrid = self.gtrid(dtid);
        // Phase 1, in parallel: XA END + XA PREPARE in one round trip; a
        // held branch writes its redo rows (autocommit, another
        // connection) and stays open with its locks.
        let deadline = self.cfg.prepare_timeout;
        let mut failed: Option<DbError> = None;
        {
            let admin = self.admin.clone();
            let futs = self
                .branches
                .iter_mut()
                .enumerate()
                .filter(|(i, _)| prepared.contains(i))
                .map(|(_, b)| {
                    let admin = admin.clone();
                    let gtrid = gtrid.clone();
                    async move {
                        let r = match tokio::time::timeout(deadline, prepare_one(&*admin, b, dtid, &gtrid))
                            .await
                        {
                            Ok(r) => r,
                            Err(_) => Err(DbError::lost("prepare timed out")),
                        };
                        match &r {
                            Ok(()) => b.phase = XaPhase::Prepared,
                            // Unknown whether it prepared: treat it as
                            // prepared, so the rollback finds it either way.
                            Err(e) if e.is_lost() => b.phase = XaPhase::Prepared,
                            Err(_) => {}
                        }
                        r
                    }
                });
            for r in join_all(futs).await {
                if let Err(e) = r {
                    failed.get_or_insert(e);
                }
            }
        }
        if let Some(e) = failed {
            self.abort_all("prepare_failed").await;
            if e.errno == UNREPLAYABLE {
                return Err(SprigError::new(
                    SprigCode::HeldBranchStatement,
                    format!(
                        "a held branch (Sprig 2PC) ran a statement that doesn't replay to the same \
                         result ({}); the transaction was rolled back",
                        e.message
                    ),
                ));
            }
            return Err(rollback_error(&e));
        }
        self.hook(Point::Prepared).await;
        let coordinator = self.cfg.coordinator_id.clone();
        let participants: Vec<Participant> = writers
            .iter()
            .map(|&i| Participant {
                cluster: self.branches[i].cluster.clone(),
                shards: self.branches[i].shards.clone(),
                kind: if i == l {
                    ParticipantKind::Last
                } else if self.branches[i].kind == BranchKind::Held {
                    ParticipantKind::Held
                } else {
                    ParticipantKind::Xa
                },
            })
            .collect();
        if l != 0 {
            // The home points at the last resource's anchor. It lists the
            // participants too, so the resolver knows when it can go.
            let home = &self.branches[0];
            let mut row = DecisionRow::new(dtid, Decision::Pending, &coordinator);
            row.decider = Some(self.branches[l].anchor.anchor());
            row.participants.clone_from(&participants);
            let r = self
                .admin
                .insert_decision(&home.cluster, &home.anchor.schema, &row)
                .await;
            if let Err(e) = r {
                // Duplicate: the resolver recorded abort first. Anything
                // else: unknown, but the last resource will never commit,
                // so abort is the outcome either way.
                self.abort_all("prepare_failed").await;
                return Err(if e.errno == errno::DUP_ENTRY {
                    dtx_rolled_back("the resolver aborted it")
                } else {
                    dtx_rolled_back("could not record the decision pointer")
                });
            }
            self.hook(Point::PendingRecorded).await;
        }
        // The commit point: the decision row in the last resource's own
        // transaction.
        let mut row = DecisionRow::new(dtid, Decision::Commit, &coordinator);
        row.participants = participants;
        self.hook(Point::BeforeCommitPoint).await;
        let lb = &mut self.branches[l];
        let mut ctl = vec![Ctl::InsertDecision {
            schema: lb.anchor.schema.clone(),
            row,
        }];
        match &lb.xid {
            Some(x) => {
                ctl.push(Ctl::XaEnd(x.clone()));
                ctl.push(Ctl::XaCommitOnePhase(x.clone()));
            }
            None => ctl.push(Ctl::Commit),
        }
        if lb.lowered {
            ctl.push(Ctl::LockWaitTimeout(self.cfg.session_lock_wait_timeout_s));
        }
        if lb.kind == BranchKind::Held {
            // The last resource commits itself: its record isn't needed.
            let _ = lb.conn.take_redo();
            ctl.push(Ctl::SetTimestamp(None));
        }
        let res = lb.conn.control(&ctl).await;
        let decided = match res {
            Ok(()) => {
                lb.phase = XaPhase::Done;
                true
            }
            Err(e) if e.errno == errno::DUP_ENTRY => {
                self.abort_all("resolver").await;
                return Err(dtx_rolled_back("the resolver aborted it"));
            }
            Err(e) if !e.is_lost() => {
                self.abort_all("prepare_failed").await;
                return Err(rollback_error_or_backend(&e));
            }
            Err(_) => {
                lb.phase = XaPhase::Done;
                match self.resolve_unknown(l, dtid).await {
                    Some(Decision::Commit) => true,
                    Some(_) => {
                        self.abort_all("resolver").await;
                        return Err(dtx_rolled_back("the last resource did not commit"));
                    }
                    None => {
                        metrics::counter!("sprig_txn_unknown_outcomes_total").increment(1);
                        // Prepared branches stay for the resolver.
                        return Err(SprigError::new(
                            SprigCode::CommitOutcomeUnknown,
                            "the outcome of the distributed commit is unknown",
                        ));
                    }
                }
            }
        };
        debug_assert!(decided);
        self.hook(Point::CommitPoint).await;
        // Best effort: the other writers without XA, one at a time. The
        // decision is already commit, so a failure here leaves the prepared
        // branches to commit and reports 39442.
        let mut partial: Option<(ClusterId, DbError)> = None;
        let mut extra_done: Vec<usize> = Vec::new();
        for &i in &extra {
            let restore = self.cfg.session_lock_wait_timeout_s;
            let b = &mut self.branches[i];
            if partial.is_some() {
                end_branch(b, false, restore).await;
                continue;
            }
            match commit_one(b, restore).await {
                Ok(()) => extra_done.push(i),
                Err(e) => partial = Some((b.cluster.clone(), e)),
            }
        }
        // Phase 2.
        let stragglers = self.phase2(&prepared).await;
        if let Some((c, e)) = partial {
            metrics::counter!("sprig_txn_partial_commits_total").increment(1);
            let committed: Vec<String> = writers
                .iter()
                .filter(|&&i| !extra.contains(&i) || extra_done.contains(&i))
                .map(|&i| self.branches[i].cluster.to_string())
                .collect();
            return Err(SprigError::new(
                SprigCode::PartialCommit,
                format!(
                    "best effort committed only clusters {} (error {} on {c})",
                    committed.join(", "),
                    e.errno
                ),
            ));
        }
        let held = prepared
            .iter()
            .any(|&i| self.branches[i].kind == BranchKind::Held);
        Ok(CommitOutcome {
            mode: if self.best_effort {
                TxnMode::BestEffort
            } else if held {
                TxnMode::Sprig2pc
            } else {
                TxnMode::Xa2pc
            },
            dtid: Some(dtid),
            stragglers,
        })
    }

    /// The gtrid of this transaction (an XA branch's, or what a held
    /// branch's redo rows record).
    fn gtrid(&self, dtid: Dtid) -> Gtrid {
        self.branches
            .iter()
            .find_map(|b| b.xid.as_ref().map(|x| x.gtrid.clone()))
            .unwrap_or_else(|| Gtrid {
                tenant: self.tenant,
                dtid,
                start_ms: self.start_ms,
                home: self.branches.first().map_or(
                    AnchorRef {
                        db: DbId(0),
                        group: GroupId(0),
                        hash_point: 0,
                    },
                    |b| b.anchor.anchor(),
                ),
            })
    }

    /// Ends read-only branches early (`pred` selects which).
    async fn end_readers(&mut self, pred: impl Fn(usize) -> bool) {
        let restore = self.cfg.session_lock_wait_timeout_s;
        join_all(
            self.branches
                .iter_mut()
                .enumerate()
                .filter(|(i, b)| !b.wrote && pred(*i))
                .map(|(_, b)| end_branch(b, true, restore)),
        )
        .await;
    }

    /// The last resource's connection broke during its commit: kill its
    /// old thread, then decide by insert race at its anchor.
    async fn resolve_unknown(&mut self, l: usize, dtid: Dtid) -> Option<Decision> {
        let admin = self.admin.clone();
        let (cluster, schema, thread) = {
            let b = &self.branches[l];
            (b.cluster.clone(), b.anchor.schema.clone(), b.thread_id)
        };
        let me = self.cfg.coordinator_id.clone();
        let tenant = self.tenant;
        let deadline = tokio::time::Instant::now() + self.cfg.commit_resolve_timeout;
        let mut killed = false;
        loop {
            if !killed {
                killed = admin.kill(&cluster, thread).await.is_ok();
            }
            if killed {
                let r = decide_by_race(
                    &*admin,
                    None,
                    tenant,
                    RaceAt {
                        cluster: &cluster,
                        schema: &schema,
                    },
                    dtid,
                    &me,
                    false,
                )
                .await;
                if let Race::Decided(d) = r {
                    return Some(d);
                }
            }
            if tokio::time::Instant::now() + Duration::from_millis(200) > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Phase 2: `XA COMMIT` every prepared branch, waiting at most
    /// `txn.phase2_wait_ms`. Returns the stragglers left to the resolver.
    async fn phase2(&mut self, prepared: &[usize]) -> usize {
        let admin = self.admin.clone();
        let hooks = self.hooks.clone();
        let dtid = self.dtid;
        let gtrid = self.gtrid(dtid.unwrap_or(sprig_common::ids::TxnId(0)));
        let wait = self.cfg.phase2_wait;
        let futs = self
            .branches
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| prepared.contains(i))
            .map(|(_, b)| {
                let admin = admin.clone();
                let hooks = hooks.clone();
                let gtrid = gtrid.clone();
                async move {
                    let ok = finish_prepared(&*admin, b, true, dtid, &gtrid).await;
                    if ok && let Some(h) = &hooks {
                        h.at(dtid, Point::Phase2Branch).await;
                    }
                    ok
                }
            });
        match tokio::time::timeout(wait, join_all(futs)).await {
            Ok(done) => done.iter().filter(|ok| !**ok).count(),
            Err(_) => prepared.len(),
        }
    }

    /// Best effort with no XA writer (chapter 18, "Best effort and
    /// autocommit statements"): commit the writers one at a time in open
    /// order. With XA writers, `commit_inner` makes the first writer
    /// without XA the last resource, so the prepared branches have a
    /// recorded decision.
    async fn commit_best_effort(&mut self, writers: &[usize]) -> Result<CommitOutcome> {
        self.end_readers(|_| true).await;
        let (xa, local): (Vec<usize>, Vec<usize>) = writers
            .iter()
            .copied()
            .partition(|&i| self.branches[i].kind == BranchKind::Xa);
        let mut failed = None;
        for &i in &xa {
            let b = &mut self.branches[i];
            let x = b.xid.clone().expect("XA");
            match b
                .conn
                .control(&[Ctl::XaEnd(x.clone()), Ctl::XaPrepare(x)])
                .await
            {
                Ok(()) => b.phase = XaPhase::Prepared,
                Err(e) => {
                    if e.is_lost() {
                        b.phase = XaPhase::Prepared;
                    }
                    failed = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = failed {
            self.abort_all("prepare_failed").await;
            return Err(rollback_error(&e));
        }
        let mut committed: Vec<ClusterId> = Vec::new();
        for &i in &local {
            let b = &mut self.branches[i];
            match commit_one(b, self.cfg.session_lock_wait_timeout_s).await {
                Ok(()) => committed.push(b.cluster.clone()),
                Err(e) => {
                    b.phase = XaPhase::Done;
                    let failed_on = b.cluster.clone();
                    self.abort_all("prepare_failed").await;
                    if committed.is_empty() {
                        if e.is_lost() {
                            // It may have committed: unknown.
                            metrics::counter!("sprig_txn_unknown_outcomes_total").increment(1);
                            return Err(SprigError::new(
                                SprigCode::CommitOutcomeUnknown,
                                "lost the backend connection during a best-effort commit",
                            ));
                        }
                        return Err(rollback_error_or_backend(&e));
                    }
                    metrics::counter!("sprig_txn_partial_commits_total").increment(1);
                    let names: Vec<String> = committed.iter().map(ToString::to_string).collect();
                    return Err(SprigError::new(
                        SprigCode::PartialCommit,
                        format!(
                            "best effort committed only clusters {} (error {} on {})",
                            names.join(", "),
                            e.errno,
                            failed_on
                        ),
                    ));
                }
            }
        }
        let stragglers = self.phase2(&xa).await;
        Ok(CommitOutcome {
            mode: TxnMode::BestEffort,
            dtid: self.dtid,
            stragglers,
        })
    }
}

fn rollback_error_or_backend(e: &DbError) -> SprigError {
    match e.errno {
        errno::DEADLOCK | errno::LOCK_WAIT_TIMEOUT => rollback_error(e),
        _ => backend_error(e),
    }
}

/// Commits a single writer: `COMMIT`, or `XA END` + `XA COMMIT ONE PHASE`.
async fn commit_one<C: BranchConn>(b: &mut Branch<C>, restore: u32) -> DbResult<()> {
    let mut ctl = match &b.xid {
        Some(x) => vec![Ctl::XaEnd(x.clone()), Ctl::XaCommitOnePhase(x.clone())],
        None => vec![Ctl::Commit],
    };
    if b.lowered {
        ctl.push(Ctl::LockWaitTimeout(restore));
    }
    if b.kind == BranchKind::Held {
        let _ = b.conn.take_redo();
        ctl.push(Ctl::SetTimestamp(None));
    }
    let r = b.conn.control(&ctl).await;
    b.phase = XaPhase::Done;
    r
}

/// Ends an active branch: commit (readers) or roll back. Errors are
/// ignored: a broken connection rolls back by itself.
async fn end_branch<C: BranchConn>(b: &mut Branch<C>, commit: bool, restore: u32) {
    if b.phase != XaPhase::Active {
        return;
    }
    let r = match (&b.xid, commit) {
        (Some(x), true) => {
            b.conn
                .control(&[Ctl::XaEnd(x.clone()), Ctl::XaCommitOnePhase(x.clone())])
                .await
        }
        (Some(x), false) => {
            // After a deadlock XA END fails with 1399; XA ROLLBACK works.
            let _ = b.conn.control(&[Ctl::XaEnd(x.clone())]).await;
            b.conn.control(&[Ctl::XaRollback(x.clone())]).await
        }
        (None, true) => b.conn.control(&[Ctl::Commit]).await,
        (None, false) => b.conn.control(&[Ctl::Rollback]).await,
    };
    if r.is_ok() && b.lowered {
        let _ = b.conn.control(&[Ctl::LockWaitTimeout(restore)]).await;
    }
    if b.kind == BranchKind::Held {
        let _ = b.conn.take_redo();
        if r.is_ok() {
            let _ = b.conn.control(&[Ctl::SetTimestamp(None)]).await;
        }
    }
    b.phase = XaPhase::Done;
}

/// Errno of a prepare refused because a held branch ran a statement that
/// doesn't replay (39446).
const UNREPLAYABLE: u16 = 39446;

/// Phase 1 for one branch: `XA END` + `XA PREPARE`, or a held branch's
/// redo rows.
async fn prepare_one<C: BranchConn>(
    admin: &dyn ClusterAdmin,
    b: &mut Branch<C>,
    dtid: Dtid,
    gtrid: &Gtrid,
) -> DbResult<()> {
    if b.kind == BranchKind::Held {
        let stmts = b
            .conn
            .take_redo()
            .map_err(|why| DbError::new(UNREPLAYABLE, why))?;
        if stmts.is_empty() {
            // Locking reads only: nothing to replay.
            return Ok(());
        }
        return admin
            .insert_redo(&b.cluster, &b.anchor.schema, dtid, gtrid, &stmts)
            .await;
    }
    let x = b.xid.clone().expect("prepared branches are XA or held");
    b.conn.control(&[Ctl::XaEnd(x.clone()), Ctl::XaPrepare(x)]).await
}

/// Finishes a prepared held branch: on commit, its `done` marker goes in
/// inside it and it commits (one fact: data in, branch finished), then the
/// rows are deleted; on abort it rolls back and the rows are deleted from
/// the admin. Returns whether it
/// is known finished; a commit that fails rolls the branch back and
/// leaves the rows to the resolver, which replays them.
async fn finish_held<C: BranchConn>(
    admin: &dyn ClusterAdmin,
    b: &mut Branch<C>,
    commit: bool,
    dtid: Option<Dtid>,
    gtrid: &Gtrid,
) -> bool {
    b.phase = XaPhase::Done;
    let Some(dtid) = dtid else {
        let _ = b.conn.control(&[Ctl::Rollback]).await;
        return true;
    };
    if commit {
        let ctl = [
            Ctl::RedoDone {
                schema: b.anchor.schema.clone(),
                dtid,
                gtrid: gtrid.clone(),
            },
            Ctl::Commit,
            Ctl::SetTimestamp(None),
        ];
        match b.conn.control(&ctl).await {
            Ok(()) => {
                // Finished: the rows can go (the resolver deletes them
                // otherwise).
                let _ = admin.delete_redo(&b.cluster, &b.anchor.schema, dtid).await;
                true
            }
            Err(e) => {
                if !e.is_lost() {
                    let _ = b.conn.control(&[Ctl::Rollback, Ctl::SetTimestamp(None)]).await;
                }
                false
            }
        }
    } else {
        let _ = b.conn.control(&[Ctl::Rollback, Ctl::SetTimestamp(None)]).await;
        admin
            .delete_redo(&b.cluster, &b.anchor.schema, dtid)
            .await
            .is_ok()
    }
}

/// Finishes a prepared branch with `XA COMMIT` (or `XA ROLLBACK`) on its
/// own connection; on 1397 or a broken connection, through the admin
/// after killing the old thread (spike-xa: `KILL` detaches the branch).
/// Returns whether the branch is known finished.
async fn finish_prepared<C: BranchConn>(
    admin: &dyn ClusterAdmin,
    b: &mut Branch<C>,
    commit: bool,
    dtid: Option<Dtid>,
    gtrid: &Gtrid,
) -> bool {
    if b.kind == BranchKind::Held {
        return finish_held(admin, b, commit, dtid, gtrid).await;
    }
    let Some(x) = b.xid.clone() else {
        return true;
    };
    let ctl = if commit {
        Ctl::XaCommit(x.clone())
    } else {
        Ctl::XaRollback(x.clone())
    };
    let r = b.conn.control(std::slice::from_ref(&ctl)).await;
    b.phase = XaPhase::Done;
    let e = match r {
        Ok(()) => return true,
        Err(e) => e,
    };
    if !e.is_lost() && e.errno != errno::XAER_NOTA {
        return false;
    }
    // Detach the branch from our old thread, then finish it from the admin.
    let _ = admin.kill(&b.cluster, b.thread_id).await;
    for attempt in 0..5u32 {
        match admin.xa_finish(&b.cluster, &x, commit).await {
            Ok(()) => return true,
            Err(e) if e.errno == errno::XAER_NOTA => match admin.xa_recover(&b.cluster).await {
                // Not listed: it already finished.
                Ok(list) if !list.contains(&x) => return true,
                Ok(_) => {}
                Err(_) => return false,
            },
            Err(_) => return false,
        }
        tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt + 1))).await;
    }
    false
}
