//! The seams between the coordinator and MariaDB (chapter 18,
//! "Interfaces"; chapter 46, "Deterministic simulation").
//!
//! The coordinator and the resolver reach MariaDB only through these
//! traits:
//!
//! - [`BranchConn`]: a pinned backend connection that carries one branch.
//!   The coordinator sends it typed control statements ([`Ctl`]); the
//!   router sends its own statements on the same connection. The gateway
//!   implements it on its backend connections, [`crate::mysql`] on
//!   `mysql_async`, and the simulation on the MariaDB model.
//! - [`TxnBackend`]: capability profiles, pinned checkouts and the admin.
//! - [`ClusterAdmin`]: out-of-band, autocommit work on a cluster: `XA
//!   RECOVER`, finishing detached branches, `KILL`, and decision rows.
//! - [`AnchorLocator`]: where an anchor's decision rows live now, from the
//!   current shard map (chapter 15).

use std::fmt;
use std::sync::Arc;

use sprig_common::ids::{ClusterId, TenantId};

use crate::decision::{DTX_TABLE, Decision, DecisionRow, encode_participants};
use crate::xid::{AnchorRef, Dtid, Xid, hex};

/// MariaDB error numbers the coordinator acts on.
pub mod errno {
    /// `ER_DUP_ENTRY`: the decision insert lost the race.
    pub const DUP_ENTRY: u16 = 1062;
    /// `ER_LOCK_WAIT_TIMEOUT`.
    pub const LOCK_WAIT_TIMEOUT: u16 = 1205;
    /// `ER_LOCK_DEADLOCK` (also a Galera certification failure).
    pub const DEADLOCK: u16 = 1213;
    /// `ER_XAER_NOTA`: unknown xid, or a prepared branch still attached to
    /// a live connection (spike-xa).
    pub const XAER_NOTA: u16 = 1397;
    /// `ER_XAER_RMFAIL`: the branch is in the wrong state (rollback-only
    /// after a deadlock).
    pub const XAER_RMFAIL: u16 = 1399;
    /// `ER_UNKNOWN_COM_ERROR` from a Galera node outside the primary
    /// component.
    pub const WSREP_NOT_READY: u16 = 1047;
    /// `ER_CONNECTION_KILLED`.
    pub const CONNECTION_KILLED: u16 = 1927;
    /// `CR_SERVER_GONE_ERROR`.
    pub const SERVER_GONE: u16 = 2006;
    /// `CR_SERVER_LOST`.
    pub const SERVER_LOST: u16 = 2013;
}

/// A backend error: MariaDB's number, or [`errno::SERVER_LOST`] when the
/// connection broke (the statement's outcome is then unknown).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    /// MariaDB or client error number.
    pub errno: u16,
    /// MariaDB's message (internal; never sent to clients as is).
    pub message: String,
}

impl DbError {
    /// An error with a number.
    #[must_use]
    pub fn new(errno: u16, message: impl Into<String>) -> Self {
        Self {
            errno,
            message: message.into(),
        }
    }

    /// The connection is gone.
    #[must_use]
    pub fn lost(message: impl Into<String>) -> Self {
        Self::new(errno::SERVER_LOST, message)
    }

    /// Whether the connection is gone (outcome of the statement unknown).
    #[must_use]
    pub fn is_lost(&self) -> bool {
        matches!(
            self.errno,
            errno::SERVER_GONE | errno::SERVER_LOST | errno::CONNECTION_KILLED | 2002 | 2003 | 2055
        )
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.errno, self.message)
    }
}

impl std::error::Error for DbError {}

/// Result of a backend call.
pub type DbResult<T> = std::result::Result<T, DbError>;

/// A control statement the coordinator sends on a branch connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ctl {
    /// `START TRANSACTION [READ ONLY | WITH CONSISTENT SNAPSHOT]`.
    Begin {
        /// `READ ONLY`.
        read_only: bool,
        /// `WITH CONSISTENT SNAPSHOT`.
        snapshot: bool,
    },
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `XA START xid`.
    XaStart(Xid),
    /// `XA END xid`.
    XaEnd(Xid),
    /// `XA PREPARE xid`.
    XaPrepare(Xid),
    /// `XA COMMIT xid ONE PHASE` (after `XA END`).
    XaCommitOnePhase(Xid),
    /// `XA COMMIT xid` of a prepared branch.
    XaCommit(Xid),
    /// `XA ROLLBACK xid` (after `XA END`, or of a prepared branch).
    XaRollback(Xid),
    /// `SAVEPOINT name`.
    Savepoint(String),
    /// `ROLLBACK TO SAVEPOINT name`.
    RollbackTo(String),
    /// `SET SESSION innodb_lock_wait_timeout = n`.
    LockWaitTimeout(u32),
    /// `INSERT INTO schema.__sprig_dtx ...` inside the branch's
    /// transaction: the commit point of a last resource.
    InsertDecision {
        /// The anchor schema.
        schema: String,
        /// The row.
        row: DecisionRow,
    },
}

impl Ctl {
    /// The SQL text.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Ctl::Begin {
                read_only: true, ..
            } => "START TRANSACTION READ ONLY".to_owned(),
            Ctl::Begin { snapshot: true, .. } => {
                "START TRANSACTION WITH CONSISTENT SNAPSHOT".to_owned()
            }
            Ctl::Begin { .. } => "START TRANSACTION".to_owned(),
            Ctl::Commit => "COMMIT".to_owned(),
            Ctl::Rollback => "ROLLBACK".to_owned(),
            Ctl::XaStart(x) => format!("XA START {}", x.sql()),
            Ctl::XaEnd(x) => format!("XA END {}", x.sql()),
            Ctl::XaPrepare(x) => format!("XA PREPARE {}", x.sql()),
            Ctl::XaCommitOnePhase(x) => format!("XA COMMIT {} ONE PHASE", x.sql()),
            Ctl::XaCommit(x) => format!("XA COMMIT {}", x.sql()),
            Ctl::XaRollback(x) => format!("XA ROLLBACK {}", x.sql()),
            Ctl::Savepoint(n) => format!("SAVEPOINT `{}`", n.replace('`', "``")),
            Ctl::RollbackTo(n) => format!("ROLLBACK TO SAVEPOINT `{}`", n.replace('`', "``")),
            Ctl::LockWaitTimeout(n) => format!("SET SESSION innodb_lock_wait_timeout = {n}"),
            Ctl::InsertDecision { schema, row } => insert_decision_sql(schema, row),
        }
    }
}

/// `INSERT INTO schema.__sprig_dtx (...) VALUES (...)`.
#[must_use]
pub fn insert_decision_sql(schema: &str, row: &DecisionRow) -> String {
    let decider = row.decider.map_or_else(
        || "NULL".to_owned(),
        |a| format!("X'{}'", hex(&a.to_bytes())),
    );
    let participants = if row.participants.is_empty() {
        "NULL".to_owned()
    } else {
        format!("X'{}'", hex(&encode_participants(&row.participants)))
    };
    format!(
        "INSERT INTO `{}`.`{DTX_TABLE}` (dtid, outcome, decider, coordinator, participants) \
         VALUES (X'{}', '{}', {decider}, '{}', {participants})",
        schema.replace('`', "``"),
        hex(&row.dtid.to_bytes()),
        row.outcome.as_str(),
        row.coordinator.replace('\\', "\\\\").replace('\'', "''"),
    )
}

/// A pinned backend connection carrying one branch.
#[async_trait::async_trait]
pub trait BranchConn: Send + 'static {
    /// MariaDB's `CONNECTION_ID()` of this connection, for `KILL`.
    fn thread_id(&self) -> u64;

    /// Runs `ctl` in order, in one round trip where the connection can
    /// pack statements, stopping at the first error.
    ///
    /// # Errors
    /// The first statement's error; [`DbError::is_lost`] when the
    /// connection broke.
    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()>;
}

/// What the coordinator needs to know about a cluster (chapter 09's
/// capability profile).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterCaps {
    /// XA works, and prepared branches survive disconnects and restarts
    /// (`derived.xa`).
    pub xa: bool,
}

/// The backend side of a coordinator: capability profiles, pinned
/// checkouts and the admin.
#[async_trait::async_trait]
pub trait TxnBackend: Send + Sync + 'static {
    /// The branch connection type (the router's connection type too).
    type Conn: BranchConn;

    /// The cluster's capabilities; an unknown cluster has none.
    fn caps(&self, cluster: &ClusterId) -> ClusterCaps;

    /// A pinned connection to the cluster's writer, as the session's
    /// backend account (chapter 06 replays session state).
    ///
    /// # Errors
    /// When no connection can be had.
    async fn checkout(&self, cluster: &ClusterId) -> DbResult<Self::Conn>;

    /// Out-of-band operations.
    fn admin(&self) -> Arc<dyn ClusterAdmin>;
}

/// Out-of-band, autocommit operations on a cluster's writer. The
/// coordinator uses them as the session's backend account (insert and
/// select on `__sprig_dtx`, `KILL` of its own threads, finishing its own
/// detached branches); the resolver as `sprig_ctl`.
#[async_trait::async_trait]
pub trait ClusterAdmin: Send + Sync {
    /// `XA RECOVER FORMAT='SQL'`, keeping coordinator xids only.
    async fn xa_recover(&self, cluster: &ClusterId) -> DbResult<Vec<Xid>>;

    /// `XA COMMIT xid` or `XA ROLLBACK xid` of a detached prepared branch.
    async fn xa_finish(&self, cluster: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()>;

    /// `KILL CONNECTION thread`. An unknown thread is success.
    async fn kill(&self, cluster: &ClusterId, thread: u64) -> DbResult<()>;

    /// Autocommit insert of a decision row. [`errno::DUP_ENTRY`] when a
    /// row with the dtid exists; [`errno::LOCK_WAIT_TIMEOUT`] while an
    /// open transaction holds one.
    async fn insert_decision(
        &self,
        cluster: &ClusterId,
        schema: &str,
        row: &DecisionRow,
    ) -> DbResult<()>;

    /// The decision row of `dtid`, if any.
    async fn read_decision(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<Option<DecisionRow>>;

    /// Turns a `pending` row into `outcome` (resolver only).
    async fn settle_pending(
        &self,
        cluster: &ClusterId,
        schema: &str,
        dtid: Dtid,
        outcome: Decision,
    ) -> DbResult<()>;

    /// Every decision row on the cluster, with its schema and age.
    async fn decision_rows(&self, cluster: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>>;

    /// Deletes a decision row (resolver only).
    async fn delete_decision(&self, cluster: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()>;
}

/// Finds where an anchor's decision rows live now.
#[async_trait::async_trait]
pub trait AnchorLocator: Send + Sync {
    /// The cluster and shard schema whose hash range holds the anchor's
    /// hash point, in the tenant's current shard map.
    ///
    /// # Errors
    /// When the map can't be read or has no such group.
    async fn locate(
        &self,
        tenant: TenantId,
        anchor: AnchorRef,
    ) -> sprig_common::Result<(ClusterId, String)>;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{DbId, GroupId, ShardId};

    use super::*;
    use crate::decision::{Participant, ParticipantKind};
    use crate::xid::{DtidGen, Gtrid, bqual};

    #[test]
    fn control_statements_render() {
        let c = ClusterId::new("mum1-c7").unwrap();
        let x = Xid {
            gtrid: Gtrid {
                tenant: TenantId(1),
                dtid: DtidGen::new(1).next(),
                start_ms: 0,
                home: AnchorRef {
                    db: DbId(1),
                    group: GroupId(1),
                    hash_point: 0,
                },
            },
            bqual: bqual(&c, ShardId(1)),
        };
        assert!(Ctl::XaStart(x.clone()).sql().starts_with("XA START X'01"));
        assert!(
            Ctl::XaCommitOnePhase(x)
                .sql()
                .ends_with(",1397772871 ONE PHASE")
        );
        let mut row = DecisionRow::new(DtidGen::new(1).next(), Decision::Commit, "gw-o'k");
        row.participants.push(Participant {
            cluster: c,
            shards: vec![ShardId(1)],
            kind: ParticipantKind::Xa,
        });
        let s = Ctl::InsertDecision {
            schema: "t1_d1_g1_s1".into(),
            row,
        }
        .sql();
        assert!(s.contains("'commit', NULL, 'gw-o''k', X'"), "{s}");
        assert!(DbError::lost("x").is_lost());
        assert!(!DbError::new(1062, "dup").is_lost());
    }
}
