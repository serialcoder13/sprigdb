//! The MariaDB model.
//!
//! Nothing simulates MariaDB, so the testkit models what SprigDB relies on:
//!
//! - tables as ordered maps from a `BIGINT` key to a `VARCHAR` value, with
//!   multi-version rows, `InnoDB` row locks, lock-wait timeouts, deadlock
//!   detection, `REPEATABLE READ` snapshots taken at the first read
//!   (`READ COMMITTED` per statement) and 11.8's `innodb_snapshot_isolation`;
//! - XA: start, end, prepare, commit, rollback, recover; prepared branches
//!   survive disconnects and crashes, holding their locks; with 12.3's
//!   binlog in `InnoDB` ([`ServerConfig::binlog_in_innodb`]) `XA PREPARE` is
//!   not binlogged, so it takes no GTID and replicas never see the branch;
//! - a binlog of event groups with MariaDB GTIDs (`domain-server-seq`),
//!   `@@last_gtid`, `@@gtid_binlog_pos` and `MASTER_GTID_WAIT`;
//! - grants per schema with the current-database cache that makes a
//!   `REVOKE` miss connections already in the schema (cluster fencing
//!   uses grants *and* `KILL` for that reason), `KILL QUERY`,
//!   `KILL CONNECTION` and `read_only`;
//! - replicas that receive events after a network delay, acknowledge them
//!   for semi-sync `AFTER_SYNC` (commits wait for one acknowledgement up to
//!   `rpl_semi_sync_master_timeout`, then fall back to asynchronous) and
//!   apply them after a configurable lag; stopping and starting the SQL
//!   thread; promotion of a replica, and the GTIDs a failover loses;
//! - Galera-style clusters: writesets certified first-committer-wins, a
//!   brute-force abort of conflicting local transactions on other nodes,
//!   and non-primary nodes that refuse queries;
//! - crashes that keep committed and prepared transactions only (with
//!   `sync_binlog = 1` and `innodb_flush_log_at_trx_commit = 1`, as
//!   provisioning configures every node); a transaction waiting for its semi-sync
//!   acknowledgement is in the binlog, so recovery commits it.
//!
//! The core, [`Model`], is synchronous and takes virtual time as an
//! argument, so conformance scripts drive it step by step;
//! [`SimMariaDb`] wraps it for simulations, on the simulated network and
//! tokio's paused clock. [`conformance`] runs the same scripted scenarios
//! against the model and real MariaDB: 11.8 (the Compose servers) and
//! 12.3 (a throwaway container configured like the Compose single node,
//! with the file binlog and with `binlog_storage_engine = innodb`) give the
//! same observations on the single-node and throwaway scenarios. The
//! `InnoDB` binlog's XA difference is invisible there, because GTIDs are
//! compared without sequence numbers; [`ServerConfig::binlog_in_innodb`]
//! carries it, measured by hand on 12.3.3.
//!
//! Not modelled: SQL text (statements are [`Stmt`] values), gap and
//! next-key locks (a locking read or write locks the key itself, present
//! or not), secondary indexes, DDL inside transactions, metadata locks,
//! binlog purging, and replica-side lock conflicts outside Galera.

pub mod conformance;
mod model;
mod sim;

pub use model::{ConnId, Delay, Model, Reach, ServerConfig, Step};
pub use sim::{CLIENT_TIMEOUT, SimConn, SimMariaDb};

use std::fmt;

use serde::{Deserialize, Serialize};

/// MariaDB error numbers the model returns.
pub mod errno {
    /// `ER_CHECKREAD`: the row changed after the snapshot was taken
    /// (`innodb_snapshot_isolation`).
    pub const CHECKREAD: u16 = 1020;
    /// `ER_UNKNOWN_COM_ERROR`: a Galera node outside the primary component.
    pub const WSREP_NOT_READY: u16 = 1047;
    /// `ER_DUP_ENTRY`.
    pub const DUP_ENTRY: u16 = 1062;
    /// `ER_NO_SUCH_TABLE`.
    pub const NO_SUCH_TABLE: u16 = 1146;
    /// `ER_TABLEACCESS_DENIED_ERROR`.
    pub const TABLE_ACCESS_DENIED: u16 = 1142;
    /// `ER_LOCK_WAIT_TIMEOUT`.
    pub const LOCK_WAIT_TIMEOUT: u16 = 1205;
    /// `ER_LOCK_DEADLOCK`.
    pub const DEADLOCK: u16 = 1213;
    /// `ER_OPTION_PREVENTS_STATEMENT`: `read_only`.
    pub const READ_ONLY: u16 = 1290;
    /// `ER_QUERY_INTERRUPTED`: `KILL QUERY`.
    pub const QUERY_INTERRUPTED: u16 = 1317;
    /// `ER_XAER_NOTA`: unknown XID.
    pub const XAER_NOTA: u16 = 1397;
    /// `ER_XAER_RMFAIL`: the command can't run in this XA state.
    pub const XAER_RMFAIL: u16 = 1399;
    /// `ER_XAER_OUTSIDE`: work done outside the global transaction.
    pub const XAER_OUTSIDE: u16 = 1400;
    /// `ER_CONNECTION_KILLED`.
    pub const CONNECTION_KILLED: u16 = 1927;
    /// Client error `CR_SERVER_LOST`: the connection is gone (killed,
    /// crashed server or partition). The conformance runner maps every
    /// client-side connection failure to this number.
    pub const SERVER_LOST: u16 = 2013;
}

/// A GTID: `domain-server-seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Gtid {
    /// `gtid_domain_id`.
    pub domain: u32,
    /// `server_id` of the server that first committed it.
    pub server: u32,
    /// Sequence number within the domain.
    pub seq: u64,
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.domain, self.server, self.seq)
    }
}

impl std::str::FromStr for Gtid {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let mut it = s.trim().split('-');
        let mut next = |what: &str| {
            it.next()
                .ok_or_else(|| format!("GTID {s:?}: no {what}"))?
                .parse::<u64>()
                .map_err(|e| format!("GTID {s:?}: {what}: {e}"))
        };
        let domain = u32::try_from(next("domain")?).map_err(|e| e.to_string())?;
        let server = u32::try_from(next("server")?).map_err(|e| e.to_string())?;
        let seq = next("seq")?;
        Ok(Gtid {
            domain,
            server,
            seq,
        })
    }
}

/// Transaction isolation levels the model implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Isolation {
    /// A new snapshot per statement.
    ReadCommitted,
    /// One snapshot per transaction, from its first consistent read.
    #[default]
    RepeatableRead,
}

/// A statement the model runs. Tables have the shape
/// `(k BIGINT PRIMARY KEY, v VARCHAR(255))` and live in the scenario's
/// schema; `t` names the table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stmt {
    /// `CREATE TABLE t (k BIGINT PRIMARY KEY, v VARCHAR(255))`.
    CreateTable(String),
    /// `BEGIN`.
    Begin,
    /// `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
    BeginSnapshot,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `SET SESSION TRANSACTION ISOLATION LEVEL ...`.
    SetIsolation(Isolation),
    /// `SET SESSION innodb_lock_wait_timeout = n` (seconds).
    SetLockWaitTimeout(u32),
    /// `SELECT k, v FROM t WHERE k = ?`: a consistent read.
    Get {
        /// Table.
        t: String,
        /// Key.
        k: i64,
    },
    /// `SELECT k, v FROM t WHERE k = ? FOR UPDATE`: a locking read.
    GetForUpdate {
        /// Table.
        t: String,
        /// Key.
        k: i64,
    },
    /// `SELECT k, v FROM t ORDER BY k`: a consistent read of every row.
    Scan(String),
    /// `INSERT INTO t (k, v) VALUES (?, ?)`.
    Insert {
        /// Table.
        t: String,
        /// Key.
        k: i64,
        /// Value.
        v: String,
    },
    /// `UPDATE t SET v = ? WHERE k = ?`.
    Update {
        /// Table.
        t: String,
        /// Key.
        k: i64,
        /// Value.
        v: String,
    },
    /// `DELETE FROM t WHERE k = ?`.
    Delete {
        /// Table.
        t: String,
        /// Key.
        k: i64,
    },
    /// `XA START 'x'`.
    XaStart(String),
    /// `XA END 'x'`.
    XaEnd(String),
    /// `XA PREPARE 'x'`.
    XaPrepare(String),
    /// `XA COMMIT 'x'`.
    XaCommit(String),
    /// `XA COMMIT 'x' ONE PHASE`.
    XaCommitOnePhase(String),
    /// `XA ROLLBACK 'x'`.
    XaRollback(String),
    /// `XA RECOVER`: one row per prepared XID (its data).
    XaRecover,
    /// `SELECT @@last_gtid`.
    LastGtid,
    /// `SELECT @@gtid_binlog_pos`.
    GtidBinlogPos,
    /// `SELECT MASTER_GTID_WAIT(?, timeout)`: `0` once the GTID is applied
    /// here, `-1` on timeout.
    MasterGtidWait {
        /// The GTID to wait for.
        gtid: Gtid,
        /// Seconds.
        timeout: u32,
    },
    /// `KILL QUERY <id>` of a connection.
    KillQuery(ConnId),
    /// `KILL CONNECTION <id>`.
    KillConnection(ConnId),
    /// `SET GLOBAL read_only = ?`.
    SetReadOnly(bool),
    /// `CREATE USER u`.
    CreateUser(String),
    /// `GRANT ALL ON <schema>.* TO u`.
    GrantAll(String),
    /// `REVOKE ALL ON <schema>.* FROM u`.
    RevokeAll(String),
    /// `USE <schema>`.
    Use,
}

/// One row: column values as text, `None` for NULL.
pub type Row = Vec<Option<String>>;

/// What a statement returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// Success: rows of a query, rows changed by DML.
    Ok {
        /// Result rows.
        rows: Vec<Row>,
        /// Changed rows.
        affected: u64,
    },
    /// A MariaDB error number (see [`errno`]).
    Err(u16),
}

impl Outcome {
    /// Success with nothing returned.
    #[must_use]
    pub fn ok() -> Self {
        Outcome::Ok {
            rows: Vec::new(),
            affected: 0,
        }
    }

    /// Success with rows.
    #[must_use]
    pub fn rows(rows: Vec<Row>) -> Self {
        Outcome::Ok { rows, affected: 0 }
    }

    /// Success with a count of changed rows.
    #[must_use]
    pub fn affected(n: u64) -> Self {
        Outcome::Ok {
            rows: Vec::new(),
            affected: n,
        }
    }

    /// The error number, if this is an error.
    #[must_use]
    pub fn errno(&self) -> Option<u16> {
        match self {
            Outcome::Err(e) => Some(*e),
            Outcome::Ok { .. } => None,
        }
    }

    /// The first column of the first row.
    #[must_use]
    pub fn scalar(&self) -> Option<&str> {
        match self {
            Outcome::Ok { rows, .. } => rows.first()?.first()?.as_deref(),
            Outcome::Err(_) => None,
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Err(e) => write!(f, "error {e}"),
            Outcome::Ok { rows, affected } if rows.is_empty() => write!(f, "ok ({affected})"),
            Outcome::Ok { rows, .. } => write!(f, "{rows:?}"),
        }
    }
}
