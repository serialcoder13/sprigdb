//! Transaction footprints.
//!
//! A session keeps a [`TxnTracker`]. Each statement inside an explicit
//! transaction adds its fingerprint and the table groups and tables it
//! wrote; at `COMMIT` the tracker yields a [`TxnFootprint`]. A committed
//! transaction that wrote **two or more table groups** becomes a `txn`
//! fingerprint, with text such as `TXN WRITE d1.g1 d2.g1` (groups sorted),
//! which the recorder counts like any other: its latency is the
//! transaction's duration, its shard and cluster counts the transaction's.
//! Placement reads the co-write rates from these.
//!
//! An unsharded logical database has one table group, so the gateway
//! passes the database's default group; the router passes the groups it
//! routed to.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{DbId, GroupId, ObjectId};

use crate::fingerprint::{Fingerprint, FingerprintId, FpFlags, FpKind, FrontEnd, digest};
use crate::recorder::ExecSummary;
use crate::shape::{Shape, StmtType, TableRef};

/// The most distinct statement fingerprints a footprint keeps.
pub const MAX_FINGERPRINTS: usize = 64;
/// The most distinct written tables a footprint keeps.
pub const MAX_TABLES: usize = 64;

/// A table group a statement wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupRef {
    /// The logical database.
    pub db: DbId,
    /// The table group.
    pub group: GroupId,
}

/// What one statement of a transaction wrote.
#[derive(Debug, Clone, Copy, Default)]
pub struct StatementWrites<'a> {
    /// The table groups written (empty for reads).
    pub groups: &'a [GroupRef],
    /// The tables written, when known.
    pub tables: &'a [ObjectId],
    /// Shards the statement ran on.
    pub shards: u32,
    /// A key for each cluster it ran on (any stable hash of the cluster
    /// id).
    pub clusters: &'a [u64],
    /// Rows changed.
    pub rows_affected: u64,
}

/// One finished transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnFootprint {
    /// The table groups written, sorted and distinct.
    pub groups: Vec<GroupRef>,
    /// The distinct statement fingerprints, in first-execution order (at
    /// most [`MAX_FINGERPRINTS`]).
    pub fingerprints: Vec<FingerprintId>,
    /// The distinct tables written, sorted (at most [`MAX_TABLES`]).
    pub tables: Vec<ObjectId>,
    /// Statements executed.
    pub statements: u32,
    /// Time from the first statement to the commit.
    pub duration: Duration,
    /// Shards touched (sum over statements, an upper bound).
    pub shards: u32,
    /// Distinct clusters touched.
    pub clusters: u32,
    /// Rows changed.
    pub rows_affected: u64,
}

impl TxnFootprint {
    /// Whether it is recorded as a `txn` fingerprint: it wrote two or more
    /// table groups.
    #[must_use]
    pub fn is_multi_group(&self) -> bool {
        self.groups.len() >= 2
    }

    /// The normalized text: `TXN WRITE d1.g1 d2.g1`.
    #[must_use]
    pub fn text(&self) -> String {
        let mut s = String::from("TXN WRITE");
        for g in &self.groups {
            let _ = write!(s, " d{}.g{}", g.db.0, g.group.0);
        }
        s
    }

    /// The `txn` fingerprint, with the written tables as the shape's tables
    /// (logical names are not known here; `object_id` is). `None` when it
    /// wrote fewer than two groups.
    #[must_use]
    pub fn fingerprint(&self, front_end: FrontEnd) -> Option<Fingerprint> {
        if !self.is_multi_group() {
            return None;
        }
        let text = self.text();
        let id = digest(FpKind::Txn, DbId(0), None, &text);
        let mut shape = Shape::empty(StmtType::Transaction);
        for t in &self.tables {
            shape.tables.push(TableRef {
                alias: format!("o{}", t.0),
                object_id: Some(*t),
                db: None,
                table: String::new(),
                db_id: None,
                derived: false,
            });
        }
        Some(Fingerprint {
            id,
            kind: FpKind::Txn,
            text: Arc::from(text),
            shape: Arc::new(shape),
            literals: Vec::new(),
            front_end,
            caller: None,
            db_id: DbId(0),
            flags: FpFlags::default(),
            in_list_max: 0,
            sensitive: false,
            list_slots: Vec::new(),
        })
    }

    /// The execution summary to record it with.
    #[must_use]
    pub fn exec_summary(&self, session: u64) -> ExecSummary {
        ExecSummary {
            latency_us: u64::try_from(self.duration.as_micros()).unwrap_or(u64::MAX),
            error: false,
            rows_returned: 0,
            rows_affected: self.rows_affected,
            shards: self.shards,
            clusters: self.clusters,
            scatter: false,
            in_list_max: 0,
            examined: None,
            session,
        }
    }
}

/// One session's open transaction.
#[derive(Debug, Clone, Default)]
pub struct TxnTracker {
    open: bool,
    started: Option<std::time::Instant>,
    groups: Vec<GroupRef>,
    fingerprints: Vec<FingerprintId>,
    tables: Vec<ObjectId>,
    clusters: Vec<u64>,
    statements: u32,
    shards: u32,
    rows_affected: u64,
}

impl TxnTracker {
    /// A tracker with no open transaction.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a transaction is open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// `BEGIN`, `START TRANSACTION`, or the first statement with
    /// `autocommit = 0`. An open transaction is implicitly committed first
    /// by the caller, as MySQL does.
    pub fn begin(&mut self) {
        *self = Self {
            open: true,
            ..Self::default()
        };
    }

    /// One statement of the open transaction, after it ran. Ignored when
    /// none is open (autocommit statements are not footprints).
    pub fn statement(
        &mut self,
        fp: &Fingerprint,
        now: std::time::Instant,
        w: &StatementWrites<'_>,
    ) {
        if !self.open {
            return;
        }
        self.started.get_or_insert(now);
        self.statements = self.statements.saturating_add(1);
        if self.fingerprints.len() < MAX_FINGERPRINTS && !self.fingerprints.contains(&fp.id) {
            self.fingerprints.push(fp.id);
        }
        for g in w.groups {
            if let Err(i) = self.groups.binary_search(g) {
                self.groups.insert(i, *g);
            }
        }
        for t in w.tables {
            if let Err(i) = self.tables.binary_search(t)
                && self.tables.len() < MAX_TABLES
            {
                self.tables.insert(i, *t);
            }
        }
        for c in w.clusters {
            if !self.clusters.contains(c) {
                self.clusters.push(*c);
            }
        }
        self.shards = self.shards.saturating_add(w.shards);
        self.rows_affected = self.rows_affected.saturating_add(w.rows_affected);
    }

    /// `COMMIT`: the footprint, and the tracker is closed. `None` when no
    /// transaction was open or it ran no statement.
    pub fn commit(&mut self, now: std::time::Instant) -> Option<TxnFootprint> {
        let t = std::mem::take(self);
        if !t.open || t.statements == 0 {
            return None;
        }
        Some(TxnFootprint {
            groups: t.groups,
            fingerprints: t.fingerprints,
            tables: t.tables,
            statements: t.statements,
            duration: t
                .started
                .map_or(Duration::ZERO, |s| now.saturating_duration_since(s)),
            shards: t.shards,
            clusters: u32::try_from(t.clusters.len()).unwrap_or(u32::MAX),
            rows_affected: t.rows_affected,
        })
    }

    /// `ROLLBACK` (or a lost connection): nothing is recorded.
    pub fn rollback(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use sprig_common::ids::TenantId;
    use sprig_proto_mysql::packets::Persona;

    use super::*;
    use crate::fingerprint::{SqlCtx, SqlFingerprinter};
    use crate::shape::NoResolver;

    #[test]
    fn multi_group_commit_is_a_txn_fingerprint() {
        let f = SqlFingerprinter::default();
        let ctx = SqlCtx::new(Persona::MySql84).current_db("shop", DbId(1));
        let a = f.sql(
            TenantId(1),
            b"UPDATE stock SET n = n - 1 WHERE id = 5",
            &ctx,
            &NoResolver,
        );
        let b = f.sql(
            TenantId(1),
            b"INSERT INTO billing.charges (amount) VALUES (10)",
            &ctx,
            &NoResolver,
        );
        let g1 = GroupRef {
            db: DbId(2),
            group: GroupId(4),
        };
        let g2 = GroupRef {
            db: DbId(1),
            group: GroupId(1),
        };
        let t0 = Instant::now();
        let mut t = TxnTracker::new();
        t.begin();
        t.statement(
            &a,
            t0,
            &StatementWrites {
                groups: &[g1],
                tables: &[ObjectId(9)],
                shards: 1,
                clusters: &[7],
                rows_affected: 1,
            },
        );
        t.statement(
            &b,
            t0,
            &StatementWrites {
                groups: &[g2],
                tables: &[ObjectId(3)],
                shards: 1,
                clusters: &[8],
                rows_affected: 1,
            },
        );
        t.statement(
            &a,
            t0,
            &StatementWrites {
                groups: &[g1],
                tables: &[ObjectId(9)],
                shards: 1,
                clusters: &[7],
                rows_affected: 1,
            },
        );
        let fp = t.commit(t0 + Duration::from_millis(12)).unwrap();
        assert!(!t.is_open());
        assert_eq!(fp.groups, vec![g2, g1]);
        assert_eq!(fp.text(), "TXN WRITE d1.g1 d2.g4");
        assert_eq!(fp.fingerprints, vec![a.id, b.id]);
        assert_eq!(fp.tables, vec![ObjectId(3), ObjectId(9)]);
        assert_eq!((fp.statements, fp.clusters, fp.rows_affected), (3, 2, 3));
        let tf = fp.fingerprint(FrontEnd::Mysql).unwrap();
        assert_eq!(tf.kind, FpKind::Txn);
        assert_eq!(
            tf.id,
            digest(FpKind::Txn, DbId(0), None, "TXN WRITE d1.g1 d2.g4")
        );
        assert_eq!(tf.shape.object_ids(), vec![ObjectId(3), ObjectId(9)]);
        assert_eq!(fp.exec_summary(5).latency_us, 12_000);
    }

    #[test]
    fn single_group_rollback_and_autocommit_record_nothing() {
        let f = SqlFingerprinter::default();
        let ctx = SqlCtx::new(Persona::MySql84);
        let a = f.sql(TenantId(1), b"UPDATE t SET a = 1", &ctx, &NoResolver);
        let g = GroupRef {
            db: DbId(1),
            group: GroupId(1),
        };
        let now = Instant::now();
        let mut t = TxnTracker::new();
        t.statement(
            &a,
            now,
            &StatementWrites {
                groups: &[g],
                ..StatementWrites::default()
            },
        );
        assert!(t.commit(now).is_none());
        t.begin();
        t.statement(
            &a,
            now,
            &StatementWrites {
                groups: &[g],
                ..StatementWrites::default()
            },
        );
        let one = t.commit(now).unwrap();
        assert!(!one.is_multi_group() && one.fingerprint(FrontEnd::Mysql).is_none());
        t.begin();
        t.statement(
            &a,
            now,
            &StatementWrites {
                groups: &[g],
                ..StatementWrites::default()
            },
        );
        t.rollback();
        assert!(t.commit(now).is_none());
    }
}
