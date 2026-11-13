//! What the type layer needs from the executor that runs it: a [`Runner`]
//! for the statements a plan is made of, and the transaction bracket every
//! multi-statement plan runs in.
//!
//! The gateway's runner sends each statement through its own tenant path
//! (chapter 07's names, chapter 29's checks, chapter 17's router), so the
//! statements here use logical names, and a typed write is planned by the
//! router like any other once the tenant has shards.

use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;

use crate::store::TypesChange;

/// A result set read in full.
#[derive(Debug, Clone, Default)]
pub struct Rows {
    /// The columns, as the client would see them.
    pub columns: Vec<ColumnDef>,
    /// Text-protocol values; `None` is SQL `NULL`.
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

impl Rows {
    /// Whether column `i` holds numbers (its values are literals as they
    /// are).
    #[must_use]
    pub fn numeric(&self, i: usize) -> bool {
        use sprig_proto_mysql::resultset::column_type as ct;
        self.columns.get(i).is_some_and(|c| {
            matches!(
                c.col_type,
                ct::DECIMAL
                    | ct::TINY
                    | ct::SHORT
                    | ct::LONG
                    | ct::FLOAT
                    | ct::DOUBLE
                    | ct::LONGLONG
                    | ct::INT24
                    | ct::NEWDECIMAL
                    | ct::YEAR
            )
        })
    }

    /// The first row's first value as a number.
    #[must_use]
    pub fn scalar_u64(&self) -> Option<u64> {
        let v = self.rows.first()?.first()?.as_ref()?;
        std::str::from_utf8(v).ok()?.parse().ok()
    }
}

/// What a statement without a result set did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Done {
    /// Affected rows, as the client's capabilities count them.
    pub affected_rows: u64,
    /// `LAST_INSERT_ID()`.
    pub last_insert_id: u64,
    /// The info text: `Rows matched: 1  Changed: 1  Warnings: 0`.
    pub info: String,
    /// Warnings.
    pub warnings: u16,
}

/// Runs the statements of a plan.
#[async_trait::async_trait]
pub trait Runner: Send {
    /// Runs a statement that returns rows.
    ///
    /// # Errors
    /// The statement's error, with logical names.
    async fn query(&mut self, sql: &str) -> Result<Rows>;

    /// Runs a statement that returns no rows (DDL included).
    ///
    /// # Errors
    /// The statement's error, with logical names.
    async fn exec(&mut self, sql: &str) -> Result<Done>;

    /// Whether the session is inside a transaction, or has autocommit off:
    /// plans then bracket themselves with a savepoint instead of `BEGIN`.
    fn in_transaction(&self) -> bool;

    /// Whether the client asked for found rows (`CLIENT_FOUND_ROWS`).
    fn found_rows(&self) -> bool;

    /// The object id of table `table` in logical database `db` (the
    /// current one if `None`), at a catalog snapshot that includes this
    /// session's own DDL.
    ///
    /// # Errors
    /// The catalog's.
    async fn object_id(
        &mut self,
        db: Option<&str>,
        table: &str,
    ) -> Result<Option<(DbId, ObjectId)>>;

    /// Commits `change` to the tenant's `types` module in one versioned
    /// catalog change (migrating the module first if needed).
    ///
    /// # Errors
    /// The catalog's.
    async fn commit_types(&mut self, change: &TypesChange) -> Result<()>;

    /// After type DDL changed the tables of the tree rooted at `root`
    /// (chapter 42, typed tables in inheritance groups): when the tree is
    /// inheritable, its new tables and columns follow
    /// (`SPRIG INHERITABLE KEY (key)` again on the root). A no-op
    /// otherwise.
    ///
    /// # Errors
    /// The inheritance layer's.
    async fn tree_changed(&mut self, db: Option<&str>, root: &str, key: &[String]) -> Result<()> {
        let _ = (db, root, key);
        Ok(())
    }

    /// Whether the tree rooted at `root` is inheritable (chapter 42).
    ///
    /// # Errors
    /// The catalog's.
    async fn tree_inheritable(&mut self, db: Option<&str>, root: &str) -> Result<bool> {
        let _ = (db, root);
        Ok(false)
    }

    /// `true`: the following statements run on the physical tables, every
    /// space's layer rows included, instead of through chapter 42's merged
    /// views; `false`: back through them. A drop on an inheritable type
    /// tree changes the rows of every space (decision T18). A no-op where
    /// there is no inheritance layer.
    fn set_physical(&mut self, on: bool) {
        let _ = on;
    }

    /// A structural change of the record hierarchy of `table`, which is
    /// inheritable ([`Runner::tree_inheritable`]): chapter 42 serializes
    /// the plan ([`HierChange::Lock`]) and brings the paths of every space
    /// below the session's up to date (decision T22). A no-op where there
    /// is no inheritance layer.
    ///
    /// # Errors
    /// The inheritance layer's: 39711-39716 when a space below would get a
    /// cycle, lose a parent or grow too deep.
    async fn hierarchy_changed(
        &mut self,
        db: Option<&str>,
        table: &str,
        change: &HierChange,
    ) -> Result<()> {
        let _ = (db, table, change);
        Ok(())
    }

    /// Runs a statement that only maintains hierarchy paths (the fill, a
    /// move's range `UPDATE`): every row's path must follow, so the gateway
    /// runs it without row-level security.
    ///
    /// # Errors
    /// The statement's error, with logical names.
    async fn exec_paths(&mut self, sql: &str) -> Result<Done> {
        self.exec(sql).await
    }

    /// [`Runner::exec_paths`] for a statement that returns rows.
    ///
    /// # Errors
    /// The statement's error, with logical names.
    async fn query_paths(&mut self, sql: &str) -> Result<Rows> {
        self.query(sql).await
    }

    /// Whether the session's `innodb_snapshot_isolation` is on (MariaDB
    /// 11.8's default; the MySQL persona turns it off, decision D9). Asked
    /// only by a plan inside a client's transaction ([`Txn::begin`]).
    ///
    /// # Errors
    /// The backend's.
    async fn snapshot_isolation(&mut self) -> Result<bool> {
        let rows = self.query(SNAPSHOT_ISOLATION_READ).await?;
        Ok(rows
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(Option::as_deref)
            .is_some_and(|v| matches!(v, b"1" | b"ON" | b"on")))
    }
}

/// A structural change of an inheritable table's record hierarchy
/// ([`Runner::hierarchy_changed`]). Keys are SQL literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HierChange {
    /// The plan starts: structural changes of the table run one at a time.
    Lock,
    /// These rows got a new path in the session's space (inserted or
    /// moved rows, the tops of their subtrees).
    Changed(Vec<String>),
    /// These rows were deleted in the session's space.
    Deleted(Vec<String>),
    /// `ADD HIERARCHY` gave every row of the session's space its path.
    Added,
}

impl HierChange {
    /// The words after `ALTER TABLE t SPRIG RECONCILE HIERARCHY` that
    /// carry the change to chapter 42.
    #[must_use]
    pub fn clause(&self) -> String {
        match self {
            Self::Lock => "LOCK".into(),
            Self::Added => "ADDED".into(),
            Self::Changed(k) => format!("CHANGED ({})", k.join(", ")),
            Self::Deleted(k) => format!("DELETED ({})", k.join(", ")),
        }
    }
}

/// The savepoint a plan inside a client's transaction uses.
pub const SAVEPOINT: &str = "sprig_types";

/// The isolation of a plan's own transaction ([`Txn`]), for the next
/// transaction only.
pub const PLAN_ISOLATION: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";

/// Reads the session's `innodb_snapshot_isolation`.
pub const SNAPSHOT_ISOLATION_READ: &str = "SELECT @@session.innodb_snapshot_isolation";

/// Turns the snapshot-isolation check off for a plan inside a client's
/// transaction ([`Txn`], bugfix-54).
pub const SNAPSHOT_ISOLATION_OFF: &str = "SET SESSION innodb_snapshot_isolation = OFF";

/// Turns it back on after the plan.
pub const SNAPSHOT_ISOLATION_ON: &str = "SET SESSION innodb_snapshot_isolation = ON";

/// A plan's transaction: `BEGIN ... COMMIT` in autocommit mode, a
/// savepoint inside the client's transaction.
///
/// In autocommit mode the bracket runs at `READ COMMITTED`
/// ([`PLAN_ISOLATION`]): a plan stands for one client statement, which
/// never fails with 1020 on MariaDB, but its own first read would open a
/// `REPEATABLE READ` snapshot, and MariaDB 11.8's
/// `innodb_snapshot_isolation` then fails the plan's later writes with
/// 1020 when another session (a parent tenant, the copy worker) changed
/// those rows in between (decision T17).
///
/// Inside the client's transaction the isolation can't change, and a
/// retry can't help: MariaDB's 1020 rolls back the whole transaction (the
/// savepoint is gone with it), and a statement retried in the same
/// transaction reads from the same snapshot. So when the session has the
/// check on, the bracket turns it off for the plan's statements
/// ([`SNAPSHOT_ISOLATION_OFF`]) and on again after them: the plan's writes
/// then act on the latest committed rows, as a single MariaDB statement
/// that opened no snapshot does (bugfix-54).
#[derive(Debug)]
pub struct Txn {
    nested: bool,
    /// The check was on and is off for the plan.
    snapshot_off: bool,
}

impl Txn {
    /// Opens the bracket.
    ///
    /// # Errors
    /// The backend's.
    pub async fn begin(r: &mut dyn Runner) -> Result<Self> {
        let nested = r.in_transaction();
        let mut snapshot_off = false;
        if nested {
            if r.snapshot_isolation().await? {
                r.exec(SNAPSHOT_ISOLATION_OFF).await?;
                snapshot_off = true;
            }
            if let Err(e) = r.exec(&format!("SAVEPOINT {SAVEPOINT}")).await {
                if snapshot_off {
                    restore_snapshot(r).await;
                }
                return Err(e);
            }
        } else {
            r.exec(PLAN_ISOLATION).await?;
            r.exec("BEGIN").await?;
        }
        Ok(Self {
            nested,
            snapshot_off,
        })
    }

    /// Closes the bracket: commits on `Ok`, rolls back on `Err` (and
    /// returns the error).
    ///
    /// # Errors
    /// `result`'s error, or the commit's.
    pub async fn end<T>(self, r: &mut dyn Runner, result: Result<T>) -> Result<T> {
        let out = match result {
            Ok(v) => {
                let close = if self.nested {
                    r.exec(&format!("RELEASE SAVEPOINT {SAVEPOINT}")).await
                } else {
                    r.exec("COMMIT").await
                };
                close.map(|_| v)
            }
            Err(e) => {
                let undo = if self.nested {
                    format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}")
                } else {
                    "ROLLBACK".to_owned()
                };
                if let Err(re) = r.exec(&undo).await {
                    tracing::warn!(error = %re, "rolling back a type plan failed");
                }
                Err(e)
            }
        };
        if self.snapshot_off {
            restore_snapshot(r).await;
        }
        out
    }
}

/// Turns the snapshot-isolation check on again after a plan ([`Txn`]).
async fn restore_snapshot(r: &mut dyn Runner) {
    if let Err(e) = r.exec(SNAPSHOT_ISOLATION_ON).await {
        tracing::warn!(error = %e, "innodb_snapshot_isolation not restored after a plan");
    }
}

/// Limits from the `types.*` and `hierarchy.*` settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `types.max_depth`.
    pub type_max_depth: u8,
    /// `types.write_batch_rows`.
    pub write_batch_rows: usize,
    /// `hierarchy.max_depth`: the default `MAX DEPTH`.
    pub hierarchy_max_depth: u16,
    /// `hierarchy.max_move_rows`.
    pub max_move_rows: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            type_max_depth: 8,
            write_batch_rows: 1000,
            hierarchy_max_depth: 64,
            max_move_rows: 100_000,
        }
    }
}

/// 39710 with a message.
pub(crate) fn unsupported(what: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::TypeUnsupported, what.into())
}

#[cfg(test)]
pub(crate) mod fake {
    //! A runner that records statements and answers from a script.

    use std::collections::VecDeque;

    use super::*;

    #[derive(Debug, Default)]
    pub(crate) struct Fake {
        pub(crate) log: Vec<String>,
        pub(crate) answers: VecDeque<Rows>,
        pub(crate) in_txn: bool,
        pub(crate) committed: Vec<TypesChange>,
        pub(crate) ids: Vec<(String, u64)>,
        /// [`super::PLAN_ISOLATION`]s seen (not logged).
        pub(crate) plan_isolation: usize,
        /// The session's `innodb_snapshot_isolation` (asked inside a
        /// client transaction only; not logged).
        pub(crate) snapshot_isolation: bool,
    }

    #[async_trait::async_trait]
    impl Runner for Fake {
        async fn query(&mut self, sql: &str) -> Result<Rows> {
            self.log.push(sql.to_owned());
            Ok(self.answers.pop_front().unwrap_or_default())
        }
        async fn exec(&mut self, sql: &str) -> Result<Done> {
            if sql == super::PLAN_ISOLATION {
                self.plan_isolation += 1;
                return Ok(Done::default());
            }
            self.log.push(sql.to_owned());
            Ok(Done {
                affected_rows: 1,
                ..Done::default()
            })
        }
        fn in_transaction(&self) -> bool {
            self.in_txn
        }
        fn found_rows(&self) -> bool {
            false
        }
        async fn snapshot_isolation(&mut self) -> Result<bool> {
            Ok(self.snapshot_isolation)
        }
        async fn object_id(
            &mut self,
            _db: Option<&str>,
            table: &str,
        ) -> Result<Option<(DbId, ObjectId)>> {
            Ok(self
                .ids
                .iter()
                .find(|(n, _)| n == table)
                .map(|(_, id)| (DbId(1), ObjectId(*id))))
        }
        async fn commit_types(&mut self, change: &TypesChange) -> Result<()> {
            self.committed.push(change.clone());
            Ok(())
        }
    }
}
