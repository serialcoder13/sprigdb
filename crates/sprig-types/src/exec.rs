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
}

/// The savepoint a plan inside a client's transaction uses.
pub const SAVEPOINT: &str = "sprig_types";

/// The isolation of a plan's own transaction ([`Txn`]), for the next
/// transaction only.
pub const PLAN_ISOLATION: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";

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
#[derive(Debug)]
pub struct Txn {
    nested: bool,
}

impl Txn {
    /// Opens the bracket.
    ///
    /// # Errors
    /// The backend's.
    pub async fn begin(r: &mut dyn Runner) -> Result<Self> {
        let nested = r.in_transaction();
        if nested {
            r.exec(&format!("SAVEPOINT {SAVEPOINT}")).await?;
        } else {
            r.exec(PLAN_ISOLATION).await?;
            r.exec("BEGIN").await?;
        }
        Ok(Self { nested })
    }

    /// Closes the bracket: commits on `Ok`, rolls back on `Err` (and
    /// returns the error).
    ///
    /// # Errors
    /// `result`'s error, or the commit's.
    pub async fn end<T>(self, r: &mut dyn Runner, result: Result<T>) -> Result<T> {
        match result {
            Ok(v) => {
                if self.nested {
                    r.exec(&format!("RELEASE SAVEPOINT {SAVEPOINT}")).await?;
                } else {
                    r.exec("COMMIT").await?;
                }
                Ok(v)
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
        }
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
