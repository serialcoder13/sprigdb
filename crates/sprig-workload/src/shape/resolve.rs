//! The table resolver: how the shape extractor maps logical names to
//! catalog objects and physical tables.
//!
//! Shapes work on logical names, as the client wrote them. Only the
//! gateway knows the tenant's catalog and schema mapping (chapter 07), so
//! it hands the extractor a [`TableResolver`].

use sprig_common::ids::{DbId, ObjectId};

/// A table the resolver knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTable {
    /// The catalog object (chapter 08).
    pub object_id: ObjectId,
    /// Its logical database.
    pub db_id: DbId,
    /// Where it lives in MariaDB, when the caller knows: in R0 the
    /// database's only shard schema.
    pub physical: Option<PhysicalTable>,
}

/// A physical table: `` `t42_d1_g1_s1`.`orders` ``.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalTable {
    /// The shard schema, `t<tenant>_d<db>_g<group>_s<shard>`.
    pub schema: String,
    /// The table name (table names are never translated, chapter 07).
    pub table: String,
}

/// The type class of a column, for the `type_conversion` reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnClass {
    /// `CHAR`, `VARCHAR`, `TEXT`, `ENUM`, `SET`.
    String,
    /// Integers, `DECIMAL`, `FLOAT`, `DOUBLE`, `BIT`.
    Numeric,
    /// `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `YEAR`.
    Temporal,
    /// Anything else (`BLOB`, `JSON`, geometry, vectors).
    Other,
}

/// Maps the statement's logical names to catalog objects. Implemented by
/// the gateway over the session's catalog snapshot; every method must be
/// cheap and must not block (the extractor runs on first sight of a
/// fingerprint, on the session's task).
pub trait TableResolver: Send + Sync {
    /// The table `db.table`. `db` is the qualifier as written, else the
    /// session's current database; `None` when there is neither. Names are
    /// as written (quotes removed); the resolver applies the tenant's
    /// `lower_case_table_names` rule. `None` for unknown tables, views it
    /// doesn't index and system schemas.
    fn resolve(&self, db: Option<&str>, table: &str) -> Option<ResolvedTable>;

    /// The id of logical database `db` (as written; the resolver applies
    /// `lower_case_table_names`), for the database part of a fingerprint's
    /// id when a statement names a table outside the session's database.
    /// `table` is a table the statement names in it: the default resolves
    /// it, so it is `None` for an unknown table; resolvers that can look
    /// up a database by name alone should.
    fn database(&self, db: &str, table: &str) -> Option<DbId> {
        self.resolve(Some(db), table).map(|t| t.db_id)
    }

    /// Whether `table` has a column named `column` (lower case), to
    /// attribute unqualified columns in multi-table statements. `None`
    /// when unknown.
    fn has_column(&self, table: &ResolvedTable, column: &str) -> Option<bool> {
        let _ = (table, column);
        None
    }

    /// The type class of a column, for the `type_conversion` reason.
    /// `None` when unknown, in which case that reason is never given.
    fn column_class(&self, table: &ResolvedTable, column: &str) -> Option<ColumnClass> {
        let _ = (table, column);
        None
    }
}

/// A resolver that knows no table: shapes keep logical names only, with
/// no object ids.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoResolver;

impl TableResolver for NoResolver {
    fn resolve(&self, _db: Option<&str>, _table: &str) -> Option<ResolvedTable> {
        None
    }
}
