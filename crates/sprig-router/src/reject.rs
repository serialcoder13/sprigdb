//! The rejection catalogue (chapter 17, "Statements R1 rejects"): a
//! statement the router can't plan exactly fails with
//! `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420) naming one of these
//! reasons, and never returns an answer that differs from one unsharded
//! MariaDB's.

use sprig_common::{SprigCode, SprigError};

/// Why a statement can't be distributed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    /// A cross-shard join without a usable equality, or with a condition
    /// the gateway can't evaluate.
    JoinCondition,
    /// A cross-shard join of a string with a number.
    JoinTypes,
    /// More lookup joins than `router.max_lookup_joins`.
    JoinDepth,
    /// A correlated subquery not co-located with its outer row.
    CorrelatedSubquery,
    /// A derived table, CTE or view over shards.
    DerivedTable,
    /// `WITH RECURSIVE` over multi-shard tables.
    RecursiveCte,
    /// A window function across shards.
    WindowFunction,
    /// `UNION`, `INTERSECT`, `EXCEPT` across shards.
    SetOperation,
    /// An aggregate the gateway can't split exactly.
    Aggregate,
    /// Ordering the gateway can't reproduce (GEOMETRY, ENUM, an unknown
    /// collation, a non-deterministic expression).
    Ordering,
    /// Multi-shard UPDATE or DELETE with `ORDER BY` or `LIMIT`.
    LimitedDml,
    /// Multi-table UPDATE or DELETE, not on one shard.
    MultiTableDml,
    /// `INSERT … SELECT` into a sharded table.
    InsertSelect,
    /// Upsert or `REPLACE` with a unique key lacking the distribution key.
    UpsertUnique,
    /// An UPDATE that changes a distribution key (key moves come later).
    KeyChange,
    /// Variable assignment across shards.
    Variables,
    /// `SQL_CALC_FOUND_ROWS` across shards.
    FoundRows,
    /// `LOCK TABLES` or `HANDLER` on a sharded table.
    Locking,
    /// Statement text the router's parser doesn't read.
    Parse,
    /// An uncorrelated subquery over shards (pull-out comes later).
    Subquery,
    /// An `INSERT` row without a key value (ID blocks, chapter 19, come
    /// later).
    AutoIncrement,
    /// An `INSERT` key value that isn't a literal of the key's class.
    InsertKey,
    /// Shards on several clusters where a write or a join with an
    /// unsharded table needs one (distributed transactions, chapter 18).
    CrossCluster,
    /// Shards answered with different result columns.
    SchemaSkew,
    /// A write form that would leave a unique lookup index (chapter 19)
    /// unmaintained: an upsert, `INSERT IGNORE`, a non-literal value, an
    /// `UPDATE` of a lookup or primary-key column, a limited `DELETE`.
    Lookup,
    /// A statement form the router doesn't distribute.
    Unsupported,
}

impl Reason {
    /// The reason code shown in messages and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JoinCondition => "join_condition",
            Self::JoinTypes => "join_types",
            Self::JoinDepth => "join_depth",
            Self::CorrelatedSubquery => "correlated_subquery",
            Self::DerivedTable => "derived_table",
            Self::RecursiveCte => "recursive_cte",
            Self::WindowFunction => "window_function",
            Self::SetOperation => "set_operation",
            Self::Aggregate => "aggregate",
            Self::Ordering => "ordering",
            Self::LimitedDml => "limited_dml",
            Self::MultiTableDml => "multi_table_dml",
            Self::InsertSelect => "insert_select",
            Self::UpsertUnique => "upsert_unique",
            Self::KeyChange => "key_change",
            Self::Variables => "variables",
            Self::FoundRows => "found_rows",
            Self::Locking => "locking",
            Self::Parse => "parse",
            Self::Subquery => "subquery",
            Self::AutoIncrement => "auto_increment",
            Self::InsertKey => "insert_key",
            Self::CrossCluster => "cross_cluster",
            Self::SchemaSkew => "schema_skew",
            Self::Lookup => "lookup",
            Self::Unsupported => "unsupported",
        }
    }
}

/// A rejection: the reason and a short detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    /// Why.
    pub reason: Reason,
    /// What, in a few words.
    pub detail: String,
}

impl Rejection {
    /// A rejection for `reason`.
    pub fn new(reason: Reason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    /// The client's error: 39420 with the reason code first.
    #[must_use]
    pub fn error(&self) -> SprigError {
        metrics::counter!("sprig_router_rejected_total", "reason" => self.reason.as_str())
            .increment(1);
        SprigError::new(
            SprigCode::StatementNotDistributable,
            format!(
                "Statement not distributable ({}): {}",
                self.reason.as_str(),
                self.detail
            ),
        )
    }
}

/// Shorthand for a rejection result.
pub(crate) fn reject<T>(reason: Reason, detail: impl Into<String>) -> Result<T, Rejection> {
    Err(Rejection::new(reason, detail))
}
