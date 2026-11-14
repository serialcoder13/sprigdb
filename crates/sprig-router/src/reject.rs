//! The rejection catalogue: a
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
    /// An `INSERT` row without a key value.
    AutoIncrement,
    /// An `INSERT` key value that isn't a literal of the key's class.
    InsertKey,
    /// Shards on several clusters where a write or a join with an
    /// unsharded table needs one (distributed transactions).
    CrossCluster,
    /// Shards answered with different result columns.
    SchemaSkew,
    /// A write form that would leave a unique lookup index
    /// unmaintained: an upsert, `INSERT IGNORE`, a non-literal value, an
    /// `UPDATE` of a lookup or primary-key column, a limited `DELETE`.
    Lookup,
    /// A statement form the router doesn't distribute.
    Unsupported,
    /// A stored function or UDF in a statement across shards (it could
    /// read tables the router doesn't see).
    Routine,
    /// MariaDB `FULLTEXT` relevance compared across shards.
    FulltextRelevance,
    /// A gathered read the session can't run: the guard's reason is
    /// in the detail.
    Gather,
    /// A write form that would leave a foreign key the gateway keeps
    /// unchecked: a non-literal key value, `INSERT
    /// IGNORE` or an upsert on a child, `REPLACE` on a parent, a
    /// multi-table write over such tables.
    ForeignKey,
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
            Self::Routine => "routine",
            Self::FulltextRelevance => "fulltext_relevance",
            Self::Gather => "gather",
            Self::ForeignKey => "foreign_key",
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

impl Rejection {
    /// The rejection a 39420 error carries (its message names the reason
    /// first, as [`Rejection::error`] writes it).
    #[must_use]
    pub fn from_error(e: &SprigError) -> Option<Self> {
        if e.code() != Some(SprigCode::StatementNotDistributable) {
            return None;
        }
        let rest = e.message.split_once('(')?.1;
        let (code, detail) = rest.split_once("): ")?;
        let reason = ALL.iter().copied().find(|r| r.as_str() == code)?;
        Some(Self::new(reason, detail))
    }
}

/// Every reason, for [`Rejection::from_error`].
const ALL: &[Reason] = &[
    Reason::JoinCondition,
    Reason::JoinTypes,
    Reason::JoinDepth,
    Reason::CorrelatedSubquery,
    Reason::DerivedTable,
    Reason::RecursiveCte,
    Reason::WindowFunction,
    Reason::SetOperation,
    Reason::Aggregate,
    Reason::Ordering,
    Reason::LimitedDml,
    Reason::MultiTableDml,
    Reason::InsertSelect,
    Reason::UpsertUnique,
    Reason::KeyChange,
    Reason::Variables,
    Reason::FoundRows,
    Reason::Locking,
    Reason::Parse,
    Reason::Subquery,
    Reason::AutoIncrement,
    Reason::InsertKey,
    Reason::CrossCluster,
    Reason::SchemaSkew,
    Reason::Lookup,
    Reason::Unsupported,
    Reason::Routine,
    Reason::FulltextRelevance,
    Reason::Gather,
    Reason::ForeignKey,
];

/// Shorthand for a rejection result.
pub(crate) fn reject<T>(reason: Reason, detail: impl Into<String>) -> Result<T, Rejection> {
    Err(Rejection::new(reason, detail))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn a_rejection_survives_its_error() {
        for r in ALL {
            let x = Rejection::new(*r, "a detail (with parentheses): here");
            assert_eq!(Rejection::from_error(&x.error()), Some(x));
        }
        assert_eq!(Rejection::from_error(&SprigError::internal("x")), None);
    }
}
