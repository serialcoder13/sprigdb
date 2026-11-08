//! The shape extractor (chapter 12, "Extracting predicates and columns";
//! build step 2).
//!
//! [`extract`] turns one SQL statement into a [`Shape`]: its tables, the
//! predicates of the top-level `AND` conjuncts of every `WHERE` and `ON`,
//! joins, `ORDER BY`, `GROUP BY`, selected columns, written columns and
//! the unsargable reasons. It parses with sqlparser-rs where it can and
//! falls back to a lexer-level reading of the tables and simple conjuncts
//! ([`Shape::partial`]) where it can't. It never panics.
//!
//! The public types are in [`types`] and re-exported here; they are the
//! contract with `sprig-advisor`.

mod extract;
mod resolve;
pub mod types;

pub use extract::{ExtractCtx, extract};

pub use resolve::{ColumnClass, NoResolver, PhysicalTable, ResolvedTable, TableResolver};
pub use types::{
    ColumnRef, Join, JoinKind, JoinOp, OrderItem, PredOp, Predicate, SHAPE_VERSION, Shape, SortDir,
    StmtType, TableRef, Unsargable, WriteKind, Writes,
};

#[cfg(test)]
pub(crate) mod tests;
