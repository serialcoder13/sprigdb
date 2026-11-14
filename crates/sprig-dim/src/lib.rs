//! `sprig-dim`: dimensional models and the Analytics API compiler.
//!
//! Layer: 2 data layer.
//!
//! A compiler onto the one execution layer, not an engine: a query becomes
//! SQL with partial aggregates the caller's session runs (one shard, or
//! many through the router), and the gateway merges, adds subtotals with
//! `GROUPING()` bits, orders and limits.
//!
//! - [`model`]: definitions and their checks.
//! - [`expr`]: the safe grammar of measure expressions and filters.
//! - [`query`]: queries and their compilation to SQL.
//! - [`merge`]: subtotals, order, limit; exact decimal sums.
//! - [`store`]: models in `__sprig_dim_models`, validation, running a
//!   query, drilling.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::missing_errors_doc,
    clippy::many_single_char_names
)]

pub mod expr;
pub mod merge;
pub mod model;
pub mod query;
pub mod store;

pub use model::Model;
pub use query::{Compiled, Query};
