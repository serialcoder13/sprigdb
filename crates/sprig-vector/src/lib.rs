//! `sprig-vector`: vector search (chapter 34).
//!
//! Layer: 2 data layer. Design: [chapter 34](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/34-vector.md) (Vectors) of the SprigDB low-level design.
//!
//! A translator onto the one execution layer, not an engine: every
//! statement runs through the caller's [`sprig_kv::SqlRunner`] (in the
//! gateway, the user's executor session), so names, privileges and the
//! router apply, and a sharded table's `ORDER BY distance LIMIT n` runs on
//! each shard and is merged by distance in the gateway (chapter 17).
//!
//! - [`index`]: standalone indexes (Vectors API): table, definition in the
//!   table comment, the HNSW graph on and off.
//! - [`items`]: upserts and deletes, dimension checks, `dotProduct`
//!   normalization, metadata stored as chapter 31 stores documents.
//! - [`plan`]: chapter 34's planner (exact, prefiltered exact, ANN, ANN
//!   with over-fetch).
//! - [`search`]: running a plan: the per-shard SQL, residual filters with
//!   MongoDB's semantics, re-queries, scores; automatic graphs.
//! - [`brute`]: exact distances in Rust, the reference of the tests and of
//!   recall validation.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::missing_errors_doc,
    clippy::many_single_char_names
)]

pub mod brute;
pub mod index;
pub mod items;
pub mod plan;
pub mod search;

pub use index::{AnnMode, AnnState, IndexDef, Metric, Similarity};
pub use plan::{Plan, Settings};
pub use search::{Hit, SearchOutcome, SearchSpec};

use sprig_common::SprigError;
use sprig_kv::SqlError;

/// A backend error as SprigDB's (its number and message kept).
#[must_use]
pub fn backend(e: SqlError) -> SprigError {
    SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", e.message)
}
