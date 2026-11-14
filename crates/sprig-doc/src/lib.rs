//! `sprig-doc`: the document engine.
//!
//! Layer: 2 data layer.
//!
//! - [`json`]: JSON rendering, the type map and the way back to BSON;
//! - [`keyenc`]: the order-preserving `_id` encoding and MongoDB's
//!   comparison order;
//! - [`sessions`]: retryable-write records;
//! - [`geo`]: `$geoWithin`, `$geoIntersects`, `$near`, `$geoNear` and
//!   `2dsphere` columns.

// MongoDB operator and type names fill the docs; the matcher and
// translator mirror MongoDB's single-letter notation (p, v, d, k) and
// long operator tables.
#![allow(
    clippy::doc_markdown,
    clippy::many_single_char_names,
    clippy::too_many_lines
)]
#![recursion_limit = "512"]

pub mod agg;
pub mod catalog;
/// Collations.
pub mod collation;
pub mod engine;
pub mod filter;
/// MongoDB's geospatial operators.
pub mod geo;
pub mod heavy;
/// Index hints as MariaDB `FORCE INDEX` (M6).
pub mod hint;
pub mod json;
pub mod keyenc;
pub mod meta;
/// MongoDB 9.0's per-operation memory limit (M3).
pub mod opmem;
pub mod policy;
/// MongoDB query shapes for query settings and `$queryStats` (M3).
pub mod qshape;
/// Server-side JavaScript: `$where`, `$function`, `$accumulator` (M4).
pub mod serverjs;
pub mod sessions;
/// Spilling to disk: `allowDiskUse` (M5).
pub mod spill;
pub mod sql;
/// `$text`, `$search` and `$searchMeta`.
pub mod text;
pub mod translate;
pub mod update;
/// Per-collection latencies and per-index use (M5).
pub mod usage;
/// Schema validation, `$jsonSchema` and the `$bits*` operators (M3).
pub mod validate;
