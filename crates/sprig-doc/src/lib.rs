//! `sprig-doc`: the document engine.
//!
//! Layer: 2 data layer. Design: [chapter 31](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/31-documents.md) (Documents) of the SprigDB low-level design.
//!
//! - [`json`]: JSON rendering, the type map and the way back to BSON;
//! - [`keyenc`]: the order-preserving `_id` encoding and MongoDB's
//!   comparison order;
//! - [`sessions`]: retryable-write records (chapter 30);
//! - [`geo`]: `$geoWithin`, `$geoIntersects`, `$near`, `$geoNear` and
//!   `2dsphere` columns (chapter 36, R6 round 3).

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
/// Collations (chapter 31, R5 round 4).
pub mod collation;
pub mod engine;
pub mod filter;
/// MongoDB's geospatial operators (chapter 36, R6 round 3).
pub mod geo;
pub mod heavy;
pub mod json;
pub mod keyenc;
pub mod meta;
pub mod sessions;
pub mod sql;
/// `$text`, `$search` and `$searchMeta` (chapter 39, R5 round 5).
pub mod text;
pub mod translate;
pub mod update;
