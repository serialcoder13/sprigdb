#![recursion_limit = "512"]
//! `sprig-doc`: the document engine.
//!
//! Layer: 2 data layer. Design: [chapter 31](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/31-documents.md) (Documents) of the SprigDB low-level design.
//!
//! - [`json`]: JSON rendering, the type map and the way back to BSON;
//! - [`keyenc`]: the order-preserving `_id` encoding and MongoDB's
//!   comparison order.

pub mod agg;
pub mod filter;
pub mod json;
pub mod keyenc;
pub mod meta;
pub mod sql;
pub mod translate;
pub mod update;
