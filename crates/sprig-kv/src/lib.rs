//! `sprig-kv`: the key-value engine (chapter 33).
//!
//! Layer: 2 data layer. Design: [chapter 33](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/33-kv.md) (Key-value) of the SprigDB low-level design.
//!
//! - [`sql`]: the SQL a front end runs ([`SqlRunner`]), and binary-safe
//!   literals.
//! - [`layout`]: a namespace's three tables, their DDL, the distribution
//!   rules (segments and Redis hash tags) and the routing hash.
//! - [`time`]: expiry instants as MariaDB `DATETIME(3)` literals, so only
//!   MariaDB's clock counts.
//! - [`api`]: the `KeyValue` operations (get, put with preconditions,
//!   delete, scan, transactions) over string keys.
//! - [`geo`]: keys' locations and `GeoSearch` (chapter 36, R6 round 3).
//!
//! Redis command semantics live in `sprig-redis` (chapter 40), over the
//! same tables.

// Redis's number rules are casts by design: lengths and counts become RESP
// integers, scores move between doubles and 52-bit geohashes, and the
// casts are checked where a value could leave its range.
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_cmp
)]

pub mod api;
pub mod geo;
pub mod layout;
pub mod sql;
pub mod sweep;
pub mod time;

pub use layout::{DistRule, KeyType, Namespace};
pub use sql::{Row, SqlError, SqlRunner};
