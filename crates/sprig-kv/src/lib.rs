//! `sprig-kv`: the key-value engine.
//!
//! Layer: 2 data layer.
//!
//! - [`sql`]: the SQL a front end runs ([`SqlRunner`]), and binary-safe
//!   literals.
//! - [`layout`]: a namespace's three tables, their DDL, the distribution
//!   rules (segments and Redis hash tags) and the routing hash.
//! - [`time`]: expiry instants as MariaDB `DATETIME(3)` literals, so only
//!   MariaDB's clock counts.
//! - [`api`]: the `KeyValue` operations (get, put with preconditions,
//!   delete, scan, transactions) over string keys.
//! - [`geo`]: keys' locations and `GeoSearch`.
//!
//! Redis command semantics live in `sprig-redis`, over the
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
