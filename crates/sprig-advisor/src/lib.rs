//! `sprig-advisor`: the sharding advisor and automatic indexing.
//!
//! Layer: 2 operations.
//!
//! What exists: the core of automatic indexing, [`autoindex`], and the
//! sharding advisor's first design, [`sharding`].

pub mod autoindex;
pub mod sharding;
mod sql;

#[cfg(test)]
mod live_tests;
