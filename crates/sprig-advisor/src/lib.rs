//! `sprig-advisor`: the sharding advisor and automatic indexing.
//!
//! Layer: 2 operations. Design: [chapter 13](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/13-auto-indexing.md) (Automatic indexing) and [chapter 16](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/16-advisor.md) (Sharding advisor) of the SprigDB low-level design.
//!
//! What exists: the core of automatic indexing, [`autoindex`] (chapter 13
//! build steps 1, 3, 4, 5 and 16), and the sharding advisor's first
//! design, [`sharding`] (chapter 16, R1).

pub mod autoindex;
pub mod sharding;
mod sql;

#[cfg(test)]
mod live_tests;
