//! `sprig-logic`: the logic layer: catalog of code, invocation, budgets and the `db` interface.
//!
//! Layer: 2 logic. Design: [chapter 41](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/41-logic.md) (Logic) of the SprigDB low-level design.
//!
//! What exists (R7 order 1 and 2):
//!
//! - [`catalog`]: the `logic` migration module (`logic_modules`,
//!   `logic_versions`, `logic_bindings`, `logic_schedules`,
//!   `logic_dead_letters`) and [`catalog::LogicStore`];
//! - [`declare`]: `CREATE FUNCTION | PROCEDURE ... LANGUAGE kirun |
//!   javascript AS ...`;
//! - [`placement`]: the tier and its reasons;
//! - [`deploy`]: routines into one shard schema as the deploying user,
//!   with a wrapper under the logical name;
//! - [`service`]: a deploy end to end, which the gateway runs for a
//!   declaration.
//!
//! Not yet: validations, triggers, schedules, computed fields, the
//! fallback runtime (`sprig-logic-js`, the `kirun` crate), invocation from
//! other protocols, rollout and rollback, deploys through chapter 21.

// Hand-written lexers and parsers read best with short names.
#![allow(
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::format_push_string,
    clippy::cast_precision_loss,
    clippy::assigning_clones
)]

pub mod catalog;
pub mod declare;
pub mod deploy;
pub mod placement;
pub mod service;

pub use catalog::{LOGIC_MODULE, LogicStore};
pub use declare::{Declaration, is_declaration};
