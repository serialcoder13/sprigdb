//! `sprig-logic`: the logic layer: catalog of code, invocation, budgets and the `db` interface.
//!
//! Layer: 2 logic.
//!
//! What exists:
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
//! - [`invoke`]: calling an engine-tier version through its wrapper's
//!   marker row, on the fallback runtime ([`engine`]);
//! - [`settings`]: the fallback tier's budgets and outbound allowlist;
//! - [`messages`]: triggers on streams and queues;
//! - [`testrun`]: `Logic.TestRun`, a run that never writes;
//! - [`rollout`]: staged rollout by percentage;
//! - [`drop`](mod@drop): `DROP FUNCTION | PROCEDURE` and their dependents.
//!
//! Not yet: validations, triggers, schedules, computed fields, QuickJS
//! for JavaScript, invocation from other protocols, rollout and rollback,
//! deploys through the schema-change engine.

// Hand-written lexers and parsers read best with short names.
#![allow(
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::format_push_string,
    clippy::cast_precision_loss,
    clippy::assigning_clones
)]

pub mod catalog;
pub mod cron;
pub mod declare;
pub mod deploy;
pub mod drop;
pub mod dropdb;
pub mod enforce;
pub mod invoke;
pub mod messages;
pub mod msgrun;
pub mod placement;
pub mod rollback;
pub mod rollout;
pub mod schedule;
pub mod service;
pub mod settings;
pub mod testrun;
pub mod triggers;

/// The fallback tier (`sprig-logic-js`): the native KIRun runtime, its
/// `db` bridge, budgets and outbound allowlist.
pub use sprig_logic_js as engine;

/// The compiler (`sprig-logic-compile`).
pub use sprig_logic_compile as compiler;

pub use catalog::{LOGIC_MODULE, LogicStore};
pub use declare::{Declaration, is_declaration};
