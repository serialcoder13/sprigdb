//! The sharding advisor (chapter 16): table groups, distribution keys and
//! the first design before a tenant's first split.
//!
//! | Build step | Module |
//! |---|---|
//! | 1. Input assembly from the catalog and chapter 12's store | [`input`] |
//! | 2-6. Graph, structural design, key filters, scoring, search, confidence, verdicts | [`design`] |
//! | 4. Key-value units: shard a Redis database or namespace only when a rule says so | [`kv`] |
//! | 7. `advisor_runs` and decision-log evidence | [`runs`] |
//! | 1. Stored code keeps the tables it names in place (bugfix-31) | [`code`] |
//! | 9. The first-design plan: regroup by `RENAME` under a hold, `_sprig_rk`, back-fill | [`apply`] |
//!
//! [`settings`] declares the `advisor.*` keys.

// Short names (c, d, s, t, g) follow the book's pseudocode; the apply steps
// follow the book step by step.
#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::too_many_arguments
)]

pub mod apply;
pub mod code;
pub mod design;
pub mod input;
pub mod kv;
pub mod runs;
pub mod settings;

pub use design::{Cause, Design, Input, Score, Verdict, decide, search, structural};
pub use settings::{AdvisorMode, AdvisorSettings};
