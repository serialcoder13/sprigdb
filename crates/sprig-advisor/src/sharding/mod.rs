//! The sharding advisor: table groups, distribution keys and
//! the first design before a tenant's first split.
//!
//! | Part | Module |
//! |---|---|
//! | Input assembly from the catalog and the workload store | [`input`] |
//! | Graph, structural design, key filters, scoring, search, confidence, verdicts | [`design`] |
//! | Key-value units: shard a Redis database or namespace only when a rule says so | [`kv`] |
//! | `advisor_runs` and decision-log evidence | [`runs`] |
//! | Stored code keeps the tables it names in place | [`code`] |
//! | The first-design plan: regroup by `RENAME` under a hold, `_sprig_rk`, back-fill | [`apply`] |
//!
//! [`settings`] declares the `advisor.*` keys.

// Short names (c, d, s, t, g) follow the design's pseudocode; the apply
// steps follow the design's plan step by step.
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
