//! `sprig-sinks`: continuous delivery from SprigDB sources to outside
//! systems (chapter 51, R9).
//!
//! Layer: 2 data layer. Design: [chapter 51](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/51-sinks.md) (Sinks) of the SprigDB low-level design.
//!
//! - [`model`]: connections, sinks, tasks, records, batches, outcomes;
//! - [`runtime`]: the task pipeline: lanes, batches, retries with full
//!   jitter, poison splitting, dead letters, pause and probe, settling.

// Statements and request texts read best as format!; the chapter's
// pseudocode names are kept.
#![allow(
    clippy::many_single_char_names,
    clippy::format_push_string,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::items_after_statements,
    clippy::match_same_arms,
    clippy::single_match_else,
    clippy::needless_pass_by_value,
    clippy::map_unwrap_or,
    clippy::collapsible_if,
    clippy::match_like_matches_macro,
    clippy::redundant_closure_for_method_calls
)]

pub mod filter;
pub mod grammar;
pub mod job;
pub mod meta;
pub mod model;
pub mod policy;
pub mod runtime;
pub mod secrets;
pub mod settings;
pub mod source;
pub mod sql;
pub mod store;
pub mod target;
