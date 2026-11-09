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
    clippy::missing_errors_doc
)]

pub mod model;
pub mod policy;
pub mod runtime;
pub mod target;
