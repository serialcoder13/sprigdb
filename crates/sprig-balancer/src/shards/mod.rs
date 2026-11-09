//! Automatic split, merge and move (chapter 20): when a group's shards
//! split or merge, where the split points go, and which proposals become
//! mover jobs now.
//!
//! | Build step | Module |
//! |---|---|
//! | 1. Shard sizes; gateway write sketches | [`stats`], [`sketch`] |
//! | 2. Size histograms by index dives, refinement, heavy hashes | [`stats`] |
//! | 3. `choose_split` with hot-key isolation | [`split`] |
//! | 4. Triggers, sustain windows, cooldowns | [`triggers`] |
//! | 5. Merge rules | [`triggers`] |
//! | 6. Scheduler: priorities, limits, lock sets, exclusion | [`schedule`] |
//!
//! The controller's sharding loop (`sprig-controller::sharding`) gathers
//! the inputs, runs these, submits jobs to the mover and records each
//! decision in the tenant's decision log.

// Short names (a, b, c, h, p, s) follow the book's pseudocode.
#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines
)]

pub mod schedule;
pub mod settings;
pub mod sketch;
pub mod split;
pub mod stats;
pub mod triggers;

pub use schedule::{Admission, Backoff, RunningJob, admit, build_spec, decision, next_shard_id};
pub use settings::{ShardingMode, ShardingSettings};
pub use sketch::{HashRange, SketchCollector, WriteSketch, WriteSketches};
pub use split::{Bucket, HashHistogram, NoSplit, SplitPlan, Weight, choose_split};
pub use triggers::{GroupStat, Proposal, Reason, ShardJob, ShardStat, evaluate};
