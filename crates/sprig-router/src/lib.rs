//! `sprig-router`: the distributed planner and executor.
//!
//! Layer: 2 execution. Design: [chapter 17](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/17-router.md) (Router) of the SprigDB low-level design.
//!
//! The gateway hands the router each statement after chapter 07's name
//! rewrite. [`planner::plan`] decides which shards run what:
//!
//! - **Pass**: every sharded table on one shard of one cluster; the
//!   statement goes there with its schema names spliced, and the gateway
//!   relays the answer undecoded (the single-shard path).
//! - **Read** (`Scatter`, `Merge`, `Aggregate`): one statement per shard
//!   with hidden columns (`WEIGHT_STRING(e LEVEL k)` per sort key, partial
//!   aggregates), merged in the gateway on weight bytes with the
//!   `PAD SPACE` rule, aggregates split and finished exactly, `LIMIT`
//!   pushed down as `m+n`.
//! - **`LookupJoin`**: two tables of different groups, batched `IN` lists.
//! - **Write**: multi-row `INSERT` split by shard, multi-shard `UPDATE`
//!   and `DELETE`, in one transaction (or a statement savepoint).
//! - **Explain**: `EXPLAIN FORMAT=SPRIG`.
//! - **Reject**: `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420) with a
//!   reason code; never a different answer.
//!
//! [`exec::execute`] runs a plan through the gateway's [`exec::ShardBackend`].
//! [`topology`] is the router's interface to chapter 15 until
//! `sprig-shard` publishes table groups and shard maps.

// Short names (a, b, e, q, s, t) follow the SQL and the book's pseudocode.
#![allow(clippy::many_single_char_names)]
// The planner and executor follow the book step by step.
#![allow(clippy::too_many_lines)]

pub mod collation;
pub mod exec;
pub mod plan;
pub mod planner;
pub mod reject;
pub mod topology;
pub mod value;

pub use exec::{Budget, Output, ShardBackend, ShardOk, ShardOutcome, ShardRows, execute};
pub use plan::{Plan, Target};
pub use planner::{PlanCtx, plan};
pub use reject::{Reason, Rejection};
pub use topology::{StaticTopology, Topology, TopologyFile};

/// `router.max_route_values`' default.
pub const MAX_ROUTE_VALUES: usize = 1024;
/// `router.lookup_batch_size`'s default.
pub const LOOKUP_BATCH: usize = 256;
/// `router.max_statement_memory_mb`'s default.
pub const MAX_STATEMENT_MEMORY_MB: u64 = 256;
