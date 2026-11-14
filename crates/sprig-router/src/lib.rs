//! `sprig-router`: the distributed planner and executor.
//!
//! Layer: 2 execution.
//!
//! The gateway hands the router each statement after the tenant name
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
//! - **Gather**: a `SELECT` none of the above splits exactly (window
//!   functions, subqueries, set operations, joins of several groups or on
//!   any condition) runs once on the session's cluster, over temporary
//!   copies of the rows it reads ([`gather`]).
//! - **Analytic reads**: a gathered `SELECT` the analytic guard
//!   accepts runs in DataFusion over streams of the shards' rows instead,
//!   on keys that compare as MariaDB compares ([`analytics`]).
//! - **Graph**: a statement with `GRAPH_TABLE` ([`graph`]):
//!   the graph calls run first, then the statement with their rows as
//!   derived tables plans again; literal rows join inside each shard.
//!   Each hop of a graph read is a router route too ([`graph_route`]):
//!   Bolt and `GRAPH_TABLE` share this planner.
//! - **Reject**: `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420) with a
//!   reason code; never a different answer.
//!
//! [`exec::execute`] runs a plan through the gateway's [`exec::ShardBackend`]
//! ([`exec::execute_to`] streams a scatter's rows). [`topology`] is the
//! router's interface to sharding: the gateway builds it from the
//! catalog's table groups and `sprig-shard`'s shard maps
//! ([`topology::catalog`]). Prepared statements are parsed once
//! ([`planner::parse_template`]) and planned per execution with their
//! parameters ([`planner::plan_prepared`]).

// Short names (a, b, e, q, s, t) follow the SQL.
#![allow(clippy::many_single_char_names)]
// The planner and executor are long step-by-step functions.
#![allow(clippy::too_many_lines)]

pub mod analytics;
pub mod collation;
pub mod compound;
pub mod exec;
pub mod fk;
pub mod gather;
pub mod graph;
pub mod graph_route;
pub mod plan;
pub mod planner;
pub mod reject;
pub mod settings;
pub mod topology;
pub mod value;
pub mod write_notes;

pub use analytics::{AnalyticsLimits, AnalyticsMode};
pub use exec::{
    Budget, Output, RowSink, ShardBackend, ShardOk, ShardOutcome, ShardRows, ShardStream, execute,
    execute_to,
};
pub use gather::{GatherLimits, GatherPlan, Scratch};
pub use graph::{CallRows, GraphCall, GraphLit, GraphPlan, GraphStep};
pub use plan::{Plan, ShardAt, Target};
pub use planner::{
    ParamLit, PlanCtx, PreparedPlan, Template, metadata_sql, parse_template, plan, plan_gathered,
    plan_graph, plan_prepared, render_params,
};
pub use reject::{Reason, Rejection};
pub use settings::RouterConfig;
pub use topology::{StaticTopology, Topology, TopologyFile};

/// `router.max_route_values`' default (the setting:
/// [`settings::MAX_ROUTE_VALUES`]).
pub const MAX_ROUTE_VALUES: usize = 1024;
/// `router.lookup_batch_size`'s default.
pub const LOOKUP_BATCH: usize = 256;
/// What a fenced or retired source shard returns: 1142, 1044, 1146.
pub const STALE_MAP_CODES: [u16; 3] = [1142, 1044, 1146];
