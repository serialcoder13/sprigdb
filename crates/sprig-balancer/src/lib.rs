//! `sprig-balancer`: placement, rebalancing, and split and merge scheduling.
//!
//! Layer: 2 operations.
//!
//! Placement and rebalancing:
//!
//! | Part | Module |
//! |---|---|
//! | The decision log | [`decisions`] |
//! | Load collection | [`load`] |
//! | Capacity, utilization and pressure | [`model`] |
//! | Constraints and `feasible()` | [`constraints`] |
//! | `place()` and the rebalance planner | [`plan`] |
//! | Scheduling, guardrails, Galera writer weights | [`schedule`], [`writers`] |
//! | Recommendations and `explain placement` | [`recommend`], [`explain`] |
//! | Fleet simulator | `sprig-testkit::fleet` |
//! | Schema caps and the creation rate limit | [`model`], [`constraints`], [`ratelimit`] |
//!
//! Automatic split, merge and move are in [`shards`].
//!
//! The planner is pure and deterministic over a [`model::FleetView`]; the
//! controller builds the view from etcd, probes, storage scans and gateway
//! load reports, and the [`schedule::Scheduler`] turns plans into decisions
//! and mover jobs through a [`schedule::MoveExecutor`].

pub mod constraints;
pub mod decisions;
pub mod explain;
pub mod judge;
pub mod load;
pub mod model;
pub mod mover;
pub mod pins;
pub mod plan;
pub mod ratelimit;
pub mod recommend;
pub mod schedule;
pub mod settings;
pub mod shards;
pub mod thrash;
pub mod writers;

pub use decisions::{DECISIONS_MODULE, DecisionLog, MemDecisionLog, NewDecision, Outcome};
pub use model::{ClusterView, FleetView, LoadVector, ShardKey, ShardView, Timestamp};
pub use plan::{PlacementPolicy, PlanReport, PlannedMove, Planner};
pub use schedule::{MoveExecutor, MoveOrder, MoveStatus, MoveTicket, Scheduler};
pub use settings::{BalancerMode, BalancerSettings};

#[cfg(test)]
pub(crate) mod testutil;
