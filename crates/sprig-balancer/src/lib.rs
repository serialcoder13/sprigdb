//! `sprig-balancer`: placement, rebalancing, and split and merge scheduling.
//!
//! Layer: 2 operations. Design: [chapter 11](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/11-placement.md) (Placement) and [chapter 20](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/20-split-merge.md) (Split and merge) of the SprigDB low-level design.
//!
//! Chapter 11's R0 build steps (milestone M0.6):
//!
//! | Step | Module |
//! |---|---|
//! | 1. The decision log | [`decisions`] |
//! | 2. Load collection | [`load`] |
//! | 3. Capacity, utilization and pressure | [`model`] |
//! | 4. Constraints and `feasible()` | [`constraints`] |
//! | 5. `place()` and the rebalance planner | [`plan`] |
//! | 6. Scheduling, guardrails, Galera writer weights | [`schedule`], [`writers`] |
//! | 7. Recommendations and `explain placement` | [`recommend`], [`explain`] |
//! | 8. Fleet simulator | `sprig-testkit::fleet` |
//! | 9. Schema caps and the creation rate limit | [`model`], [`constraints`], [`ratelimit`] |
//!
//! The planner is pure and deterministic over a [`model::FleetView`]; the
//! controller builds the view from etcd, probes, storage scans and gateway
//! load reports, and the [`schedule::Scheduler`] turns plans into decisions
//! and mover jobs through a [`schedule::MoveExecutor`].

pub mod constraints;
pub mod decisions;
pub mod explain;
pub mod load;
pub mod model;
pub mod mover;
pub mod plan;
pub mod ratelimit;
pub mod recommend;
pub mod schedule;
pub mod settings;
pub mod thrash;
pub mod writers;

pub use decisions::{DECISIONS_MODULE, DecisionLog, MemDecisionLog, NewDecision, Outcome};
pub use model::{ClusterView, FleetView, LoadVector, ShardKey, ShardView, Timestamp};
pub use plan::{PlacementPolicy, PlanReport, PlannedMove, Planner};
pub use schedule::{MoveExecutor, MoveOrder, MoveStatus, MoveTicket, Scheduler};
pub use settings::{BalancerMode, BalancerSettings};

#[cfg(test)]
pub(crate) mod testutil;
