//! `sprig-cluster`: the cluster registry, probes, topology drivers, fencing primitives and `BackendExecutor`, the one way into MariaDB.
//!
//! Layer: 1 foundations.
//!
//! What exists:
//! - [`record`] and [`registry`]: the cluster record, the registry over
//!   [`MetaStore`](sprig_meta::MetaStore) with global id allocation,
//!   compare-and-set writes, events and watches;
//! - [`probe`]: light, full and deep probes against MariaDB and their
//!   parsers (version, topology, semi-sync, binlog, GTID, privileges), and
//!   the active probes (XA, features, JSON, generated columns, PL/SQL), the
//!   binlog settings the change feed needs ([`probe::feed`]), and the
//!   binlog dump probe ([`probe::dump`]: a short non-blocking dump as
//!   `sprig_repl` through `sprig-binlog`, run with the active probes when
//!   a `sprig_repl` endpoint is given);
//! - [`profile`]: `derive()` from probes to a capability profile, profile
//!   diffs, registration gates and publishing with hysteresis;
//! - [`health`]: the health model and the heartbeat writer;
//! - [`driver`]: the topology drivers for a single node, a primary with
//!   replicas, an external cluster and Galera, with the rendezvous writer
//!   choice ([`driver::galera`]);
//! - [`fence`]: `RevokeKill` and `Rename` shard fences, receipts, unfence;
//! - [`sys`]: the internal schema `__sprig_sys`;
//! - [`settings`]: the `cluster.*` keys these modules use;
//! - [`failover`]: cluster failover per topology (primary +
//!   replicas, Galera quorum, single node, external) and the flapping
//!   damper.
//!
//! The Admin API's `Clusters` service and `sprigctl cluster` are in
//! `sprig-controller::admin` and `sprigctl`; the controller's `registry`
//! loop (`sprig-controller::monitor`) runs the probes and publishes.
//!
//! Not yet: the true feed retention (now minus the horizon's commit time,
//! which needs a binlog read at the horizon), the dump probe in the
//! controller's `registry` loop (it calls [`probe::Prober::probe_cluster`],
//! which has no `sprig_repl` endpoint; registration runs it), live
//! tuning through the agent and operator ([`tuning`] has the plan and its
//! hysteresis). Failover is [`failover`], as plain functions rather than
//! `FailoverHooks` (see there).

pub mod driver;
pub mod failover;
pub mod fence;
pub mod health;
pub mod probe;
pub mod profile;
pub mod purge;
pub mod record;
pub mod registry;
pub mod settings;
pub mod sql;
pub mod sys;
#[cfg(test)]
mod testutil;
pub mod tuning;

pub use driver::{BasicDriver, GaleraDriver, TopologyDriver};
pub use fence::{FenceReceipt, FenceRequest, FenceScope};
pub use health::{ClusterHealth, HealthReport, HealthTracker, HeartbeatWriter, NodeState};
pub use probe::{NodeProbe, ProbeDepth, Prober};
pub use profile::{CapabilityProfile, CapabilityPublisher, DeriveInput, derive};
pub use record::{AdminState, ClusterRecord, TopologyKind};
pub use registry::{ClusterEvent, ClusterRegistry, Registration};
pub use settings::ClusterSettings;
pub use sql::{NodeEndpoint, ProbeError, ProbeSession};
