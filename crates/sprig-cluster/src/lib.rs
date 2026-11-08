//! `sprig-cluster`: the cluster registry, probes, topology drivers, fencing primitives and `BackendExecutor`, the one way into MariaDB.
//!
//! Layer: 1 foundations. Design: [chapter 09](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/09-clusters.md) (Clusters) of the SprigDB low-level design.
//!
//! What exists (chapter 09, build steps 1-8):
//! - [`record`] and [`registry`]: the cluster record, the registry over
//!   [`MetaStore`](sprig_meta::MetaStore) with global id allocation,
//!   compare-and-set writes, events and watches;
//! - [`probe`]: light, full and deep probes against MariaDB and their
//!   parsers (version, topology, semi-sync, binlog, GTID, privileges), and
//!   the active probes (XA, features, JSON, generated columns, PL/SQL), and
//!   the binlog settings the change feed needs ([`probe::feed`]);
//! - [`profile`]: `derive()` from probes to a capability profile, profile
//!   diffs, registration gates and publishing with hysteresis;
//! - [`health`]: the health model and the heartbeat writer;
//! - [`driver`]: the topology drivers for a single node, a primary with
//!   replicas, an external cluster and Galera, with the rendezvous writer
//!   choice ([`driver::galera`]);
//! - [`fence`]: `RevokeKill` and `Rename` shard fences, receipts, unfence;
//! - [`sys`]: the internal schema `__sprig_sys`;
//! - [`settings`]: the `cluster.*` keys these steps use.
//!
//! Step 9, the Admin API's `Clusters` service and `sprigctl cluster`, is in
//! `sprig-controller::admin` and `sprigctl`; the controller's `registry`
//! loop (`sprig-controller::monitor`) runs the probes and publishes.
//!
//! Not yet: the binlog dump probe and the true feed retention (both need
//! `sprig-binlog`), server tuning (steps 10-12), and failover hooks with
//! the `read_only` self-fence (step 13, R2).

pub mod driver;
pub mod fence;
pub mod health;
pub mod probe;
pub mod profile;
pub mod record;
pub mod registry;
pub mod settings;
pub mod sql;
pub mod sys;
#[cfg(test)]
mod testutil;

pub use driver::{BasicDriver, GaleraDriver, TopologyDriver};
pub use fence::{FenceReceipt, FenceRequest, FenceScope};
pub use health::{ClusterHealth, HealthReport, HealthTracker, HeartbeatWriter, NodeState};
pub use probe::{NodeProbe, ProbeDepth, Prober};
pub use profile::{CapabilityProfile, CapabilityPublisher, DeriveInput, derive};
pub use record::{AdminState, ClusterRecord, TopologyKind};
pub use registry::{ClusterEvent, ClusterRegistry, Registration};
pub use settings::ClusterSettings;
pub use sql::{NodeEndpoint, ProbeError, ProbeSession};
