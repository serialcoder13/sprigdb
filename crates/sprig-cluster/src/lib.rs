//! `sprig-cluster`: the cluster registry, probes, topology drivers, fencing primitives and `BackendExecutor`, the one way into MariaDB.
//!
//! Layer: 1 foundations. Design: [chapter 09](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/09-clusters.md) (Clusters) of the SprigDB low-level design.
//!
//! What exists (chapter 09, build steps 1-4 and 8):
//! - [`record`] and [`registry`]: the cluster record, the registry over
//!   [`MetaStore`](sprig_meta::MetaStore) with global id allocation,
//!   compare-and-set writes, events and watches;
//! - [`probe`]: light, full and deep probes against MariaDB and their
//!   parsers (version, topology, semi-sync, binlog, GTID, privileges), and
//!   the active probes (XA, features, JSON, generated columns, PL/SQL);
//! - [`profile`]: `derive()` from probes to a capability profile, profile
//!   diffs, registration gates and publishing with hysteresis;
//! - [`health`]: the health model and the heartbeat writer;
//! - [`sys`]: the internal schema `__sprig_sys`;
//! - [`settings`]: the `cluster.*` keys these steps use.
//!
//! Not yet: topology drivers and the Galera writer choice (steps 5-6),
//! fences (step 7), the binlog dump probe (needs `sprig-binlog`), the admin
//! API (step 9) and server tuning (steps 10-12).

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

pub use health::{ClusterHealth, HealthReport, HealthTracker, HeartbeatWriter, NodeState};
pub use probe::{NodeProbe, ProbeDepth, Prober};
pub use profile::{CapabilityProfile, CapabilityPublisher, DeriveInput, derive};
pub use record::{AdminState, ClusterRecord, TopologyKind};
pub use registry::{ClusterEvent, ClusterRegistry, Registration};
pub use settings::ClusterSettings;
pub use sql::{NodeEndpoint, ProbeError, ProbeSession};
