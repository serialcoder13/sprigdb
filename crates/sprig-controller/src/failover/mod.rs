//! DC failover and recovery (chapter 25, "DC leases, sub-leases and
//! self-fencing", "The global failover decision", "Promotion and epochs",
//! "Recovery after an outage"; build steps 7-11).
//!
//! The pieces, by who runs them:
//!
//! - **every controller of a DC**: `sprig_meta::dclease::DcLease` keeps the
//!   DC lease; [`service`] serves `Permits.Renew`, `Peer.Ping` and
//!   `Observer.ObserveDc` from it ([`observe`] is the observer's half);
//! - **the global leader**: [`global::GlobalFailover`], the decision
//!   (records in [`record`], pure rules in [`decision`]) and
//!   [`promote`], the per-tenant promotion with its commit point;
//! - **the returning DC's leader**: [`recovery::ReturningDc`] fences moved
//!   tenants before the DC grants permits again; [`recovery`] also has the
//!   diverged-transaction report and the stepped return of traffic;
//! - **gateways, agents, workers**: `sprig_meta::permit::PermitHolder`
//!   and its watchdog, renewed through [`client::HttpPermitSource`].
//!
//! Cluster-level failover per topology is `sprig_cluster::failover`;
//! [`cluster`] is the DC leader's `cluster_failover` loop that drives it.
//! Settings are [`settings`].
//!
//! In the controller role ([`role`], R2 wiring): every controller keeps
//! the DC lease and serves the internal routes; the DC leader publishes
//! the observe targets and runs the return of its DC; the global leader
//! runs the decision. [`nodes`] is the MariaDB side (standby replication,
//! the copy fence, standby lags, the diverged report); [`manual`] is
//! `sprigctl tenant failover` and `sprigctl tenant standby`.

pub mod client;
pub mod cluster;
pub mod decision;
pub mod global;
pub mod manual;
pub mod nodes;
pub mod observe;
pub mod observed;
pub mod promote;
pub mod record;
pub mod recovery;
pub mod relay;
pub mod role;
pub mod service;
pub mod settings;
pub mod two_tier;

pub use global::{GlobalFailover, ObserverClient};
pub use promote::{Guard, PromoteHooks, Promotion};
pub use record::{FailoverRecord, FailoverState, Verdict, VerdictKind};
pub use settings::FailoverSettings;
