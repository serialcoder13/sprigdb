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
//! Cluster-level failover per topology is `sprig_cluster::failover`.
//! Settings are [`settings`].

pub mod client;
pub mod decision;
pub mod global;
pub mod observe;
pub mod promote;
pub mod record;
pub mod recovery;
pub mod service;
pub mod settings;

pub use global::{GlobalFailover, ObserverClient};
pub use promote::{Guard, PromoteHooks, Promotion};
pub use record::{FailoverRecord, FailoverState, Verdict, VerdictKind};
pub use settings::FailoverSettings;
