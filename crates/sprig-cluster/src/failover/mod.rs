//! Cluster failover per topology.
//!
//! - [`primary_replica`]: drain relay logs, choose the most advanced
//!   replica, estimate the loss from the semi-sync sample, check the RPO,
//!   promote, repoint the others, fence a node with `read_only`, and the
//!   diverged-transaction report for a returning former primary;
//! - [`galera`]: the quorum watch: writer re-picks come from the Galera
//!   writer set (`driver::galera::WriterSetTracker`), and lost quorum is
//!   bootstrapped only from the node holding the highest seqno seen in the
//!   last primary component, with every other node confirmed down;
//! - [`topology`]: single nodes (a restart is not a failover; standbys or a
//!   restore) and external clusters (follow the provider's endpoint);
//! - [`damper`]: the failover-flapping damper (independent sources, the
//!   minimum interval, suppression);
//! - [`extract`]: `errant_extract`'s binlog read, row images and SQL,
//!   the sealed file format and the extract stores;
//! - [`rebuild`]: `node_rebuild`'s ticket, token, phases and the
//!   restored node's attach, shared by the worker and the agents;
//! - [`writer`]: the writer lease and the agent's registration, shared by
//!   the agent and the controller's `cluster_failover` loop;
//! - [`internal_tls`]: mutual TLS for the agents' internal service (the
//!   backup stream), shared by the agents and the workers.
//!
//! These are the operations the controller's `cluster_failover` loop
//! drives; they hold no state of their own beyond what is passed in, so a
//! controller that dies mid-way resumes from the cluster record.
//!
//! **Why not `FailoverHooks`.** These are not behind the drivers'
//! `FailoverHooks` trait (`TopologyDriver::failover()`): the steps need per-topology inputs the
//! trait doesn't carry (replication credentials as the replicas see the
//! new primary, the semi-sync sample, the seqno history), so they are
//! plain functions and small state machines here; `failover()` still
//! returns `None`.

pub mod damper;
pub mod extract;
pub mod galera;
pub mod internal_tls;
pub mod primary_replica;
pub mod rebuild;
pub mod topology;
pub mod writer;

pub use damper::{Damp, DownSources, FlapDamper};
pub use galera::{GaleraAction, GaleraNodeStatus, GaleraQuorum};
pub use primary_replica::{Candidate, Choice, DivergedRange, ReplSource, ReplicaState};
pub use writer::{AgentRecord, MysqldState, WriterLease};
