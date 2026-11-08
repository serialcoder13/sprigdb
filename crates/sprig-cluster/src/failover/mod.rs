//! Cluster failover per topology (chapter 25, "Galera: writer nodes and
//! lost quorum", "Primary + replicas, driven by SprigDB", "Primary +
//! replicas on Kubernetes, single nodes, external clusters", "Damping
//! failover flapping"; build steps 2-5).
//!
//! - [`primary_replica`]: drain relay logs, choose the most advanced
//!   replica, estimate the loss from the semi-sync sample, check the RPO,
//!   promote, repoint the others, fence a node with `read_only`, and the
//!   diverged-transaction report for a returning former primary;
//! - [`galera`]: the quorum watch: writer re-picks come from chapter 09's
//!   writer set (`driver::galera::WriterSetTracker`), and lost quorum is
//!   bootstrapped only from the node holding the highest seqno seen in the
//!   last primary component, with every other node confirmed down;
//! - [`topology`]: single nodes (a restart is not a failover; standbys or a
//!   restore) and external clusters (follow the provider's endpoint);
//! - [`damper`]: the failover-flapping damper (independent sources, the
//!   minimum interval, suppression).
//!
//! These are the operations the controller's `cluster_failover` loop
//! drives; they hold no state of their own beyond what is passed in, so a
//! controller that dies mid-way resumes from the cluster record.
//!
//! **Where it differs from the book.** Chapter 25 puts these behind
//! chapter 09's `FailoverHooks` trait, returned by
//! `TopologyDriver::failover()`. The steps need per-topology inputs the
//! trait doesn't carry (replication credentials as the replicas see the
//! new primary, the semi-sync sample, the seqno history), so they are
//! plain functions and small state machines here; `failover()` still
//! returns `None`.

pub mod damper;
pub mod galera;
pub mod primary_replica;
pub mod topology;

pub use damper::{Damp, DownSources, FlapDamper};
pub use galera::{GaleraAction, GaleraNodeStatus, GaleraQuorum};
pub use primary_replica::{Candidate, Choice, DivergedRange, ReplSource, ReplicaState};
