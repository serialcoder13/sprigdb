//! Fault injection and Jepsen-style checking (chapter 46, "Fault injection
//! and Jepsen-style checking"; build step 3).
//!
//! One trait, [`Faults`], injects faults wherever a test runs:
//!
//! - in a simulation, [`crate::sim::SimFaults`] cuts and slows simulated
//!   links, kills, pauses and skews simulated nodes;
//! - against Docker Compose, [`ComposeFaults`] uses [`toxiproxy`] for link
//!   faults on proxied routes (latency, loss, resets, timeouts),
//!   [`faultd`] for `iptables` partitions inside containers, the Docker
//!   CLI for `SIGKILL` plus restart and `docker pause`, and [`faketime`]
//!   for clock skew in throwaway MariaDB containers started with
//!   libfaketime (the shared Compose servers can't be skewed: faultd has
//!   no way to move one container's wall clock, see [`faketime`]).
//!
//! [`history`] records operation histories ([`history::OpRecord`]) as JSON
//! lines and converts them to EDN; [`elle`] runs `elle-cli` on them.
//!
//! **Safety rail.** Compose faults act only on containers whose name starts
//! with [`OWN_CONTAINER_PREFIX`] (`sprig-sim-`) and on toxiproxy routes this
//! test created; a test can't break a shared server by mistake.

pub mod compose;
pub mod elle;
pub mod faketime;
pub mod faultd;
pub mod history;
pub mod toxiproxy;

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::Result;

pub use compose::ComposeFaults;

/// Containers that Compose faults may touch start with this prefix.
pub const OWN_CONTAINER_PREFIX: &str = "sprig-sim-";

/// A node a fault applies to: a simulated node, a container, or a
/// toxiproxy route, by name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Node(pub String);

impl Node {
    /// A node named `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Node {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Identifies one active fault, so it can be healed alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FaultId(pub u64);

impl fmt::Display for FaultId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fault-{}", self.0)
    }
}

/// A degraded link: added latency with jitter, and packet loss.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LinkFault {
    /// Added one-way latency.
    pub latency: Duration,
    /// Up to this much more, chosen per message (sim) or by toxiproxy.
    pub jitter: Duration,
    /// Probability of losing a message (sim), or of a connection being
    /// reset (toxiproxy's `reset_peer` toxicity).
    pub loss: f64,
}

impl LinkFault {
    /// Only latency.
    #[must_use]
    pub fn latency(d: Duration) -> Self {
        Self {
            latency: d,
            jitter: Duration::ZERO,
            loss: 0.0,
        }
    }
}

/// Injects faults (chapter 46, "Interfaces"). Every method returns once the
/// fault is in force; [`Faults::heal`] undoes one, [`Faults::heal_all`]
/// every fault this injector made.
#[async_trait::async_trait]
pub trait Faults: Send + Sync {
    /// Cuts every link between a node of `a` and a node of `b`, both ways.
    async fn partition(&self, a: &[Node], b: &[Node]) -> Result<FaultId>;
    /// Degrades the link between `a` and `b`, both ways.
    async fn link(&self, a: &Node, b: &Node, fault: LinkFault) -> Result<FaultId>;
    /// `SIGKILL`, then a restart.
    async fn kill(&self, n: &Node) -> Result<()>;
    /// Freezes the node (`SIGSTOP`, `docker pause`) until healed.
    async fn pause(&self, n: &Node) -> Result<FaultId>;
    /// Offsets the node's wall clock by `offset_ms` and its rate by
    /// `rate_ppm`, until healed.
    async fn skew_clock(&self, n: &Node, offset_ms: i64, rate_ppm: i64) -> Result<FaultId>;
    /// Undoes one fault. Healing a healed fault is a no-op.
    async fn heal(&self, id: FaultId) -> Result<()>;
    /// Undoes every fault this injector made.
    async fn heal_all(&self) -> Result<()>;
}
