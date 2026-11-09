//! The writer lease and the agent's registration (chapter 25, "Primary +
//! replicas, driven by SprigDB" step 7; chapter 23's DC-tier keys
//! `leases/writer/<cluster_id>` and `agents/<id>`), shared by the agent
//! that holds them and the controller's `cluster_failover` loop that reads
//! them.
//!
//! - An agent registers `agents/<id>` under its own lease and says whether
//!   its `mysqld` answers. A node that refuses connections for
//!   [`MYSQLD_DOWN_AFTER`] is `down`; one that only times out is slow, not
//!   down (a paused process).
//! - The writer's agent holds `leases/writer/<cluster>`. When the
//!   controller fails a cluster over, it first claims the key for the
//!   chosen replica (`agent` = `controller:<id>`, a short lease of its own,
//!   created only when missing): no other node can take it from then on,
//!   and the chosen node's agent takes it over (compare on the revision),
//!   which binds it to the agent's own session.

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::ClusterId;
use sprig_meta::{Key, Tier, Versioned};

/// A node whose connections are refused this long is `down`.
pub const MYSQLD_DOWN_AFTER: std::time::Duration = std::time::Duration::from_secs(2);

/// The `agent` of a lease the controller claimed for a node it promotes.
pub const CONTROLLER_PREFIX: &str = "controller:";

/// What an agent last saw of its `mysqld`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MysqldState {
    /// Not known yet (older agents don't say).
    #[default]
    Unknown,
    /// It answers.
    Up,
    /// It refused connections for [`MYSQLD_DOWN_AFTER`].
    Down,
}

/// `agents/<id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    /// The cluster.
    pub cluster: String,
    /// The node.
    pub node: String,
    /// The node's `host:port`.
    pub addr: String,
    /// The build.
    #[serde(default)]
    pub version: String,
    /// Whether `mysqld` answers.
    #[serde(default)]
    pub mysqld: MysqldState,
}

impl Versioned for AgentRecord {
    const V: u32 = 1;
}

/// `leases/writer/<cluster>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLease {
    /// The writer node.
    pub node_id: String,
    /// The holding agent, or `controller:<id>` while a promotion hands it
    /// to the node's agent.
    pub agent: String,
    /// Raised by every automatic or manual cluster failover.
    #[serde(default)]
    pub writer_epoch: u64,
}

impl Versioned for WriterLease {
    const V: u32 = 1;
}

impl WriterLease {
    /// The controller holds the key for `node_id`'s agent to take over.
    #[must_use]
    pub fn claimed_by_controller(&self) -> bool {
        self.agent.starts_with(CONTROLLER_PREFIX)
    }

    /// Whether the agent `agent` of node `node` may hold this lease: it is
    /// its own, or the controller claimed it for that node.
    #[must_use]
    pub fn may_be_held_by(&self, agent: &str, node: &str) -> bool {
        self.agent == agent || (self.claimed_by_controller() && self.node_id == node)
    }
}

/// `leases/writer/<cluster>` in `tier`.
///
/// # Errors
/// Internal for an id that isn't a key segment.
pub fn writer_lease_key(tier: &Tier, cluster: &ClusterId) -> Result<Key> {
    tier.key(&format!("leases/writer/{cluster}"))
}

/// `agents/` in `tier`.
///
/// # Errors
/// Internal never in practice.
pub fn agents_prefix(tier: &Tier) -> Result<Key> {
    tier.key("agents/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_controller_claim_is_taken_only_by_its_node() {
        let l = WriterLease {
            node_id: "db2".into(),
            agent: format!("{CONTROLLER_PREFIX}ctl-fra1"),
            writer_epoch: 3,
        };
        assert!(l.claimed_by_controller());
        assert!(l.may_be_held_by("agent-db2", "db2"));
        assert!(!l.may_be_held_by("agent-db1", "db1"));
        let own = WriterLease {
            agent: "agent-db2".into(),
            ..l
        };
        assert!(own.may_be_held_by("agent-db2", "db2"));
        assert!(!own.may_be_held_by("agent-db3", "db2"));
    }

    #[test]
    fn older_records_decode_with_defaults() {
        let a: AgentRecord =
            serde_json::from_str(r#"{"cluster":"fra1-c1","node":"n1","addr":"h:1"}"#)
                .expect("decode");
        assert_eq!(a.mysqld, MysqldState::Unknown);
        let w: WriterLease =
            serde_json::from_str(r#"{"node_id":"n1","agent":"a"}"#).expect("decode");
        assert_eq!(w.writer_epoch, 0);
    }
}
