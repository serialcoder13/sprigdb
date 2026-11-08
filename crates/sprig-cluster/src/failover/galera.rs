//! Galera: writer nodes and lost quorum (chapter 25).
//!
//! ```text
//! galera_watch(c), every second on the DC leader:
//! 1. probe each node: wsrep_cluster_status, wsrep_local_state_comment, wsrep_ready,
//!    wsrep_last_committed; remember the highest last_committed seen in a Primary component
//! 2. P = nodes that are Primary, Synced and ready
//! 3. P not empty: each shard whose writer node is not in P gets a node of P
//!    (chapter 09's writer set and rendezvous choice). No fence is needed: nodes
//!    outside the primary component refuse queries (error 1047)
//! 4. P empty for failover.galera_quorum_wait_seconds (quorum lost):
//!    a. failover.mode is manual -> alert only
//!    b. b = the reachable node with the highest wsrep_last_committed
//!    c. require b's value >= the highest seen in the last Primary component, and every
//!       other node confirmed down; otherwise alert and wait
//!    d. on b: SET GLOBAL wsrep_provider_options = 'pc.bootstrap=YES'
//! ```
//!
//! Step 3 is chapter 09's `WriterSetTracker` (the controller's `registry`
//! loop publishes the writer set; gateways pick each shard's writer from
//! it by rendezvous hashing). [`GaleraQuorum`] is steps 1, 2 and 4.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_common::ids::NodeId;

use crate::sql::{NodeEndpoint, ProbeError, ProbeSession, TextRow};

/// One node's wsrep state (step 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GaleraNodeStatus {
    /// The node.
    pub node: NodeId,
    /// The probe connected.
    pub reachable: bool,
    /// `wsrep_cluster_status`.
    pub cluster_status: String,
    /// `wsrep_local_state_comment`.
    pub local_state: String,
    /// `wsrep_ready`.
    pub ready: bool,
    /// `wsrep_last_committed`.
    pub last_committed: u64,
    /// The node is confirmed down: its agent lease is gone and its host is
    /// unreachable, or the provisioner reports it gone. A probe failure
    /// alone never confirms it.
    pub confirmed_down: bool,
}

impl GaleraNodeStatus {
    /// Primary, Synced and ready: may take writes.
    #[must_use]
    pub fn usable(&self) -> bool {
        self.reachable
            && self.cluster_status == "Primary"
            && self.local_state == "Synced"
            && self.ready
    }

    /// An unreachable node.
    #[must_use]
    pub fn unreachable(node: NodeId) -> Self {
        Self {
            node,
            reachable: false,
            cluster_status: String::new(),
            local_state: String::new(),
            ready: false,
            last_committed: 0,
            confirmed_down: false,
        }
    }
}

fn status_value(rows: &[TextRow], name: &str) -> Option<String> {
    rows.iter().find_map(|r| {
        let n = r.first()?.1.as_deref()?;
        n.eq_ignore_ascii_case(name)
            .then(|| r.get(1).and_then(|(_, v)| v.clone()))
            .flatten()
    })
}

/// Probes one node (step 1).
pub async fn probe_node(ep: &NodeEndpoint, timeout: Duration) -> GaleraNodeStatus {
    async fn go(ep: &NodeEndpoint, timeout: Duration) -> Result<GaleraNodeStatus, ProbeError> {
        let mut s = ProbeSession::connect(ep, timeout).await?;
        let rows = s
            .rows(
                "SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_cluster_status', \
                 'wsrep_local_state_comment', 'wsrep_ready', 'wsrep_last_committed')",
            )
            .await?;
        s.close().await;
        Ok(GaleraNodeStatus {
            node: ep.node.clone(),
            reachable: true,
            cluster_status: status_value(&rows, "wsrep_cluster_status").unwrap_or_default(),
            local_state: status_value(&rows, "wsrep_local_state_comment").unwrap_or_default(),
            ready: status_value(&rows, "wsrep_ready").is_some_and(|v| v == "ON"),
            last_committed: status_value(&rows, "wsrep_last_committed")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            confirmed_down: false,
        })
    }
    go(ep, timeout)
        .await
        .unwrap_or_else(|_| GaleraNodeStatus::unreachable(ep.node.clone()))
}

/// What the watch decided this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GaleraAction {
    /// A primary component exists; these nodes may write (step 3 re-picks
    /// writers among them).
    Primary(Vec<NodeId>),
    /// No primary component, for less than the wait.
    WaitQuorum {
        /// How long quorum has been lost.
        lost_for: Duration,
    },
    /// Bootstrap a new primary component on this node (step 4d).
    Bootstrap(NodeId),
    /// Quorum lost and no automatic bootstrap is safe: alert, keep waiting.
    Blocked(String),
}

/// The quorum watch's memory (steps 1, 2 and 4).
#[derive(Debug, Clone, Default)]
pub struct GaleraQuorum {
    /// The highest `wsrep_last_committed` seen in a primary component.
    highest_primary: u64,
    /// Per node, the last value seen while it was in a primary component.
    seen: BTreeMap<NodeId, u64>,
    lost_since: Option<Instant>,
}

impl GaleraQuorum {
    /// A watch with no history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest seqno seen in a primary component.
    #[must_use]
    pub fn highest_primary(&self) -> u64 {
        self.highest_primary
    }

    /// One pass over every node's status.
    pub fn observe(
        &mut self,
        nodes: &[GaleraNodeStatus],
        now: Instant,
        quorum_wait: Duration,
        manual: bool,
    ) -> GaleraAction {
        for n in nodes.iter().filter(|n| n.reachable && n.cluster_status == "Primary") {
            self.highest_primary = self.highest_primary.max(n.last_committed);
            self.seen.insert(n.node.clone(), n.last_committed);
        }
        let mut primary: Vec<NodeId> = nodes
            .iter()
            .filter(|n| n.usable())
            .map(|n| n.node.clone())
            .collect();
        if !primary.is_empty() {
            primary.sort();
            self.lost_since = None;
            return GaleraAction::Primary(primary);
        }
        let since = *self.lost_since.get_or_insert(now);
        let lost_for = now.saturating_duration_since(since);
        if lost_for < quorum_wait {
            return GaleraAction::WaitQuorum { lost_for };
        }
        if manual {
            return GaleraAction::Blocked("quorum lost; failover.mode is manual".into());
        }
        let Some(best) = nodes
            .iter()
            .filter(|n| n.reachable)
            .max_by(|a, b| {
                a.last_committed
                    .cmp(&b.last_committed)
                    .then_with(|| b.node.cmp(&a.node))
            })
        else {
            return GaleraAction::Blocked("quorum lost; no node is reachable".into());
        };
        if best.last_committed < self.highest_primary {
            return GaleraAction::Blocked(format!(
                "quorum lost; the most advanced reachable node {} has seqno {}, below {} seen \
                 in the last primary component",
                best.node, best.last_committed, self.highest_primary
            ));
        }
        let unconfirmed: Vec<&str> = nodes
            .iter()
            .filter(|n| n.node != best.node && !n.confirmed_down)
            .map(|n| n.node.as_str())
            .collect();
        if !unconfirmed.is_empty() {
            return GaleraAction::Blocked(format!(
                "quorum lost; not confirmed down: {}",
                unconfirmed.join(", ")
            ));
        }
        GaleraAction::Bootstrap(best.node.clone())
    }
}

/// Step 4d on the chosen node.
///
/// # Errors
/// The statement failed.
pub async fn bootstrap(ep: &NodeEndpoint, timeout: Duration) -> Result<(), ProbeError> {
    let mut s = ProbeSession::connect(ep, timeout).await?;
    s.exec("SET GLOBAL wsrep_provider_options = 'pc.bootstrap=YES'")
        .await?;
    s.close().await;
    Ok(())
}

#[cfg(test)]
mod tests;
