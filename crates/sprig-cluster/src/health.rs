//! The health model, light-probe assessment and the heartbeat writer.
//!
//! Writers' heartbeats, read on replicas, measure lag without
//! `Seconds_Behind_Master`. A prober that can't reach the writer reports
//! `unknown`, never `no_writer`, so a partitioned controller never fails a
//! cluster over.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use sprig_common::ids::NodeId;

use crate::probe::{GaleraLocalState, NodeFacts, NodeProbe};
use crate::record::TopologyKind;
use crate::settings::ClusterSettings;
use crate::sql::{ProbeError, ProbeSession, literal};
use crate::sys;

/// One node's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// Reachable, expected role, lag in bounds.
    Ok,
    /// Lag above `cluster.max_replica_lag_seconds`.
    Lagging,
    /// Galera donor, or desynced for an RSU build.
    Desynced,
    /// Galera state transfer.
    Joining,
    /// Replication stopped, unexpected `read_only`, failed login, or a
    /// Galera node outside the primary component.
    Broken,
    /// `cluster.unhealthy_after_failures` failed probes.
    Unreachable,
}

/// The cluster's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterHealth {
    /// Every node `ok`.
    Healthy,
    /// A writer, but something is wrong: no new placements.
    Degraded,
    /// Positively no writer: writes fail with 39100; failover decides.
    NoWriter,
    /// No probe of the writer succeeds from this controller: nothing
    /// automatic acts.
    Unknown,
}

impl fmt::Display for ClusterHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::NoWriter => "no_writer",
            Self::Unknown => "unknown",
        })
    }
}

/// The role a node was seen in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedRole {
    /// Accepts writes and replicates from no one.
    Writer,
    /// Replicates from another node.
    Replica,
    /// A Galera node.
    Galera,
    /// Not known (no answer).
    Unknown,
}

/// The assessment of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAssessment {
    /// The node.
    pub node: NodeId,
    /// Its state.
    pub state: NodeState,
    /// Its role.
    pub role: ObservedRole,
    /// Replication lag, from the writer's heartbeat or else
    /// `Seconds_Behind_Master`.
    pub lag_ms: Option<u64>,
    /// Consecutive failed probes.
    pub failures: u32,
    /// Why, when not `ok`; on an `ok` Galera node, [`LOST_IDENTITY`]
    /// when its cluster lost its state UUID.
    pub reason: Option<String>,
}

/// The assessment of a cluster from one probe pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReport {
    /// The cluster's state.
    pub health: ClusterHealth,
    /// Each node.
    pub nodes: Vec<NodeAssessment>,
    /// The writer nodes seen.
    pub writers: Vec<NodeId>,
}

/// Tracks consecutive failures per node across light probes.
#[derive(Debug, Clone, Default)]
pub struct HealthTracker {
    failures: HashMap<NodeId, u32>,
    last: HashMap<NodeId, NodeAssessment>,
}

impl HealthTracker {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Assesses one pass of probes.
    pub fn observe(
        &mut self,
        topology: TopologyKind,
        probes: &[NodeProbe],
        settings: &ClusterSettings,
    ) -> HealthReport {
        let nodes: Vec<NodeAssessment> = probes
            .iter()
            .map(|p| {
                let failures = self.failures.entry(p.node.clone()).or_insert(0);
                if p.result.is_ok() {
                    *failures = 0;
                } else {
                    *failures += 1;
                }
                let failures = *failures;
                let a = assess_node(topology, p, failures, probes, settings);
                // Below the failure threshold a node keeps its last state.
                let a = match (&p.result, self.last.get(&p.node)) {
                    (Err(e), Some(prev))
                        if !e.is_auth_failure() && failures < settings.unhealthy_after_failures =>
                    {
                        NodeAssessment {
                            failures,
                            reason: Some(format!("probe failed ({failures}): {e}")),
                            ..prev.clone()
                        }
                    }
                    _ => a,
                };
                self.last.insert(p.node.clone(), a.clone());
                a
            })
            .collect();
        let (health, writers) = assess_cluster(topology, probes, &nodes);
        HealthReport {
            health,
            nodes,
            writers,
        }
    }
}

/// Lag of `facts` behind the writers, in milliseconds: the freshest writer
/// heartbeat it holds, or `Seconds_Behind_Master`.
fn lag_ms(facts: &NodeFacts, writers: &[&str]) -> Option<u64> {
    let beat = facts.heartbeats.as_ref().and_then(|rows| {
        rows.iter()
            .filter(|h| writers.contains(&h.writer_node.as_str()))
            .map(|h| h.age_ms)
            .min()
    });
    beat.or_else(|| {
        facts
            .channels
            .iter()
            .filter_map(|c| c.seconds_behind)
            .max()
            .map(|s| s.saturating_mul(1000))
    })
}

fn role(facts: &NodeFacts) -> ObservedRole {
    if facts.galera.is_some() {
        ObservedRole::Galera
    } else if facts.is_replica() {
        ObservedRole::Replica
    } else {
        ObservedRole::Writer
    }
}

/// The reason a Galera node whose cluster state UUID is all zeros carries.
pub const LOST_IDENTITY: &str = "Galera cluster has lost its state UUID \
     (wsrep_cluster_state_uuid is all zeros): a node that leaves can't rejoin; \
     see RUNBOOK.md, \"Galera: a node that can't rejoin\"";

/// What is wrong with a Galera node, if anything.
fn galera_problem(g: &crate::probe::GaleraStatus) -> Option<(NodeState, String)> {
    if !g.ready || g.cluster_status != "Primary" {
        return Some((
            NodeState::Broken,
            format!(
                "wsrep_ready={} wsrep_cluster_status={}",
                g.ready, g.cluster_status
            ),
        ));
    }
    match g.local_state {
        GaleraLocalState::Synced => None,
        GaleraLocalState::DonorDesynced => Some((NodeState::Desynced, "donor or desynced".into())),
        GaleraLocalState::Joining | GaleraLocalState::Joined => {
            Some((NodeState::Joining, "state transfer".into()))
        }
        GaleraLocalState::Other => Some((NodeState::Broken, "unknown wsrep_local_state".into())),
    }
}

/// Assesses one node.
#[must_use]
pub fn assess_node(
    topology: TopologyKind,
    probe: &NodeProbe,
    failures: u32,
    all: &[NodeProbe],
    settings: &ClusterSettings,
) -> NodeAssessment {
    let mut a = NodeAssessment {
        node: probe.node.clone(),
        state: NodeState::Ok,
        role: ObservedRole::Unknown,
        lag_ms: None,
        failures,
        reason: None,
    };
    let facts = match &probe.result {
        Ok(f) => f,
        Err(e) => {
            a.state = if e.is_auth_failure() {
                NodeState::Broken
            } else if failures >= settings.unhealthy_after_failures {
                NodeState::Unreachable
            } else {
                NodeState::Ok
            };
            a.reason = Some(format!("probe failed ({failures}): {e}"));
            return a;
        }
    };
    a.role = role(facts);
    let not_ok = |a: &mut NodeAssessment, s: NodeState, why: String| {
        a.state = s;
        a.reason = Some(why);
    };
    match (&facts.galera, a.role) {
        (Some(g), _) => {
            if let Some((state, why)) = galera_problem(g) {
                not_ok(&mut a, state, why);
            } else if g.lost_identity() {
                // Still a writer (the node is `ok`), but the cluster is
                // degraded: see `assess_cluster`.
                a.reason = Some(LOST_IDENTITY.into());
            }
        }
        (None, ObservedRole::Replica) => {
            let writers: Vec<String> = all
                .iter()
                .filter(|p| p.facts().is_some_and(|f| role(f) == ObservedRole::Writer))
                .map(|p| p.node.to_string())
                .collect();
            let writers: Vec<&str> = writers.iter().map(String::as_str).collect();
            a.lag_ms = lag_ms(facts, &writers);
            if let Some(c) = facts.channels.iter().find(|c| !c.running()) {
                not_ok(
                    &mut a,
                    NodeState::Broken,
                    format!(
                        "replication stopped: io={:?} sql={:?} io_errno={} sql_errno={}",
                        c.io, c.sql, c.last_io_errno, c.last_sql_errno
                    ),
                );
            } else if let Some(lag) = a.lag_ms
                && lag > settings.max_replica_lag_seconds.saturating_mul(1000)
            {
                not_ok(&mut a, NodeState::Lagging, format!("lag {lag} ms"));
            }
        }
        (None, _) => {
            if facts
                .server
                .read_only
                .is_some_and(crate::probe::ReadOnly::is_read_only)
            {
                not_ok(
                    &mut a,
                    NodeState::Broken,
                    "unexpected read_only on the writer".into(),
                );
            } else if topology == TopologyKind::Galera {
                not_ok(
                    &mut a,
                    NodeState::Broken,
                    "wsrep is off on a Galera node".into(),
                );
            }
        }
    }
    a
}

/// Assesses the cluster from its nodes.
#[must_use]
pub fn assess_cluster(
    topology: TopologyKind,
    probes: &[NodeProbe],
    nodes: &[NodeAssessment],
) -> (ClusterHealth, Vec<NodeId>) {
    if probes.iter().all(|p| p.result.is_err()) {
        return (ClusterHealth::Unknown, Vec::new());
    }
    // A Galera cluster that lost its state UUID still writes, but is
    // degraded: no node can join it until it is bootstrapped again.
    let lost_identity = probes.iter().any(|p| {
        p.facts()
            .and_then(|f| f.galera.as_ref())
            .is_some_and(crate::probe::GaleraStatus::lost_identity)
    });
    let all_ok = !lost_identity && nodes.iter().all(|n| n.state == NodeState::Ok);
    let unanswered = probes.iter().any(|p| p.result.is_err());
    // Galera: every usable node can write. Otherwise: writable non-replicas.
    let writer_role = if topology == TopologyKind::Galera {
        ObservedRole::Galera
    } else {
        ObservedRole::Writer
    };
    let writers: Vec<NodeId> = nodes
        .iter()
        .zip(probes)
        .filter(|(n, p)| n.role == writer_role && n.state == NodeState::Ok && p.result.is_ok())
        .map(|(n, _)| n.node.clone())
        .collect();
    let health = if writers.is_empty() {
        // A writer we can't reach may still be there: only a positive
        // observation gives no_writer.
        if unanswered {
            ClusterHealth::Unknown
        } else {
            ClusterHealth::NoWriter
        }
    } else if writers.len() > 1 && topology != TopologyKind::Galera {
        // Two writable primaries.
        ClusterHealth::Degraded
    } else if all_ok && !unanswered {
        ClusterHealth::Healthy
    } else {
        ClusterHealth::Degraded
    };
    (health, writers)
}

/// Writes the heartbeat of one writer node into
/// `__sprig_sys.__sprig_heartbeat` every `cluster.heartbeat_interval_ms`.
/// The role owning the loop calls [`HeartbeatWriter::beat`].
#[derive(Debug, Clone)]
pub struct HeartbeatWriter {
    writer: NodeId,
    seq: u64,
}

impl HeartbeatWriter {
    /// A writer for `writer`, starting after `last_seq` (0 for a new one).
    #[must_use]
    pub fn new(writer: NodeId, last_seq: u64) -> Self {
        Self {
            writer,
            seq: last_seq,
        }
    }

    /// The last sequence number written.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Writes the next heartbeat; returns its sequence number.
    ///
    /// # Errors
    /// The write failed or timed out; the sequence number is not consumed.
    pub async fn beat(&mut self, session: &mut ProbeSession) -> Result<u64, ProbeError> {
        let seq = self.seq + 1;
        // Only while the node is writable: sprig_ctl bypasses
        // read_only, and a beat sent to a writer that a provider just
        // demoted (from facts a probe read moments before) logged an errant
        // transaction there. A node that turned read-only writes nothing,
        // so nothing reaches its binlog.
        session
            .exec(&format!(
                "INSERT INTO {} (writer_node, seq, written_at) \
                 SELECT {}, {seq}, UTC_TIMESTAMP(6) FROM DUAL WHERE @@GLOBAL.read_only = 0 \
                 ON DUPLICATE KEY UPDATE seq = VALUES(seq), written_at = VALUES(written_at)",
                sys::heartbeat_table(),
                literal(self.writer.as_str())
            ))
            .await?;
        self.seq = seq;
        Ok(seq)
    }
}

#[cfg(test)]
mod tests;
