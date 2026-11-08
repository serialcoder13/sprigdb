//! Read routing and read-your-writes (chapter 06, "Read routing and
//! read-your-writes"; build step 6).
//!
//! A plain read may leave the writer. Its **causal target** is the
//! session's last commit ([`GtidSet`], from session tracking's `last_gtid`)
//! plus, under `reads.causal_scope = tenant_gateway`, the [`Watermarks`] of
//! every commit by the same tenant through this gateway (in M0.2, the same
//! cluster and backend account: decision D1). [`choose`] then picks:
//!
//! - **Galera:** the least-loaded usable node; `wsrep_sync_wait = 1`
//!   through the sync prefix when the target isn't empty and the node isn't
//!   the shard's writer.
//! - **Primary and replicas:** a healthy replica within
//!   `reads.max_replica_lag_ms` whose polled position covers the target, or
//!   else one expected to catch up within `reads.gtid_wait_timeout_ms`,
//!   read with the [`wait_prefix`] (`MASTER_GTID_WAIT`, failing with 1242 on
//!   timeout so the statement reruns on the writer); else the writer.
//! - **Single node, external:** the writer.
//!
//! Positions come from the backend monitor, which polls replicas'
//! `@@gtid_slave_pos` and writers' `@@gtid_binlog_pos` every
//! `reads.replica_position_poll_ms`.

pub mod gtid;
pub mod settings;

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

pub use gtid::{GtidParseError, GtidSet};
use sprig_cluster::profile::RywMethod;
use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{ClusterId, NodeId};

use crate::backend::cluster::{ClusterView, NodeKey, NodeRef};
use crate::backend::health::{NodeHealth, NodeRole, NodeTable};
use crate::backend::settings::{BackendSettings, Routing};

/// Where a read goes (chapter 06, "Interfaces").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadTarget {
    /// The writer.
    Writer,
    /// A replica; `wait_for` is the target to wait for, `None` when its
    /// position already covers it.
    Replica {
        /// The replica.
        node: NodeRef,
        /// What to wait for with the wait-or-fail prefix.
        wait_for: Option<GtidSet>,
    },
    /// A Galera node other than (or equal to) the writer.
    GaleraNode {
        /// The node.
        node: NodeRef,
        /// Read with `wsrep_sync_wait = 1`.
        sync_wait: bool,
    },
}

impl ReadTarget {
    /// The metric label.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadTarget::Writer => "writer",
            ReadTarget::Replica { .. } => "replica",
            ReadTarget::GaleraNode { .. } => "galera",
        }
    }
}

/// Every commit by a tenant through this gateway, per cluster and backend
/// account (chapter 06: `reads.causal_scope = tenant_gateway`).
#[derive(Debug, Default)]
pub struct Watermarks {
    map: Mutex<HashMap<(ClusterId, String), GtidSet>>,
}

impl Watermarks {
    /// No commits yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a commit.
    pub fn advance(&self, cluster: &ClusterId, account: &str, gtid: &GtidSet) {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((cluster.clone(), account.to_owned()))
            .or_default()
            .union(gtid);
    }

    /// The watermark of a tenant's account on a cluster.
    #[must_use]
    pub fn get(&self, cluster: &ClusterId, account: &str) -> GtidSet {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(cluster.clone(), account.to_owned()))
            .cloned()
            .unwrap_or_default()
    }
}

/// Picks the node for a plain read (see the module docs). `writer` is the
/// shard's writer (`None` when no node qualifies right now), `load` the
/// leased connections per node, `exclude` nodes that already failed this
/// statement, and `spread` a counter that rotates among equally loaded
/// Galera nodes so reads spread over the cluster.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn choose(
    view: &ClusterView,
    writer: Option<&NodeRef>,
    nodes: &NodeTable,
    load: &dyn Fn(&NodeKey) -> u32,
    target: &GtidSet,
    settings: &BackendSettings,
    exclude: &[NodeId],
    spread: u64,
) -> ReadTarget {
    if settings.routing == Routing::Writer {
        return ReadTarget::Writer;
    }
    match view.topology {
        TopologyKind::Galera => {
            let usable: Vec<&NodeRef> = view
                .nodes
                .iter()
                .filter(|n| !exclude.contains(&n.node))
                .filter(|n| {
                    let s = nodes.status(&view.key(&n.node));
                    s.usable() && s.health == NodeHealth::Up
                })
                .collect();
            let least = usable.iter().map(|n| load(&view.key(&n.node))).min();
            let ties: Vec<&NodeRef> = usable
                .into_iter()
                .filter(|n| Some(load(&view.key(&n.node))) == least)
                .collect();
            let best =
                (!ties.is_empty()).then(|| ties[usize::try_from(spread).unwrap_or(0) % ties.len()]);
            match best {
                Some(n) => ReadTarget::GaleraNode {
                    sync_wait: !target.is_empty() && Some(&n.node) != writer.map(|w| &w.node),
                    node: n.clone(),
                },
                None => ReadTarget::Writer,
            }
        }
        TopologyKind::PrimaryReplica => {
            if view.ryw == RywMethod::WriterOnly && !target.is_empty() {
                return ReadTarget::Writer;
            }
            let mut cands: Vec<(&NodeRef, Option<GtidSet>, Duration)> = view
                .nodes
                .iter()
                .filter(|n| Some(&n.node) != writer.map(|w| &w.node) && !exclude.contains(&n.node))
                .filter_map(|n| {
                    let s = nodes.status(&view.key(&n.node));
                    let ok = s.usable()
                        && s.health == NodeHealth::Up
                        && matches!(s.role(), NodeRole::Replica)
                        && s.lag.is_some_and(|l| l <= settings.max_replica_lag);
                    ok.then(|| (n, s.slave_pos.clone(), s.lag.unwrap_or_default()))
                })
                .collect();
            cands.sort_by_key(|(n, _, _)| load(&view.key(&n.node)));
            if let Some((n, _, _)) = cands
                .iter()
                .find(|(_, pos, _)| pos.as_ref().is_some_and(|p| p.covers(target)))
            {
                return ReadTarget::Replica {
                    node: (*n).clone(),
                    wait_for: None,
                };
            }
            if view.ryw == RywMethod::GtidWait
                && let Some((n, _, _)) = cands
                    .iter()
                    .find(|(_, _, lag)| *lag < settings.gtid_wait_timeout)
            {
                return ReadTarget::Replica {
                    node: (*n).clone(),
                    wait_for: Some(target.clone()),
                };
            }
            ReadTarget::Writer
        }
        TopologyKind::SingleNode | TopologyKind::External => ReadTarget::Writer,
    }
}

/// `MASTER_GTID_WAIT`'s timeout argument, in seconds.
fn wait_seconds(timeout: Duration) -> String {
    format!("{:.3}", timeout.as_secs_f64())
}

/// The wait-or-fail prefix for text statements: it waits for `target` and,
/// on timeout, fails with 1242 before the client's statement runs.
#[must_use]
pub fn wait_prefix(target: &GtidSet, timeout: Duration) -> String {
    format!(
        "SET @__sprig_w = (SELECT CASE WHEN MASTER_GTID_WAIT('{target}', {}) = 0 \
         THEN 1 ELSE (SELECT 1 FROM information_schema.ENGINES) END)",
        wait_seconds(timeout)
    )
}

/// The separate round trip prepared statements wait with: returns 0 once
/// applied, -1 on timeout.
#[must_use]
pub fn wait_query(target: &GtidSet, timeout: Duration) -> String {
    format!(
        "SELECT MASTER_GTID_WAIT('{target}', {})",
        wait_seconds(timeout)
    )
}

/// The error MariaDB raises when the wait-or-fail prefix times out.
pub const WAIT_TIMEOUT_ERRNO: u16 = 1242;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::health::Observation;

    fn view(topology: TopologyKind) -> ClusterView {
        let nodes = ["n1", "n2", "n3"]
            .iter()
            .map(|n| NodeRef {
                node: NodeId::new(*n).unwrap(),
                addr: format!("{n}:3306"),
            })
            .collect();
        ClusterView::new(ClusterId::new("dc-a-c1").unwrap(), topology, nodes)
    }

    fn replica(t: &NodeTable, v: &ClusterView, n: &str, pos: &str) {
        let k = v.key(&NodeId::new(n).unwrap());
        t.record_pass(
            &k,
            Observation {
                role: NodeRole::Replica,
                ..Observation::default()
            },
        );
        t.record_positions(&k, Some(GtidSet::parse(pos).unwrap()), None, false);
    }

    #[test]
    fn primary_replica_routing() {
        let v = view(TopologyKind::PrimaryReplica);
        let t = NodeTable::new();
        let w = v.nodes[0].clone();
        let s = BackendSettings::default();
        let none = |_: &NodeKey| 0;
        // No replica observed yet: the writer.
        assert_eq!(
            choose(&v, Some(&w), &t, &none, &GtidSet::new(), &s, &[], 0),
            ReadTarget::Writer
        );
        // The writer's history makes lag 0 for caught-up replicas.
        t.record_positions(
            &v.key(&w.node),
            None,
            Some(GtidSet::parse("2-1-10").unwrap()),
            true,
        );
        replica(&t, &v, "n2", "2-1-10");
        replica(&t, &v, "n3", "2-1-9");
        let target = GtidSet::parse("2-1-10").unwrap();
        match choose(&v, Some(&w), &t, &none, &target, &s, &[], 0) {
            ReadTarget::Replica { node, wait_for } => {
                assert_eq!(node.node.as_str(), "n2");
                assert_eq!(wait_for, None);
            }
            other => panic!("{other:?}"),
        }
        // n2 excluded: n3 lags a little, so wait on it.
        let n2 = NodeId::new("n2").unwrap();
        match choose(&v, Some(&w), &t, &none, &target, &s, &[n2], 0) {
            ReadTarget::Replica { node, wait_for } => {
                assert_eq!(node.node.as_str(), "n3");
                assert_eq!(wait_for, Some(target.clone()));
            }
            other => panic!("{other:?}"),
        }
        let writer_only = BackendSettings {
            routing: Routing::Writer,
            ..BackendSettings::default()
        };
        assert_eq!(
            choose(&v, Some(&w), &t, &none, &target, &writer_only, &[], 0),
            ReadTarget::Writer
        );
        // A Down replica isn't chosen.
        let k2 = v.key(&NodeId::new("n2").unwrap());
        let k3 = v.key(&NodeId::new("n3").unwrap());
        for _ in 0..3 {
            t.record_failure(&k2, 3);
            t.record_failure(&k3, 3);
        }
        assert_eq!(
            choose(&v, Some(&w), &t, &none, &target, &s, &[], 0),
            ReadTarget::Writer
        );
    }

    #[test]
    fn galera_routing_and_sync_wait() {
        let v = view(TopologyKind::Galera);
        let t = NodeTable::new();
        let w = v.nodes[1].clone();
        let s = BackendSettings::default();
        let load = |k: &NodeKey| if k.node.as_str() == "n1" { 5 } else { 0 };
        match choose(&v, Some(&w), &t, &load, &GtidSet::new(), &s, &[], 0) {
            ReadTarget::GaleraNode { node, sync_wait } => {
                assert_eq!(node.node.as_str(), "n2", "least loaded");
                assert!(!sync_wait);
            }
            other => panic!("{other:?}"),
        }
        let target = GtidSet::parse("3-1-5").unwrap();
        let busy_writer = |k: &NodeKey| if k.node.as_str() == "n3" { 0 } else { 5 };
        match choose(&v, Some(&w), &t, &busy_writer, &target, &s, &[], 0) {
            ReadTarget::GaleraNode { node, sync_wait } => {
                assert_eq!(node.node.as_str(), "n3");
                assert!(sync_wait, "not the writer, target set");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn prefixes() {
        let g = GtidSet::parse("0-11-5402,1-12-77").unwrap();
        assert_eq!(
            wait_prefix(&g, Duration::from_millis(100)),
            "SET @__sprig_w = (SELECT CASE WHEN MASTER_GTID_WAIT('0-11-5402,1-12-77', 0.100) = 0 \
             THEN 1 ELSE (SELECT 1 FROM information_schema.ENGINES) END)"
        );
        assert_eq!(
            wait_query(&g, Duration::from_millis(2500)),
            "SELECT MASTER_GTID_WAIT('0-11-5402,1-12-77', 2.500)"
        );
        let w = Watermarks::new();
        let c = ClusterId::new("dc-a-c1").unwrap();
        w.advance(&c, "app", &g);
        w.advance(&c, "app", &GtidSet::parse("0-11-5403").unwrap());
        assert_eq!(w.get(&c, "app").to_string(), "0-11-5403,1-12-77");
        assert!(w.get(&c, "other").is_empty());
    }
}
