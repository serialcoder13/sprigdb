//! Writer selection per topology (chapter 06, "Writer selection"; build
//! step 5).
//!
//! | Topology | Writer for shard `S` |
//! |---|---|
//! | Galera | the usable node of the published writer set with the highest weighted rendezvous score for `S` (chapter 09's `choose_writer`) |
//! | Primary and replicas | the node this gateway's checks see writable and replicating from nobody; the current one while it stays so |
//! | Single node | the node |
//! | External | the declared writer endpoint |
//!
//! Interim (M0.2): chapter 06 names the primary holding
//! `leases/writer/<cluster_id>`; that lease arrives with failover
//! (chapter 25, R2), so the gateway's own health checks decide. Before the
//! first check the first node of the record (the writer by registration
//! convention) is used.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_cluster::driver::galera::choose_writer;
use sprig_cluster::record::{TopologyKind, WriterWeight};
use sprig_common::ids::{ClusterId, NodeId, ShardId};
use sprig_common::{Result, SprigCode, SprigError};

use super::cluster::{ClusterView, NodeRef};
use super::health::{NodeRole, NodeTable};

/// Chooses a shard's writer (chapter 06, "Interfaces").
pub trait WriterSelector: Send + Sync {
    /// The writer of `shard` on `cluster`.
    ///
    /// # Errors
    /// 39100 if no node qualifies.
    fn writer(&self, cluster: &ClusterView, shard: ShardId) -> Result<NodeRef>;
}

/// The topology rules above, over the gateway's node table.
#[derive(Debug)]
pub struct TopologyWriters {
    nodes: Arc<NodeTable>,
    current: Mutex<HashMap<(ClusterId, ShardId), NodeId>>,
}

impl TopologyWriters {
    /// Writers judged from `nodes`.
    #[must_use]
    pub fn new(nodes: Arc<NodeTable>) -> Self {
        Self {
            nodes,
            current: Mutex::new(HashMap::new()),
        }
    }

    fn remember(&self, view: &ClusterView, shard: ShardId, node: &NodeRef) {
        let mut cur = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        let prev = cur.insert((view.id.clone(), shard), node.node.clone());
        if prev.as_ref().is_some_and(|p| p != &node.node) {
            tracing::info!(
                event = "backend.writer_changed",
                cluster = %view.id,
                shard = shard.0,
                from = %prev.as_ref().map(NodeId::as_str).unwrap_or_default(),
                to = %node.node,
                "writer changed"
            );
            metrics::counter!("sprig_backend_writer_changes_total", "cluster" => view.id.to_string())
                .increment(1);
        }
    }

    fn current(&self, view: &ClusterView, shard: ShardId) -> Option<NodeId> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(view.id.clone(), shard))
            .cloned()
    }
}

fn no_writer(view: &ClusterView) -> SprigError {
    SprigError::new(
        SprigCode::ClusterUnavailable,
        format!("cluster {} has no usable writer", view.id),
    )
}

impl WriterSelector for TopologyWriters {
    fn writer(&self, view: &ClusterView, shard: ShardId) -> Result<NodeRef> {
        let usable = |n: &NodeRef| self.nodes.status(&view.key(&n.node)).usable();
        let chosen = match view.topology {
            TopologyKind::Galera => {
                let set: Vec<WriterWeight> = view
                    .galera_writers
                    .iter()
                    .filter(|w| view.node(&w.node).is_some_and(usable))
                    .cloned()
                    .collect();
                choose_writer(shard, &set).and_then(|w| view.node(&w.node).cloned())
            }
            TopologyKind::PrimaryReplica => {
                let seen_writers: Vec<&NodeRef> = view
                    .nodes
                    .iter()
                    .filter(|n| {
                        let s = self.nodes.status(&view.key(&n.node));
                        s.usable() && s.role() == NodeRole::Writer
                    })
                    .collect();
                let current = self.current(view, shard);
                if let Some(c) = seen_writers
                    .iter()
                    .find(|n| Some(&n.node) == current.as_ref())
                {
                    Some((*c).clone())
                } else if let Some(first) = seen_writers.first() {
                    Some((*first).clone())
                } else {
                    // Nothing seen writable yet: the current writer, or the
                    // first node not known to be a replica.
                    view.nodes
                        .iter()
                        .find(|n| Some(&n.node) == current.as_ref() && usable(n))
                        .or_else(|| {
                            view.nodes.iter().find(|n| {
                                let s = self.nodes.status(&view.key(&n.node));
                                s.usable() && s.role() == NodeRole::Unknown
                            })
                        })
                        .cloned()
                }
            }
            TopologyKind::SingleNode => view.nodes.first().filter(|n| usable(n)).cloned(),
            TopologyKind::External => view
                .nodes
                .iter()
                .find(|n| n.node.as_str() == "writer")
                .or_else(|| view.nodes.first())
                .filter(|n| usable(n))
                .cloned(),
        };
        let node = chosen.ok_or_else(|| no_writer(view))?;
        self.remember(view, shard, &node);
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::health::Observation;

    fn view(t: TopologyKind) -> ClusterView {
        ClusterView::new(
            ClusterId::new("dc-a-c1").unwrap(),
            t,
            ["n1", "n2", "n3"]
                .iter()
                .map(|n| NodeRef {
                    node: NodeId::new(*n).unwrap(),
                    addr: format!("{n}:3306"),
                })
                .collect(),
        )
    }

    fn seen(t: &NodeTable, v: &ClusterView, n: &str, role: NodeRole) {
        t.record_pass(
            &v.key(&NodeId::new(n).unwrap()),
            Observation {
                role,
                ..Observation::default()
            },
        );
    }

    #[test]
    fn primary_replica_follows_the_checks() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        let v = view(TopologyKind::PrimaryReplica);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n1");
        seen(&t, &v, "n1", NodeRole::Replica);
        seen(&t, &v, "n2", NodeRole::Writer);
        seen(&t, &v, "n3", NodeRole::Replica);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n2");
        let k2 = v.key(&NodeId::new("n2").unwrap());
        for _ in 0..3 {
            t.record_failure(&k2, 3);
        }
        assert!(w.writer(&v, ShardId(0)).is_err(), "only replicas left");
    }

    #[test]
    fn galera_is_rendezvous_over_usable_nodes() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        let v = view(TopologyKind::Galera);
        let first = w.writer(&v, ShardId(7)).unwrap();
        assert_eq!(w.writer(&v, ShardId(7)).unwrap(), first, "stable");
        let k = v.key(&first.node);
        for _ in 0..3 {
            t.record_failure(&k, 3);
        }
        let second = w.writer(&v, ShardId(7)).unwrap();
        assert_ne!(second, first);
        // Shards spread over nodes.
        let distinct: std::collections::HashSet<_> = (0..50)
            .map(|s| w.writer(&v, ShardId(s)).unwrap().node)
            .collect();
        assert_eq!(distinct.len(), 2);
    }

    #[test]
    fn single_and_external() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        assert_eq!(
            w.writer(&view(TopologyKind::SingleNode), ShardId(0))
                .unwrap()
                .node
                .as_str(),
            "n1"
        );
        let mut ext = view(TopologyKind::External);
        ext.nodes[2].node = NodeId::new("writer").unwrap();
        assert_eq!(w.writer(&ext, ShardId(0)).unwrap().node.as_str(), "writer");
    }
}
