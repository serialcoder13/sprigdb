//! Writer selection per topology (chapter 06, "Writer selection"; build
//! step 5).
//!
//! | Topology | Writer for shard `S` |
//! |---|---|
//! | Galera | the usable node of the published writer set with the highest weighted rendezvous score for `S` (chapter 09's `choose_writer`) |
//! | Primary and replicas | the node the controller publishes (chapter 25): the holder of `leases/writer/<cluster_id>`, else the writer the last failover recorded, else the record's registered writer (its first node); the gateway's checks may only veto it |
//! | Single node | the node |
//! | External | the declared writer endpoint |
//!
//! For primary + replicas the gateway never nominates a writer of its own:
//! a replica may be writable (`read_only = 0` on the dev clusters), and a
//! write there forks its history (the 9 November replica-1 incident: a
//! gateway whose checks couldn't read the replication status took a
//! replica that came back first for the writer). Writes are refused (39100,
//! `ER_SPRIG_CLUSTER_UNAVAILABLE`) while the published writer is down or
//! seen as a replica, while a failover hands the lease over, when the lease
//! and the failover record disagree, and when the last read of them is
//! older than the writer lease's TTL ([`PUBLISHED_MAX_AGE`]): the
//! controller's failover moves the lease, and the gateway follows it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_cluster::driver::galera::choose_writer;
use sprig_cluster::record::{TopologyKind, WriterWeight};
use sprig_common::ids::{ClusterId, NodeId, ShardId};
use sprig_common::{Result, SprigCode, SprigError};

use super::cluster::{ClusterView, NodeRef, PublishedWriter};
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
}

/// A published writer read longer ago than this isn't trusted
/// (`failover.writer_lease_seconds`' default, chapter 25): a failover may
/// have moved the lease since.
pub const PUBLISHED_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(10);

fn refused(view: &ClusterView, why: &str) -> SprigError {
    SprigError::new(
        SprigCode::ClusterUnavailable,
        format!("cluster {} has no usable writer: {why}", view.id),
    )
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
                let p = &view.published;
                let named = match &p.writer {
                    PublishedWriter::InTransit(why) => return Err(refused(view, why)),
                    _ if !matches!(p.writer, PublishedWriter::Unread)
                        && p.read_at.elapsed() > PUBLISHED_MAX_AGE =>
                    {
                        return Err(refused(
                            view,
                            "the published writer couldn't be read for longer than the writer \
                             lease's TTL",
                        ));
                    }
                    PublishedWriter::Node { node, .. } => view.node(node),
                    // Nothing published, or a view without the store: the
                    // registered writer.
                    PublishedWriter::Registered | PublishedWriter::Unread => view.nodes.first(),
                };
                let Some(n) = named else {
                    return Err(refused(
                        view,
                        "the published writer isn't in the cluster record",
                    ));
                };
                // The checks may only veto it.
                let s = self.nodes.status(&view.key(&n.node));
                if !s.usable() {
                    return Err(refused(view, &format!("its writer {} is down", n.node)));
                }
                if s.role() == NodeRole::Replica {
                    return Err(refused(
                        view,
                        &format!("its writer {} is read-only or replicating", n.node),
                    ));
                }
                Some(n.clone())
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
    use std::time::Duration;

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

    fn published(v: &mut ClusterView, w: PublishedWriter, age: std::time::Duration) {
        v.published = crate::backend::cluster::Published {
            writer: w,
            read_at: std::time::Instant::now()
                .checked_sub(age)
                .unwrap_or_else(std::time::Instant::now),
        };
    }

    fn down(t: &NodeTable, v: &ClusterView, n: &str) {
        let k = v.key(&NodeId::new(n).unwrap());
        for _ in 0..3 {
            t.record_failure(&k, 3);
        }
    }

    fn up(t: &NodeTable, v: &ClusterView, n: &str, role: NodeRole) {
        seen(t, v, n, role);
        seen(t, v, n, role);
    }

    fn node(n: &str) -> NodeId {
        NodeId::new(n).unwrap()
    }

    /// Before any lease is seen (no failover agents, or startup): the
    /// registered writer only, whatever the checks say of the others.
    #[test]
    fn before_a_lease_the_registered_writer_only() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        let mut v = view(TopologyKind::PrimaryReplica);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n1");
        published(&mut v, PublishedWriter::Registered, Duration::ZERO);
        up(&t, &v, "n2", NodeRole::Writer);
        up(&t, &v, "n3", NodeRole::Writer);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n1");
    }

    /// The 9 November replica-1 incident: every node down under load, then
    /// n2 and n3 back before n1, all looking writable. No write may go
    /// anywhere until n1, the registered writer, is back.
    #[test]
    fn the_incident_sequence_never_picks_a_replica() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        let mut v = view(TopologyKind::PrimaryReplica);
        published(&mut v, PublishedWriter::Registered, Duration::ZERO);
        for n in ["n1", "n2", "n3"] {
            up(&t, &v, n, NodeRole::Unknown);
        }
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n1");
        for n in ["n1", "n2", "n3"] {
            down(&t, &v, n);
        }
        up(&t, &v, "n2", NodeRole::Writer);
        up(&t, &v, "n3", NodeRole::Unknown);
        let e = w.writer(&v, ShardId(0)).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ClusterUnavailable), "{e}");
        up(&t, &v, "n1", NodeRole::Unknown);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n1");
    }

    /// A real failover moves the lease to n2: the gateway follows it, and
    /// does not go back to n1 when n1 answers again (it is stale).
    #[test]
    fn the_lease_is_followed_and_the_old_primary_not_taken_back() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        let mut v = view(TopologyKind::PrimaryReplica);
        published(
            &mut v,
            PublishedWriter::Node {
                node: node("n1"),
                epoch: 1,
            },
            Duration::ZERO,
        );
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n1");
        down(&t, &v, "n1");
        assert!(
            w.writer(&v, ShardId(0)).is_err(),
            "the lease still names n1"
        );
        published(
            &mut v,
            PublishedWriter::InTransit("a failover is handing the writer to n2".into()),
            Duration::ZERO,
        );
        assert!(
            w.writer(&v, ShardId(0)).is_err(),
            "no writes during the handover"
        );
        published(
            &mut v,
            PublishedWriter::Node {
                node: node("n2"),
                epoch: 2,
            },
            Duration::ZERO,
        );
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n2");
        up(&t, &v, "n1", NodeRole::Writer);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n2");
    }

    /// The checks only veto; an old reading of the lease isn't trusted.
    #[test]
    fn checks_veto_and_stale_readings_refuse() {
        let t = Arc::new(NodeTable::new());
        let w = TopologyWriters::new(Arc::clone(&t));
        let mut v = view(TopologyKind::PrimaryReplica);
        let n2 = PublishedWriter::Node {
            node: node("n2"),
            epoch: 4,
        };
        published(&mut v, n2.clone(), Duration::ZERO);
        up(&t, &v, "n2", NodeRole::Replica);
        assert!(
            w.writer(&v, ShardId(0)).is_err(),
            "published, but replicating"
        );
        up(&t, &v, "n2", NodeRole::Unknown);
        assert_eq!(w.writer(&v, ShardId(0)).unwrap().node.as_str(), "n2");
        published(&mut v, n2, PUBLISHED_MAX_AGE + Duration::from_secs(1));
        assert!(w.writer(&v, ShardId(0)).is_err(), "read too long ago");
        published(
            &mut v,
            PublishedWriter::Node {
                node: node("n9"),
                epoch: 5,
            },
            Duration::ZERO,
        );
        assert!(w.writer(&v, ShardId(0)).is_err(), "not in the record");
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
