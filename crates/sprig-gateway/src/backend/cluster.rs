//! [`ClusterView`]: what the gateway needs from a cluster record and its
//! capability profile, and where views come from ([`ClusterSource`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sprig_cluster::driver::discover_nodes;
use sprig_cluster::profile::RywMethod;
use sprig_cluster::record::{AdminState, ClusterRecord, TopologyKind, WriterWeight};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, NodeId};
use sprig_common::{Result, SprigCode, SprigError};

/// One node of a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeRef {
    /// The node id, unique within its cluster.
    pub node: NodeId,
    /// `host:port`.
    pub addr: String,
}

/// A node, globally: its cluster and id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeKey {
    /// The cluster.
    pub cluster: ClusterId,
    /// The node.
    pub node: NodeId,
}

impl std::fmt::Display for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.cluster, self.node)
    }
}

/// The cached cluster record and capability profile.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterView {
    /// The cluster.
    pub id: ClusterId,
    /// Its topology.
    pub topology: TopologyKind,
    /// Its nodes, in record order (declared endpoints for external
    /// clusters: `writer`, then `reader-N`).
    pub nodes: Vec<NodeRef>,
    /// The Galera writer set; every node with weight 1 until the
    /// controller publishes one.
    pub galera_writers: Vec<WriterWeight>,
    /// How reads see the session's writes, from the profile (or the
    /// topology's usual method before a profile is published).
    pub ryw: RywMethod,
    /// Primary + replicas: the writer the controller publishes (its writer
    /// lease), as last read.
    pub published: Published,
    /// The record's `sprig_ctl` secret reference, which also locates the
    /// `sprig_gw` monitoring account's credential the checks log in with
    /// (`sprig_provision::gw_account`); `None` for views built without a
    /// record.
    pub monitor_secret: Option<String>,
}

/// The writer of a primary + replicas cluster as the controller publishes
/// it: `leases/writer/<cluster>`, else the `cluster_failover` record's
/// writer, else the record's registered writer (its first node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    /// What was read.
    pub writer: PublishedWriter,
    /// When it was read (a view served from the cache while the store is
    /// unreachable keeps its old time).
    pub read_at: Instant,
}

/// See [`Published`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishedWriter {
    /// The node holding the writer lease, or the one the last failover
    /// recorded, with its writer epoch.
    Node {
        /// The writer.
        node: NodeId,
        /// Its `writer_epoch`.
        epoch: u64,
    },
    /// Nothing published (no failover agent, no failover yet): the
    /// record's registered writer.
    Registered,
    /// A failover is handing the lease to a promoted node, or the lease
    /// and the failover record disagree: nobody may write.
    InTransit(String),
    /// Not read (a view built without the store: tests, static clusters).
    Unread,
}

impl Default for Published {
    fn default() -> Self {
        Self {
            writer: PublishedWriter::Unread,
            read_at: Instant::now(),
        }
    }
}

/// The `cluster_failover` record's fields the gateway reads
/// (`/sprig/dc/<dc>/cluster-failover/<cluster>`, written by the
/// controller's failover loop).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct FailoverWriter {
    #[serde(default)]
    writer: Option<String>,
    #[serde(default)]
    writer_epoch: u64,
}

impl sprig_meta::Versioned for FailoverWriter {
    const V: u32 = 1;
}

/// Reads the published writer of `cluster` from the DC tier.
///
/// # Errors
/// The store's errors.
pub async fn read_published(
    store: &dyn sprig_meta::MetaStore,
    cluster: &ClusterId,
) -> Result<PublishedWriter> {
    use sprig_cluster::failover::writer::{WriterLease, writer_lease_key};
    use sprig_meta::{Consistency, Record};
    let lease = match store
        .get(
            &writer_lease_key(store.tier(), cluster)?,
            Consistency::Serializable,
        )
        .await?
    {
        Some(kv) => Some(Record::<WriterLease>::decode(&kv.value)?.body),
        None => None,
    };
    let recorded = match store
        .get(
            &store.tier().key(&format!("cluster-failover/{cluster}"))?,
            Consistency::Serializable,
        )
        .await?
    {
        Some(kv) => Some(Record::<FailoverWriter>::decode(&kv.value)?.body),
        None => None,
    };
    Ok(published_writer(lease.as_ref(), recorded.as_ref()))
}

fn published_writer(
    lease: Option<&sprig_cluster::failover::writer::WriterLease>,
    recorded: Option<&FailoverWriter>,
) -> PublishedWriter {
    let recorded_epoch = recorded.map_or(0, |r| r.writer_epoch);
    match (lease, recorded) {
        (Some(l), _) if l.claimed_by_controller() => {
            PublishedWriter::InTransit(format!("a failover is handing the writer to {}", l.node_id))
        }
        (Some(l), _) if l.writer_epoch < recorded_epoch => PublishedWriter::InTransit(format!(
            "the writer lease (epoch {}) is older than the last failover (epoch {recorded_epoch})",
            l.writer_epoch
        )),
        (Some(l), _) => match NodeId::new(&l.node_id) {
            Ok(node) => PublishedWriter::Node {
                node,
                epoch: l.writer_epoch,
            },
            Err(_) => PublishedWriter::InTransit(format!("a lease names node {:?}", l.node_id)),
        },
        (
            None,
            Some(FailoverWriter {
                writer: Some(w),
                writer_epoch,
            }),
        ) => match NodeId::new(w) {
            Ok(node) => PublishedWriter::Node {
                node,
                epoch: *writer_epoch,
            },
            Err(_) => PublishedWriter::InTransit(format!("the failover record names {w:?}")),
        },
        (None, _) => PublishedWriter::Registered,
    }
}

impl ClusterView {
    /// The view of a record.
    #[must_use]
    pub fn from_record(rec: &ClusterRecord) -> Self {
        let nodes: Vec<NodeRef> = discover_nodes(rec)
            .into_iter()
            .map(|n| NodeRef {
                node: n.node,
                addr: n.addr,
            })
            .collect();
        let galera_writers = match &rec.galera {
            Some(g) if !g.writers.is_empty() => g.writers.clone(),
            _ => nodes
                .iter()
                .map(|n| WriterWeight {
                    node: n.node.clone(),
                    weight: 1.0,
                })
                .collect(),
        };
        let ryw = rec.profile.as_ref().map_or(
            match rec.topology {
                TopologyKind::Galera => RywMethod::WsrepSyncWait,
                TopologyKind::PrimaryReplica => RywMethod::GtidWait,
                _ => RywMethod::WriterOnly,
            },
            |p| p.derived.ryw,
        );
        Self {
            id: rec.cluster_id.clone(),
            topology: rec.topology,
            nodes,
            galera_writers,
            ryw,
            published: Published::default(),
            monitor_secret: Some(rec.secrets.ctl.clone()),
        }
    }

    /// A view built directly (tests and tools).
    #[must_use]
    pub fn new(id: ClusterId, topology: TopologyKind, nodes: Vec<NodeRef>) -> Self {
        let galera_writers = nodes
            .iter()
            .map(|n| WriterWeight {
                node: n.node.clone(),
                weight: 1.0,
            })
            .collect();
        let ryw = match topology {
            TopologyKind::Galera => RywMethod::WsrepSyncWait,
            TopologyKind::PrimaryReplica => RywMethod::GtidWait,
            _ => RywMethod::WriterOnly,
        };
        Self {
            id,
            topology,
            nodes,
            galera_writers,
            ryw,
            published: Published::default(),
            monitor_secret: None,
        }
    }

    /// The node `id`.
    #[must_use]
    pub fn node(&self, id: &NodeId) -> Option<&NodeRef> {
        self.nodes.iter().find(|n| &n.node == id)
    }

    /// The global key of node `id`.
    #[must_use]
    pub fn key(&self, id: &NodeId) -> NodeKey {
        NodeKey {
            cluster: self.id.clone(),
            node: id.clone(),
        }
    }
}

/// Where cluster views come from.
#[async_trait]
pub trait ClusterSource: Send + Sync {
    /// The view of `id`.
    ///
    /// # Errors
    /// 39104 for an unknown cluster, 39100 for one not in service, 39500
    /// if the registry is unreachable and nothing is cached.
    async fn view(&self, id: &ClusterId) -> Result<Arc<ClusterView>>;
}

/// Fixed views: tests and tools.
#[derive(Debug, Default)]
pub struct StaticClusters {
    views: RwLock<HashMap<ClusterId, Arc<ClusterView>>>,
}

impl StaticClusters {
    /// No clusters.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Adds or replaces a view.
    pub fn insert(&self, view: ClusterView) {
        self.views
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(view.id.clone(), Arc::new(view));
    }
}

#[async_trait]
impl ClusterSource for StaticClusters {
    async fn view(&self, id: &ClusterId) -> Result<Arc<ClusterView>> {
        self.views
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
            .ok_or_else(|| SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {id}")))
    }
}

/// Views from the DC tier's cluster registry, cached for `ttl`; a stale
/// view serves while the registry is unreachable.
pub struct RegistryClusters {
    registry: ClusterRegistry,
    /// The DC tier, where the writer lease and the failover record live.
    leases: Option<Arc<dyn sprig_meta::MetaStore>>,
    ttl: Duration,
    cache: Mutex<HashMap<ClusterId, (Instant, Arc<ClusterView>)>>,
}

impl std::fmt::Debug for RegistryClusters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryClusters")
            .field("ttl", &self.ttl)
            .field("leases", &self.leases.is_some())
            .finish_non_exhaustive()
    }
}

impl RegistryClusters {
    /// Views from `registry`, refreshed after `ttl`.
    #[must_use]
    pub fn new(registry: ClusterRegistry, ttl: Duration) -> Arc<Self> {
        Self::with_leases(registry, None, ttl)
    }

    /// Views that also carry each primary + replicas cluster's published
    /// writer, read from `dc`.
    #[must_use]
    pub fn with_leases(
        registry: ClusterRegistry,
        dc: Option<Arc<dyn sprig_meta::MetaStore>>,
        ttl: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            leases: dc,
            ttl,
            cache: Mutex::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl ClusterSource for RegistryClusters {
    async fn view(&self, id: &ClusterId) -> Result<Arc<ClusterView>> {
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned();
        if let Some((at, v)) = &cached
            && at.elapsed() < self.ttl
        {
            return Ok(Arc::clone(v));
        }
        match self.registry.get(id).await {
            Ok(Some(stored)) => {
                let rec = stored.body();
                if matches!(
                    rec.admin_state,
                    AdminState::Rejected | AdminState::Drained | AdminState::Retired
                ) {
                    return Err(SprigError::new(
                        SprigCode::ClusterUnavailable,
                        format!("cluster {id} is {}", rec.admin_state),
                    ));
                }
                let mut view = ClusterView::from_record(rec);
                if rec.topology == TopologyKind::PrimaryReplica
                    && let Some(store) = &self.leases
                {
                    match read_published(&**store, id).await {
                        Ok(w) => {
                            view.published = Published {
                                writer: w,
                                read_at: Instant::now(),
                            };
                        }
                        Err(e) => {
                            // Keep what was read before, with its age: the
                            // writer selection refuses a stale one.
                            tracing::warn!(cluster = %id, error = %e, "the published writer can't be read; keeping the last one");
                            if let Some((_, old)) = &cached {
                                view.published = old.published.clone();
                            } else {
                                view.published = Published {
                                    writer: PublishedWriter::InTransit(format!(
                                        "the writer lease can't be read: {e}"
                                    )),
                                    read_at: Instant::now(),
                                };
                            }
                        }
                    }
                }
                let v = Arc::new(view);
                self.cache
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id.clone(), (Instant::now(), Arc::clone(&v)));
                Ok(v)
            }
            Ok(None) => Err(SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {id} in this data center"),
            )),
            Err(e) => match cached {
                Some((_, v)) => {
                    tracing::warn!(cluster = %id, error = %e, "cluster registry unreachable; using the cached record");
                    Ok(v)
                }
                None => Err(e),
            },
        }
    }
}

#[cfg(test)]
mod published_tests {
    use super::*;
    use sprig_cluster::failover::writer::WriterLease;

    fn lease(node: &str, agent: &str, epoch: u64) -> WriterLease {
        WriterLease {
            node_id: node.into(),
            agent: agent.into(),
            writer_epoch: epoch,
        }
    }

    #[test]
    fn the_lease_then_the_failover_record_then_the_registered_writer() {
        let rec = |w: Option<&str>, e| FailoverWriter {
            writer: w.map(str::to_owned),
            writer_epoch: e,
        };
        assert_eq!(published_writer(None, None), PublishedWriter::Registered);
        assert_eq!(
            published_writer(None, Some(&rec(None, 0))),
            PublishedWriter::Registered
        );
        assert_eq!(
            published_writer(None, Some(&rec(Some("n2"), 3))),
            PublishedWriter::Node {
                node: NodeId::new("n2").unwrap(),
                epoch: 3
            }
        );
        assert_eq!(
            published_writer(Some(&lease("n3", "agent-n3", 4)), Some(&rec(Some("n2"), 3))),
            PublishedWriter::Node {
                node: NodeId::new("n3").unwrap(),
                epoch: 4
            }
        );
        // A promotion in progress, and a lease behind the last failover.
        assert!(matches!(
            published_writer(Some(&lease("n2", "controller:ctl-1", 4)), None),
            PublishedWriter::InTransit(_)
        ));
        assert!(matches!(
            published_writer(Some(&lease("n1", "agent-n1", 2)), Some(&rec(Some("n2"), 3))),
            PublishedWriter::InTransit(_)
        ));
    }
}
