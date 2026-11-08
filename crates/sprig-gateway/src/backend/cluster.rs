//! [`ClusterView`]: what the gateway needs from a cluster record and its
//! capability profile (chapter 06, "Interfaces"), and where views come
//! from ([`ClusterSource`]).

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
#[derive(Debug)]
pub struct RegistryClusters {
    registry: ClusterRegistry,
    ttl: Duration,
    cache: Mutex<HashMap<ClusterId, (Instant, Arc<ClusterView>)>>,
}

impl RegistryClusters {
    /// Views from `registry`, refreshed after `ttl`.
    #[must_use]
    pub fn new(registry: ClusterRegistry, ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            registry,
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
                let v = Arc::new(ClusterView::from_record(rec));
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
