//! Topology drivers: one per topology, each knowing how to operate it
//! (chapter 09, "Interfaces"; build steps 5 and 6).
//!
//! A driver answers, from the latest probes and the published record:
//! which node writes a shard, which nodes may serve its reads and how they
//! see the session's own writes, where binlogs are read, where index
//! validation runs, how DDL runs, and how a shard is fenced.
//!
//! The controller feeds a driver: [`TopologyDriver::observe`] with each
//! record it publishes, [`TopologyDriver::health`] with each light probe
//! and [`TopologyDriver::derive`] with each full probe. Everything else
//! reads that state; no method holds a lock across an `.await`.
//!
//! - [`BasicDriver`]: single node, primary + replicas, and external.
//! - [`galera::GaleraDriver`]: Galera with the rendezvous writer choice.

mod basic;
pub mod galera;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{NodeId, ShardId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};

pub use basic::BasicDriver;
pub use galera::GaleraDriver;

use crate::fence::{FenceOptions, FenceReason, FenceReceipt, FenceRequest};
use crate::health::{ClusterHealth, HealthReport, HealthTracker, NodeState, ObservedRole};
use crate::probe::NodeProbe;
use crate::probe::feed::{self, FeedLevel};
use crate::profile::{
    CapabilityProfile, DeriveInput, FenceMethod, RywMethod, ValidationMethod, derive,
};
use crate::record::{ClusterRecord, Declared, NodeSpec, TopologyKind};
use crate::settings::ClusterSettings;
use crate::sql::NodeEndpoint;

/// Where to send a shard's writes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint {
    /// The node.
    pub node: NodeId,
    /// `host:port`.
    pub addr: String,
}

/// A node that may serve a shard's reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadEndpoint {
    /// The node.
    pub endpoint: Endpoint,
    /// How a read here sees the session's own writes.
    pub ryw: RywMethod,
    /// Replication lag, when measured.
    pub lag_ms: Option<u64>,
}

/// Which node the change feed or the mover prefers to read binlogs from
/// (`feed.source`, chapter 38).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePref {
    /// The writer.
    Writer,
    /// The best replica (fails without one).
    Replica,
    /// Galera: any synced node; zero-loss clusters: the writer; else the
    /// best replica, falling back to the writer.
    Auto,
}

/// A node whose binlog can be read (chapter 10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinlogSource {
    /// The node.
    pub node: NodeId,
    /// `host:port`.
    pub addr: String,
    /// The GTID domain its own writes use.
    pub gtid_domain_id: u32,
    /// Readers can switch to another node at the same position (Galera
    /// with `wsrep_gtid_mode` and a shared domain).
    pub switchable: bool,
    /// What the feed can do with this node's binlog.
    pub feed_level: FeedLevel,
}

/// The node automatic-index validation runs on (chapter 13).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationNode {
    /// The node.
    pub node: NodeId,
    /// `host:port`.
    pub addr: String,
    /// How it is taken out of service.
    pub method: ValidationMethod,
}

/// A schema change, as far as choosing its method needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DdlKind {
    /// `ADD INDEX`.
    AddIndex,
    /// `DROP INDEX`.
    DropIndex,
    /// `ALTER INDEX ... [NOT] IGNORED`.
    ToggleIgnoredIndex,
    /// `ADD COLUMN`.
    AddColumn,
    /// `DROP COLUMN`.
    DropColumn,
    /// Anything else that rebuilds the table.
    Rebuild,
}

/// What a table has that changes how DDL on it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct TableTraits {
    /// The new index is spatial.
    pub spatial: bool,
    /// The new index is full-text (never built online by `InnoDB`).
    pub fulltext: bool,
    /// The new index covers a virtual generated column.
    pub virtual_column: bool,
    /// The table has foreign keys.
    pub foreign_keys: bool,
}

/// How a schema change runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DdlMethod {
    /// `ALGORITHM=INPLACE, LOCK=NONE` (or `INSTANT`).
    Online,
    /// The mover's copy-and-swap (chapter 10).
    CopySwap,
    /// Galera total order isolation, for metadata-only changes.
    Toi,
}

/// Failover operations of one topology (chapter 25, build step 13).
/// Chapter 25 adds the methods; no driver returns hooks yet.
pub trait FailoverHooks: Send + Sync {}

/// Turns a node of the record into a connectable endpoint, resolving the
/// `sprig_ctl` secret. Only controllers and workers have one.
pub trait NodeConnector: Send + Sync + fmt::Debug {
    /// The endpoint of `node`.
    ///
    /// # Errors
    /// The secret couldn't be resolved or the address is invalid.
    fn endpoint(&self, node: &NodeSpec) -> Result<NodeEndpoint>;
}

/// A connector over fixed endpoints, by node id (tests and tools).
#[derive(Debug, Clone, Default)]
pub struct StaticConnector {
    endpoints: HashMap<NodeId, NodeEndpoint>,
}

impl StaticConnector {
    /// A connector over `endpoints`.
    #[must_use]
    pub fn new(endpoints: impl IntoIterator<Item = NodeEndpoint>) -> Self {
        Self {
            endpoints: endpoints.into_iter().map(|e| (e.node.clone(), e)).collect(),
        }
    }
}

impl NodeConnector for StaticConnector {
    fn endpoint(&self, node: &NodeSpec) -> Result<NodeEndpoint> {
        self.endpoints
            .get(&node.node)
            .cloned()
            .ok_or_else(|| SprigError::internal(format!("no endpoint for node {}", node.node)))
    }
}

/// The topology driver (chapter 09, "Interfaces").
#[async_trait::async_trait]
pub trait TopologyDriver: Send + Sync {
    /// The topology this driver operates.
    fn kind(&self) -> TopologyKind;
    /// Takes in a published record: nodes, declarations, the Galera writer
    /// set and the capability profile.
    fn observe(&self, rec: &ClusterRecord);
    /// The nodes to probe.
    async fn discover(&self, rec: &ClusterRecord) -> Result<Vec<NodeSpec>>;
    /// Derives a capability profile from one probe pass.
    fn derive(&self, probes: &[NodeProbe], declared: Option<&Declared>) -> CapabilityProfile;
    /// Assesses one probe pass; also updates the writer and readers.
    fn health(&self, probes: &[NodeProbe], profile: &CapabilityProfile) -> ClusterHealth;
    /// The shard's writer.
    ///
    /// # Errors
    /// 39100 if there is no writer.
    fn writer(&self, shard: ShardId) -> Result<Endpoint>;
    /// Nodes that may serve the shard's reads, best first.
    fn readers(&self, shard: ShardId) -> Vec<ReadEndpoint>;
    /// Where to read binlogs from (chapters 10 and 38).
    ///
    /// # Errors
    /// 39102 if no node qualifies.
    fn binlog_source(&self, pref: SourcePref) -> Result<BinlogSource>;
    /// Where index validation runs (chapter 13).
    fn validation_node(&self) -> Option<ValidationNode>;
    /// How a schema change on `table` runs here.
    fn ddl_method(&self, ddl: DdlKind, table: &TableTraits) -> DdlMethod;
    /// Fences one shard.
    ///
    /// # Errors
    /// 39102 if neither method is available, 39100 without a writer, 39103
    /// if the fence couldn't finish (and was undone).
    async fn fence_shard(&self, req: &FenceRequest) -> Result<FenceReceipt>;
    /// Undoes a fence exactly.
    ///
    /// # Errors
    /// A statement failed; what could be restored was.
    async fn unfence_shard(&self, receipt: &FenceReceipt) -> Result<()>;
    /// Fences the whole cluster with `read_only` (managed clusters, R2).
    ///
    /// # Errors
    /// 39102 until R2.
    async fn fence_cluster(&self, reason: FenceReason) -> Result<()>;
    /// Failover hooks (chapter 25), where the topology has them.
    fn failover(&self) -> Option<&dyn FailoverHooks>;
}

/// A driver for `kind`.
#[must_use]
pub fn for_topology(
    kind: TopologyKind,
    connector: Arc<dyn NodeConnector>,
    settings: ClusterSettings,
) -> Arc<dyn TopologyDriver> {
    match kind {
        TopologyKind::Galera => Arc::new(GaleraDriver::new(connector, settings)),
        other => Arc::new(BasicDriver::new(other, connector, settings)),
    }
}

/// What a driver last saw.
#[derive(Debug, Default)]
pub(crate) struct State {
    pub(crate) record: Option<ClusterRecord>,
    pub(crate) probes: Vec<NodeProbe>,
    pub(crate) report: Option<HealthReport>,
    pub(crate) profile: Option<CapabilityProfile>,
    /// The chosen writer (primary + replicas, external, single node).
    pub(crate) writer: Option<NodeId>,
    /// The last deep probe's active results (chapter 09: XA, features,
    /// privileges only a deep probe tests), carried into the passes in
    /// between so a held upgrade can sustain.
    pub(crate) deep: Option<crate::probe::ActiveProbes>,
}

/// The state and code every driver shares.
pub(crate) struct Core {
    pub(crate) kind: TopologyKind,
    connector: Arc<dyn NodeConnector>,
    settings: ClusterSettings,
    state: RwLock<State>,
    tracker: Mutex<HealthTracker>,
}

impl fmt::Debug for Core {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Core")
            .field("kind", &self.kind)
            .field("connector", &self.connector)
            .finish_non_exhaustive()
    }
}

/// `probes` with the last deep probe's active results attached to the
/// node active probes run on, when this pass ran none; `None` when the
/// pass ran them (they are remembered) or nothing was seen yet.
fn carry_deep(core: &Core, probes: &[NodeProbe]) -> Option<Vec<NodeProbe>> {
    if let Some(a) = probes.iter().find_map(|p| p.active.clone()) {
        core.write(|s| s.deep = Some(a));
        return None;
    }
    let a = core.read(|s| s.deep.clone())?;
    let i = crate::probe::active_target(core.kind, probes)?;
    let mut v = probes.to_vec();
    v[i].active = Some(a);
    Some(v)
}

/// 39100.
pub(crate) fn unavailable(what: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ClusterUnavailable, what.into())
}

/// 39102.
pub(crate) fn capability(what: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ClusterCapabilityRequired, what.into())
}

impl Core {
    pub(crate) fn new(
        kind: TopologyKind,
        connector: Arc<dyn NodeConnector>,
        settings: ClusterSettings,
    ) -> Self {
        Self {
            kind,
            connector,
            settings,
            state: RwLock::new(State::default()),
            tracker: Mutex::new(HealthTracker::new()),
        }
    }

    pub(crate) fn settings(&self) -> &ClusterSettings {
        &self.settings
    }

    /// Reads the state. A poisoned lock still holds consistent data: every
    /// writer replaces whole fields.
    pub(crate) fn read<T>(&self, f: impl FnOnce(&State) -> T) -> T {
        f(&self.state.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn write<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.state.write().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn observe(&self, rec: &ClusterRecord) {
        self.write(|s| {
            if let Some(p) = &rec.profile {
                s.profile = Some(p.clone());
            }
            s.record = Some(rec.clone());
        });
    }

    pub(crate) fn derive(
        &self,
        probes: &[NodeProbe],
        declared: Option<&Declared>,
    ) -> CapabilityProfile {
        let previous = self.read(|s| s.profile.clone());
        // Deep-only facts come from the last deep probe, not from the
        // published profile: an upgrade the publisher holds (a sustain
        // period) would otherwise be undone by the next light pass and
        // never publish (the dev clusters' XA stayed `false` that way).
        let carried = carry_deep(self, probes);
        let p = derive(DeriveInput {
            topology: self.kind,
            probes: carried.as_deref().unwrap_or(probes),
            declared,
            previous: previous.as_ref(),
        });
        self.write(|s| s.probes = probes.to_vec());
        p
    }

    /// Runs the health model; returns the report.
    pub(crate) fn assess(&self, probes: &[NodeProbe], profile: &CapabilityProfile) -> HealthReport {
        let report = self
            .tracker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(self.kind, probes, &self.settings);
        self.write(|s| {
            s.probes = probes.to_vec();
            s.profile = Some(profile.clone());
            s.report = Some(report.clone());
        });
        report
    }

    /// The record's node spec for `node`.
    pub(crate) fn spec(&self, node: &NodeId) -> Option<NodeSpec> {
        self.read(|s| {
            s.record
                .as_ref()
                .and_then(|r| r.nodes.iter().find(|n| &n.node == node).cloned())
                .or_else(|| {
                    // Declared endpoints of an external cluster.
                    s.record
                        .as_ref()
                        .and_then(|r| discover_nodes(r).into_iter().find(|n| &n.node == node))
                })
        })
    }

    pub(crate) fn endpoint_of(&self, node: &NodeId) -> Result<Endpoint> {
        self.spec(node)
            .map(|n| Endpoint {
                node: n.node,
                addr: n.addr,
            })
            .ok_or_else(|| unavailable(format!("node {node} is not in the cluster record")))
    }

    pub(crate) fn connect_spec(&self, node: &NodeId) -> Result<NodeEndpoint> {
        let spec = self
            .spec(node)
            .ok_or_else(|| unavailable(format!("node {node} is not in the cluster record")))?;
        self.connector.endpoint(&spec)
    }

    /// Nodes whose last probe answered and whose state isn't `broken` or
    /// `unreachable`, with the role they were seen in.
    pub(crate) fn live_nodes(&self) -> Vec<(NodeId, ObservedRole, NodeState, Option<u64>)> {
        self.read(|s| {
            s.report.as_ref().map_or_else(Vec::new, |r| {
                r.nodes
                    .iter()
                    .filter(|n| !matches!(n.state, NodeState::Broken | NodeState::Unreachable))
                    .filter(|n| n.role != ObservedRole::Unknown)
                    .map(|n| (n.node.clone(), n.role, n.state, n.lag_ms))
                    .collect()
            })
        })
    }

    pub(crate) fn profile(&self) -> Option<CapabilityProfile> {
        self.read(|s| s.profile.clone())
    }

    /// Binlog candidates: answering nodes whose settings qualify, with
    /// their feed level and the role and lag the last health pass saw.
    pub(crate) fn binlog_candidates(&self) -> Vec<(BinlogSource, ObservedRole, Option<u64>)> {
        let live = self.live_nodes();
        let switchable = self.read(|s| {
            s.profile
                .as_ref()
                .and_then(|p| p.galera.as_ref())
                .is_some_and(|g| g.gtid_mode && g.shared_gtid_domain)
        });
        self.read(|s| {
            s.probes
                .iter()
                .filter_map(|p| {
                    let f = p.facts()?;
                    let (_, role, state, lag) = live.iter().find(|(n, ..)| n == &p.node)?;
                    if *state != NodeState::Ok && *state != NodeState::Lagging {
                        return None;
                    }
                    let level = feed::check(f, feed::DEFAULT_RETENTION_HOURS).level;
                    if level == FeedLevel::Unsupported {
                        return None;
                    }
                    let addr = s
                        .record
                        .as_ref()
                        .and_then(|r| discover_nodes(r).into_iter().find(|n| n.node == p.node))
                        .map(|n| n.addr)?;
                    let domain = match &f.galera {
                        Some(g) if g.gtid_mode => {
                            g.gtid_domain_id.unwrap_or(f.binlog.gtid_domain_id)
                        }
                        _ => f.binlog.gtid_domain_id,
                    };
                    Some((
                        BinlogSource {
                            node: p.node.clone(),
                            addr,
                            gtid_domain_id: domain,
                            switchable: f.galera.is_some() && switchable,
                            feed_level: level,
                        },
                        *role,
                        *lag,
                    ))
                })
                .collect()
        })
    }

    /// Picks the fence method: the requested one if the profile allows it,
    /// else `Rename` (always available), as on external clusters where
    /// `KILL` isn't allowed.
    pub(crate) fn fence_method(&self, req: &FenceRequest) -> Result<FenceMethod> {
        let methods = self
            .profile()
            .map(|p| p.derived.fence_methods)
            .unwrap_or_default();
        if methods.contains(&req.method) {
            Ok(req.method)
        } else if methods.contains(&FenceMethod::Rename) {
            tracing::info!(requested = ?req.method, "fence method unavailable; renaming instead");
            Ok(FenceMethod::Rename)
        } else {
            Err(capability(
                "no fence method is available on this cluster (no profile yet?)",
            ))
        }
    }

    pub(crate) fn fence_options(&self) -> FenceOptions {
        let ps = self
            .profile()
            .is_some_and(|p| p.features.performance_schema);
        FenceOptions::from_settings(&self.settings, ps)
    }

    pub(crate) async fn fence_on(
        &self,
        req: &FenceRequest,
        writer: &NodeId,
        kill_on: &[NodeId],
    ) -> Result<FenceReceipt> {
        let method = self.fence_method(req)?;
        let req = FenceRequest {
            method,
            ..req.clone()
        };
        let w = self.connect_spec(writer)?;
        let kill: Vec<NodeEndpoint> = if method == FenceMethod::RevokeKill {
            kill_on
                .iter()
                .map(|n| self.connect_spec(n))
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };
        crate::fence::fence(&req, &w, &kill, &self.fence_options()).await
    }

    pub(crate) async fn unfence(&self, receipt: &FenceReceipt) -> Result<()> {
        let w = self.connect_spec(&receipt.writer)?;
        crate::fence::unfence(receipt, &w, &self.fence_options()).await
    }
}

/// The nodes of a record, plus declared endpoints of an external cluster
/// that aren't already listed: `writer`, then `reader-1`, `reader-2`...
#[must_use]
pub fn discover_nodes(rec: &ClusterRecord) -> Vec<NodeSpec> {
    let mut nodes = rec.nodes.clone();
    if rec.topology != TopologyKind::External {
        return nodes;
    }
    let Some(d) = &rec.declared else {
        return nodes;
    };
    let mut add = |name: String, addr: &str| {
        if nodes.iter().any(|n| n.addr == addr) {
            return;
        }
        if let Ok(node) = NodeId::new(name) {
            nodes.push(NodeSpec {
                node,
                addr: addr.to_owned(),
                zone: None,
            });
        }
    };
    if let Some(w) = &d.writer_endpoint {
        add("writer".to_owned(), w);
    }
    for (i, r) in d.reader_endpoints.iter().enumerate() {
        add(format!("reader-{}", i + 1), r);
    }
    nodes
}

/// The shard a fence is for, from its first schema; used to pick the
/// shard's writer.
pub(crate) fn fence_shard_id(req: &FenceRequest) -> ShardId {
    req.schemas
        .first()
        .and_then(|s| s.parse::<SchemaName>().ok())
        .and_then(|s| match s {
            SchemaName::Shard { shard, .. } => Some(shard),
            _ => None,
        })
        .unwrap_or(ShardId(0))
}

/// The method of a schema change, from the profile's features.
#[must_use]
pub fn ddl_method_for(
    galera: bool,
    profile: Option<&CapabilityProfile>,
    ddl: DdlKind,
    table: &TableTraits,
) -> DdlMethod {
    let features = profile.map(|p| p.features).unwrap_or_default();
    if galera {
        // TOI blocks the whole cluster for the statement: only
        // metadata-only changes run that way.
        return match ddl {
            DdlKind::DropIndex => DdlMethod::Toi,
            DdlKind::ToggleIgnoredIndex if features.instant_ignored_toggle => DdlMethod::Toi,
            _ => DdlMethod::CopySwap,
        };
    }
    let online_build = profile.is_some_and(|p| p.derived.online_index_build);
    match ddl {
        DdlKind::DropIndex
        | DdlKind::ToggleIgnoredIndex
        | DdlKind::AddColumn
        | DdlKind::DropColumn => DdlMethod::Online,
        DdlKind::AddIndex => {
            let ok = online_build
                && !table.fulltext
                && (!table.spatial || features.online_spatial_index)
                && (!table.virtual_column || features.vcol_index_online);
            if ok {
                DdlMethod::Online
            } else {
                DdlMethod::CopySwap
            }
        }
        DdlKind::Rebuild => DdlMethod::CopySwap,
    }
}

#[cfg(test)]
mod tests;
