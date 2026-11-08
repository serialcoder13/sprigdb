//! The drivers for a single node, a primary with replicas, and an external
//! (managed) cluster (chapter 09, build step 5). They differ only in which
//! nodes they expect, so one type serves all three.

use std::sync::Arc;

use sprig_common::Result;
use sprig_common::ids::{NodeId, ShardId};

use super::{
    BinlogSource, Core, DdlKind, DdlMethod, Endpoint, FailoverHooks, NodeConnector, ReadEndpoint,
    SourcePref, TableTraits, TopologyDriver, ValidationNode, capability, ddl_method_for,
    discover_nodes, unavailable,
};
use crate::fence::{FenceReason, FenceReceipt, FenceRequest, FenceScope};
use crate::health::{ClusterHealth, HealthReport, ObservedRole};
use crate::probe::NodeProbe;
use crate::profile::{CapabilityProfile, RywMethod, ValidationMethod};
use crate::record::{ClusterRecord, Declared, NodeSpec, TopologyKind};
use crate::settings::ClusterSettings;

/// The driver for [`TopologyKind::SingleNode`], [`TopologyKind::PrimaryReplica`]
/// and [`TopologyKind::External`].
#[derive(Debug)]
pub struct BasicDriver {
    pub(super) core: Core,
}

impl BasicDriver {
    /// A driver for `kind`, which must not be Galera.
    ///
    /// # Panics
    /// For [`TopologyKind::Galera`]: use [`GaleraDriver`](super::GaleraDriver).
    #[must_use]
    pub fn new(
        kind: TopologyKind,
        connector: Arc<dyn NodeConnector>,
        settings: ClusterSettings,
    ) -> Self {
        assert!(kind != TopologyKind::Galera, "use GaleraDriver for Galera");
        Self {
            core: Core::new(kind, connector, settings),
        }
    }

    /// The last health report.
    #[must_use]
    pub fn last_report(&self) -> Option<HealthReport> {
        self.core.read(|s| s.report.clone())
    }
}

/// Picks the writer among the writable nodes a health pass saw: the
/// current one while it stays writable; else the only one; else the one
/// most replicas replicate from. Two writable primaries leave the cluster
/// `degraded`; the unexpected one's shards are fenced by the caller.
fn choose_writer(
    current: Option<&NodeId>,
    writers: &[NodeId],
    probes: &[NodeProbe],
) -> Option<NodeId> {
    if let Some(c) = current
        && writers.contains(c)
    {
        return Some(c.clone());
    }
    match writers {
        [] => None,
        [only] => Some(only.clone()),
        many => {
            let server_id = |n: &NodeId| {
                probes
                    .iter()
                    .find(|p| &p.node == n)
                    .and_then(NodeProbe::facts)
                    .map(|f| f.server.server_id)
            };
            let followers = |id: Option<u32>| {
                probes
                    .iter()
                    .filter_map(NodeProbe::facts)
                    .filter(|f| f.channels.iter().any(|c| Some(c.master_server_id) == id))
                    .count()
            };
            many.iter()
                .max_by_key(|n| (followers(server_id(n)), std::cmp::Reverse((*n).clone())))
                .cloned()
        }
    }
}

#[async_trait::async_trait]
impl TopologyDriver for BasicDriver {
    fn kind(&self) -> TopologyKind {
        self.core.kind
    }

    fn observe(&self, rec: &ClusterRecord) {
        self.core.observe(rec);
    }

    async fn discover(&self, rec: &ClusterRecord) -> Result<Vec<NodeSpec>> {
        Ok(discover_nodes(rec))
    }

    fn derive(&self, probes: &[NodeProbe], declared: Option<&Declared>) -> CapabilityProfile {
        self.core.derive(probes, declared)
    }

    fn health(&self, probes: &[NodeProbe], profile: &CapabilityProfile) -> ClusterHealth {
        let report = self.core.assess(probes, profile);
        let mut health = report.health;
        self.core.write(|s| match report.health {
            // A writer we can't see may still be there: keep it.
            ClusterHealth::Unknown => {}
            ClusterHealth::NoWriter => s.writer = None,
            _ => s.writer = choose_writer(s.writer.as_ref(), &report.writers, probes),
        });
        // A verified fact overrides a declaration; a contradiction degrades.
        if self.core.kind == TopologyKind::External && health == ClusterHealth::Healthy {
            let declared_writer_is_replica = self.core.read(|s| {
                let declared = s
                    .record
                    .as_ref()
                    .and_then(|r| r.declared.as_ref())
                    .and_then(|d| d.writer_endpoint.clone());
                let Some(addr) = declared else { return false };
                let node = s.record.as_ref().and_then(|r| {
                    discover_nodes(r)
                        .into_iter()
                        .find(|n| n.addr == addr)
                        .map(|n| n.node)
                });
                report
                    .nodes
                    .iter()
                    .any(|n| Some(&n.node) == node.as_ref() && n.role == ObservedRole::Replica)
            });
            if declared_writer_is_replica {
                tracing::warn!("the declared writer endpoint is a replica");
                health = ClusterHealth::Degraded;
            }
        }
        health
    }

    fn writer(&self, _shard: ShardId) -> Result<Endpoint> {
        let w = self.core.read(|s| s.writer.clone());
        match w {
            Some(w) => self.core.endpoint_of(&w),
            None => Err(unavailable("the cluster has no writer")),
        }
    }

    fn readers(&self, shard: ShardId) -> Vec<ReadEndpoint> {
        if self.core.kind == TopologyKind::SingleNode {
            return self
                .writer(shard)
                .map(|endpoint| ReadEndpoint {
                    endpoint,
                    ryw: RywMethod::WriterOnly,
                    lag_ms: None,
                })
                .into_iter()
                .collect();
        }
        let mut out: Vec<ReadEndpoint> = self
            .core
            .live_nodes()
            .into_iter()
            .filter(|(_, role, state, _)| {
                *role == ObservedRole::Replica && *state == crate::health::NodeState::Ok
            })
            .filter_map(|(node, _, _, lag)| {
                Some(ReadEndpoint {
                    endpoint: self.core.endpoint_of(&node).ok()?,
                    ryw: RywMethod::GtidWait,
                    lag_ms: lag,
                })
            })
            .collect();
        out.sort_by_key(|r| (r.lag_ms.unwrap_or(u64::MAX), r.endpoint.node.clone()));
        out
    }

    fn binlog_source(&self, pref: SourcePref) -> Result<BinlogSource> {
        let candidates = self.core.binlog_candidates();
        let writer = self.core.read(|s| s.writer.clone());
        let from_writer = || {
            candidates
                .iter()
                .find(|(c, ..)| Some(&c.node) == writer.as_ref())
                .map(|(c, ..)| c.clone())
        };
        let best_replica = || {
            candidates
                .iter()
                .filter(|(_, role, _)| *role == ObservedRole::Replica)
                .min_by_key(|(c, _, lag)| {
                    (std::cmp::Reverse(c.feed_level), lag.unwrap_or(u64::MAX))
                })
                .map(|(c, ..)| c.clone())
        };
        let zero_loss = self
            .core
            .profile()
            .is_some_and(|p| p.derived.zero_loss_failover);
        let pick = match pref {
            SourcePref::Writer => from_writer(),
            SourcePref::Replica => best_replica(),
            SourcePref::Auto if zero_loss => from_writer().or_else(best_replica),
            SourcePref::Auto => best_replica().or_else(from_writer),
        };
        pick.ok_or_else(|| {
            capability(format!(
                "no node qualifies as a binlog source ({pref:?}): needs log_bin, ROW, FULL \
                 row image, gtid_strict_mode and log_slave_updates on replicas"
            ))
        })
    }

    fn validation_node(&self) -> Option<ValidationNode> {
        let method = self.core.profile()?.derived.validation?;
        if method != ValidationMethod::ReplicaBuild {
            return None;
        }
        // The healthy replica with the highest id: stable, and the others
        // keep serving reads.
        let node = self
            .core
            .live_nodes()
            .into_iter()
            .filter(|(_, role, state, _)| {
                *role == ObservedRole::Replica && *state == crate::health::NodeState::Ok
            })
            .map(|(n, ..)| n)
            .max()?;
        let ep = self.core.endpoint_of(&node).ok()?;
        Some(ValidationNode {
            node: ep.node,
            addr: ep.addr,
            method,
        })
    }

    fn ddl_method(&self, ddl: DdlKind, table: &TableTraits) -> DdlMethod {
        ddl_method_for(false, self.core.profile().as_ref(), ddl, table)
    }

    async fn fence_shard(&self, req: &FenceRequest) -> Result<FenceReceipt> {
        let writer = self
            .core
            .read(|s| s.writer.clone())
            .ok_or_else(|| unavailable("the cluster has no writer to fence"))?;
        // Writes happen only on the writer; an `All` fence also cuts the
        // tenant's read sessions on replicas.
        let kill_on: Vec<NodeId> = match req.scope {
            FenceScope::Writes => vec![writer.clone()],
            FenceScope::All => {
                let mut v: Vec<NodeId> = self
                    .core
                    .live_nodes()
                    .into_iter()
                    .map(|(n, ..)| n)
                    .collect();
                if !v.contains(&writer) {
                    v.insert(0, writer.clone());
                }
                v
            }
        };
        self.core.fence_on(req, &writer, &kill_on).await
    }

    async fn unfence_shard(&self, receipt: &FenceReceipt) -> Result<()> {
        self.core.unfence(receipt).await
    }

    async fn fence_cluster(&self, reason: FenceReason) -> Result<()> {
        Err(capability(format!(
            "self-fencing ({reason:?}) needs a managed cluster (R2)"
        )))
    }

    fn failover(&self) -> Option<&dyn FailoverHooks> {
        // Chapter 25, build step 13.
        None
    }
}

#[cfg(test)]
pub(super) fn choose_writer_for_tests(
    current: Option<&NodeId>,
    writers: &[NodeId],
    probes: &[NodeProbe],
) -> Option<NodeId> {
    choose_writer(current, writers, probes)
}
