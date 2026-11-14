//! The Galera driver and the writer choice.
//!
//! One writer node per shard avoids certification conflicts. The
//! controller publishes the writer set and `writer_epoch`
//! ([`WriterSetTracker`]); gateways pick each shard's writer by weighted
//! rendezvous hashing ([`choose_writer`]): the node maximizing
//! `-weight / ln(u)`, with `u = (xxh3_64(shard_id || node) + 0.5) / 2^64`.
//! A leaving node moves only its own shards; a returning one rejoins after
//! `cluster.galera_writer_rejoin_seconds`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::Result;
use sprig_common::ids::{NodeId, ShardId};
use twox_hash::XxHash3_64;

use super::{
    BinlogSource, Core, DdlKind, DdlMethod, Endpoint, FailoverHooks, NodeConnector, ReadEndpoint,
    SourcePref, TableTraits, TopologyDriver, ValidationNode, capability, ddl_method_for,
    discover_nodes, fence_shard_id, unavailable,
};
use crate::fence::{FenceReason, FenceReceipt, FenceRequest};
use crate::health::{ClusterHealth, HealthReport, NodeState, ObservedRole};
use crate::probe::NodeProbe;
use crate::profile::{CapabilityProfile, RywMethod, ValidationMethod};
use crate::record::{ClusterRecord, Declared, GaleraWriters, NodeSpec, TopologyKind, WriterWeight};
use crate::settings::ClusterSettings;

/// The hash input of the writer choice: the shard id as four big-endian
/// bytes, then the node id's UTF-8 bytes.
#[must_use]
pub fn rendezvous_hash(shard: ShardId, node: &NodeId) -> u64 {
    let mut buf = Vec::with_capacity(4 + node.as_str().len());
    buf.extend_from_slice(&shard.0.to_be_bytes());
    buf.extend_from_slice(node.as_str().as_bytes());
    XxHash3_64::oneshot(&buf)
}

/// The weighted rendezvous score of `node` for `shard`: `-weight / ln(u)`
/// with `u = (hash + 0.5) / 2^64`, so `0 < u < 1`. A non-positive weight
/// scores 0: the node never wins against a positive one.
#[must_use]
#[allow(clippy::cast_precision_loss)] // u only needs to be uniform in (0, 1)
pub fn rendezvous_score(shard: ShardId, node: &NodeId, weight: f64) -> f64 {
    if weight.is_nan() || weight <= 0.0 {
        return 0.0;
    }
    let u = (rendezvous_hash(shard, node) as f64 + 0.5) / 18_446_744_073_709_551_616.0;
    // u can round to exactly 1.0 for the largest hashes; keep ln(u) < 0.
    let ln = u.ln().min(-f64::MIN_POSITIVE);
    -weight / ln
}

/// The shard's writer among `writers`: the highest score, ties broken by
/// node id so every gateway agrees.
#[must_use]
pub fn choose_writer(shard: ShardId, writers: &[WriterWeight]) -> Option<&WriterWeight> {
    writers.iter().filter(|w| w.weight > 0.0).max_by(|a, b| {
        rendezvous_score(shard, &a.node, a.weight)
            .total_cmp(&rendezvous_score(shard, &b.node, b.weight))
            .then_with(|| b.node.cmp(&a.node))
    })
}

/// Maintains the published writer set from health passes (the
/// controller's side). A node leaves the set as soon as it stops being
/// usable; it returns only after being usable for the rejoin period, so a
/// flapping node doesn't move shards back and forth.
#[derive(Debug, Clone, Default)]
pub struct WriterSetTracker {
    usable_since: HashMap<NodeId, Instant>,
    weights: HashMap<NodeId, f64>,
}

impl WriterSetTracker {
    /// A tracker with no history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one health pass; returns the new writer set when it changed,
    /// with `writer_epoch` raised. With no published set yet (a new
    /// cluster), every usable node joins at once. An `unknown` pass (this
    /// controller can't see the nodes) changes nothing.
    pub fn observe(
        &mut self,
        current: Option<&GaleraWriters>,
        report: &HealthReport,
        now: Instant,
        rejoin: Duration,
    ) -> Option<GaleraWriters> {
        if report.health == ClusterHealth::Unknown {
            return None;
        }
        let usable: Vec<&NodeId> = report
            .nodes
            .iter()
            .filter(|n| n.role == ObservedRole::Galera && n.state == NodeState::Ok)
            .map(|n| &n.node)
            .collect();
        self.usable_since.retain(|n, _| usable.contains(&n));
        for n in &usable {
            self.usable_since.entry((*n).clone()).or_insert(now);
        }
        if let Some(c) = current {
            for w in &c.writers {
                self.weights.insert(w.node.clone(), w.weight);
            }
        }
        let weight = |n: &NodeId| self.weights.get(n).copied().unwrap_or(1.0);
        let mut next: Vec<WriterWeight> = match current {
            None => usable
                .iter()
                .map(|n| WriterWeight {
                    node: (*n).clone(),
                    weight: weight(n),
                })
                .collect(),
            Some(c) => {
                let mut v: Vec<WriterWeight> = c
                    .writers
                    .iter()
                    .filter(|w| usable.contains(&&w.node))
                    .cloned()
                    .collect();
                for n in &usable {
                    let ready = self
                        .usable_since
                        .get(*n)
                        .is_some_and(|since| now.saturating_duration_since(*since) >= rejoin);
                    if ready && !v.iter().any(|w| &w.node == *n) {
                        v.push(WriterWeight {
                            node: (*n).clone(),
                            weight: weight(n),
                        });
                    }
                }
                v
            }
        };
        next.sort_by(|a, b| a.node.cmp(&b.node));
        let same = current.is_some_and(|c| {
            let mut old = c.writers.clone();
            old.sort_by(|a, b| a.node.cmp(&b.node));
            old == next
        });
        if same {
            return None;
        }
        Some(GaleraWriters {
            writer_epoch: current.map_or(1, |c| c.writer_epoch + 1),
            writers: next,
        })
    }
}

/// The Galera driver.
#[derive(Debug)]
pub struct GaleraDriver {
    core: Core,
    writers: Mutex<WriterSetTracker>,
}

impl GaleraDriver {
    /// A driver.
    #[must_use]
    pub fn new(connector: Arc<dyn NodeConnector>, settings: ClusterSettings) -> Self {
        Self {
            core: Core::new(TopologyKind::Galera, connector, settings),
            writers: Mutex::new(WriterSetTracker::new()),
        }
    }

    /// The last health report.
    #[must_use]
    pub fn last_report(&self) -> Option<HealthReport> {
        self.core.read(|s| s.report.clone())
    }

    /// The writer set to publish after the last health pass, if it changed
    /// (the controller writes it to the record, which raises
    /// `WriterChanged`).
    pub fn plan_writer_set(&self, now: Instant) -> Option<GaleraWriters> {
        let (current, report) = self.core.read(|s| {
            (
                s.record.as_ref().and_then(|r| r.galera.clone()),
                s.report.clone(),
            )
        });
        let report = report?;
        self.writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(
                current.as_ref(),
                &report,
                now,
                self.core.settings().galera_writer_rejoin,
            )
    }

    /// The writer set the shard choice uses: the published one, without
    /// nodes this process saw fail; every usable node before a set is
    /// published.
    fn effective_writers(&self) -> Vec<WriterWeight> {
        let usable: Vec<NodeId> = self
            .core
            .live_nodes()
            .into_iter()
            .filter(|(_, role, state, _)| *role == ObservedRole::Galera && *state == NodeState::Ok)
            .map(|(n, ..)| n)
            .collect();
        let published = self
            .core
            .read(|s| s.record.as_ref().and_then(|r| r.galera.clone()));
        let seen = self.core.read(|s| s.report.is_some());
        match published {
            Some(p) => {
                let alive: Vec<WriterWeight> = p
                    .writers
                    .iter()
                    .filter(|w| !seen || usable.contains(&w.node))
                    .cloned()
                    .collect();
                // Brief disagreement with other gateways is fine (at
                // worst 1213); writing to a dead node is not.
                if alive.is_empty() { p.writers } else { alive }
            }
            None => usable
                .into_iter()
                .map(|node| WriterWeight { node, weight: 1.0 })
                .collect(),
        }
    }

    fn synced_nodes(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self
            .core
            .live_nodes()
            .into_iter()
            .filter(|(_, role, state, _)| *role == ObservedRole::Galera && *state == NodeState::Ok)
            .map(|(n, ..)| n)
            .collect();
        v.sort();
        v
    }
}

#[async_trait::async_trait]
impl TopologyDriver for GaleraDriver {
    fn kind(&self) -> TopologyKind {
        TopologyKind::Galera
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
        self.core.assess(probes, profile).health
    }

    fn writer(&self, shard: ShardId) -> Result<Endpoint> {
        let writers = self.effective_writers();
        let w = choose_writer(shard, &writers)
            .ok_or_else(|| unavailable("the Galera cluster has no writer"))?;
        self.core.endpoint_of(&w.node)
    }

    fn readers(&self, _shard: ShardId) -> Vec<ReadEndpoint> {
        let lags: Vec<(NodeId, Option<u64>)> = self
            .core
            .live_nodes()
            .into_iter()
            .map(|(n, _, _, lag)| (n, lag))
            .collect();
        self.synced_nodes()
            .into_iter()
            .filter_map(|n| {
                Some(ReadEndpoint {
                    lag_ms: lags.iter().find(|(x, _)| x == &n).and_then(|(_, l)| *l),
                    endpoint: self.core.endpoint_of(&n).ok()?,
                    ryw: RywMethod::WsrepSyncWait,
                })
            })
            .collect()
    }

    fn binlog_source(&self, pref: SourcePref) -> Result<BinlogSource> {
        let mut candidates = self.core.binlog_candidates();
        candidates.sort_by(|a, b| {
            b.0.feed_level
                .cmp(&a.0.feed_level)
                .then_with(|| a.0.node.cmp(&b.0.node))
        });
        // Every synced node logs every write (with log_slave_updates), so
        // all preferences mean "any synced node".
        candidates
            .into_iter()
            .next()
            .map(|(c, ..)| c)
            .ok_or_else(|| {
                capability(format!(
                    "no synced Galera node qualifies as a binlog source ({pref:?}): needs log_bin, \
                 ROW, FULL row image, gtid_strict_mode and log_slave_updates"
                ))
            })
    }

    fn validation_node(&self) -> Option<ValidationNode> {
        let method = self.core.profile()?.derived.validation?;
        if method != ValidationMethod::GaleraRsu {
            return None;
        }
        let synced = self.synced_nodes();
        if synced.len() < 3 {
            return None;
        }
        // Prefer a node outside the writer set; else the highest id.
        let writers = self.effective_writers();
        let node = synced
            .iter()
            .rev()
            .find(|n| !writers.iter().any(|w| &w.node == *n))
            .or_else(|| synced.last())?
            .clone();
        let ep = self.core.endpoint_of(&node).ok()?;
        Some(ValidationNode {
            node: ep.node,
            addr: ep.addr,
            method,
        })
    }

    fn ddl_method(&self, ddl: DdlKind, table: &TableTraits) -> DdlMethod {
        ddl_method_for(true, self.core.profile().as_ref(), ddl, table)
    }

    async fn fence_shard(&self, req: &FenceRequest) -> Result<FenceReceipt> {
        // Grants and renames replicate in total order from the shard's
        // writer; every node accepts writes, so sessions die everywhere.
        let writer = self.writer(fence_shard_id(req))?.node;
        let mut kill_on = self.synced_nodes();
        if !kill_on.contains(&writer) {
            kill_on.insert(0, writer.clone());
        }
        // Nodes that answer but aren't synced (donor, joining) can still
        // hold sessions.
        for (n, ..) in self.core.live_nodes() {
            if !kill_on.contains(&n) {
                kill_on.push(n);
            }
        }
        self.core.fence_on(req, &writer, &kill_on).await
    }

    async fn unfence_shard(&self, receipt: &FenceReceipt) -> Result<()> {
        self.core.unfence(receipt).await
    }

    async fn fence_cluster(&self, reason: FenceReason) -> Result<()> {
        Err(capability(format!(
            "self-fencing ({reason:?}) needs a managed cluster"
        )))
    }

    fn failover(&self) -> Option<&dyn FailoverHooks> {
        // Failover runs as plain functions in `crate::failover`.
        None
    }
}
