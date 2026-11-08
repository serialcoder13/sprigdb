//! Capacity recommendations (chapter 11, "Scheduling, guardrails and
//! recommendations"; build step 7).
//!
//! Recommendations that belong to no single tenant live in the DC tier at
//! `/sprig/dc/<dc_id>/recommendations/<rec_id>` as JSON. At most one is
//! open per kind and cluster: the id is `<kind>-<cluster>`, so reopening
//! overwrites the closed record.
//!
//! Kinds and when they open:
//!
//! - `add_cluster`: a hot cluster had no feasible move for an hour, or a
//!   placement failed on the schema cap (reason `schema_cap`) or found no
//!   cluster at all (reason `no_capacity`);
//! - `grow_storage`: as `add_cluster`, when storage is the bottleneck;
//! - `approve_move`: the best move off a hot cluster is a shard above
//!   `migration.auto_size_limit_gb`;
//! - `add_replica` exists for the provisioner hand-off (R2) and is not
//!   opened by R0's planner, which has no read/write split of busy time.
//!
//! `need` is the load to absorb plus 30%.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_meta::{Key, ListOpts, MetaStore, Op, Record, Tier, Txn, Versioned};

use crate::model::{Bottleneck, Dim, FleetView, LoadVector, Timestamp};
use crate::plan::PlanReport;

/// A hot cluster with no feasible move for this long gets `add_cluster`.
pub const STUCK_SECS: u64 = 3600;

/// Margin on top of the load to absorb.
pub const NEED_MARGIN: f64 = 1.3;

/// A recommendation's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecKind {
    /// Add a cluster to the DC.
    AddCluster,
    /// Add a replica to a cluster.
    AddReplica,
    /// Grow a cluster's storage.
    GrowStorage,
    /// Approve a move above the automatic size limit.
    ApproveMove,
}

impl RecKind {
    /// The text form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AddCluster => "add_cluster",
            Self::AddReplica => "add_replica",
            Self::GrowStorage => "grow_storage",
            Self::ApproveMove => "approve_move",
        }
    }
}

/// Open or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecState {
    /// Waiting for an operator or the provisioner.
    Open,
    /// The condition cleared.
    Closed,
}

/// What a recommendation asks for.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Need {
    /// Bytes of storage.
    pub storage_bytes: f64,
    /// Busy time, ms per second.
    pub busy_ms_per_sec: f64,
    /// Tables.
    pub tables: f64,
    /// Capabilities the target must have.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    /// DCs the capacity must be in (residency).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub residency: Vec<String>,
}

/// A recommendation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    /// `<kind>-<cluster>`.
    pub rec_id: String,
    /// The kind.
    pub kind: RecKind,
    /// The cluster concerned (`dc` for DC-wide `add_cluster`).
    pub cluster: String,
    /// What it asks for.
    pub need: Need,
    /// Why: `no_feasible_move`, `schema_cap`, `no_capacity`, `size_limit`.
    pub reason: String,
    /// Tenants affected (ids only).
    pub tenants: Vec<TenantId>,
    /// The shard to approve, for `approve_move`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<String>,
    /// First seen, seconds since the epoch.
    pub first_seen: Timestamp,
    /// Open or closed.
    pub state: RecState,
}

impl Versioned for Recommendation {
    const V: u32 = 1;
}

/// The id of a recommendation.
#[must_use]
pub fn rec_id(kind: RecKind, cluster: &str) -> String {
    format!("{}-{cluster}", kind.as_str())
}

/// A change to persist.
#[derive(Debug, Clone, PartialEq)]
pub enum RecChange {
    /// Opened (or reopened).
    Opened(Recommendation),
    /// Closed.
    Closed(Recommendation),
}

/// The open recommendations and the timers that open them.
#[derive(Debug, Clone, Default)]
pub struct RecommendationBook {
    open: BTreeMap<String, Recommendation>,
    stuck_since: BTreeMap<ClusterId, Timestamp>,
}

fn need_of(load: LoadVector) -> Need {
    Need {
        storage_bytes: load.storage_bytes * NEED_MARGIN,
        busy_ms_per_sec: load.busy_ms_per_sec * NEED_MARGIN,
        tables: load.tables * NEED_MARGIN,
        ..Need::default()
    }
}

impl RecommendationBook {
    /// An empty book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open recommendations.
    #[must_use]
    pub fn open(&self) -> Vec<&Recommendation> {
        self.open.values().collect()
    }

    /// Open recommendations by kind (the `sprig_balancer_recommendations`
    /// gauge).
    #[must_use]
    pub fn counts(&self) -> BTreeMap<RecKind, usize> {
        let mut m = BTreeMap::new();
        for r in self.open.values() {
            *m.entry(r.kind).or_default() += 1;
        }
        m
    }

    fn put(&mut self, r: Recommendation, out: &mut Vec<RecChange>) {
        if self.open.get(&r.rec_id).is_some_and(|x| x.need == r.need && x.tenants == r.tenants) {
            return;
        }
        let r = match self.open.get(&r.rec_id) {
            Some(old) => Recommendation {
                first_seen: old.first_seen,
                ..r
            },
            None => r,
        };
        self.open.insert(r.rec_id.clone(), r.clone());
        out.push(RecChange::Opened(r));
    }

    /// Updates the book from a planning run.
    pub fn observe(&mut self, view: &FleetView, report: &PlanReport, now: Timestamp) -> Vec<RecChange> {
        let mut out = Vec::new();
        // Hot clusters with no move: an hour, then add_cluster or
        // grow_storage.
        let stuck: Vec<ClusterId> = report.stuck.clone();
        self.stuck_since.retain(|c, _| stuck.contains(c));
        let mut keep: Vec<String> = Vec::new();
        for c in &stuck {
            let since = *self.stuck_since.entry(c.clone()).or_insert(now);
            if now < since + STUCK_SECS {
                continue;
            }
            let Some(cv) = view.clusters.get(c) else {
                continue;
            };
            let load = view.load_on(c);
            let f = cv.full_with(&load);
            let kind = if f.bottleneck == Bottleneck::Dim(Dim::Storage) {
                RecKind::GrowStorage
            } else {
                RecKind::AddCluster
            };
            // Absorb what is above the target.
            let excess = if f.value > 0.0 {
                load.scale(((f.value - cv.limits.target) / f.value).max(0.0))
            } else {
                LoadVector::default()
            };
            let mut tenants: Vec<TenantId> = view
                .shards
                .values()
                .filter(|s| &s.cluster == c)
                .map(|s| s.key.tenant)
                .collect();
            tenants.sort_unstable();
            tenants.dedup();
            let r = Recommendation {
                rec_id: rec_id(kind, c.as_str()),
                kind,
                cluster: c.to_string(),
                need: need_of(excess),
                reason: "no_feasible_move".to_owned(),
                tenants,
                shard: None,
                first_seen: since,
                state: RecState::Open,
            };
            keep.push(r.rec_id.clone());
            self.put(r, &mut out);
        }
        for a in &report.approvals {
            let id = rec_id(RecKind::ApproveMove, a.from.as_str());
            let r = Recommendation {
                rec_id: id.clone(),
                kind: RecKind::ApproveMove,
                cluster: a.from.to_string(),
                need: Need {
                    storage_bytes: a.size_bytes,
                    ..Need::default()
                },
                reason: "size_limit".to_owned(),
                tenants: vec![a.shard.tenant],
                shard: Some(format!("{} to {}", a.shard, a.to)),
                first_seen: now,
                state: RecState::Open,
            };
            keep.push(id);
            self.put(r, &mut out);
        }
        // Close what no longer holds (placement-time ones stay until a
        // later placement succeeds on that cluster: see `placed`).
        let close: Vec<String> = self
            .open
            .iter()
            .filter(|(id, r)| !keep.contains(id) && r.reason != "schema_cap" && r.reason != "no_capacity")
            .map(|(id, _)| id.clone())
            .collect();
        for id in close {
            if let Some(mut r) = self.open.remove(&id) {
                r.state = RecState::Closed;
                out.push(RecChange::Closed(r));
            }
        }
        out
    }

    /// A placement found no cluster: `add_cluster` for the DC, reason
    /// `schema_cap` when the schema cap blocked it, else `no_capacity`.
    pub fn placement_failed(
        &mut self,
        dc: &str,
        tenant: TenantId,
        predicted: LoadVector,
        schema_cap: bool,
        residency: Vec<String>,
        now: Timestamp,
    ) -> Vec<RecChange> {
        let mut out = Vec::new();
        let id = rec_id(RecKind::AddCluster, dc);
        let mut tenants = self.open.get(&id).map(|r| r.tenants.clone()).unwrap_or_default();
        if !tenants.contains(&tenant) {
            tenants.push(tenant);
            tenants.sort_unstable();
        }
        let r = Recommendation {
            rec_id: id,
            kind: RecKind::AddCluster,
            cluster: dc.to_owned(),
            need: Need {
                residency,
                ..need_of(predicted)
            },
            reason: if schema_cap { "schema_cap" } else { "no_capacity" }.to_owned(),
            tenants,
            shard: None,
            first_seen: now,
            state: RecState::Open,
        };
        self.put(r, &mut out);
        out
    }

    /// A placement succeeded in `dc`: the DC-wide `add_cluster` closes.
    pub fn placed(&mut self, dc: &str) -> Vec<RecChange> {
        let id = rec_id(RecKind::AddCluster, dc);
        match self.open.remove(&id) {
            Some(mut r) => {
                r.state = RecState::Closed;
                vec![RecChange::Closed(r)]
            }
            None => Vec::new(),
        }
    }
}

/// Recommendations in the DC tier.
pub struct RecommendationStore {
    store: Arc<dyn MetaStore>,
    tier: Tier,
}

impl std::fmt::Debug for RecommendationStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecommendationStore").finish_non_exhaustive()
    }
}

impl RecommendationStore {
    /// The store over `store` in `tier` (the DC tier).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, tier: Tier) -> Self {
        Self { store, tier }
    }

    fn key(&self, id: &str) -> Result<Key> {
        self.tier.key(&format!("recommendations/{id}"))
    }

    /// Writes the changes.
    ///
    /// # Errors
    /// The store's.
    pub async fn apply(&self, changes: &[RecChange]) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut then = Vec::new();
        for c in changes {
            let (RecChange::Opened(r) | RecChange::Closed(r)) = c;
            then.push(Op::Put {
                key: self.key(&r.rec_id)?,
                value: Record::new(r.clone()).encode()?,
                lease: None,
            });
        }
        self.store
            .txn(Txn {
                when: Vec::new(),
                then,
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// Every recommendation, open and closed.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<Vec<Recommendation>> {
        let prefix = self.tier.key("recommendations/")?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if let Ok(r) = Record::<Recommendation>::decode(&kv.value) {
                    out.push(r.body);
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
}

#[cfg(test)]
mod tests;
