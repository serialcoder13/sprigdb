//! The cluster registry over [`MetaStore`].
//!
//! Ids come from the global tier by create-if-absent on
//! `/sprig/g/cluster-ids/<number>`, so any gateway can find a cluster's data
//! center. Records live in the data center's tier at
//! `/sprig/dc/<dc_id>/clusters/<cluster_id>`. Every write is a transaction
//! on the record's `mod_revision`.

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{
    Cmp, Consistency, Key, KeyValue, ListOpts, MetaStore, Op, Record, Revision, Tier, Txn,
    WatchEvent,
};
use tokio::sync::broadcast;

use crate::health::ClusterHealth;
use crate::profile::{FieldChange, Publication};
use crate::record::{
    AdminState, Capacity, ClusterIdEntry, ClusterRecord, Declared, HealthSummary, NodeSpec,
    ProvisionerKind, SecretRefs, TopologyKind,
};

/// Attempts of a compare-and-set loop before giving up.
const CAS_ATTEMPTS: usize = 16;

/// Events the registry broadcasts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ClusterEvent {
    /// A cluster was registered.
    Registered {
        /// The cluster.
        cluster: ClusterId,
    },
    /// The admin state changed.
    AdminStateChanged {
        /// The cluster.
        cluster: ClusterId,
        /// Before.
        from: AdminState,
        /// After.
        to: AdminState,
    },
    /// A new capability profile was published.
    CapabilityChanged {
        /// The cluster.
        cluster: ClusterId,
        /// The profile version.
        version: u64,
        /// What changed.
        changes: Vec<FieldChange>,
    },
    /// The published health changed.
    HealthChanged {
        /// The cluster.
        cluster: ClusterId,
        /// Before.
        from: Option<ClusterHealth>,
        /// After.
        to: ClusterHealth,
    },
    /// The Galera writer set changed.
    WriterChanged {
        /// The cluster.
        cluster: ClusterId,
        /// The new epoch.
        writer_epoch: u64,
    },
    /// The tuning state changed.
    TuningChanged {
        /// The cluster.
        cluster: ClusterId,
    },
    /// The record was deleted.
    Removed {
        /// The cluster.
        cluster: ClusterId,
    },
}

/// The events that turn `old` into `new`, for watchers that diff records.
#[must_use]
pub fn events_between(old: Option<&ClusterRecord>, new: &ClusterRecord) -> Vec<ClusterEvent> {
    let cluster = new.cluster_id.clone();
    let Some(old) = old else {
        return vec![ClusterEvent::Registered { cluster }];
    };
    let mut out = Vec::new();
    if old.admin_state != new.admin_state {
        out.push(ClusterEvent::AdminStateChanged {
            cluster: cluster.clone(),
            from: old.admin_state,
            to: new.admin_state,
        });
    }
    if old.profile_version != new.profile_version
        && let Some(p) = &new.profile
    {
        let changes = old
            .profile
            .as_ref()
            .map(|o| crate::profile::diff(o, p))
            .unwrap_or_default();
        out.push(ClusterEvent::CapabilityChanged {
            cluster: cluster.clone(),
            version: new.profile_version,
            changes,
        });
    }
    let state = |r: &ClusterRecord| r.health.as_ref().map(|h| h.state);
    if let Some(to) = state(new)
        && state(old) != Some(to)
    {
        out.push(ClusterEvent::HealthChanged {
            cluster: cluster.clone(),
            from: state(old),
            to,
        });
    }
    let epoch = |r: &ClusterRecord| r.galera.as_ref().map(|g| g.writer_epoch);
    if let Some(e) = epoch(new)
        && epoch(old) != Some(e)
    {
        out.push(ClusterEvent::WriterChanged {
            cluster: cluster.clone(),
            writer_epoch: e,
        });
    }
    if old.tuning != new.tuning {
        out.push(ClusterEvent::TuningChanged { cluster });
    }
    out
}

/// What `sprigctl cluster register` supplies.
#[derive(Debug, Clone, PartialEq)]
pub struct Registration {
    /// The operator's name, unique within the data center.
    pub name: String,
    /// The topology.
    pub topology: TopologyKind,
    /// Who made the nodes.
    pub provisioner: ProvisionerKind,
    /// The nodes.
    pub nodes: Vec<NodeSpec>,
    /// Secret references.
    pub secrets: SecretRefs,
    /// Declared capacity.
    pub capacity: Capacity,
    /// `[declared]`, for external clusters.
    pub declared: Option<Declared>,
}

/// A record as stored, with its revision for compare-and-set.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredCluster {
    /// The decoded record.
    pub record: Record<ClusterRecord>,
    /// Its `mod_revision`.
    pub mod_revision: Revision,
}

impl StoredCluster {
    /// The record's body.
    #[must_use]
    pub fn body(&self) -> &ClusterRecord {
        &self.record.body
    }

    fn decode(kv: &KeyValue) -> Result<Self> {
        Ok(Self {
            record: Record::decode(&kv.value)?,
            mod_revision: kv.mod_revision,
        })
    }
}

/// A change seen by [`ClusterRegistry::watch`].
#[derive(Debug, Clone, PartialEq)]
pub enum RegistryChange {
    /// A record was written.
    Put(Box<StoredCluster>),
    /// A record was deleted.
    Deleted {
        /// The cluster.
        cluster: ClusterId,
        /// The revision of the deletion.
        mod_revision: Revision,
    },
    /// Every change up to this revision has been delivered.
    Progress(Revision),
    /// The start revision was compacted: list again and watch from the
    /// listing's revision + 1. The stream ends after this.
    Compacted(Revision),
}

/// A stream of [`RegistryChange`]s.
pub type RegistryStream = Pin<Box<dyn Stream<Item = Result<RegistryChange>> + Send>>;

/// The data center of a cluster id, `<dc_id>-c<number>`; `None` for an id of another form.
#[must_use]
pub fn dc_of(id: &ClusterId) -> Option<DcId> {
    let (dc, n) = id.as_str().rsplit_once("-c")?;
    if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    DcId::new(dc).ok()
}

/// The cluster registry of one data center.
#[derive(Clone)]
pub struct ClusterRegistry {
    global: Arc<dyn MetaStore>,
    dc: Arc<dyn MetaStore>,
    dc_id: DcId,
    events: broadcast::Sender<ClusterEvent>,
    /// Extra compares on every DC-tier write, such as the DC leader's
    /// fencing token ([`with_guard`](Self::with_guard)).
    guard: Vec<Cmp>,
}

impl std::fmt::Debug for ClusterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterRegistry")
            .field("dc", &self.dc_id)
            .finish_non_exhaustive()
    }
}

impl ClusterRegistry {
    /// Event queue length; a subscriber that falls this far behind sees
    /// `Lagged` and should re-list.
    pub const EVENT_QUEUE: usize = 256;

    /// A registry over the global tier and one data center's tier.
    ///
    /// # Errors
    /// `Internal` if the stores' tiers are not global and a DC.
    pub fn new(global: Arc<dyn MetaStore>, dc: Arc<dyn MetaStore>) -> Result<Self> {
        let Tier::Dc(dc_id) = dc.tier().clone() else {
            return Err(SprigError::internal(
                "cluster registry needs a DC-tier store",
            ));
        };
        if *global.tier() != Tier::Global {
            return Err(SprigError::internal(
                "cluster registry needs the global store",
            ));
        }
        let (events, _) = broadcast::channel(Self::EVENT_QUEUE);
        Ok(Self {
            global,
            dc,
            dc_id,
            events,
            guard: Vec::new(),
        })
    }

    /// The same registry, with `guard` added to every write in the DC tier:
    /// a leader passes its fencing token (`Cmp::CreateRev` of its leader
    /// key) so a deposed leader's late writes never commit. A write that
    /// fails only its guard returns 39504 `ER_SPRIG_NOT_LEADER`.
    #[must_use]
    pub fn with_guard(mut self, guard: Vec<Cmp>) -> Self {
        self.guard = guard;
        self
    }

    fn not_leader() -> SprigError {
        SprigError::new(
            SprigCode::NotLeader,
            "the registry write's leader guard no longer holds",
        )
    }

    /// The data center.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc_id
    }

    /// The DC tier's store (where the failover loop publishes each
    /// cluster's writer).
    #[must_use]
    pub fn dc_store(&self) -> &Arc<dyn MetaStore> {
        &self.dc
    }

    /// Events from writes made through this registry.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.events.subscribe()
    }

    fn emit(&self, events: Vec<ClusterEvent>) {
        for e in events {
            // No subscriber is fine.
            let _ = self.events.send(e);
        }
    }

    fn record_key(&self, id: &ClusterId) -> Result<Key> {
        self.dc.tier().key(&format!("clusters/{id}"))
    }

    fn records_prefix(&self) -> Result<Key> {
        self.dc.tier().key("clusters/")
    }

    fn id_key(number: u32) -> Result<Key> {
        Tier::Global.key(&format!("cluster-ids/{number}"))
    }

    /// The textual id for a number: `<dc>-c<number>`.
    ///
    /// # Errors
    /// `Internal` if the result is longer than a cluster id may be.
    pub fn cluster_id(dc: &DcId, number: u32) -> Result<ClusterId> {
        ClusterId::new(format!("{dc}-c{number}"))
            .map_err(|e| SprigError::internal(format!("cluster id: {e}")))
    }

    /// Allocates the next free cluster number for `name` in this data center.
    ///
    /// # Errors
    /// 39500 if the global tier is unreachable; `Internal` if the race for
    /// a number is lost too often.
    pub async fn allocate_id(&self, name: &str) -> Result<(u32, ClusterId)> {
        let prefix = Tier::Global.key("cluster-ids/")?;
        for _ in 0..CAS_ATTEMPTS {
            let mut max = 0u32;
            for kv in list_all(&*self.global, &prefix).await? {
                let n = kv
                    .key
                    .as_str()
                    .rsplit('/')
                    .next()
                    .and_then(|s| s.parse::<u32>().ok());
                max = max.max(n.unwrap_or(0));
            }
            let number = max
                .checked_add(1)
                .ok_or_else(|| SprigError::internal("cluster numbers exhausted"))?;
            let cluster_id = Self::cluster_id(&self.dc_id, number)?;
            let key = Self::id_key(number)?;
            let entry = Record::new(ClusterIdEntry {
                dc_id: self.dc_id.clone(),
                name: name.to_owned(),
                cluster_id: cluster_id.clone(),
            });
            let res = self
                .global
                .txn(Txn {
                    when: vec![Cmp::Missing(key.clone())],
                    then: vec![Op::Put {
                        key,
                        value: entry.encode()?,
                        lease: None,
                    }],
                    otherwise: vec![],
                })
                .await?;
            if res.succeeded {
                return Ok((number, cluster_id));
            }
        }
        Err(SprigError::internal(
            "lost the race for a cluster number too often",
        ))
    }

    /// Finds a cluster's data center from its number, through the global
    /// tier.
    ///
    /// # Errors
    /// 39500 if the global tier is unreachable.
    pub async fn lookup(&self, number: u32) -> Result<Option<ClusterIdEntry>> {
        let kv = self
            .global
            .get(&Self::id_key(number)?, Consistency::Serializable)
            .await?;
        kv.map(|kv| Record::<ClusterIdEntry>::decode(&kv.value).map(|r| r.body))
            .transpose()
    }

    /// Registers a cluster: allocates its id and writes the record as
    /// `probing`. The caller then probes and publishes `active` or
    /// `rejected`.
    ///
    /// # Errors
    /// `Internal` for an empty node list; 39105 `ER_SPRIG_CLUSTER_EXISTS`
    /// for a name already used in this data center; 39500 if etcd is
    /// unreachable; 39504 if a guard fails.
    pub async fn register(&self, reg: Registration) -> Result<StoredCluster> {
        if reg.nodes.is_empty() {
            return Err(SprigError::internal("a cluster needs at least one node"));
        }
        if reg.name.is_empty() || reg.name.len() > 64 {
            return Err(SprigError::internal("cluster names are 1 to 64 characters"));
        }
        if self
            .list()
            .await?
            .iter()
            .any(|c| c.body().name == reg.name && c.body().admin_state != AdminState::Retired)
        {
            return Err(SprigError::new(
                SprigCode::ClusterExists,
                format!(
                    "a cluster named {:?} already exists in {}",
                    reg.name, self.dc_id
                ),
            ));
        }
        let (number, cluster_id) = self.allocate_id(&reg.name).await?;
        self.register_numbered(reg, number, cluster_id).await
    }

    /// Registers `reg` under a number [`Self::allocate_id`] reserved
    /// earlier: the VM provisioner needs the number for the
    /// nodes' `server_id` and `gtid_domain_id` before the cluster exists.
    ///
    /// # Errors
    /// As [`Self::register`]; `Internal` if `cluster_id` isn't `number`'s.
    pub async fn register_numbered(
        &self,
        reg: Registration,
        number: u32,
        cluster_id: ClusterId,
    ) -> Result<StoredCluster> {
        if Self::cluster_id(&self.dc_id, number)? != cluster_id {
            return Err(SprigError::internal(format!(
                "cluster id {cluster_id} isn't number {number} of {}",
                self.dc_id
            )));
        }
        if reg.nodes.is_empty() {
            return Err(SprigError::internal("a cluster needs at least one node"));
        }
        let body = ClusterRecord {
            cluster_id: cluster_id.clone(),
            cluster_number: number,
            name: reg.name,
            dc_id: self.dc_id.clone(),
            topology: reg.topology,
            provisioner: reg.provisioner,
            admin_state: AdminState::Probing,
            nodes: reg.nodes,
            secrets: reg.secrets,
            capacity: reg.capacity,
            galera: None,
            declared: reg.declared,
            profile_version: 0,
            profile: None,
            health: None,
            tuning: None,
            gaps: Vec::new(),
            node_identities: std::collections::BTreeMap::new(),
        };
        let record = Record::new(body);
        let key = self.record_key(&cluster_id)?;
        let mut when = vec![Cmp::Missing(key.clone())];
        when.extend(self.guard.iter().cloned());
        let res = self
            .dc
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key: key.clone(),
                    value: record.encode()?,
                    lease: None,
                }],
                otherwise: vec![Op::Get(key)],
            })
            .await?;
        if !res.succeeded {
            if !self.guard.is_empty()
                && matches!(res.results.first(), Some(sprig_meta::OpResult::Get(None)))
            {
                return Err(Self::not_leader());
            }
            return Err(SprigError::internal(format!(
                "cluster record {cluster_id} exists without an id entry"
            )));
        }
        self.emit(vec![ClusterEvent::Registered {
            cluster: cluster_id,
        }]);
        Ok(StoredCluster {
            record,
            mod_revision: res.revision,
        })
    }

    /// Reads one record.
    ///
    /// # Errors
    /// 39500 if etcd is unreachable; `Internal` for an undecodable record.
    pub async fn get(&self, id: &ClusterId) -> Result<Option<StoredCluster>> {
        // Another DC's cluster (a tenant homed there, or the source of a
        // move between DCs): read from that DC's tier, which single-etcd
        // mode serves from the same etcd; with separate etcds the relay's
        // `RemoteMaps` replaces this.
        if let Some(other) = dc_of(id).filter(|d| *d != self.dc_id) {
            let tier = Tier::Dc(other);
            let key = tier.key(&format!("clusters/{id}"))?;
            return sprig_meta::TierView::over(&self.dc, tier)
                .get(&key, Consistency::Linearizable)
                .await?
                .as_ref()
                .map(StoredCluster::decode)
                .transpose();
        }
        self.dc
            .get(&self.record_key(id)?, Consistency::Linearizable)
            .await?
            .as_ref()
            .map(StoredCluster::decode)
            .transpose()
    }

    /// Every record of this data center.
    ///
    /// # Errors
    /// 39500 if etcd is unreachable; `Internal` for an undecodable record.
    pub async fn list(&self) -> Result<Vec<StoredCluster>> {
        list_all(&*self.dc, &self.records_prefix()?)
            .await?
            .iter()
            .map(StoredCluster::decode)
            .collect()
    }

    /// Read-modify-write on the record's `mod_revision`, retried on
    /// conflict. `f` returns `false` to leave the record unchanged.
    ///
    /// # Errors
    /// `Internal` if the record is missing, was written by a newer build,
    /// or `f` fails; 39500 if etcd is unreachable.
    pub async fn update<F>(&self, id: &ClusterId, mut f: F) -> Result<StoredCluster>
    where
        F: FnMut(&mut ClusterRecord) -> Result<bool> + Send,
    {
        let key = self.record_key(id)?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(cur) = self.get(id).await? else {
                return Err(SprigError::internal(format!("no cluster {id}")));
            };
            let old = cur.record.body.clone();
            let mut next = cur.record.clone();
            if !f(&mut next.body)? {
                return Ok(cur);
            }
            if next.body.cluster_id != old.cluster_id {
                return Err(SprigError::internal("a cluster's id never changes"));
            }
            let mut when = vec![Cmp::ModRev(key.clone(), cur.mod_revision)];
            when.extend(self.guard.iter().cloned());
            let res = self
                .dc
                .txn(Txn {
                    when,
                    then: vec![Op::Put {
                        key: key.clone(),
                        value: next.encode()?,
                        lease: None,
                    }],
                    otherwise: vec![Op::Get(key.clone())],
                })
                .await?;
            if !res.succeeded
                && !self.guard.is_empty()
                && matches!(res.results.first(),
                    Some(sprig_meta::OpResult::Get(Some(kv))) if kv.mod_revision == cur.mod_revision)
            {
                return Err(Self::not_leader());
            }
            if res.succeeded {
                self.emit(events_between(Some(&old), &next.body));
                return Ok(StoredCluster {
                    record: next,
                    mod_revision: res.revision,
                });
            }
        }
        Err(SprigError::new(
            SprigCode::MetaUnavailable,
            format!("cluster {id}: too many conflicting writes"),
        ))
    }

    /// Moves the admin state along the state machine.
    ///
    /// # Errors
    /// `Internal` for a transition the state machine doesn't allow.
    pub async fn set_admin_state(
        &self,
        id: &ClusterId,
        to: AdminState,
        gaps: Vec<String>,
    ) -> Result<StoredCluster> {
        self.update(id, |r| {
            if r.admin_state == to {
                let changed = r.gaps != gaps;
                r.gaps.clone_from(&gaps);
                return Ok(changed);
            }
            if !r.admin_state.can_become(to) {
                return Err(SprigError::internal(format!(
                    "cluster {}: {} cannot become {to}",
                    r.cluster_id, r.admin_state
                )));
            }
            r.admin_state = to;
            r.gaps.clone_from(&gaps);
            Ok(true)
        })
        .await
    }

    /// Publishes a profile from a [`CapabilityPublisher`](crate::profile::CapabilityPublisher):
    /// the record's `profile_version` follows the profile's version.
    ///
    /// # Errors
    /// As [`update`](Self::update).
    pub async fn publish_profile(&self, id: &ClusterId, p: &Publication) -> Result<StoredCluster> {
        self.update(id, |r| {
            let mut profile = p.profile.clone();
            profile.version = profile.version.max(r.profile_version + 1);
            r.profile_version = profile.version;
            r.profile = Some(profile);
            Ok(true)
        })
        .await
    }

    /// Publishes a health state; `since` is RFC 3339 and kept while the
    /// state is unchanged.
    ///
    /// # Errors
    /// As [`update`](Self::update).
    pub async fn publish_health(
        &self,
        id: &ClusterId,
        state: ClusterHealth,
        since: String,
    ) -> Result<StoredCluster> {
        self.update(id, |r| {
            if r.health.as_ref().is_some_and(|h| h.state == state) {
                return Ok(false);
            }
            r.health = Some(HealthSummary {
                state,
                since: since.clone(),
            });
            Ok(true)
        })
        .await
    }

    /// Watches every record of this data center from revision `from`
    /// (0: from the next write).
    ///
    /// # Errors
    /// 39500 if etcd is unreachable.
    pub async fn watch(&self, from: Revision) -> Result<RegistryStream> {
        let prefix = self.records_prefix()?;
        let stream = self.dc.watch(&prefix, from).await?;
        Ok(Box::pin(stream.map(|ev| {
            ev.and_then(|ev| match ev {
                WatchEvent::Put(kv) => {
                    StoredCluster::decode(&kv).map(|s| RegistryChange::Put(Box::new(s)))
                }
                WatchEvent::Delete { key, mod_revision } => {
                    let id = key.as_str().rsplit('/').next().unwrap_or_default();
                    ClusterId::new(id)
                        .map(|cluster| RegistryChange::Deleted {
                            cluster,
                            mod_revision,
                        })
                        .map_err(|e| SprigError::internal(format!("bad cluster key {key}: {e}")))
                }
                WatchEvent::Progress(r) => Ok(RegistryChange::Progress(r)),
                WatchEvent::Compacted { compact_revision } => {
                    Ok(RegistryChange::Compacted(compact_revision))
                }
            })
        })))
    }
}

/// Every key under `prefix`, paging at one revision.
async fn list_all(store: &dyn MetaStore, prefix: &Key) -> Result<Vec<KeyValue>> {
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(prefix, opts.clone()).await?;
        let more = page.more;
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        out.extend(page.kvs);
        if !more {
            return Ok(out);
        }
    }
}

#[cfg(test)]
mod tests;
