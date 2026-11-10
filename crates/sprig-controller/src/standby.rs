//! The `standby` loop (chapter 25, "Standbys"; decision RC12; r2final):
//! each tenant homed in this DC keeps `failover.standby_dcs` standby DCs,
//! each a copy kept current by a chapter 10 `move_shard` job with
//! `Continuous` cutover in the standby DC's tier.
//!
//! ```text
//! per tenant homed here, on the DC leader, every 10 s and on directory changes:
//!   a standby recorded for the home DC itself (a promotion made it home) -> forgotten
//!   state isn't active (migrating, failing_over, ...)                    -> wait
//!   for each job standby Y (directory entry: standby_jobs.<Y>):
//!     Y outside the residency, the source moved (a move or split at home),
//!     or its job ended or vanished -> cancel the job (the copy is dropped),
//!                                     delete Y's standby maps, forget it
//!   want = failover.standby_dcs (tenant ctx at this DC's scopes)
//!   fewer: one new standby per pass:
//!     Y = an active, non-witness DC, not home, allowed by residency, holding
//!         no map of the tenant (no replica-cluster standby, no quarantined
//!         copy of an earlier home), preferring another `region` tag, then
//!         the lowest measured RTT from home (chapter 24's matrix), then id
//!     C = Y's active cluster with the lowest id that residency allows
//!     submit move_shard(home cluster -> C, Continuous) in Y's tier
//!     write Y's maps: the home maps with role standby on C
//!     directory: standby_jobs.<Y> = {cluster, source, job}; Y in standby_dcs
//!   more: drop the farthest
//! ```
//!
//! The promotion stops a job standby ([`crate::failover::nodes::ClusterHooks`]):
//! it waits until the job has applied the source's position when the
//! source still answers (a fenced home: nothing is lost), then stops the
//! job (`Mover::cut_over`, which ends a `Continuous` job `done` with its
//! target kept) and records `applied_source_gtid`. Its lag
//! ([`crate::failover::nodes::standby_lags`]) is `now − source_seen_ms +
//! lag_ms` while it streams, unbounded otherwise, so the RPO check stays
//! honest while a standby is still copying.
//!
//! **Where it differs from the book.** One job per tenant, not per unit:
//! R0's tenants are one placement unit (catalog and databases on one
//! cluster); tenants with table groups get no job standby yet (their
//! groups' maps would need one job per shard). Single-etcd mode only: the
//! loop writes the standby DC's tier through the one etcd, as moves
//! between DCs do; a two-tier deployment keeps replica-cluster standbys
//! (decision W3) until the job is submitted through the standby DC's own
//! leader. Every standby holds a worker slot while it streams.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::record::AdminState;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, JobId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_meta::{ListOpts, MetaStore, Op, Record, Tier, TierView, Txn};
use sprig_mover::{CutoverMode, JobSpec, JobState, JobStore, MetaMover, Mover};
use sprig_settings::{LiveSettings, SettingCtx};
use sprig_shard::home::{self, HomeEntry};
use sprig_shard::{Placement, TenantMaps};

use crate::failover::settings::STANDBY_DCS;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// How often each tenant is looked at.
pub const PASS: Duration = Duration::from_secs(10);

/// The directory entry's field holding job standbys, by DC.
pub const FIELD: &str = "standby_jobs";

/// One job standby, as the directory entry records it
/// (`standby_jobs.<dc>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStandby {
    /// The cluster in the standby DC holding the copy.
    pub cluster: ClusterId,
    /// The home cluster it streams from.
    pub source: ClusterId,
    /// The `Continuous` mover job, in the standby DC's tier.
    pub job: JobId,
    /// When the loop created it, ms since the epoch.
    #[serde(default)]
    pub created_ms: u64,
}

/// The job standbys of an entry, by DC.
#[must_use]
pub fn job_standbys(raw: &serde_json::Map<String, Value>) -> BTreeMap<DcId, JobStandby> {
    raw.get(FIELD)
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(dc, v)| {
                    Some((
                        DcId::new(dc).ok()?,
                        serde_json::from_value(v.clone()).ok()?,
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn standby_dcs(raw: &serde_json::Map<String, Value>) -> Vec<String> {
    raw.get("standby_dcs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

async fn del(tier: &dyn MetaStore, key: sprig_meta::Key) -> Result<()> {
    tier.txn(Txn {
        when: Vec::new(),
        then: vec![Op::Delete(key)],
        otherwise: Vec::new(),
    })
    .await?;
    Ok(())
}

/// The loop (see the module docs).
pub struct StandbyLoop {
    dc: DcId,
    /// The one etcd (single-etcd mode): every DC's tier is a view of it.
    store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    live: Option<LiveSettings>,
}

impl std::fmt::Debug for StandbyLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandbyLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl StandbyLoop {
    /// The loop of a controller in `dc`; `store` is the one etcd's DC tier,
    /// `global` the global tier.
    #[must_use]
    pub fn new(
        dc: DcId,
        store: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        live: Option<LiveSettings>,
    ) -> Self {
        Self {
            dc,
            store,
            global,
            clock,
            live,
        }
    }

    fn tier(&self, dc: &DcId) -> Arc<dyn MetaStore> {
        TierView::over(&self.store, Tier::Dc(dc.clone()))
    }

    fn mover_in(&self, dc: &DcId) -> MetaMover {
        MetaMover::new(JobStore::new(self.tier(dc), Arc::clone(&self.clock)))
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(
            self.clock
                .wall()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    fn want(&self, tenant: TenantId) -> usize {
        let n = self.live.as_ref().map_or(STANDBY_DCS.default, |l| {
            l.get(
                &STANDBY_DCS,
                &SettingCtx::process(&self.dc).for_tenant(tenant, &[]),
            )
        });
        usize::try_from(n).unwrap_or(0)
    }

    /// One tenant (see the module docs).
    ///
    /// # Errors
    /// The stores' errors.
    pub async fn reconcile_tenant(&self, tenant: TenantId) -> Result<Outcome> {
        let Some(e) = home::read_home(&*self.global, tenant).await? else {
            return Ok(Outcome::Done);
        };
        if e.home_dc != self.dc {
            return Ok(Outcome::Done);
        }
        let mut sbs = job_standbys(&e.raw);
        // Nothing while the tenant moves or fails over: a promotion stops
        // the standby in the new home itself.
        if e.state != "active" {
            return Ok(Outcome::RequeueAfter(PASS));
        }
        // A promotion made a standby DC the home: its job ended `done` and
        // its maps are the home's now. Forget it, touching nothing.
        if sbs.remove(&e.home_dc).is_some() {
            self.forget(tenant, &e.home_dc).await?;
        }
        let home_tier = self.tier(&self.dc);
        let js = JobStore::new(Arc::clone(&home_tier), Arc::clone(&self.clock));
        if !js.sharded_groups(tenant).await?.is_empty() {
            tracing::debug!(%tenant, "tenant with table groups: no job standby yet");
            return Ok(Outcome::RequeueAfter(PASS * 6));
        }
        let source = match TenantMaps::new(Arc::clone(&home_tier)).load(tenant).await {
            Ok((map, _)) => Placement::of(&map)?.cluster,
            Err(e) => {
                tracing::debug!(%tenant, error = %e, "no home map yet");
                return Ok(Outcome::RequeueAfter(PASS));
            }
        };
        // Existing standbys still valid?
        for (y, sb) in &sbs.clone() {
            let why = if !e.residency_allows(y) {
                Some("outside the tenant's residency".to_owned())
            } else if sb.source != source {
                Some(format!("the home cluster is {source} now, not {}", sb.source))
            } else {
                match self.mover_in(y).status(&sb.job).await {
                    Ok(st) if matches!(st.state, JobState::RolledBack | JobState::Done) => {
                        Some(format!("its job ended {}", st.state))
                    }
                    Ok(_) => None,
                    Err(err) => Some(format!("its job can't be read: {err}")),
                }
            };
            if let Some(why) = why {
                self.drop_standby(tenant, y, sb, &why).await?;
                sbs.remove(y);
            }
        }
        let want = self.want(tenant);
        if sbs.len() < want {
            if let Some((y, sb)) = self.create(tenant, &e, &source, &sbs).await? {
                sbs.insert(y, sb);
            }
        } else if sbs.len() > want {
            let matrix = crate::latency::Matrix::read(&*self.global).await.ok();
            let far = sbs
                .iter()
                .max_by(|a, b| {
                    let r = |d: &DcId| {
                        matrix
                            .as_ref()
                            .and_then(|m| m.rtt(self.dc.as_str(), d.as_str()))
                            .unwrap_or(f64::MAX)
                    };
                    r(a.0).total_cmp(&r(b.0)).then_with(|| a.0.cmp(b.0))
                })
                .map(|(d, s)| (d.clone(), s.clone()));
            if let Some((y, sb)) = far {
                self.drop_standby(tenant, &y, &sb, "more standbys than failover.standby_dcs")
                    .await?;
            }
        }
        Ok(Outcome::RequeueAfter(PASS))
    }

    /// The tenant's maps in `dc`'s tier.
    async fn maps_in(&self, dc: &DcId, tenant: TenantId) -> Result<Vec<(sprig_meta::Key, Record<ShardMap>)>> {
        let tier = self.tier(dc);
        let page = tier
            .list(
                &tier.tier().key(&format!("shardmaps/{tenant}/"))?,
                ListOpts::default(),
            )
            .await?;
        Ok(page
            .kvs
            .into_iter()
            .filter_map(|kv| Some((kv.key, Record::<ShardMap>::decode(&kv.value).ok()?)))
            .collect())
    }

    /// Picks a DC and a cluster and starts one standby; `None` when no DC
    /// fits.
    async fn create(
        &self,
        tenant: TenantId,
        e: &HomeEntry,
        source: &ClusterId,
        have: &BTreeMap<DcId, JobStandby>,
    ) -> Result<Option<(DcId, JobStandby)>> {
        let dcs = crate::dc::list(&*self.global).await?;
        let home_region = dcs
            .iter()
            .find(|d| d.dc_id == self.dc)
            .and_then(|d| d.tags.get("region").cloned());
        let matrix = crate::latency::Matrix::read(&*self.global).await.ok();
        let mut cands = Vec::new();
        for d in &dcs {
            if d.state != "active"
                || d.kind == "witness"
                || d.dc_id == self.dc
                || have.contains_key(&d.dc_id)
                || !e.residency_allows(&d.dc_id)
            {
                continue;
            }
            // A DC holding any map of the tenant (a replica-cluster
            // standby, a quarantined copy of an earlier home) gets no job.
            if !self.maps_in(&d.dc_id, tenant).await?.is_empty() {
                continue;
            }
            let same_region = home_region.is_some() && d.tags.get("region") == home_region.as_ref();
            let rtt = matrix
                .as_ref()
                .and_then(|m| m.rtt(self.dc.as_str(), d.dc_id.as_str()))
                .unwrap_or(f64::MAX);
            cands.push((same_region, rtt, d.dc_id.clone()));
        }
        cands.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.total_cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        let allowed = e.allowed_dcs();
        for (_, _, y) in cands {
            let reg = ClusterRegistry::new(Arc::clone(&self.global), self.tier(&y))?;
            let mut clusters: Vec<ClusterId> = reg
                .list()
                .await?
                .into_iter()
                .filter(|c| c.body().admin_state == AdminState::Active)
                .filter(|c| crate::residency::eligible(c.body(), allowed.as_ref()))
                .map(|c| c.body().cluster_id.clone())
                .collect();
            clusters.sort();
            let Some(target) = clusters.into_iter().next() else {
                continue;
            };
            return self.start(tenant, e, source, &y, &target).await.map(Some);
        }
        tracing::debug!(%tenant, "no DC can hold a standby now");
        Ok(None)
    }

    async fn start(
        &self,
        tenant: TenantId,
        e: &HomeEntry,
        source: &ClusterId,
        y: &DcId,
        target: &ClusterId,
    ) -> Result<(DcId, JobStandby)> {
        let mut spec = JobSpec::move_tenant(tenant, source.clone(), target.clone())
            .with_cutover(CutoverMode::Continuous);
        spec.requested_by = Some(format!("standby:{}", self.dc));
        let mover = self.mover_in(y);
        let job = mover.submit(spec).await?;
        // Y's maps: the home maps, role standby, on the target.
        let ytier = self.tier(y);
        let mut written = Vec::new();
        for (_, mut m) in self.maps_in(&self.dc, tenant).await? {
            if m.body.role != MapRole::Home {
                continue;
            }
            m.body.role = MapRole::Standby;
            for r in &mut m.body.ranges {
                r.cluster_id = target.clone();
            }
            let key = ytier
                .tier()
                .key(&format!("shardmaps/{tenant}/{}", m.body.group_id))?;
            ytier
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key: key.clone(),
                        value: m.encode()?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            written.push(key);
        }
        let sb = JobStandby {
            cluster: target.clone(),
            source: source.clone(),
            job,
            created_ms: self.now_ms(),
        };
        let (home_dc, epoch) = (e.home_dc.clone(), e.epoch);
        let rec = serde_json::to_value(&sb).map_err(|e| SprigError::internal(e.to_string()))?;
        let r = home::update_home(&*self.global, tenant, |cur| {
            if cur.home_dc != home_dc || cur.epoch != epoch || cur.state != "active" {
                return Err(SprigError::internal("the tenant changed meanwhile"));
            }
            let mut all = cur
                .raw
                .get(FIELD)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            all.insert(y.to_string(), rec.clone());
            cur.raw.insert(FIELD.into(), Value::Object(all));
            let mut list = standby_dcs(&cur.raw);
            if !list.iter().any(|d| d == y.as_str()) {
                list.push(y.to_string());
            }
            cur.raw.insert("standby_dcs".into(), json!(list));
            Ok(true)
        })
        .await;
        if let Err(err) = r {
            // Undo: the job (it drops its copy) and the maps.
            let _ = mover.cancel(&sb.job, "standby not recorded").await;
            for k in written {
                let _ = del(&*ytier, k).await;
            }
            return Err(err);
        }
        metrics::counter!("sprig_controller_standbys_total", "result" => "created").increment(1);
        tracing::info!(%tenant, dc = %y, cluster = %target, %source, job = %sb.job, "standby started");
        Ok((y.clone(), sb))
    }

    /// Cancels a standby's job (its copy is dropped), deletes its standby
    /// maps, and forgets it.
    async fn drop_standby(
        &self,
        tenant: TenantId,
        y: &DcId,
        sb: &JobStandby,
        why: &str,
    ) -> Result<()> {
        tracing::warn!(%tenant, dc = %y, job = %sb.job, why, "standby dropped");
        if let Err(e) = self.mover_in(y).cancel(&sb.job, why).await {
            tracing::debug!(%tenant, job = %sb.job, error = %e, "standby job not cancelled");
        }
        let ytier = self.tier(y);
        for (k, m) in self.maps_in(y, tenant).await? {
            if m.body.role == MapRole::Standby {
                del(&*ytier, k).await?;
            }
        }
        self.forget(tenant, y).await?;
        metrics::counter!("sprig_controller_standbys_total", "result" => "dropped").increment(1);
        Ok(())
    }

    /// Removes `y` from the entry's `standby_jobs` and `standby_dcs`.
    async fn forget(&self, tenant: TenantId, y: &DcId) -> Result<()> {
        home::update_home(&*self.global, tenant, |cur| {
            let mut changed = false;
            if let Some(Value::Object(m)) = cur.raw.get_mut(FIELD) {
                changed |= m.remove(y.as_str()).is_some();
            }
            let list = standby_dcs(&cur.raw);
            let keep: Vec<String> = list.iter().filter(|d| *d != y.as_str()).cloned().collect();
            if keep.len() != list.len() {
                cur.raw.insert("standby_dcs".into(), json!(keep));
                changed = true;
            }
            Ok(changed)
        })
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Reconciler for StandbyLoop {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "standby"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        crate::tree::Directory::new(Arc::clone(&self.global))
            .tenant_of(&ev.key)
            .into_iter()
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        let dir = crate::tree::Directory::new(Arc::clone(&self.global));
        let keys: BTreeSet<TenantId> = dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| e.home_dc == self.dc)
            .map(|e| e.tenant_id)
            .collect();
        Ok(keys.into_iter().collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        self.reconcile_tenant(*key).await
    }
}

#[cfg(test)]
mod tests;
