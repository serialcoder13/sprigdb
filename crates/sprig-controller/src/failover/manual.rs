//! Manual DC failover and standby set-up: what `sprigctl tenant failover` and `sprigctl tenant
//! standby` call.
//!
//! ```text
//! failover(T, Y, accept_data_loss, old_home_fenced, dry_run):
//! 1. X = T's home; Y != X, Y active, T not pinned to X       else 39505
//! 2. Y holds every unit: a standby map of T in Y's tier       else 39505
//! 3. X fenced: dc-leases/X is gone, or old_home_fenced (audited)
//!    else 39505, suggesting `sprigctl tenant move` (X is healthy)
//! 4. the lag estimate (Y's standby); any possible loss needs
//!    accept_data_loss                                          else 39505
//! 5. dry_run: the plan only
//! 6. a failover record (manual), then the same promote() as the global
//!    decision; with old_home_fenced while X still holds its lease, X's
//!    maps of T are quarantined too, so X's gateways follow T to Y
//! ```
//!
//! With two sites this is the only DC failover. The old home returns
//! through `returning` as after an automatic failover, which fences its
//! copy of T and reports its diverged transactions.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::{DcRecord, rfc3339};
use sprig_meta::dclease::{dc_key, lease_key};
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Tier, Txn};
use sprig_shard::home::{directory_key, read_home};

use super::nodes::standby_lags;
use super::promote::{Guard, Promoted, promote};
use super::record::{ErrantReport, FailoverRecord, FailoverState, TenantOutcome, failover_key};
use super::role::FailoverRole;
use crate::tree::Directory;

/// `POST /admin/v1/tenants/{name}:failover`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverRequest {
    /// The new home.
    pub to: DcId,
    /// Accept losing what the standby hadn't applied.
    #[serde(default)]
    pub accept_data_loss: bool,
    /// The operator asserts the old home is fenced (audited).
    #[serde(default)]
    pub old_home_fenced: bool,
    /// Check and plan only.
    #[serde(default)]
    pub dry_run: bool,
}

/// `POST /admin/v1/failovers:all`: every tenant of `from` with a standby
/// in `to`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverAllRequest {
    /// The lost DC.
    pub from: DcId,
    /// The new home.
    pub to: DcId,
    /// As [`FailoverRequest`].
    #[serde(default)]
    pub accept_data_loss: bool,
    /// As [`FailoverRequest`].
    #[serde(default)]
    pub old_home_fenced: bool,
    /// As [`FailoverRequest`].
    #[serde(default)]
    pub dry_run: bool,
}

/// What a manual failover did, or would do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverView {
    /// The tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: u64,
    /// The old home.
    pub from: DcId,
    /// The new home.
    pub to: DcId,
    /// The standby's lag; `None` when unknown.
    pub lag_ms: Option<u32>,
    /// `lease` (the old home's DC lease is gone) or `operator`.
    pub old_home_fenced_by: String,
    /// `planned` (dry run), `promoted` or `skipped`.
    pub outcome: String,
    /// The failover record.
    pub failover_id: Option<String>,
    /// The new directory epoch.
    pub epoch: Option<u64>,
    /// Positions the standby had applied, per source.
    #[serde(default)]
    pub applied: BTreeMap<String, String>,
    /// Why it was skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// `POST /admin/v1/tenants/{name}:standby`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandbyRequest {
    /// The standby DC.
    pub dc: DcId,
    /// Its cluster that replicates the home cluster.
    pub cluster: ClusterId,
}

/// A tenant's failovers and their diverged-transaction reports
/// (`sprigctl tenant failover --errant`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrantView {
    /// The tenant.
    pub tenant: String,
    /// Per failover that moved it: id, from, to, outcome, report.
    pub failovers: Vec<ErrantEntry>,
}

/// One failover of [`ErrantView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrantEntry {
    /// The record.
    pub failover_id: String,
    /// The DC it failed over from.
    pub dc_id: DcId,
    /// The record's state.
    pub state: FailoverState,
    /// The tenant's outcome.
    pub outcome: String,
    /// The report, once the old home returned.
    pub errant: Option<ErrantReport>,
}

fn refused(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::FailoverRefused, msg)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

async fn has_role(tier: &dyn MetaStore, tenant: TenantId, role: MapRole) -> Result<bool> {
    let prefix = tier.tier().key(&format!("shardmaps/{tenant}/"))?;
    let page = tier.list(&prefix, ListOpts::default()).await?;
    Ok(page
        .kvs
        .iter()
        .any(|kv| Record::<ShardMap>::decode(&kv.value).is_ok_and(|m| m.body.role == role)))
}

impl FailoverRole {
    fn global_store(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.stores().global)
    }

    /// Manual failover of the tenant named `name` (see the module docs).
    ///
    /// # Errors
    /// 39072 for an unknown tenant; 39505 when a check refuses; the
    /// stores' and the nodes' errors.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the checks, in order
    pub async fn failover_tenant(&self, name: &str, req: &FailoverRequest) -> Result<FailoverView> {
        let global = self.global_store();
        let entry = Directory::new(Arc::clone(&global)).require(name).await?;
        let tenant = entry.body().tenant_id;
        let e = read_home(&*global, tenant)
            .await?
            .ok_or_else(|| refused(format!("tenant {name} has no directory entry")))?;
        let (x, y) = (e.home_dc.clone(), req.to.clone());
        // 1.
        if x == y {
            return Err(refused(format!("tenant {name} is homed in {y} already")));
        }
        if e.state != "active" && e.state != "failing_over" {
            return Err(refused(format!(
                "tenant {name} is {}: only an active tenant fails over",
                e.state
            )));
        }
        if e.pinned_dc.as_ref() == Some(&x) {
            return Err(refused(format!(
                "tenant {name} is pinned to {x}: it never leaves its data center (sprigctl tenant unpin first)"
            )));
        }
        if !e.residency_allows(&y) {
            return Err(refused(format!(
                "tenant {name}'s residency doesn't allow {y}: it never leaves its allowed data centers"
            )));
        }
        let ydc = global
            .get(&dc_key(&y)?, Consistency::Linearizable)
            .await?
            .map(|kv| Record::<DcRecord>::decode(&kv.value))
            .transpose()?
            .ok_or_else(|| refused(format!("data center {y} isn't registered")))?;
        if ydc.body.state != "active" {
            return Err(refused(format!(
                "data center {y} is {}: a tenant fails over only to an active one",
                ydc.body.state
            )));
        }
        // 2.
        let ytier = (self.dc_tiers())(&y);
        let holds = has_role(&*ytier, tenant, MapRole::Standby).await?
            || has_role(&*ytier, tenant, MapRole::Home).await?;
        if !holds {
            return Err(refused(format!(
                "{y} holds no standby of tenant {name} (sprigctl tenant standby {name} --dc {y} --cluster ...)"
            )));
        }
        // 3.
        let lease_gone = global
            .get(&lease_key(&x)?, Consistency::Linearizable)
            .await?
            .is_none();
        let fenced_by = if lease_gone {
            "lease"
        } else if req.old_home_fenced {
            "operator"
        } else {
            return Err(refused(format!(
                "the old home {x} holds its DC lease (it is healthy): use sprigctl tenant move {name} --to {y}, \
                 or pass --old-home-fenced if you know {x} is fenced"
            )));
        };
        // 4.
        let lags = standby_lags(&global, &ytier).await?;
        let lag = lags
            .iter()
            .find(|l| l.tenant_id == tenant.0)
            .map(|l| l.lag_ms);
        let lag_ms = lag.filter(|l| *l != u32::MAX);
        if lag_ms != Some(0) && !req.accept_data_loss {
            return Err(refused(format!(
                "the standby in {y} may be missing writes (lag {}): pass --accept-data-loss",
                lag_ms.map_or_else(|| "unknown".to_owned(), |l| format!("{l} ms"))
            )));
        }
        let mut view = FailoverView {
            tenant: name.to_owned(),
            tenant_id: tenant.0,
            from: x.clone(),
            to: y.clone(),
            lag_ms,
            old_home_fenced_by: fenced_by.to_owned(),
            outcome: "planned".into(),
            failover_id: None,
            epoch: None,
            applied: BTreeMap::new(),
            reason: None,
        };
        // 5.
        if req.dry_run {
            return Ok(view);
        }
        sprig_common::audit::record(
            sprig_common::audit::AuditStream::Tenant(tenant),
            "admin.tenant_failover",
            json!({"tenant": name, "from": x.as_str(), "to": y.as_str(), "old_home_fenced_by": fenced_by,
                   "accept_data_loss": req.accept_data_loss, "lag_ms": lag_ms}),
        );
        tracing::warn!(tenant = name, from = %x, to = %y, fenced_by, accept_data_loss = req.accept_data_loss, "manual tenant failover");
        // 6.
        let id = format!("fo-{}-{x}-manual-{tenant}", now_ms());
        let mut f = FailoverRecord {
            failover_id: id.clone(),
            dc_id: x.clone(),
            state: FailoverState::Promoting,
            lease_lost_at: rfc3339(SystemTime::now()),
            votes: Vec::new(),
            tenants: vec![TenantOutcome {
                tenant_id: tenant.0,
                outcome: "planned".into(),
                to: Some(y.clone()),
                lag_ms,
                reason: Some(format!("manual; old home fenced by {fenced_by}")),
                epoch: Some(e.epoch),
                applied: BTreeMap::new(),
                errant: None,
            }],
            reason: Some("sprigctl tenant failover".into()),
        };
        put(&*global, &f).await?;
        let p = self.promotion(Guard::Store(Arc::clone(&global)));
        let epoch = if e.state == "failing_over" && e.home_dc == y {
            e.epoch.saturating_sub(1)
        } else {
            e.epoch
        };
        let r = promote(&p, &id, tenant, &x, &y, epoch).await;
        let t = &mut f.tenants[0];
        match &r {
            Ok(Promoted::Done { epoch, applied }) => {
                t.outcome = "promoted".into();
                t.epoch = Some(*epoch);
                t.applied.clone_from(applied);
                view.outcome = "promoted".into();
                view.epoch = Some(*epoch);
                view.applied.clone_from(applied);
            }
            Ok(Promoted::Skipped(why)) => {
                t.outcome = "skipped".into();
                t.reason = Some(why.clone());
                view.outcome = "skipped".into();
                view.reason = Some(why.clone());
            }
            Ok(Promoted::Committed { epoch }) => {
                // Not reached: a manual failover runs in the target DC.
                t.outcome = "committed".into();
                t.epoch = Some(*epoch);
                view.outcome = "committed".into();
                view.epoch = Some(*epoch);
            }
            Err(_) => {}
        }
        f.state = FailoverState::Done;
        view.failover_id = Some(id);
        put(&*global, &f).await?;
        r?;
        // The old home's maps were quarantined by the promotion's first data
        // step (`ClusterHooks`), so its gateways follow the tenant to `y`
        // even when the operator vouched for the fence.
        metrics::counter!("sprig_controller_dc_failovers_total", "result" => "manual").increment(1);
        Ok(view)
    }

    /// Manual failover of every tenant of `req.from` that has a standby in
    /// `req.to`; tenants a check refuses are reported as skipped.
    ///
    /// # Errors
    /// The stores' errors.
    pub async fn failover_all(&self, req: &FailoverAllRequest) -> Result<Vec<FailoverView>> {
        let global = self.global_store();
        let mut out = Vec::new();
        for s in Directory::new(Arc::clone(&global)).list().await? {
            let b = s.body();
            if b.home_dc != req.from {
                continue;
            }
            let one = FailoverRequest {
                to: req.to.clone(),
                accept_data_loss: req.accept_data_loss,
                old_home_fenced: req.old_home_fenced,
                dry_run: req.dry_run,
            };
            match self.failover_tenant(&b.name, &one).await {
                Ok(v) => out.push(v),
                Err(e) if e.code() == Some(SprigCode::FailoverRefused) => out.push(FailoverView {
                    tenant: b.name.clone(),
                    tenant_id: b.tenant_id.0,
                    from: req.from.clone(),
                    to: req.to.clone(),
                    lag_ms: None,
                    old_home_fenced_by: String::new(),
                    outcome: "skipped".into(),
                    failover_id: None,
                    epoch: None,
                    applied: BTreeMap::new(),
                    reason: Some(e.message.clone()),
                }),
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// The tenant's failovers with their diverged-transaction reports.
    ///
    /// # Errors
    /// 39072 for an unknown tenant; the store's errors.
    pub async fn errant(&self, name: &str) -> Result<ErrantView> {
        let global = self.global_store();
        let tenant = Directory::new(Arc::clone(&global))
            .require(name)
            .await?
            .body()
            .tenant_id;
        let mut failovers = Vec::new();
        for f in list_records(&*global).await? {
            if let Some(t) = f.tenants.iter().find(|t| t.tenant_id == tenant.0) {
                failovers.push(ErrantEntry {
                    failover_id: f.failover_id.clone(),
                    dc_id: f.dc_id.clone(),
                    state: f.state,
                    outcome: t.outcome.clone(),
                    errant: t.errant.clone(),
                });
            }
        }
        Ok(ErrantView {
            tenant: name.to_owned(),
            failovers,
        })
    }

    /// Records a standby of the tenant named `name`: its maps mirrored in
    /// `req.dc`'s tier with role `standby` on `req.cluster`, and `req.dc`
    /// added to the directory entry's `standby_dcs`. The cluster must
    /// replicate the home cluster (the operator sets that up).
    ///
    /// # Errors
    /// 39505 for the home DC or an unknown cluster; the stores' errors.
    #[allow(clippy::too_many_lines)] // maps, then the directory entry
    pub async fn add_standby(&self, name: &str, req: &StandbyRequest) -> Result<serde_json::Value> {
        let global = self.global_store();
        let tenant = Directory::new(Arc::clone(&global))
            .require(name)
            .await?
            .body()
            .tenant_id;
        let e = read_home(&*global, tenant)
            .await?
            .ok_or_else(|| refused(format!("tenant {name} has no directory entry")))?;
        if e.home_dc == req.dc {
            return Err(refused(format!(
                "{} is tenant {name}'s home, not a standby",
                req.dc
            )));
        }
        if !e.residency_allows(&req.dc) {
            return Err(refused(format!(
                "tenant {name}'s residency doesn't allow a copy in {}",
                req.dc
            )));
        }
        let tiers = self.dc_tiers();
        let (home, standby) = (tiers(&e.home_dc), tiers(&req.dc));
        if super::nodes::cluster(&global, &standby, &req.cluster)
            .await?
            .is_none()
        {
            return Err(refused(format!(
                "cluster {} isn't registered in {}",
                req.cluster, req.dc
            )));
        }
        let page = home
            .list(
                &home.tier().key(&format!("shardmaps/{tenant}/"))?,
                ListOpts::default(),
            )
            .await?;
        let mut maps = 0;
        for kv in page.kvs {
            let mut m = Record::<ShardMap>::decode(&kv.value)?;
            if m.body.role != MapRole::Home {
                continue;
            }
            m.body.role = MapRole::Standby;
            for r in &mut m.body.ranges {
                r.cluster_id = req.cluster.clone();
            }
            let key = standby
                .tier()
                .key(&format!("shardmaps/{tenant}/{}", m.body.group_id))?;
            standby
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key,
                        value: m.encode()?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            maps += 1;
        }
        // standby_dcs in the directory entry, guarded by its revision.
        loop {
            let e = read_home(&*global, tenant)
                .await?
                .ok_or_else(|| refused(format!("tenant {name} has no directory entry")))?;
            let mut list: Vec<String> = e
                .raw
                .get("standby_dcs")
                .and_then(serde_json::Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            if list.iter().any(|d| d == req.dc.as_str()) {
                break;
            }
            list.push(req.dc.to_string());
            let mut raw = e.raw.clone();
            raw.insert("standby_dcs".into(), json!(list));
            let key = directory_key(tenant)?;
            let r = global
                .txn(Txn {
                    when: vec![Cmp::ModRev(key.clone(), e.rev)],
                    then: vec![Op::Put {
                        key,
                        value: serde_json::to_vec(&serde_json::Value::Object(raw))
                            .map_err(|e| SprigError::internal(e.to_string()))?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                break;
            }
        }
        tracing::info!(tenant = name, dc = %req.dc, cluster = %req.cluster, maps, "standby recorded");
        Ok(
            json!({"tenant": name, "tenant_id": tenant.0, "dc": req.dc.as_str(), "cluster": req.cluster.as_str(), "maps": maps}),
        )
    }

    /// Every failover record, newest first.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn failovers(&self) -> Result<Vec<FailoverRecord>> {
        let mut v = list_records(&*self.global_store()).await?;
        v.reverse();
        Ok(v)
    }
}

async fn list_records(global: &dyn MetaStore) -> Result<Vec<FailoverRecord>> {
    let page = global
        .list(&Tier::Global.key("failovers/")?, ListOpts::default())
        .await?;
    let mut v: Vec<FailoverRecord> = page
        .kvs
        .iter()
        .filter_map(|kv| Record::<FailoverRecord>::decode(&kv.value).ok())
        .map(|r| r.body)
        .collect();
    v.sort_by(|a, b| a.failover_id.cmp(&b.failover_id));
    Ok(v)
}

async fn put(global: &dyn MetaStore, f: &FailoverRecord) -> Result<()> {
    global
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key: failover_key(&f.failover_id)?,
                value: Record::new(f.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use serde_json::Map;
    use sprig_common::time::SystemClock;
    use sprig_meta::{MemStore, TierView};

    use super::*;
    use crate::runtime::Stores;
    use crate::tree::{NewTenant, TenantState};

    fn dc(s: &str) -> DcId {
        DcId::new(s).unwrap()
    }

    async fn put_raw(store: &dyn MetaStore, key: sprig_meta::Key, value: Vec<u8>) {
        store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key,
                    value,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await
            .unwrap();
    }

    /// ams1's controller; tenant `acme` homed in fra1, its map in fra1's
    /// tier; `with_standby` adds its standby map in ams1's tier.
    async fn setup(with_standby: bool) -> (Arc<FailoverRole>, Arc<dyn MetaStore>, TenantId) {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("ams1"))));
        let global = TierView::over(&mem, Tier::Global);
        for d in ["fra1", "ams1"] {
            let rec = Record::new(DcRecord {
                dc_id: dc(d),
                kind: "full".into(),
                state: "active".into(),
                dc_epoch: 1,
                tags: Map::new(),
                global_voter: true,
            });
            put_raw(
                &*global,
                sprig_meta::dclease::dc_key(&dc(d)).unwrap(),
                rec.encode().unwrap(),
            )
            .await;
        }
        let dir = Directory::new(Arc::clone(&global));
        let e = dir
            .create(
                &NewTenant {
                    name: "acme".into(),
                    parent: None,
                    home_dc: dc("fra1"),
                    placement_hint: None,
                },
                64,
                SystemTime::now(),
            )
            .await
            .unwrap();
        let t = e.body().tenant_id;
        dir.update(t, |b| {
            b.state = TenantState::Active;
            Ok(true)
        })
        .await
        .unwrap();
        let home = TierView::over(&mem, Tier::Dc(dc("fra1")));
        let map = ShardMap::catalog(t, ClusterId::new("fra1-c1").unwrap(), 1);
        put_raw(
            &*home,
            ShardMap::catalog_key(home.tier(), t).unwrap(),
            Record::new(map.clone()).encode().unwrap(),
        )
        .await;
        if with_standby {
            let mut sb = map;
            sb.role = MapRole::Standby;
            sb.ranges[0].cluster_id = ClusterId::new("ams1-c2").unwrap();
            let tier = TierView::over(&mem, Tier::Dc(dc("ams1")));
            put_raw(
                &*tier,
                ShardMap::catalog_key(tier.tier(), t).unwrap(),
                Record::new(sb).encode().unwrap(),
            )
            .await;
        }
        let role = FailoverRole::new(
            dc("ams1"),
            "ctl-ams1".into(),
            Stores {
                dc: Arc::clone(&mem),
                global: Arc::clone(&global),
            },
            Arc::new(SystemClock),
            None,
            true,
        );
        (role, global, t)
    }

    fn req(accept: bool, fenced: bool) -> FailoverRequest {
        FailoverRequest {
            to: dc("ams1"),
            accept_data_loss: accept,
            old_home_fenced: fenced,
            dry_run: true,
        }
    }

    fn code(r: &Result<FailoverView>) -> Option<SprigCode> {
        r.as_ref().err().and_then(SprigError::code)
    }

    #[tokio::test]
    async fn manual_failover_checks_refuse_in_order() {
        // No standby in ams1.
        let (role, _, _) = setup(false).await;
        let r = role.failover_tenant("acme", &req(true, true)).await;
        assert_eq!(code(&r), Some(SprigCode::FailoverRefused));
        assert!(r.unwrap_err().message.contains("no standby"));

        let (role, global2, t) = setup(true).await;
        // fra1 holds its DC lease: healthy, use `tenant move`.
        let lease = global2.grant_lease(Duration::from_secs(30)).await.unwrap();
        global2
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key: lease_key(&dc("fra1")).unwrap(),
                    value: b"{\"v\":1}".to_vec(),
                    lease: Some(lease),
                }],
                otherwise: Vec::new(),
            })
            .await
            .unwrap();
        let r = role.failover_tenant("acme", &req(true, false)).await;
        assert!(
            r.as_ref().unwrap_err().message.contains("tenant move"),
            "{r:?}"
        );
        // Vouched for, but the standby's lag is unknown (no cluster to ask).
        let r = role.failover_tenant("acme", &req(false, true)).await;
        assert!(
            r.as_ref()
                .unwrap_err()
                .message
                .contains("--accept-data-loss"),
            "{r:?}"
        );
        // Both flags: the dry run plans it.
        let v = role
            .failover_tenant("acme", &req(true, true))
            .await
            .unwrap();
        assert_eq!(
            (v.outcome.as_str(), v.old_home_fenced_by.as_str()),
            ("planned", "operator")
        );
        assert_eq!(v.tenant_id, t.0);
        // The lease gone: fenced by the lease, no flag needed.
        global2.revoke_lease(lease).await.unwrap();
        let v = role
            .failover_tenant("acme", &req(true, false))
            .await
            .unwrap();
        assert_eq!(v.old_home_fenced_by, "lease");
        // Pinned to its home: never.
        let mut e = read_home(&*global2, t).await.unwrap().unwrap();
        e.raw.insert("pinned_dc".into(), json!("fra1"));
        put_raw(
            &*global2,
            directory_key(t).unwrap(),
            serde_json::to_vec(&e.raw).unwrap(),
        )
        .await;
        let r = role.failover_tenant("acme", &req(true, true)).await;
        assert!(r.as_ref().unwrap_err().message.contains("pinned"), "{r:?}");
    }
}
