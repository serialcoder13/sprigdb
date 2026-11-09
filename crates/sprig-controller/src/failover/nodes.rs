//! The MariaDB side of DC failover in the controller role (chapter 25):
//! what the libraries leave to the deployment.
//!
//! - [`ClusterHooks`]: promotion's data-path steps 2 and 4 for standbys
//!   that are replicas of the home cluster: stop the replication after
//!   the relay log is applied, record the position, then make the copy a
//!   primary;
//! - [`GrantFence`]: the returning DC's fence of a moved tenant's copy:
//!   write privileges on its schemas revoked (not logged to the binlog)
//!   and its sessions killed;
//! - [`standby_lags`]: what this DC's observer reports for the standbys it
//!   holds;
//! - [`report_diverged`]: the diverged-transaction report of a returning
//!   copy, written into the failover record.
//!
//! **Where it differs from the book.** A standby here is a cluster in the
//! standby DC that replicates the home cluster (set up by the operator,
//! `sprigctl tenant standby`), not chapter 10's `Continuous` mover jobs per
//! unit, which aren't built. Every tenant on such a cluster moves with it:
//! promoting one stops the replication for all of them, and the others'
//! standby lag then grows until they are promoted too or set up again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::failover::primary_replica::drain::stop_drained;
use sprig_cluster::failover::primary_replica::{diverged, diverged_events};
use sprig_cluster::record::ClusterRecord;
use sprig_cluster::registry::ClusterRegistry;
use sprig_cluster::sql::{NodeEndpoint, ProbeSession, TextRow, literal};
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_meta::{Cmp, ListOpts, MetaStore, Op, Record, Tier, Txn};
use sprig_shard::home::read_home;

use super::promote::{DcTiers, PromoteHooks, quarantine_maps};
use super::record::{FailoverRecord, StandbyLag, failover_key};
use super::recovery::{CopyFence, errant_report};

/// Per statement on a node.
const NODE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a standby may take to apply its relay log before it stops.
const STANDBY_DRAIN: Duration = Duration::from_secs(30);

/// Accounts a fence never touches: SprigDB's own and MariaDB's.
const SYSTEM_USERS: [&str; 6] = [
    "system user",
    "event_scheduler",
    "sprig_ctl",
    "sprig_repl",
    "root",
    "mariadb.sys",
];

/// Database-level privileges that change data or objects.
const WRITE_PRIVS: &str = "INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, \
                           CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER, LOCK TABLES, \
                           CREATE TEMPORARY TABLES, DELETE HISTORY";
/// Table-level privileges that change data or objects.
const TABLE_WRITE_PRIVS: &str =
    "INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, TRIGGER, DELETE HISTORY";

fn col<'a>(r: &'a TextRow, name: &str) -> Option<&'a str> {
    r.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

fn node_err(e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(e.to_string())
}

/// `sprig_ctl` endpoints of every node of `rec`.
///
/// # Errors
/// 39998 for a credential reference that can't be resolved or a bad node
/// address.
pub fn ctl_endpoints(rec: &ClusterRecord) -> Result<Vec<NodeEndpoint>> {
    let cred = sprig_provision::credentials::resolve(&rec.secrets.ctl, "sprig_ctl")?;
    rec.nodes.iter().map(|n| cred.endpoint(n)).collect()
}

/// The cluster's nodes as `sprig_repl` sees them (binlog dumps:
/// `errant_extract`, chapter 25).
///
/// # Errors
/// The credentials can't be resolved or an address isn't `host:port`.
pub fn repl_endpoints(rec: &ClusterRecord) -> Result<Vec<NodeEndpoint>> {
    let cred = sprig_provision::credentials::resolve(&rec.secrets.repl, "sprig_repl")?;
    rec.nodes.iter().map(|n| cred.endpoint(n)).collect()
}

/// The cluster record of `id` (any DC: other DCs' tiers are read through
/// the registry, single-etcd mode).
///
/// # Errors
/// The store's errors.
pub async fn cluster(
    global: &Arc<dyn MetaStore>,
    dc_tier: &Arc<dyn MetaStore>,
    id: &ClusterId,
) -> Result<Option<ClusterRecord>> {
    let reg = ClusterRegistry::new(Arc::clone(global), Arc::clone(dc_tier))?;
    Ok(reg.get(id).await?.map(|s| s.body().clone()))
}

/// The tenant's maps in `tier` with `role`, and the clusters they name.
async fn maps_with(
    tier: &dyn MetaStore,
    tenant: TenantId,
    role: MapRole,
) -> Result<BTreeSet<ClusterId>> {
    let prefix = tier.tier().key(&format!("shardmaps/{tenant}/"))?;
    let page = tier.list(&prefix, ListOpts::default()).await?;
    let mut out = BTreeSet::new();
    for kv in page.kvs {
        let Ok(m) = Record::<ShardMap>::decode(&kv.value) else {
            continue;
        };
        if m.body.role == role {
            out.extend(m.body.ranges.iter().map(|r| r.cluster_id.clone()));
        }
    }
    Ok(out)
}

/// Promotion's data path for replica standbys (see the module docs).
pub struct ClusterHooks {
    /// The global tier.
    pub global: Arc<dyn MetaStore>,
    /// A DC's tier.
    pub dc_tier: DcTiers,
}

impl std::fmt::Debug for ClusterHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHooks").finish_non_exhaustive()
    }
}

impl ClusterHooks {
    async fn standby_nodes(
        &self,
        tenant: TenantId,
        to: &DcId,
    ) -> Result<Vec<(ClusterId, NodeEndpoint)>> {
        let tier = (self.dc_tier)(to);
        // Before step 3 the maps are `standby`; on a resume they may be
        // `home` already.
        let mut ids = maps_with(&*tier, tenant, MapRole::Standby).await?;
        ids.extend(maps_with(&*tier, tenant, MapRole::Home).await?);
        let mut out = Vec::new();
        for id in ids {
            let Some(rec) = cluster(&self.global, &tier, &id).await? else {
                return Err(SprigError::internal(format!(
                    "standby cluster {id} of tenant {tenant} has no record in {to}"
                )));
            };
            for ep in ctl_endpoints(&rec)? {
                out.push((id.clone(), ep));
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl PromoteHooks for ClusterHooks {
    async fn stop_standby(&self, tenant: TenantId, to: &DcId) -> Result<BTreeMap<String, String>> {
        // First, right after the commit point: the old home's maps become
        // `quarantined`, so every gateway still routing the tenant there
        // (its own DC's, and other DCs' following a remote home) reads the
        // directory again and follows the new home. Reachable in
        // single-etcd mode even while the old home is cut off.
        if let Some(e) = read_home(&*self.global, tenant).await?
            && let Some(from) = e
                .raw
                .get("failover")
                .and_then(|f| f.get("from"))
                .and_then(serde_json::Value::as_str)
                .and_then(|d| DcId::new(d).ok())
            && from != *to
        {
            let tier = (self.dc_tier)(&from);
            if *tier.tier() == Tier::Dc(from.clone()) {
                let n = quarantine_maps(&*tier, tenant).await?;
                if n > 0 {
                    tracing::warn!(%tenant, dc = %from, maps = n, "old home's maps quarantined");
                }
            }
        }
        let mut applied = BTreeMap::new();
        for (id, ep) in self.standby_nodes(tenant, to).await? {
            let mut s = ProbeSession::connect(&ep, NODE_TIMEOUT)
                .await
                .map_err(node_err)?;
            let status = s.rows("SHOW SLAVE STATUS").await.map_err(node_err)?;
            let Some(st) = status.first() else {
                // Not replicating (a resumed promotion, or a copy kept
                // current another way): its own position.
                let pos = s
                    .scalar("SELECT @@gtid_slave_pos")
                    .await
                    .map_err(node_err)?
                    .unwrap_or_default();
                applied.entry(id.to_string()).or_insert(pos);
                s.close().await;
                continue;
            };
            let source = format!(
                "{}:{}",
                col(st, "Master_Host").unwrap_or_default(),
                col(st, "Master_Port").unwrap_or_default()
            );
            // The relay log is applied before the copy stops, without ever
            // starting the SQL thread while the IO thread is stopped
            // (MariaDB would discard the relay log; r2drain). A copy that
            // can't apply it stops the promotion here, nothing reset.
            let pos = match stop_drained(&mut s, STANDBY_DRAIN)
                .await
                .map_err(node_err)?
            {
                Ok(st) => st.slave_pos,
                Err(why) => {
                    s.close().await;
                    return Err(SprigError::new(
                        sprig_common::SprigCode::FailoverBlocked,
                        format!("standby {id} of tenant {tenant}: {why}"),
                    ));
                }
            };
            s.close().await;
            tracing::info!(%tenant, cluster = %id, %source, %pos, "standby replication stopped");
            applied.insert(source, pos);
        }
        Ok(applied)
    }

    async fn make_writable(&self, tenant: TenantId, to: &DcId) -> Result<()> {
        for (id, ep) in self.standby_nodes(tenant, to).await? {
            let mut s = ProbeSession::connect(&ep, NODE_TIMEOUT)
                .await
                .map_err(node_err)?;
            s.exec("RESET SLAVE ALL").await.map_err(node_err)?;
            // A primary acknowledges commits once a replica has them, when
            // it has semi-sync replicas (chapter 25); harmless without.
            let _ = s.exec("SET GLOBAL rpl_semi_sync_master_enabled = ON").await;
            s.close().await;
            // `read_only` clears through the node's agent: it holds the
            // writer lease and a permit, and the node no longer replicates.
            tracing::info!(%tenant, cluster = %id, "standby is a primary now");
        }
        Ok(())
    }
}

/// The returning DC's fence of a moved tenant's copy (see the module
/// docs). Reads still work, so the copy can be inspected.
pub struct GrantFence {
    /// The global tier.
    pub global: Arc<dyn MetaStore>,
    /// This DC's tier.
    pub tier: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for GrantFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrantFence").finish_non_exhaustive()
    }
}

/// `Db` of `mysql.db` (a pattern, `\_` escaped) names the tenant's schemas.
fn tenant_db(db: &str, tenant: TenantId) -> bool {
    db.replace("\\_", "_")
        .strip_prefix(&format!("t{tenant}_"))
        .is_some()
}

/// Fences `tenant` on one node; returns (privilege rows revoked, sessions
/// killed).
///
/// # Errors
/// The node couldn't be reached or a statement failed.
#[allow(clippy::many_single_char_names)] // user, host, db, table
pub async fn fence_tenant_on(ep: &NodeEndpoint, tenant: TenantId) -> Result<(usize, usize)> {
    let mut s = ProbeSession::connect(ep, NODE_TIMEOUT)
        .await
        .map_err(node_err)?;
    // The fence is this copy's own: never replicated.
    s.exec("SET SESSION sql_log_bin = 0")
        .await
        .map_err(node_err)?;
    let like = literal(&format!("t{tenant}%"));
    let mut users: BTreeSet<(String, String)> = BTreeSet::new();
    let mut revoked = 0;
    let dbs = s
        .rows(&format!(
            "SELECT User, Host, Db FROM mysql.db WHERE Db LIKE {like}"
        ))
        .await
        .map_err(node_err)?;
    for r in &dbs {
        let (Some(u), Some(h), Some(db)) = (col(r, "User"), col(r, "Host"), col(r, "Db")) else {
            continue;
        };
        if SYSTEM_USERS.contains(&u) || !tenant_db(db, tenant) {
            continue;
        }
        let db_q = db.replace('`', "``");
        let sql = format!(
            "REVOKE {WRITE_PRIVS} ON `{db_q}`.* FROM {}@{}",
            literal(u),
            literal(h)
        );
        match s.exec(&sql).await {
            Ok(()) => revoked += 1,
            Err(e) => tracing::debug!(%tenant, error = %e, "revoke at database level"),
        }
        users.insert((u.to_owned(), h.to_owned()));
    }
    let tables = s
        .rows(&format!(
            "SELECT User, Host, Db, Table_name FROM mysql.tables_priv WHERE Db LIKE {like}"
        ))
        .await
        .map_err(node_err)?;
    for r in &tables {
        let (Some(u), Some(h), Some(db), Some(t)) = (
            col(r, "User"),
            col(r, "Host"),
            col(r, "Db"),
            col(r, "Table_name"),
        ) else {
            continue;
        };
        if SYSTEM_USERS.contains(&u) || !tenant_db(db, tenant) {
            continue;
        }
        let sql = format!(
            "REVOKE {TABLE_WRITE_PRIVS} ON `{}`.`{}` FROM {}@{}",
            db.replace('`', "``"),
            t.replace('`', "``"),
            literal(u),
            literal(h)
        );
        match s.exec(&sql).await {
            Ok(()) => revoked += 1,
            Err(e) => tracing::debug!(%tenant, error = %e, "revoke at table level"),
        }
        users.insert((u.to_owned(), h.to_owned()));
    }
    // Sessions opened before the revoke keep their privileges: end them.
    let me = s.connection_id().to_string();
    let names: BTreeSet<&str> = users.iter().map(|(u, _)| u.as_str()).collect();
    let mut killed = 0;
    for r in s
        .rows("SELECT ID, USER FROM information_schema.PROCESSLIST")
        .await
        .map_err(node_err)?
    {
        let (Some(id), Some(u)) = (col(&r, "ID"), col(&r, "USER")) else {
            continue;
        };
        if id != me && names.contains(u) && s.exec(&format!("KILL CONNECTION {id}")).await.is_ok() {
            killed += 1;
        }
    }
    s.close().await;
    Ok((revoked, killed))
}

#[async_trait::async_trait]
impl CopyFence for GrantFence {
    async fn fence(&self, tenant: TenantId) -> Result<()> {
        // Called before the maps are quarantined: they are still `home`.
        let mut ids = maps_with(&*self.tier, tenant, MapRole::Home).await?;
        ids.extend(maps_with(&*self.tier, tenant, MapRole::Quarantined).await?);
        for id in ids {
            let Some(rec) = cluster(&self.global, &self.tier, &id).await? else {
                continue;
            };
            for ep in ctl_endpoints(&rec)? {
                let (revoked, killed) = fence_tenant_on(&ep, tenant).await?;
                tracing::warn!(%tenant, cluster = %id, node = %ep.node, revoked, killed, "moved tenant's copy fenced");
            }
        }
        Ok(())
    }
}

/// Lag of every standby this DC holds (maps with role `standby` in
/// `tier`), measured on the first node of each standby cluster: 0 while
/// both replication threads run and the replica is caught up, its
/// `Seconds_Behind_Master` otherwise; `u32::MAX` when it doesn't
/// replicate or can't be reached.
///
/// # Errors
/// The store's errors.
pub async fn standby_lags(
    global: &Arc<dyn MetaStore>,
    tier: &Arc<dyn MetaStore>,
) -> Result<Vec<StandbyLag>> {
    let prefix = tier.tier().key("shardmaps/")?;
    let page = tier.list(&prefix, ListOpts::default()).await?;
    let mut by_tenant: BTreeMap<u64, BTreeSet<ClusterId>> = BTreeMap::new();
    for kv in page.kvs {
        let Ok(m) = Record::<ShardMap>::decode(&kv.value) else {
            continue;
        };
        if m.body.role == MapRole::Standby {
            by_tenant
                .entry(m.body.tenant_id.0)
                .or_default()
                .extend(m.body.ranges.iter().map(|r| r.cluster_id.clone()));
        }
    }
    let mut lag_of: BTreeMap<ClusterId, u32> = BTreeMap::new();
    let mut out = Vec::new();
    for (tenant, ids) in by_tenant {
        let mut worst = 0u32;
        for id in ids {
            let l = if let Some(l) = lag_of.get(&id) {
                *l
            } else {
                let l = cluster_lag(global, tier, &id).await;
                lag_of.insert(id.clone(), l);
                l
            };
            worst = worst.max(l);
        }
        out.push(StandbyLag {
            tenant_id: tenant,
            lag_ms: worst,
        });
    }
    Ok(out)
}

async fn cluster_lag(
    global: &Arc<dyn MetaStore>,
    tier: &Arc<dyn MetaStore>,
    id: &ClusterId,
) -> u32 {
    let Ok(Some(rec)) = cluster(global, tier, id).await else {
        return u32::MAX;
    };
    let Some(ep) = ctl_endpoints(&rec).ok().and_then(|v| v.into_iter().next()) else {
        return u32::MAX;
    };
    let Ok(mut s) = ProbeSession::connect(&ep, Duration::from_secs(2)).await else {
        return u32::MAX;
    };
    let rows = s.rows("SHOW SLAVE STATUS").await.unwrap_or_default();
    s.close().await;
    let Some(r) = rows.first() else {
        return u32::MAX;
    };
    let running =
        col(r, "Slave_IO_Running") == Some("Yes") && col(r, "Slave_SQL_Running") == Some("Yes");
    if !running {
        return u32::MAX;
    }
    col(r, "Seconds_Behind_Master")
        .and_then(|v| v.parse::<u32>().ok())
        .map_or(u32::MAX, |s| s.saturating_mul(1000))
}

/// Chapter 25's diverged-transaction report for `tenant`'s copy on this
/// DC's clusters, after the DC returned: the transactions its writer
/// logged past the position the standby had applied at the promotion
/// (from the latest failover record that promoted the tenant), kept to
/// the tenant's schemas, written into that record's `errant`. Returns the
/// count; `None` when no record promoted the tenant or it is reported
/// already.
///
/// # Errors
/// The store's and the nodes' errors.
#[allow(clippy::too_many_lines)] // find the record, read each copy, write the report
pub async fn report_diverged(
    global: &Arc<dyn MetaStore>,
    tier: &Arc<dyn MetaStore>,
    tenant: TenantId,
) -> Result<Option<u64>> {
    // The latest record that promoted the tenant and has no report yet.
    let page = global
        .list(&Tier::Global.key("failovers/")?, ListOpts::default())
        .await?;
    let mut found: Option<(Record<FailoverRecord>, u64, usize)> = None;
    for kv in page.kvs {
        let Ok(r) = Record::<FailoverRecord>::decode(&kv.value) else {
            continue;
        };
        if let Some(i) = r
            .body
            .tenants
            .iter()
            .position(|t| t.tenant_id == tenant.0 && t.outcome == "promoted")
            && found
                .as_ref()
                .is_none_or(|(f, _, _)| f.body.failover_id < r.body.failover_id)
        {
            found = Some((r, kv.mod_revision, i));
        }
    }
    let Some((mut rec, rev, i)) = found else {
        return Ok(None);
    };
    if rec.body.tenants[i].errant.is_some() {
        return Ok(None);
    }
    let applied = rec.body.tenants[i].applied.clone();
    let mut ids = maps_with(&**tier, tenant, MapRole::Quarantined).await?;
    ids.extend(maps_with(&**tier, tenant, MapRole::Home).await?);
    let mut total: Option<super::record::ErrantReport> = None;
    for id in ids {
        let Some(c) = cluster(global, tier, &id).await? else {
            continue;
        };
        let Some(ep) = ctl_endpoints(&c)?.into_iter().next() else {
            continue;
        };
        let mut s = ProbeSession::connect(&ep, NODE_TIMEOUT)
            .await
            .map_err(node_err)?;
        let pos = s
            .scalar("SELECT @@gtid_binlog_pos")
            .await
            .map_err(node_err)?
            .unwrap_or_default();
        let schemas: Vec<String> = s
            .rows(&format!(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE {}",
                literal(&format!("t{tenant}\\_%"))
            ))
            .await
            .map_err(node_err)?
            .iter()
            .filter_map(|r| col(r, "SCHEMA_NAME").map(str::to_owned))
            .collect();
        s.close().await;
        // The standby's position for this source: by cluster id, else the
        // only one recorded.
        let at = applied
            .get(id.as_str())
            .or_else(|| applied.values().next())
            .cloned()
            .unwrap_or_default();
        let domains: BTreeSet<u32> = pos
            .split(',')
            .filter_map(|g| g.trim().split('-').next()?.parse().ok())
            .collect();
        for d in domains {
            let Some(range) = diverged(&pos, &at, d) else {
                continue;
            };
            let txns = diverged_events(&ep, &range).await.map_err(node_err)?;
            let names: Vec<&str> = schemas.iter().map(String::as_str).collect();
            let rep = errant_report(&range, &txns, &names);
            total = Some(match total.take() {
                None => rep,
                Some(mut t) => {
                    t.count += rep.count;
                    t.gtids.extend(rep.gtids);
                    t.tables.extend(rep.tables);
                    t.statements.extend(rep.statements);
                    t
                }
            });
        }
    }
    let report = total.unwrap_or(super::record::ErrantReport {
        count: 0,
        gtids: Vec::new(),
        tables: Vec::new(),
        path: None,
        statements: Vec::new(),
        extract: None,
    });
    let count = report.count;
    rec.body.tenants[i].errant = Some(report);
    let key = failover_key(&rec.body.failover_id)?;
    let r = global
        .txn(Txn {
            when: vec![Cmp::ModRev(key.clone(), rev)],
            then: vec![Op::Put {
                key,
                value: rec.encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    if !r.succeeded {
        return Err(SprigError::internal(
            "the failover record changed while reporting; trying again",
        ));
    }
    if count > 0 {
        tracing::warn!(%tenant, failover = %rec.body.failover_id, count, "DivergedTransactions: reported, never merged");
        metrics::counter!("sprig_controller_diverged_transactions_total", "level" => "dc")
            .increment(count);
    }
    Ok(Some(count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_patterns_match_only_the_tenants_schemas() {
        let t = TenantId(42);
        assert!(tenant_db("t42\\_d%", t));
        assert!(tenant_db("t42_d1_g1_s1", t));
        assert!(tenant_db("t42\\_sprig", t));
        assert!(!tenant_db("t420\\_d%", t));
        assert!(!tenant_db("t4\\_d%", t));
        assert!(!tenant_db("other", t));
    }
}
