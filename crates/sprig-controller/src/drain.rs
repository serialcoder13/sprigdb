//! The end of a cluster's life (chapter 09, "Cluster states"; r2opfix):
//! `draining` becomes `drained` once no shard map names the cluster, and a
//! drained cluster's record is `retired` when its provisioner retires it.
//!
//! ```text
//! active ─drain─► draining ─no shard map names it─► drained ─retire─► retired
//! ```
//!
//! Before this, nothing moved a record past `draining`: a drained, retired
//! Kubernetes cluster stayed `draining` with unreachable nodes, and `sprigctl
//! cluster drain` on an empty cluster stayed `draining` (r2kind). Now the
//! drain call finishes at once on an empty cluster, the balancer's pass
//! finishes the rest when their shards have moved away, and `retire` (the
//! Admin API's `:retire`, called by the Kubernetes provisioner before it
//! deletes anything) retires the record.

use std::collections::BTreeSet;

use sprig_catalog::placement::ShardMap;
use sprig_cluster::ClusterRegistry;
use sprig_cluster::record::AdminState;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId};
use sprig_meta::{Cmp, ListOpts, MetaStore, Record};

/// Every cluster a shard map of this DC names: any table group, any range,
/// homes and standbys alike.
///
/// # Errors
/// The store's errors.
pub async fn clusters_in_use(dc: &dyn MetaStore) -> Result<BTreeSet<ClusterId>> {
    let prefix = dc.tier().key("shardmaps/")?;
    let mut out = BTreeSet::new();
    let mut opts = ListOpts::default();
    loop {
        let page = dc.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            // A map that doesn't decode might name the cluster: keep every
            // draining cluster draining rather than guess.
            match Record::<ShardMap>::decode(&kv.value) {
                Ok(m) => out.extend(m.body.ranges.into_iter().map(|r| r.cluster_id)),
                Err(e) => {
                    tracing::warn!(key = %kv.key, error = %e, "unreadable shard map; drains wait");
                    return Err(e);
                }
            }
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    Ok(out)
}

/// Moves this DC's `draining` clusters that no shard map names to
/// `drained`; returns them. `guard` fences the writes to the leader.
///
/// # Errors
/// The store's errors.
pub async fn finish_drains(
    registry: &ClusterRegistry,
    dc_store: &dyn MetaStore,
    dc: &DcId,
    guard: Vec<Cmp>,
) -> Result<Vec<ClusterId>> {
    let draining: Vec<ClusterId> = registry
        .list()
        .await?
        .iter()
        .map(sprig_cluster::registry::StoredCluster::body)
        .filter(|r| &r.dc_id == dc && r.admin_state == AdminState::Draining)
        .map(|r| r.cluster_id.clone())
        .collect();
    if draining.is_empty() {
        return Ok(Vec::new());
    }
    let used = clusters_in_use(dc_store).await?;
    let registry = registry.clone().with_guard(guard);
    let mut done = Vec::new();
    for id in draining.into_iter().filter(|c| !used.contains(c)) {
        let gaps = registry
            .get(&id)
            .await?
            .map(|s| s.body().gaps.clone())
            .unwrap_or_default();
        registry
            .set_admin_state(&id, AdminState::Drained, gaps)
            .await?;
        tracing::info!(cluster = %id, "drained: no shard map names it");
        metrics::counter!("sprig_controller_clusters_drained_total").increment(1);
        done.push(id);
    }
    Ok(done)
}
