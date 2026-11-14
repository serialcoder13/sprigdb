//! Write load on the DC leader: the sharding loop's
//! [`WriteLoad`] over the gateways' write sketches.
//!
//! ```text
//! before each sharding tick (at most every refresh_every, 30 s):
//!   list gateway-load/ in the DC tier             one LoadReport per gateway
//!   SketchCollector.ingest(report)                each (gateway, minute) once
//!   hot ← placement.above_high()                  the balancer's last run
//! write_shares(shard, since)   per settled minute: shard writes / its cluster's writes
//! cluster_hot(cluster)         cluster ∈ hot
//! write_histogram(shard, ...)  the merged sketches of the window's minutes
//! ```
//!
//! Gateways write their report under their registration lease when a
//! minute completes ([`sprig_balancer::load::report_key`]); each carries
//! the last three completed minutes, so a missed refresh loses nothing.
//! History lives in the leader's memory: a new leader starts with none,
//! so a load split needs `sharding.split_sustain_minutes` of new data.
//!
//! **Hot clusters** *(choice)*: placement's load model, read from the
//! balancer's last run (`full` above `balancer.high_pct`). Before the
//! balancer's first run, or without a balancer, no cluster is hot and no
//! load split fires.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use sprig_balancer::load::{LoadReport, REPORT_PREFIX};
use sprig_balancer::model::ShardKey;
use sprig_balancer::shards::{HashHistogram, HashRange, SketchCollector};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, GroupId, ShardId, TenantId};
use sprig_common::time::Clock;
use sprig_meta::{ListOpts, MetaStore, Record};

use crate::balancer_loop::BalancerLoop;
use crate::sharding::WriteLoad;

/// Where hot clusters come from.
#[async_trait::async_trait]
pub trait HotClusters: Send + Sync {
    /// Clusters above their high watermark; `None` when unknown yet.
    async fn above_high(&self) -> Option<BTreeSet<ClusterId>>;
}

#[async_trait::async_trait]
impl HotClusters for BalancerLoop {
    async fn above_high(&self) -> Option<BTreeSet<ClusterId>> {
        BalancerLoop::above_high(self).await
    }
}

/// The gateways' write sketches, as the sharding loop reads them (module
/// docs).
pub struct GatewayWriteLoad {
    dc: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    collector: Mutex<SketchCollector>,
    hot: Mutex<BTreeSet<ClusterId>>,
    placement: Mutex<Option<Arc<dyn HotClusters>>>,
    /// The last refresh, and each report's revision then.
    refreshed: tokio::sync::Mutex<(Option<Instant>, BTreeMap<String, u64>)>,
    /// The shortest time between two refreshes.
    pub refresh_every: Duration,
}

impl std::fmt::Debug for GatewayWriteLoad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayWriteLoad")
            .field("refresh_every", &self.refresh_every)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl GatewayWriteLoad {
    /// Reads the reports in the DC tier `dc`.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, clock: Arc<dyn Clock>) -> Self {
        Self {
            dc,
            clock,
            collector: Mutex::new(SketchCollector::new()),
            hot: Mutex::new(BTreeSet::new()),
            placement: Mutex::new(None),
            refreshed: tokio::sync::Mutex::new((None, BTreeMap::new())),
            refresh_every: Duration::from_secs(30),
        }
    }

    /// Takes hot clusters from `p` (the balancer).
    pub fn set_placement(&self, p: Arc<dyn HotClusters>) {
        *lock(&self.placement) = Some(p);
    }

    fn now_ms(&self) -> u64 {
        self.clock
            .wall()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    /// Takes one report (tests; [`Self::refresh`] reads them from etcd).
    pub fn ingest(&self, r: &LoadReport) {
        lock(&self.collector).ingest(r);
    }

    /// Reads the gateways' reports and placement's hot clusters, unless the
    /// last refresh was under `refresh_every` ago.
    ///
    /// # Errors
    /// The store's.
    pub async fn refresh(&self) -> Result<()> {
        let mut g = self.refreshed.lock().await;
        let now = self.clock.now();
        if g.0
            .is_some_and(|t| now.duration_since(t) < self.refresh_every)
        {
            return Ok(());
        }
        g.0 = Some(now);
        let prefix = self.dc.tier().key(REPORT_PREFIX)?;
        let mut opts = ListOpts::default();
        let mut reports = Vec::new();
        let mut revs = BTreeMap::new();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                let key = kv.key.as_str().to_owned();
                revs.insert(key.clone(), kv.mod_revision);
                if g.1.get(&key) == Some(&kv.mod_revision) {
                    continue;
                }
                match Record::<LoadReport>::decode(&kv.value) {
                    Ok(r) => reports.push(r.body),
                    Err(e) => tracing::warn!(%key, error = %e, "unreadable load report"),
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        g.1 = revs;
        drop(g);
        let now_ms = self.now_ms();
        {
            let mut c = lock(&self.collector);
            for r in &reports {
                c.ingest(r);
            }
            c.prune(now_ms);
        }
        metrics::counter!("sprig_balancer_load_reports_total").increment(reports.len() as u64);
        let placement = lock(&self.placement).clone();
        let hot = match placement {
            Some(p) => p.above_high().await.unwrap_or_default(),
            None => BTreeSet::new(),
        };
        tracing::debug!(reports = reports.len(), hot = ?hot, "write sketches refreshed");
        *lock(&self.hot) = hot;
        Ok(())
    }
}

impl WriteLoad for GatewayWriteLoad {
    fn write_shares(
        &self,
        tenant: TenantId,
        group: GroupId,
        shard: ShardId,
        since_ms: Option<u64>,
    ) -> Vec<f64> {
        let k = ShardKey {
            tenant,
            group,
            shard,
        };
        lock(&self.collector).write_shares(&k, since_ms, self.now_ms())
    }

    fn cluster_hot(&self, cluster: &ClusterId) -> bool {
        lock(&self.hot).contains(cluster)
    }

    fn write_histogram(
        &self,
        tenant: TenantId,
        group: GroupId,
        shard: ShardId,
        lo: u64,
        hi: Option<u64>,
        window: Duration,
    ) -> Option<HashHistogram> {
        let k = ShardKey {
            tenant,
            group,
            shard,
        };
        lock(&self.collector).write_histogram(&k, HashRange::new(lo, hi), window, self.now_ms())
    }
}

#[cfg(test)]
mod tests;
