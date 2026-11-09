//! How the controller reaches tenant catalogs (chapter 08, "Where the
//! catalog lives").
//!
//! A tenant's catalog is shard 0 of table group 0: the shard map
//! `shardmaps/<tenant>/0` in the tenant's home DC tier names its cluster,
//! the cluster record names the nodes and the `sprig_ctl` credential
//! reference, and the topology driver knows the writer. [`ShardMapLocator`]
//! follows that chain for [`sprig_catalog::MariaBackend`]; [`ClusterPools`]
//! is the cluster half, so tests can point clusters at their own servers.
//!
//! Where this differs from the book, for now: catalog loads read the writer
//! (chapter 08 lets them read a local replica); ancestors homed in other
//! DCs are unreachable until the relay (R2) carries their shard maps.
//!
//! **Connection budgets** (chapter 13, "Settings scopes and budgets"; the
//! R0 fix of the catalog-connection deadlock). Each [`RegistryPools`] is
//! one budget: at most `max` connections per cluster. The controller's
//! short catalog calls (settings, the reconciler, decisions, tenant
//! deletion, snapshots) share one budget of [`POOL_MAX`]; the automatic
//! indexing loop and its Admin API, which hold a catalog connection for a
//! whole pass or request, have their own (`autoindex_loop::SESSIONS`).
//! The rules that keep them from deadlocking:
//!
//! - a task holds at most one connection of a budget, and never waits for
//!   a second one of the same budget while it holds the first;
//! - a holder of a session connection may take a short one, never the
//!   reverse, so waits between budgets can't form a cycle;
//! - every wait for a connection ends after [`ACQUIRE_TIMEOUT`]
//!   ([`acquire`], [`bounded`]) with 39070, so a starved caller fails and
//!   retries instead of hanging.
//!
//! `sprig_controller_catalog_pool_connections{pool, cluster, state}`
//! (`in_use`, `idle`, `waiting`) shows each budget.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use std::time::Duration;

use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::ShardMap;
use sprig_catalog::{CatalogEndpoint, CatalogLocator, Store};
use sprig_cluster::driver::NodeConnector;
use sprig_cluster::record::{AdminState, ClusterRecord, NodeSpec};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, MetaStore, Record};
use sprig_provision::credentials::CredentialConnector;

use crate::monitor::ClusterMonitor;

/// Connections a controller keeps per cluster for its short catalog calls.
pub const POOL_MAX: usize = 4;

/// The longest a controller task waits for a catalog connection.
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(20);

fn starved(what: &str, after: Duration) -> SprigError {
    metrics::counter!("sprig_controller_catalog_acquire_timeouts_total").increment(1);
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!(
            "{what}: no catalog connection within {} s (the cluster's connections are all in use)",
            after.as_secs()
        ),
    )
}

/// A connection from `pool`, waiting at most [`ACQUIRE_TIMEOUT`].
///
/// # Errors
/// 39070 when none frees up in time or the connection fails.
pub async fn acquire(pool: &Pool, what: &str) -> Result<Conn> {
    match tokio::time::timeout(ACQUIRE_TIMEOUT, pool.get_conn()).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(e)) => Err(SprigError::new(
            SprigCode::CatalogUnavailable,
            format!("{what}: catalog unreachable"),
        )
        .with_source(e)),
        Err(_) => Err(starved(what, ACQUIRE_TIMEOUT)),
    }
}

/// `f` (one short catalog call that takes its own connection, such as a
/// snapshot), cut off after [`ACQUIRE_TIMEOUT`] plus `work`.
///
/// # Errors
/// `f`'s; 39070 when it runs out of time.
pub async fn bounded<T>(
    what: &str,
    work: Duration,
    f: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let limit = ACQUIRE_TIMEOUT + work;
    tokio::time::timeout(limit, f)
        .await
        .unwrap_or_else(|_| Err(starved(what, limit)))
}

/// The catalog schema of a tenant: `t<tenant>_sprig`, or a test's own name.
pub type CatalogNaming = Arc<dyn Fn(TenantId) -> String + Send + Sync>;

/// `t<tenant>_sprig`.
#[must_use]
pub fn default_naming() -> CatalogNaming {
    Arc::new(|t| SchemaName::catalog(t).to_string())
}

/// Clusters the controller can place catalogs on and connect to.
#[async_trait::async_trait]
pub trait ClusterPools: Send + Sync + 'static {
    /// Clusters of this DC a new catalog may go to: registered and
    /// `active`, by cluster number.
    ///
    /// # Errors
    /// The store's errors.
    async fn placeable(&self) -> Result<Vec<ClusterId>>;

    /// A pool of connections to `cluster`'s writer as `sprig_ctl`.
    ///
    /// # Errors
    /// 39104 for an unknown cluster; 39100 without a writer; 39998 for a
    /// credential that doesn't resolve.
    async fn writer(&self, cluster: &ClusterId) -> Result<Pool>;
}

fn pool_for(opts: mysql_async::Opts, max: usize) -> Pool {
    let constraints = PoolConstraints::new(0, max.max(1)).unwrap_or_default();
    // Keep idle connections for a minute: with mysql_async's default TTL of
    // 0, a pool with a minimum of 0 closes every connection returned to it.
    Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            PoolOpts::new()
                .with_constraints(constraints)
                .with_inactive_connection_ttl(std::time::Duration::from_secs(60)),
        ),
    )
}

/// [`ClusterPools`] over the DC's cluster registry: credentials from the
/// record's secret references, the writer from the `registry` loop's
/// driver when it has one, else the record's first node (the writer at
/// registration for primary-replica clusters; the heaviest writer of a
/// Galera writer set).
pub struct RegistryPools {
    registry: ClusterRegistry,
    monitor: Option<Arc<ClusterMonitor>>,
    pools: Mutex<HashMap<ClusterId, (String, Pool)>>,
    /// The budget's name in metrics.
    name: &'static str,
    /// Connections per cluster.
    max: usize,
}

/// One budget's connections to one cluster, from the pool's own counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolStats {
    /// Handed out to tasks.
    pub in_use: usize,
    /// Open and idle in the pool.
    pub idle: usize,
    /// Tasks waiting for one.
    pub waiting: usize,
}

/// `pool`'s counters, also published as
/// `sprig_controller_catalog_pool_connections`.
#[must_use]
pub fn pool_stats(budget: &str, cluster: &ClusterId, pool: &Pool) -> PoolStats {
    use std::sync::atomic::Ordering::Relaxed;
    let m = pool.metrics();
    let s = PoolStats {
        in_use: m.connections_in_use.load(Relaxed),
        idle: m.connections_in_pool.load(Relaxed),
        waiting: m.active_wait_requests.load(Relaxed),
    };
    #[allow(clippy::cast_precision_loss)] // a handful of connections
    for (state, n) in [
        ("in_use", s.in_use),
        ("idle", s.idle),
        ("waiting", s.waiting),
    ] {
        metrics::gauge!("sprig_controller_catalog_pool_connections",
            "pool" => budget.to_owned(), "cluster" => cluster.to_string(), "state" => state)
        .set(n as f64);
    }
    s
}

impl std::fmt::Debug for RegistryPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryPools")
            .field("dc", self.registry.dc())
            .finish_non_exhaustive()
    }
}

impl RegistryPools {
    /// Pools over `registry`, asking `monitor` for writers.
    #[must_use]
    pub fn new(registry: ClusterRegistry, monitor: Option<Arc<ClusterMonitor>>) -> Self {
        Self {
            registry,
            monitor,
            pools: Mutex::new(HashMap::new()),
            name: "catalog",
            max: POOL_MAX,
        }
    }

    /// The same pools as a budget of `max` connections per cluster, named
    /// `name` in metrics.
    #[must_use]
    pub fn budget(mut self, name: &'static str, max: usize) -> Self {
        self.name = name;
        self.max = max.max(1);
        self
    }

    /// Each cluster's connections now (and the metric updated).
    #[must_use]
    pub fn stats(&self) -> Vec<(ClusterId, PoolStats)> {
        let pools: Vec<(ClusterId, Pool)> = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(c, (_, p))| (c.clone(), p.clone()))
            .collect();
        pools
            .into_iter()
            .map(|(c, p)| {
                let s = pool_stats(self.name, &c, &p);
                (c, s)
            })
            .collect()
    }

    async fn writer_spec(&self, rec: &ClusterRecord) -> Result<NodeSpec> {
        let chosen = match &self.monitor {
            Some(m) => m.writer_node(&rec.cluster_id).await,
            None => None,
        }
        .or_else(|| {
            rec.galera.as_ref().and_then(|g| {
                g.writers
                    .iter()
                    .max_by(|a, b| a.weight.total_cmp(&b.weight))
                    .map(|w| w.node.clone())
            })
        });
        let spec = match chosen {
            Some(n) => rec.nodes.iter().find(|s| s.node == n),
            None => rec.nodes.first(),
        };
        spec.cloned().ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {} has no writer", rec.cluster_id),
            )
        })
    }
}

#[async_trait::async_trait]
impl ClusterPools for RegistryPools {
    async fn placeable(&self) -> Result<Vec<ClusterId>> {
        let mut list: Vec<(u32, ClusterId)> = self
            .registry
            .list()
            .await?
            .into_iter()
            .filter(|c| c.body().admin_state == AdminState::Active)
            .map(|c| (c.body().cluster_number, c.body().cluster_id.clone()))
            .collect();
        list.sort();
        Ok(list.into_iter().map(|(_, id)| id).collect())
    }

    async fn writer(&self, cluster: &ClusterId) -> Result<Pool> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        let spec = self.writer_spec(rec).await?;
        let fingerprint = format!("{}|{}|{}", spec.node, spec.addr, rec.secrets.ctl);
        let hit = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .filter(|(f, _)| *f == fingerprint)
            .map(|(_, p)| p.clone());
        if let Some(p) = hit {
            let _ = pool_stats(self.name, cluster, &p);
            return Ok(p);
        }
        let ep = CredentialConnector::ctl(&rec.secrets).endpoint(&spec)?;
        let pool = pool_for(ep.opts, self.max);
        let old = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), (fingerprint, pool.clone()));
        if let Some((_, old)) = old {
            tokio::spawn(async move {
                let _ = old.disconnect().await;
            });
        }
        Ok(pool)
    }
}

/// Fixed pools per cluster, for tests and single-server tools.
#[derive(Debug, Clone, Default)]
pub struct StaticPools {
    pools: Vec<(ClusterId, Pool)>,
}

impl StaticPools {
    /// No clusters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `cluster`, reached at `url` (`mysql://user:pass@host:port`).
    ///
    /// # Errors
    /// `Internal` for a bad URL.
    pub fn with_url(mut self, cluster: ClusterId, url: &str) -> Result<Self> {
        let opts = mysql_async::Opts::from_url(url)
            .map_err(|e| SprigError::internal(format!("bad MariaDB URL: {e}")))?;
        self.pools.push((cluster, pool_for(opts, POOL_MAX)));
        Ok(self)
    }

    /// Adds `cluster` with a budget of `max` connections.
    ///
    /// # Errors
    /// `Internal` for a bad URL.
    pub fn with_url_max(mut self, cluster: ClusterId, url: &str, max: usize) -> Result<Self> {
        let opts = mysql_async::Opts::from_url(url)
            .map_err(|e| SprigError::internal(format!("bad MariaDB URL: {e}")))?;
        self.pools.push((cluster, pool_for(opts, max)));
        Ok(self)
    }
}

#[async_trait::async_trait]
impl ClusterPools for StaticPools {
    async fn placeable(&self) -> Result<Vec<ClusterId>> {
        Ok(self.pools.iter().map(|(c, _)| c.clone()).collect())
    }

    async fn writer(&self, cluster: &ClusterId) -> Result<Pool> {
        self.pools
            .iter()
            .find(|(c, _)| c == cluster)
            .map(|(_, p)| p.clone())
            .ok_or_else(|| {
                SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"))
            })
    }
}

/// Finds catalogs through their shard maps in this DC's tier.
#[derive(Clone)]
pub struct ShardMapLocator {
    dc: Arc<dyn MetaStore>,
    pools: Arc<dyn ClusterPools>,
    naming: CatalogNaming,
}

impl std::fmt::Debug for ShardMapLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardMapLocator")
            .field("dc", self.dc.tier())
            .finish_non_exhaustive()
    }
}

impl ShardMapLocator {
    /// A locator reading shard maps from `dc` and connecting with `pools`.
    #[must_use]
    pub fn new(
        dc: Arc<dyn MetaStore>,
        pools: Arc<dyn ClusterPools>,
        naming: CatalogNaming,
    ) -> Self {
        Self { dc, pools, naming }
    }

    /// The catalog schema of `tenant`.
    #[must_use]
    pub fn schema(&self, tenant: TenantId) -> String {
        (self.naming)(tenant)
    }

    /// The cluster holding `tenant`'s catalog, if it has a shard map here.
    ///
    /// # Errors
    /// The store's errors; `Internal` for a bad record.
    pub async fn cluster_of(&self, tenant: TenantId) -> Result<Option<ClusterId>> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant)?;
        let Some(kv) = self.dc.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        Ok(Record::<ShardMap>::decode(&kv.value)?
            .body
            .catalog_cluster()
            .cloned())
    }

    /// The pools.
    #[must_use]
    pub fn pools(&self) -> &Arc<dyn ClusterPools> {
        &self.pools
    }
}

#[async_trait::async_trait]
impl CatalogLocator for ShardMapLocator {
    async fn writer(&self, tenant: TenantId) -> Result<CatalogEndpoint> {
        let cluster = self.cluster_of(tenant).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!(
                    "tenant {tenant} has no catalog in this data center (not bootstrapped, or \
                     homed in another DC, which needs the relay)"
                ),
            )
        })?;
        Ok(CatalogEndpoint {
            pool: self.pools.writer(&cluster).await?,
            store: Store::new(self.schema(tenant)),
        })
    }
}
