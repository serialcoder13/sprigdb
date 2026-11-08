//! Where a tenant's catalog lives, as the gateway finds it (chapter 08,
//! "Where the catalog lives"): the shard map `shardmaps/<tenant>/0` in this
//! DC's tier names the cluster; the cluster record names its nodes and the
//! `sprig_ctl` credential reference; the gateway's own writer tracking
//! ([`Backends::writer`]) picks the node.
//!
//! The same lookup decides where a tenant's data goes in R0: every
//! database of a tenant lives on its catalog's cluster (decision N4,
//! "R0 placement"), so [`GatewayLocator::cluster`] is also the cluster of
//! the session's backend target.
//!
//! Catalog reads and writes, and database-level DDL (`CREATE DATABASE`
//! needs an account that may create `t<id>_d...` schemas), run as the
//! cluster's provisioning account `sprig_ctl`, through a small
//! `mysql_async` pool per cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::ShardMap;
use sprig_catalog::{CatalogEndpoint, CatalogLocator, Store};
use sprig_cluster::driver::NodeConnector as _;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, MetaStore, Mirror, Record};
use sprig_provision::credentials::CredentialConnector;
use sprig_shard::Placement;
use tokio::sync::watch;

use crate::backend::Backends;

/// Connections per cluster for catalog work and database DDL.
const POOL_MAX: usize = 4;
/// How long an idle `sprig_ctl` connection stays open.
const IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Finds tenants' catalogs and data clusters (see the module docs).
pub struct GatewayLocator {
    dc: Arc<dyn MetaStore>,
    registry: ClusterRegistry,
    backends: Arc<Backends>,
    pools: Mutex<HashMap<ClusterId, (String, Pool)>>,
    clusters: Mutex<HashMap<TenantId, ClusterId>>,
    ctl_users: Mutex<HashMap<ClusterId, String>>,
    /// Every tenant map in this DC tier (`shardmaps/`), fed by a watch,
    /// started at first use: holds and moves (chapter 10).
    maps: OnceLock<Mirror>,
}

impl std::fmt::Debug for GatewayLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayLocator")
            .field("dc", self.dc.tier())
            .finish_non_exhaustive()
    }
}

impl GatewayLocator {
    /// A locator reading shard maps from `dc` (this DC's tier) and cluster
    /// records from `registry`, asking `backends` for writers.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, registry: ClusterRegistry, backends: Arc<Backends>) -> Self {
        Self {
            dc,
            registry,
            backends,
            pools: Mutex::new(HashMap::new()),
            clusters: Mutex::new(HashMap::new()),
            ctl_users: Mutex::new(HashMap::new()),
            maps: OnceLock::new(),
        }
    }

    fn maps(&self) -> &Mirror {
        self.maps.get_or_init(|| {
            let prefix = self
                .dc
                .tier()
                .key("shardmaps/")
                .unwrap_or_else(|_| self.dc.tier().root());
            Mirror::spawn(self.dc.clone(), prefix)
        })
    }

    /// Where `tenant`'s map points and whether a mover cutover holds it,
    /// from the watched copy (no I/O); `None` until the copy has loaded or
    /// for a tenant without a map here.
    #[must_use]
    pub fn placement(&self, tenant: TenantId) -> Option<Placement> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant).ok()?;
        let kv = self.maps().get(&key)?;
        let map = Record::<ShardMap>::decode(&kv.value).ok()?.body;
        Placement::of(&map).ok()
    }

    /// Changes whenever any tenant map changes.
    #[must_use]
    pub fn placement_changes(&self) -> watch::Receiver<u64> {
        self.maps().changes()
    }

    /// The cluster holding `tenant`'s catalog (and, in R0, its data),
    /// read from the shard map and remembered for [`Self::cached_cluster`].
    ///
    /// # Errors
    /// 39072 if the tenant has no catalog in this DC; the store's errors.
    pub async fn cluster(&self, tenant: TenantId) -> Result<ClusterId> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant)?;
        let kv = self
            .dc
            .get(&key, Consistency::Serializable)
            .await?
            .ok_or_else(|| not_here(tenant))?;
        let cluster = Record::<ShardMap>::decode(&kv.value)?
            .body
            .catalog_cluster()
            .cloned()
            .ok_or_else(|| not_here(tenant))?;
        self.clusters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant, cluster.clone());
        Ok(cluster)
    }

    /// The cluster last read for `tenant`, without I/O. Logins call it:
    /// the catalog load that precedes every login read the shard map.
    #[must_use]
    pub fn cached_cluster(&self, tenant: TenantId) -> Option<ClusterId> {
        self.clusters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned()
    }

    /// A pool of `sprig_ctl` connections to `cluster`'s writer.
    ///
    /// # Errors
    /// 39104 for an unknown cluster; 39100 without a writer; 39998 for a
    /// credential reference that doesn't resolve.
    pub async fn ctl_pool(&self, cluster: &ClusterId) -> Result<Pool> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        let writer = match self.backends.writer(cluster, ShardId(0)).await {
            Ok(n) => rec.nodes.iter().find(|s| s.node == n.node).cloned(),
            Err(e) => {
                tracing::debug!(%cluster, error = %e, "no tracked writer; using the record's first node");
                None
            }
        }
        .or_else(|| rec.nodes.first().cloned())
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {cluster} has no writer"),
            )
        })?;
        let fingerprint = format!("{}|{}|{}", writer.node, writer.addr, rec.secrets.ctl);
        if let Some((f, p)) = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            && *f == fingerprint
        {
            return Ok(p.clone());
        }
        let ep = CredentialConnector::ctl(&rec.secrets).endpoint(&writer)?;
        if let Some(u) = ep.opts.user() {
            self.ctl_users
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(cluster.clone(), u.to_owned());
        }
        let constraints = PoolConstraints::new(0, POOL_MAX).unwrap_or_default();
        let pool = Pool::new(
            OptsBuilder::from_opts(ep.opts)
                // Idle connections are kept (up to POOL_MAX) for a minute.
                // With mysql_async's default TTL of 0, every connection
                // returned to a pool with a minimum of 0 is closed, so each
                // catalog read or login-state write opened a new connection
                // (found in M0.7: thousands of TIME_WAIT sockets per minute
                // under HTTP load, one per request).
                .pool_opts(
                    PoolOpts::new()
                        .with_constraints(constraints)
                        .with_inactive_connection_ttl(IDLE_TTL),
                ),
        );
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

    /// The provisioning account's user name on `cluster`, once a pool to it
    /// was made (output mapping hides it).
    #[must_use]
    pub fn ctl_user(&self, cluster: &ClusterId) -> Option<String> {
        self.ctl_users
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .cloned()
    }

    /// Retires the pooled backend connections of `account` (chapter 29:
    /// its grants changed).
    pub fn retire_account(&self, account: &str) {
        self.backends.pools().retire_account(account);
    }

    /// Closes every pool (shutdown).
    pub async fn close(&self) {
        let pools: Vec<Pool> = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, (_, p))| p)
            .collect();
        for p in pools {
            let _ = p.disconnect().await;
        }
    }
}

fn not_here(tenant: TenantId) -> SprigError {
    SprigError::new(
        SprigCode::TenantNotReady,
        format!(
            "tenant {tenant} has no catalog in this data center (not bootstrapped, or homed in \
             another DC, which needs the relay)"
        ),
    )
}

#[async_trait::async_trait]
impl CatalogLocator for GatewayLocator {
    async fn writer(&self, tenant: TenantId) -> Result<CatalogEndpoint> {
        let cluster = self.cluster(tenant).await?;
        Ok(CatalogEndpoint {
            pool: self.ctl_pool(&cluster).await?,
            store: Store::new(SchemaName::catalog(tenant).to_string()),
        })
    }
}
