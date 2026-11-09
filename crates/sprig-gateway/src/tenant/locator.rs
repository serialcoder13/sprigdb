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
//! **Remote homes (chapter 24).** A tenant's map lives in its home DC's
//! tier. When this DC has no home map for the tenant (it was never homed
//! here, or it moved away, leaving a `quarantined` forwarding map), the
//! locator reads the directory entry's `home_dc` and the home DC's map
//! from that DC's tier of the one etcd (single-etcd mode; the relay's
//! `RemoteMaps` in R2), and follows that map from then on: holds and moves
//! reach this gateway whatever DC the tenant is homed in. A followed map
//! that turns `quarantined` (the tenant moved on) makes the locator read
//! the directory again.
//!
//! Catalog reads and writes, and database-level DDL (`CREATE DATABASE`
//! needs an account that may create `t<id>_d...` schemas), run as the
//! cluster's provisioning account `sprig_ctl`, through a small
//! `mysql_async` pool per cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_catalog::{CatalogEndpoint, CatalogLocator, Store};
use sprig_cluster::driver::NodeConnector as _;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, MetaStore, Mirror, Record, Tier, TierView};
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
    /// The same for other DCs' tiers, for tenants homed there (chapter 24).
    remote_maps: Mutex<HashMap<DcId, Mirror>>,
    /// The global tier, for directory entries; `None`: this DC only.
    global: Option<Arc<dyn MetaStore>>,
    /// Where each tenant's map is followed, and traffic counts.
    routing: Arc<crate::dc::DcRouting>,
    /// When a tenant's home was last read again after its map turned
    /// `quarantined` (at most once a second).
    refreshed: Mutex<HashMap<TenantId, std::time::Instant>>,
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
        let my_dc = registry.dc().clone();
        Self {
            remote_maps: Mutex::new(HashMap::new()),
            global: None,
            routing: crate::dc::DcRouting::new(my_dc),
            refreshed: Mutex::new(HashMap::new()),
            dc,
            registry,
            backends,
            pools: Mutex::new(HashMap::new()),
            clusters: Mutex::new(HashMap::new()),
            ctl_users: Mutex::new(HashMap::new()),
            maps: OnceLock::new(),
        }
    }

    /// The same locator, reading tenants' home DCs from `global`'s
    /// directory (chapter 24), with `routing` noting each home.
    #[must_use]
    pub fn with_homes(
        mut self,
        global: Arc<dyn MetaStore>,
        routing: Arc<crate::dc::DcRouting>,
    ) -> Self {
        self.global = Some(global);
        self.routing = routing;
        self
    }

    /// The routing state (traffic counts, homes).
    #[must_use]
    pub fn routing(&self) -> &Arc<crate::dc::DcRouting> {
        &self.routing
    }

    /// This gateway's DC.
    fn my_dc(&self) -> &DcId {
        self.routing.dc()
    }

    /// The map mirror of DC `dc`.
    fn maps_of(&self, dc: &DcId) -> Mirror {
        if dc == self.my_dc() {
            return self.maps().clone();
        }
        self.remote_maps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(dc.clone())
            .or_insert_with(|| {
                let tier = Tier::Dc(dc.clone());
                let prefix = tier.key("shardmaps/").unwrap_or_else(|_| tier.root());
                Mirror::spawn(TierView::over(&self.dc, tier), prefix)
            })
            .clone()
    }

    /// `tenant`'s map in DC `dc`, from the mirror.
    fn mirrored_map(&self, dc: &DcId, tenant: TenantId) -> Option<ShardMap> {
        let key = ShardMap::catalog_key(&Tier::Dc(dc.clone()), tenant).ok()?;
        let kv = self.maps_of(dc).get(&key)?;
        Record::<ShardMap>::decode(&kv.value).ok().map(|r| r.body)
    }

    /// The role of the map this gateway follows for `tenant` (chapter 25:
    /// a `quarantined` map is a fenced copy); `None` until it has loaded.
    #[must_use]
    pub fn followed_role(&self, tenant: TenantId) -> Option<MapRole> {
        self.mirrored_map(&self.home_of(tenant), tenant)
            .map(|m| m.role)
    }

    /// The DC whose map this gateway follows for `tenant`: its home as last
    /// read, else this DC.
    #[must_use]
    pub fn home_of(&self, tenant: TenantId) -> DcId {
        self.routing.home(tenant)
    }

    /// Reads `tenant`'s home DC from the directory again (a followed map
    /// turned `quarantined`), at most once a second per tenant.
    async fn refresh_home(&self, tenant: TenantId) {
        let Some(global) = &self.global else {
            return;
        };
        {
            let mut r = self
                .refreshed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let now = std::time::Instant::now();
            if r.get(&tenant)
                .is_some_and(|t| now.duration_since(*t) < std::time::Duration::from_secs(1))
            {
                return;
            }
            r.insert(tenant, now);
        }
        match sprig_shard::home::read_home(&**global, tenant).await {
            Ok(Some(e)) => {
                if e.home_dc != self.home_of(tenant) {
                    tracing::info!(%tenant, home = %e.home_dc, "tenant homed in another DC now; following its map there");
                }
                // Start the mirror early, so the next statement finds it.
                let _ = self.maps_of(&e.home_dc);
                self.routing.set_home(tenant, e.home_dc);
            }
            Ok(None) => {}
            Err(e) => tracing::debug!(%tenant, error = %e, "can't read the tenant's home DC"),
        }
    }

    /// Where `tenant`'s map points and whether a mover cutover holds it,
    /// following its home DC (chapter 24): reads the directory again when
    /// the followed map turned `quarantined`. `None` until a map copy has
    /// loaded.
    pub async fn placement_followed(&self, tenant: TenantId) -> Option<Placement> {
        let dc = self.home_of(tenant);
        let map = self.mirrored_map(&dc, tenant);
        if map.as_ref().is_none_or(|m| m.role != MapRole::Home) && self.global.is_some() {
            self.refresh_home(tenant).await;
            let now = self.home_of(tenant);
            if now != dc {
                // The new home's mirror may still be loading: keep following
                // the forwarding map meanwhile.
                if let Some(m) = self.mirrored_map(&now, tenant) {
                    return Placement::of(&m).ok();
                }
            }
        }
        Placement::of(&map?).ok()
    }

    /// Changes whenever a map in the DC `tenant` is followed in changes.
    #[must_use]
    pub fn placement_changes_for(&self, tenant: TenantId) -> watch::Receiver<u64> {
        self.maps_of(&self.home_of(tenant)).changes()
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
        let local = match self.dc.get(&key, Consistency::Serializable).await? {
            Some(kv) => Some(Record::<ShardMap>::decode(&kv.value)?.body),
            None => None,
        };
        let map = match local {
            Some(m) if m.role == MapRole::Home || self.global.is_none() => {
                self.routing.set_home(tenant, self.my_dc().clone());
                m
            }
            // Not homed here (chapter 24): the directory names the home.
            local => match self.remote_map(tenant).await {
                Ok(m) => m,
                // The directory or the home's tier can't be read: a
                // forwarding map still points at the cluster.
                Err(e) => match local {
                    Some(m) => m,
                    None => return Err(e),
                },
            },
        };
        let cluster = map
            .catalog_cluster()
            .cloned()
            .ok_or_else(|| not_here(tenant))?;
        self.clusters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant, cluster.clone());
        Ok(cluster)
    }

    /// `tenant`'s map in its home DC, from the directory entry's
    /// `home_dc`; notes the home.
    async fn remote_map(&self, tenant: TenantId) -> Result<ShardMap> {
        let Some(global) = &self.global else {
            return Err(not_here(tenant));
        };
        let entry = sprig_shard::home::read_home(&**global, tenant)
            .await?
            .ok_or_else(|| not_here(tenant))?;
        let home = entry.home_dc;
        let store = TierView::over(&self.dc, Tier::Dc(home.clone()));
        let key = ShardMap::catalog_key(store.tier(), tenant)?;
        let kv = store
            .get(&key, Consistency::Serializable)
            .await?
            .ok_or_else(|| not_here(tenant))?;
        let map = Record::<ShardMap>::decode(&kv.value)?.body;
        let _ = self.maps_of(&home);
        self.routing.set_home(tenant, home);
        Ok(map)
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

    /// This DC's tier (shard maps, cluster records).
    #[must_use]
    pub fn dc_store(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.dc)
    }

    /// Whether `cluster`'s published capability profile says XA works
    /// (chapter 09's `derived.xa`); `false` without a profile.
    ///
    /// # Errors
    /// The registry's errors.
    pub async fn cluster_xa(&self, cluster: &ClusterId) -> Result<bool> {
        Ok(self
            .registry
            .get(cluster)
            .await?
            .and_then(|r| r.body().profile.as_ref().map(|p| p.derived.xa))
            .unwrap_or(false))
    }

    /// Connection options of `sprig_ctl` on `cluster`'s writer (chapter
    /// 18's resolver-side admin statements).
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn ctl_opts(&self, cluster: &ClusterId) -> Result<mysql_async::Opts> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        let writer = match self.backends.writer(cluster, ShardId(0)).await {
            Ok(n) => rec.nodes.iter().find(|s| s.node == n.node).cloned(),
            Err(_) => None,
        }
        .or_else(|| rec.nodes.first().cloned())
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {cluster} has no writer"),
            )
        })?;
        Ok(CredentialConnector::ctl(&rec.secrets)
            .endpoint(&writer)?
            .opts)
    }

    /// Connection options of `sprig_ctl` on `cluster`'s replicas: its
    /// nodes other than the writer, for a primary with replicas (chapter
    /// 50's `queue.confirm_mode = replica`). Empty for one node, Galera
    /// (a commit is on every node already) and external clusters.
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn ctl_replica_opts(&self, cluster: &ClusterId) -> Result<Vec<mysql_async::Opts>> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        if rec.topology != sprig_cluster::record::TopologyKind::PrimaryReplica {
            return Ok(Vec::new());
        }
        let writer = self
            .backends
            .writer(cluster, ShardId(0))
            .await
            .ok()
            .map(|n| n.node);
        let writer = writer.or_else(|| rec.nodes.first().map(|n| n.node.clone()));
        let mut out = Vec::new();
        for n in rec.nodes.iter().filter(|n| Some(&n.node) != writer.as_ref()) {
            out.push(CredentialConnector::ctl(&rec.secrets).endpoint(n)?.opts);
        }
        Ok(out)
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
