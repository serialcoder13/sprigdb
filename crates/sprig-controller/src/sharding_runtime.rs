//! The sharding loop ([`crate::sharding::ShardingLoop`], chapters 16 and
//! 20) in the controller's runtime: a DC-leader loop over the tenants
//! homed here and `active`, one tick per tenant every
//! `sharding.check_interval_seconds`, with every setting resolved from the
//! controller's live settings (etcd: global, dc and tenant scopes).
//!
//! Where this differs from the book: chapter 16 runs the advisor as a
//! worker job; there is no worker job runtime for it yet, so the DC leader
//! runs the advisor's first design inside the tick, as the library loop
//! does. Tenant-scope values stored in tenants' catalogs are read through
//! [`crate::tenant_scopes`] (RL3, r2harden): a tenant's own `sharding.*`,
//! `advisor.*` and `mover.cutover_max_hold_ms` rows win over its DC's.

use std::sync::Arc;
use std::time::Duration;

use mysql_async::Pool;
use sprig_advisor::sharding::AdvisorSettings;
use sprig_balancer::decisions::DecisionLog;
use sprig_balancer::shards::ShardingSettings;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::Clock;
use sprig_meta::MetaStore;
use sprig_settings::{LiveSettings, MemSettings, SettingCtx};

use crate::catalogs::ClusterPools as CatalogPools;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::sharding::{ClusterPools, SettingsSource, ShardingLoop, TenantSettings};
use crate::tree::{Directory, DirectoryEntry, TenantState};
use crate::write_load::GatewayWriteLoad;

/// Connections per cluster the loop may hold (size dives, the advisor's
/// apply and its back-fill).
pub const POOL_MAX: usize = 4;

/// The sharding loop's view of the controller's cluster pools.
pub struct WriterPools(pub Arc<dyn CatalogPools>);

#[async_trait::async_trait]
impl ClusterPools for WriterPools {
    async fn pool(&self, cluster: &ClusterId) -> Result<Pool> {
        self.0.writer(cluster).await
    }
}

/// The tenant-rows cache, set once the controller has built it.
type ScopesCell = Arc<std::sync::OnceLock<Arc<crate::tenant_scopes::TenantScopes>>>;

/// Settings from the controller's live settings.
pub struct LiveSource {
    live: Option<LiveSettings>,
    dc: DcId,
    /// Tenants' own catalog rows (RL3), as cached.
    scopes: ScopesCell,
}

impl LiveSource {
    /// Resolves in `dc` from `live` (defaults without it).
    #[must_use]
    pub fn new(live: Option<LiveSettings>, dc: DcId) -> Self {
        Self {
            live,
            dc,
            scopes: Arc::default(),
        }
    }

    /// The tenant's rows as cached (read in the background when missing
    /// or stale); `None` without the cache.
    fn rows(&self, tenant: TenantId) -> Option<Arc<sprig_settings::TenantSettings>> {
        self.scopes.get().and_then(|s| s.cached(tenant))
    }
}

impl SettingsSource for LiveSource {
    fn settings(&self, tenant: TenantId) -> TenantSettings {
        let Some(live) = &self.live else {
            return TenantSettings::default();
        };
        let snap = live.snapshot();
        let s: &MemSettings<'_> = &snap;
        let ctx = SettingCtx::process(&self.dc).for_tenant(tenant, &[]);
        let rows = self.rows(tenant);
        let m = sprig_mover::MoverSettings::resolve_with(s, rows.as_deref(), &ctx);
        TenantSettings {
            sharding: ShardingSettings::resolve_with(s, rows.as_deref(), &ctx),
            advisor: AdvisorSettings::resolve_with(s, rows.as_deref(), &ctx),
            max_hold: m.cutover_max_hold,
            txn_grace: m.txn_grace,
            hold_grace: m.hold_grace,
            chunk_rows: m.copy_chunk_rows,
        }
    }

    fn min_shards(&self, tenant: TenantId, db: sprig_common::ids::DbId) -> u32 {
        let Some(live) = &self.live else {
            return ShardingSettings::default().min_shards;
        };
        let snap = live.snapshot();
        let s: &MemSettings<'_> = &snap;
        let ctx = SettingCtx::process(&self.dc)
            .for_tenant(tenant, &[])
            .with_db(db);
        ShardingSettings::resolve_with(s, self.rows(tenant).as_deref(), &ctx).min_shards
    }
}

/// The loop (module docs).
pub struct ShardingRuntime {
    inner: Arc<ShardingLoop>,
    dir: Directory,
    dc: DcId,
    settings: Arc<dyn SettingsSource>,
    /// Chapter 20's gateway write sketches (load splits).
    write_load: Arc<GatewayWriteLoad>,
    /// [`LiveSource`]'s tenant-rows cache (RL3).
    scopes: ScopesCell,
}

impl std::fmt::Debug for ShardingRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardingRuntime")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl ShardingRuntime {
    /// The loop over DC `dc`'s tier and the global tier, with clusters
    /// from `pools`, decisions in `decisions` and settings from `live`.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dc: DcId,
        dc_store: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        pools: Arc<dyn CatalogPools>,
        decisions: Arc<dyn DecisionLog>,
        live: Option<LiveSettings>,
        actor: &str,
    ) -> Self {
        let source = LiveSource::new(live, dc.clone());
        let scopes = Arc::clone(&source.scopes);
        let settings: Arc<dyn SettingsSource> = Arc::new(source);
        let write_load = Arc::new(GatewayWriteLoad::new(
            Arc::clone(&dc_store),
            Arc::clone(&clock),
        ));
        let inner = ShardingLoop::new(
            dc_store,
            clock,
            Arc::new(WriterPools(pools)),
            decisions,
            Arc::clone(&settings),
            actor,
        )
        .with_directory(Arc::clone(&global))
        .with_write_load(Arc::clone(&write_load) as Arc<dyn crate::sharding::WriteLoad>);
        Self {
            inner: Arc::new(inner),
            dir: Directory::new(global),
            dc,
            settings,
            write_load,
            scopes,
        }
    }

    /// The same loop reading tenants' own catalog rows (RL3, r2harden).
    #[must_use]
    pub fn with_tenant_scopes(self, s: Arc<crate::tenant_scopes::TenantScopes>) -> Self {
        let _ = self.scopes.set(s);
        self
    }

    /// The gateways' write sketches the loop reads (hot clusters come from
    /// [`GatewayWriteLoad::set_placement`]).
    #[must_use]
    pub fn write_load(&self) -> Arc<GatewayWriteLoad> {
        Arc::clone(&self.write_load)
    }

    /// The same loop publishing the advisor's catalog changes with
    /// `versioner` (gateways reload their layouts from new versions).
    ///
    /// # Panics
    /// Never: called while building, before the loop is shared.
    #[must_use]
    pub fn with_publish(
        mut self,
        versioner: sprig_catalog::Versioner<sprig_catalog::MariaBackend>,
    ) -> Self {
        let p: Arc<dyn sprig_advisor::sharding::apply::Publish> =
            Arc::new(VersionPublish(versioner));
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.set_publish(p);
        }
        self
    }

    /// The library loop (the Admin API's manual jobs and advisor calls).
    #[must_use]
    pub fn sharding(&self) -> Arc<ShardingLoop> {
        Arc::clone(&self.inner)
    }

    fn ours(&self, e: &DirectoryEntry) -> bool {
        e.home_dc == self.dc && e.state == TenantState::Active
    }
}

#[async_trait::async_trait]
impl Reconciler for ShardingRuntime {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "sharding"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        self.dir.tenant_of(&ev.key).into_iter().collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        Ok(self
            .dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| self.ours(e))
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let Some(entry) = self.dir.get(*key).await? else {
            return Ok(Outcome::Done);
        };
        if !self.ours(entry.body()) {
            return Ok(Outcome::Done);
        }
        let every = self
            .settings
            .settings(*key)
            .sharding
            .check_interval
            .max(Duration::from_secs(1));
        if let Err(e) = self.write_load.refresh().await {
            tracing::warn!(error = %e, "gateway write sketches not read");
        }
        let rep = self.inner.tick(*key).await?;
        if rep != crate::sharding::TickReport::default() {
            tracing::info!(tenant = %key, ?rep, "sharding tick");
        }
        Ok(Outcome::RequeueAfter(every))
    }

    fn workers(&self) -> usize {
        2
    }
}

/// [`Publish`](sprig_advisor::sharding::apply::Publish) as an empty
/// versioned catalog change.
pub struct VersionPublish(pub sprig_catalog::Versioner<sprig_catalog::MariaBackend>);

impl std::fmt::Debug for VersionPublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VersionPublish")
    }
}

#[async_trait::async_trait]
impl sprig_advisor::sharding::apply::Publish for VersionPublish {
    async fn catalog_changed(&self, tenant: TenantId) -> Result<()> {
        let v = self
            .0
            .change(tenant, sprig_catalog::VersionKind::Catalog)
            .await?
            .commit()
            .await?;
        tracing::info!(tenant = %tenant, version = v, "table groups published");
        Ok(())
    }
}
