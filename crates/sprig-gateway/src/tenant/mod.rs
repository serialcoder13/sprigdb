//! Tenants in the gateway (milestone M0.3): catalog logins, the catalog
//! cache, name virtualization, DDL through the catalog and settings in SQL.
//!
//! [`TenantServices`] is the process-wide half, built once in
//! [`crate::run`] when `[gateway] login_mode` is `catalog`:
//!
//! - the [`TenantCache`] of catalog snapshots (chapter 08 step 4), loading
//!   through a [`MariaSource`] with chapter 14's [`AuthExtension`]
//!   registered, invalidated by the global tier's directory watch, and
//!   telling [`Settings`] about every load (so resolved-setting watches
//!   follow tenant, database and object rows);
//! - the catalog's [`DdlEngine`] (chapter 08 step 10) and
//!   [`TenantSettingsWriter`] (step 7), both versioned through the
//!   tenant's directory entry;
//! - the [`locator::GatewayLocator`]: shard maps to clusters, and
//!   `sprig_ctl` pools for catalog work and database DDL;
//! - what chapter 07's plan needs per statement: the
//!   `information_schema` column registry, the gateway's sessions, and
//!   the backend version for executable-comment neutralization.
//!
//! [`session::TenantSession`] is the per-session half, held by the
//! executor's session (see [`crate::exec`]).

pub mod aliases;
pub mod backends;
pub mod ddl;
pub mod locator;
pub mod names;
pub mod password;
pub mod session;
pub mod settings_sql;

use std::sync::Arc;

use sprig_auth::AuthExtension;
use sprig_catalog::ddl::DdlEngine;
use sprig_catalog::{
    AllowAll, CacheConfig, CatalogExtension, CatalogLocator, DirectoryAllocator, DirectoryWatch,
    LogDecisions, MariaBackend, MariaCatalog, MariaSource, TenantCache, TenantSettingsWriter,
    Versioner,
};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_common::time::Clock;
use sprig_meta::MetaStore;
use sprig_settings::{LiveSettings, Registration, Registry, Settings};
use sprig_sql::names::ColumnRegistry;

use crate::backend::Backends;
use crate::session::SessionTable;

use self::locator::GatewayLocator;

/// The MariaDB version executable comments are judged against: any
/// version-gated `/*M!...*/` or `/*!...*/` comment the persona skips is
/// neutralized as if the backend would run it. Over-neutralizing is
/// harmless (MariaDB would skip such a comment anyway); under-neutralizing
/// would let MariaDB run text the gateway never checked, so the gateway
/// assumes the newest possible server rather than the probed one.
pub const BACKEND_VERSION: u32 = 999_999;

/// The process-wide half of tenant sessions (see the module docs).
pub struct TenantServices {
    /// Catalog snapshots.
    pub cache: TenantCache,
    /// Six-scope resolution with watches.
    pub settings: Settings,
    /// This gateway's DC.
    pub dc: DcId,
    /// Versioned catalog changes (passwords, settings, DDL).
    pub catalog: MariaCatalog,
    /// The DDL write path.
    pub ddl: DdlEngine<MariaBackend>,
    /// Setting writes at tenant, database and object scope.
    pub writer: TenantSettingsWriter<MariaBackend>,
    /// Catalogs, clusters and `sprig_ctl` pools.
    pub locator: Arc<GatewayLocator>,
    /// The gateway's sessions (`SHOW PROCESSLIST`).
    pub sessions: Arc<SessionTable>,
    /// `information_schema` column lists.
    pub columns: ColumnRegistry,
    /// The global tier: the tenant directory.
    pub global: Arc<dyn MetaStore>,
    /// Wall-clock time.
    pub clock: Arc<dyn Clock>,
    _follow: Registration,
}

impl std::fmt::Debug for TenantServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantServices")
            .field("dc", &self.dc)
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

impl TenantServices {
    /// Builds the services over `live`'s stores (the global tier for the
    /// directory, this DC's tier for shard maps and cluster records),
    /// asking `backends` for writers.
    ///
    /// # Errors
    /// `Internal` if the catalog modules' migrations are malformed (a
    /// bug); the stores' key errors.
    pub fn start(
        live: &LiveSettings,
        backends: Arc<Backends>,
        sessions: Arc<SessionTable>,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<Self>> {
        let registry: &'static Registry = live.registry();
        let stores = live.stores();
        let dc = stores.dc_id().clone();
        let global = Arc::clone(&stores.global);
        let locator = Arc::new(GatewayLocator::new(
            Arc::clone(&stores.dc),
            ClusterRegistry::new(Arc::clone(&global), Arc::clone(&stores.dc))?,
            backends,
        ));
        let dyn_locator: Arc<dyn CatalogLocator> = locator.clone();
        let extensions: Vec<Arc<dyn CatalogExtension>> = vec![Arc::new(AuthExtension)];
        let source = MariaSource::new(Arc::clone(&dyn_locator), extensions)?;
        let feed = DirectoryWatch::start(Arc::clone(&global))?;
        let settings = Settings::new(live.clone());
        let cache = TenantCache::start(
            registry,
            Arc::new(source),
            Arc::new(feed),
            Some(Arc::new(settings.clone())),
            CacheConfig::default(),
        );
        let follow = cache.follow_settings(live);
        let catalog = Versioner::new(
            MariaBackend::new(Arc::clone(&dyn_locator)).with_clock(Arc::clone(&clock)),
            Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
        );
        let ddl = DdlEngine::new(catalog.clone(), Arc::clone(&clock));
        let writer = TenantSettingsWriter::new(
            catalog.clone(),
            registry,
            Arc::new(live.clone()),
            Arc::new(AllowAll),
            Arc::new(LogDecisions),
            Arc::clone(&clock),
        );
        Ok(Arc::new(Self {
            cache,
            settings,
            dc,
            catalog,
            ddl,
            writer,
            locator,
            sessions,
            columns: ColumnRegistry::mariadb_11_8(),
            global,
            clock,
            _follow: follow,
        }))
    }
}
