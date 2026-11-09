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
pub mod authz;
pub mod autoindex;
pub mod backends;
pub mod ddl;
pub mod locator;
pub mod names;
pub mod password;
pub mod pins_sql;
pub mod session;
pub mod settings_sql;
pub mod stored_names;

use std::sync::Arc;

use sprig_auth::AuthExtension;
use sprig_auth::revoke::AuthChanges;
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
    /// Chapter 14's auth change feed (revocation), over the cache's load
    /// feed; chapter 29's privilege cache can follow it too.
    pub auth_changes: Arc<AuthChanges>,
    /// Chapter 29: per-user backend accounts and their reconcile state.
    pub authz: authz::AuthzServices,
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
        // Chapter 24: tenants homed in other DCs, the DC registry for
        // client hints, and traffic reports to the home DCs.
        let routing = crate::dc::DcRouting::new(dc.clone());
        let locator = Arc::new(
            GatewayLocator::new(
                Arc::clone(&stores.dc),
                ClusterRegistry::new(Arc::clone(&global), Arc::clone(&stores.dc))?,
                backends,
            )
            .with_homes(Arc::clone(&global), Arc::clone(&routing)),
        );
        crate::dc::install_registry(crate::dc::DcRegistry::start(&global)?);
        drop(crate::dc::DcRouting::spawn_reporter(
            &routing,
            Arc::clone(&stores.dc),
            Some(live.clone()),
        ));
        let dyn_locator: Arc<dyn CatalogLocator> = locator.clone();
        let extensions: Vec<Arc<dyn CatalogExtension>> = vec![
            Arc::new(AuthExtension),
            Arc::new(sprig_auth::authz::AuthzExtension),
            // Chapter 32: types and record hierarchies.
            Arc::new(sprig_types::TypesExtension),
            // Chapter 42: spaces, inheritable objects and shares.
            Arc::new(sprig_inherit::InheritExtension),
            // Chapter 15's table groups, for the router (chapter 17).
            Arc::new(crate::exec::router::ShardingExtension),
        ];
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
        let auth_changes = AuthChanges::start(cache.loads());
        close_revoked_sessions(
            auth_changes.subscribe(),
            Arc::clone(&sessions),
            settings.clone(),
            dc.clone(),
        );
        let catalog = Versioner::new(
            MariaBackend::new(Arc::clone(&dyn_locator)).with_clock(Arc::clone(&clock)),
            Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
        );
        // Chapter 42: no destructive change of a shared object, checked
        // under the catalog lock (decision I7).
        let ddl = DdlEngine::new(catalog.clone(), Arc::clone(&clock))
            .with_check(sprig_inherit::guard::SharedCheck::shared());
        // Chapter 29: SPRIG SET auth.authorizer only names an engine every
        // live gateway knows.
        let writer = TenantSettingsWriter::new(
            catalog.clone(),
            registry,
            Arc::new(live.clone()),
            Arc::new(AllowAll),
            Arc::new(LogDecisions),
            Arc::clone(&clock),
        )
        .with_guard(Arc::new(sprig_auth::authz::engine_guard::EngineGuard::new(
            Arc::clone(&live.stores().global),
            Arc::clone(&live.stores().dc),
        )));
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
            auth_changes,
            authz: authz::AuthzServices::default(),
            _follow: follow,
        }))
    }
}

/// Follows the auth change feed: sessions of users who were dropped or
/// locked, or lost the password method, close at their next safe point
/// with 39306, where the tenant's `auth.close_sessions_on_revoke` is on
/// (chapter 14, "Caching and revocation").
fn close_revoked_sessions(
    mut changes: tokio::sync::broadcast::Receiver<Arc<sprig_auth::revoke::AuthChange>>,
    sessions: Arc<SessionTable>,
    settings: Settings,
    dc: DcId,
) {
    use sprig_auth::settings::CLOSE_SESSIONS_ON_REVOKE;
    use tokio::sync::broadcast::error::RecvError;
    tokio::spawn(async move {
        loop {
            let change = match changes.recv().await {
                Ok(c) => c,
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "revocation feed lagged");
                    continue;
                }
                Err(RecvError::Closed) => return,
            };
            if !change.revoked.iter().any(|r| r.reason.closes_sessions()) {
                continue;
            }
            let ancestry: Vec<_> = change.catalog.ancestry.iter().map(|a| a.id).collect();
            let ctx = sprig_settings::SettingCtx::process(&dc).for_tenant(change.tenant, &ancestry);
            if !settings.get(
                &CLOSE_SESSIONS_ON_REVOKE,
                &ctx,
                Some(&change.catalog.settings),
            ) {
                continue;
            }
            for r in change.revoked.iter().filter(|r| r.reason.closes_sessions()) {
                let n = sessions.close_user(change.tenant, r.user_id);
                if n > 0 {
                    tracing::info!(tenant = %change.tenant, user = %r.user_id, reason = r.reason.as_str(), sessions = n, auth_version = change.auth_version, "closing sessions of a revoked user");
                }
            }
        }
    });
}
