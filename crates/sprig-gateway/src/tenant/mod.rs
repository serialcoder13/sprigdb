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
// Chapter 52: recommendations in SQL.
pub mod rec_sql;
// Chapter 29: row-level security (R4).
pub mod rls;
// Chapter 29: row-level security for streamed rows (RLS round 4).
pub mod rls_streams;
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
    /// Chapter 50: the queue engine, set when the AMQP front end starts
    /// (`sprig.mq_*` calls in SQL use it).
    pub mq: std::sync::OnceLock<Arc<sprig_queues::QueueEngine>>,
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
            // Chapter 29: row-level security policies (R4).
            Arc::new(sprig_auth::authz::rls::RlsExtension),
            // Chapter 32: types and record hierarchies.
            Arc::new(sprig_types::TypesExtension),
            // Chapter 45: event streams (R9).
            Arc::new(sprig_streams::StreamsExtension),
            // Chapter 50: exchanges, queues and bindings (R9).
            Arc::new(sprig_queues::catalog::MqExtension),
            // Chapter 51: sinks and connections (R9).
            Arc::new(sprig_sinks::store::SinksExtension),
            // Chapter 43: graphs, labels and relationship types (R8).
            Arc::new(sprig_graph::catalog::GraphExtension),
            // Chapter 42: spaces, inheritable objects and shares.
            Arc::new(sprig_inherit::InheritExtension),
            // Chapter 42 (R9): inherited messaging objects.
            Arc::new(sprig_inherit::msg::MsgExtension),
            // Chapter 15's table groups, for the router (chapter 17).
            Arc::new(crate::exec::router::ShardingExtension),
            // Chapter 31: collections, their indexes and field columns (R5).
            Arc::new(crate::mongo::catalog::DocExtension),
            // Chapter 39: search indexes and analyzers (R5 round 5).
            Arc::new(crate::search::catalog::SearchExtension),
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
        let svc = Arc::new(Self {
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
            mq: std::sync::OnceLock::new(),
            _follow: follow,
        });
        // Chapter 42 (R9): rows of cross-tenant outboxes a lost commit hook
        // left behind.
        crate::inherited::outbox::start_sweeper(&svc);
        crate::inherited::broadcast::start_fan_out(&svc);
        crate::inherited::lifecycle::start_sweeper(&svc);
        // R9 (r9queue): members' failed inherited sink deploys, on a timer.
        crate::inherited::sinks::retry::start(&svc);
        // Decision bugfix-29: a deleted tenant's pooled backend
        // connections close as soon as its directory entry goes.
        close_deleted_tenants(&svc);
        close_dropped_users(&svc);
        Ok(svc)
    }
}

/// Follows the directory (`tenants/<id>`): when a tenant's entry is
/// deleted (its cleanup finished), every pooled backend connection of its
/// accounts on this gateway closes, and the gateway forgets what it
/// reconciled for it (decision bugfix-29; before, each served user kept an
/// idle connection per cluster until `backend.idle_timeout_seconds`).
fn close_deleted_tenants(svc: &Arc<TenantServices>) {
    use futures::StreamExt;
    use sprig_common::ids::TenantId;
    use sprig_meta::WatchEvent;
    let weak = Arc::downgrade(svc);
    let store = Arc::clone(&svc.global);
    let Ok(prefix) = store.tier().key("tenants/") else {
        return;
    };
    tokio::spawn(async move {
        loop {
            match store.watch(&prefix, 0).await {
                Ok(mut w) => {
                    while let Some(ev) = w.next().await {
                        let id_of = |key: &sprig_meta::Key| {
                            key.as_str()
                                .strip_prefix(prefix.as_str())
                                .and_then(|r| r.parse::<u64>().ok())
                                .map(TenantId)
                        };
                        let key = match ev {
                            Ok(WatchEvent::Delete { key, .. }) => key,
                            // Decision bugfix-32: a tenant marked `deleting`
                            // gets no backend account made again here.
                            Ok(WatchEvent::Put(kv)) => {
                                let deleting = sprig_meta::Record::<
                                    crate::inherited::DirEntry,
                                >::decode(&kv.value)
                                .is_ok_and(|r| {
                                    matches!(r.body.state.as_str(), "deleting" | "deleted")
                                });
                                if deleting
                                    && let (Some(t), Some(svc)) = (id_of(&kv.key), weak.upgrade())
                                {
                                    svc.authz.deleting(t);
                                }
                                continue;
                            }
                            Ok(_) => continue,
                            Err(_) => break,
                        };
                        let Some(t) = id_of(&key) else {
                            continue;
                        };
                        let Some(svc) = weak.upgrade() else {
                            return;
                        };
                        svc.authz.deleting(t);
                        svc.locator.retire_tenant(t);
                        svc.authz.forget(t);
                        tracing::info!(tenant = %t, "tenant deleted: its backend connections closed");
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "directory watch for deleted tenants failed; retrying");
                }
            }
            if weak.strong_count() == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}

/// Follows the auth change feed: a dropped user's pooled backend
/// connections close on every gateway that loads the change (decision
/// bugfix-29). The reconciling gateway also retires them when it drops the
/// account; the others never see a grant change for it.
fn close_dropped_users(svc: &Arc<TenantServices>) {
    use sprig_auth::revoke::RevokeReason;
    use tokio::sync::broadcast::error::RecvError;
    let mut changes = svc.auth_changes.subscribe();
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        loop {
            let change = match changes.recv().await {
                Ok(c) => c,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            };
            let Some(svc) = weak.upgrade() else {
                return;
            };
            for r in &change.revoked {
                if r.reason == RevokeReason::Dropped {
                    svc.locator
                        .retire_account(&sprig_auth::authz::backend::account(
                            change.tenant,
                            r.user_id,
                        ));
                }
            }
        }
    });
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
