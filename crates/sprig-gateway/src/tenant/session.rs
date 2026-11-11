//! One tenant session's catalog state (chapter 08, "Gateway contract";
//! chapter 07): the pinned tenant, the snapshot statements read, the
//! [`NameMap`](sprig_sql::names::NameMap) built from it, the session's
//! last own catalog change (read your own DDL), its temporary tables, the
//! warnings the gateway raised for its last statement, and the live
//! settings it follows.
//!
//! **Freshness.** The first statement (or `USE` at login) asks the cache
//! for [`Freshness::Latest`]; every later one for `Cached { min_version:
//! last_change }`, which returns the newest cached snapshot at once and
//! waits only when the session's own change isn't loaded yet. The tenant
//! is pinned for the session's life, so its snapshots reload as soon as its
//! directory entry moves: another session's change (DDL, `SPRIG SET`)
//! reaches this one within one load.
//!
//! **Live settings.** A resolved-setting watch
//! ([`Settings::watch_resolved`]) follows `gateway.long_data_max_bytes`
//! for the tenant: a `SPRIG SET` at tenant scope (or a wider one) applies
//! to running sessions as soon as the cache loads it. Statement-level keys
//! (`sql.local_infile`) are resolved per statement against the snapshot.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sprig_auth::{Principal, TenantAuth};
use sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES;
use sprig_catalog::{Freshness, TenantCatalog, TenantPin};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_settings::{OwnedCtx, Settings};
use sprig_sql::names::{MemoryNameMap, NameMap, Purpose, SystemSchema, Visibility};
use tokio::task::JoinHandle;

use super::TenantServices;
use super::names::name_map;
use crate::exec::settings::LONG_DATA_MAX_BYTES;

/// A warning or note the gateway raised itself (a DDL note, a clamp
/// warning of `SPRIG SET`), answered by the next `SHOW WARNINGS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalWarning {
    /// `Note` or `Warning`.
    pub level: &'static str,
    /// The MySQL code.
    pub code: u16,
    /// The message, with logical names.
    pub message: String,
}

/// The per-session half of tenant sessions.
pub struct TenantSession {
    svc: Arc<TenantServices>,
    /// Who logged in.
    pub principal: Principal,
    /// The tenant.
    pub tenant: TenantId,
    _pin: TenantPin,
    cat: Option<Arc<TenantCatalog>>,
    map: Arc<MemoryNameMap>,
    /// The catalog version of the session's last own change.
    pub last_change: u64,
    /// The session's temporary tables: (logical database, lower-case name).
    pub temp: BTreeSet<(Option<String>, String)>,
    /// What the gateway raised for the last statement, if it answered it.
    pub warnings: Option<Vec<LocalWarning>>,
    /// The current logical database before the backend session exists.
    pub current: Option<String>,
    /// `SET @sprig_show_auto_indexes = 1` (chapter 13): automatic indexes
    /// shown in `SHOW INDEX`, `SHOW CREATE TABLE` and `STATISTICS`, and
    /// droppable. Kept by the gateway, never sent to MariaDB.
    pub show_auto_indexes: bool,
    /// Chapter 29: active roles and the compiled privileges.
    pub authz: super::authz::SessionAuthz,
    /// Chapter 32: the type layer's session state.
    pub types: crate::exec::types::TypesSession,
    /// Chapter 42: the session's space.
    pub inherit: crate::exec::inherit::InheritSession,
    /// Chapter 29: row-level security (claims, the covered tables).
    pub rls: super::rls::RlsSession,
    long_data_max: Arc<AtomicU64>,
    watch: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for TenantSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantSession")
            .field("tenant", &self.tenant)
            .field("user", &self.principal.user)
            .field("version", &self.cat.as_ref().map(|c| c.version))
            .field("last_change", &self.last_change)
            .finish_non_exhaustive()
    }
}

impl Drop for TenantSession {
    fn drop(&mut self) {
        if let Some(w) = self.watch.take() {
            w.abort();
        }
    }
}

impl TenantSession {
    /// The session of `principal`, which must be a catalog login.
    ///
    /// # Errors
    /// `Internal` for a principal without a tenant (an accounts-file login).
    pub fn new(svc: Arc<TenantServices>, principal: Principal) -> Result<Self> {
        let tenant = principal
            .tenant
            .ok_or_else(|| SprigError::internal("a tenant session without a tenant"))?;
        let pin = svc.cache.pin(tenant);
        Ok(Self {
            map: Arc::new(MemoryNameMap::new(tenant)),
            svc,
            principal,
            tenant,
            _pin: pin,
            cat: None,
            last_change: 0,
            temp: BTreeSet::new(),
            warnings: None,
            current: None,
            show_auto_indexes: false,
            authz: super::authz::SessionAuthz::default(),
            types: crate::exec::types::TypesSession::default(),
            inherit: crate::exec::inherit::InheritSession::default(),
            rls: super::rls::RlsSession::default(),
            long_data_max: Arc::new(AtomicU64::new(LONG_DATA_MAX_BYTES.default)),
            watch: None,
        })
    }

    /// The services.
    #[must_use]
    pub fn services(&self) -> &Arc<TenantServices> {
        &self.svc
    }

    /// The snapshot statements read now: `Latest` the first time, then
    /// `Cached { min_version: last_change }`.
    ///
    /// # Errors
    /// The cache's (39070 catalog unavailable, 39072, 39073).
    pub async fn refresh(&mut self) -> Result<Arc<TenantCatalog>> {
        let fresh = match &self.cat {
            None => Freshness::Latest,
            Some(_) => Freshness::Cached {
                min_version: self.last_change,
            },
        };
        if self.cat.is_none() {
            // Learn the provisioning account's name, which output hides.
            let _ = self
                .svc
                .locator
                .ctl_pool(&self.principal.backend.cluster)
                .await;
        }
        let cat = self.svc.cache.get(self.tenant, fresh).await?;
        self.install(&cat);
        // Chapter 29: the tenant's per-user backend accounts follow the
        // snapshot before this session uses them.
        if let Err(e) = self
            .svc
            .authz
            .ensure(&self.svc, &cat, &self.principal.backend.cluster)
            .await
        {
            // A tenant being deleted (bugfix-32): every statement says so.
            if e.code() == Some(sprig_common::SprigCode::TenantNotReady) {
                tracing::debug!(tenant = %self.tenant, error = %e, "backend grants not reconciled");
            } else {
                tracing::warn!(tenant = %self.tenant, error = %e, "backend grants not reconciled");
            }
        }
        Ok(cat)
    }

    /// After a name miss: one writer load (at most once a second per
    /// tenant). `true` if a newer snapshot arrived, so the statement is
    /// worth planning again.
    ///
    /// # Errors
    /// The cache's.
    pub async fn reload_after_miss(&mut self) -> Result<bool> {
        let before = self.cat.as_ref().map_or(0, |c| c.version);
        match self.svc.cache.reload_after_miss(self.tenant).await? {
            Some(cat) if cat.version > before => {
                self.install(&cat);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn install(&mut self, cat: &Arc<TenantCatalog>) {
        let same = self.cat.as_ref().is_some_and(|c| c.version == cat.version);
        if !same {
            self.map = Arc::new(name_map(cat));
        }
        // An auth change (chapter 29) moves only the auth version.
        if !same
            || self
                .cat
                .as_ref()
                .is_some_and(|c| c.auth_version != cat.auth_version)
        {
            self.cat = Some(Arc::clone(cat));
        }
        if self.watch.is_none() {
            self.start_watch(cat);
        }
    }

    fn start_watch(&mut self, cat: &Arc<TenantCatalog>) {
        let settings: Settings = self.svc.settings.clone();
        let ctx = OwnedCtx::from_ctx(&cat.setting_ctx(&self.svc.dc));
        let w = settings.watch_resolved(
            LONG_DATA_MAX_BYTES.key,
            ctx,
            Some(Arc::clone(&cat.settings)),
        );
        let mut w = match w {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, error = %e, "no live watch of gateway.long_data_max_bytes");
                return;
            }
        };
        let cell = Arc::clone(&self.long_data_max);
        let value = |r: &sprig_settings::Resolved| {
            <u64 as sprig_settings::SettingValue>::from_value(&r.value)
                .unwrap_or(LONG_DATA_MAX_BYTES.default)
        };
        cell.store(value(w.current()), Ordering::Relaxed);
        self.watch = Some(tokio::spawn(async move {
            while let Ok(r) = w.changed().await {
                let v = value(&r);
                tracing::debug!(
                    key = LONG_DATA_MAX_BYTES.key,
                    value = v,
                    "live setting applied to a session"
                );
                cell.store(v, Ordering::Relaxed);
            }
        }));
    }

    /// The snapshot last installed (after [`Self::refresh`]).
    #[must_use]
    pub fn catalog(&self) -> Option<&Arc<TenantCatalog>> {
        self.cat.as_ref()
    }

    /// The tenant's names at the installed snapshot.
    #[must_use]
    pub fn map(&self) -> Arc<MemoryNameMap> {
        Arc::clone(&self.map)
    }

    /// The schema a logical database's session statements run in (a
    /// system schema keeps its name); `None` for an unknown name.
    #[must_use]
    pub fn schema_of(&self, logical: Option<&str>) -> Option<String> {
        let cur = logical?;
        if let Some(s) = SystemSchema::parse(cur) {
            return Some(s.as_str().to_owned());
        }
        self.map
            .physical(cur, Purpose::Session)
            .ok()
            .and_then(|s| s.single())
            .map(|s| s.to_string())
    }

    /// Backend accounts this session's output must not show (see
    /// [`super::aliases`]).
    #[must_use]
    pub fn aliases(&self) -> super::aliases::AccountAliases {
        let mut backend = vec![self.principal.backend.user.clone()];
        if let Some(u) = self.svc.locator.ctl_user(&self.principal.backend.cluster)
            && !backend.contains(&u)
        {
            backend.push(u);
        }
        super::aliases::AccountAliases {
            backend,
            user: self.principal.user.clone(),
        }
    }

    /// The snapshot's auth module (account names in output).
    #[must_use]
    pub fn auth(&self) -> Option<&TenantAuth> {
        self.cat.as_deref().and_then(TenantAuth::of)
    }

    /// `names.lower_case_table_names` for the tenant.
    #[must_use]
    pub fn lower_case_table_names(&self) -> bool {
        let Some(cat) = &self.cat else { return false };
        self.svc.settings.get(
            &LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// `gateway.long_data_max_bytes` as the live watch last saw it.
    #[must_use]
    pub fn long_data_max(&self) -> usize {
        usize::try_from(self.long_data_max.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
    }

    /// Records a catalog change of this session: its next statements read
    /// at least this version.
    pub fn changed(&mut self, version: Option<u64>) {
        if let Some(v) = version {
            self.last_change = self.last_change.max(v);
        }
    }

    /// Whether `name` (in logical database `db`) is one of the session's
    /// temporary tables.
    #[must_use]
    pub fn is_temp(&self, db: Option<&str>, name: &str) -> bool {
        self.temp
            .contains(&(db.map(str::to_owned), name.to_ascii_lowercase()))
    }

    /// `COM_RESET_CONNECTION` or `COM_CHANGE_USER`: temporary tables and
    /// gateway warnings go.
    pub fn reset(&mut self) {
        self.temp.clear();
        self.warnings = None;
        self.show_auto_indexes = false;
        self.inherit = crate::exec::inherit::InheritSession::default();
    }

    /// What the session may see of internal objects: the persona's
    /// defaults and `@sprig_show_auto_indexes`.
    #[must_use]
    pub fn visibility(&self, persona: sprig_proto_mysql::packets::Persona) -> Visibility {
        Visibility {
            auto_indexes: self.show_auto_indexes,
            ..Visibility::for_persona(persona)
        }
    }
}
