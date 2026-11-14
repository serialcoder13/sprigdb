//! Live `vector.*` and `analytics.*` settings for the Vectors and
//! Analytics APIs.
//!
//! Read at each call from the gateway's live settings mirror, so a change
//! applies to the next call: at the caller's tenant and database scopes
//! when the tenant's catalog is cached here, else at the gateway's process
//! scopes (dc, global). The limits (`vector.max_k`, `max_overfetch`,
//! `prefilter_max_rows`, `analytics.max_groups`, `query_timeout_seconds`,
//! `max_concurrent_per_tenant`) are capped by their dc and global value, so
//! a tenant can lower them but not raise them.
//! `analytics.max_concurrent_per_gateway` is the key the router already
//! registers (the router's analytic reads share it).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use sprig_catalog::cache::TenantCatalog;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Apply, HardBounds, Scopes, Setting, SettingCtx, SettingValue};

use super::settings::ApiSettingsSource;
use super::sql::{Caller, SqlEngine};
use crate::tenant::TenantServices;

/// Database, tenant (and its ancestors), dc and global.
const DB_TENANT_DC: Scopes = Scopes(0b11_0110);
/// Tenant (and its ancestors), dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $owner:literal, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: $owner,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `vector.default_m`.
    VECTOR_DEFAULT_M: u64 = "vector.default_m", 16, DB_TENANT_DC, HardBounds::range(3, 200), 34,
    "M for indexes SprigDB creates"
);
key!(
    /// `vector.default_ef_search`.
    VECTOR_DEFAULT_EF: u64 = "vector.default_ef_search", 64, DB_TENANT_DC, HardBounds::range(1, 10_000), 34,
    "ef when the request gives none"
);
key!(
    /// `vector.prefilter_max_rows`.
    VECTOR_PREFILTER_MAX_ROWS: u64 = "vector.prefilter_max_rows", 10_000, DB_TENANT_DC, HardBounds::range(0, 100_000_000), 34,
    "Filtered rows per shard for exact search"
);
key!(
    /// `vector.max_overfetch`.
    VECTOR_MAX_OVERFETCH: u64 = "vector.max_overfetch", 16, DB_TENANT_DC, HardBounds::range(1, 1000), 34,
    "Largest over-fetch multiple"
);
key!(
    /// `vector.max_k`.
    VECTOR_MAX_K: u64 = "vector.max_k", 10_000, TENANT_DC, HardBounds::range(1, 100_000), 34,
    "Largest k"
);
key!(
    /// `vector.ann_min_rows`.
    VECTOR_ANN_MIN_ROWS: u64 = "vector.ann_min_rows", 20_000, DB_TENANT_DC, HardBounds::range(0, 1_000_000_000), 34,
    "Rows before an auto index gets its graph"
);
key!(
    /// `vector.min_recall`.
    VECTOR_MIN_RECALL: f64 = "vector.min_recall", 0.95, DB_TENANT_DC, HardBounds::NONE, 34,
    "Recall at k to accept and keep a graph"
);
key!(
    /// `analytics.max_groups`.
    ANALYTICS_MAX_GROUPS: u64 = "analytics.max_groups", 1_000_000, DB_TENANT_DC, HardBounds::range(1, 1_000_000_000), 35,
    "Groups held by one merge"
);
key!(
    /// `analytics.query_timeout_seconds`.
    ANALYTICS_QUERY_TIMEOUT: u64 = "analytics.query_timeout_seconds", 300, DB_TENANT_DC, HardBounds::range(1, 86_400), 35,
    "Per query"
);
key!(
    /// `analytics.max_concurrent_per_tenant`.
    ANALYTICS_PER_TENANT: u64 = "analytics.max_concurrent_per_tenant", 2, TENANT_DC, HardBounds::range(1, 1024), 35,
    "Queries per tenant per gateway"
);

/// Resolves the keys for one call: at the caller's tenant and database
/// when its catalog is cached here, at the process scopes otherwise.
struct For<'a> {
    src: &'a ApiSettingsSource,
    tenant: Option<(&'a TenantServices, Arc<TenantCatalog>)>,
    db: Option<DbId>,
}

impl<'a> For<'a> {
    fn new(engine: &'a SqlEngine, caller: &Caller, database: &str) -> Self {
        let tenant = engine.tenants.as_deref().and_then(|svc| {
            let cat = svc.cache.cached(caller.principal.tenant?)?;
            Some((svc, cat))
        });
        Self::over(engine, tenant, database)
    }

    fn over(
        engine: &'a SqlEngine,
        tenant: Option<(&'a TenantServices, Arc<TenantCatalog>)>,
        database: &str,
    ) -> Self {
        let db = tenant.as_ref().and_then(|(_, cat)| {
            (!database.is_empty() && database != "-")
                .then(|| {
                    cat.database(&database.to_lowercase())
                        .or_else(|| cat.database(database))
                        .map(|d| d.db_id)
                })
                .flatten()
        });
        Self {
            src: &engine.api,
            tenant,
            db,
        }
    }

    /// The process-scope value (dc, global).
    fn process<T: SettingValue>(&self, s: &'static Setting<T>) -> T {
        match self.src {
            ApiSettingsSource::Live { live, dc } => {
                live.snapshot().get(s, &SettingCtx::process(dc))
            }
            ApiSettingsSource::Fixed(_) => s.default.clone(),
        }
    }

    /// The value at the tenant's and database's scopes.
    fn get<T: SettingValue>(&self, s: &'static Setting<T>) -> T {
        let Some((svc, cat)) = &self.tenant else {
            return self.process(s);
        };
        let mut ctx = cat.setting_ctx(&svc.dc);
        if let Some(d) = self.db {
            ctx = ctx.with_db(d);
        }
        svc.settings.get(s, &ctx, Some(&cat.settings))
    }

    /// A limit: the tenant's value, at most the process scopes' value.
    fn capped<T: SettingValue + PartialOrd>(&self, s: &'static Setting<T>) -> T {
        let t = self.get(s);
        let p = self.process(s);
        if t > p { p } else { t }
    }
}

/// The Vectors API's settings now, for `caller` on `database`.
#[must_use]
pub fn vector_for(
    engine: &SqlEngine,
    caller: &Caller,
    database: &str,
) -> sprig_vector::plan::Settings {
    if !matches!(engine.api, ApiSettingsSource::Live { .. }) {
        return vector(&engine.api);
    }
    let r = For::new(engine, caller, database);
    let d = sprig_vector::plan::Settings::default();
    let u32_of = |v: u64| u32::try_from(v).unwrap_or(u32::MAX);
    sprig_vector::plan::Settings {
        default_ef: u32_of(r.get(&VECTOR_DEFAULT_EF)),
        prefilter_max_rows: r.capped(&VECTOR_PREFILTER_MAX_ROWS),
        max_overfetch: u32_of(r.capped(&VECTOR_MAX_OVERFETCH)),
        max_k: u32_of(r.capped(&VECTOR_MAX_K)),
        ann_min_rows: r.get(&VECTOR_ANN_MIN_ROWS),
        min_recall: r.get(&VECTOR_MIN_RECALL).clamp(0.0, 1.0),
        default_m: u8::try_from(r.get(&VECTOR_DEFAULT_M)).unwrap_or(d.default_m),
    }
}

/// The Analytics API's limits now, for `caller` on `database`.
#[must_use]
pub fn analytics_for(engine: &SqlEngine, caller: &Caller, database: &str) -> Analytics {
    if !matches!(engine.api, ApiSettingsSource::Live { .. }) {
        return analytics(&engine.api);
    }
    let r = For::new(engine, caller, database);
    let us = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
    Analytics {
        max_groups: us(r.capped(&ANALYTICS_MAX_GROUPS)),
        timeout: Duration::from_secs(r.capped(&ANALYTICS_QUERY_TIMEOUT)),
        per_tenant: us(r.capped(&ANALYTICS_PER_TENANT)),
        per_gateway: us(r.process(&sprig_router::settings::ANALYTICS_MAX_CONCURRENT)),
    }
}

/// The Vectors API's settings now, at the process scopes.
#[must_use]
pub fn vector(src: &ApiSettingsSource) -> sprig_vector::plan::Settings {
    let d = sprig_vector::plan::Settings::default();
    let ApiSettingsSource::Live { live, dc } = src else {
        return d;
    };
    let snap = live.snapshot();
    let cx = SettingCtx::process(dc);
    let u32_of = |s: &Setting<u64>| u32::try_from(snap.get(s, &cx)).unwrap_or(u32::MAX);
    sprig_vector::plan::Settings {
        default_ef: u32_of(&VECTOR_DEFAULT_EF),
        prefilter_max_rows: snap.get(&VECTOR_PREFILTER_MAX_ROWS, &cx),
        max_overfetch: u32_of(&VECTOR_MAX_OVERFETCH),
        max_k: u32_of(&VECTOR_MAX_K),
        ann_min_rows: snap.get(&VECTOR_ANN_MIN_ROWS, &cx),
        min_recall: snap.get(&VECTOR_MIN_RECALL, &cx).clamp(0.0, 1.0),
        default_m: u8::try_from(snap.get(&VECTOR_DEFAULT_M, &cx)).unwrap_or(d.default_m),
    }
}

/// The Analytics API's limits now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Analytics {
    /// `analytics.max_groups`.
    pub max_groups: usize,
    /// `analytics.query_timeout_seconds`.
    pub timeout: Duration,
    /// `analytics.max_concurrent_per_tenant`.
    pub per_tenant: usize,
    /// `analytics.max_concurrent_per_gateway`.
    pub per_gateway: usize,
}

/// [`Analytics`] from `src`, at the process scopes.
#[must_use]
pub fn analytics(src: &ApiSettingsSource) -> Analytics {
    let ApiSettingsSource::Live { live, dc } = src else {
        return Analytics {
            max_groups: 1_000_000,
            timeout: Duration::from_secs(300),
            per_tenant: 2,
            per_gateway: 8,
        };
    };
    let snap = live.snapshot();
    let cx = SettingCtx::process(dc);
    let us = |s: &Setting<u64>| usize::try_from(snap.get(s, &cx)).unwrap_or(usize::MAX);
    Analytics {
        max_groups: us(&ANALYTICS_MAX_GROUPS),
        timeout: Duration::from_secs(snap.get(&ANALYTICS_QUERY_TIMEOUT, &cx)),
        per_tenant: us(&ANALYTICS_PER_TENANT),
        per_gateway: us(&sprig_router::settings::ANALYTICS_MAX_CONCURRENT),
    }
}

/// One admitted analytics query: frees its slots when dropped.
#[derive(Debug)]
pub struct Admitted {
    tenant: Arc<AtomicUsize>,
    gateway: &'static AtomicUsize,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.tenant.fetch_sub(1, Ordering::AcqRel);
        self.gateway.fetch_sub(1, Ordering::AcqRel);
    }
}

fn take(c: &AtomicUsize, limit: usize) -> bool {
    c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
        (n < limit).then_some(n + 1)
    })
    .is_ok()
}

/// Admits a query of `tenant` under the limits now, waiting up to a
/// second for a slot.
///
/// # Errors
/// 39780 (retryable) when no slot frees in time.
pub async fn admit(tenant: u64, limits: Analytics) -> Result<Admitted> {
    static GATEWAY: AtomicUsize = AtomicUsize::new(0);
    static TENANTS: OnceLock<Mutex<HashMap<u64, Arc<AtomicUsize>>>> = OnceLock::new();
    let t = Arc::clone(
        TENANTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(tenant)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0))),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if take(&t, limits.per_tenant) {
            if take(&GATEWAY, limits.per_gateway) {
                return Ok(Admitted {
                    tenant: t,
                    gateway: &GATEWAY,
                });
            }
            t.fetch_sub(1, Ordering::AcqRel);
        }
        if tokio::time::Instant::now() >= deadline {
            metrics::counter!("sprig_dim_rejected_total", "reason" => "admission").increment(1);
            let mut e = SprigError::new(
                SprigCode::RateLimited,
                "too many analytics queries running (analytics.max_concurrent_per_tenant, analytics.max_concurrent_per_gateway); retry",
            );
            e.retryable = true;
            return Err(e);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// [`vector_auto_ann`] once this gateway's copy of the tenant's catalog
/// holds the directory's catalog version as read now (linearizable), so a
/// `vector.auto_ann` change made a moment ago on any gateway is seen
/// (otherwise an automatic graph could be built just after `approve` was
/// set, before the settings cache had it). Falls back to the cached value
/// when the directory or the catalog can't be read.
pub async fn vector_auto_ann_fresh(engine: &SqlEngine, caller: &Caller, database: &str) -> String {
    let cached = vector_auto_ann(engine, caller, database);
    if !matches!(engine.api, ApiSettingsSource::Live { .. }) {
        return cached;
    }
    let (Some(svc), Some(t)) = (engine.tenants.as_deref(), caller.principal.tenant) else {
        return cached;
    };
    let Ok(key) = svc.global.tier().key(&format!("tenants/{}", t.0)) else {
        return cached;
    };
    let want = match svc
        .global
        .get(&key, sprig_meta::Consistency::Linearizable)
        .await
    {
        Ok(Some(kv)) => serde_json::from_slice::<serde_json::Value>(&kv.value)
            .ok()
            .and_then(|v| v.get("catalog_version")?.as_u64())
            .unwrap_or(0),
        _ => return cached,
    };
    let fresh = sprig_catalog::cache::Freshness::Cached { min_version: want };
    match svc.cache.get(t, fresh).await {
        Ok(cat) => For::over(engine, Some((svc, cat)), database)
            .get(&sprig_approvals::settings::VECTOR_AUTO_ANN)
            .into_owned(),
        Err(_) => cached,
    }
}

/// `vector.auto_ann` (`on`, `approve`, `recommend` or `off`) for `caller`
/// on `database`, at the same scopes as the other `vector.*` keys.
#[must_use]
pub fn vector_auto_ann(engine: &SqlEngine, caller: &Caller, database: &str) -> String {
    if !matches!(engine.api, ApiSettingsSource::Live { .. }) {
        return "on".into();
    }
    For::new(engine, caller, database)
        .get(&sprig_approvals::settings::VECTOR_AUTO_ANN)
        .into_owned()
}
