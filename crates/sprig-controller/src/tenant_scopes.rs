//! Tenant, database and object scopes for controllers and workers.
//!
//! Gateways resolve settings over their catalog cache, which holds every
//! tenant's setting rows. Controllers and workers mirror only the etcd
//! scopes (global, dc, cluster), so a value stored in a tenant's catalog
//! (`ALTER TENANT ... SET`, `sprigctl settings set --scope tenant:<t>`) was
//! never seen by their loops and jobs: a tenant's own
//! `failover.standby_dcs`, `migration.write_pause_target_ms` or
//! `mover.cutover_max_hold_ms` didn't apply.
//!
//! [`TenantScopes`] keeps, per tenant a loop or job asks about, the
//! tenant's setting rows (tenant, database, object and ancestor rows, and
//! its ancestry) as a [`TenantSettings`], read from the tenant's catalog on
//! its home DC's catalog cluster. A cached copy is checked against the
//! directory entry's `catalog_version` at most once a second (one etcd
//! read), and read again only when the directory is ahead of the version
//! the copy was read at (`sprig.catalog_version`, committed with the rows;
//! the directory allocates a version before the catalog commits it, so the
//! directory's number alone would let a read just before the commit stick).
//! Every catalog change, settings included, moves it. So a change reaches the next resolution
//! within about a second plus one catalog read.
//!
//! Resolution layers the rows over the process's etcd snapshot
//! ([`get`]), exactly as gateways do (`TenantSettings::resolve`): the
//! narrowest scope wins and every wider scope's bounds apply. Tenant-wide
//! controller decisions resolve at the home DC, whatever DC the
//! evaluating process is in (`ctx.dc` is otherwise that process's DC).
//!
//! A tenant whose catalog can't be read (a lost DC, two-tier mode where
//! another DC's tier isn't reachable from here) resolves over the etcd
//! scopes only, as before, and is tried again a second later.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_catalog::store::Store;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_meta::{MetaStore, Tier, TierView};
use sprig_settings::{
    MemSettings, Registry, Setting, SettingCtx, SettingValue, TenantScope, TenantSettings,
};
use sprig_shard::TenantMaps;

use crate::catalogs::ClusterPools;
use crate::tree::Directory;

/// How long a cached copy is used before the directory is asked again.
pub const MAX_AGE: Duration = Duration::from_secs(1);
/// Cached tenants per process; the cache is cleared when it is full.
pub const MAX_TENANTS: usize = 10_000;

/// The internal key holding the ancestry.
const ANCESTRY_KEY: &str = "sprig.ancestry";
/// The internal key holding the committed catalog version.
const CATALOG_VERSION_KEY: &str = "sprig.catalog_version";

struct Cached {
    catalog_version: u64,
    checked: Instant,
    rows: Arc<TenantSettings>,
}

/// Every tenant's setting rows a loop or job asked for (see the module
/// docs).
pub struct TenantScopes {
    directory: Directory,
    /// The one etcd's DC tier (single-etcd mode: views of every DC's tier).
    store: Arc<dyn MetaStore>,
    pools: Arc<dyn ClusterPools>,
    registry: &'static Registry,
    cache: Mutex<HashMap<TenantId, Cached>>,
    loading: Mutex<HashSet<TenantId>>,
}

impl std::fmt::Debug for TenantScopes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantScopes")
            .field("cached", &lock(&self.cache).len())
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// `sprig.ancestry`: root first, `[{"id": 1, "name": "p"}, ...]` or bare
/// ids.
fn parse_ancestry(v: Option<&serde_json::Value>) -> Vec<TenantId> {
    v.and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.get("id").unwrap_or(x).as_u64().map(TenantId))
                .collect()
        })
        .unwrap_or_default()
}

/// Builds a tenant's [`TenantSettings`] from its catalog's `settings`
/// rows (every row: tenant, database, object and ancestor scopes, and the
/// internal `sprig.ancestry`).
#[must_use]
pub fn build_rows(
    registry: &'static Registry,
    tenant: TenantId,
    version: u64,
    rows: Vec<sprig_catalog::SettingRow>,
) -> TenantSettings {
    let mut b = TenantSettings::builder(registry, tenant, version);
    let mut ancestry = Vec::new();
    for r in rows {
        if r.is_internal() {
            if r.key == ANCESTRY_KEY && r.scope == sprig_catalog::SettingScope::Tenant {
                ancestry = parse_ancestry(r.value.as_ref());
            }
            continue;
        }
        let scope = TenantScope::from(r.scope);
        match r.entry() {
            Ok(e) => b.put(scope, &r.key, e),
            Err(_) => b.put_invalid(scope, &r.key),
        }
    }
    b.set_ancestry(ancestry);
    b.build()
}

/// `setting` in `ctx` over `etcd` and, when given, the tenant's rows
/// (`ctx`'s tenant is the rows' own): what a gateway would resolve.
#[must_use]
pub fn get<T: SettingValue>(
    etcd: &MemSettings<'_>,
    rows: Option<&TenantSettings>,
    setting: &Setting<T>,
    ctx: &SettingCtx<'_>,
) -> T {
    match rows {
        Some(r) => r
            .resolve(etcd, setting.key, ctx)
            .ok()
            .and_then(|v| T::from_value(&v.value))
            .unwrap_or_else(|| setting.default.clone()),
        None => etcd.get(setting, ctx),
    }
}

impl TenantScopes {
    /// The cache of a process: `global` holds the directory, `store` is
    /// the one etcd's DC tier (other DCs' tiers are views of it), `pools`
    /// reach catalog clusters in any DC.
    ///
    /// # Errors
    /// Internal if the registry is invalid (a bug).
    pub fn new(
        global: Arc<dyn MetaStore>,
        store: Arc<dyn MetaStore>,
        pools: Arc<dyn ClusterPools>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            directory: Directory::new(global),
            store,
            pools,
            registry: Registry::global()?,
            cache: Mutex::new(HashMap::new()),
            loading: Mutex::new(HashSet::new()),
        }))
    }

    /// The cached rows of `tenant`, without waiting: a missing or stale
    /// copy is (re)loaded in the background, and the stale copy is
    /// returned meanwhile. For synchronous callers (a mover job's
    /// settings, a trigger's retry options).
    #[must_use]
    pub fn cached(self: &Arc<Self>, tenant: TenantId) -> Option<Arc<TenantSettings>> {
        let (rows, fresh) = match lock(&self.cache).get(&tenant) {
            Some(c) => (Some(Arc::clone(&c.rows)), c.checked.elapsed() < MAX_AGE),
            None => (None, false),
        };
        if !fresh
            && lock(&self.loading).insert(tenant)
            && let Ok(h) = tokio::runtime::Handle::try_current()
        {
            let me = Arc::clone(self);
            h.spawn(async move {
                if let Err(e) = me.refresh(tenant).await {
                    tracing::debug!(%tenant, error = %e, "tenant settings not read");
                }
                lock(&me.loading).remove(&tenant);
            });
        }
        rows
    }

    /// The rows of `tenant`, checked against the directory when the cached
    /// copy is older than [`MAX_AGE`]; `None` when the catalog can't be
    /// read (the caller resolves over the etcd scopes only).
    pub async fn get(&self, tenant: TenantId) -> Option<Arc<TenantSettings>> {
        if let Some(c) = lock(&self.cache).get(&tenant)
            && c.checked.elapsed() < MAX_AGE
        {
            return Some(Arc::clone(&c.rows));
        }
        match self.refresh(tenant).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(%tenant, error = %e, "tenant settings not read");
                lock(&self.cache).get(&tenant).map(|c| Arc::clone(&c.rows))
            }
        }
    }

    /// The rows of `tenant` as of now: the directory is asked whatever the
    /// cached copy's age (one linearizable etcd read), so a change committed
    /// a moment ago is seen. For rare decisions that must not act on a
    /// stale mode (the approvals' vector gate).
    pub async fn get_fresh(&self, tenant: TenantId) -> Option<Arc<TenantSettings>> {
        match self.refresh(tenant).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(%tenant, error = %e, "tenant settings not read");
                lock(&self.cache).get(&tenant).map(|c| Arc::clone(&c.rows))
            }
        }
    }

    /// Checks the directory and reads the catalog when its version moved.
    async fn refresh(&self, tenant: TenantId) -> Result<Option<Arc<TenantSettings>>> {
        let Some(e) = self.directory.get(tenant).await? else {
            lock(&self.cache).remove(&tenant);
            return Ok(None);
        };
        let b = e.body();
        let version = b.catalog_version;
        {
            let mut cache = lock(&self.cache);
            // The directory allocates a version before the catalog commits
            // it: only a catalog that has committed `version` is current.
            if let Some(c) = cache.get_mut(&tenant)
                && c.catalog_version >= version
            {
                c.checked = Instant::now();
                return Ok(Some(Arc::clone(&c.rows)));
            }
        }
        let (rows, committed) = self.load(tenant, &b.home_dc, version).await?;
        let rows = Arc::new(rows);
        let mut cache = lock(&self.cache);
        if cache.len() >= MAX_TENANTS && !cache.contains_key(&tenant) {
            cache.clear();
        }
        cache.insert(
            tenant,
            Cached {
                catalog_version: committed,
                checked: Instant::now(),
                rows: Arc::clone(&rows),
            },
        );
        Ok(Some(rows))
    }

    /// Reads every settings row of `tenant`'s catalog on the cluster its
    /// home DC's catalog map names, and the catalog version they are at
    /// (`sprig.catalog_version`, read in the same statement).
    async fn load(
        &self,
        tenant: TenantId,
        home: &DcId,
        version: u64,
    ) -> Result<(TenantSettings, u64)> {
        let tier = TierView::over(&self.store, Tier::Dc(home.clone()));
        let (map, _) = TenantMaps::new(tier).load(tenant).await?;
        let cluster = map.catalog_cluster().cloned().ok_or_else(|| {
            SprigError::internal(format!("tenant {tenant}'s catalog map names no cluster"))
        })?;
        let pool = self.pools.writer(&cluster).await?;
        let mut c = crate::catalogs::acquire(&pool, "tenant settings").await?;
        let rows = Store::for_tenant(tenant)
            .settings_since(&mut c, None)
            .await?;
        drop(c);
        let committed = rows
            .iter()
            .find(|r| {
                r.key == CATALOG_VERSION_KEY && r.scope == sprig_catalog::SettingScope::Tenant
            })
            .and_then(|r| r.value.as_ref())
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        Ok((
            build_rows(self.registry, tenant, committed.min(version), rows),
            committed,
        ))
    }

    /// Forgets every cached tenant (tests).
    pub fn clear(&self) {
        lock(&self.cache).clear();
    }

    /// Puts `rows` in the cache as `tenant`'s at `catalog_version`, fresh
    /// for [`MAX_AGE`] (tests that have no catalog).
    pub fn seed(&self, tenant: TenantId, catalog_version: u64, rows: TenantSettings) {
        lock(&self.cache).insert(
            tenant,
            Cached {
                catalog_version,
                checked: Instant::now(),
                rows: Arc::new(rows),
            },
        );
    }
}

/// Workers' mover jobs read their tenant's rows through the cache.
pub struct MoverRows(pub Arc<TenantScopes>);

impl sprig_mover::settings::TenantRows for MoverRows {
    fn rows(&self, tenant: TenantId) -> Option<Arc<TenantSettings>> {
        self.0.cached(tenant)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use serde_json::json;
    use sprig_catalog::{SettingRow, SettingScope};
    use sprig_common::ids::DcId;

    use super::*;
    use crate::failover::settings::STANDBY_DCS;
    use crate::migrate::WRITE_PAUSE_TARGET_MS;

    fn row(scope: SettingScope, key: &str, value: serde_json::Value) -> SettingRow {
        SettingRow {
            scope,
            key: key.to_owned(),
            value: Some(value),
            bounds: None,
            set_by: "op:test".into(),
            set_at: SystemTime::UNIX_EPOCH,
            version: 3,
        }
    }

    #[test]
    fn tenant_rows_win_over_etcd_and_ancestors_apply() {
        let reg = Registry::global().unwrap();
        let fra1 = DcId::new("fra1").unwrap();
        let mut etcd = MemSettings::new(reg);
        let pctx = SettingCtx::process(&fra1);
        etcd.set_value(
            sprig_settings::Level::Dc,
            &pctx,
            STANDBY_DCS.key,
            &sprig_settings::Value::Int(1),
        )
        .unwrap();
        let t = TenantId(7);
        // No rows: the dc value.
        assert_eq!(get(&etcd, None, &STANDBY_DCS, &pctx), 1);
        let rows = build_rows(
            reg,
            t,
            3,
            vec![
                row(SettingScope::Tenant, STANDBY_DCS.key, json!(0)),
                row(
                    SettingScope::Ancestor(TenantId(2)),
                    WRITE_PAUSE_TARGET_MS.key,
                    json!(800),
                ),
                row(
                    SettingScope::Tenant,
                    "sprig.ancestry",
                    json!([{"id": 2, "name": "p"}]),
                ),
                row(SettingScope::Tenant, "sprig.catalog_version", json!(3)),
            ],
        );
        assert_eq!(rows.ancestry(), &[TenantId(2)]);
        // The tenant's own value wins over the dc's.
        assert_eq!(get(&etcd, Some(&rows), &STANDBY_DCS, &pctx), 0);
        // An ancestor's value applies where the tenant has none.
        assert_eq!(get(&etcd, Some(&rows), &WRITE_PAUSE_TARGET_MS, &pctx), 800);
        // A key nobody sets: the default.
        assert_eq!(
            get(
                &etcd,
                Some(&rows),
                &crate::failover::settings::RPO_SECONDS,
                &pctx
            ),
            5
        );
    }

    #[test]
    fn ancestry_parses_both_forms() {
        assert_eq!(
            parse_ancestry(Some(&json!([{"id": 1, "name": "a"}, 5]))),
            vec![TenantId(1), TenantId(5)]
        );
        assert!(parse_ancestry(Some(&json!("x"))).is_empty());
        assert!(parse_ancestry(None).is_empty());
    }
}
