//! [`Settings`]: six-scope resolution for a process, memoized, with watches
//! (chapter 08, "Setting resolution" and "Live application"; build step 5).
//!
//! A gateway holds one [`Settings`] over its [`LiveSettings`] (the etcd
//! scopes). Tenant, database, object and ancestor rows come with each call,
//! from the tenant's catalog snapshot ([`TenantSettings`]), so a statement
//! resolves against the snapshot it started with:
//!
//! ```text
//! let cat = cache.get(tenant, Freshness::Cached { min_version }).await?;
//! let ctx = cat.settings.ctx(&dc).with_db(db).with_object(table);
//! let mode = settings.get(&AUTO_INDEX_MODE, &ctx, Some(&cat.settings));
//! ```
//!
//! Results are memoized per (tenant snapshot version, etcd snapshot): the
//! memo lives in the [`TenantSettings`] (one catalog version) and each
//! result records the etcd snapshot's stamp.
//!
//! Watches:
//! - [`Settings::watch`] and [`Settings::subscribe`]: a receiver bumped
//!   whenever a row of the key changes at any of the six scopes, for any
//!   tenant. etcd changes arrive from the mirrors; tenant changes from the
//!   catalog cache, which calls [`TenantNotify::tenant_loaded`] after each
//!   load with the keys whose rows changed.
//! - [`Settings::watch_resolved`]: a [`ResolvedWatch`] for one key in one
//!   context, which wakes only when the resolved value (value, source,
//!   bounds, clamping) actually changes.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};
use sprig_common::{Result, SprigError};
use tokio::sync::watch;

use super::memo::Memo;
use super::tenant::{ClampedValue, TenantSettings};
use super::{Resolved, def, resolve_layered, walk};
use crate::registry::{AnySetting, Registry, Setting};
use crate::scope::SettingCtx;
use crate::value::SettingValue;
use crate::watch::{LiveSettings, Pattern, Registration};

/// Told by the catalog cache about tenant snapshots, so watches see changes
/// at tenant, database, object and ancestor scope.
pub trait TenantNotify: Send + Sync + 'static {
    /// A snapshot of `settings.tenant()` was loaded; `changed` names the
    /// keys whose rows differ from the previous snapshot (empty on the
    /// first load).
    fn tenant_loaded(&self, settings: &Arc<TenantSettings>, changed: &BTreeSet<&'static str>);
    /// The tenant left the cache.
    fn tenant_evicted(&self, tenant: TenantId);
}

/// An owned [`SettingCtx`], for watches that outlive a statement. The
/// ancestry comes from the tenant's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedCtx {
    /// The evaluating process's DC.
    pub dc: DcId,
    /// Set only for work on one cluster.
    pub cluster: Option<ClusterId>,
    /// The tenant.
    pub tenant: Option<TenantId>,
    /// The database.
    pub db: Option<DbId>,
    /// The object.
    pub object: Option<ObjectId>,
}

impl OwnedCtx {
    /// A process-level context in `dc`.
    #[must_use]
    pub fn process(dc: DcId) -> Self {
        Self {
            dc,
            cluster: None,
            tenant: None,
            db: None,
            object: None,
        }
    }

    /// The owned form of `ctx` (its ancestry is dropped).
    #[must_use]
    pub fn from_ctx(ctx: &SettingCtx<'_>) -> Self {
        Self {
            dc: ctx.dc.clone(),
            cluster: ctx.cluster.cloned(),
            tenant: ctx.tenant,
            db: ctx.db,
            object: ctx.object,
        }
    }

    /// The borrowed form, with no ancestry.
    #[must_use]
    pub fn borrow(&self) -> SettingCtx<'_> {
        SettingCtx {
            dc: &self.dc,
            cluster: self.cluster.as_ref(),
            tenant: self.tenant,
            ancestry: &[],
            db: self.db,
            object: self.object,
        }
    }
}

struct Sub {
    patterns: Vec<Pattern>,
    tx: watch::Sender<u64>,
    _reg: Registration,
}

struct Inner {
    live: LiveSettings,
    /// Results of process-level contexts (no tenant).
    memo: Memo,
    /// The newest rows of tenants some [`ResolvedWatch`] follows.
    tenants: Mutex<HashMap<TenantId, watch::Sender<Arc<TenantSettings>>>>,
    subs: Mutex<Vec<Sub>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Resolution across all six scopes for this process. Cloning shares it.
#[derive(Clone)]
pub struct Settings {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("live", &self.inner.live)
            .finish_non_exhaustive()
    }
}

impl Settings {
    /// Resolution over `live`'s etcd scopes plus the tenant rows passed to
    /// each call.
    #[must_use]
    pub fn new(live: LiveSettings) -> Self {
        Self {
            inner: Arc::new(Inner {
                live,
                memo: Memo::default(),
                tenants: Mutex::new(HashMap::new()),
                subs: Mutex::new(Vec::new()),
            }),
        }
    }

    /// The etcd scopes.
    #[must_use]
    pub fn live(&self) -> &LiveSettings {
        &self.inner.live
    }

    /// The registry keys resolve against.
    #[must_use]
    pub fn registry(&self) -> &'static Registry {
        self.inner.live.registry()
    }

    /// Resolves `key` in `ctx`: global, dc and cluster from the etcd
    /// mirrors, then the ancestors, tenant, database and object rows of
    /// `tenant` (the snapshot the statement runs with). Memoized per
    /// (tenant snapshot, etcd snapshot).
    ///
    /// # Errors
    /// 39081 if the key is unknown; `Internal` if `ctx` names a tenant
    /// other than `tenant`'s.
    pub fn resolve(
        &self,
        key: &str,
        ctx: &SettingCtx<'_>,
        tenant: Option<&TenantSettings>,
    ) -> Result<Resolved> {
        let etcd = self.inner.live.snapshot();
        match tenant {
            Some(ts) => ts.resolve(&etcd, key, ctx),
            None if ctx.tenant.is_none() => {
                let def = def(self.registry(), key)?;
                let stamp = etcd.stamp();
                if let Some(r) = self.inner.memo.get(def.key(), ctx, stamp) {
                    return Ok(r);
                }
                let r = walk(def, ctx, |s| etcd.rows.get(&(s.clone(), def.key())));
                self.inner.memo.put(def.key(), ctx, stamp, &r);
                Ok(r)
            }
            None => resolve_layered(&etcd, None, key, ctx),
        }
    }

    /// The typed value of `setting` in `ctx`, or its default if resolution
    /// fails (a key missing from this registry).
    #[must_use]
    pub fn get<T: SettingValue>(
        &self,
        setting: &Setting<T>,
        ctx: &SettingCtx<'_>,
        tenant: Option<&TenantSettings>,
    ) -> T {
        self.resolve(setting.key, ctx, tenant)
            .ok()
            .and_then(|r| T::from_value(&r.value))
            .unwrap_or_else(|| setting.default.clone())
    }

    /// A receiver bumped whenever a row of `setting` changes at any scope,
    /// for any tenant cached here.
    #[must_use]
    pub fn watch(&self, setting: &'static dyn AnySetting) -> watch::Receiver<u64> {
        self.subscribe(&[setting.key()])
    }

    /// A receiver bumped whenever a row of a key matching `patterns`
    /// changes at any scope. A pattern ending in `*` matches by prefix.
    #[must_use]
    pub fn subscribe(&self, patterns: &[&str]) -> watch::Receiver<u64> {
        let (tx, rx) = watch::channel(0_u64);
        let bump = tx.clone();
        let reg = self
            .inner
            .live
            .on_change("settings.watch", patterns, move |_| {
                bump.send_modify(|g: &mut u64| *g = g.wrapping_add(1));
                Ok(())
            });
        let mut subs = lock(&self.inner.subs);
        subs.retain(|s| !s.tx.is_closed());
        subs.push(Sub {
            patterns: patterns.iter().map(|p| Pattern::parse(p)).collect(),
            tx,
            _reg: reg,
        });
        rx
    }

    /// Follows the resolved value of `key` in `ctx`. With `tenant`, the
    /// watch follows that tenant's later snapshots as the catalog cache
    /// loads them; it starts from the snapshot given.
    ///
    /// # Errors
    /// 39081 if the key is unknown; `Internal` if `ctx` names a tenant
    /// other than `tenant`'s.
    pub fn watch_resolved(
        &self,
        key: &str,
        mut ctx: OwnedCtx,
        tenant: Option<Arc<TenantSettings>>,
    ) -> Result<ResolvedWatch> {
        let key = def(self.registry(), key)?.key();
        let tenant_rx = match tenant {
            Some(ts) => {
                if ctx.tenant.is_some_and(|t| t != ts.tenant()) {
                    return Err(SprigError::internal(
                        "watch context names another tenant than its rows",
                    ));
                }
                ctx.tenant = Some(ts.tenant());
                let mut map = lock(&self.inner.tenants);
                map.retain(|_, tx| tx.receiver_count() > 0);
                let tx = map
                    .entry(ts.tenant())
                    .or_insert_with(|| watch::channel(Arc::clone(&ts)).0);
                if tx.borrow().version() < ts.version() {
                    tx.send_replace(ts);
                }
                Some(tx.subscribe())
            }
            None => None,
        };
        let etcd = self.inner.live.subscribe(&[key]);
        let mut w = ResolvedWatch {
            settings: self.clone(),
            key,
            ctx,
            etcd,
            tenant: tenant_rx,
            tenant_closed: false,
            last: None,
        };
        let r = w.resolve_now()?;
        w.last = Some(r);
        Ok(w)
    }

    /// Stored values of `tenant` that resolution clamps or ignores (see
    /// [`TenantSettings::clamped`]), against the current etcd snapshot.
    #[must_use]
    pub fn clamped(
        &self,
        tenant: &TenantSettings,
        dc: &DcId,
        object_db: &dyn Fn(ObjectId) -> Option<DbId>,
    ) -> Vec<ClampedValue> {
        tenant.clamped(&self.inner.live.snapshot(), dc, object_db)
    }

    fn notify(&self, changed: &BTreeSet<&'static str>) {
        if changed.is_empty() {
            return;
        }
        let mut subs = lock(&self.inner.subs);
        subs.retain(|s| !s.tx.is_closed());
        for s in subs.iter() {
            if changed
                .iter()
                .any(|k| s.patterns.iter().any(|p| p.matches(k)))
            {
                s.tx.send_modify(|g: &mut u64| *g = g.wrapping_add(1));
            }
        }
    }
}

impl TenantNotify for Settings {
    fn tenant_loaded(&self, settings: &Arc<TenantSettings>, changed: &BTreeSet<&'static str>) {
        if let Some(tx) = lock(&self.inner.tenants).get(&settings.tenant())
            && tx.borrow().version() < settings.version()
        {
            tx.send_replace(Arc::clone(settings));
        }
        self.notify(changed);
    }

    fn tenant_evicted(&self, tenant: TenantId) {
        let mut map = lock(&self.inner.tenants);
        if map.get(&tenant).is_some_and(|tx| tx.receiver_count() == 0) {
            map.remove(&tenant);
        }
    }
}

/// Follows one key's resolved value in one context
/// ([`Settings::watch_resolved`]).
pub struct ResolvedWatch {
    settings: Settings,
    key: &'static str,
    ctx: OwnedCtx,
    etcd: watch::Receiver<u64>,
    tenant: Option<watch::Receiver<Arc<TenantSettings>>>,
    tenant_closed: bool,
    last: Option<Resolved>,
}

impl std::fmt::Debug for ResolvedWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedWatch")
            .field("key", &self.key)
            .field("ctx", &self.ctx)
            .field("last", &self.last)
            .finish_non_exhaustive()
    }
}

impl ResolvedWatch {
    /// The key.
    #[must_use]
    pub fn key(&self) -> &'static str {
        self.key
    }

    /// The value as last seen.
    ///
    /// # Panics
    /// Never: the watch resolves once when created.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn current(&self) -> &Resolved {
        // Set in `watch_resolved` before the watch is handed out.
        #[allow(clippy::unwrap_used)]
        self.last.as_ref().unwrap()
    }

    /// Waits until the resolved value differs from the last one seen, and
    /// returns it. Row changes that leave the result as it was don't wake
    /// the caller.
    ///
    /// # Errors
    /// `Internal` if the settings stopped (the [`LiveSettings`] task
    /// ended); resolution errors as [`Settings::resolve`].
    pub async fn changed(&mut self) -> Result<Resolved> {
        loop {
            let tenant = if self.tenant_closed {
                None
            } else {
                self.tenant.as_mut()
            };
            tokio::select! {
                r = self.etcd.changed() => {
                    if r.is_err() {
                        return Err(SprigError::internal("settings mirrors stopped"));
                    }
                }
                r = async {
                    match tenant {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if r.is_err() {
                        self.tenant_closed = true;
                    }
                }
            }
            let r = self.resolve_now()?;
            if self.last.as_ref() != Some(&r) {
                self.last = Some(r.clone());
                return Ok(r);
            }
        }
    }

    fn resolve_now(&mut self) -> Result<Resolved> {
        let ts = self
            .tenant
            .as_mut()
            .map(|rx| Arc::clone(&rx.borrow_and_update()));
        self.etcd.borrow_and_update();
        self.settings
            .resolve(self.key, &self.ctx.borrow(), ts.as_deref())
    }
}
