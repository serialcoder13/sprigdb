//! The gateway catalog cache (chapter 08, "Cache load and invalidation";
//! build step 4).
//!
//! ```text
//! get(tenant):
//!   want = directory[tenant].catalog_version        // global-tier watch
//!   if cached e with e.version >= want and e.age < catalog.cache_ttl_seconds: return e
//!   return single_flight(tenant, load(tenant, e, want))
//!
//! load(tenant, e, want):
//!   src = local replica of the catalog cluster if healthy, else the writer
//!   since = e.version if e and e.version >= sprig.purged_version else none
//!   loop until catalog.version_wait_ms:
//!     snap = src: consistent read-only snapshot; rows changed after since; modules' load
//!     if snap.version >= want: break
//!     on deadline: switch src to the writer; if already the writer, accept the gap
//!   cache[tenant] = e.apply(snap) if since else build(snap)
//!   settings.notify(tenant, keys whose rows changed)
//! ```
//!
//! The pieces: [`TenantCatalog`] (immutable snapshots), [`CatalogSource`]
//! ([`MariaSource`]; [`mem::MemSource`] for tests), [`DirectoryFeed`]
//! ([`DirectoryWatch`]) and [`CatalogExtension`]s for other chapters'
//! modules. [`TenantCache`] ties them together:
//!
//! - **Single flight.** One load per tenant at a time, run on its own task
//!   so a reader that gives up doesn't cancel it; every reader waiting for
//!   that tenant shares the result.
//! - **Freshness** ([`Freshness`]). A statement asks for
//!   `Cached { min_version }` with its session's last own change (read
//!   your own DDL): it gets the newest cached snapshot at once, never
//!   waiting behind a load, and a newer version known from the directory
//!   is loaded in the background. Only a snapshot below `min_version`, or
//!   none at all, makes it wait. `Latest` (session start, after a name
//!   miss) waits until the cache has the directory's current versions.
//! - **Invalidation.** The [`DirectoryFeed`] follows each cached tenant's
//!   directory entry. Tenants pinned by sessions here ([`TenantCache::pin`])
//!   reload as soon as their entry moves; others reload on next use. Every
//!   `catalog.cache_ttl_seconds` without a confirmed version, the next use
//!   re-reads the entry (the backstop for a missed watch event).
//! - **Name misses.** [`TenantCache::reload_after_miss`] forces a writer
//!   load, at most once per second per tenant.
//! - **Memory.** Beyond `catalog.cache_max_tenants` or `catalog.cache_max_mb`
//!   the least recently used tenants are evicted, unpinned ones first.
//!   Snapshots in use stay valid: they are `Arc`s.
//! - **Failures.** A failed load keeps the previous snapshot: cached reads
//!   continue while the catalog or the global tier is down, and only a
//!   reader that needs a newer version than cached gets the error.
//!
//! Metrics: `sprig_catalog_cache_tenants`, `sprig_catalog_cache_bytes`,
//! `sprig_catalog_loads_total` (`kind` full or incremental, `source`
//! replica or writer, `outcome` ok, gap or error),
//! `sprig_catalog_load_seconds`, and `sprig_catalog_version_lag_seconds`:
//! how long the most-behind cached tenant has known of a newer directory
//! version than it holds. The directory watch's staleness is reported as
//! `sprig_meta_cache_staleness_seconds` (`source` `g`, `prefix`
//! `tenants/`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use sprig_common::ids::{DcId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_settings::{LiveSettings, MemSettings, Registration, Registry, SettingCtx, TenantNotify};
use tokio::sync::{broadcast, watch};

pub mod directory;
pub mod ext;
pub mod mem;
pub mod settings;
pub mod snapshot;
pub mod source;

mod cow;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;

pub use directory::{DirectoryFeed, DirectoryWatch};
pub use ext::{CatalogExtension, ExtState, ExtensionStates, SnapshotTx, module_of};
pub use snapshot::{DatabaseIndex, Loaded, ObjectIndex, TenantCatalog, TenantRef};
pub use source::{CatalogSource, MariaSource, Node};

use crate::version::DirectoryVersions;

/// The cache's limits and timings (chapter 08's `catalog.*` keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheConfig {
    /// `catalog.cache_max_tenants`.
    pub max_tenants: usize,
    /// `catalog.cache_max_mb`, in bytes.
    pub max_bytes: usize,
    /// `catalog.cache_ttl_seconds`.
    pub ttl: Duration,
    /// `catalog.version_wait_ms`.
    pub version_wait: Duration,
    /// Deadline of one load from the source.
    pub load_timeout: Duration,
    /// Writer loads after name misses: at most one per this interval.
    pub miss_interval: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_tenants: 20_000,
            max_bytes: 512 << 20,
            ttl: Duration::from_secs(300),
            version_wait: Duration::from_millis(2000),
            load_timeout: Duration::from_secs(10),
            miss_interval: Duration::from_secs(1),
        }
    }
}

impl CacheConfig {
    /// The keys' values in `dc` from `rows` (an etcd snapshot); the load
    /// timeout and miss interval are kept from `self`.
    #[must_use]
    pub fn from_settings(self, rows: &MemSettings<'_>, dc: &DcId) -> Self {
        let ctx = SettingCtx::process(dc);
        let n = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
        Self {
            max_tenants: n(rows.get(&settings::CACHE_MAX_TENANTS, &ctx)),
            max_bytes: n(rows.get(&settings::CACHE_MAX_MB, &ctx)).saturating_mul(1 << 20),
            ttl: Duration::from_secs(rows.get(&settings::CACHE_TTL_SECONDS, &ctx)),
            version_wait: Duration::from_millis(rows.get(&settings::VERSION_WAIT_MS, &ctx)),
            ..self
        }
    }
}

/// How fresh a snapshot [`TenantCache::get`] must return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Any snapshot at or above `min_version`: a session passes the
    /// version of its own last catalog change, so it reads its own DDL.
    /// Never waits while such a snapshot is cached.
    Cached {
        /// The lowest acceptable catalog version.
        min_version: u64,
    },
    /// At least the directory's catalog and auth versions as this
    /// process's directory watch has delivered them (read through on first
    /// use; the chapter's `get`): waits for a load when behind. A session's
    /// own change can be newer than the watch has delivered, so read your
    /// own DDL with `Cached { min_version }`. If the load fails, a cached
    /// snapshot is returned anyway ("cached reads continue").
    Latest,
}

impl Freshness {
    /// Any cached snapshot.
    pub const ANY: Freshness = Freshness::Cached { min_version: 0 };
}

/// Catalog and auth versions a reader or the directory asks for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Want {
    catalog: u64,
    auth: u64,
}

impl Want {
    fn of(c: &TenantCatalog) -> Self {
        Self {
            catalog: c.version,
            auth: c.auth_version,
        }
    }

    fn dir(d: DirectoryVersions) -> Self {
        Self {
            catalog: d.catalog_version,
            auth: d.auth_version,
        }
    }

    fn max(self, o: Want) -> Self {
        Self {
            catalog: self.catalog.max(o.catalog),
            auth: self.auth.max(o.auth),
        }
    }

    fn within(self, reached: Want) -> bool {
        self.catalog <= reached.catalog && self.auth <= reached.auth
    }
}

#[derive(Debug, Default)]
struct SlotState {
    current: Option<Arc<TenantCatalog>>,
    /// The versions `current` stands for: its own, or higher when the
    /// loader accepted a gap (allocated, never committed versions).
    reached: Want,
    /// The highest versions asked for.
    want: Want,
    /// When `current` was last confirmed against the directory.
    confirmed_at: Option<Instant>,
    loading: bool,
    refreshing: bool,
    /// Completed loads, failed ones included.
    loads: u64,
    error: Option<Arc<SprigError>>,
    writer_requested: bool,
    writer_requested_at: Option<Instant>,
    writer_done_at: Option<Instant>,
    /// Since when the directory is known to be ahead of `current`.
    behind_since: Option<Instant>,
}

struct Slot {
    tenant: TenantId,
    used: AtomicU64,
    bytes: AtomicUsize,
    state: Mutex<SlotState>,
    published: watch::Sender<u64>,
}

impl Slot {
    fn lock(&self) -> MutexGuard<'_, SlotState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Inner {
    registry: &'static Registry,
    source: Arc<dyn CatalogSource>,
    feed: Arc<dyn DirectoryFeed>,
    notify: Option<Arc<dyn TenantNotify>>,
    config: RwLock<CacheConfig>,
    slots: RwLock<HashMap<TenantId, Arc<Slot>>>,
    pins: Mutex<HashMap<TenantId, usize>>,
    tick: AtomicU64,
    bytes: AtomicUsize,
    /// The catalog load feed ([`TenantCache::loads`]).
    loads: broadcast::Sender<CatalogLoaded>,
}

/// One published load of a tenant's catalog: what the catalog load feed
/// ([`TenantCache::loads`]) carries. Chapter 14's auth change feed
/// (revocation) and chapter 29's privilege cache follow it, the way
/// settings watches follow [`TenantNotify`].
#[derive(Debug, Clone)]
pub struct CatalogLoaded {
    /// The tenant.
    pub tenant: TenantId,
    /// The snapshot it replaced; `None` on the tenant's first load here.
    pub prev: Option<Arc<TenantCatalog>>,
    /// The new snapshot.
    pub current: Arc<TenantCatalog>,
}

/// How many loads the feed buffers per subscriber before it lags.
const LOAD_FEED_CAPACITY: usize = 1024;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn copy_err(e: &SprigError) -> SprigError {
    SprigError {
        kind: e.kind,
        message: e.message.clone(),
        retryable: e.retryable,
        source: None,
    }
}

/// Cancels the background task when the last handle drops.
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The gateway's cache of tenant catalogs (see the module docs). Cloning
/// shares it; its background task stops when the last clone drops.
#[derive(Clone)]
pub struct TenantCache {
    inner: Arc<Inner>,
    _stop: Arc<StopOnDrop>,
}

impl std::fmt::Debug for TenantCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantCache")
            .field("tenants", &self.len())
            .field("bytes", &self.bytes())
            .field("config", &self.config())
            .finish_non_exhaustive()
    }
}

/// Keeps a tenant marked as having sessions here: it reloads as soon as
/// its directory entry moves, and is evicted last. Dropping it unpins.
#[must_use = "dropping a TenantPin unpins the tenant"]
pub struct TenantPin {
    inner: Weak<Inner>,
    tenant: TenantId,
}

impl std::fmt::Debug for TenantPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantPin")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl Drop for TenantPin {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut pins = lock(&inner.pins);
            if let Some(n) = pins.get_mut(&self.tenant) {
                *n -= 1;
                if *n == 0 {
                    pins.remove(&self.tenant);
                }
            }
        }
    }
}

impl TenantCache {
    /// Starts a cache loading from `source`, invalidated by `feed`, with
    /// keys interned through `registry`. With `notify` (normally the
    /// process's [`sprig_settings::Settings`]), every load reports the
    /// setting keys whose rows changed.
    #[must_use]
    pub fn start(
        registry: &'static Registry,
        source: Arc<dyn CatalogSource>,
        feed: Arc<dyn DirectoryFeed>,
        notify: Option<Arc<dyn TenantNotify>>,
        config: CacheConfig,
    ) -> Self {
        let inner = Arc::new(Inner {
            registry,
            source,
            feed,
            notify,
            config: RwLock::new(config),
            slots: RwLock::new(HashMap::new()),
            pins: Mutex::new(HashMap::new()),
            tick: AtomicU64::new(0),
            bytes: AtomicUsize::new(0),
            loads: broadcast::channel(LOAD_FEED_CAPACITY).0,
        });
        let stop = CancellationToken::new();
        tokio::spawn(run(
            Arc::downgrade(&inner),
            inner.feed.changes(),
            stop.clone(),
        ));
        Self {
            inner,
            _stop: Arc::new(StopOnDrop(stop)),
        }
    }

    /// The current limits and timings.
    #[must_use]
    pub fn config(&self) -> CacheConfig {
        *self
            .inner
            .config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Changes the limits and timings, evicting at once if the new limits
    /// are lower.
    pub fn set_config(&self, config: CacheConfig) {
        *self
            .inner
            .config
            .write()
            .unwrap_or_else(PoisonError::into_inner) = config;
        self.inner.enforce(None);
    }

    /// Follows `live`'s `catalog.*` keys: applies their values now and
    /// after every change. Dropping the registration stops following.
    pub fn follow_settings(&self, live: &LiveSettings) -> Registration {
        let dc = live.stores().dc_id().clone();
        self.set_config(self.config().from_settings(&live.snapshot(), &dc));
        let weak = Arc::downgrade(&self.inner);
        live.on_change(
            "catalog_cache",
            &["catalog.cache_*", "catalog.version_wait_ms"],
            move |rows| {
                if let Some(inner) = weak.upgrade() {
                    let cfg = {
                        let mut c = inner.config.write().unwrap_or_else(PoisonError::into_inner);
                        *c = c.from_settings(rows, &dc);
                        *c
                    };
                    tracing::info!(?cfg, "catalog cache settings changed");
                    inner.enforce(None);
                }
                Ok(())
            },
        )
    }

    /// The catalog load feed: every snapshot this cache publishes, with the
    /// one it replaced, in load order per tenant. Tenants pinned by sessions
    /// here reload as soon as their directory entry moves, so a change
    /// reaches their subscribers within the global tier's watch delay plus
    /// one load. A subscriber that falls more than 1024 loads behind gets
    /// `Lagged` and should re-read what it follows from
    /// [`cached`](Self::cached).
    #[must_use]
    pub fn loads(&self) -> broadcast::Receiver<CatalogLoaded> {
        self.inner.loads.subscribe()
    }

    /// `tenant`'s catalog, as fresh as `fresh` requires (see
    /// [`Freshness`]).
    ///
    /// # Errors
    /// 39070 if the catalog (or, for `Latest` with nothing cached, the
    /// global tier) is unreachable and no acceptable snapshot is cached;
    /// 39072 if the tenant has no directory entry or catalog; 39073 if the
    /// catalog's format is newer than this binary reads.
    pub async fn get(&self, tenant: TenantId, fresh: Freshness) -> Result<Arc<TenantCatalog>> {
        let slot = self.inner.slot(tenant);
        let need = match fresh {
            Freshness::Cached { min_version } => Want {
                catalog: min_version,
                auth: 0,
            },
            Freshness::Latest => match self.inner.feed.versions(tenant).await {
                Ok(d) => Want::dir(d),
                Err(e) => {
                    // The global tier is down: caches serve.
                    if let Some(c) = slot.lock().current.clone() {
                        tracing::debug!(%tenant, error = %e, "directory unreachable; serving the cached catalog");
                        return Ok(c);
                    }
                    return Err(e);
                }
            },
        };
        self.inner
            .wait_for(&slot, need, matches!(fresh, Freshness::Latest))
            .await
    }

    /// `tenant`'s cached catalog, without loading or waiting.
    #[must_use]
    pub fn cached(&self, tenant: TenantId) -> Option<Arc<TenantCatalog>> {
        let slot = self
            .inner
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned()?;
        slot.lock().current.clone()
    }

    /// Marks `tenant` as having a session here (see [`TenantPin`]).
    pub fn pin(&self, tenant: TenantId) -> TenantPin {
        *lock(&self.inner.pins).entry(tenant).or_insert(0) += 1;
        TenantPin {
            inner: Arc::downgrade(&self.inner),
            tenant,
        }
    }

    /// After a name miss, or a backend "table doesn't exist": loads from
    /// the catalog's writer and returns the new snapshot, at most once per
    /// `miss_interval` per tenant; `None` when a writer load ran (or was
    /// asked for) more recently, and the miss stands.
    ///
    /// # Errors
    /// As [`get`](Self::get).
    pub async fn reload_after_miss(&self, tenant: TenantId) -> Result<Option<Arc<TenantCatalog>>> {
        let slot = self.inner.slot(tenant);
        let interval = self.config().miss_interval;
        let (asked, seen, mut rx) = {
            let mut st = slot.lock();
            let now = Instant::now();
            if st
                .writer_requested_at
                .is_some_and(|t| now.duration_since(t) < interval)
            {
                return Ok(None);
            }
            st.writer_requested = true;
            st.writer_requested_at = Some(now);
            let seen = st.loads;
            let rx = slot.published.subscribe();
            self.inner.start_load(&slot, &mut st);
            (now, seen, rx)
        };
        let limit = self.inner.wait_limit();
        let waited = tokio::time::timeout(limit, async {
            loop {
                {
                    let st = slot.lock();
                    if st.writer_done_at.is_some_and(|t| t >= asked) {
                        return Ok(st.current.clone());
                    }
                    if st.loads > seen
                        && !st.loading
                        && let Some(e) = &st.error
                    {
                        return Err(copy_err(e));
                    }
                }
                if rx.changed().await.is_err() {
                    return Err(SprigError::internal("catalog cache slot dropped"));
                }
            }
        })
        .await;
        waited.unwrap_or_else(|_| Err(load_timeout(tenant, limit)))
    }

    /// Drops `tenant` from the cache; `true` if it was cached.
    #[must_use = "whether the tenant was cached"]
    pub fn evict(&self, tenant: TenantId) -> bool {
        self.inner.evict(tenant)
    }

    /// Number of tenants in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether no tenant is cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimated memory of every cached snapshot.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.inner.bytes.load(Ordering::Relaxed)
    }

    /// How long the most-behind cached tenant has known of a newer
    /// directory version than it holds (`sprig_catalog_version_lag_seconds`).
    #[must_use]
    pub fn version_lag(&self) -> Duration {
        self.inner.version_lag()
    }
}

fn load_timeout(tenant: TenantId, limit: Duration) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!(
            "catalog of tenant {tenant} not loaded within {} ms",
            limit.as_millis()
        ),
    )
}

impl Inner {
    fn config(&self) -> CacheConfig {
        *self.config.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_limit(&self) -> Duration {
        let c = self.config();
        c.version_wait + c.load_timeout * 2
    }

    fn pinned(&self, tenant: TenantId) -> bool {
        lock(&self.pins).contains_key(&tenant)
    }

    /// The tenant's slot, created if missing; marks it used.
    fn slot(self: &Arc<Self>, tenant: TenantId) -> Arc<Slot> {
        let tick = self.tick.fetch_add(1, Ordering::Relaxed) + 1;
        let found = self
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned();
        let slot = found.unwrap_or_else(|| {
            let mut slots = self.slots.write().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(slots.entry(tenant).or_insert_with(|| {
                Arc::new(Slot {
                    tenant,
                    used: AtomicU64::new(tick),
                    bytes: AtomicUsize::new(0),
                    state: Mutex::new(SlotState::default()),
                    published: watch::channel(0).0,
                })
            }))
        });
        slot.used.store(tick, Ordering::Relaxed);
        slot
    }

    async fn wait_for(
        self: &Arc<Self>,
        slot: &Arc<Slot>,
        need: Want,
        latest: bool,
    ) -> Result<Arc<TenantCatalog>> {
        let limit = self.wait_limit();
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let (mut rx, seen) = {
                let mut st = slot.lock();
                if let Some(cur) = st.current.clone()
                    && need.within(st.reached)
                {
                    self.revalidate(slot, &mut st);
                    return Ok(cur);
                }
                st.want = st.want.max(need);
                self.start_load(slot, &mut st);
                (slot.published.subscribe(), st.loads)
            };
            loop {
                match tokio::time::timeout_at(deadline, rx.changed()).await {
                    Err(_) => {
                        if latest && let Some(c) = slot.lock().current.clone() {
                            return Ok(c);
                        }
                        return Err(load_timeout(slot.tenant, limit));
                    }
                    Ok(Err(_)) => return Err(SprigError::internal("catalog cache slot dropped")),
                    Ok(Ok(())) => {}
                }
                let st = slot.lock();
                if st.loads == seen {
                    continue;
                }
                if let Some(cur) = &st.current
                    && need.within(st.reached)
                {
                    return Ok(Arc::clone(cur));
                }
                if let Some(e) = &st.error
                    && !st.loading
                {
                    if latest && let Some(c) = &st.current {
                        tracing::debug!(tenant = %slot.tenant, error = %e.message, "load failed; serving the cached catalog");
                        return Ok(Arc::clone(c));
                    }
                    return Err(copy_err(e));
                }
                break;
            }
        }
    }

    /// With a usable snapshot in hand: loads in the background if the
    /// directory is known to be ahead, or re-reads the directory entry if
    /// the snapshot hasn't been confirmed for `catalog.cache_ttl_seconds`.
    fn revalidate(self: &Arc<Self>, slot: &Arc<Slot>, st: &mut SlotState) {
        if st.loading {
            return;
        }
        if let Some(d) = self.feed.cached(slot.tenant) {
            let d = Want::dir(d);
            if !d.within(st.reached) {
                st.want = st.want.max(d);
                self.start_load(slot, st);
                return;
            }
        }
        let ttl = self.config().ttl;
        if st.refreshing || st.confirmed_at.is_some_and(|t| t.elapsed() < ttl) {
            return;
        }
        st.refreshing = true;
        let inner = Arc::clone(self);
        let slot = Arc::clone(slot);
        tokio::spawn(async move {
            let r = inner.feed.refresh(slot.tenant).await;
            let mut st = slot.lock();
            st.refreshing = false;
            match r {
                Ok(d) if Want::dir(d).within(st.reached) => st.confirmed_at = Some(Instant::now()),
                Ok(d) => {
                    st.want = st.want.max(Want::dir(d));
                    st.behind_since.get_or_insert_with(Instant::now);
                    inner.start_load(&slot, &mut st);
                }
                Err(e) => {
                    tracing::debug!(tenant = %slot.tenant, error = %e, "directory re-read failed");
                }
            }
        });
    }

    fn start_load(self: &Arc<Self>, slot: &Arc<Slot>, st: &mut SlotState) {
        if st.loading {
            return;
        }
        st.loading = true;
        tokio::spawn(load_task(Arc::clone(self), Arc::clone(slot)));
    }

    /// The chapter's `load(tenant, e, want)`: from a replica until the
    /// version arrives or `catalog.version_wait_ms` passes, then from the
    /// writer; a version the writer doesn't have either is a gap, accepted.
    /// Returns the snapshot and whether a gap was accepted.
    async fn load_until(
        &self,
        tenant: TenantId,
        mut prev: Option<Arc<TenantCatalog>>,
        target: Want,
        writer: bool,
    ) -> Result<(Arc<TenantCatalog>, bool)> {
        let cfg = self.config();
        let mut node = if writer { Node::Writer } else { Node::Replica };
        let deadline = Instant::now() + cfg.version_wait;
        let mut pause = Duration::from_millis(5);
        loop {
            let started = Instant::now();
            let r = tokio::time::timeout(
                cfg.load_timeout,
                self.source.load(tenant, prev.as_deref(), node),
            )
            .await
            .unwrap_or_else(|_| Err(load_timeout(tenant, cfg.load_timeout)));
            metrics::histogram!("sprig_catalog_load_seconds")
                .record(started.elapsed().as_secs_f64());
            let loaded = match r {
                Ok(l) => l,
                Err(e) => {
                    count_load(prev.is_some(), node, "error");
                    return Err(e);
                }
            };
            let incremental = loaded.snapshot.since.is_some();
            let cat = match &prev {
                Some(p) => {
                    let next = p.apply(loaded);
                    // A replica behind what we hold: keep what we hold.
                    if next.version < p.version || next.auth_version < p.auth_version {
                        Arc::clone(p)
                    } else {
                        Arc::new(next)
                    }
                }
                None => Arc::new(TenantCatalog::build(self.registry, tenant, loaded)),
            };
            if target.within(Want::of(&cat)) {
                count_load(incremental, node, "ok");
                return Ok((cat, false));
            }
            if Instant::now() >= deadline {
                if node == Node::Writer {
                    count_load(incremental, node, "gap");
                    tracing::info!(%tenant, have = cat.version, want = target.catalog, "catalog version not committed; accepting the gap");
                    return Ok((cat, true));
                }
                count_load(incremental, node, "ok");
                node = Node::Writer;
                prev = Some(cat);
                continue;
            }
            count_load(incremental, node, "ok");
            prev = Some(cat);
            tokio::time::sleep(pause.min(deadline.saturating_duration_since(Instant::now()))).await;
            pause = (pause * 2).min(Duration::from_millis(100));
        }
    }

    /// Reacts to a directory event for `tenant`.
    fn directory_moved(self: &Arc<Self>, tenant: TenantId) {
        let Some(slot) = self
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned()
        else {
            return;
        };
        let Some(d) = self.feed.cached(tenant) else {
            return;
        };
        let d = Want::dir(d);
        let mut st = slot.lock();
        if d.within(st.reached) {
            return;
        }
        st.behind_since.get_or_insert_with(Instant::now);
        if self.pinned(tenant) && st.current.is_some() {
            st.want = st.want.max(d);
            self.start_load(&slot, &mut st);
        }
    }

    fn evict(&self, tenant: TenantId) -> bool {
        let removed = self
            .slots
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&tenant);
        let Some(slot) = removed else {
            return false;
        };
        let b = slot.bytes.swap(0, Ordering::Relaxed);
        self.bytes.fetch_sub(b, Ordering::Relaxed);
        self.feed.forget(tenant);
        if let Some(n) = &self.notify {
            n.tenant_evicted(tenant);
        }
        tracing::debug!(%tenant, bytes = b, "catalog evicted");
        true
    }

    /// Evicts least recently used tenants, unpinned first, until within
    /// both limits. `keep` (just loaded) is never evicted, nor is a tenant
    /// whose load is running.
    fn enforce(&self, keep: Option<TenantId>) {
        let cfg = self.config();
        let over = |n: usize, b: usize| n > cfg.max_tenants || b > cfg.max_bytes;
        let (n, b) = (
            self.slots
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .len(),
            self.bytes.load(Ordering::Relaxed),
        );
        if !over(n, b) {
            self.report();
            return;
        }
        let pins = lock(&self.pins).clone();
        let mut candidates: Vec<(bool, u64, TenantId)> = self
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|s| Some(s.tenant) != keep && !s.lock().loading)
            .map(|s| {
                (
                    pins.contains_key(&s.tenant),
                    s.used.load(Ordering::Relaxed),
                    s.tenant,
                )
            })
            .collect();
        candidates.sort_unstable();
        for (_, _, t) in candidates {
            let (n, b) = (
                self.slots
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .len(),
                self.bytes.load(Ordering::Relaxed),
            );
            if !over(n, b) {
                break;
            }
            self.evict(t);
        }
        self.report();
    }

    fn version_lag(&self) -> Duration {
        self.slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter_map(|s| s.lock().behind_since.map(|t| t.elapsed()))
            .max()
            .unwrap_or(Duration::ZERO)
    }

    #[allow(clippy::cast_precision_loss)] // gauges
    fn report(&self) {
        let n = self
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        metrics::gauge!("sprig_catalog_cache_tenants").set(n as f64);
        metrics::gauge!("sprig_catalog_cache_bytes").set(self.bytes.load(Ordering::Relaxed) as f64);
    }
}

fn count_load(incremental: bool, node: Node, outcome: &'static str) {
    let kind = if incremental { "incremental" } else { "full" };
    metrics::counter!("sprig_catalog_loads_total", "kind" => kind, "source" => node.label(), "outcome" => outcome)
        .increment(1);
}

/// One tenant's single-flight load: loads until every version asked for
/// meanwhile is in, then publishes and enforces the memory bound.
async fn load_task(inner: Arc<Inner>, slot: Arc<Slot>) {
    let tenant = slot.tenant;
    loop {
        let (prev, want, writer) = {
            let mut st = slot.lock();
            let w = std::mem::take(&mut st.writer_requested);
            (st.current.clone(), st.want, w)
        };
        let target = match inner.feed.versions(tenant).await {
            Ok(d) => want.max(Want::dir(d)),
            Err(e) if e.code() == Some(SprigCode::TenantNotReady) => {
                finish(&inner, &slot, Err(e), want, writer);
                return;
            }
            // The global tier is down: load what the catalog has.
            Err(_) => want,
        };
        let r = inner.load_until(tenant, prev.clone(), target, writer).await;
        let again = finish(
            &inner,
            &slot,
            r.map(|(c, gap)| (c, gap, prev)),
            target,
            writer,
        );
        if !again {
            return;
        }
    }
}

/// Publishes a load's outcome; returns whether another load must follow
/// (versions or a writer load asked for while this one ran).
fn finish(
    inner: &Arc<Inner>,
    slot: &Arc<Slot>,
    r: Result<(Arc<TenantCatalog>, bool, Option<Arc<TenantCatalog>>)>,
    target: Want,
    writer: bool,
) -> bool {
    let mut loaded = None;
    let again = {
        let mut st = slot.lock();
        st.loads += 1;
        match r {
            Ok((cat, gap, prev)) => {
                let reached = if gap {
                    Want::of(&cat).max(target)
                } else {
                    Want::of(&cat)
                };
                st.reached = st.reached.max(reached);
                st.current = Some(Arc::clone(&cat));
                st.error = None;
                st.confirmed_at = Some(Instant::now());
                if writer {
                    st.writer_done_at = Some(Instant::now());
                }
                let caught_up = inner
                    .feed
                    .cached(slot.tenant)
                    .is_none_or(|d| Want::dir(d).within(st.reached));
                if caught_up {
                    st.behind_since = None;
                }
                let new_bytes = cat.bytes();
                let old_bytes = slot.bytes.swap(new_bytes, Ordering::Relaxed);
                inner.bytes.fetch_add(new_bytes, Ordering::Relaxed);
                inner.bytes.fetch_sub(old_bytes, Ordering::Relaxed);
                loaded = Some((cat, prev));
            }
            Err(e) => {
                tracing::warn!(tenant = %slot.tenant, error = %e, "catalog load failed");
                st.error = Some(Arc::new(e));
            }
        }
        let again = loaded.is_some() && (!st.want.within(st.reached) || st.writer_requested);
        if !again {
            st.loading = false;
        }
        again
    };
    slot.published.send_modify(|g| *g += 1);
    if let Some((cat, prev)) = loaded {
        let same = prev.as_ref().is_some_and(|p| Arc::ptr_eq(p, &cat));
        if let Some(n) = &inner.notify
            && !same
        {
            let changed = match &prev {
                Some(p) => cat.settings.changed_keys(&p.settings),
                None => cat.settings.rows().map(|(_, k, _)| k).collect(),
            };
            n.tenant_loaded(&cat.settings, &changed);
        }
        if !same {
            // No receivers is fine: nothing follows the feed.
            let _ = inner.loads.send(CatalogLoaded {
                tenant: slot.tenant,
                prev,
                current: cat,
            });
        }
        inner.enforce(Some(slot.tenant));
    }
    again
}

/// Follows directory events (eager reloads of pinned tenants) and reports
/// the gauges once a second.
async fn run(
    inner: Weak<Inner>,
    mut changes: broadcast::Receiver<TenantId>,
    stop: CancellationToken,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            r = changes.recv() => {
                let Some(inner) = inner.upgrade() else { return };
                match r {
                    Ok(t) => inner.directory_moved(t),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let all: Vec<TenantId> = inner
                            .slots
                            .read()
                            .unwrap_or_else(PoisonError::into_inner)
                            .keys()
                            .copied()
                            .collect();
                        for t in all {
                            inner.directory_moved(t);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = tick.tick() => {
                let Some(inner) = inner.upgrade() else { return };
                inner.report();
                metrics::gauge!("sprig_catalog_version_lag_seconds").set(inner.version_lag().as_secs_f64());
                metrics::gauge!("sprig_meta_cache_staleness_seconds", "source" => "g", "prefix" => "tenants/")
                    .set(inner.feed.staleness().as_secs_f64());
            }
        }
    }
}
