//! Full and lazy caches over a [`MetaStore`] (chapter 23, "Caches" and
//! "Watches, caching and epochs in gateways"; build step 4).
//!
//! The data path never waits for etcd: gateways and controllers read from
//! these caches, fed by watches. Every entry keeps its `mod_revision`, and an
//! update whose `mod_revision` is not higher than the cached one is ignored,
//! so caches only move forward.
//!
//! ```text
//! mirror(prefix):                                   # full mode: Mirror
//! 1. page through list(prefix) at one revision R; replace the cache; cache.rev = R
//! 2. watch from R + 1: Put/Delete apply only if their mod_rev is higher than cached;
//!    Progress(rev) sets fresh_rev; Compacted -> goto 1; errors -> resume at cache.rev + 1
//!
//! lazy(prefix):                                     # LazyCache, e.g. shard maps
//! 1. one prefix watch from now; ignore events for groups not loaded
//! 2. first use of T: mark loading; get T's keys at revision G; insert; apply events
//!    buffered while loading with mod_rev > G; mark loaded
//! 3. LRU evicts idle groups; Compacted re-gets every loaded group
//! ```
//!
//! Both caches run one background task each, stopped when the last handle
//! drops. Store errors are retried with a backoff (100 ms doubling to 5 s),
//! and each watch restart is counted in `sprig_meta_watch_restarts_total`
//! (`tier`, `reason`).
//!
//! **Staleness.** Each cache reports `sprig_meta_cache_staleness_seconds`
//! (`source`: `g` or the dc id, as chapter 23's `MetaEvent`; `prefix`: the
//! cached prefix under the tier root), once a second and on every change of
//! state: how long ago the cache was last known to reflect the store. While
//! its watch runs, events arrive in revision order as they commit, so the
//! cache is current and the gauge is 0. When the watch ends (a store error,
//! a closed stream, a compaction), the gauge counts from that moment, or
//! from the last listing, until a watch runs again; before the first
//! listing it counts from the cache's start. [`Mirror::staleness`] and
//! [`LazyCache::staleness`] return the same value. A connection that dies
//! silently counts as current until the store client notices (etcd's
//! keepalive).
//!
//! **Resuming.** A watch can end in the middle of a revision (a `MemStore`
//! watcher whose queue overflows), so after events the watch resumes at the
//! last revision applied rather than one past it; replayed events are
//! no-ops because their `mod_revision` is not higher than the cached one.
//! After a listing or a progress mark, which cover whole revisions, it
//! resumes one past.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use futures::StreamExt;
use sprig_common::{CancellationToken, Result, SprigError};
use tokio::sync::watch;

use crate::{Key, KeyValue, ListOpts, MetaStore, Revision, Tier, WatchEvent, WatchStream};

/// First wait after a store error.
const BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Longest wait after repeated store errors.
const BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Page size of the listings that fill a cache.
const PAGE: usize = 500;

/// Cached keys of one prefix or group, in key order.
pub type Entries = BTreeMap<Key, Arc<KeyValue>>;

/// Cancels the background task when the last handle drops.
#[derive(Debug)]
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn tier_label(t: &Tier) -> &'static str {
    match t {
        Tier::Global => "global",
        Tier::Dc(_) => "dc",
    }
}

/// The `source` label of chapter 23: `g` for the global tier, else the dc
/// id.
fn source_label(t: &Tier) -> String {
    match t {
        Tier::Global => "g".to_owned(),
        Tier::Dc(dc) => dc.to_string(),
    }
}

/// How long ago a cache was last known to reflect its store (see
/// "Staleness" above).
#[derive(Debug)]
struct Freshness {
    state: Mutex<FreshState>,
    gauge: metrics::Gauge,
}

#[derive(Debug, Clone, Copy)]
struct FreshState {
    /// When the cache started.
    started: tokio::time::Instant,
    /// Its watch is running.
    live: bool,
    /// The last moment it was known current: a listing, or the end of a
    /// watch.
    known_at: Option<tokio::time::Instant>,
}

impl Freshness {
    fn new(tier: &Tier, prefix: &Key) -> Self {
        let root = tier.root();
        let rel = prefix
            .as_str()
            .strip_prefix(root.as_str())
            .unwrap_or(prefix.as_str())
            .to_owned();
        Self {
            state: Mutex::new(FreshState {
                started: tokio::time::Instant::now(),
                live: false,
                known_at: None,
            }),
            gauge: metrics::gauge!(
                "sprig_meta_cache_staleness_seconds",
                "source" => source_label(tier),
                "prefix" => rel
            ),
        }
    }

    /// A listing just completed: the cache reflects the store now.
    fn listed(&self) {
        lock(&self.state).known_at = Some(tokio::time::Instant::now());
        self.report();
    }

    /// The watch is running (`true`) or just ended (`false`).
    fn set_live(&self, live: bool) {
        {
            let mut st = lock(&self.state);
            if st.live && !live {
                st.known_at = Some(tokio::time::Instant::now());
            }
            st.live = live;
        }
        self.report();
    }

    fn staleness(&self) -> Duration {
        let st = *lock(&self.state);
        if st.live {
            Duration::ZERO
        } else {
            st.known_at.unwrap_or(st.started).elapsed()
        }
    }

    fn report(&self) {
        self.gauge.set(self.staleness().as_secs_f64());
    }
}

/// Sets the staleness gauge once a second until `stop` fires.
async fn report_staleness(fresh: Arc<Freshness>, stop: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = tick.tick() => fresh.report(),
        }
    }
}

fn count_restart(tier: &Tier, reason: &'static str) {
    metrics::counter!("sprig_meta_watch_restarts_total", "tier" => tier_label(tier), "reason" => reason)
        .increment(1);
}

/// Doubling backoff with a ceiling.
struct Backoff(Duration);

impl Backoff {
    fn new() -> Self {
        Self(BACKOFF_MIN)
    }

    fn reset(&mut self) {
        self.0 = BACKOFF_MIN;
    }

    /// Waits the current delay and doubles it; `false` if `stop` fired.
    async fn wait(&mut self, stop: &CancellationToken) -> bool {
        let d = self.0;
        self.0 = (self.0 * 2).min(BACKOFF_MAX);
        tokio::select! {
            () = stop.cancelled() => false,
            () = tokio::time::sleep(d) => true,
        }
    }
}

/// Every key under `prefix`, paged at one revision, and that revision.
async fn list_all(
    store: &dyn MetaStore,
    prefix: &Key,
    at: Option<Revision>,
) -> Result<(Vec<KeyValue>, Revision)> {
    let mut out = Vec::new();
    let mut opts = ListOpts {
        at,
        after: None,
        limit: PAGE,
    };
    loop {
        let page = store.list(prefix, opts.clone()).await?;
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        out.extend(page.kvs);
        if !page.more || opts.after.is_none() {
            return Ok((out, page.revision));
        }
    }
}

/// Applies a put if it is newer than the cached entry. `true` if it changed
/// the cache.
fn apply_put(entries: &mut Entries, kv: KeyValue) -> bool {
    if entries
        .get(&kv.key)
        .is_some_and(|old| old.mod_revision >= kv.mod_revision)
    {
        return false;
    }
    entries.insert(kv.key.clone(), Arc::new(kv));
    true
}

/// Applies a delete if it is newer than the cached entry. `true` if it
/// changed the cache.
fn apply_delete(entries: &mut Entries, key: &Key, rev: Revision) -> bool {
    if entries.get(key).is_some_and(|old| old.mod_revision < rev) {
        entries.remove(key);
        return true;
    }
    false
}

// ---- full mode -----------------------------------------------------------

/// What a [`Mirror`] holds.
#[derive(Debug, Default)]
struct MirrorState {
    entries: Entries,
    /// The highest revision the cache reflects.
    rev: Revision,
    /// The last progress mark.
    fresh_rev: Revision,
}

#[derive(Debug)]
struct MirrorShared {
    prefix: Key,
    state: RwLock<MirrorState>,
    /// Bumped after every change; 0 until the first listing is in.
    generation: watch::Sender<u64>,
    /// The highest revision the cache reflects, for [`Mirror::wait_for`].
    revision: watch::Sender<Revision>,
    /// For `sprig_meta_cache_staleness_seconds`.
    fresh: Arc<Freshness>,
}

impl MirrorShared {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, MirrorState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, MirrorState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn bump(&self) {
        self.generation.send_modify(|g| *g += 1);
    }

    fn reached(&self, rev: Revision) {
        self.revision.send_if_modified(|r| {
            let moved = rev > *r;
            *r = (*r).max(rev);
            moved
        });
    }
}

/// A full cache of one prefix: listed once, then kept current by a watch
/// (full mode). Gateways mirror their DC's `clusters/`, `settings/` and
/// `cluster-settings/`; controllers mirror the global tier.
///
/// Cloning shares the cache; the background task stops when the last clone
/// drops.
#[derive(Debug, Clone)]
pub struct Mirror {
    shared: Arc<MirrorShared>,
    _stop: Arc<StopOnDrop>,
}

impl Mirror {
    /// Starts mirroring `prefix` of `store` in the background. Reads return
    /// nothing until the first listing is in; [`ready`](Self::ready) waits
    /// for it.
    #[must_use]
    pub fn spawn(store: Arc<dyn MetaStore>, prefix: Key) -> Self {
        let stop = CancellationToken::new();
        let fresh = Arc::new(Freshness::new(store.tier(), &prefix));
        let shared = Arc::new(MirrorShared {
            prefix,
            state: RwLock::new(MirrorState::default()),
            generation: watch::channel(0).0,
            revision: watch::channel(0).0,
            fresh: Arc::clone(&fresh),
        });
        tokio::spawn(report_staleness(fresh, stop.clone()));
        tokio::spawn(run_mirror(store, Arc::clone(&shared), stop.clone()));
        Self {
            shared,
            _stop: Arc::new(StopOnDrop(stop)),
        }
    }

    /// The mirrored prefix.
    #[must_use]
    pub fn prefix(&self) -> &Key {
        &self.shared.prefix
    }

    /// Whether the first listing is in.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        *self.shared.generation.borrow() > 0
    }

    /// Waits until the first listing is in. Retries store errors meanwhile,
    /// so this waits for as long as the store is unreachable.
    pub async fn ready(&self) {
        let mut rx = self.shared.generation.subscribe();
        // The sender lives in `self`, so this can't fail.
        let _ = rx.wait_for(|g| *g > 0).await;
    }

    /// The cached value of `key`.
    #[must_use]
    pub fn get(&self, key: &Key) -> Option<Arc<KeyValue>> {
        self.shared.read().entries.get(key).cloned()
    }

    /// Every cached key, in key order.
    #[must_use]
    pub fn entries(&self) -> Vec<Arc<KeyValue>> {
        self.shared.read().entries.values().cloned().collect()
    }

    /// Runs `f` on the cached entries under the read lock. Keep `f` short:
    /// it blocks the watch task.
    pub fn with<R>(&self, f: impl FnOnce(&Entries) -> R) -> R {
        f(&self.shared.read().entries)
    }

    /// Number of cached keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shared.read().entries.len()
    }

    /// Whether the cache holds no key.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The highest revision the cache reflects: of the listing, of the last
    /// event applied, or of the last progress mark.
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.shared.read().rev
    }

    /// The revision of the last progress mark: every change up to it is in.
    #[must_use]
    pub fn fresh_revision(&self) -> Revision {
        self.shared.read().fresh_rev
    }

    /// A receiver whose value is bumped after every change to the cache
    /// (and when the first listing is in). Changes coalesce: a slow reader
    /// sees the latest generation, then reads the cache.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.shared.generation.subscribe()
    }

    /// The current generation (the value [`changes`](Self::changes)
    /// carries): it is bumped after every change, so anything worked out
    /// from the cache after reading generation `g` stays right while the
    /// generation is still `g`.
    #[must_use]
    pub fn generation(&self) -> u64 {
        *self.shared.generation.borrow()
    }

    /// Waits until the cache reflects revision `rev`: an event at or after
    /// it was applied, or a progress mark reached it.
    pub async fn wait_for(&self, rev: Revision) {
        let mut rx = self.shared.revision.subscribe();
        // The sender lives in `self`, so this can't fail.
        let _ = rx.wait_for(|r| *r >= rev).await;
    }

    /// How long ago the cache was last known to reflect the store: zero
    /// while its watch runs (see "Staleness" in the module docs). The value
    /// of `sprig_meta_cache_staleness_seconds`.
    #[must_use]
    pub fn staleness(&self) -> Duration {
        self.shared.fresh.staleness()
    }
}

/// The mirror's background task: list, watch, re-list on compaction.
async fn run_mirror(store: Arc<dyn MetaStore>, shared: Arc<MirrorShared>, stop: CancellationToken) {
    let tier = store.tier().clone();
    let prefix = shared.prefix.clone();
    let mut backoff = Backoff::new();
    'relist: loop {
        let listed = tokio::select! {
            () = stop.cancelled() => return,
            r = list_all(&*store, &prefix, None) => r,
        };
        let (kvs, rev) = match listed {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(%prefix, error = %e, "mirror listing failed; retrying");
                if !backoff.wait(&stop).await {
                    return;
                }
                continue;
            }
        };
        {
            let mut st = shared.write();
            st.entries = kvs
                .into_iter()
                .map(|kv| (kv.key.clone(), Arc::new(kv)))
                .collect();
            st.rev = rev;
        }
        shared.fresh.listed();
        shared.bump();
        shared.reached(rev);
        backoff.reset();
        let mut resume = rev + 1;
        loop {
            let watched = tokio::select! {
                () = stop.cancelled() => return,
                r = store.watch(&prefix, resume) => r,
            };
            let mut w = match watched {
                Ok(w) => w,
                Err(e) => {
                    tracing::debug!(%prefix, error = %e, "mirror watch failed; retrying");
                    count_restart(&tier, "error");
                    if !backoff.wait(&stop).await {
                        return;
                    }
                    continue;
                }
            };
            shared.fresh.set_live(true);
            let ended = follow_mirror(&shared, &mut w, &mut resume, &mut backoff, &stop).await;
            shared.fresh.set_live(false);
            match ended {
                Ended::Stopped => return,
                Ended::Compacted => {
                    tracing::info!(%prefix, "mirror watch compacted; listing again");
                    count_restart(&tier, "compacted");
                    continue 'relist;
                }
                Ended::Error => {
                    count_restart(&tier, "error");
                    if !backoff.wait(&stop).await {
                        return;
                    }
                }
                Ended::Closed => {
                    count_restart(&tier, "closed");
                    if !backoff.wait(&stop).await {
                        return;
                    }
                }
            }
        }
    }
}

/// Why a watch stream stopped being followed.
enum Ended {
    Stopped,
    Compacted,
    Error,
    Closed,
}

async fn follow_mirror(
    shared: &MirrorShared,
    w: &mut WatchStream,
    resume: &mut Revision,
    backoff: &mut Backoff,
    stop: &CancellationToken,
) -> Ended {
    loop {
        let item = tokio::select! {
            () = stop.cancelled() => return Ended::Stopped,
            i = w.next() => i,
        };
        let (changed, rev) = match item {
            None => return Ended::Closed,
            Some(Err(e)) => {
                tracing::debug!(prefix = %shared.prefix, error = %e, "mirror watch ended");
                return Ended::Error;
            }
            Some(Ok(WatchEvent::Compacted { .. })) => return Ended::Compacted,
            Some(Ok(WatchEvent::Progress(r))) => {
                let mut st = shared.write();
                st.fresh_rev = st.fresh_rev.max(r);
                st.rev = st.rev.max(r);
                *resume = (*resume).max(r + 1);
                (false, r)
            }
            Some(Ok(WatchEvent::Put(kv))) => {
                let r = kv.mod_revision;
                let mut st = shared.write();
                let c = apply_put(&mut st.entries, kv);
                st.rev = st.rev.max(r);
                *resume = (*resume).max(r);
                (c, r)
            }
            Some(Ok(WatchEvent::Delete { key, mod_revision })) => {
                let mut st = shared.write();
                let c = apply_delete(&mut st.entries, &key, mod_revision);
                st.rev = st.rev.max(mod_revision);
                *resume = (*resume).max(mod_revision);
                (c, mod_revision)
            }
        };
        backoff.reset();
        if changed {
            shared.bump();
        }
        shared.reached(rev);
    }
}

// ---- lazy mode -----------------------------------------------------------

/// The cached keys of one group, as a snapshot: later changes don't alter
/// it.
pub type Group = Arc<Entries>;

/// One group's state in a [`LazyCache`].
#[derive(Debug)]
enum Slot {
    /// A load is running; events for the group are buffered.
    Loading {
        buffer: Vec<WatchEvent>,
        done: watch::Sender<bool>,
    },
    /// Loaded at revision `at`; events at or below it are ignored.
    Loaded {
        entries: Group,
        at: Revision,
        used: u64,
    },
}

#[derive(Debug, Default)]
struct LazyState {
    slots: HashMap<String, Slot>,
    /// Loaded groups by last use, for LRU eviction.
    lru: BTreeMap<u64, String>,
    tick: u64,
    /// Bumped when the watch restarts after a compaction: a load that began
    /// before it may have missed events and must run again.
    epoch: u64,
}

impl LazyState {
    fn touch(&mut self, group: &str) {
        self.tick += 1;
        let now = self.tick;
        if let Some(Slot::Loaded { used, .. }) = self.slots.get_mut(group) {
            self.lru.remove(used);
            *used = now;
            self.lru.insert(now, group.to_owned());
        }
    }

    fn evict(&mut self, group: &str) -> bool {
        if let Some(Slot::Loaded { used, .. }) = self.slots.get(group) {
            self.lru.remove(used);
            self.slots.remove(group);
            return true;
        }
        false
    }

    /// Evicts least recently used groups beyond `capacity`.
    fn trim(&mut self, capacity: usize) {
        while self.lru.len() > capacity {
            let Some((_, g)) = self.lru.pop_first() else {
                return;
            };
            self.slots.remove(&g);
        }
    }

    fn loaded(&self) -> usize {
        self.lru.len()
    }
}

#[derive(Debug)]
struct LazyShared {
    prefix: Key,
    capacity: usize,
    state: Mutex<LazyState>,
    /// The watch is running.
    watching: watch::Sender<bool>,
    /// Bumped after every change to a loaded group.
    generation: watch::Sender<u64>,
    /// For `sprig_meta_cache_staleness_seconds`.
    fresh: Arc<Freshness>,
}

impl LazyShared {
    fn lock(&self) -> MutexGuard<'_, LazyState> {
        lock(&self.state)
    }

    /// The group a key belongs to: its first path segment under the prefix,
    /// when more segments follow.
    fn group_of<'k>(&self, key: &'k Key) -> Option<&'k str> {
        let rest = key.as_str().strip_prefix(self.prefix.as_str())?;
        let (g, _) = rest.split_once('/')?;
        (!g.is_empty()).then_some(g)
    }

    fn bump(&self) {
        self.generation.send_modify(|g| *g += 1);
    }
}

/// A lazy cache of the groups under one prefix: a group (the keys under
/// `<prefix><group>/`) is loaded on first use, kept current by one watch on
/// the whole prefix, and evicted least recently used beyond a capacity
/// (`[meta] cache_tenants`). Gateways cache `shardmaps/` this way, a group
/// per tenant.
///
/// Keys directly under the prefix, with no group segment, are not cached.
/// Cloning shares the cache; the background task stops when the last clone
/// drops.
#[derive(Clone)]
pub struct LazyCache {
    store: Arc<dyn MetaStore>,
    shared: Arc<LazyShared>,
    _stop: Arc<StopOnDrop>,
}

impl std::fmt::Debug for LazyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyCache")
            .field("tier", self.store.tier())
            .field("prefix", &self.shared.prefix)
            .field("capacity", &self.shared.capacity)
            .finish_non_exhaustive()
    }
}

impl LazyCache {
    /// Starts watching `prefix` of `store` in the background, caching at
    /// most `capacity` groups (at least 1).
    #[must_use]
    pub fn spawn(store: Arc<dyn MetaStore>, prefix: Key, capacity: usize) -> Self {
        let stop = CancellationToken::new();
        let fresh = Arc::new(Freshness::new(store.tier(), &prefix));
        let shared = Arc::new(LazyShared {
            prefix,
            capacity: capacity.max(1),
            state: Mutex::new(LazyState::default()),
            watching: watch::channel(false).0,
            generation: watch::channel(0).0,
            fresh: Arc::clone(&fresh),
        });
        tokio::spawn(report_staleness(fresh, stop.clone()));
        tokio::spawn(run_lazy(
            Arc::clone(&store),
            Arc::clone(&shared),
            stop.clone(),
        ));
        Self {
            store,
            shared,
            _stop: Arc::new(StopOnDrop(stop)),
        }
    }

    /// The watched prefix.
    #[must_use]
    pub fn prefix(&self) -> &Key {
        &self.shared.prefix
    }

    /// Waits until the watch is running. [`group`](Self::group) waits for
    /// it too.
    pub async fn ready(&self) {
        let mut rx = self.shared.watching.subscribe();
        let _ = rx.wait_for(|w| *w).await;
    }

    /// The keys of `group`, loading them on first use.
    ///
    /// # Errors
    /// Internal if `group` is empty or contains `/` or `*`; the store's
    /// error if the load fails (nothing is cached then).
    pub async fn group(&self, group: &str) -> Result<Group> {
        let prefix = self.group_prefix(group)?;
        self.ready().await;
        loop {
            let wait = {
                let mut st = self.shared.lock();
                match st.slots.get(group) {
                    Some(Slot::Loaded { entries, .. }) => {
                        let e = Arc::clone(entries);
                        st.touch(group);
                        return Ok(e);
                    }
                    Some(Slot::Loading { done, .. }) => Some(done.subscribe()),
                    None => {
                        st.slots.insert(
                            group.to_owned(),
                            Slot::Loading {
                                buffer: Vec::new(),
                                done: watch::channel(false).0,
                            },
                        );
                        None
                    }
                }
            };
            match wait {
                // Another caller is loading: wait, then look again.
                Some(mut rx) => {
                    let _ = rx.wait_for(|d| *d).await;
                }
                None => return self.load(group, &prefix).await,
            }
        }
    }

    /// The keys of `group` if it is loaded; never loads.
    #[must_use]
    pub fn cached(&self, group: &str) -> Option<Group> {
        match self.shared.lock().slots.get(group) {
            Some(Slot::Loaded { entries, .. }) => Some(Arc::clone(entries)),
            _ => None,
        }
    }

    /// Drops `group` from the cache; `true` if it was loaded.
    #[must_use = "whether the group was loaded"]
    pub fn evict(&self, group: &str) -> bool {
        self.shared.lock().evict(group)
    }

    /// Number of loaded groups.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shared.lock().loaded()
    }

    /// Whether no group is loaded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A receiver bumped after every change to a loaded group.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.shared.generation.subscribe()
    }

    /// How long ago the loaded groups were last known to reflect the
    /// store: zero while the watch runs (see "Staleness" in the module
    /// docs). The value of `sprig_meta_cache_staleness_seconds`.
    #[must_use]
    pub fn staleness(&self) -> Duration {
        self.shared.fresh.staleness()
    }

    fn group_prefix(&self, group: &str) -> Result<Key> {
        if group.is_empty() || group.contains(['/', '*']) {
            return Err(SprigError::internal(format!(
                "invalid cache group {group:?}"
            )));
        }
        Ok(Key::from_stored(format!("{}{group}/", self.shared.prefix)))
    }

    /// Loads `group`, whose slot this caller set to `Loading`.
    async fn load(&self, group: &str, prefix: &Key) -> Result<Group> {
        loop {
            let epoch = self.shared.lock().epoch;
            let listed = list_all(&*self.store, prefix, None).await;
            let mut st = self.shared.lock();
            let Some(Slot::Loading { buffer, done }) = st.slots.remove(group) else {
                // Evicted or replaced meanwhile: shouldn't happen, as only
                // the loader removes a Loading slot.
                return Err(SprigError::internal(format!(
                    "cache group {group} vanished while loading"
                )));
            };
            let (kvs, at) = match listed {
                Ok(v) => v,
                Err(e) => {
                    done.send_replace(true);
                    return Err(e);
                }
            };
            if st.epoch != epoch {
                // The watch restarted after a compaction while the listing
                // ran: events between the listing and the new watch may be
                // lost. Load again.
                st.slots.insert(
                    group.to_owned(),
                    Slot::Loading {
                        buffer: Vec::new(),
                        done,
                    },
                );
                continue;
            }
            let mut entries: Entries = kvs
                .into_iter()
                .map(|kv| (kv.key.clone(), Arc::new(kv)))
                .collect();
            for ev in buffer {
                apply_event(&mut entries, ev, at);
            }
            let entries = Arc::new(entries);
            st.slots.insert(
                group.to_owned(),
                Slot::Loaded {
                    entries: Arc::clone(&entries),
                    at,
                    used: 0,
                },
            );
            st.touch(group);
            st.trim(self.shared.capacity);
            drop(st);
            done.send_replace(true);
            return Ok(entries);
        }
    }
}

/// Applies `ev` to a group loaded at revision `at`; events at or below `at`
/// are already in the listing. `true` if the group changed.
fn apply_event(entries: &mut Entries, ev: WatchEvent, at: Revision) -> bool {
    match ev {
        WatchEvent::Put(kv) if kv.mod_revision > at => apply_put(entries, kv),
        WatchEvent::Delete { key, mod_revision } if mod_revision > at => {
            apply_delete(entries, &key, mod_revision)
        }
        _ => false,
    }
}

/// The lazy cache's background task: one prefix watch; reloads every loaded
/// group after a compaction.
async fn run_lazy(store: Arc<dyn MetaStore>, shared: Arc<LazyShared>, stop: CancellationToken) {
    let tier = store.tier().clone();
    let prefix = shared.prefix.clone();
    let mut backoff = Backoff::new();
    // Start from the current revision: groups loaded later are listed at a
    // revision at least this one, so the watch covers everything after.
    let mut resume = loop {
        let r = tokio::select! {
            () = stop.cancelled() => return,
            r = current_revision(&*store, &prefix) => r,
        };
        match r {
            Ok(r) => break r + 1,
            Err(e) => {
                tracing::debug!(%prefix, error = %e, "lazy cache start failed; retrying");
                if !backoff.wait(&stop).await {
                    return;
                }
            }
        }
    };
    loop {
        let watched = tokio::select! {
            () = stop.cancelled() => return,
            r = store.watch(&prefix, resume) => r,
        };
        let mut w = match watched {
            Ok(w) => w,
            Err(e) => {
                tracing::debug!(%prefix, error = %e, "lazy cache watch failed; retrying");
                count_restart(&tier, "error");
                if !backoff.wait(&stop).await {
                    return;
                }
                continue;
            }
        };
        shared.watching.send_replace(true);
        shared.fresh.set_live(true);
        let ended = follow_lazy(&shared, &mut w, &mut resume, &mut backoff, &stop).await;
        shared.fresh.set_live(false);
        match ended {
            Ended::Stopped => return,
            Ended::Compacted => {
                tracing::info!(%prefix, "lazy cache watch compacted; reloading every loaded group");
                count_restart(&tier, "compacted");
                drop(w);
                loop {
                    let r = tokio::select! {
                        () = stop.cancelled() => return,
                        r = reload_all(&*store, &shared) => r,
                    };
                    match r {
                        Ok(rev) => {
                            shared.fresh.listed();
                            resume = rev + 1;
                            break;
                        }
                        Err(e) => {
                            tracing::debug!(%prefix, error = %e, "lazy cache reload failed; retrying");
                            if !backoff.wait(&stop).await {
                                return;
                            }
                        }
                    }
                }
            }
            Ended::Error | Ended::Closed => {
                count_restart(
                    &tier,
                    if matches!(ended, Ended::Error) {
                        "error"
                    } else {
                        "closed"
                    },
                );
                if !backoff.wait(&stop).await {
                    return;
                }
            }
        }
    }
}

/// The store's current revision, from a one-key listing.
async fn current_revision(store: &dyn MetaStore, prefix: &Key) -> Result<Revision> {
    let page = store
        .list(
            prefix,
            ListOpts {
                limit: 1,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page.revision)
}

/// Re-lists every loaded group at one revision R after a compaction and
/// returns R; the watch then resumes at R + 1. Loads in progress start over
/// (the epoch changes).
async fn reload_all(store: &dyn MetaStore, shared: &LazyShared) -> Result<Revision> {
    let rev = current_revision(store, &shared.prefix).await?;
    let groups: Vec<String> = {
        let mut st = shared.lock();
        st.epoch += 1;
        st.lru.values().cloned().collect()
    };
    for g in groups {
        let prefix = Key::from_stored(format!("{}{g}/", shared.prefix));
        let (kvs, _) = list_all(store, &prefix, Some(rev)).await?;
        let mut st = shared.lock();
        if let Some(Slot::Loaded { entries, at, .. }) = st.slots.get_mut(&g) {
            *entries = Arc::new(
                kvs.into_iter()
                    .map(|kv| (kv.key.clone(), Arc::new(kv)))
                    .collect(),
            );
            *at = rev;
        }
    }
    shared.bump();
    Ok(rev)
}

async fn follow_lazy(
    shared: &LazyShared,
    w: &mut WatchStream,
    resume: &mut Revision,
    backoff: &mut Backoff,
    stop: &CancellationToken,
) -> Ended {
    loop {
        let item = tokio::select! {
            () = stop.cancelled() => return Ended::Stopped,
            i = w.next() => i,
        };
        let ev = match item {
            None => return Ended::Closed,
            Some(Err(e)) => {
                tracing::debug!(prefix = %shared.prefix, error = %e, "lazy cache watch ended");
                return Ended::Error;
            }
            Some(Ok(WatchEvent::Compacted { .. })) => return Ended::Compacted,
            Some(Ok(WatchEvent::Progress(r))) => {
                *resume = (*resume).max(r + 1);
                continue;
            }
            Some(Ok(ev)) => ev,
        };
        backoff.reset();
        let (key, rev) = match &ev {
            WatchEvent::Put(kv) => (&kv.key, kv.mod_revision),
            WatchEvent::Delete { key, mod_revision } => (key, *mod_revision),
            WatchEvent::Progress(_) | WatchEvent::Compacted { .. } => continue,
        };
        *resume = (*resume).max(rev);
        let Some(group) = shared.group_of(key).map(str::to_owned) else {
            continue;
        };
        let changed = {
            let mut st = shared.lock();
            match st.slots.get_mut(&group) {
                Some(Slot::Loading { buffer, .. }) => {
                    buffer.push(ev);
                    false
                }
                Some(Slot::Loaded { entries, at, .. }) => {
                    let at = *at;
                    apply_event(Arc::make_mut(entries), ev, at)
                }
                // Not loaded: ignored.
                None => false,
            }
        };
        if changed {
            shared.bump();
        }
    }
}

#[cfg(test)]
mod tests;
