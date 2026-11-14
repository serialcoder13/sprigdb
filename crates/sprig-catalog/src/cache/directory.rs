//! The directory's versions, for invalidation.
//!
//! The directory entry `/sprig/g/tenants/<tenant_id>` carries
//! `catalog_version` and `auth_version`, allocated there before a catalog
//! change commits, so a cache that follows it never misses an
//! invalidation. Directory entries are single keys: gateways read them
//! through rather than caching them lazily, and gateways don't mirror the
//! whole directory.
//!
//! [`DirectoryWatch`] keeps one watch on the `tenants/` prefix and follows
//! only the tenants the cache holds: first use reads the entry through
//! (linearizable) and follows it from then on; events for other tenants
//! are dropped; an evicted tenant is forgotten. The watch is established
//! before the first read, and an entry only moves forward (an event or
//! read whose `mod_revision` is not higher is ignored), so no update falls
//! between the read and the watch. When the watch restarts (an error, a
//! compaction), every followed entry is read again, which covers the
//! events missed meanwhile. The cache's TTL backstop
//! ([`DirectoryFeed::refresh`]) covers a watch that dies silently.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use futures::StreamExt;
use sprig_common::ids::TenantId;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::{Consistency, Key, KeyValue, MetaStore, Record, Revision, WatchEvent};
use tokio::sync::{broadcast, watch};

use crate::version::DirectoryVersions;

/// The directory's per-tenant versions, as the cache sees them.
#[async_trait::async_trait]
pub trait DirectoryFeed: Send + Sync + 'static {
    /// `tenant`'s versions: as last seen if followed, else read through
    /// and followed from now on.
    ///
    /// # Errors
    /// 39072 if the tenant has no directory entry; the store's error if
    /// the global tier is unreachable.
    async fn versions(&self, tenant: TenantId) -> Result<DirectoryVersions>;

    /// `tenant`'s versions as last seen, without I/O; `None` if not
    /// followed yet.
    fn cached(&self, tenant: TenantId) -> Option<DirectoryVersions>;

    /// Reads `tenant`'s entry again, linearizable: the backstop for a
    /// missed watch event (`catalog.cache_ttl_seconds`).
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    async fn refresh(&self, tenant: TenantId) -> Result<DirectoryVersions>;

    /// Stops following `tenant`.
    fn forget(&self, tenant: TenantId);

    /// A receiver of tenants whose versions moved. A lagging receiver
    /// should recheck every tenant it cares about.
    fn changes(&self) -> broadcast::Receiver<TenantId>;

    /// How long ago the feed was last known current; zero while its watch
    /// runs.
    fn staleness(&self) -> Duration {
        Duration::ZERO
    }
}

#[derive(Debug, Clone, Copy)]
struct Followed {
    /// `None` until the first read returns.
    versions: Option<DirectoryVersions>,
    rev: Revision,
}

struct Shared {
    store: Arc<dyn MetaStore>,
    prefix: Key,
    followed: Mutex<HashMap<TenantId, Followed>>,
    watching: watch::Sender<bool>,
    changes: broadcast::Sender<TenantId>,
    down_since: Mutex<Option<Instant>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Shared {
    fn key(&self, tenant: TenantId) -> Result<Key> {
        self.store.tier().key(&format!("tenants/{tenant}"))
    }

    fn tenant_of(&self, key: &Key) -> Option<TenantId> {
        key.as_str()
            .strip_prefix(self.prefix.as_str())?
            .parse::<u64>()
            .ok()
            .map(TenantId)
    }

    /// Applies a stored entry if it is newer; returns whether the versions
    /// moved.
    fn apply(&self, tenant: TenantId, kv: &KeyValue, follow: bool) -> bool {
        let v = match Record::<DirectoryVersions>::decode(&kv.value) {
            Ok(r) => r.body,
            Err(e) => {
                tracing::warn!(%tenant, error = %e, "bad directory entry; ignored");
                return false;
            }
        };
        let mut f = lock(&self.followed);
        let entry = match f.get_mut(&tenant) {
            Some(e) => e,
            None if follow => f.entry(tenant).or_insert(Followed {
                versions: None,
                rev: 0,
            }),
            None => return false,
        };
        if kv.mod_revision <= entry.rev {
            return false;
        }
        let moved = entry.versions != Some(v);
        entry.versions = Some(v);
        entry.rev = kv.mod_revision;
        moved
    }

    async fn read(&self, tenant: TenantId) -> Result<DirectoryVersions> {
        let key = self.key(tenant)?;
        let Some(kv) = self.store.get(&key, Consistency::Linearizable).await? else {
            lock(&self.followed).remove(&tenant);
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} has no directory entry"),
            ));
        };
        if self.apply(tenant, &kv, true) {
            let _ = self.changes.send(tenant);
        }
        lock(&self.followed)
            .get(&tenant)
            .and_then(|f| f.versions)
            .ok_or_else(|| SprigError::internal("directory entry vanished while read"))
    }

    fn set_live(&self, live: bool) {
        let mut d = lock(&self.down_since);
        match (live, d.is_some()) {
            (true, _) => *d = None,
            (false, false) => *d = Some(Instant::now()),
            (false, true) => {}
        }
        self.watching.send_replace(live);
    }
}

/// Cancels the watch task when the last handle drops.
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Follows cached tenants' directory entries with one watch (see the
/// module docs). Cloning shares it; the watch stops when the last clone
/// drops.
#[derive(Clone)]
pub struct DirectoryWatch {
    shared: Arc<Shared>,
    _stop: Arc<StopOnDrop>,
}

impl std::fmt::Debug for DirectoryWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryWatch")
            .field("prefix", &self.shared.prefix)
            .field("followed", &lock(&self.shared.followed).len())
            .finish_non_exhaustive()
    }
}

/// How long a first read waits for the watch before reading anyway.
const WATCH_WAIT: Duration = Duration::from_secs(2);

impl DirectoryWatch {
    /// Starts watching `tenants/` in `store`, the global tier (in
    /// single-etcd mode the same etcd as the DC tier; tests may pass a
    /// private tier standing in for it, as [`DirectoryAllocator`] does).
    ///
    /// [`DirectoryAllocator`]: crate::version::DirectoryAllocator
    ///
    /// # Errors
    /// `Internal` never in practice (the prefix is always valid).
    pub fn start(store: Arc<dyn MetaStore>) -> Result<Self> {
        let prefix = store.tier().key("tenants/")?;
        let shared = Arc::new(Shared {
            store,
            prefix,
            followed: Mutex::new(HashMap::new()),
            watching: watch::channel(false).0,
            changes: broadcast::channel(1024).0,
            down_since: Mutex::new(Some(Instant::now())),
        });
        let stop = CancellationToken::new();
        tokio::spawn(run(Arc::clone(&shared), stop.clone()));
        Ok(Self {
            shared,
            _stop: Arc::new(StopOnDrop(stop)),
        })
    }

    /// Waits until the watch runs.
    pub async fn ready(&self) {
        let mut rx = self.shared.watching.subscribe();
        let _ = rx.wait_for(|w| *w).await;
    }

    /// Number of followed tenants.
    #[must_use]
    pub fn followed(&self) -> usize {
        lock(&self.shared.followed).len()
    }
}

#[async_trait::async_trait]
impl DirectoryFeed for DirectoryWatch {
    async fn versions(&self, tenant: TenantId) -> Result<DirectoryVersions> {
        if let Some(v) = self.cached(tenant) {
            return Ok(v);
        }
        let _ = tokio::time::timeout(WATCH_WAIT, self.ready()).await;
        lock(&self.shared.followed)
            .entry(tenant)
            .or_insert(Followed {
                versions: None,
                rev: 0,
            });
        self.shared.read(tenant).await
    }

    fn cached(&self, tenant: TenantId) -> Option<DirectoryVersions> {
        lock(&self.shared.followed).get(&tenant)?.versions
    }

    async fn refresh(&self, tenant: TenantId) -> Result<DirectoryVersions> {
        self.shared.read(tenant).await
    }

    fn forget(&self, tenant: TenantId) {
        lock(&self.shared.followed).remove(&tenant);
    }

    fn changes(&self) -> broadcast::Receiver<TenantId> {
        self.shared.changes.subscribe()
    }

    fn staleness(&self) -> Duration {
        lock(&self.shared.down_since).map_or(Duration::ZERO, |t| t.elapsed())
    }
}

const BACKOFF_MIN: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

async fn run(shared: Arc<Shared>, stop: CancellationToken) {
    let mut backoff = BACKOFF_MIN;
    loop {
        let watched = tokio::select! {
            () = stop.cancelled() => return,
            w = shared.store.watch(&shared.prefix, 0) => w,
        };
        match watched {
            Ok(mut w) => {
                shared.set_live(true);
                // Events missed while the watch was down: read every
                // followed entry again.
                let tenants: Vec<TenantId> = lock(&shared.followed).keys().copied().collect();
                for t in tenants {
                    if let Err(e) = shared.read(t).await {
                        tracing::debug!(tenant = %t, error = %e, "directory re-read failed");
                    }
                }
                loop {
                    let item = tokio::select! {
                        () = stop.cancelled() => return,
                        i = w.next() => i,
                    };
                    match item {
                        Some(Ok(WatchEvent::Put(kv))) => {
                            backoff = BACKOFF_MIN;
                            if let Some(t) = shared.tenant_of(&kv.key)
                                && shared.apply(t, &kv, false)
                            {
                                let _ = shared.changes.send(t);
                            }
                        }
                        Some(Ok(WatchEvent::Delete { key, .. })) => {
                            if let Some(t) = shared.tenant_of(&key)
                                && lock(&shared.followed).remove(&t).is_some()
                            {
                                let _ = shared.changes.send(t);
                            }
                        }
                        Some(Ok(WatchEvent::Progress(_))) => {}
                        Some(Ok(WatchEvent::Compacted { .. })) => {
                            tracing::info!("directory watch compacted; restarting");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::debug!(error = %e, "directory watch ended");
                            break;
                        }
                        None => break,
                    }
                }
                shared.set_live(false);
            }
            Err(e) => {
                tracing::debug!(error = %e, "directory watch failed; retrying");
            }
        }
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}
