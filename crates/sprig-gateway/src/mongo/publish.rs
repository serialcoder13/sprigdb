//! The publication wait of chapter 31's `check_flags` (build step 5;
//! `decisions/r5-interim.md` D13): a write holding a new array path
//! commits only after every gateway of the DC has loaded the catalog
//! version that registers the path, so no gateway plans a query on that
//! path as scalar-safe while the write is visible.
//!
//! Gateways acknowledge through the DC tier, without a peer call:
//!
//! ```text
//! doc-gw/<gateway>              presence of a gateway's publisher (its lease)
//! doc-publish/<tenant>          {version}: "load at least this" (the writer's lease)
//! doc-acks/<tenant>/<gateway>   {version}: the version that gateway holds (its lease)
//!
//! publish(tenant, v), by the writing gateway, after the change committed:
//!   1. put doc-acks/<tenant>/<me> = v (its own snapshots are already >= v)
//!   2. put doc-publish/<tenant> = v
//!   3. until every gateway in doc-gw/ has an ack >= v: re-read both
//!      prefixes every PUBLISH_POLL; past doc.path_publish_timeout_ms the
//!      write fails with 39664 SprigPathPublishTimeout (retryable)
//! every gateway, on doc-publish/<tenant> = v:
//!   tenant cached here: load it to version >= v; ack the version loaded
//!   not cached: ack v (its first use loads the newest from the directory)
//! ```
//!
//! A gateway that dies leaves `doc-gw/` with its lease, so a crash stalls
//! a publication for at most the lease's time. Gateways of other DCs are
//! not waited for (a tenant's documents are served from its home DC).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sprig_catalog::Freshness;
use sprig_common::ids::TenantId;
use sprig_common::{CancellationToken, Result};
use sprig_meta::{Key, LeaseId, ListOpts, MetaStore, Op, Record, Txn, Versioned, WatchEvent};
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use crate::tenant::TenantServices;

/// `doc.path_publish_timeout_ms`.
pub static PATH_PUBLISH_TIMEOUT_MS: Setting<u64> = Setting {
    key: "doc.path_publish_timeout_ms",
    default: 10_000,
    scopes: Scopes::DATA,
    hard: HardBounds::range(100, 600_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 31,
    doc: "Wait for gateways to load a new path or flag",
};
sprig_settings::register!(PATH_PUBLISH_TIMEOUT_MS);

/// Lease of this gateway's presence and acknowledgements.
const LEASE_TTL: Duration = Duration::from_secs(10);
/// How often a waiting writer looks at the acknowledgements.
const PUBLISH_POLL: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct VersionRec {
    version: u64,
}

impl Versioned for VersionRec {
    const V: u32 = 1;
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn encode(version: u64) -> Vec<u8> {
    Record::new(VersionRec { version })
        .encode()
        .unwrap_or_default()
}

fn decode(b: &[u8]) -> Option<u64> {
    Record::<VersionRec>::decode(b).ok().map(|r| r.body.version)
}

/// The last path segment of `key` after `prefix/`.
fn after<'a>(key: &'a Key, prefix: &str) -> Option<&'a str> {
    let s = key.as_str();
    Some(&s[s.find(prefix)? + prefix.len()..])
}

#[derive(Default)]
struct State {
    lease: Option<LeaseId>,
    /// Acknowledgements this gateway wrote, rewritten under a new lease.
    acked: HashMap<u64, u64>,
}

/// This gateway's side of the publication wait.
pub struct Publisher {
    store: Arc<dyn MetaStore>,
    svc: Arc<TenantServices>,
    me: String,
    state: Mutex<State>,
    write: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

impl Publisher {
    /// Gateway `me` over the DC tier `store`; call [`Publisher::run`].
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, svc: Arc<TenantServices>, me: String) -> Self {
        Self {
            store,
            svc,
            me,
            state: Mutex::new(State::default()),
            write: tokio::sync::Mutex::new(()),
        }
    }

    fn key(&self, rel: &str) -> Option<Key> {
        self.store.tier().key(rel).ok()
    }

    async fn lease(&self) -> Option<LeaseId> {
        if let Some(l) = lock(&self.state).lease {
            return Some(l);
        }
        let l = self.store.grant_lease(LEASE_TTL).await.ok()?;
        lock(&self.state).lease = Some(l);
        Some(l)
    }

    async fn put(&self, key: Key, value: Vec<u8>, lease: Option<LeaseId>) -> Result<()> {
        self.store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put { key, value, lease }],
                otherwise: Vec::new(),
            })
            .await
            .map(|_| ())
    }

    /// Writes this gateway's presence under its lease.
    async fn present(&self) {
        let _w = self.write.lock().await;
        let (Some(key), Some(lease)) =
            (self.key(&format!("doc-gw/{}", self.me)), self.lease().await)
        else {
            return;
        };
        if let Err(e) = self.put(key, encode(0), Some(lease)).await {
            tracing::debug!(error = %e, "document publication presence write failed");
        }
    }

    /// Records that this gateway holds `tenant` at `version` or newer.
    async fn ack(&self, tenant: u64, version: u64) {
        let _w = self.write.lock().await;
        {
            let mut st = lock(&self.state);
            let e = st.acked.entry(tenant).or_insert(0);
            if *e >= version {
                return;
            }
            *e = version;
        }
        let (Some(key), Some(lease)) = (
            self.key(&format!("doc-acks/{tenant}/{}", self.me)),
            self.lease().await,
        ) else {
            return;
        };
        if let Err(e) = self.put(key, encode(version), Some(lease)).await {
            lock(&self.state).acked.remove(&tenant);
            tracing::debug!(tenant, error = %e, "document publication acknowledgement failed");
        }
    }

    /// A publication request seen: load `tenant` to `version` if it is
    /// cached here, then acknowledge the version held.
    async fn on_request(&self, tenant: u64, version: u64) {
        let t = TenantId(tenant);
        let held = if self.svc.cache.cached(t).is_some() {
            match self
                .svc
                .cache
                .get(
                    t,
                    Freshness::Cached {
                        min_version: version,
                    },
                )
                .await
            {
                Ok(c) => c.version.max(version),
                Err(e) => {
                    // No acknowledgement: the writer waits, then fails
                    // with 39664 rather than risk a stale plan here.
                    tracing::warn!(%t, version, error = %e, "catalog load for a document publication failed");
                    return;
                }
            }
        } else {
            version
        };
        self.ack(tenant, held).await;
    }

    /// Follows publication requests and keeps the lease alive until
    /// `stop`; the lease is revoked then, so no writer waits for this
    /// gateway any more.
    pub async fn run(self: Arc<Self>, stop: CancellationToken) {
        let Some(prefix) = self.key("doc-publish/") else {
            return;
        };
        self.present().await;
        let keeper = {
            let me = Arc::clone(&self);
            let stop = stop.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = stop.cancelled() => break,
                        () = tokio::time::sleep(LEASE_TTL / 3) => {}
                    }
                    let l = lock(&me.state).lease;
                    let alive = match l {
                        Some(l) => matches!(me.store.keep_alive(l).await, Ok(Some(_))),
                        None => false,
                    };
                    if !alive {
                        // A new lease: presence and acknowledgements again.
                        let acked: Vec<(u64, u64)> = {
                            let mut st = lock(&me.state);
                            st.lease = None;
                            st.acked.drain().collect()
                        };
                        me.present().await;
                        for (t, v) in acked {
                            me.ack(t, v).await;
                        }
                    }
                }
                let l = lock(&me.state).lease.take();
                if let Some(l) = l {
                    let _ = me.store.revoke_lease(l).await;
                }
            })
        };
        'outer: loop {
            if stop.is_cancelled() {
                break;
            }
            let page = match self.store.list(&prefix, ListOpts::default()).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!(error = %e, "document publication requests unreadable");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            for kv in &page.kvs {
                self.request(&kv.key, &kv.value);
            }
            let Ok(mut w) = self.store.watch(&prefix, page.revision + 1).await else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            loop {
                let ev = tokio::select! {
                    () = stop.cancelled() => break 'outer,
                    ev = w.next() => ev,
                };
                match ev {
                    Some(Ok(WatchEvent::Put(kv))) => self.request(&kv.key, &kv.value),
                    Some(Ok(WatchEvent::Delete { .. } | WatchEvent::Progress(_))) => {}
                    // Compacted, broken or ended: list again.
                    _ => continue 'outer,
                }
            }
        }
        let _ = keeper.await;
    }

    fn request(self: &Arc<Self>, key: &Key, value: &[u8]) {
        let (Some(tenant), Some(version)) = (
            after(key, "doc-publish/").and_then(|t| t.parse::<u64>().ok()),
            decode(value),
        ) else {
            return;
        };
        let me = Arc::clone(self);
        tokio::spawn(async move { me.on_request(tenant, version).await });
    }

    /// The gateways present without an acknowledgement of `version`.
    async fn missing(
        &self,
        gws: &Key,
        acks: &Key,
        tenant: TenantId,
        version: u64,
    ) -> Result<Vec<String>> {
        let live: HashSet<String> = self
            .store
            .list(gws, ListOpts::default())
            .await?
            .kvs
            .iter()
            .filter_map(|kv| after(&kv.key, "doc-gw/").map(str::to_owned))
            .collect();
        let prefix = format!("doc-acks/{}/", tenant.0);
        let held: HashMap<String, u64> = self
            .store
            .list(acks, ListOpts::default())
            .await?
            .kvs
            .iter()
            .filter_map(|kv| Some((after(&kv.key, &prefix)?.to_owned(), decode(&kv.value)?)))
            .collect();
        Ok(live
            .into_iter()
            .filter(|g| held.get(g).is_none_or(|v| *v < version))
            .collect())
    }

    /// Waits until every gateway of the DC holds `tenant`'s catalog at
    /// `version` or newer.
    ///
    /// # Errors
    /// 39664 `SprigPathPublishTimeout` past `timeout`; the change stays
    /// committed and the write may be retried.
    pub async fn publish(
        &self,
        tenant: TenantId,
        version: u64,
        timeout: Duration,
    ) -> std::result::Result<(), MongoError> {
        let started = Instant::now();
        let deadline = started + timeout;
        self.ack(tenant.0, version).await;
        let failed = |why: String| {
            MongoError::new(
                code::SPRIG_PATH_PUBLISH_TIMEOUT,
                format!(
                    "a new array path of tenant {tenant} wasn't confirmed by every gateway within {} ms: {why}",
                    timeout.as_millis()
                ),
            )
        };
        let (Some(req), Some(gws), Some(acks)) = (
            self.key(&format!("doc-publish/{}", tenant.0)),
            self.key("doc-gw/"),
            self.key(&format!("doc-acks/{}/", tenant.0)),
        ) else {
            return Err(failed("no keys".into()));
        };
        // The request lives on this gateway's lease: it goes when this
        // gateway does, and the next request of the tenant replaces it.
        // Errors of the store are retried until the deadline.
        let mut requested = false;
        let mut last_error = String::new();
        loop {
            if !requested {
                match self.lease().await {
                    Some(lease) => {
                        match self.put(req.clone(), encode(version), Some(lease)).await {
                            Ok(()) => requested = true,
                            Err(e) => last_error = e.to_string(),
                        }
                    }
                    None => last_error = "no lease".into(),
                }
            }
            if requested {
                match self.missing(&gws, &acks, tenant, version).await {
                    Ok(m) if m.is_empty() => break,
                    Ok(mut m) => {
                        m.sort();
                        last_error = format!("waiting for {}", m.join(", "));
                    }
                    Err(e) => last_error = e.to_string(),
                }
            }
            if Instant::now() >= deadline {
                metrics::counter!("sprig_doc_path_publications_total", "kind" => "timeout")
                    .increment(1);
                return Err(failed(last_error));
            }
            tokio::time::sleep(PUBLISH_POLL).await;
        }
        metrics::histogram!("sprig_doc_path_publish_seconds")
            .record(started.elapsed().as_secs_f64());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip() {
        assert_eq!(decode(&encode(913)), Some(913));
        assert_eq!(decode(b"{}"), None);
    }
}
