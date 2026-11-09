//! The relay's `RemoteMaps`, client side (chapter 23, "The relay"; build
//! step 11).
//!
//! A DC's etcd is reachable only inside its DC. In two tiers, a process
//! that needs another DC's shard maps or cluster records (a gateway
//! following a tenant homed there, chapter 24) reads them through a
//! controller of that DC, which serves them from its own tier. The
//! controllers are found in the global key `dc-observe/<dc>` (their
//! internal URLs, chapter 25).
//!
//! - [`RelayStore`] is a read-only [`MetaStore`] over one remote DC's tier:
//!   `get` and `list` go to `POST /internal/v1/relay/{get,list}`; a watch
//!   polls the prefix every [`POLL`] and turns the differences into events
//!   (puts with their revisions, deletes, progress marks), so the
//!   [`crate::Mirror`] caches built over it work unchanged.
//! - [`RoutedStore`] is a process's own DC store with remote DCs' keys
//!   routed to their relays: `TierView`s over it (the cluster registry,
//!   tenant maps, the gateway's locator) reach other DCs in two tiers as
//!   they do in single-etcd mode.
//!
//! **Where it differs from the book.** The book's relay is one
//! `Subscribe` stream per remote DC over mTLS gRPC on port 7400, with
//! interests added and removed; this one is JSON over HTTP on the
//! controllers' internal address (decision F2), and watches poll. Only
//! `shardmaps/` and `clusters/` are served.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::StreamExt;
use hyper::Method;
use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::permit::http::JsonHttp;
use crate::{
    Consistency, Key, KeyValue, LeaseId, ListOpts, ListPage, MetaStore, Revision, Tier, Txn,
    TxnResult, WatchEvent, WatchStream,
};

/// A relayed watch lists its prefix this often.
pub const POLL: Duration = Duration::from_secs(1);

/// Prefixes of a DC tier the relay serves.
pub const SERVED: [&str; 2] = ["shardmaps/", "clusters/"];

/// Whether the relay serves `key` (a full key) of `tier`.
#[must_use]
pub fn served(tier: &Tier, key: &str) -> bool {
    let root = tier.root();
    key.strip_prefix(root.as_str())
        .is_some_and(|rel| SERVED.iter().any(|p| rel.starts_with(p)))
}

/// One key-value, as the relay sends it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayKv {
    /// The full key.
    pub key: String,
    /// The value, as text (records are JSON).
    pub value: String,
    /// Create revision.
    pub create_revision: Revision,
    /// Mod revision.
    pub mod_revision: Revision,
    /// Version.
    pub version: u64,
}

impl RelayKv {
    /// From a store's key-value.
    #[must_use]
    pub fn from_kv(kv: &KeyValue) -> Self {
        Self {
            key: kv.key.as_str().to_owned(),
            value: String::from_utf8_lossy(&kv.value).into_owned(),
            create_revision: kv.create_revision,
            mod_revision: kv.mod_revision,
            version: kv.version,
        }
    }

    fn into_kv(self) -> KeyValue {
        KeyValue {
            key: Key::from_stored(self.key),
            value: self.value.into_bytes(),
            create_revision: self.create_revision,
            mod_revision: self.mod_revision,
            version: self.version,
            lease: None,
        }
    }
}

/// `POST /internal/v1/relay/get` and `/list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayRequest {
    /// The full key or prefix.
    pub key: String,
}

/// The answer to a get.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayGet {
    /// The key-value, if any.
    pub kv: Option<RelayKv>,
}

/// The answer to a list: every key under the prefix at one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayList {
    /// The keys, in order.
    pub kvs: Vec<RelayKv>,
    /// The revision they were read at.
    pub revision: Revision,
}

fn refused(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::Internal,
        format!("{what}: a relayed DC tier is read-only"),
    )
}

/// One remote DC's tier, read through its controllers (see the module
/// docs).
pub struct RelayStore {
    tier: Tier,
    global: Arc<dyn MetaStore>,
    http: JsonHttp,
}

impl std::fmt::Debug for RelayStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayStore")
            .field("tier", &self.tier)
            .finish_non_exhaustive()
    }
}

impl RelayStore {
    /// DC `dc`'s tier, found through `global`'s `dc-observe/<dc>`.
    #[must_use]
    pub fn new(dc: DcId, global: Arc<dyn MetaStore>, timeout: Duration) -> Self {
        Self {
            tier: Tier::Dc(dc),
            global,
            http: JsonHttp::new(timeout),
        }
    }

    async fn controllers(&self) -> Result<Vec<String>> {
        let Tier::Dc(dc) = &self.tier else {
            return Ok(Vec::new());
        };
        let key = Tier::Global.key(&format!("dc-observe/{dc}"))?;
        let Some(kv) = self.global.get(&key, Consistency::Serializable).await? else {
            return Ok(Vec::new());
        };
        let v: serde_json::Value =
            serde_json::from_slice(&kv.value).map_err(|e| SprigError::internal(e.to_string()))?;
        Ok(v.get("controllers")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|c| c.as_str().map(str::to_owned))
            .collect())
    }

    async fn call<T: serde::de::DeserializeOwned>(&self, path: &str, key: &str) -> Result<T> {
        let req = RelayRequest {
            key: key.to_owned(),
        };
        let mut last = SprigError::new(
            SprigCode::MetaUnavailable,
            format!("no controller of {:?} answers the relay", self.tier),
        );
        for c in self.controllers().await? {
            let url = format!("{}/internal/v1/relay/{path}", c.trim_end_matches('/'));
            match self.http.call(Method::POST, &url, Some(&req)).await {
                Ok(v) => return Ok(v),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    async fn list_all(&self, prefix: &Key) -> Result<RelayList> {
        self.call("list", prefix.as_str()).await
    }
}

#[async_trait::async_trait]
impl MetaStore for RelayStore {
    fn tier(&self) -> &Tier {
        &self.tier
    }

    async fn get(&self, key: &Key, _c: Consistency) -> Result<Option<KeyValue>> {
        let r: RelayGet = self.call("get", key.as_str()).await?;
        Ok(r.kv.map(RelayKv::into_kv))
    }

    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        // One page with every key: the relay serves small prefixes (one
        // tenant's maps, one DC's clusters).
        let r = self.list_all(prefix).await?;
        let kvs = r
            .kvs
            .into_iter()
            .filter(|kv| {
                opts.after
                    .as_ref()
                    .is_none_or(|a| kv.key.as_str() > a.as_str())
            })
            .map(RelayKv::into_kv)
            .collect();
        Ok(ListPage {
            kvs,
            revision: r.revision,
            more: false,
        })
    }

    async fn txn(&self, _txn: Txn) -> Result<TxnResult> {
        Err(refused("txn"))
    }

    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        let me = Self {
            tier: self.tier.clone(),
            global: Arc::clone(&self.global),
            http: self.http.clone(),
        };
        let prefix = prefix.clone();
        // What the watcher has seen: key -> mod revision.
        let state: (
            RelayStore,
            Key,
            Revision,
            Option<BTreeMap<String, Revision>>,
        ) = (me, prefix, from, None);
        let s = futures::stream::unfold(state, |(me, prefix, from, seen)| async move {
            {
                if seen.is_some() {
                    tokio::time::sleep(POLL).await;
                }
                let list = match me.list_all(&prefix).await {
                    Ok(l) => l,
                    Err(e) => return Some((Err(e), (me, prefix, from, seen))),
                };
                let mut events = Vec::new();
                let now: BTreeMap<String, Revision> = list
                    .kvs
                    .iter()
                    .map(|kv| (kv.key.clone(), kv.mod_revision))
                    .collect();
                for kv in &list.kvs {
                    let changed = seen
                        .as_ref()
                        .is_none_or(|s| s.get(&kv.key) != Some(&kv.mod_revision));
                    if changed && kv.mod_revision >= from {
                        events.push(WatchEvent::Put(kv.clone().into_kv()));
                    }
                }
                if let Some(s) = &seen {
                    for k in s.keys().filter(|k| !now.contains_key(*k)) {
                        events.push(WatchEvent::Delete {
                            key: Key::from_stored(k.clone()),
                            mod_revision: list.revision,
                        });
                    }
                }
                events.push(WatchEvent::Progress(list.revision));
                let next = (me, prefix, from, Some(now));
                Some((Ok(events), next))
            }
        });
        let flat = s.flat_map(|r: Result<Vec<WatchEvent>>| {
            futures::stream::iter(match r {
                Ok(v) => v.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
        });
        Ok(Box::pin(flat))
    }

    async fn grant_lease(&self, _ttl: Duration) -> Result<LeaseId> {
        Err(refused("grant_lease"))
    }

    async fn keep_alive(&self, _lease: LeaseId) -> Result<Option<Duration>> {
        Err(refused("keep_alive"))
    }

    async fn revoke_lease(&self, _lease: LeaseId) -> Result<()> {
        Err(refused("revoke_lease"))
    }
}

/// A process's own DC store with other DCs' served keys routed to their
/// relays (see the module docs).
pub struct RoutedStore {
    local: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    remotes: Mutex<HashMap<DcId, Arc<RelayStore>>>,
    timeout: Duration,
}

impl std::fmt::Debug for RoutedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedStore")
            .field("tier", self.local.tier())
            .finish_non_exhaustive()
    }
}

impl RoutedStore {
    /// `local` (this DC's tier), reaching other DCs through their
    /// controllers listed in `global`.
    #[must_use]
    pub fn new(local: Arc<dyn MetaStore>, global: Arc<dyn MetaStore>) -> Self {
        Self {
            local,
            global,
            remotes: Mutex::new(HashMap::new()),
            timeout: Duration::from_secs(3),
        }
    }

    /// The relay for the DC whose tier holds `key`, if it isn't this one.
    fn route(&self, key: &str) -> Option<Arc<RelayStore>> {
        let rest = key.strip_prefix("/sprig/dc/")?;
        let (dc, _) = rest.split_once('/')?;
        let dc = DcId::new(dc).ok()?;
        if Tier::Dc(dc.clone()) == *self.local.tier() {
            return None;
        }
        if !served(&Tier::Dc(dc.clone()), key) {
            return None;
        }
        Some(Arc::clone(
            self.remotes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(dc.clone())
                .or_insert_with(|| {
                    Arc::new(RelayStore::new(dc, Arc::clone(&self.global), self.timeout))
                }),
        ))
    }
}

#[async_trait::async_trait]
impl MetaStore for RoutedStore {
    fn tier(&self) -> &Tier {
        self.local.tier()
    }
    async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>> {
        match self.route(key.as_str()) {
            Some(r) => r.get(key, c).await,
            None => self.local.get(key, c).await,
        }
    }
    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        match self.route(prefix.as_str()) {
            Some(r) => r.list(prefix, opts).await,
            None => self.local.list(prefix, opts).await,
        }
    }
    async fn txn(&self, txn: Txn) -> Result<TxnResult> {
        self.local.txn(txn).await
    }
    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        match self.route(prefix.as_str()) {
            Some(r) => r.watch(prefix, from).await,
            None => self.local.watch(prefix, from).await,
        }
    }
    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
        self.local.grant_lease(ttl).await
    }
    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
        self.local.keep_alive(lease).await
    }
    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        self.local.revoke_lease(lease).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_maps_and_clusters_are_served() {
        let t = Tier::Dc(DcId::new("ams1").expect("dc"));
        assert!(served(&t, "/sprig/dc/ams1/shardmaps/42/0"));
        assert!(served(&t, "/sprig/dc/ams1/clusters/ams1-c1"));
        assert!(!served(&t, "/sprig/dc/ams1/leases/writer/ams1-c1"));
        assert!(!served(&t, "/sprig/dc/ams1/settings/x"));
        assert!(!served(&t, "/sprig/dc/fra1/shardmaps/42/0"));
    }
}
