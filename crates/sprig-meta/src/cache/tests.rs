//! Cache tests that need `MemStore`'s fault injection: watches that end,
//! compaction under a live cache, and an update racing a group load. The
//! cases that run on both stores are in the conformance suite.

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::struct_field_names
)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::Result;
use sprig_common::ids::DcId;
use tokio::sync::Notify;

use super::*;
use crate::{Consistency, LeaseId, ListPage, MemStore, Op, Txn, TxnResult};

const WAIT: Duration = Duration::from_secs(10);

fn store() -> MemStore {
    MemStore::new(Tier::Dc(DcId::new("mum1").unwrap()))
}

fn key(s: &MemStore, rel: &str) -> Key {
    s.tier().key(rel).unwrap()
}

async fn put(s: &dyn MetaStore, k: &Key, v: &str) -> Revision {
    s.txn(Txn {
        then: vec![Op::Put {
            key: k.clone(),
            value: v.as_bytes().to_vec(),
            lease: None,
        }],
        ..Txn::default()
    })
    .await
    .unwrap()
    .revision
}

async fn delete(s: &dyn MetaStore, k: &Key) -> Revision {
    s.txn(Txn {
        then: vec![Op::Delete(k.clone())],
        ..Txn::default()
    })
    .await
    .unwrap()
    .revision
}

/// Polls `f` until it holds, within [`WAIT`].
async fn until(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition in time");
}

fn kv(k: &Key, rev: Revision) -> KeyValue {
    KeyValue {
        key: k.clone(),
        value: Vec::new(),
        create_revision: rev,
        mod_revision: rev,
        version: 1,
        lease: None,
    }
}

#[test]
fn caches_only_move_forward() {
    let s = store();
    let k = key(&s, "clusters/c1");
    let mut e = Entries::new();
    assert!(apply_put(&mut e, kv(&k, 5)));
    assert!(!apply_put(&mut e, kv(&k, 5)), "a replayed put is a no-op");
    assert!(!apply_put(&mut e, kv(&k, 3)), "an older put is ignored");
    assert!(!apply_delete(&mut e, &k, 4), "an older delete is ignored");
    assert!(apply_delete(&mut e, &k, 6));
    assert!(
        !apply_delete(&mut e, &k, 7),
        "deleting a missing key changes nothing"
    );
    // A group loaded at revision 10 ignores events at or below it.
    assert!(!apply_event(&mut e, WatchEvent::Put(kv(&k, 10)), 10));
    assert!(apply_event(&mut e, WatchEvent::Put(kv(&k, 11)), 10));
}

#[tokio::test(start_paused = true)]
async fn mirror_resumes_after_an_outage_and_relists_after_compaction() {
    let s = store();
    let a = key(&s, "settings/a");
    let b = key(&s, "settings/b");
    put(&s, &a, "1").await;
    let m = Mirror::spawn(Arc::new(s.clone()), key(&s, "settings/"));
    tokio::time::timeout(WAIT, m.ready()).await.unwrap();
    assert_eq!(m.len(), 1);

    // The watch ends; the mirror retries until the store is back and then
    // resumes where it was, so writes made meanwhile... arrive.
    s.set_unavailable(true);
    tokio::task::yield_now().await;
    s.set_unavailable(false);
    let r = put(&s, &b, "2").await;
    tokio::time::timeout(WAIT, m.wait_for(r)).await.unwrap();
    assert_eq!(m.get(&b).unwrap().value, b"2");

    // The watch ends again, and history is compacted past it before it
    // resumes: the mirror lists again and sees the delete it missed.
    s.set_unavailable(true);
    tokio::task::yield_now().await;
    s.set_unavailable(false);
    delete(&s, &a).await;
    let r = put(&s, &b, "3").await;
    s.compact(r);
    tokio::time::timeout(WAIT, m.wait_for(r)).await.unwrap();
    until(|| m.get(&a).is_none() && m.get(&b).is_some_and(|kv| kv.value == b"3")).await;
    assert_eq!(m.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn mirror_waits_for_an_unavailable_store() {
    let s = store();
    put(&s, &key(&s, "clusters/c1"), "x").await;
    s.set_unavailable(true);
    let m = Mirror::spawn(Arc::new(s.clone()), key(&s, "clusters/"));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!m.is_ready());
    s.set_unavailable(false);
    tokio::time::timeout(WAIT, m.ready()).await.unwrap();
    assert_eq!(m.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn lazy_cache_reloads_loaded_groups_after_compaction() {
    let s = store();
    let k = key(&s, "shardmaps/42/0");
    let gone = key(&s, "shardmaps/42/1");
    put(&s, &k, "v1").await;
    put(&s, &gone, "x").await;
    let c = LazyCache::spawn(Arc::new(s.clone()), key(&s, "shardmaps/"), 10);
    assert_eq!(c.group("42").await.unwrap().len(), 2);

    s.set_unavailable(true);
    tokio::task::yield_now().await;
    s.set_unavailable(false);
    delete(&s, &gone).await;
    let r = put(&s, &k, "v2").await;
    s.compact(r);
    until(|| {
        c.cached("42")
            .is_some_and(|g| g.len() == 1 && g[&k].value == b"v2")
    })
    .await;

    // The new watch follows later writes.
    put(&s, &k, "v3").await;
    until(|| c.cached("42").is_some_and(|g| g[&k].value == b"v3")).await;
}

/// A store whose listings of `gated` wait until released, so a test can
/// write while a group load is in flight.
struct Gated {
    inner: MemStore,
    gated: Key,
    entered: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl MetaStore for Gated {
    fn tier(&self) -> &Tier {
        self.inner.tier()
    }
    async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>> {
        self.inner.get(key, c).await
    }
    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        if *prefix == self.gated {
            // The listing happens first, then the gate holds its reply.
            let page = self.inner.list(prefix, opts).await;
            self.entered.notify_one();
            self.release.notified().await;
            return page;
        }
        self.inner.list(prefix, opts).await
    }
    async fn txn(&self, txn: Txn) -> Result<TxnResult> {
        self.inner.txn(txn).await
    }
    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        self.inner.watch(prefix, from).await
    }
    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
        self.inner.grant_lease(ttl).await
    }
    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
        self.inner.keep_alive(lease).await
    }
    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        self.inner.revoke_lease(lease).await
    }
}

#[tokio::test(start_paused = true)]
async fn an_update_during_a_load_is_not_lost() {
    let s = store();
    let k = key(&s, "shardmaps/42/0");
    let stale = key(&s, "shardmaps/42/1");
    put(&s, &k, "old").await;
    put(&s, &stale, "x").await;
    let gated = Arc::new(Gated {
        inner: s.clone(),
        gated: key(&s, "shardmaps/42/"),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let c = LazyCache::spawn(gated.clone(), key(&s, "shardmaps/"), 10);
    c.ready().await;

    let loader = {
        let c = c.clone();
        tokio::spawn(async move { c.group("42").await })
    };
    gated.entered.notified().await;
    // The listing is taken; these writes come after it and reach the cache
    // through the watch while the group is still loading.
    let r = put(&s, &k, "new").await;
    delete(&s, &stale).await;
    // Let the watch task buffer them.
    tokio::time::sleep(Duration::from_millis(50)).await;
    gated.release.notify_one();
    let g = loader.await.unwrap().unwrap();
    assert_eq!(g.len(), 1);
    assert_eq!(g[&k].value, b"new");
    assert_eq!(g[&k].mod_revision, r);
}

#[tokio::test(start_paused = true)]
async fn concurrent_first_uses_share_one_load() {
    let s = store();
    put(&s, &key(&s, "shardmaps/9/0"), "x").await;
    let c = LazyCache::spawn(Arc::new(s.clone()), key(&s, "shardmaps/"), 10);
    let (a, b) = tokio::join!(c.group("9"), c.group("9"));
    assert!(Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
    assert_eq!(c.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_load_caches_nothing() {
    let s = store();
    let c = LazyCache::spawn(Arc::new(s.clone()), key(&s, "shardmaps/"), 10);
    c.ready().await;
    s.set_unavailable(true);
    assert!(c.group("1").await.is_err());
    s.set_unavailable(false);
    assert!(c.cached("1").is_none());
    put(&s, &key(&s, "shardmaps/1/0"), "x").await;
    assert_eq!(c.group("1").await.unwrap().len(), 1);
}

/// `sprig_meta_cache_staleness_seconds`: zero while the watch runs; while
/// the store is unavailable it counts from the moment the watch ended; it is
/// zero again once a watch runs. Before the first listing it counts from
/// the start.
#[tokio::test(start_paused = true)]
async fn staleness_counts_from_the_end_of_the_watch() {
    let s = store();
    put(&s, &key(&s, "clusters/c1"), "x").await;
    s.set_unavailable(true);
    let m = Mirror::spawn(Arc::new(s.clone()), key(&s, "clusters/"));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        m.staleness() >= Duration::from_secs(3),
        "{:?}",
        m.staleness()
    );
    s.set_unavailable(false);
    tokio::time::timeout(WAIT, m.ready()).await.unwrap();
    until(|| m.staleness().is_zero()).await;

    // The watch ends: staleness counts from then, not from the listing.
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(m.staleness().is_zero());
    s.set_unavailable(true);
    until(|| !m.staleness().is_zero()).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let st = m.staleness();
    assert!(
        st >= Duration::from_secs(4) && st < Duration::from_secs(10),
        "{st:?}"
    );
    s.set_unavailable(false);
    until(|| m.staleness().is_zero()).await;
    let r = put(&s, &key(&s, "clusters/c2"), "y").await;
    tokio::time::timeout(WAIT, m.wait_for(r)).await.unwrap();
    assert!(m.staleness().is_zero());
}

#[tokio::test(start_paused = true)]
async fn lazy_cache_staleness_follows_its_watch() {
    let s = store();
    put(&s, &key(&s, "shardmaps/t1/m"), "x").await;
    s.set_unavailable(true);
    let c = LazyCache::spawn(Arc::new(s.clone()), key(&s, "shardmaps/"), 8);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(c.staleness() >= Duration::from_secs(2));
    s.set_unavailable(false);
    tokio::time::timeout(WAIT, c.ready()).await.unwrap();
    until(|| c.staleness().is_zero()).await;
    assert_eq!(c.group("t1").await.unwrap().len(), 1);
    s.set_unavailable(true);
    until(|| !c.staleness().is_zero()).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(c.staleness() >= Duration::from_secs(5));
    s.set_unavailable(false);
    until(|| c.staleness().is_zero()).await;
}

#[test]
fn staleness_source_labels_follow_chapter_23() {
    assert_eq!(source_label(&Tier::Global), "g");
    assert_eq!(source_label(&Tier::Dc(DcId::new("mum1").unwrap())), "mum1");
}
