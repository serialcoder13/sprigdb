//! The `MetaStore` conformance suite: one suite
//! against `MemStore` and `EtcdStore`.
//!
//! Every case runs against both stores, so they behave the same wherever
//! SprigDB relies on it: versions and revisions, guarded transactions,
//! paging at one revision, watches with replay, progress and compaction,
//! leases, sessions, election with fencing, bootstrap, and the full and lazy
//! caches.
//!
//! - `MemStore` cases run on a current-thread runtime with tokio's clock
//!   paused, and the store's leases follow it ([`TokioClock`]), so waits
//!   are instant and deterministic.
//! - `EtcdStore` cases run against the etcd named by `SPRIG_TEST_ETCD`
//!   (for example `http://127.0.0.1:23790`) in real time, each case under a
//!   DC tier of its own. Without the variable they are skipped with a
//!   message. Compaction is cluster-wide, so the case that compacts runs
//!   alone. Each case's keys are deleted afterwards, also when it fails.
//!
//! Cases never assert absolute revisions: on a shared etcd other cases
//! write concurrently. Other crates (`sprig-testkit`) can run [`CASES`]
//! against their own stores with the `conformance` feature.

// A test suite: failures are panics with a message.
#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::many_single_char_names,
    clippy::wildcard_imports
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use sprig_common::ids::DcId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode};

use crate::bootstrap::{self, Bootstrap, DcRecord};
use crate::cache::{LazyCache, Mirror};
use crate::elect::{self, Election, Elector, LeaderRecord};
use crate::session::{Session, SessionOpts, TokioClock};
use crate::{
    Cmp, Consistency, EtcdConfig, EtcdStore, Key, KeyValue, LeaseId, ListOpts, MemStore, MetaStore,
    Op, OpResult, Record, Revision, Tier, Txn, WatchEvent, WatchStream,
};

/// The environment variable naming the etcd for the etcd half.
pub const ETCD_ENV: &str = "SPRIG_TEST_ETCD";

/// The test-only operations a store under test must offer besides
/// [`MetaStore`].
#[async_trait::async_trait]
pub trait TestStore: MetaStore {
    /// Drops history at or below `revision`.
    async fn compact_history(&self, revision: Revision) -> Result<()>;
    /// Sends a progress mark to every live watch.
    async fn progress_all(&self) -> Result<()>;
    /// Another tier over the same backing state.
    fn view(&self, tier: Tier) -> Arc<dyn TestStore>;
}

#[async_trait::async_trait]
impl TestStore for MemStore {
    async fn compact_history(&self, revision: Revision) -> Result<()> {
        self.compact(revision);
        Ok(())
    }
    async fn progress_all(&self) -> Result<()> {
        self.request_progress();
        Ok(())
    }
    fn view(&self, tier: Tier) -> Arc<dyn TestStore> {
        Arc::new(self.with_tier(tier))
    }
}

#[async_trait::async_trait]
impl TestStore for EtcdStore {
    async fn compact_history(&self, revision: Revision) -> Result<()> {
        self.compact(revision).await
    }
    async fn progress_all(&self) -> Result<()> {
        self.request_progress().await
    }
    fn view(&self, tier: Tier) -> Arc<dyn TestStore> {
        Arc::new(self.with_tier(tier))
    }
}

/// What a case gets: a store with an empty tier of its own, and the clock
/// its sessions use.
pub struct Env {
    /// The store under test.
    pub store: Arc<dyn TestStore>,
    /// The clock leases follow.
    pub clock: Arc<dyn Clock>,
}

impl Env {
    fn meta(&self) -> Arc<dyn MetaStore> {
        self.store.clone()
    }
    fn key(&self, rel: &str) -> Key {
        self.store.tier().key(rel).unwrap()
    }
}

/// One conformance case.
pub struct Case {
    /// Its name.
    pub name: &'static str,
    /// Its body.
    pub run: fn(Env) -> BoxFuture<'static, ()>,
    /// Must run alone on a shared server (it compacts).
    pub exclusive: bool,
}

macro_rules! cases {
    ($($name:ident $(: $excl:ident)?),* $(,)?) => {
        /// Every case, in order.
        pub const CASES: &[Case] = &[$(Case {
            name: stringify!($name),
            run: |env| -> BoxFuture<'static, ()> { Box::pin(cases::$name(env)) },
            exclusive: cases!(@excl $($excl)?),
        }),*];

        #[cfg(test)]
        mod mem {
            $(#[test] fn $name() { super::run_mem(super::case(stringify!($name))); })*
        }

        #[cfg(test)]
        mod etcd {
            $(#[test] fn $name() { super::run_etcd(super::case(stringify!($name))); })*
        }
    };
    (@excl exclusive) => { true };
    (@excl) => { false };
}

cases! {
    kv_versions_and_guarded_updates,
    txn_results_and_branches,
    listing_pages_at_one_revision,
    serializable_reads,
    watch_replays_history_then_follows_live_writes,
    watch_from_zero_starts_with_the_next_write,
    compaction_invalidates_old_revisions: exclusive,
    leases_expire_unless_kept_alive,
    revoke_deletes_bound_keys_in_one_revision,
    session_keeps_its_lease,
    session_is_lost_before_its_lease_expires,
    election_hands_over_on_resign,
    deposed_leader_cannot_commit,
    election_hands_over_on_lease_expiry,
    bootstrap_writes_once,
    mirror_lists_then_follows_writes,
    lazy_cache_loads_groups_on_first_use,
    lazy_cache_evicts_least_recently_used,
}

/// The case named `name`.
#[must_use]
pub fn case(name: &str) -> &'static Case {
    CASES
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("no conformance case {name}"))
}

/// A DC tier no other case uses.
#[must_use]
pub fn unique_tier() -> Tier {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let id = format!(
        "t{:x}-{:x}-{:x}",
        std::process::id(),
        nanos,
        N.fetch_add(1, Ordering::Relaxed)
    );
    Tier::Dc(DcId::new(id).unwrap())
}

/// Runs a case against a fresh `MemStore`, with tokio's clock paused.
pub fn run_mem(case: &Case) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
        let store = MemStore::with_clock(unique_tier(), clock.clone());
        (case.run)(Env {
            store: Arc::new(store),
            clock,
        })
        .await;
    });
}

/// Runs a case against the etcd named by `SPRIG_TEST_ETCD`, or skips it
/// with a message when the variable isn't set.
pub fn run_etcd(case: &Case) {
    static GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());
    let Ok(endpoints) = std::env::var(ETCD_ENV) else {
        eprintln!(
            "skipping etcd conformance case {}: set {ETCD_ENV} (e.g. http://127.0.0.1:23790) to run it",
            case.name
        );
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let cfg = EtcdConfig::new(endpoints.split(',').map(str::to_owned).collect());
        let store = EtcdStore::connect(unique_tier(), &cfg).await.unwrap();
        let cleanup = store.clone();
        let env = Env {
            store: Arc::new(store),
            clock: Arc::new(SystemClock),
        };
        let run = std::panic::AssertUnwindSafe((case.run)(env));
        let outcome = if case.exclusive {
            let _g = GATE.write().await;
            run.catch_unwind().await
        } else {
            let _g = GATE.read().await;
            run.catch_unwind().await
        };
        // The etcd is shared: leave nothing behind under the case's tier.
        delete_prefix(&cleanup, &cleanup.tier().root()).await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    });
}

/// Deletes every key under `prefix`, best effort.
pub async fn delete_prefix(store: &dyn MetaStore, prefix: &Key) {
    loop {
        let Ok(page) = store.list(prefix, ListOpts::default()).await else {
            return;
        };
        if page.kvs.is_empty() {
            return;
        }
        // etcd allows 128 operations per transaction by default.
        for chunk in page.kvs.chunks(100) {
            let ops = chunk.iter().map(|kv| Op::Delete(kv.key.clone())).collect();
            if store
                .txn(Txn {
                    then: ops,
                    ..Txn::default()
                })
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

// ---- helpers --------------------------------------------------------------

/// How long a case waits for an event that must come.
const WAIT: Duration = Duration::from_secs(10);

fn put(k: &Key, v: &str) -> Op {
    Op::Put {
        key: k.clone(),
        value: v.as_bytes().to_vec(),
        lease: None,
    }
}

fn put_bound(k: &Key, v: &str, lease: LeaseId) -> Op {
    Op::Put {
        key: k.clone(),
        value: v.as_bytes().to_vec(),
        lease: Some(lease),
    }
}

async fn write(s: &dyn MetaStore, ops: Vec<Op>) -> Revision {
    let r = s
        .txn(Txn {
            then: ops,
            ..Txn::default()
        })
        .await
        .unwrap();
    assert!(r.succeeded);
    r.revision
}

async fn kv(s: &dyn MetaStore, k: &Key) -> Option<KeyValue> {
    s.get(k, Consistency::Linearizable).await.unwrap()
}

async fn value(s: &dyn MetaStore, k: &Key) -> Option<String> {
    kv(s, k)
        .await
        .map(|kv| String::from_utf8(kv.value).unwrap())
}

async fn next(w: &mut WatchStream) -> WatchEvent {
    tokio::time::timeout(WAIT, w.next())
        .await
        .expect("a watch event in time")
        .expect("the watch stream is open")
        .unwrap()
}

/// Lets `d` pass: virtual time for `MemStore`, real time for etcd.
async fn advance(d: Duration) {
    tokio::time::sleep(d).await;
}

/// Polls until `key` is gone, within `within`.
async fn until_gone(s: &dyn MetaStore, k: &Key, within: Duration) {
    let start = tokio::time::Instant::now();
    while kv(s, k).await.is_some() {
        assert!(
            start.elapsed() < within,
            "{k} still exists after {within:?}"
        );
        advance(Duration::from_millis(100)).await;
    }
}

fn leader(name: &str) -> LeaderRecord {
    LeaderRecord {
        holder: name.to_owned(),
        dc: None,
        build: "test".to_owned(),
    }
}

// ---- cases ----------------------------------------------------------------

mod cases {
    use super::*;

    pub(super) async fn kv_versions_and_guarded_updates(env: Env) {
        let s = &*env.store;
        let k = env.key("clusters/c7");
        let create = Txn {
            when: vec![Cmp::Missing(k.clone())],
            then: vec![put(&k, r#"{"v":1}"#)],
            otherwise: vec![Op::Get(k.clone())],
        };
        let r = s.txn(create.clone()).await.unwrap();
        assert!(r.succeeded);
        let r0 = r.revision;
        let got = kv(s, &k).await.unwrap();
        assert_eq!(
            (
                got.create_revision,
                got.mod_revision,
                got.version,
                got.lease
            ),
            (r0, r0, 1, None)
        );

        // A second create fails and returns the current value instead.
        let r = s.txn(create).await.unwrap();
        assert!(!r.succeeded);
        assert!(r.revision >= r0);
        assert!(matches!(&r.results[0], OpResult::Get(Some(kv)) if kv.mod_revision == r0));

        // Guards on a stale revision, or on a missing key, lose.
        let missing = env.key("clusters/none");
        for stale in [
            Cmp::ModRev(k.clone(), r0 - 1),
            Cmp::CreateRev(k.clone(), r0 + 1),
            Cmp::ModRev(missing.clone(), 0),
            Cmp::CreateRev(missing.clone(), 0),
        ] {
            let t = Txn {
                when: vec![stale.clone()],
                then: vec![put(&k, "stale")],
                otherwise: vec![],
            };
            assert!(!s.txn(t).await.unwrap().succeeded, "{stale:?}");
        }
        let fresh = Txn {
            when: vec![Cmp::ModRev(k.clone(), r0), Cmp::CreateRev(k.clone(), r0)],
            then: vec![put(&k, "fresh")],
            otherwise: vec![],
        };
        let r1 = s.txn(fresh).await.unwrap();
        assert!(r1.succeeded);
        assert!(r1.revision > r0);
        let got = kv(s, &k).await.unwrap();
        assert_eq!(
            (got.create_revision, got.mod_revision, got.version),
            (r0, r1.revision, 2)
        );

        // One revision per transaction, however many writes it has.
        let (a, b) = (env.key("jobs/a"), env.key("jobs/b"));
        let both = Txn {
            then: vec![put(&a, "1"), put(&b, "2"), Op::Delete(k.clone())],
            ..Txn::default()
        };
        let r2 = s.txn(both).await.unwrap();
        assert_eq!(r2.results[2], OpResult::Delete(true));
        assert_eq!(kv(s, &a).await.unwrap().mod_revision, r2.revision);
        assert_eq!(kv(s, &b).await.unwrap().mod_revision, r2.revision);
        assert_eq!(kv(s, &k).await, None);

        // Re-creating starts a new life: new create revision, version 1.
        let r3 = write(s, vec![put(&k, "again")]).await;
        let got = kv(s, &k).await.unwrap();
        assert_eq!((got.create_revision, got.version), (r3, 1));
    }

    pub(super) async fn txn_results_and_branches(env: Env) {
        let s = &*env.store;
        let k = env.key("jobs/j1");
        let r = s
            .txn(Txn {
                when: vec![Cmp::Missing(k.clone())],
                then: vec![Op::Get(k.clone()), Op::Delete(k.clone())],
                otherwise: vec![],
            })
            .await
            .unwrap();
        assert!(r.succeeded);
        assert_eq!(
            r.results,
            vec![OpResult::Get(None), OpResult::Delete(false)]
        );

        write(s, vec![put(&k, "1")]).await;
        let r = s
            .txn(Txn {
                when: vec![Cmp::Missing(k.clone())],
                then: vec![put(&k, "x")],
                otherwise: vec![Op::Get(k.clone()), Op::Delete(k.clone())],
            })
            .await
            .unwrap();
        assert!(!r.succeeded);
        assert_eq!(r.results.len(), 2);
        assert!(matches!(&r.results[0], OpResult::Get(Some(kv)) if kv.value == b"1"));
        assert_eq!(r.results[1], OpResult::Delete(true));
        assert_eq!(kv(s, &k).await, None);

        let empty = s.txn(Txn::default()).await.unwrap();
        assert!(empty.succeeded);
        assert!(empty.results.is_empty());
    }

    pub(super) async fn listing_pages_at_one_revision(env: Env) {
        let s = &*env.store;
        let prefix = env.key("clusters/");
        for name in ["a", "b", "c"] {
            write(s, vec![put(&env.key(&format!("clusters/{name}")), name)]).await;
        }
        // A sibling prefix sharing the first letters stays out.
        write(s, vec![put(&env.key("clusters-x/a"), "x")]).await;
        write(s, vec![put(&env.key("dcs/x"), "x")]).await;

        let first = s
            .list(
                &prefix,
                ListOpts {
                    limit: 2,
                    ..ListOpts::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(first.kvs.len(), 2);
        assert!(first.more);

        // Writes between pages don't show in a listing pinned to its revision.
        write(s, vec![put(&env.key("clusters/c"), "c2")]).await;
        write(s, vec![Op::Delete(env.key("clusters/b"))]).await;
        write(s, vec![put(&env.key("clusters/d"), "d")]).await;

        let rest = s
            .list(
                &prefix,
                ListOpts {
                    at: Some(first.revision),
                    after: first.kvs.last().map(|kv| kv.key.clone()),
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert!(!rest.more);
        assert_eq!(rest.revision, first.revision);
        assert_eq!(rest.kvs.len(), 1);
        assert_eq!(rest.kvs[0].value, b"c");

        let now = s.list(&prefix, ListOpts::default()).await.unwrap();
        let values: Vec<_> = now.kvs.iter().map(|kv| kv.value.clone()).collect();
        assert_eq!(values, vec![b"a".to_vec(), b"c2".to_vec(), b"d".to_vec()]);
        assert!(!now.more);
        assert!(now.revision > first.revision);

        // Page by page with limit 1.
        let mut after = None;
        let mut seen = Vec::new();
        loop {
            let page = s
                .list(
                    &prefix,
                    ListOpts {
                        at: Some(now.revision),
                        after: after.clone(),
                        limit: 1,
                    },
                )
                .await
                .unwrap();
            seen.extend(page.kvs.iter().map(|kv| kv.key.clone()));
            after = page.kvs.last().map(|kv| kv.key.clone());
            if !page.more {
                break;
            }
        }
        let keys: Vec<_> = now.kvs.iter().map(|kv| kv.key.clone()).collect();
        assert_eq!(seen, keys);

        let none = s
            .list(&env.key("nothing/"), ListOpts::default())
            .await
            .unwrap();
        assert!(none.kvs.is_empty());
        assert!(!none.more);
    }

    pub(super) async fn serializable_reads(env: Env) {
        let s = &*env.store;
        let k = env.key("settings/x");
        write(s, vec![put(&k, "1")]).await;
        let got = s.get(&k, Consistency::Serializable).await.unwrap().unwrap();
        assert_eq!(got.value, b"1");
        assert_eq!(
            s.get(&env.key("settings/none"), Consistency::Serializable)
                .await
                .unwrap(),
            None
        );
    }

    pub(super) async fn watch_replays_history_then_follows_live_writes(env: Env) {
        let s = &*env.store;
        let prefix = env.key("settings/");
        let k = env.key("settings/auto_index.min_runs");
        let other = env.key("clusters/x");
        let r1 = write(s, vec![put(&k, "50")]).await;
        write(s, vec![put(&other, "x")]).await;

        let mut w = s.watch(&prefix, r1).await.unwrap();
        assert!(
            matches!(next(&mut w).await, WatchEvent::Put(kv) if kv.value == b"50" && kv.mod_revision == r1)
        );

        write(s, vec![put(&other, "y")]).await;
        let r60 = write(s, vec![put(&k, "60")]).await;
        let rdel = write(s, vec![Op::Delete(k.clone())]).await;
        assert!(
            matches!(next(&mut w).await, WatchEvent::Put(kv) if kv.value == b"60" && kv.mod_revision == r60 && kv.version == 2)
        );
        assert_eq!(
            next(&mut w).await,
            WatchEvent::Delete {
                key: k.clone(),
                mod_revision: rdel
            }
        );

        env.store.progress_all().await.unwrap();
        match next(&mut w).await {
            WatchEvent::Progress(rev) => assert!(rev >= rdel),
            ev => panic!("expected progress, got {ev:?}"),
        }
    }

    pub(super) async fn watch_from_zero_starts_with_the_next_write(env: Env) {
        let s = &*env.store;
        let prefix = env.key("settings/");
        let k = env.key("settings/a");
        write(s, vec![put(&k, "before")]).await;
        let mut live = s.watch(&prefix, 0).await.unwrap();
        let r = write(s, vec![put(&k, "after")]).await;
        assert!(
            matches!(next(&mut live).await, WatchEvent::Put(kv) if kv.mod_revision == r && kv.value == b"after")
        );
    }

    pub(super) async fn compaction_invalidates_old_revisions(env: Env) {
        let s = &*env.store;
        let prefix = env.key("jobs/");
        let k = env.key("jobs/a");
        let r1 = write(s, vec![put(&k, "1")]).await;
        let r2 = write(s, vec![put(&k, "2")]).await;
        env.store.compact_history(r2).await.unwrap();
        // Compacting again at or below is a no-op.
        env.store.compact_history(r1).await.unwrap();

        let old = ListOpts {
            at: Some(r1),
            ..ListOpts::default()
        };
        assert_eq!(
            s.list(&prefix, old).await.unwrap_err().code(),
            Some(SprigCode::MetaUnavailable)
        );
        let mut w = s.watch(&prefix, r1).await.unwrap();
        assert_eq!(
            next(&mut w).await,
            WatchEvent::Compacted {
                compact_revision: r2
            }
        );
        let end = tokio::time::timeout(WAIT, w.next()).await.unwrap();
        assert!(end.is_none(), "the stream ends after Compacted");

        // The latest value survives compaction, and so does a listing at it.
        assert_eq!(value(s, &k).await.as_deref(), Some("2"));
        let at = ListOpts {
            at: Some(r2),
            ..ListOpts::default()
        };
        assert_eq!(s.list(&prefix, at).await.unwrap().kvs.len(), 1);
    }

    pub(super) async fn leases_expire_unless_kept_alive(env: Env) {
        let s = &*env.store;
        let k = env.key("gateways/gw-7");
        let ttl = Duration::from_secs(4);
        let lease = s.grant_lease(ttl).await.unwrap();
        write(s, vec![put_bound(&k, "{}", lease)]).await;
        assert_eq!(kv(s, &k).await.unwrap().lease, Some(lease));
        let mut w = s.watch(&env.key("gateways/"), 0).await.unwrap();

        advance(Duration::from_secs(2)).await;
        assert_eq!(s.keep_alive(lease).await.unwrap(), Some(ttl));
        advance(Duration::from_secs(3)).await;
        assert!(kv(s, &k).await.is_some(), "kept alive");

        until_gone(s, &k, Duration::from_secs(6)).await;
        assert!(matches!(next(&mut w).await, WatchEvent::Delete { key, .. } if key == k));
        assert_eq!(s.keep_alive(lease).await.unwrap(), None);
    }

    pub(super) async fn revoke_deletes_bound_keys_in_one_revision(env: Env) {
        let s = &*env.store;
        let (k1, k2, k3) = (
            env.key("workers/w1"),
            env.key("workers/w2"),
            env.key("workers/w3"),
        );
        let lease = s.grant_lease(Duration::from_secs(30)).await.unwrap();
        write(
            s,
            vec![
                put_bound(&k1, "1", lease),
                put_bound(&k2, "2", lease),
                put_bound(&k3, "3", lease),
            ],
        )
        .await;
        // Rewriting a key without the lease unbinds it.
        write(s, vec![put(&k2, "2b")]).await;
        assert_eq!(kv(s, &k2).await.unwrap().lease, None);

        let mut w = s.watch(&env.key("workers/"), 0).await.unwrap();
        s.revoke_lease(lease).await.unwrap();
        assert_eq!(kv(s, &k1).await, None);
        assert_eq!(kv(s, &k3).await, None);
        assert_eq!(value(s, &k2).await.as_deref(), Some("2b"));

        let mut deleted = Vec::new();
        let mut revs = Vec::new();
        for _ in 0..2 {
            match next(&mut w).await {
                WatchEvent::Delete { key, mod_revision } => {
                    deleted.push(key);
                    revs.push(mod_revision);
                }
                ev => panic!("expected a delete, got {ev:?}"),
            }
        }
        deleted.sort();
        assert_eq!(deleted, vec![k1.clone(), k3.clone()]);
        assert_eq!(revs[0], revs[1], "one revision for the whole lease");

        // A put bound to an ended lease fails and writes nothing.
        let orphan = Txn {
            then: vec![put(&k3, "x"), put_bound(&k1, "x", lease)],
            ..Txn::default()
        };
        assert!(s.txn(orphan).await.is_err());
        assert_eq!(kv(s, &k1).await, None);
        assert_eq!(kv(s, &k3).await, None);

        // Ended and unknown leases: no keep-alive, and revoking is a no-op.
        assert_eq!(s.keep_alive(lease).await.unwrap(), None);
        s.revoke_lease(lease).await.unwrap();
        assert_eq!(s.keep_alive(LeaseId(0x7fff_0000_dead)).await.unwrap(), None);
    }

    pub(super) async fn session_keeps_its_lease(env: Env) {
        let s = env.meta();
        let k = env.key("gateways/g1");
        let session = Session::open(
            s.clone(),
            env.clock.clone(),
            SessionOpts::new(Duration::from_secs(3)),
        )
        .await
        .unwrap();
        write(&*s, vec![put_bound(&k, "{}", session.lease())]).await;
        advance(Duration::from_secs(7)).await;
        assert!(!session.is_lost());
        assert!(kv(&*s, &k).await.is_some(), "kept alive past its TTL");
        assert!(session.valid_until() > env.clock.now());

        session.close().await.unwrap();
        assert!(session.is_lost());
        assert_eq!(kv(&*s, &k).await, None, "closing revokes the lease");
        assert_eq!(s.keep_alive(session.lease()).await.unwrap(), None);
    }

    pub(super) async fn session_is_lost_before_its_lease_expires(env: Env) {
        let s = env.meta();
        let k = env.key("gateways/g2");
        let session = Session::open(
            s.clone(),
            env.clock.clone(),
            SessionOpts::new(Duration::from_secs(3)),
        )
        .await
        .unwrap();
        write(&*s, vec![put_bound(&k, "{}", session.lease())]).await;
        session.halt_keep_alive();
        tokio::time::timeout(WAIT, session.lost())
            .await
            .expect("the session notices it is lost");
        assert!(session.is_lost());
        assert!(
            kv(&*s, &k).await.is_some(),
            "the process stops before etcd expires the lease"
        );
        until_gone(&*s, &k, Duration::from_secs(6)).await;
    }

    pub(super) async fn election_hands_over_on_resign(env: Env) {
        let s = env.meta();
        let key = env.key("leader");
        let opts = SessionOpts::new(Duration::from_secs(3));
        let ea = Election::new(s.clone(), env.clock.clone(), opts);
        let eb = Election::new(s.clone(), env.clock.clone(), opts);
        let mut obs = elect::observe(s.clone(), key.clone());
        let first = tokio::time::timeout(WAIT, obs.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first, None);

        let a = ea.campaign(&key, &leader("a")).await.unwrap();
        assert!(a.is_leading());
        let seen = tokio::time::timeout(WAIT, obs.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!((seen.record.holder.as_str(), seen.token), ("a", a.token));
        let kva = kv(&*s, &key).await.unwrap();
        assert_eq!(
            (kva.create_revision, kva.lease),
            (a.token, Some(a.session.lease()))
        );

        let job = env.key("jobs/x");
        a.commit(vec![put(&job, "by a")]).await.unwrap();

        let (kb, rb) = (key.clone(), leader("b"));
        let b_task = tokio::spawn(async move { eb.campaign(&kb, &rb).await });
        advance(Duration::from_secs(4)).await;
        assert!(!b_task.is_finished(), "b waits while a leads");

        a.resign().await.unwrap();
        assert!(!a.is_leading());
        let b = tokio::time::timeout(WAIT, b_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(b.token > a.token, "tokens only grow");
        assert_eq!(
            a.commit(vec![put(&job, "by a, late")])
                .await
                .unwrap_err()
                .code(),
            Some(SprigCode::NotLeader)
        );
        b.commit(vec![put(&job, "by b")]).await.unwrap();
        assert_eq!(value(&*s, &job).await.as_deref(), Some("by b"));

        // Observers see the handover and end on b.
        let mut last = None;
        while last
            .as_ref()
            .is_none_or(|l: &elect::Leader| l.token != b.token)
        {
            last = tokio::time::timeout(WAIT, obs.next())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            elect::current_leader(&*s, &key)
                .await
                .unwrap()
                .map(|l| l.token),
            Some(b.token)
        );
        b.resign().await.unwrap();
        assert_eq!(kv(&*s, &key).await, None);
    }

    pub(super) async fn deposed_leader_cannot_commit(env: Env) {
        let s = env.meta();
        let key = env.key("leader");
        let opts = SessionOpts::new(Duration::from_secs(3));
        let ea = Election::new(s.clone(), env.clock.clone(), opts);
        let eb = Election::new(s.clone(), env.clock.clone(), opts);
        let a = ea.campaign(&key, &leader("a")).await.unwrap();

        // The key is taken from a while its session is still alive (an
        // operator deleted it); b wins.
        write(&*s, vec![Op::Delete(key.clone())]).await;
        let b = eb.campaign(&key, &leader("b")).await.unwrap();
        assert!(a.is_leading(), "a doesn't know yet");

        // a's guarded writes cannot commit, and the failed compare deposes it.
        let job = env.key("jobs/y");
        let raw = s.txn(ea.guarded(&a, vec![put(&job, "a")])).await.unwrap();
        assert!(!raw.succeeded);
        let err = a.commit(vec![put(&job, "a")]).await.unwrap_err();
        assert_eq!(err.code(), Some(SprigCode::NotLeader));
        assert!(!err.retryable);
        assert!(!a.is_leading());
        tokio::time::timeout(WAIT, a.stepped_down()).await.unwrap();
        assert_eq!(kv(&*s, &job).await, None);
        b.commit(vec![put(&job, "b")]).await.unwrap();
        a.session.close().await.unwrap();
        b.resign().await.unwrap();
    }

    pub(super) async fn election_hands_over_on_lease_expiry(env: Env) {
        let s = env.meta();
        let key = env.key("leader");
        let opts = SessionOpts::new(Duration::from_secs(3));
        let ea = Election::new(s.clone(), env.clock.clone(), opts);
        let eb = Election::new(s.clone(), env.clock.clone(), opts);
        let a = ea.campaign(&key, &leader("a")).await.unwrap();
        let (kb, rb) = (key.clone(), leader("b"));
        let b_task = tokio::spawn(async move { eb.campaign(&kb, &rb).await });

        // a hangs: its session stops renewing, it steps down on its own
        // deadline, and b takes over once the lease expires.
        a.session.halt_keep_alive();
        tokio::time::timeout(WAIT, a.stepped_down()).await.unwrap();
        assert!(!a.is_leading());
        assert!(a.commit(vec![put(&env.key("jobs/z"), "a")]).await.is_err());

        let b = tokio::time::timeout(WAIT, b_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(b.token > a.token);
        let now = elect::current_leader(&*s, &key).await.unwrap().unwrap();
        assert_eq!((now.record.holder.as_str(), now.token), ("b", b.token));
        assert!(
            !s.txn(a.guarded(vec![put(&env.key("jobs/z"), "a")]))
                .await
                .unwrap()
                .succeeded
        );
        b.resign().await.unwrap();
    }

    pub(super) async fn bootstrap_writes_once(env: Env) {
        // Single-etcd mode: the global tier is a view over the same store.
        let g = env.store.view(Tier::Global);
        let Tier::Dc(dc) = env.store.tier().clone() else {
            panic!("cases run on a DC tier");
        };
        // The deployment id and the tenant id counter are global keys, shared
        // by everything on this etcd. Never delete them: if a deployment is
        // already bootstrapped here (a dev controller, another test run),
        // skip the case instead. The DC key is this case's own tier.
        let [dep, dc_key, ids] = bootstrap::bootstrap_keys(&dc).unwrap();
        if kv(&*g, &dep).await.is_some() || kv(&*g, &ids).await.is_some() {
            eprintln!(
                "skipping bootstrap_writes_once: this store already holds a deployment \
                 ({dep} or {ids} exists); run it on an empty etcd"
            );
            return;
        }
        assert!(bootstrap::verify_deployment_id(&*g, "dep-1").await.is_err());

        let clock = &*env.clock;
        assert_eq!(
            bootstrap::bootstrap_first_dc(&*g, clock, "dep-1", &dc)
                .await
                .unwrap(),
            Bootstrap::Created
        );
        assert_eq!(
            bootstrap::bootstrap_first_dc(&*g, clock, "dep-1", &dc)
                .await
                .unwrap(),
            Bootstrap::AlreadyDone
        );
        let other = bootstrap::bootstrap_first_dc(&*g, clock, "dep-2", &dc)
            .await
            .unwrap_err();
        assert_eq!(other.code(), Some(SprigCode::ConfigInvalid));
        bootstrap::verify_deployment_id(&*g, "dep-1").await.unwrap();
        assert_eq!(
            bootstrap::verify_deployment_id(&*g, "dep-2")
                .await
                .unwrap_err()
                .code(),
            Some(SprigCode::ConfigInvalid)
        );

        let rec = Record::<DcRecord>::decode(&kv(&*g, &dc_key).await.unwrap().value).unwrap();
        assert_eq!((rec.body.dc_id, rec.body.state.as_str()), (dc, "active"));
        // The DC tier doesn't see global keys.
        assert!(
            env.store
                .list(&env.store.tier().root(), ListOpts::default())
                .await
                .unwrap()
                .kvs
                .is_empty()
        );
        write(
            &*g,
            vec![Op::Delete(dep), Op::Delete(dc_key), Op::Delete(ids)],
        )
        .await;
    }

    pub(super) async fn mirror_lists_then_follows_writes(env: Env) {
        let s = &*env.store;
        let a = env.key("clusters/c1");
        let b = env.key("clusters/c2");
        let outside = env.key("clusters-other/x");
        write(s, vec![put(&a, "a1"), put(&b, "b1"), put(&outside, "x")]).await;

        let m = Mirror::spawn(env.meta(), env.key("clusters/"));
        tokio::time::timeout(WAIT, m.ready()).await.unwrap();
        let got: Vec<_> = m.entries().iter().map(|kv| kv.key.clone()).collect();
        assert_eq!(got, vec![a.clone(), b.clone()]);
        let listed_at = m.revision();
        assert!(listed_at > 0);

        let mut changes = m.changes();
        changes.borrow_and_update();
        let r = write(s, vec![put(&a, "a2"), Op::Delete(b.clone())]).await;
        tokio::time::timeout(WAIT, m.wait_for(r)).await.unwrap();
        assert!(changes.has_changed().unwrap());
        assert_eq!(m.get(&a).unwrap().value, b"a2");
        assert_eq!(m.get(&a).unwrap().mod_revision, r);
        assert!(m.get(&b).is_none());
        assert_eq!(m.len(), 1);

        // Writes outside the prefix don't reach it.
        let r = write(s, vec![put(&outside, "y"), put(&b, "b2")]).await;
        tokio::time::timeout(WAIT, m.wait_for(r)).await.unwrap();
        assert_eq!(m.len(), 2);
        assert!(m.get(&outside).is_none());

        // Progress marks move the fresh revision.
        env.store.progress_all().await.unwrap();
        let fresh = async {
            while m.fresh_revision() < r {
                advance(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(WAIT, fresh).await.unwrap();
    }

    pub(super) async fn lazy_cache_loads_groups_on_first_use(env: Env) {
        let s = &*env.store;
        let m42_0 = env.key("shardmaps/42/0");
        let m42_1 = env.key("shardmaps/42/1");
        let m7_0 = env.key("shardmaps/7/0");
        write(s, vec![put(&m42_0, "a"), put(&m42_1, "b"), put(&m7_0, "c")]).await;

        let c = LazyCache::spawn(env.meta(), env.key("shardmaps/"), 10);
        assert!(c.cached("42").is_none());
        let g = tokio::time::timeout(WAIT, c.group("42"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            g.keys().cloned().collect::<Vec<_>>(),
            vec![m42_0.clone(), m42_1.clone()]
        );
        assert_eq!(c.len(), 1);
        assert!(c.cached("7").is_none(), "groups load only on use");
        assert!(c.group("a/b").await.is_err());

        // Changes reach a loaded group; other groups stay unloaded.
        let m42_2 = env.key("shardmaps/42/2");
        write(
            s,
            vec![put(&m42_2, "d"), put(&env.key("shardmaps/7/1"), "e")],
        )
        .await;
        write(s, vec![Op::Delete(m42_0.clone())]).await;
        let r = write(s, vec![put(&m42_1, "b2")]).await;
        let caught_up = async {
            loop {
                let g = c.cached("42").unwrap();
                if g.get(&m42_1).is_some_and(|kv| kv.mod_revision == r) {
                    return g;
                }
                advance(Duration::from_millis(10)).await;
            }
        };
        let g = tokio::time::timeout(WAIT, caught_up).await.unwrap();
        assert_eq!(
            g.keys().cloned().collect::<Vec<_>>(),
            vec![m42_1.clone(), m42_2]
        );
        assert_eq!(g[&m42_1].value, b"b2");
        assert!(c.cached("7").is_none());
        let g7 = c.group("7").await.unwrap();
        assert_eq!(g7.len(), 2);
        assert!(c.evict("7"));
        assert!(c.cached("7").is_none());
    }

    pub(super) async fn lazy_cache_evicts_least_recently_used(env: Env) {
        let s = &*env.store;
        write(
            s,
            vec![
                put(&env.key("shardmaps/1/0"), "1"),
                put(&env.key("shardmaps/2/0"), "2"),
                put(&env.key("shardmaps/3/0"), "3"),
            ],
        )
        .await;
        let c = LazyCache::spawn(env.meta(), env.key("shardmaps/"), 2);
        c.group("1").await.unwrap();
        c.group("2").await.unwrap();
        // Using 1 again makes 2 the least recently used.
        c.group("1").await.unwrap();
        c.group("3").await.unwrap();
        assert_eq!(c.len(), 2);
        assert!(c.cached("1").is_some());
        assert!(c.cached("2").is_none());
        assert!(c.cached("3").is_some());
    }
}
