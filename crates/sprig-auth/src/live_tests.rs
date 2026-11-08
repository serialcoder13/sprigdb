//! Live tests against the Compose stack.
//!
//! - `SPRIG_TEST_MARIADB_SINGLE` (for example
//!   `mysql://root:sprig-dev-root@127.0.0.1:13301`) names the MariaDB;
//!   without it every case is skipped with a message.
//! - `SPRIG_TEST_ETCD` (for example `http://127.0.0.1:2379`) names an etcd
//!   for the global tier (a private tier, deleted afterwards); without it
//!   the cases use `MemStore`.
//!
//! Each case works in its own catalog schema `__sprig_test_auth_<pid>_<n>`,
//! dropped afterwards, also when the case fails.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use serde_json::json;
use sprig_catalog::{
    CacheConfig, CatalogEndpoint, CatalogLocator, DirectoryAllocator, DirectoryWatch, MariaBackend,
    MariaCatalog, MariaSource, StaticLocator, Store, TenantCache, VersionKind, Versioner,
};
use sprig_common::ids::{ClusterId, TenantId, UserId};
use sprig_common::names::quote;
use sprig_common::time::SystemClock;
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::{EtcdConfig, EtcdStore, MemStore, MetaStore, Op, Tier, Txn};
use sprig_proto_mysql::auth::AuthVerdict;
use sprig_settings::Registry;

use crate::account::MetaTenants;
use crate::accounts::BackendTarget;
use crate::admin::{self, NewUser, SetPassword};
use crate::cache::{AuthExtension, TenantAuth};
use crate::fast_cache::FastCache;
use crate::mysql::tests::{FAST, FULL, err, req, run};
use crate::mysql::{CatalogLogins, DefaultPolicy, LoginService, LoginSource, StaticBackends};
use crate::store::{AuthStore, LoginStateRow, Slot, UserState, VerifierKind};
use crate::testing;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

fn unique() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Runs `body` with a pool, a migrated catalog schema and a tenant id;
/// drops the schema afterwards. Skips without `SPRIG_TEST_MARIADB_SINGLE`.
async fn with_catalog<F, Fut>(case: &str, body: F)
where
    F: FnOnce(Pool, String, TenantId) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped {case}: set {MARIADB_ENV} to run it");
        return;
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let n = unique();
    let schema = format!("__sprig_test_auth_{}_{nanos:x}_{n}", std::process::id());
    let tenant = TenantId(9_100_000_000 + u64::from(std::process::id()) * 1000 + n);
    {
        let mut conn = pool.get_conn().await.unwrap();
        testing::migrate(&mut conn, &schema).await.unwrap();
    }
    let r = AssertUnwindSafe(body(pool.clone(), schema.clone(), tenant))
        .catch_unwind()
        .await;
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// Runs `body` with the global tier and deletes its keys afterwards.
async fn with_global<F, Fut>(body: F)
where
    F: FnOnce(Arc<dyn MetaStore>) -> Fut,
    Fut: Future<Output = ()>,
{
    let g: Arc<dyn MetaStore> = match std::env::var(ETCD_ENV) {
        Ok(url) if !url.trim().is_empty() => Arc::new(
            EtcdStore::connect(unique_tier(), &EtcdConfig::new(vec![url]))
                .await
                .unwrap(),
        ),
        _ => Arc::new(MemStore::new(Tier::Global)),
    };
    let r = AssertUnwindSafe(body(Arc::clone(&g))).catch_unwind().await;
    delete_prefix(&*g, &g.tier().root()).await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// The tenant's directory entry and name key; returns its name.
async fn seed_directory(g: &dyn MetaStore, tenant: TenantId) -> String {
    let name = format!("t{}", tenant.0);
    let entry = json!({"v": 1, "tenant_id": tenant.0, "name": name, "state": "active",
                       "path": format!("/{}/", tenant.0), "catalog_version": 0, "auth_version": 0});
    let key = |k: String| g.tier().key(&k).unwrap();
    g.txn(Txn {
        then: vec![
            Op::Put {
                key: key(format!("tenants/{tenant}")),
                value: serde_json::to_vec(&entry).unwrap(),
                lease: None,
            },
            Op::Put {
                key: key(format!("tenant-names/{name}")),
                value: serde_json::to_vec(&json!({"v": 1, "tenant_id": tenant.0})).unwrap(),
                lease: None,
            },
        ],
        ..Txn::default()
    })
    .await
    .unwrap();
    name
}

fn locator(pool: &Pool, schema: &str, tenant: TenantId) -> Arc<dyn CatalogLocator> {
    StaticLocator::new()
        .with(
            tenant,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::new(schema),
            },
        )
        .shared()
}

fn catalog(pool: &Pool, schema: &str, tenant: TenantId, g: &Arc<dyn MetaStore>) -> MariaCatalog {
    Versioner::new(
        MariaBackend::new(locator(pool, schema, tenant)),
        Arc::new(DirectoryAllocator::new(Arc::clone(g))),
    )
}

async fn cache(pool: &Pool, schema: &str, tenant: TenantId, g: &Arc<dyn MetaStore>) -> TenantCache {
    let feed = DirectoryWatch::start(Arc::clone(g)).unwrap();
    feed.ready().await;
    let source =
        MariaSource::new(locator(pool, schema, tenant), vec![Arc::new(AuthExtension)]).unwrap();
    TenantCache::start(
        Registry::global().unwrap(),
        Arc::new(source),
        Arc::new(feed),
        None,
        CacheConfig {
            version_wait: Duration::from_millis(200),
            ..CacheConfig::default()
        },
    )
}

/// Waits until the cache holds `tenant` at auth version `v` or later.
async fn reach(cache: &TenantCache, tenant: TenantId, v: u64) {
    let t0 = Instant::now();
    loop {
        let c = cache
            .get(tenant, sprig_catalog::Freshness::Latest)
            .await
            .unwrap();
        if c.auth_version >= v {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "auth version {v} never reached the cache"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A full load of the tenant's auth state, straight from the tables.
async fn full_state(pool: &Pool, schema: &str) -> TenantAuth {
    let mut conn = pool.get_conn().await.unwrap();
    let s = AuthStore::new(schema);
    let users = s.users_since(&mut conn, None).await.unwrap();
    let verifiers = s.all_verifiers(&mut conn).await.unwrap();
    let locks = s.temp_locks(&mut conn).await.unwrap();
    TenantAuth::build(users, &verifiers, locks)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_users_and_passwords_through_auth_changes() {
    with_catalog("users and passwords", |pool, schema, t| async move {
        with_global(|g| async move {
            seed_directory(&*g, t).await;
            let cat = catalog(&pool, &schema, t, &g);
            let clock = SystemClock;
            let created = admin::create_user(&cat, t, &NewUser::new("alice", b"pw1"), &clock)
                .await
                .unwrap();
            assert_eq!(created.user_id, UserId(1));
            assert_eq!(created.auth_version, 1);
            // The same live account again: MySQL's 1396.
            let e = admin::create_user(&cat, t, &NewUser::new("alice", b"x"), &clock)
                .await
                .unwrap_err();
            assert!(
                matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1396, .. }),
                "{e}"
            );
            // Another host is another account.
            let mut office = NewUser::new("alice", b"office");
            office.host = "10.%".into();
            let office = admin::create_user(&cat, t, &office, &clock).await.unwrap();
            assert_eq!(
                office.user_id,
                UserId(2),
                "a rolled-back create reserved nothing"
            );

            let store = AuthStore::new(&schema);
            let mut conn = pool.get_conn().await.unwrap();
            let row = store.user(&mut conn, UserId(1)).await.unwrap().unwrap();
            assert_eq!(
                (row.name.as_str(), row.host.as_str(), row.changed_version),
                ("alice", "%", 1)
            );
            assert!(row.password_changed_at.is_some());
            assert_eq!(
                store
                    .user_by_account(&mut conn, "alice", "10.%")
                    .await
                    .unwrap()
                    .unwrap()
                    .user_id,
                UserId(2)
            );
            assert_eq!(
                admin::find_user(&cat, t, "alice", "%").await.unwrap(),
                Some(UserId(1))
            );

            // RETAIN CURRENT PASSWORD: primary → secondary; again: the old
            // secondary goes to history.
            let keep = SetPassword {
                retain_current: true,
                keep_history: 5,
                ..SetPassword::default()
            };
            admin::set_password(&cat, t, UserId(1), b"pw2", keep, &clock)
                .await
                .unwrap();
            let c = admin::set_password(&cat, t, UserId(1), b"pw3", keep, &clock)
                .await
                .unwrap();
            let vs = store.verifiers_of(&mut conn, &[UserId(1)]).await.unwrap();
            let slots: Vec<(Slot, u64)> = vs.iter().map(|v| (v.slot, v.generation)).collect();
            assert_eq!(
                slots,
                [(Slot::Primary, 3), (Slot::Secondary, 2), (Slot::History, 1)]
            );
            assert!(vs.iter().all(|v| v.kind == VerifierKind::CachingSha2));
            let row = store.user(&mut conn, UserId(1)).await.unwrap().unwrap();
            assert_eq!(row.changed_version, c.auth_version);
            // DISCARD OLD PASSWORD, keeping no history.
            admin::discard_old_password(&cat, t, UserId(1), 0)
                .await
                .unwrap();
            let vs = store.verifiers_of(&mut conn, &[UserId(1)]).await.unwrap();
            assert_eq!(
                vs.iter().map(|v| v.slot).collect::<Vec<_>>(),
                [Slot::Primary]
            );

            // Native verifiers when the tenant enables them.
            let mut n = SetPassword::default();
            n.hashing.native = true;
            admin::set_password(&cat, t, UserId(2), b"office2", n, &clock)
                .await
                .unwrap();
            let vs = store.verifiers_of(&mut conn, &[UserId(2)]).await.unwrap();
            let primary: Vec<VerifierKind> = vs
                .iter()
                .filter(|v| v.slot == Slot::Primary)
                .map(|v| v.kind)
                .collect();
            assert_eq!(primary, [VerifierKind::CachingSha2, VerifierKind::Native]);

            // DROP USER: a tombstone; the name is free again, with a new id.
            admin::drop_user(&cat, t, UserId(1)).await.unwrap();
            let row = store.user(&mut conn, UserId(1)).await.unwrap().unwrap();
            assert_eq!(row.state, UserState::Dropped);
            assert!(
                store
                    .verifiers_of(&mut conn, &[UserId(1)])
                    .await
                    .unwrap()
                    .is_empty()
            );
            let again = admin::create_user(&cat, t, &NewUser::new("alice", b"pw"), &clock)
                .await
                .unwrap();
            assert_eq!(
                again.user_id,
                UserId(3),
                "ids of dropped users are never reused"
            );
            assert!(
                admin::set_account_locked(&cat, t, UserId(1), true)
                    .await
                    .is_err(),
                "dropped"
            );

            // login_state round trip.
            let ls = LoginStateRow {
                user_id: UserId(3),
                consecutive_failures: 2,
                locked_until: Some(UNIX_EPOCH + Duration::from_micros(1_900_000_000_123_456)),
                last_failure_at: None,
                last_success_at: None,
                last_success_ip: Some(vec![10, 0, 0, 1]),
                updated_at: UNIX_EPOCH + Duration::from_secs(1_762_560_000),
            };
            store.put_login_state(&mut conn, &ls).await.unwrap();
            assert_eq!(
                store.login_state(&mut conn, UserId(3)).await.unwrap(),
                Some(ls)
            );
            assert_eq!(store.temp_locks(&mut conn).await.unwrap().len(), 1);
        })
        .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_the_cache_loads_auth_incrementally() {
    with_catalog("auth cache", |pool, schema, t| async move {
        with_global(|g| async move {
            seed_directory(&*g, t).await;
            let cat = catalog(&pool, &schema, t, &g);
            let cache = cache(&pool, &schema, t, &g).await;
            let _pin = cache.pin(t);
            let first = cache
                .get(t, sprig_catalog::Freshness::Latest)
                .await
                .unwrap();
            assert!(TenantAuth::of(&first).unwrap().is_empty());

            let clock = SystemClock;
            let mut v = 0;
            for i in 0..6_u32 {
                v = admin::create_user(&cat, t, &NewUser::new(format!("u{i}"), b"pw"), &clock)
                    .await
                    .unwrap()
                    .auth_version;
            }
            reach(&cache, t, v).await;
            v = admin::set_password(
                &cat,
                t,
                UserId(2),
                b"new",
                SetPassword {
                    retain_current: true,
                    ..SetPassword::default()
                },
                &clock,
            )
            .await
            .unwrap()
            .auth_version;
            admin::set_account_locked(&cat, t, UserId(3), true)
                .await
                .unwrap();
            admin::drop_user(&cat, t, UserId(4)).await.unwrap();
            v = v.max(
                admin::set_password_expired(&cat, t, UserId(5), true)
                    .await
                    .unwrap()
                    .auth_version,
            );
            reach(&cache, t, v).await;

            let cached = cache.cached(t).unwrap();
            let auth = TenantAuth::of(&cached).unwrap();
            assert_eq!(
                *auth,
                full_state(&pool, &schema).await,
                "incremental equals full"
            );
            assert_eq!(auth.len(), 5);
            assert_eq!(auth.user(UserId(2)).unwrap().caching_sha2.len(), 2);
            assert!(auth.user(UserId(3)).unwrap().row.account_locked);
            assert!(auth.user(UserId(4)).is_none());
            assert!(auth.user(UserId(5)).unwrap().row.password_expired);
            assert!(cached.ext.bytes() > 0);
        })
        .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_logins_follow_auth_changes() {
    with_catalog("catalog logins", |pool, schema, t| async move {
        with_global(|g| async move {
            let name = seed_directory(&*g, t).await;
            let cat = catalog(&pool, &schema, t, &g);
            let cache = Arc::new(cache(&pool, &schema, t, &g).await);
            let _pin = cache.pin(t);
            let backend = BackendTarget {
                cluster: ClusterId::new("dc-a-c1").unwrap(),
                user: "root".into(),
                credentials: "env:SPRIG_TEST_BACKEND".parse().unwrap(),
            };
            let logins = CatalogLogins::new(
                Arc::new(MetaTenants::new(Arc::clone(&g), Duration::ZERO)),
                cache.clone(),
                Arc::new(StaticBackends::new(None).with(t, backend.clone())),
                Arc::new(DefaultPolicy),
            );
            let svc = Arc::new(LoginService::new(
                LoginSource::Catalog(Arc::new(logins)),
                Arc::new(FastCache::new(
                    Arc::new(SystemClock),
                    crate::fast_cache::DEFAULT_TTL,
                )),
            ));
            let alice = format!("{name}.alice");
            let clock = SystemClock;

            // No user yet: the decoy.
            let r = run(&svc, req(&alice), b"pw").await;
            assert_eq!(
                (r.seen.as_slice(), err(&r.verdict).code),
                ([FULL.to_vec()].as_slice(), 1045)
            );

            let id = testing::create_user(&cat, t, "alice", "%", "pw")
                .await
                .unwrap();
            reach(&cache, t, 1).await;
            let r = run(&svc, req(&alice), b"pw").await;
            assert_eq!(r.verdict, AuthVerdict::Accept);
            let p = r.login.principal().unwrap();
            assert_eq!(
                (p.tenant, p.user_id, p.backend.clone()),
                (Some(t), Some(id), backend.clone())
            );
            assert_eq!(run(&svc, req(&alice), b"pw").await.seen, [FAST]);

            // ACCOUNT LOCK reaches the login: 3118 with the right password.
            let v = admin::set_account_locked(&cat, t, id, true)
                .await
                .unwrap()
                .auth_version;
            reach(&cache, t, v).await;
            assert_eq!(err(&run(&svc, req(&alice), b"pw").await.verdict).code, 3118);
            assert_eq!(
                err(&run(&svc, req(&alice), b"bad").await.verdict).code,
                1045
            );
            let v = admin::set_account_locked(&cat, t, id, false)
                .await
                .unwrap()
                .auth_version;
            reach(&cache, t, v).await;

            // A password change: the old one (and its fast-cache entry)
            // stop working at once.
            let v = admin::set_password(&cat, t, id, b"pw2", SetPassword::default(), &clock)
                .await
                .unwrap()
                .auth_version;
            reach(&cache, t, v).await;
            let r = run(&svc, req(&alice), b"pw").await;
            assert_eq!(
                (r.seen.as_slice(), err(&r.verdict).code),
                ([FULL.to_vec()].as_slice(), 1045)
            );
            assert_eq!(
                run(&svc, req(&alice), b"pw2").await.verdict,
                AuthVerdict::Accept
            );

            // A temporary lock (step 5 writes it; here by hand, with an
            // empty auth change as the lock transition's version bump).
            let mut c = cat.change(t, VersionKind::Auth).await.unwrap();
            let v = c.version();
            {
                let (core, q) = c.tx().parts();
                AuthStore::of(core)
                    .put_login_state(
                        q,
                        &LoginStateRow {
                            user_id: id,
                            consecutive_failures: 4,
                            locked_until: Some(SystemTime::now() + Duration::from_secs(600)),
                            last_failure_at: Some(SystemTime::now()),
                            last_success_at: None,
                            last_success_ip: None,
                            updated_at: SystemTime::now(),
                        },
                    )
                    .await
                    .unwrap();
            }
            c.commit().await.unwrap();
            reach(&cache, t, v).await;
            assert_eq!(
                err(&run(&svc, req(&alice), b"pw2").await.verdict).code,
                3955
            );

            // A dropped user logs in as an unknown one.
            let v = admin::drop_user(&cat, t, id).await.unwrap().auth_version;
            reach(&cache, t, v).await;
            let r = run(&svc, req(&alice), b"pw2").await;
            assert_eq!(
                (r.seen.as_slice(), err(&r.verdict).code),
                ([FULL.to_vec()].as_slice(), 1045)
            );
        })
        .await;
    })
    .await;
}
