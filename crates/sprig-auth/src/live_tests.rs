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
            let slots: Vec<(Slot, u64)> = vs
                .iter()
                .filter(|v| v.kind == VerifierKind::CachingSha2)
                .map(|v| (v.slot, v.generation))
                .collect();
            assert_eq!(
                slots,
                [(Slot::Primary, 3), (Slot::Secondary, 2), (Slot::History, 1)]
            );
            // Every generation has both SCRAM verifiers, moved with it; a
            // retained password shares the new one's SCRAM salt.
            for (slot, generation) in slots {
                let mut kinds: Vec<VerifierKind> = vs
                    .iter()
                    .filter(|v| v.slot == slot && v.generation == generation)
                    .map(|v| v.kind)
                    .collect();
                kinds.sort();
                assert_eq!(
                    kinds,
                    [
                        VerifierKind::CachingSha2,
                        VerifierKind::ScramSha256,
                        VerifierKind::ScramSha512
                    ]
                );
            }
            let salt = |slot: Slot, kind: VerifierKind| {
                vs.iter()
                    .find(|v| v.slot == slot && v.kind == kind)
                    .and_then(|v| v.salt.clone())
                    .unwrap()
            };
            for kind in [VerifierKind::ScramSha256, VerifierKind::ScramSha512] {
                assert_eq!(salt(Slot::Primary, kind), salt(Slot::Secondary, kind));
                assert_eq!(salt(Slot::Secondary, kind), salt(Slot::History, kind));
            }
            let row = store.user(&mut conn, UserId(1)).await.unwrap().unwrap();
            assert_eq!(row.changed_version, c.auth_version);
            // DISCARD OLD PASSWORD, keeping no history.
            admin::discard_old_password(&cat, t, UserId(1), 0)
                .await
                .unwrap();
            let vs = store.verifiers_of(&mut conn, &[UserId(1)]).await.unwrap();
            assert_eq!(
                vs.iter().map(|v| v.slot).collect::<Vec<_>>(),
                [Slot::Primary; 3],
                "caching_sha2_password and both SCRAM verifiers"
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
            assert_eq!(
                primary,
                [
                    VerifierKind::CachingSha2,
                    VerifierKind::Native,
                    VerifierKind::ScramSha256,
                    VerifierKind::ScramSha512
                ]
            );

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

/// Chapter 14's SCRAM verifiers in a real catalog: format 3's migration
/// on a format-2 catalog, the backfill of a password without them (and of
/// a retained one, with the primary's salt), and the upgrade on login
/// through the login service with the catalog's writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_scram_verifiers_migration_and_backfill() {
    use crate::scram::{ScramKind, ScramVerifier};
    use sprig_catalog::Migrator;
    use sprig_catalog::migrate::MigrateOpts;

    with_catalog("scram verifiers", |pool, schema, t| async move {
        // Format 3 on a format-2 catalog: scram_sha_512 rows fail before
        // and work after; rerunning changes nothing.
        let old = format!("{schema}_f2");
        let mut conn = pool.get_conn().await.unwrap();
        let f2 = sprig_catalog::Module {
            name: crate::store::MODULE,
            migrations: &crate::store::AUTH_MIGRATIONS[..2],
        };
        let migrated = async {
            Migrator::new(&[f2])
                .unwrap()
                .run(&mut conn, &old, &MigrateOpts::default())
                .await
                .unwrap();
            let insert = format!(
                "INSERT INTO {}.verifiers (user_id, mechanism, slot, generation, created_at) \
                 VALUES (1, 'scram_sha_512', 'primary', 1, UTC_TIMESTAMP(6))",
                quote(&old)
            );
            let before = conn.query_drop(insert.as_str()).await;
            let t0 = Instant::now();
            Migrator::new(&[crate::store::AUTH_MODULE])
                .unwrap()
                .run(&mut conn, &old, &MigrateOpts::default())
                .await
                .unwrap();
            let took = t0.elapsed();
            Migrator::new(&[crate::store::AUTH_MODULE])
                .unwrap()
                .run(&mut conn, &old, &MigrateOpts::default())
                .await
                .unwrap();
            let after = conn.query_drop(insert.as_str()).await;
            let column: Option<String> = conn
                .exec_first(
                    "SELECT CAST(COLUMN_TYPE AS CHAR) FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'verifiers' \
                     AND COLUMN_NAME = 'mechanism'",
                    (old.as_str(),),
                )
                .await
                .unwrap();
            (before, after, column, took)
        }
        .await;
        conn.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&old)))
            .await
            .unwrap();
        drop(conn);
        let (before, after, column, took) = migrated;
        assert!(before.is_err(), "format 2 has no scram_sha_512");
        after.unwrap();
        assert!(column.unwrap().ends_with(",'scram_sha_512')"));
        println!("format 2 to 3 on an empty verifiers table: {took:?}");

        with_global(|g| async move {
            let name = seed_directory(&*g, t).await;
            let cat = catalog(&pool, &schema, t, &g);
            let clock = SystemClock;
            let store = AuthStore::new(&schema);
            let mut conn = pool.get_conn().await.unwrap();
            let id = admin::create_user(&cat, t, &NewUser::new("kafka", b"pw-1"), &clock)
                .await
                .unwrap()
                .user_id;
            let scram_rows = |vs: &[crate::store::VerifierRow]| {
                vs.iter()
                    .filter(|v| ScramKind::of(v.kind).is_some())
                    .map(|v| (v.kind, v.slot, v.generation))
                    .collect::<Vec<_>>()
            };
            let vs = store.verifiers_of(&mut conn, &[id]).await.unwrap();
            assert_eq!(
                scram_rows(&vs),
                [
                    (VerifierKind::ScramSha256, Slot::Primary, 1),
                    (VerifierKind::ScramSha512, Slot::Primary, 1)
                ]
            );
            for v in vs.iter().filter(|v| ScramKind::of(v.kind).is_some()) {
                let s = ScramVerifier::from_row(v).unwrap();
                assert_eq!(s.iterations(), 15_000);
                assert_eq!(s.salt().len(), 28);
                assert!(s.verify_password(b"pw-1"));
                assert!(v.auth_string.is_none());
            }
            // A password set before SCRAM verifiers existed: none.
            conn.query_drop(format!(
                "DELETE FROM {}.verifiers WHERE mechanism IN ('scram_sha_256','scram_sha_512')",
                quote(&schema)
            ))
            .await
            .unwrap();
            let sha2_id = |vs: &[crate::store::VerifierRow], slot: Slot| {
                vs.iter()
                    .find(|v| v.kind == VerifierKind::CachingSha2 && v.slot == slot)
                    .unwrap()
                    .verifier_id
            };
            let primary = sha2_id(&vs, Slot::Primary);
            let (c, kinds) = admin::backfill_scram(
                &cat,
                t,
                id,
                primary,
                b"pw-1",
                admin::Hashing::default(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(kinds, ScramKind::ALL);
            let vs = store.verifiers_of(&mut conn, &[id]).await.unwrap();
            assert_eq!(scram_rows(&vs).len(), 2);
            let row = store.user(&mut conn, id).await.unwrap().unwrap();
            assert_eq!(row.changed_version, c.auth_version, "the cache reloads it");
            // Nothing missing: no change.
            assert!(
                admin::backfill_scram(&cat, t, id, primary, b"pw-1", admin::Hashing::default())
                    .await
                    .unwrap()
                    .is_none()
            );
            // A retained password without SCRAM verifiers gets the new
            // primary's salt.
            admin::set_password(
                &cat,
                t,
                id,
                b"pw-2",
                SetPassword {
                    retain_current: true,
                    ..SetPassword::default()
                },
                &clock,
            )
            .await
            .unwrap();
            conn.query_drop(format!(
                "DELETE FROM {}.verifiers WHERE slot = 'secondary' AND mechanism <> 'caching_sha2_password'",
                quote(&schema)
            ))
            .await
            .unwrap();
            let vs = store.verifiers_of(&mut conn, &[id]).await.unwrap();
            admin::backfill_scram(
                &cat,
                t,
                id,
                sha2_id(&vs, Slot::Secondary),
                b"pw-1",
                admin::Hashing::default(),
            )
            .await
            .unwrap()
            .unwrap();
            let vs = store.verifiers_of(&mut conn, &[id]).await.unwrap();
            for kind in ScramKind::ALL {
                let get = |slot: Slot| {
                    let r = vs
                        .iter()
                        .find(|v| v.kind == kind.verifier_kind() && v.slot == slot)
                        .unwrap();
                    ScramVerifier::from_row(r).unwrap()
                };
                let (p, s) = (get(Slot::Primary), get(Slot::Secondary));
                assert_eq!(p.salt(), s.salt());
                assert!(p.verify_password(b"pw-2") && s.verify_password(b"pw-1"));
            }

            // Upgrade on login: a full login through the login service with
            // the catalog's writer stores them.
            let id2 = admin::create_user(&cat, t, &NewUser::new("legacy", b"pw-l"), &clock)
                .await
                .unwrap()
                .user_id;
            conn.query_drop(format!(
                "DELETE FROM {}.verifiers WHERE user_id = {} AND mechanism <> 'caching_sha2_password'",
                quote(&schema),
                id2.0
            ))
            .await
            .unwrap();
            let v = admin::set_require_tls(&cat, t, id2, crate::store::RequireTls::None)
                .await
                .unwrap()
                .auth_version;
            let cache = Arc::new(cache(&pool, &schema, t, &g).await);
            reach(&cache, t, v).await;
            let logins = CatalogLogins::new(
                Arc::new(MetaTenants::new(Arc::clone(&g), Duration::ZERO)),
                cache.clone(),
                Arc::new(StaticBackends::new(None).with(
                    t,
                    BackendTarget {
                        cluster: ClusterId::new("dc-a-c1").unwrap(),
                        user: "root".into(),
                        credentials: "env:SPRIG_TEST_BACKEND".parse().unwrap(),
                    },
                )),
                Arc::new(DefaultPolicy),
            );
            let svc = Arc::new(
                LoginService::new(
                    LoginSource::Catalog(Arc::new(logins)),
                    Arc::new(FastCache::new(
                        Arc::new(SystemClock),
                        crate::fast_cache::DEFAULT_TTL,
                    )),
                )
                .with_login_state(Arc::new(crate::policy::CatalogLoginState::new(
                    cat.clone(),
                ))),
            );
            let login = format!("{name}.legacy");
            let t0 = Instant::now();
            assert_eq!(
                run(&svc, req(&login), b"pw-l").await.verdict,
                AuthVerdict::Accept
            );
            let stored = loop {
                let vs = store.verifiers_of(&mut conn, &[id2]).await.unwrap();
                if scram_rows(&vs).len() == 2 {
                    break t0.elapsed();
                }
                assert!(t0.elapsed() < Duration::from_secs(5), "never backfilled");
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            println!("login to SCRAM verifiers stored: {stored:?}");
            // The cache follows; a later login finds them and adds nothing.
            let row = store.user(&mut conn, id2).await.unwrap().unwrap();
            reach(&cache, t, row.changed_version).await;
            let cached = cache.cached(t).unwrap();
            let u = TenantAuth::of(&cached).unwrap().user(id2).unwrap().clone();
            assert!(u.has_scram(ScramKind::Sha256, Slot::Primary));
            assert!(u.has_scram(ScramKind::Sha512, Slot::Primary));

            // DROP USER deletes them with the others.
            admin::drop_user(&cat, t, id).await.unwrap();
            assert!(
                store
                    .verifiers_of(&mut conn, &[id])
                    .await
                    .unwrap()
                    .is_empty()
            );
        })
        .await;
    })
    .await;
}

/// Decision bugfix-interim (P1 of 13 Nov): `CREATE USER` on a catalog
/// left at `auth` format 2 failed with 1265 (its `verifiers.mechanism`
/// lacks `scram_sha_512`, which every new password stores). The gateway
/// now runs [`crate::authz::migrate`] once per process and tenant; after
/// it, creating a user works on a fresh catalog and on one upgraded from
/// format 2, and running it again on a current catalog changes nothing (no
/// auth change).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_create_user_on_fresh_and_upgraded_catalogs() {
    use sprig_catalog::Migrator;
    use sprig_catalog::migrate::MigrateOpts;

    with_catalog(
        "create user, fresh and upgraded",
        |pool, schema, t| async move {
            with_global(|g| async move {
                // Fresh: every format from the start.
                seed_directory(&*g, t).await;
                let cat = catalog(&pool, &schema, t, &g);
                let loc = locator(&pool, &schema, t);
                assert!(crate::authz::migrate(&*loc, &cat, t).await.unwrap() > 0);
                assert_eq!(
                    crate::authz::migrate(&*loc, &cat, t).await.unwrap(),
                    0,
                    "a current catalog: no auth change"
                );
                admin::create_user(&cat, t, &NewUser::new("fresh", b"pw-1"), &SystemClock)
                    .await
                    .unwrap();

                // Upgraded: a catalog at auth format 2, as the dev stack's t1.
                let old = format!("{schema}_f2");
                let t2 = TenantId(t.0 + 500);
                seed_directory(&*g, t2).await;
                let r = async {
                    let mut conn = pool.get_conn().await.unwrap();
                    let f2 = sprig_catalog::Module {
                        name: crate::store::MODULE,
                        migrations: &crate::store::AUTH_MIGRATIONS[..2],
                    };
                    Migrator::new(&[f2])
                        .unwrap()
                        .run(&mut conn, &old, &MigrateOpts::default())
                        .await
                        .unwrap();
                    drop(conn);
                    let cat2 = catalog(&pool, &old, t2, &g);
                    let loc2 = locator(&pool, &old, t2);
                    let before = admin::create_user(
                        &cat2,
                        t2,
                        &NewUser::new("before", b"pw-1"),
                        &SystemClock,
                    )
                    .await;
                    crate::authz::migrate(&*loc2, &cat2, t2).await.unwrap();
                    let after = admin::create_user(
                        &cat2,
                        t2,
                        &NewUser::new("after", b"pw-1"),
                        &SystemClock,
                    )
                    .await;
                    (before, after)
                }
                .await;
                let mut conn = pool.get_conn().await.unwrap();
                conn.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&old)))
                    .await
                    .unwrap();
                drop(conn);
                let (before, after) = r;
                let e = before.expect_err("format 2 can't store a scram_sha_512 verifier");
                println!("CREATE USER on auth format 2 before the migration: {e}");
                after.unwrap();
            })
            .await;
        },
    )
    .await;
}
