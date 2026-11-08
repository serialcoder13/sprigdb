//! Live cache tests against the Compose stack (see `crate::live_tests` for
//! the environment variables; each case uses its own
//! `__sprig_test_catalog_*` schema, dropped afterwards, and a private etcd
//! tier standing in for the global tier when `SPRIG_TEST_ETCD` is set).
//!
//! - invalidation latency: a committed change reaches a pinned tenant's
//!   cached snapshot through the etcd watch, without any read;
//! - incremental loads equal full loads against MariaDB, for random change
//!   sequences through `MariaCatalog` (property test, few cases);
//! - a `CatalogExtension` loads in the snapshot's transaction, fully then
//!   incrementally; a catalog format newer than the binary is 39073.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use proptest::prelude::*;
use serde_json::json;
use sprig_common::SprigCode;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;

use super::tests::{Model, OpSpec, T, W, obj, op_spec, registry, writes};
use super::*;
use crate::live_tests::{
    catalog_for, maria_url, migrate, seed_directory, unique_tenant, with_global, with_schema,
};
use crate::migrate::{FORMAT_KEY, MigrateOpts, Migration, Migrator, Phase};
use crate::store::{CatalogEndpoint, ObjectState, StaticLocator, Store};
use crate::version::VersionKind;

fn locator(pool: &Pool, schema: &str, tenant: TenantId) -> Arc<dyn crate::store::CatalogLocator> {
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

fn config() -> CacheConfig {
    CacheConfig {
        version_wait: Duration::from_millis(200),
        ..CacheConfig::default()
    }
}

/// Writes `ws` in the open change `c`.
async fn apply_in(c: &mut crate::version::CatalogTx, ws: Vec<W>) {
    let (store, tx) = c.tx().parts();
    for w in ws {
        match w {
            W::Db(r, true) => store.insert_database(tx, &r).await.unwrap(),
            W::Db(r, false) => {
                store.update_database(tx, &r).await.unwrap();
            }
            W::Obj(r, true) => store.insert_object(tx, &r).await.unwrap(),
            W::Obj(r, false) => {
                store.update_object(tx, &r).await.unwrap();
            }
            W::Set(r) => store.put_setting(tx, &r).await.unwrap(),
            W::Reset(s, k, v, b, ver) => {
                store
                    .reset_setting(tx, s, k, v, b, "user:1", std::time::SystemTime::now(), ver)
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_pinned_tenants_follow_etcd_invalidations() {
    with_schema("cache invalidation", |pool, schema| async move {
        migrate(&pool, &schema).await;
        with_global(|g| async move {
            let t = unique_tenant();
            seed_directory(&*g, t).await;
            let cat = catalog_for(&pool, &schema, t, Arc::clone(&g));
            let feed = DirectoryWatch::start(Arc::clone(&g)).unwrap();
            feed.ready().await;
            let source = MariaSource::new(locator(&pool, &schema, t), Vec::new()).unwrap();
            let cache = TenantCache::start(
                registry(),
                Arc::new(source),
                Arc::new(feed.clone()),
                None,
                config(),
            );
            let _pin = cache.pin(t);
            assert_eq!(cache.get(t, Freshness::Latest).await.unwrap().version, 0);

            let mut lat = Vec::new();
            for i in 0..20_u64 {
                let mut c = cat.change(t, VersionKind::Catalog).await.unwrap();
                let v = c.version();
                {
                    let (store, tx) = c.tx().parts();
                    store
                        .insert_object(tx, &obj(i + 1, 1, &format!("t{i}"), v, ObjectState::Active))
                        .await
                        .unwrap();
                }
                c.commit().await.unwrap();
                let t0 = Instant::now();
                while cache.cached(t).is_none_or(|c| c.version < v) {
                    assert!(
                        t0.elapsed() < Duration::from_secs(5),
                        "change {v} never reached the cache"
                    );
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                lat.push(t0.elapsed());
            }
            lat.sort();
            eprintln!(
                "invalidation latency over {} changes ({}): p50 {:?}, max {:?}",
                lat.len(),
                g.tier().root(),
                lat[lat.len() / 2],
                lat[lat.len() - 1]
            );
            assert!(lat[lat.len() - 1] < Duration::from_secs(2));
            assert_eq!(cache.version_lag(), Duration::ZERO);

            // The incrementally loaded snapshot equals a full load.
            let full = TenantCatalog::build(
                registry(),
                t,
                Loaded::core(Store::new(&schema).snapshot(&pool, None).await.unwrap()),
            );
            assert_eq!(*cache.cached(t).unwrap(), full);
            assert_eq!(full.objects.len(), 20);
        })
        .await;
    })
    .await;
}

fn run_live_sequence(ops: Vec<OpSpec>) {
    if maria_url().is_none() {
        return;
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(with_schema(
        "cache incremental property",
        |pool, schema| async move {
            migrate(&pool, &schema).await;
            with_global(|g| async move {
                seed_directory(&*g, T).await;
                let cat = catalog_for(&pool, &schema, T, Arc::clone(&g));
                let store = Store::new(&schema);
                let mut m = Model::default();
                let mut cached: Option<TenantCatalog> = None;
                let mut checks = 0;
                for op in ops.iter().chain(std::iter::once(&OpSpec::Load)) {
                    match op {
                        OpSpec::Load => {
                            let since = cached.as_ref().map(|c| c.version);
                            let loaded = Loaded::core(store.snapshot(&pool, since).await.unwrap());
                            let inc = match &cached {
                                Some(c) => c.apply(loaded),
                                None => TenantCatalog::build(registry(), T, loaded),
                            };
                            let full = TenantCatalog::build(
                                registry(),
                                T,
                                Loaded::core(store.snapshot(&pool, None).await.unwrap()),
                            );
                            assert_eq!(
                                inc, full,
                                "incremental and full loads differ after {ops:?}"
                            );
                            cached = Some(inc);
                            checks += 1;
                        }
                        OpSpec::Gap => {
                            let c = cat.change(T, VersionKind::Catalog).await.unwrap();
                            c.rollback().await.unwrap();
                        }
                        OpSpec::Purge(back) => {
                            let mut conn = pool.get_conn().await.unwrap();
                            let committed = store.versions(&mut conn).await.unwrap().catalog;
                            drop(conn);
                            cat.purge(T, committed.saturating_sub(u64::from(*back)))
                                .await
                                .unwrap();
                        }
                        OpSpec::Batch(_) => {}
                        op => {
                            let mut c = cat.change(T, VersionKind::Catalog).await.unwrap();
                            let v = c.version();
                            let ws = writes(&mut m, v, op);
                            if ws.is_empty() {
                                c.rollback().await.unwrap();
                            } else {
                                apply_in(&mut c, ws).await;
                                c.commit().await.unwrap();
                            }
                        }
                    }
                }
                assert!(checks >= 1);
            })
            .await;
        },
    ));
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 6, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn live_incremental_loads_equal_full_loads(ops in prop::collection::vec(op_spec(), 5..40)) {
        run_live_sequence(ops);
    }
}

// ---- an extension module ----

static ITEMS_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "items table",
    statements: &["CREATE TABLE IF NOT EXISTS test_items (\
          id INT UNSIGNED NOT NULL PRIMARY KEY, label VARCHAR(32) NOT NULL, \
          changed_version BIGINT UNSIGNED NOT NULL, KEY k_changed (changed_version)\
        ) ENGINE=InnoDB"],
}];

#[derive(Debug)]
struct ItemsState {
    items: BTreeMap<u32, String>,
    since: Option<u64>,
}

impl ExtState for ItemsState {
    fn bytes(&self) -> usize {
        self.items.values().map(|l| 64 + l.len()).sum()
    }
}

struct Items;

#[async_trait::async_trait]
impl CatalogExtension for Items {
    fn module(&self) -> &'static str {
        "test_items"
    }
    fn migrations(&self) -> &'static [Migration] {
        ITEMS_MIGRATIONS
    }
    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }
    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        since: Option<u64>,
        prev: Option<&Arc<dyn ExtState>>,
    ) -> sprig_common::Result<Arc<dyn ExtState>> {
        let mut items = match (since, prev) {
            (Some(_), Some(p)) => {
                let p: &dyn std::any::Any = &**p;
                p.downcast_ref::<ItemsState>()
                    .map(|s| s.items.clone())
                    .unwrap_or_default()
            }
            _ => BTreeMap::new(),
        };
        let sql = format!(
            "SELECT id, label FROM {}.test_items WHERE changed_version > ?",
            quote(tx.schema())
        );
        let rows: Vec<(u32, String)> = tx
            .conn()
            .exec(sql, (since.unwrap_or(0),))
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        items.extend(rows);
        Ok(Arc::new(ItemsState { items, since }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_extensions_load_in_the_snapshot_and_formats_are_checked() {
    with_schema("cache extension", |pool, schema| async move {
        let mut conn = pool.get_conn().await.unwrap();
        Migrator::new(&[module_of(&Items)])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        drop(conn);
        with_global(|g| async move {
            let t = unique_tenant();
            seed_directory(&*g, t).await;
            let cat = catalog_for(&pool, &schema, t, Arc::clone(&g));
            let feed = DirectoryWatch::start(Arc::clone(&g)).unwrap();
            feed.ready().await;
            let exts: Vec<Arc<dyn CatalogExtension>> = vec![Arc::new(Items)];
            let source = MariaSource::new(locator(&pool, &schema, t), exts).unwrap();
            let cache = TenantCache::start(
                registry(),
                Arc::new(source),
                Arc::new(feed.clone()),
                None,
                config(),
            );

            let add = |id: u32, label: &'static str| {
                let cat = cat.clone();
                async move {
                    let mut c = cat.change(t, VersionKind::Catalog).await.unwrap();
                    let v = c.version();
                    let (store, tx) = c.tx().parts();
                    tx.exec_drop(
                        format!(
                            "INSERT INTO {}.test_items VALUES (?, ?, ?)",
                            quote(store.schema())
                        ),
                        (id, label, v),
                    )
                    .await
                    .unwrap();
                    c.commit().await.unwrap()
                }
            };
            let v1 = add(1, "one").await;
            let c1 = cache
                .get(t, Freshness::Cached { min_version: v1 })
                .await
                .unwrap();
            let s1 = c1.ext.get::<ItemsState>("test_items").unwrap();
            assert_eq!(s1.items.len(), 1);
            assert_eq!(s1.since, None);
            let v2 = add(2, "two").await;
            let c2 = cache
                .get(t, Freshness::Cached { min_version: v2 })
                .await
                .unwrap();
            let s2 = c2.ext.get::<ItemsState>("test_items").unwrap();
            assert_eq!(s2.items.len(), 2);
            assert_eq!(s2.since, Some(c1.version), "the second load is incremental");
            assert!(c2.bytes() > c1.bytes());

            // A catalog whose core format is newer than this binary reads.
            let mut conn = pool.get_conn().await.unwrap();
            let store = Store::new(&schema);
            let mut fmt = store
                .internal(&mut conn, FORMAT_KEY)
                .await
                .unwrap()
                .unwrap();
            fmt["catalog"]["compat"] = json!(99);
            fmt["catalog"]["format"] = json!(99);
            let v = store.versions(&mut conn).await.unwrap().catalog;
            store
                .put_internal(&mut conn, FORMAT_KEY, &fmt, v, std::time::SystemTime::now())
                .await
                .unwrap();
            drop(conn);
            let source = MariaSource::new(locator(&pool, &schema, t), Vec::new()).unwrap();
            let e = source.load(t, None, Node::Writer).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::CatalogFormatTooNew));
        })
        .await;
    })
    .await;
}
