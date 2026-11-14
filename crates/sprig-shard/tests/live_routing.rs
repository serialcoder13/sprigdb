//! A hand-made two-shard tenant, routed live: the shard maps
//! live in etcd and reach the code through `ShardMapCache`; shard 1 is on
//! the single node, shard 2 on the replication primary.
//!
//! - group 1, keyed by a numeric customer id: `customers` (column member)
//!   and `orders` (parent member, `_sprig_rk` copied from its customer);
//! - group 2, keyed by `email` under `utf8mb4_uca1400_ai_ci`: equal emails
//!   in other case, with trailing spaces or NBSP must route to the row.
//!
//! Checks: no row sits outside its shard's range, every row is found on
//! the shard its key routes to and not on the other, the hashes Rust
//! computes from MariaDB's own `WEIGHT_STRING(... LEVEL k)` equal the
//! stored `_sprig_rk`, and a map change reaches the cache.
//!
//! Needs `SPRIG_TEST_ETCD`, `SPRIG_TEST_MARIADB_SINGLE` and
//! `SPRIG_TEST_MARIADB_REPLICATION` (its first URL). Everything it creates
//! is named after the agent and dropped by exact name.

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, ShardId, TenantId};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_shard::collation::{Charset, collation_by_name};
use sprig_shard::rk::{RK_COLUMN, add_rk_column, misrouted_count_sql};
use sprig_shard::{
    KeyClass, KeySpec, KeyValue, ShardMap, ShardMapCache, ShardMapStore, routing_hash, shard_schema,
};

const TENANT: TenantId = TenantId(7_310_001); // the test's tenant id
const DB: DbId = DbId(1);

fn env(n: &str) -> Option<String> {
    std::env::var(n).ok().filter(|v| !v.is_empty())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_two_shard_tenant_routes() {
    let (Some(etcd), Some(single), Some(repl)) = (
        env("SPRIG_TEST_ETCD"),
        env("SPRIG_TEST_MARIADB_SINGLE"),
        env("SPRIG_TEST_MARIADB_REPLICATION"),
    ) else {
        eprintln!("skipped: needs SPRIG_TEST_ETCD, _MARIADB_SINGLE, _MARIADB_REPLICATION");
        return;
    };
    let primary = repl.split(',').next().unwrap().trim().to_owned();
    let tier = Tier::Dc(DcId::new("r1shard-dc").unwrap());
    let meta: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(tier, &EtcdConfig::new(vec![etcd]))
            .await
            .unwrap(),
    );
    let c1 = ClusterId::new("r1shard-c1").unwrap();
    let c2 = ClusterId::new("r1shard-c2").unwrap();
    let pools = [
        (c1.clone(), mysql_async::Pool::new(single.as_str())),
        (c2.clone(), mysql_async::Pool::new(primary.as_str())),
    ];
    let pool_of = |c: &ClusterId| &pools.iter().find(|(k, _)| k == c).unwrap().1;

    let ids = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    let ci = collation_by_name("utf8mb4_uca1400_ai_ci").unwrap();
    let emails = KeySpec::new(vec![KeyClass::String { collation: ci.id }]).unwrap();
    let store = ShardMapStore::new(Arc::clone(&meta));
    // Leftovers of an interrupted run, by exact key.
    for g in [GroupId(1), GroupId(2)] {
        store.delete(TENANT, g).await.unwrap();
    }
    let m1 =
        ShardMap::presplit(TENANT, GroupId(1), ids.digest, 2, &[c1.clone(), c2.clone()]).unwrap();
    let m2 = ShardMap::presplit(
        TENANT,
        GroupId(2),
        emails.digest,
        2,
        &[c1.clone(), c2.clone()],
    )
    .unwrap();
    store.create(&m1).await.unwrap();
    store.create(&m2).await.unwrap();
    let cache = ShardMapCache::spawn(Arc::clone(&meta)).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        cache.ready().await;
        cache.wait_version(TENANT, GroupId(1), 1).await;
        cache.wait_version(TENANT, GroupId(2), 1).await;
    })
    .await
    .unwrap();

    // Schemas: one per shard of each group (exact names, recorded here).
    let mut schemas: Vec<(ClusterId, String)> = Vec::new();
    for m in [&m1, &m2] {
        for r in &m.ranges {
            let s = shard_schema(TENANT, DB, m.group, r.shard);
            let mut c = pool_of(&r.cluster).get_conn().await.unwrap();
            c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
                .await
                .unwrap();
            c.query_drop(format!("CREATE DATABASE `{s}`"))
                .await
                .unwrap();
            if m.group == GroupId(1) {
                c.query_drop(format!(
                    "CREATE TABLE `{s}`.customers (id BIGINT PRIMARY KEY, name VARCHAR(40))"
                ))
                .await
                .unwrap();
                c.query_drop(format!(
                    "CREATE TABLE `{s}`.orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL)"
                ))
                .await
                .unwrap();
                c.query_drop(format!("USE `{s}`")).await.unwrap();
                c.query_drop(add_rk_column("customers")).await.unwrap();
                c.query_drop(add_rk_column("orders")).await.unwrap();
            } else {
                c.query_drop(format!(
                    "CREATE TABLE `{s}`.people (email VARCHAR(100) COLLATE utf8mb4_uca1400_ai_ci \
                     PRIMARY KEY, n INT)"
                ))
                .await
                .unwrap();
                c.query_drop(format!("USE `{s}`")).await.unwrap();
                c.query_drop(add_rk_column("people")).await.unwrap();
            }
            schemas.push((r.cluster.clone(), s));
        }
    }

    // Group 1: customers by id; orders follow their customer.
    let map1 = cache.get(TENANT, GroupId(1)).unwrap();
    let mut per_shard = std::collections::BTreeMap::<ShardId, usize>::new();
    for id in 1..=200i64 {
        let r = sprig_shard::route_key(&map1, &ids, &[KeyValue::Int(i128::from(id))]).unwrap();
        *per_shard.entry(r.range.shard).or_default() += 1;
        let s = shard_schema(TENANT, DB, GroupId(1), r.range.shard);
        let mut c = pool_of(&r.range.cluster).get_conn().await.unwrap();
        c.exec_drop(
            format!("INSERT INTO `{s}`.customers (id, name, {RK_COLUMN}) VALUES (?, ?, ?)"),
            (id, format!("c{id}"), r.hash.0),
        )
        .await
        .unwrap();
        for k in 0..3 {
            // A parent member: the order takes its customer's hash.
            c.exec_drop(
                format!("INSERT INTO `{s}`.orders (id, customer_id, {RK_COLUMN}) VALUES (?, ?, ?)"),
                (id * 10 + k, id, r.hash.0),
            )
            .await
            .unwrap();
        }
    }
    assert_eq!(per_shard.len(), 2, "both shards get rows: {per_shard:?}");
    for (i, r) in map1.ranges.iter().enumerate() {
        let s = shard_schema(TENANT, DB, GroupId(1), r.shard);
        let mut c = pool_of(&r.cluster).get_conn().await.unwrap();
        c.query_drop(format!("USE `{s}`")).await.unwrap();
        for t in ["customers", "orders"] {
            let bad: u64 = c
                .query_first(misrouted_count_sql(t, r.lo, map1.hi(i)))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(bad, 0, "{s}.{t}");
        }
        // A co-located join stays inside the shard.
        let n: u64 = c
            .query_first("SELECT COUNT(*) FROM customers c JOIN orders o ON o.customer_id = c.id")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, per_shard[&r.shard] as u64 * 3);
    }
    for id in [1i64, 42, 99, 200] {
        let r = sprig_shard::route_key(&map1, &ids, &[KeyValue::Int(i128::from(id))]).unwrap();
        for rr in &map1.ranges {
            let s = shard_schema(TENANT, DB, GroupId(1), rr.shard);
            let mut c = pool_of(&rr.cluster).get_conn().await.unwrap();
            let found: Option<i64> = c
                .exec_first(
                    format!("SELECT id FROM `{s}`.customers WHERE id = ?"),
                    (id,),
                )
                .await
                .unwrap();
            assert_eq!(
                found.is_some(),
                rr.shard == r.range.shard,
                "customer {id} on {s}"
            );
        }
    }

    // Group 2: equal emails route together.
    let map2 = cache.get(TENANT, GroupId(2)).unwrap();
    let mut n = 0;
    for i in 0..100 {
        let e = format!("Person{i}@Example.com");
        let r = sprig_shard::route_key(&map2, &emails, &[KeyValue::str(&e)]).unwrap();
        let s = shard_schema(TENANT, DB, GroupId(2), r.range.shard);
        let mut c = pool_of(&r.range.cluster).get_conn().await.unwrap();
        c.exec_drop(
            format!("INSERT INTO `{s}`.people (email, n, {RK_COLUMN}) VALUES (?, ?, ?)"),
            (&e, i, r.hash.0),
        )
        .await
        .unwrap();
        for variant in [
            e.to_uppercase(),
            e.to_lowercase(),
            format!("{e}   "),
            format!("{}\u{a0}", e.to_lowercase()),
        ] {
            let rv = sprig_shard::route_key(&map2, &emails, &[KeyValue::str(&variant)]).unwrap();
            assert_eq!(rv.hash, r.hash, "{variant:?}");
            let sv = shard_schema(TENANT, DB, GroupId(2), rv.range.shard);
            let mut cv = pool_of(&rv.range.cluster).get_conn().await.unwrap();
            let got: Option<i32> = cv
                .exec_first(
                    format!("SELECT n FROM `{sv}`.people WHERE email = ?"),
                    (&variant,),
                )
                .await
                .unwrap();
            assert_eq!(got, Some(i), "{variant:?} finds its row");
            n += 1;
        }
    }
    // MariaDB's weights, canonicalized in Rust, give the stored hashes.
    let mut checked = 0;
    for r in &map2.ranges {
        let s = shard_schema(TENANT, DB, GroupId(2), r.shard);
        let mut c = pool_of(&r.cluster).get_conn().await.unwrap();
        let rows: Vec<(Vec<u8>, u64)> = c
            .query(format!(
                "SELECT WEIGHT_STRING(email LEVEL 1), {RK_COLUMN} FROM `{s}`.people"
            ))
            .await
            .unwrap();
        for (w, rk) in rows {
            let mut body = Vec::new();
            ci.canonical_from_levels(&[&w], &mut body);
            let mut framed = vec![0x10];
            framed.extend_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
            framed.extend_from_slice(&body);
            assert_eq!(routing_hash(&framed).0, rk);
            checked += 1;
        }
    }
    assert_eq!(checked, 100);
    let _ = Charset::Utf8mb4;

    // A map change (a move of shard 2's range) reaches the cache.
    let (mut cur, rev) = store.load(TENANT, GroupId(1)).await.unwrap();
    cur.ranges[1].epoch += 1;
    cur.version += 1;
    store.compare_and_put(rev, &cur).await.unwrap().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        cache.wait_version(TENANT, GroupId(1), 2),
    )
    .await
    .unwrap();
    assert_eq!(cache.get(TENANT, GroupId(1)).unwrap().ranges[1].epoch, 2);

    // Clean up by exact name.
    for (cl, s) in &schemas {
        let mut c = pool_of(cl).get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
    }
    for g in [GroupId(1), GroupId(2)] {
        store.delete(TENANT, g).await.unwrap();
    }
    for (_, p) in pools {
        p.disconnect().await.unwrap();
    }
    println!(
        "routed 200 customers ({per_shard:?}), 600 orders, 100 emails with {n} variant lookups; \
         {checked} stored hashes re-derived from MariaDB weights"
    );
}

/// The `sharding` catalog module on a real catalog schema: migrate twice
/// (idempotent), write a group and members, read them back.
#[tokio::test(flavor = "multi_thread")]
async fn live_catalog_module() {
    use sprig_common::ids::ObjectId;
    use sprig_shard::groups::{GroupMember, GroupState, GroupStore, RkState, TableGroup, migrate};
    use sprig_shard::{GroupKind, MemberRouting};
    let Some(single) = env("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE not set");
        return;
    };
    let schema = "t7310005_sprig"; // the test's catalog, dropped by name below
    let pool = mysql_async::Pool::new(single.as_str());
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE `{schema}`"))
        .await
        .unwrap();
    c.query_drop(format!("USE `{schema}`")).await.unwrap();
    // The migrator needs the core module's tables first.
    sprig_catalog::Migrator::new(&[])
        .unwrap()
        .run(
            &mut c,
            schema,
            &sprig_catalog::migrate::MigrateOpts::default(),
        )
        .await
        .unwrap();
    migrate(&mut c, schema).await.unwrap();
    migrate(&mut c, schema).await.unwrap();
    let spec = KeySpec::new(vec![
        KeyClass::Numeric,
        KeyClass::String {
            collation: collation_by_name("utf8mb4_0900_ai_ci").unwrap().id,
        },
    ])
    .unwrap();
    let g = TableGroup {
        id: GroupId(3),
        kind: GroupKind::Sharded,
        name: "customers".into(),
        key: Some(spec),
        state: GroupState::Active,
        min_shards: Some(4),
    };
    let st = GroupStore;
    st.create_group(&mut c, &g).await.unwrap();
    let members = [
        GroupMember {
            object: ObjectId(10),
            group: GroupId(3),
            db: DB,
            routing: MemberRouting::Columns(vec!["id".into(), "region".into()]),
            rk_state: RkState::Ready,
            off_class_seen: false,
        },
        GroupMember {
            object: ObjectId(11),
            group: GroupId(3),
            db: DB,
            routing: MemberRouting::Parent {
                parent: ObjectId(10),
                fk: "fk_orders_customer".into(),
            },
            rk_state: RkState::Filling,
            off_class_seen: false,
        },
    ];
    for m in &members {
        st.add_member(&mut c, m).await.unwrap();
    }
    st.mark_off_class(&mut c, ObjectId(10)).await.unwrap();
    assert_eq!(st.groups(&mut c).await.unwrap(), vec![g]);
    let got = st.members(&mut c, GroupId(3)).await.unwrap();
    assert!(got[0].off_class_seen);
    assert_eq!(got[1], members[1]);
    for t in [
        "table_groups",
        "group_members",
        "reference_tables",
        "sequences",
        "lookup_indexes",
        "foreign_keys",
    ] {
        let n: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{schema}' \
                 AND TABLE_NAME = '{t}'"
            ))
            .await
            .unwrap();
        assert_eq!(n, Some(1), "{t}");
    }
    c.query_drop(format!("DROP DATABASE `{schema}`"))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
}
