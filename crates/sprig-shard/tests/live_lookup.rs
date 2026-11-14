//! Unique lookups under concurrent inserts and deletes, live: the
//! consistent-lookup algorithm.
//!
//! A tenant with `customers` sharded by `id` over two shards and a unique
//! key on `email` (`utf8mb4_uca1400_ai_ci`) that doesn't include the key,
//! so uniqueness across shards comes from a lookup in a two-shard lookup
//! group. The lookup is built with `direct::build` over an empty owner,
//! seeded with orphans (lookup rows whose owner never committed), then two
//! "gateways" with 12 workers each insert and delete customers whose emails
//! collide on purpose (case and trailing-space variants of a small pool),
//! and some transactions crash between the Pre and Main commits.
//!
//! Afterwards: no two customers share an email under the collation; every
//! customer has a lookup row pointing at it; routing through the lookup
//! finds every customer; the orphan scan leaves exactly one lookup row per
//! customer.
//!
//! Runs on the single node, and (with `SPRIG_TEST_MARIADB_GALERA`) with the
//! lookup shards and one owner shard on Galera, each shard written through
//! one node (its writer) and the two lookup
//! shards through different nodes. Writing one shard through two nodes at
//! once is not safe: the owner check's share lock is local to its node, and
//! the test then finds duplicate owners.

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_shard::collation::{Charset, collation_by_name};
use sprig_shard::key::{KeyClass, KeySpec, KeyValue, KeyValueOwned};
use sprig_shard::lookup::direct::{self, DirectReader, DirectTxn, pool};
use sprig_shard::lookup::{
    self, LookupCatalog, LookupColumn, LookupId, LookupIndex, LookupOrigin, LookupPlacement,
    LookupState, MapPlacement, OwnerRef, errno, fq, text,
};
use sprig_shard::{ShardMap, hash_key};

struct Setup {
    tenant: TenantId,
    placement: MapPlacement,
    /// One pool map per gateway.
    gateways: Vec<direct::Pools>,
    schemas: Vec<(ClusterId, String)>,
    catalog: (ClusterId, String),
}

fn lookup_def() -> LookupIndex {
    LookupIndex {
        id: LookupId(1),
        owner: ObjectId(11),
        owner_table: "customers".into(),
        index_name: "uk_email".into(),
        columns: vec![LookupColumn {
            name: "email".into(),
            sql_type: "VARCHAR(100) COLLATE utf8mb4_uca1400_ai_ci".into(),
            class: KeyClass::String {
                collation: collation_by_name("utf8mb4_uca1400_ai_ci").unwrap().id,
            },
        }],
        owner_pk: vec![LookupColumn {
            name: "id".into(),
            sql_type: "BIGINT".into(),
            class: KeyClass::Numeric,
        }],
        unique: true,
        origin: LookupOrigin::UniqueKey,
        group: GroupId(9),
        state: LookupState::Building,
        auto_index_id: None,
    }
}

fn ids_spec() -> KeySpec {
    KeySpec::new(vec![KeyClass::Numeric]).unwrap()
}

async fn setup(
    tenant: TenantId,
    owners: [&ClusterId; 2],
    lookups: [&ClusterId; 2],
    gateways: Vec<direct::Pools>,
) -> Setup {
    let owner_map = ShardMap::presplit(
        tenant,
        GroupId(1),
        ids_spec().digest,
        2,
        &[owners[0].clone(), owners[1].clone()],
    )
    .unwrap();
    let l = lookup_def();
    let lookup_map = ShardMap::presplit(
        tenant,
        GroupId(9),
        l.spec().unwrap().digest,
        2,
        &[lookups[0].clone(), lookups[1].clone()],
    )
    .unwrap();
    let placement = MapPlacement {
        tenant,
        db: DbId(1),
        lookup_map: Arc::new(lookup_map),
        owner_map: Arc::new(owner_map),
    };
    let pools = &gateways[0];
    let mut schemas = Vec::new();
    for t in placement.owner_shards() {
        let mut c = pools[&t.cluster].get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", t.schema))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{}`", t.schema))
            .await
            .unwrap();
        c.query_drop(format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, email VARCHAR(100) COLLATE \
             utf8mb4_uca1400_ai_ci NOT NULL, _sprig_rk BIGINT UNSIGNED NOT NULL DEFAULT 0 INVISIBLE, \
             UNIQUE KEY uk_email (email)) ENGINE=InnoDB",
            fq(&t, "customers")
        ))
        .await
        .unwrap();
        schemas.push((t.cluster.clone(), t.schema.clone()));
    }
    for t in placement.lookup_shards() {
        let mut c = pools[&t.cluster].get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", t.schema))
            .await
            .unwrap();
        schemas.push((t.cluster.clone(), t.schema.clone()));
    }
    let catalog = (owners[0].clone(), format!("t{tenant}_sprig"));
    let mut c = pools[&catalog.0].get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", catalog.1))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE `{}`", catalog.1))
        .await
        .unwrap();
    c.query_drop(format!("USE `{}`", catalog.1)).await.unwrap();
    c.query_drop(lookup::DDL_LOOKUP_INDEXES).await.unwrap();
    Setup {
        tenant,
        placement,
        gateways,
        schemas,
        catalog,
    }
}

fn rk_of(id: i64) -> sprig_shard::RoutingHash {
    hash_key(&ids_spec(), &[KeyValue::Int(i128::from(id))])
        .unwrap()
        .0
}

#[derive(Default)]
struct Counts {
    inserted: AtomicU64,
    duplicate: AtomicU64,
    retryable: AtomicU64,
    deleted: AtomicU64,
    crashed: AtomicU64,
    reclaimed: AtomicU64,
}

#[allow(clippy::too_many_lines)]
async fn run(s: Setup, label: &str) {
    let pools0 = Arc::clone(&s.gateways[0]);
    let mut cat = pools0[&s.catalog.0].get_conn().await.unwrap();
    cat.query_drop(format!("USE `{}`", s.catalog.1))
        .await
        .unwrap();
    let mut l = lookup_def();
    LookupCatalog.insert(&mut cat, &l).await.unwrap();
    let report = direct::build(&pools0, &mut cat, &s.placement, &mut l, 100, || async {
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(l.state, LookupState::Public);
    assert_eq!(report.rows, 0);

    // Orphans: lookup rows of owners that never committed.
    let mut orphans = 0;
    for i in 0..20i64 {
        let mut t = DirectTxn::new(Arc::clone(&pools0), 5);
        let owner = OwnerRef {
            pk: vec![KeyValueOwned::Int(i128::from(1_000_000 + i))],
            rk: rk_of(1_000_000 + i),
        };
        lookup::claim(
            &mut t,
            &s.placement,
            &l,
            &[text(&format!("user{i}@x.com"))],
            &owner,
        )
        .await
        .unwrap();
        t.abort_after_pre().await;
        orphans += 1;
    }

    let next_id = Arc::new(AtomicI64::new(1));
    let counts = Arc::new(Counts::default());
    let l = Arc::new(l);
    let placement = Arc::new(s.placement.clone());
    let mut tasks = Vec::new();
    for (g, pools) in s.gateways.iter().enumerate() {
        for w in 0..12u64 {
            let (pools, l, placement, next_id, counts) = (
                Arc::clone(pools),
                Arc::clone(&l),
                Arc::clone(&placement),
                Arc::clone(&next_id),
                Arc::clone(&counts),
            );
            tasks.push(tokio::spawn(async move {
                let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 100 * g as u64 + 1);
                let mut rnd = move |n: u64| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed % n
                };
                let reader = DirectReader(Arc::clone(&pools));
                for _ in 0..150 {
                    let base = format!("user{}@x.com", rnd(40));
                    let email = match rnd(4) {
                        0 => base.to_uppercase(),
                        1 => format!("{base}  "),
                        2 => base.replacen('u', "U", 1),
                        _ => base,
                    };
                    if rnd(4) == 0 {
                        // Delete the owner of this email, if any.
                        let rks =
                            lookup::route(&reader, placement.as_ref(), &l, &[vec![text(&email)]])
                                .await
                                .unwrap();
                        for rk in rks {
                            let o = placement.owner_target(rk).unwrap();
                            let mut t = DirectTxn::new(Arc::clone(&pools), 5);
                            let r = async {
                                let rows = t
                                    .exec_main(
                                        &o,
                                        &format!(
                                            "SELECT id, email FROM {} WHERE email = ? FOR UPDATE",
                                            fq(&o, "customers")
                                        ),
                                        mysql_async::Params::Positional(vec![email.clone().into()]),
                                    )
                                    .await?;
                                let Some(row) = rows.into_iter().next() else {
                                    return Ok(false);
                                };
                                let (id, stored): (i64, String) = mysql_async::from_row(row);
                                t.exec_main(
                                    &o,
                                    &format!("DELETE FROM {} WHERE id = ?", fq(&o, "customers")),
                                    mysql_async::Params::Positional(vec![id.into()]),
                                )
                                .await?;
                                lookup::release(
                                    &mut t,
                                    placement.as_ref(),
                                    &l,
                                    &[text(&stored)],
                                    &OwnerRef {
                                        pk: vec![KeyValueOwned::Int(i128::from(id))],
                                        rk: rk_of(id),
                                    },
                                )?;
                                Ok::<_, sprig_common::SprigError>(true)
                            }
                            .await;
                            match r {
                                Ok(true) => match t.commit().await {
                                    Ok(_) => {
                                        counts.deleted.fetch_add(1, Ordering::Relaxed);
                                    }
                                    // Galera certification at commit.
                                    Err(e) if e.retryable => {
                                        counts.retryable.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(e) => panic!("delete commit: {e}"),
                                },
                                Ok(false) => t.rollback().await,
                                Err(e) if e.retryable => {
                                    t.rollback().await;
                                    counts.retryable.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => panic!("delete: {e}"),
                            }
                        }
                        continue;
                    }
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let owner = OwnerRef {
                        pk: vec![KeyValueOwned::Int(i128::from(id))],
                        rk: rk_of(id),
                    };
                    let o = placement.owner_target(owner.rk).unwrap();
                    let mut t = DirectTxn::new(Arc::clone(&pools), 5);
                    let r = async {
                        let c =
                            lookup::claim(&mut t, placement.as_ref(), &l, &[text(&email)], &owner)
                                .await?;
                        if c == lookup::ClaimResult::Reclaimed {
                            counts.reclaimed.fetch_add(1, Ordering::Relaxed);
                        }
                        t.exec_main(
                            &o,
                            &format!(
                                "INSERT INTO {} (id, email, _sprig_rk) VALUES (?, ?, ?)",
                                fq(&o, "customers")
                            ),
                            mysql_async::Params::Positional(vec![
                                id.into(),
                                email.clone().into(),
                                owner.rk.0.into(),
                            ]),
                        )
                        .await?;
                        Ok::<_, sprig_common::SprigError>(())
                    }
                    .await;
                    match r {
                        Ok(()) if rnd(20) == 0 => {
                            t.abort_after_pre().await;
                            counts.crashed.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(()) => match t.commit().await {
                            Ok(_) => {
                                counts.inserted.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) if e.retryable => {
                                counts.retryable.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => panic!("commit: {e}"),
                        },
                        Err(e) if errno(&e) == Some(1062) => {
                            t.rollback().await;
                            counts.duplicate.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) if e.retryable => {
                            t.rollback().await;
                            counts.retryable.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("insert: {e}"),
                    }
                }
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Late crashes: orphans nobody reclaims, for the scan to find.
    for i in 0..5i64 {
        let mut t = DirectTxn::new(Arc::clone(&pools0), 5);
        let owner = OwnerRef {
            pk: vec![KeyValueOwned::Int(i128::from(2_000_000 + i))],
            rk: rk_of(2_000_000 + i),
        };
        lookup::claim(
            &mut t,
            &s.placement,
            &l,
            &[text(&format!("late{i}@x.com"))],
            &owner,
        )
        .await
        .unwrap();
        t.abort_after_pre().await;
    }

    // Invariants.
    let ci = collation_by_name("utf8mb4_uca1400_ai_ci").unwrap();
    let mut customers: Vec<(i64, String)> = Vec::new();
    for t in s.placement.owner_shards() {
        let mut c = pools0[&t.cluster].get_conn().await.unwrap();
        let rows: Vec<(i64, String, u64)> = c
            .query(format!(
                "SELECT id, email, _sprig_rk FROM {}",
                fq(&t, "customers")
            ))
            .await
            .unwrap();
        for (id, e, rk) in rows {
            assert_eq!(rk, rk_of(id).0);
            assert_eq!(s.placement.owner_target(rk_of(id)).unwrap(), t);
            customers.push((id, e));
        }
    }
    let mut seen = HashSet::new();
    for (_, e) in &customers {
        let mut k = Vec::new();
        ci.hash_key(e.as_bytes(), Charset::Utf8mb4, &mut k).unwrap();
        assert!(seen.insert(k), "{label}: email {e:?} has two owners");
    }
    let reader = DirectReader(Arc::clone(&pools0));
    for (id, e) in &customers {
        let t = s
            .placement
            .lookup_target(lookup::value_hash(&l, &[text(e)]).unwrap())
            .unwrap();
        let mut c = pools0[&t.cluster].get_conn().await.unwrap();
        let row: Option<(i64, u64)> = c
            .exec_first(
                format!(
                    "SELECT owner_id, owner_rk FROM {} WHERE email = ?",
                    fq(&t, &l.physical())
                ),
                (e,),
            )
            .await
            .unwrap();
        assert_eq!(
            row,
            Some((*id, rk_of(*id).0)),
            "{label}: lookup row of {e:?}"
        );
        let rks = lookup::route(&reader, &s.placement, &l, &[vec![text(&e.to_uppercase())]])
            .await
            .unwrap();
        assert!(rks.contains(&rk_of(*id)));
    }
    let lookup_rows = |pools: direct::Pools| {
        let p = s.placement.clone();
        let phys = l.physical();
        async move {
            let mut n = 0u64;
            for t in p.lookup_shards() {
                let mut c = pools[&t.cluster].get_conn().await.unwrap();
                let k: u64 = c
                    .query_first(format!("SELECT COUNT(*) FROM {}", fq(&t, &phys)))
                    .await
                    .unwrap()
                    .unwrap();
                n += k;
            }
            n
        }
    };
    let before = lookup_rows(Arc::clone(&pools0)).await;
    let removed = direct::orphan_scan(&pools0, &s.placement, &l, 5)
        .await
        .unwrap();
    let after = lookup_rows(Arc::clone(&pools0)).await;
    assert_eq!(
        after,
        customers.len() as u64,
        "{label}: one lookup row per customer"
    );
    assert_eq!(before - removed, after);
    assert!(removed >= 5, "{label}: the scan removes the late orphans");
    let by_email: HashMap<_, _> = customers.iter().map(|(i, e)| (e.clone(), *i)).collect();
    println!(
        "{label}: {} customers; inserted {} deleted {} duplicate(1062) {} retryable {} \
         crashed-after-Pre {} reclaimed-orphans {}; {orphans} seeded orphans; scan removed \
         {removed} ({before} -> {after} lookup rows); {} distinct emails",
        customers.len(),
        counts.inserted.load(Ordering::Relaxed),
        counts.deleted.load(Ordering::Relaxed),
        counts.duplicate.load(Ordering::Relaxed),
        counts.retryable.load(Ordering::Relaxed),
        counts.crashed.load(Ordering::Relaxed),
        counts.reclaimed.load(Ordering::Relaxed),
        by_email.len()
    );

    // Clean up by exact name.
    for (cl, schema) in s.schemas.iter().chain(std::iter::once(&s.catalog)) {
        let mut c = pools0[cl].get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            .await
            .unwrap();
    }
    drop(cat);
    let _ = s.tenant;
    for g in s.gateways {
        for p in g.values() {
            p.clone().disconnect().await.unwrap();
        }
    }
}

fn pools(entries: &[(&ClusterId, &str)]) -> direct::Pools {
    Arc::new(
        entries
            .iter()
            .map(|(c, u)| ((*c).clone(), pool(u, 32).unwrap()))
            .collect::<HashMap<ClusterId, Pool>>(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn live_unique_lookup_concurrent() {
    let Ok(single) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE not set");
        return;
    };
    let c1 = ClusterId::new("r1shard-c1").unwrap();
    let gws = vec![pools(&[(&c1, &single)]), pools(&[(&c1, &single)])];
    let s = setup(TenantId(7_310_002), [&c1, &c1], [&c1, &c1], gws).await;
    run(s, "single node").await;

    if let Ok(g) = std::env::var("SPRIG_TEST_MARIADB_GALERA") {
        let nodes: Vec<&str> = g.split(',').map(str::trim).collect();
        if nodes.len() >= 2 {
            // One Galera cluster; each shard has one writer node, the same
            // for every gateway. The cluster
            // ids name the shard's writer: lookup shard 1 and owner shard 2
            // write through node 1, lookup shard 2 through node 2.
            let ga = ClusterId::new("r1shard-galera-n1").unwrap();
            let gb = ClusterId::new("r1shard-galera-n2").unwrap();
            let gws = vec![
                pools(&[(&c1, &single), (&ga, nodes[0]), (&gb, nodes[1])]),
                pools(&[(&c1, &single), (&ga, nodes[0]), (&gb, nodes[1])]),
            ];
            let s = setup(TenantId(7_310_003), [&c1, &ga], [&ga, &gb], gws).await;
            run(
                s,
                "galera (one writer node per shard; lookup shards on nodes 1 and 2)",
            )
            .await;
        }
    }
}

/// Build over existing rows: a real duplicate across shards fails the
/// build with 1062 and drops the lookup; without it the back-fill makes it
/// public. An automatic (non-unique) lookup from automatic indexing's hook
/// builds the same way and routes to every owner of a value.
#[tokio::test(flavor = "multi_thread")]
async fn live_lookup_build_backfill_and_auto() {
    let Ok(single) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE not set");
        return;
    };
    let c1 = ClusterId::new("r1shard-c1").unwrap();
    let gws = vec![pools(&[(&c1, &single)])];
    let s = setup(TenantId(7_310_004), [&c1, &c1], [&c1, &c1], gws).await;
    let p = Arc::clone(&s.gateways[0]);
    let mut cat = p[&c1].get_conn().await.unwrap();
    cat.query_drop(format!("USE `{}`", s.catalog.1))
        .await
        .unwrap();
    // 300 owners; ids 1 and the first id on the other shard share an email
    // under the collation.
    let first = s.placement.owner_target(rk_of(1)).unwrap();
    let other = (2..300)
        .find(|i| s.placement.owner_target(rk_of(*i)).unwrap() != first)
        .unwrap();
    for id in 1..=300i64 {
        let email = if id == other {
            "PERSON1@x.com  ".to_owned()
        } else {
            format!("person{id}@x.com")
        };
        let t = s.placement.owner_target(rk_of(id)).unwrap();
        let mut c = p[&t.cluster].get_conn().await.unwrap();
        c.exec_drop(
            format!(
                "INSERT INTO {} (id, email, _sprig_rk) VALUES (?, ?, ?)",
                fq(&t, "customers")
            ),
            (id, email, rk_of(id).0),
        )
        .await
        .unwrap();
    }
    let mut l = lookup_def();
    LookupCatalog.insert(&mut cat, &l).await.unwrap();
    let err = direct::build(&p, &mut cat, &s.placement, &mut l, 64, || async { Ok(()) })
        .await
        .unwrap_err();
    assert_eq!(errno(&err), Some(1062), "{err}");
    let left: Option<u64> = cat
        .query_first(format!(
            "SELECT COUNT(*) FROM lookup_indexes WHERE lookup_id = {}",
            l.id.0
        ))
        .await
        .unwrap();
    assert_eq!(left, Some(0), "the failed lookup is dropped");
    // Remove the duplicate and build again.
    let t = s.placement.owner_target(rk_of(other)).unwrap();
    let mut c = p[&t.cluster].get_conn().await.unwrap();
    c.exec_drop(
        format!("DELETE FROM {} WHERE id = ?", fq(&t, "customers")),
        (other,),
    )
    .await
    .unwrap();
    let mut l = lookup_def();
    LookupCatalog.insert(&mut cat, &l).await.unwrap();
    let rep = direct::build(&p, &mut cat, &s.placement, &mut l, 64, || async { Ok(()) })
        .await
        .unwrap();
    assert_eq!((rep.rows, rep.inserted, rep.skipped), (299, 299, 0));
    assert_eq!(l.state, LookupState::Public);
    let reader = DirectReader(Arc::clone(&p));
    let rks = lookup::route(&reader, &s.placement, &l, &[vec![text("PERSON7@X.COM")]])
        .await
        .unwrap();
    assert_eq!(rks, vec![rk_of(7)]);

    // The automatic-lookup hook: a non-unique lookup on `status`.
    for t in s.placement.owner_shards() {
        let mut c = p[&t.cluster].get_conn().await.unwrap();
        c.query_drop(format!(
            "ALTER TABLE {} ADD COLUMN status VARCHAR(10) NOT NULL DEFAULT 'new'",
            fq(&t, "customers")
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "UPDATE {} SET status = IF(id % 50 = 0, 'vip', 'new')",
            fq(&t, "customers")
        ))
        .await
        .unwrap();
    }
    let mut auto = lookup::propose_auto_lookup(
        LookupId(2),
        ObjectId(11),
        "customers",
        vec![LookupColumn {
            name: "status".into(),
            sql_type: "VARCHAR(10)".into(),
            class: KeyClass::String {
                collation: collation_by_name("utf8mb4_uca1400_ai_ci").unwrap().id,
            },
        }],
        lookup_def().owner_pk,
        GroupId(9),
        Some(77),
    );
    LookupCatalog.insert(&mut cat, &auto).await.unwrap();
    let rep = direct::build(&p, &mut cat, &s.placement, &mut auto, 64, || async {
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(rep.inserted, 299);
    let rks = lookup::route(&reader, &s.placement, &auto, &[vec![text("VIP")]])
        .await
        .unwrap();
    let want: std::collections::BTreeSet<_> = (1..=300)
        .filter(|i| i % 50 == 0 && *i != other)
        .map(rk_of)
        .collect();
    assert_eq!(
        rks.into_iter().collect::<std::collections::BTreeSet<_>>(),
        want
    );
    println!(
        "build: duplicate refused with 1062; 299 owners back-filled; auto lookup {} routes 'VIP' to {} owners",
        auto.index_name,
        want.len()
    );
    for (cl, schema) in s.schemas.iter().chain(std::iter::once(&s.catalog)) {
        let mut c = p[cl].get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            .await
            .unwrap();
    }
}
