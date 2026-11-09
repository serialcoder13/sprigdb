//! The R1 differential suite (chapter 46, "Differential testing"; chapter
//! 17, "Testing"): the same statements against a sharded tenant through
//! the gateway and against one unsharded MariaDB schema, compared exactly.
//! A statement the router can't plan exactly must fail with 39420
//! (`ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`); it is counted as skipped,
//! never as a pass. Any other difference fails the test.
//!
//! The layout is made the way chapters 15, 20 and 21 make it, and the
//! gateway reads it from the catalog and the shard maps:
//!
//! - the tables are created through the gateway (catalog objects, the
//!   database's template schema);
//! - table groups, members and reference copies go into the catalog's
//!   `sharding` module, shard maps into etcd (`ShardMapStore`), shard
//!   schemas onto the clusters; a DDL through the gateway then moves the
//!   catalog version, so the gateway reloads;
//! - group 901 (`customers`, `orders`, integer key) has 5 uneven ranges over
//!   both clusters, group 902 (`products`) 2 shards on the second cluster,
//!   group 903 (`tags`, `utf8mb4_general_ci` key) 3 shards over both;
//!   `countries` is a reference table with a copy on each cluster.
//!
//! Phases: seed through the gateway, the read corpus, prepared statements
//! (binary protocol), DML with full-table checks (writes across clusters
//! commit through chapter 18's coordinator), transactions spanning clusters
//! (committed, rolled back, with savepoints), a split of one range onto the
//! other cluster while point reads run (the map changes mid-run, under a
//! hold and a rename fence), the corpus again on the new layout, and a
//! branch killed inside a transaction.
//!
//! ```sh
//! export SPRIG_TEST_R1_GATEWAY=127.0.0.1:36307          # the tenant's gateway endpoint
//! export SPRIG_TEST_R1_USER=r1finish.app SPRIG_TEST_R1_PASSWORD=sprig-dev-app
//! export SPRIG_TEST_R1_TENANT_ID=<the tenant's id> SPRIG_TEST_R1_DB=r1f_db
//! export SPRIG_TEST_R1_CLUSTERS=dc-a-c1=127.0.0.1:13301,dc-a-c2=127.0.0.1:13311
//! export SPRIG_TEST_ETCD=http://127.0.0.1:2379 SPRIG_TEST_R1_DC=dc-a
//! cargo test -p sprig-router --test differential_live -- --nocapture --test-threads 1
//! ```
//!
//! The first cluster holds the tenant's catalog and the reference database.
//! Without `SPRIG_TEST_R1_GATEWAY` the test reports what it needs and
//! passes.

#![allow(
    clippy::many_single_char_names,
    clippy::ref_option,
    clippy::items_after_statements,
    clippy::cast_possible_wrap,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::panic,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    missing_docs
)]

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Params, Row, SslOpts, Value};
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, ObjectId, ShardId, TenantId};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_shard::groups::{GroupMember, GroupState, GroupStore, RkState, TableGroup};
use sprig_shard::shardmap::RangeState;
use sprig_shard::{
    GroupKind, KeyClass, KeySpec, KeyValue, MemberRouting, RangeEntry, ShardMap, ShardMapStore,
    shard_schema,
};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

struct Env {
    gateway: String,
    user: String,
    password: String,
    tenant: TenantId,
    db: String,
    /// `(cluster id, root endpoint)`; the first holds the catalog.
    clusters: Vec<(ClusterId, String)>,
    root_password: String,
    etcd: String,
    dc: DcId,
}

impl Env {
    fn load() -> Option<Self> {
        let clusters = env("SPRIG_TEST_R1_CLUSTERS")?
            .split(',')
            .filter_map(|p| {
                let (c, a) = p.split_once('=')?;
                Some((ClusterId::new(c.trim()).ok()?, a.trim().to_owned()))
            })
            .collect();
        Some(Self {
            gateway: env("SPRIG_TEST_R1_GATEWAY")?,
            user: env("SPRIG_TEST_R1_USER").unwrap_or_else(|| "r1finish.app".into()),
            password: env("SPRIG_TEST_R1_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into()),
            tenant: TenantId(env("SPRIG_TEST_R1_TENANT_ID")?.parse().ok()?),
            db: env("SPRIG_TEST_R1_DB").unwrap_or_else(|| "r1f_db".into()),
            clusters,
            root_password: env("SPRIG_TEST_R1_ROOT_PASSWORD")
                .unwrap_or_else(|| "sprig-dev-root".into()),
            etcd: env("SPRIG_TEST_ETCD")?,
            dc: DcId::new(env("SPRIG_TEST_R1_DC").unwrap_or_else(|| "dc-a".into())).ok()?,
        })
    }
    fn catalog(&self) -> String {
        format!("t{}_sprig", self.tenant)
    }
    fn reference_db(&self) -> String {
        format!("r1finish_diff_{}", self.tenant)
    }
    fn cluster(&self, i: usize) -> &(ClusterId, String) {
        &self.clusters[i % self.clusters.len()]
    }
}

async fn connect(addr: &str, user: &str, pass: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pass))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {user}@{addr}: {e}"))
}

/// One shard: its id, its range start and its cluster (index).
#[derive(Debug, Clone, Copy)]
struct Sh {
    id: u32,
    lo: u64,
    cluster: usize,
}

/// A group of the test layout.
struct Grp {
    id: u32,
    spec: KeySpec,
    tables: &'static [(&'static str, &'static str)],
    shards: Vec<Sh>,
}

fn layout() -> Vec<Grp> {
    let int = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    let ci = sprig_shard::collation::collation_by_name("utf8mb4_general_ci").unwrap();
    let string = KeySpec::new(vec![KeyClass::String { collation: ci.id }]).unwrap();
    let even = |n: u32, clusters: &[usize]| -> Vec<Sh> {
        (0..n)
            .map(|i| Sh {
                id: i + 1,
                lo: u64::try_from((u128::from(i) << 64) / u128::from(n)).unwrap(),
                cluster: clusters[i as usize],
            })
            .collect()
    };
    vec![
        Grp {
            id: 901,
            spec: int.clone(),
            tables: &[("customers", "cid"), ("orders", "cid"), ("accounts", "aid")],
            // Uneven ranges over both clusters.
            shards: [
                (1, 0u64, 0),
                (2, 1 << 62, 1),
                (3, 1 << 63, 0),
                (4, (1 << 63) + (1 << 61), 1),
                (5, 3 << 62, 1),
            ]
            .into_iter()
            .map(|(id, lo, cluster)| Sh { id, lo, cluster })
            .collect(),
        },
        Grp {
            id: 902,
            spec: int,
            tables: &[("products", "pid")],
            shards: even(2, &[1, 1]),
        },
        Grp {
            id: 903,
            spec: string,
            tables: &[("tags", "tag")],
            shards: even(3, &[0, 1, 0]),
        },
    ]
}

const REF_GROUP: u32 = 904;
/// The tenant's lookup group (chapter 19): `accounts.email`'s unique
/// lookup, two shards, one per cluster.
const LOOKUP_GROUP: u32 = 905;
const LOOKUP_ID: u64 = 9051;

/// `accounts.email`'s unique lookup, as chapter 19 records it.
fn email_lookup(owner: ObjectId) -> sprig_shard::lookup::LookupIndex {
    use sprig_shard::lookup::{LookupColumn, LookupId, LookupIndex, LookupOrigin, LookupState};
    let ci = sprig_shard::collation::collation_by_name("utf8mb4_general_ci").unwrap();
    LookupIndex {
        id: LookupId(LOOKUP_ID),
        owner,
        owner_table: "accounts".into(),
        index_name: "email".into(),
        columns: vec![LookupColumn {
            name: "email".into(),
            sql_type: "varchar(40) COLLATE utf8mb4_general_ci".into(),
            class: KeyClass::String { collation: ci.id },
        }],
        owner_pk: vec![LookupColumn {
            name: "aid".into(),
            sql_type: "int(11)".into(),
            class: KeyClass::Numeric,
        }],
        unique: true,
        origin: LookupOrigin::UniqueKey,
        group: GroupId(LOOKUP_GROUP),
        state: LookupState::Public,
        auto_index_id: None,
    }
}

fn lookup_spec() -> KeySpec {
    let ci = sprig_shard::collation::collation_by_name("utf8mb4_general_ci").unwrap();
    KeySpec::new(vec![KeyClass::String { collation: ci.id }]).unwrap()
}

fn ddl_of(table: &str) -> &'static str {
    match table {
        "customers" => DDL[0],
        "orders" => DDL[1],
        "products" => DDL[2],
        "tags" => TAG_DDL,
        "accounts" => ACCOUNTS_DDL,
        _ => REF_DDL,
    }
}

fn map_of(e: &Env, g: &Grp, version: u64, shards: &[Sh], next: u32) -> ShardMap {
    ShardMap {
        tenant: e.tenant,
        group: GroupId(g.id),
        version,
        tenant_epoch: 0,
        key_spec_digest: g.spec.digest,
        next_shard_id: next,
        ranges: shards
            .iter()
            .map(|s| RangeEntry {
                lo: s.lo,
                shard: ShardId(s.id),
                cluster: e.cluster(s.cluster).0.clone(),
                epoch: 1,
                state: RangeState::Serving,
                job_id: None,
                hold_until_ms: None,
            })
            .collect(),
    }
}

/// What the suite set up, for its checks and teardown.
struct World {
    db_id: DbId,
    default_schema: String,
    store: ShardMapStore,
    /// `(cluster index, schema)` of every schema the suite created.
    schemas: Vec<(usize, String)>,
}

async fn root(e: &Env, i: usize, db: Option<&str>) -> Conn {
    connect(&e.cluster(i).1, "root", &e.root_password, db).await
}

/// The database's id and template schema, from the catalog.
async fn database(e: &Env) -> (DbId, String) {
    let mut c = root(e, 0, None).await;
    let row: Option<(u32, u32, Option<String>)> = c
        .exec_first(
            format!(
                "SELECT db_id, default_group_id, CAST(JSON_EXTRACT(options, '$.shards[0]') AS CHAR) \
                 FROM `{}`.databases WHERE name = ? AND state = 'active'",
                e.catalog()
            ),
            (e.db.as_str(),),
        )
        .await
        .unwrap();
    let (db, g, s) = row.unwrap_or_else(|| panic!("no database {} in the catalog", e.db));
    let shard: u32 = s.unwrap_or_else(|| "1".into()).parse().unwrap();
    (
        DbId(db),
        shard_schema(e.tenant, DbId(db), GroupId(g), ShardId(shard)),
    )
}

/// Moves the catalog version (a DDL through the gateway), so gateways load
/// the `sharding` rows the suite wrote behind their back.
async fn bump(sut: &mut Conn) {
    sut.query_drop("CREATE TABLE IF NOT EXISTS r1f_bump (x INT)")
        .await
        .unwrap();
    sut.query_drop("DROP TABLE r1f_bump").await.unwrap();
}

async fn clear_layout(e: &Env, store: &ShardMapStore) {
    for g in [901, 902, 903, LOOKUP_GROUP] {
        store.delete(e.tenant, GroupId(g)).await.unwrap();
    }
    let mut c = root(e, 0, Some(&e.catalog())).await;
    // The suite's own rows, by exact group and lookup id.
    for sql in [
        "DELETE m FROM group_members m WHERE m.group_id IN (901, 902, 903, 904)",
        "DELETE FROM table_groups WHERE group_id IN (901, 902, 903, 904, 905)",
        "DELETE FROM lookup_indexes WHERE lookup_id = 9051",
    ] {
        match c.query_drop(sql).await {
            Ok(()) | Err(mysql_async::Error::Server(_)) => {}
            Err(e) => panic!("{e}"),
        }
    }
    let _ = c
        .query_drop(
            "DELETE r FROM reference_tables r JOIN objects o ON o.object_id = r.object_id \
             WHERE o.name = 'countries'",
        )
        .await;
}

async fn setup(e: &Env, sut: &mut Conn) -> World {
    let meta: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(
            Tier::Dc(e.dc.clone()),
            &EtcdConfig::new(vec![e.etcd.clone()]),
        )
        .await
        .unwrap(),
    );
    let store = ShardMapStore::new(meta);
    clear_layout(e, &store).await;
    bump(sut).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // The tables, through the gateway: catalog objects and the template.
    for t in TABLES {
        sut.query_drop(format!("DROP TABLE IF EXISTS {t}"))
            .await
            .unwrap();
    }
    for t in TABLES {
        sut.query_drop(ddl_of(t)).await.unwrap();
    }
    let (db_id, default_schema) = database(e).await;
    // Chapter 18's decision table came with the database's schema
    // (`CREATE DATABASE` through the gateway), not from the suite.
    {
        let mut c = root(e, 0, None).await;
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = '__sprig_dtx'",
                (default_schema.as_str(),),
            )
            .await
            .unwrap();
        assert_eq!(
            n,
            Some(1),
            "CREATE DATABASE {} didn't create __sprig_dtx in {default_schema}",
            e.db
        );
    }
    let mut schemas: Vec<(usize, String)> = Vec::new();
    // Shard schemas, reference copies, decision tables.
    let groups = layout();
    for g in &groups {
        for s in &g.shards {
            let name = shard_schema(e.tenant, db_id, GroupId(g.id), ShardId(s.id));
            schemas.push((s.cluster % e.clusters.len(), name));
        }
    }
    let ref_schema = sprig_shard::reference_schema(e.tenant, db_id);
    for i in 0..e.clusters.len() {
        schemas.push((i, ref_schema.clone()));
    }
    for (i, s) in &schemas {
        let mut c = root(e, *i, None).await;
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
        // A shard schema anchors decisions too: the advisor and the mover
        // create the table with it (`sprig_catalog::ddl::dtx_table_sql`).
        c.query_drop(sprig_catalog::ddl::dtx_table_sql(s))
            .await
            .unwrap();
        c.query_drop(format!("USE `{s}`")).await.unwrap();
        let tables: Vec<&str> = if *s == ref_schema {
            vec!["countries"]
        } else {
            let g = groups
                .iter()
                .find(|g| s.contains(&format!("_g{}_", g.id)))
                .unwrap();
            g.tables.iter().map(|(t, _)| *t).collect()
        };
        for t in tables {
            c.query_drop(ddl_of(t)).await.unwrap();
        }
        if *s == ref_schema {
            c.query_drop(COUNTRIES).await.unwrap();
        }
    }
    {
        let mut c = root(e, 0, None).await;
        let r = e.reference_db();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{r}`"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{r}`"))
            .await
            .unwrap();
        c.query_drop(format!("USE `{r}`")).await.unwrap();
        for t in TABLES {
            c.query_drop(ddl_of(t)).await.unwrap();
        }
        c.query_drop(COUNTRIES).await.unwrap();
    }
    // Shard maps.
    for g in &groups {
        let next = g.shards.iter().map(|s| s.id).max().unwrap() + 1;
        store
            .create(&map_of(e, g, 1, &g.shards, next))
            .await
            .unwrap();
    }
    // The catalog's sharding module.
    let mut c = root(e, 0, Some(&e.catalog())).await;
    sprig_shard::groups::migrate(&mut c, &e.catalog())
        .await
        .unwrap();
    let st = GroupStore;
    let objects: Vec<(u64, String)> = c
        .exec(
            "SELECT object_id, name FROM objects WHERE db_id = ? AND state = 'active' \
             AND namespace = 'relation'",
            (db_id.0,),
        )
        .await
        .unwrap();
    let object = |n: &str| ObjectId(objects.iter().find(|(_, x)| x == n).unwrap().0);
    for g in &groups {
        st.create_group(
            &mut c,
            &TableGroup {
                id: GroupId(g.id),
                kind: GroupKind::Sharded,
                name: format!("r1f_g{}", g.id),
                key: Some(g.spec.clone()),
                state: GroupState::Active,
                min_shards: None,
            },
        )
        .await
        .unwrap();
        for (t, k) in g.tables {
            st.add_member(
                &mut c,
                &GroupMember {
                    object: object(t),
                    group: GroupId(g.id),
                    db: db_id,
                    routing: MemberRouting::Columns(vec![(*k).to_owned()]),
                    rk_state: RkState::Absent,
                    off_class_seen: false,
                },
            )
            .await
            .unwrap();
        }
    }
    st.create_group(
        &mut c,
        &TableGroup {
            id: GroupId(REF_GROUP),
            kind: GroupKind::Reference,
            name: "r1f_ref".into(),
            key: None,
            state: GroupState::Active,
            min_shards: None,
        },
    )
    .await
    .unwrap();
    st.add_member(
        &mut c,
        &GroupMember {
            object: object("countries"),
            group: GroupId(REF_GROUP),
            db: db_id,
            routing: MemberRouting::None,
            rk_state: RkState::Absent,
            off_class_seen: false,
        },
    )
    .await
    .unwrap();
    for (i, (cl, _)) in e.clusters.iter().enumerate() {
        c.exec_drop(
            "INSERT INTO reference_tables (object_id, cluster_id, role, state, origin, updated_at) \
             VALUES (?, ?, ?, 'serving', 'pinned', UTC_TIMESTAMP(6))",
            (
                object("countries").0,
                cl.as_str(),
                if i == 0 { "primary" } else { "copy" },
            ),
        )
        .await
        .unwrap();
    }
    // Chapter 19: accounts.email's unique lookup, in the tenant's lookup
    // group (two shards, one per cluster), public from the start (the
    // table is empty).
    let lk = email_lookup(object("accounts"));
    st.create_group(
        &mut c,
        &TableGroup {
            id: GroupId(LOOKUP_GROUP),
            kind: GroupKind::Lookup,
            name: "r1f_lookups".into(),
            key: Some(lookup_spec()),
            state: GroupState::Active,
            min_shards: None,
        },
    )
    .await
    .unwrap();
    sprig_shard::lookup::LookupCatalog
        .insert(&mut c, &lk)
        .await
        .unwrap();
    let lgrp = Grp {
        id: LOOKUP_GROUP,
        spec: lookup_spec(),
        tables: &[],
        shards: vec![
            Sh {
                id: 1,
                lo: 0,
                cluster: 0,
            },
            Sh {
                id: 2,
                lo: 1 << 63,
                cluster: 1,
            },
        ],
    };
    store
        .create(&map_of(e, &lgrp, 1, &lgrp.shards, 3))
        .await
        .unwrap();
    for sh in &lgrp.shards {
        let schema = shard_schema(e.tenant, db_id, GroupId(LOOKUP_GROUP), ShardId(sh.id));
        let i = sh.cluster % e.clusters.len();
        let mut lc = root(e, i, None).await;
        lc.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            .await
            .unwrap();
        lc.query_drop(format!("CREATE DATABASE `{schema}`"))
            .await
            .unwrap();
        lc.query_drop(sprig_catalog::ddl::dtx_table_sql(&schema))
            .await
            .unwrap();
        lc.query_drop(lk.create_table_sql(&schema)).await.unwrap();
        schemas.push((i, schema));
    }
    drop(c);
    bump(sut).await;
    // The gateway routes once it loaded the catalog and the maps.
    let started = Instant::now();
    loop {
        if let Outcome::Rows(..) = run(sut, "EXPLAIN FORMAT=SPRIG SELECT * FROM customers").await {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the gateway never routed the new layout"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    World {
        db_id,
        default_schema,
        store,
        schemas,
    }
}

async fn teardown(e: &Env, w: &World, sut: &mut Conn) {
    clear_layout(e, &w.store).await;
    bump(sut).await;
    for t in TABLES {
        let _ = sut.query_drop(format!("DROP TABLE IF EXISTS {t}")).await;
    }
    for (i, s) in &w.schemas {
        let mut c = root(e, *i, None).await;
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
    }
    let mut c = root(e, 0, None).await;
    c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", e.reference_db()))
        .await
        .unwrap();
    // The database's `__sprig_dtx` came with it and stays with it.
    let _ = &w.default_schema;
}

const COUNTRIES: &str = "INSERT INTO countries VALUES ('us', 'United States'), ('de', 'Germany'), ('fr', 'France'), ('in', 'India')";

/// The cluster index that holds integer key `k` of group 901 now.
fn cluster_of(e: &Env, shards: &[Sh], k: i64) -> usize {
    let spec = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    let (h, _) = sprig_shard::hash_key(&spec, &[KeyValue::Int(i128::from(k))]).unwrap();
    let i = shards.partition_point(|s| s.lo <= h.0).saturating_sub(1);
    shards[i].cluster % e.clusters.len()
}

const DDL: &[&str] = &[
    "CREATE TABLE customers (cid INT NOT NULL PRIMARY KEY, name VARCHAR(40) COLLATE utf8mb4_general_ci, \
     region VARCHAR(10) COLLATE utf8mb4_uca1400_ai_ci, score DECIMAL(10,2), rating DOUBLE, created DATETIME, \
     flag TINYINT, cc CHAR(2)) DEFAULT CHARSET utf8mb4",
    "CREATE TABLE orders (oid INT NOT NULL, cid INT NOT NULL, pid INT, amount DECIMAL(12,2), qty INT, \
     note VARCHAR(30) COLLATE utf8mb4_uca1400_as_cs, PRIMARY KEY (cid, oid)) DEFAULT CHARSET utf8mb4",
    "CREATE TABLE products (pid INT NOT NULL PRIMARY KEY, pname VARCHAR(40) COLLATE utf8mb4_unicode_ci, \
     price DECIMAL(10,2), cat VARCHAR(10)) DEFAULT CHARSET utf8mb4",
];
const ACCOUNTS_DDL: &str = "CREATE TABLE accounts (aid INT NOT NULL PRIMARY KEY, \
     email VARCHAR(40) COLLATE utf8mb4_general_ci, cid INT, UNIQUE KEY email (email)) DEFAULT CHARSET utf8mb4";
/// The suite's tables.
const TABLES: [&str; 6] = [
    "customers",
    "orders",
    "products",
    "tags",
    "countries",
    "accounts",
];
const TAG_DDL: &str = "CREATE TABLE tags (tag VARCHAR(20) COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY, \
     cnt INT) DEFAULT CHARSET utf8mb4";
const REF_DDL: &str = "CREATE TABLE countries (code CHAR(2) NOT NULL PRIMARY KEY, cname VARCHAR(30)) DEFAULT CHARSET utf8mb4";

/// A small deterministic generator.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn pick<'a>(&mut self, v: &'a [&'a str]) -> &'a str {
        v[self.below(v.len() as u64) as usize]
    }
}

const NAMES: &[&str] = &[
    "alice", "Alice", "ALICE", "bob", "Bob ", "carol", "dave", "", " eve", "eve", "straße",
    "strasse", "Ärger", "arger", "zoë", "zoe", "émile", "Emile", "o'neil", "x\\y", "a\tb", "Zed",
    "zed", "mañana",
];
const REGIONS: &[&str] = &["north", "North", "south", "east", "west", "WEST", "", "ost"];
const NOTES: &[&str] = &[
    "a", "A", "á", "b", "B", "résumé", "resume", "Resume", "", " ", "z", "Z ", "ß", "ss",
];
const CATS: &[&str] = &["tools", "Tools", "toys", "food", "books"];

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn opt<T: std::fmt::Display>(r: &mut Rng, v: impl FnOnce(&mut Rng) -> T) -> String {
    if r.below(10) == 0 {
        "NULL".into()
    } else {
        v(r).to_string()
    }
}

/// Seed data: INSERT statements, run on both sides.
fn seed(r: &mut Rng) -> Vec<String> {
    let mut out = Vec::new();
    let mut rows = Vec::new();
    for cid in 1..=120i64 {
        let cid = if cid % 17 == 0 { -cid } else { cid };
        let name = if r.below(12) == 0 {
            "NULL".into()
        } else {
            lit(r.pick(NAMES))
        };
        let region = if r.below(12) == 0 {
            "NULL".into()
        } else {
            lit(r.pick(REGIONS))
        };
        let score = opt(r, |r| {
            format!("{}.{:02}", r.below(2000) as i64 - 500, r.below(100))
        });
        let rating = opt(r, |r| format!("{}.{}e0", r.below(100), r.below(1000)));
        let created = opt(r, |r| {
            format!(
                "'2026-{:02}-{:02} {:02}:{:02}:00'",
                1 + r.below(12),
                1 + r.below(28),
                r.below(24),
                r.below(60)
            )
        });
        let flag = opt(r, |r| r.below(3));
        let cc = lit(["us", "de", "fr", "in", "zz"][r.below(5) as usize]);
        rows.push(format!(
            "({cid}, {name}, {region}, {score}, {rating}, {created}, {flag}, {cc})"
        ));
    }
    for chunk in rows.chunks(25) {
        out.push(format!("INSERT INTO customers VALUES {}", chunk.join(", ")));
    }
    let mut rows = Vec::new();
    for oid in 1..=400i64 {
        let cid = 1 + r.below(120) as i64;
        let cid = if cid % 17 == 0 { -cid } else { cid };
        let pid = opt(r, |r| 1 + r.below(40));
        let amount = opt(r, |r| format!("{}.{:02}", r.below(1000), r.below(100)));
        let qty = opt(r, |r| r.below(20));
        let note = if r.below(10) == 0 {
            "NULL".into()
        } else {
            lit(r.pick(NOTES))
        };
        rows.push(format!("({oid}, {cid}, {pid}, {amount}, {qty}, {note})"));
    }
    for chunk in rows.chunks(40) {
        out.push(format!(
            "INSERT INTO orders (oid, cid, pid, amount, qty, note) VALUES {}",
            chunk.join(", ")
        ));
    }
    let mut rows = Vec::new();
    for pid in 1..=36 {
        rows.push(format!(
            "({pid}, {}, {}, {})",
            lit(&format!("{} {pid}", r.pick(NAMES))),
            opt(r, |r| format!("{}.{:02}", r.below(300), r.below(100))),
            lit(r.pick(CATS))
        ));
    }
    out.push(format!("INSERT INTO products VALUES {}", rows.join(", ")));
    let tags: Vec<String> = NAMES
        .iter()
        .filter(|n| !n.is_empty())
        .enumerate()
        .map(|(i, n)| format!("({}, {i})", lit(n)))
        .collect();
    out.push(format!(
        "INSERT IGNORE INTO tags VALUES {}",
        tags.join(", ")
    ));
    // `accounts`: a unique email (case-insensitive) on a table keyed by
    // `aid`, kept by chapter 19's lookup.
    let rows: Vec<String> = (1..=60)
        .map(|aid| {
            let email = if aid % 11 == 0 {
                "NULL".to_owned()
            } else {
                lit(&format!(
                    "{}{aid}@x.org",
                    if aid % 3 == 0 { "U" } else { "u" }
                ))
            };
            format!("({aid}, {email}, {})", aid % 7)
        })
        .collect();
    out.push(format!("INSERT INTO accounts VALUES {}", rows.join(", ")));
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmp {
    /// ORDER BY fixes the order completely.
    Ordered,
    /// Compare as multisets.
    Bag,
    /// Compare the row count only (LIMIT without a total order).
    Count,
}

/// The statement corpus.
fn corpus(r: &mut Rng) -> Vec<(String, Cmp)> {
    use Cmp::{Bag, Count, Ordered};
    let mut v: Vec<(String, Cmp)> = Vec::new();
    let k = |r: &mut Rng| {
        let c = 1 + r.below(125) as i64;
        if c % 17 == 0 { -c } else { c }
    };
    // Point reads and key filters (single shard).
    for _ in 0..30 {
        let c = k(r);
        v.push((format!("SELECT * FROM customers WHERE cid = {c}"), Bag));
        v.push((
            format!("SELECT oid, amount, note FROM orders WHERE cid = '{c}' ORDER BY oid"),
            Ordered,
        ));
    }
    for _ in 0..10 {
        let (a, b, c) = (k(r), k(r), k(r));
        v.push((
            format!("SELECT cid, name FROM customers WHERE cid IN ({a}, {b}, {c}) ORDER BY cid"),
            Ordered,
        ));
        v.push((
            format!("SELECT cid, score FROM customers WHERE cid = {a} OR cid = {b}"),
            Bag,
        ));
        v.push((
            format!("SELECT COUNT(*), SUM(amount) FROM orders WHERE cid = {a}"),
            Bag,
        ));
    }
    // Filters (scatter).
    for _ in 0..15 {
        let x = r.below(1500) as i64 - 500;
        v.push((
            format!("SELECT cid, name, score FROM customers WHERE score > {x}"),
            Bag,
        ));
        v.push((
            format!(
                "SELECT oid, cid FROM orders WHERE qty BETWEEN {} AND {}",
                r.below(5),
                5 + r.below(10)
            ),
            Bag,
        ));
        v.push((
            format!(
                "SELECT cid FROM customers WHERE name = {}",
                lit(r.pick(NAMES))
            ),
            Bag,
        ));
        v.push((
            format!(
                "SELECT cid, region FROM customers WHERE region LIKE '{}%'",
                &r.pick(REGIONS).chars().next().unwrap_or('n').to_string()
            ),
            Bag,
        ));
    }
    v.push(("SELECT * FROM customers WHERE name IS NULL".into(), Bag));
    v.push(("SELECT cid FROM customers WHERE cid > 1000".into(), Bag));
    // ORDER BY with collations, LIMIT and OFFSET (ordered merge).
    let cols = [
        "name", "region", "score", "rating", "created", "cid", "flag",
    ];
    for _ in 0..40 {
        let c = cols[r.below(cols.len() as u64) as usize];
        let dir = if r.below(2) == 0 { "ASC" } else { "DESC" };
        let lim = match r.below(4) {
            0 => String::new(),
            1 => format!(" LIMIT {}", 1 + r.below(30)),
            2 => format!(" LIMIT {} OFFSET {}", 1 + r.below(20), r.below(60)),
            _ => format!(" LIMIT {}, {}", r.below(50), 1 + r.below(20)),
        };
        v.push((
            format!("SELECT cid, {c} FROM customers ORDER BY {c} {dir}, cid{lim}"),
            Ordered,
        ));
    }
    for _ in 0..15 {
        let dir = if r.below(2) == 0 { "" } else { " DESC" };
        v.push((
            format!(
                "SELECT cid, oid, note FROM orders ORDER BY note{dir}, cid, oid LIMIT {}",
                5 + r.below(40)
            ),
            Ordered,
        ));
        v.push((format!("SELECT oid, cid, amount FROM orders WHERE amount IS NOT NULL ORDER BY amount{dir}, cid, oid LIMIT {} OFFSET {}", 1 + r.below(20), r.below(30)), Ordered));
    }
    v.push((
        "SELECT cid FROM customers ORDER BY name, region, cid".into(),
        Ordered,
    ));
    v.push((
        "SELECT cid, name FROM customers ORDER BY 2, 1".into(),
        Ordered,
    ));
    v.push((
        "SELECT cid AS id, name AS n FROM customers ORDER BY n DESC, id".into(),
        Ordered,
    ));
    v.push((
        "SELECT cid FROM customers ORDER BY score * 2, cid LIMIT 10".into(),
        Ordered,
    ));
    v.push(("SELECT * FROM customers ORDER BY cid".into(), Ordered));
    v.push(("SELECT cid FROM customers LIMIT 7".into(), Count));
    // Aggregates.
    let aggs = [
        "COUNT(*)",
        "COUNT(score)",
        "SUM(score)",
        "AVG(score)",
        "MIN(name)",
        "MAX(name)",
        "MIN(score)",
        "MAX(created)",
        "SUM(rating)",
        "AVG(rating)",
        "AVG(flag)",
        "SUM(flag)",
        "MIN(region)",
    ];
    for a in aggs {
        v.push((format!("SELECT {a} FROM customers"), Bag));
        v.push((format!("SELECT {a} FROM customers WHERE cid > 100000"), Bag));
        v.push((
            format!("SELECT region, {a} FROM customers GROUP BY region"),
            Bag,
        ));
        v.push((
            format!("SELECT region, {a} FROM customers GROUP BY region ORDER BY region DESC"),
            Ordered,
        ));
    }
    for _ in 0..10 {
        let n = r.below(5);
        v.push((format!("SELECT region, COUNT(*) AS n, SUM(score) FROM customers GROUP BY region HAVING COUNT(*) > {n} ORDER BY n DESC, region"), Ordered));
        v.push((format!("SELECT flag, AVG(score), MIN(name), MAX(name) FROM customers GROUP BY flag HAVING AVG(score) > {} OR flag IS NULL", r.below(500) as i64 - 200), Bag));
        v.push((format!("SELECT note, COUNT(*), SUM(qty), AVG(amount) FROM orders GROUP BY note ORDER BY note LIMIT {}", 1 + r.below(8)), Ordered));
    }
    v.push((
        "SELECT cid, COUNT(*), SUM(amount) FROM orders GROUP BY cid".into(),
        Bag,
    ));
    v.push((
        "SELECT cid, COUNT(*) AS n FROM orders GROUP BY cid ORDER BY n DESC, cid LIMIT 5".into(),
        Ordered,
    ));
    v.push((
        "SELECT cid, MAX(note) FROM orders GROUP BY cid HAVING COUNT(*) > 3".into(),
        Bag,
    ));
    v.push((
        "SELECT region, cc, COUNT(*) FROM customers GROUP BY region, cc".into(),
        Bag,
    ));
    v.push(("SELECT DISTINCT region FROM customers".into(), Bag));
    v.push((
        "SELECT DISTINCT flag FROM customers ORDER BY flag".into(),
        Ordered,
    ));
    v.push(("SELECT DISTINCT cid FROM orders".into(), Bag));
    v.push(("SELECT 'x', COUNT(*) FROM orders".into(), Bag));
    // Joins within a table group, with reference tables, lookup joins.
    for _ in 0..10 {
        let c = k(r);
        v.push((format!("SELECT c.cid, c.name, o.oid, o.amount FROM customers c JOIN orders o ON o.cid = c.cid WHERE c.cid = {c} ORDER BY o.oid"), Ordered));
    }
    v.push(("SELECT c.cid, o.oid, o.amount FROM customers c JOIN orders o ON o.cid = c.cid WHERE o.qty > 10".into(), Bag));
    v.push(("SELECT c.region, COUNT(*), SUM(o.amount) FROM customers c JOIN orders o ON o.cid = c.cid GROUP BY c.region".into(), Bag));
    v.push(("SELECT c.cid, o.oid FROM customers c LEFT JOIN orders o ON o.cid = c.cid ORDER BY c.cid, o.oid".into(), Ordered));
    v.push((
        "SELECT c.cid, k.cname FROM customers c JOIN countries k ON k.code = c.cc ORDER BY c.cid"
            .into(),
        Ordered,
    ));
    v.push((
        "SELECT c.cid, k.cname FROM customers c LEFT JOIN countries k ON k.code = c.cc".into(),
        Bag,
    ));
    v.push(("SELECT * FROM countries ORDER BY code".into(), Ordered));
    v.push(("SELECT k.cname, COUNT(*) FROM customers c JOIN countries k ON k.code = c.cc GROUP BY k.cname".into(), Bag));
    for _ in 0..8 {
        let c = k(r);
        v.push((format!("SELECT o.oid, o.pid, p.pname, p.price FROM orders o JOIN products p ON p.pid = o.pid WHERE o.cid = {c} ORDER BY o.oid"), Ordered));
        v.push((format!("SELECT o.oid, p.pname FROM orders o LEFT JOIN products p ON p.pid = o.pid WHERE o.cid = {c} ORDER BY o.oid"), Ordered));
    }
    v.push(("SELECT o.cid, o.oid, p.pname FROM orders o JOIN products p ON p.pid = o.pid WHERE p.cat = 'tools'".into(), Bag));
    v.push(("SELECT o.cid, o.oid, p.cat FROM orders o LEFT JOIN products p ON p.pid = o.pid AND p.price > 100 ORDER BY o.cid, o.oid LIMIT 50".into(), Ordered));
    v.push((
        "SELECT p.pid, p.pname FROM products p WHERE p.pid IN (3, 5, 7) ORDER BY p.pid".into(),
        Ordered,
    ));
    // A string key: case and trailing-space variants route alike.
    for n in NAMES {
        v.push((
            format!("SELECT tag, cnt FROM tags WHERE tag = {}", lit(n)),
            Bag,
        ));
        v.push((
            format!(
                "SELECT COUNT(*) FROM tags WHERE tag IN ({}, {})",
                lit(n),
                lit(&n.to_uppercase())
            ),
            Bag,
        ));
    }
    v.push((
        "SELECT tag, cnt FROM tags ORDER BY tag, cnt".into(),
        Ordered,
    ));
    v.push(("SELECT SUM(cnt), MIN(tag), MAX(tag) FROM tags".into(), Bag));
    // Statements the router may reject (counted as skipped, never wrong).
    v.push(("SELECT COUNT(DISTINCT region) FROM customers".into(), Bag));
    v.push(("SELECT SUM(score) / COUNT(*) FROM customers".into(), Bag));
    v.push((
        "SELECT cid, ROW_NUMBER() OVER (ORDER BY cid) FROM customers".into(),
        Bag,
    ));
    v.push((
        "SELECT cid FROM customers UNION SELECT cid FROM orders".into(),
        Bag,
    ));
    v.push((
        "SELECT cid FROM customers WHERE cid IN (SELECT cid FROM orders WHERE qty > 15)".into(),
        Bag,
    ));
    v.push((
        "SELECT region, GROUP_CONCAT(name ORDER BY name) FROM customers GROUP BY region".into(),
        Bag,
    ));
    v.push((
        "SELECT o.oid, p.pname FROM orders o JOIN products p ON p.pid > o.pid WHERE o.cid = 1"
            .into(),
        Bag,
    ));
    v
}

/// DML, each followed by a full comparison of the table it touched.
fn dml(r: &mut Rng) -> Vec<(String, &'static str)> {
    let mut v = Vec::new();
    for i in 0..12 {
        let c = 1 + r.below(120) as i64;
        v.push((
            format!("UPDATE customers SET score = score + 1 WHERE cid = {c}"),
            "customers",
        ));
        v.push((
            format!(
                "UPDATE customers SET flag = {} WHERE region = {}",
                r.below(3),
                lit(r.pick(REGIONS))
            ),
            "customers",
        ));
        v.push((
            format!(
                "DELETE FROM orders WHERE cid = {c} AND oid > {}",
                300 + r.below(100)
            ),
            "orders",
        ));
        v.push((
            format!(
                "UPDATE orders SET qty = qty + 1 WHERE note = {}",
                lit(r.pick(NOTES))
            ),
            "orders",
        ));
        v.push((
            format!(
                "INSERT INTO orders (oid, cid, pid, amount, qty, note) VALUES ({}, {}, 3, 1.50, 1, 'n'), ({}, {}, 4, 2.25, 2, 'N'), ({}, {}, NULL, NULL, NULL, NULL)",
                1000 + i * 3,
                1 + r.below(120),
                1001 + i * 3,
                1 + r.below(120),
                1002 + i * 3,
                1 + r.below(120)
            ),
            "orders",
        ));
    }
    v.push((
        "INSERT INTO customers (cid, name) VALUES (5000, 'new')".into(),
        "customers",
    ));
    v.push((
        "INSERT IGNORE INTO customers (cid, name) VALUES (5000, 'dup'), (5001, 'n2'), (5002, 'n3')"
            .into(),
        "customers",
    ));
    v.push((
        "INSERT INTO customers (cid, name) VALUES (5003, 'u') ON DUPLICATE KEY UPDATE name = 'u2'"
            .into(),
        "customers",
    ));
    v.push((
        "INSERT INTO customers (cid, name) VALUES (5003, 'u') ON DUPLICATE KEY UPDATE name = 'u2'"
            .into(),
        "customers",
    ));
    v.push((
        "REPLACE INTO customers (cid, name) VALUES (5001, 'r')".into(),
        "customers",
    ));
    v.push((
        "DELETE FROM customers WHERE cid >= 5000".into(),
        "customers",
    ));
    v.push((
        "UPDATE customers SET name = 'x' ORDER BY cid LIMIT 2".into(),
        "customers",
    ));
    v.push((
        "UPDATE customers SET cid = cid + 1000 WHERE cid = 3".into(),
        "customers",
    ));
    v.push((
        "INSERT INTO customers (name) VALUES ('no key')".into(),
        "customers",
    ));
    v.push(("DELETE FROM orders WHERE qty IS NULL".into(), "orders"));
    v.push((
        "UPDATE tags SET cnt = cnt + 10 WHERE tag = 'ALICE'".into(),
        "tags",
    ));
    v.push((
        "INSERT INTO tags VALUES ('NewTag', 100), ('newtag2', 200), ('Zz', 300)".into(),
        "tags",
    ));
    v.push(("DELETE FROM tags WHERE tag = 'newtag '".into(), "tags"));
    v
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Rows(Vec<ColumnType>, Vec<Vec<Option<String>>>),
    Ok(u64),
    Err(u16, String),
}

async fn run(c: &mut Conn, sql: &str) -> Outcome {
    match c.query_iter(sql).await {
        Err(mysql_async::Error::Server(e)) => Outcome::Err(e.code, e.message),
        Err(e) => panic!("{sql}: {e}"),
        Ok(mut res) => {
            let cols: Vec<ColumnType> = res
                .columns()
                .map(|c| c.iter().map(mysql_async::Column::column_type).collect())
                .unwrap_or_default();
            let affected = res.affected_rows();
            match res.collect::<Row>().await {
                Err(mysql_async::Error::Server(e)) => Outcome::Err(e.code, e.message),
                Err(e) => panic!("{sql}: {e}"),
                Ok(rows) if cols.is_empty() => {
                    let _ = rows;
                    Outcome::Ok(affected)
                }
                Ok(rows) => Outcome::Rows(
                    cols,
                    rows.into_iter()
                        .map(|r| {
                            r.unwrap()
                                .into_iter()
                                .map(|v| match v {
                                    Value::NULL => None,
                                    Value::Bytes(b) => {
                                        Some(String::from_utf8_lossy(&b).into_owned())
                                    }
                                    other => Some(other.as_sql(false)),
                                })
                                .collect()
                        })
                        .collect(),
                ),
            }
        }
    }
}

fn float_col(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::MYSQL_TYPE_DOUBLE | ColumnType::MYSQL_TYPE_FLOAT
    )
}

fn same_value(a: &Option<String>, b: &Option<String>, float: bool) -> bool {
    match (a, b) {
        (Some(x), Some(y)) if float && x != y => match (x.parse::<f64>(), y.parse::<f64>()) {
            (Ok(p), Ok(q)) => (p - q).abs() <= 1e-9 * p.abs().max(q.abs()).max(1e-300),
            _ => false,
        },
        _ => a == b,
    }
}

fn same_rows(types: &[ColumnType], a: &[Vec<Option<String>>], b: &[Vec<Option<String>>]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .zip(types)
                    .all(|((p, q), t)| same_value(p, q, float_col(*t)))
        })
}

fn sorted(mut v: Vec<Vec<Option<String>>>, types: &[ColumnType]) -> Vec<Vec<Option<String>>> {
    // Sorted by the other columns first, floats last and rounded, so the
    // multiset sort pairs rows up; the values themselves stay exact, and
    // `same_rows` compares floats within its tolerance (rounding them in
    // place made 50.796625 and 50.796625000000006 differ in the 7th digit).
    let key = |row: &Vec<Option<String>>| {
        let mut exact = Vec::new();
        let mut rounded = Vec::new();
        for (c, t) in row.iter().zip(types) {
            if float_col(*t) {
                rounded.push(
                    c.as_ref()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|f| format!("{f:.4e}")),
                );
            } else {
                exact.push(c.clone());
            }
        }
        (exact, rounded)
    };
    v.sort_by_key(key);
    v
}

fn fold_str(s: &str) -> String {
    s.trim_end_matches(' ')
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| match c {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' => 'a',
            'è' | 'é' | 'ê' | 'ë' => 'e',
            'ì' | 'í' | 'î' | 'ï' => 'i',
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' => 'o',
            'ù' | 'ú' | 'û' | 'ü' => 'u',
            'ñ' => 'n',
            c => c,
        })
        .collect()
}

fn fold(o: &Outcome) -> Outcome {
    match o {
        Outcome::Rows(t, rows) => Outcome::Rows(
            t.clone(),
            rows.iter()
                .map(|r| r.iter().map(|v| v.as_deref().map(fold_str)).collect())
                .collect(),
        ),
        Outcome::Ok(n) => Outcome::Ok(*n),
        Outcome::Err(c, m) => Outcome::Err(*c, m.clone()),
    }
}

#[derive(Default)]
struct Tally {
    compared: usize,
    skipped: usize,
    mismatches: Vec<String>,
    reasons: std::collections::BTreeMap<String, usize>,
}

impl Tally {
    fn check(&mut self, sql: &str, mode: Cmp, sut: &Outcome, reference: &Outcome) {
        // Groups, DISTINCT, MIN and MAX under case- and accent-insensitive
        // collations may show any member of a set of equal strings
        // ('north' or 'North'): such values compare folded.
        let upper = sql.to_ascii_uppercase();
        if upper.contains("GROUP BY")
            || upper.contains("DISTINCT")
            || upper.contains("MIN(")
            || upper.contains("MAX(")
        {
            let (a, b) = (fold(sut), fold(reference));
            return self.check_exact(sql, mode, &a, &b);
        }
        self.check_exact(sql, mode, sut, reference);
    }

    fn check_exact(&mut self, sql: &str, mode: Cmp, sut: &Outcome, reference: &Outcome) {
        self.compared += 1;
        let ok = match (sut, reference) {
            (Outcome::Err(39420, m), _) => {
                self.skipped += 1;
                let reason = m.split(['(', ')']).nth(1).unwrap_or("?").to_owned();
                *self.reasons.entry(reason).or_default() += 1;
                return;
            }
            (Outcome::Err(a, _), Outcome::Err(b, _)) => a == b,
            (Outcome::Ok(a), Outcome::Ok(b)) => a == b,
            (Outcome::Rows(ta, a), Outcome::Rows(_, b)) => match mode {
                Cmp::Ordered => same_rows(ta, a, b),
                Cmp::Bag => same_rows(ta, &sorted(a.clone(), ta), &sorted(b.clone(), ta)),
                Cmp::Count => a.len() == b.len(),
            },
            _ => false,
        };
        if !ok {
            let mut m = format!("MISMATCH {sql}\n");
            let show = |o: &Outcome| match o {
                Outcome::Rows(_, r) => format!(
                    "{} rows: {:?}",
                    r.len(),
                    r.iter().take(6).collect::<Vec<_>>()
                ),
                other => format!("{other:?}"),
            };
            let _ = writeln!(m, "  sut:       {}", show(sut));
            let _ = writeln!(m, "  reference: {}", show(reference));
            self.mismatches.push(m);
        }
    }
}

/// Makes the SUT's table equal to the reference's (after a statement the
/// router rejected but the reference ran).
async fn resync(sut: &mut Conn, reference: &mut Conn, table: &str) {
    let rows = run(reference, &format!("SELECT * FROM {table}")).await;
    let Outcome::Rows(_, rows) = rows else { return };
    sut.query_drop(format!("DELETE FROM {table}"))
        .await
        .unwrap();
    for chunk in rows.chunks(50) {
        let values: Vec<String> = chunk
            .iter()
            .map(|r| {
                let vals: Vec<String> = r
                    .iter()
                    .map(|v| v.as_ref().map_or_else(|| "NULL".into(), |s| lit(s)))
                    .collect();
                format!("({})", vals.join(", "))
            })
            .collect();
        sut.query_drop(format!("INSERT INTO {table} VALUES {}", values.join(", ")))
            .await
            .unwrap();
    }
}

/// A statement through the binary protocol (`COM_STMT_PREPARE` and
/// `COM_STMT_EXECUTE`).
async fn run_exec(c: &mut Conn, sql: &str, params: Vec<Value>) -> Outcome {
    let p = if params.is_empty() {
        Params::Empty
    } else {
        Params::Positional(params)
    };
    match c.exec_iter(sql, p).await {
        Err(mysql_async::Error::Server(e)) => Outcome::Err(e.code, e.message),
        Err(e) => panic!("{sql}: {e}"),
        Ok(mut res) => {
            let cols: Vec<ColumnType> = res
                .columns()
                .map(|c| c.iter().map(mysql_async::Column::column_type).collect())
                .unwrap_or_default();
            let affected = res.affected_rows();
            match res.collect::<Row>().await {
                Err(mysql_async::Error::Server(e)) => Outcome::Err(e.code, e.message),
                Err(e) => panic!("{sql}: {e}"),
                Ok(_) if cols.is_empty() => Outcome::Ok(affected),
                Ok(rows) => Outcome::Rows(
                    cols,
                    rows.into_iter()
                        .map(|r| {
                            r.unwrap()
                                .into_iter()
                                .map(|v| match v {
                                    Value::NULL => None,
                                    Value::Bytes(b) => {
                                        Some(String::from_utf8_lossy(&b).into_owned())
                                    }
                                    other => Some(other.as_sql(false)),
                                })
                                .collect()
                        })
                        .collect(),
                ),
            }
        }
    }
}

/// Prepared statements over shards: single-shard ones run as the backend's
/// own prepared statement, the rest as text with rows encoded in binary.
fn prepared(r: &mut Rng) -> Vec<(&'static str, Vec<Value>, Cmp)> {
    use Cmp::{Bag, Ordered};
    let mut v = Vec::new();
    let k = |r: &mut Rng| {
        let c = 1 + r.below(125) as i64;
        if c % 17 == 0 { -c } else { c }
    };
    for _ in 0..20 {
        let c = k(r);
        v.push((
            "SELECT * FROM customers WHERE cid = ?",
            vec![Value::Int(c)],
            Bag,
        ));
        v.push((
            "SELECT oid, amount, note FROM orders WHERE cid = ? ORDER BY oid",
            vec![Value::Bytes(c.to_string().into_bytes())],
            Ordered,
        ));
    }
    for _ in 0..6 {
        let (a, b, c) = (k(r), k(r), k(r));
        v.push((
            "SELECT cid, name FROM customers WHERE cid IN (?, ?, ?) ORDER BY cid",
            vec![Value::Int(a), Value::Int(b), Value::Int(c)],
            Ordered,
        ));
        v.push((
            "SELECT cid, name, score, rating, created FROM customers WHERE score > ?",
            vec![Value::Double(r.below(800) as f64 - 300.5)],
            Bag,
        ));
        v.push((
            "SELECT region, COUNT(*), SUM(score), AVG(score), MIN(created) FROM customers WHERE cid > ? GROUP BY region",
            vec![Value::Int(k(r))],
            Bag,
        ));
        v.push((
            "SELECT cid, created FROM customers WHERE created > ? ORDER BY created, cid LIMIT 7",
            vec![Value::Date(2026, 1 + r.below(12) as u8, 1, 12, 0, 0, 0)],
            Ordered,
        ));
        v.push((
            "SELECT o.oid, o.pid, p.pname, p.price FROM orders o JOIN products p ON p.pid = o.pid WHERE o.cid = ? ORDER BY o.oid",
            vec![Value::Int(k(r))],
            Ordered,
        ));
    }
    for n in NAMES.iter().filter(|n| !n.is_empty()) {
        v.push((
            "SELECT tag, cnt FROM tags WHERE tag = ?",
            vec![Value::Bytes(n.as_bytes().to_vec())],
            Bag,
        ));
    }
    v.push((
        "SELECT c.cid, k.cname FROM customers c JOIN countries k ON k.code = c.cc WHERE c.cid > ? ORDER BY c.cid",
        vec![Value::Int(50)],
        Ordered,
    ));
    v
}

/// Prepared writes, each followed by a full comparison.
fn prepared_dml(r: &mut Rng) -> Vec<(&'static str, Vec<Value>, &'static str)> {
    let mut v = Vec::new();
    for i in 0..8 {
        let c = 1 + r.below(120) as i64;
        v.push((
            "UPDATE customers SET score = score + ? WHERE cid = ?",
            vec![Value::Bytes(b"1.25".to_vec()), Value::Int(c)],
            "customers",
        ));
        v.push((
            "INSERT INTO orders (oid, cid, pid, amount, qty, note) VALUES (?, ?, ?, ?, ?, ?)",
            vec![
                Value::Int(2000 + i),
                Value::Int(1 + r.below(120) as i64),
                Value::Int(3),
                Value::Bytes(b"9.99".to_vec()),
                Value::Int(4),
                Value::Bytes(b"prep".to_vec()),
            ],
            "orders",
        ));
        v.push((
            "UPDATE orders SET qty = qty + ? WHERE note = ?",
            vec![
                Value::Int(1),
                Value::Bytes(r.pick(NOTES).as_bytes().to_vec()),
            ],
            "orders",
        ));
    }
    v
}

fn full(t: &str) -> String {
    match t {
        "customers" => "SELECT * FROM customers ORDER BY cid".to_owned(),
        "tags" => "SELECT cnt FROM tags ORDER BY cnt".to_owned(),
        "products" => "SELECT * FROM products ORDER BY pid".to_owned(),
        "accounts" => "SELECT * FROM accounts ORDER BY aid".to_owned(),
        _ => "SELECT * FROM orders ORDER BY cid, oid".to_owned(),
    }
}

/// Chapter 21 through the gateway: an `ALTER TABLE` of a sharded table
/// reaches every shard (the DDL path reads the same catalog layout as the
/// router). Returns the shards that have the new column.
async fn ddl_across_shards(
    e: &Env,
    w: &World,
    sut: &mut Conn,
    reference: &mut Conn,
    tally: &mut Tally,
) -> (usize, usize) {
    both(
        sut,
        reference,
        tally,
        &["ALTER TABLE products ADD COLUMN extra INT NULL".into()],
    )
    .await;
    let g = &layout()[1];
    let mut have = 0;
    for s in &g.shards {
        let schema = shard_schema(e.tenant, w.db_id, GroupId(g.id), ShardId(s.id));
        let mut c = root(e, s.cluster, None).await;
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'products' AND COLUMN_NAME = 'extra'",
                (schema.as_str(),),
            )
            .await
            .unwrap();
        if n == Some(1) {
            have += 1;
        }
    }
    both(
        sut,
        reference,
        tally,
        &["UPDATE products SET extra = pid * 2 WHERE pid < 20".into()],
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after ALTER TABLE across shards",
        "products",
    )
    .await;
    (have, g.shards.len())
}

async fn compare_full(
    sut: &mut Conn,
    reference: &mut Conn,
    tally: &mut Tally,
    what: &str,
    t: &str,
) {
    let q = full(t);
    let a = run(sut, &q).await;
    let b = run(reference, &q).await;
    tally.check(&format!("{what}: {q}"), Cmp::Ordered, &a, &b);
}

/// Both sides run `stmts` in order; the outcome of each is compared.
async fn both(sut: &mut Conn, reference: &mut Conn, tally: &mut Tally, stmts: &[String]) {
    for s in stmts {
        let a = run(sut, s).await;
        let b = run(reference, s).await;
        tally.check(s, Cmp::Bag, &a, &b);
    }
}

/// Keys of group 901 on cluster 0 and on cluster 1.
fn keys_by_cluster(e: &Env, shards: &[Sh]) -> (Vec<i64>, Vec<i64>) {
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for k in 1..=120i64 {
        if k % 17 == 0 {
            continue;
        }
        if cluster_of(e, shards, k) == 0 {
            a.push(k);
        } else {
            b.push(k);
        }
    }
    (a, b)
}

/// Transactions spanning clusters: committed, rolled back, with savepoints.
async fn transactions(
    e: &Env,
    sut: &mut Conn,
    reference: &mut Conn,
    tally: &mut Tally,
    shards: &[Sh],
) {
    let (a, b) = keys_by_cluster(e, shards);
    let (x, y, z) = (a[0], b[0], b[1]);
    // Committed.
    both(
        sut,
        reference,
        tally,
        &[
            "BEGIN".into(),
            format!("UPDATE customers SET flag = 7 WHERE cid = {x}"),
            format!("UPDATE customers SET flag = 7 WHERE cid = {y}"),
            format!("INSERT INTO orders (oid, cid, pid, amount, qty, note) VALUES (3001, {x}, 1, 1.00, 1, 't'), (3002, {y}, 2, 2.00, 2, 't')"),
            "SELECT cid, flag FROM customers WHERE flag = 7 ORDER BY cid".into(),
            "UPDATE orders SET qty = qty + 100 WHERE note = 't'".into(),
            "COMMIT".into(),
        ],
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after a committed transaction",
        "customers",
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after a committed transaction",
        "orders",
    )
    .await;
    // Rolled back, with a savepoint rolled back to in the middle.
    both(
        sut,
        reference,
        tally,
        &[
            "START TRANSACTION".into(),
            format!("UPDATE customers SET name = 'tx1' WHERE cid = {x}"),
            "SAVEPOINT s1".into(),
            format!("UPDATE customers SET name = 'tx2' WHERE cid = {z}"),
            format!("DELETE FROM orders WHERE cid = {y}"),
            "ROLLBACK TO SAVEPOINT s1".into(),
            "SELECT cid, name FROM customers WHERE name LIKE 'tx%' ORDER BY cid".into(),
            format!("SELECT COUNT(*) FROM orders WHERE cid = {y}"),
            "UPDATE customers SET score = 0 WHERE region = 'north'".into(),
            "ROLLBACK".into(),
        ],
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after a rolled-back transaction",
        "customers",
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after a rolled-back transaction",
        "orders",
    )
    .await;
    // Committed after a savepoint released, and a failing statement inside.
    both(
        sut,
        reference,
        tally,
        &[
            "BEGIN".into(),
            "SAVEPOINT s2".into(),
            format!("UPDATE customers SET score = score + 3 WHERE cid IN ({x}, {y}, {z})"),
            "RELEASE SAVEPOINT s2".into(),
            format!("INSERT INTO customers (cid, name) VALUES ({x}, 'dup')"),
            format!("UPDATE orders SET note = 'u' WHERE cid IN ({x}, {z})"),
            "COMMIT".into(),
        ],
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after a transaction with a failed statement",
        "customers",
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after a transaction with a failed statement",
        "orders",
    )
    .await;
}

/// Chapter 19 through the router: reads, updates and deletes by a unique
/// lookup's column reach only the owners' shards; inserts claim values
/// (a duplicate fails with 1062 everywhere), deletes release them, all in
/// the statement's transaction; a transaction sees its own claims. The
/// lookup tables end with exactly one row per non-NULL email.
async fn lookups(e: &Env, w: &World, sut: &mut Conn, reference: &mut Conn, tally: &mut Tally) {
    let reads: [(&str, Cmp); 7] = [
        ("SELECT * FROM accounts WHERE email = 'u7@x.org'", Cmp::Bag),
        ("SELECT * FROM accounts WHERE email = 'U7@X.ORG'", Cmp::Bag),
        (
            "SELECT aid, cid FROM accounts WHERE email IN ('u1@x.org', 'U3@X.org', 'u40@x.org', 'nobody@x.org') ORDER BY aid",
            Cmp::Ordered,
        ),
        (
            "SELECT COUNT(*) FROM accounts WHERE email = 'nobody@x.org'",
            Cmp::Bag,
        ),
        (
            "SELECT cid, COUNT(*) FROM accounts WHERE email IN ('u1@x.org', 'u2@x.org', 'u4@x.org') GROUP BY cid ORDER BY cid",
            Cmp::Ordered,
        ),
        (
            "SELECT * FROM accounts WHERE email = 'u8@x.org' AND cid = 1",
            Cmp::Bag,
        ),
        (
            "SELECT * FROM accounts WHERE email IS NULL ORDER BY aid",
            Cmp::Ordered,
        ),
    ];
    for (q, mode) in reads {
        let a = run(sut, q).await;
        let b = run(reference, q).await;
        tally.check(q, mode, &a, &b);
    }
    match run(
        sut,
        "EXPLAIN FORMAT=SPRIG SELECT * FROM accounts WHERE email = 'u7@x.org'",
    )
    .await
    {
        Outcome::Rows(_, rows) if rows.first().and_then(|r| r[2].as_deref()) == Some("Lookup") => {}
        other => tally
            .mismatches
            .push(format!("EXPLAIN of a lookup read: {other:?}")),
    }
    both(
        sut,
        reference,
        tally,
        &[
            "UPDATE accounts SET cid = cid + 10 WHERE email = 'u2@x.org'".into(),
            "DELETE FROM accounts WHERE email IN ('u4@x.org', 'U6@x.org')".into(),
            "INSERT INTO accounts VALUES (200, 'u4@x.org', 1)".into(),
            "INSERT INTO accounts VALUES (201, 'U5@X.ORG', 1)".into(),
            "INSERT INTO accounts VALUES (202, 'v1@x.org', 2), (203, 'v2@x.org', 3), (204, 'v3@x.org', 4), (205, NULL, 5)".into(),
            "INSERT INTO accounts VALUES (206, 'v4@x.org', 2), (207, 'V4@x.org', 3)".into(),
            "DELETE FROM accounts WHERE aid = 10".into(),
            "DELETE FROM accounts WHERE cid = 3 AND aid > 40".into(),
        ],
    )
    .await;
    // A changed email needs a claim and a release: refused in R1, and
    // only the gateway is asked.
    match run(sut, "UPDATE accounts SET email = 'zz@x.org' WHERE aid = 12").await {
        Outcome::Err(39420, m) if m.contains("(lookup)") => {}
        other => tally
            .mismatches
            .push(format!("UPDATE of a lookup column: {other:?}")),
    }
    compare_full(sut, reference, tally, "after lookup writes", "accounts").await;
    both(
        sut,
        reference,
        tally,
        &[
            "BEGIN".into(),
            "INSERT INTO accounts VALUES (300, 'new@x.org', 2)".into(),
            "SELECT * FROM accounts WHERE email = 'NEW@x.org'".into(),
            "DELETE FROM accounts WHERE email = 'v1@x.org'".into(),
            "SELECT COUNT(*) FROM accounts WHERE email = 'v1@x.org'".into(),
            "INSERT INTO accounts VALUES (301, 'new@x.org', 3)".into(),
            "COMMIT".into(),
            "BEGIN".into(),
            "INSERT INTO accounts VALUES (302, 'gone@x.org', 2)".into(),
            "ROLLBACK".into(),
            "INSERT INTO accounts VALUES (303, 'gone@x.org', 4)".into(),
        ],
    )
    .await;
    compare_full(
        sut,
        reference,
        tally,
        "after lookup transactions",
        "accounts",
    )
    .await;
    // One lookup row per non-NULL email, nothing else.
    let want = match run(reference, "SELECT COUNT(email) FROM accounts").await {
        Outcome::Rows(_, r) => r[0][0].clone().unwrap_or_default(),
        other => format!("{other:?}"),
    };
    let mut have = 0u64;
    for (i, (cl, _)) in e.clusters.iter().enumerate().take(2) {
        let schema = shard_schema(
            e.tenant,
            w.db_id,
            GroupId(LOOKUP_GROUP),
            ShardId(u32::try_from(i).unwrap() + 1),
        );
        let mut c = root(e, i, None).await;
        let n: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM `{schema}`.`__sprig_lk_accounts__email`"
            ))
            .await
            .unwrap_or_else(|x| panic!("lookup rows on {cl}: {x}"));
        have += n.unwrap_or(0);
    }
    println!("lookups: {have} lookup rows for {want} emails");
    if have.to_string() != want {
        tally
            .mismatches
            .push(format!("LOOKUP ROWS {have} for {want} emails"));
    }
}

/// `autocommit = 0` on a sharded tenant: statements open a coordinator
/// transaction that spans clusters; `COMMIT`, `ROLLBACK` and `SET
/// autocommit = 1` end it as on MariaDB. A second connection checks what
/// was committed.
async fn autocommit_off(
    e: &Env,
    sut: &mut Conn,
    reference: &mut Conn,
    tally: &mut Tally,
    shards: &[Sh],
) {
    let (a, b) = keys_by_cluster(e, shards);
    let (x, y, z) = (a[1], b[1], b[2]);
    both(
        sut,
        reference,
        tally,
        &[
            "SET autocommit = 0".into(),
            format!("UPDATE customers SET flag = 3 WHERE cid = {x}"),
            format!("UPDATE customers SET flag = 3 WHERE cid = {y}"),
            "SELECT cid, flag FROM customers WHERE flag = 3 ORDER BY cid".into(),
            "COMMIT".into(),
            format!("UPDATE customers SET flag = 4 WHERE cid IN ({x}, {y})"),
            "ROLLBACK".into(),
            format!("SELECT cid, flag FROM customers WHERE cid IN ({x}, {y}) ORDER BY cid"),
            format!("UPDATE customers SET name = 'ac' WHERE cid = {z}"),
            format!("INSERT INTO orders (oid, cid, pid, amount, qty, note) VALUES (3101, {x}, 1, 1.00, 1, 'ac'), (3102, {y}, 2, 2.00, 2, 'ac')"),
            "SET autocommit = 1".into(),
        ],
    )
    .await;
    let mut fresh = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    fresh.query_drop("SET NAMES utf8mb4").await.unwrap();
    compare_full(
        &mut fresh,
        reference,
        tally,
        "after autocommit = 0",
        "customers",
    )
    .await;
    compare_full(
        &mut fresh,
        reference,
        tally,
        "after autocommit = 0",
        "orders",
    )
    .await;
}

/// DDL inside a coordinator transaction commits it first, as MySQL does.
async fn ddl_in_txn(
    e: &Env,
    sut: &mut Conn,
    reference: &mut Conn,
    tally: &mut Tally,
    shards: &[Sh],
) {
    let (a, b) = keys_by_cluster(e, shards);
    let (x, y) = (a[2], b[3]);
    both(
        sut,
        reference,
        tally,
        &[
            "BEGIN".into(),
            format!("UPDATE customers SET flag = 6 WHERE cid IN ({x}, {y})"),
            "CREATE TABLE r1f_ddl_t (x INT)".into(),
            "ROLLBACK".into(),
            "DROP TABLE r1f_ddl_t".into(),
        ],
    )
    .await;
    let mut fresh = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    fresh.query_drop("SET NAMES utf8mb4").await.unwrap();
    compare_full(
        &mut fresh,
        reference,
        tally,
        "after DDL in a transaction",
        "customers",
    )
    .await;
}

/// A schema change dropping a column runs on the shards first and is
/// published afterwards (chapter 21, decision D8): reads of the whole
/// table meanwhile retry once after the change publishes, instead of
/// failing.
async fn drop_column_window(
    e: &Env,
    sut: &mut Conn,
    reference: &mut Conn,
    tally: &mut Tally,
) -> (usize, Vec<String>) {
    let mut rc = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let reader = tokio::spawn(async move {
        let mut errors = Vec::new();
        let mut n = 0usize;
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            n += 1;
            match run(&mut rc, "SELECT * FROM products ORDER BY pid").await {
                Outcome::Rows(..) => {}
                other => errors.push(format!("{other:?}")),
            }
        }
        (n, errors)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    both(
        sut,
        reference,
        tally,
        &["ALTER TABLE products DROP COLUMN extra".into()],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (n, errors) = reader.await.unwrap();
    compare_full(sut, reference, tally, "after DROP COLUMN", "products").await;
    (n, errors)
}

/// Splits group 901's first range into two shards on the other cluster
/// while point reads of its keys run, as chapter 20's split does: hold the
/// range, fence the source (rename), copy rows by hash, commit the new map,
/// drop the source. Returns the new shard list and the reads' errors.
async fn split(
    e: &Env,
    w: &mut World,
    sut_reads: Conn,
    at: usize,
    hold_ms: u64,
    mut under: Option<&mut Conn>,
) -> (Vec<Sh>, Vec<String>, Option<Outcome>) {
    let groups = layout();
    let g = &groups[0];
    let (mut m, rev) = w.store.load(e.tenant, GroupId(g.id)).await.unwrap();
    let src = m.ranges[at].clone();
    let lo = src.lo;
    let hi = m.ranges.get(at + 1).map_or(u64::MAX, |r| r.lo);
    let target = (0..e.clusters.len())
        .find(|i| e.cluster(*i).0 != src.cluster)
        .unwrap_or(0);
    let (n1, n2) = (m.next_shard_id, m.next_shard_id + 1);
    let mid = lo + (hi - lo) / 2;
    let src_schema = shard_schema(e.tenant, w.db_id, GroupId(g.id), src.shard);
    let new: Vec<(u32, String)> = [n1, n2]
        .iter()
        .map(|s| {
            (
                *s,
                shard_schema(e.tenant, w.db_id, GroupId(g.id), ShardId(*s)),
            )
        })
        .collect();
    let mut tc = root(e, target, None).await;
    for (_, s) in &new {
        tc.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
        tc.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
        // The mover copies the decision table into every child.
        tc.query_drop(sprig_catalog::ddl::dtx_table_sql(s))
            .await
            .unwrap();
        tc.query_drop(format!("USE `{s}`")).await.unwrap();
        for (t, _) in g.tables {
            tc.query_drop(ddl_of(t)).await.unwrap();
        }
        w.schemas.push((target, s.clone()));
    }
    // Point reads of the moving keys run through the cutover.
    let moving: Vec<i64> = (1..=120i64)
        .map(|k| if k % 17 == 0 { -k } else { k })
        .filter(|k| {
            let (h, _) = sprig_shard::hash_key(&g.spec, &[KeyValue::Int(i128::from(*k))]).unwrap();
            h.0 >= lo && h.0 < hi
        })
        .collect();
    // A transaction that wrote a moving key before the cutover: the move
    // rolls it back, and its next statement gets 39424.
    let under_key = moving.first().copied();
    if let (Some(u), Some(k)) = (under.as_deref_mut(), under_key) {
        u.query_drop("BEGIN").await.unwrap();
        u.query_drop(format!("UPDATE customers SET flag = 77 WHERE cid = {k}"))
            .await
            .unwrap();
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let keys = moving.clone();
    let reader = tokio::spawn(async move {
        let mut c = sut_reads;
        let mut errors = Vec::new();
        let mut n = 0usize;
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            let k = keys[n % keys.len().max(1)];
            n += 1;
            match run(
                &mut c,
                &format!("SELECT cid, name FROM customers WHERE cid = {k}"),
            )
            .await
            {
                // Keys the DML phase deleted on both sides answer no
                // rows; only an error is wrong here (the corpus checks
                // the rows afterwards).
                Outcome::Rows(..) => {}
                other => errors.push(format!("cid {k}: {other:?}")),
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        (n, errors)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Hold the range.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    m.ranges[at].state = RangeState::Held;
    m.ranges[at].job_id = Some("r1finish-split".into());
    m.ranges[at].hold_until_ms = Some(now + hold_ms);
    m.version += 1;
    let rev = w.store.compare_and_put(rev, &m).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Fence: the source's tables are renamed away.
    let src_idx = e
        .clusters
        .iter()
        .position(|(c, _)| *c == src.cluster)
        .unwrap();
    let mut sc = root(e, src_idx, None).await;
    if under.is_some() {
        // The revoke-and-kill fence: the tenant's open transactions on the
        // source end (their metadata locks would hold the rename).
        let threads: Vec<u64> = sc
            .query(format!(
                "SELECT t.trx_mysql_thread_id FROM information_schema.INNODB_TRX t \
                 JOIN information_schema.PROCESSLIST p ON p.ID = t.trx_mysql_thread_id \
                 WHERE p.USER LIKE 't{}\\_u%'",
                e.tenant
            ))
            .await
            .unwrap();
        for t in &threads {
            let _ = sc.query_drop(format!("KILL {t}")).await;
        }
    }
    for (t, _) in g.tables {
        sc.query_drop(format!(
            "RENAME TABLE `{src_schema}`.`{t}` TO `{src_schema}`.`__fenced_{t}`"
        ))
        .await
        .unwrap();
    }
    // Copy by hash.
    for (t, _) in g.tables {
        let rows: Vec<Row> = sc
            .query(format!("SELECT * FROM `{src_schema}`.`__fenced_{t}`"))
            .await
            .unwrap();
        for row in rows {
            let k: i64 = match *t {
                "orders" => row.get(1).unwrap(),
                _ => row.get(0).unwrap(),
            };
            let (h, _) = sprig_shard::hash_key(&g.spec, &[KeyValue::Int(i128::from(k))]).unwrap();
            let to = if h.0 < mid { &new[0].1 } else { &new[1].1 };
            let vals = row.unwrap();
            let marks = vec!["?"; vals.len()].join(", ");
            tc.exec_drop(format!("INSERT INTO `{to}`.`{t}` VALUES ({marks})"), vals)
                .await
                .unwrap();
        }
    }
    // The copy reached the target's replicas (reads may go there).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // Commit: the two new ranges on the target cluster.
    let mut next = m.clone();
    next.ranges.remove(at);
    for (lo, (s, _)) in [(lo, &new[0]), (mid, &new[1])].into_iter().rev() {
        next.ranges.insert(
            at,
            RangeEntry {
                lo,
                shard: ShardId(*s),
                cluster: e.cluster(target).0.clone(),
                epoch: src.epoch + 1,
                state: RangeState::Serving,
                job_id: Some("r1finish-split".into()),
                hold_until_ms: None,
            },
        );
    }
    next.version += 1;
    next.next_shard_id = n2 + 1;
    w.store.compare_and_put(rev, &next).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (n, errors) = reader.await.unwrap();
    let under_outcome = match (under, under_key) {
        (Some(u), Some(k)) => {
            let o = run(
                u,
                &format!("SELECT cid, flag FROM customers WHERE cid = {k}"),
            )
            .await;
            let _ = u.query_drop("ROLLBACK").await;
            Some(o)
        }
        _ => None,
    };
    println!(
        "split: {} keys moved to {}, {n} point reads during the cutover, {} errors",
        moving.len(),
        e.cluster(target).0,
        errors.len()
    );
    sc.query_drop(format!("DROP DATABASE `{src_schema}`"))
        .await
        .unwrap();
    let shards: Vec<Sh> = next
        .ranges
        .iter()
        .map(|r| Sh {
            id: r.shard.0,
            lo: r.lo,
            cluster: e
                .clusters
                .iter()
                .position(|(c, _)| *c == r.cluster)
                .unwrap(),
        })
        .collect();
    (shards, errors, under_outcome)
}

/// A branch killed inside a transaction spanning clusters: COMMIT fails
/// and neither cluster keeps the transaction's writes.
async fn kill_branch(e: &Env, sut: &mut Conn, tally: &mut Tally, shards: &[Sh]) -> bool {
    if e.clusters.len() < 2 {
        return false;
    }
    let (a, b) = keys_by_cluster(e, shards);
    let (x, y) = (a[0], b[0]);
    let before = run(
        sut,
        &format!("SELECT cid, flag FROM customers WHERE cid IN ({x}, {y}) ORDER BY cid"),
    )
    .await;
    sut.query_drop("BEGIN").await.unwrap();
    sut.query_drop(format!("UPDATE customers SET flag = 99 WHERE cid = {x}"))
        .await
        .unwrap();
    sut.query_drop(format!("UPDATE customers SET flag = 99 WHERE cid = {y}"))
        .await
        .unwrap();
    // The branch on the second cluster: the tenant's backend account's
    // connection with an open transaction.
    let mut c2 = root(e, 1, None).await;
    let threads: Vec<u64> = c2
        .query(format!(
            "SELECT t.trx_mysql_thread_id FROM information_schema.INNODB_TRX t \
             JOIN information_schema.PROCESSLIST p ON p.ID = t.trx_mysql_thread_id \
             WHERE p.USER LIKE 't{}\\_u%'",
            e.tenant
        ))
        .await
        .unwrap();
    for t in &threads {
        let _ = c2.query_drop(format!("KILL {t}")).await;
    }
    let commit = run(sut, "COMMIT").await;
    let after = run(
        sut,
        &format!("SELECT cid, flag FROM customers WHERE cid IN ({x}, {y}) ORDER BY cid"),
    )
    .await;
    println!(
        "kill: {} branch thread(s) killed on {}; COMMIT answered {commit:?}",
        threads.len(),
        e.cluster(1).0
    );
    tally.check("rows after a killed branch", Cmp::Ordered, &after, &before);
    !threads.is_empty() && matches!(commit, Outcome::Err(..))
}

#[tokio::test]
async fn differential_against_one_unsharded_mariadb() {
    let Some(e) = Env::load() else {
        eprintln!(
            "differential_live: set SPRIG_TEST_R1_GATEWAY, SPRIG_TEST_R1_TENANT_ID, SPRIG_TEST_R1_CLUSTERS and SPRIG_TEST_ETCD to run"
        );
        return;
    };
    let seed_value: u64 = env("SPRIG_TEST_R1_SEED")
        .and_then(|s| s.parse().ok())
        .unwrap_or(4242);
    let mut sut = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    let mut w = setup(&e, &mut sut).await;
    let mut reference = connect(
        &e.cluster(0).1,
        "root",
        &e.root_password,
        Some(&e.reference_db()),
    )
    .await;
    for c in [&mut sut, &mut reference] {
        c.query_drop("SET NAMES utf8mb4").await.unwrap();
    }
    let mut r = Rng(seed_value);
    let mut tally = Tally::default();
    let started = Instant::now();
    for s in seed(&mut r) {
        let a = run(&mut sut, &s).await;
        let b = run(&mut reference, &s).await;
        tally.check(&s, Cmp::Bag, &a, &b);
    }
    let corpus_now = corpus(&mut r);
    for (s, mode) in &corpus_now {
        let a = run(&mut sut, s).await;
        let b = run(&mut reference, s).await;
        tally.check(s, *mode, &a, &b);
    }
    let (ddl_have, ddl_of_n) =
        ddl_across_shards(&e, &w, &mut sut, &mut reference, &mut tally).await;
    if ddl_have != ddl_of_n {
        tally.mismatches.push(format!(
            "ALTER TABLE reached {ddl_have} of {ddl_of_n} product shards"
        ));
    }
    let reads_done = tally.compared;
    // Prepared statements (binary protocol).
    for (s, p, mode) in prepared(&mut r) {
        let a = run_exec(&mut sut, s, p.clone()).await;
        let b = run_exec(&mut reference, s, p).await;
        tally.check(s, mode, &a, &b);
    }
    for (s, p, table) in prepared_dml(&mut r) {
        let a = run_exec(&mut sut, s, p.clone()).await;
        let b = run_exec(&mut reference, s, p).await;
        tally.check(s, Cmp::Bag, &a, &b);
        compare_full(
            &mut sut,
            &mut reference,
            &mut tally,
            "after a prepared write",
            table,
        )
        .await;
    }
    let prepared_done = tally.compared;
    for (s, table) in dml(&mut r) {
        let a = run(&mut sut, &s).await;
        let b = run(&mut reference, &s).await;
        let skipped = matches!(a, Outcome::Err(39420, _));
        tally.check(&s, Cmp::Bag, &a, &b);
        if skipped && !matches!(b, Outcome::Err(..)) {
            resync(&mut sut, &mut reference, table).await;
        }
        compare_full(&mut sut, &mut reference, &mut tally, "after a write", table).await;
    }
    lookups(&e, &w, &mut sut, &mut reference, &mut tally).await;
    let shards0 = layout().remove(0).shards;
    transactions(&e, &mut sut, &mut reference, &mut tally, &shards0).await;
    autocommit_off(&e, &mut sut, &mut reference, &mut tally, &shards0).await;
    ddl_in_txn(&e, &mut sut, &mut reference, &mut tally, &shards0).await;
    // Inside a transaction: a multi-shard write rolled back leaves nothing.
    for c in [&mut sut, &mut reference] {
        c.query_drop("BEGIN").await.unwrap();
        c.query_drop("UPDATE orders SET qty = 99 WHERE note = 'a'")
            .await
            .unwrap();
    }
    let a = run(&mut sut, "SELECT COUNT(*) FROM orders WHERE qty = 99").await;
    let b = run(&mut reference, "SELECT COUNT(*) FROM orders WHERE qty = 99").await;
    tally.check(
        "in-transaction read of a multi-shard write",
        Cmp::Bag,
        &a,
        &b,
    );
    for c in [&mut sut, &mut reference] {
        c.query_drop("ROLLBACK").await.unwrap();
    }
    compare_full(
        &mut sut,
        &mut reference,
        &mut tally,
        "after ROLLBACK",
        "orders",
    )
    .await;
    let txn_done = tally.compared;
    // The map changes mid-run: a split onto the other cluster.
    let reads = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    // The first split holds long enough for reads to wait it out; the
    // second's hold expires before the cutover commits, so reads hit the
    // fenced source and retry after the map moves (stale-map retry).
    let mut under = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    let (_, split_errors, under_outcome) =
        split(&e, &mut w, reads, 0, 6000, Some(&mut under)).await;
    drop(under);
    for m in &split_errors {
        tally.mismatches.push(format!("READ DURING SPLIT {m}"));
    }
    println!("split under a transaction: its next statement answered {under_outcome:?}");
    tally.compared += 1;
    if !matches!(under_outcome, Some(Outcome::Err(39424, _))) {
        tally.mismatches.push(format!(
            "A SHARD MOVED UNDER A TRANSACTION answered {under_outcome:?}, not 39424"
        ));
    }
    let reads = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    let at = {
        let (m, _) = w.store.load(e.tenant, GroupId(901)).await.unwrap();
        m.ranges
            .iter()
            .position(|r| r.cluster == e.cluster(1).0)
            .unwrap_or(0)
    };
    let (shards1, split_errors, _) = split(&e, &mut w, reads, at, 200, None).await;
    for m in &split_errors {
        tally
            .mismatches
            .push(format!("READ DURING SPLIT (expired hold) {m}"));
    }
    for (s, mode) in &corpus_now {
        if s.contains("customers") || s.contains("orders") {
            let a = run(&mut sut, s).await;
            let b = run(&mut reference, s).await;
            tally.check(s, *mode, &a, &b);
        }
    }
    for (s, p, mode) in prepared(&mut Rng(seed_value + 1)) {
        let a = run_exec(&mut sut, s, p.clone()).await;
        let b = run_exec(&mut reference, s, p).await;
        tally.check(s, mode, &a, &b);
    }
    transactions(&e, &mut sut, &mut reference, &mut tally, &shards1).await;
    autocommit_off(&e, &mut sut, &mut reference, &mut tally, &shards1).await;
    compare_full(
        &mut sut,
        &mut reference,
        &mut tally,
        "after the split",
        "customers",
    )
    .await;
    compare_full(
        &mut sut,
        &mut reference,
        &mut tally,
        "after the split",
        "orders",
    )
    .await;
    // EXPLAIN FORMAT=SPRIG answers.
    match run(
        &mut sut,
        "EXPLAIN FORMAT=SPRIG SELECT cid, name FROM customers ORDER BY name LIMIT 20 OFFSET 40",
    )
    .await
    {
        Outcome::Rows(_, rows) => {
            let ops: Vec<_> = rows
                .iter()
                .map(|r| r[2].clone().unwrap_or_default())
                .collect();
            assert_eq!(ops, ["Limit", "MergeSort", "Route", "Shard"], "{rows:?}");
        }
        other => panic!("EXPLAIN FORMAT=SPRIG: {other:?}"),
    }
    let (drop_reads, drop_errors) =
        drop_column_window(&e, &mut sut, &mut reference, &mut tally).await;
    println!(
        "drop column: {drop_reads} reads of the whole table during it, {} errors",
        drop_errors.len()
    );
    for m in &drop_errors {
        tally
            .mismatches
            .push(format!("READ DURING DROP COLUMN {m}"));
    }
    let killed = if env("SPRIG_TEST_R1_KILL").is_some() {
        kill_branch(&e, &mut sut, &mut tally, &shards1).await
    } else {
        false
    };
    let secs = started.elapsed().as_secs_f64();
    println!(
        "differential: {} statements compared ({} seed and reads, {} prepared, {} writes and transactions, {} after the split), {} skipped as not distributable ({:.1}%), {} mismatches, {secs:.1} s",
        tally.compared,
        reads_done,
        prepared_done - reads_done,
        txn_done - prepared_done,
        tally.compared - txn_done,
        tally.skipped,
        100.0 * tally.skipped as f64 / tally.compared.max(1) as f64,
        tally.mismatches.len()
    );
    println!("skipped by reason: {:?}", tally.reasons);
    println!("ddl: ALTER TABLE reached {ddl_have} of {ddl_of_n} shards");
    if env("SPRIG_TEST_R1_KILL").is_some() {
        println!("kill test: COMMIT failed after a killed branch: {killed}");
    }
    for m in &tally.mismatches {
        println!("{m}");
    }
    if env("SPRIG_TEST_R1_KEEP").is_none() {
        teardown(&e, &w, &mut sut).await;
    }
    drop(sut);
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches",
        tally.mismatches.len()
    );
}

/// Single-shard latency (chapter 17: pass-through adds at most 50 µs at
/// p50 over R0 routing). Point selects: direct to the backend, through the
/// gateway on an unsharded table, through the gateway on a sharded table
/// whose shard is on the session's cluster, and as prepared statements.
#[tokio::test]
async fn single_shard_latency() {
    let Some(e) = Env::load() else {
        eprintln!("single_shard_latency: set the SPRIG_TEST_R1_* variables to run");
        return;
    };
    let Some(n) = env("SPRIG_TEST_R1_LATENCY").and_then(|s| s.parse::<usize>().ok()) else {
        eprintln!("single_shard_latency: set SPRIG_TEST_R1_LATENCY=<statements per mode> to run");
        return;
    };
    let mut gw = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    let w = setup(&e, &mut gw).await;
    gw.query_drop("DROP TABLE IF EXISTS plain_lat")
        .await
        .unwrap();
    gw.query_drop("CREATE TABLE plain_lat (cid INT PRIMARY KEY, name VARCHAR(40))")
        .await
        .unwrap();
    let vals: Vec<String> = (1..=1000).map(|i| format!("({i}, 'n{i}')")).collect();
    gw.query_drop(format!("INSERT INTO plain_lat VALUES {}", vals.join(", ")))
        .await
        .unwrap();
    gw.query_drop(format!(
        "INSERT INTO customers (cid, name) VALUES {}",
        vals.join(", ")
    ))
    .await
    .unwrap();
    // Keys whose shard is on the session's (first) cluster.
    let shards = layout().remove(0).shards;
    let local: Vec<i64> = (1..=1000)
        .filter(|k| cluster_of(&e, &shards, *k) == 0)
        .collect();
    let mut direct = connect(
        &e.cluster(0).1,
        "root",
        &e.root_password,
        Some(&w.default_schema),
    )
    .await;
    async fn measure(c: &mut Conn, n: usize, q: impl Fn(usize) -> String) -> (f64, f64) {
        for i in 0..200 {
            let _: Vec<Row> = c.query(q(i)).await.unwrap();
        }
        let mut t = Vec::with_capacity(n);
        for i in 0..n {
            let s = q(i);
            let start = Instant::now();
            let rows: Vec<Row> = c.query(s).await.unwrap();
            t.push(start.elapsed().as_secs_f64() * 1e6);
            assert_eq!(rows.len(), 1);
        }
        t.sort_by(f64::total_cmp);
        (t[n / 2], t[n * 99 / 100])
    }
    let d = measure(&mut direct, n, |i| {
        format!("SELECT * FROM plain_lat WHERE cid = {}", 1 + i % 1000)
    })
    .await;
    let u = measure(&mut gw, n, |i| {
        format!("SELECT * FROM plain_lat WHERE cid = {}", 1 + i % 1000)
    })
    .await;
    // The same rows read directly from their shard schemas (the backend's
    // own cost of the sharded statement).
    let spec = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    let shard_of = |k: i64| {
        let (h, _) = sprig_shard::hash_key(&spec, &[KeyValue::Int(i128::from(k))]).unwrap();
        let i = shards.partition_point(|s| s.lo <= h.0).saturating_sub(1);
        shard_schema(e.tenant, w.db_id, GroupId(901), ShardId(shards[i].id))
    };
    let direct_sharded: Vec<String> = local
        .iter()
        .map(|k| format!("SELECT * FROM `{}`.customers WHERE cid = {k}", shard_of(*k)))
        .collect();
    let ds = measure(&mut direct, n, |i| {
        direct_sharded[i % direct_sharded.len()].clone()
    })
    .await;
    let s = measure(&mut gw, n, |i| {
        format!(
            "SELECT * FROM customers WHERE cid = {}",
            local[i % local.len()]
        )
    })
    .await;
    // Prepared: one statement, executed n times.
    let stmt = gw
        .prep("SELECT * FROM customers WHERE cid = ?")
        .await
        .unwrap();
    let mut t = Vec::with_capacity(n);
    for i in 0..n + 200 {
        let start = Instant::now();
        let rows: Vec<Row> = gw.exec(&stmt, (local[i % local.len()],)).await.unwrap();
        if i >= 200 {
            t.push(start.elapsed().as_secs_f64() * 1e6);
        }
        assert_eq!(rows.len(), 1);
    }
    t.sort_by(f64::total_cmp);
    let p = (t[n / 2], t[n * 99 / 100]);
    println!("latency p50/p99 in µs over {n} point selects:");
    println!("  direct to MariaDB:            {:.0} / {:.0}", d.0, d.1);
    println!(
        "  gateway, unsharded table:     {:.0} / {:.0}  (+{:.0} µs p50)",
        u.0,
        u.1,
        u.0 - d.0
    );
    println!("  direct, the shard's table:    {:.0} / {:.0}", ds.0, ds.1);
    println!(
        "  gateway, sharded, one shard:  {:.0} / {:.0}  (+{:.0} µs p50 over direct to the shard; the unsharded path adds {:.0}; the router's share {:.0})",
        s.0,
        s.1,
        s.0 - ds.0,
        u.0 - d.0,
        (s.0 - ds.0) - (u.0 - d.0)
    );
    println!(
        "  gateway, sharded, prepared:   {:.0} / {:.0}  (+{:.0} µs p50 over direct text to the shard)",
        p.0,
        p.1,
        p.0 - ds.0
    );
    // Interleaved: per iteration, the same four reads back to back (direct
    // and through the gateway, unsharded and on one shard), so load on a
    // shared machine hits all four alike. The router's share is the
    // sharded path's extra over direct minus the unsharded path's.
    let mut share = Vec::with_capacity(n);
    let mut extra = Vec::with_capacity(n);
    for i in 0..n {
        let k = local[i % local.len()];
        let plain = format!("SELECT * FROM plain_lat WHERE cid = {}", 1 + i % 1000);
        let qs = [
            plain.clone(),
            plain,
            direct_sharded[i % direct_sharded.len()].clone(),
            format!("SELECT * FROM customers WHERE cid = {k}"),
        ];
        let mut t = [0f64; 4];
        for (j, q) in qs.into_iter().enumerate() {
            let c = if j % 2 == 0 { &mut direct } else { &mut gw };
            let start = Instant::now();
            let rows: Vec<Row> = c.query(q).await.unwrap();
            t[j] = start.elapsed().as_secs_f64() * 1e6;
            assert_eq!(rows.len(), 1);
        }
        extra.push(t[3] - t[1]);
        share.push((t[3] - t[2]) - (t[1] - t[0]));
    }
    share.sort_by(f64::total_cmp);
    extra.sort_by(f64::total_cmp);
    println!(
        "  interleaved: router's share p50 {:.0} µs; sharded minus unsharded through the gateway p50 {:.0} µs",
        share[n / 2],
        extra[n / 2]
    );
    gw.query_drop("DROP TABLE plain_lat").await.unwrap();
    teardown(&e, &w, &mut gw).await;
}
