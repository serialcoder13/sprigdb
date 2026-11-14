//! Online re-key, live:
//! a table group keyed by `store_id` is re-keyed to `customer_id` while
//! writers keep inserting, updating, moving and deleting rows.
//!
//! One tenant (id 8,930,699,001: in a private deployment's range, port
//! 49306 × 100,000 + 4,000,000,000) on the Compose single node 13301, in a
//! private DC tier `r3crekey` of a private etcd (49379), so no other worker claims
//! its jobs:
//!
//! 1. group 7 (`orders`, `order_items`, `events`, all keyed by `store_id`)
//!    on 3 shards of one cluster, `_sprig_rk` filled;
//! 2. four writers route by what a gateway reads (the catalog's key, and
//!    the members' next routing once every range of the map is a new
//!    shard), wait out holds, retry fenced tables, and log every
//!    acknowledged write; a quarter of their updates change an order's
//!    `customer_id` (a key change under the new key);
//! 3. `ShardingLoop::rekey` (what `sprigctl shard rekey` calls) with 4 new
//!    shards; the mover worker copies, catches up, verifies and cuts over;
//!    `rekey_follow` finishes the catalog;
//! 4. checks: every acknowledged row is on exactly one serving shard, the
//!    one its `customer_id` routes to, with `_sprig_rk` equal to that hash
//!    and its last acknowledged values; nothing lost, duplicated, extra or
//!    misplaced; the hold stayed within `mover.cutover_max_hold_ms`;
//! 5. a second re-key (back to `store_id`) cancelled before its cutover is
//!    undone: the group keeps `customer_id` and its next routing is gone.
//!
//! ```sh
//! SPRIG_TEST_REKEY_LIVE=1 SPRIG_TEST_ETCD=http://127.0.0.1:49379 \
//!   cargo test -p sprig-controller --test live_rekey -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::type_complexity,
    clippy::single_match_else,
    clippy::cast_precision_loss,
    clippy::similar_names,
    clippy::too_many_arguments
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value};
use sprig_balancer::decisions::{DecisionLog, DecisionStore, NewDecision, Outcome};
use sprig_catalog::placement::ShardMap as TenantMap;
use sprig_cluster::TopologyKind;
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, NodeId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_common::{CancellationToken, Result as SResult};
use sprig_controller::sharding::{
    ClusterPools, FixedSettings, RekeyRequest, ShardingLoop, TenantSettings,
};
use sprig_meta::{EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::{ClusterAccess, group_env};
use sprig_mover::{
    EnvFactory, JobEnv, JobRecord, JobRuntime, JobStore, MoverSettings, ShardRoute, WorkerOpts,
};
use sprig_shard::shardmap::RangeState;
use sprig_shard::{KeyClass, KeySpec, ShardMap, ShardMapCache, ShardMapStore};

const TENANT: TenantId = TenantId(8_930_699_001);
const DB: DbId = DbId(1);
const GROUP: GroupId = GroupId(7);
const URL: &str = "mysql://root:sprig-dev-root@127.0.0.1:13301";
const WRITERS: u32 = 4;
const TABLES: [(&str, u64); 3] = [("orders", 1), ("order_items", 2), ("events", 3)];

fn pool() -> Pool {
    Pool::new(
        OptsBuilder::from_opts(Opts::from_url(URL).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 24).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

fn catalog() -> String {
    SchemaName::catalog(TENANT).to_string()
}

fn schema(shard: u32) -> String {
    sprig_shard::shard_schema(TENANT, DB, GROUP, ShardId(shard))
}

fn now_ms() -> u64 {
    sprig_mover::record::wall_ms(SystemTime::now())
}

fn int() -> KeySpec {
    KeySpec::new(vec![KeyClass::Numeric]).unwrap()
}

fn rk(v: i64) -> u64 {
    sprig_shard::rk::hash_values(&int(), &[Value::Int(v)]).unwrap()
}

// ---- the environment -------------------------------------------------------

struct Pools(Pool);

#[async_trait::async_trait]
impl ClusterPools for Pools {
    async fn pool(&self, _cluster: &ClusterId) -> SResult<Pool> {
        Ok(self.0.clone())
    }
}

struct CatalogLog(Pool);

#[async_trait::async_trait]
impl DecisionLog for CatalogLog {
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> SResult<u64> {
        let mut c = self
            .0
            .get_conn()
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        DecisionStore::for_tenant(tenant).insert(&mut c, d).await
    }

    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> SResult<()> {
        let mut c = self
            .0
            .get_conn()
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        DecisionStore::for_tenant(tenant)
            .set_outcome(&mut c, id, o, None, at_us)
            .await
    }
}

struct Factory {
    access: ClusterAccess,
    meta: Arc<dyn MetaStore>,
    settings: MoverSettings,
}

#[async_trait::async_trait]
impl EnvFactory for Factory {
    async fn env(&self, rec: &JobRecord) -> SResult<JobEnv> {
        let route = ShardRoute::from_spec(&rec.spec)?
            .ok_or_else(|| sprig_common::SprigError::internal("not a hash-range job"))?;
        group_env(
            self.access.clone(),
            self.access.clone(),
            rec,
            route,
            self.meta.clone(),
            self.settings.server_id_base,
        )
    }

    fn settings(&self, rec: &JobRecord) -> MoverSettings {
        let mut s = self.settings.clone();
        if let Some(ms) = rec.spec.retain_ms {
            s.source_retain = Duration::from_millis(ms);
        }
        s
    }
}

fn access(cluster: &ClusterId) -> ClusterAccess {
    let ep = sprig_cluster::NodeEndpoint::from_url(NodeId::new("r3c-n1").unwrap(), URL).unwrap();
    ClusterAccess {
        cluster: cluster.clone(),
        topology: TopologyKind::SingleNode,
        writer: ep.clone(),
        nodes: vec![ep],
        repl: sprig_binlog::BinlogSource::from_url(URL).unwrap(),
        domain: None,
    }
}

// ---- setup and cleanup -----------------------------------------------------

/// Drops this tenant's schemas (by the exact tenant id) and the private
/// tier's keys.
async fn cleanup(p: &Pool, dc: &Arc<dyn MetaStore>) {
    let mut c = p.get_conn().await.unwrap();
    let names: Vec<String> = c
        .query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
        .await
        .unwrap();
    for n in names {
        if n.parse::<SchemaName>().is_ok_and(|s| s.tenant() == TENANT) {
            c.query_drop(format!("DROP DATABASE IF EXISTS `{n}`"))
                .await
                .unwrap();
        }
    }
    let mut keys = Vec::new();
    for pre in ["shardmaps/", "mover/", "feed/"] {
        let prefix = dc.tier().key(pre).unwrap();
        let mut opts = ListOpts::default();
        loop {
            let page = dc.list(&prefix, opts.clone()).await.unwrap();
            keys.extend(page.kvs.iter().map(|kv| kv.key.clone()));
            if !page.more {
                break;
            }
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
    for k in keys {
        let _ = dc
            .txn(Txn {
                when: vec![],
                then: vec![Op::Delete(k)],
                otherwise: vec![],
            })
            .await;
    }
}

const DDL: [&str; 3] = [
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, store_id BIGINT NOT NULL, \
     customer_id BIGINT NOT NULL, amount INT NOT NULL, note VARCHAR(200) NOT NULL, \
     _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk), KEY (customer_id))",
    "CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, \
     store_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, qty INT NOT NULL, \
     pad VARCHAR(100) NOT NULL, _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk), \
     KEY (order_id))",
    "CREATE TABLE events (id BIGINT PRIMARY KEY, store_id BIGINT NOT NULL, \
     customer_id BIGINT NOT NULL, kind VARCHAR(20) NOT NULL, payload VARCHAR(200) NOT NULL, \
     _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk))",
];

async fn setup(p: &Pool, dc: &Arc<dyn MetaStore>, cluster: &ClusterId) {
    let mut c = p.get_conn().await.unwrap();
    let cat = catalog();
    c.query_drop(format!("CREATE DATABASE `{cat}`"))
        .await
        .unwrap();
    sprig_catalog::Migrator::new(&[
        sprig_balancer::DECISIONS_MODULE,
        sprig_shard::groups::SHARDING_MODULE,
    ])
    .unwrap()
    .run(
        &mut c,
        &cat,
        &sprig_catalog::migrate::MigrateOpts::default(),
    )
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{cat}`.`databases` (db_id, name, name_key, default_charset, default_collation, \
         default_group_id, state, options, created_at, changed_version) VALUES \
         (1, 'shop', 'shop', 'utf8mb4', 'utf8mb4_uca1400_ai_ci', 1, 'active', '{{}}', UTC_TIMESTAMP(6), 1)"
    ))
    .await
    .unwrap();
    for (name, id) in TABLES {
        c.exec_drop(
            format!(
                "INSERT INTO `{cat}`.objects (object_id, db_id, namespace, name, name_key, kind, state, \
                 schema_version, created_at, changed_version) VALUES (?, 1, 'relation', ?, ?, 'table', \
                 'active', 1, UTC_TIMESTAMP(6), 1)"
            ),
            (id, name, name),
        )
        .await
        .unwrap();
        c.exec_drop(
            format!(
                "INSERT INTO `{cat}`.group_members (object_id, group_id, db_id, routing, key_columns, \
                 rk_state, joined_at) VALUES (?, ?, 1, 'columns', '[\"store_id\"]', 'ready', \
                 UTC_TIMESTAMP(6))"
            ),
            (id, GROUP.0),
        )
        .await
        .unwrap();
    }
    c.exec_drop(
        format!(
            "INSERT INTO `{cat}`.table_groups (group_id, kind, name, key_spec, key_spec_digest, state, \
             origin, created_at, updated_at) VALUES (?, 'sharded', 'orders', ?, ?, 'active', 'pinned', \
             UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
        ),
        (GROUP.0, int().to_json().to_string(), int().digest),
    )
    .await
    .unwrap();
    for s in 1..=3 {
        let sc = schema(s);
        c.query_drop(format!("CREATE DATABASE `{sc}` CHARACTER SET utf8mb4"))
            .await
            .unwrap();
        c.query_drop(format!("USE `{sc}`")).await.unwrap();
        for d in DDL {
            c.query_drop(d).await.unwrap();
        }
    }
    // The tenant map (its catalog's cluster) and group 7's map: 3 shards.
    let key = TenantMap::catalog_key(dc.tier(), TENANT).unwrap();
    dc.txn(Txn {
        when: vec![],
        then: vec![Op::Put {
            key,
            value: Record::new(TenantMap::catalog(TENANT, cluster.clone(), 1))
                .encode()
                .unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    })
    .await
    .unwrap();
    let mut m = ShardMap::presplit(
        TENANT,
        GROUP,
        int().digest,
        3,
        std::slice::from_ref(cluster),
    )
    .unwrap();
    m.tenant_epoch = 1;
    ShardMapStore::new(dc.clone()).create(&m).await.unwrap();
}

// ---- how writers route ----------------------------------------------------

/// What a gateway reads: the catalog's key column per table, the members'
/// next routing (column, first new shard), and the map.
#[derive(Default, Clone)]
struct Routing {
    column: BTreeMap<String, String>,
    next: BTreeMap<String, (String, u32)>,
}

struct View {
    cache: ShardMapCache,
    routing: Mutex<Routing>,
}

impl View {
    async fn refresh(&self, c: &mut Conn) {
        let cat = catalog();
        let rows: Vec<(String, Option<String>, Option<String>)> = c
            .query(format!(
                "SELECT o.name, CAST(m.key_columns AS CHAR), CAST(m.next_routing AS CHAR) \
                 FROM `{cat}`.group_members m JOIN `{cat}`.objects o ON o.object_id = m.object_id"
            ))
            .await
            .unwrap_or_default();
        if rows.is_empty() {
            return;
        }
        let mut r = Routing::default();
        for (name, cols, next) in rows {
            let col: Vec<String> = serde_json::from_str(cols.as_deref().unwrap_or("[]")).unwrap();
            r.column.insert(name.clone(), col[0].clone());
            if let Some(n) = next {
                let v: serde_json::Value = serde_json::from_str(&n).unwrap();
                let c: Vec<String> = serde_json::from_value(v["key_columns"].clone()).unwrap();
                let from = u32::try_from(v["shard_from"].as_u64().unwrap()).unwrap();
                r.next.insert(name, (c[0].clone(), from));
            }
        }
        *self.routing.lock().unwrap() = r;
    }

    /// The column `table` routes by now, the map, and whether a hold is on.
    fn route(&self, table: &str) -> Option<(String, ShardMap, bool)> {
        let map = self.cache.get(TENANT, GROUP).ok()?;
        let r = self.routing.lock().unwrap().clone();
        let mut col = r.column.get(table)?.clone();
        if let Some((c, from)) = r.next.get(table)
            && map.ranges.iter().all(|x| x.shard.0 >= *from)
        {
            c.clone_into(&mut col);
        }
        let now = now_ms();
        let held = map
            .ranges
            .iter()
            .any(|x| x.state == RangeState::Held && x.hold_until_ms.is_some_and(|u| now < u));
        Some((col, (*map).clone(), held))
    }
}

/// One logical row: (store, customer, the other values).
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowV {
    store: i64,
    customer: i64,
    vals: Vec<String>,
}

#[derive(Default)]
struct Log {
    rows: BTreeMap<(String, i64), Option<RowV>>,
    uncertain: BTreeMap<(String, i64), Vec<Option<RowV>>>,
    ops: u64,
    key_moves: u64,
    max_pause: Duration,
}

fn is_retry(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if matches!(s.code, 1146 | 1049 | 1142 | 1044 | 1213 | 1205 | 1054))
}

/// A write: table, id, the row after it (`None`: deleted).
type Write = (&'static str, i64, Option<RowV>);

/// The statements for `writes` (deletes everywhere the key could be,
/// inserts on the shard the row routes to) under the current routing.
fn statements(view: &View, writes: &[Write]) -> Option<(Vec<(String, Vec<Value>)>, bool)> {
    let mut out = Vec::new();
    let mut held = false;
    for (table, id, row) in writes {
        let (col, map, h) = view.route(table)?;
        held |= h;
        // A key can be on any serving shard after a key change: delete it
        // from all of them (one server, one transaction), then insert it.
        for r in &map.ranges {
            out.push((
                format!("DELETE FROM `{}`.`{table}` WHERE id = ?", schema(r.shard.0)),
                vec![Value::Int(*id)],
            ));
        }
        if let Some(v) = row {
            let k = if col == "customer_id" {
                v.customer
            } else {
                v.store
            };
            let h = rk(k);
            let shard = map.route(sprig_shard::RoutingHash(h)).shard.0;
            let (cols, n) = match *table {
                "orders" => ("id, store_id, customer_id, amount, note", 5),
                "order_items" => ("id, order_id, store_id, customer_id, qty, pad", 6),
                _ => ("id, store_id, customer_id, kind, payload", 5),
            };
            let mut params: Vec<Value> = vec![Value::Int(*id)];
            match *table {
                "order_items" => {
                    params.push(Value::Bytes(v.vals[0].clone().into_bytes()));
                    params.push(Value::Int(v.store));
                    params.push(Value::Int(v.customer));
                    params.push(Value::Bytes(v.vals[1].clone().into_bytes()));
                    params.push(Value::Bytes("p".repeat(80).into_bytes()));
                }
                _ => {
                    params.push(Value::Int(v.store));
                    params.push(Value::Int(v.customer));
                    params.push(Value::Bytes(v.vals[0].clone().into_bytes()));
                    params.push(Value::Bytes(v.vals[1].clone().into_bytes()));
                }
            }
            params.push(Value::UInt(h));
            out.push((
                format!(
                    "INSERT INTO `{}`.`{table}` ({cols}, _sprig_rk) VALUES ({}, ?)",
                    schema(shard),
                    vec!["?"; n].join(", ")
                ),
                params,
            ));
        }
    }
    Some((out, held))
}

async fn run_txn(c: &mut Conn, stmts: &[(String, Vec<Value>)]) -> Result<(), mysql_async::Error> {
    c.query_drop("BEGIN").await?;
    for (q, p) in stmts {
        if let Err(e) = c.exec_drop(q.as_str(), p.clone()).await {
            let _ = c.query_drop("ROLLBACK").await;
            return Err(e);
        }
    }
    c.query_drop("COMMIT").await
}

#[allow(clippy::needless_pass_by_value)]
async fn writer(
    me: u32,
    p: Pool,
    view: Arc<View>,
    log: Arc<Mutex<Log>>,
    stop: CancellationToken,
    paused: Arc<AtomicBool>,
    pace: Duration,
) {
    let mut c = p.get_conn().await.unwrap();
    let base = i64::from(me) * 1_000_000_000;
    let mut seq: i64 = 0;
    // order id -> (store, customer, amount, item ids)
    let mut orders: BTreeMap<i64, (i64, i64, i64, Vec<i64>)> = BTreeMap::new();
    while !stop.is_cancelled() {
        if paused.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        tokio::time::sleep(pace).await;
        seq += 1;
        let store = (seq * 7 + i64::from(me)) % 40;
        let customer = (seq * 13 + i64::from(me) * 3) % 500;
        let op = seq % 10;
        let mut writes: Vec<Write> = Vec::new();
        let mut key_move = false;
        let pick = |orders: &BTreeMap<i64, (i64, i64, i64, Vec<i64>)>| -> Option<i64> {
            if orders.is_empty() {
                return None;
            }
            let i = usize::try_from(seq).unwrap() % orders.len();
            orders.keys().nth(i).copied()
        };
        match op {
            0..=3 => {
                let o = base + seq;
                let items = vec![base + seq * 10 + 500_000_000, base + seq * 10 + 500_000_001];
                writes.push((
                    "orders",
                    o,
                    Some(RowV {
                        store,
                        customer,
                        vals: vec!["100".into(), "n".repeat(150)],
                    }),
                ));
                for it in &items {
                    writes.push((
                        "order_items",
                        *it,
                        Some(RowV {
                            store,
                            customer,
                            vals: vec![o.to_string(), "1".into()],
                        }),
                    ));
                }
                orders.insert(o, (store, customer, 100, items));
            }
            4 | 5 => {
                writes.push((
                    "events",
                    base + seq,
                    Some(RowV {
                        store,
                        customer,
                        vals: vec!["click".into(), "x".repeat(120)],
                    }),
                ));
            }
            6 | 7 => {
                let Some(o) = pick(&orders) else { continue };
                let e = orders.get_mut(&o).unwrap();
                e.2 += 7;
                writes.push((
                    "orders",
                    o,
                    Some(RowV {
                        store: e.0,
                        customer: e.1,
                        vals: vec![e.2.to_string(), "n".repeat(150)],
                    }),
                ));
            }
            8 => {
                // A key change under the new key: the order and its items
                // get another customer.
                let Some(o) = pick(&orders) else { continue };
                let e = orders.get_mut(&o).unwrap();
                e.1 = (e.1 + 101) % 500;
                key_move = true;
                writes.push((
                    "orders",
                    o,
                    Some(RowV {
                        store: e.0,
                        customer: e.1,
                        vals: vec![e.2.to_string(), "n".repeat(150)],
                    }),
                ));
                for it in &e.3 {
                    writes.push((
                        "order_items",
                        *it,
                        Some(RowV {
                            store: e.0,
                            customer: e.1,
                            vals: vec![o.to_string(), "1".into()],
                        }),
                    ));
                }
            }
            _ => {
                let Some(o) = pick(&orders) else { continue };
                let (_, _, _, items) = orders.remove(&o).unwrap();
                writes.push(("orders", o, None));
                for it in items {
                    writes.push(("order_items", it, None));
                }
            }
        }
        let started = Instant::now();
        let mut met_pause = false;
        loop {
            if stop.is_cancelled() {
                return;
            }
            let Some((stmts, held)) = statements(&view, &writes) else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            if held {
                met_pause = true;
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            match run_txn(&mut c, &stmts).await {
                Ok(()) => {
                    let mut l = log.lock().unwrap();
                    for (t, id, row) in &writes {
                        let k = ((*t).to_owned(), *id);
                        l.rows.insert(k.clone(), row.clone());
                        l.uncertain.remove(&k);
                    }
                    l.ops += 1;
                    if key_move {
                        l.key_moves += 1;
                    }
                    if met_pause {
                        let lat = started.elapsed();
                        l.max_pause = l.max_pause.max(lat);
                    }
                    break;
                }
                Err(e) if is_retry(&e) => {
                    met_pause = true;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(mysql_async::Error::Server(s)) => {
                    panic!("writer {me}: {s:?}");
                }
                Err(e) => {
                    {
                        let mut l = log.lock().unwrap();
                        for (t, id, row) in &writes {
                            let k = ((*t).to_owned(), *id);
                            let prev = l.rows.get(&k).cloned().flatten();
                            l.uncertain
                                .entry(k)
                                .or_insert_with(|| vec![prev])
                                .push(row.clone());
                        }
                    }
                    eprintln!("writer {me}: {e}; reconnecting");
                    c = p.get_conn().await.unwrap();
                    break;
                }
            }
        }
    }
}

// ---- the check -------------------------------------------------------------

fn text(v: &Value) -> String {
    match v {
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        _ => String::new(),
    }
}

/// Every acknowledged row on exactly the serving shard its `key` column
/// routes to, with `_sprig_rk` equal to that hash.
async fn check(p: &Pool, cache: &ShardMapCache, log: &Log, key: &str, label: &str) -> String {
    let mut c = p.get_conn().await.unwrap();
    let map = cache.get(TENANT, GROUP).unwrap();
    let mut summary = Vec::new();
    for (t, _) in TABLES {
        let mut have: BTreeMap<i64, RowV> = BTreeMap::new();
        let (mut dup, mut misplaced, mut bad_rk) = (0, 0, 0);
        for (i, r) in map.ranges.iter().enumerate() {
            let (lo, hi) = (r.lo, map.hi(i));
            let vals = match t {
                "order_items" => "CAST(order_id AS CHAR), CAST(qty AS CHAR)",
                "orders" => "CAST(amount AS CHAR), note",
                _ => "kind, payload",
            };
            let rows: Vec<mysql_async::Row> = c
                .query(format!(
                    "SELECT id, store_id, customer_id, {vals}, CAST(_sprig_rk AS CHAR) FROM `{}`.`{t}`",
                    schema(r.shard.0)
                ))
                .await
                .unwrap();
            for row in rows {
                let v: Vec<Value> = row.unwrap();
                let id: i64 = text(&v[0]).parse().unwrap();
                let store: i64 = text(&v[1]).parse().unwrap();
                let customer: i64 = text(&v[2]).parse().unwrap();
                let h = rk(if key == "customer_id" {
                    customer
                } else {
                    store
                });
                if h < lo || hi.is_some_and(|x| h >= x) {
                    misplaced += 1;
                }
                if text(&v[5]).parse::<u64>().ok() != Some(h) {
                    bad_rk += 1;
                }
                let rv = RowV {
                    store,
                    customer,
                    vals: vec![text(&v[3]), text(&v[4])],
                };
                if have.insert(id, rv).is_some() {
                    dup += 1;
                }
            }
        }
        let (mut lost, mut wrong, mut extra) = (0, 0, 0);
        for ((tt, id), want) in &log.rows {
            if tt != t {
                continue;
            }
            let got = have.get(id).cloned();
            let ok = got == *want
                || log
                    .uncertain
                    .get(&(tt.clone(), *id))
                    .is_some_and(|a| a.contains(&got));
            if !ok {
                if got.is_none() {
                    lost += 1;
                } else {
                    wrong += 1;
                }
                if lost + wrong < 4 {
                    eprintln!("{label}: {t} {id}: have {got:?}, log {want:?}");
                }
            }
        }
        for id in have.keys() {
            if !log.rows.contains_key(&(t.to_owned(), *id)) {
                extra += 1;
            }
        }
        assert_eq!(
            (lost, wrong, extra, dup, misplaced, bad_rk),
            (0, 0, 0, 0, 0, 0),
            "{label}: {t}: lost {lost}, wrong {wrong}, extra {extra}, duplicated {dup}, \
             misplaced {misplaced}, wrong _sprig_rk {bad_rk}"
        );
        summary.push(format!("{t} {}", have.len()));
    }
    format!(
        "{label}: rows by {key} on shards {:?}: {} (0 lost, wrong, extra, duplicated, misplaced)",
        map.ranges.iter().map(|r| r.shard.0).collect::<Vec<_>>(),
        summary.join(", ")
    )
}

fn snapshot(log: &Mutex<Log>) -> Log {
    let l = log.lock().unwrap();
    Log {
        rows: l.rows.clone(),
        uncertain: l.uncertain.clone(),
        ops: l.ops,
        key_moves: l.key_moves,
        max_pause: l.max_pause,
    }
}

// ---- the test ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_group_is_rekeyed_online_under_writes() {
    if std::env::var("SPRIG_TEST_REKEY_LIVE").is_err() {
        eprintln!("skipped: set SPRIG_TEST_REKEY_LIVE=1 with the Compose stack and r3c's etcd up");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "sprig_mover=info,sprig_controller=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
    let etcd = std::env::var("SPRIG_TEST_ETCD").unwrap_or_else(|_| "http://127.0.0.1:49379".into());
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(
            Tier::Dc(DcId::new("r3crekey").unwrap()),
            &EtcdConfig::new(vec![etcd]),
        )
        .await
        .unwrap(),
    );
    let cluster = ClusterId::new("r3crekey-c1").unwrap();
    let p = pool();
    cleanup(&p, &dc).await;
    setup(&p, &dc, &cluster).await;

    let ms = MoverSettings {
        copy_chunk_rows: 500,
        ready_sustain: Duration::from_secs(2),
        cutover_retry: Duration::from_secs(1),
        server_id_base: 893_060_000,
        job_lease: Duration::from_secs(10),
        ..MoverSettings::default()
    };
    let factory = Arc::new(Factory {
        access: access(&cluster),
        meta: dc.clone(),
        settings: ms.clone(),
    });
    let stop_all = CancellationToken::new();
    let mut opts = WorkerOpts::new("r3crekey-worker");
    opts.scan_interval = Duration::from_millis(200);
    opts.lease = ms.job_lease;
    let rt = JobRuntime::new(
        JobStore::new(dc.clone(), Arc::new(SystemClock)),
        factory,
        opts,
    );
    let worker = tokio::spawn(rt.run(stop_all.child_token()));

    let cache = ShardMapCache::spawn(dc.clone()).unwrap();
    cache.ready().await;
    let view = Arc::new(View {
        cache: cache.clone(),
        routing: Mutex::new(Routing::default()),
    });
    {
        let mut c = p.get_conn().await.unwrap();
        view.refresh(&mut c).await;
    }
    let log = Arc::new(Mutex::new(Log::default()));
    let paused = Arc::new(AtomicBool::new(false));
    let pace = Duration::from_millis(
        std::env::var("SPRIG_TEST_REKEY_PACE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(12),
    );
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        writers.push(tokio::spawn(writer(
            w,
            p.clone(),
            view.clone(),
            log.clone(),
            stop_all.child_token(),
            paused.clone(),
            pace,
        )));
    }
    let bg = {
        let (p, view, stop) = (p.clone(), view.clone(), stop_all.child_token());
        tokio::spawn(async move {
            let mut c = p.get_conn().await.unwrap();
            while !stop.is_cancelled() {
                view.refresh(&mut c).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    let mut ts = TenantSettings::default();
    ts.sharding.retire = Duration::from_secs(5);
    let lp = Arc::new(ShardingLoop::new(
        dc.clone(),
        Arc::new(SystemClock),
        Arc::new(Pools(p.clone())),
        Arc::new(CatalogLog(p.clone())),
        Arc::new(FixedSettings(Arc::new(Mutex::new(ts)))),
        "r3crekey-controller",
    ));

    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(
        std::env::var("SPRIG_TEST_REKEY_WARMUP_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    ))
    .await;
    // Before: every row by store_id.
    paused.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = check(&p, &cache, &snapshot(&log), "store_id", "before").await;
    eprintln!("[{:>4}s] {before}", t0.elapsed().as_secs());
    paused.store(false, Ordering::Relaxed);

    // Refusals first: a column a member lacks, a key the group has.
    let bad = lp
        .rekey(
            TENANT,
            GROUP.0,
            RekeyRequest {
                column: "nope".into(),
                ..RekeyRequest::default()
            },
            "r3c",
        )
        .await
        .unwrap_err();
    eprintln!("refused: {bad}");
    let same = lp
        .rekey(
            TENANT,
            GROUP.0,
            RekeyRequest {
                column: "store_id".into(),
                ..RekeyRequest::default()
            },
            "r3c",
        )
        .await
        .unwrap_err();
    eprintln!("refused: {same}");

    // The re-key, under the writers.
    let started = Instant::now();
    let job = lp
        .rekey(
            TENANT,
            GROUP.0,
            RekeyRequest {
                column: "customer_id".into(),
                shards: Some(4),
                ..RekeyRequest::default()
            },
            "r3c",
        )
        .await
        .unwrap();
    eprintln!(
        "[{:>4}s] re-key job {} submitted",
        t0.elapsed().as_secs(),
        job.0
    );
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(900),
            "the re-key didn't finish"
        );
        let r = match lp.rekey_follow(TENANT).await {
            Ok(r) => r,
            Err(e) => {
                // A slow etcd on a loaded laptop: the next round retries.
                eprintln!("rekey_follow: {e}; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if r.iter().any(|(g, s)| *g == GROUP.0 && *s == "finished") {
            break;
        }
        assert!(
            !r.iter().any(|(_, s)| *s == "undone"),
            "the re-key was rolled back: {:?}",
            JobStore::new(dc.clone(), Arc::new(SystemClock))
                .get(&job)
                .await
                .map(|(r, _)| (r.state, r.error))
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let took = started.elapsed();
    eprintln!(
        "[{:>4}s] re-key finished in {} ms",
        t0.elapsed().as_secs(),
        took.as_millis()
    );
    // Writes go on by the new key for a while.
    tokio::time::sleep(Duration::from_secs(5)).await;
    paused.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = check(&p, &cache, &snapshot(&log), "customer_id", "after").await;
    eprintln!("[{:>4}s] {after}", t0.elapsed().as_secs());
    // The catalog has the new key; the old shards no longer serve.
    let mut c = p.get_conn().await.unwrap();
    let (spec, state): (String, String) = c
        .query_first(format!(
            "SELECT CAST(key_spec AS CHAR), state FROM `{}`.table_groups WHERE group_id = {}",
            catalog(),
            GROUP.0
        ))
        .await
        .unwrap()
        .unwrap();
    let cols: Vec<String> = c
        .query(format!(
            "SELECT CAST(key_columns AS CHAR) FROM `{}`.group_members WHERE next_routing IS NULL",
            catalog()
        ))
        .await
        .unwrap();
    assert_eq!(state, "active");
    assert!(cols.iter().all(|c| c.contains("customer_id")), "{cols:?}");
    eprintln!("catalog: key {spec}, members {cols:?}");
    let prev_left: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA LIKE 't{TENANT}\\_%' \
             AND COLUMN_NAME = '_sprig_rk_prev'"
        ))
        .await
        .unwrap();
    assert_eq!(prev_left, Some(0), "_sprig_rk_prev left on the new shards");
    paused.store(false, Ordering::Relaxed);

    // A second re-key, cancelled before its cutover, is undone.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let job2 = lp
        .rekey(
            TENANT,
            GROUP.0,
            RekeyRequest {
                column: "store_id".into(),
                shards: Some(2),
                ..RekeyRequest::default()
            },
            "r3c",
        )
        .await
        .unwrap();
    let mover = sprig_mover::MetaMover::new(JobStore::new(dc.clone(), Arc::new(SystemClock)));
    sprig_mover::Mover::cancel(&mover, &job2, "r3c: test the undo")
        .await
        .unwrap();
    let undo_start = Instant::now();
    loop {
        assert!(undo_start.elapsed() < Duration::from_secs(300), "no undo");
        let Ok(r) = lp.rekey_follow(TENANT).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        if r.iter().any(|(g, s)| *g == GROUP.0 && *s == "undone") {
            break;
        }
        if r.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let next_left: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM `{}`.group_members WHERE next_routing IS NOT NULL",
            catalog()
        ))
        .await
        .unwrap();
    assert_eq!(next_left, Some(0));
    tokio::time::sleep(Duration::from_secs(2)).await;

    stop_all.cancel();
    for w in writers {
        let _ = w.await;
    }
    let _ = bg.await;
    worker.abort();
    let fin = snapshot(&log);
    let last = check(
        &p,
        &cache,
        &fin,
        "customer_id",
        "after the cancelled re-key",
    )
    .await;
    eprintln!("[{:>4}s] {last}", t0.elapsed().as_secs());

    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut holds = Vec::new();
    for (r, _) in store.all().await.unwrap() {
        eprintln!(
            "job {} {} {:?} -> {:?}: {} rows copied, {} applied, {} verify mismatches, holds {:?} ms, \
             {} cutover attempts, state {}",
            r.job_id.0,
            r.kind.as_str(),
            r.spec.sources.iter().map(|s| s.shard).collect::<Vec<_>>(),
            r.spec.targets.iter().map(|t| t.shard).collect::<Vec<_>>(),
            r.progress.rows_copied,
            r.progress.rows_applied,
            r.progress.verify_mismatches,
            r.progress.holds_ms,
            r.progress.cutover_attempts,
            r.state
        );
        if r.job_id == job {
            holds.extend(r.progress.holds_ms.iter().copied());
        }
    }
    let max_hold = holds.iter().copied().max().unwrap_or(0);
    eprintln!(
        "writes: {} acknowledged transactions ({} key changes), longest write that met a hold or \
         fence {} ms; re-key {} ms; hold max {} ms (limit {} ms)",
        fin.ops,
        fin.key_moves,
        fin.max_pause.as_millis(),
        took.as_millis(),
        max_hold,
        ms.cutover_max_hold.as_millis()
    );
    let decisions: Vec<(String, String, String)> = c
        .query(format!(
            "SELECT kind, outcome, summary FROM `{}`.decisions ORDER BY decision_id",
            catalog()
        ))
        .await
        .unwrap();
    for d in &decisions {
        eprintln!("decision {d:?}");
    }
    drop(c);
    assert!(
        u128::from(max_hold) <= ms.cutover_max_hold.as_millis(),
        "a hold ran over"
    );
    if std::env::var("SPRIG_TEST_REKEY_KEEP").is_err() {
        cleanup(&p, &dc).await;
    }
}
