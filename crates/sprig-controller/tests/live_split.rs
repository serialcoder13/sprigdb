//! Automatic sharding, live: a tenant grows past `sharding.split_size_gb`
//! and splits automatically under load.
//!
//! One tenant (id 3,030,600,001, in a private deployment's id range) on
//! the Compose single node, in a private DC tier `r1split` of the dev etcd (its own
//! mover jobs and maps, so no other worker claims them):
//!
//! 1. a shop schema (`customers`, `orders`, `order_items`, `events`, `countries`) in
//!    the default group, with foreign keys, written by gateway-like
//!    writers that record every statement in the tenant's workload store
//!    and log every acknowledged write;
//! 2. the sharding loop (`sprig_controller::sharding`) with a split size of
//!    a few MB: the advisor's first design (groups from foreign-key trees
//!    and joins, `_sprig_rk` back-filled), then automatic splits to 2 and 4
//!    shards, then, after the split size is raised, a merge back;
//! 3. after each step: every acknowledged write is on exactly the shard its
//!    hash routes to (nothing lost, duplicated or misplaced), reads routed
//!    by the map find every sampled customer with its orders and items,
//!    and the write pauses writers saw and the mover's holds stay within
//!    `mover.cutover_max_hold_ms`.
//!
//! `SPRIG_TEST_SPLIT_LIVE=1 cargo test -p sprig-controller --test live_split -- --nocapture`

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::similar_names,
    clippy::too_many_arguments
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value};
use sprig_advisor::sharding::apply::rk_of;
use sprig_balancer::decisions::{DecisionLog, DecisionStore, NewDecision, Outcome};
use sprig_catalog::placement::ShardMap as TenantMap;
use sprig_cluster::TopologyKind;
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, NodeId, ObjectId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_common::{CancellationToken, Result as SResult};
use sprig_controller::sharding::{ClusterPools, FixedSettings, ShardingLoop, TenantSettings};
use sprig_meta::{EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::{ClusterAccess, group_env};
use sprig_mover::{
    EnvFactory, JobEnv, JobRecord, JobRuntime, JobStore, MoverSettings, ShardRoute, WorkerOpts,
};
use sprig_shard::shardmap::RangeState;
use sprig_shard::{KeySpec, Placement, ShardMapCache, TenantMaps, shard_schema};
use sprig_workload::fingerprint::{FrontEnd, SqlCtx, SqlFingerprinter};
use sprig_workload::recorder::{ExecSummary, Recorder, RecorderConfig};
use sprig_workload::shape::{ResolvedTable, TableResolver};
use sprig_workload::store::WorkloadTables;

const TENANT: TenantId = TenantId(3_030_600_001);
const DB: DbId = DbId(1);
const DEFAULT_GROUP: GroupId = GroupId(1);
const URL: &str = "mysql://root:sprig-dev-root@127.0.0.1:13301";
const WRITERS: u32 = 4;
const TABLES: [(&str, u64); 5] = [
    ("customers", 1),
    ("orders", 2),
    ("order_items", 3),
    ("events", 4),
    ("countries", 5),
];

fn pool() -> Pool {
    let opts = Opts::from_url(URL).unwrap();
    Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 24).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

fn default_schema() -> String {
    shard_schema(TENANT, DB, DEFAULT_GROUP, ShardId(1))
}

fn catalog() -> String {
    SchemaName::catalog(TENANT).to_string()
}

fn now_ms() -> u64 {
    sprig_mover::record::wall_ms(SystemTime::now())
}

// ---- the environment -------------------------------------------------------

struct Pools(Pool);

#[async_trait::async_trait]
impl ClusterPools for Pools {
    async fn pool(&self, _cluster: &ClusterId) -> SResult<Pool> {
        Ok(self.0.clone())
    }
}

/// Decisions into the tenant's catalog.
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

/// Hash-range jobs on the single node.
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
    let ep =
        sprig_cluster::NodeEndpoint::from_url(NodeId::new("r1split-n1").unwrap(), URL).unwrap();
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

/// Drops this tenant's schemas (by exact tenant id) and the private tier.
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
    // Everything of the private tier.
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

async fn setup(p: &Pool, dc: &Arc<dyn MetaStore>, cluster: &ClusterId) {
    let mut c = p.get_conn().await.unwrap();
    let cat = catalog();
    c.query_drop(format!("CREATE DATABASE `{cat}`"))
        .await
        .unwrap();
    sprig_catalog::Migrator::new(&[
        sprig_workload::store::WORKLOAD_MODULE,
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
    }
    let s = default_schema();
    c.query_drop(format!(
        "CREATE DATABASE `{s}` CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci"
    ))
    .await
    .unwrap();
    for ddl in [
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(40) NOT NULL, balance BIGINT NOT NULL)",
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, amount INT NOT NULL, \
         note VARCHAR(200) NOT NULL, KEY (customer_id), \
         CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (id))",
        "CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, sku INT NOT NULL, \
         qty INT NOT NULL, pad VARCHAR(120) NOT NULL, \
         CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE)",
        "CREATE TABLE events (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, kind VARCHAR(20) NOT NULL, \
         payload VARCHAR(200) NOT NULL, KEY (customer_id))",
        "CREATE TABLE countries (code CHAR(2) PRIMARY KEY, name VARCHAR(40) NOT NULL)",
    ] {
        c.query_drop(format!("USE `{s}`")).await.unwrap();
        c.query_drop(ddl).await.unwrap();
    }
    c.query_drop(format!(
        "INSERT INTO `{s}`.countries VALUES ('IN','India'),('US','United States'),('DE','Germany')"
    ))
    .await
    .unwrap();
    // The tenant map (group 0), on the private cluster id.
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
}

// ---- the writers -------------------------------------------------------------

/// Logical tables for fingerprints.
struct Resolver;

impl TableResolver for Resolver {
    fn resolve(&self, db: Option<&str>, table: &str) -> Option<ResolvedTable> {
        if db != Some("shop") {
            return None;
        }
        let id = TABLES.iter().find(|(n, _)| *n == table)?.1;
        Some(ResolvedTable {
            object_id: ObjectId(id),
            db_id: DB,
            physical: None,
        })
    }
}

/// One row as the log keeps it.
type RowVal = Vec<String>;

/// One logical statement: table, SQL, parameters, and the row it
/// acknowledges.
type Stmt = (
    &'static str,
    String,
    Vec<Value>,
    Option<((String, i64), Option<RowVal>)>,
);

/// A schema and the hash range it serves, if a shard.
type Placed = (String, Option<(u64, Option<u64>)>);

#[derive(Default)]
struct Log {
    /// (table, id) -> last acknowledged row, `None` deleted.
    rows: BTreeMap<(String, i64), Option<RowVal>>,
    /// Keys whose last write's outcome is unknown, and their candidates.
    uncertain: BTreeMap<(String, i64), Vec<Option<RowVal>>>,
    ops: u64,
    max_pause: Duration,
    max_latency: Duration,
}

/// How the writers see the tenant: the map cache, the groups, rk states.
struct View {
    cache: ShardMapCache,
    tenant_maps: TenantMaps,
    /// table -> (group, rk filled)
    groups: Mutex<BTreeMap<String, (GroupId, bool)>>,
    keys: Mutex<BTreeMap<GroupId, KeySpec>>,
}

impl View {
    async fn refresh(&self, c: &mut Conn) {
        let cat = catalog();
        let rows: Vec<(String, u32, String, String)> = c
            .query(format!(
                "SELECT o.name, m.group_id, m.rk_state, CAST(g.key_spec AS CHAR) FROM `{cat}`.group_members m \
                 JOIN `{cat}`.objects o ON o.object_id = m.object_id \
                 JOIN `{cat}`.table_groups g ON g.group_id = m.group_id"
            ))
            .await
            .unwrap_or_default();
        let mut g = BTreeMap::new();
        let mut k = BTreeMap::new();
        for (name, gid, state, spec) in rows {
            g.insert(name, (GroupId(gid), state != "absent"));
            if let Ok(v) = serde_json::from_str(&spec)
                && let Ok(spec) = KeySpec::from_json(&v)
            {
                k.insert(GroupId(gid), spec);
            }
        }
        *self.groups.lock().unwrap() = g;
        *self.keys.lock().unwrap() = k;
    }

    /// Where a customer's rows go: schema, rk (when filled), held.
    async fn route(&self, table: &str, customer: i64) -> Option<(String, Option<u64>, bool)> {
        let entry = self.groups.lock().unwrap().get(table).copied();
        match entry {
            None => {
                let (m, _) = self.tenant_maps.load(TENANT).await.ok()?;
                let p = Placement::of(&m).ok()?;
                Some((default_schema(), None, p.held_at(now_ms())))
            }
            Some((g, filled)) => {
                let spec = self.keys.lock().unwrap().get(&g).cloned()?;
                let rk = rk_of(&spec, &[Value::Int(customer)]).ok()?;
                let map = self.cache.get(TENANT, g).ok()?;
                let r = map.route(sprig_shard::RoutingHash(rk));
                let held =
                    r.state == RangeState::Held && r.hold_until_ms.is_some_and(|u| now_ms() < u);
                // The regroup's hold is on the tenant map.
                let (tm, _) = self.tenant_maps.load(TENANT).await.ok()?;
                let tenant_held = Placement::of(&tm).ok()?.held_at(now_ms());
                Some((
                    shard_schema(TENANT, DB, g, r.shard),
                    filled.then_some(rk),
                    held || tenant_held,
                ))
            }
        }
    }
}

struct Wl {
    fps: SqlFingerprinter,
    rec: Recorder,
}

impl Wl {
    fn record(&self, sql: &str, rows: u64) {
        let ctx = SqlCtx {
            lex: sprig_sql::lex::LexOptions::mariadb(),
            current_db: Some("shop"),
            db_id: DB,
            front_end: FrontEnd::Mariadb,
            caller: None,
            lower_case_table_names: false,
            max_text_bytes: 4096,
        };
        let fp = self.fps.sql(TENANT, sql.as_bytes(), &ctx, &Resolver);
        self.rec.record(
            TENANT,
            &fp,
            &ExecSummary {
                latency_us: 500,
                error: false,
                rows_returned: rows,
                rows_affected: rows,
                shards: 1,
                clusters: 1,
                scatter: false,
                in_list_max: 0,
                examined: None,
                session: 1,
            },
        );
    }

    async fn flush(&self, p: &Pool) {
        let Ok(mut c) = p.get_conn().await else {
            return;
        };
        let t = WorkloadTables::for_tenant(TENANT);
        let new = self.rec.drain_new();
        let _ = t.insert_fingerprints(&mut c, &new).await;
        let rows = self.rec.snapshot();
        let _ = t.replace_stats(&mut c, 1, &rows).await;
    }
}

fn is_retry(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if matches!(s.code, 1146 | 1049 | 1142 | 1044 | 1213 | 1205 | 1054))
}

/// One logical operation: statements on one shard schema, in one
/// transaction, with the rows they acknowledge.
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
    wl: Arc<Wl>,
    log: Arc<Mutex<Log>>,
    stop: CancellationToken,
    paused: Arc<AtomicBool>,
    pace: Duration,
) {
    let mut c = p.get_conn().await.unwrap();
    let customers: Vec<i64> = (0..500)
        .map(|i| i64::from(me) + i64::from(WRITERS) * i)
        .collect();
    let mut balance: BTreeMap<i64, i64> = BTreeMap::new();
    let mut orders: Vec<(i64, i64, Vec<i64>)> = Vec::new(); // (order, customer, items)
    let mut seq: i64 = 0;
    let base = i64::from(me) * 1_000_000_000;
    while !stop.is_cancelled() {
        if paused.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        tokio::time::sleep(pace).await;
        seq += 1;
        let cust = customers[usize::try_from(seq).unwrap() % customers.len()];
        let op = seq % 10;
        // Build the logical statements, then route them.
        let mut logical: Vec<Stmt> = Vec::new();
        match op {
            0 if !balance.contains_key(&cust) => {}
            0 => {
                let b = balance[&cust] + 7;
                logical.push((
                    "customers",
                    "UPDATE customers SET balance = ? WHERE id = ?".into(),
                    vec![b.into(), cust.into()],
                    Some((
                        ("customers".into(), cust),
                        Some(vec![format!("c{cust}"), b.to_string()]),
                    )),
                ));
            }
            1 | 2 => {
                let id = base + seq;
                logical.push((
                    "events",
                    "INSERT INTO events (id, customer_id, kind, payload) VALUES (?, ?, ?, ?)"
                        .into(),
                    vec![
                        id.into(),
                        cust.into(),
                        "click".into(),
                        "x".repeat(150).into(),
                    ],
                    Some((
                        ("events".into(), id),
                        Some(vec![cust.to_string(), "click".into()]),
                    )),
                ));
            }
            9 if !orders.is_empty() => {
                let (o, oc, items) =
                    orders.swap_remove(usize::try_from(seq).unwrap() % orders.len());
                let _ = oc;
                logical.push((
                    "order_items",
                    "DELETE FROM order_items WHERE order_id = ?".into(),
                    vec![o.into()],
                    None,
                ));
                logical.push((
                    "orders",
                    "DELETE FROM orders WHERE id = ?".into(),
                    vec![o.into()],
                    Some((("orders".into(), o), None)),
                ));
                for it in items {
                    logical.push((
                        "order_items",
                        String::new(),
                        Vec::new(),
                        Some((("order_items".into(), it), None)),
                    ));
                }
            }
            _ => {
                if let std::collections::btree_map::Entry::Vacant(e) = balance.entry(cust) {
                    e.insert(0);
                    logical.push((
                        "customers",
                        "INSERT INTO customers (id, name, balance) VALUES (?, ?, ?)".into(),
                        vec![cust.into(), format!("c{cust}").into(), 0i64.into()],
                        Some((
                            ("customers".into(), cust),
                            Some(vec![format!("c{cust}"), "0".into()]),
                        )),
                    ));
                }
                let o = base + seq;
                logical.push((
                    "orders",
                    "INSERT INTO orders (id, customer_id, amount, note) VALUES (?, ?, ?, ?)".into(),
                    vec![o.into(), cust.into(), 100i64.into(), "n".repeat(180).into()],
                    Some((
                        ("orders".into(), o),
                        Some(vec![cust.to_string(), "100".into()]),
                    )),
                ));
                let mut items = Vec::new();
                for k in 0..2 {
                    let it = base + seq * 10 + 100_000_000 + k;
                    items.push(it);
                    logical.push(("order_items", "INSERT INTO order_items (id, order_id, sku, qty, pad) VALUES (?, ?, ?, ?, ?)".into(),
                        vec![it.into(), o.into(), 7i64.into(), 1i64.into(), "p".repeat(100).into()],
                        Some((("order_items".into(), it), Some(vec![o.to_string(), "1".into()])))));
                }
                orders.push((o, cust, items));
            }
        }
        if logical.is_empty() {
            continue;
        }
        // The customer whose hash routes this transaction.
        let route_cust = match op {
            9 => logical
                .iter()
                .find_map(|(t, _, p, _)| (*t == "orders").then(|| p.first().cloned()))
                .flatten()
                .and_then(|o| match o {
                    Value::Int(o) => Some(o),
                    _ => None,
                })
                .and_then(|o| ((o - base) >= 0).then_some(o))
                .map_or(cust, |o| {
                    customers[usize::try_from(o - base).unwrap() % customers.len()]
                }),
            _ => cust,
        };
        let started = Instant::now();
        let mut met_pause = false;
        loop {
            if stop.is_cancelled() {
                return;
            }
            let mut stmts = Vec::new();
            let mut held = false;
            let mut ok = true;
            for (table, sql, params, _) in &logical {
                if sql.is_empty() {
                    continue;
                }
                let Some((schema, rk, h)) = view.route(table, route_cust).await else {
                    ok = false;
                    break;
                };
                held |= h;
                let mut q = sql.replace(&format!(" {table} "), &format!(" `{schema}`.`{table}` "));
                let mut params = params.clone();
                if let Some(rk) = rk {
                    if q.starts_with("INSERT") {
                        q = q
                            .replacen(") VALUES (", ", _sprig_rk) VALUES (", 1)
                            .replacen("?)", "?, ?)", 1);
                        params.push(Value::UInt(rk));
                    } else if q.starts_with("UPDATE") {
                        q = q.replacen(" SET ", " SET _sprig_rk = ?, ", 1);
                        params.insert(0, Value::UInt(rk));
                    }
                }
                stmts.push((q, params));
            }
            if !ok || held {
                met_pause = true;
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            match run_txn(&mut c, &stmts).await {
                Ok(()) => {
                    for (_, sql, _, _) in &logical {
                        if !sql.is_empty() {
                            wl.record(sql, 1);
                        }
                    }
                    let lat = started.elapsed();
                    {
                        let mut l = log.lock().unwrap();
                        for (_, _, _, ack) in &logical {
                            if let Some((k, v)) = ack {
                                l.rows.insert(k.clone(), v.clone());
                                l.uncertain.remove(k);
                            }
                        }
                        if op == 0 {
                            balance.entry(cust).and_modify(|b| *b += 7);
                        }
                        l.ops += 1;
                        l.max_latency = l.max_latency.max(lat);
                        if met_pause {
                            l.max_pause = l.max_pause.max(lat);
                        }
                    }
                    break;
                }
                Err(e) if is_retry(&e) => {
                    met_pause = true;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(mysql_async::Error::Server(s)) => {
                    panic!("writer {me}: {s:?} on {stmts:?}");
                }
                Err(e) => {
                    // Connection trouble: the outcome is unknown.
                    {
                        let mut l = log.lock().unwrap();
                        for (_, _, _, ack) in &logical {
                            if let Some((k, v)) = ack {
                                let prev = l.rows.get(k).cloned().flatten();
                                l.uncertain
                                    .entry(k.clone())
                                    .or_insert_with(|| vec![prev])
                                    .push(v.clone());
                            }
                        }
                    }
                    eprintln!("writer {me}: {e}; reconnecting");
                    c = p.get_conn().await.unwrap();
                    break;
                }
            }
        }
        // Some reads, recorded for the advisor.
        if seq % 3 == 0 {
            for (t, sql) in [
                (
                    "customers",
                    "SELECT id, balance FROM customers WHERE id = ?",
                ),
                (
                    "orders",
                    "SELECT id, amount FROM orders WHERE customer_id = ?",
                ),
                ("events", "SELECT id FROM events WHERE customer_id = ?"),
            ] {
                if let Some((schema, _, false)) = view.route(t, cust).await {
                    let q = sql.replace(&format!(" {t} "), &format!(" `{schema}`.`{t}` "));
                    if c.exec_drop(q, (cust,)).await.is_ok() {
                        wl.record(sql, 1);
                    }
                }
            }
            wl.record("SELECT c.id, e.kind FROM customers c JOIN events e ON e.customer_id = c.id WHERE c.id = ?", 1);
            wl.record("SELECT o.id, i.sku FROM orders o JOIN order_items i ON i.order_id = o.id WHERE o.id = ?", 1);
        }
    }
}

// ---- checks ------------------------------------------------------------------

fn cols(t: &str) -> &'static str {
    match t {
        "customers" => "id, name, CAST(balance AS CHAR)",
        "orders" => "id, CAST(customer_id AS CHAR), CAST(amount AS CHAR)",
        "order_items" => "id, CAST(order_id AS CHAR), CAST(qty AS CHAR)",
        _ => "id, CAST(customer_id AS CHAR), kind",
    }
}

/// Every acknowledged write on exactly the shard its hash routes to.
async fn check(p: &Pool, view: &View, log: &Log, label: &str) -> String {
    let mut c = p.get_conn().await.unwrap();
    view.refresh(&mut c).await;
    let groups = view.groups.lock().unwrap().clone();
    let mut summary = Vec::new();
    let mut total_rows = 0usize;
    for t in ["customers", "orders", "order_items", "events"] {
        // Where the table lives: its group's serving shards, or the default schema.
        let mut schemas: Vec<Placed> = Vec::new();
        match groups.get(t) {
            Some((g, _)) => {
                let m = view.cache.get(TENANT, *g).unwrap();
                for (i, r) in m.ranges.iter().enumerate() {
                    schemas.push((shard_schema(TENANT, DB, *g, r.shard), Some((r.lo, m.hi(i)))));
                }
            }
            None => schemas.push((default_schema(), None)),
        }
        let mut have: BTreeMap<i64, (RowVal, usize)> = BTreeMap::new();
        let mut dup = 0;
        let mut misplaced = 0;
        for (s, range) in &schemas {
            let rk = if range.is_some() {
                ", CAST(_sprig_rk AS CHAR)"
            } else {
                ""
            };
            let rows: Vec<mysql_async::Row> = c
                .query(format!("SELECT {}{rk} FROM `{s}`.`{t}`", cols(t)))
                .await
                .unwrap();
            for r in rows {
                let v: Vec<Value> = r.unwrap();
                let id = match &v[0] {
                    Value::Int(i) => *i,
                    Value::Bytes(b) => String::from_utf8_lossy(b).parse().unwrap(),
                    _ => 0,
                };
                let text = |x: &Value| match x {
                    Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                    Value::Int(i) => i.to_string(),
                    _ => String::new(),
                };
                let vals: RowVal = v[1..3].iter().map(text).collect();
                if let (Some((lo, hi)), Some(h)) = (range, v.get(3)) {
                    let h: u64 = text(h).parse().unwrap_or(u64::MAX);
                    if h < *lo || hi.is_some_and(|hi| h >= hi) {
                        misplaced += 1;
                    }
                }
                if have.insert(id, (vals, 0)).is_some() {
                    dup += 1;
                }
            }
        }
        let mut lost = 0;
        let mut wrong = 0;
        let mut extra = 0;
        for ((tt, id), want) in &log.rows {
            if tt != t {
                continue;
            }
            let got = have.get(id).map(|(v, _)| v.clone());
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
        total_rows += have.len();
        assert_eq!(
            (lost, wrong, extra, dup, misplaced),
            (0, 0, 0, 0, 0),
            "{label}: {t}: lost {lost}, wrong {wrong}, extra {extra}, duplicated {dup}, misplaced {misplaced}"
        );
        summary.push(format!(
            "{t} {} rows on {} schema(s)",
            have.len(),
            schemas.len()
        ));
    }
    // Routed reads: sampled customers, their orders and items, on the shard
    // the map gives.
    let mut found = 0;
    for ((t, id), v) in log
        .rows
        .iter()
        .filter(|(k, v)| k.0 == "customers" && v.is_some())
        .step_by(7)
        .take(200)
    {
        let _ = (t, v);
        let (schema, _, _) = view.route("customers", *id).await.unwrap();
        let n: Option<u64> = c
            .exec_first(
                format!("SELECT COUNT(*) FROM `{schema}`.customers WHERE id = ?"),
                (*id,),
            )
            .await
            .unwrap();
        assert_eq!(
            n,
            Some(1),
            "{label}: customer {id} not on its routed shard {schema}"
        );
        let (oschema, _, _) = view.route("orders", *id).await.unwrap();
        let joined: Option<u64> = c
            .exec_first(
                format!("SELECT COUNT(*) FROM `{oschema}`.orders o JOIN `{oschema}`.order_items i ON i.order_id = o.id WHERE o.customer_id = ?"),
                (*id,),
            )
            .await
            .unwrap();
        let want = log
            .rows
            .iter()
            .filter(|((tt, _), v)| {
                tt == "orders" && v.as_ref().is_some_and(|v| v[0] == id.to_string())
            })
            .count()
            * 2;
        assert!(
            usize::try_from(joined.unwrap_or(0)).unwrap() >= want.saturating_sub(2),
            "{label}: customer {id}'s orders on {oschema}: {joined:?} items, {want} expected"
        );
        found += 1;
    }
    format!(
        "{label}: {total_rows} rows checked ({}), {found} routed reads ok",
        summary.join(", ")
    )
}

// ---- the test ------------------------------------------------------------------

fn shards_of(cache: &ShardMapCache, view: &View, table: &str) -> usize {
    let g = view.groups.lock().unwrap().get(table).map(|x| x.0);
    g.and_then(|g| cache.get(TENANT, g).ok())
        .map_or(0, |m| m.ranges.len())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_tenant_groups_splits_and_merges_under_load() {
    if std::env::var("SPRIG_TEST_SPLIT_LIVE").is_err() {
        eprintln!("skipped: set SPRIG_TEST_SPLIT_LIVE=1 with the Compose stack up");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "sprig_mover=info,sprig_controller=info,sprig_advisor=info,warn".into()
        }))
        .with_test_writer()
        .try_init();
    let etcd = std::env::var("SPRIG_TEST_ETCD").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(
            Tier::Dc(DcId::new("r1split").unwrap()),
            &EtcdConfig::new(vec![etcd]),
        )
        .await
        .unwrap(),
    );
    let cluster = ClusterId::new("r1split-c1").unwrap();
    let p = pool();
    cleanup(&p, &dc).await;
    setup(&p, &dc, &cluster).await;

    // The mover worker.
    let mut ms = MoverSettings {
        copy_chunk_rows: 500,
        ready_sustain: Duration::from_secs(2),
        cutover_retry: Duration::from_secs(1),
        server_id_base: 303_060_000,
        ..MoverSettings::default()
    };
    ms.job_lease = Duration::from_secs(10);
    let factory = Arc::new(Factory {
        access: access(&cluster),
        meta: dc.clone(),
        settings: ms.clone(),
    });
    let stop_all = CancellationToken::new();
    let mut opts = WorkerOpts::new("r1split-worker");
    opts.scan_interval = Duration::from_millis(200);
    opts.lease = ms.job_lease;
    let rt = JobRuntime::new(
        JobStore::new(dc.clone(), Arc::new(SystemClock)),
        factory,
        opts,
    );
    let worker = tokio::spawn(rt.run(stop_all.child_token()));

    // The writers.
    let cache = ShardMapCache::spawn(dc.clone()).unwrap();
    cache.ready().await;
    let view = Arc::new(View {
        cache: cache.clone(),
        tenant_maps: TenantMaps::new(dc.clone()),
        groups: Mutex::new(BTreeMap::new()),
        keys: Mutex::new(BTreeMap::new()),
    });
    let wl = Arc::new(Wl {
        fps: SqlFingerprinter::new(10_000),
        rec: Recorder::new(RecorderConfig::default(), Arc::new(SystemClock)),
    });
    let log = Arc::new(Mutex::new(Log::default()));
    let paused = Arc::new(AtomicBool::new(false));
    let pace = Duration::from_millis(
        std::env::var("SPRIG_TEST_SPLIT_PACE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4),
    );
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        writers.push(tokio::spawn(writer(
            w,
            p.clone(),
            view.clone(),
            wl.clone(),
            log.clone(),
            stop_all.child_token(),
            paused.clone(),
            pace,
        )));
    }
    // View refresh and workload flush.
    let bg = {
        let (p, view, wl, stop) = (p.clone(), view.clone(), wl.clone(), stop_all.child_token());
        tokio::spawn(async move {
            let mut c = p.get_conn().await.unwrap();
            let mut n = 0u32;
            while !stop.is_cancelled() {
                view.refresh(&mut c).await;
                n += 1;
                if n.is_multiple_of(10) {
                    wl.flush(&p).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };

    // The sharding loop, with a split size of 3 MB.
    let split_mb = std::env::var("SPRIG_TEST_SPLIT_MB")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(3.0);
    let mb = |x: f64| x * 1_048_576.0;
    let mut ts = TenantSettings::default();
    ts.sharding.split_bytes = mb(split_mb);
    ts.sharding.merge_bytes = mb(split_mb / 4.0);
    ts.sharding.max_split_ways = 2;
    ts.sharding.merge_cooldown = Duration::from_secs(20);
    ts.sharding.retire = Duration::from_secs(5);
    ts.sharding.check_interval = Duration::from_secs(2);
    ts.advisor.min_observe = Duration::ZERO;
    ts.advisor.min_units = 300;
    ts.advisor.max_wait = Duration::from_secs(120);
    ts.chunk_rows = 500;
    let settings = FixedSettings(Arc::new(Mutex::new(ts)));
    let mut lp = ShardingLoop::new(
        dc.clone(),
        Arc::new(SystemClock),
        Arc::new(Pools(p.clone())),
        Arc::new(CatalogLog(p.clone())),
        Arc::new(settings.clone()),
        "r1split-controller",
    );
    lp.advisor_retry = Duration::from_secs(10);
    let lp = Arc::new(lp);
    let loop_task = {
        let lp = lp.clone();
        let stop = stop_all.child_token();
        tokio::spawn(async move {
            lp.run(Arc::new(|| vec![TENANT]), stop).await;
        })
    };

    let t0 = Instant::now();
    let mut milestones: Vec<String> = Vec::new();
    let wait_for = |what: &'static str, limit: Duration| (what, limit);
    let steps = [
        wait_for("first design", Duration::from_secs(300)),
        wait_for("2 shards", Duration::from_secs(600)),
        wait_for("4 shards", Duration::from_secs(900)),
    ];
    for (what, limit) in steps {
        let start = Instant::now();
        loop {
            assert!(
                start.elapsed() < limit,
                "timed out waiting for {what}; log ops {}",
                log.lock().unwrap().ops
            );
            let groups = view.groups.lock().unwrap().clone();
            let n = shards_of(&cache, &view, "customers");
            let done = match what {
                "first design" => {
                    groups.get("customers").is_some_and(|(_, f)| *f) && {
                        let mut c = p.get_conn().await.unwrap();
                        let s: Option<String> = c
                            .query_first(format!(
                                "SELECT state FROM `{}`.table_groups WHERE name = 'customers'",
                                catalog()
                            ))
                            .await
                            .unwrap_or(None);
                        s.as_deref() == Some("active")
                    }
                }
                "2 shards" => n >= 2,
                _ => n >= 4,
            };
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // Let the job finish retiring, pause the writers, check.
        tokio::time::sleep(Duration::from_secs(8)).await;
        paused.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let snapshot = {
            let l = log.lock().unwrap();
            Log {
                rows: l.rows.clone(),
                uncertain: l.uncertain.clone(),
                ..Log::default()
            }
        };
        let line = check(&p, &view, &snapshot, what).await;
        paused.store(false, Ordering::Relaxed);
        eprintln!("[{:>4}s] {line}", t0.elapsed().as_secs());
        milestones.push(line);
    }

    // Merge back: raise the split size so the shards are small and quiet.
    {
        let mut s = settings.0.lock().unwrap();
        s.sharding.split_bytes = mb(split_mb * 40.0);
        s.sharding.merge_bytes = mb(split_mb * 10.0);
    }
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(600),
            "no merge happened"
        );
        if shards_of(&cache, &view, "customers") < 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop_all.cancel();
    for w in writers {
        let _ = w.await;
    }
    let _ = loop_task.await;
    let _ = bg.await;
    worker.abort();
    let final_log = {
        let l = log.lock().unwrap();
        Log {
            rows: l.rows.clone(),
            uncertain: l.uncertain.clone(),
            ops: l.ops,
            max_pause: l.max_pause,
            max_latency: l.max_latency,
        }
    };
    let line = check(&p, &view, &final_log, "after the merge").await;
    eprintln!("[{:>4}s] {line}", t0.elapsed().as_secs());

    // Jobs and holds.
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut holds = Vec::new();
    for (r, _) in store.all().await.unwrap() {
        eprintln!(
            "job {} {} {:?} -> {:?}: {} rows copied, {} applied, holds {:?} ms, {} cutover attempts, state {}",
            r.job_id.0,
            r.kind.as_str(),
            r.spec.sources.iter().map(|s| s.shard).collect::<Vec<_>>(),
            r.spec.targets.iter().map(|t| t.shard).collect::<Vec<_>>(),
            r.progress.rows_copied,
            r.progress.rows_applied,
            r.progress.holds_ms,
            r.progress.cutover_attempts,
            r.state
        );
        holds.extend(r.progress.holds_ms.iter().copied());
    }
    let mut c = p.get_conn().await.unwrap();
    let decisions: Vec<(String, String, String, String)> = c
        .query(format!(
            "SELECT kind, reason_code, outcome, summary FROM `{}`.decisions ORDER BY decision_id",
            catalog()
        ))
        .await
        .unwrap();
    for d in &decisions {
        eprintln!("decision {d:?}");
    }
    #[allow(clippy::type_complexity)]
    let run: Option<(String, Option<f64>, Option<f64>, Option<String>)> = c
        .query_first(format!("SELECT verdict, best_score, confidence, CAST(design AS CHAR) FROM `{}`.advisor_runs ORDER BY run_id DESC LIMIT 1", catalog()))
        .await
        .unwrap();
    eprintln!("advisor run: {run:?}");
    let max_hold = holds.iter().copied().max().unwrap_or(0);
    eprintln!(
        "writes: {} acknowledged ops, longest write that met a hold or fence {} ms, longest write {} ms; mover holds max {} ms (limit {} ms)",
        final_log.ops,
        final_log.max_pause.as_millis(),
        final_log.max_latency.as_millis(),
        max_hold,
        ms.cutover_max_hold.as_millis()
    );
    for m in &milestones {
        eprintln!("{m}");
    }
    assert!(
        u128::from(max_hold) <= ms.cutover_max_hold.as_millis(),
        "a hold ran over"
    );
    assert!(
        final_log.max_pause <= ms.cutover_max_hold + ms.hold_grace + Duration::from_millis(500),
        "a write paused {:?}",
        final_log.max_pause
    );
    let ids: BTreeSet<GroupId> = view.groups.lock().unwrap().values().map(|x| x.0).collect();
    eprintln!("groups {ids:?}");
    if std::env::var("SPRIG_TEST_SPLIT_KEEP").is_err() {
        cleanup(&p, &dc).await;
    }
    let _ = AtomicU64::new(0);
}
