//! The R1 differential suite (chapter 46, "Differential testing"; chapter
//! 17, "Testing"): the same statements against a sharded tenant through
//! the gateway and against one unsharded MariaDB schema, compared exactly.
//! A statement the router can't plan exactly must fail with 39420
//! (`ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`); it is counted as skipped,
//! never as a pass. Any other difference fails the test.
//!
//! Needs a gateway with `[gateway] router_topology_file` and a tenant with
//! one logical database (the suite creates its tables behind the gateway's
//! back, in shard schemas, as chapter 21's DDL across shards will later):
//!
//! ```sh
//! export SPRIG_TEST_R1_GATEWAY=127.0.0.1:25307          # the tenant's gateway endpoint
//! export SPRIG_TEST_R1_USER=r1router.app SPRIG_TEST_R1_PASSWORD=sprig-dev-app
//! export SPRIG_TEST_R1_TENANT=r1router SPRIG_TEST_R1_TENANT_ID=990000112
//! export SPRIG_TEST_R1_DB=r1r_db SPRIG_TEST_R1_DB_ID=1 SPRIG_TEST_R1_CLUSTER=dc-a-c1
//! export SPRIG_TEST_R1_BACKEND=127.0.0.1:13301          # the cluster's writer, as root
//! export SPRIG_TEST_R1_TOPOLOGY=target/agent-r1router/topology.json
//! cargo test -p sprig-router --test differential_live -- --nocapture --test-threads 1
//! ```
//!
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
use std::time::{Duration, Instant};

use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};

const SHARDS_A: u32 = 4; // group 901: customers, orders
const SHARDS_B: u32 = 2; // group 902: products

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

struct Env {
    gateway: String,
    user: String,
    password: String,
    tenant: String,
    tenant_id: String,
    db: String,
    db_id: String,
    cluster: String,
    backend: String,
    root_password: String,
    topology: String,
}

impl Env {
    fn load() -> Option<Self> {
        Some(Self {
            gateway: env("SPRIG_TEST_R1_GATEWAY")?,
            user: env("SPRIG_TEST_R1_USER").unwrap_or_else(|| "r1router.app".into()),
            password: env("SPRIG_TEST_R1_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into()),
            tenant: env("SPRIG_TEST_R1_TENANT").unwrap_or_else(|| "r1router".into()),
            tenant_id: env("SPRIG_TEST_R1_TENANT_ID")?,
            db: env("SPRIG_TEST_R1_DB").unwrap_or_else(|| "r1r_db".into()),
            db_id: env("SPRIG_TEST_R1_DB_ID").unwrap_or_else(|| "1".into()),
            cluster: env("SPRIG_TEST_R1_CLUSTER").unwrap_or_else(|| "dc-a-c1".into()),
            backend: env("SPRIG_TEST_R1_BACKEND").unwrap_or_else(|| "127.0.0.1:13301".into()),
            root_password: env("SPRIG_TEST_R1_ROOT_PASSWORD")
                .unwrap_or_else(|| "sprig-dev-root".into()),
            topology: env("SPRIG_TEST_R1_TOPOLOGY")?,
        })
    }
    fn default_schema(&self) -> String {
        format!("t{}_d{}_g1_s1", self.tenant_id, self.db_id)
    }
    fn shard_schema(&self, g: u32, s: u32) -> String {
        format!("t{}_d{}_g{g}_s{s}", self.tenant_id, self.db_id)
    }
    fn ref_schema(&self) -> String {
        format!("t{}_d{}_ref", self.tenant_id, self.db_id)
    }
    fn reference_db(&self) -> String {
        format!("r1router_diff_{}", self.tenant_id)
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

const DDL: &[&str] = &[
    "CREATE TABLE customers (cid INT NOT NULL PRIMARY KEY, name VARCHAR(40) COLLATE utf8mb4_general_ci, \
     region VARCHAR(10) COLLATE utf8mb4_uca1400_ai_ci, score DECIMAL(10,2), rating DOUBLE, created DATETIME, \
     flag TINYINT, cc CHAR(2)) DEFAULT CHARSET utf8mb4",
    "CREATE TABLE orders (oid INT NOT NULL, cid INT NOT NULL, pid INT, amount DECIMAL(12,2), qty INT, \
     note VARCHAR(30) COLLATE utf8mb4_uca1400_as_cs, PRIMARY KEY (cid, oid)) DEFAULT CHARSET utf8mb4",
    "CREATE TABLE products (pid INT NOT NULL PRIMARY KEY, pname VARCHAR(40) COLLATE utf8mb4_unicode_ci, \
     price DECIMAL(10,2), cat VARCHAR(10)) DEFAULT CHARSET utf8mb4",
];
const REF_DDL: &str = "CREATE TABLE countries (code CHAR(2) NOT NULL PRIMARY KEY, cname VARCHAR(30)) DEFAULT CHARSET utf8mb4";

fn topology_json(e: &Env) -> String {
    let shards = |g: u32, n: u32| {
        (1..=n)
            .map(|s| {
                format!(
                    r#"{{"shard": {s}, "schema": "{}", "cluster": "{}"}}"#,
                    e.shard_schema(g, s),
                    e.cluster
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let d = e.default_schema();
    format!(
        r#"{{"tenants": {{"{t}": {{
  "groups": [
    {{"id": 901, "key_class": "int", "shards": [{a}]}},
    {{"id": 902, "key_class": "int", "shards": [{b}]}}],
  "tables": [
    {{"schema": "{d}", "table": "customers", "group": 901, "key": "cid", "key_ordinal": 0, "upsert_safe": true}},
    {{"schema": "{d}", "table": "orders", "group": 901, "key": "cid", "key_ordinal": 1}},
    {{"schema": "{d}", "table": "products", "group": 902, "key": "pid", "key_ordinal": 0, "upsert_safe": true}},
    {{"schema": "{d}", "table": "countries", "group": 901, "reference": "{r}"}}]}}}}}}"#,
        t = e.tenant,
        a = shards(901, SHARDS_A),
        b = shards(902, SHARDS_B),
        r = e.ref_schema(),
    )
}

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
    out.push("INSERT INTO countries VALUES ('us', 'United States'), ('de', 'Germany'), ('fr', 'France'), ('in', 'India')".into());
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
    // Floats rounded so the multiset sort pairs them up.
    for row in &mut v {
        for (c, t) in row.iter_mut().zip(types) {
            if float_col(*t)
                && let Some(s) = c
                && let Ok(f) = s.parse::<f64>()
            {
                *s = format!("{f:.6e}");
            }
        }
    }
    v.sort();
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

async fn setup(e: &Env, root: &mut Conn) {
    let schemas: Vec<String> = (1..=SHARDS_A)
        .map(|s| e.shard_schema(901, s))
        .chain((1..=SHARDS_B).map(|s| e.shard_schema(902, s)))
        .collect();
    for s in schemas.iter().chain([&e.ref_schema(), &e.reference_db()]) {
        root.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
        root.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
    }
    for s in &schemas {
        let tables: &[&str] = if s.contains("_g901_") {
            &DDL[..2]
        } else {
            &DDL[2..]
        };
        for d in tables {
            root.query_drop(format!("USE `{s}`")).await.unwrap();
            root.query_drop(*d).await.unwrap();
        }
    }
    root.query_drop(format!("USE `{}`", e.ref_schema()))
        .await
        .unwrap();
    root.query_drop(REF_DDL).await.unwrap();
    root.query_drop(format!("USE `{}`", e.reference_db()))
        .await
        .unwrap();
    for d in DDL.iter().chain([&REF_DDL]) {
        root.query_drop(*d).await.unwrap();
    }
    std::fs::write(&e.topology, topology_json(e)).unwrap();
    // The gateway re-reads the file within a second.
    tokio::time::sleep(Duration::from_millis(2500)).await;
}

async fn teardown(e: &Env, root: &mut Conn) {
    for s in (1..=SHARDS_A)
        .map(|s| e.shard_schema(901, s))
        .chain((1..=SHARDS_B).map(|s| e.shard_schema(902, s)))
        .chain([e.ref_schema(), e.reference_db()])
    {
        root.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn differential_against_one_unsharded_mariadb() {
    let Some(e) = Env::load() else {
        eprintln!(
            "differential_live: set SPRIG_TEST_R1_GATEWAY, SPRIG_TEST_R1_TENANT_ID and SPRIG_TEST_R1_TOPOLOGY to run"
        );
        return;
    };
    let seed_value: u64 = env("SPRIG_TEST_R1_SEED")
        .and_then(|s| s.parse().ok())
        .unwrap_or(4242);
    let mut root = connect(&e.backend, "root", &e.root_password, None).await;
    setup(&e, &mut root).await;
    let mut sut = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
    let mut reference = connect(
        &e.backend,
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
    let full = |t: &str| match t {
        "customers" => "SELECT * FROM customers ORDER BY cid".to_owned(),
        _ => "SELECT * FROM orders ORDER BY cid, oid".to_owned(),
    };
    for (s, mode) in corpus(&mut r) {
        let a = run(&mut sut, &s).await;
        let b = run(&mut reference, &s).await;
        tally.check(&s, mode, &a, &b);
    }
    for (s, table) in dml(&mut r) {
        let a = run(&mut sut, &s).await;
        let b = run(&mut reference, &s).await;
        let skipped = matches!(a, Outcome::Err(39420, _));
        tally.check(&s, Cmp::Bag, &a, &b);
        if skipped {
            // Keep the reference in step: undo nothing, but the statement
            // did not run on the SUT, so run nothing more for it.
            if !matches!(b, Outcome::Err(..)) {
                // The reference changed and the SUT didn't: resync by
                // replaying the reference's table.
                resync(&mut sut, &mut reference, table).await;
            }
        }
        let q = full(table);
        let a = run(&mut sut, &q).await;
        let b = run(&mut reference, &q).await;
        tally.check(&q, Cmp::Ordered, &a, &b);
    }
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
    let q = full("orders");
    let a = run(&mut sut, &q).await;
    let b = run(&mut reference, &q).await;
    tally.check("after ROLLBACK", Cmp::Ordered, &a, &b);
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
    let secs = started.elapsed().as_secs_f64();
    println!(
        "differential: {} statements compared, {} skipped as not distributable ({:.1}%), {} mismatches, {secs:.1} s",
        tally.compared,
        tally.skipped,
        100.0 * tally.skipped as f64 / tally.compared.max(1) as f64,
        tally.mismatches.len()
    );
    println!("skipped by reason: {:?}", tally.reasons);
    for m in &tally.mismatches {
        println!("{m}");
    }
    let keep = env("SPRIG_TEST_R1_KEEP").is_some();
    if !keep {
        teardown(&e, &mut root).await;
        std::fs::write(&e.topology, r#"{"tenants": {}}"#).unwrap();
    }
    drop(sut);
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches",
        tally.mismatches.len()
    );
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

/// Single-shard latency (chapter 17: pass-through adds at most 50 µs at
/// p50 over R0 routing). Point selects: direct to the backend, through the
/// gateway on an unsharded table, through the gateway on a sharded table.
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
    let mut root = connect(&e.backend, "root", &e.root_password, None).await;
    setup(&e, &mut root).await;
    let ds = e.default_schema();
    let mut gw = connect(&e.gateway, &e.user, &e.password, Some(&e.db)).await;
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
    let mut direct = connect(&e.backend, "root", &e.root_password, Some(&ds)).await;
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
    let s = measure(&mut gw, n, |i| {
        format!("SELECT * FROM customers WHERE cid = {}", 1 + i % 1000)
    })
    .await;
    println!("latency p50/p99 in µs over {n} point selects:");
    println!("  direct to MariaDB:            {:.0} / {:.0}", d.0, d.1);
    println!(
        "  gateway, unsharded table:     {:.0} / {:.0}  (+{:.0} µs p50)",
        u.0,
        u.1,
        u.0 - d.0
    );
    println!(
        "  gateway, sharded, one shard:  {:.0} / {:.0}  (+{:.0} µs p50 over unsharded)",
        s.0,
        s.1,
        s.0 - u.0
    );
    gw.query_drop("DROP TABLE plain_lat").await.unwrap();
    teardown(&e, &mut root).await;
    std::fs::write(&e.topology, r#"{"tenants": {}}"#).unwrap();
}
