//! Live Bolt tests through a gateway (chapter 43, R8), with this
//! workspace's own Bolt client (no Neo4j driver crate is in the local
//! cache).
//!
//! - `gateway_bolt_end_to_end` needs `SPRIG_TEST_BOLT_ADDR` (the gateway's
//!   Bolt listener, TLS off), `SPRIG_TEST_BOLT_USER` and
//!   `SPRIG_TEST_BOLT_PASSWORD` (a `tenant.user` login), and
//!   `SPRIG_TEST_SQL_ADDR` (the gateway's MySQL listener) to create the
//!   graph `r8graphs_g.live` (the database `r8graphs_g` must exist).
//! - `differential_against_neo4j` also needs `SPRIG_TEST_NEO4J_ADDR` and
//!   `SPRIG_TEST_NEO4J_PASSWORD` (user `neo4j`): a reference Neo4j 5. It
//!   runs one corpus on both and compares the answers.
//!
//! - `round_two_end_to_end` (R8 round 2) checks signed bookmarks,
//!   streamed results, constraints and concurrent `MERGE` under one; with
//!   `SPRIG_TEST_BOLT_REPL_USER` (a `tenant.user` whose tenant lives on a
//!   primary with replicas) and `SPRIG_TEST_GW_METRICS` (the gateway's
//!   `/metrics` URL) it also checks that `mode: "r"` reads wait on a
//!   replica for the session's bookmark.
//!
//! - Round 3: `sharded_graph_split_and_move` (also `SPRIG_TEST_SPRIGCTL`
//!   and `SPRIGCTL_ENDPOINT`: two splits and two moves across clusters
//!   under concurrent writes), `mapped_graphs_graph_table_and_gql` (mapped
//!   graphs, `GRAPH_TABLE`, GQL forms), `bolt_sessions_in_processlist_and_kill`;
//!   `SPRIG_TEST_BOLT_DIFF_DB` points the differential at another graph.
//!
//! Without the variables the tests skip.

#![allow(clippy::many_single_char_names, clippy::items_after_statements)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use sprig_graph::eval::Params;
use sprig_graph::mem::MemStore;
use sprig_graph::{Budgets, execute};
use sprig_proto_bolt::client::{BoltClient, ClientError, Rows};
use sprig_proto_bolt::{PackValue, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type Client = BoltClient<TcpStream>;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn connect(addr: &str, user: &str, pw: &str) -> Result<Client, ClientError> {
    let s = TcpStream::connect(addr).await.map_err(|e| ClientError {
        code: String::new(),
        message: e.to_string(),
    })?;
    let mut c = BoltClient::handshake(s, None).await?;
    c.login(user, pw).await?;
    Ok(c)
}

/// A value as text with ids and element ids left out, so two servers
/// compare: nodes as `(:Labels {props})`, relationships as `[:T {props}]`.
fn norm(v: &PackValue) -> String {
    match v {
        PackValue::Null => "null".into(),
        PackValue::Bool(b) => b.to_string(),
        PackValue::Int(i) => i.to_string(),
        PackValue::Float(f) => format!("{f:?}"),
        PackValue::String(s) => format!("'{s}'"),
        PackValue::Bytes(b) => format!("{b:?}"),
        PackValue::List(l) => format!("[{}]", l.iter().map(norm).collect::<Vec<_>>().join(", ")),
        PackValue::Map(m) => {
            let mut kv: Vec<(String, String)> =
                m.iter().map(|(k, x)| (k.clone(), norm(x))).collect();
            kv.sort();
            let parts: Vec<String> = kv.into_iter().map(|(k, x)| format!("{k}: {x}")).collect();
            format!("{{{}}}", parts.join(", "))
        }
        PackValue::Struct { tag: 0x4E, fields } => {
            let mut labels: Vec<String> = fields[1]
                .as_list()
                .unwrap_or_default()
                .iter()
                .filter_map(|l| l.as_str().map(str::to_owned))
                .collect();
            labels.sort();
            format!("(:{} {})", labels.join(":"), norm(&fields[2]))
        }
        PackValue::Struct { tag: 0x52, fields } => {
            format!("[:{} {}]", norm_str(&fields[3]), norm(&fields[4]))
        }
        PackValue::Struct { tag: 0x72, fields } => {
            format!("[:{} {}]", norm_str(&fields[1]), norm(&fields[2]))
        }
        PackValue::Struct { tag: 0x50, fields } => {
            let nodes = fields[0].as_list().unwrap_or_default();
            let rels = fields[1].as_list().unwrap_or_default();
            let idx = fields[2].as_list().unwrap_or_default();
            let mut s = norm(&nodes[0]);
            for pair in idx.chunks(2) {
                let r = pair[0].as_int().unwrap_or(0);
                let n = usize::try_from(pair[1].as_int().unwrap_or(0)).unwrap_or(0);
                let rel = norm(&rels[usize::try_from(r.unsigned_abs()).unwrap_or(1) - 1]);
                if r > 0 {
                    let _ = write!(s, "-{rel}->");
                } else {
                    let _ = write!(s, "<-{rel}-");
                }
                s.push_str(&norm(&nodes[n]));
            }
            s
        }
        // Temporal and spatial structures: the tag and the fields.
        PackValue::Struct { tag, fields } => format!(
            "<{tag:#x} {}>",
            fields.iter().map(norm).collect::<Vec<_>>().join(" ")
        ),
    }
}

fn norm_str(v: &PackValue) -> String {
    v.as_str().unwrap_or_default().to_owned()
}

fn rows_text(r: &Rows, ordered: bool) -> Vec<String> {
    let mut v: Vec<String> = r
        .records
        .iter()
        .map(|rec| rec.iter().map(norm).collect::<Vec<_>>().join(" | "))
        .collect();
    if !ordered {
        v.sort();
    }
    v
}

const SETUP: &str = "CREATE (a:Person {name: 'Alice', email: 'alice@example.com', age: 30}),
    (b:Person {name: 'Bob', age: 25}), (c:Person {name: 'Carol', age: 35}),
    (d:Person {name: 'Dave', age: 40}), (e:Person {name: 'Eve'}),
    (p1:Product {name: 'Lamp', price: 10.5}), (p2:Product {name: 'Desk', price: 100}),
    (p3:Product {name: 'Pen', price: 1}),
    (a)-[:KNOWS {since: 2010}]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(a),
    (a)-[:BOUGHT {qty: 2}]->(p1), (b)-[:BOUGHT {qty: 1}]->(p2), (c)-[:BOUGHT {qty: 1}]->(p2),
    (c)-[:BOUGHT {qty: 5}]->(p3), (d)-[:BOUGHT {qty: 1}]->(p3)";

/// The differential corpus: each statement runs on both servers.
const CORPUS: &[&str] = &[
    "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS n ORDER BY n",
    "MATCH (p:Person) RETURN p ORDER BY p.name",
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN b.name, r.since, r",
    "MATCH (a {name: 'Alice'})-[:KNOWS]-(x) RETURN x.name ORDER BY x.name",
    "MATCH (a)<-[:KNOWS]-(b {name: 'Bob'}) RETURN a.name",
    "MATCH (n) RETURN count(*) AS c, count(n.age) AS a",
    "MATCH (n:Nope) RETURN count(*) AS c",
    "MATCH (a:Person {email: 'alice@example.com'})-[:KNOWS*1..2]->(f:Person)-[:BOUGHT]->(p:Product) WHERE NOT (a)-[:BOUGHT]->(p) AND f <> a RETURN p.name AS product, count(DISTINCT f) AS buyers ORDER BY buyers DESC, product LIMIT 5",
    "MATCH (a {name: 'Alice'})-[:KNOWS*1..3]->(x) RETURN DISTINCT x.name AS n ORDER BY n",
    "MATCH p = (a {name: 'Alice'})-[:KNOWS*1..4]->(x) RETURN x.name, length(p) ORDER BY length(p)",
    "MATCH p = (a {name: 'Alice'})-[:KNOWS*2]->(x) RETURN p",
    "MATCH (a {name: 'Alice'})-[:KNOWS*0..1]->(x) RETURN x.name ORDER BY x.name",
    "MATCH (a {name: 'Alice'})-[:KNOWS*]->(x) RETURN count(DISTINCT x) AS c",
    "MATCH (a {name: 'Alice'})-[r:KNOWS*2]->(x) RETURN size(r), x.name",
    "MATCH (a {name: 'Alice'})-[:KNOWS*2]-(x) RETURN x.name ORDER BY x.name",
    "MATCH (a {name: 'Alice'})<-[:KNOWS*1..2]-(x) RETURN DISTINCT x.name AS n ORDER BY n",
    "MATCH (a {name: 'Alice'}), (d {name: 'Dave'}) MATCH p = shortestPath((a)-[:KNOWS*]-(d)) RETURN length(p), [n IN nodes(p) | n.name]",
    "MATCH (a {name: 'Alice'}), (d {name: 'Dave'}) MATCH p = shortestPath((a)-[:KNOWS*]->(d)) RETURN p",
    "MATCH (a {name: 'Alice'}), (c {name: 'Carol'}) MATCH p = allShortestPaths((a)-[:KNOWS*]-(c)) RETURN [n IN nodes(p) | n.name] AS ns ORDER BY ns",
    "MATCH (p:Person) OPTIONAL MATCH (p)-[:BOUGHT]->(x) RETURN p.name, count(x) AS c ORDER BY p.name",
    "MATCH (p:Person) OPTIONAL MATCH (p)-[b:BOUGHT]->(x) RETURN p.name, x.name, b.qty ORDER BY p.name, x.name",
    "MATCH (p:Product) RETURN sum(p.price) AS s, avg(p.price) AS a, min(p.name) AS mn, max(p.price) AS mx",
    "MATCH (p:Product) RETURN collect(p.name) AS names",
    // Round 3: GQL's `IS` labels, chained comparisons and their
    // precedence, `truncate`, `ORDER BY` after `DISTINCT`.
    "MATCH (n IS Person)-[IS KNOWS]->(m IS Person) RETURN n.name, m.name ORDER BY n.name",
    "MATCH (p:Person) WHERE 20 < p.age <= 35 RETURN p.name ORDER BY p.name",
    "RETURN false = true IS NULL AS a, 1 < 2 = true AS b, 2 IN [1, 2] = true AS c",
    "RETURN date.truncate('month', date('2020-05-17')) AS d, toString(datetime.truncate('hour', datetime('2020-05-17T10:35:12+02:00'))) AS t, toString(localtime.truncate('minute', localtime('10:35:12.5'))) AS l",
    "MATCH (p:Person)-[:BOUGHT]->(x) RETURN DISTINCT x.name ORDER BY x.name",
    // `collect` keeps its input's order, which is storage order unless
    // sorted first (Neo4j reuses ids, so its order drifts between runs).
    "MATCH (p:Person)-[b:BOUGHT]->(x) WITH p, b, x ORDER BY p.name RETURN x.name, sum(b.qty) AS q, collect(DISTINCT p.name) AS who ORDER BY q DESC, x.name",
    "UNWIND [3, 1, 2, 1] AS x WITH DISTINCT x ORDER BY x DESC RETURN collect(x) AS xs",
    "UNWIND range(1, 10) AS x WITH x WHERE x % 3 = 0 RETURN x SKIP 1 LIMIT 1",
    "MATCH (p:Person) WITH p.age IS NULL AS missing, count(*) AS c RETURN missing, c ORDER BY missing",
    "RETURN 1 AS x UNION RETURN 1 AS x",
    "RETURN 1 AS x UNION ALL RETURN 1 AS x",
    "MATCH (n:Person) WHERE n.name =~ 'A.*' OR n.name IN ['Eve'] RETURN n.name ORDER BY n.name",
    "MATCH (n:Person) WHERE n.name STARTS WITH 'C' OR n.name ENDS WITH 'e' RETURN n.name ORDER BY n.name",
    "MATCH (a:Person) WHERE exists((a)-[:BOUGHT]->(:Product {name: 'Pen'})) RETURN a.name ORDER BY a.name",
    "MATCH (a:Person) WHERE NOT EXISTS { (a)-[:BOUGHT]->() } RETURN a.name",
    "MATCH (n:Product) WHERE n.price = 100 RETURN n.name",
    "MATCH (n:Product) WHERE n.price = '100' RETURN n.name",
    "MATCH (n:Product {price: 10.5}) RETURN n.name, n.price * 2 AS twice",
    "MATCH (x)-[r1:KNOWS]-(y)-[r2:KNOWS]-(z) WHERE x.name = 'Alice' RETURN z.name ORDER BY z.name",
    "MATCH (n:Person) RETURN n.name, CASE WHEN n.age IS NULL THEN 'unknown' WHEN n.age < 30 THEN 'young' ELSE 'older' END AS band ORDER BY n.name",
    "MATCH (n:Person) RETURN labels(n) AS l, size(keys(n)) AS k ORDER BY n.name LIMIT 2",
    "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY t",
    "RETURN toUpper('ab') AS a, substring('hello', 1, 3) AS b, size([1,2]) AS c, coalesce(null, 2) AS d, toInteger('42') AS e, toString(1.0) AS f, split('a,b', ',') AS g, reverse('abc') AS h, range(0, 10, 5) AS i",
    "RETURN 7 / 2 AS a, 7 / 2.0 AS b, -7 % 3 AS c, 2 ^ 3 AS d, 'a' + 1 AS e, [1, 2, 3][-1] AS f, [1,2,3,4][1..3] AS g, null IS NULL AS h, 1 = 1.0 AS i",
    "RETURN [x IN range(1, 6) WHERE x % 2 = 0 | x * 10] AS a, all(x IN [1, 2] WHERE x > 0) AS b, any(x IN [1, 2] WHERE x > 1) AS c",
    "MATCH (n:Person) WHERE n.age >= 30 RETURN n.name ORDER BY n.age DESC SKIP 1",
    "MATCH (a:Person {name: 'Alice'}) RETURN a {.name, .age} AS m",
    // Round 2: map projections, subqueries, FOREACH.
    "MATCH (p:Person) RETURN p {.name, k: size(p.name), .nope} AS m ORDER BY m.name",
    "MATCH (p:Person {name: 'Bob'}) RETURN p {.*} AS m",
    "MATCH (p:Person) CALL { WITH p MATCH (p)-[:BOUGHT]->(x) RETURN count(x) AS n } RETURN p.name, n ORDER BY p.name",
    "MATCH (p:Person {name: 'Carol'}) CALL (p) { MATCH (p)-[:BOUGHT]->(x) RETURN x.name AS x } RETURN x ORDER BY x",
    "CALL { MATCH (p:Product) RETURN p.name AS n UNION MATCH (p:Person) WHERE p.age > 35 RETURN p.name AS n } RETURN n ORDER BY n",
    "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:BOUGHT]->(x) WHERE x.price > 50 } RETURN p.name ORDER BY p.name",
    "MATCH (p:Person) RETURN p.name, COUNT { (p)-[:KNOWS]-() } AS k ORDER BY p.name",
    "MATCH (p:Person {name: 'Carol'}) RETURN COLLECT { MATCH (p)-[:BOUGHT]->(x) RETURN x.name ORDER BY x.name } AS xs",
    "MATCH (a:Person {name: 'Alice'}) FOREACH (i IN [1, 2, 3] | CREATE (a)-[:TAGGED]->(:Tag {i: i}))",
    "MATCH (:Person {name: 'Alice'})-[:TAGGED]->(t:Tag) RETURN sum(t.i) AS s, count(t) AS c",
    "MATCH (t:Tag) FOREACH (x IN CASE WHEN t.i > 1 THEN [1] ELSE [] END | SET t.big = true)",
    "MATCH (t:Tag) RETURN t.i, t.big ORDER BY t.i",
    "MATCH (t:Tag) DETACH DELETE t",
    // Temporal and spatial values.
    "RETURN date('2020-01-31') + duration('P1M') AS a, time('12:30:15.5+02:00') AS b, datetime('2020-01-05T10:00:00[Europe/Berlin]') AS c, duration.between(date('2020-01-01'), date('2021-03-04')) AS d, localdatetime('2020-01-05T10:00') AS e, localtime('12:00') AS f, datetime('2020-01-05T10:00:00+01:00') AS g",
    "RETURN toString(date('2020-01-05')) AS a, toString(localtime('12:00')) AS b, toString(datetime('2020-01-05T10:00:00[Europe/Berlin]')) AS c, toString(duration({days: 1, hours: 25})) AS d, date('2020-02-29').dayOfWeek AS e, date('2020-02-29').week AS f, datetime('2020-01-05T10:00:00+01:00').epochSeconds AS g, duration('PT90M').minutes AS h",
    "RETURN point({x: 1, y: 2}) AS a, point({latitude: 55.6, longitude: 12.5}) AS b, point.distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS c, point.distance(point({latitude: 55.6, longitude: 12.5}), point({latitude: 56.0, longitude: 13.0})) AS d, point({x: 1, y: 2, z: 3}).crs AS e, toString(point({x: 1, y: 2})) AS f",
    "CREATE (:Ev {at: date('2021-05-01'), t: localtime('09:30:00.25'), d: duration('P2D'), p: point({x: 1.5, y: 2}), ds: [date('2020-01-01'), date('2020-01-02')]}), (:Ev {at: date('2020-05-01'), t: localtime('10:00'), d: duration('P1D'), p: point({x: 0, y: 0})})",
    "MATCH (e:Ev) RETURN e.at, e.t, e.d, e.p, e.ds ORDER BY e.at",
    "MATCH (e:Ev) WHERE e.at > date('2021-01-01') RETURN e.at.year AS y, e.p.x AS x",
    "MATCH (e:Ev) DETACH DELETE e",
    // Indexes and constraints (schema persists on the reference: IF NOT
    // EXISTS / IF EXISTS keep reruns clean).
    "CREATE CONSTRAINT r8g2_person_email IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS UNIQUE",
    "CREATE INDEX r8g2_product_name IF NOT EXISTS FOR (p:Product) ON (p.name)",
    "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, owningConstraint WHERE name STARTS WITH 'r8g2' OR type = 'LOOKUP' RETURN * ORDER BY name",
    "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, ownedIndex WHERE name STARTS WITH 'r8g2' RETURN * ORDER BY name",
    "MATCH (p:Product {name: 'Desk'}) RETURN p.price",
    "MATCH (p:Person) WHERE p.email = 'alice@example.com' RETURN p.name",
    "MERGE (p:Person {email: 'alice@example.com'}) RETURN p.name, p.age",
    "DROP INDEX r8g2_product_name IF EXISTS",
    "DROP CONSTRAINT r8g2_person_email IF EXISTS",
];

/// Statements the differential leaves out, with the reason (shown in the
/// test output).
const KNOWN_GAPS: &[(&str, &str)] = &[];

async fn fresh(c: &mut Client, extra: &[(&str, PackValue)]) {
    c.query("MATCH (n) DETACH DELETE n", vec![], extra.to_vec())
        .await
        .expect("clean");
    c.query(SETUP, vec![], extra.to_vec()).await.expect("setup");
}

fn ordered(q: &str) -> bool {
    q.contains("ORDER BY")
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one end-to-end walk
async fn gateway_bolt_end_to_end() {
    let (Some(addr), Some(user), Some(pw), Some(sql)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_SQL_ADDR"),
    ) else {
        eprintln!(
            "skipped: SPRIG_TEST_BOLT_ADDR / _USER / _PASSWORD / SPRIG_TEST_SQL_ADDR not set"
        );
        return;
    };
    // The graph, through SQL.
    {
        let (host, port) = sql.split_once(':').expect("host:port");
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().expect("port"))
            .user(Some(user.clone()))
            .pass(Some(pw.clone()))
            .ssl_opts(Some(
                mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ));
        let mut c = mysql_async::Conn::new(opts).await.expect("sql login");
        c.query_drop("DROP GRAPH IF EXISTS r8graphs_g.live")
            .await
            .expect("drop");
        // The default graph (bolt.logical_database = r8graphs_g here).
        c.query_drop("DROP GRAPH IF EXISTS r8graphs_g.neo4j")
            .await
            .expect("drop default");
        c.query_drop("CREATE GRAPH IF NOT EXISTS r8graphs_g.diff")
            .await
            .expect("diff graph");
        c.query_drop("CREATE GRAPH r8graphs_g.live")
            .await
            .expect("create");
        c.disconnect().await.expect("bye");
    }
    let db = [("db", PackValue::str("r8graphs_g.live"))];
    // Logins.
    let e = connect(&addr, &user, "wrong")
        .await
        .expect_err("bad password");
    assert_eq!(e.code, "Neo.ClientError.Security.Unauthorized");
    let bare = user.split_once('.').map_or(user.as_str(), |(_, u)| u);
    let e = connect(&addr, bare, &pw).await.expect_err("no tenant");
    assert_eq!(e.code, "Neo.ClientError.Security.Unauthorized");
    let mut c = connect(&addr, &user, &pw).await.expect("login");
    assert_eq!(c.version.to_string(), "5.4");
    assert!(
        c.server
            .get("server")
            .and_then(PackValue::as_str)
            .is_some_and(|s| s.starts_with("Neo4j/"))
    );
    // Unknown graph.
    let e = c
        .query(
            "MATCH (n) RETURN n",
            vec![],
            vec![("db", PackValue::str("r8graphs_g.nope"))],
        )
        .await
        .expect_err("unknown graph");
    assert_eq!(e.code, "Neo.ClientError.Database.DatabaseNotFound");
    // The corpus agrees with the in-memory executor (4 shards).
    fresh(&mut c, &db).await;
    let mut mem = MemStore::new(4);
    execute(&mut mem, SETUP, &Params::new(), &Budgets::default())
        .await
        .expect("mem");
    let mut agree = 0;
    let mut timings = Vec::new();
    for q in CORPUS {
        if KNOWN_GAPS.iter().any(|(k, _)| q.contains(k)) {
            continue;
        }
        let t = Instant::now();
        let got = c
            .query(q, vec![], db.to_vec())
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        timings.push(t.elapsed());
        let want = execute(&mut mem, q, &Params::new(), &Budgets::default())
            .await
            .unwrap_or_else(|e| panic!("mem {q}: {e}"));
        assert_eq!(got.records.len(), want.rows.len(), "{q}");
        assert_eq!(got.fields, want.columns, "{q}");
        agree += 1;
    }
    timings.sort();
    eprintln!(
        "gateway vs memory: {agree} statements agree on row counts and columns; latency p50 {:?}, max {:?}",
        timings[timings.len() / 2],
        timings[timings.len() - 1]
    );
    // Graph structures on the wire.
    let r = c
        .query(
            "MATCH p = (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN a, r, p",
            vec![],
            db.to_vec(),
        )
        .await
        .expect("structs");
    let tags: Vec<u8> = r.records[0]
        .iter()
        .map(|v| match v {
            PackValue::Struct { tag, .. } => *tag,
            _ => 0,
        })
        .collect();
    assert_eq!(tags, [0x4E, 0x52, 0x50]);
    assert_eq!(
        norm(&r.records[0][2]),
        "(:Person {age: 30, email: 'alice@example.com', name: 'Alice'})-[:KNOWS {since: 2010}]->(:Person {age: 25, name: 'Bob'})"
    );
    // Parameters, and update counters in the summary.
    let r = c
        .query(
            "CREATE (n:Tmp {k: $k, xs: $xs}) RETURN n.k",
            vec![
                ("k", PackValue::str("v")),
                (
                    "xs",
                    PackValue::List(vec![PackValue::Int(1), PackValue::Int(2)]),
                ),
            ],
            db.to_vec(),
        )
        .await
        .expect("params");
    assert_eq!(r.records[0][0], PackValue::str("v"));
    assert_eq!(
        r.summary.get("type").and_then(PackValue::as_str),
        Some("rw")
    );
    assert_eq!(
        r.summary.get("stats").and_then(|s| s.get("nodes-created")),
        Some(&PackValue::Int(1))
    );
    assert!(
        r.summary
            .get("bookmark")
            .and_then(PackValue::as_str)
            .is_some_and(|b| b.starts_with("SB1:"))
    );
    // Explicit transactions: rollback, commit.
    c.begin(db.to_vec()).await.expect("begin");
    c.query("CREATE (:Tx {v: 1})", vec![], vec![])
        .await
        .expect("in tx");
    let r = c
        .query("MATCH (t:Tx) RETURN count(t)", vec![], vec![])
        .await
        .expect("own write");
    assert_eq!(r.records[0][0], PackValue::Int(1));
    c.rollback().await.expect("rollback");
    let r = c
        .query("MATCH (t:Tx) RETURN count(t)", vec![], db.to_vec())
        .await
        .expect("count");
    assert_eq!(r.records[0][0], PackValue::Int(0));
    c.begin(db.to_vec()).await.expect("begin");
    c.query("CREATE (:Tx {v: 2})", vec![], vec![])
        .await
        .expect("in tx");
    let b = c.commit().await.expect("commit");
    assert!(b.get("bookmark").is_some());
    let r = c
        .query("MATCH (t:Tx) RETURN t.v", vec![], db.to_vec())
        .await
        .expect("count");
    assert_eq!(r.records[0][0], PackValue::Int(2));
    // DELETE of a node with relationships fails at commit (39906).
    c.begin(db.to_vec()).await.expect("begin");
    c.query("MATCH (n:Person {name: 'Bob'}) DELETE n", vec![], vec![])
        .await
        .expect("delete");
    let e = c.commit().await.expect_err("dangling");
    assert_eq!(e.code, "Neo.ClientError.Schema.ConstraintValidationFailed");
    let r = c
        .query(
            "MATCH (n {name: 'Bob'}) RETURN count(n)",
            vec![],
            db.to_vec(),
        )
        .await
        .expect("bob");
    assert_eq!(r.records[0][0], PackValue::Int(1));
    // A syntax error, then the connection works again.
    let e = c
        .query("MATCH (n RETURN n", vec![], db.to_vec())
        .await
        .expect_err("syntax");
    assert_eq!(e.code, "Neo.ClientError.Statement.SyntaxError");
    let e = c
        .query("RETURN $nope", vec![], db.to_vec())
        .await
        .expect_err("param");
    assert_eq!(e.code, "Neo.ClientError.Statement.ParameterMissing");
    // Budgets: a chain of 40 refused when unbounded, exact when bounded.
    c.query(
        "UNWIND range(0, 39) AS i CREATE (:C {i: i})",
        vec![],
        db.to_vec(),
    )
    .await
    .expect("chain nodes");
    c.query(
        "MATCH (a:C), (b:C) WHERE b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)",
        vec![],
        db.to_vec(),
    )
    .await
    .expect("chain edges");
    let e = c
        .query(
            "MATCH (a:C {i: 0})-[:NEXT*]->(b) RETURN count(DISTINCT b)",
            vec![],
            db.to_vec(),
        )
        .await
        .expect_err("deep");
    assert_eq!(e.code, "Neo.ClientError.Statement.SprigGraphBudgetExceeded");
    assert!(e.message.contains("hops"), "{e}");
    let e = c
        .query(
            "MATCH (a:C {i: 0})-[:NEXT*1..30]->(b) RETURN b",
            vec![],
            db.to_vec(),
        )
        .await
        .expect_err("bound over max_hops");
    assert_eq!(e.code, "Neo.ClientError.Statement.SprigGraphBudgetExceeded");
    let r = c
        .query(
            "MATCH (a:C {i: 0})-[:NEXT*1..15]->(b) RETURN count(DISTINCT b) AS n",
            vec![],
            db.to_vec(),
        )
        .await
        .expect("bounded");
    assert_eq!(r.records[0][0], PackValue::Int(15));
    let e = c
        .query(
            "MATCH (a:C)-[:NEXT*1..15]->(b) RETURN a, b",
            vec![],
            vec![
                ("db", PackValue::str("r8graphs_g.live")),
                ("tx_timeout", PackValue::Int(1)),
            ],
        )
        .await
        .expect_err("time");
    assert_eq!(
        e.code, "Neo.ClientError.Statement.SprigGraphBudgetExceeded",
        "{e}"
    );
    // ROUTE.
    let rt = c.route(Some("r8graphs_g.live")).await.expect("route");
    let servers = rt
        .get("rt")
        .and_then(|t| t.get("servers"))
        .and_then(PackValue::as_list)
        .expect("servers");
    assert_eq!(servers.len(), 3);
    // PULL in batches.
    c.send(&Request::Run {
        query: "UNWIND range(1, 5) AS x RETURN x".into(),
        params: PackValue::Map(vec![]),
        extra: PackValue::map(db.to_vec()),
    })
    .await
    .expect("send");
    assert!(matches!(c.recv().await.expect("run"), Response::Success(_)));
    c.send(&Request::Pull(PackValue::map([("n", PackValue::Int(2))])))
        .await
        .expect("pull");
    let mut n = 0;
    loop {
        match c.recv().await.expect("recv") {
            Response::Record(_) => n += 1,
            Response::Success(m) => {
                assert_eq!(m.get("has_more"), Some(&PackValue::Bool(true)));
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(n, 2);
    c.reset().await.expect("reset");
    // The default graph: absent before its first write, then created.
    let r = c
        .query("MATCH (n) RETURN count(n) AS c", vec![], vec![])
        .await
        .expect("default read");
    assert!(r.records.is_empty() || r.records[0][0] == PackValue::Int(0));
    c.query("MERGE (:Default {k: 1})", vec![], vec![])
        .await
        .expect("default write");
    let r = c
        .query("MATCH (n:Default) RETURN count(n) AS c", vec![], vec![])
        .await
        .expect("default read 2");
    assert_eq!(r.records[0][0], PackValue::Int(1));
    c.goodbye().await.expect("goodbye");
    // Concurrent writers: 8 connections each create 50 linked nodes.
    let mut hs = Vec::new();
    let started = Instant::now();
    for w in 0..8 {
        let (addr, user, pw) = (addr.clone(), user.clone(), pw.clone());
        hs.push(tokio::spawn(async move {
            let mut c = connect(&addr, &user, &pw).await.expect("login");
            for i in 0..50 {
                c.query(
                    "MERGE (a:W {w: $w, i: $i}) WITH a MATCH (h:Person {name: 'Alice'}) CREATE (a)-[:SEEN]->(h)",
                    vec![("w", PackValue::Int(w)), ("i", PackValue::Int(i))],
                    vec![("db", PackValue::str("r8graphs_g.live"))],
                )
                .await
                .expect("concurrent write");
            }
        }));
    }
    for h in hs {
        h.await.expect("join");
    }
    let took = started.elapsed();
    let mut c = connect(&addr, &user, &pw).await.expect("login");
    let r = c
        .query(
            "MATCH (a:W)-[:SEEN]->(:Person {name: 'Alice'}) RETURN count(a)",
            vec![],
            db.to_vec(),
        )
        .await
        .expect("count");
    assert_eq!(r.records[0][0], PackValue::Int(400));
    eprintln!("400 concurrent MERGE+CREATE statements over 8 connections in {took:?}");
    c.query("MATCH (n) DETACH DELETE n", vec![], db.to_vec())
        .await
        .expect("clean");
    c.query("MATCH (n) DETACH DELETE n", vec![], vec![])
        .await
        .expect("clean default");
}

#[tokio::test]
async fn differential_against_neo4j() {
    let (Some(addr), Some(user), Some(pw), Some(neo), Some(neo_pw)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_NEO4J_ADDR"),
        env("SPRIG_TEST_NEO4J_PASSWORD"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* and SPRIG_TEST_NEO4J_ADDR / _PASSWORD not set");
        return;
    };
    // `SPRIG_TEST_BOLT_DIFF_DB` names another graph (round 3: one in a
    // database whose `sharding.min_shards` is 4).
    let diff_db = env("SPRIG_TEST_BOLT_DIFF_DB").unwrap_or_else(|| "r8graphs_g.diff".into());
    let db = [("db", PackValue::String(diff_db.clone()))];
    eprintln!("differential graph: {diff_db}");
    // The graph `r8graphs_g.diff` is created on demand through SQL by the
    // end-to-end test's helper; here the default graph route is avoided.
    let mut g = connect(&addr, &user, &pw).await.expect("gateway");
    let mut n = connect(&neo, "neo4j", &neo_pw).await.expect("neo4j");
    eprintln!(
        "reference: {}",
        n.server
            .get("server")
            .and_then(PackValue::as_str)
            .unwrap_or_default()
    );
    fresh(&mut g, &db).await;
    fresh(&mut n, &[]).await;
    let mut same = 0;
    let mut diffs: BTreeMap<&str, (Vec<String>, Vec<String>)> = BTreeMap::new();
    for q in CORPUS {
        if let Some((_, why)) = KNOWN_GAPS.iter().find(|(k, _)| q.contains(k)) {
            eprintln!("known gap: {q}: {why}");
            continue;
        }
        let want = n
            .query(q, vec![], vec![])
            .await
            .unwrap_or_else(|e| panic!("neo4j {q}: {e}"));
        let got = match g.query(q, vec![], db.to_vec()).await {
            Ok(r) => r,
            Err(e) => {
                diffs.insert(q, (vec![format!("error {e}")], rows_text(&want, true)));
                continue;
            }
        };
        let (a, b) = (rows_text(&got, ordered(q)), rows_text(&want, ordered(q)));
        if a == b && got.fields == want.fields {
            same += 1;
        } else {
            let mut a = a;
            a.insert(0, format!("columns {:?}", got.fields));
            let mut b = b;
            b.insert(0, format!("columns {:?}", want.fields));
            diffs.insert(q, (a, b));
        }
    }
    for (q, (a, b)) in &diffs {
        eprintln!("DIFF {q}\n  sprig: {a:?}\n  neo4j: {b:?}");
    }
    eprintln!(
        "differential: {same} of {} statements identical to Neo4j ({} known gaps)",
        CORPUS.len() - KNOWN_GAPS.len(),
        KNOWN_GAPS.len()
    );
    g.query("MATCH (n) DETACH DELETE n", vec![], db.to_vec())
        .await
        .expect("clean");
    n.query("MATCH (n) DETACH DELETE n", vec![], vec![])
        .await
        .expect("clean");
    assert!(diffs.is_empty(), "{} statements differ", diffs.len());
    tokio::time::sleep(Duration::from_millis(1)).await;
}

/// Pulls everything of an open result with `PULL {n}` batches: (records,
/// batches, the last summary).
async fn pull_all(c: &mut Client, n: i64, qid: i64) -> (usize, usize, PackValue) {
    let mut recs = 0;
    let mut batches = 0;
    loop {
        c.send(&Request::Pull(PackValue::map([
            ("n", PackValue::Int(n)),
            ("qid", PackValue::Int(qid)),
        ])))
        .await
        .expect("pull");
        batches += 1;
        loop {
            match c.recv().await.expect("recv") {
                Response::Record(_) => recs += 1,
                Response::Success(m) => {
                    if m.get("has_more") == Some(&PackValue::Bool(true)) {
                        break;
                    }
                    return (recs, batches, m);
                }
                other => panic!("{other:?}"),
            }
        }
    }
}

async fn run_only(c: &mut Client, q: &str, extra: Vec<(&str, PackValue)>) -> PackValue {
    c.send(&Request::Run {
        query: q.into(),
        params: PackValue::Map(vec![]),
        extra: PackValue::map(extra),
    })
    .await
    .expect("send");
    match c.recv().await.expect("run") {
        Response::Success(m) => m,
        other => panic!("{q}: {other:?}"),
    }
}

async fn metric(url: &str, name: &str) -> f64 {
    let Some(rest) = url.strip_prefix("http://") else {
        return 0.0;
    };
    let (host, path) = rest.split_once('/').unwrap_or((rest, "metrics"));
    let Ok(mut s) = TcpStream::connect(host).await else {
        return 0.0;
    };
    let _ = s
        .write_all(format!("GET /{path} HTTP/1.0\r\nHost: {host}\r\n\r\n").as_bytes())
        .await;
    let mut body = String::new();
    let _ = s.read_to_string(&mut body).await;
    body.lines()
        .filter(|l| l.starts_with(name))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one walk over the round-two features
async fn round_two_end_to_end() {
    let (Some(addr), Some(user), Some(pw)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* not set");
        return;
    };
    let db = || vec![("db", PackValue::str("r8graphs_g.round2"))];
    if let Some(sql) = env("SPRIG_TEST_SQL_ADDR") {
        let (host, port) = sql.split_once(':').expect("host:port");
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().expect("port"))
            .user(Some(user.clone()))
            .pass(Some(pw.clone()))
            .ssl_opts(Some(
                mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ));
        let mut s = mysql_async::Conn::new(opts).await.expect("sql login");
        s.query_drop("CREATE GRAPH IF NOT EXISTS r8graphs_g.round2")
            .await
            .expect("graph");
        s.disconnect().await.expect("bye");
    }
    let mut c = connect(&addr, &user, &pw).await.expect("login");
    c.query("MATCH (n) DETACH DELETE n", vec![], db())
        .await
        .expect("clean");
    // Signed bookmarks: a commit's bookmark is accepted, a forged one not.
    c.begin(db()).await.expect("begin");
    c.query("CREATE (:Bm {v: 1})", vec![], vec![])
        .await
        .expect("write");
    let b = c.commit().await.expect("commit");
    let bm = b
        .get("bookmark")
        .and_then(PackValue::as_str)
        .expect("bookmark")
        .to_owned();
    assert!(bm.starts_with("SB1:") && bm.contains('.'), "{bm}");
    let mut extra = db();
    extra.push((
        "bookmarks",
        PackValue::List(vec![PackValue::str(bm.clone())]),
    ));
    let r = c
        .query("MATCH (b:Bm) RETURN count(b)", vec![], extra)
        .await
        .expect("with bookmark");
    assert_eq!(r.records[0][0], PackValue::Int(1));
    let forged = format!("{}x", &bm[..bm.len() - 1]);
    let mut extra = db();
    extra.push(("bookmarks", PackValue::List(vec![PackValue::str(forged)])));
    let e = c
        .query("MATCH (b:Bm) RETURN count(b)", vec![], extra)
        .await
        .expect_err("forged");
    assert_eq!(e.code, "Neo.ClientError.Transaction.InvalidBookmark", "{e}");
    let e = c
        .begin(vec![
            ("db", PackValue::str("r8graphs_g.round2")),
            (
                "bookmarks",
                PackValue::List(vec![PackValue::str("SB1:x.y")]),
            ),
        ])
        .await
        .expect_err("garbage bookmark");
    assert_eq!(e.code, "Neo.ClientError.Transaction.InvalidBookmark", "{e}");
    c.reset().await.expect("reset");
    // Streamed results: 3,000 rows in batches of 250.
    c.query(
        "UNWIND range(1, 3000) AS i CREATE (:Big {i: i})",
        vec![],
        db(),
    )
    .await
    .expect("big");
    let started = Instant::now();
    let m = run_only(&mut c, "MATCH (n:Big) RETURN n.i AS i", db()).await;
    let first = started.elapsed();
    assert_eq!(
        m.get("fields"),
        Some(&PackValue::List(vec![PackValue::str("i")]))
    );
    let (n, batches, end) = pull_all(&mut c, 250, -1).await;
    assert_eq!((n, batches), (3000, 12));
    assert!(end.get("bookmark").is_some(), "{end:?}");
    eprintln!(
        "streamed 3000 rows in 12 PULLs; first answer after {first:?}, all after {:?}",
        started.elapsed()
    );
    // DISCARD in the middle ends the statement.
    run_only(&mut c, "MATCH (n:Big) RETURN n", db()).await;
    c.send(&Request::Pull(PackValue::map([("n", PackValue::Int(10))])))
        .await
        .expect("pull");
    let mut got = 0;
    loop {
        match c.recv().await.expect("recv") {
            Response::Record(_) => got += 1,
            Response::Success(_) => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(got, 10);
    c.send(&Request::Discard(PackValue::map([(
        "n",
        PackValue::Int(-1),
    )])))
    .await
    .expect("discard");
    assert!(matches!(
        c.recv().await.expect("discard"),
        Response::Success(_)
    ));
    // In a transaction: a second RUN while the first streams buffers it.
    c.begin(db()).await.expect("begin");
    run_only(&mut c, "MATCH (n:Big) WHERE n.i <= 1500 RETURN n.i", vec![]).await;
    run_only(&mut c, "MATCH (n:Big) WHERE n.i > 1500 RETURN n.i", vec![]).await;
    let (a, _, _) = pull_all(&mut c, 100, 1).await;
    let (b, _, _) = pull_all(&mut c, 100, 0).await;
    assert_eq!((a, b), (1500, 1500));
    c.commit().await.expect("commit");
    // Constraints: violations, creation over duplicates, MERGE.
    c.query(
        "CREATE CONSTRAINT live_k IF NOT EXISTS FOR (n:U) REQUIRE n.k IS UNIQUE",
        vec![],
        db(),
    )
    .await
    .expect("constraint");
    c.query("CREATE (:U {k: 'a'})", vec![], db())
        .await
        .expect("first");
    let e = c
        .query("CREATE (:U {k: 'a'})", vec![], db())
        .await
        .expect_err("duplicate");
    assert_eq!(
        e.code, "Neo.ClientError.Schema.ConstraintValidationFailed",
        "{e}"
    );
    assert!(e.message.contains("already exists with label `U`"), "{e}");
    let r = c
        .query(
            "SHOW CONSTRAINTS YIELD name, type RETURN name, type",
            vec![],
            db(),
        )
        .await
        .expect("show");
    assert_eq!(r.records.len(), 1);
    c.query("CREATE (:V {k: 1}), (:V {k: 1})", vec![], db())
        .await
        .expect("dups");
    let e = c
        .query(
            "CREATE CONSTRAINT FOR (n:V) REQUIRE n.k IS UNIQUE",
            vec![],
            db(),
        )
        .await
        .expect_err("existing duplicates");
    assert_eq!(
        e.code, "Neo.ClientError.Schema.ConstraintCreationFailed",
        "{e}"
    );
    // Concurrent MERGE on the constrained key: one node.
    let mut hs = Vec::new();
    for _ in 0..8 {
        let (addr, user, pw) = (addr.clone(), user.clone(), pw.clone());
        hs.push(tokio::spawn(async move {
            let mut c = connect(&addr, &user, &pw).await.expect("login");
            for i in 0..20 {
                c.query(
                    "MERGE (n:U {k: $k}) ON CREATE SET n.c = 1 ON MATCH SET n.m = coalesce(n.m, 0) + 1",
                    vec![("k", PackValue::str(format!("m{}", i % 5)))],
                    vec![("db", PackValue::str("r8graphs_g.round2"))],
                )
                .await
                .expect("merge");
            }
        }));
    }
    for h in hs {
        h.await.expect("join");
    }
    let r = c
        .query(
            "MATCH (n:U) WHERE n.k STARTS WITH 'm' RETURN count(n), sum(n.m)",
            vec![],
            db(),
        )
        .await
        .expect("merged");
    assert_eq!(r.records[0][0], PackValue::Int(5), "{:?}", r.records);
    assert_eq!(r.records[0][1], PackValue::Int(155), "{:?}", r.records);
    // The index serves equality lookups.
    let r = c
        .query("PROFILE MATCH (n:U {k: 'a'}) RETURN n.k", vec![], db())
        .await
        .expect("profile");
    assert_eq!(r.records.len(), 1);
    c.query("DROP CONSTRAINT live_k", vec![], db())
        .await
        .expect("drop");
    c.query("MATCH (n) DETACH DELETE n", vec![], db())
        .await
        .expect("clean");
    // Replica reads that wait for the session's bookmark.
    let (Some(ruser), Some(metrics)) = (
        env("SPRIG_TEST_BOLT_REPL_USER"),
        env("SPRIG_TEST_GW_METRICS"),
    ) else {
        eprintln!(
            "replica reads skipped: SPRIG_TEST_BOLT_REPL_USER / SPRIG_TEST_GW_METRICS not set"
        );
        return;
    };
    let rdb = || vec![("db", PackValue::str("r8graphs_g.repl"))];
    let mut r = connect(&addr, &ruser, &pw).await.expect("repl login");
    let ok_before = metric(&metrics, "sprig_bolt_bookmark_waits_total{outcome=\"ok\"").await;
    let mut misses = 0;
    for i in 0..20 {
        r.begin(rdb()).await.expect("begin");
        r.query(
            "CREATE (:R {i: $i})",
            vec![("i", PackValue::Int(i))],
            vec![],
        )
        .await
        .expect("write");
        let b = r.commit().await.expect("commit");
        let bm = b
            .get("bookmark")
            .and_then(PackValue::as_str)
            .expect("bm")
            .to_owned();
        let mut extra = rdb();
        extra.push(("mode", PackValue::str("r")));
        extra.push(("bookmarks", PackValue::List(vec![PackValue::str(bm)])));
        let got = r
            .query("MATCH (n:R) RETURN count(n) AS c", vec![], extra)
            .await
            .expect("replica read");
        if got.records[0][0] != PackValue::Int(i + 1) {
            misses += 1;
        }
    }
    let ok_after = metric(&metrics, "sprig_bolt_bookmark_waits_total{outcome=\"ok\"").await;
    eprintln!(
        "replica reads: 20 read-your-writes checks, {misses} misses, {} replica waits",
        ok_after - ok_before
    );
    assert_eq!(misses, 0);
    assert!(ok_after > ok_before, "no read waited on a replica");
    r.query("MATCH (n) DETACH DELETE n", vec![], rdb())
        .await
        .expect("clean");
}

/// `sprigctl -o json <args>` (the binary `SPRIG_TEST_SPRIGCTL`, the Admin
/// API from `SPRIGCTL_ENDPOINT`).
fn sprigctl(bin: &str, args: &[&str]) -> serde_json::Value {
    let out = std::process::Command::new(bin)
        .arg("-o")
        .arg("json")
        .args(args)
        .output()
        .expect("sprigctl");
    assert!(
        out.status.success(),
        "sprigctl {args:?}: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null)
}

/// `sprigctl -o json <args>`, `None` on a failure (etcd under load).
fn sprigctl_try(bin: &str, args: &[&str]) -> Option<serde_json::Value> {
    let out = std::process::Command::new(bin)
        .arg("-o")
        .arg("json")
        .args(args)
        .output()
        .ok()?;
    if !out.status.success() {
        eprintln!(
            "sprigctl {args:?} (retried): {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
}

/// Waits for mover job `id` to commit (or fails the test).
async fn wait_job(bin: &str, id: &str) -> Duration {
    let t = Instant::now();
    loop {
        let Some(j) = sprigctl_try(bin, &["job", "show", id]) else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let state = j["state"].as_str().unwrap_or_default().to_owned();
        if ["post_cutover", "retaining", "cleaning", "done"].contains(&state.as_str()) {
            return t.elapsed();
        }
        assert!(
            !["rolling_back", "rolled_back", "failed", "cancelled"].contains(&state.as_str()),
            "job {id}: {j}"
        );
        assert!(
            t.elapsed() < Duration::from_secs(600),
            "job {id} stuck: {j}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The shards of the graph's group, from `sprigctl shard list`:
/// `(shard, cluster)`.
fn graph_shards(bin: &str, tenant: &str, group: &str) -> (u64, Vec<(u64, String)>) {
    let l = sprigctl(bin, &["shard", "list", tenant]);
    let groups = l.as_array().cloned().unwrap_or_default();
    let g = groups
        .iter()
        .find(|g| g["name"].as_str() == Some(group))
        .unwrap_or_else(|| panic!("no group {group} in {l}"));
    let id = g["group"].as_u64().expect("group id");
    let shards = g["shards"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|s| {
            (
                s["shard"].as_u64().unwrap_or(0),
                s["cluster"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    (id, shards)
}

/// Round 3 (decision G25 steps 1–4): a graph is a sharded table group the
/// mover splits and moves like any other. Answers before, during and after
/// two splits and two moves (across the three dev clusters) stay the same,
/// and writes running throughout all land exactly once.
///
/// Needs the end-to-end variables plus `SPRIG_TEST_SPRIGCTL` (a sprigctl
/// binary) and `SPRIGCTL_ENDPOINT`; `SPRIG_TEST_TENANT` (default `acme`).
#[tokio::test]
#[allow(clippy::too_many_lines)] // load, split, split, move, move, check
async fn sharded_graph_split_and_move() {
    let (Some(addr), Some(user), Some(pw), Some(sql), Some(ctl)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_SQL_ADDR"),
        env("SPRIG_TEST_SPRIGCTL"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* / SPRIG_TEST_SQL_ADDR / SPRIG_TEST_SPRIGCTL not set");
        return;
    };
    let tenant = env("SPRIG_TEST_TENANT").unwrap_or_else(|| "acme".into());
    {
        let (host, port) = sql.split_once(':').expect("host:port");
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().expect("port"))
            .user(Some(user.clone()))
            .pass(Some(pw.clone()))
            .ssl_opts(Some(
                mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ));
        let mut c = mysql_async::Conn::new(opts).await.expect("sql login");
        c.query_drop("CREATE DATABASE IF NOT EXISTS r8g3_split")
            .await
            .expect("db");
        c.query_drop("DROP GRAPH IF EXISTS r8g3_split.sg")
            .await
            .expect("drop");
        c.query_drop("CREATE GRAPH r8g3_split.sg")
            .await
            .expect("create");
        c.disconnect().await.expect("bye");
    }
    let db = vec![("db", PackValue::str("r8g3_split.sg"))];
    let mut c = connect(&addr, &user, &pw).await.expect("login");
    let ring = "UNWIND range(0, 299) AS i CREATE (:R {i: i})";
    let link = "MATCH (a:R), (b:R) WHERE b.i = (a.i + 1) % 300 OR b.i = (a.i * 7) % 300 CREATE (a)-[:NEXT {w: a.i}]->(b)";
    c.query(ring, vec![], db.clone()).await.expect("ring");
    c.query(link, vec![], db.clone()).await.expect("link");
    let eid = c
        .query("MATCH (n:R {i: 0}) RETURN elementId(n)", vec![], db.clone())
        .await
        .expect("element id");
    let graph_id = eid.records[0][0]
        .as_str()
        .and_then(|s| s.split(':').nth(1))
        .expect("graph id")
        .to_owned();
    let group_name = format!("graph_{graph_id}");
    let corpus = [
        "MATCH (n:R) RETURN count(n)",
        "MATCH ()-[r:NEXT]->() RETURN count(r), sum(r.w)",
        "MATCH (a:R {i: 5})-[:NEXT*1..4]->(x) RETURN count(DISTINCT x)",
        "MATCH (a:R {i: 5})<-[:NEXT*1..3]-(x) RETURN count(DISTINCT x)",
        "MATCH (a:R {i: 0})<-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
        "MATCH (a:R {i: 1}), (b:R {i: 151}) MATCH p = shortestPath((a)-[:NEXT*]->(b)) RETURN length(p)",
        "MATCH (a:R)-[:NEXT]->(b:R) WHERE a.i < 20 RETURN a.i, collect(b.i) ORDER BY a.i",
    ];
    let mut before = Vec::new();
    for q in corpus {
        before.push(rows_text(
            &c.query(q, vec![], db.clone()).await.expect(q),
            true,
        ));
    }
    // Writes throughout: nodes and an edge between consecutive ones.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = std::sync::Arc::clone(&stop);
        let (addr, user, pw) = (addr.clone(), user.clone(), pw.clone());
        let db = db.clone();
        tokio::spawn(async move {
            let mut w = connect(&addr, &user, &pw).await.expect("writer login");
            let (mut ok, mut retried, mut k) = (0i64, 0u64, 0i64);
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let q = "MERGE (a:W {k: $k}) WITH a OPTIONAL MATCH (p:W {k: $k - 1}) FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END | CREATE (p)-[:THEN]->(a))";
                loop {
                    match w.query(q, vec![("k", PackValue::Int(k))], db.clone()).await {
                        Ok(_) => break,
                        Err(e) if e.code.starts_with("Neo.TransientError") => {
                            retried += 1;
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(e) => panic!("writer {k}: {e}"),
                    }
                }
                ok += 1;
                k += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            (ok, retried)
        })
    };
    let (gid, shards) = graph_shards(&ctl, &tenant, &group_name);
    let gid = gid.to_string();
    assert_eq!(shards.len(), 1, "a new graph starts with one shard");
    let mut steps = Vec::new();
    async fn same(
        c: &mut Client,
        db: &[(&str, PackValue)],
        corpus: &[&str],
        before: &[Vec<String>],
        label: &str,
    ) {
        for (q, want) in corpus.iter().zip(before) {
            let got = c.query(q, vec![], db.to_vec()).await.expect(q);
            assert_eq!(&rows_text(&got, true), want, "{label}: {q}");
        }
    }
    // Split shard 1, then the larger child again.
    let first = shards[0].0.to_string();
    let j = sprigctl(&ctl, &["shard", "split", &tenant, &gid, &first]);
    let id = j["id"].as_str().expect("job id").to_owned();
    steps.push(("split 1 -> 2", wait_job(&ctl, &id).await));
    same(&mut c, &db, &corpus, &before, "after the first split").await;
    let (_, shards) = graph_shards(&ctl, &tenant, &group_name);
    assert_eq!(shards.len(), 2, "{shards:?}");
    let s = shards[1].0.to_string();
    let j = sprigctl(&ctl, &["shard", "split", &tenant, &gid, &s]);
    let id = j["id"].as_str().expect("job id").to_owned();
    steps.push(("split 2 -> 3", wait_job(&ctl, &id).await));
    same(&mut c, &db, &corpus, &before, "after the second split").await;
    // Move two shards to the other clusters.
    let (_, shards) = graph_shards(&ctl, &tenant, &group_name);
    assert_eq!(shards.len(), 3, "{shards:?}");
    for ((s, _), to) in shards.iter().skip(1).zip(["dc-a-c2", "dc-a-c3"]) {
        let s = s.to_string();
        let j = sprigctl(&ctl, &["shard", "move", &tenant, &gid, &s, "--to", to]);
        let id = j["id"].as_str().expect("job id").to_owned();
        let label = if to == "dc-a-c2" {
            "move to c2"
        } else {
            "move to c3"
        };
        steps.push((label, wait_job(&ctl, &id).await));
        same(&mut c, &db, &corpus, &before, label).await;
    }
    let (_, shards) = graph_shards(&ctl, &tenant, &group_name);
    let clusters: std::collections::BTreeSet<&str> =
        shards.iter().map(|(_, c)| c.as_str()).collect();
    assert_eq!(clusters.len(), 3, "{shards:?}");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (ok, retried) = writer.await.expect("writer");
    let w = c
        .query(
            "MATCH (a:W) WITH count(a) AS n, count(DISTINCT a.k) AS d MATCH ()-[t:THEN]->() RETURN n, d, count(t)",
            vec![],
            db.clone(),
        )
        .await
        .expect("writes");
    let row: Vec<i64> = w.records[0].iter().filter_map(PackValue::as_int).collect();
    eprintln!(
        "split and move: {steps:?}; shards now {shards:?}; writer: {ok} statements, {retried} retried; graph has {row:?}"
    );
    assert_eq!(row, vec![ok, ok, ok - 1], "every write landed once");
    // Edges crossing clusters: incoming hops and a commit check.
    let e = c
        .query("MATCH (n:R {i: 0}) DELETE n", vec![], db.clone())
        .await
        .expect_err("dangling");
    assert_eq!(e.code, "Neo.ClientError.Schema.ConstraintValidationFailed");
    let count = |rows: &Rows| rows.records[0][0].as_int().unwrap_or(-1);
    let all = count(
        &c.query("MATCH ()-[r:NEXT]->() RETURN count(r)", vec![], db.clone())
            .await
            .expect("count"),
    );
    let touching = count(
        &c.query(
            "MATCH (n:R {i: 0})-[r]-() RETURN count(DISTINCT r)",
            vec![],
            db.clone(),
        )
        .await
        .expect("touching"),
    );
    c.query("MATCH (n:R {i: 0}) DETACH DELETE n", vec![], db.clone())
        .await
        .expect("detach");
    let o = c
        .query("MATCH ()-[r:NEXT]->() RETURN count(r)", vec![], db.clone())
        .await
        .expect("after detach");
    assert_eq!(count(&o), all - touching);
}

/// Round 3: a mapped graph over SQL tables (`CREATE PROPERTY GRAPH`),
/// read through Bolt and through `GRAPH_TABLE` in SQL; `GRAPH_TABLE` over a
/// native graph; GQL's forms through Bolt. Answers are checked against the
/// same question in plain SQL.
#[tokio::test]
#[allow(clippy::too_many_lines)] // tables, graph, Bolt, SQL
async fn mapped_graphs_graph_table_and_gql() {
    let (Some(addr), Some(user), Some(pw), Some(sql)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_SQL_ADDR"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* / SPRIG_TEST_SQL_ADDR not set");
        return;
    };
    let (host, port) = sql.split_once(':').expect("host:port");
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().expect("port"))
        .user(Some(user.clone()))
        .pass(Some(pw.clone()))
        .db_name(Some("r8g3_mapped"))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ));
    {
        let mut c = mysql_async::Conn::new(opts.clone().db_name(None::<String>))
            .await
            .expect("sql login");
        c.query_drop("CREATE DATABASE IF NOT EXISTS r8g3_mapped")
            .await
            .expect("db");
        c.disconnect().await.expect("bye");
    }
    let mut s = mysql_async::Conn::new(opts).await.expect("sql login");
    for q in [
        "DROP GRAPH IF EXISTS shop",
        "DROP GRAPH IF EXISTS social",
        "DROP TABLE IF EXISTS orders, customers, products",
        "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(40), email VARCHAR(80), joined DATE)",
        "CREATE TABLE products (sku INT PRIMARY KEY, name VARCHAR(40), price DECIMAL(8,2))",
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, sku INT, qty INT, placed_at DATETIME)",
        "INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com', '2020-01-02'), (2, 'Bob', 'bob@example.com', '2021-03-04'), (3, 'Carol', NULL, NULL)",
        "INSERT INTO products VALUES (10, 'Lamp', 10.50), (11, 'Desk', 100.00), (12, 'Pen', 1.25)",
        "INSERT INTO orders VALUES (100, 1, 10, 2, '2024-01-01 10:00:00'), (101, 1, 12, 10, '2024-01-02 11:00:00'), (102, 2, 11, 1, '2024-02-01 09:30:00'), (103, 2, 12, 3, '2024-02-03 08:00:00'), (104, 3, 10, 1, '2024-03-01 12:00:00')",
        "CREATE PROPERTY GRAPH shop
           VERTEX TABLES (
             customers KEY (id) LABEL Person PROPERTIES (id, name, email, joined),
             products KEY (sku) LABEL Product PROPERTIES (sku, name, price))
           EDGE TABLES (
             orders KEY (id)
               SOURCE KEY (customer_id) REFERENCES customers (id)
               DESTINATION KEY (sku) REFERENCES products (sku)
               LABEL BOUGHT PROPERTIES (qty, placed_at))",
        "CREATE GRAPH social",
    ] {
        s.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    // Errors: a missing table, a composite key, a write.
    let e = s
        .query_drop("CREATE PROPERTY GRAPH bad VERTEX TABLES (nope KEY (id))")
        .await
        .expect_err("missing table");
    assert!(e.to_string().contains("doesn't exist"), "{e}");
    let mut c = connect(&addr, &user, &pw).await.expect("login");
    let shop = vec![("db", PackValue::str("r8g3_mapped.shop"))];
    let rows = |r: &Rows| rows_text(r, true);
    // Bolt over the mapped graph.
    let got = c
        .query(
            "MATCH (c:Person)-[o:BOUGHT]->(p:Product) RETURN c.name, p.name, o.qty ORDER BY c.name, p.name",
            vec![],
            shop.clone(),
        )
        .await
        .expect("mapped match");
    assert_eq!(
        rows(&got),
        [
            "'Alice' | 'Lamp' | 2",
            "'Alice' | 'Pen' | 10",
            "'Bob' | 'Desk' | 1",
            "'Bob' | 'Pen' | 3",
            "'Carol' | 'Lamp' | 1"
        ],
        "{:?}",
        rows(&got)
    );
    let got = c
        .query(
            "MATCH (p:Product {name: 'Pen'})<-[:BOUGHT]-(c) RETURN c.name, c.joined ORDER BY c.name",
            vec![],
            shop.clone(),
        )
        .await
        .expect("incoming");
    assert_eq!(got.records.len(), 2);
    let got = c
        .query(
            "MATCH (a:Person {name: 'Alice'})-[:BOUGHT]->(:Product)<-[:BOUGHT]-(b:Person) WHERE a <> b RETURN DISTINCT b.name AS n ORDER BY n",
            vec![],
            shop.clone(),
        )
        .await
        .expect("two hops");
    assert_eq!(rows(&got), ["'Bob'", "'Carol'"]);
    let e = c
        .query("CREATE (:Person {id: 9})", vec![], shop.clone())
        .await
        .expect_err("read-only");
    assert!(e.code.contains("SprigGraphReadOnlySource"), "{e:?}");
    // GQL forms through Bolt.
    let got = c
        .query(
            "MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product) FILTER o.qty >= 2 LET t = o.qty * p.price RETURN c.name AS n, t ORDER BY n, t",
            vec![],
            shop.clone(),
        )
        .await
        .expect("gql");
    assert_eq!(got.records.len(), 3, "{:?}", rows(&got));
    // GRAPH_TABLE in SQL, the chapter's example, against plain SQL.
    let pgq: Vec<(String, f64)> = s
        .query(
            "SELECT g.name, SUM(g.total) AS spent FROM GRAPH_TABLE (shop
               MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product)
               WHERE c.email = 'alice@example.com'
               COLUMNS (p.name AS name, o.qty * p.price AS total)) AS g
             GROUP BY g.name ORDER BY g.name",
        )
        .await
        .expect("graph_table");
    let plain: Vec<(String, f64)> = s
        .query(
            "SELECT p.name, SUM(o.qty * p.price) FROM customers c JOIN orders o ON o.customer_id = c.id
             JOIN products p ON p.sku = o.sku WHERE c.email = 'alice@example.com' GROUP BY p.name ORDER BY p.name",
        )
        .await
        .expect("plain");
    assert_eq!(pgq, plain);
    // GRAPH_TABLE joins with tables, and reads native graphs too.
    let social = vec![("db", PackValue::str("r8g3_mapped.social"))];
    c.query(
        "CREATE (:P {cid: 1})-[:FOLLOWS]->(:P {cid: 2}), (:P {cid: 3})-[:FOLLOWS]->(:P {cid: 1})",
        vec![],
        social.clone(),
    )
    .await
    .expect("native data");
    let j: Vec<(String, String)> = s
        .query(
            "SELECT a.name, b.name FROM GRAPH_TABLE (social MATCH (x IS P)-[IS FOLLOWS]->(y IS P)
               COLUMNS (x.cid AS f, y.cid AS t)) AS g
             JOIN customers a ON a.id = g.f JOIN customers b ON b.id = g.t ORDER BY a.name",
        )
        .await
        .expect("join");
    assert_eq!(
        j,
        [
            ("Alice".to_owned(), "Bob".to_owned()),
            ("Carol".to_owned(), "Alice".to_owned())
        ]
    );
    let empty: Vec<(i64,)> = s
        .query("SELECT COUNT(*) FROM GRAPH_TABLE (social MATCH (x IS Nobody) COLUMNS (x.cid AS c)) AS g")
        .await
        .expect("empty");
    assert_eq!(empty, [(0,)]);
    for q in [
        "DROP GRAPH shop",
        "DROP GRAPH social",
        "DROP TABLE orders, customers, products",
    ] {
        s.query_drop(q).await.expect(q);
    }
    s.disconnect().await.expect("bye");
}

/// Round 3: a Bolt session shows in `SHOW PROCESSLIST` with its running
/// statement; `KILL QUERY` stops the statement (`Terminated`) and the
/// session carries on; `KILL` closes the connection.
#[tokio::test]
async fn bolt_sessions_in_processlist_and_kill() {
    let (Some(addr), Some(user), Some(pw), Some(sql)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_SQL_ADDR"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* / SPRIG_TEST_SQL_ADDR not set");
        return;
    };
    let (host, port) = sql.split_once(':').expect("host:port");
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().expect("port"))
        .user(Some(user.clone()))
        .pass(Some(pw.clone()))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ));
    let mut s = mysql_async::Conn::new(opts).await.expect("sql login");
    s.query_drop("CREATE DATABASE IF NOT EXISTS r8g3_kill")
        .await
        .expect("db");
    s.query_drop("CREATE GRAPH IF NOT EXISTS r8g3_kill.g")
        .await
        .expect("graph");
    let db = vec![("db", PackValue::str("r8g3_kill.g"))];
    // Slow but within the budgets: a subquery per row.
    let slow = "UNWIND range(1, 4000) AS a CALL { WITH a UNWIND range(1, 900) AS b RETURN count(*) AS c } RETURN sum(c) AS s";
    let find = |rows: &[(u64, Option<String>, Option<String>)]| {
        rows.iter()
            .find(|r| r.2.as_deref().is_some_and(|i| i.contains("range(1, 4000)")))
            .map(|r| r.0)
    };
    for kill_conn in [false, true] {
        let mut c = connect(&addr, &user, &pw).await.expect("login");
        let db2 = db.clone();
        let run = tokio::spawn(async move {
            let r = c.query(slow, vec![], db2.clone()).await;
            let after = c.query("RETURN 1 AS one", vec![], db2).await;
            (r, after)
        });
        let mut id = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let rows: Vec<(u64, Option<String>, Option<String>)> = s
                .query("SHOW FULL PROCESSLIST")
                .await
                .expect("processlist")
                .into_iter()
                .map(|r: mysql_async::Row| {
                    let mut r = r;
                    (
                        r.take::<u64, _>(0).unwrap_or(0),
                        r.take::<Option<String>, _>(4).flatten(),
                        r.take::<Option<String>, _>(7).flatten(),
                    )
                })
                .collect();
            id = find(&rows);
            if id.is_some() {
                break;
            }
        }
        let id = id.expect("the Bolt statement in SHOW PROCESSLIST");
        let stmt = if kill_conn {
            format!("KILL {id}")
        } else {
            format!("KILL QUERY {id}")
        };
        s.query_drop(&stmt).await.expect("kill");
        let (r, after) = run.await.expect("join");
        let e = r.expect_err("killed");
        assert_eq!(e.code, "Neo.TransientError.Transaction.Terminated", "{e:?}");
        if kill_conn {
            assert!(after.is_err(), "the connection is closed after KILL");
        } else {
            assert!(
                after.is_ok(),
                "the session carries on after KILL QUERY: {after:?}"
            );
        }
        eprintln!(
            "{stmt}: statement stopped; connection {}",
            if kill_conn { "closed" } else { "kept" }
        );
    }
    s.query_drop("DROP GRAPH r8g3_kill.g").await.expect("drop");
    s.disconnect().await.expect("bye");
}

async fn sql_conn(sql: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = sql.split_once(':').expect("host:port");
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().expect("port"))
        .user(Some(user.to_owned()))
        .pass(Some(pw.to_owned()))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ));
    mysql_async::Conn::new(opts).await.expect("sql login")
}

fn int0(r: &Rows) -> i64 {
    r.records[0][0].as_int().expect("an integer")
}

/// Round 4 through a gateway: uniqueness across the shards of a graph
/// spread over the three dev clusters (G32, decision G41: the claim
/// table, its back-fill, concurrent `MERGE`), per-label read grants
/// (decision G42) and `sprigctl graph`.
///
/// Needs the end-to-end variables plus `SPRIG_TEST_SPRIGCTL`;
/// `SPRIG_TEST_TENANT` (default `acme`); `SPRIG_TEST_R4_MOVES=0` skips
/// the two shard moves.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario, step by step
async fn round_four_uniqueness_grants_and_sprigctl() {
    let (Some(addr), Some(user), Some(pw), Some(sql), Some(ctl)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_SQL_ADDR"),
        env("SPRIG_TEST_SPRIGCTL"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* / SPRIG_TEST_SQL_ADDR / SPRIG_TEST_SPRIGCTL not set");
        return;
    };
    let tenant = env("SPRIG_TEST_TENANT").unwrap_or_else(|| "acme".into());
    let mut s = sql_conn(&sql, &user, &pw).await;
    s.query_drop("CREATE DATABASE IF NOT EXISTS r8four_u")
        .await
        .expect("db");
    sprigctl(
        &ctl,
        &[
            "settings",
            "set",
            "sharding.min_shards",
            "4",
            "--scope",
            &format!("database:{tenant}.r8four_u"),
        ],
    );
    s.query_drop("DROP GRAPH IF EXISTS r8four_u.g")
        .await
        .expect("drop");
    s.query_drop("CREATE GRAPH r8four_u.g")
        .await
        .expect("create");
    let db = vec![("db", PackValue::str("r8four_u.g"))];
    let mut c = connect(&addr, &user, &pw).await.expect("login");
    let eid = c
        .query("CREATE (n:Seed) RETURN elementId(n)", vec![], db.clone())
        .await
        .expect("seed");
    let graph_id = eid.records[0][0]
        .as_str()
        .and_then(|x| x.split(':').nth(1))
        .expect("graph id")
        .to_owned();
    let (gid, shards) = graph_shards(&ctl, &tenant, &format!("graph_{graph_id}"));
    assert_eq!(shards.len(), 4, "{shards:?}");
    if env("SPRIG_TEST_R4_MOVES").as_deref() != Some("0") {
        for (shard, to) in [(2u64, "dc-a-c2"), (3, "dc-a-c3")] {
            let j = sprigctl(
                &ctl,
                &[
                    "shard",
                    "move",
                    &tenant,
                    &gid.to_string(),
                    &shard.to_string(),
                    "--to",
                    to,
                ],
            );
            let id = j["id"].as_str().expect("job id").to_owned();
            let took = wait_job(&ctl, &id).await;
            eprintln!("moved shard {shard} to {to} in {took:?}");
        }
    }
    let (_, shards) = graph_shards(&ctl, &tenant, &format!("graph_{graph_id}"));
    eprintln!("graph {graph_id}: shards {shards:?}");
    // The moves can outlast the idle timeout of the first connection.
    let _ = c.goodbye().await;
    let mut c = connect(&addr, &user, &pw).await.expect("login again");

    // Back-fill: a duplicate across shards refuses the constraint.
    c.query(
        "UNWIND range(1, 30) AS i CREATE (:U {k: i})",
        vec![],
        db.clone(),
    )
    .await
    .expect("keys");
    c.query("CREATE (:U {k: 5.0})", vec![], db.clone())
        .await
        .expect("duplicate before the constraint");
    let e = c
        .query(
            "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE",
            vec![],
            db.clone(),
        )
        .await
        .expect_err("existing duplicate");
    assert!(e.code.contains("ConstraintCreationFailed"), "{e:?}");
    c.query(
        "MATCH (n:U {k: 5.0}) WITH n ORDER BY id(n) DESC LIMIT 1 DELETE n",
        vec![],
        db.clone(),
    )
    .await
    .expect("drop the duplicate");
    c.query(
        "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE",
        vec![],
        db.clone(),
    )
    .await
    .expect("constraint");
    let show = sprigctl(&ctl, &["graph", "show", &tenant, "r8four_u.g"]);
    let claims: u64 = show["shards"]
        .as_array()
        .expect("shards")
        .iter()
        .map(|x| x["claims"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(claims, 30, "{show}");
    // Every second copy is refused, wherever its id puts it.
    let mut refused = 0;
    for i in 1..=30 {
        let e = c
            .query(
                "CREATE (:U {k: $k})",
                vec![("k", PackValue::Int(i))],
                db.clone(),
            )
            .await
            .expect_err("duplicate");
        assert_eq!(
            e.code, "Neo.ClientError.Schema.ConstraintValidationFailed",
            "{e:?}"
        );
        refused += 1;
    }
    // Concurrent MERGEs on 25 new keys from 8 connections.
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..8i64 {
        let (addr, user, pw, db) = (addr.clone(), user.clone(), pw.clone(), db.clone());
        tasks.push(tokio::spawn(async move {
            let mut c = connect(&addr, &user, &pw).await.expect("login");
            let mut retried = 0;
            for j in 0..25i64 {
                let k = 1000 + (j * 7 + w * 3) % 25;
                loop {
                    match c
                        .query(
                            "MERGE (n:U {k: $k}) ON CREATE SET n.by = $w ON MATCH SET n.seen = coalesce(n.seen, 0) + 1",
                            vec![("k", PackValue::Int(k)), ("w", PackValue::Int(w))],
                            db.clone(),
                        )
                        .await
                    {
                        Ok(_) => break,
                        Err(e) if e.code.starts_with("Neo.TransientError") => retried += 1,
                        Err(e) => panic!("merge {k}: {e:?}"),
                    }
                }
            }
            retried
        }));
    }
    let mut retried = 0;
    for t in tasks {
        retried += t.await.expect("task");
    }
    let merge_time = t0.elapsed();
    let r = c
        .query(
            "MATCH (n:U) WHERE n.k >= 1000 RETURN count(n), count(DISTINCT n.k), sum(coalesce(n.seen, 0))",
            vec![],
            db.clone(),
        )
        .await
        .expect("count");
    assert_eq!(rows_text(&r, true), ["25 | 25 | 175"]);
    let show = sprigctl(&ctl, &["graph", "show", &tenant, "r8four_u.g"]);
    let claims: u64 = show["shards"]
        .as_array()
        .expect("shards")
        .iter()
        .map(|x| x["claims"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(claims, 55, "{show}");
    let clusters: std::collections::BTreeSet<String> = show["shards"]
        .as_array()
        .expect("shards")
        .iter()
        .map(|x| x["cluster"].as_str().unwrap_or_default().to_owned())
        .collect();
    eprintln!(
        "uniqueness: {refused} duplicates refused, 200 concurrent MERGEs in {merge_time:?} ({retried} transient retries), {claims} claims on clusters {clusters:?}"
    );

    // sprigctl graph.
    let list = sprigctl(&ctl, &["graph", "list", &tenant]);
    assert!(
        list.as_array()
            .expect("list")
            .iter()
            .any(|g| g["name"] == "g" && g["database"] == "r8four_u" && g["shards"] == 4),
        "{list}"
    );
    c.query(
        "MATCH (a:U), (b:U) WHERE b.k = a.k + 1 AND a.k < 30 CREATE (a)-[:NEXT]->(b)",
        vec![],
        db.clone(),
    )
    .await
    .expect("edges");
    let part = sprigctl(&ctl, &["graph", "partition", &tenant, "r8four_u.g"]);
    assert_eq!(part["relationships"], 29, "{part}");
    eprintln!(
        "partition: {} of {} relationships cross shards ({}%)",
        part["cross_shard"], part["relationships"], part["cross_shard_pct"]
    );

    // Per-label read grants.
    c.query(
        "CREATE (:Secret {k: 'x'})-[:HIDES]->(:U {k: 99}), (:Public {k: 'p'})",
        vec![],
        db.clone(),
    )
    .await
    .expect("more");
    let _ = s.query_drop("DROP USER IF EXISTS r8four_reader").await;
    s.query_drop("CREATE USER r8four_reader IDENTIFIED BY 'r8four-reader-pw'")
        .await
        .expect("user");
    s.query_drop("GRANT READ ON GRAPH r8four_u.g NODES U TO r8four_reader")
        .await
        .expect("grant nodes");
    s.query_drop("GRANT READ ON GRAPH r8four_u.g RELATIONSHIPS NEXT TO r8four_reader")
        .await
        .expect("grant types");
    let grants: Vec<String> = s
        .query("SHOW GRANTS FOR r8four_reader")
        .await
        .expect("show grants");
    assert!(
        grants
            .iter()
            .any(|g| g.contains("GRANT READ ON GRAPH `r8four_u`.`g` NODES `U`")),
        "{grants:?}"
    );
    let reader = format!("{tenant}.r8four_reader");
    let mut rc = connect(&addr, &reader, "r8four-reader-pw")
        .await
        .expect("reader login");
    let n = rc
        .query("MATCH (n) RETURN count(n)", vec![], db.clone())
        .await
        .expect("reader count");
    let all_u = c
        .query("MATCH (n:U) RETURN count(n)", vec![], db.clone())
        .await
        .expect("u count");
    assert_eq!(int0(&n), int0(&all_u));
    let rels = rc
        .query(
            "MATCH ()-[r]->() RETURN type(r), count(*)",
            vec![],
            db.clone(),
        )
        .await
        .expect("reader rels");
    assert_eq!(rows_text(&rels, true), ["'NEXT' | 29"]);
    let hidden = rc
        .query("MATCH (n:Secret) RETURN count(n)", vec![], db.clone())
        .await
        .expect("hidden label");
    assert_eq!(int0(&hidden), 0);
    let path = rc
        .query(
            "MATCH (a:U {k: 1})-[*1..10]->(b) RETURN count(DISTINCT b)",
            vec![],
            db.clone(),
        )
        .await
        .expect("var-length");
    assert_eq!(int0(&path), 10);
    let e = rc
        .query("CREATE (:U {k: 5000})", vec![], db.clone())
        .await
        .expect_err("no write grant");
    assert!(e.message.contains("denied"), "{e:?}");
    let _ = rc.goodbye().await;
    s.query_drop("REVOKE READ ON GRAPH r8four_u.g NODES U FROM r8four_reader")
        .await
        .expect("revoke");
    s.query_drop("REVOKE READ ON GRAPH r8four_u.g RELATIONSHIPS NEXT FROM r8four_reader")
        .await
        .expect("revoke types");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut rc = connect(&addr, &reader, "r8four-reader-pw")
        .await
        .expect("reader login");
    let e = rc
        .query("MATCH (n) RETURN count(n)", vec![], db.clone())
        .await
        .expect_err("revoked");
    assert!(e.message.contains("denied"), "{e:?}");
    let _ = rc.goodbye().await;
    eprintln!(
        "label grants: the reader saw {} U nodes, 29 NEXT, no Secret; revoked",
        int0(&n)
    );
    s.query_drop("DROP USER r8four_reader")
        .await
        .expect("drop user");
    let _ = c.goodbye().await;
    s.disconnect().await.expect("bye");
}
