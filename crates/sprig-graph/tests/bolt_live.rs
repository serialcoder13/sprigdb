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
//! Without the variables the tests skip.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use sprig_graph::eval::Params;
use sprig_graph::mem::MemStore;
use sprig_graph::{Budgets, execute};
use sprig_proto_bolt::client::{BoltClient, ClientError, Rows};
use sprig_proto_bolt::{PackValue, Request, Response};
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
        PackValue::Struct { tag, .. } => format!("<struct {tag:#x}>"),
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
    "MATCH (p:Person)-[b:BOUGHT]->(x) RETURN x.name, sum(b.qty) AS q, collect(DISTINCT p.name) AS who ORDER BY q DESC, x.name",
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
];

/// Statements the differential leaves out, with the reason (shown in the
/// test output).
const KNOWN_GAPS: &[(&str, &str)] = &[(
    "RETURN a {.name, .age}",
    "map projections are not supported yet (39900)",
)];

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
    let db = [("db", PackValue::str("r8graphs_g.diff"))];
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
