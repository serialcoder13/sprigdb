//! An LDBC-SNB-shaped workload through Bolt (chapter 43, "Testing"; R8's
//! done-when, decision G25 step 5). The official LDBC data generator and
//! driver are downloads that need approval, so this test generates a
//! social network of its own with the SNB schema's shape: persons in
//! cities, `KNOWS` with a skewed degree, posts and comments with creators
//! and tags, and `LIKES`. It loads them through Bolt, then runs the
//! interactive short reads IS1–IS7 and the complex reads IC1, IC2, IC9 and
//! IC13 (simplified to this schema) with random parameters, checks every
//! answer against the in-memory executor over the same data, and reports
//! latency percentiles against chapter 43's budgets (any 39903 fails the
//! test).
//!
//! Needs `SPRIG_TEST_BOLT_ADDR`, `SPRIG_TEST_BOLT_USER`,
//! `SPRIG_TEST_BOLT_PASSWORD` and `SPRIG_TEST_LDBC_GRAPH` (an existing
//! graph `db.graph`, sharded as the run wants). `SPRIG_TEST_LDBC_PERSONS`
//! sets the scale (default 1000 persons; SF0.1 has about 1,500) and
//! `SPRIG_TEST_LDBC_ROUNDS` the parameter sets per query (default 20).
//! Without the variables it skips.

#![allow(
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use sprig_graph::eval::Params;
use sprig_graph::mem::MemStore;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, execute};
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::client::BoltClient;
use tokio::net::TcpStream;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

/// A small deterministic generator (xorshift64*), so runs repeat.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    /// Skewed towards small values: about a power law.
    fn skewed(&mut self, n: u64) -> u64 {
        let a = self.below(n);
        let b = self.below(n);
        a.min(b).min(self.below(n))
    }
}

/// A parameter value both servers take.
#[derive(Clone)]
enum V {
    I(i64),
    S(String),
    L(Vec<V>),
    M(Vec<(&'static str, V)>),
}

impl V {
    fn pack(&self) -> PackValue {
        match self {
            V::I(i) => PackValue::Int(*i),
            V::S(s) => PackValue::String(s.clone()),
            V::L(l) => PackValue::List(l.iter().map(V::pack).collect()),
            V::M(m) => PackValue::map(m.iter().map(|(k, v)| (*k, v.pack()))),
        }
    }
    fn val(&self) -> Value {
        match self {
            V::I(i) => Value::Int(*i),
            V::S(s) => Value::String(s.clone()),
            V::L(l) => Value::List(l.iter().map(V::val).collect()),
            V::M(m) => Value::Map(
                m.iter()
                    .map(|(k, v)| ((*k).to_owned(), v.val()))
                    .collect::<BTreeMap<_, _>>(),
            ),
        }
    }
}

fn pack_text(v: &PackValue) -> String {
    match v {
        PackValue::Null => "null".into(),
        PackValue::Bool(b) => b.to_string(),
        PackValue::Int(i) => i.to_string(),
        PackValue::Float(f) => format!("{f:?}"),
        PackValue::String(s) => s.clone(),
        PackValue::List(l) => format!(
            "[{}]",
            l.iter().map(pack_text).collect::<Vec<_>>().join(",")
        ),
        other => format!("{other:?}"),
    }
}

fn val_text(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::String(s) => s.clone(),
        Value::List(l) => format!("[{}]", l.iter().map(val_text).collect::<Vec<_>>().join(",")),
        other => other.display(),
    }
}

const FIRST: &[&str] = &[
    "Ana", "Bo", "Chen", "Dara", "Eli", "Fay", "Gus", "Hana", "Ivo", "Jun", "Kai", "Lea",
];
const LAST: &[&str] = &["Ito", "Khan", "Lopez", "Meyer", "Novak", "Okafor", "Park", "Rossi"];

/// The data: statements with batched rows.
struct Data {
    persons: i64,
    posts: i64,
    comments: i64,
    batches: Vec<(&'static str, Vec<V>)>,
    elements: usize,
}

fn generate(persons: i64, seed: u64) -> Data {
    let mut r = Rng(seed);
    let cities = 20;
    let tags = 40;
    let mut batches: Vec<(&'static str, Vec<V>)> = Vec::new();
    let mut elements = 0;
    let mut push = |q: &'static str, rows: Vec<V>, batches: &mut Vec<(&'static str, Vec<V>)>| {
        elements += rows.len();
        for c in rows.chunks(250) {
            batches.push((q, c.to_vec()));
        }
    };
    push(
        "UNWIND $rows AS r CREATE (:City {id: r.id, name: r.name})",
        (0..cities)
            .map(|i| V::M(vec![("id", V::I(i)), ("name", V::S(format!("city{i}")))]))
            .collect(),
        &mut batches,
    );
    push(
        "UNWIND $rows AS r CREATE (:Tag {id: r.id, name: r.name})",
        (0..tags)
            .map(|i| V::M(vec![("id", V::I(i)), ("name", V::S(format!("tag{i}")))]))
            .collect(),
        &mut batches,
    );
    let mut rows = Vec::new();
    for i in 0..persons {
        let u = u64::try_from(i).unwrap_or(0);
        rows.push(V::M(vec![
            ("id", V::I(i)),
            ("first", V::S(FIRST[(r.next() % FIRST.len() as u64) as usize].into())),
            ("last", V::S(LAST[(u % LAST.len() as u64) as usize].into())),
            ("born", V::I(1_970 + i64::try_from(r.below(35)).unwrap_or(0))),
            ("created", V::I(1_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0))),
            ("city", V::I(i64::try_from(r.below(20)).unwrap_or(0))),
        ]));
    }
    push(
        "UNWIND $rows AS r CREATE (:Person {id: r.id, firstName: r.first, lastName: r.last, birthYear: r.born, creationDate: r.created, browserUsed: 'Firefox', locationIP: '10.0.0.' + toString(r.id % 250), cityId: r.city})",
        rows,
        &mut batches,
    );
    let pn = u64::try_from(persons).unwrap_or(1);
    // KNOWS: about 10 per person on average, skewed, no self loops, each
    // pair once.
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for a in 0..pn {
        let k = 1 + r.skewed(30);
        for _ in 0..k {
            let b = r.below(pn);
            let (x, y) = (a.min(b), a.max(b));
            if x == y || !seen.insert((x, y)) {
                continue;
            }
            rows.push(V::M(vec![
                ("a", V::I(i64::try_from(x).unwrap_or(0))),
                ("b", V::I(i64::try_from(y).unwrap_or(0))),
                ("d", V::I(2_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0))),
            ]));
        }
    }
    let knows = rows.len();
    push(
        "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (b:Person {id: r.b}) CREATE (a)-[:KNOWS {creationDate: r.d}]->(b)",
        rows,
        &mut batches,
    );
    push(
        "UNWIND $rows AS r MATCH (p:Person {id: r.p}), (c:City {id: r.c}) CREATE (p)-[:IS_LOCATED_IN]->(c)",
        (0..persons)
            .map(|i| {
                V::M(vec![
                    ("p", V::I(i)),
                    ("c", V::I(i64::try_from(r.below(20)).unwrap_or(0))),
                ])
            })
            .collect(),
        &mut batches,
    );
    let posts = persons * 3;
    let mut rows = Vec::new();
    for i in 0..posts {
        rows.push(V::M(vec![
            ("id", V::I(i)),
            ("creator", V::I(i64::try_from(r.skewed(pn)).unwrap_or(0))),
            ("created", V::I(3_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0))),
            ("tag", V::I(i64::try_from(r.below(40)).unwrap_or(0))),
        ]));
    }
    push(
        "UNWIND $rows AS r MATCH (c:Person {id: r.creator}), (t:Tag {id: r.tag}) CREATE (m:Post {id: r.id, content: 'post ' + toString(r.id), creationDate: r.created, length: r.id % 140})-[:HAS_CREATOR]->(c), (m)-[:HAS_TAG]->(t)",
        rows,
        &mut batches,
    );
    let comments = persons * 2;
    let pp = u64::try_from(posts).unwrap_or(1);
    let mut rows = Vec::new();
    for i in 0..comments {
        rows.push(V::M(vec![
            ("id", V::I(1_000_000 + i)),
            ("creator", V::I(i64::try_from(r.below(pn)).unwrap_or(0))),
            ("post", V::I(i64::try_from(r.skewed(pp)).unwrap_or(0))),
            ("created", V::I(4_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0))),
        ]));
    }
    push(
        "UNWIND $rows AS r MATCH (c:Person {id: r.creator}), (p:Post {id: r.post}) CREATE (m:Comment {id: r.id, content: 'comment ' + toString(r.id), creationDate: r.created})-[:HAS_CREATOR]->(c), (m)-[:REPLY_OF]->(p)",
        rows,
        &mut batches,
    );
    let mut rows = Vec::new();
    for a in 0..pn {
        for _ in 0..r.skewed(12) {
            rows.push(V::M(vec![
                ("a", V::I(i64::try_from(a).unwrap_or(0))),
                ("p", V::I(i64::try_from(r.below(pp)).unwrap_or(0))),
                ("d", V::I(5_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0))),
            ]));
        }
    }
    push(
        "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (p:Post {id: r.p}) CREATE (a)-[:LIKES {creationDate: r.d}]->(p)",
        rows,
        &mut batches,
    );
    eprintln!("ldbc-shaped data: {persons} persons, {knows} KNOWS, {posts} posts, {comments} comments");
    Data {
        persons,
        posts,
        comments,
        batches,
        elements,
    }
}

/// The reads, with their parameter names.
const READS: &[(&str, &str, &[&str])] = &[
    (
        "IS1",
        "MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(p:City) RETURN n.firstName, n.lastName, n.birthYear, n.locationIP, n.browserUsed, p.id AS cityId, n.creationDate",
        &["personId"],
    ),
    (
        "IS2",
        "MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(m) WITH m ORDER BY m.creationDate DESC, m.id DESC LIMIT 10 MATCH (m)-[:REPLY_OF*0..1]->(p:Post)-[:HAS_CREATOR]->(c:Person) RETURN m.id, m.content, m.creationDate, p.id, c.id, c.firstName, c.lastName ORDER BY m.creationDate DESC, m.id DESC",
        &["personId"],
    ),
    (
        "IS3",
        "MATCH (n:Person {id: $personId})-[r:KNOWS]-(friend) RETURN friend.id, friend.firstName, friend.lastName, r.creationDate ORDER BY r.creationDate DESC, friend.id",
        &["personId"],
    ),
    (
        "IS4",
        "MATCH (m:Post {id: $messageId}) RETURN m.creationDate, m.content",
        &["messageId"],
    ),
    (
        "IS5",
        "MATCH (m:Post {id: $messageId})-[:HAS_CREATOR]->(p:Person) RETURN p.id, p.firstName, p.lastName",
        &["messageId"],
    ),
    (
        "IS6",
        "MATCH (m:Comment {id: $commentId})-[:REPLY_OF]->(p:Post)-[:HAS_CREATOR]->(mod:Person) RETURN p.id, mod.id, mod.firstName, mod.lastName",
        &["commentId"],
    ),
    (
        "IS7",
        "MATCH (m:Post {id: $messageId})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) RETURN c.id, c.content, c.creationDate, p.id, p.firstName, p.lastName, r IS NOT NULL AS knows ORDER BY c.creationDate DESC, p.id",
        &["messageId"],
    ),
    (
        "IC1",
        "MATCH (p:Person {id: $personId}), (f:Person {firstName: $firstName}) WHERE p <> f MATCH path = shortestPath((p)-[:KNOWS*1..3]-(f)) WITH f, length(path) AS d ORDER BY d, f.lastName, f.id LIMIT 20 MATCH (f)-[:IS_LOCATED_IN]->(c:City) RETURN f.id, f.lastName, d, c.name ORDER BY d, f.lastName, f.id",
        &["personId", "firstName"],
    ),
    (
        "IC2",
        "MATCH (:Person {id: $personId})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Post) WHERE m.creationDate <= $maxDate RETURN f.id, f.firstName, f.lastName, m.id, m.content, m.creationDate ORDER BY m.creationDate DESC, m.id LIMIT 20",
        &["personId", "maxDate"],
    ),
    (
        "IC9",
        "MATCH (:Person {id: $personId})-[:KNOWS*1..2]-(f:Person) WHERE f.id <> $personId WITH DISTINCT f MATCH (f)<-[:HAS_CREATOR]-(m:Post) WHERE m.creationDate < $maxDate RETURN f.id, f.firstName, m.id, m.creationDate ORDER BY m.creationDate DESC, m.id LIMIT 20",
        &["personId", "maxDate"],
    ),
    (
        "IC13",
        "MATCH (a:Person {id: $personId}), (b:Person {id: $person2Id}) OPTIONAL MATCH path = shortestPath((a)-[:KNOWS*]-(b)) RETURN CASE WHEN path IS NULL THEN -1 ELSE length(path) END AS len",
        &["personId", "person2Id"],
    ),
];

fn percentile(v: &mut [Duration], p: f64) -> Duration {
    if v.is_empty() {
        return Duration::ZERO;
    }
    v.sort();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i.min(v.len() - 1)]
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // load, read, compare, report
async fn ldbc_shaped_workload() {
    let (Some(addr), Some(user), Some(pw), Some(graph)) = (
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_BOLT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_LDBC_GRAPH"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BOLT_* / SPRIG_TEST_LDBC_GRAPH not set");
        return;
    };
    let persons: i64 = env("SPRIG_TEST_LDBC_PERSONS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let rounds: u64 = env("SPRIG_TEST_LDBC_ROUNDS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let s = TcpStream::connect(&addr).await.expect("connect");
    let mut c = BoltClient::handshake(s, None).await.expect("handshake");
    c.login(&user, &pw).await.expect("login");
    let db = vec![("db", PackValue::String(graph.clone()))];
    c.query("MATCH (n) DETACH DELETE n", vec![], db.clone())
        .await
        .expect("clean");
    for (l, p) in [("Person", "id"), ("Post", "id"), ("Comment", "id"), ("City", "id"), ("Tag", "id"), ("Person", "firstName")] {
        c.query(
            &format!("CREATE INDEX IF NOT EXISTS FOR (n:{l}) ON (n.{p})"),
            vec![],
            db.clone(),
        )
        .await
        .expect("index");
    }
    let mut mem = MemStore::new(4);
    for (l, p) in [("Person", "id"), ("Post", "id"), ("Comment", "id"), ("City", "id"), ("Tag", "id"), ("Person", "firstName")] {
        execute(
            &mut mem,
            &format!("CREATE INDEX FOR (n:{l}) ON (n.{p})"),
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .expect("mem index");
    }
    let data = generate(persons, 0x5EED_1DBC);
    let t0 = Instant::now();
    for (q, rows) in &data.batches {
        c.query(q, vec![("rows", V::L(rows.clone()).pack())], db.clone())
            .await
            .unwrap_or_else(|e| panic!("load {q}: {e}"));
    }
    let load = t0.elapsed();
    for (q, rows) in &data.batches {
        let mut p = Params::new();
        p.insert("rows".into(), V::L(rows.clone()).val());
        execute(&mut mem, q, &p, &Budgets::default())
            .await
            .unwrap_or_else(|e| panic!("mem load {q}: {e}"));
    }
    eprintln!(
        "loaded {} elements in {} statements in {:.1} s ({:.0} elements/s)",
        data.elements,
        data.batches.len(),
        load.as_secs_f64(),
        data.elements as f64 / load.as_secs_f64()
    );
    let counts = c
        .query(
            "MATCH (n) WITH count(n) AS n MATCH ()-[r]->() RETURN n, count(r) AS r",
            vec![],
            db.clone(),
        )
        .await
        .expect("counts");
    eprintln!("graph: {:?}", counts.records);

    let mut r = Rng(42);
    let mut report = Vec::new();
    let mut total_mismatch = 0;
    for (name, q, params) in READS {
        let mut times = Vec::new();
        let mut rows_total = 0;
        for _ in 0..rounds {
            let mut ps: Vec<(&str, V)> = Vec::new();
            for p in *params {
                let v = match *p {
                    "personId" | "person2Id" => {
                        V::I(i64::try_from(r.below(u64::try_from(data.persons).unwrap_or(1))).unwrap_or(0))
                    }
                    "messageId" => V::I(
                        i64::try_from(r.below(u64::try_from(data.posts).unwrap_or(1))).unwrap_or(0),
                    ),
                    "commentId" => V::I(
                        1_000_000
                            + i64::try_from(r.below(u64::try_from(data.comments).unwrap_or(1)))
                                .unwrap_or(0),
                    ),
                    "firstName" => V::S(FIRST[(r.next() % FIRST.len() as u64) as usize].into()),
                    "maxDate" => V::I(3_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0)),
                    _ => V::I(0),
                };
                ps.push((p, v));
            }
            let t = Instant::now();
            let got = c
                .query(q, ps.iter().map(|(k, v)| (*k, v.pack())).collect(), db.clone())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            times.push(t.elapsed());
            rows_total += got.records.len();
            let mut mp = Params::new();
            for (k, v) in &ps {
                mp.insert((*k).to_owned(), v.val());
            }
            let want = execute(&mut mem, q, &mp, &Budgets::default())
                .await
                .unwrap_or_else(|e| panic!("mem {name}: {e}"));
            let a: Vec<String> = got
                .records
                .iter()
                .map(|r| r.iter().map(pack_text).collect::<Vec<_>>().join("|"))
                .collect();
            let b: Vec<String> = want
                .rows
                .iter()
                .map(|r| r.iter().map(val_text).collect::<Vec<_>>().join("|"))
                .collect();
            if a != b {
                total_mismatch += 1;
                eprintln!("MISMATCH {name} {:?}\n  sprig: {a:?}\n  mem:   {b:?}", ps.iter().map(|(k, v)| (*k, pack_text(&v.pack()))).collect::<Vec<_>>());
            }
        }
        let mut t2 = times.clone();
        let p50 = percentile(&mut t2, 0.5);
        let p95 = percentile(&mut t2, 0.95);
        let max = percentile(&mut t2, 1.0);
        report.push(format!(
            "{name:>5}: p50 {:>7.1} ms  p95 {:>7.1} ms  max {:>7.1} ms  rows {rows_total}",
            p50.as_secs_f64() * 1e3,
            p95.as_secs_f64() * 1e3,
            max.as_secs_f64() * 1e3
        ));
    }
    eprintln!("ldbc-shaped reads, {rounds} parameter sets each:\n{}", report.join("\n"));
    let profile = c
        .query(
            &format!("PROFILE {}", READS[9].1),
            vec![
                ("personId", PackValue::Int(1)),
                ("maxDate", PackValue::Int(3_500_000)),
            ],
            db.clone(),
        )
        .await
        .expect("profile");
    eprintln!("IC9 profile: {:?}", profile.summary.get("profile").map(pack_text));
    assert_eq!(total_mismatch, 0, "answers differ from the in-memory executor");
}
