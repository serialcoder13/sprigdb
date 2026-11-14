//! Logic tooling through a live gateway: `Logic.TestRun`
//! over SQL, REST and gRPC; staged rollout by percentage with per-version
//! numbers and the automatic rollback; `DROP FUNCTION | PROCEDURE |
//! VALIDATION | SCHEDULE` with dependents.
//!
//! Needs a gateway with an HTTP listener (REST and gRPC) and a tenant user
//! who may create databases, users and routines:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:46306,127.0.0.1:46081   # MySQL wire, HTTP
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=sprig-dev-app
//! cargo test -p sprig-gateway --test logic_follow_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the databases `r7follow_tr`, `r7follow_ro` and `r7follow_dr`
//! and with the users `r7f_x` and `r7f_u0` to `r7f_u11`, dropped
//! afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_api::proto::v1::TestRunRequest;
use sprig_api::proto::v1::logic_client::LogicClient;
use sprig_testkit::skip;
use tonic::transport::Endpoint;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";

struct Env {
    mysql: String,
    http: String,
    user: String,
    password: String,
}

fn env(test: &str) -> Option<Env> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    assert_eq!(urls.len(), 2, "{GATEWAY}: mysql,http");
    Some(Env {
        mysql: urls[0].clone(),
        http: urls[1].clone(),
        user: std::env::var("SPRIG_TEST_LOGIC_USER").unwrap_or_else(|_| "acme.app".into()),
        password: std::env::var("SPRIG_TEST_LOGIC_PASSWORD")
            .unwrap_or_else(|_| "sprig-dev-app".into()),
    })
}

impl Env {
    fn tenant(&self) -> &str {
        self.user.split('.').next().unwrap()
    }
}

async fn conn(addr: &str, user: &str, password: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(password))
        .db_name(db)
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.unwrap()
}

type Rows = Vec<Vec<Option<String>>>;

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Rows, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn must(c: &mut Conn, q: &str) -> Rows {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

/// The notes of the last statement (`SHOW WARNINGS`), joined.
async fn notes(c: &mut Conn) -> String {
    let w = must(c, "SHOW WARNINGS").await;
    w.into_iter()
        .filter_map(|r| r.get(2).cloned().flatten())
        .collect::<Vec<_>>()
        .join("; ")
}

fn basic(user: &str, password: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
    )
}

async fn http(
    addr: &str,
    auth: &str,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            body.map(Value::to_string).unwrap_or_default(),
        )))
        .unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

fn proto(v: &Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match v {
        Value::Null => Kind::NullValue(0),
        Value::Bool(b) => Kind::BoolValue(*b),
        Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap()),
        Value::String(s) => Kind::StringValue(s.clone()),
        Value::Array(a) => Kind::ListValue(prost_types::ListValue {
            values: a.iter().map(proto).collect(),
        }),
        Value::Object(o) => Kind::StructValue(prost_types::Struct {
            fields: o.iter().map(|(k, v)| (k.clone(), proto(v))).collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

fn unproto(v: &prost_types::Value) -> Value {
    use prost_types::value::Kind;
    match &v.kind {
        None | Some(Kind::NullValue(_)) => Value::Null,
        Some(Kind::BoolValue(b)) => Value::Bool(*b),
        Some(Kind::NumberValue(n)) => json!(n),
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::ListValue(l)) => Value::Array(l.values.iter().map(unproto).collect()),
        Some(Kind::StructValue(s)) => Value::Object(
            s.fields
                .iter()
                .map(|(k, v)| (k.clone(), unproto(v)))
                .collect(),
        ),
    }
}

struct Checks {
    passed: usize,
    failed: Vec<String>,
}

impl Checks {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if ok {
            self.passed += 1;
            println!("ok   {name}");
        } else {
            println!("FAIL {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn done(self, what: &str) {
        println!(
            "{what}: {} of {} checks pass",
            self.passed,
            self.passed + self.failed.len()
        );
        assert!(self.failed.is_empty(), "{:#?}", self.failed);
    }
}

/// The report of a `TEST ...` statement: its third column.
fn report_of(r: &Result<Rows, (u16, String)>) -> Value {
    match r {
        Ok(rows) => rows
            .first()
            .and_then(|row| row.get(2).cloned().flatten())
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

const PLACE_JS: &str =
    "CREATE OR REPLACE FUNCTION place(productId BIGINT, qty INT) LANGUAGE javascript \
    EXPOSE (sql, http, grpc) AS $$
  const p = db.sqlOne('SELECT price FROM products WHERE id = ?', [productId]);
  if (p === null) { return sprig.event('error', {message: 'no product'}); }
  console.log('pricing ' + productId + ' x ' + qty);
  const total = p.price * qty;
  db.exec('INSERT INTO orders (product, qty, total) VALUES (?, ?, ?)', [productId, qty, total]);
  return {total: total};
$$";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_runs() {
    let Some(e) = env("test_runs") else { return };
    let db = "r7follow_tr";
    let mut ck = Checks::new();
    let mut c = conn(&e.mysql, &e.user, &e.password, None).await;
    let _ = sql(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    let _ = sql(&mut c, "DROP USER IF EXISTS 'r7f_x'@'%'").await;
    must(&mut c, &format!("CREATE DATABASE {db}")).await;
    must(&mut c, &format!("USE {db}")).await;
    for s in [
        "CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR(40), price DOUBLE)",
        "CREATE TABLE orders (id BIGINT AUTO_INCREMENT PRIMARY KEY, product BIGINT, qty INT, total DOUBLE)",
        "CREATE TABLE audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, k VARCHAR(64))",
        "CREATE TABLE docs (id INT PRIMARY KEY, owner VARCHAR(64), body VARCHAR(32))",
        "CREATE PROCEDURE plainp() BEGIN COMMIT; END",
        "INSERT INTO products VALUES (7, 'pen', 2.5), (8, 'ink', 4)",
        "INSERT INTO docs VALUES (1, 'r7f_x@%', 'mine'), (2, 'someone@%', 'theirs'), (3, 'r7f_x@%', 'also mine')",
    ] {
        must(&mut c, s).await;
    }
    must(&mut c, PLACE_JS).await;
    let deploy_notes = notes(&mut c).await;
    println!("place: {deploy_notes}");

    // 1. SQL: TEST FUNCTION runs it and writes nothing.
    let r = sql(&mut c, &format!("TEST FUNCTION {db}.place (7, 3)")).await;
    let rep = report_of(&r);
    ck.check(
        "SQL TEST FUNCTION answers the event and fields",
        matches!(&r, Ok(rows) if rows[0][0].as_deref() == Some("output"))
            && rep["result"]["total"].as_f64() == Some(7.5),
        format!("{r:?}"),
    );
    ck.check(
        "the report lists the write (rolled back), the read and the log line",
        rep["writes"].as_array().is_some_and(|w| {
            w.len() == 1
                && w[0]["sql"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("INSERT INTO"))
                && w[0]["affected"] == 1
        }) && rep["statements"]
            .as_array()
            .is_some_and(|s| s.len() == 2 && s[0]["kind"] == "read" && s[0]["rows"] == 1)
            && rep["logs"] == json!(["pricing 7 x 3"])
            && rep["timing"]["totalMs"].as_f64().is_some_and(|t| t > 0.0)
            && rep["budget"]["used"]["dataOps"] == 2,
        &rep,
    );
    let n = must(&mut c, "SELECT COUNT(*) FROM orders").await;
    ck.check(
        "nothing was written",
        n[0][0].as_deref() == Some("0"),
        format!("{n:?}"),
    );
    // Inside the client's own transaction: a savepoint, the transaction
    // stays open and keeps its own writes.
    must(&mut c, "START TRANSACTION").await;
    must(&mut c, "INSERT INTO audit (k) VALUES ('before')").await;
    let r = sql(&mut c, "TEST FUNCTION place (8, 1)").await;
    let mine = must(&mut c, "SELECT COUNT(*) FROM audit").await;
    must(&mut c, "ROLLBACK").await;
    let n = must(&mut c, "SELECT COUNT(*) FROM orders").await;
    ck.check(
        "inside a transaction: a savepoint; the transaction's own writes stay",
        report_of(&r)["result"]["total"].as_f64() == Some(4.0)
            && mine[0][0].as_deref() == Some("1")
            && n[0][0].as_deref() == Some("0"),
        format!("{r:?} {mine:?} {n:?}"),
    );

    // 2. REST with a snapshot of rows: the code sees exactly these.
    let auth = basic(&e.user, &e.password);
    let (st, v) = http(
        &e.http,
        &auth,
        "POST",
        &format!("/v1/databases/{db}/logic:testRun"),
        Some(&json!({
            "name": "place",
            "args": {"productId": 7, "qty": 2},
            "rows": {"products": [{"id": 7, "name": "pen", "price": 10}]},
        })),
    )
    .await;
    ck.check(
        "REST logic:testRun with a snapshot of products",
        st == 200
            && v["result"]["total"].as_f64() == Some(20.0)
            && v["tables"]["products"][0]["price"].as_f64() == Some(10.0)
            && v["writes"].as_array().is_some_and(|w| w.len() == 1)
            && v["timing"]["setupMs"].as_f64().is_some(),
        format!("{st} {v}"),
    );
    let p = must(&mut c, "SELECT price FROM products WHERE id = 7").await;
    let n = must(&mut c, "SELECT COUNT(*) FROM orders").await;
    ck.check(
        "the real table is untouched and the snapshot is gone",
        p == vec![vec![Some("2.5".into())]] && n[0][0].as_deref() == Some("0"),
        format!("{p:?} {n:?}"),
    );
    let (st, v) = http(
        &e.http,
        &auth,
        "POST",
        &format!("/v1/databases/{db}/logic:testRun"),
        Some(&json!({"name": "place", "args": [99, 1]})),
    )
    .await;
    ck.check(
        "REST: another event",
        st == 200 && v["event"] == "error" && v["result"]["message"] == "no product",
        format!("{st} {v}"),
    );
    let (st, v) = http(
        &e.http,
        &auth,
        "POST",
        &format!("/v1/databases/{db}/logic:testRun"),
        Some(&json!({"name": "nosuch"})),
    )
    .await;
    ck.check(
        "REST: an unknown function is 39860",
        st != 200 && v["code"] == 39860,
        format!("{st} {v}"),
    );

    // 3. gRPC, with a draft (nothing recorded or deployed).
    let ch = Endpoint::from_shared(format!("http://{}", e.http))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = LogicClient::new(ch);
    let mut req = tonic::Request::new(TestRunRequest {
        database: db.into(),
        source: "CREATE FUNCTION draft1(a INT) RETURNS INT LANGUAGE lua AS $$ log('doubling ' .. a) return a * 2 $$".into(),
        args: Some(proto(&json!([21]))),
        ..TestRunRequest::default()
    });
    req.metadata_mut()
        .insert("authorization", auth.parse().unwrap());
    let r = client.test_run(req).await;
    let ok = match &r {
        Ok(answer) => {
            let answer = answer.get_ref();
            let report = answer.report.as_ref().map_or(Value::Null, |s| {
                unproto(&prost_types::Value {
                    kind: Some(prost_types::value::Kind::StructValue(s.clone())),
                })
            });
            answer.event == "output"
                && report["result"]["value"].as_f64() == Some(42.0)
                && report["draft"] == true
                && report["logs"] == json!(["doubling 21"])
        }
        Err(_) => false,
    };
    ck.check("gRPC Logic.TestRun of a draft", ok, format!("{r:?}"));
    let r = sql(&mut c, "CALL draft1(1)").await;
    ck.check(
        "the draft wasn't deployed",
        matches!(r, Err((1305, _))),
        format!("{r:?}"),
    );

    // 4. Statements that could escape the sandbox are refused; fetch is
    // never sent.
    let draft = r#"CREATE FUNCTION esc() LANGUAGE javascript AS $$
  const out = {};
  try { db.exec('COMMIT'); out.commit = 'ran'; } catch (e) { out.commit = e.message; }
  try { db.exec('CREATE TABLE r7f_tmp (a INT)'); out.ddl = 'ran'; } catch (e) { out.ddl = e.message; }
  try { fetch('https://example.com/x'); out.fetch = 'sent'; } catch (e) { out.fetch = e.message; }
  try { db.call('plainp'); out.plain = 'ran'; } catch (e) { out.plain = e.message; }
  db.exec("INSERT INTO audit (k) VALUES ('kept?')");
  return out;
$$"#;
    let (st, v) = http(
        &e.http,
        &auth,
        "POST",
        &format!("/v1/databases/{db}/logic:testRun"),
        Some(&json!({"source": draft})),
    )
    .await;
    let stmts = v["statements"].as_array().cloned().unwrap_or_default();
    ck.check(
        "COMMIT, DDL and a plain procedure (which commits) are refused; fetch isn't sent",
        st == 200
            && v["result"]["commit"] != "ran"
            && v["result"]["ddl"] != "ran"
            && v["result"]["plain"]
                .as_str()
                .is_some_and(|m| m.contains("calls logic functions only"))
            && v["result"]["fetch"]
                .as_str()
                .is_some_and(|m| m.contains("allowlist"))
            && stmts.iter().filter(|s| s["kind"] == "refused").count() == 1
            && v["writes"].as_array().is_some_and(|w| w.len() == 1)
            && v["fetches"][0]["error"].is_string(),
        format!("{st} {v}"),
    );
    let n = must(&mut c, "SELECT COUNT(*) FROM audit").await;
    let t = sql(&mut c, "SELECT 1 FROM r7f_tmp").await;
    ck.check(
        "nothing escaped",
        n[0][0].as_deref() == Some("0") && matches!(t, Err((1146, _))),
        format!("{n:?} {t:?}"),
    );

    // 5. Trigger bodies: a stream trigger's ctx.record; a KIRun-style
    // validation's row binding is covered by the unit tests.
    let trig = "CREATE TRIGGER audit_k ON STREAM events LANGUAGE javascript AS $$
  db.exec('INSERT INTO audit (k) VALUES (?)', [ctx.record.key]);
  return {seen: ctx.record.key};
$$";
    let r = sql(
        &mut c,
        &format!(
            "TEST TRIGGER audit_k WITH '{}'",
            json!({"source": trig, "record": {"key": "k1", "value": "{}", "partition": 0, "offset": 5}})
                .to_string()
                .replace('\\', "\\\\")
                .replace('\'', "''")
        ),
    )
    .await;
    let rep = report_of(&r);
    ck.check(
        "a stream trigger's body with a given record",
        rep["result"]["seen"] == "k1"
            && rep["writes"][0]["sql"]
                .as_str()
                .is_some_and(|s| s.contains("audit"))
            && rep["notes"].to_string().contains("definer"),
        format!("{r:?}"),
    );

    // 6. The caller's rights and row policies.
    must(
        &mut c,
        "CREATE USER 'r7f_x'@'%' IDENTIFIED BY 'Sprig-dev-r7f-1A'",
    )
    .await;
    must(&mut c, &format!("GRANT SELECT ON {db}.docs TO 'r7f_x'@'%'")).await;
    must(
        &mut c,
        &format!("GRANT SELECT ON {db}.products TO 'r7f_x'@'%'"),
    )
    .await;
    for s in [
        format!(
            "CREATE POLICY own ON {db}.docs AS PERMISSIVE FOR ALL TO PUBLIC USING (owner = CURRENT_USER())"
        ),
        format!("ALTER TABLE {db}.docs ENABLE ROW LEVEL SECURITY"),
    ] {
        must(&mut c, &s).await;
    }
    must(
        &mut c,
        "CREATE OR REPLACE FUNCTION count_docs() LANGUAGE javascript AS $$
  const r = db.sqlOne('SELECT COUNT(*) AS n FROM docs');
  return {n: r.n};
$$",
    )
    .await;
    let xuser = format!("{}.r7f_x", e.tenant());
    let mut x = conn(&e.mysql, &xuser, "Sprig-dev-r7f-1A", Some(db)).await;
    let r = sql(&mut x, "TEST FUNCTION place (7, 1)").await;
    ck.check(
        "no EXECUTE: 1044",
        matches!(r, Err((1044, _))),
        format!("{r:?}"),
    );
    must(&mut c, &format!("GRANT EXECUTE ON {db}.* TO 'r7f_x'@'%'")).await;
    drop(x);
    let mut x = conn(&e.mysql, &xuser, "Sprig-dev-r7f-1A", Some(db)).await;
    let r = sql(&mut x, "TEST FUNCTION place (7, 1)").await;
    let rep = report_of(&r);
    ck.check(
        "EXECUTE but no INSERT: the run fails with 1142, as a call would",
        rep["error"]["code"] == 1142 && rep["event"].is_null(),
        format!("{r:?}"),
    );
    let r = sql(&mut x, "TEST FUNCTION count_docs").await;
    let rep = report_of(&r);
    let all = must(&mut c, "SELECT COUNT(*) FROM docs").await;
    ck.check(
        "row policies apply: r7f_x counts its own 2 docs",
        rep["result"]["n"] == 2,
        format!("{r:?} (all: {all:?})"),
    );
    let r = sql(
        &mut x,
        "TEST FUNCTION f WITH '{\"source\": \"CREATE FUNCTION f() LANGUAGE lua AS $$ return 1 $$\"}'",
    )
    .await;
    ck.check(
        "a draft needs CREATE ROUTINE",
        matches!(r, Err((1044, _))),
        format!("{r:?}"),
    );
    drop(x);

    let _ = sql(&mut c, "DROP USER IF EXISTS 'r7f_x'@'%'").await;
    must(&mut c, &format!("DROP DATABASE {db}")).await;
    ck.done("test runs");
}

const VER: &str =
    "CREATE OR REPLACE FUNCTION ver() RETURNS INT LANGUAGE javascript EXPOSE (sql, http)";

async fn ver(c: &mut Conn) -> Result<i64, (u16, String)> {
    let r = sql(c, "CALL ver()").await?;
    Ok(r[0][1].as_deref().unwrap_or("0").parse().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_rollout() {
    let Some(e) = env("staged_rollout") else {
        return;
    };
    let db = "r7follow_ro";
    let mut ck = Checks::new();
    let mut c = conn(&e.mysql, &e.user, &e.password, None).await;
    let _ = sql(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    for i in 0..12 {
        let _ = sql(&mut c, &format!("DROP USER IF EXISTS 'r7f_u{i}'@'%'")).await;
    }
    must(&mut c, &format!("CREATE DATABASE {db}")).await;
    must(&mut c, &format!("USE {db}")).await;
    for i in 0..12 {
        must(
            &mut c,
            &format!("CREATE USER 'r7f_u{i}'@'%' IDENTIFIED BY 'Sprig-dev-r7f-1A'"),
        )
        .await;
        must(
            &mut c,
            &format!("GRANT EXECUTE ON {db}.* TO 'r7f_u{i}'@'%'"),
        )
        .await;
    }
    must(&mut c, &format!("{VER} AS $$ return 1; $$")).await;
    let mut users = Vec::new();
    for i in 0..12 {
        users.push(
            conn(
                &e.mysql,
                &format!("{}.r7f_u{i}", e.tenant()),
                "Sprig-dev-r7f-1A",
                Some(db),
            )
            .await,
        );
    }
    // A candidate for half the callers.
    must(
        &mut c,
        &format!("{VER} WITH (SPRIG_ROLLOUT = 50) AS $$ return 2; $$"),
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "a deploy WITH (SPRIG_ROLLOUT = 50) stages version 2",
        n.contains("version 2 is the candidate for 50% of callers"),
        &n,
    );
    let mut got = Vec::new();
    let mut sticky = true;
    for u in &mut users {
        let a = ver(u).await.unwrap();
        for _ in 0..2 {
            sticky &= ver(u).await.unwrap() == a;
        }
        got.push(a);
    }
    ck.check(
        "each caller keeps its version; both versions answer",
        sticky && got.contains(&1) && got.contains(&2),
        format!("{got:?}"),
    );
    let mine = ver(&mut c).await.unwrap();
    let auth = basic(&e.user, &e.password);
    let (st, v) = http(
        &e.http,
        &auth,
        "POST",
        &format!("/v1/databases/{db}/functions/ver:call"),
        Some(&json!({"args": []})),
    )
    .await;
    ck.check(
        "REST follows the same pick as SQL for the same caller",
        st == 200 && v["result"]["value"] == json!(mine),
        format!("{st} {v} (SQL: {mine})"),
    );
    // A larger stage keeps everyone the smaller one had.
    must(&mut c, "ALTER FUNCTION ver ROLLOUT TO 90 PERCENT").await;
    let mut got90 = Vec::new();
    for u in &mut users {
        got90.push(ver(u).await.unwrap());
    }
    ck.check(
        "ROLLOUT TO 90: everyone who had version 2 keeps it",
        got.iter().zip(&got90).all(|(a, b)| *a == 1 || *b == 2)
            && got90.iter().filter(|v| **v == 2).count() >= got.iter().filter(|v| **v == 2).count(),
        format!("{got:?} → {got90:?}"),
    );
    // Numbers per version (gateways add them every 5 s).
    tokio::time::sleep(std::time::Duration::from_millis(5200)).await;
    let _ = ver(&mut users[0]).await;
    let (st, v) = http(
        &e.http,
        &auth,
        "GET",
        &format!("/v1/databases/{db}/logic/function/ver/rollout"),
        None,
    )
    .await;
    let stats = v["stats"].as_array().cloned().unwrap_or_default();
    let calls: i64 = stats.iter().map(|s| s["calls"].as_i64().unwrap_or(0)).sum();
    ck.check(
        "Studio's rollout view: the stage and each version's calls",
        st == 200
            && v["rollout"]["candidate_version"] == 2
            && v["rollout"]["pct"] == 90
            && stats.len() == 2
            && calls >= 30,
        format!("{st} {v}"),
    );
    // A different signature can't be staged.
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION ver(a INT) RETURNS INT LANGUAGE javascript WITH (SPRIG_ROLLOUT = 10) AS $$ return a; $$",
    )
    .await;
    ck.check(
        "other parameters: 39870",
        matches!(&r, Err((39870, m)) if m.contains("parameters")),
        format!("{r:?}"),
    );
    // Promote.
    must(&mut c, "ALTER FUNCTION ver ROLLOUT PROMOTE").await;
    let mut all2 = true;
    for u in &mut users {
        all2 &= ver(u).await.unwrap() == 2;
    }
    let (_, v) = http(
        &e.http,
        &auth,
        "GET",
        &format!("/v1/databases/{db}/logic/function/ver/rollout"),
        None,
    )
    .await;
    ck.check(
        "PROMOTE: version 2 for everyone; no rollout left",
        all2 && v["rollout"].is_null() && v["active_version"] == 2,
        format!("{v}"),
    );
    let s = sql(&mut c, &format!("SELECT {db}.ver()")).await;
    ck.check(
        "SELECT ver() answers the promoted version",
        s == Ok(vec![vec![Some("2".into())]]),
        format!("{s:?}"),
    );
    // Abort.
    must(
        &mut c,
        &format!("{VER} WITH (SPRIG_ROLLOUT = 50) AS $$ return 3; $$"),
    )
    .await;
    must(&mut c, "ALTER FUNCTION ver ROLLOUT ABORT").await;
    let mut all2 = true;
    for u in &mut users {
        all2 &= ver(u).await.unwrap() == 2;
    }
    ck.check("ABORT: version 2 again for everyone", all2, "");
    // By tenant: every caller of the tenant gets the same version.
    must(
        &mut c,
        &format!("{VER} WITH (SPRIG_ROLLOUT = 50, SPRIG_ROLLOUT_BY = 'tenant') AS $$ return 4; $$"),
    )
    .await;
    let n = notes(&mut c).await;
    let mut seen = std::collections::BTreeSet::new();
    for u in &mut users {
        seen.insert(ver(u).await.unwrap());
    }
    ck.check(
        "BY TENANT: one version for all of the tenant's callers",
        seen.len() == 1 && n.contains("this tenant gets version"),
        format!("{seen:?} {n}"),
    );
    must(&mut c, "ALTER FUNCTION ver ROLLOUT ABORT").await;
    // The automatic rollback: a candidate that fails every call.
    must(
        &mut c,
        &format!("{VER} WITH (SPRIG_ROLLOUT = 50) AS $$ throw new Error('bad version'); $$"),
    )
    .await;
    let mut failed = 0;
    let mut good = 0;
    for _ in 0..3 {
        for u in &mut users {
            match ver(u).await {
                Ok(_) => good += 1,
                Err(_) => failed += 1,
            }
        }
    }
    for u in &mut users {
        for _ in 0..4 {
            match ver(u).await {
                Ok(_) => good += 1,
                Err(_) => failed += 1,
            }
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(5200)).await;
    let _ = ver(&mut users[0]).await;
    let _ = ver(&mut users[1]).await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut all2 = true;
    for u in &mut users {
        all2 &= ver(u).await == Ok(2);
    }
    let (_, v) = http(
        &e.http,
        &auth,
        "GET",
        &format!("/v1/databases/{db}/logic/function/ver/rollout"),
        None,
    )
    .await;
    let (_, logs) = http(
        &e.http,
        &auth,
        "GET",
        &format!("/v1/databases/{db}/logic/function/ver/logs"),
        None,
    )
    .await;
    ck.check(
        "a failing candidate rolls back by itself and logic_log says why",
        failed >= 20
            && good > 0
            && all2
            && v["rollout"].is_null()
            && logs.to_string().contains("rolled back by itself"),
        format!("failed {failed}, good {good}, all2 {all2}, {v} {logs}"),
    );
    drop(users);
    for i in 0..12 {
        let _ = sql(&mut c, &format!("DROP USER IF EXISTS 'r7f_u{i}'@'%'")).await;
    }
    must(&mut c, &format!("DROP DATABASE {db}")).await;
    ck.done("staged rollout");
}

const QTY_RULE: &str = r#"{"namespace": "Test", "name": "QtyPositive",
 "parameters": {"qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}}},
 "events": {"output": {"name": "output", "parameters": {}},
            "reject": {"name": "reject", "parameters": {"message": {"type": ["STRING"]}}}},
 "steps": {
  "bad": {"statementName": "bad", "namespace": "System", "name": "If",
    "parameterMap": {"condition": {"c": {"type": "EXPRESSION", "expression": "Arguments.qty <= 0"}}}},
  "no": {"statementName": "no", "namespace": "System", "name": "GenerateEvent",
    "executeIftrue": {"Steps.bad.true": true},
    "parameterMap": {"eventName": {"e": {"type": "VALUE", "value": "reject"}},
      "results": {"r": {"type": "VALUE", "value": {"name": "message", "value": {"isExpression": false, "value": "qty must be positive"}}}}}}}}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drops_with_dependents() {
    let Some(e) = env("drops_with_dependents") else {
        return;
    };
    let db = "r7follow_dr";
    let mut ck = Checks::new();
    let mut c = conn(&e.mysql, &e.user, &e.password, None).await;
    let _ = sql(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    must(&mut c, &format!("CREATE DATABASE {db}")).await;
    must(&mut c, &format!("USE {db}")).await;
    must(&mut c, "CREATE TABLE t (id INT PRIMARY KEY, qty INT)").await;
    let auth = basic(&e.user, &e.password);
    let listed = |v: &Value, name: &str| {
        v["modules"]
            .as_array()
            .is_some_and(|m| m.iter().any(|x| x["name"] == name))
    };

    // A procedure a schedule calls.
    must(&mut c, "CREATE PROCEDURE p1() LANGUAGE javascript AS $$ db.exec('INSERT INTO t VALUES (1, 1)'); return {}; $$").await;
    must(&mut c, "CREATE SCHEDULE s1 CRON '0 3 * * *' DO CALL p1()").await;
    let r = sql(&mut c, "DROP PROCEDURE p1").await;
    ck.check(
        "DROP PROCEDURE with a schedule calling it: 39875",
        matches!(&r, Err((39875, m)) if m.contains("schedules s1")),
        format!("{r:?}"),
    );
    let r = sql(&mut c, "CALL p1()").await;
    ck.check(
        "still callable after the refusal",
        r.is_ok(),
        format!("{r:?}"),
    );
    must(&mut c, "DELETE FROM t").await;
    let r = sql(&mut c, "DROP PROCEDURE p1 CASCADE").await;
    let n = notes(&mut c).await;
    ck.check(
        "DROP PROCEDURE ... CASCADE drops the schedule too",
        r.is_ok() && n.contains("schedule s1 dropped with it"),
        format!("{r:?} {n}"),
    );
    let r = sql(&mut c, "CALL p1()").await;
    let (_, v) = http(
        &e.http,
        &auth,
        "GET",
        &format!("/v1/databases/{db}/logic"),
        None,
    )
    .await;
    ck.check(
        "the procedure and the schedule are gone",
        matches!(r, Err((1305, _))) && !listed(&v, "p1") && !listed(&v, "s1"),
        format!("{r:?} {v}"),
    );

    // A function other code calls.
    must(
        &mut c,
        "CREATE FUNCTION g(a INT) RETURNS INT LANGUAGE javascript AS $$ return a + 1; $$",
    )
    .await;
    must(&mut c, "CREATE FUNCTION h(a INT) RETURNS INT LANGUAGE javascript AS $$ return db.call('g', a).fields.value * 10; $$").await;
    let r = sql(&mut c, "CALL h(1)").await;
    ck.check(
        "h calls g",
        matches!(&r, Ok(rows) if rows[0][1].as_deref() == Some("20")),
        format!("{r:?}"),
    );
    let r = sql(&mut c, "DROP FUNCTION g").await;
    ck.check(
        "DROP FUNCTION g, which h calls: 39875",
        matches!(&r, Err((39875, m)) if m.contains("function h")),
        format!("{r:?}"),
    );
    let r = sql(&mut c, "DROP FUNCTION g CASCADE").await;
    let n = notes(&mut c).await;
    ck.check(
        "CASCADE drops g and says h's calls now fail",
        r.is_ok() && n.contains("function h calls g"),
        format!("{r:?} {n}"),
    );
    let r = sql(&mut c, "CALL g(1)").await;
    let s = sql(&mut c, &format!("SELECT {db}.g(1)")).await;
    ck.check(
        "g's wrapper and SELECT function are gone",
        matches!(r, Err((1305, _))) && s.is_err(),
        format!("{r:?} {s:?}"),
    );
    let r = sql(
        &mut c,
        "CREATE FUNCTION g(a INT) RETURNS INT LANGUAGE lua AS $$ return a + 2 $$",
    )
    .await;
    let n = notes(&mut c).await;
    let r2 = sql(&mut c, "CALL h(1)").await;
    ck.check(
        "the name can be used again, from version 1",
        r.is_ok()
            && n.contains("version 1")
            && matches!(&r2, Ok(rows) if rows[0][1].as_deref() == Some("30")),
        format!("{r:?} {n} {r2:?}"),
    );

    // A validation.
    must(
        &mut c,
        &format!(
            "CREATE VALIDATION qty_positive ON t FOR INSERT, UPDATE LANGUAGE kirun AS '{QTY_RULE}'"
        ),
    )
    .await;
    let r = sql(&mut c, "INSERT INTO t VALUES (5, -1)").await;
    ck.check(
        "the validation rejects",
        matches!(r, Err((39863, _))),
        format!("{r:?}"),
    );
    let r = sql(&mut c, "DROP VALIDATION qty_positive").await;
    let w = sql(&mut c, "INSERT INTO t VALUES (6, -1)").await;
    ck.check(
        "DROP VALIDATION: the rule is gone",
        r.is_ok() && w.is_ok(),
        format!("{r:?} {w:?}"),
    );
    let r = sql(&mut c, "DROP VALIDATION qty_positive").await;
    let r2 = sql(&mut c, "DROP VALIDATION IF EXISTS qty_positive").await;
    ck.check(
        "a second DROP VALIDATION: 39860; IF EXISTS: a note",
        matches!(r, Err((39860, _))) && r2.is_ok(),
        format!("{r:?} {r2:?}"),
    );
    // Plain SQL routines are ordinary DDL.
    must(&mut c, "CREATE PROCEDURE plain() SELECT 1").await;
    let r = sql(&mut c, "DROP PROCEDURE plain").await;
    let r2 = sql(&mut c, "DROP FUNCTION IF EXISTS nosuch").await;
    ck.check(
        "plain SQL routines drop as before",
        r.is_ok() && r2.is_ok(),
        format!("{r:?} {r2:?}"),
    );
    must(&mut c, &format!("DROP DATABASE {db}")).await;
    ck.done("drops");
}
