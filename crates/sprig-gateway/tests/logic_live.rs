//! Logic functions through a live gateway (chapter 41, "Calling code";
//! chapter 37, "Logic functions as endpoints"; R7 order 6): the same
//! function written in KIRun (compiled into MariaDB), JavaScript (QuickJS)
//! and Lua (Lua 5.1) called with `CALL`, `SELECT f(...)`, REST and gRPC;
//! transactions; exposure; the caller's rights; budgets and the sandbox.
//!
//! Needs a gateway with an HTTP listener and a tenant user who may create
//! databases, users and routines:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:38307,127.0.0.1:38080   # MySQL wire, HTTP
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=sprig-dev-app
//! cargo test -p sprig-gateway --test logic_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `r7rest_live` and with the user `r7rexec`,
//! dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_api::proto::v1::CallRequest;
use sprig_api::proto::v1::functions_client::FunctionsClient;
use sprig_testkit::skip;
use tonic::transport::{Channel, Endpoint};

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "r7rest_live";

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

async fn conn(addr: &str, user: &str, password: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(password))
        .db_name(db)
        .ssl_opts(Some(SslOpts::default().with_danger_accept_invalid_certs(true)));
    Conn::new(opts).await.unwrap()
}

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Vec<Vec<Option<String>>>, (u16, String)> {
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

fn basic(user: &str, password: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
    )
}

async fn rest(addr: &str, auth: &str, path: &str, body: &Value) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", addr)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
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

async fn call_rest(e: &Env, auth: &str, f: &str, args: Value, txn: &str) -> (u16, Value) {
    rest(
        &e.http,
        auth,
        &format!("/v1/databases/{DB}/functions/{f}:call"),
        &json!({"args": args, "transaction": txn}),
    )
    .await
}

async fn channel(addr: &str) -> Channel {
    Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
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

fn quote_kirun() -> String {
    std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sprig-logic-compile/tests/kirun/quote.json"
    ))
    .unwrap()
    .replace('\'', "''")
}

const SETUP: &[&str] = &[
    "DROP TABLE IF EXISTS products",
    "DROP TABLE IF EXISTS orders",
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64))",
    "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0)",
    "INSERT INTO orders VALUES (100, 7, 1, 'first')",
];

fn deploys() -> Vec<String> {
    vec![
        format!(
            "CREATE OR REPLACE FUNCTION quote_k LANGUAGE kirun EXPOSE (sql, http, grpc) AS '{}'",
            quote_kirun()
        ),
        "CREATE OR REPLACE FUNCTION quote_js(productId BIGINT, qty INT) LANGUAGE javascript \
         EXPOSE (sql, http, grpc) AS $$
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || !(p.stock >= qty)) return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}
$$"
        .into(),
        "CREATE OR REPLACE FUNCTION quote_lua(productId BIGINT, qty INT) LANGUAGE lua \
         EXPOSE (sql, http) AS $$
local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', productId)
if p == sprig.null or not (p.stock >= qty) then
  return sprig.event('error', { message = 'not enough stock' })
end
return { total = p.price * qty + 40 }
$$"
        .into(),
        "CREATE OR REPLACE PROCEDURE order_js(orderId BIGINT, productId BIGINT, qty INT) LANGUAGE js \
         EXPOSE (http, grpc) AS $$
const t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', [qty, productId, qty]);
if (t.affected !== 1) return sprig.event('error', { message: 'out of stock' });
db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [orderId, productId, qty]);
return { left: db.sqlOne('SELECT stock FROM products WHERE id = ?', [productId]).stock };
$$"
        .into(),
        "CREATE OR REPLACE FUNCTION net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE js \
         AS $$ return price * qty * 0.9; $$"
            .into(),
        "CREATE OR REPLACE PROCEDURE spin_lua() LANGUAGE lua \
         AS $$ while true do pcall(function () while true do end end) end $$"
            .into(),
        "CREATE OR REPLACE PROCEDURE escape_js() LANGUAGE js AS $$
return { require: typeof require, process: typeof process, os: typeof os, std: typeof std,
         status: (() => { try { return fetch('http://169.254.169.254/').status; } catch (e) { return e.code; } })() };
$$"
        .into(),
    ]
}

#[tokio::test]
async fn functions_through_sql_rest_and_grpc() {
    let Some(e) = env("functions_through_sql_rest_and_grpc") else {
        return;
    };
    let auth = basic(&e.user, &e.password);
    let mut c = conn(&e.mysql, &e.user, &e.password, None).await;
    c.query_drop(format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .await
        .unwrap();
    c.query_drop(format!("USE {DB}")).await.unwrap();
    for s in SETUP {
        c.query_drop(*s).await.unwrap();
    }
    let started = std::time::Instant::now();
    for d in deploys() {
        sql(&mut c, &d)
            .await
            .unwrap_or_else(|err| panic!("{d}: {err:?}"));
    }
    println!("7 deploys in {:?}", started.elapsed());
    let mut failures = Vec::new();
    let mut check = |what: &str, ok: bool, got: String| {
        println!("{}  {what}: {got}", if ok { "ok  " } else { "FAIL" });
        if !ok {
            failures.push(format!("{what}: {got}"));
        }
    };

    // CALL: the three languages agree.
    for f in ["quote_k", "quote_js", "quote_lua"] {
        let r = sql(&mut c, &format!("CALL {f}(7, 3)")).await;
        let ok = matches!(&r, Ok(rows) if rows[0][0].as_deref() == Some("output") && rows[0][1].as_deref() == Some("1240"));
        check(&format!("CALL {f}(7, 3)"), ok, format!("{r:?}"));
        let r = sql(&mut c, &format!("CALL {f}(7, 1000)")).await;
        let ok = matches!(&r, Ok(rows) if rows[0][0].as_deref() == Some("error"));
        check(&format!("CALL {f}(7, 1000)"), ok, format!("{r:?}"));
    }
    // SELECT f(...): compiled through its stored function, engine-tier
    // through the gateway.
    let r = sql(
        &mut c,
        "SELECT quote_k(7, 3), quote_js(7, 3), quote_lua(7, 3) AS l, net(10, 3)",
    )
    .await;
    check(
        "SELECT quote_k, quote_js, quote_lua, net",
        r == Ok(vec![vec![
            Some("1240".into()),
            Some("1240".into()),
            Some("1240".into()),
            Some("27".into()),
        ]]),
        format!("{r:?}"),
    );
    for f in ["quote_k", "quote_js"] {
        let r = sql(&mut c, &format!("SELECT {f}(7, 1000)")).await;
        check(
            &format!("SELECT {f}(7, 1000)"),
            matches!(&r, Err((39865, m)) if m.contains("not enough stock")),
            format!("{r:?}"),
        );
    }

    // REST: positional and named arguments; the same answers.
    for f in ["quote_k", "quote_js", "quote_lua"] {
        for args in [json!([7, 3]), json!({"qty": 3, "productId": 7})] {
            let (st, v) = call_rest(&e, &auth, f, args.clone(), "").await;
            let ok = st == 200
                && v["event"] == "output"
                && v["result"]["total"].as_f64() == Some(1240.0);
            check(&format!("REST {f} {args}"), ok, format!("{st} {v}"));
        }
    }
    let (st, v) = call_rest(&e, &auth, "net", json!([1, 2]), "").await;
    check(
        "REST net (not exposed to http)",
        st == 404 && v["code"] == 39789,
        format!("{st} {v}"),
    );
    let (st, v) = call_rest(&e, &auth, "nope", json!([]), "").await;
    check(
        "REST unknown function",
        st == 404 && v["code"] == 39860,
        format!("{st} {v}"),
    );
    let (st, v) = call_rest(&e, &auth, "quote_js", json!({"nope": 1}), "").await;
    check(
        "REST unknown parameter",
        v["code"] == 39874,
        format!("{st} {v}"),
    );

    // gRPC.
    let ch = channel(&e.http).await;
    let mut fc = FunctionsClient::new(ch);
    for (f, want_ok) in [("quote_js", true), ("quote_k", true), ("quote_lua", false)] {
        let mut req = tonic::Request::new(CallRequest {
            database: DB.into(),
            name: f.into(),
            args: Some(proto(&json!([7, 3]))),
            transaction: String::new(),
        });
        req.metadata_mut()
            .insert("authorization", auth.parse().unwrap());
        let r = fc.call(req).await;
        let ok = match (&r, want_ok) {
            (Ok(resp), true) => {
                let resp = resp.get_ref();
                resp.event == "output"
                    && resp
                        .result
                        .as_ref()
                        .and_then(|s| s.fields.get("total"))
                        .and_then(|v| match v.kind {
                            Some(prost_types::value::Kind::NumberValue(n)) => Some(n),
                            _ => None,
                        })
                        == Some(1240.0)
            }
            (Err(s), false) => s.code() == tonic::Code::NotFound && s.message().contains("grpc"),
            _ => false,
        };
        check(&format!("gRPC {f}"), ok, format!("{r:?}").chars().take(200).collect());
    }

    // Transactions: a handle's rollback undoes the call; without one the
    // call commits.
    let (st, t) = rest(
        &e.http,
        &auth,
        &format!("/v1/databases/{DB}/transactions"),
        &json!({}),
    )
    .await;
    let handle = t["name"].as_str().unwrap_or_default().to_owned();
    check("REST begin", st == 200 && !handle.is_empty(), format!("{st} {t}"));
    let (st, v) = call_rest(&e, &auth, "order_js", json!([1, 7, 2]), &handle).await;
    check("REST order_js in the transaction", st == 200 && v["result"]["left"] == 8, format!("{st} {v}"));
    let (st, _) = rest(&e.http, &auth, &format!("/v1/{handle}:rollback"), &json!({})).await;
    let r = sql(&mut c, "SELECT COUNT(*) FROM orders").await;
    check(
        "after rollback",
        st == 200 && r == Ok(vec![vec![Some("1".into())]]),
        format!("{st} {r:?}"),
    );
    let (st, v) = call_rest(&e, &auth, "order_js", json!([2, 7, 2]), "").await;
    let r = sql(&mut c, "SELECT COUNT(*) FROM orders").await;
    check(
        "REST order_js without a handle commits",
        st == 200 && r == Ok(vec![vec![Some("2".into())]]),
        format!("{st} {v} {r:?}"),
    );
    // Its own transaction: a failure half way leaves nothing.
    let (st, v) = call_rest(&e, &auth, "order_js", json!([100, 7, 1]), "").await;
    let r = sql(&mut c, "SELECT stock FROM products WHERE id = 7").await;
    check(
        "REST order_js failing on a duplicate rolls back its update",
        st != 200 && v["code"] == 1062 && r == Ok(vec![vec![Some("8".into())]]),
        format!("{st} {v} {r:?}"),
    );

    // The caller's rights: EXECUTE only, then SELECT too.
    let _ = sql(&mut c, "DROP USER IF EXISTS 'r7rexec'@'%'").await;
    sql(
        &mut c,
        "CREATE USER 'r7rexec'@'%' IDENTIFIED BY 'Sprig-dev-exec-1A'",
    )
    .await
    .unwrap();
    sql(&mut c, &format!("GRANT EXECUTE ON {DB}.* TO 'r7rexec'@'%'"))
        .await
        .unwrap();
    let tenant = e.user.split('.').next().unwrap();
    let xuser = format!("{tenant}.r7rexec");
    let xauth = basic(&xuser, "Sprig-dev-exec-1A");
    let mut x = conn(&e.mysql, &xuser, "Sprig-dev-exec-1A", Some(DB)).await;
    for f in ["quote_k", "quote_js", "quote_lua"] {
        let r = sql(&mut x, &format!("CALL {f}(7, 3)")).await;
        check(
            &format!("EXECUTE only: CALL {f}"),
            matches!(r, Err((1142, _))),
            format!("{r:?}"),
        );
    }
    let (st, v) = call_rest(&e, &xauth, "quote_js", json!([7, 3]), "").await;
    check("EXECUTE only: REST quote_js", v["code"] == 1142, format!("{st} {v}"));
    sql(&mut c, &format!("GRANT SELECT ON {DB}.products TO 'r7rexec'@'%'"))
        .await
        .unwrap();
    drop(x);
    let mut x = conn(&e.mysql, &xuser, "Sprig-dev-exec-1A", Some(DB)).await;
    for f in ["quote_k", "quote_js", "quote_lua"] {
        let r = sql(&mut x, &format!("CALL {f}(7, 3)")).await;
        check(
            &format!("EXECUTE + SELECT: CALL {f}"),
            matches!(&r, Ok(rows) if rows[0][1].as_deref() == Some("1240")),
            format!("{r:?}"),
        );
    }
    drop(x);

    // Budgets and the sandbox.
    let t0 = std::time::Instant::now();
    let r = sql(&mut c, "CALL spin_lua()").await;
    check(
        "a pcall-wrapped infinite Lua loop",
        matches!(&r, Err((39864, m)) if m.contains("wall time")),
        format!("{r:?} after {:?}", t0.elapsed()),
    );
    let r = sql(&mut c, "CALL escape_js()").await;
    check(
        "the JavaScript sandbox",
        matches!(&r, Ok(rows) if rows[0][1..5].iter().all(|v| v.as_deref() == Some("undefined")) && rows[0][5].as_deref() == Some("39867")),
        format!("{r:?}"),
    );

    let _ = sql(&mut c, "DROP USER IF EXISTS 'r7rexec'@'%'").await;
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    drop(c);
    assert!(failures.is_empty(), "failures:\n{}", failures.join("\n"));
}
