//! Logic functions from MongoDB and Redis through a live gateway
//! (chapter 41, "Calling code"; chapter 40 step 19; R7 close-out):
//! `db.runCommand({sprigCall: ...})` through the Rust `mongodb` driver and
//! `FCALL` / `FCALL_RO` through the `redis` crate, of functions compiled
//! into MariaDB by the JavaScript front end and of engine-tier JavaScript
//! and Lua; exposure, transactions, read-only calls, and Redis's own Lua
//! library functions found first.
//!
//! Needs a gateway with MySQL, MongoDB and Redis listeners and a tenant
//! user who may create databases and routines:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_SQL=mysql://acme.app:sprig-dev-app@127.0.0.1:26306
//! export SPRIG_TEST_LOGIC_MONGO='mongodb://acme.app:sprig-dev-app@127.0.0.1:26737/?authSource=admin&directConnection=true'
//! export SPRIG_TEST_LOGIC_REDIS=redis://acme.app:sprig-dev-app@127.0.0.1:26380
//! cargo test -p sprig-gateway --test logic_protocols_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `r7close_live` (and functions in the tenant's
//! Redis database `redis` named `r7c_*`), dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    missing_docs
)]

use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;

const DB: &str = "r7close_live";

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

const QUOTE_JS: &str = r"
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || !(p.stock >= qty))
    return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}";

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

async fn sql(c: &mut mysql_async::Conn, q: &str) -> Vec<mysql_async::Row> {
    c.query(q).await.unwrap_or_else(|e| panic!("{q}: {e}"))
}

/// The deploy's notes (`SHOW WARNINGS`), joined.
async fn deploy(c: &mut mysql_async::Conn, q: &str) -> String {
    c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
    let notes: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap();
    notes
        .into_iter()
        .map(|(_, _, m)| m)
        .collect::<Vec<_>>()
        .join("; ")
}

async fn setup(c: &mut mysql_async::Conn) {
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)".into(),
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64))".into(),
        "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3)".into(),
    ] {
        c.query_drop(&q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    let n = deploy(
        c,
        &format!(
            "CREATE FUNCTION quote(productId BIGINT, qty INT) LANGUAGE javascript \
             EXPOSE (mongo, redis) AS $${QUOTE_JS}$$"
        ),
    )
    .await;
    println!("quote: {n}");
    assert!(
        n.contains("tier mariadb"),
        "the JavaScript quote compiles: {n}"
    );
    let n = deploy(
        c,
        "CREATE FUNCTION place(orderId BIGINT, productId BIGINT, qty INT) LANGUAGE javascript \
         EXPOSE (mongo, redis) AS $$ \
           db.exec('INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, ?)', \
                   [orderId, productId, qty, 'placed ' + orderId]); \
           return { placed: orderId }; $$",
    )
    .await;
    println!("place: {n}");
    assert!(n.contains("tier mariadb"), "{n}");
    let n = deploy(
        c,
        "CREATE FUNCTION greet(name VARCHAR(32)) RETURNS VARCHAR(64) LANGUAGE javascript \
         EXPOSE (mongo, redis) AS $$ \
           try { return 'hello ' + name.toUpperCase(); } catch (e) { return 'hello nobody'; } $$",
    )
    .await;
    println!("greet: {n}");
    assert!(
        n.contains("tier engine"),
        "try/catch runs in the engine tier: {n}"
    );
    deploy(
        c,
        "CREATE FUNCTION lsum(a INT, b INT) LANGUAGE lua EXPOSE (mongo, redis) \
         AS $$ return { sum = a + b } $$",
    )
    .await;
    deploy(
        c,
        "CREATE FUNCTION hidden(x INT) LANGUAGE javascript AS $$ return { x }; $$",
    )
    .await;
    // A validation on orders (R7 order 3): it holds for writes that
    // logic functions make, whichever protocol called them.
    deploy(c, &format!("CREATE VALIDATION qty_positive ON orders FOR INSERT, UPDATE LANGUAGE kirun AS '{QTY_RULE}'")).await;
    // SQL still reaches all of them.
    let r = sql(c, "CALL quote(7, 3)").await;
    let event: String = r[0].get(0).unwrap();
    let total: f64 = r[0].get(1).unwrap();
    assert_eq!((event.as_str(), total), ("output", 1240.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mongo_and_redis_call_logic_functions() {
    let (Some(sql_url), Some(mongo_url), Some(redis_url)) = (
        var("SPRIG_TEST_LOGIC_SQL"),
        var("SPRIG_TEST_LOGIC_MONGO"),
        var("SPRIG_TEST_LOGIC_REDIS"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_LOGIC_SQL, SPRIG_TEST_LOGIC_MONGO and SPRIG_TEST_LOGIC_REDIS"
        );
        return;
    };
    let opts = mysql_async::Opts::from_url(&sql_url).unwrap();
    let opts = mysql_async::OptsBuilder::from_opts(opts).ssl_opts(Some(
        mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true),
    ));
    let mut my = mysql_async::Conn::new(opts).await.unwrap();
    setup(&mut my).await;
    let mut checks = 0;
    let started = std::time::Instant::now();

    // ---- MongoDB ---------------------------------------------------------------
    let client = mongodb::Client::with_uri_str(&mongo_url).await.unwrap();
    let mdb = client.database(DB);
    let call = |cmd: Document| {
        let mdb = mdb.clone();
        async move { mdb.run_command(cmd).await }
    };
    let r = call(doc! {"sprigCall": "quote", "args": [7i64, 3i64]})
        .await
        .unwrap();
    assert_eq!(r.get_str("event").unwrap(), "output");
    let total = r.get_document("result").unwrap().get("total").unwrap();
    assert_eq!(bson_f64(total), 1240.0, "{r}");
    checks += 1;
    let r = call(doc! {"sprigCall": "quote", "args": {"qty": 1000, "productId": 7}})
        .await
        .unwrap();
    assert_eq!(r.get_str("event").unwrap(), "error", "named arguments: {r}");
    assert_eq!(
        r.get_document("result")
            .unwrap()
            .get_str("message")
            .unwrap(),
        "not enough stock"
    );
    checks += 1;
    let r = call(doc! {"sprigCall": "greet", "args": ["ann"]})
        .await
        .unwrap();
    assert_eq!(
        r.get_document("result").unwrap().get_str("value").unwrap(),
        "hello ANN",
        "engine-tier JavaScript: {r}"
    );
    let r = call(doc! {"sprigCall": "greet", "args": [Bson::Null]})
        .await
        .unwrap();
    assert_eq!(
        r.get_document("result").unwrap().get_str("value").unwrap(),
        "hello nobody"
    );
    checks += 2;
    let r = call(doc! {"sprigCall": "lsum", "args": [2, 3]})
        .await
        .unwrap();
    assert_eq!(
        bson_f64(r.get_document("result").unwrap().get("sum").unwrap()),
        5.0,
        "lua: {r}"
    );
    checks += 1;
    let e = call(doc! {"sprigCall": "hidden", "args": [1]})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 39789, "not exposed: {e}");
    let e = call(doc! {"sprigCall": "nosuch"}).await.unwrap_err();
    assert_eq!(code_of(&e), 39860, "unknown: {e}");
    let e = call(doc! {"sprigCall": "quote", "args": {"nope": 1}})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 39874, "unknown parameter: {e}");
    checks += 3;
    // In a transaction the call joins it: aborted, nothing is written.
    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    let r = mdb
        .run_command(doc! {"sprigCall": "place", "args": [501, 7, 1]})
        .session(&mut session)
        .await
        .unwrap();
    assert_eq!(
        bson_f64(r.get_document("result").unwrap().get("placed").unwrap()),
        501.0
    );
    session.abort_transaction().await.unwrap();
    let n: Vec<u64> = my
        .query(format!("SELECT COUNT(*) FROM {DB}.orders WHERE id = 501"))
        .await
        .unwrap();
    assert_eq!(n, [0], "the aborted transaction's call wrote nothing");
    session.start_transaction().await.unwrap();
    mdb.run_command(doc! {"sprigCall": "place", "args": [502, 7, 1]})
        .session(&mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();
    call(doc! {"sprigCall": "place", "args": [503, 8, 1]})
        .await
        .unwrap();
    let n: Vec<u64> = my
        .query(format!(
            "SELECT COUNT(*) FROM {DB}.orders WHERE id IN (502, 503)"
        ))
        .await
        .unwrap();
    assert_eq!(n, [2], "committed and autocommit calls wrote");
    checks += 3;
    let e = call(doc! {"sprigCall": "place", "args": [504, 7, 0]})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 121, "a validation through MongoDB: {e}");
    checks += 1;
    println!("mongo: {checks} checks pass");

    // ---- Redis --------------------------------------------------------------------
    let rc = redis::Client::open(redis_url.as_str()).unwrap();
    let mut r = rc.get_multiplexed_async_connection().await.unwrap();
    async fn cmd(
        r: &mut redis::aio::MultiplexedConnection,
        args: &[&str],
    ) -> redis::RedisResult<redis::Value> {
        let mut c = redis::cmd(args[0]);
        for a in &args[1..] {
            c.arg(*a);
        }
        c.query_async(r).await
    }
    let quote = format!("{DB}.quote");
    let v = cmd(&mut r, &["FCALL", &quote, "0", "7", "3"])
        .await
        .unwrap();
    assert_eq!(value_f64(&v), 1240.0, "{v:?}");
    let v = cmd(&mut r, &["FCALL", &quote, "1", "7", "3"])
        .await
        .unwrap();
    assert_eq!(
        value_f64(&v),
        1240.0,
        "a key binds the first parameter: {v:?}"
    );
    let v = cmd(&mut r, &["FCALL_RO", &quote, "1", "7", "3"])
        .await
        .unwrap();
    assert_eq!(value_f64(&v), 1240.0, "a read-only call of a reader: {v:?}");
    checks += 3;
    let e = cmd(&mut r, &["FCALL", &quote, "0", "7", "1000"])
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("39865") && e.to_string().contains("not enough stock"),
        "{e}"
    );
    checks += 1;
    let place = format!("{DB}.place");
    let v = cmd(&mut r, &["FCALL", &place, "0", "601", "7", "1"])
        .await
        .unwrap();
    assert_eq!(value_f64(&v), 601.0, "{v:?}");
    let e = cmd(&mut r, &["FCALL_RO", &place, "0", "602", "7", "1"])
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("Write commands are not allowed"),
        "{e}"
    );
    let n: Vec<u64> = my
        .query(format!(
            "SELECT COUNT(*) FROM {DB}.orders WHERE id IN (601, 602)"
        ))
        .await
        .unwrap();
    assert_eq!(n, [1], "FCALL wrote, FCALL_RO didn't");
    checks += 3;
    let e = cmd(&mut r, &["FCALL", &place, "0", "604", "7", "0"])
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("39863") && e.to_string().contains("qty must be positive"),
        "a validation through Redis: {e}"
    );
    checks += 1;
    let v = cmd(&mut r, &["FCALL", &format!("{DB}.greet"), "0", "bo"])
        .await
        .unwrap();
    assert_eq!(value_text(&v), "hello BO", "{v:?}");
    let e = cmd(&mut r, &["FCALL", &format!("{DB}.hidden"), "0", "1"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("39789"), "not exposed: {e}");
    let e = cmd(&mut r, &["FCALL", "r7c_nosuch", "0"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("Function not found"), "{e}");
    checks += 3;
    // MULTI: FCALL joins EXEC's transaction; FCALL_RO can't.
    let mut pipe = redis::pipe();
    pipe.atomic()
        .cmd("FCALL")
        .arg(&place)
        .arg("0")
        .arg("603")
        .arg("8")
        .arg("1");
    let v: redis::Value = pipe.query_async(&mut r).await.unwrap();
    println!("MULTI FCALL: {v:?}");
    let n: Vec<u64> = my
        .query(format!("SELECT COUNT(*) FROM {DB}.orders WHERE id = 603"))
        .await
        .unwrap();
    assert_eq!(n, [1], "FCALL inside MULTI ran");
    let mut pipe = redis::pipe();
    pipe.atomic()
        .cmd("FCALL_RO")
        .arg(&quote)
        .arg("0")
        .arg("7")
        .arg("3");
    let v: redis::RedisResult<redis::Value> = pipe.query_async(&mut r).await;
    println!("MULTI FCALL_RO: {v:?}");
    assert!(
        format!("{v:?}").contains("read-only call can't join"),
        "FCALL_RO inside MULTI is refused"
    );
    checks += 2;
    // A plain name is looked up in the tenant's Redis database; Redis's
    // own Lua library functions are found first.
    cmd(&mut r, &["SET", "r7c:k", "1"]).await.unwrap();
    deploy(
        &mut my,
        "CREATE OR REPLACE FUNCTION redis.r7c_sum(a INT, b INT) LANGUAGE lua EXPOSE (redis) \
         AS $$ return { sum = a + b } $$",
    )
    .await;
    let v = cmd(&mut r, &["FCALL", "r7c_sum", "0", "2", "3"])
        .await
        .unwrap();
    assert_eq!(value_f64(&v), 5.0, "{v:?}");
    let _ = cmd(&mut r, &["FUNCTION", "DELETE", "r7clib"]).await;
    cmd(
        &mut r,
        &[
            "FUNCTION",
            "LOAD",
            "#!lua name=r7clib\nredis.register_function('r7c_sum', function(keys, args) return 'lua' end)",
        ],
    )
    .await
    .unwrap();
    let v = cmd(&mut r, &["FCALL", "r7c_sum", "0", "2", "3"])
        .await
        .unwrap();
    assert_eq!(
        value_text(&v),
        "lua",
        "Lua library functions come first: {v:?}"
    );
    cmd(&mut r, &["FUNCTION", "DELETE", "r7clib"])
        .await
        .unwrap();
    let v = cmd(&mut r, &["FCALL", "r7c_sum", "0", "2", "3"])
        .await
        .unwrap();
    assert_eq!(value_f64(&v), 5.0, "{v:?}");
    let v = cmd(&mut r, &["EVAL", "return ARGV[1]", "0", "still"])
        .await
        .unwrap();
    assert_eq!(value_text(&v), "still");
    checks += 4;
    cmd(&mut r, &["DEL", "r7c:k"]).await.unwrap();
    println!(
        "mongo and redis: {checks} checks pass in {:?}",
        started.elapsed()
    );

    // ---- cleanup --------------------------------------------------------------------
    my.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    for f in ["r7c_sum"] {
        let _ = my
            .query_drop(format!("DROP PROCEDURE IF EXISTS redis.{f}"))
            .await;
        let _ = my
            .query_drop(format!("DROP FUNCTION IF EXISTS redis.{f}"))
            .await;
    }
    drop(my);
}

fn bson_f64(v: &Bson) -> f64 {
    match v {
        Bson::Double(d) => *d,
        Bson::Int32(i) => f64::from(*i),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => *i as f64,
        other => panic!("not a number: {other}"),
    }
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        other => panic!("not a command error: {other:?}"),
    }
}

fn value_text(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(i) => i.to_string(),
        redis::Value::Double(d) => d.to_string(),
        other => panic!("not text: {other:?}"),
    }
}

fn value_f64(v: &redis::Value) -> f64 {
    value_text(v).parse().unwrap()
}
