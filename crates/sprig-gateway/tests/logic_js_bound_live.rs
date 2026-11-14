//! JavaScript computed columns, validations and after-commit triggers
//! through a live gateway. Each
//! JavaScript object has a KIRun twin doing the same thing on its own
//! table, and the two must give the same results: generated values, which
//! writes are refused and with what, and the audit rows a trigger writes.
//!
//! Needs a gateway with workers (after-commit triggers run in a worker)
//! and a tenant user who may create databases and routines:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:36306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=sprig-dev-app
//! cargo test -p sprig-gateway --test logic_js_bound_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `jsbind_db`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "jsbind_db";

async fn connect(test: &str) -> Option<Conn> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    let (host, port) = urls[0].rsplit_once(':').unwrap();
    let user = std::env::var("SPRIG_TEST_LOGIC_USER").unwrap_or_else(|_| "acme.app".into());
    let password =
        std::env::var("SPRIG_TEST_LOGIC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into());
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Some(Conn::new(opts).await.unwrap())
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

/// KIRun: 10% off from 10 items (`pure`'s test definition).
const NET_K: &str = r#"{"namespace": "Shop", "name": "Net",
  "parameters": {
    "price": {"parameterName": "price", "schema": {"type": ["DOUBLE"]}},
    "qty": {"parameterName": "qty", "schema": {"type": ["LONG"]}}},
  "events": {"output": {"name": "output", "parameters": {"value": {"type": ["DOUBLE"]}}}},
  "steps": {
    "bulk": {"statementName": "bulk", "namespace": "System", "name": "If",
      "parameterMap": {"condition": {"c": {"type": "EXPRESSION",
        "expression": "Arguments.qty >= 10"}}}},
    "cheap": {"statementName": "cheap", "namespace": "System", "name": "GenerateEvent",
      "executeIftrue": {"Steps.bulk.true": true},
      "parameterMap": {"results": {"r": {"type": "VALUE", "value": {"name": "value",
          "value": {"isExpression": true, "value": "Arguments.price * Arguments.qty * 0.9"}}}}}},
    "full": {"statementName": "full", "namespace": "System", "name": "GenerateEvent",
      "executeIftrue": {"Steps.bulk.false": true},
      "parameterMap": {"results": {"r": {"type": "VALUE", "value": {"name": "value",
          "value": {"isExpression": true, "value": "Arguments.price * Arguments.qty"}}}}}}}}"#;

const NET_JS: &str = "export default function (price, qty) {
  if (qty >= 10) return price * qty * 0.9;
  return price * qty;
}";

/// KIRun: no discount above 20% (one fixed message: a `CHECK`).
const DISCOUNT_K: &str = r#"{"namespace": "Shop", "name": "Discount",
  "parameters": {
    "discount_pct": {"parameterName": "discount_pct", "schema": {"type": ["DOUBLE", "NULL"]}}},
  "events": {"output": {"name": "output", "parameters": {}},
             "reject": {"name": "reject", "parameters": {"message": {"type": ["STRING"]}}}},
  "steps": {
    "big": {"statementName": "big", "namespace": "System", "name": "If",
      "parameterMap": {"condition": {"c": {"type": "EXPRESSION",
        "expression": "Arguments.discount_pct > 20"}}}},
    "no": {"statementName": "no", "namespace": "System", "name": "GenerateEvent",
      "executeIftrue": {"Steps.big.true": true},
      "parameterMap": {"eventName": {"e": {"type": "VALUE", "value": "reject"}},
        "results": {"r": {"type": "VALUE", "value": {"name": "message",
          "value": {"isExpression": false, "value": "discount above 20%"}}}}}}}}"#;

const DISCOUNT_JS: &str = "export default function (row) {
  if (row.discount_pct > 20) sprig.reject('discount above 20%');
}";

/// KIRun: a shipped order's total stays (reads the old row: triggers).
const SHIPPED_K: &str = r#"{"namespace": "Shop", "name": "Shipped",
  "parameters": {
    "total": {"parameterName": "total", "schema": {"type": ["DOUBLE"]}},
    "old_total": {"parameterName": "old_total", "schema": {"type": ["DOUBLE"]}},
    "old_status": {"parameterName": "old_status", "schema": {"type": ["STRING"]}}},
  "events": {"output": {"name": "output", "parameters": {}},
             "reject": {"name": "reject", "parameters": {"message": {"type": ["STRING"]}}}},
  "steps": {
    "moved": {"statementName": "moved", "namespace": "System", "name": "If",
      "parameterMap": {"condition": {"c": {"type": "EXPRESSION",
        "expression": "Arguments.old_status = 'shipped' and Arguments.total != Arguments.old_total"}}}},
    "no": {"statementName": "no", "namespace": "System", "name": "GenerateEvent",
      "executeIftrue": {"Steps.moved.true": true},
      "parameterMap": {"eventName": {"e": {"type": "VALUE", "value": "reject"}},
        "results": {"r": {"type": "VALUE", "value": {"name": "message",
          "value": {"isExpression": false, "value": "the total of a shipped order can't change"}}}}}}}}"#;

const SHIPPED_JS: &str = "if (old !== null && old.status === 'shipped' && row.total !== old.total)
  sprig.reject(\"the total of a shipped order can't change\");";

/// KIRun: one audit row per change.
const AUDIT_K: &str = r#"{"namespace": "Shop", "name": "Audit",
  "parameters": {
    "id": {"parameterName": "id", "schema": {"type": ["LONG"]}},
    "qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}},
    "old_qty": {"parameterName": "old_qty", "schema": {"type": ["INTEGER", "NULL"]}},
    "op": {"parameterName": "op", "schema": {"type": ["STRING"]}}},
  "steps": {
    "ins": {"statementName": "ins", "namespace": "Sprig.Db", "name": "Exec",
      "parameterMap": {
        "sql": {"s": {"key": "s", "type": "VALUE",
          "value": "INSERT INTO audit_k (id, op, qty, prev_qty) VALUES (?, ?, ?, ?)"}},
        "args": {
          "a": {"key": "a", "type": "EXPRESSION", "order": 1, "expression": "Arguments.id"},
          "b": {"key": "b", "type": "EXPRESSION", "order": 2, "expression": "Arguments.op"},
          "c": {"key": "c", "type": "EXPRESSION", "order": 3, "expression": "Arguments.qty"},
          "d": {"key": "d", "type": "EXPRESSION", "order": 4, "expression": "Arguments.old_qty"}}}}}}"#;

const AUDIT_JS: &str = "export default function (row, old, op) {
  db.exec('INSERT INTO audit_js (id, op, qty, prev_qty) VALUES (?, ?, ?, ?)',
          [row.id, op, row.qty, old === null ? null : old.qty]);
}";

async fn setup(c: &mut Conn) {
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE products (id BIGINT PRIMARY KEY, price DOUBLE NOT NULL, qty INT NOT NULL)"
            .into(),
        "CREATE TABLE sales_k (id BIGINT PRIMARY KEY, status VARCHAR(16) NOT NULL, \
         total DOUBLE NOT NULL, discount_pct DOUBLE NULL)"
            .into(),
        "CREATE TABLE sales_js (id BIGINT PRIMARY KEY, status VARCHAR(16) NOT NULL, \
         total DOUBLE NOT NULL, discount_pct DOUBLE NULL)"
            .into(),
        "CREATE TABLE book (id BIGINT PRIMARY KEY, status VARCHAR(16) NOT NULL, \
         total DECIMAL(12,2) NOT NULL, discount_pct DECIMAL(5,2) NULL)"
            .into(),
        "CREATE TABLE items_k (id BIGINT PRIMARY KEY, qty INT NOT NULL)".into(),
        "CREATE TABLE items_js (id BIGINT PRIMARY KEY, qty INT NOT NULL)".into(),
        "CREATE TABLE audit_k (id BIGINT NOT NULL, op VARCHAR(8) NOT NULL, qty INT NULL, \
         prev_qty INT NULL, PRIMARY KEY (id, op))"
            .into(),
        "CREATE TABLE audit_js (id BIGINT NOT NULL, op VARCHAR(8) NOT NULL, qty INT NULL, \
         prev_qty INT NULL, PRIMARY KEY (id, op))"
            .into(),
    ] {
        must(c, &q).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn javascript_bound_logic_matches_kirun() {
    let Some(mut c) = connect("javascript_bound_logic_matches_kirun").await else {
        return;
    };
    let started = Instant::now();
    setup(&mut c).await;
    let mut ck = Checks::new();

    // ---- computed columns ------------------------------------------------
    must(
        &mut c,
        &format!("CREATE FUNCTION net_k LANGUAGE kirun AS $${NET_K}$$"),
    )
    .await;
    must(
        &mut c,
        &format!(
            "CREATE FUNCTION net_js(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript AS $${NET_JS}$$"
        ),
    )
    .await;
    must(
        &mut c,
        "CREATE FUNCTION rnd_js(price DOUBLE) RETURNS DOUBLE LANGUAGE javascript AS $$ return price * Math.random(); $$",
    )
    .await;
    for (col, f, kind) in [
        ("c_k", "net_k", "STORED"),
        ("v_k", "net_k", "VIRTUAL"),
        ("c_js", "net_js", "STORED"),
        ("v_js", "net_js", "VIRTUAL"),
    ] {
        let r = sql(
            &mut c,
            &format!("ALTER TABLE products ADD COLUMN {col} DOUBLE AS ({f}(price, qty)) {kind}"),
        )
        .await;
        ck.check(
            &format!("computed column {col} from {f} ({kind})"),
            r.is_ok(),
            format!("{r:?}"),
        );
    }
    let mut vals = Vec::new();
    for i in 1..=24_i64 {
        #[allow(clippy::cast_precision_loss)]
        let price = (i as f64) * 1.25 + 0.1;
        vals.push(format!("({i}, {price}, {})", (i * 7) % 23));
    }
    must(
        &mut c,
        &format!(
            "INSERT INTO products (id, price, qty) VALUES {}",
            vals.join(", ")
        ),
    )
    .await;
    must(&mut c, "UPDATE products SET qty = qty + 9 WHERE id % 3 = 0").await;
    let rows = must(
        &mut c,
        "SELECT id, ROUND(c_k, 9), ROUND(v_k, 9), ROUND(c_js, 9), ROUND(v_js, 9), \
         ROUND(IF(qty >= 10, price * qty * 0.9, price * qty), 9) FROM products ORDER BY id",
    )
    .await;
    let same = rows
        .iter()
        .filter(|r| r[1] == r[2] && r[2] == r[3] && r[3] == r[4] && r[4] == r[5])
        .count();
    ck.check(
        "JavaScript and KIRun computed columns agree, stored and virtual, on every row",
        same == rows.len() && rows.len() == 24,
        format!("{same} of {} rows agree: {rows:?}", rows.len()),
    );
    let branches = must(&mut c, "SELECT SUM(qty >= 10), SUM(qty < 10) FROM products").await;
    ck.check(
        "rows took both branches (10% off from 10 items, and full price)",
        branches[0]
            .iter()
            .all(|v| v.as_deref().is_some_and(|n| n != "0")),
        format!("{branches:?}"),
    );
    for kind in ["STORED", "VIRTUAL"] {
        let r = sql(
            &mut c,
            &format!("ALTER TABLE products ADD COLUMN r DOUBLE AS (rnd_js(price)) {kind}"),
        )
        .await;
        ck.check(
            &format!("a non-deterministic JavaScript function makes no {kind} column (39862)"),
            matches!(&r, Err((39862, m)) if m.contains("js.nondeterministic") && m.contains("Math.random()")),
            format!("{r:?}"),
        );
    }
    // A new version with the same signature: the columns keep theirs.
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION net_js(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript \
         AS $$ return price * qty * 2; $$",
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "replacing a computed column's function, same signature: deployed, the column keeps its expression",
        r.is_ok() && n.contains("products.c_js keeps the expression of version 1"),
        format!("{r:?} / {n}"),
    );
    let after = must(
        &mut c,
        "SELECT COUNT(*) FROM products WHERE ROUND(c_js, 9) = ROUND(c_k, 9) AND ROUND(v_js, 9) = ROUND(v_k, 9)",
    )
    .await;
    ck.check(
        "the columns' values did not change",
        after[0][0].as_deref() == Some("24"),
        format!("{after:?}"),
    );
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION net_js(price DOUBLE) RETURNS DOUBLE LANGUAGE javascript \
         AS $$ return price; $$",
    )
    .await;
    ck.check(
        "replacing it with another signature is refused (39875) naming the columns",
        matches!(&r, Err((39875, m)) if m.contains("products.c_js") && m.contains("products.v_js")),
        format!("{r:?}"),
    );

    // ---- validations ----------------------------------------------------------
    for (q, check) in [
        (
            format!(
                "CREATE VALIDATION disc_k ON sales_k FOR INSERT, UPDATE LANGUAGE kirun AS $${DISCOUNT_K}$$"
            ),
            true,
        ),
        (
            format!(
                "CREATE VALIDATION disc_js ON sales_js FOR INSERT, UPDATE LANGUAGE javascript AS $${DISCOUNT_JS}$$"
            ),
            true,
        ),
        (
            format!(
                "CREATE VALIDATION ship_k ON sales_k FOR UPDATE LANGUAGE kirun AS $${SHIPPED_K}$$"
            ),
            false,
        ),
        (
            format!(
                "CREATE VALIDATION ship_js ON sales_js FOR UPDATE LANGUAGE javascript AS $${SHIPPED_JS}$$"
            ),
            false,
        ),
    ] {
        let r = sql(&mut c, &q).await;
        let n = notes(&mut c).await;
        let want = if check {
            "enforced by CHECK"
        } else {
            "enforced by BEFORE triggers"
        };
        ck.check(
            &format!("{}: deployed, {want}", &q[..40]),
            r.is_ok() && n.contains(want),
            format!("{r:?} / {n}"),
        );
    }
    let script = [
        "INSERT INTO {t} VALUES (1, 'new', 100, 10)",
        "INSERT INTO {t} VALUES (2, 'new', 100, 25)",
        "INSERT INTO {t} VALUES (3, 'shipped', 50, NULL)",
        "INSERT INTO {t} VALUES (4, 'new', 10, 20.5), (5, 'new', 10, 1)",
        "UPDATE {t} SET discount_pct = 30 WHERE id = 1",
        "UPDATE {t} SET total = 60 WHERE id = 3",
        "UPDATE {t} SET status = 'done' WHERE id = 3",
        "UPDATE {t} SET total = 70 WHERE id = 3",
        "UPDATE {t} SET total = 200 WHERE id = 1",
        "UPDATE {t} SET discount_pct = 21 WHERE id IN (1, 3)",
        "UPDATE {t} SET status = 'shipped' WHERE id = 1",
        "UPDATE {t} SET total = total + 1, discount_pct = 5 WHERE id IN (1, 5)",
        "INSERT INTO {t} VALUES (6, 'new', 1, 20)",
    ];
    let mut agree = 0;
    let mut refused = 0;
    for s in script {
        let kirun = sql(&mut c, &s.replace("{t}", "sales_k")).await.map(|_| ());
        let js = sql(&mut c, &s.replace("{t}", "sales_js")).await.map(|_| ());
        if kirun == js {
            agree += 1;
        } else {
            println!("  differ: {s}: kirun {kirun:?}, javascript {js:?}");
        }
        if matches!(kirun, Err((39863, _))) {
            refused += 1;
        }
    }
    ck.check(
        "JavaScript and KIRun validations give the same outcome and message for every write",
        agree == script.len(),
        format!("{agree} of {}", script.len()),
    );
    ck.check(
        "both refused the same 6 writes with 39863",
        refused == 6,
        format!("{refused} refused"),
    );
    let kirun = must(&mut c, "SELECT * FROM sales_k ORDER BY id").await;
    let js = must(&mut c, "SELECT * FROM sales_js ORDER BY id").await;
    ck.check(
        "the two tables hold the same rows",
        kirun == js && kirun.len() == 3,
        format!("{kirun:?} / {js:?}"),
    );
    // The CHECK's failure carries the validation's message.
    let r = sql(&mut c, "INSERT INTO sales_js VALUES (9, 'new', 1, 99)").await;
    ck.check(
        "a JavaScript CHECK refuses with its message",
        matches!(&r, Err((39863, m)) if m.contains("discount above 20%")),
        format!("{r:?}"),
    );

    // A JavaScript validation with a dynamic message.
    let book = "CREATE VALIDATION discount_rules ON book FOR INSERT, UPDATE
LANGUAGE javascript AS $$
export default function (row, old) {
  if (row.discount_pct > 0) {
    const limit = row.total >= 1000 ? 20 : 10;
    if (row.discount_pct > limit)
      sprig.reject(`discount ${row.discount_pct}% is above ${limit}%`);
  }
  if (old !== null && old.status === \"shipped\" && row.total !== old.total)
    sprig.reject(\"the total of a shipped order can't change\");
}
$$";
    let r = sql(&mut c, book).await;
    let n = notes(&mut c).await;
    ck.check(
        "the example JavaScript validation deploys as BEFORE triggers",
        r.is_ok() && n.contains("enforced by BEFORE triggers") && n.contains("tier mariadb"),
        format!("{r:?} / {n}"),
    );
    for (q, want) in [
        (
            "INSERT INTO book VALUES (1, 'new', 600, 25)",
            Some("discount 25% is above 10%"),
        ),
        (
            "INSERT INTO book VALUES (1, 'new', 1600, 25)",
            Some("discount 25% is above 20%"),
        ),
        ("INSERT INTO book VALUES (1, 'new', 1600, 15)", None),
        ("UPDATE book SET status = 'shipped' WHERE id = 1", None),
        (
            "UPDATE book SET total = 1700 WHERE id = 1",
            Some("the total of a shipped order can't change"),
        ),
        (
            "UPDATE book SET discount_pct = 12.5, total = 1600 WHERE id = 1",
            None,
        ),
    ] {
        let r = sql(&mut c, q).await;
        let ok = match want {
            Some(msg) => matches!(&r, Err((39863, got)) if got == msg),
            None => r.is_ok(),
        };
        ck.check(&format!("book: {q}"), ok, format!("{r:?}"));
    }
    // Test runs take the same code with the row by name.
    let r = sql(
        &mut c,
        r#"TEST VALIDATION discount_rules WITH '{"row": {"status": "shipped", "total": 900, "discount_pct": 0}, "old": {"status": "shipped", "total": 800, "discount_pct": 0}, "op": "update"}'"#,
    )
    .await;
    let report = r
        .as_ref()
        .ok()
        .and_then(|rows| rows.first())
        .and_then(|row| row.get(2).cloned().flatten())
        .unwrap_or_default();
    ck.check(
        "TEST VALIDATION runs the JavaScript (row, old) form with the given rows",
        report.contains("39863") && report.contains("shipped order"),
        format!("{r:?}"),
    );

    // ---- after-commit triggers ---------------------------------------------------
    for q in [
        format!(
            "CREATE TRIGGER audit_k_t AFTER COMMIT ON items_k FOR INSERT, UPDATE, DELETE LANGUAGE kirun AS $${AUDIT_K}$$"
        ),
        format!(
            "CREATE TRIGGER audit_js_t AFTER COMMIT ON items_js FOR INSERT, UPDATE, DELETE LANGUAGE javascript AS $${AUDIT_JS}$$"
        ),
    ] {
        let r = sql(&mut c, &q).await;
        let n = notes(&mut c).await;
        ck.check(
            &format!("{}: deployed", &q[..30]),
            r.is_ok() && n.contains("after-commit trigger"),
            format!("{r:?} / {n}"),
        );
    }
    for t in ["items_k", "items_js"] {
        let vals: Vec<String> = (1..=10).map(|i| format!("({i}, {i})")).collect();
        must(
            &mut c,
            &format!("INSERT INTO {t} VALUES {}", vals.join(", ")),
        )
        .await;
        must(
            &mut c,
            &format!("UPDATE {t} SET qty = qty + 100 WHERE id <= 5"),
        )
        .await;
        must(&mut c, &format!("DELETE FROM {t} WHERE id BETWEEN 6 AND 8")).await;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    let (mut kirun, mut js) = (Vec::new(), Vec::new());
    while Instant::now() < deadline {
        kirun = must(
            &mut c,
            "SELECT id, op, qty, prev_qty FROM audit_k ORDER BY id, op",
        )
        .await;
        js = must(
            &mut c,
            "SELECT id, op, qty, prev_qty FROM audit_js ORDER BY id, op",
        )
        .await;
        if kirun.len() >= 18 && js.len() >= 18 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "both triggers wrote one audit row per change (18)",
        kirun.len() == 18 && js.len() == 18,
        format!("kirun {}, javascript {}", kirun.len(), js.len()),
    );
    ck.check(
        "the JavaScript trigger's audit rows equal the KIRun trigger's",
        kirun == js,
        format!("{kirun:?} / {js:?}"),
    );
    let ins = js
        .iter()
        .find(|r| r[0].as_deref() == Some("2") && r[1].as_deref() == Some("insert"));
    let upd = js
        .iter()
        .find(|r| r[0].as_deref() == Some("2") && r[1].as_deref() == Some("update"));
    ck.check(
        "old is null on insert and the row before on update",
        ins.is_some_and(|r| r[3].is_none()) && upd.is_some_and(|r| r[3].as_deref() == Some("2")),
        format!("{ins:?} / {upd:?}"),
    );

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!("took {:.1} s", started.elapsed().as_secs_f64());
    ck.done("JavaScript bound logic");
}
