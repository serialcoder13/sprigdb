//! The same functions in three languages (chapter 41, "Testing": the
//! differential corpus "emitted as JavaScript, KIRun and Lua"): each case
//! runs the KIRun definition compiled into MariaDB (the reference), the
//! JavaScript version on QuickJS and the Lua version on the shared Lua
//! runtime (`sprig_logic_js::Program`), each on its own copy of the same
//! tables, and compares the event, its fields (doubles to 1e-9), the error
//! class and every table's rows afterwards. The JavaScript version is also
//! compiled into MariaDB by the JavaScript front end (R7 close-out) and
//! must agree too; further JavaScript-only cases compare the compiled
//! JavaScript with QuickJS.
//!
//! Runs on the Compose single node; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`. Works in `r7_lang_compiled`, `r7_lang_js`,
//! `r7_lang_lua` and `r7_lang_jsc`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, Runtime, StaticColumns, compile, plsql, sql};
use sprig_logic_js::bridge::{self as eb, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};

const COMPILED: &str = "r7_lang_compiled";
const JS: &str = "r7_lang_js";
const LUA: &str = "r7_lang_lua";
/// JavaScript compiled into MariaDB by the JavaScript front end.
const JSC: &str = "r7_lang_jsc";
const SCHEMAS: [&str; 4] = [COMPILED, JS, LUA, JSC];

const SEED: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64))",
    "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0)",
    "INSERT INTO orders VALUES (100, 7, 1, 'first')",
];

fn text(v: &My) -> Option<String> {
    match v {
        My::NULL => None,
        My::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        My::Int(i) => Some(i.to_string()),
        My::UInt(u) => Some(u.to_string()),
        My::Float(f) => Some(f.to_string()),
        My::Double(f) => Some(f.to_string()),
        other => Some(other.as_sql(true)),
    }
}

fn my_err(e: mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => SprigError::backend(s.code, *b"HY000", s.message),
        other => SprigError::internal(other.to_string()),
    }
}

/// The engine's `db`: each statement as text on its own schema.
struct EngineDb {
    pool: Pool,
    schema: &'static str,
}

#[async_trait::async_trait]
impl DbBridge for EngineDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let mut c = self.pool.get_conn().await.map_err(my_err)?;
        c.query_drop(format!("USE `{}`", self.schema))
            .await
            .map_err(my_err)?;
        if op.shape == Shape::Exec {
            c.query_drop(op.sql.as_str()).await.map_err(my_err)?;
            return Ok(Reply::Affected(c.affected_rows()));
        }
        let mut r = c.query_iter(op.sql.as_str()).await.map_err(my_err)?;
        let columns: Vec<Column> = r
            .columns()
            .map(|cs| {
                cs.iter()
                    .map(|col| Column {
                        name: col.name_str().into_owned(),
                        kind: eb::kind_of_type(
                            col.column_type() as u8,
                            col.flags()
                                .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG),
                        ),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let rows: Vec<mysql_async::Row> = r.collect().await.map_err(my_err)?;
        Ok(Reply::Rows {
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.unwrap().iter().map(text).collect())
                .collect(),
        })
    }
}

fn class(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => errno,
    }
}

fn close(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Number(x), J::Number(y)) => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            x == y || (x - y).abs() <= 1e-9 * x.abs().max(y.abs())
        }
        (J::Object(x), J::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| close(v, w)))
        }
        (J::Array(x), J::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| close(v, w))
        }
        _ => a == b,
    }
}

type Got = std::result::Result<(String, J), u16>;

fn same(a: &Got, b: &Got) -> bool {
    match (a, b) {
        (Ok((e1, r1)), Ok((e2, r2))) => e1 == e2 && close(r1, r2),
        (Err(x), Err(y)) => x == y,
        _ => false,
    }
}

/// One function in three languages.
struct Trio {
    name: &'static str,
    kirun: String,
    js: &'static str,
    lua: &'static str,
    sig: ScriptSig,
}

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    cases: usize,
    failures: Vec<String>,
}

impl Suite {
    fn conn(&self, schema: Option<&str>) -> Conn {
        let mut c = self.rt.block_on(self.pool.get_conn()).unwrap();
        if let Some(s) = schema {
            self.rt
                .block_on(c.query_drop(format!("USE `{s}`")))
                .unwrap();
        }
        c
    }

    fn reset(&self) {
        let mut c = self.conn(None);
        for s in SCHEMAS {
            self.rt
                .block_on(async {
                    c.query_drop(format!("USE `{s}`")).await?;
                    c.query_drop("DROP TABLE IF EXISTS products, orders")
                        .await?;
                    for q in SEED {
                        c.query_drop(*q).await?;
                    }
                    Ok::<_, mysql_async::Error>(())
                })
                .unwrap();
        }
    }

    fn columns(&self) -> StaticColumns {
        let mut c = self.conn(None);
        let rows: Vec<(String, String, String, String)> = self
            .rt
            .block_on(c.exec(
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?",
                (COMPILED,),
            ))
            .unwrap();
        let mut cols = StaticColumns::new();
        for (t, col, dt, ct) in rows {
            if let Some(ty) = sql::ty_of_column(&dt, &ct) {
                cols.add(&t, &col, ty);
            }
        }
        cols
    }

    /// Compiles and creates the KIRun version; returns its entry routine.
    fn deploy(&self, t: &Trio, module: u64) -> String {
        let opts = Options {
            module_id: module,
            version: 1,
            ..Options::default()
        };
        let c = compile(Runtime::Kirun, &t.kirun, &self.columns(), &opts).unwrap();
        assert!(
            c.analysis.reasons.is_empty(),
            "{}: {:#?}",
            t.name,
            c.analysis.reasons
        );
        let mut conn = self.conn(Some(COMPILED));
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE)))
            .unwrap();
        let p = c.plsql.unwrap();
        for s in &p.statements {
            self.rt.block_on(conn.query_drop(s.as_str())).unwrap();
        }
        p.entry
    }

    /// Compiles the JavaScript version with the JavaScript front end and
    /// creates it in `JSC`; returns its entry routine.
    fn deploy_js(&self, name: &str, js: &str, sig: &ScriptSig, module: u64) -> String {
        let opts = Options {
            module_id: module,
            version: 1,
            ..Options::default()
        };
        let jsig = sprig_logic_compile::js::Signature {
            params: sig
                .params
                .iter()
                .map(|(n, t)| {
                    (
                        n.clone(),
                        match t {
                            ParamTy::Int => Ty::Int,
                            ParamTy::Float => Ty::Float,
                            ParamTy::Bool => Ty::Bool,
                            ParamTy::Json => Ty::Json,
                            ParamTy::Str => Ty::Str,
                        },
                    )
                })
                .collect(),
            returns: sig.scalar.then_some(Ty::Int),
            row: None,
        };
        let c =
            sprig_logic_compile::js::compile_with(name, js, &jsig, &self.columns(), &opts).unwrap();
        assert!(
            c.compilable(),
            "{name}: the JavaScript doesn't compile: {:#?}",
            c.analysis.reasons
        );
        let mut conn = self.conn(Some(JSC));
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE)))
            .unwrap();
        let p = c.plsql.unwrap();
        for s in &p.statements {
            self.rt.block_on(conn.query_drop(s.as_str())).unwrap();
        }
        p.entry
    }

    fn compiled(&self, entry: &str, args: &[J]) -> Got {
        self.compiled_in(COMPILED, entry, args)
    }

    fn compiled_in(&self, schema: &str, entry: &str, args: &[J]) -> Got {
        let mut conn = self.conn(Some(schema));
        let marks = vec!["?"; args.len()];
        let mut call = format!("CALL {entry}({}", marks.join(", "));
        call.push_str(if args.is_empty() {
            "@e, @r)"
        } else {
            ", @e, @r)"
        });
        let params: Vec<My> = args
            .iter()
            .map(|v| match v {
                J::Null => My::NULL,
                J::Bool(b) => My::Int(i64::from(*b)),
                J::Number(n) => n
                    .as_i64()
                    .map_or_else(|| My::Double(n.as_f64().unwrap()), My::Int),
                J::String(s) => My::Bytes(s.clone().into_bytes()),
                other => My::Bytes(other.to_string().into_bytes()),
            })
            .collect();
        let r: std::result::Result<Option<(Option<String>, Option<String>)>, _> =
            self.rt.block_on(async {
                conn.exec_drop(call.as_str(), params).await?;
                conn.query_first("SELECT @e, @r").await
            });
        match r {
            Ok(Some((Some(e), r))) => Ok((
                e,
                serde_json::from_str(&r.unwrap_or_else(|| "null".into())).unwrap(),
            )),
            Ok(_) => Err(0),
            Err(e) => Err(class(&my_err(e))),
        }
    }

    fn engine(&self, runtime: &str, t: &Trio, schema: &'static str, args: &[J]) -> Got {
        let p = Program::new(
            runtime,
            t.name,
            if runtime == "lua" { t.lua } else { t.js },
            t.sig.clone(),
        )
        .unwrap();
        let host = Host {
            db: Arc::new(EngineDb {
                pool: self.pool.clone(),
                schema,
            }),
            outbound: Arc::new(NoOutbound),
            default_db: Some(schema.to_owned()),
            info: J::Null,
        };
        let args = p.coerce(t.name, args.to_vec()).map_err(|e| class(&e))?;
        self.rt
            .block_on(p.run(args, host, Limits::default()))
            .map(|o| (o.event, o.result))
            .map_err(|e| {
                println!("  {runtime} error: {e}");
                class(&e)
            })
    }

    fn tables(&self, schema: &str) -> Vec<Vec<Option<String>>> {
        let mut conn = self.conn(Some(schema));
        let mut out = Vec::new();
        for q in [
            "SELECT id, price, stock FROM products ORDER BY id",
            "SELECT id, product_id, qty, note FROM orders ORDER BY id",
        ] {
            let rows: Vec<mysql_async::Row> = self.rt.block_on(conn.query(q)).unwrap();
            out.extend(
                rows.into_iter()
                    .map(|r| r.unwrap().iter().map(text).collect::<Vec<_>>()),
            );
        }
        out
    }

    fn case(&mut self, t: &Trio, entry: &str, js_entry: &str, args: &[J]) {
        self.cases += 1;
        self.reset();
        let want = self.compiled(entry, args);
        let js = self.engine("javascript", t, JS, args);
        let lua = self.engine("lua", t, LUA, args);
        let jsc = self.compiled_in(JSC, js_entry, args);
        let label = format!("{}{args:?}", t.name);
        for (lang, got, schema) in [
            ("javascript", &js, JS),
            ("lua", &lua, LUA),
            ("compiled javascript", &jsc, JSC),
        ] {
            if !same(&want, got) {
                self.failures
                    .push(format!("{label}: compiled {want:?} vs {lang} {got:?}"));
            }
            let (a, b) = (self.tables(COMPILED), self.tables(schema));
            if a != b {
                self.failures
                    .push(format!("{label}: {lang} tables differ: {a:?} vs {b:?}"));
            }
        }
        println!("{label}: {want:?}");
    }

    /// A JavaScript-only case: compiled JavaScript against QuickJS.
    fn js_case(&mut self, t: &Trio, js_entry: &str, args: &[J]) {
        self.cases += 1;
        self.reset();
        let want = self.engine("javascript", t, JS, args);
        let got = self.compiled_in(JSC, js_entry, args);
        let label = format!("{}{args:?}", t.name);
        if !same(&want, &got) {
            self.failures.push(format!(
                "{label}: quickjs {want:?} vs compiled javascript {got:?}"
            ));
        }
        let (a, b) = (self.tables(JS), self.tables(JSC));
        if a != b {
            self.failures.push(format!(
                "{label}: compiled javascript tables differ: {a:?} vs {b:?}"
            ));
        }
        println!("{label}: {want:?}");
    }
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/kirun/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

fn sig(params: &[(&str, ParamTy)]) -> ScriptSig {
    ScriptSig {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        scalar: false,
    }
}

const QUOTE_JS: &str = r"
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || !(p.stock >= qty))
    return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}";

const QUOTE_LUA: &str = r"
local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', productId)
if p == sprig.null or not (p.stock >= qty) then
  return sprig.event('error', { message = 'not enough stock' })
end
return { total = p.price * qty + 40 }";

const ORDERS_JS: &str = r#"
const t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', [qty, productId, qty]);
if (t.affected !== 1) return sprig.event('error', { message: 'out of stock for ' + productId });
db.exec("INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, CONCAT('it''s ', ?))",
        [orderId, productId, qty, 'order ' + orderId]);
const left = db.sqlOne('SELECT stock FROM products WHERE id = ?', [productId]);
const orders = db.sql('SELECT id, qty, note FROM orders WHERE product_id = ? ORDER BY id', [productId]);
return { left: left.stock, orders, changed: t.affected };"#;

const ORDERS_LUA: &str = r#"
local t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', qty, productId, qty)
if t.affected ~= 1 then return sprig.event('error', { message = 'out of stock for ' .. productId }) end
db.exec("INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, CONCAT('it''s ', ?))",
        orderId, productId, qty, 'order ' .. orderId)
local left = db.sqlOne('SELECT stock FROM products WHERE id = ?', productId)
local orders = db.sql('SELECT id, qty, note FROM orders WHERE product_id = ? ORDER BY id', productId)
return { left = left.stock, orders = orders, changed = t.affected }"#;

const BRANCHES_JS: &str = r"
if (score === null) return sprig.event('error', { message: 'no score' });
if (score >= 90) return { grade: 'A', bonus: score + 5 };
return { grade: score >= 50 ? 'pass' : 'fail', bonus: 0 };";

const BRANCHES_LUA: &str = r"
if score == sprig.null then return sprig.event('error', { message = 'no score' }) end
if score >= 90 then return { grade = 'A', bonus = score + 5 } end
return { grade = score >= 50 and 'pass' or 'fail', bonus = 0 }";

const TEXT_JS: &str = r"
let s = `hi ${name}`;
if (name !== null && name.startsWith('A')) s = s + '!';
const up = name === null ? 'none' : name.toUpperCase();
return {
  s,
  up,
  len: name === null ? -1 : name.length,
  idx: name === null ? -2 : name.indexOf('b'),
  has: name !== null && name.includes('e'),
  n2: n * 2 + 0.5,
  empty: !name,
  joined: 'n=' + n + '/' + (n > 1),
};";

const ROWS_JS: &str = r"
for (let i = 0; i < 4; i++) {
  if (i >= n) break;
  db.exec('INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, ?)',
          [200 + i, 8, i + 1, i % 2 === 0 ? 'even' : null]);
}
let total = 0;
let notes = '';
let count = 0;
for (const o of db.sql('SELECT qty, note FROM orders WHERE product_id = ? ORDER BY id', [8])) {
  if (count >= limit) break;
  total += o.qty;
  notes = notes + o.note + ';';
  count++;
}
const all = db.sql('SELECT id, qty FROM orders ORDER BY id', []);
return { total, notes, count, all };";

const NULLS_JS: &str = r"
return {
  s: a + b,
  d: a - 1,
  p: a * b,
  q: b / 2,
  lt: a < b,
  m: Math.max(a, 3),
  r: Math.round(b),
  f: Math.floor(b * 10) / 10,
  eq: a === 3,
  ne: a !== null,
  c: a ?? 42,
  o: a || 9,
  pw: 2 ** 3,
  ab: Math.abs(a),
};";

const FACT_JS: &str = r"
export default function (n) {
  let f = 1;
  for (let i = 1; i < 21; i++) {
    if (i > n) break;
    f = f * i;
  }
  return f;
}";

const ERRS_JS: &str = r"
import { sprig } from 'sprig:db';
export default function (x) {
  if (x < 0) throw new Error('negative: ' + x);
  if (x === 0) sprig.reject('zero is not allowed');
  return { half: x / 2, kind: x === null ? 'none' : 'some' };
}";

// R7 follow-ups: `continue` in each kind of loop, and `switch`.
const FLOW_JS: &str = r"
let sum = 0;
for (let i = 0; i < n; i++) {
  if (i % 3 === 0) continue;
  sum += i;
}
let k = 0;
let kept = 0;
while (k < n) {
  k++;
  if (k % 2 === 0) { continue; }
  kept++;
}
let seen = 0;
for (const o of db.sql('SELECT qty, note FROM orders ORDER BY id', [])) {
  if (o.note === null) continue;
  seen += o.qty;
}
let label = 'none';
switch (kind) {
  case 'a':
  case 'b':
    label = 'ab';
    break;
  case 'z':
    return { sum, kept, seen, label: 'zed' };
  default:
    label = 'other';
}
switch (n % 3) {
  case 0: label = label + '/0'; break;
  case 1: label = label + '/1'; break;
}
return { sum, kept, seen, label };";

#[test]
fn kirun_javascript_and_lua_agree() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let opts = OptsBuilder::from_opts(Opts::from_url(&url).unwrap()).pool_opts(
        PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, 4).unwrap())
            .with_inactive_connection_ttl(std::time::Duration::from_secs(30)),
    );
    let mut s = Suite {
        rt,
        pool: Pool::new(opts),
        cases: 0,
        failures: Vec::new(),
    };
    {
        let mut c = s.conn(None);
        for db in SCHEMAS {
            s.rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")))
                .unwrap();
            s.rt.block_on(c.query_drop(format!("CREATE DATABASE `{db}` CHARACTER SET utf8mb4")))
                .unwrap();
        }
    }
    s.reset();
    let start = std::time::Instant::now();
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let i = ParamTy::Int;
        let quote = Trio {
            name: "quote",
            kirun: fixture("quote"),
            js: QUOTE_JS,
            lua: QUOTE_LUA,
            sig: sig(&[("productId", i), ("qty", i)]),
        };
        let e = s.deploy(&quote, 9101);
        let j = s.deploy_js(quote.name, quote.js, &quote.sig, 9201);
        for a in [[7, 3], [7, 1000], [999, 1], [8, 2], [9, 1]] {
            s.case(&quote, &e, &j, &[json!(a[0]), json!(a[1])]);
        }
        let orders = Trio {
            name: "orders",
            kirun: fixture("orders"),
            js: ORDERS_JS,
            lua: ORDERS_LUA,
            sig: sig(&[("orderId", i), ("productId", i), ("qty", i)]),
        };
        let e = s.deploy(&orders, 9102);
        let j = s.deploy_js(orders.name, orders.js, &orders.sig, 9202);
        for a in [[1, 7, 2], [2, 8, 3], [3, 8, 4], [4, 999, 1], [100, 7, 1]] {
            s.case(&orders, &e, &j, &[json!(a[0]), json!(a[1]), json!(a[2])]);
        }
        let branches = Trio {
            name: "branches",
            kirun: fixture("branches"),
            js: BRANCHES_JS,
            lua: BRANCHES_LUA,
            sig: sig(&[("score", i), ("vip", ParamTy::Bool)]),
        };
        let e = s.deploy(&branches, 9103);
        let j = s.deploy_js(branches.name, branches.js, &branches.sig, 9203);
        for a in [
            [json!(95), json!(true)],
            [json!(60), json!(false)],
            [J::Null, json!(true)],
            [json!(40), json!(true)],
        ] {
            s.case(&branches, &e, &j, &a);
        }
        // JavaScript-only cases: compiled JavaScript against QuickJS.
        let js_only = |name: &'static str, js: &'static str, sig: ScriptSig| Trio {
            name,
            kirun: String::new(),
            js,
            lua: "",
            sig,
        };
        let f = ParamTy::Float;
        let text = js_only("text", TEXT_JS, sig(&[("name", ParamTy::Str), ("n", i)]));
        let j = s.deploy_js(text.name, text.js, &text.sig, 9204);
        for a in [
            [json!("Abba"), json!(2)],
            [json!("zeb"), J::Null],
            [J::Null, json!(1)],
            [json!(""), json!(-3)],
        ] {
            s.js_case(&text, &j, &a);
        }
        let rows = js_only("rows", ROWS_JS, sig(&[("n", i), ("limit", i)]));
        let j = s.deploy_js(rows.name, rows.js, &rows.sig, 9205);
        for a in [[3, 10], [4, 2], [0, 5], [2, 0]] {
            s.js_case(&rows, &j, &[json!(a[0]), json!(a[1])]);
        }
        let nulls = js_only("nulls", NULLS_JS, sig(&[("a", i), ("b", f)]));
        let j = s.deploy_js(nulls.name, nulls.js, &nulls.sig, 9206);
        for a in [
            [J::Null, json!(2.5)],
            [json!(3), J::Null],
            [json!(4), json!(4.0)],
            [json!(-7), json!(-2.5)],
        ] {
            s.js_case(&nulls, &j, &a);
        }
        let fact = js_only(
            "fact",
            FACT_JS,
            ScriptSig {
                params: vec![("n".to_owned(), i)],
                scalar: true,
            },
        );
        let j = s.deploy_js(fact.name, fact.js, &fact.sig, 9207);
        for a in [json!(5), json!(0), J::Null, json!(12)] {
            s.js_case(&fact, &j, &[a]);
        }
        let errs = js_only("errs", ERRS_JS, sig(&[("x", i)]));
        let j = s.deploy_js(errs.name, errs.js, &errs.sig, 9208);
        for a in [json!(-1), json!(0), json!(4), J::Null] {
            s.js_case(&errs, &j, &[a]);
        }
        let flow = js_only("flow", FLOW_JS, sig(&[("n", i), ("kind", ParamTy::Str)]));
        let j = s.deploy_js(flow.name, flow.js, &flow.sig, 9209);
        for a in [
            [json!(7), json!("a")],
            [json!(3), json!("z")],
            [json!(0), json!("q")],
            [J::Null, json!("b")],
            [json!(11), J::Null],
        ] {
            s.js_case(&flow, &j, &a);
        }
    }));
    let elapsed = start.elapsed();
    {
        let mut c = s.conn(None);
        for db in SCHEMAS {
            s.rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")))
                .unwrap();
        }
        drop(c);
    }
    s.rt.block_on(s.pool.clone().disconnect()).unwrap();
    if let Err(p) = run {
        std::panic::resume_unwind(p);
    }
    println!(
        "{} cases in {elapsed:?}, {} mismatches",
        s.cases,
        s.failures.len()
    );
    assert!(
        s.failures.is_empty(),
        "mismatches:\n{}",
        s.failures.join("\n")
    );
}
