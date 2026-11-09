//! The differential suite (chapter 41, "Testing"; chapter 46): KIRun
//! functions compiled to PL/SQL and run in MariaDB, against the reference
//! interpreter running the same IR, and against the fallback tier running
//! the same definition on the native `kirun` runtime
//! (`sprig_logic_js::kirun`), on three copies of the same data. Each case
//! compares the event, its fields (doubles to 1e-9 relative), the error
//! class, and the rows of every table afterwards.
//!
//! The fallback tier's statements go through [`EngineDb`], which runs
//! them as text on its own schema, bound exactly as the gateway binds
//! them ([`sprig_logic_js::bridge::bind`]); its arguments are coerced to
//! the definition's types as the gateway coerces a `CALL`'s.
//!
//! Runs on the Compose single node; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`). Works in the schemas
//! `r7_diff_compiled`, `r7_diff_interp` and `r7_diff_engine`, dropped
//! afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    missing_docs
)]

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_logic_compile::interp::{self, Db, Limits, Value};
use sprig_logic_compile::{Compiled, Options, Runtime, StaticColumns, compile, plsql, sql};
use sprig_logic_js::bridge::{self as eb, ColKind, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::kirun::{self as engine, Host, Prepared};
use sprig_logic_js::outbound::NoOutbound;

const COMPILED: &str = "r7_diff_compiled";
const INTERP: &str = "r7_diff_interp";
const ENGINE: &str = "r7_diff_engine";
const SCHEMAS: [&str; 3] = [COMPILED, INTERP, ENGINE];

const SEED: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64))",
    "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0)",
    "INSERT INTO orders VALUES (100, 7, 1, 'first')",
];

fn to_my(v: &Value) -> My {
    match v {
        Value::Null => My::NULL,
        Value::Bool(b) => My::Int(i64::from(*b)),
        Value::Int(i) => My::Int(*i),
        Value::Float(x) => My::Double(*x),
        Value::Str(s) => My::Bytes(s.clone().into_bytes()),
        Value::Json(j) => My::Bytes(j.to_string().into_bytes()),
    }
}

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

/// The interpreter's `Db`: one statement at a time on its own schema.
struct MyDb<'a> {
    rt: &'a tokio::runtime::Runtime,
    conn: &'a mut Conn,
}

impl Db for MyDb<'_> {
    fn query(&mut self, sql: &str, binds: &[Value]) -> Result<Vec<Vec<Option<String>>>> {
        let params: Vec<My> = binds.iter().map(to_my).collect();
        let rows: Vec<mysql_async::Row> = self
            .rt
            .block_on(self.conn.exec(sql, params))
            .map_err(my_err)?;
        Ok(rows
            .into_iter()
            .map(|r| r.unwrap().iter().map(text).collect())
            .collect())
    }

    fn exec(&mut self, sql: &str, binds: &[Value]) -> Result<u64> {
        let params: Vec<My> = binds.iter().map(to_my).collect();
        self.rt
            .block_on(async {
                self.conn.exec_drop(sql, params).await?;
                Ok::<_, mysql_async::Error>(self.conn.affected_rows())
            })
            .map_err(my_err)
    }
}

/// The fallback tier's `db`: each statement as text on [`ENGINE`].
struct EngineDb {
    pool: Pool,
}

#[async_trait::async_trait]
impl DbBridge for EngineDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let mut c = self.pool.get_conn().await.map_err(my_err)?;
        c.query_drop(format!("USE `{ENGINE}`"))
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
                        kind: kind_of(col),
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

fn kind_of(col: &mysql_async::Column) -> ColKind {
    let unsigned = col
        .flags()
        .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG);
    eb::kind_of_type(col.column_type() as u8, unsigned)
}

/// A definition deployed in both tiers.
struct Def {
    c: Compiled,
    p: Prepared,
}

/// The error class compared across tiers: MariaDB's 1365 (division by
/// zero) and 1690 (out of range) are 39866, as the gateway maps them.
fn class(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => match errno {
            1365 | 1690 => SprigCode::LogicRuntimeError.number(),
            n => n,
        },
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

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    next_module: u64,
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

    /// The shard schema's columns, from `information_schema`, as the
    /// deploy path reads them.
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

    fn deploy(&mut self, name: &str, src: &str, max_loops: u64) -> Def {
        self.next_module += 1;
        let opts = Options {
            module_id: self.next_module,
            version: 1,
            max_loop_iterations: max_loops,
            ..Options::default()
        };
        let c = compile(Runtime::Kirun, src, &self.columns(), &opts).unwrap();
        assert!(
            c.analysis.reasons.is_empty(),
            "{name}: {:#?}",
            c.analysis.reasons
        );
        let mut conn = self.conn(Some(COMPILED));
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE)))
            .unwrap();
        for s in &c.plsql.as_ref().unwrap().statements {
            if let Err(e) = self.rt.block_on(conn.query_drop(s.as_str())) {
                panic!("{name}: {e}\n{s}");
            }
        }
        let p = Prepared::new(src).unwrap();
        let missing = self.rt.block_on(p.missing_functions());
        assert!(missing.is_empty(), "{name}: {missing:?}");
        Def { c, p }
    }

    fn engine(&self, d: &Def, args: &[Value], max_loops: u64) -> Got {
        let host = Host {
            db: Arc::new(EngineDb {
                pool: self.pool.clone(),
            }),
            outbound: Arc::new(NoOutbound),
            // Every statement is qualified with the schema, as the gateway
            // qualifies them with the code's database.
            default_db: Some(ENGINE.to_owned()),
            info: J::Null,
        };
        let limits = sprig_logic_js::Limits {
            max_loop_iterations: max_loops,
            ..sprig_logic_js::Limits::default()
        };
        let args =
            d.p.coerce(args.iter().map(Value::to_json).collect())
                .map_err(|e| class(&e))?;
        self.rt
            .block_on(engine::run(&d.p, args, host, limits))
            .map(|o| (o.event, o.result))
            .map_err(|e| {
                println!("  fallback error: {e}");
                class(&e)
            })
    }

    fn compiled(&self, c: &Compiled, args: &[Value]) -> Got {
        let p = c.plsql.as_ref().unwrap();
        let mut conn = self.conn(Some(COMPILED));
        let marks = vec!["?"; args.len()];
        let mut call = format!("CALL {}({}", p.entry, marks.join(", "));
        call.push_str(if args.is_empty() {
            "@e, @r)"
        } else {
            ", @e, @r)"
        });
        let params: Vec<My> = args.iter().map(to_my).collect();
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

    fn interpreted(&self, c: &Compiled, args: &[Value], max_loops: u64) -> Got {
        let mut conn = self.conn(Some(INTERP));
        let mut db = MyDb {
            rt: &self.rt,
            conn: &mut conn,
        };
        let lim = Limits {
            max_loop_iterations: max_loops,
        };
        interp::run(c.ir.as_ref().unwrap(), args, &mut db, &lim)
            .map(|o| (o.event, o.result))
            .map_err(|e| class(&e))
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

    fn case(&mut self, name: &str, d: &Def, args: Vec<Value>, max_loops: u64, expect: Option<Got>) {
        let c = &d.c;
        self.cases += 1;
        self.reset();
        let a = self.compiled(c, &args);
        let b = self.interpreted(c, &args, max_loops);
        let e = self.engine(d, &args, max_loops);
        let label = format!("{name}{args:?}");
        if !same(&a, &b) {
            self.failures
                .push(format!("{label}: compiled {a:?} vs interpreted {b:?}"));
        }
        if !same(&a, &e) {
            self.failures
                .push(format!("{label}: compiled {a:?} vs fallback {e:?}"));
        }
        let (ta, tb, te) = (
            self.tables(COMPILED),
            self.tables(INTERP),
            self.tables(ENGINE),
        );
        if ta != tb {
            self.failures
                .push(format!("{label}: tables differ: {ta:?} vs {tb:?}"));
        }
        if ta != te {
            self.failures
                .push(format!("{label}: fallback tables differ: {ta:?} vs {te:?}"));
        }
        if let Some(want) = expect {
            let ok = match (&want, &a) {
                (Ok((e1, r1)), Ok((e2, r2))) => e1 == e2 && close(r1, r2),
                (Err(x), Err(y)) => x == y,
                _ => false,
            };
            if !ok {
                self.failures
                    .push(format!("{label}: expected {want:?}, got {a:?}"));
            }
        }
        println!("{label}: {a:?}");
    }
}

fn same(a: &Got, b: &Got) -> bool {
    match (a, b) {
        (Ok((e1, r1)), Ok((e2, r2))) => e1 == e2 && close(r1, r2),
        (Err(x), Err(y)) => x == y,
        _ => false,
    }
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/kirun/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

#[test]
fn compiled_interpreted_and_fallback_runs_agree() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let opts = OptsBuilder::from_opts(Opts::from_url(&url).unwrap()).pool_opts(
        PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, 4).unwrap())
            .with_inactive_connection_ttl(std::time::Duration::from_secs(30)),
    );
    let pool = Pool::new(opts);
    let mut s = Suite {
        rt,
        pool,
        next_module: 9000,
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
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_cases(&mut s)));
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

fn run_cases(s: &mut Suite) {
    use Value::{Bool as B, Float as F, Int as I, Json as Js, Null as N, Str as S};
    let rt_err = Err(SprigCode::LogicRuntimeError.number());
    let budget = Err(SprigCode::LogicBudgetExceeded.number());
    let big = 100_000;

    let quote = s.deploy("quote", &fixture("quote"), big);
    s.case(
        "quote",
        &quote,
        vec![I(7), I(3)],
        big,
        Some(Ok(("output".into(), json!({"total": 1240})))),
    );
    s.case(
        "quote",
        &quote,
        vec![I(7), I(1000)],
        big,
        Some(Ok(("error".into(), json!({"message": "not enough stock"})))),
    );
    s.case("quote", &quote, vec![I(999), I(1)], big, None);
    s.case("quote", &quote, vec![I(8), I(2)], big, None);

    let arith = s.deploy("arith", &fixture("arith"), big);
    for args in [
        vec![I(7), I(2), F(0.5)],
        vec![I(-7), I(2), N],
        vec![I(100), I(7), F(2.25)],
        vec![I(3), I(-4), F(-1.75)],
        vec![I(i64::MAX), I(1), N],
        vec![I(1), I(0), F(1.5)],
    ] {
        s.case("arith", &arith, args, big, None);
    }
    s.case(
        "arith",
        &arith,
        vec![I(1), I(0), N],
        big,
        Some(rt_err.clone()),
    );

    let strings = s.deploy("strings", &fixture("strings"), big);
    for args in [
        vec![S("Abcz".into()), I(3)],
        vec![N, I(8)],
        vec![S("abc".into()), I(-5)],
        vec![S("ábc€".into()), I(1)],
        vec![S(String::new()), I(0)],
        vec![S("it's \\ \"q\"".into()), I(2)],
    ] {
        s.case("strings", &strings, args, big, None);
    }

    let branches = s.deploy("branches", &fixture("branches"), big);
    for args in [
        vec![I(95), B(true)],
        vec![I(60), B(false)],
        vec![N, B(true)],
        vec![I(40), B(true)],
        vec![I(95), B(false)],
    ] {
        s.case("branches", &branches, args, big, None);
    }

    let loops = s.deploy("loops", &fixture("loops"), big);
    s.case(
        "loops",
        &loops,
        vec![
            I(10),
            I(4),
            Js(json!([{"price": 2.5, "qty": 2}, {"price": 1, "qty": "3"}])),
        ],
        big,
        Some(Ok((
            "output".into(),
            json!({"total": 55, "evens": 5, "stoppedAt": 5, "spend": 8.0, "count": 2, "down": 7.5}),
        ))),
    );
    s.case("loops", &loops, vec![I(0), I(0), Js(json!([]))], big, None);
    s.case(
        "loops",
        &loops,
        vec![I(50), I(2000), Js(json!([{"price": "2", "qty": 1}]))],
        big,
        None,
    );

    let orders = s.deploy("orders", &fixture("orders"), big);
    s.case("orders", &orders, vec![I(1), I(7), I(2)], big, None);
    s.case("orders", &orders, vec![I(2), I(8), I(3)], big, None);
    s.case("orders", &orders, vec![I(3), I(8), I(4)], big, None);
    s.case("orders", &orders, vec![I(4), I(999), I(1)], big, None);
    // Duplicate key: the same server error in both tiers.
    s.case(
        "orders",
        &orders,
        vec![I(100), I(7), I(1)],
        big,
        Some(Err(1062)),
    );

    let guarded = s.deploy("guarded", &fixture("guarded"), 50);
    s.case("guarded", &guarded, vec![I(10), I(4)], 50, None);
    s.case("guarded", &guarded, vec![I(50), I(3)], 50, None);
    s.case("guarded", &guarded, vec![I(51), I(2)], 50, Some(budget));
    s.case(
        "guarded",
        &guarded,
        vec![I(5), I(0)],
        50,
        Some(rt_err.clone()),
    );

    let many = s.deploy(
        "many",
        r#"{"name": "Many", "namespace": "T",
          "parameters": {"min": {"parameterName": "min", "schema": {"type": ["INTEGER"]}}},
          "events": {"output": {"name": "output", "parameters": {"stock": {"type": ["INTEGER"]}, "found": {"type": ["BOOLEAN"]}, "row": {"type": ["OBJECT"]}}}},
          "steps": {
            "q": {"statementName": "q", "namespace": "Sprig.Db", "name": "QueryOne",
                  "parameterMap": {"sql": {"x": {"type": "VALUE", "value": "SELECT stock, price AS cost FROM products WHERE stock >= ?"}},
                                   "args": {"a": {"type": "EXPRESSION", "expression": "Arguments.min"}}}},
            "o": {"statementName": "o", "namespace": "System", "name": "GenerateEvent",
                  "parameterMap": {"results": {
                    "r1": {"type": "VALUE", "order": 1, "value": {"name": "stock", "value": {"isExpression": true, "value": "Steps.q.output.row.stock"}}},
                    "r2": {"type": "VALUE", "order": 2, "value": {"name": "found", "value": {"isExpression": true, "value": "Steps.q.output.found"}}},
                    "r3": {"type": "VALUE", "order": 3, "value": {"name": "row", "value": {"isExpression": true, "value": "Steps.q.output.row"}}}}}}}}"#,
        big,
    );
    s.case("many", &many, vec![I(0)], big, Some(rt_err.clone()));
    s.case(
        "many",
        &many,
        vec![I(5)],
        big,
        Some(Ok((
            "output".into(),
            json!({"stock": 10, "found": true, "row": {"stock": 10, "cost": 400.0}}),
        ))),
    );
    // No row: `Steps.q.output.row.stock` is a value that isn't there,
    // which fails the run in KIRun ("t2"), so it does in compiled code.
    s.case("many", &many, vec![I(1000)], big, Some(rt_err.clone()));

    // KIRun's null arithmetic: `null + y` is `y`, a number plus null
    // fails, and so do the other operators.
    let nulls = s.deploy(
        "nulls",
        r#"{"name": "Nulls", "namespace": "T",
          "parameters": {"x": {"parameterName": "x", "schema": {"type": ["DOUBLE", "NULL"]}},
                         "y": {"parameterName": "y", "schema": {"type": ["INTEGER"]}}},
          "events": {"output": {"name": "output", "parameters": {"a": {"type": ["DOUBLE"]}, "b": {"type": ["DOUBLE"]}}}},
          "steps": {
            "o": {"statementName": "o", "namespace": "System", "name": "GenerateEvent",
                  "parameterMap": {"results": {
                    "r1": {"type": "VALUE", "order": 1, "value": {"name": "a", "value": {"isExpression": true, "value": "Arguments.x + Arguments.y"}}},
                    "r2": {"type": "VALUE", "order": 2, "value": {"name": "b", "value": {"isExpression": true, "value": "-(Arguments.x ?? 2)"}}}}}}}}"#,
        big,
    );
    s.case(
        "nulls",
        &nulls,
        vec![N, I(3)],
        big,
        Some(Ok(("output".into(), json!({"a": 3, "b": -2})))),
    );
    s.case(
        "nulls",
        &nulls,
        vec![F(1.5), I(3)],
        big,
        Some(Ok(("output".into(), json!({"a": 4.5, "b": -1.5})))),
    );
    let plus_null = s.deploy(
        "plus_null",
        r#"{"name": "PlusNull", "namespace": "T",
          "parameters": {"x": {"parameterName": "x", "schema": {"type": ["DOUBLE", "NULL"]}},
                         "y": {"parameterName": "y", "schema": {"type": ["INTEGER"]}}},
          "events": {"output": {"name": "output", "parameters": {"c": {"type": ["DOUBLE"]}}}},
          "steps": {
            "o": {"statementName": "o", "namespace": "System", "name": "GenerateEvent",
                  "parameterMap": {"results": {
                    "r1": {"type": "VALUE", "value": {"name": "c", "value": {"isExpression": true, "value": "Arguments.y + Arguments.x"}}}}}}}}"#,
        big,
    );
    s.case(
        "plus_null",
        &plus_null,
        vec![N, I(3)],
        big,
        Some(rt_err.clone()),
    );
    s.case(
        "plus_null",
        &plus_null,
        vec![F(1.5), I(3)],
        big,
        Some(Ok(("output".into(), json!({"c": 4.5})))),
    );
    // Arguments KIRun's schema refuses: null for a STRING, an INTEGER past
    // Java's int, a qty below its minimum.
    s.case(
        "strings",
        &strings,
        vec![N, I(8)],
        big,
        Some(rt_err.clone()),
    );
    s.case(
        "arith",
        &arith,
        vec![I(3_000_000_000), I(1), N],
        big,
        Some(rt_err.clone()),
    );
    s.case("quote", &quote, vec![I(7), I(0)], big, Some(rt_err));
}
