//! Unit tests of the KIRun front end, the IR, the checks, the generator
//! and the interpreter. Golden files (IR and PL/SQL per fixture) are in
//! `tests/golden/`; `SPRIG_UPDATE_GOLDEN=1` rewrites them.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::needless_pass_by_value
)]

use serde_json::json;

use super::*;
use crate::interp::{Limits, NoDb, Value, run};
use crate::ir::Ty;

const QUOTE: &str = include_str!("../tests/kirun/quote.json");
const ARITH: &str = include_str!("../tests/kirun/arith.json");
const STRINGS: &str = include_str!("../tests/kirun/strings.json");
const BRANCHES: &str = include_str!("../tests/kirun/branches.json");
const LOOPS: &str = include_str!("../tests/kirun/loops.json");
const ORDERS: &str = include_str!("../tests/kirun/orders.json");
const GUARDED: &str = include_str!("../tests/kirun/guarded.json");

fn shop() -> StaticColumns {
    StaticColumns::new()
        .table(
            "products",
            &[("id", Ty::Int), ("price", Ty::Float), ("stock", Ty::Int)],
        )
        .table(
            "orders",
            &[
                ("id", Ty::Int),
                ("product_id", Ty::Int),
                ("qty", Ty::Int),
                ("note", Ty::Str),
            ],
        )
}

fn opts() -> Options {
    Options {
        module_id: 44,
        version: 1,
        ..Options::default()
    }
}

fn ok(src: &str) -> Compiled {
    let c = compile(Runtime::Kirun, src, &shop(), &opts()).unwrap();
    assert!(c.analysis.reasons.is_empty(), "{:#?}", c.analysis.reasons);
    assert!(c.compilable());
    c
}

fn golden(name: &str, c: &Compiled) {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");
    let text = format!(
        "{}\n-- cost {} reads {:?} writes {:?} guarded {}\n\n{}\n",
        c.ir.as_ref().unwrap(),
        c.analysis.cost,
        c.analysis.reads,
        c.analysis.writes,
        c.analysis.guarded_loops,
        c.plsql.as_ref().unwrap().statements.join(";\n\n")
    );
    let path = format!("{dir}/{name}.txt");
    if std::env::var_os("SPRIG_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(&path, &text).unwrap();
        return;
    }
    let want = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("{path} is missing; run with SPRIG_UPDATE_GOLDEN=1"));
    assert_eq!(text, want, "{name} differs from its golden file");
}

fn call(c: &Compiled, args: Vec<Value>) -> (String, serde_json::Value) {
    let o = run(c.ir.as_ref().unwrap(), &args, &mut NoDb, &Limits::default()).unwrap();
    (o.event, o.result)
}

#[test]
fn chapter_example_2_lowers_as_the_book_shows() {
    let c = ok(QUOTE);
    let f = c.ir.as_ref().unwrap();
    assert_eq!(
        f.params.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        ["productId", "qty"]
    );
    assert_eq!(f.params[1].min, Some(1.0));
    assert_eq!(f.events[0].name, "output");
    assert_eq!(c.analysis.reads, vec!["products".to_owned()]);
    assert_eq!(c.analysis.cost, 1);
    let p = c.plsql.as_ref().unwrap();
    assert_eq!(p.entry, "__sprig_l44_v1");
    assert!(
        p.procedure.contains("p_productid IN BIGINT"),
        "{}",
        p.procedure
    );
    assert!(
        p.procedure
            .contains("FROM (SELECT price, stock FROM products WHERE id = (p_productid)) q")
    );
    assert!(
        p.procedure
            .contains("WHEN NO_DATA_FOUND THEN v_item_found := FALSE;")
    );
    assert!(p.procedure.contains("p__event := 'error';"));
    // The package carries only the helpers the procedure uses.
    let spec = p.package_spec.as_ref().unwrap();
    assert!(spec.contains("PROCEDURE fail"));
    assert!(!spec.contains("num_str"));
    golden("quote", &c);
}

#[test]
fn arithmetic_in_the_interpreter() {
    let c = ok(ARITH);
    let (e, r) = call(&c, vec![Value::Int(7), Value::Int(2), Value::Float(0.5)]);
    assert_eq!(e, "output");
    assert_eq!(
        r,
        json!({"sum": 9, "prod": 11, "quot": 3.5, "idiv": 3, "rem": 1, "mix": 5.0, "neg": -3,
               "pick": "a", "safe": 0.5, "big": false, "rounded": 2.0, "maxv": 7.0})
    );
    let (_, r) = call(&c, vec![Value::Int(-7), Value::Int(2), Value::Float(-1.0)]);
    assert_eq!(r["idiv"], json!(-4));
    assert_eq!(r["rem"], json!(-1));
    assert_eq!(r["rounded"], json!(-2.0));
    // `x` may be null, and `x * a` then fails as KIRun's `*` does.
    let e = run(
        c.ir.as_ref().unwrap(),
        &[Value::Int(-7), Value::Int(2), Value::Null],
        &mut NoDb,
        &Limits::default(),
    )
    .unwrap_err();
    assert!(e.message.contains("null value"), "{e}");
    // Division by zero and overflow are runtime errors.
    let e = run(
        c.ir.as_ref().unwrap(),
        &[Value::Int(1), Value::Int(0), Value::Null],
        &mut NoDb,
        &Limits::default(),
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
    let e = run(
        c.ir.as_ref().unwrap(),
        &[Value::Int(i64::MAX), Value::Int(1), Value::Null],
        &mut NoDb,
        &Limits::default(),
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
    golden("arith", &c);
}

#[test]
fn strings_in_the_interpreter() {
    let c = ok(STRINGS);
    let (_, r) = call(&c, vec![Value::Str("Abcz".into()), Value::Int(3)]);
    assert_eq!(
        r,
        json!({"greeting": "Hi Abcz! #3 0.75", "len": 4, "upper": "ABCZ", "lower": "abcz",
               "hasA": false, "startsAb": true, "endsZ": true, "posB": 1, "joined": "Abcz-6",
               "same": false, "before": true})
    );
    // `name` is `STRING`, not nullable: KIRun refuses the call, and so
    // does compiled code.
    let e = run(
        c.ir.as_ref().unwrap(),
        &[Value::Null, Value::Int(8)],
        &mut NoDb,
        &Limits::default(),
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
    assert!(e.message.contains("argument name is null"), "{e}");
    // KIRun's string functions take only text: a number sends the
    // function to the engine tier.
    let numeric = STRINGS.replace("'' + Arguments.n * 2", "Arguments.n * 2");
    let c2 = compile(
        Runtime::Kirun,
        &numeric,
        &StaticColumns::new(),
        &Options::default(),
    )
    .unwrap();
    assert!(
        c2.analysis
            .reasons
            .iter()
            .any(|r| r.code == "kirun.string_types"),
        "{:?}",
        c2.analysis.reasons
    );
    assert!(
        c.plsql
            .as_ref()
            .unwrap()
            .package_spec
            .as_ref()
            .unwrap()
            .contains("num_str")
    );
    golden("strings", &c);
}

#[test]
fn branches_follow_kirun() {
    let c = ok(BRANCHES);
    assert_eq!(
        call(&c, vec![Value::Int(95), Value::Bool(true)]),
        ("output".into(), json!({"grade": "A", "bonus": 100}))
    );
    assert_eq!(
        call(&c, vec![Value::Int(60), Value::Bool(false)]),
        ("output".into(), json!({"grade": "pass", "bonus": 0}))
    );
    assert_eq!(
        call(&c, vec![Value::Null, Value::Bool(true)]),
        ("error".into(), json!({"message": "no score"}))
    );
    // `a` waits for `extra`, which a non-VIP skips: no event at all.
    let e = run(
        c.ir.as_ref().unwrap(),
        &[Value::Int(95), Value::Bool(false)],
        &mut NoDb,
        &Limits::default(),
    )
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
    golden("branches", &c);
}

#[test]
fn loops_follow_kirun() {
    let c = ok(LOOPS);
    let items = Value::Json(json!([{"price": 2.5, "qty": 2}, {"price": 1, "qty": "3"}]));
    let (_, r) = call(&c, vec![Value::Int(10), Value::Int(4), items]);
    // Break lets the rest of the iteration run and stops before the next:
    // the counter has moved past the iteration that broke.
    assert_eq!(
        r,
        json!({"total": 55, "evens": 5, "stoppedAt": 5, "spend": 8.0, "count": 2, "down": 7.5})
    );
    // `n` has a maximum, so the range loop is statically bounded; the
    // for-each loop is guarded.
    assert_eq!(c.analysis.guarded_loops, 1);
    golden("loops", &c);
}

#[test]
fn guarded_loops_stop_at_the_budget() {
    let c = ok(GUARDED);
    assert_eq!(c.analysis.guarded_loops, 1);
    let lim = Limits {
        max_loop_iterations: 10,
    };
    let f = c.ir.as_ref().unwrap();
    let o = run(f, &[Value::Int(10), Value::Int(4)], &mut NoDb, &lim).unwrap();
    assert_eq!(o.result, json!({"steps": 10, "ratio": 2.5}));
    let e = run(f, &[Value::Int(11), Value::Int(4)], &mut NoDb, &lim).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicBudgetExceeded));
    assert!(
        c.plsql
            .as_ref()
            .unwrap()
            .procedure
            .contains("IF v__guard0 > 100000 THEN")
    );
}

#[test]
fn writes_compile() {
    let c = ok(ORDERS);
    assert_eq!(
        c.analysis.writes,
        vec!["orders".to_owned(), "products".to_owned()]
    );
    let p = c.plsql.as_ref().unwrap();
    assert!(p.procedure.contains("v_take_affected := ROW_COUNT();"));
    assert!(
        p.procedure
            .contains("FOR r_list_rows IN (SELECT id, qty, note FROM orders")
    );
    golden("orders", &c);
}

#[test]
fn unsupported_functions_become_reasons() {
    let src = r#"{"name": "Shuffle", "namespace": "T",
      "parameters": {"a": {"parameterName": "a", "schema": {"type": ["ARRAY"]}}},
      "steps": {
        "s": {"statementName": "s", "namespace": "System.Array", "name": "Shuffle",
              "parameterMap": {"source": {"x": {"type": "EXPRESSION", "expression": "Arguments.a"}}}},
        "o": {"statementName": "o", "namespace": "System", "name": "GenerateEvent",
              "parameterMap": {"results": {"r": {"type": "VALUE", "value": {"name": "v",
                 "value": {"isExpression": true, "value": "Steps.s.output.result"}}}}}}}}"#;
    let c = compile(Runtime::Kirun, src, &shop(), &opts()).unwrap();
    let codes: Vec<&str> = c.analysis.reasons.iter().map(|r| r.code.as_str()).collect();
    assert!(codes.contains(&"kirun.function"), "{codes:?}");
    assert!(c.plsql.is_none());
    assert!(!c.compilable());
}

#[test]
fn malformed_definitions_are_syntax_errors() {
    let bad = |src: &str| {
        compile(Runtime::Kirun, src, &shop(), &opts())
            .unwrap_err()
            .code()
    };
    assert_eq!(bad("{"), Some(SprigCode::LogicSyntax));
    // A cycle.
    let cyc = r#"{"name": "C", "steps": {
      "a": {"statementName": "a", "namespace": "System.Math", "name": "Add",
            "parameterMap": {"value": {"x": {"type": "EXPRESSION", "expression": "Steps.b.output.value"}}}},
      "b": {"statementName": "b", "namespace": "System.Math", "name": "Add",
            "parameterMap": {"value": {"x": {"type": "EXPRESSION", "expression": "Steps.a.output.value"}}}}}}"#;
    assert_eq!(bad(cyc), Some(SprigCode::LogicSyntax));
    // An unknown step.
    let unknown = r#"{"name": "U", "steps": {
      "a": {"statementName": "a", "namespace": "System.Math", "name": "Add",
            "dependentStatements": {"Steps.zz.output": true}}}}"#;
    assert_eq!(bad(unknown), Some(SprigCode::LogicSyntax));
    // An expression that doesn't parse.
    let expr = r#"{"name": "E", "steps": {
      "a": {"statementName": "a", "namespace": "System.Math", "name": "Add",
            "parameterMap": {"value": {"x": {"type": "EXPRESSION", "expression": "1 +"}}}}}}"#;
    assert_eq!(bad(expr), Some(SprigCode::LogicSyntax));
    // Placeholders and args disagree.
    let sig = r#"{"name": "S", "steps": {
      "a": {"statementName": "a", "namespace": "Sprig.Db", "name": "QueryOne",
            "parameterMap": {"sql": {"x": {"type": "VALUE", "value": "SELECT stock FROM products WHERE id = ?"}}}}}}"#;
    assert_eq!(bad(sig), Some(SprigCode::LogicSignatureMismatch));
}

#[test]
fn javascript_goes_to_the_engine_tier_for_now() {
    let c = compile(
        Runtime::Javascript,
        "export default () => 1",
        &shop(),
        &opts(),
    )
    .unwrap();
    assert_eq!(c.analysis.reasons[0].code, "js.front_end");
}

#[test]
fn cost_over_the_limit_is_a_reason() {
    let o = Options {
        compile_max_cost: 10,
        ..opts()
    };
    // The for-each loop in ORDERS has none, but LOOPS' guarded loop counts
    // max_loop_iterations; with a data access inside it would be over.
    let src = r#"{"name": "Many", "namespace": "T",
      "parameters": {"n": {"parameterName": "n", "schema": {"type": ["INTEGER"]}}},
      "steps": {
        "l": {"statementName": "l", "namespace": "System.Loop", "name": "CountLoop",
              "parameterMap": {"count": {"x": {"type": "EXPRESSION", "expression": "Arguments.n"}}}},
        "q": {"statementName": "q", "namespace": "Sprig.Db", "name": "QueryOne",
              "dependentStatements": {"Steps.l.iteration": true},
              "parameterMap": {"sql": {"x": {"type": "VALUE", "value": "SELECT stock FROM products WHERE id = ?"}},
                               "args": {"a": {"type": "EXPRESSION", "expression": "Steps.l.iteration.index"}}}}}}"#;
    let c = compile(Runtime::Kirun, src, &shop(), &o).unwrap();
    assert_eq!(c.analysis.cost, 100_000);
    assert_eq!(c.analysis.reasons[0].code, "bounds.cost");
}

#[test]
fn literals_quote_safely() {
    assert_eq!(plsql::quote("it's a \\ test"), "'it''s a \\\\ test'");
}
