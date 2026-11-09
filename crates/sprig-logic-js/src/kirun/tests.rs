#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::sync::Mutex;
use std::time::Duration;

use super::*;
use crate::bridge::{ColKind, Column, DbOp, NoDb};
use crate::outbound::NoOutbound;

/// A bridge that records statements and answers from a script.
#[derive(Default)]
struct Script {
    seen: Mutex<Vec<String>>,
    rows: Vec<Vec<Option<String>>>,
    fail: Option<SprigError>,
}

#[async_trait]
impl DbBridge for Script {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        self.seen.lock().unwrap().push(op.sql.clone());
        if let Some(e) = &self.fail {
            return Err(crate::budget::copy(e));
        }
        Ok(match op.shape {
            Shape::Exec => Reply::Affected(1),
            _ => Reply::Rows {
                columns: vec![
                    Column {
                        name: "price".into(),
                        kind: ColKind::Float,
                    },
                    Column {
                        name: "stock".into(),
                        kind: ColKind::Int,
                    },
                ],
                rows: self.rows.clone(),
            },
        })
    }
}

fn host(db: Arc<dyn DbBridge>) -> Host {
    Host {
        db,
        outbound: Arc::new(NoOutbound),
        default_db: None,
        info: serde_json::Value::Null,
    }
}

const QUOTE: &str = r#"{"namespace": "Shop", "name": "Quote",
 "parameters": {
   "productId": {"parameterName": "productId", "schema": {"type": ["LONG"]}},
   "qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"], "minimum": 1}}},
 "events": {"output": {"name": "output", "parameters": {"total": {"type": ["DOUBLE"]}}},
            "error": {"name": "error", "parameters": {"message": {"type": ["STRING"]}}}},
 "steps": {
  "item": {"statementName": "item", "namespace": "Sprig.Db", "name": "QueryOne",
    "parameterMap": {
      "sql": {"s": {"key": "s", "type": "VALUE", "value": "SELECT price, stock FROM products WHERE id = ?"}},
      "args": {"a": {"key": "a", "type": "EXPRESSION", "expression": "Arguments.productId"}}}},
  "enough": {"statementName": "enough", "namespace": "System", "name": "If",
    "parameterMap": {"condition": {"c": {"key": "c", "type": "EXPRESSION",
      "expression": "Steps.item.output.row.stock >= Arguments.qty"}}}},
  "sum": {"statementName": "sum", "namespace": "System.Math", "name": "Add",
    "executeIftrue": {"Steps.enough.true": true},
    "parameterMap": {"value": {
      "v1": {"key": "v1", "type": "EXPRESSION", "order": 1, "expression": "Steps.item.output.row.price * Arguments.qty"},
      "v2": {"key": "v2", "type": "VALUE", "order": 2, "value": 40}}}},
  "done": {"statementName": "done", "namespace": "System", "name": "GenerateEvent",
    "dependentStatements": {"Steps.sum.output": true},
    "parameterMap": {"results": {"r": {"key": "r", "type": "VALUE", "value": {"name": "total",
      "value": {"isExpression": true, "value": "Steps.sum.output.value"}}}}}},
  "short": {"statementName": "short", "namespace": "System", "name": "GenerateEvent",
    "executeIftrue": {"Steps.enough.false": true},
    "parameterMap": {"eventName": {"e": {"key": "e", "type": "VALUE", "value": "error"}},
      "results": {"r": {"key": "r", "type": "VALUE", "value": {"name": "message",
        "value": {"isExpression": false, "value": "not enough stock"}}}}}}}}"#;

#[tokio::test]
async fn the_chapter_example_runs_with_a_bridge() {
    let p = Prepared::new(QUOTE).unwrap();
    assert_eq!(p.params(), ["productId", "qty"]);
    assert!(p.missing_functions().await.is_empty());
    let db = Arc::new(Script {
        rows: vec![vec![Some("400.00".into()), Some("10".into())]],
        ..Script::default()
    });
    let o = run(
        &p,
        vec![json!(7), json!(3)],
        host(db.clone()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(o.event, "output");
    assert_eq!(o.result["total"].as_f64(), Some(1240.0));
    assert_eq!(
        db.seen.lock().unwrap().as_slice(),
        ["SELECT price, stock FROM products WHERE id = 7"]
    );
    let o = run(&p, vec![json!(7), json!(30)], host(db), Limits::default())
        .await
        .unwrap();
    assert_eq!(o.event, "error");
    assert_eq!(o.result, json!({"message": "not enough stock"}));
}

#[tokio::test]
async fn db_errors_come_back_unchanged() {
    let p = Prepared::new(QUOTE).unwrap();
    let db = Arc::new(Script {
        fail: Some(SprigError::backend(
            1142,
            *b"42000",
            "SELECT command denied to user 'u'@'%' for table `shop`.`products`",
        )),
        ..Script::default()
    });
    let e = run(&p, vec![json!(7), json!(3)], host(db), Limits::default())
        .await
        .unwrap_err();
    assert_eq!(number(&e), 1142, "{e}");
    assert!(e.message.contains("SELECT command denied"), "{e}");
}

#[tokio::test]
async fn query_one_with_many_rows_and_wrong_arity() {
    let p = Prepared::new(QUOTE).unwrap();
    let db = Arc::new(Script {
        rows: vec![
            vec![Some("1".into()), Some("1".into())],
            vec![Some("2".into()), Some("2".into())],
        ],
        ..Script::default()
    });
    let e = run(&p, vec![json!(7), json!(3)], host(db), Limits::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
    assert!(e.message.contains("more than one row"), "{e}");
    let e = run(&p, vec![json!(7)], host(Arc::new(NoDb)), Limits::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicSignatureMismatch));
}

fn counter(n: u64) -> String {
    format!(
        r#"{{"namespace": "T", "name": "Spin", "parameters": {{}},
        "events": {{"output": {{"name": "output", "parameters": {{"n": {{"type": ["INTEGER"]}}}}}}}},
        "steps": {{
          "mk": {{"statementName": "mk", "namespace": "System.Context", "name": "Create",
            "parameterMap": {{"name": {{"x": {{"key": "x", "type": "VALUE", "value": "acc"}}}},
              "schema": {{"x": {{"key": "x", "type": "VALUE", "value": {{"type": "STRING", "defaultValue": ""}}}}}}}}}},
          "loop": {{"statementName": "loop", "namespace": "System.Loop", "name": "CountLoop",
            "dependentStatements": {{"Steps.mk.output": true}},
            "parameterMap": {{"count": {{"x": {{"key": "x", "type": "VALUE", "value": {n}}}}}}}}},
          "grow": {{"statementName": "grow", "namespace": "System.Context", "name": "Set",
            "dependentStatements": {{"Steps.loop.iteration": true}},
            "parameterMap": {{"name": {{"x": {{"key": "x", "type": "VALUE", "value": "Context.acc"}}}},
              "value": {{"x": {{"key": "x", "type": "EXPRESSION", "expression": "Context.acc + 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"}}}}}}}},
          "out": {{"statementName": "out", "namespace": "System", "name": "GenerateEvent",
            "dependentStatements": {{"Steps.loop.output": true}},
            "parameterMap": {{"results": {{"r": {{"key": "r", "type": "VALUE", "value": {{"name": "n",
              "value": {{"isExpression": true, "value": "Steps.loop.output.value"}}}}}}}}}}}}}}}}"#
    )
}

#[tokio::test]
async fn budgets_stop_runs_with_clear_errors() {
    let small = Prepared::new(&counter(10)).unwrap();
    let o = run(&small, vec![], host(Arc::new(NoDb)), Limits::default())
        .await
        .unwrap();
    assert_eq!(o.result, json!({"n": 10}));
    assert!(o.steps >= 12, "{o:?}");

    let big = Prepared::new(&counter(5000)).unwrap();
    let cases: Vec<(Limits, &str)> = vec![
        (
            Limits {
                max_steps: 500,
                ..Limits::default()
            },
            "logic.kirun_max_steps = 500",
        ),
        (
            Limits {
                max_loop_iterations: 100,
                ..Limits::default()
            },
            "logic.max_loop_iterations = 100",
        ),
        (
            Limits {
                memory_bytes: 64 << 10,
                ..Limits::default()
            },
            "logic.memory_limit_mb = 0 MB",
        ),
        (
            Limits {
                wall: Duration::from_millis(1),
                ..Limits::default()
            },
            "logic.function_budget_ms = 1 ms",
        ),
    ];
    for (l, want) in cases {
        let e = run(&big, vec![], host(Arc::new(NoDb)), l)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicBudgetExceeded), "{e}");
        assert!(e.message.contains(want), "{want}: {e}");
    }
}

#[tokio::test]
async fn unknown_and_refused_functions_are_listed() {
    let src = r#"{"namespace": "T", "name": "Odd", "parameters": {}, "events": {},
      "steps": {
        "a": {"statementName": "a", "namespace": "System", "name": "Wait", "parameterMap": {}},
        "b": {"statementName": "b", "namespace": "Acme", "name": "Thing", "parameterMap": {}},
        "c": {"statementName": "c", "namespace": "System.Array", "name": "Reverse", "parameterMap": {}},
        "d": {"statementName": "d", "namespace": "Sprig.Db", "name": "Drop", "parameterMap": {}}}}"#;
    let p = Prepared::new(src).unwrap();
    assert_eq!(
        p.missing_functions().await,
        ["System.Wait", "Acme.Thing", "Sprig.Db.Drop"]
    );
}

#[tokio::test]
async fn outbound_calls_are_refused_without_an_allowlist() {
    let src = r#"{"namespace": "T", "name": "Hook", "parameters": {}, "events": {},
      "steps": {
        "f": {"statementName": "f", "namespace": "Sprig.Http", "name": "Fetch",
          "parameterMap": {"url": {"u": {"key": "u", "type": "VALUE", "value": "http://169.254.169.254/latest/meta-data/"}}}}}}"#;
    let p = Prepared::new(src).unwrap();
    let e = run(&p, vec![], host(Arc::new(NoDb)), Limits::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicOutboundDenied), "{e}");
}

#[test]
fn tokens_round_trip() {
    assert_eq!(token("x [sprig#3 1142] denied"), Some((3, 1142)));
    assert_eq!(token("no token"), None);
}
