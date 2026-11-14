//! `db.publish`, `db.commitOffset` and `db.mq.*` in JavaScript, Lua and
//! KIRun: each is the one statement the host runs.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::json;

use crate::budget::Limits;
use crate::engine::{ParamTy, ScriptSig};
use crate::testutil::{Script, host};

fn ctx_sig() -> ScriptSig {
    ScriptSig {
        params: vec![("ctx".to_owned(), ParamTy::Json)],
        scalar: false,
    }
}

fn shop_host(db: Arc<Script>) -> crate::kirun::Host {
    let mut h = host(db);
    h.default_db = Some("shop".into());
    h
}

const EXPECTED: [&str; 6] = [
    "INSERT INTO `shop`.`order_events` (key, value, headers) VALUES (_utf8mb4 X'37303031', _utf8mb4 X'7B2274797065223A226F726465722E70616964227D', _utf8mb4 X'7B22736F75726365223A22636865636B6F7574227D')",
    "CALL sprig.mq_publish(_utf8mb4 X'6F7264657273', _utf8mb4 X'6F726465722E70616964', _utf8mb4 X'7B226964223A373030317D', _utf8mb4 X'7B7D')",
    "CALL sprig.mq_get(_utf8mb4 X'62696C6C696E67', 2, 5000)",
    "CALL sprig.mq_ack(_utf8mb4 X'71312E746F6B')",
    "CALL sprig.mq_nack(_utf8mb4 X'71312E746F6B', 1)",
    "CALL sprig.commit_offset(_utf8mb4 X'62696C6C696E67', _utf8mb4 X'73686F702E6F726465725F6576656E7473', 3, 1206)",
];

#[tokio::test]
async fn javascript_publishes_and_consumes() {
    let src = r"
export default function (ctx) {
  db.publish('order_events', 7001, {type: 'order.paid'}, {source: 'checkout'});
  db.mq.publish('orders', 'order.paid', {id: 7001});
  db.mq.get('billing', {max: 2, leaseMs: 5000});
  db.mq.ack('q1.tok');
  db.mq.nack('q1.tok');
  db.commitOffset('billing', 'order_events', 3, 1206);
  return { ok: ctx.record.offset };
}
";
    let db = Arc::new(Script::default());
    let p = crate::js::JsProgram::new("t", src, ctx_sig()).unwrap();
    let o = crate::js::run(
        &p,
        vec![json!({"record": {"offset": 1205}})],
        shop_host(Arc::clone(&db)),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(o.result, json!({"ok": 1205}));
    assert_eq!(db.seen(), EXPECTED);
}

#[tokio::test]
async fn lua_publishes_and_consumes() {
    let src = r"
db.publish('order_events', 7001, {type = 'order.paid'}, {source = 'checkout'})
db.mq.publish('orders', 'order.paid', {id = 7001})
db.mq.get('billing', {max = 2, leaseMs = 5000})
db.mq.ack('q1.tok')
db.mq.nack('q1.tok')
db.commitOffset('billing', 'order_events', 3, 1206)
return { ok = ctx.record.offset }
";
    let db = Arc::new(Script::default());
    let p = crate::lua::LuaProgram::new("t", src, ctx_sig()).unwrap();
    let mut h = shop_host(Arc::clone(&db));
    h.info = json!({"record": {"offset": 1205}});
    let o = crate::lua::run(
        &p,
        vec![json!({"record": {"offset": 1205}})],
        h,
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(o.result, json!({"ok": 1205}));
    assert_eq!(db.seen(), EXPECTED);
}

#[tokio::test]
async fn kirun_publishes_and_consumes() {
    let def = r#"{"namespace": "Shop", "name": "OnOrder",
 "parameters": {"offset": {"parameterName": "offset", "schema": {"type": ["LONG"]}}},
 "steps": {
  "pub": {"statementName": "pub", "namespace": "Sprig.Db", "name": "Publish",
    "parameterMap": {
      "stream": {"a": {"key": "a", "type": "VALUE", "value": "order_events"}},
      "key": {"b": {"key": "b", "type": "VALUE", "value": 7001}},
      "value": {"c": {"key": "c", "type": "VALUE", "value": {"type": "order.paid"}}},
      "headers": {"d": {"key": "d", "type": "VALUE", "value": {"source": "checkout"}}}}},
  "mq": {"statementName": "mq", "namespace": "Sprig.Mq", "name": "Publish",
    "dependentStatements": {"Steps.pub.output": true},
    "parameterMap": {
      "exchange": {"a": {"key": "a", "type": "VALUE", "value": "orders"}},
      "routingKey": {"b": {"key": "b", "type": "VALUE", "value": "order.paid"}},
      "body": {"c": {"key": "c", "type": "VALUE", "value": {"id": 7001}}},
      "properties": {"d": {"key": "d", "type": "VALUE", "value": {}}}}},
  "commit": {"statementName": "commit", "namespace": "Sprig.Db", "name": "CommitOffset",
    "dependentStatements": {"Steps.mq.output": true},
    "parameterMap": {
      "group": {"a": {"key": "a", "type": "VALUE", "value": "billing"}},
      "stream": {"b": {"key": "b", "type": "VALUE", "value": "order_events"}},
      "partition": {"c": {"key": "c", "type": "VALUE", "value": 3}},
      "offset": {"d": {"key": "d", "type": "EXPRESSION", "expression": "Arguments.offset + 1"}}}}}}"#;
    let db = Arc::new(Script::default());
    let p = crate::kirun::Prepared::new(def).unwrap();
    assert!(p.missing_functions().await.is_empty());
    let o = crate::kirun::run(
        &p,
        vec![json!(1205)],
        shop_host(Arc::clone(&db)),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(o.event, "output");
    let seen = db.seen();
    assert_eq!(seen.len(), 3, "{seen:?}");
    assert_eq!(seen[0], EXPECTED[0]);
    assert!(
        seen[1].starts_with("CALL sprig.mq_publish(_utf8mb4 X'6F7264657273'"),
        "{}",
        seen[1]
    );
    assert_eq!(seen[2], EXPECTED[5]);
}
