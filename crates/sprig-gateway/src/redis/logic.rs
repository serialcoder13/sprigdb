//! `FCALL` and `FCALL_RO` of logic-layer functions (chapter 40 step 19;
//! chapter 41, "Calling code"; R7).
//!
//! `sprig-redis` asks this hook when a name isn't one of the tenant's Lua
//! library functions (Lua functions are found first). The call becomes
//! chapter 41's function call statement on the login's own executor
//! session, exactly as `Functions.Call` over HTTP does: the session finds
//! the function (the database a `db.name` names, else the tenant's
//! `redis.logical_database`), checks `EXPOSE (redis)` (39789) and runs
//! `CALL db.name(...)`, so `EXECUTE`, tiers, budgets and transactions are
//! SQL's. `KEYS` bind the leading parameters, then `ARGV`, all as text.
//! `FCALL_RO` runs the call in a read-only transaction, so a write fails
//! whatever the tier; inside `MULTI` it is refused.
//!
//! The reply is the `output` event's value: its one field, else its fields
//! as a map (a flat array in RESP2); `null` is a nil reply; numbers that
//! aren't whole are doubles (bulk strings in RESP2), booleans RESP3
//! booleans (`:1`/`:0` in RESP2). Any other event is
//! `-SPRIG LOGIC_USER_ERROR 39865 <event>: <message>`, as `SELECT f(...)`
//! answers it. SprigDB errors are `-SPRIG <NAME> <code> <message>`.

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sprig_common::SprigCode;
use sprig_kv::{SqlError, SqlRunner};
use sprig_logic::invoke::FunctionCall;
use sprig_proto_redis::Frame;
use sprig_redis::logic::{Fcall, LogicCalls};

/// The gateway's [`LogicCalls`].
#[derive(Debug, Default)]
pub struct GatewayLogic;

#[async_trait]
impl LogicCalls for GatewayLogic {
    async fn fcall(&self, sql: &mut dyn SqlRunner, call: Fcall<'_>) -> Frame {
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(call.keys.len() + call.args.len() + 2);
        argv.push(call.name.to_vec());
        argv.push(call.keys.len().to_string().into_bytes());
        argv.extend(call.keys.iter().map(|k| k.to_vec()));
        argv.extend(call.args.iter().map(|a| a.to_vec()));
        let mut req = match FunctionCall::from_redis_fcall(&argv) {
            Ok(r) => r,
            Err(e) => return sprig_error(e.code().map_or(39874, SprigCode::number), &e.message),
        };
        if req.db.is_none() {
            req.db = Some(call.database.to_owned());
        }
        req.read_only = call.read_only;
        match sql.query(&req.statement()).await {
            Ok(rows) => reply(rows.first().map(Vec::as_slice).unwrap_or_default()),
            Err(e) => error(&e),
        }
    }
}

/// The reply for the call's row: `event`, `result` (JSON text).
fn reply(row: &[Option<Vec<u8>>]) -> Frame {
    let text = |i: usize| {
        row.get(i)
            .and_then(Option::as_deref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
    };
    let event = text(0).unwrap_or_else(|| "output".to_owned());
    let result: Value = text(1)
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null);
    if event != "output" {
        let message = result
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| result.to_string(), str::to_owned);
        return sprig_error(
            SprigCode::LogicUserError.number(),
            &format!("{event}: {message}"),
        );
    }
    match result {
        Value::Object(fields) if fields.len() == 1 => {
            frame(fields.into_iter().next().map_or(Value::Null, |(_, v)| v))
        }
        Value::Object(fields) if fields.is_empty() => Frame::null(),
        other => frame(other),
    }
}

/// A JSON value as a Redis reply.
pub(crate) fn frame(v: Value) -> Frame {
    match v {
        Value::Null => Frame::null(),
        Value::Bool(b) => Frame::Boolean(b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => Frame::Integer(i),
            None => Frame::Double(n.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(s) => Frame::bulk(Bytes::from(s.into_bytes())),
        Value::Array(a) => Frame::Array(a.into_iter().map(frame).collect()),
        Value::Object(o) => Frame::Map(
            o.into_iter()
                .map(|(k, v)| (Frame::bulk(Bytes::from(k.into_bytes())), frame(v)))
                .collect(),
        ),
    }
}

/// `-SPRIG <NAME> <code> <message>` for a SprigDB code.
fn sprig_error(code: u16, message: &str) -> Frame {
    match SprigCode::from_number(code) {
        Some(c) => sprig_redis::reply::sprig(c, message),
        None => Frame::error(format!("SPRIG ERROR {code} {message}")),
    }
}

/// A failed call as Redis shows it.
fn error(e: &SqlError) -> Frame {
    match e.errno {
        // Redis's own text for an unknown function.
        Some(39860) => Frame::err("Function not found"),
        Some(1792) => Frame::err(&format!(
            "Write commands are not allowed from read-only scripts. {}",
            e.message
        )),
        Some(1370) => Frame::error(format!(
            "NOPERM this user has no permissions to run this function: {}",
            e.message
        )),
        Some(n) if (39000..40000).contains(&n) => sprig_error(n, &e.message),
        _ => sprig_redis::reply::backend(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(event: &str, result: &Value) -> Vec<Option<Vec<u8>>> {
        vec![
            Some(event.as_bytes().to_vec()),
            Some(result.to_string().into_bytes()),
        ]
    }

    #[test]
    fn replies() {
        assert_eq!(
            reply(&row("output", &json!({"total": 1240}))),
            Frame::Integer(1240)
        );
        assert_eq!(
            reply(&row("output", &json!({"value": "x"}))),
            Frame::bulk(Bytes::from_static(b"x"))
        );
        assert_eq!(reply(&row("output", &json!({}))), Frame::null());
        assert_eq!(
            reply(&row("output", &json!({"a": 1.5, "b": [true, null]}))),
            Frame::Map(vec![
                (Frame::bulk(Bytes::from_static(b"a")), Frame::Double(1.5)),
                (
                    Frame::bulk(Bytes::from_static(b"b")),
                    Frame::Array(vec![Frame::Boolean(true), Frame::null()])
                ),
            ])
        );
        assert_eq!(
            reply(&row("error", &json!({"message": "no stock"}))),
            Frame::error("SPRIG LOGIC_USER_ERROR 39865 error: no stock")
        );
    }

    #[test]
    fn errors() {
        let e = |n: u16, m: &str| SqlError {
            errno: Some(n),
            message: m.into(),
        };
        assert_eq!(
            error(&e(39860, "no function")),
            Frame::err("Function not found")
        );
        assert_eq!(
            error(&e(39789, "quote isn't exposed to redis")),
            Frame::error("SPRIG FUNCTION_NOT_EXPOSED 39789 quote isn't exposed to redis")
        );
        assert_eq!(
            error(&e(39864, "budget")),
            Frame::error("SPRIG LOGIC_BUDGET_EXCEEDED 39864 budget")
        );
        assert!(matches!(error(&e(1370, "x")), Frame::Error(b) if b.starts_with(b"NOPERM")));
    }
}
