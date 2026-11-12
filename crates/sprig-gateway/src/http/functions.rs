//! `sprig.v1.Functions` (chapter 37, "Logic functions as endpoints";
//! chapter 41, "Calling code"; R7 order 6): `POST
//! /v1/databases/{db}/functions/{name}:call` and `Functions.Call`.
//!
//! A call is one statement through [`SqlEngine`](super::sql::SqlEngine),
//! as `Sql.Execute` runs one: the gateway session, the caller's
//! principal, `USE {db}`, and the transaction handle if one is given. The
//! statement is a [`FunctionCall`]'s (its prefix and the request as JSON); the
//! tenant session (`exec::logic`) finds the function, checks its exposure
//! (39789), maps named arguments, and runs `CALL db.name(...)` (in its own
//! transaction outside a handle), so `EXECUTE`, tiers, budgets and errors
//! are a SQL `CALL`'s. Its answer is one row, `event` and `result`, which
//! becomes `{"event", "result"}`.

use std::sync::Arc;

use prost_types::value::Kind;
use serde_json::{Value, json};
use sprig_api::proto::v1::{CallRequest, CallResponse};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::invoke::FunctionCall;
use tokio::sync::mpsc;

use super::sql::{CHANNEL_DEPTH, Caller, SqlCall, SqlEngine, collect};

/// Calls `name` in `database` for `caller` over `protocol` (`http` or
/// `grpc`); returns `{"event", "result"}`.
///
/// # Errors
/// 39860 for an unknown function, 39789 when it isn't exposed to
/// `protocol`, 39874 for arguments that don't fit, and the call's own
/// errors (1370, 39864, 39865, ...).
pub async fn call(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    protocol: &str,
    database: String,
    name: &str,
    args: Value,
    transaction: &str,
) -> Result<Value> {
    if name.is_empty() {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            "the call names no function",
        ));
    }
    let text = FunctionCall {
        protocol: protocol.to_owned(),
        name: name.to_owned(),
        args,
        db: None,
        read_only: false,
    }
    .statement();
    let call = SqlCall {
        database,
        sql: text,
        multi: false,
    };
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let max = engine.settings().max_buffered;
    let charge = engine.charge(caller);
    let e2 = Arc::clone(engine);
    let c2 = caller.clone();
    let txn = (!transaction.is_empty()).then(|| transaction.to_owned());
    tokio::spawn(async move { e2.run(&c2, call, txn.as_deref(), tx).await });
    let reply = collect(rx, max, charge).await?;
    let row = reply
        .results
        .first()
        .and_then(|r| r.rows.first())
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::BackendProtocol,
                "the function call returned no row",
            )
        })?;
    let text_at = |i: usize| -> Option<String> {
        row.values.get(i).and_then(|v| match &v.kind {
            Some(Kind::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
    };
    let event = text_at(0).unwrap_or_else(|| "output".to_owned());
    let result = text_at(1)
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .unwrap_or_else(|| json!({}));
    Ok(json!({"event": event, "result": result}))
}

/// `Logic.TestRun` (chapter 41; R7 follow-ups): `body` is the request as
/// `sprig_logic::testrun::TestRun`'s JSON (REST's body, or gRPC's fields
/// with `inputs` spread in); runs as one marked statement in the caller's
/// session, as [`call`] does, and returns the report.
///
/// # Errors
/// 39860, 39861, 39874, 1044 and the setup's errors; the run's own error
/// is in the report.
pub async fn test_run(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    database: String,
    mut body: Value,
) -> Result<Value> {
    if let Value::Object(o) = &mut body {
        o.insert("database".to_owned(), Value::String(database.clone()));
        // Empty strings and version 0 mean "not given" (gRPC's defaults).
        for k in ["kind", "name", "source"] {
            if o.get(k).and_then(Value::as_str) == Some("") {
                o.remove(k);
            }
        }
        if o.get("version").and_then(Value::as_u64) == Some(0) {
            o.remove("version");
        }
    }
    let req: sprig_logic::testrun::TestRun = serde_json::from_value(body).map_err(|e| {
        SprigError::new(
            SprigCode::LogicSignatureMismatch,
            format!("a bad test run request: {e}"),
        )
    })?;
    let call = SqlCall {
        database,
        sql: req.statement(),
        multi: false,
    };
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let max = engine.settings().max_buffered;
    let charge = engine.charge(caller);
    let e2 = Arc::clone(engine);
    let c2 = caller.clone();
    tokio::spawn(async move { e2.run(&c2, call, None, tx).await });
    let reply = collect(rx, max, charge).await?;
    let report = reply
        .results
        .first()
        .and_then(|r| r.rows.first())
        .and_then(|row| row.values.get(2))
        .and_then(|v| match &v.kind {
            Some(Kind::StringValue(s)) => serde_json::from_str::<Value>(s).ok(),
            _ => None,
        })
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::BackendProtocol,
                "the test run returned no report",
            )
        })?;
    Ok(report)
}

/// A `TestRunRequest` as the JSON [`test_run`] takes.
#[must_use]
pub fn test_run_json(r: &sprig_api::proto::v1::TestRunRequest) -> Value {
    let mut v = json!({
        "kind": r.kind, "name": r.name, "version": r.version, "source": r.source,
        "args": r.args.as_ref().map_or(Value::Null, from_proto),
    });
    if let Some(inputs) = &r.inputs
        && let Value::Object(o) = from_proto(&prost_types::Value {
            kind: Some(Kind::StructValue(inputs.clone())),
        })
        && let Value::Object(m) = &mut v
    {
        for (k, x) in o {
            m.entry(k).or_insert(x);
        }
    }
    v
}

/// A report as `TestRunResponse`.
#[must_use]
pub fn test_run_response(report: &Value) -> sprig_api::proto::v1::TestRunResponse {
    let st = |v: &Value| match to_proto(v).kind {
        Some(Kind::StructValue(s)) => Some(s),
        _ => None,
    };
    sprig_api::proto::v1::TestRunResponse {
        event: report["event"].as_str().unwrap_or_default().to_owned(),
        result: st(&report["result"]),
        report: st(report),
    }
}

/// A protobuf `Value` as JSON.
#[must_use]
pub fn from_proto(v: &prost_types::Value) -> Value {
    match &v.kind {
        None | Some(Kind::NullValue(_)) => Value::Null,
        Some(Kind::BoolValue(b)) => Value::Bool(*b),
        Some(Kind::NumberValue(n)) => {
            if n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 {
                #[allow(clippy::cast_possible_truncation)]
                let i = *n as i64;
                Value::from(i)
            } else {
                serde_json::Number::from_f64(*n).map_or(Value::Null, Value::Number)
            }
        }
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::ListValue(l)) => Value::Array(l.values.iter().map(from_proto).collect()),
        Some(Kind::StructValue(s)) => Value::Object(
            s.fields
                .iter()
                .map(|(k, v)| (k.clone(), from_proto(v)))
                .collect(),
        ),
    }
}

/// JSON as a protobuf `Value`.
#[must_use]
pub fn to_proto(v: &Value) -> prost_types::Value {
    let kind = match v {
        Value::Null => Kind::NullValue(0),
        Value::Bool(b) => Kind::BoolValue(*b),
        Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        Value::String(s) => Kind::StringValue(s.clone()),
        Value::Array(a) => Kind::ListValue(prost_types::ListValue {
            values: a.iter().map(to_proto).collect(),
        }),
        Value::Object(o) => Kind::StructValue(prost_types::Struct {
            fields: o.iter().map(|(k, v)| (k.clone(), to_proto(v))).collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

/// A call's JSON reply as `CallResponse`.
#[must_use]
pub fn response(v: &Value) -> CallResponse {
    let result = match to_proto(&v["result"]).kind {
        Some(Kind::StructValue(s)) => Some(s),
        _ => Some(prost_types::Struct::default()),
    };
    CallResponse {
        event: v["event"].as_str().unwrap_or("output").to_owned(),
        result,
    }
}

/// A `CallRequest` as the JSON body REST and forwarding use.
#[must_use]
pub fn request_json(r: &CallRequest) -> Value {
    json!({
        "database": r.database, "name": r.name, "transaction": r.transaction,
        "args": r.args.as_ref().map_or(Value::Null, from_proto),
        "protocol": "grpc",
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn values_round_trip() {
        let v = json!({"a": [1, 2.5, "x", null, true], "b": {"c": -7}});
        assert_eq!(from_proto(&to_proto(&v)), v);
        let r = response(&json!({"event": "error", "result": {"message": "no"}}));
        assert_eq!(r.event, "error");
        assert_eq!(
            from_proto(&prost_types::Value {
                kind: Some(Kind::StructValue(r.result.unwrap()))
            }),
            json!({"message": "no"})
        );
    }
}
