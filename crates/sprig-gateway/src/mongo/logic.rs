//! `db.runCommand({sprigCall: "quote", args: [7, 3]})`: logic-layer
//! functions from MongoDB drivers.
//!
//! The command becomes the logic layer's function call statement on the
//! command's executor session (the connection's own, or the transaction's
//! for a command inside `startTransaction`), exactly as `Functions.Call`
//! over HTTP: the session finds the function in the command's database
//! (`$db`), checks `EXPOSE (mongo)` (39789), maps `args` (a list by
//! position, a document by parameter name, one value) to the signature and
//! runs `CALL db.name(...)`, so `EXECUTE`, tiers, budgets and the
//! transaction are SQL's. Inside a transaction the call joins it; outside,
//! it runs in its own.
//!
//! The reply is `{event, result, ok: 1}`, as HTTP's `{event, result}`.
//! Errors keep their SprigDB codes, except that a validation's 39863 is
//! MongoDB's 121 `DocumentValidationFailure`.

use bson::{Bson, Document, doc};
use serde_json::Value;
use sprig_doc::engine::SqlRunner;
use sprig_logic::invoke::FunctionCall;
use sprig_proto_mongo::errors::MongoError;

use super::sql::SqlSession;

/// Runs `sprigCall` in `db`.
///
/// # Errors
/// 39860 for an unknown function, 39789 when it isn't exposed to `mongo`,
/// 39874 for arguments that don't fit, and the call's own errors.
pub async fn sprig_call(
    sql: &mut SqlSession,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let mut cmd = serde_json::Map::new();
    for (k, v) in body {
        if k == "sprigCall" || k == "args" {
            cmd.insert(k.clone(), v.clone().into_relaxed_extjson());
        }
    }
    let mut req = FunctionCall::from_mongo(&Value::Object(cmd))
        .map_err(|e| MongoError::new(code_of(&e), e.message))?;
    req.db = Some(db.to_owned());
    let rows = sql
        .query(&req.statement())
        .await
        .map_err(|e| mongo_error(&e))?;
    let row = rows.first();
    let text = |i: usize| {
        row.and_then(|r| r.get(i))
            .and_then(Option::as_deref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
    };
    let event = text(0).unwrap_or_else(|| "output".to_owned());
    let result: Value = text(1)
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let result = Bson::try_from(result)
        .map_err(|e| MongoError::new(1, format!("the function's result: {e}")))?;
    Ok(doc! {"event": event, "result": result, "ok": 1.0})
}

/// A SprigDB error's number as a MongoDB code.
fn code_of(e: &sprig_common::SprigError) -> i32 {
    e.code().map_or(1, |c| i32::from(c.number()))
}

/// A SQL error as the driver sees it (39863 is MongoDB's 121).
fn mongo_error(e: &sprig_doc::engine::SqlError) -> MongoError {
    if e.errno == Some(39863) {
        return MongoError::new(121, e.message.clone());
    }
    sprig_doc::engine::mongo_error(e)
}
