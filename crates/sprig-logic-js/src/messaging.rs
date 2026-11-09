//! Publishing and consuming from logic code (chapter 41, "The `db`
//! interface"; R9): `db.publish`, `db.commitOffset` and `db.mq.publish |
//! get | ack | nack` in JavaScript and Lua, `Sprig.Db.Publish`,
//! `Sprig.Db.CommitOffset` and `Sprig.Mq.Publish | Get | Ack | Nack` in
//! KIRun.
//!
//! Each is one statement the host runs through the caller's session, as
//! every `db` call is, so it is inside the caller's transaction and under
//! the caller's (or the definer's) rights:
//!
//! | Call | Statement |
//! |---|---|
//! | `db.publish(stream, key, value, headers?)` | `INSERT INTO [db.]stream (key, value, headers) VALUES (...)`: an outbox row (chapter 45) |
//! | `db.commitOffset(group, stream, partition, offset)` | `CALL sprig.commit_offset(...)` (chapter 45) |
//! | `db.mq.publish(exchange, routingKey, body, props?)` | `CALL sprig.mq_publish(...)`: an outbox row inside a transaction (chapter 50) |
//! | `db.mq.get(queue, {max, leaseMs})` | `CALL sprig.mq_get(...)` |
//! | `db.mq.ack(token)`, `db.mq.nack(token, requeue)` | `CALL sprig.mq_ack(...)`, `sprig.mq_nack(...)` |
//!
//! A stream name with a dot is `<database>.<stream>`, split at the first
//! dot as Kafka topic names are; without one, the code's own database.
//! Values that aren't strings (objects, arrays, numbers) are sent as their
//! JSON text; `null` is SQL `NULL` (a stream tombstone).

use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};

use crate::bridge::{self, Column, DbOp, Reply, Shape, literal};

/// Which messaging call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgOp {
    /// `db.publish`.
    Publish,
    /// `db.commitOffset`.
    CommitOffset,
    /// `db.mq.publish`.
    MqPublish,
    /// `db.mq.get`.
    MqGet,
    /// `db.mq.ack`.
    MqAck,
    /// `db.mq.nack`.
    MqNack,
}

impl MsgOp {
    /// The JavaScript name, for messages.
    #[must_use]
    pub const fn js_name(self) -> &'static str {
        match self {
            MsgOp::Publish => "db.publish",
            MsgOp::CommitOffset => "db.commitOffset",
            MsgOp::MqPublish => "db.mq.publish",
            MsgOp::MqGet => "db.mq.get",
            MsgOp::MqAck => "db.mq.ack",
            MsgOp::MqNack => "db.mq.nack",
        }
    }

    /// The KIRun function's full name.
    #[must_use]
    pub const fn kirun_name(self) -> &'static str {
        match self {
            MsgOp::Publish => "Sprig.Db.Publish",
            MsgOp::CommitOffset => "Sprig.Db.CommitOffset",
            MsgOp::MqPublish => "Sprig.Mq.Publish",
            MsgOp::MqGet => "Sprig.Mq.Get",
            MsgOp::MqAck => "Sprig.Mq.Ack",
            MsgOp::MqNack => "Sprig.Mq.Nack",
        }
    }

    /// Every operation.
    pub const ALL: [MsgOp; 6] = [
        MsgOp::Publish,
        MsgOp::CommitOffset,
        MsgOp::MqPublish,
        MsgOp::MqGet,
        MsgOp::MqAck,
        MsgOp::MqNack,
    ];

    /// The KIRun parameter names, in call order.
    #[must_use]
    pub const fn params(self) -> &'static [&'static str] {
        match self {
            MsgOp::Publish => &["stream", "key", "value", "headers"],
            MsgOp::CommitOffset => &["group", "stream", "partition", "offset"],
            MsgOp::MqPublish => &["exchange", "routingKey", "body", "properties"],
            MsgOp::MqGet => &["queue", "max", "leaseMs"],
            MsgOp::MqAck => &["token"],
            MsgOp::MqNack => &["token", "requeue"],
        }
    }

    /// Whether the call writes (an engine-tier call in autocommit then
    /// opens its own transaction first).
    #[must_use]
    pub const fn writes(self) -> bool {
        !matches!(self, MsgOp::MqGet)
    }
}

fn bad(op: MsgOp, m: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::LogicRuntimeError,
        format!("{}: {m}", op.js_name()),
    )
}

/// A name part as a quoted identifier.
fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// `value` as the text a SQL column gets: strings as they are, `null` as
/// `NULL`, anything else as its JSON text.
fn text_of(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::String(_) => v.clone(),
        other => Value::String(other.to_string()),
    }
}

/// The target of a publish: `[db.]stream`, the code's database when the
/// name has no dot.
///
/// # Errors
/// 39866 for an empty name.
pub fn stream_target(name: &str, default_db: Option<&str>) -> Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(bad(MsgOp::Publish, "the stream name is empty"));
    }
    Ok(match (name.split_once('.'), default_db) {
        (Some((db, s)), _) if !db.is_empty() && !s.is_empty() => {
            format!("{}.{}", ident(db), ident(s))
        }
        (_, Some(db)) => format!("{}.{}", ident(db), ident(name)),
        (_, None) => ident(name),
    })
}

/// The statement of one call; `args` in [`MsgOp::params`] order (missing
/// ones are `null`).
///
/// # Errors
/// 39866 for arguments of the wrong kind.
pub fn op(op: MsgOp, args: &[Value], default_db: Option<&str>) -> Result<DbOp> {
    let arg = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
    let need_text = |i: usize, what: &str| -> Result<String> {
        match arg(i) {
            Value::String(s) if !s.is_empty() => Ok(s),
            Value::Number(n) => Ok(n.to_string()),
            _ => Err(bad(op, format!("{what} must be a non-empty string"))),
        }
    };
    let need_int = |i: usize, what: &str| -> Result<u64> {
        match arg(i) {
            Value::Number(n) if n.as_u64().is_some() => Ok(n.as_u64().unwrap_or(0)),
            Value::Number(n) if n.as_f64().is_some_and(|f| f >= 0.0 && f.fract() == 0.0) => {
                Ok(n.as_f64().unwrap_or(0.0) as u64)
            }
            Value::String(s) if s.trim().parse::<u64>().is_ok() => {
                Ok(s.trim().parse::<u64>().unwrap_or(0))
            }
            _ => Err(bad(op, format!("{what} must be a whole number at least 0"))),
        }
    };
    let sql = match op {
        MsgOp::Publish => {
            let target = stream_target(&need_text(0, "the stream")?, default_db)?;
            let headers = match arg(3) {
                Value::Null => Value::Null,
                Value::Object(o) => Value::String(Value::Object(o).to_string()),
                _ => return Err(bad(op, "headers must be an object of strings")),
            };
            format!(
                "INSERT INTO {target} (key, value, headers) VALUES ({}, {}, {})",
                literal(&text_of(&arg(1))),
                literal(&text_of(&arg(2))),
                literal(&headers)
            )
        }
        MsgOp::CommitOffset => {
            let group = need_text(0, "the group")?;
            let stream = need_text(1, "the stream")?;
            let stream = match (stream.contains('.'), default_db) {
                (false, Some(db)) => format!("{db}.{stream}"),
                _ => stream,
            };
            format!(
                "CALL sprig.commit_offset({}, {}, {}, {})",
                literal(&Value::String(group)),
                literal(&Value::String(stream)),
                need_int(2, "the partition")?,
                need_int(3, "the offset")?
            )
        }
        MsgOp::MqPublish => {
            let exchange = match arg(0) {
                Value::Null => String::new(),
                Value::String(s) => s,
                _ => return Err(bad(op, "the exchange must be a string ('' is the default)")),
            };
            let key = match arg(1) {
                Value::Null => String::new(),
                Value::String(s) => s,
                other => other.to_string(),
            };
            let props = match arg(3) {
                Value::Null => Value::String("{}".to_owned()),
                Value::Object(o) => Value::String(Value::Object(o).to_string()),
                _ => return Err(bad(op, "properties must be an object")),
            };
            let body = match text_of(&arg(2)) {
                Value::Null => Value::String(String::new()),
                v => v,
            };
            format!(
                "CALL sprig.mq_publish({}, {}, {}, {})",
                literal(&Value::String(exchange)),
                literal(&Value::String(key)),
                literal(&body),
                literal(&props)
            )
        }
        MsgOp::MqGet => {
            let queue = need_text(0, "the queue")?;
            let max = match arg(1) {
                Value::Null => 1,
                _ => need_int(1, "max")?.clamp(1, 1000),
            };
            let lease = match arg(2) {
                Value::Null => 30_000,
                _ => need_int(2, "leaseMs")?.max(1),
            };
            format!(
                "CALL sprig.mq_get({}, {max}, {lease})",
                literal(&Value::String(queue))
            )
        }
        MsgOp::MqAck => format!(
            "CALL sprig.mq_ack({})",
            literal(&Value::String(need_text(0, "the delivery token")?))
        ),
        MsgOp::MqNack => {
            let requeue = !matches!(arg(1), Value::Bool(false))
                && !matches!(&arg(1), Value::Number(n) if n.as_f64() == Some(0.0));
            format!(
                "CALL sprig.mq_nack({}, {})",
                literal(&Value::String(need_text(0, "the delivery token")?)),
                u8::from(requeue)
            )
        }
    };
    Ok(DbOp {
        shape: if op == MsgOp::Publish {
            Shape::Exec
        } else {
            Shape::Sprig
        },
        sql,
    })
}

/// `db.mq.get`'s `opts` (`{max, leaseMs}`) as positional arguments after
/// the queue.
#[must_use]
pub fn get_args(queue: Value, opts: &Value) -> Vec<Value> {
    vec![
        queue,
        opts.get("max").cloned().unwrap_or(Value::Null),
        opts.get("leaseMs")
            .or_else(|| opts.get("lease_ms"))
            .cloned()
            .unwrap_or(Value::Null),
    ]
}

fn cell<'a>(columns: &[Column], row: &'a [Option<String>], name: &str) -> Option<&'a str> {
    columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .and_then(|i| row.get(i))
        .and_then(Option::as_deref)
}

/// What the code gets back: `db.publish` `{affected}`, `db.mq.publish` the
/// number of queues reached, `db.mq.get` an array of messages
/// `{deliveryToken, body, properties, routingKey, exchange, redelivered,
/// deliveryCount}`, the others `null`.
///
/// # Errors
/// 39866 for a reply of the wrong kind.
pub fn value(op: MsgOp, reply: Reply) -> Result<Value> {
    match (op, reply) {
        (MsgOp::Publish, Reply::Affected(n)) => Ok(json!({ "affected": n })),
        (MsgOp::MqPublish, Reply::Rows { columns, rows }) => Ok(json!(
            rows.first()
                .and_then(|r| cell(&columns, r, "routed"))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        )),
        (MsgOp::MqGet, Reply::Rows { columns, rows }) => Ok(Value::Array(
            rows.iter()
                .map(|r| {
                    let props = cell(&columns, r, "properties")
                        .and_then(|p| serde_json::from_str(p).ok())
                        .unwrap_or_else(|| json!({}));
                    json!({
                        "deliveryToken": cell(&columns, r, "delivery_token"),
                        "body": cell(&columns, r, "body"),
                        "properties": props,
                        "routingKey": cell(&columns, r, "routing_key"),
                        "exchange": cell(&columns, r, "exchange"),
                        "redelivered": cell(&columns, r, "redelivered") == Some("1"),
                        "deliveryCount": cell(&columns, r, "delivery_count")
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0),
                    })
                })
                .collect(),
        )),
        (MsgOp::Publish | MsgOp::MqPublish | MsgOp::MqGet, _) => {
            Err(bad(op, "the statement returned the wrong kind of result"))
        }
        _ => Ok(Value::Null),
    }
}

/// Whether a statement's first word is `CALL` and it names `sprig.`: the
/// shape [`Shape::Sprig`] accepts.
#[must_use]
pub fn is_sprig_call(sql: &str) -> bool {
    let t = sql.trim_start();
    if !t.get(..4).is_some_and(|w| w.eq_ignore_ascii_case("call")) {
        return false;
    }
    let name: String = t[4..]
        .chars()
        .take_while(|c| *c != '(')
        .filter(|c| !c.is_whitespace() && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase();
    name.starts_with("sprig.")
}

/// A run of `op` through `db` (the KIRun and script hosts share it).
///
/// # Errors
/// As [`op`] and [`value`]; the statement's own error.
pub async fn call(
    db: &dyn bridge::DbBridge,
    o: MsgOp,
    args: &[Value],
    default_db: Option<&str>,
) -> Result<Value> {
    let stmt = op(o, args, default_db)?;
    let reply = db.exec(stmt).await?;
    value(o, reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_statements() {
        let s = op(
            MsgOp::Publish,
            &[
                json!("user_events"),
                json!(901),
                json!({"type": "user.added"}),
                json!({"source": "sec"}),
            ],
            Some("security"),
        )
        .unwrap();
        assert_eq!(s.shape, Shape::Exec);
        assert!(
            s.sql.starts_with("INSERT INTO `security`.`user_events` (key, value, headers) VALUES (_utf8mb4 X'393031', _utf8mb4 X'"),
            "{}",
            s.sql
        );
        let s = op(
            MsgOp::Publish,
            &[json!("shop.orders"), Value::Null, Value::Null],
            None,
        )
        .unwrap();
        assert_eq!(
            s.sql,
            "INSERT INTO `shop`.`orders` (key, value, headers) VALUES (NULL, NULL, NULL)"
        );
        assert!(op(MsgOp::Publish, &[json!("")], None).is_err());
        assert!(
            op(
                MsgOp::Publish,
                &[json!("s"), json!(1), json!(2), json!([1])],
                None
            )
            .is_err()
        );
    }

    #[test]
    fn queue_and_offset_statements() {
        let s = op(
            MsgOp::MqGet,
            &get_args(json!("billing"), &json!({"max": 5})),
            None,
        )
        .unwrap();
        assert_eq!(s.shape, Shape::Sprig);
        assert_eq!(
            s.sql,
            "CALL sprig.mq_get(_utf8mb4 X'62696C6C696E67', 5, 30000)"
        );
        let s = op(MsgOp::MqNack, &[json!("q1.x"), json!(false)], None).unwrap();
        assert!(s.sql.ends_with(", 0)"));
        let s = op(
            MsgOp::CommitOffset,
            &[json!("g"), json!("events"), json!(3), json!(1206)],
            Some("shop"),
        )
        .unwrap();
        assert_eq!(
            s.sql,
            "CALL sprig.commit_offset(_utf8mb4 X'67', _utf8mb4 X'73686F702E6576656E7473', 3, 1206)"
        );
        assert!(
            op(
                MsgOp::CommitOffset,
                &[json!("g"), json!("e"), json!(-1), json!(1)],
                None
            )
            .is_err()
        );
        assert!(is_sprig_call("CALL sprig.mq_ack('x')"));
        assert!(is_sprig_call(" call `sprig`.mq_ack('x')"));
        assert!(!is_sprig_call("CALL shop.quote(1)"));
    }

    #[test]
    fn replies() {
        let cols = |names: &[&str]| -> Vec<Column> {
            names
                .iter()
                .map(|n| Column {
                    name: (*n).to_owned(),
                    kind: bridge::ColKind::Str,
                })
                .collect()
        };
        let v = value(
            MsgOp::MqPublish,
            Reply::Rows {
                columns: cols(&["routed"]),
                rows: vec![vec![Some("2".into())]],
            },
        )
        .unwrap();
        assert_eq!(v, json!(2));
        let v = value(
            MsgOp::MqGet,
            Reply::Rows {
                columns: cols(&[
                    "delivery_token",
                    "body",
                    "properties",
                    "routing_key",
                    "exchange",
                    "redelivered",
                    "delivery_count",
                ]),
                rows: vec![vec![
                    Some("q1.t".into()),
                    Some("hi".into()),
                    Some("{\"message_id\":\"m\"}".into()),
                    Some("k".into()),
                    Some("x".into()),
                    Some("1".into()),
                    Some("2".into()),
                ]],
            },
        )
        .unwrap();
        assert_eq!(v[0]["deliveryToken"], "q1.t");
        assert_eq!(v[0]["properties"]["message_id"], "m");
        assert_eq!(v[0]["redelivered"], true);
        assert_eq!(v[0]["deliveryCount"], 2);
        assert_eq!(
            value(MsgOp::MqAck, Reply::Affected(1)).unwrap(),
            Value::Null
        );
    }
}
