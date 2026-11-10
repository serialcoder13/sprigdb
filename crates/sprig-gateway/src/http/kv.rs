//! `sprig.v1.KeyValue` (chapter 33, "Interfaces"; chapter 37): `Get`,
//! `Put`, `Delete`, `Scan` and `Exec` on a namespace, over REST
//! (`POST /v1/databases/{db}/namespaces/{ns}:get` ...) and gRPC.
//!
//! A call opens the caller's own executor session, as a Redis login does,
//! so privileges, name virtualization and the router apply. The
//! namespace's tables (and SQL's read-only `<ns>__hash`, `__list`, `__set`
//! and `__zset` views) are created on first use. `Exec` runs one Redis
//! command through `sprig-redis` on the namespace instead of a Redis
//! database, its reply mapped to JSON.

use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use bytes::Bytes;
use serde_json::{Map, Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_kv::api::{self, Entry, Pre};
use sprig_kv::layout::{DistRule, Namespace};
use sprig_proto_redis::Frame;
use sprig_redis::session::{Auth, Login};

use super::sql::{Caller, SqlEngine};

/// The shared state of `Exec` sessions.
fn env() -> &'static Arc<sprig_redis::Env> {
    static ENV: OnceLock<Arc<sprig_redis::Env>> = OnceLock::new();
    ENV.get_or_init(|| Arc::new(sprig_redis::Env::new(sprig_redis::Config::default(), "api")))
}

struct NoAuth;

#[async_trait::async_trait]
impl Auth for NoAuth {
    async fn login(&self, _u: Option<&[u8]>, _p: &[u8]) -> std::result::Result<Login, Frame> {
        Err(Frame::err("the API's Exec is already logged in"))
    }
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

/// The namespace `database`.`name`.
///
/// # Errors
/// 39720 for a name that can't be a namespace.
pub fn namespace(database: &str, name: &str) -> Result<Namespace> {
    if name.is_empty()
        || name.len() > 40
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        || database.is_empty()
    {
        return Err(SprigError::new(
            SprigCode::KvNamespaceNotFound,
            format!(
                "'{database}.{name}' can't be a namespace: names are 1 to 40 letters, digits or underscores"
            ),
        ));
    }
    Ok(Namespace {
        database: database.to_owned(),
        table: name.to_owned(),
        rule: DistRule::Segments(1),
        max_value_bytes: 1 << 20,
    })
}

fn entry_json(e: &Entry) -> Value {
    let mut m = Map::new();
    m.insert(
        "key".into(),
        Value::String(String::from_utf8_lossy(&e.key).into_owned()),
    );
    m.insert("type".into(), Value::String(e.kind.as_str().into()));
    if e.kind == sprig_kv::KeyType::String {
        m.insert(
            "value".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(&e.value)),
        );
        if let Ok(t) = std::str::from_utf8(&e.value) {
            m.insert("text".into(), Value::String(t.to_owned()));
        }
    }
    m.insert("version".into(), Value::String(e.version.to_string()));
    m.insert(
        "expires_at_ms".into(),
        Value::String(e.expires_at_ms.unwrap_or(0).to_string()),
    );
    Value::Object(m)
}

/// A reply as JSON (chapter 33: "RESP3 mapped to JSON").
#[must_use]
pub fn frame_json(f: &Frame) -> Value {
    let text = |b: &[u8]| match std::str::from_utf8(b) {
        Ok(t) => Value::String(t.to_owned()),
        Err(_) => json!({"base64": base64::engine::general_purpose::STANDARD.encode(b)}),
    };
    match f {
        Frame::Simple(b) | Frame::Bulk(b) | Frame::BigNumber(b) | Frame::Verbatim(_, b) => text(b),
        Frame::Error(e) => json!({"error": String::from_utf8_lossy(e)}),
        Frame::Integer(i) => json!(i),
        Frame::Null(_) => Value::Null,
        Frame::Boolean(b) => json!(b),
        Frame::Double(d) => serde_json::Number::from_f64(*d).map_or_else(
            || Value::String(sprig_proto_redis::num::d2string(*d)),
            Value::Number,
        ),
        Frame::DoubleText(t) => std::str::from_utf8(t)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .and_then(serde_json::Number::from_f64)
            .map_or(Value::Null, Value::Number),
        Frame::Array(xs) | Frame::Set(xs) | Frame::Push(xs) => {
            Value::Array(xs.iter().map(frame_json).collect())
        }
        Frame::Map(pairs) => {
            let mut m = Map::new();
            for (k, v) in pairs {
                let key = match frame_json(k) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                m.insert(key, frame_json(v));
            }
            Value::Object(m)
        }
    }
}

fn version_of(v: &Value, k: &str) -> Option<u64> {
    match v.get(k) {
        Some(Value::String(s)) => s.parse().ok(),
        Some(Value::Number(n)) => n.as_u64(),
        _ => None,
    }
}

fn int_of(v: &Value, k: &str) -> i64 {
    match v.get(k) {
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

/// Runs `KeyValue.<rpc>` for `caller` with the request `body` (proto3
/// JSON, path variables merged); the reply as proto3 JSON.
///
/// # Errors
/// The operation's (39720-39731) and the backend's.
#[allow(clippy::too_many_lines)] // one arm per RPC
pub async fn call(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    rpc: &str,
    body: &Value,
) -> Result<Value> {
    let ns = namespace(&s(body, "database"), &s(body, "namespace"))?;
    let mut sql = crate::redis::SqlSession::open(
        &engine.sessions,
        &engine.executor,
        &caller.principal,
        caller.listener,
        caller.generation,
        caller.peer,
        caller.tls.clone(),
    )?;
    let scope = caller
        .principal
        .tenant
        .map_or_else(String::new, |t| t.0.to_string());
    // The namespace's tables, once per gateway.
    let key = format!("{scope}/{}/{}", ns.database, ns.table);
    let env = env();
    if !env.is_ensured(&key) {
        for st in ns.ddl() {
            sprig_kv::SqlRunner::exec(&mut sql, &st)
                .await
                .map_err(|e| {
                    SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", &e.message)
                })?;
        }
        for st in api::view_ddl(&ns) {
            if let Err(e) = sprig_kv::SqlRunner::exec(&mut sql, &st).await {
                tracing::debug!(error = %e, "a key-value view was not created");
            }
        }
        env.set_ensured(&key);
    }
    let key_bytes = s(body, "key").into_bytes();
    match rpc {
        "KeyValue.Get" => {
            let e = api::get(&mut sql, &ns, &key_bytes).await?;
            Ok(match e {
                Some(e) => json!({"entry": entry_json(&e), "found": true}),
                None => json!({"found": false}),
            })
        }
        "KeyValue.Put" => {
            let value = match body.get("value").and_then(Value::as_str) {
                Some(b64) if !b64.is_empty() => base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| {
                        SprigError::new(
                            SprigCode::KvValueInvalid,
                            format!("value isn't base64: {e}"),
                        )
                    })?,
                _ => s(body, "text").into_bytes(),
            };
            let pre = match (
                version_of(body, "if_version"),
                body.get("if_absent").and_then(Value::as_bool),
                body.get("if_exists").and_then(Value::as_bool),
            ) {
                (Some(0), _, _) | (None, Some(true), _) => Pre::Absent,
                (Some(v), _, _) => Pre::Version(v),
                (None, _, Some(true)) => Pre::Exists,
                _ => Pre::None,
            };
            let ttl = int_of(body, "ttl_ms");
            // Chapter 36 (R6 round 3): a location, with its columns.
            let loc = lat_lng(body.get("location"));
            if loc.is_some() {
                ensure_geo(&mut sql, env, &key, &ns).await?;
            }
            let v = api::put_at(
                &mut sql,
                &ns,
                &key_bytes,
                &value,
                pre,
                (ttl > 0).then_some(ttl),
                loc,
            )
            .await?;
            Ok(json!({"version": v.to_string()}))
        }
        "KeyValue.Delete" => {
            let d = api::delete(&mut sql, &ns, &key_bytes, version_of(body, "if_version")).await?;
            Ok(json!({"deleted": d}))
        }
        "KeyValue.Scan" => {
            let limit = usize::try_from(int_of(body, "limit"))
                .ok()
                .filter(|l| *l > 0)
                .unwrap_or(100);
            let (entries, next) = api::scan(
                &mut sql,
                &ns,
                s(body, "prefix").as_bytes(),
                s(body, "page_token").as_bytes(),
                limit,
            )
            .await?;
            Ok(json!({
                "entries": entries.iter().map(entry_json).collect::<Vec<_>>(),
                "next_page_token": next.map(|n| String::from_utf8_lossy(&n).into_owned()).unwrap_or_default(),
            }))
        }
        "KeyValue.Exec" => {
            let cmd: Vec<Bytes> = body
                .get("command")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|x| match x {
                            Value::String(s) => Bytes::from(s.clone()),
                            other => Bytes::from(other.to_string()),
                        })
                        .collect()
                })
                .unwrap_or_default();
            if cmd.is_empty() {
                return Err(SprigError::new(
                    SprigCode::AdminRequestInvalid,
                    "command is empty",
                ));
            }
            let login = Login {
                sql: Box::new(sql),
                scope,
                user: caller.principal.user.clone(),
            };
            let mut sess =
                sprig_redis::Session::with_login(Arc::clone(env), Arc::new(NoAuth), login, ns);
            sess.proto = sprig_proto_redis::Proto::Resp3;
            let mut frames = sess
                .handle(sprig_proto_redis::Command::from_parts(cmd.iter()))
                .await;
            sess.close().await;
            let f = if frames.len() == 1 {
                frames.remove(0)
            } else {
                Frame::Array(frames)
            };
            Ok(json!({"reply": frame_json(&f)}))
        }
        "KeyValue.GeoSearch" => {
            let limit = usize::try_from(int_of(body, "limit"))
                .ok()
                .filter(|l| *l > 0)
                .unwrap_or(100)
                .min(10_000);
            let near = lat_lng(body.get("near"));
            let region = body
                .get("region")
                .filter(|r| r.is_object())
                .map(super::vectors::region_of)
                .transpose()?
                .flatten();
            let hits = match (region, near) {
                (Some(r), n) => sprig_kv::geo::search(&mut sql, &ns, &r, n, limit).await?,
                (None, Some((la, lo))) => {
                    let max = body
                        .get("max_distance_m")
                        .and_then(Value::as_f64)
                        .filter(|m| *m > 0.0);
                    sprig_kv::geo::nearest(&mut sql, &ns, la, lo, limit, max).await?
                }
                (None, None) => {
                    return Err(SprigError::new(
                        SprigCode::GeoInvalid,
                        "GeoSearch needs a region or a point to search near",
                    ));
                }
            };
            Ok(json!({"hits": hits.iter().map(|h| json!({
                "entry": entry_json(&h.entry),
                "location": {"latitude": h.lat, "longitude": h.lng},
                "distance_m": h.distance_m.unwrap_or(0.0),
            })).collect::<Vec<_>>()}))
        }
        other => Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("no key-value call {other}"),
        )),
    }
}

/// `{latitude, longitude}`.
fn lat_lng(v: Option<&Value>) -> Option<(f64, f64)> {
    let v = v?;
    Some((
        v.get("latitude")?.as_f64()?,
        v.get("longitude")?.as_f64()?,
    ))
}

/// The namespace's location columns, once per gateway (chapter 36, R6
/// round 3).
async fn ensure_geo(
    sql: &mut crate::redis::SqlSession,
    env: &sprig_redis::Env,
    key: &str,
    ns: &Namespace,
) -> Result<()> {
    let gkey = format!("{key}#geo");
    if env.is_ensured(&gkey) {
        return Ok(());
    }
    sprig_kv::SqlRunner::exec(sql, &sprig_kv::geo::ddl(ns))
        .await
        .map_err(|e| SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", &e.message))?;
    env.set_ensured(&gkey);
    Ok(())
}
