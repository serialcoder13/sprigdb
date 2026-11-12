//! The stream, queue and sink views' routes (chapter 44 steps 28–31;
//! chapter 37's `Streams.ListGroups`, the `Mq` topology and the `Sinks`
//! dead letters, Studio part; decision ST44), for the caller's own tenant.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/databases/{db}/streams/{name}/groups` | `Streams.ListGroups`: per partition the log start and high watermark (read as the caller, so `SELECT` on the stream is checked), and every consumer group with its committed offset, lag, generation and state (`stream_groups`, `stream_offsets`) |
//! | `GET /v1/databases/{db}/mq` | The database's virtual host (`mq_vhosts`, else the database's own name once it has topology), exchanges, queues and bindings, with names (`mq_exchanges`, `mq_queues`, `mq_bindings`) |
//! | `GET /v1/databases/{db}/sinks/{name}/deadLetters` | The sink's dead letters (`sink_dead_letters`) |
//! | `POST /v1/databases/{db}/sinks/{name}/deadLetters:replay`, `:discard` | `{ids}` or `{all: true}`: marked `replaying` (the sink's task feeds them back) or `discarded` |
//! | `POST /v1/databases/{db}/sinks/{name}:test` | `Sinks.Test` (R9, r9last): `{sample}` (10 by default): `TEST SINK db.name WITH SAMPLE n` as the caller; `{columns, rows}` |
//! | `POST /v1/databases/{db}/connections/{name}:test` | `Connections.Test` (R9, r9last): `{platform, write, to}`: `TEST [PLATFORM] CONNECTION [db.]name [WRITE [TO 'to']]` as the caller; `{columns, rows}` |
//!
//! Everything else the views show comes from the statements a SQL client
//! runs, which the gateway checks as for any client: `SHOW STREAMS FROM`,
//! `SELECT ... FROM <stream>`, `SHOW SINKS FROM`, `SHOW SINK STATUS`,
//! `SHOW CREATE SINK`, `ALTER SINK ... PAUSE | RESUME | RESET TO`,
//! `SHOW INHERITED DEFINITIONS | INSTANCES | OVERRIDES`, `ALTER INHERITED`.
//!
//! Who may see what: the topology and the consumer groups need a grant on
//! the database (or `*.*`, or `SPRIG_TENANT_ADMIN`), as the workload views
//! decide; watermarks need `SELECT` on the stream (the read runs as the
//! caller). Dead letters hold record data, so they need
//! `SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN`, or being the sink's owner
//! (1227 otherwise).

use std::collections::BTreeMap;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::logic::access_of;
use super::{hdr, invalid, json_response, parse_json, read_body, rows_of, run_sql};

/// Dead letters listed at most.
const MAX_DEAD: u32 = 1000;

/// A committed offset: group, partition, offset, when, generation, state.
type Off = (String, u32, u64, String, Option<u32>, Option<String>);
/// An exchange row: id, name, type, durable, auto-delete, internal, arguments.
type Ex = (u64, Vec<u8>, String, bool, bool, bool, Option<String>);
/// A queue row: id, name, type, durable, auto-delete, arguments, lanes, state.
type Qu = (
    u64,
    Vec<u8>,
    String,
    bool,
    bool,
    Option<String>,
    u16,
    String,
);
/// A binding row: source, destination, destination kind, key, arguments.
type Bind = (u64, u64, String, Vec<u8>, Option<String>);

/// Whether `path` is one of these routes.
#[must_use]
pub fn is_messaging_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("/v1/databases/") else {
        return false;
    };
    let Some((_, tail)) = rest.split_once('/') else {
        return false;
    };
    tail == "mq"
        || (tail.starts_with("streams/") && tail.ends_with("/groups"))
        || (tail.starts_with("sinks/") && tail.contains("/deadLetters"))
        || ((tail.starts_with("sinks/") || tail.starts_with("connections/"))
            && tail.ends_with(":test")
            && tail.matches('/').count() == 1)
}

/// `Sinks.Test` and `Connections.Test`'s statement (R9, r9last): the
/// statements Studio would run, names quoted.
fn test_statement(db: &str, kind: &str, name: &str, body: &Value) -> Result<String, SprigError> {
    let q = |s: &str| format!("{}.{}", ident(db), ident(s));
    if kind == "sinks" {
        let n = body.get("sample").and_then(Value::as_u64).unwrap_or(10);
        if !(1..=1000).contains(&n) {
            return Err(invalid("sample must be 1 to 1000"));
        }
        return Ok(format!("TEST SINK {} WITH SAMPLE {n}", q(name)));
    }
    let platform = body
        .get("platform")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut sql = if platform {
        format!("TEST PLATFORM CONNECTION {}", ident(name))
    } else {
        format!("TEST CONNECTION {}", q(name))
    };
    if body.get("write").and_then(Value::as_bool).unwrap_or(false) {
        sql.push_str(" WRITE");
        if let Some(to) = body.get("to").and_then(Value::as_str) {
            sql.push_str(" TO '");
            sql.push_str(&to.replace('\\', "\\\\").replace('\'', "''"));
            sql.push('\'');
        }
    }
    Ok(sql)
}

/// `Sinks.Test` / `Connections.Test` (see the module docs).
async fn test_route(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    body: &Value,
) -> Result<Response<Body>, SprigError> {
    let (columns, rows) = run_test(core, caller, db, kind, name, body).await?;
    Ok(json_response(
        200,
        &json!({"columns": columns, "rows": rows}),
    ))
}

/// `Sinks.Test` (`kind` `"sinks"`) or `Connections.Test`
/// (`"connections"`) as the caller: the columns and rows, for the REST
/// route above and the gRPC services (`sprig.v1.Sinks`,
/// `sprig.v1.Connections`).
pub(crate) async fn run_test(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    body: &Value,
) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), SprigError> {
    let sql = test_statement(db, kind, name, body)?;
    let r = run_sql(core, caller, db, sql).await?;
    let columns: Vec<String> = r
        .results
        .iter()
        .find(|x| !x.columns.is_empty())
        .map(|x| x.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    Ok((columns, rows_of(&r)))
}

fn decode(seg: &str) -> String {
    let b = seg.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    let hex = |c: u8| (c as char).to_digit(16);
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            out.push(u8::try_from(h * 16 + l).unwrap_or(b'?'));
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn db_err(e: &mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("the tenant's catalog: {e}"),
    )
}

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

async fn grant_lines(core: &Arc<Core>, caller: &Caller) -> Result<Vec<String>, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    Ok(rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}

/// The catalog connection, its schema and the database's id.
async fn catalog(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
) -> Result<(mysql_async::Conn, String, u32), SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("these views need a tenant login"));
    };
    let cat = svc
        .cache
        .cached(tid)
        .ok_or_else(|| invalid("the tenant's catalog isn't loaded yet; retry"))?;
    let row = cat
        .database(db)
        .or_else(|| cat.database(&db.to_ascii_lowercase()))
        .ok_or_else(|| SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'")))?;
    let db_id = row.db_id.0;
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::CatalogUnavailable,
            "the tenant's catalog is unreachable",
        )
        .with_source(e)
    })?;
    Ok((conn, ep.store.schema().to_owned(), db_id))
}

fn denied_db(caller: &Caller, db: &str) -> SprigError {
    SprigError::standard(
        1044,
        *b"42000",
        format!(
            "Access denied for user '{}' to database '{db}'",
            caller.principal.user
        ),
    )
}

/// Answers one of the routes in the module docs.
///
/// # Errors
/// As the module docs say.
pub async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let caller = core
        .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
        .await?;
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let rest = path.trim_start_matches("/v1/databases/");
    let (db_enc, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let db = decode(db_enc);
    let segs: Vec<String> = tail.split('/').map(decode).collect();
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    match (&method, segs.as_slice()) {
        (&Method::GET, ["mq"]) => topology(core, &caller, &db).await,
        (&Method::GET, ["streams", name, "groups"]) => groups(core, &caller, &db, name).await,
        (&Method::GET, ["sinks", name, "deadLetters"]) => {
            sink_dead(core, &caller, &db, name, None).await
        }
        (&Method::POST, [kind @ ("sinks" | "connections"), name]) if name.ends_with(":test") => {
            let (kind, name) = (
                (*kind).to_owned(),
                name.trim_end_matches(":test").to_owned(),
            );
            let body = read_body(req).await?;
            let body = if body.is_empty() {
                json!({})
            } else {
                parse_json(&body)?
            };
            test_route(core, &caller, &db, &kind, &name, &body).await
        }
        (&Method::POST, ["sinks", name, verb])
            if *verb == "deadLetters:replay" || *verb == "deadLetters:discard" =>
        {
            let (name, verb) = ((*name).to_owned(), (*verb).to_owned());
            let body = parse_json(&read_body(req).await?)?;
            sink_dead(core, &caller, &db, &name, Some((&verb, &body))).await
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

/// `Streams.ListGroups` for one stream.
async fn groups(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    name: &str,
) -> Result<Response<Body>, SprigError> {
    let lines = grant_lines(core, caller).await?;
    if !access_of(&lines, db).list {
        return Err(denied_db(caller, db));
    }
    // Watermarks as the caller: SELECT on the stream is checked here.
    let marks = run_sql(
        core,
        caller,
        db,
        format!(
            "SELECT partition_no, MIN(offset_no), MAX(offset_no) + 1, COUNT(*) FROM {} GROUP BY partition_no",
            ident(name)
        ),
    )
    .await?;
    let mut parts: BTreeMap<u64, Value> = BTreeMap::new();
    for row in rows_of(&marks) {
        let n = |i: usize| {
            row.get(i)
                .cloned()
                .flatten()
                .and_then(|s| s.parse::<u64>().ok())
        };
        if let Some(p) = n(0) {
            parts.insert(
                p,
                json!({"partition": p, "log_start": n(1), "high_watermark": n(2), "records": n(3)}),
            );
        }
    }
    let (mut conn, schema, db_id) = catalog(core, caller, db).await?;
    let s = ident(&schema);
    let stream: Option<(u64, u32, String, String)> = conn
        .exec_first(
            format!(
                "SELECT stream_id, partitions, kind, cleanup FROM {s}.streams WHERE db_id = ? AND name = ?"
            ),
            (db_id, name),
        )
        .await
        .map_err(|e| db_err(&e))?;
    let Some((stream_id, partitions, kind, cleanup)) = stream else {
        return Err(SprigError::new(
            SprigCode::StreamNotFound,
            format!("Unknown stream '{db}.{name}'"),
        ));
    };
    for p in 0..u64::from(partitions) {
        parts.entry(p).or_insert_with(
            || json!({"partition": p, "log_start": null, "high_watermark": 0, "records": 0}),
        );
    }
    let offs: Vec<Off> = match conn
        .exec(
            format!(
                "SELECT o.group_name, o.partition_no, o.offset_no, DATE_FORMAT(o.committed_at, '%Y-%m-%d %H:%i:%s'), \
                 g.generation, CAST(g.state AS CHAR) FROM {s}.stream_offsets o LEFT JOIN {s}.stream_groups g \
                 ON g.group_name = o.group_name WHERE o.stream_id = ? ORDER BY o.group_name, o.partition_no"
            ),
            (stream_id,),
        )
        .await
    {
        Ok(v) => v,
        Err(e) if missing_table(&e) => Vec::new(),
        Err(e) => return Err(db_err(&e)),
    };
    let mut groups: BTreeMap<String, Value> = BTreeMap::new();
    for (g, p, off, at, generation, state) in offs {
        let hw = parts
            .get(&u64::from(p))
            .and_then(|v| v["high_watermark"].as_u64())
            .unwrap_or(0);
        let e = groups.entry(g.clone()).or_insert_with(|| {
            json!({"group": g, "generation": generation, "state": state, "partitions": [], "lag": 0})
        });
        let lag = hw.saturating_sub(off);
        e["lag"] = json!(e["lag"].as_u64().unwrap_or(0) + lag);
        if let Some(a) = e["partitions"].as_array_mut() {
            a.push(json!({"partition": p, "committed": off, "lag": lag, "committed_at": at}));
        }
    }
    Ok(json_response(
        200,
        &json!({
            "stream": name,
            "kind": kind,
            "cleanup": cleanup,
            "partitions": parts.into_values().collect::<Vec<_>>(),
            "groups": groups.into_values().collect::<Vec<_>>(),
        }),
    ))
}

fn text(b: Option<Vec<u8>>) -> Value {
    b.map_or(Value::Null, |v| {
        Value::String(String::from_utf8_lossy(&v).into_owned())
    })
}

fn json_text(s: Option<String>) -> Value {
    s.and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null)
}

/// The database's virtual host and its topology.
#[allow(clippy::too_many_lines)] // one query and one mapping per object kind
async fn topology(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
) -> Result<Response<Body>, SprigError> {
    let lines = grant_lines(core, caller).await?;
    if !access_of(&lines, db).list {
        return Err(denied_db(caller, db));
    }
    let (mut conn, schema, db_id) = catalog(core, caller, db).await?;
    let s = ident(&schema);
    let vhost: Option<(String, String)> = match conn
        .exec_first(
            format!("SELECT vhost_name, CAST(default_queue_type AS CHAR) FROM {s}.mq_vhosts WHERE db_id = ?"),
            (db_id,),
        )
        .await
    {
        Ok(v) => v,
        Err(e) if missing_table(&e) => {
            return Ok(json_response(
                200,
                &json!({"vhost": null, "exchanges": [], "queues": [], "bindings": []}),
            ));
        }
        Err(e) => return Err(db_err(&e)),
    };
    let ex: Vec<Ex> = conn
        .exec(
            format!(
                "SELECT object_id, name, CAST(type AS CHAR), durable, auto_delete, internal, CAST(arguments AS CHAR) \
                 FROM {s}.mq_exchanges WHERE db_id = ? ORDER BY name"
            ),
            (db_id,),
        )
        .await
        .map_err(|e| db_err(&e))?;
    let qs: Vec<Qu> = conn
        .exec(
            format!(
                "SELECT object_id, name, CAST(qtype AS CHAR), durable, auto_delete, CAST(arguments AS CHAR), lanes, \
                 CAST(state AS CHAR) FROM {s}.mq_queues WHERE db_id = ? ORDER BY name"
            ),
            (db_id,),
        )
        .await
        .map_err(|e| db_err(&e))?;
    let bs: Vec<Bind> = conn
        .exec(
            format!(
                "SELECT source_id, dest_id, CAST(dest_kind AS CHAR), binding_key, CAST(arguments AS CHAR) \
                 FROM {s}.mq_bindings WHERE db_id = ?"
            ),
            (db_id,),
        )
        .await
        .map_err(|e| db_err(&e))?;
    let mut names: BTreeMap<u64, Value> = BTreeMap::new();
    let exchanges: Vec<Value> = ex
        .into_iter()
        .map(|(id, name, ty, durable, auto_delete, internal, args)| {
            let n = text(Some(name));
            names.insert(id, n.clone());
            json!({"id": id, "name": n, "type": ty, "durable": durable, "auto_delete": auto_delete,
                   "internal": internal, "arguments": json_text(args)})
        })
        .collect();
    let queues: Vec<Value> = qs
        .into_iter()
        .map(|(id, name, ty, durable, auto_delete, args, lanes, state)| {
            let n = text(Some(name));
            names.insert(id, n.clone());
            json!({"id": id, "name": n, "type": ty, "durable": durable, "auto_delete": auto_delete,
                   "arguments": json_text(args), "lanes": lanes, "state": state})
        })
        .collect();
    let bindings: Vec<Value> = bs
        .into_iter()
        .map(|(src, dst, kind, key, args)| {
            json!({"source": names.get(&src).cloned().unwrap_or(Value::Null),
                   "destination": names.get(&dst).cloned().unwrap_or(Value::Null),
                   "destination_kind": kind, "binding_key": text(Some(key)), "arguments": json_text(args)})
        })
        .collect();
    // A database is its own virtual host (AMQP clients name it in the
    // vhost; `/` maps to `amqp.default_vhost_database`). `mq_vhosts` only
    // holds a row when a vhost got limits or a description, so without one
    // the database's name is the vhost once it has any topology (Studio
    // round 6: the queue pages showed "no virtual host yet" otherwise).
    let vhost_name = vhost
        .as_ref()
        .map(|v| v.0.clone())
        .or_else(|| (!exchanges.is_empty() || !queues.is_empty()).then(|| db.to_owned()));
    Ok(json_response(
        200,
        &json!({
            "vhost": vhost_name,
            "default_queue_type": vhost.map(|v| v.1),
            "exchanges": exchanges,
            "queues": queues,
            "bindings": bindings,
        }),
    ))
}

/// Whether the grants hold `SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN`.
#[must_use]
pub fn sink_admin(lines: &[String]) -> bool {
    super::grants::Privileges::of(lines).tenant_admin
        || lines.iter().any(|l| {
            let u = l.to_ascii_uppercase();
            u.starts_with("GRANT ") && u.contains("SPRIG_SINK_ADMIN") && u.contains(" ON *.* ")
        })
}

/// A sink's dead letters: listed, or replayed or discarded.
async fn sink_dead(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    name: &str,
    act: Option<(&str, &Value)>,
) -> Result<Response<Body>, SprigError> {
    let lines = grant_lines(core, caller).await?;
    let (mut conn, schema, db_id) = catalog(core, caller, db).await?;
    let s = ident(&schema);
    let sink: Option<(u64, u32)> = match conn
        .exec_first(
            format!("SELECT sink_id, owner_user_id FROM {s}.sinks WHERE db_id = ? AND name = ?"),
            (db_id, name),
        )
        .await
    {
        Ok(v) => v,
        Err(e) if missing_table(&e) => None,
        Err(e) => return Err(db_err(&e)),
    };
    let Some((sink_id, owner)) = sink else {
        return Err(SprigError::new(
            SprigCode::SinkNotFound,
            format!("Unknown sink '{db}.{name}'"),
        ));
    };
    let is_owner = caller.principal.user_id.is_some_and(|u| u.0 == owner);
    if !(is_owner || sink_admin(&lines)) {
        return Err(SprigError::standard(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the SPRIG_SINK_ADMIN privilege(s) for this operation, or to own the sink",
        ));
    }
    let store = sprig_sinks::store::SinksStore::new(schema.clone());
    let id = sprig_sinks::model::SinkId(sink_id);
    if let Some((verb, body)) = act {
        let ids: Vec<u64> = body
            .get("ids")
            .and_then(Value::as_array)
            .map(|xs| {
                xs.iter()
                    .filter_map(|x| {
                        x.as_u64()
                            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let all = body.get("all").and_then(Value::as_bool).unwrap_or(false);
        if ids.is_empty() && !all {
            return Err(invalid(
                "name the dead letters (ids) or ask for all of them",
            ));
        }
        let state = if verb.ends_with(":replay") {
            "replaying"
        } else {
            "discarded"
        };
        let n = store.mark_dead(&mut conn, id, &ids, state).await?;
        return Ok(json_response(200, &json!({"marked": n, "state": state})));
    }
    let rows = store.dead_letters(&mut conn, id, MAX_DEAD).await?;
    let list: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({"id": r.id, "key": r.idem_key, "reason": r.reason, "class": r.class,
                   "attempts": r.attempts, "state": r.state, "at": r.created_at})
        })
        .collect();
    Ok(json_response(
        200,
        &json!({"sink": name, "dead_letters": list}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messaging_paths() {
        assert!(is_messaging_path("/v1/databases/shop/mq"));
        assert!(is_messaging_path(
            "/v1/databases/shop/streams/orders/groups"
        ));
        assert!(is_messaging_path(
            "/v1/databases/shop/sinks/out/deadLetters"
        ));
        assert!(is_messaging_path(
            "/v1/databases/shop/sinks/out/deadLetters:replay"
        ));
        assert!(!is_messaging_path("/v1/databases/shop/streams/orders"));
        // R9 (r9last): Sinks.Test and Connections.Test.
        assert!(is_messaging_path("/v1/databases/shop/sinks/out:test"));
        assert!(is_messaging_path(
            "/v1/databases/shop/connections/hook:test"
        ));
        assert!(!is_messaging_path("/v1/databases/shop/sinks/out/x:test"));
        assert_eq!(
            test_statement("shop", "sinks", "out", &json!({})).unwrap(),
            "TEST SINK `shop`.`out` WITH SAMPLE 10"
        );
        assert!(test_statement("shop", "sinks", "out", &json!({"sample": 0})).is_err());
        assert_eq!(
            test_statement(
                "shop",
                "connections",
                "h`k",
                &json!({"write": true, "to": "t'1"})
            )
            .unwrap(),
            "TEST CONNECTION `shop`.`h``k` WRITE TO 't''1'"
        );
        assert_eq!(
            test_statement("-", "connections", "kafka_main", &json!({"platform": true})).unwrap(),
            "TEST PLATFORM CONNECTION `kafka_main`"
        );
        assert!(!is_messaging_path("/v1/databases/shop/sql:query"));
        assert_eq!(decode("a%20b%2Fc"), "a b/c");
        assert_eq!(decode("%zz%"), "%zz%");
    }

    #[test]
    fn sink_admins() {
        let l = |xs: &[&str]| xs.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert!(sink_admin(&l(&[
            "GRANT SPRIG_SINK_ADMIN ON *.* TO `a`@`%`"
        ])));
        assert!(sink_admin(&l(&[
            "GRANT SPRIG_TENANT_ADMIN ON *.* TO `a`@`%`"
        ])));
        assert!(!sink_admin(&l(&["GRANT SELECT ON `shop`.* TO `a`@`%`"])));
    }
}
