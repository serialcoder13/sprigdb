//! `$currentOp` and `$listSessions` (M2, 12 Nov 2026): MongoDB's shapes,
//! filled from the gateway's session table.
//!
//! - `$currentOp` (`aggregate: 1` on `admin`) lists the caller's own
//!   connections on this gateway, as `SHOW PROCESSLIST` does for a user
//!   without chapter 29's `PROCESS` privilege. `allUsers: true` lists
//!   every session of the tenant on this gateway for a user holding
//!   `PROCESS` on the tenant (MongoDB's `inprog`), and answers 13
//!   `Unauthorized` otherwise (M5, decision S78). The running command is
//!   always listed;
//!   `idleConnections: true` adds idle ones. Fields with no meaning in
//!   SprigDB (locks, yields, flow control) are zero or empty.
//! - `$listSessions` (on `config.system.sessions`) lists the caller's
//!   current logical session: SprigDB keeps no cluster-wide session
//!   collection.
//!
//! The collection-level diagnostics (`$collStats`, `$indexStats`,
//! `$planCacheStats`) and `$listClusterCatalog` run in the engine.

use bson::{Bson, DateTime, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::commands::owner;
use super::conn::Conn;
use crate::frontend::Protocol;

fn unknown(stage: &str, k: &str) -> MongoError {
    MongoError::new(
        40415,
        format!("BSON field '{stage}.{k}' is an unknown field."),
    )
}

/// The documents of a gateway-run first stage, or `None` for other
/// stages.
///
/// # Errors
/// 73 for the wrong namespace; 40415 for an unknown option.
pub(super) fn first_docs(
    c: &Conn,
    db: &str,
    coll: &str,
    body: &Document,
    first: Option<&Document>,
    lsid: Option<Vec<u8>>,
    process: Option<bool>,
) -> Result<Option<Vec<Document>>, MongoError> {
    let Some((name, arg)) = first.and_then(|s| s.iter().next()) else {
        return Ok(None);
    };
    match name.as_str() {
        "$currentOp" => {
            if db != "admin" || !coll.is_empty() {
                return Err(MongoError::new(
                    code::INVALID_NAMESPACE,
                    "$currentOp must be run against the 'admin' database with {aggregate: 1}",
                ));
            }
            Ok(Some(current_op(c, arg, body, lsid.as_deref(), process)?))
        }
        "$listSessions" => {
            if db != "config" || coll != "system.sessions" {
                return Err(MongoError::new(
                    code::INVALID_NAMESPACE,
                    "$listSessions may only be run against config.system.sessions",
                ));
            }
            Ok(Some(list_sessions(c, arg, lsid)?))
        }
        _ => Ok(None),
    }
}

/// Whether the caller holds `PROCESS` on the tenant, asked only for
/// `$currentOp` with `allUsers` (M5, S78). A session that hasn't run a
/// statement yet has no catalog installed: one `SELECT 1` installs it.
pub(super) async fn process_privilege(
    sql: &mut super::sql::SqlSession,
    first: Option<&Document>,
) -> Option<bool> {
    let asks = first
        .and_then(|s| s.get_document("$currentOp").ok())
        .is_some_and(|o| o.get_bool("allUsers").unwrap_or(false));
    if !asks {
        return None;
    }
    let p = sprig_auth::authz::Action::Process;
    if let Some(v) = sql.holds_tenant_privilege(p) {
        return Some(v);
    }
    let _ = sprig_doc::engine::SqlRunner::query(sql, "SELECT 1").await;
    sql.holds_tenant_privilege(p)
}

#[allow(clippy::too_many_lines)]
fn current_op(
    c: &Conn,
    arg: &Bson,
    body: &Document,
    lsid: Option<&[u8]>,
    process: Option<bool>,
) -> Result<Vec<Document>, MongoError> {
    let Bson::Document(spec) = arg else {
        return Err(MongoError::new(
            code::TYPE_MISMATCH,
            "$currentOp options must be specified in an object",
        ));
    };
    for (k, v) in spec {
        match k.as_str() {
            "allUsers" | "idleConnections" | "idleCursors" | "idleSessions" | "localOps"
            | "backtrace" | "truncateOps" | "targetAllNodes" => {
                if !matches!(v, Bson::Boolean(_)) {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        format!(
                            "The '{k}' parameter of the $currentOp stage must be a boolean value, but found: {v}"
                        ),
                    ));
                }
            }
            _ => return Err(unknown("$currentOp", k)),
        }
    }
    let idle = spec.get_bool("idleConnections").unwrap_or(false);
    let who = owner(c);
    // M5 (S78): every user's sessions need `PROCESS` on the tenant.
    let all_users = spec.get_bool("allUsers").unwrap_or(false);
    if all_users && process != Some(true) {
        return Err(MongoError::new(
            code::UNAUTHORIZED,
            format!(
                "not authorized on admin to execute command {{ aggregate: 1, pipeline: [ {{ $currentOp: {spec} }} ] }} (needs the PROCESS privilege)"
            ),
        ));
    }
    let own = c.sql.as_ref().map(super::sql::SqlSession::session_id);
    let now = DateTime::now();
    let mut out = Vec::new();
    for r in c.shared.d.sessions.list() {
        if r.tenant.map_or(0, |t| t.0) != who.tenant
            || (!all_users && r.user.as_deref() != Some(who.user.as_str()))
        {
            continue;
        }
        // The caller's own row: its SQL session, or its client address.
        let mine = Some(r.id) == own || r.host == c.peer;
        let active = mine || r.command != "Sleep";
        if !active && !idle {
            continue;
        }
        let micros = i64::try_from(r.time.as_micros()).unwrap_or(i64::MAX);
        let id = i64::from(r.id.0);
        let mut d = doc! {
            "type": "op",
            "host": "sprigdb",
            "desc": format!("conn{id}"),
            "connectionId": id,
            "client": r.host.to_string(),
        };
        if mine && let Some(m) = &c.client_meta {
            d.insert("clientMetadata", m.clone());
            if let Ok(a) = m
                .get_document("application")
                .and_then(|a| a.get_str("name"))
            {
                d.insert("appName", a);
            }
        }
        d.insert("active", active);
        d.insert(
            "currentOpTime",
            sprig_doc::json::iso_from_ms(now.timestamp_millis()).unwrap_or_default(),
        );
        d.insert(
            "effectiveUsers",
            vec![Bson::Document(
                doc! {"user": r.user.clone().unwrap_or_default(), "db": "admin"},
            )],
        );
        d.insert("isFromUserConnection", true);
        d.insert("threaded", true);
        d.insert("opid", id);
        if active {
            d.insert("secs_running", micros / 1_000_000);
            d.insert("microsecs_running", micros);
            d.insert("op", if mine { "command" } else { "query" });
            if mine && let Some(l) = lsid {
                d.insert(
                    "lsid",
                    doc! {"id": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: l.to_vec() }},
                );
            }
            d.insert("redacted", false);
            d.insert(
                "ns",
                r.db.clone()
                    .map_or_else(String::new, |db| format!("{db}.$cmd")),
            );
            d.insert(
                "command",
                if mine {
                    body.clone()
                } else {
                    doc! {"$sprig": r.info.clone().unwrap_or_default()}
                },
            );
            d.insert("numYields", 0i32);
            d.insert("numInterruptChecks", 0i32);
            d.insert("locks", Document::new());
            d.insert("waitingForLock", false);
            d.insert("lockStats", Document::new());
            d.insert("waitingForFlowControl", false);
            d.insert("flowControlStats", Document::new());
        } else {
            d.insert("op", "none");
        }
        d.insert("killPending", false);
        d.insert(
            "sprig",
            doc! {
                "protocol": match r.protocol {
                    Protocol::Mongo => "mongodb",
                    Protocol::MySql84 => "mysql",
                    Protocol::MariaDb => "mariadb",
                    Protocol::Redis => "redis",
                    Protocol::Http => "http",
                    Protocol::Bolt => "bolt",
                    _ => "other",
                },
                "listener": r.listener,
                "command": r.command,
                "statement": r.info.clone().map_or(Bson::Null, Bson::String),
            },
        );
        out.push(d);
    }
    // Idle change-stream cursors with their position (M4, 9.0's
    // `cursor.changeStreams.optime`).
    if spec.get_bool("idleCursors").unwrap_or(false) {
        out.extend(super::status::change_stream_ops(c));
    }
    Ok(out)
}

/// Records a command on a collection for `$collStats.latencyStats` (M5,
/// decision S79): reads, writes, other commands, and anything inside a
/// transaction as a transaction.
pub(super) fn record_latency(
    tenant: u64,
    name: &str,
    body: &Document,
    db: &str,
    took: std::time::Duration,
) {
    use sprig_doc::usage::Class;
    let coll = match name {
        "getMore" => body.get_str("collection").ok(),
        _ => body.get_str(name).ok(),
    };
    let Some(coll) = coll.filter(|c| !c.is_empty()) else {
        return;
    };
    let class = if body.get_bool("autocommit") == Ok(false) {
        Class::Transactions
    } else {
        match name {
            "find" | "aggregate" | "count" | "distinct" | "getMore" => Class::Reads,
            "insert" | "update" | "delete" | "findAndModify" => Class::Writes,
            _ => Class::Commands,
        }
    };
    let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
    sprig_doc::usage::record_latency(tenant, db, coll, class, micros);
}

fn list_sessions(c: &Conn, arg: &Bson, lsid: Option<Vec<u8>>) -> Result<Vec<Document>, MongoError> {
    let Bson::Document(spec) = arg else {
        return Err(MongoError::new(
            code::TYPE_MISMATCH,
            "$listSessions options must be specified in an object",
        ));
    };
    for k in spec.keys() {
        if !matches!(k.as_str(), "allUsers" | "users") {
            return Err(unknown("$listSessions", k));
        }
    }
    let who = owner(c);
    let mut out = Vec::new();
    if let Some(l) = lsid {
        out.push(doc! {
            "_id": {
                "id": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: l },
                "uid": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: super::retry::uid_of(&who).to_vec() },
            },
            "lastUse": DateTime::now(),
            "user": {"name": format!("{}@admin", who.user)},
        });
    }
    Ok(out)
}
