//! Command handlers (chapter 30, "Dispatch"; build steps 4-8 in part).
//!
//! Each handler parses the command, runs `sprig-doc`'s engine over a
//! [`SqlRunner`] and builds the reply. Outside transactions the runner is
//! the connection's own executor session; inside one it is the session of
//! the transaction's `lsid`, so the statements of a transaction run on one
//! executor session whichever connection carries them (drivers without
//! `loadBalanced` may use any pooled connection).

#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::too_many_arguments
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use sprig_doc::engine::{FindSpec, Ns, SqlRunner};
use sprig_doc::filter::as_i64;
use sprig_proto_mongo::errors::MongoError;

use super::conn::Conn;
use super::cursors::{CursorTable, Owner};
use super::sql::SqlSession;

/// Largest batch reply, in BSON bytes (16 MiB less room for the envelope).
const BATCH_BYTES: usize = 16 * 1024 * 1024 - 64 * 1024;

fn bad(m: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, m)
}

fn no_txn() -> MongoError {
    MongoError::new(
        code::NO_SUCH_TRANSACTION,
        "Transaction is not in progress or was aborted",
    )
    .label("TransientTransactionError")
}

/// One transaction's executor session.
#[derive(Debug)]
pub struct TxnEntry {
    sql: Option<SqlSession>,
    txn_number: i64,
    owner: Owner,
    started: Instant,
    /// 0 open, 1 committed, 2 aborted.
    state: u8,
}

/// Transactions by `lsid`.
#[derive(Debug, Default)]
pub struct TxnTable {
    map: tokio::sync::Mutex<HashMap<Vec<u8>, Arc<tokio::sync::Mutex<TxnEntry>>>>,
}

impl TxnTable {
    /// Rolls back transactions past `lifetime`.
    pub async fn reap(&self, lifetime: Duration) {
        let entries: Vec<(Vec<u8>, Arc<tokio::sync::Mutex<TxnEntry>>)> = self
            .map
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (k, e) in entries {
            let Ok(mut g) = e.try_lock() else { continue };
            if g.state == 0 && g.started.elapsed() > lifetime {
                if let Some(mut s) = g.sql.take() {
                    let _ = s.exec("ROLLBACK").await;
                    s.close().await;
                }
                g.state = 2;
                metrics::counter!("sprig_mongo_transactions_total", "outcome" => "expired")
                    .increment(1);
            }
            if g.state != 0 && g.started.elapsed() > lifetime * 10 {
                drop(g);
                self.map.lock().await.remove(&k);
            }
        }
    }

    async fn get(&self, lsid: &[u8]) -> Option<Arc<tokio::sync::Mutex<TxnEntry>>> {
        self.map.lock().await.get(lsid).cloned()
    }

    async fn put(&self, lsid: Vec<u8>, e: TxnEntry) -> Arc<tokio::sync::Mutex<TxnEntry>> {
        let a = Arc::new(tokio::sync::Mutex::new(e));
        if let Some(old) = self.map.lock().await.insert(lsid, Arc::clone(&a)) {
            let mut g = old.lock().await;
            if let Some(mut s) = g.sql.take() {
                let _ = s.exec("ROLLBACK").await;
                s.close().await;
            }
        }
        a
    }

    async fn end_session(&self, lsid: &[u8]) {
        let e = self.map.lock().await.remove(lsid);
        if let Some(e) = e {
            let mut g = e.lock().await;
            if let Some(mut s) = g.sql.take() {
                let _ = s.exec("ROLLBACK").await;
                s.close().await;
            }
        }
    }
}

fn lsid_of(body: &Document) -> Option<Vec<u8>> {
    match body.get_document("lsid").ok()?.get("id")? {
        Bson::Binary(b) => Some(b.bytes.clone()),
        _ => None,
    }
}

pub(super) fn owner(c: &Conn) -> Owner {
    let p = c.principal.as_ref();
    Owner {
        tenant: p.and_then(|p| p.tenant).map_or(0, |t| t.0),
        user: p.map(|p| p.user.clone()).unwrap_or_default(),
    }
}

pub(super) fn coll_of(body: &Document, name: &str) -> Result<String, MongoError> {
    match body.get(name) {
        Some(Bson::String(s)) => Ok(s.clone()),
        _ => Err(MongoError::new(
            code::INVALID_NAMESPACE,
            format!(
                "collection name has invalid type {}",
                body.get(name)
                    .map_or("missing", sprig_doc::update::type_name)
            ),
        )),
    }
}

fn i64_of(body: &Document, k: &str) -> Option<i64> {
    body.get(k).and_then(as_i64)
}

pub(super) fn docs_of(body: &Document, k: &str) -> Vec<Document> {
    body.get_array(k)
        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
        .unwrap_or_default()
}

/// Runs a command after authentication.
///
/// # Errors
/// The command's.
pub async fn run(c: &mut Conn, name: &str, body: &Document) -> Result<Document, MongoError> {
    let db = body.get_str("$db").unwrap_or("admin").to_owned();
    if let Ok(api) = body.get_str("apiVersion")
        && api != "1"
    {
        return Err(MongoError::new(
            322,
            format!("API version must be \"1\" but got \"{api}\""),
        ));
    }
    // Commands that touch no data.
    match name {
        "ping" => return Ok(doc! {"ok": 1.0}),
        "buildInfo" | "buildinfo" => {
            let v = c.shared.settings().server_version;
            let parts: Vec<i32> = v.split('.').filter_map(|x| x.parse().ok()).collect();
            return Ok(
                doc! {"version": v.clone(), "versionArray": [parts.first().copied().unwrap_or(8), parts.get(1).copied().unwrap_or(0), parts.get(2).copied().unwrap_or(0), 0], "gitVersion": "sprigdb", "bits": 64, "maxBsonObjectSize": 16_777_216, "sprig": {"gateway": c.shared.d.gateway_id.clone()}, "ok": 1.0},
            );
        }
        "connectionStatus" => {
            let p = c.principal.as_ref();
            let users: Vec<Document> = p.map(|p| doc! {"user": format!("{}.{}", p.tenant_name.clone().unwrap_or_default(), p.user), "db": "admin"}).into_iter().collect();
            return Ok(
                doc! {"authInfo": {"authenticatedUsers": users, "authenticatedUserRoles": []}, "ok": 1.0},
            );
        }
        "getParameter" => {
            let mut d = Document::new();
            if body.contains_key("featureCompatibilityVersion")
                || body.get("getParameter") == Some(&Bson::String("*".into()))
            {
                d.insert("featureCompatibilityVersion", doc! {"version": "8.0"});
            }
            d.insert("ok", 1.0);
            return Ok(d);
        }
        "hostInfo" => {
            return Ok(
                doc! {"system": {"hostname": c.shared.d.gateway_id.clone()}, "os": {}, "extra": {}, "ok": 1.0},
            );
        }
        "serverStatus" => {
            return Ok(
                doc! {"host": c.shared.d.gateway_id.clone(), "version": c.shared.settings().server_version, "process": "mongos", "uptime": c.shared.started.elapsed().as_secs_f64(), "localTime": bson::DateTime::now(), "connections": {"current": 1}, "ok": 1.0},
            );
        }
        "whatsmyuri" => return Ok(doc! {"you": c.peer.to_string(), "ok": 1.0}),
        "getLog" => return Ok(doc! {"totalLinesWritten": 0, "log": [], "ok": 1.0}),
        "getCmdLineOpts" => return Ok(doc! {"argv": [], "parsed": {}, "ok": 1.0}),
        "endSessions" | "killSessions" => {
            if let Ok(a) = body.get_array(name) {
                for s in a {
                    if let Some(Bson::Binary(b)) = s.as_document().and_then(|d| d.get("id")) {
                        c.shared.cursors.kill_session(&b.bytes);
                        c.shared.txns.end_session(&b.bytes).await;
                    }
                }
            }
            return Ok(doc! {"ok": 1.0});
        }
        "refreshSessions" | "startSession" => {
            if name == "startSession" {
                let id = uuid::Uuid::new_v4();
                return Ok(
                    doc! {"id": {"id": bson::Binary{subtype: bson::spec::BinarySubtype::Uuid, bytes: id.as_bytes().to_vec()}}, "timeoutMinutes": c.shared.settings().session_timeout_minutes, "ok": 1.0},
                );
            }
            return Ok(doc! {"ok": 1.0});
        }
        "mapReduce"
        | "eval"
        | "$eval"
        | "geoNear"
        | "shardCollection"
        | "enableSharding"
        | "createUser"
        | "updateUser"
        | "dropUser"
        | "usersInfo"
        | "createRole"
        | "dropRole"
        | "rolesInfo"
        | "grantRolesToUser"
        | "revokeRolesFromUser" => {
            if matches!(name, "shardCollection" | "enableSharding") {
                return Ok(
                    doc! {"ok": 1.0, "note": "recorded as a hint only: SprigDB shards collections itself (chapter 16)"},
                );
            }
            if name == "usersInfo" || name == "rolesInfo" {
                return Ok(doc! {"users": [], "roles": [], "ok": 1.0});
            }
            return Err(MongoError::new(
                code::COMMAND_NOT_SUPPORTED,
                format!("{name} is not supported in this release"),
            ));
        }
        _ => {}
    }
    // Transactions.
    let lsid = lsid_of(body);
    let autocommit_false = body.get_bool("autocommit") == Ok(false);
    let txn_number = i64_of(body, "txnNumber");
    if name == "commitTransaction" || name == "abortTransaction" {
        let (Some(l), Some(n)) = (lsid.as_ref(), txn_number) else {
            return Err(no_txn());
        };
        let Some(e) = c.shared.txns.get(l).await else {
            return Err(no_txn());
        };
        let mut g = e.lock().await;
        if g.owner != owner(c) || g.txn_number != n {
            return Err(no_txn());
        }
        let commit = name == "commitTransaction";
        match (g.state, commit) {
            (1, true) | (2, false) => return Ok(doc! {"ok": 1.0}),
            (0, _) => {}
            _ => return Err(no_txn()),
        }
        let mut s = g.sql.take().ok_or_else(no_txn)?;
        let r = s.exec(if commit { "COMMIT" } else { "ROLLBACK" }).await;
        s.close().await;
        g.state = if commit && r.is_ok() { 1 } else { 2 };
        metrics::counter!("sprig_mongo_transactions_total", "outcome" => if g.state == 1 { "committed" } else { "aborted" }).increment(1);
        return match r {
            Ok(_) => Ok(doc! {"ok": 1.0}),
            Err(e) => {
                Err(sprig_doc::engine::mongo_error(&e).label("UnknownTransactionCommitResult"))
            }
        };
    }
    if autocommit_false {
        let (Some(l), Some(n)) = (lsid.clone(), txn_number) else {
            return Err(bad("autocommit: false needs lsid and txnNumber"));
        };
        if name != "create"
            && matches!(
                name,
                "count"
                    | "listCollections"
                    | "listIndexes"
                    | "create"
                    | "drop"
                    | "dropDatabase"
                    | "createIndexes"
                    | "dropIndexes"
                    | "renameCollection"
                    | "listDatabases"
            )
        {
            return Err(MongoError::new(
                code::OPERATION_NOT_SUPPORTED_IN_TRANSACTION,
                format!("Cannot run '{name}' in a multi-document transaction."),
            ));
        }
        let entry = if body.get_bool("startTransaction") == Ok(true) {
            if let Some(e) = c.shared.txns.get(&l).await {
                let g = e.lock().await;
                if g.txn_number > n {
                    return Err(MongoError::new(
                        code::TRANSACTION_TOO_OLD,
                        format!(
                            "txnNumber {n} is less than last txnNumber {} seen in session",
                            g.txn_number
                        ),
                    ));
                }
            }
            let p = c
                .principal
                .clone()
                .ok_or_else(|| MongoError::new(code::UNAUTHORIZED, "not authenticated"))?;
            let mut s = SqlSession::open(
                &c.shared.d.sessions,
                &c.shared.d.executor,
                &p,
                c.listener,
                c.generation,
                c.peer,
                c.tls.clone(),
                64 * 1024 * 1024,
            )
            .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.message))?;
            let snapshot = body
                .get_document("readConcern")
                .ok()
                .and_then(|r| r.get_str("level").ok())
                == Some("snapshot");
            s.exec(if snapshot {
                "START TRANSACTION WITH CONSISTENT SNAPSHOT"
            } else {
                "START TRANSACTION"
            })
            .await
            .map_err(|e| sprig_doc::engine::mongo_error(&e))?;
            s.in_txn = true;
            c.shared
                .txns
                .put(
                    l,
                    TxnEntry {
                        sql: Some(s),
                        txn_number: n,
                        owner: owner(c),
                        started: Instant::now(),
                        state: 0,
                    },
                )
                .await
        } else {
            let e = c.shared.txns.get(&l).await.ok_or_else(no_txn)?;
            {
                let g = e.lock().await;
                if g.txn_number != n || g.state != 0 || g.owner != owner(c) {
                    return Err(no_txn());
                }
            }
            e
        };
        // A collection created inside a transaction is created first on
        // the connection's own session (MariaDB DDL commits implicitly).
        if matches!(
            name,
            "insert" | "update" | "findAndModify" | "findandmodify" | "create"
        ) {
            let coll = coll_of(body, name)?;
            let ns = Ns {
                db: db.clone(),
                coll,
            };
            let engine = c.shared.engine;
            let sql = c.sql()?;
            if engine.meta(sql, &ns).await?.is_none() {
                engine.create(sql, &ns, Document::new(), false).await?;
            }
            if name == "create" {
                return Ok(doc! {"ok": 1.0});
            }
        }
        let mut g = entry.lock().await;
        let mut s = g.sql.take().ok_or_else(no_txn)?;
        let r = exec(c, &mut s, name, body, &db, lsid.as_deref()).await;
        g.sql = Some(s);
        return r.map_err(|e| {
            if e.code == code::WRITE_CONFLICT {
                e.label("TransientTransactionError")
            } else {
                e
            }
        });
    }
    if c.sql.is_none() {
        c.sql()?;
    }
    let mut s = c
        .sql
        .take()
        .ok_or_else(|| MongoError::new(code::INTERNAL_ERROR, "no session"))?;
    // Retryable writes (chapter 30 step 7): the writes and their record
    // in one MariaDB transaction.
    let r = match (&lsid, txn_number) {
        (Some(l), Some(n)) if super::retry::applies(name, body) => {
            super::retry::run(c, &mut s, name, body, &db, l, n).await
        }
        _ => exec(c, &mut s, name, body, &db, lsid.as_deref()).await,
    };
    c.sql = Some(s);
    r
}

fn cursor_reply(id: i64, ns: &str, batch: Vec<Document>, first: bool) -> Document {
    let key = if first { "firstBatch" } else { "nextBatch" };
    doc! {"cursor": {key: batch.into_iter().map(Bson::Document).collect::<Vec<_>>(), "id": id, "ns": ns}, "ok": 1.0}
}

async fn open_cursor(
    shared: &super::Shared,
    who: Owner,
    sql: &mut SqlSession,
    mut plan: sprig_doc::engine::CursorPlan,
    ns: String,
    body: &Document,
    lsid: Option<&[u8]>,
    first_n: usize,
    single: bool,
) -> Result<Document, MongoError> {
    let batch = if first_n == 0 {
        Vec::new()
    } else {
        shared
            .engine
            .next_batch(sql, &mut plan, first_n, BATCH_BYTES)
            .await?
    };
    if plan.exhausted || single {
        return Ok(cursor_reply(0, &ns, batch, true));
    }
    let bs = body
        .get_document("cursor")
        .ok()
        .and_then(|cd| cd.get("batchSize").and_then(as_i64))
        .or_else(|| i64_of(body, "batchSize"))
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0);
    let id = shared.cursors.open(
        plan,
        ns.clone(),
        who,
        lsid.map(<[u8]>::to_vec),
        bs,
        body.get_bool("noCursorTimeout").unwrap_or(false),
        shared.settings().max_cursors,
    )?;
    Ok(cursor_reply(id, &ns, batch, true))
}

fn first_batch(body: &Document, default: usize) -> usize {
    body.get_document("cursor")
        .ok()
        .and_then(|cd| cd.get("batchSize").and_then(as_i64))
        .or_else(|| i64_of(body, "batchSize"))
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(default)
}

#[allow(clippy::too_many_lines)]
async fn exec(
    c: &mut Conn,
    sql: &mut SqlSession,
    name: &str,
    body: &Document,
    db: &str,
    lsid: Option<&[u8]>,
) -> Result<Document, MongoError> {
    let engine = c.shared.engine;
    let ns_of = |n: &str| -> Result<Ns, MongoError> {
        Ok(Ns {
            db: db.to_owned(),
            coll: coll_of(body, n)?,
        })
    };
    match name {
        "listDatabases" => {
            let dbs = engine.list_databases(sql).await?;
            let name_only = body.get_bool("nameOnly").unwrap_or(false);
            let filter = body.get_document("filter").cloned().unwrap_or_default();
            let pred = sprig_doc::filter::parse(&filter)?;
            let list: Vec<Bson> = dbs
                .into_iter()
                .map(|d| {
                    if name_only {
                        doc! {"name": d}
                    } else {
                        doc! {"name": d, "sizeOnDisk": 0i64, "empty": false}
                    }
                })
                .filter(|d| pred.matches(d))
                .map(Bson::Document)
                .collect();
            if name_only {
                Ok(doc! {"databases": list, "ok": 1.0})
            } else {
                Ok(doc! {"databases": list, "totalSize": 0i64, "totalSizeMb": 0i64, "ok": 1.0})
            }
        }
        "listCollections" => {
            let filter = body.get_document("filter").cloned().unwrap_or_default();
            let docs = engine
                .list_collections(sql, db, &filter, body.get_bool("nameOnly").unwrap_or(false))
                .await?;
            let plan = sprig_doc::engine::CursorPlan::from_docs(
                Ns {
                    db: db.to_owned(),
                    coll: "$cmd.listCollections".into(),
                },
                docs,
            );
            open_cursor(
                &c.shared,
                owner(c),
                sql,
                plan,
                format!("{db}.$cmd.listCollections"),
                body,
                lsid,
                first_batch(body, 101),
                false,
            )
            .await
        }
        "create" => {
            let ns = ns_of("create")?;
            let opts: Document = body
                .iter()
                .filter(|(k, _)| {
                    !k.starts_with('$')
                        && !matches!(
                            k.as_str(),
                            "create"
                                | "lsid"
                                | "txnNumber"
                                | "writeConcern"
                                | "comment"
                                | "autocommit"
                                | "startTransaction"
                        )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            engine.create(sql, &ns, opts, true).await?;
            Ok(doc! {"ok": 1.0})
        }
        "drop" => {
            let ns = ns_of("drop")?;
            let n = engine.list_indexes(sql, &ns).await?.map_or(0, |v| v.len());
            engine.drop(sql, &ns).await?;
            Ok(doc! {"nIndexesWas": i32::try_from(n).unwrap_or(0), "ns": ns.to_string(), "ok": 1.0})
        }
        "dropDatabase" => {
            engine.drop_database(sql, db).await?;
            Ok(doc! {"dropped": db, "ok": 1.0})
        }
        "renameCollection" => {
            let from = body
                .get_str("renameCollection")
                .map_err(|_| bad("renameCollection needs a namespace"))?;
            let to = body
                .get_str("to")
                .map_err(|_| bad("renameCollection needs 'to'"))?;
            let split = |s: &str| -> Result<Ns, MongoError> {
                let (d, c) = s.split_once('.').ok_or_else(|| {
                    MongoError::new(
                        code::INVALID_NAMESPACE,
                        format!("Invalid namespace specified '{s}'"),
                    )
                })?;
                Ok(Ns {
                    db: d.into(),
                    coll: c.into(),
                })
            };
            engine
                .rename(
                    sql,
                    &split(from)?,
                    &split(to)?,
                    body.get_bool("dropTarget").unwrap_or(false),
                )
                .await?;
            Ok(doc! {"ok": 1.0})
        }
        "collMod" => {
            let ns = ns_of("collMod")?;
            if engine.meta(sql, &ns).await?.is_none() {
                return Err(MongoError::new(
                    code::NAMESPACE_NOT_FOUND,
                    format!("ns does not exist: {ns}"),
                ));
            }
            Ok(doc! {"ok": 1.0})
        }
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" => {
            let done = sprig_doc::sessions::StmtResults::new();
            Ok(super::writes::run(engine, sql, name, body, db, &done).await?.reply)
        }
        "find" => {
            let ns = ns_of("find")?;
            let limit = i64_of(body, "limit").unwrap_or(0);
            let single = body.get_bool("singleBatch").unwrap_or(false) || limit < 0;
            let spec = FindSpec {
                filter: body.get_document("filter").cloned().unwrap_or_default(),
                sort: body.get_document("sort").ok().cloned(),
                projection: body.get_document("projection").ok().cloned(),
                skip: u64::try_from(i64_of(body, "skip").unwrap_or(0).max(0)).unwrap_or(0),
                limit: limit.unsigned_abs(),
            };
            let plan = engine.find(sql, &ns, spec).await?;
            let mut first = first_batch(body, 101);
            if limit != 0 {
                first = first.min(usize::try_from(limit.unsigned_abs()).unwrap_or(usize::MAX));
            }
            open_cursor(
                &c.shared,
                owner(c),
                sql,
                plan,
                ns.to_string(),
                body,
                lsid,
                first,
                single,
            )
            .await
        }
        "getMore" => {
            let id = i64_of(body, "getMore")
                .ok_or_else(|| MongoError::new(code::TYPE_MISMATCH, "getMore needs a cursor id"))?;
            let cur = c.shared.cursors.get(id, &owner(c))?;
            let mut g = cur.lock().await;
            let Some(cursor) = g.as_mut() else {
                return Err(MongoError::new(
                    code::CURSOR_NOT_FOUND,
                    format!("cursor id {id} not found"),
                ));
            };
            if cursor.lsid.is_some() && lsid.is_some() && cursor.lsid.as_deref() != lsid {
                return Err(MongoError::new(
                    code::UNAUTHORIZED,
                    "Cannot run getMore on cursor which was created in a different session",
                ));
            }
            CursorTable::touch(cursor);
            let n = i64_of(body, "batchSize")
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| *n > 0)
                .or(cursor.batch_size)
                .unwrap_or(1_000_000);
            let batch = engine
                .next_batch(sql, &mut cursor.plan, n, BATCH_BYTES)
                .await?;
            let ns = cursor.ns.clone();
            let done = cursor.plan.exhausted;
            drop(g);
            let rid = if done {
                c.shared.cursors.remove(id);
                0
            } else {
                id
            };
            Ok(cursor_reply(rid, &ns, batch, false))
        }
        "killCursors" => {
            let ids: Vec<i64> = body
                .get_array("cursors")
                .map(|a| a.iter().filter_map(as_i64).collect())
                .unwrap_or_default();
            let (mut killed, mut missing) = (Vec::new(), Vec::new());
            for id in ids {
                let ok = c.shared.cursors.get(id, &owner(c)).is_ok() && c.shared.cursors.remove(id);
                if ok {
                    killed.push(id);
                } else {
                    missing.push(id);
                }
            }
            Ok(
                doc! {"cursorsKilled": killed, "cursorsNotFound": missing, "cursorsAlive": [], "cursorsUnknown": [], "ok": 1.0},
            )
        }
        "count" => {
            let ns = ns_of("count")?;
            let q = body.get_document("query").cloned().unwrap_or_default();
            let n = engine
                .count(
                    sql,
                    &ns,
                    &q,
                    u64::try_from(i64_of(body, "skip").unwrap_or(0).max(0)).unwrap_or(0),
                    i64_of(body, "limit").unwrap_or(0).unsigned_abs(),
                )
                .await?;
            Ok(
                doc! {"n": i32::try_from(n).map_or(Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)), Bson::Int32), "ok": 1.0},
            )
        }
        "distinct" => {
            let ns = ns_of("distinct")?;
            let key = body
                .get_str("key")
                .map_err(|_| MongoError::new(code::FAILED_TO_PARSE, "distinct needs a key"))?;
            let q = body.get_document("query").cloned().unwrap_or_default();
            let vals = engine.distinct(sql, &ns, key, &q).await?;
            Ok(doc! {"values": vals, "ok": 1.0})
        }
        "aggregate" => {
            let coll = match body.get("aggregate") {
                Some(Bson::String(s)) => s.clone(),
                Some(x) if as_i64(x) == Some(1) => String::new(),
                _ => {
                    return Err(MongoError::new(
                        code::INVALID_NAMESPACE,
                        "aggregate needs a collection name or 1",
                    ));
                }
            };
            let pipeline = docs_of(body, "pipeline");
            if body.contains_key("explain") || body.get_bool("explain").unwrap_or(false) {
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    "explain is not supported in this release",
                ));
            }
            let ns = Ns {
                db: db.to_owned(),
                coll: coll.clone(),
            };
            let docs = engine.aggregate(sql, &ns, &pipeline).await?;
            let plan = sprig_doc::engine::CursorPlan::from_docs(ns.clone(), docs);
            open_cursor(
                &c.shared,
                owner(c),
                sql,
                plan,
                if coll.is_empty() {
                    format!("{db}.$cmd.aggregate")
                } else {
                    ns.to_string()
                },
                body,
                lsid,
                first_batch(body, 101),
                false,
            )
            .await
        }
        "createIndexes" => {
            let ns = ns_of("createIndexes")?;
            let specs = docs_of(body, "indexes");
            if specs.is_empty() {
                return Err(MongoError::new(
                    code::BAD_VALUE,
                    "Must specify at least one index to create",
                ));
            }
            let migrated = c.restore
                || body
                    .get_document("sprig")
                    .ok()
                    .and_then(|s| s.get_str("origin").ok())
                    == Some("migrated");
            let (before, after, created) =
                engine.create_indexes(sql, &ns, &specs, migrated).await?;
            Ok(
                doc! {"numIndexesBefore": i32::try_from(before).unwrap_or(0), "numIndexesAfter": i32::try_from(after).unwrap_or(0), "createdCollectionAutomatically": created, "ok": 1.0},
            )
        }
        "listIndexes" => {
            let ns = ns_of("listIndexes")?;
            let Some(list) = engine.list_indexes(sql, &ns).await? else {
                return Err(MongoError::new(
                    code::NAMESPACE_NOT_FOUND,
                    format!("ns does not exist: {ns}"),
                ));
            };
            let plan = sprig_doc::engine::CursorPlan::from_docs(ns.clone(), list);
            open_cursor(
                &c.shared,
                owner(c),
                sql,
                plan,
                ns.to_string(),
                body,
                lsid,
                first_batch(body, 101),
                false,
            )
            .await
        }
        "dropIndexes" | "deleteIndexes" => {
            let ns = ns_of(name)?;
            let which = body.get("index").cloned().ok_or_else(|| {
                MongoError::new(code::FAILED_TO_PARSE, "dropIndexes needs 'index'")
            })?;
            let was = engine.drop_indexes(sql, &ns, &which).await?;
            Ok(doc! {"nIndexesWas": i32::try_from(was).unwrap_or(0), "ok": 1.0})
        }
        "dbStats" => Ok(
            doc! {"db": db, "collections": i64::try_from(engine.list_collections(sql, db, &doc! {}, true).await?.len()).unwrap_or(i64::MAX), "objects": 0i64, "dataSize": 0.0, "storageSize": 0.0, "ok": 1.0},
        ),
        "collStats" => Ok(
            doc! {"ns": format!("{db}.{}", coll_of(body, "collStats")?), "count": i64::try_from(engine.count(sql, &ns_of("collStats")?, &doc! {}, 0, 0).await?).unwrap_or(i64::MAX), "size": 0i64, "ok": 1.0},
        ),
        other => Err(MongoError::new(
            code::COMMAND_NOT_FOUND,
            format!("no such command: '{other}'"),
        )),
    }
}

/// Codes not in the shared table.
mod code {
    pub(crate) use sprig_proto_mongo::errors::code::*;
    pub(crate) const INVALID_LENGTH: i32 = 16;
}
