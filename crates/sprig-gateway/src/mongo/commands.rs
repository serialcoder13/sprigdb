//! Command handlers.
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
    /// The databases it wrote: their records hold its outcome.
    dbs: std::collections::BTreeSet<String>,
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

    /// Open transactions other than session `except`'s (for 9.0's
    /// `TooManyOpenTransactions`); an entry in use counts as open.
    async fn open_count(&self, except: &[u8]) -> usize {
        let entries: Vec<Arc<tokio::sync::Mutex<TxnEntry>>> = self
            .map
            .lock()
            .await
            .iter()
            .filter(|(k, _)| k.as_slice() != except)
            .map(|(_, v)| Arc::clone(v))
            .collect();
        entries
            .iter()
            .filter(|e| e.try_lock().map_or(true, |g| g.state == 0))
            .count()
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

    /// Rolls back every open transaction of `who` (`killAllSessions`).
    async fn kill_owner(&self, who: &Owner) {
        let entries: Vec<Arc<tokio::sync::Mutex<TxnEntry>>> =
            self.map.lock().await.values().cloned().collect();
        for e in entries {
            let mut g = e.lock().await;
            if g.owner == *who && g.state == 0 {
                if let Some(mut s) = g.sql.take() {
                    let _ = s.exec("ROLLBACK").await;
                    s.close().await;
                }
                g.state = 2;
            }
        }
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

/// The command with its `let` variables bound: each `$$name` the command's
/// filters, pipelines and pipeline updates read becomes the value's
/// `$literal`. `None` when it has no `let`.
///
/// # Errors
/// The `let` expressions' own.
pub(super) fn bind_lets(name: &str, body: &Document) -> Result<Option<Document>, MongoError> {
    use sprig_doc::agg::{bind_filter, bind_pipeline, let_values};
    let Ok(lets) = body.get_document("let") else {
        return Ok(None);
    };
    let lets = let_values(lets, bson::DateTime::now())?;
    let filter = |d: &mut Document, k: &str| {
        if let Ok(f) = d.get_document(k) {
            let f = bind_filter(f, &lets);
            d.insert(k, f);
        }
    };
    let pipeline = |d: &mut Document, k: &str| {
        if let Ok(p) = d.get_array(k) {
            let p: Vec<Document> = p.iter().filter_map(|x| x.as_document().cloned()).collect();
            let p: Vec<Bson> = bind_pipeline(&p, &lets)
                .into_iter()
                .map(Bson::Document)
                .collect();
            d.insert(k, p);
        }
    };
    let mut b = body.clone();
    match name {
        "find" => filter(&mut b, "filter"),
        "count" | "distinct" => filter(&mut b, "query"),
        "aggregate" => pipeline(&mut b, "pipeline"),
        "findAndModify" | "findandmodify" => {
            filter(&mut b, "query");
            pipeline(&mut b, "update");
        }
        "update" | "delete" | "bulkWrite" => {
            let list = match name {
                "update" => "updates",
                "delete" => "deletes",
                _ => "ops",
            };
            if let Ok(a) = b.get_array(list) {
                let a: Vec<Bson> = a
                    .iter()
                    .map(|x| match x {
                        Bson::Document(d) => {
                            let mut d = d.clone();
                            filter(&mut d, "q");
                            filter(&mut d, "filter");
                            pipeline(&mut d, "u");
                            pipeline(&mut d, "updateMods");
                            Bson::Document(d)
                        }
                        o => o.clone(),
                    })
                    .collect();
                b.insert(list, a);
            }
        }
        _ => return Ok(None),
    }
    b.remove("let");
    Ok(Some(b))
}

/// Runs a command after authentication.
///
/// # Errors
/// The command's.
pub async fn run(c: &mut Conn, name: &str, body: &Document) -> Result<Document, MongoError> {
    let bound = bind_lets(name, body)?;
    let body = bound.as_ref().unwrap_or(body);
    // The command's collation: every comparison it makes.
    // Writes carry theirs per statement (`writes`).
    let coll = sprig_doc::collation::Collation::of(body)?;
    // Server-side JavaScript: its scope, and the first error its code met,
    // which the command answers with.
    let js = super::serverjs::scope_for(c, name, body).await;
    // The command's state is boxed once; the task-local scopes around it
    // move only the box (moving the whole state through each scope was
    // about 9% of a point read's gateway CPU, in `memmove`).
    let r = sprig_doc::serverjs::scope(
        js.clone(),
        sprig_doc::collation::scope(coll, Box::pin(run_inner(c, name, body))),
    )
    .await;
    match js.and_then(|s| s.take_error()) {
        Some(e) => Err(e),
        None => r,
    }
}

async fn run_inner(c: &mut Conn, name: &str, body: &Document) -> Result<Document, MongoError> {
    let db = body.get_str("$db").unwrap_or("admin").to_owned();
    if let Ok(api) = body.get_str("apiVersion")
        && api != "1"
    {
        return Err(MongoError::new(
            322,
            format!("API version must be \"1\" but got \"{api}\""),
        ));
    }
    // User and role commands.
    if super::users::handles(name) {
        if c.sql.is_none() {
            c.sql()?;
        }
        return super::users::command(c, name, body, &db).await;
    }
    // Commands that touch no data.
    match name {
        "ping" => return Ok(doc! {"ok": 1.0}),
        "buildInfo" | "buildinfo" => {
            let v = c.shared.settings().server_version.clone();
            let parts: Vec<i32> = v.split('.').filter_map(|x| x.parse().ok()).collect();
            return Ok(
                doc! {"version": v.clone(), "versionArray": [parts.first().copied().unwrap_or(8), parts.get(1).copied().unwrap_or(0), parts.get(2).copied().unwrap_or(0), 0], "gitVersion": "sprigdb", "bits": 64, "maxBsonObjectSize": 16_777_216, "sprig": {"gateway": c.shared.d.gateway_id.clone()}, "ok": 1.0},
            );
        }
        // With MongoDB 8.1's `authInfo.UUID`.
        "connectionStatus" => return Ok(super::status::connection_status(c)),
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
        // `none: 1` and change-stream metrics.
        "serverStatus" => return Ok(super::status::server_status(c, body)),
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
        "killAllSessions" => {
            // The caller's own sessions on this gateway (not other users',
            // which MongoDB's killAnySession privilege would allow): their
            // open transactions roll back.
            let who = owner(c);
            c.shared.txns.kill_owner(&who).await;
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
        "mapReduce" | "eval" | "$eval" | "geoNear" | "shardCollection" | "enableSharding" => {
            if matches!(name, "shardCollection" | "enableSharding") {
                return Ok(
                    doc! {"ok": 1.0, "note": "recorded as a hint only: SprigDB shards collections itself"},
                );
            }
            return Err(MongoError::new(
                code::COMMAND_NOT_SUPPORTED,
                format!("{name} is not supported in this release"),
            ));
        }
        _ => {}
    }
    // One-time work on the database (old metadata moving into the
    // catalog), outside any transaction.
    if !matches!(
        name,
        "listDatabases"
            | "endSessions"
            | "killSessions"
            | "commitTransaction"
            | "abortTransaction"
            | "getMore"
            | "killCursors"
    ) && db != "admin"
    {
        let engine = c.shared.engine.clone();
        engine.catalog.prepare_db(c.sql()?, &db).await?;
    }
    if name == "bulkWrite" {
        let engine = c.shared.engine.clone();
        for d in super::writes::target_dbs(name, body, &db) {
            engine.catalog.prepare_db(c.sql()?, &d).await?;
        }
    }
    // DDL ends the tenant's snapshot reads first (their open transactions
    // hold the tables' metadata locks; `snapshot`).
    if matches!(
        name,
        "drop"
            | "dropDatabase"
            | "create"
            | "createIndexes"
            | "dropIndexes"
            | "renameCollection"
            | "collMod"
            | "convertToCapped"
            | "cloneCollectionAsCapped"
    ) {
        c.shared.snapshots.close_tenant(owner(c).tenant).await;
    }
    // Transactions.
    let lsid = lsid_of(body);
    let autocommit_false = body.get_bool("autocommit") == Ok(false);
    // Snapshot reads outside transactions: one point in time across
    // commands (`atClusterTime`), kept by `snapshot`.
    if !autocommit_false
        && body
            .get_document("readConcern")
            .ok()
            .and_then(|r| r.get_str("level").ok())
            == Some("snapshot")
    {
        if !super::snapshot::reads(name, body) {
            return Err(MongoError::new(
                code::INVALID_OPTIONS,
                format!(
                    "readConcern level snapshot is not supported for {name} outside multi-document transactions"
                ),
            ));
        }
        return snapshot_read(c, name, body, &db, lsid.as_deref()).await;
    }
    let txn_number = i64_of(body, "txnNumber");
    if name == "commitTransaction" || name == "abortTransaction" {
        let (Some(l), Some(n)) = (lsid.as_ref(), txn_number) else {
            return Err(no_txn());
        };
        let commit = name == "commitTransaction";
        let who = owner(c);
        let uid = super::retry::uid_of(&who);
        let mut dbs = super::txn::token_dbs(body, who.tenant);
        if let Some(e) = c.shared.txns.get(l).await {
            let mut g = e.lock().await;
            if g.owner != who {
                return Err(no_txn());
            }
            if g.txn_number == n {
                match (g.state, commit) {
                    (1, true) | (2, false) => return Ok(doc! {"ok": 1.0}),
                    (0, _) => {
                        let mut s = g.sql.take().ok_or_else(no_txn)?;
                        // The outcome commits with the writes.
                        let r = if commit {
                            match super::txn::commit_records(&mut s, &g.dbs, l, &uid, n).await {
                                Ok(()) => s.exec("COMMIT").await.map(|_| ()).map_err(|e| {
                                    sprig_doc::engine::mongo_error(&e)
                                        .label("UnknownTransactionCommitResult")
                                }),
                                Err(e) => {
                                    let _ = s.exec("ROLLBACK").await;
                                    Err(e)
                                }
                            }
                        } else {
                            let _ = s.exec("ROLLBACK").await;
                            Ok(())
                        };
                        s.close().await;
                        g.state = if commit && r.is_ok() { 1 } else { 2 };
                        metrics::counter!("sprig_mongo_transactions_total", "outcome" => if g.state == 1 { "committed" } else { "aborted" }).increment(1);
                        return r.map(|()| doc! {"ok": 1.0});
                    }
                    // A commit after a failed COMMIT, or an abort after a
                    // commit: the records know.
                    _ => dbs = Some(g.dbs.clone()),
                }
            }
        }
        // Not open here: another gateway ran it, or this one restarted.
        let Some(dbs) = dbs else {
            return Err(no_txn());
        };
        return super::txn::recover(c.sql()?, commit, &dbs, l, &uid, n).await;
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
                    | "convertToCapped"
                    | "cloneCollectionAsCapped"
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
            // MongoDB 9.0's limit on open transactions.
            let limit = c.shared.settings().max_open_transactions;
            if c.shared.txns.open_count(&l).await >= limit {
                metrics::counter!("sprig_mongo_transactions_total", "outcome" => "refused")
                    .increment(1);
                return Err(MongoError::new(
                    code::TOO_MANY_OPEN_TRANSACTIONS,
                    format!(
                        "Too many open transactions: this gateway holds {limit} (mongo.max_open_transactions)"
                    ),
                ));
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
            s.set_client_dc(c.client_dc.clone());
            let snapshot = body
                .get_document("readConcern")
                .ok()
                .and_then(|r| r.get_str("level").ok())
                == Some("snapshot");
            // Conflicts fail fast inside transactions, as in MongoDB
            // (`mongo.txn_lock_wait_seconds`): 1205 becomes 112
            // WriteConflict with TransientTransactionError.
            if let Err(e) = s.exec("SET SESSION innodb_lock_wait_timeout = 1").await {
                tracing::debug!(error = %e.message, "lock wait for a transaction not set");
            }
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
                        dbs: std::collections::BTreeSet::new(),
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
            let engine = c.shared.engine.clone();
            let sql = c.sql()?;
            if engine.meta(sql, &ns).await?.is_none() {
                engine.create(sql, &ns, Document::new(), false).await?;
            }
            if name == "create" {
                return Ok(doc! {"ok": 1.0});
            }
        }
        // As MongoDB 8.0, no writes to capped collections inside
        // transactions.
        if matches!(
            name,
            "insert" | "update" | "delete" | "findAndModify" | "findandmodify"
        ) {
            let ns = Ns {
                db: db.clone(),
                coll: coll_of(body, name)?,
            };
            let engine = c.shared.engine.clone();
            if engine
                .meta(c.sql()?, &ns)
                .await?
                .is_some_and(|m| sprig_doc::engine::capped::Cap::of(&m.options).is_some())
            {
                // The error aborts the transaction, as in MongoDB.
                let mut g = entry.lock().await;
                if let Some(mut s) = g.sql.take() {
                    let _ = s.exec("ROLLBACK").await;
                    s.close().await;
                }
                g.state = 2;
                return Err(MongoError::new(
                    code::OPERATION_NOT_SUPPORTED_IN_TRANSACTION,
                    format!(
                        "Collection '{ns}' is a capped collection. Writes in transactions are not allowed on capped collections."
                    ),
                ));
            }
        }
        if name == "bulkWrite" {
            let engine = c.shared.engine.clone();
            for ns in super::writes::created_namespaces(name, body, &db) {
                let sql = c.sql()?;
                if engine.meta(sql, &ns).await?.is_none() {
                    engine.create(sql, &ns, Document::new(), false).await?;
                }
            }
        }
        // The records tables of the databases it writes, for its
        // outcome; DDL, so before the statement, outside it.
        let wdbs = super::writes::target_dbs(name, body, &db);
        let shared = Arc::clone(&c.shared);
        let tenant = owner(c).tenant;
        for d in &wdbs {
            super::retry::ensure_table(&shared.records, c.sql()?, &(tenant, d.clone())).await?;
        }
        let mut g = entry.lock().await;
        let mut s = g.sql.take().ok_or_else(no_txn)?;
        let r = exec(c, &mut s, name, body, &db, lsid.as_deref()).await;
        g.sql = Some(s);
        g.dbs.extend(wdbs);
        let token = super::txn::token(tenant, &c.shared.d.gateway_id, &g.dbs);
        return r
            .map(|mut d| {
                d.insert("recoveryToken", token);
                d
            })
            .map_err(|e| {
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
    // Query settings: a rejected shape stops here; a
    // per-shape maxTimeMS bounds the command's statements.
    let tenant = owner(c).tenant;
    let applied = match super::qsettings::before(&mut s, tenant, name, body, &db).await {
        Ok(a) => a,
        Err(e) => {
            c.sql = Some(s);
            return Err(e);
        }
    };
    // The command's own `maxTimeMS` and 9.0's per-shape one: the
    // smaller bounds every statement; past its own, the command answers
    // 50 even when its statements finished.
    let own_max = super::qsettings::own_max_time(name, body);
    let max_time_ms = match (applied.max_time_ms, own_max) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    if let Some(ms) = max_time_ms
        && let Err(e) = super::qsettings::set_max_time(&mut s, ms).await
    {
        c.sql = Some(s);
        return Err(e);
    }
    // Index hints: the command's `hint`, else query settings'
    // first usable `indexHints` entry, as MariaDB's FORCE INDEX.
    let engine = c.shared.engine.clone();
    let hint = match hint_for(&engine, &mut s, name, body, &db, &applied).await {
        Ok(h) => h,
        Err(e) => {
            if max_time_ms.is_some() {
                super::qsettings::reset_max_time(&mut s).await;
            }
            c.sql = Some(s);
            return Err(e);
        }
    };
    // The workload store (`Fingerprinter::mongo`).
    let wl = super::workload::start(&c.shared.engine, &mut s, tenant, name, body, &db).await;
    let started = Instant::now();
    // Retryable writes: the writes and their record
    // in one MariaDB transaction.
    let mut tries = 0u64;
    let r = super::workload::scoped(
        wl.as_ref(),
        sprig_doc::hint::scope(
            hint,
            Box::pin(async {
                loop {
                    let r = match (&lsid, txn_number) {
                        (Some(l), Some(n)) if super::retry::applies(name, body) => {
                            Box::pin(super::retry::run(c, &mut s, name, body, &db, l, n)).await
                        }
                        _ => {
                            // MongoDB 9.0's per-operation memory limit.
                            let mem = c.shared.settings().max_operation_memory_bytes;
                            sprig_doc::opmem::scope(
                                mem,
                                Box::pin(exec(c, &mut s, name, body, &db, lsid.as_deref())),
                            )
                            .await
                        }
                    };
                    // Concurrent first writes to a new database: another session
                    // created it a moment ago, and this one's names may not show it
                    // yet (the name map follows the catalog's watch).
                    match &r {
                        Err(e)
                            if e.code == code::NAMESPACE_NOT_FOUND
                                && e.message.contains("Unknown database")
                                && super::writes::is_write(name)
                                && tries < 8 =>
                        {
                            tries += 1;
                            tokio::time::sleep(Duration::from_millis(50 * tries)).await;
                        }
                        _ => break r,
                    }
                }
            }),
        ),
    )
    .await;
    let r = if max_time_ms.is_some() {
        super::qsettings::reset_max_time(&mut s).await;
        r.map_err(super::qsettings::map_timeout)
    } else {
        r
    };
    let r = match (r, own_max) {
        (Ok(_), Some(ms)) if started.elapsed() > Duration::from_millis(ms.unsigned_abs()) => Err(
            MongoError::new(code::MAX_TIME_MS_EXPIRED, "operation exceeded time limit"),
        ),
        (r, _) => r,
    };
    c.sql = Some(s);
    // A point read answers with raw bytes (`Conn::raw`): count them.
    let raw_n = c.raw.as_ref().and_then(|raw| {
        let cur = raw.get_document("cursor").ok()?;
        let b = cur.get_array("firstBatch").ok()?;
        i64::try_from(b.into_iter().count()).ok()
    });
    if let Some(st) = wl {
        super::workload::finish(&st, &r, raw_n, name);
    }
    // `$queryStats`.
    if let Ok(d) = &r {
        super::qsettings::record(
            tenant,
            name,
            body,
            &db,
            c.client_meta.as_ref(),
            started.elapsed(),
            d,
            raw_n,
        );
    }
    // `$collStats.latencyStats`.
    super::diag::record_latency(tenant, name, body, &db, started.elapsed());
    r
}

/// The MariaDB index a command is hinted onto: its own `hint` (`find`,
/// `count`, `distinct`, `aggregate`; an unknown index fails with 2), else
/// the first of query settings' `indexHints` that exists.
async fn hint_for(
    engine: &sprig_doc::engine::Engine,
    s: &mut super::sql::SqlSession,
    name: &str,
    body: &Document,
    db: &str,
    applied: &super::qsettings::Applied,
) -> Result<Option<String>, MongoError> {
    if !matches!(name, "find" | "count" | "distinct" | "aggregate") {
        return Ok(None);
    }
    let Ok(coll) = body.get_str(name) else {
        return Ok(None);
    };
    let ns = Ns {
        db: db.to_owned(),
        coll: coll.to_owned(),
    };
    if let Some(h) = body.get("hint").filter(|h| match h {
        Bson::String(x) => !x.is_empty(),
        Bson::Document(d) => !d.is_empty(),
        _ => true,
    }) {
        return super::qsettings::hint_index(engine, s, &ns, h).await;
    }
    for h in &applied.index_hints {
        if let Ok(i) = super::qsettings::hint_index(engine, s, &ns, h).await {
            return Ok(i);
        }
    }
    Ok(None)
}

/// A read at a snapshot (`snapshot`): on the snapshot's own executor
/// session, its time in the reply.
async fn snapshot_read(
    c: &mut Conn,
    name: &str,
    body: &Document,
    db: &str,
    lsid: Option<&[u8]>,
) -> Result<Document, MongoError> {
    let who = owner(c);
    let entry = if let Some(at) = super::snapshot::at_of(body) {
        c.shared.snapshots.get(&who, at).await?
    } else {
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
        s.set_client_dc(c.client_dc.clone());
        if let Err(e) = s
            .exec("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            .await
        {
            s.close().await;
            return Err(sprig_doc::engine::mongo_error(&e));
        }
        s.in_txn = true;
        let at = super::conn::cluster_time();
        c.shared.snapshots.put(who, at, s).await
    };
    let mut g = entry.lock().await;
    let at = g.at;
    let mut s = g.sql.take().ok_or_else(|| {
        MongoError::new(super::snapshot::SNAPSHOT_TOO_OLD, "the snapshot was closed")
    })?;
    let r = exec(c, &mut s, name, body, db, lsid).await;
    g.sql = Some(s);
    let mut d = r?;
    super::snapshot::stamp(&mut d, at);
    Ok(d)
}

/// `find`'s sort, or a `{$natural: ±1}` hint as one.
fn sort_or_natural_hint(body: &Document) -> Option<Document> {
    body.get_document("sort")
        .ok()
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| {
            body.get_document("hint")
                .ok()
                .filter(|h| h.len() == 1 && h.contains_key("$natural"))
                .cloned()
        })
}

/// `find` with `tailable`: MongoDB 8.0's checks, then a cursor in the
/// capped collection's insertion order that stays open at the end. A cursor
/// that read no row (an empty collection) closes after its first batch, as
/// MongoDB's does.
async fn find_tailable(
    c: &mut Conn,
    sql: &mut SqlSession,
    ns: &Ns,
    body: &Document,
    lsid: Option<&[u8]>,
) -> Result<Document, MongoError> {
    let tailable = body.get_bool("tailable").unwrap_or(false);
    let await_data = body.get_bool("awaitData").unwrap_or(false);
    if !tailable {
        if await_data {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "Cannot set 'awaitData' without also setting 'tailable'",
            ));
        }
        // `awaitData: false` alone: a plain find.
        let mut b = body.clone();
        b.remove("awaitData");
        return Box::pin(exec(c, sql, "find", &b, &ns.db, lsid)).await;
    }
    if body.get_bool("singleBatch").unwrap_or(false) {
        return Err(MongoError::new(
            code::BAD_VALUE,
            "cannot use tailable option with the 'singleBatch' option",
        ));
    }
    let limit = i64_of(body, "limit").unwrap_or(0);
    if limit < 0 {
        return Err(MongoError::new(
            code::BAD_VALUE,
            format!("BSON field 'limit' value must be >= 0, actual value '{limit}'"),
        ));
    }
    if let Ok(s) = body.get_document("sort")
        && !s.is_empty()
        && sprig_doc::engine::capped::natural_of(Some(s))
            .ok()
            .flatten()
            != Some(1)
    {
        return Err(MongoError::new(
            code::BAD_VALUE,
            "cannot use tailable option with a sort other than {$natural: 1}",
        ));
    }
    let engine = c.shared.engine.clone();
    let reply_ns = ns.to_string();
    if !engine.check_tailable(sql, ns).await? {
        return Ok(cursor_reply(0, &reply_ns, Vec::new(), true));
    }
    let spec = FindSpec {
        filter: body.get_document("filter").cloned().unwrap_or_default(),
        sort: None,
        projection: body.get_document("projection").ok().cloned(),
        skip: u64::try_from(i64_of(body, "skip").unwrap_or(0).max(0)).unwrap_or(0),
        limit: limit.unsigned_abs(),
    };
    let mut plan = engine.find(sql, ns, spec).await?;
    plan.make_tailable();
    let mut first = first_batch(body, 101);
    if limit != 0 {
        first = first.min(usize::try_from(limit).unwrap_or(usize::MAX));
    }
    let batch = if first == 0 {
        Vec::new()
    } else {
        engine
            .next_batch(sql, &mut plan, first, BATCH_BYTES)
            .await?
    };
    let open = if first == 0 {
        engine.count(sql, ns, &doc! {}, 0, 1).await? > 0
    } else {
        plan.tail_positioned()
    };
    if plan.exhausted || !open {
        return Ok(cursor_reply(0, &reply_ns, batch, true));
    }
    let bs = first_batch(body, 0);
    let id = c.shared.cursors.open(
        plan,
        reply_ns.clone(),
        owner(c),
        lsid.map(<[u8]>::to_vec),
        (bs > 0).then_some(bs),
        body.get_bool("noCursorTimeout").unwrap_or(false),
        c.shared.settings().max_cursors,
    )?;
    if await_data {
        c.shared.cursors.set_await_data(id);
    }
    Ok(cursor_reply(id, &reply_ns, batch, true))
}

/// A tailable cursor's next batch: its position checked (136 when
/// its row is gone), then the rows after it; while there are none, waits
/// up to `wait` for inserts (signalled on this gateway, polled every
/// 100 ms for inserts through other gateways).
async fn tail_batch(
    engine: &sprig_doc::engine::Engine,
    sql: &mut SqlSession,
    plan: &mut sprig_doc::engine::CursorPlan,
    n: usize,
    wait: Duration,
) -> Result<Vec<Document>, MongoError> {
    let deadline = Instant::now() + wait;
    let tenant = sprig_doc::engine::SqlRunner::tenant(sql);
    loop {
        let probe = plan.tail_probe();
        engine.check_tail_position(sql, probe).await?;
        let batch = engine.next_batch(sql, plan, n, BATCH_BYTES).await?;
        let now = Instant::now();
        if !batch.is_empty() || plan.exhausted || now >= deadline {
            return Ok(batch);
        }
        let ns = plan.ns().clone();
        sprig_doc::engine::capped::wait_insert(
            tenant,
            &ns,
            (deadline - now).min(Duration::from_millis(100)),
        )
        .await;
    }
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

/// `aggregate` with `$changeStream`.
async fn open_change_stream(
    c: &mut Conn,
    sql: &mut SqlSession,
    body: &Document,
    db: &str,
    coll: &str,
    stage: &Document,
    rest: &[Document],
    lsid: Option<&[u8]>,
) -> Result<Document, MongoError> {
    use super::changes::{StreamSpec, check_pipeline, parse_stage, token_doc};
    let (full, before, resume, all) = parse_stage(stage)?;
    check_pipeline(rest)?;
    let Some(feed) = c.shared.feed.as_ref() else {
        return Err(MongoError::new(
            code::COMMAND_NOT_SUPPORTED,
            "change streams need the tenant catalog (catalog logins)",
        ));
    };
    let who = owner(c);
    if db == "admin" && (!all || !coll.is_empty()) {
        return Err(MongoError::new(
            code::INVALID_OPTIONS,
            "$changeStream on admin must be {aggregate: 1} with allChangesForCluster: true",
        ));
    }
    if db != "admin" && all {
        return Err(MongoError::new(
            code::INVALID_OPTIONS,
            "allChangesForCluster is only for the admin database",
        ));
    }
    // Privileges: reading the collection, or the database, as SQL would.
    let engine = c.shared.engine.clone();
    let coll_spec = if coll.is_empty() {
        None
    } else {
        let ns = Ns {
            db: db.to_owned(),
            coll: coll.to_owned(),
        };
        let table = engine
            .meta(sql, &ns)
            .await?
            .map_or_else(|| sprig_doc::meta::table_name(coll), |m| m.table);
        Some((coll.to_owned(), table))
    };
    if db != "admin" {
        let probe = match &coll_spec {
            Some((_, t)) => format!(
                "SELECT 1 FROM {}.{} LIMIT 0",
                sprig_doc::sql::ident(db),
                sprig_doc::sql::ident(t)
            ),
            None => format!("SHOW TABLES FROM {}", sprig_doc::sql::ident(db)),
        };
        if let Err(e) = sql.query(&probe).await
            && !matches!(e.errno, Some(1146 | 1049))
        {
            return Err(sprig_doc::engine::mongo_error(&e));
        }
    }
    let visibility = sql.row_policy_docs();
    // A stream needs WATCH (MongoDB's `changeStream`) on what it watches,
    // besides `find`; a database or deployment stream leaves out the
    // collections without it (their events are hidden).
    if let (Some(v), Some((_, t))) = (&visibility, &coll_spec)
        && v.unwatched.contains(&(db.to_lowercase(), t.to_lowercase()))
    {
        return Err(MongoError::new(
            code::UNAUTHORIZED,
            format!(
                "not authorized on {db} to execute command {{ aggregate: \"{coll}\", pipeline: [ {{ $changeStream: {{}} }} ] }}: \
                 WATCH (changeStream) is required"
            ),
        ));
    }
    // A collection whose binding policies have no MongoDB form (written in
    // SQL) can't be evaluated per event.
    if let (Some(v), Some((_, t))) = (&visibility, &coll_spec)
        && matches!(
            v.tables.get(&(db.to_lowercase(), t.to_lowercase())),
            Some(crate::tenant::rls::DocPolicies::Hidden)
        )
    {
        return Err(MongoError::new(
            39366,
            "ER_SPRIG_RLS_UNSUPPORTED: a change stream over a collection whose row-level security \
             policies have no MongoDB form (create them with sprigCreatePolicy) isn't supported",
        ));
    }
    let spec = StreamSpec {
        tenant: sprig_common::ids::TenantId(who.tenant),
        db: (db != "admin").then(|| db.to_owned()),
        coll: coll_spec,
        tables: c
            .shared
            .docs
            .as_ref()
            .map(|d| Arc::clone(d) as Arc<dyn super::changes::TableNames>),
        full,
        before,
        pipeline: rest.to_vec(),
        expanded: super::changes::expanded_of(stage),
        visibility,
    };
    let stream = feed
        .open(spec, resume.as_ref())
        .await
        .inspect_err(super::status::stream_error)?;
    let token = stream.token.clone();
    let ns = if coll.is_empty() {
        format!("{db}.$cmd.aggregate")
    } else {
        format!("{db}.{coll}")
    };
    let bs = body
        .get_document("cursor")
        .ok()
        .and_then(|cd| cd.get("batchSize").and_then(as_i64))
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0);
    let id = c.shared.cursors.open_stream(
        stream,
        ns.clone(),
        who,
        lsid.map(<[u8]>::to_vec),
        bs,
        c.shared.settings().max_cursors,
    )?;
    // MongoDB 9.0's option counters.
    super::status::stream_options(stage, body);
    Ok(
        doc! {"cursor": {"firstBatch": [], "postBatchResumeToken": token_doc(&token), "id": id, "ns": ns}, "ok": 1.0},
    )
}

fn first_batch(body: &Document, default: usize) -> usize {
    body.get_document("cursor")
        .ok()
        .and_then(|cd| cd.get("batchSize").and_then(as_i64))
        .or_else(|| i64_of(body, "batchSize"))
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(default)
}

/// The fields a find may carry and still be answered by [`find_point`].
pub(super) const POINT_FIND_KEYS: [&str; 18] = [
    "find",
    "filter",
    "$db",
    "lsid",
    "$clusterTime",
    "$readPreference",
    "readConcern",
    "comment",
    "maxTimeMS",
    "limit",
    "singleBatch",
    "batchSize",
    "projection",
    "sort",
    "skip",
    "apiVersion",
    "apiStrict",
    "apiDeprecationErrors",
];

/// A find whose filter is `_id` alone, with no projection, sort, skip,
/// collation or `let`, outside transactions and snapshot reads, is answered
/// by
/// [`sprig_doc::engine::Engine::find_point_raw`]: the document's BSON is
/// written straight from its stored JSON into the reply, which goes into
/// `c.raw` (`true`). The reply is the one the general path gives: the
/// document, or none, in `firstBatch`, cursor id 0. Anything else, or a
/// filter that isn't a point here, is `false` and plans as usual.
async fn find_point(
    c: &mut Conn,
    sql: &mut SqlSession,
    ns: &Ns,
    body: &Document,
    limit: i64,
    single: bool,
) -> Result<bool, MongoError> {
    if sql.in_txn {
        return Ok(false);
    }
    if !body.keys().all(|k| POINT_FIND_KEYS.contains(&k.as_str()))
        || body.get_document("projection").is_ok_and(|p| !p.is_empty())
        || body.get_document("sort").is_ok_and(|p| !p.is_empty())
        || body.contains_key("projection") && body.get_document("projection").is_err()
        || body.contains_key("sort") && body.get_document("sort").is_err()
        || i64_of(body, "skip").unwrap_or(0) != 0
        || body
            .get_document("readConcern")
            .ok()
            .and_then(|r| r.get_str("level").ok())
            == Some("snapshot")
    {
        return Ok(false);
    }
    // The general path's cursor is exhausted after the first batch (id
    // 0) when a limit or `singleBatch` ends it, or when the batch has room
    // for more than one document.
    let first = first_batch(body, 101);
    if first == 0 || !(limit != 0 || single || first >= 2) {
        return Ok(false);
    }
    let Ok(filter) = body.get_document("filter") else {
        return Ok(false);
    };
    let engine = c.shared.engine.clone();
    let Some(found) = engine.find_point_raw(sql, ns, filter).await? else {
        return Ok(false);
    };
    let mut batch = bson::RawArrayBuf::new();
    if let Some(bytes) = found {
        let d = bson::RawDocumentBuf::from_bytes(bytes)
            .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.to_string()))?;
        batch.push(d);
    }
    let mut cursor = bson::RawDocumentBuf::new();
    cursor.append("firstBatch", batch);
    cursor.append("id", 0i64);
    cursor.append("ns", ns.to_string());
    let mut reply = bson::RawDocumentBuf::new();
    reply.append("cursor", cursor);
    c.raw = Some(reply);
    Ok(true)
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
    let engine = c.shared.engine.clone();
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
        // Capped collections from existing ones.
        "convertToCapped" => {
            let ns = ns_of("convertToCapped")?;
            engine.convert_to_capped(sql, &ns, body.get("size")).await?;
            Ok(doc! {"ok": 1.0})
        }
        "cloneCollectionAsCapped" => {
            let from = ns_of("cloneCollectionAsCapped")?;
            let to = Ns {
                db: db.to_owned(),
                coll: body
                    .get_str("toCollection")
                    .map_err(|_| bad("cloneCollectionAsCapped needs 'toCollection'"))?
                    .to_owned(),
            };
            sprig_doc::engine::check_coll_name(&to.coll)?;
            engine
                .clone_as_capped(sql, &from, &to, body.get("size"))
                .await?;
            Ok(doc! {"ok": 1.0})
        }
        "collMod" => {
            let ns = ns_of("collMod")?;
            let Some(mut m) = engine.meta(sql, &ns).await? else {
                return Err(MongoError::new(
                    code::NAMESPACE_NOT_FOUND,
                    format!("ns does not exist: {ns}"),
                ));
            };
            // Options kept with the collection: change
            // streams' pre- and post-images.
            if let Some(v) = body.get("changeStreamPreAndPostImages") {
                let Some(enabled) = v.as_document().and_then(|d| d.get_bool("enabled").ok()) else {
                    return Err(bad(
                        "changeStreamPreAndPostImages must be an object with a boolean 'enabled'",
                    ));
                };
                m.options
                    .insert("changeStreamPreAndPostImages", doc! {"enabled": enabled});
                engine.catalog.save_indexes(sql, &ns, &m).await?;
            }
            // TTL (wireup): `index: {name | keyPattern, expireAfterSeconds}`;
            // the worker's next pass uses the new value.
            let mut reply = doc! {};
            if let Some(ix) = body.get("index") {
                reply = sprig_doc::engine::ttl::coll_mod_index(&mut m, &ns, ix)?;
                engine.catalog.save_indexes(sql, &ns, &m).await?;
            }
            // `cappedSize` and `cappedMax`.
            if let Some(o) = sprig_doc::engine::capped::coll_mod(&m.options, body)? {
                m.options = o;
                engine.catalog.save_indexes(sql, &ns, &m).await?;
            }
            // Schema validation.
            if let Some(o) = Box::pin(sprig_doc::validate::coll_mod(
                &engine, sql, &ns, &m.options, body,
            ))
            .await?
            {
                m.options = o;
                engine.catalog.save_indexes(sql, &ns, &m).await?;
            }
            // A metadata-only table comment, so change streams with
            // `showExpandedEvents` see a `modify` event, as MongoDB's do.
            let mark = format!(
                "ALTER TABLE {}.{} COMMENT = '{}'",
                sprig_doc::sql::ident(&ns.db),
                sprig_doc::sql::ident(&m.table),
                super::changes::COLLMOD_MARK
            );
            if let Err(e) = sql.exec(&mark).await {
                tracing::debug!(error = %e.message, "collMod's table comment not set");
            }
            reply.insert("ok", 1.0);
            Ok(reply)
        }
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "bulkWrite" => {
            let done = sprig_doc::sessions::StmtResults::new();
            Ok(super::writes::run(&engine, sql, name, body, db, &done)
                .await?
                .reply)
        }
        "find" => {
            let ns = ns_of("find")?;
            let limit = i64_of(body, "limit").unwrap_or(0);
            let single = body.get_bool("singleBatch").unwrap_or(false) || limit < 0;
            // Tailable cursors on capped collections.
            if body.get_bool("tailable").unwrap_or(false) || body.contains_key("awaitData") {
                return find_tailable(c, sql, &ns, body, lsid).await;
            }
            // A find by `_id` alone answers from the
            // stored JSON as BSON bytes (`Conn::raw`).
            if find_point(c, sql, &ns, body, limit, single).await? {
                return Ok(Document::new());
            }
            let spec = FindSpec {
                filter: body.get_document("filter").cloned().unwrap_or_default(),
                sort: sort_or_natural_hint(body),
                projection: body.get_document("projection").ok().cloned(),
                skip: u64::try_from(i64_of(body, "skip").unwrap_or(0).max(0)).unwrap_or(0),
                limit: limit.unsigned_abs(),
            };
            // `allowDiskUse` lets a sort past `doc.max_sort_bytes`
            // spill (as MongoDB 8.0's find, true by default).
            let ctx =
                super::spill::ctx_for(body, "find", owner(c).tenant, c.shared.d.live.as_ref())?;
            let plan = sprig_doc::spill::scope(ctx, Box::pin(engine.find(sql, &ns, spec))).await?;
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
        // MongoDB's `explain` of searches.
        "explain" => {
            let cmd = body.get_document("explain").map_err(|_| {
                MongoError::new(code::FAILED_TO_PARSE, "explain needs a command document")
            })?;
            let verbosity = body.get_str("verbosity").unwrap_or("allPlansExecution");
            if !matches!(
                verbosity,
                "queryPlanner" | "executionStats" | "allPlansExecution"
            ) {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    format!(
                        "verbosity string must be one of {{'queryPlanner', 'executionStats', 'allPlansExecution'}}, got '{verbosity}'"
                    ),
                ));
            }
            explain(&engine, sql, cmd, db, verbosity).await
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
            if let Some(st) = cursor.stream.as_mut() {
                super::status::stream_get_more(body);
                // A change stream: wait up to maxTimeMS for events.
                let wait = Duration::from_millis(
                    i64_of(body, "maxTimeMS")
                        .and_then(|n| u64::try_from(n).ok())
                        .filter(|n| *n > 0)
                        .unwrap_or(1000),
                );
                let n = i64_of(body, "batchSize")
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n > 0)
                    .or(cursor.batch_size)
                    .unwrap_or(1000);
                let r = st.next_batch(n, wait).await;
                let token = st.token.clone();
                let done = st.done();
                let opened_with = st.policies;
                let ns = cursor.ns.clone();
                drop(g);
                // When the watcher's policies, roles or grants changed since
                // the stream opened, this batch was filtered by the old ones:
                // it is dropped and the stream ends with a resumable error,
                // so the driver resumes after the last token it returned,
                // under the new policies.
                let _ = sql.query("SELECT 1").await;
                let now =
                    crate::tenant::rls::DocViews::fingerprint(sql.row_policy_docs().as_deref());
                if now != opened_with {
                    c.shared.cursors.remove(id);
                    let e = MongoError::new(
                        234,
                        "row-level security for this change stream changed; resume it to apply \
                         the new policies",
                    )
                    .label("ResumableChangeStreamError");
                    super::status::stream_error(&e);
                    return Err(e);
                }
                let batch = match r {
                    Ok(b) => b,
                    Err(e) => {
                        c.shared.cursors.remove(id);
                        super::status::stream_error(&e);
                        return Err(e);
                    }
                };
                let rid = if done {
                    c.shared.cursors.remove(id);
                    0
                } else {
                    id
                };
                return Ok(
                    doc! {"cursor": {"nextBatch": batch.into_iter().map(Bson::Document).collect::<Vec<_>>(), "postBatchResumeToken": super::changes::token_doc(&token), "id": rid, "ns": ns}, "ok": 1.0},
                );
            }
            // MongoDB: only an awaitData cursor's getMore takes a
            // maxTimeMS (its wait).
            if body.contains_key("maxTimeMS") && !cursor.await_data {
                return Err(MongoError::new(
                    code::BAD_VALUE,
                    "cannot set maxTimeMS on getMore command for a non-awaitData cursor",
                ));
            }
            let n = i64_of(body, "batchSize")
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| *n > 0)
                .or(cursor.batch_size)
                .unwrap_or(1_000_000);
            let batch = if cursor.plan.is_tailable() {
                // A tailable cursor reads on from its position; with
                // awaitData it waits up to maxTimeMS (1 s by default).
                let wait = if cursor.await_data {
                    Duration::from_millis(
                        i64_of(body, "maxTimeMS")
                            .and_then(|n| u64::try_from(n).ok())
                            .filter(|n| *n > 0)
                            .unwrap_or(1000),
                    )
                } else {
                    Duration::ZERO
                };
                match tail_batch(&engine, sql, &mut cursor.plan, n, wait).await {
                    Ok(b) => b,
                    Err(e) => {
                        drop(g);
                        c.shared.cursors.remove(id);
                        return Err(e);
                    }
                }
            } else {
                engine
                    .next_batch(sql, &mut cursor.plan, n, BATCH_BYTES)
                    .await?
            };
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
            // `$where` is refused in aggregation.
            if let Ok(p) = body.get_array("pipeline") {
                sprig_doc::serverjs::check_pipeline(p)?;
                sprig_doc::engine::fusion::check_nested(p)?;
            }
            if let Some(st) = pipeline
                .first()
                .and_then(|s| s.get_document("$changeStream").ok())
            {
                return open_change_stream(c, sql, body, db, &coll, st, &pipeline[1..], lsid).await;
            }
            if body.contains_key("explain") || body.get_bool("explain").unwrap_or(false) {
                // `aggregate` with `explain: true`: queryPlanner, as MongoDB.
                let mut cmd = body.clone();
                cmd.remove("explain");
                return explain(&engine, sql, &cmd, db, "queryPlanner").await;
            }
            let ns = Ns {
                db: db.to_owned(),
                coll: coll.clone(),
            };
            let mut spilled = None;
            let process = super::diag::process_privilege(sql, pipeline.first()).await;
            let docs = if coll.is_empty()
                && pipeline
                    .first()
                    .is_some_and(super::qsettings::handles_stage)
            {
                // `$querySettings` and `$queryStats`, then the
                // in-gateway stages.
                // `$queryStats` adds the workload store's shapes.
                let stored = match (&c.shared.docs, pipeline[0].contains_key("$queryStats")) {
                    (Some(d), true) => {
                        let d = Arc::clone(d);
                        d.stored_shapes(sprig_common::ids::TenantId(owner(c).tenant))
                            .await
                            .unwrap_or_default()
                    }
                    _ => Vec::new(),
                };
                let mut docs = Box::pin(super::qsettings::stage_docs(
                    &engine,
                    sql,
                    owner(c).tenant,
                    db,
                    &pipeline[0],
                    stored,
                ))
                .await?;
                let lim = sprig_doc::agg::AggLimits {
                    stage_bytes: engine.agg_stage_bytes(),
                    now: bson::DateTime::now(),
                };
                for st in &pipeline[1..] {
                    docs = sprig_doc::agg::run_stage(docs, st, &lim)?;
                }
                docs
            } else if let Some(mut docs) = super::diag::first_docs(
                c,
                db,
                &coll,
                body,
                pipeline.first(),
                lsid.map(<[u8]>::to_vec),
                // `allUsers` needs `PROCESS`.
                process,
            )? {
                // `$currentOp` and `$listSessions`, then the
                // in-gateway stages.
                let lim = sprig_doc::agg::AggLimits {
                    stage_bytes: engine.agg_stage_bytes(),
                    now: bson::DateTime::now(),
                };
                for st in &pipeline[1..] {
                    docs = sprig_doc::agg::run_stage(docs, st, &lim)?;
                }
                docs
            } else if coll.is_empty()
                && pipeline
                    .first()
                    .is_some_and(|s| s.contains_key("$listLocalSessions"))
            {
                // `$listLocalSessions` (database-level): the caller's own
                // session on this gateway, then the in-gateway stages.
                let mut docs = Vec::new();
                if let Some(l) = lsid {
                    let who = owner(c);
                    docs.push(doc! {
                        "_id": {
                            "id": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: l.to_vec() },
                            "uid": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: super::retry::uid_of(&who).to_vec() },
                        },
                        "lastUse": bson::DateTime::now(),
                    });
                }
                let lim = sprig_doc::agg::AggLimits {
                    stage_bytes: engine.agg_stage_bytes(),
                    now: bson::DateTime::now(),
                };
                for st in &pipeline[1..] {
                    docs = sprig_doc::agg::run_stage(docs, st, &lim)?;
                }
                docs
            } else {
                // `allowDiskUse`: the result may be on disk.
                let ctx = super::spill::ctx_for(
                    body,
                    "aggregate",
                    owner(c).tenant,
                    c.shared.d.live.as_ref(),
                )?;
                // `$out`/`$merge` with `bypassDocumentValidation`.
                spilled = Some(
                    sprig_doc::validate::bypass(
                        body.get_bool("bypassDocumentValidation").unwrap_or(false),
                        sprig_doc::spill::scope(
                            ctx,
                            Box::pin(engine.aggregate_seq(sql, &ns, &pipeline)),
                        ),
                    )
                    .await?,
                );
                Vec::new()
            };
            let plan = match spilled {
                Some(seq) => sprig_doc::engine::CursorPlan::from_seq(ns.clone(), seq)?,
                None => sprig_doc::engine::CursorPlan::from_docs(ns.clone(), docs),
            };
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
            // Automatic indexes only when asked.
            let show_auto = body.get_bool("showAutoIndexes").unwrap_or(false);
            let Some(list) = engine.list_indexes_with(sql, &ns, show_auto).await? else {
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
        "collStats" => {
            let ns = ns_of("collStats")?;
            let n = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
            // A capped collection's size, `max` and `maxSize`.
            if let Some((_, count, bytes)) = engine.capped_stats(sql, &ns).await? {
                let o = engine
                    .meta(sql, &ns)
                    .await?
                    .map(|m| m.options)
                    .unwrap_or_default();
                return Ok(
                    doc! {"ns": ns.to_string(), "size": n(bytes), "count": n(count), "storageSize": n(bytes), "capped": true, "max": o.get("max").cloned().unwrap_or(Bson::Int32(0)), "maxSize": o.get("size").cloned().unwrap_or(Bson::Int32(0)), "ok": 1.0},
                );
            }
            Ok(
                doc! {"ns": ns.to_string(), "count": n(engine.count(sql, &ns, &doc! {}, 0, 0).await?), "size": 0i64, "capped": false, "ok": 1.0},
            )
        }
        // A logic-layer function exposed to `mongo`.
        "sprigCall" => super::logic::sprig_call(sql, body, db).await,
        // Row-level security on collections.
        n if super::rls::handles(n) => super::rls::command(&engine, sql, n, body, db).await,
        // Atlas's vector search index commands.
        n if super::search::handles(n) => super::search::command(&engine, sql, n, body, db).await,
        // Query settings.
        n if super::qsettings::handles(n) => {
            Box::pin(super::qsettings::command(
                &engine,
                sql,
                owner(c).tenant,
                n,
                body,
                db,
            ))
            .await
        }
        other => Err(MongoError::new(
            code::COMMAND_NOT_FOUND,
            format!("no such command: '{other}'"),
        )),
    }
}

/// `explain` of a command: searches here, the other reads and the writes
/// in [`super::explain`].
async fn explain(
    engine: &sprig_doc::engine::Engine,
    sql: &mut SqlSession,
    cmd: &Document,
    db: &str,
    verbosity: &str,
) -> Result<Document, MongoError> {
    if let Some(d) = engine.explain_search(sql, db, cmd, verbosity).await? {
        return Ok(d);
    }
    // The other reads and the writes.
    match super::explain::explain(engine, sql, cmd, db, verbosity).await? {
        Some(d) => Ok(d),
        None => Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "explain is supported for find, aggregate, count, distinct, update and delete in this release",
        )),
    }
}

/// Codes not in the shared table.
mod code {
    pub(crate) use sprig_proto_mongo::errors::code::*;
}
