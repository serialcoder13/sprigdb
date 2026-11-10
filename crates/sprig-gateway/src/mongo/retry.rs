//! Retryable writes, exactly once (chapter 30, "Retryable writes, exactly
//! once"; build step 7).
//!
//! A write command that carries `lsid` and `txnNumber` outside a
//! transaction runs in one MariaDB transaction on the connection's own
//! executor session, together with its record in the database's
//! `__sprig_doc_sessions` table ([`sprig_doc::sessions`]):
//!
//! ```text
//! 1. the collection exists first when the command may create it (DDL commits)
//! 2. START TRANSACTION
//! 3. SELECT the (lsid, uid) row FOR UPDATE
//!    row.txn_number > N: ROLLBACK, 225 TransactionTooOld
//!    row.txn_number = N: done := its statements, else none
//! 4. run the statements not in done (crate::mongo::writes); a deadlock, a
//!    lock wait timeout or a write conflict rolls everything back and runs
//!    the command again, up to mongo.write_conflict_retries times
//! 5. upsert the row with N and done plus the statements run now
//! 6. COMMIT: the writes and their record commit together or not at all
//! ```
//!
//! A retry after a lost reply therefore answers from the record: `n`,
//! `nModified`, upserted ids and `findAndModify`'s image equal the first
//! attempt's, and nothing runs twice. Rows older than
//! `mongo.session_record_retention_minutes` are deleted, at most once a
//! minute per database and gateway, after a retryable write commits there.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bson::{Bson, Document};
use sprig_doc::engine::{Done, Rows, SqlError, SqlRunner, mongo_error};
use sprig_doc::sessions::{self, Record, StmtResults};
use sprig_proto_mongo::errors::{MongoError, code};

use super::conn::Conn;
use super::cursors::Owner;
use super::sql::SqlSession;
use super::writes;

/// How often a database's old records are purged, per gateway.
const PURGE_EVERY: Duration = Duration::from_secs(60);
/// Rows per purge statement.
const PURGE_ROWS: u64 = 1000;

/// What the front end remembers about record tables.
#[derive(Debug, Default)]
pub struct Records {
    /// Databases (tenant, name) whose table is known to exist.
    tables: Mutex<HashMap<(u64, String), Instant>>,
}

impl Records {
    fn known(&self, k: &(u64, String)) -> bool {
        self.tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(k)
    }

    fn learn(&self, k: (u64, String)) {
        // An old instant makes the first write purge.
        let then = Instant::now()
            .checked_sub(PURGE_EVERY)
            .unwrap_or_else(Instant::now);
        self.tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(k)
            .or_insert(then);
    }

    fn forget(&self, k: &(u64, String)) {
        self.tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(k);
    }

    /// Whether `k` is due a purge; marks it purged.
    fn purge_due(&self, k: &(u64, String)) -> bool {
        let mut m = self
            .tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match m.get_mut(k) {
            Some(t) if t.elapsed() >= PURGE_EVERY => {
                *t = Instant::now();
                true
            }
            _ => false,
        }
    }
}

/// The `uid` of chapter 30: SHA-256 of the authenticated identity, so one
/// user can never read or continue another's records.
#[must_use]
pub fn uid_of(o: &Owner) -> [u8; 32] {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(&o.tenant.to_be_bytes());
    ctx.update(b"\0");
    ctx.update(o.user.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(ctx.finish().as_ref());
    out
}

/// The drivers' retryable codes (chapter 30, "Error codes and labels").
#[must_use]
pub fn retryable_code(c: i32) -> bool {
    matches!(
        c,
        6 | 7 | 89 | 91 | 189 | 262 | 9001 | 10107 | 11600 | 11602 | 13435 | 13436
    )
}

/// MongoDB's view of an error inside a retryable write: backend losses
/// become 6 `HostUnreachable`, and every retryable code gets the label
/// `RetryableWriteError`, as MongoDB servers attach it.
#[must_use]
pub fn labelled(e: MongoError) -> MongoError {
    let lost = matches!(e.code, 2002 | 2003 | 2006 | 2013 | 39030 | 39034 | 39100);
    let e = if lost {
        let mut m = MongoError::new(code::HOST_UNREACHABLE, e.message);
        m.labels = e.labels;
        m
    } else {
        e
    };
    if retryable_code(e.code) {
        e.label("RetryableWriteError")
    } else {
        e
    }
}

/// Errors after which the MariaDB transaction may be gone (rolled back),
/// so nothing else may run in it.
fn breaks_txn(e: &SqlError) -> bool {
    match e.errno {
        None => true,
        Some(n) => matches!(n, 1213 | 1205 | 1180 | 1181 | 2006 | 2013) || n >= 39000,
    }
}

/// The session, refusing every statement once one broke the transaction:
/// after a deadlock MariaDB has rolled back, and a later statement would
/// commit on its own.
struct Guarded<'a> {
    s: &'a mut SqlSession,
    broken: Option<SqlError>,
}

fn refused() -> SqlError {
    SqlError {
        errno: Some(1213),
        message: "the retryable write's transaction was rolled back".into(),
        mongo: None,
    }
}

impl Guarded<'_> {
    fn note(&mut self, r: Result<(), &SqlError>) {
        if let Err(e) = r
            && breaks_txn(e)
            && self.broken.is_none()
        {
            self.broken = Some((*e).clone());
        }
    }
}

#[async_trait]
impl SqlRunner for Guarded<'_> {
    async fn query(&mut self, sql: &str) -> Result<Rows, SqlError> {
        if self.broken.is_some() {
            return Err(refused());
        }
        let r = self.s.query(sql).await;
        self.note(r.as_ref().map(|_| ()));
        r
    }

    async fn exec(&mut self, sql: &str) -> Result<Done, SqlError> {
        if self.broken.is_some() {
            return Err(refused());
        }
        let r = self.s.exec(sql).await;
        self.note(r.as_ref().map(|_| ()));
        r
    }

    fn tenant(&self) -> u64 {
        self.s.tenant()
    }
}

fn conflict(e: &MongoError) -> bool {
    e.code == code::WRITE_CONFLICT
}

fn reply_conflict(d: &Document) -> bool {
    d.get_array("writeErrors").is_ok_and(|a| {
        a.iter().any(|w| {
            w.as_document()
                .and_then(|w| w.get_i32("code").ok())
                .is_some_and(|c| c == code::WRITE_CONFLICT)
        })
    })
}

async fn rollback(s: &mut SqlSession) {
    let _ = s.exec("ROLLBACK").await;
}

/// Makes sure `db`'s record table exists. `false` when the database
/// doesn't exist (a write that can't create anything, on nothing).
pub(super) async fn ensure_table(
    records: &Records,
    s: &mut SqlSession,
    key: &(u64, String),
) -> Result<bool, MongoError> {
    if records.known(key) {
        return Ok(true);
    }
    let db = &key.1;
    let probe = format!(
        "SELECT 1 FROM {}.{} LIMIT 0",
        sprig_doc::sql::ident(db),
        sprig_doc::sql::ident(sessions::SESSIONS_TABLE)
    );
    match s.query(&probe).await {
        Ok(_) => {}
        Err(e) if e.errno == Some(1049) => return Ok(false),
        Err(e) if e.errno == Some(1146) => match s.exec(&sessions::table_sql(db)).await {
            Ok(_) => {}
            Err(e) if e.errno == Some(1049) => return Ok(false),
            Err(e) => return Err(mongo_error(&e)),
        },
        Err(e) => return Err(mongo_error(&e)),
    }
    records.learn(key.clone());
    Ok(true)
}

/// Runs write command `name` as a retryable write of `txn_number` in
/// session `lsid`.
///
/// # Errors
/// 225 for a `txnNumber` older than the session's; the command's errors,
/// labelled for retry where drivers retry them.
pub async fn run(
    c: &mut Conn,
    s: &mut SqlSession,
    name: &str,
    body: &Document,
    db: &str,
    lsid: &[u8],
    txn_number: i64,
) -> Result<Document, MongoError> {
    run_inner(c, s, name, body, db, lsid, txn_number)
        .await
        .map_err(labelled)
}

#[allow(clippy::too_many_lines)]
async fn run_inner(
    c: &mut Conn,
    s: &mut SqlSession,
    name: &str,
    body: &Document,
    db: &str,
    lsid: &[u8],
    txn_number: i64,
) -> Result<Document, MongoError> {
    let engine = c.shared.engine.clone();
    let who = super::commands::owner(c);
    let uid = uid_of(&who);
    // `bulkWrite` (on `admin`) keeps its record with its first namespace.
    let request_db = db;
    let record_db = writes::record_db(name, body, db);
    let db = record_db.as_str();
    let key = (who.tenant, db.to_owned());
    // 1. Collections a statement may create exist before the transaction.
    for ns in writes::created_namespaces(name, body, request_db) {
        if engine.meta(s, &ns).await?.is_none() {
            engine.create(s, &ns, Document::new(), false).await?;
        }
    }
    if !ensure_table(&c.shared.records, s, &key).await? {
        // Nothing exists to write or to record.
        let done = StmtResults::new();
        return Ok(writes::run(&engine, s, name, body, request_db, &done)
            .await?
            .reply);
    }
    let cfg = c.shared.settings();
    let mut attempt = 0u32;
    let mut recreated = false;
    loop {
        attempt += 1;
        s.exec("START TRANSACTION")
            .await
            .map_err(|e| mongo_error(&e))?;
        // 3. The record, locked.
        let rows = match s.query(&sessions::select_sql(db, lsid, &uid)).await {
            Ok(r) => r,
            Err(e) if e.errno == Some(1146) && !recreated => {
                // Dropped with its database since we learned it.
                rollback(s).await;
                c.shared.records.forget(&key);
                recreated = true;
                if !ensure_table(&c.shared.records, s, &key).await? {
                    let done = StmtResults::new();
                    return Ok(writes::run(&engine, s, name, body, request_db, &done)
                        .await?
                        .reply);
                }
                attempt -= 1;
                continue;
            }
            Err(e) if matches!(e.errno, Some(1142 | 1143 | 1044)) => {
                // Every writer of a database reaches its records table
                // (chapter 29's backend grants and the gateway's check,
                // decisions/r5-interim.md D12). Should a grant still be
                // missing, the write is refused rather than run without
                // its record, which a retry could run twice.
                rollback(s).await;
                metrics::counter!("sprig_mongo_retryable_unrecorded_total", "command" => name.to_owned())
                    .increment(1);
                return Err(mongo_error(&e));
            }
            Err(e) => {
                rollback(s).await;
                return Err(mongo_error(&e));
            }
        };
        let rec = match rows.first() {
            Some(r) => match sessions::decode(r) {
                Ok(x) => x,
                Err(e) => {
                    rollback(s).await;
                    return Err(e);
                }
            },
            None => Record {
                txn_number: -1,
                ..Record::default()
            },
        };
        if rec.txn_number > txn_number
            || (rec.txn_number == txn_number && (rec.committed || rec.aborted))
        {
            rollback(s).await;
            return Err(MongoError::new(
                code::TRANSACTION_TOO_OLD,
                format!(
                    "Retryable write with txnNumber {txn_number} is prohibited on session because a newer retryable write with txnNumber {} has already started on this session.",
                    rec.txn_number
                ),
            ));
        }
        let done = if rec.txn_number == txn_number {
            rec.done
        } else {
            StmtResults::new()
        };
        // 4. The statements not done yet.
        let (r, broken) = {
            let mut g = Guarded {
                s: &mut *s,
                broken: None,
            };
            let r = writes::run(&engine, &mut g, name, body, request_db, &done).await;
            (r, g.broken)
        };
        let transient = broken
            .as_ref()
            .is_some_and(|e| matches!(e.errno, Some(1213 | 1205)))
            || match &r {
                Ok(o) => reply_conflict(&o.reply),
                Err(e) => conflict(e),
            };
        if transient && attempt <= cfg.write_conflict_retries {
            rollback(s).await;
            metrics::counter!("sprig_mongo_write_conflict_retries_total", "command" => name.to_owned())
                .increment(1);
            continue;
        }
        let out = match (r, broken) {
            (_, Some(e)) => {
                rollback(s).await;
                return Err(mongo_error(&e));
            }
            (Err(e), None) => {
                rollback(s).await;
                return Err(e);
            }
            (Ok(o), None) => o,
        };
        // 5. The record, in the same transaction.
        if !out.executed.is_empty() || rec.txn_number != txn_number {
            let mut all = done.clone();
            all.extend(out.executed);
            let bytes = match sessions::encode(&all) {
                Ok(b) => b,
                Err(e) => {
                    rollback(s).await;
                    return Err(e);
                }
            };
            if let Err(e) = s
                .exec(&sessions::upsert_sql(
                    db, lsid, &uid, txn_number, &bytes, false,
                ))
                .await
            {
                rollback(s).await;
                return Err(mongo_error(&e));
            }
        }
        // 6. Together or not at all. A failed COMMIT has an unknown
        // outcome; the driver's retry finds the record if it committed.
        if let Err(e) = s.exec("COMMIT").await {
            rollback(s).await;
            let m = mongo_error(&e);
            return Err(if retryable_code(m.code) {
                m
            } else {
                MongoError::new(code::HOST_UNREACHABLE, m.message)
            });
        }
        if !done.is_empty() {
            metrics::counter!("sprig_mongo_retryable_replays_total", "command" => name.to_owned())
                .increment(1);
        }
        if c.shared.records.purge_due(&key) {
            let q = sessions::purge_sql(db, cfg.session_record_retention_minutes, PURGE_ROWS);
            if let Err(e) = s.exec(&q).await {
                tracing::debug!(db, error = %e.message, "retryable-write record purge failed");
            }
        }
        return Ok(out.reply);
    }
}

/// Whether a command is a retryable write: a write command with `lsid`
/// and `txnNumber`, outside a transaction.
#[must_use]
pub fn applies(name: &str, body: &Document) -> bool {
    writes::is_write(name)
        && body.contains_key("txnNumber")
        && matches!(body.get("lsid"), Some(Bson::Document(_)))
        && body.get_bool("autocommit") != Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uid_depends_on_tenant_and_user() {
        let a = uid_of(&Owner {
            tenant: 1,
            user: "app".into(),
        });
        let b = uid_of(&Owner {
            tenant: 2,
            user: "app".into(),
        });
        let c = uid_of(&Owner {
            tenant: 1,
            user: "app".into(),
        });
        assert_ne!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn labels() {
        let e = labelled(MongoError::new(39034, "lost"));
        assert_eq!(e.code, 6);
        assert_eq!(e.labels, vec!["RetryableWriteError"]);
        let e = labelled(MongoError::new(11000, "dup"));
        assert!(e.labels.is_empty());
        assert!(applies(
            "insert",
            &bson::doc! {"insert": "c", "lsid": {"id": 1}, "txnNumber": 1i64}
        ));
        assert!(!applies(
            "insert",
            &bson::doc! {"insert": "c", "lsid": {"id": 1}, "txnNumber": 1i64, "autocommit": false}
        ));
        assert!(!applies(
            "find",
            &bson::doc! {"find": "c", "lsid": {"id": 1}, "txnNumber": 1i64}
        ));
    }
}
