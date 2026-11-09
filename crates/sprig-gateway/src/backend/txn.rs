//! The session's backend connection as a chapter 18 transaction branch:
//! [`sprig_txn::BranchConn`] on [`BackendSession`], so the coordinator
//! sends its control statements (`START TRANSACTION`, `XA START`, `XA END;
//! XA PREPARE`, the decision insert and `COMMIT`, savepoints) through the
//! same pinned connection the router's statements use.
//!
//! Not yet: the session holding one `TxnCoordinator` with a
//! `BackendSession` per cluster (a gateway `TxnBackend`), and the router's
//! executor calling it (chapter 18, "Interfaces", the router's side).

use sprig_common::ErrorKind;
use sprig_proto_mysql::client::PacketKind;
use sprig_txn::{BranchConn, Ctl, DbError, DbResult, RedoStmt};

use super::state::SessionVars;
use super::{BackendSession, StmtClass, StmtInfo};

/// Sprig 2PC (R3): a held branch's record of its writes on a
/// [`BackendSession`]: each text `INSERT`, `UPDATE`, `DELETE` or `REPLACE`
/// that succeeded, with its replay context (the branch's fixed clock,
/// `insert_id`, `sql_mode`, `time_zone`, the client character set), and
/// the first write it can't replay (chapter 18's determinism rule; a
/// binary-protocol write, whose values aren't text here).
#[derive(Debug, Default)]
pub(crate) struct Recording {
    timestamp_us: u64,
    stmts: Vec<RedoStmt>,
    savepoints: Vec<(String, usize)>,
    unsafe_why: Option<String>,
    /// The statement in flight, recorded when it succeeds.
    pub(super) pending: Option<String>,
    /// The next binary-protocol statement with its bound values written
    /// in (decision Q17): recorded like a text statement.
    pub(super) bound: Option<String>,
}

fn first_word(sql: &[u8]) -> String {
    let s = String::from_utf8_lossy(sql);
    s.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

impl Recording {
    /// Before a text statement.
    pub(super) fn before(&mut self, info: &StmtInfo, sql: &[u8]) {
        self.pending = None;
        let w = first_word(sql);
        if matches!(w.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE") {
            let text = String::from_utf8_lossy(sql).into_owned();
            if self.unsafe_why.is_none()
                && let Some(why) = sprig_txn::redo::unsafe_reason(&text)
            {
                self.unsafe_why = Some(why.to_owned());
            }
            self.pending = Some(text);
        } else if info.class == StmtClass::Write
            && matches!(w.as_str(), "CALL" | "LOAD" | "SET" | "HANDLER")
            && self.unsafe_why.is_none()
        {
            self.unsafe_why = Some(format!("a {w} statement that writes"));
        }
    }

    /// Before a binary-protocol statement.
    pub(super) fn before_binary(&mut self, info: &StmtInfo, sql: &[u8]) {
        self.pending = None;
        if let Some(text) = self.bound.take() {
            // The values are written in: the same rules as a text write.
            self.before(info, text.as_bytes());
            return;
        }
        let w = first_word(sql);
        if (info.class == StmtClass::Write
            && matches!(w.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE"))
            && self.unsafe_why.is_none()
        {
            self.unsafe_why = Some("a prepared (binary protocol) write".to_owned());
        }
    }

    /// A recorded statement succeeded.
    pub(super) fn record(
        &mut self,
        sql: String,
        affected: u64,
        insert_id: u64,
        vars: &SessionVars,
    ) {
        let update = first_word(sql.as_bytes()) == "UPDATE";
        let var = |n: &str| vars.vars.get(n).map(|v| (n.to_owned(), v.clone()));
        self.stmts.push(RedoStmt {
            // UPDATE counts depend on CLIENT_FOUND_ROWS: not checked.
            affected: if update { u64::MAX } else { affected },
            insert_id,
            vars: ["sql_mode", "time_zone"]
                .into_iter()
                .filter_map(var)
                .collect(),
            charset: vars.vars.get("character_set_client").cloned(),
            timestamp_us: Some(self.timestamp_us),
            sql,
        });
    }
}

fn db_error(e: &sprig_common::SprigError) -> DbError {
    match e.kind {
        ErrorKind::Backend { errno, .. } => DbError::new(errno, e.message.clone()),
        // Anything else (lost connection, pool, protocol): the outcome of
        // the statement is unknown.
        _ => DbError::lost(e.to_string()),
    }
}

#[async_trait::async_trait]
impl BranchConn for BackendSession {
    fn thread_id(&self) -> u64 {
        BackendSession::thread_id(self).map_or(0, |(_, t)| t)
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        for c in ctl {
            let sql = c.sql();
            let info = StmtInfo::classify(sql.as_bytes());
            // The coordinator's own statements (`SET timestamp`, the lock
            // wait, savepoints, the redo marker) aren't the branch's writes:
            // the record pauses for them (found by the gateway's live test
            // of Sprig 2PC: a recorded `SET timestamp` made every held
            // branch unreplayable).
            let rec = self.redo.take();
            let ran = async {
                self.query(&info, sql.as_bytes())
                    .await
                    .map_err(|e| db_error(&e))?;
                let mut err = None;
                while let Some(p) = self.next().await.map_err(|e| db_error(&e))? {
                    if let PacketKind::Err(e) = p.kind {
                        err = Some(DbError::new(e.code, e.message.clone()));
                    }
                }
                self.finish().await.map_err(|e| db_error(&e))?;
                Ok::<Option<DbError>, DbError>(err)
            }
            .await;
            if self.redo.is_none() {
                self.redo = rec;
            }
            if let Some(e) = ran? {
                return Err(e);
            }
            if let Some(r) = self.redo.as_mut() {
                match c {
                    Ctl::Savepoint(n) => {
                        r.savepoints.retain(|(x, _)| x != n);
                        r.savepoints.push((n.clone(), r.stmts.len()));
                    }
                    Ctl::RollbackTo(n) => {
                        if let Some(&(_, at)) = r.savepoints.iter().find(|(x, _)| x == n) {
                            r.stmts.truncate(at);
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn start_redo(&mut self, timestamp_us: u64) -> bool {
        self.redo = Some(Recording {
            timestamp_us,
            ..Recording::default()
        });
        true
    }

    fn take_redo(&mut self) -> Result<Vec<RedoStmt>, String> {
        let Some(r) = self.redo.take() else {
            return Ok(Vec::new());
        };
        match r.unsafe_why {
            Some(why) => Err(why),
            None => Ok(r.stmts),
        }
    }
}
