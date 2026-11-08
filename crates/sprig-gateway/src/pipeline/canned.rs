//! A tiny in-process executor with canned answers, so the front end's whole
//! path (handshake, login, query, resultset, prepared statements, quit) can
//! run without a backend: in tests and for local experiments.
//!
//! It knows (case-insensitively, one trailing `;` allowed):
//!
//! | Statement | Answer |
//! |---|---|
//! | `SELECT <integer>` | One `BIGINT` row |
//! | `SELECT 'text'` | One `VARCHAR` row |
//! | `SELECT REPEAT('<c>', <n>)` | One row of `n` copies of `c` |
//! | `SELECT SLEEP(<n>)` | Waits `n` seconds, then one row `0` |
//! | `SELECT USER()`, `CURRENT_USER()`, `DATABASE()` | The session's user or database |
//! | `SELECT @@<var>[, @@<var>...]` | `wait_timeout`, `max_allowed_packet`, `socket`, `autocommit`, `sql_mode` |
//! | `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK` | OK, tracking `IN_TRANS` |
//! | `SET ...`, `DO ...`, `USE ...` | OK |
//!
//! With multi-statements on, `;`-separated statements answer in turn.
//! `COM_STMT_PREPARE` accepts `SELECT ?` (echoes the parameter as text) and
//! `SELECT <integer>`. Anything else fails with 1064.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::command::ParamValue;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{StatusFlags, Value};

use super::{
    EndOfRows, ExecCtx, ExecSummary, Executor, LoginInfo, PreparedMeta, Request, ResultSink,
    RowFormat, RowValues, SessionExec, StmtRef, int_column, text_column,
};

/// The canned executor. See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct CannedExecutor;

impl Executor for CannedExecutor {
    fn open(&self, _login: &LoginInfo) -> Box<dyn SessionExec> {
        Box::new(CannedSession::default())
    }
}

#[derive(Debug, Clone)]
enum Canned {
    /// `SELECT ?`.
    Echo,
    /// `SELECT <integer>`.
    Int(i64),
}

#[derive(Debug, Default)]
struct CannedSession {
    in_trans: bool,
    next_stmt: u64,
    stmts: HashMap<u64, Canned>,
}

fn parse_error(sql: &str) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!("You have an error in your SQL syntax: the canned executor doesn't know {sql:?}"),
    )
}

/// The argument of `name(...)` when `s` is exactly that call.
fn call_arg<'a>(s: &'a str, lower: &str, name: &str) -> Option<&'a str> {
    let rest = lower.strip_prefix(name)?.trim_start();
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return None;
    }
    let open = s.len() - rest.len();
    Some(s[open + 1..s.len() - 1].trim())
}

fn unquote(s: &str) -> Option<&str> {
    s.strip_prefix('\'')?.strip_suffix('\'')
}

impl CannedSession {
    fn status(&self) -> StatusFlags {
        let mut s = StatusFlags::AUTOCOMMIT;
        s.set(StatusFlags::IN_TRANS, self.in_trans);
        s
    }

    async fn one_row(
        &self,
        out: &mut dyn ResultSink,
        name: &str,
        value: Option<&[u8]>,
        int: bool,
        more: bool,
    ) -> Result<()> {
        let charset = u16::from(out.wire().persona.greeting_collation());
        let col = if int {
            int_column(name, false)
        } else {
            text_column(name, charset)
        };
        out.begin_rows(std::slice::from_ref(&col), RowFormat::Text)
            .await?;
        out.row(RowValues::Text(&[value])).await?;
        out.end_rows(EndOfRows {
            status: self.end_status(more),
            warnings: 0,
        })
        .await
    }

    fn end_status(&self, more: bool) -> StatusFlags {
        let mut s = self.status();
        s.set(StatusFlags::MORE_RESULTS_EXISTS, more);
        s
    }

    async fn ok(&self, out: &mut dyn ResultSink, more: bool) -> Result<()> {
        out.ok(OkPacket::new(self.end_status(more))).await
    }

    /// Runs one statement.
    async fn statement(
        &mut self,
        cx: &ExecCtx<'_>,
        sql: &str,
        out: &mut dyn ResultSink,
        more: bool,
    ) -> Result<()> {
        let lower = sql.to_ascii_lowercase();
        match lower.as_str() {
            "begin" | "start transaction" => {
                self.in_trans = true;
                return self.ok(out, more).await;
            }
            "commit" | "rollback" => {
                self.in_trans = false;
                return self.ok(out, more).await;
            }
            _ => {}
        }
        if ["set ", "do ", "use "].iter().any(|p| lower.starts_with(p)) {
            return self.ok(out, more).await;
        }
        let Some(item) = lower.strip_prefix("select ").map(str::trim) else {
            return Err(parse_error(sql));
        };
        let orig = sql[sql.len() - item.len()..].trim();
        if let Ok(n) = item.parse::<i64>() {
            return self
                .one_row(out, orig, Some(n.to_string().as_bytes()), true, more)
                .await;
        }
        if let Some(text) = unquote(orig) {
            return self
                .one_row(out, text, Some(text.as_bytes()), false, more)
                .await;
        }
        if let Some(arg) = call_arg(orig, item, "sleep") {
            let secs: f64 = arg.parse().map_err(|_| parse_error(sql))?;
            tokio::time::sleep(Duration::from_secs_f64(secs.clamp(0.0, 3600.0))).await;
            return self.one_row(out, orig, Some(b"0"), true, more).await;
        }
        if let Some(arg) = call_arg(orig, item, "repeat") {
            let (c, n) = arg.split_once(',').ok_or_else(|| parse_error(sql))?;
            let c = unquote(c.trim()).ok_or_else(|| parse_error(sql))?;
            let n: usize = n.trim().parse().map_err(|_| parse_error(sql))?;
            let v = c.repeat(n.min(16 << 20));
            return self
                .one_row(out, orig, Some(v.as_bytes()), false, more)
                .await;
        }
        let user = cx.session.principal.as_ref().map(|p| p.user.clone());
        match item {
            "user()" | "current_user()" => {
                let v = user.map(|u| format!("{u}@%"));
                return self
                    .one_row(out, orig, v.as_deref().map(str::as_bytes), false, more)
                    .await;
            }
            "database()" => {
                return self
                    .one_row(out, orig, cx.db.map(str::as_bytes), false, more)
                    .await;
            }
            _ => {}
        }
        if item.starts_with("@@") {
            return self.variables(out, orig, more).await;
        }
        Err(parse_error(sql))
    }

    async fn variables(&self, out: &mut dyn ResultSink, list: &str, more: bool) -> Result<()> {
        let charset = u16::from(out.wire().persona.greeting_collation());
        let mut cols = Vec::new();
        let mut vals: Vec<Option<String>> = Vec::new();
        for name in list.split(',').map(str::trim) {
            let var = name.trim_start_matches("@@").to_ascii_lowercase();
            let var = var
                .strip_prefix("session.")
                .or_else(|| var.strip_prefix("global."))
                .unwrap_or(&var);
            let (v, int) = match var {
                "wait_timeout" | "interactive_timeout" => (Some("28800".to_owned()), true),
                "max_allowed_packet" => (Some("67108864".to_owned()), true),
                "autocommit" => (Some("1".to_owned()), true),
                "socket" => (None, false),
                "sql_mode" => (Some("STRICT_TRANS_TABLES".to_owned()), false),
                _ => return Err(parse_error(list)),
            };
            cols.push(if int {
                int_column(name, false)
            } else {
                text_column(name, charset)
            });
            vals.push(v);
        }
        out.begin_rows(&cols, RowFormat::Text).await?;
        let row: Vec<Option<&[u8]>> = vals
            .iter()
            .map(|v| v.as_deref().map(str::as_bytes))
            .collect();
        out.row(RowValues::Text(&row)).await?;
        out.end_rows(EndOfRows {
            status: self.end_status(more),
            warnings: 0,
        })
        .await
    }
}

fn text_of(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Null => None,
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::UInt(u) => Some(u.to_string().into_bytes()),
        Value::Float(f) => Some(f.to_string().into_bytes()),
        Value::Double(d) => Some(d.to_string().into_bytes()),
        Value::Bytes(b) => Some(b.to_vec()),
        Value::DateTime(_) | Value::Time(_) => Some(format!("{v:?}").into_bytes()),
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)] // one arm per request kind
impl SessionExec for CannedSession {
    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        match req {
            Request::Query { sql, .. } => {
                let text = String::from_utf8_lossy(&sql).into_owned();
                let parts: Vec<&str> = text
                    .split(';')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.len() > 1 && !cx.proto.multi_statements {
                    return Err(parse_error(&text));
                }
                if parts.is_empty() {
                    return Err(SprigError::standard(1065, *b"42000", "Query was empty"));
                }
                let last = parts.len() - 1;
                for (i, s) in parts.iter().enumerate() {
                    if let Err(e) = self.statement(cx, s, out, i < last).await {
                        out.error(&e).await?;
                        break;
                    }
                }
            }
            Request::Prepare { sql } => {
                let text = String::from_utf8_lossy(&sql).trim().to_ascii_lowercase();
                let item = text
                    .trim_end_matches(';')
                    .strip_prefix("select ")
                    .map(str::trim)
                    .ok_or_else(|| parse_error(&text))?;
                let canned = if item == "?" {
                    Canned::Echo
                } else {
                    Canned::Int(item.parse().map_err(|_| parse_error(&text))?)
                };
                self.next_stmt += 1;
                let id = self.next_stmt;
                let charset = u16::from(out.wire().persona.greeting_collation());
                let meta = match &canned {
                    Canned::Echo => PreparedMeta {
                        stmt: StmtRef(id),
                        params: vec![text_column("?", charset)],
                        columns: vec![text_column("?", charset)],
                        warnings: 0,
                    },
                    Canned::Int(n) => PreparedMeta {
                        stmt: StmtRef(id),
                        params: Vec::new(),
                        columns: vec![int_column(&n.to_string(), false)],
                        warnings: 0,
                    },
                };
                self.stmts.insert(id, canned);
                return Ok(ExecSummary {
                    status: Some(self.status()),
                    prepared: Some(meta),
                });
            }
            Request::Execute { stmt, params, .. } => {
                let Some(canned) = self.stmts.get(&stmt.0).cloned() else {
                    return Err(SprigError::standard(
                        1243,
                        *b"HY000",
                        "Unknown prepared statement handler",
                    ));
                };
                let charset = u16::from(out.wire().persona.greeting_collation());
                let (col, val) = match canned {
                    Canned::Echo => {
                        let v = match params.params.first().map(|p| &p.value) {
                            Some(ParamValue::Value(v)) => text_of(v).map(Bytes::from),
                            _ => None,
                        };
                        (
                            text_column("?", charset),
                            v.map_or(Value::Null, Value::Bytes),
                        )
                    }
                    Canned::Int(n) => (int_column(&n.to_string(), false), Value::Int(n)),
                };
                out.begin_rows(std::slice::from_ref(&col), RowFormat::Binary)
                    .await?;
                out.row(RowValues::Binary(&[val])).await?;
                out.end_rows(EndOfRows {
                    status: self.status(),
                    warnings: 0,
                })
                .await?;
            }
            Request::CloseStmt { stmt } => {
                self.stmts.remove(&stmt.0);
            }
            Request::LongData { .. } => {}
            Request::ResetStmt { .. } => self.ok(out, false).await?,
            Request::Fetch { .. } | Request::BulkExecute { .. } | Request::FieldList { .. } => {
                return Err(SprigError::standard(
                    1047,
                    *b"08S01",
                    format!("the canned executor doesn't support {}", req.name()),
                ));
            }
        }
        Ok(ExecSummary {
            status: Some(self.status()),
            prepared: None,
        })
    }

    async fn init_db(&mut self, _cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        if schema == "missing" {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{schema}'"),
            ));
        }
        Ok(())
    }

    async fn reset(&mut self, _cx: &mut ExecCtx<'_>) -> Result<()> {
        self.in_trans = false;
        self.stmts.clear();
        Ok(())
    }
}
