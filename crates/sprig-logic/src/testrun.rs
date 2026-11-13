//! `Logic.TestRun` (chapter 41, "Test runs"; chapter 44's logic editors;
//! R7 follow-ups): runs a function, procedure or trigger body against
//! given inputs, in a sandbox that never writes.
//!
//! ```text
//! test_run(request) in the caller's tenant session:
//!   program := the draft's source, or version n (default: active) of the module
//!   START TRANSACTION (SAVEPOINT inside the caller's transaction)
//!   rows {t: [...]}  → CREATE TEMPORARY TABLE t (t's definition, without
//!                      foreign keys); INSERT the rows
//!                      (the code sees exactly these rows of t)
//!   run the program on the engine tier, every db statement through this
//!   session (the caller's rights and row policies), each one checked and
//!   recorded: refused if it could end the transaction or reach outside it
//!   fetch(...)       → the allowlist, then a mock from the request (never sent)
//!   read back the snapshot tables
//!   ROLLBACK (TO SAVEPOINT); DROP TEMPORARY TABLE ...
//!   report: event and fields or the error, statements with timing, the
//!   writes, logs, fetches, the snapshot tables after the run, budgets used
//! ```
//!
//! The same request arrives three ways: REST `POST
//! /v1/databases/{db}/logic:testRun`, gRPC `sprig.v1.Logic/TestRun`, and
//! SQL:
//!
//! ```text
//! TEST {FUNCTION | PROCEDURE | TRIGGER | VALIDATION} [db.]name [VERSION n]
//!   [(arg, ...)] [WITH '<json options>']
//! ```
//!
//! REST and gRPC send the request as one marked statement
//! ([`TEST_PREFIX`] and the request as JSON) through the caller's session,
//! as `Functions.Call` does; every form answers one row, `event`,
//! `result` and `report` (JSON).
//!
//! Test runs always use the engine tier (QuickJS, Lua, the native KIRun
//! runtime): the differential suite holds the compiled tier to the same
//! answers, and only the engine tier lets each statement be seen. Code
//! that always runs as its definer (triggers, validations) runs as the
//! caller here.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_js::outbound::{self, Outbound, OutboundPolicy, Request, Response};

/// The statement text of a test run from REST or gRPC starts with this;
/// the rest is the [`TestRun`] as JSON.
pub const TEST_PREFIX: &str = "/* sprig logic test run */ ";

/// Rows of each snapshot table returned after the run, at most.
pub const MAX_TABLE_ROWS: usize = 1000;
/// Statements recorded in a report, at most (the run's own budget,
/// `logic.max_data_ops`, usually stops it first).
pub const MAX_RECORDED: usize = 1000;
/// Rows given for one snapshot table, at most.
pub const MAX_SNAPSHOT_ROWS: usize = 10_000;

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicSignatureMismatch, m)
}

/// One mocked outbound call: the first mock whose `method` (any when
/// empty) and `url` (exact, or a prefix ending in `*`) match answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default)]
pub struct FetchMock {
    /// `GET`, `POST`, ... (empty: any).
    pub method: String,
    /// The URL, or a prefix ending in `*`.
    pub url: String,
    /// The status answered (200 when 0).
    pub status: u16,
    /// Headers answered.
    pub headers: BTreeMap<String, String>,
    /// The body answered: text as given, anything else as JSON.
    pub body: Value,
}

impl FetchMock {
    fn matches(&self, req: &Request) -> bool {
        let method_ok = self.method.is_empty() || self.method.eq_ignore_ascii_case(&req.method);
        let url_ok = match self.url.strip_suffix('*') {
            Some(prefix) => req.url.starts_with(prefix),
            None => self.url == req.url,
        };
        method_ok && url_ok
    }

    fn response(&self) -> Response {
        Response {
            status: if self.status == 0 { 200 } else { self.status },
            headers: self
                .headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                .collect(),
            body: match &self.body {
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            },
        }
    }
}

/// A test run's request (see the module docs).
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct TestRun {
    /// The logical database; `None` uses the session's.
    #[serde(alias = "db")]
    pub database: Option<String>,
    /// `function` (default), `procedure`, `trigger` or `validation`.
    pub kind: Option<String>,
    /// The deployed module's name (not needed with `source`).
    pub name: Option<String>,
    /// The version to run; `None` is the active one.
    pub version: Option<u64>,
    /// A draft: a whole `CREATE [OR REPLACE] FUNCTION | PROCEDURE |
    /// TRIGGER | VALIDATION ... AS ...` declaration, run without being
    /// recorded or deployed.
    pub source: Option<String>,
    /// Function and procedure arguments: a list (by position), an object
    /// (by parameter name), one value, or `null` for none.
    pub args: Value,
    /// Validations and after-commit triggers: the row after the write.
    pub row: Option<Value>,
    /// ... and before it (`NULL` for an insert).
    pub old: Option<Value>,
    /// ... and the operation: `insert` (default), `update`, `delete`.
    pub op: Option<String>,
    /// Triggers on streams: `ctx.record`.
    pub record: Option<Value>,
    /// Triggers on queues: `ctx.message`.
    pub message: Option<Value>,
    /// The snapshot: table name (of the code's database) → its rows, as
    /// objects of column values. The code sees exactly these rows of
    /// each table named.
    pub rows: BTreeMap<String, Vec<Value>>,
    /// Answers to `fetch` (never sent).
    pub fetch: Vec<FetchMock>,
}

impl TestRun {
    /// The marked statement REST and gRPC send.
    #[must_use]
    pub fn statement(&self) -> String {
        format!(
            "{TEST_PREFIX}{}",
            serde_json::to_string(self).unwrap_or_default()
        )
    }

    /// The kind asked for, lower case (`function` by default).
    #[must_use]
    pub fn kind(&self) -> String {
        self.kind
            .as_deref()
            .unwrap_or("function")
            .to_ascii_lowercase()
    }

    /// Checks what the request names before anything runs.
    ///
    /// # Errors
    /// 39874 for a request that names nothing, a kind that can't be
    /// tested, too many snapshot rows, or a table name that isn't one.
    pub fn check(&self) -> Result<()> {
        if self.source.is_none() && self.name.as_deref().is_none_or(str::is_empty) {
            return Err(invalid(
                "a test run names a module (name) or gives a draft (source)",
            ));
        }
        let k = self.kind();
        if !matches!(
            k.as_str(),
            "function" | "procedure" | "trigger" | "validation"
        ) {
            return Err(invalid(format!(
                "a test run runs a function, procedure, trigger or validation, not a {k} \
                 (test the procedure a schedule calls)"
            )));
        }
        for (t, rows) in &self.rows {
            if t.is_empty()
                || t.chars().count() > 64
                || !t
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
            {
                return Err(invalid(format!("rows: {t:?} isn't a table name")));
            }
            if rows.len() > MAX_SNAPSHOT_ROWS {
                return Err(invalid(format!(
                    "rows: at most {MAX_SNAPSHOT_ROWS} rows per table ({t} has {})",
                    rows.len()
                )));
            }
            if let Some(r) = rows.iter().find(|r| !r.is_object()) {
                return Err(invalid(format!(
                    "rows: each row of {t} is an object of column values, not {r}"
                )));
            }
        }
        Ok(())
    }
}

/// Parses a test run: the marked statement, or `TEST FUNCTION ...`.
/// `None` when `text` is neither.
#[must_use]
pub fn parse_statement(text: &[u8]) -> Option<Result<TestRun>> {
    if let Some(rest) = text.strip_prefix(TEST_PREFIX.as_bytes()) {
        return Some(
            serde_json::from_slice::<TestRun>(rest)
                .map_err(|e| invalid(format!("a bad test run request: {e}"))),
        );
    }
    let s = std::str::from_utf8(text).ok()?;
    let t = s.trim_start();
    let head: Vec<String> = t
        .split_whitespace()
        .take(2)
        .map(str::to_ascii_lowercase)
        .collect();
    if head.first().map(String::as_str) != Some("test") {
        return None;
    }
    if !matches!(
        head.get(1).map(String::as_str),
        Some("function" | "procedure" | "trigger" | "validation")
    ) {
        return None;
    }
    Some(parse_sql(t))
}

/// `TEST {FUNCTION | ...} [db.]name [VERSION n] [(args)] [WITH 'json']`.
fn parse_sql(text: &str) -> Result<TestRun> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let dialect = sqlparser::dialect::MySqlDialect {};
    let toks: Vec<Token> = Tokenizer::new(&dialect, text.trim().trim_end_matches(';'))
        .tokenize()
        .map_err(|e| SprigError::new(SprigCode::LogicSyntax, format!("TEST: {e}")))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    let syntax = |m: &str| {
        SprigError::new(
            SprigCode::LogicSyntax,
            format!(
                "{m}: TEST {{FUNCTION | PROCEDURE | TRIGGER | VALIDATION}} [db.]name \
                 [VERSION n] [(arg, ...)] [WITH '{{json options}}']"
            ),
        )
    };
    let word = |t: Option<&Token>| match t {
        Some(Token::Word(w)) => Some(w.value.clone()),
        _ => None,
    };
    let mut i = 1;
    let mut req = TestRun {
        kind: word(toks.get(i)).map(|k| k.to_ascii_lowercase()),
        ..TestRun::default()
    };
    i += 1;
    let first = word(toks.get(i)).ok_or_else(|| syntax("expected a name"))?;
    i += 1;
    if toks.get(i) == Some(&Token::Period) {
        req.database = Some(first);
        req.name = Some(word(toks.get(i + 1)).ok_or_else(|| syntax("expected a name"))?);
        i += 2;
    } else {
        req.name = Some(first);
    }
    if word(toks.get(i)).is_some_and(|w| w.eq_ignore_ascii_case("version")) {
        let n = match toks.get(i + 1) {
            Some(Token::Number(n, _)) => n.parse::<u64>().ok(),
            _ => None,
        };
        req.version = Some(n.ok_or_else(|| syntax("VERSION takes a number"))?);
        i += 2;
    }
    if toks.get(i) == Some(&Token::LParen) {
        i += 1;
        let mut args = Vec::new();
        if toks.get(i) == Some(&Token::RParen) {
            i += 1;
        } else {
            loop {
                let neg = toks.get(i) == Some(&Token::Minus);
                if neg {
                    i += 1;
                }
                let v = match toks.get(i) {
                    Some(Token::Number(n, _)) => {
                        let text = if neg { format!("-{n}") } else { n.clone() };
                        text.parse::<i64>().map(Value::from).or_else(|_| {
                            text.parse::<f64>()
                                .map(Value::from)
                                .map_err(|_| syntax("a bad number"))
                        })?
                    }
                    Some(Token::SingleQuotedString(s) | Token::DoubleQuotedString(s)) if !neg => {
                        Value::String(s.clone())
                    }
                    Some(Token::Word(w)) if !neg => match w.value.to_ascii_lowercase().as_str() {
                        "null" => Value::Null,
                        "true" => Value::Bool(true),
                        "false" => Value::Bool(false),
                        _ => return Err(syntax("arguments are constants")),
                    },
                    _ => return Err(syntax("arguments are constants")),
                };
                args.push(v);
                i += 1;
                match toks.get(i) {
                    Some(Token::Comma) => i += 1,
                    Some(Token::RParen) => {
                        i += 1;
                        break;
                    }
                    _ => return Err(syntax("expected , or )")),
                }
            }
        }
        req.args = Value::Array(args);
    }
    if word(toks.get(i)).is_some_and(|w| w.eq_ignore_ascii_case("with")) {
        let Some(Token::SingleQuotedString(s) | Token::DoubleQuotedString(s)) = toks.get(i + 1)
        else {
            return Err(syntax("WITH takes the options as a JSON string"));
        };
        let opts: Value = serde_json::from_str(s)
            .map_err(|e| invalid(format!("TEST ... WITH: the options aren't JSON: {e}")))?;
        let Value::Object(mut o) = opts else {
            return Err(invalid("TEST ... WITH: the options are a JSON object"));
        };
        // The statement's own parts win over the options'.
        for k in ["database", "db", "kind", "name", "version"] {
            o.remove(k);
        }
        if !req.args.is_null() {
            o.remove("args");
        }
        let mut merged = serde_json::to_value(&req).unwrap_or_default();
        if let Value::Object(m) = &mut merged {
            for (k, v) in o {
                m.insert(k, v);
            }
        }
        req = serde_json::from_value(merged)
            .map_err(|e| invalid(format!("TEST ... WITH: bad options: {e}")))?;
        i += 2;
    }
    if i < toks.len() {
        return Err(syntax("unexpected text"));
    }
    Ok(req)
}

/// What a statement of the code is, for a test run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StmtClass {
    /// A read: runs.
    Read,
    /// A write inside the transaction: runs, recorded as a write.
    Write,
    /// `CALL` of another logic function: runs (it never ends the
    /// transaction), its own statements inside it.
    Call(String),
    /// Refused, with the reason (it could end the transaction or reach
    /// outside it).
    Refused(String),
}

/// The first word of `sql`, upper case, past comments and `(`.
fn first_word(sql: &str) -> String {
    let mut s = sql.trim_start();
    loop {
        if let Some(r) = s.strip_prefix("/*") {
            s = r.split_once("*/").map_or("", |(_, b)| b).trim_start();
        } else if let Some(r) = s.strip_prefix('(') {
            s = r.trim_start();
        } else if s.starts_with("--") || s.starts_with('#') {
            s = s.split_once('\n').map_or("", |(_, b)| b).trim_start();
        } else {
            break;
        }
    }
    s.chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_ascii_uppercase()
}

/// Classifies one statement the code sends (see [`StmtClass`]).
#[must_use]
pub fn classify(sql: &str) -> StmtClass {
    let w = first_word(sql);
    match w.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "SHOW" | "EXPLAIN" | "DESCRIBE" | "DESC" => {
            StmtClass::Read
        }
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => StmtClass::Write,
        "CALL" => {
            let rest = sql.trim_start()[4..].trim_start();
            let name: String = rest
                .chars()
                .take_while(|c| *c != '(' && !c.is_whitespace())
                .collect();
            let plain = name.replace('`', "").to_ascii_lowercase();
            match plain.as_str() {
                "sprig.mq_publish" | "sprig.commit_offset" => StmtClass::Write,
                "sprig.mq_get" | "sprig.mq_ack" | "sprig.mq_nack" => StmtClass::Refused(
                    "db.mq.get, ack and nack take or settle real messages, outside the \
                     transaction; a test run doesn't run them"
                        .to_owned(),
                ),
                _ => StmtClass::Call(name.replace('`', "")),
            }
        }
        _ => StmtClass::Refused(format!(
            "a test run runs reads, writes and logic calls only; {w} could end the \
             transaction or change what it can't undo"
        )),
    }
}

/// The JSON a test-run error shows: code, name and message.
#[must_use]
pub fn error_json(e: &SprigError) -> Value {
    let mut v = json!({
        "code": e.protocol().mysql_errno,
        "message": e.message,
    });
    if let Some(c) = e.code() {
        v["name"] = Value::String(c.name().to_owned());
    }
    v
}

/// One statement the code ran (or tried to).
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Statement {
    /// The text, with its arguments bound.
    pub sql: String,
    /// `read`, `write`, `call` or `refused`.
    pub kind: String,
    /// Rows returned (reads).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// Rows changed (writes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected: Option<u64>,
    /// How long it took, in milliseconds.
    pub ms: f64,
    /// Its error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

/// One outbound call the code made.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Fetched {
    /// The method.
    pub method: String,
    /// The URL.
    pub url: String,
    /// The body sent, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// The mock's status, or `None` when it was refused.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// Why it was refused.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What the run recorded while it ran (shared with the bridge and the
/// outbound mock).
#[derive(Debug, Default)]
pub struct Recorder {
    /// Statements.
    pub statements: Mutex<Vec<Statement>>,
    /// Log lines.
    pub logs: Mutex<Vec<String>>,
    /// Outbound calls.
    pub fetches: Mutex<Vec<Fetched>>,
}

impl Recorder {
    /// Records a statement (the first [`MAX_RECORDED`]).
    pub fn statement(&self, s: Statement) {
        let mut v = self
            .statements
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if v.len() < MAX_RECORDED {
            v.push(s);
        }
    }

    /// Records a log line (the first [`MAX_RECORDED`]).
    pub fn log(&self, line: &str) {
        let mut v = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if v.len() < MAX_RECORDED {
            v.push(line.to_owned());
        }
    }

    fn fetched(&self, f: Fetched) {
        let mut v = self
            .fetches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if v.len() < MAX_RECORDED {
            v.push(f);
        }
    }

    /// Everything recorded, as the report's `statements`, `writes`,
    /// `logs` and `fetches`.
    #[must_use]
    pub fn take(&self) -> (Vec<Statement>, Vec<String>, Vec<Fetched>) {
        let s = std::mem::take(
            &mut *self
                .statements
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let l = std::mem::take(
            &mut *self
                .logs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let f = std::mem::take(
            &mut *self
                .fetches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        (s, l, f)
    }
}

/// `fetch` in a test run: the allowlist decides as for a real call, then
/// a mock answers; nothing is sent.
pub struct MockOutbound {
    /// `logic.outbound_allowlist` and the rest of the policy.
    pub policy: OutboundPolicy,
    /// The request's mocks.
    pub mocks: Vec<FetchMock>,
    /// Where calls are recorded.
    pub rec: std::sync::Arc<Recorder>,
}

impl std::fmt::Debug for MockOutbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockOutbound").finish_non_exhaustive()
    }
}

fn host_port(url: &str) -> Option<(String, u16)> {
    let (scheme, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, a)| a);
    let default = if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    };
    if let Some(r) = authority.strip_prefix('[') {
        let (h, p) = r.split_once(']')?;
        let port = p.strip_prefix(':').and_then(|p| p.parse().ok());
        return Some((h.to_owned(), port.unwrap_or(default)));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) => Some((h.to_owned(), p.parse().ok()?)),
        None => Some((authority.to_owned(), default)),
    }
}

#[async_trait]
impl Outbound for MockOutbound {
    async fn fetch(&self, req: Request) -> Result<Response> {
        let mut f = Fetched {
            method: req.method.clone(),
            url: req.url.clone(),
            body: req.body.clone(),
            status: None,
            error: None,
        };
        let refused = |msg: String, f: &mut Fetched| {
            f.error = Some(msg.clone());
            SprigError::new(SprigCode::LogicOutboundDenied, msg)
        };
        let Some((host, port)) = host_port(&req.url) else {
            let e = refused(
                format!("outbound call refused: {} isn't a URL", req.url),
                &mut f,
            );
            self.rec.fetched(f);
            return Err(e);
        };
        if outbound::allowed(&self.policy.allow, &host, port) == outbound::Allowed::No {
            let e = refused(
                format!("outbound call refused: {host}:{port} isn't on logic.outbound_allowlist"),
                &mut f,
            );
            self.rec.fetched(f);
            return Err(e);
        }
        let Some(m) = self.mocks.iter().find(|m| m.matches(&req)) else {
            let e = refused(
                format!(
                    "a test run sends no outbound calls: give a mock for {} {} in fetch",
                    req.method, req.url
                ),
                &mut f,
            );
            self.rec.fetched(f);
            return Err(e);
        };
        let r = m.response();
        f.status = Some(r.status);
        self.rec.fetched(f);
        Ok(r)
    }
}

/// The names a JavaScript validation or after-commit trigger takes when
/// it is a body alone (as `sprig_logic_compile::js::compile_with` binds
/// them).
pub const JS_ROW_NAMES: [&str; 4] = ["row", "old", "op", "ctx"];

/// The engine's signature of a JavaScript validation or after-commit
/// trigger: its default export's parameters by name, or [`JS_ROW_NAMES`]
/// for a body alone; [`trigger_args`] fills them by name.
#[must_use]
pub fn js_row_sig(source: &str) -> sprig_logic_js::ScriptSig {
    let names = sprig_logic_compile::js::entry_params(source)
        .unwrap_or_else(|| JS_ROW_NAMES.iter().map(|n| (*n).to_owned()).collect());
    sprig_logic_js::ScriptSig {
        params: names
            .into_iter()
            .map(|n| (n, sprig_logic_js::ParamTy::Json))
            .collect(),
        scalar: false,
    }
}

/// The arguments a trigger or validation body gets in a test run.
///
/// - Triggers on streams and queues: as the trigger's job gives them
///   ([`crate::messages::args_for`]): `ctx` with `record` or `message`.
/// - Validations and after-commit triggers written in KIRun: parameters
///   bound by name (chapter 41, decision V2): a column gets `row`'s
///   value, `old_<column>` `old`'s, `op` the operation.
/// - The same in Lua: one argument, `ctx`, with `trigger {op, before,
///   after}`, `row` and `old`.
/// - The same in JavaScript, by the parameters' names `params` (see
///   [`js_row_sig`]): `row` the row, `old` the old row, `ctx`
///   that `ctx`, `op`, a column or `old_<column>` as for KIRun; a
///   function of `ctx` alone gets `ctx`.
#[must_use]
pub fn trigger_args(
    req: &TestRun,
    runtime: &str,
    kirun_params: &[String],
    info: &Value,
) -> (Vec<Value>, Value) {
    let mut ctx = info.clone();
    if !ctx.is_object() {
        ctx = json!({});
    }
    if let Some(r) = &req.record {
        ctx["record"] = r.clone();
        return (
            crate::messages::args_for(runtime, kirun_params, r, &ctx),
            ctx,
        );
    }
    if let Some(m) = &req.message {
        ctx["message"] = m.clone();
        return (
            crate::messages::args_for(runtime, kirun_params, m, &ctx),
            ctx,
        );
    }
    let op = req.op.clone().unwrap_or_else(|| "insert".to_owned());
    let row = req.row.clone().unwrap_or(Value::Null);
    let old = req.old.clone().unwrap_or(Value::Null);
    ctx["trigger"] = json!({"op": op, "before": old, "after": row});
    ctx["row"] = row.clone();
    ctx["old"] = old.clone();
    let js_names = runtime == "javascript" && kirun_params.iter().any(|p| p != "ctx");
    if runtime != "kirun" && !js_names {
        return (vec![ctx.clone()], ctx);
    }
    let field = |v: &Value, k: &str| -> Value {
        v.as_object()
            .and_then(|o| {
                o.iter()
                    .find(|(c, _)| c.eq_ignore_ascii_case(k))
                    .map(|(_, x)| x.clone())
            })
            .unwrap_or(Value::Null)
    };
    let args = kirun_params
        .iter()
        .map(|p| {
            let l = p.to_ascii_lowercase();
            if js_names && l == "row" {
                row.clone()
            } else if js_names && l == "old" {
                old.clone()
            } else if js_names && l == "ctx" {
                ctx.clone()
            } else if l == "op" {
                Value::String(op.clone())
            } else if let Some(c) = l.strip_prefix("old_") {
                field(&old, c)
            } else {
                field(&row, &l)
            }
        })
        .collect();
    (args, ctx)
}

/// A function's or procedure's arguments by position, from a list, an
/// object by parameter name, one value or `null`.
///
/// # Errors
/// 39874 for a name that isn't a parameter.
pub fn positional(args: &Value, params: &[String], name: &str) -> Result<Vec<Value>> {
    Ok(match args {
        Value::Null => Vec::new(),
        Value::Array(a) => a.clone(),
        Value::Object(o) => {
            if let Some(k) = o.keys().find(|k| !params.contains(k)) {
                return Err(invalid(format!(
                    "{name} has no parameter {k} (it takes {})",
                    params.join(", ")
                )));
            }
            params
                .iter()
                .map(|p| o.get(p).cloned().unwrap_or(Value::Null))
                .collect()
        }
        other => vec![other.clone()],
    })
}

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// The statement that reads table `t`'s definition, for
/// [`temporary_from_show`].
#[must_use]
pub fn snapshot_show(db: &str, t: &str) -> String {
    format!("SHOW CREATE TABLE {}.{}", bq(db), bq(t))
}

/// `t`'s definition (`SHOW CREATE TABLE`'s text) as a temporary table of
/// the same name in `db`: MariaDB finds a session's temporary table
/// before the real one. Foreign keys are left out (a temporary table
/// can't have them, and the given rows needn't match other tables).
///
/// # Errors
/// 39874 for text that isn't a table's definition.
pub fn temporary_from_show(db: &str, t: &str, show: &str) -> Result<String> {
    let bad = || invalid(format!("rows: {t} isn't a table this test run can copy"));
    let open = show.find('(').ok_or_else(bad)?;
    let close = show.rfind(')').ok_or_else(bad)?;
    if !show[..open]
        .trim_start()
        .to_ascii_uppercase()
        .starts_with("CREATE TABLE")
        || close < open
    {
        return Err(bad());
    }
    let items: Vec<&str> = show[open + 1..close]
        .split('\n')
        .map(|l| l.trim().trim_end_matches(','))
        .filter(|l| !l.is_empty() && !l.to_ascii_uppercase().contains("FOREIGN KEY"))
        .collect();
    Ok(format!(
        "CREATE TEMPORARY TABLE {}.{} (\n  {}\n){}",
        bq(db),
        bq(t),
        items.join(",\n  "),
        &show[close + 1..]
    ))
}

/// The statements that put a snapshot in place for table `t` of `db`:
/// `create` (from [`temporary_from_show`]), then the rows.
///
/// # Errors
/// 39874 for rows whose columns can't be written.
pub fn snapshot_sql(db: &str, t: &str, create: &str, rows: &[Value]) -> Result<Vec<String>> {
    let name = format!("{}.{}", bq(db), bq(t));
    let mut out = vec![create.to_owned()];
    // One INSERT per distinct column list, in batches of 100 rows.
    let mut batch: Vec<String> = Vec::new();
    let mut cols: Option<Vec<String>> = None;
    let flush = |cols: &Option<Vec<String>>, batch: &mut Vec<String>, out: &mut Vec<String>| {
        if let Some(c) = cols
            && !batch.is_empty()
        {
            let list: Vec<String> = c.iter().map(|x| bq(x)).collect();
            out.push(format!(
                "INSERT INTO {name} ({}) VALUES {}",
                list.join(", "),
                batch.join(", ")
            ));
            batch.clear();
        }
    };
    for r in rows {
        let Value::Object(o) = r else {
            return Err(invalid(format!("rows of {t} are objects")));
        };
        let these: Vec<String> = o.keys().cloned().collect();
        if these.is_empty() {
            return Err(invalid(format!("a row of {t} has no columns")));
        }
        if cols.as_ref() != Some(&these) || batch.len() >= 100 {
            flush(&cols, &mut batch, &mut out);
            cols = Some(these);
        }
        let vals: Vec<String> = o.values().map(sprig_logic_js::bridge::literal).collect();
        batch.push(format!("({})", vals.join(", ")));
    }
    flush(&cols, &mut batch, &mut out);
    Ok(out)
}

/// The statement that removes a snapshot table.
#[must_use]
pub fn snapshot_drop(db: &str, t: &str) -> String {
    format!("DROP TEMPORARY TABLE IF EXISTS {}.{}", bq(db), bq(t))
}

/// The statement that reads a snapshot table back after the run.
#[must_use]
pub fn snapshot_read(db: &str, t: &str) -> String {
    format!(
        "SELECT * FROM {}.{} LIMIT {}",
        bq(db),
        bq(t),
        MAX_TABLE_ROWS + 1
    )
}

/// Builds the report from its parts (see the module docs).
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    /// The database.
    pub database: String,
    /// The module's kind.
    pub kind: String,
    /// Its name.
    pub name: String,
    /// The version run (`None` for a draft).
    pub version: Option<u64>,
    /// A draft's source was run.
    pub draft: bool,
    /// `kirun`, `javascript`, `lua`.
    pub runtime: String,
    /// The tier the deployed version runs in (`mariadb`, `engine`), or
    /// what placement chose for a draft; the test ran on the engine tier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployed_tier: Option<String>,
    /// The event raised.
    pub event: Option<String>,
    /// Its fields.
    pub result: Option<Value>,
    /// The error that ended the run.
    pub error: Option<Value>,
    /// Every statement, in order.
    pub statements: Vec<Statement>,
    /// The statements that wrote, in order (rolled back).
    pub writes: Vec<Statement>,
    /// Log lines.
    pub logs: Vec<String>,
    /// Outbound calls.
    pub fetches: Vec<Fetched>,
    /// The snapshot tables after the run (rolled back), at most
    /// [`MAX_TABLE_ROWS`] rows each.
    pub tables: BTreeMap<String, Value>,
    /// Limits and use.
    pub budget: Value,
    /// Milliseconds: `total`, `setup` (snapshot), `run`, `db`.
    pub timing: Value,
    /// Anything else to know.
    pub notes: Vec<String>,
}

impl Report {
    /// Files the recorder's statements, logs and fetches.
    pub fn fill(&mut self, rec: &Recorder) {
        let (s, l, f) = rec.take();
        self.writes = s
            .iter()
            .filter(|x| x.kind == "write" && x.error.is_none())
            .cloned()
            .collect();
        self.statements = s;
        self.logs = l;
        self.fetches = f;
    }

    /// Milliseconds spent in statements.
    #[must_use]
    pub fn db_ms(&self) -> f64 {
        self.statements.iter().map(|s| s.ms).sum()
    }
}

/// A version to test, read from the catalog.
#[derive(Debug, Clone)]
pub struct Loaded {
    /// The module.
    pub module_id: u64,
    /// The version.
    pub version: u64,
    /// The module's name.
    pub name: String,
    /// `function`, `procedure`, `trigger`, `validation`.
    pub kind: String,
    /// `kirun`, `javascript`, `lua`.
    pub runtime: String,
    /// The version's tier as deployed.
    pub tier: String,
    /// Its source.
    pub source: String,
    /// Its signature.
    pub signature: Value,
}

impl Loaded {
    /// The version as an engine-tier program, whatever tier it deployed
    /// to.
    ///
    /// # Errors
    /// 39861 for source the runtime refuses; 39876 for a runtime the
    /// engine tier lacks.
    pub fn program(&self) -> Result<sprig_logic_js::Program> {
        // A JavaScript validation or after-commit trigger is compiled
        // (jsbind): its recorded parameters are the columns it reads, but
        // its code takes the row by name (`row`, `old`, `ctx`, ...).
        let sig = if self.runtime == "javascript"
            && matches!(self.kind.as_str(), "validation" | "trigger")
            && self.tier == "mariadb"
        {
            js_row_sig(&self.source)
        } else {
            crate::invoke::script_sig(&self.signature)
        };
        sprig_logic_js::Program::new(&self.runtime, &self.name, &self.source, sig)
    }
}

/// Version `version` (the active one when `None`) of the live module
/// `kind` `name` of `db_id`.
///
/// # Errors
/// 39860 for no such module or version; the catalog's.
pub async fn load(
    c: &mut mysql_async::Conn,
    store: &crate::LogicStore,
    db_id: u32,
    kind: &str,
    name: &str,
    version: Option<u64>,
) -> Result<Loaded> {
    use mysql_async::prelude::Queryable;
    let not_found = |m: String| SprigError::new(SprigCode::LogicNotFound, m);
    let m = match store.module(c, db_id, kind, name).await {
        Ok(m) => m,
        Err(e) if format!("{e:?}").contains("1146") => None,
        Err(e) => return Err(e),
    }
    .ok_or_else(|| not_found(format!("no {kind} {name}")))?;
    let v = version
        .or(m.active_version)
        .ok_or_else(|| not_found(format!("{kind} {name} has no active version")))?;
    let q = sprig_common::names::quote(store.schema());
    let row: Option<(String, String, String)> = c
        .exec_first(
            format!(
                "SELECT CAST(tier AS CHAR), source, CAST(signature AS CHAR) FROM {q}.logic_versions \
                 WHERE module_id = ? AND version = ?"
            ),
            (m.module_id, v),
        )
        .await
        .map_err(|e| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "the logic catalog is unreachable",
            )
            .with_source(e)
        })?;
    let (tier, source, signature) =
        row.ok_or_else(|| not_found(format!("{kind} {name} has no version {v}")))?;
    Ok(Loaded {
        module_id: m.module_id,
        version: v,
        name: m.name,
        kind: m.kind,
        runtime: m.runtime,
        tier,
        source,
        signature: serde_json::from_str(&signature).unwrap_or(Value::Null),
    })
}

/// A draft's program: its declaration parsed, its source checked by its
/// runtime (no catalog, nothing recorded).
///
/// # Errors
/// 39861 for a declaration or source that doesn't parse; 39874 when the
/// draft's kind isn't the one asked for.
pub fn draft(
    source: &str,
    kind: &str,
) -> Result<(crate::declare::Declaration, sprig_logic_js::Program)> {
    let d = crate::declare::parse(source)?;
    if d.kind.as_str() != kind {
        return Err(invalid(format!(
            "the draft declares a {}, the test run asks for a {kind}",
            d.kind.as_str()
        )));
    }
    if d.kind == crate::declare::Kind::Schedule {
        return Err(invalid("test the procedure a schedule calls"));
    }
    // Triggers and validations in JavaScript or Lua take one argument,
    // `ctx`, as their jobs call them.
    let sig = if matches!(
        d.kind,
        crate::declare::Kind::Trigger | crate::declare::Kind::Validation
    ) && d.runtime == sprig_logic_compile::Runtime::Javascript
        && d.source.is_none()
    {
        js_row_sig(&d.body)
    } else if matches!(
        d.kind,
        crate::declare::Kind::Trigger | crate::declare::Kind::Validation
    ) {
        sprig_logic_js::ScriptSig {
            params: vec![("ctx".to_owned(), sprig_logic_js::ParamTy::Json)],
            scalar: false,
        }
    } else {
        crate::service::script_sig(&d)
    };
    let p = sprig_logic_js::Program::new(d.runtime.as_str(), &d.name, &d.body, sig)?;
    Ok((d, p))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn sql_form() {
        let r =
            parse_statement(b"TEST FUNCTION shop.quote VERSION 2 (7, -3, 'x', NULL, 2.5, TRUE)")
                .unwrap()
                .unwrap();
        assert_eq!(r.database.as_deref(), Some("shop"));
        assert_eq!(r.name.as_deref(), Some("quote"));
        assert_eq!(r.kind(), "function");
        assert_eq!(r.version, Some(2));
        assert_eq!(r.args, json!([7, -3, "x", null, 2.5, true]));
        let r = parse_statement(
            br#"test trigger audit WITH '{"record": {"key": "k", "value": "v"}, "rows": {"t": [{"a": 1}]}, "name": "ignored"}'"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(r.kind(), "trigger");
        assert_eq!(r.name.as_deref(), Some("audit"));
        assert_eq!(r.record.unwrap()["key"], "k");
        assert_eq!(r.rows["t"][0]["a"], 1);
        assert!(parse_statement(b"TEST SINK s").is_none());
        assert!(parse_statement(b"SELECT 1").is_none());
        assert!(parse_statement(b"TEST FUNCTION f (a)").unwrap().is_err());
        assert!(
            parse_statement(b"TEST FUNCTION f (1) extra")
                .unwrap()
                .is_err()
        );
        assert!(
            parse_statement(b"TEST FUNCTION f WITH 'nope'")
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn marked_statement_round_trips() {
        let r = TestRun {
            database: Some("shop".into()),
            name: Some("quote".into()),
            args: json!({"qty": 3}),
            rows: BTreeMap::from([("products".to_owned(), vec![json!({"id": 7})])]),
            fetch: vec![FetchMock {
                url: "https://api.example/*".into(),
                body: json!({"ok": true}),
                ..FetchMock::default()
            }],
            ..TestRun::default()
        };
        let back = parse_statement(r.statement().as_bytes()).unwrap().unwrap();
        assert_eq!(back, r);
        assert!(back.check().is_ok());
        let bad = TestRun {
            name: Some("x".into()),
            rows: BTreeMap::from([("a;b".to_owned(), vec![])]),
            ..TestRun::default()
        };
        assert!(bad.check().is_err());
        assert!(TestRun::default().check().is_err());
        let sched = TestRun {
            name: Some("x".into()),
            kind: Some("schedule".into()),
            ..TestRun::default()
        };
        assert!(sched.check().is_err());
    }

    #[test]
    fn statements_that_could_escape_are_refused() {
        assert_eq!(classify("SELECT 1"), StmtClass::Read);
        assert_eq!(classify("  /* x */ (SELECT 1)"), StmtClass::Read);
        assert_eq!(
            classify("with a as (select 1) select * from a"),
            StmtClass::Read
        );
        assert_eq!(classify("UPDATE t SET a = 1"), StmtClass::Write);
        assert_eq!(classify("insert into s (key) values (1)"), StmtClass::Write);
        assert_eq!(
            classify("CALL sprig.mq_publish('x', 'k', 'b', NULL)"),
            StmtClass::Write
        );
        assert_eq!(
            classify("CALL `shop`.`quote`(1)"),
            StmtClass::Call("shop.quote".into())
        );
        for s in [
            "COMMIT",
            "rollback",
            "START TRANSACTION",
            "BEGIN",
            "CREATE TABLE x (a INT)",
            "DROP TABLE t",
            "TRUNCATE t",
            "SET autocommit = 1",
            "LOCK TABLES t WRITE",
            "XA START 'x'",
            "CALL sprig.mq_get('q', 1, 1000)",
            "CALL sprig.mq_ack('tok')",
            "LOAD DATA INFILE 'x' INTO TABLE t",
            "DO SLEEP(1)",
        ] {
            assert!(matches!(classify(s), StmtClass::Refused(_)), "{s}");
        }
    }

    #[test]
    fn snapshots_and_trigger_arguments() {
        let show = "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `p` bigint DEFAULT NULL,\n  \
            PRIMARY KEY (`id`),\n  CONSTRAINT `fk_p` FOREIGN KEY (`p`) REFERENCES `p` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        assert_eq!(
            temporary_from_show("shop", "orders", show).unwrap(),
            "CREATE TEMPORARY TABLE `shop`.`orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  \
             `p` bigint DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        assert!(temporary_from_show("shop", "v", "CREATE VIEW `v` AS select 1").is_err());
        let s = snapshot_sql(
            "shop",
            "products",
            "CREATE TEMPORARY TABLE `shop`.`products` (`id` int)",
            &[
                json!({"id": 1, "name": "a"}),
                json!({"id": 2, "name": "b's"}),
                json!({"id": 3}),
            ],
        )
        .unwrap();
        assert_eq!(s[0], "CREATE TEMPORARY TABLE `shop`.`products` (`id` int)");
        assert_eq!(s.len(), 3);
        assert!(s[1].starts_with("INSERT INTO `shop`.`products` (`id`, `name`) VALUES (1, "));
        assert!(s[2].starts_with("INSERT INTO `shop`.`products` (`id`) VALUES (3)"));
        let req = TestRun {
            row: Some(json!({"Qty": 3, "status": "paid"})),
            old: Some(json!({"qty": 1})),
            op: Some("update".into()),
            ..TestRun::default()
        };
        let params = vec![
            "qty".to_owned(),
            "old_qty".to_owned(),
            "op".to_owned(),
            "x".to_owned(),
        ];
        let (args, ctx) = trigger_args(&req, "kirun", &params, &json!({"user": "u"}));
        assert_eq!(args, vec![json!(3), json!(1), json!("update"), Value::Null]);
        assert_eq!(ctx["trigger"]["op"], "update");
        let (args, _) = trigger_args(&req, "javascript", &[], &json!({}));
        assert_eq!(args.len(), 1);
        assert_eq!(args[0]["trigger"]["after"]["status"], "paid");
        // JavaScript by name (jsbind): the book's (row, old), and a body.
        let names = |src: &str| -> Vec<String> {
            js_row_sig(src).params.into_iter().map(|(n, _)| n).collect()
        };
        let p = names("export default function (row, old, op) {}");
        assert_eq!(p, ["row", "old", "op"]);
        let (args, _) = trigger_args(&req, "javascript", &p, &json!({}));
        assert_eq!(args[0]["status"], "paid");
        assert_eq!(args[2], json!("update"));
        let p = names("if (old && old.qty > row.qty) sprig.reject('x');");
        assert_eq!(p, JS_ROW_NAMES);
        let (args, _) = trigger_args(&req, "javascript", &p, &json!({}));
        assert_eq!(args[3]["trigger"]["op"], "update");
        let p = names("export default function (ctx) {}");
        let (args, _) = trigger_args(&req, "javascript", &p, &json!({}));
        assert_eq!(args.len(), 1);
        assert_eq!(args[0]["trigger"]["op"], "update");
        let req = TestRun {
            record: Some(json!({"key": "k", "value": "{}"})),
            ..TestRun::default()
        };
        let (args, ctx) = trigger_args(&req, "lua", &[], &json!({}));
        assert_eq!(args[0]["record"]["key"], "k");
        assert_eq!(ctx["record"]["key"], "k");
        assert_eq!(
            positional(&json!({"b": 2}), &["a".into(), "b".into()], "f").unwrap(),
            vec![Value::Null, json!(2)]
        );
        assert!(positional(&json!({"c": 2}), &["a".into()], "f").is_err());
    }

    #[test]
    fn drafts() {
        let (d, p) = draft(
            "CREATE FUNCTION add2(a INT, b INT) RETURNS INT LANGUAGE javascript AS $$ return a + b; $$",
            "function",
        )
        .unwrap();
        assert_eq!(d.name, "add2");
        assert!(matches!(p, sprig_logic_js::Program::Js(_)));
        let (_, t) = draft(
            "CREATE TRIGGER t ON STREAM s LANGUAGE javascript AS $$ return {k: ctx.record.key}; $$",
            "trigger",
        )
        .unwrap();
        assert_eq!(t.coerce("t", vec![json!({})]).unwrap().len(), 1);
        assert!(
            draft(
                "CREATE PROCEDURE p LANGUAGE lua AS $$ return {} $$",
                "function"
            )
            .is_err()
        );
        assert!(
            draft(
                "CREATE FUNCTION f() LANGUAGE javascript AS $$ return ( $$",
                "function"
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn fetch_is_mocked_never_sent() {
        let rec = std::sync::Arc::new(Recorder::default());
        let o = MockOutbound {
            policy: OutboundPolicy {
                allow: vec!["api.example".into()],
                ..OutboundPolicy::default()
            },
            mocks: vec![FetchMock {
                method: "GET".into(),
                url: "https://api.example/rates*".into(),
                status: 201,
                body: json!({"rate": 2}),
                ..FetchMock::default()
            }],
            rec: std::sync::Arc::clone(&rec),
        };
        let req = |url: &str| Request {
            method: "GET".into(),
            url: url.into(),
            headers: Vec::new(),
            body: None,
        };
        let r = o.fetch(req("https://api.example/rates/usd")).await.unwrap();
        assert_eq!(r.status, 201);
        assert_eq!(r.body, r#"{"rate":2}"#);
        let e = o.fetch(req("https://evil.example/x")).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicOutboundDenied));
        let e = o.fetch(req("https://api.example/other")).await.unwrap_err();
        assert!(e.message.contains("give a mock"));
        let (_, _, f) = rec.take();
        assert_eq!(f.len(), 3);
        assert_eq!(f[0].status, Some(201));
        assert!(f[1].error.is_some());
    }
}
