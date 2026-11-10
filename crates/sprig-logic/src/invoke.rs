//! Calling an engine-tier version (chapter 41, "Calling code"; build step
//! 15 in part): the same `CALL db.name(…)` a compiled version answers.
//!
//! An engine-tier version's wrapper ([`crate::deploy::engine_wrapper`])
//! lets MariaDB do what it does for any `CALL`: resolve the name in the
//! session's schema, check `EXECUTE` (chapter 29), and convert the
//! arguments to the declared parameter types. It then returns one
//! **marker** row, `__sprig_engine, module, version, args`, and fails with
//! 39876. The gateway sees the marker, drops it and the error, runs the
//! version on the fallback runtime with the caller's session as its `db`,
//! and answers with the row a compiled wrapper returns: `event`, then
//! every event's fields, typed as the wrapper types them.
//!
//! ```text
//! client ─ CALL shop.quote(7, 3) ─► gateway ─► MariaDB: wrapper `quote`
//!   ◄── marker (module 44, version 2, [7, 3]); ERR 39876 ──┘
//! gateway: version 44/2 from the cache (else the catalog), KIRun run,
//!   each Sprig.Db.* statement through the caller's own tenant session
//! client ◄── event | total ; OK
//! ```
//!
//! A gateway without the runtime passes the marker and 39876 to the
//! client, which then sees a clear error.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_js::{ParamTy, Program, ScriptSig};

/// The marker column's name.
pub const MARKER: &str = "__sprig_engine";

/// What a marker row says.
#[derive(Debug, Clone, PartialEq)]
pub struct Marker {
    /// The module.
    pub module_id: u64,
    /// The version.
    pub version: u64,
    /// The arguments, as MariaDB converted them.
    pub args: Vec<Value>,
}

impl Marker {
    /// Reads a marker row: `__sprig_engine`, `module`, `version`, `args`.
    #[must_use]
    pub fn parse(columns: &[String], row: &[Option<String>]) -> Option<Self> {
        if columns.first().map(String::as_str) != Some(MARKER) || row.len() < 4 {
            return None;
        }
        let module_id = row[1].as_deref()?.parse().ok()?;
        let version = row[2].as_deref()?.parse().ok()?;
        let args = match row[3].as_deref() {
            Some(t) => serde_json::from_str::<Vec<Value>>(t).ok()?,
            None => Vec::new(),
        };
        Some(Self {
            module_id,
            version,
            args,
        })
    }
}

/// An output column's type, as the compiled wrapper types it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutTy {
    /// `event`: `VARCHAR(64)`.
    Event,
    /// `i64` and `bool`: `BIGINT`.
    Int,
    /// `f64`: `DOUBLE`.
    Float,
    /// `str`: text.
    Str,
    /// `json` and `null`: JSON text.
    Json,
}

/// One output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutCol {
    /// Its name.
    pub name: String,
    /// Its type.
    pub ty: OutTy,
}

/// An engine-tier version, ready to call.
#[derive(Debug)]
pub struct EngineVersion {
    /// The module.
    pub module_id: u64,
    /// The version.
    pub version: u64,
    /// The function's name.
    pub name: String,
    /// Its database.
    pub db_id: u32,
    /// `kirun`, `javascript`, ...
    pub runtime: String,
    /// `SQL SECURITY DEFINER`.
    pub definer: bool,
    /// The tier placement chose.
    pub tier: String,
    /// The parsed version, in its runtime.
    pub program: Option<Program>,
    /// The result row's declared columns (see [`EngineVersion::columns_for`]).
    pub columns: Vec<OutCol>,
}

impl EngineVersion {
    /// Checks that this gateway can run the version.
    ///
    /// # Errors
    /// 39876 for a runtime this build lacks or for `SQL SECURITY
    /// DEFINER` (the fallback tier runs with the caller's session only,
    /// so far).
    pub fn runnable(&self) -> Result<&Program> {
        let unavailable = |why: &str| {
            Err(SprigError::new(
                SprigCode::LogicEngineUnavailable,
                format!("{} version {}: {why}", self.name, self.version),
            ))
        };
        if self.definer {
            return unavailable(
                "SQL SECURITY DEFINER isn't supported in the engine tier yet; deploy it SQL SECURITY INVOKER",
            );
        }
        match &self.program {
            Some(p) => Ok(p),
            None => unavailable(&format!("the {} runtime isn't built yet", self.runtime)),
        }
    }

    /// The result's columns: the declared ones, then any field of
    /// `result` they lack (JavaScript and Lua procedures declare none),
    /// typed by its value.
    #[must_use]
    pub fn columns_for(&self, result: &Value) -> Vec<OutCol> {
        let mut cols = self.columns.clone();
        if let Value::Object(o) = result {
            for (k, v) in o {
                if k == "event" || cols.iter().any(|c| &c.name == k) {
                    continue;
                }
                let ty = match v {
                    Value::Bool(_) => OutTy::Int,
                    Value::Number(n) if n.is_i64() || n.is_u64() => OutTy::Int,
                    Value::Number(_) => OutTy::Float,
                    Value::String(_) => OutTy::Str,
                    _ => OutTy::Json,
                };
                cols.push(OutCol {
                    name: k.clone(),
                    ty,
                });
            }
        }
        cols
    }

    /// [`EngineVersion::row`] over `cols` ([`EngineVersion::columns_for`]).
    #[must_use]
    pub fn row_with(cols: &[OutCol], event: &str, result: &Value) -> Vec<Option<String>> {
        cols.iter()
            .map(|c| match c.ty {
                OutTy::Event => Some(event.to_owned()),
                ty => cell(result.get(&c.name), ty),
            })
            .collect()
    }

    /// The result row for an event and its fields, as text (`None` is
    /// `NULL`), formatted as MariaDB formats the compiled wrapper's row.
    #[must_use]
    pub fn row(&self, event: &str, result: &Value) -> Vec<Option<String>> {
        self.columns
            .iter()
            .map(|c| match c.ty {
                OutTy::Event => Some(event.to_owned()),
                ty => cell(result.get(&c.name), ty),
            })
            .collect()
    }
}

fn cell(v: Option<&Value>, ty: OutTy) -> Option<String> {
    let v = v?;
    if v.is_null() {
        return None;
    }
    Some(match ty {
        OutTy::Int => match v {
            Value::Bool(b) => i64::from(*b).to_string(),
            Value::Number(n) => n.as_i64().map_or_else(
                || {
                    #[allow(clippy::cast_possible_truncation)]
                    let t = n.as_f64().unwrap_or(0.0).trunc() as i64;
                    t.to_string()
                },
                |i| i.to_string(),
            ),
            Value::String(s) => s.trim().parse::<f64>().map_or_else(
                |_| "0".to_owned(),
                |x| {
                    #[allow(clippy::cast_possible_truncation)]
                    let t = x.trunc() as i64;
                    t.to_string()
                },
            ),
            other => mariadb_json(other),
        },
        OutTy::Float => match v {
            Value::Bool(b) => i64::from(*b).to_string(),
            Value::Number(n) => double_text(n.as_f64().unwrap_or(0.0)),
            Value::String(s) => double_text(s.trim().parse::<f64>().unwrap_or(0.0)),
            other => mariadb_json(other),
        },
        OutTy::Str => match v {
            Value::String(s) => s.clone(),
            other => mariadb_json(other),
        },
        OutTy::Json | OutTy::Event => mariadb_json(v),
    })
}

/// A double as MariaDB's text protocol prints it.
#[allow(clippy::float_cmp)] // whole numbers print without a fraction
fn double_text(x: f64) -> String {
    if x == x.trunc() && x.abs() < 1e15 {
        #[allow(clippy::cast_possible_truncation)]
        let i = x as i64;
        i.to_string()
    } else {
        let s = format!("{x}");
        if x.abs() >= 1e15 || (x != 0.0 && x.abs() < 1e-4) {
            format!("{x:e}")
        } else {
            s
        }
    }
}

/// JSON as MariaDB writes it (`JSON_OBJECT`, `JSON_EXTRACT`): `, ` and
/// `: ` between members.
#[must_use]
pub fn mariadb_json(v: &Value) -> String {
    match v {
        Value::Array(a) => {
            let parts: Vec<String> = a.iter().map(mariadb_json).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(o) => {
            let parts: Vec<String> = o
                .iter()
                .map(|(k, v)| format!("{}: {}", Value::String(k.clone()), mariadb_json(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        other => other.to_string(),
    }
}

/// The wrapper's columns from a version's stored signature: `event`, then
/// every event's fields once each, in event order.
#[must_use]
pub fn columns(signature: &Value) -> Vec<OutCol> {
    let mut out = vec![OutCol {
        name: "event".to_owned(),
        ty: OutTy::Event,
    }];
    for e in signature["events"].as_array().into_iter().flatten() {
        for f in e["fields"].as_array().into_iter().flatten() {
            let Some(name) = f["name"].as_str() else {
                continue;
            };
            if name == "event" || out.iter().any(|c| c.name == name) {
                continue;
            }
            let ty = match f["type"].as_str() {
                Some("i64" | "bool") => OutTy::Int,
                Some("f64") => OutTy::Float,
                Some("str") => OutTy::Str,
                _ => OutTy::Json,
            };
            out.push(OutCol {
                name: name.to_owned(),
                ty,
            });
        }
    }
    out
}

/// A JavaScript or Lua version's signature, from its stored JSON.
#[must_use]
pub fn script_sig(signature: &Value) -> ScriptSig {
    ScriptSig {
        params: signature["params"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| {
                let ty = match p["type"].as_str() {
                    Some("i64") => ParamTy::Int,
                    Some("f64") => ParamTy::Float,
                    Some("bool") => ParamTy::Bool,
                    Some("json") => ParamTy::Json,
                    _ => ParamTy::Str,
                };
                (p["name"].as_str().unwrap_or_default().to_owned(), ty)
            })
            .collect(),
        scalar: signature["scalar"].as_bool().unwrap_or(false),
    }
}

/// The value `SELECT f(…)` and scalar callers see for a call's outcome:
/// the `output` event's `value` for a function declared `RETURNS`, else
/// its only field, else its fields as one JSON object.
///
/// # Errors
/// 39865 when the call raised another event (`error`, ...), with its
/// `message` field or its fields as text.
pub fn scalar(event: &str, result: &Value) -> Result<Value> {
    if event != "output" {
        let msg = result
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| mariadb_json(result), str::to_owned);
        return Err(SprigError::new(
            SprigCode::LogicUserError,
            format!("{event}: {msg}"),
        ));
    }
    Ok(match result {
        Value::Object(o) if o.len() == 1 => o.values().next().cloned().unwrap_or(Value::Null),
        Value::Object(o) if o.is_empty() => Value::Null,
        other => other.clone(),
    })
}

// ---- calls from other protocols (R7 order 6) -------------------------------

/// The statement text of a call from outside SQL starts with this; the
/// rest is the [`FunctionCall`] as JSON. Each front end sends it through
/// a tenant session, which finds the function, checks its exposure, and
/// runs `CALL db.name(...)` (chapter 41, "Calling code").
pub const CALL_PREFIX: &str = "/* sprig function call */ ";

/// A call of a logic function from HTTP, gRPC, MongoDB or Redis.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    /// `http`, `grpc`, `mongo` or `redis`: the exposure it needs.
    pub protocol: String,
    /// The function's name, in the session's database.
    pub name: String,
    /// A list (by position), an object (by parameter name), one value,
    /// or `null` for none.
    pub args: Value,
    /// The function's database when the front end names it (MongoDB's
    /// `$db`, Redis's logical database or a `db.name` in `FCALL`);
    /// `None` uses the session's current database.
    pub db: Option<String>,
    /// Redis's `FCALL_RO`: the call runs in a read-only transaction, so
    /// a write fails (1792) whatever the tier.
    pub read_only: bool,
}

impl FunctionCall {
    /// The statement a front end sends for this call.
    #[must_use]
    pub fn statement(&self) -> String {
        let mut v =
            serde_json::json!({"protocol": self.protocol, "name": self.name, "args": self.args});
        if let Some(db) = &self.db {
            v["db"] = Value::String(db.clone());
        }
        if self.read_only {
            v["read_only"] = Value::Bool(true);
        }
        format!("{CALL_PREFIX}{v}")
    }

    /// The call in a statement text, if it is one.
    #[must_use]
    pub fn parse(text: &[u8]) -> Option<Self> {
        let rest = text.strip_prefix(CALL_PREFIX.as_bytes())?;
        let v: Value = serde_json::from_slice(rest).ok()?;
        Some(Self {
            protocol: v["protocol"].as_str().unwrap_or("http").to_owned(),
            name: v["name"].as_str().unwrap_or_default().to_owned(),
            args: v["args"].clone(),
            db: v["db"].as_str().map(str::to_owned),
            read_only: v["read_only"].as_bool().unwrap_or(false),
        })
    }

    /// MongoDB's `db.runCommand({sprigCall: "quote", args: [7, 3]})`:
    /// the function of the command's database (`$db`, when the command
    /// carries it), by position (`args: [...]`) or by name
    /// (`args: {...}`).
    ///
    /// # Errors
    /// 39860 when the command has no `sprigCall` name.
    pub fn from_mongo(command: &Value) -> Result<Self> {
        let name = command["sprigCall"].as_str().ok_or_else(|| {
            SprigError::new(SprigCode::LogicNotFound, "sprigCall takes a function name")
        })?;
        Ok(Self {
            protocol: "mongo".to_owned(),
            name: name.to_owned(),
            args: command.get("args").cloned().unwrap_or(Value::Null),
            db: command["$db"].as_str().map(str::to_owned),
            read_only: false,
        })
    }

    /// Redis's `FCALL name numkeys key... arg...` of a logic function
    /// exposed to `redis` (chapter 40's front end calls this after its own
    /// Lua libraries miss): the keys bind the leading parameters, then the
    /// arguments, all as text. `name` may be `db.name`; otherwise the
    /// front end sets [`Self::db`] to its logical database.
    ///
    /// # Errors
    /// 39874 for a bad `numkeys`.
    pub fn from_redis_fcall(argv: &[Vec<u8>]) -> Result<Self> {
        let bad = || SprigError::new(SprigCode::LogicSignatureMismatch, "FCALL: bad numkeys");
        let name = argv.first().ok_or_else(bad)?;
        let numkeys: usize = argv
            .get(1)
            .and_then(|n| std::str::from_utf8(n).ok())
            .and_then(|n| n.parse().ok())
            .ok_or_else(bad)?;
        if numkeys > argv.len().saturating_sub(2) {
            return Err(bad());
        }
        let full = String::from_utf8_lossy(name).into_owned();
        let (db, name) = match full.split_once('.') {
            Some((d, n)) if !d.is_empty() && !n.is_empty() => (Some(d.to_owned()), n.to_owned()),
            _ => (None, full),
        };
        Ok(Self {
            protocol: "redis".to_owned(),
            name,
            db,
            read_only: false,
            args: Value::Array(
                argv[2..]
                    .iter()
                    .map(|a| Value::String(String::from_utf8_lossy(a).into_owned()))
                    .collect(),
            ),
        })
    }
}

// ---- SELECT f(constants) of engine-tier functions (R7 order 6) -------------

/// One call of a function with constant arguments in a `SELECT` without
/// `FROM`.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectCall {
    /// The database, when the name was qualified.
    pub db: Option<String>,
    /// The function's name.
    pub name: String,
    /// The arguments.
    pub args: Vec<Value>,
    /// Where the call is in the text (bytes).
    pub range: std::ops::Range<usize>,
    /// The call's text, when it is a whole select item without an alias:
    /// the replacement keeps it as the column's name.
    pub alias: Option<String>,
}

/// Whether `text` may be a `SELECT` without `FROM` that calls something:
/// it has a `(` and no `FROM` word outside quotes. Cheap, so the gateway
/// can ask for every `SELECT`.
#[must_use]
pub fn select_candidate(text: &[u8]) -> bool {
    let mut paren = false;
    let mut i = 0;
    while i < text.len() {
        let c = text[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < text.len() && text[i] != c {
                    if text[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'(' => paren = true,
            b'f' | b'F' => {
                let word = text
                    .get(i..i + 4)
                    .is_some_and(|w| w.eq_ignore_ascii_case(b"from"));
                let before =
                    i == 0 || !(text[i - 1].is_ascii_alphanumeric() || text[i - 1] == b'_');
                let after = text
                    .get(i + 4)
                    .is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'));
                if word && before && after {
                    return false;
                }
            }
            _ => {}
        }
        i += 1;
    }
    paren
}

fn byte_offset(text: &str, loc: sqlparser::tokenizer::Location) -> Option<usize> {
    if loc.line == 0 {
        return None;
    }
    let mut line = 1u64;
    let mut start = 0usize;
    for (i, c) in text.char_indices() {
        if line == loc.line {
            break;
        }
        if c == '\n' {
            line += 1;
            start = i + 1;
        }
    }
    if line != loc.line {
        return None;
    }
    let col = usize::try_from(loc.column.saturating_sub(1)).ok()?;
    text[start..]
        .char_indices()
        .nth(col)
        .map(|(i, _)| start + i)
        .or_else(|| (col == text[start..].chars().count()).then_some(text.len()))
}

/// The end of the call whose name starts at `from`: just past the `)`
/// matching the first `(` after it.
fn call_end(text: &str, from: usize) -> Option<usize> {
    let b = text.as_bytes();
    let mut i = from + text[from..].find('(')?;
    let mut depth = 0usize;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < b.len() && b[i] != q {
                    if b[i] == b'\\' && q != b'`' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn const_value(e: &sqlparser::ast::Expr) -> Option<Value> {
    use sqlparser::ast::{Expr, UnaryOperator, Value as V};
    match e {
        Expr::Nested(x) => const_value(x),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match const_value(expr)? {
            Value::Number(n) => n
                .as_i64()
                .map(|i| Value::from(-i))
                .or_else(|| n.as_f64().map(|f| Value::from(-f))),
            _ => None,
        },
        Expr::Value(v) => Some(match &v.value {
            V::Number(n, _) => n
                .parse::<i64>()
                .map(Value::from)
                .or_else(|_| n.parse::<f64>().map(Value::from))
                .ok()?,
            V::SingleQuotedString(s) | V::DoubleQuotedString(s) => Value::String(s.clone()),
            V::Boolean(b) => Value::Bool(*b),
            V::Null => Value::Null,
            _ => return None,
        }),
        _ => None,
    }
}

/// The calls with constant arguments in `text`, a `SELECT` without `FROM`
/// or `WHERE`; `None` for anything else.
#[must_use]
pub fn select_calls(text: &str) -> Option<Vec<SelectCall>> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart, SelectItem, SetExpr,
        Spanned, Statement, Visit, Visitor,
    };
    struct Calls<'a> {
        text: &'a str,
        whole: Option<usize>,
        out: Vec<SelectCall>,
    }
    impl Visitor for Calls<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> std::ops::ControlFlow<()> {
            let Expr::Function(f) = e else {
                return std::ops::ControlFlow::Continue(());
            };
            let parts: Vec<String> = f
                .name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                    ObjectNamePart::Function(_) => None,
                })
                .collect();
            let args = match &f.args {
                FunctionArguments::List(l)
                    if l.duplicate_treatment.is_none() && l.clauses.is_empty() =>
                {
                    l.args
                        .iter()
                        .map(|a| match a {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => const_value(x),
                            _ => None,
                        })
                        .collect::<Option<Vec<Value>>>()
                }
                _ => None,
            };
            let start = f
                .name
                .0
                .first()
                .and_then(|p| byte_offset(self.text, p.span().start));
            if let (Some(args), Some(start), 1..=2) = (args, start, parts.len())
                && let Some(end) = call_end(self.text, start)
            {
                let (db, name) = if parts.len() == 2 {
                    (Some(parts[0].clone()), parts[1].clone())
                } else {
                    (None, parts[0].clone())
                };
                let alias = (self.whole == Some(start)).then(|| self.text[start..end].to_owned());
                self.out.push(SelectCall {
                    db,
                    name,
                    args,
                    range: start..end,
                    alias,
                });
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, text).ok()?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return None;
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    if !sel.from.is_empty() || sel.selection.is_some() {
        return None;
    }
    let mut calls = Calls {
        text,
        whole: None,
        out: Vec::new(),
    };
    for item in &sel.projection {
        let (e, whole) = match item {
            SelectItem::UnnamedExpr(e) => (e, true),
            SelectItem::ExprWithAlias { expr, .. } => (expr, false),
            _ => continue,
        };
        calls.whole = match e {
            Expr::Function(f) if whole => f
                .name
                .0
                .first()
                .and_then(|p| byte_offset(text, p.span().start)),
            _ => None,
        };
        let _ = e.visit(&mut calls);
    }
    // Nested calls inside a replaced call go with it.
    let mut out: Vec<SelectCall> = Vec::new();
    for c in calls.out {
        if !out
            .iter()
            .any(|o| o.range.start <= c.range.start && c.range.end <= o.range.end)
        {
            out.push(c);
        }
    }
    Some(out)
}

/// `text` with each range replaced (ranges don't overlap).
#[must_use]
pub fn splice(text: &str, mut edits: Vec<(std::ops::Range<usize>, String)>) -> String {
    edits.sort_by_key(|(r, _)| std::cmp::Reverse(r.start));
    let mut s = text.to_owned();
    for (r, with) in edits {
        if r.end <= s.len() && s.is_char_boundary(r.start) && s.is_char_boundary(r.end) {
            s.replace_range(r, &with);
        }
    }
    s
}

type Key = (u64, u64, u64);

/// Versions already loaded, by (tenant, module, version). Versions never
/// change once recorded, so entries never go stale; the cache is bounded
/// by clearing it whole.
static CACHE: LazyLock<Mutex<HashMap<Key, Arc<EngineVersion>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Entries kept before the cache is cleared.
const CACHE_MAX: usize = 1024;

/// A version already loaded.
#[must_use]
pub fn cached(tenant: u64, module_id: u64, version: u64) -> Option<Arc<EngineVersion>> {
    CACHE
        .lock()
        .ok()
        .and_then(|m| m.get(&(tenant, module_id, version)).cloned())
}

/// Loads version `version` of `module_id` from the tenant's logic catalog
/// (`t<tenant>_sprig`), parsing a KIRun definition once.
///
/// # Errors
/// 39860 when there is no such engine-tier version; 39861 for a source
/// that no longer parses; 39070 or a backend error.
pub async fn load(
    c: &mut Conn,
    catalog_schema: &str,
    tenant: u64,
    module_id: u64,
    version: u64,
) -> Result<Arc<EngineVersion>> {
    let key = (tenant, module_id, version);
    if let Some(v) = CACHE.lock().ok().and_then(|m| m.get(&key).cloned()) {
        return Ok(v);
    }
    let q = sprig_common::names::quote(catalog_schema);
    let row: Option<(String, u32, String, String, String, String, String)> = c
        .exec_first(
            format!(
                "SELECT m.name, m.db_id, m.runtime, m.security, v.tier, v.source, CAST(v.signature AS CHAR) \
                 FROM {q}.logic_versions v JOIN {q}.logic_modules m ON m.module_id = v.module_id \
                 WHERE v.module_id = ? AND v.version = ?"
            ),
            (module_id, version),
        )
        .await
        .map_err(|e| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "the logic catalog is unreachable",
            )
            .with_source(e)
        })?;
    let Some((name, db_id, runtime, security, tier, source, signature)) = row else {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            format!("logic module {module_id} has no version {version}"),
        ));
    };
    if tier != "engine" {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            format!("{name} version {version} isn't an engine-tier version"),
        ));
    }
    let sig: Value = serde_json::from_str(&signature).unwrap_or_default();
    let program = match runtime.as_str() {
        "kirun" | "javascript" | "lua" => {
            Some(Program::new(&runtime, &name, &source, script_sig(&sig))?)
        }
        _ => None,
    };
    let v = Arc::new(EngineVersion {
        module_id,
        version,
        name,
        db_id,
        runtime,
        definer: security == "definer",
        tier,
        program,
        columns: columns(&sig),
    });
    if let Ok(mut m) = CACHE.lock() {
        if m.len() >= CACHE_MAX {
            m.clear();
        }
        m.insert(key, Arc::clone(&v));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn markers_and_rows() {
        let cols: Vec<String> = ["__sprig_engine", "module", "version", "args"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let m = Marker::parse(
            &cols,
            &[
                Some("sprig".into()),
                Some("44".into()),
                Some("2".into()),
                Some("[7, \"x\", null]".into()),
            ],
        )
        .unwrap();
        assert_eq!(m.module_id, 44);
        assert_eq!(m.args, vec![json!(7), json!("x"), Value::Null]);
        assert!(Marker::parse(&cols[1..], &[]).is_none());

        let sig = json!({"events": [
            {"name": "output", "fields": [{"name": "total", "type": "f64"}, {"name": "n", "type": "i64"}, {"name": "rows", "type": "json"}]},
            {"name": "error", "fields": [{"name": "message", "type": "str"}, {"name": "n", "type": "i64"}]}]});
        let v = EngineVersion {
            module_id: 44,
            version: 2,
            name: "quote".into(),
            db_id: 1,
            runtime: "kirun".into(),
            definer: false,
            tier: "engine".into(),
            program: None,
            columns: columns(&sig),
        };
        let names: Vec<&str> = v.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["event", "total", "n", "rows", "message"]);
        let row = v.row(
            "output",
            &json!({"total": 1240.0, "n": true, "rows": [{"a": 1, "b": "x"}]}),
        );
        assert_eq!(
            row,
            vec![
                Some("output".into()),
                Some("1240".into()),
                Some("1".into()),
                Some("[{\"a\": 1, \"b\": \"x\"}]".into()),
                None
            ]
        );
        assert!(v.runnable().is_err());
    }

    #[test]
    fn calls_from_other_protocols() {
        let c = FunctionCall {
            protocol: "http".into(),
            name: "quote".into(),
            args: json!({"qty": 3, "productId": 7}),
            db: None,
            read_only: false,
        };
        assert_eq!(FunctionCall::parse(c.statement().as_bytes()).unwrap(), c);
        let ro = FunctionCall {
            db: Some("shop".into()),
            read_only: true,
            ..c.clone()
        };
        assert_eq!(FunctionCall::parse(ro.statement().as_bytes()).unwrap(), ro);
        assert!(FunctionCall::parse(b"SELECT 1").is_none());
        let m =
            FunctionCall::from_mongo(&json!({"sprigCall": "quote", "args": [7, 3], "$db": "shop"}))
                .unwrap();
        assert_eq!(
            (m.protocol.as_str(), m.args.clone(), m.db.as_deref()),
            ("mongo", json!([7, 3]), Some("shop"))
        );
        let r = FunctionCall::from_redis_fcall(&[
            b"quote".to_vec(),
            b"1".to_vec(),
            b"7".to_vec(),
            b"3".to_vec(),
        ])
        .unwrap();
        assert_eq!(
            (r.name.as_str(), r.db.as_deref(), r.args),
            ("quote", None, json!(["7", "3"]))
        );
        let q = FunctionCall::from_redis_fcall(&[b"shop.quote".to_vec(), b"0".to_vec()]).unwrap();
        assert_eq!((q.name.as_str(), q.db.as_deref()), ("quote", Some("shop")));
        assert!(FunctionCall::from_redis_fcall(&[b"q".to_vec(), b"5".to_vec()]).is_err());
    }

    #[test]
    fn select_calls_with_constants() {
        assert!(select_candidate(b"SELECT shop.quote(7, 3)"));
        assert!(!select_candidate(b"SELECT f(1) FROM t"));
        assert!(select_candidate(b"SELECT f('from'), `from`(2)"));
        assert!(!select_candidate(b"SELECT 1"));
        let t = "SELECT shop.quote(7, -3) , total(2.5) AS t, upper(x(1)), f(col), g('it''s', NULL, TRUE)";
        let c = select_calls(t).unwrap();
        let got: Vec<(Option<&str>, &str, &str, Option<&str>)> = c
            .iter()
            .map(|c| {
                (
                    c.db.as_deref(),
                    c.name.as_str(),
                    &t[c.range.clone()],
                    c.alias.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                (
                    Some("shop"),
                    "quote",
                    "shop.quote(7, -3)",
                    Some("shop.quote(7, -3)")
                ),
                (None, "total", "total(2.5)", None),
                (None, "x", "x(1)", None),
                (
                    None,
                    "g",
                    "g('it''s', NULL, TRUE)",
                    Some("g('it''s', NULL, TRUE)")
                ),
            ]
        );
        assert_eq!(c[0].args, vec![json!(7), json!(-3)]);
        assert_eq!(c[3].args, vec![json!("it's"), Value::Null, json!(true)]);
        assert!(select_calls("SELECT f(1) FROM t").is_none());
        let s = splice(
            "SELECT\n  é(1), f(2)",
            vec![(9..14, "7 AS `é(1)`".into()), (16..20, "8".into())],
        );
        assert_eq!(s, "SELECT\n  7 AS `é(1)`, 8");
        let c = select_calls("SELECT\n  é(1), f(2)").unwrap();
        assert_eq!(c[0].range, 9..14);
        assert_eq!(c[1].range, 16..20);
        assert_eq!(
            scalar("output", &json!({"total": 1240})).unwrap(),
            json!(1240)
        );
        assert_eq!(
            scalar("output", &json!({"a": 1, "b": 2})).unwrap(),
            json!({"a": 1, "b": 2})
        );
        assert_eq!(
            scalar("error", &json!({"message": "not enough stock"}))
                .unwrap_err()
                .message,
            "error: not enough stock"
        );
    }
}
