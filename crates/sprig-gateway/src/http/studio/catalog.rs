//! `Catalog` and `Sql.Check` (chapter 37, "APIs for Studio"; chapter 44,
//! "Data structures and storage" and "Editors and completion").
//!
//! | Route | RPC | Does |
//! |---|---|---|
//! | `GET /v1/catalog` | `Catalog.ListObjects` (databases) | The logical databases the caller can see |
//! | `GET /v1/databases/{db}/catalog` | `Catalog.Snapshot` | Tables, views, routines and columns of one database |
//! | `GET /v1/databases/{db}/catalog/{kind}/{name}` | `Catalog.GetObject` | One object: columns, indexes, definition |
//! | `POST /v1/databases/{db}/sql:check` | `Sql.Check` | Each statement's class, and its errors with positions, without running it |
//!
//! **Catalog.** Every answer is read with the caller's own rights (the same
//! `information_schema` the MySQL front end serves, so privilege filtering
//! and name virtualization are chapter 29's and chapter 07's), and carries
//! an `ETag` built from the tenant's catalog version and auth version: DDL
//! moves the first, grants and account changes the second. A request with a
//! matching `If-None-Match` gets 304 without reading anything, so Studio's
//! 60-second refetch costs one cached lookup while nothing changed.
//!
//! **Sql.Check.** Statements are split at semicolons outside quotes and
//! comments, then each one is
//!
//! 1. classified by its leading words (`read`, `write`, `ddl`, `account`,
//!    `transaction`, `other`), which `studio.confirm_writes` uses;
//! 2. parsed with sqlparser-rs's MySQL dialect: a syntax error gets its line
//!    and column;
//! 3. bound, for reads and writes, by `EXPLAIN` run as the caller: names,
//!    columns and privileges are the server's verdict, and nothing runs.
//!
//! A statement the parser refuses but the server accepts (a MariaDB form
//! sqlparser-rs doesn't know) gets no marker; one only the parser can judge
//! (DDL) gets a warning, never an error, so the checker never blocks valid
//! SQL. Decision ST19 in `decisions/studio-interim.md`.

use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use sprig_common::SprigError;
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{hdr, json_response, parse_json, quote_literal, read_body, rows_of, run_sql, text};

/// Statements one check looks at.
pub const MAX_CHECK_STATEMENTS: usize = 50;
/// Longest text one check takes.
pub const MAX_CHECK_BYTES: usize = 256 * 1024;

const SYSTEM: &str = "('information_schema','mysql','performance_schema','sys')";

// ---- statements ----

/// One statement of a text, with UTF-16 offsets (what the browser's editor
/// counts) and byte offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stmt {
    /// The text, trimmed.
    pub text: String,
    /// UTF-16 offset of its first character.
    pub start: usize,
    /// UTF-16 offset after its last character.
    pub end: usize,
}

/// Splits `sql` at semicolons outside quotes, backquotes and comments.
#[must_use]
pub fn split(sql: &str) -> Vec<Stmt> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let mut begin = 0;
    let push = |out: &mut Vec<Stmt>, from: usize, to: usize| {
        let raw: String = chars[from..to].iter().collect();
        let lead = raw.len() - raw.trim_start().len();
        let text = raw.trim().to_owned();
        if text.is_empty() {
            return;
        }
        let u16_at = |n: usize| chars[..n].iter().map(|c| c.len_utf16()).sum::<usize>();
        let lead_chars = raw[..lead].chars().count();
        let start = u16_at(from + lead_chars);
        let end = start + text.encode_utf16().count();
        out.push(Stmt { text, start, end });
    };
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\'' | '"' | '`' => {
                i += 1;
                while i < chars.len() {
                    if chars[i] == '\\' && c != '`' {
                        i += 2;
                        continue;
                    }
                    if chars[i] == c {
                        if chars.get(i + 1) == Some(&c) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            '-' if chars.get(i + 1) == Some(&'-')
                && chars.get(i + 2).is_none_or(|c| c.is_whitespace()) =>
            {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '#' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i += 2;
            }
            ';' => {
                push(&mut out, begin, i);
                i += 1;
                begin = i;
            }
            _ => i += 1,
        }
    }
    push(&mut out, begin, chars.len().min(i));
    out
}

/// The leading words of a statement, upper-cased, comments skipped.
fn words(sql: &str, n: usize) -> Vec<String> {
    let mut s = sql.trim_start();
    loop {
        if let Some(r) = s.strip_prefix("/*") {
            s = r.split_once("*/").map_or("", |(_, b)| b).trim_start();
        } else if s.starts_with("--") || s.starts_with('#') {
            s = s.split_once('\n').map_or("", |(_, b)| b).trim_start();
        } else {
            break;
        }
    }
    s.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .take(n)
        .map(str::to_ascii_uppercase)
        .collect()
}

/// A statement's class (what `studio.confirm_writes` and the checker use).
#[must_use]
pub fn classify(sql: &str) -> &'static str {
    let w = words(sql, 3);
    let w0 = w.first().map_or("", String::as_str);
    let w1 = w.get(1).map_or("", String::as_str);
    match w0 {
        "SELECT" | "WITH" | "SHOW" | "DESC" | "DESCRIBE" | "EXPLAIN" | "TABLE" | "VALUES"
        | "HELP" => "read",
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "LOAD" | "CALL" | "DO" => "write",
        "GRANT" | "REVOKE" => "account",
        "CREATE" | "ALTER" | "DROP" | "RENAME" if matches!(w1, "USER" | "ROLE") => "account",
        "SET" if matches!(w1, "PASSWORD" | "ROLE" | "DEFAULT") => "account",
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" | "OPTIMIZE" | "ANALYZE" => "ddl",
        "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "XA" => "transaction",
        _ => "other",
    }
}

/// Whether `EXPLAIN` can bind the statement.
fn bindable(sql: &str, class: &str) -> bool {
    let w0 = words(sql, 1).into_iter().next().unwrap_or_default();
    match class {
        "read" => matches!(w0.as_str(), "SELECT" | "WITH" | "TABLE" | "VALUES"),
        "write" => matches!(w0.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE"),
        _ => false,
    }
}

/// A sqlparser-rs syntax error, with the 1-based line and column it names
/// (an error at the end of the text names none: `usize::MAX`, the end).
#[must_use]
pub fn parse_error(sql: &str) -> Option<(String, usize, usize)> {
    let dialect = sqlparser::dialect::MySqlDialect {};
    let e = sqlparser::parser::Parser::parse_sql(&dialect, sql).err()?;
    let msg = e.to_string();
    let pos = |key: &str| {
        msg.rsplit_once(key)
            .and_then(|(_, r)| r.split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|n| n.parse::<usize>().ok())
    };
    let (line, col) = match (pos("Line: "), pos("Column: ")) {
        (Some(l), Some(c)) => (l, c),
        _ => (usize::MAX, 1),
    };
    let text = msg
        .trim_start_matches("sql parser error: ")
        .split(" at Line: ")
        .next()
        .unwrap_or(&msg)
        .to_owned();
    Some((text, line, col))
}

/// The UTF-16 offset of 1-based `line`, `col` inside `stmt`, from the start
/// of the whole text.
fn offset_of(stmt: &Stmt, line: usize, col: usize) -> usize {
    if line == usize::MAX {
        return stmt.end.saturating_sub(1).max(stmt.start);
    }
    let mut off = stmt.start;
    for (i, l) in stmt.text.split('\n').enumerate() {
        if i + 1 == line {
            let c: usize = l
                .chars()
                .take(col.saturating_sub(1))
                .map(char::len_utf16)
                .sum();
            return (off + c).min(stmt.end);
        }
        off += l.encode_utf16().count() + 1;
    }
    stmt.start
}

fn error_fields(e: &SprigError) -> (u16, String) {
    let p = e.protocol();
    let name = match e.kind {
        sprig_common::ErrorKind::Sprig(c) => c.name().to_owned(),
        _ => String::new(),
    };
    (p.mysql_errno, name)
}

/// `Sql.Check`.
#[allow(clippy::similar_names)] // start, end and stmt are the chapter's words
pub(super) async fn check(
    core: &Arc<Core>,
    caller: &Caller,
    database: &str,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let body = parse_json(&read_body(req).await?)?;
    let sql = text(&body, "sql");
    if sql.len() > MAX_CHECK_BYTES {
        return Err(super::invalid("the text is too long to check"));
    }
    let mut out = Vec::new();
    for s in split(&sql).into_iter().take(MAX_CHECK_STATEMENTS) {
        let class = classify(&s.text);
        let mut diags: Vec<Value> = Vec::new();
        let parsed = parse_error(&s.text);
        let mut bound_ok = None;
        if bindable(&s.text, class) {
            match run_sql(core, caller, database, format!("EXPLAIN {}", s.text)).await {
                Ok(_) => bound_ok = Some(true),
                Err(e) => {
                    bound_ok = Some(false);
                    let (code, name) = error_fields(&e);
                    let (start, end) = match (&parsed, code) {
                        (Some((_, l, c)), 1064) => (offset_of(&s, *l, *c), s.end),
                        _ => (s.start, s.end),
                    };
                    // Unavailability isn't the statement's fault: a warning.
                    let status = e.protocol().http_status;
                    diags.push(json!({
                        "severity": if matches!(status, 503 | 504) { "warning" } else { "error" },
                        "source": "server",
                        "code": code, "name": name, "message": e.message,
                        "start": start, "end": end,
                    }));
                }
            }
        }
        if let Some((msg, l, c)) = &parsed
            && bound_ok.is_none()
        {
            // Only the parser could judge it: a warning, never an error.
            diags.push(json!({
                "severity": "warning",
                "source": "parser",
                "code": 1064, "name": "", "message": msg,
                "start": offset_of(&s, *l, *c), "end": s.end,
            }));
        }
        out.push(json!({
            "start": s.start, "end": s.end, "class": class,
            "checked": if bound_ok.is_some() { "bound" } else { "parsed" },
            "diagnostics": diags,
        }));
    }
    Ok(json_response(200, &json!({ "statements": out })))
}

// ---- Catalog ----

/// The `ETag` of the caller's tenant's catalog now: `None` when the
/// gateway has no snapshot of it (no caching then).
fn etag(core: &Core, caller: &Caller, what: &str) -> Option<String> {
    let svc = core.studio.tenants.as_ref()?;
    let cat = svc.cache.cached(caller.principal.tenant?)?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(
        &(what, &caller.principal.user, &caller.principal.host),
        &mut h,
    );
    Some(format!(
        "\"c{}.a{}.{:x}\"",
        cat.version,
        cat.auth_version,
        std::hash::Hasher::finish(&h)
    ))
}

fn not_modified(tag: &str) -> Response<Body> {
    let mut r = Response::new(Body::new(http_body_util::Full::new(Bytes::new())));
    *r.status_mut() = StatusCode::NOT_MODIFIED;
    if let Ok(v) = HeaderValue::from_str(tag) {
        r.headers_mut().insert(header::ETAG, v);
    }
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    r
}

fn with_etag(mut r: Response<Body>, tag: Option<&str>) -> Response<Body> {
    if let Some(v) = tag.and_then(|t| HeaderValue::from_str(t).ok()) {
        r.headers_mut().insert(header::ETAG, v);
        r.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    }
    r
}

/// Rows of the first result as objects.
async fn objects(
    core: &Arc<Core>,
    caller: &Caller,
    sql: String,
) -> Result<Vec<Map<String, Value>>, SprigError> {
    let r = run_sql(core, caller, "-", sql).await?;
    let cols: Vec<String> = r
        .results
        .iter()
        .find(|x| !x.columns.is_empty())
        .map(|x| x.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    Ok(rows_of(&r)
        .into_iter()
        .map(|row| {
            cols.iter()
                .cloned()
                .zip(
                    row.into_iter()
                        .map(|v| v.map_or(Value::Null, Value::String)),
                )
                .collect()
        })
        .collect())
}

fn s(m: &Map<String, Value>, k: &str) -> String {
    m.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

/// `Catalog.ListObjects` for databases.
pub(super) async fn databases(
    core: &Arc<Core>,
    caller: &Caller,
    if_none_match: Option<&str>,
) -> Result<Response<Body>, SprigError> {
    let tag = etag(core, caller, "databases");
    if let (Some(t), Some(inm)) = (&tag, if_none_match)
        && inm == t
    {
        return Ok(not_modified(t));
    }
    let rows = objects(
        core,
        caller,
        format!(
            "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA \
             WHERE SCHEMA_NAME NOT IN {SYSTEM} ORDER BY SCHEMA_NAME"
        ),
    )
    .await?;
    let names: Vec<String> = rows.iter().map(|r| s(r, "name")).collect();
    Ok(with_etag(
        json_response(200, &json!({ "databases": names })),
        tag.as_deref(),
    ))
}

async fn columns_of(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    table: Option<&str>,
) -> Result<Map<String, Value>, SprigError> {
    let filter = table.map_or(String::new(), |t| {
        format!(" AND TABLE_NAME = {}", quote_literal(t))
    });
    let rows = objects(
        core,
        caller,
        format!(
            "SELECT TABLE_NAME AS t, COLUMN_NAME AS c, COLUMN_TYPE AS type, IS_NULLABLE AS n, \
             COLUMN_KEY AS k, COLUMN_DEFAULT AS d, EXTRA AS x, ORDINAL_POSITION AS p \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {}{filter} \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
            quote_literal(db)
        ),
    )
    .await?;
    let mut out: Map<String, Value> = Map::new();
    for r in rows {
        let extra = s(&r, "x");
        let up = extra.to_ascii_uppercase();
        let col = json!({
            "name": s(&r, "c"),
            "type": s(&r, "type"),
            "nullable": s(&r, "n") == "YES",
            "key": s(&r, "k"),
            "default": r.get("d").cloned().unwrap_or(Value::Null),
            "extra": extra,
            "generated": up.contains("GENERATED") || up.contains("VIRTUAL") || up.contains("STORED"),
            "position": s(&r, "p").parse::<u64>().unwrap_or(0),
        });
        if let Value::Array(a) = out
            .entry(s(&r, "t"))
            .or_insert_with(|| Value::Array(Vec::new()))
        {
            a.push(col);
        }
    }
    Ok(out)
}

/// `Catalog.Snapshot` of one database.
pub(super) async fn snapshot(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    if_none_match: Option<&str>,
) -> Result<Response<Body>, SprigError> {
    let tag = etag(core, caller, &format!("db:{db}"));
    if let (Some(t), Some(inm)) = (&tag, if_none_match)
        && inm == t
    {
        return Ok(not_modified(t));
    }
    let d = quote_literal(db);
    let tables = objects(
        core,
        caller,
        format!(
            "SELECT TABLE_NAME AS name, TABLE_TYPE AS type, ENGINE AS engine, TABLE_ROWS AS est, \
             TABLE_COMMENT AS comment FROM information_schema.TABLES WHERE TABLE_SCHEMA = {d} \
             ORDER BY TABLE_NAME"
        ),
    )
    .await?;
    let columns = columns_of(core, caller, db, None).await?;
    let routines = objects(
        core,
        caller,
        format!(
            "SELECT ROUTINE_NAME AS name, ROUTINE_TYPE AS type FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = {d} ORDER BY ROUTINE_NAME"
        ),
    )
    .await
    .unwrap_or_default();
    let mut objs: Vec<Value> = tables
        .iter()
        .map(|t| {
            json!({
                "name": s(t, "name"),
                "kind": if s(t, "type").to_ascii_uppercase().contains("VIEW") { "view" } else { "table" },
                "rows": t.get("est").and_then(Value::as_str).and_then(|v| v.parse::<u64>().ok()),
                "engine": t.get("engine").cloned().unwrap_or(Value::Null),
                "comment": s(t, "comment"),
            })
        })
        .collect();
    objs.extend(routines.iter().map(|r| {
        json!({
            "name": s(r, "name"),
            "kind": if s(r, "type").eq_ignore_ascii_case("FUNCTION") { "function" } else { "procedure" },
            "rows": null, "engine": null, "comment": "",
        })
    }));
    Ok(with_etag(
        json_response(
            200,
            &json!({ "db": db, "objects": objs, "columns": columns }),
        ),
        tag.as_deref(),
    ))
}

fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// `Catalog.GetObject`: columns, indexes and the definition of one object.
pub(super) async fn object(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
) -> Result<Response<Body>, SprigError> {
    let what = match kind {
        "table" => "TABLE",
        "view" => "VIEW",
        "procedure" => "PROCEDURE",
        "function" => "FUNCTION",
        _ => return Err(super::invalid("kind is table, view, procedure or function")),
    };
    let q = format!("{}.{}", quote_ident(db), quote_ident(name));
    let columns = if matches!(kind, "table" | "view") {
        columns_of(core, caller, db, Some(name))
            .await?
            .remove(name)
            .unwrap_or(Value::Array(Vec::new()))
    } else {
        Value::Array(Vec::new())
    };
    let indexes = if kind == "table" {
        objects(core, caller, format!("SHOW INDEX FROM {q}"))
            .await
            .map(|v| v.into_iter().map(Value::Object).collect::<Vec<_>>())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let definition = match run_sql(core, caller, "-", format!("SHOW CREATE {what} {q}")).await {
        Ok(r) => {
            let r0 = rows_of(&r).into_iter().next().unwrap_or_default();
            // The definition is the column whose name starts with "Create".
            let cols: Vec<String> = r
                .results
                .iter()
                .find(|x| !x.columns.is_empty())
                .map(|x| x.columns.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();
            cols.iter()
                .position(|c| c.starts_with("Create "))
                .and_then(|i| r0.get(i).cloned().flatten())
                .map_or(Value::Null, Value::String)
        }
        Err(e) => json!({ "error": { "code": e.protocol().mysql_errno, "message": e.message } }),
    };
    Ok(json_response(
        200,
        &json!({
            "db": db, "kind": kind, "name": name,
            "columns": columns, "indexes": indexes, "definition": definition,
        }),
    ))
}

/// Routes `/v1/catalog` and `/v1/databases/{db}/catalog...`,
/// `/v1/databases/{db}/sql:check`; `None` for other paths.
#[allow(clippy::items_after_statements)] // the route enum next to its match
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Result<Response<Body>, SprigError>, Request<Incoming>> {
    let path = req.uri().path().to_owned();
    let method = req.method().as_str().to_owned();
    let seg: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(percent_decode)
        .collect();
    let seg: Vec<&str> = seg.iter().map(String::as_str).collect();
    enum Op<'a> {
        Databases,
        Snapshot(&'a str),
        Object(&'a str, &'a str, &'a str),
        Check(&'a str),
    }
    let op = match (method.as_str(), seg.as_slice()) {
        ("GET", ["v1", "catalog"]) => Op::Databases,
        ("GET", ["v1", "databases", db, "catalog"]) => Op::Snapshot(db),
        ("GET", ["v1", "databases", db, "catalog", kind, name]) => Op::Object(db, kind, name),
        ("POST", ["v1", "databases", db, "sql:check"]) => Op::Check(db),
        _ => return Err(req),
    };
    let r = async {
        let auth = hdr(&req, "authorization");
        let host = hdr(&req, "host");
        let inm = hdr(&req, "if-none-match");
        let caller = core.caller(info, auth, None, host).await?;
        match op {
            Op::Databases => databases(core, &caller, inm.as_deref()).await,
            Op::Snapshot(db) => snapshot(core, &caller, db, inm.as_deref()).await,
            Op::Object(db, kind, name) => object(core, &caller, db, kind, name).await,
            Op::Check(db) => check(core, &caller, db, req).await,
        }
    }
    .await;
    Ok(r)
}

pub(super) fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && let Some(h) = s.get(i + 1..i + 3)
            && let Ok(n) = u8::from_str_radix(h, 16)
        {
            out.push(n);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_outside_quotes_and_comments() {
        let sql = "SELECT 'a;b';\n-- c;d\nSELECT `x;y` FROM t; /* e;f */ SELECT 1";
        let st = split(sql);
        assert_eq!(st.len(), 3, "{st:?}");
        assert_eq!(st[0].text, "SELECT 'a;b'");
        assert_eq!(st[1].text, "-- c;d\nSELECT `x;y` FROM t");
        assert_eq!(st[2].text, "/* e;f */ SELECT 1");
        // UTF-16 offsets, as the editor counts.
        let st = split("SELECT '😀'; SELECT 2");
        assert_eq!(st[0].end, "SELECT '😀'".encode_utf16().count());
        assert_eq!(st[1].start, "SELECT '😀'; ".encode_utf16().count());
    }

    #[test]
    fn classes() {
        assert_eq!(classify("select 1"), "read");
        assert_eq!(
            classify("/* x */ WITH a AS (SELECT 1) SELECT * FROM a"),
            "read"
        );
        assert_eq!(classify("update t set a = 1"), "write");
        assert_eq!(classify("CREATE TABLE t (a int)"), "ddl");
        assert_eq!(classify("CREATE USER u"), "account");
        assert_eq!(classify("GRANT SELECT ON *.* TO u"), "account");
        assert_eq!(classify("start transaction"), "transaction");
        assert_eq!(classify("SET @a = 1"), "other");
    }

    #[test]
    fn parse_errors_have_positions() {
        let (msg, line, col) = parse_error("SELECT *\nFROM t WHERE WHERE b = 1").unwrap();
        assert_eq!(line, 2, "{msg}");
        assert!(col > 1, "{msg} {col}");
        // At the end of the text: no position, the statement's end.
        let (_, line, _) = parse_error("SELECT * FROM t WHERE").unwrap();
        assert_eq!(line, usize::MAX);
        assert!(parse_error("SELECT a FROM t WHERE b = 1").is_none());
        let s = Stmt {
            text: "SELECT *\nFROM t WHERE".into(),
            start: 10,
            end: 31,
        };
        assert_eq!(offset_of(&s, 2, 3), 10 + 9 + 2);
    }
}
