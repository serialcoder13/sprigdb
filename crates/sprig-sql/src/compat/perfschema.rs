//! `performance_schema` and `sys` on port 3306.
//!
//! A tenant's backend accounts hold no rights on MariaDB's own
//! `performance_schema` (which the dev clusters run with
//! `performance_schema=OFF` anyway) or `sys`, and MariaDB's tables would
//! show every tenant's threads. MySQL tools read them all the same:
//! Workbench's dashboard and session list, `DBeaver`'s session manager,
//! monitoring agents (`global_status`, `events_statements_summary_by_digest`),
//! and drivers' suites (`session_variables`, `session_status`).
//!
//! A reference `performance_schema.<t>` or `sys.<v>` in a statement becomes
//! a derived table with MySQL 8.4's columns, in this order:
//!
//! - **From the session and MariaDB's `information_schema`:**
//!   `session_variables`, `global_variables` and `variables_by_thread`
//!   (MariaDB's `SESSION_VARIABLES` / `GLOBAL_VARIABLES`, names in lower case,
//!   the persona's own values written in: version strings, `sql_mode`,
//!   `max_execution_time`, collations in MySQL's names, `performance_schema =
//!   ON`, ...); `session_status`, `global_status` and `status_by_thread`
//!   (MariaDB's `SESSION_STATUS`, names in MySQL's case, the gateway's
//!   per-session counters and `Uptime` written in, `Threads_connected` the
//!   tenant's). `global_status` is built the same way: the server's totals
//!   (`GLOBAL_STATUS`, which the name layer denies) count every tenant's
//!   statements, so a tenant sees the server's global-only values and its
//!   session's counters; `variables_info` (`SYSTEM_VARIABLES`' limits).
//! - **From the tenant's sessions** (the `PROCESSLIST` view, which the
//!   name layer fills from the gateway's session table): `threads`,
//!   `processlist`, `sys.session`, `sys.processlist` and their `x$` forms.
//!   The current session's connection attributes come from the gateway:
//!   `session_connect_attrs` and `session_account_connect_attrs`.
//! - **From the tenant's catalog views:** `sys.schema_object_overview` and
//!   `sys.schema_auto_increment_columns`.
//! - **Constants:** `sys.version`, `sys.memory_global_total`, `sys.metrics`'s
//!   clock rows.
//! - **Every other table and view MySQL 8.4 has is empty**, with its
//!   columns (the generated [`catalog`]): MariaDB has no data for them
//!   (waits, stages, memory, locks, replication, digests). A name MySQL
//!   doesn't have fails with 1146, as on MySQL.
//!
//! Functions: `FORMAT_BYTES()`, `FORMAT_PICO_TIME()`,
//! `PS_CURRENT_THREAD_ID()` and `PS_THREAD_ID()` (MySQL 8.0's native ones),
//! and `sys.format_bytes()`, `sys.format_time()`, `sys.format_statement()`,
//! `sys.quote_identifier()`, `sys.version_major()` / `_minor()` /
//! `_patch()`, `sys.ps_thread_id()` and `sys.sys_get_config()` become
//! expressions MariaDB evaluates. A call with a `?` argument is left alone,
//! since the argument appears several times.
//!
//! Thread ids are the gateway's connection ids (`THREAD_ID` equals
//! `PROCESSLIST_ID`), so `WHERE PROCESSLIST_ID = CONNECTION_ID()` and
//! `PS_CURRENT_THREAD_ID()` agree. Unlike MySQL, every tenant user may read
//! them: what they show is the tenant's own (`PROCESSLIST` already applies
//! the `PROCESS` privilege). Unqualified names in `FROM` and `JOIN` resolve
//! here when the session's current database is `performance_schema` or
//! `sys`.

mod catalog;
mod digests;

pub use digests::{PsDigest, is_truncate_digests};

use std::fmt::Write as _;

use super::errmap::WireError;
use super::vars::PersonaVars;
use crate::lex::{self, LexOptions, Token, TokenKind};
use crate::names::rewrite::sql_string;

/// `sys.version`'s `sys_version` in MySQL 8.4.
pub const SYS_VERSION: &str = "2.1.3";

/// MySQL-only variables 3306 adds to the variable tables, and the
/// persona's values of shared ones, by name.
const PERSONA_VARIABLES: &[&str] = &[
    "version",
    "version_comment",
    "gtid_mode",
    "enforce_gtid_consistency",
    "terminology_use_previous",
    "sql_mode",
    "max_execution_time",
    "max_allowed_packet",
    "max_connections",
    "performance_schema",
];

/// What the session contributes to the views.
#[derive(Debug, Clone, Copy)]
pub struct PsSession<'a> {
    /// The persona's variables (versions, `sql_mode`, ...).
    pub vars: &'a PersonaVars,
    /// Gateway-owned session variables and their values (`wait_timeout`).
    pub owned_vars: &'a [(&'a str, String)],
    /// Session status rows the gateway answers (`Com_select`, `Ssl_cipher`).
    pub status: &'a [(&'a str, String)],
    /// The gateway's uptime in seconds.
    pub uptime: u64,
    /// The session's connection id (its thread id).
    pub connection_id: u64,
    /// The session's connection attributes, in the order sent.
    pub connect_attrs: &'a [(String, String)],
    /// The session's current database, as the client named it.
    pub current_db: Option<&'a str>,
    /// The tenant's statement digests the user may see, when the statement
    /// reads them ([`wants_digests`]).
    pub digests: &'a [PsDigest],
}

/// Whether `sql` reads a table computed from statement digests, so the
/// gateway gathers [`PsSession::digests`].
#[must_use]
pub fn wants_digests(sql: &[u8]) -> bool {
    let has = |w: &[u8]| sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    has(b"by_digest") || has(b"statement_analysis") || has(b"statements_with_errors")
}

/// Which system schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Schema {
    Ps,
    Sys,
}

impl Schema {
    fn parse(name: &[u8]) -> Option<Self> {
        if name.eq_ignore_ascii_case(b"performance_schema") {
            Some(Self::Ps)
        } else if name.eq_ignore_ascii_case(b"sys") {
            Some(Self::Sys)
        } else {
            None
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Ps => "performance_schema",
            Self::Sys => "sys",
        }
    }
}

/// Whether `sql` may need [`rewrite`]: a cheap test on the bytes.
#[must_use]
pub fn wanted(sql: &[u8], current_db: Option<&str>) -> bool {
    if current_db.is_some_and(|d| Schema::parse(d.as_bytes()).is_some()) {
        return true;
    }
    let has = |w: &[u8]| sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    has(b"performance_schema")
        || has(b"sys.")
        || has(b"sys`.")
        || has(b"format_bytes")
        || has(b"format_pico_time")
        || has(b"thread_id")
        || (has(b"show") && has(b"tables") && has(b"sys"))
}

/// The statement with every `performance_schema` and `sys` reference as a
/// derived table and the functions as expressions; `None` when nothing
/// changes.
///
/// # Errors
/// 1146 for a table or view MySQL 8.4 doesn't have.
#[must_use]
#[allow(clippy::too_many_lines)] // one pass over the tokens
pub fn rewrite(
    sql: &[u8],
    opts: &LexOptions,
    s: &PsSession<'_>,
) -> Option<Result<Vec<u8>, WireError>> {
    if !wanted(sql, s.current_db) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let nbe = opts.no_backslash_escapes;
    let current = s.current_db.and_then(|d| Schema::parse(d.as_bytes()));
    if let Some(q) = show_tables(sql, &t, current, nbe) {
        return Some(Ok(q.into_bytes()));
    }
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut j = 0;
    while j < t.len() {
        let ident = |k: usize| {
            t.get(k)
                .filter(|x| matches!(x.kind, TokenKind::Word | TokenKind::QuotedIdent))
                .map(|x| lex::ident_value(x.bytes(sql)).into_owned())
        };
        let punct = |k: usize, c: u8| {
            t.get(k)
                .is_some_and(|x| x.kind == TokenKind::Punct && sql[x.start as usize] == c)
        };
        let after_dot = j > 0 && punct(j - 1, b'.');
        // `schema.object`, `schema.function(...)`, `schema.table.column`.
        if !after_dot
            && let Some(schema) = ident(j).and_then(|n| Schema::parse(&n))
            && punct(j + 1, b'.')
            && let Some(obj) = ident(j + 2)
        {
            let obj = String::from_utf8_lossy(&obj).into_owned();
            if punct(j + 3, b'(') {
                if let Some((end, expr)) = call(sql, &t, j + 3, schema, &obj, *opts, s) {
                    let expr = named(sql, &t, j, end, expr);
                    edits.push((t[j].start as usize, t[end].end as usize, expr));
                    j = end + 1;
                    continue;
                }
            } else if punct(j + 3, b'.') {
                // A column qualified with the table: the table's alias.
                edits.push((
                    t[j].start as usize,
                    t[j + 2].end as usize,
                    format!("`{}`", obj.replace('`', "``")),
                ));
                j += 3;
                continue;
            } else {
                match object_sql(schema, &obj, s, nbe) {
                    Some(view) => {
                        let mut rep = format!("({view})");
                        if !has_alias(sql, &t, j + 3) {
                            let _ = write!(rep, " AS `{}`", obj.replace('`', "``"));
                        }
                        edits.push((t[j].start as usize, t[j + 2].end as usize, rep));
                        j += 3;
                        continue;
                    }
                    None => return Some(Err(no_such_table(schema, &obj))),
                }
            }
        }
        // MySQL's native functions.
        if !after_dot
            && t[j].kind == TokenKind::Word
            && punct(j + 1, b'(')
            && let Some((end, expr)) = native_call(sql, &t, j, *opts, s)
        {
            let expr = named(sql, &t, j, end, expr);
            edits.push((t[j].start as usize, t[end].end as usize, expr));
            j = end + 1;
            continue;
        }
        // The current database is one of them: unqualified tables.
        if let Some(schema) = current
            && !after_dot
            && j > 0
            && table_position(sql, &t, j)
            && !punct(j + 1, b'.')
            && !punct(j + 1, b'(')
            && let Some(obj) = ident(j)
        {
            let obj = String::from_utf8_lossy(&obj).into_owned();
            match object_sql(schema, &obj, s, nbe) {
                Some(view) => {
                    let mut rep = format!("({view})");
                    if !has_alias(sql, &t, j + 1) {
                        let _ = write!(rep, " AS `{}`", obj.replace('`', "``"));
                    }
                    edits.push((t[j].start as usize, t[j].end as usize, rep));
                }
                None => return Some(Err(no_such_table(schema, &obj))),
            }
        }
        j += 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 256);
    let mut at = 0;
    for (start, end, rep) in edits {
        out.extend_from_slice(&sql[at..start]);
        out.extend_from_slice(rep.as_bytes());
        at = end;
    }
    out.extend_from_slice(&sql[at..]);
    Some(Ok(out))
}

/// `SHOW [FULL] TABLES [{FROM | IN} db] [LIKE 'p']` in `performance_schema`
/// or `sys` (named, or current after `USE`): the catalog's
/// names, as MySQL lists them (`sys`' objects are views but `sys_config`).
fn show_tables(sql: &[u8], t: &[Token], current: Option<Schema>, nbe: bool) -> Option<String> {
    let word = |k: usize, w: &[u8]| {
        t.get(k)
            .is_some_and(|x| x.kind == TokenKind::Word && x.bytes(sql).eq_ignore_ascii_case(w))
    };
    if !word(0, b"SHOW") {
        return None;
    }
    let mut j = 1;
    let full = word(j, b"FULL");
    if full {
        j += 1;
    }
    if !word(j, b"TABLES") {
        return None;
    }
    j += 1;
    let mut schema = current;
    if word(j, b"FROM") || word(j, b"IN") {
        let name = t
            .get(j + 1)
            .filter(|x| x.kind.is_ident_like())
            .map(|x| lex::ident_value(x.bytes(sql)).into_owned())?;
        schema = Some(Schema::parse(&name)?);
        j += 2;
    }
    let schema = schema?;
    let mut like = None;
    if word(j, b"LIKE") {
        let p = t.get(j + 1).filter(|x| x.kind == TokenKind::String)?;
        like = Some(String::from_utf8_lossy(p.bytes(sql)).into_owned());
        j += 2;
    }
    if j < t.len() && !(j + 1 == t.len() && t[j].bytes(sql) == b";") {
        return None;
    }
    let col = format!("Tables_in_{}", schema.name());
    let rows: Vec<String> = catalog::OBJECTS
        .iter()
        .filter(|(sc, _, _)| *sc == schema.name())
        .map(|(_, name, _)| {
            let kind = if schema == Schema::Sys && *name != "sys_config" {
                "VIEW"
            } else {
                "BASE TABLE"
            };
            if full {
                format!(
                    "SELECT {} AS `{col}`, '{kind}' AS `Table_type`",
                    sql_string(name, nbe)
                )
            } else {
                format!("SELECT {} AS `{col}`", sql_string(name, nbe))
            }
        })
        .collect();
    // MySQL names the column with the pattern: `Tables_in_sys (x$%)`.
    let shown = match &like {
        Some(p) => {
            let v = lex::string_value(p.as_bytes(), nbe).unwrap_or_default();
            format!("{col} ({})", String::from_utf8_lossy(&v))
        }
        None => col.clone(),
    };
    let filter = like.map_or_else(String::new, |p| format!(" WHERE `{col}` LIKE {p}"));
    Some(format!(
        "SELECT `{col}` AS `{}`{} FROM ({}) AS t{filter} ORDER BY 1",
        shown.replace('`', "``"),
        if full { ", `Table_type`" } else { "" },
        rows.join(" UNION ALL ")
    ))
}

/// `expr` for the call `t[from..=to]`, with the call's text as its column
/// name when the call is a whole select item without an alias, as MySQL
/// names it.
fn named(sql: &[u8], t: &[Token], from: usize, to: usize, expr: String) -> String {
    if !whole_select_item(sql, t, from, to) {
        return expr;
    }
    let written = String::from_utf8_lossy(&sql[t[from].start as usize..t[to].end as usize]);
    format!("{expr} AS `{}`", written.replace('`', "``"))
}

/// Whether `t[from..=to]` is a whole select item without an alias (also
/// used by [`super::jsonops`]).
pub(crate) fn whole_select_item(sql: &[u8], t: &[Token], from: usize, to: usize) -> bool {
    let word = |k: usize, ws: &[&[u8]]| {
        t.get(k).is_some_and(|x| {
            x.kind == TokenKind::Word && ws.iter().any(|w| x.bytes(sql).eq_ignore_ascii_case(w))
        })
    };
    let punct = |k: usize, c: u8| {
        t.get(k)
            .is_some_and(|x| x.kind == TokenKind::Punct && sql[x.start as usize] == c)
    };
    let ends = to + 1 == t.len()
        || punct(to + 1, b',')
        || punct(to + 1, b';')
        || punct(to + 1, b')')
        || word(
            to + 1,
            &[
                b"FROM", b"WHERE", b"GROUP", b"HAVING", b"ORDER", b"LIMIT", b"UNION", b"INTO",
                b"FOR", b"WINDOW",
            ],
        );
    if !ends || from == 0 {
        return false;
    }
    // After SELECT or DISTINCT, or a comma of the select list.
    if word(from - 1, &[b"SELECT", b"DISTINCT"]) {
        true
    } else if punct(from - 1, b',') {
        let mut depth = 0i32;
        let mut k = from - 1;
        let mut found = false;
        while k > 0 {
            k -= 1;
            if punct(k, b')') {
                depth += 1;
            } else if punct(k, b'(') {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            } else if depth == 0 && word(k, &[b"FROM", b"WHERE", b"ON", b"BY", b"SET"]) {
                break;
            } else if depth == 0 && word(k, &[b"SELECT"]) {
                found = true;
                break;
            }
        }
        found
    } else {
        false
    }
}

fn no_such_table(schema: Schema, obj: &str) -> WireError {
    WireError::new(
        1146,
        *b"42S02",
        format!("Table '{}.{obj}' doesn't exist", schema.name()),
    )
}

/// Words after a table name that are not its alias.
const NOT_ALIAS: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "WINDOW",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "STRAIGHT_JOIN",
    "FULL",
    "ON",
    "USING",
    "FOR",
    "LOCK",
    "INTO",
    "USE",
    "IGNORE",
    "FORCE",
    "PARTITION",
    "SET",
    "TABLESAMPLE",
];

fn has_alias(sql: &[u8], t: &[Token], k: usize) -> bool {
    match t.get(k) {
        Some(x) if x.kind == TokenKind::QuotedIdent => true,
        Some(x) if x.kind == TokenKind::Word => {
            let w = x.bytes(sql);
            w.eq_ignore_ascii_case(b"AS")
                || !NOT_ALIAS
                    .iter()
                    .any(|n| w.eq_ignore_ascii_case(n.as_bytes()))
        }
        _ => false,
    }
}

/// Whether `t[j]` stands where a table name goes: after `FROM`, `JOIN`
/// or `STRAIGHT_JOIN`, or after a comma in a `FROM` list.
fn table_position(sql: &[u8], t: &[Token], j: usize) -> bool {
    let word = |k: usize, w: &[u8]| {
        t[k].kind == TokenKind::Word && t[k].bytes(sql).eq_ignore_ascii_case(w)
    };
    let punct = |k: usize, c: u8| t[k].kind == TokenKind::Punct && sql[t[k].start as usize] == c;
    if word(j - 1, b"FROM") || word(j - 1, b"JOIN") || word(j - 1, b"STRAIGHT_JOIN") {
        return true;
    }
    if !punct(j - 1, b',') {
        return false;
    }
    // A comma in a FROM list: walk back at this depth to FROM, with no
    // other clause between (a join's `ON` condition may end before it).
    let mut depth = 0i32;
    let mut k = j - 1;
    while k > 0 {
        k -= 1;
        if punct(k, b')') {
            depth += 1;
        } else if punct(k, b'(') {
            if depth == 0 {
                return false;
            }
            depth -= 1;
        } else if depth == 0 && t[k].kind == TokenKind::Word {
            let w = t[k].bytes(sql);
            if w.eq_ignore_ascii_case(b"FROM") {
                return true;
            }
            if [
                &b"SELECT"[..],
                b"WHERE",
                b"GROUP",
                b"ORDER",
                b"HAVING",
                b"SET",
                b"VALUES",
            ]
            .iter()
            .any(|x| w.eq_ignore_ascii_case(x))
            {
                return false;
            }
        }
    }
    false
}

/// The matching `)` of the `(` at `t[open]`.
fn close_of(sql: &[u8], t: &[Token], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (k, x) in t.iter().enumerate().skip(open) {
        if x.kind == TokenKind::Punct {
            match sql[x.start as usize] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(k);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

/// The arguments' texts between `t[open]` and its `)`, rewritten in turn;
/// `None` when one holds a `?` (each argument is used more than once).
#[allow(clippy::many_single_char_names)]
fn args(
    sql: &[u8],
    t: &[Token],
    open: usize,
    opts: LexOptions,
    s: &PsSession<'_>,
) -> Option<(usize, Vec<String>)> {
    let close = close_of(sql, t, open)?;
    let mut out = Vec::new();
    if close == open + 1 {
        return Some((close, out));
    }
    let mut depth = 0i32;
    let mut from = open + 1;
    for k in open + 1..=close {
        let x = &t[k];
        if x.kind == TokenKind::Param {
            return None;
        }
        if x.kind == TokenKind::Punct {
            match sql[x.start as usize] {
                b'(' => depth += 1,
                b')' if depth > 0 => depth -= 1,
                b',' | b')' if depth == 0 => {
                    let text = &sql[t[from].start as usize..t[k - 1].end as usize];
                    let text = match rewrite(text, &opts, s) {
                        Some(Ok(v)) => v,
                        _ => text.to_vec(),
                    };
                    out.push(format!("({})", String::from_utf8_lossy(&text)));
                    from = k + 1;
                }
                _ => {}
            }
        }
    }
    Some((close, out))
}

/// `sys.<f>(...)` from `t[open]` (its `(`): the `)` and the expression.
#[allow(clippy::many_single_char_names)]
fn call(
    sql: &[u8],
    t: &[Token],
    open: usize,
    schema: Schema,
    name: &str,
    opts: LexOptions,
    s: &PsSession<'_>,
) -> Option<(usize, String)> {
    if schema != Schema::Sys {
        return None;
    }
    let (close, a) = args(sql, t, open, opts, s)?;
    let nbe = opts.no_backslash_escapes;
    let one = || (a.len() == 1).then(|| a[0].clone());
    let expr = match name.to_ascii_lowercase().as_str() {
        "format_bytes" => format_bytes(&one()?),
        "format_time" => sys_format_time(&one()?),
        "format_statement" => {
            let v = one()?;
            format!(
                "(CASE WHEN CHAR_LENGTH({v}) > 64 THEN CONCAT(LEFT({v}, 30), ' ... ', RIGHT({v}, 30)) ELSE {v} END)"
            )
        }
        "format_path" => one()?,
        "quote_identifier" => {
            let v = one()?;
            format!("CONCAT('`', REPLACE({v}, '`', '``'), '`')")
        }
        "version_major" | "version_minor" | "version_patch" if a.is_empty() => {
            let n = match name.to_ascii_lowercase().as_str() {
                "version_major" => 0,
                "version_minor" => 1,
                _ => 2,
            };
            let part = s
                .vars
                .version
                .split(['.', '-'])
                .nth(n)
                .and_then(|p| p.parse::<u32>().ok())
                .unwrap_or(0);
            format!("{part}")
        }
        "ps_thread_id" => {
            let v = one()?;
            format!("COALESCE({v}, {})", s.connection_id)
        }
        "ps_is_thread_instrumented" | "ps_is_account_enabled" | "ps_is_consumer_enabled" => {
            "'YES'".to_owned()
        }
        "sys_get_config" if a.len() == 2 => {
            // sys_config's defaults; anything else gives the default
            // argument, as an empty sys_config would.
            let (k, d) = (&a[0], &a[1]);
            let known = [
                ("statement_truncate_len", "64"),
                ("statement_performance_analyzer.limit", "100"),
                ("diagnostics.allow_i_s_tables", "OFF"),
                ("diagnostics.include_raw", "OFF"),
                ("ps_thread_trx_info.max_length", "65535"),
            ];
            let mut e = format!("(CASE {k}");
            for (n, v) in known {
                let _ = write!(
                    e,
                    " WHEN {} THEN {}",
                    sql_string(n, nbe),
                    sql_string(v, nbe)
                );
            }
            let _ = write!(e, " ELSE {d} END)");
            e
        }
        _ => return None,
    };
    Some((close, expr))
}

/// MySQL's native `FORMAT_BYTES`, `FORMAT_PICO_TIME`,
/// `PS_CURRENT_THREAD_ID` and `PS_THREAD_ID` at `t[j]`.
#[allow(clippy::many_single_char_names)]
fn native_call(
    sql: &[u8],
    t: &[Token],
    j: usize,
    opts: LexOptions,
    s: &PsSession<'_>,
) -> Option<(usize, String)> {
    let w = t[j].bytes(sql);
    let is = |n: &[u8]| w.eq_ignore_ascii_case(n);
    if !(is(b"FORMAT_BYTES")
        || is(b"FORMAT_PICO_TIME")
        || is(b"PS_CURRENT_THREAD_ID")
        || is(b"PS_THREAD_ID"))
    {
        return None;
    }
    let (close, a) = args(sql, t, j + 1, opts, s)?;
    let expr = if is(b"FORMAT_BYTES") && a.len() == 1 {
        format_bytes(&a[0])
    } else if is(b"FORMAT_PICO_TIME") && a.len() == 1 {
        format_pico_time(&a[0])
    } else if is(b"PS_CURRENT_THREAD_ID") && a.is_empty() {
        // CONNECTION_ID() rather than the number: the gateway answers it
        // per statement, and a stored body keeps the call.
        "CAST(CONNECTION_ID() AS UNSIGNED)".to_owned()
    } else if is(b"PS_THREAD_ID") && a.len() == 1 {
        // The thread id of a connection id is itself.
        format!("CAST({} AS UNSIGNED)", a[0])
    } else {
        return None;
    };
    Some((close, expr))
}

/// MySQL's `FORMAT_BYTES(v)`: `%4d bytes` below 1 KiB (truncated), else
/// two decimals and the largest binary unit that fits.
fn format_bytes(v: &str) -> String {
    let units = [
        ("1152921504606846976", "EiB"),
        ("1125899906842624", "PiB"),
        ("1099511627776", "TiB"),
        ("1073741824", "GiB"),
        ("1048576", "MiB"),
        ("1024", "KiB"),
    ];
    let mut e = format!("(CASE WHEN {v} IS NULL THEN NULL");
    for (d, u) in units {
        let _ = write!(
            e,
            " WHEN ABS({v}) >= {d} THEN CONCAT(CAST({v} / {d} AS DECIMAL(65,2)), ' {u}')"
        );
    }
    let n = format!("CAST(TRUNCATE({v}, 0) AS SIGNED)");
    let _ = write!(
        e,
        " ELSE CONCAT(LPAD({n}, GREATEST(4, CHAR_LENGTH({n})), ' '), ' bytes') END)"
    );
    e
}

/// MySQL's `FORMAT_PICO_TIME(v)`: `%3d ps` below 1 ns, else two decimals
/// and the largest unit up to days.
fn format_pico_time(v: &str) -> String {
    let units = [
        ("86400000000000000", "d"),
        ("3600000000000000", "h"),
        ("60000000000000", "min"),
        ("1000000000000", "s"),
        ("1000000000", "ms"),
        ("1000000", "us"),
        ("1000", "ns"),
    ];
    let mut e = format!("(CASE WHEN {v} IS NULL THEN NULL");
    for (d, u) in units {
        let _ = write!(
            e,
            " WHEN ABS({v}) >= {d} THEN CONCAT(CAST({v} / {d} AS DECIMAL(65,2)), ' {u}')"
        );
    }
    let n = format!("CAST(TRUNCATE({v}, 0) AS SIGNED)");
    let _ = write!(
        e,
        " ELSE CONCAT(LPAD({n}, GREATEST(3, CHAR_LENGTH({n})), ' '), ' ps') END)"
    );
    e
}

/// `sys.format_time(v)`: rounded to two places without trailing zeros, in
/// units up to weeks; below 1 ns the value as given.
fn sys_format_time(v: &str) -> String {
    let units = [
        ("604800000000000000", "w"),
        ("86400000000000000", "d"),
        ("3600000000000000", "h"),
        ("60000000000000", "m"),
        ("1000000000000", "s"),
        ("1000000000", "ms"),
        ("1000000", "us"),
        ("1000", "ns"),
    ];
    let f = format!("CAST({v} AS DOUBLE)");
    let mut e = format!("(CASE WHEN {v} IS NULL THEN NULL");
    for (d, u) in units {
        let _ = write!(
            e,
            " WHEN {f} >= {d} THEN CONCAT(TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM \
             CAST({f} / {d} AS DECIMAL(65,2)))), ' {u}')"
        );
    }
    let _ = write!(e, " ELSE CONCAT({v}, ' ps') END)");
    e
}

/// The derived table standing for `schema.obj`; `None` when MySQL 8.4 has
/// no such table or view.
#[allow(clippy::too_many_lines)] // one arm per computed object
fn object_sql(schema: Schema, obj: &str, s: &PsSession<'_>, nbe: bool) -> Option<String> {
    let lower = obj.to_ascii_lowercase();
    let cols = catalog::OBJECTS
        .iter()
        .find(|(sc, t, _)| *sc == schema.name() && *t == lower)
        .map(|(_, _, c)| *c)?;
    let lit = |v: &str| sql_string(v, nbe);
    if digests::is_digest_object(schema.name(), &lower) {
        return Some(
            digests::object(schema.name(), &lower, cols, s.digests, nbe)
                .unwrap_or_else(|| empty(cols)),
        );
    }
    let computed = match (schema, lower.as_str()) {
        (Schema::Ps, "session_variables") => Some(variables("SESSION_VARIABLES", false, s, nbe)),
        (Schema::Ps, "global_variables") => Some(variables("GLOBAL_VARIABLES", true, s, nbe)),
        (Schema::Ps, "variables_by_thread") => Some(format!(
            "SELECT CAST({} AS UNSIGNED) AS THREAD_ID, v.VARIABLE_NAME, v.VARIABLE_VALUE FROM ({}) AS v",
            s.connection_id,
            variables("SESSION_VARIABLES", false, s, nbe)
        )),
        (Schema::Ps, "session_status") => Some(status("SESSION_STATUS", false, s, nbe)),
        (Schema::Ps, "global_status") => Some(status("SESSION_STATUS", true, s, nbe)),
        (Schema::Ps, "status_by_thread") => Some(format!(
            "SELECT CAST({} AS UNSIGNED) AS THREAD_ID, v.VARIABLE_NAME, v.VARIABLE_VALUE FROM ({}) AS v",
            s.connection_id,
            status("SESSION_STATUS", false, s, nbe)
        )),
        (Schema::Ps, "variables_info") => Some(
            "SELECT LOWER(VARIABLE_NAME) AS VARIABLE_NAME, 'COMPILED' AS VARIABLE_SOURCE, \
             '' AS VARIABLE_PATH, CAST(COALESCE(NUMERIC_MIN_VALUE, 0) AS CHAR(64)) AS MIN_VALUE, \
             CAST(COALESCE(NUMERIC_MAX_VALUE, 0) AS CHAR(64)) AS MAX_VALUE, \
             CAST(NULL AS DATETIME(6)) AS SET_TIME, CAST(NULL AS CHAR(32)) AS SET_USER, \
             CAST(NULL AS CHAR(255)) AS SET_HOST FROM information_schema.SYSTEM_VARIABLES"
                .to_owned(),
        ),
        (Schema::Ps, "threads") => Some(
            "SELECT CAST(ID AS UNSIGNED) AS THREAD_ID, 'thread/sql/one_connection' AS NAME, \
             'FOREGROUND' AS TYPE, CAST(ID AS UNSIGNED) AS PROCESSLIST_ID, USER AS PROCESSLIST_USER, \
             SUBSTRING_INDEX(HOST, ':', 1) AS PROCESSLIST_HOST, DB AS PROCESSLIST_DB, \
             COMMAND AS PROCESSLIST_COMMAND, TIME AS PROCESSLIST_TIME, STATE AS PROCESSLIST_STATE, \
             INFO AS PROCESSLIST_INFO, CAST(NULL AS UNSIGNED) AS PARENT_THREAD_ID, \
             CAST(NULL AS CHAR(64)) AS ROLE, 'YES' AS INSTRUMENTED, 'YES' AS HISTORY, \
             'TCP/IP' AS CONNECTION_TYPE, CAST(NULL AS UNSIGNED) AS THREAD_OS_ID, \
             'USR_default' AS RESOURCE_GROUP, 'PRIMARY' AS EXECUTION_ENGINE, \
             CAST(0 AS UNSIGNED) AS CONTROLLED_MEMORY, CAST(0 AS UNSIGNED) AS MAX_CONTROLLED_MEMORY, \
             CAST(0 AS UNSIGNED) AS TOTAL_MEMORY, CAST(0 AS UNSIGNED) AS MAX_TOTAL_MEMORY, \
             'NO' AS TELEMETRY_ACTIVE FROM information_schema.PROCESSLIST"
                .to_owned(),
        ),
        (Schema::Ps, "processlist") => Some(
            "SELECT CAST(ID AS UNSIGNED) AS ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO, \
             'PRIMARY' AS EXECUTION_ENGINE FROM information_schema.PROCESSLIST"
                .to_owned(),
        ),
        (Schema::Ps, "session_connect_attrs" | "session_account_connect_attrs") => {
            if s.connect_attrs.is_empty() {
                None
            } else {
                let rows: Vec<String> = s
                    .connect_attrs
                    .iter()
                    .enumerate()
                    .map(|(i, (k, v))| {
                        format!(
                            "SELECT CAST({} AS UNSIGNED) AS PROCESSLIST_ID, {} AS ATTR_NAME, {} AS ATTR_VALUE, {i} AS ORDINAL_POSITION",
                            s.connection_id,
                            lit(k),
                            lit(v)
                        )
                    })
                    .collect();
                Some(rows.join(" UNION ALL "))
            }
        }
        (Schema::Sys, "version") => Some(format!(
            "SELECT {} AS sys_version, {} AS mysql_version",
            lit(SYS_VERSION),
            lit(s.vars.version.split('-').next().unwrap_or(&s.vars.version))
        )),
        (Schema::Sys, "session" | "processlist" | "x$session" | "x$processlist") => {
            Some(sys_session(lower.starts_with("x$"), s, nbe))
        }
        (Schema::Sys, "schema_object_overview") => Some(
            "SELECT ROUTINE_SCHEMA AS db, ROUTINE_TYPE AS object_type, COUNT(*) AS count \
             FROM information_schema.ROUTINES GROUP BY ROUTINE_SCHEMA, ROUTINE_TYPE \
             UNION SELECT TABLE_SCHEMA, TABLE_TYPE, COUNT(*) FROM information_schema.TABLES \
             GROUP BY TABLE_SCHEMA, TABLE_TYPE \
             UNION SELECT TABLE_SCHEMA, CONCAT('INDEX (', INDEX_TYPE, ')'), COUNT(*) \
             FROM information_schema.STATISTICS GROUP BY TABLE_SCHEMA, INDEX_TYPE \
             UNION SELECT TRIGGER_SCHEMA, 'TRIGGER', COUNT(*) FROM information_schema.TRIGGERS \
             GROUP BY TRIGGER_SCHEMA \
             UNION SELECT EVENT_SCHEMA, 'EVENT', COUNT(*) FROM information_schema.EVENTS \
             GROUP BY EVENT_SCHEMA"
                .to_owned(),
        ),
        (Schema::Sys, "schema_auto_increment_columns") => {
            let max = "(CASE c.DATA_TYPE WHEN 'tinyint' THEN 255 WHEN 'smallint' THEN 65535 \
                       WHEN 'mediumint' THEN 16777215 WHEN 'int' THEN 4294967295 \
                       WHEN 'bigint' THEN 18446744073709551615 END \
                       >> IF(LOCATE('unsigned', c.COLUMN_TYPE) > 0, 0, 1))";
            Some(format!(
                "SELECT c.TABLE_SCHEMA AS table_schema, c.TABLE_NAME AS table_name, \
                 c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type, c.COLUMN_TYPE AS column_type, \
                 (LOCATE('unsigned', c.COLUMN_TYPE) = 0) AS is_signed, \
                 (LOCATE('unsigned', c.COLUMN_TYPE) > 0) AS is_unsigned, {max} AS max_value, \
                 t.AUTO_INCREMENT AS auto_increment, \
                 CAST(t.AUTO_INCREMENT / {max} AS DECIMAL(25,4)) AS auto_increment_ratio \
                 FROM information_schema.COLUMNS AS c JOIN information_schema.TABLES AS t \
                 ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
                 WHERE c.DATA_TYPE IN ('tinyint', 'smallint', 'mediumint', 'int', 'bigint') \
                 AND c.COLUMN_KEY = 'PRI' AND c.EXTRA = 'auto_increment'"
            ))
        }
        (Schema::Sys, "memory_global_total") => {
            Some("SELECT '   0 bytes' AS total_allocated".to_owned())
        }
        (Schema::Sys, "metrics") => Some(format!(
            "SELECT LOWER(VARIABLE_NAME) AS Variable_name, VARIABLE_VALUE AS Variable_value, \
             'Global Status' AS Type, 'YES' AS Enabled FROM ({}) AS g \
             UNION ALL SELECT 'NOW()', NOW(3), 'System Time', 'YES' \
             UNION ALL SELECT 'UNIX_TIMESTAMP()', ROUND(UNIX_TIMESTAMP(NOW(3)), 3), 'System Time', 'YES'",
            status("SESSION_STATUS", true, s, nbe)
        )),
        _ => None,
    };
    Some(computed.unwrap_or_else(|| empty(cols)))
}

/// An empty table with these `NAME:CAST;...` columns.
fn empty(cols: &str) -> String {
    let items: Vec<String> = cols
        .split(';')
        .filter_map(|c| c.split_once(':'))
        .map(|(n, ty)| format!("CAST(NULL AS {ty}) AS `{n}`"))
        .collect();
    format!("SELECT {} LIMIT 0", items.join(", "))
}

/// A variable table over MariaDB's `information_schema.<source>`, with the
/// persona's values (and the session's gateway-owned ones) written in.
fn variables(source: &str, global: bool, s: &PsSession<'_>, nbe: bool) -> String {
    let lit = |v: &str| sql_string(v, nbe);
    let mut over: Vec<(String, String)> = Vec::new();
    for name in PERSONA_VARIABLES {
        if let Some(v) = s.vars.value_at(name, global) {
            over.push(((*name).to_owned(), v.into_owned()));
        }
    }
    if !global {
        for (n, v) in s.owned_vars {
            let n = n.to_ascii_lowercase();
            if !over.iter().any(|(o, _)| *o == n) {
                over.push((n, v.clone()));
            }
        }
    }
    // MariaDB's UCA 14.0 defaults by MySQL's names.
    let mut coll = String::from("CASE VARIABLE_VALUE");
    for (m, my) in super::collation::output_pairs() {
        if my.starts_with("utf8mb4_0900_") && m != "utf8mb4_bin" {
            let _ = write!(coll, " WHEN {} THEN {}", lit(&m), lit(my));
        }
    }
    coll.push_str(" ELSE VARIABLE_VALUE END");
    let names: Vec<String> = over.iter().map(|(n, _)| lit(n)).collect();
    let mut sql = format!(
        "SELECT LOWER(VARIABLE_NAME) AS VARIABLE_NAME, CASE WHEN LEFT(LOWER(VARIABLE_NAME), 10) = 'collation_' \
         THEN {coll} ELSE VARIABLE_VALUE END AS VARIABLE_VALUE FROM information_schema.{source}"
    );
    if !names.is_empty() {
        let _ = write!(
            sql,
            " WHERE LOWER(VARIABLE_NAME) NOT IN ({})",
            names.join(", ")
        );
    }
    for (n, v) in &over {
        let _ = write!(sql, " UNION ALL SELECT {}, {}", lit(n), lit(v));
    }
    sql
}

/// A status table over MariaDB's `information_schema.<source>`
/// (`SESSION_STATUS` for both scopes): names in MySQL's case, the
/// gateway's counters (session) or uptime (global) and the tenant's
/// thread count written in. MariaDB's `wsrep` rows and the
/// `Com_*` counters are left out, as MySQL's `performance_schema` leaves
/// out `Com_*`.
fn status(source: &str, global: bool, s: &PsSession<'_>, nbe: bool) -> String {
    let lit = |v: &str| sql_string(v, nbe);
    let up = s.uptime.to_string();
    let mut over: Vec<(String, String)> = if global {
        vec![
            ("Uptime".to_owned(), lit(&up)),
            ("Uptime_since_flush_status".to_owned(), lit(&up)),
        ]
    } else {
        s.status
            .iter()
            .filter(|(n, _)| !n.starts_with("Com_"))
            .map(|(n, v)| ((*n).to_owned(), lit(v)))
            .collect()
    };
    // The tenant's sessions, in both scopes (MySQL lists the global value
    // in session_status too).
    over.push((
        "Threads_connected".to_owned(),
        "(SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.PROCESSLIST)".to_owned(),
    ));
    over.dedup_by(|a, b| a.0.eq_ignore_ascii_case(&b.0));
    let names: Vec<String> = over
        .iter()
        .map(|(n, _)| lit(&n.to_ascii_uppercase()))
        .collect();
    let mut sql = format!(
        "SELECT CONCAT(UPPER(LEFT(VARIABLE_NAME, 1)), LOWER(SUBSTR(VARIABLE_NAME, 2))) AS VARIABLE_NAME, \
         VARIABLE_VALUE FROM information_schema.{source} WHERE UPPER(LEFT(VARIABLE_NAME, 5)) <> 'WSREP' \
         AND UPPER(LEFT(VARIABLE_NAME, 4)) <> 'COM_'"
    );
    if !names.is_empty() {
        let _ = write!(
            sql,
            " AND UPPER(VARIABLE_NAME) NOT IN ({})",
            names.join(", ")
        );
    }
    for (n, v) in &over {
        let _ = write!(sql, " UNION ALL SELECT {}, {v}", lit(n));
    }
    sql
}

/// `sys.session` and `sys.processlist` (`x`: the `x$` forms, raw
/// numbers) over the tenant's sessions.
fn sys_session(x: bool, s: &PsSession<'_>, nbe: bool) -> String {
    let lit = |v: &str| sql_string(v, nbe);
    let attr = |k: &str| {
        s.connect_attrs
            .iter()
            .find(|(n, _)| n == k)
            .map_or_else(|| "NULL".to_owned(), |(_, v)| lit(v))
    };
    let (lat, mem) = if x {
        ("CAST(NULL AS UNSIGNED)", "CAST(0 AS DECIMAL(41,0))")
    } else {
        ("CAST(NULL AS CHAR(11))", "'   0 bytes'")
    };
    format!(
        "SELECT CAST(ID AS UNSIGNED) AS thd_id, CAST(ID AS UNSIGNED) AS conn_id, \
         CONCAT(USER, '@', SUBSTRING_INDEX(HOST, ':', 1)) AS user, DB AS db, COMMAND AS command, \
         STATE AS state, TIME AS time, INFO AS current_statement, 'PRIMARY' AS execution_engine, \
         {lat} AS statement_latency, CAST(NULL AS DECIMAL(26,2)) AS progress, {lat} AS lock_latency, \
         {lat} AS cpu_latency, CAST(0 AS UNSIGNED) AS rows_examined, CAST(0 AS UNSIGNED) AS rows_sent, \
         CAST(0 AS UNSIGNED) AS rows_affected, CAST(0 AS UNSIGNED) AS tmp_tables, \
         CAST(0 AS UNSIGNED) AS tmp_disk_tables, 'NO' AS full_scan, CAST(NULL AS CHAR(65535)) AS last_statement, \
         {lat} AS last_statement_latency, {mem} AS current_memory, CAST(NULL AS CHAR(128)) AS last_wait, \
         CAST(NULL AS CHAR(20)) AS last_wait_latency, CAST(NULL AS CHAR(64)) AS source, {lat} AS trx_latency, \
         CAST(NULL AS CHAR(11)) AS trx_state, CAST(NULL AS CHAR(3)) AS trx_autocommit, \
         CASE ID WHEN {id} THEN {pid} END AS pid, CASE ID WHEN {id} THEN {prog} END AS program_name \
         FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Daemon'",
        id = s.connection_id,
        pid = attr("_pid"),
        prog = attr("program_name"),
    )
}

#[cfg(test)]
mod tests;
