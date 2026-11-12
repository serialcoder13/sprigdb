//! The 3306 rules inside stored programs and views (chapter 28 step 12,
//! R4 round 4), and 39602 `ER_SPRIG_NEEDS_GATEWAY`.
//!
//! MariaDB runs a view's query and a routine's, trigger's or event's body
//! itself, so the gateway's per-statement rules never see the statements
//! inside. The rules that are plain SQL are applied when the object is
//! created instead: each query in the body (a statement starting with
//! `SELECT` or `WITH`, a cursor's query, a parenthesized subquery, a
//! view's query) goes through [`super::lateral`] and [`super::rollup`], as
//! the same query sent on its own would. Lock names were already scoped
//! in stored code ([`super::locks`], R4 round 3), and
//! [`super::perfschema`]'s functions are expressions.
//!
//! What would need the gateway while the body runs fails at `CREATE` with
//! 39602, naming it:
//!
//! - a `LATERAL` the rewrite can't turn into SQL (chapter 28's gateway
//!   apply);
//! - a `performance_schema` or `sys` table (the gateway answers them from
//!   its sessions);
//! - a `CALL sprig.<procedure>` (queues and stream offsets, chapters 45
//!   and 50, which the gateway runs).
//!
//! Writes to collections (chapter 31's rule) need the catalog: the gateway
//! resolves [`write_targets`] and refuses a collection with
//! [`collection_write_error`] (R4 round 5).
//!
//! Not checked here: MySQL's `ONLY_FULL_GROUP_BY`, which MySQL itself applies only
//! when the body runs (MariaDB doesn't apply it, decision T1), and
//! `EXPLAIN` inside a body, which keeps MariaDB's columns.

use super::errmap::WireError;
use crate::lex::{self, LexOptions, Token, TokenKind};

/// What a `CREATE` or `ALTER` statement defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Object {
    /// `CREATE VIEW`, `ALTER VIEW`.
    View,
    /// `CREATE PROCEDURE`.
    Procedure,
    /// `CREATE FUNCTION` (not a loadable or `LANGUAGE javascript | kirun`
    /// one).
    Function,
    /// `CREATE TRIGGER`.
    Trigger,
    /// `CREATE EVENT`, `ALTER EVENT ... DO`.
    Event,
}

fn word(sql: &[u8], t: &[Token], k: usize, w: &[u8]) -> bool {
    t.get(k)
        .is_some_and(|x| x.kind == TokenKind::Word && x.bytes(sql).eq_ignore_ascii_case(w))
}

fn punct(sql: &[u8], t: &[Token], k: usize, c: u8) -> bool {
    t.get(k)
        .is_some_and(|x| x.kind == TokenKind::Punct && sql[x.start as usize] == c)
}

/// The object a statement creates or alters, when it holds stored code,
/// and the index of its kind's keyword.
fn object_at(sql: &[u8], t: &[Token]) -> Option<(Object, usize)> {
    if !(word(sql, t, 0, b"CREATE") || word(sql, t, 0, b"ALTER")) {
        return None;
    }
    // The kind comes before any parenthesis: OR REPLACE, ALGORITHM = x,
    // DEFINER = user, SQL SECURITY x, AGGREGATE, IF NOT EXISTS.
    for k in 1..t.len().min(16) {
        if punct(sql, t, k, b'(') {
            return None;
        }
        let kinds = [
            (&b"VIEW"[..], Object::View),
            (b"PROCEDURE", Object::Procedure),
            (b"FUNCTION", Object::Function),
            (b"TRIGGER", Object::Trigger),
            (b"EVENT", Object::Event),
        ];
        for (w, o) in kinds {
            if word(sql, t, k, w) {
                return Some((o, k));
            }
        }
        if [
            &b"TABLE"[..],
            b"INDEX",
            b"DATABASE",
            b"SCHEMA",
            b"USER",
            b"ROLE",
            b"SERVER",
        ]
        .iter()
        .any(|w| word(sql, t, k, w))
        {
            return None;
        }
    }
    None
}

/// What `sql` creates, when it holds stored code.
#[must_use]
pub fn object(sql: &[u8], opts: &LexOptions) -> Option<Object> {
    let t: Vec<Token> = lex::lex(sql, opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    object_at(sql, &t).map(|(o, _)| o)
}

/// Words a body statement can follow: where a query that is a whole
/// statement starts.
const STATEMENT_AFTER: &[&[u8]] = &[
    b"BEGIN", b"THEN", b"ELSE", b"DO", b"LOOP", b"REPEAT", b"ROW", b"RETURN",
];

/// The 3306 rules applied inside a view's query or a stored program's
/// body; `None` when `sql` defines no stored code or nothing changes.
///
/// # Errors
/// 39602 for what needs the gateway while the body runs.
#[must_use]
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Result<Vec<u8>, WireError>> {
    if opts.persona != sprig_proto_mysql::packets::Persona::MySql84 {
        return None;
    }
    let t: Vec<Token> = lex::lex(sql, opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let (obj, kind_at) = object_at(sql, &t)?;
    // Logic-layer functions (chapter 41) and loadable functions.
    if t.iter().enumerate().any(|(k, _)| {
        word(sql, &t, k, b"LANGUAGE")
            && (word(sql, &t, k + 1, b"JAVASCRIPT") || word(sql, &t, k + 1, b"KIRUN"))
    }) || (word(sql, &t, kind_at + 2, b"RETURNS") && word(sql, &t, kind_at + 4, b"SONAME"))
    {
        return None;
    }
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut k = kind_at + 1;
    // A view's query starts after its first AS at depth 0.
    let mut view_as = obj == Object::View;
    while k < t.len() {
        let starts_query = word(sql, &t, k, b"SELECT") || word(sql, &t, k, b"WITH");
        let prev_ok = k > 0
            && (punct(sql, &t, k - 1, b';')
                || punct(sql, &t, k - 1, b'(')
                || STATEMENT_AFTER.iter().any(|w| word(sql, &t, k - 1, w))
                || (word(sql, &t, k - 1, b"FOR") && word(sql, &t, k.saturating_sub(2), b"CURSOR"))
                || (view_as && word(sql, &t, k - 1, b"AS")));
        if view_as && word(sql, &t, k, b"AS") {
            view_as = word(sql, &t, k + 1, b"SELECT")
                || word(sql, &t, k + 1, b"WITH")
                || punct(sql, &t, k + 1, b'(');
        }
        if !(starts_query && prev_ok) {
            k += 1;
            continue;
        }
        view_as = false;
        let end = query_end(sql, &t, k, punct(sql, &t, k - 1, b'('), obj);
        let seg = &sql[t[k].start as usize..t[end].end as usize];
        let mut out: Option<Vec<u8>> = None;
        if let Some(v) = super::lateral::rewrite(seg, opts) {
            out = Some(v);
        }
        if let Some(v) = super::rollup::rewrite(
            out.as_deref().unwrap_or(seg),
            super::rollup::DEFAULT_MAX_COLUMNS,
        ) {
            out = Some(v.into_bytes());
        }
        match out {
            Some(v) => {
                edits.push((
                    t[k].start as usize,
                    t[end].end as usize,
                    String::from_utf8_lossy(&v).into_owned(),
                ));
                k = end + 1;
            }
            // Unchanged: its subqueries get their turn.
            None => k += 1,
        }
    }
    let text = if edits.is_empty() {
        None
    } else {
        let mut out = Vec::with_capacity(sql.len() + 128);
        let mut at = 0;
        for (s, e, rep) in &edits {
            out.extend_from_slice(&sql[at..*s]);
            // R4 round 5 (gaps row 69): routines, triggers and events keep
            // their comments, so the text as written rides along for
            // `SHOW CREATE` and `information_schema` ([`restore_original`]);
            // MariaDB drops a view's comments, and MySQL shows a view's
            // query reprinted anyway.
            if obj == Object::View {
                out.extend_from_slice(rep.as_bytes());
            } else {
                out.extend_from_slice(&marked(&sql[*s..*e], rep));
            }
            at = *e;
        }
        out.extend_from_slice(&sql[at..]);
        Some(out)
    };
    if let Some(what) = needs_gateway(text.as_deref().unwrap_or(sql), *opts) {
        return Some(Err(needs_gateway_error(obj, &what)));
    }
    text.map(Ok)
}

/// The opening of a marked segment: `/*sprig:o:<hex of the text as
/// written>*/`, then the rewritten text, then [`MARK_END`].
const MARK_OPEN: &[u8] = b"/*sprig:o:";
/// The end of a marked segment.
const MARK_END: &[u8] = b"/*sprig:e*/";

fn marked(original: &[u8], rewritten: &str) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(original.len() * 2 + rewritten.len() + 24);
    out.extend_from_slice(MARK_OPEN);
    for b in original {
        out.push(HEX[usize::from(b >> 4)]);
        out.push(HEX[usize::from(b & 15)]);
    }
    out.extend_from_slice(b"*/");
    out.extend_from_slice(rewritten.as_bytes());
    out.extend_from_slice(MARK_END);
    out
}

/// Whether `v` holds a marked segment (cheap test for result values).
#[must_use]
pub fn has_original(v: &[u8]) -> bool {
    v.windows(MARK_OPEN.len()).any(|w| w == MARK_OPEN)
}

/// A stored program's text (a body, or a whole `SHOW CREATE` answer) with
/// each segment the 3306 rules rewrote at `CREATE` given back as written
/// (R4 round 5, gaps row 69): MySQL shows routines, triggers and events
/// as their author wrote them. `None` when `v` has no marked segment.
#[must_use]
pub fn restore_original(v: &[u8]) -> Option<Vec<u8>> {
    if !has_original(v) {
        return None;
    }
    let find = |from: usize, pat: &[u8]| {
        v.get(from..)?
            .windows(pat.len())
            .position(|w| w == pat)
            .map(|p| p + from)
    };
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = Vec::with_capacity(v.len());
    let mut at = 0;
    while let Some(open) = find(at, MARK_OPEN) {
        let hex_from = open + MARK_OPEN.len();
        let close = find(hex_from, b"*/")?;
        let end = find(close + 2, MARK_END)?;
        let hex = &v[hex_from..close];
        if !hex.len().is_multiple_of(2) {
            return None;
        }
        out.extend_from_slice(&v[at..open]);
        for pair in hex.chunks(2) {
            out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
        }
        at = end + MARK_END.len();
    }
    out.extend_from_slice(&v[at..]);
    Some(out)
}

/// The last token of the query starting at `t[start]`: the token before
/// the `)` that closes it (`in_parens`), else before the `;` that ends its
/// statement, else the statement's last (a view's `WITH CHECK OPTION`
/// left out).
fn query_end(sql: &[u8], t: &[Token], start: usize, in_parens: bool, obj: Object) -> usize {
    let mut depth = 0i32;
    let mut k = start;
    while k < t.len() {
        if punct(sql, t, k, b'(') {
            depth += 1;
        } else if punct(sql, t, k, b')') {
            if depth == 0 {
                if in_parens {
                    return k - 1;
                }
            } else {
                depth -= 1;
            }
        } else if punct(sql, t, k, b';') && depth == 0 {
            return k - 1;
        }
        k += 1;
    }
    let mut last = t.len() - 1;
    while last > start && punct(sql, t, last, b';') {
        last -= 1;
    }
    if obj == Object::View
        && last >= start + 3
        && word(sql, t, last, b"OPTION")
        && word(sql, t, last - 1, b"CHECK")
    {
        last -= 2;
        if word(sql, t, last, b"CASCADED") || word(sql, t, last, b"LOCAL") {
            last -= 1;
        }
        if word(sql, t, last, b"WITH") {
            last -= 1;
        }
    }
    last
}

/// What in `sql` (a stored program's or view's text, rules applied) would
/// need the gateway at run time.
fn needs_gateway(sql: &[u8], opts: LexOptions) -> Option<String> {
    let t: Vec<Token> = lex::lex(sql, &opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    for k in 0..t.len() {
        let ident = |j: usize| {
            t.get(j)
                .filter(|x| matches!(x.kind, TokenKind::Word | TokenKind::QuotedIdent))
                .map(|x| lex::ident_value(x.bytes(sql)).into_owned())
        };
        if word(sql, &t, k, b"LATERAL") {
            return Some("this LATERAL derived table (it needs the gateway's apply)".to_owned());
        }
        if punct(sql, &t, k + 1, b'.')
            && !(k > 0 && punct(sql, &t, k - 1, b'.'))
            && let Some(s) = ident(k)
            && (s.eq_ignore_ascii_case(b"performance_schema") || s.eq_ignore_ascii_case(b"sys"))
            && let Some(o) = ident(k + 2)
            && !punct(sql, &t, k + 3, b'(')
        {
            return Some(format!(
                "{}.{} (the gateway answers it)",
                String::from_utf8_lossy(&s).to_ascii_lowercase(),
                String::from_utf8_lossy(&o)
            ));
        }
        if word(sql, &t, k, b"CALL")
            && ident(k + 1).is_some_and(|s| s.eq_ignore_ascii_case(b"sprig"))
            && punct(sql, &t, k + 2, b'.')
            && let Some(p) = ident(k + 3)
        {
            return Some(format!(
                "CALL sprig.{} (the gateway runs it)",
                String::from_utf8_lossy(&p)
            ));
        }
    }
    None
}

/// The tables the DML statements of a procedure's, function's, trigger's
/// or event's body write (`INSERT`, `REPLACE`, `UPDATE`, `DELETE`, multi-
/// table forms included), as written: (database, table). Empty for other
/// statements, views and logic-layer functions. Multi-table `UPDATE` and
/// `DELETE` list every table named before `SET` or `FROM`/`USING`, aliases
/// too (an alias resolves to no collection).
#[must_use]
#[allow(clippy::too_many_lines)] // one branch per statement kind
pub fn write_targets(sql: &[u8], opts: &LexOptions) -> Vec<(Option<String>, String)> {
    const MODIFIERS: &[&[u8]] = &[
        b"LOW_PRIORITY",
        b"DELAYED",
        b"HIGH_PRIORITY",
        b"IGNORE",
        b"QUICK",
    ];
    let Ok(tokens) = lex::lex(sql, opts) else {
        return Vec::new();
    };
    let t: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let Some((obj, kind_at)) = object_at(sql, &t) else {
        return Vec::new();
    };
    if obj == Object::View
        || t.iter().enumerate().any(|(k, _)| {
            word(sql, &t, k, b"LANGUAGE")
                && (word(sql, &t, k + 1, b"JAVASCRIPT") || word(sql, &t, k + 1, b"KIRUN"))
        })
    {
        return Vec::new();
    }
    let ident = |j: usize| {
        t.get(j)
            .filter(|x| x.kind.is_ident_like())
            .map(|x| String::from_utf8_lossy(&lex::ident_value(x.bytes(sql))).into_owned())
    };
    // `t` or `d.t` at `j`: the name and the index after it.
    let name_at = |j: usize| -> Option<((Option<String>, String), usize)> {
        let first = ident(j)?;
        if punct(sql, &t, j + 1, b'.') {
            Some(((Some(first), ident(j + 2)?), j + 3))
        } else {
            Some(((None, first), j + 1))
        }
    };
    // A trigger's own event (`BEFORE INSERT ON t`) isn't a write.
    let mut out: Vec<(Option<String>, String)> = Vec::new();
    let mut push = |n: (Option<String>, String)| {
        if !out.contains(&n) {
            out.push(n);
        }
    };
    for k in kind_at + 1..t.len() {
        let dml = [&b"INSERT"[..], b"REPLACE", b"UPDATE", b"DELETE"]
            .into_iter()
            .find(|w| word(sql, &t, k, w));
        let Some(dml) = dml else { continue };
        if punct(sql, &t, k + 1, b'(')
            || [&b"KEY"[..], b"BEFORE", b"AFTER", b"FOR", b"ON", b"OR"]
                .iter()
                .any(|w| word(sql, &t, k - 1, w))
        {
            continue;
        }
        let mut j = k + 1;
        while MODIFIERS.iter().any(|w| word(sql, &t, j, w)) {
            j += 1;
        }
        match dml {
            b"INSERT" | b"REPLACE" => {
                if word(sql, &t, j, b"INTO") {
                    j += 1;
                }
                if let Some((n, _)) = name_at(j) {
                    push(n);
                }
            }
            _ => {
                // UPDATE refs SET / DELETE [FROM] refs [FROM | USING]
                let delete_from = dml == b"DELETE" && word(sql, &t, j, b"FROM");
                if delete_from {
                    j += 1;
                }
                let stops: &[&[u8]] = if dml == b"UPDATE" {
                    &[b"SET"]
                } else if delete_from {
                    &[b"USING", b"WHERE", b"ORDER", b"LIMIT", b"PARTITION"]
                } else {
                    &[b"FROM", b"USING"]
                };
                let mut expect = true;
                let mut depth = 0i32;
                while j < t.len() && !(depth == 0 && stops.iter().any(|w| word(sql, &t, j, w))) {
                    if punct(sql, &t, j, b';') && depth == 0 {
                        break;
                    }
                    if punct(sql, &t, j, b'(') {
                        depth += 1;
                    } else if punct(sql, &t, j, b')') {
                        depth -= 1;
                    } else if depth == 0
                        && expect
                        && let Some((n, next)) = name_at(j)
                    {
                        push(n);
                        j = next;
                        expect = false;
                        continue;
                    }
                    if depth == 0 && (punct(sql, &t, j, b',') || word(sql, &t, j, b"JOIN")) {
                        expect = true;
                    }
                    j += 1;
                }
            }
        }
    }
    out
}

/// 39602 for a stored program `sql` whose body writes the collection
/// `name` (chapter 31: such writes take the gateway's document path).
#[must_use]
pub fn collection_write_error(sql: &[u8], opts: &LexOptions, name: &str) -> WireError {
    let obj = object(sql, opts).unwrap_or(Object::Procedure);
    needs_gateway_error(
        obj,
        &format!("a write to the collection {name} (the gateway's document path keeps it)"),
    )
}

fn needs_gateway_error(obj: Object, what: &str) -> WireError {
    let kind = match obj {
        Object::View => "a view",
        Object::Procedure => "a procedure",
        Object::Function => "a function",
        Object::Trigger => "a trigger",
        Object::Event => "an event",
    };
    WireError::new(
        39602,
        *b"42000",
        format!(
            "ER_SPRIG_NEEDS_GATEWAY: {kind} can't use {what}: MariaDB runs stored code without \
             the gateway; run the statement from the client instead"
        ),
    )
}

#[cfg(test)]
mod tests;
