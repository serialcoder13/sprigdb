//! Statement text for replay (chapter 13, "Replay and acceptance"):
//! binding a sample's literals into a fingerprint's text, the select form
//! of a write, and the `IGNORE INDEX` hint that gives the before plan.
//!
//! Everything here works on a small MariaDB-aware token stream with byte
//! spans, and edits the original text by span, so what runs is the user's
//! statement with the fewest possible changes.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

/// One token with its byte span in the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tok {
    pub(crate) kind: TokKind,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// Token kinds; whitespace and comments are not returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokKind {
    /// An unquoted word: keyword or identifier.
    Word,
    /// A back-quoted identifier.
    Quoted,
    /// A string literal, single- or double-quoted.
    Str,
    /// A number.
    Num,
    /// `?`.
    Placeholder,
    /// `@name` or `@@name`.
    Var,
    /// Any other character.
    Punct(u8),
}

impl Tok {
    pub(crate) fn text<'a>(&self, sql: &'a str) -> &'a str {
        &sql[self.start..self.end]
    }

    /// Whether the token is the (unquoted) keyword `kw`.
    pub(crate) fn is_kw(&self, sql: &str, kw: &str) -> bool {
        self.kind == TokKind::Word && self.text(sql).eq_ignore_ascii_case(kw)
    }

    fn is_punct(&self, c: u8) -> bool {
        self.kind == TokKind::Punct(c)
    }

    fn is_ident(&self) -> bool {
        matches!(self.kind, TokKind::Word | TokKind::Quoted)
    }

    /// The identifier's name: unquoted, with doubled back quotes undone.
    pub(crate) fn ident(&self, sql: &str) -> String {
        let t = self.text(sql);
        if self.kind == TokKind::Quoted && t.len() >= 2 {
            t[1..t.len() - 1].replace("``", "`")
        } else {
            t.to_owned()
        }
    }
}

/// Why a statement can't be turned into a replayable `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum Unreplayable {
    /// The text didn't tokenize (an unterminated quote or comment).
    Lex(String),
    /// A statement kind with no read to replay (`INSERT ... VALUES`, DDL).
    NoRead(String),
    /// `SELECT ... INTO`, which writes variables or files.
    SelectInto,
    /// The shape wasn't understood.
    Shape(String),
    /// The sample doesn't fit the text: placeholder and value counts differ.
    Binding(String),
    /// The target table wasn't found in the statement, so no
    /// `IGNORE INDEX` could be placed.
    TableNotFound,
}

impl std::fmt::Display for Unreplayable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unreplayable::Lex(s) => write!(f, "lex: {s}"),
            Unreplayable::NoRead(s) => write!(f, "no read to replay: {s}"),
            Unreplayable::SelectInto => f.write_str("SELECT ... INTO"),
            Unreplayable::Shape(s) => write!(f, "shape: {s}"),
            Unreplayable::Binding(s) => write!(f, "binding: {s}"),
            Unreplayable::TableNotFound => f.write_str("target table not referenced"),
        }
    }
}

/// Tokenizes MariaDB SQL. `#`, `-- ` and `/* */` comments are skipped,
/// executable comments (`/*! */`, `/*M! */`) included: replay doesn't need
/// their content, and fingerprints have already normalized them.
#[allow(clippy::too_many_lines)]
pub(crate) fn lex(sql: &str) -> Result<Vec<Tok>, Unreplayable> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        match c {
            b' ' | b'\t' | b'\r' | b'\n' | b'\x0c' => {
                i += 1;
            }
            b'#' => {
                i = skip_line(b, i);
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                i = skip_line(b, i);
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let end = find(b, i + 2, b"*/")
                    .ok_or_else(|| Unreplayable::Lex("unterminated comment".into()))?;
                i = end + 2;
            }
            b'\'' | b'"' => {
                i = skip_quoted(b, i, c, true)
                    .ok_or_else(|| Unreplayable::Lex("unterminated string".into()))?;
                out.push(Tok {
                    kind: TokKind::Str,
                    start,
                    end: i,
                });
            }
            b'`' => {
                i = skip_quoted(b, i, b'`', false)
                    .ok_or_else(|| Unreplayable::Lex("unterminated identifier".into()))?;
                out.push(Tok {
                    kind: TokKind::Quoted,
                    start,
                    end: i,
                });
            }
            b'?' => {
                i += 1;
                out.push(Tok {
                    kind: TokKind::Placeholder,
                    start,
                    end: i,
                });
            }
            b'@' => {
                i += 1;
                if b.get(i) == Some(&b'@') {
                    i += 1;
                }
                if let Some(&q @ (b'`' | b'\'' | b'"')) = b.get(i) {
                    i = skip_quoted(b, i, q, q != b'`')
                        .ok_or_else(|| Unreplayable::Lex("unterminated variable".into()))?;
                } else {
                    while i < b.len() && (is_word_byte(b[i]) || b[i] == b'.') {
                        i += 1;
                    }
                }
                out.push(Tok {
                    kind: TokKind::Var,
                    start,
                    end: i,
                });
            }
            b'0'..=b'9' => {
                i = skip_number(b, i);
                // `1abc` is an identifier in MariaDB.
                if i < b.len() && is_word_byte(b[i]) {
                    while i < b.len() && is_word_byte(b[i]) {
                        i += 1;
                    }
                    out.push(Tok {
                        kind: TokKind::Word,
                        start,
                        end: i,
                    });
                } else {
                    out.push(Tok {
                        kind: TokKind::Num,
                        start,
                        end: i,
                    });
                }
            }
            b'.' if b.get(i + 1).is_some_and(u8::is_ascii_digit)
                && !out.last().is_some_and(|t: &Tok| t.is_ident()) =>
            {
                i = skip_number(b, i);
                out.push(Tok {
                    kind: TokKind::Num,
                    start,
                    end: i,
                });
            }
            c if is_word_byte(c) => {
                while i < b.len() && is_word_byte(b[i]) {
                    i += 1;
                }
                out.push(Tok {
                    kind: TokKind::Word,
                    start,
                    end: i,
                });
            }
            c => {
                // Multi-byte UTF-8 outside quotes: part of a word.
                if c >= 0x80 {
                    while i < b.len() && (b[i] >= 0x80 || is_word_byte(b[i])) {
                        i += 1;
                    }
                    out.push(Tok {
                        kind: TokKind::Word,
                        start,
                        end: i,
                    });
                } else {
                    i += 1;
                    out.push(Tok {
                        kind: TokKind::Punct(c),
                        start,
                        end: i,
                    });
                }
            }
        }
    }
    Ok(out)
}

fn is_word_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

fn skip_line(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i] != b'\n' {
        i += 1;
    }
    i
}

fn find(b: &[u8], from: usize, pat: &[u8]) -> Option<usize> {
    (from..b.len().saturating_sub(pat.len() - 1)).find(|&i| b[i..].starts_with(pat))
}

/// Past the closing quote; a doubled quote is an escaped one, and with
/// `backslash` a backslash escapes the next byte.
fn skip_quoted(b: &[u8], mut i: usize, q: u8, backslash: bool) -> Option<usize> {
    i += 1;
    while i < b.len() {
        let c = b[i];
        if backslash && c == b'\\' {
            i += 2;
            continue;
        }
        if c == q {
            if b.get(i + 1) == Some(&q) {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn skip_number(b: &[u8], mut i: usize) -> usize {
    if b[i] == b'0' && matches!(b.get(i + 1), Some(b'x' | b'X' | b'b' | b'B')) {
        i += 2;
        while i < b.len() && b[i].is_ascii_hexdigit() {
            i += 1;
        }
        return i;
    }
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E'))
        && (b.get(i + 1).is_some_and(u8::is_ascii_digit)
            || (matches!(b.get(i + 1), Some(b'+' | b'-'))
                && b.get(i + 2).is_some_and(u8::is_ascii_digit)))
    {
        i += 2;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    }
    i
}

/// The parenthesis depth before each token.
fn depths(toks: &[Tok]) -> Vec<i32> {
    let mut d = 0;
    toks.iter()
        .map(|t| {
            if t.is_punct(b')') {
                d -= 1;
            }
            let here = d;
            if t.is_punct(b'(') {
                d += 1;
            }
            here
        })
        .collect()
}

/// A sampled literal (chapter 12, "Parameter samples for replay"). The
/// sample's literal list holds one value per `?` of the fingerprint text,
/// in order; a collapsed `IN (...)` takes one [`SampleValue::List`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "t", content = "v")]
pub enum SampleValue {
    /// `NULL`.
    Null,
    /// A signed integer.
    Int(i64),
    /// An unsigned integer above `i64::MAX`.
    UInt(u64),
    /// A decimal or float exactly as written, e.g. `-1.50` or `3e5`.
    Number(String),
    /// A string (sent as a quoted literal in the connection character set).
    Str(String),
    /// A hexadecimal or binary-string literal's bytes, sent as `X'..'`.
    Bytes(Vec<u8>),
    /// `DATE '...'`.
    Date(String),
    /// `TIME '...'`.
    Time(String),
    /// `TIMESTAMP '...'`.
    Timestamp(String),
    /// `TRUE` or `FALSE`.
    Bool(bool),
    /// The literal exactly as the client wrote it (`'open'`,
    /// `_utf8mb4'x'`, `-42`, `X'0A'`, `DATE '2026-06-01'`): chapter 12's
    /// literal spans, replayed without re-typing. Must be one literal;
    /// [`bind_literals`] refuses anything else.
    Raw(String),
    /// An `IN` list, or a row constructor when nested.
    List(Vec<SampleValue>),
}

impl SampleValue {
    /// The literal as SQL text. `no_backslash_escapes` follows the
    /// sample's `sql_mode`.
    #[must_use]
    pub fn render(&self, no_backslash_escapes: bool) -> String {
        let mut s = String::new();
        self.render_into(&mut s, no_backslash_escapes);
        s
    }

    fn render_into(&self, out: &mut String, nbe: bool) {
        match self {
            SampleValue::Null => out.push_str("NULL"),
            SampleValue::Int(v) => {
                let _ = write!(out, "{v}");
            }
            SampleValue::UInt(v) => {
                let _ = write!(out, "{v}");
            }
            SampleValue::Number(v) => {
                // Only digits, sign, point and exponent: never SQL.
                if !v.is_empty()
                    && v.bytes().all(|c| {
                        c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E')
                    })
                {
                    out.push_str(v);
                } else {
                    quote_str(out, v, nbe);
                }
            }
            SampleValue::Str(v) => quote_str(out, v, nbe),
            SampleValue::Bytes(v) => {
                out.push_str("X'");
                for b in v {
                    let _ = write!(out, "{b:02X}");
                }
                out.push('\'');
            }
            SampleValue::Date(v) => {
                out.push_str("DATE ");
                quote_str(out, v, nbe);
            }
            SampleValue::Time(v) => {
                out.push_str("TIME ");
                quote_str(out, v, nbe);
            }
            SampleValue::Timestamp(v) => {
                out.push_str("TIMESTAMP ");
                quote_str(out, v, nbe);
            }
            SampleValue::Bool(v) => out.push_str(if *v { "TRUE" } else { "FALSE" }),
            SampleValue::Raw(v) => {
                if is_single_literal(v) {
                    out.push_str(v);
                } else {
                    // Never SQL: falls back to a string.
                    quote_str(out, v, nbe);
                }
            }
            SampleValue::List(items) => {
                out.push('(');
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    v.render_into(out, nbe);
                }
                out.push(')');
            }
        }
    }
}

/// A single-quoted literal. Quotes are doubled; with backslash escapes on,
/// backslashes and NUL are escaped too.
fn quote_str(out: &mut String, s: &str, no_backslash_escapes: bool) {
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' if !no_backslash_escapes => out.push_str("\\\\"),
            '\0' if !no_backslash_escapes => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
}

/// Whether `s` is exactly one literal: a number (optionally signed), a
/// string with an optional introducer (`_utf8mb4`, `N`, `X`, `B`) or
/// temporal keyword, `NULL`, `TRUE` or `FALSE`.
#[must_use]
pub fn is_single_literal(s: &str) -> bool {
    let Ok(t) = lex(s) else { return false };
    let word = |t: &Tok, ok: &dyn Fn(&str) -> bool| t.kind == TokKind::Word && ok(t.text(s));
    match t.as_slice() {
        [a] => {
            matches!(a.kind, TokKind::Num | TokKind::Str)
                || word(a, &|w| {
                    ["NULL", "TRUE", "FALSE"]
                        .iter()
                        .any(|k| w.eq_ignore_ascii_case(k))
                })
        }
        [a, b] if a.kind == TokKind::Punct(b'-') || a.kind == TokKind::Punct(b'+') => {
            b.kind == TokKind::Num
        }
        [a, b] => {
            b.kind == TokKind::Str
                && word(a, &|w| {
                    w.starts_with('_')
                        || ["N", "X", "B", "DATE", "TIME", "TIMESTAMP"]
                            .iter()
                            .any(|k| w.eq_ignore_ascii_case(k))
                })
        }
        _ => false,
    }
}

fn check_raw(v: &SampleValue) -> Result<(), Unreplayable> {
    match v {
        SampleValue::Raw(r) if !is_single_literal(r) => Err(Unreplayable::Binding(
            "a raw value that is not one literal".into(),
        )),
        SampleValue::List(items) => items.iter().try_for_each(check_raw),
        _ => Ok(()),
    }
}

/// Binds a sample's literals into a fingerprint's normalized text: each `?`
/// takes the next value, and each collapsed `(...)` takes a list.
///
/// # Errors
/// [`Unreplayable::Binding`] when the counts differ or a collapsed list
/// gets a scalar; [`Unreplayable::Lex`] when the text doesn't tokenize.
pub fn bind_literals(
    text: &str,
    values: &[SampleValue],
    no_backslash_escapes: bool,
) -> Result<String, Unreplayable> {
    values.iter().try_for_each(check_raw)?;
    let toks = lex(text)?;
    let mut out = String::with_capacity(text.len() + 16 * values.len());
    let mut last = 0;
    let mut next = values.iter();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if t.kind == TokKind::Placeholder {
            let v = next
                .next()
                .ok_or_else(|| Unreplayable::Binding("more placeholders than values".into()))?;
            out.push_str(&text[last..t.start]);
            v.render_into(&mut out, no_backslash_escapes);
            last = t.end;
        } else if t.is_punct(b'(')
            && toks.get(i + 4).is_some_and(|c| c.is_punct(b')'))
            && toks[i + 1..i + 4].iter().all(|d| d.is_punct(b'.'))
        {
            let v = next
                .next()
                .ok_or_else(|| Unreplayable::Binding("more lists than values".into()))?;
            let SampleValue::List(items) = v else {
                return Err(Unreplayable::Binding(
                    "a collapsed list needs a list value".into(),
                ));
            };
            if items.is_empty() {
                return Err(Unreplayable::Binding("an empty list".into()));
            }
            out.push_str(&text[last..t.start]);
            v.render_into(&mut out, no_backslash_escapes);
            last = toks[i + 4].end;
            i += 5;
            continue;
        }
        i += 1;
    }
    if next.next().is_some() {
        return Err(Unreplayable::Binding(
            "more values than placeholders".into(),
        ));
    }
    out.push_str(&text[last..]);
    Ok(out)
}

/// What a statement became for replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectForm {
    /// The `SELECT` to replay.
    pub sql: String,
    /// The original statement kind, upper case (`SELECT`, `UPDATE`, ...).
    pub from_kind: String,
}

/// The replayable `SELECT` of a statement (chapter 13):
///
/// - `SELECT` and `WITH`: as written, without a locking clause
///   (`FOR UPDATE`, `LOCK IN SHARE MODE`), since replay must take no row
///   locks on the validation node;
/// - `UPDATE`: `SELECT <pk> FROM <tables> [WHERE] [ORDER BY] [LIMIT]`;
///   a multi-table update selects `1`;
/// - `DELETE`: the same; the multi-table forms select `1` from their
///   join;
/// - `INSERT ... SELECT` and `REPLACE ... SELECT`: the `SELECT`, without
///   `ON DUPLICATE KEY UPDATE` and `RETURNING`.
///
/// `pk` lists the table's primary-key columns; empty selects `1`.
///
/// # Errors
/// [`Unreplayable`] for statements with no read and shapes it can't take
/// apart.
pub fn select_form(sql: &str, pk: &[String]) -> Result<SelectForm, Unreplayable> {
    let toks = lex(sql)?;
    let d = depths(&toks);
    let first = toks
        .iter()
        .position(|t| !t.is_punct(b'('))
        .ok_or_else(|| Unreplayable::Shape("empty statement".into()))?;
    let head = toks[first];
    let kind = head.text(sql).to_ascii_uppercase();
    let trimmed_end = trim_end(sql, &toks);
    let body = match kind.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" if head.kind == TokKind::Word => {
            if kind == "VALUES" || kind == "TABLE" {
                // Table value constructors read nothing worth an index.
                return Err(Unreplayable::NoRead(kind));
            }
            check_no_into(sql, &toks)?;
            strip_spans(sql, 0, trimmed_end, &locking_spans(sql, &toks))
        }
        "UPDATE" => update_form(sql, &toks, &d, first, trimmed_end, pk)?,
        "DELETE" => delete_form(sql, &toks, &d, first, trimmed_end, pk)?,
        "INSERT" | "REPLACE" => insert_select_form(sql, &toks, &d, first, trimmed_end)?,
        _ => return Err(Unreplayable::NoRead(kind)),
    };
    Ok(SelectForm {
        sql: body,
        from_kind: kind,
    })
}

/// The end of the statement without a trailing `;`.
fn trim_end(sql: &str, toks: &[Tok]) -> usize {
    let mut n = toks.len();
    while n > 0 && toks[n - 1].is_punct(b';') {
        n -= 1;
    }
    if n == 0 {
        0
    } else {
        toks[n - 1].end.min(sql.len())
    }
}

/// Copies `sql[from..to]` leaving out `cuts`.
fn strip_spans(sql: &str, from: usize, to: usize, cuts: &[(usize, usize)]) -> String {
    let mut out = String::with_capacity(to - from);
    let mut at = from;
    for &(s, e) in cuts {
        if s >= at && e <= to {
            out.push_str(&sql[at..s]);
            at = e;
        }
    }
    out.push_str(&sql[at..to]);
    out.trim().to_owned()
}

fn check_no_into(sql: &str, toks: &[Tok]) -> Result<(), Unreplayable> {
    // `INTO` appears in a SELECT only as SELECT ... INTO @var / OUTFILE /
    // DUMPFILE, at any depth of a union part.
    if toks.iter().any(|t| t.is_kw(sql, "INTO")) {
        return Err(Unreplayable::SelectInto);
    }
    Ok(())
}

/// Spans of top-level and union-part locking clauses: `FOR UPDATE`
/// (with `NOWAIT`, `SKIP LOCKED`, `WAIT n`), `FOR SHARE` and
/// `LOCK IN SHARE MODE`.
fn locking_spans(sql: &str, toks: &[Tok]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        let next_is = |k: usize, kw: &str| toks.get(i + k).is_some_and(|x| x.is_kw(sql, kw));
        let mut end_idx = None;
        if t.is_kw(sql, "FOR") && (next_is(1, "UPDATE") || next_is(1, "SHARE")) {
            let mut j = i + 2;
            if toks.get(j).is_some_and(|x| x.is_kw(sql, "NOWAIT")) {
                j += 1;
            } else if (toks.get(j).is_some_and(|x| x.is_kw(sql, "SKIP"))
                && toks.get(j + 1).is_some_and(|x| x.is_kw(sql, "LOCKED")))
                || (toks.get(j).is_some_and(|x| x.is_kw(sql, "WAIT"))
                    && toks.get(j + 1).is_some_and(|x| x.kind == TokKind::Num))
            {
                j += 2;
            }
            end_idx = Some(j - 1);
        } else if t.is_kw(sql, "LOCK")
            && next_is(1, "IN")
            && next_is(2, "SHARE")
            && next_is(3, "MODE")
        {
            end_idx = Some(i + 3);
        }
        if let Some(e) = end_idx {
            out.push((t.start, toks[e].end));
            i = e + 1;
        } else {
            i += 1;
        }
    }
    out
}

fn top_level_kw(sql: &str, toks: &[Tok], d: &[i32], from: usize, kws: &[&str]) -> Option<usize> {
    (from..toks.len()).find(|&i| d[i] == 0 && kws.iter().any(|k| toks[i].is_kw(sql, k)))
}

fn pk_list(pk: &[String], single: bool) -> String {
    if pk.is_empty() || !single {
        return "1".into();
    }
    pk.iter()
        .map(|c| sprig_common::names::quote(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `UPDATE [LOW_PRIORITY] [IGNORE] refs SET ... [WHERE ...] [ORDER BY ...]
/// [LIMIT ...]`.
fn update_form(
    sql: &str,
    toks: &[Tok],
    d: &[i32],
    first: usize,
    end: usize,
    pk: &[String],
) -> Result<String, Unreplayable> {
    let mut i = first + 1;
    while toks
        .get(i)
        .is_some_and(|t| t.is_kw(sql, "LOW_PRIORITY") || t.is_kw(sql, "IGNORE"))
    {
        i += 1;
    }
    let set = top_level_kw(sql, toks, d, i, &["SET"])
        .ok_or_else(|| Unreplayable::Shape("UPDATE without SET".into()))?;
    if set == i {
        return Err(Unreplayable::Shape("UPDATE without a table".into()));
    }
    let refs = &sql[toks[i].start..toks[set - 1].end];
    let single =
        !(i..set).any(|k| d[k] == 0 && (toks[k].is_punct(b',') || toks[k].is_kw(sql, "JOIN")));
    let tail = top_level_kw(sql, toks, d, set + 1, &["WHERE", "ORDER", "LIMIT"]);
    let mut out = format!("SELECT {} FROM {refs}", pk_list(pk, single));
    if let Some(t) = tail {
        let cut = top_level_kw(sql, toks, d, t, &["RETURNING"]).map_or(end, |r| toks[r].start);
        out.push(' ');
        out.push_str(sql[toks[t].start..cut].trim());
    }
    Ok(out)
}

/// `DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM t ...`, `DELETE t1 FROM
/// refs ...` and `DELETE FROM t1 USING refs ...`.
fn delete_form(
    sql: &str,
    toks: &[Tok],
    d: &[i32],
    first: usize,
    end: usize,
    pk: &[String],
) -> Result<String, Unreplayable> {
    let mut i = first + 1;
    while toks.get(i).is_some_and(|t| {
        t.is_kw(sql, "LOW_PRIORITY") || t.is_kw(sql, "QUICK") || t.is_kw(sql, "IGNORE")
    }) {
        i += 1;
    }
    let cut_returning = |from: usize| {
        top_level_kw(sql, toks, d, from, &["RETURNING"]).map_or(end, |r| toks[r].start)
    };
    if toks.get(i).is_some_and(|t| t.is_kw(sql, "FROM")) {
        // `DELETE FROM t1, t2 USING refs WHERE ...`: read the USING part.
        if let Some(u) = top_level_kw(sql, toks, d, i + 1, &["USING"]) {
            let body = sql[toks.get(u + 1).map_or(end, |t| t.start)..cut_returning(u)].trim();
            return Ok(format!("SELECT 1 FROM {body}"));
        }
        let single = !(i + 1..toks.len())
            .take_while(|&k| !(d[k] == 0 && toks[k].is_kw(sql, "WHERE")))
            .any(|k| d[k] == 0 && (toks[k].is_punct(b',') || toks[k].is_kw(sql, "JOIN")));
        let body = sql[toks.get(i + 1).map_or(end, |t| t.start)..cut_returning(i + 1)].trim();
        if body.is_empty() {
            return Err(Unreplayable::Shape("DELETE without a table".into()));
        }
        return Ok(format!("SELECT {} FROM {body}", pk_list(pk, single)));
    }
    // `DELETE t1, t2 FROM refs WHERE ...`.
    let from = top_level_kw(sql, toks, d, i, &["FROM"])
        .ok_or_else(|| Unreplayable::Shape("DELETE without FROM".into()))?;
    let body = sql[toks.get(from + 1).map_or(end, |t| t.start)..cut_returning(from)].trim();
    Ok(format!("SELECT 1 FROM {body}"))
}

/// `INSERT ... SELECT` and `REPLACE ... SELECT`.
fn insert_select_form(
    sql: &str,
    toks: &[Tok],
    d: &[i32],
    first: usize,
    end: usize,
) -> Result<String, Unreplayable> {
    // The SELECT (or WITH, or a parenthesized SELECT) at depth 0 after the
    // table and its column list.
    let mut start = None;
    for k in first + 1..toks.len() {
        if d[k] != 0 {
            continue;
        }
        let t = toks[k];
        if t.is_kw(sql, "VALUES") || t.is_kw(sql, "VALUE") || t.is_kw(sql, "SET") {
            return Err(Unreplayable::NoRead("INSERT ... VALUES".into()));
        }
        if t.is_kw(sql, "SELECT") || t.is_kw(sql, "WITH") {
            start = Some(k);
            break;
        }
        if t.is_punct(b'(')
            && toks
                .get(k + 1)
                .is_some_and(|n| n.is_kw(sql, "SELECT") || n.is_kw(sql, "WITH"))
        {
            start = Some(k);
            break;
        }
    }
    let s = start.ok_or_else(|| Unreplayable::NoRead("INSERT without SELECT".into()))?;
    let stop = (s..toks.len())
        .find(|&k| {
            d[k] == 0
                && ((toks[k].is_kw(sql, "ON")
                    && toks.get(k + 1).is_some_and(|n| n.is_kw(sql, "DUPLICATE")))
                    || toks[k].is_kw(sql, "RETURNING"))
        })
        .map_or(end, |k| toks[k].start);
    let body = &sql[toks[s].start..stop];
    // Re-run the SELECT rules on the extracted text (locking clauses).
    let inner = select_form(body.trim(), &[])?;
    Ok(inner.sql)
}

/// Words that end a table reference instead of being its alias.
const NOT_ALIAS: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "STRAIGHT_JOIN",
    "FULL",
    "OUTER",
    "ON",
    "USING",
    "USE",
    "IGNORE",
    "FORCE",
    "FOR",
    "LOCK",
    "INTO",
    "PARTITION",
    "SET",
    "RETURNING",
    "PROCEDURE",
    "AS",
    "OFFSET",
    "FETCH",
];

/// Words that end a `FROM` clause at their depth.
const FROM_ENDS: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "RETURNING",
    "INTO",
    "FOR",
    "LOCK",
    "SET",
    "PROCEDURE",
];

/// Adds `` IGNORE INDEX (`index`) `` after every reference to
/// `schema.table` in a `SELECT` (`FROM` and `JOIN` lists at any depth,
/// subqueries included). Unqualified references match when
/// `default_db` is `schema`. Names compare without case.
///
/// # Errors
/// [`Unreplayable::TableNotFound`] when no reference matched, and lexing
/// errors.
pub fn with_ignore_index(
    sql: &str,
    schema: &str,
    table: &str,
    index: &str,
    default_db: Option<&str>,
) -> Result<String, Unreplayable> {
    let toks = lex(sql)?;
    let hint = format!(" IGNORE INDEX ({})", sprig_common::names::quote(index));
    let points = table_ref_ends(sql, &toks, schema, table, default_db);
    if points.is_empty() {
        return Err(Unreplayable::TableNotFound);
    }
    let mut out = String::with_capacity(sql.len() + points.len() * hint.len());
    let mut at = 0;
    for p in points {
        out.push_str(&sql[at..p]);
        out.push_str(&hint);
        at = p;
    }
    out.push_str(&sql[at..]);
    Ok(out)
}

/// Byte offsets right after each matching table reference and its alias.
#[allow(clippy::too_many_lines)]
fn table_ref_ends(
    sql: &str,
    toks: &[Tok],
    schema: &str,
    table: &str,
    default_db: Option<&str>,
) -> Vec<usize> {
    // Per depth: inside a FROM clause, and whether a table name may come
    // next.
    let mut in_from: Vec<bool> = vec![false];
    let mut expect = false;
    let mut out = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        let depth = in_from.len() - 1;
        if t.is_punct(b'(') {
            let sub_select = toks
                .get(i + 1)
                .is_some_and(|n| n.is_kw(sql, "SELECT") || n.is_kw(sql, "WITH"));
            // A nested join list `FROM (a JOIN b)` keeps expecting tables.
            in_from.push(expect && !sub_select);
            i += 1;
            continue;
        }
        if t.is_punct(b')') {
            if in_from.len() > 1 {
                in_from.pop();
            }
            expect = false;
            i += 1;
            continue;
        }
        if t.is_kw(sql, "FROM") {
            in_from[depth] = true;
            expect = true;
            i += 1;
            continue;
        }
        if t.is_kw(sql, "JOIN") || t.is_kw(sql, "STRAIGHT_JOIN") {
            expect = true;
            i += 1;
            continue;
        }
        if t.is_punct(b',') {
            expect = in_from[depth];
            i += 1;
            continue;
        }
        if t.kind == TokKind::Word && FROM_ENDS.iter().any(|k| t.is_kw(sql, k)) {
            in_from[depth] = false;
            expect = false;
            i += 1;
            continue;
        }
        if t.is_kw(sql, "ON") || t.is_kw(sql, "USING") {
            expect = false;
            i += 1;
            continue;
        }
        if expect && t.is_ident() {
            expect = false;
            // A table function such as JSON_TABLE(...) isn't a table.
            if toks.get(i + 1).is_some_and(|n| n.is_punct(b'(')) {
                i += 1;
                continue;
            }
            // [schema .] table
            let (db, name, mut j) = if toks.get(i + 1).is_some_and(|n| n.is_punct(b'.'))
                && toks.get(i + 2).is_some_and(Tok::is_ident)
            {
                (Some(t.ident(sql)), toks[i + 2].ident(sql), i + 3)
            } else {
                (None, t.ident(sql), i + 1)
            };
            let db_ok = match &db {
                Some(d) => d.eq_ignore_ascii_case(schema),
                None => default_db.is_some_and(|d| d.eq_ignore_ascii_case(schema)),
            };
            if db_ok && name.eq_ignore_ascii_case(table) {
                let mut end = toks[j - 1].end;
                // PARTITION (p0, p1)
                if toks.get(j).is_some_and(|n| n.is_kw(sql, "PARTITION"))
                    && toks.get(j + 1).is_some_and(|n| n.is_punct(b'('))
                {
                    let mut k = j + 2;
                    while k < toks.len() && !toks[k].is_punct(b')') {
                        k += 1;
                    }
                    if k < toks.len() {
                        end = toks[k].end;
                        j = k + 1;
                    }
                }
                // [AS] alias
                if toks.get(j).is_some_and(|n| n.is_kw(sql, "AS"))
                    && toks.get(j + 1).is_some_and(Tok::is_ident)
                {
                    end = toks[j + 1].end;
                    j += 2;
                } else if toks.get(j).is_some_and(|n| {
                    n.kind == TokKind::Quoted
                        || (n.kind == TokKind::Word && !NOT_ALIAS.iter().any(|k| n.is_kw(sql, k)))
                }) {
                    end = toks[j].end;
                    j += 1;
                }
                out.push(end);
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn pk() -> Vec<String> {
        vec!["id".into()]
    }

    #[test]
    fn binds_scalars_lists_and_signs() {
        let text = "SELECT o.id FROM shop.orders o WHERE o.customer_id = ? AND o.status IN (...) \
                    AND o.note = ? ORDER BY o.created_at DESC LIMIT ?";
        let v = vec![
            SampleValue::Int(-42),
            SampleValue::List(vec![
                SampleValue::Str("open".into()),
                SampleValue::Str("ho'ld".into()),
            ]),
            SampleValue::Str("a\\b".into()),
            SampleValue::Int(20),
        ];
        let s = bind_literals(text, &v, false).unwrap();
        assert_eq!(
            s,
            "SELECT o.id FROM shop.orders o WHERE o.customer_id = -42 AND o.status IN ('open', \
             'ho''ld') AND o.note = 'a\\\\b' ORDER BY o.created_at DESC LIMIT 20"
        );
        // NO_BACKSLASH_ESCAPES: backslashes are literal.
        let s = bind_literals("SELECT ?", &[SampleValue::Str("a\\b".into())], true).unwrap();
        assert_eq!(s, "SELECT 'a\\b'");
    }

    #[test]
    fn raw_literals_replay_as_written() {
        for ok in [
            "'open'",
            "_utf8mb4'x'",
            "N'x'",
            "-42",
            "+1.5e3",
            "X'0A'",
            "0x0A",
            "DATE '2026-06-01'",
            "NULL",
            "true",
            "\"q\"",
        ] {
            assert!(is_single_literal(ok), "{ok}");
        }
        for bad in [
            "1; DROP TABLE t",
            "'a' OR 1=1",
            "",
            "a",
            "(1)",
            "1 1",
            "-- x",
        ] {
            assert!(!is_single_literal(bad), "{bad}");
        }
        let v = [
            SampleValue::Raw("_utf8mb4'open'".into()),
            SampleValue::List(vec![
                SampleValue::Raw("1".into()),
                SampleValue::Raw("-2".into()),
            ]),
        ];
        assert_eq!(
            bind_literals("SELECT 1 FROM t WHERE s = ? AND a IN (...)", &v, false).unwrap(),
            "SELECT 1 FROM t WHERE s = _utf8mb4'open' AND a IN (1, -2)"
        );
        assert!(matches!(
            bind_literals("SELECT ?", &[SampleValue::Raw("1 OR 1".into())], false),
            Err(Unreplayable::Binding(_))
        ));
        assert_eq!(SampleValue::Raw("1 OR 1".into()).render(false), "'1 OR 1'");
    }

    #[test]
    fn binding_counts_must_match() {
        assert!(matches!(
            bind_literals("SELECT ? + ?", &[SampleValue::Int(1)], false),
            Err(Unreplayable::Binding(_))
        ));
        assert!(matches!(
            bind_literals("SELECT 1", &[SampleValue::Int(1)], false),
            Err(Unreplayable::Binding(_))
        ));
        assert!(matches!(
            bind_literals(
                "SELECT 1 FROM t WHERE a IN (...)",
                &[SampleValue::Int(1)],
                false
            ),
            Err(Unreplayable::Binding(_))
        ));
        // A `?` inside a string or comment is not a placeholder.
        let s = bind_literals("SELECT '?' /* ? */, ?", &[SampleValue::Int(1)], false).unwrap();
        assert_eq!(s, "SELECT '?' /* ? */, 1");
    }

    #[test]
    fn renders_typed_literals() {
        let v = SampleValue::List(vec![
            SampleValue::Null,
            SampleValue::Bytes(vec![0xde, 0xad]),
            SampleValue::Date("2026-06-01".into()),
            SampleValue::Timestamp("2026-06-01 10:00:00".into()),
            SampleValue::Bool(true),
            SampleValue::Number("1.50".into()),
            SampleValue::Number("1; DROP TABLE t".into()),
            SampleValue::UInt(u64::MAX),
            SampleValue::List(vec![SampleValue::Int(1), SampleValue::Int(2)]),
        ]);
        assert_eq!(
            v.render(false),
            "(NULL, X'DEAD', DATE '2026-06-01', TIMESTAMP '2026-06-01 10:00:00', TRUE, 1.50, \
             '1; DROP TABLE t', 18446744073709551615, (1, 2))"
        );
    }

    #[test]
    fn selects_stay_and_lose_locking_clauses() {
        let f = select_form(
            "SELECT id FROM t WHERE a = 1 FOR UPDATE SKIP LOCKED;",
            &pk(),
        )
        .unwrap();
        assert_eq!(f.sql, "SELECT id FROM t WHERE a = 1");
        assert_eq!(f.from_kind, "SELECT");
        let f = select_form("select id from t where a = 1 lock in share mode", &pk()).unwrap();
        assert_eq!(f.sql, "select id from t where a = 1");
        let f = select_form("WITH x AS (SELECT 1) SELECT * FROM x", &pk()).unwrap();
        assert_eq!(f.sql, "WITH x AS (SELECT 1) SELECT * FROM x");
        assert_eq!(
            select_form("SELECT a INTO @x FROM t", &pk()),
            Err(Unreplayable::SelectInto)
        );
        let f = select_form("(SELECT 1 FROM t) UNION (SELECT 2 FROM t)", &pk()).unwrap();
        assert_eq!(f.sql, "(SELECT 1 FROM t) UNION (SELECT 2 FROM t)");
    }

    #[test]
    fn updates_become_selects_of_the_primary_key() {
        let f = select_form(
            "UPDATE LOW_PRIORITY orders AS o SET o.status = 'x', total = (SELECT 1 FROM y WHERE \
             y.a = o.a) WHERE o.customer_id = 7 ORDER BY o.created_at LIMIT 10",
            &pk(),
        )
        .unwrap();
        assert_eq!(
            f.sql,
            "SELECT `id` FROM orders AS o WHERE o.customer_id = 7 ORDER BY o.created_at LIMIT 10"
        );
        let f = select_form("UPDATE t SET a = 1", &pk()).unwrap();
        assert_eq!(f.sql, "SELECT `id` FROM t");
        let f = select_form(
            "UPDATE a JOIN b ON a.id = b.a_id SET a.x = b.y WHERE b.z = 3",
            &pk(),
        )
        .unwrap();
        assert_eq!(
            f.sql,
            "SELECT 1 FROM a JOIN b ON a.id = b.a_id WHERE b.z = 3"
        );
    }

    #[test]
    fn deletes_become_selects() {
        let f = select_form(
            "DELETE QUICK FROM s.t WHERE a < 5 ORDER BY id LIMIT 100 RETURNING id",
            &pk(),
        )
        .unwrap();
        assert_eq!(
            f.sql,
            "SELECT `id` FROM s.t WHERE a < 5 ORDER BY id LIMIT 100"
        );
        let f = select_form(
            "DELETE t1 FROM t1 JOIN t2 ON t1.k = t2.k WHERE t2.v = 1",
            &pk(),
        )
        .unwrap();
        assert_eq!(
            f.sql,
            "SELECT 1 FROM t1 JOIN t2 ON t1.k = t2.k WHERE t2.v = 1"
        );
        let f = select_form(
            "DELETE FROM t1 USING t1 JOIN t2 ON t1.k = t2.k WHERE t2.v = 1",
            &pk(),
        )
        .unwrap();
        assert_eq!(
            f.sql,
            "SELECT 1 FROM t1 JOIN t2 ON t1.k = t2.k WHERE t2.v = 1"
        );
    }

    #[test]
    fn insert_select_keeps_only_the_select() {
        let f = select_form(
            "INSERT INTO t (a, b) SELECT x, y FROM u WHERE z = 1 ON DUPLICATE KEY UPDATE b = 2",
            &pk(),
        )
        .unwrap();
        assert_eq!(f.sql, "SELECT x, y FROM u WHERE z = 1");
        assert_eq!(f.from_kind, "INSERT");
        assert!(matches!(
            select_form("INSERT INTO t VALUES (1, 2)", &pk()),
            Err(Unreplayable::NoRead(_))
        ));
        assert!(matches!(
            select_form("INSERT INTO t SET a = 1", &pk()),
            Err(Unreplayable::NoRead(_))
        ));
        assert!(matches!(
            select_form("CREATE TABLE x (a INT)", &pk()),
            Err(Unreplayable::NoRead(_))
        ));
    }

    #[test]
    fn ignore_index_goes_after_the_alias() {
        let h = |s: &str, db: Option<&str>| with_ignore_index(s, "s1", "t", "sprig_auto_1", db);
        assert_eq!(
            h("SELECT o.id FROM s1.t AS o WHERE o.a = 1", None).unwrap(),
            "SELECT o.id FROM s1.t AS o IGNORE INDEX (`sprig_auto_1`) WHERE o.a = 1"
        );
        assert_eq!(
            h("SELECT id FROM `s1`.`t` o JOIN u ON u.x = o.id", None).unwrap(),
            "SELECT id FROM `s1`.`t` o IGNORE INDEX (`sprig_auto_1`) JOIN u ON u.x = o.id"
        );
        // Unqualified: only with the right default database.
        assert_eq!(
            h("SELECT id FROM t WHERE a = 1", Some("s1")).unwrap(),
            "SELECT id FROM t IGNORE INDEX (`sprig_auto_1`) WHERE a = 1"
        );
        assert_eq!(
            h("SELECT id FROM t WHERE a = 1", Some("s2")),
            Err(Unreplayable::TableNotFound)
        );
        // Self-join, comma join, subquery, and an existing hint.
        assert_eq!(
            h(
                "SELECT 1 FROM t a, t b USE INDEX (x) WHERE a.id IN (SELECT id FROM t WHERE c = 2)",
                Some("s1")
            )
            .unwrap(),
            "SELECT 1 FROM t a IGNORE INDEX (`sprig_auto_1`), t b IGNORE INDEX (`sprig_auto_1`) \
             USE INDEX (x) WHERE a.id IN (SELECT id FROM t IGNORE INDEX (`sprig_auto_1`) WHERE c \
             = 2)"
        );
        // A column named like the table isn't a reference.
        assert_eq!(
            h("SELECT t FROM u WHERE t = 1", Some("s1")),
            Err(Unreplayable::TableNotFound)
        );
        // Nested join list, LEFT JOIN, partition clause.
        assert_eq!(
            h(
                "SELECT 1 FROM (u JOIN t PARTITION (p0) ON u.a = t.a) LEFT JOIN v ON 1",
                Some("s1")
            )
            .unwrap(),
            "SELECT 1 FROM (u JOIN t PARTITION (p0) IGNORE INDEX (`sprig_auto_1`) ON u.a = t.a) \
             LEFT JOIN v ON 1"
        );
        // Derived table aliases named t are not the table.
        assert_eq!(
            h("SELECT 1 FROM (SELECT 1 x) t", Some("s1")),
            Err(Unreplayable::TableNotFound)
        );
        // Union parts.
        assert_eq!(
            h(
                "SELECT id FROM t WHERE a = 5 UNION SELECT id FROM t WHERE c = 7",
                Some("s1")
            )
            .unwrap(),
            "SELECT id FROM t IGNORE INDEX (`sprig_auto_1`) WHERE a = 5 UNION SELECT id FROM t \
             IGNORE INDEX (`sprig_auto_1`) WHERE c = 7"
        );
    }

    #[test]
    fn lexer_handles_comments_numbers_and_quotes() {
        let toks =
            lex("SELECT 1.5e3, .5, 0x1F, `a``b`, 'it''s', \"q\" -- c\n # d\n /* e */ x").unwrap();
        let kinds: Vec<_> = toks.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                TokKind::Word,
                TokKind::Num,
                TokKind::Punct(b','),
                TokKind::Num,
                TokKind::Punct(b','),
                TokKind::Num,
                TokKind::Punct(b','),
                TokKind::Quoted,
                TokKind::Punct(b','),
                TokKind::Str,
                TokKind::Punct(b','),
                TokKind::Str,
                TokKind::Word,
            ]
        );
        let src = "`a``b`";
        assert_eq!(lex(src).unwrap()[0].ident(src), "a`b");
        assert!(lex("SELECT 'x").is_err());
        assert!(lex("SELECT /* x").is_err());
    }
}
