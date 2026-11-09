//! R3's write forms (chapter 17, "`INSERT … SELECT` and `LOAD DATA` (R3)"):
//! statements that write a sharded table from rows the router can't route
//! as one statement.
//!
//! ```text
//! INSERT [IGNORE] INTO t [(cols)] SELECT … [ON DUPLICATE KEY UPDATE …]
//! REPLACE INTO t [(cols)] SELECT …
//!   1. open: BEGIN (autocommit) or SAVEPOINT (inside a transaction)
//!   2. the SELECT runs through the router like any read (split, gathered
//!      or relayed), its rows kept in the gateway
//!   3. each row becomes a tuple of literals by its column definition, and
//!      batches of `INSERT … VALUES …` (same head and tail) run through the
//!      router's INSERT planning: key routing, ID blocks, lookups, foreign keys
//!   4. COMMIT or RELEASE; on any error ROLLBACK or ROLLBACK TO, and the
//!      error goes to the client
//!
//! LOAD DATA LOCAL INFILE 'f' [REPLACE | IGNORE] INTO TABLE t … into a
//! sharded table:
//!   1. the client's file is read whole (the statement's FIELDS and LINES
//!      options, IGNORE n LINES, a column list)
//!   2. its rows become INSERT batches as above (LOCAL without REPLACE
//!      means IGNORE, as in MariaDB)
//! ```
//!
//! The statement is atomic as MariaDB's is: one transaction, or one
//! savepoint inside the client's. The rows are read before the first one
//! is written, as MariaDB materializes `INSERT … SELECT` from its own
//! target. Values keep their bytes: numbers and temporal values as their
//! text, strings without a backslash as quoted text, other strings and
//! binary values as `X'…'`.

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::column_type as ct;
use sprig_router::{Reason, Rejection};
use sprig_sql::names::{NameMap, ResultMapper};

use super::super::PassSession;
use super::super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// Rows per `INSERT` batch.
const BATCH_ROWS: usize = 500;
/// Text bytes per `INSERT` batch.
const BATCH_BYTES: usize = 512 * 1024;
/// The largest file `LOAD DATA LOCAL` reads into a sharded table.
const MAX_FILE_BYTES: usize = 256 * 1024 * 1024;

// ---- a small scanner ---------------------------------------------------------

/// One token of a statement: a word (upper-cased), a quoted string, a
/// backticked name, or one punctuation character, with its byte range and
/// parenthesis depth.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Str(Vec<u8>),
    Ident(String),
    Punct(u8),
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    start: usize,
    end: usize,
    depth: u32,
}

/// The content of a quoted literal starting at `q` (the quote), with
/// MariaDB's backslash escapes and doubled quotes, and the offset after it.
fn quoted(b: &[u8], q: usize) -> (Vec<u8>, usize) {
    let quote = b[q];
    let mut out = Vec::new();
    let mut i = q + 1;
    while i < b.len() {
        let c = b[i];
        if c == b'\\' && i + 1 < b.len() && quote != b'`' {
            out.push(match b[i + 1] {
                b'0' => 0,
                b'b' => 8,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'Z' => 26,
                x => x,
            });
            i += 2;
            continue;
        }
        if c == quote {
            if b.get(i + 1) == Some(&quote) {
                out.push(quote);
                i += 2;
                continue;
            }
            return (out, i + 1);
        }
        out.push(c);
        i += 1;
    }
    (out, i)
}

fn scan(text: &str) -> Vec<Token> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut depth = 0u32;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'#' || (c == b'-' && b.get(i + 1) == Some(&b'-')) {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
        } else if c == b'\'' || c == b'"' {
            let (s, end) = quoted(b, i);
            out.push(Token {
                tok: Tok::Str(s),
                start: i,
                end,
                depth,
            });
            i = end;
        } else if c == b'`' {
            let (s, end) = quoted(b, i);
            out.push(Token {
                tok: Tok::Ident(String::from_utf8_lossy(&s).into_owned()),
                start: i,
                end,
                depth,
            });
            i = end;
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c == b'@' || c >= 0x80 {
            let s = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric()
                    || b[i] == b'_'
                    || b[i] == b'$'
                    || b[i] == b'@'
                    || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Token {
                tok: Tok::Word(text[s..i].to_ascii_uppercase()),
                start: s,
                end: i,
                depth,
            });
        } else {
            if c == b')' {
                depth = depth.saturating_sub(1);
            }
            out.push(Token {
                tok: Tok::Punct(c),
                start: i,
                end: i + 1,
                depth,
            });
            if c == b'(' {
                depth += 1;
            }
            i += 1;
        }
    }
    out
}

fn is_word(t: &Token, w: &str) -> bool {
    matches!(&t.tok, Tok::Word(x) if x == w)
}

// ---- INSERT … SELECT -------------------------------------------------------

/// An `INSERT … SELECT` cut into the part before the query, the query, and
/// the `ON DUPLICATE KEY UPDATE` part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InsertSelect {
    /// `INSERT [IGNORE] INTO t (cols)`.
    pub head: String,
    /// The query.
    pub select: String,
    /// ` ON DUPLICATE KEY UPDATE …`, or empty.
    pub tail: String,
}

/// `text` as an `INSERT … SELECT` or `REPLACE … SELECT`; `None` for any
/// other statement, or one with `RETURNING`.
pub(super) fn insert_select_parts(text: &str) -> Option<InsertSelect> {
    let toks = scan(text);
    let first = toks.first()?;
    if !(is_word(first, "INSERT") || is_word(first, "REPLACE")) {
        return None;
    }
    // The query: the first SELECT or WITH at depth 0, or right inside a
    // parenthesis at depth 0 (`INSERT INTO t (SELECT …)`).
    let mut at = None;
    for (i, t) in toks.iter().enumerate() {
        if t.depth == 0 && (is_word(t, "VALUES") || is_word(t, "VALUE") || is_word(t, "SET")) {
            return None;
        }
        if is_word(t, "SELECT") || is_word(t, "WITH") {
            if t.depth == 0 {
                at = Some(t.start);
            } else if t.depth == 1
                && i > 0
                && toks[i - 1].tok == Tok::Punct(b'(')
                && toks[i - 1].depth == 0
            {
                at = Some(toks[i - 1].start);
            }
            break;
        }
    }
    let start = at?;
    let mut end = text.len();
    for (i, t) in toks.iter().enumerate() {
        if t.start < start || t.depth != 0 {
            continue;
        }
        if is_word(t, "RETURNING") {
            return None;
        }
        if is_word(t, "ON")
            && toks.get(i + 1).is_some_and(|x| is_word(x, "DUPLICATE"))
            && toks.get(i + 2).is_some_and(|x| is_word(x, "KEY"))
            && toks.get(i + 3).is_some_and(|x| is_word(x, "UPDATE"))
        {
            end = t.start;
            break;
        }
    }
    let select = text[start..end].trim();
    let select = select.strip_suffix(';').unwrap_or(select).trim();
    if select.is_empty() {
        return None;
    }
    Some(InsertSelect {
        head: text[..start].trim_end().to_owned(),
        select: select.to_owned(),
        tail: if end < text.len() {
            format!(" {}", text[end..].trim().trim_end_matches(';'))
        } else {
            String::new()
        },
    })
}

// ---- LOAD DATA ---------------------------------------------------------------

/// What `LOAD DATA … INTO TABLE` does with duplicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Dup {
    /// Fail on the first (server files only).
    Error,
    /// Skip them.
    Ignore,
    /// Replace the old row.
    Replace,
}

/// A parsed `LOAD DATA` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LoadData {
    /// `LOCAL`: the client sends the file.
    pub local: bool,
    /// The file name as the client will be asked for it.
    pub file: Vec<u8>,
    /// Duplicates.
    pub dup: Dup,
    /// The table as the statement writes it (rewritten names).
    pub table_text: String,
    /// Its schema, when qualified.
    pub schema: Option<String>,
    /// Its name.
    pub table: String,
    /// `FIELDS TERMINATED BY` (default tab).
    pub field_term: Vec<u8>,
    /// `ENCLOSED BY`.
    pub enclosed: Option<u8>,
    /// `ESCAPED BY` (default backslash; empty: none).
    pub escaped: Option<u8>,
    /// `LINES STARTING BY`.
    pub line_start: Vec<u8>,
    /// `LINES TERMINATED BY` (default newline).
    pub line_term: Vec<u8>,
    /// `IGNORE n LINES`.
    pub skip: u64,
    /// The column list (names only).
    pub columns: Option<Vec<String>>,
    /// Something this path doesn't load (`@var`s, `SET`, `CHARACTER SET`
    /// other than utf8mb4/binary, `PARTITION`, fixed-width fields): why.
    pub unsupported: Option<String>,
}

fn name_of(t: &Token) -> Option<String> {
    match &t.tok {
        Tok::Word(_) => None,
        Tok::Ident(s) => Some(s.clone()),
        _ => None,
    }
}

/// `text` as a `LOAD DATA … INTO TABLE` statement; `None` for any other.
#[allow(clippy::too_many_lines)] // the grammar, clause by clause
pub(super) fn load_data_parse(text: &str) -> Option<LoadData> {
    let toks = scan(text);
    if !(toks.first().is_some_and(|t| is_word(t, "LOAD"))
        && toks.get(1).is_some_and(|t| is_word(t, "DATA")))
    {
        return None;
    }
    let mut i = 2;
    let w = |i: usize, s: &str| toks.get(i).is_some_and(|t| is_word(t, s));
    while w(i, "LOW_PRIORITY") || w(i, "CONCURRENT") {
        i += 1;
    }
    let local = w(i, "LOCAL");
    if local {
        i += 1;
    }
    if !w(i, "INFILE") {
        return None;
    }
    i += 1;
    let Some(Tok::Str(file)) = toks.get(i).map(|t| t.tok.clone()) else {
        return None;
    };
    i += 1;
    let mut dup = if local { Dup::Ignore } else { Dup::Error };
    if w(i, "REPLACE") {
        dup = Dup::Replace;
        i += 1;
    } else if w(i, "IGNORE") {
        dup = Dup::Ignore;
        i += 1;
    }
    if !(w(i, "INTO") && w(i + 1, "TABLE")) {
        return None;
    }
    i += 2;
    // The table: `t`, `s.t`, backticked or not.
    let part = |t: &Token| -> Option<String> {
        match &t.tok {
            Tok::Word(_) => Some(text[t.start..t.end].to_owned()),
            Tok::Ident(_) => name_of(t),
            _ => None,
        }
    };
    let first = part(toks.get(i)?)?;
    let tstart = toks[i].start;
    let mut tend = toks[i].end;
    let (schema, table) = if toks.get(i + 1).is_some_and(|t| t.tok == Tok::Punct(b'.')) {
        let second = part(toks.get(i + 2)?)?;
        tend = toks[i + 2].end;
        i += 3;
        (Some(first), second)
    } else {
        i += 1;
        (None, first)
    };
    let mut ld = LoadData {
        local,
        file,
        dup,
        table_text: text[tstart..tend].to_owned(),
        schema,
        table,
        field_term: b"\t".to_vec(),
        enclosed: None,
        escaped: Some(b'\\'),
        line_start: Vec::new(),
        line_term: b"\n".to_vec(),
        skip: 0,
        columns: None,
        unsupported: None,
    };
    let str_at = |i: usize| match toks.get(i).map(|t| &t.tok) {
        Some(Tok::Str(s)) => Some(s.clone()),
        _ => None,
    };
    while i < toks.len() {
        if w(i, "PARTITION") {
            ld.unsupported = Some("PARTITION".into());
            break;
        }
        if w(i, "CHARACTER") && w(i + 1, "SET") || w(i, "CHARSET") {
            i += if w(i, "CHARSET") { 1 } else { 2 };
            let cs = toks
                .get(i)
                .map(|t| {
                    text[t.start..t.end]
                        .trim_matches(['\'', '"', '`'])
                        .to_ascii_lowercase()
                })
                .unwrap_or_default();
            if !matches!(
                cs.as_str(),
                "utf8mb4" | "utf8" | "utf8mb3" | "binary" | "ascii"
            ) {
                ld.unsupported = Some(format!("CHARACTER SET {cs}"));
            }
            i += 1;
            continue;
        }
        if w(i, "FIELDS") || w(i, "COLUMNS") {
            i += 1;
            loop {
                if w(i, "TERMINATED") && w(i + 1, "BY") {
                    ld.field_term = str_at(i + 2)?;
                    i += 3;
                } else if w(i, "OPTIONALLY") && w(i + 1, "ENCLOSED") && w(i + 2, "BY") {
                    ld.enclosed = str_at(i + 3)?.first().copied();
                    i += 4;
                } else if w(i, "ENCLOSED") && w(i + 1, "BY") {
                    ld.enclosed = str_at(i + 2)?.first().copied();
                    i += 3;
                } else if w(i, "ESCAPED") && w(i + 1, "BY") {
                    ld.escaped = str_at(i + 2)?.first().copied();
                    i += 3;
                } else {
                    break;
                }
            }
            continue;
        }
        if w(i, "LINES") || w(i, "ROWS") && !w(i - 1, "IGNORE") {
            i += 1;
            loop {
                if w(i, "STARTING") && w(i + 1, "BY") {
                    ld.line_start = str_at(i + 2)?;
                    i += 3;
                } else if w(i, "TERMINATED") && w(i + 1, "BY") {
                    ld.line_term = str_at(i + 2)?;
                    i += 3;
                } else {
                    break;
                }
            }
            continue;
        }
        if w(i, "IGNORE") {
            let n = toks
                .get(i + 1)
                .and_then(|t| text[t.start..t.end].parse().ok())?;
            ld.skip = n;
            i += 3; // IGNORE n LINES|ROWS
            continue;
        }
        if toks[i].tok == Tok::Punct(b'(') {
            let mut cols = Vec::new();
            i += 1;
            while i < toks.len() && toks[i].tok != Tok::Punct(b')') {
                match &toks[i].tok {
                    Tok::Punct(b',') => {}
                    Tok::Word(x) if x.starts_with('@') => {
                        ld.unsupported = Some("user variables in the column list".into());
                    }
                    Tok::Word(_) => cols.push(text[toks[i].start..toks[i].end].to_owned()),
                    Tok::Ident(x) => cols.push(x.clone()),
                    _ => ld.unsupported = Some("the column list".into()),
                }
                i += 1;
            }
            i += 1;
            ld.columns = Some(cols);
            continue;
        }
        if w(i, "SET") {
            ld.unsupported = Some("SET".into());
            break;
        }
        if toks[i].tok == Tok::Punct(b';') {
            break;
        }
        ld.unsupported = Some(format!("`{}`", &text[toks[i].start..toks[i].end]));
        break;
    }
    if ld.field_term.is_empty() || ld.line_term.is_empty() {
        ld.unsupported = Some("fixed-width fields".into());
    }
    Some(ld)
}

fn at(data: &[u8], pos: usize, pat: &[u8]) -> bool {
    !pat.is_empty() && data.get(pos..pos + pat.len()) == Some(pat)
}

fn unescape(c: u8) -> u8 {
    match c {
        b'0' => 0,
        b'b' => 8,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'Z' => 26,
        x => x,
    }
}

/// The file's rows under `ld`'s options: one value per field, `None` for
/// `NULL` (`\N`, or the bare word `NULL` when fields are enclosed).
pub(super) fn split_rows(data: &[u8], ld: &LoadData) -> Vec<Vec<Option<Vec<u8>>>> {
    let (ft, lt) = (ld.field_term.as_slice(), ld.line_term.as_slice());
    let mut rows = Vec::new();
    let mut pos = 0;
    let mut skip = ld.skip;
    while pos < data.len() {
        if !ld.line_start.is_empty() {
            let Some(k) = data[pos..]
                .windows(ld.line_start.len())
                .position(|x| x == ld.line_start.as_slice())
            else {
                break;
            };
            pos += k + ld.line_start.len();
        }
        let mut fields = Vec::new();
        loop {
            let mut val = Vec::new();
            let mut null = false;
            let mut quoted = false;
            if ld.enclosed.is_some_and(|e| data.get(pos) == Some(&e)) {
                let e = ld.enclosed.unwrap_or(b'"');
                quoted = true;
                pos += 1;
                while pos < data.len() {
                    let c = data[pos];
                    if ld.escaped == Some(c) && pos + 1 < data.len() {
                        val.push(unescape(data[pos + 1]));
                        pos += 2;
                        continue;
                    }
                    if c == e {
                        if data.get(pos + 1) == Some(&e) {
                            val.push(e);
                            pos += 2;
                            continue;
                        }
                        if pos + 1 >= data.len() || at(data, pos + 1, ft) || at(data, pos + 1, lt) {
                            pos += 1;
                            break;
                        }
                    }
                    val.push(c);
                    pos += 1;
                }
            } else {
                while pos < data.len() && !at(data, pos, ft) && !at(data, pos, lt) {
                    let c = data[pos];
                    if ld.escaped == Some(c) && pos + 1 < data.len() {
                        let n = data[pos + 1];
                        if n == b'N' && val.is_empty() {
                            let after = pos + 2;
                            if after >= data.len() || at(data, after, ft) || at(data, after, lt) {
                                null = true;
                                pos = after;
                                break;
                            }
                        }
                        val.push(unescape(n));
                        pos += 2;
                        continue;
                    }
                    val.push(c);
                    pos += 1;
                }
            }
            let v = if null || (!quoted && ld.enclosed.is_some() && val == b"NULL") {
                None
            } else {
                Some(val)
            };
            fields.push(v);
            if at(data, pos, ft) {
                pos += ft.len();
                continue;
            }
            if at(data, pos, lt) {
                pos += lt.len();
            }
            break;
        }
        if skip > 0 {
            skip -= 1;
            continue;
        }
        rows.push(fields);
    }
    rows
}

// ---- values as literals ------------------------------------------------------

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(3 + 2 * b.len());
    s.push_str("X'");
    for x in b {
        s.push_str(&format!("{x:02X}"));
    }
    s.push('\'');
    s
}

/// A string as a literal: quoted text when it holds no backslash, no NUL
/// and is UTF-8 (quotes doubled, so it reads the same with or without
/// `NO_BACKSLASH_ESCAPES`), else its bytes as `X'…'`.
pub(super) fn str_literal(v: &[u8]) -> String {
    match std::str::from_utf8(v) {
        Ok(s) if !s.contains(['\\', '\0']) => format!("'{}'", s.replace('\'', "''")),
        _ => hex(v),
    }
}

fn numeric_text(v: &[u8]) -> bool {
    !v.is_empty()
        && v.iter()
            .all(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E'))
}

/// A result value of column `c` as a literal the same column type takes
/// back unchanged.
pub(super) fn literal(c: &ColumnDef, v: Option<&[u8]>) -> String {
    let Some(v) = v else {
        return "NULL".into();
    };
    match c.col_type {
        ct::DECIMAL
        | ct::NEWDECIMAL
        | ct::TINY
        | ct::SHORT
        | ct::LONG
        | ct::LONGLONG
        | ct::INT24
        | ct::YEAR
        | ct::FLOAT
        | ct::DOUBLE
            if numeric_text(v) =>
        {
            String::from_utf8_lossy(v).into_owned()
        }
        ct::BIT => hex(v),
        // Binary strings, blobs and geometry keep their bytes.
        _ if c.charset == 63
            && !matches!(
                c.col_type,
                ct::DATE
                    | ct::NEWDATE
                    | ct::TIME
                    | ct::TIME2
                    | ct::DATETIME
                    | ct::DATETIME2
                    | ct::TIMESTAMP
                    | ct::TIMESTAMP2
            ) =>
        {
            hex(v)
        }
        _ => str_literal(v),
    }
}

// ---- a sink that keeps one statement's answer -----------------------------

/// One internal statement's answer.
struct Capture {
    wire: Wire,
    columns: Vec<ColumnDef>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    ok: Option<OkPacket>,
    error: Option<SprigError>,
    done: bool,
}

impl Capture {
    fn new(wire: Wire) -> Self {
        Self {
            wire,
            columns: Vec::new(),
            rows: Vec::new(),
            ok: None,
            error: None,
            done: false,
        }
    }
}

#[async_trait::async_trait]
impl ResultSink for Capture {
    fn wire(&self) -> Wire {
        self.wire
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.to_vec();
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match values {
            RowValues::Text(v) => self
                .rows
                .push(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect()),
            RowValues::Binary(_) => {
                return Err(SprigError::internal(
                    "a binary row in an INSERT … SELECT's query",
                ));
            }
        }
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.len())?
            .into_iter()
            .map(|c| c.map(<[u8]>::to_vec))
            .collect();
        self.rows.push(r);
        Ok(())
    }

    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.ok = Some(ok);
        self.done = true;
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }

    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Ok(())
    }

    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(SprigError::internal(
            "LOAD DATA LOCAL INFILE inside an INSERT … SELECT",
        ))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.done
    }
}

/// What the batches added up to.
#[derive(Debug, Default)]
struct Totals {
    affected: u64,
    warnings: u64,
    last_insert_id: u64,
}

impl PassSession {
    /// Runs `sql` through the router into a capture.
    async fn capture(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        mapper: &ResultMapper,
        map: &dyn NameMap,
        wire: Wire,
    ) -> Result<Capture> {
        let mut cap = Capture::new(wire);
        Box::pin(self.routed(
            cx,
            sql.as_bytes().to_vec(),
            sql.as_bytes(),
            false,
            RowFormat::Text,
            mapper.clone(),
            map,
            &mut cap,
        ))
        .await?;
        Ok(cap)
    }

    /// Opens the statement's transaction (or savepoint); returns how to end
    /// it: `(commit, rollback)`.
    async fn write_form_open(
        &mut self,
        cx: &mut ExecCtx<'_>,
        mapper: &ResultMapper,
        map: &dyn NameMap,
        wire: Wire,
    ) -> Result<std::result::Result<(&'static str, &'static str), SprigError>> {
        let (open, commit, rollback) = if self.router.in_txn() || self.autocommit_off() {
            (
                "SAVEPOINT __sprig_write_form",
                "RELEASE SAVEPOINT __sprig_write_form",
                "ROLLBACK TO SAVEPOINT __sprig_write_form",
            )
        } else {
            ("BEGIN", "COMMIT", "ROLLBACK")
        };
        let c = self.capture(cx, open, mapper, map, wire).await?;
        Ok(match c.error {
            Some(e) => Err(e),
            None => Ok((commit, rollback)),
        })
    }

    /// Inserts `tuples` (rendered `(…)` rows) as `head VALUES … tail`
    /// batches; stops at the first error.
    #[allow(clippy::too_many_arguments)]
    async fn write_form_rows(
        &mut self,
        cx: &mut ExecCtx<'_>,
        head: &str,
        tail: &str,
        tuples: &[String],
        mapper: &ResultMapper,
        map: &dyn NameMap,
        wire: Wire,
    ) -> Result<std::result::Result<Totals, SprigError>> {
        let mut t = Totals::default();
        let mut i = 0;
        while i < tuples.len() {
            let mut sql = format!("{head} VALUES ");
            let mut n = 0;
            while i < tuples.len() && n < BATCH_ROWS && (n == 0 || sql.len() < BATCH_BYTES) {
                if n > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&tuples[i]);
                i += 1;
                n += 1;
            }
            sql.push_str(tail);
            let c = self.capture(cx, &sql, mapper, map, wire).await?;
            if let Some(e) = c.error {
                return Ok(Err(e));
            }
            if let Some(ok) = c.ok {
                t.affected += ok.affected_rows;
                t.warnings += u64::from(ok.warnings);
                if t.last_insert_id == 0 {
                    t.last_insert_id = ok.last_insert_id;
                }
            }
        }
        Ok(Ok(t))
    }

    /// Ends the statement: commits (or releases) on success, rolls back on
    /// an error, and answers the client.
    #[allow(clippy::too_many_arguments)]
    async fn write_form_close(
        &mut self,
        cx: &mut ExecCtx<'_>,
        ends: (&'static str, &'static str),
        result: std::result::Result<(Totals, String), SprigError>,
        more: bool,
        mapper: &ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let wire = out.wire();
        match result {
            Ok((t, info)) => {
                let c = self.capture(cx, ends.0, mapper, map, wire).await?;
                if let Some(e) = c.error {
                    let _ = self.capture(cx, ends.1, mapper, map, wire).await;
                    out.error(&e).await?;
                    return Ok(Step {
                        errored: true,
                        status: None,
                    });
                }
                let mut status = self.status();
                if more {
                    status |= sprig_proto_mysql::StatusFlags::MORE_RESULTS_EXISTS;
                }
                let mut p = OkPacket::new(status);
                p.affected_rows = t.affected;
                p.last_insert_id = t.last_insert_id;
                p.warnings = u16::try_from(t.warnings).unwrap_or(u16::MAX);
                p.info = Bytes::from(info);
                out.ok(p).await?;
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => {
                let _ = self.capture(cx, ends.1, mapper, map, wire).await;
                out.error(&e).await?;
                Ok(Step {
                    errored: true,
                    status: None,
                })
            }
        }
    }

    /// `INSERT … SELECT` or `REPLACE … SELECT` into (or from) sharded
    /// tables (module docs).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn insert_select(
        &mut self,
        cx: &mut ExecCtx<'_>,
        parts: InsertSelect,
        more: bool,
        mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let wire = out.wire();
        self.router.write_form = true;
        let r = async {
            let ends = match self.write_form_open(cx, &mapper, map, wire).await? {
                Ok(e) => e,
                Err(e) => {
                    out.error(&e).await?;
                    return Ok(Step {
                        errored: true,
                        status: None,
                    });
                }
            };
            let q = self.capture(cx, &parts.select, &mapper, map, wire).await?;
            let result = if let Some(e) = q.error {
                Err(e)
            } else {
                let tuples: Vec<String> = q
                    .rows
                    .iter()
                    .map(|r| {
                        let vals: Vec<String> = r
                            .iter()
                            .enumerate()
                            .map(|(i, v)| {
                                q.columns.get(i).map_or_else(
                                    || v.as_deref().map_or_else(|| "NULL".into(), str_literal),
                                    |c| literal(c, v.as_deref()),
                                )
                            })
                            .collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();
                let records = tuples.len() as u64;
                match self
                    .write_form_rows(cx, &parts.head, &parts.tail, &tuples, &mapper, map, wire)
                    .await?
                {
                    Ok(t) => {
                        let dups = if parts.tail.is_empty() {
                            records.saturating_sub(t.affected.min(records))
                        } else {
                            0
                        };
                        let info = format!(
                            "Records: {records}  Duplicates: {dups}  Warnings: {}",
                            t.warnings
                        );
                        Ok((t, info))
                    }
                    Err(e) => Err(e),
                }
            };
            self.write_form_close(cx, ends, result, more, &mapper, map, out)
                .await
        }
        .await;
        self.router.write_form = false;
        metrics::counter!("sprig_router_statements_total", "plan" => "insert_select").increment(1);
        r
    }

    /// `LOAD DATA LOCAL INFILE … INTO TABLE` a sharded table (module docs).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn load_data(
        &mut self,
        cx: &mut ExecCtx<'_>,
        ld: LoadData,
        more: bool,
        mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        if !ld.local {
            let e = Rejection::new(
                Reason::Unsupported,
                "LOAD DATA INFILE of a server file into a sharded table (use LOCAL)",
            );
            return self.fail(out, &e.error()).await;
        }
        if !(cx.proto.has(sprig_proto_mysql::Caps::LOCAL_FILES) && self.tenant_local_infile()) {
            return Err(super::super::local_infile_disabled(self.persona));
        }
        // The client sends its file now, whatever happens next.
        out.local_infile_request(&ld.file).await?;
        let mut data: Vec<u8> = Vec::new();
        let mut too_big = false;
        loop {
            let chunk = out.local_infile_read().await?;
            if chunk.is_empty() {
                break;
            }
            if data.len() + chunk.len() > MAX_FILE_BYTES {
                too_big = true;
            } else {
                data.extend_from_slice(&chunk);
            }
        }
        if let Some(why) = &ld.unsupported {
            let e = Rejection::new(
                Reason::Unsupported,
                format!("LOAD DATA into a sharded table with {why}"),
            );
            return self.fail(out, &e.error()).await;
        }
        if too_big {
            let e = Rejection::new(
                Reason::Unsupported,
                format!("LOAD DATA into a sharded table of a file over {MAX_FILE_BYTES} bytes"),
            );
            return self.fail(out, &e.error()).await;
        }
        let rows = split_rows(&data, &ld);
        let width = ld.columns.as_ref().map(Vec::len);
        let tuples: Vec<String> = rows
            .iter()
            .map(|r| {
                let n = width.unwrap_or(r.len());
                let vals: Vec<String> = (0..n)
                    .map(|i| match r.get(i) {
                        Some(Some(v)) => str_literal(v),
                        Some(None) => "NULL".into(),
                        None => "DEFAULT".into(),
                    })
                    .collect();
                format!("({})", vals.join(", "))
            })
            .collect();
        let verb = match ld.dup {
            Dup::Replace => "REPLACE INTO",
            Dup::Ignore => "INSERT IGNORE INTO",
            Dup::Error => "INSERT INTO",
        };
        let head = match &ld.columns {
            Some(c) => format!(
                "{verb} {} ({})",
                ld.table_text,
                c.iter()
                    .map(|x| format!("`{}`", x.replace('`', "``")))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => format!("{verb} {}", ld.table_text),
        };
        let wire = out.wire();
        self.router.write_form = true;
        let r = async {
            let ends = match self.write_form_open(cx, &mapper, map, wire).await? {
                Ok(e) => e,
                Err(e) => {
                    out.error(&e).await?;
                    return Ok(Step {
                        errored: true,
                        status: None,
                    });
                }
            };
            let result = match self
                .write_form_rows(cx, &head, "", &tuples, &mapper, map, wire)
                .await?
            {
                Ok(t) => {
                    let records = tuples.len() as u64;
                    let (deleted, skipped) = match ld.dup {
                        Dup::Replace => (t.affected.saturating_sub(records), 0),
                        _ => (0, records.saturating_sub(t.affected)),
                    };
                    let info = format!(
                        "Records: {records}  Deleted: {deleted}  Skipped: {skipped}  Warnings: {}",
                        t.warnings
                    );
                    Ok((t, info))
                }
                Err(e) => Err(e),
            };
            self.write_form_close(cx, ends, result, more, &mapper, map, out)
                .await
        }
        .await;
        self.router.write_form = false;
        metrics::counter!("sprig_router_statements_total", "plan" => "load_data").increment(1);
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_select_splits_at_the_query_and_the_upsert() {
        let p = insert_select_parts(
            "INSERT IGNORE INTO t (a, b) SELECT x, (SELECT 1) FROM u JOIN v ON v.k = u.k \
             ON DUPLICATE KEY UPDATE b = VALUES(b)",
        )
        .unwrap_or_else(|| unreachable!());
        assert_eq!(p.head, "INSERT IGNORE INTO t (a, b)");
        assert_eq!(p.select, "SELECT x, (SELECT 1) FROM u JOIN v ON v.k = u.k");
        assert_eq!(p.tail, " ON DUPLICATE KEY UPDATE b = VALUES(b)");
        let q = insert_select_parts("REPLACE INTO `s`.t (SELECT * FROM w WHERE a = 'ON')")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(q.head, "REPLACE INTO `s`.t");
        assert_eq!(q.select, "(SELECT * FROM w WHERE a = 'ON')");
        let w = insert_select_parts("INSERT INTO t WITH c AS (SELECT 1) SELECT * FROM c")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(w.select, "WITH c AS (SELECT 1) SELECT * FROM c");
        assert!(insert_select_parts("INSERT INTO t VALUES (1)").is_none());
        assert!(insert_select_parts("INSERT INTO t SET a = (SELECT 1)").is_none());
        assert!(insert_select_parts("SELECT 1").is_none());
        assert!(insert_select_parts("INSERT INTO t SELECT 1 RETURNING a").is_none());
    }

    #[test]
    fn load_data_reads_its_options() {
        let ld = load_data_parse(
            "LOAD DATA LOCAL INFILE '/tmp/x.csv' INTO TABLE `db`.orders FIELDS TERMINATED BY ',' \
             OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\r\\n' \
             IGNORE 1 LINES (oid, cid, `note`)",
        )
        .unwrap_or_else(|| unreachable!());
        assert!(ld.local);
        assert_eq!(ld.file, b"/tmp/x.csv");
        assert_eq!(ld.dup, Dup::Ignore);
        assert_eq!(ld.table_text, "`db`.orders");
        assert_eq!(ld.schema.as_deref(), Some("db"));
        assert_eq!(ld.table, "orders");
        assert_eq!(ld.field_term, b",");
        assert_eq!(ld.enclosed, Some(b'"'));
        assert_eq!(ld.escaped, Some(b'\\'));
        assert_eq!(ld.line_term, b"\r\n");
        assert_eq!(ld.skip, 1);
        assert_eq!(
            ld.columns,
            Some(vec!["oid".to_owned(), "cid".into(), "note".into()])
        );
        assert!(ld.unsupported.is_none());
        let r = load_data_parse("LOAD DATA INFILE 'f' REPLACE INTO TABLE t (a, @b) SET c = 1")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(r.dup, Dup::Replace);
        assert!(!r.local);
        assert!(r.unsupported.is_some());
        assert!(load_data_parse("LOAD XML LOCAL INFILE 'f' INTO TABLE t").is_none());
    }

    #[test]
    fn files_split_like_mariadb() {
        let ld = load_data_parse(
            "LOAD DATA LOCAL INFILE 'f' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '\"'",
        )
        .unwrap_or_else(|| unreachable!());
        let rows = split_rows(
            b"1,\"a,b\",NULL\n2,\"say \"\"hi\"\"\",\\N\n3,\"NULL\",x\\,y\n4,,\"\"\n",
            &ld,
        );
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0],
            vec![Some(b"1".to_vec()), Some(b"a,b".to_vec()), None]
        );
        assert_eq!(
            rows[1],
            vec![Some(b"2".to_vec()), Some(b"say \"hi\"".to_vec()), None]
        );
        assert_eq!(
            rows[2],
            vec![
                Some(b"3".to_vec()),
                Some(b"NULL".to_vec()),
                Some(b"x,y".to_vec())
            ]
        );
        assert_eq!(
            rows[3],
            vec![Some(b"4".to_vec()), Some(Vec::new()), Some(Vec::new())]
        );
        // Tabs, the default; a header skipped; LINES STARTING BY.
        let tab = load_data_parse(
            "LOAD DATA LOCAL INFILE 'f' INTO TABLE t LINES STARTING BY '>' IGNORE 1 LINES",
        )
        .unwrap_or_else(|| unreachable!());
        let rows = split_rows(b">h1\th2\n>1\tx\\ty\nnoise\n>2\t\\N\n", &tab);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some(b"1".to_vec()), Some(b"x\ty".to_vec())]);
        assert_eq!(rows[1], vec![Some(b"2".to_vec()), None]);
    }

    #[test]
    fn values_become_literals_of_their_type() {
        let col = |t: u8, cs: u16| ColumnDef {
            col_type: t,
            charset: cs,
            ..ColumnDef::default()
        };
        assert_eq!(literal(&col(ct::LONG, 63), Some(b"-12")), "-12");
        assert_eq!(literal(&col(ct::NEWDECIMAL, 63), Some(b"1.50")), "1.50");
        assert_eq!(literal(&col(ct::LONG, 63), None), "NULL");
        assert_eq!(literal(&col(ct::VAR_STRING, 45), Some(b"it's")), "'it''s'");
        assert_eq!(
            literal(&col(ct::VAR_STRING, 45), Some(b"a\\b")),
            "X'615C62'"
        );
        assert_eq!(literal(&col(ct::BLOB, 63), Some(&[0, 255])), "X'00FF'");
        assert_eq!(
            literal(&col(ct::DATETIME, 63), Some(b"2026-11-09 10:00:00")),
            "'2026-11-09 10:00:00'"
        );
        assert_eq!(literal(&col(ct::BIT, 63), Some(&[1])), "X'01'");
    }
}
