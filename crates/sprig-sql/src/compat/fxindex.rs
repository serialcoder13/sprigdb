//! MySQL's functional key parts on port 3306 (chapter 28, "Functional
//! index parts"; build step 10; R4 round 3).
//!
//! MySQL 8.4 indexes an expression: `INDEX i ((lower(email)))`. MariaDB
//! 11.8 can't (MDEV-35853) and fails with 1064; it can index a virtual
//! column. Each functional part `(e)` therefore becomes a hidden column
//! [`FX_PREFIX`]`<8 hex>` (a hash of `e`'s text), `GENERATED ALWAYS AS
//! (e) VIRTUAL INVISIBLE` with the type MySQL would give `e`, and the part
//! names that column, as MySQL itself does under the hood (its hidden
//! column is `!hidden!i!0!0`). MariaDB 11.8 uses an indexed virtual column
//! for a condition written over its expression, so queries need nothing.
//!
//! Shown as MySQL shows it: `SHOW CREATE TABLE` puts `((e))` back in the
//! key and leaves the column out ([`super::schema::mysql_functional_parts`]);
//! `SHOW INDEX` and `information_schema.STATISTICS` give `NULL` as the
//! column and `e` as `Expression`; chapter 07 already hides `_sprig_`
//! columns everywhere else.
//!
//! The column's type: `CAST(x AS t)` gives `t` (`CHAR(n)` as `VARCHAR(n)`,
//! `SIGNED` as `BIGINT`, ...); a string function of one column (`lower`,
//! `upper`, `trim`, `left`, `substring`, ...) the column's type when the
//! same `CREATE TABLE` defines it; date parts `INT`; `date()` `DATE`;
//! `md5()` `CHAR(32)`; arithmetic on integer columns `BIGINT`. Any other
//! expression leaves the statement as it is (MariaDB's 1064 stands), as do
//! `CREATE INDEX` and `ALTER TABLE ... ADD INDEX` on expressions whose
//! type needs a column's definition.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::lex::{self, LexOptions, Token, TokenKind};

/// The name prefix of the hidden columns behind functional key parts.
pub const FX_PREFIX: &str = "_sprig_fx_";

/// The hidden column for expression text `e`: [`FX_PREFIX`] and 8 hex
/// digits of a hash (FNV-1a) of `e` without whitespace and backquotes, in
/// lower case.
#[must_use]
pub fn fx_column(e: &str) -> String {
    let canon: String = e
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '`')
        .flat_map(char::to_lowercase)
        .collect();
    // FNV-1a, 64 bits: the name only has to be stable and spread.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in canon.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{FX_PREFIX}{:08x}", h >> 32)
}

/// Whether a column name is a functional part's hidden column.
#[must_use]
pub fn is_fx_column(name: &str) -> bool {
    name.len() == FX_PREFIX.len() + 8
        && name[..FX_PREFIX.len()].eq_ignore_ascii_case(FX_PREFIX)
        && name[FX_PREFIX.len()..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
}

struct Sig<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
}

impl Sig<'_> {
    fn word(&self, j: usize, w: &[u8]) -> bool {
        self.t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w)
        })
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t.get(j).is_some_and(|tk| {
            matches!(tk.kind, TokenKind::Punct | TokenKind::Op) && tk.bytes(self.sql) == [c]
        })
    }

    fn ident(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&lex::ident_value(tk.bytes(self.sql))).into_owned())
    }

    fn matching(&self, from: usize) -> Option<usize> {
        let mut d = 0;
        for k in from..self.t.len() {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d -= 1;
                if d == 0 {
                    return Some(k);
                }
            }
        }
        None
    }

    fn split(&self, a: usize, b: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut d = 0;
        let mut start = a;
        for k in a..b {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d -= 1;
            } else if d == 0 && self.punct(k, b',') {
                out.push((start, k));
                start = k + 1;
            }
        }
        out.push((start, b));
        out
    }

    fn text(&self, a: usize, b: usize) -> String {
        String::from_utf8_lossy(&self.sql[self.t[a].start as usize..self.t[b].end as usize])
            .into_owned()
    }
}

/// Words that end a column's type in its definition.
const ATTRIBUTE: &[&[u8]] = &[
    b"NOT",
    b"NULL",
    b"DEFAULT",
    b"AUTO_INCREMENT",
    b"PRIMARY",
    b"UNIQUE",
    b"KEY",
    b"COMMENT",
    b"GENERATED",
    b"AS",
    b"CHECK",
    b"REFERENCES",
    b"INVISIBLE",
    b"VISIBLE",
    b"ON",
    b"SRID",
    b"COLUMN_FORMAT",
    b"STORAGE",
];

/// The column definitions of a `CREATE TABLE` element list: lower-case
/// name to type text.
fn column_types(s: &Sig<'_>, elems: &[(usize, usize)]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for &(a, b) in elems {
        if a >= b
            || !s.t[a].kind.is_ident_like()
            || key_element(s, a).is_some()
            || s.word(a, b"CONSTRAINT")
            || s.word(a, b"CHECK")
        {
            continue;
        }
        let Some(name) = s.ident(a) else { continue };
        let mut e = a + 1;
        while e < b && !ATTRIBUTE.iter().any(|w| s.word(e, w)) {
            if s.punct(e, b'(') {
                match s.matching(e) {
                    Some(m) => e = m,
                    None => break,
                }
            }
            e += 1;
        }
        if e > a + 1 {
            out.insert(name.to_lowercase(), s.text(a + 1, e - 1));
        }
    }
    out
}

/// For a key element at `a`: the index of its `(` part list.
fn key_element(s: &Sig<'_>, a: usize) -> Option<usize> {
    let mut j = a;
    if s.word(j, b"CONSTRAINT") {
        j += 1;
        if s.t.get(j).is_some_and(|t| t.kind.is_ident_like())
            && !s.word(j, b"UNIQUE")
            && !s.word(j, b"PRIMARY")
        {
            j += 1;
        }
    }
    if s.word(j, b"UNIQUE") {
        j += 1;
        if s.word(j, b"KEY") || s.word(j, b"INDEX") {
            j += 1;
        }
    } else if s.word(j, b"KEY") || s.word(j, b"INDEX") {
        j += 1;
    } else {
        return None;
    }
    // An optional name, then the part list.
    if !s.punct(j, b'(') {
        j += 1;
    }
    if s.word(j, b"USING") {
        j += 2;
    }
    s.punct(j, b'(').then_some(j)
}

/// MariaDB's type for MySQL's `CAST(.. AS t)` target `t`.
fn cast_type(t: &str) -> Option<String> {
    let up = t.trim().to_ascii_uppercase();
    let word = up
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .unwrap_or("");
    Some(match word {
        "CHAR" if up.contains('(') => format!("VARCHAR{}", &t.trim()[4..]),
        "BINARY" if up.contains('(') => format!("VARBINARY{}", &t.trim()[6..]),
        "SIGNED" => "BIGINT".to_owned(),
        "UNSIGNED" => "BIGINT UNSIGNED".to_owned(),
        "DATE" | "DATETIME" | "TIME" | "DECIMAL" | "DOUBLE" | "FLOAT" | "REAL" | "YEAR" => {
            t.trim().to_owned()
        }
        _ => return None,
    })
}

fn is_integer_type(t: &str) -> bool {
    let up = t.trim_start().to_ascii_uppercase();
    [
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "INT",
        "INTEGER",
        "BIGINT",
        "BOOL",
        "BOOLEAN",
    ]
    .iter()
    .any(|w| {
        up.starts_with(w)
            && up[w.len()..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_ascii_alphabetic())
    })
}

fn is_string_type(t: &str) -> bool {
    let up = t.trim_start().to_ascii_uppercase();
    up.starts_with("VARCHAR") || up.starts_with("CHAR")
}

/// The type MySQL gives the expression `[a, b]`, as MariaDB DDL; `None`
/// when it isn't known (see the module docs).
#[allow(clippy::many_single_char_names, clippy::too_many_lines)] // one rule per expression shape
fn infer(s: &Sig<'_>, a: usize, b: usize, cols: &BTreeMap<String, String>) -> Option<String> {
    let col_type = |j: usize| -> Option<&String> {
        // `c` or `t.c`.
        let name = if s.punct(j + 1, b'.') {
            s.ident(j + 2)?
        } else {
            s.ident(j)?
        };
        cols.get(&name.to_lowercase())
    };
    // CAST(x AS t), CONVERT(x, t)
    if (s.word(a, b"CAST") || s.word(a, b"CONVERT"))
        && s.punct(a + 1, b'(')
        && s.matching(a + 1) == Some(b)
    {
        let mut d = 0;
        for k in a + 2..b {
            if s.punct(k, b'(') {
                d += 1;
            } else if s.punct(k, b')') {
                d -= 1;
            } else if d == 0
                && ((s.word(a, b"CAST") && s.word(k, b"AS"))
                    || (s.word(a, b"CONVERT") && s.punct(k, b',')))
            {
                if k + 1 > b - 1 {
                    return None;
                }
                let t = s.text(k + 1, b - 1);
                if t.to_ascii_uppercase().contains("ARRAY")
                    || t.to_ascii_uppercase().starts_with("JSON")
                {
                    return None;
                }
                return cast_type(&t);
            }
        }
        return None;
    }
    // f(args)
    if s.t[a].kind == TokenKind::Word && s.punct(a + 1, b'(') && s.matching(a + 1) == Some(b) {
        let f = String::from_utf8_lossy(s.t[a].bytes(s.sql)).to_ascii_lowercase();
        let args = s.split(a + 2, b);
        let first_col = args.first().and_then(|&(x, y)| {
            let single = y == x + 1 || (y == x + 3 && s.punct(x + 1, b'.'));
            if single { col_type(x).cloned() } else { None }
        });
        return match f.as_str() {
            "lower" | "upper" | "lcase" | "ucase" | "trim" | "ltrim" | "rtrim" | "reverse"
            | "left" | "right" | "substring" | "substr" | "mid" => {
                first_col.filter(|t| is_string_type(t))
            }
            "year" | "month" | "day" | "dayofmonth" | "dayofweek" | "dayofyear" | "hour"
            | "minute" | "second" | "quarter" | "week" | "weekday" | "weekofyear" => {
                Some("INT".to_owned())
            }
            "date" => Some("DATE".to_owned()),
            "md5" => Some("CHAR(32)".to_owned()),
            "sha1" | "sha" => Some("CHAR(40)".to_owned()),
            "crc32" => Some("INT UNSIGNED".to_owned()),
            "length" | "char_length" | "character_length" | "octet_length" | "bit_length" => {
                Some("BIGINT".to_owned())
            }
            "abs" => first_col
                .filter(|t| is_integer_type(t))
                .map(|_| "BIGINT".to_owned()),
            // R4 round 5: string columns and literals, `VARCHAR` of their
            // lengths' sum, as MySQL types it.
            // R4 round 5: string columns and literals, `VARCHAR` of their
            // lengths' sum (`CONCAT_WS`: the separator once between each
            // pair), as MySQL types it.
            "concat" | "concat_ws" => {
                let mut lens = Vec::new();
                for &(x, y) in &args {
                    lens.push(string_len(s, x, y, &col_type)?);
                }
                let n = if f == "concat_ws" {
                    let (sep, rest) = lens.split_first()?;
                    rest.iter().sum::<u64>() + sep * (rest.len().max(1) as u64 - 1)
                } else {
                    lens.iter().sum()
                };
                (n > 0 && n <= 16_383).then(|| format!("VARCHAR({n})"))
            }
            _ => None,
        };
    }
    // R4 round 5: `CASE ... END`, the type its results share (strings:
    // the longest).
    if s.word(a, b"CASE") && s.word(b, b"END") {
        return case_type(s, a, b, cols);
    }
    // R4 round 5: a comparison at the top level is 0, 1 or NULL.
    {
        let mut d = 0i32;
        for k in a..=b {
            if s.punct(k, b'(') {
                d += 1;
            } else if s.punct(k, b')') {
                d -= 1;
            } else if d == 0
                && (matches!(s.t[k].kind, TokenKind::Op)
                    && matches!(
                        s.t[k].bytes(s.sql),
                        b"=" | b"<" | b">" | b"<=" | b">=" | b"<>" | b"!=" | b"<=>"
                    )
                    || [&b"BETWEEN"[..], b"LIKE", b"IN", b"IS", b"REGEXP"]
                        .iter()
                        .any(|w| s.word(k, w)))
            {
                return Some("BIGINT".to_owned());
            }
        }
    }
    // `c COLLATE x` over a string column (R4 round 4): the column's type
    // with that collation.
    if b == a + 2 && s.word(a + 1, b"COLLATE") && s.t[a + 2].kind.is_ident_like() {
        let t = col_type(a)?.clone();
        if !is_string_type(&t) {
            return None;
        }
        let base = t.split(" COLLATE ").next().unwrap_or(&t).to_owned();
        return Some(format!("{base} COLLATE {}", s.text(a + 2, a + 2)));
    }
    // Arithmetic over numeric columns and literals: integers alone give
    // `BIGINT`; a `/`, a decimal literal or a non-integer column (R4
    // round 4) gives `DOUBLE`.
    let mut any_col = false;
    let mut real = false;
    for k in a..=b {
        let tk = &s.t[k];
        match tk.kind {
            TokenKind::Number if tk.bytes(s.sql).iter().all(u8::is_ascii_digit) => {}
            TokenKind::Number => real = true,
            TokenKind::Op if matches!(tk.bytes(s.sql), b"+" | b"-" | b"*") => {}
            TokenKind::Op if tk.bytes(s.sql) == b"/" => real = true,
            TokenKind::Punct if matches!(tk.bytes(s.sql), b"(" | b")" | b".") => {}
            _ if tk.kind.is_ident_like() => {
                if s.punct(k + 1, b'.') {
                    continue;
                }
                let name = s.ident(k)?.to_lowercase();
                let t = cols.get(&name)?;
                if !is_integer_type(t) {
                    let up = t.trim_start().to_ascii_uppercase();
                    if !["DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"]
                        .iter()
                        .any(|n| up.starts_with(n))
                    {
                        return None;
                    }
                    real = true;
                }
                any_col = true;
            }
            _ => return None,
        }
    }
    any_col.then(|| if real { "DOUBLE" } else { "BIGINT" }.to_owned())
}

/// The length of a string argument `[x, y)`: a literal (with or without
/// an introducer such as `_utf8mb4`) or a `CHAR`/`VARCHAR` column.
fn string_len<'c>(
    s: &Sig<'_>,
    x: usize,
    y: usize,
    col_type: &dyn Fn(usize) -> Option<&'c String>,
) -> Option<u64> {
    let lit_at = if y == x + 1 {
        Some(x)
    } else if y == x + 2
        && s.t[x].kind == TokenKind::Word
        && s.t[x].bytes(s.sql).first() == Some(&b'_')
    {
        Some(x + 1)
    } else {
        None
    };
    if let Some(l) = lit_at
        && s.t[l].kind == TokenKind::String
    {
        let lit = lex::string_value(s.t[l].bytes(s.sql), false)?;
        return Some(String::from_utf8_lossy(&lit).chars().count() as u64);
    }
    let single = y == x + 1 || (y == x + 3 && s.punct(x + 1, b'.'));
    let t = if single { col_type(x)? } else { return None };
    if !is_string_type(t) {
        return None;
    }
    t.split_once('(')
        .and_then(|(_, r)| r.split_once(')'))
        .and_then(|(l, _)| l.trim().parse().ok())
}

/// The type of `CASE ... END` at `[a, b]`: every `THEN` and `ELSE` result
/// typed alike (`NULL` results left out); strings give the longest.
#[allow(clippy::many_single_char_names)] // as infer
fn case_type(s: &Sig<'_>, a: usize, b: usize, cols: &BTreeMap<String, String>) -> Option<String> {
    let mut results: Vec<(usize, usize)> = Vec::new();
    let mut d = 0i32;
    let mut start: Option<usize> = None;
    for k in a + 1..=b {
        if s.punct(k, b'(') {
            d += 1;
        } else if s.punct(k, b')') {
            d -= 1;
        } else if d == 0 {
            let ends = s.word(k, b"WHEN") || s.word(k, b"ELSE") || s.word(k, b"END");
            if ends && let Some(st) = start.take() {
                results.push((st, k - 1));
            }
            if s.word(k, b"THEN") || s.word(k, b"ELSE") {
                start = Some(k + 1);
            }
        }
    }
    let mut types: Vec<String> = Vec::new();
    for (x, y) in results {
        if x > y {
            return None;
        }
        if x == y && s.word(x, b"NULL") {
            continue;
        }
        types.push(infer(s, x, y, cols)?);
    }
    let first = types.first()?.clone();
    if types.iter().all(|t| *t == first) {
        return Some(first);
    }
    // Strings of several lengths: the longest VARCHAR.
    let mut n = 0u64;
    for t in &types {
        if !is_string_type(t) {
            return None;
        }
        n = n.max(
            t.split_once('(')
                .and_then(|(_, r)| r.split_once(')'))
                .and_then(|(l, _)| l.trim().parse().ok())?,
        );
    }
    Some(format!("VARCHAR({n})"))
}

/// One functional part found: the part's `(` and `)`, its expression's
/// token range, and the hidden column definition and name.
struct Part {
    open: usize,
    close: usize,
    column: String,
    definition: String,
}

/// The functional parts of the part list at `open` (`(` of the list).
fn parts(s: &Sig<'_>, open: usize, cols: &BTreeMap<String, String>) -> Option<Vec<Part>> {
    let close = s.matching(open)?;
    let mut out = Vec::new();
    for (a, b) in s.split(open + 1, close) {
        // `(expr) [ASC | DESC]`
        if !s.punct(a, b'(') {
            continue;
        }
        let pc = s.matching(a)?;
        if pc >= b || (pc + 1 < b && !(s.word(pc + 1, b"ASC") || s.word(pc + 1, b"DESC"))) {
            continue;
        }
        if pc == a + 1 {
            return None;
        }
        let ty = infer(s, a + 1, pc - 1, cols)?;
        let expr = s.text(a + 1, pc - 1);
        let column = fx_column(&expr);
        out.push(Part {
            open: a,
            close: pc,
            definition: format!("`{column}` {ty} GENERATED ALWAYS AS ({expr}) VIRTUAL INVISIBLE"),
            column,
        });
    }
    Some(out)
}

fn apply(sql: &[u8], mut edits: Vec<(usize, usize, String)>) -> Vec<u8> {
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 128);
    let mut at = 0;
    for (from, to, text) in edits {
        out.extend_from_slice(&sql[at..from]);
        out.extend_from_slice(text.as_bytes());
        at = to;
    }
    out.extend_from_slice(&sql[at..]);
    out
}

/// A cheap test on the bytes: a functional part opens with `(` right
/// after the part list's `(` or a `,`, once whitespace is gone (R4 round
/// 5: Django's mixed lists, `(author_id DESC, (LOWER(title)) ASC)`, have
/// no `((`).
fn maybe_functional(sql: &[u8]) -> bool {
    let mut last = 0u8;
    for &b in sql {
        if b.is_ascii_whitespace() {
            continue;
        }
        if b == b'(' && (last == b'(' || last == b',') {
            return true;
        }
        last = b;
    }
    false
}

/// Functional key parts as hidden virtual columns (see the module docs).
/// `None` when nothing changes or a part's type isn't known.
#[must_use]
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    rewrite_with(sql, opts, &BTreeMap::new())
}

/// The table whose column types a `CREATE INDEX` or `ALTER TABLE ... ADD
/// INDEX` with functional parts needs (its text as written, perhaps
/// qualified), when [`rewrite`] can't type a part from the statement
/// alone (R4 round 4: the gateway reads them from `information_schema`
/// and calls [`rewrite_with`]).
#[must_use]
pub fn needs_types(sql: &[u8], opts: &LexOptions) -> Option<String> {
    if !maybe_functional(sql) {
        return None;
    }
    if rewrite(sql, opts).is_some() {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let s = Sig {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    if s.word(0, b"CREATE") {
        let j = if s.word(1, b"UNIQUE") { 2 } else { 1 };
        if !s.word(j, b"INDEX") {
            return None;
        }
        let on = (j + 2..s.t.len()).find(|&k| s.word(k, b"ON"))?;
        let list = (on + 1..s.t.len()).find(|&k| s.punct(k, b'('))?;
        return Some(s.text(on + 1, list - 1));
    }
    if s.word(0, b"ALTER") && s.word(1, b"TABLE") {
        let add = (2..s.t.len()).find(|&k| s.word(k, b"ADD"))?;
        return Some(s.text(2, add - 1));
    }
    None
}

/// [`rewrite`] with the table's column types known (`known`: lower-case
/// name to type), for `CREATE INDEX` and `ALTER TABLE`.
#[must_use]
#[allow(clippy::too_many_lines)] // the three statement forms
pub fn rewrite_with(
    sql: &[u8],
    opts: &LexOptions,
    known: &BTreeMap<String, String>,
) -> Option<Vec<u8>> {
    if !maybe_functional(sql) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let s = Sig {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    let st = |j: usize| s.t[j].start as usize;
    let en = |j: usize| s.t[j].end as usize;
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    if s.word(0, b"CREATE")
        && (s.word(1, b"TABLE") || (s.word(1, b"TEMPORARY") && s.word(2, b"TABLE")))
    {
        let open = (0..s.t.len()).find(|&j| s.punct(j, b'('))?;
        let close = s.matching(open)?;
        let elems = s.split(open + 1, close);
        let cols = column_types(&s, &elems);
        let mut defs: BTreeMap<String, String> = BTreeMap::new();
        for &(a, _) in &elems {
            let Some(list) = key_element(&s, a) else {
                continue;
            };
            for p in parts(&s, list, &cols)? {
                edits.push((st(p.open), en(p.close), format!("`{}`", p.column)));
                defs.insert(p.column, p.definition);
            }
        }
        if defs.is_empty() {
            return None;
        }
        let at = st(close);
        let text = defs.values().fold(String::new(), |mut s, d| {
            let _ = write!(s, ", {d}");
            s
        });
        edits.push((at, at, text));
        return Some(apply(sql, edits));
    }
    // CREATE [UNIQUE] INDEX i [USING ..] ON t (parts) -> ALTER TABLE
    if s.word(0, b"CREATE") {
        let mut j = 1;
        let unique = s.word(j, b"UNIQUE");
        if unique {
            j += 1;
        }
        if !s.word(j, b"INDEX") {
            return None;
        }
        let name = s.ident(j + 1)?;
        let on = (j + 2..s.t.len()).find(|&k| s.word(k, b"ON"))?;
        let list = (on + 1..s.t.len()).find(|&k| s.punct(k, b'('))?;
        let table = s.text(on + 1, list - 1);
        let ps = parts(&s, list, known)?;
        if ps.is_empty() {
            return None;
        }
        let list_close = s.matching(list)?;
        let mut parts_text = String::from_utf8_lossy(&sql[st(list)..en(list_close)]).into_owned();
        // Replace from the end so offsets stay valid.
        for p in ps.iter().rev() {
            let a = st(p.open) - st(list);
            let b = en(p.close) - st(list);
            parts_text.replace_range(a..b, &format!("`{}`", p.column));
        }
        let mut cols = String::new();
        let mut seen = Vec::new();
        for p in &ps {
            if !seen.contains(&p.column) {
                let _ = write!(cols, "ADD COLUMN IF NOT EXISTS {}, ", p.definition);
                seen.push(p.column.clone());
            }
        }
        let tail = String::from_utf8_lossy(&sql[en(list_close)..]).into_owned();
        return Some(
            format!(
                "ALTER TABLE {table} {cols}ADD {}INDEX `{}` {parts_text}{tail}",
                if unique { "UNIQUE " } else { "" },
                name.replace('`', "``")
            )
            .into_bytes(),
        );
    }
    // ALTER TABLE t ADD [UNIQUE] {INDEX | KEY} [i] (parts), ...
    if s.word(0, b"ALTER") && s.word(1, b"TABLE") {
        let mut defs: Vec<(String, String)> = Vec::new();
        for (a, _) in s.split(2, s.t.len()) {
            let start = if a == 2 {
                // The first specification follows the table name.
                (a..s.t.len()).find(|&k| s.word(k, b"ADD"))?
            } else {
                a
            };
            if !s.word(start, b"ADD") {
                continue;
            }
            let Some(list) = key_element(&s, start + 1) else {
                continue;
            };
            for p in parts(&s, list, known)? {
                edits.push((st(p.open), en(p.close), format!("`{}`", p.column)));
                if !defs.iter().any(|(c, _)| *c == p.column) {
                    defs.push((p.column, p.definition));
                }
            }
        }
        if defs.is_empty() {
            return None;
        }
        // The columns come first: `ADD INDEX` names them.
        let first_add = (2..s.t.len()).find(|&k| s.word(k, b"ADD"))?;
        let at = st(first_add);
        let text = defs.iter().fold(String::new(), |mut s, (_, d)| {
            let _ = write!(s, "ADD COLUMN IF NOT EXISTS {d}, ");
            s
        });
        edits.push((at, at, text));
        return Some(apply(sql, edits));
    }
    None
}

/// `DROP INDEX i ON t` or `ALTER TABLE t DROP {INDEX | KEY} i` alone
/// (Django's `remove_index` and `remove_constraint`): the table as
/// written and the index's name (R4 round 5, [`fk_indexes_kept`]).
#[must_use]
pub fn drop_index_target(sql: &[u8], opts: &LexOptions) -> Option<(String, String)> {
    if sql.len() > 4096 || !sql.windows(4).any(|w| w.eq_ignore_ascii_case(b"DROP")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let s = Sig {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    let n = s.t.len() - usize::from(s.punct(s.t.len().checked_sub(1)?, b';'));
    let table_end = |from: usize| -> Option<usize> {
        // `t` or `d.t`.
        if s.punct(from + 1, b'.') {
            s.ident(from + 2).map(|_| from + 2)
        } else {
            s.ident(from).map(|_| from)
        }
    };
    if s.word(0, b"DROP") && s.word(1, b"INDEX") && s.word(3, b"ON") {
        let name = s.ident(2)?;
        let end = table_end(4)?;
        return (end + 1 == n).then(|| (s.text(4, end), name));
    }
    if s.word(0, b"ALTER") && s.word(1, b"TABLE") {
        let end = table_end(2)?;
        if s.word(end + 1, b"DROP")
            && (s.word(end + 2, b"INDEX") || s.word(end + 2, b"KEY"))
            && end + 4 == n
        {
            return Some((s.text(2, end), s.ident(end + 3)?));
        }
    }
    None
}

/// One key part of a table's index, as `information_schema.STATISTICS`
/// lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPart {
    /// The index.
    pub index: String,
    /// Its position, from 1.
    pub seq: u32,
    /// The column (`None` for an expression).
    pub column: Option<String>,
    /// Descending.
    pub desc: bool,
}

/// The foreign keys MySQL would still back with their own index once
/// `dropping` is gone (R4 round 5; Django's `test_composed_desc_func_index_with_fk`).
///
/// Both servers create an index for a foreign key and drop it silently
/// when a later index can serve the key. MySQL never counts an index whose
/// leading parts are descending as one that can; MariaDB does. So after
/// `CREATE INDEX i ON book (author_id DESC, title)` MySQL keeps the
/// foreign key's index and `DROP INDEX i` works, while MariaDB dropped it
/// and refuses with 1553. For each foreign key (name, columns) whose only
/// serving index in MariaDB's sense is `dropping`, and which no
/// ascending index serves (MySQL's sense, so MySQL would refuse too):
/// the key's name and columns, for an `ADD INDEX` in the same `ALTER`.
#[must_use]
pub fn fk_indexes_kept(
    dropping: &str,
    parts: &[IndexPart],
    fks: &[(String, Vec<String>)],
) -> Vec<(String, Vec<String>)> {
    let mut by_index: BTreeMap<String, Vec<&IndexPart>> = BTreeMap::new();
    for p in parts {
        by_index.entry(p.index.to_lowercase()).or_default().push(p);
    }
    for v in by_index.values_mut() {
        v.sort_by_key(|p| p.seq);
    }
    // Whether `ps` leads with `cols`; `asc`: every one of them ascending.
    let leads = |ps: &[&IndexPart], cols: &[String], asc: bool| {
        ps.len() >= cols.len()
            && cols.iter().zip(ps).all(|(c, p)| {
                p.column
                    .as_deref()
                    .is_some_and(|pc| pc.eq_ignore_ascii_case(c))
                    && (!asc || !p.desc)
            })
    };
    let drop_key = dropping.to_lowercase();
    let Some(dropped) = by_index.get(&drop_key) else {
        return Vec::new();
    };
    fks.iter()
        .filter(|(_, cols)| {
            !cols.is_empty()
                && leads(dropped, cols, false)
                && !leads(dropped, cols, true)
                && !by_index
                    .iter()
                    .any(|(k, ps)| *k != drop_key && leads(ps, cols, false))
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descending_index_over_a_foreign_key() {
        let o = LexOptions::mysql84();
        assert_eq!(
            drop_index_target(b"DROP INDEX `book_author_title_idx` ON `schema_book`", &o),
            Some((
                "`schema_book`".to_owned(),
                "book_author_title_idx".to_owned()
            ))
        );
        assert_eq!(
            drop_index_target(b"ALTER TABLE d.t DROP KEY k;", &o),
            Some(("d.t".to_owned(), "k".to_owned()))
        );
        assert_eq!(
            drop_index_target(b"ALTER TABLE t DROP INDEX k, ADD INDEX j (a)", &o),
            None
        );
        assert_eq!(
            drop_index_target(b"DROP INDEX k ON t ALGORITHM=INPLACE", &o),
            None
        );
        let p = |index: &str, seq: u32, column: &str, desc: bool| IndexPart {
            index: index.to_owned(),
            seq,
            column: Some(column.to_owned()),
            desc,
        };
        let fks = vec![("book_author_id_fk".to_owned(), vec!["author_id".to_owned()])];
        // After CREATE INDEX i (author_id DESC, title), MariaDB dropped the
        // key's own index.
        let parts = vec![
            p("PRIMARY", 1, "id", false),
            p("i", 1, "author_id", true),
            p("i", 2, "title", false),
        ];
        assert_eq!(fk_indexes_kept("i", &parts, &fks), fks);
        // Ascending: MySQL refuses with 1553 too.
        let asc = vec![p("i", 1, "author_id", false), p("i", 2, "title", false)];
        assert!(fk_indexes_kept("i", &asc, &fks).is_empty());
        // Another index still serves the key.
        let mut other = parts.clone();
        other.push(p("book_author_id_fk", 1, "author_id", false));
        assert!(fk_indexes_kept("i", &other, &fks).is_empty());
        // The index doesn't lead with the key's column.
        let unrelated = vec![p("i", 1, "title", true), p("i", 2, "author_id", true)];
        assert!(fk_indexes_kept("i", &unrelated, &fks).is_empty());
    }

    fn rw(s: &str) -> Option<String> {
        rewrite(s.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn concat_and_comparisons() {
        // R4 round 5: MySQL indexes these; the hidden column needs a type.
        let out = rw("CREATE TABLE fx (id INT PRIMARY KEY, a INT, s VARCHAR(20), KEY k2 ((CONCAT(s, 'x'))), KEY k3 ((a BETWEEN 1 AND 5)), KEY k4 ((a = 1)))").unwrap();
        assert!(
            out.contains("` VARCHAR(21) GENERATED ALWAYS AS (CONCAT(s, 'x'))"),
            "{out}"
        );
        assert!(
            out.contains("` BIGINT GENERATED ALWAYS AS (a BETWEEN 1 AND 5)"),
            "{out}"
        );
        assert!(
            out.contains("` BIGINT GENERATED ALWAYS AS (a = 1)"),
            "{out}"
        );
        assert_eq!(rw("CREATE TABLE t (j JSON, KEY ((CONCAT(j, 'x'))))"), None);
        // Rails' Active Record test schema (companies).
        let out = rw("CREATE TABLE `companies` (`id` int NOT NULL, `firm_name` varchar(255), `name` varchar(255), `rating` int DEFAULT 1, INDEX `company_expression_index` ((CASE WHEN rating > 0 THEN lower(name) END) DESC), INDEX `full_name_index` ((CONCAT_WS(`firm_name`, `name`, _utf8mb4' '))))").unwrap();
        assert!(
            out.contains(
                "` varchar(255) GENERATED ALWAYS AS (CASE WHEN rating > 0 THEN lower(name) END)"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "` VARCHAR(511) GENERATED ALWAYS AS (CONCAT_WS(`firm_name`, `name`, _utf8mb4' '))"
            ),
            "{out}"
        );
    }

    #[test]
    fn mixed_part_lists() {
        // Django's Index(F("author").desc(), Lower("title").asc(), "pub_date")
        // and UniqueConstraint(F("height").desc(), "uuid", Lower("name").asc()):
        // no `((` anywhere (R4 round 5).
        let o = LexOptions::mysql84();
        let known: BTreeMap<String, String> = [("title".to_owned(), "varchar(100)".to_owned())]
            .into_iter()
            .collect();
        let sql = "CREATE INDEX `func_f_lower_field_idx` ON `schema_book` (`author_id` DESC, (LOWER(`title`)) ASC, `pub_date`)";
        assert_eq!(
            needs_types(sql.as_bytes(), &o).as_deref(),
            Some("`schema_book`")
        );
        let out = String::from_utf8(rewrite_with(sql.as_bytes(), &o, &known).unwrap()).unwrap();
        let k = fx_column("LOWER(`title`)");
        assert_eq!(
            out,
            format!(
                "ALTER TABLE `schema_book` ADD COLUMN IF NOT EXISTS `{k}` varchar(100) GENERATED ALWAYS AS (LOWER(`title`)) VIRTUAL INVISIBLE, ADD INDEX `func_f_lower_field_idx` (`author_id` DESC, `{k}` ASC, `pub_date`)"
            )
        );
        let alter = "ALTER TABLE `schema_author` ADD CONSTRAINT `u` UNIQUE (`height` DESC, `uuid`, (LOWER(`title`)) ASC)";
        assert!(rewrite_with(alter.as_bytes(), &o, &known).is_some());
    }

    #[test]
    fn collate_and_real_arithmetic() {
        let o = LexOptions::mysql84();
        let known: BTreeMap<String, String> = [
            ("title".to_owned(), "varchar(100)".to_owned()),
            ("height".to_owned(), "double".to_owned()),
            ("weight".to_owned(), "int".to_owned()),
        ]
        .into_iter()
        .collect();
        let r = |sql: &str| {
            rewrite_with(sql.as_bytes(), &o, &known).map(|v| String::from_utf8(v).unwrap())
        };
        let out = r("CREATE INDEX i ON t ((`title` COLLATE `utf8mb4_esperanto_ci`) DESC)").unwrap();
        assert!(
            out.contains("varchar(100) COLLATE `utf8mb4_esperanto_ci` GENERATED ALWAYS"),
            "{out}"
        );
        let out = r("CREATE INDEX i ON t (((`height` / (`weight` + 5))))").unwrap();
        assert!(
            out.contains("` DOUBLE GENERATED ALWAYS AS ((`height` / (`weight` + 5)))"),
            "{out}"
        );
        let out = r("CREATE INDEX i ON t ((`weight` + 0.0))").unwrap();
        assert!(out.contains("` DOUBLE GENERATED"), "{out}");
        let out = r("CREATE INDEX i ON t ((`weight` * 2))").unwrap();
        assert!(out.contains("` BIGINT GENERATED"), "{out}");
    }

    #[test]
    fn create_index_with_known_types() {
        let o = LexOptions::mysql84();
        let sql = b"CREATE UNIQUE INDEX u ON `t` ((LOWER(`name`)), `rank`)";
        assert!(rewrite(sql, &o).is_none());
        assert_eq!(needs_types(sql, &o).as_deref(), Some("`t`"));
        let known: BTreeMap<String, String> = [("name".to_owned(), "varchar(50)".to_owned())]
            .into_iter()
            .collect();
        let c = fx_column("LOWER(`name`)");
        assert_eq!(
            String::from_utf8(rewrite_with(sql, &o, &known).unwrap()).unwrap(),
            format!(
                "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `{c}` varchar(50) GENERATED ALWAYS AS (LOWER(`name`)) VIRTUAL INVISIBLE, ADD UNIQUE INDEX `u` (`{c}`, `rank`)"
            )
        );
        assert_eq!(
            needs_types(b"ALTER TABLE s.t ADD INDEX i ((upper(a)))", &o).as_deref(),
            Some("s.t")
        );
        assert!(needs_types(b"CREATE INDEX i ON t (a)", &o).is_none());
    }

    #[test]
    fn create_table_parts_become_hidden_columns() {
        let c = fx_column("lower(email)");
        assert_eq!(c, fx_column("LOWER( `email` )"));
        assert_eq!(
            rw("CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL, INDEX i ((lower(email))))").unwrap(),
            format!(
                "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL, INDEX i (`{c}`), \
                 `{c}` VARCHAR(255) GENERATED ALWAYS AS (lower(email)) VIRTUAL INVISIBLE)"
            )
        );
        let y = fx_column("year(d)");
        let k = fx_column("cast(j->>'$.k' as char(20))");
        let out = rw("CREATE TABLE t (d DATE, j JSON, KEY a (id, (year(d)) DESC), UNIQUE KEY b ((cast(j->>'$.k' as char(20)))))").unwrap();
        assert!(
            out.starts_with(&format!(
                "CREATE TABLE t (d DATE, j JSON, KEY a (id, `{y}` DESC), UNIQUE KEY b (`{k}`), "
            )),
            "{out}"
        );
        assert!(
            out.contains(&format!(
                "`{y}` INT GENERATED ALWAYS AS (year(d)) VIRTUAL INVISIBLE"
            )),
            "{out}"
        );
        assert!(
            out.contains(&format!("`{k}` VARCHAR(20) GENERATED ALWAYS AS (cast(j->>'$.k' as char(20))) VIRTUAL INVISIBLE")),
            "{out}"
        );
        // Unknown types leave the statement alone; plain keys too.
        assert_eq!(rw("CREATE TABLE t (j JSON, KEY ((j->>'$.a')))"), None);
        assert_eq!(rw("CREATE TABLE t (a INT, KEY (a), CHECK ((a > 0)))"), None);
    }

    #[test]
    fn create_index_becomes_alter_table() {
        let c = fx_column("cast(n as unsigned)");
        assert_eq!(
            rw("CREATE INDEX i ON t ((cast(n as unsigned)))").unwrap(),
            format!(
                "ALTER TABLE t ADD COLUMN IF NOT EXISTS `{c}` BIGINT UNSIGNED GENERATED ALWAYS AS (cast(n as unsigned)) VIRTUAL INVISIBLE, ADD INDEX `i` (`{c}`)"
            )
        );
        let m = fx_column("md5(x)");
        assert_eq!(
            rw("ALTER TABLE s.t ADD INDEX m ((md5(x))), ALGORITHM=INPLACE").unwrap(),
            format!(
                "ALTER TABLE s.t ADD COLUMN IF NOT EXISTS `{m}` CHAR(32) GENERATED ALWAYS AS (md5(x)) VIRTUAL INVISIBLE, ADD INDEX m (`{m}`), ALGORITHM=INPLACE"
            )
        );
        // A column's type is needed and not known here.
        assert_eq!(rw("CREATE INDEX i ON t ((lower(email)))"), None);
        assert_eq!(rw("CREATE INDEX i ON t (a, b)"), None);
    }

    #[test]
    fn arithmetic_on_integers() {
        let c = fx_column("a + b * 2");
        assert_eq!(
            rw("CREATE TABLE t (a INT, b BIGINT UNSIGNED, KEY ((a + b * 2)))").unwrap(),
            format!(
                "CREATE TABLE t (a INT, b BIGINT UNSIGNED, KEY (`{c}`), `{c}` BIGINT GENERATED ALWAYS AS (a + b * 2) VIRTUAL INVISIBLE)"
            )
        );
    }
}
