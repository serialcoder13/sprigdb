//! MySQL 8.4's answers to DDL where MariaDB 11.8 answers differently. Each was
//! found by Sequelize's or Rails' suite and measured against MySQL 8.4.11.
//!
//! - **Long names** ([`long_name`]): a table, view, database or stored
//!   program named with more than 64 characters fails with MySQL's 1059
//!   (`Identifier name '...' is too long`). Before, the catalog refused
//!   the name first (1406) or MariaDB said 1102 or 1103. On 3307 a table
//!   or view gets MariaDB's own 1103.
//! - **Descending key parts of a non-InnoDB table** ([`engine_descending`],
//!   [`descending_index_table`]): MySQL's `MyISAM`, `MEMORY` and the other
//!   engines have no descending indexes and refuse one with 1178; MariaDB
//!   accepts it.
//! - **`DEFAULT (now())`** ([`now_defaults`]): MySQL keeps an expression
//!   default apart from the special `CURRENT_TIMESTAMP` default and shows
//!   it as `now()` with `DEFAULT_GENERATED`; MariaDB stores both as
//!   `current_timestamp()`. On 3306 a default that is exactly `(NOW())`,
//!   `(CURRENT_TIMESTAMP)`, `(LOCALTIME)` or `(LOCALTIMESTAMP)`, with or
//!   without a precision, is stored as MariaDB's `localtimestamp(n)`,
//!   which MariaDB keeps by that name and which means the same; output on
//!   3306 reads it `now(n)` ([`super::schema`]). A bare `LOCALTIME` or
//!   `LOCALTIMESTAMP` default or `ON UPDATE` becomes `CURRENT_TIMESTAMP`, as
//!   MySQL reads it (MariaDB reads `LOCALTIME(n)` as `curtime(n)`).
//! - **Dropping a column of a multi-column unique index**
//!   ([`dropped_columns`], [`unique_rebuild`]): MySQL takes the column out
//!   of the index; MariaDB refuses with 1072. The gateway reads the
//!   table's indexes and rebuilds each such index without the column in
//!   the same `ALTER TABLE`.
//! - **Foreign key errors** ([`fk_refs`], [`fk_missing_table`],
//!   [`fk_duplicate_name`]): MariaDB's 1005 (`errno: 150` or `121`)
//!   becomes MySQL's 1824 (the referenced table doesn't exist) or 1826 (a
//!   constraint of that name exists in the database), once the gateway has
//!   looked the names up.

use std::fmt::Write as _;

use super::errmap::WireError;
use crate::lex::{self, LexOptions, Token, TokenKind};
use sprig_proto_mysql::packets::Persona;

/// MySQL's longest identifier, in characters.
pub const MAX_IDENT: usize = 64;

/// MySQL 8.4's engines without descending indexes (lower case).
const NO_DESC_ENGINES: &[&str] = &[
    "myisam",
    "memory",
    "heap",
    "csv",
    "archive",
    "blackhole",
    "mrg_myisam",
    "merge",
    "federated",
];

/// The statement's tokens without trivia.
struct Toks<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
}

impl<'a> Toks<'a> {
    fn new(sql: &'a [u8], opts: LexOptions) -> Option<Self> {
        let t = lex::lex(sql, &opts)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        Some(Self { sql, t })
    }

    fn word(&self, j: usize, w: &[u8]) -> bool {
        self.t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w)
        })
    }

    fn any(&self, j: usize, ws: &[&[u8]]) -> bool {
        ws.iter().any(|w| self.word(j, w))
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && self.sql[tk.start as usize] == c)
    }

    fn op(&self, j: usize, s: &[u8]) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Op && tk.bytes(self.sql) == s)
    }

    fn ident(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&lex::ident_value(tk.bytes(self.sql))).into_owned())
    }

    /// A possibly qualified name at `j`: its parts and the index after it.
    fn qualified(&self, j: usize) -> Option<(Vec<String>, usize)> {
        let mut parts = vec![self.ident(j)?];
        let mut k = j + 1;
        while self.punct(k, b'.')
            && let Some(p) = self.ident(k + 1)
        {
            parts.push(p);
            k += 2;
        }
        Some((parts, k))
    }

    /// The statement's text of tokens `[a, b)`.
    fn text(&self, a: usize, b: usize) -> String {
        if a >= b {
            return String::new();
        }
        String::from_utf8_lossy(&self.sql[self.t[a].start as usize..self.t[b - 1].end as usize])
            .into_owned()
    }

    /// The `)` matching the `(` at `from`.
    fn matching(&self, from: usize) -> Option<usize> {
        let mut d = 0usize;
        for k in from..self.t.len() {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d = d.checked_sub(1)?;
                if d == 0 {
                    return Some(k);
                }
            }
        }
        None
    }

    /// `[a, b)` split at depth-0 commas.
    fn split(&self, a: usize, b: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut d = 0usize;
        let mut start = a;
        for k in a..b {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d = d.saturating_sub(1);
            } else if d == 0 && self.punct(k, b',') {
                out.push((start, k));
                start = k + 1;
            }
        }
        out.push((start, b));
        out
    }

    /// The first `(` in `[a, b)`.
    fn first_open(&self, a: usize, b: usize) -> Option<usize> {
        (a..b).find(|&k| self.punct(k, b'('))
    }

    /// Whether a key-part list `( ... )` opening at `open` has a part
    /// ending in `DESC`.
    fn has_desc_part(&self, open: usize) -> bool {
        let Some(close) = self.matching(open) else {
            return false;
        };
        self.split(open + 1, close)
            .iter()
            .any(|&(a, b)| b > a && self.word(b - 1, b"DESC"))
    }

    /// The index after `ALTER [ONLINE] [IGNORE] TABLE`, if that is how
    /// the statement starts.
    fn alter_table(&self) -> Option<usize> {
        if !self.word(0, b"ALTER") {
            return None;
        }
        let mut j = 1;
        while self.any(j, &[b"ONLINE", b"OFFLINE", b"IGNORE"]) {
            j += 1;
        }
        self.word(j, b"TABLE").then_some(j + 1)
    }

    /// `[IF EXISTS]` or `[IF NOT EXISTS]` at `j`: the index after it.
    fn skip_if(&self, j: usize) -> usize {
        if self.word(j, b"IF") && self.word(j + 1, b"NOT") && self.word(j + 2, b"EXISTS") {
            j + 3
        } else if self.word(j, b"IF") && self.word(j + 1, b"EXISTS") {
            j + 2
        } else {
            j
        }
    }
}

/// Whether `sql` starts with one of `words` (cheap check before lexing).
fn starts_with_any(sql: &[u8], words: &[&[u8]]) -> bool {
    let t = sql.trim_ascii_start();
    words
        .iter()
        .any(|w| t.len() > w.len() && t[..w.len()].eq_ignore_ascii_case(w))
}

/// MySQL's 1059 for `name`.
#[must_use]
pub fn too_long(name: &str) -> WireError {
    WireError::new(
        1059,
        *b"42000",
        format!("Identifier name '{name}' is too long"),
    )
}

/// The kind of object a long name names, for the MariaDB persona's error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameKind {
    Table,
    Database,
    Other,
}

/// The error for a table, view, database or stored program named with
/// more than [`MAX_IDENT`] characters in `CREATE`, `RENAME TABLE` or
/// `ALTER TABLE ... RENAME`: MySQL's 1059 on 3306; on 3307 MariaDB's 1103
/// for a table or view, its 1059 for a stored program, and `None` for a
/// database (MariaDB's 1102 already comes back). `None` when every name
/// fits.
#[must_use]
pub fn long_name(sql: &[u8], opts: &LexOptions) -> Option<WireError> {
    if !starts_with_any(sql, &[b"CREATE", b"RENAME", b"ALTER"]) {
        return None;
    }
    let s = Toks::new(sql, *opts)?;
    let mut found: Option<(String, NameKind)> = None;
    let long = |n: &str| n.chars().count() > MAX_IDENT;
    if s.word(0, b"CREATE") {
        // The object keyword within the statement's head.
        let mut j = 1;
        let kind = loop {
            if j > 16 || j >= s.t.len() || s.punct(j, b'(') {
                return None;
            }
            if s.any(j, &[b"TABLE", b"VIEW"]) {
                break NameKind::Table;
            }
            if s.any(j, &[b"DATABASE", b"SCHEMA"]) {
                break NameKind::Database;
            }
            if s.any(
                j,
                &[b"PROCEDURE", b"FUNCTION", b"TRIGGER", b"EVENT", b"SEQUENCE"],
            ) {
                break NameKind::Other;
            }
            if s.any(j, &[b"INDEX", b"USER", b"ROLE", b"SERVER", b"TABLESPACE"]) {
                return None;
            }
            j += 1;
        };
        let j = s.skip_if(j + 1);
        let (parts, _) = s.qualified(j)?;
        let name = parts.last()?;
        if long(name) {
            found = Some((name.clone(), kind));
        }
    } else if s.word(0, b"RENAME") && s.any(1, &[b"TABLE", b"TABLES"]) {
        let j = s.skip_if(2);
        for (a, b) in s.split(j, s.t.len()) {
            let Some(to) = (a..b).find(|&k| s.word(k, b"TO")) else {
                continue;
            };
            if let Some((parts, _)) = s.qualified(to + 1)
                && let Some(n) = parts.last()
                && long(n)
            {
                found = Some((n.clone(), NameKind::Table));
                break;
            }
        }
    } else if let Some(j) = s.alter_table() {
        let (_, after) = s.qualified(j)?;
        for (a, b) in s.split(after, s.t.len()) {
            if !s.word(a, b"RENAME") || s.any(a + 1, &[b"COLUMN", b"INDEX", b"KEY", b"CONSTRAINT"])
            {
                continue;
            }
            let k = if s.any(a + 1, &[b"TO", b"AS"]) {
                a + 2
            } else {
                a + 1
            };
            if k < b
                && let Some((parts, _)) = s.qualified(k)
                && let Some(n) = parts.last()
                && long(n)
            {
                found = Some((n.clone(), NameKind::Table));
                break;
            }
        }
    }
    let (name, kind) = found?;
    match (opts.persona, kind) {
        (Persona::MySql84, _) | (_, NameKind::Other) => Some(too_long(&name)),
        (_, NameKind::Table) => Some(WireError::new(
            1103,
            *b"42000",
            format!("Incorrect table name '{name}'"),
        )),
        (_, NameKind::Database) => None,
    }
}

/// MySQL's 1178 for a descending key part on an engine without them.
#[must_use]
pub fn no_descending() -> WireError {
    WireError::new(
        1178,
        *b"42000",
        "The storage engine for the table doesn't support descending indexes",
    )
}

/// Whether `engine` (as written) is one of MySQL's engines without
/// descending indexes.
#[must_use]
pub fn engine_lacks_descending(engine: &str) -> bool {
    NO_DESC_ENGINES.contains(&engine.to_ascii_lowercase().as_str())
}

/// 1178 for a `CREATE TABLE` whose `ENGINE` has no descending indexes
/// (`MyISAM`, `MEMORY`, ...) and that declares an index with a `DESC` part
/// (3306 only). `None` otherwise.
#[must_use]
pub fn engine_descending(sql: &[u8], opts: &LexOptions) -> Option<WireError> {
    if opts.persona != Persona::MySql84 || !starts_with_any(sql, &[b"CREATE"]) {
        return None;
    }
    let has = |n: &[u8]| sql.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n));
    if !has(b"DESC") || !has(b"ENGINE") {
        return None;
    }
    let s = Toks::new(sql, *opts)?;
    let mut j = 1;
    if s.word(j, b"TEMPORARY") {
        j += 1;
    }
    if !s.word(j, b"TABLE") {
        return None;
    }
    let open = s.first_open(j + 1, s.t.len())?;
    let close = s.matching(open)?;
    // `ENGINE [=] name` among the table options.
    let mut k = close + 1;
    let mut engine = None;
    while k < s.t.len() {
        if s.any(k, &[b"ENGINE", b"TYPE"]) {
            let v = if s.op(k + 1, b"=") { k + 2 } else { k + 1 };
            engine = s.ident(v).or_else(|| {
                s.t.get(v)
                    .filter(|t| t.kind == TokenKind::String)
                    .and_then(|t| lex::string_value(t.bytes(sql), opts.no_backslash_escapes))
                    .map(|v| String::from_utf8_lossy(&v).into_owned())
            });
            break;
        }
        k += 1;
    }
    if !engine.as_deref().is_some_and(engine_lacks_descending) {
        return None;
    }
    for (a, b) in s.split(open + 1, close) {
        let index = s.any(
            a,
            &[
                b"KEY",
                b"INDEX",
                b"UNIQUE",
                b"PRIMARY",
                b"FULLTEXT",
                b"SPATIAL",
            ],
        ) || (s.word(a, b"CONSTRAINT")
            && (a..b).any(|x| s.any(x, &[b"UNIQUE", b"PRIMARY"]))
            && !(a..b).any(|x| s.any(x, &[b"FOREIGN", b"CHECK"])));
        if index
            && let Some(o) = s.first_open(a, b)
            && s.has_desc_part(o)
        {
            return Some(no_descending());
        }
    }
    None
}

/// The table (as written) of a `CREATE INDEX` or `ALTER TABLE ... ADD
/// INDEX` (or `KEY`, `UNIQUE`, `PRIMARY KEY`) with a `DESC` key part, on
/// 3306: the gateway reads its engine, and refuses with
/// [`no_descending`] when [`engine_lacks_descending`]. `None` otherwise.
#[must_use]
pub fn descending_index_table(sql: &[u8], opts: &LexOptions) -> Option<String> {
    if opts.persona != Persona::MySql84 || !starts_with_any(sql, &[b"CREATE", b"ALTER"]) {
        return None;
    }
    if !sql.windows(4).any(|w| w.eq_ignore_ascii_case(b"DESC")) {
        return None;
    }
    let s = Toks::new(sql, *opts)?;
    if s.word(0, b"CREATE") {
        let mut j = 1;
        if s.word(j, b"OR") && s.word(j + 1, b"REPLACE") {
            j += 2;
        }
        while s.any(
            j,
            &[b"UNIQUE", b"FULLTEXT", b"SPATIAL", b"ONLINE", b"OFFLINE"],
        ) {
            j += 1;
        }
        if !s.word(j, b"INDEX") {
            return None;
        }
        let on = (j + 1..s.t.len()).find(|&k| s.word(k, b"ON"))?;
        let (_, after) = s.qualified(on + 1)?;
        let open = s.first_open(after, s.t.len())?;
        return s.has_desc_part(open).then(|| s.text(on + 1, after));
    }
    let j = s.alter_table()?;
    let (_, after) = s.qualified(j)?;
    for (a, b) in s.split(after, s.t.len()) {
        if !s.word(a, b"ADD") {
            continue;
        }
        let index = s.any(a + 1, &[b"KEY", b"INDEX", b"UNIQUE", b"PRIMARY"])
            || (s.word(a + 1, b"CONSTRAINT")
                && (a..b).any(|x| s.any(x, &[b"UNIQUE", b"PRIMARY"]))
                && !(a..b).any(|x| s.any(x, &[b"FOREIGN", b"CHECK"])));
        if index
            && let Some(o) = s.first_open(a, b)
            && s.has_desc_part(o)
        {
            return Some(s.text(j, after));
        }
    }
    None
}

/// The precision inside `( [n] )` at `j` and the index after it, or
/// `(None, j)` without parentheses.
fn precision(s: &Toks<'_>, j: usize) -> Option<(Option<String>, usize)> {
    if !s.punct(j, b'(') {
        return Some((None, j));
    }
    if s.punct(j + 1, b')') {
        return Some((None, j + 2));
    }
    let t = s.t.get(j + 1)?;
    if t.kind != TokenKind::Number || !s.punct(j + 2, b')') {
        return None;
    }
    Some((
        Some(String::from_utf8_lossy(t.bytes(s.sql)).into_owned()),
        j + 3,
    ))
}

/// `DEFAULT (NOW())` and its synonyms as `DEFAULT (localtimestamp())`,
/// and bare `LOCALTIME`/`LOCALTIMESTAMP` defaults as `CURRENT_TIMESTAMP`
/// (see the module docs); 3306's `CREATE` and `ALTER` only. `None` when
/// nothing changes.
#[must_use]
pub fn now_defaults(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if opts.persona != Persona::MySql84 || !starts_with_any(sql, &[b"CREATE", b"ALTER"]) {
        return None;
    }
    let has = |n: &[u8]| sql.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n));
    if !has(b"DEFAULT") && !has(b"UPDATE") {
        return None;
    }
    if !has(b"NOW") && !has(b"CURRENT_TIMESTAMP") && !has(b"LOCALTIME") {
        return None;
    }
    let s = Toks::new(sql, *opts)?;
    let now_words: &[&[u8]] = &[
        b"NOW",
        b"CURRENT_TIMESTAMP",
        b"LOCALTIMESTAMP",
        b"LOCALTIME",
    ];
    let local: &[&[u8]] = &[b"LOCALTIMESTAMP", b"LOCALTIME"];
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut k = 0;
    while k < s.t.len() {
        let after_kw = if s.word(k, b"DEFAULT") {
            Some((k + 1, true))
        } else if s.word(k, b"ON") && s.word(k + 1, b"UPDATE") {
            Some((k + 2, false))
        } else {
            None
        };
        let Some((v, is_default)) = after_kw else {
            k += 1;
            continue;
        };
        if is_default && s.punct(v, b'(') && s.any(v + 1, now_words) {
            // `DEFAULT ( NOW [( [n] )] )`.
            if let Some((p, next)) = precision(&s, v + 2)
                && s.punct(next, b')')
            {
                // `NOW` itself needs its parentheses.
                if !s.word(v + 1, b"NOW") || s.punct(v + 2, b'(') {
                    let n = p.unwrap_or_default();
                    edits.push((
                        s.t[v].start as usize,
                        s.t[next].end as usize,
                        format!("(localtimestamp({n}))"),
                    ));
                }
                k = next + 1;
                continue;
            }
        } else if s.any(v, local)
            && let Some((p, next)) = precision(&s, v + 1)
        {
            let text = match p {
                Some(n) => format!("CURRENT_TIMESTAMP({n})"),
                None => "CURRENT_TIMESTAMP".to_owned(),
            };
            edits.push((s.t[v].start as usize, s.t[next - 1].end as usize, text));
            k = next;
            continue;
        }
        k = v;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 32);
    let mut at = 0;
    for (from, to, text) in edits {
        out.extend_from_slice(&sql[at..from]);
        out.extend_from_slice(text.as_bytes());
        at = to;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// The columns an `ALTER TABLE` drops: the table as written, the byte
/// offset right after the table's name (where the gateway inserts its
/// index rebuilds), and the columns' names. `None` for other statements.
#[must_use]
pub fn dropped_columns(sql: &[u8], opts: &LexOptions) -> Option<(String, usize, Vec<String>)> {
    if opts.persona != Persona::MySql84 || !starts_with_any(sql, &[b"ALTER"]) {
        return None;
    }
    if !sql.windows(4).any(|w| w.eq_ignore_ascii_case(b"DROP")) {
        return None;
    }
    let s = Toks::new(sql, *opts)?;
    let j = s.alter_table()?;
    let (_, after) = s.qualified(j)?;
    if after <= j {
        return None;
    }
    let mut cols = Vec::new();
    for (a, b) in s.split(after, s.t.len()) {
        if !s.word(a, b"DROP") {
            continue;
        }
        let mut k = a + 1;
        if s.any(
            k,
            &[
                b"INDEX",
                b"KEY",
                b"PRIMARY",
                b"FOREIGN",
                b"CHECK",
                b"CONSTRAINT",
                b"PARTITION",
                b"SYSTEM",
                b"PERIOD",
            ],
        ) {
            continue;
        }
        if s.word(k, b"COLUMN") {
            k += 1;
        }
        k = s.skip_if(k);
        if k + 1 == b
            && let Some(c) = s.ident(k)
        {
            cols.push(c);
        }
    }
    if cols.is_empty() {
        return None;
    }
    Some((s.text(j, after), s.t[after - 1].end as usize, cols))
}

/// One key part of a table's index, as `information_schema.STATISTICS`
/// lists it on 3306.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPart {
    /// `INDEX_NAME`.
    pub index: String,
    /// `SEQ_IN_INDEX`.
    pub seq: u32,
    /// `COLUMN_NAME` (`None`: a functional key part).
    pub column: Option<String>,
    /// `SUB_PART`.
    pub sub_part: Option<u32>,
    /// `COLLATION = 'D'`.
    pub desc: bool,
    /// `NON_UNIQUE = 0`.
    pub unique: bool,
    /// `INDEX_TYPE`.
    pub index_type: String,
    /// `INDEX_COMMENT`.
    pub comment: String,
    /// `IS_VISIBLE = 'YES'`.
    pub visible: bool,
}

/// `sql` with each unique index (not the primary key) that holds a
/// dropped column and another column rebuilt without the dropped ones,
/// in the same `ALTER TABLE`: `DROP INDEX u, ADD UNIQUE INDEX u (...)`
/// inserted at `at` (from [`dropped_columns`]). `None` when no index
/// needs it, or one has a functional part (MariaDB's 1072 then stands).
#[must_use]
pub fn unique_rebuild(
    sql: &[u8],
    at: usize,
    dropped: &[String],
    parts: &[KeyPart],
    no_backslash_escapes: bool,
) -> Option<Vec<u8>> {
    let gone = |c: &str| dropped.iter().any(|d| d.eq_ignore_ascii_case(c));
    let mut names: Vec<&str> = Vec::new();
    for p in parts {
        if !names.contains(&p.index.as_str()) {
            names.push(&p.index);
        }
    }
    let q = |n: &str| format!("`{}`", n.replace('`', "``"));
    let mut clauses = String::new();
    for name in names {
        if name.eq_ignore_ascii_case("PRIMARY") {
            continue;
        }
        let mut ps: Vec<&KeyPart> = parts.iter().filter(|p| p.index == name).collect();
        ps.sort_by_key(|p| p.seq);
        let first = ps[0];
        if !first.unique {
            continue;
        }
        let hit = ps.iter().any(|p| p.column.as_deref().is_some_and(gone));
        if !hit {
            continue;
        }
        if ps.iter().any(|p| p.column.is_none()) {
            return None;
        }
        let kept: Vec<String> = ps
            .iter()
            .filter_map(|p| {
                let c = p.column.as_deref()?;
                if gone(c) {
                    return None;
                }
                let mut t = q(c);
                if let Some(n) = p.sub_part {
                    let _ = write!(t, "({n})");
                }
                if p.desc {
                    t.push_str(" DESC");
                }
                Some(t)
            })
            .collect();
        if kept.is_empty() {
            continue;
        }
        let _ = write!(
            clauses,
            " DROP INDEX {qn}, ADD UNIQUE INDEX {qn} ({})",
            kept.join(", "),
            qn = q(name)
        );
        if first.index_type.eq_ignore_ascii_case("HASH") {
            clauses.push_str(" USING HASH");
        }
        if let Some(c) = ps.iter().map(|p| &p.comment).find(|c| !c.is_empty()) {
            clauses.push_str(" COMMENT ");
            clauses.push_str(&crate::names::rewrite::sql_string(c, no_backslash_escapes));
        }
        if !first.visible {
            clauses.push_str(" IGNORED");
        }
        clauses.push(',');
    }
    if clauses.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + clauses.len());
    out.extend_from_slice(&sql[..at]);
    out.extend_from_slice(clauses.as_bytes());
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// The foreign keys of a `CREATE TABLE` or `ALTER TABLE`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FkRefs {
    /// The table the statement creates or alters (its name only).
    pub table: String,
    /// Referenced tables: database (if qualified) and name.
    pub refs: Vec<(Option<String>, String)>,
    /// Constraint names given with `CONSTRAINT name FOREIGN KEY`.
    pub names: Vec<String>,
}

/// The foreign keys `sql` declares; `None` when it declares none.
#[must_use]
pub fn fk_refs(sql: &[u8], opts: &LexOptions) -> Option<FkRefs> {
    if !starts_with_any(sql, &[b"CREATE", b"ALTER"]) {
        return None;
    }
    if !sql
        .windows(10)
        .any(|w| w.eq_ignore_ascii_case(b"REFERENCES"))
    {
        return None;
    }
    let s = Toks::new(sql, *opts)?;
    let table = if s.word(0, b"CREATE") {
        let mut j = 1;
        while s.any(j, &[b"OR", b"REPLACE", b"TEMPORARY"]) {
            j += 1;
        }
        if !s.word(j, b"TABLE") {
            return None;
        }
        let (parts, _) = s.qualified(s.skip_if(j + 1))?;
        parts.last()?.clone()
    } else {
        let j = s.alter_table()?;
        let (parts, _) = s.qualified(j)?;
        parts.last()?.clone()
    };
    let mut out = FkRefs {
        table,
        ..FkRefs::default()
    };
    for k in 0..s.t.len() {
        if s.word(k, b"REFERENCES")
            && let Some((mut parts, _)) = s.qualified(k + 1)
        {
            let t = parts.pop()?;
            out.refs.push((parts.pop(), t));
        } else if s.word(k, b"CONSTRAINT")
            && let Some(n) = s.ident(k + 1)
            && s.word(k + 2, b"FOREIGN")
        {
            out.names.push(n);
        }
    }
    (!out.refs.is_empty()).then_some(out)
}

/// Whether a MariaDB error is the 1005 of a foreign key whose referenced
/// table or columns are wrong (`errno: 150`).
#[must_use]
pub fn is_fk_formed_error(code: u16, message: &str) -> bool {
    code == 1005 && message.contains("errno: 150")
}

/// Whether a MariaDB error is the 1005 of a duplicate foreign key name
/// (`errno: 121`).
#[must_use]
pub fn is_fk_duplicate_error(code: u16, message: &str) -> bool {
    code == 1005 && message.contains("errno: 121")
}

/// MySQL's 1824 for a referenced table that doesn't exist.
#[must_use]
pub fn fk_missing_table(db: Option<&str>, table: &str) -> WireError {
    let name = match db {
        Some(d) => format!("{d}.{table}"),
        None => table.to_owned(),
    };
    WireError::new(
        1824,
        *b"HY000",
        format!("Failed to open the referenced table '{name}'"),
    )
}

/// MySQL's 1826 for a foreign key name the database already has.
#[must_use]
pub fn fk_duplicate_name(name: &str) -> WireError {
    WireError::new(
        1826,
        *b"HY000",
        format!("Duplicate foreign key constraint name '{name}'"),
    )
}

/// MySQL's (and MariaDB's) 1317 for `KILL QUERY` of the session's own
/// statement: the `KILL` itself is interrupted.
#[must_use]
pub fn interrupted() -> WireError {
    WireError::new(1317, *b"70100", "Query execution was interrupted")
}

#[cfg(test)]
mod tests;
