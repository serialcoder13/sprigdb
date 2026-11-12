//! The SQL extensions (chapter 32, "SQL extensions"; build step 2), read
//! from the token stream of chapter 07's lexer so they work on both
//! personas, and the classification of every statement the type layer
//! looks at.
//!
//! | Form | Read as |
//! |---|---|
//! | `CREATE TABLE t (...) INHERITS (p) [WHERE c = 'v'] [WITH (SPRIG_ABSTRACT = 1, SPRIG_STORAGE = 'joined', SPRIG_DISCRIMINATOR = 'c')]` | [`Stmt::CreateTable`] with the extension clauses cut out of the text |
//! | `CREATE TYPE ...` | [`Stmt::CreateType`] (refused in this release) |
//! | `ALTER TABLE t ADD HIERARCHY (parent) [ON DELETE RESTRICT \| CASCADE] [MAX DEPTH n]` | [`Stmt::AddHierarchy`] |
//! | `ALTER TABLE t DROP HIERARCHY` | [`Stmt::DropHierarchy`] |
//! | `ALTER TABLE t ADD DISCRIMINATOR (c) [VALUES (subtype = v, ...)]` | [`Stmt::AddDiscriminator`] |
//! | `ALTER TABLE t DROP DISCRIMINATOR` | [`Stmt::DropDiscriminator`] |
//! | `SET @sprig_inheritance = 'only' \| 'polymorphic' \| DEFAULT` | [`Stmt::SetInheritance`] |
//! | `ONLY t` in `FROM`, `UPDATE` and `DELETE` | the name `` `__sprig_only__t` `` before parsing ([`mark_only`]) |
//!
//! `ONLY` is not reserved in MySQL: `FROM only x` is table `only` with
//! alias `x`. So `ONLY x` is the extension only when `x` names a typed
//! table; otherwise the text is left alone.

use std::ops::Range;

use sprig_common::{SprigCode, SprigError};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex, string_value};

use crate::model::DiscValue;

/// The name prefix [`mark_only`] gives a table read with `ONLY`.
pub const ONLY_PREFIX: &str = "__sprig_only__";

/// A table name as written: optional database, name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Name {
    /// The database, if qualified.
    pub db: Option<String>,
    /// The table.
    pub name: String,
}

/// What a statement is, as far as types are concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `SELECT`, `WITH`, `(`, `VALUES`, `TABLE`.
    Query,
    /// `INSERT` or `REPLACE`.
    Insert,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// `CREATE TABLE`, with the extensions found.
    CreateTable(CreateTable),
    /// `CREATE TYPE`.
    CreateType,
    /// `CREATE VIEW` and other `CREATE`s.
    CreateOther,
    /// `ALTER TABLE t ADD HIERARCHY ...`.
    AddHierarchy(AddHierarchy),
    /// `ALTER TABLE t DROP HIERARCHY`.
    DropHierarchy(Name),
    /// `ALTER TABLE t ADD DISCRIMINATOR (c) [VALUES (...)]`: a hierarchy
    /// typed by `_type` takes a discriminator column.
    AddDiscriminator(AddDiscriminator),
    /// `ALTER TABLE t DROP DISCRIMINATOR`: back to `_type`.
    DropDiscriminator(Name),
    /// Another `ALTER TABLE`.
    AlterTable(AlterTable),
    /// `DROP TABLE`.
    DropTable(DropTable),
    /// `TRUNCATE`, `RENAME TABLE`.
    TruncateOrRename,
    /// `DROP DATABASE name`: its types and hierarchies go with it.
    DropDatabase(String),
    /// `SET @sprig_inheritance = ...`: `Some(true)` for `'only'`,
    /// `Some(false)` for `'polymorphic'`, `None` for `DEFAULT`.
    SetInheritance(Option<bool>),
    /// `SHOW CREATE TABLE t`.
    ShowCreate(Name),
    /// `SHOW [FULL] COLUMNS FROM t [FROM db] [LIKE | WHERE]`, `DESCRIBE t`.
    ShowColumns(ShowColumns),
    /// `SHOW [FULL] TABLES [FROM db] [LIKE | WHERE]`.
    ShowTables {
        /// The database, if named.
        db: Option<String>,
    },
    /// Anything else.
    Other,
}

/// `CREATE TABLE` and its extension clauses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    /// The new table.
    pub name: Name,
    /// `INHERITS (...)`: every parent named.
    pub inherits: Vec<Name>,
    /// `WITH (SPRIG_ABSTRACT = 1)`.
    pub is_abstract: bool,
    /// `WITH (SPRIG_STORAGE = '...')`, lower-cased.
    pub storage: Option<String>,
    /// `WITH (SPRIG_DISCRIMINATOR = 'c')`: the hierarchy's discriminator
    /// column.
    pub discriminator: Option<String>,
    /// `INHERITS (p) WHERE c = v`: the discriminator column and this
    /// subtype's value.
    pub disc_where: Option<(String, DiscValue)>,
    /// `CREATE TEMPORARY TABLE`.
    pub temporary: bool,
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// `CREATE TABLE ... LIKE` or `... SELECT`.
    pub like_or_select: bool,
    /// The statement without the extension clauses.
    pub plain: String,
}

impl CreateTable {
    /// Whether the statement uses any extension.
    #[must_use]
    pub fn is_typed(&self) -> bool {
        !self.inherits.is_empty()
            || self.is_abstract
            || self.storage.is_some()
            || self.discriminator.is_some()
            || self.disc_where.is_some()
    }
}

/// `ALTER TABLE t ADD DISCRIMINATOR (c) [VALUES (subtype = v, ...)]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddDiscriminator {
    /// The root.
    pub table: Name,
    /// The column.
    pub column: String,
    /// Values given per subtype; the others are read from the rows.
    pub values: Vec<(String, DiscValue)>,
}

/// `ALTER TABLE t ADD HIERARCHY (parent) ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddHierarchy {
    /// The table.
    pub table: Name,
    /// The parent columns.
    pub parent: Vec<String>,
    /// `ON DELETE CASCADE`.
    pub cascade: bool,
    /// `MAX DEPTH n`.
    pub max_depth: Option<u16>,
}

/// `ALTER TABLE t ...` (another form).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    /// The table.
    pub table: Name,
    /// The table name's byte range in the statement.
    pub name_span: Range<usize>,
    /// Columns added (`ADD [COLUMN] c ...`, `ADD [COLUMN] (c ..., ...)`).
    pub added: Vec<String>,
    /// Every clause adds columns (nothing else).
    pub only_add_columns: bool,
    /// Identifiers in clauses that drop, modify, change, rename or alter.
    pub changed_columns: Vec<String>,
    /// The statement renames the table.
    pub renames: bool,
    /// A clause adds, drops, changes or renames something (else: options
    /// such as `DISABLE KEYS` or `ENGINE`).
    pub changes: bool,
}

/// `DROP TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTable {
    /// The tables.
    pub tables: Vec<Name>,
    /// `IF EXISTS`.
    pub if_exists: bool,
    /// `CASCADE`.
    pub cascade: bool,
    /// `DROP TEMPORARY TABLE`.
    pub temporary: bool,
}

/// `SHOW COLUMNS` and its shorthands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowColumns {
    /// The table.
    pub table: Name,
    /// `FULL`.
    pub full: bool,
    /// The `LIKE '...'` or `WHERE ...` tail, as written.
    pub filter: Option<String>,
}

/// The tokens of a statement without trivia, over its text.
pub(crate) struct Toks<'a> {
    pub(crate) src: &'a [u8],
    pub(crate) tokens: Vec<Token>,
    pub(crate) lex: LexOptions,
}

impl<'a> Toks<'a> {
    #[allow(clippy::trivially_copy_pass_by_ref)] // as the lexer takes them
    pub(crate) fn new(src: &'a [u8], lex_opts: &LexOptions) -> Option<Self> {
        let tokens = lex(src, lex_opts)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        Some(Self {
            src,
            tokens,
            lex: *lex_opts,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.tokens.len()
    }

    pub(crate) fn kind(&self, i: usize) -> Option<TokenKind> {
        self.tokens.get(i).map(|t| t.kind)
    }

    pub(crate) fn bytes(&self, i: usize) -> &'a [u8] {
        self.tokens.get(i).map_or(&[], |t| t.bytes(self.src))
    }

    /// Whether token `i` is the keyword `w` (a bare word, any case).
    pub(crate) fn is(&self, i: usize, w: &str) -> bool {
        self.kind(i) == Some(TokenKind::Word) && self.bytes(i).eq_ignore_ascii_case(w.as_bytes())
    }

    pub(crate) fn punct(&self, i: usize, p: u8) -> bool {
        matches!(self.kind(i), Some(TokenKind::Punct | TokenKind::Op)) && self.bytes(i) == [p]
    }

    pub(crate) fn ident(&self, i: usize) -> Option<String> {
        self.kind(i)
            .filter(|k| k.is_ident_like())
            .map(|_| String::from_utf8_lossy(&ident_value(self.bytes(i))).into_owned())
    }

    pub(crate) fn string(&self, i: usize) -> Option<String> {
        (self.kind(i) == Some(TokenKind::String))
            .then(|| string_value(self.bytes(i), self.lex.no_backslash_escapes))
            .flatten()
            .map(|v| String::from_utf8_lossy(&v).into_owned())
    }

    pub(crate) fn start(&self, i: usize) -> usize {
        self.tokens
            .get(i)
            .map_or(self.src.len(), |t| t.start as usize)
    }

    pub(crate) fn end(&self, i: usize) -> usize {
        self.tokens
            .get(i)
            .map_or(self.src.len(), |t| t.end as usize)
    }

    /// A possibly qualified name at `i`: the name and the index after it.
    pub(crate) fn name(&self, i: usize) -> Option<(Name, usize)> {
        let first = self.ident(i)?;
        if self.punct(i + 1, b'.')
            && let Some(second) = self.ident(i + 2)
        {
            return Some((
                Name {
                    db: Some(first),
                    name: second,
                },
                i + 3,
            ));
        }
        Some((
            Name {
                db: None,
                name: first,
            },
            i + 1,
        ))
    }

    /// The index of the `)` matching the `(` at `open`.
    pub(crate) fn close_of(&self, open: usize) -> Option<usize> {
        let mut depth = 0usize;
        for i in open..self.len() {
            if self.punct(i, b'(') {
                depth += 1;
            } else if self.punct(i, b')') {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
        None
    }

    /// The text from token `a` to token `b` (inclusive).
    pub(crate) fn text(&self, a: usize, b: usize) -> String {
        if a > b || a >= self.len() {
            return String::new();
        }
        String::from_utf8_lossy(&self.src[self.start(a)..self.end(b.min(self.len() - 1))])
            .into_owned()
    }
}

fn unsupported(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::TypeUnsupported,
        format!("{what} is not supported"),
    )
}

/// Classifies `sql` (one statement).
///
/// # Errors
/// 39701 for `INHERITS` with two parents; 39710 for malformed or
/// unsupported extension clauses.
pub fn classify(sql: &[u8], lex_opts: &LexOptions) -> Result<Stmt, SprigError> {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return Ok(Stmt::Other);
    };
    let first = |w: &str| t.is(0, w);
    if first("SELECT") || first("WITH") || first("VALUES") || first("TABLE") || t.punct(0, b'(') {
        return Ok(Stmt::Query);
    }
    if first("INSERT") || first("REPLACE") {
        return Ok(Stmt::Insert);
    }
    if first("UPDATE") {
        return Ok(Stmt::Update);
    }
    if first("DELETE") {
        return Ok(Stmt::Delete);
    }
    if first("TRUNCATE") || first("RENAME") {
        return Ok(Stmt::TruncateOrRename);
    }
    if first("SET") {
        return Ok(set_inheritance(&t).unwrap_or(Stmt::Other));
    }
    if first("SHOW") || first("DESCRIBE") || first("DESC") || first("EXPLAIN") {
        return Ok(show(&t).unwrap_or(Stmt::Other));
    }
    if first("CREATE") {
        return create(&t, sql);
    }
    if first("ALTER") {
        return alter(&t);
    }
    if first("DROP") {
        return Ok(drop_table(&t).unwrap_or(Stmt::Other));
    }
    Ok(Stmt::Other)
}

fn set_inheritance(t: &Toks<'_>) -> Option<Stmt> {
    if t.kind(1) != Some(TokenKind::UserVar) {
        return None;
    }
    let var = String::from_utf8_lossy(&ident_value(t.bytes(1).get(1..)?)).to_lowercase();
    if var != "sprig_inheritance" {
        return None;
    }
    if !(t.punct(2, b'=') || t.bytes(2) == b":=") || t.len() != 4 {
        return Some(Stmt::Other);
    }
    if t.is(3, "DEFAULT") || t.is(3, "NULL") {
        return Some(Stmt::SetInheritance(None));
    }
    match t.string(3)?.to_lowercase().as_str() {
        "only" => Some(Stmt::SetInheritance(Some(true))),
        "polymorphic" => Some(Stmt::SetInheritance(Some(false))),
        _ => Some(Stmt::Other),
    }
}

fn show(t: &Toks<'_>) -> Option<Stmt> {
    if t.is(0, "SHOW") {
        let mut i = 1;
        if t.is(i, "CREATE") && t.is(i + 1, "TABLE") {
            let (name, next) = t.name(i + 2)?;
            return (next == t.len()).then_some(Stmt::ShowCreate(name));
        }
        let full = t.is(i, "FULL") || t.is(i, "EXTENDED");
        if full {
            i += 1;
            if t.is(i, "FULL") {
                i += 1;
            }
        }
        if t.is(i, "TABLES") {
            i += 1;
            let mut db = None;
            if t.is(i, "FROM") || t.is(i, "IN") {
                db = t.ident(i + 1);
            }
            return Some(Stmt::ShowTables { db });
        }
        if t.is(i, "COLUMNS") || t.is(i, "FIELDS") {
            i += 1;
            if !(t.is(i, "FROM") || t.is(i, "IN")) {
                return None;
            }
            let (mut table, mut next) = t.name(i + 1)?;
            if (t.is(next, "FROM") || t.is(next, "IN"))
                && let Some(db) = t.ident(next + 1)
            {
                table.db = Some(db);
                next += 2;
            }
            let filter = (next < t.len()).then(|| t.text(next, t.len() - 1));
            return Some(Stmt::ShowColumns(ShowColumns {
                table,
                full,
                filter,
            }));
        }
        return None;
    }
    // DESCRIBE t [col | 'wild'], also as DESC and EXPLAIN.
    let (table, next) = t.name(1)?;
    if next == t.len() {
        return Some(Stmt::ShowColumns(ShowColumns {
            table,
            full: false,
            filter: None,
        }));
    }
    if next + 1 == t.len() {
        let pat = t.string(next).or_else(|| t.ident(next))?;
        let quoted = pat.replace('\\', "\\\\").replace('\'', "''");
        return Some(Stmt::ShowColumns(ShowColumns {
            table,
            full: false,
            filter: Some(format!("LIKE '{quoted}'")),
        }));
    }
    None
}

fn create(t: &Toks<'_>, sql: &[u8]) -> Result<Stmt, SprigError> {
    let mut i = 1;
    if t.is(i, "OR") && t.is(i + 1, "REPLACE") {
        i += 2;
    }
    let temporary = t.is(i, "TEMPORARY");
    if temporary {
        i += 1;
    }
    if t.is(i, "TYPE") {
        return Ok(Stmt::CreateType);
    }
    if !t.is(i, "TABLE") {
        return Ok(Stmt::CreateOther);
    }
    i += 1;
    let if_not_exists = t.is(i, "IF") && t.is(i + 1, "NOT") && t.is(i + 2, "EXISTS");
    if if_not_exists {
        i += 3;
    }
    let Some((name, next)) = t.name(i) else {
        return Ok(Stmt::Other);
    };
    let mut ct = CreateTable {
        name,
        inherits: Vec::new(),
        is_abstract: false,
        storage: None,
        discriminator: None,
        disc_where: None,
        temporary,
        if_not_exists,
        like_or_select: false,
        plain: String::from_utf8_lossy(sql).into_owned(),
    };
    // The column list, if any; the extension clauses come after it.
    let after = if t.punct(next, b'(') {
        match t.close_of(next) {
            Some(c) => c + 1,
            None => return Ok(Stmt::CreateTable(ct)),
        }
    } else {
        next
    };
    let mut cut: Vec<Range<usize>> = Vec::new();
    let mut j = after;
    while j < t.len() {
        if t.is(j, "LIKE") || t.is(j, "SELECT") || t.is(j, "AS") {
            ct.like_or_select = true;
        }
        if t.is(j, "INHERITS") && t.punct(j + 1, b'(') {
            let close = t
                .close_of(j + 1)
                .ok_or_else(|| unsupported("an unterminated INHERITS clause"))?;
            let mut k = j + 2;
            while k < close {
                let (n, nk) = t
                    .name(k)
                    .ok_or_else(|| unsupported("INHERITS without a table name"))?;
                ct.inherits.push(n);
                k = nk;
                if t.punct(k, b',') {
                    k += 1;
                }
            }
            cut.push(t.start(j)..t.end(close));
            j = close + 1;
            continue;
        }
        // `INHERITS (p) WHERE c = v`: the discriminator. `WHERE` can't
        // follow a column list in MySQL's CREATE TABLE, except inside the
        // SELECT of `CREATE TABLE ... SELECT`, which is left alone.
        if t.is(j, "WHERE") && !ct.like_or_select {
            let col = t
                .ident(j + 1)
                .ok_or_else(|| unsupported("a WHERE clause other than WHERE column = value"))?;
            if !t.punct(j + 2, b'=') {
                return Err(unsupported(
                    "a WHERE clause other than WHERE column = value",
                ));
            }
            let (value, last) = disc_literal(&t, j + 3)?;
            if ct.disc_where.is_some() {
                return Err(unsupported("two WHERE clauses"));
            }
            ct.disc_where = Some((col, value));
            cut.push(t.start(j)..t.end(last));
            j = last + 1;
            continue;
        }
        if t.is(j, "WITH") && t.punct(j + 1, b'(') {
            let close = t
                .close_of(j + 1)
                .ok_or_else(|| unsupported("an unterminated WITH clause"))?;
            let mut k = j + 2;
            while k < close {
                let key = t
                    .ident(k)
                    .ok_or_else(|| unsupported("a WITH option without a name"))?
                    .to_uppercase();
                if !t.punct(k + 1, b'=') {
                    return Err(unsupported("a WITH option without a value"));
                }
                let v = k + 2;
                let value = t
                    .string(v)
                    .or_else(|| (t.kind(v) == Some(TokenKind::Number)).then(|| t.text(v, v)))
                    .or_else(|| t.ident(v))
                    .ok_or_else(|| unsupported("a WITH option value"))?;
                match key.as_str() {
                    "SPRIG_ABSTRACT" => {
                        ct.is_abstract = matches!(value.to_lowercase().as_str(), "1" | "true");
                    }
                    "SPRIG_STORAGE" => ct.storage = Some(value.to_lowercase()),
                    "SPRIG_DISCRIMINATOR" => ct.discriminator = Some(value),
                    other => return Err(unsupported(&format!("the table option {other}"))),
                }
                k = v + 1;
                if t.punct(k, b',') {
                    k += 1;
                }
            }
            cut.push(t.start(j)..t.end(close));
            j = close + 1;
            continue;
        }
        j += 1;
    }
    if ct.inherits.len() > 1 {
        return Err(SprigError::new(
            SprigCode::TypeMultipleInheritance,
            format!("Table '{}' may inherit from one parent only", ct.name.name),
        ));
    }
    if !cut.is_empty() {
        let mut out = Vec::with_capacity(sql.len());
        let mut pos = 0;
        for r in cut {
            out.extend_from_slice(&sql[pos..r.start]);
            out.push(b' ');
            pos = r.end;
        }
        out.extend_from_slice(&sql[pos..]);
        String::from_utf8_lossy(&out)
            .trim_end()
            .clone_into(&mut ct.plain);
    }
    Ok(Stmt::CreateTable(ct))
}

/// A discriminator value at token `j`: a string, a number or a negative
/// number; with its last token.
fn disc_literal(t: &Toks<'_>, j: usize) -> Result<(DiscValue, usize), SprigError> {
    if let Some(v) = t.string(j) {
        Ok((DiscValue::Str(v), j))
    } else if t.kind(j) == Some(TokenKind::Number) {
        Ok((DiscValue::Num(t.text(j, j)), j))
    } else if t.punct(j, b'-') && t.kind(j + 1) == Some(TokenKind::Number) {
        Ok((DiscValue::Num(format!("-{}", t.text(j + 1, j + 1))), j + 1))
    } else {
        Err(unsupported(
            "a discriminator value other than a string or a number",
        ))
    }
}

fn alter(t: &Toks<'_>) -> Result<Stmt, SprigError> {
    let mut i = 1;
    for w in ["ONLINE", "IGNORE"] {
        if t.is(i, w) {
            i += 1;
        }
    }
    if !t.is(i, "TABLE") {
        return Ok(Stmt::Other);
    }
    i += 1;
    if t.is(i, "IF") && t.is(i + 1, "EXISTS") {
        i += 2;
    }
    let name_at = i;
    let Some((table, mut i)) = t.name(i) else {
        return Ok(Stmt::Other);
    };
    let name_span = t.start(name_at)..t.end(i - 1);
    if t.is(i, "ADD") && t.is(i + 1, "HIERARCHY") {
        i += 2;
        if !t.punct(i, b'(') {
            return Err(unsupported("ADD HIERARCHY without a parent column list"));
        }
        let close = t
            .close_of(i)
            .ok_or_else(|| unsupported("an unterminated ADD HIERARCHY"))?;
        let mut parent = Vec::new();
        let mut k = i + 1;
        while k < close {
            parent.push(
                t.ident(k)
                    .ok_or_else(|| unsupported("ADD HIERARCHY with a non-column parent"))?,
            );
            k += 1;
            if t.punct(k, b',') {
                k += 1;
            }
        }
        let mut h = AddHierarchy {
            table,
            parent,
            cascade: false,
            max_depth: None,
        };
        let mut k = close + 1;
        while k < t.len() {
            if t.is(k, "ON") && t.is(k + 1, "DELETE") {
                if t.is(k + 2, "CASCADE") {
                    h.cascade = true;
                } else if !t.is(k + 2, "RESTRICT") {
                    return Err(unsupported("ON DELETE other than RESTRICT or CASCADE"));
                }
                k += 3;
            } else if t.is(k, "MAX") && t.is(k + 1, "DEPTH") {
                let n = std::str::from_utf8(t.bytes(k + 2))
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                    .ok_or_else(|| unsupported("MAX DEPTH without a number"))?;
                h.max_depth = Some(n);
                k += 3;
            } else {
                return Err(unsupported("this ADD HIERARCHY clause"));
            }
        }
        return Ok(Stmt::AddHierarchy(h));
    }
    if t.is(i, "DROP") && t.is(i + 1, "HIERARCHY") {
        return Ok(Stmt::DropHierarchy(table));
    }
    if t.is(i, "DROP") && t.is(i + 1, "DISCRIMINATOR") && i + 2 >= t.len() {
        return Ok(Stmt::DropDiscriminator(table));
    }
    if t.is(i, "ADD") && t.is(i + 1, "DISCRIMINATOR") {
        let bad = || unsupported("ADD DISCRIMINATOR (column) [VALUES (subtype = value, ...)]");
        let column = if t.punct(i + 2, b'(') && t.punct(i + 4, b')') {
            t.ident(i + 3).ok_or_else(bad)?
        } else {
            return Err(bad());
        };
        let mut d = AddDiscriminator {
            table,
            column,
            values: Vec::new(),
        };
        let mut k = i + 5;
        if t.is(k, "VALUES") {
            if !t.punct(k + 1, b'(') {
                return Err(bad());
            }
            let close = t.close_of(k + 1).ok_or_else(bad)?;
            k += 2;
            while k < close {
                let sub = t.ident(k).ok_or_else(bad)?;
                if !t.punct(k + 1, b'=') {
                    return Err(bad());
                }
                let (v, last) = disc_literal(t, k + 2)?;
                d.values.push((sub, v));
                k = last + 1;
                if t.punct(k, b',') {
                    k += 1;
                } else if k != close {
                    return Err(bad());
                }
            }
            k = close + 1;
        }
        if k < t.len() {
            return Err(bad());
        }
        return Ok(Stmt::AddDiscriminator(d));
    }
    let mut at = AlterTable {
        table,
        name_span,
        added: Vec::new(),
        renames: false,
        changes: false,
        only_add_columns: true,
        changed_columns: Vec::new(),
    };
    let mut clause_word: Option<String> = None;
    let mut depth = 0usize;
    let mut k = i;
    let mut clause_start = true;
    while k < t.len() {
        if t.punct(k, b'(') {
            depth += 1;
        } else if t.punct(k, b')') {
            depth = depth.saturating_sub(1);
        }
        if depth == 0 {
            if t.punct(k, b',') {
                clause_start = true;
                k += 1;
                continue;
            }
            if clause_start
                && t.is(k, "RENAME")
                && !t.is(k + 1, "COLUMN")
                && !t.is(k + 1, "INDEX")
                && !t.is(k + 1, "KEY")
            {
                at.renames = true;
            }
            if clause_start
                && ["ADD", "DROP", "MODIFY", "CHANGE", "RENAME", "ALTER"]
                    .iter()
                    .any(|w| t.is(k, w))
            {
                at.changes = true;
            }
            if clause_start {
                clause_word = Some(String::from_utf8_lossy(t.bytes(k)).to_uppercase());
                if !t.is(k, "ADD") {
                    at.only_add_columns = false;
                }
            }
            if clause_start && t.is(k, "ADD") {
                let mut c = k + 1;
                if t.is(c, "COLUMN") {
                    c += 1;
                }
                if t.is(c, "IF") && t.is(c + 1, "NOT") && t.is(c + 2, "EXISTS") {
                    c += 3;
                }
                let constraint = [
                    "PRIMARY",
                    "KEY",
                    "INDEX",
                    "UNIQUE",
                    "CONSTRAINT",
                    "FOREIGN",
                    "CHECK",
                    "FULLTEXT",
                    "SPATIAL",
                    "VECTOR",
                    "PERIOD",
                    "PARTITION",
                    "SYSTEM",
                ];
                if t.punct(c, b'(') {
                    // ADD [COLUMN] (c1 type, c2 type)
                    if let Some(close) = t.close_of(c) {
                        let mut m = c + 1;
                        let mut start = true;
                        let mut d = 0usize;
                        while m < close {
                            if t.punct(m, b'(') {
                                d += 1;
                            } else if t.punct(m, b')') {
                                d = d.saturating_sub(1);
                            } else if d == 0 && t.punct(m, b',') {
                                start = true;
                                m += 1;
                                continue;
                            }
                            if start {
                                if let Some(n) = t.ident(m) {
                                    at.added.push(n);
                                }
                                start = false;
                            }
                            m += 1;
                        }
                    }
                } else if !constraint.iter().any(|w| t.is(c, w))
                    && let Some(n) = t.ident(c)
                {
                    at.added.push(n);
                } else {
                    at.only_add_columns = false;
                }
            }
            clause_start = false;
        }
        // Columns named by a clause that drops, changes or renames.
        if !clause_start
            && clause_word
                .as_deref()
                .is_some_and(|w| ["DROP", "MODIFY", "CHANGE", "RENAME", "ALTER"].contains(&w))
            && let Some(n) = t.ident(k)
        {
            at.changed_columns.push(n);
        }
        k += 1;
    }
    Ok(Stmt::AlterTable(at))
}

fn drop_table(t: &Toks<'_>) -> Option<Stmt> {
    if t.is(1, "DATABASE") || t.is(1, "SCHEMA") {
        let i = if t.is(2, "IF") && t.is(3, "EXISTS") {
            4
        } else {
            2
        };
        return t.ident(i).map(Stmt::DropDatabase);
    }
    let mut i = 1;
    let temporary = t.is(i, "TEMPORARY");
    if temporary {
        i += 1;
    }
    if !t.is(i, "TABLE") && !t.is(i, "TABLES") {
        return None;
    }
    i += 1;
    let if_exists = t.is(i, "IF") && t.is(i + 1, "EXISTS");
    if if_exists {
        i += 2;
    }
    let mut tables = Vec::new();
    while let Some((n, next)) = t.name(i) {
        tables.push(n);
        i = next;
        if t.punct(i, b',') {
            i += 1;
        } else {
            break;
        }
    }
    let mut cascade = false;
    while i < t.len() {
        if t.is(i, "CASCADE") {
            cascade = true;
        } else if !(t.is(i, "RESTRICT")
            || t.is(i, "WAIT")
            || t.is(i, "NOWAIT")
            || t.kind(i) == Some(TokenKind::Number))
        {
            return None;
        }
        i += 1;
    }
    Some(Stmt::DropTable(DropTable {
        tables,
        if_exists,
        cascade,
        temporary,
    }))
}

/// Every identifier in `sql` (words and quoted identifiers), as values.
#[must_use]
pub fn identifiers(sql: &[u8], lex_opts: &LexOptions) -> Vec<String> {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return Vec::new();
    };
    (0..t.len()).filter_map(|i| t.ident(i)).collect()
}

/// The `SPRIG_*` functions of chapters 32 (hierarchies) and 42
/// (`SPRIG_ORIGIN`, data inheritance).
pub const SPRIG_FUNCTIONS: [&str; 6] = [
    "SPRIG_SUBTREE",
    "SPRIG_DESCENDANTS",
    "SPRIG_ANCESTORS",
    "SPRIG_DEPTH",
    "SPRIG_PATH",
    "SPRIG_ORIGIN",
];

/// Whether `sql` calls one of the [`SPRIG_FUNCTIONS`]: the bare word in
/// function-call position (followed by `(`). A database, table or column
/// that merely starts with `sprig_` (`sprig_dump_1`), or a quoted name,
/// doesn't count.
#[must_use]
pub fn calls_sprig_function(sql: &[u8], lex_opts: &LexOptions) -> bool {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return false;
    };
    (0..t.len()).any(|i| {
        t.kind(i) == Some(TokenKind::Word)
            && t.punct(i + 1, b'(')
            && !t.punct(i.wrapping_sub(1), b'.')
            && SPRIG_FUNCTIONS
                .iter()
                .any(|f| t.bytes(i).eq_ignore_ascii_case(f.as_bytes()))
    })
}

/// Whether `sql` names something reserved (`_sprig_` columns, `__sprig_`
/// objects): such a statement is left to chapter 07, which refuses it.
#[must_use]
pub fn names_reserved(sql: &[u8], lex_opts: &LexOptions) -> bool {
    identifiers(sql, lex_opts).iter().any(|n| {
        sprig_common::names::is_internal_column(n) || sprig_common::names::is_internal_object(n)
    })
}

/// Whether `sql` is a dump tool's table read, `SELECT /*!40001 SQL_NO_CACHE
/// */ * FROM t` (`mysqldump`) or with a column list (`mariadb-dump`): read
/// with `ONLY`, so each row is exported once.
#[must_use]
pub fn is_dump_read(sql: &[u8]) -> bool {
    let s = String::from_utf8_lossy(sql);
    let compact: String = s
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_uppercase();
    compact.starts_with("SELECT/*!40001SQL_NO_CACHE*/")
}

/// Replaces `ONLY x` with `` `__sprig_only__x` `` wherever `is_typed(x)`
/// says `x` (as `(db, name)`) is a typed table and `ONLY` follows `FROM`,
/// `JOIN`, `,`, `UPDATE` or `DELETE FROM` (chapter 32, "Resolving a table
/// reference", step 2). Returns `None` when nothing was marked.
#[must_use]
pub fn mark_only(
    sql: &[u8],
    lex_opts: &LexOptions,
    is_typed: &dyn Fn(Option<&str>, &str) -> bool,
) -> Option<String> {
    let t = Toks::new(sql, lex_opts)?;
    let mut edits: Vec<(Range<usize>, String)> = Vec::new();
    for i in 1..t.len() {
        if !t.is(i, "ONLY") {
            continue;
        }
        let prev_ok = t.is(i - 1, "FROM")
            || t.is(i - 1, "JOIN")
            || t.is(i - 1, "UPDATE")
            || t.punct(i - 1, b',');
        if !prev_ok {
            continue;
        }
        let Some((n, next)) = t.name(i + 1) else {
            continue;
        };
        if !is_typed(n.db.as_deref(), &n.name) {
            continue;
        }
        let marked = sprig_common::names::quote(&format!("{ONLY_PREFIX}{}", n.name));
        let repl = match &n.db {
            Some(db) => format!("{}.{marked}", sprig_common::names::quote(db)),
            None => marked,
        };
        edits.push((t.start(i)..t.end(next - 1), repl));
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + 32);
    let mut pos = 0;
    let src = String::from_utf8_lossy(sql);
    let src = src.as_bytes();
    for (r, s) in edits {
        out.push_str(&String::from_utf8_lossy(&src[pos..r.start]));
        out.push_str(&s);
        pos = r.end;
    }
    out.push_str(&String::from_utf8_lossy(&src[pos..]));
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(sql: &str) -> Stmt {
        classify(sql.as_bytes(), &LexOptions::mariadb()).unwrap()
    }

    #[test]
    fn create_table_extensions_are_cut_out() {
        let Stmt::CreateTable(ct) = c(
            "CREATE TABLE vehicle (vin CHAR(17) NOT NULL UNIQUE, embedding VECTOR(768)) INHERITS (asset)",
        ) else {
            panic!()
        };
        assert_eq!(ct.name.name, "vehicle");
        assert_eq!(ct.inherits[0].name, "asset");
        assert!(!ct.plain.contains("INHERITS"));
        assert!(ct.plain.ends_with("VECTOR(768))"));

        let Stmt::CreateTable(ct) = c(
            "create table `s`.`car` (doors int) engine=InnoDB inherits (`s`.vehicle) \
             WITH (SPRIG_ABSTRACT = 1, sprig_storage = 'joined')",
        ) else {
            panic!()
        };
        assert_eq!(ct.inherits[0].db.as_deref(), Some("s"));
        assert!(ct.is_abstract);
        assert_eq!(ct.storage.as_deref(), Some("joined"));
        assert!(ct.plain.contains("engine=InnoDB"));
        assert!(!ct.plain.to_lowercase().contains("with ("));

        let Stmt::CreateTable(ct) = c("CREATE TABLE plain (id INT PRIMARY KEY)") else {
            panic!()
        };
        assert!(!ct.is_typed());

        // The discriminator forms (chapter 32, discriminator columns).
        let Stmt::CreateTable(ct) = c(
            "CREATE TABLE truck (payload_kg DECIMAL(10,2)) INHERITS (vehicle) WHERE vtype = 'TRUCK' \
             WITH (SPRIG_STORAGE = 'single_table')",
        ) else {
            panic!()
        };
        assert_eq!(
            ct.disc_where,
            Some(("vtype".into(), DiscValue::Str("TRUCK".into())))
        );
        assert_eq!(ct.storage.as_deref(), Some("single_table"));
        assert_eq!(
            ct.plain.trim_end(),
            "CREATE TABLE truck (payload_kg DECIMAL(10,2))"
        );
        let Stmt::CreateTable(ct) =
            c("CREATE TABLE k (x INT) INHERITS (p) WHERE `kind` = -3 ENGINE=InnoDB")
        else {
            panic!()
        };
        assert_eq!(
            ct.disc_where,
            Some(("kind".into(), DiscValue::Num("-3".into())))
        );
        assert!(ct.plain.contains("ENGINE=InnoDB") && !ct.plain.contains("WHERE"));
        let Stmt::CreateTable(ct) = c(
            "CREATE TABLE vehicle (id INT PRIMARY KEY, vtype VARCHAR(20)) WITH (SPRIG_DISCRIMINATOR = 'vtype')",
        ) else {
            panic!()
        };
        assert_eq!(ct.discriminator.as_deref(), Some("vtype"));
        assert!(ct.is_typed());
        for bad in [
            "CREATE TABLE k (x INT) INHERITS (p) WHERE kind > 3",
            "CREATE TABLE k (x INT) INHERITS (p) WHERE kind = other",
            "CREATE TABLE k (x INT) INHERITS (p) WHERE kind = 'a' WHERE kind = 'b'",
        ] {
            let e = classify(bad.as_bytes(), &LexOptions::mariadb()).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::TypeUnsupported), "{bad}");
        }
        // CREATE TABLE ... SELECT keeps its WHERE.
        let Stmt::CreateTable(ct) = c("CREATE TABLE c2 SELECT * FROM a WHERE x = 1") else {
            panic!()
        };
        assert!(ct.disc_where.is_none() && ct.like_or_select);

        let e = classify(
            b"CREATE TABLE x (a INT) INHERITS (p, q)",
            &LexOptions::mariadb(),
        )
        .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::TypeMultipleInheritance));
        assert_eq!(
            c("CREATE TYPE money AS (amount DECIMAL(10,2))"),
            Stmt::CreateType
        );
    }

    #[test]
    fn hierarchy_clauses() {
        let Stmt::AddHierarchy(h) =
            c("ALTER TABLE org_unit ADD HIERARCHY (parent_id) ON DELETE RESTRICT MAX DEPTH 32")
        else {
            panic!()
        };
        assert_eq!(h.parent, ["parent_id"]);
        assert_eq!(h.max_depth, Some(32));
        assert!(!h.cascade);
        let Stmt::AddHierarchy(h) = c("alter table t add hierarchy (p) on delete cascade") else {
            panic!()
        };
        assert!(h.cascade);
        assert!(matches!(
            c("ALTER TABLE t DROP HIERARCHY"),
            Stmt::DropHierarchy(_)
        ));
        let Stmt::AlterTable(a) =
            c("ALTER TABLE t ADD COLUMN x INT, ADD y INT, ADD KEY (x), ADD (z INT, w INT)")
        else {
            panic!()
        };
        assert_eq!(a.added, ["x", "y", "z", "w"]);
        let Stmt::AlterTable(a) = c("ALTER TABLE t RENAME TO u") else {
            panic!()
        };
        assert!(a.renames);
        let Stmt::AlterTable(a) = c("ALTER TABLE t RENAME COLUMN a TO b") else {
            panic!()
        };
        assert!(!a.renames);
    }

    #[test]
    fn sessions_shows_and_drops() {
        assert_eq!(
            c("SET @sprig_inheritance = 'only'"),
            Stmt::SetInheritance(Some(true))
        );
        assert_eq!(
            c("set @sprig_inheritance := 'POLYMORPHIC'"),
            Stmt::SetInheritance(Some(false))
        );
        assert_eq!(
            c("SET @sprig_inheritance = DEFAULT"),
            Stmt::SetInheritance(None)
        );
        assert_eq!(c("SET @x = 1"), Stmt::Other);
        assert!(
            matches!(c("SHOW CREATE TABLE `vehicle`"), Stmt::ShowCreate(n) if n.name == "vehicle")
        );
        let Stmt::ShowColumns(s) = c("SHOW FULL COLUMNS FROM vehicle FROM shop LIKE 'v%'") else {
            panic!()
        };
        assert!(s.full);
        assert_eq!(s.table.db.as_deref(), Some("shop"));
        assert_eq!(s.filter.as_deref(), Some("LIKE 'v%'"));
        let Stmt::ShowColumns(s) = c("DESC vehicle vin") else {
            panic!()
        };
        assert_eq!(s.filter.as_deref(), Some("LIKE 'vin'"));
        assert!(
            matches!(c("SHOW FULL TABLES FROM shop"), Stmt::ShowTables { db: Some(d) } if d == "shop")
        );
        let Stmt::DropTable(d) = c("DROP TABLE IF EXISTS truck, vehicle CASCADE") else {
            panic!()
        };
        assert_eq!(d.tables.len(), 2);
        assert!(d.if_exists && d.cascade);
        assert_eq!(
            c("DROP DATABASE IF EXISTS shop"),
            Stmt::DropDatabase("shop".into())
        );
    }

    #[test]
    fn only_is_marked_for_typed_tables() {
        let typed = |_: Option<&str>, n: &str| n == "vehicle";
        let lex = LexOptions::mariadb();
        assert_eq!(
            mark_only(b"SELECT * FROM ONLY vehicle v WHERE 1", &lex, &typed).as_deref(),
            Some("SELECT * FROM `__sprig_only__vehicle` v WHERE 1")
        );
        assert_eq!(
            mark_only(b"UPDATE ONLY s.vehicle SET a = 1", &lex, &typed).as_deref(),
            Some("UPDATE `s`.`__sprig_only__vehicle` SET a = 1")
        );
        // `FROM only x`: table `only`, alias `x`.
        assert_eq!(mark_only(b"SELECT * FROM only x", &lex, &typed), None);
    }

    #[test]
    fn dump_reads_and_reserved_names() {
        assert!(is_dump_read(
            b"SELECT /*!40001 SQL_NO_CACHE */ * FROM `asset`"
        ));
        assert!(!is_dump_read(b"SELECT * FROM asset"));
        let lex = LexOptions::mariadb();
        assert!(names_reserved(b"SELECT _sprig_hpath FROM t", &lex));
        assert!(!names_reserved(b"SELECT _type FROM t", &lex));
    }
}
