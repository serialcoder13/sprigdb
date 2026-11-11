//! The SQL extensions (chapter 42, "Interfaces"), read from the token
//! stream of chapter 07's lexer so they work on both personas, and the
//! classification of every statement the inheritance layer looks at.
//!
//! | Form | Read as |
//! |---|---|
//! | `CREATE SPACE s [UNDER p]` | [`Stmt::CreateSpace`] |
//! | `DROP SPACE [IF EXISTS] s [CASCADE]` | [`Stmt::DropSpace`] |
//! | `ALTER SPACE s SET HOME FOR u [, u ...]` | [`Stmt::SpaceHome`] (users' home spaces) |
//! | `USE SPACE s`, `SET @sprig_space = 's' \| DEFAULT` | [`Stmt::UseSpace`] |
//! | `ALTER TABLE t SPRIG INHERITABLE KEY (c, ...) [TAG FIELD f]` | [`Stmt::Inheritable`] |
//! | `ALTER TABLE t DROP SPRIG INHERITABLE` | [`Stmt::DropInheritable`] |
//! | `CREATE SHARE n ON t TO SPACE s \| TO ALL CHILDREN [WHERE e] [COLUMNS (c, ...)] [WITH RESHARE]` | [`Stmt::CreateShare`] |
//! | `ALTER SHARE n WHERE e`, `ALTER SHARE n TAKE WHERE e \| TAKE ALL` | [`Stmt::AlterShare`] |
//! | `DROP SHARE [IF EXISTS] n` | [`Stmt::DropShare`] |
//! | `SPRIG RESET t [(c, ...)] [WHERE e]` | [`Stmt::Reset`] |
//! | `SHOW SPACES`, `SHOW SHARES` | [`Stmt::ShowSpaces`], [`Stmt::ShowShares`] |
//! | `SPRIG CHECK IDS t` | [`Stmt::CheckIds`] (ids held by more than one tenant of the tree) |
//! | `SPRIG CHECK REFERENCES t [WHERE e]` | [`Stmt::CheckRefs`] (what refuses a delete of those rows) |
//! | `SHOW CREATE SHARE n` | [`Stmt::ShowCreateShare`] (with the share's foreign-key group) |
//! | `SHOW INHERITABLE TABLES` | [`Stmt::ShowInheritable`] (each table's origin level and group) |
//! | `CREATE TABLE t (... REFERENCES ...)` | [`Stmt::CreateTable`] (foreign keys onto inheritable tables) |

use sprig_common::{SprigCode, SprigError};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex, string_value};

/// A table name as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Name {
    /// The database, if qualified.
    pub db: Option<String>,
    /// The table.
    pub name: String,
}

/// Where `CREATE SHARE` sends the share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum To {
    /// `TO SPACE s` (a child space, or a child tenant by name).
    Space(String),
    /// `TO ALL CHILDREN`.
    AllChildren,
}

/// `ALTER SHARE`'s change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareChange {
    /// `WHERE e` (the parent's filter); `None` for `WHERE TRUE`-like
    /// removal is not special: the text is kept.
    Filter(String),
    /// `TAKE WHERE e` (the child's narrowing); `None` for `TAKE ALL`.
    Take(Option<String>),
}

/// What a statement is, as far as inheritance is concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `CREATE SPACE`.
    CreateSpace {
        /// The new space.
        name: String,
        /// Its parent (the session's space when absent).
        parent: Option<String>,
    },
    /// `DROP SPACE`.
    DropSpace {
        /// The space.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
        /// `CASCADE`.
        cascade: bool,
    },
    /// `ALTER SPACE s SET HOME FOR users`.
    SpaceHome {
        /// The space.
        space: String,
        /// The users' names.
        users: Vec<String>,
    },
    /// `USE SPACE s` or `SET @sprig_space`; `None`: back to the home space.
    UseSpace(Option<String>),
    /// `ALTER TABLE t SPRIG INHERITABLE KEY (...)`.
    Inheritable {
        /// The table.
        table: Name,
        /// The key columns.
        key: Vec<String>,
        /// `TAG FIELD`.
        tag: Option<String>,
    },
    /// `ALTER TABLE t DROP SPRIG INHERITABLE`.
    DropInheritable(Name),
    /// `CREATE SHARE`.
    CreateShare {
        /// The share's name.
        name: String,
        /// The object.
        table: Name,
        /// Where it goes.
        to: To,
        /// The row filter's text.
        filter: Option<String>,
        /// The shared columns.
        columns: Option<Vec<String>>,
        /// `WITH RESHARE`.
        reshare: bool,
    },
    /// `ALTER SHARE`.
    AlterShare {
        /// The share.
        name: String,
        /// What changes.
        change: ShareChange,
    },
    /// `DROP SHARE`.
    DropShare {
        /// The share.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `SPRIG RESET`.
    Reset {
        /// The object.
        table: Name,
        /// Only these fields' overrides.
        columns: Option<Vec<String>>,
        /// Which records (all of the space's when absent).
        filter: Option<String>,
    },
    /// `SHOW SPACES`.
    ShowSpaces,
    /// `SHOW SHARES`.
    ShowShares,
    /// `SELECT`, `WITH`, `(`, `VALUES`, `TABLE`.
    Query,
    /// `INSERT`.
    Insert,
    /// `REPLACE`.
    Replace,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// Another `ALTER TABLE` (the guard of "Schema changes while an object
    /// is shared").
    AlterTable(Name),
    /// `DROP TABLE`.
    DropTable(Vec<Name>),
    /// `DROP DATABASE`.
    DropDatabase(String),
    /// `TRUNCATE [TABLE] t`.
    Truncate(Name),
    /// `RENAME TABLE a TO b, ...`: the old names.
    Rename(Vec<Name>),
    /// `SHOW CREATE TABLE t`.
    ShowCreate(Name),
    /// `LOAD DATA ... INTO TABLE t`.
    LoadData(Name),
    /// `CREATE VIEW`, routines, triggers, `CREATE TABLE ... LIKE | SELECT`.
    CreateOther,
    /// `CREATE TABLE t (...)` with a foreign key (`REFERENCES`): the
    /// table joins the group of an inheritable table it references
    /// ([`crate::links`]).
    CreateTable(Name),
    /// `SPRIG CHECK IDS t`: the ids of `t` that more than one tenant of
    /// its tree holds as its own ([`crate::ids`]).
    CheckIds(Name),
    /// `SPRIG CHECK REFERENCES t [WHERE e]`: the rows and tenants whose
    /// references refuse a delete of `t`'s rows ([`crate::links`]).
    CheckRefs {
        /// The referenced table.
        table: Name,
        /// Which rows (every row of the session's view when absent).
        filter: Option<String>,
    },
    /// `SHOW CREATE SHARE n`.
    ShowCreateShare(String),
    /// `SHOW INHERITABLE TABLES`.
    ShowInheritable,
    /// Anything else.
    Other,
}

/// The tokens of a statement without trivia, over its text.
pub(crate) struct Toks<'a> {
    src: &'a [u8],
    tokens: Vec<Token>,
    lex: LexOptions,
}

impl<'a> Toks<'a> {
    #[allow(clippy::trivially_copy_pass_by_ref)]
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

    /// An identifier or a string (user and space names).
    fn ident_or_string(&self, i: usize) -> Option<String> {
        self.ident(i).or_else(|| self.string(i))
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

    /// `(a, b, ...)` at `open`: the names and the index after `)`.
    pub(crate) fn ident_list(&self, open: usize) -> Option<(Vec<String>, usize)> {
        if !self.punct(open, b'(') {
            return None;
        }
        let close = self.close_of(open)?;
        let mut out = Vec::new();
        let mut i = open + 1;
        while i < close {
            out.push(self.ident(i)?);
            i += 1;
            if self.punct(i, b',') {
                i += 1;
            } else if i != close {
                return None;
            }
        }
        (!out.is_empty()).then_some((out, close + 1))
    }

    /// The first index at or after `from` at parenthesis depth 0 where
    /// `stop` holds, or the end.
    pub(crate) fn until(&self, from: usize, stop: impl Fn(usize) -> bool) -> usize {
        let mut depth = 0usize;
        for i in from..self.len() {
            if self.punct(i, b'(') {
                depth += 1;
            } else if self.punct(i, b')') {
                depth = depth.saturating_sub(1);
            } else if depth == 0 && stop(i) {
                return i;
            }
        }
        self.len()
    }
}

fn syntax(what: &str) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!("You have an error in your SQL syntax: {what}"),
    )
}

/// Classifies `sql` (one statement).
///
/// # Errors
/// 1064 for a malformed extension statement.
pub fn classify(sql: &[u8], lex_opts: &LexOptions) -> Result<Stmt, SprigError> {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return Ok(Stmt::Other);
    };
    let first = |w: &str| t.is(0, w);
    if first("SELECT") || first("WITH") || first("VALUES") || first("TABLE") || t.punct(0, b'(') {
        return Ok(Stmt::Query);
    }
    // EXPLAIN [FORMAT=...] SELECT: the plan of the merged view.
    if (first("EXPLAIN") || first("DESCRIBE") || first("DESC"))
        && (1..t.len().min(5)).any(|i| t.is(i, "SELECT") || t.is(i, "WITH"))
    {
        return Ok(Stmt::Query);
    }
    if first("INSERT") {
        return Ok(Stmt::Insert);
    }
    if first("REPLACE") {
        return Ok(Stmt::Replace);
    }
    if first("UPDATE") {
        return Ok(Stmt::Update);
    }
    if first("DELETE") {
        return Ok(Stmt::Delete);
    }
    if first("USE") && t.is(1, "SPACE") && t.len() == 3 {
        let s = t
            .ident_or_string(2)
            .ok_or_else(|| syntax("USE SPACE needs a space name"))?;
        return Ok(Stmt::UseSpace(Some(s)));
    }
    if first("SET") {
        return Ok(set_space(&t).unwrap_or(Stmt::Other));
    }
    if first("SPRIG") && t.is(1, "RESET") {
        return reset(&t);
    }
    if first("SPRIG") && t.is(1, "CHECK") && t.is(2, "IDS") {
        return match t.name(3) {
            Some((n, next)) if next == t.len() => Ok(Stmt::CheckIds(n)),
            _ => Err(syntax("SPRIG CHECK IDS table")),
        };
    }
    if first("SHOW") && t.len() == 2 {
        if t.is(1, "SPACES") {
            return Ok(Stmt::ShowSpaces);
        }
        if t.is(1, "SHARES") {
            return Ok(Stmt::ShowShares);
        }
    }
    if first("SPRIG") && t.is(1, "CHECK") && t.is(2, "REFERENCES") {
        let (table, i) = t
            .name(3)
            .ok_or_else(|| syntax("SPRIG CHECK REFERENCES table [WHERE condition]"))?;
        let filter = if t.is(i, "WHERE") && i + 1 < t.len() {
            Some(t.text(i + 1, t.len() - 1))
        } else if i == t.len() {
            None
        } else {
            return Err(syntax("SPRIG CHECK REFERENCES table [WHERE condition]"));
        };
        return Ok(Stmt::CheckRefs { table, filter });
    }
    if first("SHOW") && t.is(1, "CREATE") && t.is(2, "SHARE") {
        return match t.ident_or_string(3) {
            Some(n) if t.len() == 4 => Ok(Stmt::ShowCreateShare(n)),
            _ => Err(syntax("SHOW CREATE SHARE name")),
        };
    }
    if first("SHOW") && t.len() == 3 && t.is(1, "INHERITABLE") && t.is(2, "TABLES") {
        return Ok(Stmt::ShowInheritable);
    }
    if first("SHOW") && t.is(1, "CREATE") && t.is(2, "TABLE") {
        if let Some((n, next)) = t.name(3)
            && next == t.len()
        {
            return Ok(Stmt::ShowCreate(n));
        }
        return Ok(Stmt::Other);
    }
    if first("CREATE") {
        return create(&t);
    }
    if first("ALTER") {
        return alter(&t);
    }
    if first("DROP") {
        return drop(&t);
    }
    if first("TRUNCATE") {
        let i = if t.is(1, "TABLE") { 2 } else { 1 };
        return Ok(t.name(i).map_or(Stmt::Other, |(n, _)| Stmt::Truncate(n)));
    }
    if first("RENAME") && t.is(1, "TABLE") {
        let mut names = Vec::new();
        let mut i = 2;
        while let Some((n, next)) = t.name(i) {
            names.push(n);
            // `a TO b`, then `,` or the end.
            let after = t.until(next, |j| t.punct(j, b','));
            i = after + 1;
            if after >= t.len() {
                break;
            }
        }
        return Ok(Stmt::Rename(names));
    }
    if first("LOAD") && (t.is(1, "DATA") || t.is(1, "XML")) {
        let into = t.until(1, |j| t.is(j, "TABLE") && t.is(j - 1, "INTO"));
        return Ok(t
            .name(into + 1)
            .map_or(Stmt::Other, |(n, _)| Stmt::LoadData(n)));
    }
    Ok(Stmt::Other)
}

fn set_space(t: &Toks<'_>) -> Option<Stmt> {
    if t.kind(1) != Some(TokenKind::UserVar) {
        return None;
    }
    let var = String::from_utf8_lossy(&ident_value(t.bytes(1).get(1..)?)).to_lowercase();
    if var != "sprig_space" {
        return None;
    }
    if !(t.punct(2, b'=') || t.bytes(2) == b":=") || t.len() != 4 {
        return None;
    }
    if t.is(3, "DEFAULT") || t.is(3, "NULL") {
        return Some(Stmt::UseSpace(None));
    }
    t.string(3).map(|s| Stmt::UseSpace(Some(s)))
}

fn reset(t: &Toks<'_>) -> Result<Stmt, SprigError> {
    let (table, mut i) = t
        .name(2)
        .ok_or_else(|| syntax("SPRIG RESET needs a table"))?;
    let mut columns = None;
    if t.punct(i, b'(') {
        let (cols, next) = t
            .ident_list(i)
            .ok_or_else(|| syntax("SPRIG RESET's column list"))?;
        columns = Some(cols);
        i = next;
    }
    let filter = if t.is(i, "WHERE") {
        if i + 1 >= t.len() {
            return Err(syntax("SPRIG RESET ... WHERE needs a condition"));
        }
        Some(t.text(i + 1, t.len() - 1))
    } else if i == t.len() {
        None
    } else {
        return Err(syntax("SPRIG RESET t [(columns)] [WHERE condition]"));
    };
    Ok(Stmt::Reset {
        table,
        columns,
        filter,
    })
}

fn create(t: &Toks<'_>) -> Result<Stmt, SprigError> {
    if t.is(1, "SPACE") {
        let name = t
            .ident(2)
            .ok_or_else(|| syntax("CREATE SPACE needs a name"))?;
        if t.len() == 3 {
            return Ok(Stmt::CreateSpace { name, parent: None });
        }
        if t.is(3, "UNDER")
            && t.len() == 5
            && let Some(p) = t.ident(4)
        {
            return Ok(Stmt::CreateSpace {
                name,
                parent: Some(p),
            });
        }
        return Err(syntax("CREATE SPACE name [UNDER parent]"));
    }
    if t.is(1, "SHARE") {
        return create_share(t);
    }
    // CREATE INDEX: additive, forwarded (a UNIQUE one would need widening
    // with the space, so it is refused when it names an inheritable
    // object). Views, routines, triggers and tables (LIKE, SELECT or a
    // foreign key) that name one are refused.
    if t.is(1, "INDEX") || ((t.is(1, "FULLTEXT") || t.is(1, "SPATIAL")) && t.is(2, "INDEX")) {
        return Ok(Stmt::Other);
    }
    if t.is(1, "DATABASE") || t.is(1, "SCHEMA") || t.is(1, "USER") || t.is(1, "ROLE") {
        return Ok(Stmt::Other);
    }
    // A plain CREATE TABLE names no other table.
    let mut i = 1;
    if t.is(i, "OR") && t.is(i + 1, "REPLACE") {
        i += 2;
    }
    if t.is(i, "TEMPORARY") {
        i += 1;
    }
    if t.is(i, "TABLE") {
        let copies = (i..t.len()).any(|j| t.is(j, "LIKE") || t.is(j, "SELECT") || t.is(j, "AS"));
        let refers = (i..t.len()).any(|j| t.is(j, "REFERENCES"));
        if !copies && !refers {
            return Ok(Stmt::Other);
        }
        if !copies {
            let mut j = i + 1;
            if t.is(j, "IF") && t.is(j + 1, "NOT") && t.is(j + 2, "EXISTS") {
                j += 3;
            }
            if let Some((n, _)) = t.name(j) {
                return Ok(Stmt::CreateTable(n));
            }
        }
    }
    Ok(Stmt::CreateOther)
}

fn create_share(t: &Toks<'_>) -> Result<Stmt, SprigError> {
    let usage = "CREATE SHARE name ON table TO SPACE s | TO ALL CHILDREN [WHERE condition] \
                 [COLUMNS (c, ...)] [WITH RESHARE]";
    let name = t.ident(2).ok_or_else(|| syntax(usage))?;
    if !t.is(3, "ON") {
        return Err(syntax(usage));
    }
    let (table, mut i) = t.name(4).ok_or_else(|| syntax(usage))?;
    if !t.is(i, "TO") {
        return Err(syntax(usage));
    }
    let to = if t.is(i + 1, "SPACE") {
        let s = t.ident_or_string(i + 2).ok_or_else(|| syntax(usage))?;
        i += 3;
        To::Space(s)
    } else if t.is(i + 1, "ALL") && t.is(i + 2, "CHILDREN") {
        i += 3;
        To::AllChildren
    } else {
        return Err(syntax(usage));
    };
    let mut filter = None;
    let mut columns = None;
    let mut reshare = false;
    while i < t.len() {
        if t.is(i, "WHERE") {
            let end = t.until(i + 1, |j| {
                (t.is(j, "COLUMNS") && t.punct(j + 1, b'('))
                    || (t.is(j, "WITH") && t.is(j + 1, "RESHARE"))
            });
            if end == i + 1 {
                return Err(syntax(usage));
            }
            filter = Some(t.text(i + 1, end - 1));
            i = end;
        } else if t.is(i, "COLUMNS") {
            let (cols, next) = t.ident_list(i + 1).ok_or_else(|| syntax(usage))?;
            columns = Some(cols);
            i = next;
        } else if t.is(i, "WITH") && t.is(i + 1, "RESHARE") {
            reshare = true;
            i += 2;
        } else {
            return Err(syntax(usage));
        }
    }
    Ok(Stmt::CreateShare {
        name,
        table,
        to,
        filter,
        columns,
        reshare,
    })
}

fn alter(t: &Toks<'_>) -> Result<Stmt, SprigError> {
    if t.is(1, "SPACE") {
        let usage = "ALTER SPACE s SET HOME FOR user [, user ...]";
        let space = t.ident(2).ok_or_else(|| syntax(usage))?;
        if !(t.is(3, "SET") && t.is(4, "HOME") && t.is(5, "FOR")) {
            return Err(syntax(usage));
        }
        let mut users = Vec::new();
        let mut i = 6;
        while i < t.len() {
            users.push(t.ident_or_string(i).ok_or_else(|| syntax(usage))?);
            i += 1;
            // `'bob'@'%'`: the host part is ignored (one user per name).
            if t.kind(i) == Some(TokenKind::UserVar) || t.punct(i, b'@') {
                i += if t.punct(i, b'@') { 2 } else { 1 };
            }
            if t.punct(i, b',') {
                i += 1;
            } else if i != t.len() {
                return Err(syntax(usage));
            }
        }
        if users.is_empty() {
            return Err(syntax(usage));
        }
        return Ok(Stmt::SpaceHome { space, users });
    }
    if t.is(1, "SHARE") {
        let usage = "ALTER SHARE name WHERE condition | TAKE WHERE condition | TAKE ALL";
        let name = t.ident(2).ok_or_else(|| syntax(usage))?;
        if t.is(3, "WHERE") && t.len() > 4 {
            return Ok(Stmt::AlterShare {
                name,
                change: ShareChange::Filter(t.text(4, t.len() - 1)),
            });
        }
        if t.is(3, "TAKE") {
            if t.is(4, "ALL") && t.len() == 5 {
                return Ok(Stmt::AlterShare {
                    name,
                    change: ShareChange::Take(None),
                });
            }
            if t.is(4, "WHERE") && t.len() > 5 {
                return Ok(Stmt::AlterShare {
                    name,
                    change: ShareChange::Take(Some(t.text(5, t.len() - 1))),
                });
            }
        }
        return Err(syntax(usage));
    }
    let mut i = 1;
    if t.is(i, "ONLINE") || t.is(i, "IGNORE") {
        i += 1;
    }
    if !t.is(i, "TABLE") {
        return Ok(Stmt::Other);
    }
    i += 1;
    if t.is(i, "IF") && t.is(i + 1, "EXISTS") {
        i += 2;
    }
    let Some((table, next)) = t.name(i) else {
        return Ok(Stmt::Other);
    };
    if t.is(next, "SPRIG") && t.is(next + 1, "INHERITABLE") {
        let usage = "ALTER TABLE t SPRIG INHERITABLE KEY (c, ...) [TAG FIELD f]";
        if !t.is(next + 2, "KEY") {
            return Err(syntax(usage));
        }
        let (key, mut j) = t.ident_list(next + 3).ok_or_else(|| syntax(usage))?;
        let mut tag = None;
        if t.is(j, "TAG") && t.is(j + 1, "FIELD") {
            tag = Some(t.ident(j + 2).ok_or_else(|| syntax(usage))?);
            j += 3;
        }
        if j != t.len() {
            return Err(syntax(usage));
        }
        return Ok(Stmt::Inheritable { table, key, tag });
    }
    if t.is(next, "DROP") && t.is(next + 1, "SPRIG") && t.is(next + 2, "INHERITABLE") {
        return Ok(Stmt::DropInheritable(table));
    }
    Ok(Stmt::AlterTable(table))
}

fn drop(t: &Toks<'_>) -> Result<Stmt, SprigError> {
    let if_exists = |i: usize| t.is(i, "IF") && t.is(i + 1, "EXISTS");
    if t.is(1, "SPACE") {
        let mut i = 2;
        let ie = if_exists(i);
        if ie {
            i += 2;
        }
        let name = t.ident(i).ok_or_else(|| syntax("DROP SPACE name"))?;
        let cascade = t.is(i + 1, "CASCADE");
        if i + 1 + usize::from(cascade) != t.len() {
            return Err(syntax("DROP SPACE [IF EXISTS] name [CASCADE]"));
        }
        return Ok(Stmt::DropSpace {
            name,
            if_exists: ie,
            cascade,
        });
    }
    if t.is(1, "SHARE") {
        let mut i = 2;
        let ie = if_exists(i);
        if ie {
            i += 2;
        }
        let name = t.ident(i).ok_or_else(|| syntax("DROP SHARE name"))?;
        if i + 1 != t.len() {
            return Err(syntax("DROP SHARE [IF EXISTS] name"));
        }
        return Ok(Stmt::DropShare {
            name,
            if_exists: ie,
        });
    }
    if t.is(1, "DATABASE") || t.is(1, "SCHEMA") {
        let i = if if_exists(2) { 4 } else { 2 };
        return Ok(t.ident(i).map_or(Stmt::Other, Stmt::DropDatabase));
    }
    let mut i = 1;
    if t.is(i, "TEMPORARY") {
        return Ok(Stmt::Other);
    }
    if !t.is(i, "TABLE") {
        return Ok(Stmt::Other);
    }
    i += 1;
    if if_exists(i) {
        i += 2;
    }
    let mut names = Vec::new();
    while let Some((n, next)) = t.name(i) {
        names.push(n);
        if t.punct(next, b',') {
            i = next + 1;
        } else {
            break;
        }
    }
    Ok(Stmt::DropTable(names))
}

/// Whether `sql` is one of this crate's statements that chapter 08's
/// settings grammar would otherwise claim (`ALTER TABLE t SPRIG
/// INHERITABLE ...`, and `SPRIG RESET t ...` naming a table rather than a
/// setting): the gateway leaves them to [`crate::handle`].
#[must_use]
pub fn is_extension(sql: &[u8], lex_opts: &LexOptions) -> bool {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return false;
    };
    if t.is(0, "ALTER") && t.is(1, "TABLE") {
        let i = if t.is(2, "IF") && t.is(3, "EXISTS") {
            4
        } else {
            2
        };
        if let Some((_, next)) = t.name(i) {
            return (t.is(next, "SPRIG") && t.is(next + 1, "INHERITABLE"))
                || (t.is(next, "DROP")
                    && t.is(next + 1, "SPRIG")
                    && t.is(next + 2, "INHERITABLE"));
        }
        return false;
    }
    if t.is(0, "SPRIG") && t.is(1, "CHECK") && (t.is(2, "IDS") || t.is(2, "REFERENCES")) {
        return true;
    }
    if t.is(0, "SPRIG") && t.is(1, "RESET") && !t.is(2, "BOUNDS") && !t.punct(2, b'(') {
        let Some((n, _)) = t.name(2) else {
            return false;
        };
        let key = match &n.db {
            Some(d) => format!("{d}.{}", n.name),
            None => n.name.clone(),
        };
        let setting = sprig_settings::Registry::global()
            .ok()
            .and_then(|r| r.get(&key.to_ascii_lowercase()))
            .is_some();
        return !setting;
    }
    false
}

/// Every possibly qualified name in `sql` (`a`, `a.b`).
#[must_use]
pub fn names(sql: &[u8], lex_opts: &LexOptions) -> Vec<Name> {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut i = 0;
    while i < t.len() {
        match t.name(i) {
            Some((n, next)) => {
                out.push(n);
                i = next;
            }
            None => i += 1,
        }
    }
    out
}

/// Every identifier in `sql`, unquoted.
#[must_use]
pub fn identifiers(sql: &[u8], lex_opts: &LexOptions) -> Vec<String> {
    let Some(t) = Toks::new(sql, lex_opts) else {
        return Vec::new();
    };
    (0..t.len()).filter_map(|i| t.ident(i)).collect()
}

/// Whether the text names a reserved (`_sprig_` or `__sprig_`) name: such
/// a client statement is left to chapter 07, which refuses it.
#[must_use]
pub fn names_reserved(sql: &[u8], lex_opts: &LexOptions) -> bool {
    identifiers(sql, lex_opts)
        .iter()
        .any(|i| i.to_ascii_lowercase().starts_with("_sprig_") || i.starts_with("__sprig_"))
}

/// The error for a malformed or unsupported form.
#[must_use]
pub fn unsupported(what: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::InheritStatementUnsupported, what.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(sql: &str) -> Stmt {
        classify(sql.as_bytes(), &LexOptions::mariadb()).unwrap()
    }

    #[test]
    fn extension_statements() {
        assert_eq!(
            c("CREATE SPACE pune UNDER acme"),
            Stmt::CreateSpace {
                name: "pune".into(),
                parent: Some("acme".into())
            }
        );
        assert_eq!(
            c("drop space if exists `pune` cascade"),
            Stmt::DropSpace {
                name: "pune".into(),
                if_exists: true,
                cascade: true
            }
        );
        assert_eq!(
            c("USE SPACE `r6-inh-a`"),
            Stmt::UseSpace(Some("r6-inh-a".into()))
        );
        assert_eq!(
            c("SET @sprig_space = 'pune'"),
            Stmt::UseSpace(Some("pune".into()))
        );
        assert_eq!(c("SET @sprig_space = DEFAULT"), Stmt::UseSpace(None));
        assert_eq!(c("USE space"), Stmt::Other);
        assert_eq!(
            c("ALTER TABLE shop.products SPRIG INHERITABLE KEY (id) TAG FIELD tags"),
            Stmt::Inheritable {
                table: Name {
                    db: Some("shop".into()),
                    name: "products".into()
                },
                key: vec!["id".into()],
                tag: Some("tags".into())
            }
        );
        assert_eq!(
            c(
                "CREATE SHARE in_products ON products TO SPACE acme WHERE region = 'IN' AND \
               (price < 10 OR price > 20) COLUMNS (name, colour) WITH RESHARE"
            ),
            Stmt::CreateShare {
                name: "in_products".into(),
                table: Name {
                    db: None,
                    name: "products".into()
                },
                to: To::Space("acme".into()),
                filter: Some(
                    "region = 'IN' AND \n               (price < 10 OR price > 20)"
                        .replace("\n               ", "")
                ),
                columns: Some(vec!["name".into(), "colour".into()]),
                reshare: true
            }
        );
        assert_eq!(
            c("CREATE SHARE a ON p TO ALL CHILDREN"),
            Stmt::CreateShare {
                name: "a".into(),
                table: Name {
                    db: None,
                    name: "p".into()
                },
                to: To::AllChildren,
                filter: None,
                columns: None,
                reshare: false
            }
        );
        assert_eq!(
            c("ALTER SHARE a TAKE WHERE price < 100"),
            Stmt::AlterShare {
                name: "a".into(),
                change: ShareChange::Take(Some("price < 100".into()))
            }
        );
        assert_eq!(
            c("ALTER SHARE a TAKE ALL"),
            Stmt::AlterShare {
                name: "a".into(),
                change: ShareChange::Take(None)
            }
        );
        assert_eq!(
            c("SPRIG CHECK REFERENCES shop.t2 WHERE id IN (1, 2)"),
            Stmt::CheckRefs {
                table: Name {
                    db: Some("shop".into()),
                    name: "t2".into()
                },
                filter: Some("id IN (1, 2)".into())
            }
        );
        assert_eq!(c("SHOW CREATE SHARE s1"), Stmt::ShowCreateShare("s1".into()));
        assert_eq!(c("show inheritable tables"), Stmt::ShowInheritable);
        assert!(is_extension(
            b"SPRIG CHECK REFERENCES t2",
            &LexOptions::mariadb()
        ));
        assert_eq!(
            c("SPRIG RESET products (colour) WHERE id = 2"),
            Stmt::Reset {
                table: Name {
                    db: None,
                    name: "products".into()
                },
                columns: Some(vec!["colour".into()]),
                filter: Some("id = 2".into())
            }
        );
        assert_eq!(
            c("ALTER SPACE pune SET HOME FOR 'bob'@'%', carol"),
            Stmt::SpaceHome {
                space: "pune".into(),
                users: vec!["bob".into(), "carol".into()]
            }
        );
        assert_eq!(c("SHOW SPACES"), Stmt::ShowSpaces);
        assert!(classify(b"CREATE SHARE x ON t", &LexOptions::mariadb()).is_err());
    }

    #[test]
    fn plain_statements() {
        assert_eq!(
            c("ALTER TABLE products ADD COLUMN sku INT NULL"),
            Stmt::AlterTable(Name {
                db: None,
                name: "products".into()
            })
        );
        assert_eq!(
            c("RENAME TABLE a TO b, c TO d"),
            Stmt::Rename(vec![
                Name {
                    db: None,
                    name: "a".into()
                },
                Name {
                    db: None,
                    name: "c".into()
                }
            ])
        );
        assert_eq!(
            c("DROP TABLE IF EXISTS a, s.b"),
            Stmt::DropTable(vec![
                Name {
                    db: None,
                    name: "a".into()
                },
                Name {
                    db: Some("s".into()),
                    name: "b".into()
                }
            ])
        );
        assert_eq!(
            c("TRUNCATE TABLE p"),
            Stmt::Truncate(Name {
                db: None,
                name: "p".into()
            })
        );
        assert_eq!(c("REPLACE INTO p VALUES (1)"), Stmt::Replace);
        assert!(names_reserved(
            b"SELECT _sprig_space FROM p",
            &LexOptions::mariadb()
        ));
        let l = LexOptions::mariadb();
        assert!(is_extension(
            b"ALTER TABLE s.p SPRIG INHERITABLE KEY (id)",
            &l
        ));
        assert!(!is_extension(
            b"ALTER TABLE p SPRIG SET (auto_index.mode = 'off')",
            &l
        ));
        assert!(is_extension(b"SPRIG RESET products WHERE id = 2", &l));
        assert!(!is_extension(b"SPRIG RESET (auto_index.mode)", &l));
    }
}
