//! Settings in SQL: parsing `SPRIG SET`, `RESET`, `BOUNDS` and `SHOW SPRIG
//! SETTINGS`, including `ALTER TENANT <name>` for descendants.
//!
//! ```sql
//! ALTER TABLE orders SPRIG SET (auto_index.mode = 'recommend', auto_index.max_per_table = 8);
//! ALTER TABLE orders SPRIG RESET (auto_index.mode);
//! ALTER DATABASE shop SPRIG SET (doc.wildcard_index = 'on');
//! ALTER TENANT SPRIG SET BOUNDS (auto_index.max_per_table MAX 6,
//!                                auto_index.mode ALLOWED ('on', 'recommend'));
//! ALTER DATABASE shop SPRIG SET BOUNDS (doc.wildcard_index LOCKED);
//! ALTER TENANT SPRIG RESET BOUNDS (auto_index.max_per_table);
//! ALTER TENANT pune SPRIG SET (auth.password_lock_minutes = 30);   -- a descendant
//! SHOW SPRIG SETTINGS LIKE 'auto_index.%' FOR TABLE orders;
//! ```
//!
//! Grammar (keywords in any case):
//!
//! ```text
//! stmt     := alter | short | show
//! alter    := ALTER target SPRIG (SET | RESET) [BOUNDS] '(' items ')'
//! target   := TENANT [name] | (DATABASE | SCHEMA) [name] | TABLE [db '.'] name
//! short    := SPRIG (SET | RESET) [BOUNDS] (item | '(' items ')')     -- the tenant
//! show     := SHOW SPRIG SETTINGS [LIKE 'pattern'] [FOR target]
//! set item := key '=' value
//! bounds   := key { MIN number | MAX number | ALLOWED '(' value {, value} ')'
//!                 | LOCKED | UNLOCKED }
//! reset    := key
//! key      := word { '.' word }          -- dotted snake_case, or quoted
//! value    := string | number | word     -- TRUE, ON, recommend, ...
//! ```
//!
//! `SPRIG SET k = v` without `ALTER` is the tenant scope, as `ALTER TENANT
//! SPRIG SET (k = v)`. A database or table name may be omitted only where
//! noted: `ALTER DATABASE SPRIG ...` and `FOR DATABASE` mean the current
//! database. The parser only reads the statement; the gateway resolves
//! names against the session's catalog snapshot, parses each value with
//! its key's type ([`parse_value`]) and writes through the catalog's
//! `TenantSettingsWriter`.
//!
//! Not yet: `ALTER FUNCTION f SPRIG SET` (logic functions)
//! fails with 39051.

use sprig_settings::{AnySetting, Value, ValueKind};

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};
use crate::names::NameError;

/// What a settings statement addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsTarget {
    /// The session's tenant (`None`), or a tenant by name (a descendant,
    /// or the session's own tenant named).
    Tenant(Option<String>),
    /// A database by name; `None` for the session's current database.
    Database(Option<String>),
    /// A table (or collection, namespace, search index) in a database;
    /// `db` is `None` for the current database.
    Table {
        /// The logical database, when qualified.
        db: Option<String>,
        /// The table's name.
        name: String,
    },
}

/// A value as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    /// A quoted string.
    Text(String),
    /// A number or a bare word (`8`, `-1.5`, `on`, `TRUE`).
    Bare(String),
}

impl Literal {
    /// The text, without quotes.
    #[must_use]
    pub fn text(&self) -> &str {
        match self {
            Literal::Text(s) | Literal::Bare(s) => s,
        }
    }
}

/// The bounds one item of `SET BOUNDS` names; parts it doesn't name are
/// kept.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BoundsSpec {
    /// `MIN n`.
    pub min: Option<f64>,
    /// `MAX n`.
    pub max: Option<f64>,
    /// `ALLOWED (v, ...)`.
    pub allowed: Option<Vec<Literal>>,
    /// `LOCKED` (`Some(true)`) or `UNLOCKED` (`Some(false)`).
    pub locked: Option<bool>,
}

/// What changes.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingsOp {
    /// `SET (k = v, ...)`.
    Set(Vec<(String, Literal)>),
    /// `RESET (k, ...)`: clears the values.
    Reset(Vec<String>),
    /// `SET BOUNDS (k MAX 6, ...)`.
    SetBounds(Vec<(String, BoundsSpec)>),
    /// `RESET BOUNDS (k, ...)`: clears the bounds.
    ResetBounds(Vec<String>),
}

/// A parsed settings statement.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingsStmt {
    /// A write.
    Alter {
        /// Where.
        target: SettingsTarget,
        /// What.
        op: SettingsOp,
    },
    /// `SHOW SPRIG SETTINGS [LIKE 'p'] [FOR target]`.
    Show {
        /// The `LIKE` pattern.
        like: Option<String>,
        /// Where; the tenant without `FOR`.
        target: SettingsTarget,
    },
}

struct Parser<'a> {
    sql: &'a [u8],
    toks: Vec<&'a Token>,
    at: usize,
    nbe: bool,
}

fn unsupported(what: impl Into<String>) -> NameError {
    NameError::Unsupported(what.into())
}

/// A syntax error in MySQL's words (1064).
fn syntax(near: &str) -> NameError {
    NameError::Refused {
        errno: 1064,
        sqlstate: *b"42000",
        message: format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             server version for the right syntax to use near '{near}' at line 1"
        ),
    }
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.at).copied()
    }

    fn text(&self, t: &Token) -> String {
        String::from_utf8_lossy(t.bytes(self.sql)).into_owned()
    }

    fn near(&self) -> String {
        match self.peek() {
            Some(t) => {
                let s = String::from_utf8_lossy(&self.sql[t.start as usize..]);
                s.chars().take(80).collect()
            }
            None => String::new(),
        }
    }

    fn err(&self) -> NameError {
        syntax(&self.near())
    }

    fn is_word(&self, w: &str) -> bool {
        self.peek().is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.sql).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if self.is_word(w) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, w: &str) -> Result<(), NameError> {
        if self.eat_word(w) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    fn is_punct(&self, c: u8) -> bool {
        self.peek().is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op)
                && t.bytes(self.sql) == [c].as_slice()
        })
    }

    fn eat_punct(&mut self, c: u8) -> bool {
        if self.is_punct(c) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, c: u8) -> Result<(), NameError> {
        if self.eat_punct(c) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    fn at_end(&self) -> bool {
        self.peek().is_none() || (self.is_punct(b';') && self.at + 1 == self.toks.len())
    }

    /// An identifier: a word or a quoted identifier, unquoted.
    fn ident(&mut self) -> Result<String, NameError> {
        let t = self.peek().ok_or_else(|| self.err())?;
        match t.kind {
            TokenKind::Word | TokenKind::QuotedIdent => {
                self.at += 1;
                Ok(String::from_utf8_lossy(&ident_value(t.bytes(self.sql))).into_owned())
            }
            _ => Err(self.err()),
        }
    }

    /// An identifier that isn't one of the clause keywords following it.
    fn optional_name(&mut self) -> Result<Option<String>, NameError> {
        if self.is_word("SPRIG") || self.is_word("LIKE") || self.at_end() {
            return Ok(None);
        }
        self.ident().map(Some)
    }

    /// `word { . word }`, or one quoted identifier or string.
    fn key(&mut self) -> Result<String, NameError> {
        let t = self.peek().ok_or_else(|| self.err())?;
        if t.kind == TokenKind::String {
            self.at += 1;
            return string_value(t.bytes(self.sql), self.nbe)
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .ok_or_else(|| self.err());
        }
        let mut key = self.ident()?;
        while self.is_punct(b'.') {
            self.at += 1;
            key.push('.');
            key.push_str(&self.ident()?);
        }
        Ok(key)
    }

    fn number(&mut self) -> Result<f64, NameError> {
        let neg = self
            .peek()
            .is_some_and(|t| t.kind == TokenKind::Op && t.bytes(self.sql) == b"-".as_slice());
        if neg {
            self.at += 1;
        }
        let t = self.peek().ok_or_else(|| self.err())?;
        if t.kind != TokenKind::Number {
            return Err(self.err());
        }
        self.at += 1;
        let v: f64 = self.text(t).parse().map_err(|_| syntax(&self.text(t)))?;
        Ok(if neg { -v } else { v })
    }

    fn literal(&mut self) -> Result<Literal, NameError> {
        let t = self.peek().ok_or_else(|| self.err())?;
        match t.kind {
            TokenKind::String | TokenKind::NationalString => {
                self.at += 1;
                let raw = t.bytes(self.sql);
                let raw = if t.kind == TokenKind::NationalString {
                    &raw[1..]
                } else {
                    raw
                };
                string_value(raw, self.nbe)
                    .map(|v| Literal::Text(String::from_utf8_lossy(&v).into_owned()))
                    .ok_or_else(|| self.err())
            }
            TokenKind::Op if t.bytes(self.sql) == b"-".as_slice() => {
                self.at += 1;
                let n = self.peek().filter(|n| n.kind == TokenKind::Number);
                let n = n.ok_or_else(|| self.err())?;
                self.at += 1;
                Ok(Literal::Bare(format!("-{}", self.text(n))))
            }
            TokenKind::Word | TokenKind::Number => {
                self.at += 1;
                Ok(Literal::Bare(self.text(t)))
            }
            _ => Err(self.err()),
        }
    }

    /// A list in parentheses, or (when `bare` is allowed) one item.
    fn items<T>(
        &mut self,
        bare: bool,
        mut item: impl FnMut(&mut Self) -> Result<T, NameError>,
    ) -> Result<Vec<T>, NameError> {
        if !self.eat_punct(b'(') {
            if bare {
                let mut out = vec![item(self)?];
                while self.eat_punct(b',') {
                    out.push(item(self)?);
                }
                return Ok(out);
            }
            return Err(self.err());
        }
        let mut out = vec![item(self)?];
        while self.eat_punct(b',') {
            out.push(item(self)?);
        }
        self.expect_punct(b')')?;
        Ok(out)
    }

    fn set_item(&mut self) -> Result<(String, Literal), NameError> {
        let k = self.key()?;
        self.expect_punct(b'=')?;
        Ok((k, self.literal()?))
    }

    fn bounds_item(&mut self) -> Result<(String, BoundsSpec), NameError> {
        let k = self.key()?;
        let mut b = BoundsSpec::default();
        let mut any = false;
        loop {
            if self.eat_word("MIN") {
                b.min = Some(self.number()?);
            } else if self.eat_word("MAX") {
                b.max = Some(self.number()?);
            } else if self.eat_word("ALLOWED") {
                b.allowed = Some(self.items(false, Self::literal)?);
            } else if self.eat_word("LOCKED") {
                b.locked = Some(true);
            } else if self.eat_word("UNLOCKED") {
                b.locked = Some(false);
            } else {
                break;
            }
            any = true;
        }
        if !any {
            return Err(self.err());
        }
        Ok((k, b))
    }

    fn op(&mut self, bare: bool) -> Result<SettingsOp, NameError> {
        let set = if self.eat_word("SET") {
            true
        } else if self.eat_word("RESET") {
            false
        } else {
            return Err(self.err());
        };
        let bounds = self.eat_word("BOUNDS");
        Ok(match (set, bounds) {
            (true, false) => SettingsOp::Set(self.items(bare, Self::set_item)?),
            (false, false) => SettingsOp::Reset(self.items(bare, Self::key)?),
            (true, true) => SettingsOp::SetBounds(self.items(bare, Self::bounds_item)?),
            (false, true) => SettingsOp::ResetBounds(self.items(bare, Self::key)?),
        })
    }

    fn target(&mut self) -> Result<SettingsTarget, NameError> {
        if self.eat_word("TENANT") {
            return Ok(SettingsTarget::Tenant(self.optional_name()?));
        }
        if self.eat_word("DATABASE") || self.eat_word("SCHEMA") {
            return Ok(SettingsTarget::Database(self.optional_name()?));
        }
        if self.eat_word("TABLE") {
            let first = self.ident()?;
            if self.eat_punct(b'.') {
                return Ok(SettingsTarget::Table {
                    db: Some(first),
                    name: self.ident()?,
                });
            }
            return Ok(SettingsTarget::Table {
                db: None,
                name: first,
            });
        }
        if self.is_word("FUNCTION") {
            return Err(unsupported(
                "ALTER FUNCTION ... SPRIG SET (settings on logic functions aren't offered in this release)",
            ));
        }
        Err(self.err())
    }

    fn finish<T>(&self, v: T) -> Result<T, NameError> {
        if self.at_end() {
            Ok(v)
        } else {
            Err(self.err())
        }
    }
}

/// Whether `toks` (trivia removed) look like a settings statement: decided
/// from the first words, so every other statement costs one look.
fn is_settings(sql: &[u8], toks: &[&Token]) -> bool {
    let w = |i: usize, s: &str| {
        toks.get(i).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(s.as_bytes())
        })
    };
    // `SPRIG APPROVE|IGNORE|SNOOZE RECOMMENDATION` and `SHOW SPRIG
    // RECOMMENDATIONS` belong to `rec_stmt`.
    if crate::rec_stmt::is_rec(sql, toks) {
        return false;
    }
    if w(0, "SPRIG") {
        // `SPRIG PIN`, `UNPIN` and `SHOW` belong to `pin_stmt`.
        return !(w(1, "PIN") || w(1, "UNPIN") || w(1, "SHOW"));
    }
    if w(0, "SHOW") && w(1, "SPRIG") {
        return true;
    }
    if !w(0, "ALTER") {
        return false;
    }
    // ALTER TENANT ... SPRIG, ALTER DATABASE [x] SPRIG, ALTER TABLE x[.y] SPRIG,
    // ALTER FUNCTION f SPRIG: the SPRIG keyword within the next few tokens.
    if !(w(1, "TENANT") || w(1, "DATABASE") || w(1, "SCHEMA") || w(1, "TABLE") || w(1, "FUNCTION"))
    {
        return false;
    }
    (2..=5).any(|i| w(i, "SPRIG"))
}

/// Parses `sql` as a settings statement. `None` when it isn't one (the
/// caller plans it as ordinary SQL); `Some(Err)` for a settings statement
/// with a syntax error (1064) or an unsupported form (39051).
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<Result<SettingsStmt, NameError>> {
    let tokens = lex::lex(sql, opts).ok()?;
    let toks: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    if !is_settings(sql, &toks) {
        return None;
    }
    let mut p = Parser {
        sql,
        toks,
        at: 0,
        nbe: opts.no_backslash_escapes,
    };
    Some(parse_stmt(&mut p))
}

fn parse_stmt(p: &mut Parser<'_>) -> Result<SettingsStmt, NameError> {
    if p.eat_word("SHOW") {
        p.expect_word("SPRIG")?;
        p.expect_word("SETTINGS")?;
        let mut like = None;
        if p.eat_word("LIKE") {
            let t = p.peek().ok_or_else(|| p.err())?;
            if t.kind != TokenKind::String {
                return Err(p.err());
            }
            p.at += 1;
            like = Some(
                string_value(t.bytes(p.sql), p.nbe)
                    .map(|v| String::from_utf8_lossy(&v).into_owned())
                    .ok_or_else(|| p.err())?,
            );
        }
        let target = if p.eat_word("FOR") {
            p.target()?
        } else {
            SettingsTarget::Tenant(None)
        };
        return p.finish(SettingsStmt::Show { like, target });
    }
    if p.eat_word("SPRIG") {
        let op = p.op(true)?;
        return p.finish(SettingsStmt::Alter {
            target: SettingsTarget::Tenant(None),
            op,
        });
    }
    p.expect_word("ALTER")?;
    let target = p.target()?;
    p.expect_word("SPRIG")?;
    let op = p.op(false)?;
    p.finish(SettingsStmt::Alter { target, op })
}

/// A value written in SQL, parsed with `def`'s type (as the admin API does
/// for text): integers (`_` separators allowed), finite floats, strings as
/// given, booleans as `true`/`false`, `on`/`off`, `yes`/`no` or `1`/`0` in
/// any case.
///
/// # Errors
/// 39084 `ER_SPRIG_SETTING_BAD_VALUE` if the text isn't one.
pub fn parse_value(def: &dyn AnySetting, lit: &Literal) -> sprig_common::Result<Value> {
    let text = lit.text();
    let t = text.trim();
    let v = match def.kind() {
        ValueKind::Bool => match t.to_ascii_lowercase().as_str() {
            "true" | "on" | "yes" | "1" => Some(Value::Bool(true)),
            "false" | "off" | "no" | "0" => Some(Value::Bool(false)),
            _ => None,
        },
        ValueKind::Int => t.replace('_', "").parse::<i64>().ok().map(Value::Int),
        ValueKind::Float => t
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .map(Value::Float),
        ValueKind::Str => Some(Value::Str(text.to_owned())),
    };
    v.and_then(|v| def.normalize(&v)).ok_or_else(|| {
        sprig_common::SprigError::new(
            sprig_common::SprigCode::SettingBadValue,
            format!("{}: {text:?} is not a valid {:?}", def.key(), def.kind()),
        )
    })
}

/// `LIKE` matching as MySQL does it for `SHOW`: `%` any run, `_` one
/// character, `\` escapes; case-insensitive.
#[must_use]
pub fn like_matches(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('%', rest)) => (0..=t.len()).any(|i| go(rest, &t[i..])),
            Some(('_', rest)) => !t.is_empty() && go(rest, &t[1..]),
            Some(('\\', rest)) if !rest.is_empty() => {
                !t.is_empty() && t[0] == rest[0] && go(&rest[1..], &t[1..])
            }
            Some((c, rest)) => !t.is_empty() && t[0] == *c && go(rest, &t[1..]),
        }
    }
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let t: Vec<char> = text.to_lowercase().chars().collect();
    go(&p, &t)
}

#[cfg(test)]
mod tests;
