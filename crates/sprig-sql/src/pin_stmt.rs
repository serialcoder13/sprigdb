//! Sharding pins and advice in SQL: parsing `SPRIG PIN`, `SPRIG UNPIN`, `SPRIG
//! SHOW ADVICE` and `SPRIG SHOW PINS`.
//!
//! ```sql
//! SPRIG PIN orders SHARD KEY (customer_id);
//! SPRIG PIN events COLOCATE WITH customers ON (customer_id);
//! SPRIG PIN countries REFERENCE;
//! SPRIG PIN audit_log UNSHARDED;
//! SPRIG UNPIN orders;
//! SPRIG SHOW ADVICE;            -- the latest advisor run: design, scores, verdict
//! SPRIG SHOW PINS;              -- every pin of the tenant
//! ```
//!
//! Grammar (keywords in any case):
//!
//! ```text
//! stmt   := SPRIG PIN table spec | SPRIG UNPIN table
//!         | SPRIG SHOW ADVICE | SPRIG SHOW PINS
//! spec   := SHARD KEY cols | COLOCATE WITH table ON cols | REFERENCE | UNSHARDED
//! table  := name ['.' name]          -- [database.]table
//! cols   := '(' name {',' name} ')'
//! ```
//!
//! The parser only reads the statement. The gateway resolves names against
//! the session's catalog snapshot (an unqualified `COLOCATE WITH` table is
//! in the pinned table's database, as an unqualified `REFERENCES` table is),
//! checks `ALTER` on each named table, applies the conflict rules and
//! stores the pin in the tenant's catalog.

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value};
use crate::names::NameError;

/// A table as written: `[db.]name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinTable {
    /// The database, when qualified.
    pub db: Option<String>,
    /// The table.
    pub name: String,
}

/// What a pin fixes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinSpec {
    /// `SHARD KEY (cols)`: the table routes by exactly these columns.
    ShardKey(Vec<String>),
    /// `COLOCATE WITH parent ON (cols)`: the table joins the parent's
    /// group; its `cols` hold the group's key.
    Colocate {
        /// The table to co-locate with.
        parent: PinTable,
        /// The pinned table's columns holding the key.
        columns: Vec<String>,
    },
    /// `REFERENCE`: a full copy everywhere (unsharded).
    Reference,
    /// `UNSHARDED`: stays in its database's default group.
    Unsharded,
}

/// A parsed pin statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinStmt {
    /// `SPRIG PIN table spec`.
    Pin {
        /// The pinned table.
        table: PinTable,
        /// The pin.
        spec: PinSpec,
    },
    /// `SPRIG UNPIN table`.
    Unpin {
        /// The table.
        table: PinTable,
    },
    /// `SPRIG SHOW ADVICE`.
    ShowAdvice,
    /// `SPRIG SHOW PINS`.
    ShowPins,
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

struct Parser<'a> {
    sql: &'a [u8],
    toks: Vec<&'a Token>,
    at: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.at).copied()
    }

    fn err(&self) -> NameError {
        let near = match self.peek() {
            Some(t) => String::from_utf8_lossy(&self.sql[t.start as usize..])
                .chars()
                .take(80)
                .collect(),
            None => String::new(),
        };
        syntax(&near)
    }

    fn is_word(&self, w: &str) -> bool {
        self.peek().is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.sql).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn eat_word(&mut self, w: &str) -> bool {
        let yes = self.is_word(w);
        if yes {
            self.at += 1;
        }
        yes
    }

    fn expect_word(&mut self, w: &str) -> Result<(), NameError> {
        if self.eat_word(w) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    fn eat_punct(&mut self, c: u8) -> bool {
        let yes = self.peek().is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op)
                && t.bytes(self.sql) == [c].as_slice()
        });
        if yes {
            self.at += 1;
        }
        yes
    }

    fn expect_punct(&mut self, c: u8) -> Result<(), NameError> {
        if self.eat_punct(c) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

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

    fn table(&mut self) -> Result<PinTable, NameError> {
        let first = self.ident()?;
        if self.eat_punct(b'.') {
            return Ok(PinTable {
                db: Some(first),
                name: self.ident()?,
            });
        }
        Ok(PinTable {
            db: None,
            name: first,
        })
    }

    fn columns(&mut self) -> Result<Vec<String>, NameError> {
        self.expect_punct(b'(')?;
        let mut out = vec![self.ident()?];
        while self.eat_punct(b',') {
            out.push(self.ident()?);
        }
        self.expect_punct(b')')?;
        Ok(out)
    }

    fn finish<T>(&mut self, v: T) -> Result<T, NameError> {
        self.eat_punct(b';');
        if self.peek().is_none() {
            Ok(v)
        } else {
            Err(self.err())
        }
    }
}

/// Whether the first words are `SPRIG PIN`, `SPRIG UNPIN` or `SPRIG SHOW`.
fn is_pin(sql: &[u8], toks: &[&Token]) -> bool {
    let w = |i: usize, s: &str| {
        toks.get(i).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(s.as_bytes())
        })
    };
    w(0, "SPRIG") && (w(1, "PIN") || w(1, "UNPIN") || w(1, "SHOW"))
}

/// Parses `sql` as a pin statement. `None` when it isn't one (decided from
/// the first two words); `Some(Err)` with 1064 for a syntax error.
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<Result<PinStmt, NameError>> {
    let tokens = lex::lex(sql, opts).ok()?;
    let toks: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    if !is_pin(sql, &toks) {
        return None;
    }
    let mut p = Parser { sql, toks, at: 1 };
    Some(parse_stmt(&mut p))
}

fn parse_stmt(p: &mut Parser<'_>) -> Result<PinStmt, NameError> {
    if p.eat_word("SHOW") {
        if p.eat_word("ADVICE") {
            return p.finish(PinStmt::ShowAdvice);
        }
        p.expect_word("PINS")?;
        return p.finish(PinStmt::ShowPins);
    }
    if p.eat_word("UNPIN") {
        let table = p.table()?;
        return p.finish(PinStmt::Unpin { table });
    }
    p.expect_word("PIN")?;
    let table = p.table()?;
    let spec = if p.eat_word("SHARD") {
        p.expect_word("KEY")?;
        PinSpec::ShardKey(p.columns()?)
    } else if p.eat_word("COLOCATE") {
        p.expect_word("WITH")?;
        let parent = p.table()?;
        p.expect_word("ON")?;
        PinSpec::Colocate {
            parent,
            columns: p.columns()?,
        }
    } else if p.eat_word("REFERENCE") {
        PinSpec::Reference
    } else if p.eat_word("UNSHARDED") {
        PinSpec::Unsharded
    } else {
        return Err(p.err());
    };
    p.finish(PinStmt::Pin { table, spec })
}

#[cfg(test)]
mod tests;
