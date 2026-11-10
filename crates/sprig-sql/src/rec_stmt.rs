//! Recommendations in SQL (chapter 52, "SQL"):
//!
//! ```sql
//! SHOW SPRIG RECOMMENDATIONS [ALL] [FOR DATABASE db | FOR TABLE [db.]t];
//! SPRIG APPROVE RECOMMENDATION 17 [REASON 'text'];
//! SPRIG IGNORE RECOMMENDATION 17 [FOR 30 DAYS | FOREVER] [REASON 'text'];
//! SPRIG SNOOZE RECOMMENDATION 17 FOR 48 HOURS;
//! ```
//!
//! Ids are the tenant's own numbers (`t<tenant>.<n>` elsewhere).

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};
use crate::names::NameError;

/// What `SHOW SPRIG RECOMMENDATIONS` lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecScope {
    /// The whole tenant.
    Tenant,
    /// One database.
    Database(String),
    /// One table, optionally qualified.
    Table {
        /// The database.
        db: Option<String>,
        /// The table.
        name: String,
    },
}

/// A parsed statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecStmt {
    /// `SHOW SPRIG RECOMMENDATIONS`.
    Show {
        /// Closed items too.
        all: bool,
        /// Which.
        scope: RecScope,
    },
    /// `SPRIG APPROVE RECOMMENDATION n`.
    Approve {
        /// The number.
        id: u64,
        /// Why.
        reason: Option<String>,
    },
    /// `SPRIG IGNORE RECOMMENDATION n`.
    Ignore {
        /// The number.
        id: u64,
        /// `FOR n DAYS`.
        days: Option<u32>,
        /// `FOREVER`.
        forever: bool,
        /// Why.
        reason: Option<String>,
    },
    /// `SPRIG SNOOZE RECOMMENDATION n FOR h HOURS`.
    Snooze {
        /// The number.
        id: u64,
        /// Hours.
        hours: u32,
    },
}

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

    fn eat_word(&mut self, w: &str) -> bool {
        let yes = self.peek().is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.sql).eq_ignore_ascii_case(w.as_bytes())
        });
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

    fn number<T: std::str::FromStr>(&mut self) -> Result<T, NameError> {
        let t = self.peek().ok_or_else(|| self.err())?;
        if t.kind != TokenKind::Number {
            return Err(self.err());
        }
        let v = std::str::from_utf8(t.bytes(self.sql))
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| self.err())?;
        self.at += 1;
        Ok(v)
    }

    fn string(&mut self) -> Result<String, NameError> {
        let t = self.peek().ok_or_else(|| self.err())?;
        if t.kind != TokenKind::String {
            return Err(self.err());
        }
        let v = string_value(t.bytes(self.sql), false).ok_or_else(|| self.err())?;
        self.at += 1;
        Ok(String::from_utf8_lossy(&v).into_owned())
    }

    fn reason(&mut self) -> Result<Option<String>, NameError> {
        if self.eat_word("REASON") {
            return Ok(Some(self.string()?));
        }
        Ok(None)
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

fn word(sql: &[u8], toks: &[&Token], i: usize, s: &str) -> bool {
    toks.get(i).is_some_and(|t| {
        t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(s.as_bytes())
    })
}

/// Whether the first words make one of these statements:
/// `SHOW SPRIG RECOMMENDATIONS`, or `SPRIG APPROVE|IGNORE|SNOOZE
/// RECOMMENDATION`. Settings statements (`settings_stmt`) leave these
/// alone.
#[must_use]
pub fn is_rec(sql: &[u8], toks: &[&Token]) -> bool {
    (word(sql, toks, 0, "SHOW")
        && word(sql, toks, 1, "SPRIG")
        && word(sql, toks, 2, "RECOMMENDATIONS"))
        || (word(sql, toks, 0, "SPRIG")
            && (word(sql, toks, 1, "APPROVE")
                || word(sql, toks, 1, "IGNORE")
                || word(sql, toks, 1, "SNOOZE"))
            && word(sql, toks, 2, "RECOMMENDATION"))
}

/// Parses `sql`. `None` when it isn't one of these statements;
/// `Some(Err)` with 1064 for a syntax error.
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<Result<RecStmt, NameError>> {
    let tokens = lex::lex(sql, opts).ok()?;
    let toks: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    if !is_rec(sql, &toks) {
        return None;
    }
    let mut p = Parser { sql, toks, at: 0 };
    Some(parse_stmt(&mut p))
}

fn parse_stmt(p: &mut Parser<'_>) -> Result<RecStmt, NameError> {
    if p.eat_word("SHOW") {
        p.expect_word("SPRIG")?;
        p.expect_word("RECOMMENDATIONS")?;
        let all = p.eat_word("ALL");
        let scope = if p.eat_word("FOR") {
            if p.eat_word("DATABASE") || p.eat_word("SCHEMA") {
                RecScope::Database(p.ident()?)
            } else {
                p.expect_word("TABLE")?;
                let first = p.ident()?;
                if p.eat_punct(b'.') {
                    RecScope::Table {
                        db: Some(first),
                        name: p.ident()?,
                    }
                } else {
                    RecScope::Table {
                        db: None,
                        name: first,
                    }
                }
            }
        } else {
            RecScope::Tenant
        };
        return p.finish(RecStmt::Show { all, scope });
    }
    p.expect_word("SPRIG")?;
    if p.eat_word("APPROVE") {
        p.expect_word("RECOMMENDATION")?;
        let id = p.number()?;
        let reason = p.reason()?;
        return p.finish(RecStmt::Approve { id, reason });
    }
    if p.eat_word("SNOOZE") {
        p.expect_word("RECOMMENDATION")?;
        let id = p.number()?;
        p.expect_word("FOR")?;
        let hours = p.number()?;
        if !p.eat_word("HOURS") {
            p.expect_word("HOUR")?;
        }
        return p.finish(RecStmt::Snooze { id, hours });
    }
    p.expect_word("IGNORE")?;
    p.expect_word("RECOMMENDATION")?;
    let id = p.number()?;
    let (mut days, mut forever) = (None, false);
    if p.eat_word("FOREVER") {
        forever = true;
    } else if p.eat_word("FOR") {
        days = Some(p.number()?);
        if !p.eat_word("DAYS") {
            p.expect_word("DAY")?;
        }
    }
    let reason = p.reason()?;
    p.finish(RecStmt::Ignore {
        id,
        days,
        forever,
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> RecStmt {
        parse(s.as_bytes(), &LexOptions::mariadb())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn statements_parse() {
        assert_eq!(
            ok("show sprig recommendations"),
            RecStmt::Show {
                all: false,
                scope: RecScope::Tenant
            }
        );
        assert_eq!(
            ok("SHOW SPRIG RECOMMENDATIONS ALL FOR TABLE shop.`orders`;"),
            RecStmt::Show {
                all: true,
                scope: RecScope::Table {
                    db: Some("shop".into()),
                    name: "orders".into()
                }
            }
        );
        assert_eq!(
            ok("SHOW SPRIG RECOMMENDATIONS FOR DATABASE shop"),
            RecStmt::Show {
                all: false,
                scope: RecScope::Database("shop".into())
            }
        );
        assert_eq!(
            ok("SPRIG APPROVE RECOMMENDATION 17"),
            RecStmt::Approve {
                id: 17,
                reason: None
            }
        );
        assert_eq!(
            ok("sprig approve recommendation 17 reason 'tonight'"),
            RecStmt::Approve {
                id: 17,
                reason: Some("tonight".into())
            }
        );
        assert_eq!(
            ok("SPRIG IGNORE RECOMMENDATION 3 FOR 30 DAYS REASON 'seasonal'"),
            RecStmt::Ignore {
                id: 3,
                days: Some(30),
                forever: false,
                reason: Some("seasonal".into())
            }
        );
        assert_eq!(
            ok("SPRIG IGNORE RECOMMENDATION 3 FOREVER"),
            RecStmt::Ignore {
                id: 3,
                days: None,
                forever: true,
                reason: None
            }
        );
        assert_eq!(
            ok("SPRIG IGNORE RECOMMENDATION 3"),
            RecStmt::Ignore {
                id: 3,
                days: None,
                forever: false,
                reason: None
            }
        );
        assert_eq!(
            ok("SPRIG SNOOZE RECOMMENDATION 4 FOR 48 HOURS"),
            RecStmt::Snooze { id: 4, hours: 48 }
        );
    }

    #[test]
    fn others_are_not_ours_and_errors_are_1064() {
        assert!(parse(b"SHOW SPRIG SETTINGS", &LexOptions::mariadb()).is_none());
        assert!(parse(b"SPRIG SET x = 1", &LexOptions::mariadb()).is_none());
        assert!(parse(b"SELECT 1", &LexOptions::mariadb()).is_none());
        for bad in [
            "SPRIG APPROVE RECOMMENDATION x",
            "SPRIG IGNORE RECOMMENDATION 1 FOR DAYS",
            "SHOW SPRIG RECOMMENDATIONS FOR VIEW v",
        ] {
            let e = parse(bad.as_bytes(), &LexOptions::mariadb())
                .unwrap()
                .unwrap_err();
            assert!(matches!(e, NameError::Refused { errno: 1064, .. }), "{bad}");
        }
    }
}
