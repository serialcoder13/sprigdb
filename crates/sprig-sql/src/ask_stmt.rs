//! `ASK`: a question for an AI assistant, answered with a proposed query
//! that never runs.
//!
//! ```sql
//! ASK 'top 10 customers by revenue this year';
//! ASK 'why is this slow: SELECT ...' ROLE dba;
//! ASK 'orders per day' IN DATABASE shop ROLE data;
//! ```
//!
//! The statement is recognized only when `ASK` is followed by a string
//! literal, which no MySQL or MariaDB statement allows, so a table, column
//! or variable named `ask` keeps working everywhere. Adjacent literals are
//! joined, as MySQL joins them. `IN DATABASE` and `ROLE` may come in either
//! order, each once.

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};
use crate::names::NameError;

/// A parsed `ASK`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskStmt {
    /// The question.
    pub question: String,
    /// `ROLE data|dba`, lower-cased as written; `None` for the default.
    pub role: Option<String>,
    /// `IN DATABASE db`; `None` for the session's database.
    pub database: Option<String>,
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
    no_backslash_escapes: bool,
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

    fn string(&mut self) -> Option<String> {
        let t = self.peek()?;
        if t.kind != TokenKind::String {
            return None;
        }
        let v = string_value(t.bytes(self.sql), self.no_backslash_escapes)?;
        self.at += 1;
        Some(String::from_utf8_lossy(&v).into_owned())
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

    fn at_end(&mut self) -> bool {
        if self
            .peek()
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(self.sql) == b";")
        {
            self.at += 1;
        }
        self.peek().is_none()
    }
}

/// Parses `sql`. `None` when it isn't an `ASK` (the first word isn't `ASK`,
/// or no string literal follows it); `Some(Err)` with 1064 for a syntax
/// error after the question.
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<Result<AskStmt, NameError>> {
    let tokens = lex::lex(sql, opts).ok()?;
    // Executable-comment markers are trivia here: `/*!ASK ...*/` stays
    // the backend's business, since its first token is the marker.
    if tokens
        .iter()
        .find(|t| {
            !matches!(
                t.kind,
                TokenKind::Whitespace | TokenKind::LineComment | TokenKind::BlockComment
            )
        })
        .is_some_and(|t| t.kind != TokenKind::Word)
    {
        return None;
    }
    let toks: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut p = Parser {
        sql,
        toks,
        at: 0,
        no_backslash_escapes: opts.no_backslash_escapes,
    };
    if !p.eat_word("ASK") {
        return None;
    }
    let mut question = p.string()?;
    while let Some(more) = p.string() {
        question.push_str(&more);
    }
    Some(parse_rest(&mut p, question))
}

fn parse_rest(p: &mut Parser<'_>, question: String) -> Result<AskStmt, NameError> {
    let mut stmt = AskStmt {
        question,
        role: None,
        database: None,
    };
    loop {
        if p.at_end() {
            return Ok(stmt);
        }
        if stmt.database.is_none() && p.is_word("IN") {
            p.at += 1;
            if !(p.eat_word("DATABASE") || p.eat_word("SCHEMA")) {
                return Err(p.err());
            }
            stmt.database = Some(p.ident()?);
        } else if stmt.role.is_none() && p.eat_word("ROLE") {
            let role = match p.string() {
                Some(s) => s,
                None => p.ident()?,
            };
            stmt.role = Some(role.trim().to_ascii_lowercase());
        } else {
            return Err(p.err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> AskStmt {
        parse(s.as_bytes(), &LexOptions::mariadb())
            .expect("an ASK")
            .expect("parses")
    }

    fn none(s: &str) -> bool {
        parse(s.as_bytes(), &LexOptions::mariadb()).is_none()
    }

    #[test]
    fn questions_and_clauses_parse() {
        assert_eq!(
            ok("ASK 'top 10 customers'"),
            AskStmt {
                question: "top 10 customers".into(),
                role: None,
                database: None,
            }
        );
        assert_eq!(
            ok("  ask 'it''s slow' ROLE DBA ;"),
            AskStmt {
                question: "it's slow".into(),
                role: Some("dba".into()),
                database: None,
            }
        );
        assert_eq!(
            ok("/* c */ Ask \"orders\" ' per day' in database `shop` role 'data'"),
            AskStmt {
                question: "orders per day".into(),
                role: Some("data".into()),
                database: Some("shop".into()),
            }
        );
        assert_eq!(
            ok("ASK 'x' ROLE data IN SCHEMA s").database.as_deref(),
            Some("s")
        );
        assert_eq!(ok("ASK 'a\\nb'").question, "a\nb");
        let nbe = LexOptions {
            no_backslash_escapes: true,
            ..LexOptions::mariadb()
        };
        assert_eq!(
            parse(b"ASK 'a\\nb'", &nbe).unwrap().unwrap().question,
            "a\\nb"
        );
    }

    #[test]
    fn mistakes_after_the_question_are_syntax_errors() {
        for bad in [
            "ASK 'x' ROLE",
            "ASK 'x' IN shop",
            "ASK 'x' ROLE data ROLE dba",
            "ASK 'x' IN DATABASE a IN DATABASE b",
            "ASK 'x' LIMIT 3",
            "ASK 'x'; SELECT 1",
        ] {
            let e = parse(bad.as_bytes(), &LexOptions::mariadb())
                .expect(bad)
                .expect_err(bad);
            assert!(
                matches!(e, NameError::Refused { errno: 1064, .. }),
                "{bad}: {e:?}"
            );
        }
    }

    #[test]
    fn names_called_ask_are_left_alone() {
        for other in [
            "SELECT ask FROM ask",
            "ask",
            "ASK ask",
            "ASK `x`",
            "ASK(1)",
            "ask = 1",
            "INSERT INTO ask (ask) VALUES ('ask')",
            "CREATE TABLE ask (ask INT)",
            "UPDATE ask SET ask = 'ask'",
            "SET @ask = 'x'",
            "/*!100000 ASK 'x' */",
            "",
        ] {
            assert!(none(other), "{other}");
        }
        // Under ANSI_QUOTES a double-quoted word is a name.
        let ansi = LexOptions {
            ansi_quotes: true,
            ..LexOptions::mariadb()
        };
        assert!(parse(b"ASK \"x\"", &ansi).is_none());
    }
}
