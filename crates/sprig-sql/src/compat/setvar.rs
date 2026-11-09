//! `SET` of the variables the MySQL persona emulates, and the statement
//! hints that set them (chapter 28, "Emulated system variables" and
//! "Queries and expressions"; R4).
//!
//! | Client text (3306) | Sent to MariaDB |
//! |---|---|
//! | `SET [SESSION] sql_mode = 'v'` (a string, a number or `DEFAULT`) | the backend value of `v` ([`super::sqlmode`]); 1231 when MySQL 8.4 refuses `v` |
//! | `SET [SESSION] max_execution_time = n` | `max_statement_time = n / 1000` |
//! | `SELECT /*+ MAX_EXECUTION_TIME(n) */ ...` | `SET STATEMENT max_statement_time = n / 1000 FOR SELECT ...` |
//! | `/*+ SET_VAR(v = x) */` after the statement's first word | `SET STATEMENT v = x FOR ...` |
//!
//! Assignments whose value is an expression pass unchanged: MariaDB
//! evaluates them, and the gateway reads the client's view back from the
//! value it reports ([`super::sqlmode::from_mariadb`]). `GLOBAL`
//! assignments pass unchanged too (tenants can't make them).
//!
//! MySQL's `max_execution_time` limits only `SELECT`; MariaDB's
//! `max_statement_time` limits every statement. A session that sets it
//! therefore also bounds its writes (a documented difference).

use super::errmap::WireError;
use super::sqlmode::{self, MYSQL84_DEFAULT, Mode};
use crate::lex::{self, LexOptions, Token, TokenKind};

/// The result of [`rewrite_set`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetRewrite {
    /// Send this text instead; `sql_mode` is the client's view of the
    /// value it sets (when it sets one), for the session to record once
    /// the statement succeeds.
    Rewritten {
        /// The statement for MariaDB.
        sql: Vec<u8>,
        /// The `sql_mode` set, if any.
        sql_mode: Option<Mode>,
    },
    /// The statement fails with this error; nothing is sent.
    Refused(WireError),
}

/// `ms` milliseconds as MariaDB's seconds, `1.5` for 1500.
fn seconds(ms: u64) -> String {
    if ms.is_multiple_of(1000) {
        (ms / 1000).to_string()
    } else {
        format!("{}.{:03}", ms / 1000, ms % 1000)
    }
}

fn sig(tokens: &[Token]) -> Vec<Token> {
    tokens
        .iter()
        .copied()
        .filter(|t| !t.kind.is_trivia())
        .collect()
}

/// The variable an assignment target names, lower case, with its scope:
/// `None` for `GLOBAL` and user variables.
fn target_name(t: &[Token], sql: &[u8], i: &mut usize) -> Option<String> {
    let word = |j: usize, w: &str| {
        t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(sql).eq_ignore_ascii_case(w.as_bytes())
        })
    };
    let global = word(*i, "GLOBAL") || word(*i, "PERSIST") || word(*i, "PERSIST_ONLY");
    if global || word(*i, "SESSION") || word(*i, "LOCAL") {
        *i += 1;
    }
    let tk = t.get(*i)?;
    *i += 1;
    let text = String::from_utf8_lossy(tk.bytes(sql)).to_ascii_lowercase();
    let name = match tk.kind {
        TokenKind::Word => text,
        TokenKind::SysVar => {
            let n = text.trim_start_matches("@@");
            if n.starts_with("global.") || n.starts_with("persist") {
                return None;
            }
            n.strip_prefix("session.")
                .or_else(|| n.strip_prefix("local."))
                .unwrap_or(n)
                .trim_matches('`')
                .to_owned()
        }
        _ => return None,
    };
    (!global).then_some(name)
}

/// Rewrites a `SET` statement's assignments of `sql_mode` and
/// `max_execution_time` for the MySQL persona (see the module
/// documentation). `None` when the statement isn't such a `SET` or has
/// nothing to change.
#[must_use]
#[allow(clippy::too_many_lines)] // one pass over the assignments
pub fn rewrite_set(sql: &[u8], opts: &LexOptions) -> Option<SetRewrite> {
    let has = |n: &[u8]| sql.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n));
    if !has(b"sql_mode") && !has(b"max_execution_time") {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let t = sig(&tokens);
    let first = t.first()?;
    if first.kind != TokenKind::Word || !first.bytes(sql).eq_ignore_ascii_case(b"SET") {
        return None;
    }
    let mut edits: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    let mut mode = None;
    let mut i = 1;
    loop {
        let start = t.get(i)?.start as usize;
        let name = target_name(&t, sql, &mut i);
        let op = t.get(i)?;
        if op.kind != TokenKind::Op || !matches!(op.bytes(sql), b"=" | b":=") {
            return None;
        }
        // The value: one token followed by `,`, `;` or the end; anything
        // longer is an expression MariaDB evaluates.
        let v = *t.get(i + 1)?;
        let after = t.get(i + 2);
        let single = match after {
            None => true,
            Some(a) => a.kind == TokenKind::Punct && matches!(a.bytes(sql), b"," | b";"),
        };
        if single {
            let vtext = v.bytes(sql);
            match name.as_deref() {
                Some("sql_mode") => {
                    let m = match v.kind {
                        TokenKind::String | TokenKind::NationalString => {
                            let s = lex::string_value(vtext, opts.no_backslash_escapes)?;
                            sqlmode::parse_mysql(&String::from_utf8_lossy(&s))
                        }
                        TokenKind::Number => {
                            match std::str::from_utf8(vtext).ok()?.parse::<u64>() {
                                Ok(n) => sqlmode::from_number(n),
                                Err(_) => Err(sqlmode::BadValue(
                                    String::from_utf8_lossy(vtext).into_owned(),
                                )),
                            }
                        }
                        TokenKind::Word if vtext.eq_ignore_ascii_case(b"DEFAULT") => {
                            Ok(MYSQL84_DEFAULT)
                        }
                        TokenKind::Word if vtext.eq_ignore_ascii_case(b"NULL") => {
                            Err(sqlmode::BadValue("NULL".to_owned()))
                        }
                        // ON, a bare word: MySQL reads it as a mode name.
                        TokenKind::Word => sqlmode::parse_mysql(&String::from_utf8_lossy(vtext)),
                        _ => return None,
                    };
                    match m {
                        Ok(m) => {
                            edits.push((
                                v.start as usize..v.end as usize,
                                format!("'{}'", m.backend()),
                            ));
                            mode = Some(m);
                        }
                        Err(bad) => {
                            return Some(SetRewrite::Refused(WireError::new(
                                1231,
                                *b"42000",
                                bad.message(),
                            )));
                        }
                    }
                }
                Some("max_execution_time") => {
                    let value = match v.kind {
                        TokenKind::Number => {
                            let s = std::str::from_utf8(vtext).ok()?;
                            if s.contains(['.', 'e', 'E']) {
                                return Some(SetRewrite::Refused(WireError::new(
                                    1232,
                                    *b"42000",
                                    "Incorrect argument type to variable 'max_execution_time'",
                                )));
                            }
                            seconds(s.parse::<u64>().ok()?.min(u64::from(u32::MAX)))
                        }
                        TokenKind::Word if vtext.eq_ignore_ascii_case(b"DEFAULT") => "0".to_owned(),
                        _ => return None,
                    };
                    let name_end = t[i - 1].end as usize;
                    edits.push((start..name_end, "SESSION max_statement_time".to_owned()));
                    edits.push((v.start as usize..v.end as usize, value));
                }
                _ => {}
            }
            i += 2;
        } else {
            // An expression: skip to the next top-level comma.
            let mut depth = 0i32;
            i += 1;
            while let Some(tk) = t.get(i) {
                if tk.kind == TokenKind::Punct {
                    match tk.bytes(sql) {
                        b"(" => depth += 1,
                        b")" => depth -= 1,
                        b"," if depth == 0 => break,
                        _ => {}
                    }
                }
                i += 1;
            }
            if name.as_deref() == Some("max_execution_time") {
                // An expression for a variable MariaDB lacks: refused as
                // MariaDB would refuse it, so leave it.
            }
        }
        match t.get(i) {
            None => break,
            Some(c) if c.kind == TokenKind::Punct && c.bytes(sql) == b"," => i += 1,
            Some(c) if c.kind == TokenKind::Punct && c.bytes(sql) == b";" => break,
            Some(_) => return None,
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 16);
    let mut at = 0;
    for (r, s) in edits {
        out.extend_from_slice(&sql[at..r.start]);
        out.extend_from_slice(s.as_bytes());
        at = r.end;
    }
    out.extend_from_slice(&sql[at..]);
    Some(SetRewrite::Rewritten {
        sql: out,
        sql_mode: mode,
    })
}

/// Variables `SET_VAR` may not set through `SET STATEMENT` (MariaDB
/// refuses some; these would change the session beyond the statement).
const NO_SET_VAR: &[&str] = &["autocommit", "character_set_client", "sql_log_bin"];

/// The `SET STATEMENT ... FOR` prefix of a statement's leading hint:
/// `MAX_EXECUTION_TIME(n)` (only on `SELECT`, as MySQL) and `SET_VAR(v =
/// x)`. Other hints stay as they are (MariaDB 11.8 ignores them). `None`
/// when the statement has no such hint.
#[must_use]
pub fn rewrite_hints(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if !sql.windows(3).any(|w| w == b"/*+") {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let first = tokens.iter().position(|t| !t.kind.is_trivia())?;
    let verb = tokens[first];
    if verb.kind != TokenKind::Word {
        return None;
    }
    let is_select = verb.bytes(sql).eq_ignore_ascii_case(b"SELECT");
    let hint = tokens[first + 1..]
        .iter()
        .take_while(|t| t.kind.is_trivia())
        .find(|t| t.kind == TokenKind::Hint)?;
    let body = hint.bytes(sql);
    let body = String::from_utf8_lossy(&body[3..body.len().saturating_sub(2)]).into_owned();
    let mut sets: Vec<String> = Vec::new();
    let mut rest = body.as_str();
    while let Some(open) = rest.find('(') {
        let name = rest[..open]
            .trim()
            .rsplit(char::is_whitespace)
            .next()?
            .to_owned();
        let close = rest[open..].find(')')? + open;
        let arg = rest[open + 1..close].trim();
        if name.eq_ignore_ascii_case("MAX_EXECUTION_TIME") && is_select {
            let ms: u64 = arg.parse().ok()?;
            sets.push(format!("max_statement_time={}", seconds(ms)));
        } else if name.eq_ignore_ascii_case("SET_VAR")
            && let Some((v, x)) = arg.split_once('=')
        {
            let v = v.trim().to_ascii_lowercase();
            let x = x.trim();
            let simple = |s: &str| {
                !s.is_empty()
                    && s.bytes().all(|b| {
                        b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'\'' | b'-' | b',')
                    })
            };
            if simple(&v) && simple(x) && !NO_SET_VAR.contains(&v.as_str()) {
                if v == "max_execution_time" {
                    let ms: u64 = x.parse().ok()?;
                    sets.push(format!("max_statement_time={}", seconds(ms)));
                } else if v == "sql_mode" {
                    let m = sqlmode::parse_mysql(x.trim_matches('\'')).ok()?;
                    sets.push(format!("sql_mode='{}'", m.backend()));
                } else {
                    sets.push(format!("{v}={x}"));
                }
            }
        }
        rest = &rest[close + 1..];
    }
    if sets.is_empty() {
        return None;
    }
    let mut out = format!("SET STATEMENT {} FOR ", sets.join(", ")).into_bytes();
    out.extend_from_slice(&sql[verb.start as usize..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(s: &str) -> Option<SetRewrite> {
        rewrite_set(s.as_bytes(), &LexOptions::mysql84())
    }

    fn text(r: Option<SetRewrite>) -> String {
        match r {
            Some(SetRewrite::Rewritten { sql, .. }) => String::from_utf8(sql).unwrap(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn sql_mode_literals_become_backend_values() {
        assert_eq!(
            text(set("SET sql_mode = 'TRADITIONAL'")),
            "SET sql_mode = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,TIME_ROUND_FRACTIONAL'"
        );
        match set("SET SESSION sql_mode='ANSI', autocommit=1") {
            Some(SetRewrite::Rewritten { sql, sql_mode }) => {
                assert_eq!(
                    String::from_utf8(sql).unwrap(),
                    "SET SESSION sql_mode='REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,\
                     TIME_ROUND_FRACTIONAL', autocommit=1"
                );
                assert!(sql_mode.unwrap().only_full_group_by());
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            text(set("SET @@session.sql_mode := DEFAULT")),
            "SET @@session.sql_mode := 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,TIME_ROUND_FRACTIONAL'"
        );
        // mysqldump's own.
        assert_eq!(
            text(set(
                "SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO'"
            )),
            "SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO,TIME_ROUND_FRACTIONAL'"
        );
    }

    #[test]
    fn expressions_and_global_pass() {
        assert!(set("SET sql_mode = @OLD_SQL_MODE").is_none());
        assert!(set("SET sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')").is_none());
        assert!(set("SET GLOBAL sql_mode = ''").is_none());
        assert!(set("SET @@global.sql_mode = ''").is_none());
        assert!(set("SELECT @@sql_mode").is_none());
    }

    #[test]
    fn refused_values() {
        match set("SET sql_mode = 'MAXDB'") {
            Some(SetRewrite::Refused(e)) => {
                assert_eq!(e.code, 1231);
                assert_eq!(
                    e.message,
                    "Variable 'sql_mode' can't be set to the value of 'MAXDB'"
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            set("SET sql_mode = NULL"),
            Some(SetRewrite::Refused(_))
        ));
    }

    #[test]
    fn max_execution_time_is_mariadbs_seconds() {
        assert_eq!(
            text(set("SET max_execution_time = 1500")),
            "SET SESSION max_statement_time = 1.500"
        );
        assert_eq!(
            text(set("SET SESSION max_execution_time=2000, sql_mode=''")),
            "SET SESSION max_statement_time=2, sql_mode='TIME_ROUND_FRACTIONAL'"
        );
        assert_eq!(
            text(set("SET @@max_execution_time = 0")),
            "SET SESSION max_statement_time = 0"
        );
    }

    #[test]
    fn hints() {
        let o = LexOptions::mysql84();
        let r = |s: &str| rewrite_hints(s.as_bytes(), &o).map(|v| String::from_utf8(v).unwrap());
        assert_eq!(
            r("SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t").as_deref(),
            Some(
                "SET STATEMENT max_statement_time=1 FOR SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t"
            )
        );
        assert_eq!(
            r("UPDATE /*+ SET_VAR(sort_buffer_size = 16777216) */ t SET a = 1").as_deref(),
            Some(
                "SET STATEMENT sort_buffer_size=16777216 FOR UPDATE /*+ SET_VAR(sort_buffer_size = 16777216) */ t SET a = 1"
            )
        );
        assert_eq!(r("UPDATE /*+ MAX_EXECUTION_TIME(5) */ t SET a = 1"), None);
        assert_eq!(r("SELECT /*+ BKA(t) */ * FROM t"), None);
        assert_eq!(r("SELECT '/*+ MAX_EXECUTION_TIME(1) */'"), None);
    }
}
