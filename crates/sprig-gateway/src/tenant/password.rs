//! A user changing its own password (chapter 14, "MySQL logins": sandbox
//! mode lets only `ALTER USER ... IDENTIFIED BY`, `SET PASSWORD` and `SET`
//! run). Until chapter 29's account statements (M0.5) this is the only
//! account statement a tenant session runs; the rest fail with 1227. They
//! must never reach MariaDB, whose accounts are SprigDB's backend
//! accounts, shared by the tenant's users in R0.
//!
//! Recognized forms, for the session's own account only:
//!
//! ```text
//! SET PASSWORD [FOR <self>] = 'pw'
//! ALTER USER [IF EXISTS] {USER() | CURRENT_USER[()] | <self>}
//!     IDENTIFIED [WITH <plugin>] BY 'pw' [REPLACE 'old'] [RETAIN CURRENT PASSWORD]
//! ```
//!
//! `<self>` is `'user'` or `'user'@'host'` naming the session's account.

use sprig_sql::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};
use zeroize::Zeroizing;

/// A password change the session asked for.
pub struct PasswordChange {
    /// The new password.
    pub password: Zeroizing<Vec<u8>>,
    /// `RETAIN CURRENT PASSWORD`.
    pub retain_current: bool,
    /// The statement names another account (refused with 1227).
    pub other_account: bool,
}

impl std::fmt::Debug for PasswordChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordChange")
            .field("retain_current", &self.retain_current)
            .field("other_account", &self.other_account)
            .finish_non_exhaustive()
    }
}

struct P<'a> {
    sql: &'a [u8],
    t: Vec<&'a Token>,
    at: usize,
    nbe: bool,
}

impl P<'_> {
    fn word(&self, w: &str) -> bool {
        self.t.get(self.at).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.sql).eq_ignore_ascii_case(w.as_bytes())
        })
    }
    fn eat(&mut self, w: &str) -> bool {
        let y = self.word(w);
        if y {
            self.at += 1;
        }
        y
    }
    fn punct(&mut self, c: &[u8]) -> bool {
        let y = self.t.get(self.at).is_some_and(|t| t.bytes(self.sql) == c);
        if y {
            self.at += 1;
        }
        y
    }
    fn string(&mut self) -> Option<Vec<u8>> {
        let t = self.t.get(self.at)?;
        if t.kind != TokenKind::String {
            return None;
        }
        self.at += 1;
        string_value(t.bytes(self.sql), self.nbe)
    }
    /// `'user'[@'host']`, `user`, or the current-user forms; `Some(true)`
    /// when it names `user`@`host` (or the session).
    fn account(&mut self, user: &str, host: &str) -> Option<bool> {
        if self.eat("CURRENT_USER") || self.eat("USER") {
            if self.punct(b"(") && !self.punct(b")") {
                return None;
            }
            return Some(true);
        }
        let t = self.t.get(self.at)?;
        let name = match t.kind {
            TokenKind::String => string_value(t.bytes(self.sql), self.nbe)?,
            TokenKind::Word | TokenKind::QuotedIdent => ident_value(t.bytes(self.sql)).into_owned(),
            _ => return None,
        };
        self.at += 1;
        let mut h = b"%".to_vec();
        if self.punct(b"@") {
            let t = self.t.get(self.at)?;
            h = match t.kind {
                TokenKind::String => string_value(t.bytes(self.sql), self.nbe)?,
                TokenKind::Word | TokenKind::QuotedIdent => {
                    ident_value(t.bytes(self.sql)).into_owned()
                }
                _ => return None,
            };
            self.at += 1;
        } else if let Some(t) = self.t.get(self.at)
            && t.kind == TokenKind::UserVar
        {
            // `'u'@'h'` lexes the host as a user variable.
            let raw = t.bytes(self.sql);
            h = string_value(&raw[1..], self.nbe)
                .unwrap_or_else(|| ident_value(&raw[1..]).into_owned());
            self.at += 1;
        }
        Some(name == user.as_bytes() && h.eq_ignore_ascii_case(host.as_bytes()))
    }
    fn done(&self) -> bool {
        self.t[self.at..]
            .iter()
            .all(|t| t.kind == TokenKind::Punct && t.bytes(self.sql) == b";")
    }
}

/// Parses `sql` as a password change of `user`@`host`; `None` when it is
/// not one of the forms above.
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions, user: &str, host: &str) -> Option<PasswordChange> {
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut p = P {
        sql,
        t,
        at: 0,
        nbe: opts.no_backslash_escapes,
    };
    let mut other = false;
    let mut retain = false;
    let pw;
    if p.eat("SET") {
        if !p.eat("PASSWORD") {
            return None;
        }
        if p.eat("FOR") {
            other = !p.account(user, host)?;
        }
        if !p.punct(b"=") {
            return None;
        }
        pw = p.string()?;
    } else if p.eat("ALTER") {
        if !p.eat("USER") {
            return None;
        }
        if p.eat("IF") && !p.eat("EXISTS") {
            return None;
        }
        other = !p.account(user, host)?;
        if !p.eat("IDENTIFIED") {
            return None;
        }
        if p.eat("WITH") {
            // The plugin: every SprigDB password stores every verifier.
            p.at += 1;
        }
        if !p.eat("BY") {
            return None;
        }
        pw = p.string()?;
        loop {
            if p.eat("REPLACE") {
                p.string()?;
            } else if p.eat("RETAIN") {
                if !(p.eat("CURRENT") && p.eat("PASSWORD")) {
                    return None;
                }
                retain = true;
            } else {
                break;
            }
        }
    } else {
        return None;
    }
    if !p.done() {
        return None;
    }
    Some(PasswordChange {
        password: Zeroizing::new(pw),
        retain_current: retain,
        other_account: other,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn pc(sql: &str) -> Option<PasswordChange> {
        parse(sql.as_bytes(), &LexOptions::mysql84(), "app", "%")
    }

    #[test]
    fn own_password_changes() {
        let p = pc("SET PASSWORD = 'n3w'").unwrap();
        assert_eq!(&p.password[..], b"n3w");
        assert!(!p.other_account);
        let p = pc("ALTER USER USER() IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD;").unwrap();
        assert!(p.retain_current && !p.other_account);
        assert!(
            !pc("alter user current_user identified by 'x'")
                .unwrap()
                .other_account
        );
        assert!(
            !pc("ALTER USER 'app'@'%' IDENTIFIED WITH caching_sha2_password BY 'x'")
                .unwrap()
                .other_account
        );
        assert!(!pc("SET PASSWORD FOR app = 'x'").unwrap().other_account);
        assert!(
            pc("SET PASSWORD FOR 'root'@'%' = 'x'")
                .unwrap()
                .other_account
        );
        assert!(
            pc("ALTER USER bob IDENTIFIED BY 'x'")
                .unwrap()
                .other_account
        );
        assert!(pc("ALTER USER app ACCOUNT LOCK").is_none());
        assert!(pc("SET autocommit = 1").is_none());
        assert!(pc("SET PASSWORD = 'x', autocommit = 1").is_none());
    }
}
