//! Account statements in SQL (chapter 29, "MySQL account statements"):
//! parsing into [`sprig_auth::authz::stmt::AccountStmt`] with logical
//! names; `sprig_auth::authz::account_sql` runs them.
//!
//! ```text
//! CREATE USER [IF NOT EXISTS] acct [auth] {, acct [auth]} [DEFAULT ROLE roles]
//!     [REQUIRE NONE|SSL|X509] [PASSWORD EXPIRE [DEFAULT|NEVER]] [ACCOUNT LOCK|UNLOCK]
//! ALTER USER [IF EXISTS] acct [auth [RETAIN CURRENT PASSWORD]] {, ...} [DEFAULT ROLE roles]
//!     [REQUIRE ...] [PASSWORD EXPIRE ...] [ACCOUNT ...] [DISCARD OLD PASSWORD]
//! DROP USER [IF EXISTS] acct {, acct}
//! CREATE ROLE [IF NOT EXISTS] acct {, acct};  DROP ROLE [IF EXISTS] acct {, acct}
//! GRANT priv [(cols)] {, priv [(cols)]} ON [TABLE|FUNCTION|PROCEDURE] target
//!     TO acct {, acct} [WITH GRANT OPTION]
//! GRANT role {, role} TO acct {, acct} [WITH ADMIN OPTION]
//! REVOKE priv ... ON target FROM acct {, acct}
//! REVOKE ALL [PRIVILEGES], GRANT OPTION FROM acct {, acct}
//! REVOKE role {, role} FROM acct {, acct}
//! SET ROLE DEFAULT | NONE | ALL [EXCEPT roles] | roles
//! SET DEFAULT ROLE NONE | ALL | roles TO accts      (MySQL)
//! SET DEFAULT ROLE role | NONE [FOR acct]           (MariaDB)
//! SHOW GRANTS [FOR acct [USING roles]];  FLUSH PRIVILEGES
//! auth   := IDENTIFIED BY 'pw' | IDENTIFIED WITH plugin [BY 'pw']
//! acct   := name ['@' host] | CURRENT_USER [()]
//! target := *.* | * | db.* | db.obj | obj
//! ```
//!
//! [`parse`] returns `None` for text that isn't one of these, so other
//! paths keep it (a user's own password change, chapter 14). Shapes
//! SprigDB doesn't take (`GRANT PROXY`, `IDENTIFIED ... AS 'hash'`,
//! `RENAME USER`, password and resource options) fail with 1235.

use sprig_auth::authz::ObjectType;
use sprig_auth::authz::stmt::{
    AccountRef, AccountStmt, GrantOn, Identified, PrivItem, RoleChoice, UserItem, UserOptions,
    UserSpec,
};
use sprig_auth::store::{RequireCurrent, RequireTls};
use sprig_common::SprigError;
use zeroize::Zeroizing;

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};

type R<T> = Result<T, SprigError>;

struct P<'a> {
    sql: &'a [u8],
    toks: Vec<&'a Token>,
    at: usize,
    nbe: bool,
}

fn syntax(near: &str) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             server version for the right syntax to use near '{near}'"
        ),
    )
}

fn unsupported(what: &str) -> SprigError {
    SprigError::standard(
        1235,
        *b"42000",
        format!("This version of SprigDB doesn't yet support '{what}'"),
    )
}

impl<'a> P<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.at).copied()
    }

    fn text(&self, t: &Token) -> String {
        String::from_utf8_lossy(t.bytes(self.sql)).into_owned()
    }

    fn is_word(&self, i: usize, w: &str) -> bool {
        self.toks.get(i).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.sql).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn word(&self, w: &str) -> bool {
        self.is_word(self.at, w)
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if self.word(w) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn eat_words(&mut self, ws: &[&str]) -> bool {
        if ws
            .iter()
            .enumerate()
            .all(|(i, w)| self.is_word(self.at + i, w))
        {
            self.at += ws.len();
            true
        } else {
            false
        }
    }

    fn punct(&self, p: &str) -> bool {
        self.peek().is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(self.sql) == p.as_bytes()
        })
    }

    fn eat_punct(&mut self, p: &str) -> bool {
        if self.punct(p) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn err(&self) -> SprigError {
        let near = self.peek().map_or_else(String::new, |t| {
            String::from_utf8_lossy(&self.sql[t.start as usize..])
                .chars()
                .take(40)
                .collect()
        });
        syntax(&near)
    }

    fn expect_word(&mut self, w: &str) -> R<()> {
        if self.eat_word(w) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    fn end(&mut self) -> R<()> {
        self.eat_punct(";");
        if self.at == self.toks.len() {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    /// An identifier or a string as a name.
    fn name(&mut self) -> R<String> {
        let t = self.peek().ok_or_else(|| self.err())?;
        let v = match t.kind {
            TokenKind::Word | TokenKind::QuotedIdent => {
                String::from_utf8_lossy(&ident_value(t.bytes(self.sql))).into_owned()
            }
            TokenKind::String => string_value(t.bytes(self.sql), self.nbe)
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .ok_or_else(|| self.err())?,
            _ => return Err(self.err()),
        };
        self.at += 1;
        Ok(v)
    }

    /// A non-negative integer.
    fn number(&mut self) -> R<u64> {
        let t = self.peek().ok_or_else(|| self.err())?;
        if t.kind != TokenKind::Number {
            return Err(self.err());
        }
        let v = std::str::from_utf8(t.bytes(self.sql))
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| self.err())?;
        self.at += 1;
        Ok(v)
    }

    fn string(&mut self) -> R<Vec<u8>> {
        let t = self.peek().ok_or_else(|| self.err())?;
        if t.kind != TokenKind::String {
            return Err(self.err());
        }
        self.at += 1;
        string_value(t.bytes(self.sql), self.nbe).ok_or_else(|| self.err())
    }

    /// `name ['@' host]`.
    fn account(&mut self) -> R<AccountRef> {
        let name = self.name()?;
        let host = match self.peek() {
            Some(t) if t.kind == TokenKind::UserVar => {
                self.at += 1;
                let raw = &t.bytes(self.sql)[1..];
                let v = match raw.first() {
                    Some(b'\'' | b'"') => string_value(raw, self.nbe).ok_or_else(|| self.err())?,
                    _ => ident_value(raw).into_owned(),
                };
                String::from_utf8_lossy(&v).into_owned()
            }
            Some(t) if t.bytes(self.sql) == b"@" => {
                self.at += 1;
                self.name()?
            }
            _ => "%".to_owned(),
        };
        Ok(AccountRef { name, host })
    }

    /// An account or `CURRENT_USER[()]`.
    fn user_spec(&mut self) -> R<UserSpec> {
        if self.eat_word("CURRENT_USER") {
            if self.eat_punct("(") && !self.eat_punct(")") {
                return Err(self.err());
            }
            return Ok(UserSpec::Current);
        }
        Ok(UserSpec::Named(self.account()?))
    }

    fn accounts(&mut self) -> R<Vec<AccountRef>> {
        let mut v = vec![self.account()?];
        while self.eat_punct(",") {
            v.push(self.account()?);
        }
        Ok(v)
    }

    fn user_specs(&mut self) -> R<Vec<UserSpec>> {
        let mut v = vec![self.user_spec()?];
        while self.eat_punct(",") {
            v.push(self.user_spec()?);
        }
        Ok(v)
    }
}

/// Parses `sql` if it is an account statement (see the module docs).
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<R<AccountStmt>> {
    let tokens = lex::lex(sql, opts).ok()?;
    let toks: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut p = P {
        sql,
        toks,
        at: 0,
        nbe: opts.no_backslash_escapes,
    };
    let ours = p.word("CREATE") && (p.is_word(1, "USER") || p.is_word(1, "ROLE"))
        || p.word("ALTER") && p.is_word(1, "USER")
        || p.word("DROP") && (p.is_word(1, "USER") || p.is_word(1, "ROLE"))
        || p.word("GRANT")
        || p.word("REVOKE")
        || p.word("SHOW") && p.is_word(1, "GRANTS")
        || p.word("FLUSH") && p.is_word(1, "PRIVILEGES")
        || p.word("RENAME") && p.is_word(1, "USER")
        || p.word("SET")
            && (p.is_word(1, "ROLE") || p.is_word(1, "DEFAULT") && p.is_word(2, "ROLE"));
    if !ours {
        return None;
    }
    Some(stmt(&mut p))
}

fn stmt(p: &mut P<'_>) -> R<AccountStmt> {
    if p.eat_words(&["FLUSH", "PRIVILEGES"]) {
        p.end()?;
        return Ok(AccountStmt::FlushPrivileges);
    }
    if p.eat_words(&["RENAME", "USER"]) {
        return Err(unsupported("RENAME USER"));
    }
    if p.eat_words(&["SHOW", "GRANTS"]) {
        let mut account = None;
        let mut using = Vec::new();
        if p.eat_word("FOR") {
            account = Some(p.user_spec()?);
            if p.eat_word("USING") {
                using = p.accounts()?;
            }
        }
        p.end()?;
        return Ok(AccountStmt::ShowGrants { account, using });
    }
    if p.eat_words(&["CREATE", "USER"]) {
        let if_not_exists = p.eat_words(&["IF", "NOT", "EXISTS"]);
        let users = user_items(p, false)?;
        let options = user_options(p)?;
        p.end()?;
        return Ok(AccountStmt::CreateUser {
            if_not_exists,
            users,
            options,
        });
    }
    if p.eat_words(&["ALTER", "USER"]) {
        let if_exists = p.eat_words(&["IF", "EXISTS"]);
        let users = user_items(p, true)?;
        let options = user_options(p)?;
        p.end()?;
        return Ok(AccountStmt::AlterUser {
            if_exists,
            users,
            options,
        });
    }
    if p.eat_words(&["DROP", "USER"]) {
        let if_exists = p.eat_words(&["IF", "EXISTS"]);
        let users = p.accounts()?;
        p.end()?;
        return Ok(AccountStmt::DropUser { if_exists, users });
    }
    if p.eat_words(&["CREATE", "ROLE"]) {
        let if_not_exists = p.eat_words(&["IF", "NOT", "EXISTS"]);
        let roles = p.accounts()?;
        if p.word("WITH") {
            return Err(unsupported("CREATE ROLE ... WITH ADMIN"));
        }
        p.end()?;
        return Ok(AccountStmt::CreateRole {
            if_not_exists,
            roles,
        });
    }
    if p.eat_words(&["DROP", "ROLE"]) {
        let if_exists = p.eat_words(&["IF", "EXISTS"]);
        let roles = p.accounts()?;
        p.end()?;
        return Ok(AccountStmt::DropRole { if_exists, roles });
    }
    if p.eat_word("SET") {
        if p.eat_word("ROLE") {
            let c = role_choice(p)?;
            p.end()?;
            return Ok(AccountStmt::SetRole(c));
        }
        p.expect_word("DEFAULT")?;
        p.expect_word("ROLE")?;
        let roles = role_choice(p)?;
        let users = if p.eat_word("TO") || p.eat_word("FOR") {
            p.user_specs()?
        } else {
            Vec::new()
        };
        p.end()?;
        return Ok(AccountStmt::SetDefaultRole { roles, users });
    }
    if p.eat_word("GRANT") {
        return grant(p);
    }
    if p.eat_word("REVOKE") {
        return revoke(p);
    }
    Err(p.err())
}

fn role_choice(p: &mut P<'_>) -> R<RoleChoice> {
    if p.eat_word("DEFAULT") {
        return Ok(RoleChoice::Default);
    }
    if p.eat_word("NONE") {
        return Ok(RoleChoice::None);
    }
    if p.eat_word("ALL") {
        if p.eat_word("EXCEPT") {
            return Ok(RoleChoice::AllExcept(p.accounts()?));
        }
        return Ok(RoleChoice::All);
    }
    Ok(RoleChoice::List(p.accounts()?))
}

fn user_items(p: &mut P<'_>, alter: bool) -> R<Vec<UserItem>> {
    let mut v = Vec::new();
    loop {
        let user = if alter {
            p.user_spec()?
        } else {
            UserSpec::Named(p.account()?)
        };
        let mut identified = None;
        if p.eat_word("IDENTIFIED") {
            let mut plugin = None;
            if p.eat_word("WITH") {
                plugin = Some(p.name()?.to_ascii_lowercase());
            }
            let password = if p.eat_word("BY") {
                if p.eat_word("PASSWORD") {
                    return Err(unsupported("IDENTIFIED BY PASSWORD 'hash'"));
                }
                p.string()?
            } else if p.word("AS") || p.word("USING") {
                return Err(unsupported("IDENTIFIED WITH ... AS 'hash'"));
            } else if plugin.is_some() {
                Vec::new()
            } else {
                return Err(p.err());
            };
            let retain_current = p.eat_words(&["RETAIN", "CURRENT", "PASSWORD"]);
            identified = Some(Identified {
                password: Zeroizing::new(password),
                plugin,
                retain_current,
            });
        }
        v.push(UserItem { user, identified });
        if !p.eat_punct(",") {
            break;
        }
    }
    Ok(v)
}

fn user_options(p: &mut P<'_>) -> R<UserOptions> {
    let mut o = UserOptions::default();
    loop {
        if p.eat_words(&["DEFAULT", "ROLE"]) {
            o.default_roles = p.accounts()?;
        } else if p.eat_word("REQUIRE") {
            o.require = Some(if p.eat_word("NONE") {
                RequireTls::None
            } else if p.eat_word("SSL") {
                RequireTls::Ssl
            } else if p.eat_word("X509") {
                RequireTls::X509
            } else {
                return Err(unsupported("REQUIRE SUBJECT, ISSUER or CIPHER"));
            });
        } else if p.eat_words(&["PASSWORD", "EXPIRE"]) {
            if p.eat_word("DEFAULT") {
                o.clauses.password_lifetime = Some(None);
            } else if p.eat_word("NEVER") {
                o.clauses.password_lifetime = Some(Some(0));
            } else if p.eat_word("INTERVAL") {
                let n = p.number()?;
                p.expect_word("DAY")?;
                o.clauses.password_lifetime = Some(Some(i16::try_from(n).map_err(|_| p.err())?));
            } else {
                o.expire = Some(true);
            }
        } else if p.eat_words(&["PASSWORD", "HISTORY"]) {
            o.clauses.password_history = Some(if p.eat_word("DEFAULT") {
                None
            } else {
                Some(i16::try_from(p.number()?).map_err(|_| p.err())?)
            });
        } else if p.eat_words(&["PASSWORD", "REUSE", "INTERVAL"]) {
            o.clauses.password_reuse_days = Some(if p.eat_word("DEFAULT") {
                None
            } else {
                let n = i16::try_from(p.number()?).map_err(|_| p.err())?;
                p.expect_word("DAY")?;
                Some(n)
            });
        } else if p.eat_words(&["PASSWORD", "REQUIRE", "CURRENT"]) {
            o.clauses.require_current = Some(if p.eat_word("OPTIONAL") {
                RequireCurrent::Optional
            } else if p.eat_word("DEFAULT") {
                RequireCurrent::Default
            } else {
                RequireCurrent::Yes
            });
        } else if p.eat_word("FAILED_LOGIN_ATTEMPTS") {
            o.clauses.failed_login_attempts =
                Some(Some(u16::try_from(p.number()?).map_err(|_| p.err())?));
        } else if p.eat_word("PASSWORD_LOCK_TIME") {
            o.clauses.password_lock_minutes = Some(Some(if p.eat_word("UNBOUNDED") {
                -1
            } else {
                i32::try_from(p.number()?).map_err(|_| p.err())?
            }));
        } else if p.eat_word("ACCOUNT") {
            if p.eat_word("LOCK") {
                o.lock = Some(true);
            } else if p.eat_word("UNLOCK") {
                o.lock = Some(false);
            } else {
                return Err(p.err());
            }
        } else if p.eat_words(&["DISCARD", "OLD", "PASSWORD"]) {
            o.discard_old = true;
        } else if p.word("WITH") || p.word("PASSWORD") || p.word("COMMENT") || p.word("ATTRIBUTE") {
            let t = p.peek().map(|t| p.text(t)).unwrap_or_default();
            return Err(unsupported(&format!("{} options", t.to_ascii_uppercase())));
        } else {
            return Ok(o);
        }
    }
}

/// Whether `ON` follows at the top level before `TO` / `FROM`.
fn has_on(p: &P<'_>) -> bool {
    let mut depth = 0i32;
    for i in p.at..p.toks.len() {
        let t = p.toks[i];
        match t.bytes(p.sql) {
            b"(" => depth += 1,
            b")" => depth -= 1,
            _ => {}
        }
        if depth == 0 && p.is_word(i, "ON") {
            return true;
        }
        if depth == 0 && (p.is_word(i, "TO") || p.is_word(i, "FROM")) {
            return false;
        }
    }
    false
}

/// `priv [(cols)] {, priv [(cols)]}` up to `ON` (or `FROM` for `REVOKE
/// ALL, GRANT OPTION`).
fn priv_list(p: &mut P<'_>) -> R<Vec<PrivItem>> {
    let mut v = Vec::new();
    loop {
        let mut words = Vec::new();
        while let Some(t) = p.peek() {
            if t.kind != TokenKind::Word || p.word("ON") || p.word("FROM") || p.word("TO") {
                break;
            }
            words.push(p.text(t).to_ascii_uppercase());
            p.at += 1;
        }
        if words.is_empty() {
            return Err(p.err());
        }
        let mut columns = Vec::new();
        if p.eat_punct("(") {
            loop {
                columns.push(p.name()?);
                if p.eat_punct(")") {
                    break;
                }
                if !p.eat_punct(",") {
                    return Err(p.err());
                }
            }
        }
        v.push(PrivItem {
            name: words.join(" "),
            columns,
        });
        if !p.eat_punct(",") {
            return Ok(v);
        }
    }
}

fn target(p: &mut P<'_>) -> R<GrantOn> {
    let kind = if p.eat_word("TABLE") {
        ObjectType::Table
    } else if p.eat_word("FUNCTION") {
        ObjectType::Function
    } else if p.eat_word("PROCEDURE") {
        ObjectType::Procedure
    } else {
        ObjectType::Table
    };
    let star = |p: &mut P<'_>| p.eat_punct("*");
    if star(p) {
        if p.eat_punct(".") {
            if star(p) {
                return Ok(GrantOn::Tenant);
            }
            return Err(p.err());
        }
        return Ok(GrantOn::Database(None));
    }
    let first = p.name()?;
    if p.eat_punct(".") {
        if star(p) {
            return Ok(GrantOn::Database(Some(first)));
        }
        let name = p.name()?;
        return Ok(GrantOn::Object {
            db: Some(first),
            name,
            kind,
        });
    }
    Ok(GrantOn::Object {
        db: None,
        name: first,
        kind,
    })
}

fn grant(p: &mut P<'_>) -> R<AccountStmt> {
    if p.word("PROXY") {
        return Err(unsupported("GRANT PROXY"));
    }
    if has_on(p) {
        let privileges = priv_list(p)?;
        p.expect_word("ON")?;
        let on = target(p)?;
        p.expect_word("TO")?;
        let to = p.user_specs()?;
        if p.word("IDENTIFIED") || p.word("REQUIRE") {
            return Err(unsupported("GRANT ... IDENTIFIED BY or REQUIRE"));
        }
        let mut grant_option = false;
        while p.eat_word("WITH") {
            if p.eat_words(&["GRANT", "OPTION"]) {
                grant_option = true;
            } else {
                return Err(unsupported("GRANT ... WITH resource options"));
            }
        }
        if p.word("AS") {
            return Err(unsupported("GRANT ... AS"));
        }
        p.end()?;
        return Ok(AccountStmt::Grant {
            privileges,
            on,
            to,
            grant_option,
        });
    }
    let roles = p.accounts()?;
    p.expect_word("TO")?;
    let to = p.user_specs()?;
    let admin_option = p.eat_words(&["WITH", "ADMIN", "OPTION"]);
    p.end()?;
    Ok(AccountStmt::GrantRoles {
        roles,
        to,
        admin_option,
    })
}

fn revoke(p: &mut P<'_>) -> R<AccountStmt> {
    if p.word("PROXY") {
        return Err(unsupported("REVOKE PROXY"));
    }
    p.eat_words(&["IF", "EXISTS"]);
    if has_on(p) {
        let privileges = priv_list(p)?;
        p.expect_word("ON")?;
        let on = target(p)?;
        p.expect_word("FROM")?;
        let from = p.user_specs()?;
        p.end()?;
        return Ok(AccountStmt::Revoke {
            privileges,
            on,
            from,
        });
    }
    // REVOKE ALL [PRIVILEGES], GRANT OPTION FROM ...
    let save = p.at;
    if p.eat_word("ALL") {
        p.eat_word("PRIVILEGES");
        if p.eat_punct(",") && p.eat_words(&["GRANT", "OPTION"]) && p.eat_word("FROM") {
            let from = p.user_specs()?;
            p.end()?;
            return Ok(AccountStmt::RevokeAll { from });
        }
        p.at = save;
    }
    let roles = p.accounts()?;
    p.expect_word("FROM")?;
    let from = p.user_specs()?;
    p.end()?;
    Ok(AccountStmt::RevokeRoles { roles, from })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_proto_mysql::Persona;

    fn parse_ok(s: &str) -> AccountStmt {
        let opts = LexOptions::for_persona(Persona::MySql84);
        match parse(s.as_bytes(), &opts) {
            Some(Ok(st)) => st,
            other => panic!("{s}: {other:?}"),
        }
    }

    fn acc(n: &str, h: &str) -> AccountRef {
        AccountRef {
            name: n.into(),
            host: h.into(),
        }
    }

    #[test]
    fn grants_and_targets() {
        assert_eq!(
            parse_ok(
                "GRANT SELECT (a, `B`), INSERT ON shop.orders TO 'bob'@'%', alice WITH GRANT OPTION"
            ),
            AccountStmt::Grant {
                privileges: vec![
                    PrivItem {
                        name: "SELECT".into(),
                        columns: vec!["a".into(), "B".into()]
                    },
                    PrivItem {
                        name: "INSERT".into(),
                        columns: vec![]
                    },
                ],
                on: GrantOn::Object {
                    db: Some("shop".into()),
                    name: "orders".into(),
                    kind: ObjectType::Table
                },
                to: vec![
                    UserSpec::Named(acc("bob", "%")),
                    UserSpec::Named(acc("alice", "%"))
                ],
                grant_option: true,
            }
        );
        assert!(matches!(
            parse_ok("grant all privileges on *.* to app"),
            AccountStmt::Grant {
                on: GrantOn::Tenant,
                ..
            }
        ));
        assert!(matches!(
            parse_ok("GRANT CREATE TEMPORARY TABLES, LOCK TABLES ON `crm`.* TO `u`@`localhost`"),
            AccountStmt::Grant { on: GrantOn::Database(Some(_)), ref privileges, ref to, .. }
                if privileges[0].name == "CREATE TEMPORARY TABLES"
                && to[0] == UserSpec::Named(acc("u", "localhost"))
        ));
        assert!(matches!(
            parse_ok("GRANT EXECUTE ON FUNCTION shop.f TO u"),
            AccountStmt::Grant {
                on: GrantOn::Object {
                    kind: ObjectType::Function,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            parse_ok("GRANT SELECT ON * TO u"),
            AccountStmt::Grant {
                on: GrantOn::Database(None),
                ..
            }
        ));
    }

    #[test]
    fn roles_and_revokes() {
        assert_eq!(
            parse_ok("GRANT r1, 'r2'@'%' TO bob WITH ADMIN OPTION"),
            AccountStmt::GrantRoles {
                roles: vec![acc("r1", "%"), acc("r2", "%")],
                to: vec![UserSpec::Named(acc("bob", "%"))],
                admin_option: true
            }
        );
        assert_eq!(
            parse_ok("REVOKE ALL PRIVILEGES, GRANT OPTION FROM bob"),
            AccountStmt::RevokeAll {
                from: vec![UserSpec::Named(acc("bob", "%"))]
            }
        );
        assert!(matches!(
            parse_ok("REVOKE ALL ON shop.* FROM bob"),
            AccountStmt::Revoke {
                on: GrantOn::Database(Some(_)),
                ..
            }
        ));
        assert!(matches!(
            parse_ok("REVOKE r1 FROM bob"),
            AccountStmt::RevokeRoles { .. }
        ));
        assert_eq!(
            parse_ok("SET ROLE ALL"),
            AccountStmt::SetRole(RoleChoice::All)
        );
        assert!(matches!(
            parse_ok("SET DEFAULT ROLE r1 FOR bob"),
            AccountStmt::SetDefaultRole { .. }
        ));
        assert_eq!(
            parse_ok("SHOW GRANTS FOR CURRENT_USER()"),
            AccountStmt::ShowGrants {
                account: Some(UserSpec::Current),
                using: vec![]
            }
        );
    }

    #[test]
    fn policy_clauses() {
        match parse_ok(
            "ALTER USER bob FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED \
             PASSWORD EXPIRE INTERVAL 90 DAY PASSWORD HISTORY 5 \
             PASSWORD REUSE INTERVAL DEFAULT PASSWORD REQUIRE CURRENT OPTIONAL",
        ) {
            AccountStmt::AlterUser { options, .. } => {
                let c = options.clauses;
                assert_eq!(c.failed_login_attempts, Some(Some(3)));
                assert_eq!(c.password_lock_minutes, Some(Some(-1)));
                assert_eq!(c.password_lifetime, Some(Some(90)));
                assert_eq!(c.password_history, Some(Some(5)));
                assert_eq!(c.password_reuse_days, Some(None));
                assert_eq!(c.require_current, Some(RequireCurrent::Optional));
                assert_eq!(options.expire, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn users() {
        match parse_ok(
            "CREATE USER IF NOT EXISTS 'bob'@'%' IDENTIFIED BY 's3cret', carol \
             IDENTIFIED WITH caching_sha2_password BY 'x' ACCOUNT LOCK",
        ) {
            AccountStmt::CreateUser {
                if_not_exists,
                users,
                options,
            } => {
                assert!(if_not_exists);
                assert_eq!(users.len(), 2);
                assert_eq!(
                    users[0].identified.as_ref().map(|i| i.password.to_vec()),
                    Some(b"s3cret".to_vec())
                );
                assert_eq!(options.lock, Some(true));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse_ok("DROP USER IF EXISTS bob, 'c'@'h'"),
            AccountStmt::DropUser {
                if_exists: true,
                ..
            }
        ));
        let opts = LexOptions::for_persona(Persona::MySql84);
        assert!(parse(b"SELECT 1", &opts).is_none());
        assert!(parse(b"SET @a = 1", &opts).is_none());
        assert!(matches!(
            parse(b"GRANT PROXY ON a TO b", &opts),
            Some(Err(_))
        ));
        assert!(matches!(
            parse(
                b"CREATE USER u IDENTIFIED WITH mysql_native_password AS '*AB'",
                &opts
            ),
            Some(Err(_))
        ));
    }
}
