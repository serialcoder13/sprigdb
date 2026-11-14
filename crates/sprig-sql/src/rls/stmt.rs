//! Row-level security statements: parsing only. The gateway resolves names
//! against the session's snapshot, checks `ALTER` on the table, validates the
//! expressions ([`super::expr`]) and writes the catalog.
//!
//! ```text
//! CREATE POLICY name ON [db.]table [AS PERMISSIVE | RESTRICTIVE]
//!     [FOR cmd {, cmd}] [TO grantee {, grantee}]
//!     [USING (expr)] [WITH CHECK (expr)]
//! ALTER POLICY name ON [db.]table RENAME TO new
//! ALTER POLICY name ON [db.]table [TO grantee {, grantee}] [USING (expr)] [WITH CHECK (expr)]
//! DROP POLICY [IF EXISTS] name ON [db.]table
//! ALTER TABLE [db.]table ENABLE | DISABLE | FORCE | NO FORCE ROW LEVEL SECURITY
//! ALTER TABLE [db.]table SPRIG ROW LEVEL SECURITY ON | OFF     -- ENABLE | DISABLE
//! SHOW POLICIES [FROM | IN db | ON [db.]table]
//! SHOW CREATE POLICY name ON [db.]table
//! cmd     := ALL | SELECT | INSERT | UPDATE | DELETE
//! grantee := PUBLIC | CURRENT_USER [()] | name ['@' host]
//! ```
//!
//! [`parse`] returns `None` for text that isn't one of these, so every
//! other path keeps it. Expression texts are kept exactly as written
//! (between the clause's parentheses).

use sprig_auth::authz::stmt::AccountRef;
use sprig_common::SprigError;

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};

type R<T> = Result<T, SprigError>;

/// A table as written: `[db.]name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    /// The database, when qualified.
    pub db: Option<String>,
    /// The table.
    pub name: String,
}

/// Permissive policies are OR-ed, restrictive ones AND-ed (PostgreSQL's
/// model).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyMode {
    /// `AS PERMISSIVE` (the default).
    Permissive,
    /// `AS RESTRICTIVE`.
    Restrictive,
}

impl PolicyMode {
    /// The catalog's `mode` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            PolicyMode::Permissive => "permissive",
            PolicyMode::Restrictive => "restrictive",
        }
    }
}

/// The commands a policy applies to, as bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Commands(pub u8);

impl Commands {
    /// `SELECT`.
    pub const SELECT: Commands = Commands(1);
    /// `INSERT`.
    pub const INSERT: Commands = Commands(2);
    /// `UPDATE`.
    pub const UPDATE: Commands = Commands(4);
    /// `DELETE`.
    pub const DELETE: Commands = Commands(8);
    /// `ALL`.
    pub const ALL: Commands = Commands(15);

    /// Whether every command of `o` is in `self`.
    #[must_use]
    pub const fn has(self, o: Commands) -> bool {
        self.0 & o.0 == o.0 && o.0 != 0
    }

    /// The union.
    #[must_use]
    pub const fn with(self, o: Commands) -> Commands {
        Commands(self.0 | o.0)
    }

    /// The catalog's `SET('select','insert','update','delete')` text.
    #[must_use]
    pub fn set_text(self) -> String {
        let mut v = Vec::new();
        for (c, n) in [
            (Self::SELECT, "select"),
            (Self::INSERT, "insert"),
            (Self::UPDATE, "update"),
            (Self::DELETE, "delete"),
        ] {
            if self.has(c) {
                v.push(n);
            }
        }
        v.join(",")
    }

    /// The commands of a catalog `SET` value.
    #[must_use]
    pub fn from_set_text(s: &str) -> Commands {
        let mut c = Commands(0);
        for part in s.split(',') {
            c = c.with(match part.trim().to_ascii_lowercase().as_str() {
                "select" => Self::SELECT,
                "insert" => Self::INSERT,
                "update" => Self::UPDATE,
                "delete" => Self::DELETE,
                _ => Commands(0),
            });
        }
        c
    }

    /// `FOR ...` as `SHOW CREATE POLICY` writes it.
    #[must_use]
    pub fn sql(self) -> String {
        if self == Self::ALL {
            return "ALL".to_owned();
        }
        self.set_text().to_ascii_uppercase().replace(',', ", ")
    }
}

/// Who a policy applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PolicyGrantee {
    /// `PUBLIC`: everyone the table's security binds.
    Public,
    /// `CURRENT_USER`: the account running the statement.
    CurrentUser,
    /// A user or role of the tenant.
    Account(AccountRef),
}

/// `ENABLE`, `DISABLE`, `FORCE` or `NO FORCE ROW LEVEL SECURITY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecuritySwitch {
    /// `ENABLE` (and `SPRIG ROW LEVEL SECURITY ON`).
    Enable,
    /// `DISABLE` (and `SPRIG ROW LEVEL SECURITY OFF`).
    Disable,
    /// `FORCE`: managers of the table are bound too.
    Force,
    /// `NO FORCE`.
    NoForce,
}

/// A parsed row-level security statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyStmt {
    /// `CREATE POLICY`.
    Create {
        /// The policy's name.
        name: String,
        /// Its table.
        table: TableName,
        /// Permissive or restrictive.
        mode: PolicyMode,
        /// The commands.
        commands: Commands,
        /// The grantees; empty is `PUBLIC`.
        to: Vec<PolicyGrantee>,
        /// `USING (...)`, the text between the parentheses.
        using: Option<String>,
        /// `WITH CHECK (...)`.
        check: Option<String>,
        /// `SPRIG MONGO USING '<filter>'`: the `USING` as a MongoDB
        /// filter (extended JSON), kept for change streams.
        mongo_using: Option<String>,
        /// `SPRIG MONGO ... CHECK '<filter>'`.
        mongo_check: Option<String>,
    },
    /// `ALTER POLICY ... RENAME TO`.
    Rename {
        /// The policy.
        name: String,
        /// Its table.
        table: TableName,
        /// The new name.
        to: String,
    },
    /// `ALTER POLICY` changing grantees or expressions.
    Alter {
        /// The policy.
        name: String,
        /// Its table.
        table: TableName,
        /// New grantees, if given.
        to: Option<Vec<PolicyGrantee>>,
        /// A new `USING`, if given.
        using: Option<String>,
        /// A new `WITH CHECK`, if given.
        check: Option<String>,
    },
    /// `DROP POLICY`.
    Drop {
        /// The policy.
        name: String,
        /// Its table.
        table: TableName,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `ALTER TABLE ... ROW LEVEL SECURITY`.
    Security {
        /// The table.
        table: TableName,
        /// The switch.
        switch: SecuritySwitch,
    },
    /// `SHOW POLICIES`.
    Show {
        /// `FROM db`.
        db: Option<String>,
        /// `ON table`.
        table: Option<TableName>,
    },
    /// `SHOW CREATE POLICY`.
    ShowCreate {
        /// The policy.
        name: String,
        /// Its table.
        table: TableName,
    },
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

struct P<'a> {
    sql: &'a [u8],
    toks: Vec<&'a Token>,
    at: usize,
    nbe: bool,
}

impl<'a> P<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.at).copied()
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

    /// An identifier (a string too, for account names).
    fn name(&mut self, strings: bool) -> R<String> {
        let t = self.peek().ok_or_else(|| self.err())?;
        let v = match t.kind {
            TokenKind::Word | TokenKind::QuotedIdent => {
                String::from_utf8_lossy(&ident_value(t.bytes(self.sql))).into_owned()
            }
            TokenKind::String if strings => string_value(t.bytes(self.sql), self.nbe)
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .ok_or_else(|| self.err())?,
            _ => return Err(self.err()),
        };
        self.at += 1;
        Ok(v)
    }

    /// `[db.]name`.
    fn table(&mut self) -> R<TableName> {
        let first = self.name(false)?;
        if self.eat_punct(".") {
            let name = self.name(false)?;
            return Ok(TableName {
                db: Some(first),
                name,
            });
        }
        Ok(TableName {
            db: None,
            name: first,
        })
    }

    /// `name ['@' host]`.
    fn account(&mut self) -> R<AccountRef> {
        let name = self.name(true)?;
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
                self.name(true)?
            }
            _ => "%".to_owned(),
        };
        Ok(AccountRef { name, host })
    }

    fn grantees(&mut self) -> R<Vec<PolicyGrantee>> {
        let mut v = Vec::new();
        loop {
            if self.eat_word("PUBLIC") {
                v.push(PolicyGrantee::Public);
            } else if self.eat_word("CURRENT_USER") {
                if self.eat_punct("(") && !self.eat_punct(")") {
                    return Err(self.err());
                }
                v.push(PolicyGrantee::CurrentUser);
            } else {
                v.push(PolicyGrantee::Account(self.account()?));
            }
            if !self.eat_punct(",") {
                break;
            }
        }
        // `TO PUBLIC` alone is the same as no grantees.
        if v.contains(&PolicyGrantee::Public) {
            return Ok(Vec::new());
        }
        Ok(v)
    }

    /// `( expr )`: the text between the parentheses, which must balance.
    fn paren_text(&mut self) -> R<String> {
        if !self.eat_punct("(") {
            return Err(self.err());
        }
        let start = self.peek().ok_or_else(|| self.err())?.start as usize;
        let mut depth = 1usize;
        while let Some(t) = self.peek() {
            if matches!(t.kind, TokenKind::Punct | TokenKind::Op) {
                match t.bytes(self.sql) {
                    b"(" => depth += 1,
                    b")" => {
                        depth -= 1;
                        if depth == 0 {
                            let end = t.start as usize;
                            self.at += 1;
                            let text = std::str::from_utf8(&self.sql[start..end])
                                .map_err(|_| self.err())?
                                .trim()
                                .to_owned();
                            if text.is_empty() {
                                return Err(self.err());
                            }
                            return Ok(text);
                        }
                    }
                    _ => {}
                }
            }
            self.at += 1;
        }
        Err(self.err())
    }

    fn command(&mut self) -> R<Commands> {
        for (w, c) in [
            ("ALL", Commands::ALL),
            ("SELECT", Commands::SELECT),
            ("INSERT", Commands::INSERT),
            ("UPDATE", Commands::UPDATE),
            ("DELETE", Commands::DELETE),
        ] {
            if self.eat_word(w) {
                return Ok(c);
            }
        }
        Err(self.err())
    }
}

/// Parses `sql` if it is a row-level security statement (see the module
/// docs); `None` otherwise.
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<R<PolicyStmt>> {
    let all = lex::lex(sql, opts).ok()?;
    let toks: Vec<&Token> = all.iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut p = P {
        sql,
        toks,
        at: 0,
        nbe: opts.no_backslash_escapes,
    };
    let kind = if p.is_word(0, "CREATE") && p.is_word(1, "POLICY") {
        1
    } else if p.is_word(0, "ALTER") && p.is_word(1, "POLICY") {
        2
    } else if p.is_word(0, "DROP") && p.is_word(1, "POLICY") {
        3
    } else if p.is_word(0, "SHOW") && p.is_word(1, "POLICIES") {
        4
    } else if p.is_word(0, "SHOW") && p.is_word(1, "CREATE") && p.is_word(2, "POLICY") {
        5
    } else if p.is_word(0, "ALTER") && p.is_word(1, "TABLE") && is_security_switch(&p) {
        6
    } else {
        return None;
    };
    Some(match kind {
        1 => create(&mut p),
        2 => alter(&mut p),
        3 => drop(&mut p),
        4 => show(&mut p),
        5 => show_create(&mut p),
        _ => security(&mut p),
    })
}

/// `ALTER TABLE t ... ROW LEVEL SECURITY` (the words after the table name).
fn is_security_switch(p: &P<'_>) -> bool {
    // ALTER TABLE name [. name] <switch words> ROW LEVEL SECURITY
    let mut i = 3;
    if p.toks.get(3).is_some_and(|t| t.bytes(p.sql) == b".") {
        i = 5;
    }
    let tail_ok =
        |j: usize| p.is_word(j, "ROW") && p.is_word(j + 1, "LEVEL") && p.is_word(j + 2, "SECURITY");
    (p.is_word(i, "ENABLE") || p.is_word(i, "DISABLE") || p.is_word(i, "FORCE")) && tail_ok(i + 1)
        || p.is_word(i, "NO") && p.is_word(i + 1, "FORCE") && tail_ok(i + 2)
        || p.is_word(i, "SPRIG") && tail_ok(i + 1)
}

fn create(p: &mut P<'_>) -> R<PolicyStmt> {
    p.at = 2;
    let name = p.name(false)?;
    p.expect_word("ON")?;
    let table = p.table()?;
    let mut mode = PolicyMode::Permissive;
    let mut commands = None;
    let mut to = Vec::new();
    let mut using = None;
    let mut check = None;
    if p.eat_word("AS") {
        mode = if p.eat_word("PERMISSIVE") {
            PolicyMode::Permissive
        } else if p.eat_word("RESTRICTIVE") {
            PolicyMode::Restrictive
        } else {
            return Err(p.err());
        };
    }
    if p.eat_word("FOR") {
        let mut c = p.command()?;
        while p.eat_punct(",") {
            c = c.with(p.command()?);
        }
        commands = Some(c);
    }
    if p.eat_word("TO") {
        to = p.grantees()?;
    }
    if p.eat_word("USING") {
        using = Some(p.paren_text()?);
    }
    if p.eat_words(&["WITH", "CHECK"]) {
        check = Some(p.paren_text()?);
    }
    // The MongoDB forms of a collection's policy (`sprigCreatePolicy`
    // writes them; the SQL above is what the gateway and MariaDB apply).
    let mut mongo_using = None;
    let mut mongo_check = None;
    if p.eat_words(&["SPRIG", "MONGO"]) {
        if p.eat_word("USING") {
            mongo_using = Some(p.name(true)?);
        }
        if p.eat_word("CHECK") {
            mongo_check = Some(p.name(true)?);
        }
        if mongo_using.is_none() && mongo_check.is_none() {
            return Err(p.err());
        }
    }
    p.end()?;
    let commands = commands.unwrap_or(Commands::ALL);
    // PostgreSQL's rules: INSERT has no USING, SELECT and DELETE no WITH
    // CHECK.
    if using.is_some() && commands == Commands::INSERT {
        return Err(syntax(
            "USING: only WITH CHECK expression allowed for INSERT",
        ));
    }
    if check.is_some() && (commands == Commands::SELECT || commands == Commands::DELETE) {
        return Err(syntax(
            "WITH CHECK: WITH CHECK cannot be applied to SELECT or DELETE",
        ));
    }
    if using.is_none() && check.is_none() {
        return Err(syntax("a policy needs USING or WITH CHECK"));
    }
    Ok(PolicyStmt::Create {
        name,
        table,
        mode,
        commands,
        to,
        using,
        check,
        mongo_using,
        mongo_check,
    })
}

fn alter(p: &mut P<'_>) -> R<PolicyStmt> {
    p.at = 2;
    let name = p.name(false)?;
    p.expect_word("ON")?;
    let table = p.table()?;
    if p.eat_words(&["RENAME", "TO"]) {
        let to = p.name(false)?;
        p.end()?;
        return Ok(PolicyStmt::Rename { name, table, to });
    }
    let mut to = None;
    let mut using = None;
    let mut check = None;
    if p.eat_word("TO") {
        to = Some(p.grantees()?);
    }
    if p.eat_word("USING") {
        using = Some(p.paren_text()?);
    }
    if p.eat_words(&["WITH", "CHECK"]) {
        check = Some(p.paren_text()?);
    }
    p.end()?;
    if to.is_none() && using.is_none() && check.is_none() {
        return Err(p.err());
    }
    Ok(PolicyStmt::Alter {
        name,
        table,
        to,
        using,
        check,
    })
}

fn drop(p: &mut P<'_>) -> R<PolicyStmt> {
    p.at = 2;
    let if_exists = p.eat_words(&["IF", "EXISTS"]);
    let name = p.name(false)?;
    p.expect_word("ON")?;
    let table = p.table()?;
    p.end()?;
    Ok(PolicyStmt::Drop {
        name,
        table,
        if_exists,
    })
}

fn show(p: &mut P<'_>) -> R<PolicyStmt> {
    p.at = 2;
    let mut db = None;
    let mut table = None;
    if p.eat_word("FROM") || p.eat_word("IN") {
        db = Some(p.name(false)?);
    } else if p.eat_word("ON") {
        table = Some(p.table()?);
    }
    p.end()?;
    Ok(PolicyStmt::Show { db, table })
}

fn show_create(p: &mut P<'_>) -> R<PolicyStmt> {
    p.at = 3;
    let name = p.name(false)?;
    p.expect_word("ON")?;
    let table = p.table()?;
    p.end()?;
    Ok(PolicyStmt::ShowCreate { name, table })
}

fn security(p: &mut P<'_>) -> R<PolicyStmt> {
    p.at = 2;
    let table = p.table()?;
    let switch = if p.eat_word("ENABLE") {
        SecuritySwitch::Enable
    } else if p.eat_word("DISABLE") {
        SecuritySwitch::Disable
    } else if p.eat_word("FORCE") {
        SecuritySwitch::Force
    } else if p.eat_words(&["NO", "FORCE"]) {
        SecuritySwitch::NoForce
    } else if p.eat_word("SPRIG") {
        p.expect_word("ROW")?;
        p.expect_word("LEVEL")?;
        p.expect_word("SECURITY")?;
        let s = if p.eat_word("ON") {
            SecuritySwitch::Enable
        } else if p.eat_word("OFF") {
            SecuritySwitch::Disable
        } else {
            return Err(p.err());
        };
        p.end()?;
        return Ok(PolicyStmt::Security { table, switch: s });
    } else {
        return Err(p.err());
    };
    p.expect_word("ROW")?;
    p.expect_word("LEVEL")?;
    p.expect_word("SECURITY")?;
    p.end()?;
    Ok(PolicyStmt::Security { table, switch })
}

/// Quotes an identifier with backquotes.
#[must_use]
pub fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// `SHOW CREATE POLICY`'s text for a policy.
#[must_use]
#[allow(clippy::too_many_arguments)] // the policy's parts, one by one
pub fn create_sql(
    name: &str,
    db: &str,
    table: &str,
    mode: PolicyMode,
    commands: Commands,
    to: &[String],
    using: Option<&str>,
    check: Option<&str>,
) -> String {
    let mut s = format!(
        "CREATE POLICY {} ON {}.{} AS {} FOR {} TO {}",
        quote_ident(name),
        quote_ident(db),
        quote_ident(table),
        mode.as_str().to_ascii_uppercase(),
        commands.sql(),
        if to.is_empty() {
            "PUBLIC".to_owned()
        } else {
            to.join(", ")
        }
    );
    if let Some(u) = using {
        s.push_str(" USING (");
        s.push_str(u);
        s.push(')');
    }
    if let Some(c) = check {
        s.push_str(" WITH CHECK (");
        s.push_str(c);
        s.push(')');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> PolicyStmt {
        parse(s.as_bytes(), &LexOptions::mariadb())
            .expect("a policy statement")
            .expect("parses")
    }

    fn t(db: Option<&str>, n: &str) -> TableName {
        TableName {
            db: db.map(str::to_owned),
            name: n.to_owned(),
        }
    }

    #[test]
    fn create_in_both_styles() {
        assert_eq!(
            ok(
                "CREATE POLICY own ON shop.orders AS PERMISSIVE FOR SELECT, UPDATE, DELETE \
                TO app_user, 'bob'@'%' USING (owner_id = SPRIG_CLAIM('sub'))"
            ),
            PolicyStmt::Create {
                name: "own".into(),
                table: t(Some("shop"), "orders"),
                mode: PolicyMode::Permissive,
                commands: Commands::SELECT
                    .with(Commands::UPDATE)
                    .with(Commands::DELETE),
                to: vec![
                    PolicyGrantee::Account(AccountRef::any_host("app_user")),
                    PolicyGrantee::Account(AccountRef::any_host("bob")),
                ],
                using: Some("owner_id = SPRIG_CLAIM('sub')".into()),
                check: None,
                mongo_using: None,
                mongo_check: None,
            }
        );
        match ok("CREATE POLICY m ON c USING (TRUE) SPRIG MONGO USING '{\"o\": \"$$USER\"}'") {
            PolicyStmt::Create {
                mongo_using,
                mongo_check,
                ..
            } => {
                assert_eq!(mongo_using.as_deref(), Some("{\"o\": \"$$USER\"}"));
                assert!(mongo_check.is_none());
            }
            o => panic!("{o:?}"),
        }
        match ok("create policy p on t for insert with check ((a > 1) and b in (select x from u))")
        {
            PolicyStmt::Create {
                commands,
                check,
                using,
                to,
                ..
            } => {
                assert_eq!(commands, Commands::INSERT);
                assert_eq!(check.as_deref(), Some("(a > 1) and b in (select x from u)"));
                assert!(using.is_none());
                assert!(to.is_empty());
            }
            o => panic!("{o:?}"),
        }
        match ok("CREATE POLICY r ON t AS RESTRICTIVE TO PUBLIC USING (x <> ')')") {
            PolicyStmt::Create {
                mode, using, to, ..
            } => {
                assert_eq!(mode, PolicyMode::Restrictive);
                assert_eq!(using.as_deref(), Some("x <> ')'"));
                assert!(to.is_empty());
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn refusals() {
        let e = |s: &str| {
            parse(s.as_bytes(), &LexOptions::mariadb())
                .unwrap()
                .unwrap_err()
        };
        assert_eq!(
            e("CREATE POLICY p ON t FOR INSERT USING (a)")
                .protocol()
                .mysql_errno,
            1064
        );
        assert_eq!(
            e("CREATE POLICY p ON t FOR SELECT WITH CHECK (a)")
                .protocol()
                .mysql_errno,
            1064
        );
        assert_eq!(e("CREATE POLICY p ON t").protocol().mysql_errno, 1064);
        assert_eq!(
            e("CREATE POLICY p ON t USING (a").protocol().mysql_errno,
            1064
        );
        assert!(parse(b"CREATE TABLE p (a int)", &LexOptions::mariadb()).is_none());
        assert!(parse(b"ALTER TABLE t ADD COLUMN c int", &LexOptions::mariadb()).is_none());
    }

    #[test]
    fn alter_drop_show() {
        assert_eq!(
            ok("ALTER POLICY own ON orders RENAME TO mine"),
            PolicyStmt::Rename {
                name: "own".into(),
                table: t(None, "orders"),
                to: "mine".into()
            }
        );
        assert_eq!(
            ok("ALTER POLICY own ON orders TO CURRENT_USER() USING (a = 1)"),
            PolicyStmt::Alter {
                name: "own".into(),
                table: t(None, "orders"),
                to: Some(vec![PolicyGrantee::CurrentUser]),
                using: Some("a = 1".into()),
                check: None
            }
        );
        assert_eq!(
            ok("DROP POLICY IF EXISTS own ON `shop`.`orders`;"),
            PolicyStmt::Drop {
                name: "own".into(),
                table: t(Some("shop"), "orders"),
                if_exists: true
            }
        );
        assert_eq!(
            ok("SHOW POLICIES FROM shop"),
            PolicyStmt::Show {
                db: Some("shop".into()),
                table: None
            }
        );
        assert_eq!(
            ok("SHOW POLICIES ON shop.orders"),
            PolicyStmt::Show {
                db: None,
                table: Some(t(Some("shop"), "orders"))
            }
        );
        assert_eq!(
            ok("SHOW CREATE POLICY own ON orders"),
            PolicyStmt::ShowCreate {
                name: "own".into(),
                table: t(None, "orders")
            }
        );
    }

    #[test]
    fn security_switches() {
        for (s, w) in [
            (
                "ALTER TABLE shop.orders ENABLE ROW LEVEL SECURITY",
                SecuritySwitch::Enable,
            ),
            (
                "ALTER TABLE orders DISABLE ROW LEVEL SECURITY",
                SecuritySwitch::Disable,
            ),
            (
                "ALTER TABLE orders FORCE ROW LEVEL SECURITY",
                SecuritySwitch::Force,
            ),
            (
                "alter table orders no force row level security",
                SecuritySwitch::NoForce,
            ),
            (
                "ALTER TABLE shop.orders SPRIG ROW LEVEL SECURITY ON",
                SecuritySwitch::Enable,
            ),
            (
                "ALTER TABLE orders SPRIG ROW LEVEL SECURITY OFF",
                SecuritySwitch::Disable,
            ),
        ] {
            match ok(s) {
                PolicyStmt::Security { switch, .. } => assert_eq!(switch, w, "{s}"),
                o => panic!("{o:?}"),
            }
        }
        assert!(parse(b"ALTER TABLE orders FORCE", &LexOptions::mariadb()).is_none());
    }

    #[test]
    fn show_create_renders() {
        let s = create_sql(
            "own",
            "shop",
            "orders",
            PolicyMode::Permissive,
            Commands::SELECT.with(Commands::DELETE),
            &["`app`@`%`".into()],
            Some("owner_id = 1"),
            None,
        );
        assert_eq!(
            s,
            "CREATE POLICY `own` ON `shop`.`orders` AS PERMISSIVE FOR SELECT, DELETE TO \
             `app`@`%` USING (owner_id = 1)"
        );
        assert_eq!(Commands::from_set_text("select,update"), Commands(5));
        assert_eq!(Commands::ALL.sql(), "ALL");
    }
}
