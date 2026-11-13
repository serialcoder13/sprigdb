//! Account statements as the gateway parses them (`sprig_sql::account_stmt`
//! builds these): logical names only, privilege names normalized (upper
//! case, single spaces).

use zeroize::Zeroizing;

use super::privilege::ObjectType;
use crate::store::RequireTls;

/// `'name'@'host'`; the host is `%` when omitted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountRef {
    /// The user or role name.
    pub name: String,
    /// The host pattern.
    pub host: String,
}

impl AccountRef {
    /// `'name'@'%'`.
    #[must_use]
    pub fn any_host(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            host: "%".to_owned(),
        }
    }
}

impl std::fmt::Display for AccountRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'@'{}'", self.name, self.host)
    }
}

/// A user named in a statement, or `CURRENT_USER`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UserSpec {
    /// The session's own account.
    Current,
    /// A named account.
    Named(AccountRef),
}

/// `IDENTIFIED ...` of `CREATE USER` or `ALTER USER`.
#[derive(Clone, PartialEq, Eq)]
pub struct Identified {
    /// The clear-text password.
    pub password: Zeroizing<Vec<u8>>,
    /// `IDENTIFIED WITH <plugin>`, lower case.
    pub plugin: Option<String>,
    /// `RETAIN CURRENT PASSWORD`.
    pub retain_current: bool,
}

impl std::fmt::Debug for Identified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identified")
            .field("plugin", &self.plugin)
            .field("retain_current", &self.retain_current)
            .finish_non_exhaustive()
    }
}

/// One account of `CREATE USER` or `ALTER USER`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserItem {
    /// The account.
    pub user: UserSpec,
    /// Its password, if given.
    pub identified: Option<Identified>,
}

/// The options after the account list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserOptions {
    /// `ACCOUNT LOCK` / `ACCOUNT UNLOCK`.
    pub lock: Option<bool>,
    /// `PASSWORD EXPIRE` (and `PASSWORD EXPIRE NEVER` as `Some(false)`).
    pub expire: Option<bool>,
    /// `REQUIRE NONE | SSL | X509` (`X509` also for `SUBJECT` and
    /// `ISSUER`).
    pub require: Option<RequireTls>,
    /// `REQUIRE SUBJECT '..' [AND] ISSUER '..'`: `(subject, issuer)`, set
    /// whenever `require` is (`(None, None)` clears them).
    pub require_dn: (Option<String>, Option<String>),
    /// `DEFAULT ROLE ...` of `CREATE USER` (MySQL).
    pub default_roles: Vec<AccountRef>,
    /// `DISCARD OLD PASSWORD`.
    pub discard_old: bool,
    /// Chapter 14's account-level policy clauses (`FAILED_LOGIN_ATTEMPTS`,
    /// `PASSWORD_LOCK_TIME`, `PASSWORD EXPIRE INTERVAL | NEVER | DEFAULT`,
    /// `PASSWORD HISTORY`, `PASSWORD REUSE INTERVAL`, `PASSWORD REQUIRE
    /// CURRENT`).
    pub clauses: crate::policy::AccountClauses,
    /// The statement was `SET PASSWORD FOR acct = '...'`: MySQL 8.4 and
    /// MariaDB let it change another account's password with `UPDATE` on
    /// the `mysql` schema (a tenant-wide `UPDATE` here) instead of
    /// `CREATE USER` (bugfix-26).
    pub set_password: bool,
    /// `ATTRIBUTE '{json}'` (an object merged into the user's attributes,
    /// a `null` value removing its key, as MySQL 8.4) or `COMMENT 'text'`
    /// (`{"comment": text}`): what `SPRIG_USER_ATTR()` reads (chapter 29;
    /// decision bugfix-interim). JSON text of an object.
    pub attribute: Option<String>,
}

/// What `GRANT` and `REVOKE` act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantOn {
    /// `*.*`.
    Tenant,
    /// `db.*`, or `*` (the current database) as `None`.
    Database(Option<String>),
    /// `[TABLE|FUNCTION|PROCEDURE] [db.]name`.
    Object {
        /// The database; `None` for the current one.
        db: Option<String>,
        /// The object.
        name: String,
        /// What it is.
        kind: ObjectType,
    },
    /// `GRAPH [db.]g NODES L` or `GRAPH [db.]g RELATIONSHIPS T` (chapter
    /// 43, R8): one label or relationship type.
    GraphElement {
        /// The database; `None` for the current one.
        db: Option<String>,
        /// The graph.
        graph: String,
        /// A relationship type (else a label).
        rels: bool,
        /// The label or type.
        name: String,
    },
}

/// One privilege of a `GRANT` or `REVOKE` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivItem {
    /// The name, normalized (`SELECT`, `CREATE ROUTINE`, `ALL`).
    pub name: String,
    /// Its column list, if any.
    pub columns: Vec<String>,
}

/// `SET ROLE` and `SET DEFAULT ROLE` choices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleChoice {
    /// `DEFAULT`.
    Default,
    /// `NONE`.
    None,
    /// `ALL`.
    All,
    /// `ALL EXCEPT ...`.
    AllExcept(Vec<AccountRef>),
    /// A list.
    List(Vec<AccountRef>),
}

/// An account statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountStmt {
    /// `CREATE USER [IF NOT EXISTS] ...`.
    CreateUser {
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// The accounts.
        users: Vec<UserItem>,
        /// Options.
        options: UserOptions,
    },
    /// `ALTER USER [IF EXISTS] ...`.
    AlterUser {
        /// `IF EXISTS`.
        if_exists: bool,
        /// The accounts.
        users: Vec<UserItem>,
        /// Options.
        options: UserOptions,
    },
    /// `DROP USER [IF EXISTS] ...`.
    DropUser {
        /// `IF EXISTS`.
        if_exists: bool,
        /// The accounts.
        users: Vec<AccountRef>,
    },
    /// `CREATE ROLE [IF NOT EXISTS] ...`.
    CreateRole {
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// The roles.
        roles: Vec<AccountRef>,
    },
    /// `DROP ROLE [IF EXISTS] ...`.
    DropRole {
        /// `IF EXISTS`.
        if_exists: bool,
        /// The roles.
        roles: Vec<AccountRef>,
    },
    /// `GRANT privileges ON target TO accounts [WITH GRANT OPTION]`.
    Grant {
        /// The privileges.
        privileges: Vec<PrivItem>,
        /// Where.
        on: GrantOn,
        /// To whom.
        to: Vec<UserSpec>,
        /// `WITH GRANT OPTION`.
        grant_option: bool,
    },
    /// `REVOKE privileges ON target FROM accounts`.
    Revoke {
        /// The privileges (`GRANT OPTION` included as an item).
        privileges: Vec<PrivItem>,
        /// Where.
        on: GrantOn,
        /// From whom.
        from: Vec<UserSpec>,
    },
    /// `REVOKE ALL [PRIVILEGES], GRANT OPTION FROM accounts`.
    RevokeAll {
        /// From whom.
        from: Vec<UserSpec>,
    },
    /// `GRANT role, ... TO accounts [WITH ADMIN OPTION]`.
    GrantRoles {
        /// The roles.
        roles: Vec<AccountRef>,
        /// To whom (users or roles).
        to: Vec<UserSpec>,
        /// `WITH ADMIN OPTION`.
        admin_option: bool,
    },
    /// `REVOKE role, ... FROM accounts`.
    RevokeRoles {
        /// The roles.
        roles: Vec<AccountRef>,
        /// From whom.
        from: Vec<UserSpec>,
    },
    /// `SET DEFAULT ROLE ... TO users` (MySQL) or `SET DEFAULT ROLE r
    /// [FOR user]` (MariaDB).
    SetDefaultRole {
        /// The roles.
        roles: RoleChoice,
        /// The users; the session's own when empty.
        users: Vec<UserSpec>,
    },
    /// `SET ROLE ...`.
    SetRole(RoleChoice),
    /// `SHOW GRANTS [FOR account [USING roles]]`.
    ShowGrants {
        /// The account; the session's own when `None`.
        account: Option<UserSpec>,
        /// `USING`.
        using: Vec<AccountRef>,
    },
    /// `FLUSH PRIVILEGES`: a no-op.
    FlushPrivileges,
}
