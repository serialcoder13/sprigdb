//! MySQL account statements for tenant users: `CREATE`, `ALTER` and `DROP USER`, `CREATE`
//! and `DROP ROLE`, `GRANT` and `REVOKE` of privileges and roles, `SET
//! [DEFAULT] ROLE`, `SHOW GRANTS` and `FLUSH PRIVILEGES`.
//!
//! Each statement is checked against the session's compiled privileges,
//! planned against the snapshot (names to ids, levels, the privileges
//! each level accepts), then applied in one auth change, so every gateway
//! sees it at the next auth version. `CREATE`, `ALTER` and `DROP USER`
//! go through [`crate::admin`] (which owns users and passwords);
//! dropping a user also deletes its privilege rows in a second change.
//!
//! Who may run what (`SPRIG_TENANT_ADMIN` may run everything):
//!
//! | Statement | Needs | Refused with |
//! |---|---|---|
//! | `CREATE`, `ALTER`, `DROP USER`; another's `SHOW GRANTS` | `CREATE USER` | 1227; `SHOW GRANTS`: 1142 (MySQL) or 1044 (MariaDB) |
//! | `CREATE ROLE`, `DROP ROLE` | `CREATE ROLE` / `DROP ROLE`, or `CREATE USER` | 1227 |
//! | `GRANT`, `REVOKE` privileges | the privileges and `GRANT OPTION` at the level or above | 1045 (`*.*`), 1044 (`db.*`), 1142 (`db.t`) |
//! | `GRANT`, `REVOKE` roles | `ROLE_ADMIN`, or the role `WITH ADMIN OPTION` | 1227 |
//!
//! Server-wide privileges are dropped with warning 39350; an unknown one
//! fails with 3929 (MySQL) or 1064 (MariaDB). A grant to a missing account
//! fails with 1410 (MySQL) or 1133 (MariaDB), as each server does.

use std::collections::BTreeMap;

use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{ChangeOpts, MariaCatalog, TenantCatalog, VersionKind};
use sprig_common::ids::{RoleId, TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::Persona;

use super::compile::{CompiledPrivileges, Target};
use super::privilege::{
    Action, ActionSet, Level, ObjectType, PrivName, all_at, grantable_at, parse_privilege,
};
use super::show;
use super::state::AuthzState;
use super::stmt::{
    AccountRef, AccountStmt, GrantOn, PrivItem, RoleChoice, UserItem, UserOptions, UserSpec,
};
use super::store::{AuthzStore, Grantee, PrivTarget};
use crate::admin::{self, AccountOps, Hashing, NewUser};
use crate::audit::{Actor, AuditSink};
use crate::cache::TenantAuth;
use crate::mysql::PolicySettings;
use crate::policy::{
    AccountClauses, PasswordChange, PasswordCtx, PasswordProblem, PasswordRules, change_password,
};
use crate::store::{UserRow, UserSource};

/// A warning or note for `SHOW WARNINGS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// `Note` or `Warning`.
    pub level: &'static str,
    /// The code.
    pub code: u16,
    /// The message.
    pub message: String,
}

/// What a statement did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `OK`, with warnings, and the auth version the change committed at
    /// (the session waits for it before its next statement).
    Ok {
        /// Warnings.
        notes: Vec<Note>,
        /// The newest auth version written.
        auth_version: Option<u64>,
    },
    /// `SHOW GRANTS`: the column header and the lines.
    Grants {
        /// `Grants for u@h`.
        header: String,
        /// One `GRANT` per row.
        lines: Vec<String>,
    },
    /// `SET ROLE`: the session's new active roles.
    SetRole(Vec<RoleId>),
}

/// The session and the stores a statement runs against.
pub struct AccountCtx<'a> {
    /// Versioned catalog writes.
    pub catalog: &'a MariaCatalog,
    /// The snapshot the statement reads names in.
    pub snapshot: &'a TenantCatalog,
    /// The tenant.
    pub tenant: TenantId,
    /// The session's user.
    pub user: UserId,
    /// Its name.
    pub user_name: &'a str,
    /// Its account host (`CURRENT_USER()`'s).
    pub user_host: &'a str,
    /// The client's address, for 1142 messages.
    pub client_host: &'a str,
    /// The listener's persona.
    pub persona: Persona,
    /// The session's current logical database.
    pub current_db: Option<&'a str>,
    /// `lower_case_table_names` of the tenant.
    pub lower_case_table_names: bool,
    /// The session's privileges, as the `mysql_grants` engine compiled
    /// them ([`super::authorizer::SessionGrants`]); anything else holds
    /// nothing here.
    pub privileges: &'a dyn super::authorizer::SessionGrants,
    /// The session's active roles.
    pub active_roles: &'a [RoleId],
    /// The tenant's name, for audit events.
    pub tenant_name: Option<&'a str>,
    /// Resolution of the `auth.*` keys (the password rules).
    pub policy: &'a dyn PolicySettings,
    /// Where the account audit events go.
    pub audit: &'a dyn AuditSink,
    /// Password hashing for new passwords.
    pub hashing: Hashing,
    /// `auth.unsafe_privilege_action = error`: refuse server-wide
    /// privileges with 1227 instead of dropping them with 39350.
    pub unsafe_error: bool,
    /// `auth.partial_revokes` (MySQL's `partial_revokes`): a
    /// database-level `REVOKE` of what `*.*` gives stores a restriction
    /// instead of failing with 1141.
    pub partial_revokes: bool,
    /// Wall-clock time.
    pub clock: &'a dyn Clock,
}

impl std::fmt::Debug for AccountCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountCtx")
            .field("tenant", &self.tenant)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

// ---- errors, as MySQL and MariaDB word them ----

/// 1227: missing a tenant-level privilege.
#[must_use]
pub fn need_privilege(names: &str) -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        format!(
            "Access denied; you need (at least one of) the {names} privilege(s) for this operation"
        ),
    )
}

/// 1142: an action on a table, in the persona's words (MySQL names the
/// table alone, MariaDB qualifies it).
#[must_use]
pub fn table_denied(
    persona: Persona,
    command: &str,
    user: &str,
    client_host: &str,
    db: &str,
    table: &str,
) -> SprigError {
    let what = match persona {
        Persona::MySql84 => format!("'{table}'"),
        Persona::MariaDb => format!("`{db}`.`{table}`"),
    };
    SprigError::standard(
        1142,
        *b"42000",
        format!("{command} command denied to user '{user}'@'{client_host}' for table {what}"),
    )
}

/// 1370: a routine.
#[must_use]
pub fn routine_denied(
    command: &str,
    user: &str,
    client_host: &str,
    db: &str,
    routine: &str,
) -> SprigError {
    SprigError::standard(
        1370,
        *b"42000",
        format!(
            "{} command denied to user '{user}'@'{client_host}' for routine '{db}.{routine}'",
            command.to_ascii_lowercase()
        ),
    )
}

/// 1044: a database.
#[must_use]
pub fn db_denied(user: &str, host: &str, db: &str) -> SprigError {
    SprigError::standard(
        1044,
        *b"42000",
        format!("Access denied for user '{user}'@'{host}' to database '{db}'"),
    )
}

/// 1045: what MySQL answers a `GRANT ... ON *.*` the grantor can't give.
#[must_use]
pub fn global_denied(user: &str, host: &str) -> SprigError {
    SprigError::standard(
        1045,
        *b"28000",
        format!("Access denied for user '{user}'@'{host}' (using password: YES)"),
    )
}

fn cannot(op: &str, accounts: &[String]) -> SprigError {
    SprigError::standard(
        1396,
        *b"HY000",
        format!("Operation {op} failed for {}", accounts.join(",")),
    )
}

fn no_database() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

fn no_table(db: &str, t: &str) -> SprigError {
    SprigError::standard(1146, *b"42S02", format!("Table '{db}.{t}' doesn't exist"))
}

fn no_routine(kind: ObjectType, db: &str, name: &str) -> SprigError {
    let k = if kind == ObjectType::Function {
        "FUNCTION"
    } else {
        "PROCEDURE"
    };
    SprigError::standard(1305, *b"42000", format!("{k} {db}.{name} does not exist"))
}

fn illegal_grant() -> SprigError {
    SprigError::standard(
        1144,
        *b"42000",
        "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
    )
}

fn wrong_level() -> SprigError {
    SprigError::standard(
        1221,
        *b"HY000",
        "Incorrect usage of DB GRANT and GLOBAL PRIVILEGES",
    )
}

fn unknown_privilege(persona: Persona, name: &str) -> SprigError {
    match persona {
        Persona::MySql84 => SprigError::standard(
            3929,
            *b"HY000",
            format!("Dynamic privilege '{name}' is not registered with the server."),
        ),
        Persona::MariaDb => SprigError::standard(
            1064,
            *b"42000",
            format!(
                "You have an error in your SQL syntax; check the manual that corresponds to \
                 your MariaDB server version for the right syntax to use near '{name}'"
            ),
        ),
    }
}

fn no_account(persona: Persona) -> SprigError {
    match persona {
        Persona::MySql84 => SprigError::standard(
            1410,
            *b"42000",
            "You are not allowed to create a user with GRANT",
        ),
        Persona::MariaDb => SprigError::standard(
            1133,
            *b"28000",
            "Can't find any matching row in the user table",
        ),
    }
}

fn no_such_grant(acc: &AccountRef, table: Option<&str>) -> SprigError {
    match table {
        None => SprigError::standard(
            1141,
            *b"42000",
            format!(
                "There is no such grant defined for user '{}' on host '{}'",
                acc.name, acc.host
            ),
        ),
        Some(t) => SprigError::standard(
            1147,
            *b"42000",
            format!(
                "There is no such grant defined for user '{}' on host '{}' on table '{t}'",
                acc.name, acc.host
            ),
        ),
    }
}

fn role_not_granted(persona: Persona, role: &AccountRef, user: &AccountRef) -> SprigError {
    match persona {
        Persona::MySql84 => SprigError::standard(
            3530,
            *b"HY000",
            format!(
                "`{}`@`{}` is not granted to `{}`@`{}`",
                role.name, role.host, user.name, user.host
            ),
        ),
        // MariaDB 11.8's wording for a role that exists but isn't granted
        // (measured).
        Persona::MariaDb => SprigError::standard(
            1959,
            *b"OP000",
            format!(
                "User `{}`@`{}` has not been granted role `{}`",
                user.name, user.host, role.name
            ),
        ),
    }
}

fn role_cycle(role: &AccountRef, to: &AccountRef) -> SprigError {
    SprigError::standard(
        3573,
        *b"HY000",
        format!(
            "User account `{}`@`{}` is directly or indirectly granted to the role `{}`@`{}`. \
             The GRANT would create a loop in the role graph.",
            to.name, to.host, role.name, role.host
        ),
    )
}

fn dropped_note(name: &str) -> Note {
    Note {
        level: "Warning",
        code: 39350,
        message: format!(
            "ER_SPRIG_PRIVILEGE_DROPPED: {name} is a server-wide privilege and means nothing in a \
             tenant; it was dropped"
        ),
    }
}

// ---- resolution ----

/// Column `c` of a table target.
fn column_of(target: &PrivTarget, c: &str) -> Option<PrivTarget> {
    match target {
        PrivTarget::Object { db, object } => Some(PrivTarget::Column {
            db: *db,
            object: *object,
            column: c.to_owned(),
        }),
        PrivTarget::Named {
            db_key, name_key, ..
        } => Some(PrivTarget::Named {
            db_key: db_key.clone(),
            name_key: name_key.clone(),
            column: Some(c.to_owned()),
        }),
        _ => None,
    }
}

/// A planned catalog write.
#[derive(Debug)]
enum Op {
    Grant(Grantee, PrivTarget, ActionSet),
    Revoke {
        grantee: Grantee,
        target: PrivTarget,
        actions: ActionSet,
        missing: Option<SprigError>,
        /// The same grant held by name (a dropped table's, kept by
        /// `auth.grants_on_drop = keep`), revoked too: both apply to the
        /// table.
        twin: Option<PrivTarget>,
    },
    ForgetPrivileges(Grantee),
    Forget(Grantee),
    CreateRole(AccountRef),
    DropRole(RoleId),
    GrantRole(Grantee, RoleId, bool),
    RevokeRole {
        grantee: Grantee,
        role: RoleId,
        missing: Option<SprigError>,
    },
    SetDefaults(UserId, Vec<RoleId>),
}

impl AccountCtx<'_> {
    fn authz(&self) -> AuthzState {
        AuthzState::of(self.snapshot).cloned().unwrap_or_default()
    }

    fn auth(&self) -> Option<&TenantAuth> {
        TenantAuth::of(self.snapshot)
    }

    fn me(&self) -> AccountRef {
        AccountRef {
            name: self.user_name.to_owned(),
            host: self.user_host.to_owned(),
        }
    }

    fn spec(&self, u: &UserSpec) -> AccountRef {
        match u {
            UserSpec::Current => self.me(),
            UserSpec::Named(a) => a.clone(),
        }
    }

    /// The live user `'name'@'host'`.
    fn user_id(&self, a: &AccountRef) -> Option<UserId> {
        self.auth()?
            .accounts(&a.name)
            .iter()
            .find(|u| u.row.host.eq_ignore_ascii_case(&a.host))
            .map(|u| u.row.user_id)
    }

    fn grantee(&self, state: &AuthzState, a: &AccountRef) -> Option<Grantee> {
        self.user_id(a)
            .map(Grantee::User)
            .or_else(|| state.role_named(&a.name, &a.host).map(Grantee::Role))
    }

    /// The session's compiled privileges; nothing for another engine's.
    fn p(&self) -> &CompiledPrivileges {
        static NONE: std::sync::LazyLock<CompiledPrivileges> =
            std::sync::LazyLock::new(CompiledPrivileges::default);
        self.privileges
            .as_any()
            .downcast_ref::<CompiledPrivileges>()
            .unwrap_or(&NONE)
    }

    fn admin(&self) -> bool {
        self.p().everything || self.p().tenant_admin()
    }

    fn has_tenant(&self, a: Action) -> bool {
        self.admin() || self.p().tenant.has(a)
    }

    /// `SET PASSWORD FOR` another account: MySQL 8.4 and MariaDB 11.8
    /// both ask for `UPDATE` on the `mysql` schema (`check_change_password`),
    /// which a tenant-wide `UPDATE` (`GRANT UPDATE ON *.*`, `ALL`) gives
    /// here; refused like theirs, 1044 on database `mysql`.
    fn require_set_password(&self) -> Result<()> {
        if self.has_tenant(Action::Update) {
            Ok(())
        } else {
            Err(db_denied(self.user_name, self.user_host, "mysql"))
        }
    }

    fn require_create_user(&self) -> Result<()> {
        if self.has_tenant(Action::CreateUser) {
            Ok(())
        } else {
            Err(need_privilege("CREATE USER"))
        }
    }

    fn db_key(&self, db: &str) -> String {
        database_key(db, self.lower_case_table_names)
    }

    fn current(&self) -> Result<String> {
        self.current_db.map(str::to_owned).ok_or_else(no_database)
    }

    /// Refuses grants on system schemas (1044) and on names reserved for
    /// physical schemas (1049): neither is a database of the tenant.
    fn grantable_db(&self, db: &str) -> Result<()> {
        if !sprig_common::names::is_reserved_database(db) {
            return Ok(());
        }
        let system = ["information_schema", "performance_schema", "mysql", "sys"]
            .iter()
            .any(|s| s.eq_ignore_ascii_case(db));
        Err(if system {
            db_denied(self.user_name, self.user_host, db)
        } else {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
        })
    }

    /// The target of `on`, with its level and object type. For `REVOKE`
    /// (`held_by_name`), a table that doesn't exist resolves to its grants
    /// held by name, if any (MySQL revokes `tables_priv` rows of missing
    /// tables too).
    #[allow(clippy::too_many_lines)] // one arm per level
    fn target(
        &self,
        on: &GrantOn,
        held_by_name: Option<&AuthzState>,
    ) -> Result<(PrivTarget, ObjectType, String, String)> {
        match on {
            GrantOn::Tenant => Ok((
                PrivTarget::Tenant,
                ObjectType::Table,
                String::new(),
                String::new(),
            )),
            GrantOn::Database(db) => {
                let db = match db {
                    Some(d) => d.clone(),
                    None => self.current()?,
                };
                self.grantable_db(&db)?;
                Ok((
                    PrivTarget::Database(self.db_key(&db)),
                    ObjectType::Table,
                    db,
                    String::new(),
                ))
            }
            GrantOn::Object { db, name, kind } => {
                let db = match db {
                    Some(d) => d.clone(),
                    None => self.current()?,
                };
                self.grantable_db(&db)?;
                let missing = || match kind {
                    ObjectType::Table => no_table(&db, name),
                    k => no_routine(*k, &db, name),
                };
                let (ns, okind) = match kind {
                    ObjectType::Table => (Namespace::Relation, ObjectKind::Table),
                    ObjectType::Function => (Namespace::Function, ObjectKind::Function),
                    ObjectType::Procedure => (Namespace::Procedure, ObjectKind::Procedure),
                };
                let key = object_key(okind, name, self.lower_case_table_names);
                let named = || {
                    let db_key = self.db_key(&db);
                    let t = PrivTarget::Named {
                        db_key: db_key.clone(),
                        name_key: key.clone(),
                        column: None,
                    };
                    let held = held_by_name.is_some_and(|st| {
                        st.rows().privileges.iter().any(|p| {
                            matches!(&p.target, PrivTarget::Named { db_key: d, name_key: n, .. }
                                if *d == db_key && *n == key)
                        })
                    });
                    (*kind == ObjectType::Table && held)
                        .then(|| (t, *kind, db.clone(), name.clone()))
                };
                let Some(row) = self
                    .snapshot
                    .database(&self.db_key(&db))
                    .filter(|d| d.state == DbState::Active)
                else {
                    return named().ok_or_else(missing);
                };
                let Some(obj) = self
                    .snapshot
                    .object(row.db_id, ns, &key)
                    .filter(|o| o.state == ObjectState::Active)
                else {
                    return named().ok_or_else(missing);
                };
                Ok((
                    PrivTarget::Object {
                        db: row.db_id,
                        object: obj.object_id,
                    },
                    *kind,
                    db,
                    name.clone(),
                ))
            }
            GrantOn::GraphElement {
                db,
                graph,
                rels,
                name,
            } => {
                let db = match db {
                    Some(d) => d.clone(),
                    None => self.current()?,
                };
                self.grantable_db(&db)?;
                let missing = || {
                    SprigError::new(
                        sprig_common::SprigCode::GraphNotFound,
                        format!("Graph '{db}.{graph}' doesn't exist"),
                    )
                };
                let row = self
                    .snapshot
                    .database(&self.db_key(&db))
                    .filter(|d| d.state == DbState::Active)
                    .ok_or_else(missing)?;
                let key = object_key(ObjectKind::Graph, graph, self.lower_case_table_names);
                let obj = self
                    .snapshot
                    .object(row.db_id, Namespace::Graph, &key)
                    .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Graph)
                    .ok_or_else(missing)?;
                Ok((
                    PrivTarget::GraphElement {
                        db: row.db_id,
                        graph: obj.object_id,
                        rels: *rels,
                        name: name.clone(),
                    },
                    ObjectType::Table,
                    db,
                    graph.clone(),
                ))
            }
        }
    }

    /// The actions a privilege list names at `level`, column lists apart.
    /// Dropped privileges become notes.
    fn actions(
        &self,
        items: &[PrivItem],
        level: Level,
        kind: ObjectType,
        notes: &mut Vec<Note>,
    ) -> Result<(ActionSet, BTreeMap<String, ActionSet>)> {
        let mut set = ActionSet::EMPTY;
        let mut cols: BTreeMap<String, ActionSet> = BTreeMap::new();
        if level == Level::GraphElement {
            // Neo4j's graph privileges: READ (and MATCH, TRAVERSE) read
            // the label or type; SELECT and ALL are accepted as their SQL
            // names. CREATE, DELETE and SET
            // PROPERTY (all properties, `{*}`) write elements of the label
            // or type; MERGE is MATCH, CREATE and SET PROPERTY; WRITE all
            // three writes; INSERT and UPDATE are accepted as their SQL
            // names.
            for it in items {
                if !it.columns.is_empty() {
                    return Err(illegal_grant());
                }
                match it.name.as_str() {
                    "READ" | "MATCH" | "TRAVERSE" | "SELECT" | "ALL" | "ALL PRIVILEGES" => {
                        set = set.with(Action::Select);
                    }
                    "CREATE" | "INSERT" => set = set.with(Action::Insert),
                    "DELETE" => set = set.with(Action::Delete),
                    "SET PROPERTY" | "SET PROPERTY {*}" | "UPDATE" => {
                        set = set.with(Action::Update);
                    }
                    "MERGE" | "MERGE {*}" => {
                        set = set
                            .with(Action::Select)
                            .with(Action::Insert)
                            .with(Action::Update);
                    }
                    "WRITE" => {
                        set = set
                            .with(Action::Insert)
                            .with(Action::Update)
                            .with(Action::Delete);
                    }
                    "USAGE" => {}
                    _ => return Err(illegal_grant()),
                }
            }
            return Ok((set, cols));
        }
        for it in items {
            let actions = match parse_privilege(&it.name) {
                PrivName::All => all_at(level, kind),
                PrivName::Usage => ActionSet::EMPTY,
                PrivName::Action(a) => ActionSet::of(&[a]),
                PrivName::Dropped if self.unsafe_error => {
                    return Err(need_privilege(&it.name));
                }
                PrivName::Dropped => {
                    notes.push(dropped_note(&it.name));
                    continue;
                }
                PrivName::Unknown => return Err(unknown_privilege(self.persona, &it.name)),
            };
            if it.columns.is_empty() {
                if !grantable_at(level, kind).contains(actions) {
                    return Err(if level == Level::Object {
                        illegal_grant()
                    } else {
                        wrong_level()
                    });
                }
                set = set.union(actions);
            } else {
                if level != Level::Object
                    || kind != ObjectType::Table
                    || !grantable_at(Level::Column, kind).contains(actions)
                {
                    return Err(illegal_grant());
                }
                for c in &it.columns {
                    let e = cols.entry(c.to_lowercase()).or_default();
                    *e = e.union(actions);
                }
            }
        }
        Ok((set, cols))
    }

    /// The grant held by name that also applies to the live table or view
    /// `target` names (see [`Op::Revoke`]).
    fn twin(&self, target: &PrivTarget) -> Option<PrivTarget> {
        let (object, column) = match target {
            PrivTarget::Object { object, .. } => (*object, None),
            PrivTarget::Column { object, column, .. } => (*object, Some(column.clone())),
            _ => return None,
        };
        let o = self.snapshot.objects.get(object)?;
        if o.namespace != Namespace::Relation {
            return None;
        }
        let d = self.snapshot.databases.get(o.db_id)?;
        Some(PrivTarget::Named {
            db_key: d.name_key.clone(),
            name_key: o.name_key.clone(),
            column,
        })
    }

    /// Whether the session may grant or revoke `actions` on `target`.
    fn may_grant(
        &self,
        target: &PrivTarget,
        actions: ActionSet,
        db: &str,
        table: &str,
    ) -> Result<()> {
        if self.admin() {
            return Ok(());
        }
        let p = self.p();
        let need = actions.with(Action::GrantOption);
        let held = match target {
            PrivTarget::Tenant => p.tenant,
            PrivTarget::Database(k) | PrivTarget::Restriction(k) => {
                p.actions_on(Target::Database { db_key: k })
            }
            PrivTarget::Named {
                db_key, name_key, ..
            } => p.actions_on(Target::Named { db_key, name_key }),
            // A graph's element grants need the graph-wide grant (or one
            // above) with GRANT OPTION.
            PrivTarget::GraphElement { db: d, graph, .. } => p.actions_on(Target::Object {
                db_key: &self.db_key(db),
                db: *d,
                object: *graph,
            }),
            PrivTarget::Object { db: d, object } | PrivTarget::Column { db: d, object, .. } => p
                .actions_on(Target::Object {
                    db_key: &self.db_key(db),
                    db: *d,
                    object: *object,
                })
                .union(match target {
                    PrivTarget::Column { .. } => p.col_any.get(object).copied().unwrap_or_default(),
                    _ => ActionSet::EMPTY,
                }),
        };
        if held.contains(need) {
            return Ok(());
        }
        Err(match target {
            PrivTarget::Tenant => global_denied(self.user_name, self.user_host),
            PrivTarget::Database(_) | PrivTarget::Restriction(_) => {
                db_denied(self.user_name, self.user_host, db)
            }
            _ => table_denied(
                self.persona,
                "GRANT",
                self.user_name,
                self.client_host,
                db,
                table,
            ),
        })
    }

    fn may_admin_role(&self, state: &AuthzState, role: RoleId) -> bool {
        if self.has_tenant(Action::RoleAdmin) {
            return true;
        }
        state
            .edges_of(Grantee::User(self.user))
            .iter()
            .any(|e| e.role == role && e.admin_option)
    }
}

// ---- execution ----

/// Runs `stmt` for the session in `cx`.
///
/// # Errors
/// The MySQL error the statement fails with (see the module docs), the
/// catalog's (39070, 39071, 39072), or 39970 for a change while the audit
/// spool is full.
#[allow(clippy::too_many_lines)] // one arm per statement
pub async fn execute(cx: &AccountCtx<'_>, stmt: &AccountStmt) -> Result<Outcome> {
    // Account changes fail closed while the audit spool is
    // full (39970); reads and SET ROLE don't.
    if !matches!(
        stmt,
        AccountStmt::FlushPrivileges
            | AccountStmt::SetRole(_)
            | AccountStmt::ShowGrants { .. }
            | AccountStmt::ShowCreateUser { .. }
    ) {
        sprig_common::audit::check_available()?;
    }
    let state = cx.authz();
    let mut notes = Vec::new();
    let mut version = None;
    match stmt {
        AccountStmt::FlushPrivileges => {}
        AccountStmt::CreateUser {
            if_not_exists,
            users,
            options,
        } => {
            cx.require_create_user()?;
            version = create_users(cx, &state, *if_not_exists, users, options, &mut notes).await?;
        }
        AccountStmt::AlterUser {
            if_exists,
            users,
            options,
        } => {
            version = alter_users(cx, &state, *if_exists, users, options, &mut notes).await?;
        }
        AccountStmt::DropUser { if_exists, users } => {
            cx.require_create_user()?;
            version = drop_users(cx, &state, *if_exists, users, &mut notes).await?;
        }
        AccountStmt::CreateRole {
            if_not_exists,
            roles,
        } => {
            if !(cx.has_tenant(Action::CreateRole) || cx.has_tenant(Action::CreateUser)) {
                return Err(need_privilege("CREATE USER or CREATE ROLE"));
            }
            let mut ops = Vec::new();
            let mut failed = Vec::new();
            for r in roles {
                if cx.grantee(&state, r).is_some() {
                    if *if_not_exists {
                        notes.push(Note {
                            level: "Note",
                            code: 3163,
                            message: format!("Authorization ID {r} already exists."),
                        });
                    } else {
                        failed.push(r.to_string());
                    }
                    continue;
                }
                ops.push(Op::CreateRole(r.clone()));
            }
            if !failed.is_empty() {
                return Err(cannot("CREATE ROLE", &failed));
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::DropRole { if_exists, roles } => {
            if !(cx.has_tenant(Action::DropRole) || cx.has_tenant(Action::CreateUser)) {
                return Err(need_privilege("CREATE USER or DROP ROLE"));
            }
            let mut ops = Vec::new();
            let mut failed = Vec::new();
            for r in roles {
                match state.role_named(&r.name, &r.host) {
                    Some(id) => {
                        ops.push(Op::Forget(Grantee::Role(id)));
                        ops.push(Op::DropRole(id));
                    }
                    None if *if_exists => notes.push(Note {
                        level: "Note",
                        code: 3162,
                        message: format!("Authorization ID {r} does not exist."),
                    }),
                    None => failed.push(r.to_string()),
                }
            }
            if !failed.is_empty() {
                return Err(cannot("DROP ROLE", &failed));
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::Grant {
            privileges,
            on,
            to,
            grant_option,
        } => {
            let (target, kind, db, table) = cx.target(on, None)?;
            let level = target.level();
            let (mut actions, cols) = cx.actions(privileges, level, kind, &mut notes)?;
            if *grant_option {
                actions = actions.with(Action::GrantOption);
            }
            let all_cols = cols.values().fold(ActionSet::EMPTY, |a, b| a.union(*b));
            cx.may_grant(
                &target,
                actions
                    .union(all_cols)
                    .minus(ActionSet::of(&[Action::GrantOption])),
                &db,
                &table,
            )?;
            let mut ops = Vec::new();
            for g in to {
                let acc = cx.spec(g);
                let grantee = cx
                    .grantee(&state, &acc)
                    .ok_or_else(|| no_account(cx.persona))?;
                // A partial revoke on this database is lifted rather than
                // granted over (MySQL 8.4, measured).
                let mut actions = actions;
                if let PrivTarget::Database(k) = &target {
                    let restricted = held(&state, grantee, &PrivTarget::Restriction(k.clone()));
                    let lift = actions.intersect(restricted);
                    if !lift.is_empty() {
                        ops.push(Op::Revoke {
                            grantee,
                            twin: None,
                            target: PrivTarget::Restriction(k.clone()),
                            actions: lift,
                            missing: None,
                        });
                        actions = actions.minus(lift);
                        if actions.is_empty() && cols.is_empty() {
                            continue;
                        }
                    }
                }
                if !actions.is_empty() || cols.is_empty() {
                    ops.push(Op::Grant(grantee, target.clone(), actions));
                }
                for (c, a) in &cols {
                    if let Some(t) = column_of(&target, c) {
                        ops.push(Op::Grant(grantee, t, *a));
                    }
                }
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::Revoke {
            privileges,
            on,
            from,
        } => {
            let (target, kind, db, table) = cx.target(on, Some(&state))?;
            let level = target.level();
            let (actions, cols) = cx.actions(privileges, level, kind, &mut notes)?;
            let all_cols = cols.values().fold(ActionSet::EMPTY, |a, b| a.union(*b));
            cx.may_grant(
                &target,
                actions
                    .union(all_cols)
                    .minus(ActionSet::of(&[Action::GrantOption])),
                &db,
                &table,
            )?;
            let mut ops = Vec::new();
            for g in from {
                let acc = cx.spec(g);
                let grantee = cx
                    .grantee(&state, &acc)
                    .ok_or_else(|| no_account(cx.persona))?;
                let missing = || match &target {
                    PrivTarget::Tenant => None,
                    PrivTarget::Database(_) | PrivTarget::Restriction(_) => {
                        Some(no_such_grant(&acc, None))
                    }
                    _ => Some(no_such_grant(&acc, Some(&table))),
                };
                let mut actions = actions;
                match &target {
                    // `*.*` also clears those actions from the grantee's
                    // partial revokes (MySQL 8.4, measured).
                    PrivTarget::Tenant => {
                        for p in state.privileges_of(grantee) {
                            if let PrivTarget::Restriction(_) = &p.target
                                && !p.actions.intersect(actions).is_empty()
                            {
                                ops.push(Op::Revoke {
                                    grantee,
                                    twin: None,
                                    target: p.target.clone(),
                                    actions,
                                    missing: None,
                                });
                            }
                        }
                    }
                    // MySQL refuses to revoke what the level doesn't hold
                    // (1141, 1147), and with partial revokes on turns
                    // what `*.*` gives into a restriction; MariaDB only
                    // needs the row to exist.
                    PrivTarget::Database(k) if cx.persona == Persona::MySql84 => {
                        let have = held(&state, grantee, &target);
                        let unheld = actions.minus(have);
                        if !unheld.is_empty() {
                            let global = held(&state, grantee, &PrivTarget::Tenant);
                            if cx.partial_revokes && global.contains(unheld) {
                                ops.push(Op::Grant(
                                    grantee,
                                    PrivTarget::Restriction(k.clone()),
                                    unheld,
                                ));
                                actions = actions.intersect(have);
                            } else {
                                return Err(no_such_grant(&acc, None));
                            }
                        }
                    }
                    PrivTarget::Object { .. } | PrivTarget::Named { .. }
                        if cx.persona == Persona::MySql84 =>
                    {
                        let mut have = held(&state, grantee, &target);
                        if let Some(t) = cx.twin(&target) {
                            have = have.union(held(&state, grantee, &t));
                        }
                        if !actions.minus(have).is_empty() {
                            return Err(no_such_grant(&acc, Some(&table)));
                        }
                    }
                    _ => {}
                }
                if !actions.is_empty() {
                    ops.push(Op::Revoke {
                        grantee,
                        twin: cx.twin(&target),
                        target: target.clone(),
                        actions,
                        missing: missing(),
                    });
                }
                for (c, a) in &cols {
                    let Some(t) = column_of(&target, c) else {
                        continue;
                    };
                    ops.push(Op::Revoke {
                        grantee,
                        twin: cx.twin(&t),
                        target: t,
                        actions: *a,
                        missing: missing(),
                    });
                }
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::RevokeAll { from } => {
            cx.require_create_user()?;
            let mut ops = Vec::new();
            for g in from {
                let acc = cx.spec(g);
                let grantee = cx
                    .grantee(&state, &acc)
                    .ok_or_else(|| no_account(cx.persona))?;
                ops.push(Op::ForgetPrivileges(grantee));
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::GrantRoles {
            roles,
            to,
            admin_option,
        } => {
            let mut ops = Vec::new();
            for r in roles {
                let Some(role) = state.role_named(&r.name, &r.host) else {
                    if cx.user_id(r).is_some() {
                        return Err(SprigError::standard(
                            39355,
                            *b"HY000",
                            format!(
                                "ER_SPRIG_ACCOUNT_AS_ROLE: {r} is a user; only roles can be \
                                 granted to accounts"
                            ),
                        ));
                    }
                    return Err(cannot("GRANT", &[r.to_string()]));
                };
                if !cx.may_admin_role(&state, role) {
                    return Err(need_privilege("WITH ADMIN, ROLE_ADMIN"));
                }
                for g in to {
                    let acc = cx.spec(g);
                    let grantee = cx
                        .grantee(&state, &acc)
                        .ok_or_else(|| cannot("GRANT", &[acc.to_string()]))?;
                    if let Grantee::Role(to_role) = grantee
                        && state.would_cycle(role, to_role)
                    {
                        return Err(role_cycle(r, &acc));
                    }
                    ops.push(Op::GrantRole(grantee, role, *admin_option));
                }
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::RevokeRoles { roles, from } => {
            let mut ops = Vec::new();
            for r in roles {
                let role = state
                    .role_named(&r.name, &r.host)
                    .ok_or_else(|| cannot("REVOKE", &[r.to_string()]))?;
                if !cx.may_admin_role(&state, role) {
                    return Err(need_privilege("WITH ADMIN, ROLE_ADMIN"));
                }
                for g in from {
                    let acc = cx.spec(g);
                    let grantee = cx
                        .grantee(&state, &acc)
                        .ok_or_else(|| cannot("REVOKE", &[acc.to_string()]))?;
                    ops.push(Op::RevokeRole {
                        grantee,
                        role,
                        missing: Some(role_not_granted(cx.persona, r, &acc)),
                    });
                }
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::SetDefaultRole { roles, users } => {
            let users: Vec<AccountRef> = if users.is_empty() {
                vec![cx.me()]
            } else {
                users.iter().map(|u| cx.spec(u)).collect()
            };
            let mut ops = Vec::new();
            for acc in &users {
                let uid = cx
                    .user_id(acc)
                    .ok_or_else(|| cannot("SET DEFAULT ROLE", &[acc.to_string()]))?;
                if uid != cx.user {
                    cx.require_create_user()?;
                }
                let granted = state.granted_roles(uid);
                let chosen = choose(cx, &state, roles, &granted, acc)?;
                ops.push(Op::SetDefaults(uid, chosen));
            }
            version = apply(cx, ops).await?;
        }
        AccountStmt::SetRole(choice) => {
            let granted = state.granted_roles(cx.user);
            let chosen = match choice {
                RoleChoice::Default => state.default_roles(cx.user),
                c => choose(cx, &state, c, &granted, &cx.me())?,
            };
            return Ok(Outcome::SetRole(chosen));
        }
        AccountStmt::ShowGrants { account, using } => {
            return show_grants(cx, &state, account.as_ref(), using);
        }
        AccountStmt::ShowCreateUser { account } => return show_create_user(cx, account),
    }
    // Every grant and role change is an audit event (users' own events
    // come from `AccountOps` above). Only changes
    // that committed are recorded.
    if version.is_some()
        && let Some(event) = grant_event(stmt)
    {
        let mut e = crate::audit::AuditEvent::new(
            event,
            match cx.persona {
                Persona::MySql84 => "mysql",
                Persona::MariaDb => "mariadb",
            },
            cx.clock.wall(),
        );
        e.tenant = Some(cx.tenant);
        e.tenant_name = cx.tenant_name.map(str::to_owned);
        e.actor = Some(cx.actor());
        let mut detail = format!("{stmt:?}");
        if detail.len() > 4000 {
            let mut cut = 4000;
            while !detail.is_char_boundary(cut) {
                cut -= 1;
            }
            detail.truncate(cut);
        }
        e.detail = Some(detail);
        cx.audit.emit(&e);
    }
    Ok(Outcome::Ok {
        notes,
        auth_version: version,
    })
}

/// The roles a `SET ROLE` or `SET DEFAULT ROLE` choice names, each granted.
fn choose(
    cx: &AccountCtx<'_>,
    state: &AuthzState,
    c: &RoleChoice,
    granted: &[RoleId],
    user: &AccountRef,
) -> Result<Vec<RoleId>> {
    let named = |list: &[AccountRef]| -> Result<Vec<RoleId>> {
        list.iter()
            .map(|r| {
                state
                    .role_named(&r.name, &r.host)
                    .filter(|id| granted.contains(id))
                    .ok_or_else(|| role_not_granted(cx.persona, r, user))
            })
            .collect()
    };
    Ok(match c {
        RoleChoice::None => Vec::new(),
        RoleChoice::Default => state.default_roles(cx.user_id(user).unwrap_or(cx.user)),
        RoleChoice::All => granted.to_vec(),
        RoleChoice::AllExcept(list) => {
            let except = named(list)?;
            granted
                .iter()
                .filter(|r| !except.contains(r))
                .copied()
                .collect()
        }
        RoleChoice::List(list) => named(list)?,
    })
}

/// `ATTRIBUTE` or `COMMENT` of `options` as the JSON object to merge.
fn attribute_patch(options: &UserOptions) -> Result<Option<serde_json::Value>> {
    options
        .attribute
        .as_deref()
        .map(|t| {
            serde_json::from_str::<serde_json::Value>(t)
                .ok()
                .filter(serde_json::Value::is_object)
                .ok_or_else(|| {
                    SprigError::standard(
                        3934,
                        *b"HY000",
                        "The user attribute must be a valid JSON object",
                    )
                })
        })
        .transpose()
}

/// The audit event of a grant or role statement; `None` for the others
/// (users are audited by `AccountOps`).
fn grant_event(stmt: &AccountStmt) -> Option<&'static str> {
    use crate::audit::events;
    Some(match stmt {
        AccountStmt::Grant { .. } => events::PRIVILEGES_GRANTED,
        AccountStmt::Revoke { .. } | AccountStmt::RevokeAll { .. } => events::PRIVILEGES_REVOKED,
        AccountStmt::CreateRole { .. } => events::ROLE_CREATED,
        AccountStmt::DropRole { .. } => events::ROLE_DROPPED,
        AccountStmt::GrantRoles { .. } => events::ROLES_GRANTED,
        AccountStmt::RevokeRoles { .. } => events::ROLES_REVOKED,
        AccountStmt::SetDefaultRole { .. } => events::DEFAULT_ROLES_SET,
        _ => return None,
    })
}

impl AccountCtx<'_> {
    /// The account operations, as this session's user (audited).
    fn ops(&self) -> AccountOps<'_> {
        AccountOps {
            cat: self.catalog,
            tenant: self.tenant,
            tenant_name: self.tenant_name.map(str::to_owned),
            actor: self.actor(),
            audit: self.audit,
            clock: self.clock,
            protocol: match self.persona {
                Persona::MySql84 => "mysql",
                Persona::MariaDb => "mariadb",
            },
        }
    }

    fn actor(&self) -> Actor {
        Actor {
            tenant: self.tenant,
            tenant_name: self.tenant_name.map(str::to_owned),
            user: self.user_name.to_owned(),
            user_id: Some(self.user),
        }
    }
}

async fn create_users(
    cx: &AccountCtx<'_>,
    state: &AuthzState,
    if_not_exists: bool,
    users: &[UserItem],
    options: &UserOptions,
    notes: &mut Vec<Note>,
) -> Result<Option<u64>> {
    options.clauses.validate(cx.policy, cx.snapshot)?;
    let mut failed = Vec::new();
    let mut todo = Vec::new();
    for u in users {
        let acc = cx.spec(&u.user);
        if cx.grantee(state, &acc).is_some() {
            if if_not_exists {
                notes.push(Note {
                    level: "Note",
                    code: 3163,
                    message: format!("Authorization ID {acc} already exists."),
                });
            } else {
                failed.push(acc.to_string());
            }
            continue;
        }
        todo.push((acc, u));
    }
    if !failed.is_empty() {
        return Err(cannot("CREATE USER", &failed));
    }
    let mut defaults = Vec::new();
    for r in &options.default_roles {
        defaults.push(
            state
                .role_named(&r.name, &r.host)
                .ok_or_else(|| cannot("CREATE USER", &[r.to_string()]))?,
        );
    }
    let ops = cx.ops();
    let mut version = None;
    for (acc, u) in todo {
        let password = u
            .identified
            .as_ref()
            .map(|i| i.password.to_vec())
            .unwrap_or_default();
        // The strength rules (`auth.password_policy`).
        let probe = UserRow::new(
            UserId(0),
            acc.name.clone(),
            acc.host.clone(),
            UserSource::Sql,
            cx.clock.wall(),
            0,
        );
        if !PasswordRules::of(&probe, cx.policy, cx.snapshot)
            .strength
            .accepts(&password, &acc.name)
        {
            return Err(PasswordProblem::Weak.error(cx.persona, &acc.name, &acc.host));
        }
        let mut new = NewUser::new(acc.name.clone(), &password);
        new.host.clone_from(&acc.host);
        new.source = UserSource::Sql;
        new.hashing = cx.hashing;
        new.account_locked = options.lock.unwrap_or(false);
        new.password_expired = options.expire.unwrap_or(false);
        if let Some(r) = options.require {
            new.require_tls = r;
            new.require_dn.clone_from(&options.require_dn);
            if new.require_dn.0.is_some() || new.require_dn.1.is_some() {
                admin::migrate_dn(cx.catalog, cx.tenant).await?;
            }
        }
        let changed = ops.create_user(&new).await.map_err(|e| match e.kind {
            sprig_common::ErrorKind::Backend { errno: 1396, .. }
            | sprig_common::ErrorKind::Standard { errno: 1396, .. } => {
                cannot("CREATE USER", &[acc.to_string()])
            }
            _ => e,
        })?;
        version = Some(changed.auth_version);
        if options.clauses != AccountClauses::default() {
            version = Some(
                ops.set_clauses(changed.user_id, &acc.name, &acc.host, options.clauses)
                    .await?
                    .auth_version,
            );
        }
        if let Some(a) = attribute_patch(options)? {
            version = Some(
                admin::merge_attributes(cx.catalog, cx.tenant, changed.user_id, a)
                    .await?
                    .auth_version,
            );
        }
        if !defaults.is_empty() {
            let mut list = Vec::new();
            for r in &defaults {
                list.push(Op::GrantRole(Grantee::User(changed.user_id), *r, false));
            }
            list.push(Op::SetDefaults(changed.user_id, defaults.clone()));
            version = apply(cx, list).await?.or(version);
        }
    }
    Ok(version)
}

#[allow(clippy::too_many_lines)] // each clause in turn, as MySQL applies them
async fn alter_users(
    cx: &AccountCtx<'_>,
    state: &AuthzState,
    if_exists: bool,
    users: &[UserItem],
    options: &UserOptions,
    notes: &mut Vec<Note>,
) -> Result<Option<u64>> {
    options.clauses.validate(cx.policy, cx.snapshot)?;
    let mut version = None;
    let mut targets = Vec::new();
    let mut failed = Vec::new();
    for u in users {
        let acc = cx.spec(&u.user);
        match cx.user_id(&acc) {
            Some(id) => {
                let own_password_only = id == cx.user
                    && options.default_roles.is_empty()
                    && options.lock.is_none()
                    && options.require.is_none()
                    && options.attribute.is_none()
                    && options.clauses == AccountClauses::default();
                if !own_password_only {
                    if options.set_password {
                        cx.require_set_password()?;
                    } else {
                        cx.require_create_user()?;
                    }
                }
                targets.push((id, acc, u));
            }
            // MySQL and MariaDB: `SET PASSWORD FOR` an unknown account.
            None if options.set_password => {
                cx.require_set_password()?;
                return Err(SprigError::standard(
                    1133,
                    *b"28000",
                    "Can't find any matching row in the user table",
                ));
            }
            None if if_exists => notes.push(Note {
                level: "Note",
                code: 3162,
                message: format!("Authorization ID {acc} does not exist."),
            }),
            None => failed.push(acc.to_string()),
        }
    }
    if !failed.is_empty() {
        return Err(cannot("ALTER USER", &failed));
    }
    let ops = cx.ops();
    let keep = |c: admin::Changed| Some(c.auth_version);
    for (id, acc, u) in targets {
        if let Some(i) = &u.identified {
            // The password rules: strength, history, reuse; REQUIRE
            // CURRENT only for the user's own change.
            let pctx = PasswordCtx {
                tenant_name: cx.tenant_name,
                cat: cx.catalog,
                catalog: cx.snapshot,
                policy: cx.policy,
                audit: cx.audit,
                clock: cx.clock,
            };
            let c = change_password(
                &pctx,
                id,
                &PasswordChange {
                    password: &i.password,
                    current: None,
                    retain_current: i.retain_current,
                    discard_old: options.discard_old,
                    actor: (id != cx.user).then(|| cx.actor()),
                    persona: cx.persona,
                },
            )
            .await?;
            version = keep(c);
        } else if options.discard_old {
            version = keep(admin::discard_old_password(cx.catalog, cx.tenant, id, 0).await?);
        }
        if let Some(l) = options.lock {
            version = keep(ops.set_locked(id, &acc.name, &acc.host, l).await?);
        }
        if let Some(e) = options.expire {
            version = keep(ops.set_expired(id, &acc.name, &acc.host, e).await?);
        }
        if let Some(r) = options.require {
            version = keep(
                ops.set_require(id, &acc.name, &acc.host, r, options.require_dn.clone())
                    .await?,
            );
        }
        if options.clauses != AccountClauses::default() {
            version = keep(
                ops.set_clauses(id, &acc.name, &acc.host, options.clauses)
                    .await?,
            );
        }
        if let Some(a) = attribute_patch(options)? {
            version = keep(ops.merge_attributes(id, &acc.name, &acc.host, a).await?);
        }
        if !options.default_roles.is_empty() {
            let granted = state.granted_roles(id);
            let chosen = choose(
                cx,
                state,
                &RoleChoice::List(options.default_roles.clone()),
                &granted,
                &acc,
            )?;
            version = apply(cx, vec![Op::SetDefaults(id, chosen)])
                .await?
                .or(version);
        }
    }
    Ok(version)
}

async fn drop_users(
    cx: &AccountCtx<'_>,
    state: &AuthzState,
    if_exists: bool,
    users: &[AccountRef],
    notes: &mut Vec<Note>,
) -> Result<Option<u64>> {
    let mut failed = Vec::new();
    let mut todo = Vec::new();
    for acc in users {
        match cx.grantee(state, acc) {
            Some(g) => todo.push((g, acc)),
            None if if_exists => notes.push(Note {
                level: "Note",
                code: 3162,
                message: format!("Authorization ID {acc} does not exist."),
            }),
            None => failed.push(acc.to_string()),
        }
    }
    if !failed.is_empty() {
        return Err(cannot("DROP USER", &failed));
    }
    let ops = cx.ops();
    let mut version = None;
    for (g, acc) in todo {
        match g {
            Grantee::User(u) => {
                // The user's sessions are closed (39306) from the
                // auth change feed.
                version = Some(ops.drop_user(u, &acc.name, &acc.host).await?.auth_version);
                version = apply(cx, vec![Op::Forget(g)]).await?.or(version);
            }
            Grantee::Role(r) => {
                version = apply(cx, vec![Op::Forget(g), Op::DropRole(r)])
                    .await?
                    .or(version);
            }
        }
    }
    Ok(version)
}

/// The actions `grantee`'s own row at `target` holds in the snapshot.
fn held(state: &AuthzState, grantee: Grantee, target: &PrivTarget) -> ActionSet {
    state
        .privileges_of(grantee)
        .filter(|p| &p.target == target)
        .fold(ActionSet::EMPTY, |a, p| a.union(p.actions))
}

/// Applies `ops` in one auth change; `None` when there was nothing to do.
async fn apply(cx: &AccountCtx<'_>, ops: Vec<Op>) -> Result<Option<u64>> {
    if ops.is_empty() {
        return Ok(None);
    }
    let mut change = cx
        .catalog
        .change_with(cx.tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let result: Result<()> = async {
        let (core, q) = change.tx().parts();
        let store = AuthzStore::new(core.schema());
        let by = Some(cx.user);
        for op in ops {
            match op {
                Op::Grant(grantee, target, actions) => {
                    store.grant(q, grantee, &target, actions, by).await?;
                }
                Op::Revoke {
                    grantee,
                    target,
                    actions,
                    missing,
                    twin,
                } => {
                    let mut before = store.revoke(q, grantee, &target, actions).await?;
                    if let Some(t) = twin {
                        before = before.union(store.revoke(q, grantee, &t, actions).await?);
                    }
                    if before.is_empty()
                        && let Some(e) = missing
                    {
                        return Err(e);
                    }
                }
                Op::ForgetPrivileges(g) => store.forget_privileges(q, g).await?,
                Op::Forget(g) => store.forget(q, g).await?,
                Op::CreateRole(r) => {
                    if store.create_role(q, &r.name, &r.host, by).await?.is_none() {
                        return Err(cannot("CREATE ROLE", &[r.to_string()]));
                    }
                }
                Op::DropRole(r) => store.delete_role(q, r).await?,
                Op::GrantRole(g, r, admin) => store.grant_role(q, g, r, admin, by).await?,
                Op::RevokeRole {
                    grantee,
                    role,
                    missing,
                } => {
                    if !store.revoke_role(q, grantee, role).await?
                        && let Some(e) = missing
                    {
                        return Err(e);
                    }
                }
                Op::SetDefaults(u, roles) => store.set_default_roles(q, u, &roles).await?,
            }
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(Some(change.commit().await?)),
        Err(e) => {
            let _ = change.rollback().await;
            let _ = cx.catalog.settle(cx.tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

fn show_grants(
    cx: &AccountCtx<'_>,
    state: &AuthzState,
    account: Option<&UserSpec>,
    using: &[AccountRef],
) -> Result<Outcome> {
    let acc = account.map_or_else(|| cx.me(), |a| cx.spec(a));
    let own = acc.name == cx.user_name && acc.host.eq_ignore_ascii_case(cx.user_host);
    let grantee = cx.grantee(state, &acc);
    let granted_role =
        matches!(grantee, Some(Grantee::Role(r)) if state.closure(cx.active_roles).contains(&r));
    if !own && !granted_role && !cx.has_tenant(Action::CreateUser) {
        return Err(match cx.persona {
            Persona::MySql84 => table_denied(
                cx.persona,
                "SELECT",
                cx.user_name,
                cx.client_host,
                "mysql",
                "user",
            ),
            Persona::MariaDb => db_denied(cx.user_name, cx.user_host, "mysql"),
        });
    }
    let Some(grantee) = grantee else {
        return Err(no_such_grant(&acc, None));
    };
    let mut extra = Vec::new();
    for r in using {
        let id = state
            .role_named(&r.name, &r.host)
            .ok_or_else(|| role_not_granted(cx.persona, r, &acc))?;
        extra.push(id);
    }
    let lines = if state.is_migrated() {
        show::render(state, cx.snapshot, grantee, &acc, &extra, cx.persona)
    } else {
        vec![format!(
            "GRANT ALL PRIVILEGES ON *.* TO `{}`@`{}` WITH GRANT OPTION",
            acc.name, acc.host
        )]
    };
    Ok(Outcome::Grants {
        header: format!("Grants for {}@{}", acc.name, acc.host),
        lines,
    })
}

/// `SHOW CREATE USER`: the account as MySQL 8.4 (3306) or MariaDB (3307)
/// prints it, never with a password hash or verifier. The session's own account
/// always; another needs `CREATE USER` (MySQL also accepts `SELECT` on
/// `mysql.user`, which tenants never hold), as `SHOW GRANTS` does. An unknown
/// account fails with 1396.
fn show_create_user(cx: &AccountCtx<'_>, account: &UserSpec) -> Result<Outcome> {
    let acc = cx.spec(account);
    let own = acc.name == cx.user_name && acc.host.eq_ignore_ascii_case(cx.user_host);
    if !own && !cx.has_tenant(Action::CreateUser) {
        return Err(match cx.persona {
            Persona::MySql84 => table_denied(
                cx.persona,
                "SELECT",
                cx.user_name,
                cx.client_host,
                "mysql",
                "user",
            ),
            Persona::MariaDb => db_denied(cx.user_name, cx.user_host, "mysql"),
        });
    }
    let row = cx.auth().and_then(|a| {
        a.accounts(&acc.name)
            .iter()
            .find(|u| u.row.host.eq_ignore_ascii_case(&acc.host))
            .map(|u| u.row.clone())
    });
    let Some(row) = row else {
        return Err(cannot("SHOW CREATE USER", &[acc.to_string()]));
    };
    Ok(Outcome::Grants {
        header: format!("CREATE USER for {}@{}", acc.name, acc.host),
        lines: vec![show::create_user(&row, cx.persona)],
    })
}

/// The privileges a user created through the Admin API or `sprigctl`
/// gets: everything in its tenant, as the bootstrap admin
/// (`ALL` and `SPRIG_TENANT_ADMIN` on `*.*` `WITH GRANT OPTION`).
///
/// # Errors
/// The catalog's.
pub async fn grant_tenant_admin(
    catalog: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
) -> Result<u64> {
    let mut c = catalog
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let r = {
        let (core, q) = c.tx().parts();
        AuthzStore::new(core.schema())
            .grant(
                q,
                Grantee::User(user),
                &PrivTarget::Tenant,
                super::store::LEGACY_ACTIONS,
                None,
            )
            .await
    };
    match r {
        Ok(()) => c.commit().await,
        Err(e) => {
            let _ = c.rollback().await;
            let _ = catalog.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}
