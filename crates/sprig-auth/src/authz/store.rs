//! Chapter 29's catalog tables (build step 1): `roles`, `role_edges` and
//! `privileges`, as the `authz` migration module, with typed rows and
//! access.
//!
//! The module is separate from chapter 14's `auth` module so each chapter
//! numbers its own formats. Format 1 creates the three tables exactly as
//! the chapter's DDL defines them, then gives every live user that existed
//! before (users created through the Admin API or `sprigctl` before M0.5,
//! who could do everything in their tenant) `ALL PRIVILEGES` and
//! `SPRIG_TENANT_ADMIN` on `*.*` `WITH GRANT OPTION`, so nothing they could
//! do stops working. `row_policies` and `foreign_grants` arrive with the
//! steps that use them.
//!
//! **Rows.** Unused key parts are `0` or `''`, never `NULL`. Grants on a
//! database keep its *name* in `name_pattern` (`db_id` 0): MySQL keeps
//! database grants by name, so a grant on a database that doesn't exist
//! yet works, and one survives `DROP DATABASE` and `CREATE DATABASE`.
//! Object and column grants keep `db_id` and `object_id`: MySQL refuses
//! them on a missing table (1146), and they go with the object.
//!
//! **Versioning.** Every change commits with a new auth version
//! ([`VersionKind::Auth`](sprig_catalog::VersionKind)); the gateway's
//! [`AuthzExtension`](super::state::AuthzExtension) reloads all three
//! tables (they are small) at each version.

use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use sprig_catalog::{Migration, Module, Phase};
use sprig_common::ids::{DbId, ObjectId, RoleId, UserId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

use super::privilege::{ALL_TENANT, Action, ActionSet, Level};

/// The module name in the catalog's format records.
pub const MODULE: &str = "authz";

/// `roles` (chapter 29).
pub const DDL_ROLES: &str = "CREATE TABLE IF NOT EXISTS roles (
  role_id      INT UNSIGNED NOT NULL AUTO_INCREMENT,
  kind         ENUM('sql','mongo','builtin','combined','mediated','share') NOT NULL,
  name         VARCHAR(255) NOT NULL COLLATE utf8mb4_bin,
  host         VARCHAR(255) NOT NULL COLLATE ascii_general_ci DEFAULT '%',
  db_id        INT UNSIGNED NOT NULL DEFAULT 0,
  members_hash BIGINT UNSIGNED NULL,
  created_by   INT UNSIGNED NULL,
  created_at   DATETIME(3) NOT NULL,
  last_used_at DATETIME(3) NULL,
  PRIMARY KEY (role_id),
  UNIQUE KEY uk_role_name (kind, name, host, db_id),
  UNIQUE KEY uk_role_members (members_hash)
) ENGINE=InnoDB";

/// `role_edges` (chapter 29).
pub const DDL_ROLE_EDGES: &str = "CREATE TABLE IF NOT EXISTS role_edges (
  grantee_tenant_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  grantee_kind ENUM('user','role') NOT NULL,
  grantee_id   INT UNSIGNED NOT NULL,
  role_id      INT UNSIGNED NOT NULL,
  admin_option TINYINT(1) NOT NULL DEFAULT 0,
  is_default   TINYINT(1) NOT NULL DEFAULT 0,
  granted_by   INT UNSIGNED NULL,
  granted_by_tenant_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  granted_at   DATETIME(3) NOT NULL,
  PRIMARY KEY (grantee_tenant_id, grantee_kind, grantee_id, role_id),
  KEY k_edges_role (role_id)
) ENGINE=InnoDB";

/// `privileges` (chapter 29).
pub const DDL_PRIVILEGES: &str = "CREATE TABLE IF NOT EXISTS privileges (
  priv_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  grantee_tenant_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  grantee_kind ENUM('user','role') NOT NULL,
  grantee_id   INT UNSIGNED NOT NULL,
  level        ENUM('tenant','database','object','column','kv_prefix','channel',
                    'redis_commands','graph_element') NOT NULL,
  db_id        INT UNSIGNED NOT NULL DEFAULT 0,
  object_id    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  column_name  VARCHAR(64) NOT NULL DEFAULT '',
  kv_prefix    VARBINARY(1024) NOT NULL DEFAULT '',
  element      INT UNSIGNED NOT NULL DEFAULT 0,
  name_pattern VARCHAR(255) NOT NULL COLLATE utf8mb4_bin DEFAULT '',
  actions      BIGINT UNSIGNED NOT NULL,
  restriction  TINYINT(1) NOT NULL DEFAULT 0,
  detail       JSON NULL,
  granted_by   INT UNSIGNED NULL,
  granted_by_tenant_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  granted_at   DATETIME(3) NOT NULL,
  PRIMARY KEY (priv_id),
  UNIQUE KEY uk_priv_target (grantee_tenant_id, grantee_kind, grantee_id, level, db_id,
                    object_id, column_name, kv_prefix, element, name_pattern, restriction),
  KEY k_priv_object (db_id, object_id)
) ENGINE=InnoDB";

/// What every user that existed before this module holds: `ALL` and
/// `SPRIG_TENANT_ADMIN` on `*.*` `WITH GRANT OPTION`.
pub const LEGACY_ACTIONS: ActionSet = ALL_TENANT
    .with(Action::TenantAdmin)
    .with(Action::GrantOption);

/// The data step of format 1 (idempotent through `uk_priv_target`).
const GRANT_EXISTING: &str = "INSERT IGNORE INTO privileges
  (grantee_tenant_id, grantee_kind, grantee_id, level, actions, granted_at)
  SELECT 0, 'user', user_id, 'tenant', 4026531839, UTC_TIMESTAMP(3)
  FROM users WHERE state = 'active'";

/// The module's migrations.
pub static AUTHZ_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "roles, role_edges and privileges; existing users keep everything",
    statements: &[DDL_ROLES, DDL_ROLE_EDGES, DDL_PRIVILEGES, GRANT_EXISTING],
}];

/// The `authz` module, for [`Migrator::new`](sprig_catalog::Migrator::new).
/// It needs chapter 14's `auth` module first (format 1 reads `users`).
pub const AUTHZ_MODULE: Module = Module {
    name: MODULE,
    migrations: AUTHZ_MIGRATIONS,
};

/// Who holds a privilege or a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Grantee {
    /// A user of this tenant.
    User(UserId),
    /// A role of this tenant.
    Role(RoleId),
}

impl Grantee {
    fn kind(self) -> &'static str {
        match self {
            Grantee::User(_) => "user",
            Grantee::Role(_) => "role",
        }
    }

    fn id(self) -> u32 {
        match self {
            Grantee::User(u) => u.0,
            Grantee::Role(r) => r.0,
        }
    }

    fn from_parts(kind: &str, id: u32) -> Result<Self> {
        match kind {
            "user" => Ok(Grantee::User(UserId(id))),
            "role" => Ok(Grantee::Role(RoleId(id))),
            _ => Err(SprigError::internal(format!(
                "authz catalog: bad grantee_kind {kind:?}"
            ))),
        }
    }
}

/// Where a privilege row points (the R0 levels).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrivTarget {
    /// `*.*`.
    Tenant,
    /// `db.*`, by the database's name key.
    Database(String),
    /// `db.obj`.
    Object {
        /// The database.
        db: DbId,
        /// The table, view or routine.
        object: ObjectId,
    },
    /// `db.t (c)`.
    Column {
        /// The database.
        db: DbId,
        /// The table or view.
        object: ObjectId,
        /// The column name, lower case (MySQL compares column names
        /// case-insensitively).
        column: String,
    },
}

impl PrivTarget {
    /// The level.
    #[must_use]
    pub fn level(&self) -> Level {
        match self {
            PrivTarget::Tenant => Level::Tenant,
            PrivTarget::Database(_) => Level::Database,
            PrivTarget::Object { .. } => Level::Object,
            PrivTarget::Column { .. } => Level::Column,
        }
    }

    /// `(db_id, object_id, column_name, name_pattern)`.
    fn parts(&self) -> (u32, u64, &str, &str) {
        match self {
            PrivTarget::Tenant => (0, 0, "", ""),
            PrivTarget::Database(n) => (0, 0, "", n.as_str()),
            PrivTarget::Object { db, object } => (db.0, object.0, "", ""),
            PrivTarget::Column { db, object, column } => (db.0, object.0, column.as_str(), ""),
        }
    }
}

/// A `privileges` row (the R0 levels, this tenant's grantees).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivRow {
    /// The id.
    pub priv_id: u64,
    /// Who holds it.
    pub grantee: Grantee,
    /// Where.
    pub target: PrivTarget,
    /// What.
    pub actions: ActionSet,
}

/// A `roles` row of kind `sql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRow {
    /// The id; the backend role would be `t<tenant>_r<role_id>`.
    pub role_id: RoleId,
    /// The role name.
    pub name: String,
    /// The host part (MySQL roles are accounts).
    pub host: String,
}

/// A `role_edges` row: `grantee` holds `role`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeRow {
    /// The user or role holding the role.
    pub grantee: Grantee,
    /// The role held.
    pub role: RoleId,
    /// `WITH ADMIN OPTION`.
    pub admin_option: bool,
    /// One of the grantee's default roles.
    pub is_default: bool,
}

/// Everything the module stores for one tenant (this tenant's grantees,
/// the R0 levels and SQL roles).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthzRows {
    /// SQL roles.
    pub roles: Vec<RoleRow>,
    /// Role grants.
    pub edges: Vec<EdgeRow>,
    /// Privileges.
    pub privileges: Vec<PrivRow>,
}

/// The module's tables in one catalog schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzStore {
    roles: String,
    edges: String,
    privileges: String,
    schema: String,
}

fn map_err(e: mysql_async::Error) -> SprigError {
    crate::store::map_err(e)
}

fn get<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| SprigError::internal(format!("authz catalog: bad column {i}")))
}

impl AuthzStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            roles: format!("{q}.roles"),
            edges: format!("{q}.role_edges"),
            privileges: format!("{q}.privileges"),
            schema,
        }
    }

    /// Whether the module's tables exist (the module is migrated).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'privileges'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Every row the gateway uses: SQL roles, this tenant's role edges and
    /// privileges at the R0 levels. Rows of later releases are skipped.
    ///
    /// # Errors
    /// 39070, a backend error, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<AuthzRows> {
        let roles: Vec<Row> = q
            .query(format!(
                "SELECT role_id, name, host FROM {} WHERE kind = 'sql' ORDER BY role_id",
                self.roles
            ))
            .await
            .map_err(map_err)?;
        let roles = roles
            .iter()
            .map(|r| {
                Ok(RoleRow {
                    role_id: RoleId(get(r, 0)?),
                    name: get(r, 1)?,
                    host: get(r, 2)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let edges: Vec<Row> = q
            .query(format!(
                "SELECT grantee_kind, grantee_id, role_id, admin_option, is_default FROM {} \
                 WHERE grantee_tenant_id = 0 ORDER BY grantee_kind, grantee_id, role_id",
                self.edges
            ))
            .await
            .map_err(map_err)?;
        let edges = edges
            .iter()
            .map(|r| {
                let kind: String = get(r, 0)?;
                Ok(EdgeRow {
                    grantee: Grantee::from_parts(&kind, get(r, 1)?)?,
                    role: RoleId(get(r, 2)?),
                    admin_option: get::<u8>(r, 3)? != 0,
                    is_default: get::<u8>(r, 4)? != 0,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let privs: Vec<Row> = q
            .query(format!(
                "SELECT priv_id, grantee_kind, grantee_id, level, db_id, object_id, column_name, \
                 name_pattern, actions FROM {} \
                 WHERE grantee_tenant_id = 0 AND restriction = 0 \
                 AND level IN ('tenant','database','object','column') ORDER BY priv_id",
                self.privileges
            ))
            .await
            .map_err(map_err)?;
        let mut privileges = Vec::with_capacity(privs.len());
        for r in &privs {
            let kind: String = get(r, 1)?;
            let level: String = get(r, 3)?;
            let db = DbId(get(r, 4)?);
            let object = ObjectId(get(r, 5)?);
            let target = match Level::parse(&level) {
                Some(Level::Tenant) => PrivTarget::Tenant,
                Some(Level::Database) => PrivTarget::Database(get(r, 7)?),
                Some(Level::Object) => PrivTarget::Object { db, object },
                Some(Level::Column) => PrivTarget::Column {
                    db,
                    object,
                    column: get(r, 6)?,
                },
                None => continue,
            };
            privileges.push(PrivRow {
                priv_id: get(r, 0)?,
                grantee: Grantee::from_parts(&kind, get(r, 2)?)?,
                target,
                actions: ActionSet(get(r, 8)?),
            });
        }
        Ok(AuthzRows {
            roles,
            edges,
            privileges,
        })
    }

    /// Adds `actions` to `grantee`'s row at `target` (creating it).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn grant<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        grantee: Grantee,
        target: &PrivTarget,
        actions: ActionSet,
        granted_by: Option<UserId>,
    ) -> Result<()> {
        let (db, object, column, pattern) = target.parts();
        q.exec_drop(
            format!(
                "INSERT INTO {} (grantee_tenant_id, grantee_kind, grantee_id, level, db_id, \
                 object_id, column_name, name_pattern, actions, granted_by, granted_at) \
                 VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE actions = actions | VALUES(actions), \
                 granted_by = VALUES(granted_by), granted_at = VALUES(granted_at)",
                self.privileges
            ),
            vec![
                Value::from(grantee.kind()),
                Value::from(grantee.id()),
                Value::from(target.level().as_str()),
                Value::from(db),
                Value::from(object),
                Value::from(column),
                Value::from(pattern),
                Value::from(actions.0),
                Value::from(granted_by.map(|u| u.0)),
            ],
        )
        .await
        .map_err(map_err)
    }

    /// Removes `actions` from `grantee`'s row at `target`, deleting the
    /// row when nothing is left. Returns the actions the row held before
    /// (empty when there was no row).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn revoke<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        grantee: Grantee,
        target: &PrivTarget,
        actions: ActionSet,
    ) -> Result<ActionSet> {
        let (db, object, column, pattern) = target.parts();
        let key = "grantee_tenant_id = 0 AND grantee_kind = ? AND grantee_id = ? AND level = ? \
                   AND db_id = ? AND object_id = ? AND column_name = ? AND kv_prefix = '' \
                   AND element = 0 AND name_pattern = ? AND restriction = 0";
        let params = || {
            vec![
                Value::from(grantee.kind()),
                Value::from(grantee.id()),
                Value::from(target.level().as_str()),
                Value::from(db),
                Value::from(object),
                Value::from(column),
                Value::from(pattern),
            ]
        };
        let before: Option<u64> = q
            .exec_first(
                format!(
                    "SELECT actions FROM {} WHERE {key} FOR UPDATE",
                    self.privileges
                ),
                params(),
            )
            .await
            .map_err(map_err)?;
        let Some(before) = before else {
            return Ok(ActionSet::EMPTY);
        };
        let left = ActionSet(before).minus(actions);
        if left.is_empty() {
            q.exec_drop(
                format!("DELETE FROM {} WHERE {key}", self.privileges),
                params(),
            )
            .await
            .map_err(map_err)?;
        } else {
            let mut p = vec![Value::from(left.0)];
            p.extend(params());
            q.exec_drop(
                format!("UPDATE {} SET actions = ? WHERE {key}", self.privileges),
                p,
            )
            .await
            .map_err(map_err)?;
        }
        Ok(ActionSet(before))
    }

    /// Deletes every privilege and role edge `grantee` holds, and, for a
    /// role, every edge granting it.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn forget<Q: Queryable + Send>(&self, q: &mut Q, grantee: Grantee) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE grantee_tenant_id = 0 AND grantee_kind = ? \
                 AND grantee_id = ?",
                self.privileges
            ),
            (grantee.kind(), grantee.id()),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE grantee_tenant_id = 0 AND grantee_kind = ? \
                 AND grantee_id = ?",
                self.edges
            ),
            (grantee.kind(), grantee.id()),
        )
        .await
        .map_err(map_err)?;
        if let Grantee::Role(r) = grantee {
            q.exec_drop(
                format!("DELETE FROM {} WHERE role_id = ?", self.edges),
                (r.0,),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }

    /// Deletes every privilege row of `grantee` (`REVOKE ALL PRIVILEGES,
    /// GRANT OPTION`); its roles stay.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn forget_privileges<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        grantee: Grantee,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE grantee_tenant_id = 0 AND grantee_kind = ? \
                 AND grantee_id = ?",
                self.privileges
            ),
            (grantee.kind(), grantee.id()),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes every privilege on `object` (it was dropped).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn forget_object<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: DbId,
        object: ObjectId,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE db_id = ? AND object_id = ?",
                self.privileges
            ),
            (db.0, object.0),
        )
        .await
        .map_err(map_err)
    }

    /// Creates the SQL role `'name'@'host'` and returns its id; `None` if
    /// it exists.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn create_role<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        name: &str,
        host: &str,
        created_by: Option<UserId>,
    ) -> Result<Option<RoleId>> {
        let r = q
            .exec_drop(
                format!(
                    "INSERT INTO {} (kind, name, host, created_by, created_at) \
                     VALUES ('sql', ?, ?, ?, UTC_TIMESTAMP(3))",
                    self.roles
                ),
                (name, host, created_by.map(|u| u.0)),
            )
            .await;
        match r {
            Ok(()) => {}
            Err(mysql_async::Error::Server(s)) if s.code == 1062 => return Ok(None),
            Err(e) => return Err(map_err(e)),
        }
        let id: Option<u32> = q
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .map_err(map_err)?;
        id.map(RoleId)
            .map(Some)
            .ok_or_else(|| SprigError::internal("no LAST_INSERT_ID for a new role"))
    }

    /// The SQL role `'name'@'host'`, read in the transaction.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn role<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        name: &str,
        host: &str,
    ) -> Result<Option<RoleId>> {
        let id: Option<u32> = q
            .exec_first(
                format!(
                    "SELECT role_id FROM {} WHERE kind = 'sql' AND name = ? AND host = ?",
                    self.roles
                ),
                (name, host),
            )
            .await
            .map_err(map_err)?;
        Ok(id.map(RoleId))
    }

    /// Deletes the role row (its grants with [`Self::forget`]).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete_role<Q: Queryable + Send>(&self, q: &mut Q, role: RoleId) -> Result<()> {
        q.exec_drop(
            format!("DELETE FROM {} WHERE role_id = ?", self.roles),
            (role.0,),
        )
        .await
        .map_err(map_err)
    }

    /// `GRANT role TO grantee [WITH ADMIN OPTION]`. An existing edge keeps
    /// its default flag and gains the admin option if asked.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn grant_role<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        grantee: Grantee,
        role: RoleId,
        admin_option: bool,
        granted_by: Option<UserId>,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (grantee_tenant_id, grantee_kind, grantee_id, role_id, \
                 admin_option, granted_by, granted_at) VALUES (0, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE admin_option = admin_option OR VALUES(admin_option)",
                self.edges
            ),
            (
                grantee.kind(),
                grantee.id(),
                role.0,
                u8::from(admin_option),
                granted_by.map(|u| u.0),
            ),
        )
        .await
        .map_err(map_err)
    }

    /// `REVOKE role FROM grantee`; whether there was an edge.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn revoke_role<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        grantee: Grantee,
        role: RoleId,
    ) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE grantee_tenant_id = 0 AND grantee_kind = ? \
                     AND grantee_id = ? AND role_id = ?",
                    self.edges
                ),
                (grantee.kind(), grantee.id(), role.0),
            )
            .await
            .map_err(map_err)?;
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE grantee_tenant_id = 0 AND grantee_kind = ? \
                 AND grantee_id = ? AND role_id = ?",
                self.edges
            ),
            (grantee.kind(), grantee.id(), role.0),
        )
        .await
        .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// `SET DEFAULT ROLE`: exactly `roles` (each already granted) become
    /// `user`'s defaults.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_default_roles<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: UserId,
        roles: &[RoleId],
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET is_default = 0 WHERE grantee_tenant_id = 0 \
                 AND grantee_kind = 'user' AND grantee_id = ?",
                self.edges
            ),
            (user.0,),
        )
        .await
        .map_err(map_err)?;
        for r in roles {
            q.exec_drop(
                format!(
                    "UPDATE {} SET is_default = 1 WHERE grantee_tenant_id = 0 \
                     AND grantee_kind = 'user' AND grantee_id = ? AND role_id = ?",
                    self.edges
                ),
                (user.0, r.0),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_bits_match_the_migration() {
        assert!(GRANT_EXISTING.contains(&LEGACY_ACTIONS.0.to_string()));
        assert!(LEGACY_ACTIONS.has(Action::TenantAdmin));
        assert!(LEGACY_ACTIONS.has(Action::GrantOption));
        assert!(LEGACY_ACTIONS.has(Action::Select));
        assert!(!LEGACY_ACTIONS.has(Action::BypassRls));
    }
}
