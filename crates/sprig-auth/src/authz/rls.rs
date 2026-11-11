//! Row-level security's catalog and per-session view (chapter 29,
//! "Row-level security"; build steps 21 and 22).
//!
//! - **Tables** (module `rls`, in `t<tenant>_sprig`): `row_policies` as
//!   the chapter defines it, and `row_security`, one row per object whose
//!   security was ever switched: `enabled`, `forced`, and the auth version
//!   whose MariaDB write checks (triggers) are deployed (`checks_version`,
//!   against `changed_version`, which every policy change of the object
//!   moves). Both change with the auth version, so a policy reaches every
//!   gateway like a `REVOKE`.
//! - **[`RlsExtension`]** loads [`RlsState`] into every snapshot.
//! - **[`compile`]**: per session (user, active roles), the protected
//!   tables that bind it and the policies that apply to it:
//!
//! ```text
//! table T binds the session when
//!   T has security enabled
//!   and the session lacks BYPASS RLS on T (tenant, database or object)
//!   and (T is FORCEd or the session lacks ALTER on T)   -- managers, as
//!                                                          PostgreSQL's owner
//! policy p applies when it is PUBLIC or names the user or an active role
//! USING(cmd)  = (OR of permissive p.using) AND (AND of restrictive p.using)
//!               over applicable p with cmd, USING given; no permissive: FALSE
//! CHECK(cmd)  = the same over p.check, else p.using     (INSERT, UPDATE)
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use sprig_catalog::{
    CatalogExtension, ExtState, Migration, Module, Phase, SnapshotTx, TenantCatalog, VersionKind,
};
use sprig_common::ids::{DbId, ObjectId, RoleId, UserId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

use super::compile::{CompiledPrivileges, Target};
use super::privilege::Action;
use super::store::Grantee;

/// The module name in the catalog's format records.
pub const MODULE: &str = "rls";

/// `row_policies` (chapter 29).
pub const DDL_ROW_POLICIES: &str = "CREATE TABLE IF NOT EXISTS row_policies (
  policy_id    INT UNSIGNED NOT NULL AUTO_INCREMENT,
  object_id    BIGINT UNSIGNED NOT NULL,
  name         VARCHAR(64) NOT NULL COLLATE utf8mb4_bin,
  mode         ENUM('permissive','restrictive') NOT NULL,
  commands     SET('select','insert','update','delete') NOT NULL,
  grantees     JSON NOT NULL,
  element      INT UNSIGNED NOT NULL DEFAULT 0,
  using_expr   TEXT NULL,
  check_expr   TEXT NULL,
  using_mongo  JSON NULL,
  check_mongo  JSON NULL,
  created_by   INT UNSIGNED NOT NULL,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (policy_id),
  UNIQUE KEY uk_policy (object_id, name)
) ENGINE=InnoDB";

/// `row_security`: the switches and the deployed write checks per object.
pub const DDL_ROW_SECURITY: &str = "CREATE TABLE IF NOT EXISTS row_security (
  object_id       BIGINT UNSIGNED NOT NULL,
  db_id           INT UNSIGNED NOT NULL,
  enabled         TINYINT(1) NOT NULL DEFAULT 0,
  forced          TINYINT(1) NOT NULL DEFAULT 0,
  checks_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (object_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static RLS_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "row_policies and row_security",
    statements: &[DDL_ROW_POLICIES, DDL_ROW_SECURITY],
}];

/// The `rls` module.
pub const RLS_MODULE: Module = Module {
    name: MODULE,
    migrations: RLS_MIGRATIONS,
};

/// Command bits (as `sprig_sql::rls::stmt::Commands`).
pub mod cmd {
    /// `SELECT`.
    pub const SELECT: u8 = 1;
    /// `INSERT`.
    pub const INSERT: u8 = 2;
    /// `UPDATE`.
    pub const UPDATE: u8 = 4;
    /// `DELETE`.
    pub const DELETE: u8 = 8;
}

/// One `row_policies` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRow {
    /// The id.
    pub policy_id: u32,
    /// The table.
    pub object_id: ObjectId,
    /// The name.
    pub name: String,
    /// `AS RESTRICTIVE`.
    pub restrictive: bool,
    /// Command bits ([`cmd`]).
    pub commands: u8,
    /// The grantees; empty is `PUBLIC`.
    pub grantees: Vec<Grantee>,
    /// `USING`, logical names.
    pub using_expr: Option<String>,
    /// `WITH CHECK`.
    pub check_expr: Option<String>,
    /// Who created it.
    pub created_by: UserId,
    /// The auth version of its last change.
    pub changed_version: u64,
}

impl PolicyRow {
    fn applies_to(&self, user: UserId, roles: &[RoleId]) -> bool {
        self.grantees.is_empty()
            || self.grantees.iter().any(|g| match g {
                Grantee::User(u) => *u == user,
                Grantee::Role(r) => roles.contains(r),
            })
    }
}

/// One `row_security` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityRow {
    /// The table.
    pub object_id: ObjectId,
    /// Its database.
    pub db_id: DbId,
    /// `ENABLE ROW LEVEL SECURITY`.
    pub enabled: bool,
    /// `FORCE ROW LEVEL SECURITY`.
    pub forced: bool,
    /// The `changed_version` whose write checks are deployed.
    pub checks_version: u64,
    /// The auth version of the object's last policy or switch change.
    pub changed_version: u64,
}

impl SecurityRow {
    /// Whether MariaDB's write checks match the current policies.
    #[must_use]
    pub fn checks_current(&self) -> bool {
        self.checks_version >= self.changed_version
    }
}

/// The module's rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RlsRows {
    /// Every policy.
    pub policies: Vec<PolicyRow>,
    /// Every switch row.
    pub security: Vec<SecurityRow>,
}

/// One tenant's policies at one auth version, indexed.
#[derive(Debug, Clone, Default)]
pub struct RlsState {
    migrated: bool,
    rows: Arc<RlsRows>,
    by_object: HashMap<ObjectId, Vec<usize>>,
    security: HashMap<ObjectId, SecurityRow>,
}

impl ExtState for RlsState {
    fn bytes(&self) -> usize {
        128 + self.rows.policies.len() * 512 + self.rows.security.len() * 48
    }
}

impl RlsState {
    /// The state of `rows`.
    #[must_use]
    pub fn new(rows: RlsRows) -> Self {
        let mut by_object: HashMap<ObjectId, Vec<usize>> = HashMap::new();
        for (i, p) in rows.policies.iter().enumerate() {
            by_object.entry(p.object_id).or_default().push(i);
        }
        let security = rows.security.iter().map(|s| (s.object_id, *s)).collect();
        Self {
            migrated: true,
            rows: Arc::new(rows),
            by_object,
            security,
        }
    }

    /// The tenant's state in `catalog`, if the extension is registered.
    #[must_use]
    pub fn of(catalog: &TenantCatalog) -> Option<&RlsState> {
        catalog.ext.get::<RlsState>(MODULE)
    }

    /// Whether the module's tables exist.
    #[must_use]
    pub fn is_migrated(&self) -> bool {
        self.migrated
    }

    /// All rows.
    #[must_use]
    pub fn rows(&self) -> &RlsRows {
        &self.rows
    }

    /// The policies of `object`.
    pub fn policies_of(&self, object: ObjectId) -> impl Iterator<Item = &PolicyRow> {
        self.by_object
            .get(&object)
            .into_iter()
            .flatten()
            .map(|i| &self.rows.policies[*i])
    }

    /// The switches of `object`.
    #[must_use]
    pub fn security(&self, object: ObjectId) -> Option<&SecurityRow> {
        self.security.get(&object)
    }

    /// Whether any table has security enabled.
    #[must_use]
    pub fn any_enabled(&self) -> bool {
        self.security.values().any(|s| s.enabled)
    }
}

/// A protected table as one session sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRls {
    /// The table.
    pub object: ObjectId,
    /// Its database.
    pub db: DbId,
    /// The policies that apply to the session.
    pub policies: Vec<PolicyRow>,
    /// Whether MariaDB's write checks are deployed for the current
    /// policies.
    pub checks_current: bool,
    /// Whether they (and the policy view) were ever deployed: the view
    /// exists, perhaps a policy change behind (the rewrite still applies
    /// the current policies).
    pub checks_deployed: bool,
}

/// Which expression of a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clause {
    /// `USING`: rows a command sees.
    Using,
    /// `WITH CHECK` (else `USING`): new row versions.
    Check,
}

/// One policy's expression in a combination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Term<'a> {
    /// The policy.
    pub policy: &'a PolicyRow,
    /// Its expression text.
    pub expr: &'a str,
}

/// A combination: OR of `permissive`, AND of `restrictive`; no permissive
/// term denies everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Combination<'a> {
    /// OR-ed.
    pub permissive: Vec<Term<'a>>,
    /// AND-ed.
    pub restrictive: Vec<Term<'a>>,
}

impl TableRls {
    /// The terms for command bit `c` ([`cmd`]) and clause `k`.
    #[must_use]
    pub fn combination(&self, c: u8, k: Clause) -> Combination<'_> {
        let mut out = Combination::default();
        for p in &self.policies {
            if p.commands & c == 0 {
                continue;
            }
            let e = match k {
                Clause::Using => p.using_expr.as_deref(),
                Clause::Check => p.check_expr.as_deref().or(p.using_expr.as_deref()),
            };
            let Some(expr) = e else { continue };
            let t = Term { policy: p, expr };
            if p.restrictive {
                out.restrictive.push(t);
            } else {
                out.permissive.push(t);
            }
        }
        out
    }
}

/// A session's row-level security at one snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionRls {
    /// The tables that bind the session.
    pub tables: HashMap<ObjectId, Arc<TableRls>>,
    /// Tables with security on that don't bind the session (bypass,
    /// managers of unforced tables).
    pub exempt: Vec<ObjectId>,
    /// `BYPASS RLS` at tenant level.
    pub bypass: bool,
    /// Ids of every policy that applies to the session, sorted.
    pub policy_ids: Vec<u32>,
    /// The auth version compiled at.
    pub auth_version: u64,
}

impl SessionRls {
    /// Whether no table binds the session.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// Compiles `user`'s row-level security from `c` (its privileges at the
/// same snapshot) and the tenant's [`RlsState`].
#[must_use]
pub fn compile(cat: &TenantCatalog, c: &CompiledPrivileges, user: UserId) -> SessionRls {
    let Some(state) = RlsState::of(cat) else {
        return SessionRls::default();
    };
    let mut out = SessionRls {
        auth_version: cat.auth_version,
        bypass: !c.everything && c.tenant.has(Action::BypassRls),
        ..SessionRls::default()
    };
    if c.everything || !state.any_enabled() {
        return out;
    }
    let mut ids = Vec::new();
    for s in state.security.values().filter(|s| s.enabled) {
        let Some(obj) = cat.objects.get(s.object_id) else {
            continue;
        };
        let Some(db) = cat.databases.get(obj.db_id) else {
            continue;
        };
        let target = Target::Object {
            db_key: &db.name_key,
            db: obj.db_id,
            object: s.object_id,
        };
        let manager = c.allows(Action::Alter, target);
        if c.allows(Action::BypassRls, target) || (manager && !s.forced) {
            out.exempt.push(s.object_id);
            continue;
        }
        let policies: Vec<PolicyRow> = state
            .policies_of(s.object_id)
            .filter(|p| p.applies_to(user, &c.roles))
            .cloned()
            .collect();
        ids.extend(policies.iter().map(|p| p.policy_id));
        out.tables.insert(
            s.object_id,
            Arc::new(TableRls {
                object: s.object_id,
                db: obj.db_id,
                policies,
                checks_current: s.checks_current(),
                checks_deployed: s.checks_version > 0,
            }),
        );
    }
    ids.sort_unstable();
    ids.dedup();
    out.policy_ids = ids;
    out.exempt.sort_unstable();
    out
}

/// The `rls` module's tables in catalog schema `schema`.
#[derive(Debug, Clone)]
pub struct RlsStore {
    schema: String,
    policies: String,
    security: String,
}

fn map_err(e: mysql_async::Error) -> SprigError {
    crate::store::map_err(e)
}

fn get<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| SprigError::internal(format!("rls catalog: bad column {i}")))
}

fn commands_of(set: &str) -> u8 {
    set.split(',').fold(0, |a, p| {
        a | match p.trim() {
            "select" => cmd::SELECT,
            "insert" => cmd::INSERT,
            "update" => cmd::UPDATE,
            "delete" => cmd::DELETE,
            _ => 0,
        }
    })
}

/// The catalog's `SET` text for command bits.
#[must_use]
pub fn commands_text(bits: u8) -> String {
    let mut v = Vec::new();
    for (b, n) in [
        (cmd::SELECT, "select"),
        (cmd::INSERT, "insert"),
        (cmd::UPDATE, "update"),
        (cmd::DELETE, "delete"),
    ] {
        if bits & b != 0 {
            v.push(n);
        }
    }
    v.join(",")
}

/// `grantees` as stored: `[{"user": 7}, {"role": 3}]`.
#[must_use]
pub fn grantees_json(g: &[Grantee]) -> String {
    let items: Vec<String> = g
        .iter()
        .map(|x| match x {
            Grantee::User(u) => format!("{{\"user\":{}}}", u.0),
            Grantee::Role(r) => format!("{{\"role\":{}}}", r.0),
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn grantees_of(json: &str) -> Result<Vec<Grantee>> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| SprigError::internal(format!("rls catalog: grantees: {e}")))?;
    let mut out = Vec::new();
    for item in v.as_array().into_iter().flatten() {
        let id = |k: &str| {
            item.get(k)
                .and_then(serde_json::Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
        };
        if let Some(u) = id("user") {
            out.push(Grantee::User(UserId(u)));
        } else if let Some(r) = id("role") {
            out.push(Grantee::Role(RoleId(r)));
        }
    }
    Ok(out)
}

/// Runs a statement; the rows it changed.
async fn affected<Q: Queryable + Send>(
    q: &mut Q,
    sql: String,
    params: mysql_async::Params,
) -> Result<u64> {
    let r = q.exec_iter(sql, params).await.map_err(map_err)?;
    let n = r.affected_rows();
    r.drop_result().await.map_err(map_err)?;
    Ok(n)
}

/// A policy to write.
#[derive(Debug, Clone)]
pub struct NewPolicy<'a> {
    /// The table.
    pub object: ObjectId,
    /// Its database.
    pub db: DbId,
    /// The name.
    pub name: &'a str,
    /// `AS RESTRICTIVE`.
    pub restrictive: bool,
    /// Command bits.
    pub commands: u8,
    /// Grantees; empty is `PUBLIC`.
    pub grantees: &'a [Grantee],
    /// `USING`.
    pub using_expr: Option<&'a str>,
    /// `WITH CHECK`.
    pub check_expr: Option<&'a str>,
    /// Who creates it.
    pub by: UserId,
}

impl RlsStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            policies: format!("{q}.row_policies"),
            security: format!("{q}.row_security"),
            schema,
        }
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'row_security'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Creates the module's tables (idempotent; DDL, so outside any
    /// change).
    ///
    /// # Errors
    /// A backend error.
    pub async fn migrate(&self, conn: &mut mysql_async::Conn) -> Result<()> {
        sprig_catalog::Migrator::new(&[RLS_MODULE])?
            .run(
                conn,
                &self.schema,
                &sprig_catalog::migrate::MigrateOpts::default(),
            )
            .await?;
        Ok(())
    }

    /// Every row.
    ///
    /// # Errors
    /// 39070, a backend error, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<RlsRows> {
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT policy_id, object_id, name, mode, commands, CAST(grantees AS CHAR), \
                 using_expr, check_expr, created_by, changed_version FROM {} \
                 ORDER BY object_id, name",
                self.policies
            ))
            .await
            .map_err(map_err)?;
        let mut policies = Vec::with_capacity(rows.len());
        for r in &rows {
            let mode: String = get(r, 3)?;
            let cmds: String = get(r, 4)?;
            let grantees: String = get(r, 5)?;
            policies.push(PolicyRow {
                policy_id: get(r, 0)?,
                object_id: ObjectId(get(r, 1)?),
                name: get(r, 2)?,
                restrictive: mode == "restrictive",
                commands: commands_of(&cmds),
                grantees: grantees_of(&grantees)?,
                using_expr: r
                    .get_opt::<Option<String>, usize>(6)
                    .and_then(Result::ok)
                    .flatten(),
                check_expr: r
                    .get_opt::<Option<String>, usize>(7)
                    .and_then(Result::ok)
                    .flatten(),
                created_by: UserId(get(r, 8)?),
                changed_version: get(r, 9)?,
            });
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT object_id, db_id, enabled, forced, checks_version, changed_version \
                 FROM {}",
                self.security
            ))
            .await
            .map_err(map_err)?;
        let mut security = Vec::with_capacity(rows.len());
        for r in &rows {
            security.push(SecurityRow {
                object_id: ObjectId(get(r, 0)?),
                db_id: DbId(get(r, 1)?),
                enabled: get::<u8>(r, 2)? != 0,
                forced: get::<u8>(r, 3)? != 0,
                checks_version: get(r, 4)?,
                changed_version: get(r, 5)?,
            });
        }
        Ok(RlsRows { policies, security })
    }

    /// Records a change of `object`'s policies or switches at `version`
    /// (the change's auth version): its write checks are stale until
    /// [`Self::checks_deployed`].
    ///
    /// # Errors
    /// A backend error.
    pub async fn touch<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        db: DbId,
        version: u64,
        switch: Option<(Option<bool>, Option<bool>)>,
    ) -> Result<()> {
        let (enabled, forced) = switch.unwrap_or((None, None));
        q.exec_drop(
            format!(
                "INSERT INTO {} (object_id, db_id, enabled, forced, checks_version, \
                 changed_version) VALUES (?, ?, ?, ?, 0, ?) \
                 ON DUPLICATE KEY UPDATE changed_version = VALUES(changed_version), \
                 enabled = COALESCE(?, enabled), forced = COALESCE(?, forced)",
                self.security
            ),
            (
                object.0,
                db.0,
                enabled.unwrap_or(false),
                forced.unwrap_or(false),
                version,
                enabled.map(Value::from),
                forced.map(Value::from),
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Records that `object`'s write checks match `version`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn checks_deployed<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        version: u64,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET checks_version = GREATEST(checks_version, ?) WHERE object_id = ?",
                self.security
            ),
            (version, object.0),
        )
        .await
        .map_err(map_err)
    }

    /// Adds a policy; `false` if the table has one of that name.
    ///
    /// # Errors
    /// A backend error.
    pub async fn insert<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        p: &NewPolicy<'_>,
        version: u64,
    ) -> Result<bool> {
        let r = q
            .exec_drop(
                format!(
                    "INSERT INTO {} (object_id, name, mode, commands, grantees, using_expr, \
                     check_expr, created_by, changed_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    self.policies
                ),
                (
                    p.object.0,
                    p.name,
                    if p.restrictive {
                        "restrictive"
                    } else {
                        "permissive"
                    },
                    commands_text(p.commands),
                    grantees_json(p.grantees),
                    p.using_expr,
                    p.check_expr,
                    p.by.0,
                    version,
                ),
            )
            .await;
        match r {
            Ok(()) => Ok(true),
            Err(mysql_async::Error::Server(e)) if e.code == 1062 => Ok(false),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Changes a policy's grantees and expressions (`None` keeps one);
    /// `false` if there is no such policy.
    ///
    /// # Errors
    /// A backend error.
    #[allow(clippy::too_many_arguments)]
    pub async fn alter<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
        grantees: Option<&[Grantee]>,
        using_expr: Option<&str>,
        check_expr: Option<&str>,
        version: u64,
    ) -> Result<bool> {
        affected(
            q,
            format!(
                "UPDATE {} SET grantees = COALESCE(?, grantees), \
                 using_expr = COALESCE(?, using_expr), check_expr = COALESCE(?, check_expr), \
                 changed_version = ? WHERE object_id = ? AND name = ?",
                self.policies
            ),
            (
                grantees.map(grantees_json),
                using_expr,
                check_expr,
                version,
                object.0,
                name,
            )
                .into(),
        )
        .await
        .map(|n| n > 0)
    }

    /// Renames a policy; `Ok(None)` if it doesn't exist, `Ok(Some(false))`
    /// if the new name is taken.
    ///
    /// # Errors
    /// A backend error.
    pub async fn rename<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
        to: &str,
        version: u64,
    ) -> Result<Option<bool>> {
        let r = affected(
            q,
            format!(
                "UPDATE {} SET name = ?, changed_version = ? WHERE object_id = ? AND name = ?",
                self.policies
            ),
            (to, version, object.0, name).into(),
        )
        .await;
        match r {
            Ok(n) if n > 0 => Ok(Some(true)),
            Ok(_) => Ok(None),
            Err(e) if e.protocol().mysql_errno == 1062 => Ok(Some(false)),
            Err(e) => Err(e),
        }
    }

    /// Drops a policy; `false` if it doesn't exist.
    ///
    /// # Errors
    /// A backend error.
    pub async fn delete<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
    ) -> Result<bool> {
        affected(
            q,
            format!(
                "DELETE FROM {} WHERE object_id = ? AND name = ?",
                self.policies
            ),
            (object.0, name).into(),
        )
        .await
        .map(|n| n > 0)
    }

    /// Forgets everything of dropped objects (PostgreSQL drops a table's
    /// policies with it).
    ///
    /// # Errors
    /// A backend error.
    pub async fn forget_objects<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        objects: &[ObjectId],
    ) -> Result<()> {
        for o in objects {
            q.exec_drop(
                format!("DELETE FROM {} WHERE object_id = ?", self.policies),
                (o.0,),
            )
            .await
            .map_err(map_err)?;
            q.exec_drop(
                format!("DELETE FROM {} WHERE object_id = ?", self.security),
                (o.0,),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }
}

/// The function policy views read the session's context through
/// (`sprig_sql::rls::views::CTX_FUNCTION`).
pub const CTX_FUNCTION: &str = "__sprig_rls_ctx";

/// The policy view of table `table` (object `object`;
/// `sprig_sql::rls::views::view_name`): an updatable view of the rows the
/// session's `SELECT` policies show.
#[must_use]
pub fn policy_view(table: &str, object: ObjectId) -> String {
    if table.chars().count() <= 52 {
        format!("__sprig_rls_{table}")
    } else {
        let head: String = table.chars().take(40).collect();
        format!("__sprig_rls_{head}~{}", object.0)
    }
}

fn relation_of(cat: &TenantCatalog, db_key: &str, name_key: &str) -> Option<(DbId, ObjectId)> {
    use sprig_catalog::store::{DbState, Namespace, ObjectState};
    let d = cat
        .database(db_key)
        .filter(|d| d.state == DbState::Active)?;
    let o = cat
        .object(d.db_id, Namespace::Relation, name_key)
        .filter(|o| o.state == ObjectState::Active)?;
    Some((d.db_id, o.object_id))
}

/// The protected tables that bind each backend account, as MariaDB sees
/// it: an account holds the rights of every role granted to its user,
/// active or not (chapter 29, "Backend accounts"), so a table binds the
/// account only when it binds the user with every granted role active.
/// Accounts not bound by any table are left out.
#[must_use]
pub fn backend_bound(
    cat: &TenantCatalog,
    state: &super::AuthzState,
    auth: &crate::cache::TenantAuth,
) -> HashMap<String, Vec<ObjectId>> {
    let mut out = HashMap::new();
    if !RlsState::of(cat).is_some_and(RlsState::any_enabled) || !state.is_migrated() {
        return out;
    }
    let resolve = |d: &str, n: &str| relation_of(cat, d, n);
    for u in auth.users() {
        let uid = u.row.user_id;
        let roles = state.granted_roles(uid);
        let c = CompiledPrivileges::compile_at(state, &resolve, uid, &roles);
        let r = compile(cat, &c, uid);
        if r.tables.is_empty() {
            continue;
        }
        let mut v: Vec<ObjectId> = r.tables.keys().copied().collect();
        v.sort_unstable();
        out.insert(super::backend::account(cat.tenant, uid), v);
    }
    out
}

/// Narrows the wanted backend grants `want` for row-level security's
/// policy views (`rls.backend_enforcement = views`; decision RL18).
/// `physical` lists the relations (schema, name) of the tenant's
/// databases with protected tables on the cluster being reconciled,
/// `bound` is [`backend_bound`], and `definers` the (account, lower-case
/// view name) pairs of policy views' definers, which keep reading their
/// table itself (a view reads it with its definer's rights; RL19).
///
/// ```text
/// for each account a, database D, schema s of D holding table T's view:
///   T binds a:   no SELECT on T (the database row loses it; every other
///                relation of s gets SELECT one by one), REFERENCES on T
///                (metadata), and T's SELECT, INSERT, UPDATE, DELETE on the view
///   otherwise:   T's actions on the view too, where the database row
///                doesn't give them (views and routines of the tenant read
///                T through it, with their definer's rights)
/// ```
#[allow(clippy::implicit_hasher, clippy::too_many_lines)] // one pass per account
pub fn narrow(
    want: &mut super::backend::BackendState,
    cat: &TenantCatalog,
    bound: &HashMap<String, Vec<ObjectId>>,
    physical: &[(String, String)],
    definers: &std::collections::HashSet<(String, String)>,
) {
    use super::backend::{BackendTarget, db_pattern, db_row_for};
    use super::privilege::ActionSet;
    let Some(state) = RlsState::of(cat) else {
        return;
    };
    let siud = ActionSet::of(&[
        Action::Select,
        Action::Insert,
        Action::Update,
        Action::Delete,
    ]);
    let select = ActionSet::of(&[Action::Select]);
    let tenant = cat.tenant;
    // Protected tables with a view, per database: (object, name lower).
    let mut protected: HashMap<DbId, Vec<(ObjectId, String)>> = HashMap::new();
    let mut names: HashMap<ObjectId, String> = HashMap::new();
    for s in state.security.values().filter(|s| s.enabled) {
        if let Some(o) = cat.objects.get(s.object_id) {
            protected
                .entry(o.db_id)
                .or_default()
                .push((s.object_id, o.name.to_lowercase()));
            names.insert(s.object_id, o.name.clone());
        }
    }
    let db_of = |schema: &str| -> Option<DbId> {
        let rest = schema.strip_prefix(&format!("t{tenant}_d"))?;
        let end = rest.find('_')?;
        rest[..end].parse().ok().map(DbId)
    };
    // Per database: its schemas here and their relations (lower case).
    let mut schemas: HashMap<DbId, std::collections::BTreeMap<String, Vec<String>>> =
        HashMap::new();
    for (s, n) in physical {
        if let Some(d) = db_of(s).filter(|d| protected.contains_key(d)) {
            schemas
                .entry(d)
                .or_default()
                .entry(s.clone())
                .or_default()
                .push(n.clone());
        }
    }
    for (acct, grants) in want.iter_mut() {
        let mine = bound.get(acct).map(Vec::as_slice).unwrap_or_default();
        for (db, tables) in &protected {
            let Some(in_db) = schemas.get(db) else {
                continue;
            };
            let binds: Vec<&(ObjectId, String)> = tables
                .iter()
                .filter(|(o, _)| mine.contains(o))
                .filter(|(o, _)| {
                    let v = policy_view(names.get(o).map_or("", String::as_str), *o).to_lowercase();
                    !definers.contains(&(acct.clone(), v))
                })
                .collect();
            // The database row as it was (the same pattern rows cover
            // every schema of the database).
            let first = in_db.keys().next().cloned().unwrap_or_default();
            let eff = db_row_for(grants, &first).unwrap_or_default();
            // Only where some binding table's view exists: until then the
            // gateway's rewrite alone filters reads.
            let any_view = binds.iter().any(|(o, _)| {
                let v = policy_view(names.get(o).map_or("", String::as_str), *o);
                in_db
                    .values()
                    .any(|rels| rels.iter().any(|r| r.eq_ignore_ascii_case(&v)))
            });
            let narrowed = any_view && eff.has(Action::Select);
            if narrowed {
                let p = db_pattern(tenant, *db);
                let e = grants.entry(BackendTarget::Db(p)).or_default();
                *e = eff.minus(select);
                // Rows more specific than the database's (old per-schema
                // rows) lose SELECT as well.
                for (t, a) in grants.iter_mut() {
                    if let BackendTarget::Db(pat) = t
                        && !pat.contains('%')
                        && in_db.contains_key(&pat.replace("\\_", "_"))
                    {
                        *a = a.minus(select);
                    }
                }
            }
            for (schema, rels) in in_db {
                let has = |n: &str| rels.iter().any(|r| r.eq_ignore_ascii_case(n));
                // Every account may call the context function: policy
                // views (and the views and routines reading them) call it
                // with their definer's rights; it returns the caller's own
                // context.
                if has(CTX_FUNCTION) {
                    let e = grants
                        .entry(BackendTarget::Routine(
                            schema.clone(),
                            CTX_FUNCTION.to_owned(),
                            "FUNCTION",
                        ))
                        .or_default();
                    *e = e.union(ActionSet::of(&[Action::Execute]));
                }
                for (object, name) in tables {
                    if !has(name) {
                        continue;
                    }
                    let spelled_name = rels
                        .iter()
                        .find(|r| r.eq_ignore_ascii_case(name))
                        .cloned()
                        .unwrap_or_else(|| name.clone());
                    let pview = policy_view(&spelled_name, *object);
                    if !has(&pview) {
                        continue;
                    }
                    let spelled = rels
                        .iter()
                        .find(|r| r.eq_ignore_ascii_case(name))
                        .cloned()
                        .unwrap_or_else(|| name.clone());
                    let tkey = BackendTarget::Table(schema.clone(), spelled);
                    // MariaDB keeps the table's name as created; the
                    // wanted rows use the catalog's spelling.
                    let tkey = grants
                        .keys()
                        .find(|k| matches!(k, BackendTarget::Table(s, n) if s == schema && n.to_lowercase() == *name))
                        .cloned()
                        .unwrap_or(tkey);
                    let table_row = grants.get(&tkey).copied().unwrap_or_default();
                    let held = eff.union(table_row).intersect(siud);
                    if held.is_empty() {
                        continue;
                    }
                    let vkey = BackendTarget::Table(schema.clone(), pview.clone());
                    if binds.iter().any(|(o, _)| o == object) {
                        let refs = ActionSet::of(&[Action::References]);
                        let e = grants.entry(tkey.clone()).or_default();
                        *e = e.minus(select).union(refs);
                        let v = grants.entry(vkey).or_default();
                        *v = v.union(held);
                        // Column SELECT grants move to the view's columns.
                        let cols: Vec<(BackendTarget, ActionSet)> = grants
                            .iter()
                            .filter(|(k, a)| {
                                matches!(k, BackendTarget::Column(s, t, _) if s == schema && t.to_lowercase() == *name)
                                    && a.has(Action::Select)
                            })
                            .map(|(k, a)| (k.clone(), *a))
                            .collect();
                        for (k, a) in cols {
                            if let BackendTarget::Column(s, _, c) = &k {
                                let vc = BackendTarget::Column(s.clone(), pview.clone(), c.clone());
                                let e = grants.entry(vc).or_default();
                                *e = e.union(select);
                            }
                            grants.insert(k, a.minus(select));
                        }
                    } else {
                        let now = db_row_for(grants, schema).unwrap_or_default();
                        let need = held.minus(now);
                        if !need.is_empty() {
                            let v = grants.entry(vkey).or_default();
                            *v = v.union(need);
                        }
                    }
                }
                if narrowed {
                    for rel in rels {
                        // A bound table keeps SELECT until its view is
                        // here: the gateway's rewrite still filters it.
                        let lower = rel.to_lowercase();
                        if lower == CTX_FUNCTION {
                            continue;
                        }
                        let read_through_view = binds
                            .iter()
                            .any(|(o, n)| *n == lower && has(&policy_view(rel, *o)));
                        if read_through_view {
                            continue;
                        }
                        let key = grants
                            .keys()
                            .find(|k| matches!(k, BackendTarget::Table(s, n) if s == schema && n.eq_ignore_ascii_case(rel)))
                            .cloned()
                            .unwrap_or_else(|| BackendTarget::Table(schema.clone(), rel.clone()));
                        let e = grants.entry(key).or_default();
                        *e = e.union(select);
                    }
                }
            }
        }
        grants.retain(|_, a| !a.is_empty());
    }
}

/// `sprig-auth`'s row-level security catalog module.
#[derive(Debug, Clone, Copy, Default)]
pub struct RlsExtension;

#[async_trait::async_trait]
impl CatalogExtension for RlsExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        RLS_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Auth)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = RlsStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(RlsState::default()));
        }
        let rows = store.load(conn).await?;
        Ok(Arc::new(RlsState::new(rows)))
    }
}

/// `prev` as an [`RlsState`], for tests.
#[must_use]
pub fn downcast(prev: &Arc<dyn ExtState>) -> Option<&RlsState> {
    let any: &dyn Any = &**prev;
    any.downcast_ref::<RlsState>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pol(
        id: u32,
        restrictive: bool,
        cmds: u8,
        g: Vec<Grantee>,
        u: Option<&str>,
        c: Option<&str>,
    ) -> PolicyRow {
        PolicyRow {
            policy_id: id,
            object_id: ObjectId(9),
            name: format!("p{id}"),
            restrictive,
            commands: cmds,
            grantees: g,
            using_expr: u.map(str::to_owned),
            check_expr: c.map(str::to_owned),
            created_by: UserId(1),
            changed_version: 1,
        }
    }

    fn exprs<'a>(c: &Combination<'a>) -> Vec<&'a str> {
        c.permissive.iter().map(|x| x.expr).collect()
    }

    #[test]
    fn combinations_follow_postgresql() {
        let table = TableRls {
            object: ObjectId(9),
            db: DbId(1),
            policies: vec![
                pol(1, false, cmd::SELECT | cmd::UPDATE, vec![], Some("a"), None),
                pol(2, false, 15, vec![], None, Some("w")),
                pol(3, true, 15, vec![], Some("r"), None),
                pol(4, false, cmd::INSERT, vec![], None, Some("i")),
            ],
            checks_current: true,
            checks_deployed: true,
        };
        let select = table.combination(cmd::SELECT, Clause::Using);
        assert_eq!(exprs(&select), vec!["a"]);
        assert_eq!(select.restrictive.len(), 1);
        assert_eq!(
            exprs(&table.combination(cmd::INSERT, Clause::Check)),
            vec!["w", "i"]
        );
        assert_eq!(
            exprs(&table.combination(cmd::UPDATE, Clause::Check)),
            vec!["a", "w"]
        );
        assert!(
            table
                .combination(cmd::DELETE, Clause::Using)
                .permissive
                .is_empty()
        );
    }

    fn catalog_with(enabled: &[u64]) -> TenantCatalog {
        use sprig_catalog::cache::Loaded;
        use sprig_catalog::store::{
            DatabaseRow, DbState, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
        };
        use sprig_catalog::{Snapshot, Versions};
        use sprig_common::ids::{GroupId, TenantId};
        use sprig_settings::{AnySetting, Registry};
        use std::time::SystemTime;
        static R: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
        let reg = R.get_or_init(|| {
            Registry::new([&crate::authz::settings::RLS_BACKEND_ENFORCEMENT as &dyn AnySetting])
                .unwrap()
        });
        let obj = |id: u64, name: &str| ObjectRow {
            object_id: ObjectId(id),
            db_id: DbId(1),
            namespace: ObjectKind::Table.namespace(),
            name: name.to_owned(),
            name_key: name.to_lowercase(),
            kind: ObjectKind::Table,
            parent_id: None,
            state: ObjectState::Active,
            schema_version: 1,
            flags: ObjectFlags::empty(),
            pending_job_id: None,
            created_at: SystemTime::UNIX_EPOCH,
            changed_version: 1,
        };
        let rls = RlsState::new(RlsRows {
            policies: Vec::new(),
            security: enabled
                .iter()
                .map(|o| SecurityRow {
                    object_id: ObjectId(*o),
                    db_id: DbId(1),
                    enabled: true,
                    forced: false,
                    checks_version: 1,
                    changed_version: 1,
                })
                .collect(),
        });
        TenantCatalog::build(
            reg,
            TenantId(42),
            Loaded {
                snapshot: Snapshot {
                    versions: Versions {
                        catalog: 1,
                        auth: 1,
                        purged: 0,
                    },
                    since: None,
                    databases: vec![DatabaseRow {
                        db_id: DbId(1),
                        name: "shop".into(),
                        name_key: "shop".into(),
                        default_charset: "utf8mb4".into(),
                        default_collation: "utf8mb4_uca1400_ai_ci".into(),
                        default_group_id: GroupId(0),
                        state: DbState::Active,
                        options: serde_json::json!({ "shards": [1] }),
                        created_at: SystemTime::UNIX_EPOCH,
                        created_by: None,
                        changed_version: 1,
                    }],
                    objects: vec![obj(10, "Docs"), obj(11, "notes")],
                    settings: Vec::new(),
                },
                ext: vec![(MODULE, Arc::new(rls) as Arc<dyn ExtState>)],
            },
        )
    }

    #[test]
    fn bound_accounts_read_protected_tables_through_their_views() {
        use super::super::backend::{BackendState, BackendTarget, db_row_for};
        use super::super::privilege::ActionSet;
        let cat = catalog_with(&[10]);
        let all = ActionSet::of(&[
            Action::Select,
            Action::Insert,
            Action::Update,
            Action::Delete,
        ]);
        let tenant_row = BackendTarget::Db("t42\\_d%".into());
        let mut want = BackendState::new();
        // u1: *.* (bound); u2: *.* (not bound); u3: table grants only (bound).
        want.entry("t42_u1".into())
            .or_default()
            .insert(tenant_row.clone(), all);
        want.entry("t42_u2".into())
            .or_default()
            .insert(tenant_row.clone(), all);
        let u3 = want.entry("t42_u3".into()).or_default();
        u3.insert(
            BackendTarget::Table("t42_d1_g0_s1".into(), "Docs".into()),
            all,
        );
        u3.insert(
            BackendTarget::Table("t42_d1_g0_s1".into(), "notes".into()),
            all,
        );
        let bound: HashMap<String, Vec<ObjectId>> = [
            ("t42_u1".to_owned(), vec![ObjectId(10)]),
            ("t42_u3".to_owned(), vec![ObjectId(10)]),
        ]
        .into_iter()
        .collect();
        let s = "t42_d1_g0_s1".to_owned();
        let physical = vec![
            (s.clone(), "Docs".to_owned()),
            (s.clone(), "notes".to_owned()),
            (s.clone(), "__sprig_rls_Docs".to_owned()),
            (s.clone(), "__sprig_redo".to_owned()),
            (s.clone(), CTX_FUNCTION.to_owned()),
            ("t42_d1_g7_s1".to_owned(), "other".to_owned()),
        ];
        narrow(&mut want, &cat, &bound, &physical, &std::collections::HashSet::new());
        let sel = ActionSet::of(&[Action::Select]);
        let refs = ActionSet::of(&[Action::References]);
        let t = |s: &str, n: &str| BackendTarget::Table(s.into(), n.into());
        let u1 = &want["t42_u1"];
        // The database's row has no SELECT; everything else is readable
        // one by one, the table only through its view.
        assert_eq!(db_row_for(u1, &s), Some(all.minus(sel)));
        assert_eq!(u1.get(&t(&s, "Docs")).copied(), Some(refs));
        assert_eq!(u1.get(&t(&s, "__sprig_rls_Docs")).copied(), Some(all));
        assert_eq!(u1.get(&t(&s, "notes")).copied(), Some(sel));
        assert_eq!(u1.get(&t(&s, "__sprig_redo")).copied(), Some(sel));
        assert_eq!(u1.get(&t("t42_d1_g7_s1", "other")).copied(), Some(sel));
        let f = BackendTarget::Routine(s.clone(), CTX_FUNCTION.into(), "FUNCTION");
        assert_eq!(u1.get(&f).copied(), Some(ActionSet::of(&[Action::Execute])));
        assert!(!u1.contains_key(&t(&s, CTX_FUNCTION)));
        // Not bound: its database row covers the view; it may call the
        // context function.
        assert_eq!(want["t42_u2"].len(), 2);
        // Table grants: SELECT moves to the view.
        let u3 = &want["t42_u3"];
        assert_eq!(
            u3.get(&t(&s, "Docs")).copied(),
            Some(all.minus(sel).union(refs))
        );
        assert_eq!(u3.get(&t(&s, "__sprig_rls_Docs")).copied(), Some(all));
        assert_eq!(u3.get(&t(&s, "notes")).copied(), Some(all));
        // Without the view (not deployed yet), nothing changes.
        let mut want2 = BackendState::new();
        want2
            .entry("t42_u1".into())
            .or_default()
            .insert(tenant_row.clone(), all);
        let before = want2.clone();
        narrow(&mut want2, &cat, &bound, &physical[..2], &std::collections::HashSet::new());
        // A view's definer keeps reading the table itself.
        let mut want3 = BackendState::new();
        want3.entry("t42_u1".into()).or_default().insert(tenant_row.clone(), all);
        let before3 = want3.clone();
        let defs = [("t42_u1".to_owned(), "__sprig_rls_docs".to_owned())].into_iter().collect();
        narrow(&mut want3, &cat, &bound, &physical[..3], &defs);
        assert_eq!(want3, before3);
        assert_eq!(want2, before);
    }

    #[test]
    fn grantees_round_trip() {
        let g = vec![Grantee::User(UserId(7)), Grantee::Role(RoleId(3))];
        assert_eq!(grantees_of(&grantees_json(&g)).unwrap(), g);
        assert!(grantees_of("[]").unwrap().is_empty());
        assert_eq!(commands_of("select,delete"), cmd::SELECT | cmd::DELETE);
        assert_eq!(commands_text(15), "select,insert,update,delete");
        let p = pol(1, false, 1, vec![Grantee::Role(RoleId(3))], Some("a"), None);
        assert!(p.applies_to(UserId(5), &[RoleId(3)]));
        assert!(!p.applies_to(UserId(5), &[RoleId(4)]));
    }
}
