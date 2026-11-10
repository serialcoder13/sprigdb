//! The `inherit_msg` catalog module (chapter 42, "Where definitions and
//! instances live"): `inherited_defs`, `inherited_overrides` and
//! `inherited_instances` in `t<tenant>_sprig`, their migration, the
//! snapshot loader ([`MsgExtension`]) and the writes.
//!
//! Definitions and overrides are versioned with the catalog: they are
//! written inside a catalog change ([`MsgStore`] on the change's
//! transaction). Instance rows are written outside one (chapter 42: "not
//! versioned"); the member's catalog change that creates the instance's
//! physical object (a stream's `streams` row, a trigger's binding) comes
//! right after, so the next snapshot has both.

use std::sync::Arc;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use serde_json::Value;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module, Phase};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use super::model::{
    AppliesTo, Cause, DefKey, DefKind, DefState, Definition, Instance, InstanceState, MsgState,
    Override, Scope, What, db_key,
};

/// The module name.
pub const MODULE: &str = "inherit_msg";

/// `inherited_defs` (chapter 42), in the declaring tenant.
pub const DDL_DEFS: &str = "CREATE TABLE IF NOT EXISTS inherited_defs (
  object_id         BIGINT UNSIGNED NOT NULL,
  kind              ENUM('stream','exchange','queue','binding','trigger',
                         'connection','sink') NOT NULL,
  db_name           VARCHAR(64)  NOT NULL,
  name              VARBINARY(255) NOT NULL,
  scope             JSON NOT NULL,
  definition        JSON NOT NULL,
  grants            JSON NULL,
  def_version       BIGINT UNSIGNED NOT NULL,
  state             ENUM('active','dropping','dropped') NOT NULL,
  drop_after        DATETIME(3) NULL,
  created_by        INT UNSIGNED NOT NULL,
  created_at        DATETIME(3) NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (object_id),
  UNIQUE KEY uk_def_name (db_name, kind, name)
) ENGINE=InnoDB";

/// `inherited_overrides` (chapter 42), in a member or an intermediate
/// tenant.
pub const DDL_OVERRIDES: &str = "CREATE TABLE IF NOT EXISTS inherited_overrides (
  def_tenant_id     BIGINT UNSIGNED NOT NULL,
  def_object_id     BIGINT UNSIGNED NOT NULL,
  target_tenant_id  BIGINT UNSIGNED NOT NULL,
  applies_to        ENUM('self','subtree') NOT NULL,
  what              ENUM('settings','body','options','credentials','state') NOT NULL,
  value             JSON NOT NULL,
  set_by            INT UNSIGNED NOT NULL,
  set_at            DATETIME(3) NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (def_tenant_id, def_object_id, target_tenant_id, applies_to, what)
) ENGINE=InnoDB";

/// `inherited_instances` (chapter 42), in the member; not versioned.
pub const DDL_INSTANCES: &str = "CREATE TABLE IF NOT EXISTS inherited_instances (
  def_tenant_id     BIGINT UNSIGNED NOT NULL,
  def_object_id     BIGINT UNSIGNED NOT NULL,
  instance_id       BIGINT UNSIGNED NOT NULL,
  kind              ENUM('stream','exchange','queue','trigger','sink') NOT NULL,
  def_version       BIGINT UNSIGNED NOT NULL,
  definition        JSON NOT NULL,
  state             ENUM('creating','active','dropping','detached') NOT NULL,
  cause             ENUM('publish','consume','declare','precreate') NOT NULL,
  physical          JSON NULL,
  created_at        DATETIME(3) NOT NULL,
  last_used_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (def_tenant_id, def_object_id),
  UNIQUE KEY uk_instance (instance_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static MSG_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "inherited_defs, inherited_overrides and inherited_instances",
    statements: &[DDL_DEFS, DDL_OVERRIDES, DDL_INSTANCES],
}];

/// The `inherit_msg` module, for [`Migrator::new`].
pub const MSG_MODULE: Module = Module {
    name: MODULE,
    migrations: MSG_MIGRATIONS,
};

/// Applies the module's migrations to catalog `schema` (idempotent).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[MSG_MODULE])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

/// A catalog error.
pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            1062 => SprigError::new(
                SprigCode::InheritNotOverridable,
                "ER_SPRIG_INHERIT_NOT_OVERRIDABLE: the name is taken",
            ),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("inherit_msg catalog: {what}"))
}

fn col<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| bad(&format!("bad column {i}")))
}

fn json_col(row: &Row, i: usize) -> Result<Value> {
    let s: String = col(row, i)?;
    serde_json::from_str(&s).map_err(|e| bad(&format!("column {i}: {e}")))
}

fn opt_json_col(row: &Row, i: usize) -> Result<Option<Value>> {
    let s: Option<String> = row
        .get_opt::<Option<String>, usize>(i)
        .and_then(std::result::Result::ok)
        .flatten();
    s.map(|s| serde_json::from_str(&s).map_err(|e| bad(&format!("column {i}: {e}"))))
        .transpose()
}

/// Milliseconds since the epoch as MariaDB's `DATETIME(3)` text.
#[must_use]
pub fn datetime(ms: u64) -> String {
    let secs = i64::try_from(ms / 1000).unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rest = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03}",
        rest / 3600,
        rest / 60 % 60,
        rest % 60,
        ms % 1000
    )
}

/// Now, ms since the epoch.
#[must_use]
pub fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

/// The module's rows in one catalog schema.
#[derive(Debug, Clone)]
pub struct MsgStore {
    schema: String,
}

impl MsgStore {
    /// The store of catalog schema `schema` (`t<tenant>_sprig`).
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(table))
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// The backend's.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'inherited_instances'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// The whole state of tenant `tenant`.
    ///
    /// # Errors
    /// The backend's, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q, tenant: TenantId) -> Result<MsgState> {
        let mut st = MsgState {
            migrated: true,
            ..MsgState::default()
        };
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT object_id, kind, db_name, CAST(name AS CHAR), CAST(scope AS CHAR), \
                 CAST(definition AS CHAR), CAST(grants AS CHAR), def_version, state, \
                 CAST(UNIX_TIMESTAMP(drop_after) * 1000 AS UNSIGNED), created_by, \
                 CAST(UNIX_TIMESTAMP(created_at) * 1000 AS UNSIGNED), changed_version \
                 FROM {} WHERE state <> 'dropped'",
                self.t("inherited_defs")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let d = def_of(r, tenant)?;
            st.defs
                .insert((db_key(&d.db_name), d.kind, d.name.clone()), Arc::new(d));
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT def_tenant_id, def_object_id, target_tenant_id, applies_to, what, \
                 CAST(value AS CHAR), set_by, CAST(UNIX_TIMESTAMP(set_at) * 1000 AS UNSIGNED) \
                 FROM {}",
                self.t("inherited_overrides")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let applies: String = col(r, 3)?;
            let what: String = col(r, 4)?;
            st.overrides.push(Arc::new(Override {
                made_in: tenant,
                def: DefKey {
                    tenant: TenantId(col(r, 0)?),
                    object: col(r, 1)?,
                },
                target: TenantId(col(r, 2)?),
                applies_to: if applies == "subtree" {
                    AppliesTo::Subtree
                } else {
                    AppliesTo::Own
                },
                what: What::parse(&what).ok_or_else(|| bad("what"))?,
                value: json_col(r, 5)?,
                set_by: col(r, 6)?,
                set_at_ms: col(r, 7)?,
            }));
        }
        for i in self.instances(q).await? {
            st.instances.insert(i.def, Arc::new(i));
        }
        Ok(st)
    }

    /// Every instance row.
    ///
    /// # Errors
    /// The backend's.
    pub async fn instances<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Vec<Instance>> {
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT def_tenant_id, def_object_id, instance_id, kind, def_version, \
                 CAST(definition AS CHAR), state, cause, CAST(physical AS CHAR), \
                 CAST(UNIX_TIMESTAMP(created_at) * 1000 AS UNSIGNED) FROM {}",
                self.t("inherited_instances")
            ))
            .await
            .map_err(map_err)?;
        rows.iter().map(instance_of).collect()
    }

    /// One instance row (a primary-key read).
    ///
    /// # Errors
    /// The backend's (a missing table is `None`).
    pub async fn instance<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        def: DefKey,
    ) -> Result<Option<Instance>> {
        let r: std::result::Result<Option<Row>, _> = q
            .exec_first(
                format!(
                    "SELECT def_tenant_id, def_object_id, instance_id, kind, def_version, \
                     CAST(definition AS CHAR), state, cause, CAST(physical AS CHAR), \
                     CAST(UNIX_TIMESTAMP(created_at) * 1000 AS UNSIGNED) FROM {} \
                     WHERE def_tenant_id = ? AND def_object_id = ?",
                    self.t("inherited_instances")
                ),
                (def.tenant.0, def.object),
            )
            .await;
        match r {
            Ok(Some(row)) => Ok(Some(instance_of(&row)?)),
            Ok(None) => Ok(None),
            Err(mysql_async::Error::Server(s)) if s.code == 1146 => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Claims an instance: inserts its row in state `creating` unless one
    /// exists (two gateways racing: one insert wins). Returns whether this
    /// call inserted it.
    ///
    /// # Errors
    /// The backend's.
    pub async fn claim<Q: Queryable + Send>(&self, q: &mut Q, i: &Instance) -> Result<bool> {
        let n = affected(
            q,
            format!(
                "INSERT IGNORE INTO {} (def_tenant_id, def_object_id, instance_id, kind, \
                 def_version, definition, state, cause, physical, created_at, last_used_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("inherited_instances")
            ),
            (
                i.def.tenant.0,
                i.def.object,
                i.instance_id,
                i.kind.as_str(),
                i.def_version,
                i.definition.to_string(),
                i.state.as_str(),
                i.cause.as_str(),
                i.physical.as_ref().map(Value::to_string),
            )
                .into(),
        )
        .await?;
        Ok(n == 1)
    }

    /// Takes over a `creating` row nobody finished since `older_than_ms`
    /// (a gateway that died while creating): moves its `created_at` to now
    /// if it still has the old one. Returns whether this call took it.
    ///
    /// # Errors
    /// The backend's.
    pub async fn take_over<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        def: DefKey,
        older_than_ms: u64,
    ) -> Result<bool> {
        let n = affected(
            q,
            format!(
                "UPDATE {} SET created_at = UTC_TIMESTAMP(3) WHERE def_tenant_id = ? AND \
                 def_object_id = ? AND state = 'creating' AND \
                 created_at < UTC_TIMESTAMP(3) - INTERVAL ? MICROSECOND",
                self.t("inherited_instances")
            ),
            (def.tenant.0, def.object, older_than_ms.saturating_mul(1000)).into(),
        )
        .await?;
        Ok(n == 1)
    }

    /// Sets an instance's state, version and definition copy.
    ///
    /// # Errors
    /// The backend's.
    pub async fn set_instance<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        def: DefKey,
        state: InstanceState,
        def_version: u64,
        definition: &Value,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET state = ?, def_version = ?, definition = ?, \
                 last_used_at = UTC_TIMESTAMP(3) WHERE def_tenant_id = ? AND def_object_id = ?",
                self.t("inherited_instances")
            ),
            (
                state.as_str(),
                def_version,
                definition.to_string(),
                def.tenant.0,
                def.object,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes an instance row.
    ///
    /// # Errors
    /// The backend's.
    pub async fn delete_instance<Q: Queryable + Send>(&self, q: &mut Q, def: DefKey) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE def_tenant_id = ? AND def_object_id = ?",
                self.t("inherited_instances")
            ),
            (def.tenant.0, def.object),
        )
        .await
        .map_err(map_err)
    }

    /// Writes (or replaces) a definition, at catalog version `version`.
    ///
    /// # Errors
    /// The backend's; 39897 when the name is taken.
    pub async fn put_def<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        d: &Definition,
        version: u64,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "REPLACE INTO {} (object_id, kind, db_name, name, scope, definition, grants, \
                 def_version, state, drop_after, created_by, created_at, changed_version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.t("inherited_defs")
            ),
            mysql_async::Params::Positional(vec![
                d.object_id.into(),
                d.kind.as_str().into(),
                d.db_name.as_str().into(),
                d.name.as_bytes().into(),
                serde_json::to_string(&d.scope).unwrap_or_default().into(),
                d.definition.to_string().into(),
                d.grants.as_ref().map(Value::to_string).into(),
                d.def_version.into(),
                d.state.as_str().into(),
                d.drop_after_ms.map(datetime).into(),
                d.created_by.into(),
                datetime(d.created_at_ms).into(),
                version.into(),
            ]),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes a definition row.
    ///
    /// # Errors
    /// The backend's.
    pub async fn delete_def<Q: Queryable + Send>(&self, q: &mut Q, object_id: u64) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE object_id = ?",
                self.t("inherited_defs")
            ),
            (object_id,),
        )
        .await
        .map_err(map_err)
    }

    /// Writes (or replaces) an override made in this tenant.
    ///
    /// # Errors
    /// The backend's.
    pub async fn put_override<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        o: &Override,
        version: u64,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "REPLACE INTO {} (def_tenant_id, def_object_id, target_tenant_id, applies_to, \
                 what, value, set_by, set_at, changed_version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?)",
                self.t("inherited_overrides")
            ),
            (
                o.def.tenant.0,
                o.def.object,
                o.target.0,
                o.applies_to.as_str(),
                o.what.as_str(),
                o.value.to_string(),
                o.set_by,
                version,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes overrides made in this tenant: of `what` (every kind when
    /// `None`) for `target` and `applies_to`.
    ///
    /// # Errors
    /// The backend's.
    pub async fn delete_override<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        def: DefKey,
        target: TenantId,
        applies_to: AppliesTo,
        what: Option<What>,
    ) -> Result<u64> {
        let (cond, params): (&str, Vec<mysql_async::Value>) = match what {
            Some(w) => (
                " AND what = ?",
                vec![
                    def.tenant.0.into(),
                    def.object.into(),
                    target.0.into(),
                    applies_to.as_str().into(),
                    w.as_str().into(),
                ],
            ),
            None => (
                "",
                vec![
                    def.tenant.0.into(),
                    def.object.into(),
                    target.0.into(),
                    applies_to.as_str().into(),
                ],
            ),
        };
        affected(
            q,
            format!(
                "DELETE FROM {} WHERE def_tenant_id = ? AND def_object_id = ? AND \
                 target_tenant_id = ? AND applies_to = ?{cond}",
                self.t("inherited_overrides")
            ),
            mysql_async::Params::Positional(params),
        )
        .await
    }
}

/// Runs one statement; its affected rows.
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

fn def_of(r: &Row, tenant: TenantId) -> Result<Definition> {
    let kind: String = col(r, 1)?;
    let state: String = col(r, 8)?;
    let scope: Scope =
        serde_json::from_value(json_col(r, 4)?).map_err(|e| bad(&format!("scope: {e}")))?;
    Ok(Definition {
        declarer: tenant,
        object_id: col(r, 0)?,
        kind: DefKind::parse(&kind).ok_or_else(|| bad("kind"))?,
        db_name: col(r, 2)?,
        name: col(r, 3)?,
        scope,
        definition: json_col(r, 5)?,
        grants: opt_json_col(r, 6)?,
        def_version: col(r, 7)?,
        state: DefState::parse(&state).ok_or_else(|| bad("state"))?,
        drop_after_ms: r
            .get_opt::<Option<u64>, usize>(9)
            .and_then(std::result::Result::ok)
            .flatten(),
        created_by: col(r, 10)?,
        created_at_ms: col(r, 11)?,
        changed_version: col(r, 12)?,
    })
}

fn instance_of(r: &Row) -> Result<Instance> {
    let kind: String = col(r, 3)?;
    let state: String = col(r, 6)?;
    let cause: String = col(r, 7)?;
    Ok(Instance {
        def: DefKey {
            tenant: TenantId(col(r, 0)?),
            object: col(r, 1)?,
        },
        instance_id: col(r, 2)?,
        kind: DefKind::parse(&kind).ok_or_else(|| bad("kind"))?,
        def_version: col(r, 4)?,
        definition: json_col(r, 5)?,
        state: InstanceState::parse(&state).ok_or_else(|| bad("state"))?,
        cause: Cause::parse(&cause).ok_or_else(|| bad("cause"))?,
        physical: opt_json_col(r, 8)?,
        created_at_ms: col(r, 9)?,
    })
}

/// The module in the catalog cache: [`MsgState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgExtension;

#[async_trait::async_trait]
impl CatalogExtension for MsgExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        MSG_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = MsgStore::new(tx.schema());
        let tenant = tenant_of_schema(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(MsgState::default()));
        }
        Ok(Arc::new(store.load(conn, tenant).await?))
    }
}

/// `t<tenant>_sprig` → the tenant.
#[must_use]
pub fn tenant_of_schema(schema: &str) -> TenantId {
    TenantId(
        schema
            .strip_prefix('t')
            .and_then(|s| s.strip_suffix("_sprig"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
    )
}
