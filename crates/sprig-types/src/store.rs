//! The `types` catalog module (chapter 32, "Catalog tables"; build step 1):
//! `types`, `type_fields`, `type_bindings` and `hierarchies` in
//! `t<tenant>_sprig`, their migrations, the snapshot loader
//! ([`TypesExtension`]) and the writes type DDL makes ([`TypesChange`]).
//!
//! The module is versioned with the catalog version: every type DDL commits
//! its rows in one versioned change, and every gateway's snapshot reloads
//! the four tables whole (they are small). A catalog bootstrapped before the
//! module has no tables; the first type DDL migrates it ([`migrate`]), and
//! until then the state is [`TypesState::unmigrated`].
//!
//! Names are never stored here: a type's or hierarchy's table name comes
//! from its `objects` row at load time, so `objects` stays the one source of
//! names.

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module, Phase};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    DiscValue, FieldDef, HierState, HierarchyDef, KeyClass, OnDelete, Storage, TypeDef, TypeState,
    TypesState,
};

/// The module name.
pub const MODULE: &str = "types";

/// `types` (chapter 32).
pub const DDL_TYPES: &str = "CREATE TABLE IF NOT EXISTS types (
  type_id        BIGINT UNSIGNED NOT NULL,
  db_id          INT UNSIGNED NOT NULL,
  parent_id      BIGINT UNSIGNED NULL,
  root_id        BIGINT UNSIGNED NOT NULL,
  depth          TINYINT UNSIGNED NOT NULL,
  type_path      VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  kind           ENUM('entity','composite') NOT NULL,
  is_abstract    BOOLEAN NOT NULL DEFAULT FALSE,
  storage        ENUM('joined','single_table','none') NOT NULL,
  key_fields     JSON NULL,
  physical_table VARCHAR(64) NULL,
  json_schema    JSON NULL,
  aliases        JSON NULL,
  state          ENUM('active','renaming','dropping') NOT NULL DEFAULT 'active',
  version        BIGINT UNSIGNED NOT NULL,
  created_at     DATETIME(3) NOT NULL,
  updated_at     DATETIME(3) NOT NULL,
  PRIMARY KEY (type_id),
  UNIQUE KEY uk_types_path (db_id, type_path),
  KEY k_types_parent (parent_id),
  KEY k_types_root (root_id)
) ENGINE=InnoDB";

/// `type_fields` (chapter 32).
pub const DDL_TYPE_FIELDS: &str = "CREATE TABLE IF NOT EXISTS type_fields (
  type_id         BIGINT UNSIGNED NOT NULL,
  field_name      VARCHAR(64) NOT NULL,
  ordinal         SMALLINT UNSIGNED NOT NULL,
  data_type       VARCHAR(255) NOT NULL,
  composite_id    BIGINT UNSIGNED NULL,
  is_array        BOOLEAN NOT NULL DEFAULT FALSE,
  array_storage   ENUM('child_table','json') NULL,
  nullable        BOOLEAN NOT NULL DEFAULT TRUE,
  default_expr    TEXT NULL,
  srid            INT UNSIGNED NULL,
  doc_path        VARCHAR(255) NULL,
  physical_table  VARCHAR(64) NULL,
  physical_column VARCHAR(64) NULL,
  PRIMARY KEY (type_id, field_name),
  UNIQUE KEY uk_tf_ordinal (type_id, ordinal),
  KEY k_tf_composite (composite_id)
) ENGINE=InnoDB";

/// `type_bindings` (chapter 32; written by the document, key-value, vector
/// and dimensional models, none built yet).
pub const DDL_TYPE_BINDINGS: &str = "CREATE TABLE IF NOT EXISTS type_bindings (
  object_id  BIGINT UNSIGNED NOT NULL,
  type_id    BIGINT UNSIGNED NOT NULL,
  model      ENUM('table','collection','kv','vector','dimension') NOT NULL,
  field_name VARCHAR(64) NULL,
  options    JSON NULL,
  created_at DATETIME(3) NOT NULL,
  PRIMARY KEY (object_id),
  KEY k_tb_type (type_id)
) ENGINE=InnoDB";

/// `hierarchies` (chapter 32).
pub const DDL_HIERARCHIES: &str = "CREATE TABLE IF NOT EXISTS hierarchies (
  object_id     BIGINT UNSIGNED NOT NULL,
  model         ENUM('table','collection') NOT NULL,
  key_fields    JSON NOT NULL,
  parent_fields JSON NOT NULL,
  on_delete     ENUM('restrict','cascade') NOT NULL DEFAULT 'restrict',
  max_depth     SMALLINT UNSIGNED NOT NULL,
  state         ENUM('backfilling','active','failed','dropping') NOT NULL,
  job_id        BINARY(16) NULL,
  created_at    DATETIME(3) NOT NULL,
  updated_at    DATETIME(3) NOT NULL,
  PRIMARY KEY (object_id)
) ENGINE=InnoDB";

/// Format 2: `types.discriminator`, a root's discriminator column
/// (`{"column": "vtype"}`) or a subtype's value (`{"value": "TRUCK"}`,
/// `{"number": "5"}`).
pub const DDL_DISCRIMINATOR: &str =
    "ALTER TABLE types ADD COLUMN IF NOT EXISTS discriminator JSON NULL AFTER storage";

/// The module's migrations.
pub static TYPES_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "types, type_fields, type_bindings and hierarchies",
        statements: &[
            DDL_TYPES,
            DDL_TYPE_FIELDS,
            DDL_TYPE_BINDINGS,
            DDL_HIERARCHIES,
        ],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "types.discriminator",
        statements: &[DDL_DISCRIMINATOR],
    },
];

/// The `types` module, for [`Migrator::new`].
pub const TYPES_MODULE: Module = Module {
    name: MODULE,
    migrations: TYPES_MIGRATIONS,
};

/// Applies the module's migrations to catalog `schema` (idempotent, under
/// the migrator's lock).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut Conn, schema: &str) -> Result<()> {
    Migrator::new(&[TYPES_MODULE])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
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

fn col<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| SprigError::internal(format!("types catalog: bad column {i}")))
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("types catalog: {what}"))
}

/// A `types.discriminator` value: a root's column, or a subtype's value.
fn discriminator_json(t: &TypeDef) -> Option<String> {
    if let Some(c) = &t.discriminator {
        return Some(serde_json::json!({ "column": c }).to_string());
    }
    t.disc_value.as_ref().map(|v| v.to_json().to_string())
}

/// Reads [`discriminator_json`]'s text back.
fn parse_discriminator(j: Option<String>) -> Result<(Option<String>, Option<DiscValue>)> {
    let Some(j) = j else {
        return Ok((None, None));
    };
    let v: serde_json::Value = serde_json::from_str(&j).map_err(|_| bad("discriminator"))?;
    if let Some(c) = v.get("column").and_then(serde_json::Value::as_str) {
        return Ok((Some(c.to_owned()), None));
    }
    if let Some(s) = v.get("value").and_then(serde_json::Value::as_str) {
        return Ok((None, Some(DiscValue::Str(s.to_owned()))));
    }
    if let Some(n) = v.get("number").and_then(serde_json::Value::as_str) {
        return Ok((None, Some(DiscValue::Num(n.to_owned()))));
    }
    Ok((None, None))
}

/// The JSON of a hierarchy's `key_fields`: `[{"name":..,"class":..,"type":..}]`.
fn key_fields_json(h: &HierarchyDef) -> String {
    serde_json::json!([{ "name": h.key, "class": h.key_class.as_str(), "type": h.key_type }])
        .to_string()
}

/// Reads and writes the module's tables in one catalog schema.
#[derive(Debug, Clone)]
pub struct TypesStore {
    schema: String,
}

impl TypesStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{table}", quote(&self.schema))
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'hierarchies'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Whether the module is at its newest format (format 2's
    /// `types.discriminator` exists).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn current<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'types' AND COLUMN_NAME = 'discriminator'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Every entity type and hierarchy whose table is live. The state is
    /// `migrated` only at the newest format, so the next type DDL migrates
    /// a format-1 catalog first.
    ///
    /// # Errors
    /// 39070, a backend error, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<TypesState> {
        let current = self.current(q).await?;
        let disc = if current {
            "CAST(t.discriminator AS CHAR)"
        } else {
            "NULL"
        };
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT t.type_id, t.db_id, t.parent_id, t.root_id, t.depth, t.type_path, \
                 t.is_abstract, t.storage, CAST(t.key_fields AS CHAR), t.state, t.version, o.name, \
                 {disc} FROM {} t JOIN {} o ON o.object_id = t.type_id \
                 WHERE t.kind = 'entity' AND o.state <> 'dropped' ORDER BY t.type_id",
                self.t("types"),
                self.t("objects")
            ))
            .await
            .map_err(map_err)?;
        let mut types = Vec::with_capacity(rows.len());
        for r in &rows {
            let key_fields: Option<String> = col(r, 8)?;
            let key_fields: Vec<String> = match key_fields {
                Some(j) => serde_json::from_str(&j).map_err(|_| bad("key_fields"))?,
                None => Vec::new(),
            };
            let storage: String = col(r, 7)?;
            let state: String = col(r, 9)?;
            let (discriminator, disc_value) = parse_discriminator(col(r, 12)?)?;
            types.push(TypeDef {
                id: ObjectId(col(r, 0)?),
                db: DbId(col(r, 1)?),
                parent: col::<Option<u64>>(r, 2)?.map(ObjectId),
                root: ObjectId(col(r, 3)?),
                depth: col(r, 4)?,
                path: col(r, 5)?,
                is_abstract: col::<i64>(r, 6)? != 0,
                storage: Storage::parse(&storage).ok_or_else(|| bad("storage"))?,
                key_fields,
                own_fields: Vec::new(),
                state: TypeState::parse(&state).ok_or_else(|| bad("state"))?,
                version: col(r, 10)?,
                name: col(r, 11)?,
                discriminator,
                disc_value,
            });
        }
        let fields: Vec<Row> = q
            .query(format!(
                "SELECT type_id, field_name, ordinal, data_type, nullable, default_expr, \
                 physical_table FROM {} ORDER BY type_id, ordinal",
                self.t("type_fields")
            ))
            .await
            .map_err(map_err)?;
        for r in &fields {
            let id = ObjectId(col(r, 0)?);
            let Some(t) = types.iter_mut().find(|t| t.id == id) else {
                continue;
            };
            let table: Option<String> = col(r, 6)?;
            t.own_fields.push(FieldDef {
                name: col(r, 1)?,
                ordinal: col(r, 2)?,
                data_type: col(r, 3)?,
                nullable: col::<i64>(r, 4)? != 0,
                default_expr: col(r, 5)?,
                physical_table: table.unwrap_or_else(|| t.name.clone()),
            });
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT h.object_id, o.db_id, o.name, CAST(h.key_fields AS CHAR), \
                 CAST(h.parent_fields AS CHAR), h.on_delete, h.max_depth, h.state \
                 FROM {} h JOIN {} o ON o.object_id = h.object_id \
                 WHERE h.model = 'table' AND o.state <> 'dropped' ORDER BY h.object_id",
                self.t("hierarchies"),
                self.t("objects")
            ))
            .await
            .map_err(map_err)?;
        let mut hierarchies = Vec::with_capacity(rows.len());
        for r in &rows {
            let keys: String = col(r, 3)?;
            let keys: serde_json::Value =
                serde_json::from_str(&keys).map_err(|_| bad("key_fields"))?;
            let key = keys.get(0).ok_or_else(|| bad("key_fields"))?;
            let parents: String = col(r, 4)?;
            let parents: Vec<String> =
                serde_json::from_str(&parents).map_err(|_| bad("parent_fields"))?;
            let on_delete: String = col(r, 5)?;
            let state: String = col(r, 7)?;
            hierarchies.push(HierarchyDef {
                object_id: ObjectId(col(r, 0)?),
                db: DbId(col(r, 1)?),
                table: col(r, 2)?,
                key: key["name"].as_str().ok_or_else(|| bad("key"))?.to_owned(),
                key_class: KeyClass::parse(key["class"].as_str().unwrap_or(""))
                    .ok_or_else(|| bad("key class"))?,
                key_type: key["type"].as_str().unwrap_or("").to_owned(),
                parent: parents.into_iter().next().ok_or_else(|| bad("parent"))?,
                on_delete: if on_delete == "cascade" {
                    OnDelete::Cascade
                } else {
                    OnDelete::Restrict
                },
                max_depth: col(r, 6)?,
                state: HierState::parse(&state).ok_or_else(|| bad("hierarchy state"))?,
            });
        }
        let mut state = TypesState::new(types, hierarchies);
        state.migrated = current;
        Ok(state)
    }

    /// Applies `change` in `q` (the versioned change's transaction).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn apply<Q: Queryable + Send>(&self, q: &mut Q, change: &TypesChange) -> Result<()> {
        for id in &change.delete_types {
            q.exec_drop(
                format!("DELETE FROM {} WHERE type_id = ?", self.t("type_fields")),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
            q.exec_drop(
                format!("DELETE FROM {} WHERE type_id = ?", self.t("types")),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        }
        for (id, state) in &change.type_states {
            q.exec_drop(
                format!(
                    "UPDATE {} SET state = ?, version = version + 1, updated_at = UTC_TIMESTAMP(3) \
                     WHERE type_id = ?",
                    self.t("types")
                ),
                (state.as_str(), id.0),
            )
            .await
            .map_err(map_err)?;
        }
        for t in &change.upsert_types {
            let key_fields = serde_json::to_string(&t.key_fields).unwrap_or_else(|_| "[]".into());
            q.exec_drop(
                format!(
                    "INSERT INTO {} (type_id, db_id, parent_id, root_id, depth, type_path, kind, \
                     is_abstract, storage, key_fields, physical_table, state, version, created_at, \
                     updated_at, discriminator) VALUES (?, ?, ?, ?, ?, ?, 'entity', ?, ?, ?, ?, ?, \
                     1, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), ?) \
                     ON DUPLICATE KEY UPDATE parent_id = VALUES(parent_id), \
                     root_id = VALUES(root_id), depth = VALUES(depth), \
                     type_path = VALUES(type_path), is_abstract = VALUES(is_abstract), \
                     storage = VALUES(storage), key_fields = VALUES(key_fields), \
                     discriminator = VALUES(discriminator), \
                     physical_table = VALUES(physical_table), state = VALUES(state), \
                     version = version + 1, updated_at = VALUES(updated_at)",
                    self.t("types")
                ),
                (
                    t.id.0,
                    t.db.0,
                    t.parent.map(|p| p.0),
                    t.root.0,
                    t.depth,
                    t.path.as_str(),
                    t.is_abstract,
                    t.storage.as_str(),
                    key_fields,
                    t.name.as_str(),
                    t.state.as_str(),
                    discriminator_json(t),
                ),
            )
            .await
            .map_err(map_err)?;
            if let Some(fields) = change.fields_of(t.id) {
                self.replace_fields(q, t.id, fields).await?;
            }
        }
        for (id, fields) in &change.fields {
            if !change.upsert_types.iter().any(|t| t.id == *id) {
                self.replace_fields(q, *id, fields).await?;
            }
        }
        for id in &change.delete_hierarchies {
            q.exec_drop(
                format!("DELETE FROM {} WHERE object_id = ?", self.t("hierarchies")),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        }
        for h in &change.upsert_hierarchies {
            let parents = serde_json::to_string(&[&h.parent]).unwrap_or_else(|_| "[]".into());
            q.exec_drop(
                format!(
                    "INSERT INTO {} (object_id, model, key_fields, parent_fields, on_delete, \
                     max_depth, state, created_at, updated_at) VALUES (?, 'table', ?, ?, ?, ?, ?, \
                     UTC_TIMESTAMP(3), UTC_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE \
                     key_fields = VALUES(key_fields), parent_fields = VALUES(parent_fields), \
                     on_delete = VALUES(on_delete), max_depth = VALUES(max_depth), \
                     state = VALUES(state), updated_at = VALUES(updated_at)",
                    self.t("hierarchies")
                ),
                (
                    h.object_id.0,
                    key_fields_json(h),
                    parents,
                    h.on_delete.as_str(),
                    h.max_depth,
                    h.state.as_str(),
                ),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }

    async fn replace_fields<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: ObjectId,
        fields: &[FieldDef],
    ) -> Result<()> {
        q.exec_drop(
            format!("DELETE FROM {} WHERE type_id = ?", self.t("type_fields")),
            (id.0,),
        )
        .await
        .map_err(map_err)?;
        for (i, f) in fields.iter().enumerate() {
            q.exec_drop(
                format!(
                    "INSERT INTO {} (type_id, field_name, ordinal, data_type, nullable, \
                     default_expr, physical_table, physical_column) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    self.t("type_fields")
                ),
                (
                    id.0,
                    f.name.as_str(),
                    u16::try_from(i + 1).unwrap_or(u16::MAX),
                    f.data_type.as_str(),
                    f.nullable,
                    f.default_expr.as_deref(),
                    f.physical_table.as_str(),
                    f.name.as_str(),
                ),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }
}

/// What one type DDL statement writes to the module's tables, committed in
/// one versioned change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypesChange {
    /// Types created or replaced (with their fields, if in `fields`).
    pub upsert_types: Vec<TypeDef>,
    /// Types whose own fields are replaced.
    pub fields: Vec<(ObjectId, Vec<FieldDef>)>,
    /// State changes.
    pub type_states: Vec<(ObjectId, TypeState)>,
    /// Types removed with their fields.
    pub delete_types: Vec<ObjectId>,
    /// Hierarchies declared or changed.
    pub upsert_hierarchies: Vec<HierarchyDef>,
    /// Hierarchies removed.
    pub delete_hierarchies: Vec<ObjectId>,
}

impl TypesChange {
    fn fields_of(&self, id: ObjectId) -> Option<&[FieldDef]> {
        self.fields
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, f)| f.as_slice())
    }

    /// Whether the change writes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &TypesChange::default()
    }
}

impl ExtState for TypesState {
    fn bytes(&self) -> usize {
        self.dbs()
            .map(|(_, d)| {
                d.iter()
                    .map(|t| 256 + t.own_fields.len() * 128)
                    .sum::<usize>()
                    + d.hierarchies().count() * 256
            })
            .sum()
    }
}

/// The module in the catalog cache: [`TypesState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypesExtension;

#[async_trait::async_trait]
impl CatalogExtension for TypesExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        TYPES_MIGRATIONS
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
        let store = TypesStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(TypesState::unmigrated()));
        }
        Ok(Arc::new(store.load(conn).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        Migrator::new(&[TYPES_MODULE]).unwrap();
        assert_eq!(TYPES_MODULE.newest(), 2);
        for ddl in TYPES_MIGRATIONS[0].statements {
            assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS "));
        }
        assert!(TYPES_MIGRATIONS[1].statements[0].contains("ADD COLUMN IF NOT EXISTS"));
    }

    #[test]
    fn discriminators_round_trip_as_json() {
        let mut t = (**crate::model::fixtures::assets()
            .get("asset", false)
            .unwrap())
        .clone();
        assert_eq!(discriminator_json(&t), None);
        t.discriminator = Some("vtype".into());
        let j = discriminator_json(&t);
        assert_eq!(
            parse_discriminator(j).unwrap(),
            (Some("vtype".into()), None)
        );
        t.discriminator = None;
        for v in [
            DiscValue::Str("TRUCK 'x'".into()),
            DiscValue::Num("-5".into()),
        ] {
            t.disc_value = Some(v.clone());
            assert_eq!(
                parse_discriminator(discriminator_json(&t)).unwrap(),
                (None, Some(v))
            );
        }
    }

    #[test]
    fn hierarchy_keys_round_trip_as_json() {
        let db = crate::model::fixtures::assets();
        let h = db.hierarchy("org_unit", false).unwrap();
        let j: serde_json::Value = serde_json::from_str(&key_fields_json(h)).unwrap();
        assert_eq!(j[0]["name"], "id");
        assert_eq!(j[0]["class"], "int");
    }
}
