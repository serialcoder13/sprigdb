//! The `inherit` catalog module (chapter 42, "Catalog tables"; build step
//! 1): `spaces`, `inheritable_objects`, `shares` and `space_homes` in
//! `t<tenant>_sprig`, their migration, the snapshot loader
//! ([`InheritExtension`]) and the writes the extension statements make
//! ([`InheritChange`]).
//!
//! Like chapter 32's `types` module, the tables are versioned with the
//! catalog version and reloaded whole into every snapshot (they are small).
//! A catalog bootstrapped before the module has no tables; the first
//! extension statement migrates it ([`migrate`]).
//!
//! Differences from the chapter's tables, all additive: `shares.name`
//! (shares are named in SQL), `inheritable_objects.columns` (the column
//! order of the merged view) and `unique_keys` (the user `UNIQUE` keys as
//! declared, before `_sprig_space` widened them), and `space_homes` for
//! chapter 14's `users.home_space_id`, which the auth catalog doesn't have
//! yet.

use std::collections::BTreeMap;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module, Phase};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    InheritState, InheritableObject, MergeMode, ObjectExt, ROOT_SPACE, Share, ShareTarget, Space,
    SpaceId, Take, UniqueKey,
};

/// The module name.
pub const MODULE: &str = "inherit";

/// `spaces` (chapter 42).
pub const DDL_SPACES: &str = "CREATE TABLE IF NOT EXISTS spaces (
  space_id          BIGINT UNSIGNED NOT NULL,
  name              VARCHAR(64)     NOT NULL,
  parent_space_id   BIGINT UNSIGNED NULL,
  parent_tenant_id  BIGINT UNSIGNED NULL,
  parent_ext_space  BIGINT UNSIGNED NULL,
  depth             SMALLINT UNSIGNED NOT NULL,
  path              VARBINARY(2048) NOT NULL,
  state             ENUM('creating','active','dropping','dropped') NOT NULL,
  created_by        INT UNSIGNED    NOT NULL,
  created_at        DATETIME(6)     NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (space_id),
  UNIQUE KEY uk_name (name),
  KEY k_parent (parent_space_id),
  KEY k_path (path)
) ENGINE=InnoDB";

/// `inheritable_objects` (chapter 42).
pub const DDL_OBJECTS: &str = "CREATE TABLE IF NOT EXISTS inheritable_objects (
  object_id         BIGINT UNSIGNED NOT NULL,
  model             ENUM('table','collection','kv') NOT NULL,
  key_fields        JSON            NOT NULL,
  field_slots       JSON            NOT NULL,
  columns           JSON            NOT NULL,
  unique_keys       JSON            NOT NULL,
  tag_field         VARCHAR(255)    NULL,
  source_tenant_id  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  source_object_id  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  state             ENUM('enabling','active','disabling') NOT NULL,
  ddl_job_id        BIGINT UNSIGNED NULL,
  version           BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at        DATETIME(6)     NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (object_id)
) ENGINE=InnoDB";

/// `shares` (chapter 42).
pub const DDL_SHARES: &str = "CREATE TABLE IF NOT EXISTS shares (
  share_id          BIGINT UNSIGNED NOT NULL,
  role              ENUM('granted','received') NOT NULL,
  name              VARCHAR(64)     NOT NULL,
  object_id         BIGINT UNSIGNED NOT NULL,
  from_space_id     BIGINT UNSIGNED NOT NULL,
  peer_tenant_id    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  to_space_id       BIGINT UNSIGNED NOT NULL DEFAULT 0,
  filter_sql        TEXT            NULL,
  filter_mongo      JSON            NULL,
  tag               VARCHAR(64)     NULL,
  columns           JSON            NULL,
  take_filter_sql   TEXT            NULL,
  reshare           BOOLEAN         NOT NULL DEFAULT FALSE,
  merge_mode        ENUM('read_through','materialized','federated') NOT NULL,
  copy_state        ENUM('none','seeding','following','serving','stale') NOT NULL
                    DEFAULT 'none',
  share_role_id     BIGINT UNSIGNED NULL,
  version           BIGINT UNSIGNED NOT NULL,
  state             ENUM('creating','active','dropping') NOT NULL,
  created_by        INT UNSIGNED    NOT NULL,
  created_at        DATETIME(6)     NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (share_id, role),
  KEY k_from (object_id, from_space_id)
) ENGINE=InnoDB";

/// `space_homes`: users' home spaces (chapter 14's `home_space_id`).
pub const DDL_HOMES: &str = "CREATE TABLE IF NOT EXISTS space_homes (
  user_id           INT UNSIGNED    NOT NULL,
  space_id          BIGINT UNSIGNED NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (user_id)
) ENGINE=InnoDB";

/// `inherit_ext` (format 2, Kiran's decisions of 11 November 2026): per
/// object, the `AUTO_INCREMENT` column, the foreign keys the gateway keeps
/// and whether SprigDB created the object from its parent tenant's, as
/// JSON ([`crate::model::ObjectExt`]).
pub const DDL_EXT: &str = "CREATE TABLE IF NOT EXISTS inherit_ext (
  object_id         BIGINT UNSIGNED NOT NULL,
  ext               JSON            NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (object_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static INHERIT_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "spaces, inheritable_objects, shares and space_homes",
        statements: &[DDL_SPACES, DDL_OBJECTS, DDL_SHARES, DDL_HOMES],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "inherit_ext: AUTO_INCREMENT columns, foreign keys, adopted objects",
        statements: &[DDL_EXT],
    },
];

/// The `inherit` module, for [`Migrator::new`].
pub const INHERIT_MODULE: Module = Module {
    name: MODULE,
    migrations: INHERIT_MIGRATIONS,
};

/// Applies the module's migrations to catalog `schema` (idempotent).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut Conn, schema: &str) -> Result<()> {
    Migrator::new(&[INHERIT_MODULE])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            1062 => SprigError::new(SprigCode::SpaceExists, "the name is taken"),
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
        .ok_or_else(|| SprigError::internal(format!("inherit catalog: bad column {i}")))
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("inherit catalog: {what}"))
}

fn json<T: serde::de::DeserializeOwned>(s: &str, what: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|_| bad(what))
}

/// A space to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSpace {
    /// The name.
    pub name: String,
    /// The parent space.
    pub parent: SpaceId,
    /// The creating user.
    pub created_by: u32,
}

/// What one extension statement writes, committed in one versioned change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InheritChange {
    /// Make sure the root space has a row, with this name.
    pub root_name: Option<String>,
    /// Spaces created (ids allocated on commit).
    pub add_spaces: Vec<NewSpace>,
    /// Spaces removed.
    pub drop_spaces: Vec<SpaceId>,
    /// Objects made inheritable or changed.
    pub upsert_objects: Vec<InheritableObject>,
    /// Objects no longer inheritable (or dropped).
    pub delete_objects: Vec<ObjectId>,
    /// Shares created (`id` 0: allocated on commit) or replaced.
    pub upsert_shares: Vec<Share>,
    /// Shares dropped.
    pub drop_shares: Vec<u64>,
    /// Narrowings of received shares set or replaced.
    pub upsert_takes: Vec<Take>,
    /// Home spaces set (`None`: back to the root).
    pub homes: Vec<(u32, Option<SpaceId>)>,
}

impl InheritChange {
    /// Whether nothing is written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Reads and writes the module's tables in one catalog schema.
#[derive(Debug, Clone)]
pub struct InheritStore {
    schema: String,
}

impl InheritStore {
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
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'space_homes'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Whether format 2's `inherit_ext` exists.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn ext_ready<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'inherit_ext'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// The whole state.
    ///
    /// # Errors
    /// 39070, a backend error, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<InheritState> {
        let ext_ready = self.ext_ready(q).await?;
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT space_id, name, parent_space_id, depth FROM {} \
                 WHERE state = 'active' ORDER BY space_id",
                self.t("spaces")
            ))
            .await
            .map_err(map_err)?;
        let mut spaces = Vec::with_capacity(rows.len());
        for r in &rows {
            spaces.push(Space {
                id: SpaceId(col(r, 0)?),
                name: col(r, 1)?,
                parent: col::<Option<u64>>(r, 2)?.map(SpaceId),
                depth: col(r, 3)?,
            });
        }
        let ext_sql = if ext_ready {
            format!(
                "(SELECT CAST(e.ext AS CHAR) FROM {} e WHERE e.object_id = i.object_id)",
                self.t("inherit_ext")
            )
        } else {
            "NULL".to_owned()
        };
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT i.object_id, o.db_id, o.name, CAST(i.key_fields AS CHAR), \
                 CAST(i.field_slots AS CHAR), CAST(i.columns AS CHAR), \
                 CAST(i.unique_keys AS CHAR), i.tag_field, i.version, {ext_sql} \
                 FROM {} i JOIN {} o ON o.object_id = i.object_id \
                 WHERE o.state <> 'dropped' ORDER BY i.object_id",
                self.t("inheritable_objects"),
                self.t("objects")
            ))
            .await
            .map_err(map_err)?;
        let mut objects = Vec::with_capacity(rows.len());
        let mut missing = Vec::new();
        for r in &rows {
            let key: String = col(r, 3)?;
            let slots: String = col(r, 4)?;
            let columns: String = col(r, 5)?;
            let uniques: String = col(r, 6)?;
            let ext: Option<String> = col(r, 9)?;
            let ext: ObjectExt = if let Some(e) = ext {
                json(&e, "inherit_ext")?
            } else {
                missing.push(ObjectId(col(r, 0)?));
                ObjectExt::default()
            };
            objects.push(InheritableObject {
                object_id: ObjectId(col(r, 0)?),
                db: DbId(col(r, 1)?),
                table: col(r, 2)?,
                key: json(&key, "key_fields")?,
                slots: json::<BTreeMap<String, u8>>(&slots, "field_slots")?,
                columns: json(&columns, "columns")?,
                uniques: json::<Vec<UniqueKey>>(&uniques, "unique_keys")?,
                tag_field: col(r, 7)?,
                version: col(r, 8)?,
                auto: ext.auto,
                links: ext.links,
                adopted: ext.adopted,
                type_root: ext.type_root,
            });
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT share_id, role, name, object_id, from_space_id, peer_tenant_id, \
                 to_space_id, filter_sql, CAST(columns AS CHAR), take_filter_sql, reshare, \
                 version, created_by, CAST(merge_mode AS CHAR) FROM {} \
                 WHERE state = 'active' ORDER BY share_id",
                self.t("shares")
            ))
            .await
            .map_err(map_err)?;
        let mut shares = Vec::new();
        let mut takes = Vec::new();
        for r in &rows {
            let role: String = col(r, 1)?;
            let peer: u64 = col(r, 5)?;
            let to: u64 = col(r, 6)?;
            if role == "received" {
                takes.push(Take {
                    share_id: col(r, 0)?,
                    peer_tenant: TenantId(peer),
                    object_id: ObjectId(col(r, 3)?),
                    take: col(r, 9)?,
                });
                continue;
            }
            let columns: Option<String> = col(r, 8)?;
            shares.push(Share {
                id: col(r, 0)?,
                name: col(r, 2)?,
                object_id: ObjectId(col(r, 3)?),
                from: SpaceId(col(r, 4)?),
                to: match (peer, to) {
                    (0, 0) => ShareTarget::AllChildren,
                    (0, s) => ShareTarget::Space(SpaceId(s)),
                    (t, _) => ShareTarget::Tenant(TenantId(t)),
                },
                filter: col(r, 7)?,
                columns: match columns {
                    Some(c) => Some(json(&c, "columns")?),
                    None => None,
                },
                take: col(r, 9)?,
                reshare: col::<i64>(r, 10)? != 0,
                version: col(r, 11)?,
                created_by: col(r, 12)?,
                mode: MergeMode::parse(&col::<String>(r, 13)?).unwrap_or_default(),
                via: None,
            });
        }
        let homes: Vec<(u32, u64)> = q
            .query(format!(
                "SELECT user_id, space_id FROM {}",
                self.t("space_homes")
            ))
            .await
            .map_err(map_err)?;
        let mut st = InheritState::new(
            spaces,
            objects,
            shares,
            takes,
            homes.into_iter().map(|(u, s)| (u, SpaceId(s))).collect(),
        );
        st.ext_ready = ext_ready;
        st.set_ext_missing(missing);
        Ok(st)
    }

    async fn next_id<Q: Queryable + Send>(&self, q: &mut Q, table: &str, id: &str) -> Result<u64> {
        let n: Option<u64> = q
            .query_first(format!(
                "SELECT COALESCE(MAX({id}), 1) + 1 FROM {} FOR UPDATE",
                self.t(table)
            ))
            .await
            .map_err(map_err)?;
        n.ok_or_else(|| bad("no id"))
    }

    /// Applies `change` in `q` (the versioned change's transaction) at
    /// catalog version `version`.
    ///
    /// # Errors
    /// 39881 for a name taken; 39070 or a backend error.
    #[allow(clippy::too_many_lines)] // one block per kind of row
    pub async fn apply<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        change: &InheritChange,
        version: u64,
    ) -> Result<()> {
        if let Some(name) = &change.root_name {
            q.exec_drop(
                format!(
                    "INSERT IGNORE INTO {} (space_id, name, parent_space_id, depth, path, state, \
                     created_by, created_at, changed_version) VALUES (1, ?, NULL, 0, '/1/', \
                     'active', 0, UTC_TIMESTAMP(6), ?)",
                    self.t("spaces")
                ),
                (name.as_str(), version),
            )
            .await
            .map_err(map_err)?;
        }
        for s in &change.add_spaces {
            let id = self.next_id(q, "spaces", "space_id").await?;
            let parent: Option<(u16, Vec<u8>)> = q
                .exec_first(
                    format!(
                        "SELECT depth, path FROM {} WHERE space_id = ? AND state = 'active'",
                        self.t("spaces")
                    ),
                    (s.parent.0,),
                )
                .await
                .map_err(map_err)?;
            let (depth, path) = parent.unwrap_or((0, b"/1/".to_vec()));
            let mut path = String::from_utf8_lossy(&path).into_owned();
            path.push_str(&format!("{id}/"));
            q.exec_drop(
                format!(
                    "INSERT INTO {} (space_id, name, parent_space_id, depth, path, state, \
                     created_by, created_at, changed_version) VALUES (?, ?, ?, ?, ?, 'active', ?, \
                     UTC_TIMESTAMP(6), ?)",
                    self.t("spaces")
                ),
                (
                    id,
                    s.name.as_str(),
                    s.parent.0,
                    depth.saturating_add(1),
                    path,
                    s.created_by,
                    version,
                ),
            )
            .await
            .map_err(map_err)?;
        }
        for s in &change.drop_spaces {
            for (table, col) in [("spaces", "space_id"), ("space_homes", "space_id")] {
                q.exec_drop(
                    format!("DELETE FROM {} WHERE {col} = ?", self.t(table)),
                    (s.0,),
                )
                .await
                .map_err(map_err)?;
            }
            q.exec_drop(
                format!(
                    "DELETE FROM {} WHERE role = 'granted' AND (from_space_id = ? OR \
                     (peer_tenant_id = 0 AND to_space_id = ?))",
                    self.t("shares")
                ),
                (s.0, s.0),
            )
            .await
            .map_err(map_err)?;
        }
        for id in &change.delete_objects {
            q.exec_drop(
                format!(
                    "DELETE FROM {} WHERE object_id = ?",
                    self.t("inheritable_objects")
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
            q.exec_drop(
                format!("DELETE FROM {} WHERE object_id = ?", self.t("inherit_ext")),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
            q.exec_drop(
                format!("DELETE FROM {} WHERE object_id = ?", self.t("shares")),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        }
        for o in &change.upsert_objects {
            let enc = |v: &dyn erased::Ser| v.json();
            q.exec_drop(
                format!(
                    "INSERT INTO {} (object_id, model, key_fields, field_slots, columns, \
                     unique_keys, tag_field, state, version, created_at, changed_version) \
                     VALUES (?, 'table', ?, ?, ?, ?, ?, 'active', 1, UTC_TIMESTAMP(6), ?) \
                     ON DUPLICATE KEY UPDATE key_fields = VALUES(key_fields), \
                     field_slots = VALUES(field_slots), columns = VALUES(columns), \
                     unique_keys = VALUES(unique_keys), tag_field = VALUES(tag_field), \
                     version = version + 1, changed_version = VALUES(changed_version)",
                    self.t("inheritable_objects")
                ),
                (
                    o.object_id.0,
                    enc(&o.key),
                    enc(&o.slots),
                    enc(&o.columns),
                    enc(&o.uniques),
                    o.tag_field.as_deref(),
                    version,
                ),
            )
            .await
            .map_err(map_err)?;
            q.exec_drop(
                format!(
                    "INSERT INTO {} (object_id, ext, changed_version) VALUES (?, ?, ?) \
                     ON DUPLICATE KEY UPDATE ext = VALUES(ext), \
                     changed_version = VALUES(changed_version)",
                    self.t("inherit_ext")
                ),
                (o.object_id.0, enc(&o.ext()), version),
            )
            .await
            .map_err(map_err)?;
        }
        for id in &change.drop_shares {
            q.exec_drop(
                format!(
                    "DELETE FROM {} WHERE share_id = ? AND role = 'granted'",
                    self.t("shares")
                ),
                (id,),
            )
            .await
            .map_err(map_err)?;
        }
        for s in &change.upsert_shares {
            let id = if s.id == 0 {
                self.next_id(q, "shares", "share_id").await?
            } else {
                s.id
            };
            let (peer, to) = match s.to {
                ShareTarget::AllChildren => (0, 0),
                ShareTarget::Space(c) => (0, c.0),
                ShareTarget::Tenant(t) => (t.0, 0),
            };
            let columns = s
                .columns
                .as_ref()
                .map(|c| serde_json::to_string(c).unwrap_or_else(|_| "[]".into()));
            q.exec_drop(
                format!(
                    "INSERT INTO {} (share_id, role, name, object_id, from_space_id, \
                     peer_tenant_id, to_space_id, filter_sql, columns, take_filter_sql, reshare, \
                     merge_mode, version, state, created_by, created_at, changed_version) \
                     VALUES (?, 'granted', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', \
                     ?, UTC_TIMESTAMP(6), ?) ON DUPLICATE KEY UPDATE \
                     filter_sql = VALUES(filter_sql), columns = VALUES(columns), \
                     merge_mode = VALUES(merge_mode), \
                     take_filter_sql = VALUES(take_filter_sql), reshare = VALUES(reshare), \
                     version = VALUES(version), changed_version = VALUES(changed_version)",
                    self.t("shares")
                ),
                mysql_async::Params::Positional(vec![
                    id.into(),
                    s.name.as_str().into(),
                    s.object_id.0.into(),
                    s.from.0.into(),
                    peer.into(),
                    to.into(),
                    s.filter.as_deref().into(),
                    columns.into(),
                    s.take.as_deref().into(),
                    s.reshare.into(),
                    s.mode.as_str().into(),
                    s.version.into(),
                    s.created_by.into(),
                    version.into(),
                ]),
            )
            .await
            .map_err(|e| match &e {
                mysql_async::Error::Server(x) if x.code == 1062 => SprigError::new(
                    SprigCode::SpaceExists,
                    format!("a share named '{}' exists", s.name),
                ),
                _ => map_err(e),
            })?;
        }
        for t in &change.upsert_takes {
            q.exec_drop(
                format!(
                    "INSERT INTO {} (share_id, role, name, object_id, from_space_id, \
                     peer_tenant_id, to_space_id, take_filter_sql, merge_mode, version, state, \
                     created_by, created_at, changed_version) VALUES (?, 'received', '', ?, 0, ?, \
                     ?, ?, 'read_through', 1, 'active', 0, UTC_TIMESTAMP(6), ?) \
                     ON DUPLICATE KEY UPDATE take_filter_sql = VALUES(take_filter_sql), \
                     version = version + 1, changed_version = VALUES(changed_version)",
                    self.t("shares")
                ),
                (
                    t.share_id,
                    t.object_id.0,
                    t.peer_tenant.0,
                    ROOT_SPACE.0,
                    t.take.as_deref(),
                    version,
                ),
            )
            .await
            .map_err(map_err)?;
        }
        for (user, space) in &change.homes {
            match space {
                Some(s) => q
                    .exec_drop(
                        format!(
                            "INSERT INTO {} (user_id, space_id, changed_version) VALUES (?, ?, ?) \
                             ON DUPLICATE KEY UPDATE space_id = VALUES(space_id), \
                             changed_version = VALUES(changed_version)",
                            self.t("space_homes")
                        ),
                        (user, s.0, version),
                    )
                    .await
                    .map_err(map_err)?,
                None => q
                    .exec_drop(
                        format!("DELETE FROM {} WHERE user_id = ?", self.t("space_homes")),
                        (user,),
                    )
                    .await
                    .map_err(map_err)?,
            }
        }
        Ok(())
    }
}

mod erased {
    //! JSON of the few shapes the module stores.
    pub(super) trait Ser {
        fn json(&self) -> String;
    }
    impl<T: serde::Serialize> Ser for T {
        fn json(&self) -> String {
            serde_json::to_string(self).unwrap_or_else(|_| "null".into())
        }
    }
}

impl ExtState for InheritState {
    fn bytes(&self) -> usize {
        self.spaces().count() * 128
            + self
                .objects()
                .map(|o| 256 + o.columns.len() * 64)
                .sum::<usize>()
            + self.shares().count() * 256
    }
}

/// The module in the catalog cache: [`InheritState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct InheritExtension;

#[async_trait::async_trait]
impl CatalogExtension for InheritExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        INHERIT_MIGRATIONS
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
        let store = InheritStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(InheritState::unmigrated()));
        }
        Ok(Arc::new(store.load(conn).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        Migrator::new(&[INHERIT_MODULE]).unwrap();
        assert_eq!(INHERIT_MODULE.newest(), 2);
        for m in INHERIT_MIGRATIONS {
            for ddl in m.statements {
                assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS "));
            }
        }
    }
}
