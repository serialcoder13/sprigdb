//! Table groups, members and co-location (chapter 15, "Table groups",
//! "Catalog tables", "Co-location safety"), and the catalog module that
//! owns chapter 15's and chapter 19's tables.

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use serde_json::Value;
use sprig_catalog::migrate::{Migration, Module, Phase};
use sprig_common::ids::{DbId, GroupId, ObjectId};
use sprig_common::{Result, SprigError};

use crate::collation::{self, CollationId, Pad};
use crate::key::{GroupKind, KeyClass, KeySpec, MemberRouting};

/// The catalog module's name.
pub const MODULE: &str = "sharding";

/// `table_groups` (chapter 15).
pub const DDL_TABLE_GROUPS: &str = "CREATE TABLE IF NOT EXISTS table_groups (
  group_id         INT UNSIGNED NOT NULL,
  kind             ENUM('default','sharded','lookup','reference') NOT NULL,
  name             VARCHAR(64) NOT NULL,
  key_spec         JSON NULL,
  key_spec_digest  BIGINT UNSIGNED NULL,
  state            ENUM('active','preparing','rekeying','retired') NOT NULL,
  min_shards       INT UNSIGNED NULL,
  origin           ENUM('default','structural','advisor','pinned') NOT NULL,
  advisor_run_id   BIGINT UNSIGNED NULL,
  created_at       DATETIME(6) NOT NULL,
  updated_at       DATETIME(6) NOT NULL,
  row_version      BIGINT UNSIGNED NOT NULL DEFAULT 1,
  PRIMARY KEY (group_id),
  UNIQUE KEY uk_name (name)
) ENGINE=InnoDB";

/// `group_members` (chapter 15).
pub const DDL_GROUP_MEMBERS: &str = "CREATE TABLE IF NOT EXISTS group_members (
  object_id        BIGINT UNSIGNED NOT NULL,
  group_id         INT UNSIGNED NOT NULL,
  db_id            INT UNSIGNED NOT NULL,
  routing          ENUM('columns','parent','lookup','none') NOT NULL,
  key_columns      JSON NULL,
  parent_object_id BIGINT UNSIGNED NULL,
  parent_fk        VARCHAR(64) NULL,
  member_key_spec  JSON NULL,
  rk_state         ENUM('absent','filling','backfilling','ready') NOT NULL,
  off_class_seen   TINYINT(1) NOT NULL DEFAULT 0,
  pinned           TINYINT(1) NOT NULL DEFAULT 0,
  next_routing     JSON NULL,
  joined_at        DATETIME(6) NOT NULL,
  row_version      BIGINT UNSIGNED NOT NULL DEFAULT 1,
  PRIMARY KEY (object_id),
  KEY by_group (group_id, db_id)
) ENGINE=InnoDB";

/// `reference_tables` (chapter 15).
pub const DDL_REFERENCE_TABLES: &str = "CREATE TABLE IF NOT EXISTS reference_tables (
  object_id        BIGINT UNSIGNED NOT NULL,
  cluster_id       VARCHAR(64) NOT NULL,
  role             ENUM('primary','copy') NOT NULL,
  state            ENUM('seeding','following','serving','stale','dropping') NOT NULL,
  job_id           BINARY(16) NULL,
  origin           ENUM('advisor','pinned') NOT NULL,
  updated_at       DATETIME(6) NOT NULL,
  PRIMARY KEY (object_id, cluster_id),
  KEY by_cluster (cluster_id)
) ENGINE=InnoDB";

/// The module's migrations: chapter 15's tables, then chapter 19's, then
/// the pins of chapter 16.
pub static SHARDING_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "table_groups, group_members and reference_tables",
        statements: &[DDL_TABLE_GROUPS, DDL_GROUP_MEMBERS, DDL_REFERENCE_TABLES],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "sequences, lookup_indexes and foreign_keys (chapter 19)",
        statements: &[
            crate::ids::DDL_SEQUENCES,
            crate::lookup::DDL_LOOKUP_INDEXES,
            crate::lookup::DDL_FOREIGN_KEYS,
        ],
    },
    Migration {
        format: 3,
        phase: Phase::Expand,
        name: "group_pins (chapter 16's SPRIG PIN)",
        statements: &[crate::pins::DDL_GROUP_PINS],
    },
    Migration {
        format: 4,
        phase: Phase::Expand,
        name: "sequence_blocks (chapter 19's leased ID blocks)",
        statements: &[crate::ids::DDL_SEQUENCE_BLOCKS],
    },
];

/// The `sharding` module, for `sprig_catalog::Migrator::new`.
pub const SHARDING_MODULE: Module = Module {
    name: MODULE,
    migrations: SHARDING_MIGRATIONS,
};

/// Applies the `sharding` module's migrations to the catalog `schema`
/// (idempotent, under the migrator's lock). Callers run it when a catalog
/// bootstrapped before the module meets its first table group (1146).
///
/// # Errors
/// The migrator's errors.
pub async fn migrate(c: &mut Conn, schema: &str) -> Result<()> {
    sprig_catalog::Migrator::new(&[SHARDING_MODULE])?
        .run(c, schema, &sprig_catalog::migrate::MigrateOpts::default())
        .await?;
    Ok(())
}

/// A group's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// Serving; only active groups split.
    Active,
    /// Members back-filling, lookups building.
    Preparing,
    /// An online re-key (R3).
    Rekeying,
    /// Its members left.
    Retired,
}

impl GroupState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Preparing => "preparing",
            Self::Rekeying => "rekeying",
            Self::Retired => "retired",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "active" => Self::Active,
            "preparing" => Self::Preparing,
            "rekeying" => Self::Rekeying,
            "retired" => Self::Retired,
            _ => return None,
        })
    }
}

/// A member's `_sprig_rk` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RkState {
    /// No column yet.
    Absent,
    /// Gateways fill it on every write.
    Filling,
    /// The `backfill_rk` job runs.
    Backfilling,
    /// No `NULL` left; sampled rows re-hash correctly.
    Ready,
}

impl RkState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Filling => "filling",
            Self::Backfilling => "backfilling",
            Self::Ready => "ready",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "absent" => Self::Absent,
            "filling" => Self::Filling,
            "backfilling" => Self::Backfilling,
            "ready" => Self::Ready,
            _ => return None,
        })
    }
}

/// A `table_groups` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableGroup {
    /// The id, never reused.
    pub id: GroupId,
    /// Its kind.
    pub kind: GroupKind,
    /// Display name.
    pub name: String,
    /// The key (sharded groups).
    pub key: Option<KeySpec>,
    /// Its state.
    pub state: GroupState,
    /// Overrides `sharding.min_shards`.
    pub min_shards: Option<u32>,
}

/// A `group_members` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    /// The table, collection or namespace.
    pub object: ObjectId,
    /// Its group.
    pub group: GroupId,
    /// Its database.
    pub db: DbId,
    /// How it supplies its key.
    pub routing: MemberRouting,
    /// The `_sprig_rk` state.
    pub rk_state: RkState,
    /// An off-class value was written.
    pub off_class_seen: bool,
}

/// The comparison type MySQL picks for `a = b` (chapter 17 resolves it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpType {
    /// Exact integer comparison.
    Integer,
    /// Exact decimal comparison.
    Decimal,
    /// Double precision: never co-location-safe.
    Double,
    /// Strings under this collation.
    String(CollationId),
    /// Bytes.
    Binary,
    /// UUIDs.
    Uuid,
    /// `DATE`/`DATETIME` against each other.
    DateTime,
    /// `TIMESTAMP` against each other.
    Timestamp,
}

/// One equality the router wants to run inside a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Comparison {
    /// The comparison type.
    pub cmp: CmpType,
    /// The key part side `a` is, for its object (through a parent chain).
    pub part_a: Option<usize>,
    /// The key part side `b` is.
    pub part_b: Option<usize>,
    /// SQL rules (MySQL comparisons), as opposed to MongoDB's.
    pub sql_rules: bool,
}

/// Whether `cmp` between members `a` and `b` of group `g` holds only for
/// rows on the same shard (chapter 15, `colocation_safe`).
#[must_use]
pub fn colocation_safe(cmp: &Comparison, g: &TableGroup, a: &GroupMember, b: &GroupMember) -> bool {
    if a.group != g.id || b.group != g.id {
        return false;
    }
    let (Some(pa), Some(pb)) = (cmp.part_a, cmp.part_b) else {
        return false;
    };
    if pa != pb {
        return false;
    }
    let Some(class) = g.key.as_ref().and_then(|k| k.parts.get(pa)) else {
        return false;
    };
    if cmp.sql_rules && (a.off_class_seen || b.off_class_seen) {
        return false;
    }
    match (*class, cmp.cmp) {
        (KeyClass::Numeric, CmpType::Integer | CmpType::Decimal)
        | (KeyClass::Binary, CmpType::Binary)
        | (KeyClass::Uuid, CmpType::Uuid)
        | (KeyClass::DateTime, CmpType::DateTime)
        | (KeyClass::Timestamp, CmpType::Timestamp) => true,
        (KeyClass::String { collation: gc }, CmpType::String(cc)) => finer_or_equal(cc, gc),
        _ => false,
    }
}

/// Whether equality under `c` implies equality under `g`: the same
/// collation, or a `_bin` collation of the same character set that pads no
/// more than `g` does.
pub(crate) fn finer_or_equal(c: CollationId, g: CollationId) -> bool {
    let (Some(c), Some(g)) = (collation::collation(c), collation::collation(g)) else {
        return false;
    };
    if c.id == g.id {
        return true;
    }
    c.is_binary() && c.charset == g.charset && (c.pad == Pad::NoPad || g.pad == Pad::Space)
}

/// Reads and writes `table_groups` and `group_members` in a tenant's
/// catalog (the connection's default database).
#[derive(Debug, Clone, Copy, Default)]
pub struct GroupStore;

impl GroupStore {
    /// Inserts a group.
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn create_group(&self, c: &mut Conn, g: &TableGroup) -> Result<()> {
        c.exec_drop(
            "INSERT INTO table_groups (group_id, kind, name, key_spec, key_spec_digest, state,
               min_shards, origin, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'pinned', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))",
            (
                g.id.0,
                g.kind.as_str(),
                &g.name,
                g.key.as_ref().map(|k| k.to_json().to_string()),
                g.key.as_ref().map(|k| k.digest),
                g.state.as_str(),
                g.min_shards,
            ),
        )
        .await
        .map_err(db_err)
    }

    /// Inserts a member.
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn add_member(&self, c: &mut Conn, m: &GroupMember) -> Result<()> {
        let (routing, cols, parent, fk) = match &m.routing {
            MemberRouting::Columns(cols) => (
                "columns",
                Some(serde_json::to_string(cols).unwrap_or_default()),
                None,
                None,
            ),
            MemberRouting::Parent { parent, fk } => {
                ("parent", None, Some(parent.0), Some(fk.clone()))
            }
            MemberRouting::Lookup => ("lookup", None, None, None),
            MemberRouting::None => ("none", None, None, None),
        };
        c.exec_drop(
            "INSERT INTO group_members (object_id, group_id, db_id, routing, key_columns,
               parent_object_id, parent_fk, rk_state, off_class_seen, joined_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))",
            (
                m.object.0,
                m.group.0,
                m.db.0,
                routing,
                cols,
                parent,
                fk,
                m.rk_state.as_str(),
                m.off_class_seen,
            ),
        )
        .await
        .map_err(db_err)
    }

    /// Sets `off_class_seen` (once; the router stops trusting SQL
    /// co-location for the member).
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn mark_off_class(&self, c: &mut Conn, object: ObjectId) -> Result<()> {
        c.exec_drop(
            "UPDATE group_members SET off_class_seen = 1, row_version = row_version + 1
             WHERE object_id = ? AND off_class_seen = 0",
            (object.0,),
        )
        .await
        .map_err(db_err)
    }

    /// Every group.
    ///
    /// # Errors
    /// MariaDB's errors; `Internal` for rows that don't parse.
    pub async fn groups(&self, c: &mut Conn) -> Result<Vec<TableGroup>> {
        let rows: Vec<Row> = c
            .query(
                "SELECT group_id, kind, name, CAST(key_spec AS CHAR), state, min_shards
                 FROM table_groups ORDER BY group_id",
            )
            .await
            .map_err(db_err)?;
        rows.into_iter().map(group_from_row).collect()
    }

    /// Every member of group `g`.
    ///
    /// # Errors
    /// MariaDB's errors; `Internal` for rows that don't parse.
    pub async fn members(&self, c: &mut Conn, g: GroupId) -> Result<Vec<GroupMember>> {
        let rows: Vec<Row> = c
            .exec(
                "SELECT object_id, group_id, db_id, routing, CAST(key_columns AS CHAR),
                        parent_object_id, parent_fk, rk_state, off_class_seen
                 FROM group_members WHERE group_id = ? ORDER BY object_id",
                (g.0,),
            )
            .await
            .map_err(db_err)?;
        rows.into_iter().map(member_from_row).collect()
    }
}

fn bad_row(what: &str) -> SprigError {
    SprigError::internal(format!("unreadable {what} row"))
}

fn group_from_row(mut r: Row) -> Result<TableGroup> {
    let id: u32 = r.take(0).ok_or_else(|| bad_row("table_groups"))?;
    let kind: String = r.take(1).ok_or_else(|| bad_row("table_groups"))?;
    let name: String = r.take(2).ok_or_else(|| bad_row("table_groups"))?;
    let spec: Option<String> = r.take(3).ok_or_else(|| bad_row("table_groups"))?;
    let state: String = r.take(4).ok_or_else(|| bad_row("table_groups"))?;
    let min_shards: Option<u32> = r.take(5).ok_or_else(|| bad_row("table_groups"))?;
    let key = match spec {
        Some(s) => {
            let v: Value =
                serde_json::from_str(&s).map_err(|_| bad_row("table_groups.key_spec"))?;
            Some(KeySpec::from_json(&v)?)
        }
        None => None,
    };
    Ok(TableGroup {
        id: GroupId(id),
        kind: GroupKind::parse(&kind).ok_or_else(|| bad_row("table_groups.kind"))?,
        name,
        key,
        state: GroupState::parse(&state).ok_or_else(|| bad_row("table_groups.state"))?,
        min_shards,
    })
}

fn member_from_row(mut r: Row) -> Result<GroupMember> {
    let e = || bad_row("group_members");
    let object: u64 = r.take(0).ok_or_else(e)?;
    let group: u32 = r.take(1).ok_or_else(e)?;
    let db: u32 = r.take(2).ok_or_else(e)?;
    let routing: String = r.take(3).ok_or_else(e)?;
    let cols: Option<String> = r.take(4).ok_or_else(e)?;
    let parent: Option<u64> = r.take(5).ok_or_else(e)?;
    let fk: Option<String> = r.take(6).ok_or_else(e)?;
    let rk: String = r.take(7).ok_or_else(e)?;
    let off: bool = r.take(8).ok_or_else(e)?;
    let routing = match routing.as_str() {
        "columns" => MemberRouting::Columns(
            serde_json::from_str(cols.as_deref().unwrap_or("[]")).map_err(|_| e())?,
        ),
        "parent" => MemberRouting::Parent {
            parent: ObjectId(parent.ok_or_else(e)?),
            fk: fk.unwrap_or_default(),
        },
        "lookup" => MemberRouting::Lookup,
        _ => MemberRouting::None,
    };
    Ok(GroupMember {
        object: ObjectId(object),
        group: GroupId(group),
        db: DbId(db),
        routing,
        rk_state: RkState::parse(&rk).ok_or_else(e)?,
        off_class_seen: off,
    })
}

/// Maps a MariaDB error to a backend error.
pub(crate) fn db_err(e: mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => {
            let mut st = [b'H', b'Y', b'0', b'0', b'0'];
            for (i, b) in s.state.bytes().take(5).enumerate() {
                st[i] = b;
            }
            SprigError::backend(s.code, st, s.message)
        }
        other => SprigError::new(
            sprig_common::SprigCode::BackendUnavailable,
            format!("MariaDB: {other}"),
        ),
    }
}

#[cfg(test)]
mod tests;
