//! Sharding pins (chapter 16, "Interfaces"; build step 8): the
//! `group_pins` catalog table, the conflict rules (39410) and the R1 rule
//! that a split group's tables keep their key (39411).
//!
//! A pin fixes one table's place in every design the advisor makes:
//!
//! | Pin | The table |
//! |---|---|
//! | `SHARD KEY (cols)` | routes by exactly `cols`: a column member on them, the root of a group keyed by them when no parent's group has that key |
//! | `COLOCATE WITH p ON (cols)` | is a column member of `p`'s group, its `cols` holding the group's key |
//! | `REFERENCE` | is a reference table; in R1 it stays unsharded (no reference copies yet) |
//! | `UNSHARDED` | stays in its database's default group |
//!
//! Pins can be set before the first design exists, so they have their own
//! table rather than living only on `group_members` rows; once a pinned
//! table is a member placed as its pin says, `group_members.pinned` is 1
//! too, and a group whose root is pinned by `SHARD KEY` has `origin =
//! 'pinned'` (chapter 15).
//!
//! Conflict rules ([`check_pin`]), 39410 `ER_SPRIG_PIN_CONFLICT`:
//!
//! 1. Every named column exists (else 1072, as MariaDB words a missing key
//!    column), appears once, and has a type and collation a key can use.
//! 2. A table can't be co-located with itself, with a table pinned
//!    `UNSHARDED` or `REFERENCE`, or around a cycle of `COLOCATE` pins.
//! 3. A `COLOCATE` table's columns match the key of the group it joins, in
//!    number and class: the chain's root `SHARD KEY` pin, else the root's
//!    current group key; unknown before either exists.
//! 4. A table that other tables are pinned `COLOCATE WITH` can't be pinned
//!    `UNSHARDED` or `REFERENCE`, and a new `SHARD KEY` must match their
//!    columns (rule 3 again).
//!
//! 39411 `ER_SPRIG_PIN_NEEDS_REKEY`: in R1, a table in a group with more than
//! one shard takes only a pin that matches its current place; anything else
//! needs the online re-key (R3).

use std::collections::{BTreeMap, BTreeSet};

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{DbId, GroupId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::collation::{self, collation_by_name};
use crate::groups::db_err;
use crate::key::{KeyClass, KeySpec, MemberRouting};

/// `group_pins` (chapter 15's `sharding` module, format 3): one row per
/// pinned table.
pub const DDL_GROUP_PINS: &str = "CREATE TABLE IF NOT EXISTS group_pins (
  object_id        BIGINT UNSIGNED NOT NULL,
  db_id            INT UNSIGNED NOT NULL,
  kind             ENUM('shard_key','colocate','reference','unsharded') NOT NULL,
  key_columns      JSON NULL,
  parent_object_id BIGINT UNSIGNED NULL,
  pinned_by        VARCHAR(128) NULL,
  pinned_at        DATETIME(6) NOT NULL,
  row_version      BIGINT UNSIGNED NOT NULL DEFAULT 1,
  PRIMARY KEY (object_id),
  KEY by_parent (parent_object_id)
) ENGINE=InnoDB";

/// What a pin fixes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinKind {
    /// `SHARD KEY (columns)`.
    ShardKey {
        /// The routing columns, lower case, in key order.
        columns: Vec<String>,
    },
    /// `COLOCATE WITH parent ON (columns)`.
    Colocate {
        /// The table whose group it joins.
        parent: ObjectId,
        /// Its columns holding the key, lower case, in key order.
        columns: Vec<String>,
    },
    /// `REFERENCE` (unsharded in R1).
    Reference,
    /// `UNSHARDED`.
    Unsharded,
}

impl PinKind {
    /// The `group_pins.kind` text.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ShardKey { .. } => "shard_key",
            Self::Colocate { .. } => "colocate",
            Self::Reference => "reference",
            Self::Unsharded => "unsharded",
        }
    }

    /// The pin's columns (empty for `REFERENCE` and `UNSHARDED`).
    #[must_use]
    pub fn columns(&self) -> &[String] {
        match self {
            Self::ShardKey { columns } | Self::Colocate { columns, .. } => columns,
            Self::Reference | Self::Unsharded => &[],
        }
    }

    /// Whether the table stays out of sharded groups.
    #[must_use]
    pub fn keeps_unsharded(&self) -> bool {
        matches!(self, Self::Reference | Self::Unsharded)
    }
}

/// One pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pin {
    /// The pinned table.
    pub object: ObjectId,
    /// Its database.
    pub db: DbId,
    /// What it fixes.
    pub kind: PinKind,
}

/// A `group_pins` row with who set it and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinRow {
    /// The pin.
    pub pin: Pin,
    /// The user who set it.
    pub by: Option<String>,
    /// When, `YYYY-MM-DD hh:mm:ss.ffffff` UTC.
    pub at: String,
}

/// A table's current place in a sharded group.
#[derive(Debug, Clone, PartialEq)]
pub struct Placed {
    /// The table.
    pub object: ObjectId,
    /// Its group.
    pub group: GroupId,
    /// How it routes.
    pub routing: MemberRouting,
    /// The group's key.
    pub key: Option<KeySpec>,
    /// Shards in the group's map (0 when unknown or no map yet).
    pub shards: usize,
}

/// The key class of a column of MariaDB type `data_type` (as
/// `information_schema.COLUMNS.DATA_TYPE` gives it) and collation; `None`
/// when it can't be a key part.
#[must_use]
pub fn column_class(data_type: &str, collation: Option<&str>) -> Option<KeyClass> {
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
        | "numeric" | "year" => Some(KeyClass::Numeric),
        "char" | "varchar" => {
            let c = collation_by_name(collation?)?;
            Some(KeyClass::String { collation: c.id })
        }
        "binary" | "varbinary" => Some(KeyClass::Binary),
        "uuid" => Some(KeyClass::Uuid),
        "date" | "datetime" => Some(KeyClass::DateTime),
        "timestamp" => Some(KeyClass::Timestamp),
        _ => None,
    }
}

/// A key class as messages and `SPRIG SHOW ADVICE` print it: `numeric`,
/// `string(utf8mb4_bin)`, ...
#[must_use]
pub fn class_name(c: KeyClass) -> String {
    match c {
        KeyClass::Numeric => "numeric".into(),
        KeyClass::String { collation } => format!(
            "string({})",
            collation::collation(collation).map_or("?", |c| c.name)
        ),
        KeyClass::Binary => "binary".into(),
        KeyClass::Uuid => "uuid".into(),
        KeyClass::DateTime => "datetime".into(),
        KeyClass::Timestamp => "timestamp".into(),
    }
}

fn classes_text(cs: &[KeyClass]) -> String {
    cs.iter()
        .map(|c| class_name(*c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reads and writes `group_pins` in catalog `schema`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PinStore;

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

impl PinStore {
    /// Every pin, by object id; empty before the module's format 3.
    ///
    /// # Errors
    /// MariaDB's; `Internal` for rows that don't parse.
    pub async fn load<Q: Queryable + Send>(&self, c: &mut Q, schema: &str) -> Result<Vec<Pin>> {
        Ok(self
            .rows(c, schema, false)
            .await?
            .into_iter()
            .map(|r| r.pin)
            .collect())
    }

    /// Every row; `lock` reads them `FOR UPDATE` (inside a transaction, so
    /// two pin statements check one after the other).
    ///
    /// # Errors
    /// MariaDB's; `Internal` for rows that don't parse.
    pub async fn rows<Q: Queryable + Send>(
        &self,
        c: &mut Q,
        schema: &str,
        lock: bool,
    ) -> Result<Vec<PinRow>> {
        let q = schema.replace('`', "``");
        let r: std::result::Result<Vec<Row>, _> = c
            .query(format!(
                "SELECT object_id, db_id, kind, CAST(key_columns AS CHAR), parent_object_id, \
                 pinned_by, DATE_FORMAT(pinned_at, '%Y-%m-%d %H:%i:%s.%f') \
                 FROM `{q}`.group_pins ORDER BY object_id{}",
                if lock { " FOR UPDATE" } else { "" }
            ))
            .await;
        let rows = match r {
            Ok(rows) => rows,
            Err(e) if missing_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(db_err(e)),
        };
        rows.into_iter().map(pin_from_row).collect()
    }

    /// Inserts or replaces `p`.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn put<Q: Queryable + Send>(
        &self,
        c: &mut Q,
        schema: &str,
        p: &Pin,
        by: &str,
    ) -> Result<()> {
        let q = schema.replace('`', "``");
        let cols = match &p.kind {
            PinKind::ShardKey { columns } | PinKind::Colocate { columns, .. } => {
                Some(serde_json::to_string(columns).unwrap_or_default())
            }
            PinKind::Reference | PinKind::Unsharded => None,
        };
        let parent = match &p.kind {
            PinKind::Colocate { parent, .. } => Some(parent.0),
            _ => None,
        };
        c.exec_drop(
            format!(
                "INSERT INTO `{q}`.group_pins (object_id, db_id, kind, key_columns, \
                 parent_object_id, pinned_by, pinned_at) \
                 VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6)) \
                 ON DUPLICATE KEY UPDATE db_id = VALUES(db_id), kind = VALUES(kind), \
                 key_columns = VALUES(key_columns), parent_object_id = VALUES(parent_object_id), \
                 pinned_by = VALUES(pinned_by), pinned_at = VALUES(pinned_at), \
                 row_version = row_version + 1"
            ),
            (p.object.0, p.db.0, p.kind.as_str(), cols, parent, by),
        )
        .await
        .map_err(db_err)
    }

    /// Removes `object`'s pin; whether there was one.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn remove<Q: Queryable + Send>(
        &self,
        c: &mut Q,
        schema: &str,
        object: ObjectId,
    ) -> Result<bool> {
        let q = schema.replace('`', "``");
        let r = c
            .exec_drop(
                format!("DELETE FROM `{q}`.group_pins WHERE object_id = ?"),
                (object.0,),
            )
            .await;
        match r {
            Ok(()) => {}
            Err(e) if missing_table(&e) => return Ok(false),
            Err(e) => return Err(db_err(e)),
        }
        let n: Option<i64> = c.query_first("SELECT ROW_COUNT()").await.map_err(db_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Sets `group_members.pinned` of `object` (no row: nothing).
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn mark_member<Q: Queryable + Send>(
        &self,
        c: &mut Q,
        schema: &str,
        object: ObjectId,
        pinned: bool,
    ) -> Result<()> {
        let q = schema.replace('`', "``");
        let r = c
            .exec_drop(
                format!(
                    "UPDATE `{q}`.group_members SET pinned = ?, row_version = row_version + 1 \
                     WHERE object_id = ? AND pinned <> ?"
                ),
                (pinned, object.0, pinned),
            )
            .await;
        match r {
            Ok(()) => Ok(()),
            Err(e) if missing_table(&e) => Ok(()),
            Err(e) => Err(db_err(e)),
        }
    }

    /// Every member of a live sharded group with its group's key
    /// (`shards` left 0: the shard maps are in etcd).
    ///
    /// # Errors
    /// MariaDB's; `Internal` for rows that don't parse.
    pub async fn placements<Q: Queryable + Send>(
        &self,
        c: &mut Q,
        schema: &str,
    ) -> Result<Vec<Placed>> {
        let q = schema.replace('`', "``");
        let r: std::result::Result<Vec<Row>, _> = c
            .query(format!(
                "SELECT m.object_id, m.group_id, m.routing, CAST(m.key_columns AS CHAR), \
                 m.parent_object_id, m.parent_fk, CAST(g.key_spec AS CHAR) \
                 FROM `{q}`.group_members m JOIN `{q}`.table_groups g ON g.group_id = m.group_id \
                 WHERE g.kind = 'sharded' AND g.state <> 'retired' ORDER BY m.object_id"
            ))
            .await;
        let rows = match r {
            Ok(rows) => rows,
            Err(e) if missing_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(db_err(e)),
        };
        rows.into_iter().map(placed_from_row).collect()
    }
}

fn bad_row(what: &str) -> SprigError {
    SprigError::internal(format!("unreadable {what} row"))
}

fn pin_from_row(mut r: Row) -> Result<PinRow> {
    let e = || bad_row("group_pins");
    let object: u64 = r.take(0).ok_or_else(e)?;
    let db: u32 = r.take(1).ok_or_else(e)?;
    let kind: String = r.take(2).ok_or_else(e)?;
    let cols: Option<String> = r.take(3).ok_or_else(e)?;
    let parent: Option<u64> = r.take(4).ok_or_else(e)?;
    let by: Option<String> = r.take(5).ok_or_else(e)?;
    let at: String = r.take(6).ok_or_else(e)?;
    let columns = || -> Result<Vec<String>> {
        serde_json::from_str(cols.as_deref().unwrap_or("[]")).map_err(|_| e())
    };
    let kind = match kind.as_str() {
        "shard_key" => PinKind::ShardKey {
            columns: columns()?,
        },
        "colocate" => PinKind::Colocate {
            parent: ObjectId(parent.ok_or_else(e)?),
            columns: columns()?,
        },
        "reference" => PinKind::Reference,
        "unsharded" => PinKind::Unsharded,
        _ => return Err(e()),
    };
    Ok(PinRow {
        pin: Pin {
            object: ObjectId(object),
            db: DbId(db),
            kind,
        },
        by,
        at,
    })
}

fn placed_from_row(mut r: Row) -> Result<Placed> {
    let e = || bad_row("group_members");
    let object: u64 = r.take(0).ok_or_else(e)?;
    let group: u32 = r.take(1).ok_or_else(e)?;
    let routing: String = r.take(2).ok_or_else(e)?;
    let cols: Option<String> = r.take(3).ok_or_else(e)?;
    let parent: Option<u64> = r.take(4).ok_or_else(e)?;
    let fk: Option<String> = r.take(5).ok_or_else(e)?;
    let spec: Option<String> = r.take(6).ok_or_else(e)?;
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
    let key = match spec {
        Some(s) => {
            let v: serde_json::Value = serde_json::from_str(&s).map_err(|_| e())?;
            Some(KeySpec::from_json(&v)?)
        }
        None => None,
    };
    Ok(Placed {
        object: ObjectId(object),
        group: GroupId(group),
        routing,
        key,
        shards: 0,
    })
}

/// A table's columns: lower-case name and key class (`None`: not a key
/// type).
pub type Columns = Vec<(String, Option<KeyClass>)>;

/// What [`check_pin`] reads besides the new pin.
pub struct Facts<'a> {
    /// Every pin now (the pinned table's own included, if it has one).
    pub pins: &'a [Pin],
    /// The columns of every table a pin names; a table missing here is
    /// unknown and its rules are skipped.
    pub columns: &'a BTreeMap<ObjectId, Columns>,
    /// Current members of sharded groups, with their groups' shard counts.
    pub placed: &'a [Placed],
    /// A table's name for messages, `db.table`.
    pub name: &'a dyn Fn(ObjectId) -> String,
}

fn conflict(msg: String) -> SprigError {
    SprigError::new(SprigCode::PinConflict, msg)
}

impl Facts<'_> {
    fn classes(&self, o: ObjectId, cols: &[String]) -> Option<Vec<Option<KeyClass>>> {
        let known = self.columns.get(&o)?;
        Some(
            cols.iter()
                .map(|c| known.iter().find(|(n, _)| n == c).and_then(|(_, k)| *k))
                .collect(),
        )
    }

    fn placed(&self, o: ObjectId) -> Option<&Placed> {
        self.placed.iter().find(|p| p.object == o)
    }
}

/// Rule 1 for `p`'s own columns.
fn check_columns(p: &Pin, f: &Facts<'_>) -> Result<()> {
    let cols = p.kind.columns();
    let mut seen = BTreeSet::new();
    for c in cols {
        if !seen.insert(c.as_str()) {
            return Err(conflict(format!(
                "pin on {}: column '{c}' named twice",
                (f.name)(p.object)
            )));
        }
    }
    let Some(known) = f.columns.get(&p.object) else {
        return Ok(());
    };
    for c in cols {
        match known.iter().find(|(n, _)| n == c) {
            None => {
                return Err(SprigError::standard(
                    1072,
                    *b"42000",
                    format!("Key column '{c}' doesn't exist in table"),
                ));
            }
            Some((_, None)) => {
                return Err(conflict(format!(
                    "pin on {}: column '{c}' has a type or collation a distribution key can't use",
                    (f.name)(p.object)
                )));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The group a `COLOCATE` chain from `start` ends in: the root, and the
/// root's key classes when known. Errors on a cycle or a
/// parent pinned out of sharded groups.
fn chain_root(
    start: &Pin,
    by_object: &BTreeMap<ObjectId, &Pin>,
    f: &Facts<'_>,
) -> Result<(ObjectId, Option<Vec<KeyClass>>)> {
    let mut path = vec![start.object];
    let mut cur = start;
    loop {
        let PinKind::Colocate { parent, .. } = &cur.kind else {
            break;
        };
        if *parent == cur.object {
            return Err(conflict(format!(
                "{} can't be co-located with itself",
                (f.name)(cur.object)
            )));
        }
        if path.contains(parent) {
            let names: Vec<String> = path.iter().map(|o| (f.name)(*o)).collect();
            return Err(conflict(format!(
                "COLOCATE pins form a cycle: {} -> {}",
                names.join(" -> "),
                (f.name)(*parent)
            )));
        }
        path.push(*parent);
        match by_object.get(parent) {
            Some(p) if p.kind.keeps_unsharded() => {
                return Err(conflict(format!(
                    "{} is pinned {}: {} can't be co-located with it",
                    (f.name)(*parent),
                    p.kind.as_str().to_ascii_uppercase(),
                    (f.name)(cur.object)
                )));
            }
            Some(p) => cur = p,
            None => break,
        }
    }
    let root = *path.last().unwrap_or(&start.object);
    let key = match by_object.get(&root).map(|p| &p.kind) {
        Some(PinKind::ShardKey { columns }) => f
            .classes(root, columns)
            .and_then(|cs| cs.into_iter().collect::<Option<Vec<_>>>()),
        _ => f
            .placed(root)
            .and_then(|p| p.key.as_ref())
            .map(|k| k.parts.clone()),
    };
    Ok((root, key))
}

/// Whether `p`'s `COLOCATE` chain (cycles cut) passes through `target`.
fn reaches(p: &Pin, target: ObjectId, by_object: &BTreeMap<ObjectId, &Pin>) -> bool {
    let mut seen = BTreeSet::new();
    let mut cur = p;
    loop {
        if cur.object == target {
            return true;
        }
        if !seen.insert(cur.object) {
            return false;
        }
        let PinKind::Colocate { parent, .. } = &cur.kind else {
            return false;
        };
        if *parent == target {
            return true;
        }
        match by_object.get(parent) {
            Some(next) => cur = next,
            None => return false,
        }
    }
}

/// Chapter 16's conflict rules and the R1 re-key rule for `new` (see the
/// module docs).
///
/// # Errors
/// 39410 `ER_SPRIG_PIN_CONFLICT`, 1072 for a missing column, 39411
/// `ER_SPRIG_PIN_NEEDS_REKEY`.
pub fn check_pin(new: &Pin, f: &Facts<'_>) -> Result<()> {
    check_columns(new, f)?;
    let after: Vec<&Pin> = f
        .pins
        .iter()
        .filter(|p| p.object != new.object)
        .chain(std::iter::once(new))
        .collect();
    let by_object: BTreeMap<ObjectId, &Pin> = after.iter().map(|p| (p.object, *p)).collect();
    // Rules 2-4: every COLOCATE chain the new pin is on.
    for p in &after {
        let PinKind::Colocate { columns, .. } = &p.kind else {
            continue;
        };
        // Only chains through the new pin's table: an older pin that a
        // later change (a dropped table, an altered column) broke doesn't
        // block unrelated pins.
        if !reaches(p, new.object, &by_object) {
            continue;
        }
        let (root, key) = chain_root(p, &by_object, f)?;
        let (Some(key), Some(mine)) = (key, f.classes(p.object, columns)) else {
            continue;
        };
        let mine: Option<Vec<KeyClass>> = mine.into_iter().collect();
        let Some(mine) = mine else { continue };
        if mine != key {
            return Err(conflict(format!(
                "{} ({}) is ({}), but the key of {}'s group is ({})",
                (f.name)(p.object),
                columns.join(", "),
                classes_text(&mine),
                (f.name)(root),
                classes_text(&key)
            )));
        }
    }
    // Rule 4: tables co-located with the new pin's table.
    if new.kind.keeps_unsharded() {
        let deps: Vec<String> = after
            .iter()
            .filter(
                |p| matches!(&p.kind, PinKind::Colocate { parent, .. } if *parent == new.object),
            )
            .map(|p| (f.name)(p.object))
            .collect();
        if !deps.is_empty() {
            return Err(conflict(format!(
                "{} can't be pinned {}: {} {} pinned COLOCATE WITH it",
                (f.name)(new.object),
                new.kind.as_str().to_ascii_uppercase(),
                deps.join(", "),
                if deps.len() == 1 { "is" } else { "are" }
            )));
        }
    }
    check_rekey(new, f)
}

/// Whether `pin` describes `placed` as it is: same routing, and for
/// `COLOCATE` the parent's group.
#[must_use]
pub fn matches_placement(pin: &Pin, placed: &[Placed]) -> bool {
    let Some(me) = placed.iter().find(|p| p.object == pin.object) else {
        return false;
    };
    match &pin.kind {
        PinKind::ShardKey { columns } => {
            matches!(&me.routing, MemberRouting::Columns(c) if c == columns)
        }
        PinKind::Colocate { parent, columns } => {
            matches!(&me.routing, MemberRouting::Columns(c) if c == columns)
                && placed
                    .iter()
                    .any(|p| p.object == *parent && p.group == me.group)
        }
        PinKind::Reference | PinKind::Unsharded => false,
    }
}

fn check_rekey(new: &Pin, f: &Facts<'_>) -> Result<()> {
    let Some(me) = f.placed(new.object) else {
        return Ok(());
    };
    if me.shards <= 1 || matches_placement(new, f.placed) {
        return Ok(());
    }
    Err(SprigError::new(
        SprigCode::PinNeedsRekey,
        format!(
            "{} is in a table group that has already split into {} shards; pinning it {} would \
             change its key or group, which needs an online re-key (R3)",
            (f.name)(new.object),
            me.shards,
            new.kind.as_str().to_ascii_uppercase()
        ),
    ))
}

#[cfg(test)]
mod tests;
