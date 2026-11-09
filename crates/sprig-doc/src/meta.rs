//! Collection metadata and DDL (chapter 31, "Data structures and storage";
//! build steps 4 and 6).
//!
//! **Interim storage (R5 interim decision).** Chapter 31 keeps
//! collections, indexes and field columns in the tenant catalog
//! (`collections`, `doc_indexes`, `promoted_fields`), published to every
//! gateway through catalog versions. Until that module exists, each
//! logical database keeps one hidden table, `__sprig_doc_meta`, with a row
//! per collection: its table, uuid, the path registry (`array_paths`), its
//! indexes and options. The engine reads the row at the start of every
//! command, so a gateway never plans on a stale registry, and adds a new
//! array path in its own committed statement *before* the write that
//! needs it, which is chapter 31's publish-before-commit rule without the
//! catalog round.
//!
//! The collection table has chapter 31's columns. An index on a path adds
//! three `VIRTUAL INVISIBLE` field columns (a *pair* in the chapter's
//! terms, with a string and a number value column) and one MariaDB index
//! over them:
//!
//! ```text
//! _sprig_p_<slug>_c  TINYINT UNSIGNED   type class in MongoDB order (20 = null or missing)
//! _sprig_p_<slug>_n  DOUBLE NOT NULL    numbers (±∞ at ±DBL_MAX), booleans 0/1, out-of-range dates (ms)
//! _sprig_p_<slug>_s  VARCHAR(160) bin   strings, ObjectId hex, ISO dates, UUID text
//! KEY sprig_dix_<id> (a_c, a_n, a_s, b_c DESC, b_n DESC, b_s DESC)
//! ```

use std::collections::BTreeSet;

use bson::{Bson, Document, doc};
use serde_json::{Value, json};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::sql::{ident, json_path, str_lit, types_path};

/// The hidden metadata table of a logical database.
pub const META_TABLE: &str = "__sprig_doc_meta";

/// Characters of a string kept in a field column.
pub const S_CHARS: usize = 160;

/// `CREATE TABLE` of the metadata table.
#[must_use]
pub fn meta_table_sql(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  coll        VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  table_name  VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  coll_uuid   BINARY(16) NOT NULL,
  array_paths JSON NOT NULL,
  arrays_full TINYINT(1) NOT NULL DEFAULT 0,
  indexes     JSON NOT NULL,
  options     JSON NOT NULL,
  next_index  INT UNSIGNED NOT NULL DEFAULT 1,
  created_at  DATETIME(3) NOT NULL,
  PRIMARY KEY (coll)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        ident(db),
        ident(META_TABLE)
    )
}

/// The columns the engine selects from the metadata table, in order.
pub const META_COLUMNS: &str =
    "coll, table_name, HEX(coll_uuid), array_paths, arrays_full, indexes, options, next_index";

/// One index (chapter 31's `doc_indexes` row).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDef {
    /// Index id; 0 is `_id`.
    pub id: u32,
    /// The MongoDB name.
    pub name: String,
    /// The key pattern, in order: `(path, 1 | -1)`.
    pub key: Vec<(String, i32)>,
    /// `unique`.
    pub unique: bool,
    /// `sparse` (recorded; MariaDB indexes every row).
    pub sparse: bool,
    /// `expireAfterSeconds` (recorded; the TTL worker isn't built yet).
    pub expire_after: Option<i64>,
    /// `user`, `migrated` or `auto`.
    pub origin: String,
    /// Options passed through to `listIndexes` as given.
    pub extra: Document,
}

impl IndexDef {
    /// The MariaDB index name.
    #[must_use]
    pub fn mariadb_name(&self) -> String {
        format!("sprig_dix_{}", self.id)
    }

    /// The key pattern as a document.
    #[must_use]
    pub fn key_doc(&self) -> Document {
        let mut d = Document::new();
        for (p, dir) in &self.key {
            d.insert(p.clone(), *dir);
        }
        d
    }

    /// `listIndexes`' entry.
    #[must_use]
    pub fn spec(&self) -> Document {
        let mut d = doc! {"v": 2, "key": self.key_doc(), "name": self.name.clone()};
        if self.unique {
            d.insert("unique", true);
        }
        if self.sparse {
            d.insert("sparse", true);
        }
        if let Some(e) = self.expire_after {
            d.insert("expireAfterSeconds", e);
        }
        for (k, v) in &self.extra {
            d.insert(k.clone(), v.clone());
        }
        d
    }

    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "key": self.key.iter().map(|(p, d)| json!([p, d])).collect::<Vec<_>>(),
            "unique": self.unique,
            "sparse": self.sparse,
            "expire": self.expire_after,
            "origin": self.origin,
            "extra": Bson::Document(self.extra.clone()).into_relaxed_extjson(),
        })
    }

    fn from_json(v: &Value) -> Option<Self> {
        let key = v
            .get("key")?
            .as_array()?
            .iter()
            .filter_map(|e| {
                let a = e.as_array()?;
                Some((
                    a.first()?.as_str()?.to_owned(),
                    i32::try_from(a.get(1)?.as_i64()?).ok()?,
                ))
            })
            .collect();
        let extra = v
            .get("extra")
            .and_then(|e| Bson::try_from(e.clone()).ok())
            .and_then(|b| b.as_document().cloned())
            .unwrap_or_default();
        Some(Self {
            id: u32::try_from(v.get("id")?.as_u64()?).ok()?,
            name: v.get("name")?.as_str()?.to_owned(),
            key,
            unique: v.get("unique").and_then(Value::as_bool).unwrap_or(false),
            sparse: v.get("sparse").and_then(Value::as_bool).unwrap_or(false),
            expire_after: v.get("expire").and_then(Value::as_i64),
            origin: v
                .get("origin")
                .and_then(Value::as_str)
                .unwrap_or("user")
                .to_owned(),
            extra,
        })
    }
}

/// A collection's metadata row.
#[derive(Debug, Clone, PartialEq)]
pub struct CollMeta {
    /// The MongoDB name.
    pub name: String,
    /// The MariaDB table.
    pub table: String,
    /// `info.uuid`.
    pub uuid: [u8; 16],
    /// The path registry: every path where some document held an array.
    pub array_paths: BTreeSet<String>,
    /// Past `doc.max_array_paths`: every path counts as an array path.
    pub arrays_full: bool,
    /// Secondary indexes (not `_id`).
    pub indexes: Vec<IndexDef>,
    /// `create` options (validator, collation, ...), as given.
    pub options: Document,
    /// The next index id.
    pub next_index: u32,
}

/// Paths with field columns.
impl CollMeta {
    /// Whether some document may hold an array at `path` or any prefix.
    #[must_use]
    pub fn may_cross_array(&self, path: &str) -> bool {
        if self.arrays_full {
            return true;
        }
        let mut p = String::new();
        for part in path.split('.') {
            if !p.is_empty() {
                p.push('.');
            }
            p.push_str(part);
            if self.array_paths.contains(&p) {
                return true;
            }
        }
        false
    }

    /// Which parts of `path` may hold arrays (for `[*]` variants).
    #[must_use]
    pub fn array_parts(&self, path: &str) -> Vec<bool> {
        let mut p = String::new();
        path.split('.')
            .map(|part| {
                if !p.is_empty() {
                    p.push('.');
                }
                p.push_str(part);
                self.arrays_full || self.array_paths.contains(&p)
            })
            .collect()
    }

    /// Whether `path` has field columns (some ready index covers it).
    #[must_use]
    pub fn has_columns(&self, path: &str) -> bool {
        self.indexes
            .iter()
            .any(|i| i.key.iter().any(|(p, _)| p == path))
    }

    /// The metadata row's values for an insert.
    #[must_use]
    pub fn insert_sql(&self, db: &str) -> String {
        format!(
            "INSERT INTO {}.{} (coll, table_name, coll_uuid, array_paths, arrays_full, indexes, options, next_index, created_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, UTC_TIMESTAMP(3))",
            ident(db),
            ident(META_TABLE),
            str_lit(&self.name),
            str_lit(&self.table),
            crate::sql::bin_lit(&self.uuid),
            str_lit(&self.array_paths_json()),
            u8::from(self.arrays_full),
            str_lit(&self.indexes_json()),
            str_lit(
                &Bson::Document(self.options.clone())
                    .into_relaxed_extjson()
                    .to_string()
            ),
            self.next_index,
        )
    }

    /// `array_paths` as JSON text.
    #[must_use]
    pub fn array_paths_json(&self) -> String {
        Value::from(self.array_paths.iter().cloned().collect::<Vec<_>>()).to_string()
    }

    /// `indexes` as JSON text.
    #[must_use]
    pub fn indexes_json(&self) -> String {
        Value::from(
            self.indexes
                .iter()
                .map(IndexDef::to_json)
                .collect::<Vec<_>>(),
        )
        .to_string()
    }

    /// Reads a row selected with [`META_COLUMNS`].
    ///
    /// # Errors
    /// 1 `InternalError` for a malformed row.
    pub fn from_row(row: &[Option<Vec<u8>>]) -> Result<Self, MongoError> {
        let bad = || MongoError::new(code::INTERNAL_ERROR, "malformed collection metadata");
        let text = |i: usize| -> Result<String, MongoError> {
            row.get(i)
                .and_then(Option::as_ref)
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .ok_or_else(bad)
        };
        let uuid_hex = text(2)?;
        let mut uuid = [0u8; 16];
        for (i, b) in uuid.iter_mut().enumerate() {
            *b = u8::from_str_radix(uuid_hex.get(2 * i..2 * i + 2).ok_or_else(bad)?, 16)
                .map_err(|_| bad())?;
        }
        let paths: Value = serde_json::from_str(&text(3)?).map_err(|_| bad())?;
        let idx: Value = serde_json::from_str(&text(5)?).map_err(|_| bad())?;
        let opts: Value = serde_json::from_str(&text(6)?).map_err(|_| bad())?;
        Ok(Self {
            name: text(0)?,
            table: text(1)?,
            uuid,
            array_paths: paths
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            arrays_full: text(4)? == "1",
            indexes: idx
                .as_array()
                .map(|a| a.iter().filter_map(IndexDef::from_json).collect())
                .unwrap_or_default(),
            options: Bson::try_from(opts)
                .ok()
                .and_then(|b| b.as_document().cloned())
                .unwrap_or_default(),
            next_index: text(7)?.parse().unwrap_or(1),
        })
    }
}

/// The MariaDB table of collection `name`: the name itself when it is at
/// most 40 characters of `[A-Za-z0-9_]` and not reserved, else `c` and 16
/// hex digits of its hash.
#[must_use]
pub fn table_name(name: &str) -> String {
    let plain = !name.is_empty()
        && name.len() <= 40
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.starts_with("_sprig")
        && !name.starts_with("__sprig");
    if plain {
        name.to_owned()
    } else {
        format!("c{:016x}", fnv64(name.as_bytes()))
    }
}

/// FNV-1a, 64 bits: stable names from paths.
#[must_use]
pub fn fnv64(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for x in b {
        h ^= u64::from(*x);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// The slug of a field path: up to 20 safe characters and 8 hex digits
/// of its hash, so `_sprig_p_<slug>_c` stays within 64 characters.
#[must_use]
pub fn slug(path: &str) -> String {
    let safe: String = path
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .take(20)
        .collect();
    format!("{safe}_{:08x}", fnv64(path.as_bytes()) & 0xFFFF_FFFF)
}

/// The three field columns of `path`: `(class, number, string)`.
#[must_use]
pub fn columns(path: &str) -> (String, String, String) {
    let s = slug(path);
    (
        format!("_sprig_p_{s}_c"),
        format!("_sprig_p_{s}_n"),
        format!("_sprig_p_{s}_s"),
    )
}

/// `CREATE TABLE` of a collection.
#[must_use]
pub fn create_table_sql(db: &str, table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  _id    VARBINARY(1024) NOT NULL,
  _t     VARCHAR(255) NULL,
  doc    JSON NOT NULL,
  _types JSON NULL,
  _v     BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        ident(db),
        ident(table)
    )
}

/// The SQL class expression of a scalar path (chapter 31, "`_id`
/// encoding and type classes").
#[must_use]
pub fn class_expr(path: &str) -> String {
    let parts: Vec<&str> = path.split('.').collect();
    let p = str_lit(&json_path(&parts, &[]));
    let t = str_lit(&types_path(path));
    format!(
        "CASE COALESCE(SUBSTRING_INDEX(JSON_VALUE(_types, {t}), ':', 1), JSON_TYPE(JSON_EXTRACT(doc, {p})), 'MISSING') \
WHEN 'INTEGER' THEN 30 WHEN 'DOUBLE' THEN 30 WHEN 'long' THEN 30 WHEN 'decimal' THEN 30 WHEN 'double' THEN 30 \
WHEN 'STRING' THEN 40 WHEN 'symbol' THEN 40 WHEN 'OBJECT' THEN 50 WHEN 'ARRAY' THEN 60 \
WHEN 'binData' THEN 70 WHEN 'uuid' THEN 70 WHEN 'objectId' THEN 80 WHEN 'BOOLEAN' THEN 90 \
WHEN 'date' THEN 100 WHEN 'timestamp' THEN 110 WHEN 'regex' THEN 120 WHEN 'dbPointer' THEN 130 \
WHEN 'javascript' THEN 140 WHEN 'javascriptWithScope' THEN 145 WHEN 'minKey' THEN 10 \
WHEN 'maxKey' THEN 250 ELSE 20 END"
    )
}

/// `ALTER TABLE` adding the field columns of `path` (instant; no index).
#[must_use]
pub fn add_columns_sql(db: &str, table: &str, path: &str) -> String {
    let parts: Vec<&str> = path.split('.').collect();
    let p = str_lit(&json_path(&parts, &[]));
    let t = str_lit(&types_path(path));
    let (c, n, s) = columns(path);
    let ty = format!("JSON_VALUE(_types, {t})");
    let jt = format!("JSON_TYPE(JSON_EXTRACT(doc, {p}))");
    let val = format!("JSON_VALUE(doc, {p})");
    let num = format!(
        "COALESCE(CASE \
WHEN {jt} IN ('INTEGER','DOUBLE') AND ({ty} IS NULL OR {ty} IN ('long','decimal','double')) THEN CAST({val} AS DOUBLE) \
WHEN {ty} IN ('double','decimal') AND {val} = 'Infinity' THEN 1.7976931348623157E308 \
WHEN {ty} IN ('double','decimal') AND {val} = '-Infinity' THEN -1.7976931348623157E308 \
WHEN {jt} = 'BOOLEAN' THEN IF({val} = 'true', 1, 0) \
WHEN {ty} = 'date' AND {jt} = 'INTEGER' THEN CAST({val} AS DOUBLE) \
END, 0)"
    );
    let st = format!(
        "COALESCE(CASE WHEN {jt} = 'STRING' AND ({ty} IS NULL OR {ty} IN ('symbol','objectId','date','uuid')) \
THEN LEFT({val}, {S_CHARS}) END, '')"
    );
    format!(
        "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} TINYINT UNSIGNED AS ({}) VIRTUAL INVISIBLE, \
ADD COLUMN IF NOT EXISTS {} DOUBLE AS ({num}) VIRTUAL INVISIBLE, \
ADD COLUMN IF NOT EXISTS {} VARCHAR({S_CHARS}) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin AS ({st}) VIRTUAL INVISIBLE",
        ident(db),
        ident(table),
        ident(&c),
        class_expr(path),
        ident(&n),
        ident(&s),
    )
}

/// `CREATE INDEX` of `idx`.
#[must_use]
pub fn create_index_sql(db: &str, table: &str, idx: &IndexDef) -> String {
    let mut cols = Vec::new();
    for (p, dir) in &idx.key {
        let (c, n, s) = columns(p);
        let d = if *dir < 0 { " DESC" } else { "" };
        cols.push(format!(
            "{}{d}, {}{d}, {}{d}",
            ident(&c),
            ident(&n),
            ident(&s)
        ));
    }
    format!(
        "CREATE {}INDEX {} ON {}.{} ({})",
        if idx.unique { "UNIQUE " } else { "" },
        ident(&idx.mariadb_name()),
        ident(db),
        ident(table),
        cols.join(", ")
    )
}

/// `DROP INDEX` of `idx`.
#[must_use]
pub fn drop_index_sql(db: &str, table: &str, idx: &IndexDef) -> String {
    format!(
        "DROP INDEX {} ON {}.{}",
        ident(&idx.mariadb_name()),
        ident(db),
        ident(table)
    )
}

/// `ALTER TABLE` dropping the field columns of `path`.
#[must_use]
pub fn drop_columns_sql(db: &str, table: &str, path: &str) -> String {
    let (c, n, s) = columns(path);
    format!(
        "ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}, DROP COLUMN IF EXISTS {}, DROP COLUMN IF EXISTS {}",
        ident(db),
        ident(table),
        ident(&c),
        ident(&n),
        ident(&s)
    )
}

/// The default index name MongoDB gives a key pattern: `a_1_b_-1`.
#[must_use]
pub fn default_index_name(key: &Document) -> String {
    key.iter()
        .map(|(k, v)| {
            let d = match v {
                Bson::String(s) => s.clone(),
                Bson::Double(f) => format!("{f}"),
                other => crate::filter::as_i64(other).map_or_else(|| "1".into(), |i| i.to_string()),
            };
            format!("{k}_{d}")
        })
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_round_trip() {
        assert_eq!(table_name("orders"), "orders");
        assert!(table_name("system.views").starts_with('c'));
        assert!(table_name("__sprig_x").starts_with('c'));
        let (c, _, _) = columns("customer.id.with.a.very.long.path.name");
        assert!(c.len() <= 64, "{c}");
        let m = CollMeta {
            name: "o".into(),
            table: "o".into(),
            uuid: [7; 16],
            array_paths: ["items".to_owned()].into(),
            arrays_full: false,
            indexes: vec![IndexDef {
                id: 1,
                name: "a_1_b_-1".into(),
                key: vec![("a".into(), 1), ("b".into(), -1)],
                unique: true,
                sparse: false,
                expire_after: None,
                origin: "user".into(),
                extra: doc! {"collation": {"locale": "simple"}},
            }],
            options: doc! {},
            next_index: 2,
        };
        let row: Vec<Option<Vec<u8>>> = vec![
            Some(b"o".to_vec()),
            Some(b"o".to_vec()),
            Some(b"07070707070707070707070707070707".to_vec()),
            Some(m.array_paths_json().into_bytes()),
            Some(b"0".to_vec()),
            Some(m.indexes_json().into_bytes()),
            Some(b"{}".to_vec()),
            Some(b"2".to_vec()),
        ];
        assert_eq!(CollMeta::from_row(&row).unwrap(), m);
        assert!(m.may_cross_array("items.sku"));
        assert!(!m.may_cross_array("item"));
        assert_eq!(m.array_parts("items.sku"), vec![true, false]);
        assert_eq!(default_index_name(&doc! {"a": 1, "b": -1}), "a_1_b_-1");
        let sql = create_index_sql("d", "o", &m.indexes[0]);
        assert!(
            sql.starts_with("CREATE UNIQUE INDEX `sprig_dix_1` ON `d`.`o`"),
            "{sql}"
        );
    }
}
