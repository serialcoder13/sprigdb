//! What the router knows about a tenant's layout: which tables are sharded,
//! by which key, in which table group, and where each shard lives.
//!
//! [`Topology`] is the router's own small interface to chapter 15's
//! layout. Until table groups and shard maps reach the catalog,
//! [`StaticTopology`] serves it from a JSON description (the gateway's
//! `[gateway] router_topology_file`, a development and test hook). Keys
//! normalize and hash with chapter 15's `sprig-shard` (`hash_key`: the
//! one normalizer, then `xxh3_64`), so rows the router writes sit where
//! splits, moves and the feed expect them; a shard owns the hashes from
//! its `lo` up to the next shard's (chapter 15's range entries).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Deserialize;
use sprig_common::{Result, SprigCode, SprigError};

/// How a group's key compares, which decides which literals route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyClass {
    /// An integer column: values route by their integer value.
    Int,
    /// A string under a collation (chapter 15's weight normalization).
    Str {
        /// The collation id.
        collation: u16,
    },
    /// Binary strings: values route by their bytes.
    Binary,
}

/// One shard of a group: its id, its schema and its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoc {
    /// Shard id, unique within the group.
    pub shard: u32,
    /// Physical schema (`t<tenant>_d<db>_g<group>_s<shard>`).
    pub schema: String,
    /// The cluster holding it.
    pub cluster: String,
    /// The lowest routing hash it owns.
    pub lo: u64,
}

/// A table group: one key class and the shards, ordered by `lo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// Group id within the tenant.
    pub id: u32,
    /// The key class.
    pub key_class: KeyClass,
    /// Chapter 15's key specification for the class.
    pub spec: sprig_shard::KeySpec,
    /// Shards, by `lo` ascending; the first has `lo` 0.
    pub shards: Vec<ShardLoc>,
}

impl Group {
    /// The shard that owns key value `v` (chapter 15's `hash_key`), or
    /// `None` when the value doesn't normalize (39405: scatter instead).
    #[must_use]
    pub fn shard_of(&self, v: &KeyValue) -> Option<usize> {
        let kv = match (self.key_class, v) {
            (KeyClass::Int, KeyValue::Int(i)) => sprig_shard::KeyValue::Int(*i),
            (KeyClass::Str { .. }, KeyValue::Bytes(b)) => sprig_shard::KeyValue::Str {
                bytes: b,
                charset: sprig_shard::Charset::Utf8mb4,
            },
            (KeyClass::Binary, KeyValue::Bytes(b)) => sprig_shard::KeyValue::Bytes(b),
            _ => return None,
        };
        let (h, _) = sprig_shard::hash_key(&self.spec, &[kv]).ok()?;
        Some(self.route(h.0))
    }

    /// The shard that owns routing hash `h`.
    #[must_use]
    pub fn route(&self, h: u64) -> usize {
        match self.shards.binary_search_by(|s| s.lo.cmp(&h)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        }
    }
}

/// How a table is distributed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    /// Rows spread over the group's shards by one key column.
    Sharded {
        /// The key column, lowercase.
        key: String,
        /// Its position in the table's column list (`INSERT` without a
        /// column list).
        key_ordinal: usize,
        /// Every unique key contains the key column, so upserts and
        /// `REPLACE` are exact per shard.
        upsert_safe: bool,
    },
    /// A full copy on every cluster that holds the tenant's shards, in
    /// this schema.
    Reference {
        /// The reference schema (`t<tenant>_d<db>_ref`).
        schema: String,
    },
}

/// One table the router knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntry {
    /// The table name, lowercase.
    pub name: String,
    /// How it is distributed.
    pub kind: TableKind,
    /// Its group (for reference tables, a group of the shards' clusters).
    pub group: Arc<Group>,
}

impl TableEntry {
    /// The key column of a sharded table.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        match &self.kind {
            TableKind::Sharded { key, .. } => Some(key),
            TableKind::Reference { .. } => None,
        }
    }

    /// Whether it is a reference table.
    #[must_use]
    pub fn is_reference(&self) -> bool {
        matches!(self.kind, TableKind::Reference { .. })
    }
}

/// A tenant's layout as the router reads it.
pub trait Topology: Send + Sync {
    /// The table `table` of the logical database whose default schema (the
    /// one chapter 07's rewrite writes) is `schema`. Both lowercase.
    fn table(&self, schema: &str, table: &str) -> Option<&TableEntry>;

    /// Whether `sql` may name a table this topology knows: a cheap word
    /// scan that lets statements over unsharded tables skip the parser.
    fn mentions(&self, sql: &str) -> bool;
}

/// A key value, as the router extracts it from a literal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    /// An integer.
    Int(i128),
    /// A string's bytes (unescaped).
    Bytes(Vec<u8>),
}

/// `n` shards' `lo` values, splitting the hash space evenly.
#[must_use]
pub fn even_ranges(n: u32) -> Vec<u64> {
    let n = u128::from(n.max(1));
    (0..n)
        .map(|i| u64::try_from((i << 64) / n).unwrap_or(u64::MAX))
        .collect()
}

/// A topology held in memory, built from [`TopologyFile`] or by hand.
#[derive(Debug, Clone, Default)]
pub struct StaticTopology {
    tables: HashMap<(String, String), TableEntry>,
    words: HashSet<String>,
}

impl StaticTopology {
    /// Adds `entry` under the default schema `schema`.
    pub fn insert(&mut self, schema: &str, entry: TableEntry) {
        self.words.insert(entry.name.clone());
        self.tables
            .insert((schema.to_ascii_lowercase(), entry.name.clone()), entry);
    }

    /// Whether no table is known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl Topology for StaticTopology {
    fn table(&self, schema: &str, table: &str) -> Option<&TableEntry> {
        self.tables
            .get(&(schema.to_ascii_lowercase(), table.to_ascii_lowercase()))
    }

    fn mentions(&self, sql: &str) -> bool {
        let bytes = sql.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i];
            if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
                {
                    i += 1;
                }
                let w = sql[start..i].to_ascii_lowercase();
                if self.words.contains(&w) {
                    return true;
                }
            } else {
                i += 1;
            }
        }
        false
    }
}

/// The JSON description `[gateway] router_topology_file` names: per tenant
/// name, groups and tables.
///
/// ```json
/// {"tenants": {"acme": {
///   "groups": [{"id": 901, "key_class": "int", "shards": [
///       {"shard": 1, "schema": "t5_d1_g901_s1", "cluster": "dc-a-c1"},
///       {"shard": 2, "schema": "t5_d1_g901_s2", "cluster": "dc-a-c1"}]}],
///   "tables": [
///       {"schema": "t5_d1_g1_s1", "table": "orders", "group": 901,
///        "key": "customer_id", "key_ordinal": 1, "upsert_safe": true},
///       {"schema": "t5_d1_g1_s1", "table": "countries", "group": 901,
///        "reference": "t5_d1_ref"}]}}}
/// ```
///
/// Shards without `lo` split the hash space evenly in the listed order.
#[derive(Debug, Clone, Deserialize)]
pub struct TopologyFile {
    /// Per tenant name.
    pub tenants: HashMap<String, TenantFile>,
}

/// One tenant of a [`TopologyFile`].
#[derive(Debug, Clone, Deserialize)]
pub struct TenantFile {
    /// Table groups.
    pub groups: Vec<GroupFile>,
    /// Tables.
    pub tables: Vec<TableFile>,
}

/// One group of a [`TenantFile`].
#[derive(Debug, Clone, Deserialize)]
pub struct GroupFile {
    /// Group id.
    pub id: u32,
    /// `int`, `binary`, or `string:<collation>` (`string:utf8mb4_general_ci`).
    pub key_class: String,
    /// Shards.
    pub shards: Vec<ShardFile>,
}

/// One shard of a [`GroupFile`].
#[derive(Debug, Clone, Deserialize)]
pub struct ShardFile {
    /// Shard id.
    pub shard: u32,
    /// Physical schema.
    pub schema: String,
    /// Cluster id.
    pub cluster: String,
    /// Lowest routing hash owned; unset: even split.
    #[serde(default)]
    pub lo: Option<u64>,
}

/// One table of a [`TenantFile`].
#[derive(Debug, Clone, Deserialize)]
pub struct TableFile {
    /// The database's default schema, as chapter 07 rewrites names.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Its group.
    pub group: u32,
    /// Key column (sharded tables).
    #[serde(default)]
    pub key: Option<String>,
    /// Key column position, from 0.
    #[serde(default)]
    pub key_ordinal: usize,
    /// Every unique key contains the key.
    #[serde(default)]
    pub upsert_safe: bool,
    /// Reference schema (reference tables).
    #[serde(default)]
    pub reference: Option<String>,
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, msg.into())
}

impl TenantFile {
    /// Builds the tenant's [`StaticTopology`].
    ///
    /// # Errors
    /// `ConfigInvalid` for an unknown key class, a table naming an unknown
    /// group, or a table that is neither sharded nor reference.
    pub fn build(&self) -> Result<StaticTopology> {
        let mut groups = HashMap::new();
        for g in &self.groups {
            let (key_class, part) = match g.key_class.as_str() {
                "int" => (KeyClass::Int, sprig_shard::KeyClass::Numeric),
                "binary" => (KeyClass::Binary, sprig_shard::KeyClass::Binary),
                other => match other.strip_prefix("string:") {
                    Some(name) => {
                        let c = sprig_shard::collation::require_name(name)?;
                        (
                            KeyClass::Str { collation: c.id.0 },
                            sprig_shard::KeyClass::String { collation: c.id },
                        )
                    }
                    None => {
                        return Err(bad(format!(
                            "group {}: key class {other:?} (int, binary or string:<collation>)",
                            g.id
                        )));
                    }
                },
            };
            let spec = sprig_shard::KeySpec::new(vec![part])?;
            let even = even_ranges(u32::try_from(g.shards.len()).unwrap_or(u32::MAX));
            let mut shards: Vec<ShardLoc> = g
                .shards
                .iter()
                .zip(even)
                .map(|(s, lo)| ShardLoc {
                    shard: s.shard,
                    schema: s.schema.clone(),
                    cluster: s.cluster.clone(),
                    lo: s.lo.unwrap_or(lo),
                })
                .collect();
            shards.sort_by_key(|s| s.lo);
            if shards.first().is_none_or(|s| s.lo != 0) {
                return Err(bad(format!("group {}: no shard owns hash 0", g.id)));
            }
            groups.insert(
                g.id,
                Arc::new(Group {
                    id: g.id,
                    key_class,
                    spec,
                    shards,
                }),
            );
        }
        let mut topo = StaticTopology::default();
        for t in &self.tables {
            let group = groups
                .get(&t.group)
                .cloned()
                .ok_or_else(|| bad(format!("table {}: unknown group {}", t.table, t.group)))?;
            let kind = match (&t.key, &t.reference) {
                (Some(k), None) => TableKind::Sharded {
                    key: k.to_ascii_lowercase(),
                    key_ordinal: t.key_ordinal,
                    upsert_safe: t.upsert_safe,
                },
                (None, Some(r)) => TableKind::Reference { schema: r.clone() },
                _ => {
                    return Err(bad(format!(
                        "table {}: give exactly one of key and reference",
                        t.table
                    )));
                }
            };
            topo.insert(
                &t.schema,
                TableEntry {
                    name: t.table.to_ascii_lowercase(),
                    kind,
                    group,
                },
            );
        }
        Ok(topo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn even_ranges_cover_the_space() {
        assert_eq!(even_ranges(1), vec![0]);
        let r = even_ranges(4);
        assert_eq!(r[0], 0);
        assert_eq!(r[1], 1 << 62);
        assert_eq!(r[3], 3 << 62);
    }

    fn group(key_class: &str, n: u32) -> Group {
        let f: TopologyFile = serde_json::from_str(&format!(
            r#"{{"tenants": {{"a": {{"groups": [{{"id": 9, "key_class": "{key_class}", "shards": [{}]}}],
              "tables": [{{"schema": "s", "table": "t", "group": 9, "key": "k"}}]}}}}}}"#,
            (1..=n)
                .map(|i| format!(r#"{{"shard": {i}, "schema": "x{i}", "cluster": "c"}}"#))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .unwrap_or_else(|e| panic!("{e}"));
        let t = f.tenants["a"].build().unwrap_or_else(|e| panic!("{e}"));
        (*t.table("s", "t").unwrap_or_else(|| panic!()).group).clone()
    }

    #[test]
    fn route_picks_the_owning_range() {
        let g = group("int", 4);
        assert_eq!(g.route(0), 0);
        assert_eq!(g.route((1 << 62) - 1), 0);
        assert_eq!(g.route(1 << 62), 1);
        assert_eq!(g.route(u64::MAX), 3);
    }

    #[test]
    fn string_keys_route_by_collation() {
        let g = group("string:utf8mb4_general_ci", 16);
        let s = |v: &str| g.shard_of(&KeyValue::Bytes(v.as_bytes().to_vec()));
        assert_eq!(s("Alice"), s("alice "));
        assert_eq!(g.shard_of(&KeyValue::Int(1)), None);
        let i = group("int", 16);
        assert!(i.shard_of(&KeyValue::Int(42)).is_some());
    }

    #[test]
    fn file_builds_and_mentions_scan_words() {
        let f: TopologyFile = serde_json::from_str(
            r#"{"tenants": {"a": {"groups": [{"id": 9, "key_class": "int", "shards": [
                {"shard": 1, "schema": "t1_d1_g9_s1", "cluster": "c1"},
                {"shard": 2, "schema": "t1_d1_g9_s2", "cluster": "c1"}]}],
              "tables": [{"schema": "t1_d1_g1_s1", "table": "Orders", "group": 9, "key": "cid"}]}}}"#,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let t = f.tenants["a"].build().unwrap_or_else(|e| panic!("{e}"));
        assert!(t.table("T1_D1_G1_S1", "orders").is_some());
        assert!(t.mentions("select * from `orders` where x=1"));
        assert!(!t.mentions("select * from orders_x"));
    }
}
