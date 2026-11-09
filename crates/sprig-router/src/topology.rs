//! What the router knows about a tenant's layout: which tables are sharded,
//! by which key, in which table group, and where each shard lives.
//!
//! [`Topology`] is the router's own small interface to chapter 15's
//! layout. [`StaticTopology`] serves it from memory: the gateway builds one
//! per tenant with [`catalog::build`] from the catalog's table groups and
//! members and `sprig-shard`'s shard maps; unit tests build one from a JSON
//! description ([`TopologyFile`]). Keys normalize and hash with chapter
//! 15's `sprig-shard` (`hash_key`: the one normalizer, then `xxh3_64`), so
//! rows the router writes sit where splits, moves and the feed expect them;
//! a shard owns the hashes from its `lo` up to the next shard's (chapter
//! 15's range entries).

pub mod catalog;

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
    /// The range's epoch (raised by every split, merge and move).
    pub epoch: u64,
    /// While a mover cutover holds the range: when the hold ends (ms since
    /// the Unix epoch). New work waits for it (chapter 10, "Cutover").
    pub held_until_ms: Option<u64>,
}

/// A table group's shards as one logical database sees them: one key
/// class and the shards, ordered by `lo`. Members of one group in two
/// databases share shards but not schemas, so each database gets its own
/// `Group` (same `id`, its own `db`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// Group id within the tenant.
    pub id: u32,
    /// The logical database whose schemas `shards` name (0 in test
    /// layouts).
    pub db: u32,
    /// The shard map's `map_version` (0 in test layouts).
    pub version: u64,
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
        let h = self.hash_of(v)?;
        let i = self.route(h);
        crate::write_notes::note(self, i, h);
        Some(i)
    }

    /// Shard `i`'s place, for transaction branches.
    #[must_use]
    pub fn at(&self, i: usize) -> crate::plan::ShardAt {
        self.shards
            .get(i)
            .map(|s| crate::plan::ShardAt {
                group: self.id,
                db: self.db,
                lo: s.lo,
                schema: s.schema.clone(),
            })
            .unwrap_or_default()
    }

    /// Whether rows of this group and `o`'s sit on the same shards: the
    /// same group and logical database.
    #[must_use]
    pub fn same_layout(&self, o: &Group) -> bool {
        self.id == o.id && self.db == o.db
    }

    /// The routing hash of key value `v` (chapter 15's `hash_key`).
    #[must_use]
    pub fn hash_of(&self, v: &KeyValue) -> Option<u64> {
        let kv = match (self.key_class, v) {
            (KeyClass::Int, KeyValue::Int(i)) => sprig_shard::KeyValue::Int(*i),
            (KeyClass::Str { .. }, KeyValue::Bytes(b)) => sprig_shard::KeyValue::Str {
                bytes: b,
                charset: sprig_shard::Charset::Utf8mb4,
            },
            (KeyClass::Binary, KeyValue::Bytes(b)) => sprig_shard::KeyValue::Bytes(b),
            _ => return None,
        };
        sprig_shard::hash_key(&self.spec, &[kv])
            .ok()
            .map(|(h, _)| h.0)
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
        /// The key column, lowercase; empty when no single column holds
        /// the key (a parent member or a composite key): every read
        /// scatters, and inserts are refused unless the table is a parent
        /// member ([`TableEntry::parent`]), whose rows follow their parent.
        key: String,
        /// Its position among the table's visible columns (`INSERT`
        /// without a column list); `None` when unknown.
        key_ordinal: Option<usize>,
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
    /// Its unique lookup indexes (chapter 19) the router maintains and
    /// routes through: one value column each.
    pub lookups: Vec<Arc<LookupRef>>,
    /// A maintained lookup index the router can't follow (several value
    /// columns, an unknown column type): writes are refused (39420
    /// `lookup`) rather than left unmaintained.
    pub lookup_gap: bool,
    /// The visible columns in order, lowercase, from the stored definition
    /// (empty when unknown): an `INSERT` without a column list gets one,
    /// so the router can add `_sprig_rk` (chapter 15).
    pub columns: Vec<String>,
    /// Writes add `_sprig_rk` (the member is `filling` or later).
    pub fill_rk: bool,
    /// A parent member (chapter 15): no key column of its own; its rows
    /// go where their parent row is, through this foreign key.
    pub parent: Option<ParentLink>,
    /// Its `AUTO_INCREMENT` column, which takes chapter 19's ID blocks
    /// (every member of a sharded group with one).
    pub auto_inc: Option<AutoInc>,
}

/// A sharded table's `AUTO_INCREMENT` column (chapter 19, "ID blocks"):
/// inserts take its values from the gateway's block of the table's
/// `sequences` row, never from a shard's own counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoInc {
    /// The column, lowercase.
    pub column: String,
    /// Its position among the visible columns (`INSERT` without a column
    /// list).
    pub ordinal: Option<usize>,
    /// The `sequences` row: `seq_id` is the table's object id.
    pub seq: u64,
}

/// How a parent member's row finds its shard (chapter 15, "Table groups";
/// chapter 17, "INSERT routing"): through a declared foreign key to a
/// member of the same group and database, whose row holds the routing
/// hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentLink {
    /// The constraint's name.
    pub fk: String,
    /// The child's foreign-key columns, lowercase, in key order.
    pub columns: Vec<String>,
    /// Their positions among the child's visible columns (`INSERT`
    /// without a column list).
    pub ordinals: Vec<Option<usize>>,
    /// The parent table, lowercase.
    pub table: String,
    /// The parent's referenced columns, lowercase, matching `columns`.
    pub parent_columns: Vec<String>,
    /// Every unique key of the child holds every foreign-key column, so
    /// rows that collide share a parent and a shard: upserts and
    /// `REPLACE` are exact per shard.
    pub upsert_safe: bool,
    /// The parent's public unique lookup (chapter 19) on its one
    /// referenced column, when it has one: parents are found through it
    /// instead of by asking every shard.
    pub lookup: Option<Arc<LookupRef>>,
    /// The referenced columns hold a unique key of the parent, so a join
    /// on the foreign key pairs each row with its own parent: such a join
    /// is co-located (chapter 15's `colocation_safe`, through a parent
    /// chain).
    pub parent_unique: bool,
}

/// A unique lookup index of a sharded table (chapter 19), as the router
/// uses it: writes claim and release its values in the write's
/// transaction (`sprig_txn::lookup::DistTxn`), and reads, updates and
/// deletes with an equality on its column go to the shards its rows
/// name.
#[derive(Debug, Clone)]
pub struct LookupRef {
    /// The index (columns, owner primary key, lookup group, state).
    pub index: sprig_shard::lookup::LookupIndex,
    /// Its value column, lowercase.
    pub column: String,
    /// How a literal of the column converts.
    pub class: KeyClass,
    /// The owner's primary-key columns, lowercase, in key order.
    pub pk: Vec<String>,
    /// The value column's and the primary-key columns' positions among
    /// the visible columns (`INSERT` without a column list).
    pub ordinals: Vec<Option<usize>>,
    /// Where lookup rows and owner rows live.
    pub placement: Arc<sprig_shard::lookup::MapPlacement>,
}

impl PartialEq for LookupRef {
    fn eq(&self, o: &Self) -> bool {
        self.index == o.index
            && self.placement.lookup_map == o.placement.lookup_map
            && self.placement.owner_map == o.placement.owner_map
    }
}

impl Eq for LookupRef {}

impl LookupRef {
    /// Whether reads may route through it (`public`; a `write_only` or
    /// `backfilling` index is maintained but not complete).
    #[must_use]
    pub fn routes(&self) -> bool {
        self.index.state == sprig_shard::lookup::LookupState::Public
    }

    /// The class of key part `i` of the lookup's value or the owner's
    /// primary key, as `sprig-shard` normalizes it.
    #[must_use]
    pub fn pk_class(&self, i: usize) -> Option<sprig_shard::KeyClass> {
        self.index.owner_pk.get(i).map(|c| c.class)
    }
}

impl TableEntry {
    /// The key column of a sharded table.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        match &self.kind {
            TableKind::Sharded { key, .. } if !key.is_empty() => Some(key),
            _ => None,
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

    /// The foreign keys the gateway keeps in the logical database whose
    /// default schema is `schema` (chapter 19, R3), if any.
    fn fk_graph(&self, schema: &str) -> Option<Arc<crate::fk::FkGraph>> {
        let _ = schema;
        None
    }
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
    /// Per default schema, the foreign keys the gateway keeps.
    fks: HashMap<String, Arc<crate::fk::FkGraph>>,
}

impl StaticTopology {
    /// Adds `entry` under the default schema `schema`.
    pub fn insert(&mut self, schema: &str, entry: TableEntry) {
        self.words.insert(entry.name.clone());
        self.tables
            .insert((schema.to_ascii_lowercase(), entry.name.clone()), entry);
    }

    /// Sets the foreign keys of the database whose default schema is
    /// `schema`.
    pub fn set_fk_graph(&mut self, schema: &str, graph: crate::fk::FkGraph) {
        self.fks
            .insert(schema.to_ascii_lowercase(), Arc::new(graph));
    }

    /// Whether no table is known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Every table, with the default schema it is known under.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &TableEntry)> {
        self.tables.iter().map(|((s, _), e)| (s.as_str(), e))
    }

    /// Every shard of the sharded tables under the default schema
    /// `schema`, each schema once, by group and then shard: where chapter
    /// 21 deploys a database's routines, events and views (added by r1ddl).
    #[must_use]
    pub fn shards_under(&self, schema: &str) -> Vec<ShardLoc> {
        let schema = schema.to_ascii_lowercase();
        let mut groups: Vec<&Arc<Group>> = self
            .tables
            .iter()
            .filter(|((s, _), e)| *s == schema && !e.is_reference())
            .map(|(_, e)| &e.group)
            .collect();
        groups.sort_by_key(|g| g.id);
        let mut out: Vec<ShardLoc> = Vec::new();
        for g in groups {
            for sh in &g.shards {
                if !out.iter().any(|o| o.schema == sh.schema) {
                    out.push(sh.clone());
                }
            }
        }
        out
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

    fn fk_graph(&self, schema: &str) -> Option<Arc<crate::fk::FkGraph>> {
        self.fks.get(&schema.to_ascii_lowercase()).cloned()
    }
}

/// A JSON description of tenants' layouts, per tenant name: groups and
/// tables (unit tests and tools; the gateway reads the catalog).
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
    pub key_ordinal: Option<usize>,
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
                    epoch: 1,
                    held_until_ms: None,
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
                    db: 0,
                    version: 0,
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
                    lookups: Vec::new(),
                    lookup_gap: false,
                    columns: Vec::new(),
                    fill_rk: false,
                    parent: None,
                    auto_inc: None,
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

    #[test]
    fn shards_under_a_schema_list_each_shard_once() {
        let f: TopologyFile = serde_json::from_str(
            r#"{"tenants": {"a": {"groups": [
                {"id": 9, "key_class": "int", "shards": [
                  {"shard": 1, "schema": "t1_d1_g9_s1", "cluster": "c1"},
                  {"shard": 2, "schema": "t1_d1_g9_s2", "cluster": "c2"}]},
                {"id": 8, "key_class": "int", "shards": [
                  {"shard": 1, "schema": "t1_d1_g8_s1", "cluster": "c1"}]}],
              "tables": [{"schema": "t1_d1_g1_s1", "table": "orders", "group": 9, "key": "cid"},
                         {"schema": "t1_d1_g1_s1", "table": "items", "group": 9, "key": "cid"},
                         {"schema": "t1_d1_g1_s1", "table": "tags", "group": 8, "key": "id"},
                         {"schema": "t1_d2_g1_s1", "table": "x", "group": 8, "key": "id"}]}}}"#,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let t = f.tenants["a"].build().unwrap_or_else(|e| panic!("{e}"));
        let s: Vec<String> = t
            .shards_under("T1_D1_G1_S1")
            .into_iter()
            .map(|l| l.schema)
            .collect();
        assert_eq!(s, ["t1_d1_g8_s1", "t1_d1_g9_s1", "t1_d1_g9_s2"]);
        assert!(t.shards_under("t1_d9_g1_s1").is_empty());
    }
}
