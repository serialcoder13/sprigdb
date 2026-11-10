//! The vocabulary of automatic indexing (chapter 13, "Interfaces"): index
//! definitions, origins, candidates and their evidence, and the canonical
//! form that names an index.
//!
//! **Canonical definition.** One line of text per definition, so equal
//! definitions hash equally everywhere:
//!
//! ```text
//! <kind>(<part>,<part>,...)[;<option>=<value>;...]
//! part := <column, lower case>[(<prefix>)][ desc]
//! ```
//!
//! for example `btree(customer_id,created_at)` or
//! `btree(sku(12),created_at desc)`. Options are listed in key order and
//! only when set. Column names are compared in lower case because MariaDB
//! column names are case-insensitive.
//!
//! **Names** ([`IndexDef::name`]): `sprig_auto_<8 hex>`, the hex being the
//! first (most significant) 32 bits of `xxh3_64(object_id ‖ canonical)`,
//! with the object id as 8 bytes little-endian. The definition hash stored
//! in `auto_index_candidates.def_hash` ([`IndexDef::def_hash`]) is
//! `xxh3_64(canonical)` without the object, so the same definition on two
//! tables can be compared.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_common::names::auto_index;
use twox_hash::XxHash3_64;

/// An `auto_indexes.index_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AutoIndexId(pub u64);

impl fmt::Display for AutoIndexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// What an index is made of. Only [`IndexKind::BTree`] is generated in R0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    /// A MariaDB secondary B-tree index.
    BTree,
    /// A document side table (R5).
    SideTable,
    /// A field column plus its index (R5, R6).
    FieldColumn,
    /// A vector (ANN) index (R6).
    Vector,
    /// A `SPATIAL` index (R6).
    Spatial,
    /// An S2 side table (R6).
    S2,
    /// A global lookup index (R1).
    Lookup,
}

impl IndexKind {
    /// The text of `auto_indexes.kind`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            IndexKind::BTree => "btree",
            IndexKind::SideTable => "side_table",
            IndexKind::FieldColumn => "field_column",
            IndexKind::Vector => "vector",
            IndexKind::Spatial => "spatial",
            IndexKind::S2 => "s2",
            IndexKind::Lookup => "lookup",
        }
    }
}

/// Who made an index (chapter 13, "Index origins"). Only `Auto` indexes are
/// ever dropped automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// The primary key, `_id`.
    System,
    /// Automatic indexing.
    Auto,
    /// `CREATE INDEX`, `createIndex`.
    User,
    /// Arrived with an import or restore.
    Migrated,
}

impl Origin {
    /// The text form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Origin::System => "system",
            Origin::Auto => "auto",
            Origin::User => "user",
            Origin::Migrated => "migrated",
        }
    }
}

/// A recommendation that is never applied automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Advice {
    /// A wildcard index (R5).
    Wildcard,
    /// A search index (R5).
    Search,
    /// Dropping an unused `migrated` index (R4).
    DropMigrated,
}

/// One key part.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyPart {
    /// The column, in lower case.
    pub column: String,
    /// A prefix length in characters, for long strings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<u16>,
    /// A descending key part (MariaDB 10.8+), only for mixed-direction
    /// `ORDER BY`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub desc: bool,
}

impl KeyPart {
    /// An ascending, whole-column part.
    #[must_use]
    pub fn col(column: &str) -> Self {
        Self {
            column: column.to_ascii_lowercase(),
            prefix: None,
            desc: false,
        }
    }

    /// The same part, descending.
    #[must_use]
    pub fn desc(mut self) -> Self {
        self.desc = true;
        self
    }

    /// The part's text in the canonical definition and in DDL (unquoted).
    fn canonical(&self, out: &mut String) {
        out.push_str(&self.column);
        if let Some(p) = self.prefix {
            let _ = write!(out, "({p})");
        }
        if self.desc {
            out.push_str(" desc");
        }
    }
}

/// Options of non-B-tree kinds: vector `M` and distance, field-column
/// class. Empty for B-trees.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexOptions(pub BTreeMap<String, String>);

/// An index definition on one catalog object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexDef {
    /// The catalog object (chapter 08).
    pub object: ObjectId,
    /// What it is made of.
    pub kind: IndexKind,
    /// Key parts, in order.
    pub parts: Vec<KeyPart>,
    /// Kind-specific options.
    #[serde(default, skip_serializing_if = "is_empty_options")]
    pub options: IndexOptions,
}

fn is_empty_options(o: &IndexOptions) -> bool {
    o.0.is_empty()
}

impl IndexDef {
    /// A B-tree over `columns`, ascending.
    #[must_use]
    pub fn btree(object: ObjectId, columns: &[&str]) -> Self {
        Self {
            object,
            kind: IndexKind::BTree,
            parts: columns.iter().map(|c| KeyPart::col(c)).collect(),
            options: IndexOptions::default(),
        }
    }

    /// The canonical definition (module docs).
    #[must_use]
    pub fn canonical(&self) -> String {
        let mut out = String::with_capacity(16 + 16 * self.parts.len());
        out.push_str(self.kind.as_str());
        out.push('(');
        for (i, p) in self.parts.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            p.canonical(&mut out);
        }
        out.push(')');
        for (k, v) in &self.options.0 {
            let _ = write!(out, ";{k}={v}");
        }
        out
    }

    /// `xxh3_64(canonical)`: `auto_index_candidates.def_hash`.
    #[must_use]
    pub fn def_hash(&self) -> u64 {
        XxHash3_64::oneshot(self.canonical().as_bytes())
    }

    /// The first 32 bits of `xxh3_64(object_id LE ‖ canonical)`.
    #[must_use]
    pub fn name_hash(&self) -> u32 {
        let canonical = self.canonical();
        let mut buf = Vec::with_capacity(8 + canonical.len());
        buf.extend_from_slice(&self.object.0.to_le_bytes());
        buf.extend_from_slice(canonical.as_bytes());
        let h = XxHash3_64::oneshot(&buf);
        u32::try_from(h >> 32).unwrap_or(u32::MAX)
    }

    /// `sprig_auto_<8 hex>`: the same on every shard and every retry.
    #[must_use]
    pub fn name(&self) -> String {
        auto_index(self.name_hash())
    }

    /// The column list in DDL form, each identifier quoted:
    /// `` `customer_id`, `sku`(12), `created_at` DESC ``.
    #[must_use]
    pub fn ddl_columns(&self) -> String {
        let mut out = String::new();
        for (i, p) in self.parts.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&sprig_common::names::quote(&p.column));
            if let Some(n) = p.prefix {
                let _ = write!(out, "({n})");
            }
            if p.desc {
                out.push_str(" DESC");
            }
        }
        out
    }

    /// The column names, in order.
    pub fn columns(&self) -> impl Iterator<Item = &str> + '_ {
        self.parts.iter().map(|p| p.column.as_str())
    }

    /// The index clause after `ADD`, for this kind (chapter 13, "Other
    /// models"; R6 round 3): `INDEX`, `SPATIAL INDEX` or `VECTOR INDEX`,
    /// then `IF NOT EXISTS <name> (<columns>)` and, for vectors,
    /// `M=<m> DISTANCE=<d>`.
    #[must_use]
    pub fn add_clause(&self) -> String {
        self.add_clause_named(&self.name())
    }

    /// [`Self::add_clause`] under another name (validation copies).
    #[must_use]
    pub fn add_clause_named(&self, name: &str) -> String {
        let kw = match self.kind {
            IndexKind::Spatial => "SPATIAL INDEX",
            IndexKind::Vector => "VECTOR INDEX",
            _ => "INDEX",
        };
        let mut out = format!(
            "{kw} IF NOT EXISTS {} ({})",
            sprig_common::names::quote(name),
            self.ddl_columns()
        );
        if self.kind == IndexKind::Vector {
            if let Some(m) = self.options.0.get("m") {
                let _ = write!(out, " M={m}");
            }
            if let Some(d) = self.options.0.get("distance") {
                let _ = write!(out, " DISTANCE={d}");
            }
        }
        out
    }

    /// Whether `self`'s parts are a prefix of `other`'s (same columns,
    /// prefixes and directions).
    #[must_use]
    pub fn is_prefix_of(&self, other: &IndexDef) -> bool {
        self.parts.len() <= other.parts.len() && other.parts.starts_with(&self.parts)
    }
}

/// How a candidate serves a fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// It was generated for the fingerprint.
    Primary,
    /// A shorter candidate for the fingerprint was merged into it.
    Merged,
}

/// Why a part is where it is in a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartSource {
    /// An equality, `IS NULL`, short `IN` list or join column.
    Eq,
    /// The one range part.
    Range,
    /// An `ORDER BY` column.
    Order,
    /// A `GROUP BY` column.
    Group,
    /// Added for covering.
    Cover,
}

/// What the generator saw, kept with the candidate and printed by
/// `sprigctl explain`. Column values never appear here, only selectivities.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    /// The source of each part, in part order.
    pub parts: Vec<PartSource>,
    /// The selectivity used for each predicate column, by column.
    pub selectivity: BTreeMap<String, f64>,
    /// Where each selectivity came from (`histogram`, `avg_frequency`,
    /// `index_stats`, `default`), by column.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sel_source: BTreeMap<String, String>,
    /// Rows the generator expects one run to read through the index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries_per_run: Option<f64>,
    /// The candidate relies on the primary-key order `InnoDB` appends to a
    /// secondary key (an `ORDER BY` on the primary key after equalities).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pk_order: bool,
    /// Covering columns needed by the fingerprints it serves, beyond its
    /// own parts and the primary key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<String>,
    /// Free-form notes: variants, plans, the `EXPLAIN` check.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// A proposed index with the fingerprints it serves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    /// The definition.
    pub def: IndexDef,
    /// It holds every column the fingerprints it serves read on its table.
    pub covering: bool,
    /// Fingerprints served, and how.
    pub serves: Vec<(Fingerprint, Role)>,
    /// What it was built from.
    pub evidence: Evidence,
}

impl Candidate {
    /// The fingerprints the candidate was generated for.
    pub fn primaries(&self) -> impl Iterator<Item = Fingerprint> + '_ {
        self.serves
            .iter()
            .filter(|(_, r)| *r == Role::Primary)
            .map(|(f, _)| *f)
    }

    /// Whether it serves `f`.
    #[must_use]
    pub fn serves_fp(&self, f: Fingerprint) -> bool {
        self.serves.iter().any(|(g, _)| *g == f)
    }

    /// Number of leading equality parts.
    #[must_use]
    pub fn eq_len(&self) -> usize {
        self.evidence
            .parts
            .iter()
            .take_while(|s| **s == PartSource::Eq)
            .count()
    }
}

/// An index that already exists on the object, from the shards'
/// `information_schema.STATISTICS` and the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexInfo {
    /// The name in MariaDB.
    pub name: String,
    /// Key parts.
    pub parts: Vec<KeyPart>,
    /// `UNIQUE` (including the primary key).
    pub unique: bool,
    /// Who made it.
    pub origin: Origin,
    /// `IGNORED` (retiring): doesn't count for coverage.
    pub ignored: bool,
    /// For automatic indexes: the state is `active` (or past rollout). An
    /// automatic index still validating or rolling out doesn't cover.
    pub usable: bool,
}

impl IndexInfo {
    /// Whether the index counts as existing for coverage checks: user,
    /// system and migrated indexes, and usable automatic ones; never an
    /// `IGNORED` one.
    #[must_use]
    pub fn counts(&self) -> bool {
        !self.ignored && (self.origin != Origin::Auto || self.usable)
    }

    /// The column names, in order.
    pub fn columns(&self) -> impl Iterator<Item = &str> + '_ {
        self.parts.iter().map(|p| p.column.as_str())
    }
}

/// The text form of a fingerprint list, as `auto_index_candidates.fingerprints`
/// stores it: `[{"id": "<16 hex>", "role": "primary"}]`.
#[must_use]
pub fn fingerprints_json(serves: &[(Fingerprint, Role)]) -> serde_json::Value {
    serde_json::Value::Array(
        serves
            .iter()
            .map(|(f, r)| {
                serde_json::json!({
                    "id": f.to_string(),
                    "role": match r { Role::Primary => "primary", Role::Merged => "merged" },
                })
            })
            .collect(),
    )
}

/// Parses [`fingerprints_json`]'s form.
#[must_use]
pub fn parse_fingerprints(v: &serde_json::Value) -> Option<Vec<(Fingerprint, Role)>> {
    v.as_array()?
        .iter()
        .map(|e| {
            let id = e.get("id")?.as_str()?.parse::<Fingerprint>().ok()?;
            let role = match e.get("role")?.as_str()? {
                "primary" => Role::Primary,
                "merged" => Role::Merged,
                _ => return None,
            };
            Some((id, role))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn canonical_form_is_stable_and_lower_case() {
        let mut d = IndexDef::btree(ObjectId(1201), &["Customer_ID", "created_at"]);
        assert_eq!(d.canonical(), "btree(customer_id,created_at)");
        d.parts[1] = KeyPart::col("created_at").desc();
        d.parts[0].prefix = Some(12);
        assert_eq!(d.canonical(), "btree(customer_id(12),created_at desc)");
        d.options.0.insert("m".into(), "8".into());
        assert_eq!(d.canonical(), "btree(customer_id(12),created_at desc);m=8");
        assert_eq!(
            d.ddl_columns(),
            "`customer_id`(12), `created_at` DESC".to_owned()
        );
    }

    #[test]
    fn name_depends_on_object_and_definition_only() {
        let a = IndexDef::btree(ObjectId(7), &["a", "b"]);
        let b = IndexDef::btree(ObjectId(7), &["A", "B"]);
        let c = IndexDef::btree(ObjectId(8), &["a", "b"]);
        assert_eq!(a.name(), b.name());
        assert_ne!(a.name(), c.name());
        assert_eq!(a.def_hash(), c.def_hash());
        assert!(sprig_common::names::is_auto_index(&a.name()));
        assert_eq!(
            sprig_common::names::parse_auto_index(&a.name()),
            Some(a.name_hash())
        );
        // The hex is the top 32 bits of xxh3_64(object LE ‖ canonical).
        let mut buf = 7u64.to_le_bytes().to_vec();
        buf.extend_from_slice(b"btree(a,b)");
        let h = XxHash3_64::oneshot(&buf);
        assert_eq!(a.name(), format!("sprig_auto_{:08x}", h >> 32));
    }

    #[test]
    fn fingerprint_list_round_trips() {
        let serves = vec![
            (Fingerprint(0x9c41_d2e0_7b3a_5f18), Role::Primary),
            (Fingerprint(1), Role::Merged),
        ];
        let j = fingerprints_json(&serves);
        assert_eq!(j[0]["id"], "9c41d2e07b3a5f18");
        assert_eq!(parse_fingerprints(&j).unwrap(), serves);
    }
}
