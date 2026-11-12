//! MongoDB shapes in automatic indexing (chapter 13, "Other models",
//! documents; R5; decisions S130-S133).
//!
//! A MongoDB command's fingerprint (`sprig_workload::fingerprint::mongo`)
//! names the collection's table and one `json_path` predicate per
//! top-level filter condition: `func` `mongo` for a path no document has
//! held an array on, `mongo_multikey` for one that crosses an array, and
//! `cmp` the condition. [`candidates`] turns the scalar ones into a
//! **document index**: chapter 31's index on field columns, built the way
//! `createIndexes` builds one, so the document engine plans with it like a
//! user index once it is registered in `doc_indexes` with origin `auto`.
//!
//! ```text
//! paths := equalities (=, $in, null) by path, then the sort, then one
//!          range: MongoDB's equality-sort-range rule, at most MAX_PATHS
//! each path p := the pair of field columns of chapter 31
//!          (_sprig_p_<slug>_c, _n, _s), descending for a descending sort
//! definition := field_column(c1, n1, s1, c2, ...); options doc.<i> = path
//! ```
//!
//! The columns are `VIRTUAL`: builds add them instantly first
//! ([`add_fields_clause`]), then build the index online; the index costs
//! every write that changes `doc`. A path crossing an array would need
//! chapter 31's side table (a multikey index), which isn't built: such
//! shapes are skipped with the reason `doc_multikey`.
//!
//! **Replay.** A MongoDB sample is the condition values by slot, not SQL.
//! [`replay_sql`] writes the statement the engine sends once the index
//! is registered: `SELECT _id FROM <db>.<table> WHERE` the candidate's
//! paths compared on their field columns (class and value), `LIMIT 101`
//! (the engine's first batch). Conditions on paths the candidate doesn't
//! hold are left out; a fingerprint none of whose paths it holds isn't
//! replayed (the engine wouldn't use the index for it). The replay's A
//! side (`IGNORE INDEX`) reads the same columns without the index, which
//! is the scan the engine does today.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use sprig_common::ids::ObjectId;
use sprig_workload::fingerprint::LiteralKind;
use sprig_workload::fingerprint::mongo::{DOC_COLUMN, FUNC_MULTIKEY, FUNC_SCALAR};
use sprig_workload::shape::{PredOp, Shape, SortDir};
use sprig_workload::window::{SampleValue, SampledLiterals, SampledSlot};

use super::candidates::{FpInput, MODEL_READ_FACTOR};
use super::model::{Candidate, Evidence, IndexDef, IndexKind, IndexOptions, KeyPart, PartSource, Role};
use super::stats::TableStats;

/// The note that marks a document candidate.
pub const NOTE: &str = "variant:doc_index";
/// The option key prefix naming a candidate's paths: `doc.<i>` = path.
pub const OPTION: &str = "doc.";
/// The most paths one candidate holds: three pairs are 1,947 key bytes
/// (1 + 8 + 640 each), under `InnoDB`'s 3,072.
pub const MAX_PATHS: usize = 3;
/// Rows the engine's first batch asks for.
pub const FIRST_BATCH: u32 = 101;

/// Whether `stats` describe a collection table (chapter 31: `_id`, `doc`,
/// `_types`, `_v`).
#[must_use]
pub fn is_collection_table(stats: &TableStats) -> bool {
    ["_id", DOC_COLUMN, "_types", "_v"]
        .iter()
        .all(|c| stats.columns.contains_key(*c))
}

/// Whether `def` is a document index.
#[must_use]
pub fn is_doc(def: &IndexDef) -> bool {
    def.kind == IndexKind::FieldColumn && def.options.0.keys().any(|k| k.starts_with(OPTION))
}

/// Whether `c` is a document candidate.
#[must_use]
pub fn is_doc_candidate(c: &Candidate) -> bool {
    is_doc(&c.def) || c.evidence.notes.iter().any(|n| n == NOTE)
}

/// The paths of a document index with their direction, in key order.
#[must_use]
pub fn paths_of(def: &IndexDef) -> Vec<(String, i32)> {
    if !is_doc(def) {
        return Vec::new();
    }
    let mut v: Vec<(usize, String)> = def
        .options
        .0
        .iter()
        .filter_map(|(k, p)| Some((k.strip_prefix(OPTION)?.parse().ok()?, p.clone())))
        .collect();
    v.sort();
    v.into_iter()
        .map(|(_, p)| {
            let (c, _, _) = sprig_doc::meta::columns(&p);
            let desc = def
                .parts
                .iter()
                .any(|k| k.column == c.to_ascii_lowercase() && k.desc);
            (p, if desc { -1 } else { 1 })
        })
        .collect()
}

/// The field columns of a document index, `(class, number, string)` per
/// path, flattened in key order.
#[must_use]
pub fn columns_of(def: &IndexDef) -> Vec<String> {
    paths_of(def)
        .iter()
        .flat_map(|(p, _)| {
            let (c, n, s) = sprig_doc::meta::columns(p);
            [c, n, s]
        })
        .collect()
}

/// The `ADD COLUMN IF NOT EXISTS` clauses of a document index's field
/// columns; `None` for any other definition.
#[must_use]
pub fn add_fields_clause(def: &IndexDef) -> Option<String> {
    let paths = paths_of(def);
    (!paths.is_empty()).then(|| {
        paths
            .iter()
            .map(|(p, _)| sprig_doc::meta::add_columns_clause(p))
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// The definition of a document index over `paths` on `object`.
#[must_use]
pub fn definition(object: ObjectId, paths: &[(String, i32)]) -> IndexDef {
    let mut options = BTreeMap::new();
    let mut parts = Vec::new();
    for (i, (p, dir)) in paths.iter().enumerate() {
        options.insert(format!("{OPTION}{}", i + 1), p.clone());
        let (c, n, s) = sprig_doc::meta::columns(p);
        for col in [c, n, s] {
            let k = KeyPart::col(&col);
            parts.push(if *dir < 0 { k.desc() } else { k });
        }
    }
    IndexDef {
        object,
        kind: IndexKind::FieldColumn,
        parts,
        options: IndexOptions(options),
    }
}

/// Whether the shape filters on a path crossing an array of `alias`.
#[must_use]
pub fn has_multikey(shape: &Shape, alias: &str) -> bool {
    shape.predicates_on(alias).any(|p| {
        matches!(&p.op, PredOp::JsonPath { func: Some(f), .. } if f == FUNC_MULTIKEY)
    })
}

fn scalar_conditions<'a>(
    shape: &'a Shape,
    alias: &'a str,
) -> impl Iterator<Item = (&'a str, &'a PredOp, &'a [u32])> + 'a {
    shape.predicates_on(alias).filter_map(|p| match &p.op {
        PredOp::JsonPath {
            path,
            func: Some(f),
            cmp: Some(c),
        } if f == FUNC_SCALAR && p.column.col == DOC_COLUMN => {
            Some((path.as_str(), &**c, p.args.as_slice()))
        }
        _ => None,
    })
}

/// The document candidate of `f` on collection table `alias` (module
/// docs). Nothing when it filters on `_id` by equality (the primary key
/// serves it) or on no scalar path.
#[must_use]
pub fn candidates(f: &FpInput, alias: &str, object: ObjectId, stats: &TableStats) -> Vec<Candidate> {
    if !is_collection_table(stats) {
        return Vec::new();
    }
    let mut eq: Vec<&str> = Vec::new();
    let mut range: Vec<&str> = Vec::new();
    for (path, cmp, _) in scalar_conditions(&f.shape, alias) {
        match cmp {
            PredOp::Eq | PredOp::In { .. } | PredOp::IsNull => eq.push(path),
            PredOp::Range { .. } => range.push(path),
            _ => {}
        }
    }
    if eq.contains(&"_id") {
        return Vec::new();
    }
    eq.sort_unstable();
    eq.dedup();
    let mut paths: Vec<(String, i32, PartSource)> = eq
        .into_iter()
        .take(MAX_PATHS)
        .map(|p| (p.to_owned(), 1, PartSource::Eq))
        .collect();
    // Equality, sort, range (MongoDB's rule): a sort the index gives
    // spares the engine's in-memory sort.
    for o in f
        .shape
        .order_by
        .iter()
        .filter(|o| o.column.table.as_deref() == Some(alias))
    {
        if paths.len() >= MAX_PATHS || paths.iter().any(|(p, ..)| *p == o.column.col) {
            break;
        }
        if o.column.col == "_id" {
            break;
        }
        paths.push((
            o.column.col.clone(),
            if o.dir == SortDir::Desc { -1 } else { 1 },
            PartSource::Order,
        ));
    }
    if paths.len() < MAX_PATHS
        && let Some(r) = range
            .into_iter()
            .find(|r| *r != "_id" && !paths.iter().any(|(p, ..)| p == r))
    {
        paths.push((r.to_owned(), 1, PartSource::Range));
    }
    if paths.iter().all(|(_, _, s)| *s == PartSource::Order) {
        // A sort alone: the engine's sort over a full read is what it
        // already does; nothing to filter by.
        return Vec::new();
    }
    let def = definition(
        object,
        &paths
            .iter()
            .map(|(p, d, _)| (p.clone(), *d))
            .collect::<Vec<_>>(),
    );
    let sources = paths.iter().flat_map(|(_, _, s)| [*s, *s, *s]).collect();
    vec![Candidate {
        def,
        covering: false,
        serves: vec![(f.id, Role::Primary)],
        evidence: Evidence {
            parts: sources,
            entries_per_run: Some(f.returned_per_run.max(1.0) * MODEL_READ_FACTOR),
            notes: vec![NOTE.to_owned()],
            ..Evidence::default()
        },
    }]
}

/// `S` of a document index: per path a class byte, a `DOUBLE` and the
/// string column (the table's average document length, at most its 160
/// characters, or 16 bytes when unknown), plus the primary key.
#[must_use]
pub fn bytes(def: &IndexDef, stats: &TableStats) -> f64 {
    let doc = stats.avg_length(DOC_COLUMN);
    let s = if doc > 0.0 { doc.min(160.0) } else { 16.0 };
    #[allow(clippy::cast_precision_loss)]
    let paths = paths_of(def).len() as f64;
    stats.rows() * (paths * (1.0 + 8.0 + s) + stats.pk_length() + 14.0) / 0.75
}

/// The class code (chapter 31's MongoDB order) and the field column a
/// sampled value compares on: `false` for the number column, `true` for
/// the string column.
fn class_of(v: &SampleValue) -> Option<(u8, bool)> {
    Some(match v.kind {
        LiteralKind::Number => (30, false),
        LiteralKind::String => (40, true),
        LiteralKind::Hex => (80, true),
        LiteralKind::Bool => (90, false),
        LiteralKind::Temporal => (100, true),
        _ => return None,
    })
}

fn bool_number(v: &SampleValue) -> Vec<u8> {
    match v.text.as_slice() {
        b"TRUE" => b"1".to_vec(),
        b"FALSE" => b"0".to_vec(),
        t => t.to_vec(),
    }
}

fn quote(id: &str) -> String {
    sprig_common::names::quote(id)
}

fn eq_text(path: &str, v: &SampleValue) -> Option<String> {
    let (class, string) = class_of(v)?;
    let (c, n, s) = sprig_doc::meta::columns(path);
    let col = if string { s } else { n };
    let val = if v.kind == LiteralKind::Bool {
        bool_number(v)
    } else {
        v.text.clone()
    };
    Some(format!(
        "({} = {class} AND {} = {})",
        quote(&c),
        quote(&col),
        String::from_utf8_lossy(&val)
    ))
}

/// The statement replay runs for a MongoDB fingerprint's sample under
/// document index `def` (module docs), in logical names; `None` when the
/// index holds none of its paths or a value has no field column.
#[must_use]
pub fn replay_sql(shape: &Shape, sample: &SampledLiterals, def: &IndexDef) -> Option<String> {
    let t = shape.main_table()?;
    let db = t.db.as_deref()?;
    let held: Vec<String> = paths_of(def).into_iter().map(|(p, _)| p).collect();
    let mut conds = Vec::new();
    for (path, cmp, args) in scalar_conditions(shape, &t.alias) {
        if !held.iter().any(|h| h == path) {
            continue;
        }
        let slot = sample.slots.get(*args.first()? as usize);
        let one = || match slot {
            Some(SampledSlot::One(v)) => Some(v),
            _ => None,
        };
        let (c, n, s) = sprig_doc::meta::columns(path);
        let cond = match cmp {
            PredOp::Eq => eq_text(path, one()?)?,
            PredOp::IsNull => format!("{} = 20", quote(&c)),
            PredOp::In { .. } => {
                let Some(SampledSlot::List(vs)) = slot else {
                    return None;
                };
                let parts: Option<Vec<String>> = vs.iter().map(|v| eq_text(path, v)).collect();
                let parts = parts.filter(|p| !p.is_empty())?;
                format!("({})", parts.join(" OR "))
            }
            PredOp::Range { lo, .. } => {
                let v = one()?;
                let (class, string) = class_of(v)?;
                let col = if string { &s } else { &n };
                let op = if *lo { ">=" } else { "<=" };
                format!(
                    "({} = {class} AND {} {op} {})",
                    quote(&c),
                    quote(col),
                    String::from_utf8_lossy(&v.text)
                )
            }
            _ => continue,
        };
        conds.push(cond);
    }
    if conds.is_empty() {
        return None;
    }
    let mut sql = format!(
        "SELECT `_id` FROM {}.{} WHERE {}",
        quote(db),
        quote(&t.table),
        conds.join(" AND ")
    );
    let order: Vec<String> = shape
        .order_by
        .iter()
        .filter(|o| held.iter().any(|h| *h == o.column.col))
        .flat_map(|o| {
            let (c, n, s) = sprig_doc::meta::columns(&o.column.col);
            let d = if o.dir == SortDir::Desc { " DESC" } else { "" };
            [c, n, s].map(|x| format!("{}{d}", quote(&x)))
        })
        .collect();
    if !order.is_empty() {
        let _ = write!(sql, " ORDER BY {}", order.join(", "));
    }
    let _ = write!(sql, " LIMIT {FIRST_BATCH}");
    Some(sql)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use sprig_common::ids::{DbId, Fingerprint};
    use sprig_workload::fingerprint::mongo::{MongoInput, MongoPred, fingerprint, number, quoted};

    use super::*;
    use crate::autoindex::stats::ColumnInfo;

    fn stats() -> TableStats {
        let mut s = TableStats::default();
        s.table = "orders".into();
        for c in ["_id", "doc", "_types", "_v"] {
            s.columns.insert(
                c.into(),
                ColumnInfo {
                    name: c.into(),
                    data_type: "longtext".into(),
                    class: None,
                    charset: None,
                    collation: None,
                    nullable: false,
                    max_bytes: None,
                    lob: c == "doc",
                    generated: false,
                },
            );
        }
        s
    }

    fn fp(preds: Vec<MongoPred>, sort: Vec<(String, i32)>) -> (FpInput, SampledLiterals) {
        let (f, lits) = fingerprint(&MongoInput {
            db: "shop",
            db_id: DbId(2),
            coll: "orders",
            table: "orders",
            object_id: Some(ObjectId(77)),
            preds,
            sort,
            ..MongoInput::default()
        });
        (
            FpInput {
                id: Fingerprint(f.id.0),
                shape: Arc::clone(&f.shape),
                runs: 100,
                mean_ms: 300.0,
                p95_ms: 400.0,
                examined_per_run: 50_000.0,
                returned_per_run: 2.0,
                affected_per_run: 0.0,
                limit: None,
                in_list_max: None,
                samples: Vec::new(),
                fast_only: false,
            },
            lits,
        )
    }

    fn eq(path: &str, v: SampleValue) -> MongoPred {
        MongoPred {
            path: path.into(),
            cmp: PredOp::Eq,
            multikey: false,
            values: vec![v],
        }
    }

    #[test]
    fn equality_sort_range() {
        let (f, lits) = fp(
            vec![
                MongoPred {
                    path: "qty".into(),
                    cmp: PredOp::Range { lo: true, hi: false },
                    multikey: false,
                    values: vec![number("5")],
                },
                eq("status", quoted(LiteralKind::String, "open")),
                MongoPred {
                    path: "tags".into(),
                    cmp: PredOp::Eq,
                    multikey: true,
                    values: vec![quoted(LiteralKind::String, "x")],
                },
            ],
            vec![("createdAt".into(), -1)],
        );
        let c = candidates(&f, "orders", ObjectId(77), &stats());
        assert_eq!(c.len(), 1);
        let d = &c[0].def;
        assert!(is_doc(d));
        assert_eq!(
            paths_of(d),
            vec![
                ("status".to_owned(), 1),
                ("createdAt".to_owned(), -1),
                ("qty".to_owned(), 1)
            ]
        );
        assert_eq!(d.parts.len(), 9);
        assert_eq!(columns_of(d).len(), 9);
        assert!(add_fields_clause(d).unwrap().contains("ADD COLUMN IF NOT EXISTS"));
        assert!(has_multikey(&f.shape, "orders"));
        assert!(d.name().starts_with("sprig_auto_"));
        let sql = replay_sql(&f.shape, &lits, d).unwrap();
        let (sc, _, ss) = sprig_doc::meta::columns("status");
        assert!(sql.starts_with("SELECT `_id` FROM `shop`.`orders` WHERE "), "{sql}");
        assert!(sql.contains(&format!("(`{sc}` = 40 AND `{ss}` = 'open')")), "{sql}");
        assert!(sql.contains(">= 5)"), "{sql}");
        assert!(sql.contains(" DESC"), "{sql}");
        assert!(sql.ends_with("LIMIT 101"), "{sql}");
    }

    #[test]
    fn nothing_for_id_or_other_tables() {
        let (f, lits) = fp(vec![eq("_id", number("1"))], vec![]);
        assert!(candidates(&f, "orders", ObjectId(77), &stats()).is_empty());
        let (g, _) = fp(vec![eq("a", number("1"))], vec![]);
        let mut plain = stats();
        plain.columns.remove("_types");
        assert!(candidates(&g, "orders", ObjectId(77), &plain).is_empty());
        // An index on another path: not replayed.
        let d = definition(ObjectId(77), &[("b".into(), 1)]);
        assert!(replay_sql(&f.shape, &lits, &d).is_none());
        // A sort alone isn't a candidate.
        let (h, _) = fp(vec![], vec![("a".into(), 1)]);
        assert!(candidates(&h, "orders", ObjectId(77), &stats()).is_empty());
    }

    #[test]
    fn in_lists_bools_and_nulls() {
        let (f, lits) = fp(
            vec![
                MongoPred {
                    path: "s".into(),
                    cmp: PredOp::In { list: true },
                    multikey: false,
                    values: vec![
                        quoted(LiteralKind::String, "a"),
                        quoted(LiteralKind::String, "b"),
                    ],
                },
                eq("ok", sprig_workload::fingerprint::mongo::boolean(true)),
                MongoPred {
                    path: "gone".into(),
                    cmp: PredOp::IsNull,
                    multikey: false,
                    values: vec![],
                },
            ],
            vec![],
        );
        let c = candidates(&f, "orders", ObjectId(77), &stats());
        let sql = replay_sql(&f.shape, &lits, &c[0].def).unwrap();
        assert!(sql.contains(" OR "), "{sql}");
        assert!(sql.contains("= 90 AND"), "{sql}");
        assert!(sql.contains("` = 1)"), "{sql}");
        assert!(sql.contains("` = 20"), "{sql}");
    }
}
