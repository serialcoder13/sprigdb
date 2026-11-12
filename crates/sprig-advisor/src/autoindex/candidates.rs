//! Candidate generation (chapter 13, "Candidate generation", "Covering
//! columns", "Merging candidates and checking existing indexes"; build
//! step 4).
//!
//! For each qualifying fingerprint and each table it references, the
//! generator reads the fingerprint's [`Shape`] into a [`TableAccess`]: the
//! predicates on the table with their selectivities, its `ORDER BY` and
//! `GROUP BY`, the columns it reads, its `LIMIT`. From that it emits
//! candidates whose parts are the equality columns, most selective first,
//! then one range column, then the order or grouping columns:
//!
//! ```text
//! E := eq, is_null, IN lists of at most 32 values, join columns of the table
//! R := range, prefix, IS NOT NULL, longer IN lists
//! A := E by sel ascending ++ [r = argmin sel over R] ++ (O, or G without O)
//!      (O or G only if r is none or r = O[0] / G[0])
//! variants: E ++ O        when O, LIMIT, and r != O[0]
//!           E1 ++ O       when O, LIMIT, and E has IN lists (E1: E without them)
//!           E ++ G        when G and r != G[0]
//! drop a candidate that reads over 20% of the rows and gives no order the
//! query uses; then add_covering; then merge per table.
//! ```
//!
//! **Choices where the chapter is silent** (each also in the report):
//!
//! - An `IN` list is an equality for the index's seek but breaks the order
//!   of the columns after it (MariaDB doesn't merge the per-value ranges in
//!   order), so with an `ORDER BY ... LIMIT` the generator also emits the
//!   variant without the `IN` columns. This is how worked example 1's
//!   variant B `(customer_id, created_at)` arises: its `status IN (...)` is
//!   left as a filter.
//! - An `IN` list longer than 32 values (sampled, else `in_list_max`) is
//!   treated as a range.
//! - `ORDER BY` on the primary key after equalities needs no part: `InnoDB`'s
//!   secondary entries end with the primary key, so `(account_id)` already
//!   returns an account's rows in `id` order. Such a candidate is marked
//!   [`Evidence::pk_order`] and never merged into a longer index (worked
//!   example 2's F3).
//! - A uniform `ORDER BY ... DESC` gets ascending parts (a backward scan
//!   serves it), so `ASC` and `DESC` queries share one index; only mixed
//!   directions get descending parts.
//! - `BLOB`, `TEXT`, `JSON` and geometry columns are never key parts (they
//!   would need a prefix, which can't deliver order or cover).
//! - Write statements (`UPDATE`, `DELETE`) are never covered: they read
//!   the row anyway.

pub mod explain;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_workload::shape::{JoinKind, JoinOp, PredOp, Shape, SortDir, StmtType};

use super::model::{
    Candidate, Evidence, IndexDef, IndexInfo, IndexKind, IndexOptions, KeyPart, PartSource, Role,
};
use super::stats::{DEFAULT_SEL, Key, Param, Sel, SelSource, TableStats};

/// `IN` lists longer than this are ranges, not equalities.
pub const MAX_IN_EQ: u64 = 32;
/// Candidates reading more than this share of rows, with no order the
/// query uses, are dropped.
pub const MAX_SCAN_FRACTION: f64 = 0.20;
/// A covering variant is added only when the candidate reads more than
/// this many rows per run.
pub const COVER_MIN_ROWS: f64 = 50.0;
/// The selectivity of a filter the index can't use and statistics don't
/// describe (`<>`, `LIKE '%x'`, a function), for the early-stop estimate.
pub const RESIDUAL_SEL: f64 = 0.5;
/// The `IN` list length assumed with neither samples nor `in_list_max`.
pub const DEFAULT_IN_LEN: u64 = 3;

/// One literal of a sampled execution: a value, or a collapsed `IN` list.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// One value.
    Value(Param),
    /// An `IN (...)` list.
    List(Vec<Param>),
}

/// A qualifying fingerprint with the window statistics generation and the
/// cost model use. Chapter 12's `FpWindowStats` will convert into it.
#[derive(Debug, Clone)]
pub struct FpInput {
    /// The fingerprint.
    pub id: Fingerprint,
    /// Its shape.
    pub shape: Arc<Shape>,
    /// Runs in the window (`n_f`).
    pub runs: u64,
    /// Mean time per run, ms (`t_f`).
    pub mean_ms: f64,
    /// p95, ms.
    pub p95_ms: f64,
    /// Rows examined per run (`x_f`), from probes.
    pub examined_per_run: f64,
    /// Rows returned per run (`r_f`).
    pub returned_per_run: f64,
    /// Rows affected per run (writes).
    pub affected_per_run: f64,
    /// The `LIMIT` value from samples, if known.
    pub limit: Option<u64>,
    /// The longest `IN` list seen (`in_list_max`).
    pub in_list_max: Option<u64>,
    /// Decrypted samples' literal lists (empty without samples). Values
    /// never leave this structure; evidence keeps selectivities only.
    pub samples: Vec<Vec<Literal>>,
    /// Qualified only by the fast trigger (`large_scan`): never displaces
    /// an active automatic index.
    pub fast_only: bool,
}

/// Replaces the text values in `f`'s samples that table `alias` compares
/// with a text column by their `WEIGHT_STRING()` under the column's
/// collation ([`Param::Weight`]), so they compare with histogram bounds the
/// reader reweighed. One query per column.
///
/// # Errors
/// As [`weigh`](super::stats::weigh).
pub async fn weigh_samples<Q: mysql_async::prelude::Queryable + Send>(
    q: &mut Q,
    alias: &str,
    stats: &TableStats,
    f: &mut FpInput,
) -> sprig_common::Result<()> {
    let shape = Arc::clone(&f.shape);
    for p in shape
        .predicates
        .iter()
        .filter(|p| p.column.table.as_deref() == Some(alias))
    {
        let col = p.column.col.to_ascii_lowercase();
        let Some(info) = stats.columns.get(&col) else {
            continue;
        };
        let (Some(cs), Some(coll)) = (info.charset.as_deref(), info.collation.as_deref()) else {
            continue;
        };
        if info.class != Some(super::stats::KeyClass::Text) {
            continue;
        }
        // Every text value at the predicate's literal slots.
        let mut slots: Vec<(usize, usize, Option<usize>)> = Vec::new();
        let mut texts: Vec<String> = Vec::new();
        for (si, s) in f.samples.iter().enumerate() {
            for a in &p.args {
                let a = *a as usize;
                match s.get(a) {
                    Some(Literal::Value(Param::Text(t))) => {
                        slots.push((si, a, None));
                        texts.push(t.clone());
                    }
                    Some(Literal::List(vs)) => {
                        for (li, v) in vs.iter().enumerate() {
                            if let Param::Text(t) = v {
                                slots.push((si, a, Some(li)));
                                texts.push(t.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        if texts.is_empty() {
            continue;
        }
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let weights = super::stats::weigh(q, cs, coll, &refs).await?;
        for ((si, a, li), w) in slots.into_iter().zip(weights) {
            match (f.samples[si].get_mut(a), li) {
                (Some(Literal::Value(v)), None) => *v = Param::Weight(w),
                (Some(Literal::List(vs)), Some(li)) => vs[li] = Param::Weight(w),
                _ => {}
            }
        }
    }
    Ok(())
}

/// How a predicate column constrains an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredKind {
    /// One value per run: `=`, `IS NULL`, a one-value `IN`, a join column.
    Eq,
    /// An `IN` list of 2 to 32 values: a seek, but no order after it.
    InList,
    /// A range: `<`, `BETWEEN`, `LIKE 'x%'`, `IS NOT NULL`, a long `IN`.
    Range,
}

/// One predicate column with its selectivity.
#[derive(Debug, Clone, PartialEq)]
pub struct PredSel {
    /// The column, lower case.
    pub column: String,
    /// Its kind.
    pub kind: PredKind,
    /// Selectivity.
    pub sel: f64,
    /// Where it came from.
    pub source: SelSource,
}

/// What one fingerprint does with one table: the input of generation and
/// of the cost model's per-index estimate.
#[derive(Debug, Clone, PartialEq)]
pub struct TableAccess {
    /// The fingerprint.
    pub fp: Fingerprint,
    /// The table's alias in the shape.
    pub alias: String,
    /// The catalog object.
    pub object: ObjectId,
    /// Sargable predicates, one per column (the most selective form).
    pub preds: Vec<PredSel>,
    /// Selectivities of the other filters on the table (unsargable,
    /// `<>`, ...), for the early-stop estimate.
    pub residual: Vec<f64>,
    /// `ORDER BY` columns on this table, with equality columns removed;
    /// empty when the order involves other tables or expressions.
    pub order: Vec<(String, SortDir)>,
    /// `GROUP BY` columns on this table, with equality columns removed.
    pub group: Vec<String>,
    /// The order is the primary key's (a leading part of it, one
    /// direction).
    pub pk_order: bool,
    /// Columns of the table the statement reads (select list, filters,
    /// order, grouping, join).
    pub reads: BTreeSet<String>,
    /// `SELECT *`, or a write: no index covers it.
    pub whole_row: bool,
    /// `LIMIT k`, with `k` from samples, else rows returned per run.
    pub limit: Option<f64>,
    /// Unsargable reasons on this table, for `explain`.
    pub unsargable: Vec<String>,
}

impl TableAccess {
    fn pred(&self, column: &str) -> Option<&PredSel> {
        self.preds.iter().find(|p| p.column == column)
    }
}

/// How an index serves a [`TableAccess`].
#[derive(Debug, Clone, PartialEq)]
pub struct Usage {
    /// Leading parts used for the seek.
    pub used: usize,
    /// Selectivity of the seek.
    pub sel: f64,
    /// Entries read per run (`e_f`), after any `LIMIT` early stop.
    pub entries: f64,
    /// The index returns rows in the statement's `ORDER BY` order.
    pub delivers_order: bool,
    /// The index returns rows grouped by the `GROUP BY` columns.
    pub delivers_group: bool,
    /// Every column the statement reads is in the index (or the primary
    /// key).
    pub covering: bool,
}

/// Limits from the resolved settings (see [`policy`](super::policy)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GenSettings {
    /// `auto_index.max_cover_columns`.
    pub max_cover_columns: usize,
    /// `auto_index.max_key_bytes`.
    pub max_key_bytes: u64,
}

impl Default for GenSettings {
    fn default() -> Self {
        Self {
            max_cover_columns: 3,
            max_key_bytes: 512,
        }
    }
}

/// Why a table of a fingerprint got no candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// The fingerprint.
    pub fp: Fingerprint,
    /// The table's alias.
    pub alias: String,
    /// The catalog object, if known.
    pub object: Option<ObjectId>,
    /// `function:LOWER`, `no_sargable_predicate`, `no_statistics`, ...
    pub reason: String,
}

/// Spatial functions whose predicates a `SPATIAL INDEX` serves: the MBR
/// functions MariaDB reads the R-tree for, and the geodesic ones the
/// MySQL persona narrows with a candidate box (chapter 36).
const SPATIAL_FUNCTIONS: &[&str] = &[
    "ST_DISTANCE",
    "ST_DISTANCE_SPHERE",
    "ST_CONTAINS",
    "ST_WITHIN",
    "ST_INTERSECTS",
    "ST_OVERLAPS",
    "ST_TOUCHES",
    "ST_CROSSES",
    "ST_EQUALS",
];

/// Rows a spatial or vector index reads per row it returns: an R-tree
/// box holds about 4/π of a circle's points; an HNSW search reads its
/// candidate list. A rough figure for the cost model.
pub const MODEL_READ_FACTOR: f64 = 1.3;

/// Chapter 13's "Other models" for SQL tables (R6 round 3):
///
/// - **Spatial:** a `SPATIAL INDEX` on a `NOT NULL` geometry column that
///   a spatial predicate filters on (an MBR function, or a geodesic one
///   the MySQL persona turns into an R-tree box). A nullable column would
///   need chapter 36's companion box: no candidate.
/// - **Vector:** a `VECTOR INDEX` on a `NOT NULL` vector column a query
///   orders by `VEC_DISTANCE_*(col, ...)` with a `LIMIT`, with the
///   queries' metric and `M` 8 below a million rows, 16 above. The
///   evaluator recommends it unless `auto_index.vector_ann` is `on`.
#[must_use]
pub fn model_candidates(
    f: &FpInput,
    alias: &str,
    object: ObjectId,
    stats: &TableStats,
) -> Vec<Candidate> {
    let mut out: Vec<Candidate> = Vec::new();
    let on = |t: &Option<String>| t.as_deref() == Some(alias);
    let not_null = |c: &str, ty: &[&str]| {
        stats
            .columns
            .get(c)
            .is_some_and(|i| !i.nullable && ty.iter().any(|t| i.data_type.contains(t)))
    };
    let mut add = |kind: IndexKind, col: &str, options: IndexOptions, note: &str| {
        let def = IndexDef {
            object,
            kind,
            parts: vec![KeyPart::col(col)],
            options,
        };
        if out.iter().any(|c| c.def == def) {
            return;
        }
        let returned = f.returned_per_run.max(1.0);
        out.push(Candidate {
            def,
            covering: false,
            serves: vec![(f.id, Role::Primary)],
            evidence: Evidence {
                parts: vec![PartSource::Range],
                entries_per_run: Some(returned * MODEL_READ_FACTOR),
                notes: vec![note.to_owned()],
                ..Evidence::default()
            },
        });
    };
    let fn_of = |p: &sprig_workload::shape::Predicate| match &p.op {
        PredOp::Unsargable {
            why: sprig_workload::shape::Unsargable::Function(n),
        } => Some(n.clone()),
        _ => None,
    };
    let preds: Vec<_> = f
        .shape
        .predicates
        .iter()
        .filter(|p| on(&p.column.table))
        .collect();
    for p in &preds {
        let col = p.column.col.to_ascii_lowercase();
        let spatial = p.op == PredOp::Spatial
            || fn_of(p).is_some_and(|n| SPATIAL_FUNCTIONS.contains(&n.as_str()));
        if spatial
            && not_null(
                &col,
                &[
                    "geometry",
                    "point",
                    "polygon",
                    "linestring",
                    "multi",
                    "collection",
                ],
            )
        {
            add(
                IndexKind::Spatial,
                &col,
                IndexOptions::default(),
                "variant:spatial",
            );
        }
    }
    if f.shape.limit {
        for p in preds.iter().filter(|p| p.op == PredOp::Vector) {
            let col = p.column.col.to_ascii_lowercase();
            if !not_null(&col, &["vector", "varbinary", "binary"]) {
                continue;
            }
            let metric = preds.iter().find_map(|q| {
                let n = fn_of(q)?;
                (q.column.col.eq_ignore_ascii_case(&col) && n.starts_with("VEC_DISTANCE")).then(
                    || {
                        if n.ends_with("COSINE") {
                            "cosine"
                        } else {
                            "euclidean"
                        }
                    },
                )
            });
            let rows = stats.n_rows.or(stats.cardinality).unwrap_or(0);
            let mut o = BTreeMap::new();
            o.insert(
                "distance".to_owned(),
                metric.unwrap_or("euclidean").to_owned(),
            );
            o.insert(
                "m".to_owned(),
                if rows >= 1_000_000 { "16" } else { "8" }.to_owned(),
            );
            add(IndexKind::Vector, &col, IndexOptions(o), "variant:vector");
        }
    }
    out
}

/// Candidates of one fingerprint, and the tables that got none.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Generated {
    /// Candidates, per table, before merging.
    pub candidates: Vec<Candidate>,
    /// Tables without candidates, with the reason.
    pub skipped: Vec<Skipped>,
}

/// Chapter 13's generator interface, for one table's statistics.
pub trait CandidateGenerator: Send + Sync {
    /// The candidates `f` gets on `object`, whose statistics are `stats`
    /// and whose existing indexes are `existing` (unused by generation,
    /// which [`covered_by`] checks after merging).
    fn generate(
        &self,
        f: &FpInput,
        object: ObjectId,
        stats: &TableStats,
        existing: &[IndexInfo],
    ) -> Vec<Candidate>;
}

/// The B-tree generator.
#[derive(Debug, Clone, Copy, Default)]
pub struct Generator {
    /// Covering limits.
    pub settings: GenSettings,
}

impl CandidateGenerator for Generator {
    fn generate(
        &self,
        f: &FpInput,
        object: ObjectId,
        stats: &TableStats,
        _existing: &[IndexInfo],
    ) -> Vec<Candidate> {
        self.generate_all(f, &|o| (o == object).then_some(stats))
            .candidates
    }
}

#[allow(clippy::cast_precision_loss)]
fn f64_of(n: u64) -> f64 {
    n as f64
}

impl Generator {
    /// A generator with `settings`.
    #[must_use]
    pub fn new(settings: GenSettings) -> Self {
        Self { settings }
    }

    /// Candidates for every table of `f` whose statistics `stats_of`
    /// knows.
    pub fn generate_all<'a>(
        &self,
        f: &FpInput,
        stats_of: &dyn Fn(ObjectId) -> Option<&'a TableStats>,
    ) -> Generated {
        let mut out = Generated::default();
        if !f.shape.stmt_type.is_indexable() || f.shape.partial {
            out.skipped.push(Skipped {
                fp: f.id,
                alias: String::new(),
                object: None,
                reason: if f.shape.partial {
                    "partial_shape".into()
                } else {
                    "not_indexable".into()
                },
            });
            return out;
        }
        for t in &f.shape.tables {
            let skip = |reason: &str| Skipped {
                fp: f.id,
                alias: t.alias.clone(),
                object: t.object_id,
                reason: reason.to_owned(),
            };
            if t.derived {
                continue;
            }
            let Some(object) = t.object_id else {
                continue;
            };
            let Some(stats) = stats_of(object) else {
                out.skipped.push(skip("no_statistics"));
                continue;
            };
            let acc = access(f, &t.alias, object, stats);
            // Chapter 33's element tables (R6 round 5) get only their own
            // shapes' indexes (`super::kv`).
            // Chapter 31's collection tables (R5, S131) get only document
            // indexes, from MongoDB shapes (`super::doc`).
            if super::doc::is_collection_table(stats) {
                let cands = super::doc::candidates(f, &t.alias, object, stats);
                if cands.is_empty() {
                    out.skipped.push(skip(if super::doc::has_multikey(&f.shape, &t.alias) {
                        "doc_multikey"
                    } else {
                        "no_doc_path"
                    }));
                }
                out.candidates.extend(cands);
                continue;
            }
            let cands = if super::kv::is_element_table(stats) {
                super::kv::candidates(f, &t.alias, object, stats)
            } else {
                let mut cands = self.for_table(&acc, stats);
                // Chapter 13's other models (R6 round 3): spatial and vector.
                cands.extend(model_candidates(f, &t.alias, object, stats));
                // Value fields of key-value keys (R6 round 6, decision K3).
                cands.extend(super::kv::field_candidates(f, &t.alias, object, stats));
                cands
            };
            if cands.is_empty() && super::kv::value_field(f, &t.alias, stats) {
                // A path MariaDB doesn't substitute (`LIKE`, `->`, a number).
                out.skipped.push(skip("kv_value_field"));
            } else if cands.is_empty() {
                let reason = acc
                    .unsargable
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "no_sargable_predicate".into());
                out.skipped.push(skip(&reason));
            }
            out.candidates.extend(cands);
        }
        out
    }

    /// The candidates of one table access (chapter 13's `generate`).
    #[must_use]
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the chapter's `generate`, in one place
    pub fn for_table(&self, acc: &TableAccess, stats: &TableStats) -> Vec<Candidate> {
        let lob = |c: &str| stats.columns.get(c).is_some_and(|i| i.lob);
        let mut eq: Vec<&PredSel> = acc
            .preds
            .iter()
            .filter(|p| p.kind != PredKind::Range && !lob(&p.column))
            .collect();
        eq.sort_by(|a, b| {
            a.sel
                .total_cmp(&b.sel)
                .then_with(|| a.column.cmp(&b.column))
        });
        let r = acc
            .preds
            .iter()
            .filter(|p| p.kind == PredKind::Range && !lob(&p.column))
            .min_by(|a, b| {
                a.sel
                    .total_cmp(&b.sel)
                    .then_with(|| a.column.cmp(&b.column))
            });
        let order: Vec<(String, SortDir)> = if acc.pk_order {
            Vec::new()
        } else {
            acc.order.iter().filter(|(c, _)| !lob(c)).cloned().collect()
        };
        let order_complete = order.len() == acc.order.len();
        let group: Vec<String> = acc.group.iter().filter(|c| !lob(c)).cloned().collect();
        if eq.is_empty() && r.is_none() && order.is_empty() && group.is_empty() {
            return Vec::new();
        }
        let mixed = order.windows(2).any(|w| w[0].1 != w[1].1);
        let order_parts: Vec<(KeyPart, PartSource)> = order
            .iter()
            .map(|(c, d)| {
                let p = KeyPart::col(c);
                let p = if mixed && *d == SortDir::Desc {
                    p.desc()
                } else {
                    p
                };
                (p, PartSource::Order)
            })
            .collect();
        let group_parts: Vec<(KeyPart, PartSource)> = group
            .iter()
            .map(|c| (KeyPart::col(c), PartSource::Group))
            .collect();
        let eq_parts: Vec<(KeyPart, PartSource)> = eq
            .iter()
            .map(|p| (KeyPart::col(&p.column), PartSource::Eq))
            .collect();
        let first = if order_parts.is_empty() {
            &group_parts
        } else {
            &order_parts
        };
        let mut variants: Vec<(Vec<(KeyPart, PartSource)>, &'static str)> = Vec::new();
        // A: eq ++ [r] ++ (O or G).
        let mut a = eq_parts.clone();
        match r {
            None => a.extend(first.iter().cloned()),
            Some(r) => {
                a.push((KeyPart::col(&r.column), PartSource::Range));
                if first.first().is_some_and(|(p, _)| p.column == r.column) {
                    a.extend(first.iter().skip(1).cloned());
                }
            }
        }
        variants.push((a, "eq_range_order"));
        let has_limit = acc.limit.is_some();
        if !order_parts.is_empty() && has_limit && order_complete {
            if r.is_some_and(|r| r.column != order_parts[0].0.column) {
                let mut v = eq_parts.clone();
                v.extend(order_parts.iter().cloned());
                variants.push((v, "eq_order_limit"));
            }
            if eq.iter().any(|p| p.kind == PredKind::InList) {
                let mut v: Vec<(KeyPart, PartSource)> = eq
                    .iter()
                    .filter(|p| p.kind == PredKind::Eq)
                    .map(|p| (KeyPart::col(&p.column), PartSource::Eq))
                    .collect();
                v.extend(order_parts.iter().cloned());
                variants.push((v, "eq_order_limit_in_as_filter"));
            }
        }
        if !group_parts.is_empty() && r.is_some_and(|r| r.column != group_parts[0].0.column) {
            let mut v = eq_parts.clone();
            v.extend(group_parts.iter().cloned());
            variants.push((v, "eq_group"));
        }
        let mut out: Vec<Candidate> = Vec::new();
        for (parts, note) in variants {
            let parts = dedup_parts(parts);
            if parts.is_empty() {
                continue;
            }
            let c = Self::candidate(acc, stats, parts, note);
            let Some(u) = usage(&c.def.parts, acc, stats) else {
                continue;
            };
            let fraction = u.entries / stats.rows();
            let gives_order = (u.delivers_order && !acc.order.is_empty())
                || (u.delivers_group && !acc.group.is_empty());
            if fraction > MAX_SCAN_FRACTION && !gives_order && !(u.covering && u.used > 0) {
                continue;
            }
            let covering_variant = self.add_covering(&c, acc, stats, &u);
            push_unique(&mut out, c);
            if let Some(cv) = covering_variant {
                push_unique(&mut out, cv);
            }
        }
        out
    }

    fn candidate(
        acc: &TableAccess,
        stats: &TableStats,
        parts: Vec<(KeyPart, PartSource)>,
        note: &str,
    ) -> Candidate {
        let (key, sources): (Vec<KeyPart>, Vec<PartSource>) = parts.into_iter().unzip();
        let def = IndexDef {
            object: acc.object,
            kind: IndexKind::BTree,
            parts: key,
            options: IndexOptions::default(),
        };
        let mut ev = Evidence {
            parts: sources,
            notes: vec![format!("variant:{note}")],
            pk_order: acc.pk_order && !acc.order.is_empty(),
            ..Evidence::default()
        };
        for p in &acc.preds {
            if def.parts.iter().any(|k| k.column == p.column) {
                ev.selectivity.insert(p.column.clone(), p.sel);
                ev.sel_source
                    .insert(p.column.clone(), p.source.as_str().to_owned());
            }
        }
        let u = usage(&def.parts, acc, stats);
        ev.entries_per_run = u.as_ref().map(|u| u.entries);
        ev.needs = needs(&def.parts, acc, stats);
        let covering = u.as_ref().is_some_and(|u| u.covering);
        ev.pk_order = ev.pk_order && u.as_ref().is_some_and(|u| u.delivers_order);
        Candidate {
            def,
            covering,
            serves: vec![(acc.fp, Role::Primary)],
            evidence: ev,
        }
    }

    /// Chapter 13's `add_covering`: the variant `c ++ need` when it is
    /// small and `c` reads enough rows for lookups to matter.
    fn add_covering(
        &self,
        c: &Candidate,
        acc: &TableAccess,
        stats: &TableStats,
        u: &Usage,
    ) -> Option<Candidate> {
        if c.covering || acc.whole_row || u.entries <= COVER_MIN_ROWS {
            return None;
        }
        self.extend_covering(c, acc, stats)
    }

    /// `c` extended by the columns `acc` reads that it lacks, within the
    /// covering limits; `None` when they don't fit. Also the validation's
    /// covering retry (worked example 3): `acc` is then the regressing
    /// statement's access.
    #[must_use]
    pub fn extend_covering(
        &self,
        c: &Candidate,
        acc: &TableAccess,
        stats: &TableStats,
    ) -> Option<Candidate> {
        if acc.whole_row {
            return None;
        }
        let need = needs(&c.def.parts, acc, stats);
        if need.is_empty() || need.len() > self.settings.max_cover_columns {
            return None;
        }
        if need
            .iter()
            .any(|n| stats.columns.get(n).is_none_or(|i| i.lob))
        {
            return None;
        }
        let mut parts = c.def.parts.clone();
        parts.extend(need.iter().map(|n| KeyPart::col(n)));
        if key_bytes(&parts, stats) > self.settings.max_key_bytes {
            return None;
        }
        let mut ev = c.evidence.clone();
        ev.parts
            .extend(std::iter::repeat_n(PartSource::Cover, need.len()));
        ev.notes.push("covering".into());
        ev.needs.clear();
        let mut out = Candidate {
            def: IndexDef {
                parts,
                ..c.def.clone()
            },
            covering: true,
            serves: c.serves.clone(),
            evidence: ev,
        };
        if let Some(u) = usage(&out.def.parts, acc, stats) {
            out.covering = u.covering;
            out.evidence.entries_per_run = Some(u.entries);
        }
        Some(out)
    }
}

fn dedup_parts(parts: Vec<(KeyPart, PartSource)>) -> Vec<(KeyPart, PartSource)> {
    let mut seen = BTreeSet::new();
    parts
        .into_iter()
        .filter(|(p, _)| seen.insert(p.column.clone()))
        .collect()
}

fn push_unique(out: &mut Vec<Candidate>, c: Candidate) {
    if let Some(e) = out.iter_mut().find(|e| e.def == c.def) {
        e.evidence.notes.extend(c.evidence.notes);
        return;
    }
    out.push(c);
}

/// The columns `acc` reads that `parts` and the primary key lack, order
/// and grouping columns first, then the rest by name.
#[must_use]
pub fn needs(parts: &[KeyPart], acc: &TableAccess, stats: &TableStats) -> Vec<String> {
    let have: BTreeSet<&str> = parts
        .iter()
        .filter(|p| p.prefix.is_none())
        .map(|p| p.column.as_str())
        .chain(stats.primary_key.iter().map(String::as_str))
        .collect();
    let mut out: Vec<String> = Vec::new();
    for c in acc
        .order
        .iter()
        .map(|(c, _)| c)
        .chain(acc.group.iter())
        .chain(acc.reads.iter())
    {
        if !have.contains(c.as_str()) && !out.contains(c) {
            out.push(c.clone());
        }
    }
    out
}

/// Bytes of a key over `parts`: each column's largest size, else its
/// average length.
#[must_use]
pub fn key_bytes(parts: &[KeyPart], stats: &TableStats) -> u64 {
    parts
        .iter()
        .map(|p| {
            let full = stats
                .columns
                .get(&p.column)
                .and_then(|c| c.max_bytes)
                .unwrap_or_else(|| {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let a = stats.avg_length(&p.column).ceil() as u64;
                    a
                });
            p.prefix.map_or(full, |n| full.min(u64::from(n) * 4))
        })
        .sum()
}

/// The key range of each sampled `LIKE 'abc%'` pattern at literal `arg`:
/// from the fixed prefix to the prefix followed by `0xff`.
fn prefix_bounds(samples: &[Vec<Literal>], arg: u32) -> Vec<(Key, Key)> {
    samples
        .iter()
        .filter_map(|s| match s.get(arg as usize)? {
            Literal::Value(Param::Text(t)) => {
                let fixed = t.split(['%', '_']).next().unwrap_or("");
                let lo = Key::Bytes(fixed.as_bytes().to_vec());
                let mut hi = fixed.as_bytes().to_vec();
                hi.push(0xff);
                Some((lo, Key::Bytes(hi)))
            }
            _ => None,
        })
        .collect()
}

/// The predicates of `f` on table `alias`, one per column, with their
/// selectivities; the other filters' selectivities; the unsargable
/// reasons; and the columns they read.
struct TablePreds {
    by_col: BTreeMap<String, PredSel>,
    residual: Vec<f64>,
    unsargable: Vec<String>,
    reads: BTreeSet<String>,
}

fn table_predicates(f: &FpInput, alias: &str, stats: &TableStats) -> TablePreds {
    let mut by_col: BTreeMap<String, PredSel> = BTreeMap::new();
    let mut residual = Vec::new();
    let mut unsargable = Vec::new();
    let mut reads: BTreeSet<String> = BTreeSet::new();
    let mut ranges: BTreeMap<String, (Vec<Key>, Vec<Key>)> = BTreeMap::new();
    let samples = &f.samples;
    let lit = |s: &Vec<Literal>, arg: u32| s.get(arg as usize).cloned();
    let shape = &f.shape;
    let on = |t: &Option<String>| t.as_deref() == Some(alias);

    for p in shape.predicates.iter().filter(|p| on(&p.column.table)) {
        let col = p.column.col.to_ascii_lowercase();
        reads.insert(col.clone());
        let keys_of = |arg: u32| -> Vec<Key> {
            samples
                .iter()
                .filter_map(|s| match lit(s, arg)? {
                    Literal::Value(v) => stats.key(&col, &v),
                    Literal::List(_) => None,
                })
                .collect()
        };
        let ps = match &p.op {
            PredOp::Eq => {
                let vals = p.args.first().map(|a| keys_of(*a)).unwrap_or_default();
                let s = stats.sel_eq(&col, &vals);
                Some(pred(&col, PredKind::Eq, s))
            }
            PredOp::IsNull => Some(pred(&col, PredKind::Eq, stats.sel_null(&col))),
            PredOp::NotNull => Some(pred(&col, PredKind::Range, stats.sel_not_null(&col))),
            PredOp::In { list } => Some(in_pred(f, stats, &col, *list, &p.args)),
            PredOp::Range { lo, hi } => {
                let e = ranges.entry(col.clone()).or_default();
                let args = &p.args;
                if *lo && let Some(a) = args.first() {
                    e.0.extend(keys_of(*a));
                }
                if *hi && let Some(a) = args.last() {
                    e.1.extend(keys_of(*a));
                }
                None
            }
            PredOp::Prefix => {
                let vals = p
                    .args
                    .first()
                    .map_or_else(Vec::new, |a| prefix_bounds(samples, *a));
                let s = if vals.is_empty() {
                    stats.sel_range(&col, None, None)
                } else {
                    mean_sel(
                        vals.iter()
                            .map(|(lo, hi)| stats.sel_range(&col, Some(lo), Some(hi))),
                    )
                };
                Some(pred(&col, PredKind::Range, s))
            }
            PredOp::Ne => {
                residual.push(0.9);
                None
            }
            PredOp::Unsargable { why } => {
                unsargable.push(why.as_string());
                residual.push(RESIDUAL_SEL);
                None
            }
            PredOp::Like | PredOp::JsonPath { .. } | PredOp::Fulltext | PredOp::Spatial => {
                residual.push(RESIDUAL_SEL);
                None
            }
            PredOp::Vector => None,
        };
        if let Some(ps) = ps {
            keep_better(&mut by_col, ps);
        }
    }
    for (col, (los, his)) in ranges {
        let s = if los.is_empty() && his.is_empty() {
            stats.sel_range(&col, None, None)
        } else {
            let n = los.len().max(his.len());
            mean_sel((0..n).map(|i| stats.sel_range(&col, los.get(i), his.get(i))))
        };
        keep_better(&mut by_col, pred(&col, PredKind::Range, s));
    }
    TablePreds {
        by_col,
        residual,
        unsargable,
        reads,
    }
}

/// Reads `f`'s shape for table `alias` (chapter 13: `P`, `E`, `R`, `O`,
/// `G` and the covering inputs).
#[must_use]
pub fn access(f: &FpInput, alias: &str, object: ObjectId, stats: &TableStats) -> TableAccess {
    let shape = &f.shape;
    let on = |t: &Option<String>| t.as_deref() == Some(alias);
    let TablePreds {
        mut by_col,
        residual,
        unsargable,
        mut reads,
    } = table_predicates(f, alias, stats);
    // Join columns of this table are equalities for lookups into it.
    for j in &shape.joins {
        if j.op != JoinOp::Eq {
            continue;
        }
        let sides: &[&sprig_workload::shape::ColumnRef] = match j.kind {
            JoinKind::Left => &[&j.r],
            JoinKind::Right => &[&j.l],
            _ => &[&j.l, &j.r],
        };
        for c in [&j.l, &j.r] {
            if on(&c.table) {
                reads.insert(c.col.to_ascii_lowercase());
            }
        }
        for c in sides.iter().filter(|c| on(&c.table)) {
            let col = c.col.to_ascii_lowercase();
            let s = stats.sel_eq(&col, &[]);
            keep_better(&mut by_col, pred(&col, PredKind::Eq, s));
        }
    }
    let eq_cols: BTreeSet<String> = by_col
        .values()
        .filter(|p| p.kind == PredKind::Eq)
        .map(|p| p.column.clone())
        .collect();
    let order_all_here = !shape.order_by_expr && shape.order_by.iter().all(|o| on(&o.column.table));
    let order: Vec<(String, SortDir)> = if order_all_here {
        shape
            .order_by
            .iter()
            .map(|o| (o.column.col.to_ascii_lowercase(), o.dir))
            .filter(|(c, _)| !eq_cols.contains(c))
            .collect()
    } else {
        Vec::new()
    };
    let group_all_here = !shape.group_by_expr && shape.group_by.iter().all(|g| on(&g.table));
    let group: Vec<String> = if group_all_here {
        shape
            .group_by
            .iter()
            .map(|g| g.col.to_ascii_lowercase())
            .filter(|c| !eq_cols.contains(c))
            .collect()
    } else {
        Vec::new()
    };
    for o in shape.order_by.iter().filter(|o| on(&o.column.table)) {
        reads.insert(o.column.col.to_ascii_lowercase());
    }
    for g in shape.group_by.iter().filter(|g| on(&g.table)) {
        reads.insert(g.col.to_ascii_lowercase());
    }
    let mut whole_row = matches!(shape.stmt_type, StmtType::Update | StmtType::Delete);
    if let Some(cols) = shape.select.get(alias) {
        for c in cols {
            if c == "*" {
                whole_row = true;
            } else {
                reads.insert(c.to_ascii_lowercase());
            }
        }
    }
    let pk = &stats.primary_key;
    let pk_order = !order.is_empty()
        && order.len() <= pk.len()
        && order.iter().zip(pk).all(|((c, _), p)| c == p)
        && order.windows(2).all(|w| w[0].1 == w[1].1);
    let limit = shape
        .limit
        .then(|| f.limit.map_or_else(|| f.returned_per_run.max(1.0), f64_of));
    TableAccess {
        fp: f.id,
        alias: alias.to_owned(),
        object,
        preds: by_col.into_values().collect(),
        residual,
        order,
        group,
        pk_order,
        reads,
        whole_row,
        limit,
        unsargable,
    }
}

fn pred(col: &str, kind: PredKind, s: Sel) -> PredSel {
    PredSel {
        column: col.to_owned(),
        kind,
        sel: s.value,
        source: s.source,
    }
}

/// Keeps the more useful predicate per column: an equality over an `IN`
/// over a range; the more selective among equals.
fn keep_better(by_col: &mut BTreeMap<String, PredSel>, p: PredSel) {
    let rank = |k: PredKind| match k {
        PredKind::Eq => 0,
        PredKind::InList => 1,
        PredKind::Range => 2,
    };
    match by_col.get(&p.column) {
        Some(e) if (rank(e.kind), e.sel) <= (rank(p.kind), p.sel) => {}
        _ => {
            by_col.insert(p.column.clone(), p);
        }
    }
}

fn mean_sel(it: impl Iterator<Item = Sel>) -> Sel {
    let mut n = 0u64;
    let mut sum = 0.0;
    let mut source = SelSource::Default;
    for s in it {
        n += 1;
        sum += s.value;
        source = s.source;
    }
    if n == 0 {
        return Sel {
            value: DEFAULT_SEL,
            source: SelSource::Default,
        };
    }
    Sel {
        value: sum / f64_of(n),
        source,
    }
}

fn in_pred(f: &FpInput, stats: &TableStats, col: &str, list: bool, args: &[u32]) -> PredSel {
    // Per sample: the list's values; lengths give k.
    let mut per_sample: Vec<Vec<Key>> = Vec::new();
    let mut lens: Vec<u64> = Vec::new();
    for s in &f.samples {
        if list {
            if let Some(Literal::List(vs)) = args.first().and_then(|a| s.get(*a as usize)) {
                lens.push(vs.len() as u64);
                per_sample.push(vs.iter().filter_map(|v| stats.key(col, v)).collect());
            }
        } else {
            let vs: Vec<Key> = args
                .iter()
                .filter_map(|a| match s.get(*a as usize)? {
                    Literal::Value(v) => stats.key(col, v),
                    Literal::List(_) => None,
                })
                .collect();
            lens.push(args.len() as u64);
            per_sample.push(vs);
        }
    }
    let k = if lens.is_empty() {
        if list {
            f.in_list_max.unwrap_or(DEFAULT_IN_LEN)
        } else {
            args.len() as u64
        }
    } else {
        // The mean length, rounded up.
        lens.iter().sum::<u64>().div_ceil(lens.len() as u64)
    };
    let s = if per_sample.iter().all(Vec::is_empty) {
        stats.sel_in(col, &[], k)
    } else {
        mean_sel(
            per_sample
                .iter()
                .filter(|v| !v.is_empty())
                .map(|v| stats.sel_in(col, v, v.len() as u64)),
        )
    };
    let kind = match k {
        0 | 1 => PredKind::Eq,
        k if k <= MAX_IN_EQ => PredKind::InList,
        _ => PredKind::Range,
    };
    pred(col, kind, s)
}

/// How the index with key `parts` serves `acc`: the seek prefix, entries
/// read, order, grouping and covering. `None` when it can't serve it at
/// all (no seek, no order or grouping it delivers, and not covering).
#[must_use]
pub fn usage(parts: &[KeyPart], acc: &TableAccess, stats: &TableStats) -> Option<Usage> {
    let n = stats.rows();
    let mut used = 0;
    let mut eq_cols: Vec<&str> = Vec::new();
    let mut eq_sels: Vec<f64> = Vec::new();
    let mut range_sel = 1.0;
    let mut order_broken = false;
    let mut after_seek = 0; // first part after the seek
    for p in parts {
        let Some(ps) = acc.pred(&p.column).filter(|_| p.prefix.is_none()) else {
            break;
        };
        used += 1;
        match ps.kind {
            PredKind::Eq => {
                eq_cols.push(&p.column);
                eq_sels.push(ps.sel);
            }
            PredKind::InList => {
                eq_cols.push(&p.column);
                eq_sels.push(ps.sel);
                order_broken = true;
            }
            PredKind::Range => {
                range_sel = ps.sel;
                break;
            }
        }
        after_seek = used;
    }
    let seek_sel = stats.sel_prefix(&eq_cols, &eq_sels).value * range_sel;
    let range_part = (used > after_seek).then(|| parts[after_seek].column.as_str());
    // Order: after the equality prefix, the parts must follow O (a range
    // part only as O[0]); or O is the primary key and every part is fixed.
    let delivers_order = !acc.order.is_empty()
        && !order_broken
        && (follows_order(&parts[after_seek..], &acc.order, range_part)
            || (acc.pk_order && range_part.is_none() && after_seek == parts.len()));
    let delivers_group = !acc.group.is_empty() && !order_broken && {
        let rest = &parts[after_seek..];
        let first_ok = range_part.is_none_or(|r| acc.group.first().is_some_and(|g| g == r));
        first_ok
            && rest.len() >= acc.group.len()
            && rest[..acc.group.len()]
                .iter()
                .map(|p| p.column.as_str())
                .collect::<BTreeSet<_>>()
                == acc.group.iter().map(String::as_str).collect()
    };
    let covering = !acc.whole_row && needs(parts, acc, stats).is_empty();
    if used == 0 && !delivers_order && !delivers_group && !covering {
        return None;
    }
    let mut entries = (n * seek_sel).max(1.0);
    if delivers_order && let Some(k) = acc.limit {
        // Rows that pass the filters the seek didn't apply.
        let mut residual: f64 = acc.residual.iter().product();
        for p in &acc.preds {
            let in_seek = parts[..used].iter().any(|k| k.column == p.column);
            if !in_seek {
                residual *= p.sel;
            }
        }
        let residual = residual.clamp(1.0 / n, 1.0);
        entries = entries.min((k / residual).max(1.0));
    }
    Some(Usage {
        used,
        sel: seek_sel,
        entries,
        delivers_order,
        delivers_group,
        covering,
    })
}

/// Whether `rest` (the parts after the equality prefix) delivers `order`:
/// a range part may only be `order[0]`; then the parts follow the order's
/// columns with directions all equal or all inverted.
fn follows_order(rest: &[KeyPart], order: &[(String, SortDir)], range: Option<&str>) -> bool {
    if let Some(r) = range
        && order.first().is_none_or(|(c, _)| c != r)
    {
        return false;
    }
    if rest.len() < order.len() {
        return false;
    }
    let mut flip: Option<bool> = None;
    for (p, (c, d)) in rest.iter().zip(order) {
        if p.column != *c || p.prefix.is_some() {
            return false;
        }
        let want_desc = *d == SortDir::Desc;
        let f = p.desc != want_desc;
        if *flip.get_or_insert(f) != f {
            return false;
        }
    }
    true
}

/// Chapter 13's `merge` for the candidates of one table: shortest first,
/// a candidate whose parts are a prefix of a longer one's is merged into
/// the first such longer one, unless it relies on the primary-key order a
/// longer key breaks, or it covers columns the longer one lacks.
#[must_use]
pub fn merge(mut cands: Vec<Candidate>) -> Vec<Candidate> {
    // Identical definitions first: one candidate serving both.
    let mut uniq: Vec<Candidate> = Vec::new();
    for c in cands.drain(..) {
        match uniq.iter_mut().find(|u| u.def == c.def) {
            Some(u) => {
                for s in c.serves {
                    if !u.serves.iter().any(|(f, _)| *f == s.0) {
                        u.serves.push(s);
                    }
                }
                u.covering = u.covering && c.covering;
                u.evidence.pk_order |= c.evidence.pk_order;
            }
            None => uniq.push(c),
        }
    }
    uniq.sort_by(|a, b| {
        a.def
            .parts
            .len()
            .cmp(&b.def.parts.len())
            .then_with(|| a.def.canonical().cmp(&b.def.canonical()))
    });
    let mut out: Vec<Option<Candidate>> = uniq.into_iter().map(Some).collect();
    for i in 0..out.len() {
        let Some(c) = out[i].clone() else { continue };
        for j in (i + 1)..out.len() {
            let Some(d) = out[j].as_mut() else { continue };
            if d.def.object != c.def.object || d.def.parts.len() <= c.def.parts.len() {
                continue;
            }
            let covers = !c.covering
                || c.evidence
                    .needs
                    .iter()
                    .all(|n| d.def.columns().any(|x| x == n));
            if c.def.is_prefix_of(&d.def) && !c.evidence.pk_order && covers {
                for (f, _) in &c.serves {
                    if !d.serves.iter().any(|(g, _)| g == f) {
                        d.serves.push((*f, Role::Merged));
                    }
                }
                d.evidence
                    .notes
                    .push(format!("merged:{}", c.def.canonical()));
                out[i] = None;
                break;
            }
        }
    }
    out.into_iter().flatten().collect()
}

/// Chapter 13's `covered_by`: the first existing index that serves `c`
/// already. Only indexes that [count](IndexInfo::counts) are considered:
/// user, system and migrated ones, and usable automatic ones, never an
/// `IGNORED` one.
#[must_use]
pub fn covered_by<'a>(c: &Candidate, existing: &'a [IndexInfo]) -> Option<&'a IndexInfo> {
    let k = c.eq_len();
    let cp = &c.def.parts;
    existing.iter().filter(|e| e.counts()).find(|e| {
        let ep = &e.parts;
        if ep.len() < cp.len() {
            return false;
        }
        // The equalities in any order.
        let set = |v: &[KeyPart]| v.iter().map(|p| p.column.clone()).collect::<BTreeSet<_>>();
        if set(&ep[..k]) != set(&cp[..k]) {
            return false;
        }
        // Then the same columns in order, directions equal or all
        // inverted.
        let mut flip: Option<bool> = None;
        for (a, b) in ep[k..].iter().zip(&cp[k..]) {
            if a.column != b.column {
                return false;
            }
            let f = a.desc != b.desc;
            if *flip.get_or_insert(f) != f {
                return false;
            }
        }
        // Prefix lengths at least c's.
        let prefix_ok = cp.iter().all(|p| {
            ep.iter()
                .find(|q| q.column == p.column)
                .is_some_and(|q| match (q.prefix, p.prefix) {
                    (None, _) => true,
                    (Some(_), None) => false,
                    (Some(a), Some(b)) => a >= b,
                })
        });
        // A candidate relying on the primary-key suffix needs exactly its
        // parts (or a unique key, which pins one row per value).
        let pk_ok = !c.evidence.pk_order || ep.len() == cp.len() || e.unique;
        let cover_ok = !c.covering
            || c.evidence
                .needs
                .iter()
                .all(|n| ep.iter().any(|q| &q.column == n));
        prefix_ok && pk_ok && cover_ok
    })
}

#[cfg(test)]
mod tests;
