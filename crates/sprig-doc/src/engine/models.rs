//! The engine's model operations from R6 round 3 (chapters 34 and 36):
//! `find` with `$near` or `$nearSphere`, the `$geoNear` stage, `2dsphere`
//! index columns, search indexes and the `$vectorSearch` stage.
//!
//! **Geo.** A `2dsphere` index on a path is two virtual columns (a stored
//! point's latitude and longitude) and a B-tree index on them, kept by
//! MariaDB on every write path; a query pushes the region's box onto them
//! and every candidate is rechecked on the sphere ([`crate::geo`]).
//! Non-point values have `NULL` columns and stay candidates. `$near` and
//! `$geoNear` need the index, as in MongoDB (291), read their candidates
//! the same way and sort by distance in the gateway.
//!
//! **Vectors.** A search index of type `vectorSearch` is a row of the
//! database's hidden table `__sprig_doc_search_ix` (its definition as sent).
//! `$vectorSearch` runs an exact search inside MariaDB: the distance of
//! every document whose path holds a numeric array of the index's
//! dimensions, `VEC_DISTANCE_*` over `VEC_FromText(JSON_EXTRACT(...))`,
//! ordered and cut by `LIMIT`; the filter (declared filter fields only) is
//! pushed when the translator says it is exact, otherwise candidates are
//! read in pages and rechecked. `numCandidates` is checked but every search
//! is exact: an approximate graph over a collection needs a side table
//! (MariaDB refuses a vector index on a generated column: 1252).
//!
//! **Approximate vectors (R5 round 5, decision R33).** Each vector index
//! has a side table `__sprig_doc_vx_<hash>` (`_id`, the vector, the
//! document's `_v`) with MariaDB's HNSW `VECTOR INDEX`. It is kept in step
//! on read, not on write: before an approximate search, rows whose `_v`
//! is newer than the side table's newest minus a margin are copied in
//! (an upsert, read through an index on `_v`), at most every
//! `vector_sync_ms` (1 s) per gateway, and every minute rows of deleted
//! documents are removed; approximate results are thus up to about a
//! second behind writes, as Atlas Vector Search is eventually consistent.
//! A missing side table (a new index, or one a move didn't carry) is
//! created and filled the same way (MariaDB limits a vector index's
//! primary key to 256 bytes with its length: documents whose encoded
//! `_id` is over 254 bytes stay out of approximate searches). An approximate search reads
//! `numCandidates` nearest `_id`s from the side table, then the documents
//! themselves, rechecks the filter, and ranks by the exact distance of
//! the stored vector, so a stale side row can cost recall but never a
//! wrong result. Below `vector.ann_min_rows` vectors the search stays
//! exact.

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{CursorPlan, Engine, FindSpec, Ns, SqlRunner, mongo_error, now_date, text};
use crate::agg::{self, AggExpr, Projection};
use crate::filter::{self, FieldOp, Pred};
use crate::geo::{self, GeoOp, Near};
use crate::meta::{CollMeta, IndexDef};
use crate::sql::{f64_lit, ident, json_path, str_lit};
use crate::translate::translate;

/// The hidden field carrying `$vectorSearch`'s score to `{$meta:
/// "vectorSearchScore"}`; removed before results leave the engine.
pub const SCORE_FIELD: &str = "__sprig_vector_score";

/// The hidden table of search index definitions in each database.
const SEARCH_TABLE: &str = "__sprig_doc_search_ix";

/// How far back (in `_v` units: ms << 16) each side-table sync re-reads:
/// writers' clocks and transactions that commit late.
const SIDE_MARGIN: u64 = 30_000 << 16;

/// Side tables synced, and when (this process): at most once a second.
static SYNCED: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, (std::time::Instant, u64)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Side tables purged of deleted documents, and when (this process).
static PURGED: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// The side table of vector index `name` on collection table `table`.
#[must_use]
pub fn vector_side_table(table: &str, name: &str) -> String {
    let mut k = table.as_bytes().to_vec();
    k.push(0);
    k.extend_from_slice(name.as_bytes());
    format!("__sprig_doc_vx_{:016x}", crate::meta::fnv64(&k))
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

fn num(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(f) => Some(*f),
        Bson::Int32(i) => Some(f64::from(*i)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => Some(*i as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// Whether a `$near` sits under `$or` or `$nor` (MongoDB refuses it: 2).
fn nested_near(p: &Pred, under: bool) -> bool {
    match p {
        Pred::Field {
            op: FieldOp::Geo(g),
            ..
        } => under && matches!(g.as_ref(), GeoOp::Near(_)),
        Pred::And(k) => k.iter().any(|x| nested_near(x, under)),
        Pred::Or(k) | Pred::Nor(k) => k.iter().any(|x| nested_near(x, true)),
        _ => false,
    }
}

/// Refuses `$near` where MongoDB does: under `$or` (2), or in a command
/// that can't sort by distance (`count`, ...: 5626500).
///
/// # Errors
/// As above.
pub fn check_near(p: &Pred, sorting: bool) -> Result<(), MongoError> {
    if nested_near(p, false) {
        return Err(bad("geo $near must be top-level expr"));
    }
    if !sorting && near_of(p).is_some() {
        return Err(MongoError::new(
            5_626_500,
            "$geoNear, $near, and $nearSphere are not allowed in this context, as these operators require sorting geospatial data. If you do not need sort, consider using $geoWithin instead.",
        ));
    }
    Ok(())
}

/// The `$near` / `$nearSphere` of a filter, with its path, when the filter
/// has one at the top level (alone or inside a top-level `$and`).
#[must_use]
pub fn near_of(p: &Pred) -> Option<(String, Near)> {
    match p {
        Pred::Field {
            path,
            op: FieldOp::Geo(g),
        } => match g.as_ref() {
            GeoOp::Near(n) => Some((path.clone(), n.clone())),
            _ => None,
        },
        Pred::And(kids) => kids.iter().find_map(near_of),
        _ => None,
    }
}

fn no_index(what: &str) -> MongoError {
    MongoError::new(291, format!("unable to find index for {what} query"))
}

/// `{$meta: "vectorSearchScore"}` (and other `$meta` names, which have no
/// value here) for the document `root`.
#[must_use]
pub fn meta_value(arg: &Bson, root: Option<&Bson>) -> Bson {
    match (arg.as_str(), root) {
        (Some("vectorSearchScore" | "textScore" | "searchScore"), Some(Bson::Document(d))) => {
            d.get(SCORE_FIELD).cloned().unwrap_or(Bson::Null)
        }
        (Some("searchHighlights"), Some(Bson::Document(d))) => d
            .get(crate::text::HIGHLIGHT_FIELD)
            .cloned()
            .unwrap_or_else(|| Bson::Array(Vec::new())),
        _ => Bson::Null,
    }
}

/// Removes [`SCORE_FIELD`] from results.
pub fn strip_scores(docs: &mut [Document]) {
    for d in docs {
        d.remove(SCORE_FIELD);
    }
}

/// Sets `path` (dotted) in `d`.
fn set_path(d: &mut Document, path: &str, v: Bson) {
    match path.split_once('.') {
        None => {
            d.insert(path, v);
        }
        Some((head, rest)) => {
            let mut inner = match d.get(head) {
                Some(Bson::Document(x)) => x.clone(),
                _ => Document::new(),
            };
            set_path(&mut inner, rest, v);
            d.insert(head, inner);
        }
    }
}

/// A search index definition (`createSearchIndexes`, type
/// `vectorSearch`).
#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndex {
    /// The vector field.
    pub path: String,
    /// `numDimensions`.
    pub dims: usize,
    /// `euclidean`, `cosine` or `dotProduct`.
    pub similarity: String,
    /// Declared filter fields.
    pub filters: Vec<String>,
}

impl VectorIndex {
    /// Parses and checks a definition.
    ///
    /// # Errors
    /// 2 `BadValue` for a malformed definition.
    pub fn parse(def: &Document) -> Result<Self, MongoError> {
        let fields = def
            .get_array("fields")
            .map_err(|_| bad("a vectorSearch index definition needs 'fields'"))?;
        let mut vector = None;
        let mut filters = Vec::new();
        for f in fields {
            let Bson::Document(f) = f else {
                return Err(bad("each field definition must be an object"));
            };
            let path = f
                .get_str("path")
                .map_err(|_| bad("a field definition needs a 'path'"))?
                .to_owned();
            match f.get_str("type").unwrap_or("") {
                "vector" => {
                    if vector.is_some() {
                        return Err(bad("only one vector field per index in this release"));
                    }
                    let dims = f
                        .get("numDimensions")
                        .and_then(num)
                        .filter(|n| *n >= 1.0 && *n <= 8192.0 && n.fract() == 0.0)
                        .ok_or_else(|| {
                            bad("numDimensions must be a whole number from 1 to 8192")
                        })?;
                    let similarity = f.get_str("similarity").unwrap_or("").to_owned();
                    if !matches!(similarity.as_str(), "euclidean" | "cosine" | "dotProduct") {
                        return Err(bad(
                            "similarity must be one of euclidean, cosine, dotProduct",
                        ));
                    }
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let dims = dims as usize;
                    vector = Some((path, dims, similarity));
                }
                "filter" => filters.push(path),
                other => {
                    return Err(bad(format!("unknown field type '{other}'")));
                }
            }
        }
        let (path, dims, similarity) =
            vector.ok_or_else(|| bad("a vectorSearch index needs one field of type vector"))?;
        Ok(Self {
            path,
            dims,
            similarity,
            filters,
        })
    }

    /// The SQL distance of the path to vector literal `q`.
    fn distance_sql(&self, q: &str) -> String {
        let f = if self.similarity == "euclidean" {
            "VEC_DISTANCE_EUCLIDEAN"
        } else {
            "VEC_DISTANCE_COSINE"
        };
        format!(
            "{f}(VEC_FromText(JSON_EXTRACT(doc, {})), VEC_FromText({}))",
            self.path_lit(),
            str_lit(q)
        )
    }

    fn path_lit(&self) -> String {
        let parts: Vec<&str> = self.path.split('.').collect();
        str_lit(&json_path(&parts, &[]))
    }

    /// The side table's distance function (the one its index uses).
    fn side_distance(&self) -> (&'static str, &'static str) {
        if self.similarity == "euclidean" {
            ("VEC_DISTANCE_EUCLIDEAN", "euclidean")
        } else {
            ("VEC_DISTANCE_COSINE", "cosine")
        }
    }

    /// Rows whose path holds a numeric array of the right length.
    fn valid_sql(&self) -> String {
        let p = self.path_lit();
        format!(
            "JSON_TYPE(JSON_EXTRACT(doc, {p})) = 'ARRAY' AND JSON_LENGTH(JSON_EXTRACT(doc, {p})) = {} \
             AND JSON_SCHEMA_VALID('{{\"type\":\"array\",\"items\":{{\"type\":\"number\"}}}}', JSON_EXTRACT(doc, {p}))",
            self.dims
        )
    }

    /// Atlas's score of distance `d`.
    #[must_use]
    pub fn score(&self, d: f64) -> f64 {
        if self.similarity == "euclidean" {
            1.0 / (1.0 + d)
        } else {
            1.0 - d / 2.0
        }
    }
}

/// The paths a filter names.
fn filter_paths(p: &Pred, out: &mut Vec<String>) {
    match p {
        Pred::And(k) | Pred::Or(k) | Pred::Nor(k) => {
            for x in k {
                filter_paths(x, out);
            }
        }
        Pred::Field { path, .. } => out.push(path.clone()),
        Pred::Expr(_) => out.push("$expr".into()),
        Pred::True => {}
    }
}

/// The paths of `pred`'s geo conditions.
fn geo_paths(pred: &Pred, out: &mut Vec<String>) {
    match pred {
        Pred::And(v) | Pred::Or(v) | Pred::Nor(v) => {
            for p in v {
                geo_paths(p, out);
            }
        }
        Pred::Field {
            path,
            op: FieldOp::Geo(_),
        } if !out.contains(path) => out.push(path.clone()),
        _ => {}
    }
}

impl Engine {
    /// `find` with `$near` or `$nearSphere`: candidates through the
    /// `2dsphere` index's box, sorted by distance (then by `sort`, which
    /// MongoDB lets re-order them), skipped, limited and projected.
    #[allow(clippy::too_many_arguments)] // the find's parts, as `find` holds them
    pub(super) async fn find_near(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &FindSpec,
        pred: &Pred,
        path: &str,
        near: &Near,
        m: &CollMeta,
    ) -> Result<CursorPlan, MongoError> {
        if geo::index_on(m, path).is_none() {
            return Err(no_index("$geoNear"));
        }
        let sqlp = translate(pred, m);
        let docs = self
            .scan(sql, ns, &m.table, pred, &sqlp, self.cfg.max_sort_bytes)
            .await?;
        let mut ranked: Vec<(f64, Document)> = docs
            .into_iter()
            .filter_map(|d| {
                let v = filter::values_at(&d, path);
                let best = v
                    .leaves
                    .iter()
                    .filter_map(|x| near.nearest(x))
                    .fold(None, |b: Option<f64>, x| Some(b.map_or(x, |b| b.min(x))))?;
                Some((best, d))
            })
            .collect();
        ranked.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut docs: Vec<Document> = ranked.into_iter().map(|x| x.1).collect();
        if let Some(s) = spec.sort.as_ref().filter(|s| !s.is_empty()) {
            let s = agg::parse_sort(s)?;
            agg::sort_docs(&mut docs, &s);
        }
        let projection = spec
            .projection
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(Projection::parse)
            .transpose()?;
        let skip = usize::try_from(spec.skip).unwrap_or(usize::MAX);
        let take = if spec.limit > 0 {
            usize::try_from(spec.limit).unwrap_or(usize::MAX)
        } else {
            usize::MAX
        };
        let now = now_date();
        let docs = docs
            .into_iter()
            .skip(skip)
            .take(take)
            .map(|d| match &projection {
                Some(p) => p.apply(&d, now),
                None => Ok(d),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CursorPlan::from_docs(ns.clone(), docs))
    }

    /// A first stage the engine runs itself (`$geoNear`, `$vectorSearch`,
    /// `$listSearchIndexes`), or `None`. These stages anywhere else fail
    /// with 40602, as in MongoDB.
    pub(super) async fn first_stage(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        pipeline: &[Document],
    ) -> Result<Option<Vec<Document>>, MongoError> {
        const OWN: [&str; 5] = [
            "$geoNear",
            "$vectorSearch",
            "$listSearchIndexes",
            "$search",
            "$searchMeta",
        ];
        // Empty `$match` stages optimize away, as in MongoDB.
        let pipeline: Vec<&Document> = pipeline
            .iter()
            .filter(|s| !(s.len() == 1 && s.get_document("$match").is_ok_and(Document::is_empty)))
            .collect();
        for s in pipeline.iter().skip(1) {
            if let Some(k) = s.keys().next().filter(|k| OWN.contains(&k.as_str())) {
                if k == "$geoNear" {
                    return Err(MongoError::new(
                        40603,
                        "$geoNear was not the first stage in the pipeline after optimization. Is optimization disabled or inhibited?",
                    ));
                }
                return Err(MongoError::new(
                    40602,
                    format!("{k} is only valid as the first stage in a pipeline"),
                ));
            }
        }
        let Some(first) = pipeline.first() else {
            return Ok(None);
        };
        let Some((name, arg)) = first.iter().next() else {
            return Ok(None);
        };
        if !OWN.contains(&name.as_str()) {
            return Ok(None);
        }
        let Bson::Document(spec) = arg else {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                format!("{name} must take a nested object"),
            ));
        };
        Ok(Some(match name.as_str() {
            "$geoNear" => self.geo_near(sql, ns, spec).await?,
            "$vectorSearch" => self.vector_search(sql, ns, spec).await?,
            // Chapter 39 (R5 round 5).
            "$search" => self.search_stage(sql, ns, spec).await?,
            "$searchMeta" => self.search_meta_stage(sql, ns, spec).await?,
            _ => {
                let name = spec.get_str("name").ok();
                self.list_search_indexes(sql, ns, name).await?
            }
        }))
    }

    /// The `$geoNear` stage.
    async fn geo_near(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        let field = spec
            .get_str("distanceField")
            .map_err(|_| MongoError::new(25_278, "$geoNear requires a 'distanceField' argument"))?;
        let near_v = spec
            .get("near")
            .ok_or_else(|| MongoError::new(5_860_400, "$geoNear requires a 'near' argument"))?;
        let spherical = spec.get_bool("spherical").unwrap_or(false);
        let (at, metres) = match near_v {
            Bson::Document(d) if d.contains_key("type") => match geo::geojson(d)? {
                geo::Geom::Point(p) => (p, true),
                _ => return Err(bad("$geoNear 'near' must be a point")),
            },
            other => {
                let Some(geo::Geom::Point(p)) = geo::stored(other) else {
                    return Err(bad("$geoNear 'near' must be a point"));
                };
                // Flat distances need a `2d` index, which SprigDB never has.
                if !spherical {
                    return Err(no_index("$geoNear"));
                }
                (p, false)
            }
        };
        let Some(m) = self.meta(sql, ns).await? else {
            return Err(no_index("$geoNear"));
        };
        let geo_ix: Vec<&IndexDef> = m.indexes.iter().filter(|i| geo::is_2dsphere(i)).collect();
        let path = match spec.get_str("key") {
            Ok(k) => {
                if geo::index_on(&m, k).is_none() {
                    return Err(no_index("$geoNear"));
                }
                k.to_owned()
            }
            Err(_) => match geo_ix.as_slice() {
                [] => {
                    return Err(MongoError::new(
                        code::INDEX_NOT_FOUND,
                        "$geoNear requires a 2d or 2dsphere index, but none were found",
                    ));
                }
                [one] => one.key.first().map(|k| k.0.clone()).unwrap_or_default(),
                _ => {
                    return Err(MongoError::new(
                        code::INDEX_NOT_FOUND,
                        "There is more than one 2dsphere index; unsure which to use for $geoNear",
                    ));
                }
            },
        };
        let near = Near {
            at,
            min: spec.get("minDistance").and_then(num),
            max: spec.get("maxDistance").and_then(num),
            metres,
        };
        let query = spec.get_document("query").cloned().unwrap_or_default();
        let mut pred = filter::parse(&query)?;
        let geo_pred = Pred::Field {
            path: path.clone(),
            op: FieldOp::Geo(Box::new(GeoOp::Near(near.clone()))),
        };
        pred = match pred {
            Pred::True => geo_pred,
            Pred::And(mut k) => {
                k.push(geo_pred);
                Pred::And(k)
            }
            other => Pred::And(vec![other, geo_pred]),
        };
        let sqlp = translate(&pred, &m);
        let docs = self
            .scan(sql, ns, &m.table, &pred, &sqlp, self.cfg.max_sort_bytes)
            .await?;
        let mult = spec.get("distanceMultiplier").and_then(num).unwrap_or(1.0);
        let locs = spec.get_str("includeLocs").ok();
        let mut ranked: Vec<(f64, Document)> = Vec::new();
        for mut d in docs {
            let vals = filter::values_at(&d, &path);
            let mut best: Option<(f64, Bson)> = None;
            for leaf in &vals.leaves {
                // A location, or an array of them: the nearest element.
                let cands: Vec<&Bson> = if geo::stored(leaf).is_some() {
                    vec![*leaf]
                } else if let Bson::Array(a) = leaf {
                    a.iter().collect()
                } else {
                    Vec::new()
                };
                for c in cands {
                    if let Some(x) = near.nearest(c)
                        && near.in_bounds(x)
                        && best.as_ref().is_none_or(|b| x < b.0)
                    {
                        best = Some((x, c.clone()));
                    }
                }
            }
            let Some((dist, loc)) = best else { continue };
            set_path(&mut d, field, Bson::Double(dist * mult));
            if let Some(l) = locs {
                set_path(&mut d, l, loc);
            }
            ranked.push((dist, d));
        }
        ranked.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(ranked.into_iter().map(|x| x.1).collect())
    }

    /// Ensures the database's search index table.
    async fn search_table(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<(), MongoError> {
        self.ensure_db(sql, db).await?;
        sql.exec(&format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (coll VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, \
             name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, type VARCHAR(32) NOT NULL, \
             id CHAR(24) NOT NULL, def JSON NOT NULL, PRIMARY KEY (coll, name)) ENGINE=InnoDB",
            ident(db),
            ident(SEARCH_TABLE)
        ))
        .await
        .map_err(|e| mongo_error(&e))?;
        Ok(())
    }

    /// The search indexes of `ns` (`name`: only that one): `(id, name,
    /// type, definition)`.
    async fn search_rows(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: Option<&str>,
    ) -> Result<Vec<(String, String, String, Document)>, MongoError> {
        let mut q = format!(
            "SELECT id, name, type, def FROM {}.{} WHERE coll = {}",
            ident(&ns.db),
            ident(SEARCH_TABLE),
            str_lit(&ns.coll)
        );
        if let Some(n) = name {
            q.push_str(" AND name = ");
            q.push_str(&str_lit(n));
        }
        q.push_str(" ORDER BY name");
        let rows = match sql.query(&q).await {
            Ok(r) => r,
            Err(e) if e.is(1146) || e.is(1049) => return Ok(Vec::new()),
            Err(e) => return Err(mongo_error(&e)),
        };
        Ok(rows
            .into_iter()
            .map(|r| {
                let def = serde_json::from_str::<serde_json::Value>(&text(
                    r.get(3).and_then(Option::as_ref),
                ))
                .ok()
                .and_then(|v| Bson::try_from(v).ok())
                .and_then(|b| b.as_document().cloned())
                .unwrap_or_default();
                (
                    text(r.first().and_then(Option::as_ref)),
                    text(r.get(1).and_then(Option::as_ref)),
                    text(r.get(2).and_then(Option::as_ref)),
                    def,
                )
            })
            .collect())
    }

    /// `$listSearchIndexes`.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn list_search_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: Option<&str>,
    ) -> Result<Vec<Document>, MongoError> {
        let mut out: Vec<Document> = self
            .search_rows(sql, ns, name)
            .await?
            .into_iter()
            .map(|(id, name, t, def)| {
                doc! {"id": id, "name": name, "type": t, "status": "READY", "queryable": true,
                "latestDefinition": def}
            })
            .collect();
        // Chapter 39: full-text search indexes.
        if let Some(t) = &self.text {
            for i in t.list(sql, ns).await? {
                if i.kind != crate::text::TextKind::Search || name.is_some_and(|n| n != i.name) {
                    continue;
                }
                out.push(doc! {"id": i.id, "name": i.name, "type": "search",
                "status": if i.ready { "READY" } else { "BUILDING" }, "queryable": i.ready,
                "latestDefinition": i.spec});
            }
            out.sort_by(|a, b| {
                a.get_str("name")
                    .unwrap_or("")
                    .cmp(b.get_str("name").unwrap_or(""))
            });
        }
        Ok(out)
    }

    /// `createSearchIndexes`: the created indexes' `{id, name}`.
    ///
    /// # Errors
    /// 2 for a malformed definition; 68 when the name exists with another
    /// definition; 39662 for `search` (full-text) indexes.
    pub async fn create_search_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        specs: &[Document],
    ) -> Result<Vec<Document>, MongoError> {
        let Some(m) = self.meta(sql, ns).await? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("Collection '{ns}' does not exist."),
            ));
        };
        self.search_table(sql, &ns.db).await?;
        let mut out = Vec::new();
        for s in specs {
            let t = s.get_str("type").unwrap_or("search");
            if t == "search" {
                // Chapter 39 (R5 round 5): full-text search indexes.
                let hook = self.text.as_ref().ok_or_else(|| {
                    MongoError::new(
                        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                        "full-text search indexes need the gateway's search service",
                    )
                })?;
                let def = s
                    .get_document("definition")
                    .map_err(|_| bad("a search index needs a 'definition'"))?;
                let name = s.get_str("name").unwrap_or("default").to_owned();
                let i = hook
                    .create(
                        sql,
                        ns,
                        &m,
                        crate::text::TextCreate::Search {
                            name,
                            definition: def.clone(),
                        },
                    )
                    .await?;
                out.push(doc! {"id": i.id, "name": i.name});
                continue;
            }
            if t != "vectorSearch" {
                return Err(bad(format!("unknown search index type '{t}'")));
            }
            let def = s
                .get_document("definition")
                .map_err(|_| bad("a search index needs a 'definition'"))?;
            VectorIndex::parse(def)?;
            let name = s.get_str("name").unwrap_or("default").to_owned();
            if let Some((id, _, _, old)) = self.search_rows(sql, ns, Some(&name)).await?.pop() {
                if &old == def {
                    out.push(doc! {"id": id, "name": name});
                    continue;
                }
                return Err(MongoError::new(
                    code::INDEX_KEY_SPECS_CONFLICT,
                    format!("Index {name} already exists with a different definition"),
                ));
            }
            let id = bson::oid::ObjectId::new().to_hex();
            let json = Bson::Document(def.clone())
                .into_relaxed_extjson()
                .to_string();
            sql.exec(&format!(
                "INSERT INTO {}.{} (coll, name, type, id, def) VALUES ({}, {}, 'vectorSearch', {}, {})",
                ident(&ns.db),
                ident(SEARCH_TABLE),
                str_lit(&ns.coll),
                str_lit(&name),
                str_lit(&id),
                str_lit(&json)
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
            out.push(doc! {"id": id, "name": name});
        }
        Ok(out)
    }

    /// `updateSearchIndex`.
    ///
    /// # Errors
    /// 27 when the index doesn't exist; 2 for a malformed definition.
    pub async fn update_search_index(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
        def: &Document,
    ) -> Result<(), MongoError> {
        if let Some(t) = &self.text
            && t.update(sql, ns, name, def).await?
        {
            return Ok(());
        }
        VectorIndex::parse(def)?;
        // A new definition: its side table is rebuilt on the next search.
        if let Some(m) = self.meta(sql, ns).await? {
            self.drop_vector_sides(sql, ns, &m.table, Some(name)).await;
        }
        let json = Bson::Document(def.clone())
            .into_relaxed_extjson()
            .to_string();
        let r = sql
            .exec(&format!(
                "UPDATE {}.{} SET def = {} WHERE coll = {} AND name = {}",
                ident(&ns.db),
                ident(SEARCH_TABLE),
                str_lit(&json),
                str_lit(&ns.coll),
                str_lit(name)
            ))
            .await;
        match r {
            Ok(d) if d.affected > 0 || !self.search_rows(sql, ns, Some(name)).await?.is_empty() => {
                Ok(())
            }
            Ok(_) => Err(MongoError::new(
                code::INDEX_NOT_FOUND,
                format!("Search index {name} not found"),
            )),
            Err(e) if e.is(1146) => Err(MongoError::new(
                code::INDEX_NOT_FOUND,
                format!("Search index {name} not found"),
            )),
            Err(e) => Err(mongo_error(&e)),
        }
    }

    /// `dropSearchIndex` (by name).
    ///
    /// # Errors
    /// SQL errors (dropping a missing index succeeds, as in Atlas).
    pub async fn drop_search_index(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
    ) -> Result<(), MongoError> {
        if let Some(t) = &self.text
            && t.drop_index(sql, ns, name).await?
        {
            return Ok(());
        }
        if let Some(m) = self.meta(sql, ns).await? {
            self.drop_vector_sides(sql, ns, &m.table, Some(name)).await;
        }
        match sql
            .exec(&format!(
                "DELETE FROM {}.{} WHERE coll = {} AND name = {}",
                ident(&ns.db),
                ident(SEARCH_TABLE),
                str_lit(&ns.coll),
                str_lit(name)
            ))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.is(1146) || e.is(1049) => Ok(()),
            Err(e) => Err(mongo_error(&e)),
        }
    }

    /// The `$vectorSearch` stage (exact).
    async fn vector_search(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        let index = spec
            .get_str("index")
            .map_err(|_| bad("$vectorSearch requires 'index'"))?;
        let path = spec
            .get_str("path")
            .map_err(|_| bad("$vectorSearch requires 'path'"))?;
        let qv: Vec<f64> = match spec.get("queryVector") {
            Some(Bson::Array(a)) => a
                .iter()
                .map(num)
                .collect::<Option<_>>()
                .ok_or_else(|| bad("queryVector must hold numbers"))?,
            Some(Bson::Binary(b)) if b.subtype == bson::spec::BinarySubtype::Vector => {
                let v = bson::binary::Vector::try_from(b.clone())
                    .map_err(|e| bad(format!("queryVector: {e}")))?;
                match v {
                    bson::binary::Vector::Float32(f) => f.into_iter().map(f64::from).collect(),
                    bson::binary::Vector::Int8(i) => i.into_iter().map(f64::from).collect(),
                    bson::binary::Vector::PackedBit(_) => {
                        return Err(bad("packed bit query vectors are not supported"));
                    }
                }
            }
            _ => return Err(bad("$vectorSearch requires 'queryVector' as an array")),
        };
        let limit = spec
            .get("limit")
            .and_then(num)
            .filter(|n| *n >= 1.0 && n.fract() == 0.0)
            .ok_or_else(|| {
                MongoError::new(
                    39_738,
                    "$vectorSearch 'limit' must be a positive whole number",
                )
            })?;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let limit = limit as usize;
        let exact = spec.get_bool("exact").unwrap_or(false);
        if !exact {
            let nc = spec.get("numCandidates").and_then(num).ok_or_else(|| {
                MongoError::new(
                    39_738,
                    "$vectorSearch requires 'numCandidates' unless exact is true",
                )
            })?;
            #[allow(clippy::cast_precision_loss)]
            if nc < limit as f64 || nc > 10_000.0 {
                return Err(MongoError::new(
                    39_738,
                    "numCandidates must be at least 'limit' and at most 10000",
                ));
            }
        } else if spec.contains_key("numCandidates") {
            return Err(bad("numCandidates is not allowed when exact is true"));
        }
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(Vec::new());
        };
        // As in Atlas, an unknown index finds nothing.
        let Some((_, _, _, def)) = self.search_rows(sql, ns, Some(index)).await?.pop() else {
            return Ok(Vec::new());
        };
        let vx = VectorIndex::parse(&def)?;
        if vx.path != path {
            return Err(bad(format!(
                "Path '{path}' needs to be indexed as vector in index '{index}'"
            )));
        }
        if qv.len() != vx.dims {
            return Err(MongoError::new(
                39_736,
                format!(
                    "queryVector has {} dimensions; index '{index}' has {}",
                    qv.len(),
                    vx.dims
                ),
            ));
        }
        if vx.similarity == "dotProduct" && qv.iter().all(|x| *x == 0.0) {
            return Err(MongoError::new(
                39_737,
                "a zero queryVector has no direction under dotProduct",
            ));
        }
        let fdoc = spec.get_document("filter").cloned().unwrap_or_default();
        let pred = filter::parse(&fdoc)?;
        let mut paths = Vec::new();
        filter_paths(&pred, &mut paths);
        if let Some(p) = paths.iter().find(|p| !vx.filters.contains(p)) {
            return Err(MongoError::new(
                39_739,
                format!("Path '{p}' needs to be indexed as filter in index '{index}'"),
            ));
        }
        let sqlp = translate(&pred, &m);
        let q = format!(
            "[{}]",
            qv.iter().map(|x| f64_lit(*x)).collect::<Vec<_>>().join(",")
        );
        let dist = vx.distance_sql(&q);
        let cond = format!("{} AND ({})", vx.valid_sql(), sqlp.sql);
        let cx = AggExpr { now: now_date() };
        // R33: approximate, from the side table's HNSW index.
        if !exact {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let nc = spec.get("numCandidates").and_then(num).unwrap_or(0.0) as usize;
            if let Some(out) = self
                .vector_ann(sql, ns, &m, index, &vx, &q, &dist, &cond, &pred, limit, nc)
                .await?
            {
                return Ok(out);
            }
        }
        let page = if sqlp.exact {
            limit
        } else {
            (limit * 4).max(100)
        };
        let mut out = Vec::new();
        let mut offset = 0usize;
        loop {
            let rows = sql
                .query(&format!(
                    "SELECT _id, doc, _types, {dist} AS d FROM {}.{} WHERE {cond} ORDER BY d, _id LIMIT {page} OFFSET {offset}",
                    ident(&ns.db),
                    ident(&m.table)
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
            let n = rows.len();
            for r in rows {
                let mut d = crate::json::to_bson(
                    &text(r.get(1).and_then(Option::as_ref)),
                    r.get(2)
                        .and_then(Option::as_ref)
                        .map(|b| String::from_utf8_lossy(b))
                        .as_deref(),
                )?;
                if !pred.matches_with(&d, &cx) {
                    continue;
                }
                let dv: f64 = text(r.get(3).and_then(Option::as_ref))
                    .parse()
                    .unwrap_or(f64::MAX);
                d.insert(SCORE_FIELD, Bson::Double(vx.score(dv)));
                out.push(d);
                if out.len() >= limit {
                    return Ok(out);
                }
            }
            if n < page || sqlp.exact {
                return Ok(out);
            }
            offset += n;
        }
    }

    /// Drops the side tables of `table`'s vector indexes (`name`: that
    /// one only). Failures are ignored: a leftover side table is harmless
    /// (a new index with the name rebuilds it from the documents).
    pub(crate) async fn drop_vector_sides(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        name: Option<&str>,
    ) {
        let Ok(rows) = self.search_rows(sql, ns, name).await else {
            return;
        };
        for (_, n, t, _) in rows {
            if t != "vectorSearch" {
                continue;
            }
            let side = vector_side_table(table, &n);
            let _ = sql
                .exec(&format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    ident(&ns.db),
                    ident(&side)
                ))
                .await;
        }
    }

    /// Brings the side table of vector index `name` up to date (creating
    /// and filling it when missing); its vector count.
    async fn vector_sync(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &crate::meta::CollMeta,
        name: &str,
        vx: &VectorIndex,
    ) -> Result<u64, MongoError> {
        let side = vector_side_table(&m.table, name);
        let st = format!("{}.{}", ident(&ns.db), ident(&side));
        let coll = format!("{}.{}", ident(&ns.db), ident(&m.table));
        let sync_key = format!("{}.{side}", ns.db);
        if let Some((at, n)) = SYNCED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&sync_key)
            && at.elapsed() < std::time::Duration::from_millis(self.cfg.vector_sync_ms)
        {
            return Ok(*n);
        }
        let probe = format!("SELECT COUNT(*), COALESCE(MAX(_v), 0) FROM {st}");
        let (count, newest) = match sql.query(&probe).await {
            Ok(r) => {
                let row = r.into_iter().next().unwrap_or_default();
                (
                    text(row.first().and_then(Option::as_ref))
                        .parse::<u64>()
                        .unwrap_or(0),
                    text(row.get(1).and_then(Option::as_ref))
                        .parse::<u64>()
                        .unwrap_or(0),
                )
            }
            Err(e) if e.is(1146) => {
                let (_, metric) = vx.side_distance();
                // The documents' `_v`, for the syncs' range reads.
                sql.exec(&format!(
                    "ALTER TABLE {coll} ADD INDEX IF NOT EXISTS _sprig_dix_v (_v)"
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
                sql.exec(&format!(
                    "CREATE TABLE IF NOT EXISTS {st} (_id VARBINARY(254) NOT NULL, \
                     v VECTOR({}) NOT NULL, _v BIGINT UNSIGNED NOT NULL, PRIMARY KEY (_id), KEY by_v (_v), \
                     VECTOR INDEX (v) M=16 DISTANCE={metric}) ENGINE=InnoDB",
                    vx.dims
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
                (0, 0)
            }
            Err(e) => return Err(mongo_error(&e)),
        };
        let lower = if count == 0 {
            0
        } else {
            newest.saturating_sub(SIDE_MARGIN)
        };
        let done = sql
            .exec(&format!(
                "INSERT INTO {st} (_id, v, _v) SELECT _id, VEC_FromText(JSON_EXTRACT(doc, {p})), _v FROM {coll} \
                 WHERE _v > {lower} AND LENGTH(_id) <= 254 AND {valid} \
                 ON DUPLICATE KEY UPDATE v = VALUES(v), _v = VALUES(_v)",
                p = vx.path_lit(),
                valid = vx.valid_sql()
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        let due = {
            let mut g = PURGED
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = format!("{}.{side}", ns.db);
            let due = g
                .get(&key)
                .is_none_or(|t| t.elapsed() >= std::time::Duration::from_secs(60));
            if due {
                g.insert(key, std::time::Instant::now());
            }
            due
        };
        if due {
            // Documents deleted, or whose vector went away.
            sql.exec(&format!(
                "DELETE FROM {st} WHERE _id NOT IN (SELECT _id FROM {coll} WHERE {})",
                vx.valid_sql()
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        }
        let n = count + done.affected;
        SYNCED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(sync_key, (std::time::Instant::now(), n));
        Ok(n)
    }

    /// Approximate `$vectorSearch` (R33): `None` when the index is too
    /// small for it (the exact search runs).
    #[allow(clippy::too_many_arguments)]
    async fn vector_ann(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &crate::meta::CollMeta,
        name: &str,
        vx: &VectorIndex,
        q: &str,
        dist: &str,
        cond: &str,
        pred: &Pred,
        limit: usize,
        num_candidates: usize,
    ) -> Result<Option<Vec<Document>>, MongoError> {
        let n = self.vector_sync(sql, ns, m, name, vx).await?;
        if n < self.cfg.vector_ann_min_rows {
            return Ok(None);
        }
        let side = format!(
            "{}.{}",
            ident(&ns.db),
            ident(&vector_side_table(&m.table, name))
        );
        let (f, _) = vx.side_distance();
        let cx = AggExpr { now: now_date() };
        let mut nc = num_candidates.max(limit);
        loop {
            let ids = sql
                .query(&format!(
                    "SELECT _id FROM {side} ORDER BY {f}(v, VEC_FromText({})) LIMIT {nc}",
                    str_lit(q)
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
            let got = ids.len();
            let keys: Vec<String> = ids
                .into_iter()
                .filter_map(|mut r| r.first_mut().and_then(Option::take))
                .map(|k| crate::sql::bin_lit(&k))
                .collect();
            let mut ranked: Vec<(f64, Vec<u8>, Document)> = Vec::new();
            for chunk in keys.chunks(500) {
                let rows = sql
                    .query(&format!(
                        "SELECT _id, doc, _types, {dist} AS d FROM {}.{} WHERE _id IN ({}) AND {cond}",
                        ident(&ns.db),
                        ident(&m.table),
                        chunk.join(", ")
                    ))
                    .await
                    .map_err(|e| mongo_error(&e))?;
                for r in rows {
                    let d = crate::json::to_bson(
                        &text(r.get(1).and_then(Option::as_ref)),
                        r.get(2)
                            .and_then(Option::as_ref)
                            .map(|b| String::from_utf8_lossy(b))
                            .as_deref(),
                    )?;
                    if !pred.matches_with(&d, &cx) {
                        continue;
                    }
                    let dv: f64 = text(r.get(3).and_then(Option::as_ref))
                        .parse()
                        .unwrap_or(f64::MAX);
                    let k = r.first().cloned().flatten().unwrap_or_default();
                    ranked.push((dv, k, d));
                }
            }
            // A filter that removed too many: look further, up to 10,000.
            if ranked.len() < limit && got == nc && nc < 10_000 {
                nc = (nc * 4).min(10_000);
                continue;
            }
            ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            return Ok(Some(
                ranked
                    .into_iter()
                    .take(limit)
                    .map(|(dv, _, mut d)| {
                        d.insert(SCORE_FIELD, Bson::Double(vx.score(dv)));
                        d
                    })
                    .collect(),
            ));
        }
    }

    /// `2dsphere` key parts of an index spec: `Some(path)` for a
    /// single-path `2dsphere` index, `None` for other indexes.
    ///
    /// # Errors
    /// 39662 for compound `2dsphere` indexes and other index types.
    pub(super) fn geo_key(key: &Document) -> Result<Option<String>, MongoError> {
        let geo: Vec<&String> = key
            .iter()
            .filter(|(_, v)| matches!(v, Bson::String(t) if t == "2dsphere"))
            .map(|(k, _)| k)
            .collect();
        if geo.is_empty() {
            return Ok(None);
        }
        if key.len() > 1 {
            return Err(MongoError::new(
                code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                "compound 2dsphere indexes are not supported in this release",
            ));
        }
        Ok(Some(geo[0].clone()))
    }

    /// Adds a `2dsphere` index: its columns, then its MariaDB index.
    pub(super) async fn create_geo_index(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        idx: &IndexDef,
        path: &str,
    ) -> Result<(), MongoError> {
        sql.exec(&geo::add_columns_sql(&ns.db, &m.table, path))
            .await
            .map_err(|e| mongo_error(&e))?;
        sql.exec(&geo::create_index_sql(
            &ns.db,
            &m.table,
            &idx.mariadb_name(),
            path,
        ))
        .await
        .map_err(|e| mongo_error(&e))?;
        Ok(())
    }

    /// Drops a dropped `2dsphere` index's columns.
    pub(super) async fn drop_geo_columns(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        idx: &IndexDef,
    ) -> Result<(), MongoError> {
        if !geo::is_2dsphere(idx) {
            return Ok(());
        }
        let Some((p, _)) = idx.key.first() else {
            return Ok(());
        };
        if geo::index_on(m, p).is_some() {
            return Ok(());
        }
        sql.exec(&geo::drop_columns_sql(&ns.db, &m.table, p))
            .await
            .map_err(|e| mongo_error(&e))?;
        // Its lines' and polygons' side table (R5 round 6).
        let _ = sql
            .exec(&format!(
                "DROP TABLE IF EXISTS {}.{}",
                ident(&ns.db),
                ident(&geo::side_table(&m.table, p))
            ))
            .await;
        Ok(())
    }
}

impl Engine {
    /// `m` with the side tables of `pred`'s geo paths synced (R5 round 6:
    /// lines and polygons skipped by their boxes), for one translation;
    /// `None` when no `2dsphere` path is queried.
    ///
    /// # Errors
    /// The backend's.
    pub(crate) async fn geo_sides(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        pred: &Pred,
    ) -> Result<Option<CollMeta>, MongoError> {
        let mut paths = Vec::new();
        geo_paths(pred, &mut paths);
        paths.retain(|p| geo::index_on(m, p).is_some());
        if paths.is_empty() {
            return Ok(None);
        }
        let mut sides = Document::new();
        for p in &paths {
            let min_v = self.geo_sync(sql, ns, m, p).await?;
            let st = format!(
                "{}.{}",
                ident(&ns.db),
                ident(&geo::side_table(&m.table, p))
            );
            sides.insert(
                p.clone(),
                doc! {"t": st, "v": i64::try_from(min_v).unwrap_or(i64::MAX)},
            );
        }
        let mut m = m.clone();
        m.options.insert(geo::SIDE_OPTION, sides);
        Ok(Some(m))
    }

    /// Brings `path`'s side table up to date (at most once a
    /// `vector_sync_ms` per process): every document written since the
    /// last sync, less a margin, whose value at the path isn't a point gets
    /// its box; a value with no location loses its row. Returns the `_v`
    /// past which documents may not be in the table yet.
    async fn geo_sync(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        path: &str,
    ) -> Result<u64, MongoError> {
        let side = geo::side_table(&m.table, path);
        let st = format!("{}.{}", ident(&ns.db), ident(&side));
        let coll = format!("{}.{}", ident(&ns.db), ident(&m.table));
        let key = format!("{}.{side}", ns.db);
        if let Some((at, v)) = SYNCED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            && at.elapsed() < std::time::Duration::from_millis(self.cfg.vector_sync_ms)
        {
            return Ok(*v);
        }
        let first = |rows: super::Rows| -> u64 {
            text(rows.first().and_then(|r| r.first()).and_then(Option::as_ref))
                .parse::<u64>()
                .unwrap_or(0)
        };
        let now_v = first(
            sql.query(&format!("SELECT COALESCE(MAX(_v), 0) FROM {coll}"))
                .await
                .map_err(|e| mongo_error(&e))?,
        );
        let marker = match sql.query(&format!("SELECT _v FROM {st} WHERE _id = ''")).await {
            Ok(r) => first(r),
            Err(e) if e.is(1146) => {
                sql.exec(&format!(
                    "ALTER TABLE {coll} ADD INDEX IF NOT EXISTS _sprig_dix_v (_v)"
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
                sql.exec(&geo::side_table_sql(&st))
                    .await
                    .map_err(|e| mongo_error(&e))?;
                0
            }
            Err(e) => return Err(mongo_error(&e)),
        };
        let lower = if marker == 0 {
            0
        } else {
            marker.saturating_sub(SIDE_MARGIN)
        };
        let parts: Vec<&str> = path.split('.').collect();
        let jp = str_lit(&json_path(&parts, &[]));
        let (lat, _) = geo::columns(path);
        let mut after: Option<Vec<u8>> = None;
        loop {
            let cond = after
                .as_ref()
                .map(|k| format!(" AND _id > {}", crate::sql::bin_lit(k)))
                .unwrap_or_default();
            let rows = sql
                .query(&format!(
                    "SELECT _id, JSON_EXTRACT(doc, {jp}), _v FROM {coll} WHERE _v > {lower} AND {} IS NULL \
                     AND JSON_EXTRACT(doc, {jp}) IS NOT NULL AND LENGTH(_id) <= 3000{cond} ORDER BY _id LIMIT 2000",
                    ident(&lat)
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
            let got = rows.len();
            let mut ups = Vec::new();
            let mut dels = Vec::new();
            for r in &rows {
                let id = r.first().cloned().flatten().unwrap_or_default();
                let v = r
                    .get(1)
                    .cloned()
                    .flatten()
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                    .and_then(|j| Bson::try_from(j).ok());
                let dv = text(r.get(2).and_then(Option::as_ref));
                match v.as_ref().and_then(geo::side_bounds) {
                    Some((a, b, c, d)) => ups.push(format!(
                        "({}, {}, {}, {}, {}, {})",
                        crate::sql::bin_lit(&id),
                        f64_lit(a),
                        f64_lit(b),
                        f64_lit(c),
                        f64_lit(d),
                        dv.parse::<u64>().unwrap_or(0)
                    )),
                    None => dels.push(crate::sql::bin_lit(&id)),
                }
                after = Some(id);
            }
            if !ups.is_empty() {
                sql.exec(&format!(
                    "INSERT INTO {st} (_id, lat_lo, lat_hi, lng_lo, lng_hi, _v) VALUES {} \
                     ON DUPLICATE KEY UPDATE lat_lo = VALUES(lat_lo), lat_hi = VALUES(lat_hi), \
                     lng_lo = VALUES(lng_lo), lng_hi = VALUES(lng_hi), _v = VALUES(_v)",
                    ups.join(", ")
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
            }
            if !dels.is_empty() {
                sql.exec(&format!(
                    "DELETE FROM {st} WHERE _id IN ({})",
                    dels.join(", ")
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
            }
            if got < 2000 {
                break;
            }
        }
        sql.exec(&format!(
            "INSERT INTO {st} (_id, lat_lo, lat_hi, lng_lo, lng_hi, _v) VALUES ('', 0, 0, 0, 0, {now_v}) \
             ON DUPLICATE KEY UPDATE _v = GREATEST(_v, VALUES(_v))"
        ))
        .await
        .map_err(|e| mongo_error(&e))?;
        let due = {
            let mut g = PURGED
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let due = g
                .get(&key)
                .is_none_or(|t| t.elapsed() >= std::time::Duration::from_secs(60));
            if due {
                g.insert(key.clone(), std::time::Instant::now());
            }
            due
        };
        if due {
            sql.exec(&format!(
                "DELETE FROM {st} WHERE _id <> '' AND _id NOT IN (SELECT _id FROM {coll} WHERE {} IS NULL)",
                ident(&lat)
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        }
        let min_v = now_v.saturating_sub(SIDE_MARGIN);
        SYNCED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, (std::time::Instant::now(), min_v));
        Ok(min_v)
    }
}
