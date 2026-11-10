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
//! (MariaDB refuses a vector index on a generated column: 1252), which
//! belongs with chapter 39's search build.

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
        (Some("vectorSearchScore"), Some(Bson::Document(d))) => {
            d.get(SCORE_FIELD).cloned().unwrap_or(Bson::Null)
        }
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
                        .ok_or_else(|| bad("numDimensions must be a whole number from 1 to 8192"))?;
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

impl Engine {
    /// `find` with `$near` or `$nearSphere`: candidates through the
    /// `2dsphere` index's box, sorted by distance (then by `sort`, which
    /// MongoDB lets re-order them), skipped, limited and projected.
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
        const OWN: [&str; 3] = ["$geoNear", "$vectorSearch", "$listSearchIndexes"];
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
        let field = spec.get_str("distanceField").map_err(|_| {
            MongoError::new(25_278, "$geoNear requires a 'distanceField' argument")
        })?;
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
            q.push_str(&format!(" AND name = {}", str_lit(n)));
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
                let def = serde_json::from_str::<serde_json::Value>(&text(r.get(3).and_then(Option::as_ref)))
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
        Ok(self
            .search_rows(sql, ns, name)
            .await?
            .into_iter()
            .map(|(id, name, t, def)| {
                doc! {"id": id, "name": name, "type": t, "status": "READY", "queryable": true,
                "latestDefinition": def}
            })
            .collect())
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
        if self.meta(sql, ns).await?.is_none() {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("Collection '{ns}' does not exist."),
            ));
        }
        self.search_table(sql, &ns.db).await?;
        let mut out = Vec::new();
        for s in specs {
            let t = s.get_str("type").unwrap_or("search");
            if t != "vectorSearch" {
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    "search indexes of type 'search' (full text, chapter 39) are not supported in this release",
                ));
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
            let json = Bson::Document(def.clone()).into_relaxed_extjson().to_string();
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
        VectorIndex::parse(def)?;
        let json = Bson::Document(def.clone()).into_relaxed_extjson().to_string();
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
            .ok_or_else(|| MongoError::new(39_738, "$vectorSearch 'limit' must be a positive whole number"))?;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let limit = limit as usize;
        let exact = spec.get_bool("exact").unwrap_or(false);
        if !exact {
            let nc = spec.get("numCandidates").and_then(num).ok_or_else(|| {
                MongoError::new(39_738, "$vectorSearch requires 'numCandidates' unless exact is true")
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
            return Err(MongoError::new(39_737, "a zero queryVector has no direction under dotProduct"));
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
        let page = if sqlp.exact { limit } else { (limit * 4).max(100) };
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
                let dv: f64 = text(r.get(3).and_then(Option::as_ref)).parse().unwrap_or(f64::MAX);
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
        Ok(())
    }
}
