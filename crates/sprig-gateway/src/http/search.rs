//! `sprig.v1.Search` (chapter 39, "Queries"; chapter 37; build step 11):
//! `Search.Query` over REST (`POST /v1/databases/{db}/search/{index}:query`)
//! and gRPC.
//!
//! ```text
//! Query(database, index, query_string | query, filter, limit, after, ...):
//!   the caller's session (privileges, row-level security, the router)
//!   the index by name, else 39820; SELECT on its object, else 1142
//!   the AST: the query string or the Atlas operator, AND the filter
//!     operators as unscored filter clauses
//!   ranked hits from a copy (on a worker or in this gateway), waited for as
//!     consistency (or a commit token) asks; facets in the same call
//!   from the cursor's position: re-read the hits' rows through the session;
//!     a hit whose row the caller can't read is skipped (fetching more, up to
//!     search.max_window, until the page is full)
//!   → hits {key, score, highlights, row}, facets, after, applied
//! ```
//!
//! The cursor is the position in the ranking of the page's last hit with
//! its key and score; the next page starts after that key (found again by
//! key, so writes that move hits around neither repeat nor skip the hit
//! the cursor names).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use base64::Engine as _;
use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::engine::SqlRunner as _;
use sprig_search::def::{FieldDef, IndexDef, Model};
use sprig_search::query::{SearchQuery, parse_atlas, parse_query_string};
use sprig_search::rpc::{Answer, Op};
use sprig_search::schema::Key;
use sprig_search::{Hit, highlight};
use tonic::{Request, Response, Status};

use super::sql::{Caller, SqlEngine};
use crate::mongo::sql::SqlSession;
use crate::search::catalog as scat;
use crate::search::{Consistency, SearchService};

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

fn u(v: &Value, k: &str) -> u64 {
    match v.get(k) {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, m)
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn q(s: &str) -> String {
    sprig_common::names::quote(s)
}

fn sql_err(e: &sprig_doc::engine::SqlError) -> SprigError {
    match e.errno {
        Some(n) => SprigError::standard(n, *b"42000", e.message.clone()),
        None => SprigError::internal(e.message.clone()),
    }
}

/// The page cursor.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Cursor {
    /// The ranking position of the last hit returned.
    p: usize,
    /// Its key.
    k: Key,
    /// Its score.
    s: f32,
}

impl Cursor {
    fn encode(&self) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(self).unwrap_or_default())
    }

    fn decode(t: &str) -> Result<Self> {
        let b = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(t)
            .map_err(|_| syntax("after: not a cursor of this API"))?;
        serde_json::from_slice(&b).map_err(|_| syntax("after: not a cursor of this API"))
    }
}

/// Where the next page starts in `hits`: after the cursor's key; a key no
/// longer ranked starts at the first hit ranked below the cursor.
fn start_of(hits: &[Hit], c: &Cursor) -> usize {
    if let Some(i) = hits.iter().position(|h| h.key == c.k) {
        return i + 1;
    }
    hits.iter()
        .position(|h| h.score < c.s || (h.score == c.s && h.key > c.k))
        .unwrap_or(hits.len())
}

/// A JSON object or an array of operators, as filter clauses.
fn filters(f: &str) -> Result<Vec<SearchQuery>> {
    if f.trim().is_empty() {
        return Ok(Vec::new());
    }
    let v: Value = serde_json::from_str(f).map_err(|e| syntax(format!("filter: {e}")))?;
    let list = match v {
        Value::Array(a) => a,
        o @ Value::Object(_) => vec![o],
        _ => return Err(syntax("filter: a JSON object or an array of them")),
    };
    list.iter().map(parse_atlas).collect()
}

/// The request's AST.
fn ast(body: &Value) -> Result<SearchQuery> {
    let qs = s(body, "query_string");
    let qo = s(body, "query");
    let main = match (qs.trim().is_empty(), qo.trim().is_empty()) {
        (false, true) => parse_query_string(&qs, &[])?,
        (true, false) => {
            let v: Value = serde_json::from_str(&qo).map_err(|e| syntax(format!("query: {e}")))?;
            parse_atlas(&v)?
        }
        (true, true) => SearchQuery::All,
        (false, false) => return Err(syntax("give query_string or query, not both")),
    };
    let filter = filters(&s(body, "filter"))?;
    Ok(if filter.is_empty() {
        main
    } else {
        SearchQuery::Bool {
            must: vec![main],
            should: Vec::new(),
            must_not: Vec::new(),
            filter,
            min_should: 0,
        }
    })
}

/// The facet ops of `facets` (as `$searchMeta` takes them) with names.
fn facet_ops(f: &str) -> Result<Vec<(String, Op)>> {
    if f.trim().is_empty() {
        return Ok(Vec::new());
    }
    let v: Value = serde_json::from_str(f).map_err(|e| syntax(format!("facets: {e}")))?;
    let Value::Object(m) = v else {
        return Err(syntax("facets: a JSON object of facets"));
    };
    let mut out = Vec::new();
    for (name, spec) in m {
        let path = s(&spec, "path");
        if path.is_empty() {
            return Err(syntax(format!("facet {name}: no path")));
        }
        let n = usize::try_from(u(&spec, "numBuckets")).unwrap_or(10);
        let bounds = spec
            .get("boundaries")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_f64).collect());
        out.push((
            name,
            Op::Facet {
                path,
                n: if n == 0 { 10 } else { n },
                bounds,
            },
        ));
    }
    Ok(out)
}

/// One re-read hit.
struct Seen {
    key: Value,
    row: Option<Value>,
    texts: HashMap<String, String>,
}

/// Reads the rows of `hits` through `sql`: the visible ones, keyed by
/// their search key.
async fn reread(
    sql: &mut SqlSession,
    def: &IndexDef,
    hits: &[Hit],
    texts: &[&FieldDef],
    with_row: bool,
) -> Result<HashMap<Key, Seen>> {
    let mut out = HashMap::new();
    if hits.is_empty() {
        return Ok(out);
    }
    let table = format!("{}.{}", q(&def.db), q(&def.table));
    if def.model == Model::Table {
        let keys: Vec<String> = def.key_columns.iter().map(|c| q(c)).collect();
        let key_expr = if keys.len() == 1 {
            keys[0].clone()
        } else {
            format!("({})", keys.join(", "))
        };
        let row_expr = if with_row {
            let cols = sql
                .query(&format!("SHOW COLUMNS FROM {table}"))
                .await
                .map_err(|e| sql_err(&e))?;
            let pairs: Vec<String> = cols
                .iter()
                .filter_map(|r| r.first().cloned().flatten())
                .map(|c| {
                    let c = String::from_utf8_lossy(&c).into_owned();
                    format!("{}, {}", lit(&c), q(&c))
                })
                .collect();
            format!(", JSON_OBJECT({})", pairs.join(", "))
        } else {
            String::new()
        };
        let text_cols: String = texts
            .iter()
            .map(|f| format!(", {}", q(&f.source_ref)))
            .collect();
        for chunk in hits.chunks(500) {
            let list: Vec<String> = chunk
                .iter()
                .map(|h| h.key.sql(Model::Table))
                .collect::<Result<_>>()?;
            let rows = sql
                .query(&format!(
                    "SELECT {}{text_cols}{row_expr} FROM {table} WHERE {key_expr} IN ({})",
                    keys.join(", "),
                    list.join(", ")
                ))
                .await
                .map_err(|e| sql_err(&e))?;
            let nk = keys.len();
            for r in rows {
                let parts: Vec<sprig_search::KeyPart> = (0..nk)
                    .map(|i| {
                        let ty = def.key_types.get(i).map_or("", String::as_str);
                        crate::search::runtime::key_part_from_text(
                            r.get(i).and_then(Option::as_deref).unwrap_or_default(),
                            ty,
                        )
                    })
                    .collect();
                let mut key = serde_json::Map::new();
                for (i, c) in def.key_columns.iter().enumerate() {
                    key.insert(
                        c.clone(),
                        match &parts[i] {
                            sprig_search::KeyPart::Int(x) => json!(x),
                            sprig_search::KeyPart::UInt(x) => json!(x),
                            sprig_search::KeyPart::Str(x) | sprig_search::KeyPart::Text(x) => {
                                json!(x)
                            }
                            sprig_search::KeyPart::Bin(b) => {
                                json!(base64::engine::general_purpose::STANDARD.encode(b))
                            }
                        },
                    );
                }
                let mut t = HashMap::new();
                for (j, f) in texts.iter().enumerate() {
                    if let Some(Some(v)) = r.get(nk + j) {
                        t.insert(f.name.clone(), String::from_utf8_lossy(v).into_owned());
                    }
                }
                let row = with_row
                    .then(|| r.get(nk + texts.len()).cloned().flatten())
                    .flatten()
                    .and_then(|b| serde_json::from_slice(&b).ok());
                out.insert(
                    Key::of_parts(&parts),
                    Seen {
                        key: Value::Object(key),
                        row,
                        texts: t,
                    },
                );
            }
        }
    } else {
        for chunk in hits.chunks(500) {
            let list: Vec<String> = chunk
                .iter()
                .map(|h| sprig_doc::sql::bin_lit(&h.key.0))
                .collect();
            let rows = sql
                .query(&format!(
                    "SELECT _id, doc, _types FROM {table} WHERE _id IN ({})",
                    list.join(", ")
                ))
                .await
                .map_err(|e| sql_err(&e))?;
            for r in rows {
                let id = r.first().cloned().flatten().unwrap_or_default();
                let doc = r.get(1).cloned().flatten().unwrap_or_default();
                let types = r.get(2).cloned().flatten();
                let doc = String::from_utf8_lossy(&doc).into_owned();
                let types = types.map(|t| String::from_utf8_lossy(&t).into_owned());
                let bson_doc = sprig_doc::json::to_bson(&doc, types.as_deref())
                    .map_err(|e| SprigError::internal(e.message))?;
                let ejson = bson::Bson::Document(bson_doc.clone()).into_relaxed_extjson();
                let id_json = ejson.get("_id").cloned().unwrap_or(Value::Null);
                let plain: Value = serde_json::from_str(&doc).unwrap_or(Value::Null);
                let mut t = HashMap::new();
                for f in texts {
                    let mut v = &plain;
                    for part in f.name.split('.') {
                        v = v.get(part).unwrap_or(&Value::Null);
                    }
                    if let Some(x) = v.as_str() {
                        t.insert(f.name.clone(), x.to_owned());
                    }
                }
                out.insert(
                    Key(id),
                    Seen {
                        key: json!({"_id": id_json}),
                        row: with_row.then_some(ejson),
                        texts: t,
                    },
                );
            }
        }
    }
    Ok(out)
}

/// Runs `Search.Query` for `caller` with the request `body` (proto3 JSON,
/// path variables merged); the reply as proto3 JSON.
///
/// # Errors
/// 39820-39827, 1049, 1142 and the backend's.
#[allow(clippy::too_many_lines)]
pub async fn call(engine: &Arc<SqlEngine>, caller: &Caller, rpc: &str, body: &Value) -> Result<Value> {
    if rpc != "Search.Query" {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("no search call {rpc}"),
        ));
    }
    let svc: &Arc<SearchService> = crate::search::service().ok_or_else(|| {
        let mut e = SprigError::new(SprigCode::SearchUnavailable, "search isn't running on this gateway");
        e.retryable = true;
        e
    })?;
    let database = s(body, "database");
    let name = s(body, "index");
    let mut sql = SqlSession::open(
        &engine.sessions,
        &engine.executor,
        &caller.principal,
        caller.listener,
        caller.generation,
        caller.peer,
        caller.tls.clone(),
        64 << 20,
    )?;
    let r = run(svc, &mut sql, &database, &name, body).await;
    sql.close().await;
    r
}

#[allow(clippy::too_many_lines)]
async fn run(
    svc: &Arc<SearchService>,
    sql: &mut SqlSession,
    database: &str,
    name: &str,
    body: &Value,
) -> Result<Value> {
    let tenant = TenantId(sql.tenant());
    if tenant.0 == 0 {
        return Err(SprigError::new(
            SprigCode::SearchIndexNotFound,
            "search indexes belong to tenants",
        ));
    }
    let knobs = svc.knobs();
    let cat = svc.catalog().snapshot(tenant, 0).await?;
    let db_id = svc.catalog().db_id(&cat, database).ok_or_else(|| {
        SprigError::standard(1049, *b"42000", format!("Unknown database '{database}'"))
    })?;
    let def = scat::state(&cat).by_name(db_id.0, name).ok_or_else(|| {
        SprigError::new(
            SprigCode::SearchIndexNotFound,
            format!("search index '{name}' doesn't exist in {database}"),
        )
    })?;
    // SELECT on the object, checked as a read of it.
    sql.query(&format!(
        "SELECT 1 FROM {}.{} LIMIT 0",
        q(&def.db),
        q(&def.table)
    ))
    .await
    .map_err(|e| sql_err(&e))?;
    let sort = s(body, "sort");
    if !(sort.is_empty() || sort.eq_ignore_ascii_case("score")) {
        return Err(SprigError::new(
            SprigCode::SearchUnsupported,
            "Search.Query sorts by score only in this release",
        ));
    }
    let query = ast(body)?;
    let limit = match u(body, "limit") {
        0 => 10,
        n => usize::try_from(n).unwrap_or(usize::MAX),
    }
    .min(knobs.max_window);
    let cursor = match s(body, "after") {
        a if a.is_empty() => None,
        a => Some(Cursor::decode(&a)?),
    };
    let consistency = if !s(body, "commit_token").is_empty() {
        Consistency::Latest
    } else {
        match s(body, "consistency") {
            c if c.is_empty() => knobs.consistency,
            c => Consistency::parse(&c)?,
        }
    };
    let highlight: Vec<String> = body
        .get("highlight")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_owned)).collect())
        .unwrap_or_default();
    let texts: Vec<&FieldDef> = highlight
        .iter()
        .filter_map(|n| {
            def.fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(n) && f.kind.is_text())
        })
        .collect();
    let with_row = body
        .get("include_row")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let facets = facet_ops(&s(body, "facets"))?;
    let analyzers = &scat::state(&cat).analyzers;
    // Ranked hits, fetched deeper while hidden rows leave the page short.
    let mut k = (cursor.as_ref().map_or(0, |c| c.p + 1) + limit)
        .saturating_mul(knobs.overfetch.max(1))
        .min(knobs.max_window)
        .max(1);
    let mut first = true;
    let mut facet_out = serde_json::Map::new();
    let mut applied = String::new();
    let (page, next) = loop {
        let mut ops = vec![Op::Top {
            k,
            scoring: knobs.scoring,
        }];
        if first {
            ops.extend(facets.iter().map(|(_, o)| o.clone()));
        }
        let (answers, ap) = svc
            .query(tenant, &def, analyzers, query.clone(), ops, consistency)
            .await?;
        applied = ap;
        let mut it = answers.into_iter();
        let Some(Answer::Hits(hits)) = it.next() else {
            return Err(SprigError::internal("a search copy answered without hits"));
        };
        if first {
            for ((fname, _), a) in facets.iter().zip(it) {
                if let Answer::Buckets(b) = a {
                    let buckets: Vec<Value> = b
                        .into_iter()
                        .map(|x| json!({"_id": x.value, "count": x.count}))
                        .collect();
                    facet_out.insert(fname.clone(), json!({ "buckets": buckets }));
                }
            }
            first = false;
        }
        let start = cursor.as_ref().map_or(0, |c| start_of(&hits, c));
        let rest = hits.get(start..).unwrap_or_default();
        let seen = reread(sql, &def, rest, &texts, with_row).await?;
        let mut page = Vec::new();
        let mut last = None;
        for (i, h) in rest.iter().enumerate() {
            if page.len() == limit {
                break;
            }
            last = Some(start + i);
            if let Some(v) = seen.get(&h.key) {
                page.push((h.clone(), v));
            }
        }
        let exhausted = hits.len() < k;
        if page.len() == limit || exhausted || k >= knobs.max_window {
            let more = page.len() == limit && !(exhausted && last == Some(hits.len() - 1));
            let next = if more {
                last.and_then(|p| {
                    hits.get(p).map(|h| {
                        Cursor {
                            p,
                            k: h.key.clone(),
                            s: h.score,
                        }
                        .encode()
                    })
                })
            } else {
                None
            };
            let page: Vec<Value> = page
                .into_iter()
                .map(|(h, v)| {
                    let mut hl = serde_json::Map::new();
                    if !texts.is_empty()
                        && let Ok(schema) = sprig_search::IndexSchema::build(&def, analyzers)
                    {
                        let terms = crate::exec::search::query_terms(&query, &schema);
                        for f in &texts {
                            let Some(t) = v.texts.get(&f.name) else { continue };
                            let Ok(an) = schema
                                .analyzers
                                .get(f.index_analyzer(&def.default_analyzer))
                            else {
                                continue;
                            };
                            if let Some(fr) =
                                highlight::fragment(an, t, &terms, knobs.highlight_chars)
                            {
                                hl.insert(f.name.clone(), json!([fr]));
                            }
                        }
                    }
                    json!({
                        "key": v.key.to_string(),
                        "score": f64::from(h.score),
                        "highlights": if hl.is_empty() { String::new() } else { Value::Object(hl).to_string() },
                        "row": v.row.as_ref().map(Value::to_string).unwrap_or_default(),
                    })
                })
                .collect();
            break (page, next);
        }
        k = k.saturating_mul(3).min(knobs.max_window);
    };
    Ok(json!({
        "hits": page,
        "facets": if facet_out.is_empty() { String::new() } else { Value::Object(facet_out).to_string() },
        "after": next.unwrap_or_default(),
        "applied": applied,
    }))
}

// ---- gRPC ----

use sprig_api::proto::v1 as pb;

/// `sprig.v1.Search` over gRPC: each message as the REST JSON.
#[derive(Clone)]
pub struct SearchSvc(pub Arc<super::Core>);

#[tonic::async_trait]
impl pb::search_server::Search for SearchSvc {
    async fn query(
        &self,
        req: Request<pb::SearchQueryRequest>,
    ) -> std::result::Result<Response<pb::SearchQueryResponse>, Status> {
        metrics::counter!("sprig_api_requests_total", "service" => "sprig.v1.Search", "method" => "Query", "protocol" => "grpc").increment(1);
        let c = super::grpc::caller(&self.0, &req).await?;
        let r = req.get_ref();
        let body = json!({
            "database": r.database, "index": r.index, "query_string": r.query_string,
            "query": r.query, "filter": r.filter, "limit": r.limit, "after": r.after,
            "highlight": r.highlight, "facets": r.facets, "consistency": r.consistency,
            "commit_token": r.commit_token, "include_row": r.include_row, "sort": r.sort,
        });
        let v = call(&self.0.engine, &c, "Search.Query", &body)
            .await
            .map_err(|e| super::grpc::st(&e))?;
        let hits = v
            .get("hits")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .map(|h| pb::SearchHit {
                        key: s(h, "key"),
                        score: h.get("score").and_then(Value::as_f64).unwrap_or(0.0),
                        highlights: s(h, "highlights"),
                        row: s(h, "row"),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Response::new(pb::SearchQueryResponse {
            hits,
            facets: s(&v, "facets"),
            after: s(&v, "after"),
            applied: s(&v, "applied"),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(k: u8, score: f32) -> Hit {
        Hit {
            score,
            part: 0,
            key: Key(vec![k]),
        }
    }

    #[test]
    fn cursors_restart_after_their_key() {
        let hits = vec![hit(1, 3.0), hit(2, 2.0), hit(3, 2.0), hit(4, 1.0)];
        let c = Cursor {
            p: 1,
            k: Key(vec![2]),
            s: 2.0,
        };
        assert_eq!(Cursor::decode(&c.encode()).unwrap(), c);
        assert_eq!(start_of(&hits, &c), 2);
        // The key left the ranking: the first hit ranked below it.
        let gone = Cursor {
            p: 1,
            k: Key(vec![9]),
            s: 2.0,
        };
        assert_eq!(start_of(&hits, &gone), 3);
        assert!(Cursor::decode("not a cursor").is_err());
    }

    #[test]
    fn filters_become_unscored_clauses() {
        let body = json!({"query_string": "red shoe",
            "filter": r#"[{"range": {"path": "price", "lt": 100}}]"#});
        match ast(&body).unwrap() {
            SearchQuery::Bool { must, filter, .. } => {
                assert_eq!(must.len(), 1);
                assert_eq!(filter.len(), 1);
            }
            other => panic!("{other:?}"),
        }
        assert!(ast(&json!({"query_string": "a", "query": "{}"})).is_err());
        assert_eq!(ast(&json!({})).unwrap(), SearchQuery::All);
        let f = facet_ops(r#"{"b": {"type": "string", "path": "brand", "numBuckets": 3}}"#).unwrap();
        assert_eq!(f.len(), 1);
    }
}
