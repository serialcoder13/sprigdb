//! Hybrid docs search.
//!
//! ```text
//! q_vec := embed(query)                        -- none: keyword only
//! bm25  := MATCH(text, heading_path, terms) AGAINST (query) top 40,
//!          on the version, audience-filtered
//! vec   := VEC_DISTANCE_COSINE(embedding, q_vec) top 40 through the VECTOR index
//!          (200 read, then filtered by version and audience)
//! hits  := RRF(bm25 weighted keyword_weight(query), vec), k = 60;
//!          at most 2 per page; the first `limit`
//! ```
//!
//! `keyword_weight` counts the full-text list three times when the query
//! names an exact identifier (an error code or name, a SQLSTATE, a setting
//! key), which full-text matches exactly through the `terms` column and the
//! embedding model doesn't know.

use std::collections::HashMap;
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};
use serde::Serialize;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_docs::pack::hex;
use sprig_docs::search::{
    CANDIDATES, MAX_PER_PAGE, RRF_K, cap_per_page, keyword_weight, rrf_weighted,
};
use sprig_docs::table::{self, AUDIENCES};

use super::DocsService;

/// The longest query, in characters.
pub const MAX_QUERY: usize = 2000;
/// The most hits a search returns.
pub const MAX_LIMIT: usize = 50;

/// A search request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchRequest {
    /// What to look for.
    pub query: String,
    /// `user`, `dba` or `operator`: only pages for that audience.
    pub audience: Option<String>,
    /// Hits wanted (`docs.search_limit` when absent; at most
    /// [`MAX_LIMIT`]).
    pub limit: Option<usize>,
    /// The release (this gateway's, or the newest loaded, when absent).
    pub version: Option<String>,
}

/// One hit.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Hit {
    /// The chunk's id, hexadecimal.
    pub chunk_id: String,
    /// The page's path under `docs/`.
    pub page: String,
    /// The page's title.
    pub title: String,
    /// The headings above the chunk.
    pub heading_path: String,
    /// The page's audiences.
    pub audience: Vec<String>,
    /// The chunk's Markdown.
    pub text: String,
    /// The fused score.
    pub score: f64,
    /// Rank in the full-text list (1 is first), if it is there.
    pub bm25_rank: Option<usize>,
    /// Rank in the vector list, if it is there.
    pub vector_rank: Option<usize>,
}

/// A search's answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchReply {
    /// The release searched.
    pub version: String,
    /// `hybrid`, or `keyword` without a query embedding.
    pub mode: &'static str,
    /// Best first.
    pub hits: Vec<Hit>,
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn db(what: &str, e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::BackendUnavailable,
        format!("docs search: {what}"),
    )
    .with_source(e)
}

/// Checks a request: a query, a known audience, a limit in range.
///
/// # Errors
/// 39790-style request errors naming the field.
pub fn validate(
    req: &SearchRequest,
    default_limit: usize,
) -> Result<(String, Option<String>, usize)> {
    let q = req.query.trim();
    if q.is_empty() {
        return Err(invalid("query is required"));
    }
    if q.chars().count() > MAX_QUERY {
        return Err(invalid(format!(
            "query is longer than {MAX_QUERY} characters"
        )));
    }
    let audience = match req.audience.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(a) => {
            let a = a.to_ascii_lowercase();
            if !AUDIENCES.contains(&a.as_str()) {
                return Err(invalid(format!(
                    "audience must be one of {}",
                    AUDIENCES.join(", ")
                )));
            }
            Some(a)
        }
    };
    let limit = match req.limit {
        Some(0) => return Err(invalid("limit must be at least 1")),
        Some(n) => n.min(MAX_LIMIT),
        None => default_limit.clamp(1, MAX_LIMIT),
    };
    Ok((q.to_owned(), audience, limit))
}

/// The full-text list: chunk ids, best first.
async fn keyword_ids(
    conn: &mut Conn,
    version: &str,
    query: &str,
    audience: Option<&str>,
) -> Result<Vec<Vec<u8>>> {
    let filter = if audience.is_some() {
        " AND FIND_IN_SET(?, audience)"
    } else {
        ""
    };
    let sql = format!(
        "SELECT chunk_id, MATCH(text, heading_path, terms) AGAINST (? IN NATURAL LANGUAGE MODE) AS s \
         FROM {} WHERE version = ? \
         AND MATCH(text, heading_path, terms) AGAINST (? IN NATURAL LANGUAGE MODE){filter} \
         ORDER BY s DESC, chunk_id LIMIT {CANDIDATES}",
        table::qualified(table::CHUNKS)
    );
    let mut params: Vec<Value> = vec![query.into(), version.into(), query.into()];
    if let Some(a) = audience {
        params.push(a.into());
    }
    let rows: Vec<(Vec<u8>, f64)> = conn
        .exec(sql, params)
        .await
        .map_err(|e| db("full-text", e))?;
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}

/// Nearest neighbours read from the vector index before the version and
/// audience filters apply: every kept release and audience shares the
/// index, so it is read past [`CANDIDATES`].
const VECTOR_OVERSAMPLE: usize = 5;

/// The vector list: chunk ids, nearest first. The `VECTOR` index answers
/// the inner `ORDER BY ... LIMIT` (a few milliseconds); an exact scan of
/// a few thousand rows took a few hundred.
async fn vector_ids(
    conn: &mut Conn,
    version: &str,
    q: &[f32],
    audience: Option<&str>,
) -> Result<Vec<Vec<u8>>> {
    let filter = if audience.is_some() {
        " AND FIND_IN_SET(?, audience)"
    } else {
        ""
    };
    let sql = format!(
        "SELECT chunk_id FROM (SELECT chunk_id, version, audience, \
         VEC_DISTANCE_COSINE(embedding, VEC_FromText(?)) AS d FROM {} \
         ORDER BY VEC_DISTANCE_COSINE(embedding, VEC_FromText(?)) LIMIT {}) AS near \
         WHERE version = ?{filter} ORDER BY d, chunk_id LIMIT {CANDIDATES}",
        table::qualified(table::CHUNKS),
        CANDIDATES * VECTOR_OVERSAMPLE
    );
    let v = table::vector_text(q);
    let mut params: Vec<Value> = vec![v.clone().into(), v.into(), version.into()];
    if let Some(a) = audience {
        params.push(a.into());
    }
    conn.exec(sql, params).await.map_err(|e| db("vector", e))
}

struct Meta {
    page: String,
    title: String,
    heading_path: String,
    audience: String,
    text: String,
}

async fn metadata(
    conn: &mut Conn,
    version: &str,
    ids: &[Vec<u8>],
) -> Result<HashMap<Vec<u8>, Meta>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let marks = vec!["?"; ids.len()].join(", ");
    let sql = format!(
        "SELECT chunk_id, page, title, heading_path, CAST(audience AS CHAR), text FROM {} \
         WHERE version = ? AND chunk_id IN ({marks})",
        table::qualified(table::CHUNKS)
    );
    let mut params: Vec<Value> = vec![version.into()];
    params.extend(ids.iter().map(|i| Value::Bytes(i.clone())));
    let rows: Vec<(Vec<u8>, String, String, String, String, String)> = conn
        .exec(sql, params)
        .await
        .map_err(|e| db("reading hits", e))?;
    Ok(rows
        .into_iter()
        .map(|(id, page, title, heading_path, audience, text)| {
            (
                id,
                Meta {
                    page,
                    title,
                    heading_path,
                    audience,
                    text,
                },
            )
        })
        .collect())
}

/// One fused hit: the chunk id, its score, and its ranks (from 1) in the
/// full-text and vector lists.
pub type Fused = (Vec<u8>, f64, Option<usize>, Option<usize>);

/// Fuses the two lists into hits (pure, for tests): reciprocal rank with
/// the keyword list weighted, at most [`MAX_PER_PAGE`] per page, `limit`
/// hits. `page_of` names each id's page.
#[must_use]
pub fn fuse<'a>(
    keyword: &[Vec<u8>],
    vector: &[Vec<u8>],
    keyword_weight: f64,
    page_of: impl Fn(&[u8]) -> &'a str,
    limit: usize,
) -> Vec<Fused> {
    let mut ids: Vec<Vec<u8>> = Vec::new();
    let mut index = |id: &Vec<u8>| -> usize {
        if let Some(i) = ids.iter().position(|x| x == id) {
            i
        } else {
            ids.push(id.clone());
            ids.len() - 1
        }
    };
    let k: Vec<usize> = keyword.iter().map(&mut index).collect();
    let v: Vec<usize> = vector.iter().map(&mut index).collect();
    let fused = rrf_weighted(&[(&k, keyword_weight), (&v, 1.0)], RRF_K);
    let capped = cap_per_page(&fused, |i| page_of(&ids[i]), MAX_PER_PAGE, limit);
    capped
        .into_iter()
        .map(|(i, score)| {
            (
                ids[i].clone(),
                score,
                k.iter().position(|x| *x == i).map(|r| r + 1),
                v.iter().position(|x| *x == i).map(|r| r + 1),
            )
        })
        .collect()
}

impl DocsService {
    /// Runs a search for a caller of `tenant` (see the module docs).
    ///
    /// # Errors
    /// Request errors; 39989 when docs are unavailable; the backend's.
    pub async fn search(
        &self,
        tenant: Option<TenantId>,
        req: &SearchRequest,
    ) -> Result<SearchReply> {
        let started = Instant::now();
        let r = self.search_inner(tenant, req).await;
        let (mode, result) = match &r {
            Ok(rep) if rep.hits.is_empty() => (rep.mode, "empty"),
            Ok(rep) => (rep.mode, "ok"),
            Err(e) if e.code() == Some(SprigCode::AdminRequestInvalid) => ("none", "invalid"),
            Err(e) if e.code() == Some(SprigCode::DocsUnavailable) => ("none", "unavailable"),
            Err(_) => ("none", "error"),
        };
        metrics::counter!("sprig_docs_search_total", "result" => result).increment(1);
        if mode != "none" {
            metrics::histogram!("sprig_docs_search_seconds", "mode" => mode)
                .record(started.elapsed().as_secs_f64());
        }
        r
    }

    async fn search_inner(
        &self,
        tenant: Option<TenantId>,
        req: &SearchRequest,
    ) -> Result<SearchReply> {
        let (query, audience, limit) = validate(req, self.default_limit())?;
        // The embedding first: the connection isn't held while the model
        // runs (or loads, the first time).
        let q_vec = self.embed_query(&query).await;
        let mut t = self.target(tenant, req.version.as_deref()).await?;
        let keyword = keyword_ids(&mut t.conn, &t.version, &query, audience.as_deref()).await?;
        let vector = match &q_vec {
            Some(q) => vector_ids(&mut t.conn, &t.version, q, audience.as_deref()).await?,
            None => Vec::new(),
        };
        let mut all: Vec<Vec<u8>> = keyword.clone();
        for v in &vector {
            if !all.contains(v) {
                all.push(v.clone());
            }
        }
        let meta = metadata(&mut t.conn, &t.version, &all).await?;
        let fused = fuse(
            &keyword,
            &vector,
            keyword_weight(&query),
            |id| meta.get(id).map_or("", |m| m.page.as_str()),
            limit,
        );
        let hits = fused
            .into_iter()
            .filter_map(|(id, score, bm25_rank, vector_rank)| {
                let m = meta.get(&id)?;
                Some(Hit {
                    chunk_id: hex(&id),
                    page: m.page.clone(),
                    title: m.title.clone(),
                    heading_path: m.heading_path.clone(),
                    audience: m
                        .audience
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .collect(),
                    text: m.text.clone(),
                    score,
                    bm25_rank,
                    vector_rank,
                })
            })
            .collect();
        Ok(SearchReply {
            version: t.version,
            mode: if q_vec.is_some() { "hybrid" } else { "keyword" },
            hits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_checked() {
        let r = |q: &str| SearchRequest {
            query: q.into(),
            ..SearchRequest::default()
        };
        assert!(validate(&r("  "), 8).is_err());
        assert_eq!(validate(&r(" lag "), 8).unwrap(), ("lag".into(), None, 8));
        let mut x = r("lag");
        x.audience = Some("DBA".into());
        x.limit = Some(500);
        assert_eq!(
            validate(&x, 8).unwrap(),
            ("lag".into(), Some("dba".into()), MAX_LIMIT)
        );
        x.audience = Some("root".into());
        assert!(validate(&x, 8).is_err());
        x.audience = None;
        x.limit = Some(0);
        assert!(validate(&x, 8).is_err());
        assert!(validate(&r(&"x".repeat(MAX_QUERY + 1)), 8).is_err());
    }

    #[test]
    fn fusion_weights_ranks_and_caps_pages() {
        let id = |b: u8| vec![b];
        let pages: HashMap<Vec<u8>, &str> = [
            (id(1), "a.md"),
            (id(2), "a.md"),
            (id(3), "a.md"),
            (id(4), "b.md"),
            (id(5), "c.md"),
        ]
        .into_iter()
        .collect();
        let page = |i: &[u8]| pages[i];
        // 1 and 2 first by keywords, 4 first by vector.
        let k = [id(1), id(2), id(3)];
        let v = [id(4), id(1), id(5)];
        let hits = fuse(&k, &v, 1.0, page, 10);
        let order: Vec<u8> = hits.iter().map(|h| h.0[0]).collect();
        // 1 is in both lists; a.md keeps two hits.
        assert_eq!(order[0], 1);
        assert_eq!(order.iter().filter(|x| [1, 2, 3].contains(*x)).count(), 2);
        assert_eq!(hits[0].2, Some(1));
        assert_eq!(hits[0].3, Some(2));
        // An identifier query weights the keyword list: 2 beats 4.
        let hits = fuse(&k, &v, 3.0, page, 10);
        let p2 = hits.iter().position(|h| h.0[0] == 2).unwrap();
        let p4 = hits.iter().position(|h| h.0[0] == 4).unwrap();
        assert!(p2 < p4);
        // Keyword only.
        let hits = fuse(&k, &[], 1.0, page, 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].3, None);
    }
}
