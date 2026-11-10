//! Running a search (chapter 34, "Per-shard SQL", "Merge").
//!
//! ```text
//! search(spec):
//!   pushed, exact = the filter as SQL (chapter 31's translator; a superset unless exact)
//!   plan (crate::plan)
//!   Ann*: SET SESSION mhnsw_ef_search = ef
//!   SELECT id, <distance>(v, q) AS _d, doc, _types FROM t [IGNORE INDEX (ann)]
//!     WHERE pushed [AND geo cells] ORDER BY <distance>(v, q) LIMIT limit
//!       -- one shard: MariaDB; a sharded table: each shard, merged by _d (chapter 17)
//!   drop rows failing the residual filter (MongoDB's semantics) or the exact geo test
//!   fewer than k left and the query returned `limit` rows: limit and ef doubled,
//!     up to k x vector.max_overfetch; then accept what remains
//!   keep k; score from the similarity
//! ```

use std::collections::BTreeSet;

use bson::Document;
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::filter::{Pred, parse};
use sprig_doc::meta::CollMeta;
use sprig_doc::translate::translate;
use sprig_kv::SqlRunner;
use sprig_kv::sql::{cell_f64, cell_u64};

use crate::backend;
use crate::index::{ANN_INDEX, AnnMode, AnnState, IndexDef, set_ann};
use crate::items::{metadata_doc, prepare, vec_sql};
use crate::plan::{Plan, Settings, Stats, plan};

/// Rows sampled to measure a residual filter's selectivity.
const SAMPLE_ROWS: u64 = 1000;
/// MariaDB's largest `mhnsw_ef_search`.
const MAX_EF: u32 = 10_000;

/// A location filter (chapter 36): the rows inside, tested exactly.
pub trait GeoFilter: Send + Sync {
    /// S2 cell-id ranges covering the region, `[min, max]` inclusive, for
    /// the `_sprig_s2` index.
    fn ranges(&self) -> Vec<(u64, u64)>;
    /// The exact test of a location (degrees).
    fn contains(&self, lat: f64, lng: f64) -> bool;
}

impl GeoFilter for sprig_geo::region::Region {
    fn ranges(&self) -> Vec<(u64, u64)> {
        sprig_geo::region::Region::ranges(self)
    }

    fn contains(&self, lat: f64, lng: f64) -> bool {
        sprig_geo::region::Region::contains(self, lat, lng)
    }
}

/// A search request.
pub struct SearchSpec {
    /// The query vector.
    pub query: Vec<f32>,
    /// Results wanted.
    pub k: u32,
    /// `numCandidates`: the graph's `ef`.
    pub num_candidates: Option<u32>,
    /// Exact search.
    pub exact: bool,
    /// A MongoDB query on the metadata.
    pub filter: Option<Document>,
    /// A location filter (geo indexes).
    pub geo: Option<Box<dyn GeoFilter>>,
}

impl std::fmt::Debug for SearchSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchSpec")
            .field("k", &self.k)
            .field("exact", &self.exact)
            .finish_non_exhaustive()
    }
}

/// One result.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    /// The item's id.
    pub id: Vec<u8>,
    /// MariaDB's distance.
    pub distance: f64,
    /// The score (chapter 34's formula).
    pub score: f64,
    /// Its metadata, as relaxed Extended JSON.
    pub metadata: Option<Value>,
    /// Its location, `(latitude, longitude)`.
    pub location: Option<(f64, f64)>,
}

/// What a search returned and how it ran.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutcome {
    /// The k nearest, nearest first.
    pub hits: Vec<Hit>,
    /// The plan.
    pub plan: Plan,
    /// Times the query was rerun with a larger limit.
    pub requeries: u32,
}

fn invalid(code: SprigCode, m: String) -> SprigError {
    SprigError::new(code, m)
}

/// The metadata a filter reads, for chapter 31's translator: every path
/// may hold arrays (so pushed conditions are supersets, rechecked here).
fn meta_for(def: &IndexDef) -> CollMeta {
    CollMeta {
        name: def.name.clone(),
        table: def.name.clone(),
        uuid: [0; 16],
        array_paths: BTreeSet::new(),
        arrays_full: true,
        pending_paths: BTreeSet::new(),
        indexes: Vec::new(),
        options: Document::new(),
        next_index: 1,
    }
}

/// The filter: SQL to push, and the predicate when a residual remains.
fn compile_filter(def: &IndexDef, filter: Option<&Document>) -> Result<(String, Option<Pred>)> {
    let Some(f) = filter.filter(|f| !f.is_empty()) else {
        return Ok(("TRUE".into(), None));
    };
    let pred = parse(f).map_err(|e| {
        invalid(
            SprigCode::VectorFilterNotIndexed,
            format!("filter: {}", e.message),
        )
    })?;
    let sql = translate(&pred, &meta_for(def));
    let residual = (!sql.exact).then_some(pred);
    Ok((sql.sql, residual))
}

fn geo_sql(geo: Option<&dyn GeoFilter>) -> String {
    let Some(g) = geo else {
        return String::new();
    };
    let parts: Vec<String> = g
        .ranges()
        .iter()
        .map(|(a, b)| format!("_sprig_s2 BETWEEN {a} AND {b}"))
        .collect();
    if parts.is_empty() {
        " AND FALSE".into()
    } else {
        format!(" AND ({})", parts.join(" OR "))
    }
}

struct Row {
    id: Vec<u8>,
    d: f64,
    doc: Document,
    loc: Option<(f64, f64)>,
}

/// Runs a search on a standalone index.
///
/// # Errors
/// 39736 for the query's length, 39738 for `k` and `num_candidates`,
/// 39739 for a filter that can't be parsed; the backend's.
#[allow(clippy::too_many_lines)]
pub async fn search(
    sql: &mut dyn SqlRunner,
    def: &IndexDef,
    spec: &SearchSpec,
    s: &Settings,
) -> Result<SearchOutcome> {
    if spec.k == 0 || spec.k > s.max_k {
        return Err(invalid(
            SprigCode::VectorLimitInvalid,
            format!("k must be 1 to {} (vector.max_k), not {}", s.max_k, spec.k),
        ));
    }
    if let Some(n) = spec.num_candidates
        && (n < spec.k || n > MAX_EF)
    {
        return Err(invalid(
            SprigCode::VectorLimitInvalid,
            format!(
                "numCandidates must be at least k ({}) and at most {MAX_EF}, not {n}",
                spec.k
            ),
        ));
    }
    if spec.geo.is_some() && !def.geo {
        return Err(invalid(
            SprigCode::VectorFilterNotIndexed,
            format!("the index '{}' holds no locations", def.name),
        ));
    }
    let q = prepare(def, &spec.query)?;
    let qsql = vec_sql(&q)?;
    let (pushed, residual) = compile_filter(def, spec.filter.as_ref())?;
    let geo = spec.geo.as_deref();
    let where_sql = format!("({pushed}){}", geo_sql(geo));
    let filtered = spec.filter.as_ref().is_some_and(|f| !f.is_empty()) || geo.is_some();
    let ann_ready = def.ann_state == AnnState::Ready;
    let mut stats = Stats {
        ann_ready,
        filtered,
        pushed_rows: None,
        residual: residual.is_some() || geo.is_some(),
        residual_sel: 1.0,
    };
    if filtered && ann_ready && !spec.exact {
        let r = sql
            .query(&format!(
                "SELECT COUNT(*) FROM (SELECT 1 FROM {} WHERE {where_sql} LIMIT {}) x",
                def.table(),
                s.prefilter_max_rows + 1
            ))
            .await
            .map_err(backend)?;
        let n = cell_u64(r.first().and_then(|r| r.first()).and_then(Option::as_ref));
        stats.pushed_rows = Some(n);
        if n > s.prefilter_max_rows && stats.residual {
            // The residual's selectivity on a sample of the pushed rows.
            let rows = sql
                .query(&format!(
                    "SELECT doc, _types{} FROM {} WHERE {where_sql} LIMIT {SAMPLE_ROWS}",
                    if def.geo { ", lat, lng" } else { "" },
                    def.table()
                ))
                .await
                .map_err(backend)?;
            let total = rows.len().max(1) as f64;
            let kept = rows
                .iter()
                .filter(|r| {
                    let doc = metadata_doc(
                        r.first().and_then(Option::as_deref),
                        r.get(1).and_then(Option::as_deref),
                    );
                    residual.as_ref().is_none_or(|p| p.matches(&doc))
                        && geo.is_none_or(|g| {
                            r.get(2).and_then(Option::as_ref).is_some()
                                && g.contains(
                                    cell_f64(r.get(2).and_then(Option::as_ref)),
                                    cell_f64(r.get(3).and_then(Option::as_ref)),
                                )
                        })
                })
                .count() as f64;
            stats.residual_sel = (kept / total).max(1.0 / SAMPLE_ROWS as f64);
        }
    }
    let p = plan(spec.exact, spec.k, spec.num_candidates, stats, s);
    let f = def.similarity.metric().function();
    let k = spec.k as usize;
    let (mut ef, mut limit) = match p {
        Plan::Ann { ef, limit } | Plan::AnnOverfetch { ef, limit } => (ef, limit),
        Plan::PrefilterExact | Plan::Exact => (0, spec.k),
    };
    let cap = spec.k.saturating_mul(s.max_overfetch.max(1));
    let hint = if !p.approximate() && ann_ready {
        format!(" IGNORE INDEX ({ANN_INDEX})")
    } else {
        String::new()
    };
    let cols = if def.geo { ", lat, lng" } else { "" };
    let mut requeries = 0;
    loop {
        if p.approximate() {
            sql.exec(&format!(
                "SET SESSION mhnsw_ef_search = {}",
                ef.clamp(1, MAX_EF)
            ))
            .await
            .map_err(backend)?;
        }
        // Exact plans with a residual read every row the pushed filter
        // passes: an exact search must not stop at a LIMIT the residual
        // then thins out.
        // The graph ranks by its own compressed distances, so an
        // approximate plan fetches all `ef` candidates and re-ranks them by
        // the exact distance MariaDB computes in the select list.
        let sent = if p.approximate() {
            limit.max(ef.min(MAX_EF))
        } else {
            limit
        };
        let lim = if !p.approximate() && stats.residual {
            String::new()
        } else {
            format!(" LIMIT {sent}")
        };
        // The graph serves only `ORDER BY <distance> LIMIT n`: a second
        // sort key would turn it off, so ties are broken by id in Rust.
        let tie = if p.approximate() { "" } else { ", id" };
        let rows = sql
            .query(&format!(
                "SELECT id, {f}(v, {qsql}) AS _d, doc, _types{cols} FROM {}{hint} WHERE {where_sql} ORDER BY {f}(v, {qsql}){tie}{lim}",
                def.table()
            ))
            .await
            .map_err(backend)?;
        let returned = rows.len();
        // Chapter 04 (decision bugfix-1): an exact search with a residual
        // filter reads every row the pushed filter passes; decoding and
        // testing them runs off the runtime's worker threads.
        let mut kept = sprig_common::cpu::heavy(returned, || {
            keep_rows(rows, def, residual.as_ref(), geo, k, p.approximate())
        });
        if p.approximate() {
            kept.sort_by(|a, b| a.d.total_cmp(&b.d).then_with(|| a.id.cmp(&b.id)));
            kept.truncate(k);
        }
        let more = p.approximate() && kept.len() < k && returned == sent as usize && limit < cap;
        if more {
            limit = limit.saturating_mul(2).min(cap);
            ef = ef.saturating_mul(2).min(MAX_EF);
            requeries += 1;
            continue;
        }
        let hits = kept
            .into_iter()
            .map(|r| Hit {
                metadata: (!r.doc.is_empty())
                    .then(|| bson::Bson::Document(r.doc).into_relaxed_extjson()),
                score: def.similarity.score(r.d),
                id: r.id,
                distance: r.d,
                location: r.loc,
            })
            .collect();
        return Ok(SearchOutcome {
            hits,
            plan: p,
            requeries,
        });
    }
}

/// The rows of one search statement that pass the residual filter and the
/// exact geo test, decoded; an exact plan stops at `k`.
fn keep_rows(
    rows: Vec<sprig_kv::Row>,
    def: &IndexDef,
    residual: Option<&Pred>,
    geo: Option<&dyn GeoFilter>,
    k: usize,
    approximate: bool,
) -> Vec<Row> {
    let mut kept: Vec<Row> = Vec::with_capacity(rows.len().min(k * 4));
    for r in rows {
        let id = r.first().and_then(Clone::clone).unwrap_or_default();
        let d = cell_f64(r.get(1).and_then(Option::as_ref));
        let doc = metadata_doc(
            r.get(2).and_then(Option::as_deref),
            r.get(3).and_then(Option::as_deref),
        );
        let loc = if def.geo {
            match (
                r.get(4).and_then(Option::as_ref),
                r.get(5).and_then(Option::as_ref),
            ) {
                (Some(a), Some(b)) => Some((cell_f64(Some(a)), cell_f64(Some(b)))),
                _ => None,
            }
        } else {
            None
        };
        if let Some(pred) = residual
            && !pred.matches(&doc)
        {
            continue;
        }
        if let Some(g) = geo
            && !loc.is_some_and(|(la, lo)| g.contains(la, lo))
        {
            continue;
        }
        kept.push(Row { id, d, doc, loc });
        if !approximate && kept.len() == k {
            break;
        }
    }
    kept
}

/// `(id, vector)` of the rows `id, VEC_ToText(v)`, decoded off the
/// runtime's worker threads when there are many.
fn decode_vectors(rows: Vec<sprig_kv::Row>) -> Vec<(Vec<u8>, Vec<f32>)> {
    sprig_common::cpu::heavy(rows.len(), || {
        rows.into_iter()
            .filter_map(|r| {
                let id = r.first()?.clone()?;
                let t = String::from_utf8(r.get(1)?.clone()?).ok()?;
                let v: Vec<f32> = serde_json::from_str(&t).ok()?;
                Some((id, v))
            })
            .collect()
    })
}

/// About `n` items' ids and vectors, spread evenly over the index by id:
/// MariaDB picks every `step`-th row, so only the sample travels and is
/// decoded (chapter 34's recall check).
///
/// # Errors
/// The backend's.
pub async fn sample_vectors(
    sql: &mut dyn SqlRunner,
    def: &IndexDef,
    rows: u64,
    n: usize,
) -> Result<Vec<(Vec<u8>, Vec<f32>)>> {
    let n = n.max(1);
    let step = (rows / n as u64).max(1);
    let got = sql
        .query(&format!(
            "SELECT id, VEC_ToText(v) FROM (SELECT id, v, \
             ROW_NUMBER() OVER (ORDER BY id) AS _n FROM {}) x \
             WHERE MOD(_n - 1, {step}) = 0 ORDER BY _n LIMIT {n}",
            def.table()
        ))
        .await
        .map_err(backend)?;
    Ok(decode_vectors(got))
}

/// Every item's id and vector (recall validation and tests).
///
/// # Errors
/// The backend's.
pub async fn all_vectors(
    sql: &mut dyn SqlRunner,
    def: &IndexDef,
) -> Result<Vec<(Vec<u8>, Vec<f32>)>> {
    let rows = sql
        .query(&format!("SELECT id, VEC_ToText(v) FROM {}", def.table()))
        .await
        .map_err(backend)?;
    Ok(decode_vectors(rows))
}

/// Chapter 34's automatic graph for an `ann_mode = auto` index: once it
/// holds `vector.ann_min_rows` rows, build the graph, replay `samples`
/// queries (items' own vectors) with and without it, and keep it when the
/// mean recall at `k` reaches `vector.min_recall`; else drop it. Returns
/// the recall measured, or `None` when nothing was due.
///
/// # Errors
/// The backend's.
pub async fn auto_ann(
    sql: &mut dyn SqlRunner,
    def: &mut IndexDef,
    s: &Settings,
    samples: usize,
    k: u32,
) -> Result<Option<f64>> {
    if def.ann_mode != AnnMode::Auto || def.ann_state == AnnState::Ready {
        return Ok(None);
    }
    let rows = crate::index::rows(sql, def).await?;
    if rows < s.ann_min_rows {
        return Ok(None);
    }
    set_ann(sql, def, true).await?;
    let picked = sample_vectors(sql, def, rows, samples).await?;
    let mut total = 0.0;
    let mut n = 0;
    for (_, q) in picked.iter().take(samples) {
        let exact = search(
            sql,
            def,
            &SearchSpec {
                query: q.clone(),
                k,
                num_candidates: None,
                exact: true,
                filter: None,
                geo: None,
            },
            s,
        )
        .await?;
        let ann = search(
            sql,
            def,
            &SearchSpec {
                query: q.clone(),
                k,
                num_candidates: None,
                exact: false,
                filter: None,
                geo: None,
            },
            s,
        )
        .await?;
        let e: Vec<Vec<u8>> = exact.hits.into_iter().map(|h| h.id).collect();
        let a: Vec<Vec<u8>> = ann.hits.into_iter().map(|h| h.id).collect();
        total += crate::brute::recall(&e, &a);
        n += 1;
    }
    let recall = if n == 0 { 1.0 } else { total / f64::from(n) };
    def.recall = Some(recall);
    let keep = recall >= s.min_recall;
    if !keep {
        tracing::info!(index = %def.name, recall, "vector graph dropped: recall below vector.min_recall");
    }
    set_ann(sql, def, keep).await?;
    Ok(Some(recall))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use sprig_kv::SqlError;

    use super::*;
    use crate::index::{AnnMode, Similarity};

    /// Answers every search with `n` rows and records the statements.
    struct Fake {
        n: usize,
        sent: Vec<String>,
    }

    #[async_trait::async_trait]
    impl SqlRunner for Fake {
        async fn query(&mut self, sql: &str) -> std::result::Result<Vec<sprig_kv::Row>, SqlError> {
            self.sent.push(sql.to_owned());
            Ok((0..self.n)
                .map(|i| {
                    vec![
                        Some(format!("id{i:07}").into_bytes()),
                        Some(b"0.5".to_vec()),
                        None,
                        None,
                        Some(b"19.0".to_vec()),
                        Some(b"72.8".to_vec()),
                    ]
                })
                .collect())
        }

        async fn exec(&mut self, _sql: &str) -> std::result::Result<u64, SqlError> {
            Ok(0)
        }
    }

    /// A region nothing is inside, slow enough per row to show a stall.
    struct Nowhere;

    impl GeoFilter for Nowhere {
        fn ranges(&self) -> Vec<(u64, u64)> {
            vec![(1, 2)]
        }

        fn contains(&self, lat: f64, _lng: f64) -> bool {
            let until = Instant::now() + Duration::from_micros(10);
            let mut x = lat;
            while Instant::now() < until {
                x = x.mul_add(1.000_001, 0.5);
            }
            x < -1e300
        }
    }

    fn def() -> IndexDef {
        IndexDef::new("shop", "emb", 2, Similarity::Cosine, 16, AnnMode::Off, true).unwrap()
    }

    /// Two exact searches whose geo test reads 100,000 rows each, at once
    /// on a two-worker runtime, leave a ticker running (chapter 04,
    /// decision bugfix-1): the residual loop runs off the workers.
    #[test]
    fn residual_loops_leave_the_workers_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stop = Arc::new(AtomicBool::new(false));
            let s2 = Arc::clone(&stop);
            let ticker = tokio::spawn(async move {
                let mut worst = Duration::ZERO;
                let mut last = Instant::now();
                while !s2.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    worst = worst.max(last.elapsed());
                    last = Instant::now();
                }
                worst
            });
            let run = || {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    let mut sql = Fake {
                        n: 100_000,
                        sent: Vec::new(),
                    };
                    let spec = SearchSpec {
                        query: vec![1.0, 0.0],
                        k: 5,
                        num_candidates: None,
                        exact: true,
                        filter: None,
                        geo: Some(Box::new(Nowhere)),
                    };
                    let t = Instant::now();
                    let out = search(&mut sql, &def(), &spec, &Settings::default())
                        .await
                        .unwrap();
                    (out.hits.len(), t.elapsed())
                })
            };
            let (a, b) = (run(), run());
            let (ha, ta) = a.await.unwrap();
            let (hb, _) = b.await.unwrap();
            stop.store(true, Ordering::Relaxed);
            let worst = ticker.await.unwrap();
            assert_eq!((ha, hb), (0, 0));
            assert!(
                worst < Duration::from_millis(200),
                "ticker stalled {worst:?} while each search took {ta:?}"
            );
        });
    }

    /// The recall check's sample is picked by MariaDB: every 50th of 1,000
    /// rows, 20 of them, decoded.
    #[tokio::test]
    async fn samples_are_picked_in_sql() {
        struct Two(Vec<String>);
        #[async_trait::async_trait]
        impl SqlRunner for Two {
            async fn query(
                &mut self,
                sql: &str,
            ) -> std::result::Result<Vec<sprig_kv::Row>, SqlError> {
                self.0.push(sql.to_owned());
                Ok(vec![
                    vec![Some(b"a".to_vec()), Some(b"[1,2]".to_vec())],
                    vec![Some(b"b".to_vec()), Some(b"[0.5,-1]".to_vec())],
                ])
            }
            async fn exec(&mut self, _sql: &str) -> std::result::Result<u64, SqlError> {
                Ok(0)
            }
        }
        let mut sql = Two(Vec::new());
        let got = sample_vectors(&mut sql, &def(), 1000, 20).await.unwrap();
        assert_eq!(
            got,
            [
                (b"a".to_vec(), vec![1.0, 2.0]),
                (b"b".to_vec(), vec![0.5, -1.0])
            ]
        );
        let q = &sql.0[0];
        assert!(q.contains("WHERE MOD(_n - 1, 50) = 0 ORDER BY _n LIMIT 20"), "{q}");
        assert!(q.starts_with("SELECT id, VEC_ToText(v) FROM (SELECT id, v,"), "{q}");
    }
}
