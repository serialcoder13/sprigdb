//! Vector search over MariaDB against brute force:
//! exact plans must equal brute force; HNSW plans are measured for recall;
//! filters, geo filters, automatic graphs and errors.
//!
//! - `SPRIG_TEST_KV_MYSQL`: a MariaDB 11.8 as root (`mysql://root:pw@127.0.0.1:13301`).
//!
//! Skipped when unset. Uses database `r6models_vec`, dropped at the end.

#![allow(
    clippy::format_push_string,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::type_complexity,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::items_after_statements,
    clippy::needless_update,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::float_cmp
)]

use async_trait::async_trait;
use bson::doc;
use mysql_async::prelude::Queryable;
use serde_json::json;
use sprig_common::SprigCode;
use sprig_geo::region::{Region, distance_m};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_vector::brute::{self, recall, top_k};
use sprig_vector::index::{self, AnnMode, IndexDef, Similarity};
use sprig_vector::items::{self, Item};
use sprig_vector::plan::Settings;
use sprig_vector::search::{SearchSpec, auto_ann, search};
use sprig_vector::{Plan, backend};

const DB: &str = "r6models_vec";

struct My(mysql_async::Conn);

#[async_trait]
impl SqlRunner for My {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect())
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        self.0.query_drop(sql).await.map_err(err)?;
        Ok(self.0.affected_rows())
    }
}

fn err(e: mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message,
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
        },
    }
}

struct Rng(u64);

impl Rng {
    fn f(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    fn vec(&mut self, n: usize) -> Vec<f32> {
        // Clustered data, as embeddings are: 20 centers plus noise.
        let c = (self.f() * 20.0) as u64;
        let mut center = Rng(c * 7919 + 1);
        (0..n)
            .map(|_| (center.f() * 2.0 - 1.0 + (self.f() - 0.5) * 0.6) as f32)
            .collect()
    }
}

fn spec(q: &[f32], k: u32) -> SearchSpec {
    SearchSpec {
        query: q.to_vec(),
        k,
        num_candidates: None,
        exact: false,
        filter: None,
        geo: None,
    }
}

fn ids(o: &sprig_vector::SearchOutcome) -> Vec<Vec<u8>> {
    o.hits.iter().map(|h| h.id.clone()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_against_brute_force() {
    let Some(url) = std::env::var("SPRIG_TEST_KV_MYSQL")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_KV_MYSQL not set: skipped");
        return;
    };
    let mut sql = My(
        mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
            .await
            .unwrap(),
    );
    sql.exec(&format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    const N: usize = 3000;
    const DIMS: usize = 16;
    let mut rng = Rng(42);
    let data: Vec<(Vec<u8>, Vec<f32>, i64, (f64, f64))> = (0..N)
        .map(|i| {
            let v = rng.vec(DIMS);
            // Locations around Mumbai, within about 50 km.
            let loc = (19.07 + (rng.f() - 0.5) * 0.9, 72.87 + (rng.f() - 0.5) * 0.9);
            (format!("item{i:05}").into_bytes(), v, (i % 10) as i64, loc)
        })
        .collect();
    let s = Settings::default();
    let mut report = Vec::new();

    for sim in [
        Similarity::Euclidean,
        Similarity::Cosine,
        Similarity::DotProduct,
    ] {
        let def = IndexDef::new(
            DB,
            &format!("v_{}", sim.name().to_lowercase()),
            DIMS as u32,
            sim,
            16,
            AnnMode::Off,
            true,
        )
        .unwrap();
        let mut def = index::create(&mut sql, def).await.unwrap();
        let batch: Vec<Item> = data
            .iter()
            .map(|(id, v, cat, loc)| Item {
                id: id.clone(),
                vector: v.clone(),
                metadata: Some(
                    json!({"cat": cat, "tags": [format!("t{}", cat % 3)], "n": id.len()}),
                ),
                location: Some(*loc),
            })
            .collect();
        assert_eq!(
            items::upsert(&mut sql, &def, &batch).await.unwrap(),
            N as u64
        );
        let stored: Vec<(Vec<u8>, Vec<f32>)> = data
            .iter()
            .map(|(id, v, _, _)| {
                let v = if sim == Similarity::DotProduct {
                    brute::normalized(v).unwrap()
                } else {
                    v.clone()
                };
                (id.clone(), v)
            })
            .collect();
        let metric = sim.metric();
        let mut qrng = Rng(7);
        // 1. Exact search equals brute force: same ids, same distances.
        for _ in 0..20 {
            let q = qrng.vec(DIMS);
            let qn = if sim == Similarity::DotProduct {
                brute::normalized(&q).unwrap()
            } else {
                q.clone()
            };
            let want = top_k(
                metric,
                &qn,
                stored.iter().map(|(i, v)| (i.as_slice(), v.as_slice())),
                10,
            );
            let got = search(&mut sql, &def, &spec(&q, 10), &s).await.unwrap();
            assert_eq!(got.plan, Plan::Exact);
            assert_eq!(
                ids(&got),
                want.iter().map(|w| w.0.clone()).collect::<Vec<_>>(),
                "{sim:?}"
            );
            for (h, w) in got.hits.iter().zip(&want) {
                assert!(
                    (h.distance - w.1).abs() <= 1e-5 * w.1.max(1.0),
                    "{} vs {}",
                    h.distance,
                    w.1
                );
                assert!((h.score - sim.score(h.distance)).abs() < 1e-12);
            }
        }
        // 2. Exact with a metadata filter (MongoDB's semantics) and a geo filter.
        let circle = Region::Circle {
            lat: 19.07,
            lng: 72.87,
            radius_m: 20_000.0,
        };
        for (filter, geo) in [
            (Some(doc! {"cat": 3}), false),
            (Some(doc! {"cat": {"$in": [1, 2]}, "tags": "t1"}), false),
            (
                Some(doc! {"$or": [{"cat": {"$gte": 8}}, {"tags": "t0"}]}),
                false,
            ),
            (None, true),
            (Some(doc! {"cat": {"$lt": 5}}), true),
        ] {
            let keep = |i: usize| {
                let (_, _, cat, (la, lo)) = &data[i];
                let m = match &filter {
                    None => true,
                    Some(f) => {
                        let d = doc! {"cat": *cat, "tags": [format!("t{}", cat % 3)]};
                        sprig_doc::filter::parse(f).unwrap().matches(&d)
                    }
                };
                m && (!geo || distance_m(19.07, 72.87, *la, *lo) <= 20_000.0)
            };
            let q = qrng.vec(DIMS);
            let qn = if sim == Similarity::DotProduct {
                brute::normalized(&q).unwrap()
            } else {
                q.clone()
            };
            let want = top_k(
                metric,
                &qn,
                stored
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| keep(*i))
                    .map(|(_, (i, v))| (i.as_slice(), v.as_slice())),
                10,
            );
            let mut sp = spec(&q, 10);
            sp.filter = filter.clone();
            sp.geo =
                geo.then(|| Box::new(circle.clone()) as Box<dyn sprig_vector::search::GeoFilter>);
            let got = search(&mut sql, &def, &sp, &s).await.unwrap();
            assert_eq!(got.plan, Plan::PrefilterExact);
            assert_eq!(
                ids(&got),
                want.iter().map(|w| w.0.clone()).collect::<Vec<_>>(),
                "{filter:?} geo {geo}"
            );
        }
        // 3. HNSW: recall at 10 against brute force.
        index::set_ann(&mut sql, &mut def, true).await.unwrap();
        let mut total = 0.0;
        let mut filtered_total = 0.0;
        let mut requeries = 0;
        let mut plans = std::collections::BTreeSet::new();
        // Small enough that filtered queries take the graph with over-fetch.
        let ann_s = Settings {
            prefilter_max_rows: 100,
            ..s
        };
        for _ in 0..50 {
            let q = qrng.vec(DIMS);
            let qn = if sim == Similarity::DotProduct {
                brute::normalized(&q).unwrap()
            } else {
                q.clone()
            };
            let want: Vec<Vec<u8>> = top_k(
                metric,
                &qn,
                stored.iter().map(|(i, v)| (i.as_slice(), v.as_slice())),
                10,
            )
            .into_iter()
            .map(|w| w.0)
            .collect();
            let got = search(&mut sql, &def, &spec(&q, 10), &ann_s).await.unwrap();
            assert_eq!(got.plan, Plan::Ann { ef: 64, limit: 10 });
            total += recall(&want, &ids(&got));
            // Filtered: cat in (1, 2) keeps 20% of the rows.
            let want_f: Vec<Vec<u8>> = top_k(
                metric,
                &qn,
                stored
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| matches!(data[*i].2, 1 | 2))
                    .map(|(_, (i, v))| (i.as_slice(), v.as_slice())),
                10,
            )
            .into_iter()
            .map(|w| w.0)
            .collect();
            let mut sp = spec(&q, 10);
            sp.filter = Some(doc! {"cat": {"$in": [1, 2]}});
            let got = search(&mut sql, &def, &sp, &ann_s).await.unwrap();
            plans.insert(got.plan.name());
            requeries += got.requeries;
            assert_eq!(got.hits.len(), 10);
            filtered_total += recall(&want_f, &ids(&got));
        }
        let (r, rf) = (total / 50.0, filtered_total / 50.0);
        report.push(format!(
            "{}: HNSW recall@10 {r:.3}, filtered (20% of rows) {rf:.3} with plans {plans:?} and {requeries} re-queries",
            sim.name()
        ));
        assert!(r >= 0.9, "{sim:?} recall {r}");
        assert!(rf >= 0.85, "{sim:?} filtered recall {rf}");
        // exact: true still equals brute force with the graph present.
        let q = qrng.vec(DIMS);
        let qn = if sim == Similarity::DotProduct {
            brute::normalized(&q).unwrap()
        } else {
            q.clone()
        };
        let want = top_k(
            metric,
            &qn,
            stored.iter().map(|(i, v)| (i.as_slice(), v.as_slice())),
            10,
        );
        let mut sp = spec(&q, 10);
        sp.exact = true;
        let got = search(&mut sql, &def, &sp, &s).await.unwrap();
        assert_eq!(got.plan, Plan::Exact);
        assert_eq!(
            ids(&got),
            want.iter().map(|w| w.0.clone()).collect::<Vec<_>>()
        );
    }

    // 4. Automatic graphs: built at ann_min_rows and kept on recall.
    let def = IndexDef::new(
        DB,
        "v_auto",
        DIMS as u32,
        Similarity::Euclidean,
        16,
        AnnMode::Auto,
        false,
    )
    .unwrap();
    let mut def = index::create(&mut sql, def).await.unwrap();
    let batch: Vec<Item> = data
        .iter()
        .take(1500)
        .map(|(id, v, _, _)| Item {
            id: id.clone(),
            vector: v.clone(),
            metadata: None,
            location: None,
        })
        .collect();
    items::upsert(&mut sql, &def, &batch).await.unwrap();
    let auto_s = Settings {
        ann_min_rows: 1000,
        ..s
    };
    let small = Settings {
        ann_min_rows: 2000,
        ..s
    };
    assert_eq!(
        auto_ann(&mut sql, &mut def, &small, 20, 10).await.unwrap(),
        None
    );
    let rec = auto_ann(&mut sql, &mut def, &auto_s, 20, 10)
        .await
        .unwrap()
        .unwrap();
    let back = index::describe(&mut sql, DB, "v_auto").await.unwrap();
    report.push(format!(
        "auto graph: recall {rec:.3}, state {}",
        back.ann_state.name()
    ));
    assert_eq!(
        back.ann_state.name(),
        if rec >= 0.95 { "ready" } else { "none" }
    );
    // A recall bar it can't reach drops the graph again.
    let mut def2 = back.clone();
    index::set_ann(&mut sql, &mut def2, false).await.unwrap();
    let strict = Settings {
        ann_min_rows: 1000,
        min_recall: 1.01,
        ..s
    };
    auto_ann(&mut sql, &mut def2, &strict, 5, 10).await.unwrap();
    assert_eq!(
        index::describe(&mut sql, DB, "v_auto")
            .await
            .unwrap()
            .ann_state
            .name(),
        "none"
    );

    // 5. Errors.
    let def = index::describe(&mut sql, DB, "v_euclidean").await.unwrap();
    let code = |e: sprig_common::SprigError| e.code();
    assert_eq!(
        code(
            search(&mut sql, &def, &spec(&[1.0; 3], 5), &s)
                .await
                .unwrap_err()
        ),
        Some(SprigCode::VectorDimensionMismatch)
    );
    assert_eq!(
        code(
            search(&mut sql, &def, &spec(&[1.0; DIMS], 0), &s)
                .await
                .unwrap_err()
        ),
        Some(SprigCode::VectorLimitInvalid)
    );
    let mut sp = spec(&[1.0; DIMS], 20);
    sp.num_candidates = Some(10);
    assert_eq!(
        code(search(&mut sql, &def, &sp, &s).await.unwrap_err()),
        Some(SprigCode::VectorLimitInvalid)
    );
    let dot = index::describe(&mut sql, DB, "v_dotproduct").await.unwrap();
    assert_eq!(
        code(
            search(&mut sql, &dot, &spec(&[0.0; DIMS], 5), &s)
                .await
                .unwrap_err()
        ),
        Some(SprigCode::VectorMetricMismatch)
    );
    assert_eq!(
        code(index::describe(&mut sql, DB, "nope").await.unwrap_err()),
        Some(SprigCode::VectorIndexNotFound)
    );
    let mut sp = spec(&[1.0; DIMS], 5);
    sp.filter = Some(doc! {"$where": "1"});
    assert_eq!(
        code(search(&mut sql, &def, &sp, &s).await.unwrap_err()),
        Some(SprigCode::VectorFilterNotIndexed)
    );
    // Deletes.
    assert_eq!(
        items::delete(&mut sql, &def, &[b"item00000".to_vec(), b"nope".to_vec()])
            .await
            .unwrap(),
        1
    );
    let _ = backend;
    sql.exec(&format!("DROP DATABASE {DB}")).await.unwrap();
    for r in report {
        eprintln!("{r}");
    }
}

/// HNSW recall on a harder set: 20,000 uniform random vectors of 64
/// dimensions (a hard case: no clusters), at `ef` 20, 64 (the default),
/// 200 and 1000, with latencies against exact search. Reports only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recall_at_scale() {
    let Some(url) = std::env::var("SPRIG_TEST_KV_MYSQL")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_KV_MYSQL not set: skipped");
        return;
    };
    let db = "r6models_vec2";
    let mut sql = My(
        mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
            .await
            .unwrap(),
    );
    sql.exec(&format!("DROP DATABASE IF EXISTS {db}"))
        .await
        .unwrap();
    const N: usize = 20_000;
    const DIMS: usize = 64;
    let mut rng = Rng(99);
    let data: Vec<(Vec<u8>, Vec<f32>)> = (0..N)
        .map(|i| {
            let v: Vec<f32> = (0..DIMS).map(|_| (rng.f() * 2.0 - 1.0) as f32).collect();
            (format!("r{i:06}").into_bytes(), v)
        })
        .collect();
    let def = IndexDef::new(
        db,
        "big",
        DIMS as u32,
        Similarity::Cosine,
        16,
        AnnMode::Off,
        false,
    )
    .unwrap();
    let mut def = index::create(&mut sql, def).await.unwrap();
    let batch: Vec<Item> = data
        .iter()
        .map(|(id, v)| Item {
            id: id.clone(),
            vector: v.clone(),
            metadata: None,
            location: None,
        })
        .collect();
    let t = std::time::Instant::now();
    items::upsert(&mut sql, &def, &batch).await.unwrap();
    let load = t.elapsed();
    let t = std::time::Instant::now();
    index::set_ann(&mut sql, &mut def, true).await.unwrap();
    let build = t.elapsed();
    let s = Settings::default();
    let mut out = Vec::new();
    for ef in [20u32, 64, 200, 1000] {
        let mut qrng = Rng(5);
        let mut total = 0.0;
        let mut ms = Vec::new();
        for _ in 0..30 {
            let q: Vec<f32> = (0..DIMS).map(|_| (qrng.f() * 2.0 - 1.0) as f32).collect();
            let want: Vec<Vec<u8>> = top_k(
                sprig_vector::Metric::Cosine,
                &q,
                data.iter().map(|(i, v)| (i.as_slice(), v.as_slice())),
                10,
            )
            .into_iter()
            .map(|w| w.0)
            .collect();
            let mut sp = spec(&q, 10);
            sp.num_candidates = Some(ef);
            let t = std::time::Instant::now();
            let got = search(&mut sql, &def, &sp, &s).await.unwrap();
            ms.push(t.elapsed().as_secs_f64() * 1000.0);
            total += recall(&want, &ids(&got));
        }
        ms.sort_by(f64::total_cmp);
        out.push((ef, total / 30.0, ms[15]));
    }
    // Exact for comparison.
    let mut ms = Vec::new();
    let mut qrng = Rng(5);
    for _ in 0..5 {
        let q: Vec<f32> = (0..DIMS).map(|_| (qrng.f() * 2.0 - 1.0) as f32).collect();
        let mut sp = spec(&q, 10);
        sp.exact = true;
        let t = std::time::Instant::now();
        search(&mut sql, &def, &sp, &s).await.unwrap();
        ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    ms.sort_by(f64::total_cmp);
    eprintln!(
        "20,000 x 64 cosine: load {:.1} s, graph build {:.1} s; {}; exact median {:.1} ms",
        load.as_secs_f64(),
        build.as_secs_f64(),
        out.iter()
            .map(|(ef, r, m)| format!("ef {ef}: recall@10 {r:.3}, median {m:.1} ms"))
            .collect::<Vec<_>>()
            .join("; "),
        ms[2]
    );
    // Measured, not asserted: on unstructured data MariaDB's graph
    // (compressed distances, M = 16) stays near 0.6 whatever ef, which is
    // why `auto` graphs are validated by recall before they are used.
    assert!(out.iter().all(|(_, r, _)| *r > 0.3), "{out:?}");
    sql.exec(&format!("DROP DATABASE {db}")).await.unwrap();
}
