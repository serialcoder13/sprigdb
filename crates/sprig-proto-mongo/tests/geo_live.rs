//! Geospatial queries and `$vectorSearch` on documents (chapters 34 and
//! 36, R6 round 3), through the MongoDB Rust driver.
//!
//! - `SPRIG_TEST_MONGO_URI`: a SprigDB gateway's MongoDB listener with a
//!   tenant login.
//! - `SPRIG_TEST_MONGO_REF_URI`: a reference MongoDB 8.0 (the geo
//!   differential suite; MongoDB has no `$vectorSearch` outside Atlas, so
//!   vector search is checked against brute force).
//!
//! Each test is skipped when its variables are unset. The tests use the
//! databases `r6three_geo` and `r6three_vec` and drop them at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Collection, IndexModel};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next()
    }
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

fn pt(x: f64, y: f64) -> Document {
    doc! {"type": "Point", "coordinates": [x, y]}
}

fn poly(ring: &[(f64, f64)]) -> Document {
    let mut r: Vec<Bson> = ring
        .iter()
        .map(|(x, y)| Bson::Array(vec![Bson::Double(*x), Bson::Double(*y)]))
        .collect();
    r.push(r[0].clone());
    doc! {"type": "Polygon", "coordinates": [r]}
}

/// The data: points, legacy pairs, lines, polygons, arrays of points.
fn data() -> Vec<Document> {
    let mut g = Rng(42);
    let mut out = Vec::new();
    for i in 0..400 {
        let (x, y) = (g.range(72.0, 74.0), g.range(18.0, 20.0));
        let loc: Bson = match i % 20 {
            0..=13 => Bson::Document(pt(x, y)),
            14 | 15 => Bson::Array(vec![Bson::Double(x), Bson::Double(y)]),
            16 => Bson::Document(
                doc! {"type": "LineString", "coordinates": [[x, y], [x + g.range(-0.2, 0.2), y + g.range(-0.2, 0.2)]]},
            ),
            17 => {
                let h = g.range(0.01, 0.08);
                Bson::Document(poly(&[
                    (x - h, y - h),
                    (x + h, y - h),
                    (x + h, y + h),
                    (x - h, y + h),
                ]))
            }
            // MongoDB reads an array of GeoJSON points as a malformed
            // legacy pair, so arrays aren't in the data.
            18 => Bson::Document(
                doc! {"type": "MultiLineString", "coordinates": [[[x, y], [x + 0.1, y]], [[x, y + 0.1], [x + 0.1, y + 0.15]]]},
            ),
            _ => Bson::Document(
                doc! {"type": "MultiPoint", "coordinates": [[x, y], [x + 0.05, y - 0.05]]},
            ),
        };
        let kind = if i % 3 == 0 { "a" } else { "b" };
        out.push(doc! {"_id": i, "kind": kind, "loc": loc});
    }
    // No location at all.
    for i in 400..410 {
        out.push(doc! {"_id": i, "kind": "a"});
    }
    out
}

async fn ids(c: &Collection<Document>, filter: Document) -> Result<Vec<i32>, i32> {
    let r = c.find(filter).await.map_err(|e| code_of(&e))?;
    let docs: Vec<Document> = r.try_collect().await.map_err(|e| code_of(&e))?;
    Ok(docs
        .iter()
        .map(|d| d.get_i32("_id").unwrap_or(-1))
        .collect())
}

async fn agg(c: &Collection<Document>, p: Vec<Document>) -> Result<Vec<Document>, i32> {
    let r = c.aggregate(p).await.map_err(|e| code_of(&e))?;
    r.try_collect().await.map_err(|e| code_of(&e))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn geo_matches_mongodb() {
    let (Some(s_uri), Some(r_uri)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_REF_URI"))
    else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let s = Client::with_uri_str(&s_uri)
        .await
        .unwrap()
        .database("r6three_geo");
    let r = Client::with_uri_str(&r_uri)
        .await
        .unwrap()
        .database("r6three_geo");
    for db in [&s, &r] {
        db.drop().await.unwrap();
        let c = db.collection::<Document>("places");
        c.insert_many(data()).await.unwrap();
        // A collection without a geo index, for the error cases.
        db.collection::<Document>("plain")
            .insert_one(doc! {"_id": 1, "loc": pt(73.0, 19.0)})
            .await
            .unwrap();
    }
    // Errors before the index exists.
    let mut bad: Vec<String> = Vec::new();
    let mut checks = 0;
    let mut rows = 0usize;
    let near0 = doc! {"loc": {"$near": {"$geometry": pt(73.0, 19.0)}}};
    {
        let a = ids(&s.collection("places"), near0.clone()).await;
        let b = ids(&r.collection("places"), near0.clone()).await;
        checks += 1;
        if a.as_ref().err() != b.as_ref().err() {
            bad.push(format!("$near without an index: sprig {a:?} mongo {b:?}"));
        }
    }
    for db in [&s, &r] {
        db.collection::<Document>("places")
            .create_index(IndexModel::builder().keys(doc! {"loc": "2dsphere"}).build())
            .await
            .unwrap();
    }
    // listIndexes.
    {
        let li = |db: &mongodb::Database| {
            let c = db.collection::<Document>("places");
            async move {
                let v: Vec<IndexModel> =
                    c.list_indexes().await.unwrap().try_collect().await.unwrap();
                v.into_iter()
                    .map(|i| {
                        let mut d = mongodb::bson::to_document(&i).unwrap();
                        d.remove("v");
                        d.remove("ns");
                        d
                    })
                    .collect::<Vec<_>>()
            }
        };
        let (a, b) = (li(&s).await, li(&r).await);
        checks += 1;
        if a != b {
            bad.push(format!("listIndexes: sprig {a:?} mongo {b:?}"));
        }
    }
    let mut g = Rng(7);
    let mut filters: Vec<(String, Document)> = Vec::new();
    for k in 0..10 {
        let (x, y) = (g.range(72.2, 73.8), g.range(18.2, 19.8));
        let (w, h) = (g.range(0.1, 0.6), g.range(0.1, 0.6));
        let ring = if k % 3 == 2 {
            // A concave "L".
            vec![
                (x - w, y - h),
                (x + w, y - h),
                (x + w, y),
                (x, y),
                (x, y + h),
                (x - w, y + h),
            ]
        } else {
            vec![
                (x - w, y - h),
                (x + w, y - h),
                (x + w, y + h),
                (x - w, y + h),
            ]
        };
        filters.push((
            format!("within polygon {k}"),
            doc! {"loc": {"$geoWithin": {"$geometry": poly(&ring)}}},
        ));
        filters.push((
            format!("intersects polygon {k}"),
            doc! {"loc": {"$geoIntersects": {"$geometry": poly(&ring)}}},
        ));
    }
    // A polygon with a hole.
    filters.push((
        "within polygon with a hole".into(),
        doc! {"loc": {"$geoWithin": {"$geometry": {"type": "Polygon", "coordinates": [
        [[72.5, 18.5], [73.5, 18.5], [73.5, 19.5], [72.5, 19.5], [72.5, 18.5]],
        [[72.8, 18.8], [73.2, 18.8], [73.2, 19.2], [72.8, 19.2], [72.8, 18.8]]]}}}},
    ));
    for k in 0..4 {
        let (x, y, rad) = (
            g.range(72.2, 73.8),
            g.range(18.2, 19.8),
            g.range(0.002, 0.01),
        );
        filters.push((
            format!("centerSphere {k}"),
            doc! {"loc": {"$geoWithin": {"$centerSphere": [[x, y], rad]}}},
        ));
        filters.push((
            format!("box {k}"),
            doc! {"loc": {"$geoWithin": {"$box": [[x - 0.3, y - 0.2], [x + 0.3, y + 0.2]]}}},
        ));
        filters.push((
            format!("center {k}"),
            doc! {"loc": {"$geoWithin": {"$center": [[x, y], 0.25]}}},
        ));
        filters.push((
            format!("flat polygon {k}"),
            doc! {"loc": {"$geoWithin": {"$polygon": [[x - 0.3, y - 0.3], [x + 0.3, y - 0.1], [x, y + 0.3]]}}},
        ));
        filters.push((
            format!("intersects line {k}"),
            doc! {"loc": {"$geoIntersects": {"$geometry": {"type": "LineString", "coordinates": [[x - 0.4, y], [x + 0.4, y + 0.05]]}}}},
        ));
        filters.push((
            format!("within and kind {k}"),
            doc! {"kind": "a", "loc": {"$geoWithin": {"$centerSphere": [[x, y], 0.008]}}},
        ));
    }
    for (name, f) in &filters {
        let mut a = ids(&s.collection("places"), f.clone()).await;
        let mut b = ids(&r.collection("places"), f.clone()).await;
        if let Ok(v) = a.as_mut() {
            v.sort_unstable();
        }
        if let Ok(v) = b.as_mut() {
            v.sort_unstable();
        }
        rows += b.as_ref().map_or(0, Vec::len);
        checks += 1;
        if a != b {
            bad.push(format!("{name}: {f}\n  sprig {a:?}\n  mongo {b:?}"));
        }
    }
    // $near and $nearSphere: order matters.
    let mut nears: Vec<(String, Document)> = Vec::new();
    for k in 0..5 {
        let (x, y) = (g.range(72.2, 73.8), g.range(18.2, 19.8));
        nears.push((
            format!("near {k}"),
            doc! {"loc": {"$near": {"$geometry": pt(x, y), "$maxDistance": 40_000.0, "$minDistance": 2_000.0}}},
        ));
        nears.push((
            format!("nearSphere legacy {k}"),
            doc! {"loc": {"$nearSphere": [x, y], "$maxDistance": 0.004}},
        ));
        nears.push((
            format!("near and kind {k}"),
            doc! {"kind": "b", "loc": {"$nearSphere": {"$geometry": pt(x, y), "$maxDistance": 30_000.0}}},
        ));
    }
    for (name, f) in &nears {
        let a = ids(&s.collection("places"), f.clone()).await;
        let b = ids(&r.collection("places"), f.clone()).await;
        rows += b.as_ref().map_or(0, Vec::len);
        checks += 1;
        if a != b {
            bad.push(format!("{name}: {f}\n  sprig {a:?}\n  mongo {b:?}"));
        }
    }
    // $geoNear: order, distances, locations.
    for k in 0..5 {
        let (x, y) = (g.range(72.2, 73.8), g.range(18.2, 19.8));
        let stage = if k % 2 == 0 {
            doc! {"$geoNear": {"near": pt(x, y), "distanceField": "d.m", "maxDistance": 50_000.0,
            "query": {"kind": "b"}, "includeLocs": "where", "distanceMultiplier": 0.001}}
        } else {
            doc! {"$geoNear": {"near": [x, y], "distanceField": "dist", "spherical": true, "key": "loc"}}
        };
        let p = vec![stage.clone(), doc! {"$limit": 25}];
        let a = agg(&s.collection("places"), p.clone()).await;
        let b = agg(&r.collection("places"), p).await;
        checks += 1;
        let same = match (&a, &b) {
            (Ok(x), Ok(y)) => {
                x.len() == y.len()
                    && x.iter().zip(y).all(|(p, q)| {
                        let dist = |d: &Document| {
                            d.get_document("d")
                                .ok()
                                .and_then(|m| m.get_f64("m").ok())
                                .or_else(|| d.get_f64("dist").ok())
                                .unwrap_or(-1.0)
                        };
                        let (dp, dq) = (dist(p), dist(q));
                        p.get("_id") == q.get("_id")
                            && p.get("where") == q.get("where")
                            && (dp - dq).abs() <= 1e-9 * dq.abs().max(1e-12) * 1e3
                    })
            }
            (Err(x), Err(y)) => x == y,
            _ => false,
        };
        if !same {
            bad.push(format!(
                "geoNear {k}: {stage}\n  sprig {a:?}\n  mongo {b:?}"
            ));
        }
    }
    // Error codes.
    let errs: Vec<(&str, Vec<Document>)> = vec![
        (
            "geoNear without an index",
            vec![doc! {"$geoNear": {"near": pt(73.0, 19.0), "distanceField": "d"}}],
        ),
        (
            "geoNear without distanceField",
            vec![doc! {"$geoNear": {"near": pt(73.0, 19.0)}}],
        ),
        (
            "geoNear not first",
            vec![
                doc! {"$match": {"kind": "a"}},
                doc! {"$geoNear": {"near": pt(73.0, 19.0), "distanceField": "d"}},
            ],
        ),
        (
            "geoNear legacy flat",
            vec![doc! {"$geoNear": {"near": [73.0, 19.0], "distanceField": "d"}}],
        ),
        (
            "count with near",
            vec![
                doc! {"$match": {"loc": {"$near": {"$geometry": pt(73.0, 19.0)}}}},
                doc! {"$count": "n"},
            ],
        ),
    ];
    for (name, p) in errs {
        let coll = if name.contains("without an index") {
            "plain"
        } else {
            "places"
        };
        let a = agg(&s.collection(coll), p.clone()).await.err();
        let b = agg(&r.collection(coll), p.clone()).await.err();
        checks += 1;
        if a != b {
            bad.push(format!("{name}: sprig {a:?} mongo {b:?}"));
        }
    }
    for (name, f) in [
        (
            "near under $or",
            doc! {"$or": [{"loc": {"$near": {"$geometry": pt(73.0, 19.0)}}}, {"_id": 1}]},
        ),
        ("near legacy", doc! {"loc": {"$near": [73.0, 19.0]}}),
        (
            "within a point",
            doc! {"loc": {"$geoWithin": {"$geometry": pt(73.0, 19.0)}}},
        ),
        (
            "open loop",
            doc! {"loc": {"$geoIntersects": {"$geometry": {"type": "Polygon", "coordinates": [[[72, 18], [74, 18], [74, 20], [72, 20]]]}}}},
        ),
    ] {
        let a = ids(&s.collection("places"), f.clone()).await.err();
        let b = ids(&r.collection("places"), f.clone()).await.err();
        checks += 1;
        if a != b {
            bad.push(format!("{name}: sprig {a:?} mongo {b:?}"));
        }
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!(
        "geo: {checks} checks, {} mismatches, {rows} documents in MongoDB's answers",
        bad.len()
    );
    s.drop().await.unwrap();
    r.drop().await.unwrap();
    assert!(bad.is_empty());
}

/// `$vectorSearch` against brute force: an exact search returns the
/// nearest documents in order with Atlas's scores, filters on declared
/// fields, refuses undeclared ones; search index commands round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_search_matches_brute_force() {
    let Some(s_uri) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let db = Client::with_uri_str(&s_uri)
        .await
        .unwrap()
        .database("r6three_vec");
    db.drop().await.unwrap();
    let c = db.collection::<Document>("items");
    let mut g = Rng(99);
    let dims: i32 = 8;
    let mut docs = Vec::new();
    let mut vecs: Vec<(i32, Vec<f64>, i32)> = Vec::new();
    for i in 0..600 {
        let v: Vec<f64> = (0..dims).map(|_| g.range(-1.0, 1.0)).collect();
        let cat = i % 5;
        if i % 50 == 7 {
            // No vector, or a wrong one: not searchable.
            docs.push(doc! {"_id": i, "cat": cat, "v": [1.0, 2.0]});
            continue;
        }
        docs.push(doc! {"_id": i, "cat": cat, "v": v.clone()});
        vecs.push((i, v, cat));
    }
    c.insert_many(docs).await.unwrap();
    let r = db
        .run_command(doc! {"createSearchIndexes": "items", "indexes": [
        {"name": "vx", "type": "vectorSearch", "definition": {"fields": [
            {"type": "vector", "path": "v", "numDimensions": dims, "similarity": "cosine"},
            {"type": "filter", "path": "cat"}]}},
        {"name": "ve", "type": "vectorSearch", "definition": {"fields": [
            {"type": "vector", "path": "v", "numDimensions": dims, "similarity": "euclidean"}]}}]})
        .await
        .unwrap();
    assert_eq!(r.get_array("indexesCreated").unwrap().len(), 2);
    let listed = agg(&c, vec![doc! {"$listSearchIndexes": {}}])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2, "{listed:?}");
    let mut bad = Vec::new();
    let mut checks = 0;
    for q in 0..20 {
        let qv: Vec<f64> = (0..dims).map(|_| g.range(-1.0, 1.0)).collect();
        let cosine = q % 2 == 0;
        let filter = q % 4 == 0;
        let dist = |v: &[f64]| -> f64 {
            if cosine {
                let dot: f64 = v.iter().zip(&qv).map(|(a, b)| a * b).sum();
                let n = |x: &[f64]| x.iter().map(|a| a * a).sum::<f64>().sqrt();
                1.0 - dot / (n(v) * n(&qv))
            } else {
                v.iter()
                    .zip(&qv)
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum::<f64>()
                    .sqrt()
            }
        };
        let mut want: Vec<(f64, i32)> = vecs
            .iter()
            .filter(|(_, _, c)| !filter || *c == 2)
            .map(|(i, v, _)| (dist(v), *i))
            .collect();
        want.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        want.truncate(10);
        let mut stage = doc! {"index": if cosine { "vx" } else { "ve" }, "path": "v",
        "queryVector": qv.clone(), "numCandidates": 100, "limit": 10};
        if filter {
            stage.insert("filter", doc! {"cat": {"$eq": 2}});
        }
        let got = agg(
            &c,
            vec![
                doc! {"$vectorSearch": stage},
                doc! {"$project": {"_id": 1, "score": {"$meta": "vectorSearchScore"}}},
            ],
        )
        .await
        .unwrap();
        checks += 1;
        let ok = got.len() == want.len()
            && got.iter().zip(&want).all(|(d, (dw, iw))| {
                let score = d.get_f64("score").unwrap_or(-1.0);
                let ws = if cosine {
                    1.0 - dw / 2.0
                } else {
                    1.0 / (1.0 + dw)
                };
                d.get_i32("_id").ok() == Some(*iw) && (score - ws).abs() < 1e-5
            });
        if !ok {
            bad.push(format!(
                "query {q} (cosine {cosine}, filter {filter}): got {got:?}, want {want:?}"
            ));
        }
    }
    // Errors: undeclared filter, wrong dimensions, not first, numCandidates.
    let errs: Vec<(&str, Vec<Document>, i32)> = vec![
        (
            "undeclared filter",
            vec![
                doc! {"$vectorSearch": {"index": "ve", "path": "v", "queryVector": vec![0.5; 8], "numCandidates": 50, "limit": 5, "filter": {"cat": 1}}},
            ],
            39739,
        ),
        (
            "dimensions",
            vec![
                doc! {"$vectorSearch": {"index": "vx", "path": "v", "queryVector": [1.0, 2.0], "numCandidates": 50, "limit": 5}},
            ],
            39736,
        ),
        (
            "numCandidates",
            vec![
                doc! {"$vectorSearch": {"index": "vx", "path": "v", "queryVector": vec![0.5; 8], "numCandidates": 3, "limit": 5}},
            ],
            39738,
        ),
        (
            "not first",
            vec![
                doc! {"$match": {"cat": 1}},
                doc! {"$vectorSearch": {"index": "vx", "path": "v", "queryVector": vec![0.5; 8], "numCandidates": 50, "limit": 5}},
            ],
            40602,
        ),
    ];
    for (name, p, want) in errs {
        let e = agg(&c, p).await.err();
        checks += 1;
        if e != Some(want) {
            bad.push(format!("{name}: {e:?}, want {want}"));
        }
    }
    // An unknown index finds nothing (as in Atlas); a dropped one too.
    let none = agg(&c, vec![doc! {"$vectorSearch": {"index": "nope", "path": "v", "queryVector": vec![0.5; 8], "numCandidates": 50, "limit": 5}}]).await.unwrap();
    db.run_command(doc! {"dropSearchIndex": "items", "name": "ve"})
        .await
        .unwrap();
    let after = agg(&c, vec![doc! {"$listSearchIndexes": {}}])
        .await
        .unwrap();
    checks += 2;
    if !none.is_empty() || after.len() != 1 {
        bad.push(format!("unknown index {none:?}; after drop {after:?}"));
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("vector: {checks} checks, {} mismatches", bad.len());
    db.drop().await.unwrap();
    assert!(bad.is_empty());
}
