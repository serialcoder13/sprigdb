//! Spilling's leftovers through a live gateway, in
//! `spill_live.rs`'s style.
//!
//! - `spill_leftovers_match_mongodb`: generated documents (about 3.6 MB)
//!   with the gateway's stage, sort and `$graphLookup` limits and MongoDB
//!   8.0.34's stage limits at `SPRIG_TEST_SPILL_STAGE_BYTES` (1 MiB by
//!   default): a single `$setWindowFields` partition, `$densify` (all
//!   three bounds), one `$graphLookup` search larger than the limit,
//!   `$out` and `$merge` of a spilled input, and `find` sorting past the
//!   limit (with `allowDiskUse`, without it, and as a top-k). Answers are
//!   compared document by document (exact BSON) and error codes.
//! - `serverStatus` counts the spills (`metrics.query`).
//! - `find_spill_files_go_away`: a spilled `find`'s files exist while its
//!   cursor is open and are gone after `killCursors` and at its end.
//!
//! Variables: `SPRIG_TEST_MONGO_URI` (the gateway; skipped without it),
//! `SPRIG_TEST_MONGO_REF_URI` (MongoDB 8.0.34), `SPRIG_TEST_SPILL_DIR` (the
//! gateway's `doc.spill_dir`), `SPRIG_TEST_SPILL_STAGE_BYTES`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::print_stderr,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names
)]

use std::time::Instant;

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Database};

fn var(v: &str) -> Option<String> {
    std::env::var(v).ok().filter(|s| !s.is_empty())
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((ka, va), (kb, vb))| ka == kb && same(va, vb))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        _ => a == b,
    }
}

const PARAMS: [&str; 6] = [
    "internalDocumentSourceGroupMaxMemoryBytes",
    "internalQueryMaxBlockingSortMemoryUsageBytes",
    "internalDocumentSourceSetWindowFieldsMaxMemoryBytes",
    "internalDocumentSourceDensifyMaxMemoryBytes",
    "internalQuerySlotBasedExecutionHashAggApproxMemoryUseInBytesBeforeSpill",
    "internalQuerySlotBasedExecutionHashLookupApproxMemoryUseInBytesBeforeSpill",
];

/// About 600 bytes each; `v` repeats with gaps, `h` partitions.
fn generate(n: i64) -> Vec<Document> {
    (0..n)
        .map(|i| {
            doc! {
                "_id": i,
                "h": i % 7,
                "v": ((i * 31) % 1000) as i32 * 3,
                "next": i + 1,
                "s": "x".repeat(500 + (i % 5) as usize * 10),
            }
        })
        .collect()
}

async fn fill(db: &Database, n: i64) {
    let _ = db.drop().await;
    for c in generate(n).chunks(1000) {
        db.collection::<Document>("c")
            .insert_many(c.to_vec())
            .await
            .unwrap();
    }
    db.collection::<Document>("m")
        .insert_many((0..50_i64).map(|i| doc! {"_id": i * 3, "old": true}))
        .await
        .unwrap();
}

async fn agg(db: &Database, coll: &str, p: &[Document], disk: bool) -> Result<Vec<Document>, i32> {
    let cur = db
        .collection::<Document>(coll)
        .aggregate(p.to_vec())
        .allow_disk_use(disk)
        .batch_size(500)
        .await
        .map_err(|e| code_of(&e))?;
    cur.try_collect().await.map_err(|e| code_of(&e))
}

async fn find(db: &Database, body: Document) -> Result<Vec<Document>, i32> {
    let r = db.run_command(body).await.map_err(|e| code_of(&e))?;
    let c = r.get_document("cursor").unwrap();
    let mut out: Vec<Document> = c
        .get_array("firstBatch")
        .unwrap()
        .iter()
        .filter_map(|d| d.as_document().cloned())
        .collect();
    let mut id = c.get_i64("id").unwrap_or(0);
    let coll = c
        .get_str("ns")
        .unwrap()
        .split_once('.')
        .unwrap()
        .1
        .to_owned();
    while id != 0 {
        let r = db
            .run_command(doc! {"getMore": id, "collection": &coll, "batchSize": 500})
            .await
            .map_err(|e| code_of(&e))?;
        let c = r.get_document("cursor").unwrap();
        out.extend(
            c.get_array("nextBatch")
                .unwrap()
                .iter()
                .filter_map(|d| d.as_document().cloned()),
        );
        id = c.get_i64("id").unwrap_or(0);
    }
    Ok(out)
}

/// (name, collection, pipeline, compare `allowDiskUse: false` too)
fn pipelines() -> Vec<(&'static str, &'static str, Vec<Document>, bool)> {
    vec![
        (
            "window_one_partition",
            "c",
            vec![
                doc! {"$setWindowFields": {"sortBy": {"v": 1, "_id": 1}, "output": {
                    "run": {"$sum": "$v", "window": {"documents": ["unbounded", "current"]}},
                    "prev": {"$shift": {"output": "$_id", "by": -1}},
                    "near": {"$push": "$h", "window": {"documents": [-2, 1]}},
                    "avg": {"$avg": "$v", "window": {"documents": [-3, 3]}},
                }}},
                doc! {"$project": {"s": 0}},
            ],
            false,
        ),
        (
            "window_one_partition_range",
            "c",
            vec![
                doc! {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {
                    "mx": {"$max": "$v", "window": {"range": [-30, 30]}},
                    "n": {"$count": {}, "window": {"range": [-5, 0]}},
                    "rk": {"$denseRank": {}},
                }}},
                doc! {"$project": {"s": 0}},
            ],
            false,
        ),
        (
            "window_one_partition_nested_output",
            "c",
            vec![
                doc! {"$setWindowFields": {"sortBy": {"_id": -1}, "output": {
                    "w.cnt": {"$count": {}, "window": {"documents": ["unbounded", "current"]}},
                    "w.top": {"$topN": {"n": 2, "sortBy": {"v": -1}, "output": "$_id"}, "window": {"documents": [-5, 0]}},
                }}},
                doc! {"$project": {"s": 0}},
            ],
            false,
        ),
        (
            "densify_partition",
            "c",
            vec![
                doc! {"$densify": {"field": "v", "partitionByFields": ["h"], "range": {"step": 1, "bounds": "partition"}}},
                doc! {"$project": {"s": 0}},
            ],
            true,
        ),
        (
            "densify_full",
            "c",
            vec![
                doc! {"$densify": {"field": "v", "range": {"step": 2, "bounds": "full"}}},
                // Documents with equal `v` come in no set order.
                doc! {"$sort": {"v": 1, "_id": 1}},
                doc! {"$project": {"s": 0}},
            ],
            true,
        ),
        (
            "densify_range",
            "c",
            vec![
                doc! {"$densify": {"field": "v", "partitionByFields": ["h"], "range": {"step": 1, "bounds": [100, 200]}}},
                doc! {"$project": {"s": 0}},
            ],
            true,
        ),
        (
            "graph_one_search",
            "c",
            vec![
                doc! {"$match": {"_id": {"$in": [0, 1]}}},
                doc! {"$graphLookup": {"from": "c", "startWith": "$next", "connectFromField": "next", "connectToField": "_id", "as": "chain", "depthField": "d"}},
                doc! {"$project": {"n": {"$size": "$chain"}, "ids": {"$sortArray": {"input": "$chain._id", "sortBy": 1}}, "depth": {"$max": "$chain.d"}}},
            ],
            false,
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn spill_leftovers_match_mongodb() {
    let (Some(gw), Some(rf)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_MONGO_REF_URI"))
    else {
        eprintln!("SPRIG_TEST_MONGO_URI / SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let bytes: i64 = var("SPRIG_TEST_SPILL_STAGE_BYTES").map_or(1 << 20, |s| s.parse().unwrap());
    let n: i64 = var("SPRIG_TEST_SPILL_DOCS").map_or(6000, |s| s.parse().unwrap());
    let g = Client::with_uri_str(&gw).await.unwrap();
    let r = Client::with_uri_str(&rf).await.unwrap();
    let admin = r.database("admin");
    for p in PARAMS {
        admin
            .run_command(doc! {"setParameter": 1, p: bytes})
            .await
            .unwrap();
    }
    let gdb = g.database("m7_spill_diff");
    let rdb = r.database("m7_spill_diff");
    fill(&gdb, n).await;
    fill(&rdb, n).await;
    let status = |c: Client| async move {
        c.database("admin")
            .run_command(doc! {"serverStatus": 1})
            .await
            .unwrap()
            .get_document("metrics")
            .unwrap()
            .get_document("query")
            .unwrap()
            .clone()
    };
    let before = status(g.clone()).await;
    let (mut ok, mut bad) = (0, 0);
    let mut check =
        |name: &str, want: &Result<Vec<Document>, i32>, got: &Result<Vec<Document>, i32>| {
            let equal = match (want, got) {
                (Ok(a), Ok(b)) => {
                    a.len() == b.len()
                        && a.iter().zip(b).all(|(x, y)| {
                            same(&Bson::Document(x.clone()), &Bson::Document(y.clone()))
                        })
                }
                (Err(a), Err(b)) => a == b,
                _ => false,
            };
            let shape = |r: &Result<Vec<Document>, i32>| match r {
                Ok(v) => format!("{} docs", v.len()),
                Err(c) => format!("error {c}"),
            };
            eprintln!(
                "{name}: {} (mongodb {}, sprigdb {})",
                if equal { "same" } else { "DIFFERENT" },
                shape(want),
                shape(got)
            );
            if equal {
                ok += 1;
            } else {
                bad += 1;
                if let (Ok(a), Ok(b)) = (want, got) {
                    let i = a
                        .iter()
                        .zip(b)
                        .position(|(x, y)| {
                            !same(&Bson::Document(x.clone()), &Bson::Document(y.clone()))
                        })
                        .unwrap_or(a.len().min(b.len()));
                    eprintln!(
                        "  first difference at {i}:\n    mongodb {:?}\n    sprigdb {:?}",
                        a.get(i),
                        b.get(i)
                    );
                }
            }
        };
    for (name, coll, p, without) in pipelines() {
        let t = Instant::now();
        let want = agg(&rdb, coll, &p, true).await;
        let tr = t.elapsed();
        let t = Instant::now();
        let got = agg(&gdb, coll, &p, true).await;
        eprintln!("  ({name}: mongodb {tr:?}, sprigdb {:?})", t.elapsed());
        check(name, &want, &got);
        if without {
            let want = agg(&rdb, coll, &p, false).await;
            let got = agg(&gdb, coll, &p, false).await;
            check(&format!("{name} without allowDiskUse"), &want, &got);
        }
    }
    // One `$graphLookup` search past `doc.graph_lookup_max_bytes` without
    // `allowDiskUse`: 292 on SprigDB (MongoDB 8.0's limit is a fixed
    // 100 MB, so it isn't compared).
    let (_, _, gp, _) = pipelines()
        .into_iter()
        .find(|p| p.0 == "graph_one_search")
        .unwrap();
    assert_eq!(agg(&gdb, "c", &gp, false).await.unwrap_err(), 292);
    // `$out` and `$merge` of a spilled input.
    for (name, p, target) in [
        (
            "out_spilled",
            vec![doc! {"$sort": {"v": -1, "_id": 1}}, doc! {"$out": "o"}],
            "o",
        ),
        (
            "merge_spilled",
            vec![
                doc! {"$sort": {"v": 1, "_id": -1}},
                doc! {"$project": {"s": 0}},
                doc! {"$merge": {"into": "m", "whenMatched": "merge", "whenNotMatched": "insert"}},
            ],
            "m",
        ),
    ] {
        let want = agg(&rdb, "c", &p, true).await;
        let got = agg(&gdb, "c", &p, true).await;
        check(name, &want, &got);
        let q =
            doc! {"find": target, "sort": {"_id": 1}, "projection": {"s": 0}, "batchSize": 1000};
        let want = find(&rdb, q.clone()).await;
        let got = find(&gdb, q).await;
        check(&format!("{name} target"), &want, &got);
    }
    // `find` sorting past the limit.
    for (name, q) in [
        (
            "find_sort",
            doc! {"find": "c", "sort": {"v": -1, "_id": 1}, "projection": {"s": 0}, "batchSize": 500},
        ),
        (
            "find_sort_disk",
            doc! {"find": "c", "sort": {"s": 1, "v": -1}, "projection": {"s": 0}, "allowDiskUse": true},
        ),
        (
            "find_sort_no_disk",
            doc! {"find": "c", "sort": {"v": -1, "_id": 1}, "allowDiskUse": false},
        ),
        (
            "find_sort_top_k_no_disk",
            doc! {"find": "c", "sort": {"v": -1, "_id": 1}, "limit": 20, "skip": 5, "allowDiskUse": false, "projection": {"s": 0}},
        ),
        (
            "find_sort_skip_limit",
            doc! {"find": "c", "sort": {"h": 1, "v": 1, "_id": 1}, "skip": 2500, "limit": 700, "projection": {"s": 0}},
        ),
        (
            "find_allow_disk_use_bad",
            doc! {"find": "c", "allowDiskUse": 1},
        ),
    ] {
        let want = find(&rdb, q.clone()).await;
        let got = find(&gdb, q).await;
        check(name, &want, &got);
    }
    // serverStatus's counters.
    let after = status(g.clone()).await;
    let delta = |path: &[&str]| {
        let get = |d: &Document| {
            let mut cur = d;
            for p in &path[..path.len() - 1] {
                cur = cur.get_document(*p).unwrap();
            }
            cur.get_i64(path[path.len() - 1]).unwrap()
        };
        get(&after) - get(&before)
    };
    eprintln!("serverStatus metrics.query after: {after}");
    for p in [
        &["sort", "spillToDisk"][..],
        &["sort", "spillToDiskBytes"],
        &["sort", "totalKeysSorted"],
        &["setWindowFields", "spills"],
        &["graphLookup", "spills"],
        &["allowDiskUseFalse"],
    ] {
        let d = delta(p);
        eprintln!("  {}: +{d}", p.join("."));
        assert!(d > 0, "{} counted", p.join("."));
    }
    let _ = gdb.drop().await;
    let _ = rdb.drop().await;
    for p in PARAMS {
        let _ = admin
            .run_command(doc! {"setParameter": 1, p: 104_857_600i64})
            .await;
    }
    eprintln!("spill leftovers: {ok} same, {bad} different");
    assert_eq!(bad, 0);
}

/// Spill files under `dir` (the gateway's process directories).
fn files(dir: &str) -> usize {
    let mut n = 0;
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if e.path().is_dir() {
            for f in std::fs::read_dir(e.path()).into_iter().flatten().flatten() {
                if f.file_name() != "lock" {
                    n += 1;
                }
            }
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread")]
async fn find_spill_files_go_away() {
    let (Some(gw), Some(dir)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_SPILL_DIR")) else {
        eprintln!("SPRIG_TEST_MONGO_URI / SPRIG_TEST_SPILL_DIR not set: skipped");
        return;
    };
    let g = Client::with_uri_str(&gw).await.unwrap();
    let db = g.database("m7_spill_files");
    fill(&db, 5000).await;
    assert_eq!(files(&dir), 0, "no spill files before");
    let r = db
        .run_command(doc! {"find": "c", "sort": {"v": -1, "_id": 1}, "batchSize": 10})
        .await
        .unwrap();
    let id = r.get_document("cursor").unwrap().get_i64("id").unwrap();
    assert_ne!(id, 0);
    let open = files(&dir);
    eprintln!("open find cursor {id}: {open} spill files");
    assert!(open > 0);
    db.run_command(doc! {"killCursors": "c", "cursors": [id]})
        .await
        .unwrap();
    assert_eq!(files(&dir), 0, "killCursors deletes the files");
    let all = find(
        &db,
        doc! {"find": "c", "sort": {"v": -1, "_id": 1}, "batchSize": 100},
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 5000);
    assert_eq!(files(&dir), 0, "an exhausted cursor deletes the files");
    let _ = db.drop().await;
}
