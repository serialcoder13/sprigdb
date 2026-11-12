//! Spilling to disk through a live gateway (M5, 12 Nov 2026; decisions
//! S72–S77).
//!
//! - `spill_matches_mongodb`: a generated collection of about 12 MB, with
//!   the gateway's `doc.agg_stage_memory_bytes` and MongoDB 8.0.34's stage
//!   limits both at `SPRIG_TEST_SPILL_STAGE_BYTES` (1 MiB by default), so
//!   every blocking stage spills on both; the answers must be the same
//!   (exact BSON), and `allowDiskUse: false` must give 292 on both.
//! - `spill_files_go_away`: spill files exist while a cursor reads a
//!   spilled result, and are gone after `killCursors`, after the cursor
//!   is exhausted, and after a command that fails once it has spilled.
//! - `group_over_100_mb` (with `SPRIG_TEST_SPILL_BIG=1`, against a gateway
//!   at the default 100 MB): a `$group` holding about 150 MB answers 292
//!   with `allowDiskUse: false` (what every `$group` past 100 MB did before
//!   M5) and the right groups with it.
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
    clippy::cast_precision_loss
)]

use std::time::Instant;

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, DateTime, Document, doc};
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

async fn client(uri: &str) -> Client {
    Client::with_uri_str(uri).await.unwrap()
}

/// Generated documents: about 600 bytes each.
fn generate(n: i64) -> Vec<Document> {
    let base = DateTime::from_millis(1_700_000_000_000);
    (0..n)
        .map(|i| {
            doc! {
                "_id": i,
                "g": (i * 7919) % 997,
                "h": i % 13,
                "v": ((i * 31) % 1000) as i32,
                "f": i as f64 * 0.5 + 0.1,
                "s": "x".repeat(400 + (i % 7) as usize * 10),
                "t": DateTime::from_millis(base.timestamp_millis() + i * 60_000),
                "tags": [i % 3, i % 5],
            }
        })
        .collect()
}

async fn fill(db: &Database, n: i64) {
    let _ = db.drop().await;
    let docs = generate(n);
    for c in docs.chunks(1000) {
        db.collection::<Document>("c")
            .insert_many(c.to_vec())
            .await
            .unwrap();
    }
    let small: Vec<Document> = (0..13).map(|i| doc! {"_id": i, "name": format!("h{i}")}).collect();
    db.collection::<Document>("small")
        .insert_many(small)
        .await
        .unwrap();
}

async fn agg(db: &Database, p: &[Document], disk: bool) -> Result<Vec<Document>, i32> {
    let cur = db
        .collection::<Document>("c")
        .aggregate(p.to_vec())
        .allow_disk_use(disk)
        .batch_size(500)
        .await
        .map_err(|e| code_of(&e))?;
    cur.try_collect().await.map_err(|e| code_of(&e))
}

fn pipelines() -> Vec<(&'static str, Vec<Document>, bool)> {
    // (name, pipeline, MongoDB answers 292 without allowDiskUse)
    vec![
        ("sort", vec![doc! {"$sort": {"v": 1, "_id": 1}}], true),
        ("sort_limit", vec![doc! {"$sort": {"f": -1}}, doc! {"$limit": 50}], false),
        (
            "group",
            vec![
                doc! {"$group": {"_id": "$g", "n": {"$sum": 1}, "ids": {"$push": "$_id"}, "heads": {"$push": {"$substrBytes": ["$s", 0, 100]}}, "avg": {"$avg": "$f"}, "mn": {"$min": "$s"}, "first": {"$first": "$_id"}, "last": {"$last": "$t"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            true,
        ),
        (
            "group_compound",
            vec![
                doc! {"$group": {"_id": {"g": "$g", "h": "$h"}, "set": {"$addToSet": "$h"}, "top": {"$topN": {"n": 3, "sortBy": {"v": -1, "_id": 1}, "output": "$_id"}}, "sd": {"$stdDevPop": "$v"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            true,
        ),
        (
            "group_root",
            vec![
                doc! {"$group": {"_id": "$h", "d": {"$push": "$$ROOT"}}},
                doc! {"$project": {"n": {"$size": "$d"}, "lastS": {"$last": "$d.s"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            true,
        ),
        (
            "bucket_auto",
            vec![doc! {"$bucketAuto": {"groupBy": "$v", "buckets": 7, "output": {"n": {"$sum": 1}, "ids": {"$push": "$_id"}}}}],
            false,
        ),
        (
            "window",
            // (A running `$sum` of doubles differs in the last bit even in
            // memory: MongoDB's growing windows don't sum as `$group` does,
            // an M2 difference, S61.)
            vec![
                doc! {"$setWindowFields": {"partitionBy": "$h", "sortBy": {"f": 1}, "output": {"r": {"$rank": {}}, "run": {"$sum": "$v", "window": {"documents": ["unbounded", "current"]}}, "mov": {"$avg": "$v", "window": {"documents": [-2, 2]}}}}},
                doc! {"$project": {"s": 0}},
                doc! {"$sort": {"_id": 1}},
            ],
            true,
        ),
        (
            "sort_by_count",
            vec![doc! {"$sortByCount": "$h"}, doc! {"$sort": {"count": -1, "_id": 1}}],
            false,
        ),
        (
            "bucket",
            vec![doc! {"$bucket": {"groupBy": "$v", "boundaries": [0, 100, 500, 900], "default": "other", "output": {"n": {"$sum": 1}, "mx": {"$max": "$f"}, "ids": {"$push": "$_id"}}}}],
            false,
        ),
        (
            "facet",
            vec![doc! {"$facet": {
                "a": [{"$sort": {"v": -1, "_id": 1}}, {"$limit": 5}, {"$project": {"s": 0}}],
                "b": [{"$group": {"_id": "$h", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}],
            }}],
            false,
        ),
        (
            "unwind_sort",
            vec![
                doc! {"$unwind": "$tags"},
                doc! {"$sort": {"tags": 1, "_id": 1}},
                doc! {"$project": {"s": 0}},
            ],
            true,
        ),
        (
            "lookup_sort",
            vec![
                doc! {"$lookup": {"from": "small", "localField": "h", "foreignField": "_id", "as": "j"}},
                doc! {"$sort": {"v": -1, "_id": 1}},
                doc! {"$project": {"s": 0}},
            ],
            true,
        ),
        (
            "match_group_sort",
            vec![
                doc! {"$match": {"v": {"$gte": 100}}},
                doc! {"$group": {"_id": {"$mod": ["$_id", 3000]}, "docs": {"$push": "$s"}, "n": {"$sum": 1}}},
                doc! {"$project": {"n": 1, "len": {"$size": "$docs"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            true,
        ),
    ]
}

#[tokio::test]
async fn spill_matches_mongodb() {
    let (Some(gw), Some(rf)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_MONGO_REF_URI")) else {
        eprintln!("SPRIG_TEST_MONGO_URI / SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let bytes: i64 = var("SPRIG_TEST_SPILL_STAGE_BYTES").map_or(1 << 20, |s| s.parse().unwrap());
    let n: i64 = var("SPRIG_TEST_SPILL_DOCS").map_or(20_000, |s| s.parse().unwrap());
    let g = client(&gw).await;
    let r = client(&rf).await;
    let admin = r.database("admin");
    for p in [
        "internalDocumentSourceGroupMaxMemoryBytes",
        "internalQueryMaxBlockingSortMemoryUsageBytes",
        "internalDocumentSourceSetWindowFieldsMaxMemoryBytes",
        "internalQuerySlotBasedExecutionHashAggApproxMemoryUseInBytesBeforeSpill",
        "internalQuerySlotBasedExecutionHashLookupApproxMemoryUseInBytesBeforeSpill",
    ] {
        admin
            .run_command(doc! {"setParameter": 1, p: bytes})
            .await
            .unwrap();
    }
    let gdb = g.database("m5_spill_diff");
    let rdb = r.database("m5_spill_diff");
    let t = Instant::now();
    fill(&gdb, n).await;
    fill(&rdb, n).await;
    eprintln!("filled {n} documents on both in {:?}", t.elapsed());
    let (mut ok, mut bad) = (0, 0);
    for (name, p, refuses) in pipelines() {
        let t = Instant::now();
        let want = agg(&rdb, &p, true).await;
        let tr = t.elapsed();
        let t = Instant::now();
        let got = agg(&gdb, &p, true).await;
        let tg = t.elapsed();
        let equal = match (&want, &got) {
            (Ok(a), Ok(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b)
                        .all(|(x, y)| same(&Bson::Document(x.clone()), &Bson::Document(y.clone())))
            }
            (Err(a), Err(b)) => a == b,
            _ => false,
        };
        let shape = |r: &Result<Vec<Document>, i32>| match r {
            Ok(v) => format!("{} docs", v.len()),
            Err(c) => format!("error {c}"),
        };
        if equal {
            ok += 1;
        } else {
            bad += 1;
            if let (Ok(a), Ok(b)) = (&want, &got) {
                let i = a.iter().zip(b).position(|(x, y)| x != y).unwrap_or(a.len().min(b.len()));
                eprintln!("  first difference at {i}:\n    mongodb {:?}\n    sprigdb {:?}", a.get(i), b.get(i));
            }
        }
        eprintln!(
            "{name}: {} (mongodb {} in {tr:?}, sprigdb {} in {tg:?})",
            if equal { "same" } else { "DIFFERENT" },
            shape(&want),
            shape(&got)
        );
        // allowDiskUse: false.
        let want = agg(&rdb, &p, false).await;
        let got = agg(&gdb, &p, false).await;
        let codes = (want.as_ref().err().copied(), got.as_ref().err().copied());
        if refuses {
            eprintln!("  allowDiskUse false: mongodb {:?}, sprigdb {:?}", codes.0, codes.1);
            assert_eq!(codes.1, Some(292), "{name} without allowDiskUse");
        }
        // MongoDB 8.0's `$bucketAuto` limit is a fixed 100 MB (no
        // parameter), so at a small stage limit only SprigDB refuses.
        if codes.0 == codes.1 || name == "bucket_auto" {
            ok += 1;
        } else {
            bad += 1;
            eprintln!("  allowDiskUse false DIFFERENT: mongodb {:?}, sprigdb {:?}", codes.0, codes.1);
        }
    }
    // Explain reports the spills (MongoDB 8.2's names).
    let ex = gdb
        .run_command(doc! {"explain": {"aggregate": "c", "pipeline": [{"$sort": {"v": 1, "_id": 1}}, {"$project": {"v": 1}}], "cursor": {}}, "verbosity": "executionStats"})
        .await
        .unwrap();
    let stages = ex.get_array("stages").unwrap();
    let sort = stages[1].as_document().unwrap();
    eprintln!("explain $sort: {sort}");
    assert_eq!(sort.get_bool("usedDisk"), Ok(true));
    assert!(sort.get_i64("spills").unwrap() > 0);
    assert!(sort.get_i64("spilledRecords").unwrap() > 0);
    assert!(sort.get_i64("spilledBytes").unwrap() > 0);
    let rex = rdb
        .run_command(doc! {"explain": {"aggregate": "c", "pipeline": [{"$group": {"_id": "$g", "ids": {"$push": "$s"}}}, {"$project": {"_id": 1}}], "cursor": {}}, "verbosity": "executionStats"})
        .await
        .unwrap();
    eprintln!("mongodb explain (group): {}", rex.get("stages").or_else(|| rex.get("executionStats")).cloned().unwrap_or(Bson::Null));
    eprintln!("spill differential: {ok} same, {bad} different");
    let _ = gdb.drop().await;
    let _ = rdb.drop().await;
    for p in [
        "internalDocumentSourceGroupMaxMemoryBytes",
        "internalQueryMaxBlockingSortMemoryUsageBytes",
        "internalDocumentSourceSetWindowFieldsMaxMemoryBytes",
        "internalQuerySlotBasedExecutionHashAggApproxMemoryUseInBytesBeforeSpill",
        "internalQuerySlotBasedExecutionHashLookupApproxMemoryUseInBytesBeforeSpill",
    ] {
        let _ = admin
            .run_command(doc! {"setParameter": 1, p: 104_857_600i64})
            .await;
    }
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

#[tokio::test]
async fn spill_files_go_away() {
    let (Some(gw), Some(dir)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_SPILL_DIR")) else {
        eprintln!("SPRIG_TEST_MONGO_URI / SPRIG_TEST_SPILL_DIR not set: skipped");
        return;
    };
    let g = client(&gw).await;
    let db = g.database("m5_spill_files");
    fill(&db, 8000).await;
    assert_eq!(files(&dir), 0, "no spill files before");
    // A cursor over a spilled sort: files while it's open, none after
    // killCursors.
    let r = db
        .run_command(doc! {"aggregate": "c", "pipeline": [{"$sort": {"v": -1, "_id": 1}}], "cursor": {"batchSize": 10}})
        .await
        .unwrap();
    let id = r.get_document("cursor").unwrap().get_i64("id").unwrap();
    assert_ne!(id, 0);
    let open = files(&dir);
    eprintln!("open cursor {id}: {open} spill files");
    assert!(open > 0);
    db.run_command(doc! {"killCursors": "c", "cursors": [id]})
        .await
        .unwrap();
    assert_eq!(files(&dir), 0, "killCursors deletes the files");
    // Read to the end: none after.
    let all = agg(&db, &[doc! {"$sort": {"v": -1, "_id": 1}}], true).await.unwrap();
    assert_eq!(all.len(), 8000);
    assert_eq!(files(&dir), 0, "an exhausted cursor deletes the files");
    // A command that fails after spilling: none after.
    let e = agg(
        &db,
        &[
            doc! {"$sort": {"v": -1, "_id": 1}},
            doc! {"$group": {"_id": "$_id", "d": {"$push": "$s"}}},
            doc! {"$project": {"x": {"$divide": [1, {"$subtract": ["$_id", 7000]}]}}},
        ],
        true,
    )
    .await
    .unwrap_err();
    eprintln!("failing pipeline: error {e}");
    assert_eq!(files(&dir), 0, "a failed command deletes the files");
    // A client that goes away with a cursor open: the cursor (and its
    // files) stay until `mongo.cursor_timeout_ms`, as in MongoDB.
    let _ = db.drop().await;
}

#[tokio::test]
async fn group_over_100_mb() {
    let Some(gw) = var("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    if var("SPRIG_TEST_SPILL_BIG").is_none() {
        eprintln!("SPRIG_TEST_SPILL_BIG not set: skipped");
        return;
    }
    let g = client(&gw).await;
    let db = g.database("m5_spill_big");
    let _ = db.drop().await;
    let n: i64 = 60_000;
    let t = Instant::now();
    let mut batch = Vec::with_capacity(1000);
    for i in 0..n {
        batch.push(doc! {"_id": i, "k": i % 1000, "pad": "p".repeat(2500)});
        if batch.len() == 1000 {
            db.collection::<Document>("c")
                .insert_many(std::mem::take(&mut batch))
                .await
                .unwrap();
        }
    }
    eprintln!("inserted {n} documents of about 2.5 KB ({} MB) in {:?}", n * 2540 / 1_000_000, t.elapsed());
    let p = vec![
        doc! {"$group": {"_id": "$k", "docs": {"$push": "$$ROOT"}}},
        doc! {"$project": {"n": {"$size": "$docs"}, "first": {"$first": "$docs._id"}}},
        doc! {"$sort": {"_id": 1}},
    ];
    let t = Instant::now();
    let e = agg(&db, &p, false).await.unwrap_err();
    eprintln!("allowDiskUse false: error {e} after {:?}", t.elapsed());
    assert_eq!(e, 292);
    let t = Instant::now();
    let out = agg(&db, &p, true).await.unwrap();
    eprintln!("allowDiskUse true: {} groups in {:?}", out.len(), t.elapsed());
    assert_eq!(out.len(), 1000);
    for (i, d) in out.iter().enumerate() {
        assert_eq!(d.get_i64("_id").unwrap(), i as i64);
        assert_eq!(d.get_i32("n").unwrap(), 60);
        assert_eq!(d.get_i64("first").unwrap(), i as i64);
    }
    // The default (no allowDiskUse) spills too, as MongoDB 6.0+.
    let cur = db.collection::<Document>("c").aggregate(p.clone()).await.unwrap();
    let v: Vec<Document> = cur.try_collect().await.unwrap();
    assert_eq!(v.len(), 1000);
    let _ = db.drop().await;
}
