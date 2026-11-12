//! Capped collections and tailable cursors against MongoDB 8.0.34 (M7,
//! 12 Nov 2026; decisions S81–S84).
//!
//! - `capped_matches_mongodb`: the same commands on the gateway and on the
//!   reference, each answer compared (documents in order, error codes,
//!   cursor ids open or closed, the options and `collStats` fields):
//!   `create` and its checks, eviction by count and by size (one insert at
//!   a time and in batches, a document larger than the cap), natural
//!   order and `$natural`, deletes, updates that grow a document,
//!   `convertToCapped`, `cloneCollectionAsCapped`, `collMod`'s
//!   `cappedSize`/`cappedMax`, `$out` into a capped collection, writes in
//!   a transaction, and tailable cursors: their checks, `getMore` with and
//!   without `awaitData`, `maxTimeMS` as the wait, and position lost.
//! - `driver_tail_matches_mongodb`: the driver's own tailable `awaitData`
//!   cursor (an oplog-style tail) reading what another client inserts.
//!
//! Variables: `SPRIG_TEST_MONGO_URI` (the gateway; skipped without it) and
//! `SPRIG_TEST_MONGO_REF_URI` (MongoDB 8.0.34, a replica set).

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

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{CursorType, FindOptions};
use mongodb::{Client, Database};

fn var(v: &str) -> Option<String> {
    std::env::var(v).ok().filter(|s| !s.is_empty())
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code,
        _ => -1,
    }
}

const DB: &str = "m7_capped";

/// One server's answers, by step name.
#[derive(Default)]
struct Log(Vec<(String, Bson)>);

impl Log {
    fn put(&mut self, name: &str, v: Bson) {
        self.0.push((name.to_owned(), v));
    }
}

/// A command's answer: the error code, or `f` of the reply.
async fn run(db: &Database, c: Document, f: impl Fn(&Document) -> Bson) -> Bson {
    match db.run_command(c).await {
        Ok(r) => f(&r),
        Err(e) => Bson::Document(doc! {"error": code_of(&e)}),
    }
}

fn ok(_: &Document) -> Bson {
    Bson::String("ok".into())
}

/// A cursor reply: open or closed, and its batch.
fn cursor(r: &Document) -> Bson {
    let c = r.get_document("cursor").unwrap();
    let batch = c
        .get_array("firstBatch")
        .or_else(|_| c.get_array("nextBatch"))
        .cloned()
        .unwrap_or_default();
    let open = match c.get("id") {
        Some(Bson::Int64(i)) => *i != 0,
        Some(Bson::Int32(i)) => *i != 0,
        _ => false,
    };
    Bson::Document(doc! {"open": open, "batch": batch})
}

fn cursor_id(r: &Document) -> i64 {
    match r.get_document("cursor").unwrap().get("id") {
        Some(Bson::Int64(i)) => *i,
        Some(Bson::Int32(i)) => i64::from(*i),
        _ => 0,
    }
}

async fn find_all(db: &Database, coll: &str) -> Bson {
    run(db, doc! {"find": coll, "batchSize": 1000}, cursor).await
}

async fn options(db: &Database, coll: &str) -> Bson {
    run(
        db,
        doc! {"listCollections": 1, "filter": {"name": coll}},
        |r| {
            r.get_document("cursor")
                .unwrap()
                .get_array("firstBatch")
                .unwrap()
                .first()
                .and_then(|c| c.as_document())
                .and_then(|c| c.get("options").cloned())
                .unwrap_or(Bson::Null)
        },
    )
    .await
}

async fn stats(db: &Database, coll: &str) -> Bson {
    run(db, doc! {"collStats": coll}, |r| {
        let n = |k: &str| {
            r.get(k)
                .and_then(Bson::as_i64)
                .or_else(|| r.get(k).and_then(Bson::as_i32).map(i64::from))
        };
        Bson::Document(doc! {
            "capped": r.get_bool("capped").unwrap_or(false),
            "count": n("count"),
            "size": n("size"),
            "max": n("max"),
            "maxSize": n("maxSize"),
        })
    })
    .await
}

async fn insert(db: &Database, coll: &str, docs: Vec<Document>) -> Bson {
    run(db, doc! {"insert": coll, "documents": docs}, |r| {
        r.get("n").cloned().unwrap_or(Bson::Null)
    })
    .await
}

fn y(n: usize) -> String {
    "y".repeat(n)
}

/// Every step, on one server.
async fn scenario(client: &Client) -> Log {
    let db = client.database(DB);
    let _ = db.drop().await;
    let mut l = Log::default();
    // `create`'s checks.
    for (name, c) in [
        ("create no size", doc! {"create": "a", "capped": true}),
        (
            "create size 0",
            doc! {"create": "a", "capped": true, "size": 0},
        ),
        (
            "create size -5",
            doc! {"create": "a", "capped": true, "size": -5},
        ),
        (
            "create size str",
            doc! {"create": "a", "capped": true, "size": "x"},
        ),
        (
            "create max only",
            doc! {"create": "a", "capped": false, "max": 3},
        ),
        ("create size uncapped", doc! {"create": "a", "size": 3000}),
        (
            "create max big",
            doc! {"create": "a", "capped": true, "size": 3000, "max": 4_294_967_296_i64},
        ),
        (
            "create capped str",
            doc! {"create": "a", "capped": "yes", "size": 4096},
        ),
        (
            "create max str",
            doc! {"create": "a", "capped": true, "size": 4096, "max": "5"},
        ),
        (
            "create size big",
            doc! {"create": "a", "capped": true, "size": 1_125_899_906_842_625_i64},
        ),
    ] {
        let v = run(&db, c, ok).await;
        l.put(name, v);
    }
    // Options as stored.
    for (coll, c) in [
        ("c3", doc! {"create": "c3", "capped": true, "size": 1e15}),
        (
            "c6",
            doc! {"create": "c6", "capped": true, "size": 3000, "max": -1},
        ),
        ("c8", doc! {"create": "c8", "capped": true, "size": 4097.5}),
        (
            "m0",
            doc! {"create": "m0", "capped": true, "size": 4096, "max": 0},
        ),
        (
            "md",
            doc! {"create": "md", "capped": true, "size": 4096, "max": 5.7},
        ),
        ("c1n", doc! {"create": "c1n", "capped": 1, "size": 4096}),
    ] {
        let v = run(&db, c, ok).await;
        l.put(&format!("create {coll}"), v);
        let o = options(&db, coll).await;
        l.put(&format!("options {coll}"), o);
    }
    // The same create again answers ok; different options 48.
    let v = run(&db, doc! {"create": "c3", "capped": true, "size": 1e15}, ok).await;
    l.put("create same", v);
    let v = run(&db, doc! {"create": "c3", "capped": true, "size": 4096}, ok).await;
    l.put("create different", v);
    // Eviction by size, one insert at a time.
    run(&db, doc! {"create": "c1", "capped": true, "size": 1000}, ok).await;
    l.put("stats c1 empty", stats(&db, "c1").await);
    for i in 0..20 {
        insert(&db, "c1", vec![doc! {"_id": i, "s": y(80)}]).await;
    }
    l.put("c1 after 20", find_all(&db, "c1").await);
    l.put("stats c1", stats(&db, "c1").await);
    // By count, in one batch, and later batches.
    run(
        &db,
        doc! {"create": "c2", "capped": true, "size": 5000, "max": 3},
        ok,
    )
    .await;
    l.put(
        "c2 batch",
        insert(
            &db,
            "c2",
            (0..5).map(|i| doc! {"_id": i, "s": "x"}).collect(),
        )
        .await,
    );
    l.put("c2 after batch", find_all(&db, "c2").await);
    l.put(
        "c2 batch 2",
        insert(&db, "c2", (5..7).map(|i| doc! {"_id": i}).collect()).await,
    );
    l.put("c2 after batch 2", find_all(&db, "c2").await);
    l.put("stats c2", stats(&db, "c2").await);
    // A batch larger than the size: the newest documents that fit.
    run(
        &db,
        doc! {"create": "big", "capped": true, "size": 4096},
        ok,
    )
    .await;
    l.put(
        "big batch",
        insert(
            &db,
            "big",
            (0..10).map(|i| doc! {"_id": i, "s": y(900)}).collect(),
        )
        .await,
    );
    l.put(
        "big after batch",
        run(&db, doc! {"find": "big", "projection": {"_id": 1}}, cursor).await,
    );
    // A document larger than the cap stays alone.
    l.put(
        "big one",
        insert(&db, "big", vec![doc! {"_id": 99, "s": y(5000)}]).await,
    );
    l.put(
        "big after one",
        run(&db, doc! {"find": "big", "projection": {"_id": 1}}, cursor).await,
    );
    l.put("stats big", stats(&db, "big").await);
    // A duplicate in an ordered batch.
    l.put(
        "c2 dup",
        insert(
            &db,
            "c2",
            vec![doc! {"_id": 10}, doc! {"_id": 6}, doc! {"_id": 11}],
        )
        .await,
    );
    l.put("c2 after dup", find_all(&db, "c2").await);
    // Natural order.
    run(&db, doc! {"create": "o", "capped": true, "size": 10000}, ok).await;
    insert(
        &db,
        "o",
        vec![doc! {"_id": 5}, doc! {"_id": 1}, doc! {"_id": 3}],
    )
    .await;
    l.put("o natural", find_all(&db, "o").await);
    l.put(
        "o natural -1",
        run(&db, doc! {"find": "o", "sort": {"$natural": -1}}, cursor).await,
    );
    l.put(
        "o hint natural -1",
        run(&db, doc! {"find": "o", "hint": {"$natural": -1}}, cursor).await,
    );
    l.put(
        "o sort _id",
        run(&db, doc! {"find": "o", "sort": {"_id": 1}}, cursor).await,
    );
    l.put(
        "o natural bad",
        run(&db, doc! {"find": "o", "sort": {"$natural": 2}}, cursor).await,
    );
    l.put(
        "o natural mixed",
        run(
            &db,
            doc! {"find": "o", "sort": {"$natural": 1, "a": 1}},
            cursor,
        )
        .await,
    );
    l.put(
        "o skip limit",
        run(&db, doc! {"find": "o", "skip": 1, "limit": 1}, cursor).await,
    );
    l.put(
        "o aggregate",
        run(
            &db,
            doc! {"aggregate": "o", "pipeline": [{"$match": {}}], "cursor": {}},
            cursor,
        )
        .await,
    );
    l.put(
        "o count",
        run(&db, doc! {"count": "o"}, |r| r.get("n").cloned().unwrap()).await,
    );
    // Deletes and updates, as MongoDB 8.0 allows them.
    l.put(
        "o deleteOne",
        run(
            &db,
            doc! {"delete": "o", "deletes": [{"q": {}, "limit": 1}]},
            |r| r.get("n").cloned().unwrap(),
        )
        .await,
    );
    l.put("o after deleteOne", find_all(&db, "o").await);
    run(&db, doc! {"create": "g", "capped": true, "size": 4096}, ok).await;
    insert(
        &db,
        "g",
        (1..=3).map(|i| doc! {"_id": i, "s": "a"}).collect(),
    )
    .await;
    l.put(
        "g grow",
        run(
            &db,
            doc! {"update": "g", "updates": [{"q": {"_id": 2}, "u": {"$set": {"s": y(5000)}}}]},
            |r| Bson::Document(doc! {"n": r.get("n").cloned(), "m": r.get("nModified").cloned()}),
        )
        .await,
    );
    l.put(
        "g after grow",
        run(&db, doc! {"find": "g", "projection": {"_id": 1}}, cursor).await,
    );
    l.put("stats g after grow", stats(&db, "g").await);
    insert(&db, "g", vec![doc! {"_id": 4}]).await;
    l.put(
        "g after insert",
        run(&db, doc! {"find": "g", "projection": {"_id": 1}}, cursor).await,
    );
    l.put("stats g after insert", stats(&db, "g").await);
    l.put(
        "g replace",
        run(
            &db,
            doc! {"update": "g", "updates": [{"q": {"_id": 4}, "u": {"z": y(10)}}]},
            |r| r.get("nModified").cloned().unwrap(),
        )
        .await,
    );
    l.put(
        "g findAndModify remove",
        run(
            &db,
            doc! {"findAndModify": "g", "query": {"_id": 3}, "remove": true},
            |r| r.get("value").cloned().unwrap(),
        )
        .await,
    );
    l.put("stats g after delete", stats(&db, "g").await);
    l.put(
        "g deleteMany",
        run(
            &db,
            doc! {"delete": "g", "deletes": [{"q": {}, "limit": 0}]},
            |r| r.get("n").cloned().unwrap(),
        )
        .await,
    );
    insert(&db, "g", vec![doc! {"_id": 7}]).await;
    l.put("stats g after deleteMany", stats(&db, "g").await);
    l.put("g upsert", run(&db, doc! {"update": "g", "updates": [{"q": {"_id": 8}, "u": {"$set": {"a": 1}}, "upsert": true}]}, |r| r.get("n").cloned().unwrap()).await);
    l.put("g after upsert", find_all(&db, "g").await);
    // Indexes.
    l.put(
        "o createIndex",
        run(
            &db,
            doc! {"createIndexes": "o", "indexes": [{"key": {"a": 1}, "name": "a_1"}]},
            ok,
        )
        .await,
    );
    l.put(
        "o drop _id_",
        run(&db, doc! {"dropIndexes": "o", "index": "_id_"}, ok).await,
    );
    // convertToCapped: documents inserted in `_id` order (a plain
    // collection's natural order is insertion order in MongoDB and `_id`
    // order in SprigDB).
    insert(
        &db,
        "u",
        (1..=6)
            .map(|i| doc! {"_id": i, "x": i, "s": y(1000)})
            .collect(),
    )
    .await;
    run(
        &db,
        doc! {"createIndexes": "u", "indexes": [{"key": {"x": 1}, "name": "x_1"}]},
        ok,
    )
    .await;
    l.put(
        "convert",
        run(&db, doc! {"convertToCapped": "u", "size": 4096}, ok).await,
    );
    l.put(
        "u after convert",
        run(&db, doc! {"find": "u", "projection": {"s": 0}}, cursor).await,
    );
    l.put(
        "u indexes",
        run(&db, doc! {"listIndexes": "u"}, |r| {
            Bson::Array(
                r.get_document("cursor")
                    .unwrap()
                    .get_array("firstBatch")
                    .unwrap()
                    .iter()
                    .filter_map(|i| i.as_document().and_then(|i| i.get("name").cloned()))
                    .collect(),
            )
        })
        .await,
    );
    l.put("u options", options(&db, "u").await);
    l.put(
        "convert no size",
        run(&db, doc! {"convertToCapped": "u"}, ok).await,
    );
    l.put(
        "convert missing",
        run(&db, doc! {"convertToCapped": "nope", "size": 4096}, ok).await,
    );
    l.put(
        "convert again",
        run(&db, doc! {"convertToCapped": "u", "size": 8192}, ok).await,
    );
    l.put(
        "u after again",
        run(&db, doc! {"find": "u", "projection": {"s": 0}}, cursor).await,
    );
    l.put("u options again", options(&db, "u").await);
    // cloneCollectionAsCapped.
    insert(
        &db,
        "v",
        (1..=3)
            .map(|i| doc! {"_id": i, "s": y(3000 - i as usize * 900)})
            .collect(),
    )
    .await;
    l.put(
        "clone",
        run(
            &db,
            doc! {"cloneCollectionAsCapped": "v", "toCollection": "vc", "size": 4096},
            ok,
        )
        .await,
    );
    l.put(
        "vc",
        run(&db, doc! {"find": "vc", "projection": {"s": 0}}, cursor).await,
    );
    l.put(
        "v kept",
        run(&db, doc! {"find": "v", "projection": {"s": 0}}, cursor).await,
    );
    l.put("vc options", options(&db, "vc").await);
    l.put(
        "clone exists",
        run(
            &db,
            doc! {"cloneCollectionAsCapped": "v", "toCollection": "vc", "size": 4096},
            ok,
        )
        .await,
    );
    l.put(
        "clone missing",
        run(
            &db,
            doc! {"cloneCollectionAsCapped": "nope", "toCollection": "x", "size": 4096},
            ok,
        )
        .await,
    );
    l.put(
        "clone no size",
        run(
            &db,
            doc! {"cloneCollectionAsCapped": "v", "toCollection": "x2"},
            ok,
        )
        .await,
    );
    // collMod.
    run(
        &db,
        doc! {"create": "cm", "capped": true, "size": 100_000},
        ok,
    )
    .await;
    for i in 0..10 {
        insert(&db, "cm", vec![doc! {"_id": i}]).await;
    }
    l.put(
        "collMod max",
        run(&db, doc! {"collMod": "cm", "cappedMax": 3}, ok).await,
    );
    l.put("cm after collMod", find_all(&db, "cm").await);
    insert(&db, "cm", vec![doc! {"_id": 10}]).await;
    l.put("cm after insert", find_all(&db, "cm").await);
    l.put(
        "collMod size",
        run(&db, doc! {"collMod": "cm", "cappedSize": 8192}, ok).await,
    );
    l.put("cm options", options(&db, "cm").await);
    l.put(
        "collMod size 0",
        run(&db, doc! {"collMod": "cm", "cappedSize": 0}, ok).await,
    );
    l.put(
        "collMod max -1",
        run(&db, doc! {"collMod": "cm", "cappedMax": -1}, ok).await,
    );
    l.put("cm options max -1", options(&db, "cm").await);
    l.put(
        "collMod plain",
        run(&db, doc! {"collMod": "v", "cappedSize": 5000}, ok).await,
    );
    l.put(
        "rename",
        run(
            &client.database("admin"),
            doc! {"renameCollection": format!("{DB}.cm"), "to": format!("{DB}.cm2")},
            ok,
        )
        .await,
    );
    l.put("cm2 options", options(&db, "cm2").await);
    l.put("stats cm2", stats(&db, "cm2").await);
    // `$out` never replaces a capped collection.
    l.put(
        "out to capped",
        run(
            &db,
            doc! {"aggregate": "v", "pipeline": [{"$out": "cm2"}], "cursor": {}},
            ok,
        )
        .await,
    );
    // No writes to capped collections in transactions.
    {
        let mut s = client.start_session().await.unwrap();
        s.start_transaction().await.unwrap();
        let r = db
            .collection::<Document>("cm2")
            .insert_one(doc! {"_id": 99})
            .session(&mut s)
            .await;
        l.put(
            "txn insert",
            Bson::Int32(r.map_or_else(|e| code_of(&e), |_| 0)),
        );
        let r = db
            .collection::<Document>("cm2")
            .find_one(doc! {"_id": 10})
            .session(&mut s)
            .await;
        l.put("txn read", Bson::Boolean(r.is_ok()));
        let _ = s.abort_transaction().await;
    }
    // Tailable cursors: the checks.
    insert(&db, "plain", vec![doc! {"_id": 1}]).await;
    run(
        &db,
        doc! {"create": "cap", "capped": true, "size": 100_000},
        ok,
    )
    .await;
    run(
        &db,
        doc! {"create": "empty", "capped": true, "size": 100_000},
        ok,
    )
    .await;
    for (name, c) in [
        ("tail plain", doc! {"find": "plain", "tailable": true}),
        ("tail missing", doc! {"find": "nope", "tailable": true}),
        ("tail empty", doc! {"find": "empty", "tailable": true}),
        (
            "tail empty await",
            doc! {"find": "empty", "tailable": true, "awaitData": true},
        ),
        ("await alone", doc! {"find": "cap", "awaitData": true}),
        ("await false", doc! {"find": "cap", "awaitData": false}),
        (
            "tail sort",
            doc! {"find": "cap", "tailable": true, "sort": {"a": 1}},
        ),
        (
            "tail natural",
            doc! {"find": "cap", "tailable": true, "sort": {"$natural": 1}},
        ),
        (
            "tail natural -1",
            doc! {"find": "cap", "tailable": true, "sort": {"$natural": -1}},
        ),
        (
            "tail singleBatch",
            doc! {"find": "cap", "tailable": true, "singleBatch": true},
        ),
        (
            "tail limit -2",
            doc! {"find": "cap", "tailable": true, "limit": -2},
        ),
        (
            "tail batch 0 empty",
            doc! {"find": "empty", "tailable": true, "batchSize": 0},
        ),
    ] {
        let v = run(&db, c, cursor).await;
        l.put(name, v);
    }
    // Reading on, waiting, and new documents.
    insert(
        &db,
        "cap",
        (1..=3).map(|i| doc! {"_id": i, "a": i}).collect(),
    )
    .await;
    let r = db
        .run_command(doc! {"find": "cap", "tailable": true, "awaitData": true, "batchSize": 2})
        .await
        .unwrap();
    l.put("tail first", cursor(&r));
    let id = cursor_id(&r);
    l.put(
        "tail gm1",
        run(&db, doc! {"getMore": id, "collection": "cap"}, cursor).await,
    );
    let t = Instant::now();
    let v = run(&db, doc! {"getMore": id, "collection": "cap"}, cursor).await;
    l.put("tail gm empty", v);
    l.put(
        "tail waited 1 s",
        Bson::Boolean(t.elapsed() >= Duration::from_millis(900)),
    );
    let t = Instant::now();
    let v = run(
        &db,
        doc! {"getMore": id, "collection": "cap", "maxTimeMS": 300},
        cursor,
    )
    .await;
    l.put("tail gm 300", v);
    let e = t.elapsed();
    l.put(
        "tail waited 300 ms",
        Bson::Boolean(e >= Duration::from_millis(250) && e < Duration::from_millis(900)),
    );
    insert(&db, "cap", vec![doc! {"_id": 4, "a": 4}]).await;
    l.put(
        "tail after insert",
        run(
            &db,
            doc! {"getMore": id, "collection": "cap", "maxTimeMS": 300},
            cursor,
        )
        .await,
    );
    // An insert from another client while getMore waits.
    let other = client.database(DB);
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        other
            .collection::<Document>("cap")
            .insert_one(doc! {"_id": 5, "a": 5})
            .await
            .unwrap();
    });
    let t = Instant::now();
    let v = run(
        &db,
        doc! {"getMore": id, "collection": "cap", "maxTimeMS": 5000},
        cursor,
    )
    .await;
    writer.await.unwrap();
    l.put("tail during wait", v);
    l.put(
        "tail woke early",
        Bson::Boolean(t.elapsed() < Duration::from_millis(2500)),
    );
    // A filter.
    let r = db
        .run_command(doc! {"find": "cap", "tailable": true, "filter": {"a": {"$gt": 3}}})
        .await
        .unwrap();
    l.put("tail filter", cursor(&r));
    insert(
        &db,
        "cap",
        vec![doc! {"_id": 6, "a": 1}, doc! {"_id": 7, "a": 7}],
    )
    .await;
    l.put(
        "tail filter gm",
        run(
            &db,
            doc! {"getMore": cursor_id(&r), "collection": "cap"},
            cursor,
        )
        .await,
    );
    // Without awaitData: no wait, and maxTimeMS refused.
    let r = db
        .run_command(doc! {"find": "cap", "tailable": true})
        .await
        .unwrap();
    let id2 = cursor_id(&r);
    l.put(
        "tail plain getMore maxTimeMS",
        run(
            &db,
            doc! {"getMore": id2, "collection": "cap", "maxTimeMS": 100},
            cursor,
        )
        .await,
    );
    let t = Instant::now();
    l.put(
        "tail no await",
        run(&db, doc! {"getMore": id2, "collection": "cap"}, cursor).await,
    );
    l.put(
        "tail no await quick",
        Bson::Boolean(t.elapsed() < Duration::from_millis(500)),
    );
    // A plain cursor's getMore with maxTimeMS.
    let r = db
        .run_command(doc! {"find": "o", "batchSize": 1})
        .await
        .unwrap();
    l.put(
        "plain getMore maxTimeMS",
        run(
            &db,
            doc! {"getMore": cursor_id(&r), "collection": "o", "maxTimeMS": 100},
            cursor,
        )
        .await,
    );
    // Position lost: evicted, then deleted.
    run(
        &db,
        doc! {"create": "small", "capped": true, "size": 4096, "max": 3},
        ok,
    )
    .await;
    insert(&db, "small", (1..=3).map(|i| doc! {"_id": i}).collect()).await;
    let r = db
        .run_command(doc! {"find": "small", "tailable": true, "batchSize": 1})
        .await
        .unwrap();
    l.put("lost first", cursor(&r));
    insert(&db, "small", (4..=7).map(|i| doc! {"_id": i}).collect()).await;
    l.put(
        "lost evicted",
        run(
            &db,
            doc! {"getMore": cursor_id(&r), "collection": "small"},
            cursor,
        )
        .await,
    );
    l.put(
        "lost cursor gone",
        run(
            &db,
            doc! {"getMore": cursor_id(&r), "collection": "small"},
            cursor,
        )
        .await,
    );
    let r = db
        .run_command(doc! {"find": "small", "tailable": true})
        .await
        .unwrap();
    l.put("lost2 first", cursor(&r));
    run(
        &db,
        doc! {"delete": "small", "deletes": [{"q": {"_id": 7}, "limit": 1}]},
        ok,
    )
    .await;
    l.put(
        "lost deleted",
        run(
            &db,
            doc! {"getMore": cursor_id(&r), "collection": "small"},
            cursor,
        )
        .await,
    );
    // A plain collection's `size` isn't kept (0 on SprigDB).
    l.put(
        "isCapped plain",
        run(&db, doc! {"collStats": "plain"}, |r| {
            Bson::Boolean(r.get_bool("capped").unwrap_or(true))
        })
        .await,
    );
    let _ = db.drop().await;
    l
}

fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
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

fn compare(want: &Log, got: &Log) -> (usize, Vec<String>) {
    let mut ok = 0;
    let mut bad = Vec::new();
    assert_eq!(want.0.len(), got.0.len(), "both servers ran every step");
    for ((name, a), (_, b)) in want.0.iter().zip(&got.0) {
        if same(a, b) {
            ok += 1;
        } else {
            bad.push(format!("{name}:\n    mongodb {a}\n    sprigdb {b}"));
        }
    }
    (ok, bad)
}

#[tokio::test(flavor = "multi_thread")]
async fn capped_matches_mongodb() {
    let (Some(gw), Some(rf)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_MONGO_REF_URI"))
    else {
        eprintln!("SPRIG_TEST_MONGO_URI / SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let r = Client::with_uri_str(&rf).await.unwrap();
    let g = Client::with_uri_str(&gw).await.unwrap();
    let want = scenario(&r).await;
    let got = scenario(&g).await;
    let (ok, bad) = compare(&want, &got);
    for b in &bad {
        eprintln!("DIFFERENT {b}");
    }
    eprintln!("capped and tailable: {ok} of {} equal", want.0.len());
    assert!(bad.is_empty(), "{} steps differ", bad.len());
}

/// The driver's tailable `awaitData` cursor reading documents another
/// client inserts, as an oplog-style tail does.
async fn tail(client: &Client) -> Vec<Bson> {
    let db = client.database("m7_tail");
    let _ = db.drop().await;
    db.run_command(doc! {"create": "log", "capped": true, "size": 1_000_000, "max": 1000})
        .await
        .unwrap();
    let log = db.collection::<Document>("log");
    log.insert_many((0..5).map(|i| doc! {"_id": i, "ts": i, "msg": format!("m{i}")}))
        .await
        .unwrap();
    let opts = FindOptions::builder()
        .cursor_type(CursorType::TailableAwait)
        .max_await_time(Duration::from_millis(200))
        .batch_size(3)
        .build();
    let mut cur = log
        .find(doc! {"ts": {"$gte": 2}})
        .with_options(opts)
        .await
        .unwrap();
    let w = db.collection::<Document>("log");
    let writer = tokio::spawn(async move {
        for i in 5..25 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            w.insert_one(doc! {"_id": i, "ts": i, "msg": format!("m{i}")})
                .await
                .unwrap();
        }
    });
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while seen.len() < 23 && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), cur.next()).await {
            Ok(Some(Ok(d))) => seen.push(Bson::Document(d)),
            Ok(Some(Err(e))) => {
                seen.push(Bson::String(format!("error {}", code_of(&e))));
                break;
            }
            Ok(None) | Err(_) => break,
        }
    }
    writer.await.unwrap();
    let _ = db.drop().await;
    seen
}

#[tokio::test(flavor = "multi_thread")]
async fn driver_tail_matches_mongodb() {
    let (Some(gw), Some(rf)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_MONGO_REF_URI"))
    else {
        eprintln!("SPRIG_TEST_MONGO_URI / SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let r = Client::with_uri_str(&rf).await.unwrap();
    let g = Client::with_uri_str(&gw).await.unwrap();
    let want = tail(&r).await;
    let t = Instant::now();
    let got = tail(&g).await;
    eprintln!(
        "driver tail: mongodb {} documents, sprigdb {} in {:?}",
        want.len(),
        got.len(),
        t.elapsed()
    );
    assert_eq!(want.len(), 23);
    assert!(same(&Bson::Array(want), &Bson::Array(got)));
}
