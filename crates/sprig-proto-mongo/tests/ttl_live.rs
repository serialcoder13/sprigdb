//! TTL indexes against MongoDB 8.0.34 (wireup, 13 Nov 2026).
//!
//! - `ttl_matches_mongodb`: the same commands on the gateway and on the
//!   reference, each answer compared: `createIndexes` with every
//!   `expireAfterSeconds` check (types, NaN, range, compound, `_id`,
//!   conflicts), `listIndexes` (with the value's BSON type), `collMod`'s
//!   `index` option and its errors; then expiry: dates, arrays holding a
//!   date, nested paths through arrays, non-dates that never expire, a
//!   capped collection, an `expireAfterSeconds` lowered by `collMod`, and
//!   the `delete` change events the monitor's deletes give.
//! - `ttl_deletes_are_batched_and_paced`: 1,000 expired documents go in
//!   several passes' batches; with `SPRIG_TEST_TTL_RATE` (the deployment's
//!   `doc.ttl_tenant_deletes_per_second`) they take at least about
//!   1,000 / rate seconds.
//! - `ttl_through_a_move`: with `SPRIG_TEST_MOVE_CMDS` (commands separated
//!   by `;;` that each move the test tenant and return once the cutover
//!   committed), documents keep expiring while the tenant moves: every one
//!   is deleted once, each with one `delete` event, none twice.
//!
//! Variables: `SPRIG_TEST_MONGO_URI` (the gateway; skipped without it),
//! `SPRIG_TEST_MONGO_REF_URI` (MongoDB 8.0.34 with `ttlMonitorSleepSecs`
//! as the gateway's `doc.ttl_interval_seconds`, a replica set). Both
//! monitors should run every few seconds: the test waits up to 60 s.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::print_stderr,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use std::collections::BTreeSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use mongodb::bson::{Bson, DateTime, Document, Timestamp, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ChangeStreamEvent;
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn at(off_ms: i64) -> Bson {
    Bson::DateTime(DateTime::from_millis(now_ms() + off_ms))
}

const DB: &str = "wireup_ttl";

/// A command's answer: the error code, or the reply without the cluster
/// fields.
async fn run(db: &Database, c: Document) -> Bson {
    match db.run_command(c).await {
        Ok(mut r) => {
            // Replica-set fields (`commitQuorum`) and notes aren't compared.
            for k in [
                "$clusterTime",
                "operationTime",
                "createdCollectionAutomatically",
                "commitQuorum",
                "note",
            ] {
                r.remove(k);
            }
            Bson::Document(r)
        }
        Err(e) => Bson::Document(doc! {"error": code_of(&e)}),
    }
}

/// The indexes of `coll` as listed, with each value's BSON type.
async fn indexes(db: &Database, coll: &str) -> Bson {
    match db.run_command(doc! {"listIndexes": coll}).await {
        Ok(r) => {
            let b = r
                .get_document("cursor")
                .unwrap()
                .get_array("firstBatch")
                .unwrap()
                .clone();
            Bson::Array(
                b.into_iter()
                    .map(|i| {
                        let d = i.as_document().unwrap();
                        let mut o = doc! {"name": d.get_str("name").unwrap(), "key": d.get_document("key").unwrap().clone()};
                        if let Some(v) = d.get("expireAfterSeconds") {
                            o.insert("expireAfterSeconds", v.clone());
                            o.insert("type", format!("{:?}", v.element_type()));
                        }
                        Bson::Document(o)
                    })
                    .collect(),
            )
        }
        Err(e) => Bson::Document(doc! {"error": code_of(&e)}),
    }
}

async fn ids(db: &Database, coll: &str) -> Vec<Bson> {
    let mut c = db
        .collection::<Document>(coll)
        .find(doc! {})
        .sort(doc! {"_id": 1})
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Some(d) = c.next().await {
        out.push(d.unwrap().get("_id").cloned().unwrap());
    }
    out
}

/// The checks and their answers, by step name.
async fn checks(db: &Database) -> Vec<(String, Bson)> {
    let _ = db.drop().await;
    let mut log: Vec<(String, Bson)> = Vec::new();
    let ci = |coll: &str, key: Document, extra: Document| {
        let mut spec = doc! {"key": key.clone()};
        spec.extend(extra);
        if !spec.contains_key("name") {
            let n: Vec<String> = key.iter().map(|(k, v)| format!("{k}_{v}")).collect();
            spec.insert("name", n.join("_"));
        }
        doc! {"createIndexes": coll, "indexes": [spec]}
    };
    let steps: Vec<(&str, Document)> = vec![
        (
            "create_ttl",
            ci("c", doc! {"at": 1}, doc! {"expireAfterSeconds": 10}),
        ),
        (
            "create_ttl_double",
            ci("c2", doc! {"at": 1}, doc! {"expireAfterSeconds": 10.7}),
        ),
        (
            "create_ttl_long",
            ci("c2b", doc! {"at": 1}, doc! {"expireAfterSeconds": 12_i64}),
        ),
        (
            "create_ttl_neg",
            ci("c3", doc! {"at": 1}, doc! {"expireAfterSeconds": -1}),
        ),
        (
            "create_ttl_big",
            ci(
                "c3",
                doc! {"at": 1},
                doc! {"expireAfterSeconds": 2_147_483_648.0},
            ),
        ),
        (
            "create_ttl_max",
            ci(
                "c3",
                doc! {"b": 1},
                doc! {"expireAfterSeconds": 2_147_483_647},
            ),
        ),
        (
            "create_ttl_str",
            ci("c3", doc! {"c": 1}, doc! {"expireAfterSeconds": "10"}),
        ),
        (
            "create_ttl_compound",
            ci("c4", doc! {"a": 1, "b": 1}, doc! {"expireAfterSeconds": 10}),
        ),
        (
            "create_ttl_id",
            ci("c4", doc! {"_id": 1}, doc! {"expireAfterSeconds": 10}),
        ),
        (
            "create_ttl_desc",
            ci("c4", doc! {"z": -1}, doc! {"expireAfterSeconds": 10}),
        ),
        (
            "create_ttl_nan",
            ci("c4", doc! {"n": 1}, doc! {"expireAfterSeconds": f64::NAN}),
        ),
        (
            "create_ttl_bool",
            ci("c4", doc! {"bo": 1}, doc! {"expireAfterSeconds": true}),
        ),
        (
            "same_key_diff_ttl",
            ci("c", doc! {"at": 1}, doc! {"expireAfterSeconds": 20}),
        ),
        ("same_key_no_ttl", ci("c", doc! {"at": 1}, doc! {})),
        (
            "same_key_same",
            ci("c", doc! {"at": 1}, doc! {"expireAfterSeconds": 10}),
        ),
        ("create_plain", ci("c5", doc! {"w": 1}, doc! {})),
        ("create_compound", ci("c5", doc! {"p": 1, "q": 1}, doc! {})),
        (
            "collmod_name",
            doc! {"collMod": "c", "index": {"name": "at_1", "expireAfterSeconds": 30}},
        ),
        (
            "collmod_key",
            doc! {"collMod": "c", "index": {"keyPattern": {"at": 1}, "expireAfterSeconds": 40}},
        ),
        (
            "collmod_same",
            doc! {"collMod": "c", "index": {"keyPattern": {"at": 1}, "expireAfterSeconds": 40}},
        ),
        (
            "collmod_neg",
            doc! {"collMod": "c", "index": {"keyPattern": {"at": 1}, "expireAfterSeconds": -5}},
        ),
        (
            "collmod_str",
            doc! {"collMod": "c", "index": {"keyPattern": {"at": 1}, "expireAfterSeconds": "5"}},
        ),
        (
            "collmod_dbl",
            doc! {"collMod": "c", "index": {"keyPattern": {"at": 1}, "expireAfterSeconds": 5.5}},
        ),
        (
            "collmod_missing",
            doc! {"collMod": "c", "index": {"name": "nope", "expireAfterSeconds": 5}},
        ),
        (
            "collmod_noident",
            doc! {"collMod": "c", "index": {"expireAfterSeconds": 5}},
        ),
        (
            "collmod_both",
            doc! {"collMod": "c", "index": {"name": "at_1", "keyPattern": {"at": 1}, "expireAfterSeconds": 5}},
        ),
        (
            "collmod_nothing",
            doc! {"collMod": "c", "index": {"name": "at_1"}},
        ),
        (
            "collmod_convert",
            doc! {"collMod": "c5", "index": {"name": "w_1", "expireAfterSeconds": 7}},
        ),
        (
            "collmod_convert_compound",
            doc! {"collMod": "c5", "index": {"name": "p_1_q_1", "expireAfterSeconds": 7}},
        ),
        (
            "collmod_id",
            doc! {"collMod": "c5", "index": {"name": "_id_", "expireAfterSeconds": 7}},
        ),
        (
            "collmod_nocoll",
            doc! {"collMod": "nosuch", "index": {"name": "x", "expireAfterSeconds": 7}},
        ),
        ("collmod_index_notdoc", doc! {"collMod": "c", "index": 5}),
        (
            "collmod_unknownfield",
            doc! {"collMod": "c", "index": {"name": "at_1", "expireAfterSeconds": 5, "foo": 1}},
        ),
    ];
    for (name, cmd) in steps {
        log.push((name.to_owned(), run(db, cmd).await));
    }
    for coll in ["c", "c2", "c2b", "c3", "c4", "c5"] {
        log.push((format!("list_{coll}"), indexes(db, coll).await));
    }
    log
}

/// The expiry data set (each server gets its own copy, made now).
async fn load(db: &Database) {
    let c = db.collection::<Document>("x");
    db.run_command(doc! {"createIndexes": "x", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
        .await
        .unwrap();
    c.insert_many(vec![
        doc! {"_id": "old", "at": at(-100_000)},
        doc! {"_id": "new", "at": at(100_000)},
        doc! {"_id": "arr_mixed", "at": [at(100_000), at(-100_000)]},
        doc! {"_id": "arr_new", "at": [at(100_000), at(200_000)]},
        doc! {"_id": "arr_nondate_old", "at": ["x", 5, at(-100_000)]},
        doc! {"_id": "arr_nested", "at": [[at(-100_000)]]},
        doc! {"_id": "str", "at": "2001-01-01"},
        doc! {"_id": "num", "at": 5},
        doc! {"_id": "null", "at": Bson::Null},
        doc! {"_id": "missing"},
        doc! {"_id": "ts", "at": Timestamp { time: 1, increment: 1 }},
        doc! {"_id": "epoch_neg", "at": Bson::DateTime(DateTime::from_millis(-1000))},
        doc! {"_id": "obj", "at": {"d": at(-100_000)}},
    ])
    .await
    .unwrap();
    db.run_command(doc! {"createIndexes": "n", "indexes": [{"key": {"a.b": 1}, "name": "ab", "expireAfterSeconds": 0}]})
        .await
        .unwrap();
    db.collection::<Document>("n")
        .insert_many(vec![
            doc! {"_id": 1, "a": {"b": at(-100_000)}},
            doc! {"_id": 2, "a": [{"b": at(-100_000)}, {"b": at(100_000)}]},
            doc! {"_id": 3, "a": [{"b": at(100_000)}]},
        ])
        .await
        .unwrap();
    db.run_command(doc! {"createIndexes": "e", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 3600}]})
        .await
        .unwrap();
    db.collection::<Document>("e")
        .insert_many(vec![
            doc! {"_id": 1, "at": at(-3_000_000)},
            doc! {"_id": 2, "at": at(-4_000_000)},
        ])
        .await
        .unwrap();
    db.run_command(doc! {"create": "cap", "capped": true, "size": 100_000})
        .await
        .unwrap();
    db.run_command(doc! {"createIndexes": "cap", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
        .await
        .unwrap();
    db.collection::<Document>("cap")
        .insert_many(vec![
            doc! {"_id": 1, "at": at(-100_000)},
            doc! {"_id": 2, "at": at(100_000)},
        ])
        .await
        .unwrap();
    // Lowered by collMod: nothing expires at an hour, everything at 10 s.
    db.run_command(doc! {"createIndexes": "m", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 3600}]})
        .await
        .unwrap();
    db.collection::<Document>("m")
        .insert_many(vec![
            doc! {"_id": 1, "at": at(-100_000)},
            doc! {"_id": 2, "at": at(-1000)},
        ])
        .await
        .unwrap();
}

/// What expiry leaves, once both servers' monitors have run: the
/// collections' `_id`s.
async fn left(db: &Database) -> Vec<(String, Vec<Bson>)> {
    let mut out = Vec::new();
    for c in ["x", "n", "e", "cap", "m"] {
        out.push((c.to_owned(), ids(db, c).await));
    }
    out
}

async fn events(
    s: &mut ChangeStream<ChangeStreamEvent<Document>>,
    n: usize,
    secs: u64,
) -> Vec<(String, Bson)> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while out.len() < n {
        match tokio::time::timeout_at(deadline, s.next()).await {
            Ok(Some(Ok(e))) => out.push((
                format!("{:?}", e.operation_type),
                e.document_key
                    .and_then(|k| k.get("_id").cloned())
                    .unwrap_or(Bson::Null),
            )),
            Ok(Some(Err(e))) => panic!("change stream failed: {e}"),
            Ok(None) | Err(_) => break,
        }
    }
    out
}

/// Waits until `f` holds, up to `secs`.
async fn until<F, Fut>(secs: u64, f: F) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(secs) {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

async fn clients() -> Option<(Database, Database)> {
    let (Some(gw), Some(r)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_MONGO_REF_URI")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return None;
    };
    let g = Client::with_uri_str(&gw).await.unwrap().database(DB);
    let r = Client::with_uri_str(&r).await.unwrap().database(DB);
    Some((g, r))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_matches_mongodb() {
    let Some((g, r)) = clients().await else {
        return;
    };
    let (cg, cr) = (checks(&g).await, checks(&r).await);
    let mut same = 0;
    let mut diff = Vec::new();
    for ((n, a), (_, b)) in cg.iter().zip(&cr) {
        if a == b {
            same += 1;
        } else {
            diff.push(format!("{n}:\n  gateway   {a}\n  reference {b}"));
        }
    }
    eprintln!("TTL checks: {same} of {} equal", cg.len());
    assert!(diff.is_empty(), "{}", diff.join("\n"));

    // Expiry, with a change stream on one collection.
    for db in [&g, &r] {
        let _ = db.drop().await;
        db.run_command(doc! {"create": "cs"}).await.unwrap();
    }
    let mut sg = g.collection::<Document>("cs").watch().await.unwrap();
    let mut sr = r.collection::<Document>("cs").watch().await.unwrap();
    for db in [&g, &r] {
        load(db).await;
        db.run_command(doc! {"createIndexes": "cs", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
            .await
            .unwrap();
        db.collection::<Document>("cs")
            .insert_many(vec![
                doc! {"_id": 1, "at": at(-1000)},
                doc! {"_id": 2, "at": at(60_000)},
            ])
            .await
            .unwrap();
        let rep = run(
            db,
            doc! {"collMod": "m", "index": {"name": "at_1", "expireAfterSeconds": 10}},
        )
        .await;
        eprintln!("collMod m: {rep}");
    }
    let want_x: Vec<Bson> = [
        "arr_nested",
        "arr_new",
        "missing",
        "new",
        "null",
        "num",
        "obj",
        "str",
        "ts",
    ]
    .into_iter()
    .map(|s| Bson::String(s.into()))
    .collect();
    let done = |db: Database| {
        let w = want_x.clone();
        async move {
            let l = left(&db).await;
            l[0].1 == w
                && l[1].1.len() == 1
                && l[2].1.len() == 1
                && l[3].1.len() == 1
                && l[4].1.is_empty()
        }
    };
    let ok_r = until(60, || done(r.clone())).await;
    let ok_g = until(60, || done(g.clone())).await;
    let (lg, lr) = (left(&g).await, left(&r).await);
    eprintln!("left on the gateway:   {lg:?}");
    eprintln!("left on the reference: {lr:?}");
    assert!(ok_r, "the reference didn't expire in time");
    assert!(ok_g, "the gateway didn't expire in time");
    assert_eq!(lg, lr);
    let (eg, er) = (events(&mut sg, 3, 30).await, events(&mut sr, 3, 30).await);
    eprintln!("change events: gateway {eg:?}, reference {er:?}");
    assert_eq!(eg, er);
    let _ = g.drop().await;
    let _ = r.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_deletes_are_batched_and_paced() {
    let Some(gw) = var("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let db = Client::with_uri_str(&gw)
        .await
        .unwrap()
        .database("wireup_ttl_bulk");
    let _ = db.drop().await;
    let c = db.collection::<Document>("bulk");
    c.insert_many((0..1000).map(|i| doc! {"_id": i, "at": at(-60_000)}))
        .await
        .unwrap();
    db.run_command(doc! {"createIndexes": "bulk", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
        .await
        .unwrap();
    let t0 = Instant::now();
    let mut first: Option<Duration> = None;
    let mut samples = Vec::new();
    loop {
        let n = c.count_documents(doc! {}).await.unwrap();
        if n < 1000 && first.is_none() {
            first = Some(t0.elapsed());
        }
        samples.push(n);
        if n == 0 || t0.elapsed() > Duration::from_secs(120) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let last = t0.elapsed();
    let first = first.unwrap_or(last);
    let distinct: BTreeSet<u64> = samples.iter().copied().collect();
    eprintln!(
        "1000 expired: first delete after {first:?}, all gone after {last:?}; {} distinct counts seen",
        distinct.len()
    );
    assert_eq!(*samples.last().unwrap(), 0, "not all expired in 120 s");
    if let Some(rate) = var("SPRIG_TEST_TTL_RATE").and_then(|r| r.parse::<f64>().ok()) {
        let took = last.saturating_sub(first).as_secs_f64();
        assert!(
            took >= 0.75 * 1000.0 / rate - 0.5,
            "1000 deletes in {took:.1} s at {rate}/s"
        );
        // Paced: the count went down in several steps.
        assert!(distinct.len() >= 4, "{samples:?}");
    }
    let _ = db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_through_a_move() {
    let (Some(gw), Some(cmds)) = (var("SPRIG_TEST_MONGO_URI"), var("SPRIG_TEST_MOVE_CMDS")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MOVE_CMDS not set: skipped");
        return;
    };
    let span_ms: i64 = var("SPRIG_TEST_TTL_SPAN_MS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(240_000);
    let n: i64 = 600;
    let db = Client::with_uri_str(&gw)
        .await
        .unwrap()
        .database("wireup_ttl_mv");
    let _ = db.drop().await;
    db.run_command(doc! {"create": "docs"}).await.unwrap();
    let c = db.collection::<Document>("docs");
    db.run_command(doc! {"createIndexes": "docs", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
        .await
        .unwrap();
    let mut s = c.watch().await.unwrap();
    // Expiring one by one over the span, while the tenant moves.
    let base = now_ms() + 5_000;
    c.insert_many((0..n).map(|i| {
        doc! {"_id": i, "at": Bson::DateTime(DateTime::from_millis(base + i * span_ms / n))}
    }))
    .await
    .unwrap();
    let watcher = tokio::spawn(async move {
        events(&mut s, (2 * n) as usize, (span_ms / 1000 + 400) as u64).await
    });
    for cmd in cmds.split(";;").map(str::trim).filter(|c| !c.is_empty()) {
        let t = Instant::now();
        let left = c.count_documents(doc! {}).await.unwrap();
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .unwrap();
        eprintln!(
            "move `{cmd}`: {} in {:?} ({left} documents before), {}",
            out.status,
            t.elapsed(),
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .last()
                .unwrap_or("")
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let gone = until((span_ms / 1000 + 180) as u64, || {
        let c = c.clone();
        async move { c.count_documents(doc! {}).await.unwrap() == 0 }
    })
    .await;
    assert!(
        gone,
        "documents left: {}",
        c.count_documents(doc! {}).await.unwrap()
    );
    let ev = watcher.await.unwrap();
    let inserts = ev.iter().filter(|(t, _)| t == "Insert").count();
    let deletes: Vec<&Bson> = ev
        .iter()
        .filter(|(t, _)| t == "Delete")
        .map(|(_, k)| k)
        .collect();
    let distinct: BTreeSet<String> = deletes.iter().map(ToString::to_string).collect();
    eprintln!(
        "through the move: {inserts} insert events, {} delete events, {} distinct",
        deletes.len(),
        distinct.len()
    );
    assert_eq!(inserts as i64, n);
    assert_eq!(deletes.len() as i64, n, "every document deleted once");
    assert_eq!(distinct.len() as i64, n);
    let _ = db.drop().await;
}
