//! Schema validation, `$jsonSchema`, `$bits*`, query settings, query
//! statistics and the 9.0 limits (M3, 12 Nov 2026) through a SprigDB
//! gateway's MongoDB listener and the MongoDB Rust driver.
//!
//! - `validation_corpus_through_the_gateway`: the corpus of
//!   `crates/sprig-doc/tests/validate_diff/cases.json` (answers from MongoDB
//!   8.0.34): every validator case through `collMod` and `insert`, compared
//!   by code and `errInfo`; every filter case through `find`.
//! - `validation_on_every_write_path`: insert, update, replace, upsert,
//!   `findAndModify`, `bulkWrite`, `$merge`, `$out`, `bypassDocumentValidation`,
//!   the levels (`moderate`, `off`, 9.0's `constraint`) and actions (`warn`,
//!   8.1's `errorAndLog`), and `listCollections`' options.
//! - `query_settings_and_stats`: `setQuerySettings` (`reject`, 8.1's
//!   `comment`, 9.0's `maxTimeMS`), `$querySettings`, `removeQuerySettings`,
//!   9.0's per-command `querySettings`, and `$queryStats` with writes.
//! - `limits` (only with `SPRIG_TEST_M3_LIMITS=1`, after setting
//!   `mongo.max_open_transactions` to 2 and
//!   `mongo.max_operation_memory_bytes` to 100000 on the deployment): 509
//!   and 146.
//!
//! `SPRIG_TEST_MONGO_URI`: a gateway's MongoDB listener with a tenant login.
//! Skipped when unset. Uses the databases `m3_*` and drops them.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::print_stderr)]

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Database};

fn uri() -> Option<String> {
    let u = std::env::var("SPRIG_TEST_MONGO_URI")
        .ok()
        .filter(|s| !s.is_empty());
    if u.is_none() {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
    }
    u
}

fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && x
                    .iter()
                    .zip(y.iter())
                    .all(|((ka, va), (kb, vb))| ka == kb && same(va, vb))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        // mongosh recorded whole doubles in errInfo as int32.
        (Bson::Double(x), Bson::Int32(y)) | (Bson::Int32(y), Bson::Double(x)) => {
            *x == f64::from(*y)
        }
        _ => a == b,
    }
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code,
        mongodb::error::ErrorKind::InsertMany(m) => m
            .write_errors
            .as_ref()
            .and_then(|w| w.first())
            .map_or(-1, |w| w.code),
        mongodb::error::ErrorKind::BulkWrite(b) => {
            b.write_errors.values().next().map_or(-1, |w| w.code)
        }
        _ => -1,
    }
}

async fn run(db: &Database, cmd: Document) -> Result<Document, mongodb::error::Error> {
    db.run_command(cmd).await
}

/// `insert` of one document: `Ok` or `(code, errInfo)`.
async fn insert_one(db: &Database, coll: &str, d: &Document, extra: Document) -> Result<(), (i32, Option<Document>)> {
    let mut cmd = doc! {"insert": coll, "documents": [d.clone()]};
    cmd.extend(extra);
    match run(db, cmd).await {
        Err(e) => Err((code_of(&e), None)),
        Ok(r) => match r.get_array("writeErrors").ok().and_then(|a| a.first()) {
            Some(Bson::Document(w)) => Err((
                w.get_i32("code").unwrap_or(-1),
                w.get_document("errInfo").ok().cloned(),
            )),
            _ => Ok(()),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_corpus_through_the_gateway() {
    let Some(uri) = uri() else { return };
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../sprig-doc/tests/validate_diff");
    let text = std::fs::read_to_string(format!("{dir}/cases.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    let Bson::Array(cases) = Bson::try_from(json).unwrap() else {
        panic!("cases.json is not an array");
    };
    let known: Vec<String> = std::fs::read_to_string(format!("{dir}/known.txt"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .filter_map(|l| l.split_whitespace().next().map(str::to_owned))
        .collect();
    let client = Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("m3_corpus");
    let _ = db.drop().await;
    run(&db, doc! {"create": "v"}).await.unwrap();
    let (mut pass, mut fail, mut known_fail, mut with_info) = (0, Vec::new(), 0, 0);
    let mut filter_colls: Vec<(Bson, String)> = Vec::new();
    for c in &cases {
        let c = c.as_document().unwrap();
        let name = c.get_str("name").unwrap();
        let want = c.get_document("expect").unwrap();
        let ok = if let Ok(v) = c.get_document("validator") {
            let modded = run(&db, doc! {"collMod": "v", "validator": v.clone(), "validationLevel": "strict", "validationAction": "error"}).await;
            match (modded, want.get("createCode")) {
                (Err(e), Some(w)) => Bson::Int32(code_of(&e)) == *w || w.as_i64() == Some(i64::from(code_of(&e))),
                (Err(_), None) | (Ok(_), Some(_)) => false,
                (Ok(_), None) => {
                    let d = c.get_document("doc").unwrap();
                    let r = insert_one(&db, "v", d, doc! {}).await;
                    if r.is_ok() {
                        let _ = run(&db, doc! {"delete": "v", "deletes": [{"q": {"_id": d.get("_id").cloned().unwrap()}, "limit": 0}]}).await;
                    }
                    match (r, want.get_bool("ok").is_ok()) {
                        (Ok(()), true) => true,
                        (Err((code, info)), false) => {
                            let wc = want.get("code").and_then(Bson::as_i32).unwrap_or(-2);
                            let wi = want.get_document("errInfo").ok();
                            if wi.is_some() {
                                with_info += 1;
                            }
                            code == wc
                                && match (info, wi) {
                                    (Some(g), Some(w)) => same(&Bson::Document(g), &Bson::Document(w.clone())),
                                    (None, None) => true,
                                    _ => false,
                                }
                        }
                        _ => false,
                    }
                }
            }
        } else {
            // Filter cases: each distinct set of documents once.
            let docs = c.get("docs").unwrap().clone();
            let coll = if let Some((_, n)) = filter_colls.iter().find(|(d, _)| *d == docs) {
                n.clone()
            } else {
                let n = format!("f{}", filter_colls.len());
                let list: Vec<Document> = docs
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|d| d.as_document().unwrap().clone())
                    .collect();
                db.collection::<Document>(&n).insert_many(list).await.unwrap();
                filter_colls.push((docs, n.clone()));
                n
            };
            let f = c.get_document("filter").unwrap().clone();
            let r = db
                .collection::<Document>(&coll)
                .find(f)
                .sort(doc! {"_id": 1})
                .await;
            let got = match r {
                Ok(cur) => match cur.try_collect::<Vec<Document>>().await {
                    Ok(v) => Ok(v.into_iter().map(|d| d.get("_id").cloned().unwrap()).collect::<Vec<_>>()),
                    Err(e) => Err(code_of(&e)),
                },
                Err(e) => Err(code_of(&e)),
            };
            match (got, want.get_array("ids"), want.get("code")) {
                (Ok(ids), Ok(w), _) => same(&Bson::Array(ids), &Bson::Array(w.clone())),
                (Err(code), _, Some(w)) => w.as_i32() == Some(code),
                _ => false,
            }
        };
        if ok {
            pass += 1;
        } else if known.iter().any(|k| k == name) {
            known_fail += 1;
        } else {
            eprintln!("MISMATCH {name}");
            fail.push(name.to_owned());
        }
    }
    eprintln!(
        "validation corpus through the gateway: {pass} pass, {} fail, {known_fail} known ({with_info} with errInfo)",
        fail.len()
    );
    db.drop().await.unwrap();
    assert!(fail.is_empty(), "mismatches: {fail:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_on_every_write_path() {
    let Some(uri) = uri() else { return };
    let client = Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("m3_paths");
    let _ = db.drop().await;
    let schema = doc! {"$jsonSchema": {"required": ["x"], "properties": {"x": {"bsonType": "int", "minimum": 0}}}};
    run(&db, doc! {"create": "c", "validator": schema.clone()}).await.unwrap();
    // listCollections shows the validator as given.
    let info = run(&db, doc! {"listCollections": 1, "filter": {"name": "c"}}).await.unwrap();
    let opts = info.get_document("cursor").unwrap().get_array("firstBatch").unwrap()[0]
        .as_document().unwrap().get_document("options").unwrap().clone();
    assert_eq!(opts, doc! {"validator": schema.clone()});
    // insert: the bad one fails with 121 and errInfo; an unordered batch
    // keeps the good ones.
    let r = run(&db, doc! {"insert": "c", "documents": [{"_id": 1, "x": 1}, {"_id": 2, "x": -1}, {"_id": 3, "x": 3}], "ordered": false}).await.unwrap();
    assert_eq!(r.get_i32("n").unwrap(), 2);
    let we = r.get_array("writeErrors").unwrap()[0].as_document().unwrap().clone();
    assert_eq!(we.get_i32("code").unwrap(), 121);
    assert_eq!(we.get_i32("index").unwrap(), 1);
    let ei = we.get_document("errInfo").unwrap();
    assert_eq!(ei.get_i32("failingDocumentId").unwrap(), 2);
    assert_eq!(ei.get_document("details").unwrap().get_str("operatorName").unwrap(), "$jsonSchema");
    // update and replace.
    let r = run(&db, doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"x": "s"}}}]}).await.unwrap();
    assert_eq!(r.get_array("writeErrors").unwrap()[0].as_document().unwrap().get_i32("code").unwrap(), 121);
    let r = run(&db, doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"y": 1}}]}).await.unwrap();
    assert_eq!(r.get_array("writeErrors").unwrap()[0].as_document().unwrap().get_i32("code").unwrap(), 121);
    let r = run(&db, doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"x": 5}}}]}).await.unwrap();
    assert_eq!(r.get_i32("nModified").unwrap(), 1);
    // upsert.
    let r = run(&db, doc! {"update": "c", "updates": [{"q": {"_id": 9}, "u": {"$set": {"y": 1}}, "upsert": true}]}).await.unwrap();
    assert_eq!(r.get_array("writeErrors").unwrap()[0].as_document().unwrap().get_i32("code").unwrap(), 121);
    // findAndModify: a command error with errInfo.
    let e = run(&db, doc! {"findAndModify": "c", "query": {"_id": 1}, "update": {"$set": {"x": -5}}}).await.unwrap_err();
    assert_eq!(code_of(&e), 121);
    if let mongodb::error::ErrorKind::Command(ce) = e.kind.as_ref() {
        assert!(ce.message.contains("Document failed validation"), "{}", ce.message);
    }
    // bulkWrite (8.0) on admin.
    let admin = client.database("admin");
    let r = run(&admin, doc! {"bulkWrite": 1, "ops": [{"insert": 0, "document": {"_id": 20, "x": "bad"}}, {"insert": 0, "document": {"_id": 21, "x": 2}}], "nsInfo": [{"ns": "m3_paths.c"}], "ordered": false}).await.unwrap();
    assert_eq!(r.get_i32("nErrors").unwrap(), 1);
    assert_eq!(r.get_i32("nInserted").unwrap(), 1);
    // bypassDocumentValidation.
    let r = run(&db, doc! {"insert": "c", "documents": [{"_id": 30, "x": "bypassed"}], "bypassDocumentValidation": true}).await.unwrap();
    assert_eq!(r.get_i32("n").unwrap(), 1);
    // moderate: an already-invalid document may be updated.
    run(&db, doc! {"collMod": "c", "validationLevel": "moderate"}).await.unwrap();
    let r = run(&db, doc! {"update": "c", "updates": [{"q": {"_id": 30}, "u": {"$set": {"z": 1}}}]}).await.unwrap();
    assert!(r.get_array("writeErrors").is_err(), "{r}");
    let r = run(&db, doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"x": "now bad"}}}]}).await.unwrap();
    assert!(r.get_array("writeErrors").is_ok(), "{r}");
    // collMod writes all three options.
    let info = run(&db, doc! {"listCollections": 1, "filter": {"name": "c"}}).await.unwrap();
    let opts = info.get_document("cursor").unwrap().get_array("firstBatch").unwrap()[0]
        .as_document().unwrap().get_document("options").unwrap().clone();
    assert_eq!(opts.get_str("validationLevel").unwrap(), "moderate");
    assert_eq!(opts.get_str("validationAction").unwrap(), "error");
    // warn accepts, errorAndLog refuses.
    run(&db, doc! {"collMod": "c", "validationLevel": "strict", "validationAction": "warn"}).await.unwrap();
    insert_one(&db, "c", &doc! {"_id": 40, "x": "warned"}, doc! {}).await.unwrap();
    run(&db, doc! {"collMod": "c", "validationAction": "errorAndLog"}).await.unwrap();
    assert_eq!(insert_one(&db, "c", &doc! {"_id": 41, "x": "logged"}, doc! {}).await.unwrap_err().0, 121);
    // off.
    run(&db, doc! {"collMod": "c", "validationLevel": "off"}).await.unwrap();
    insert_one(&db, "c", &doc! {"_id": 42}, doc! {}).await.unwrap();
    // 9.0's constraint: needs prepare; the scan finds invalid documents.
    run(&db, doc! {"collMod": "c", "validationLevel": "strict", "validationAction": "error"}).await.unwrap();
    let e = run(&db, doc! {"collMod": "c", "validationLevel": "constraint"}).await.unwrap_err();
    assert_eq!(code_of(&e), 72);
    run(&db, doc! {"collMod": "c", "prepareConstraintValidationLevel": true}).await.unwrap();
    // Prepared: no bypass.
    assert_eq!(insert_one(&db, "c", &doc! {"_id": 43}, doc! {"bypassDocumentValidation": true}).await.unwrap_err().0, 121);
    let e = run(&db, doc! {"collMod": "c", "validationLevel": "constraint"}).await.unwrap_err();
    assert_eq!(code_of(&e), 121, "{e}");
    run(&db, doc! {"delete": "c", "deletes": [{"q": {"$nor": [schema.clone()]}, "limit": 0}]}).await.unwrap();
    run(&db, doc! {"collMod": "c", "validationLevel": "constraint"}).await.unwrap();
    let e = run(&db, doc! {"collMod": "c", "validator": {"x": 1}}).await.unwrap_err();
    assert_eq!(code_of(&e), 72);
    assert_eq!(insert_one(&db, "c", &doc! {"_id": 44}, doc! {"bypassDocumentValidation": true}).await.unwrap_err().0, 121);
    // $merge and $out into a validated collection.
    run(&db, doc! {"create": "t", "validator": {"x": {"$type": "int"}}}).await.unwrap();
    db.collection::<Document>("src").insert_many(vec![doc! {"_id": 1, "x": 1}, doc! {"_id": 2, "x": "s"}]).await.unwrap();
    let e = run(&db, doc! {"aggregate": "src", "pipeline": [{"$merge": {"into": "t"}}], "cursor": {}}).await.unwrap_err();
    assert_eq!(code_of(&e), 121);
    run(&db, doc! {"aggregate": "src", "pipeline": [{"$merge": {"into": "t"}}], "cursor": {}, "bypassDocumentValidation": true}).await.unwrap();
    let e = run(&db, doc! {"aggregate": "src", "pipeline": [{"$out": "t"}], "cursor": {}}).await.unwrap_err();
    assert_eq!(code_of(&e), 121);
    let info = run(&db, doc! {"listCollections": 1, "filter": {"name": "t"}}).await.unwrap();
    let opts = info.get_document("cursor").unwrap().get_array("firstBatch").unwrap()[0]
        .as_document().unwrap().get_document("options").unwrap().clone();
    assert!(opts.contains_key("validator"), "$out kept the target's options: {opts}");
    // Bad validators are refused at create.
    let e = run(&db, doc! {"create": "bad", "validator": {"$jsonSchema": {"foo": 1}}}).await.unwrap_err();
    assert_eq!(code_of(&e), 9);
    let e = run(&db, doc! {"create": "bad", "validator": {"$where": "1"}}).await.unwrap_err();
    assert_eq!(code_of(&e), 2);
    // $jsonSchema and $bits in queries.
    let n = db.collection::<Document>("src").count_documents(doc! {"$jsonSchema": {"properties": {"x": {"bsonType": "int"}}}}).await.unwrap();
    assert_eq!(n, 1);
    db.collection::<Document>("bits").insert_many(vec![doc! {"_id": 1, "a": 5}, doc! {"_id": 2, "a": 2}]).await.unwrap();
    let n = db.collection::<Document>("bits").count_documents(doc! {"a": {"$bitsAllSet": [0, 2]}}).await.unwrap();
    assert_eq!(n, 1);
    db.drop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_settings_and_stats() {
    let Some(uri) = uri() else { return };
    let client = Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("m3_qs");
    let admin = client.database("admin");
    let _ = db.drop().await;
    db.collection::<Document>("c").insert_many((0..20).map(|i| doc! {"_id": i, "a": i % 4, "b": i})).await.unwrap();
    // reject, with 8.1's comment.
    let r = run(&admin, doc! {"setQuerySettings": {"find": "c", "filter": {"a": 1}, "$db": "m3_qs"}, "settings": {"reject": true, "comment": "too slow"}}).await.unwrap();
    let hash = r.get_str("queryShapeHash").unwrap().to_owned();
    assert_eq!(hash.len(), 64);
    // The same shape with another value is rejected; another shape isn't.
    let e = db.collection::<Document>("c").find(doc! {"a": 3}).await.unwrap_err();
    assert_eq!(code_of(&e), 411);
    db.collection::<Document>("c").find(doc! {"a": "x"}).await.unwrap();
    db.collection::<Document>("c").find(doc! {"b": 3}).await.unwrap();
    // $querySettings lists it.
    let list: Vec<Document> = admin.aggregate(vec![doc! {"$querySettings": {"showDebugQueryShape": true}}]).await.unwrap().try_collect().await.unwrap();
    let e = list.iter().find(|d| d.get_str("queryShapeHash").ok() == Some(hash.as_str())).unwrap();
    assert_eq!(e.get_document("settings").unwrap().get_str("comment").unwrap(), "too slow");
    assert!(e.contains_key("debugQueryShape"));
    // Updating by hash keeps the rest; reject false + maxTimeMS (9.0).
    run(&admin, doc! {"setQuerySettings": hash.clone(), "settings": {"reject": false, "maxTimeMS": 5000}}).await.unwrap();
    let got: Vec<Document> = db.collection::<Document>("c").find(doc! {"a": 1}).await.unwrap().try_collect().await.unwrap();
    assert_eq!(got.len(), 5);
    // 9.0's per-command querySettings.
    let e = run(&db, doc! {"find": "c", "filter": {"b": 1}, "querySettings": {"reject": true}}).await.unwrap_err();
    assert_eq!(code_of(&e), 411);
    let e = run(&db, doc! {"distinct": "c", "key": "a", "querySettings": {"reject": true}}).await.unwrap_err();
    assert_eq!(code_of(&e), 411);
    // remove.
    run(&admin, doc! {"removeQuerySettings": hash.clone()}).await.unwrap();
    let list: Vec<Document> = admin.aggregate(vec![doc! {"$querySettings": {}}]).await.unwrap().try_collect().await.unwrap();
    assert!(!list.iter().any(|d| d.get_str("queryShapeHash").ok() == Some(hash.as_str())));
    // Errors.
    let e = run(&admin, doc! {"setQuerySettings": {"find": "c", "filter": {"a": 1}, "$db": "m3_qs"}, "settings": {}}).await.unwrap_err();
    assert_eq!(code_of(&e), 8_727_502);
    let e = run(&admin, doc! {"setQuerySettings": {"find": "c", "filter": {"a": 1}, "$db": "m3_qs"}, "settings": {"reject": false}}).await.unwrap_err();
    assert_eq!(code_of(&e), 7_746_604);
    // $queryStats: the finds above, and writes (9.0).
    for i in 0..3 {
        let _: Vec<Document> = db.collection::<Document>("c").find(doc! {"b": {"$gt": i}}).sort(doc! {"b": 1}).await.unwrap().try_collect().await.unwrap();
    }
    db.collection::<Document>("c").update_many(doc! {"a": 2}, doc! {"$set": {"z": 1}}).await.unwrap();
    let stats: Vec<Document> = admin.aggregate(vec![doc! {"$queryStats": {}}]).await.unwrap().try_collect().await.unwrap();
    let find = stats
        .iter()
        .find(|s| {
            let q = s.get_document("key").unwrap().get_document("queryShape").unwrap();
            q.get_str("command").ok() == Some("find")
                && q.get_document("filter").ok() == Some(&doc! {"b": {"$gt": "?number"}})
                && q.get_document("cmdNs").unwrap().get_str("db").ok() == Some("m3_qs")
        })
        .expect("the find's shape");
    let m = find.get_document("metrics").unwrap();
    assert_eq!(m.get_i64("execCount").unwrap(), 3);
    assert_eq!(m.get_document("docsReturned").unwrap().get_i64("sum").unwrap(), 19 + 18 + 17);
    let upd = stats
        .iter()
        .find(|s| s.get_document("key").unwrap().get_document("queryShape").unwrap().get_str("command").ok() == Some("update"))
        .expect("the update's shape");
    let w = upd.get_document("metrics").unwrap().get_document("writes").unwrap();
    assert_eq!(w.get_document("nModified").unwrap().get_i64("sum").unwrap(), 5);
    eprintln!("$queryStats: {} shapes", stats.len());
    db.drop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limits() {
    let Some(uri) = uri() else { return };
    if std::env::var("SPRIG_TEST_M3_LIMITS").ok().as_deref() != Some("1") {
        eprintln!("SPRIG_TEST_M3_LIMITS not set: skipped");
        return;
    }
    let client = Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("m3_limits");
    let _ = db.drop().await;
    db.collection::<Document>("c").insert_many((0..50).map(|i| doc! {"_id": i, "pad": "x".repeat(4000)})).await.unwrap();
    // 146: a $group holding more than 100,000 bytes.
    let e = db.collection::<Document>("c").aggregate(vec![doc! {"$group": {"_id": null, "all": {"$push": "$pad"}}}]).await.unwrap_err();
    assert_eq!(code_of(&e), 146, "{e}");
    // 509: a third open transaction.
    let mut sessions = Vec::new();
    for i in 0..2 {
        let mut s = client.start_session().await.unwrap();
        s.start_transaction().await.unwrap();
        db.collection::<Document>("c").insert_one(doc! {"_id": 1000 + i}).session(&mut s).await.unwrap();
        sessions.push(s);
    }
    let mut s3 = client.start_session().await.unwrap();
    s3.start_transaction().await.unwrap();
    let e = db.collection::<Document>("c").insert_one(doc! {"_id": 2000}).session(&mut s3).await.unwrap_err();
    assert_eq!(code_of(&e), 509, "{e}");
    for mut s in sessions {
        s.abort_transaction().await.unwrap();
    }
    db.drop().await.unwrap();
}
