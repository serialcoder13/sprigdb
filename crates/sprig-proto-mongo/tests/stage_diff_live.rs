//! The M2 stage corpus through a live gateway (12 Nov 2026): every case
//! of `crates/sprig-doc/tests/stage_diff/cases.json` (recorded from
//! MongoDB 8.0.34) is run through the MongoDB driver as
//! `aggregate([{$documents: docs}, ...pipeline])` (`$graphLookup` reads
//! collections filled first), and compared with the recorded answer:
//! documents with their exact BSON, errors by code. The diagnostic stages
//! are checked for MongoDB's shape against the reference server when
//! `SPRIG_TEST_MONGO_REF_URI` is set.
//!
//! - `SPRIG_TEST_MONGO_URI`: a gateway's MongoDB listener with a tenant
//!   login.
//! - `SPRIG_TEST_MONGO_REF_URI`: a private MongoDB 8.0 (optional).
//!
//! Skipped without `SPRIG_TEST_MONGO_URI`. Uses the database
//! `m2_stage_live` and drops it at the end. Known differences are listed
//! in `crates/sprig-doc/tests/stage_diff/known.txt`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::print_stderr
)]

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Database};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
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

fn docs_of(v: Option<&Bson>) -> Vec<Document> {
    match v {
        Some(Bson::Array(a)) => a.iter().filter_map(|d| d.as_document().cloned()).collect(),
        _ => Vec::new(),
    }
}

async fn run(db: &Database, c: &Document) -> Result<Vec<Document>, i32> {
    let _ = db.drop().await;
    if let Ok(colls) = c.get_document("colls") {
        for (k, v) in colls {
            db.collection::<Document>(k)
                .insert_many(docs_of(Some(v)))
                .await
                .map_err(|e| code_of(&e))?;
        }
    }
    let mut pipeline = docs_of(c.get("pipeline"));
    if !c.get_bool("db").unwrap_or(false) {
        pipeline.insert(0, doc! {"$documents": docs_of(c.get("docs"))});
    }
    let cur = db.aggregate(pipeline).await.map_err(|e| code_of(&e))?;
    cur.try_collect().await.map_err(|e| code_of(&e))
}

#[tokio::test]
async fn stage_corpus_through_the_gateway() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let client = Client::with_uri_str(&u).await.unwrap();
    let db = client.database("m2_stage_live");
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../sprig-doc/tests/stage_diff");
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
    let only = std::env::var("SPRIG_STAGE_DIFF_ONLY").unwrap_or_default();
    let (mut total, mut matched, mut known_hit) = (0, 0, 0);
    let mut fails = Vec::new();
    for c in cases {
        let Bson::Document(c) = c else { continue };
        let name = c.get_str("name").unwrap().to_owned();
        if !name.starts_with(&only) {
            continue;
        }
        total += 1;
        let expect = c.get_document("expect").unwrap();
        let got = run(&db, &c).await;
        let ok = match (&got, expect.get("ok")) {
            (Ok(d), Some(Bson::Array(w))) => same(
                &Bson::Array(d.iter().cloned().map(Bson::Document).collect()),
                &Bson::Array(w.clone()),
            ),
            (Err(code), None) => *code == expect.get_i32("code").unwrap_or(-2),
            _ => false,
        };
        if ok {
            matched += 1;
        } else if known.contains(&name) {
            known_hit += 1;
        } else {
            fails.push(format!(
                "{name}\n  want {expect}\n  got  {}",
                match &got {
                    Ok(d) => format!("{d:?}"),
                    Err(c) => format!("error {c}"),
                }
            ));
        }
    }
    let _ = db.drop().await;
    eprintln!(
        "stage corpus through the gateway: {total} cases, {matched} match, {known_hit} known differences, {} unexpected",
        fails.len()
    );
    assert!(fails.is_empty(), "{}", fails.join("\n"));
}

/// A stage's name and its first document's field names (or error code).
type Shape = (String, Result<Vec<String>, i32>);

/// The top-level field names of a stage's first output document.
async fn first_keys(
    db: &Database,
    coll: Option<&str>,
    stage: Document,
) -> Result<Vec<String>, i32> {
    let docs: Vec<Document> = match coll {
        Some(c) => db
            .collection::<Document>(c)
            .aggregate(vec![stage])
            .await
            .map_err(|e| code_of(&e))?
            .try_collect()
            .await
            .map_err(|e| code_of(&e))?,
        None => db
            .aggregate(vec![stage])
            .await
            .map_err(|e| code_of(&e))?
            .try_collect()
            .await
            .map_err(|e| code_of(&e))?,
    };
    Ok(docs
        .first()
        .map(|d| d.keys().cloned().collect())
        .unwrap_or_default())
}

#[tokio::test]
async fn diagnostic_stages_have_mongodb_shapes() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let mut targets = vec![("sprig", Client::with_uri_str(&u).await.unwrap())];
    if let Some(r) = uri("SPRIG_TEST_MONGO_REF_URI") {
        targets.push(("mongo", Client::with_uri_str(&r).await.unwrap()));
    }
    let mut shapes: Vec<Vec<Shape>> = Vec::new();
    for (who, client) in &targets {
        let db = client.database("m2_diag_live");
        let _ = db.drop().await;
        let c = db.collection::<Document>("items");
        c.insert_many(
            (0..20)
                .map(|i| doc! {"_id": i, "k": i % 3})
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        c.create_index(mongodb::IndexModel::builder().keys(doc! {"k": 1}).build())
            .await
            .unwrap();
        let admin = client.database("admin");
        let config = client.database("config");
        let mut s = Vec::new();
        let mut one = |name: &str, r: Result<Vec<String>, i32>| s.push((name.to_owned(), r));
        one(
            "collStats",
            first_keys(&db, Some("items"), doc! {"$collStats": {"storageStats": {}, "count": {}, "latencyStats": {}, "queryExecStats": {}}}).await,
        );
        one(
            "indexStats",
            first_keys(&db, Some("items"), doc! {"$indexStats": {}}).await,
        );
        one(
            "planCacheStats",
            first_keys(&db, Some("items"), doc! {"$planCacheStats": {}}).await,
        );
        one(
            "currentOp",
            first_keys(&admin, None, doc! {"$currentOp": {}}).await,
        );
        one(
            "currentOp.idle",
            first_keys(&admin, None, doc! {"$currentOp": {"idleConnections": true}}).await,
        );
        one(
            "currentOp.notAdmin",
            first_keys(&db, None, doc! {"$currentOp": {}}).await,
        );
        one(
            "currentOp.bad",
            first_keys(&admin, None, doc! {"$currentOp": {"foo": 1}}).await,
        );
        one(
            "listSessions.wrongNs",
            first_keys(&db, Some("items"), doc! {"$listSessions": {}}).await,
        );
        let _ = config;
        one(
            "collStats.notFirst",
            db.collection::<Document>("items")
                .aggregate(vec![doc! {"$match": {}}, doc! {"$collStats": {}}])
                .await
                .map(|_| Vec::new())
                .map_err(|e| code_of(&e)),
        );
        one(
            "indexStats.dbLevel",
            first_keys(&db, None, doc! {"$indexStats": {}}).await,
        );
        db.drop().await.unwrap();
        eprintln!("{who}: {s:?}");
        shapes.push(s);
    }
    // The fields SprigDB promises (chapter 30) are present.
    let sprig = &shapes[0];
    let has = |name: &str, fields: &[&str]| {
        let keys = sprig
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, r)| r.as_ref().ok())
            .unwrap_or_else(|| panic!("{name} failed: {sprig:?}"));
        for f in fields {
            assert!(keys.iter().any(|k| k == f), "{name} lacks {f}: {keys:?}");
        }
    };
    has(
        "collStats",
        &[
            "ns",
            "host",
            "localTime",
            "latencyStats",
            "storageStats",
            "count",
            "queryExecStats",
        ],
    );
    has("indexStats", &["name", "key", "host", "accesses", "spec"]);
    has(
        "currentOp",
        &[
            "type",
            "host",
            "desc",
            "connectionId",
            "client",
            "active",
            "opid",
            "op",
            "ns",
            "command",
        ],
    );
    // Errors match the reference's codes where it is given.
    if let Some(m) = shapes.get(1) {
        for ((n, a), (_, b)) in sprig.iter().zip(m) {
            if let (Err(x), Err(y)) = (a, b) {
                assert_eq!(x, y, "{n}: error codes differ");
            }
            assert_eq!(a.is_ok(), b.is_ok(), "{n}: {a:?} vs MongoDB {b:?}");
            if let (Ok(x), Ok(y)) = (a, b) {
                let missing: Vec<&String> = y.iter().filter(|k| !x.contains(k)).collect();
                eprintln!("{n}: fields MongoDB has that SprigDB doesn't: {missing:?}");
            }
        }
    }
}
