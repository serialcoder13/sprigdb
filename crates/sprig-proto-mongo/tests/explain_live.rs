//! MongoDB's `explain` of searches (chapter 39, build step 14; decision
//! S25), through a running gateway's MongoDB listener.
//!
//! ```sh
//! export SPRIG_TEST_MONGO_URI='mongodb://acme.app:sprig-dev-app@127.0.0.1:26737/?authSource=admin&directConnection=true'
//! cargo test -p sprig-proto-mongo --test explain_live -- --nocapture
//! ```
//!
//! Database `r5seven_explain` gets a collection with a text index and a
//! search index; the test checks:
//!
//! - `explain` of `find` with `$text` (executionStats): stage
//!   `TEXT_MATCH`, the index and its copies, the parsed query, the copy's
//!   hit count and `nReturned` (the documents the caller gets);
//! - `aggregate` with `explain: true` on `$search`: `queryPlanner` only
//!   (no hit count, nothing run);
//! - `$searchMeta` under `executionStats`;
//! - `explain` of a command without a search: refused (code 39662,
//!   unsupported in this release), and a bad verbosity (9).
//!
//! The database is dropped at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use mongodb::Client;
use mongodb::bson::{Document, doc};

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_of_searches() {
    let Some(u) = std::env::var("SPRIG_TEST_MONGO_URI")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let client = Client::with_uri_str(&u).await.unwrap();
    let db = client.database("r5seven_explain");
    db.drop().await.unwrap();
    let p = db.collection::<Document>("p");
    p.insert_many(vec![
        doc! {"_id": 1, "name": "red shoe", "brand": "acme"},
        doc! {"_id": 2, "name": "blue shoe", "brand": "zoom"},
        doc! {"_id": 3, "name": "red hat", "brand": "acme"},
        doc! {"_id": 4, "name": "green shoe", "brand": "tiny"},
    ])
    .await
    .unwrap();
    db.run_command(
        doc! {"createIndexes": "p", "indexes": [{"key": {"name": "text"}, "name": "name_text"}]},
    )
    .await
    .unwrap();
    db.run_command(
        doc! {"createSearchIndexes": "p", "indexes": [{"name": "default", "definition": {
        "mappings": {"dynamic": false, "fields": {"name": {"type": "string"},
            "brand": [{"type": "token"}, {"type": "stringFacet"}]}}}}]},
    )
    .await
    .unwrap();

    // find with $text, executionStats.
    let r = db
        .run_command(doc! {
            "explain": {"find": "p", "filter": {"$text": {"$search": "shoe"}, "brand": {"$ne": "tiny"}}},
            "verbosity": "executionStats",
        })
        .await
        .unwrap();
    let qp = r.get_document("queryPlanner").unwrap();
    assert_eq!(qp.get_str("namespace").unwrap(), "r5seven_explain.p");
    let wp = qp.get_document("winningPlan").unwrap();
    assert_eq!(wp.get_str("stage").unwrap(), "TEXT_MATCH");
    let s = wp.get_document("sprigSearch").unwrap();
    assert_eq!(s.get_str("index").unwrap(), "r5seven_explain.name_text");
    assert_eq!(s.get_str("flavor").unwrap(), "mongo_text");
    assert_eq!(s.get_str("consistency").unwrap(), "latest");
    assert!(s.get_document("query").is_ok(), "{s}");
    assert!(!s.get_str("copies").unwrap().is_empty());
    // Three documents say "shoe"; the rest of the filter keeps two.
    let hits = s
        .get_i64("hits")
        .or_else(|_| s.get_i32("hits").map(i64::from))
        .unwrap();
    assert_eq!(hits, 3, "{s}");
    let es = r.get_document("executionStats").unwrap();
    assert_eq!(es.get_i64("nReturned").unwrap(), 2, "{es}");
    println!("explain find $text: {s}");

    // aggregate with explain: true → queryPlanner only.
    let r = db
        .run_command(doc! {
            "aggregate": "p",
            "pipeline": [{"$search": {"index": "default", "consistency": "latest", "text": {"query": "red", "path": "name"}}}],
            "explain": true,
        })
        .await
        .unwrap();
    let wp = r
        .get_document("queryPlanner")
        .unwrap()
        .get_document("winningPlan")
        .unwrap();
    assert_eq!(wp.get_str("stage").unwrap(), "SEARCH");
    let s = wp.get_document("sprigSearch").unwrap();
    assert_eq!(s.get_str("flavor").unwrap(), "mongo_search");
    assert!(!s.contains_key("hits"), "queryPlanner runs nothing: {s}");
    assert!(!r.contains_key("executionStats"));

    // $searchMeta, executionStats.
    let r = db
        .run_command(doc! {
            "explain": {"aggregate": "p", "cursor": {}, "pipeline": [{"$searchMeta": {"index": "default", "consistency": "latest",
                "facet": {"operator": {"text": {"query": "shoe", "path": "name"}},
                          "facets": {"b": {"type": "string", "path": "brand"}}}}}]},
            "verbosity": "executionStats",
        })
        .await
        .unwrap();
    let wp = r
        .get_document("queryPlanner")
        .unwrap()
        .get_document("winningPlan")
        .unwrap();
    assert_eq!(wp.get_str("stage").unwrap(), "SEARCH_META");
    assert_eq!(
        r.get_document("executionStats")
            .unwrap()
            .get_i64("nReturned")
            .unwrap(),
        1
    );

    // Not a search; a bad verbosity.
    let e = db
        .run_command(doc! {"explain": {"find": "p", "filter": {"name": "x"}}})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 39662, "{e}");
    let e = db
        .run_command(doc! {"explain": {"find": "p", "filter": {"$text": {"$search": "x"}}}, "verbosity": "loud"})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 9, "{e}");
    println!("explain: $search queryPlanner, $searchMeta executionStats, refusals");
    db.drop().await.unwrap();
}
