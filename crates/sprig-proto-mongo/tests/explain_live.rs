//! MongoDB's `explain` of searches, through a running gateway's MongoDB listener.
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
//! - `explain` of `findAndModify`: refused (code 39662, unsupported in
//!   this release), and a bad verbosity (9).
//!
//! `explain_of_reads_and_writes` checks the other commands on
//! database `r5eight_explain`: a point read is `EXPRESS_IXSCAN`, a filter
//! on an indexed field `IXSCAN` under `FETCH`, an unindexed one `COLLSCAN`;
//! `count`, `distinct`, `aggregate` (in the `stages` form), `update` and
//! `delete` (which write nothing); MariaDB's plan of each statement under
//! `queryPlanner.sprig`, without physical schema names; `executionStats`'
//! counts; `EOF` for a missing collection.
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

/// A physical schema name: `t`, six or more digits, `_`.
fn has_physical(s: &str) -> bool {
    let b = s.as_bytes();
    (0..b.len()).any(|i| {
        b[i] == b't' && (i == 0 || !b[i - 1].is_ascii_alphanumeric()) && {
            let d = b[i + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
            d >= 6 && b.get(i + 1 + d) == Some(&b'_')
        }
    })
}

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

    // A command explain doesn't cover (explain covers find, aggregate,
    // count, distinct, update and delete); a bad verbosity.
    let e = db
        .run_command(
            doc! {"explain": {"findAndModify": "p", "query": {"name": "x"}, "remove": true}},
        )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_of_reads_and_writes() {
    let Some(u) = std::env::var("SPRIG_TEST_MONGO_URI")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let client = Client::with_uri_str(&u).await.unwrap();
    let db = client.database("r5eight_explain");
    db.drop().await.unwrap();
    let o = db.collection::<Document>("o");
    let docs: Vec<Document> = (1..=40)
        .map(|i| doc! {"_id": i, "sku": format!("s{}", i % 7), "qty": i, "tag": if i % 2 == 0 { "even" } else { "odd" }})
        .collect();
    o.insert_many(docs).await.unwrap();
    db.run_command(doc! {"createIndexes": "o", "indexes": [{"key": {"sku": 1}, "name": "sku_1"}]})
        .await
        .unwrap();
    let ex = |cmd: Document, verbosity: &'static str| {
        let db = db.clone();
        async move {
            db.run_command(doc! {"explain": cmd, "verbosity": verbosity})
                .await
                .unwrap()
        }
    };
    let qp = |r: &Document| r.get_document("queryPlanner").unwrap().clone();
    let wp = |r: &Document| qp(r).get_document("winningPlan").unwrap().clone();
    let stmts = |r: &Document| {
        qp(r)
            .get_document("sprig")
            .unwrap()
            .get_array("statements")
            .unwrap()
            .clone()
    };

    // A point read.
    let r = ex(doc! {"find": "o", "filter": {"_id": 5}}, "executionStats").await;
    assert_eq!(wp(&r).get_str("stage").unwrap(), "EXPRESS_IXSCAN", "{r}");
    assert_eq!(
        r.get_document("executionStats")
            .unwrap()
            .get_i64("nReturned")
            .unwrap(),
        1
    );
    let s = stmts(&r);
    let first = s[0].as_document().unwrap();
    let sql = first.get_str("sql").unwrap();
    assert!(
        sql.contains("`r5eight_explain`.`o`") && sql.contains("_id = X'"),
        "{sql}"
    );
    let plan = first.get_document("mariadb").unwrap().to_string();
    assert!(plan.contains("query_block"), "{plan}");
    assert!(!has_physical(&plan), "a physical name: {plan}");
    println!("point: {}", wp(&r));

    // An indexed field, with sort, skip, limit and projection, queryPlanner.
    let r = ex(
        doc! {"find": "o", "filter": {"sku": "s3"}, "sort": {"qty": -1}, "skip": 1, "limit": 2, "projection": {"qty": 1}},
        "queryPlanner",
    )
    .await;
    let mut w = wp(&r);
    let mut seen = Vec::new();
    loop {
        seen.push(w.get_str("stage").unwrap().to_owned());
        match w.get_document("inputStage") {
            Ok(i) => w = i.clone(),
            Err(_) => break,
        }
    }
    assert_eq!(
        seen.first().map(String::as_str),
        Some("PROJECTION_DEFAULT"),
        "{seen:?}"
    );
    assert!(
        seen.contains(&"SORT".to_owned())
            && seen.contains(&"LIMIT".to_owned())
            && seen.contains(&"SKIP".to_owned()),
        "{seen:?}"
    );
    assert!(!r.contains_key("executionStats"));
    println!("indexed find: {seen:?} {}", wp(&r));

    // An unindexed field: a scan, every document examined.
    let r = ex(
        doc! {"find": "o", "filter": {"tag": "even"}},
        "executionStats",
    )
    .await;
    let st = r.get_document("executionStats").unwrap();
    assert_eq!(st.get_i64("nReturned").unwrap(), 20, "{r}");
    assert_eq!(wp(&r).get_str("stage").unwrap(), "COLLSCAN", "{r}");
    for x in stmts(&r) {
        let t = x.as_document().unwrap().to_string();
        assert!(!has_physical(&t), "a physical name: {t}");
    }
    println!(
        "scan: {} examined {}",
        wp(&r),
        st.get_i64("totalDocsExamined").unwrap()
    );

    // count and distinct.
    let r = ex(
        doc! {"count": "o", "query": {"tag": "odd"}},
        "executionStats",
    )
    .await;
    assert_eq!(wp(&r).get_str("stage").unwrap(), "COUNT");
    assert_eq!(
        r.get_document("executionStats")
            .unwrap()
            .get_i64("nReturned")
            .unwrap(),
        20
    );
    let r = ex(
        doc! {"distinct": "o", "key": "sku", "query": {}},
        "executionStats",
    )
    .await;
    assert_eq!(
        r.get_document("executionStats")
            .unwrap()
            .get_i64("nReturned")
            .unwrap(),
        7
    );

    // aggregate: the stages form.
    let r = ex(
        doc! {"aggregate": "o", "cursor": {}, "pipeline": [{"$match": {"tag": "odd"}}, {"$group": {"_id": "$sku", "n": {"$sum": 1}}}]},
        "executionStats",
    )
    .await;
    let stages = r.get_array("stages").unwrap();
    let cursor = stages[0]
        .as_document()
        .unwrap()
        .get_document("$cursor")
        .unwrap();
    assert!(
        cursor.contains_key("queryPlanner") && cursor.contains_key("executionStats"),
        "{r}"
    );
    assert!(
        stages[1].as_document().unwrap().contains_key("$group"),
        "{r}"
    );
    let r = db
        .run_command(doc! {"aggregate": "o", "pipeline": [{"$match": {"qty": {"$gt": 30}}}], "explain": true})
        .await
        .unwrap();
    assert!(r.get_array("stages").is_ok(), "{r}");

    // update and delete: planned, nothing written.
    let r = ex(
        doc! {"update": "o", "updates": [{"q": {"_id": 7}, "u": {"$set": {"qty": 700}}}]},
        "executionStats",
    )
    .await;
    assert_eq!(wp(&r).get_str("stage").unwrap(), "UPDATE");
    assert_eq!(
        r.get_document("executionStats")
            .unwrap()
            .get_i64("nMatched")
            .unwrap(),
        1
    );
    assert!(
        stmts(&r).iter().any(|s| s
            .as_document()
            .unwrap()
            .get_str("sql")
            .unwrap()
            .starts_with("UPDATE")),
        "{r}"
    );
    let r = ex(
        doc! {"delete": "o", "deletes": [{"q": {"tag": "odd"}, "limit": 0}]},
        "executionStats",
    )
    .await;
    assert_eq!(wp(&r).get_str("stage").unwrap(), "DELETE");
    assert_eq!(
        r.get_document("executionStats")
            .unwrap()
            .get_i64("nWouldDelete")
            .unwrap(),
        20
    );
    assert_eq!(
        o.count_documents(doc! {}).await.unwrap(),
        40,
        "explain wrote"
    );
    assert_eq!(
        o.find_one(doc! {"_id": 7})
            .await
            .unwrap()
            .unwrap()
            .get_i32("qty")
            .unwrap(),
        7,
        "explain wrote"
    );

    // A missing collection.
    let r = ex(doc! {"find": "nope", "filter": {}}, "queryPlanner").await;
    assert_eq!(wp(&r).get_str("stage").unwrap(), "EOF");
    println!("explain: point, indexed, scan, count, distinct, aggregate, update, delete, EOF");
    db.drop().await.unwrap();
}
