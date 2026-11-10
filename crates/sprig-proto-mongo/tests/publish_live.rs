//! The publication wait across two gateways (chapter 31, `check_flags`;
//! `decisions/r5-interim.md` D13): once a write holding a new array path
//! is acknowledged, a query on that path through *another* gateway finds
//! the document. Without the wait, that gateway may still plan with the
//! old path registry (the path scalar-safe, an exact SQL translation that
//! skips arrays) for as long as its directory watch takes to deliver the
//! new catalog version.
//!
//! - `SPRIG_TEST_MONGO_URI`: gateway 1, as in `driver_live.rs`.
//! - `SPRIG_TEST_MONGO_URI_2`: gateway 2 of the same deployment.
//!
//! Skipped unless both are set. Uses the database `r5mongo3_publish` and
//! drops it at the end.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::doc_markdown)]

use std::time::Instant;

use futures_util::TryStreamExt;
use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

async fn client(u: &str, app: &str) -> Client {
    let mut o = ClientOptions::parse(u).await.unwrap();
    o.app_name = Some(format!("{app}~dc-a"));
    Client::with_options(o).unwrap()
}

async fn ids(c: &Collection<Document>, f: Document) -> Vec<i32> {
    let v: Vec<Document> = c.find(f).await.unwrap().try_collect().await.unwrap();
    let mut out: Vec<i32> = v.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    out.sort_unstable();
    out
}

#[tokio::test]
async fn new_array_paths_are_seen_by_every_gateway() {
    let (Some(u1), Some(u2)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_URI_2")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_URI_2 not set: skipped");
        return;
    };
    let rounds: usize = std::env::var("SPRIG_TEST_PUBLISH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let a = client(&u1, "r5mongo3-publish-a").await;
    let b = client(&u2, "r5mongo3-publish-b").await;
    let db_a = a.database("r5mongo3_publish");
    let _ = db_a.drop().await;
    let ca = db_a.collection::<Document>("p");
    let cb = b.database("r5mongo3_publish").collection::<Document>("p");
    // Each round: path f<i> holds a scalar in document 0; gateway B plans
    // a query on it (so its snapshot is current and the path scalar-safe);
    // gateway A inserts a document holding an array at f<i>; B queries
    // for an element of that array at once.
    let mut misses = Vec::new();
    let mut waits = Vec::new();
    for i in 0..rounds {
        let f = format!("f{i}");
        ca.insert_one(doc! {"_id": 10_000 + i32::try_from(i).unwrap(), &f: "s"})
            .await
            .unwrap();
        assert_eq!(
            ids(&cb, doc! {&f: "y"}).await,
            Vec::<i32>::new(),
            "round {i}: nothing holds y yet"
        );
        let t = Instant::now();
        ca.insert_one(doc! {"_id": i32::try_from(i).unwrap(), &f: ["x", "y"]})
            .await
            .unwrap();
        waits.push(t.elapsed().as_secs_f64() * 1000.0);
        let seen = ids(&cb, doc! {&f: "y"}).await;
        if seen != vec![i32::try_from(i).unwrap()] {
            misses.push((i, seen));
        }
    }
    waits.sort_by(f64::total_cmp);
    eprintln!(
        "{rounds} rounds: {} misses on gateway B; insert with a new array path p50 {:.1} ms, max {:.1} ms",
        misses.len(),
        waits[waits.len() / 2],
        waits[waits.len() - 1]
    );
    assert!(misses.is_empty(), "gateway B missed: {misses:?}");
    db_a.drop().await.unwrap();
}
