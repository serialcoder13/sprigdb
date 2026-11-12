//! The aggregation-expression corpus (M1, 12 Nov 2026) through a SprigDB
//! gateway's MongoDB listener and the MongoDB Rust driver: the live check
//! of `crates/sprig-doc/tests/expr_diff.rs`, which runs the same cases on
//! the engine directly. Answers are compared with the ones MongoDB 8.0.34
//! gave (recorded in `crates/sprig-doc/tests/expr_diff/cases.json`), values
//! with their exact BSON types and errors by code; the known differences
//! of `expr_diff/known.txt` are reported, not failed.
//!
//! - `SPRIG_TEST_MONGO_URI`: a gateway's MongoDB listener with a tenant
//!   login. Skipped when unset. Uses the database `m1_expr` and drops it.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::print_stderr)]

use futures_util::TryStreamExt;
use mongodb::Client;
use mongodb::bson::{Bson, Document, doc};

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
        (Bson::Decimal128(x), Bson::Decimal128(y)) => x.bytes() == y.bytes(),
        _ => a == b,
    }
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expressions_through_the_gateway() {
    let Some(uri) = std::env::var("SPRIG_TEST_MONGO_URI")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../sprig-doc/tests/expr_diff");
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
    let only = std::env::var("SPRIG_EXPR_DIFF_ONLY").unwrap_or_default();
    let db = Client::with_uri_str(&uri)
        .await
        .unwrap()
        .database("m1_expr");
    db.drop().await.unwrap();
    // One collection for the single-document cases, keyed by case number;
    // one per `$group` case.
    let cases: Vec<Document> = cases
        .into_iter()
        .filter_map(|c| match c {
            Bson::Document(d) => Some(d),
            _ => None,
        })
        .filter(|c| c.get_str("name").unwrap().starts_with(&only))
        .collect();
    let single = db.collection::<Document>("single");
    let mut docs = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        if let Ok(d) = c.get_document("doc") {
            let mut d = d.clone();
            d.insert("_id", i32::try_from(i).unwrap());
            docs.push(d);
        }
    }
    for chunk in docs.chunks(500) {
        single.insert_many(chunk.to_vec()).await.unwrap();
    }
    let (mut total, mut matched, mut known_hit) = (0, 0, 0);
    let mut failures = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        let name = c.get_str("name").unwrap().to_owned();
        total += 1;
        let (coll, pipeline) = if let Ok(g) = c.get_document("group") {
            let coll = db.collection::<Document>(&format!("g{i}"));
            let gdocs: Vec<Document> = c
                .get_array("docs")
                .unwrap()
                .iter()
                .map(|d| d.as_document().unwrap().clone())
                .collect();
            coll.insert_many(gdocs).await.unwrap();
            (
                coll,
                vec![doc! {"$group": g.clone()}, doc! {"$sort": {"_id": 1}}],
            )
        } else {
            (
                single.clone(),
                vec![
                    doc! {"$match": {"_id": i32::try_from(i).unwrap()}},
                    doc! {"$replaceWith": {"r": c.get("expr").unwrap().clone()}},
                ],
            )
        };
        let got: Result<Vec<Document>, i32> = match coll.aggregate(pipeline).await {
            Ok(cur) => cur.try_collect().await.map_err(|e| code_of(&e)),
            Err(e) => Err(code_of(&e)),
        };
        let expect = c.get_document("expect").unwrap();
        let ok = match (&got, expect.get("ok")) {
            (Ok(docs), Some(Bson::Array(want))) => {
                // The single-document cases ran on a document whose `_id`
                // is the case number; MongoDB's had the case's own.
                let got = Bson::Array(docs.iter().cloned().map(Bson::Document).collect());
                same(&got, &Bson::Array(want.clone()))
            }
            (Err(code), None) => *code == expect.get_i32("code").unwrap_or(-1),
            _ => false,
        };
        if ok {
            matched += 1;
        } else if known.contains(&name) {
            known_hit += 1;
        } else {
            eprintln!(
                "DIFF {name} {}\n      want {expect}\n      got  {got:?}",
                c.get("expr").or_else(|| c.get("group")).unwrap()
            );
            failures.push(name);
        }
    }
    db.drop().await.unwrap();
    eprintln!(
        "expression differential through the gateway: {total} cases, {matched} match, \
         {known_hit} known differences, {} unexpected",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} unexpected differences, first: {:?}",
        failures.len(),
        &failures[..failures.len().min(20)]
    );
}
