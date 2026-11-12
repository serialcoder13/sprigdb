//! The aggregation-expression differential test (M1, 12 Nov 2026).
//!
//! `tests/expr_diff/cases.json` holds expression cases and the answers
//! MongoDB 8.0.34 gave for them (recorded by `tests/expr_diff/gen_cases.js`
//! on a private reference server). This test evaluates every case with
//! SprigDB's in-gateway evaluator, as the engine runs it
//! (`$replaceWith: {r: <expr>}` on the case's document, or `$group`
//! sorted by `_id`), and compares: values with their exact BSON types
//! (doubles bit for bit), errors by code.
//!
//! Cases listed in `tests/expr_diff/known.txt` are known differences and
//! are reported but not failed. `SPRIG_EXPR_DIFF_VERBOSE=1` prints every
//! mismatch, error messages included; `SPRIG_EXPR_DIFF_ONLY=<prefix>`
//! runs only the cases whose names start with the prefix.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines
)]

use bson::{Bson, DateTime, Document, doc};
use sprig_doc::agg::{self, AggLimits};

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

fn run(case: &Document) -> Result<Vec<Document>, (i32, String)> {
    let lim = AggLimits {
        stage_bytes: 100 << 20,
        now: DateTime::now(),
    };
    let wrap = |r: Result<Vec<Document>, sprig_proto_mongo::errors::MongoError>| {
        r.map_err(|e| (e.code, e.message.clone()))
    };
    if let Ok(g) = case.get_document("group") {
        let docs: Vec<Document> = case
            .get_array("docs")
            .unwrap()
            .iter()
            .map(|d| d.as_document().unwrap().clone())
            .collect();
        let mut out = wrap(agg::run_stage(docs, &doc! {"$group": g.clone()}, &lim))?;
        agg::sort_docs(&mut out, &vec![("_id".to_owned(), 1)]);
        Ok(out)
    } else {
        let d = case.get_document("doc").unwrap().clone();
        let e = case.get("expr").unwrap().clone();
        wrap(agg::run_stage(
            vec![d],
            &doc! {"$replaceWith": {"r": e}},
            &lim,
        ))
    }
}

#[test]
fn expressions_match_mongodb_8_0() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/expr_diff");
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
    let verbose = std::env::var("SPRIG_EXPR_DIFF_VERBOSE").is_ok();
    let only = std::env::var("SPRIG_EXPR_DIFF_ONLY").unwrap_or_default();
    let (mut total, mut matched, mut msg_diff, mut known_hit) = (0, 0, 0, 0);
    let mut failures = Vec::new();
    for c in cases {
        let Bson::Document(c) = c else { continue };
        let name = c.get_str("name").unwrap().to_owned();
        if !name.starts_with(&only) {
            continue;
        }
        total += 1;
        let expect = c.get_document("expect").unwrap();
        let got = std::panic::catch_unwind(|| run(&c));
        let (ok, detail) = match (&got, expect.get("ok")) {
            (Err(_), _) => (false, "panicked".to_owned()),
            (Ok(Ok(docs)), Some(Bson::Array(want))) => {
                let got = Bson::Array(docs.iter().cloned().map(Bson::Document).collect());
                let want = Bson::Array(want.clone());
                (same(&got, &want), format!("want {want}\n      got  {got}"))
            }
            (Ok(Err((code, msg))), None) => {
                let want_code = expect.get_i32("code").unwrap_or(-1);
                // MongoDB prefixes errors raised while optimizing or
                // running the pipeline; the engine's own messages don't.
                let mut want_msg = expect.get_str("errmsg").unwrap_or("");
                for p in [
                    "Failed to optimize pipeline :: caused by :: ",
                    "PlanExecutor error during aggregation :: caused by :: ",
                    "Invalid $project :: caused by :: ",
                ] {
                    want_msg = want_msg.strip_prefix(p).unwrap_or(want_msg);
                }
                if *code == want_code && msg != want_msg {
                    msg_diff += 1;
                    if verbose {
                        eprintln!("MSG {name}\n      want {want_msg}\n      got  {msg}");
                    }
                }
                (
                    *code == want_code,
                    format!("want error {want_code} {want_msg}\n      got  error {code} {msg}"),
                )
            }
            (Ok(Ok(docs)), _) => (
                false,
                format!(
                    "want error {} {}\n      got  {:?}",
                    expect.get_i32("code").unwrap_or(-1),
                    expect.get_str("errmsg").unwrap_or(""),
                    docs
                ),
            ),
            (Ok(Err((code, msg))), _) => (
                false,
                format!(
                    "want {}\n      got  error {code} {msg}",
                    expect.get("ok").unwrap()
                ),
            ),
        };
        if ok {
            matched += 1;
            continue;
        }
        let expr = c
            .get("expr")
            .or_else(|| c.get("group"))
            .cloned()
            .unwrap_or(Bson::Null);
        if known.contains(&name) {
            known_hit += 1;
            if verbose {
                eprintln!("KNOWN {name} {expr}\n      {detail}");
            }
            continue;
        }
        if verbose {
            eprintln!("DIFF {name} {expr}\n      {detail}");
        }
        failures.push(name);
    }
    eprintln!(
        "expression differential: {total} cases, {matched} match, {known_hit} known differences, \
         {} unexpected; {msg_diff} with the same code but another message",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} unexpected differences, first: {:?}",
        failures.len(),
        &failures[..failures.len().min(20)]
    );
}
