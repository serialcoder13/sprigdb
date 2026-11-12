//! The server-side JavaScript differential test (M4, 12 Nov 2026):
//! `$where`, `$function` and `$accumulator`.
//!
//! `tests/serverjs_diff/cases.json` holds the cases and the answers
//! MongoDB 8.0.34 gave (recorded by `tests/serverjs_diff/gen_cases.js` on
//! a private reference server). This test runs every case on SprigDB's
//! engine with its QuickJS runner (`sprig_logic_js::mongo`, as the
//! gateway installs it): `where` cases parse the filter and match each
//! document, `expr` cases run `$replaceWith: {r: <expr>}`, `group` cases
//! run `$group` sorted by `_id`. Values compare with their exact BSON
//! types (doubles bit for bit), errors by code.
//!
//! Cases listed in `tests/serverjs_diff/known.txt` are known differences
//! and are reported but not failed. `SPRIG_JS_DIFF_VERBOSE=1` prints every
//! mismatch.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr)]

use std::sync::Arc;
use std::time::Duration;

use bson::{Bson, DateTime, Document, doc};
use sprig_doc::agg::{self, AggLimits};
use sprig_doc::serverjs::{self, JsCall, JsFailure, Runner, Scope};
use sprig_proto_mongo::errors::MongoError;

struct Qjs;

impl Runner for Qjs {
    fn call(&self, c: &JsCall<'_>) -> Result<Bson, JsFailure> {
        sprig_logic_js::mongo::call(&sprig_logic_js::mongo::Call {
            op: c.op,
            src: c.src,
            this: c.this,
            args: c.args,
            as_bool: c.as_bool,
            deadline: c.deadline,
            memory_bytes: c.memory_bytes,
        })
        .map_err(|f| match f {
            sprig_logic_js::mongo::Failure::Interrupted => JsFailure::Interrupted,
            sprig_logic_js::mongo::Failure::Error(c, m) => JsFailure::Error(c, m),
        })
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
        (Bson::Decimal128(x), Bson::Decimal128(y)) => x.bytes() == y.bytes(),
        _ => a == b,
    }
}

fn docs_of(case: &Document) -> Vec<Document> {
    case.get_array("docs")
        .unwrap()
        .iter()
        .map(|d| d.as_document().unwrap().clone())
        .collect()
}

/// One case on SprigDB: the answer as `Bson` (an array), or the error.
fn run(case: &Document) -> Result<Bson, MongoError> {
    let lim = AggLimits {
        stage_bytes: 100 << 20,
        now: DateTime::now(),
    };
    let scope = Scope::new(Arc::new(Qjs), Duration::from_secs(5), 64 << 20);
    let r = serverjs::sync_scope(Some(Arc::clone(&scope)), || -> Result<Bson, MongoError> {
        match case.get_str("kind").unwrap() {
            "where" => {
                let pred = sprig_doc::filter::parse(case.get_document("filter").unwrap())?;
                let ids: Vec<Bson> = docs_of(case)
                    .iter()
                    .filter(|d| pred.matches(d))
                    .map(|d| d.get("_id").unwrap().clone())
                    .collect();
                Ok(Bson::Array(ids))
            }
            "group" => {
                let mut out = agg::run_stage(
                    docs_of(case),
                    &doc! {"$group": case.get_document("group").unwrap().clone()},
                    &lim,
                )?;
                agg::sort_docs(&mut out, &vec![("_id".to_owned(), 1)]);
                Ok(Bson::Array(out.into_iter().map(Bson::Document).collect()))
            }
            _ => {
                let d = case.get_document("doc").unwrap().clone();
                let out = agg::run_stage(
                    vec![d],
                    &doc! {"$replaceWith": {"r": case.get("expr").unwrap().clone()}},
                    &lim,
                )?;
                Ok(Bson::Array(out.into_iter().map(Bson::Document).collect()))
            }
        }
    });
    match scope.take_error() {
        Some(e) => Err(e),
        None => r,
    }
}

#[test]
fn server_js_matches_mongodb_8_0() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/serverjs_diff");
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
    let verbose = std::env::var("SPRIG_JS_DIFF_VERBOSE").is_ok();
    let (mut ok, mut known_n, mut errors) = (0, 0, 0);
    let mut failed = Vec::new();
    for c in &cases {
        let c = c.as_document().unwrap();
        let name = c.get_str("name").unwrap();
        let expect = c.get_document("expect").unwrap();
        if expect.contains_key("code") {
            errors += 1;
        }
        let got = run(c);
        let matched = match (expect.get("ok"), expect.get("code"), &got) {
            (Some(want), _, Ok(g)) => same(want, g),
            (_, Some(code), Err(e)) => code.as_i32() == Some(e.code),
            _ => false,
        };
        if matched {
            ok += 1;
            continue;
        }
        let line = format!(
            "{name}: expected {expect}, got {}",
            match &got {
                Ok(g) => g.to_string(),
                Err(e) => format!("error {} {}", e.code, e.message),
            }
        );
        if known.iter().any(|k| k == name) {
            known_n += 1;
            if verbose {
                eprintln!("known  {line}");
            }
        } else {
            eprintln!("DIFF   {line}");
            failed.push(name.to_owned());
        }
    }
    eprintln!(
        "server-side JavaScript vs MongoDB 8.0.34: {ok} of {} matched ({errors} error cases), {known_n} known differences, {} new",
        cases.len(),
        failed.len()
    );
    assert!(failed.is_empty(), "new differences: {failed:?}");
}
