//! The aggregation-stage differential test (M2, 12 Nov 2026): window
//! functions, accumulators and the stages `$setWindowFields`, `$bucket`,
//! `$bucketAuto`, `$densify`, `$fill`, `$redact`, `$documents` and
//! `$graphLookup`.
//!
//! `tests/stage_diff/cases.json` holds pipelines and the answers MongoDB
//! 8.0.34 gave for them (recorded by `tests/stage_diff/gen_cases.js` on a
//! private reference server). This test runs every pipeline over the
//! case's documents with SprigDB's in-gateway stages, as the engine runs
//! them (`$graphLookup` reads the case's collections in memory), and
//! compares: documents in order with their exact BSON types (doubles bit
//! for bit), errors by code.
//!
//! Cases listed in `tests/stage_diff/known.txt` are known differences and
//! are reported but not failed. `SPRIG_STAGE_DIFF_VERBOSE=1` prints every
//! mismatch; `SPRIG_STAGE_DIFF_ONLY=<prefix>` runs only the cases whose
//! names start with the prefix.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr)]

use bson::{Bson, DateTime, Document};
use sprig_doc::agg::{self, AggLimits, GraphSpec};
use sprig_proto_mongo::errors::MongoError;

fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()),
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
        (Bson::Decimal128(x), Bson::Decimal128(y)) => x.bytes() == y.bytes(),
        _ => a == b,
    }
}

fn docs_of(v: Option<&Bson>) -> Vec<Document> {
    match v {
        Some(Bson::Array(a)) => a.iter().filter_map(|d| d.as_document().cloned()).collect(),
        _ => Vec::new(),
    }
}

fn run(case: &Document) -> Result<Vec<Document>, (i32, String)> {
    let lim = AggLimits {
        stage_bytes: 100 << 20,
        now: DateTime::now(),
    };
    let wrap = |e: MongoError| (e.code, e.message.clone());
    let colls = case.get_document("colls").ok();
    let mut stages = docs_of(case.get("pipeline"));
    let mut cur = if case.get_bool("db").unwrap_or(false) {
        let first = stages.remove(0);
        let (name, arg) = first.iter().next().unwrap();
        if let Some(e) = agg::db_level_error(name) {
            return Err(wrap(e));
        }
        agg::documents(arg, &lim).map_err(wrap)?
    } else {
        docs_of(case.get("docs"))
    };
    for s in &stages {
        cur = match s.get("$graphLookup") {
            Some(g) => {
                let spec = GraphSpec::parse(g).map_err(wrap)?;
                let from = colls
                    .map(|c| docs_of(c.get(&spec.from)))
                    .unwrap_or_default();
                agg::graph_lookup_in_memory(cur, &spec, &from, &lim).map_err(wrap)?
            }
            None => agg::run_stage(cur, s, &lim).map_err(wrap)?,
        };
    }
    Ok(cur)
}

#[test]
fn stages_match_mongodb_8_0() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/stage_diff");
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
    let verbose = std::env::var("SPRIG_STAGE_DIFF_VERBOSE").is_ok();
    let only = std::env::var("SPRIG_STAGE_DIFF_ONLY").unwrap_or_default();
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
                let ok = same(&got, &want);
                let mut detail = String::new();
                if let (Bson::Array(g), Bson::Array(w)) = (&got, &want) {
                    if g.len() != w.len() {
                        detail = format!("{} documents, want {}\n      ", g.len(), w.len());
                    }
                    for (i, (x, y)) in g.iter().zip(w).enumerate() {
                        if !same(x, y) {
                            detail.push_str(&format!("#{i} want {y:?}\n      #{i} got  {x:?}\n      "));
                            break;
                        }
                    }
                }
                (ok, detail)
            }
            (Ok(Err((code, msg))), None) => {
                let want_code = expect.get_i32("code").unwrap_or(-1);
                let want_msg = expect.get_str("errmsg").unwrap_or("");
                if *code == want_code && !want_msg.ends_with(msg.as_str()) {
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
                format!("want {}\n      got  error {code} {msg}", expect.get("ok").unwrap()),
            ),
        };
        if ok {
            matched += 1;
            continue;
        }
        let pipeline = c.get("pipeline").cloned().unwrap_or(Bson::Null);
        if known.contains(&name) {
            known_hit += 1;
            if verbose {
                eprintln!("KNOWN {name} {pipeline}\n      {detail}");
            }
            continue;
        }
        if verbose {
            eprintln!("DIFF {name} {pipeline}\n      {detail}");
        }
        failures.push(name);
    }
    eprintln!(
        "stage differential: {total} cases, {matched} match, {known_hit} known differences, \
         {} unexpected; {msg_diff} with the same code but another message",
        failures.len()
    );
    assert!(failures.is_empty(), "unexpected differences: {failures:?}");
}
