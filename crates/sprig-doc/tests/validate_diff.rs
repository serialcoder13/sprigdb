//! The schema-validation differential test.
//!
//! `tests/validate_diff/cases.json` holds validators with a document, and
//! filters with documents, and the answers MongoDB 8.0.34 gave for them
//! (recorded by `tests/validate_diff/gen_cases.js` on a private reference
//! server). This test runs every case through SprigDB's validation
//! ([`sprig_doc::validate`]) and filter matcher and compares: whether the
//! insert passed, the error code, and `errInfo` exactly (field order and
//! BSON types included); `create` errors and find errors by code; and the
//! `_id`s a find matched.
//!
//! Cases listed in `tests/validate_diff/known.txt` are known differences,
//! reported but not failed. `SPRIG_VALIDATE_DIFF_VERBOSE=1` prints every
//! mismatch; `SPRIG_VALIDATE_DIFF_ONLY=<prefix>` runs only the cases whose
//! names start with the prefix.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::float_cmp,
    clippy::cast_possible_truncation,
    clippy::match_same_arms,
    clippy::too_many_lines
)]

use bson::{Bson, Document};
use sprig_doc::filter;
use sprig_doc::validate::{self, Rules};

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
        // mongosh turns whole doubles in `errInfo` into numbers, which it
        // records as int32: compare those by value.
        (Bson::Double(x), Bson::Int32(y)) | (Bson::Int32(y), Bson::Double(x)) => {
            *x == f64::from(*y)
        }
        _ => a == b,
    }
}

/// What SprigDB answers for a case, in the corpus's `expect` shape.
fn run(case: &Document) -> Document {
    if let Ok(v) = case.get_document("validator") {
        let opts = bson::doc! {"validator": v.clone()};
        if let Err(e) = validate::check_options(&opts) {
            return bson::doc! {"createCode": e.code};
        }
        let rules = Rules::of(&opts).unwrap();
        let d = case.get_document("doc").unwrap();
        match rules.map(|r| r.check_insert("m3diff.c", d)) {
            None | Some(Ok(())) => bson::doc! {"ok": true},
            Some(Err(e)) => {
                let info = e
                    .extra
                    .as_ref()
                    .and_then(|x| x.get("errInfo").cloned())
                    .unwrap_or(Bson::Null);
                bson::doc! {"code": e.code, "errInfo": info}
            }
        }
    } else {
        let f = case.get_document("filter").unwrap();
        match filter::parse(f) {
            Err(e) => bson::doc! {"code": e.code},
            Ok(p) => {
                let ids: Vec<Bson> = case
                    .get_array("docs")
                    .unwrap()
                    .iter()
                    .filter_map(|d| d.as_document())
                    .filter(|d| p.matches(d))
                    .map(|d| d.get("_id").cloned().unwrap())
                    .collect();
                bson::doc! {"ids": ids}
            }
        }
    }
}

fn agrees(got: &Document, want: &Document) -> bool {
    for k in ["ok", "ids", "createCode", "code"] {
        if let Some(w) = want.get(k) {
            let Some(g) = got.get(k) else { return false };
            let num = |b: &Bson| match b {
                Bson::Int32(i) => Some(i64::from(*i)),
                Bson::Int64(i) => Some(*i),
                Bson::Double(f) => Some(*f as i64),
                _ => None,
            };
            let eq = match (num(g), num(w)) {
                (Some(x), Some(y)) if k != "ids" => x == y,
                _ => same(g, w),
            };
            if !eq {
                return false;
            }
        }
    }
    match (got.get("errInfo"), want.get("errInfo")) {
        (Some(g), Some(w)) => same(g, w),
        (None, None) => true,
        (None, Some(Bson::Null)) => true,
        _ => false,
    }
}

#[test]
fn validation_matches_mongodb_8_0() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/validate_diff");
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
    let verbose = std::env::var("SPRIG_VALIDATE_DIFF_VERBOSE").is_ok();
    let only = std::env::var("SPRIG_VALIDATE_DIFF_ONLY").ok();
    let (mut pass, mut fail, mut known_fail, mut err_info) = (0, Vec::new(), 0, 0);
    for c in &cases {
        let c = c.as_document().unwrap();
        let name = c.get_str("name").unwrap();
        if only.as_deref().is_some_and(|p| !name.starts_with(p)) {
            continue;
        }
        let want = c.get_document("expect").unwrap();
        let got = run(c);
        if want.get("errInfo").is_some_and(|e| *e != Bson::Null) {
            err_info += 1;
        }
        if agrees(&got, want) {
            pass += 1;
        } else if known.iter().any(|k| k == name) {
            known_fail += 1;
        } else {
            if verbose {
                eprintln!(
                    "MISMATCH {name}\n  case {}\n  want {}\n  got  {}",
                    Bson::Document(c.clone()).into_canonical_extjson(),
                    Bson::Document(want.clone()).into_canonical_extjson(),
                    Bson::Document(got.clone()).into_canonical_extjson()
                );
            }
            fail.push(name.to_owned());
        }
    }
    eprintln!(
        "validation differential: {pass} pass, {} fail, {known_fail} known ({err_info} cases with errInfo)",
        fail.len()
    );
    assert!(fail.is_empty(), "mismatches: {fail:?}");
}
