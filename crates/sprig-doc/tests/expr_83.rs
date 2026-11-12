//! The MongoDB 8.1–8.3 expressions SprigDB enables (M1, 12 Nov 2026),
//! checked against the examples in MongoDB's documentation: the reference
//! server of the differential test (`expr_diff.rs`) is 8.0 and refuses
//! them.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Document, Timestamp, doc};
use sprig_doc::agg::{self, AggLimits};

fn eval_in(expr: Bson, d: Document) -> Result<Bson, i32> {
    let lim = AggLimits {
        stage_bytes: 1 << 20,
        now: DateTime::now(),
    };
    agg::run_stage(vec![d], &doc! {"$replaceWith": {"r": expr}}, &lim)
        .map(|mut out| {
            out.pop()
                .unwrap()
                .get("r")
                .cloned()
                .unwrap_or(Bson::Undefined)
        })
        .map_err(|e| e.code)
}

fn eval(expr: Bson) -> Result<Bson, i32> {
    eval_in(expr, doc! {"_id": 1})
}

fn ok(expr: Bson) -> Bson {
    eval(expr).expect("expression failed")
}

fn bin(st: u8, b64: &str) -> Bson {
    use base64::Engine as _;
    Bson::Binary(Binary {
        subtype: BinarySubtype::from(st),
        bytes: base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap(),
    })
}

fn hexbin(h: &str) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: (0..h.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
            .collect(),
    })
}

#[test]
fn subtype() {
    assert_eq!(
        ok(Bson::Document(
            doc! {"$subtype": {"$literal": bin(0, "gf1UcxdHTJ2HQ/EGQrO7mQ==")}}
        )),
        Bson::Int32(0)
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$subtype": {"$literal": bin(4, "gf1UcxdHTJ2HQ/EGQrO7mQ==")}}
        )),
        Bson::Int32(4)
    );
    assert_eq!(ok(Bson::Document(doc! {"$subtype": null})), Bson::Null);
    assert_eq!(ok(Bson::Document(doc! {"$subtype": "$nope"})), Bson::Null);
    assert!(eval(Bson::Document(doc! {"$subtype": 5})).is_err());
}

#[test]
fn hash_and_hex_hash() {
    // The documentation's example: SHA-256 of "report.pdf".
    assert_eq!(
        ok(Bson::Document(
            doc! {"$hash": {"input": "report.pdf", "algorithm": "sha256"}}
        )),
        bin(0, "ZGbkUKFrd7hlxYKda2xW2fiSlWR1ZC2+uczENA/gGxU=")
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$hexHash": {"input": "abc", "algorithm": "md5"}}
        )),
        Bson::String("900150983CD24FB0D6963F7D28E17F72".into())
    );
    // XXH64("abc", seed 0), big-endian.
    assert_eq!(
        ok(Bson::Document(
            doc! {"$hexHash": {"input": "abc", "algorithm": "xxh64"}}
        )),
        Bson::String("44BC2CF5AD770999".into())
    );
    // Binary input hashes its bytes.
    assert_eq!(
        ok(Bson::Document(
            doc! {"$hexHash": {"input": {"$literal": hexbin("616263")}, "algorithm": "md5"}}
        )),
        Bson::String("900150983CD24FB0D6963F7D28E17F72".into())
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$hash": {"input": null, "algorithm": "sha256"}}
        )),
        Bson::Null
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$hash": {"input": "$val", "algorithm": "sha256"}}
        )),
        Bson::Null
    );
    assert!(
        eval(Bson::Document(
            doc! {"$hash": {"input": 5, "algorithm": "md5"}}
        ))
        .is_err()
    );
    assert!(
        eval(Bson::Document(
            doc! {"$hash": {"input": "a", "algorithm": "sha1"}}
        ))
        .is_err()
    );
}

#[test]
fn create_object_id_and_current_date() {
    let a = ok(Bson::Document(doc! {"$createObjectId": {}}));
    let b = ok(Bson::Document(doc! {"$createObjectId": {}}));
    assert!(matches!(a, Bson::ObjectId(_)));
    assert_ne!(a, b);
    assert_eq!(
        eval(Bson::Document(doc! {"$createObjectId": {"a": 1}})),
        Err(9)
    );
    assert_eq!(eval(Bson::Document(doc! {"$createObjectId": 1})), Err(9));
    let before = DateTime::now().timestamp_millis();
    let Bson::DateTime(d) = ok(Bson::Document(doc! {"$currentDate": {}})) else {
        panic!("not a date");
    };
    assert!(d.timestamp_millis() >= before);
}

#[test]
fn ejson_round_trip() {
    let d = doc! {
        "_id": bson::oid::ObjectId::parse_str("573a1398f29313caabcea974").unwrap(),
        "title": "Inception",
        "year": 2010_i32,
        "big": 42_i64,
        "rating": 8.8,
        "released": DateTime::from_millis(1_279_238_400_000),
        "cast": ["a", "b"],
    };
    let canon = ok(Bson::Document(
        doc! {"$serializeEJSON": {"input": "$$ROOT"}},
    ));
    let canon_in = eval_in(
        Bson::Document(doc! {"$serializeEJSON": {"input": "$$ROOT"}}),
        d.clone(),
    )
    .unwrap();
    assert!(matches!(canon, Bson::Document(_)));
    let c = canon_in.as_document().unwrap();
    assert_eq!(
        c.get_document("_id").unwrap(),
        &doc! {"$oid": "573a1398f29313caabcea974"}
    );
    assert_eq!(
        c.get_document("year").unwrap(),
        &doc! {"$numberInt": "2010"}
    );
    assert_eq!(c.get_document("big").unwrap(), &doc! {"$numberLong": "42"});
    assert_eq!(
        c.get_document("released").unwrap(),
        &doc! {"$date": {"$numberLong": "1279238400000"}}
    );
    let relaxed = eval_in(
        Bson::Document(doc! {"$serializeEJSON": {"input": "$$ROOT", "relaxed": true}}),
        d.clone(),
    )
    .unwrap();
    let r = relaxed.as_document().unwrap();
    assert_eq!(r.get("year"), Some(&Bson::Int32(2010)));
    assert_eq!(
        r.get_document("released").unwrap(),
        &doc! {"$date": "2010-07-16T00:00:00.000Z"}
    );
    // And back.
    let back = eval_in(
        Bson::Document(
            doc! {"$deserializeEJSON": {"input": {"$serializeEJSON": {"input": "$$ROOT"}}}},
        ),
        d.clone(),
    )
    .unwrap();
    assert_eq!(back, Bson::Document(d.clone()));
    let back_relaxed = eval_in(
        Bson::Document(doc! {"$deserializeEJSON": {"input": {"$serializeEJSON": {"input": "$$ROOT", "relaxed": true}}}}),
        d.clone(),
    )
    .unwrap();
    assert_eq!(
        back_relaxed.as_document().unwrap().get("released"),
        d.get("released")
    );
    assert_eq!(
        ok(Bson::Document(doc! {"$serializeEJSON": {"input": null}})),
        Bson::Null
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$deserializeEJSON": {"input": {"$literal": {"x": {"$numberLong": "zz"}}}, "onError": "bad"}}
        )),
        Bson::String("bad".into())
    );
    // A JSON string parsed with $convert, then deserialized (the
    // documentation's example).
    let movie = ok(Bson::Document(
        doc! {"$deserializeEJSON": {"input": {"$convert": {
        "input": "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"title\":\"The Matrix\",\"year\":{\"$numberInt\":\"1999\"},\"rating\":{\"$numberDouble\":\"8.7\"}}",
        "to": "object"}}}},
    ));
    assert_eq!(
        movie,
        Bson::Document(doc! {
            "_id": bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
            "title": "The Matrix", "year": 1999_i32, "rating": 8.7,
        })
    );
}

#[test]
fn to_string_of_any_type() {
    let s = |e: Bson| ok(Bson::Document(doc! {"$toString": e}));
    assert_eq!(
        s(Bson::Document(
            doc! {"$literal": {"pizza": {"type": "cheese"}}}
        )),
        Bson::String("{\"pizza\":{\"type\":\"cheese\"}}".into())
    );
    assert_eq!(
        s(Bson::Array(vec![Bson::Array(vec![
            "pizza".into(),
            Bson::Document(doc! {"type": "cheese"})
        ])])),
        Bson::String("[\"pizza\",{\"type\":\"cheese\"}]".into())
    );
    assert_eq!(
        s(Bson::RegularExpression(bson::Regex {
            pattern: "^ABC".into(),
            options: "i".into()
        })),
        Bson::String("/^ABC/i".into())
    );
    assert_eq!(
        s(Bson::Timestamp(Timestamp {
            time: 1_565_545_664,
            increment: 1
        })),
        Bson::String("Timestamp(1565545664, 1)".into())
    );
    assert_eq!(s(Bson::MinKey), Bson::String("MinKey".into()));
    assert_eq!(s(Bson::MaxKey), Bson::String("MaxKey".into()));
}

#[test]
fn convert_base_and_json() {
    let c = |d: Document| eval(Bson::Document(doc! {"$convert": d}));
    assert_eq!(
        c(doc! {"input": "ff", "to": "int", "base": 16}),
        Ok(Bson::Int32(255))
    );
    assert_eq!(
        c(doc! {"input": "777", "to": "long", "base": 8}),
        Ok(Bson::Int64(511))
    );
    assert_eq!(
        c(doc! {"input": "101", "to": "double", "base": 2}),
        Ok(Bson::Double(5.0))
    );
    assert_eq!(
        c(doc! {"input": 255, "to": "string", "base": 16}),
        Ok(Bson::String("FF".into()))
    );
    assert_eq!(
        c(doc! {"input": 5, "to": "string", "base": 2}),
        Ok(Bson::String("101".into()))
    );
    assert_eq!(
        c(doc! {"input": 10.5, "to": "string", "base": 2, "onError": "invalid"}),
        Ok(Bson::String("invalid".into()))
    );
    assert_eq!(
        c(doc! {"input": "0x1A", "to": "int", "base": 16, "onError": "E"}),
        Ok(Bson::String("E".into()))
    );
    assert_eq!(c(doc! {"input": "12", "to": "int", "base": 3}), Err(9));
    assert_eq!(
        c(doc! {"input": "12", "to": "int", "base": null}),
        Ok(Bson::Int32(12))
    );
    // Strings to arrays and objects.
    assert_eq!(
        c(doc! {"input": "[1, 2, 3]", "to": "array"}),
        Ok(Bson::Array(vec![1.into(), 2.into(), 3.into()]))
    );
    assert_eq!(
        c(doc! {"input": "[]", "to": "array"}),
        Ok(Bson::Array(vec![]))
    );
    assert_eq!(c(doc! {"input": "{}", "to": "array"}), Err(241));
    assert_eq!(c(doc! {"input": 123, "to": "array"}), Err(241));
    assert_eq!(c(doc! {"input": "123", "to": "array"}), Err(241));
    assert_eq!(
        c(doc! {"input": "asdf", "to": "array", "onError": "on error!"}),
        Ok(Bson::String("on error!".into()))
    );
    assert_eq!(
        c(doc! {"input": null, "to": "array", "onNull": "on null!"}),
        Ok(Bson::String("on null!".into()))
    );
    assert_eq!(
        c(doc! {"input": "{\"a\": 1, \"b\": 2, \"a\": 3}", "to": "object"}),
        Ok(Bson::Document(doc! {"a": 3, "b": 2}))
    );
    assert_eq!(
        c(doc! {"input": "{\"foo\": 4294967296}", "to": "object"}),
        Ok(Bson::Document(doc! {"foo": 4_294_967_296_i64}))
    );
    assert_eq!(
        c(doc! {"input": "{\"nam\\u0000e\": \"foo\"}", "to": "object"}),
        Err(241)
    );
    assert_eq!(
        ok(Bson::Document(doc! {"$toArray": "[\"a\", \"b\"]"})),
        Bson::Array(vec!["a".into(), "b".into()])
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$toObject": "{\"z\": {\"y\": [true, null]}}"}
        )),
        Bson::Document(doc! {"z": {"y": [true, null]}})
    );
}

#[test]
fn convert_bindata_and_numbers() {
    let c = |d: Document| eval(Bson::Document(doc! {"$convert": d}));
    // Numbers to binData (8.1), the documentation's examples.
    assert_eq!(
        c(doc! {"input": 42_i64, "to": "binData"}),
        Ok(bin(0, "KgAAAAAAAAA="))
    );
    assert_eq!(
        c(doc! {"input": 42_i64, "to": "binData", "byteOrder": "big"}),
        Ok(bin(0, "AAAAAAAAACo="))
    );
    assert_eq!(
        c(doc! {"input": 42.0, "to": "binData"}),
        Ok(bin(0, "AAAAAAAARUA="))
    );
    assert_eq!(
        c(doc! {"input": 42, "to": "binData"}),
        Ok(bin(0, "KgAAAA=="))
    );
    // And back.
    assert_eq!(
        c(doc! {"input": {"$literal": hexbin("00100000")}, "to": "int", "byteOrder": "big"}),
        Ok(Bson::Int32(1_048_576))
    );
    assert_eq!(
        c(doc! {"input": {"$literal": hexbin("FFFFE796")}, "to": "int"}),
        Ok(Bson::Int32(-1_763_180_545))
    );
    assert_eq!(
        c(doc! {"input": {"$literal": hexbin("00100000000000")}, "to": "int", "byteOrder": "big"}),
        Err(241)
    );
    assert_eq!(
        c(doc! {"input": {"$literal": hexbin("04CCCCCD")}, "to": "double", "byteOrder": "big"}),
        Ok(Bson::Double(4.814_824_932_714_571e-36))
    );
    assert_eq!(
        c(doc! {"input": {"$literal": hexbin("0102")}, "to": "double"}),
        Err(241)
    );
    // Objects to binData (9.0): their BSON bytes.
    assert_eq!(
        c(doc! {"input": {"$literal": {}}, "to": "binData"}),
        Ok(bin(0, "BQAAAAA="))
    );
    assert_eq!(
        c(doc! {"input": {"$literal": {"a": "a"}}, "to": {"type": 5, "subtype": 128}}),
        Ok(bin(128, "DgAAAAJhAAIAAABhAAA="))
    );
}

#[test]
fn array_index_variables() {
    // `arrayIndexAs` and `$$IDX` (8.3) in $map, $filter and $reduce.
    assert_eq!(
        ok(Bson::Document(doc! {"$map": {
            "input": ["Documentary", "History", "Sport"], "as": "genre", "arrayIndexAs": "i",
            "in": {"genre": "$$genre", "rank": {"$add": ["$$i", 1]}, "isPrimary": {"$eq": ["$$i", 0]}}
        }})),
        Bson::Array(vec![
            Bson::Document(doc! {"genre": "Documentary", "rank": 1, "isPrimary": true}),
            Bson::Document(doc! {"genre": "History", "rank": 2, "isPrimary": false}),
            Bson::Document(doc! {"genre": "Sport", "rank": 3, "isPrimary": false}),
        ])
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$map": {"input": ["a", "b"], "in": "$$IDX"}}
        )),
        Bson::Array(vec![0.into(), 1.into()])
    );
    assert_eq!(
        ok(Bson::Document(
            doc! {"$filter": {"input": [5, 6, 7, 8], "cond": {"$eq": [{"$mod": ["$$IDX", 2]}, 1]}}}
        )),
        Bson::Array(vec![6.into(), 8.into()])
    );
    assert_eq!(
        ok(Bson::Document(doc! {"$reduce": {
            "input": [10, 20, 30], "initialValue": 0, "arrayIndexAs": "k",
            "in": {"$add": ["$$value", {"$multiply": ["$$this", "$$k"]}]}
        }})),
        Bson::Int32(80)
    );
    // Nested: the inner $map's IDX shadows the outer one.
    assert_eq!(
        ok(Bson::Document(
            doc! {"$map": {"input": [[1, 2], [3]], "arrayIndexAs": "o", "in": {
                "$map": {"input": "$$this", "in": {"$add": [{"$multiply": ["$$o", 10]}, "$$IDX"]}}
            }}}
        )),
        Bson::Array(vec![
            Bson::Array(vec![0.into(), 1.into()]),
            Bson::Array(vec![10.into()]),
        ])
    );
}

#[test]
fn concat_arrays_and_set_union_in_group() {
    // 8.1 accumulators: they live in $group (mongo2's accumulators); this
    // checks the expression side they share.
    assert_eq!(
        ok(Bson::Document(doc! {"$concatArrays": [[1], [2, [3]]]})),
        Bson::Array(vec![1.into(), 2.into(), Bson::Array(vec![3.into()])])
    );
    assert_eq!(
        ok(Bson::Document(doc! {"$setUnion": [[1, 2], [2, 3]]})),
        Bson::Array(vec![1.into(), 2.into(), 3.into()])
    );
    let lim = AggLimits {
        stage_bytes: 1 << 20,
        now: DateTime::now(),
    };
    let docs = vec![
        doc! {"_id": 1, "k": "a", "t": [1, 2]},
        doc! {"_id": 2, "k": "a", "t": [2, 3]},
        doc! {"_id": 3, "k": "b", "t": []},
    ];
    let mut out = agg::run_stage(
        docs,
        &doc! {"$group": {"_id": "$k", "all": {"$concatArrays": "$t"}, "set": {"$setUnion": "$t"}}},
        &lim,
    )
    .unwrap();
    agg::sort_docs(&mut out, &vec![("_id".to_owned(), 1)]);
    assert_eq!(
        out[0].get_array("all").unwrap(),
        &vec![Bson::Int32(1), 2.into(), 2.into(), 3.into()]
    );
    assert_eq!(out[0].get_array("set").unwrap().len(), 3);
    assert_eq!(out[1].get_array("all").unwrap(), &Vec::<Bson>::new());
}

/// The cost of the correctly rounded trigonometric functions (decision
/// S36): `cargo test --release -p sprig-doc --test expr_83 -- --ignored
/// --nocapture trig_cost`.
#[test]
#[ignore = "timing, run by hand"]
fn trig_cost() {
    let lim = AggLimits {
        stage_bytes: 1 << 30,
        now: DateTime::now(),
    };
    let docs: Vec<Document> = (0..10_000)
        .map(|i| doc! {"_id": i, "x": f64::from(i) / 997.0})
        .collect();
    for op in ["$sin", "$atan", "$exp", "$sinh"] {
        let t = std::time::Instant::now();
        agg::run_stage(docs.clone(), &doc! {"$project": {"r": {op: "$x"}}}, &lim).unwrap();
        eprintln!(
            "{op}: {:.1} µs per document",
            t.elapsed().as_secs_f64() * 1e6 / 10_000.0
        );
    }
}

/// The correctly rounded trigonometry (S36) stays within one unit in the
/// last place of the platform's functions, over a spread of arguments.
#[test]
fn trig_within_one_ulp_of_libm() {
    let lim = AggLimits {
        stage_bytes: 1 << 30,
        now: DateTime::now(),
    };
    let mut seed: u64 = 7;
    let mut xs = Vec::new();
    for _ in 0..2000 {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        #[allow(clippy::cast_precision_loss)]
        let u = (seed >> 11) as f64 / (1u64 << 53) as f64;
        let e = (seed % 40) as i32 - 30;
        xs.push((u * 2.0 - 1.0) * 10f64.powi(e));
    }
    let docs: Vec<Document> = xs
        .iter()
        .enumerate()
        .map(|(i, x)| doc! {"_id": i32::try_from(i).unwrap(), "x": *x})
        .collect();
    let ulps = |a: f64, b: f64| {
        a.to_bits()
            .cast_signed()
            .wrapping_sub(b.to_bits().cast_signed())
            .unsigned_abs()
    };
    for (op, f) in [
        ("$sin", f64::sin as fn(f64) -> f64),
        ("$cos", f64::cos),
        ("$tan", f64::tan),
        ("$atan", f64::atan),
    ] {
        let out =
            agg::run_stage(docs.clone(), &doc! {"$project": {"r": {op: "$x"}}}, &lim).unwrap();
        for (d, x) in out.iter().zip(&xs) {
            // macOS's tan is several units off for large arguments (its
            // answer for -758253.37 is 5 units from the true value).
            if op == "$tan" && x.abs() > 100.0 {
                continue;
            }
            let r = d.get_f64("r").unwrap();
            assert!(ulps(r, f(*x)) <= 1, "{op}({x:e}) = {r:e}, libm {:e}", f(*x));
        }
    }
    let small: Vec<Document> = docs
        .iter()
        .zip(&xs)
        .map(|(d, x)| {
            let mut d = d.clone();
            d.insert("x", x.clamp(-1.0, 1.0));
            d
        })
        .collect();
    for (op, f) in [("$asin", f64::asin as fn(f64) -> f64), ("$acos", f64::acos)] {
        let out =
            agg::run_stage(small.clone(), &doc! {"$project": {"r": {op: "$x"}}}, &lim).unwrap();
        for (d, s) in out.iter().zip(&small) {
            let x = s.get_f64("x").unwrap();
            let r = d.get_f64("r").unwrap();
            assert!(ulps(r, f(x)) <= 1, "{op}({x:e}) = {r:e}, libm {:e}", f(x));
        }
    }
}
