#![allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    clippy::manual_midpoint
)]

use bson::{Bson, Document, bson, doc};

use super::*;
use crate::agg::AggLimits;

fn lim() -> AggLimits {
    AggLimits {
        stage_bytes: usize::MAX,
        now: bson::DateTime::now(),
    }
}

/// Runs the input pipelines in memory (no `$search` here: the fixtures
/// carry their ranks and scores), then fuses.
fn fuse(docs: &[Document], stage: Document) -> Result<Vec<Document>, MongoError> {
    let (name, arg) = stage.iter().next().unwrap();
    let kind = if name == "$rankFusion" {
        Kind::Rank
    } else {
        Kind::Score
    };
    let spec = parse(kind, arg)?;
    let mut inputs = Vec::new();
    for (_, p) in &spec.pipelines {
        let mut ds = docs.to_vec();
        for s in p {
            ds = match s.get("$score") {
                Some(a) => score_stage(ds, a, lim().now)?,
                None => crate::agg::run_stage(ds, s, &lim())?,
            };
        }
        inputs.push(Input {
            docs: ds
                .into_iter()
                .map(|mut d| {
                    let s = d.remove(SCORE_FIELD).as_ref().and_then(num);
                    (d, s)
                })
                .collect(),
        });
    }
    Ok(match kind {
        Kind::Rank => rank_fusion(&spec, &inputs),
        Kind::Score => score_fusion(&spec, &inputs, lim().now)?,
    })
}

fn ids_scores(out: &[Document]) -> Vec<(Bson, f64)> {
    out.iter()
        .map(|d| {
            (
                d.get("_id").unwrap().clone(),
                d.get_f64(FUSION_SCORE).unwrap(),
            )
        })
        .collect()
}

fn coll1() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "a": 3, "b": 1},
        doc! {"_id": 2, "a": 2, "b": 2},
        doc! {"_id": 3, "a": 1, "b": 3},
        doc! {"_id": 4, "a": 0, "b": 9},
    ]
}

/// MongoDB 8.0.34 (`featureFlagRankFusionBasic`) on the same documents:
/// ids 1, 3, 2, 4 with these scores.
#[test]
fn rrf_matches_mongodb() {
    let out = fuse(
        &coll1(),
        doc! {"$rankFusion": {"input": {"pipelines": {"p1": [{"$sort": {"a": -1}}], "p2": [{"$match": {"b": {"$lt": 5}}}, {"$sort": {"b": -1}}]}}}},
    )
    .unwrap();
    assert_eq!(
        ids_scores(&out),
        [
            (Bson::Int32(1), 0.032_266_458_495_966_696),
            (Bson::Int32(3), 0.032_266_458_495_966_696),
            (Bson::Int32(2), 0.032_258_064_516_129_03),
            (Bson::Int32(4), 0.015_625),
        ]
    );
    // MongoDB's documented example: ranks 3 and 1 give 1/63 + 1/61.
    assert!((1.0 / 63.0 + 1.0 / 61.0 - 0.032_266_458_5_f64).abs() < 1e-10);
}

#[test]
fn rrf_weights_and_details_match_mongodb() {
    let out = fuse(
        &coll1(),
        doc! {"$rankFusion": {"input": {"pipelines": {"p1": [{"$sort": {"a": -1}}], "p2": [{"$match": {"b": {"$lt": 5}}}, {"$sort": {"b": -1}}]}}, "combination": {"weights": {"p2": 2}}, "scoreDetails": true}},
    )
    .unwrap();
    assert_eq!(
        ids_scores(&out),
        [
            (Bson::Int32(3), 0.048_659_901_118_917_51),
            (Bson::Int32(2), 0.048_387_096_774_193_55),
            (Bson::Int32(1), 0.048_139_474_368_982_57),
            (Bson::Int32(4), 0.015_625),
        ]
    );
    let det = out[3].get_document(FUSION_DETAILS).unwrap();
    assert_eq!(
        det,
        &doc! {"value": 0.015_625, "description": RRF_DESCRIPTION, "details": [
            {"inputPipelineName": "p1", "rank": 4, "weight": 1.0, "details": []},
            {"inputPipelineName": "p2", "rank": 0, "weight": 2.0},
        ]}
    );
}

fn coll2() -> Vec<Document> {
    vec![
        doc! {"_id": 5, "a": 3, "b": 1},
        doc! {"_id": 2, "a": 2, "b": 2},
        doc! {"_id": 9, "a": 1, "b": 3},
        doc! {"_id": 1, "a": 0, "b": 9},
        doc! {"_id": "x", "a": -1, "b": 0},
    ]
}

/// Ties go by `_id` ascending, whatever the pipelines' names or order
/// (MongoDB 8.0.34 answers 5, "x", 1, 2, 9).
#[test]
fn ties_by_id() {
    for spec in [
        doc! {"$rankFusion": {"input": {"pipelines": {"p1": [{"$sort": {"a": -1}}], "p2": [{"$sort": {"a": 1}}]}}}},
        doc! {"$rankFusion": {"input": {"pipelines": {"z": [{"$sort": {"a": 1}}], "a": [{"$sort": {"a": -1}}]}}}},
    ] {
        let out = fuse(&coll2(), spec).unwrap();
        let ids: Vec<Bson> = out.iter().map(|d| d.get("_id").unwrap().clone()).collect();
        assert_eq!(ids, [bson!(5), bson!("x"), bson!(1), bson!(2), bson!(9)]);
        assert_eq!(
            out[0].get_f64(FUSION_SCORE).unwrap(),
            0.031_778_058_007_566_21
        );
    }
    // Weight 0 and 1.5; skip and limit inside the pipelines.
    let out = fuse(
        &coll2(),
        doc! {"$rankFusion": {"input": {"pipelines": {"p1": [{"$sort": {"a": -1}}], "p2": [{"$sort": {"b": -1}}]}}, "combination": {"weights": {"p1": 0, "p2": 1.5}}}},
    )
    .unwrap();
    assert_eq!(
        ids_scores(&out),
        [
            (bson!(1), 0.024_590_163_934_426_23),
            (bson!(9), 0.024_193_548_387_096_774),
            (bson!(2), 0.023_809_523_809_523_808),
            (bson!(5), 0.023_437_5),
            (bson!("x"), 0.023_076_923_076_923_078),
        ]
    );
    let out = fuse(
        &coll2(),
        doc! {"$rankFusion": {"input": {"pipelines": {"p1": [{"$sort": {"a": -1}}, {"$skip": 1}, {"$limit": 2}], "p2": [{"$sort": {"b": 1}}, {"$limit": 3}]}}}},
    )
    .unwrap();
    assert_eq!(
        ids_scores(&out),
        [
            (bson!(2), 0.032_266_458_495_966_696),
            (bson!("x"), 0.016_393_442_622_950_82),
            (bson!(5), 0.016_129_032_258_064_516),
            (bson!(9), 0.016_129_032_258_064_516),
        ]
    );
}

/// Ranks and scores from a search: fixtures with known ranks (what
/// `$search` and `$vectorSearch` would output), checked against the
/// formula exactly.
#[test]
fn rrf_over_search_fixtures() {
    let spec = parse(
        Kind::Rank,
        &bson!({"input": {"pipelines": {"text": [{"$search": {}}], "vector": [{"$vectorSearch": {}}]}}, "combination": {"weights": {"vector": 3}}, "scoreDetails": true}),
    )
    .unwrap();
    let text = Input {
        docs: vec![
            (doc! {"_id": "d3"}, Some(2.5)),
            (doc! {"_id": "d2"}, Some(1.25)),
            (doc! {"_id": "d1"}, Some(0.5)),
        ],
    };
    let vector = Input {
        docs: vec![
            (doc! {"_id": "d1"}, Some(0.99)),
            (doc! {"_id": "d4"}, Some(0.8)),
            (doc! {"_id": "d2"}, Some(0.7)),
        ],
    };
    let out = rank_fusion(&spec, &[text, vector]);
    let want = [
        ("d1", 1.0 / 63.0 + 3.0 / 61.0),
        ("d2", 1.0 / 62.0 + 3.0 / 63.0),
        ("d4", 3.0 / 62.0),
        ("d3", 1.0 / 61.0),
    ];
    for (d, (id, s)) in out.iter().zip(want) {
        assert_eq!(d.get_str("_id").unwrap(), id);
        assert_eq!(d.get_f64(FUSION_SCORE).unwrap(), s);
    }
    assert_eq!(
        out[0]
            .get_document(FUSION_DETAILS)
            .unwrap()
            .get_array("details")
            .unwrap()[0],
        bson!({"inputPipelineName": "text", "rank": 3, "weight": 1.0, "value": 0.5, "details": []})
    );
}

#[test]
fn score_fusion_math() {
    let spec = |norm: &str, comb: Document| {
        parse(
            Kind::Score,
            &bson!({"input": {"pipelines": {"s": [{"$search": {}}], "v": [{"$vectorSearch": {}}]}, "normalization": norm}, "combination": comb, "scoreDetails": true}),
        )
        .unwrap()
    };
    let inputs = || {
        [
            Input {
                docs: vec![
                    (doc! {"_id": 1}, Some(4.0)),
                    (doc! {"_id": 2}, Some(2.0)),
                    (doc! {"_id": 3}, Some(1.0)),
                ],
            },
            Input {
                docs: vec![(doc! {"_id": 2}, Some(0.9)), (doc! {"_id": 4}, Some(0.5))],
            },
        ]
    };
    let now = bson::DateTime::now();
    // none + avg: the sum over both pipelines divided by 2.
    let out = score_fusion(&spec("none", doc! {}), &inputs(), now).unwrap();
    assert_eq!(
        ids_scores(&out),
        [
            (bson!(1), 2.0),
            (bson!(2), 1.45),
            (bson!(3), 0.5),
            (bson!(4), 0.25)
        ]
    );
    // minMaxScaler per pipeline, weights.
    let out = score_fusion(
        &spec("minMaxScaler", doc! {"weights": {"v": 2}}),
        &inputs(),
        now,
    )
    .unwrap();
    assert_eq!(
        ids_scores(&out),
        [
            (bson!(2), (1.0 / 3.0 + 2.0) / 2.0),
            (bson!(1), 0.5),
            (bson!(3), 0.0),
            (bson!(4), 0.0)
        ]
    );
    // sigmoid + expression, MongoDB's documented example shape.
    let out = score_fusion(
        &spec("sigmoid", doc! {"method": "expression", "expression": {"$sum": [{"$multiply": ["$$s", 10]}, "$$v"]}}),
        &inputs(),
        now,
    )
    .unwrap();
    let sig = |x: f64| 1.0 / (1.0 + (-x).exp());
    assert_eq!(out[0].get("_id").unwrap(), &bson!(1));
    assert_eq!(out[0].get_f64(FUSION_SCORE).unwrap(), 10.0 * sig(4.0));
    let two = out
        .iter()
        .find(|d| d.get("_id") == Some(&bson!(2)))
        .unwrap();
    assert_eq!(
        two.get_f64(FUSION_SCORE).unwrap(),
        10.0 * sig(2.0) + sig(0.9)
    );
    let det = two.get_document(FUSION_DETAILS).unwrap();
    assert_eq!(det.get_str("normalization").unwrap(), "sigmoid");
    assert_eq!(
        det.get_array("details").unwrap()[1],
        bson!({"inputPipelineName": "v", "inputPipelineRawScore": 0.9, "weight": 1.0, "value": sig(0.9), "details": []})
    );
}

#[test]
fn score_stage_feeds_score_fusion() {
    let out = fuse(
        &coll1(),
        doc! {"$scoreFusion": {"input": {"pipelines": {"x": [{"$match": {"a": {"$gte": 1}}}, {"$score": {"score": "$a"}}], "y": [{"$score": {"score": "$b", "normalization": "minMaxScaler"}}]}, "normalization": "none"}}},
    )
    .unwrap();
    // x: 3, 2, 1 for ids 1, 2, 3; y: (b - 1) / 8.
    assert_eq!(
        ids_scores(&out),
        [
            (bson!(1), 1.5),
            (bson!(2), (2.0 + 0.125) / 2.0),
            (bson!(3), (1.0 + 0.25) / 2.0),
            (bson!(4), 0.5)
        ]
    );
}

#[test]
fn errors_match_mongodb() {
    let code = |arg: Bson| parse(Kind::Rank, &arg).unwrap_err().code;
    let msg = |arg: Bson| parse(Kind::Rank, &arg).unwrap_err().message;
    let p = || bson!({"p": [{"$sort": {"a": 1}}]});
    assert_eq!(
        code(bson!({"input": {"pipelines": {"p1": [{"$match": {"a": 1}}]}}})),
        9_191_100
    );
    assert_eq!(
        code(
            bson!({"input": {"pipelines": {"p1": [{"$sort": {"a": 1}}, {"$project": {"a": 1}}]}}})
        ),
        9_191_103
    );
    assert_eq!(
        msg(bson!({})),
        "BSON field '$rankFusion.input' is missing but a required field"
    );
    assert_eq!(code(bson!({"input": {"pipelines": {}}})), 2);
    assert_eq!(
        code(bson!({"input": {"pipelines": {"$x": [{"$sort": {"a": 1}}]}}})),
        16_410
    );
    assert_eq!(
        code(bson!({"input": {"pipelines": {"a.b": [{"$sort": {"a": 1}}]}}})),
        16_412
    );
    assert_eq!(
        code(bson!({"input": {"pipelines": {"": [{"$sort": {"a": 1}}]}}})),
        15_998
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": p()}, "combination": {"weights": {"p": -1}}})),
        "Rank fusion pipeline weight must be non-negative, but given -1 for pipeline 'p'."
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": p()}, "combination": {"weights": {"q": 1}}})),
        "$rankFusion stage contained (1) weight(s) in 'combination.weights' that did not reference valid pipeline names. Suggestions for valid pipeline names: (provided: 'q' -> suggested: 'p')"
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": p()}, "combination": {"weights": {"p": "1"}}})),
        "expected p to have a numeric type, but it is a string"
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": p()}, "foo": 1})),
        "BSON field '$rankFusion.foo' is an unknown field."
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": p(), "x": 1}})),
        "BSON field '$rankFusion.input.x' is an unknown field."
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": {"p": {"$sort": {"a": 1}}}}})),
        "Error parsing $rankFusion.input.pipelines :: caused by :: A pipeline must be an array of objects"
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": p()}, "scoreDetails": 1})),
        "BSON field '$rankFusion.scoreDetails' is the wrong type 'int', expected type 'bool'"
    );
    assert_eq!(
        msg(bson!({"input": {"pipelines": [1]}})),
        "BSON field '$rankFusion.input.pipelines' is the wrong type 'array', expected type 'object'"
    );
    assert_eq!(
        msg(Bson::Int32(1)),
        "$rankFusion must take a nested object but found: $rankFusion: 1"
    );
    assert_eq!(
        code(bson!({"input": {"pipelines": {"p": [{"$sample": {"size": 2}}]}}})),
        9_191_100
    );
    // $scoreFusion: scored pipelines.
    let sc = |arg: Bson| parse(Kind::Score, &arg).unwrap_err().code;
    assert_eq!(
        sc(bson!({"input": {"pipelines": {"p": [{"$sort": {"a": 1}}]}, "normalization": "none"}})),
        9_402_500
    );
    assert_eq!(
        sc(bson!({"input": {"pipelines": {"p": [{"$search": {}}]}}})),
        40_414
    );
}

#[test]
fn refused_in_sub_pipelines() {
    let rf = bson!({"$rankFusion": {"input": {"pipelines": {"p": [{"$sort": {"a": 1}}]}}}});
    let lookup = bson!({"$lookup": {"from": "r", "pipeline": [rf.clone()], "as": "x"}});
    let union = bson!({"$unionWith": {"coll": "r", "pipeline": [rf.clone()]}});
    assert_eq!(
        check_nested(&[bson!({"$limit": 1}), lookup])
            .unwrap_err()
            .code,
        11_178_500
    );
    assert_eq!(check_nested(&[union]).unwrap_err().code, 11_178_500);
    assert!(check_nested(&[rf]).is_ok());
}
