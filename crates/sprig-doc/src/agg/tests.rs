#![allow(clippy::needless_pass_by_value)]

use bson::{Bson, DateTime, doc};

use super::*;

fn lim() -> AggLimits {
    AggLimits {
        stage_bytes: 1 << 20,
        now: DateTime::from_millis(0),
    }
}

fn run(docs: Vec<Document>, stages: Vec<Document>) -> Vec<Document> {
    let mut cur = docs;
    for s in &stages {
        cur = run_stage(cur, s, &lim()).unwrap();
    }
    cur
}

#[test]
fn group_sort_project() {
    let docs = vec![
        doc! {"_id": 1, "cat": "a", "qty": 2, "price": 1.5},
        doc! {"_id": 2, "cat": "b", "qty": 1, "price": 10},
        doc! {"_id": 3, "cat": "a", "qty": 3, "price": 2},
    ];
    let out = run(
        docs.clone(),
        vec![
            doc! {"$group": {"_id": "$cat", "n": {"$sum": 1}, "q": {"$sum": "$qty"}, "avg": {"$avg": "$price"}, "ids": {"$push": "$_id"}}},
            doc! {"$sort": {"_id": 1}},
        ],
    );
    assert_eq!(
        out[0],
        doc! {"_id": "a", "n": 2, "q": 5, "avg": 1.75, "ids": [1, 3]}
    );
    let out = run(
        docs.clone(),
        vec![
            doc! {"$project": {"_id": 0, "cat": 1, "total": {"$multiply": ["$qty", "$price"]}}},
            doc! {"$match": {"total": {"$gt": 4}}},
        ],
    );
    assert_eq!(
        out,
        vec![
            doc! {"cat": "b", "total": 10},
            doc! {"cat": "a", "total": 6}
        ]
    );
    let out = run(docs, vec![doc! {"$count": "n"}]);
    assert_eq!(out, vec![doc! {"n": 3}]);
}

#[test]
fn unwind_facet_expressions() {
    let docs = vec![
        doc! {"_id": 1, "tags": ["x", "y"], "s": "Hello"},
        doc! {"_id": 2, "tags": []},
    ];
    let out = run(docs.clone(), vec![doc! {"$unwind": "$tags"}]);
    assert_eq!(out.len(), 2);
    let out = run(
        docs.clone(),
        vec![doc! {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}],
    );
    assert_eq!(out.len(), 3);
    let out = run(
        docs,
        vec![
            doc! {"$facet": {"c": [{"$count": "n"}], "up": [{"$project": {"u": {"$toUpper": "$s"}, "_id": 0}}]}},
        ],
    );
    assert_eq!(out[0].get_array("c").unwrap().len(), 1);
    let v = Vars::new(
        &doc! {"a": [1, 2, 3], "d": DateTime::from_millis(1_762_510_500_123)},
        DateTime::from_millis(0),
    );
    assert_eq!(
        eval(&Bson::Document(doc! {"$size": "$a"}), &v).unwrap(),
        Bson::Int32(3)
    );
    assert_eq!(
        eval(
            &Bson::Document(doc! {"$cond": [{"$gt": [{"$size": "$a"}, 2]}, "big", "small"]}),
            &v
        )
        .unwrap(),
        Bson::String("big".into())
    );
    assert_eq!(
        eval(&Bson::Document(doc! {"$year": "$d"}), &v).unwrap(),
        Bson::Int32(2025)
    );
    assert_eq!(
        eval(&Bson::Document(doc! {"$sum": "$a"}), &v).unwrap(),
        Bson::Int32(6)
    );
    assert_eq!(
        eval(
            &Bson::Document(doc! {"$map": {"input": "$a", "as": "x", "in": {"$add": ["$$x", 1]}}}),
            &v
        )
        .unwrap(),
        Bson::Array(vec![2.into(), 3.into(), 4.into()])
    );
    assert!(eval(&Bson::Document(doc! {"$bogus": 1}), &v).is_err());
}

#[test]
fn projection_and_sort() {
    let d = doc! {"_id": 1, "a": {"b": 1, "c": 2}, "arr": [{"x": 1, "y": 2}, {"x": 3}], "l": [1, 2, 3, 4]};
    let p = Projection::parse(&doc! {"a.b": 1, "arr.x": 1}).unwrap();
    assert_eq!(
        p.apply(&d, DateTime::from_millis(0)).unwrap(),
        doc! {"_id": 1, "a": {"b": 1}, "arr": [{"x": 1}, {"x": 3}]}
    );
    let p = Projection::parse(&doc! {"a": 0, "_id": 0}).unwrap();
    assert_eq!(
        p.apply(&d, DateTime::from_millis(0))
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["arr", "l"]
    );
    let p = Projection::parse(&doc! {"l": {"$slice": -2}}).unwrap();
    assert_eq!(
        p.apply(&d, DateTime::from_millis(0))
            .unwrap()
            .get_array("l")
            .unwrap()
            .len(),
        2
    );
    assert!(Projection::parse(&doc! {"a": 1, "b": 0}).is_err());
    let p = Projection::parse(&doc! {"_id": 1}).unwrap();
    assert_eq!(
        p.apply(&d, DateTime::from_millis(0)).unwrap(),
        doc! {"_id": 1}
    );
    let mut docs = vec![doc! {"v": [5, 1]}, doc! {"v": 3}, doc! {}, doc! {"v": "s"}];
    sort_docs(&mut docs, &vec![("v".to_owned(), 1)]);
    assert_eq!(docs[0], doc! {});
    assert_eq!(docs[1], doc! {"v": [5, 1]});
    sort_docs(&mut docs, &vec![("v".to_owned(), -1)]);
    assert_eq!(docs[0], doc! {"v": "s"});
    assert_eq!(docs[1], doc! {"v": [5, 1]});
}
