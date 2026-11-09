#![allow(clippy::needless_pass_by_value)]

use bson::{Bson, DateTime, doc};

use super::*;
use crate::filter;

fn up(d: Document, u: Document) -> Result<Document, MongoError> {
    up_q(d, u, doc! {})
}

fn up_q(mut d: Document, u: Document, q: Document) -> Result<Document, MongoError> {
    let spec = parse(&Bson::Document(u))?;
    let q = filter::parse(&q)?;
    let cx = ApplyCx {
        query: &q,
        array_filters: &[],
        inserting: false,
        now: DateTime::from_millis(0),
    };
    apply(&mut d, &spec, &cx)?;
    Ok(d)
}

#[test]
fn basic_operators() {
    let d = doc! {"_id": 1, "a": 1, "b": {"c": 2}, "arr": [1, 2, 3]};
    assert_eq!(
        up(d.clone(), doc! {"$set": {"b.d": "x", "n.m": 1}}).unwrap(),
        doc! {"_id": 1, "a": 1, "b": {"c": 2, "d": "x"}, "arr": [1, 2, 3], "n": {"m": 1}}
    );
    assert_eq!(
        up(d.clone(), doc! {"$inc": {"a": 2, "z": 1i64}})
            .unwrap()
            .get("z"),
        Some(&Bson::Int64(1))
    );
    assert_eq!(
        up(d.clone(), doc! {"$inc": {"a": i32::MAX}})
            .unwrap()
            .get("a"),
        Some(&Bson::Int64(2_147_483_648))
    );
    assert_eq!(
        up(d.clone(), doc! {"$mul": {"a": 2.5}}).unwrap().get("a"),
        Some(&Bson::Double(2.5))
    );
    assert_eq!(
        up(d.clone(), doc! {"$unset": {"b.c": ""}})
            .unwrap()
            .get_document("b")
            .unwrap(),
        &doc! {}
    );
    assert_eq!(
        up(
            d.clone(),
            doc! {"$push": {"arr": {"$each": [0, 9], "$sort": -1, "$slice": 3}}}
        )
        .unwrap()
        .get_array("arr")
        .unwrap(),
        &vec![Bson::Int32(9), Bson::Int32(3), Bson::Int32(2)]
    );
    assert_eq!(
        up(d.clone(), doc! {"$addToSet": {"arr": {"$each": [1, 4]}}})
            .unwrap()
            .get_array("arr")
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        up(d.clone(), doc! {"$pop": {"arr": -1}})
            .unwrap()
            .get_array("arr")
            .unwrap()[0],
        Bson::Int32(2)
    );
    assert_eq!(
        up(d.clone(), doc! {"$pull": {"arr": {"$gte": 2}}})
            .unwrap()
            .get_array("arr")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        up(d.clone(), doc! {"$rename": {"a": "aa"}})
            .unwrap()
            .get("aa"),
        Some(&Bson::Int32(1))
    );
    assert_eq!(
        up(d.clone(), doc! {"$max": {"a": 5}, "$min": {"b.c": 0}}).unwrap(),
        doc! {"_id": 1, "a": 5, "b": {"c": 0}, "arr": [1, 2, 3]}
    );
    assert_eq!(
        up(d.clone(), doc! {"x": 1}).unwrap(),
        doc! {"_id": 1, "x": 1}
    );
    assert_eq!(
        up(d.clone(), doc! {"$set": {"arr.5": 6}})
            .unwrap()
            .get_array("arr")
            .unwrap()
            .len(),
        6
    );
    let dec = up(
        doc! {"p": bson::Decimal128::from_str("19.95").unwrap()},
        doc! {"$inc": {"p": bson::Decimal128::from_str("0.05").unwrap()}},
    )
    .unwrap();
    assert_eq!(dec.get("p").unwrap().to_string(), "20.00");
}

#[test]
fn errors() {
    let d = doc! {"_id": 1, "a": 1, "s": "x"};
    assert_eq!(
        up(d.clone(), doc! {"$set": {"_id": 2}}).unwrap_err().code,
        code::IMMUTABLE_FIELD
    );
    assert_eq!(
        up(d.clone(), doc! {"$inc": {"s": 1}}).unwrap_err().code,
        code::TYPE_MISMATCH
    );
    assert_eq!(
        up(d.clone(), doc! {"$set": {"a.b": 1}}).unwrap_err().code,
        28
    );
    assert_eq!(
        parse(&Bson::Document(doc! {"$set": {"a": 1}, "$inc": {"a": 1}}))
            .unwrap_err()
            .code,
        code::CONFLICTING_UPDATE_OPERATORS
    );
    assert_eq!(
        parse(&Bson::Document(doc! {"$bogus": {"a": 1}}))
            .unwrap_err()
            .code,
        code::FAILED_TO_PARSE
    );
    assert_eq!(
        parse(&Bson::Document(doc! {"$set": {"a": 1}, "b": 2}))
            .unwrap_err()
            .code,
        code::FAILED_TO_PARSE
    );
}

#[test]
fn positional() {
    let d = doc! {"_id": 1, "items": [{"sku": "A", "q": 1}, {"sku": "B", "q": 2}]};
    let r = up_q(
        d.clone(),
        doc! {"$inc": {"items.$.q": 10}},
        doc! {"items.sku": "B"},
    )
    .unwrap();
    assert_eq!(
        r.get_array("items").unwrap()[1]
            .as_document()
            .unwrap()
            .get_i32("q")
            .unwrap(),
        12
    );
    let r = up(d.clone(), doc! {"$set": {"items.$[].q": 0}}).unwrap();
    assert!(
        r.get_array("items").unwrap().iter().all(|e| e
            .as_document()
            .unwrap()
            .get_i32("q")
            .unwrap()
            == 0)
    );
    let mut x = d.clone();
    let af = vec![("e".to_owned(), filter::parse(&doc! {"e.sku": "A"}).unwrap())];
    let q = filter::parse(&doc! {}).unwrap();
    let cx = ApplyCx {
        query: &q,
        array_filters: &af,
        inserting: false,
        now: DateTime::from_millis(0),
    };
    apply(
        &mut x,
        &parse(&Bson::Document(doc! {"$set": {"items.$[e].q": 7}})).unwrap(),
        &cx,
    )
    .unwrap();
    assert_eq!(
        x.get_array("items").unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("q")
            .unwrap(),
        7
    );
    assert_eq!(
        x.get_array("items").unwrap()[1]
            .as_document()
            .unwrap()
            .get_i32("q")
            .unwrap(),
        2
    );
    let seed =
        upsert_seed(&filter::parse(&doc! {"a": 1, "b.c": "x", "d": {"$gt": 1}}).unwrap()).unwrap();
    assert_eq!(seed, doc! {"a": 1, "b": {"c": "x"}});
}

use std::str::FromStr;
