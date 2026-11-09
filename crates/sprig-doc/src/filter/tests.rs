#![allow(clippy::needless_pass_by_value)]

use bson::{Bson, doc};

use super::*;

fn m(filter: Document, d: Document) -> bool {
    parse(&filter).unwrap().matches(&d)
}

#[test]
fn equality_and_arrays() {
    let d = doc! {"a": [1, 5, [7]], "b": {"c": "x"}, "items": [{"sku": "A"}, {"sku": "B", "q": 2}]};
    assert!(m(doc! {"a": 5}, d.clone()));
    assert!(m(doc! {"a": 5.0}, d.clone()));
    assert!(m(doc! {"a": [7]}, d.clone()));
    assert!(m(doc! {"a": [1, 5, [7]]}, d.clone()));
    assert!(
        !m(doc! {"a": 7}, d.clone()),
        "nested arrays are compared whole"
    );
    assert!(m(doc! {"b.c": "x"}, d.clone()));
    assert!(m(doc! {"items.sku": "B"}, d.clone()));
    assert!(m(doc! {"items.1.sku": "B"}, d.clone()));
    assert!(!m(doc! {"items.0.sku": "B"}, d.clone()));
    assert!(m(doc! {"items.q": null}, d.clone()), "an element without q");
    assert!(m(doc! {"zz": null}, d.clone()));
    assert!(!m(doc! {"b": null}, d.clone()));
    assert!(m(doc! {"u": null}, doc! {"u": Bson::Undefined}));
}

#[test]
fn ranges_bracket_types() {
    let d = doc! {"a": [1, 10], "s": "abc", "n": Bson::Null};
    assert!(m(doc! {"a": {"$gt": 5}}, d.clone()));
    assert!(
        m(doc! {"a": {"$gt": 5, "$lt": 2}}, d.clone()),
        "different elements"
    );
    assert!(!m(
        doc! {"a": {"$elemMatch": {"$gt": 5, "$lt": 9}}},
        d.clone()
    ));
    assert!(m(
        doc! {"a": {"$elemMatch": {"$gt": 5, "$lt": 11}}},
        d.clone()
    ));
    assert!(!m(doc! {"s": {"$gt": 5}}, d.clone()));
    assert!(m(doc! {"s": {"$gt": "abb"}}, d.clone()));
    assert!(m(doc! {"n": {"$gte": Bson::Null}}, d.clone()));
    assert!(!m(doc! {"n": {"$gt": Bson::Null}}, d.clone()));
    assert!(m(doc! {"x": {"$lt": Bson::MaxKey}}, doc! {"x": "q"}));
    assert!(!m(doc! {"x": {"$lt": 5}}, doc! {"x": f64::NAN}));
    assert!(m(doc! {"x": f64::NAN}, doc! {"x": f64::NAN}));
}

#[test]
fn logic_and_negation() {
    let d = doc! {"a": 1, "tags": ["x", "y"]};
    assert!(m(doc! {"$or": [{"a": 2}, {"tags": "y"}]}, d.clone()));
    assert!(!m(doc! {"$nor": [{"a": 2}, {"tags": "y"}]}, d.clone()));
    assert!(m(doc! {"tags": {"$ne": "z"}}, d.clone()));
    assert!(!m(doc! {"tags": {"$ne": "x"}}, d.clone()));
    assert!(m(doc! {"missing": {"$ne": 1}}, d.clone()));
    assert!(m(doc! {"tags": {"$nin": ["z"]}}, d.clone()));
    assert!(m(doc! {"a": {"$not": {"$gt": 5}}}, d.clone()));
    assert!(m(doc! {"tags": {"$all": ["y", "x"]}}, d.clone()));
    assert!(!m(doc! {"tags": {"$all": ["y", "q"]}}, d.clone()));
    assert!(m(doc! {"tags": {"$size": 2}}, d.clone()));
    assert!(m(
        doc! {"tags": {"$exists": true}, "nope": {"$exists": false}},
        d.clone()
    ));
    assert!(m(doc! {"a": {"$in": [5, 1]}}, d.clone()));
    assert!(m(doc! {"nope": {"$in": [Bson::Null]}}, d.clone()));
}

#[test]
fn types_regex_mod() {
    let d = doc! {"i": 5, "l": 5i64, "s": "Hello", "arr": [1, "two"]};
    assert!(m(doc! {"i": {"$type": "int"}}, d.clone()));
    assert!(m(doc! {"l": {"$type": "long"}}, d.clone()));
    assert!(m(doc! {"l": {"$type": "number"}}, d.clone()));
    assert!(m(doc! {"arr": {"$type": "string"}}, d.clone()));
    assert!(m(doc! {"arr": {"$type": "array"}}, d.clone()));
    assert!(m(
        doc! {"s": {"$regex": "^hel", "$options": "i"}},
        d.clone()
    ));
    assert!(!m(doc! {"s": {"$regex": "^hel"}}, d.clone()));
    assert!(m(
        doc! {"s": Bson::RegularExpression(bson::Regex { pattern: "ll".into(), options: String::new() })},
        d.clone()
    ));
    assert!(m(doc! {"i": {"$mod": [2, 1]}}, d.clone()));
    assert!(parse(&doc! {"a": {"$bogus": 1}}).is_err());
    assert!(parse(&doc! {"$where": "1"}).is_err());
    let re = Re::new("^ab\\.c", "").unwrap();
    assert_eq!(re.literal_prefix().as_deref(), Some("ab"));
    assert_eq!(
        Re::new("^abc?", "").unwrap().literal_prefix().as_deref(),
        Some("ab")
    );
    assert_eq!(Re::new("^abc", "i").unwrap().literal_prefix(), None);
}

#[test]
fn elem_match_documents_and_upsert_fields() {
    let d = doc! {"items": [{"sku": "A", "q": 1}, {"sku": "B", "q": 5}]};
    assert!(m(
        doc! {"items": {"$elemMatch": {"sku": "B", "q": {"$gt": 2}}}},
        d.clone()
    ));
    assert!(!m(
        doc! {"items": {"$elemMatch": {"sku": "A", "q": {"$gt": 2}}}},
        d.clone()
    ));
    let p = parse(&doc! {"_id": {"$in": [1, 2]}, "a": 1, "$and": [{"b.c": 2}]}).unwrap();
    assert_eq!(p.id_values().unwrap().len(), 2);
    let mut f = Vec::new();
    p.equality_fields(&mut f);
    assert_eq!(f.len(), 2, "a two-value $in pins nothing");
}
