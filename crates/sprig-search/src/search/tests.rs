use proptest::prelude::*;
use serde_json::json;

use super::*;
use crate::def::{FieldDef, FieldKind, Flavor, IndexDef, Model, Source};
use crate::query::{parse_atlas, parse_mongo_text, parse_mysql_boolean, parse_query_string};
use crate::schema::{KeyPart, Val};

fn table_def(parts: u32) -> IndexDef {
    let mut d = IndexDef::new("t", "db", "products", Model::Table, Flavor::Sql);
    d.key_columns = vec!["id".into()];
    d.partitions = parts;
    let mut name = FieldDef::new("name", Source::Column, FieldKind::Text);
    name.analyzer = Some("english".into());
    name.boost = 2.0;
    d.push(name).unwrap();
    d.push(FieldDef::new("body", Source::Column, FieldKind::Text))
        .unwrap();
    let mut brand = FieldDef::new("brand", Source::Column, FieldKind::Keyword);
    brand.facet = true;
    brand.filter = true;
    d.push(brand).unwrap();
    let mut price = FieldDef::new("price", Source::Column, FieldKind::Number);
    price.filter = true;
    d.push(price).unwrap();
    d
}

fn row(id: i64, name: &str, body: &str, brand: &str, price: f64) -> Record {
    Record::Row {
        key: Key::of_parts(&[KeyPart::Int(id)]),
        columns: vec![
            ("id".into(), Val::Int(id)),
            ("name".into(), Val::Str(name.into())),
            ("body".into(), Val::Str(body.into())),
            ("brand".into(), Val::Str(brand.into())),
            ("price".into(), Val::Float(price)),
        ],
    }
}

fn ids(hits: &[Hit]) -> Vec<i64> {
    hits.iter()
        .map(|h| match h.key.parts().unwrap()[0] {
            KeyPart::Int(i) => i,
            _ => -1,
        })
        .collect()
}

fn sample(parts: u32) -> SearchIndex {
    let s = IndexSchema::build(&table_def(parts), &[]).unwrap();
    let ix = SearchIndex::open(s, None, MIN).unwrap();
    for r in [
        row(1, "Red running shoe", "light and fast", "acme", 80.0),
        row(2, "Blue running shoes", "for trail runners", "zoom", 120.0),
        row(3, "Red hat", "warm wool", "acme", 20.0),
        row(4, "Trail shoe", "rugged sole for the trail", "zoom", 95.0),
        row(5, "Kids shoe", "red and small", "tiny", 30.0),
    ] {
        ix.upsert(&r).unwrap();
    }
    ix.commit("p1").unwrap();
    ix
}

const MIN: usize = crate::partition::MIN_WRITER_BYTES;

#[test]
fn match_phrase_bool_and_ranges() {
    let ix = sample(3);
    let all = |q: &SearchQuery| {
        let mut v = ids(&ix.top(q, 100, Scoring::Global, 50).unwrap());
        v.sort_unstable();
        v
    };
    let q = parse_query_string("red", &[]).unwrap();
    assert_eq!(all(&q), [1, 3, 5]);
    // Stemming on `name` (english): "shoes" finds "shoe".
    let q = parse_query_string("name:shoes", &[]).unwrap();
    assert_eq!(all(&q), [1, 2, 4, 5]);
    let q = parse_query_string(r#""running shoe""#, &[]).unwrap();
    assert_eq!(all(&q), [1, 2]);
    let q = parse_query_string("red -kids", &[]).unwrap();
    assert_eq!(all(&q), [1, 3]);
    let q = parse_query_string("trai*", &[]).unwrap();
    assert_eq!(all(&q), [2, 4]);
    let q = parse_query_string("name:runing~1", &[]).unwrap();
    assert_eq!(all(&q), [1, 2]);
    let q = parse_atlas(&json!({"compound": {
        "must": [{"text": {"query": "shoe", "path": "name"}}],
        "filter": [{"range": {"path": "price", "gte": 50, "lt": 100}}]}}))
    .unwrap();
    assert_eq!(all(&q), [1, 4]);
    let q = parse_atlas(&json!({"equals": {"path": "brand", "value": "zoom"}})).unwrap();
    assert_eq!(all(&q), [2, 4]);
    let q = parse_mysql_boolean("+shoe -red", &["name".into(), "body".into()]).unwrap();
    assert_eq!(all(&q), [2, 4]);
    assert_eq!(ix.count(&parse_mongo_text("red"), 50).unwrap(), 3);
}

#[test]
fn boosts_order_results() {
    let ix = sample(2);
    // "red" in the name (boost 2) beats "red" in the body.
    let q = parse_query_string("red", &[]).unwrap();
    let hits = ix.top(&q, 3, Scoring::Global, 50).unwrap();
    assert_eq!(*ids(&hits).last().unwrap(), 5);
}

#[test]
fn deletes_and_replay_are_idempotent() {
    let ix = sample(2);
    ix.delete(&Key::of_parts(&[KeyPart::Int(3)]));
    // Replaying an old insert, then its newer version, keeps one document.
    ix.upsert(&row(1, "Red running shoe", "", "acme", 1.0))
        .unwrap();
    ix.upsert(&row(1, "Green running shoe", "", "acme", 1.0))
        .unwrap();
    ix.commit("p2").unwrap();
    let q = parse_query_string("red", &[]).unwrap();
    assert_eq!(ids(&ix.top(&q, 10, Scoring::Global, 50).unwrap()), [5]);
    assert_eq!(ix.num_docs(), 4);
    assert_eq!(ix.payloads().unwrap(), vec![Some("p2".to_owned()); 2]);
}

/// Found live (r9feed2): a copy that applied updates again after a
/// worker's `kill -9` holds deleted documents until a merge; they count in
/// a term's `doc_freq`, so global BM25 statistics must count them in the
/// documents too, or Tantivy's `doc_count >= doc_freq` assertion panics.
#[test]
fn global_scoring_counts_deleted_documents_like_tantivy() {
    let ix = sample(1);
    for id in 1..=5 {
        ix.upsert(&row(id, "Running shoe", "", "acme", 1.0))
            .unwrap();
    }
    ix.commit("p2").unwrap();
    // One document updated again: its old version stays in its segment,
    // deleted, until a merge.
    ix.upsert(&row(1, "Running shoe", "", "acme", 2.0)).unwrap();
    ix.commit("p3").unwrap();
    let q = parse_query_string("shoe", &[]).unwrap();
    assert_eq!(ix.top(&q, 10, Scoring::Global, 50).unwrap().len(), 5);
    assert_eq!(ix.num_docs(), 5);
}

#[test]
fn facets_and_window() {
    let ix = sample(2);
    let q = parse_query_string("shoe", &["name".into()]).unwrap();
    let b = ix.facet(&q, "brand", 10, None, 50).unwrap();
    assert_eq!(b[0].value, json!("zoom"));
    assert_eq!(b[0].count, 2);
    let n = ix
        .facet(&q, "price", 10, Some(&[0.0, 50.0, 100.0, 1000.0]), 50)
        .unwrap();
    assert_eq!(n.iter().map(|x| x.count).collect::<Vec<_>>(), [1, 2, 1]);
    let e = ix.all(&q, 2, 50).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SearchWindowExceeded));
}

/// Counts and facets restricted to a key set (the rows a
/// caller may see) count only those keys.
#[test]
fn keys_restrict_counts_and_facets() {
    let ix = sample(2);
    let q = parse_query_string("shoe", &["name".into()]).unwrap();
    assert_eq!(ix.count(&q, 50).unwrap(), 4);
    let k = |i: i64| Key::of_parts(&[KeyPart::Int(i)]);
    // Row 2 and row 4 (both "zoom") are hidden: only 1 and 5 are visible.
    let within = |keys: Vec<Key>| SearchQuery::Bool {
        must: vec![q.clone()],
        should: Vec::new(),
        must_not: Vec::new(),
        filter: vec![SearchQuery::Keys(keys)],
        min_should: 0,
    };
    let v = within(vec![k(1), k(5), k(3)]);
    assert_eq!(ix.count(&v, 50).unwrap(), 2, "row 3 isn't a hit");
    let b = ix.facet(&v, "brand", 10, None, 50).unwrap();
    let got: Vec<(String, u64)> = b
        .iter()
        .map(|x| (x.value.as_str().unwrap().to_owned(), x.count))
        .collect();
    assert_eq!(got, [("acme".to_owned(), 1), ("tiny".to_owned(), 1)]);
    let n = ix
        .facet(&v, "price", 10, Some(&[0.0, 50.0, 100.0, 1000.0]), 50)
        .unwrap();
    assert_eq!(n.iter().map(|x| x.count).collect::<Vec<_>>(), [1, 1, 0]);
    // No visible key: nothing counted, and the round trip keeps the keys.
    assert_eq!(ix.count(&within(Vec::new()), 50).unwrap(), 0);
    let wire = serde_json::to_string(&v).unwrap();
    assert_eq!(serde_json::from_str::<SearchQuery>(&wire).unwrap(), v);
}

#[test]
fn collections_and_dynamic_text() {
    let mut d = IndexDef::from_mongo_text(
        "t",
        "db",
        "c",
        &[("$**".into(), json!("text"))],
        &[],
        None,
        None,
    )
    .unwrap();
    d.partitions = 2;
    let s = IndexSchema::build(&d, &[]).unwrap();
    let ix = SearchIndex::open(s, None, MIN).unwrap();
    ix.upsert(&Record::Doc {
        key: Key(vec![1]),
        doc: json!({"_id": 1, "a": {"b": "Café au lait"}, "n": 3}),
    })
    .unwrap();
    ix.upsert(&Record::Doc {
        key: Key(vec![2]),
        doc: json!({"_id": 2, "c": ["iced coffee", "cafes"]}),
    })
    .unwrap();
    ix.commit("x").unwrap();
    assert_eq!(ix.count(&parse_mongo_text("cafe"), 50).unwrap(), 2);
    assert_eq!(ix.count(&parse_mongo_text("\"au lait\""), 50).unwrap(), 1);
    assert_eq!(ix.count(&parse_mongo_text("-cafe"), 50).unwrap(), 0);
}

// ---- The global scoring property ----

const WORDS: [&str; 12] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima",
];

fn doc_text() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(WORDS.to_vec()), 1..12).prop_map(|w| w.join(" "))
}

fn build(parts: u32, docs: &[(String, String)]) -> SearchIndex {
    let s = IndexSchema::build(&table_def(parts), &[]).unwrap();
    let ix = SearchIndex::open(s, None, MIN).unwrap();
    for (i, (n, b)) in docs.iter().enumerate() {
        ix.upsert(&row(i64::try_from(i).unwrap(), n, b, "x", 1.0))
            .unwrap();
    }
    ix.commit("p").unwrap();
    ix
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Random corpora split into random partitions score exactly as one
    /// unsharded index.
    #[test]
    fn global_scores_equal_unsharded(
        docs in prop::collection::vec((doc_text(), doc_text()), 1..40),
        parts in 2u32..6,
        q in prop::collection::vec(prop::sample::select(WORDS.to_vec()), 1..4),
    ) {
        let one = build(1, &docs);
        let many = build(parts, &docs);
        let query = parse_query_string(&q.join(" "), &[]).unwrap();
        let a = one.top(&query, 1000, Scoring::Global, 50).unwrap();
        let b = many.top(&query, 1000, Scoring::Global, 50).unwrap();
        prop_assert_eq!(a.len(), b.len());
        let mut sa: Vec<(Vec<u8>, f32)> = a.iter().map(|h| (h.key.0.clone(), h.score)).collect();
        let mut sb: Vec<(Vec<u8>, f32)> = b.iter().map(|h| (h.key.0.clone(), h.score)).collect();
        sa.sort_by(|x, y| x.0.cmp(&y.0));
        sb.sort_by(|x, y| x.0.cmp(&y.0));
        for (x, y) in sa.iter().zip(&sb) {
            prop_assert_eq!(&x.0, &y.0);
            prop_assert!((x.1 - y.1).abs() <= 1e-4 * x.1.abs().max(1.0), "{} vs {}", x.1, y.1);
        }
    }
}

#[test]
fn local_scores_differ_where_global_agree() {
    // A term rarer in one partition.
    let mut docs = Vec::new();
    for i in 0..60 {
        let t = if i % 3 == 0 { "shoe sock" } else { "sock" };
        docs.push((t.to_owned(), String::new()));
    }
    let one = build(1, &docs);
    let many = build(4, &docs);
    let q = parse_query_string("shoe", &["name".into()]).unwrap();
    let g1 = one.top(&q, 100, Scoring::Global, 50).unwrap();
    let g4 = many.top(&q, 100, Scoring::Global, 50).unwrap();
    let l4 = many.top(&q, 100, Scoring::Local, 50).unwrap();
    let spread = |h: &[Hit]| {
        let max = h.iter().map(|x| x.score).fold(f32::MIN, f32::max);
        let min = h.iter().map(|x| x.score).fold(f32::MAX, f32::min);
        max - min
    };
    assert!(spread(&g1) < 1e-5 && spread(&g4) < 1e-5);
    assert!((g1[0].score - g4[0].score).abs() < 1e-5);
    assert!(
        spread(&l4) > 1e-3,
        "local statistics should disagree across partitions"
    );
}
