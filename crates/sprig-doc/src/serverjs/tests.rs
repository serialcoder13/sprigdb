#![allow(
    clippy::unwrap_used,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

use std::sync::Arc;
use std::time::Duration;

use bson::{Bson, Document, bson, doc};

use super::*;
use crate::agg::{Vars, eval};
use crate::filter;

/// The gateway's runner: SprigDB's QuickJS.
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

fn on(wall_ms: u64) -> Option<Arc<Scope>> {
    Some(Scope::new(
        Arc::new(Qjs),
        Duration::from_millis(wall_ms),
        32 << 20,
    ))
}

fn docs() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "a": 1, "s": "x"},
        doc! {"_id": 2, "a": 5, "s": "yy"},
    ]
}

fn ids(filter: Document) -> Result<Vec<i32>, MongoError> {
    let s = on(5000);
    sync_scope(s.clone(), || {
        let p = filter::parse(&filter)?;
        let out: Vec<i32> = docs()
            .iter()
            .filter(|d| p.matches(d))
            .map(|d| d.get_i32("_id").unwrap())
            .collect();
        match s.as_ref().and_then(|s| s.take_error()) {
            Some(e) => Err(e),
            None => Ok(out),
        }
    })
}

#[test]
fn where_matches() {
    assert_eq!(ids(doc! {"$where": "this.a > 2"}).unwrap(), [2]);
    assert_eq!(
        ids(doc! {"$where": Bson::JavaScriptCode("function () { return this.a < 2 }".into())})
            .unwrap(),
        [1]
    );
    assert_eq!(ids(doc! {"s": "yy", "$where": "this.a > 0"}).unwrap(), [2]);
    assert_eq!(
        ids(doc! {"$or": [{"a": 1}, {"$where": "obj.a == 5"}]}).unwrap(),
        [1, 2]
    );
    assert_eq!(
        ids(doc! {"$nor": [{"$where": "this.a == 5"}]}).unwrap(),
        [1]
    );
}

#[test]
fn where_errors() {
    let e = ids(doc! {"$where": "throw new Error('boom')"}).unwrap_err();
    assert_eq!(e.code, 139);
    assert!(e.message.starts_with("Error: boom"), "{}", e.message);
    assert_eq!(ids(doc! {"$where": "throw 'x'"}).unwrap_err().code, 8);
    assert_eq!(ids(doc! {"$where": 5}).unwrap_err().code, 2);
    assert_eq!(
        ids(doc! {"a": {"$where": "true"}}).unwrap_err().message,
        "$where cannot be applied to a field"
    );
    assert_eq!(
        ids(doc! {"x": {"$elemMatch": {"$where": "true"}}})
            .unwrap_err()
            .message,
        "$where can only be applied to the top-level document"
    );
    let s = on(100);
    let p = sync_scope(s.clone(), || {
        filter::parse(&doc! {"$where": "while (true) {}"})
    })
    .unwrap();
    sync_scope(s.clone(), || assert!(!p.matches(&doc! {"a": 1})));
    assert_eq!(s.unwrap().take_error().unwrap().code, 50);
    // Off: MongoDB's error for `security.javascriptEnabled: false`.
    assert_eq!(
        filter::parse(&doc! {"$where": "true"}).unwrap_err().code,
        6_108_304
    );
}

#[test]
fn where_is_refused_in_aggregation() {
    let p = |s: Document| check_pipeline(&[Bson::Document(s)]).map_err(|e| e.message);
    assert!(p(doc! {"$match": {"$where": "true"}}).is_err());
    assert!(p(doc! {"$match": {"$and": [{"$where": "true"}]}}).is_err());
    assert!(
        p(doc! {"$lookup": {"from": "c", "pipeline": [{"$match": {"$where": "true"}}], "as": "x"}})
            .is_err()
    );
    assert!(p(doc! {"$facet": {"a": [{"$match": {"$where": "true"}}]}}).is_err());
    assert!(p(doc! {"$match": {"a": 1}}).is_ok());
}

fn fun(spec: Bson) -> Result<Bson, MongoError> {
    let d = doc! {"a": 3, "sub": {"q": 1}};
    sync_scope(on(5000), || {
        eval(
            &bson!({"$function": spec}),
            &Vars::new(&d, bson::DateTime::now()),
        )
    })
}

#[test]
fn function_calls() {
    assert_eq!(
        fun(bson!({"body": "function (a) { return a * 2 }", "args": ["$a"], "lang": "js"}))
            .unwrap(),
        Bson::Double(6.0)
    );
    assert_eq!(
        fun(bson!({"body": "function (s) { return s }", "args": ["$sub"], "lang": "js"})).unwrap(),
        bson!({"q": 1})
    );
    assert_eq!(
        fun(bson!({"body": "1 + 2", "args": [], "lang": "js"})).unwrap(),
        Bson::Double(3.0)
    );
    assert_eq!(
        fun(bson!({"body": "function (a) { return a }", "args": {"$literal": [1]}, "lang": "js"}))
            .unwrap(),
        Bson::Double(1.0)
    );
    let code = |spec: Bson| fun(spec).unwrap_err().code;
    assert_eq!(code(bson!("function () {}")), 31260);
    assert_eq!(code(bson!({"args": [], "lang": "js"})), 31261);
    assert_eq!(code(bson!({"body": 5, "args": [], "lang": "js"})), 31262);
    assert_eq!(code(bson!({"body": "function () {}", "lang": "js"})), 31263);
    assert_eq!(
        code(bson!({"body": "function () {}", "args": "$a", "lang": "js"})),
        31266
    );
    assert_eq!(code(bson!({"body": "function () {}", "args": []})), 31418);
    assert_eq!(
        code(bson!({"body": "function () {}", "args": [], "lang": "py"})),
        31419
    );
    assert_eq!(
        code(bson!({"body": "function () { return x.y }", "args": [], "lang": "js"})),
        139
    );
    // Off.
    let d = Document::new();
    let off = eval(
        &bson!({"$function": {"body": "function () { return 1 }", "args": [], "lang": "js"}}),
        &Vars::new(&d, bson::DateTime::now()),
    );
    assert_eq!(off.unwrap_err().code, 31264);
}

#[test]
fn function_budget() {
    let d = Document::new();
    let r = sync_scope(on(100), || {
        eval(
            &bson!({"$function": {"body": "function () { while (true) {} }", "args": [], "lang": "js"}}),
            &Vars::new(&d, bson::DateTime::now()),
        )
    });
    assert_eq!(r.unwrap_err().code, 11601);
}

fn group(spec: Document, id_expr: &str) -> Result<Vec<Document>, MongoError> {
    let rows = vec![
        doc! {"k": "a", "v": 1},
        doc! {"k": "a", "v": 2},
        doc! {"k": "b", "v": 5},
        doc! {"k": "b"},
    ];
    sync_scope(on(5000), || {
        let mut js = GroupJs::parse(&spec)?;
        let mut keys: Vec<(Vec<u8>, Bson)> = Vec::new();
        for r in &rows {
            let vars = Vars::new(r, bson::DateTime::now());
            let id = eval(
                &Bson::String(if id_expr == "{k}" { "$k" } else { id_expr }.into()),
                &vars,
            )?;
            let id = if id_expr.starts_with('{') {
                bson!({"k": id})
            } else {
                id
            };
            let k = crate::keyenc::key(&id);
            if !keys.iter().any(|(x, _)| *x == k) {
                keys.push((k.clone(), id.clone()));
            }
            js.add(&k, &id, &vars)?;
        }
        keys.iter()
            .map(|(k, id)| {
                let mut out = doc! {"_id": id.clone()};
                js.finish(k, &mut out)?;
                Ok(out)
            })
            .collect()
    })
}

fn acc(extra: Document) -> Document {
    let mut a = doc! {
        "init": "function () { return {c: 0, s: 0} }",
        "accumulate": "function (st, v) { return {c: st.c + 1, s: st.s + (v || 0)} }",
        "accumulateArgs": ["$v"],
        "merge": "function (a, b) { return {c: a.c + b.c, s: a.s + b.s} }",
        "lang": "js",
    };
    for (k, v) in extra {
        a.insert(k, v);
    }
    doc! {"_id": "$k", "s": {"$accumulator": a}}
}

#[test]
fn accumulator_groups() {
    let r = group(
        acc(doc! {"finalize": "function (st) { return st.s / st.c }"}),
        "$k",
    )
    .unwrap();
    assert_eq!(
        r,
        [doc! {"_id": "a", "s": 1.5}, doc! {"_id": "b", "s": 2.5}]
    );
    let r = group(acc(doc! {}), "$k").unwrap();
    assert_eq!(r[0], doc! {"_id": "a", "s": {"c": 2.0, "s": 3.0}});
    // initArgs are evaluated against the `_id` document.
    let mut spec = acc(
        doc! {"init": "function (k) { return [k] }", "initArgs": ["$k"], "accumulate": "function (st, v) { st.push(v); return st }"},
    );
    let r = group(spec.clone(), "{k}").unwrap();
    assert_eq!(r[1], doc! {"_id": {"k": "b"}, "s": ["b", 5.0, null]});
    let r = group(spec.clone(), "$k").unwrap();
    assert_eq!(r[0], doc! {"_id": "a", "s": [null, 1.0, 2.0]});
    spec.insert("n", doc! {"$sum": 1});
    let code = |extra: Document| group(acc(extra), "$k").unwrap_err().code;
    assert_eq!(code(doc! {"accumulateArgs": "$v"}), 4_544_712);
    assert_eq!(code(doc! {"initArgs": "$k"}), 4_544_711);
    assert_eq!(code(doc! {"lang": "py"}), 4_544_705);
    assert_eq!(code(doc! {"lang": 1}), 4_544_704);
    assert_eq!(code(doc! {"foo": 1}), 4_544_706);
    assert_eq!(code(doc! {"init": 5}), 4_544_702);
    assert_eq!(code(doc! {"accumulate": "function () { throw 'nope' }"}), 8);
    let missing = |k: &str| {
        let mut s = acc(doc! {});
        s.get_document_mut("s")
            .unwrap()
            .get_document_mut("$accumulator")
            .unwrap()
            .remove(k);
        GroupJs::parse(&s).unwrap_err().code
    };
    assert_eq!(missing("init"), 4_544_707);
    assert_eq!(missing("accumulate"), 4_544_708);
    assert_eq!(missing("merge"), 4_544_709);
    assert_eq!(missing("accumulateArgs"), 4_544_710);
    assert!(GroupJs::parse(&acc(doc! {})).unwrap().has("s"));
}

#[test]
fn mentions() {
    assert!(mentions_js(
        &doc! {"find": "c", "filter": {"$and": [{"$where": "1"}]}}
    ));
    assert!(mentions_js(
        &doc! {"pipeline": [{"$group": {"_id": 1, "x": {"$accumulator": {}}}}]}
    ));
    assert!(!mentions_js(&doc! {"find": "c", "filter": {"a": "$where"}}));
}
