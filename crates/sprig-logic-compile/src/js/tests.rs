//! The JavaScript front end: what compiles, what goes to the engine tier
//! and why, and the lowered functions run in the reference interpreter.

use serde_json::json;

use super::*;
use crate::interp::{self, Value};
use crate::sql::StaticColumns;

fn cat() -> StaticColumns {
    StaticColumns::new()
        .table(
            "products",
            &[("id", Ty::Int), ("price", Ty::Float), ("stock", Ty::Int)],
        )
        .table(
            "orders",
            &[
                ("id", Ty::Int),
                ("product_id", Ty::Int),
                ("qty", Ty::Int),
                ("note", Ty::Str),
            ],
        )
}

fn sig(params: &[(&str, Ty)], returns: Option<Ty>) -> Signature {
    Signature {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        returns,
    }
}

fn js(src: &str, s: &Signature) -> Compiled {
    compile_with("f", src, s, &cat(), &Options::default()).unwrap()
}

fn reasons(c: &Compiled) -> Vec<String> {
    c.analysis.reasons.iter().map(|r| r.code.clone()).collect()
}

fn run(c: &Compiled, args: &[Value]) -> std::result::Result<(String, serde_json::Value), u16> {
    let f = c.ir.as_ref().unwrap();
    interp::run(f, args, &mut interp::NoDb, &interp::Limits::default())
        .map(|o| (o.event, o.result))
        .map_err(|e| e.code().map_or(0, sprig_common::SprigCode::number))
}

#[test]
fn branches_compile_and_follow_javascript() {
    let s = sig(&[("score", Ty::Int), ("vip", Ty::Bool)], None);
    let c = js(
        "if (score === null) return sprig.event('error', { message: 'no score' });
         if (score >= 90) return { grade: 'A', bonus: score + 5 };
         return { grade: score >= 50 ? 'pass' : 'fail', bonus: 0 };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(95), Value::Bool(true)]),
        Ok(("output".into(), json!({"grade": "A", "bonus": 100})))
    );
    assert_eq!(
        run(&c, &[Value::Int(40), Value::Bool(true)]),
        Ok(("output".into(), json!({"grade": "fail", "bonus": 0})))
    );
    assert_eq!(
        run(&c, &[Value::Null, Value::Bool(true)]),
        Ok(("error".into(), json!({"message": "no score"})))
    );
    let names: Vec<&str> =
        c.ir.as_ref()
            .unwrap()
            .events
            .iter()
            .map(|e| e.name.as_str())
            .collect();
    assert_eq!(names, ["output", "error"]);
}

#[test]
fn null_reads_as_zero_and_text_concatenates() {
    let s = sig(&[("a", Ty::Int), ("b", Ty::Float), ("t", Ty::Str)], None);
    let c = js(
        "let total = a * 2;
         total += b;
         const label = `n=${a}, t=` + t + '!';
         return { total, label, big: a >= 10, nt: !t };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    // `total` widened from an integer to a double for `+= b`.
    assert_eq!(
        run(&c, &[Value::Null, Value::Float(1.5), Value::Null]),
        Ok((
            "output".into(),
            json!({"total": 1.5, "label": "n=null, t=null!", "big": false, "nt": true})
        ))
    );
    assert_eq!(
        run(
            &c,
            &[Value::Int(12), Value::Null, Value::Str(String::new())]
        ),
        Ok((
            "output".into(),
            json!({"total": 24.0, "label": "n=12, t=!", "big": true, "nt": true})
        ))
    );
}

#[test]
fn loops_break_and_throw() {
    let s = sig(&[("n", Ty::Int)], Some(Ty::Int));
    let c = js(
        "export default function (n) {
           let sum = 0;
           for (let i = 0; i < 10; i++) {
             if (i >= n) break;
             sum = sum + i;
           }
           let k = 0;
           while (k < 3) { k++; }
           if (sum > 20) throw new Error('too big');
           return sum + k;
         }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(4)]),
        Ok(("output".into(), json!({"value": 9})))
    );
    assert_eq!(run(&c, &[Value::Int(9)]), Err(39865));
}

#[test]
fn rows_are_guarded_unless_tested() {
    let s = sig(&[("productId", Ty::Int), ("qty", Ty::Int)], None);
    let quote = js(
        "import { db, sprig } from 'sprig:db';
         export default function (productId, qty) {
           const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
           if (p === null || !(p.stock >= qty))
             return sprig.event('error', { message: 'not enough stock' });
           return { total: p.price * qty + 40 };
         }",
        &s,
    );
    assert!(quote.compilable(), "{:?}", quote.analysis.reasons);
    let text = quote.plsql.as_ref().unwrap().statements.join("\n");
    assert!(!text.contains("cannot read properties"), "{text}");
    let unguarded = js(
        "const p = db.sqlOne('SELECT price FROM products WHERE id = ?', [productId]);
         return { total: p.price * qty };",
        &s,
    );
    assert!(unguarded.compilable(), "{:?}", unguarded.analysis.reasons);
    let text = unguarded.plsql.as_ref().unwrap().statements.join("\n");
    assert!(
        text.contains("cannot read properties of null (reading ''price'')"),
        "{text}"
    );
    let conditional = js(
        "const p = db.sqlOne('SELECT price FROM products WHERE id = ?', [productId]);
         return { total: qty > 1 ? p.price : 0 };",
        &s,
    );
    assert_eq!(reasons(&conditional), ["js.null_row"]);
}

#[test]
fn what_runs_in_the_engine_tier() {
    let s = sig(&[("x", Ty::Int), ("t", Ty::Str)], None);
    for (src, code) in [
        (
            "try { return {a: 1}; } catch (e) { return {a: 2}; }",
            "js.try",
        ),
        ("return { u: t.toUpperCase() };", "js.nullable_receiver"),
        ("return { r: fetch('http://x') };", "js.call"),
        (
            "for (let i = 0; i < x; i++) { continue; } return {};",
            "js.continue",
        ),
        ("return { s: x == '1' };", "js.loose_equality"),
        ("return { s: t < 'b' };", "js.compare"),
        (
            "function h(a) { return a; } return { v: h(x) };",
            "js.helper",
        ),
        (
            "import fs from 'fs'; export default function (x, t) { return {}; }",
            "js.import",
        ),
        (
            "const r = db.sql('SELECT * FROM products'); return { r };",
            "db.select_star",
        ),
        (
            "const q = 'SELECT 1'; db.exec(q); return {};",
            "db.dynamic_sql",
        ),
    ] {
        let c = js(src, &s);
        assert!(
            reasons(&c).contains(&code.to_owned()),
            "{src}: {:?}",
            c.analysis.reasons
        );
        assert!(!c.compilable());
    }
    // A wrong number of SQL arguments is an error, as for KIRun.
    let e = compile_with(
        "f",
        "db.exec('DELETE FROM orders WHERE id = ?', [x, x]); return {};",
        &s,
        &cat(),
        &Options::default(),
    )
    .unwrap_err();
    assert_eq!(
        e.code(),
        Some(sprig_common::SprigCode::LogicSignatureMismatch)
    );
}

#[test]
fn for_of_reads_typed_columns() {
    let s = sig(&[("pid", Ty::Int)], None);
    let c = js(
        "let total = 0;
         let notes = '';
         for (const o of db.sql('SELECT qty, note FROM orders WHERE product_id = ? ORDER BY id', [pid])) {
           total += o.qty;
           notes = notes + o.note + ';';
         }
         return { total, notes };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
}
