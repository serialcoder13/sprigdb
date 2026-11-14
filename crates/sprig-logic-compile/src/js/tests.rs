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
        row: None,
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
fn switch_and_continue_compile() {
    let s = sig(&[("n", Ty::Int), ("kind", Ty::Str)], None);
    let c = js(
        "let sum = 0;
         for (let i = 0; i < n; i++) {
           if (i % 2 === 1) continue;
           sum = sum + i;
         }
         let k = 0;
         let odd = 0;
         while (k < n) {
           k++;
           if (k % 2 === 0) { continue; }
           odd++;
         }
         let label = '';
         switch (kind) {
           case 'a':
           case 'b':
             label = 'early';
             break;
           case 'z':
             return { sum, odd, label: 'last' };
           default:
             label = 'other';
         }
         let size = 'big';
         switch (n) {
           case 0: size = 'none'; break;
           case 1: size = 'one'; break;
         }
         return { sum, odd, label, size };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let text = c.plsql.as_ref().unwrap().statements.join("\n");
    assert!(text.contains("CONTINUE;"), "{text}");
    let r = |n: i64, k: &str| run(&c, &[Value::Int(n), Value::Str(k.into())]);
    // 0 + 2 + 4 = 6 below 6; odd k in 1..=6: 1, 3, 5.
    assert_eq!(
        r(6, "b"),
        Ok((
            "output".into(),
            json!({"sum": 6, "odd": 3, "label": "early", "size": "big"})
        ))
    );
    assert_eq!(
        r(1, "q"),
        Ok((
            "output".into(),
            json!({"sum": 0, "odd": 1, "label": "other", "size": "one"})
        ))
    );
    assert_eq!(
        r(0, "z"),
        Ok((
            "output".into(),
            json!({"sum": 0, "odd": 0, "label": "last"})
        ))
    );
    assert_eq!(
        r(0, "a"),
        Ok((
            "output".into(),
            json!({"sum": 0, "odd": 0, "label": "early", "size": "none"})
        ))
    );
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
            "outer: for (let i = 0; i < x; i++) { continue outer; } return {};",
            "js.statement",
        ),
        (
            "switch (x) { case 1: x = 2; case 2: return {a: x}; } return {};",
            "js.switch",
        ),
        (
            "switch (x) { case 1: if (t === 'a') break; return {a: 1}; } return {};",
            "js.switch",
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

// ---- validations and after-commit triggers (jsbind) ---------------------------

fn sales() -> StaticColumns {
    cat().table(
        "sales",
        &[
            ("id", Ty::Int),
            ("status", Ty::Str),
            ("total", Ty::Float),
            ("discount_pct", Ty::Float),
            ("qty", Ty::Int),
        ],
    )
}

fn bound(src: &str) -> sprig_common::Result<Compiled> {
    compile_with(
        "v",
        src,
        &Signature::row_of("sales"),
        &sales(),
        &Options::default(),
    )
}

/// Runs `c` with its parameters (the columns it reads) taken from `args`
/// by name; missing ones are `NULL`. An error is its message (the
/// interpreter reports 39863 and 39865 alike).
fn run_named(c: &Compiled, args: &[(&str, Value)]) -> std::result::Result<String, String> {
    let f = c.ir.as_ref().unwrap();
    let vals: Vec<Value> = f
        .params
        .iter()
        .map(|p| {
            args.iter()
                .find(|(n, _)| *n == p.name)
                .map_or(Value::Null, |(_, v)| v.clone())
        })
        .collect();
    interp::run(f, &vals, &mut interp::NoDb, &interp::Limits::default())
        .map(|o| o.event)
        .map_err(|e| e.message)
}

fn rejected(m: &str) -> std::result::Result<String, String> {
    Err(m.to_owned())
}

fn param_names(c: &Compiled) -> Vec<String> {
    let mut v: Vec<String> =
        c.ir.as_ref()
            .unwrap()
            .params
            .iter()
            .map(|p| p.name.clone())
            .collect();
    v.sort();
    v
}

#[test]
fn the_books_javascript_validation_compiles() {
    // A row validation in the `(row, old)` form, as users write it.
    let c = bound(
        r#"export default function (row, old) {
  if (row.discount_pct > 0) {
    const limit = row.total >= 1000 ? 20 : 10;
    if (row.discount_pct > limit)
      sprig.reject(`discount ${row.discount_pct}% is above ${limit}%`);
  }
  if (old !== null && old.status === "shipped" && row.total !== old.total)
    sprig.reject("the total of a shipped order can't change");
}"#,
    )
    .unwrap();
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    // The parameters are the columns it reads, and op for old's presence.
    assert_eq!(
        param_names(&c),
        ["discount_pct", "old_status", "old_total", "op", "total"]
    );
    let ins = |d: Value, t: f64| {
        run_named(
            &c,
            &[
                ("discount_pct", d),
                ("total", Value::Float(t)),
                ("op", Value::Str("insert".into())),
            ],
        )
    };
    assert_eq!(
        ins(Value::Float(25.0), 600.0),
        rejected("discount 25% is above 10%")
    );
    assert_eq!(
        ins(Value::Float(25.0), 1600.0),
        rejected("discount 25% is above 20%")
    );
    assert_eq!(ins(Value::Float(15.0), 1600.0), Ok("output".into()));
    assert_eq!(ins(Value::Float(5.0), 600.0), Ok("output".into()));
    // null > 0 is false in JavaScript, as NULL > 0 skips the branch.
    assert_eq!(ins(Value::Null, 600.0), Ok("output".into()));
    let upd = |status: &str, before: f64, after: f64| {
        run_named(
            &c,
            &[
                ("discount_pct", Value::Null),
                ("total", Value::Float(after)),
                ("op", Value::Str("update".into())),
                ("old_status", Value::Str(status.into())),
                ("old_total", Value::Float(before)),
            ],
        )
    };
    assert_eq!(
        upd("shipped", 10.0, 12.0),
        rejected("the total of a shipped order can't change")
    );
    assert_eq!(upd("shipped", 10.0, 10.0), Ok("output".into()));
    assert_eq!(upd("new", 10.0, 12.0), Ok("output".into()));
}

#[test]
fn a_one_message_javascript_validation_is_a_check() {
    let c = bound(
        "export default function (row) {
           if (row.qty <= 0) sprig.reject('qty must be positive');
         }",
    )
    .unwrap();
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(param_names(&c), ["qty"]);
    let (cond, msg) = crate::pure::reject_check(
        c.ir.as_ref().unwrap(),
        &[Some(crate::pure::Sql {
            text: "`qty`".into(),
            ty: Ty::Int,
        })],
    )
    .unwrap();
    assert_eq!(msg, "qty must be positive");
    assert!(cond.contains("`qty`"), "{cond}");
    // Two messages: triggers, not a CHECK.
    let two = bound(
        "if (row.qty <= 0) sprig.reject('none');
         if (row.qty > 100) sprig.reject('too many');",
    )
    .unwrap();
    assert!(two.compilable(), "{:?}", two.analysis.reasons);
    let e = crate::pure::reject_check(
        two.ir.as_ref().unwrap(),
        &[Some(crate::pure::Sql {
            text: "`qty`".into(),
            ty: Ty::Int,
        })],
    )
    .unwrap_err();
    assert!(e.contains("more than one message"), "{e}");
}

#[test]
fn the_three_ways_of_seeing_the_row_agree() {
    let forms = [
        // A test run's ctx.
        "export default function (ctx) {
           if (ctx.trigger.before !== null && ctx.trigger.before.qty > ctx.trigger.after.qty)
             sprig.reject('qty only grows');
         }",
        // KIRun's names.
        "export default function (qty, old_qty, op) {
           if (op === 'update' && old_qty > qty) sprig.reject('qty only grows');
         }",
        // A body alone.
        "if (old && old.qty > row.qty) sprig.reject('qty only grows');",
    ];
    for src in forms {
        let c = bound(src).unwrap();
        assert!(c.compilable(), "{src}: {:?}", c.analysis.reasons);
        assert_eq!(param_names(&c), ["old_qty", "op", "qty"], "{src}");
        let r = |op: &str, before: Value, after: i64| {
            run_named(
                &c,
                &[
                    ("op", Value::Str(op.into())),
                    ("old_qty", before),
                    ("qty", Value::Int(after)),
                ],
            )
        };
        assert_eq!(
            r("update", Value::Int(5), 3),
            rejected("qty only grows"),
            "{src}"
        );
        assert_eq!(r("update", Value::Int(5), 7), Ok("output".into()), "{src}");
        assert_eq!(r("insert", Value::Null, 3), Ok("output".into()), "{src}");
    }
}

#[test]
fn old_is_null_for_an_insert() {
    // Reading a column of old without testing it: JavaScript's TypeError
    // on an insert, as QuickJS would throw.
    let c = bound("if (old.status === 'shipped') sprig.reject('shipped');").unwrap();
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let r = |op: &str| {
        run_named(
            &c,
            &[
                ("op", Value::Str(op.into())),
                ("old_status", Value::Str("shipped".into())),
            ],
        )
    };
    assert_eq!(
        r("insert"),
        rejected("TypeError: cannot read properties of null (reading 'status')")
    );
    assert_eq!(r("update"), rejected("shipped"));
    // A column the table lacks, and a parameter that names nothing.
    let c = bound("if (row.nope > 1) sprig.reject('x');").unwrap();
    assert_eq!(reasons(&c), ["js.member"]);
    let Err(e) = bound("export default function (row, extra) { return; }") else {
        panic!("an unknown parameter compiled");
    };
    assert_eq!(
        e.code(),
        Some(sprig_common::SprigCode::LogicSignatureMismatch)
    );
}

#[test]
fn nondeterministic_calls_are_reasons() {
    let s = sig(&[("price", Ty::Float)], Some(Ty::Float));
    for (src, what) in [
        ("return price * Math.random();", "Math.random()"),
        ("return Date.now() + price;", "Date.now()"),
        ("const d = new Date(); return price;", "new Date()"),
    ] {
        let c = js(src, &s);
        assert_eq!(reasons(&c), ["js.nondeterministic"], "{src}");
        assert!(c.analysis.reasons[0].detail.contains(what), "{src}");
    }
    // A local named Math is the code's own.
    let c = js(
        "const Math = { random: 1 }; return price;",
        &sig(&[("price", Ty::Float)], Some(Ty::Float)),
    );
    assert!(!reasons(&c).contains(&"js.nondeterministic".to_owned()));
}

#[test]
fn a_javascript_function_is_a_computed_expression() {
    let s = sig(&[("price", Ty::Float), ("stock", Ty::Int)], Some(Ty::Float));
    let c = js(
        "export default function (price, stock) {
           if (stock <= 0) return 0;
           return price * stock - 1;
         }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let e = crate::pure::computed(
        c.ir.as_ref().unwrap(),
        &[
            Some(crate::pure::Sql {
                text: "(`price`)".into(),
                ty: Ty::Float,
            }),
            Some(crate::pure::Sql {
                text: "(`stock`)".into(),
                ty: Ty::Int,
            }),
        ],
    )
    .unwrap();
    assert!(e.text.starts_with("(CASE WHEN"), "{}", e.text);
    assert!(
        e.text.contains("`price`") && e.text.contains("`stock`"),
        "{}",
        e.text
    );
}
