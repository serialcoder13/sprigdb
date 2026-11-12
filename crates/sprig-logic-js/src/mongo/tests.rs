#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bson::{Bson, Document, bson, doc};

use super::{Call, Failure, call, reset_thread};

static OP: AtomicU64 = AtomicU64::new(1);

fn op() -> u64 {
    OP.fetch_add(1, Ordering::Relaxed)
}

fn run_in(
    op: u64,
    src: &str,
    this: Option<&Document>,
    args: &[Bson],
    as_bool: bool,
) -> Result<Bson, Failure> {
    call(&Call {
        op,
        src,
        this,
        args,
        as_bool,
        deadline: Instant::now() + Duration::from_secs(5),
        memory_bytes: 32 << 20,
    })
}

fn f(src: &str, args: &[Bson]) -> Result<Bson, Failure> {
    run_in(op(), src, None, args, false)
}

fn w(src: &str, d: &Document) -> bool {
    run_in(op(), src, Some(d), &[], true).unwrap() == Bson::Boolean(true)
}

#[test]
fn where_forms() {
    let d = doc! {"a": 5, "l": 7_i64, "s": "x"};
    assert!(w("this.a > 2", &d));
    assert!(!w("this.a > 9", &d));
    assert!(w("obj.a == 5", &d));
    assert!(w("function () { return this.a == 5 }", &d));
    assert!(w("  function () { return this.a == 5 };  ", &d));
    assert!(w("var x = 2; this.a > x", &d));
    assert!(w("return this.a === 5", &d));
    assert!(w("this.l == 7", &d));
    assert!(w("5", &d));
    assert!(!w("''", &d));
    assert!(!w("function () {}", &d));
    assert!(w("return {}", &d));
}

#[test]
fn values_in() {
    let oid = bson::oid::ObjectId::parse_str("5f0000000000000000000001").unwrap();
    let r = f(
        "function (l, a, dec, dt, o, arr, u) { return [typeof l, typeof a, typeof dec, String(dt instanceof Date), o.str, o.toString(), l.toString(), dec.toString(), String(Array.isArray(arr)), String(l + 1), String(u), JSON.stringify(l), JSON.stringify(o)] }",
        &[
            Bson::Int64(5),
            Bson::Int32(1),
            Bson::Decimal128("1.5".parse().unwrap()),
            Bson::DateTime(bson::DateTime::from_millis(1000)),
            Bson::ObjectId(oid),
            bson!([1, 2]),
            Bson::Null,
        ],
    )
    .unwrap();
    assert_eq!(
        r,
        bson!([
            "object",
            "number",
            "object",
            "true",
            "5f0000000000000000000001",
            "ObjectId(\"5f0000000000000000000001\")",
            "NumberLong(5)",
            "NumberDecimal(\"1.5\")",
            "true",
            "6",
            "null",
            "{\"$numberLong\":\"5\"}",
            "{\"$oid\":\"5f0000000000000000000001\"}"
        ])
    );
}

#[test]
fn values_out() {
    assert_eq!(
        f("function () { return 1 }", &[]).unwrap(),
        Bson::Double(1.0)
    );
    assert_eq!(
        f("function () { return undefined }", &[]).unwrap(),
        Bson::Null
    );
    assert_eq!(
        f("function () { return NumberLong(3) }", &[]).unwrap(),
        Bson::Int64(3)
    );
    assert_eq!(
        f("function () { return NumberInt(3) }", &[]).unwrap(),
        Bson::Int32(3)
    );
    assert_eq!(
        f("function () { return NumberDecimal('2.5') }", &[]).unwrap(),
        Bson::Decimal128("2.5".parse().unwrap())
    );
    assert_eq!(
        f("function () { return new Date(5) }", &[]).unwrap(),
        Bson::DateTime(bson::DateTime::from_millis(5))
    );
    assert_eq!(
        f("function () { return new Date(NaN) }", &[]).unwrap(),
        Bson::DateTime(bson::DateTime::from_millis(0))
    );
    assert_eq!(
        f("function () { return Timestamp(5, 2) }", &[]).unwrap(),
        Bson::Timestamp(bson::Timestamp {
            time: 5,
            increment: 2
        })
    );
    assert_eq!(
        f("function () { return MinKey }", &[]).unwrap(),
        Bson::MinKey
    );
    assert_eq!(
        f("function () { return MaxKey() }", &[]).unwrap(),
        Bson::MaxKey
    );
    assert_eq!(
        f(
            "function () { return {x: undefined, y: [undefined, 's']} }",
            &[]
        )
        .unwrap(),
        bson!({"x": null, "y": [null, "s"]})
    );
    assert_eq!(
        f("function () { return hex_md5('abc') }", &[]).unwrap(),
        Bson::String("900150983cd24fb0d6963f7d28e17f72".into())
    );
    assert_eq!(
        f("function () { return Object.bsonsize({a: 1}) }", &[]).unwrap(),
        Bson::Double(16.0)
    );
    assert_eq!(f("1 + 2", &[]).unwrap(), Bson::Double(3.0));
    assert_eq!(f("return 7", &[]).unwrap(), Bson::Double(7.0));
    assert!(matches!(
        f("(a) => a * 3", &[]).unwrap(),
        Bson::JavaScriptCode(_)
    ));
    assert_eq!(
        f("function () { return 5n }", &[]),
        Err(Failure::Error(2, "unable to get type".into()))
    );
}

#[test]
fn int_fields_of_documents_stay_int() {
    let sub = bson!({"q": 1, "r": 2.0});
    assert_eq!(
        f("function (s) { return s }", std::slice::from_ref(&sub)).unwrap(),
        sub
    );
    assert_eq!(
        f(
            "function (s) { s.z = 2; s.q = 7; return s }",
            std::slice::from_ref(&sub)
        )
        .unwrap(),
        bson!({"q": 7, "r": 2.0, "z": 2.0})
    );
    assert_eq!(
        f(
            "function (s) { s.q = 7.5; return s }",
            std::slice::from_ref(&sub)
        )
        .unwrap(),
        bson!({"q": 7.5, "r": 2.0})
    );
    // A copy or an array doesn't keep them.
    assert_eq!(
        f("function (s) { return {q: s.q} }", &[sub]).unwrap(),
        bson!({"q": 1.0})
    );
    assert_eq!(
        f("function (a) { return a }", &[bson!([1, 2])]).unwrap(),
        bson!([1.0, 2.0])
    );
}

#[test]
fn errors_as_mongodb_reports_them() {
    let e = |src: &str| f(src, &[]).unwrap_err();
    assert!(
        matches!(e("function () { throw new Error('boom') }"), Failure::Error(139, m) if m.starts_with("Error: boom :\n@:"))
    );
    assert_eq!(
        e("function () { throw 'x' }"),
        Failure::Error(8, "uncaught exception: x".into())
    );
    assert!(
        matches!(e("function () { throw {a: 1} }"), Failure::Error(139, m) if m == "uncaught exception: [object Object]")
    );
    assert!(
        matches!(e("function () { return undefinedVar }"), Failure::Error(139, m) if m.starts_with("ReferenceError: "))
    );
    assert!(matches!(e("function ( {"), Failure::Error(139, m) if m.starts_with("SyntaxError: ")));
    assert!(matches!(e("this.a >"), Failure::Error(139, m) if m.starts_with("SyntaxError: ")));
}

#[test]
fn no_db_or_host_reach() {
    assert_eq!(
        f("function () { return [typeof db, typeof require, typeof fetch, typeof __sprig_mongo_host, typeof __sprig_mk] }", &[]).unwrap(),
        bson!(["undefined", "undefined", "undefined", "undefined", "undefined"])
    );
}

#[test]
fn globals_last_for_the_operation_only() {
    let o = op();
    let src = "function () { if (typeof gx === 'undefined') { gx = 0 } gx++; return gx }";
    assert_eq!(run_in(o, src, None, &[], false).unwrap(), Bson::Double(1.0));
    assert_eq!(run_in(o, src, None, &[], false).unwrap(), Bson::Double(2.0));
    assert_eq!(
        run_in(op(), src, None, &[], false).unwrap(),
        Bson::Double(1.0)
    );
}

#[test]
fn deadline_and_memory() {
    let t0 = Instant::now();
    let r = call(&Call {
        op: op(),
        src: "function () { while (true) {} }",
        this: None,
        args: &[],
        as_bool: false,
        deadline: Instant::now() + Duration::from_millis(100),
        memory_bytes: 32 << 20,
    });
    assert_eq!(r, Err(Failure::Interrupted));
    assert!(t0.elapsed() < Duration::from_secs(2));
    // `try`/`catch` can't outlive it.
    let r = call(&Call {
        op: op(),
        src: "function () { for (;;) { try { while (true) {} } catch (e) {} } }",
        this: None,
        args: &[],
        as_bool: false,
        deadline: Instant::now() + Duration::from_millis(100),
        memory_bytes: 32 << 20,
    });
    assert_eq!(r, Err(Failure::Interrupted));
    let r = call(&Call {
        op: op(),
        src: "function () { var a = []; while (true) { a.push(new Array(100000).fill('x')) } }",
        this: None,
        args: &[],
        as_bool: false,
        deadline: Instant::now() + Duration::from_secs(10),
        memory_bytes: 16 << 20,
    });
    assert_eq!(r, Err(Failure::Error(139, "Out of memory".into())));
    // The thread's next operation works.
    assert_eq!(
        f("function () { return 2 }", &[]).unwrap(),
        Bson::Double(2.0)
    );
    reset_thread();
}
