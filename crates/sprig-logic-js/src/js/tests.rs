#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::time::Duration;

use serde_json::json;

use super::*;
use crate::engine::ParamTy;
use crate::testutil::{Script, class, host};

fn sig(params: &[(&str, ParamTy)], scalar: bool) -> ScriptSig {
    ScriptSig {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        scalar,
    }
}

const QUOTE: &str = r"
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || !(p.stock >= qty))
    return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}
";

async fn go(
    src: &str,
    s: ScriptSig,
    args: Vec<Value>,
    db: Arc<Script>,
    limits: Limits,
) -> Result<(String, Value)> {
    let p = JsProgram::new("f", src, s)?;
    run(&p, args, host(db), limits)
        .await
        .map(|o| (o.event, o.result))
}

fn two() -> ScriptSig {
    sig(&[("productId", ParamTy::Int), ("qty", ParamTy::Int)], false)
}

#[tokio::test]
async fn the_chapter_example_as_a_module() {
    let db = Arc::new(Script::rows(&[("400.00", "10")]));
    let r = go(
        QUOTE,
        two(),
        vec![json!(7), json!(3)],
        Arc::clone(&db),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r, ("output".into(), json!({"total": 1240})));
    assert_eq!(
        db.seen(),
        ["SELECT price, stock FROM products WHERE id = 7"]
    );
    let db = Arc::new(Script::rows(&[("400.00", "1")]));
    let r = go(QUOTE, two(), vec![json!(7), json!(3)], db, Limits::default())
        .await
        .unwrap();
    assert_eq!(
        r,
        ("error".into(), json!({"message": "not enough stock"}))
    );
}

#[tokio::test]
async fn body_form_scalar_functions_and_writes() {
    let src = "return price * qty + 40;";
    let s = sig(&[("price", ParamTy::Float), ("qty", ParamTy::Int)], true);
    let r = go(
        src,
        s,
        vec![json!(2.5), json!(4)],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r, ("output".into(), json!({"value": 50})));
    // DML through db.sql gives {affected}; db.exec too; db.call {event, fields}.
    let db = Arc::new(Script::default());
    let src = r"
      const a = db.sql('INSERT INTO orders VALUES (?, ?)', [id, note]);
      const b = db.exec('DELETE FROM orders WHERE id = ?', [id]);
      const c = db.call('other', 1, 'x');
      console.log('wrote', a.affected + b.affected);
      return { a, b, c, who: ctx.user, db: ctx.database };";
    let s = sig(&[("id", ParamTy::Int), ("note", ParamTy::Str)], false);
    let r = go(
        src,
        s,
        vec![json!(5), json!("it's")],
        Arc::clone(&db),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        r.1,
        json!({"a": {"affected": 1}, "b": {"affected": 1},
               "c": {"event": "output", "fields": {"value": 7}}, "who": "u1", "db": "shop"})
    );
    assert_eq!(
        db.seen(),
        [
            "INSERT INTO orders VALUES (5, _utf8mb4 X'69742773')",
            "DELETE FROM orders WHERE id = 5",
            "CALL `other`(1, _utf8mb4 X'78')"
        ]
    );
    // An async function's promise is settled.
    let r = go(
        "export default async function () { return { n: 1 }; }",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r.1, json!({"n": 1}));
}

#[tokio::test]
async fn errors_keep_their_codes() {
    // A db error the code doesn't catch comes back unchanged.
    let db = Arc::new(Script {
        fail: Some(SprigError::backend(
            1142,
            *b"42000",
            "SELECT command denied to user 'u1'",
        )),
        ..Script::default()
    });
    let r = go(
        "return db.sql('SELECT 1');",
        sig(&[], false),
        vec![],
        Arc::clone(&db),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(1142));
    // Caught, it is an Error with code and sqlState.
    let r = go(
        "try { db.sql('SELECT 1'); } catch (e) { return { code: e.code, state: e.sqlState }; }",
        sig(&[], false),
        vec![],
        db,
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r.1, json!({"code": 1142, "state": "42000"}));
    // A thrown error is 39865 with its message.
    let r = go(
        "throw new Error('boom');",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39865));
    assert!(r.unwrap_err().message.contains("boom"));
    // sprig.reject is 39863, even when caught.
    let r = go(
        "try { sprig.reject('no discount'); } catch (e) {} return 1;",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39863));
    assert_eq!(r.unwrap_err().message, "no discount");
    // NaN and infinities can't leave.
    for src in ["return 0/0;", "return { a: [1, Infinity] };", "return 10n;"] {
        let r = go(
            src,
            sig(&[], true),
            vec![],
            Arc::new(Script::default()),
            Limits::default(),
        )
        .await;
        assert_eq!(class(&r), Some(39866), "{src}");
    }
    // Arity, syntax, imports.
    let r = go(
        "return 1;",
        two(),
        vec![json!(1)],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39874));
    let e = JsProgram::new("f", "return (1;", sig(&[], false)).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicSyntax));
    let e = JsProgram::new("f", "return 1;", sig(&[("class", ParamTy::Int)], false)).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicSyntax));
    let r = go(
        "import * as os from 'os';\nexport default function () { return os.getcwd(); }",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39861));
}

fn quick() -> Limits {
    Limits {
        wall: Duration::from_millis(300),
        ..Limits::default()
    }
}

#[tokio::test]
async fn budgets_cant_be_caught() {
    let start = std::time::Instant::now();
    for src in [
        "while (true) {}",
        "for (;;) { try { while (true) {} } catch (e) {} finally { } }",
        "const f = () => { try { for(;;){} } catch (e) { return f(); } }; f();",
    ] {
        let r = go(src, sig(&[], false), vec![], Arc::new(Script::default()), quick()).await;
        assert_eq!(class(&r), Some(39864), "{src}");
        assert!(r.unwrap_err().message.contains("wall time"));
    }
    assert!(start.elapsed() < Duration::from_secs(5));
    // Memory: arrays past 8 MB, with the error caught.
    let lim = Limits {
        memory_bytes: 8 << 20,
        ..Limits::default()
    };
    let r = go(
        "const a = []; for (;;) { try { a.push(new Array(10000).fill(1)); } catch (e) {} }",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        lim.clone(),
    )
    .await;
    assert_eq!(class(&r), Some(39864), "{r:?}");
    assert!(r.unwrap_err().message.contains("memory"));
    let r = go(
        "const a = []; for (;;) a.push({ x: [1, 2, 3] });",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        lim,
    )
    .await;
    assert!(format!("{r:?}").contains("memory"), "{r:?}");
    // Data operations.
    let lim = Limits {
        max_data_ops: 5,
        ..Limits::default()
    };
    let r = go(
        "for (let i = 0; i < 10; i++) { try { db.sql('SELECT 1'); } catch (e) {} } return 1;",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        lim,
    )
    .await;
    assert_eq!(class(&r), Some(39864));
    assert!(r.unwrap_err().message.contains("max_data_ops"));
    // Deep recursion is a RangeError the code may catch; uncaught, 39865.
    let r = go(
        "const f = (n) => f(n + 1) + 1; return f(0);",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39865));
}

#[tokio::test]
async fn the_sandbox_has_no_way_out() {
    let r = go(
        r"return {
          process: typeof process, require: typeof require, std: typeof std, os: typeof os,
          setTimeout: typeof setTimeout, Deno: typeof Deno, scriptArgs: typeof scriptArgs,
          WebAssembly: typeof WebAssembly, XMLHttpRequest: typeof XMLHttpRequest,
          host: typeof __sprig_host, invoke: typeof __sprig_invoke,
        };",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    for (k, v) in r.1.as_object().unwrap() {
        assert_eq!(v, "undefined", "{k}");
    }
    // A dynamic import of anything but sprig:db fails.
    let r = go(
        "export default async function () { const m = await import('os'); return 1; }",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert!(r.is_err());
    // fetch is refused off the allowlist (here: none).
    let r = go(
        "return fetch('http://169.254.169.254/latest/meta-data/').status;",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39867));
    // ... through the real client too.
    let p = JsProgram::new(
        "f",
        "return fetch('http://169.254.169.254/latest/meta-data/').status;",
        sig(&[], false),
    )
    .unwrap();
    let mut h = host(Arc::new(Script::default()));
    h.outbound = Arc::new(crate::outbound::HttpOutbound::new(
        crate::outbound::OutboundPolicy {
            allow: vec!["*.example.com".into()],
            timeout: Duration::from_millis(500),
            max_bytes: 1 << 20,
        },
    ));
    let e = run(&p, vec![], h, Limits::default()).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicOutboundDenied));
}

#[tokio::test]
async fn a_dropped_call_stops() {
    let p = JsProgram::new("f", "while (true) {}", sig(&[], false)).unwrap();
    let lim = Limits {
        wall: Duration::from_secs(60),
        ..Limits::default()
    };
    let started = std::time::Instant::now();
    let r = tokio::time::timeout(
        Duration::from_millis(200),
        run(&p, vec![], host(Arc::new(Script::default())), lim),
    )
    .await;
    assert!(r.is_err());
    // The engine thread is free again soon: a short call runs.
    let p = JsProgram::new("f", "return 1;", sig(&[], true)).unwrap();
    let r = run(
        &p,
        vec![],
        host(Arc::new(Script::default())),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r.result, json!({"value": 1}));
    assert!(started.elapsed() < Duration::from_secs(10));
}
