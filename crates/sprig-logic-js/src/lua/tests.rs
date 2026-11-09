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

fn two() -> ScriptSig {
    sig(&[("productId", ParamTy::Int), ("qty", ParamTy::Int)], false)
}

const QUOTE: &str = r"
local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', productId)
if p == nil or p == sprig.null or not (p.stock >= qty) then
  return sprig.event('error', { message = 'not enough stock' })
end
return { total = p.price * qty + 40 }
";

async fn go(
    src: &str,
    s: ScriptSig,
    args: Vec<Value>,
    db: Arc<Script>,
    limits: Limits,
) -> Result<(String, Value)> {
    let p = LuaProgram::new("f", src, s)?;
    run(&p, args, host(db), limits)
        .await
        .map(|o| (o.event, o.result))
}

#[tokio::test]
async fn the_chapter_example_in_lua() {
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
    let r = go(
        QUOTE,
        two(),
        vec![json!(7), json!(30)],
        Arc::new(Script::rows(&[("400.00", "10")])),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r, ("error".into(), json!({"message": "not enough stock"})));
    // A body that returns a function has it called.
    let r = go(
        "return function (a, b) return a * b end",
        sig(&[("a", ParamTy::Int), ("b", ParamTy::Int)], true),
        vec![json!(6), json!(7)],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r, ("output".into(), json!({"value": 42})));
}

#[tokio::test]
async fn writes_nulls_and_libraries() {
    let db = Arc::new(Script::default());
    let src = r#"
      local a = db.sql('INSERT INTO orders VALUES (?, ?, ?)', id, nil, note)
      local b = db.exec('DELETE FROM orders WHERE id = ?', id)
      local c = db.call('shop.other', 1)
      log('wrote', a.affected + b.affected)
      local t = cjson.decode('{"x": [1, 2.5, null]}')
      return { a = a, b = b, c = c, who = ctx.user, x = t.x, j = cjson.encode({1, 2}),
               n = sprig.null, r = math.random(10) <= 10 }"#;
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
               "c": {"event": "output", "fields": {"value": 7}}, "who": "u1",
               "x": [1, 2.5, null], "j": "[1,2]", "n": null, "r": true})
    );
    assert_eq!(
        db.seen(),
        [
            "INSERT INTO orders VALUES (5, NULL, _utf8mb4 X'69742773')",
            "DELETE FROM orders WHERE id = 5",
            "CALL `shop`.`other`(1)"
        ]
    );
}

#[tokio::test]
async fn errors_keep_their_codes() {
    let db = Arc::new(Script {
        fail: Some(SprigError::backend(
            1142,
            *b"42000",
            "SELECT command denied",
        )),
        ..Script::default()
    });
    let r = go(
        "return db.sql('SELECT 1')",
        sig(&[], false),
        vec![],
        Arc::clone(&db),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(1142));
    let r = go(
        "local ok, e = pcall(db.sql, 'SELECT 1') return { ok = ok, e = tostring(e) }",
        sig(&[], false),
        vec![],
        db,
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r.1["ok"], json!(false));
    assert!(r.1["e"].as_str().unwrap().contains("denied"), "{r:?}");
    let r = go(
        "error('boom')",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39865));
    assert!(r.unwrap_err().message.contains("f:1: boom"));
    let r = go(
        "pcall(sprig.reject, 'no discount') return 1",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39863));
    let r = go(
        "return 1",
        two(),
        vec![json!(1)],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39874));
    let e = LuaProgram::new("f", "return (1", sig(&[], false)).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicSyntax));
    let e = LuaProgram::new("f", "return 1", sig(&[("end", ParamTy::Int)], false)).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LogicSyntax));
}

fn quick() -> Limits {
    Limits {
        wall: Duration::from_millis(300),
        ..Limits::default()
    }
}

#[tokio::test]
async fn budgets_cant_be_caught() {
    let started = std::time::Instant::now();
    for src in [
        "while true do end",
        "while true do pcall(function () while true do end end) end",
        "while true do xpcall(function () while true do end end, function (e) return e end) end",
        "local co = coroutine.create(function () while true do end end) \
         while true do coroutine.resume(co) end",
        "local f = coroutine.wrap(function () while true do end end) while true do pcall(f) end",
        "local function f() local ok = pcall(f) while true do end end f()",
    ] {
        let r = go(
            src,
            sig(&[], false),
            vec![],
            Arc::new(Script::default()),
            quick(),
        )
        .await;
        assert_eq!(class(&r), Some(39864), "{src}: {r:?}");
        assert!(r.unwrap_err().message.contains("wall time"));
    }
    assert!(started.elapsed() < Duration::from_secs(8));
    let lim = Limits {
        memory_bytes: 8 << 20,
        ..Limits::default()
    };
    for src in [
        "local s = 'x' while true do s = s .. s end",
        "local s = 'x' while true do pcall(function () s = s .. s end) end",
        "local t = {} while true do t[#t + 1] = { 1, 2, 3 } end",
        "pcall(string.rep, 'x', 1e9) return 1",
    ] {
        let r = go(
            src,
            sig(&[], false),
            vec![],
            Arc::new(Script::default()),
            lim.clone(),
        )
        .await;
        assert_eq!(class(&r), Some(39864), "{src}: {r:?}");
        assert!(r.unwrap_err().message.contains("memory"), "{src}");
    }
    let lim = Limits {
        max_data_ops: 5,
        ..Limits::default()
    };
    let r = go(
        "for i = 1, 10 do pcall(db.sql, 'SELECT 1') end return 1",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        lim,
    )
    .await;
    assert_eq!(class(&r), Some(39864));
}

#[tokio::test]
async fn the_sandbox_has_no_way_out() {
    let r = go(
        r"return { io = type(io), os = type(os), debug = type(debug), package = type(package),
          require = type(require), dofile = type(dofile), loadfile = type(loadfile),
          load = type(load), dump = type(string.dump), module = type(module),
          ffi = type(ffi), jit = type(jit) }",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    for (k, v) in r.1.as_object().unwrap() {
        assert_eq!(v, "nil", "{k}");
    }
    // Bytecode can't be loaded; text can.
    let r = go(
        r"local f, e = loadstring('\27Lua\81\0\1\4\8\4\8\0') local g = loadstring('return 2')
          return { f = f == nil, e = e, g = g() }",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        r.1,
        json!({"f": true, "e": "binary chunks are not allowed", "g": 2})
    );
    // Loaded chunks get the same sandbox.
    let r = go(
        "return type(loadstring('return io')())",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(r.1["value"], "nil");
    let r = go(
        "return fetch('http://169.254.169.254/').status",
        sig(&[], false),
        vec![],
        Arc::new(Script::default()),
        Limits::default(),
    )
    .await;
    assert_eq!(class(&r), Some(39867));
}

#[test]
fn sandbox_options_for_redis_and_sinks() {
    let st = Arc::new(RunState::new(Limits::default()));
    let cfg = Config {
        deterministic: true,
        protect_globals: true,
        ..Config::default()
    };
    let seq = |sb: &Sandbox| -> Vec<f64> {
        sb.lua()
            .load("return { math.random(), math.random(100), math.random(5, 9) }")
            .eval::<Vec<f64>>()
            .unwrap()
    };
    let a = Sandbox::new(&cfg, Arc::clone(&st)).unwrap();
    a.seal().unwrap();
    let b = Sandbox::new(&cfg, Arc::clone(&st)).unwrap();
    assert_eq!(seq(&a), seq(&b));
    let e = a.lua().load("x = 1").exec().unwrap_err();
    assert!(e.to_string().contains("create global variable x"), "{e}");
    assert!(a.lua().load("local y = 1 return y").eval::<i64>().is_ok());
    let e = a.lua().load("setmetatable(_G, nil)").exec().unwrap_err();
    assert!(e.to_string().contains("protected metatable"), "{e}");
    // Without `deterministic`, two states differ.
    let c = Sandbox::new(&Config::default(), Arc::clone(&st)).unwrap();
    let d = Sandbox::new(&Config::default(), st).unwrap();
    assert_ne!(seq(&c), seq(&d));
}
