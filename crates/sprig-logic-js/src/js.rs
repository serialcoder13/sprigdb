//! JavaScript functions on QuickJS (chapter 41, "The fallback runtime";
//! build step 12): `CREATE FUNCTION ... LANGUAGE javascript AS $$ … $$`
//! runs here when placement puts it in the engine tier, which, until the
//! JavaScript front end of the compiler exists, is always.
//!
//! **Two forms of body.** A body with `export` or `import` statements is
//! a module whose default export is the function (`import { db } from
//! 'sprig:db'` is the only import); any other body is the function's
//! body, MySQL 9 style, with the declared parameters in scope by name:
//!
//! ```text
//! CREATE FUNCTION total(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE js AS $$
//!   return price * qty + 40;
//! $$
//! CREATE PROCEDURE quote(product_id BIGINT, qty INT) LANGUAGE js AS $$
//!   export default function (productId, qty) {
//!     const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
//!     if (p === null || p.stock < qty)
//!       return sprig.event('error', { message: 'not enough stock' });
//!     return { total: p.price * qty + 40 };
//!   }
//! $$
//! ```
//!
//! **What code can reach.** `db.sql(text, args?)` (rows; DML gives
//! `{affected}`), `db.sqlOne` (a row or `null`), `db.exec` (`{affected}`),
//! `db.call(name, ...args)` (`{event, fields}`), `fetch(url, init)`
//! (synchronous; `{status, ok, headers, body, text(), json()}`), `ctx`
//! (`user`, `tenant`, `database`, `function`, `version`, `log`),
//! `console.log`, `sprig.reject(message)` (39863) and `sprig.event(name,
//! fields)`. Everything goes through the same [`SyncHost`] as KIRun's
//! `Sprig.Db.*`: statements run as the caller, with the caller's rights
//! and transaction. QuickJS here has no file system, network, timers,
//! workers or `std`/`os` modules; `fetch` is the only way out, through
//! `logic.outbound_allowlist`.
//!
//! **Budgets.** Each call gets a fresh runtime with a counting allocator
//! (`logic.memory_limit_mb`: an allocation past it fails and trips the
//! run) and an interrupt handler that stops the run on its wall time,
//! a trip (a budget, `sprig.reject`) or cancellation. QuickJS can't catch
//! an interrupt, so `try`/`catch` can't outlive a budget.
//!
//! **Values.** Arguments and results cross as JSON: `undefined` is
//! `null`, dates are ISO strings, and a `NaN`, an infinity or a `BigInt`
//! in the result is 39866, as in the compiled tier.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rquickjs::allocator::{Allocator, RustAllocator};
use rquickjs::loader::{BuiltinLoader, BuiltinResolver};
use rquickjs::{Context, Ctx, Function, Module, Runtime, Value as JsValue};
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};

use crate::bridge::Shape;
use crate::budget::{Budget, Limits, RunState};
use crate::engine::{self, ScriptSig, SyncHost};
use crate::kirun::{Host, Outcome};

/// JavaScript's reserved words: parameters with these names can't be
/// bound by name in a body-form function.
const RESERVED: &[&str] = &[
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "import",
    "in",
    "instanceof",
    "new",
    "null",
    "return",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "void",
    "while",
    "with",
    "yield",
    "let",
    "static",
    "implements",
    "interface",
    "package",
    "private",
    "protected",
    "public",
    "arguments",
    "eval",
];

/// The `sprig:db` module.
const SPRIG_DB: &str = "export const db = globalThis.db;\n\
export const sprig = globalThis.sprig;\n\
export const ctx = globalThis.ctx;\n\
export const fetch = globalThis.fetch;\n\
export const console = globalThis.console;\n\
export default globalThis.db;\n";

/// Runs before the code: wraps the one host function in the `db`,
/// `sprig`, `ctx`, `fetch` and `console` objects, and leaves two helpers
/// that [`run`] takes and removes before the code runs.
const PRELUDE: &str = r#"(() => {
  "use strict";
  const host = globalThis.__sprig_host;
  delete globalThis.__sprig_host;
  const parse = JSON.parse, stringify = JSON.stringify;
  function call(op, payload) {
    const r = parse(host(op, stringify(payload === undefined ? null : payload)));
    if (r.err) {
      const e = new Error(r.err.message);
      e.code = r.err.code;
      e.sqlState = r.err.sqlState;
      Object.defineProperty(e, "__sprig", { value: r.err.id });
      throw e;
    }
    return r.ok;
  }
  const args = (a) => (a === undefined || a === null ? [] : Array.isArray(a) ? a : [a]);
  const text = (xs) => xs.map((x) => (typeof x === "string" ? x : stringify(x) ?? String(x))).join(" ");
  globalThis.db = Object.freeze({
    sql: (sql, a) => call("sql", [String(sql), args(a)]),
    sqlOne: (sql, a) => call("sqlOne", [String(sql), args(a)]),
    exec: (sql, a) => call("exec", [String(sql), args(a)]),
    call: (name, ...a) => call("call", [String(name), a]),
  });
  globalThis.sprig = Object.freeze({
    reject: (message) => { call("reject", [String(message)]); },
    event: (name, fields) => ({ __sprig_event: String(name), fields: fields === undefined ? {} : fields }),
  });
  const log = (...xs) => { call("log", [text(xs)]); };
  globalThis.console = Object.freeze({ log, info: log, warn: log, error: log, debug: log });
  globalThis.fetch = (url, init) => {
    const r = call("fetch", [String(url), init === undefined ? null : init]);
    r.ok = r.status >= 200 && r.status < 300;
    r.text = () => r.body;
    r.json = () => parse(r.body);
    return r;
  };
  globalThis.ctx = Object.freeze(Object.assign(call("info", null) ?? {}, { log }));
  globalThis.__sprig_invoke = (f, a) => f(...parse(a));
  globalThis.__sprig_out = (v) => {
    let bad = false;
    const s = stringify(v, (k, x) => {
      if ((typeof x === "number" && !Number.isFinite(x)) || typeof x === "bigint") { bad = true; return null; }
      return x;
    });
    return bad ? "\u0000nonfinite" : s;
  };
})();
"#;

/// A JavaScript function, checked at deploy and ready to run.
#[derive(Debug, Clone)]
pub struct JsProgram {
    name: String,
    source: String,
    sig: ScriptSig,
    module: bool,
}

/// Whether `source` is a module (has `export` or `import` statements).
fn is_module(source: &str) -> bool {
    source.lines().any(|l| {
        let t = l.trim_start();
        ["export ", "export{", "import ", "import{"]
            .iter()
            .any(|p| t.starts_with(p))
    })
}

fn syntax(name: &str, m: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::LogicSyntax,
        format!("{name}: the JavaScript doesn't compile: {m}"),
    )
}

impl JsProgram {
    /// Checks `source` (parses it; runs nothing) for a function `name`
    /// with signature `sig`.
    ///
    /// # Errors
    /// 39861 for a syntax error, or a body-form function whose parameter
    /// can't be a JavaScript name.
    pub fn new(name: &str, source: &str, sig: ScriptSig) -> Result<Self> {
        let module = is_module(source);
        if !module {
            for (p, _) in &sig.params {
                if !engine::bindable(p, RESERVED) {
                    return Err(SprigError::new(
                        SprigCode::LogicSyntax,
                        format!(
                            "{name}: parameter {p} can't be a JavaScript name; rename it, or write the body as a module (export default function (...))"
                        ),
                    ));
                }
            }
        }
        let p = Self {
            name: name.to_owned(),
            source: source.to_owned(),
            sig,
            module,
        };
        let rt = Runtime::new().map_err(|e| syntax(name, e))?;
        rt.set_memory_limit(64 << 20);
        rt.set_loader(
            BuiltinResolver::default().with_module("sprig:db"),
            BuiltinLoader::default().with_module("sprig:db", SPRIG_DB),
        );
        let cx = Context::full(&rt).map_err(|e| syntax(name, e))?;
        cx.with(|ctx| -> Result<()> {
            if p.module {
                Module::declare(ctx.clone(), "main", p.source.as_str())
                    .map(|_| ())
                    .map_err(|e| syntax(name, exception_text(&ctx, &e)))
            } else {
                ctx.eval::<Function<'_>, _>(p.wrapped())
                    .map(|_| ())
                    .map_err(|e| syntax(name, exception_text(&ctx, &e)))
            }
        })?;
        Ok(p)
    }

    /// The signature.
    #[must_use]
    pub fn sig(&self) -> &ScriptSig {
        &self.sig
    }

    /// The body form as a function expression (line numbers kept).
    fn wrapped(&self) -> String {
        let params: Vec<&str> = self.sig.params.iter().map(|(n, _)| n.as_str()).collect();
        format!("(function ({}) {{{}\n}})", params.join(", "), self.source)
    }
}

/// QuickJS's allocator, counting: an allocation that would take the
/// runtime past the budget fails, and trips the run.
struct Counting {
    used: usize,
    limit: usize,
    peak: Arc<AtomicU64>,
    st: Arc<RunState>,
}

impl Counting {
    fn refuse(&self) -> *mut u8 {
        self.st.trip(self.st.error(Budget::Memory));
        std::ptr::null_mut()
    }

    fn grew(&mut self) {
        self.peak.fetch_max(self.used as u64, Ordering::Relaxed);
    }
}

// SAFETY: every block comes from `RustAllocator`, which meets the trait's
// rules (aligned blocks with a size header, `usable_size` reading it); this
// only counts and refuses. A vetted opt-out of `unsafe_code` (chapter 03):
// rquickjs's allocator trait is unsafe by nature, and counting here is the
// only way to stop code that catches QuickJS's out-of-memory error.
#[allow(unsafe_code)]
unsafe impl Allocator for Counting {
    fn alloc(&mut self, size: usize) -> *mut u8 {
        if self.used.saturating_add(size) > self.limit {
            return self.refuse();
        }
        let p = RustAllocator.alloc(size);
        if !p.is_null() {
            // SAFETY: `p` was just allocated by `RustAllocator`.
            self.used += unsafe { RustAllocator::usable_size(p) };
            self.grew();
        }
        p
    }

    fn calloc(&mut self, count: usize, size: usize) -> *mut u8 {
        if self.used.saturating_add(count.saturating_mul(size)) > self.limit {
            return self.refuse();
        }
        let p = RustAllocator.calloc(count, size);
        if !p.is_null() {
            // SAFETY: as above.
            self.used += unsafe { RustAllocator::usable_size(p) };
            self.grew();
        }
        p
    }

    unsafe fn dealloc(&mut self, ptr: *mut u8) {
        // SAFETY: the caller passes a block of this allocator.
        unsafe {
            self.used = self.used.saturating_sub(RustAllocator::usable_size(ptr));
            RustAllocator.dealloc(ptr);
        }
    }

    unsafe fn realloc(&mut self, ptr: *mut u8, new_size: usize) -> *mut u8 {
        // SAFETY: the caller passes a block of this allocator (or null).
        unsafe {
            let old = if ptr.is_null() {
                0
            } else {
                RustAllocator::usable_size(ptr)
            };
            if self.used.saturating_sub(old).saturating_add(new_size) > self.limit {
                return self.refuse();
            }
            let q = RustAllocator.realloc(ptr, new_size);
            if !q.is_null() {
                self.used = self.used.saturating_sub(old) + RustAllocator::usable_size(q);
                self.grew();
            }
            q
        }
    }

    unsafe fn usable_size(ptr: *mut u8) -> usize {
        // SAFETY: as above.
        unsafe { RustAllocator::usable_size(ptr) }
    }
}

/// The text of a QuickJS error, with the exception's message and stack.
fn exception_text(ctx: &Ctx<'_>, e: &rquickjs::Error) -> String {
    if !matches!(e, rquickjs::Error::Exception) {
        return e.to_string();
    }
    let v = ctx.catch();
    value_text(ctx, &v)
}

fn value_text<'js>(ctx: &Ctx<'js>, v: &JsValue<'js>) -> String {
    if let Some(ex) = v.as_exception() {
        let msg = ex.message().unwrap_or_default();
        let name: Option<String> = ex.get("name").ok();
        let at = ex
            .stack()
            .and_then(|s| s.lines().next().map(|l| l.trim().to_owned()))
            .filter(|l| !l.is_empty());
        let head = match name {
            Some(n) if !n.is_empty() && n != "Error" => format!("{n}: {msg}"),
            _ => msg,
        };
        return match at {
            Some(at) => format!("{head} ({at})"),
            None => head,
        };
    }
    if let Some(s) = v.as_string() {
        return s.to_string().unwrap_or_default();
    }
    ctx.json_stringify(v.clone())
        .ok()
        .flatten()
        .and_then(|s| s.to_string().ok())
        .unwrap_or_else(|| "the code threw a value".to_owned())
}

/// Runs `p` with positional `args` on an engine thread.
///
/// # Errors
/// 39874 for the wrong number of arguments; 39864 for a budget; 39863
/// from `sprig.reject`; 39865 for an exception the code didn't catch; the
/// original error of a `db` or outbound call it didn't catch; 39866 for a
/// result holding `NaN`, an infinity or a `BigInt`; 39871 when the engine
/// is busy.
pub async fn run(p: &JsProgram, args: Vec<Value>, host: Host, limits: Limits) -> Result<Outcome> {
    p.sig.check_arity(&p.name, args.len())?;
    let st = Arc::new(RunState::new(limits));
    let sync = Arc::new(SyncHost::new(
        host,
        tokio::runtime::Handle::current(),
        Arc::clone(&st),
    ));
    let prog = p.clone();
    let st2 = Arc::clone(&st);
    let (event, result) =
        engine::run_blocking(Arc::clone(&st), move || run_here(&prog, &args, &sync, &st2)).await?;
    Ok(Outcome {
        event,
        result,
        steps: st.steps(),
        data_ops: st.data_ops(),
        rows_read: st.rows_read(),
        peak_bytes: st.peak_bytes(),
    })
}

/// The run, on the engine thread.
fn run_here(
    p: &JsProgram,
    args: &[Value],
    sync: &Arc<SyncHost>,
    st: &Arc<RunState>,
) -> Result<(String, Value)> {
    let peak = Arc::new(AtomicU64::new(0));
    let limit = usize::try_from(st.limits().memory_bytes).unwrap_or(usize::MAX);
    let rt = Runtime::new_with_alloc(Counting {
        used: 0,
        limit,
        peak: Arc::clone(&peak),
        st: Arc::clone(st),
    })
    .map_err(|e| internal(&e))?;
    rt.set_max_stack_size(512 << 10);
    {
        let st = Arc::clone(st);
        rt.set_interrupt_handler(Some(Box::new(move || st.interrupted())));
    }
    rt.set_loader(
        BuiltinResolver::default().with_module("sprig:db"),
        BuiltinLoader::default().with_module("sprig:db", SPRIG_DB),
    );
    let cx = Context::full(&rt).map_err(|e| internal(&e))?;
    let out = cx.with(|ctx| invoke(&ctx, p, args, sync, st));
    st.heap(peak.load(Ordering::Relaxed));
    // A trip wins over whatever the code did after it.
    if let Some(t) = st.tripped() {
        return Err(t);
    }
    out
}

fn internal(e: &rquickjs::Error) -> SprigError {
    SprigError::new(
        SprigCode::LogicRuntimeError,
        format!("the JavaScript engine failed: {e}"),
    )
}

/// The one host function the prelude wraps: `op` and a JSON payload in,
/// `{"ok": value}` or `{"err": {...}}` out.
fn host_op(sync: &SyncHost, op: &str, payload: &str) -> Value {
    match host_inner(sync, op, payload) {
        Ok(v) => serde_json::json!({ "ok": v }),
        Err(e) => {
            let id = sync.keep(crate::budget::copy(&e));
            serde_json::json!({ "err": engine::error_json(&e, id) })
        }
    }
}

fn host_inner(sync: &SyncHost, op: &str, payload: &str) -> Result<Value> {
    {
        let v: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
        let arg = |i: usize| v.get(i).cloned().unwrap_or(Value::Null);
        let text = |i: usize| match arg(i) {
            Value::String(s) => s,
            Value::Null => String::new(),
            other => other.to_string(),
        };
        let list = |i: usize| match arg(i) {
            Value::Array(a) => a,
            Value::Null => Vec::new(),
            other => vec![other],
        };
        match op {
            "sql" => {
                let sql = text(0);
                let shape = crate::bridge::shape_of(&sql);
                sync.sql(shape, &sql, &list(1), "db.sql")
            }
            "sqlOne" => sync.sql(Shape::One, &text(0), &list(1), "db.sqlOne"),
            "exec" => sync.sql(Shape::Exec, &text(0), &list(1), "db.exec"),
            "call" => sync.call(&text(0), &list(1), "db.call"),
            "fetch" => sync.fetch(engine::fetch_request(&text(0), &arg(1))),
            "log" => {
                sync.log(text(0));
                Ok(Value::Null)
            }
            "info" => Ok(sync.info().clone()),
            "reject" => {
                let e = SprigError::new(SprigCode::LogicValidationFailed, text(0));
                sync.st.trip(crate::budget::copy(&e));
                Err(e)
            }
            other => Err(SprigError::new(
                SprigCode::LogicRuntimeError,
                format!("unknown host operation {other}"),
            )),
        }
    }
}

fn invoke(
    ctx: &Ctx<'_>,
    p: &JsProgram,
    args: &[Value],
    sync: &Arc<SyncHost>,
    st: &Arc<RunState>,
) -> Result<(String, Value)> {
    let globals = ctx.globals();
    let h = Arc::clone(sync);
    let host = Function::new(ctx.clone(), move |op: String, payload: String| -> String {
        host_op(&h, &op, &payload).to_string()
    })
    .map_err(|e| internal(&e))?;
    globals
        .set("__sprig_host", host)
        .map_err(|e| internal(&e))?;
    ctx.eval::<(), _>(PRELUDE)
        .map_err(|e| failure(ctx, &e, sync, st))?;
    let take = |name: &str| -> Result<Function<'_>> {
        let f: Function<'_> = globals.get(name).map_err(|e| internal(&e))?;
        globals.remove(name).map_err(|e| internal(&e))?;
        Ok(f)
    };
    let invoke_fn = take("__sprig_invoke")?;
    let out_fn = take("__sprig_out")?;
    let main: Function<'_> = if p.module {
        let m = Module::declare(ctx.clone(), "main", p.source.as_str())
            .map_err(|e| failure(ctx, &e, sync, st))?;
        let (m, promise) = m.eval().map_err(|e| failure(ctx, &e, sync, st))?;
        promise
            .finish::<()>()
            .map_err(|e| failure(ctx, &e, sync, st))?;
        let d: JsValue<'_> = m.get("default").map_err(|e| failure(ctx, &e, sync, st))?;
        d.into_function().ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicSyntax,
                format!(
                    "{}: the module's default export isn't a function (export default function (...))",
                    p.name
                ),
            )
        })?
    } else {
        ctx.eval::<Function<'_>, _>(p.wrapped())
            .map_err(|e| failure(ctx, &e, sync, st))?
    };
    let a = serde_json::to_string(args).unwrap_or_else(|_| "[]".to_owned());
    let r: JsValue<'_> = invoke_fn
        .call((main, a))
        .map_err(|e| failure(ctx, &e, sync, st))?;
    let r = match r.as_promise() {
        Some(pr) => pr
            .finish::<JsValue<'_>>()
            .map_err(|e| failure(ctx, &e, sync, st))?,
        None => r,
    };
    let text: Option<String> = out_fn.call((r,)).map_err(|e| failure(ctx, &e, sync, st))?;
    let v = match text.as_deref() {
        None => Value::Null,
        Some("\u{0}nonfinite") => {
            return Err(SprigError::new(
                SprigCode::LogicRuntimeError,
                format!(
                    "{}: the result holds NaN, an infinity or a BigInt, which SQL can't hold",
                    p.name
                ),
            ));
        }
        Some(t) => serde_json::from_str(t).map_err(|e| {
            SprigError::new(
                SprigCode::LogicRuntimeError,
                format!("{}: the result isn't JSON: {e}", p.name),
            )
        })?,
    };
    st.check_time()?;
    Ok(engine::event_of(v, p.sig.scalar))
}

/// The error for a failed QuickJS call: a trip; a host error the code
/// didn't catch (the original); a `WouldBlock` promise; or the exception.
fn failure(ctx: &Ctx<'_>, e: &rquickjs::Error, sync: &SyncHost, st: &RunState) -> SprigError {
    if let Some(t) = st.tripped() {
        return t;
    }
    match e {
        rquickjs::Error::Exception => {
            let v = ctx.catch();
            if let Some(o) = v.as_object()
                && let Ok(Some(id)) = o.get::<_, Option<u32>>("__sprig")
                && let Some(orig) = sync.kept(id as usize)
            {
                return orig;
            }
            engine::user_error(&value_text(ctx, &v))
        }
        rquickjs::Error::WouldBlock => SprigError::new(
            SprigCode::LogicRuntimeError,
            "the function returned a promise that never settles",
        ),
        rquickjs::Error::Resolving { .. } | rquickjs::Error::Loading { .. } => SprigError::new(
            SprigCode::LogicSyntax,
            format!("{e}: the only module code can import is sprig:db"),
        ),
        rquickjs::Error::Allocation => st.error(Budget::Memory),
        other => internal(other),
    }
}

#[cfg(test)]
mod tests;
