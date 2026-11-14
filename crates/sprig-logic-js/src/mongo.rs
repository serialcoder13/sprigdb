//! MongoDB's server-side JavaScript on SprigDB's QuickJS: the code of
//! `$where`, `$function` and `$accumulator`.
//!
//! **One runtime per operation per thread.** A MongoDB operation calls
//! its code once per document, so compiling and building a runtime per
//! call (as [`crate::js`] does for a SQL function) would cost more than
//! the call. Each thread keeps one QuickJS runtime for the operation it
//! last served ([`Call::op`]): its compiled functions, and the globals the
//! code sets, last for that operation, as MongoDB keeps one scope per
//! operation. A call for another operation drops the runtime and starts a
//! fresh one, so nothing one operation's code sets reaches another's.
//!
//! **What code can reach.** The MongoDB shell's types (`ObjectId`,
//! `NumberLong`, `NumberDecimal`, `Timestamp`, `BinData`, `UUID`,
//! `MinKey`, `MaxKey`, `ISODate` and the rest) and helpers (`tojson`,
//! `isNumber`, `hex_md5`, `Object.bsonsize`, `sleep`, `print`). No
//! `db`, no network, no files, no modules: QuickJS has none here, as in
//! MongoDB, where server-side code has no `db` since 4.4.
//!
//! **Budgets.** The caller passes a deadline (what is left of the
//! operation's `logic.function_budget_ms`) and a heap limit
//! (`logic.memory_limit_mb`); an interrupt handler stops the code at the
//! deadline, and QuickJS refuses allocations past the limit.
//!
//! **Values.** BSON crosses as MongoDB's server does: a 32-bit integer or
//! a double is a JavaScript number, a 64-bit integer a `NumberLong`, a
//! date a `Date`. A number comes back as a double, except that a field
//! of an object that came from BSON keeps its 32-bit integer type while
//! its value stays integral (MongoDB's rule for its own documents).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use base64::Engine as _;
use bson::spec::BinarySubtype;
use bson::{Bson, Document};
use rquickjs::function::Args;
use rquickjs::{Array, Context, Ctx, Function, Object, Persistent, Runtime, Value};

/// The prelude: the shell's types and helpers.
const PRELUDE: &str = include_str!("mongo_prelude.js");

/// Objects at most this deep go back to BSON (MongoDB's limit is 100
/// levels of nesting, plus room for the value itself).
const MAX_DEPTH: usize = 150;

/// One call.
#[derive(Debug)]
pub struct Call<'a> {
    /// The operation (a number the caller never reuses): calls of one
    /// operation share a runtime and its globals.
    pub op: u64,
    /// The code: a function (`function (a) { ... }`) or an expression or
    /// statements whose last value is the result (`this.a > 1`).
    pub src: &'a str,
    /// `this` (and the global `obj`, for `$where`).
    pub this: Option<&'a Document>,
    /// The arguments.
    pub args: &'a [Bson],
    /// The result as JavaScript truthiness (`$where`).
    pub as_bool: bool,
    /// When the code is stopped.
    pub deadline: Instant,
    /// The runtime's heap limit.
    pub memory_bytes: usize,
}

/// Why a call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// The deadline passed.
    Interrupted,
    /// The code failed: MongoDB's code and message (139
    /// `JSInterpreterFailure` for an error, a syntax error or memory; 8
    /// for a thrown string; 2 for a result BSON can't hold).
    Error(i32, String),
}

/// `JSInterpreterFailure`.
const JS_FAILURE: i32 = 139;

/// A thread's runtime. Fields drop in order: the saved values before the
/// context, the context before the runtime.
struct Cached {
    op: u64,
    fns: HashMap<String, Persistent<Function<'static>>>,
    mk: Persistent<Object<'static>>,
    /// Calls made so far: the generation of [`ORIGIN`] tags.
    calls: u32,
    /// Documents of this call that objects came from (their 32-bit
    /// integer fields).
    origins: Vec<Document>,
    cx: Context,
    _rt: Runtime,
    deadline: Arc<AtomicU64>,
    tripped: Arc<AtomicBool>,
    base: Instant,
}

thread_local! {
    static CACHED: RefCell<Option<Cached>> = const { RefCell::new(None) };
}

/// The hidden property tagging an object that came from a document.
const ORIGIN: &str = "__sprig_o";

/// Runs one call on this thread.
///
/// # Errors
/// [`Failure`].
pub fn call(c: &Call<'_>) -> Result<Bson, Failure> {
    CACHED.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.as_ref().is_none_or(|k| k.op != c.op) {
            *slot = None;
            *slot = Some(fresh(c.op, c.memory_bytes)?);
        }
        let Some(k) = slot.as_mut() else {
            return Err(Failure::Error(JS_FAILURE, "no JavaScript runtime".into()));
        };
        let left = c.deadline.saturating_duration_since(k.base);
        k.deadline.store(
            u64::try_from(left.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        k.tripped.store(false, Ordering::Relaxed);
        k.calls = k.calls.wrapping_add(1);
        k.origins.clear();
        let out = run(k, c);
        // A runtime that ran out of time or memory may hold half-built
        // state: the operation's next call starts a fresh one.
        if matches!(out, Err(Failure::Interrupted))
            || matches!(&out, Err(Failure::Error(_, m)) if m == "Out of memory")
        {
            *slot = None;
        }
        out
    })
}

/// Drops this thread's runtime (tests; a gateway thread keeps it until
/// the next operation).
pub fn reset_thread() {
    CACHED.with(|cell| *cell.borrow_mut() = None);
}

fn fresh(op: u64, memory_bytes: usize) -> Result<Cached, Failure> {
    let rt = Runtime::new().map_err(|e| Failure::Error(JS_FAILURE, e.to_string()))?;
    // The prelude takes a little heap of its own.
    rt.set_memory_limit(memory_bytes.max(4 << 20));
    rt.set_max_stack_size(512 << 10);
    let base = Instant::now();
    let deadline = Arc::new(AtomicU64::new(u64::MAX));
    let tripped = Arc::new(AtomicBool::new(false));
    {
        let (d, t) = (Arc::clone(&deadline), Arc::clone(&tripped));
        rt.set_interrupt_handler(Some(Box::new(move || {
            let now = u64::try_from(base.elapsed().as_nanos()).unwrap_or(u64::MAX);
            if now >= d.load(Ordering::Relaxed) {
                t.store(true, Ordering::Relaxed);
                true
            } else {
                false
            }
        })));
    }
    let cx = Context::full(&rt).map_err(|e| Failure::Error(JS_FAILURE, e.to_string()))?;
    let mk = cx.with(|ctx| -> Result<Persistent<Object<'static>>, Failure> {
        let host = Function::new(ctx.clone(), host_fn)
            .map_err(|e| Failure::Error(JS_FAILURE, e.to_string()))?;
        ctx.globals()
            .set("__sprig_mongo_host", host)
            .map_err(|e| Failure::Error(JS_FAILURE, e.to_string()))?;
        ctx.eval::<(), _>(PRELUDE)
            .map_err(|e| Failure::Error(JS_FAILURE, format!("the prelude failed: {e}")))?;
        let mk: Object<'_> = ctx
            .globals()
            .get("__sprig_mk")
            .map_err(|e| Failure::Error(JS_FAILURE, e.to_string()))?;
        let _ = ctx.globals().remove("__sprig_mk");
        Ok(Persistent::save(&ctx, mk))
    })?;
    Ok(Cached {
        op,
        fns: HashMap::new(),
        mk,
        calls: 0,
        origins: Vec::new(),
        cx,
        _rt: rt,
        deadline,
        tripped,
        base,
    })
}

/// `hex_md5` and `Object.bsonsize` (rquickjs passes the arguments
/// owned).
#[allow(clippy::needless_pass_by_value)]
fn host_fn<'js>(ctx: Ctx<'js>, op: String, v: Value<'js>) -> rquickjs::Result<Value<'js>> {
    match op.as_str() {
        "md5" => {
            use md5::Digest as _;
            let s: String = v
                .as_string()
                .and_then(|s| s.to_string().ok())
                .unwrap_or_default();
            let d = md5::Md5::digest(s.as_bytes());
            let hex = d.iter().fold(String::new(), |mut h, b| {
                use std::fmt::Write as _;
                let _ = write!(h, "{b:02x}");
                h
            });
            rquickjs::String::from_str(ctx, &hex).map(rquickjs::String::into_value)
        }
        "bsonsize" => {
            let mut origins = Vec::new();
            let n = match from_js(&v, None, 0, &mut origins, 0) {
                Ok(Bson::Document(d)) => bson::to_vec(&d).map_or(0, |b| b.len()),
                _ => 0,
            };
            #[allow(clippy::cast_precision_loss)]
            Ok(Value::new_number(ctx, n as f64))
        }
        _ => Ok(Value::new_undefined(ctx)),
    }
}

/// The function a source compiles to: a function expression as written;
/// otherwise an expression returned, statements with their own `return`,
/// or (last) statements whose last value is the result, as MongoDB
/// wraps code that isn't a function.
fn candidates(src: &str) -> Vec<String> {
    let t = src.trim().trim_end_matches(';').trim_end();
    if t.starts_with("function") {
        return vec![format!("({t}\n)")];
    }
    let mut out = vec![format!("(function () {{ return ({t}\n); }})")];
    if t.contains("return") {
        out.push(format!("(function () {{ {t}\n}})"));
    }
    let lit = serde_json::to_string(t).unwrap_or_else(|_| "\"\"".to_owned());
    out.push(format!("(function () {{ return eval({lit}); }})"));
    out
}

fn compile<'js>(ctx: &Ctx<'js>, k: &mut Cached, src: &str) -> Result<Function<'js>, Failure> {
    if let Some(f) = k.fns.get(src) {
        return f
            .clone()
            .restore(ctx)
            .map_err(|e| Failure::Error(JS_FAILURE, e.to_string()));
    }
    let mut first_err = None;
    for cand in candidates(src) {
        // Sloppy mode, as MongoDB's: `x = 1` makes a global.
        let mut opts = rquickjs::context::EvalOptions::default();
        opts.strict = false;
        opts.global = true;
        match ctx.eval_with_options::<Value<'_>, _>(cand.as_str(), opts) {
            Ok(v) => {
                let Some(f) = v.into_function() else {
                    return Err(Failure::Error(
                        JS_FAILURE,
                        "the code isn't a function".to_owned(),
                    ));
                };
                k.fns
                    .insert(src.to_owned(), Persistent::save(ctx, f.clone()));
                return Ok(f);
            }
            Err(e) => {
                let f = failure(ctx, k, &e);
                first_err.get_or_insert(f);
            }
        }
    }
    Err(first_err.unwrap_or_else(|| Failure::Error(JS_FAILURE, "SyntaxError".into())))
}

fn run(k: &mut Cached, c: &Call<'_>) -> Result<Bson, Failure> {
    let cx = k.cx.clone();
    cx.with(|ctx| -> Result<Bson, Failure> {
        let f = compile(&ctx, k, c.src)?;
        let mk =
            k.mk.clone()
                .restore(&ctx)
                .map_err(|e| Failure::Error(JS_FAILURE, e.to_string()))?;
        let gen_ = k.calls;
        let mut args = Args::new(ctx.clone(), c.args.len());
        let this = match c.this {
            Some(d) => to_js(&ctx, &mk, &Bson::Document(d.clone()), gen_, &mut k.origins)
                .map_err(|e| failure(&ctx, k, &e))?,
            None => ctx.globals().into_value(),
        };
        if c.as_bool {
            ctx.globals()
                .set("obj", this.clone())
                .map_err(|e| failure(&ctx, k, &e))?;
        }
        args.this(this).map_err(|e| failure(&ctx, k, &e))?;
        for a in c.args {
            let v = to_js(&ctx, &mk, a, gen_, &mut k.origins).map_err(|e| failure(&ctx, k, &e))?;
            args.push_arg(v).map_err(|e| failure(&ctx, k, &e))?;
        }
        let r: Value<'_> = args.apply(&f).map_err(|e| failure(&ctx, k, &e))?;
        if k.tripped.load(Ordering::Relaxed) {
            return Err(Failure::Interrupted);
        }
        if c.as_bool {
            return Ok(Bson::Boolean(truthy(&r)));
        }
        from_js(&r, None, 0, &mut k.origins, gen_)
    })
}

/// JavaScript's `ToBoolean`.
fn truthy(v: &Value<'_>) -> bool {
    if v.is_undefined() || v.is_null() {
        return false;
    }
    if let Some(b) = v.as_bool() {
        return b;
    }
    if let Some(n) = v.as_number() {
        return n != 0.0 && !n.is_nan();
    }
    if let Some(s) = v.as_string() {
        return s.to_string().is_ok_and(|s| !s.is_empty());
    }
    if let Some(b) = v.as_big_int() {
        return b.clone().to_i64().map_or(true, |n| n != 0);
    }
    true
}

/// The failure for a QuickJS error: the deadline, memory, or the thrown
/// value as MongoDB reports it.
fn failure(ctx: &Ctx<'_>, k: &Cached, e: &rquickjs::Error) -> Failure {
    if k.tripped.load(Ordering::Relaxed) {
        return Failure::Interrupted;
    }
    match e {
        rquickjs::Error::Exception => {
            let v = ctx.catch();
            thrown(ctx, k, &v)
        }
        rquickjs::Error::Allocation => Failure::Error(JS_FAILURE, "Out of memory".into()),
        other => Failure::Error(JS_FAILURE, other.to_string()),
    }
}

fn thrown<'js>(ctx: &Ctx<'js>, k: &Cached, v: &Value<'js>) -> Failure {
    let Ok(mk) = k.mk.clone().restore(ctx) else {
        return Failure::Error(JS_FAILURE, "uncaught exception".into());
    };
    let Ok(f) = mk.get::<_, Function<'_>>("thrown") else {
        return Failure::Error(JS_FAILURE, "uncaught exception".into());
    };
    let Ok(parts) = f.call::<_, Vec<rquickjs::Coerced<String>>>((v.clone(),)) else {
        return Failure::Error(JS_FAILURE, "uncaught exception".into());
    };
    let part = |i: usize| parts.get(i).map(|c| c.0.clone()).unwrap_or_default();
    let (kind, msg, stack) = (part(0).parse::<i32>().unwrap_or(2), part(1), part(2));
    match kind {
        0 => Failure::Error(8, format!("uncaught exception: {msg}")),
        1 => {
            if msg.starts_with("InternalError: out of memory") {
                return Failure::Error(JS_FAILURE, "Out of memory".into());
            }
            match location(&stack) {
                Some((l, c)) => Failure::Error(JS_FAILURE, format!("{msg} :\n@:{l}:{c}\n")),
                None => Failure::Error(JS_FAILURE, msg),
            }
        }
        _ => Failure::Error(JS_FAILURE, format!("uncaught exception: {msg}")),
    }
}

/// The line and column of a QuickJS stack's first frame (`at f
/// (<input>:1:21)`).
fn location(stack: &str) -> Option<(u32, u32)> {
    let line = stack.lines().find(|l| l.trim_start().starts_with("at "))?;
    let inner = line.rsplit_once('(')?.1.trim_end_matches(')');
    let mut parts = inner.rsplit(':');
    let c = parts.next()?.parse().ok()?;
    let l = parts.next()?.parse().ok()?;
    Some((l, c))
}

fn js_err(m: impl Into<String>) -> rquickjs::Error {
    rquickjs::Error::new_from_js_message("bson", "value", m.into())
}

/// BSON to JavaScript.
fn to_js<'js>(
    ctx: &Ctx<'js>,
    mk: &Object<'js>,
    v: &Bson,
    gen_: u32,
    origins: &mut Vec<Document>,
) -> rquickjs::Result<Value<'js>> {
    let make = |name: &str| -> rquickjs::Result<Function<'js>> { mk.get(name) };
    Ok(match v {
        Bson::Double(d) => Value::new_float(ctx.clone(), *d),
        Bson::Int32(i) => Value::new_int(ctx.clone(), *i),
        Bson::Int64(i) => make("long")?.call((i.to_string(),))?,
        Bson::String(s) | Bson::Symbol(s) => {
            rquickjs::String::from_str(ctx.clone(), s)?.into_value()
        }
        Bson::Boolean(b) => Value::new_bool(ctx.clone(), *b),
        Bson::Null | Bson::DbPointer(_) => Value::new_null(ctx.clone()),
        Bson::Undefined => Value::new_undefined(ctx.clone()),
        Bson::Array(a) => {
            let arr = Array::new(ctx.clone())?;
            for (i, x) in a.iter().enumerate() {
                arr.set(i, to_js(ctx, mk, x, gen_, origins)?)?;
            }
            arr.into_value()
        }
        Bson::Document(d) => {
            let o = Object::new(ctx.clone())?;
            for (key, x) in d {
                o.set(key.as_str(), to_js(ctx, mk, x, gen_, origins)?)?;
            }
            // Its 32-bit integer fields stay so on the way back.
            if d.values().any(|x| matches!(x, Bson::Int32(_))) && origins.len() < (1 << 20) {
                origins.push(d.clone());
                #[allow(clippy::cast_precision_loss)]
                let tag = f64::from(gen_) * f64::from(1u32 << 20) + (origins.len() - 1) as f64;
                o.prop(ORIGIN, tag)?;
            }
            o.into_value()
        }
        Bson::ObjectId(o) => make("oid")?.call((o.to_hex(),))?,
        Bson::DateTime(d) => {
            #[allow(clippy::cast_precision_loss)]
            let ms = d.timestamp_millis() as f64;
            make("date")?.call((ms,))?
        }
        Bson::Decimal128(d) => make("dec")?.call((d.to_string(),))?,
        Bson::Timestamp(t) => make("ts")?.call((t.time, t.increment))?,
        Bson::Binary(b) => {
            let sub: u8 = b.subtype.into();
            make("bin")?.call((
                i32::from(sub),
                base64::engine::general_purpose::STANDARD.encode(&b.bytes),
            ))?
        }
        Bson::RegularExpression(r) => {
            let flags: String = r
                .options
                .chars()
                .filter(|c| matches!(c, 'i' | 'm' | 's' | 'u'))
                .collect();
            make("re")?.call((r.pattern.clone(), flags))?
        }
        Bson::JavaScriptCode(c) => make("code")?.call((c.clone(),))?,
        Bson::JavaScriptCodeWithScope(c) => make("code")?.call((c.code.clone(),))?,
        Bson::MinKey => mk.get("minkey")?,
        Bson::MaxKey => mk.get("maxkey")?,
    })
}

/// JavaScript to BSON.
fn from_js(
    v: &Value<'_>,
    orig: Option<&Bson>,
    depth: usize,
    origins: &mut Vec<Document>,
    gen_: u32,
) -> Result<Bson, Failure> {
    let bad = || Failure::Error(2, "unable to get type".into());
    if depth > MAX_DEPTH {
        return Err(Failure::Error(
            JS_FAILURE,
            "the result nests too deeply (or refers to itself)".into(),
        ));
    }
    if v.is_undefined() || v.is_null() {
        return Ok(Bson::Null);
    }
    if let Some(b) = v.as_bool() {
        return Ok(Bson::Boolean(b));
    }
    if let Some(n) = v.as_number() {
        if let Some(Bson::Int32(_)) = orig
            && n.fract() == 0.0
            && n >= f64::from(i32::MIN)
            && n <= f64::from(i32::MAX)
            && !(n == 0.0 && n.is_sign_negative())
        {
            #[allow(clippy::cast_possible_truncation)]
            return Ok(Bson::Int32(n as i32));
        }
        return Ok(Bson::Double(n));
    }
    if let Some(s) = v.as_string() {
        return s
            .to_string()
            .map(Bson::String)
            .map_err(|e| Failure::Error(JS_FAILURE, e.to_string()));
    }
    if v.is_big_int() || v.is_symbol() {
        return Err(bad());
    }
    let Some(o) = v.as_object() else {
        return Err(bad());
    };
    let err = |e: rquickjs::Error| Failure::Error(JS_FAILURE, e.to_string());
    let bt: Option<String> = o.get("__bt").ok().flatten();
    if let Some(bt) = bt.as_deref() {
        return typed(o, bt).map_err(err);
    }
    if v.is_function() {
        let s: rquickjs::Coerced<String> = v.get().map_err(err)?;
        return Ok(Bson::JavaScriptCode(s.0));
    }
    if let Some(a) = v.as_array() {
        let mut out = Vec::with_capacity(a.len());
        for x in a.iter::<Value<'_>>() {
            out.push(from_js(&x.map_err(err)?, None, depth + 1, origins, gen_)?);
        }
        return Ok(Bson::Array(out));
    }
    // A plain object.
    let src: Option<Document> = o
        .get::<_, Option<f64>>(ORIGIN)
        .ok()
        .flatten()
        .and_then(|tag| {
            let scale = f64::from(1u32 << 20);
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let (g, i) = ((tag / scale).floor() as u32, (tag % scale) as usize);
            (g == gen_).then(|| origins.get(i).cloned()).flatten()
        });
    let mut out = Document::new();
    for p in o.props::<String, Value<'_>>() {
        let (key, x) = p.map_err(err)?;
        let prev = src.as_ref().and_then(|d| d.get(&key));
        out.insert(key, from_js(&x, prev, depth + 1, origins, gen_)?);
    }
    Ok(Bson::Document(out))
}

/// A shell-typed value back to BSON.
fn typed(o: &Object<'_>, bt: &str) -> rquickjs::Result<Bson> {
    let s = |k: &str| -> rquickjs::Result<String> {
        o.get::<_, rquickjs::Coerced<String>>(k).map(|c| c.0)
    };
    Ok(match bt {
        "oid" => bson::oid::ObjectId::parse_str(s("str")?)
            .map(Bson::ObjectId)
            .map_err(|e| js_err(e.to_string()))?,
        "long" => Bson::Int64(s("__v")?.parse().map_err(|_| js_err("bad NumberLong"))?),
        "int" => Bson::Int32(o.get::<_, i32>("__v")?),
        "dec" => {
            let t = s("__v")?;
            Bson::Decimal128(
                t.parse::<bson::Decimal128>()
                    .map_err(|_| js_err(format!("invalid NumberDecimal \"{t}\"")))?,
            )
        }
        "date" => {
            let f: Function<'_> = o.get("getTime")?;
            let ms: f64 = f.call((rquickjs::function::This(o.clone()),))?;
            #[allow(clippy::cast_possible_truncation)]
            let ms = if ms.is_finite() { ms as i64 } else { 0 };
            Bson::DateTime(bson::DateTime::from_millis(ms))
        }
        "re" => {
            let flags = s("flags")?;
            let mut opts: Vec<char> = flags
                .chars()
                .filter(|c| matches!(c, 'i' | 'm' | 's' | 'u'))
                .collect();
            opts.sort_unstable();
            Bson::RegularExpression(bson::Regex {
                pattern: s("source")?,
                options: opts.into_iter().collect(),
            })
        }
        "ts" => Bson::Timestamp(bson::Timestamp {
            time: o.get::<_, u32>("t")?,
            increment: o.get::<_, u32>("i")?,
        }),
        "bin" => {
            let sub: i32 = o.get("__sub")?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(s("__b64")?)
                .map_err(|e| js_err(e.to_string()))?;
            Bson::Binary(bson::Binary {
                subtype: BinarySubtype::from(u8::try_from(sub).unwrap_or(0)),
                bytes,
            })
        }
        "code" => Bson::JavaScriptCode(s("code")?),
        "minkey" => Bson::MinKey,
        "maxkey" => Bson::MaxKey,
        _ => Bson::Null,
    })
}

#[cfg(test)]
mod tests;
