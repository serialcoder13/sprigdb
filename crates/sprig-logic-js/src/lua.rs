//! SprigDB's one Lua runtime (chapter 41, "The fallback runtime";
//! chapter 40, "Lua scripting"; chapter 51's script sinks): Lua 5.1,
//! Redis's dialect, from `mlua` with vendored sources.
//!
//! Two layers, kept apart so each user adds only its own library:
//!
//! - [`Sandbox`]: a fresh, sandboxed Lua state with budgets. Every user
//!   of Lua in SprigDB starts here: logic functions ([`LuaProgram`],
//!   with the `db` / `sprig` / `ctx` library below), Redis `EVAL` and
//!   `FUNCTION` (chapter 40 adds `redis.*` with [`Sandbox::lua`]), and
//!   script sinks (chapter 51 adds its `sprig.*` with a read-only `db`).
//! - The logic library: `db.sql(text, ...)`, `db.sqlOne`, `db.exec`,
//!   `db.call(name, ...)`, `fetch(url, init)`, `sprig.reject(message)`,
//!   `sprig.event(name, fields)`, `sprig.null`, `ctx` and `log` / `print`,
//!   all over the same [`SyncHost`] as KIRun's `Sprig.Db.*` and
//!   JavaScript's `db`.
//!
//! **Sandbox.** Loaded: the base library, `string`, `table`, `math`,
//! `coroutine`, and `cjson` (`encode`, `decode`, `null`). Removed: `io`,
//! `os`, `debug`, `package`, `require`, `module`, `dofile`, `loadfile`,
//! `load` (its reader form), `string.dump`, and binary chunks in
//! `loadstring`. `print` goes to the run's log. `math.random` and
//! `math.randomseed` are SprigDB's own generator, per state: with
//! [`Config::deterministic`] it starts from the same seed every run, as
//! Redis's scripts need; otherwise from a random one (Lua 5.1's own uses
//! the C library's process-wide `rand`). [`Config::protect_globals`]
//! makes creating a global an error, as Redis 7 does.
//!
//! **Budgets.** A count hook every [`Config::hook_every`] VM instructions
//! (10,000, chapter 40's figure) checks the wall time, the cancel flag and
//! trips; the state's allocator refuses memory past the budget. Lua's
//! `pcall`, `xpcall` and `coroutine.resume` are wrapped so that a budget
//! error, once raised, can't be caught: each re-raises it while the run
//! is tripped, and a refused allocation trips the run. Coroutines inherit
//! the hook (the hook is the state's global one).

use std::cell::Cell;
use std::sync::Arc;

use mlua::chunk::ChunkMode;
use mlua::{
    HookTriggers, Lua, LuaOptions, LuaSerdeExt, MultiValue, StdLib, Table, Value as LuaValue,
    Variadic, VmState,
};
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};

use crate::bridge::Shape;
use crate::budget::{Budget, Limits, RunState, copy};
use crate::engine::{self, ScriptSig, SyncHost};
use crate::kirun::{Host, Outcome};
use crate::messaging::MsgOp;

/// Lua's reserved words.
const RESERVED: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "if", "in", "local",
    "nil", "not", "or", "repeat", "return", "then", "true", "until", "while",
];

/// Lua's out-of-memory message (`LUA_ERRMEM`), as `pcall` sees it.
const NO_MEMORY: &str = "not enough memory";

/// How a sandbox is set up.
#[derive(Debug, Clone)]
pub struct Config {
    /// The heap budget in bytes (`logic.memory_limit_mb`,
    /// `redis.lua_memory_limit_mb`).
    pub memory_bytes: usize,
    /// VM instructions between budget checks.
    pub hook_every: u32,
    /// `math.random` starts from the same seed every run.
    pub deterministic: bool,
    /// Creating a global is an error (Redis 7).
    pub protect_globals: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memory_bytes: 64 << 20,
            hook_every: 10_000,
            deterministic: false,
            protect_globals: false,
        }
    }
}

/// Runs first in every state: the uncatchable-budget wrappers and the
/// removals (see the module docs).
const PRELUDE: &str = r"
local _pcall, _xpcall, _resume, _error, _stop = pcall, xpcall, coroutine.resume, error, __sprig_stop
__sprig_stop = nil
local function pass(ok, ...)
  if not ok and _stop((...)) then _error((...), 0) end
  return ok, ...
end
pcall = function(f, ...) return pass(_pcall(f, ...)) end
xpcall = function(f, h) return pass(_xpcall(f, h)) end
coroutine.resume = function(co, ...) return pass(_resume(co, ...)) end
local _loadstring, _type = loadstring, type
loadstring = function(s, name)
  if _type(s) ~= 'string' then return nil, 'loadstring takes text' end
  if s:byte(1) == 27 then return nil, 'binary chunks are not allowed' end
  return _loadstring(s, name)
end
load, dofile, loadfile, print, module, require, string.dump = nil, nil, nil, nil, nil, nil, nil
";

/// Makes creating a global an error (after the libraries are in).
const PROTECT_GLOBALS: &str = r"
setmetatable(_G, {
  __newindex = function(_, k) error('Script attempted to create global variable ' .. tostring(k), 2) end,
  __index = function(_, k) error('Script attempted to access nonexistent global variable ' .. tostring(k), 2) end,
  __metatable = false,
})
";

/// A sandboxed Lua state with budgets (see the module docs).
pub struct Sandbox {
    lua: Lua,
    st: Arc<RunState>,
    protect: bool,
}

impl std::fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sandbox").finish_non_exhaustive()
    }
}

/// splitmix64: SprigDB's `math.random`.
fn next_random(state: &Cell<u64>) -> u64 {
    let mut z = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
    state.set(z);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn lua_err(e: SprigError) -> mlua::Error {
    mlua::Error::external(e)
}

impl Sandbox {
    /// A fresh state for one run, its budgets charged to `st`.
    ///
    /// # Errors
    /// 39866 when Lua can't start.
    pub fn new(cfg: &Config, st: Arc<RunState>) -> Result<Self> {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::new(),
        )
        .map_err(|e| internal(&e))?;
        let sb = Self {
            lua,
            st,
            protect: cfg.protect_globals,
        };
        sb.install_base(cfg).map_err(|e| internal(&e))?;
        sb.lua
            .set_memory_limit(cfg.memory_bytes)
            .map_err(|e| internal(&e))?;
        let st = Arc::clone(&sb.st);
        let every = cfg.hook_every.max(1);
        sb.lua
            .set_global_hook(
                HookTriggers::new().every_nth_instruction(every),
                move |_, _| {
                    st.count_steps(u64::from(every));
                    if st.interrupted() {
                        let e = st.tripped().unwrap_or_else(|| st.error(Budget::Cancelled));
                        return Err(lua_err(e));
                    }
                    Ok(VmState::Continue)
                },
            )
            .map_err(|e| internal(&e))?;
        Ok(sb)
    }

    fn install_base(&self, cfg: &Config) -> mlua::Result<()> {
        let lua = &self.lua;
        let g = lua.globals();
        let st = Arc::clone(&self.st);
        g.set(
            "__sprig_stop",
            lua.create_function(move |_, err: LuaValue| {
                if let LuaValue::String(s) = &err
                    && s.to_str().is_ok_and(|t| t == NO_MEMORY)
                {
                    st.trip(st.error(Budget::Memory));
                }
                Ok(st.interrupted())
            })?,
        )?;
        lua.load(PRELUDE)
            .set_name("=sprig")
            .set_mode(ChunkMode::Text)
            .exec()?;
        // math.random, math.randomseed.
        let seed = if cfg.deterministic {
            0
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64)
                ^ (Arc::as_ptr(&self.st) as u64)
        };
        let state = std::rc::Rc::new(Cell::new(seed));
        let math: Table = g.get("math")?;
        let s1 = std::rc::Rc::clone(&state);
        math.set(
            "random",
            lua.create_function(move |_, (m, n): (Option<f64>, Option<f64>)| {
                let r = (next_random(&s1) >> 11) as f64 / (1u64 << 53) as f64;
                Ok(match (m, n) {
                    (None, _) => r,
                    (Some(m), None) => {
                        if m < 1.0 {
                            return Err(mlua::Error::RuntimeError(
                                "bad argument #1 to 'random' (interval is empty)".into(),
                            ));
                        }
                        (r * m.floor()).floor() + 1.0
                    }
                    (Some(m), Some(n)) => {
                        if m > n {
                            return Err(mlua::Error::RuntimeError(
                                "bad argument #2 to 'random' (interval is empty)".into(),
                            ));
                        }
                        (r * (n.floor() - m.floor() + 1.0)).floor() + m.floor()
                    }
                })
            })?,
        )?;
        let s2 = std::rc::Rc::clone(&state);
        math.set(
            "randomseed",
            lua.create_function(move |_, x: f64| {
                s2.set(x.to_bits());
                Ok(())
            })?,
        )?;
        // cjson.
        let cjson = lua.create_table()?;
        cjson.set(
            "encode",
            lua.create_function(|lua, v: LuaValue| {
                let j = to_json(lua, v)?;
                Ok(j.to_string())
            })?,
        )?;
        cjson.set(
            "decode",
            lua.create_function(|lua, s: mlua::LuaString| {
                let j: Value = serde_json::from_slice(&s.as_bytes())
                    .map_err(|e| mlua::Error::RuntimeError(format!("cjson.decode: {e}")))?;
                lua.to_value(&j)
            })?,
        )?;
        cjson.set("null", lua.null())?;
        g.set("cjson", cjson)?;
        Ok(())
    }

    /// The state, for adding a library (`redis.*`, the logic library).
    #[must_use]
    pub fn lua(&self) -> &Lua {
        &self.lua
    }

    /// The run's budgets.
    #[must_use]
    pub fn state(&self) -> &Arc<RunState> {
        &self.st
    }

    /// Call after adding libraries: protects `_G` when configured.
    ///
    /// # Errors
    /// 39866.
    pub fn seal(&self) -> Result<()> {
        if self.protect {
            self.lua
                .load(PROTECT_GLOBALS)
                .set_name("=sprig")
                .set_mode(ChunkMode::Text)
                .exec()
                .map_err(|e| internal(&e))?;
        }
        Ok(())
    }

    /// Compiles `source` (text only, never bytecode) as chunk `name`.
    ///
    /// # Errors
    /// 39861 for a syntax error.
    pub fn load(&self, name: &str, source: &str) -> Result<mlua::Function> {
        self.lua
            .load(source)
            .set_name(format!("={name}"))
            .set_mode(ChunkMode::Text)
            .into_function()
            .map_err(|e| SprigError::new(SprigCode::LogicSyntax, format!("{name}: {e}")))
    }

    /// The error a run ends with: a trip first (a budget, `sprig.reject`,
    /// cancellation), then the original error of a host call the code
    /// didn't catch, then the memory budget, then 39865 with Lua's text.
    #[must_use]
    pub fn error(&self, e: &mlua::Error) -> SprigError {
        if let Some(t) = self.st.tripped() {
            return t;
        }
        if let Some(s) = sprig_of(e) {
            return s;
        }
        match e {
            mlua::Error::MemoryError(_) => self.st.error(Budget::Memory),
            mlua::Error::SyntaxError { message, .. } => {
                SprigError::new(SprigCode::LogicSyntax, message.clone())
            }
            other => engine::user_error(&lua_text(other)),
        }
    }
}

/// The SprigDB error inside an mlua error, if any.
fn sprig_of(e: &mlua::Error) -> Option<SprigError> {
    match e {
        mlua::Error::ExternalError(a) => a.downcast_ref::<SprigError>().map(copy),
        mlua::Error::CallbackError { cause, .. } | mlua::Error::WithContext { cause, .. } => {
            sprig_of(cause)
        }
        _ => None,
    }
}

/// An mlua error's text without the traceback.
fn lua_text(e: &mlua::Error) -> String {
    match e {
        mlua::Error::RuntimeError(m) => m.clone(),
        mlua::Error::CallbackError { cause, .. } | mlua::Error::WithContext { cause, .. } => {
            lua_text(cause)
        }
        other => other.to_string(),
    }
}

fn internal(e: &mlua::Error) -> SprigError {
    SprigError::new(
        SprigCode::LogicRuntimeError,
        format!("the Lua engine failed: {e}"),
    )
}

/// A Lua value as JSON: tables with keys 1..n are arrays, other tables
/// objects (an empty table is `{}`), `nil` and `cjson.null` are `null`,
/// functions and threads are `null`, whole numbers are integers.
///
/// # Errors
/// A recursive table, or one nested past 128 levels.
pub fn to_json(lua: &Lua, v: LuaValue) -> mlua::Result<Value> {
    let j: Value = lua.from_value_with(
        v,
        mlua::serde::DeserializeOptions::new().deny_unsupported_types(false),
    )?;
    Ok(engine::integral(j))
}

/// Checks that `source` compiles as a Lua chunk.
///
/// # Errors
/// 39861 with Lua's message (line included).
pub fn check_syntax(name: &str, source: &str) -> Result<()> {
    let lua = Lua::new_with(StdLib::NONE, LuaOptions::new()).map_err(|e| internal(&e))?;
    lua.set_memory_limit(16 << 20).map_err(|e| internal(&e))?;
    lua.load(source)
        .set_name(format!("={name}"))
        .set_mode(ChunkMode::Text)
        .into_function()
        .map(|_| ())
        .map_err(|e| match e {
            mlua::Error::SyntaxError { message, .. } => {
                SprigError::new(SprigCode::LogicSyntax, message)
            }
            other => SprigError::new(SprigCode::LogicSyntax, other.to_string()),
        })
}

// ---- logic functions -------------------------------------------------------

/// A Lua logic function (`CREATE FUNCTION ... LANGUAGE lua`), checked at
/// deploy and ready to run.
///
/// The body is a chunk run with the arguments: the declared parameters
/// are locals by name (and `...` holds them all). Its first return value
/// is the result ([`engine::event_of`]); a body that returns a function
/// (`return function (a, b) ... end`) has that function called with the
/// arguments instead.
#[derive(Debug, Clone)]
pub struct LuaProgram {
    name: String,
    chunk: String,
    sig: ScriptSig,
}

impl LuaProgram {
    /// Checks `source` for a function `name` with signature `sig`.
    ///
    /// # Errors
    /// 39861 for a syntax error or a parameter that can't be a Lua name.
    pub fn new(name: &str, source: &str, sig: ScriptSig) -> Result<Self> {
        let mut names = Vec::new();
        for (p, _) in &sig.params {
            if !engine::bindable(p, RESERVED) {
                return Err(SprigError::new(
                    SprigCode::LogicSyntax,
                    format!("{name}: parameter {p} can't be a Lua name; rename it"),
                ));
            }
            names.push(p.as_str());
        }
        // On the body's first line, so Lua's line numbers stay the source's.
        let chunk = if names.is_empty() {
            source.to_owned()
        } else {
            format!("local {} = ...; {source}", names.join(", "))
        };
        check_syntax(name, &chunk)?;
        Ok(Self {
            name: name.to_owned(),
            chunk,
            sig,
        })
    }

    /// The signature.
    #[must_use]
    pub fn sig(&self) -> &ScriptSig {
        &self.sig
    }
}

/// Runs `p` with positional `args` on an engine thread.
///
/// # Errors
/// As [`crate::js::run`].
pub async fn run(p: &LuaProgram, args: Vec<Value>, host: Host, limits: Limits) -> Result<Outcome> {
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

fn run_here(
    p: &LuaProgram,
    args: &[Value],
    sync: &Arc<SyncHost>,
    st: &Arc<RunState>,
) -> Result<(String, Value)> {
    let cfg = Config {
        memory_bytes: usize::try_from(st.limits().memory_bytes).unwrap_or(usize::MAX),
        ..Config::default()
    };
    let sb = Sandbox::new(&cfg, Arc::clone(st))?;
    install_logic(sb.lua(), sync).map_err(|e| internal(&e))?;
    sb.seal()?;
    let run = || -> mlua::Result<Value> {
        let lua = sb.lua();
        let f = lua
            .load(p.chunk.as_str())
            .set_name(format!("={}", p.name))
            .set_mode(ChunkMode::Text)
            .into_function()?;
        let mut a = Vec::with_capacity(args.len());
        for v in args {
            a.push(lua.to_value(v)?);
        }
        let r: MultiValue = f.call(MultiValue::from_vec(a.clone()))?;
        let first = r.into_iter().next().unwrap_or(LuaValue::Nil);
        let first = match first {
            LuaValue::Function(g) => {
                let r: MultiValue = g.call(MultiValue::from_vec(a))?;
                r.into_iter().next().unwrap_or(LuaValue::Nil)
            }
            other => other,
        };
        to_json(lua, first)
    };
    let out = run();
    st.heap(sb.lua().used_memory() as u64);
    if let Some(t) = st.tripped() {
        return Err(t);
    }
    let v = out.map_err(|e| sb.error(&e))?;
    st.check_time()?;
    Ok(engine::event_of(v, p.sig.scalar))
}

/// Arguments of a host call as JSON (`nil` is `null`).
fn json_args(lua: &Lua, xs: Variadic<LuaValue>) -> mlua::Result<Vec<Value>> {
    xs.into_iter().map(|x| to_json(lua, x)).collect()
}

fn text_of(v: &LuaValue) -> String {
    match v {
        LuaValue::String(s) => s.to_string_lossy(),
        LuaValue::Nil => "nil".to_owned(),
        LuaValue::Boolean(b) => b.to_string(),
        LuaValue::Integer(i) => i.to_string(),
        LuaValue::Number(n) => {
            if n.fract() == 0.0 && n.abs() < 1e15 {
                format!("{}", *n as i64)
            } else {
                n.to_string()
            }
        }
        other => other.type_name().to_owned(),
    }
}

/// The logic library: `db`, `fetch`, `sprig`, `ctx`, `log`, `print`.
fn install_logic(lua: &Lua, sync: &Arc<SyncHost>) -> mlua::Result<()> {
    let g = lua.globals();
    let db = lua.create_table()?;
    for (name, shape) in [
        ("sql", None),
        ("sqlOne", Some(Shape::One)),
        ("exec", Some(Shape::Exec)),
    ] {
        let h = Arc::clone(sync);
        let at = format!("db.{name}");
        db.set(
            name,
            lua.create_function(move |lua, (sql, rest): (String, Variadic<LuaValue>)| {
                let args = json_args(lua, rest)?;
                let shape = shape.unwrap_or_else(|| crate::bridge::shape_of(&sql));
                let v = h.sql(shape, &sql, &args, &at).map_err(lua_err)?;
                lua.to_value(&v)
            })?,
        )?;
    }
    let h = Arc::clone(sync);
    db.set(
        "call",
        lua.create_function(move |lua, (name, rest): (String, Variadic<LuaValue>)| {
            let args = json_args(lua, rest)?;
            let v = h.call(&name, &args, "db.call").map_err(lua_err)?;
            lua.to_value(&v)
        })?,
    )?;
    // R9: publishing and consuming (chapter 41, "The `db` interface").
    for (name, op) in [
        ("publish", MsgOp::Publish),
        ("commitOffset", MsgOp::CommitOffset),
    ] {
        let h = Arc::clone(sync);
        db.set(
            name,
            lua.create_function(move |lua, xs: Variadic<LuaValue>| {
                let args = json_args(lua, xs)?;
                let v = h.msg(op, &args).map_err(lua_err)?;
                lua.to_value(&v)
            })?,
        )?;
    }
    let mq = lua.create_table()?;
    for (name, op) in [
        ("publish", MsgOp::MqPublish),
        ("get", MsgOp::MqGet),
        ("ack", MsgOp::MqAck),
        ("nack", MsgOp::MqNack),
    ] {
        let h = Arc::clone(sync);
        mq.set(
            name,
            lua.create_function(move |lua, xs: Variadic<LuaValue>| {
                let mut args = json_args(lua, xs)?;
                if op == MsgOp::MqGet {
                    let opts = args.get(1).cloned().unwrap_or(Value::Null);
                    args = crate::messaging::get_args(
                        args.first().cloned().unwrap_or(Value::Null),
                        &opts,
                    );
                }
                if op == MsgOp::MqNack && args.len() < 2 {
                    args.resize(1, Value::Null);
                    args.push(Value::Bool(true));
                }
                let v = h.msg(op, &args).map_err(lua_err)?;
                lua.to_value(&v)
            })?,
        )?;
    }
    db.set("mq", mq)?;
    g.set("db", db)?;

    let h = Arc::clone(sync);
    g.set(
        "fetch",
        lua.create_function(move |lua, (url, init): (String, Option<LuaValue>)| {
            let init = match init {
                Some(v) => to_json(lua, v)?,
                None => Value::Null,
            };
            let mut v = h
                .fetch(engine::fetch_request(&url, &init))
                .map_err(lua_err)?;
            let ok = v["status"]
                .as_u64()
                .is_some_and(|s| (200..300).contains(&s));
            v["ok"] = Value::Bool(ok);
            lua.to_value(&v)
        })?,
    )?;

    let sprig = lua.create_table()?;
    let h = Arc::clone(sync);
    sprig.set(
        "reject",
        lua.create_function(move |_, msg: LuaValue| -> mlua::Result<()> {
            let e = SprigError::new(SprigCode::LogicValidationFailed, text_of(&msg));
            h.st.trip(copy(&e));
            Err(lua_err(e))
        })?,
    )?;
    sprig.set(
        "event",
        lua.create_function(|lua, (name, fields): (String, Option<Table>)| {
            let t = lua.create_table()?;
            t.set("__sprig_event", name)?;
            t.set(
                "fields",
                match fields {
                    Some(f) => f,
                    None => lua.create_table()?,
                },
            )?;
            Ok(t)
        })?,
    )?;
    sprig.set("null", lua.null())?;
    g.set("sprig", sprig)?;

    let h = Arc::clone(sync);
    let log = lua.create_function(move |_, xs: Variadic<LuaValue>| {
        let line: Vec<String> = xs.iter().map(text_of).collect();
        h.log(line.join(" "));
        Ok(())
    })?;
    let ctx = lua.to_value(sync.info())?;
    let ctx = match ctx {
        LuaValue::Table(t) => t,
        _ => lua.create_table()?,
    };
    ctx.set("log", log.clone())?;
    g.set("ctx", ctx)?;
    g.set("log", log.clone())?;
    g.set("print", log)?;
    Ok(())
}

#[cfg(test)]
mod tests;
