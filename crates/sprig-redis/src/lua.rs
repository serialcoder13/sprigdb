//! Lua scripts and functions on SprigDB's
//! one Lua runtime, `sprig_logic_js::lua::Sandbox`.
//!
//! A run gets a fresh sandbox on a blocking thread, with Redis's options
//! (deterministic `math.random`, no new globals) and the `redis.*`
//! library added here. `redis.call` sends the command to the session's
//! task, which runs it inside the script's transaction and sends the
//! reply back; the Lua thread waits meanwhile. Errors are reported as
//! Redis 7.2 does: `<error> script: <name>, on @user_script:<line>.`

use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use mlua::{Lua, MultiValue, Table, Value as LuaValue, Variadic};
use sha1::{Digest, Sha1};
use sprig_logic_js::budget::{Limits, RunState};
use sprig_logic_js::lua::{Config as SandboxConfig, Sandbox};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::d2string;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// `SHA1` of a script, in lower-case hex.
#[must_use]
pub fn sha1_hex(body: &[u8]) -> String {
    use std::fmt::Write as _;
    let d = Sha1::digest(body);
    d.iter().fold(String::with_capacity(40), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// A `redis.call` from a running script.
#[derive(Debug)]
pub struct Call {
    /// The command.
    pub args: Vec<Bytes>,
    /// The script's protocol for the reply (`redis.setresp`).
    pub resp3: bool,
    /// Where the reply goes.
    pub reply: std::sync::mpsc::SyncSender<Frame>,
}

/// What runs.
#[derive(Debug, Clone)]
pub enum Body {
    /// An `EVAL` script.
    Script {
        /// The text, shebang line blanked.
        source: String,
        /// `f_<sha>`.
        name: String,
    },
    /// A function of a library.
    Function {
        /// The library's text, shebang line blanked.
        source: String,
        /// The function's name.
        name: String,
    },
}

/// How a run ended.
#[derive(Debug)]
pub enum Outcome {
    /// The script returned (or raised an error, which Redis commits too).
    Reply(Frame),
    /// A budget ran out or it was killed: roll back.
    Abort(Frame),
}

/// A script's shebang flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    /// `#!lua` was given (flags then default to "may write" only when
    /// `no-writes` is absent).
    pub shebang: bool,
    /// `no-writes`.
    pub no_writes: bool,
}

/// Splits a shebang line off `body`: the flags and the body with the line
/// blanked (so line numbers stay).
///
/// # Errors
/// Redis's messages for another engine or an unknown flag.
pub fn shebang(body: &str) -> Result<(Flags, String), String> {
    if !body.starts_with("#!") {
        return Ok((Flags::default(), body.to_owned()));
    }
    let (line, rest) = body.split_once('\n').unwrap_or((body, ""));
    let mut parts = line[2..].split_whitespace();
    let engine = parts.next().unwrap_or("");
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Could not find engine '{engine}'"));
    }
    let mut f = Flags {
        shebang: true,
        no_writes: false,
    };
    for p in parts {
        if let Some(list) = p.strip_prefix("flags=") {
            for fl in list.split(',').filter(|s| !s.is_empty()) {
                match fl {
                    "no-writes" => f.no_writes = true,
                    "allow-oom" | "allow-stale" | "no-cluster" | "allow-cross-slot-keys" => {}
                    other => return Err(format!("ERR Unexpected flag in script shebang: {other}")),
                }
            }
        } else if p.starts_with("name=") {
            // Libraries: handled by the caller.
        } else {
            return Err(format!("ERR Unknown lua shebang option: {p}"));
        }
    }
    Ok((f, format!("\n{rest}")))
}

/// Lua 5.1's `%.14g` for numbers passed as command arguments.
fn num_arg(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        return format!("{}", n as i64);
    }
    let s = format!("{n:.13e}");
    // Re-render the shortest of %.14g: let Rust parse and print it back.
    let v: f64 = s.parse().unwrap_or(n);
    let t = format!("{v}");
    if t.len() <= 16 { t } else { s }
}

/// Converts a reply to Lua (Redis 7.2's `redisProtocolToLuaType`).
fn to_lua(lua: &Lua, f: &Frame, resp3: bool) -> mlua::Result<LuaValue> {
    let table1 = |k: &str, v: LuaValue| -> mlua::Result<LuaValue> {
        let t = lua.create_table()?;
        t.set(k, v)?;
        Ok(LuaValue::Table(t))
    };
    Ok(match f {
        Frame::Integer(i) => LuaValue::Number(*i as f64),
        Frame::Bulk(b) => LuaValue::String(lua.create_string(b)?),
        Frame::Simple(s) => table1("ok", LuaValue::String(lua.create_string(s)?))?,
        Frame::Error(e) => table1("err", LuaValue::String(lua.create_string(e)?))?,
        Frame::Null(_) => {
            if resp3 {
                LuaValue::Nil
            } else {
                LuaValue::Boolean(false)
            }
        }
        Frame::Boolean(b) => {
            if resp3 {
                LuaValue::Boolean(*b)
            } else {
                LuaValue::Number(if *b { 1.0 } else { 0.0 })
            }
        }
        Frame::Double(d) => {
            if resp3 {
                table1("double", LuaValue::Number(*d))?
            } else {
                LuaValue::String(lua.create_string(d2string(*d))?)
            }
        }
        Frame::DoubleText(t) => {
            if resp3 {
                let d: f64 = std::str::from_utf8(t)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                table1("double", LuaValue::Number(d))?
            } else {
                LuaValue::String(lua.create_string(t)?)
            }
        }
        Frame::BigNumber(n) => {
            if resp3 {
                table1("big_number", LuaValue::String(lua.create_string(n)?))?
            } else {
                LuaValue::String(lua.create_string(n)?)
            }
        }
        Frame::Verbatim(fmt, s) => {
            if resp3 {
                let t = lua.create_table()?;
                t.set("format", lua.create_string(fmt)?)?;
                t.set("string", lua.create_string(s)?)?;
                table1("verbatim_string", LuaValue::Table(t))?
            } else {
                LuaValue::String(lua.create_string(s)?)
            }
        }
        Frame::Array(items) | Frame::Push(items) => {
            let t = lua.create_table()?;
            for (i, it) in items.iter().enumerate() {
                t.raw_set(i + 1, to_lua(lua, it, resp3)?)?;
            }
            LuaValue::Table(t)
        }
        Frame::Set(items) => {
            if resp3 {
                let s = lua.create_table()?;
                for it in items {
                    s.raw_set(to_lua(lua, it, resp3)?, true)?;
                }
                table1("set", LuaValue::Table(s))?
            } else {
                let t = lua.create_table()?;
                for (i, it) in items.iter().enumerate() {
                    t.raw_set(i + 1, to_lua(lua, it, resp3)?)?;
                }
                LuaValue::Table(t)
            }
        }
        Frame::Map(pairs) => {
            if resp3 {
                let m = lua.create_table()?;
                for (k, v) in pairs {
                    m.raw_set(to_lua(lua, k, resp3)?, to_lua(lua, v, resp3)?)?;
                }
                table1("map", LuaValue::Table(m))?
            } else {
                let t = lua.create_table()?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    t.raw_set(2 * i + 1, to_lua(lua, k, resp3)?)?;
                    t.raw_set(2 * i + 2, to_lua(lua, v, resp3)?)?;
                }
                LuaValue::Table(t)
            }
        }
    })
}

/// Converts a script's result to a reply (Redis 7.2's
/// `luaReplyToRedisReply`).
fn from_lua(v: &LuaValue, resp3: bool, depth: usize) -> Frame {
    if depth > 1000 {
        return Frame::err("reached lua stack limit");
    }
    match v {
        LuaValue::String(s) => Frame::Bulk(Bytes::copy_from_slice(&s.as_bytes())),
        LuaValue::Boolean(b) => {
            if resp3 {
                Frame::Boolean(*b)
            } else if *b {
                Frame::Integer(1)
            } else {
                Frame::null()
            }
        }
        LuaValue::Integer(i) => Frame::Integer(*i),
        LuaValue::Number(n) => Frame::Integer(*n as i64),
        LuaValue::Table(t) => {
            let get = |k: &str| t.raw_get::<LuaValue>(k).unwrap_or(LuaValue::Nil);
            if let LuaValue::String(e) = get("err") {
                return Frame::Error(Bytes::copy_from_slice(&e.as_bytes()));
            }
            if let LuaValue::String(s) = get("ok") {
                return Frame::Simple(Bytes::copy_from_slice(&s.as_bytes()));
            }
            // Typed tables convert whatever setresp says (the client's
            // protocol downgrades them), as in Redis.
            {
                if let LuaValue::Number(d) = get("double") {
                    return Frame::Double(d);
                }
                if let LuaValue::Integer(d) = get("double") {
                    return Frame::Double(d as f64);
                }
                if let LuaValue::String(s) = get("big_number") {
                    return Frame::BigNumber(Bytes::copy_from_slice(&s.as_bytes()));
                }
                if let LuaValue::Table(m) = get("map") {
                    let mut out = Vec::new();
                    for (k, v) in m.pairs::<LuaValue, LuaValue>().flatten() {
                        out.push((
                            from_lua(&k, resp3, depth + 1),
                            from_lua(&v, resp3, depth + 1),
                        ));
                    }
                    return Frame::Map(out);
                }
                if let LuaValue::Table(s) = get("set") {
                    let mut out = Vec::new();
                    for (k, _) in s.pairs::<LuaValue, LuaValue>().flatten() {
                        out.push(from_lua(&k, resp3, depth + 1));
                    }
                    return Frame::Set(out);
                }
            }
            let mut out = Vec::new();
            let mut i = 1;
            loop {
                let v: LuaValue = t.raw_get(i).unwrap_or(LuaValue::Nil);
                if v.is_nil() {
                    break;
                }
                out.push(from_lua(&v, resp3, depth + 1));
                i += 1;
            }
            Frame::Array(out)
        }
        _ => Frame::null(),
    }
}

/// Runs first in a script's sandbox, after `redis` exists: the call
/// wrappers and the top-level runner with Redis's error handler.
const PRELUDE: &str = r"
local _error, _xpcall, _pcall, _type, _tostring, _where, _info, _unpack, _select =
  error, xpcall, pcall, type, tostring, __sprig_where, __sprig_errinfo, unpack, select
__sprig_where, __sprig_errinfo = nil, nil
-- pcall(redis.call, ...) sees Redis's error table.
local function conv(ok, ...)
  if ok then return ok, ... end
  local e = _info((...))
  if _type(e) == 'table' then e = {err = e.err} end
  return ok, e
end
pcall = function(f, ...) return conv(_pcall(f, ...)) end
__sprig_run = function(f, ...)
  local n, args = _select('#', ...), {...}
  return _xpcall(function() return f(_unpack(args, 1, n)) end, function(err)
    err = _info(err)
    if _type(err) ~= 'table' then err = {err = 'ERR ' .. _tostring(err)} end
    if not err.line then
      local src, line = _where()
      if line then err.source = src; err.line = line end
    end
    return err
  end)
end
";

/// A running script: tenant, is a function, start, budgets.
type RunEntry = (String, bool, Instant, Arc<RunState>);

/// Running scripts, for `SCRIPT KILL` and `FUNCTION KILL`.
#[derive(Debug, Default)]
pub struct Running {
    next: AtomicU64,
    runs: Mutex<HashMap<u64, RunEntry>>,
}

impl Running {
    /// Registers a run; its id.
    pub fn add(&self, scope: &str, function: bool, st: &Arc<RunState>) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut r) = self.runs.lock() {
            r.insert(
                id,
                (scope.to_owned(), function, Instant::now(), Arc::clone(st)),
            );
        }
        id
    }

    /// Forgets a run.
    pub fn remove(&self, id: u64) {
        if let Ok(mut r) = self.runs.lock() {
            r.remove(&id);
        }
    }

    /// Cancels `scope`'s runs of the kind older than `after`; how many.
    pub fn kill(&self, scope: &str, function: bool, after: Duration) -> u32 {
        let Ok(r) = self.runs.lock() else { return 0 };
        let mut n = 0;
        for (s, f, start, st) in r.values() {
            if s == scope && *f == function && start.elapsed() >= after {
                st.cancel();
                n += 1;
            }
        }
        n
    }

    /// Whether `scope` has a run of the kind.
    #[must_use]
    pub fn busy(&self, scope: &str, function: bool) -> bool {
        self.runs
            .lock()
            .is_ok_and(|r| r.values().any(|(s, f, _, _)| s == scope && *f == function))
    }
}

/// Where the user's code is in the stack: the first frame of
/// `@user_script` or `@user_function`.
fn where_now(lua: &Lua) -> Option<(String, usize)> {
    for level in 1..64 {
        let got = lua.inspect_stack(level, |d| {
            let s = d.source();
            let src = s
                .source
                .map(std::borrow::Cow::into_owned)
                .unwrap_or_default();
            (src, d.current_line())
        })?;
        if (got.0 == "@user_script" || got.0 == "@user_function")
            && let Some(l) = got.1
        {
            return Some((got.0, l));
        }
    }
    None
}

/// Builds the `redis` table in `lua`.
fn install_redis(
    lua: &Lua,
    calls: Option<UnboundedSender<Call>>,
    resp: &Rc<Cell<bool>>,
    registry: Option<Rc<std::cell::RefCell<Vec<Registered>>>>,
) -> mlua::Result<()> {
    let g = lua.globals();
    let redis = lua.create_table()?;
    let resp_c = Rc::clone(resp);
    // One `redis.call`: the reply, or the error text.
    let do_call = Rc::new(
        move |lua: &Lua, args: Variadic<LuaValue>| -> mlua::Result<Result<LuaValue, Vec<u8>>> {
            let Some(calls) = &calls else {
                return Ok(Err(
                    b"ERR attempt to call field 'call' (a nil value)".to_vec()
                ));
            };
            if args.is_empty() {
                return Ok(Err(
                    b"ERR Please specify at least one argument for this redis lib call".to_vec(),
                ));
            }
            let mut cmd = Vec::with_capacity(args.len());
            for a in args.iter() {
                match a {
                    LuaValue::String(s) => cmd.push(Bytes::copy_from_slice(&s.as_bytes())),
                    LuaValue::Integer(i) => cmd.push(Bytes::from(i.to_string())),
                    LuaValue::Number(n) => cmd.push(Bytes::from(num_arg(*n))),
                    _ => {
                        return Ok(Err(
                            b"ERR Lua redis lib command arguments must be strings or integers"
                                .to_vec(),
                        ));
                    }
                }
            }
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let r3 = resp_c.get();
            if calls
                .send(Call {
                    args: cmd,
                    resp3: r3,
                    reply: tx,
                })
                .is_err()
            {
                return Err(mlua::Error::RuntimeError(
                    "the script's session is gone".into(),
                ));
            }
            let Ok(f) = rx.recv() else {
                return Err(mlua::Error::RuntimeError(
                    "the script's session is gone".into(),
                ));
            };
            match &f {
                Frame::Error(e) => Ok(Err(e.to_vec())),
                other => Ok(Ok(to_lua(lua, other, r3)?)),
            }
        },
    );
    let c1 = Rc::clone(&do_call);
    // redis.call is a host function, so a `return redis.call(...)` keeps the
    // caller's frame and the error knows its line, as in Redis.
    redis.set(
        "call",
        lua.create_function(move |lua, args: Variadic<LuaValue>| match c1(lua, args)? {
            Ok(v) => Ok(v),
            Err(msg) => {
                let (source, line) =
                    where_now(lua).map_or((None, None), |(s, l)| (Some(s), Some(l)));
                Err(mlua::Error::external(ScriptErr { msg, source, line }))
            }
        })?,
    )?;
    let c2 = Rc::clone(&do_call);
    redis.set(
        "pcall",
        lua.create_function(move |lua, args: Variadic<LuaValue>| match c2(lua, args)? {
            Ok(v) => Ok(v),
            Err(msg) => {
                let t = lua.create_table()?;
                t.set("err", lua.create_string(&msg)?)?;
                Ok(LuaValue::Table(t))
            }
        })?,
    )?;
    g.set(
        "__sprig_errinfo",
        lua.create_function(|lua, v: LuaValue| {
            let LuaValue::Error(e) = &v else { return Ok(v) };
            let Some(se) = e.downcast_ref::<ScriptErr>() else {
                return Ok(v);
            };
            let t = lua.create_table()?;
            t.set("err", lua.create_string(&se.msg)?)?;
            if let (Some(s), Some(l)) = (&se.source, se.line) {
                t.set("source", s.as_str())?;
                t.set("line", l)?;
            }
            Ok(LuaValue::Table(t))
        })?,
    )?;
    g.set(
        "__sprig_where",
        lua.create_function(|lua, ()| {
            Ok(match where_now(lua) {
                Some((s, l)) => (Some(s), Some(l)),
                None => (None, None),
            })
        })?,
    )?;
    let t1 = |k: &'static str| {
        lua.create_function(move |lua, v: LuaValue| {
            let t = lua.create_table()?;
            t.set(k, v)?;
            Ok(t)
        })
    };
    redis.set("error_reply", t1("err")?)?;
    redis.set("status_reply", t1("ok")?)?;
    redis.set(
        "sha1hex",
        lua.create_function(|_, s: mlua::LuaString| Ok(sha1_hex(&s.as_bytes())))?,
    )?;
    redis.set(
        "log",
        lua.create_function(|_, args: Variadic<LuaValue>| {
            tracing::debug!(n = args.len(), "redis.log from a script");
            Ok(())
        })?,
    )?;
    for (k, v) in [
        ("LOG_DEBUG", 0),
        ("LOG_VERBOSE", 1),
        ("LOG_NOTICE", 2),
        ("LOG_WARNING", 3),
    ] {
        redis.set(k, v)?;
    }
    for (k, v) in [
        ("REPL_NONE", 0),
        ("REPL_AOF", 1),
        ("REPL_SLAVE", 2),
        ("REPL_REPLICA", 2),
        ("REPL_ALL", 3),
    ] {
        redis.set(k, v)?;
    }
    let resp_s = Rc::clone(resp);
    redis.set(
        "setresp",
        lua.create_function(move |_, n: i64| match n {
            2 => {
                resp_s.set(false);
                Ok(())
            }
            3 => {
                resp_s.set(true);
                Ok(())
            }
            _ => Err(mlua::Error::RuntimeError(
                "RESP version must be 2 or 3.".into(),
            )),
        })?,
    )?;
    redis.set("set_repl", lua.create_function(|_, _: LuaValue| Ok(()))?)?;
    redis.set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.set("breakpoint", lua.create_function(|_, ()| Ok(false))?)?;
    redis.set(
        "debug",
        lua.create_function(|_, _: Variadic<LuaValue>| Ok(()))?,
    )?;
    redis.set(
        "acl_check_cmd",
        lua.create_function(|_, _: Variadic<LuaValue>| Ok(true))?,
    )?;
    if let Some(reg) = registry {
        redis.set(
            "register_function",
            lua.create_function(move |_, args: Variadic<LuaValue>| {
                let (name, cb, flags) = match args.first() {
                    Some(LuaValue::Table(t)) => {
                        let name: String = t.get("function_name").map_err(|_| {
                            mlua::Error::RuntimeError("ERR function_name argument given to redis.register_function must be a string".into())
                        })?;
                        let cb: mlua::Function = t.get("callback").map_err(|_| {
                            mlua::Error::RuntimeError("ERR callback argument given to redis.register_function must be a function".into())
                        })?;
                        let mut flags = Vec::new();
                        if let Ok(ft) = t.get::<Table>("flags") {
                            for f in ft.sequence_values::<String>().flatten() {
                                flags.push(f);
                            }
                        }
                        (name, cb, flags)
                    }
                    Some(LuaValue::String(s)) => {
                        let cb = match args.get(1) {
                            Some(LuaValue::Function(f)) => f.clone(),
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "ERR wrong number of arguments to redis.register_function".into(),
                                ));
                            }
                        };
                        (s.to_str()?.to_owned(), cb, Vec::new())
                    }
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "ERR wrong number of arguments to redis.register_function".into(),
                        ));
                    }
                };
                if name.is_empty()
                    || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(mlua::Error::RuntimeError(
                        "ERR Function names can only contain letters, numbers, or underscores(_) and must be at least one character long".into(),
                    ));
                }
                let mut r = reg.borrow_mut();
                if r.iter().any(|x| x.name == name) {
                    return Err(mlua::Error::RuntimeError(
                        "ERR Function already exists in the library".into(),
                    ));
                }
                r.push(Registered {
                    name,
                    callback: cb,
                    no_writes: flags.iter().any(|f| f == "no-writes"),
                    flags,
                });
                Ok(())
            })?,
        )?;
    }
    g.set("redis", redis)?;
    Ok(())
}

/// A `redis.call` error, raised from the host with where it happened.
#[derive(Debug, Clone)]
struct ScriptErr {
    msg: Vec<u8>,
    source: Option<String>,
    line: Option<usize>,
}

impl std::fmt::Display for ScriptErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.msg))
    }
}

impl std::error::Error for ScriptErr {}

/// A function a library registered.
#[derive(Debug, Clone)]
pub struct Registered {
    /// Its name.
    pub name: String,
    /// The callback.
    pub callback: mlua::Function,
    /// `no-writes` among its flags.
    pub no_writes: bool,
    /// Its flags.
    pub flags: Vec<String>,
}

/// What `FUNCTION LOAD` learns from a library.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibraryInfo {
    /// `name=` of the shebang.
    pub name: String,
    /// `(function, flags)`.
    pub functions: Vec<(String, Vec<String>)>,
}

/// The library name of a `#!lua name=<lib>` body.
///
/// # Errors
/// Redis's messages.
pub fn library_name(code: &str) -> Result<String, String> {
    let Some(first) = code.lines().next().filter(|l| l.starts_with("#!")) else {
        return Err("ERR Missing library metadata".into());
    };
    let mut parts = first[2..].split_whitespace();
    let engine = parts.next().unwrap_or("");
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Engine '{engine}' not found"));
    }
    let mut name = None;
    for p in parts {
        match p.split_once('=') {
            Some(("name", v)) => name = Some(v.to_owned()),
            _ => return Err(format!("ERR Invalid metadata value given: {p}")),
        }
    }
    let Some(name) = name else {
        return Err("ERR Library name was not given".into());
    };
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err("ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long".into());
    }
    Ok(name)
}

fn sandbox(cfg: &crate::env::Config, st: Arc<RunState>) -> Result<Sandbox, Frame> {
    Sandbox::new(
        &SandboxConfig {
            memory_bytes: cfg.lua_memory_bytes,
            hook_every: 10_000,
            deterministic: true,
            protect_globals: true,
        },
        st,
    )
    .map_err(|e| Frame::err(&e.message))
}

fn lua_frame(e: &mlua::Error) -> Frame {
    let msg = match e {
        mlua::Error::SyntaxError { message, .. } => message.clone(),
        other => other.to_string(),
    };
    Frame::err(&format!("Error compiling script (new function): {msg}"))
}

/// Compiles a script without running it (`SCRIPT LOAD`).
///
/// # Errors
/// The compile error as Redis words it.
pub fn check(body: &str) -> Result<(), Frame> {
    let lua = Lua::new();
    lua.load(body)
        .set_name("@user_script")
        .into_function()
        .map(|_| ())
        .map_err(|e| lua_frame(&e))
}

/// Loads a library to learn its functions (`FUNCTION LOAD`), with no
/// `redis.call`.
///
/// # Errors
/// Redis's messages.
pub fn load_library(code: &str, cfg: &crate::env::Config) -> Result<LibraryInfo, Frame> {
    let name = library_name(code).map_err(Frame::error)?;
    let body = format!("\n{}", code.split_once('\n').map_or("", |x| x.1));
    let st = Arc::new(RunState::new(Limits {
        wall: cfg.lua_time_limit,
        memory_bytes: cfg.lua_memory_bytes as u64,
        wall_setting: "redis.lua_time_limit_ms",
        ..Limits::default()
    }));
    let sb = sandbox(cfg, st)?;
    let reg = Rc::new(std::cell::RefCell::new(Vec::new()));
    install_redis(
        sb.lua(),
        None,
        &Rc::new(Cell::new(false)),
        Some(Rc::clone(&reg)),
    )
    .map_err(|e| Frame::err(&e.to_string()))?;
    // A library may not call redis.call while loading.
    if let Ok(r) = sb.lua().globals().get::<Table>("redis") {
        let _ = r.set("call", LuaValue::Nil);
        let _ = r.set("pcall", LuaValue::Nil);
    }
    sb.seal().map_err(|e| Frame::err(&e.message))?;
    let f = sb
        .lua()
        .load(&body)
        .set_name("@user_function")
        .into_function()
        .map_err(|e| Frame::err(&format!("Error compiling function: {e}")))?;
    f.call::<()>(())
        .map_err(|e| Frame::err(&format!("Error registering functions: {}", short(&e))))?;
    let r = reg.borrow();
    if r.is_empty() {
        return Err(Frame::err("No functions registered"));
    }
    Ok(LibraryInfo {
        name,
        functions: r
            .iter()
            .map(|x| (x.name.clone(), x.flags.clone()))
            .collect(),
    })
}

fn short(e: &mlua::Error) -> String {
    match e {
        mlua::Error::RuntimeError(m) => m.clone(),
        mlua::Error::CallbackError { cause, .. } | mlua::Error::WithContext { cause, .. } => {
            short(cause)
        }
        other => other.to_string(),
    }
}

/// A started run: its `redis.call`s, then its outcome.
#[derive(Debug)]
pub struct Started {
    /// Calls to serve.
    pub calls: UnboundedReceiver<Call>,
    /// The outcome.
    pub done: tokio::task::JoinHandle<Outcome>,
    /// Its budgets (cancel to kill).
    pub state: Arc<RunState>,
}

/// Starts `body` with `KEYS` and `ARGV` on a blocking thread.
#[must_use]
pub fn start(
    body: Body,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
    resp3_client: bool,
    cfg: &crate::env::Config,
) -> Started {
    let (tx, rx) = unbounded_channel();
    let st = Arc::new(RunState::new(Limits {
        wall: cfg.lua_time_limit,
        memory_bytes: cfg.lua_memory_bytes as u64,
        wall_setting: "redis.lua_time_limit_ms",
        ..Limits::default()
    }));
    let st2 = Arc::clone(&st);
    let cfg = cfg.clone();
    let done = tokio::task::spawn_blocking(move || {
        run_blocking(&body, &keys, &args, tx, st2, &cfg, resp3_client)
    });
    Started {
        calls: rx,
        done,
        state: st,
    }
}

#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
fn run_blocking(
    body: &Body,
    keys: &[Bytes],
    args: &[Bytes],
    tx: UnboundedSender<Call>,
    st: Arc<RunState>,
    cfg: &crate::env::Config,
    _resp3_client: bool,
) -> Outcome {
    let budget = |st: &RunState| -> Outcome {
        let e = st.tripped();
        let killed = e.as_ref().is_some_and(|e| e.message.contains("cancelled"));
        if killed {
            Outcome::Abort(Frame::err(match body {
                Body::Script { .. } => "Script killed by user with SCRIPT KILL...",
                Body::Function { .. } => "Script killed by user with FUNCTION KILL...",
            }))
        } else {
            let what = e.map_or_else(String::new, |e| e.message);
            let which = if what.contains("memory") {
                format!(
                    "memory: the script used more than redis.lua_memory_limit_mb = {} MB",
                    cfg.lua_memory_bytes >> 20
                )
            } else {
                format!(
                    "wall time: the script ran past redis.lua_time_limit_ms = {} ms",
                    cfg.lua_time_limit.as_millis()
                )
            };
            Outcome::Abort(crate::reply::sprig(
                sprig_common::SprigCode::RedisLuaBudgetExceeded,
                &format!("{which}; rolled back"),
            ))
        }
    };
    let sb = match sandbox(cfg, Arc::clone(&st)) {
        Ok(s) => s,
        Err(f) => return Outcome::Reply(f),
    };
    let lua = sb.lua();
    let resp = Rc::new(Cell::new(false));
    let reg = Rc::new(std::cell::RefCell::new(Vec::new()));
    let is_fn = matches!(body, Body::Function { .. });
    let setup = (|| -> mlua::Result<()> {
        install_redis(
            lua,
            Some(tx.clone()),
            &resp,
            if is_fn { Some(Rc::clone(&reg)) } else { None },
        )?;
        lua.load(PRELUDE).set_name("=sprig").exec()?;
        if !is_fn {
            let k = lua.create_table()?;
            for (i, x) in keys.iter().enumerate() {
                k.raw_set(i + 1, lua.create_string(x)?)?;
            }
            let a = lua.create_table()?;
            for (i, x) in args.iter().enumerate() {
                a.raw_set(i + 1, lua.create_string(x)?)?;
            }
            lua.globals().set("KEYS", k)?;
            lua.globals().set("ARGV", a)?;
        }
        Ok(())
    })();
    if let Err(e) = setup {
        return Outcome::Reply(Frame::err(&e.to_string()));
    }
    if let Err(e) = sb.seal() {
        return Outcome::Reply(Frame::err(&e.message));
    }
    let (source, chunk, fname) = match body {
        Body::Script { source, name } => (source, "@user_script", name.clone()),
        Body::Function { source, name } => (source, "@user_function", name.clone()),
    };
    let f = match lua.load(source.as_str()).set_name(chunk).into_function() {
        Ok(f) => f,
        Err(e) => return Outcome::Reply(lua_frame(&e)),
    };
    let target = if is_fn {
        // Run the library to register, then call the function.
        if let Err(e) = f.call::<()>(()) {
            return Outcome::Reply(Frame::err(&format!(
                "Error registering functions: {}",
                short(&e)
            )));
        }
        let r = reg.borrow();
        let Some(found) = r.iter().find(|x| x.name == fname) else {
            return Outcome::Reply(Frame::err("Function not found"));
        };
        found.callback.clone()
    } else {
        f
    };
    let run: mlua::Function = match lua.globals().get("__sprig_run") {
        Ok(r) => r,
        Err(e) => return Outcome::Reply(Frame::err(&e.to_string())),
    };
    let result = if is_fn {
        let mk = |xs: &[Bytes]| -> mlua::Result<Table> {
            let t = lua.create_table()?;
            for (i, x) in xs.iter().enumerate() {
                t.raw_set(i + 1, lua.create_string(x)?)?;
            }
            Ok(t)
        };
        match (mk(keys), mk(args)) {
            (Ok(k), Ok(a)) => run.call::<MultiValue>((target, k, a)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    } else {
        run.call::<MultiValue>(target)
    };
    if st.tripped().is_some() || st.interrupted() {
        return budget(&st);
    }
    let vals = match result {
        Ok(v) => v,
        Err(e) => {
            if st.tripped().is_some() {
                return budget(&st);
            }
            return Outcome::Reply(Frame::err(&format!(
                "Error running script {fname}: {}",
                short(&e)
            )));
        }
    };
    let mut it = vals.into_iter();
    let ok = matches!(it.next(), Some(LuaValue::Boolean(true)));
    let v = it.next().unwrap_or(LuaValue::Nil);
    if ok {
        return Outcome::Reply(from_lua(&v, resp.get(), 0));
    }
    // An error table {err, source, line}.
    if let LuaValue::Table(t) = &v {
        let msg: String = t.raw_get::<mlua::LuaString>("err").map_or_else(
            |_| "ERR execution failure".into(),
            |s| String::from_utf8_lossy(&s.as_bytes()).into_owned(),
        );
        let src: Option<String> = t.raw_get("source").ok();
        let line: Option<i64> = t.raw_get("line").ok();
        let text = match (src, line) {
            (Some(s), Some(l)) => format!("{msg} script: {fname}, on {s}:{l}."),
            _ => msg,
        };
        return Outcome::Reply(Frame::error(text));
    }
    Outcome::Reply(Frame::err(&format!(
        "Error running script {fname}, execution failure"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shebangs() {
        let (f, b) = shebang("#!lua flags=no-writes\nreturn 1").unwrap();
        assert!(f.no_writes && f.shebang);
        assert_eq!(b, "\nreturn 1");
        assert!(shebang("#!js\nx").is_err());
        assert!(shebang("#!lua flags=bogus\nx").is_err());
        assert_eq!(library_name("#!lua name=mylib\nx").unwrap(), "mylib");
        assert!(library_name("return 1").is_err());
        assert_eq!(
            sha1_hex(b"return 1"),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
    }

    #[test]
    fn numbers_as_arguments() {
        assert_eq!(num_arg(5.0), "5");
        assert_eq!(num_arg(1.5), "1.5");
    }
}
