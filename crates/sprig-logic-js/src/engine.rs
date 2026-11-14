//! Engine threads and the synchronous `db` of the script runtimes.
//!
//! QuickJS and Lua are synchronous interpreters: a `db.sql` call must
//! return its rows to the code that made it, with no `await`. So a
//! JavaScript or Lua invocation runs on a blocking thread
//! ([`run_blocking`]), and each `db` or outbound call parks that thread
//! on the async executor ([`SyncHost`]) until the host replies, which in
//! the gateway means the caller's own session has run the statement.
//!
//! ```text
//! session task                         engine thread (spawn_blocking)
//!   run(program, args) ─ permit ─────►  fresh runtime, budgets armed
//!     select { run, db requests } ◄──── db.sql(…) parks: block_on(host.db.exec(op))
//!     runs the statement as the caller ─► reply
//!   ◄──────────────────────────────────  outcome, or the run's error
//! ```
//!
//! **Threads.** At most `logic.engine_threads` invocations run at once
//! (0 = the machine's cores); up to [`QUEUE_PER_THREAD`] per thread wait
//! for one, and beyond that a call fails at once with 39871, which the
//! client may retry.
//!
//! **Cancellation.** Dropping the future (the client went away, `KILL
//! QUERY`) sets the run's cancel flag; the runtime's interrupt check sees
//! it within a few thousand instructions and stops the run.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::Semaphore;

use crate::bridge::{self, Shape};
use crate::budget::{RunState, copy};
use crate::kirun::Host;
use crate::outbound;

/// Calls that may wait for an engine thread, per thread.
pub const QUEUE_PER_THREAD: usize = 16;

/// Log lines kept per invocation (`console.log`, Lua `log`).
pub const MAX_LOG_LINES: usize = 100;

struct Pool {
    threads: usize,
    permits: Semaphore,
    waiting: AtomicUsize,
}

static THREADS: AtomicUsize = AtomicUsize::new(0);

static POOL: LazyLock<Pool> = LazyLock::new(|| {
    let mut n = THREADS.load(Ordering::Relaxed);
    if n == 0 {
        n = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    }
    Pool {
        threads: n,
        permits: Semaphore::new(n),
        waiting: AtomicUsize::new(0),
    }
});

/// Sets `logic.engine_threads` (0 = cores). Only the first call before
/// any invocation has an effect: the pool is sized once per process.
pub fn configure_threads(n: usize) {
    THREADS.store(n, Ordering::Relaxed);
}

/// Engine threads of this process.
#[must_use]
pub fn threads() -> usize {
    POOL.threads
}

/// Sets the run's cancel flag when the invocation's future is dropped.
struct CancelOnDrop(Arc<RunState>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Runs `f` on an engine thread, under the pool's limit.
///
/// # Errors
/// 39871 when the queue is full; `f`'s error; 39866 when `f` panics.
pub async fn run_blocking<T, F>(st: Arc<RunState>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let pool = &*POOL;
    let queued = pool.waiting.fetch_add(1, Ordering::SeqCst);
    if queued >= pool.threads * QUEUE_PER_THREAD {
        pool.waiting.fetch_sub(1, Ordering::SeqCst);
        return Err(SprigError::new(
            SprigCode::LogicEngineBusy,
            format!(
                "the logic engine is busy: {} calls running and {queued} waiting; retry",
                pool.threads
            ),
        ));
    }
    let permit = pool.permits.acquire().await;
    pool.waiting.fetch_sub(1, Ordering::SeqCst);
    let _permit = permit.map_err(|_| SprigError::internal("the logic engine pool is closed"))?;
    let _cancel = CancelOnDrop(Arc::clone(&st));
    match tokio::task::spawn_blocking(f).await {
        Ok(r) => r,
        Err(e) if e.is_panic() => Err(SprigError::new(
            SprigCode::LogicRuntimeError,
            "the logic engine failed (a panic); the call was stopped",
        )),
        Err(_) => Err(SprigError::new(
            SprigCode::LogicBudgetExceeded,
            "logic budget exceeded, cancelled: the caller stopped the call",
        )),
    }
}

/// The host as code on an engine thread sees it: every call blocks the
/// thread until the host answers, and is charged to the run's budgets.
pub struct SyncHost {
    host: Host,
    handle: tokio::runtime::Handle,
    /// The run's budgets.
    pub st: Arc<RunState>,
    errors: Mutex<Vec<SprigError>>,
    logs: Mutex<Vec<String>>,
}

impl std::fmt::Debug for SyncHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncHost").finish_non_exhaustive()
    }
}

impl SyncHost {
    /// A host for one run; `handle` is the async runtime the host's
    /// futures run on (taken on the async side, before the thread starts).
    #[must_use]
    pub fn new(host: Host, handle: tokio::runtime::Handle, st: Arc<RunState>) -> Self {
        Self {
            host,
            handle,
            st,
            errors: Mutex::new(Vec::new()),
            logs: Mutex::new(Vec::new()),
        }
    }

    /// The call's context for `ctx` (user, tenant, database, version).
    #[must_use]
    pub fn info(&self) -> &Value {
        &self.host.info
    }

    /// Keeps `e` and returns its index, so an error the code doesn't
    /// catch comes back out as the original (1142 stays 1142).
    pub fn keep(&self, e: SprigError) -> usize {
        let mut v = self
            .errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        v.push(e);
        v.len() - 1
    }

    /// The error kept under `n`.
    #[must_use]
    pub fn kept(&self, n: usize) -> Option<SprigError> {
        self.errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(n)
            .map(copy)
    }

    /// One log line (`console.log`, `ctx.log`, Lua `log`); the first
    /// [`MAX_LOG_LINES`] are kept for the outcome, all go to tracing.
    pub fn log(&self, line: String) {
        tracing::debug!(target: "sprig_logic::code", "{line}");
        self.host.db.log(&line);
        let mut v = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if v.len() < MAX_LOG_LINES {
            v.push(line);
        }
    }

    /// The log lines kept.
    #[must_use]
    pub fn logs(&self) -> Vec<String> {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// `db.sql`, `db.sqlOne`, `db.exec`: binds, runs as the caller, and
    /// converts the reply ([`bridge::script_value`]).
    ///
    /// # Errors
    /// Budgets (39864), binding (39874, 39866), the statement's own error.
    pub fn sql(&self, shape: Shape, sql: &str, args: &[Value], at: &str) -> Result<Value> {
        self.budgeted(|| self.sql_inner(shape, sql, args, at))
    }

    /// A budget error trips the run, so the code can't catch it and go on.
    fn budgeted(&self, f: impl FnOnce() -> Result<Value>) -> Result<Value> {
        let r = f();
        if let Err(e) = &r
            && e.code() == Some(SprigCode::LogicBudgetExceeded)
        {
            self.st.trip(copy(e));
        }
        r
    }

    fn sql_inner(&self, shape: Shape, sql: &str, args: &[Value], at: &str) -> Result<Value> {
        self.st.data_op()?;
        let sql = match &self.host.default_db {
            Some(db) => bridge::qualify(sql, db),
            None => sql.to_owned(),
        };
        let op = bridge::bind(shape, &sql, args, at)?;
        let reply = self.handle.block_on(self.host.db.exec(op))?;
        self.st.check_time()?;
        bridge::script_value(shape, reply, &self.st, at)
    }

    /// `db.call(name, ...args)`.
    ///
    /// # Errors
    /// As [`SyncHost::sql`]; 39860 for a bad name; the callee's error.
    pub fn call(&self, name: &str, args: &[Value], at: &str) -> Result<Value> {
        self.budgeted(|| {
            self.st.data_op()?;
            let op = bridge::call_op(name, args)?;
            let reply = self.handle.block_on(self.host.db.exec(op))?;
            self.st.check_time()?;
            bridge::script_value(Shape::Call, reply, &self.st, at)
        })
    }

    /// `db.publish`, `db.commitOffset` and `db.mq.*`
    /// ([`crate::messaging`]): one statement through the caller's session.
    ///
    /// # Errors
    /// As [`SyncHost::sql`].
    pub fn msg(&self, op: crate::messaging::MsgOp, args: &[Value]) -> Result<Value> {
        self.budgeted(|| {
            self.st.data_op()?;
            let v = self.handle.block_on(crate::messaging::call(
                &*self.host.db,
                op,
                args,
                self.host.default_db.as_deref(),
            ))?;
            self.st.check_time()?;
            Ok(v)
        })
    }

    /// `fetch(url, init)`: `{status, headers, body}`.
    ///
    /// # Errors
    /// 39867 off the allowlist; 39864 for its timeout or size limit, or
    /// the run's data operations.
    pub fn fetch(&self, req: outbound::Request) -> Result<Value> {
        self.budgeted(|| self.fetch_inner(req))
    }

    fn fetch_inner(&self, req: outbound::Request) -> Result<Value> {
        self.st.data_op()?;
        let resp = self.handle.block_on(self.host.outbound.fetch(req))?;
        self.st.check_time()?;
        let headers: serde_json::Map<String, Value> = resp
            .headers
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        Ok(serde_json::json!({
            "status": resp.status,
            "headers": headers,
            "body": resp.body,
        }))
    }
}

/// `fetch`'s `init` (`{method, headers, body}`) as a request.
#[must_use]
pub fn fetch_request(url: &str, init: &Value) -> outbound::Request {
    let method = init
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET")
        .to_ascii_uppercase();
    let headers = match init.get("headers") {
        Some(Value::Object(o)) => o
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_str().map_or_else(|| v.to_string(), str::to_owned),
                )
            })
            .collect(),
        _ => Vec::new(),
    };
    let body = match init.get("body") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
    };
    outbound::Request {
        method,
        url: url.to_owned(),
        headers,
        body,
    }
}

/// How a script's return value becomes the call's event (the
/// function's result row: `event`, then the event's fields):
///
/// - `sprig.event(name, fields)` → that event;
/// - a function declared `RETURNS t` (`scalar`) → `output {value}`;
/// - an object → `output` with its members as fields;
/// - nothing (`undefined`, `nil`) → `output {}`;
/// - anything else → `output {value}`.
#[must_use]
pub fn event_of(v: Value, scalar: bool) -> (String, Value) {
    if let Value::Object(o) = &v
        && let Some(Value::String(name)) = o.get("__sprig_event")
    {
        let fields = match o.get("fields") {
            Some(Value::Object(f)) => Value::Object(f.clone()),
            None | Some(Value::Null) => Value::Object(serde_json::Map::new()),
            Some(other) => serde_json::json!({ "value": other }),
        };
        return (name.clone(), fields);
    }
    let fields = match v {
        Value::Null if !scalar => Value::Object(serde_json::Map::new()),
        Value::Object(o) if !scalar => Value::Object(o),
        other => serde_json::json!({ "value": other }),
    };
    ("output".to_owned(), fields)
}

/// A parameter's type, from the declaration's SQL type, for converting
/// a call's arguments as MariaDB hands them over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamTy {
    /// Integer types.
    Int,
    /// `DECIMAL`, `FLOAT`, `DOUBLE`.
    Float,
    /// `BOOLEAN`.
    Bool,
    /// Text, dates and anything else.
    Str,
    /// `JSON`: parsed.
    Json,
}

/// A script function's signature: parameter names and types in call
/// order, and whether it was declared `RETURNS t` (a scalar function).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScriptSig {
    /// Parameters, in call order.
    pub params: Vec<(String, ParamTy)>,
    /// Declared `RETURNS t`.
    pub scalar: bool,
}

impl ScriptSig {
    /// Converts a call's arguments, as SQL hands them over (numbers,
    /// text, `0`/`1` for booleans, JSON as text), to the parameter types.
    ///
    /// # Errors
    /// 39874 for the wrong number of arguments.
    pub fn coerce(&self, name: &str, args: Vec<Value>) -> Result<Vec<Value>> {
        if args.len() != self.params.len() {
            return Err(SprigError::new(
                SprigCode::LogicSignatureMismatch,
                format!(
                    "{name} takes {} arguments ({}), not {}",
                    self.params.len(),
                    self.params
                        .iter()
                        .map(|(n, _)| n.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    args.len()
                ),
            ));
        }
        Ok(args
            .into_iter()
            .zip(&self.params)
            .map(|(v, (_, t))| coerce(v, *t))
            .collect())
    }

    /// 39874 unless `n` arguments fit.
    ///
    /// # Errors
    /// 39874.
    pub fn check_arity(&self, name: &str, n: usize) -> Result<()> {
        if n == self.params.len() {
            Ok(())
        } else {
            self.coerce(name, vec![Value::Null; n]).map(|_| ())
        }
    }
}

fn coerce(v: Value, t: ParamTy) -> Value {
    match (t, v) {
        (_, Value::Null) => Value::Null,
        (ParamTy::Int, Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.trim().parse::<f64>().map(Value::from))
            .unwrap_or(Value::String(s)),
        (ParamTy::Int, Value::Bool(b)) => Value::from(i64::from(b)),
        (ParamTy::Float, Value::String(s)) => s
            .trim()
            .parse::<f64>()
            .map(Value::from)
            .unwrap_or(Value::String(s)),
        (ParamTy::Bool, Value::Number(n)) => Value::Bool(n.as_f64().is_some_and(|x| x != 0.0)),
        (ParamTy::Bool, Value::String(s)) => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Value::Bool(true),
            "0" | "false" => Value::Bool(false),
            _ => Value::String(s),
        },
        (ParamTy::Str, Value::Number(n)) => Value::String(n.to_string()),
        (ParamTy::Str, Value::Bool(b)) => Value::String(if b { "1" } else { "0" }.to_owned()),
        (ParamTy::Json, Value::String(s)) => serde_json::from_str(&s).unwrap_or(Value::String(s)),
        (_, v) => v,
    }
}

/// Whether `name` can be bound as a local of `lang`'s code (an
/// identifier that isn't a reserved word).
#[must_use]
pub fn bindable(name: &str, reserved: &[&str]) -> bool {
    let mut cs = name.chars();
    cs.next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && cs.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !reserved.contains(&name)
}

/// Whole doubles within ±2^53 as integers, everywhere in `v` (Lua 5.1
/// has only doubles; `7` should come back as `7`, not `7.0`).
#[must_use]
pub fn integral(v: Value) -> Value {
    match v {
        Value::Number(n) if n.is_f64() => {
            let x = n.as_f64().unwrap_or(0.0);
            if x.fract() == 0.0 && x.abs() <= 9_007_199_254_740_992.0 {
                Value::from(x as i64)
            } else {
                Value::Number(n)
            }
        }
        Value::Array(a) => Value::Array(a.into_iter().map(integral).collect()),
        Value::Object(o) => Value::Object(o.into_iter().map(|(k, v)| (k, integral(v))).collect()),
        other => other,
    }
}

/// A host error as code sees it: `{message, code, sqlState, id}`, where
/// `id` finds the original again ([`SyncHost::kept`]).
#[must_use]
pub fn error_json(e: &SprigError, id: usize) -> Value {
    let (code, state) = match &e.kind {
        sprig_common::ErrorKind::Sprig(c) => (c.number(), "HY000".to_owned()),
        sprig_common::ErrorKind::Backend { errno, sqlstate }
        | sprig_common::ErrorKind::Standard { errno, sqlstate } => {
            (*errno, String::from_utf8_lossy(sqlstate).into_owned())
        }
    };
    serde_json::json!({
        "message": e.message,
        "code": code,
        "sqlState": state,
        "id": id,
    })
}

/// The error for an exception the code didn't catch: the kept host error
/// it carries, or 39865 with its text.
#[must_use]
pub fn user_error(text: &str) -> SprigError {
    let t: String = text.chars().take(1000).collect();
    SprigError::new(
        SprigCode::LogicUserError,
        if t.is_empty() {
            "the code threw an error".to_owned()
        } else {
            t
        },
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn events_from_return_values() {
        assert_eq!(
            event_of(json!({"total": 3}), false),
            ("output".into(), json!({"total": 3}))
        );
        assert_eq!(
            event_of(json!({"total": 3}), true),
            ("output".into(), json!({"value": {"total": 3}}))
        );
        assert_eq!(event_of(Value::Null, false), ("output".into(), json!({})));
        assert_eq!(
            event_of(json!(7), false),
            ("output".into(), json!({"value": 7}))
        );
        assert_eq!(
            event_of(
                json!({"__sprig_event": "error", "fields": {"message": "x"}}),
                true
            ),
            ("error".into(), json!({"message": "x"}))
        );
    }

    #[test]
    fn arguments_and_numbers() {
        let sig = ScriptSig {
            params: vec![
                ("a".into(), ParamTy::Int),
                ("b".into(), ParamTy::Bool),
                ("c".into(), ParamTy::Json),
            ],
            scalar: false,
        };
        assert_eq!(
            sig.coerce("f", vec![json!("7"), json!(1), json!("[1,2]")])
                .unwrap(),
            vec![json!(7), json!(true), json!([1, 2])]
        );
        assert_eq!(
            sig.coerce("f", vec![]).unwrap_err().code(),
            Some(SprigCode::LogicSignatureMismatch)
        );
        assert_eq!(
            integral(json!({"a": 7.0, "b": [2.5, 3.0]})),
            json!({"a": 7, "b": [2.5, 3]})
        );
        assert!(bindable("product_id", &["end"]));
        assert!(!bindable("end", &["end"]));
        assert!(!bindable("1x", &[]));
    }
}
