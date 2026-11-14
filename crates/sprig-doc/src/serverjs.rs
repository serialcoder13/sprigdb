//! Server-side JavaScript in the document engine: the `$where` query
//! operator, the `$function` expression and the `$accumulator` group
//! accumulator, as MongoDB 8.0 runs them (deprecated there, re-enabled in
//! 9.0; the answers are the same).
//!
//! The engine doesn't run JavaScript itself: the gateway installs a
//! [`Scope`] around each command that may run code (when the tenant's
//! `mongo.server_js` is on), holding a [`Runner`] (SprigDB's QuickJS,
//! `sprig_logic_js::mongo`) and the operation's budgets. Without a scope,
//! server-side JavaScript is off, with MongoDB's errors for
//! `security.javascriptEnabled: false` (6108304 for `$where`, 31264 for
//! the others).
//!
//! **Errors.** A `$where` runs inside the matcher, which answers a bool;
//! a failure there is kept in the scope (the first one), the document
//! doesn't match, every later call fails at once, and the gateway answers
//! the command with the kept error ([`Scope::take_error`]). `$function`
//! and `$accumulator` fail through the evaluator as any expression, and
//! are kept too, for the places that swallow an expression's error
//! (`$expr` in a filter).
//!
//! **Budgets.** The code of one command shares `logic.function_budget_ms`
//! of JavaScript time (time spent elsewhere doesn't count) and runs with
//! a heap of `logic.memory_limit_mb`. Past the time: 50
//! `MaxTimeMSExpired` for `$where`, 11601 `Interrupted` for the others,
//! as MongoDB answers when `maxTimeMS` ends its code.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bson::{Bson, Document};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::agg::{self, Vars};

/// A call of code, as the runner sees it.
#[derive(Debug)]
pub struct JsCall<'a> {
    /// The operation: calls of one operation share globals.
    pub op: u64,
    /// The code.
    pub src: &'a str,
    /// `this`.
    pub this: Option<&'a Document>,
    /// The arguments.
    pub args: &'a [Bson],
    /// `$where`: the result as JavaScript truthiness.
    pub as_bool: bool,
    /// When the code is stopped.
    pub deadline: Instant,
    /// The heap limit.
    pub memory_bytes: usize,
}

/// Why a call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsFailure {
    /// The budget ran out.
    Interrupted,
    /// The code failed: MongoDB's code and message.
    Error(i32, String),
}

/// What runs the code (the gateway's QuickJS).
pub trait Runner: Send + Sync {
    /// Runs one call.
    ///
    /// # Errors
    /// [`JsFailure`].
    fn call(&self, c: &JsCall<'_>) -> Result<Bson, JsFailure>;
}

/// Which feature runs code (for the error of a spent budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `$where`.
    Where,
    /// `$function`, `$accumulator`.
    Expr,
}

/// The server-side JavaScript of one command.
pub struct Scope {
    runner: Arc<dyn Runner>,
    op: u64,
    wall: Duration,
    memory_bytes: usize,
    used: Mutex<Duration>,
    error: Mutex<Option<MongoError>>,
    /// The command's `maxTimeMS`: when its time ends, and the value.
    max_time: Option<(Instant, u64)>,
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scope")
            .field("op", &self.op)
            .field("wall", &self.wall)
            .finish_non_exhaustive()
    }
}

static OPS: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static ACTIVE: Arc<Scope>;
}

/// JavaScript time after which calls leave the runtime's worker thread
/// (`block_in_place`), as [`crate::heavy`] does for large steps.
const OFF_WORKER_AFTER: Duration = Duration::from_millis(5);

impl Scope {
    /// A scope with `wall` of JavaScript time and a heap of
    /// `memory_bytes`.
    #[must_use]
    pub fn new(runner: Arc<dyn Runner>, wall: Duration, memory_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            runner,
            op: OPS.fetch_add(1, Ordering::Relaxed),
            wall,
            memory_bytes,
            used: Mutex::new(Duration::ZERO),
            error: Mutex::new(None),
            max_time: None,
        })
    }

    /// A scope as [`Self::new`] for a command with `maxTimeMS` (from now):
    /// its code also stops when the command's time ends, with MongoDB's 50
    /// `MaxTimeMSExpired` (MongoDB checks its time limit
    /// between documents, SprigDB also inside a call).
    #[must_use]
    pub fn with_max_time(
        runner: Arc<dyn Runner>,
        wall: Duration,
        memory_bytes: usize,
        max_time_ms: Option<u64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runner,
            op: OPS.fetch_add(1, Ordering::Relaxed),
            wall,
            memory_bytes,
            used: Mutex::new(Duration::ZERO),
            error: Mutex::new(None),
            max_time: max_time_ms
                .filter(|ms| *ms > 0)
                .map(|ms| (Instant::now() + Duration::from_millis(ms), ms)),
        })
    }

    fn max_time_expired(&self) -> MongoError {
        MongoError::new(
            code::MAX_TIME_MS_EXPIRED,
            format!(
                "operation exceeded time limit: server-side JavaScript ran past the command's maxTimeMS ({} ms)",
                self.max_time.map_or(0, |(_, ms)| ms)
            ),
        )
    }

    /// The first error the command's code met, if any (taken).
    pub fn take_error(&self) -> Option<MongoError> {
        self.error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn kept(&self) -> Option<MongoError> {
        self.error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn keep(&self, e: &MongoError) {
        let mut g = self.error.lock().unwrap_or_else(PoisonError::into_inner);
        if g.is_none() {
            *g = Some(e.clone());
        }
    }

    /// Runs `src`, keeping its error.
    fn call(
        &self,
        kind: Kind,
        src: &str,
        this: Option<&Document>,
        args: &[Bson],
    ) -> Result<Bson, MongoError> {
        if let Some(e) = self.kept() {
            return Err(e);
        }
        let used = *self.used.lock().unwrap_or_else(PoisonError::into_inner);
        let start = Instant::now();
        let mut deadline = start + self.wall.saturating_sub(used);
        // The command's maxTimeMS, when it ends first.
        let by_max_time = self.max_time.is_some_and(|(t, _)| t < deadline);
        if let Some((t, _)) = self.max_time.filter(|_| by_max_time) {
            if t <= start {
                let e = self.max_time_expired();
                self.keep(&e);
                return Err(e);
            }
            deadline = t;
        }
        let c = JsCall {
            op: self.op,
            src,
            this,
            args,
            as_bool: kind == Kind::Where,
            deadline,
            memory_bytes: self.memory_bytes,
        };
        let r = if used >= OFF_WORKER_AFTER {
            crate::heavy::run(crate::heavy::HEAVY_DOCS, || self.runner.call(&c))
        } else {
            self.runner.call(&c)
        };
        *self.used.lock().unwrap_or_else(PoisonError::into_inner) += start.elapsed();
        r.map_err(|f| {
            let e = match f {
                JsFailure::Interrupted if by_max_time => self.max_time_expired(),
                JsFailure::Interrupted if kind == Kind::Where => MongoError::new(
                    code::MAX_TIME_MS_EXPIRED,
                    format!(
                        "operation exceeded time limit: server-side JavaScript ran past logic.function_budget_ms ({} ms)",
                        self.wall.as_millis()
                    ),
                ),
                JsFailure::Interrupted => MongoError::new(
                    11601,
                    format!(
                        "JavaScript execution interrupted: server-side JavaScript ran past logic.function_budget_ms ({} ms)",
                        self.wall.as_millis()
                    ),
                ),
                JsFailure::Error(c, m) => MongoError::new(c, m),
            };
            self.keep(&e);
            e
        })
    }
}

/// Runs `f` with `s` as the command's server-side JavaScript (none: off).
pub async fn scope<F: Future>(s: Option<Arc<Scope>>, f: F) -> F::Output {
    match s {
        Some(s) => ACTIVE.scope(s, f).await,
        None => f.await,
    }
}

/// Runs the synchronous `f` with `s` as the scope.
pub fn sync_scope<R>(s: Option<Arc<Scope>>, f: impl FnOnce() -> R) -> R {
    match s {
        Some(s) => ACTIVE.sync_scope(s, f),
        None => f(),
    }
}

/// The command's scope, when server-side JavaScript is on.
#[must_use]
pub fn active() -> Option<Arc<Scope>> {
    ACTIVE.try_with(Arc::clone).ok()
}

fn disabled() -> MongoError {
    MongoError::new(
        31264,
        "Cannot run server-side javascript without the javascript engine enabled (mongo.server_js is off)",
    )
}

/// Whether `d` names server-side JavaScript anywhere (`$where`,
/// `$function`, `$accumulator`): the gateway installs a scope only then
/// (and for `getMore`).
#[must_use]
pub fn mentions_js(d: &Document) -> bool {
    fn walk(v: &Bson) -> bool {
        match v {
            Bson::Document(d) => mentions_js(d),
            Bson::Array(a) => a.iter().any(walk),
            _ => false,
        }
    }
    d.iter()
        .any(|(k, v)| matches!(k.as_str(), "$where" | "$function" | "$accumulator") || walk(v))
}

// --- $where -----------------------------------------------------------

/// A parsed `$where`.
#[derive(Debug, Clone)]
pub struct Where {
    src: Arc<str>,
}

impl Where {
    /// Parses `{$where: v}`.
    ///
    /// # Errors
    /// 2 for a value that isn't code; 4649201 for code with scope;
    /// 6108304 when server-side JavaScript is off.
    pub fn parse(v: &Bson) -> Result<Self, MongoError> {
        let src = match v {
            Bson::String(s) | Bson::JavaScriptCode(s) => s.clone(),
            Bson::JavaScriptCodeWithScope(_) => {
                return Err(MongoError::new(
                    4_649_201,
                    "$where no longer supports deprecated BSON type CodeWScope",
                ));
            }
            _ => return Err(MongoError::new(code::BAD_VALUE, "$where got bad type")),
        };
        if active().is_none() {
            return Err(MongoError::new(
                6_108_304,
                "no globalScriptEngine in $where parsing (mongo.server_js is off)",
            ));
        }
        Ok(Self { src: src.into() })
    }

    /// Whether `doc` matches. A failure is kept in the scope (the
    /// command fails with it) and doesn't match.
    #[must_use]
    pub fn matches(&self, doc: &Document) -> bool {
        let Some(s) = active() else {
            return false;
        };
        matches!(
            s.call(Kind::Where, &self.src, Some(doc), &[]),
            Ok(Bson::Boolean(true))
        )
    }
}

/// `$where` inside an aggregation `$match` (or `$lookup`'s, `$facet`'s,
/// `$unionWith`'s): MongoDB refuses it there.
///
/// # Errors
/// 2 `BadValue`.
pub fn check_pipeline(stages: &[Bson]) -> Result<(), MongoError> {
    fn filter_has_where(d: &Document) -> bool {
        d.iter().any(|(k, v)| {
            k == "$where"
                || (matches!(k.as_str(), "$and" | "$or" | "$nor")
                    && matches!(v, Bson::Array(a) if a.iter().any(|x| matches!(x, Bson::Document(d) if filter_has_where(d)))))
        })
    }
    for s in stages {
        let Bson::Document(s) = s else { continue };
        let Some((name, arg)) = s.iter().next() else {
            continue;
        };
        let sub = |d: &Document, key: &str| -> Result<(), MongoError> {
            match d.get(key) {
                Some(Bson::Array(p)) => check_pipeline(p),
                _ => Ok(()),
            }
        };
        match (name.as_str(), arg) {
            ("$match", Bson::Document(f)) if filter_has_where(f) => {
                return Err(MongoError::new(
                    code::BAD_VALUE,
                    "$where is not allowed in this context",
                ));
            }
            ("$lookup" | "$unionWith", Bson::Document(d)) => sub(d, "pipeline")?,
            ("$facet", Bson::Document(d)) => {
                for p in d.values() {
                    if let Bson::Array(p) = p {
                        check_pipeline(p)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// --- $function ----------------------------------------------------------

fn code_of(v: Option<&Bson>) -> Option<&str> {
    match v {
        Some(Bson::String(s) | Bson::JavaScriptCode(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// `{$function: {body, args, lang: "js"}}`.
///
/// # Errors
/// MongoDB's 31260–31266, 31418 and 31419 for the specification; 31264
/// when server-side JavaScript is off; the code's.
pub fn function(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let Bson::Document(spec) = arg else {
        return Err(MongoError::new(
            31260,
            format!(
                "$function requires an object as an argument, found: {}",
                crate::update::type_name(arg)
            ),
        ));
    };
    let body = spec
        .get("body")
        .ok_or_else(|| MongoError::new(31261, "The body function must be specified."))?;
    let src = code_of(Some(body)).ok_or_else(|| {
        MongoError::new(
            31262,
            "The body function must evaluate to type string or code",
        )
    })?;
    let args_expr = spec
        .get("args")
        .ok_or_else(|| MongoError::new(31263, "The args field must be specified."))?;
    match spec.get("lang") {
        None => {
            return Err(MongoError::new(31418, "The lang field must be specified."));
        }
        Some(Bson::String(l)) if l == "js" => {}
        Some(_) => {
            return Err(MongoError::new(
                31419,
                "Currently the only supported language specifier is 'js'.",
            ));
        }
    }
    let Bson::Array(args) = agg::eval(args_expr, vars)? else {
        return Err(MongoError::new(
            31266,
            "The args field must be of type array",
        ));
    };
    let s = active().ok_or_else(disabled)?;
    s.call(Kind::Expr, src, None, &args)
}

// --- $accumulator -------------------------------------------------------

/// One `$accumulator` field of a `$group`.
#[derive(Debug, Clone)]
struct AccSpec {
    field: String,
    init: String,
    init_args: Option<Bson>,
    accumulate: String,
    accumulate_args: Bson,
    finalize: Option<String>,
}

fn parse_acc(field: &str, v: &Bson) -> Result<AccSpec, MongoError> {
    let Bson::Document(d) = v else {
        return Err(MongoError::new(
            4_544_703,
            format!(
                "$accumulator expects an object as an argument; found: {}",
                crate::update::type_name(v)
            ),
        ));
    };
    let code = |k: &str, required: Option<i32>| -> Result<Option<String>, MongoError> {
        match d.get(k) {
            None => match required {
                Some(c) => Err(MongoError::new(
                    c,
                    format!("$accumulator missing required argument '{k}'"),
                )),
                None => Ok(None),
            },
            Some(x) => code_of(Some(x)).map(|s| Some(s.to_owned())).ok_or_else(|| {
                MongoError::new(
                    4_544_702,
                    format!("$accumulator '{k}' must be a String or Code"),
                )
            }),
        }
    };
    for (k, x) in d {
        match k.as_str() {
            "init" | "initArgs" | "accumulate" | "accumulateArgs" | "merge" | "finalize" => {}
            "lang" => match x {
                Bson::String(l) if l == "js" => {}
                Bson::String(_) => {
                    return Err(MongoError::new(
                        4_544_705,
                        "$accumulator only supports lang: 'js'",
                    ));
                }
                other => {
                    return Err(MongoError::new(
                        4_544_704,
                        format!(
                            "$accumulator lang must be a string; found: {}",
                            crate::update::type_name(other)
                        ),
                    ));
                }
            },
            other => {
                return Err(MongoError::new(
                    4_544_706,
                    format!("$accumulator got an unexpected field: {other}"),
                ));
            }
        }
    }
    let init = code("init", Some(4_544_707))?.unwrap_or_default();
    let accumulate = code("accumulate", Some(4_544_708))?.unwrap_or_default();
    let _merge = code("merge", Some(4_544_709))?;
    let accumulate_args = d.get("accumulateArgs").cloned().ok_or_else(|| {
        MongoError::new(
            4_544_710,
            "$accumulator missing required argument 'accumulateArgs'",
        )
    })?;
    let finalize = code("finalize", None)?;
    Ok(AccSpec {
        field: field.to_owned(),
        init,
        init_args: d.get("initArgs").cloned(),
        accumulate,
        accumulate_args,
        finalize,
    })
}

fn display(v: &Bson) -> String {
    if matches!(v, Bson::Undefined) {
        "MISSING".to_owned()
    } else {
        v.to_string()
    }
}

/// The `$accumulator` fields of one `$group` (or `$bucket`/`$bucketAuto`
/// output): their states, per group key.
#[derive(Debug, Default)]
pub struct GroupJs {
    specs: Vec<AccSpec>,
    order: Vec<String>,
    states: HashMap<Vec<u8>, Vec<Bson>>,
}

impl GroupJs {
    /// The `$accumulator` fields of `spec` (a `$group` specification, or
    /// a `$bucket`'s `output`).
    ///
    /// # Errors
    /// MongoDB's 4544702–4544710 for a malformed `$accumulator`.
    pub fn parse(spec: &Document) -> Result<Self, MongoError> {
        let mut specs = Vec::new();
        for (k, v) in spec {
            if let Bson::Document(d) = v
                && let Some(a) = d.get("$accumulator")
            {
                specs.push(parse_acc(k, a)?);
            }
        }
        Ok(Self {
            specs,
            order: spec.keys().cloned().collect(),
            states: HashMap::new(),
        })
    }

    /// Whether `spec` has none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Whether `field` is one of them.
    #[must_use]
    pub fn has(&self, field: &str) -> bool {
        self.specs.iter().any(|s| s.field == field)
    }

    /// Adds one document of the group with key bytes `k` and `_id` `id`:
    /// `init` (with `initArgs` evaluated against the `_id` document) for
    /// a new group, then `accumulate`.
    ///
    /// # Errors
    /// 4544711 and 4544712 for arguments that aren't arrays; 31264 when
    /// server-side JavaScript is off; the code's.
    pub fn add(&mut self, k: &[u8], id: &Bson, vars: &Vars) -> Result<(), MongoError> {
        if self.specs.is_empty() {
            return Ok(());
        }
        let s = active().ok_or_else(disabled)?;
        if !self.states.contains_key(k) {
            let id_doc = match id {
                Bson::Document(d) => d.clone(),
                _ => Document::new(),
            };
            let id_vars = Vars::new(&id_doc, now_of(vars));
            let mut st = Vec::with_capacity(self.specs.len());
            for a in &self.specs {
                let args = match &a.init_args {
                    None => Vec::new(),
                    Some(e) => match agg::eval(e, &id_vars)? {
                        Bson::Array(x) => x,
                        other => {
                            return Err(MongoError::new(
                                4_544_711,
                                format!(
                                    "$accumulator initArgs must evaluate to an array: {}",
                                    display(&other)
                                ),
                            ));
                        }
                    },
                };
                st.push(s.call(Kind::Expr, &a.init, None, &args)?);
            }
            self.states.insert(k.to_vec(), st);
        }
        let Some(st) = self.states.get_mut(k) else {
            return Ok(());
        };
        for (i, a) in self.specs.iter().enumerate() {
            let mut args = vec![std::mem::replace(&mut st[i], Bson::Null)];
            match agg::eval(&a.accumulate_args, vars)? {
                Bson::Array(x) => args.extend(x),
                other => {
                    return Err(MongoError::new(
                        4_544_712,
                        format!(
                            "$accumulator accumulateArgs must evaluate to an array: {}",
                            display(&other)
                        ),
                    ));
                }
            }
            st[i] = s.call(Kind::Expr, &a.accumulate, None, &args)?;
        }
        Ok(())
    }

    /// Puts the group `k`'s results in `out` (`finalize`, or the state),
    /// in the specification's field order.
    ///
    /// # Errors
    /// The code's.
    pub fn finish(&self, k: &[u8], out: &mut Document) -> Result<(), MongoError> {
        if self.specs.is_empty() {
            return Ok(());
        }
        let states = self.states.get(k);
        for (i, a) in self.specs.iter().enumerate() {
            let st = states.and_then(|s| s.get(i)).cloned().unwrap_or(Bson::Null);
            let v = match &a.finalize {
                Some(f) => {
                    let s = active().ok_or_else(disabled)?;
                    s.call(Kind::Expr, f, None, &[st])?
                }
                None => st,
            };
            out.insert(a.field.clone(), v);
        }
        let mut ordered = Document::new();
        if let Some(id) = out.get("_id") {
            ordered.insert("_id", id.clone());
        }
        for f in &self.order {
            if let Some(v) = out.get(f) {
                ordered.insert(f.clone(), v.clone());
            }
        }
        for (f, v) in out.iter() {
            if !ordered.contains_key(f) {
                ordered.insert(f.clone(), v.clone());
            }
        }
        *out = ordered;
        Ok(())
    }
}

/// `$$NOW` of `vars`.
fn now_of(vars: &Vars) -> bson::DateTime {
    match agg::eval(&Bson::String("$$NOW".into()), vars) {
        Ok(Bson::DateTime(d)) => d,
        _ => bson::DateTime::now(),
    }
}

#[cfg(test)]
mod tests;
