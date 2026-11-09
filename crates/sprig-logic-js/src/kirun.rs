//! The native KIRun runtime of the fallback tier (chapter 41, "The
//! fallback runtime"; build step 14): KIRun definitions run on `kirun`,
//! Modlix's open-source KIRun runtime for Rust, in-process.
//!
//! ```text
//! prepare(source)        definition JSON → kirun FunctionDefinition, once per version
//! run(prepared, args, host, limits):
//!   arguments by position, in the definition's parameter order (39874 on a count mismatch)
//!   a repository for this run: kirun's own functions plus Sprig.Db.QueryOne | Query |
//!     Exec and Sprig.Http.Fetch, every one wrapped in the budget shim
//!   KIRuntime::execute; the first event raised is the result (KIRun's rule, and the
//!     compiled tier's): {event, fields}
//!   a budget tripped inside a loop, an error a Sprig function raised → that error;
//!   any other KIRun failure → 39866 with KIRun's message; a panic → 39866
//! ```
//!
//! The **shim** charges every function call against [`RunState`]: one
//! step (loop bodies included), the wall time and a cancel flag, the size
//! of its arguments and events, and every [`crate::budget::MEMORY_SCAN_EVERY`]
//! steps the size of the whole context and step outputs. It counts each
//! loop's iterations against `logic.max_loop_iterations` (the compiled
//! tier's guard). Every [`crate::budget::YIELD_EVERY`] steps it yields, so
//! a run without `db` calls shares its executor thread.
//!
//! **`Sprig.Db.*`** read their `args` references themselves, in order
//! (`order`, then key, as the compiler does), because KIRun's variable
//! arguments drop `null` values and flatten arrays, which would shift the
//! placeholders. The statement is checked and bound by
//! [`crate::bridge::bind`] and run by the host as the caller.
//!
//! Errors raised by the shim and the Sprig functions travel through KIRun
//! as `[sprig#<n> <code>] <message>` and come back out as the original
//! [`SprigError`]. KIRun's `catchErrors` steps may catch `db` and outbound
//! errors (the reason is the error's number); budget errors are never
//! caught.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, Once};

use async_trait::async_trait;
use futures::FutureExt;
use kirun::function::{AbstractFunction, Function};
use kirun::json::schema::Schema;
use kirun::model::parameter_reference::ParameterReferenceType;
use kirun::model::{
    Event, EventResult, FunctionDefinition, FunctionOutput, FunctionSignature, Parameter,
};
use kirun::repository::{KIRunFunctionRepository, KIRunSchemaRepository, Repository};
use kirun::runtime::catch_errors::{self, Caught};
use kirun::runtime::expression::ExpressionEvaluator;
use kirun::runtime::{FunctionExecutionParameters, KIRuntime};
use kirun::{KIRunError, Map};
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};

use crate::bridge::{self, DbBridge, Reply, Shape};
use crate::budget::{Budget, Limits, RunState, copy, size_of};
use crate::messaging::MsgOp;
use crate::outbound::{self, Outbound};

/// The functions engine code may call beyond KIRun's own.
pub const SPRIG_FUNCTIONS: &[&str] = &[
    "Sprig.Db.QueryOne",
    "Sprig.Db.Query",
    "Sprig.Db.Exec",
    "Sprig.Http.Fetch",
    // R9: publishing and consuming (`crate::messaging`).
    "Sprig.Db.Publish",
    "Sprig.Db.CommitOffset",
    "Sprig.Mq.Publish",
    "Sprig.Mq.Get",
    "Sprig.Mq.Ack",
    "Sprig.Mq.Nack",
];

/// KIRun functions with no place in server code: `System.Wait` sleeps
/// on the gateway's executor, `System.Print` writes to its stdout.
pub const REFUSED_FUNCTIONS: &[&str] = &["System.Wait", "System.Print"];

static BASE: LazyLock<Arc<KIRunFunctionRepository>> =
    LazyLock::new(|| Arc::new(KIRunFunctionRepository::new()));
static SCHEMAS: LazyLock<Arc<KIRunSchemaRepository>> =
    LazyLock::new(|| Arc::new(KIRunSchemaRepository::new()));
static POLICY: Once = Once::new();

/// Budget errors end the run even in a `catchErrors` step; `db` and
/// outbound errors may be caught, with their number as the reason.
fn install_catch_policy() {
    POLICY.call_once(|| {
        catch_errors::set_policy(Some(Arc::new(|e: &KIRunError| {
            match token(&e.to_string()) {
                Some((_, code)) if code == SprigCode::LogicBudgetExceeded.number() => {
                    Caught::Rethrow
                }
                Some((_, code)) => Caught::Event {
                    reason: Some(code.to_string()),
                },
                None => Caught::Event { reason: None },
            }
        })));
    });
}

/// `[sprig#<n> <code>]` in a message: the error's index and number.
fn token(msg: &str) -> Option<(usize, u16)> {
    let at = msg.find("[sprig#")?;
    let rest = &msg[at + 7..];
    let end = rest.find(']')?;
    let (n, code) = rest[..end].split_once(' ')?;
    Some((n.parse().ok()?, code.parse().ok()?))
}

fn number(e: &SprigError) -> u16 {
    match e.kind {
        sprig_common::ErrorKind::Sprig(c) => c.number(),
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => errno,
    }
}

/// A definition ready to run: parsed once per version.
#[derive(Debug, Clone)]
pub struct Prepared {
    fd: FunctionDefinition,
    params: Vec<String>,
    /// Each parameter's schema types (`INTEGER`, `STRING`, ...).
    types: Vec<Vec<String>>,
}

impl Prepared {
    /// Parses a definition's JSON.
    ///
    /// # Errors
    /// 39861 when it isn't a KIRun definition.
    pub fn new(source: &str) -> Result<Self> {
        let v: Value = serde_json::from_str(source).map_err(|e| {
            SprigError::new(
                SprigCode::LogicSyntax,
                format!("the KIRun definition isn't valid JSON: {e}"),
            )
        })?;
        let fd = FunctionDefinition::from_value(&v).map_err(|e| {
            SprigError::new(
                SprigCode::LogicSyntax,
                format!("the KIRun definition isn't valid: {e}"),
            )
        })?;
        let params: Vec<String> = fd.get_parameters().keys().cloned().collect();
        let types = params
            .iter()
            .map(|n| match &v["parameters"][n]["schema"]["type"] {
                Value::String(t) => vec![t.to_ascii_uppercase()],
                Value::Array(a) => a
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_ascii_uppercase)
                    .collect(),
                _ => Vec::new(),
            })
            .collect();
        Ok(Self { fd, params, types })
    }

    /// Converts a call's arguments, as SQL hands them over (numbers,
    /// text, `0`/`1` for booleans, JSON as text), to the definition's
    /// parameter types, as the compiled tier's typed parameters do.
    /// Values that don't convert are left for KIRun's own validation.
    ///
    /// # Errors
    /// 39874 for the wrong number of arguments.
    pub fn coerce(&self, args: Vec<Value>) -> Result<Vec<Value>> {
        if args.len() != self.params.len() {
            return Err(self.arity(args.len()));
        }
        Ok(args
            .into_iter()
            .zip(&self.types)
            .map(|(v, t)| coerce(v, t))
            .collect())
    }

    fn arity(&self, n: usize) -> SprigError {
        SprigError::new(
            SprigCode::LogicSignatureMismatch,
            format!(
                "{} takes {} arguments ({}), not {n}",
                self.fd.get_name(),
                self.params.len(),
                self.params.join(", "),
            ),
        )
    }

    /// The parameters, in call order.
    #[must_use]
    pub fn params(&self) -> &[String] {
        &self.params
    }

    /// `namespace.name` of every function the definition calls that this
    /// runtime can't run: unknown to KIRun and SprigDB, or refused
    /// ([`REFUSED_FUNCTIONS`]). Placement refuses such a version (39862).
    pub async fn missing_functions(&self) -> Vec<String> {
        let mut out = Vec::new();
        for s in self.fd.get_steps().values() {
            let full = format!("{}.{}", s.get_namespace(), s.get_name());
            let known = if s.get_namespace().starts_with("Sprig.") {
                SPRIG_FUNCTIONS.contains(&full.as_str())
            } else {
                !REFUSED_FUNCTIONS.contains(&full.as_str())
                    && BASE.find(s.get_namespace(), s.get_name()).await.is_some()
            };
            if !known && !out.contains(&full) {
                out.push(full);
            }
        }
        out
    }
}

/// What the code may reach.
#[derive(Clone)]
pub struct Host {
    /// `Sprig.Db.*`.
    pub db: Arc<dyn DbBridge>,
    /// `Sprig.Http.Fetch`.
    pub outbound: Arc<dyn Outbound>,
    /// The code's own database: unqualified table names in `Sprig.Db.*`
    /// statements are qualified with it ([`bridge::qualify`]), as a
    /// compiled routine's names resolve in its own schema. `None` leaves
    /// them to the session's current database.
    pub default_db: Option<String>,
    /// What JavaScript and Lua code sees as `ctx`: `user`, `tenant`,
    /// `database`, `function`, `version` (an object; `null` for none).
    pub info: Value,
}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host").finish_non_exhaustive()
    }
}

/// A finished run.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// The first event raised.
    pub event: String,
    /// Its fields, as an object.
    pub result: Value,
    /// Steps run.
    pub steps: u64,
    /// `db` and outbound calls made.
    pub data_ops: u64,
    /// Rows read.
    pub rows_read: u64,
    /// The largest value set held, in bytes (estimated).
    pub peak_bytes: u64,
}

/// One run's shared state: budgets, the host and the errors raised.
struct Run {
    st: RunState,
    host: Host,
    errors: Mutex<Vec<SprigError>>,
}

impl Run {
    /// Keeps `e` and returns the KIRun error that carries it.
    fn raise(&self, e: SprigError) -> KIRunError {
        let code = number(&e);
        let msg = e.message.clone();
        let mut v = self
            .errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        v.push(e);
        KIRunError::uncaught(format!("[sprig#{} {code}] {msg}", v.len() - 1))
    }

    fn take(&self, n: usize) -> Option<SprigError> {
        self.errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(n)
            .map(copy)
    }
}

/// Runs `p` with positional `args`.
///
/// # Errors
/// 39874 for the wrong number of arguments; 39864 for a budget; the
/// error of a `db` statement (1142, 1062, ...) or outbound call (39867);
/// 39866 for any other failure of the definition, with KIRun's message.
pub async fn run(p: &Prepared, args: Vec<Value>, host: Host, limits: Limits) -> Result<Outcome> {
    install_catch_policy();
    if args.len() != p.params.len() {
        return Err(p.arity(args.len()));
    }
    let run = Arc::new(Run {
        st: RunState::new(limits),
        host,
        errors: Mutex::new(Vec::new()),
    });
    let mut arg_map: Map<Value> = Map::new();
    for (k, v) in p.params.iter().zip(args) {
        arg_map.insert(k.clone(), v);
    }
    let size: u64 = arg_map
        .values()
        .map(|v| size_of(v, run.st.limits().memory_bytes))
        .sum();
    run.st.hold(size)?;
    let repo: Arc<dyn Repository<dyn Function>> = Arc::new(RunRepo {
        run: Arc::clone(&run),
        sprig: sprig_functions(&run),
    });
    let schemas: Arc<dyn Repository<Schema>> = SCHEMAS.clone();
    // A fresh runtime per run: KIRuntime caches the functions it finds,
    // and these carry this run's budgets.
    let runtime = KIRuntime::new(p.fd.clone(), false).map_err(|e| {
        SprigError::new(SprigCode::LogicSyntax, format!("the KIRun definition: {e}"))
    })?;
    let mut fep = FunctionExecutionParameters::new(repo, schemas).set_arguments(arg_map);
    let fut = async move { runtime.execute(&mut fep).await.map(|mut o| o.next()) };
    let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;
    let st = &run.st;
    let first = match result {
        Err(_) => {
            return Err(SprigError::new(
                SprigCode::LogicRuntimeError,
                "the KIRun runtime failed (a panic); the call was stopped",
            ));
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            if let Some(t) = st.tripped() {
                return Err(t);
            }
            if let Some((n, _)) = token(&msg)
                && let Some(err) = run.take(n)
            {
                return Err(err);
            }
            return Err(SprigError::new(
                SprigCode::LogicRuntimeError,
                truncate(&msg, 1000),
            ));
        }
        Ok(Ok(first)) => first,
    };
    if let Some(t) = st.tripped() {
        return Err(t);
    }
    let (event, result) = match first {
        Some(er) => (
            er.get_name().to_owned(),
            Value::Object(er.get_result().clone().into_iter().collect()),
        ),
        None => (Event::OUTPUT.to_owned(), json!({})),
    };
    Ok(Outcome {
        event,
        result,
        steps: st.steps(),
        data_ops: st.data_ops(),
        rows_read: st.rows_read(),
        peak_bytes: st.peak_bytes(),
    })
}

/// One argument to the first non-`NULL` type of its schema.
fn coerce(v: Value, types: &[String]) -> Value {
    let Some(t) = types.iter().find(|t| *t != "NULL") else {
        return v;
    };
    match (t.as_str(), v) {
        (_, Value::Null) => Value::Null,
        ("INTEGER" | "LONG", Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.trim().parse::<f64>().map(Value::from))
            .unwrap_or(Value::String(s)),
        ("INTEGER" | "LONG", Value::Bool(b)) => Value::from(i64::from(b)),
        ("FLOAT" | "DOUBLE", Value::String(s)) => s
            .trim()
            .parse::<f64>()
            .map(Value::from)
            .unwrap_or(Value::String(s)),
        ("BOOLEAN", Value::Number(n)) => Value::Bool(n.as_f64().is_some_and(|x| x != 0.0)),
        ("BOOLEAN", Value::String(s)) => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Value::Bool(true),
            "0" | "false" => Value::Bool(false),
            _ => Value::String(s),
        },
        ("STRING", Value::Number(n)) => Value::String(n.to_string()),
        ("STRING", Value::Bool(b)) => Value::String(if b { "1" } else { "0" }.to_owned()),
        ("ARRAY" | "OBJECT", Value::String(s)) => {
            serde_json::from_str(&s).unwrap_or(Value::String(s))
        }
        (_, v) => v,
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_owned()
    } else {
        let mut t: String = s.chars().take(n).collect();
        t.push('…');
        t
    }
}

/// The run's repository: KIRun's functions and the Sprig functions, each
/// behind the shim.
struct RunRepo {
    run: Arc<Run>,
    sprig: HashMap<String, Arc<dyn Function>>,
}

#[async_trait]
impl Repository<dyn Function> for RunRepo {
    async fn find(&self, namespace: &str, name: &str) -> Option<Arc<dyn Function>> {
        let full = format!("{namespace}.{name}");
        let f = if namespace.starts_with("Sprig.") {
            self.sprig.get(&full).cloned()?
        } else if REFUSED_FUNCTIONS.contains(&full.as_str()) {
            return None;
        } else {
            BASE.find(namespace, name).await?
        };
        Some(Arc::new(Shim {
            f,
            run: Arc::clone(&self.run),
        }))
    }

    async fn filter(&self, name: &str) -> Vec<String> {
        let mut v = BASE.filter(name).await;
        let lower = name.to_lowercase();
        v.extend(
            SPRIG_FUNCTIONS
                .iter()
                .filter(|f| f.to_lowercase().contains(&lower))
                .map(|f| (*f).to_owned()),
        );
        v
    }
}

/// The budget shim around one function (see the module docs).
struct Shim {
    f: Arc<dyn Function>,
    run: Arc<Run>,
}

#[async_trait]
impl Function for Shim {
    fn get_signature(&self) -> &FunctionSignature {
        self.f.get_signature()
    }

    fn get_probable_event_signature(&self, p: &Map<Vec<Schema>>) -> Map<Event> {
        self.f.get_probable_event_signature(p)
    }

    async fn execute(&self, cx: &mut FunctionExecutionParameters) -> kirun::Result<FunctionOutput> {
        let run = &self.run;
        let st = &run.st;
        let cap = st.limits().memory_bytes;
        let yield_now = st.step().map_err(|e| run.raise(e))?;
        let mut held: u64 = cx
            .get_arguments()
            .map_or(0, |a| a.values().map(|v| size_of(v, cap)).sum());
        if st.scan_due() {
            if let Some(c) = cx.get_context() {
                held += c
                    .read()
                    .values()
                    .map(|e| size_of(e.get_element(), cap))
                    .sum::<u64>();
            }
            if let Some(s) = cx.get_steps() {
                held += s
                    .read()
                    .values()
                    .flat_map(|ev| ev.values())
                    .flat_map(|m| m.values())
                    .map(|v| size_of(v, cap))
                    .sum::<u64>();
            }
        }
        st.hold(held).map_err(|e| run.raise(e))?;
        if yield_now {
            tokio::task::yield_now().await;
        }
        let out = self.f.execute(cx).await?;
        Ok(wrap(out, Arc::clone(run)))
    }

    fn as_any(&self) -> Option<&dyn Any> {
        self.f.as_any()
    }
}

/// Wraps a function's events: each is measured, and loop iterations are
/// counted and timed. A budget that runs out trips the run and ends the
/// loop.
fn wrap(mut inner: FunctionOutput, run: Arc<Run>) -> FunctionOutput {
    let mut iterations: u64 = 0;
    let mut ended = false;
    FunctionOutput::from_generator(move || {
        if ended {
            return Some(EventResult::output_of(Map::new()));
        }
        let ev = inner.next()?;
        let st = &run.st;
        let mut stop = None;
        if ev.get_name() == Event::ITERATION {
            iterations += 1;
            if iterations > st.limits().max_loop_iterations {
                stop = Some(st.error(Budget::Loop));
            } else if let Err(e) = st.check_time() {
                stop = Some(e);
            }
        }
        if stop.is_none() {
            let cap = st.limits().memory_bytes;
            let size: u64 = ev.get_result().values().map(|v| size_of(v, cap)).sum();
            if let Err(e) = st.hold(size) {
                stop = Some(e);
            }
        }
        if let Some(e) = stop {
            st.trip(e);
            ended = true;
            return Some(EventResult::output_of(Map::new()));
        }
        Some(ev)
    })
}

fn sprig_functions(run: &Arc<Run>) -> HashMap<String, Arc<dyn Function>> {
    let mut m: HashMap<String, Arc<dyn Function>> = HashMap::new();
    for shape in [Shape::One, Shape::Many, Shape::Exec] {
        m.insert(
            shape.function().to_owned(),
            Arc::new(SprigDb::new(shape, Arc::clone(run))),
        );
    }
    m.insert(
        "Sprig.Http.Fetch".to_owned(),
        Arc::new(Fetch::new(Arc::clone(run))),
    );
    for op in MsgOp::ALL {
        m.insert(
            op.kirun_name().to_owned(),
            Arc::new(SprigMsg::new(op, Arc::clone(run))),
        );
    }
    m
}

/// `Sprig.Db.Publish(stream, key, value, headers)` → `output {affected}`,
/// `Sprig.Db.CommitOffset(group, stream, partition, offset)`,
/// `Sprig.Mq.Publish(exchange, routingKey, body, properties)` → `output
/// {routed}`, `Sprig.Mq.Get(queue, max, leaseMs)` → `output {messages}`,
/// `Sprig.Mq.Ack(token)`, `Sprig.Mq.Nack(token, requeue)` (R9).
struct SprigMsg {
    sig: FunctionSignature,
    op: MsgOp,
    run: Arc<Run>,
}

impl SprigMsg {
    fn out_name(op: MsgOp) -> Option<&'static str> {
        match op {
            MsgOp::Publish => Some("affected"),
            MsgOp::MqPublish => Some("routed"),
            MsgOp::MqGet => Some("messages"),
            _ => None,
        }
    }

    fn new(op: MsgOp, run: Arc<Run>) -> Self {
        let full = op.kirun_name();
        let (ns, name) = full.rsplit_once('.').unwrap_or(("Sprig", full));
        let params: Map<Parameter> = op
            .params()
            .iter()
            .map(|p| Parameter::of_entry(p, Schema::of_any(p), false))
            .collect();
        let outputs: Map<Schema> = Self::out_name(op)
            .map(|n| Map::from([(n.to_owned(), Schema::of_any(n))]))
            .unwrap_or_default();
        let sig = FunctionSignature::new(name)
            .set_namespace(ns)
            .set_parameters(params)
            .set_events(Map::from([Event::output_event_map_entry(outputs)]));
        Self { sig, op, run }
    }
}

#[async_trait]
impl AbstractFunction for SprigMsg {
    fn get_signature(&self) -> &FunctionSignature {
        &self.sig
    }

    async fn internal_execute(
        &self,
        cx: &mut FunctionExecutionParameters,
    ) -> kirun::Result<FunctionOutput> {
        let run = &self.run;
        let a = cx.get_arguments().cloned().unwrap_or_default();
        let args: Vec<Value> = self
            .op
            .params()
            .iter()
            .map(|p| a.get(*p).cloned().unwrap_or(Value::Null))
            .collect();
        run.st.data_op().map_err(|e| run.raise(e))?;
        let v = crate::messaging::call(
            &*run.host.db,
            self.op,
            &args,
            run.host.default_db.as_deref(),
        )
        .await
        .map_err(|e| run.raise(e))?;
        run.st.check_time().map_err(|e| run.raise(e))?;
        let fields: Map<Value> = match Self::out_name(self.op) {
            Some("affected") => Map::from([(
                "affected".to_owned(),
                v.get("affected").cloned().unwrap_or(Value::Null),
            )]),
            Some(n) => Map::from([(n.to_owned(), v)]),
            None => Map::new(),
        };
        Ok(FunctionOutput::new(vec![EventResult::output_of(fields)]))
    }
}

/// `Sprig.Db.QueryOne | Query | Exec`.
struct SprigDb {
    sig: FunctionSignature,
    shape: Shape,
    run: Arc<Run>,
}

impl SprigDb {
    fn new(shape: Shape, run: Arc<Run>) -> Self {
        let name = shape.function().rsplit('.').next().unwrap_or_default();
        let outputs: Map<Schema> = match shape {
            Shape::One => Map::from([
                ("row".to_owned(), Schema::of_any("row")),
                ("found".to_owned(), Schema::of_boolean("found")),
            ]),
            Shape::Many => Map::from([("rows".to_owned(), Schema::of_any("rows"))]),
            Shape::Exec | Shape::Call | Shape::Sprig => {
                Map::from([("affected".to_owned(), Schema::of_long("affected"))])
            }
        };
        let sig = FunctionSignature::new(name)
            .set_namespace("Sprig.Db")
            .set_parameters(Map::from([
                Parameter::of_entry("sql", Schema::of_string("sql"), false),
                Parameter::of_entry("args", Schema::of_any("args"), true),
            ]))
            .set_events(Map::from([Event::output_event_map_entry(outputs)]));
        Self { sig, shape, run }
    }

    /// The `args` references, evaluated in the compiler's order, nulls and
    /// arrays kept as they are.
    fn binds(cx: &FunctionExecutionParameters) -> kirun::Result<Vec<Value>> {
        let Some(se) = cx.get_statement_execution() else {
            return Ok(Vec::new());
        };
        let Some(refs) = se.get_statement().get_parameter_map().get("args") else {
            return Ok(Vec::new());
        };
        let mut list: Vec<(&String, &kirun::model::ParameterReference)> = refs.iter().collect();
        list.sort_by(|a, b| {
            a.1.get_order()
                .unwrap_or(0)
                .cmp(&b.1.get_order().unwrap_or(0))
                .then(a.0.cmp(b.0))
        });
        let mut out = Vec::with_capacity(list.len());
        for (_, r) in list {
            out.push(match r.get_type() {
                ParameterReferenceType::Value => r.get_value().clone(),
                ParameterReferenceType::Expression => match r.get_expression() {
                    Some(e) if !e.trim().is_empty() => {
                        ExpressionEvaluator::new(e).evaluate(cx.get_values_map())?
                    }
                    _ => Value::Null,
                },
            });
        }
        Ok(out)
    }
}

fn step_name(cx: &FunctionExecutionParameters) -> String {
    cx.get_statement_execution().map_or_else(String::new, |s| {
        s.get_statement().get_statement_name().to_owned()
    })
}

#[async_trait]
impl AbstractFunction for SprigDb {
    fn get_signature(&self) -> &FunctionSignature {
        &self.sig
    }

    async fn internal_execute(
        &self,
        cx: &mut FunctionExecutionParameters,
    ) -> kirun::Result<FunctionOutput> {
        let run = &self.run;
        let at = format!("step {}", step_name(cx));
        let sql = cx
            .get_arguments()
            .and_then(|a| a.get("sql"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let binds = Self::binds(cx)?;
        let sql = match &run.host.default_db {
            Some(db) => bridge::qualify(&sql, db),
            None => sql,
        };
        run.st.data_op().map_err(|e| run.raise(e))?;
        let op = bridge::bind(self.shape, &sql, &binds, &at).map_err(|e| run.raise(e))?;
        let reply = run.host.db.exec(op).await.map_err(|e| run.raise(e))?;
        run.st.check_time().map_err(|e| run.raise(e))?;
        let fields: Map<Value> = match (self.shape, reply) {
            (Shape::Exec, Reply::Affected(n)) => Map::from([("affected".to_owned(), json!(n))]),
            (Shape::One | Shape::Many, Reply::Rows { columns, rows }) => {
                run.st
                    .read_rows(rows.len() as u64)
                    .map_err(|e| run.raise(e))?;
                if self.shape == Shape::One {
                    if rows.len() > 1 {
                        return Err(run.raise(SprigError::new(
                            SprigCode::LogicRuntimeError,
                            format!("Sprig.Db.QueryOne at {at} returned more than one row"),
                        )));
                    }
                    let row = match rows.first() {
                        Some(r) => bridge::row_object(&columns, r).map_err(|e| run.raise(e))?,
                        None => Value::Null,
                    };
                    Map::from([
                        ("row".to_owned(), row),
                        ("found".to_owned(), Value::Bool(rows.len() == 1)),
                    ])
                } else {
                    let mut list = Vec::with_capacity(rows.len());
                    for r in &rows {
                        list.push(bridge::row_object(&columns, r).map_err(|e| run.raise(e))?);
                    }
                    Map::from([("rows".to_owned(), Value::Array(list))])
                }
            }
            (shape, _) => {
                return Err(run.raise(SprigError::new(
                    SprigCode::LogicRuntimeError,
                    format!(
                        "{} at {at}: the statement returned the wrong kind of result",
                        shape.function()
                    ),
                )));
            }
        };
        Ok(FunctionOutput::new(vec![EventResult::output_of(fields)]))
    }
}

/// `Sprig.Http.Fetch(url, method?, headers?, body?)` → `output {status,
/// headers, body}`.
struct Fetch {
    sig: FunctionSignature,
    run: Arc<Run>,
}

impl Fetch {
    fn new(run: Arc<Run>) -> Self {
        let sig = FunctionSignature::new("Fetch")
            .set_namespace("Sprig.Http")
            .set_parameters(Map::from([
                Parameter::of_entry("url", Schema::of_string("url"), false),
                Parameter::of_entry("method", Schema::of_any("method"), false),
                Parameter::of_entry("headers", Schema::of_any("headers"), false),
                Parameter::of_entry("body", Schema::of_any("body"), false),
            ]))
            .set_events(Map::from([Event::output_event_map_entry(Map::from([
                ("status".to_owned(), Schema::of_integer("status")),
                ("headers".to_owned(), Schema::of_object("headers")),
                ("body".to_owned(), Schema::of_string("body")),
            ]))]));
        Self { sig, run }
    }
}

#[async_trait]
impl AbstractFunction for Fetch {
    fn get_signature(&self) -> &FunctionSignature {
        &self.sig
    }

    async fn internal_execute(
        &self,
        cx: &mut FunctionExecutionParameters,
    ) -> kirun::Result<FunctionOutput> {
        let run = &self.run;
        let a = cx.get_arguments().cloned().unwrap_or_default();
        let url = a
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let method = a
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("GET")
            .to_owned();
        let headers = match a.get("headers") {
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
        let body = match a.get("body") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(other) => Some(other.to_string()),
        };
        run.st.data_op().map_err(|e| run.raise(e))?;
        let resp = run
            .host
            .outbound
            .fetch(outbound::Request {
                method,
                url,
                headers,
                body,
            })
            .await
            .map_err(|e| run.raise(e))?;
        let hdrs: serde_json::Map<String, Value> = resp
            .headers
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        Ok(FunctionOutput::new(vec![EventResult::output_of(
            Map::from([
                ("status".to_owned(), json!(resp.status)),
                ("headers".to_owned(), Value::Object(hdrs)),
                ("body".to_owned(), Value::String(resp.body)),
            ]),
        )]))
    }
}

#[cfg(test)]
mod tests;
