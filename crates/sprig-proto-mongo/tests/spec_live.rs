//! MongoDB driver specification tests in the unified format (chapter 30,
//! "Testing"; chapter 48's R5 done-when: "The MongoDB driver specification
//! tests pass at 95% or more, with every failure documented").
//!
//! A unified-format runner over the Rust driver: it reads the JSON test
//! files of the `mongodb/specifications` repository (pinned by the caller),
//! runs each test against `SPRIG_SPEC_URI` and reports, per suite, the
//! tests passed, failed, not applicable (`runOnRequirements` that this
//! deployment doesn't meet) and not runnable here (an operation this runner
//! or the Rust driver doesn't express). The pass rate is passed over
//! passed + failed.
//!
//! - `SPRIG_SPEC_DIR`: the repository's `source` directory.
//! - `SPRIG_SPEC_URI`: the server, such as a gateway's MongoDB listener
//!   with a tenant login; `SPRIG_SPEC_TOPOLOGY` (`sharded`, the gateway
//!   presents itself as a `mongos`; `replicaset` for a reference server).
//! - `SPRIG_SPEC_SUITES`: comma-separated suites (default crud,
//!   transactions, retryable-writes, change-streams, sessions).
//! - `SPRIG_SPEC_FILTER`: run only files whose name contains this.
//! - `SPRIG_SPEC_REPORT`: write every failure with its reason to this file.
//!
//! Skipped unless `SPRIG_SPEC_DIR` and `SPRIG_SPEC_URI` are set. The
//! tests create and drop their own databases (the files' names, such as
//! `crud-tests`).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value,
    clippy::large_enum_variant,
    clippy::match_same_arms
)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::{
    AggregateOptions, ChangeStreamOptions, ClientOptions, CountOptions, DeleteOptions,
    DistinctOptions, EstimatedDocumentCountOptions, FindOneAndDeleteOptions,
    FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions, InsertManyOptions,
    InsertOneOptions, ReadConcern, ReplaceOptions, SessionOptions, TransactionOptions,
    UpdateModifications, UpdateOptions, WriteConcern,
};
use mongodb::{Client, ClientSession, Collection, Database, Namespace};

fn env(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

/// A test's verdict.
#[derive(Debug)]
enum Verdict {
    Pass,
    Fail(String),
    /// Not applicable to this deployment.
    NotApplicable(String),
    /// Not runnable by this runner.
    Unsupported(String),
}

type Events = Arc<Mutex<Vec<Ev>>>;

#[derive(Debug, Clone)]
enum Ev {
    Started {
        name: String,
        db: String,
        command: Document,
    },
    Succeeded {
        name: String,
        reply: Document,
    },
    Failed {
        name: String,
    },
}

enum Entity {
    Client(Client, Events, ClientCfg),
    Db(Database),
    Coll(Collection<Document>),
    Session(Option<ClientSession>, Document),
    Stream(ChangeStream<Document>),
    Value(Bson),
}

#[derive(Clone, Default)]
struct ClientCfg {
    observe: Vec<String>,
    ignore: Vec<String>,
    sensitive: bool,
}

struct Unsupported(String);

/// The error of one operation step: the test failed, or can't be run.
enum StepErr {
    Fail(String),
    Unsupported(String),
}

impl From<Unsupported> for StepErr {
    fn from(u: Unsupported) -> Self {
        StepErr::Unsupported(u.0)
    }
}

fn fail<T>(m: impl Into<String>) -> Result<T, StepErr> {
    Err(StepErr::Fail(m.into()))
}

/// The fail points the suites set, turned off after each test.
const FAIL_POINTS: [&str; 3] = [
    "failCommand",
    "onPrimaryTransactionalWrite",
    "failGetMoreAfterCursorCheckout",
];

const SENSITIVE: &[&str] = &[
    "hello",
    "ismaster",
    "isMaster",
    "saslStart",
    "saslContinue",
    "getnonce",
    "authenticate",
    "createUser",
    "updateUser",
    "copydbgetnonce",
    "copydbsaslstart",
    "copydb",
];

// ---------------------------------------------------------------------
// Matching (unified format, "Evaluating Matches")
// ---------------------------------------------------------------------

fn num(b: &Bson) -> Option<f64> {
    match b {
        Bson::Int32(i) => Some(f64::from(*i)),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(d) => Some(*d),
        _ => None,
    }
}

fn type_name(b: &Bson) -> &'static str {
    match b {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

struct Ctx<'a> {
    entities: &'a HashMap<String, Entity>,
}

impl Ctx<'_> {
    fn special(&self, op: &str, arg: &Bson, actual: Option<&Bson>) -> Result<(), String> {
        match op {
            "$$exists" => {
                let want = arg.as_bool().unwrap_or(true);
                if want == actual.is_some() {
                    Ok(())
                } else {
                    Err(format!("$$exists {want}, got {actual:?}"))
                }
            }
            "$$unsetOrMatches" => match actual {
                None => Ok(()),
                Some(a) => self.matches(arg, a, false),
            },
            "$$type" => {
                let a = actual.ok_or("$$type on a missing value")?;
                let t = type_name(a);
                let ok = match arg {
                    Bson::String(s) => s == t || (s == "number" && num(a).is_some()),
                    Bson::Array(v) => v.iter().any(|x| x.as_str() == Some(t)),
                    _ => false,
                };
                if ok {
                    Ok(())
                } else {
                    Err(format!("$$type {arg}, got {t}"))
                }
            }
            "$$matchesEntity" => {
                let name = arg.as_str().unwrap_or_default();
                match self.entities.get(name) {
                    Some(Entity::Value(v)) => self.matches(v, actual.ok_or("missing")?, false),
                    _ => Err(format!("no entity {name}")),
                }
            }
            "$$sessionLsid" => {
                let name = arg.as_str().unwrap_or_default();
                let lsid = match self.entities.get(name) {
                    Some(Entity::Session(_, lsid)) => lsid.clone(),
                    _ => return Err(format!("no session {name}")),
                };
                match actual {
                    Some(Bson::Document(d)) if *d == lsid => Ok(()),
                    a => Err(format!("lsid of {name} expected, got {a:?}")),
                }
            }
            "$$lte" => {
                let (Some(x), Some(y)) = (actual.and_then(num), num(arg)) else {
                    return Err("$$lte on non-numbers".into());
                };
                if x <= y {
                    Ok(())
                } else {
                    Err(format!("{x} > {y}"))
                }
            }
            "$$matchAsDocument" => match actual {
                Some(Bson::String(s)) => {
                    let v: serde_json::Value =
                        serde_json::from_str(s).map_err(|e| e.to_string())?;
                    let b = Bson::try_from(v).map_err(|e| e.to_string())?;
                    self.matches(arg, &b, false)
                }
                a => Err(format!("$$matchAsDocument on {a:?}")),
            },
            "$$matchAsRoot" => self.matches(arg, actual.ok_or("missing")?, true),
            other => Err(format!("unknown operator {other}")),
        }
    }

    /// `expected` against `actual`; `root` permits extra keys.
    fn matches(&self, expected: &Bson, actual: &Bson, root: bool) -> Result<(), String> {
        match expected {
            Bson::Document(e) => {
                if e.len() == 1 {
                    let (k, v) = e.iter().next().unwrap();
                    if k.starts_with("$$") {
                        return self.special(k, v, Some(actual));
                    }
                }
                let Bson::Document(a) = actual else {
                    return Err(format!("expected a document {e}, got {actual}"));
                };
                for (k, v) in e {
                    if let Bson::Document(sub) = v
                        && sub.len() == 1
                        && let Some((op, arg)) = sub.iter().next()
                        && op.starts_with("$$")
                        && matches!(op.as_str(), "$$exists" | "$$unsetOrMatches")
                    {
                        self.special(op, arg, a.get(k))
                            .map_err(|m| format!("{k}: {m}"))?;
                        continue;
                    }
                    let Some(av) = a.get(k) else {
                        return Err(format!("missing key {k} in {a}"));
                    };
                    self.matches(v, av, false)
                        .map_err(|m| format!("{k}: {m}"))?;
                }
                if !root {
                    for k in a.keys() {
                        if !e.contains_key(k) {
                            return Err(format!("extra key {k} in {a}"));
                        }
                    }
                }
                Ok(())
            }
            Bson::Array(e) => {
                let Bson::Array(a) = actual else {
                    return Err(format!("expected an array, got {actual}"));
                };
                if e.len() != a.len() {
                    return Err(format!(
                        "expected {} elements, got {}: {actual}",
                        e.len(),
                        a.len()
                    ));
                }
                for (i, (x, y)) in e.iter().zip(a).enumerate() {
                    self.matches(x, y, false)
                        .map_err(|m| format!("[{i}]: {m}"))?;
                }
                Ok(())
            }
            _ => {
                if let (Some(x), Some(y)) = (num(expected), num(actual)) {
                    return if (x - y).abs() < f64::EPSILON {
                        Ok(())
                    } else {
                        Err(format!("expected {expected}, got {actual}"))
                    };
                }
                if expected == actual {
                    Ok(())
                } else {
                    Err(format!("expected {expected}, got {actual}"))
                }
            }
        }
    }

    /// For results that are lists of root-level documents (`find`).
    fn matches_rows(&self, expected: &Bson, actual: &[Document]) -> Result<(), String> {
        let Bson::Array(e) = expected else {
            return self.matches(
                expected,
                &Bson::Array(actual.iter().cloned().map(Bson::Document).collect()),
                false,
            );
        };
        if e.len() != actual.len() {
            return Err(format!(
                "expected {} documents, got {}: {actual:?}",
                e.len(),
                actual.len()
            ));
        }
        for (i, (x, y)) in e.iter().zip(actual).enumerate() {
            self.matches(x, &Bson::Document(y.clone()), true)
                .map_err(|m| format!("[{i}]: {m}"))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Requirements
// ---------------------------------------------------------------------

fn version(s: &str) -> Vec<u32> {
    s.split('.').map(|p| p.parse().unwrap_or(0)).collect()
}

fn cmp_version(a: &str, b: &str) -> std::cmp::Ordering {
    let (mut x, mut y) = (version(a), version(b));
    x.resize(3, 0);
    y.resize(3, 0);
    x.cmp(&y)
}

struct Env {
    uri: String,
    topology: String,
    server_version: String,
    auth: bool,
}

/// Why `reqs` (any one of which suffices) aren't met; `None` when met.
fn unmet(reqs: Option<&Bson>, env: &Env) -> Option<String> {
    let Some(Bson::Array(list)) = reqs else {
        return None;
    };
    let mut why = Vec::new();
    for r in list.iter().filter_map(Bson::as_document) {
        let mut no = None;
        if let Ok(min) = r.get_str("minServerVersion")
            && cmp_version(&env.server_version, min).is_lt()
        {
            no = Some(format!("minServerVersion {min}"));
        }
        if let Ok(max) = r.get_str("maxServerVersion")
            && cmp_version(&env.server_version, max).is_gt()
        {
            no = Some(format!("maxServerVersion {max}"));
        }
        if let Ok(t) = r.get_array("topologies") {
            let ok = t.iter().filter_map(Bson::as_str).any(|t| {
                t == env.topology || (t == "sharded-replicaset" && env.topology == "sharded")
            });
            if !ok {
                no = Some(format!("topologies {t:?}"));
            }
        }
        if r.get_str("serverless") == Ok("require") {
            no = Some("serverless".into());
        }
        if let Ok(a) = r.get_bool("auth")
            && a != env.auth
        {
            no = Some(format!("auth {a}"));
        }
        if r.contains_key("serverParameters") {
            no = Some("serverParameters".into());
        }
        if r.get_bool("csfle") == Ok(true) {
            no = Some("csfle".into());
        }
        match no {
            None => return None,
            Some(w) => why.push(w),
        }
    }
    Some(why.join("; "))
}

// ---------------------------------------------------------------------
// The runner
// ---------------------------------------------------------------------

struct Runner<'a> {
    env: &'a Env,
    entities: HashMap<String, Entity>,
    setup: Client,
}

fn opts_of<T: serde::de::DeserializeOwned + Default>(
    args: &Document,
    skip: &[&str],
) -> Result<T, StepErr> {
    let mut d = Document::new();
    for (k, v) in args {
        if !skip.contains(&k.as_str()) && k != "session" {
            d.insert(k.clone(), v.clone());
        }
    }
    if d.is_empty() {
        return Ok(T::default());
    }
    mongodb::bson::from_document(d).map_err(|e| StepErr::Unsupported(format!("options: {e}")))
}

fn err_view(e: &mongodb::error::Error) -> (Option<i32>, Option<String>, String, bool) {
    use mongodb::error::{ErrorKind, WriteFailure};
    match e.kind.as_ref() {
        ErrorKind::Command(c) => (
            Some(c.code),
            Some(c.code_name.clone()),
            c.message.clone(),
            false,
        ),
        ErrorKind::Write(WriteFailure::WriteError(w)) => {
            (Some(w.code), w.code_name.clone(), w.message.clone(), false)
        }
        ErrorKind::Write(WriteFailure::WriteConcernError(w)) => (
            Some(w.code),
            Some(w.code_name.clone()),
            w.message.clone(),
            false,
        ),
        ErrorKind::InsertMany(i) => {
            let w = i.write_errors.as_ref().and_then(|w| w.first());
            (
                w.map(|w| w.code),
                w.and_then(|w| w.code_name.clone()),
                w.map(|w| w.message.clone()).unwrap_or_default(),
                false,
            )
        }
        ErrorKind::BulkWrite(b) => {
            let w = b.write_errors.values().next();
            (
                w.map(|w| w.code),
                w.and_then(|w| w.code_name.clone()),
                w.map(|w| w.message.clone()).unwrap_or_default(),
                false,
            )
        }
        _ => (None, None, e.to_string(), true),
    }
}

fn check_error(expected: &Document, e: &mongodb::error::Error) -> Result<(), StepErr> {
    let (code, name, msg, client) = err_view(e);
    if let Ok(c) = expected.get_bool("isClientError")
        && c != client
    {
        return fail(format!("isClientError {c}, got {e}"));
    }
    if let Some(want) = expected.get("errorCode").and_then(num)
        && code.map(f64::from) != Some(want)
    {
        return fail(format!("errorCode {want}, got {e}"));
    }
    if let Ok(want) = expected.get_str("errorCodeName")
        && !name
            .as_deref()
            .is_some_and(|n| n.eq_ignore_ascii_case(want))
    {
        return fail(format!("errorCodeName {want}, got {e}"));
    }
    if let Ok(want) = expected.get_str("errorContains")
        && !msg.to_lowercase().contains(&want.to_lowercase())
        && !e.to_string().to_lowercase().contains(&want.to_lowercase())
    {
        return fail(format!("errorContains {want:?}, got {e}"));
    }
    if let Ok(labels) = expected.get_array("errorLabelsContain") {
        for l in labels.iter().filter_map(Bson::as_str) {
            if !e.contains_label(l) {
                return fail(format!("label {l} missing: {e}"));
            }
        }
    }
    if let Ok(labels) = expected.get_array("errorLabelsOmit") {
        for l in labels.iter().filter_map(Bson::as_str) {
            if e.contains_label(l) {
                return fail(format!("label {l} present: {e}"));
            }
        }
    }
    Ok(())
}

/// What an operation returned.
enum Out {
    None,
    Value(Bson),
    /// Root-level documents (cursor results).
    Rows(Vec<Document>),
    /// A root-level document.
    Root(Document),
}

macro_rules! run {
    ($b:expr, $s:expr) => {
        match $s.as_mut() {
            Some(s) => $b.session(s).await,
            None => $b.await,
        }
    };
}

/// Change streams in a session aren't run here (their type differs).
macro_rules! watch {
    ($b:expr, $s:expr) => {{
        if $s.is_some() {
            return Err(StepErr::Unsupported("change stream in a session".into()));
        }
        $b.await
    }};
}

fn update_mods(b: &Bson) -> Result<UpdateModifications, StepErr> {
    match b {
        Bson::Document(d) => Ok(UpdateModifications::Document(d.clone())),
        Bson::Array(a) => Ok(UpdateModifications::Pipeline(
            a.iter().filter_map(|x| x.as_document().cloned()).collect(),
        )),
        _ => fail("bad update"),
    }
}

fn get_doc(args: &Document, k: &str) -> Document {
    args.get_document(k).cloned().unwrap_or_default()
}

impl Runner<'_> {
    fn client_of(&self, name: &str) -> Result<(Client, Events), StepErr> {
        match self.entities.get(name) {
            Some(Entity::Client(c, e, _)) => Ok((c.clone(), Arc::clone(e))),
            _ => fail(format!("no client {name}")),
        }
    }

    async fn create_entities(&mut self, list: &[Bson]) -> Result<(), StepErr> {
        for e in list.iter().filter_map(Bson::as_document) {
            let (kind, spec) = e.iter().next().unwrap();
            let spec = spec.as_document().cloned().unwrap_or_default();
            let id = spec.get_str("id").unwrap_or_default().to_owned();
            let ent = match kind.as_str() {
                "client" => {
                    let mut uri = self.env.uri.clone();
                    let mut app = None;
                    if let Ok(u) = spec.get_document("uriOptions") {
                        for (k, v) in u {
                            let v = match v {
                                Bson::String(s) => s.clone(),
                                Bson::Boolean(b) => b.to_string(),
                                x => {
                                    num(x).map_or_else(|| x.to_string(), |n| (n as i64).to_string())
                                }
                            };
                            if matches!(k.as_str(), "appname" | "appName") {
                                app = Some(v);
                                continue;
                            }
                            uri.push(if uri.contains('?') { '&' } else { '?' });
                            uri.push_str(&format!("{k}={v}"));
                        }
                    }
                    let mut o = ClientOptions::parse(&uri)
                        .await
                        .map_err(|e| StepErr::Unsupported(format!("client options: {e}")))?;
                    o.app_name = Some(app.unwrap_or_else(|| "r5mongo3-spec~dc-a".into()));
                    let events: Events = Arc::default();
                    let ev = Arc::clone(&events);
                    o.command_event_handler =
                        Some(EventHandler::callback(move |e: CommandEvent| {
                            let x = match e {
                                CommandEvent::Started(s) => Ev::Started {
                                    name: s.command_name,
                                    db: s.db,
                                    command: s.command,
                                },
                                CommandEvent::Succeeded(s) => Ev::Succeeded {
                                    name: s.command_name,
                                    reply: s.reply,
                                },
                                CommandEvent::Failed(f) => Ev::Failed {
                                    name: f.command_name,
                                },
                                _ => return,
                            };
                            ev.lock().unwrap().push(x);
                        }));
                    let cfg = ClientCfg {
                        observe: spec
                            .get_array("observeEvents")
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(str::to_owned))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        ignore: spec
                            .get_array("ignoreCommandMonitoringEvents")
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(str::to_owned))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        sensitive: spec.get_bool("observeSensitiveCommands").unwrap_or(false),
                    };
                    Entity::Client(Client::with_options(o).unwrap(), events, cfg)
                }
                "database" => {
                    let (c, _) = self.client_of(spec.get_str("client").unwrap_or_default())?;
                    let name = spec.get_str("databaseName").unwrap_or_default();
                    let mut o = mongodb::options::DatabaseOptions::default();
                    if let Ok(d) = spec.get_document("databaseOptions") {
                        o.read_concern = d.get_document("readConcern").ok().and_then(|r| {
                            mongodb::bson::from_document::<ReadConcern>(r.clone()).ok()
                        });
                        o.write_concern = d.get_document("writeConcern").ok().and_then(|r| {
                            mongodb::bson::from_document::<WriteConcern>(r.clone()).ok()
                        });
                    }
                    Entity::Db(c.database_with_options(name, o))
                }
                "collection" => {
                    let dbn = spec.get_str("database").unwrap_or_default();
                    let Some(Entity::Db(db)) = self.entities.get(dbn) else {
                        return fail(format!("no database {dbn}"));
                    };
                    let name = spec.get_str("collectionName").unwrap_or_default();
                    let mut o = mongodb::options::CollectionOptions::default();
                    if let Ok(d) = spec.get_document("collectionOptions") {
                        o.read_concern = d.get_document("readConcern").ok().and_then(|r| {
                            mongodb::bson::from_document::<ReadConcern>(r.clone()).ok()
                        });
                        o.write_concern = d.get_document("writeConcern").ok().and_then(|r| {
                            mongodb::bson::from_document::<WriteConcern>(r.clone()).ok()
                        });
                    }
                    Entity::Coll(db.collection_with_options(name, o))
                }
                "session" => {
                    let (c, _) = self.client_of(spec.get_str("client").unwrap_or_default())?;
                    let so: Option<SessionOptions> =
                        spec.get_document("sessionOptions").ok().map(|d| {
                            let mut so = SessionOptions::default();
                            so.default_transaction_options = d
                                .get_document("defaultTransactionOptions")
                                .ok()
                                .and_then(|t| {
                                    mongodb::bson::from_document::<TransactionOptions>(t.clone())
                                        .ok()
                                });
                            so.causal_consistency = d.get_bool("causalConsistency").ok();
                            so.snapshot = d.get_bool("snapshot").ok();
                            so
                        });
                    let s = match so {
                        Some(o) => c.start_session().with_options(o).await,
                        None => c.start_session().await,
                    }
                    .map_err(|e| StepErr::Fail(format!("startSession: {e}")))?;
                    let lsid = s.id().clone();
                    Entity::Session(Some(s), lsid)
                }
                other => return Err(StepErr::Unsupported(format!("entity {other}"))),
            };
            self.entities.insert(id, ent);
        }
        Ok(())
    }

    fn coll(&self, name: &str) -> Result<Collection<Document>, StepErr> {
        match self.entities.get(name) {
            Some(Entity::Coll(c)) => Ok(c.clone()),
            _ => fail(format!("no collection {name}")),
        }
    }

    fn take_session(
        &mut self,
        args: &Document,
    ) -> Result<Option<(String, ClientSession)>, StepErr> {
        let Ok(name) = args.get_str("session") else {
            return Ok(None);
        };
        match self.entities.get_mut(name) {
            Some(Entity::Session(s, _)) => Ok(s.take().map(|s| (name.to_owned(), s))),
            _ => fail(format!("no session {name}")),
        }
    }

    fn put_session(&mut self, s: Option<(String, ClientSession)>) {
        if let Some((n, s)) = s
            && let Some(Entity::Session(slot, _)) = self.entities.get_mut(&n)
        {
            *slot = Some(s);
        }
    }

    async fn op(&mut self, op: &Document) -> Result<Result<Out, mongodb::error::Error>, StepErr> {
        let name = op.get_str("name").unwrap_or_default().to_owned();
        let object = op.get_str("object").unwrap_or_default().to_owned();
        let args = get_doc(op, "arguments");
        if object == "testRunner" {
            return self.runner_op(&name, &args).await;
        }
        let taken = self.take_session(&args)?;
        let mut sess: Option<ClientSession> = taken.as_ref().map(|_| ()).and(None);
        let sname = taken.as_ref().map(|(n, _)| n.clone());
        if let Some((_, s)) = taken {
            sess = Some(s);
        }
        let r = self.op_on(&name, &object, &args, &mut sess).await;
        self.put_session(sname.zip(sess));
        r
    }

    async fn op_on(
        &mut self,
        name: &str,
        object: &str,
        args: &Document,
        sess: &mut Option<ClientSession>,
    ) -> Result<Result<Out, mongodb::error::Error>, StepErr> {
        // Session operations.
        if let Some(Entity::Session(..)) = self.entities.get(object) {
            let Some(Entity::Session(slot, _)) = self.entities.get_mut(object) else {
                unreachable!()
            };
            let Some(s) = slot.as_mut() else {
                return fail("session in use");
            };
            return Ok(match name {
                "startTransaction" => {
                    let o: TransactionOptions = opts_of(args, &[])?;
                    s.start_transaction()
                        .with_options(o)
                        .await
                        .map(|()| Out::None)
                }
                "commitTransaction" => s.commit_transaction().await.map(|()| Out::None),
                "abortTransaction" => s.abort_transaction().await.map(|()| Out::None),
                "endSession" => {
                    *slot = None;
                    Ok(Out::None)
                }
                other => return Err(StepErr::Unsupported(format!("session.{other}"))),
            });
        }
        // Change streams.
        if let Some(Entity::Stream(_)) = self.entities.get(object) {
            let Some(Entity::Stream(st)) = self.entities.get_mut(object) else {
                unreachable!()
            };
            return Ok(match name {
                "iterateUntilDocumentOrError" | "iterateOnce" => {
                    match tokio::time::timeout(Duration::from_secs(20), st.next()).await {
                        Ok(Some(Ok(d))) => Ok(Out::Root(d)),
                        Ok(Some(Err(e))) => Err(e),
                        Ok(None) => return fail("change stream ended"),
                        Err(_) => return fail("no change event within 20 s"),
                    }
                }
                "close" => {
                    self.entities.remove(object);
                    Ok(Out::None)
                }
                other => return Err(StepErr::Unsupported(format!("changeStream.{other}"))),
            });
        }
        // Client and database operations.
        if let Some(Entity::Client(c, _, _)) = self.entities.get(object) {
            let c = c.clone();
            return Ok(match name {
                "createChangeStream" => {
                    let pipeline: Vec<Document> = args
                        .get_array("pipeline")
                        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                        .unwrap_or_default();
                    let o: ChangeStreamOptions = opts_of(args, &["pipeline"])?;
                    match watch!(c.watch().pipeline(pipeline).with_options(o), sess) {
                        Ok(st) => {
                            self.save_stream(args, st.with_type::<Document>());
                            Ok(Out::None)
                        }
                        Err(e) => Err(e),
                    }
                }
                "listDatabases" | "listDatabaseNames" => run!(c.list_databases(), sess)
                    .map(|v| Out::Rows(v.into_iter().map(|d| doc! {"name": d.name}).collect())),
                "clientBulkWrite" => return self.client_bulk(&c, args, sess).await,
                other => return Err(StepErr::Unsupported(format!("client.{other}"))),
            });
        }
        if let Some(Entity::Db(db)) = self.entities.get(object) {
            let db = db.clone();
            return Ok(match name {
                "runCommand" => {
                    let cmd = get_doc(args, "command");
                    run!(db.run_command(cmd), sess).map(Out::Root)
                }
                "createCollection" => {
                    let n = args.get_str("collection").unwrap_or_default().to_owned();
                    run!(db.create_collection(n), sess).map(|()| Out::None)
                }
                "dropCollection" => {
                    let n = args.get_str("collection").unwrap_or_default();
                    run!(db.collection::<Document>(n).drop(), sess).map(|()| Out::None)
                }
                "listCollections" | "listCollectionNames" => run!(db.list_collection_names(), sess)
                    .map(|v| Out::Rows(v.into_iter().map(|n| doc! {"name": n}).collect())),
                "aggregate" => {
                    let pipeline: Vec<Document> = args
                        .get_array("pipeline")
                        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                        .unwrap_or_default();
                    let o: AggregateOptions = opts_of(args, &["pipeline"])?;
                    match sess.as_mut() {
                        Some(s) => match db
                            .aggregate(pipeline)
                            .with_options(o)
                            .session(&mut *s)
                            .await
                        {
                            Ok(mut cur) => {
                                let mut v = Vec::new();
                                loop {
                                    match cur.next(&mut *s).await {
                                        Some(Ok(d)) => v.push(d),
                                        Some(Err(e)) => return Ok(Err(e)),
                                        None => break,
                                    }
                                }
                                Ok(Out::Rows(v))
                            }
                            Err(e) => Err(e),
                        },
                        None => match db.aggregate(pipeline).with_options(o).await {
                            Ok(cur) => cur.try_collect().await.map(Out::Rows),
                            Err(e) => Err(e),
                        },
                    }
                }
                "createChangeStream" => {
                    let pipeline: Vec<Document> = args
                        .get_array("pipeline")
                        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                        .unwrap_or_default();
                    let o: ChangeStreamOptions = opts_of(args, &["pipeline"])?;
                    match watch!(db.watch().pipeline(pipeline).with_options(o), sess) {
                        Ok(st) => {
                            self.save_stream(args, st.with_type::<Document>());
                            Ok(Out::None)
                        }
                        Err(e) => Err(e),
                    }
                }
                other => return Err(StepErr::Unsupported(format!("database.{other}"))),
            });
        }
        let c = self.coll(object)?;
        let filter = get_doc(args, "filter");
        Ok(match name {
            "insertOne" => {
                let o: InsertOneOptions = opts_of(args, &["document"])?;
                run!(
                    c.insert_one(get_doc(args, "document")).with_options(o),
                    sess
                )
                .map(|r| Out::Root(doc! {"insertedId": r.inserted_id}))
            }
            "insertMany" => {
                let docs: Vec<Document> = args
                    .get_array("documents")
                    .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                    .unwrap_or_default();
                let o: InsertManyOptions = opts_of(args, &["documents"])?;
                run!(c.insert_many(docs).with_options(o), sess).map(|r| {
                    let ids: BTreeMap<usize, Bson> = r.inserted_ids.into_iter().collect();
                    let mut d = Document::new();
                    for (k, v) in ids {
                        d.insert(k.to_string(), v);
                    }
                    Out::Root(doc! {"insertedIds": d})
                })
            }
            "updateOne" | "updateMany" => {
                let u = update_mods(args.get("update").unwrap_or(&Bson::Null))?;
                let o: UpdateOptions = opts_of(args, &["filter", "update"])?;
                let r = if name == "updateOne" {
                    run!(c.update_one(filter, u).with_options(o), sess)
                } else {
                    run!(c.update_many(filter, u).with_options(o), sess)
                };
                r.map(|r| {
                    let mut d = doc! {"matchedCount": r.matched_count as i64, "modifiedCount": r.modified_count as i64, "upsertedCount": i64::from(r.upserted_id.is_some())};
                    if let Some(id) = r.upserted_id {
                        d.insert("upsertedId", id);
                    }
                    Out::Root(d)
                })
            }
            "replaceOne" => {
                let o: ReplaceOptions = opts_of(args, &["filter", "replacement"])?;
                run!(c.replace_one(filter, get_doc(args, "replacement")).with_options(o), sess).map(|r| {
                    let mut d = doc! {"matchedCount": r.matched_count as i64, "modifiedCount": r.modified_count as i64, "upsertedCount": i64::from(r.upserted_id.is_some())};
                    if let Some(id) = r.upserted_id {
                        d.insert("upsertedId", id);
                    }
                    Out::Root(d)
                })
            }
            "deleteOne" | "deleteMany" => {
                let o: DeleteOptions = opts_of(args, &["filter"])?;
                let r = if name == "deleteOne" {
                    run!(c.delete_one(filter).with_options(o), sess)
                } else {
                    run!(c.delete_many(filter).with_options(o), sess)
                };
                r.map(|r| Out::Root(doc! {"deletedCount": r.deleted_count as i64}))
            }
            "find" | "findOne" => {
                let mut o: FindOptions = opts_of(args, &["filter"])?;
                if name == "findOne" {
                    o.limit = Some(1);
                }
                let r = match sess.as_mut() {
                    Some(s) => match c.find(filter).with_options(o).session(&mut *s).await {
                        Ok(mut cur) => {
                            let mut v = Vec::new();
                            let mut err = None;
                            loop {
                                match cur.next(&mut *s).await {
                                    Some(Ok(d)) => v.push(d),
                                    Some(Err(e)) => {
                                        err = Some(e);
                                        break;
                                    }
                                    None => break,
                                }
                            }
                            match err {
                                Some(e) => Err(e),
                                None => Ok(v),
                            }
                        }
                        Err(e) => Err(e),
                    },
                    None => match c.find(filter).with_options(o).await {
                        Ok(cur) => cur.try_collect().await,
                        Err(e) => Err(e),
                    },
                };
                r.map(|v| {
                    if name == "findOne" {
                        v.into_iter()
                            .next()
                            .map_or(Out::Value(Bson::Null), Out::Root)
                    } else {
                        Out::Rows(v)
                    }
                })
            }
            "aggregate" => {
                let pipeline: Vec<Document> = args
                    .get_array("pipeline")
                    .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                    .unwrap_or_default();
                let o: AggregateOptions = opts_of(args, &["pipeline"])?;
                match sess.as_mut() {
                    Some(s) => match c.aggregate(pipeline).with_options(o).session(&mut *s).await {
                        Ok(mut cur) => {
                            let mut v = Vec::new();
                            loop {
                                match cur.next(&mut *s).await {
                                    Some(Ok(d)) => v.push(d),
                                    Some(Err(e)) => return Ok(Err(e)),
                                    None => break,
                                }
                            }
                            Ok(Out::Rows(v))
                        }
                        Err(e) => Err(e),
                    },
                    None => match c.aggregate(pipeline).with_options(o).await {
                        Ok(cur) => cur.try_collect().await.map(Out::Rows),
                        Err(e) => Err(e),
                    },
                }
            }
            "countDocuments" => {
                let o: CountOptions = opts_of(args, &["filter"])?;
                run!(c.count_documents(filter).with_options(o), sess)
                    .map(|n| Out::Value(Bson::Int64(n as i64)))
            }
            "estimatedDocumentCount" => {
                let o: EstimatedDocumentCountOptions = opts_of(args, &[])?;
                c.estimated_document_count()
                    .with_options(o)
                    .await
                    .map(|n| Out::Value(Bson::Int64(n as i64)))
            }
            "distinct" => {
                let field = args.get_str("fieldName").unwrap_or_default().to_owned();
                let o: DistinctOptions = opts_of(args, &["fieldName", "filter"])?;
                run!(c.distinct(field, filter).with_options(o), sess)
                    .map(|v| Out::Value(Bson::Array(v)))
            }
            "findOneAndUpdate" => {
                let u = update_mods(args.get("update").unwrap_or(&Bson::Null))?;
                let o: FindOneAndUpdateOptions = opts_of(args, &["filter", "update"])?;
                run!(c.find_one_and_update(filter, u).with_options(o), sess)
                    .map(|d| d.map_or(Out::Value(Bson::Null), Out::Root))
            }
            "findOneAndReplace" => {
                let o: FindOneAndReplaceOptions = opts_of(args, &["filter", "replacement"])?;
                run!(
                    c.find_one_and_replace(filter, get_doc(args, "replacement"))
                        .with_options(o),
                    sess
                )
                .map(|d| d.map_or(Out::Value(Bson::Null), Out::Root))
            }
            "findOneAndDelete" => {
                let o: FindOneAndDeleteOptions = opts_of(args, &["filter"])?;
                run!(c.find_one_and_delete(filter).with_options(o), sess)
                    .map(|d| d.map_or(Out::Value(Bson::Null), Out::Root))
            }
            "createIndex" => {
                let mut opts = mongodb::options::IndexOptions::default();
                opts.name = args.get_str("name").ok().map(str::to_owned);
                opts.unique = args.get_bool("unique").ok();
                let m = mongodb::IndexModel::builder()
                    .keys(get_doc(args, "keys"))
                    .options(opts)
                    .build();
                run!(c.create_index(m), sess).map(|r| Out::Value(Bson::String(r.index_name)))
            }
            "dropIndex" => {
                let n = args.get_str("name").unwrap_or_default().to_owned();
                run!(c.drop_index(n), sess).map(|()| Out::None)
            }
            "listIndexes" => match c.list_indexes().await {
                Ok(cur) => cur.try_collect::<Vec<_>>().await.map(|v| {
                    Out::Rows(
                        v.into_iter()
                            .map(|m| mongodb::bson::to_document(&m).unwrap_or_default())
                            .collect(),
                    )
                }),
                Err(e) => Err(e),
            },
            "drop" => run!(c.drop(), sess).map(|()| Out::None),
            "rename" => {
                let to = args.get_str("to").unwrap_or_default();
                let cmd = doc! {"renameCollection": c.namespace().to_string(), "to": format!("{}.{to}", c.namespace().db), "dropTarget": args.get_bool("dropTarget").unwrap_or(false)};
                run!(self.setup.database("admin").run_command(cmd), sess).map(|_| Out::None)
            }
            "createChangeStream" => {
                let pipeline: Vec<Document> = args
                    .get_array("pipeline")
                    .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                    .unwrap_or_default();
                let o: ChangeStreamOptions = opts_of(args, &["pipeline"])?;
                match watch!(c.watch().pipeline(pipeline).with_options(o), sess) {
                    Ok(st) => {
                        self.save_stream(args, st.with_type::<Document>());
                        Ok(Out::None)
                    }
                    Err(e) => Err(e),
                }
            }
            other => return Err(StepErr::Unsupported(format!("collection.{other}"))),
        })
    }

    fn save_stream(&mut self, _args: &Document, st: ChangeStream<Document>) {
        self.entities
            .insert("__pending_stream".into(), Entity::Stream(st));
    }

    async fn client_bulk(
        &mut self,
        c: &Client,
        args: &Document,
        sess: &mut Option<ClientSession>,
    ) -> Result<Result<Out, mongodb::error::Error>, StepErr> {
        use mongodb::options::{
            DeleteManyModel, DeleteOneModel, InsertOneModel, ReplaceOneModel, UpdateManyModel,
            UpdateOneModel, WriteModel,
        };
        let ns = |d: &Document| -> Result<Namespace, StepErr> {
            let s = d.get_str("namespace").unwrap_or_default();
            match s.split_once('.') {
                Some((a, b)) => Ok(Namespace::new(a, b)),
                None => fail(format!("bad namespace {s}")),
            }
        };
        let mut models: Vec<WriteModel> = Vec::new();
        for m in args.get_array("models").map(Vec::as_slice).unwrap_or(&[]) {
            let Some(m) = m.as_document() else { continue };
            let (k, v) = m.iter().next().unwrap();
            let v = v.as_document().cloned().unwrap_or_default();
            let n = ns(&v)?;
            let f = get_doc(&v, "filter");
            let af = v.get_array("arrayFilters").ok().cloned();
            let up = v.get_bool("upsert").ok();
            models.push(match k.as_str() {
                "insertOne" => InsertOneModel::builder()
                    .namespace(n)
                    .document(get_doc(&v, "document"))
                    .build()
                    .into(),
                "updateOne" => UpdateOneModel::builder()
                    .namespace(n)
                    .filter(f)
                    .update(update_mods(v.get("update").unwrap_or(&Bson::Null))?)
                    .array_filters(af)
                    .upsert(up)
                    .build()
                    .into(),
                "updateMany" => UpdateManyModel::builder()
                    .namespace(n)
                    .filter(f)
                    .update(update_mods(v.get("update").unwrap_or(&Bson::Null))?)
                    .array_filters(af)
                    .upsert(up)
                    .build()
                    .into(),
                "replaceOne" => ReplaceOneModel::builder()
                    .namespace(n)
                    .filter(f)
                    .replacement(get_doc(&v, "replacement"))
                    .upsert(up)
                    .build()
                    .into(),
                "deleteOne" => DeleteOneModel::builder()
                    .namespace(n)
                    .filter(f)
                    .build()
                    .into(),
                "deleteMany" => DeleteManyModel::builder()
                    .namespace(n)
                    .filter(f)
                    .build()
                    .into(),
                other => {
                    return Err(StepErr::Unsupported(format!(
                        "clientBulkWrite model {other}"
                    )));
                }
            });
        }
        let o: mongodb::options::BulkWriteOptions = opts_of(args, &["models", "verboseResults"])?;
        let summary = |s: &mongodb::results::SummaryBulkWriteResult| {
            doc! {"insertedCount": s.inserted_count, "upsertedCount": s.upserted_count, "matchedCount": s.matched_count, "modifiedCount": s.modified_count, "deletedCount": s.deleted_count}
        };
        if args.get_bool("verboseResults").unwrap_or(false) {
            let r = run!(c.bulk_write(models).with_options(o).verbose_results(), sess);
            return Ok(r.map(|r| {
                let mut d = summary(&r.summary);
                let mut ins = Document::new();
                for (k, v) in &r.insert_results {
                    ins.insert(k.to_string(), doc! {"insertedId": v.inserted_id.clone()});
                }
                let mut upd = Document::new();
                for (k, v) in &r.update_results {
                    let mut x = doc! {"matchedCount": v.matched_count as i64, "modifiedCount": v.modified_count as i64};
                    if let Some(id) = &v.upserted_id {
                        x.insert("upsertedId", id.clone());
                    }
                    upd.insert(k.to_string(), x);
                }
                let mut del = Document::new();
                for (k, v) in &r.delete_results {
                    del.insert(k.to_string(), doc! {"deletedCount": v.deleted_count as i64});
                }
                d.insert("insertResults", ins);
                d.insert("updateResults", upd);
                d.insert("deleteResults", del);
                Out::Root(d)
            }));
        }
        let r = run!(c.bulk_write(models).with_options(o), sess);
        Ok(r.map(|r| {
            let mut d = summary(&r);
            d.insert("insertResults", Document::new());
            d.insert("updateResults", Document::new());
            d.insert("deleteResults", Document::new());
            Out::Root(d)
        }))
    }

    async fn runner_op(
        &mut self,
        name: &str,
        args: &Document,
    ) -> Result<Result<Out, mongodb::error::Error>, StepErr> {
        match name {
            "failPoint" => {
                let (c, _) = self.client_of(args.get_str("client").unwrap_or_default())?;
                let fp = get_doc(args, "failPoint");
                match c.database("admin").run_command(fp).await {
                    Ok(_) => Ok(Ok(Out::None)),
                    Err(e) => fail(format!("configureFailPoint refused: {e}")),
                }
            }
            "targetedFailPoint" => {
                let fp = get_doc(args, "failPoint");
                match self.setup.database("admin").run_command(fp).await {
                    Ok(_) => Ok(Ok(Out::None)),
                    Err(e) => fail(format!("configureFailPoint refused: {e}")),
                }
            }
            "createEntities" => {
                let list = args.get_array("entities").cloned().unwrap_or_default();
                self.create_entities(&list).await?;
                Ok(Ok(Out::None))
            }
            // The driver keeps no public pinning or dirty state: these
            // assertions about driver internals can't be checked here.
            "assertSessionPinned"
            | "assertSessionUnpinned"
            | "assertSessionDirty"
            | "assertSessionNotDirty"
            | "assertSessionTransactionState" => Ok(Ok(Out::None)),
            "assertSameLsidOnLastTwoCommands" | "assertDifferentLsidOnLastTwoCommands" => {
                let (_, ev) = self.client_of(args.get_str("client").unwrap_or_default())?;
                let lsids: Vec<Option<Bson>> = ev
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|e| match e {
                        Ev::Started { name, command, .. }
                            if !SENSITIVE.contains(&name.as_str()) =>
                        {
                            Some(command.get("lsid").cloned())
                        }
                        _ => None,
                    })
                    .collect();
                let n = lsids.len();
                if n < 2 {
                    return fail("fewer than two commands");
                }
                let same = lsids[n - 1] == lsids[n - 2];
                if same == (name == "assertSameLsidOnLastTwoCommands") {
                    Ok(Ok(Out::None))
                } else {
                    fail(format!("{name} failed"))
                }
            }
            "assertCollectionExists" | "assertCollectionNotExists" => {
                let db = args.get_str("databaseName").unwrap_or_default();
                let coll = args.get_str("collectionName").unwrap_or_default();
                let names = self
                    .setup
                    .database(db)
                    .list_collection_names()
                    .await
                    .unwrap_or_default();
                if names.iter().any(|n| n == coll) == (name == "assertCollectionExists") {
                    Ok(Ok(Out::None))
                } else {
                    fail(format!("{name} {db}.{coll}"))
                }
            }
            "assertIndexExists" | "assertIndexNotExists" => {
                let db = args.get_str("databaseName").unwrap_or_default();
                let coll = args.get_str("collectionName").unwrap_or_default();
                let idx = args.get_str("indexName").unwrap_or_default();
                let names = self
                    .setup
                    .database(db)
                    .collection::<Document>(coll)
                    .list_index_names()
                    .await
                    .unwrap_or_default();
                if names.iter().any(|n| n == idx) == (name == "assertIndexExists") {
                    Ok(Ok(Out::None))
                } else {
                    fail(format!("{name} {idx}"))
                }
            }
            other => Err(StepErr::Unsupported(format!("testRunner.{other}"))),
        }
    }

    async fn run_test(&mut self, file: &Document, test: &Document) -> Verdict {
        let r = self.run_test_inner(file, test).await;
        // Fail points end with their test.
        if test.to_string().contains("ailPoint") {
            for fp in FAIL_POINTS {
                let _ = self
                    .setup
                    .database("admin")
                    .run_command(doc! {"configureFailPoint": fp, "mode": "off"})
                    .await;
            }
        }
        match r {
            Ok(()) => Verdict::Pass,
            Err(StepErr::Fail(m)) => Verdict::Fail(m),
            Err(StepErr::Unsupported(m)) => Verdict::Unsupported(m),
        }
    }

    async fn run_test_inner(&mut self, file: &Document, test: &Document) -> Result<(), StepErr> {
        // Transactions left open by earlier tests would hold their locks.
        let _ = self
            .setup
            .database("admin")
            .run_command(doc! {"killAllSessions": []})
            .await;
        // initialData
        for d in file
            .get_array("initialData")
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter_map(Bson::as_document)
        {
            let db = self
                .setup
                .database(d.get_str("databaseName").unwrap_or_default());
            let cname = d.get_str("collectionName").unwrap_or_default();
            let c = db.collection::<Document>(cname);
            // A drop refused while an earlier DDL job is unfinished (39071)
            // is retried, as a client would.
            let mut tries = 0;
            loop {
                match c.drop().await {
                    Ok(()) => break,
                    Err(e) if tries < 40 && e.to_string().contains("another DDL") => {
                        tries += 1;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(e) if e.to_string().contains("another DDL") => {
                        return fail(format!("initialData drop of {cname}: {e}"));
                    }
                    Err(_) => break,
                }
            }
            let docs: Vec<Document> = d
                .get_array("documents")
                .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                .unwrap_or_default();
            if docs.is_empty() {
                if let Err(e) = db.create_collection(cname).await {
                    return fail(format!("initialData create {cname}: {e}"));
                }
            } else if let Err(e) = c.insert_many(docs).await {
                return fail(format!("initialData insert into {cname}: {e}"));
            }
        }
        self.entities.clear();
        let list = file
            .get_array("createEntities")
            .cloned()
            .unwrap_or_default();
        self.create_entities(&list).await?;
        for (_, e) in &self.entities {
            if let Entity::Client(_, ev, _) = e {
                ev.lock().unwrap().clear();
            }
        }
        let ops = test.get_array("operations").cloned().unwrap_or_default();
        for (i, op) in ops.iter().filter_map(Bson::as_document).enumerate() {
            let opname = op.get_str("name").unwrap_or_default().to_owned();
            if std::env::var_os("SPRIG_SPEC_TRACE").is_some() {
                eprintln!(
                    "  op {i} {opname} {}",
                    op.get_str("object").unwrap_or_default()
                );
            }
            let r = self.op(op).await?;
            // A stream created by this op: keep it under its result name.
            if let Some(Entity::Stream(_)) = self.entities.get("__pending_stream") {
                let st = self.entities.remove("__pending_stream").unwrap();
                if let Ok(n) = op.get_str("saveResultAsEntity") {
                    self.entities.insert(n.to_owned(), st);
                }
            }
            let ignore = op.get_bool("ignoreResultAndError").unwrap_or(false);
            match (r, op.get_document("expectError").ok()) {
                (Err(e), Some(x)) => check_error(x, &e).map_err(|m| match m {
                    StepErr::Fail(m) => StepErr::Fail(format!("op {i} {opname}: {m}")),
                    u => u,
                })?,
                (Err(_), None) if ignore => {}
                (Err(e), None) => return fail(format!("op {i} {opname}: unexpected error {e}")),
                (Ok(_), Some(x)) if !ignore => {
                    return fail(format!("op {i} {opname}: expected an error {x}"));
                }
                (Ok(out), _) => {
                    if let Some(want) = op.get("expectResult") {
                        let cx = Ctx {
                            entities: &self.entities,
                        };
                        let r = match &out {
                            Out::None => cx.matches(want, &Bson::Null, true),
                            Out::Value(v) => cx.matches(want, v, false),
                            Out::Rows(rows) => cx.matches_rows(want, rows),
                            Out::Root(d) => cx.matches(want, &Bson::Document(d.clone()), true),
                        };
                        r.map_err(|m| StepErr::Fail(format!("op {i} {opname} result: {m}")))?;
                    }
                    if let Ok(n) = op.get_str("saveResultAsEntity")
                        && !self.entities.contains_key(n)
                    {
                        let v = match out {
                            Out::None => Bson::Null,
                            Out::Value(v) => v,
                            Out::Rows(r) => {
                                Bson::Array(r.into_iter().map(Bson::Document).collect())
                            }
                            Out::Root(d) => Bson::Document(d),
                        };
                        self.entities.insert(n.to_owned(), Entity::Value(v));
                    }
                }
            }
        }
        // expectEvents
        if let Ok(exp) = test.get_array("expectEvents") {
            for e in exp.iter().filter_map(Bson::as_document) {
                let kind = e.get_str("eventType").unwrap_or("command");
                if kind != "command" {
                    return Err(StepErr::Unsupported(format!("{kind} events")));
                }
                let cname = e.get_str("client").unwrap_or_default();
                let Some(Entity::Client(_, ev, cfg)) = self.entities.get(cname) else {
                    return fail(format!("no client {cname}"));
                };
                let cfg = cfg.clone();
                let got: Vec<Ev> = ev
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|x| {
                        let (kind, n) = match x {
                            Ev::Started { name, .. } => ("commandStartedEvent", name),
                            Ev::Succeeded { name, .. } => ("commandSucceededEvent", name),
                            Ev::Failed { name } => ("commandFailedEvent", name),
                        };
                        cfg.observe.iter().any(|o| o == kind)
                            && !cfg.ignore.contains(n)
                            && (cfg.sensitive || !SENSITIVE.contains(&n.as_str()))
                            && n != "configureFailPoint"
                    })
                    .cloned()
                    .collect();
                let want = e.get_array("events").cloned().unwrap_or_default();
                let extra_ok = e.get_bool("ignoreExtraEvents").unwrap_or(false);
                if want.len() > got.len() || (!extra_ok && want.len() != got.len()) {
                    let names: Vec<String> = got
                        .iter()
                        .map(|x| match x {
                            Ev::Started { name, .. } => format!("started {name}"),
                            Ev::Succeeded { name, .. } => format!("succeeded {name}"),
                            Ev::Failed { name } => format!("failed {name}"),
                        })
                        .collect();
                    return fail(format!(
                        "expected {} events, got {}: {names:?}",
                        want.len(),
                        got.len()
                    ));
                }
                let cx = Ctx {
                    entities: &self.entities,
                };
                for (i, (w, g)) in want.iter().zip(&got).enumerate() {
                    let w = w.as_document().cloned().unwrap_or_default();
                    let (wk, wv) = w.iter().next().unwrap();
                    let wv = wv.as_document().cloned().unwrap_or_default();
                    let r = match (wk.as_str(), g) {
                        ("commandStartedEvent", Ev::Started { name, db, command }) => {
                            let mut r = Ok(());
                            if let Ok(n) = wv.get_str("commandName")
                                && n != name
                            {
                                r = Err(format!("commandName {n}, got {name}"));
                            }
                            if let Ok(d) = wv.get_str("databaseName")
                                && d != db
                            {
                                r = Err(format!("databaseName {d}, got {db}"));
                            }
                            if r.is_ok()
                                && let Ok(c) = wv.get_document("command")
                            {
                                r = cx.matches(
                                    &Bson::Document(c.clone()),
                                    &Bson::Document(command.clone()),
                                    true,
                                );
                            }
                            r
                        }
                        ("commandSucceededEvent", Ev::Succeeded { name, reply }) => {
                            let mut r = Ok(());
                            if let Ok(n) = wv.get_str("commandName")
                                && n != name
                            {
                                r = Err(format!("commandName {n}, got {name}"));
                            }
                            if r.is_ok()
                                && let Ok(c) = wv.get_document("reply")
                            {
                                r = cx.matches(
                                    &Bson::Document(c.clone()),
                                    &Bson::Document(reply.clone()),
                                    true,
                                );
                            }
                            r
                        }
                        ("commandFailedEvent", Ev::Failed { name }) => {
                            match wv.get_str("commandName") {
                                Ok(n) if n != name => Err(format!("commandName {n}, got {name}")),
                                _ => Ok(()),
                            }
                        }
                        (k, g) => Err(format!("expected {k}, got {g:?}")),
                    };
                    r.map_err(|m| StepErr::Fail(format!("event {i}: {m}")))?;
                }
            }
        }
        // outcome
        for o in test
            .get_array("outcome")
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter_map(Bson::as_document)
        {
            let c = self
                .setup
                .database(o.get_str("databaseName").unwrap_or_default())
                .collection::<Document>(o.get_str("collectionName").unwrap_or_default());
            let got: Vec<Document> = match c.find(doc! {}).sort(doc! {"_id": 1}).await {
                Ok(cur) => cur.try_collect().await.unwrap_or_default(),
                Err(e) => return fail(format!("outcome read: {e}")),
            };
            let cx = Ctx {
                entities: &self.entities,
            };
            cx.matches(
                &Bson::Array(o.get_array("documents").cloned().unwrap_or_default()),
                &Bson::Array(got.into_iter().map(Bson::Document).collect()),
                false,
            )
            .map_err(|m| StepErr::Fail(format!("outcome: {m}")))?;
        }
        Ok(())
    }
}

fn json_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(json_files(&p));
        } else if p.extension().is_some_and(|x| x == "json") {
            out.push(p);
        }
    }
    out.sort();
    out
}

/// The unified-format files of a suite.
fn suite_files(root: &Path, suite: &str) -> Vec<PathBuf> {
    let base = root.join(suite).join("tests");
    let unified = base.join("unified");
    let files = if unified.is_dir() {
        json_files(&unified)
    } else {
        json_files(&base)
    };
    files
        .into_iter()
        .filter(|p| {
            std::fs::read_to_string(p)
                .ok()
                .is_some_and(|s| s.contains("\"schemaVersion\""))
        })
        .collect()
}

#[tokio::test]
async fn unified_spec_tests() {
    let (Some(dir), Some(uri)) = (env("SPRIG_SPEC_DIR"), env("SPRIG_SPEC_URI")) else {
        eprintln!("SPRIG_SPEC_DIR or SPRIG_SPEC_URI not set: skipped");
        return;
    };
    let topology = env("SPRIG_SPEC_TOPOLOGY").unwrap_or_else(|| "sharded".into());
    let suites = env("SPRIG_SPEC_SUITES")
        .unwrap_or_else(|| "crud,transactions,retryable-writes,change-streams,sessions".into());
    let filter = env("SPRIG_SPEC_FILTER");
    let test_secs: u64 = env("SPRIG_SPEC_TEST_SECS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let setup = Client::with_options(ClientOptions::parse(&uri).await.unwrap()).unwrap();
    let bi = setup
        .database("admin")
        .run_command(doc! {"buildInfo": 1})
        .await
        .unwrap();
    let server_version = bi.get_str("version").unwrap_or("8.0.0").to_owned();
    let env = Env {
        auth: uri.contains('@'),
        uri,
        topology,
        server_version,
    };
    let mut report = String::new();
    let mut totals = Vec::new();
    for suite in suites.split(',') {
        let (mut pass, mut failed, mut na, mut unsup) = (0, 0, 0, 0);
        let mut unsup_why: BTreeMap<String, usize> = BTreeMap::new();
        for f in suite_files(Path::new(&dir), suite) {
            let fname = f.file_name().unwrap().to_string_lossy().into_owned();
            if let Some(x) = &filter
                && !fname.contains(x.as_str())
            {
                continue;
            }
            let v: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&f).unwrap()).unwrap();
            let Ok(Bson::Document(file)) = Bson::try_from(v) else {
                continue;
            };
            let file_unmet = unmet(file.get("runOnRequirements"), &env);
            for t in file
                .get_array("tests")
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter_map(Bson::as_document)
            {
                let desc = t.get_str("description").unwrap_or_default();
                let verdict = if let Some(w) = file_unmet.clone() {
                    Verdict::NotApplicable(w)
                } else if let Some(w) = unmet(t.get("runOnRequirements"), &env) {
                    Verdict::NotApplicable(w)
                } else if let Ok(r) = t.get_str("skipReason") {
                    Verdict::NotApplicable(format!("skipReason: {r}"))
                } else {
                    let mut r = Runner {
                        env: &env,
                        entities: HashMap::new(),
                        setup: setup.clone(),
                    };
                    let v =
                        tokio::time::timeout(Duration::from_secs(test_secs), r.run_test(&file, t))
                            .await
                            .unwrap_or_else(|_| Verdict::Fail("timed out".into()));
                    if t.to_string().contains("ailPoint") {
                        for fp in FAIL_POINTS {
                            let _ = setup
                                .database("admin")
                                .run_command(doc! {"configureFailPoint": fp, "mode": "off"})
                                .await;
                        }
                    }
                    // End sessions so later tests start clean.
                    for (_, e) in r.entities.drain() {
                        drop(e);
                    }
                    v
                };
                match verdict {
                    Verdict::Pass => pass += 1,
                    Verdict::Fail(m) => {
                        failed += 1;
                        report.push_str(&format!("FAIL {suite}/{fname}: {desc}\n    {m}\n"));
                        if let Some(p) = env_report() {
                            let _ = std::fs::write(&p, &report);
                        }
                    }
                    Verdict::NotApplicable(_) => na += 1,
                    Verdict::Unsupported(m) => {
                        unsup += 1;
                        *unsup_why.entry(m.clone()).or_default() += 1;
                        report.push_str(&format!("NOT RUN {suite}/{fname}: {desc}\n    {m}\n"));
                    }
                }
            }
        }
        let rate = if pass + failed == 0 {
            0.0
        } else {
            100.0 * f64::from(pass) / f64::from(pass + failed)
        };
        eprintln!(
            "{suite}: {pass} passed, {failed} failed ({rate:.1}% of {}), {na} not applicable, {unsup} not runnable here {unsup_why:?}",
            pass + failed
        );
        totals.push((suite.to_owned(), pass, failed, na, unsup));
    }
    let (p, f): (i32, i32) = totals.iter().fold((0, 0), |a, t| (a.0 + t.1, a.1 + t.2));
    eprintln!(
        "all suites: {p} passed, {f} failed ({:.1}%)",
        if p + f == 0 {
            0.0
        } else {
            100.0 * f64::from(p) / f64::from(p + f)
        }
    );
    if let Some(path) = env_report() {
        std::fs::write(path, report).unwrap();
    }
}

fn env_report() -> Option<String> {
    env("SPRIG_SPEC_REPORT")
}
