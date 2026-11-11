//! The logic editors' routes (chapter 44 step 26; chapter 37's `Logic`
//! service, Studio part: `Logic.Compile` and the module views; decision
//! ST42), for the caller's own tenant.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/databases/{db}/logic` | The database's modules (`logic_modules`): kind, runtime, security, tier preference, active version, state |
//! | `GET /v1/databases/{db}/logic/{kind}/{name}` | One module with its versions (tier, placement, state, compiler), the active version's source, its binding and schedule, and, for routine administrators, the generated PL/SQL |
//! | `GET /v1/databases/{db}/logic/{kind}/{name}/versions/{v}` | One version's source (and PL/SQL for routine administrators) |
//! | `POST /v1/databases/{db}/logic:compile` | `Logic.Compile`: a dry run of a `CREATE FUNCTION | PROCEDURE ... LANGUAGE ...` declaration against the database's columns: tier, reasons with where they are, lints, cost, tables read and written, the signature, and the PL/SQL for routine administrators. Nothing is recorded or deployed |
//! | `GET .../{kind}/{name}/logs?after=&limit=` | `Logic.TailLogs`: what the module's triggers and schedules did (`logic_log`), newest first, after an id (Studio polls it) |
//! | `GET .../{kind}/{name}/deadLetters?state=` | The module's dead letters (`logic_dead_letters` of its binding) |
//! | `POST .../{kind}/{name}/deadLetters:replay`, `:discard` | `{ids: [..]}` or `{all: true}`: back to the trigger's worker, in order with the active version, or discarded |
//!
//! Deploying, dropping and rolling back are the statements a SQL client
//! runs (`CREATE OR REPLACE ...`, `DROP ...`, `ALTER ... ROLLBACK [TO
//! VERSION n]`), which Studio shows before it runs them; test runs are a
//! `CALL` in a transaction handle that Studio rolls back. So these routes
//! only read the catalog, compile without deploying, and settle dead
//! letters.
//!
//! Who may see what: a module shows to a caller with a grant on its
//! database (or `*.*`, or `SPRIG_TENANT_ADMIN`), as the workload views
//! decide. Source, PL/SQL, logs and dead letters (which hold row data) need
//! `ALTER ROUTINE`, `CREATE ROUTINE` or `ALL` on the database or `*.*`, or
//! `SPRIG_TENANT_ADMIN` (1044 otherwise): chapter 44 shows the generated
//! PL/SQL "to users with alter routine".

use std::sync::Arc;

use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use serde_json::{Value, json};
use sprig_common::{SprigCode, SprigError};
use sprig_logic::LogicStore;
use sprig_logic::compiler::{Options, Runtime, StaticColumns};
use sprig_logic::declare::{Declaration, Kind};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{hdr, invalid, json_response, parse_json, quote_literal, read_body, rows_of, run_sql};

/// Rows of logs and dead letters returned at most.
const MAX_ROWS: u32 = 500;
/// `logic.max_loop_iterations`'s default (chapter 41), for dry runs.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
/// `logic.compile_max_cost`'s default.
const COMPILE_MAX_COST: u64 = 100_000;

/// What the caller may see of a database's logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Access {
    /// The modules are listed.
    pub list: bool,
    /// Source, PL/SQL, logs and dead letters.
    pub routines: bool,
}

/// Reads `SHOW GRANTS` lines for `db` (lower-cased comparison).
#[must_use]
pub fn access_of(lines: &[String], db: &str) -> Access {
    let privs = super::grants::Privileges::of(lines);
    if privs.tenant_admin {
        return Access {
            list: true,
            routines: true,
        };
    }
    let db = db.to_lowercase();
    let mut a = Access {
        list: false,
        routines: false,
    };
    for l in lines {
        let up = l.to_ascii_uppercase();
        let Some(rest) = up.strip_prefix("GRANT ") else {
            continue;
        };
        let Some(on_at) = rest.find(" ON ") else {
            continue;
        };
        let privs = &rest[..on_at];
        let on = &l["GRANT ".len() + on_at + 4..];
        let target = on.split(" TO ").next().unwrap_or("").trim();
        let target_db = target
            .split_once('.')
            .map_or(target, |(d, _)| d)
            .trim_matches('`')
            .replace("``", "`")
            .to_lowercase();
        let applies = target_db == "*" || target_db == db;
        if !applies {
            continue;
        }
        let usage_only = privs.trim() == "USAGE";
        if !usage_only {
            a.list = true;
        }
        if privs.contains("ALL PRIVILEGES")
            || privs.contains("ALTER ROUTINE")
            || privs.contains("CREATE ROUTINE")
        {
            a.routines = true;
            a.list = true;
        }
    }
    a
}

async fn access(core: &Arc<Core>, caller: &Caller, db: &str) -> Result<Access, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect();
    Ok(access_of(&lines, db))
}

fn denied(caller: &Caller, db: &str) -> SprigError {
    SprigError::standard(
        1044,
        *b"42000",
        format!(
            "Access denied for user '{}' to database '{db}'",
            caller.principal.user
        ),
    )
}

fn not_found(what: &str) -> SprigError {
    SprigError::new(SprigCode::LogicNotFound, format!("no {what}"))
}

/// The tenant's catalog connection, its logic tables and the database's id.
async fn store(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
) -> Result<(mysql_async::Conn, LogicStore, u32), SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("the logic views need a tenant login"));
    };
    let cat = svc
        .cache
        .cached(tid)
        .ok_or_else(|| invalid("the tenant's catalog isn't loaded yet; retry"))?;
    let row = cat
        .database(db)
        .or_else(|| cat.database(&db.to_ascii_lowercase()))
        .ok_or_else(|| SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'")))?;
    let db_id = row.db_id.0;
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::CatalogUnavailable,
            "the tenant's catalog is unreachable",
        )
        .with_source(e)
    })?;
    Ok((conn, LogicStore::new(ep.store.schema()), db_id))
}

/// A tenant without logic has no logic tables yet: an empty list, not 1146.
fn missing_tables(e: &SprigError) -> bool {
    let s = format!("{e:?}");
    s.contains("1146") || s.contains("doesn't exist")
}

fn kind_ok(kind: &str) -> Result<(), SprigError> {
    match kind {
        "function" | "procedure" | "validation" | "trigger" | "schedule" | "computed"
        | "redis_script" | "redis_library" => Ok(()),
        _ => Err(invalid(format!("'{kind}' isn't a kind of logic module"))),
    }
}

fn query_u64(query: &str, key: &str, default: u64) -> u64 {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(default)
}

fn query_str<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
        .filter(|v| !v.is_empty())
}

/// Undoes `%XX` escapes in a path segment.
fn decode(seg: &str) -> String {
    let b = seg.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = |c: u8| (c as char).to_digit(16);
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            out.push(u8::try_from(h * 16 + l).unwrap_or(b'?'));
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether `path` is one of Studio's logic routes.
#[must_use]
pub fn is_logic_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("/v1/databases/") else {
        return false;
    };
    let Some((_, tail)) = rest.split_once('/') else {
        return false;
    };
    tail == "logic" || tail == "logic:compile" || tail.starts_with("logic/")
}

/// Answers one of the routes in the module docs.
///
/// # Errors
/// As the module docs say.
pub async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let caller = core
        .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
        .await?;
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let rest = path.trim_start_matches("/v1/databases/");
    let (db_enc, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let db = decode(db_enc);
    let segs: Vec<String> = tail.split('/').map(decode).collect();
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    match (&method, segs.as_slice()) {
        (&Method::POST, ["logic:compile"]) => {
            let body = parse_json(&read_body(req).await?)?;
            compile(core, &caller, &db, &body).await
        }
        (&Method::GET, ["logic"]) => list(core, &caller, &db).await,
        (&Method::GET, ["logic", kind, name]) => show(core, &caller, &db, kind, name).await,
        (&Method::GET, ["logic", kind, name, "versions", v]) => {
            let v: u64 = v.parse().map_err(|_| invalid("a version is a number"))?;
            version(core, &caller, &db, kind, name, v).await
        }
        (&Method::GET, ["logic", kind, name, "logs"]) => {
            logs(core, &caller, &db, kind, name, &query).await
        }
        (&Method::GET, ["logic", kind, name, "deadLetters"]) => {
            dead_letters(core, &caller, &db, kind, name, &query).await
        }
        (&Method::POST, ["logic", kind, name, verb])
            if *verb == "deadLetters:replay" || *verb == "deadLetters:discard" =>
        {
            let (kind, name, verb) = ((*kind).to_owned(), (*name).to_owned(), (*verb).to_owned());
            let body = parse_json(&read_body(req).await?)?;
            settle(core, &caller, &db, &kind, &name, &verb, &body).await
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

async fn list(core: &Arc<Core>, caller: &Caller, db: &str) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.list {
        return Err(denied(caller, db));
    }
    let (mut conn, st, db_id) = store(core, caller, db).await?;
    let modules = match st.modules(&mut conn, Some(db_id)).await {
        Ok(m) => m,
        Err(e) if missing_tables(&e) => Vec::new(),
        Err(e) => return Err(e),
    };
    Ok(json_response(
        200,
        &json!({"database": db, "modules": modules, "routines": a.routines}),
    ))
}

async fn module_of(
    st: &LogicStore,
    conn: &mut mysql_async::Conn,
    db_id: u32,
    kind: &str,
    name: &str,
) -> Result<sprig_logic::catalog::ModuleRow, SprigError> {
    kind_ok(kind)?;
    match st.module(conn, db_id, kind, name).await {
        Ok(Some(m)) => Ok(m),
        Ok(None) => Err(not_found(&format!("{kind} {name}"))),
        Err(e) if missing_tables(&e) => Err(not_found(&format!("{kind} {name}"))),
        Err(e) => Err(e),
    }
}

async fn show(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.list {
        return Err(denied(caller, db));
    }
    let (mut conn, st, db_id) = store(core, caller, db).await?;
    let m = module_of(&st, &mut conn, db_id, kind, name).await?;
    let versions = st.versions(&mut conn, m.module_id).await?;
    let binding = st.binding(&mut conn, m.module_id).await.ok().flatten();
    let schedule = st.schedule(&mut conn, m.module_id).await.ok().flatten();
    let mut out = json!({
        "module": m,
        "versions": versions,
        "binding": binding,
        "schedule": schedule,
        "routines": a.routines,
    });
    if a.routines
        && let Some(v) = m
            .active_version
            .or_else(|| versions.first().map(|x| x.version))
    {
        out["source"] = json!(st.source(&mut conn, m.module_id, v).await.ok());
        out["plsql"] = json!(st.plsql(&mut conn, m.module_id, v).await.ok().flatten());
        out["source_version"] = json!(v);
    }
    Ok(json_response(200, &out))
}

async fn version(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    v: u64,
) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.routines {
        return Err(denied(caller, db));
    }
    let (mut conn, st, db_id) = store(core, caller, db).await?;
    let m = module_of(&st, &mut conn, db_id, kind, name).await?;
    let source = st.source(&mut conn, m.module_id, v).await?;
    let plsql = st.plsql(&mut conn, m.module_id, v).await?;
    Ok(json_response(
        200,
        &json!({"version": v, "source": source, "plsql": plsql}),
    ))
}

async fn logs(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.routines {
        return Err(denied(caller, db));
    }
    let (mut conn, st, db_id) = store(core, caller, db).await?;
    let m = module_of(&st, &mut conn, db_id, kind, name).await?;
    let after = query_u64(query, "after", 0);
    let limit = u32::try_from(query_u64(query, "limit", 100))
        .unwrap_or(100)
        .clamp(1, MAX_ROWS);
    let rows = match st.logs(&mut conn, Some(m.module_id), after, limit).await {
        Ok(r) => r,
        Err(e) if missing_tables(&e) => Vec::new(),
        Err(e) => return Err(e),
    };
    Ok(json_response(200, &json!({"logs": rows})))
}

async fn dead_letters(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.routines {
        return Err(denied(caller, db));
    }
    let (mut conn, st, db_id) = store(core, caller, db).await?;
    let m = module_of(&st, &mut conn, db_id, kind, name).await?;
    let Some(b) = st.binding(&mut conn, m.module_id).await? else {
        return Ok(json_response(
            200,
            &json!({"dead_letters": [], "binding": null}),
        ));
    };
    let state = query_str(query, "state");
    if let Some(s) = state
        && !matches!(s, "dead" | "replaying" | "replayed" | "discarded")
    {
        return Err(invalid(format!("'{s}' isn't a dead letter's state")));
    }
    let mut rows = st
        .dead_letters(&mut conn, Some(b.binding_id), state, MAX_ROWS)
        .await?;
    // Chapter 29 (RL39): a trigger's dead letters hold table rows (an
    // after-commit trigger's images, a change topic's records): hidden
    // from a caller the table's policies bind.
    let hidden = match table_source(core, caller, &mut conn, &b).await? {
        Some(object) => !unbound(core, caller, object).await,
        None => false,
    };
    if hidden {
        for r in &mut rows {
            r.event = Value::Null;
        }
    }
    Ok(json_response(
        200,
        &json!({"dead_letters": rows, "binding": b.binding_id, "events_hidden": hidden}),
    ))
}

/// The table whose rows binding `b`'s events carry: an after-commit
/// trigger's table, or a stream trigger's change topic's table.
async fn table_source(
    core: &Arc<Core>,
    caller: &Caller,
    conn: &mut mysql_async::Conn,
    b: &sprig_logic::catalog::BindingRow,
) -> Result<Option<u64>, SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        // Can't tell: treat it as table rows.
        return Ok(Some(b.object_id.unwrap_or(0)));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    // A stream or queue trigger's binding names its stream or queue; an
    // after-commit trigger's names its table.
    let src = match sprig_logic::messages::source(conn, ep.store.schema(), b.binding_id).await {
        Ok(s) => s,
        Err(e) if missing_tables(&e) => None,
        Err(e) => return Err(e),
    };
    let Some(src) = src else {
        return Ok(b.object_id);
    };
    if src.kind != sprig_logic::declare::SourceKind::Stream || src.fanin_declarer != 0 {
        return Ok(None);
    }
    let Some(cat) = svc.cache.cached(tid) else {
        // Can't tell: treat it as table rows.
        return Ok(Some(0));
    };
    Ok(cat
        .ext
        .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
        .and_then(|st| {
            st.all()
                .find(|s| s.id.0 == src.object_id)
                .filter(|s| s.kind == sprig_streams::model::StreamKind::Changes)
                .map(|s| s.source_object.map_or(0, |o| o.0))
        }))
}

/// Whether policies on table `object` don't bind the caller (a session of
/// its own, as search's facets ask; RL39). `false` when it can't tell.
async fn unbound(core: &Arc<Core>, caller: &Caller, object: u64) -> bool {
    use sprig_doc::engine::SqlRunner as _;
    // 0: the table isn't known.
    if object == 0 {
        return false;
    }
    let Ok(mut sql) = crate::mongo::sql::SqlSession::open(
        &core.engine.sessions,
        &core.engine.executor,
        &caller.principal,
        caller.listener,
        caller.generation,
        caller.peer,
        caller.tls.clone(),
        1 << 20,
    ) else {
        return false;
    };
    // The session's catalog is installed by its first statement.
    let ok = sql.query("SELECT 1").await.is_ok() && sql.row_policy_binds(object) == Some(false);
    sql.close().await;
    ok
}

async fn settle(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    verb: &str,
    body: &Value,
) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.routines {
        return Err(denied(caller, db));
    }
    let ids: Vec<u64> = body
        .get("ids")
        .and_then(Value::as_array)
        .map(|xs| {
            xs.iter()
                .filter_map(|x| {
                    x.as_u64()
                        .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
                })
                .collect()
        })
        .unwrap_or_default();
    let all = body.get("all").and_then(Value::as_bool).unwrap_or(false);
    if ids.is_empty() && !all {
        return Err(invalid(
            "name the dead letters (ids) or ask for all of them",
        ));
    }
    let (mut conn, st, db_id) = store(core, caller, db).await?;
    let m = module_of(&st, &mut conn, db_id, kind, name).await?;
    let Some(b) = st.binding(&mut conn, m.module_id).await? else {
        return Err(not_found(&format!("binding of {kind} {name}")));
    };
    if verb.ends_with(":replay") {
        let (marked, skipped) = st
            .replay_dead_letters(&mut conn, Some(b.binding_id), &ids)
            .await?;
        Ok(json_response(
            200,
            &json!({"replaying": marked, "skipped": skipped}),
        ))
    } else {
        let n = st
            .set_dead_letters(&mut conn, Some(b.binding_id), &ids, "dead", "discarded")
            .await?;
        Ok(json_response(200, &json!({"discarded": n})))
    }
}

/// The database's columns as the caller sees them (logical names), for the
/// compiler's static checks.
async fn columns(core: &Arc<Core>, caller: &Caller, db: &str) -> Result<StaticColumns, SprigError> {
    let r = run_sql(
        core,
        caller,
        db,
        format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {}",
            quote_literal(db)
        ),
    )
    .await?;
    let mut cols = StaticColumns::new();
    for row in rows_of(&r) {
        if let [Some(t), Some(c), Some(dt), Some(ct)] = row.as_slice()
            && let Some(ty) = sprig_logic::compiler::sql::ty_of_column(dt, ct)
        {
            cols.add(t, c, ty);
        }
    }
    Ok(cols)
}

/// A compile error as the editor's diagnostic: code, name, message.
fn diagnostic(e: &SprigError) -> Value {
    json!({
        "severity": "error",
        "code": e.protocol().mysql_errno,
        "name": e.code().map_or("", |c| c.name()),
        "message": e.message,
    })
}

/// `Logic.Compile` for one declaration (see the module docs).
async fn compile(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    body: &Value,
) -> Result<Response<Body>, SprigError> {
    let a = access(core, caller, db).await?;
    if !a.list {
        return Err(denied(caller, db));
    }
    let sql = body.get("sql").and_then(Value::as_str).unwrap_or("");
    if sql.trim().is_empty() {
        return Err(invalid(
            "sql holds the CREATE FUNCTION or PROCEDURE to compile",
        ));
    }
    let decl = match sprig_logic::declare::parse(sql) {
        Ok(d) => d,
        Err(e) => {
            return Ok(json_response(
                200,
                &json!({"ok": false, "diagnostics": [diagnostic(&e)]}),
            ));
        }
    };
    if !matches!(decl.kind, Kind::Function | Kind::Procedure) || decl.source.is_some() {
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            "a dry run compiles functions and procedures; deploy validations, triggers and schedules to see their placement",
        ));
    }
    let cols = columns(core, caller, decl.db.as_deref().unwrap_or(db)).await?;
    Ok(json_response(200, &dry_run(&decl, &cols, a.routines).await))
}

/// Compiles and places `d` as a deploy would, without recording anything.
pub async fn dry_run(d: &Declaration, cols: &StaticColumns, show_plsql: bool) -> Value {
    let opts = Options {
        module_id: 0,
        version: 0,
        max_loop_iterations: MAX_LOOP_ITERATIONS,
        compile_max_cost: COMPILE_MAX_COST,
        definer_security: d.definer_security,
    };
    let compiled = if d.runtime == Runtime::Javascript {
        sprig_logic::compiler::js::compile_with(
            &d.name,
            &d.body,
            &sprig_logic::service::js_signature(d),
            cols,
            &opts,
        )
    } else {
        sprig_logic::compiler::compile(d.runtime, &d.body, cols, &opts)
    };
    let mut compiled = match compiled {
        Ok(c) => c,
        Err(e) => return json!({"ok": false, "diagnostics": [diagnostic(&e)]}),
    };
    let script = matches!(d.runtime, Runtime::Javascript | Runtime::Lua);
    if script && !compiled.compilable() {
        compiled.ir = Some(sprig_logic::service::script_function(d));
    }
    // A dry run doesn't know the shard layout: placed as for one shard,
    // which only changes the route, never whether the code compiles.
    let placed = match sprig_logic::placement::place(&compiled, d.tier, true) {
        Ok(p) => p,
        Err(e) => return json!({"ok": false, "diagnostics": [diagnostic(&e)]}),
    };
    let engine = placed.tier == sprig_logic::placement::Tier::Engine;
    let mut diagnostics = Vec::new();
    if engine
        && d.runtime == Runtime::Kirun
        && let Err(e) = sprig_logic::service::engine_check(d).await
    {
        diagnostics.push(diagnostic(&e));
    }
    if engine
        && script
        && let Err(e) = sprig_logic::service::script_check(d)
    {
        diagnostics.push(diagnostic(&e));
    }
    let signature = compiled.ir.as_ref().map_or(Value::Null, |f| {
        json!({
            "params": f.params.iter().map(|p| json!({"name": p.name, "type": p.ty.name()})).collect::<Vec<_>>(),
            "events": f.events.iter().map(|e| json!({
                "name": e.name,
                "fields": e.fields.iter().map(|(n, t)| json!({"name": n, "type": t.name()})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    });
    let plsql = if show_plsql {
        compiled
            .plsql
            .as_ref()
            .map_or(Value::Null, |p| Value::String(p.statements.join(";\n\n")))
    } else {
        Value::Null
    };
    json!({
        "ok": diagnostics.is_empty(),
        "kind": d.kind.as_str(),
        "name": d.name,
        "runtime": d.runtime.as_str(),
        "tier": placed.tier.as_str(),
        "placement": serde_json::to_value(&placed).unwrap_or(Value::Null),
        "signature": signature,
        "plsql": plsql,
        "plsql_hidden": !show_plsql && compiled.plsql.is_some(),
        "diagnostics": diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn access_follows_the_grants() {
        let none = lines(&["GRANT USAGE ON *.* TO `bob`@`%`"]);
        assert_eq!(
            access_of(&none, "shop"),
            Access {
                list: false,
                routines: false
            }
        );
        let reader = lines(&[
            "GRANT USAGE ON *.* TO `bob`@`%`",
            "GRANT SELECT ON `shop`.* TO `bob`@`%`",
        ]);
        assert_eq!(
            access_of(&reader, "shop"),
            Access {
                list: true,
                routines: false
            }
        );
        assert!(!access_of(&reader, "other").list);
        let dev = lines(&["GRANT SELECT, ALTER ROUTINE ON `Shop`.* TO `bob`@`%`"]);
        assert!(access_of(&dev, "shop").routines);
        let all = lines(&["GRANT ALL PRIVILEGES ON *.* TO `bob`@`%`"]);
        assert!(access_of(&all, "anything").routines);
        let admin = lines(&["GRANT SPRIG_TENANT_ADMIN ON *.* TO `bob`@`%`"]);
        assert!(access_of(&admin, "x").routines);
        // A table-level grant lists the modules, but shows no source.
        let table = lines(&["GRANT SELECT ON `shop`.`orders` TO `bob`@`%`"]);
        assert_eq!(
            access_of(&table, "shop"),
            Access {
                list: true,
                routines: false
            }
        );
    }

    #[test]
    fn logic_paths() {
        assert!(is_logic_path("/v1/databases/shop/logic"));
        assert!(is_logic_path("/v1/databases/shop/logic:compile"));
        assert!(is_logic_path("/v1/databases/shop/logic/function/f/logs"));
        assert!(!is_logic_path("/v1/databases/shop/sql:query"));
        assert!(!is_logic_path("/v1/databases/shop/logical"));
    }

    #[tokio::test]
    async fn a_dry_run_places_and_shows_reasons_without_recording() {
        let mut cols = StaticColumns::new();
        cols.add("orders", "id", sprig_logic::compiler::ir::Ty::Int);
        // A JavaScript function the compiled tier can't take (a string
        // method the subset lacks): engine tier, with a reason and where.
        let d = sprig_logic::declare::parse(
            "CREATE FUNCTION shout(s VARCHAR(20)) RETURNS VARCHAR(40) LANGUAGE javascript AS $$ return s.toUpperCase().padEnd(30, '!'); $$",
        )
        .unwrap();
        let v = dry_run(&d, &cols, true).await;
        assert_eq!(v["kind"], "function");
        assert_eq!(v["runtime"], "javascript");
        assert!(v["tier"] == "engine" || v["tier"] == "mariadb", "{v}");
        if v["tier"] == "engine" {
            assert!(
                !v["placement"]["reasons"].as_array().unwrap().is_empty(),
                "{v}"
            );
        }
        // Not shown without the routine privilege.
        let hidden = dry_run(&d, &cols, false).await;
        assert!(hidden["plsql"].is_null());
        // A KIRun definition that isn't JSON is a diagnostic, not a 500.
        let bad = sprig_logic::declare::parse("CREATE PROCEDURE p LANGUAGE kirun AS '{not json'");
        if let Ok(bad) = bad {
            let v = dry_run(&bad, &cols, true).await;
            assert_eq!(v["ok"], false);
            assert!(v["diagnostics"][0]["code"].as_u64().unwrap() > 0, "{v}");
        }
    }
}
