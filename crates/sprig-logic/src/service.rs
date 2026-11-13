//! A deploy, end to end (chapter 41, "Versions, rollout and rollback",
//! first step): migrate the catalog module, find or create the module,
//! compile against the shard schema's columns, place, record the version,
//! deploy it, activate it.
//!
//! As built there is no rollout: a deploy that succeeds is active at once
//! (`candidate_pct` stays 0), and the differential activation gate is the
//! offline suite in `sprig-logic-compile`. Earlier versions' routines stay
//! in the shard schema; nothing contracts them yet.

use mysql_async::Conn;
use serde::Serialize;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{Migrator, Store};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::{Options, Reason, StaticColumns, compile, sql};

use crate::catalog::{LOGIC_MODULE, LogicStore, ModuleRow, NewVersion};
use crate::declare::Declaration;
use crate::deploy::{self, Session, WrapperSpec};
use crate::placement::{self, Placement, Tier};

/// Where a declaration deploys.
pub struct Target<'a> {
    /// The tenant's catalog writer.
    pub catalog: &'a mut Conn,
    /// `t<tenant>_sprig`.
    pub catalog_schema: &'a str,
    /// The catalog version the session saw.
    pub catalog_version: u64,
    /// The database's id.
    pub db_id: u32,
    /// The database's shard schema (its only shard).
    pub shard_schema: &'a str,
    /// Whether the database's group is always one shard.
    pub single_shard: bool,
    /// The deploying user.
    pub user_id: u32,
    /// `logic.function_budget_ms`.
    pub budget_ms: u64,
    /// `logic.max_loop_iterations`.
    pub max_loop_iterations: u64,
    /// `logic.compile_max_cost`.
    pub compile_max_cost: u64,
}

/// What a deploy did.
#[derive(Debug, Clone, Serialize)]
pub struct Outcome {
    /// The module.
    pub module_id: u64,
    /// The new version.
    pub version: u64,
    /// Where it runs, and why.
    pub placement: Placement,
    /// The entry routine (compiled tier).
    pub entry: Option<String>,
    /// The account the routines run as.
    pub definer: String,
    /// `SELECT db.f(args)`'s stored function (functions only; see
    /// [`deploy::select_function`]), for the gateway to create through
    /// `sprig_ctl` with the deploying user as `DEFINER`.
    pub select_fn: Option<String>,
    /// The version is a staged rollout's candidate (R7 follow-ups): the
    /// active version keeps its `SELECT` function.
    pub staged: Option<crate::rollout::RolloutSpec>,
    /// Notes about the rollout, for the client.
    pub rollout_notes: Vec<String>,
}

impl Outcome {
    /// Notes for the client (`SHOW WARNINGS`): the tier and each reason.
    #[must_use]
    pub fn notes(&self) -> Vec<String> {
        let mut v = vec![format!(
            "module {} version {}: {}",
            self.module_id,
            self.version,
            placement::explain(&self.placement).replace('\n', ";")
        )];
        v.extend(self.placement.reasons.iter().map(Reason::to_string));
        v.extend(self.rollout_notes.iter().cloned());
        v
    }
}

/// The shard schema's columns as `session` sees them.
///
/// # Errors
/// The server's error.
pub async fn columns(session: &mut dyn Session, schema: &str) -> Result<StaticColumns> {
    let rows = session
        .rows(&format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {}",
            sprig_logic_compile::plsql::quote(schema)
        ))
        .await?;
    let mut cols = StaticColumns::new();
    for r in rows {
        if let [Some(t), Some(c), Some(dt), Some(ct)] = r.as_slice()
            && let Some(ty) = sql::ty_of_column(dt, ct)
        {
            cols.add(t, c, ty);
        }
    }
    Ok(cols)
}

/// What the engine tier must be able to run before a KIRun version is
/// placed there (chapter 41, "Placement": a definition calling a function
/// the `kirun` crate lacks is refused).
///
/// # Errors
/// 39861 when KIRun can't read the definition; 39862 for functions the
/// runtime doesn't have (or refuses, such as `System.Wait`), and for
/// `SQL SECURITY DEFINER`, which the engine tier can't honour yet.
pub async fn engine_check(d: &Declaration) -> Result<()> {
    let p = sprig_logic_js::kirun::Prepared::new(&d.body)?;
    let missing = p.missing_functions().await;
    if !missing.is_empty() {
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "{} calls functions no tier can run: {}",
                d.name,
                missing.join(", ")
            ),
        ));
    }
    if d.definer_security {
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "{} runs in the engine tier, which can't run SQL SECURITY DEFINER code yet; \
                 deploy it SQL SECURITY INVOKER",
                d.name
            ),
        ));
    }
    Ok(())
}

/// The signature-only IR of a JavaScript or Lua function: its declared
/// parameters, and `output {value}` when it declares `RETURNS`. The
/// wrapper procedure and the engine-tier call read only these.
#[must_use]
pub fn script_function(d: &Declaration) -> sprig_logic_compile::Function {
    use crate::declare::Param;
    use sprig_logic_compile::ir::{EventDef, Param as IrParam};
    sprig_logic_compile::Function {
        name: d.name.clone(),
        params: d
            .params
            .iter()
            .map(|p| IrParam {
                name: p.name.clone(),
                ty: Param::ir_ty(&p.ty),
                min: None,
                max: None,
            })
            .collect(),
        events: d
            .returns
            .as_deref()
            .map(|r| {
                vec![EventDef {
                    name: "output".to_owned(),
                    fields: vec![("value".to_owned(), Param::ir_ty(r))],
                }]
            })
            .unwrap_or_default(),
        locals: Vec::new(),
        body: Vec::new(),
    }
}

/// The engine's signature of a JavaScript or Lua declaration.
#[must_use]
pub fn script_sig(d: &Declaration) -> sprig_logic_js::ScriptSig {
    sprig_logic_js::ScriptSig {
        params: d
            .params
            .iter()
            .map(|p| {
                (
                    p.name.clone(),
                    param_ty(crate::declare::Param::ir_ty(&p.ty)),
                )
            })
            .collect(),
        scalar: d.returns.is_some(),
    }
}

/// The JavaScript front end's signature of a declaration.
#[must_use]
pub fn js_signature(d: &Declaration) -> sprig_logic_compile::js::Signature {
    sprig_logic_compile::js::Signature {
        params: d
            .params
            .iter()
            .map(|p| (p.name.clone(), crate::declare::Param::ir_ty(&p.ty)))
            .collect(),
        returns: d.returns.as_deref().map(crate::declare::Param::ir_ty),
        row: None,
    }
}

/// The JavaScript front end's signature of a recorded version: its
/// `logic_versions.signature` (the declared SQL types in `params[].sql`
/// and `returns`, else the IR type names).
#[must_use]
pub fn js_signature_of(sig: &serde_json::Value) -> sprig_logic_compile::js::Signature {
    use sprig_logic_compile::ir::Ty;
    let ty_name = |n: &str| match n {
        "i64" => Ty::Int,
        "f64" => Ty::Float,
        "bool" => Ty::Bool,
        "json" => Ty::Json,
        _ => Ty::Str,
    };
    sprig_logic_compile::js::Signature {
        params: sig["params"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| {
                let name = p["name"].as_str().unwrap_or_default().to_owned();
                let ty = p["sql"].as_str().map_or_else(
                    || ty_name(p["type"].as_str().unwrap_or("")),
                    crate::declare::Param::ir_ty,
                );
                (name, ty)
            })
            .collect(),
        returns: sig["returns"]
            .as_str()
            .map(crate::declare::Param::ir_ty)
            .or_else(|| {
                // A version recorded before `returns` was: `scalar` and the
                // output's one field.
                (sig["scalar"].as_bool() == Some(true)).then(|| {
                    sig["events"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|e| e["name"] == "output")
                        .and_then(|e| e["fields"][0]["type"].as_str())
                        .map_or(Ty::Json, ty_name)
                })
            }),
        row: None,
    }
}

/// Compiles a recorded version of a function in its own language: KIRun
/// from its definition, JavaScript with the version's signature (the
/// front end needs the declared parameter types), Lua and SQL to their
/// reasons. Computed columns compile the active version of the function
/// they call this way.
///
/// # Errors
/// As [`compile`] and [`sprig_logic_compile::js::compile_with`].
pub fn compile_version(
    runtime: sprig_logic_compile::Runtime,
    name: &str,
    source: &str,
    signature: &serde_json::Value,
    cat: &dyn sprig_logic_compile::Columns,
    opts: &Options,
) -> Result<sprig_logic_compile::Compiled> {
    if runtime == sprig_logic_compile::Runtime::Javascript {
        sprig_logic_compile::js::compile_with(name, source, &js_signature_of(signature), cat, opts)
    } else {
        compile(runtime, source, cat, opts)
    }
}

/// A computed column's generation expression (R7 order 3; the gateway
/// puts it in place of the call with [`enforce::rewrite_computed`]): the active
/// version of function `name` (its `runtime` text, `source` and recorded
/// `signature`) compiled, then written as one expression over the call's
/// arguments ([`enforce::computed_expr`]).
///
/// Only code that compiles with no reason qualifies: a version with
/// reasons has placeholder parts, and the routine and engine-tier forms
/// of a computed column aren't built. Among the reasons is
/// `js.nondeterministic` (`Math.random()`, `Date.now()`, `new Date()`)
/// and KIRun's random steps: a generated column, stored or virtual, must
/// give the same value for the same row on every shard and replica.
///
/// # Errors
/// 39862 for code with no expression form; 39874 for the wrong number of
/// arguments; 39861 for a definition that doesn't parse.
pub fn computed_column(
    name: &str,
    runtime: &str,
    source: &str,
    signature: &serde_json::Value,
    call: &enforce::ComputedCall,
) -> Result<String> {
    let runtime =
        sprig_logic_compile::Runtime::parse(runtime).unwrap_or(sprig_logic_compile::Runtime::Kirun);
    let compiled = compile_version(
        runtime,
        name,
        source,
        signature,
        &StaticColumns::new(),
        &Options::default(),
    )?;
    let (Some(f), true) = (&compiled.ir, compiled.analysis.reasons.is_empty()) else {
        let why: Vec<String> = compiled
            .analysis
            .reasons
            .iter()
            .map(ToString::to_string)
            .collect();
        return Err(unsupported(format!(
            "{name} can't make computed column {}: a computed column needs code that compiles \
             into one SQL expression, and {name} doesn't compile into MariaDB ({}); computed \
             columns through the engine tier aren't built",
            call.column,
            if why.is_empty() {
                "no compiled form".to_owned()
            } else {
                why.join("; ")
            }
        )));
    };
    enforce::computed_expr(f, call)
}

/// Compiles a validation's or after-commit trigger's body against `table`
/// (JavaScript binds the row by name, see
/// [`sprig_logic_compile::js::compile_with`]; KIRun's parameters bind by
/// name too, decision V2).
///
/// # Errors
/// As [`compile`].
pub fn compile_bound(
    d: &Declaration,
    table: &str,
    cat: &dyn sprig_logic_compile::Columns,
    opts: &Options,
) -> Result<sprig_logic_compile::Compiled> {
    if d.runtime == sprig_logic_compile::Runtime::Javascript {
        sprig_logic_compile::js::compile_with(
            &d.name,
            &d.body,
            &sprig_logic_compile::js::Signature::row_of(table),
            cat,
            opts,
        )
    } else {
        compile(d.runtime, &d.body, cat, opts)
    }
}

/// The engine's parameter type for an IR type.
#[must_use]
pub fn param_ty(t: sprig_logic_compile::ir::Ty) -> sprig_logic_js::ParamTy {
    use sprig_logic_compile::ir::Ty;
    use sprig_logic_js::ParamTy;
    match t {
        Ty::Int => ParamTy::Int,
        Ty::Float => ParamTy::Float,
        Ty::Bool => ParamTy::Bool,
        Ty::Json => ParamTy::Json,
        Ty::Str | Ty::Null => ParamTy::Str,
    }
}

/// What a JavaScript or Lua version must pass before it is recorded: its
/// source compiles in its runtime, and it isn't `SQL SECURITY DEFINER`
/// (the engine tier runs with the caller's session only, as for KIRun).
///
/// # Errors
/// 39861 for source that doesn't compile; 39862 for `DEFINER`.
pub fn script_check(d: &Declaration) -> Result<()> {
    sprig_logic_js::Program::new(d.runtime.as_str(), &d.name, &d.body, script_sig(d))?;
    if d.definer_security {
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "{} runs in the engine tier, which can't run SQL SECURITY DEFINER code yet; \
                 deploy it SQL SECURITY INVOKER",
                d.name
            ),
        ));
    }
    Ok(())
}

/// Who creates a version's routines when it isn't the deploying session:
/// a `sprig_ctl` connection to the shard, and the deploying user's backend
/// account as `DEFINER` (checked afterwards, 39367).
pub struct Creator<'a> {
    /// The connection.
    pub session: &'a mut dyn Session,
    /// `` `t1_u7`@`%` ``.
    pub definer: String,
}

/// Deploys `d` (see the module docs). `session` is the deploying user's
/// (columns are read as the user sees them); `creator`, when given,
/// creates the routines instead of it.
///
/// # Errors
/// 39861 or 39874 from the compiler; 39868 from placement; 1304 for an
/// existing name without `OR REPLACE`; 39869 for a concurrent deploy;
/// catalog errors; the server's error from creating a routine; 39367.
pub async fn deploy(
    d: &Declaration,
    t: Target<'_>,
    session: &mut dyn Session,
    creator: Option<Creator<'_>>,
) -> Result<Outcome> {
    deploy_at(d, t, session, creator, At::default()).await
}

/// Computed columns made from function `prev` (R7 order 3) hold a copy of
/// its body as their generation expression, made for its signature. A new
/// version `f` that takes other parameter types, or returns another type,
/// would no longer fit the calls in those columns: 39875 naming them. With
/// the same signature the columns keep the expression they were made from,
/// and the notes say so.
///
/// # Errors
/// 39875; the catalog's.
async fn computed_dependents(
    c: &mut Conn,
    store: &LogicStore,
    prev: &ModuleRow,
    f: &sprig_logic_compile::Function,
    cols: &StaticColumns,
) -> Result<Vec<String>> {
    let Some(active) = prev.active_version else {
        return Ok(Vec::new());
    };
    // Only columns that still exist: a dropped column leaves its binding.
    let columns: Vec<String> = crate::drop::computed_columns(c, store, prev.module_id)
        .await?
        .into_iter()
        .map(|(_, n)| n)
        .filter(|n| match n.split_once('.') {
            Some(("?", _)) | None => true,
            Some((t, col)) => sprig_logic_compile::Columns::column(cols, t, col).is_some(),
        })
        .collect();
    if columns.is_empty() {
        return Ok(Vec::new());
    }
    let (sig, _) = store.version_signature(c, prev.module_id, active).await?;
    let old = crate::rollout::function_of(&prev.name, &sig);
    let shape = |g: &sprig_logic_compile::Function| {
        (
            g.params.iter().map(|p| p.ty).collect::<Vec<_>>(),
            g.event("output")
                .map(|e| e.fields.iter().map(|(_, t)| *t).collect::<Vec<_>>()),
        )
    };
    let (op, oo) = shape(&old);
    let (np, no) = shape(f);
    if op != np || oo != no {
        let show = |ps: &[sprig_logic_compile::ir::Ty]| {
            ps.iter().map(|t| t.name()).collect::<Vec<_>>().join(", ")
        };
        return Err(SprigError::new(
            SprigCode::LogicInUse,
            format!(
                "{} is used by computed columns {}, made for its signature ({}) of version \
                 {active}; this version takes ({}){}. Drop or re-create those columns first, \
                 or keep the signature",
                prev.name,
                columns.join(", "),
                show(&op),
                show(&np),
                if oo == no {
                    ""
                } else {
                    " and returns another type"
                },
            ),
        ));
    }
    Ok(columns
        .iter()
        .map(|c| {
            format!(
                "computed column {c} keeps the expression of version {active}: a generated \
                 column holds its own copy; re-create it to use this version"
            )
        })
        .collect())
}

/// Where and as what [`deploy_at`] deploys (R7 close-out).
#[derive(Default)]
pub struct At<'a> {
    /// Re-deploy this existing version (a rollback) instead of adding
    /// version n+1.
    pub version: Option<u64>,
    /// Every shard of a sharded database: the routines go on each, all or
    /// nothing (chapter 21's rule), with the creator's definer.
    pub shards: Option<&'a mut dyn ShardSessions>,
    /// The tenant, for a staged rollout `BY TENANT` (decided at deploy).
    pub tenant: u64,
}

/// [`deploy`] at a fixed version and on several shards.
///
/// # Errors
/// As [`deploy`].
#[allow(clippy::too_many_lines)]
pub async fn deploy_at(
    d: &Declaration,
    t: Target<'_>,
    session: &mut dyn Session,
    creator: Option<Creator<'_>>,
    at: At<'_>,
) -> Result<Outcome> {
    Migrator::new(&[LOGIC_MODULE])?
        .run(t.catalog, t.catalog_schema, &MigrateOpts::default())
        .await?;
    let store = LogicStore::new(t.catalog_schema);
    let kind = d.kind.as_str();
    let existing = store.module(t.catalog, t.db_id, kind, &d.name).await?;
    if existing.is_some() && !d.or_replace {
        return Err(SprigError::standard(
            1304,
            *b"42000",
            format!("{} {} already exists", kind.to_ascii_uppercase(), d.name),
        ));
    }
    let previous = existing.clone();
    let module = match existing {
        Some(mut m) => {
            m.runtime = d.runtime.as_str().to_owned();
            m.security = if d.definer_security {
                "definer"
            } else {
                "invoker"
            }
            .to_owned();
            m.tier_pref = d.tier.as_str().to_owned();
            m
        }
        None => ModuleRow {
            module_id: Store::new(t.catalog_schema)
                .next_object_id(t.catalog)
                .await?
                .0,
            db_id: t.db_id,
            name: d.name.clone(),
            kind: kind.to_owned(),
            runtime: d.runtime.as_str().to_owned(),
            security: if d.definer_security {
                "definer"
            } else {
                "invoker"
            }
            .to_owned(),
            tier_pref: d.tier.as_str().to_owned(),
            active_version: None,
            state: "active".to_owned(),
            created_by: t.user_id,
        },
    };
    let version = match at.version {
        Some(v) => v,
        None => store.next_version(t.catalog, module.module_id).await?,
    };
    let cols = columns(session, t.shard_schema).await?;
    let opts = Options {
        module_id: module.module_id,
        version,
        max_loop_iterations: t.max_loop_iterations,
        compile_max_cost: t.compile_max_cost,
        definer_security: d.definer_security,
    };
    let mut compiled = if d.runtime == sprig_logic_compile::Runtime::Javascript {
        // The JavaScript front end (R7 close-out): the declaration gives
        // the parameters' types and `RETURNS`.
        sprig_logic_compile::js::compile_with(&d.name, &d.body, &js_signature(d), &cols, &opts)?
    } else {
        compile(d.runtime, &d.body, &cols, &opts)?
    };
    let script = matches!(
        d.runtime,
        sprig_logic_compile::Runtime::Javascript | sprig_logic_compile::Runtime::Lua
    );
    if script && !compiled.compilable() {
        // The engine tier: the declaration gives the signature
        // (parameters, `RETURNS`); a compiled version's comes from its
        // source (every event it can raise).
        compiled.ir = Some(script_function(d));
    }
    let placed = placement::place(&compiled, d.tier, t.single_shard)?;
    if placed.tier == Tier::Engine && d.runtime == sprig_logic_compile::Runtime::Kirun {
        engine_check(d).await?;
    }
    if script && placed.tier == Tier::Engine {
        script_check(d)?;
    } else if script {
        // Compiled: QuickJS must still accept it (the engine tier defines
        // the language); `SQL SECURITY DEFINER` is fine in MariaDB.
        sprig_logic_js::Program::new(d.runtime.as_str(), &d.name, &d.body, script_sig(d))?;
    }
    let computed_notes = match (&previous, &compiled.ir) {
        (Some(prev), Some(f)) if d.kind == Kind::Function => {
            computed_dependents(t.catalog, &store, prev, f, &cols).await?
        }
        _ => Vec::new(),
    };
    // A staged rollout (R7 follow-ups): the active version stays for most
    // callers; it must take the same parameters, in the same language,
    // with the same rights, so one wrapper can hold both.
    let mut rollout_notes = computed_notes;
    let stage = match (d.rollout, &previous) {
        (Some(spec), Some(prev))
            if matches!(d.kind, Kind::Function | Kind::Procedure)
                && prev.active_version.is_some_and(|a| a != version) =>
        {
            let active = prev.active_version.unwrap_or_default();
            let blocked = |why: String| {
                SprigError::new(
                    SprigCode::LogicRolloutBlocked,
                    format!(
                        "{}: a staged rollout {why}; deploy without SPRIG_ROLLOUT to switch at once",
                        d.name
                    ),
                )
            };
            if prev.runtime != d.runtime.as_str() {
                return Err(blocked(format!(
                    "keeps the language ({} now)",
                    prev.runtime
                )));
            }
            if (prev.security == "definer") != d.definer_security {
                return Err(blocked(format!(
                    "keeps SQL SECURITY ({} now)",
                    prev.security.to_ascii_uppercase()
                )));
            }
            let versions = store.versions(t.catalog, module.module_id).await?;
            let av = versions
                .iter()
                .find(|v| v.version == active)
                .cloned()
                .ok_or_else(|| blocked(format!("needs the active version {active}")))?;
            Some((spec, av))
        }
        (Some(_), _) => {
            rollout_notes.push(format!(
                "SPRIG_ROLLOUT ignored: {} has no other active version, so this one is active at once",
                d.name
            ));
            None
        }
        (None, _) => None,
    };
    if store
        .module(t.catalog, t.db_id, kind, &d.name)
        .await?
        .is_none()
    {
        store
            .create_module(t.catalog, &module, t.catalog_version)
            .await?;
    } else {
        store
            .update_options(t.catalog, &module, t.catalog_version)
            .await?;
    }
    let mut signature = compiled.ir.as_ref().map_or(serde_json::Value::Null, |f| {
        serde_json::json!({
            "params": f.params.iter().map(|p| serde_json::json!({"name": p.name, "type": p.ty.name()})).collect::<Vec<_>>(),
            "events": f.events.iter().map(|e| serde_json::json!({
                "name": e.name,
                "fields": e.fields.iter().map(|(n, t)| serde_json::json!({"name": n, "type": t.name()})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    });
    if script {
        // The declared SQL types, so a rollback rebuilds the same wrapper.
        if let Some(ps) = signature["params"].as_array_mut() {
            for (p, dp) in ps.iter_mut().zip(&d.params) {
                p["sql"] = serde_json::Value::String(dp.ty.clone());
            }
        }
        signature["scalar"] = serde_json::Value::Bool(d.returns.is_some());
        if let Some(r) = &d.returns {
            signature["returns"] = serde_json::Value::String(r.clone());
        }
    }
    // R9 (r9fwd): `DETERMINISTIC` is recorded with the version, so a sink
    // with `EXACTLY ONCE` can check its `TRANSFORM` (chapter 51).
    if d.deterministic {
        if !signature.is_object() {
            signature = serde_json::json!({});
        }
        signature["deterministic"] = serde_json::Value::Bool(true);
    }
    let plsql_text = compiled.plsql.as_ref().map(|p| p.statements.join(";\n\n"));
    if at.version.is_none() {
        store
            .add_version(
                t.catalog,
                module.module_id,
                version,
                &NewVersion {
                    source: &d.body,
                    signature,
                    catalog_version: t.catalog_version,
                    tier: placed.tier.as_str(),
                    placement: serde_json::to_value(&placed).unwrap_or_default(),
                    plsql: plsql_text,
                    created_by: t.user_id,
                },
            )
            .await?;
    }
    let mut select_fn = None;
    let plan = match (&compiled.ir, placed.tier, &compiled.plsql) {
        (Some(f), tier, p) => {
            let w = WrapperSpec {
                name: &d.name,
                func: f,
                definer_security: d.definer_security,
                budget_ms: t.budget_ms,
                module_id: module.module_id,
                version,
            };
            if d.kind == crate::declare::Kind::Function {
                let entry = match (tier, p) {
                    (Tier::MariaDb, Some(p)) => Some(p.entry.as_str()),
                    _ => None,
                };
                select_fn = deploy::select_function(
                    &w,
                    t.shard_schema,
                    entry,
                    !compiled.analysis.writes.is_empty(),
                );
            }
            let mut plan = match (tier, p) {
                (Tier::MariaDb, Some(p)) => deploy::plan_compiled(t.shard_schema, p, &w),
                _ => deploy::plan_engine(t.shard_schema, &w),
            };
            if let Some((spec, av)) = &stage {
                // The wrapper under the logical name is the last routine
                // of both plans: it becomes the rollout's.
                let active_f = crate::rollout::function_of(&d.name, &av.signature);
                if !crate::rollout::same_params(&active_f, f) {
                    return Err(SprigError::new(
                        SprigCode::LogicRolloutBlocked,
                        format!(
                            "{}: a staged rollout keeps the parameters (version {} takes ({})); \
                             deploy without SPRIG_ROLLOUT to switch at once",
                            d.name,
                            av.version,
                            active_f
                                .params
                                .iter()
                                .map(|p| format!("{} {}", p.name, p.ty.name()))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    ));
                }
                let active_side = deploy::Side {
                    spec: WrapperSpec {
                        name: &d.name,
                        func: &active_f,
                        definer_security: d.definer_security,
                        budget_ms: t.budget_ms,
                        module_id: module.module_id,
                        version: av.version,
                    },
                    entry: (av.tier == "mariadb")
                        .then(|| format!("__sprig_l{}_v{}", module.module_id, av.version)),
                };
                let cand_side = deploy::Side {
                    spec: w.clone(),
                    entry: match (tier, p) {
                        (Tier::MariaDb, Some(p)) => Some(p.entry.clone()),
                        _ => None,
                    },
                };
                let sql = match spec.by {
                    crate::rollout::RolloutBy::User => deploy::rollout_wrapper(
                        t.shard_schema,
                        &active_side,
                        &cand_side,
                        &crate::rollout::user_condition(module.module_id, spec.pct),
                        spec.pct,
                    ),
                    // Decided here: one tenant, one pick.
                    crate::rollout::RolloutBy::Tenant => {
                        let cand =
                            crate::rollout::picks_candidate(*spec, "", at.tenant, module.module_id);
                        rollout_notes.push(format!(
                            "this tenant gets version {} (rollout by tenant at {}%)",
                            if cand { version } else { av.version },
                            spec.pct
                        ));
                        deploy::rollout_wrapper(
                            t.shard_schema,
                            &active_side,
                            &cand_side,
                            if cand { "TRUE" } else { "FALSE" },
                            spec.pct,
                        )
                    }
                };
                if let Some(last) = plan.routines.last_mut() {
                    last.sql = sql;
                }
                select_fn = None;
            }
            Some(plan)
        }
        (None, _, _) => None,
    };
    let plan = match (plan, &creator) {
        (Some(mut p), Some(c)) => {
            p.definer = Some(c.definer.clone());
            Some(p)
        }
        (p, _) => p,
    };
    let target: &mut dyn Session = match creator {
        Some(c) => c.session,
        None => session,
    };
    let definer = match (&plan, at.shards) {
        (Some(plan), Some(shards)) => match deploy::deploy_shards(shards, plan).await {
            Ok(d) => d,
            Err(e) => {
                if at.version.is_none() {
                    let _ = store
                        .set_version_state(t.catalog, module.module_id, version, "failed")
                        .await;
                }
                return Err(e);
            }
        },
        (Some(plan), None) => match deploy::deploy(target, plan).await {
            Ok(d) => d,
            Err(e) => {
                if at.version.is_none() {
                    let _ = store
                        .set_version_state(t.catalog, module.module_id, version, "failed")
                        .await;
                }
                return Err(e);
            }
        },
        (None, _) => String::new(),
    };
    let staged = stage.as_ref().map(|(spec, _)| *spec);
    if let Some((spec, av)) = &stage {
        crate::rollout::put(
            t.catalog,
            t.catalog_schema,
            module.module_id,
            av.version,
            version,
            *spec,
        )
        .await?;
        rollout_notes.push(format!(
            "version {version} is the candidate for {}% of callers (by {}); version {} answers the rest. \
             ALTER {} {} ROLLOUT TO n | PROMOTE | ABORT",
            spec.pct,
            spec.by.as_str(),
            av.version,
            kind.to_ascii_uppercase(),
            d.name
        ));
    } else {
        store
            .activate(t.catalog, module.module_id, version, t.catalog_version)
            .await?;
        if matches!(d.kind, Kind::Function | Kind::Procedure)
            && let Some(r) =
                crate::rollout::clear(t.catalog, t.catalog_schema, module.module_id).await?
        {
            rollout_notes.push(format!(
                "the rollout of version {} ended; version {version} is active",
                r.candidate_version
            ));
        }
    }
    store
        .set_exposure(t.catalog, module.module_id, &d.exposure)
        .await?;
    tracing::info!(
        module = module.module_id,
        version,
        tier = placed.tier.as_str(),
        name = %d.name,
        staged = staged.map(|s| s.pct),
        "logic version deployed"
    );
    Ok(Outcome {
        module_id: module.module_id,
        version,
        entry: compiled.plsql.as_ref().map(|p| p.entry.clone()),
        placement: placed,
        definer,
        select_fn,
        staged,
        rollout_notes,
    })
}

// ---- validations, after-commit triggers and schedules (R7 orders 3, 5) ----

use crate::catalog::{BindingRow, ScheduleRow};
use crate::declare::Kind;
use crate::deploy::{Plan, Routine, ShardSessions};
use crate::enforce::{self, Enforcement};

/// What a bound deploy needs besides [`Target`].
pub struct Bind<'a> {
    /// The database's name (for the `ALTER TABLE` text).
    pub db_name: &'a str,
    /// Validations and triggers: the table's name and `objects.object_id`.
    pub table: Option<(&'a str, u64)>,
    /// The deploying user's backend account, `` `t1_u7`@`%` ``: every
    /// routine and trigger is created through `sprig_ctl` with it as
    /// `DEFINER` (validation triggers need `SUPER` with the binary log on).
    pub definer: String,
    /// Now, Unix ms (schedules).
    pub now_ms: u64,
}

/// A bound deploy whose routines are on every shard, waiting for the
/// gateway to run [`Pending::alter`] through chapter 21's `ALTER TABLE`
/// path, then [`finish`] (or [`abort`]).
#[derive(Debug, Clone)]
pub struct Pending {
    /// The module.
    pub module_id: u64,
    /// The new version.
    pub version: u64,
    /// The kind.
    pub kind: Kind,
    /// The binding (validations, triggers).
    pub binding: Option<BindingRow>,
    /// Placement.
    pub placement: Placement,
    /// Enforcement notes for the client.
    pub notes: Vec<String>,
    /// `ALTER TABLE ... ADD/DROP CONSTRAINT` (logical names) to run
    /// before [`finish`].
    pub alter: Option<String>,
    /// What [`finish`] drops on every shard: `(kind, name)` of the
    /// version this one replaces.
    pub retire: Vec<(String, String)>,
    /// What [`abort`] drops on every shard.
    pub plan: Option<Plan>,
    /// Schedules: the row.
    pub schedule: Option<ScheduleRow>,
    /// The definer.
    pub definer: String,
    /// `CHECK` enforcement: the constraint name and message.
    pub check: Option<(String, String)>,
}

impl Pending {
    /// Notes for the client.
    #[must_use]
    pub fn notes(&self) -> Vec<String> {
        let mut v = vec![format!(
            "{} module {} version {}: {}",
            self.kind.as_str(),
            self.module_id,
            self.version,
            placement::explain(&self.placement).replace('\n', ";")
        )];
        v.extend(self.notes.iter().cloned());
        v
    }
}

fn unsupported(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicUnsupported, m)
}

/// The module row for `d`: the existing one (with `OR REPLACE`), or a new
/// one (not yet inserted).
async fn module_for(
    d: &Declaration,
    t: &mut Target<'_>,
    store: &LogicStore,
) -> Result<(ModuleRow, bool)> {
    let kind = d.kind.as_str();
    let existing = store.module(t.catalog, t.db_id, kind, &d.name).await?;
    if existing.is_some() && !d.or_replace {
        return Err(SprigError::standard(
            1304,
            *b"42000",
            format!("{} {} already exists", kind.to_ascii_uppercase(), d.name),
        ));
    }
    Ok(match existing {
        Some(mut m) => {
            m.runtime = d.runtime.as_str().to_owned();
            m.security = "definer".to_owned();
            m.tier_pref = d.tier.as_str().to_owned();
            (m, true)
        }
        None => (
            ModuleRow {
                module_id: Store::new(t.catalog_schema)
                    .next_object_id(t.catalog)
                    .await?
                    .0,
                db_id: t.db_id,
                name: d.name.clone(),
                kind: kind.to_owned(),
                runtime: d.runtime.as_str().to_owned(),
                security: "definer".to_owned(),
                tier_pref: d.tier.as_str().to_owned(),
                active_version: None,
                state: "active".to_owned(),
                created_by: t.user_id,
            },
            false,
        ),
    })
}

/// `CREATE VALIDATION`, `CREATE TRIGGER ... AFTER COMMIT` and `CREATE
/// SCHEDULE`, up to the routines on every shard: compile, place, bind,
/// record the version, deploy all or nothing (see [`Pending`]).
///
/// # Errors
/// As [`deploy`]; 39862 for code placed in the engine tier; 39874 for
/// parameters that name no column; 39873 for a bad schedule; 39860 for a
/// schedule's unknown procedure.
#[allow(clippy::too_many_lines)]
pub async fn deploy_bound(
    d: &Declaration,
    mut t: Target<'_>,
    b: &Bind<'_>,
    shards: &mut dyn ShardSessions,
) -> Result<Pending> {
    Migrator::new(&[LOGIC_MODULE])?
        .run(t.catalog, t.catalog_schema, &MigrateOpts::default())
        .await?;
    let store = LogicStore::new(t.catalog_schema);
    let (module, exists) = module_for(d, &mut t, &store).await?;
    let version = store.next_version(t.catalog, module.module_id).await?;
    let schemas = shards.schemas();
    let first = schemas
        .first()
        .cloned()
        .ok_or_else(|| SprigError::internal("a logic deploy with no shard"))?;

    if d.kind == Kind::Schedule {
        return deploy_schedule(d, t, b, shards, &store, module, exists, version, &first).await;
    }

    let (table, object_id) = b
        .table
        .ok_or_else(|| SprigError::internal("a validation or trigger without its table"))?;
    let cols = {
        let s = shards.session(&first).await?;
        columns(s, &first).await?
    };
    if !sprig_logic_compile::Columns::has_table(&cols, table) {
        return Err(SprigError::standard(
            1146,
            *b"42S02",
            format!("Table '{}.{table}' doesn't exist", b.db_name),
        ));
    }
    let opts = Options {
        module_id: module.module_id,
        version,
        max_loop_iterations: t.max_loop_iterations,
        compile_max_cost: t.compile_max_cost,
        definer_security: true,
    };
    let compiled = compile_bound(d, table, &cols, &opts)?;
    if d.runtime == sprig_logic_compile::Runtime::Javascript {
        // QuickJS must accept it too: the engine tier defines the
        // language, and test runs run it there.
        sprig_logic_js::Program::new(
            d.runtime.as_str(),
            &d.name,
            &d.body,
            crate::testrun::js_row_sig(&d.body),
        )?;
    }
    let placed = placement::place(&compiled, d.tier, t.single_shard)?;
    let (Some(f), Some(p), Tier::MariaDb) = (&compiled.ir, &compiled.plsql, placed.tier) else {
        let why: Vec<String> = placed.reasons.iter().map(ToString::to_string).collect();
        return Err(unsupported(format!(
            "{} {} must compile into MariaDB ({}); the engine tier's {} isn't built yet",
            d.kind.as_str(),
            d.name,
            why.join("; "),
            if d.kind == Kind::Validation {
                "pre-commit hook"
            } else {
                "trigger runner in workers"
            }
        )));
    };
    let bound = enforce::bind_row(f, table, &cols)?;
    let mut binding = match store.binding(t.catalog, module.module_id).await? {
        Some(x) => x,
        None => BindingRow {
            binding_id: 0,
            module_id: module.module_id,
            object_id: Some(object_id),
            column_name: None,
            timing: if d.kind == Kind::Validation {
                "before_write"
            } else {
                "after_commit"
            }
            .to_owned(),
            events: d.events.join(","),
            when_fields: d.when_fields.clone(),
            enforcement: None,
            state: "paused".to_owned(),
        },
    };
    if exists {
        store
            .update_options(t.catalog, &module, t.catalog_version)
            .await?;
    } else {
        store
            .create_module(t.catalog, &module, t.catalog_version)
            .await?;
    }
    let old = (binding.binding_id != 0)
        .then(|| {
            (
                binding.enforcement.clone(),
                binding.events.clone(),
                module.active_version,
            )
        })
        .and_then(|(e, ev, v)| v.map(|v| (e, ev, v)));
    binding.binding_id = store.put_binding(t.catalog, &binding).await?;
    let bid = binding.binding_id;

    let mut notes = Vec::new();
    let mut plan = deploy::plan_entry(&first, p);
    plan.definer = Some(b.definer.clone());
    let mut alter = None;
    let mut retire = Vec::new();
    let mut check = None;
    let old_check = old
        .as_ref()
        .filter(|(e, _, _)| e.as_deref() == Some("check"))
        .map(|(_, _, v)| enforce::check_name(bid, *v));
    if let Some((Some(e), ev, v)) = &old
        && e == "trigger"
    {
        let evs: Vec<String> = ev.split(',').map(str::to_owned).collect();
        for (n, _) in enforce::trigger_names(bid, *v, &evs) {
            retire.push(("TRIGGER".to_owned(), n));
        }
    }
    let enforcement = if d.kind == Kind::Validation {
        let (en, why) = enforce::plan_validation(f, &bound, &d.events, bid, version);
        match &en {
            Enforcement::Check {
                name,
                expr,
                message,
            } => {
                notes.push(format!("enforced by CHECK {name}"));
                alter = Some(enforce::check_alter(
                    b.db_name,
                    table,
                    Some((name, expr)),
                    old_check.as_slice(),
                ));
                check = Some((name.clone(), message.clone()));
                // The CHECK needs no routine on the shards.
                plan.routines.clear();
            }
            Enforcement::Triggers => {
                notes.push(format!(
                    "enforced by BEFORE triggers ({})",
                    why.unwrap_or_default()
                ));
                for (name, ev) in enforce::trigger_names(bid, version, &d.events) {
                    plan.routines.push(Routine {
                        kind: "TRIGGER",
                        name: name.clone(),
                        sql: enforce::validation_trigger(
                            &first, &name, table, ev, &p.entry, &bound,
                        ),
                        oracle: false,
                    });
                }
                if let Some(c) = &old_check {
                    alter = Some(enforce::check_alter(
                        b.db_name,
                        table,
                        None,
                        std::slice::from_ref(c),
                    ));
                }
            }
        }
        en.as_str()
    } else {
        plan.routines.insert(
            0,
            Routine {
                kind: "TABLE",
                name: "__sprig_ldone".to_owned(),
                sql: enforce::done_table(&first),
                oracle: false,
            },
        );
        notes.push(format!(
            "after-commit trigger: binding {bid}, feed subscription {}",
            crate::triggers::subscription_name(bid)
        ));
        "trigger"
    };
    binding.enforcement = Some(enforcement.to_owned());
    binding.events = d.events.join(",");
    binding.when_fields = d.when_fields.clone();
    binding.object_id = Some(object_id);

    let signature = signature_of(f);
    store
        .add_version(
            t.catalog,
            module.module_id,
            version,
            &NewVersion {
                source: &d.body,
                signature,
                catalog_version: t.catalog_version,
                tier: placed.tier.as_str(),
                placement: {
                    let mut pj = serde_json::to_value(&placed).unwrap_or_default();
                    if let Some((n, m)) = &check {
                        pj["check"] = serde_json::Value::from(n.as_str());
                        pj["check_message"] = serde_json::Value::from(m.as_str());
                    }
                    pj
                },
                plsql: Some(p.statements.join(";\n\n")),
                created_by: t.user_id,
            },
        )
        .await?;
    if let Some((name, msg)) = &check {
        crate::triggers::remember_check(name, &d.name, msg);
    }
    if !plan.routines.is_empty()
        && let Err(e) = deploy::deploy_shards(shards, &plan).await
    {
        let _ = store
            .set_version_state(t.catalog, module.module_id, version, "failed")
            .await;
        return Err(e);
    }
    Ok(Pending {
        module_id: module.module_id,
        version,
        kind: d.kind,
        binding: Some(binding),
        placement: placed,
        notes,
        alter,
        retire,
        plan: (!plan.routines.is_empty()).then_some(plan),
        schedule: None,
        definer: b.definer.clone(),
        check,
    })
}

fn signature_of(f: &sprig_logic_compile::Function) -> serde_json::Value {
    serde_json::json!({
        "params": f.params.iter().map(|p| serde_json::json!({"name": p.name, "type": p.ty.name()})).collect::<Vec<_>>(),
        "events": f.events.iter().map(|e| serde_json::json!({
            "name": e.name,
            "fields": e.fields.iter().map(|(n, t)| serde_json::json!({"name": n, "type": t.name()})).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

/// A SQL literal for a schedule's JSON argument.
fn arg_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_owned(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => sprig_logic_compile::pure::str_lit(s),
        other => sprig_logic_compile::pure::str_lit(&other.to_string()),
    }
}

/// The schedule's own routine, `__sprig_l<s>_v<v>`: `SQL SECURITY
/// DEFINER`, so the procedure it calls runs with the schedule's
/// definer's rights, whoever calls it (the worker, as `sprig_ctl`).
#[must_use]
pub fn schedule_routine(
    schema: &str,
    name: &str,
    target: &str,
    args: &[serde_json::Value],
) -> String {
    let list: Vec<String> = args.iter().map(arg_literal).collect();
    format!(
        "CREATE OR REPLACE PROCEDURE `{}`.`{}`()\n  SQL SECURITY DEFINER\n  COMMENT 'sprig logic schedule'\n\
         BEGIN\n  CALL `{}`({});\nEND",
        schema.replace('`', "``"),
        name.replace('`', "``"),
        target.replace('`', "``"),
        list.join(", ")
    )
}

#[allow(clippy::too_many_arguments)]
async fn deploy_schedule(
    d: &Declaration,
    t: Target<'_>,
    b: &Bind<'_>,
    shards: &mut dyn ShardSessions,
    store: &LogicStore,
    module: ModuleRow,
    exists: bool,
    version: u64,
    first: &str,
) -> Result<Pending> {
    let spec = d
        .schedule
        .as_ref()
        .ok_or_else(|| SprigError::internal("a schedule without its clauses"))?;
    let cron = crate::cron::Cron::parse(&spec.cron, &spec.time_zone)?;
    let now = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
        i64::try_from(b.now_ms).unwrap_or(0),
    )
    .unwrap_or_default();
    let next = cron.next_after(now).ok_or_else(|| {
        SprigError::new(
            SprigCode::LogicScheduleInvalid,
            format!("{} never fires", spec.cron),
        )
    })?;
    if spec.target.db.as_deref().is_some_and(|x| x != b.db_name) {
        return Err(unsupported(
            "a schedule calls a procedure of its own database",
        ));
    }
    let mut target = None;
    for kind in ["procedure", "function"] {
        if let Some(m) = store
            .module(t.catalog, t.db_id, kind, &spec.target.name)
            .await?
        {
            target = Some(m);
            break;
        }
    }
    let target = target.ok_or_else(|| {
        SprigError::new(
            SprigCode::LogicNotFound,
            format!("no logic procedure {} in {}", spec.target.name, b.db_name),
        )
    })?;
    if target.active_version.is_none() {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            format!("{} has no active version", spec.target.name),
        ));
    }
    if exists {
        store
            .update_options(t.catalog, &module, t.catalog_version)
            .await?;
    } else {
        store
            .create_module(t.catalog, &module, t.catalog_version)
            .await?;
    }
    let (_, entry) = sprig_logic_compile::plsql::names(module.module_id, version);
    let sql = schedule_routine(first, &entry, &spec.target.name, &spec.args);
    let plan = Plan {
        schema: first.to_owned(),
        routines: vec![Routine {
            kind: "PROCEDURE",
            name: entry.clone(),
            sql: sql.clone(),
            oracle: false,
        }],
        definer: Some(b.definer.clone()),
    };
    let placed = Placement {
        tier: Tier::MariaDb,
        reasons: Vec::new(),
        lints: Vec::new(),
        route: Some(placement::Route::Any),
        reads: Vec::new(),
        writes: Vec::new(),
        cost: 1,
    };
    store
        .add_version(
            t.catalog,
            module.module_id,
            version,
            &NewVersion {
                source: &serde_json::to_string(spec).unwrap_or_default(),
                signature: serde_json::json!({"params": [], "events": [], "target": target.module_id}),
                catalog_version: t.catalog_version,
                tier: "mariadb",
                placement: serde_json::to_value(&placed).unwrap_or_default(),
                plsql: Some(sql),
                created_by: t.user_id,
            },
        )
        .await?;
    if let Err(e) = deploy::deploy_shards(shards, &plan).await {
        let _ = store
            .set_version_state(t.catalog, module.module_id, version, "failed")
            .await;
        return Err(e);
    }
    let next_ms = u64::try_from(next.timestamp_millis()).unwrap_or(0);
    Ok(Pending {
        module_id: module.module_id,
        version,
        kind: Kind::Schedule,
        binding: None,
        placement: placed,
        notes: vec![format!(
            "next run {} UTC ({} in {})",
            next.format("%Y-%m-%d %H:%M:%S"),
            next.with_timezone(
                &spec
                    .time_zone
                    .parse::<chrono_tz::Tz>()
                    .unwrap_or(chrono_tz::UTC)
            )
            .format("%Y-%m-%d %H:%M:%S"),
            spec.time_zone
        )],
        alter: None,
        retire: Vec::new(),
        plan: Some(plan),
        schedule: Some(ScheduleRow {
            module_id: module.module_id,
            cron: spec.cron.clone(),
            time_zone: spec.time_zone.clone(),
            target_module_id: target.module_id,
            args: serde_json::Value::from(spec.args.clone()).to_string(),
            overlap: if spec.queue_one { "queue_one" } else { "skip" }.to_owned(),
            paused: false,
            next_fire_ms: next_ms,
            last_fire_ms: None,
            last_outcome: None,
            running_job_id: None,
            queued: false,
        }),
        definer: b.definer.clone(),
        check: None,
    })
}

/// Ends a bound deploy once [`Pending::alter`] ran: retires what the
/// previous version left on the shards, records the binding or schedule,
/// and activates the version.
///
/// # Errors
/// Catalog errors; a shard's error while retiring (the new version is
/// active by then).
pub async fn finish(
    catalog: &mut Conn,
    catalog_schema: &str,
    catalog_version: u64,
    p: &Pending,
    shards: &mut dyn ShardSessions,
) -> Result<()> {
    let store = LogicStore::new(catalog_schema);
    if let Some(b) = &p.binding {
        let mut b = b.clone();
        b.state = "active".to_owned();
        store.put_binding(catalog, &b).await?;
    }
    if let Some(s) = &p.schedule {
        store.put_schedule(catalog, s).await?;
    }
    store
        .activate(catalog, p.module_id, p.version, catalog_version)
        .await?;
    let _ = store
        .log(
            catalog,
            p.module_id,
            Some(p.version),
            "info",
            "deploy",
            &p.notes().join("; "),
        )
        .await;
    if !p.retire.is_empty() {
        for schema in shards.schemas() {
            let s = shards.session(&schema).await?;
            deploy::drop_objects(s, &schema, &p.retire).await?;
        }
    }
    Ok(())
}

/// Undoes a bound deploy whose `ALTER TABLE` failed: drops the new
/// version's routines on every shard and marks it `failed`.
pub async fn abort(
    catalog: &mut Conn,
    catalog_schema: &str,
    p: &Pending,
    shards: &mut dyn ShardSessions,
) {
    if let Some(plan) = &p.plan {
        for schema in shards.schemas() {
            if let Ok(s) = shards.session(&schema).await {
                deploy::undo(s, &plan.for_schema(&schema)).await;
            }
        }
    }
    let _ = LogicStore::new(catalog_schema)
        .set_version_state(catalog, p.module_id, p.version, "failed")
        .await;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use sprig_logic_compile::ir::Ty;

    /// The signature `deploy_at` records for
    /// `net(price DOUBLE, stock INT) RETURNS DOUBLE` in JavaScript.
    fn net_sig() -> serde_json::Value {
        serde_json::json!({
            "params": [
                {"name": "price", "type": "f64", "sql": "double"},
                {"name": "stock", "type": "i64", "sql": "int"}
            ],
            "events": [{"name": "output", "fields": [{"name": "value", "type": "f64"}]}],
            "scalar": true,
            "returns": "double"
        })
    }

    fn call(sql: &str) -> enforce::ComputedCall {
        enforce::parse_computed(sql).unwrap()
    }

    #[test]
    fn javascript_signatures_come_from_the_version() {
        let s = js_signature_of(&net_sig());
        assert_eq!(
            s.params,
            [
                ("price".to_owned(), Ty::Float),
                ("stock".to_owned(), Ty::Int)
            ]
        );
        assert_eq!(s.returns, Some(Ty::Float));
        // Recorded before `returns` was: `scalar` and the output's field.
        let mut old = net_sig();
        old.as_object_mut().unwrap().remove("returns");
        old["params"][1].as_object_mut().unwrap().remove("sql");
        let s = js_signature_of(&old);
        assert_eq!(s.params[1].1, Ty::Int);
        assert_eq!(s.returns, Some(Ty::Float));
    }

    #[test]
    fn a_javascript_function_makes_a_computed_column() {
        let c = call("ALTER TABLE products ADD COLUMN c DOUBLE AS (net(price, stock)) STORED");
        let body = "export default function (price, stock) { return price * stock; }";
        let expr = computed_column("net", "javascript", body, &net_sig(), &c).unwrap();
        assert!(
            expr.contains("(price)") && expr.contains("(stock)"),
            "{expr}"
        );
        // Before (jsbind) every JavaScript function was 39862 here.
        let wrong = call("ALTER TABLE products ADD COLUMN c DOUBLE AS (net(price)) VIRTUAL");
        let e = computed_column("net", "javascript", body, &net_sig(), &wrong).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicSignatureMismatch));
    }

    #[test]
    fn nondeterministic_code_makes_no_computed_column() {
        for (stored, sql) in [
            (
                true,
                "ALTER TABLE p ADD COLUMN c DOUBLE AS (net(price, stock)) STORED",
            ),
            (
                false,
                "ALTER TABLE p ADD COLUMN c DOUBLE AS (net(price, stock)) VIRTUAL",
            ),
        ] {
            let c = call(sql);
            assert_eq!(c.stored, stored);
            for body in [
                "return price * stock * Math.random();",
                "return Date.now() + price;",
            ] {
                let e = computed_column("net", "javascript", body, &net_sig(), &c).unwrap_err();
                assert_eq!(e.code(), Some(SprigCode::LogicUnsupported), "{body}");
                assert!(e.message.contains("js.nondeterministic"), "{}", e.message);
            }
        }
        // Lua has no compiler front end (RC24).
        let c = call("ALTER TABLE p ADD COLUMN c DOUBLE AS (net(price, stock))");
        let e = computed_column("net", "lua", "return price * stock", &net_sig(), &c).unwrap_err();
        assert!(e.message.contains("lua.front_end"), "{}", e.message);
    }

    fn sales() -> StaticColumns {
        StaticColumns::new().table(
            "sales",
            &[("id", Ty::Int), ("qty", Ty::Int), ("status", Ty::Str)],
        )
    }

    #[test]
    fn javascript_validations_bind_the_row() {
        let opts = Options::default();
        // One message from the new row only: a CHECK.
        let d = crate::declare::parse(
            "CREATE VALIDATION qty_ok ON sales FOR INSERT, UPDATE LANGUAGE javascript AS $$
             export default function (row) { if (row.qty <= 0) sprig.reject('qty must be positive'); }
             $$",
        )
        .unwrap();
        let c = compile_bound(&d, "sales", &sales(), &opts).unwrap();
        assert!(c.compilable(), "{:?}", c.analysis.reasons);
        let f = c.ir.as_ref().unwrap();
        let bound = enforce::bind_row(f, "sales", &sales()).unwrap();
        let (en, _) = enforce::plan_validation(f, &bound, &d.events, 7, 1);
        match en {
            Enforcement::Check { message, expr, .. } => {
                assert_eq!(message, "qty must be positive");
                assert!(expr.contains("`qty`"), "{expr}");
            }
            Enforcement::Triggers => panic!("not a CHECK"),
        }
        // Reading old: BEFORE triggers with NEW, OLD and the operation.
        let d = crate::declare::parse(
            "CREATE VALIDATION grows ON sales FOR UPDATE LANGUAGE javascript AS $$
             if (old !== null && old.qty > row.qty) sprig.reject(`qty ${old.qty} can't fall to ${row.qty}`);
             $$",
        )
        .unwrap();
        let c = compile_bound(&d, "sales", &sales(), &opts).unwrap();
        assert!(c.compilable(), "{:?}", c.analysis.reasons);
        let f = c.ir.as_ref().unwrap();
        let bound = enforce::bind_row(f, "sales", &sales()).unwrap();
        assert!(matches!(
            enforce::plan_validation(f, &bound, &d.events, 7, 1).0,
            Enforcement::Triggers
        ));
        let t = enforce::validation_trigger("s", "tg", "sales", "update", "e", &bound);
        assert!(
            t.contains("NEW.`qty`") && t.contains("OLD.`qty`") && t.contains("'update'"),
            "{t}"
        );
        // An after-commit trigger, and Lua's refusal.
        let d = crate::declare::parse(
            "CREATE TRIGGER audit AFTER COMMIT ON sales FOR INSERT, DELETE LANGUAGE javascript AS $$
             export default function (row, op) {
               db.exec('INSERT INTO sales (id, qty, status) VALUES (?, ?, ?)', [row.id + 1000, row.qty, op]);
             }
             $$",
        )
        .unwrap();
        let c = compile_bound(&d, "sales", &sales(), &opts).unwrap();
        assert!(c.compilable(), "{:?}", c.analysis.reasons);
        let names: Vec<&str> =
            c.ir.as_ref()
                .unwrap()
                .params
                .iter()
                .map(|p| p.name.as_str())
                .collect();
        assert_eq!(names, ["id", "qty", "op"]);
        let mut d = d;
        d.runtime = sprig_logic_compile::Runtime::Lua;
        let c = compile_bound(&d, "sales", &sales(), &opts).unwrap();
        assert!(!c.compilable());
    }
}
