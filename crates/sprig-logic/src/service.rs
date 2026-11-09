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

/// Deploys `d` (see the module docs).
///
/// # Errors
/// 39861 or 39874 from the compiler; 39868 from placement; 1304 for an
/// existing name without `OR REPLACE`; 39869 for a concurrent deploy;
/// catalog errors; the server's error from creating a routine; 39367.
pub async fn deploy(d: &Declaration, t: Target<'_>, session: &mut dyn Session) -> Result<Outcome> {
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
    let version = store.next_version(t.catalog, module.module_id).await?;
    let cols = columns(session, t.shard_schema).await?;
    let opts = Options {
        module_id: module.module_id,
        version,
        max_loop_iterations: t.max_loop_iterations,
        compile_max_cost: t.compile_max_cost,
        definer_security: d.definer_security,
    };
    let mut compiled = compile(d.runtime, &d.body, &cols, &opts)?;
    let script = matches!(
        d.runtime,
        sprig_logic_compile::Runtime::Javascript | sprig_logic_compile::Runtime::Lua
    );
    if script {
        // No compiler front end yet: the declaration gives the signature
        // (parameters, `RETURNS`), and the version runs in the engine tier.
        compiled.ir = Some(script_function(d));
    }
    let placed = placement::place(&compiled, d.tier, t.single_shard)?;
    if placed.tier == Tier::Engine && d.runtime == sprig_logic_compile::Runtime::Kirun {
        engine_check(d).await?;
    }
    if script {
        script_check(d)?;
    }
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
        signature["scalar"] = serde_json::Value::Bool(d.returns.is_some());
        if let Some(r) = &d.returns {
            signature["returns"] = serde_json::Value::String(r.clone());
        }
    }
    let plsql_text = compiled.plsql.as_ref().map(|p| p.statements.join(";\n\n"));
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
            match (tier, p) {
                (Tier::MariaDb, Some(p)) => Some(deploy::plan_compiled(t.shard_schema, p, &w)),
                _ => Some(deploy::plan_engine(t.shard_schema, &w)),
            }
        }
        (None, _, _) => None,
    };
    let definer = match &plan {
        Some(plan) => match deploy::deploy(session, plan).await {
            Ok(d) => d,
            Err(e) => {
                let _ = store
                    .set_version_state(t.catalog, module.module_id, version, "failed")
                    .await;
                return Err(e);
            }
        },
        None => String::new(),
    };
    store
        .activate(t.catalog, module.module_id, version, t.catalog_version)
        .await?;
    store
        .set_exposure(t.catalog, module.module_id, &d.exposure)
        .await?;
    tracing::info!(
        module = module.module_id,
        version,
        tier = placed.tier.as_str(),
        name = %d.name,
        "logic version deployed"
    );
    Ok(Outcome {
        module_id: module.module_id,
        version,
        entry: compiled.plsql.as_ref().map(|p| p.entry.clone()),
        placement: placed,
        definer,
        select_fn,
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
    let compiled = compile(d.runtime, &d.body, &cols, &opts)?;
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
