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
use sprig_common::{Result, SprigError};
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
    let compiled = compile(d.runtime, &d.body, &cols, &opts)?;
    let placed = placement::place(&compiled, d.tier, t.single_shard)?;
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
    let signature = compiled.ir.as_ref().map_or(serde_json::Value::Null, |f| {
        serde_json::json!({
            "params": f.params.iter().map(|p| serde_json::json!({"name": p.name, "type": p.ty.name()})).collect::<Vec<_>>(),
            "events": f.events.iter().map(|e| serde_json::json!({
                "name": e.name,
                "fields": e.fields.iter().map(|(n, t)| serde_json::json!({"name": n, "type": t.name()})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    });
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
    })
}
