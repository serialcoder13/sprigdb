//! Deploying a version into one shard schema (chapter 41, "Deployment and
//! removal"; build step 10 in part).
//!
//! Until chapter 21's schema-change engine exists, a version deploys into
//! a single-shard database, on the deploying session's own backend
//! connection:
//!
//! ```text
//! deploy(session, plan):
//!   remember @@SESSION.sql_mode; who := CURRENT_USER()
//!   SET SESSION sql_mode = ORACLE,...   package spec, package body, entry procedure
//!   SET SESSION sql_mode = default      the wrapper under the logical name
//!   restore sql_mode (also on failure)
//!   every routine's DEFINER must be `who`, else drop them all: 39367
//! ```
//!
//! Running as the session's account keeps decision A9 (stored code runs as
//! the user who created it) without naming a definer: MariaDB records the
//! creating account, checks its `CREATE ROUTINE` and `ALTER ROUTINE`
//! rights, and the routines read data with that account's rights only.
//! Packages and procedures need no `SUPER` with the binary log on
//! (`decisions/spike-plsql-target.md`); the generator emits no stored
//! functions.
//!
//! The **wrapper** is an ordinary procedure with the logical name, so a
//! client's `CALL db.name(args)` works through the existing CALL path: it
//! calls the entry procedure under `SET STATEMENT max_statement_time` (the
//! wall budget) and returns one row, `event` and then the fields of every
//! event. An engine-tier version gets a wrapper that fails with 39876
//! until the fallback runtime exists.

use async_trait::async_trait;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::ir::{Function, Ty};
use sprig_logic_compile::plsql::{self, SQL_MODE, quote, sql_type};

/// The mode wrappers are created in: MySQL syntax, strict.
pub const WRAPPER_SQL_MODE: &str =
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";

/// A connection to the shard's writer as the deploying user.
#[async_trait]
pub trait Session: Send {
    /// Runs a statement that returns no rows.
    async fn exec(&mut self, sql: &str) -> Result<()>;
    /// Runs a query; each row's columns as text.
    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>>;
}

/// One routine to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routine {
    /// `PACKAGE`, `PACKAGE BODY` or `PROCEDURE`.
    pub kind: &'static str,
    /// Its name in the shard schema.
    pub name: String,
    /// The statement, qualified with the shard schema.
    pub sql: String,
    /// Created under `sql_mode=ORACLE`.
    pub oracle: bool,
}

/// What to create in one shard schema, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The shard schema.
    pub schema: String,
    /// The routines.
    pub routines: Vec<Routine>,
}

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// `CREATE OR REPLACE <kind> <name>` → `... <schema>.<name>`.
fn qualify(sql: &str, kind: &str, name: &str, schema: &str) -> String {
    let head = format!("CREATE OR REPLACE {kind} {name}");
    match sql.strip_prefix(&head) {
        Some(rest) => format!("CREATE OR REPLACE {kind} {}.{}{rest}", bq(schema), bq(name)),
        None => sql.to_owned(),
    }
}

/// What a wrapper needs to know.
#[derive(Debug, Clone)]
pub struct WrapperSpec<'a> {
    /// The logical routine name.
    pub name: &'a str,
    /// The IR, for parameters and events.
    pub func: &'a Function,
    /// `SQL SECURITY DEFINER`.
    pub definer_security: bool,
    /// The wall budget, `logic.function_budget_ms`.
    pub budget_ms: u64,
    /// For the comment: module and version.
    pub module_id: u64,
    /// The version.
    pub version: u64,
}

fn wrapper_params(f: &Function) -> Vec<(String, Ty)> {
    let mut out: Vec<(String, Ty)> = Vec::new();
    for p in &f.params {
        let base = format!("p_{}", sprig_logic_compile::ir::sanitize(&p.name));
        let mut n = base.clone();
        let mut i = 2;
        while out.iter().any(|(x, _)| *x == n) {
            n = format!("{base}_{i}");
            i += 1;
        }
        out.push((n, p.ty));
    }
    out
}

fn header(w: &WrapperSpec<'_>, schema: &str, tier: &str) -> String {
    let params: Vec<String> = wrapper_params(w.func)
        .iter()
        .map(|(n, t)| format!("IN {n} {}", sql_type(*t)))
        .collect();
    format!(
        "CREATE OR REPLACE PROCEDURE {}.{}({})\n  SQL SECURITY {}\n  COMMENT {}\n",
        bq(schema),
        bq(w.name),
        params.join(", "),
        if w.definer_security {
            "DEFINER"
        } else {
            "INVOKER"
        },
        quote(&format!(
            "sprig logic module {} version {} ({tier}, {})",
            w.module_id, w.version, w.func.name
        ))
    )
}

/// The compiled-tier wrapper.
#[must_use]
pub fn wrapper(w: &WrapperSpec<'_>, schema: &str, entry: &str) -> String {
    let args: Vec<String> = wrapper_params(w.func).into_iter().map(|(n, _)| n).collect();
    let mut call_args = args.join(", ");
    if !call_args.is_empty() {
        call_args.push_str(", ");
    }
    // Every event's fields, once each, in event order.
    let mut cols: Vec<(String, Ty)> = Vec::new();
    for e in &w.func.events {
        for (n, t) in &e.fields {
            if n != "event" && !cols.iter().any(|(c, _)| c == n) {
                cols.push((n.clone(), *t));
            }
        }
    }
    let mut select = vec!["v__event AS `event`".to_owned()];
    for (n, t) in &cols {
        let path = quote(&format!(
            "$.\"{}\"",
            n.replace('\\', "\\\\").replace('"', "\\\"")
        ));
        let v = match t {
            Ty::Int | Ty::Bool => format!("CAST(JSON_VALUE(v__result, {path}) AS SIGNED)"),
            Ty::Float => format!("CAST(JSON_VALUE(v__result, {path}) AS DOUBLE)"),
            Ty::Str => format!("JSON_VALUE(v__result, {path})"),
            Ty::Json | Ty::Null => format!("JSON_EXTRACT(v__result, {path})"),
        };
        select.push(format!("{v} AS {}", bq(n)));
    }
    let secs = w.budget_ms.max(1) as f64 / 1000.0;
    format!(
        "{}BEGIN\n  DECLARE v__event VARCHAR(64);\n  DECLARE v__result LONGTEXT;\n  \
         SET STATEMENT max_statement_time = {secs} FOR CALL {}({call_args}v__event, v__result);\n  \
         SELECT {};\nEND",
        header(w, schema, "mariadb"),
        bq(entry),
        select.join(", ")
    )
}

/// The engine-tier wrapper: fails with 39876 until the fallback runtime
/// exists.
#[must_use]
pub fn engine_wrapper(w: &WrapperSpec<'_>, schema: &str) -> String {
    let msg: String = format!("{} runs in the engine tier, not available yet", w.name)
        .chars()
        .take(128)
        .collect();
    format!(
        "{}BEGIN\n  SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = {}, MESSAGE_TEXT = {};\nEND",
        header(w, schema, "engine"),
        SprigCode::LogicEngineUnavailable.number(),
        quote(&msg)
    )
}

/// The plan for a compiled version: its PL/SQL, then the wrapper.
#[must_use]
pub fn plan_compiled(schema: &str, p: &plsql::Plsql, w: &WrapperSpec<'_>) -> Plan {
    let mut routines = Vec::new();
    if let (Some(spec), Some(body)) = (&p.package_spec, &p.package_body) {
        routines.push(Routine {
            kind: "PACKAGE",
            name: p.package.clone(),
            sql: qualify(spec, "PACKAGE", &p.package, schema),
            oracle: true,
        });
        routines.push(Routine {
            kind: "PACKAGE BODY",
            name: p.package.clone(),
            sql: qualify(body, "PACKAGE BODY", &p.package, schema),
            oracle: true,
        });
    }
    routines.push(Routine {
        kind: "PROCEDURE",
        name: p.entry.clone(),
        sql: qualify(&p.procedure, "PROCEDURE", &p.entry, schema),
        oracle: true,
    });
    routines.push(Routine {
        kind: "PROCEDURE",
        name: w.name.to_owned(),
        sql: wrapper(w, schema, &p.entry),
        oracle: false,
    });
    Plan {
        schema: schema.to_owned(),
        routines,
    }
}

/// The plan for an engine-tier version: only its wrapper.
#[must_use]
pub fn plan_engine(schema: &str, w: &WrapperSpec<'_>) -> Plan {
    Plan {
        schema: schema.to_owned(),
        routines: vec![Routine {
            kind: "PROCEDURE",
            name: w.name.to_owned(),
            sql: engine_wrapper(w, schema),
            oracle: false,
        }],
    }
}

/// Runs `plan` on `s` (see the module docs). Returns the definer.
///
/// # Errors
/// The server's error for a statement (the session's `sql_mode` is
/// restored first); 39367 when a routine has another definer.
pub async fn deploy(s: &mut dyn Session, plan: &Plan) -> Result<String> {
    let rows = s.rows("SELECT @@SESSION.sql_mode, CURRENT_USER()").await?;
    let (old_mode, who) = match rows.first().map(Vec::as_slice) {
        Some([m, Some(u)]) => (m.clone().unwrap_or_default(), u.clone()),
        _ => return Err(SprigError::internal("can't read the session's sql_mode")),
    };
    let result = create_all(s, plan).await;
    let restore = s
        .exec(&format!("SET SESSION sql_mode = {}", quote(&old_mode)))
        .await;
    result?;
    restore?;
    verify(s, plan, &who).await?;
    Ok(who)
}

async fn create_all(s: &mut dyn Session, plan: &Plan) -> Result<()> {
    let mut mode: Option<bool> = None;
    for r in &plan.routines {
        if mode != Some(r.oracle) {
            let m = if r.oracle { SQL_MODE } else { WRAPPER_SQL_MODE };
            s.exec(&format!("SET SESSION sql_mode = {}", quote(m)))
                .await?;
            mode = Some(r.oracle);
        }
        s.exec(&r.sql).await.map_err(|e| {
            tracing::warn!(schema = %plan.schema, routine = %r.name, error = %e, "creating a logic routine failed");
            e
        })?;
    }
    Ok(())
}

async fn verify(s: &mut dyn Session, plan: &Plan, who: &str) -> Result<()> {
    let names: Vec<String> = plan.routines.iter().map(|r| quote(&r.name)).collect();
    let rows = s
        .rows(&format!(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE, DEFINER FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = {} AND ROUTINE_NAME IN ({})",
            quote(&plan.schema),
            names.join(", ")
        ))
        .await?;
    let bad: Vec<String> = rows
        .iter()
        .filter(|r| r.get(2).cloned().flatten().as_deref() != Some(who))
        .map(|r| {
            format!(
                "{} {}",
                r[1].clone().unwrap_or_default(),
                r[0].clone().unwrap_or_default()
            )
        })
        .collect();
    if bad.is_empty() {
        return Ok(());
    }
    for r in plan.routines.iter().rev() {
        let kind = if r.kind == "PACKAGE BODY" {
            "PACKAGE BODY"
        } else {
            r.kind
        };
        let _ = s
            .exec(&format!(
                "DROP {kind} IF EXISTS {}.{}",
                bq(&plan.schema),
                bq(&r.name)
            ))
            .await;
    }
    Err(SprigError::new(
        SprigCode::DefinerRefused,
        format!("routines {} weren't created as {who}", bad.join(", ")),
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use sprig_logic_compile::ir::{EventDef, Param};

    fn f() -> Function {
        Function {
            name: "Shop.Quote".into(),
            params: vec![
                Param {
                    name: "productId".into(),
                    ty: Ty::Int,
                    min: None,
                    max: None,
                },
                Param {
                    name: "qty".into(),
                    ty: Ty::Int,
                    min: None,
                    max: None,
                },
            ],
            events: vec![
                EventDef {
                    name: "output".into(),
                    fields: vec![("total".into(), Ty::Float)],
                },
                EventDef {
                    name: "error".into(),
                    fields: vec![("message".into(), Ty::Str)],
                },
            ],
            ..Function::default()
        }
    }

    #[test]
    fn wrapper_returns_event_then_fields() {
        let func = f();
        let w = WrapperSpec {
            name: "quote",
            func: &func,
            definer_security: false,
            budget_ms: 5000,
            module_id: 44,
            version: 1,
        };
        let s = wrapper(&w, "t42_d1", "__sprig_l44_v1");
        assert!(s.starts_with("CREATE OR REPLACE PROCEDURE `t42_d1`.`quote`(IN p_productid BIGINT, IN p_qty BIGINT)"), "{s}");
        assert!(s.contains("SET STATEMENT max_statement_time = 5 FOR CALL `__sprig_l44_v1`(p_productid, p_qty, v__event, v__result);"), "{s}");
        assert!(s.contains("SELECT v__event AS `event`, CAST(JSON_VALUE(v__result, '$.\"total\"') AS DOUBLE) AS `total`, JSON_VALUE(v__result, '$.\"message\"') AS `message`;"), "{s}");
        let e = engine_wrapper(&w, "t42_d1");
        assert!(e.contains("MYSQL_ERRNO = 39876"), "{e}");
    }

    #[test]
    fn qualifying_names() {
        assert_eq!(
            qualify(
                "CREATE OR REPLACE PACKAGE BODY __sprig_l1_v1_lib AS\nEND",
                "PACKAGE BODY",
                "__sprig_l1_v1_lib",
                "t1_d2"
            ),
            "CREATE OR REPLACE PACKAGE BODY `t1_d2`.`__sprig_l1_v1_lib` AS\nEND"
        );
    }
}
