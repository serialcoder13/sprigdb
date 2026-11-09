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
//! event. An engine-tier version's wrapper returns the marker row the
//! gateway runs it from ([`crate::invoke`]), then fails with 39876 for a
//! caller the gateway doesn't serve.

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
    /// `PACKAGE`, `PACKAGE BODY`, `PROCEDURE`, `TRIGGER`, or `TABLE` (an
    /// internal table, created if missing and never dropped by an undo).
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
    /// The definer to write into every routine (`` `t1_u7`@`%` ``), for a
    /// deploy through `sprig_ctl`; `None` deploys as the session's own
    /// account.
    pub definer: Option<String>,
}

impl Plan {
    /// The same plan for another shard schema (statements are qualified
    /// with the schema, which is replaced whole).
    #[must_use]
    pub fn for_schema(&self, schema: &str) -> Plan {
        let from = bq(&self.schema);
        let to = bq(schema);
        Plan {
            schema: schema.to_owned(),
            routines: self
                .routines
                .iter()
                .map(|r| Routine {
                    sql: r.sql.replace(&from, &to),
                    ..r.clone()
                })
                .collect(),
            definer: self.definer.clone(),
        }
    }
}

/// `CREATE OR REPLACE <rest>` → `CREATE OR REPLACE DEFINER=<d> <rest>`.
fn with_definer(sql: &str, definer: &str) -> String {
    match sql.strip_prefix("CREATE OR REPLACE ") {
        Some(rest) => format!("CREATE OR REPLACE DEFINER={definer} {rest}"),
        None => sql.to_owned(),
    }
}

/// `` `u`@`h` `` or `'u'@'h'` as `information_schema` shows it, `u@h`.
fn plain_account(definer: &str) -> String {
    definer.replace(['`', '\''], "")
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

/// The engine-tier wrapper ([`crate::invoke`]): MariaDB resolves the
/// name, checks `EXECUTE` and converts the arguments to the parameter
/// types; the wrapper returns the marker row (module, version, the
/// arguments as a JSON array, numbers as numbers), then fails with 39876,
/// which a gateway with the fallback runtime never shows.
#[must_use]
pub fn engine_wrapper(w: &WrapperSpec<'_>, schema: &str) -> String {
    let msg: String = format!(
        "{} runs in the engine tier, which this gateway can't run",
        w.name
    )
    .chars()
    .take(128)
    .collect();
    let args: Vec<String> = wrapper_params(w.func)
        .into_iter()
        .map(|(n, t)| match t {
            Ty::Int | Ty::Float | Ty::Bool => format!("({n} + 0)"),
            Ty::Str | Ty::Json | Ty::Null => n,
        })
        .collect();
    format!(
        "{}BEGIN\n  SELECT 'sprig' AS `{}`, {} AS `module`, {} AS `version`, JSON_ARRAY({}) AS `args`;\n  \
         SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = {}, MESSAGE_TEXT = {};\nEND",
        header(w, schema, "engine"),
        crate::invoke::MARKER,
        w.module_id,
        w.version,
        args.join(", "),
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
        definer: None,
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
        definer: None,
    }
}

/// The package and entry procedure of a compiled version, without a
/// wrapper (validations, triggers: nothing calls them by name).
#[must_use]
pub fn plan_entry(schema: &str, p: &plsql::Plsql) -> Plan {
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
    Plan {
        schema: schema.to_owned(),
        routines,
        definer: None,
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
    let who = plan.definer.as_deref().map_or(who, plain_account);
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
        let sql = match (&plan.definer, r.kind) {
            (_, "TABLE") | (None, _) => r.sql.clone(),
            (Some(d), _) => with_definer(&r.sql, d),
        };
        if mode != Some(r.oracle) {
            let m = if r.oracle { SQL_MODE } else { WRAPPER_SQL_MODE };
            s.exec(&format!("SET SESSION sql_mode = {}", quote(m)))
                .await?;
            mode = Some(r.oracle);
        }
        s.exec(&sql).await.map_err(|e| {
            tracing::warn!(schema = %plan.schema, routine = %r.name, error = %e, "creating a logic routine failed");
            e
        })?;
    }
    Ok(())
}

async fn verify(s: &mut dyn Session, plan: &Plan, who: &str) -> Result<()> {
    let names: Vec<String> = plan
        .routines
        .iter()
        .filter(|r| r.kind != "TRIGGER" && r.kind != "TABLE")
        .map(|r| quote(&r.name))
        .collect();
    let triggers: Vec<String> = plan
        .routines
        .iter()
        .filter(|r| r.kind == "TRIGGER")
        .map(|r| quote(&r.name))
        .collect();
    let mut rows = Vec::new();
    if !names.is_empty() {
        rows = s
            .rows(&format!(
                "SELECT ROUTINE_NAME, ROUTINE_TYPE, DEFINER FROM information_schema.ROUTINES \
                 WHERE ROUTINE_SCHEMA = {} AND ROUTINE_NAME IN ({})",
                quote(&plan.schema),
                names.join(", ")
            ))
            .await?;
    }
    if !triggers.is_empty() {
        rows.extend(
            s.rows(&format!(
                "SELECT TRIGGER_NAME, 'TRIGGER', DEFINER FROM information_schema.TRIGGERS \
                 WHERE TRIGGER_SCHEMA = {} AND TRIGGER_NAME IN ({})",
                quote(&plan.schema),
                triggers.join(", ")
            ))
            .await?,
        );
    }
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
    undo(s, plan).await;
    Err(SprigError::new(
        SprigCode::DefinerRefused,
        format!("routines {} weren't created as {who}", bad.join(", ")),
    ))
}

/// Drops what `plan` creates, latest first, ignoring errors (tables
/// stay: they are shared and hold markers).
pub async fn undo(s: &mut dyn Session, plan: &Plan) {
    for r in plan.routines.iter().rev() {
        if r.kind == "TABLE" {
            continue;
        }
        let _ = s
            .exec(&format!(
                "DROP {} IF EXISTS {}.{}",
                r.kind,
                bq(&plan.schema),
                bq(&r.name)
            ))
            .await;
    }
}

/// Drops objects of an earlier version from one schema: `(kind, name)`
/// with kinds as in [`Routine`].
///
/// # Errors
/// The server's error.
pub async fn drop_objects(
    s: &mut dyn Session,
    schema: &str,
    objects: &[(String, String)],
) -> Result<()> {
    for (kind, name) in objects {
        s.exec(&format!(
            "DROP {kind} IF EXISTS {}.{}",
            bq(schema),
            bq(name)
        ))
        .await?;
    }
    Ok(())
}

/// Sessions on every shard schema a deploy reaches (chapter 21): one per
/// schema, in the order the deploy visits them.
#[async_trait]
pub trait ShardSessions: Send {
    /// The shard schemas, in order.
    fn schemas(&self) -> Vec<String>;
    /// The session for `schema`.
    ///
    /// # Errors
    /// The shard's cluster is unreachable.
    async fn session<'a>(&'a mut self, schema: &str) -> Result<&'a mut (dyn Session + 'static)>;
}

/// Deploys `plan` (written for its own schema) on every shard schema of
/// `shards`, all or nothing (chapter 21's rule for routines): when a shard
/// fails, what was created on it and on the shards before it is dropped,
/// latest first, and the shard's error returned. Returns the definer.
///
/// # Errors
/// The failing shard's error.
pub async fn deploy_shards(shards: &mut dyn ShardSessions, plan: &Plan) -> Result<String> {
    let schemas = shards.schemas();
    let mut done: Vec<String> = Vec::new();
    let mut who = String::new();
    for schema in &schemas {
        let p = plan.for_schema(schema);
        let r = match shards.session(schema).await {
            Ok(s) => deploy(s, &p).await,
            Err(e) => Err(e),
        };
        match r {
            Ok(w) => {
                who = w;
                done.push(schema.clone());
            }
            Err(e) => {
                tracing::warn!(schema = %schema, error = %e, "logic deploy failed on a shard; undoing");
                if let Ok(s) = shards.session(schema).await {
                    undo(s, &p).await;
                }
                for d in done.iter().rev() {
                    if let Ok(s) = shards.session(d).await {
                        undo(s, &plan.for_schema(d)).await;
                    }
                }
                return Err(e);
            }
        }
    }
    Ok(who)
}

/// A [`Session`] over one `mysql_async` connection (a `sprig_ctl`
/// connection to a shard's cluster, or a worker's).
pub struct ConnSession(pub mysql_async::Conn);

#[async_trait]
impl Session for ConnSession {
    async fn exec(&mut self, sql: &str) -> Result<()> {
        use mysql_async::prelude::Queryable;
        self.0.query_drop(sql).await.map_err(conn_err)
    }

    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        use mysql_async::prelude::Queryable;
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(conn_err)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (0..r.len())
                    .map(|i| match r.as_ref(i) {
                        None | Some(mysql_async::Value::NULL) => None,
                        Some(mysql_async::Value::Bytes(b)) => {
                            Some(String::from_utf8_lossy(b).into_owned())
                        }
                        Some(v) => Some(v.as_sql(true).trim_matches('\'').to_owned()),
                    })
                    .collect()
            })
            .collect())
    }
}

/// A server error as itself; anything else as 39030.
#[must_use]
pub fn conn_err(e: mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(s.code, state, s.message)
        }
        other => SprigError::new(SprigCode::BackendUnavailable, "a shard is unreachable")
            .with_source(other),
    }
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
        assert!(
            e.contains("SELECT 'sprig' AS `__sprig_engine`, 44 AS `module`, 1 AS `version`, JSON_ARRAY((p_productid + 0), (p_qty + 0)) AS `args`;"),
            "{e}"
        );
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

    /// A shard that records statements and fails a chosen one.
    struct Fake {
        log: Vec<String>,
        fail_on: Option<&'static str>,
    }

    #[async_trait]
    impl Session for Fake {
        async fn exec(&mut self, sql: &str) -> Result<()> {
            if self.fail_on.is_some_and(|f| sql.contains(f)) {
                return Err(SprigError::backend(1419, *b"HY000", "refused"));
            }
            self.log.push(sql.to_owned());
            Ok(())
        }
        async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
            Ok(if sql.contains("CURRENT_USER") {
                vec![vec![Some(String::new()), Some("sprig_ctl@%".into())]]
            } else if sql.contains("ROUTINES") || sql.contains("TRIGGERS") {
                let kind = if sql.contains("TRIGGERS") {
                    "TRIGGER"
                } else {
                    "PROCEDURE"
                };
                vec![vec![
                    Some("x".into()),
                    Some(kind.into()),
                    Some("t1_u7@%".into()),
                ]]
            } else {
                Vec::new()
            })
        }
    }

    struct Shards(Vec<(String, Fake)>);

    #[async_trait]
    impl ShardSessions for Shards {
        fn schemas(&self) -> Vec<String> {
            self.0.iter().map(|(s, _)| s.clone()).collect()
        }
        async fn session<'a>(
            &'a mut self,
            schema: &str,
        ) -> Result<&'a mut (dyn Session + 'static)> {
            Ok(&mut self.0.iter_mut().find(|(s, _)| s == schema).unwrap().1)
        }
    }

    #[tokio::test]
    async fn a_failing_shard_undoes_every_shard_before_it() {
        let plan = Plan {
            schema: "t1_d1_g1_s1".into(),
            routines: vec![
                Routine {
                    kind: "PROCEDURE",
                    name: "__sprig_l9_v1".into(),
                    sql: "CREATE OR REPLACE PROCEDURE `t1_d1_g1_s1`.`__sprig_l9_v1`() BEGIN END".into(),
                    oracle: false,
                },
                Routine {
                    kind: "TRIGGER",
                    name: "__sprig_lt3_v1_bi".into(),
                    sql: "CREATE OR REPLACE TRIGGER `t1_d1_g1_s1`.`__sprig_lt3_v1_bi` BEFORE INSERT ON `t1_d1_g1_s1`.`o` FOR EACH ROW BEGIN END".into(),
                    oracle: false,
                },
            ],
            definer: Some("`t1_u7`@`%`".into()),
        };
        let mut ok = Shards(vec![
            (
                "t1_d1_g1_s1".into(),
                Fake {
                    log: vec![],
                    fail_on: None,
                },
            ),
            (
                "t1_d1_g1_s2".into(),
                Fake {
                    log: vec![],
                    fail_on: None,
                },
            ),
        ]);
        assert_eq!(deploy_shards(&mut ok, &plan).await.unwrap(), "t1_u7@%");
        let s2 = &ok.0[1].1.log;
        assert!(s2.iter().any(|q| q.starts_with("CREATE OR REPLACE DEFINER=`t1_u7`@`%` TRIGGER `t1_d1_g1_s2`.`__sprig_lt3_v1_bi` BEFORE INSERT ON `t1_d1_g1_s2`.`o`")), "{s2:?}");

        let mut bad = Shards(vec![
            (
                "t1_d1_g1_s1".into(),
                Fake {
                    log: vec![],
                    fail_on: None,
                },
            ),
            (
                "t1_d1_g1_s2".into(),
                Fake {
                    log: vec![],
                    fail_on: Some("TRIGGER `t1_d1_g1_s2`"),
                },
            ),
        ]);
        let e = deploy_shards(&mut bad, &plan).await.unwrap_err();
        assert!(matches!(
            e.kind,
            sprig_common::ErrorKind::Backend { errno: 1419, .. }
        ));
        for (schema, f) in &bad.0 {
            assert!(
                f.log.iter().any(|q| q.starts_with(&format!(
                    "DROP PROCEDURE IF EXISTS `{schema}`.`__sprig_l9_v1`"
                ))),
                "{schema}: {:?}",
                f.log
            );
        }
        assert!(
            bad.0[0]
                .1
                .log
                .iter()
                .any(|q| q.starts_with("DROP TRIGGER IF EXISTS `t1_d1_g1_s1`.`__sprig_lt3_v1_bi`"))
        );
    }
}
