//! Row-level security in the executor (chapter 29; build steps 21 to 24):
//! policy statements, the rewrite before a statement is planned, the
//! session's `@sprig_rls` context, and MariaDB's write checks.
//!
//! ```text
//! policy statement      → tenant::rls::statement, then the table's triggers on
//!                         every shard (sprig_ctl, DEFINER = the session's account)
//!                         and checks_version recorded in a second auth change
//! any other statement of a tenant with row-level security:
//!   @sprig_rls in the text                       → 39050
//!   the context (bypass, exempt tables, policy ids, bound values)
//!                                                → the session's user variable,
//!                                                  synced to every connection
//!   no table binds the session                   → unchanged
//!   SELECT, WITH, VALUES, TABLE, INSERT, REPLACE, UPDATE, DELETE, SET, DO,
//!   EXPLAIN of a statement, ANALYZE of a statement → sprig_sql::rls::inject
//!     a written table whose triggers are stale   → redeployed now, else 39366
//!   CALL, PREPARE, EXECUTE, HANDLER, CHECKSUM, LOAD DATA ... REPLACE,
//!   CREATE VIEW | PROCEDURE | FUNCTION | TRIGGER | EVENT | TABLE ... SELECT
//!     naming a protected table (or stored code with dynamic SQL) → 39366
//!   LOAD DATA into a protected table             → MariaDB's triggers check it
//!   DDL, SHOW, DESCRIBE of a table, transactions → unchanged
//! ```

#![allow(
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::trivially_copy_pass_by_ref,
    clippy::match_same_arms,
    clippy::assigning_clones,
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::format_push_string,
    clippy::too_many_lines
)] // pedantic style: names follow the chapter's pseudocode

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use sprig_auth::authz::rls::{RlsState, RlsStore};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::{ChangeOpts, VersionKind};
use sprig_common::ids::{ClusterId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::deploy::ShardSessions;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sql::lex::{self, LexOptions, TokenKind, ident_value};
use sprig_sql::rls::stmt::PolicyStmt;
use sprig_sql::rls::triggers::{CONTEXT_VAR, TriggerPolicy, TriggerSpec, drops};
use sprig_sql::rls::{inject, unsupported};

use super::PassSession;
use super::tenant::Step;
use crate::backend::state::VarValue;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};
use crate::tenant::rls::{PolicyAnswer, Resolve, SessionValues};

/// What a statement is, for the rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// Rewritten (or refused) by `inject`.
    Rewrite,
    /// Refused if it names a protected table.
    RefuseIfNamed,
    /// Stored code: refused if it names a protected table or runs dynamic
    /// SQL.
    StoredCode,
    /// `LOAD DATA` / `LOAD XML`.
    Load,
    /// Left alone.
    Skip,
}

fn words(text: &[u8], lex_opts: &LexOptions) -> Vec<(TokenKind, String)> {
    lex::lex(text, lex_opts)
        .map(|all| {
            all.iter()
                .filter(|t| !t.kind.is_trivia())
                .map(|t| {
                    let v = if t.kind.is_ident_like() {
                        String::from_utf8_lossy(&ident_value(t.bytes(text))).into_owned()
                    } else {
                        String::from_utf8_lossy(t.bytes(text)).into_owned()
                    };
                    (t.kind, v)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn class(w: &[(TokenKind, String)]) -> Class {
    let up = |i: usize| {
        w.get(i)
            .filter(|(k, _)| *k == TokenKind::Word)
            .map(|(_, v)| v.to_ascii_uppercase())
            .unwrap_or_default()
    };
    let first = up(0);
    match first.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "INSERT" | "REPLACE" | "UPDATE" | "DELETE"
        | "SET" | "DO" => Class::Rewrite,
        "" if w.first().is_some_and(|(_, v)| v == "(") => Class::Rewrite,
        "EXPLAIN" | "DESCRIBE" | "DESC" => {
            let second = up(1);
            if matches!(
                second.as_str(),
                "SELECT"
                    | "WITH"
                    | "INSERT"
                    | "REPLACE"
                    | "UPDATE"
                    | "DELETE"
                    | "FORMAT"
                    | "EXTENDED"
                    | "PARTITIONS"
                    | "ANALYZE"
                    | "TABLE"
                    | "VALUES"
                    | "FOR"
            ) || w.get(1).is_some_and(|(_, v)| v == "(")
            {
                Class::Rewrite
            } else {
                Class::Skip
            }
        }
        "ANALYZE" => {
            if matches!(up(1).as_str(), "TABLE" | "NO_WRITE_TO_BINLOG" | "LOCAL") {
                Class::Skip
            } else {
                Class::Rewrite
            }
        }
        "LOAD" => Class::Load,
        "CALL" | "PREPARE" | "EXECUTE" | "HANDLER" | "CHECKSUM" => Class::RefuseIfNamed,
        "CREATE" => {
            let rest: Vec<String> = (1..w.len().min(12)).map(up).collect();
            let code = rest.iter().any(|r| {
                matches!(
                    r.as_str(),
                    "VIEW" | "PROCEDURE" | "FUNCTION" | "TRIGGER" | "EVENT" | "PACKAGE"
                )
            });
            if code {
                Class::StoredCode
            } else if rest
                .first()
                .is_some_and(|r| r == "TABLE" || r == "TEMPORARY" || r == "OR")
                && w.iter()
                    .any(|(k, v)| *k == TokenKind::Word && v.eq_ignore_ascii_case("SELECT"))
            {
                Class::RefuseIfNamed
            } else {
                Class::Skip
            }
        }
        _ => Class::Skip,
    }
}

/// Write checks seen on every shard: (tenant, object, catalog version,
/// policy version). A regroup, split or move that lost a table's triggers
/// comes with a new catalog version, so the next write looks again.
type Verified = std::sync::Mutex<std::collections::HashSet<(u64, u64, u64, u64)>>;

fn verified() -> &'static Verified {
    static V: std::sync::OnceLock<Verified> = std::sync::OnceLock::new();
    V.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn reserved_var() -> SprigError {
    SprigError::new(
        SprigCode::ReservedName,
        format!("'@{CONTEXT_VAR}' is a reserved name"),
    )
}

impl PassSession {
    /// Chapter 29's policy statements; `None` for any other text.
    pub(super) async fn tenant_policy(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let (lex_opts, _) = self.lex_opts();
        let Some(parsed) = sprig_sql::rls::stmt::parse(text, &lex_opts) else {
            return Ok(None);
        };
        let status = |s: StatusFlags| {
            if more {
                s | StatusFlags::MORE_RESULTS_EXISTS
            } else {
                s
            }
        };
        let stmt: PolicyStmt = match parsed {
            Ok(s) => s,
            Err(e) => return self.fail(out, &e).await.map(Some),
        };
        let current = self.current_logical();
        let charset = cx.proto.collation;
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        if ts.principal.must_change_password {
            return self
                .fail(
                    out,
                    &SprigError::standard(
                        1820,
                        *b"HY000",
                        "You must reset your password using ALTER USER statement before \
                         executing this statement."
                            .to_owned(),
                    ),
                )
                .await
                .map(Some);
        }
        let cat = match ts.refresh().await {
            Ok(c) => c,
            Err(e) => return self.fail(out, &e).await.map(Some),
        };
        let r = Box::pin(crate::tenant::rls::statement(
            ts,
            &cat,
            &stmt,
            current.as_deref(),
            &lex_opts,
            charset,
        ))
        .await;
        let s = status(self.status());
        match r {
            Ok(PolicyAnswer::Rows(ans)) => {
                super::tenant::send_local(out, &ans, format, s).await?;
            }
            Ok(PolicyAnswer::Unchanged) => out.ok(OkPacket::new(s)).await?,
            Ok(PolicyAnswer::Changed { object, .. }) => {
                let mut ok = OkPacket::new(s);
                // RL14 (round 2): change sinks over the table whose owners
                // are now bound stop.
                if let Some(ts) = self.tenant.as_deref()
                    && let Some(cat) = ts.catalog().cloned()
                    && let Err(e) = crate::tenant::rls::pause_bound_sinks(ts, &cat, object).await
                {
                    tracing::warn!(error = %e, "sinks over a protected table not checked");
                }
                if let Err(e) = Box::pin(self.deploy_checks(object)).await {
                    tracing::warn!(error = %e, "row-level security write checks not deployed");
                    if let Some(ts) = self.tenant.as_deref_mut() {
                        ts.warnings = Some(vec![crate::tenant::session::LocalWarning {
                            level: "Warning",
                            code: 39366,
                            message: format!(
                                "ER_SPRIG_RLS_UNSUPPORTED: the policy holds; writes to the \
                                 table are refused until MariaDB's checks are deployed ({e})"
                            ),
                        }]);
                    }
                    ok.warnings = 1;
                }
                out.ok(ok).await?;
            }
            Err(e) => return self.fail(out, &e).await.map(Some),
        }
        Ok(Some(Step {
            errored: false,
            status: Some(s),
        }))
    }

    /// Deploys `object`'s write checks on every shard of its table (or
    /// drops them when security is off) and records the version.
    #[allow(clippy::too_many_lines)] // one step per check, in order
    pub(super) async fn deploy_checks(&mut self, object: ObjectId) -> Result<()> {
        let persona = self.persona;
        let own_cluster = self.target.cluster.clone();
        let (lex_opts, _) = self.lex_opts();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let cat = ts.refresh().await?;
        let state = RlsState::of(&cat)
            .cloned()
            .ok_or_else(|| SprigError::internal("row-level security isn't loaded"))?;
        let Some(sec) = state.security(object).copied() else {
            return Ok(());
        };
        let obj = cat
            .objects
            .get(object)
            .cloned()
            .ok_or_else(|| SprigError::internal("the policy's table is gone"))?;
        let db = cat
            .databases
            .get(obj.db_id)
            .cloned()
            .ok_or_else(|| SprigError::internal("the policy's database is gone"))?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("no user"))?;
        let tenant = ts.tenant;
        let svc = Arc::clone(ts.services());
        let template = ts.schema_of(Some(&db.name));
        let layout = super::router::ddl_layout(ts);
        let list: Vec<(String, ClusterId)> = match (&layout, &template) {
            (Some(l), Some(t)) => l(t, Some(&obj.name)).filter(|v| !v.is_empty()),
            _ => None,
        }
        .or_else(|| template.clone().map(|t| vec![(t, own_cluster.clone())]))
        .ok_or_else(|| SprigError::internal("the table has no shard this gateway can find"))?;
        // Where a policy subquery's table lives: its group's shard here if
        // co-located with this shard, else its first shard, else its
        // database's schema.
        let table_schema = |d: &str, t: &str, here: &str| -> Option<String> {
            let tmpl = ts.schema_of(Some(d))?;
            let shards = layout
                .as_ref()
                .and_then(|l| l(&tmpl, Some(t)))
                .unwrap_or_default();
            if shards.iter().any(|(s, _)| s == here) {
                return Some(here.to_owned());
            }
            shards.first().map(|(s, _)| s.clone()).or(Some(tmpl))
        };
        // Triggers and the policy view run as a user who reads and writes
        // the table itself (RL19): the changing user when it can.
        let definer_user = crate::tenant::rls::view_definer(&cat, object, user);
        let definer_account = sprig_auth::authz::backend::account(tenant, definer_user);
        let definer = format!("`{}`@`%`", definer_account.replace('`', "``"));
        let enforcement = crate::tenant::rls::enforcement(&svc, &cat, obj.db_id);
        let views = enforcement == "views";
        let policies: Vec<_> = state.policies_of(object).cloned().collect();
        let tp: Vec<TriggerPolicy<'_>> = policies
            .iter()
            .map(|p| TriggerPolicy {
                id: p.policy_id,
                restrictive: p.restrictive,
                commands: p.commands,
                using: p.using_expr.as_deref(),
                check: p.check_expr.as_deref(),
            })
            .collect();
        // The table's columns on each shard, invisible ones marked: the
        // views name them (RL28).
        let mut probe = super::logic_bound::CtlShards {
            list: list.clone(),
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let mut columns: HashMap<String, Vec<(String, bool)>> = HashMap::new();
        let mut hidden: Vec<String> = Vec::new();
        for (schema, _) in &list {
            let s = probe.session(schema).await?;
            let cols = table_columns(s, schema, &obj.name).await?;
            for (c, inv) in &cols {
                let l = c.to_lowercase();
                if *inv && !hidden.contains(&l) {
                    hidden.push(l);
                }
            }
            columns.insert(schema.clone(), cols);
        }
        drop(probe);
        crate::tenant::rls::hidden_set(tenant.0, object.0, hidden);
        let mut plan: Vec<(String, Vec<String>)> = Vec::new();
        for (schema, _) in &list {
            // `off` (RL30): the gateway's rewrite alone, nothing in MariaDB.
            if !sec.enabled || enforcement == "off" {
                let mut d = drops(schema, object.0);
                d.push(sprig_sql::rls::views::view_drop(
                    schema, &obj.name, object.0,
                ));
                d.push(sprig_sql::rls::views::hidden_view_drop(
                    schema, &obj.name, object.0,
                ));
                plan.push((schema.clone(), d));
                continue;
            }
            let here = schema.clone();
            let db_name = db.name.clone();
            let physical = |e: &str| -> Result<String> {
                sprig_sql::rls::expr::map_table_schemas(e, &lex_opts, &|d, t| {
                    table_schema(d.unwrap_or(&db_name), t, &here)
                })
            };
            let spec = TriggerSpec {
                schema,
                table: &obj.name,
                object: object.0,
                tenant: tenant.0,
                definer: &definer,
                policies: &tp,
                lex: lex_opts,
                physical: &physical,
            };
            let mut stmts = spec.statements()?;
            if views {
                // The context function, its caller's right to run it, then
                // the views (RL18, RL28).
                let v = spec.view_statements(
                    &definer,
                    columns.get(schema).map(Vec::as_slice).unwrap_or_default(),
                )?;
                stmts.push(v.function);
                stmts.push(format!(
                    "GRANT EXECUTE ON FUNCTION {}.{} TO '{}'@'%'",
                    sprig_sql::rls::quote(schema),
                    sprig_sql::rls::quote(sprig_sql::rls::views::CTX_FUNCTION),
                    definer_account
                ));
                stmts.extend(v.views);
            } else {
                stmts.push(sprig_sql::rls::views::view_drop(
                    schema, &obj.name, object.0,
                ));
                stmts.push(sprig_sql::rls::views::hidden_view_drop(
                    schema, &obj.name, object.0,
                ));
            }
            plan.push((schema.clone(), stmts));
        }
        let _ = persona;
        let mut shards = super::logic_bound::CtlShards {
            list,
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        for (schema, stmts) in &plan {
            let s = shards.session(schema).await?;
            // The trigger texts are unqualified (see `TriggerSpec`).
            s.exec(&format!("USE {}", sprig_sql::rls::quote(schema)))
                .await?;
            for q in stmts {
                s.exec(q).await?;
            }
        }
        verified()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        // Record the deployed version (another auth change).
        let mut change = svc
            .catalog
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let r = {
            let (core, q) = change.tx().parts();
            RlsStore::new(core.schema())
                .checks_deployed(q, object, sec.changed_version)
                .await
        };
        match r {
            Ok(()) => {
                let v = change.commit().await?;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    crate::tenant::rls::await_auth(ts, v).await;
                }
                Ok(())
            }
            Err(e) => {
                let _ = change.rollback().await;
                let _ = svc.catalog.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }

    /// Whether `object`'s triggers are on every shard of its table now
    /// (cached per catalog and policy version); redeploys them if not.
    ///
    /// # Errors
    /// The deployment's.
    #[allow(clippy::too_many_lines)] // one step per check, in order
    pub(super) async fn ensure_checks(&mut self, object: ObjectId) -> Result<()> {
        let own_cluster = self.target.cluster.clone();
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(());
        };
        let Some(cat) = ts.catalog().cloned() else {
            return Ok(());
        };
        let Some(sec) = RlsState::of(&cat).and_then(|s| s.security(object).copied()) else {
            return Ok(());
        };
        let (Some(obj), Some(db)) = (
            cat.objects.get(object).cloned(),
            cat.objects
                .get(object)
                .and_then(|o| cat.databases.get(o.db_id))
                .cloned(),
        ) else {
            return Ok(());
        };
        let template = ts.schema_of(Some(&db.name));
        let layout = super::router::ddl_layout(ts);
        let list: Vec<(String, ClusterId)> = match (&layout, &template) {
            (Some(l), Some(t)) => l(t, Some(&obj.name)).filter(|v| !v.is_empty()),
            _ => None,
        }
        .or_else(|| template.clone().map(|t| vec![(t, own_cluster.clone())]))
        .unwrap_or_default();
        // The shard list is in the key: a split or move needn't change the
        // catalog version, and the mover copies triggers but not views.
        // So is `rls.backend_enforcement` (RL30): a global value changes
        // without a catalog version, and every change applies at once.
        let enforcement = crate::tenant::rls::enforcement(ts.services(), &cat, obj.db_id);
        let shards_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            list.iter()
                .map(|(s, c)| (s, c.to_string()))
                .collect::<Vec<_>>()
                .hash(&mut h);
            enforcement.hash(&mut h);
            h.finish()
        };
        let key = (
            ts.tenant.0,
            object.0,
            cat.version ^ shards_hash,
            sec.changed_version,
        );
        if verified()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&key)
        {
            return Ok(());
        }
        let names = sprig_sql::rls::triggers::names(object.0);
        let views = enforcement == "views" && sec.enabled;
        let triggers = enforcement != "off" && sec.enabled;
        let view = sprig_sql::rls::views::view_name(&obj.name, object.0);
        let hview = sprig_sql::rls::views::hidden_view_name(&obj.name, object.0);
        let mut hidden: Vec<String> = Vec::new();
        let mut shards = super::logic_bound::CtlShards {
            list: list.clone(),
            conns: HashMap::new(),
            locator: Arc::clone(&ts.services().locator),
        };
        let mut complete = !list.is_empty();
        for (schema, _) in &list {
            let s = shards.session(schema).await?;
            let q = |x: &str| sprig_sql::rls::sql_string(x, false);
            // The triggers, and (views) the views' columns against the
            // table's: a column added, dropped or renamed since makes a view
            // stale (RL28: the first names the visible columns, the second
            // all of them, and exists only when some are invisible).
            let cols = |name: &str, filter: &str| {
                format!(
                    "(SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR '\\n') \
                     FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {}{filter})",
                    q(name),
                    s = q(schema),
                )
            };
            let rows = s
                .rows(&format!(
                    "SELECT (SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = {s} \
                     AND TRIGGER_NAME IN ({}, {}, {})), {}, {}, {}, {}, {}",
                    q(&names[0]),
                    q(&names[1]),
                    q(&names[2]),
                    cols(&obj.name, " AND EXTRA NOT LIKE '%INVISIBLE%'"),
                    cols(&obj.name, ""),
                    cols(&obj.name, " AND EXTRA LIKE '%INVISIBLE%'"),
                    cols(&view, ""),
                    cols(&hview, ""),
                    s = q(schema),
                ))
                .await?;
            let cell = |i: usize| -> Option<String> { rows.first().and_then(|r| r.get(i).cloned().flatten()) };
            let count = cell(0).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            let triggers_ok = if triggers { count == 3 } else { count == 0 || !sec.enabled };
            let (visible, all, invisible) = (cell(1), cell(2), cell(3));
            for c in invisible.iter().flat_map(|v| v.split('\n')) {
                let l = c.to_lowercase();
                if !hidden.contains(&l) {
                    hidden.push(l);
                }
            }
            let views_ok = if views {
                visible.is_some()
                    && cell(4) == visible
                    && if invisible.is_some() { cell(5) == all } else { cell(5).is_none() }
            } else {
                // Views off (RL30): views left from before must go.
                cell(4).is_none() && cell(5).is_none()
            };
            if !triggers_ok || !views_ok {
                complete = false;
                break;
            }
        }
        // Stale policy views (RL29): a regroup, split or move left the table
        // in other schemas of these clusters; views of it elsewhere read
        // nothing valid and hold grants, so they go.
        let mut clusters: Vec<&str> = Vec::new();
        for (schema, c) in &list {
            if clusters.iter().any(|x| *x == c.as_str()) {
                continue;
            }
            clusters.push(c.as_str());
            let s = shards.session(schema).await?;
            let q = |x: &str| sprig_sql::rls::sql_string(x, false);
            let stale = s
                .rows(&format!(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS \
                     WHERE TABLE_SCHEMA LIKE {} AND TABLE_NAME IN ({}, {})",
                    q(&format!("t{}\\_d{}\\_%", ts.tenant.0, obj.db_id.0)),
                    q(&view),
                    q(&hview),
                ))
                .await?;
            for r in stale {
                let (Some(sch), Some(name)) = (r.first().cloned().flatten(), r.get(1).cloned().flatten())
                else {
                    continue;
                };
                if list.iter().any(|(x, _)| *x == sch) {
                    continue;
                }
                tracing::info!(object = object.0, schema = %sch, view = %name, "stale row-level security policy view dropped");
                s.exec(&format!(
                    "DROP VIEW IF EXISTS {}.{}",
                    sprig_sql::rls::quote(&sch),
                    sprig_sql::rls::quote(&name)
                ))
                .await?;
            }
        }
        drop(shards);
        crate::tenant::rls::hidden_set(ts.tenant.0, object.0, hidden);
        if !complete {
            tracing::info!(
                object = object.0,
                "row-level security checks missing on a shard: redeploying"
            );
            Box::pin(self.deploy_checks(object)).await?;
            return Ok(());
        }
        verified()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key);
        Ok(())
    }

    /// The rewrite before planning (see the module docs): the text to plan
    /// instead, if it changed.
    ///
    /// # Errors
    /// 39050, 39366, 39364.
    #[allow(clippy::too_many_lines)] // one step per check, in order
    pub(super) async fn tenant_rls(
        &mut self,
        cx: &ExecCtx<'_>,
        text: &Bytes,
        lex_opts: &LexOptions,
    ) -> Result<Option<Bytes>> {
        if let Some(bs) = self.bs.as_mut()
            && bs.vars().rls_views.is_some()
        {
            bs.vars_mut().rls_views = None;
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        ts.rls.covered.clear();
        if sprig_sql::rls::inject::names_context_variable(text, lex_opts) {
            return Err(reserved_var());
        }
        // `information_schema.SPRIG_ROW_POLICIES`: the session's rows, inline.
        if let Some(t) = crate::tenant::rls::inline_view(ts, text, lex_opts) {
            let t = Bytes::from(t);
            return Ok(Some(
                Box::pin(self.tenant_rls(cx, &t, lex_opts))
                    .await?
                    .unwrap_or(t),
            ));
        }
        let Some(cat) = ts.catalog().cloned() else {
            return Ok(None);
        };
        let Some(state) = RlsState::of(&cat) else {
            return Ok(None);
        };
        if !state.any_enabled() {
            return Ok(None);
        }
        let p = crate::tenant::authz::privileges(ts, &cat);
        let Some(rls) = p.row_policies() else {
            return Ok(None);
        };
        let mut values = SessionValues::of(ts, &cat);
        // RL10 (round 2): the subtree functions' tenant sets.
        crate::tenant::rls::bind_tree(ts, &cat, &rls, &mut values).await?;
        // An internal exempt session (a search seed, RL17): no filter, and
        // MariaDB's checks see a bypass.
        let exempt = crate::tenant::rls::is_exempt(cx.session.id);
        let context = if exempt {
            let mut all = (*rls).clone();
            all.bypass = true;
            values.context(&all)
        } else {
            values.context(&rls)
        };
        // Policy views (RL18): the bound tables' views replace them in
        // every statement sent, internal exempt sessions included (their
        // context's bypass lets the view show every row).
        let swap = crate::tenant::rls::view_swap(ts, &cat, &rls);
        let _ = self.bs(cx);
        if let Some(bs) = self.bs.as_mut() {
            bs.vars_mut()
                .user_vars
                .insert(CONTEXT_VAR.to_owned(), VarValue::Text(context.into_bytes()));
            bs.vars_mut().rls_views.clone_from(&swap);
        }
        if rls.is_empty() {
            return Ok(None);
        }
        if exempt {
            if let Some(ts) = self.tenant.as_deref_mut() {
                ts.rls.covered = rls.tables.keys().copied().collect();
            }
            return Ok(None);
        }
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let w = words(text, lex_opts);
        // Metadata that counts hidden rows: `NULL` for protected tables.
        let protected_names: Vec<(String, String)> = rls
            .tables
            .keys()
            .filter_map(|o| {
                let obj = cat.objects.get(*o)?;
                let db = cat.databases.get(obj.db_id)?;
                Some((db.name.clone(), obj.name.clone()))
            })
            .collect();
        let svc = Arc::clone(ts.services());
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let cols = |v: &str| {
            mysql
                .then(|| crate::tenant::rls::mysql84_columns(v))
                .flatten()
                .or_else(|| svc.columns.columns(v).map(<[String]>::to_vec))
        };
        let masked = crate::tenant::rls::mask_metadata(text, lex_opts, &cols, &protected_names)
            .map(Bytes::from);
        let changed = masked.is_some();
        let text: &Bytes = masked.as_ref().unwrap_or(text);
        let w = if changed { words(text, lex_opts) } else { w };
        let same = |t: &Bytes| changed.then(|| t.clone());
        // SHOW TABLE STATUS and SHOW INDEX report row counts and
        // cardinalities MariaDB measured over hidden rows.
        let up = |i: usize| {
            w.get(i)
                .map(|(_, v)| v.to_ascii_uppercase())
                .unwrap_or_default()
        };
        if up(0) == "SHOW"
            && (up(1) == "TABLE" && up(2) == "STATUS"
                || matches!(up(1).as_str(), "INDEX" | "INDEXES" | "KEYS")
                || matches!(up(2).as_str(), "INDEX" | "INDEXES" | "KEYS"))
        {
            return Err(unsupported(
                "SHOW TABLE STATUS or SHOW INDEX for a session bound by row-level security \
                 (they count hidden rows; information_schema.TABLES and STATISTICS show the \
                 rest)",
            ));
        }
        let lctn = ts.lower_case_table_names();
        let mut names: Vec<String> = rls
            .tables
            .keys()
            .filter_map(|o| cat.objects.get(*o))
            .map(|o| o.name.to_lowercase())
            .collect();
        let mut tainted = crate::tenant::rls::tainted(ts, &cat, &names).await?;
        // RL20: under policy views, views and stored code reading protected
        // tables read the views instead, and then apply the session's
        // policies in MariaDB; dynamic SQL stays refused.
        let svc2 = Arc::clone(ts.services());
        let fixable: Vec<crate::tenant::rls::TaintedCode> = tainted
            .code
            .iter()
            .filter(|c| {
                !c.dynamic
                    && matches!(
                        c.kind,
                        ObjectKind::View
                            | ObjectKind::Procedure
                            | ObjectKind::Function
                            | ObjectKind::Trigger
                    )
                    && crate::tenant::rls::enforcement(&svc2, &cat, c.db) == "views"
            })
            .cloned()
            .collect();
        if !fixable.is_empty() {
            let pointed = Box::pin(self.repoint(&cat, &fixable)).await;
            tainted.without(&pointed);
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let tainted = tainted.names;
        names.extend(tainted.iter().cloned());
        // A protected name as an identifier, or inside a string (the text
        // of PREPARE ... FROM '...').
        let named = w.iter().any(|(k, v)| {
            let v = v.to_lowercase();
            match k {
                TokenKind::Word | TokenKind::QuotedIdent => names.contains(&v),
                TokenKind::String => names.iter().any(|n| v.contains(n.as_str())),
                _ => false,
            }
        });
        let c = class(&w);
        match c {
            Class::Skip => return Ok(same(text)),
            Class::RefuseIfNamed if named => {
                return Err(unsupported(
                    "a statement whose effect on a protected table the gateway can't filter",
                ));
            }
            Class::RefuseIfNamed => return Ok(same(text)),
            Class::StoredCode => {
                let dynamic = w.iter().any(|(k, v)| {
                    *k == TokenKind::Word
                        && (v.eq_ignore_ascii_case("PREPARE")
                            || v.eq_ignore_ascii_case("IMMEDIATE"))
                });
                // Under policy views, code naming a protected table reads the
                // views (RL20): only dynamic SQL stays refused.
                let viewed = named
                    && !dynamic
                    && rls
                        .tables
                        .keys()
                        .all(|o| crate::tenant::rls::views_on(ts.services(), &cat, *o));
                if (named && !viewed) || dynamic {
                    return Err(unsupported(
                        "stored code reading a protected table, or running dynamic SQL, for a \
                         session bound by row-level security",
                    ));
                }
                return Ok(same(text));
            }
            Class::Load => {
                if !named {
                    return Ok(same(text));
                }
                if w.iter()
                    .any(|(k, v)| *k == TokenKind::Word && v.eq_ignore_ascii_case("REPLACE"))
                {
                    return Err(unsupported("LOAD DATA ... REPLACE into a protected table"));
                }
                // INTO TABLE [db.]t: MariaDB's triggers check every row.
                let at = w
                    .iter()
                    .position(|(k, v)| *k == TokenKind::Word && v.eq_ignore_ascii_case("TABLE"))
                    .ok_or_else(|| unsupported("LOAD DATA without INTO TABLE"))?;
                let (db, t) = if w.get(at + 2).is_some_and(|(_, v)| v == ".") {
                    (
                        w.get(at + 1).map(|x| x.1.clone()),
                        w.get(at + 3).map(|x| x.1.clone()),
                    )
                } else {
                    (None, w.get(at + 1).map(|x| x.1.clone()))
                };
                let t = t.unwrap_or_default();
                let temp = |d: Option<&str>, n: &str| ts.is_temp(d, n);
                let r = Resolve::new(
                    &cat,
                    &rls,
                    current.as_deref(),
                    lctn,
                    &temp,
                    &values,
                    *lex_opts,
                    &tainted,
                );
                if let Some(k) = sprig_sql::rls::Resolver::protected(&r, db.as_deref(), &t) {
                    let o = r.keys.borrow()[k];
                    if rls.tables.get(&o).is_some_and(|x| !x.checks_current) {
                        return Err(unsupported(
                            "writing a protected table before its MariaDB checks are deployed",
                        ));
                    }
                    ts.rls.covered.push(o);
                }
                return Ok(same(text));
            }
            Class::Rewrite => {}
        }
        let src = std::str::from_utf8(text)
            .map_err(|_| unsupported("a statement over a protected table that isn't UTF-8"))?;
        // The resolver isn't `Send`: everything it touches ends here.
        let (inj, covered, stale, needs_checks) = {
            let temp = |d: Option<&str>, n: &str| ts.is_temp(d, n);
            let r = Resolve::new(
                &cat,
                &rls,
                current.as_deref(),
                lctn,
                &temp,
                &values,
                *lex_opts,
                &tainted,
            );
            let Some(inj) = inject(src, lex_opts, &r)? else {
                return Ok(same(text));
            };
            let keys = r.keys.borrow().clone();
            let pick = |v: &[usize]| -> Vec<ObjectId> {
                v.iter().filter_map(|k| keys.get(*k).copied()).collect()
            };
            let filtered = pick(&inj.filtered);
            let written = pick(&inj.written);
            let stale: Vec<ObjectId> = written
                .iter()
                .chain(filtered.iter())
                .filter(|o| rls.tables.get(o).is_some_and(|x| !x.checks_current))
                .copied()
                .collect();
            let needs_checks = !written.is_empty()
                || w.first().is_some_and(|(_, v)| {
                    v.eq_ignore_ascii_case("UPDATE") || v.eq_ignore_ascii_case("DELETE")
                });
            let mut covered = filtered.clone();
            covered.extend(written.iter().copied());
            covered.extend(crate::tenant::rls::policy_reads(&cat, &filtered, lex_opts));
            (inj, covered, stale, needs_checks)
        };
        ts.rls.covered = covered.clone();
        // Reads through policy views: each view must be on every shard
        // and match the table's columns (checked once per catalog
        // version).
        if !needs_checks && let Some(sw) = &swap {
            let viewed: Vec<ObjectId> = covered
                .iter()
                .filter(|o| {
                    cat.objects.get(**o).is_some_and(|x| {
                        sw.tables
                            .get(&x.db_id.0)
                            .is_some_and(|m| m.contains_key(&x.name.to_lowercase()))
                    })
                })
                .copied()
                .collect();
            for o in viewed {
                if let Err(e) = Box::pin(self.ensure_checks(o)).await {
                    tracing::warn!(error = %e, "row-level security policy view not verified");
                }
            }
            // A table found without a view since: read it directly.
            let now = self
                .tenant
                .as_deref()
                .and_then(|ts| crate::tenant::rls::view_swap(ts, &cat, &rls));
            if let Some(bs) = self.bs.as_mut() {
                bs.vars_mut().rls_views = now;
            }
        }
        if needs_checks {
            let bound: Vec<ObjectId> = covered
                .iter()
                .filter(|o| rls.tables.contains_key(o) && !stale.contains(o))
                .copied()
                .collect();
            for o in bound {
                if let Err(e) = Box::pin(self.ensure_checks(o)).await {
                    tracing::warn!(error = %e, "row-level security write checks not verified");
                    return Err(unsupported(
                        "writing a protected table whose MariaDB checks can't be verified",
                    ));
                }
            }
        }
        if needs_checks {
            // The checks may have found invisible columns (RL28): the
            // swap reads the all-columns view for statements naming them.
            let now = self
                .tenant
                .as_deref()
                .and_then(|ts| crate::tenant::rls::view_swap(ts, &cat, &rls));
            if let Some(bs) = self.bs.as_mut() {
                bs.vars_mut().rls_views = now;
            }
        }
        if needs_checks && !stale.is_empty() {
            for o in &stale {
                if let Err(e) = Box::pin(self.deploy_checks(*o)).await {
                    tracing::warn!(error = %e, "row-level security write checks not deployed");
                    return Err(unsupported(
                        "writing a protected table before its MariaDB checks are deployed",
                    ));
                }
            }
            if let Some(ts) = self.tenant.as_deref_mut() {
                ts.rls.covered = covered;
            }
        }
        metrics::counter!("sprig_rls_rewrites_total", "protocol" => "mysql").increment(1);
        Ok(Some(Bytes::from(inj.sql.into_bytes())))
    }
}

/// Table `table`'s columns in `schema`, in order, `true` for invisible
/// ones (what the policy views name, RL28).
async fn table_columns(
    s: &mut (dyn sprig_logic::deploy::Session + 'static),
    schema: &str,
    table: &str,
) -> Result<Vec<(String, bool)>> {
    let rows = s
        .rows(&format!(
            "SELECT COLUMN_NAME, EXTRA LIKE '%INVISIBLE%' FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            sprig_sql::rls::sql_string(schema, false),
            sprig_sql::rls::sql_string(table, false),
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let name = r.first().cloned().flatten()?;
            let inv = r.get(1).cloned().flatten().is_some_and(|v| v == "1");
            Some((name, inv))
        })
        .collect())
}

/// Code already pointed at the policy views: (tenant, object, schema
/// version, protected set).
type Repointed = std::sync::Mutex<std::collections::HashSet<(u64, u64, u64, u64)>>;

fn repointed() -> &'static Repointed {
    static R: std::sync::OnceLock<Repointed> = std::sync::OnceLock::new();
    R.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

/// `CREATE ...` as `CREATE OR REPLACE ...`.
fn or_replace(text: &str) -> Option<String> {
    let t = text.trim_start();
    t.get(..6).filter(|h| h.eq_ignore_ascii_case("CREATE"))?;
    let after = &t[6..];
    if after
        .trim_start()
        .to_ascii_uppercase()
        .starts_with("OR REPLACE")
    {
        return Some(t.to_owned());
    }
    Some(format!("CREATE OR REPLACE{after}"))
}

impl PassSession {
    /// Points `code` at the policy views on every shard schema holding it
    /// (RL20): `SHOW CREATE`, [`sprig_sql::rls::views::swap`], `CREATE OR
    /// REPLACE` as `sprig_ctl`, keeping definer, `sql_mode` and character
    /// set. The objects that read no protected table unfiltered now.
    pub(super) async fn repoint(
        &mut self,
        cat: &Arc<sprig_catalog::TenantCatalog>,
        code: &[crate::tenant::rls::TaintedCode],
    ) -> std::collections::HashSet<ObjectId> {
        use std::hash::{Hash, Hasher};
        let mut safe = std::collections::HashSet::new();
        let own_cluster = self.target.cluster.clone();
        let Some(ts) = self.tenant.as_deref() else {
            return safe;
        };
        let Some(state) = RlsState::of(cat) else {
            return safe;
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant.0;
        // Protected tables: (db, lower name) → view, or no view yet.
        let mut views: HashMap<(u32, String), String> = HashMap::new();
        let mut no_view: std::collections::HashSet<(u32, String)> =
            std::collections::HashSet::new();
        let mut hidden: HashMap<String, (String, Vec<String>)> = HashMap::new();
        for sec in state.rows().security.iter().filter(|x| x.enabled) {
            let Some(o) = cat.objects.get(sec.object_id) else {
                continue;
            };
            let k = (o.db_id.0, o.name.to_lowercase());
            if sec.checks_version > 0 && crate::tenant::rls::views_on(&svc, cat, sec.object_id) {
                let view = sprig_sql::rls::views::view_name(&o.name, sec.object_id.0);
                if let Some(cols) = crate::tenant::rls::hidden_of(tenant, sec.object_id.0) {
                    hidden.insert(
                        view.clone(),
                        (
                            sprig_sql::rls::views::hidden_view_name(&o.name, sec.object_id.0),
                            cols.to_vec(),
                        ),
                    );
                }
                views.insert(k, view);
            } else {
                no_view.insert(k);
            }
        }
        let set_hash = {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            let mut v: Vec<_> = views.keys().collect();
            v.sort();
            v.hash(&mut h);
            let mut n: Vec<_> = no_view.iter().collect();
            n.sort();
            n.hash(&mut h);
            let mut x: Vec<_> = hidden.iter().collect();
            x.sort();
            x.hash(&mut h);
            h.finish()
        };
        let layout = super::router::ddl_layout(ts);
        let db_of = |schema: &str| -> Option<u32> {
            let rest = schema.strip_prefix(&format!("t{tenant}_d"))?;
            let end = rest.find('_')?;
            rest[..end].parse().ok()
        };
        for c in code {
            let key = (tenant, c.object.0, c.schema_version, set_hash);
            if repointed()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&key)
            {
                safe.insert(c.object);
                continue;
            }
            let Some(db) = cat.databases.get(c.db) else {
                continue;
            };
            let template = ts.schema_of(Some(&db.name));
            let list: Vec<(String, ClusterId)> = match (&layout, &template) {
                (Some(l), Some(t)) => l(t, None).filter(|v| !v.is_empty()),
                _ => None,
            }
            .or_else(|| template.clone().map(|t| vec![(t, own_cluster.clone())]))
            .unwrap_or_default();
            let mut shards = super::logic_bound::CtlShards {
                list: list.clone(),
                conns: HashMap::new(),
                locator: Arc::clone(&svc.locator),
            };
            let mut ok = !list.is_empty();
            for (schema, _) in &list {
                let r = repoint_one(
                    &mut shards,
                    schema,
                    c,
                    &CodeViews {
                        views: &views,
                        hidden: &hidden,
                        no_view: &no_view,
                    },
                    &db_of,
                )
                .await;
                if let Err(e) = r {
                    tracing::warn!(object = c.object.0, %schema, error = %e, "stored code not pointed at the policy views: refused to bound sessions");
                    ok = false;
                    break;
                }
            }
            if ok {
                repointed()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(key);
                safe.insert(c.object);
            }
        }
        safe
    }
}

/// The policy views stored code reads instead of protected tables.
struct CodeViews<'a> {
    /// (db, lower-case table) → its policy view.
    views: &'a HashMap<(u32, String), String>,
    /// Policy view → all-columns view and invisible columns (RL28).
    hidden: &'a HashMap<String, (String, Vec<String>)>,
    /// Protected tables without a view yet.
    no_view: &'a std::collections::HashSet<(u32, String)>,
}

struct CodeLookup<'a> {
    v: &'a CodeViews<'a>,
    schema: &'a str,
    db_of: &'a (dyn Fn(&str) -> Option<u32> + Sync),
    missing: std::cell::Cell<bool>,
}

impl sprig_sql::rls::views::Lookup for CodeLookup<'_> {
    fn view(&self, sch: Option<&str>, t: &str) -> Option<String> {
        let db = (self.db_of)(sch.unwrap_or(self.schema))?;
        let k = (db, t.to_lowercase());
        if self.v.no_view.contains(&k) {
            self.missing.set(true);
        }
        self.v.views.get(&k).cloned()
    }

    fn hidden(&self, view: &str) -> Option<(String, Vec<String>)> {
        self.v.hidden.get(view).cloned()
    }
}

/// [`sprig_sql::rls::views::swap`] of stored code in `schema`; 39366 if it
/// names a protected table that has no policy view yet.
fn swap_code(
    text: &str,
    lex: LexOptions,
    schema: &str,
    v: &CodeViews<'_>,
    db_of: &(dyn Fn(&str) -> Option<u32> + Sync),
) -> Result<Option<String>> {
    let lookup = CodeLookup {
        v,
        schema,
        db_of,
        missing: std::cell::Cell::new(false),
    };
    let new = sprig_sql::rls::views::swap(text, &lex, &lookup, &std::collections::HashSet::new())?;
    if lookup.missing.get() {
        return Err(unsupported(
            "stored code over a protected table without its policy view",
        ));
    }
    Ok(new)
}

async fn repoint_one(
    shards: &mut super::logic_bound::CtlShards,
    schema: &str,
    c: &crate::tenant::rls::TaintedCode,
    v: &CodeViews<'_>,
    db_of: &(dyn Fn(&str) -> Option<u32> + Sync),
) -> Result<()> {
    let (kind, col, mode_col, cs_col, coll_col) = match c.kind {
        ObjectKind::View => ("VIEW", 1, None, 2, 3),
        ObjectKind::Procedure => ("PROCEDURE", 2, Some(1), 3, 4),
        ObjectKind::Function => ("FUNCTION", 2, Some(1), 3, 4),
        ObjectKind::Trigger => ("TRIGGER", 2, Some(1), 3, 4),
        _ => {
            return Err(unsupported(
                "this kind of stored code over a protected table",
            ));
        }
    };
    let q = sprig_sql::rls::quote;
    let s = shards.session(schema).await?;
    let rows = match s
        .rows(&format!("SHOW CREATE {kind} {}.{}", q(schema), q(&c.name)))
        .await
    {
        Ok(r) => r,
        // Not in this schema (code lives where it was created).
        Err(e) if matches!(e.protocol().mysql_errno, 1146 | 1305 | 1360 | 1347 | 1049) => {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let cell = |i: usize| row.get(i).cloned().flatten();
    let text = cell(col).ok_or_else(|| unsupported("stored code without its text"))?;
    let mode = mode_col.and_then(cell).unwrap_or_default();
    let up = mode.to_ascii_uppercase();
    let lex = LexOptions {
        ansi_quotes: up.contains("ANSI_QUOTES"),
        no_backslash_escapes: up.contains("NO_BACKSLASH_ESCAPES"),
        ..LexOptions::mariadb()
    };
    let new = swap_code(&text, lex, schema, v, db_of)?;
    let Some(new) = new else {
        return Ok(());
    };
    let new = or_replace(&new).ok_or_else(|| unsupported("stored code text"))?;
    let cs = cell(cs_col).unwrap_or_else(|| "utf8mb4".into());
    let collation = cell(coll_col);
    let names = match collation {
        Some(c) => format!("SET NAMES {cs} COLLATE {c}"),
        None => format!("SET NAMES {cs}"),
    };
    s.exec(&names).await?;
    if mode_col.is_some() {
        s.exec(&format!(
            "SET SESSION sql_mode = {}",
            sprig_sql::rls::sql_string(&mode, false)
        ))
        .await?;
    }
    s.exec(&format!("USE {}", q(schema))).await?;
    let r = s.exec(&new).await;
    let _ = s.exec("SET SESSION sql_mode = DEFAULT").await;
    let _ = s.exec("SET NAMES utf8mb4").await;
    r?;
    tracing::info!(object = c.object.0, %schema, kind, "stored code pointed at the policy views");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(s: &str) -> Class {
        class(&words(s.as_bytes(), &LexOptions::mariadb()))
    }

    #[test]
    fn stored_code_is_recreated_in_place() {
        assert_eq!(
            or_replace("CREATE DEFINER=`t1_u2`@`%` PROCEDURE p() SELECT 1").as_deref(),
            Some("CREATE OR REPLACE DEFINER=`t1_u2`@`%` PROCEDURE p() SELECT 1")
        );
        assert_eq!(
            or_replace("create or replace view v as select 1").as_deref(),
            Some("create or replace view v as select 1")
        );
        assert_eq!(or_replace("SELECT 1"), None);
    }

    #[test]
    fn statements_are_classified() {
        assert_eq!(c("select 1"), Class::Rewrite);
        assert_eq!(c("(SELECT 1) UNION (SELECT 2)"), Class::Rewrite);
        assert_eq!(c("EXPLAIN SELECT * FROM t"), Class::Rewrite);
        assert_eq!(c("EXPLAIN t"), Class::Skip);
        assert_eq!(c("DESCRIBE t"), Class::Skip);
        assert_eq!(c("ANALYZE TABLE t"), Class::Skip);
        assert_eq!(c("ANALYZE SELECT * FROM t"), Class::Rewrite);
        assert_eq!(c("CREATE VIEW v AS SELECT 1"), Class::StoredCode);
        assert_eq!(
            c("CREATE DEFINER=CURRENT_USER PROCEDURE p() BEGIN END"),
            Class::StoredCode
        );
        assert_eq!(c("CREATE TABLE x AS SELECT * FROM t"), Class::RefuseIfNamed);
        assert_eq!(c("CREATE TABLE x LIKE t"), Class::Skip);
        assert_eq!(c("LOAD DATA LOCAL INFILE 'f' INTO TABLE t"), Class::Load);
        assert_eq!(c("CALL p()"), Class::RefuseIfNamed);
        assert_eq!(c("ALTER TABLE t ADD c INT"), Class::Skip);
        assert_eq!(c("SET @a = 1"), Class::Rewrite);
    }
}
