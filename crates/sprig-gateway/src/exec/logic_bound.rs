//! Validations, after-commit triggers, schedules and computed fields in a
//! tenant session (chapter 41; R7 orders 3 and 5).
//!
//! ```text
//! CREATE VALIDATION | TRIGGER ... AFTER COMMIT | SCHEDULE:
//!   chapter 29: TRIGGER on the table (validations, triggers), CREATE
//!     ROUTINE on the database (schedules), checked here
//!   shards: chapter 21's layout (the table's group, or every shard of the
//!     database); one sprig_ctl connection per shard, every routine and
//!     trigger written with DEFINER = the user's backend account and the
//!     definer checked afterwards; all or nothing (sprig_logic::service)
//!   a CHECK validation: ALTER TABLE ... ADD CONSTRAINT through this
//!     session's own DDL path (chapter 21's engine), as if the client sent it
//!   trigger: feed subscription logic:b<binding> registered at the cluster's
//!     binlog position now; schedule: logic/schedules/<tenant>/<module>
//! ALTER TABLE t ADD c TYPE AS (f(args)), f a logic function: f's body as
//!   one expression replaces the call, and the statement runs as ordinary
//!   DDL (a generated column through chapter 21's engine)
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::catalog::BindingRow;
use sprig_logic::declare::{Declaration, Kind};
use sprig_logic::deploy::{ConnSession, Session, ShardSessions, conn_err};
use sprig_logic::service::{self, Bind, Target};
use sprig_logic::{LogicStore, enforce};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use super::tenant::{Step, scrub_typed};
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, Wire};
use crate::tenant::session::LocalWarning;

/// `logic.max_loop_iterations`'s default.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
/// `logic.compile_max_cost`'s default (validations: `logic.validation_max_cost`).
const COMPILE_MAX_COST: u64 = 100_000;

/// `sprig_ctl` connections to every shard a deploy reaches.
struct CtlShards {
    list: Vec<(String, ClusterId)>,
    conns: HashMap<String, ConnSession>,
    locator: Arc<crate::tenant::locator::GatewayLocator>,
}

#[async_trait]
impl ShardSessions for CtlShards {
    fn schemas(&self) -> Vec<String> {
        self.list.iter().map(|(s, _)| s.clone()).collect()
    }

    async fn session<'a>(&'a mut self, schema: &str) -> Result<&'a mut (dyn Session + 'static)> {
        if !self.conns.contains_key(schema) {
            let cluster = self
                .list
                .iter()
                .find(|(s, _)| s == schema)
                .map(|(_, c)| c.clone())
                .ok_or_else(|| {
                    SprigError::internal(format!("{schema} isn't a shard of this deploy"))
                })?;
            let pool = self.locator.ctl_pool(&cluster).await?;
            let conn = pool.get_conn().await.map_err(conn_err)?;
            self.conns.insert(schema.to_owned(), ConnSession(conn));
        }
        self.conns
            .get_mut(schema)
            .map(|c| c as &mut (dyn Session + 'static))
            .ok_or_else(|| SprigError::internal("no shard session"))
    }
}

impl CtlShards {
    /// `@@gtid_binlog_pos` of the first shard's cluster.
    async fn position(&mut self) -> Result<(ClusterId, String)> {
        let (schema, cluster) = self
            .list
            .first()
            .cloned()
            .ok_or_else(|| SprigError::internal("no shard"))?;
        self.session(&schema).await?;
        let c = self
            .conns
            .get_mut(&schema)
            .ok_or_else(|| SprigError::internal("no shard session"))?;
        let pos: Option<String> =
            c.0.query_first("SELECT @@gtid_binlog_pos")
                .await
                .map_err(conn_err)?;
        Ok((cluster, pos.unwrap_or_default()))
    }
}

fn now_ms() -> u64 {
    sprig_logic::triggers::maria::now_ms()
}

fn denied(action: &str, user: &str, what: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!("{action} command denied to user '{user}' for table '{what}'"),
    )
}

/// A sink that keeps one statement's outcome (the `CHECK`'s `ALTER`).
struct Outcome {
    wire: Wire,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Outcome {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(
        &mut self,
        _c: &[sprig_proto_mysql::ColumnDef],
        _f: RowFormat,
    ) -> Result<()> {
        Ok(())
    }
    async fn row(&mut self, _v: crate::pipeline::RowValues<'_>) -> Result<()> {
        Ok(())
    }
    async fn raw_row(&mut self, _p: Bytes) -> Result<()> {
        Ok(())
    }
    async fn end_rows(&mut self, _e: crate::pipeline::EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn ok(&mut self, _ok: OkPacket) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }
    async fn column_list(
        &mut self,
        _c: &[sprig_proto_mysql::ColumnDef],
        _e: crate::pipeline::EndOfRows,
    ) -> Result<()> {
        Ok(())
    }
    async fn local_infile_request(&mut self, _f: &[u8]) -> Result<()> {
        Err(SprigError::internal("no LOAD DATA here"))
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.done
    }
}

impl PassSession {
    /// Answers a bound declaration's outcome: OK with its notes, or the
    /// error.
    async fn answer(
        &mut self,
        r: Result<Vec<String>>,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        match r {
            Ok(notes) => {
                let n = notes.len();
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.warnings = Some(
                        notes
                            .into_iter()
                            .map(|m| LocalWarning {
                                level: "Note",
                                code: 1105,
                                message: m,
                            })
                            .collect(),
                    );
                }
                let mut ok = OkPacket::new(status);
                ok.warnings = u16::try_from(n).unwrap_or(u16::MAX);
                out.ok(ok).await?;
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => {
                let shown = SprigError {
                    kind: e.kind,
                    message: e.message.clone(),
                    retryable: e.retryable,
                    source: None,
                };
                out.error(&scrub_typed(shown)).await?;
                Ok(Step {
                    errored: true,
                    status: None,
                })
            }
        }
    }

    /// `CREATE VALIDATION | TRIGGER ... AFTER COMMIT | SCHEDULE` (see the
    /// module docs). Returns the client's notes.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn logic_deploy_bound(
        &mut self,
        cx: &mut ExecCtx<'_>,
        decl: Declaration,
    ) -> Result<Vec<String>> {
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let db = decl
            .table
            .as_ref()
            .and_then(|t| t.db.clone())
            .or_else(|| decl.db.clone())
            .or(current)
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(&db, lctn);
        let row = catalog.database(&db_key).cloned().ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
        })?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let user_name = ts.principal.user.clone();
        let grants = crate::tenant::authz::privileges(ts, &catalog);
        let table = if let Some(t) = &decl.table {
            {
                let o = catalog
                    .object(
                        row.db_id,
                        Namespace::Relation,
                        &object_key(ObjectKind::Table, &t.name, lctn),
                    )
                    .filter(|o| o.state == ObjectState::Active)
                    .cloned()
                    .ok_or_else(|| {
                        SprigError::standard(
                            1146,
                            *b"42S02",
                            format!("Table '{db}.{}' doesn't exist", t.name),
                        )
                    })?;
                if !grants.unrestricted()
                    && !grants.allows(
                        Action::Trigger,
                        AuthzTarget::Object {
                            db_key: &db_key,
                            db: row.db_id,
                            object: o.object_id,
                        },
                    )
                {
                    return Err(denied("TRIGGER", &user_name, &t.name));
                }
                Some((t.name.clone(), o.object_id.0))
            }
        } else {
            {
                if !grants.unrestricted()
                    && !grants.allows(
                        Action::CreateRoutine,
                        AuthzTarget::Database { db_key: &db_key },
                    )
                {
                    return Err(SprigError::standard(
                        1044,
                        *b"42000",
                        format!("Access denied for user '{user_name}' to database '{db}'"),
                    ));
                }
                None
            }
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        // Chapter 21's shards: the table's group, or every shard of the
        // database; one shard (the template) for unsharded databases.
        let template = ts.schema_of(Some(&db));
        let layout = super::router::ddl_layout(ts);
        let shard_list: Vec<(String, ClusterId)> = {
            let from_layout = match (&layout, &template) {
                (Some(l), Some(t)) => l(t, table.as_ref().map(|(n, _)| n.as_str())),
                (Some(l), None) => {
                    let first = sprig_catalog::ddl::shard_schemas(tenant, &row)
                        .first()
                        .map(ToString::to_string);
                    first.and_then(|f| l(&f, table.as_ref().map(|(n, _)| n.as_str())))
                }
                _ => None,
            };
            if let Some(v) = from_layout.filter(|v| !v.is_empty()) {
                v
            } else {
                {
                    let t = template.clone().ok_or_else(|| {
                        SprigError::new(
                            SprigCode::LogicUnsupported,
                            format!("database {db} has no shard this gateway can find"),
                        )
                    })?;
                    vec![(t, self.target.cluster.clone())]
                }
            }
        };
        let single_shard = shard_list.len() == 1;
        let mut shards = CtlShards {
            list: shard_list,
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let account = sprig_auth::authz::backend::account(tenant, user);
        let definer = format!("`{}`@`%`", account.replace('`', "``"));
        let first = shards.list[0].0.clone();
        let pending = service::deploy_bound(
            &decl,
            Target {
                catalog: &mut conn,
                catalog_schema: ep.store.schema(),
                catalog_version: catalog.version,
                db_id: row.db_id.0,
                shard_schema: &first,
                single_shard,
                user_id: user.0,
                budget_ms: 5000,
                max_loop_iterations: MAX_LOOP_ITERATIONS,
                compile_max_cost: COMPILE_MAX_COST,
            },
            &Bind {
                db_name: &row.name,
                table: table.as_ref().map(|(n, o)| (n.as_str(), *o)),
                definer,
                now_ms: now_ms(),
            },
            &mut shards,
        )
        .await?;
        if let Some(alter) = &pending.alter {
            let mut sink = Outcome {
                wire: Wire {
                    persona: self.persona,
                    caps: cx.proto.caps,
                },
                error: None,
                done: false,
            };
            let text = Bytes::from(alter.clone().into_bytes());
            let attrs = QueryAttrs::default();
            let r =
                Box::pin(self.tenant_text(cx, &text, &attrs, false, RowFormat::Text, &mut sink))
                    .await;
            let err = match r {
                Err(e) => Some(e),
                Ok(_) => sink.error.take(),
            };
            if let Some(e) = err {
                service::abort(&mut conn, ep.store.schema(), &pending, &mut shards).await;
                return Err(sprig_logic::triggers::map_check_error(e));
            }
        }
        service::finish(
            &mut conn,
            ep.store.schema(),
            catalog.version,
            &pending,
            &mut shards,
        )
        .await?;
        let mut notes = pending.notes();
        match decl.kind {
            Kind::Trigger => {
                if let Some(b) = &pending.binding {
                    let (cluster, pos) = shards.position().await?;
                    let fresh = sprig_logic::triggers::maria::register(
                        svc.locator.dc_store(),
                        tenant,
                        b.binding_id,
                        pending.module_id,
                        cluster.clone(),
                        &pos,
                    )
                    .await?;
                    if fresh {
                        notes.push(format!("subscribed from {cluster} at {pos}"));
                    }
                }
            }
            Kind::Schedule => {
                if let Some(s) = &pending.schedule {
                    sprig_logic::schedule::put_index(
                        svc.locator.dc_store().as_ref(),
                        &sprig_logic::schedule::ScheduleIndex {
                            tenant,
                            module_id: pending.module_id,
                            schema: first.clone(),
                            next_fire_ms: s.next_fire_ms,
                        },
                    )
                    .await?;
                }
            }
            _ => {}
        }
        tracing::info!(module = pending.module_id, version = pending.version, kind = decl.kind.as_str(), name = %decl.name, "logic version deployed");
        Ok(notes)
    }

    /// `ALTER TABLE t ADD c TYPE AS (f(args))` where `f` is a logic
    /// function (see the module docs). `None`: not a computed field, the
    /// statement goes on as ordinary DDL.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    pub(super) async fn tenant_computed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let sql = String::from_utf8_lossy(text).into_owned();
        let Some(call) = enforce::parse_computed(&sql) else {
            return Ok(None);
        };
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        if ts.principal.must_change_password {
            return Ok(None);
        }
        let Some(db) = call
            .function_db
            .clone()
            .or_else(|| call.db.clone())
            .or(current)
        else {
            return Ok(None);
        };
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let Some(row) = catalog.database(&database_key(&db, lctn)).cloned() else {
            return Ok(None);
        };
        let table_db = call.db.clone().unwrap_or_else(|| db.clone());
        let table_obj = catalog
            .database(&database_key(&table_db, lctn))
            .and_then(|d| {
                catalog.object(
                    d.db_id,
                    Namespace::Relation,
                    &object_key(ObjectKind::Table, &call.table, lctn),
                )
            })
            .map(|o| o.object_id.0);
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let Ok(ep) = svc.locator.writer(tenant).await else {
            return Ok(None);
        };
        let Ok(mut conn) = ep.pool.get_conn().await else {
            return Ok(None);
        };
        let store = LogicStore::new(ep.store.schema());
        let mut module = None;
        for kind in ["function", "procedure"] {
            match store
                .module(&mut conn, row.db_id.0, kind, &call.function)
                .await
            {
                Ok(Some(m)) => {
                    module = Some(m);
                    break;
                }
                Ok(None) => {}
                // No logic catalog yet: not a logic function.
                Err(_) => return Ok(None),
            }
        }
        let Some(module) = module else {
            return Ok(None);
        };
        let r: Result<String> = async {
            let v = module.active_version.ok_or_else(|| {
                SprigError::new(
                    SprigCode::LogicNotFound,
                    format!("{} has no active version", module.name),
                )
            })?;
            let source = store.source(&mut conn, module.module_id, v).await?;
            let runtime = sprig_logic::compiler::Runtime::parse(&module.runtime)
                .unwrap_or(sprig_logic::compiler::Runtime::Kirun);
            let compiled = sprig_logic::compiler::compile(
                runtime,
                &source,
                &sprig_logic::compiler::StaticColumns::new(),
                &sprig_logic::compiler::Options::default(),
            )?;
            let f = compiled.ir.ok_or_else(|| {
                SprigError::new(
                    SprigCode::LogicUnsupported,
                    format!("{} has no compiled form", module.name),
                )
            })?;
            let expr = enforce::computed_expr(&f, &call)?;
            Ok(enforce::rewrite_computed(&sql, &call, &expr))
        }
        .await;
        let rewritten = match r {
            Ok(t) => t,
            Err(e) => return self.answer(Err(e), status, out).await.map(Some),
        };
        tracing::info!(function = %call.function, column = %call.column, "computed field as a generated column");
        let step = Box::pin(self.tenant_text(
            cx,
            &Bytes::from(rewritten.into_bytes()),
            attrs,
            more,
            format,
            out,
        ))
        .await?;
        if !step.errored {
            let b = BindingRow {
                binding_id: 0,
                module_id: module.module_id,
                object_id: table_obj,
                column_name: Some(call.column.clone()),
                timing: if call.stored { "on_write" } else { "on_read" }.to_owned(),
                events: String::new(),
                when_fields: Vec::new(),
                enforcement: Some("generated".to_owned()),
                state: "active".to_owned(),
            };
            if let Err(e) = store.put_binding(&mut conn, &b).await {
                tracing::warn!(error = %e, "computed field's binding not recorded");
            }
        }
        Ok(Some(step))
    }
}

/// A validation's `CHECK` failure as 39863 (chapter 41). A constraint
/// this gateway hasn't seen yet loads the tenant's `CHECK` messages in the
/// background, so later failures show the validation's own message.
pub(super) fn map_validation(
    ts: &crate::tenant::session::TenantSession,
    e: SprigError,
) -> SprigError {
    let mapped = sprig_logic::triggers::map_check_error(e);
    if mapped.code() == Some(SprigCode::LogicValidationFailed)
        && mapped
            .message
            .starts_with("rejected by a validation (constraint")
    {
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        tokio::spawn(async move {
            let Ok(ep) = svc.locator.writer(tenant).await else {
                return;
            };
            let Ok(mut c) = ep.pool.get_conn().await else {
                return;
            };
            if let Err(e) = sprig_logic::triggers::load_checks(&mut c, ep.store.schema()).await {
                tracing::debug!(error = %e, "validation messages not loaded");
            }
        });
    }
    mapped
}

/// Loads the tenant's validation messages once per process (and again
/// after a failure named a constraint this process didn't know).
pub(super) fn warm_validations(ts: &crate::tenant::session::TenantSession) {
    if !sprig_logic::triggers::checks_wanted(ts.tenant.0) {
        return;
    }
    let svc = Arc::clone(ts.services());
    let tenant = ts.tenant;
    tokio::spawn(async move {
        let Ok(ep) = svc.locator.writer(tenant).await else {
            return;
        };
        let Ok(mut c) = ep.pool.get_conn().await else {
            return;
        };
        // No logic catalog yet is fine: nothing to load.
        let _ = sprig_logic::triggers::load_checks(&mut c, ep.store.schema()).await;
    });
}
