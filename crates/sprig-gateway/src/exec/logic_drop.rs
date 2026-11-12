//! `DROP FUNCTION | PROCEDURE | VALIDATION | TRIGGER | SCHEDULE` of logic
//! modules in a tenant session (chapter 41, "Deployment and removal";
//! `sprig_logic::drop`; R7 follow-ups).
//!
//! ```text
//! the module of that kind and name (none: a FUNCTION, PROCEDURE or
//!   TRIGGER is ordinary DDL; a VALIDATION or SCHEDULE is 39860, or a note
//!   with IF EXISTS)
//! ALTER ROUTINE (functions, procedures, schedules) or TRIGGER (validations,
//!   triggers) on the database, or CREATE ROUTINE
//! functions and procedures: dependents → 39875, or CASCADE drops the
//!   schedules that call it
//! every shard (sprig_ctl): the routines, packages and MariaDB triggers of
//!   every version; a validation's CHECK through chapter 21's ALTER TABLE
//! the catalog: dropped, versions retired, rollout ended, index entries
//! ```
//!
//! `SHOW DEPENDENTS OF FUNCTION | PROCEDURE [db.]name` (ST67) answers what
//! the drop would find, as rows (`Kind`, `Name`, `Cascade`, `Detail`), so
//! clients needn't read 39875's text.

use std::collections::HashMap;
use std::sync::Arc;

use sprig_catalog::CatalogLocator;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::catalog::ModuleRow;
use sprig_logic::declare::Kind;
use sprig_logic::deploy::ShardSessions;
use sprig_logic::drop::{DropStmt, ShowDependents};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use super::logic_bound::CtlShards;
use super::tenant::{Step, scrub_typed};
use crate::pipeline::{ExecCtx, ResultSink};
use crate::tenant::session::LocalWarning;

fn catalog_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

impl PassSession {
    /// A `DROP` of a logic module; `None` when it names no logic module
    /// and is ordinary DDL.
    pub(super) async fn tenant_logic_drop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: Result<DropStmt>,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let r = match d {
            Ok(d) => Box::pin(self.logic_drop(cx, &d)).await,
            Err(e) => Err(e),
        };
        if let Some(ts) = self.tenant.as_deref() {
            super::logic_rollout::invalidate(ts.tenant.0);
        }
        match r {
            Ok(None) => Ok(None),
            Ok(Some(notes)) => {
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
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Err(e) => {
                let shown = SprigError {
                    kind: e.kind,
                    message: e.message.clone(),
                    retryable: e.retryable,
                    source: None,
                };
                out.error(&scrub_typed(shown)).await?;
                Ok(Some(Step {
                    errored: true,
                    status: None,
                }))
            }
        }
    }

    #[allow(clippy::too_many_lines)] // lookup, rights, dependents, shards, catalog
    async fn logic_drop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: &DropStmt,
    ) -> Result<Option<Vec<String>>> {
        let ordinary = matches!(d.kind, Kind::Function | Kind::Procedure | Kind::Trigger);
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let Some(db) = d.db.clone().or(current) else {
            return if ordinary {
                Ok(None)
            } else {
                Err(SprigError::standard(
                    1046,
                    *b"3D000",
                    "No database selected",
                ))
            };
        };
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let Some(row) = catalog
            .database(&db)
            .or_else(|| catalog.database(&db.to_ascii_lowercase()))
            .cloned()
        else {
            return if ordinary {
                Ok(None)
            } else {
                Err(SprigError::standard(
                    1049,
                    *b"42000",
                    format!("Unknown database '{db}'"),
                ))
            };
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let ep = svc.locator.writer(tenant).await?;
        let store = sprig_logic::LogicStore::new(ep.store.schema());
        let mut conn = ep.pool.get_conn().await.map_err(catalog_err)?;
        let module = match store
            .module(&mut conn, row.db_id.0, d.kind.as_str(), &d.name)
            .await
        {
            Ok(m) => m,
            Err(e) if format!("{e:?}").contains("1146") => None,
            Err(e) => return Err(e),
        };
        let Some(m) = module else {
            if ordinary {
                return Ok(None);
            }
            if d.if_exists {
                return Ok(Some(vec![format!(
                    "{} {} doesn't exist",
                    d.kind.as_str(),
                    d.name
                )]));
            }
            return Err(SprigError::new(
                SprigCode::LogicNotFound,
                format!("no {} {} in {}", d.kind.as_str(), d.name, row.name),
            ));
        };
        // Chapter 29: dropping code needs the right that created it.
        {
            use sprig_auth::authz::{Action, Target};
            let grants = crate::tenant::authz::privileges(ts, &catalog);
            let db_key = sprig_catalog::ddl::database_key(&row.name, ts.lower_case_table_names());
            let on = Target::Database { db_key: &db_key };
            let ok = grants.unrestricted()
                || grants.allows(Action::AlterRoutine, on)
                || grants.allows(Action::CreateRoutine, on)
                || (matches!(d.kind, Kind::Validation | Kind::Trigger)
                    && grants.allows(Action::Trigger, on));
            if !ok {
                return Err(SprigError::standard(
                    1044,
                    *b"42000",
                    format!(
                        "Access denied for user '{}' to database '{}'",
                        ts.principal.user, row.name
                    ),
                ));
            }
        }
        let shard_list = super::logic::shard_list(ts, &self.target.cluster, &row.name, &row)?;
        let mut shards = CtlShards {
            list: shard_list,
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let mut notes = Vec::new();
        // What uses a function or procedure.
        let mut also: Vec<ModuleRow> = Vec::new();
        if matches!(d.kind, Kind::Function | Kind::Procedure) {
            let deps = sprig_logic::drop::dependents(&mut conn, &store, row.db_id.0, &row.name, &m)
                .await?;
            if let Some(e) = deps.refusal(d.kind, &d.name, d.cascade) {
                return Err(e);
            }
            for (id, name) in &deps.schedules {
                if let Some(s) = store.module_by_id(&mut conn, *id).await? {
                    also.push(s);
                    notes.push(format!("schedule {name} dropped with it (CASCADE)"));
                }
            }
            for (_, col) in &deps.columns {
                notes.push(format!(
                    "computed column {col} keeps its own expression; its link to {} is removed",
                    d.name
                ));
            }
            for c in &deps.callers {
                notes.push(format!(
                    "{c} calls {}: such calls now fail with 39860",
                    d.name
                ));
            }
        }
        // A validation's CHECK constraints go through chapter 21's ALTER
        // TABLE path, as the deploy added them.
        let binding = store.binding(&mut conn, m.module_id).await?;
        let versions: Vec<u64> = store
            .versions(&mut conn, m.module_id)
            .await?
            .iter()
            .map(|v| v.version)
            .collect();
        if d.kind == Kind::Validation
            && let Some(b) = &binding
            && b.enforcement.as_deref() == Some("check")
            && let Some(obj) = b.object_id
        {
            let table: Option<String> = {
                use mysql_async::prelude::Queryable;
                conn.exec_first(
                    format!(
                        "SELECT name FROM {}.objects WHERE object_id = ?",
                        sprig_common::names::quote(ep.store.schema())
                    ),
                    (obj,),
                )
                .await
                .map_err(catalog_err)?
            };
            if let Some(t) = table {
                let names: Vec<String> = versions
                    .iter()
                    .map(|v| sprig_logic::enforce::check_name(b.binding_id, *v))
                    .collect();
                let alter = sprig_logic::enforce::check_alter(&row.name, &t, None, &names);
                self.drop_alter(cx, &alter).await?;
            }
        }
        // Every shard: routines, packages, MariaDB triggers.
        let mut objects = sprig_logic::drop::routine_objects(&m, &versions);
        if let Some(b) = &binding
            && b.enforcement.as_deref() == Some("trigger")
        {
            let events: Vec<String> = ["insert", "update", "delete"]
                .iter()
                .map(|e| (*e).to_owned())
                .collect();
            for v in &versions {
                for (n, _) in sprig_logic::enforce::trigger_names(b.binding_id, *v, &events) {
                    objects.insert(0, ("TRIGGER".to_owned(), n));
                }
            }
        }
        for s in &also {
            let vs: Vec<u64> = store
                .versions(&mut conn, s.module_id)
                .await?
                .iter()
                .map(|v| v.version)
                .collect();
            objects.splice(0..0, sprig_logic::drop::routine_objects(s, &vs));
        }
        for schema in shards.schemas() {
            let sess = shards.session(&schema).await?;
            sprig_logic::deploy::drop_objects(sess, &schema, &objects).await?;
        }
        let dc = svc.locator.dc_store();
        for s in &also {
            sprig_logic::drop::module_dropped(&mut conn, &store, &dc, tenant, s, catalog.version)
                .await?;
        }
        sprig_logic::drop::module_dropped(&mut conn, &store, &dc, tenant, &m, catalog.version)
            .await?;
        notes.insert(
            0,
            format!(
                "{} {} dropped ({} version{})",
                d.kind.as_str(),
                d.name,
                versions.len(),
                if versions.len() == 1 { "" } else { "s" }
            ),
        );
        Ok(Some(notes))
    }

    /// `SHOW DEPENDENTS OF FUNCTION | PROCEDURE [db.]name` (ST67): what
    /// `DROP ... RESTRICT` would refuse with 39875, one row each, with what
    /// `CASCADE` does to it. The same module and the same rights as the
    /// drop (`ALTER ROUTINE` or `CREATE ROUTINE` on the database).
    pub(super) async fn tenant_logic_dependents(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: Result<ShowDependents>,
        more: bool,
        format: crate::pipeline::RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let r = match d {
            Ok(d) => Box::pin(self.logic_dependents(&d)).await,
            Err(e) => Err(e),
        };
        let rows = match r {
            Ok(rows) => rows,
            Err(e) => return self.fail(out, &e).await,
        };
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let rows = sprig_types::Rows {
            columns: sprig_logic::drop::DEPENDENTS_COLUMNS
                .iter()
                .map(|n| crate::pipeline::text_column(n, cx.proto.collation))
                .collect(),
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|c| Some(c.into_bytes())).collect())
                .collect(),
        };
        super::types::send_rows(out, &rows, format, status).await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    async fn logic_dependents(&mut self, d: &ShowDependents) -> Result<Vec<[String; 4]>> {
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db =
            d.db.clone()
                .or(current)
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let row = catalog
            .database(&db)
            .or_else(|| catalog.database(&db.to_ascii_lowercase()))
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
            })?;
        {
            use sprig_auth::authz::{Action, Target};
            let grants = crate::tenant::authz::privileges(ts, &catalog);
            let db_key = sprig_catalog::ddl::database_key(&row.name, ts.lower_case_table_names());
            let on = Target::Database { db_key: &db_key };
            if !(grants.unrestricted()
                || grants.allows(Action::AlterRoutine, on)
                || grants.allows(Action::CreateRoutine, on))
            {
                return Err(SprigError::standard(
                    1044,
                    *b"42000",
                    format!(
                        "Access denied for user '{}' to database '{}'",
                        ts.principal.user, row.name
                    ),
                ));
            }
        }
        let svc = Arc::clone(ts.services());
        let ep = svc.locator.writer(ts.tenant).await?;
        let store = sprig_logic::LogicStore::new(ep.store.schema());
        let mut conn = ep.pool.get_conn().await.map_err(catalog_err)?;
        let module = match store
            .module(&mut conn, row.db_id.0, d.kind.as_str(), &d.name)
            .await
        {
            Ok(m) => m,
            Err(e) if format!("{e:?}").contains("1146") => None,
            Err(e) => return Err(e),
        };
        let m = module.ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no {} {} in {}", d.kind.as_str(), d.name, row.name),
            )
        })?;
        let deps =
            sprig_logic::drop::dependents(&mut conn, &store, row.db_id.0, &row.name, &m).await?;
        Ok(deps.rows())
    }

    /// Runs an `ALTER TABLE` of the drop through the session, as a
    /// client's (chapter 21's path).
    async fn drop_alter(&mut self, cx: &mut ExecCtx<'_>, alter: &str) -> Result<()> {
        let wire = crate::pipeline::Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let r = self
            .engine_db(
                cx,
                sprig_logic::engine::bridge::DbOp {
                    shape: sprig_logic::engine::bridge::Shape::Exec,
                    sql: alter.to_owned(),
                },
                wire,
            )
            .await;
        r.map(|_| ())
    }
}
