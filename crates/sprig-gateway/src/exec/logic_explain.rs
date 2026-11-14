//! `EXPLAIN FUNCTION | PROCEDURE [db.]name [FORMAT = SQL]` in a tenant
//! session: one row per stored version that
//! isn't retired or superseded, newest first, with its state, tier and
//! placement (the reasons with their source locations, the route, the
//! tables read and written, the cost), as `sprigctl logic compile` prints
//! a fresh one ([`sprig_logic::placement::explain_stored`]). `FORMAT = SQL`
//! adds the generated PL/SQL.
//!
//! ```text
//! the module of that kind and name in the database (none: 39860)
//! EXECUTE, ALTER ROUTINE or CREATE ROUTINE on the database (FORMAT = SQL:
//!   ALTER ROUTINE), else 1044
//! logic_versions of the module, state not retired or superseded
//! ```

use std::sync::Arc;

use sprig_catalog::CatalogLocator;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::placement::{EXPLAIN_COLUMNS, ExplainFunction, explain_stored};
use sprig_proto_mysql::StatusFlags;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink};

fn catalog_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

impl PassSession {
    /// Answers `EXPLAIN FUNCTION | PROCEDURE` (module docs).
    pub(super) async fn tenant_logic_explain(
        &mut self,
        cx: &mut ExecCtx<'_>,
        e: Result<ExplainFunction>,
        more: bool,
        format: crate::pipeline::RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let sql = e.as_ref().is_ok_and(|e| e.sql);
        let r = match e {
            Ok(e) => Box::pin(self.logic_explain(&e)).await,
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
        let n = if sql { 5 } else { 4 };
        let rows = sprig_types::Rows {
            columns: EXPLAIN_COLUMNS[..n]
                .iter()
                .map(|c| crate::pipeline::text_column(c, cx.proto.collation))
                .collect(),
            rows: rows
                .into_iter()
                .map(|r| {
                    r.into_iter()
                        .take(n)
                        .map(|c| c.map(String::into_bytes))
                        .collect()
                })
                .collect(),
        };
        super::types::send_rows(out, &rows, format, status).await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    async fn logic_explain(&mut self, e: &ExplainFunction) -> Result<Vec<[Option<String>; 5]>> {
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db =
            e.db.clone()
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
            let allowed = grants.unrestricted()
                || grants.allows(Action::AlterRoutine, on)
                || (!e.sql
                    && (grants.allows(Action::CreateRoutine, on)
                        || grants.allows(Action::Execute, on)));
            if !allowed {
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
            .module(&mut conn, row.db_id.0, e.kind.as_str(), &e.name)
            .await
        {
            Ok(m) => m,
            // No logic tables yet: no module.
            Err(err) if format!("{err:?}").contains("1146") => None,
            Err(err) => return Err(err),
        };
        let m = module.ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no {} {} in {}", e.kind.as_str(), e.name, row.name),
            )
        })?;
        let mut out = Vec::new();
        for v in store.versions(&mut conn, m.module_id).await? {
            if matches!(v.state.as_str(), "retired" | "superseded") {
                continue;
            }
            let plsql = if e.sql {
                store.plsql(&mut conn, m.module_id, v.version).await?
            } else {
                None
            };
            let state = if m.active_version == Some(v.version) {
                format!("{} (live)", v.state)
            } else {
                v.state.clone()
            };
            out.push([
                Some(v.version.to_string()),
                Some(state),
                Some(v.tier.clone()),
                Some(explain_stored(&v.tier, &v.placement)),
                plsql,
            ]);
        }
        Ok(out)
    }
}
