//! Logic declarations in a tenant session (chapter 41, "Declaring code";
//! R7 order 1 and 2): `CREATE FUNCTION | PROCEDURE ... LANGUAGE kirun |
//! javascript AS ...` is compiled, placed, recorded in the tenant's logic
//! catalog and deployed into the database's shard schema by
//! `sprig_logic::service`, on this session's own backend connection, so
//! the routines' definer is the session's backend account (chapter 29,
//! decision A9) and MariaDB checks its `CREATE ROUTINE` right.
//!
//! The client gets OK with one note per placement line (`SHOW WARNINGS`):
//! the module, version, tier, and each reason the code runs in the engine
//! tier.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_catalog::CatalogLocator;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::deploy::Session;
use sprig_logic::service::{self, Target};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::client::PacketKind;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use super::tenant::{Step, scrub_typed};
use crate::backend::{BackendSession, StmtInfo};
use crate::pipeline::{ExecCtx, ResultSink};
use crate::tenant::session::LocalWarning;

/// `logic.function_budget_ms`'s default (chapter 41).
const FUNCTION_BUDGET_MS: u64 = 5000;
/// `logic.max_loop_iterations`'s default.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
/// `logic.compile_max_cost`'s default.
const COMPILE_MAX_COST: u64 = 100_000;

/// The session's backend connection as a [`Session`].
struct Bs<'a>(&'a mut BackendSession);

impl Bs<'_> {
    async fn run(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let bs = &mut *self.0;
        bs.query(&StmtInfo::write(), sql.as_bytes()).await?;
        let mut rows = Vec::new();
        let mut cols = 0usize;
        let mut err = None;
        while let Some(p) = bs.next().await? {
            match p.kind {
                PacketKind::ColumnCount(n) => cols = usize::try_from(n).unwrap_or(0),
                PacketKind::Row => {
                    let vals = sprig_proto_mysql::resultset::decode_text_row(&p.payload, cols)?;
                    rows.push(
                        vals.into_iter()
                            .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
                            .collect(),
                    );
                }
                PacketKind::Err(e) => {
                    err = Some(SprigError::backend(e.code, e.sqlstate, e.message.clone()));
                }
                _ => {}
            }
        }
        let _ = bs.finish().await;
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }
}

#[async_trait]
impl Session for Bs<'_> {
    async fn exec(&mut self, sql: &str) -> Result<()> {
        self.run(sql).await.map(|_| ())
    }

    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.run(sql).await
    }
}

impl PassSession {
    /// A logic declaration (see the module docs).
    pub(super) async fn tenant_logic(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        match self.logic_deploy(cx, text).await {
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

    async fn logic_deploy(&mut self, cx: &mut ExecCtx<'_>, text: &Bytes) -> Result<Vec<String>> {
        let decl = sprig_logic::declare::parse(&String::from_utf8_lossy(text))?;
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let current = ts.current.clone();
        let db = decl
            .db
            .clone()
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
        let schema = ts.schema_of(Some(&db)).ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicUnsupported,
                format!(
                    "database {db} has more than one shard; logic deploys need chapter 21's engine"
                ),
            )
        })?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let _ = self.bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        let mut session = Bs(bs);
        let outcome = service::deploy(
            &decl,
            Target {
                catalog: &mut conn,
                catalog_schema: ep.store.schema(),
                catalog_version: catalog.version,
                db_id: row.db_id.0,
                shard_schema: &schema,
                single_shard: true,
                user_id: user.0,
                budget_ms: FUNCTION_BUDGET_MS,
                max_loop_iterations: MAX_LOOP_ITERATIONS,
                compile_max_cost: COMPILE_MAX_COST,
            },
            &mut session,
        )
        .await?;
        Ok(outcome.notes())
    }
}
