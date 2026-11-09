//! Graph DDL in a tenant session (chapter 43, "Language surface"; build
//! step 2): `CREATE GRAPH [IF NOT EXISTS] [db.]name` and `DROP GRAPH [IF
//! EXISTS] [db.]name` on 3306 and 3307, through [`crate::graphs`].
//! `CREATE PROPERTY GRAPH` and `ALTER GRAPH` answer 39901.
//!
//! `CREATE GRAPH` needs `CREATE` on the database, `DROP GRAPH` needs
//! `DROP` on the graph or its database (chapter 29).

use bytes::Bytes;
use sprig_auth::authz::Action;
use sprig_catalog::Freshness;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::catalog::GraphDdl;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use super::tenant::Step;
use crate::graphs::GatewayGraphs;
use crate::pipeline::ResultSink;

fn mentions_graph(text: &[u8]) -> bool {
    text.len() < 4096 && text.windows(5).any(|w| w.eq_ignore_ascii_case(b"graph"))
}

impl PassSession {
    /// Chapter 43's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't graph DDL.
    pub(super) async fn tenant_graph_ddl(
        &mut self,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !mentions_graph(text) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let ddl = match sprig_graph::catalog::parse_ddl(sql) {
            Ok(None) => return Ok(None),
            Ok(Some(d)) => d,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let current = self.current_logical();
        let r = self.graph_ddl(ddl, current).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        if let Err(e) = r {
            return Ok(Some(self.fail(out, &e).await?));
        }
        out.ok(OkPacket::new(status)).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    async fn graph_ddl(&mut self, ddl: GraphDdl, current: Option<String>) -> Result<()> {
        let Some(ts) = self.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let graphs = GatewayGraphs::new(std::sync::Arc::clone(ts.services()));
        let (tenant, user) = (ts.tenant, ts.principal.user_id);
        let pick = |db: Option<String>| {
            db.or_else(|| current.clone())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))
        };
        let cat = graphs.catalog(tenant, Freshness::Latest).await?;
        let grants = graphs.grants(&cat, user);
        let denied = |a: &str, db: &str, name: &str| {
            SprigError::standard(
                1142,
                *b"42000",
                format!(
                    "{a} command denied to user '{}' for graph '{db}.{name}'",
                    ts.principal.user
                ),
            )
        };
        match ddl {
            GraphDdl::Create {
                db,
                name,
                if_not_exists,
            } => {
                let db = pick(db)?;
                let lctn = graphs.lctn(&cat);
                let Some(row) = cat.database(&sprig_catalog::ddl::database_key(&db, lctn)) else {
                    return Err(SprigError::standard(
                        1049,
                        *b"42000",
                        format!("Unknown database '{db}'"),
                    ));
                };
                if !graphs.allows(
                    grants.as_deref(),
                    &cat,
                    &db,
                    row.db_id,
                    None,
                    Action::Create,
                ) {
                    return Err(denied("CREATE", &db, &name));
                }
                let (_, v) = graphs.create(tenant, &db, &name, if_not_exists).await?;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    let _ = ts
                        .services()
                        .cache
                        .get(tenant, Freshness::Cached { min_version: v })
                        .await;
                }
                Ok(())
            }
            GraphDdl::Drop {
                db,
                name,
                if_exists,
            } => {
                let db = pick(db)?;
                let g = match graphs.resolve(&cat, &db, &name).await {
                    Ok(g) => g,
                    Err(e) if e.code() == Some(SprigCode::GraphNotFound) && if_exists => None,
                    Err(e) => return Err(e),
                };
                if let Some(g) = &g
                    && !graphs.allows(
                        grants.as_deref(),
                        &cat,
                        &g.db,
                        g.db_id,
                        Some(g.graph_id),
                        Action::Drop,
                    )
                {
                    return Err(denied("DROP", &db, &name));
                }
                graphs.drop(tenant, &db, &name, if_exists).await?;
                Ok(())
            }
        }
    }
}
