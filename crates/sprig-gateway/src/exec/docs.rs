//! The `sprig_docs` step of a tenant statement: statements naming the
//! virtual docs database are answered on the docs-reader account
//! (`crate::docs::sql` has the rules), never on the tenant's.

use std::time::Instant;

use bytes::Bytes;
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Value};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};

use super::PassSession;
use super::tenant::Step;
use crate::docs::sql::{self, Kind, Refusal};
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

fn column(c: &Column) -> ColumnDef {
    let logical = |b: &[u8]| -> Bytes {
        if b == sprig_docs::table::SCHEMA.as_bytes() {
            Bytes::from_static(sprig_docs::table::VIRTUAL_DATABASE.as_bytes())
        } else {
            Bytes::copy_from_slice(b)
        }
    };
    ColumnDef {
        schema: logical(c.schema_ref()),
        table: Bytes::copy_from_slice(c.table_ref()),
        org_table: Bytes::copy_from_slice(c.org_table_ref()),
        name: Bytes::copy_from_slice(c.name_ref()),
        org_name: Bytes::copy_from_slice(c.org_name_ref()),
        ext_type: None,
        charset: c.character_set(),
        length: c.column_length(),
        col_type: c.column_type() as u8,
        flags: c.flags().bits(),
        decimals: c.decimals(),
    }
}

impl PassSession {
    /// Answers a statement that names `sprig_docs`; `None` for any other.
    pub(super) async fn tenant_docs(
        &mut self,
        _cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !sql::mentions(text) {
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        // Sandbox sessions and the gateway's own statements go on as usual.
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        let (user, host, tenant) = (
            ts.principal.user.clone(),
            ts.principal.host.clone(),
            ts.principal.tenant,
        );
        let (lex, _) = self.lex_opts();
        let analysis = match sql::analyze(text, &lex) {
            None => return Ok(None),
            Some(Ok(a)) => a,
            Some(Err(Refusal::Write)) => {
                metrics::counter!("sprig_docs_search_total", "result" => "refused").increment(1);
                return self.fail(out, &sql::write_denied(&user, &host)).await.map(Some);
            }
            Some(Err(Refusal::Error(e))) => {
                metrics::counter!("sprig_docs_search_total", "result" => "invalid").increment(1);
                return self.fail(out, &e).await.map(Some);
            }
        };
        if format == RowFormat::Binary {
            let e = SprigError::new(
                SprigCode::NameRewriteUnsupported,
                "sprig_docs in a prepared statement: run it as a plain statement",
            );
            return self.fail(out, &e).await.map(Some);
        }
        let started = Instant::now();
        let r = self.docs_rows(tenant, &analysis, text, &user, &host).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let result = match &r {
            Ok(_) => "ok",
            Err(e) if e.code() == Some(SprigCode::SearchUnavailable) => "unavailable",
            Err(_) => "error",
        };
        metrics::counter!("sprig_docs_search_total", "result" => result).increment(1);
        if analysis.kind == Kind::Read {
            metrics::histogram!("sprig_docs_search_seconds", "mode" => "sql")
                .record(started.elapsed().as_secs_f64());
        }
        match r {
            Err(e) => self.fail(out, &e).await.map(Some),
            Ok(None) => {
                out.ok(OkPacket::new(status)).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Ok(Some(rows)) => {
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
        }
    }

    /// Runs the rewritten statement as `sprig_docs_r`: its rows, or `None`
    /// for an OK.
    async fn docs_rows(
        &mut self,
        tenant: Option<sprig_common::ids::TenantId>,
        analysis: &sql::Analysis,
        text: &[u8],
        user: &str,
        host: &str,
    ) -> Result<Option<sprig_types::Rows>> {
        let svc = crate::docs::service()
            .ok_or_else(|| crate::docs::unavailable("this gateway has no tenant logins"))?;
        let mut embeds = Vec::new();
        for t in sql::embed_texts(analysis) {
            let v = svc.embed_query(t).await.ok_or_else(|| {
                crate::docs::unavailable(
                    "sprig_docs.embed needs the embedding model (docs.embed_queries, docs.model_dir)",
                )
            })?;
            embeds.push(v);
        }
        let mut t = svc.target(tenant, None).await?;
        let stmt = sql::render(text, analysis, &t.version, &embeds);
        let server = |e: mysql_async::Error| match e {
            mysql_async::Error::Server(s) => {
                let mut state = [0u8; 5];
                for (d, b) in state.iter_mut().zip(s.state.bytes()) {
                    *d = b;
                }
                sql::client_error(s.code, state, &s.message, user, host)
            }
            e => SprigError::new(SprigCode::BackendUnavailable, "docs: the read failed")
                .with_source(e),
        };
        let mut res = t.conn.query_iter(stmt).await.map_err(server)?;
        let columns: Vec<ColumnDef> = res
            .columns()
            .map(|c| c.iter().map(column).collect())
            .unwrap_or_default();
        let rows: Vec<mysql_async::Row> = res.collect().await.map_err(server)?;
        drop(res);
        if columns.is_empty() {
            return Ok(None);
        }
        let rows = rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        Value::NULL => None,
                        Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect();
        Ok(Some(sprig_types::Rows { columns, rows }))
    }
}
