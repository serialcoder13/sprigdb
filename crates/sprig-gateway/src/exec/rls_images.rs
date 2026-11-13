//! Row images checked against a session's privileges and row-level
//! security (chapter 29, "Change feed consumers"; RLS round 5, decisions
//! RL44 to RL46): what a change stream over gRPC, SSE or WebSocket, or a
//! Redis keyspace notification, may show its subscriber.
//!
//! A caller in this process (the API's `Changes.Watch`, the Redis front
//! end) sends one marked statement through a session of the subscriber,
//! `/* sprig rls images <nonce> */ <ImagesRequest as JSON>`; the nonce is
//! this process's own, so no client can send it. The answer is one row per
//! image, `(t, i, v)`:
//!
//! ```text
//! table t of the request, image i:
//!   no SELECT on the table (or no such table)  → v = 0
//!   `watch` asked and no WATCH on the table     → v = 0
//!   image null                                 → v = 1 (the privilege alone)
//!   the table's policies don't bind the session → v = 1
//!   bound: the session's SELECT policies, rendered with its session values
//!     (sprig_gateway::tenant::rls_streams::record_filter), evaluated by
//!     MariaDB over the image as a row of the table → v = 0 or 1
//! ```

use std::sync::OnceLock;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_auth::authz::{Action, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::StatusFlags;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

const MARK: &str = "/* sprig rls images ";

fn nonce() -> &'static str {
    static N: OnceLock<String> = OnceLock::new();
    N.get_or_init(|| {
        format!(
            "{}{}",
            uuid::Uuid::now_v7().simple(),
            uuid::Uuid::now_v7().simple()
        )
    })
}

/// One table's images.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableImages {
    /// The logical database.
    pub db: String,
    /// The table (a collection's or key-value namespace's table too).
    pub table: String,
    /// Each image as a JSON object of column values, or `null` for the
    /// privilege alone.
    pub images: Vec<Option<serde_json::Map<String, serde_json::Value>>>,
}

/// What is asked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImagesRequest {
    /// The tables.
    pub tables: Vec<TableImages>,
    /// Binary string columns carry their bytes as plain JSON strings (a
    /// key-value key), not base64 as change records do.
    #[serde(default)]
    pub plain_binary: bool,
    /// Also `WATCH` on each table (chapters 29 and 38: an application
    /// change stream needs it); `Changes.Watch` sets it, keyspace
    /// notifications don't.
    #[serde(default)]
    pub watch: bool,
}

/// The marked statement of `req`.
#[must_use]
pub(crate) fn statement(req: &ImagesRequest) -> String {
    format!(
        "{MARK}{} */ {}",
        nonce(),
        serde_json::to_string(req).unwrap_or_default()
    )
}

/// The answer as `[table][image] → visible`, from the statement's rows
/// `(t, i, v)` as text.
#[must_use]
pub(crate) fn answer(req: &ImagesRequest, rows: &[Vec<Option<String>>]) -> Vec<Vec<bool>> {
    let mut out: Vec<Vec<bool>> = req
        .tables
        .iter()
        .map(|t| vec![false; t.images.len()])
        .collect();
    for r in rows {
        let n = |i: usize| {
            r.get(i)
                .cloned()
                .flatten()
                .and_then(|v| v.parse::<usize>().ok())
        };
        if let (Some(t), Some(i), Some(v)) = (n(0), n(1), n(2))
            && let Some(slot) = out.get_mut(t).and_then(|x| x.get_mut(i))
        {
            *slot = v == 1;
        }
    }
    out
}

impl PassSession {
    /// The marked statement (see the module docs): `None` for any other.
    pub(super) async fn tenant_rls_images(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !text.starts_with(MARK.as_bytes()) || self.tenant.is_none() {
            return Ok(None);
        }
        let s = String::from_utf8_lossy(&text[MARK.len()..]).into_owned();
        let Some(req) = s
            .split_once(" */ ")
            .filter(|(n, _)| *n == nonce())
            .and_then(|(_, j)| serde_json::from_str::<ImagesRequest>(j).ok())
        else {
            let e = SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: an image check isn't a client statement",
            );
            return Ok(Some(self.fail(out, &e).await?));
        };
        let rows = match self.check_images(cx, &req).await {
            Ok(r) => r,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let col = |n: &str| crate::pipeline::text_column(n, cx.proto.collation);
        let rows = sprig_types::Rows {
            columns: vec![col("t"), col("i"), col("v")],
            rows: rows
                .into_iter()
                .map(|(t, i, v)| {
                    vec![
                        Some(t.to_string().into_bytes()),
                        Some(i.to_string().into_bytes()),
                        Some(if v { b"1".to_vec() } else { b"0".to_vec() }),
                    ]
                })
                .collect(),
        };
        super::types::send_rows(out, &rows, RowFormat::Text, status).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    /// `(table, image, visible)` for every image of `req`.
    #[allow(clippy::too_many_lines)] // one pass per table, in order
    async fn check_images(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &ImagesRequest,
    ) -> Result<Vec<(usize, usize, bool)>> {
        let (lex, _) = self.lex_opts();
        let current = self.current_logical();
        let mut out = Vec::new();
        for (ti, t) in req.tables.iter().enumerate() {
            let none = |out: &mut Vec<(usize, usize, bool)>| {
                for i in 0..t.images.len() {
                    out.push((ti, i, false));
                }
            };
            let (cat, lctn) = {
                let ts = self
                    .tenant
                    .as_deref_mut()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                (ts.refresh().await?, ts.lower_case_table_names())
            };
            let db_key = database_key(&t.db, lctn);
            let Some(db) = cat
                .database(&db_key)
                .filter(|d| d.state == DbState::Active)
                .cloned()
            else {
                none(&mut out);
                continue;
            };
            let Some(obj) = cat
                .object(
                    db.db_id,
                    Namespace::Relation,
                    &object_key(ObjectKind::Table, &t.table, lctn),
                )
                .filter(|o| o.state == ObjectState::Active)
                .cloned()
            else {
                none(&mut out);
                continue;
            };
            let (filter, slot) = {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                let g = crate::tenant::authz::privileges(ts, &cat);
                let on = Target::Object {
                    db_key: &db_key,
                    db: db.db_id,
                    object: obj.object_id,
                };
                if !g.unrestricted()
                    && (!g.allows(Action::Select, on)
                        || (req.watch && !g.allows(Action::Watch, on)))
                {
                    none(&mut out);
                    continue;
                }
                let bound = !g.unrestricted() && g.row_filter(on, Action::Select).is_some();
                let slot = if bound {
                    let template = ts.schema_of(Some(&db.name));
                    let layout = super::router::ddl_layout(ts);
                    match (&layout, &template) {
                        (Some(l), Some(tm)) => l(tm, Some(&obj.name))
                            .filter(|v| !v.is_empty())
                            .and_then(|v| v.into_iter().next()),
                        _ => None,
                    }
                    .or_else(|| template.map(|tm| (tm, self.target.cluster.clone())))
                } else {
                    None
                };
                (bound, slot)
            };
            if !filter {
                for i in 0..t.images.len() {
                    out.push((ti, i, true));
                }
                continue;
            }
            let Some((schema, cluster)) = slot else {
                none(&mut out);
                continue;
            };
            let f = {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                let columns = crate::tenant::rls_streams::filter_columns(
                    ts.services(),
                    cat.tenant.0,
                    &obj,
                    &cluster,
                    &schema,
                )
                .await?;
                let columns: Vec<_> = if req.plain_binary {
                    columns
                        .iter()
                        .map(|c| match &c.plain {
                            Some(ty) => crate::tenant::rls_streams::FilterColumn {
                                ty: ty.clone(),
                                opaque: false,
                                ..c.clone()
                            },
                            None => c.clone(),
                        })
                        .collect()
                } else {
                    columns.to_vec()
                };
                crate::tenant::rls_streams::record_filter(
                    ts,
                    &cat,
                    &db.name,
                    obj.object_id,
                    &columns,
                    lex,
                    current.as_deref(),
                )
                .await?
            };
            let Some(f) = f else {
                for i in 0..t.images.len() {
                    out.push((ti, i, true));
                }
                continue;
            };
            // Null images are the privilege alone; the others go to MariaDB
            // in one statement.
            let mut parts = Vec::new();
            for (i, img) in t.images.iter().enumerate() {
                match img {
                    None => out.push((ti, i, true)),
                    Some(m) => {
                        let v = serde_json::json!({ "before": m }).to_string();
                        parts.push(format!(
                            "SELECT {i} AS i, {} AS v",
                            sprig_sql::rls::sql_string(&v, lex.no_backslash_escapes)
                        ));
                    }
                }
            }
            if parts.is_empty() {
                continue;
            }
            let sql = format!(
                "SELECT `__sprig_cl`.i, {} FROM ({}) AS `__sprig_cl`",
                sprig_streams::read::image_visible(&f, "$.before"),
                parts.join(" UNION ALL ")
            );
            let c = self.run_checked(cx, &sql, false).await?;
            let mut seen = vec![false; t.images.len()];
            for r in &c.rows {
                let cell = |k: usize| {
                    r.get(k)
                        .cloned()
                        .flatten()
                        .and_then(|b| String::from_utf8(b).ok())
                };
                if let (Some(i), Some(v)) = (cell(0).and_then(|x| x.parse::<usize>().ok()), cell(1))
                    && let Some(slot) = seen.get_mut(i)
                {
                    *slot = v == "1";
                }
            }
            for (i, img) in t.images.iter().enumerate() {
                if img.is_some() {
                    out.push((ti, i, seen[i]));
                }
            }
            metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "images")
                .increment(seen.iter().filter(|v| !**v).count() as u64);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_and_answers() {
        let req = ImagesRequest {
            tables: vec![TableImages {
                db: "shop".into(),
                table: "docs".into(),
                images: vec![None, Some(serde_json::Map::new())],
            }],
            plain_binary: false,
            watch: true,
        };
        let s = statement(&req);
        assert!(s.starts_with(MARK));
        assert!(s.contains("\"watch\":true"));
        assert!(s.contains("\"table\":\"docs\""));
        let rows = vec![
            vec![Some("0".into()), Some("1".into()), Some("1".into())],
            vec![Some("0".into()), Some("0".into()), Some("0".into())],
        ];
        assert_eq!(answer(&req, &rows), vec![vec![false, true]]);
    }
}
