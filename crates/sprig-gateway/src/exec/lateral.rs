//! `LATERAL`'s gateway apply on port 3306 (chapter 28, "`LATERAL`" step 3;
//! R4 round 4).
//!
//! A `LATERAL` derived table the text rewrite can't express as SQL
//! (`sprig_sql::compat::lateral::rewrite`: any correlation other than
//! equalities, `GROUP BY` or `DISTINCT` inside, ...) is applied here: the
//! distinct outer values the subquery reads, then the subquery once per
//! value tuple in batches of [`lateral::BATCH`], then the statement with
//! those rows as a literal derived table joined on the values
//! (`sprig_sql::compat::lateral::apply_plan`). Every step runs through
//! this session, so names, privileges, row-level security and its
//! transaction apply as to the client's own statement. More than
//! [`lateral::MAX_OUTER_ROWS`] value tuples, or rows past
//! [`MAX_INLINE_BYTES`], fail with 39601 `ER_SPRIG_TRANSLATE_LIMIT`.

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_sql::compat::lateral;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

/// The largest derived table the apply inlines.
const MAX_INLINE_BYTES: usize = 8 << 20;

fn limit(what: &str) -> SprigError {
    SprigError::standard(
        39601,
        *b"HY000",
        format!("ER_SPRIG_TRANSLATE_LIMIT: this LATERAL needs the gateway's apply, and {what}"),
    )
}

/// Whether a column type is numeric (its values inline unquoted).
fn numeric(t: u8) -> bool {
    use sprig_proto_mysql::resultset::column_type as ct;
    matches!(
        t,
        ct::DECIMAL
            | ct::TINY
            | ct::SHORT
            | ct::LONG
            | ct::FLOAT
            | ct::DOUBLE
            | ct::LONGLONG
            | ct::INT24
            | ct::NEWDECIMAL
            | ct::YEAR
    )
}

impl PassSession {
    /// Applies the statement's `LATERAL` in the gateway; `None` when it
    /// has none the plan can carry.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tenant_lateral_apply(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if self.persona != Persona::MySql84 {
            return Ok(None);
        }
        let (lex, _) = self.lex_opts();
        let Some(plan) = lateral::apply_plan(text, &lex) else {
            return Ok(None);
        };
        let nbe = lex.no_backslash_escapes;
        let built: Result<String> = async {
            let keys = self.run_checked(cx, &plan.keys_sql, false).await?;
            if keys.rows.len() > lateral::MAX_OUTER_ROWS {
                return Err(limit(&format!(
                    "its outer rows have more than {} distinct values",
                    lateral::MAX_OUTER_ROWS
                )));
            }
            let key_lits: Vec<Vec<String>> = if keys.rows.is_empty() {
                // No outer row: one all-NULL run gives the columns.
                vec![vec!["NULL".to_owned(); plan.outer.len()]]
            } else {
                keys.rows
                    .iter()
                    .map(|r| {
                        r.iter()
                            .enumerate()
                            .map(|(i, v)| {
                                let num = keys.types.get(i).copied().is_some_and(numeric);
                                lateral::literal(v.as_deref(), num, nbe)
                            })
                            .collect()
                    })
                    .collect()
            };
            let mut columns: Vec<String> = Vec::new();
            let mut parts: Vec<String> = Vec::new();
            let mut size = 0usize;
            for chunk in key_lits.chunks(lateral::BATCH) {
                let got = self.run_checked(cx, &plan.batch_sql(chunk), false).await?;
                if columns.is_empty() {
                    columns.clone_from(&got.columns);
                }
                if keys.rows.is_empty() {
                    break;
                }
                for row in &got.rows {
                    let vals: Vec<String> = row
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let num = got.types.get(i).copied().is_some_and(numeric);
                            let lit = lateral::literal(v.as_deref(), num, nbe);
                            if parts.is_empty() {
                                format!("{lit} AS `{}`", columns[i].replace('`', "``"))
                            } else {
                                lit
                            }
                        })
                        .collect();
                    let p = format!("SELECT {}", vals.join(", "));
                    size += p.len();
                    if size > MAX_INLINE_BYTES {
                        return Err(limit(&format!(
                            "its rows are larger than {MAX_INLINE_BYTES} bytes"
                        )));
                    }
                    parts.push(p);
                }
            }
            let derived = if parts.is_empty() {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| format!("NULL AS `{}`", c.replace('`', "``")))
                    .collect();
                format!("(SELECT {} FROM DUAL WHERE 0)", cols.join(", "))
            } else {
                format!("({})", parts.join(" UNION ALL "))
            };
            Ok(plan.final_sql(&derived))
        }
        .await;
        match built {
            Ok(sql) => {
                metrics::counter!("sprig_translator_rules_applied_total", "rule" => "lateral_apply")
                    .increment(1);
                let sql = Bytes::from(sql);
                Box::pin(self.tenant_text(cx, &sql, attrs, more, format, out))
                    .await
                    .map(Some)
            }
            Err(e) => self.fail(out, &e).await.map(Some),
        }
    }
}
