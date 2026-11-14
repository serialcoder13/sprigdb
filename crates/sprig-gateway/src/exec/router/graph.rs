//! `GRAPH_TABLE` in the router path (the router's `Plan::Graph`).
//!
//! ```text
//! tenant_text (before the pipeline, exec/graph_ddl.rs):
//!   a single SELECT (or EXPLAIN FORMAT=SPRIG SELECT) with GRAPH_TABLE
//!     → its calls kept here (Pending), each replaced by a placeholder
//!   other statements → their rows bound before the pipeline
//! routed (after the name rewrite and the rest of the pipeline):
//!   the text holds this statement's placeholders:
//!     each call's graph resolved: its shards (the router's map cache) and
//!       the executor's EXPLAIN lines (tiers)
//!     sprig_router::plan_graph            → Graph, Explain(Graph) or Reject
//!     Explain → the router's EXPLAIN (graph rows, then the outer plan with
//!               empty rows and one shard's MariaDB plan); no router layout:
//!               the graph rows only
//!     Graph   → each call runs as a Cypher read (GatewayGraphs::query_read:
//!               SELECT on the graph, budgets, read-only transactions), the
//!               rows replace the placeholders, and the statement continues
//!               through the router: plan (narrowed to the rows' key shards),
//!               holds, stale-map retries, gathered reads
//! ```

use bytes::Bytes;
use sprig_catalog::Freshness;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::{StatusFlags, Value};
use sprig_router::graph::{CallRows, GraphCall, GraphLit, GraphPlan, GraphStep, PENDING_FN};
use sprig_router::topology::ShardLoc;
use sprig_router::{Output, Plan, PlanCtx, StaticTopology, Topology};
use sprig_sql::names::{LocalAnswer, NameMap, ResultMapper};

use super::super::PassSession;
use super::super::tenant::{Step, send_local};
use super::Ran;
use crate::graphs::GatewayGraphs;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

/// A statement's `GRAPH_TABLE` calls, kept while its text carries their
/// placeholders.
#[derive(Debug, Clone)]
pub(crate) struct Pending {
    /// The placeholders' nonce.
    pub(crate) nonce: u64,
    /// The calls, as found in the client's text.
    pub(crate) calls: Vec<GraphCall>,
}

/// A bound graph statement: its text, plan and rows (they narrow its read).
#[derive(Debug)]
pub(super) struct Bound {
    pub(super) text: String,
    pub(super) plan: Box<GraphPlan>,
    pub(super) rows: Vec<CallRows>,
}

/// What [`PassSession::graph_bind`] did.
pub(super) enum Bind {
    /// Answered (EXPLAIN, or an error).
    Done(Step),
    /// Run this text through the router.
    Run(Vec<u8>),
}

/// A graph value as the router takes it.
fn lit(v: &sprig_graph::value::Value) -> GraphLit {
    use sprig_graph::value::Value;
    match v {
        Value::Null => GraphLit::Null,
        Value::Int(i) => GraphLit::Int(*i),
        Value::String(s) => GraphLit::Str(s.clone()),
        other => GraphLit::Other(sprig_graph::pgq::literal(other)),
    }
}

/// A prepared statement's value as a Cypher literal, for a `?` inside a
/// `GRAPH_TABLE` call: numbers as numbers, text as a
/// quoted string, dates and times as their text; a value Cypher can't
/// write (NaN, infinity, bytes that aren't UTF-8) is refused.
///
/// # Errors
/// 1210 for such a value.
pub(crate) fn cypher_param(p: &sprig_proto_mysql::command::BoundParam) -> Result<String> {
    use sprig_proto_mysql::command::ParamValue;
    let bad = |what: &str| {
        SprigError::standard(
            1210,
            *b"HY000",
            format!("Incorrect arguments to EXECUTE: {what} can't be a GRAPH_TABLE parameter"),
        )
    };
    let quote = |t: &str| {
        let mut o = String::with_capacity(t.len() + 2);
        o.push('\'');
        for c in t.chars() {
            match c {
                '\'' => o.push_str("\\'"),
                '\\' => o.push_str("\\\\"),
                _ => o.push(c),
            }
        }
        o.push('\'');
        o
    };
    let float = |f: f64| {
        if f.is_finite() {
            Ok(format!("{f:?}"))
        } else {
            Err(bad("a NaN or infinite number"))
        }
    };
    let ParamValue::Value(v) = &p.value else {
        return Ok("null".to_owned());
    };
    match v {
        Value::Null => Ok("null".to_owned()),
        Value::Int(i) => Ok(i.to_string()),
        Value::UInt(u) => Ok(u.to_string()),
        Value::Float(f) => float(f64::from(*f)),
        Value::Double(f) => float(*f),
        Value::Bytes(b) => std::str::from_utf8(b)
            .map(quote)
            .map_err(|_| bad("bytes that aren't UTF-8")),
        Value::DateTime(d) => Ok(quote(&format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            d.year, d.month, d.day, d.hour, d.minute, d.second, d.micro
        ))),
        Value::Time(t) => Ok(quote(&format!(
            "{}{}:{:02}:{:02}.{:06}",
            if t.negative { "-" } else { "" },
            u64::from(t.days) * 24 + u64::from(t.hour),
            t.minute,
            t.second,
            t.micro
        ))),
    }
}

/// Where call `call`'s graph is, as `Plan::Graph` shows it.
fn step(call: usize, g: &crate::graphs::GraphRef, cypher: &str) -> GraphStep {
    let mut tiers = sprig_graph::parser::parse(cypher)
        .map(|q| sprig_graph::exec::explain(&q))
        .unwrap_or_default();
    // The call's hops are router routes over this group.
    tiers.extend(sprig_router::graph_route::explain_lines(
        &g.shards.group(),
        g.shards.mirror_async,
    ));
    GraphStep {
        call,
        graph: format!("{}.{}", g.db, g.name),
        group: g.def.group_id,
        shards: g
            .shards
            .shards
            .iter()
            .map(|s| ShardLoc {
                shard: s.shard,
                schema: s.schema.clone(),
                cluster: s.cluster.clone(),
                lo: s.lo,
                epoch: s.epoch,
                held_until_ms: None,
            })
            .collect(),
        tiers,
    }
}

impl PassSession {
    /// Before the router plans `sql`: binds this statement's `GRAPH_TABLE`
    /// placeholders (see the module docs). Any other statement runs as it
    /// is, and forgets the last graph statement's rows.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn graph_bind(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Vec<u8>,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mapper: &ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Bind> {
        if !self.router.graph_tmp.is_empty() {
            self.graph_tmp_drop(cx).await;
        }
        let Some(pending) = self.router.graph.clone() else {
            self.router.graph_bound = None;
            return Ok(Bind::Run(sql));
        };
        let ours = std::str::from_utf8(&sql).is_ok_and(|t| {
            t.to_ascii_lowercase()
                .contains(&format!("{PENDING_FN}({},", pending.nonce))
        });
        if !ours {
            self.router.graph_bound = None;
            return Ok(Bind::Run(sql));
        }
        let text = String::from_utf8_lossy(&sql).into_owned();
        match Box::pin(self.graph_run(
            cx, &text, &pending, original, more, format, mapper, map, out,
        ))
        .await
        {
            Ok(b) => Ok(b),
            Err(e) => Ok(Bind::Done(self.fail(out, &e).await?)),
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn graph_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &str,
        pending: &Pending,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mapper: &ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Bind> {
        let Some(ts) = self.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let graphs = GatewayGraphs::shared(ts.services());
        let cat = graphs.catalog(ts.tenant, Freshness::Latest).await?;
        let grants = graphs.grants(&cat, ts.principal.user_id);
        let user = ts.principal.user.clone();
        let current = self.current_logical();
        let mut steps = Vec::with_capacity(pending.calls.len());
        let mut dbs = Vec::with_capacity(pending.calls.len());
        for (i, c) in pending.calls.iter().enumerate() {
            let db =
                c.db.clone()
                    .or_else(|| current.clone())
                    .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
            if let Some(g) = graphs.resolve(&cat, &db, &c.graph).await? {
                steps.push(step(i, &g, &c.cypher));
            }
            dbs.push(db);
        }
        let layout = self.router_layout().await;
        let cfg = self.router_cfg();
        let default_schema = self
            .bs
            .as_ref()
            .and_then(|bs| bs.vars().schema.clone())
            .or_else(|| {
                self.tenant
                    .as_deref()
                    .and_then(|ts| ts.schema_of(ts.current.as_deref()))
            });
        let cluster = self.main_cluster().to_string();
        let empty = StaticTopology::default();
        let topo: &dyn Topology = layout.as_ref().map_or(&empty, |(l, _)| &l.topo);
        let pcx = PlanCtx {
            topo,
            default_schema: default_schema.as_deref(),
            session_cluster: &cluster,
            max_route_values: cfg.max_route_values,
            lookup_batch: cfg.lookup_batch,
            ids: self.id_mode(),
        };
        let plan = sprig_router::plan_graph(text, pending.nonce, &pending.calls, steps, &pcx);
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        match plan {
            Plan::Explain(inner) => {
                metrics::counter!("sprig_router_statements_total", "plan" => "explain")
                    .increment(1);
                if let Some((l, _)) = &layout {
                    self.ensure_coord(cx, l).await;
                    let ran = self
                        .run_plan(
                            cx,
                            Plan::Explain(inner),
                            Bytes::from(text.to_owned()),
                            original,
                            more,
                            format,
                            mapper.clone(),
                            map,
                            out,
                        )
                        .await?;
                    return Ok(Bind::Done(match ran {
                        Ran::Done(step) => step,
                        Ran::Stale(e)
                        | Ran::ColumnGone(e)
                        | Ran::Collision(e)
                        | Ran::Regather(e, _) => self.fail_mapped(out, e, map).await?,
                    }));
                }
                let Plan::Graph(g) = *inner else {
                    return Err(SprigError::internal("EXPLAIN of a graph statement"));
                };
                let Output::Rows { columns, rows, .. } = sprig_router::exec::explain_graph(&g)
                else {
                    return Err(SprigError::internal("EXPLAIN rows"));
                };
                let mut ans = LocalAnswer { columns, rows };
                ans.set_text_charset(cx.proto.collation);
                send_local(out, &ans, format, status).await?;
                Ok(Bind::Done(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Plan::Graph(g) => {
                metrics::counter!("sprig_router_statements_total", "plan" => "graph").increment(1);
                let mut rows = Vec::with_capacity(g.calls.len());
                let mut bytes = 0usize;
                // Past the inline limit, a statement
                // that reads no distributed table (it runs on this
                // session's connection) takes its rows from temporary
                // tables there instead of its text.
                // The temporary tables' schema: the session's, else the
                // first call's graph's database's.
                let tmp_schema = default_schema.clone().or_else(|| {
                    let ts = self.tenant.as_deref()?;
                    ts.schema_of(dbs.first().map(String::as_str))
                });
                let local = matches!(*g.outer_plan, Plan::NotRouted)
                    && !self.router.in_txn()
                    && !self.legacy_txn()
                    && tmp_schema.is_some();
                for (i, (c, db)) in g.calls.iter().zip(&dbs).enumerate() {
                    // The outer statement's filters on the call's columns
                    // run in the graph.
                    let cypher = g.cypher(i);
                    if g.filters.get(i).is_some_and(|f| !f.is_empty()) {
                        metrics::counter!("sprig_router_graph_filters_pushed_total").increment(1);
                    }
                    let o = graphs
                        .query_read(&cat, grants.as_deref(), &user, db, &c.graph, &cypher)
                        .await?;
                    let r: Vec<Vec<GraphLit>> =
                        o.rows.iter().map(|r| r.iter().map(lit).collect()).collect();
                    bytes += r
                        .iter()
                        .flatten()
                        .map(|v| match v {
                            GraphLit::Null => 4,
                            GraphLit::Int(_) => 20,
                            GraphLit::Str(s) => s.len() + 4,
                            GraphLit::Other(t) => t.len() + 2,
                        })
                        .sum::<usize>();
                    if bytes > sprig_graph::pgq::MAX_INLINE_BYTES && !local {
                        return Err(sprig_graph::exec::budget_error(
                            "rows",
                            format!(
                                "GRAPH_TABLE results larger than {} bytes",
                                sprig_graph::pgq::MAX_INLINE_BYTES
                            ),
                        ));
                    }
                    rows.push(CallRows {
                        columns: c.columns.clone(),
                        rows: r,
                    });
                }
                let bound = if bytes > sprig_graph::pgq::MAX_INLINE_BYTES {
                    let schema = tmp_schema.clone().unwrap_or_default();
                    let mut names = Vec::with_capacity(rows.len());
                    for (i, r) in rows.iter().enumerate() {
                        let name = format!(
                            "`{}`.`_sprig_graph_rows_{}_{i}`",
                            schema.replace('`', "``"),
                            g.nonce
                        );
                        let (create, inserts) = sprig_router::graph::temp_table_sql(
                            &name,
                            &r.columns,
                            &r.rows,
                            GRAPH_TMP_CHUNK,
                        );
                        self.router.graph_tmp.push(name.clone());
                        self.graph_tmp_run(cx, &create).await?;
                        for ins in &inserts {
                            self.graph_tmp_run(cx, ins).await?;
                        }
                        names.push(name);
                    }
                    metrics::counter!("sprig_router_graph_temp_tables_total")
                        .increment(names.len() as u64);
                    g.bind_with(|call| {
                        names.get(call).map_or_else(
                            || sprig_router::graph::rows_sql(&g.calls[call].columns, &[]),
                            |n| format!("SELECT * FROM {n}"),
                        )
                    })
                } else {
                    g.bind(&rows)
                };
                self.router.graph_bound = layout.is_some().then(|| Bound {
                    text: bound.clone(),
                    plan: g,
                    rows,
                });
                Ok(Bind::Run(bound.into_bytes()))
            }
            Plan::Reject(r) => Err(r.error()),
            other => Err(SprigError::internal(format!(
                "a graph statement planned as {}",
                other.kind()
            ))),
        }
    }
}

/// Rows per `INSERT` into a graph statement's temporary table: at most
/// this many bytes of SQL text.
const GRAPH_TMP_CHUNK: usize = 1 << 20;

impl PassSession {
    /// Runs `sql` on the session's own connection (a graph statement's
    /// temporary table).
    async fn graph_tmp_run(&mut self, cx: &mut ExecCtx<'_>, sql: &str) -> Result<()> {
        let bs = self.bs(cx);
        super::backend::run_on(bs, sql, &mut sprig_router::Budget::mb(64)).await?;
        Ok(())
    }

    /// Drops the last graph statement's temporary tables.
    async fn graph_tmp_drop(&mut self, cx: &mut ExecCtx<'_>) {
        let names = std::mem::take(&mut self.router.graph_tmp);
        if names.is_empty() {
            return;
        }
        let sql = format!("DROP TEMPORARY TABLE IF EXISTS {}", names.join(", "));
        if let Err(e) = self.graph_tmp_run(cx, &sql).await {
            tracing::warn!(error = %e, "dropping a graph statement's temporary tables");
        }
    }
}

/// The calls of `sql` and its text with placeholders, when it is one
/// `SELECT` (or `EXPLAIN FORMAT=SPRIG SELECT`) the router path binds; `None`
/// for other statements (their rows are bound before the pipeline).
pub(crate) fn placeholders(sql: &str, calls: Vec<GraphCall>) -> Option<(Pending, String)> {
    if calls.is_empty() || !single_select(sql) {
        return None;
    }
    let mut b = [0u8; 8];
    {
        use ring::rand::SecureRandom;
        let _ = ring::rand::SystemRandom::new().fill(&mut b);
    }
    // Below 2^63, so the router's parser reads it as a plain number.
    let nonce = (u64::from_le_bytes(b) >> 1).max(1);
    let text = sprig_router::graph::with_placeholders(sql, &calls, nonce);
    Some((Pending { nonce, calls }, text))
}

/// One statement (no `;` before trailing blanks, outside quotes and
/// comments) starting with `SELECT`, `WITH` or `(`, after an optional
/// `EXPLAIN FORMAT=SPRIG`.
fn single_select(sql: &str) -> bool {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == b'\\' && q != b'`' {
                i += 2;
                continue;
            }
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => quote = Some(c),
            b'#' => return false,
            b'-' if b.get(i + 1) == Some(&b'-') => return false,
            b'/' if b.get(i + 1) == Some(&b'*') => return false,
            b';' if !sql[i + 1..].trim().is_empty() => return false,
            _ => {}
        }
        i += 1;
    }
    let mut t = sql.trim_start();
    let word = |t: &str| {
        t.split(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '=')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase()
    };
    if word(t) == "EXPLAIN" {
        t = t["EXPLAIN".len()..].trim_start();
        if word(t).replace(' ', "") != "FORMAT=SPRIG" {
            return false;
        }
        t = t["FORMAT=SPRIG".len()..].trim_start();
    }
    t.starts_with('(') || matches!(word(t).as_str(), "SELECT" | "WITH")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_single_selects_take_placeholders() {
        assert!(single_select("SELECT 1"));
        assert!(single_select("  with x AS (SELECT 1) SELECT * FROM x;  "));
        assert!(single_select("EXPLAIN FORMAT=SPRIG SELECT 1"));
        assert!(single_select("(SELECT 1) UNION (SELECT 2)"));
        assert!(single_select("SELECT ';' AS x"));
        assert!(!single_select("SELECT 1; SELECT 2"));
        assert!(!single_select("INSERT INTO t SELECT 1"));
        assert!(!single_select("CREATE TABLE t AS SELECT 1"));
        assert!(!single_select("EXPLAIN SELECT 1"));
        assert!(!single_select("SELECT 1 /* c */"));
        let sql = "SELECT * FROM GRAPH_TABLE (g MATCH (a) COLUMNS (a.x AS x)) AS t";
        let calls = sprig_router::graph::find(sql).unwrap_or_default();
        let (p, text) = placeholders(sql, calls).expect("placeholders");
        assert_eq!(p.calls.len(), 1);
        assert!(
            text.contains(&format!("{PENDING_FN}({}, 0)", p.nonce)),
            "{text}"
        );
        assert!(text.ends_with(" AS t"));
    }
}
