//! Streamed results (chapter 43, "Bolt front end", streaming; decision
//! G4's follow-up): rows go to a [`RowSink`] as they are found, so a
//! client's pace bounds memory instead of the whole answer.
//!
//! A statement streams when its rows don't depend on each other: one read
//! part that starts with a `MATCH` and continues with `MATCH`, `OPTIONAL
//! MATCH`, `UNWIND` and plain `WITH` (no aggregates, `DISTINCT`, `ORDER BY`,
//! `SKIP` or `LIMIT`), ending in a `RETURN` without aggregates, `DISTINCT`
//! or `ORDER BY` (`SKIP` and `LIMIT` apply to the stream). The first
//! pattern's start node is then read in pages of `graph.batch_nodes` ids,
//! in id order, and each page runs through every clause before the next
//! is read; a sink that stops accepting rows ends the statement early.
//! Other statements run whole and then hand their rows to the sink.

use std::collections::BTreeMap;
use std::time::Instant;

use async_trait::async_trait;
use sprig_common::Result;

use super::matcher::conjuncts;
use super::{Exec, Output, has_writes, pattern_vars};
use crate::ast::{Clause, Explain, Expr, NodePat, PathKind, Projection, Query};
use crate::eval::{Params, Row};
use crate::store::GraphStore;
use crate::value::Value;

/// Where streamed rows go.
#[async_trait]
pub trait RowSink: Send {
    /// The column names, before any row.
    async fn columns(&mut self, cols: &[String]) -> Result<()>;
    /// One row; `false` when the consumer is gone (the statement stops).
    async fn row(&mut self, row: Vec<Value>) -> Result<bool>;
}

/// A sink that keeps everything (tests, buffered callers).
#[derive(Debug, Default)]
pub struct VecSink {
    /// The columns.
    pub columns: Vec<String>,
    /// The rows.
    pub rows: Vec<Vec<Value>>,
}

#[async_trait]
impl RowSink for VecSink {
    async fn columns(&mut self, cols: &[String]) -> Result<()> {
        cols.clone_into(&mut self.columns);
        Ok(())
    }

    async fn row(&mut self, row: Vec<Value>) -> Result<bool> {
        self.rows.push(row);
        Ok(true)
    }
}

fn plain_with(p: &Projection) -> bool {
    !p.distinct
        && p.order.is_empty()
        && p.skip.is_none()
        && p.limit.is_none()
        && !p.items.iter().any(|(e, _)| e.has_aggregate())
}

fn add(scope: &mut Vec<String>, v: impl IntoIterator<Item = String>) {
    for x in v {
        if !scope.contains(&x) {
            scope.push(x);
        }
    }
}

/// The result columns of a statement that streams; `None` when it
/// doesn't (it then runs whole).
#[must_use]
pub fn streamable(q: &Query) -> Option<Vec<String>> {
    if q.explain != Explain::None || q.parts.len() != 1 || has_writes(q) {
        return None;
    }
    let clauses = &q.parts[0].clauses;
    match clauses.first() {
        Some(Clause::Match {
            optional: false,
            patterns,
            ..
        }) if patterns.first().is_some_and(|p| p.kind == PathKind::Normal) => {}
        _ => return None,
    }
    let mut scope: Vec<String> = Vec::new();
    let last = clauses.len() - 1;
    for (i, c) in clauses.iter().enumerate() {
        match c {
            Clause::Match { patterns, .. } if i < last => {
                for p in patterns {
                    add(&mut scope, pattern_vars(p));
                }
            }
            Clause::Unwind { alias, .. } if i < last => add(&mut scope, [alias.clone()]),
            Clause::With(p) if i < last && plain_with(p) => {
                let mut next = if p.star { scope.clone() } else { Vec::new() };
                add(&mut next, p.items.iter().map(|(_, a)| a.clone()));
                scope = next;
            }
            Clause::Return(p)
                if i == last
                    && !p.distinct
                    && p.order.is_empty()
                    && !p.items.iter().any(|(e, _)| e.has_aggregate()) =>
            {
                let mut cols: Vec<String> = Vec::new();
                if p.star {
                    let mut vars: Vec<String> = scope
                        .iter()
                        .filter(|v| !v.starts_with(' '))
                        .cloned()
                        .collect();
                    vars.sort();
                    vars.dedup();
                    if vars.is_empty() && p.items.is_empty() {
                        return None;
                    }
                    cols = vars;
                }
                cols.extend(p.items.iter().map(|(_, a)| a.clone()));
                return Some(cols);
            }
            _ => return None,
        }
    }
    None
}

/// The hidden variable naming an anonymous paged start node.
const PAGED: &str = " s0";

/// Runs `q` over `store`, handing rows to `sink` as they are found (see
/// the module docs). The returned output carries the summary; its rows
/// are empty.
///
/// # Errors
/// As [`super::run`]; a sink's error.
pub async fn run_stream(
    store: &mut dyn GraphStore,
    q: &Query,
    params: &Params,
    budgets: &super::Budgets,
    sink: &mut dyn RowSink,
) -> Result<Output> {
    let Some(cols) = streamable(q) else {
        let mut out = super::run(store, q, params, budgets).await?;
        sink.columns(&out.columns).await?;
        for r in std::mem::take(&mut out.rows) {
            if !sink.row(r).await? {
                break;
            }
        }
        return Ok(out);
    };
    sink.columns(&cols).await?;
    let mut ex = Exec {
        store,
        params,
        b: budgets,
        started: Instant::now(),
        stats: BTreeMap::new(),
        deleted: Vec::new(),
        wrote: false,
        plan: Vec::new(),
        slot: 0,
        star_in_order: false,
    };
    let mut clauses = q.parts[0].clauses.clone();
    // SKIP and LIMIT apply to the whole stream, not to each page.
    let (skip, limit) = match clauses.last_mut() {
        Some(Clause::Return(p)) => {
            let s = ex.const_int(p.skip.take().as_ref(), "SKIP")?.unwrap_or(0);
            let l = ex.const_int(p.limit.take().as_ref(), "LIMIT")?;
            (s, l)
        }
        _ => (0, None),
    };
    if limit == Some(0) {
        return Ok(Output {
            columns: cols,
            qtype: "r",
            ..Output::default()
        });
    }
    // The end of the first pattern the matcher would start from: its scan
    // is paged, and each page binds it.
    let Some(Clause::Match {
        patterns, where_, ..
    }) = clauses.first_mut()
    else {
        return Err(sprig_common::SprigError::internal("stream without MATCH"));
    };
    let conj: Vec<Expr> = where_
        .as_ref()
        .map(|w| conjuncts(w).into_iter().cloned().collect())
        .unwrap_or_default();
    let first = &mut patterns[0];
    let empty = [Row::new()];
    let from_last = first.steps.last().is_some_and(|(_, last)| {
        Exec::score(last, &empty, &conj) > Exec::score(&first.start, &empty, &conj)
    });
    let np: &mut NodePat = if from_last {
        match first.steps.last_mut() {
            Some((_, n)) => n,
            None => &mut first.start,
        }
    } else {
        &mut first.start
    };
    let var = np.var.get_or_insert_with(|| PAGED.to_owned()).clone();
    let np = np.clone();
    let Some(mut scan) = ex.start_scan(&np, &conj).await? else {
        return Ok(Output {
            columns: cols,
            qtype: "r",
            ..Output::default()
        });
    };
    let page = ex.b.batch_nodes.max(1);
    ex.plan
        .push(format!("NodeScan {var}: paged by {page} ids (streamed)"));
    let mut after = 0u64;
    let mut skipped = 0usize;
    let mut sent = 0usize;
    loop {
        ex.check_time()?;
        scan.after = Some(after);
        scan.limit = Some(page);
        let found = ex.store.scan(&scan).await?;
        let Some(last) = found.last().map(|r| r.id) else {
            break;
        };
        after = last;
        let full = found.len() >= page;
        let rows: Vec<Row> = found
            .iter()
            .map(|r| Row::from([(var.clone(), Value::Node(ex.node_val(r)))]))
            .collect();
        let (_, out) = ex.clauses(&clauses, rows, vec![var.clone()]).await?;
        for r in out {
            if skipped < skip {
                skipped += 1;
                continue;
            }
            sent += 1;
            ex.check_rows(sent)?;
            if !sink.row(r).await? {
                return Ok(Output {
                    columns: cols,
                    qtype: "r",
                    ..Output::default()
                });
            }
            if limit.is_some_and(|l| sent >= l) {
                return Ok(Output {
                    columns: cols,
                    qtype: "r",
                    ..Output::default()
                });
            }
        }
        if !full {
            break;
        }
    }
    Ok(Output {
        columns: cols,
        qtype: "r",
        ..Output::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemStore;
    use crate::parser::parse;

    async fn both(store: &mut MemStore, q: &str, b: &super::super::Budgets) -> (usize, usize) {
        let parsed = parse(q).expect("parse");
        let whole = super::super::run(store, &parsed, &Params::new(), b)
            .await
            .expect("run");
        let mut sink = VecSink::default();
        run_stream(store, &parsed, &Params::new(), b, &mut sink)
            .await
            .expect("stream");
        assert_eq!(sink.columns, whole.columns, "{q}");
        let mut a: Vec<String> = whole.rows.iter().map(|r| format!("{r:?}")).collect();
        let mut s: Vec<String> = sink.rows.iter().map(|r| format!("{r:?}")).collect();
        if !q.contains("LIMIT") && !q.contains("SKIP") {
            a.sort();
            s.sort();
            assert_eq!(a, s, "{q}");
        }
        (whole.rows.len(), sink.rows.len())
    }

    #[tokio::test]
    async fn streamed_rows_equal_whole_rows() {
        let b = super::super::Budgets {
            batch_nodes: 3,
            ..super::super::Budgets::default()
        };
        for shards in [1, 4] {
            let mut m = MemStore::new(shards);
            super::super::run(
                &mut m,
                &parse("UNWIND range(1, 20) AS i CREATE (:P {i: i})-[:R]->(:Q {j: i * 2})")
                    .expect("p"),
                &Params::new(),
                &b,
            )
            .await
            .expect("setup");
            for q in [
                "MATCH (p:P) RETURN p.i AS i",
                "MATCH (p:P)-[:R]->(q) WHERE q.j > 10 RETURN p.i, q.j",
                "MATCH (p:P) OPTIONAL MATCH (p)-[:R]->(q:Nope) RETURN p.i, q",
                "MATCH (p)-[:R]->(q:Q {j: 8}) RETURN p, q",
                "MATCH (n) WITH n UNWIND [1, 2] AS k RETURN n, k",
                "MATCH ()-[r:R]->() RETURN *",
                "MATCH (p:P) WITH p.i AS i WHERE i % 2 = 0 RETURN i",
            ] {
                assert!(streamable(&parse(q).expect("p")).is_some(), "{q}");
                let (a, s) = both(&mut m, q, &b).await;
                assert_eq!(a, s, "{q}");
            }
            let (_, s) = both(&mut m, "MATCH (p:P) RETURN p.i SKIP 4 LIMIT 5", &b).await;
            assert_eq!(s, 5);
            for q in [
                "MATCH (p:P) RETURN count(*)",
                "MATCH (p:P) RETURN p ORDER BY p.i",
                "MATCH (p:P) RETURN DISTINCT p.i",
                "MATCH (p:P) SET p.x = 1 RETURN p",
                "UNWIND [1] AS x MATCH (p:P) RETURN p",
            ] {
                assert!(streamable(&parse(q).expect("p")).is_none(), "{q}");
                both(&mut m, q, &b).await;
            }
        }
    }

    /// A sink that stops after `n` rows.
    struct Stop(usize, usize);

    #[async_trait]
    impl RowSink for Stop {
        async fn columns(&mut self, _cols: &[String]) -> Result<()> {
            Ok(())
        }
        async fn row(&mut self, _row: Vec<Value>) -> Result<bool> {
            self.1 += 1;
            Ok(self.1 < self.0)
        }
    }

    #[tokio::test]
    async fn a_closed_sink_ends_the_statement_early() {
        let b = super::super::Budgets {
            batch_nodes: 10,
            ..super::super::Budgets::default()
        };
        let mut m = MemStore::new(1);
        super::super::run(
            &mut m,
            &parse("UNWIND range(1, 1000) AS i CREATE (:P {i: i})").expect("p"),
            &Params::new(),
            &b,
        )
        .await
        .expect("setup");
        let before = m.stats().shard_queries;
        let mut sink = Stop(15, 0);
        run_stream(
            &mut m,
            &parse("MATCH (p:P) RETURN p").expect("p"),
            &Params::new(),
            &b,
            &mut sink,
        )
        .await
        .expect("stream");
        assert_eq!(sink.1, 15);
        // Two pages of 10 were read, not 100.
        assert!(m.stats().shard_queries - before <= 3, "{:?}", m.stats());
    }
}
