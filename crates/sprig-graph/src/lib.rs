//! `sprig-graph`: property graphs: catalog, Cypher front end, pattern
//! planning and traversal.
//!
//! Layer: 2 data layer. Design: [chapter 43](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/43-graphs.md) (Graphs) of the SprigDB low-level design.
//!
//! - [`parser`] and [`ast`]: Cypher into the syntax tree the executor runs
//!   (build steps 5 and 6).
//! - [`value`] and [`eval`]: Cypher values and the gateway evaluator
//!   (step 10).
//! - [`store`]: what the executor needs from storage; [`mem`] in memory
//!   with any number of shards, [`maria`] over MariaDB (step 3).
//! - [`exec`]: matching, variable-length and shortest paths, projections and
//!   writes under the budgets (steps 9, 11, 12, 13).
//! - [`catalog`]: the `graph` catalog module and graph DDL (step 2).
//! - [`settings`]: the `graph.*` keys.
//! - [`neo4j`]: Neo4j status codes for errors.

// An interpreter: evaluator and executor functions are one match or one
// step-by-step walk each, with the short names of the algorithms.
#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

pub mod ast;
pub mod catalog;
pub mod eval;
pub mod exec;
pub mod maria;
pub mod mem;
pub mod neo4j;
pub mod parser;
pub mod settings;
pub mod store;
pub mod value;

pub use exec::{Budgets, Output};

/// Parses and runs `text` over `store`.
///
/// # Errors
/// The parser's and the executor's.
pub async fn execute(
    store: &mut dyn store::GraphStore,
    text: &str,
    params: &eval::Params,
    budgets: &Budgets,
) -> sprig_common::Result<Output> {
    let q = parser::parse(text)?;
    let started = std::time::Instant::now();
    let r = exec::run(store, &q, params, budgets).await;
    let kind = if r.as_ref().is_ok_and(|o| o.qtype != "r") {
        "write"
    } else {
        "read"
    };
    metrics::counter!("sprig_graph_statements_total", "language" => "cypher", "kind" => kind,
        "result" => if r.is_ok() { "ok" } else { "error" })
    .increment(1);
    metrics::histogram!("sprig_graph_statement_duration_seconds", "language" => "cypher", "kind" => kind)
        .record(started.elapsed().as_secs_f64());
    r
}
