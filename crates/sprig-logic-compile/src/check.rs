//! The checks behind placement that the compiler answers (the first
//! three placement steps): the language subset (the front ends report
//! those), loop bounds and the static cost, plus the objects the code
//! reads and writes. Locality, kinds and cluster capabilities are decided
//! by `sprig-logic`'s placement, which knows the layout.

use std::collections::BTreeSet;

use serde::Serialize;

use crate::ir::{Block, Bound, DbOp, Function, Stmt};
use crate::{Options, Reason};

/// What the checks found.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Analysis {
    /// Why the function can't run in MariaDB; empty when it can.
    pub reasons: Vec<Reason>,
    /// Warnings.
    pub lints: Vec<Reason>,
    /// Static cost: embedded statements times enclosing loop bounds, a
    /// guarded loop counting `logic.max_loop_iterations`.
    pub cost: u64,
    /// Tables read.
    pub reads: Vec<String>,
    /// Tables written.
    pub writes: Vec<String>,
    /// Loops without a static bound, guarded at run time.
    pub guarded_loops: usize,
}

impl Analysis {
    /// An analysis that refuses for one reason.
    #[must_use]
    pub fn refused(r: Reason) -> Self {
        Self {
            reasons: vec![r],
            ..Self::default()
        }
    }
}

/// Runs the checks on `f`.
#[must_use]
pub fn analyze(f: &Function, opts: &Options) -> Analysis {
    let mut a = Analysis::default();
    let mut reads = BTreeSet::new();
    let mut writes = BTreeSet::new();
    a.cost = cost(&f.body, 1, opts, &mut a, &mut reads, &mut writes);
    a.reads = reads.into_iter().collect();
    a.writes = writes.into_iter().collect();
    if a.cost > opts.compile_max_cost {
        a.reasons.push(Reason::new(
            "bounds.cost",
            "function",
            format!(
                "static cost {} is over logic.compile_max_cost ({})",
                a.cost, opts.compile_max_cost
            ),
        ));
    }
    a
}

fn cost(
    b: &Block,
    mult: u64,
    opts: &Options,
    a: &mut Analysis,
    reads: &mut BTreeSet<String>,
    writes: &mut BTreeSet<String>,
) -> u64 {
    let mut total: u64 = 0;
    for s in b {
        total = total.saturating_add(match s {
            Stmt::If(_, x, y) => {
                cost(x, mult, opts, a, reads, writes).max(cost(y, mult, opts, a, reads, writes))
            }
            Stmt::While { body, bound, .. } => {
                let n = match bound {
                    Bound::Static(n) => *n,
                    Bound::Guarded => {
                        a.guarded_loops += 1;
                        opts.max_loop_iterations
                    }
                };
                cost(body, mult.saturating_mul(n.max(1)), opts, a, reads, writes)
            }
            Stmt::Db(DbOp::Sql { stmt, .. }) => {
                reads.extend(stmt.reads.iter().cloned());
                writes.extend(stmt.writes.iter().cloned());
                mult
            }
            Stmt::Assign(..)
            | Stmt::Break
            | Stmt::Continue
            | Stmt::Emit(..)
            | Stmt::Throw(_)
            | Stmt::Return => 0,
        });
    }
    total
}
