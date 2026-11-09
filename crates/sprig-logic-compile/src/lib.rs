//! `sprig-logic-compile`: the JavaScript and KIRun to MariaDB PL/SQL
//! compiler: parser front ends, shared IR, checks and code generation.
//!
//! Layer: 2 logic. Design: [chapter 41](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/41-logic.md)
//! (Logic) of the SprigDB low-level design.
//!
//! ```text
//!  source ─► front end ─► IR ─► checks ─► PL/SQL text
//!   KIRun: JSON + expressions (kirun)    language, bounds, cost (check)
//!   JavaScript: not yet (js)             generator (plsql)
//!                        └─► reference interpreter (interp), for tests
//!                            and the differential suite
//! ```
//!
//! What exists (chapter 41, build steps 4, 6, 7 and 8 in part):
//!
//! - [`ir`]: the typed IR;
//! - [`kirun`]: the KIRun front end (model, expressions, step graph);
//! - [`sql`]: embedded `Sprig.Db.*` statements bound against the shard
//!   schema's columns and re-rendered;
//! - [`check`]: the placement checks the compiler can answer (language,
//!   bounds, cost) and their reasons;
//! - [`plsql`]: PL/SQL generation (package spec and body, entry
//!   procedure) under `sql_mode = ORACLE`;
//! - [`interp`]: a reference interpreter of the IR;
//! - [`pure`]: bodies as plain SQL expressions, for `CHECK` constraints
//!   and generated columns (R7 order 3);
//! - [`js`]: the JavaScript front end's place, which for now sends every
//!   function to the engine tier.
//!
//! Not yet: the JavaScript and Lua front ends, collections and key-value
//! access, locality over sharded groups, inlining of helper functions.

// A compiler's code reads best with short names for AST and IR nodes, and
// the interpreter mirrors SQL's numeric casts and exact double compares on
// purpose.
#![allow(
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::similar_names,
    clippy::format_push_string,
    clippy::single_match_else
)]

pub mod check;
pub mod interp;
pub mod ir;
pub mod js;
pub mod kirun;
pub mod plsql;
pub mod pure;
pub mod sql;

use std::fmt;

use serde::Serialize;
use sprig_common::{Result, SprigCode, SprigError};

pub use check::Analysis;
pub use ir::Function;
pub use plsql::Plsql;
pub use sql::{Columns, StaticColumns};

/// The compiler's version, recorded with every compiled version
/// (`logic_versions.compiler_version`).
pub const COMPILER_VERSION: &str = "sprig-logic-compile/0.1";

/// Why code can't run in MariaDB (chapter 41, "Placement"): a code such
/// as `kirun.function`, where in the source, and what.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Reason {
    /// `<area>.<what>`: `kirun.*`, `js.*`, `db.*`, `types.*`, `bounds.*`.
    pub code: String,
    /// Where: `step item`, `step item, parameter sql`, `line 3`.
    pub at: String,
    /// What, for people.
    pub detail: String,
}

impl Reason {
    /// A reason.
    #[must_use]
    pub fn new(code: &str, at: &str, detail: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            at: at.to_owned(),
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}: {}", self.code, self.at, self.detail)
    }
}

/// The source language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Runtime {
    /// KIRun JSON.
    Kirun,
    /// The JavaScript subset.
    Javascript,
    /// Plain SQL bodies.
    Sql,
    /// Redis Lua (chapter 40).
    Lua,
}

impl Runtime {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Runtime::Kirun => "kirun",
            Runtime::Javascript => "javascript",
            Runtime::Sql => "sql",
            Runtime::Lua => "lua",
        }
    }

    /// Parses `LANGUAGE <name>` (any case).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "kirun" => Runtime::Kirun,
            "javascript" | "js" => Runtime::Javascript,
            "sql" => Runtime::Sql,
            "lua" => Runtime::Lua,
            _ => return None,
        })
    }
}

/// Compile-time limits and names (chapter 41's settings).
#[derive(Debug, Clone)]
pub struct Options {
    /// The module's id, for routine names.
    pub module_id: u64,
    /// The version, for routine names.
    pub version: u64,
    /// `logic.max_loop_iterations`: the guard on loops without a static
    /// bound.
    pub max_loop_iterations: u64,
    /// `logic.compile_max_cost`.
    pub compile_max_cost: u64,
    /// `SQL SECURITY DEFINER` instead of `INVOKER`.
    pub definer_security: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            module_id: 0,
            version: 1,
            max_loop_iterations: 100_000,
            compile_max_cost: 100_000,
            definer_security: false,
        }
    }
}

/// The compiler's output for one version.
#[derive(Debug, Clone)]
pub struct Compiled {
    /// The IR. With reasons, parts are placeholders: for `EXPLAIN` only.
    pub ir: Option<Function>,
    /// Checks, reasons, cost and effects.
    pub analysis: Analysis,
    /// The generated text, when the function compiles.
    pub plsql: Option<Plsql>,
}

impl Compiled {
    /// Whether the function can run in MariaDB.
    #[must_use]
    pub fn compilable(&self) -> bool {
        self.analysis.reasons.is_empty() && self.plsql.is_some()
    }
}

/// Compiles `source`, written in `runtime`, against the shard schema's
/// columns `cat`.
///
/// # Errors
/// 39861 (`ER_SPRIG_LOGIC_SYNTAX`) when the source doesn't parse or is
/// malformed; 39874 when embedded SQL and its arguments disagree.
/// Anything that parses but has no compiled form is a reason in the
/// analysis instead.
pub fn compile(
    runtime: Runtime,
    source: &str,
    cat: &dyn Columns,
    opts: &Options,
) -> Result<Compiled> {
    match runtime {
        Runtime::Kirun => {
            let def: kirun::model::Definition = serde_json::from_str(source).map_err(|e| {
                SprigError::new(
                    SprigCode::LogicSyntax,
                    format!("the KIRun definition isn't valid JSON for a function: {e}"),
                )
            })?;
            let lowered = kirun::lower(&def, cat)?;
            Ok(finish(lowered.func, lowered.reasons, lowered.lints, opts))
        }
        Runtime::Javascript => Ok(js::compile(source)),
        Runtime::Sql | Runtime::Lua => Ok(Compiled {
            ir: None,
            analysis: Analysis::refused(Reason::new(
                &format!("{}.front_end", runtime.as_str()),
                "source",
                format!("{} bodies have no compiler yet", runtime.as_str()),
            )),
            plsql: None,
        }),
    }
}

fn finish(f: Function, mut reasons: Vec<Reason>, lints: Vec<Reason>, opts: &Options) -> Compiled {
    let mut analysis = check::analyze(&f, opts);
    reasons.append(&mut analysis.reasons);
    analysis.reasons = reasons;
    analysis.lints.extend(lints);
    let plsql = analysis
        .reasons
        .is_empty()
        .then(|| plsql::generate(&f, opts));
    Compiled {
        ir: Some(f),
        analysis,
        plsql,
    }
}

#[cfg(test)]
mod tests;
