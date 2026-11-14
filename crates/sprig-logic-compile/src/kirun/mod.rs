//! The KIRun front end: a definition's
//! JSON, its expressions and its step graph, lowered to the IR.
//!
//! The model and the expression grammar follow KIRun's open-source
//! definition format and runtime (`modlix-india/nocode-kirun`); the
//! compiler reads definitions itself and links no KIRun code. Execution
//! semantics follow KIRun's runtime:
//!
//! - a step runs once all the step events it depends on have happened
//!   (`dependentStatements`, `executeIftrue` keys and every
//!   `Steps.<step>.<event>` its expressions read) and every
//!   `executeIftrue` expression is present and not `false`;
//! - a step that raises an event other than `output` (`If`'s
//!   `true`/`false`, a loop's `iteration`) runs the sub-graph hanging off
//!   that event, once per event, then its `output` dependents;
//! - `System.Loop.Break` asks the loop to stop before its next iteration;
//!   the rest of the current iteration still runs;
//! - the first event a function raises is its result, and raising
//!   `output` ends it.

pub mod expr;
mod lower;
pub mod model;

pub use lower::{Lowered, lower};
