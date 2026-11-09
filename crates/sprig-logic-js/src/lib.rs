//! `sprig-logic-js`: the fallback tier: QuickJS for JavaScript, the `kirun` crate for KIRun, `mlua` for Redis Lua.
//!
//! Layer: 2 logic. Design: [chapter 41](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/41-logic.md) (Logic) of the SprigDB low-level design.
//!
//! What exists (R7 order 4; chapter 41 build steps 12 to 14 in part):
//!
//! - [`kirun`]: KIRun definitions run on `kirun`, Modlix's open-source
//!   KIRun runtime for Rust, with `Sprig.Db.*` and `Sprig.Http.Fetch`;
//! - [`js`]: JavaScript functions on QuickJS (`rquickjs`), with `db`,
//!   `fetch`, `ctx` and `sprig`;
//! - [`lua`]: SprigDB's one Lua runtime (Lua 5.1 through `mlua`): the
//!   sandbox and budgets chapters 40 and 51 reuse, and Lua functions;
//! - [`engine`]: engine threads and the synchronous `db` of JavaScript
//!   and Lua; [`program`]: one version in any of the three languages;
//! - [`budget`]: wall time, steps, loop iterations, memory, data
//!   operations and rows read, charged by a shim around every function;
//! - [`bridge`]: the `db` interface the host implements (the gateway runs
//!   each statement through the caller's own tenant session), and the
//!   checking and binding of statements;
//! - [`outbound`]: `logic.outbound_allowlist` and the HTTP client.
//!
//! Not yet: per-tenant isolate caches (each JavaScript and Lua call gets
//! a fresh runtime; KIRun runs on the caller's task, cooperatively),
//! `db.transaction`, collections and key-value access, Redis's
//! `redis.*` library (chapter 40 adds it on [`lua::Sandbox`]).

// The engine mirrors SQL's numeric conversions on purpose.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::module_name_repetitions
)]

pub mod bridge;
pub mod budget;
pub mod engine;
pub mod js;
pub mod kirun;
pub mod lua;
pub mod outbound;
pub mod program;

#[cfg(test)]
mod testutil;

pub use budget::Limits;
pub use engine::{ParamTy, ScriptSig};
pub use kirun::{Host, Outcome, Prepared, run};
pub use program::Program;
