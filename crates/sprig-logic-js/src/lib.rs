//! `sprig-logic-js`: the fallback tier: QuickJS for JavaScript, the `kirun` crate for KIRun, `mlua` for Redis Lua.
//!
//! Layer: 2 logic. Design: [chapter 41](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/41-logic.md) (Logic) of the SprigDB low-level design.
//!
//! What exists (R7 order 4, the KIRun half; chapter 41 build steps 13
//! and 14 in part):
//!
//! - [`kirun`]: KIRun definitions run on `kirun`, Modlix's open-source
//!   KIRun runtime for Rust, with `Sprig.Db.*` and `Sprig.Http.Fetch`;
//! - [`budget`]: wall time, steps, loop iterations, memory, data
//!   operations and rows read, charged by a shim around every function;
//! - [`bridge`]: the `db` interface the host implements (the gateway runs
//!   each statement through the caller's own tenant session), and the
//!   checking and binding of statements;
//! - [`outbound`]: `logic.outbound_allowlist` and the HTTP client.
//!
//! Not yet: QuickJS for JavaScript (step 12), `mlua` for Redis Lua
//! (chapter 40), engine threads and per-tenant isolates (KIRun runs on the
//! caller's task, cooperatively), `db.call`, `db.transaction`, collections
//! and key-value access.

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
pub mod kirun;
pub mod outbound;

pub use budget::Limits;
pub use kirun::{Host, Outcome, Prepared, run};
