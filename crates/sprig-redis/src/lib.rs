//! `sprig-redis`: Redis command execution over the key-value engine, Lua
//! scripts and functions, and pub/sub (chapter 40).
//!
//! Layer: 2 front ends. Design: [chapter 40](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/40-redis.md) (Redis) of the SprigDB low-level design.
//!
//! - [`session`]: one connection's state machine (`HELLO`, `AUTH`,
//!   `SELECT`, `MULTI`/`EXEC`/`WATCH`, subscriptions, blocking commands)
//!   and dispatch; the gateway feeds it decoded commands.
//! - [`spec`]: the command table (arity, flags, key positions).
//! - [`ctx`]: a command's view of one Redis database: its key-value
//!   namespace (chapter 33), the key row, elements, versions and expiry,
//!   all as SQL through the caller's [`sprig_kv::SqlRunner`].
//! - [`batch`]: the hottest writes (`SET`, `INCR`, `HSET`, pushes,
//!   pops, `ZADD`) as one guarded compound statement (R6 round 3).
//! - [`cmd`]: the commands, by type.
//! - [`logic`]: `FCALL` of logic-layer functions (chapter 41), through
//!   a hook the gateway installs.
//! - [`lua`]: `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`, `FCALL` on SprigDB's
//!   one Lua runtime (`sprig_logic_js::lua::Sandbox`).
//! - [`pubsub`]: the gateway's subscribers and the wait table of blocking
//!   commands.
//! - [`fabric`]: chapter 40's hubs, carrying publishes and wake-ups of
//!   blocked clients between gateways over the peer service.
//! - [`glob`], [`reply`]: Redis's glob matcher and reply texts.
//!
//! As built in R6 (interim; `decisions/r6-interim.md`, the R items): each
//! command runs as SQL statements computed in the gateway (lock the key
//! row, read what the command needs, write), not chapter 40's one-packet
//! guarded batches; scripts and libraries live in a table of the tenant's
//! Redis database.

// Redis's number rules are casts by design: lengths and counts become RESP
// integers, scores move between doubles and 52-bit geohashes, and the
// casts are checked where a value could leave its range.
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_cmp
)]
// Every command returns its Redis reply or a backend error (see `ctx`);
// dispatch tables and Redis-style option parsing are long by nature.
#![allow(
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

pub mod batch;
pub mod cmd;
pub mod conn;
pub mod ctx;
pub mod env;
pub mod fabric;
pub mod glob;
pub mod logic;
pub mod lua;
pub mod pubsub;
pub mod reply;
pub mod session;
pub mod spec;

pub use env::{Config, Env};
pub use session::{Auth, Login, Session};
