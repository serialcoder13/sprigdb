//! `sprig`: SprigDB's terminal shell, a full-screen TUI and a scriptable
//! CLI for tenant users and operators.
//!
//! Layer 4 (binaries). It calls only the public APIs with the user's own
//! credential, as Studio does, and runs
//! `sprigctl` for operator commands.

pub mod admin;
pub mod api;
pub mod ask;
pub mod cli;
pub mod dash;
pub mod error;
pub mod exec;
pub mod history;
pub mod http;
pub mod inbox;
pub mod lang;
pub mod model;
pub mod msg;
pub mod native;
pub mod output;
pub mod profile;
pub mod redact;
pub mod saved;
pub mod secrets;
pub mod session;
pub mod term;
pub mod tui;
