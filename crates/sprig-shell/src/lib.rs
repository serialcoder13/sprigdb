//! `sprig`: SprigDB's terminal shell, a full-screen TUI and a scriptable
//! CLI for tenant users and operators (chapter 53 of the low-level design).
//!
//! Layer 4 (binaries). It calls only the public APIs with the user's own
//! credential (chapter 37), as Studio does (chapter 44), and runs
//! `sprigctl` for operator commands.

pub mod admin;
pub mod api;
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
pub mod secrets;
pub mod session;
pub mod term;
pub mod tui;
