//! `sprig-common`: the foundations every SprigDB crate shares.
//!
//! Layer 1 (foundations). Design: chapter 03 (Codebase and conventions) of
//! the SprigDB low-level design, with telemetry from chapter 47.
//!
//! - [`error`]: [`SprigError`], [`Result`] and protocol rendering;
//! - [`ids`]: identifier newtypes, sized per the writing guide (12.12);
//! - [`config`]: process configuration from `sprigdb.toml`;
//! - [`time`]: the [`time::Clock`] every component reads time through;
//! - [`telemetry`]: tracing setup, metrics and health endpoints;
//! - [`names`]: physical MariaDB schema, account and internal names (chapter 07).
//!
//! Not here yet: `certwatch` (chapter 23 step 9).

pub mod config;
pub mod error;
pub mod ids;
pub mod names;
pub mod telemetry;
pub mod time;

use std::fmt;

pub use config::ProcessConfig;
pub use error::{ErrorKind, Result, SprigCode, SprigError};
pub use telemetry::Health;
pub use tokio_util::sync::CancellationToken;

/// A server role: one subcommand of the `sprigdb` binary (guide 5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// `sprigdb gateway`: client listeners and sessions (chapter 04).
    Gateway,
    /// `sprigdb controller`: reconcilers and leader election (chapter 23).
    Controller,
    /// `sprigdb worker`: the job runtime (chapters 10 and 13).
    Worker,
    /// `sprigdb agent`: the node agent for VMs (chapter 27).
    Agent,
    /// `sprigdb operator`: the Kubernetes operator (chapter 27).
    Operator,
}

impl Role {
    /// Every role.
    pub const ALL: [Role; 5] = [
        Role::Gateway,
        Role::Controller,
        Role::Worker,
        Role::Agent,
        Role::Operator,
    ];

    /// The subcommand and log name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Role::Gateway => "gateway",
            Role::Controller => "controller",
            Role::Worker => "worker",
            Role::Agent => "agent",
            Role::Operator => "operator",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the `sprigdb` binary hands a role when it starts it.
#[derive(Debug, Clone)]
pub struct RoleContext {
    /// The process configuration.
    pub config: ProcessConfig,
    /// Readiness and liveness, served on the telemetry port.
    pub health: Health,
    /// Cancelled on SIGTERM or SIGINT: the role drains and returns.
    pub shutdown: CancellationToken,
}
