//! `sprigdb`: the one server binary. The role is a subcommand:
//! `sprigdb gateway`, `controller`, `worker`, `agent` or `operator`.
//!
//! Layer 4 (binaries). Design: chapter 03 (Codebase and conventions) and
//! chapter 04 (Gateway, "Startup" and "Drain") of the SprigDB low-level
//! design; names in the writing guide, 5.1.
//!
//! Every role starts the same way:
//! 1. read `sprigdb.toml` and `SPRIGDB_*` overrides;
//! 2. set up logging and the Prometheus recorder;
//! 3. bind the telemetry port (`/healthz` 200, `/readyz` 503, `/metrics`);
//! 4. run the role, which marks itself ready;
//! 5. on SIGTERM or SIGINT, cancel the role's shutdown token, wait for it to
//!    drain, stop the telemetry server and exit 0.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use sprig_common::telemetry::{self, Health};
use sprig_common::{CancellationToken, ProcessConfig, Result, Role, RoleContext, SprigError};

/// The default configuration file, read from the working directory when
/// present and no `--config` is given.
const DEFAULT_CONFIG: &str = "sprigdb.toml";

/// SprigDB server.
#[derive(Debug, Parser)]
#[command(name = "sprigdb", version, about)]
struct Cli {
    /// Path to `sprigdb.toml`. Without it, `./sprigdb.toml` is read if it
    /// exists; otherwise configuration comes from `SPRIGDB_*` variables only.
    #[arg(long, short, env = "SPRIGDB_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// The role to run.
    #[command(subcommand)]
    role: RoleCommand,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum RoleCommand {
    /// Client listeners and sessions (chapter 04).
    Gateway,
    /// Reconcilers and leader election (chapter 23).
    Controller,
    /// The job runtime (chapters 10 and 13).
    Worker,
    /// The node agent for VMs (chapter 27).
    Agent,
    /// The Kubernetes operator (chapter 27).
    Operator,
}

impl From<RoleCommand> for Role {
    fn from(r: RoleCommand) -> Self {
        match r {
            RoleCommand::Gateway => Role::Gateway,
            RoleCommand::Controller => Role::Controller,
            RoleCommand::Worker => Role::Worker,
            RoleCommand::Agent => Role::Agent,
            RoleCommand::Operator => Role::Operator,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let role = Role::from(cli.role);
    let config = match load_config(cli.config.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("sprigdb {role}: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = telemetry::init_tracing(&config.telemetry) {
        eprintln!("sprigdb {role}: {e}");
        return ExitCode::FAILURE;
    }
    let runtime = match build_runtime(config.runtime(role).worker_threads) {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(%role, error = %e, "can't start the tokio runtime");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(role, config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(%role, error = %e, source = ?std::error::Error::source(&e), "stopped with an error");
            ExitCode::FAILURE
        }
    }
}

fn load_config(path: Option<&Path>) -> Result<ProcessConfig> {
    match path {
        Some(p) => ProcessConfig::load(p),
        None if Path::new(DEFAULT_CONFIG).exists() => {
            ProcessConfig::load(Path::new(DEFAULT_CONFIG))
        }
        None => ProcessConfig::from_env(),
    }
}

fn build_runtime(worker_threads: usize) -> std::io::Result<tokio::runtime::Runtime> {
    let mut b = tokio::runtime::Builder::new_multi_thread();
    b.enable_all();
    if worker_threads > 0 {
        b.worker_threads(worker_threads);
    }
    b.build()
}

async fn run(role: Role, config: ProcessConfig) -> Result<()> {
    let metrics = telemetry::install_metrics()?;
    let addr = config.telemetry.bootstrap_addr;
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        SprigError::internal(format!("can't bind telemetry on {addr}")).with_source(e)
    })?;
    tracing::info!(
        %role,
        dc = %config.node.dc,
        node = %config.node.id(),
        version = env!("CARGO_PKG_VERSION"),
        telemetry = %addr,
        "starting"
    );

    let health = Health::default();
    let stop_telemetry = CancellationToken::new();
    let telemetry = tokio::spawn(telemetry::serve(
        listener,
        health.clone(),
        metrics,
        stop_telemetry.clone(),
    ));
    let shutdown = CancellationToken::new();
    tokio::spawn(cancel_on_signal(shutdown.clone()));

    let ctx = RoleContext {
        config,
        health,
        shutdown,
    };
    let outcome = match role {
        Role::Gateway => sprig_gateway::run(ctx).await,
        Role::Controller => sprig_controller::run(ctx).await,
        Role::Worker => sprig_worker::run(ctx).await,
        Role::Agent => sprig_agent::run(ctx).await,
        Role::Operator => sprig_operator::run(ctx).await,
    };

    stop_telemetry.cancel();
    let served = telemetry
        .await
        .map_err(|e| SprigError::internal("telemetry task failed").with_source(e))
        .and_then(|r| r);
    tracing::info!(%role, "stopped");
    outcome.and(served)
}

/// Cancels `token` on SIGTERM or SIGINT.
async fn cancel_on_signal(token: CancellationToken) {
    let signal = wait_for_signal().await;
    tracing::info!(signal, "shutdown requested");
    token.cancel();
}

#[cfg(unix)]
async fn wait_for_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let Ok(mut term) = signal(SignalKind::terminate()) else {
        tracing::warn!("can't listen for SIGTERM; only SIGINT stops the process");
        let _ = tokio::signal::ctrl_c().await;
        return "SIGINT";
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "ctrl-c"
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_is_well_formed() {
        Cli::command().debug_assert();
        let cli = Cli::try_parse_from(["sprigdb", "--config", "x.toml", "worker"]).unwrap();
        assert_eq!(Role::from(cli.role), Role::Worker);
        assert!(Cli::try_parse_from(["sprigdb", "router"]).is_err());
    }
}
