//! The tier 1 connector matrix and sysbench: every connector passes
//! through the gateway on both ports.
//!
//! Runs `deploy/compose/connectors/run.sh` once per endpoint of
//! `SPRIG_TEST_CONNECTORS` (`host:port:persona`, comma-separated) and fails
//! if any check differs from `expected.toml`. Ignored by default: it builds
//! and runs a container per connector and TLS mode, a few minutes per
//! endpoint. Needs Docker (`SPRIG_TEST_DOCKER=1`).
//!
//! ```sh
//! # The references, directly (a MySQL 8.4 on 13384, see the README):
//! SPRIG_TEST_DOCKER=1 SPRIG_TEST_CONNECTORS=127.0.0.1:13301:mariadb,127.0.0.1:13384:mysql \
//!   cargo test -p sprig-testkit --test connectors_matrix -- --ignored --nocapture
//! # The gateway:
//! SPRIG_TEST_DOCKER=1 SPRIG_TEST_CONNECTORS=127.0.0.1:3306:mysql,127.0.0.1:3307:mariadb \
//!   cargo test -p sprig-testkit --test connectors_matrix -- --ignored --nocapture
//! ```
//!
//! `SPRIG_TEST_CONNECTORS_ONLY` (space- or comma-separated) runs a subset,
//! for example `mysql2,pymysql`; `SPRIG_TEST_CONNECTORS_QUICK=1` passes
//! `--quick` (drivers and `SQLAlchemy`, fast checks only: the per-PR run);
//! `run.sh`'s own `SPRIG_CONN_*` variables pass through.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::process::Command;

use sprig_testkit::connectors::{self, parse_targets};
use sprig_testkit::{require_env, skip};

#[test]
#[ignore = "runs the connector containers; set SPRIG_TEST_DOCKER=1 and SPRIG_TEST_CONNECTORS"]
fn connector_matrix() {
    require_env!(skip::DOCKER);
    let targets = parse_targets(&require_env!(connectors::CONNECTORS)).unwrap();
    let only: Vec<String> = skip::var("SPRIG_TEST_CONNECTORS_ONLY")
        .map(|v| {
            v.split([',', ' '])
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let quick = skip::var("SPRIG_TEST_CONNECTORS_QUICK").is_some_and(|v| v != "0");
    let script = connectors::run_script();
    let mut failed = Vec::new();
    for t in &targets {
        eprintln!("== connector matrix: {}:{} ({})", t.host, t.port, t.persona);
        let status = Command::new("bash")
            .arg(&script)
            .args(quick.then_some("--quick"))
            .args([&t.host, &t.port.to_string(), &t.persona])
            .args(&only)
            .status()
            .unwrap_or_else(|e| panic!("running {}: {e}", script.display()));
        if !status.success() {
            failed.push(format!("{}:{} ({}): {status}", t.host, t.port, t.persona));
        }
    }
    assert!(
        failed.is_empty(),
        "connector matrix failed for: {}",
        failed.join("; ")
    );
}
