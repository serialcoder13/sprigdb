//! The tier 1 connector matrix and the latency gate (chapter 46, step 5;
//! chapter 05, step 13; milestone M0.2's "done when" in chapter 48).
//!
//! The matrix itself lives in `deploy/compose/connectors/`: one small
//! program per client (MySQL and MariaDB `Connector/J`, `go-sql-driver`,
//! `mysql2`, `PyMySQL`, `mysqlclient`, `PDO_MySQL`, `mysqli`, the `mysql` and
//! `mariadb` CLIs) and sysbench, each in its own container, driven by
//! `run.sh <host> <port> <persona>` and compared with `expected.toml`. This
//! module holds what the Rust tests share:
//!
//! - [`MatrixTarget`] and [`parse_targets`]: the endpoints of
//!   [`CONNECTORS`], for `tests/connectors_matrix.rs`, which runs
//!   [`run_script`] once per endpoint;
//! - [`latency`]: percentiles for `tests/latency_gate.rs`, which compares
//!   point selects and inserts through a target (the gateway) with a
//!   baseline (MariaDB directly).
//!
//! | Variable | Holds | Example |
//! |---|---|---|
//! | [`CONNECTORS`] | `host:port:persona` endpoints, comma-separated | `127.0.0.1:3306:mysql,127.0.0.1:3307:mariadb` |
//! | [`LATENCY_TARGET`] | the endpoint under test | `mysql://root:sprig-dev-root@127.0.0.1:3307` |
//! | [`LATENCY_BASELINE`] | direct MariaDB; defaults to `SPRIG_TEST_MARIADB_SINGLE` | `mysql://root:sprig-dev-root@127.0.0.1:13301` |
//! | [`LATENCY_SAMPLES`] | samples per workload and endpoint (2000) | `5000` |
//! | [`LATENCY_MAX_P50_DELTA_MS`] | the gate (0.5, M0.2) | `0.3` |
//! | [`LATENCY_MAX_P99_DELTA_MS`] | an optional p99 gate (unset: report only) | `1` |

use std::path::PathBuf;

pub mod latency;

/// Endpoints for the connector matrix: `host:port:persona`, comma-separated;
/// persona is `mysql` or `mariadb`.
pub const CONNECTORS: &str = "SPRIG_TEST_CONNECTORS";
/// The endpoint the latency gate measures (a `mysql://` URL).
pub const LATENCY_TARGET: &str = "SPRIG_TEST_LATENCY_TARGET";
/// The latency gate's baseline (a `mysql://` URL); defaults to
/// [`crate::skip::MARIADB_SINGLE`].
pub const LATENCY_BASELINE: &str = "SPRIG_TEST_LATENCY_BASELINE";
/// Samples per workload and endpoint.
pub const LATENCY_SAMPLES: &str = "SPRIG_TEST_LATENCY_SAMPLES";
/// The largest p50 the target may add over the baseline, in milliseconds.
pub const LATENCY_MAX_P50_DELTA_MS: &str = "SPRIG_TEST_LATENCY_MAX_P50_DELTA_MS";
/// The largest p99 the target may add, in milliseconds; unset reports only.
pub const LATENCY_MAX_P99_DELTA_MS: &str = "SPRIG_TEST_LATENCY_MAX_P99_DELTA_MS";

/// One endpoint of the connector matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixTarget {
    /// Host as the machine running `run.sh` reaches it.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// `mysql` (the 3306 persona, MySQL 8.4) or `mariadb` (3307, MariaDB).
    pub persona: String,
}

/// Parses [`CONNECTORS`]: `host:port:persona` items, comma-separated.
///
/// # Errors
/// An item without three parts, with a bad port, or with a persona other
/// than `mysql` or `mariadb`.
pub fn parse_targets(value: &str) -> Result<Vec<MatrixTarget>, String> {
    crate::skip::urls(value)
        .iter()
        .map(|item| {
            let mut parts = item.rsplitn(3, ':');
            let (Some(persona), Some(port), Some(host)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Err(format!("{item}: want host:port:persona"));
            };
            let port = port
                .parse()
                .map_err(|e| format!("{item}: bad port {port:?}: {e}"))?;
            if persona != "mysql" && persona != "mariadb" {
                return Err(format!("{item}: persona must be mysql or mariadb"));
            }
            Ok(MatrixTarget {
                host: host.to_owned(),
                port,
                persona: persona.to_owned(),
            })
        })
        .collect()
}

/// `deploy/compose/connectors/run.sh` in this checkout.
#[must_use]
pub fn run_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/compose/connectors/run.sh")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_targets() {
        let t = parse_targets("127.0.0.1:3306:mysql, gw.local:3307:mariadb").unwrap();
        assert_eq!(
            t,
            vec![
                MatrixTarget {
                    host: "127.0.0.1".into(),
                    port: 3306,
                    persona: "mysql".into()
                },
                MatrixTarget {
                    host: "gw.local".into(),
                    port: 3307,
                    persona: "mariadb".into()
                },
            ]
        );
        assert!(parse_targets("127.0.0.1:3306").is_err());
        assert!(parse_targets("127.0.0.1:x:mysql").is_err());
        assert!(parse_targets("127.0.0.1:3306:postgres").is_err());
    }

    #[test]
    fn run_script_exists() {
        assert!(run_script().is_file(), "{}", run_script().display());
    }
}
