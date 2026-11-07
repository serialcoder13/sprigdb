//! Process configuration: `sprigdb.toml` (chapter 03, "Configuration and
//! settings"; writing guide 5.7).
//!
//! It holds only what a process needs to start: its identity, the
//! control-plane seeds, its own certificate, its runtime sizing and the
//! bootstrap telemetry. Everything else is a live policy setting
//! (`sprig-settings`).
//!
//! Environment variables override the file: `SPRIGDB_<SECTION>_<FIELD>`,
//! for example `SPRIGDB_NODE_DC=mum1` or
//! `SPRIGDB_TELEMETRY_BOOTSTRAP_LOG_LEVEL=debug`. A value is read as a TOML
//! value when it parses as one (`0`, `true`, `["a", "b"]`) and as a string
//! otherwise.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SprigCode, SprigError};
use crate::ids::DcId;

/// Prefix of environment overrides.
pub const ENV_PREFIX: &str = "SPRIGDB_";

/// The parsed `sprigdb.toml`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    /// This process's identity.
    pub node: NodeConfig,
    /// Control-plane seeds.
    #[serde(default)]
    pub meta: MetaConfig,
    /// This process's certificate for mutual TLS. Optional until mutual TLS
    /// is implemented.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Gateway runtime sizing.
    #[serde(default)]
    pub gateway: RuntimeConfig,
    /// Controller runtime sizing.
    #[serde(default)]
    pub controller: RuntimeConfig,
    /// Worker runtime sizing.
    #[serde(default)]
    pub worker: RuntimeConfig,
    /// Node agent runtime sizing.
    #[serde(default)]
    pub agent: RuntimeConfig,
    /// Kubernetes operator runtime sizing.
    #[serde(default)]
    pub operator: RuntimeConfig,
    /// Telemetry until settings arrive from the control plane.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

/// `[node]`: who this process is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Data center of this process.
    pub dc: DcId,
    /// Unique within the DC; defaults to the hostname.
    #[serde(default)]
    pub id: Option<String>,
}

impl NodeConfig {
    /// The configured id, or the hostname.
    #[must_use]
    pub fn id(&self) -> String {
        self.id
            .clone()
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned())
    }
}

/// `[meta]`: seeds only; etcd members are discovered and followed live.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaConfig {
    /// Endpoints of this DC's etcd tier.
    #[serde(default)]
    pub dc_seeds: Vec<String>,
    /// Endpoints of the global etcd tier.
    #[serde(default)]
    pub global_seeds: Vec<String>,
}

/// `[tls]`: this process's identity for mutual TLS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// CA bundle.
    pub ca: PathBuf,
    /// Certificate chain.
    pub cert: PathBuf,
    /// Private key.
    pub key: PathBuf,
}

/// A role's runtime sizing (`[gateway]`, `[controller]`, ...).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Tokio worker threads; 0 means one per CPU.
    #[serde(default)]
    pub worker_threads: usize,
}

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// JSON lines, for production.
    #[default]
    Json,
    /// Human-readable, for development.
    Pretty,
}

/// `[telemetry]`: what a process uses until its settings arrive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Log filter, in `tracing` directive syntax (`info`,
    /// `info,sprig_meta=debug`).
    #[serde(default = "default_log_level")]
    pub bootstrap_log_level: String,
    /// `json` or `pretty` (chapter 47).
    #[serde(default)]
    pub log_format: LogFormat,
    /// Where `/metrics`, `/healthz` and `/readyz` are served until the
    /// `listener.telemetry.*` settings take over (chapter 04).
    #[serde(default = "default_telemetry_addr")]
    pub bootstrap_addr: SocketAddr,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            bootstrap_log_level: default_log_level(),
            log_format: LogFormat::default(),
            bootstrap_addr: default_telemetry_addr(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_owned()
}

fn default_telemetry_addr() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 9180))
}

/// Sections that environment variables may override.
const SECTIONS: &[&str] = &[
    "node",
    "meta",
    "tls",
    "gateway",
    "controller",
    "worker",
    "agent",
    "operator",
    "telemetry",
];

impl ProcessConfig {
    /// Loads `path` and applies overrides from the process environment.
    /// A missing file is an error.
    ///
    /// # Errors
    /// `ConfigInvalid` if the file can't be read or parsed, or a value is
    /// invalid.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| config_error(format!("can't read {}", path.display())).with_source(e))?;
        Self::from_toml_and_env(&text, std::env::vars())
    }

    /// Builds the configuration from environment overrides alone, for
    /// processes started without a file.
    ///
    /// # Errors
    /// `ConfigInvalid` if a value is invalid or a required one is missing.
    pub fn from_env() -> Result<Self> {
        Self::from_toml_and_env("", std::env::vars())
    }

    /// Parses `text` and applies overrides from `env`. Variables without the
    /// `SPRIGDB_` prefix, or naming no known section, are ignored.
    ///
    /// # Errors
    /// `ConfigInvalid` if the text or a value is invalid.
    pub fn from_toml_and_env(
        text: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        let mut table: toml::Table = text
            .parse()
            .map_err(|e: toml::de::Error| config_error(format!("sprigdb.toml: {}", e.message())))?;
        for (name, raw) in env {
            let Some((section, field)) = split_env_name(&name) else {
                continue;
            };
            let section_table = table
                .entry(section.clone())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
            let toml::Value::Table(section_table) = section_table else {
                return Err(config_error(format!("[{section}] is not a table")));
            };
            section_table.insert(field, parse_env_value(&raw));
        }
        toml::Value::Table(table)
            .try_into()
            .map_err(|e: toml::de::Error| {
                config_error(format!(
                    "process configuration (file and SPRIGDB_* variables): {}",
                    e.message()
                ))
            })
    }

    /// The runtime section of `role`.
    #[must_use]
    pub fn runtime(&self, role: crate::Role) -> &RuntimeConfig {
        match role {
            crate::Role::Gateway => &self.gateway,
            crate::Role::Controller => &self.controller,
            crate::Role::Worker => &self.worker,
            crate::Role::Agent => &self.agent,
            crate::Role::Operator => &self.operator,
        }
    }
}

/// `SPRIGDB_TELEMETRY_BOOTSTRAP_LOG_LEVEL` → `("telemetry", "bootstrap_log_level")`.
fn split_env_name(name: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix(ENV_PREFIX)?.to_ascii_lowercase();
    let (section, field) = rest.split_once('_')?;
    (SECTIONS.contains(&section) && !field.is_empty())
        .then(|| (section.to_owned(), field.to_owned()))
}

fn parse_env_value(raw: &str) -> toml::Value {
    format!("v = {raw}")
        .parse::<toml::Table>()
        .ok()
        .and_then(|mut t| t.remove("v"))
        .unwrap_or_else(|| toml::Value::String(raw.to_owned()))
}

fn config_error(message: String) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
        [node]
        dc = "mum1"
        id = "gw-7"

        [meta]
        dc_seeds = ["https://etcd-mum1-0:2379", "https://etcd-mum1-1:2379"]
        global_seeds = ["https://etcd-g-mum1:2379", "https://etcd-g-fra1:2379"]

        [tls]
        ca = "/etc/sprigdb/tls/ca.pem"
        cert = "/etc/sprigdb/tls/node.pem"
        key = "/etc/sprigdb/tls/node-key.pem"

        [gateway]
        worker_threads = 0

        [telemetry]
        bootstrap_log_level = "info"
    "#;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn parses_chapter_03_example() {
        let c = ProcessConfig::from_toml_and_env(EXAMPLE, env(&[])).unwrap();
        assert_eq!(c.node.dc.as_str(), "mum1");
        assert_eq!(c.node.id(), "gw-7");
        assert_eq!(c.meta.dc_seeds.len(), 2);
        assert_eq!(c.tls.unwrap().ca, PathBuf::from("/etc/sprigdb/tls/ca.pem"));
        assert_eq!(c.gateway.worker_threads, 0);
        assert_eq!(c.telemetry.log_format, LogFormat::Json);
        assert_eq!(c.telemetry.bootstrap_addr.port(), 9180);
    }

    #[test]
    fn environment_overrides_the_file() {
        let c = ProcessConfig::from_toml_and_env(
            EXAMPLE,
            env(&[
                ("SPRIGDB_NODE_DC", "fra1"),
                ("SPRIGDB_GATEWAY_WORKER_THREADS", "4"),
                (
                    "SPRIGDB_TELEMETRY_BOOTSTRAP_LOG_LEVEL",
                    "debug,sprig_meta=trace",
                ),
                ("SPRIGDB_TELEMETRY_LOG_FORMAT", "pretty"),
                ("SPRIGDB_META_DC_SEEDS", r#"["http://etcd:2379"]"#),
                ("SPRIGDB_CONFIG", "/elsewhere.toml"), // not a section: ignored
                ("HOME", "/root"),                     // no prefix: ignored
            ]),
        )
        .unwrap();
        assert_eq!(c.node.dc.as_str(), "fra1");
        assert_eq!(c.gateway.worker_threads, 4);
        assert_eq!(c.telemetry.bootstrap_log_level, "debug,sprig_meta=trace");
        assert_eq!(c.telemetry.log_format, LogFormat::Pretty);
        assert_eq!(c.meta.dc_seeds, vec!["http://etcd:2379".to_owned()]);
    }

    #[test]
    fn environment_alone_is_enough() {
        let c = ProcessConfig::from_toml_and_env("", env(&[("SPRIGDB_NODE_DC", "dc-a")])).unwrap();
        assert_eq!(c.node.dc.as_str(), "dc-a");
        assert!(c.meta.dc_seeds.is_empty());
        assert!(c.tls.is_none());
    }

    #[test]
    fn rejects_invalid_input() {
        let missing_dc = ProcessConfig::from_toml_and_env("[node]\n", env(&[])).unwrap_err();
        assert_eq!(missing_dc.code(), Some(SprigCode::ConfigInvalid));

        let bad_dc = ProcessConfig::from_toml_and_env("[node]\ndc = \"MUM 1\"\n", env(&[]));
        assert!(bad_dc.is_err());

        let typo =
            ProcessConfig::from_toml_and_env("[node]\ndc = \"mum1\"\nidd = \"x\"\n", env(&[]));
        assert!(typo.is_err());

        let env_typo = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n",
            env(&[("SPRIGDB_NODE_IDD", "x")]),
        );
        assert!(env_typo.is_err());

        let syntax = ProcessConfig::from_toml_and_env("[node\n", env(&[])).unwrap_err();
        assert_eq!(syntax.code(), Some(SprigCode::ConfigInvalid));
    }

    #[test]
    fn hostname_is_the_default_id() {
        let c = ProcessConfig::from_toml_and_env("[node]\ndc = \"mum1\"\n", env(&[])).unwrap();
        assert!(!c.node.id().is_empty());
    }
}
