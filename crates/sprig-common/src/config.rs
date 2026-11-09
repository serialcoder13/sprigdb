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
    /// Gateway runtime sizing and the interim accounts file.
    #[serde(default)]
    pub gateway: GatewayConfig,
    /// Controller runtime sizing and start-up switches.
    #[serde(default)]
    pub controller: ControllerConfig,
    /// Worker runtime sizing.
    #[serde(default)]
    pub worker: RuntimeConfig,
    /// Node agent runtime sizing and the node it fences (chapter 25).
    #[serde(default)]
    pub agent: AgentConfig,
    /// Kubernetes operator runtime sizing.
    #[serde(default)]
    pub operator: RuntimeConfig,
    /// Telemetry until settings arrive from the control plane.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    /// The audit spool and its sink (chapter 47).
    #[serde(default)]
    pub audit: AuditConfig,
}

/// `[audit]`: where this process spools audit events and where its
/// shipper writes them (chapter 47, "The audit pipeline"). Without
/// `spool_dir` the process has no pipeline and audit events go only to
/// the log (target `sprig_audit`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    /// The local spool directory (one per process).
    #[serde(default)]
    pub spool_dir: Option<PathBuf>,
    /// The sink: a directory that receives chapter 47's object-storage
    /// layout (`audit/<stream>/<date>/...`). Required with `spool_dir`.
    #[serde(default)]
    pub sink_dir: Option<PathBuf>,
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
    /// The deployment this process belongs to; must match the global
    /// `system.deployment_id` setting (chapter 23, "Bootstrapping the first
    /// DC"). Empty means not configured.
    #[serde(default)]
    pub deployment_id: String,
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

/// `[gateway]`: runtime sizing and where logins come from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    /// Tokio worker threads; 0 means one per CPU.
    #[serde(default)]
    pub worker_threads: usize,
    /// Where logins come from: `catalog` (chapter 14: tenants, their
    /// catalogs' users, logical names) or `accounts_file` (M0.2's interim
    /// file, raw clusters, names unchanged). Unset: `accounts_file` when
    /// [`GatewayConfig::accounts_file`] is set, else `catalog`
    /// ([`GatewayConfig::effective_login_mode`]).
    #[serde(default)]
    pub login_mode: Option<LoginModeConfig>,
    /// Interim (M0.2, decision D1 in the plan's `decisions/m0.2-interim.md`),
    /// used in `accounts_file` mode only: the TOML file listing the logins
    /// the gateway accepts, each with its chapter 14 password verifier,
    /// cluster, backend account and backend `credentials_ref` (format in
    /// `sprig_auth::accounts`). Re-read within a second of a change. Unset,
    /// or a missing file, means no accounts: every login fails with 1045.
    #[serde(default)]
    pub accounts_file: Option<PathBuf>,
    /// Interim (chapter 14, "Backend target, interim"), used in `catalog`
    /// mode until chapter 29's per-user backend accounts (M0.5): the TOML
    /// file naming the backend account each tenant's sessions use, or a
    /// default for every tenant (format in `sprig_gateway::tenant::backends`).
    /// Without one, a verified catalog login fails with 39998.
    #[serde(default)]
    pub backends_file: Option<PathBuf>,
    /// This process's own listener addresses, by listener name (`mysql =
    /// "127.0.0.1:23306"`), replacing the DC's `listener.<name>.bind`
    /// settings: a second gateway in the same DC (development, tests).
    #[serde(default)]
    pub listen_override: std::collections::BTreeMap<String, String>,
    /// Chapter 12's sealed parameter samples: the file of versioned master
    /// secrets the tenants' workload data keys derive from (format in
    /// `sprig_workload::keyring`). Unset: no samples are captured, and
    /// statistics continue.
    #[serde(default)]
    pub workload_key_file: Option<PathBuf>,
    /// Turns chapter 12's workload capture off on this gateway (the hook
    /// records nothing, as before M0.4): an operator's kill switch, and the
    /// baseline of the overhead measurement.
    #[serde(default)]
    pub workload_capture_off: bool,
    /// The host other gateways, load balancers and `sprigctl` reach this
    /// gateway at, published in its registration record (chapter 04,
    /// "Registration records"). Unset: the hostname.
    #[serde(default)]
    pub advertise_addr: Option<String>,
}

/// `[gateway] login_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoginModeConfig {
    /// M0.2's interim accounts file.
    AccountsFile,
    /// Tenant catalogs (chapter 14).
    Catalog,
}

impl GatewayConfig {
    /// The login mode in force: the configured one, else `accounts_file`
    /// when an accounts file is configured (so M0.2 configs keep working),
    /// else `catalog`.
    #[must_use]
    pub fn effective_login_mode(&self) -> LoginModeConfig {
        self.login_mode.unwrap_or(if self.accounts_file.is_some() {
            LoginModeConfig::AccountsFile
        } else {
            LoginModeConfig::Catalog
        })
    }
}

/// `[controller]`: runtime sizing plus the start-up switches of chapter 23.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfig {
    /// Tokio worker threads; 0 means one per CPU.
    #[serde(default)]
    pub worker_threads: usize,
    /// Write the first DC's deployment records when the global tier has
    /// none (chapter 23, "Bootstrapping the first DC").
    #[serde(default)]
    pub bootstrap: bool,
    /// Campaign for the global leader as well as the DC leader.
    #[serde(default)]
    pub global_candidate: bool,
    /// Where the controller serves the Admin API's REST face (chapter 37)
    /// until the gateway's HTTP listeners serve it. Port 0 picks a free
    /// port.
    #[serde(default = "default_admin_addr")]
    pub admin_addr: SocketAddr,
    /// A file holding the bearer token Admin API calls must present. Without
    /// one, `admin_addr` must be a loopback address.
    #[serde(default)]
    pub admin_token_file: Option<PathBuf>,
    /// Chapter 12's sealed parameter samples: the same file of versioned
    /// master secrets as the gateway's `[gateway] workload_key_file`, from
    /// which automatic-index validation opens the samples it replays
    /// (chapter 13). Unset: validation has no samples and falls back to
    /// estimates and a canary.
    #[serde(default)]
    pub workload_key_file: Option<PathBuf>,
    /// Development and scenario tests only: a file holding whole seconds by
    /// which the automatic-indexing loop's clock runs ahead (chapter 13's
    /// observation periods in minutes instead of days). Unset in
    /// production.
    #[serde(default)]
    pub autoindex_test_clock_file: Option<PathBuf>,
    /// Where the controller serves chapter 25's internal services
    /// (`Permits.Renew`, `Peer.Ping`, `Observer.ObserveDc`, and chapter
    /// 23's relay reads) as JSON over HTTP until the mTLS gRPC port 7400
    /// exists. Published in the controller's registration, so gateways,
    /// agents and other DCs' observers find it. Port 0 picks a free port.
    #[serde(default = "default_internal_addr")]
    pub internal_addr: SocketAddr,
    /// The address other processes reach `internal_addr` at, when it
    /// differs from the bound one (a published port, a proxy). Unset: the
    /// bound address (an unspecified IP becomes the hostname).
    #[serde(default)]
    pub internal_advertise: Option<String>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            worker_threads: 0,
            bootstrap: false,
            global_candidate: false,
            admin_addr: default_admin_addr(),
            admin_token_file: None,
            workload_key_file: None,
            autoindex_test_clock_file: None,
            internal_addr: default_internal_addr(),
            internal_advertise: None,
        }
    }
}

/// The internal services' default address: loopback, any free port.
fn default_internal_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// `[agent]`: runtime sizing and the MariaDB node the agent fences
/// (chapter 25, "DC leases, sub-leases and self-fencing" steps 6 and 9;
/// chapter 27's plan engine comes later).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// Tokio worker threads; 0 means one per CPU.
    #[serde(default)]
    pub worker_threads: usize,
    /// The cluster the node belongs to, such as `fra1-c1`. Unset: the
    /// agent fences nothing (the chapter 27 skeleton).
    #[serde(default)]
    pub cluster: Option<String>,
    /// The node's id in the cluster record. Unset: the cluster's node
    /// whose address is [`AgentConfig::mysql_addr`].
    #[serde(default)]
    pub node: Option<String>,
    /// `host:port` of the local MariaDB. Unset: the node's address in the
    /// cluster record.
    #[serde(default)]
    pub mysql_addr: Option<String>,
    /// The secret reference of the account the agent fences with (needs
    /// `READ_ONLY ADMIN` and `CONNECTION ADMIN`), `file:<path>#<table>`.
    /// Unset: the cluster's `sprig_ctl` reference.
    #[serde(default)]
    pub credentials: Option<String>,
}

/// The Admin API's default address on a controller: loopback, port 7480.
fn default_admin_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 7480))
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
    "audit",
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

    /// The runtime sizing of `role`.
    #[must_use]
    pub fn runtime(&self, role: crate::Role) -> RuntimeConfig {
        match role {
            crate::Role::Gateway => RuntimeConfig {
                worker_threads: self.gateway.worker_threads,
            },
            crate::Role::Controller => RuntimeConfig {
                worker_threads: self.controller.worker_threads,
            },
            crate::Role::Worker => self.worker.clone(),
            crate::Role::Agent => RuntimeConfig {
                worker_threads: self.agent.worker_threads,
            },
            crate::Role::Operator => self.operator.clone(),
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
    fn controller_switches_and_deployment_id() {
        let c = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n[meta]\ndeployment_id = \"dep-1\"\n[controller]\nbootstrap = true\nworker_threads = 2\n",
            env(&[("SPRIGDB_CONTROLLER_GLOBAL_CANDIDATE", "true")]),
        )
        .unwrap();
        assert_eq!(c.meta.deployment_id, "dep-1");
        assert!(c.controller.bootstrap && c.controller.global_candidate);
        assert_eq!(c.runtime(crate::Role::Controller).worker_threads, 2);
        let d = ProcessConfig::from_toml_and_env("[node]\ndc = \"mum1\"\n", env(&[])).unwrap();
        assert!(!d.controller.bootstrap && !d.controller.global_candidate);
        assert!(d.meta.deployment_id.is_empty());
        assert_eq!(d.controller.admin_addr.to_string(), "127.0.0.1:7480");
        assert!(d.controller.admin_token_file.is_none());
        assert!(d.controller.workload_key_file.is_none());
        let admin = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n",
            env(&[("SPRIGDB_CONTROLLER_ADMIN_ADDR", "0.0.0.0:7481")]),
        )
        .unwrap();
        assert_eq!(admin.controller.admin_addr.port(), 7481);
        // Controller switches belong to [controller] only.
        let wrong = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n[gateway]\nbootstrap = true\n",
            env(&[]),
        );
        assert!(wrong.is_err());
    }

    #[test]
    fn gateway_accounts_file() {
        let c = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n[gateway]\nworker_threads = 2\naccounts_file = \"/etc/sprigdb/accounts.toml\"\n",
            env(&[]),
        )
        .unwrap();
        assert_eq!(
            c.gateway.accounts_file,
            Some(PathBuf::from("/etc/sprigdb/accounts.toml"))
        );
        assert_eq!(c.runtime(crate::Role::Gateway).worker_threads, 2);
        let d = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n",
            env(&[("SPRIGDB_GATEWAY_ACCOUNTS_FILE", "/tmp/a.toml")]),
        )
        .unwrap();
        assert_eq!(d.gateway.accounts_file, Some(PathBuf::from("/tmp/a.toml")));
        let none = ProcessConfig::from_toml_and_env("[node]\ndc = \"mum1\"\n", env(&[])).unwrap();
        assert!(none.gateway.accounts_file.is_none());
        // The login mode: explicit, else the accounts file decides.
        assert_eq!(
            c.gateway.effective_login_mode(),
            LoginModeConfig::AccountsFile
        );
        assert_eq!(
            none.gateway.effective_login_mode(),
            LoginModeConfig::Catalog
        );
        let cat = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n[gateway]\nlogin_mode = \"catalog\"\naccounts_file = \"/a\"\nbackends_file = \"/b\"\n",
            env(&[]),
        )
        .unwrap();
        assert_eq!(cat.gateway.effective_login_mode(), LoginModeConfig::Catalog);
        assert_eq!(cat.gateway.backends_file, Some(PathBuf::from("/b")));
        let by_env = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n",
            env(&[("SPRIGDB_GATEWAY_LOGIN_MODE", "accounts_file")]),
        )
        .unwrap();
        assert_eq!(
            by_env.gateway.effective_login_mode(),
            LoginModeConfig::AccountsFile
        );
        // Gateway-only: other roles' sections refuse it.
        let wrong = ProcessConfig::from_toml_and_env(
            "[node]\ndc = \"mum1\"\n[worker]\naccounts_file = \"/a\"\n",
            env(&[]),
        );
        assert!(wrong.is_err());
    }

    #[test]
    fn hostname_is_the_default_id() {
        let c = ProcessConfig::from_toml_and_env("[node]\ndc = \"mum1\"\n", env(&[])).unwrap();
        assert!(!c.node.id().is_empty());
    }
}
