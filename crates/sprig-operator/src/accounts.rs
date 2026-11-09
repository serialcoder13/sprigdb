//! Step 4 of the reconcile: the system accounts `sprig_ctl`, `sprig_repl`
//! and `sprig_gw` (the gateways' monitoring account, `SLAVE MONITOR` only,
//! its password derived from `sprig_ctl`'s), created over TLS with the root credentials in
//! mariadb-operator's Secret (chapter 27, "The Kubernetes operator"), and
//! the cluster's `gtid_domain_id` once registration gave it a number.
//!
//! **Where it differs from the book.** The book's accounts accept only the
//! gateway's or worker's certificate subject (`REQUIRE SUBJECT`). The
//! system-account credentials SprigDB resolves today are a password over
//! TLS without a client certificate (`sprig-provision::credentials`), so
//! the accounts here are `REQUIRE SSL` with a generated password; they move
//! to `REQUIRE SUBJECT` when credentials carry client certificates.
//! `sprig_ctl` holds `SUPER` (managed clusters: the controller sets tuned
//! values live).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};
use sprig_common::{Result, SprigCode, SprigError};

/// What step 4 needs about one cluster.
#[derive(Clone, PartialEq, Eq)]
pub struct BootstrapTarget {
    /// The `SprigCluster`'s name.
    pub cluster: String,
    /// `host:port` of the writable node (the primary Service, or any
    /// Galera node).
    pub writer: String,
    /// Every node's `host:port` (for the live `gtid_domain_id`).
    pub nodes: Vec<String>,
    /// mariadb-operator's root password.
    pub root_password: String,
    /// `sprig_ctl`'s password.
    pub ctl_password: String,
    /// `sprig_repl`'s password.
    pub repl_password: String,
}

impl std::fmt::Debug for BootstrapTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BootstrapTarget")
            .field("cluster", &self.cluster)
            .field("writer", &self.writer)
            .field("nodes", &self.nodes)
            .finish_non_exhaustive()
    }
}

/// Creates the system accounts and sets live values.
#[async_trait]
pub trait SystemAccounts: Send + Sync + 'static {
    /// Creates (or re-asserts) `sprig_ctl`, `sprig_repl` and `sprig_gw` on the writer.
    ///
    /// # Errors
    /// The server's errors; the reconcile retries.
    async fn ensure(&self, t: &BootstrapTarget) -> Result<()>;
    /// Sets `gtid_domain_id` (and Galera's `wsrep_gtid_domain_id`) to
    /// `domain` on every node where it differs; returns how many changed.
    ///
    /// # Errors
    /// The servers' errors.
    async fn ensure_domain(&self, t: &BootstrapTarget, domain: u32) -> Result<usize>;
}

/// A generated password: 24 random bytes as hex.
///
/// # Errors
/// Internal if the OS generator fails.
pub fn generate_password() -> Result<String> {
    let mut b = [0u8; 24];
    getrandom::fill(&mut b).map_err(|e| SprigError::internal(format!("getrandom: {e}")))?;
    Ok(b.iter().fold(String::with_capacity(48), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    }))
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The statements of step 4, in order; idempotent.
#[must_use]
pub fn account_sql(ctl_password: &str, repl_password: &str) -> Vec<String> {
    let (ctl, repl) = (quote(ctl_password), quote(repl_password));
    vec![
        format!("CREATE USER IF NOT EXISTS 'sprig_ctl'@'%' IDENTIFIED BY {ctl} REQUIRE SSL"),
        format!("ALTER USER 'sprig_ctl'@'%' IDENTIFIED BY {ctl} REQUIRE SSL"),
        "GRANT ALL PRIVILEGES ON `t%`.* TO 'sprig_ctl'@'%' WITH GRANT OPTION".into(),
        "GRANT ALL PRIVILEGES ON `__sprig_sys`.* TO 'sprig_ctl'@'%' WITH GRANT OPTION".into(),
        "GRANT SUPER, CREATE USER, PROCESS, CONNECTION ADMIN, BINLOG MONITOR, SLAVE MONITOR, \
         RELOAD, READ_ONLY ADMIN, BINLOG ADMIN, SET USER ON *.* TO 'sprig_ctl'@'%'"
            .into(),
        // Chapter 09's full list: on kind the probes reported these three
        // missing (SET USER gates compiled logic). The gateway's per-user
        // backend accounts (chapter 29) read the grant tables as sprig_ctl.
        "GRANT SELECT ON mysql.db TO 'sprig_ctl'@'%'".into(),
        "GRANT SELECT ON mysql.global_priv TO 'sprig_ctl'@'%'".into(),
        "GRANT SELECT ON mysql.tables_priv TO 'sprig_ctl'@'%'".into(),
        "GRANT SELECT ON mysql.columns_priv TO 'sprig_ctl'@'%'".into(),
        "GRANT SELECT ON mysql.procs_priv TO 'sprig_ctl'@'%'".into(),
        format!("CREATE USER IF NOT EXISTS 'sprig_repl'@'%' IDENTIFIED BY {repl} REQUIRE SSL"),
        format!("ALTER USER 'sprig_repl'@'%' IDENTIFIED BY {repl} REQUIRE SSL"),
        "GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO 'sprig_repl'@'%'".into(),
    ]
    .into_iter()
    // The gateways' monitoring account (chapter 09): `SLAVE MONITOR` only,
    // its password derived from sprig_ctl's, as the gateways derive it.
    .chain(sprig_provision::gw_account::account_sql(
        &sprig_provision::gw_account::derived_password(ctl_password),
    ))
    .collect()
}

/// The credentials file the controllers read (`file:<dir>/<cluster>.toml`,
/// sections `sprig_ctl` and `sprig_repl`), kept in one Secret mounted on
/// controllers and workers.
#[must_use]
pub fn credentials_toml(ctl_password: &str, repl_password: &str, ca_file: Option<&str>) -> String {
    let tls = match ca_file {
        Some(ca) => format!("tls = \"verify\"\nca_file = \"{ca}\"\n"),
        None => "tls = \"skip_verify\"\n".into(),
    };
    format!(
        "# Written by the SprigDB operator (chapter 27).\n\
         [sprig_ctl]\nuser = \"sprig_ctl\"\npassword = \"{ctl_password}\"\n{tls}\n\
         [sprig_repl]\nuser = \"sprig_repl\"\npassword = \"{repl_password}\"\n{tls}"
    )
}

/// [`SystemAccounts`] over `mysql_async`, as root, over TLS.
pub struct MysqlAccounts {
    /// The CA that signs the servers' certificates; `None`: encrypt
    /// without verifying (development).
    ca_file: Option<std::path::PathBuf>,
    pools: Mutex<HashMap<(String, String), Pool>>,
}

impl std::fmt::Debug for MysqlAccounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlAccounts")
            .field("ca_file", &self.ca_file)
            .finish_non_exhaustive()
    }
}

impl MysqlAccounts {
    /// Connections verified against `ca_file` (or not verified).
    #[must_use]
    pub fn new(ca_file: Option<std::path::PathBuf>) -> Self {
        Self {
            ca_file,
            pools: Mutex::new(HashMap::new()),
        }
    }

    fn pool(&self, addr: &str, password: &str) -> Result<Pool> {
        let k = (addr.to_owned(), password.to_owned());
        let mut pools = self.pools.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(p) = pools.get(&k) {
            return Ok(p.clone());
        }
        let (host, port) = addr
            .rsplit_once(':')
            .and_then(|(h, p)| Some((h.to_owned(), p.parse::<u16>().ok()?)))
            .ok_or_else(|| SprigError::internal(format!("bad node address {addr}")))?;
        let ssl = match &self.ca_file {
            Some(ca) => SslOpts::default().with_root_certs(vec![ca.clone().into()]),
            None => SslOpts::default().with_danger_accept_invalid_certs(true),
        };
        let constraints =
            PoolConstraints::new(0, 2).ok_or_else(|| SprigError::internal("pool constraints"))?;
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(password.to_owned()))
            .ssl_opts(Some(ssl))
            .pool_opts(
                PoolOpts::default()
                    .with_constraints(constraints)
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            );
        let p = Pool::new(opts);
        pools.insert(k, p.clone());
        Ok(p)
    }
}

impl MysqlAccounts {
    /// Sets `gtid_domain_id` (and Galera's) on one node; 1 if it changed.
    async fn set_domain(&self, addr: &str, root: &str, domain: u32) -> Result<usize> {
        let mut c = self.pool(addr, root)?.get_conn().await.map_err(server)?;
        let cur: Option<(u32, Option<i64>)> = c
            .query_first("SELECT @@GLOBAL.gtid_domain_id, @@GLOBAL.wsrep_on")
            .await
            .map_err(server)?;
        let Some((cur, wsrep)) = cur else {
            return Ok(0);
        };
        let mut changed = 0;
        if cur != domain {
            c.query_drop(format!("SET GLOBAL gtid_domain_id = {domain}"))
                .await
                .map_err(server)?;
            changed = 1;
        }
        if wsrep == Some(1) {
            c.query_drop(format!("SET GLOBAL wsrep_gtid_domain_id = {domain}"))
                .await
                .map_err(server)?;
        }
        Ok(changed)
    }
}

fn server(e: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::ClusterUnavailable,
        format!("system accounts: {e}"),
    )
}

#[async_trait]
impl SystemAccounts for MysqlAccounts {
    async fn ensure(&self, t: &BootstrapTarget) -> Result<()> {
        let mut c = self
            .pool(&t.writer, &t.root_password)?
            .get_conn()
            .await
            .map_err(server)?;
        for q in account_sql(&t.ctl_password, &t.repl_password) {
            c.query_drop(q).await.map_err(server)?;
        }
        Ok(())
    }

    async fn ensure_domain(&self, t: &BootstrapTarget, domain: u32) -> Result<usize> {
        // Every node that answers gets the domain, even when another is
        // down (a pod restarted after a failover comes back in domain 0
        // while the cluster isn't Ready); the first error comes back after.
        let mut changed = 0;
        let mut first_err = None;
        for addr in &t.nodes {
            match self.set_domain(addr, &t.root_password, domain).await {
                Ok(c) => changed += c,
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) if changed == 0 => Err(e),
            Some(e) => {
                tracing::debug!(error = %e, "gtid_domain_id not set on every node");
                Ok(changed)
            }
            None => Ok(changed),
        }
    }
}

/// Keeps every pool open until dropped; tests use [`Arc`]s of fakes.
pub type SharedAccounts = Arc<dyn SystemAccounts>;

#[cfg(test)]
mod tests {
    use super::*;

    /// The statements for the container check (`SPRIG_OPERATOR_LIVE_DIR`).
    #[test]
    fn write_live_fixture() {
        if let Some(d) = std::env::var_os("SPRIG_OPERATOR_LIVE_DIR") {
            let mut sql = account_sql("ctlpw-r2prov", "replpw-r2prov").join(";\n");
            sql.push_str(";\n");
            std::fs::write(std::path::Path::new(&d).join("accounts.sql"), sql).unwrap_or(());
        }
    }

    #[test]
    fn statements_are_idempotent_and_quoted() {
        let sql = account_sql("ab'c", "r");
        assert!(sql[0].starts_with("CREATE USER IF NOT EXISTS 'sprig_ctl'@'%'"));
        assert!(sql[0].contains("'ab\\'c'"));
        assert!(sql.iter().all(|s| !s.contains("REQUIRE SUBJECT")));
        assert!(sql.iter().any(|s| s.contains("REPLICATION SLAVE")));
        assert!(
            sql.iter()
                .any(|s| s.contains("BINLOG ADMIN, SET USER ON *.*"))
        );
        assert!(sql.iter().any(|s| s.contains("SELECT ON mysql.db")));
        assert!(
            sql.iter()
                .any(|s| s == "GRANT SLAVE MONITOR ON *.* TO 'sprig_gw'@'%'")
        );
        let p = generate_password().unwrap_or_default();
        assert_eq!(p.len(), 48);
        assert!(p.bytes().all(|b| b.is_ascii_hexdigit()));
        let t = credentials_toml("c", "r", Some("/etc/sprigdb/tls/mariadb-ca.pem"));
        let v: toml::Value = toml::from_str(&t).unwrap_or(toml::Value::Integer(0));
        assert_eq!(v["sprig_ctl"]["password"].as_str(), Some("c"));
        assert_eq!(v["sprig_repl"]["tls"].as_str(), Some("verify"));
    }
}
