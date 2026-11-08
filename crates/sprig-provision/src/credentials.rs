//! System-account credentials: secret references and how they resolve
//! (chapter 09, "System accounts"; chapter 27, "External registration").
//!
//! etcd holds no secrets: a cluster record names its `sprig_ctl` and
//! `sprig_repl` credentials by reference, and only controllers and workers
//! resolve them. A reference is `scheme:location[#section]`:
//!
//! - `file:/etc/sprigdb/creds/fra1-ext1.toml` reads a TOML file (mode 0600
//!   or a mounted Kubernetes Secret);
//! - `env:SPRIG_CTL` reads a TOML document from an environment variable.
//!
//! With `#section`, the credential is the TOML table of that name, so one
//! file can hold both accounts. Chapter 27's `credentials_ref` names such a
//! file; [`refs_for`] turns it into the two references.
//!
//! ```toml
//! [sprig_ctl]
//! user = "sprig_ctl"
//! password = "..."
//! tls = "verify"             # or "skip_verify" (development only)
//! ca_file = "/etc/sprigdb/tls/mariadb-ca.pem"
//!
//! [sprig_repl]
//! password = "..."           # user defaults to the account's name
//! ```
//!
//! Every connection uses TLS: `verify` checks the server's certificate
//! against `ca_file` (or the built-in roots), `skip_verify` encrypts without
//! checking it, for development servers with self-signed certificates.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use mysql_async::{Opts, OptsBuilder, SslOpts};
use serde::{Deserialize, Serialize};
use sprig_cluster::driver::NodeConnector;
use sprig_cluster::record::{NodeSpec, SecretRefs};
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::{Result, SprigCode, SprigError};

/// The `sprig_ctl` account name, the default user of a control credential.
pub const CTL_USER: &str = "sprig_ctl";
/// The `sprig_repl` account name, the default user of a binlog credential.
pub const REPL_USER: &str = "sprig_repl";

/// How a connection checks the server's certificate. There is no plain
/// text mode: SprigDB refuses connections without TLS (chapter 27).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// Verify the certificate chain and host name.
    #[default]
    Verify,
    /// Encrypt without verifying: development only.
    SkipVerify,
}

/// A resolved credential. `Debug` never prints the password.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    /// The account.
    #[serde(default)]
    pub user: String,
    password: String,
    /// Certificate checking.
    #[serde(default)]
    pub tls: TlsMode,
    /// CA bundle for [`TlsMode::Verify`]; the built-in roots without it.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("user", &self.user)
            .field("tls", &self.tls)
            .field("ca_file", &self.ca_file)
            .finish_non_exhaustive()
    }
}

impl Credential {
    /// A credential from parts (tests and tools).
    #[must_use]
    pub fn new(user: impl Into<String>, password: impl Into<String>, tls: TlsMode) -> Self {
        Self {
            user: user.into(),
            password: password.into(),
            tls,
            ca_file: None,
        }
    }

    /// The password, for clients that log in themselves (the gateway's
    /// backend client, chapter 06). Never log it.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }

    /// An endpoint for `node` with this credential: TCP only (never the
    /// server's socket file, which would bypass TLS) and TLS always.
    ///
    /// # Errors
    /// 39998 `ER_SPRIG_CONFIG_INVALID` if the node's address isn't
    /// `host:port`.
    pub fn endpoint(&self, node: &NodeSpec) -> Result<NodeEndpoint> {
        let (host, port) = split_addr(&node.addr)?;
        let mut ssl = SslOpts::default();
        match self.tls {
            TlsMode::Verify => {
                if let Some(ca) = &self.ca_file {
                    ssl = ssl.with_root_certs(vec![ca.clone().into()]);
                }
            }
            TlsMode::SkipVerify => {
                ssl = ssl
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true);
            }
        }
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .prefer_socket(false)
            .ssl_opts(ssl);
        Ok(NodeEndpoint {
            node: node.node.clone(),
            opts: Opts::from(opts),
        })
    }
}

fn invalid(message: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, message)
}

/// `host:port` or `[v6]:port`.
fn split_addr(addr: &str) -> Result<(String, u16)> {
    let bad = || invalid(format!("node address {addr:?}: expected host:port"));
    let (host, port) = addr.rsplit_once(':').ok_or_else(bad)?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(bad());
    }
    Ok((host.to_owned(), port.parse().map_err(|_| bad())?))
}

/// Where a secret lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretSource {
    /// A file path.
    File(PathBuf),
    /// An environment variable.
    Env(String),
}

/// A parsed secret reference: `file:<path>[#section]` or
/// `env:<name>[#section]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRef {
    /// Where the TOML document is.
    pub source: SecretSource,
    /// The table to use, if not the top level.
    pub section: Option<String>,
}

impl FromStr for SecretRef {
    type Err = SprigError;

    fn from_str(s: &str) -> Result<Self> {
        let (body, section) = match s.split_once('#') {
            Some((b, sec)) if !sec.is_empty() => (b, Some(sec.to_owned())),
            Some(_) => return Err(invalid(format!("secret reference {s:?}: empty section"))),
            None => (s, None),
        };
        let source = match body.split_once(':') {
            Some(("file", p)) if !p.is_empty() => SecretSource::File(PathBuf::from(p)),
            Some(("env", v)) if !v.is_empty() => SecretSource::Env(v.to_owned()),
            _ => {
                return Err(invalid(format!(
                    "secret reference {s:?}: expected file:<path> or env:<name>, \
                     optionally with #<section>"
                )));
            }
        };
        Ok(Self { source, section })
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            SecretSource::File(p) => write!(f, "file:{}", p.display())?,
            SecretSource::Env(v) => write!(f, "env:{v}")?,
        }
        if let Some(s) = &self.section {
            write!(f, "#{s}")?;
        }
        Ok(())
    }
}

impl SecretRef {
    /// Reads and parses the credential. `default_user` fills an absent
    /// `user`.
    ///
    /// # Errors
    /// 39998 `ER_SPRIG_CONFIG_INVALID` if the secret can't be read or
    /// parsed; the message never contains the secret.
    pub fn resolve(&self, default_user: &str) -> Result<Credential> {
        let text = match &self.source {
            SecretSource::File(p) => std::fs::read_to_string(p)
                .map_err(|e| invalid(format!("secret {self}: can't read the file: {e}")))?,
            SecretSource::Env(v) => std::env::var(v)
                .map_err(|_| invalid(format!("secret {self}: the variable isn't set")))?,
        };
        parse_credential(&text, self.section.as_deref(), default_user)
            .map_err(|why| invalid(format!("secret {self}: {why}")))
    }
}

/// Parses a credential document; `Err` carries a reason without secrets.
fn parse_credential(
    text: &str,
    section: Option<&str>,
    default_user: &str,
) -> std::result::Result<Credential, String> {
    let table: toml::Table = text
        .parse()
        .map_err(|e: toml::de::Error| format!("not TOML: {}", e.message()))?;
    let value = match section {
        Some(s) => table
            .get(s)
            .cloned()
            .ok_or_else(|| format!("no [{s}] table"))?,
        None => toml::Value::Table(table),
    };
    let mut c: Credential = value
        .try_into()
        .map_err(|e: toml::de::Error| e.message().to_owned())?;
    if c.user.is_empty() {
        default_user.clone_into(&mut c.user);
    }
    Ok(c)
}

/// Resolves the reference `reference`.
///
/// # Errors
/// As [`SecretRef::resolve`], and for a reference that doesn't parse.
pub fn resolve(reference: &str, default_user: &str) -> Result<Credential> {
    reference.parse::<SecretRef>()?.resolve(default_user)
}

/// The two references for chapter 27's `credentials_ref`: the file's
/// `[sprig_ctl]` and `[sprig_repl]` tables.
///
/// # Errors
/// 39998 for a reference that doesn't parse or already names a section.
pub fn refs_for(credentials_ref: &str) -> Result<SecretRefs> {
    let r: SecretRef = credentials_ref.parse()?;
    if r.section.is_some() {
        return Err(invalid(format!(
            "credentials_ref {credentials_ref:?} names one section; it must name the whole file"
        )));
    }
    Ok(SecretRefs {
        ctl: format!("{r}#{CTL_USER}"),
        repl: format!("{r}#{REPL_USER}"),
    })
}

/// A [`NodeConnector`] over one secret reference, resolved on every call
/// so a rotated secret is picked up at the next probe (chapter 09: "secret
/// re-read").
#[derive(Debug, Clone)]
pub struct CredentialConnector {
    reference: String,
    default_user: &'static str,
}

impl CredentialConnector {
    /// A connector resolving `reference`, with `default_user` for a
    /// credential that names no user.
    #[must_use]
    pub fn new(reference: impl Into<String>, default_user: &'static str) -> Self {
        Self {
            reference: reference.into(),
            default_user,
        }
    }

    /// The `sprig_ctl` connector of a record's secrets.
    #[must_use]
    pub fn ctl(secrets: &SecretRefs) -> Self {
        Self::new(secrets.ctl.clone(), CTL_USER)
    }

    /// The `sprig_repl` connector of a record's secrets.
    #[must_use]
    pub fn repl(secrets: &SecretRefs) -> Self {
        Self::new(secrets.repl.clone(), REPL_USER)
    }
}

impl NodeConnector for CredentialConnector {
    fn endpoint(&self, node: &NodeSpec) -> Result<NodeEndpoint> {
        resolve(&self.reference, self.default_user)?.endpoint(node)
    }
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::NodeId;

    use super::*;

    fn node(addr: &str) -> NodeSpec {
        NodeSpec {
            node: NodeId::new("n1").unwrap(),
            addr: addr.to_owned(),
            zone: None,
        }
    }

    #[test]
    fn references_parse_and_print() {
        let r: SecretRef = "file:/etc/sprigdb/creds/a.toml#sprig_ctl".parse().unwrap();
        assert_eq!(
            r.source,
            SecretSource::File(PathBuf::from("/etc/sprigdb/creds/a.toml"))
        );
        assert_eq!(r.section.as_deref(), Some("sprig_ctl"));
        assert_eq!(r.to_string(), "file:/etc/sprigdb/creds/a.toml#sprig_ctl");
        let e: SecretRef = "env:SPRIG_CTL".parse().unwrap();
        assert_eq!(e.source, SecretSource::Env("SPRIG_CTL".into()));
        for bad in ["vault:kv/x", "file:", "env:", "file:/x#", "nothing"] {
            assert!(bad.parse::<SecretRef>().is_err(), "{bad}");
        }
        let pair = refs_for("file:/c.toml").unwrap();
        assert_eq!(pair.ctl, "file:/c.toml#sprig_ctl");
        assert_eq!(pair.repl, "file:/c.toml#sprig_repl");
        assert!(refs_for("file:/c.toml#x").is_err());
    }

    #[test]
    fn credentials_parse_with_defaults_and_hide_the_password() {
        let doc = r#"
            [sprig_ctl]
            user = "root"
            password = "s3cret"
            tls = "skip_verify"
            [sprig_repl]
            password = "r3pl"
            ca_file = "/ca.pem"
        "#;
        let c = parse_credential(doc, Some("sprig_ctl"), CTL_USER).unwrap();
        assert_eq!((c.user.as_str(), c.tls), ("root", TlsMode::SkipVerify));
        assert!(!format!("{c:?}").contains("s3cret"));
        let r = parse_credential(doc, Some("sprig_repl"), REPL_USER).unwrap();
        assert_eq!((r.user.as_str(), r.tls), ("sprig_repl", TlsMode::Verify));
        assert_eq!(r.ca_file, Some(PathBuf::from("/ca.pem")));
        assert!(parse_credential(doc, Some("other"), CTL_USER).is_err());
        assert!(parse_credential("password = 1", None, CTL_USER).is_err());
        let typo = parse_credential("password = \"x\"\ntsl = \"verify\"", None, CTL_USER);
        assert!(typo.is_err());
        let msg = parse_credential("password = \"leak\" x", None, CTL_USER).unwrap_err();
        assert!(!msg.contains("leak"), "{msg}");
    }

    #[test]
    fn endpoints_use_tcp_and_tls() {
        let c = Credential::new("sprig_ctl", "pw", TlsMode::SkipVerify);
        let ep = c.endpoint(&node("127.0.0.1:13301")).unwrap();
        assert_eq!(ep.addr(), "127.0.0.1:13301");
        assert!(!ep.opts.prefer_socket());
        let ssl = ep.opts.ssl_opts().unwrap();
        assert!(ssl.accept_invalid_certs());
        let v6 = c.endpoint(&node("[::1]:3306")).unwrap();
        assert_eq!(v6.opts.tcp_port(), 3306);
        for bad in ["db1", ":3306", "db1:x"] {
            assert!(c.endpoint(&node(bad)).is_err(), "{bad}");
        }
        let verify = Credential::new("u", "p", TlsMode::Verify)
            .endpoint(&node("db:3306"))
            .unwrap();
        assert!(!verify.opts.ssl_opts().unwrap().accept_invalid_certs());
    }

    #[test]
    fn connector_resolves_files_each_time() {
        let dir = std::env::temp_dir().join(format!("sprig-cred-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.toml");
        std::fs::write(
            &path,
            "[sprig_ctl]\npassword = \"a\"\ntls = \"skip_verify\"\n",
        )
        .unwrap();
        let refs = refs_for(&format!("file:{}", path.display())).unwrap();
        let conn = CredentialConnector::ctl(&refs);
        let ep = conn.endpoint(&node("db:3306")).unwrap();
        assert_eq!(ep.opts.user(), Some("sprig_ctl"));
        assert_eq!(ep.opts.pass(), Some("a"));
        std::fs::write(&path, "[sprig_ctl]\npassword = \"b\"\n").unwrap();
        assert_eq!(
            conn.endpoint(&node("db:3306")).unwrap().opts.pass(),
            Some("b")
        );
        // The repl table is missing.
        let e = CredentialConnector::repl(&refs)
            .endpoint(&node("db:3306"))
            .unwrap_err();
        assert!(e.message.contains("no [sprig_repl] table"), "{e}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
