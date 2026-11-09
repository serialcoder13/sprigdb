//! The targets (chapter 51, build steps 9–14) and what they share: an
//! opened connection ([`Conn`]: options, credentials as handles, the
//! destination rules), resolved options ([`Opts`]), TLS client settings.

pub mod db;
pub mod kafka;
pub mod s3;
pub mod webhook;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::{Map, Value};
use sprig_common::{Result, SprigCode};
use zeroize::Zeroizing;

use crate::model::{ConnKind, SinkDef, bad_option, err};
use crate::policy::Destinations;
use crate::runtime::TaskInfo;

/// A connection's opened credentials: values only inside the worker, in
/// zeroized buffers; never printed.
pub trait Secrets: Send + Sync {
    /// The current value of credential `name`.
    fn get(&self, name: &str) -> Option<Zeroizing<String>>;
    /// The previous value, while `sink.credential_previous_hours` keeps it
    /// usable (tried once after an authentication failure).
    fn previous(&self, _name: &str) -> Option<Zeroizing<String>> {
        None
    }
}

/// Credentials in memory (tests, and values opened from `sink_secrets`).
#[derive(Default, Clone)]
pub struct MemSecrets {
    current: HashMap<String, Zeroizing<String>>,
    previous: HashMap<String, Zeroizing<String>>,
}

impl std::fmt::Debug for MemSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<&String> = self.current.keys().collect();
        names.sort();
        f.debug_struct("MemSecrets")
            .field("names", &names)
            .finish_non_exhaustive()
    }
}

impl MemSecrets {
    /// From `(name, value)` pairs.
    #[must_use]
    pub fn of(pairs: &[(&str, &str)]) -> Self {
        let mut s = Self::default();
        for (k, v) in pairs {
            s.current
                .insert((*k).to_owned(), Zeroizing::new((*v).to_owned()));
        }
        s
    }

    /// Adds a current value.
    pub fn set(&mut self, name: &str, value: Zeroizing<String>) {
        self.current.insert(name.to_owned(), value);
    }

    /// Adds a previous value.
    pub fn set_previous(&mut self, name: &str, value: Zeroizing<String>) {
        self.previous.insert(name.to_owned(), value);
    }
}

impl Secrets for MemSecrets {
    fn get(&self, name: &str) -> Option<Zeroizing<String>> {
        self.current.get(name).cloned()
    }
    fn previous(&self, name: &str) -> Option<Zeroizing<String>> {
        self.previous.get(name).cloned()
    }
}

/// One opened connection.
#[derive(Clone)]
pub struct Conn {
    /// Its name.
    pub name: String,
    /// Its type.
    pub kind: ConnKind,
    /// `WITH (...)`.
    pub options: Map<String, Value>,
    /// Its credentials.
    pub secrets: Arc<dyn Secrets>,
    /// The rules every address it opens is checked under.
    pub dest: Arc<Destinations>,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl Conn {
    /// An option as text.
    #[must_use]
    pub fn opt(&self, key: &str) -> Option<String> {
        match self.options.get(key)? {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            v => Some(v.to_string()),
        }
    }

    /// An option as a number.
    ///
    /// # Errors
    /// 39218.
    pub fn opt_u64(&self, key: &str) -> Result<Option<u64>> {
        crate::model::opt_u64(&self.options, key)
    }

    /// An option as a flag (`on`, `true`, `1`, `yes`).
    #[must_use]
    pub fn opt_bool(&self, key: &str, default: bool) -> bool {
        self.opt(key).map_or(default, |v| truthy(&v))
    }

    /// A required option.
    ///
    /// # Errors
    /// 39218 when missing.
    pub fn need(&self, key: &str) -> Result<String> {
        self.opt(key)
            .ok_or_else(|| bad_option(format!("connection {} needs the option {key}", self.name)))
    }

    /// A required credential.
    ///
    /// # Errors
    /// 39216 when missing.
    pub fn secret(&self, name: &str) -> Result<Zeroizing<String>> {
        self.secrets.get(name).ok_or_else(|| {
            err(
                SprigCode::SinkCredentialsInvalid,
                format!("connection {} has no credential {name}", self.name),
            )
        })
    }
}

/// `on`, `true`, `1`, `yes` (any case).
#[must_use]
pub fn truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "on" | "true" | "1" | "yes"
    )
}

/// A sink's resolved options: its `WITH (...)` values over the `sink.*`
/// settings, keys without the `sink.` prefix (`batch_max_records`,
/// `format`, `s3.file_max_bytes`, ...).
#[derive(Debug, Clone, Default)]
pub struct Opts(pub BTreeMap<String, String>);

impl Opts {
    /// Text.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Text with a default.
    #[must_use]
    pub fn text(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_owned()
    }

    /// A number with a default.
    ///
    /// # Errors
    /// 39218 for text that isn't a number.
    pub fn u64(&self, key: &str, default: u64) -> Result<u64> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => v
                .trim()
                .parse()
                .map_err(|_| bad_option(format!("{key} must be a whole number, not '{v}'"))),
        }
    }

    /// A flag with a default.
    #[must_use]
    pub fn flag(&self, key: &str, default: bool) -> bool {
        self.get(key).map_or(default, truthy)
    }
}

/// What a target is built from.
#[derive(Clone)]
pub struct TargetCtx {
    /// The task.
    pub task: TaskInfo,
    /// The sink.
    pub sink: Arc<SinkDef>,
    /// The target's connection (none for scripts).
    pub conn: Option<Conn>,
    /// A script's `USING CONNECTIONS`.
    pub extra: Vec<Conn>,
    /// Resolved options.
    pub opts: Opts,
    /// The logical database's name (`{db}` and `CloudEvents` sources).
    pub db_name: String,
}

impl std::fmt::Debug for TargetCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetCtx")
            .field("task", &self.task)
            .field("sink", &self.sink.name)
            .field("conn", &self.conn)
            .finish_non_exhaustive()
    }
}

impl TargetCtx {
    /// The target's connection.
    ///
    /// # Errors
    /// 39210 when the sink has none.
    pub fn conn(&self) -> Result<&Conn> {
        self.conn.as_ref().ok_or_else(|| {
            err(
                SprigCode::SinkNotFound,
                format!("sink {} has no target connection", self.sink.name),
            )
        })
    }

    /// `/sprig/<tenant>/<db>/<source>`: the `CloudEvents` `source`.
    #[must_use]
    pub fn ce_source(&self) -> String {
        format!(
            "/sprig/{}/{}/{}",
            self.task.tenant,
            self.db_name,
            self.sink.source.name()
        )
    }
}

/// A rustls client configuration: webpki roots, or the connection's own CA
/// (`ca_pem` option, PEM text), and a client certificate for mTLS
/// (credentials `tls_cert` and `tls_key`, PEM).
///
/// # Errors
/// 39218 for a CA that doesn't parse; 39216 for a bad client certificate.
pub fn tls_config(conn: &Conn, alpn: &[&[u8]]) -> Result<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(pem) = conn.opt("ca_pem") {
        let mut rd = std::io::BufReader::new(pem.as_bytes());
        for c in rustls_pemfile::certs(&mut rd) {
            let c = c.map_err(|e| bad_option(format!("ca_pem: {e}")))?;
            roots
                .add(c)
                .map_err(|e| bad_option(format!("ca_pem: {e}")))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let b = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| bad_option(format!("tls: {e}")))?
        .with_root_certificates(roots);
    let mut cfg = match (conn.secrets.get("tls_cert"), conn.secrets.get("tls_key")) {
        (Some(cert), Some(key)) => {
            let mut rd = std::io::BufReader::new(cert.as_bytes());
            let chain: Vec<_> = rustls_pemfile::certs(&mut rd)
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| err(SprigCode::SinkCredentialsInvalid, format!("tls_cert: {e}")))?;
            let mut rd = std::io::BufReader::new(key.as_bytes());
            let key = rustls_pemfile::private_key(&mut rd)
                .map_err(|e| err(SprigCode::SinkCredentialsInvalid, format!("tls_key: {e}")))?
                .ok_or_else(|| err(SprigCode::SinkCredentialsInvalid, "tls_key holds no key"))?;
            b.with_client_auth_cert(chain, key)
                .map_err(|e| err(SprigCode::SinkCredentialsInvalid, format!("mTLS: {e}")))?
        }
        (None, None) => b.with_no_client_auth(),
        _ => {
            return Err(err(
                SprigCode::SinkCredentialsInvalid,
                "mTLS needs both tls_cert and tls_key",
            ));
        }
    };
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(cfg))
}
