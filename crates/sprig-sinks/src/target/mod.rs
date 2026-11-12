//! The targets (chapter 51, build steps 9–14) and what they share: an
//! opened connection ([`Conn`]: options, credentials as handles, the
//! destination rules), resolved options ([`Opts`]), TLS client settings.

pub mod db;
pub mod kafka;
pub mod s3;
pub mod script;
pub mod webhook;

use rustls::pki_types::pem::PemObject as _;
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
        for c in rustls::pki_types::CertificateDer::pem_slice_iter(pem.as_bytes()) {
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
            let chain: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(cert.as_bytes())
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| err(SprigCode::SinkCredentialsInvalid, format!("tls_cert: {e}")))?;
            let key =
                rustls::pki_types::PrivateKeyDer::from_pem_slice(key.as_bytes()).map_err(|e| {
                    match e {
                        rustls::pki_types::pem::Error::NoItemsFound => {
                            err(SprigCode::SinkCredentialsInvalid, "tls_key holds no key")
                        }
                        e => err(SprigCode::SinkCredentialsInvalid, format!("tls_key: {e}")),
                    }
                })?;
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

/// What `ctx`'s target would send for record `r` (R9, r9last: `TEST
/// SINK`'s payload preview), built by the target's own encoding and sent
/// nowhere: a webhook's request body; a Kafka record's topic, key, value
/// and headers; an S3 object's content before compression (Parquet as its
/// size and columns); a database sink's row operation. `Err` with the
/// reason when it can't be shown.
pub fn preview(
    ctx: &TargetCtx,
    r: &crate::model::SinkRecord,
) -> std::result::Result<String, String> {
    use crate::model::TargetSpec;
    let text = |b: &[u8]| match std::str::from_utf8(b) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            use std::fmt::Write as _;
            let mut s = String::from("0x");
            for x in b {
                let _ = write!(s, "{x:02x}");
            }
            s
        }
    };
    match &ctx.sink.target {
        TargetSpec::Webhook { .. } => {
            // The body doesn't depend on the signature: a connection
            // without its signing secret still shows it (as TEST
            // CONNECTION's OPTIONS request is sent unsigned).
            let mut opts = ctx.opts.clone();
            opts.0.insert("signature".into(), "none".into());
            let c = TargetCtx {
                task: ctx.task.clone(),
                sink: Arc::clone(&ctx.sink),
                conn: ctx.conn.clone(),
                extra: ctx.extra.clone(),
                opts,
                db_name: ctx.db_name.clone(),
            };
            webhook::WebhookTarget::new(&c)
                .map(|w| text(&w.preview_body(std::slice::from_ref(r))))
                .map_err(|e| e.to_string())
        }
        TargetSpec::Kafka { topic, .. } => {
            let k = kafka::KafkaTarget::new(ctx).map_err(|e| e.to_string())?;
            let (key, value, headers) = k.encode_record(r);
            let h: Map<String, Value> = headers
                .into_iter()
                .map(|x| {
                    (
                        x.key,
                        x.value.map_or(Value::Null, |v| Value::from(text(&v))),
                    )
                })
                .collect();
            Ok(serde_json::json!({
                "topic": topic,
                "key": key.map(|b| text(&b)),
                "value": value.map(|b| text(&b)),
                "headers": h,
            })
            .to_string())
        }
        TargetSpec::S3 { .. } => {
            use s3::format::{Compress, Format, encode, infer_columns, parse_columns};
            let format =
                Format::parse(&ctx.opts.text("format", "jsonl")).map_err(|e| e.to_string())?;
            let columns = match ctx.opts.get("columns").map(parse_columns).transpose() {
                Ok(Some(c)) => Some(c),
                Ok(None) => Some(infer_columns(&[r])),
                Err(e) => return Err(e.to_string()),
            };
            let e = encode(format, Compress::None, columns.as_deref(), &[r])
                .map_err(|e| e.to_string())?;
            if let Some((_, why)) = e.failed.first() {
                return Err(why.clone());
            }
            let ext = format.ext(Compress::None);
            if ext.starts_with("parquet") {
                let names: Vec<String> = columns
                    .unwrap_or_default()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                return Ok(format!(
                    "Parquet, {} bytes, columns {}",
                    e.bytes.len(),
                    names.join(", ")
                ));
            }
            Ok(text(&e.bytes))
        }
        TargetSpec::Database { .. } => {
            let db_opts = db::DbOpts::from_ctx(ctx).map_err(|e| e.to_string())?;
            let d = match ctx.conn.as_ref().map(|c| c.kind) {
                Some(ConnKind::Postgresql) => db::plan::Dialect::Postgres,
                _ => db::plan::Dialect::Mysql,
            };
            let ops = db::plan::plan(std::slice::from_ref(r), &db_opts, d)
                .map_err(|e| e.message().to_owned())?;
            Ok(Value::Array(
                ops.into_iter()
                    .map(|o| {
                        serde_json::json!({
                            "op": format!("{:?}", o.kind).to_ascii_lowercase(),
                            "table": o.table,
                            "columns": o.cols.into_iter().collect::<Map<String, Value>>(),
                        })
                    })
                    .collect(),
            )
            .to_string())
        }
        TargetSpec::Script { .. } => Err("a script sink's script decides what it sends".into()),
    }
}

#[cfg(test)]
mod preview_tests {
    use super::*;
    use crate::model::{Position, SinkRecord, TargetSpec, TaskEpoch};

    fn ctx(target: TargetSpec, opts: &[(&str, &str)]) -> TargetCtx {
        TargetCtx {
            task: TaskInfo {
                tenant: 1,
                sink: 2,
                task_no: 0,
                epoch: TaskEpoch(1),
            },
            sink: Arc::new(crate::probe::stand_in(1, "s", target)),
            conn: None,
            extra: Vec::new(),
            opts: Opts(
                opts.iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            ),
            db_name: "shop".into(),
        }
    }

    fn rec() -> SinkRecord {
        SinkRecord {
            key: Some(br#"{"id": 7}"#.to_vec()),
            value: Some(br#"{"id": 7, "total": 12.5}"#.to_vec()),
            headers: vec![("h".into(), b"v".to_vec())],
            op: None,
            before: None,
            after: None,
            ts_ms: 1_700_000_000_000,
            idem_key: "s/1/0/5".into(),
            pos: Position::Stream {
                stream: 1,
                partition: 0,
                offset: 5,
                epoch: 0,
            },
        }
    }

    /// R9 (r9last): `TEST SINK`'s payload preview is the target's own
    /// encoding: an S3 JSON line, a database row operation, a script's
    /// refusal; nothing is sent.
    #[test]
    fn previews_use_the_targets_encoding() {
        let s3 = preview(
            &ctx(
                TargetSpec::S3 {
                    prefix: "p/".into(),
                },
                &[("format", "jsonl")],
            ),
            &rec(),
        )
        .unwrap();
        assert!(s3.ends_with('\n') && s3.contains("\"total\":12.5"), "{s3}");
        let db = preview(
            &ctx(
                TargetSpec::Database {
                    table: "orders".into(),
                },
                &[],
            ),
            &rec(),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&db).unwrap();
        assert_eq!(v[0]["op"], "upsert", "{db}");
        assert_eq!(v[0]["table"], "orders", "{db}");
        assert!(v[0]["columns"].get("total").is_some(), "{db}");
        assert!(
            preview(
                &ctx(
                    TargetSpec::Script {
                        language: crate::model::ScriptLanguage::Javascript,
                        source: None,
                        function: Some("f".into()),
                    },
                    &[]
                ),
                &rec()
            )
            .is_err()
        );
    }
}
