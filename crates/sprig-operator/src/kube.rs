//! A small Kubernetes REST client (chapter 27 step 10).
//!
//! The operator needs a handful of calls on namespaced objects: get, list
//! (with a label selector), create, JSON merge patch, replace (for Lease
//! renewals and status updates, with `resourceVersion` for optimistic
//! concurrency) and delete. [`KubeApi`] is that surface; [`HttpKube`]
//! implements it with HTTP/1.1 over hyper and rustls, one kept-alive
//! connection, and the in-cluster service-account configuration
//! ([`HttpKube::in_cluster`]). Objects are `serde_json::Value`s; the typed
//! views live in [`crate::crd`].
//!
//! No kube-rs: it isn't vendored here, and the operator's needs are small
//! enough that the client is shorter than the glue around a generic one.

use std::fmt;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, StatusCode};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::Value;
use sprig_common::{SprigCode, SprigError};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

/// A kind of namespaced object and where the API serves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiResource {
    /// The group; empty for the core group.
    pub group: &'static str,
    /// The version.
    pub version: &'static str,
    /// The kind.
    pub kind: &'static str,
    /// The plural resource name in paths.
    pub plural: &'static str,
}

impl ApiResource {
    /// `apiVersion`.
    #[must_use]
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.to_owned()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }

    /// The collection path in `ns`.
    #[must_use]
    pub fn collection(&self, ns: &str) -> String {
        if self.group.is_empty() {
            format!("/api/{}/namespaces/{ns}/{}", self.version, self.plural)
        } else {
            format!(
                "/apis/{}/{}/namespaces/{ns}/{}",
                self.group, self.version, self.plural
            )
        }
    }

    /// The object path of `name` in `ns`.
    #[must_use]
    pub fn object(&self, ns: &str, name: &str) -> String {
        format!("{}/{name}", self.collection(ns))
    }
}

/// `v1` `Service`.
pub const SERVICE: ApiResource = ApiResource {
    group: "",
    version: "v1",
    kind: "Service",
    plural: "services",
};
/// `v1` `Secret`.
pub const SECRET: ApiResource = ApiResource {
    group: "",
    version: "v1",
    kind: "Secret",
    plural: "secrets",
};
/// `coordination.k8s.io/v1` `Lease`.
pub const LEASE: ApiResource = ApiResource {
    group: "coordination.k8s.io",
    version: "v1",
    kind: "Lease",
    plural: "leases",
};
/// `v1` `Event`.
pub const EVENT: ApiResource = ApiResource {
    group: "",
    version: "v1",
    kind: "Event",
    plural: "events",
};

/// What a call can fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KubeError {
    /// 404.
    NotFound,
    /// 409 on create.
    AlreadyExists,
    /// 409 on update: the `resourceVersion` moved.
    Conflict(String),
    /// Any other status, with the server's message.
    Status {
        /// The HTTP status.
        code: u16,
        /// `Status.message`, or the body.
        message: String,
    },
    /// The request didn't complete.
    Transport(String),
}

impl fmt::Display for KubeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::AlreadyExists => f.write_str("already exists"),
            Self::Conflict(m) => write!(f, "conflict: {m}"),
            Self::Status { code, message } => write!(f, "HTTP {code}: {message}"),
            Self::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}

impl std::error::Error for KubeError {}

impl From<KubeError> for SprigError {
    fn from(e: KubeError) -> Self {
        SprigError::new(SprigCode::Internal, format!("kubernetes API: {e}"))
    }
}

/// A call's result.
pub type KubeResult<T> = std::result::Result<T, KubeError>;

/// The calls the operator makes (see the module docs).
#[async_trait]
pub trait KubeApi: Send + Sync + 'static {
    /// The object, or `None`.
    async fn get(&self, r: &ApiResource, ns: &str, name: &str) -> KubeResult<Option<Value>>;
    /// The objects matching `selector` (`k=v,k2=v2`), or all.
    async fn list(
        &self,
        r: &ApiResource,
        ns: &str,
        selector: Option<&str>,
    ) -> KubeResult<Vec<Value>>;
    /// Creates `obj`; [`KubeError::AlreadyExists`] if the name is taken.
    async fn create(&self, r: &ApiResource, ns: &str, obj: &Value) -> KubeResult<Value>;
    /// Applies an RFC 7386 JSON merge patch.
    async fn merge_patch(
        &self,
        r: &ApiResource,
        ns: &str,
        name: &str,
        patch: &Value,
    ) -> KubeResult<Value>;
    /// Replaces the object (`PUT`); with `metadata.resourceVersion` set, a
    /// moved version is [`KubeError::Conflict`].
    async fn replace(
        &self,
        r: &ApiResource,
        ns: &str,
        name: &str,
        obj: &Value,
    ) -> KubeResult<Value>;
    /// Replaces the status subresource, the same way.
    async fn replace_status(
        &self,
        r: &ApiResource,
        ns: &str,
        name: &str,
        obj: &Value,
    ) -> KubeResult<Value>;
    /// Deletes; `false` if it was already gone.
    async fn delete(&self, r: &ApiResource, ns: &str, name: &str) -> KubeResult<bool>;
}

/// Where the API server is and how to authenticate.
#[derive(Clone)]
pub struct KubeConfig {
    /// `https://host:port` or `http://host:port` (tests).
    pub base: String,
    /// A file holding the bearer token, re-read on every request (bound
    /// service-account tokens rotate).
    pub token_file: Option<PathBuf>,
    /// The CA bundle that signs the API server's certificate.
    pub ca_pem: Option<Vec<u8>>,
    /// The namespace the operator works in.
    pub namespace: String,
}

impl fmt::Debug for KubeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KubeConfig")
            .field("base", &self.base)
            .field("token_file", &self.token_file)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

/// The service-account directory every pod gets.
pub const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

impl KubeConfig {
    /// The in-cluster configuration: `KUBERNETES_SERVICE_HOST` and `_PORT`,
    /// and the service account's token, CA and namespace under `sa_dir`.
    /// `None` outside a cluster.
    #[must_use]
    pub fn in_cluster(env: &dyn Fn(&str) -> Option<String>, sa_dir: &Path) -> Option<Self> {
        let host = env("KUBERNETES_SERVICE_HOST")?;
        let port = env("KUBERNETES_SERVICE_PORT").unwrap_or_else(|| "443".into());
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        };
        let token_file = sa_dir.join("token");
        if !token_file.exists() {
            return None;
        }
        let ca_pem = std::fs::read(sa_dir.join("ca.crt")).ok()?;
        let namespace = std::fs::read_to_string(sa_dir.join("namespace"))
            .map(|s| s.trim().to_owned())
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| env("SPRIG_OPERATOR_NAMESPACE"))
            .unwrap_or_else(|| "sprigdb".into());
        Some(Self {
            base: format!("https://{host}:{port}"),
            token_file: Some(token_file),
            ca_pem: Some(ca_pem),
            namespace,
        })
    }
}

/// The time one call may take.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(15);

/// [`KubeApi`] over HTTP (see the module docs).
pub struct HttpKube {
    cfg: KubeConfig,
    host: String,
    port: u16,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    conn: Mutex<Option<SendRequest<Full<Bytes>>>>,
}

impl fmt::Debug for HttpKube {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpKube")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

fn transport(e: impl fmt::Display) -> KubeError {
    KubeError::Transport(e.to_string())
}

impl HttpKube {
    /// A client for `cfg`.
    ///
    /// # Errors
    /// [`KubeError::Transport`] for a bad base URL or CA bundle.
    pub fn new(cfg: KubeConfig) -> KubeResult<Self> {
        let (https, rest) = if let Some(r) = cfg.base.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = cfg.base.strip_prefix("http://") {
            (false, r)
        } else {
            return Err(transport(format!("bad API server URL {}", cfg.base)));
        };
        let rest = rest.trim_end_matches('/');
        let (host, port) = match rest.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (
                h.trim_start_matches('[').trim_end_matches(']').to_owned(),
                p.parse::<u16>().map_err(transport)?,
            ),
            _ => (rest.to_owned(), if https { 443 } else { 80 }),
        };
        let tls = if https {
            let mut roots = rustls::RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(cfg.ca_pem.as_deref().unwrap_or_default()) {
                roots.add(c.map_err(transport)?).map_err(transport)?;
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let mut cc = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(transport)?
                .with_root_certificates(roots)
                .with_no_client_auth();
            cc.alpn_protocols = vec![b"http/1.1".to_vec()];
            let name = ServerName::try_from(host.clone()).map_err(transport)?;
            Some((TlsConnector::from(Arc::new(cc)), name))
        } else {
            None
        };
        Ok(Self {
            cfg,
            host,
            port,
            tls,
            conn: Mutex::new(None),
        })
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &KubeConfig {
        &self.cfg
    }

    async fn connect(&self) -> KubeResult<SendRequest<Full<Bytes>>> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(transport)?;
        let _ = tcp.set_nodelay(true);
        if let Some((tls, name)) = &self.tls {
            let s = tls.connect(name.clone(), tcp).await.map_err(transport)?;
            let (send, conn) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(s))
                    .await
                    .map_err(transport)?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(send)
        } else {
            let (send, conn) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
                    .await
                    .map_err(transport)?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(send)
        }
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<(&str, Vec<u8>)>,
    ) -> KubeResult<(StatusCode, Bytes)> {
        tokio::time::timeout(CALL_TIMEOUT, self.call_once(method, path, body))
            .await
            .map_err(|_| transport(format!("timed out after {CALL_TIMEOUT:?}")))?
    }

    async fn call_once(
        &self,
        method: Method,
        path: &str,
        body: Option<(&str, Vec<u8>)>,
    ) -> KubeResult<(StatusCode, Bytes)> {
        let token = match &self.cfg.token_file {
            Some(p) => Some(
                tokio::fs::read_to_string(p)
                    .await
                    .map_err(|e| transport(format!("token {}: {e}", p.display())))?
                    .trim()
                    .to_owned(),
            ),
            None => None,
        };
        let mut b = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::HOST, format!("{}:{}", self.host, self.port))
            .header(hyper::header::ACCEPT, "application/json")
            .header(hyper::header::USER_AGENT, "sprigdb-operator");
        if let Some(t) = token {
            b = b.header(hyper::header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let req = match body {
            Some((ct, bytes)) => b
                .header(hyper::header::CONTENT_TYPE, ct)
                .body(Full::new(Bytes::from(bytes))),
            None => b.body(Full::new(Bytes::new())),
        }
        .map_err(transport)?;
        let mut guard = self.conn.lock().await;
        // One kept-alive connection; a closed one is replaced, and a request
        // that fails on a reused connection is retried once on a new one.
        let mut fresh = false;
        if guard.as_ref().is_none_or(SendRequest::is_closed) {
            *guard = Some(self.connect().await?);
            fresh = true;
        }
        let send = guard.as_mut().ok_or_else(|| transport("no connection"))?;
        let resp = match send.send_request(req.clone()).await {
            Ok(r) => r,
            Err(e) if !fresh => {
                tracing::debug!(error = %e, "kubernetes API connection dropped; reconnecting");
                *guard = Some(self.connect().await?);
                let send = guard.as_mut().ok_or_else(|| transport("no connection"))?;
                send.send_request(req).await.map_err(transport)?
            }
            Err(e) => return Err(transport(e)),
        };
        drop(guard);
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(transport)?
            .to_bytes();
        Ok((status, body))
    }

    fn decode(status: StatusCode, body: &Bytes) -> KubeResult<Value> {
        let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(v);
        }
        let message = v
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| String::from_utf8_lossy(body).into_owned(), str::to_owned);
        let reason = v.get("reason").and_then(Value::as_str).unwrap_or_default();
        Err(match status {
            StatusCode::NOT_FOUND => KubeError::NotFound,
            StatusCode::CONFLICT if reason == "AlreadyExists" => KubeError::AlreadyExists,
            StatusCode::CONFLICT => KubeError::Conflict(message),
            s => KubeError::Status {
                code: s.as_u16(),
                message,
            },
        })
    }

    fn json(v: &Value) -> KubeResult<Vec<u8>> {
        serde_json::to_vec(v).map_err(transport)
    }
}

/// `k=v` selectors are passed as they are, percent-encoding what a query
/// can't hold.
fn encode_query(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[async_trait]
impl KubeApi for HttpKube {
    async fn get(&self, r: &ApiResource, ns: &str, name: &str) -> KubeResult<Option<Value>> {
        let (s, b) = self.call(Method::GET, &r.object(ns, name), None).await?;
        match Self::decode(s, &b) {
            Ok(v) => Ok(Some(v)),
            Err(KubeError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn list(
        &self,
        r: &ApiResource,
        ns: &str,
        selector: Option<&str>,
    ) -> KubeResult<Vec<Value>> {
        let mut path = r.collection(ns);
        if let Some(sel) = selector {
            path.push_str("?labelSelector=");
            path.push_str(&encode_query(sel));
        }
        let (s, b) = self.call(Method::GET, &path, None).await?;
        let v = Self::decode(s, &b)?;
        Ok(v.get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    async fn create(&self, r: &ApiResource, ns: &str, obj: &Value) -> KubeResult<Value> {
        let (s, b) = self
            .call(
                Method::POST,
                &r.collection(ns),
                Some(("application/json", Self::json(obj)?)),
            )
            .await?;
        Self::decode(s, &b)
    }

    async fn merge_patch(
        &self,
        r: &ApiResource,
        ns: &str,
        name: &str,
        patch: &Value,
    ) -> KubeResult<Value> {
        let (s, b) = self
            .call(
                Method::PATCH,
                &r.object(ns, name),
                Some(("application/merge-patch+json", Self::json(patch)?)),
            )
            .await?;
        Self::decode(s, &b)
    }

    async fn replace(
        &self,
        r: &ApiResource,
        ns: &str,
        name: &str,
        obj: &Value,
    ) -> KubeResult<Value> {
        let (s, b) = self
            .call(
                Method::PUT,
                &r.object(ns, name),
                Some(("application/json", Self::json(obj)?)),
            )
            .await?;
        Self::decode(s, &b)
    }

    async fn replace_status(
        &self,
        r: &ApiResource,
        ns: &str,
        name: &str,
        obj: &Value,
    ) -> KubeResult<Value> {
        let (s, b) = self
            .call(
                Method::PUT,
                &format!("{}/status", r.object(ns, name)),
                Some(("application/json", Self::json(obj)?)),
            )
            .await?;
        Self::decode(s, &b)
    }

    async fn delete(&self, r: &ApiResource, ns: &str, name: &str) -> KubeResult<bool> {
        let (s, b) = self.call(Method::DELETE, &r.object(ns, name), None).await?;
        match Self::decode(s, &b) {
            Ok(_) => Ok(true),
            Err(KubeError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

// ---- JSON helpers the reconciler uses ----

/// Whether every field of `want` is in `have` with the same value (arrays
/// compared whole): a merge patch of `want` onto `have` would change
/// nothing.
#[must_use]
pub fn contains(have: &Value, want: &Value) -> bool {
    match (have, want) {
        (Value::Object(h), Value::Object(w)) => w.iter().all(|(k, wv)| {
            if wv.is_null() {
                h.get(k).is_none_or(Value::is_null)
            } else {
                h.get(k).is_some_and(|hv| contains(hv, wv))
            }
        }),
        (h, w) => h == w,
    }
}

/// RFC 7386: `patch` merged into `target`.
pub fn merge(target: &mut Value, patch: &Value) {
    if let Value::Object(p) = patch {
        if !target.is_object() {
            *target = Value::Object(serde_json::Map::new());
        }
        if let Value::Object(t) = target {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    merge(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
    } else {
        *target = patch.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn paths() {
        assert_eq!(
            SERVICE.object("sprigdb", "a-0"),
            "/api/v1/namespaces/sprigdb/services/a-0"
        );
        assert_eq!(
            crate::crd::SPRIG_CLUSTER.collection("x"),
            "/apis/sprigdb.io/v1alpha1/namespaces/x/sprigclusters"
        );
        assert_eq!(LEASE.api_version(), "coordination.k8s.io/v1");
        assert_eq!(encode_query("a=b,c in (d)"), "a%3Db%2Cc%20in%20%28d%29");
    }

    #[test]
    fn merge_and_contains() {
        let mut t = json!({"a": 1, "b": {"c": 2, "d": [1, 2]}});
        let p = json!({"b": {"c": null, "e": 3}, "f": "x"});
        assert!(!contains(&t, &p));
        merge(&mut t, &p);
        assert_eq!(t, json!({"a": 1, "b": {"d": [1, 2], "e": 3}, "f": "x"}));
        assert!(contains(&t, &p));
        assert!(contains(&t, &json!({"b": {"d": [1, 2]}})));
        assert!(!contains(&t, &json!({"b": {"d": [1]}})));
    }

    #[test]
    fn in_cluster_needs_the_env_and_the_token() {
        let dir = std::env::temp_dir().join(format!("sprig-op-sa-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let env = |k: &str| match k {
            "KUBERNETES_SERVICE_HOST" => Some("10.96.0.1".to_owned()),
            "KUBERNETES_SERVICE_PORT" => Some("443".to_owned()),
            _ => None,
        };
        assert!(KubeConfig::in_cluster(&env, &dir).is_none(), "no token");
        std::fs::write(dir.join("token"), "t").expect("token");
        std::fs::write(dir.join("ca.crt"), "").expect("ca");
        std::fs::write(dir.join("namespace"), "sprig-fra1\n").expect("ns");
        let c = KubeConfig::in_cluster(&env, &dir).expect("in cluster");
        assert_eq!(c.base, "https://10.96.0.1:443");
        assert_eq!(c.namespace, "sprig-fra1");
        assert!(KubeConfig::in_cluster(&|_| None, &dir).is_none(), "no env");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
