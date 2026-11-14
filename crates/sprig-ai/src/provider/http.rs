//! The adapters' HTTP client: one POST per call over a new HTTP/1.1
//! connection, TLS for `https` (the Mozilla roots, or a CA file), the
//! response body read chunk by chunk with an idle timeout.
//!
//! A connection per call costs one handshake per model round, small next
//! to the model's own time. Redirects are never followed: a provider's
//! base URL is the operator's to set.

use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, Uri};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::net::TcpStream;

use super::{AttemptError, HttpLimits};
use crate::error::provider_error;

/// Where a provider is.
#[derive(Debug, Clone)]
pub(crate) struct Endpoint {
    /// The provider's name, for errors.
    pub(crate) provider: String,
    tls: Option<Arc<rustls::ClientConfig>>,
    host: String,
    port: u16,
    /// The base path, without a trailing `/`.
    base_path: String,
    authority: String,
}

fn config_err(msg: String) -> SprigError {
    SprigError::new(SprigCode::AiNotConfigured, msg)
}

static WEB_ROOTS: LazyLock<std::result::Result<Arc<rustls::ClientConfig>, String>> =
    LazyLock::new(|| {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        tls_with(roots)
    });

fn tls_with(
    roots: rustls::RootCertStore,
) -> std::result::Result<Arc<rustls::ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

impl Endpoint {
    /// Parses `base_url` (`https://api.anthropic.com`,
    /// `http://127.0.0.1:11434/v1`); `ca_file` replaces the Mozilla roots.
    ///
    /// # Errors
    /// 39990 for a URL that isn't `http` or `https` with a host, carries a
    /// query, or a CA file that can't be read.
    pub(crate) fn parse(provider: &str, base_url: &str, ca_file: Option<&Path>) -> Result<Self> {
        let uri: Uri = base_url.trim().parse().map_err(|e| {
            config_err(format!(
                "AI provider {provider}: base URL {base_url:?}: {e}"
            ))
        })?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => {
                return Err(config_err(format!(
                    "AI provider {provider}: base URL {base_url:?} must start with http:// or https://"
                )));
            }
        };
        let host = uri
            .host()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| {
                config_err(format!(
                    "AI provider {provider}: base URL {base_url:?} has no host"
                ))
            })?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        if uri.query().is_some() {
            return Err(config_err(format!(
                "AI provider {provider}: put query parameters in the provider's query, not its base URL"
            )));
        }
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let tls = if https {
            Some(match ca_file {
                Some(ca) => {
                    let mut roots = rustls::RootCertStore::empty();
                    for c in sprig_common::https::certs(ca, "the AI provider's CA file")? {
                        roots.add(c).map_err(|e| {
                            config_err(format!("AI provider {provider}: CA file: {e}"))
                        })?;
                    }
                    tls_with(roots)
                        .map_err(|e| config_err(format!("AI provider {provider}: TLS: {e}")))?
                }
                None => WEB_ROOTS
                    .clone()
                    .map_err(|e| config_err(format!("AI provider {provider}: TLS: {e}")))?,
            })
        } else {
            None
        };
        let authority = uri
            .authority()
            .map_or_else(|| host.clone(), |a| a.as_str().to_owned());
        Ok(Self {
            provider: provider.to_owned(),
            tls,
            host,
            port,
            base_path: uri.path().trim_end_matches('/').to_owned(),
            authority,
        })
    }

    /// POSTs `body` to the base path + `path` (+ `?query`) and returns the
    /// response head once it arrives.
    pub(crate) async fn post(
        &self,
        path: &str,
        query: Option<&str>,
        headers: &[(&str, &str)],
        body: &str,
        limits: &HttpLimits,
    ) -> std::result::Result<Response<Incoming>, AttemptError> {
        let mut target = format!("{}{path}", self.base_path);
        if let Some(q) = query.filter(|q| !q.is_empty()) {
            target.push('?');
            target.push_str(q);
        }
        let mut req = Request::post(target)
            .header(hyper::header::HOST, &self.authority)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "text/event-stream")
            .header(
                hyper::header::USER_AGENT,
                concat!("sprigdb/", env!("CARGO_PKG_VERSION")),
            );
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = req
            .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
            .map_err(|e| self.fail(format!("bad request header: {e}"), false))?;
        let mut sender = tokio::time::timeout(limits.connect_timeout, self.connect())
            .await
            .map_err(|_| self.fail("connect timed out".to_owned(), true))??;
        tokio::time::timeout(limits.idle_timeout, sender.send_request(req))
            .await
            .map_err(|_| self.fail("no response in time".to_owned(), true))?
            .map_err(|e| self.fail(format!("request failed: {e}"), true))
    }

    async fn connect(
        &self,
    ) -> std::result::Result<hyper::client::conn::http1::SendRequest<Full<Bytes>>, AttemptError>
    {
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| self.fail(format!("{}:{}: {e}", self.host, self.port), true))?;
        let _ = tcp.set_nodelay(true);
        match &self.tls {
            Some(cfg) => {
                let name = ServerName::try_from(self.host.clone())
                    .map_err(|e| self.fail(format!("{}: {e}", self.host), false))?;
                let tls = tokio_rustls::TlsConnector::from(Arc::clone(cfg))
                    .connect(name, tcp)
                    .await
                    .map_err(|e| {
                        self.fail(format!("TLS with {}:{}: {e}", self.host, self.port), true)
                    })?;
                self.handshake(TokioIo::new(tls)).await
            }
            None => self.handshake(TokioIo::new(tcp)).await,
        }
    }

    async fn handshake<T>(
        &self,
        io: T,
    ) -> std::result::Result<hyper::client::conn::http1::SendRequest<Full<Bytes>>, AttemptError>
    where
        T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let (sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|e| self.fail(format!("HTTP handshake: {e}"), true))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(sender)
    }

    pub(crate) fn fail(&self, msg: String, retryable: bool) -> AttemptError {
        AttemptError {
            error: provider_error(&self.provider, msg, retryable),
            retryable,
            retry_after: None,
        }
    }
}

/// Whether an HTTP status is worth another attempt: timeouts, rate limits,
/// server errors and overload (529).
pub(crate) fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 409 | 425 | 429 | 500..=599)
}

/// The `retry-after` header in seconds, when it is a number.
pub(crate) fn retry_after(resp: &Response<Incoming>) -> Option<Duration> {
    resp.headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// The next chunk of a body; `None` at its end.
pub(crate) async fn next_chunk(
    ep: &Endpoint,
    body: &mut Incoming,
    idle: Duration,
) -> std::result::Result<Option<Bytes>, AttemptError> {
    loop {
        let frame = tokio::time::timeout(idle, body.frame())
            .await
            .map_err(|_| ep.fail(format!("no data for {}s", idle.as_secs()), true))?;
        match frame {
            None => return Ok(None),
            Some(Err(e)) => return Err(ep.fail(format!("the response broke off: {e}"), true)),
            Some(Ok(f)) => {
                if let Ok(data) = f.into_data()
                    && !data.is_empty()
                {
                    return Ok(Some(data));
                }
            }
        }
    }
}

/// Reads up to 64 KiB of an error response's body.
pub(crate) async fn error_body(ep: &Endpoint, body: &mut Incoming, idle: Duration) -> String {
    let mut out = Vec::new();
    while out.len() < 64 * 1024 {
        match next_chunk(ep, body, idle).await {
            Ok(Some(c)) => out.extend_from_slice(&c),
            _ => break,
        }
    }
    out.truncate(64 * 1024);
    String::from_utf8_lossy(&out).into_owned()
}

/// A provider's error message from a JSON error body
/// (`{"error":{"type":..,"message":..}}`), or the body cut to 300
/// characters.
pub(crate) fn error_message(status: u16, body: &str) -> String {
    let parsed: Option<serde_json::Value> = serde_json::from_str(body).ok();
    let detail = parsed.as_ref().and_then(|v| v.get("error")).map_or_else(
        || body.chars().take(300).collect(),
        |e| {
            let kind = e.get("type").and_then(|t| t.as_str()).unwrap_or_default();
            let msg = e
                .get("message")
                .and_then(|t| t.as_str())
                .or_else(|| e.as_str())
                .unwrap_or_default();
            if kind.is_empty() {
                msg.to_owned()
            } else {
                format!("{kind}: {msg}")
            }
        },
    );
    format!("HTTP {status}: {detail}")
}
