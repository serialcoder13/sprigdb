//! A small HTTP/1.1 client over TCP or rustls (chapter 53, "Crate
//! layout").
//!
//! One request per connection at a time; up to two idle connections are
//! kept for reuse, so dashboards don't open a connection per poll. A
//! response whose body isn't read to the end closes its connection when
//! dropped: that is how a running `Sql.Query` is cancelled (the gateway
//! kills the backend query when the stream goes away).

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::AbortHandle;

use crate::error::{Result, ShellCode, ShellError};

/// How long a connection may take to open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Idle connections kept.
const MAX_IDLE: usize = 2;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A parsed base URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// `https`.
    pub tls: bool,
    /// The host name or address.
    pub host: String,
    /// The port.
    pub port: u16,
    /// A path prefix without the trailing `/` (usually empty).
    pub prefix: String,
}

impl Endpoint {
    /// Parses `http://host[:port][/prefix]` or `https://...`.
    ///
    /// # Errors
    /// 39980 for anything else.
    pub fn parse(url: &str) -> Result<Self> {
        let bad = || {
            ShellError::shell(
                ShellCode::ProfileInvalid,
                format!("url {url:?}: expected http://host:port or https://host:port"),
            )
        };
        let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = url.strip_prefix("http://") {
            (false, r)
        } else {
            return Err(bad());
        };
        let (authority, prefix) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
            None => (rest, ""),
        };
        let (host, port) = if let Some(h) = authority.strip_prefix('[') {
            let (h, p) = h.split_once(']').ok_or_else(bad)?;
            (h.to_owned(), p.strip_prefix(':'))
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h.to_owned(), Some(p)),
                None => (authority.to_owned(), None),
            }
        };
        if host.is_empty() {
            return Err(bad());
        }
        let port = match port {
            Some(p) => p.parse().map_err(|_| bad())?,
            None if tls => 8443,
            None => 8080,
        };
        Ok(Self {
            tls,
            host,
            port,
            prefix: prefix.to_owned(),
        })
    }

    /// Whether the host is this machine.
    #[must_use]
    pub fn is_loopback(&self) -> bool {
        self.host == "localhost"
            || self
                .host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|a| a.is_loopback())
    }

    /// `host:port` for the `Host` header.
    #[must_use]
    pub fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

struct Conn {
    sender: SendRequest<Full<Bytes>>,
    task: AbortHandle,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// An HTTP client of one endpoint.
#[derive(Clone)]
pub struct HttpClient {
    ep: Endpoint,
    tls: Option<tokio_rustls::TlsConnector>,
    idle: Arc<Mutex<Vec<Conn>>>,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("ep", &self.ep)
            .finish_non_exhaustive()
    }
}

fn unreachable(msg: String) -> ShellError {
    ShellError::shell(ShellCode::Unreachable, msg)
}

/// rustls with the platform's roots plus `ca_file`.
fn tls_config(ca_file: Option<&Path>) -> Result<tokio_rustls::TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for c in native.certs {
        let _ = roots.add(c);
    }
    if let Some(p) = ca_file {
        let pem = std::fs::read(p).map_err(|e| {
            ShellError::shell(ShellCode::ProfileInvalid, format!("{}: {e}", p.display()))
        })?;
        for c in rustls_pemfile::certs(&mut pem.as_slice()) {
            let c = c.map_err(|e| {
                ShellError::shell(ShellCode::ProfileInvalid, format!("{}: {e}", p.display()))
            })?;
            roots.add(c).map_err(|e| {
                ShellError::shell(ShellCode::ProfileInvalid, format!("{}: {e}", p.display()))
            })?;
        }
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| unreachable(format!("tls: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsConnector::from(Arc::new(cfg)))
}

impl HttpClient {
    /// A client of `url`, trusting `ca_file` too on https.
    ///
    /// # Errors
    /// 39980 for a bad URL or CA file.
    pub fn new(url: &str, ca_file: Option<&Path>) -> Result<Self> {
        let ep = Endpoint::parse(url)?;
        let tls = if ep.tls {
            Some(tls_config(ca_file)?)
        } else {
            None
        };
        Ok(Self {
            ep,
            tls,
            idle: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// The endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.ep
    }

    async fn connect(&self) -> Result<Conn> {
        let addr = format!("{}:{}", self.ep.host, self.ep.port);
        let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr))
            .await
            .map_err(|_| unreachable(format!("{addr}: no answer within {CONNECT_TIMEOUT:?}")))?
            .map_err(|e| unreachable(format!("{addr}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let io: Box<dyn Io> = match &self.tls {
            Some(c) => {
                let name = rustls::pki_types::ServerName::try_from(self.ep.host.clone())
                    .map_err(|e| unreachable(format!("{}: {e}", self.ep.host)))?;
                Box::new(
                    c.connect(name, tcp)
                        .await
                        .map_err(|e| unreachable(format!("{addr}: tls: {e}")))?,
                )
            }
            None => Box::new(tcp),
        };
        let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .map_err(|e| unreachable(format!("{addr}: {e}")))?;
        let task = tokio::spawn(async move {
            let _ = conn.await;
        })
        .abort_handle();
        Ok(Conn { sender, task })
    }

    fn take_idle(&self) -> Option<Conn> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(c) = idle.pop() {
            if c.sender.is_ready() && !c.sender.is_closed() {
                return Some(c);
            }
        }
        None
    }

    /// Sends one request; the body is read through the [`Response`].
    ///
    /// # Errors
    /// 39987 when the endpoint can't be reached.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, String)],
        body: Option<Vec<u8>>,
    ) -> Result<Response> {
        let mut last = None;
        // A reused connection may have been closed by the server meanwhile:
        // one retry on a new connection, only when nothing was sent.
        for attempt in 0..2 {
            let mut conn = match (attempt, self.take_idle()) {
                (0, Some(c)) => c,
                _ => self.connect().await?,
            };
            let uri = format!("{}{path}", self.ep.prefix);
            let mut b = Request::builder()
                .method(method.clone())
                .uri(&uri)
                .header(hyper::header::HOST, self.ep.authority())
                .header(
                    hyper::header::USER_AGENT,
                    concat!("sprig/", env!("CARGO_PKG_VERSION")),
                );
            for (k, v) in headers {
                b = b.header(*k, v);
            }
            if body.is_some() {
                b = b.header(hyper::header::CONTENT_TYPE, "application/json");
            }
            let req = b
                .body(Full::new(Bytes::from(body.clone().unwrap_or_default())))
                .map_err(|e| unreachable(format!("{uri}: {e}")))?;
            if conn.sender.ready().await.is_err() {
                continue;
            }
            match conn.sender.send_request(req).await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let content_type = resp
                        .headers()
                        .get(hyper::header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    let etag = resp
                        .headers()
                        .get(hyper::header::ETAG)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    return Ok(Response {
                        status,
                        content_type,
                        etag,
                        body: resp.into_body(),
                        conn: Some(conn),
                        idle: Arc::clone(&self.idle),
                        buf: Vec::new(),
                    });
                }
                Err(e) if e.is_canceled() && attempt == 0 => {
                    last = Some(e.to_string());
                }
                Err(e) => return Err(unreachable(format!("{}: {e}", self.ep.authority()))),
            }
        }
        Err(unreachable(format!(
            "{}: {}",
            self.ep.authority(),
            last.unwrap_or_else(|| "connection closed".to_owned())
        )))
    }
}

/// A response whose body is read on demand.
pub struct Response {
    /// The HTTP status.
    pub status: u16,
    /// `Content-Type`.
    pub content_type: String,
    /// `ETag`.
    pub etag: Option<String>,
    body: Incoming,
    conn: Option<Conn>,
    idle: Arc<Mutex<Vec<Conn>>>,
    buf: Vec<u8>,
}

impl Response {
    fn finished(&mut self) {
        if let Some(c) = self.conn.take() {
            let mut idle = self
                .idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if idle.len() < MAX_IDLE {
                idle.push(c);
            }
        }
    }

    /// The next chunk of the body, or `None` at its end.
    async fn chunk(&mut self) -> Result<Option<Bytes>> {
        loop {
            match self.body.frame().await {
                Some(Ok(f)) => {
                    if let Ok(d) = f.into_data() {
                        return Ok(Some(d));
                    }
                }
                Some(Err(e)) => {
                    self.conn = None;
                    return Err(unreachable(format!("reading the reply: {e}")));
                }
                None => {
                    self.finished();
                    return Ok(None);
                }
            }
        }
    }

    /// The whole body.
    ///
    /// # Errors
    /// 39987 when the connection fails while reading.
    pub async fn bytes(mut self) -> Result<Vec<u8>> {
        let mut out = std::mem::take(&mut self.buf);
        while let Some(c) = self.chunk().await? {
            out.extend_from_slice(&c);
        }
        Ok(out)
    }

    /// The next line of an NDJSON body, without its newline; `None` at the
    /// end.
    ///
    /// # Errors
    /// 39987 when the connection fails while reading.
    pub async fn next_line(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some(i) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=i).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                return Ok(Some(line));
            }
            match self.chunk().await? {
                Some(c) => self.buf.extend_from_slice(&c),
                None if self.buf.is_empty() => return Ok(None),
                None => return Ok(Some(std::mem::take(&mut self.buf))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        let e = Endpoint::parse("http://127.0.0.1:8080").expect("plain");
        assert!(!e.tls && e.port == 8080 && e.is_loopback());
        let e = Endpoint::parse("https://gw.example/sprig/").expect("tls");
        assert_eq!((e.tls, e.port, e.prefix.as_str()), (true, 8443, "/sprig"));
        assert!(!e.is_loopback());
        let e = Endpoint::parse("http://[::1]:9").expect("v6");
        assert_eq!(e.authority(), "[::1]:9");
        assert!(Endpoint::parse("ftp://x").is_err());
        assert!(Endpoint::parse("http://:80").is_err());
    }
}
