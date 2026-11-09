//! Outbound calls from engine-tier code (chapter 41, "The fallback
//! runtime", Outbound): `Sprig.Http.Fetch` reaches only hosts on
//! `logic.outbound_allowlist`.
//!
//! ```text
//! fetch(url):
//!   scheme http or https, else 39867
//!   host (and port) on the allowlist, else 39867
//!   resolve; any address private, loopback, link-local, CGNAT, unspecified
//!     or multicast → 39867, unless the allowlist names that address literally
//!   connect to the resolved address (no second lookup, so DNS rebinding
//!     can't swap it), TLS for https with the host's name and webpki roots
//!   logic.outbound_timeout_ms for the whole call, logic.outbound_max_bytes
//!     for the body → 39864 naming the limit
//! ```
//!
//! Allowlist entries are `host`, `host:port`, `*.domain` (any subdomain,
//! not the domain itself) or an IP address, optionally with a port.
//! Without a port an entry allows 80 and 443 only.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Uri;
use hyper_util::rt::TokioIo;
use sprig_common::{Result, SprigCode, SprigError};

/// The outbound settings of one invocation.
#[derive(Debug, Clone)]
pub struct OutboundPolicy {
    /// `logic.outbound_allowlist`.
    pub allow: Vec<String>,
    /// `logic.outbound_timeout_ms`.
    pub timeout: Duration,
    /// `logic.outbound_max_bytes`.
    pub max_bytes: u64,
}

impl Default for OutboundPolicy {
    fn default() -> Self {
        Self {
            allow: Vec::new(),
            timeout: Duration::from_millis(2000),
            max_bytes: 1 << 20,
        }
    }
}

/// One request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `GET`, `POST`, ...
    pub method: String,
    /// `http://` or `https://`.
    pub url: String,
    /// Extra headers.
    pub headers: Vec<(String, String)>,
    /// The body, if any.
    pub body: Option<String>,
}

/// Its response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The status code.
    pub status: u16,
    /// Headers, lower-cased names.
    pub headers: Vec<(String, String)>,
    /// The body as text (invalid UTF-8 replaced).
    pub body: String,
}

/// Makes outbound calls for engine-tier code.
#[async_trait]
pub trait Outbound: Send + Sync {
    /// One call.
    ///
    /// # Errors
    /// 39867 when the policy refuses it; 39864 for the timeout or size
    /// limit; 39866 for a failed connection.
    async fn fetch(&self, req: Request) -> Result<Response>;
}

/// Refuses every call (no allowlist).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOutbound;

#[async_trait]
impl Outbound for NoOutbound {
    async fn fetch(&self, req: Request) -> Result<Response> {
        Err(denied(&format!(
            "{}: logic.outbound_allowlist is empty",
            host_of(&req.url)
        )))
    }
}

fn denied(msg: &str) -> SprigError {
    SprigError::new(
        SprigCode::LogicOutboundDenied,
        format!("outbound call refused: {msg}"),
    )
}

fn host_of(url: &str) -> String {
    url.parse::<Uri>()
        .ok()
        .and_then(|u| u.host().map(str::to_owned))
        .unwrap_or_else(|| url.chars().take(80).collect())
}

/// What the allowlist says about `host:port`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Allowed {
    /// Not listed.
    No,
    /// Listed by name (private addresses still refused).
    ByName,
    /// Listed as this literal address: private addresses allowed.
    Literal,
}

/// Checks `host` and `port` against `allow`.
#[must_use]
pub fn allowed(allow: &[String], host: &str, port: u16) -> Allowed {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    let literal = host.parse::<IpAddr>().is_ok();
    for entry in allow {
        let entry = entry.trim().to_ascii_lowercase();
        let (h, p) = split_port(&entry);
        let port_ok = match p {
            Some(p) => p == port,
            None => port == 80 || port == 443,
        };
        if !port_ok {
            continue;
        }
        let name_ok = if let Some(domain) = h.strip_prefix("*.") {
            host.ends_with(&format!(".{domain}"))
        } else {
            h == host
        };
        if name_ok {
            return if literal {
                Allowed::Literal
            } else {
                Allowed::ByName
            };
        }
    }
    Allowed::No
}

fn split_port(entry: &str) -> (&str, Option<u16>) {
    // `[::1]:8080`, `::1`, `host:443`, `host`
    if let Some(rest) = entry.strip_prefix('[') {
        if let Some((h, p)) = rest.split_once("]:") {
            return (h, p.parse().ok());
        }
        return (rest.trim_end_matches(']'), None);
    }
    if entry.matches(':').count() == 1
        && let Some((h, p)) = entry.split_once(':')
    {
        return (h, p.parse().ok());
    }
    (entry, None)
}

/// Whether `ip` is private, loopback, link-local, shared (CGNAT),
/// unspecified, broadcast or multicast: never reached by name.
#[must_use]
pub fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let o = v.octets();
            v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || v.is_unspecified()
                || v.is_broadcast()
                || v.is_multicast()
                || o[0] == 0
                || (o[0] == 100 && (64..128).contains(&o[1]))
        }
        IpAddr::V6(v) => {
            if let Some(v4) = v.to_ipv4_mapped() {
                return is_internal(IpAddr::V4(v4));
            }
            let s = v.segments();
            v.is_loopback()
                || v.is_unspecified()
                || v.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00
                || (s[0] & 0xffc0) == 0xfe80
        }
    }
}

/// The HTTP/1.1 client behind `Sprig.Http.Fetch`.
#[derive(Debug, Clone)]
pub struct HttpOutbound {
    policy: OutboundPolicy,
}

impl HttpOutbound {
    /// A client with `policy`.
    #[must_use]
    pub fn new(policy: OutboundPolicy) -> Self {
        Self { policy }
    }
}

static TLS: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_or_else(
            |_| {
                rustls::ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth()
            },
            |b| b.with_root_certificates(roots).with_no_client_auth(),
        );
    Arc::new(cfg)
});

fn failed(msg: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::LogicRuntimeError,
        format!("outbound call failed: {msg}"),
    )
}

#[async_trait]
impl Outbound for HttpOutbound {
    async fn fetch(&self, req: Request) -> Result<Response> {
        let p = &self.policy;
        match tokio::time::timeout(p.timeout, self.fetch_inner(req)).await {
            Ok(r) => r,
            Err(_) => Err(SprigError::new(
                SprigCode::LogicBudgetExceeded,
                format!(
                    "logic budget exceeded, outbound: no response within logic.outbound_timeout_ms = {} ms",
                    p.timeout.as_millis()
                ),
            )),
        }
    }
}

impl HttpOutbound {
    async fn fetch_inner(&self, req: Request) -> Result<Response> {
        let p = &self.policy;
        let uri: Uri = req
            .url
            .parse()
            .map_err(|e| failed(format!("bad URL {}: {e}", req.url)))?;
        let https = match uri.scheme_str() {
            Some("http") => false,
            Some("https") => true,
            other => {
                return Err(denied(&format!(
                    "scheme {} (only http and https)",
                    other.unwrap_or("none")
                )));
            }
        };
        let host = uri
            .host()
            .ok_or_else(|| failed("the URL has no host"))?
            .to_owned();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let bare = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let how = allowed(&p.allow, &bare, port);
        if how == Allowed::No {
            return Err(denied(&format!(
                "{bare}:{port} is not on logic.outbound_allowlist"
            )));
        }
        let addrs: Vec<SocketAddr> = match bare.parse::<IpAddr>() {
            Ok(ip) => vec![SocketAddr::new(ip, port)],
            Err(_) => tokio::net::lookup_host((bare.as_str(), port))
                .await
                .map_err(|e| failed(format!("can't resolve {bare}: {e}")))?
                .collect(),
        };
        if how != Allowed::Literal
            && let Some(a) = addrs.iter().find(|a| is_internal(a.ip()))
        {
            return Err(denied(&format!(
                "{bare} resolves to the internal address {}",
                a.ip()
            )));
        }
        let addr = *addrs
            .first()
            .ok_or_else(|| failed(format!("{bare} has no address")))?;
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| failed(format!("connecting to {addr}: {e}")))?;
        let method = hyper::Method::from_bytes(req.method.to_ascii_uppercase().as_bytes())
            .map_err(|e| failed(format!("bad method {}: {e}", req.method)))?;
        let path = uri
            .path_and_query()
            .map_or("/", hyper::http::uri::PathAndQuery::as_str)
            .to_owned();
        let mut b = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header(
                hyper::header::HOST,
                uri.authority().map_or(host.as_str(), |a| a.as_str()),
            )
            .header(hyper::header::USER_AGENT, "sprigdb-logic");
        for (k, v) in &req.headers {
            b = b.header(k.as_str(), v.as_str());
        }
        let body = Full::new(Bytes::from(req.body.unwrap_or_default()));
        let request = b.body(body).map_err(failed)?;
        if https {
            let name = rustls::pki_types::ServerName::try_from(bare.clone())
                .map_err(|e| failed(format!("bad TLS name {bare}: {e}")))?;
            let tls = tokio_rustls::TlsConnector::from(Arc::clone(&TLS))
                .connect(name, tcp)
                .await
                .map_err(|e| failed(format!("TLS with {bare}: {e}")))?;
            send(TokioIo::new(tls), request, p.max_bytes).await
        } else {
            send(TokioIo::new(tcp), request, p.max_bytes).await
        }
    }
}

async fn send<T>(io: T, request: hyper::Request<Full<Bytes>>, max: u64) -> Result<Response>
where
    T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(failed)?;
    let driver = tokio::spawn(conn);
    let result = async {
        let resp = sender.send_request(request).await.map_err(failed)?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_owned(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        let mut body = resp.into_body();
        let mut buf: Vec<u8> = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(failed)?;
            if let Some(d) = frame.data_ref() {
                buf.extend_from_slice(d);
                if buf.len() as u64 > max {
                    return Err(SprigError::new(
                        SprigCode::LogicBudgetExceeded,
                        format!(
                            "logic budget exceeded, outbound: the response passed logic.outbound_max_bytes = {max} bytes"
                        ),
                    ));
                }
            }
        }
        Ok(Response {
            status,
            headers,
            body: String::from_utf8_lossy(&buf).into_owned(),
        })
    }
    .await;
    driver.abort();
    result
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn list(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn allowlist_entries() {
        let a = list(&[
            "api.example.com",
            "*.svc.example.org",
            "10.1.2.3:8080",
            "[::1]:9000",
        ]);
        assert_eq!(allowed(&a, "api.example.com", 443), Allowed::ByName);
        assert_eq!(allowed(&a, "API.example.com", 80), Allowed::ByName);
        assert_eq!(allowed(&a, "api.example.com", 8443), Allowed::No);
        assert_eq!(allowed(&a, "x.svc.example.org", 443), Allowed::ByName);
        assert_eq!(allowed(&a, "svc.example.org", 443), Allowed::No);
        assert_eq!(allowed(&a, "evil-api.example.com", 443), Allowed::No);
        assert_eq!(allowed(&a, "10.1.2.3", 8080), Allowed::Literal);
        assert_eq!(allowed(&a, "10.1.2.3", 80), Allowed::No);
        assert_eq!(allowed(&a, "::1", 9000), Allowed::Literal);
        assert_eq!(allowed(&a, "169.254.169.254", 80), Allowed::No);
    }

    #[test]
    fn internal_addresses() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(is_internal(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "2606:4700::1111", "100.128.0.1"] {
            assert!(!is_internal(ip.parse().unwrap()), "{ip}");
        }
    }

    /// A one-shot HTTP server on 127.0.0.1 answering `body`.
    async fn server(body: &'static str) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).await.unwrap();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
        });
        port
    }

    #[tokio::test]
    async fn a_literally_listed_address_is_reached_and_limits_hold() {
        let port = server("hello").await;
        let o = HttpOutbound::new(OutboundPolicy {
            allow: vec![format!("127.0.0.1:{port}")],
            ..OutboundPolicy::default()
        });
        let r = o
            .fetch(Request {
                method: "GET".into(),
                url: format!("http://127.0.0.1:{port}/x?y=1"),
                headers: vec![("x-test".into(), "1".into())],
                body: None,
            })
            .await
            .unwrap();
        assert_eq!((r.status, r.body.as_str()), (200, "hello"));
        // The same server by name: loopback is refused unless listed
        // literally.
        let o = HttpOutbound::new(OutboundPolicy {
            allow: vec![format!("localhost:{port}")],
            ..OutboundPolicy::default()
        });
        let e = o
            .fetch(Request {
                method: "GET".into(),
                url: format!("http://localhost:{port}/"),
                headers: Vec::new(),
                body: None,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicOutboundDenied), "{e}");
        // A body past logic.outbound_max_bytes.
        let port = server("0123456789").await;
        let o = HttpOutbound::new(OutboundPolicy {
            allow: vec![format!("127.0.0.1:{port}")],
            max_bytes: 4,
            ..OutboundPolicy::default()
        });
        let e = o
            .fetch(Request {
                method: "GET".into(),
                url: format!("http://127.0.0.1:{port}/"),
                headers: Vec::new(),
                body: None,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicBudgetExceeded), "{e}");
        assert!(e.message.contains("outbound_max_bytes"), "{e}");
    }

    #[tokio::test]
    async fn refusals_come_before_any_connection() {
        let o = HttpOutbound::new(OutboundPolicy {
            allow: list(&["localhost:1", "example.invalid"]),
            ..OutboundPolicy::default()
        });
        let get = |url: &str| Request {
            method: "GET".into(),
            url: url.into(),
            headers: Vec::new(),
            body: None,
        };
        for url in [
            "http://169.254.169.254/latest/meta-data/",
            "ftp://example.invalid/x",
            "http://localhost:1/",
        ] {
            let e = o.fetch(get(url)).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::LogicOutboundDenied), "{url}: {e}");
        }
    }
}
