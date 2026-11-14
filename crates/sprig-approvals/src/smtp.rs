//! A small SMTP client for approval emails.
//!
//! - `smtp://host[:25]`: plain SMTP. A password is sent only to a loopback
//!   relay: anywhere else, use TLS.
//! - `smtp+starttls://host[:587]`: `STARTTLS` is required. A server that
//!   doesn't offer it is refused, never used in the clear.
//! - `smtps://host[:465]`: implicit TLS (RFC 8314).
//!
//! TLS is the workspace's rustls with ring. The server certificate is
//! checked against the Mozilla roots (`webpki-roots`), or against the CA
//! in `[controller] smtp_ca_file`, for the URL's host name. A client
//! certificate (`smtp_client_cert_file` and `smtp_client_key_file`) is
//! offered when both are set. `AUTH PLAIN` follows `approvals.smtp_username`.
//! The protocol is a few lines of text (RFC 5321); no mail crate is needed.

use rustls::pki_types::pem::PemObject as _;
use std::fmt::Write as _;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;

use sprig_common::{Result, SprigCode, SprigError};

/// How long one SMTP exchange may take.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// How the connection is protected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Security {
    /// Plain SMTP (`smtp://`).
    Plain,
    /// Plain, then `STARTTLS`, which is required (`smtp+starttls://`).
    StartTls,
    /// TLS from the first byte (`smtps://`).
    Implicit,
}

/// TLS material from `[controller]` files, as PEM text.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TlsFiles {
    /// `smtp_ca_file`: the roots the server is checked against instead of
    /// the Mozilla roots.
    pub ca_pem: Option<String>,
    /// `smtp_client_cert_file`: a client certificate chain.
    pub client_cert_pem: Option<String>,
    /// `smtp_client_key_file`: its private key.
    pub client_key_pem: Option<String>,
}

impl std::fmt::Debug for TlsFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsFiles")
            .field("ca", &self.ca_pem.is_some())
            .field("client_cert", &self.client_cert_pem.is_some())
            .finish_non_exhaustive()
    }
}

/// Where and how to send.
#[derive(Clone, PartialEq, Eq)]
pub struct SmtpConfig {
    /// The host, as written (TLS checks the certificate for it).
    pub host: String,
    /// The port.
    pub port: u16,
    /// Plain, STARTTLS or implicit TLS.
    pub security: Security,
    /// The sender.
    pub from: String,
    /// `AUTH PLAIN` user and password, when set.
    pub auth: Option<(String, String)>,
    /// CA and client certificate.
    pub tls: TlsFiles,
}

impl std::fmt::Debug for SmtpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("security", &self.security)
            .field("from", &self.from)
            .field("auth", &self.auth.as_ref().map(|(u, _)| u))
            .field("tls", &self.tls)
            .finish()
    }
}

impl SmtpConfig {
    /// Parses `approvals.smtp_url`. `None` when empty (email is off).
    ///
    /// # Errors
    /// 39977 for a URL this client can't use, or a password that would go
    /// in the clear to a host other than loopback.
    pub fn parse(
        url: &str,
        from: &str,
        auth: Option<(String, String)>,
        tls: TlsFiles,
    ) -> Result<Option<Self>> {
        let url = url.trim();
        if url.is_empty() {
            return Ok(None);
        }
        let bad = |m: &str| {
            SprigError::new(
                SprigCode::ApprovalPolicyInvalid,
                format!("approvals.smtp_url {url:?}: {m}"),
            )
        };
        let (security, rest, default_port) = if let Some(r) = url.strip_prefix("smtps://") {
            (Security::Implicit, r, 465)
        } else if let Some(r) = url.strip_prefix("smtp+starttls://") {
            (Security::StartTls, r, 587)
        } else if let Some(r) = url.strip_prefix("smtp://") {
            (Security::Plain, r, 25)
        } else {
            return Err(bad(
                "expected smtp://, smtp+starttls:// or smtps:// then host[:port]",
            ));
        };
        let hp = rest.trim_end_matches('/');
        if hp.is_empty() || hp.contains(['/', '?', '@', ' ']) {
            return Err(bad("expected host[:port] after the scheme"));
        }
        let (host, port) = split_host_port(hp, default_port).ok_or_else(|| bad("bad port"))?;
        if security == Security::Plain && auth.is_some() && !is_loopback(&host) {
            return Err(bad(
                "a password is sent in the clear only to a loopback relay; use \
                 smtp+starttls:// or smtps://",
            ));
        }
        if tls.client_cert_pem.is_some() != tls.client_key_pem.is_some() {
            return Err(bad(
                "a client certificate needs both smtp_client_cert_file and smtp_client_key_file",
            ));
        }
        Ok(Some(Self {
            host,
            port,
            security,
            from: from.to_owned(),
            auth,
            tls,
        }))
    }

    /// `host:port`, for connecting.
    #[must_use]
    pub fn addr(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// `host[:port]` and `[v6][:port]`.
fn split_host_port(s: &str, default: u16) -> Option<(String, u16)> {
    if let Some(r) = s.strip_prefix('[') {
        let (h, after) = r.split_once(']')?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse().ok()?,
            None if after.is_empty() => default,
            None => return None,
        };
        return Some((h.to_owned(), port));
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => Some((h.to_owned(), p.parse().ok()?)),
        // No port, or a bare IPv6 address.
        _ => Some((s.to_owned(), default)),
    }
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// One message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    /// Recipients.
    pub to: Vec<String>,
    /// Subject (one line).
    pub subject: String,
    /// Plain-text body.
    pub body: String,
}

fn clean(s: &str) -> String {
    s.chars().filter(|c| *c != '\r' && *c != '\n').collect()
}

fn base64(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The message text with headers, dot-stuffed, ending in `CRLF.CRLF`.
#[must_use]
pub fn render(from: &str, m: &Mail, date: &str, id: &str) -> String {
    let mut s = String::new();
    let _ = write!(s, "From: {}\r\n", clean(from));
    let to = m.to.iter().map(|t| clean(t)).collect::<Vec<_>>().join(", ");
    let _ = write!(s, "To: {to}\r\n");
    let _ = write!(s, "Subject: {}\r\n", clean(&m.subject));
    let _ = write!(s, "Date: {date}\r\n");
    let _ = write!(s, "Message-ID: <{id}>\r\n");
    s.push_str("MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n");
    for line in m.body.replace("\r\n", "\n").split('\n') {
        if line.starts_with('.') {
            s.push('.');
        }
        s.push_str(line);
        s.push_str("\r\n");
    }
    s.push_str(".\r\n");
    s
}

/// The rustls client configuration for `c`: the CA file or the Mozilla
/// roots, and the client certificate when set.
///
/// # Errors
/// 39977 for PEM that doesn't parse.
pub fn tls_config(t: &TlsFiles) -> Result<Arc<rustls::ClientConfig>> {
    let bad = |m: String| SprigError::new(SprigCode::ApprovalPolicyInvalid, m);
    let mut roots = rustls::RootCertStore::empty();
    if let Some(pem) = &t.ca_pem {
        let mut n = 0;
        for c in rustls::pki_types::CertificateDer::pem_slice_iter(pem.as_bytes()) {
            let c = c.map_err(|e| bad(format!("smtp_ca_file: {e}")))?;
            roots
                .add(c)
                .map_err(|e| bad(format!("smtp_ca_file: {e}")))?;
            n += 1;
        }
        if n == 0 {
            return Err(bad("smtp_ca_file holds no certificate".into()));
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let b = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| bad(format!("smtp tls: {e}")))?
        .with_root_certificates(roots);
    let cfg = match (&t.client_cert_pem, &t.client_key_pem) {
        (Some(cert), Some(key)) => {
            let chain: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(cert.as_bytes())
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| bad(format!("smtp_client_cert_file: {e}")))?;
            let key =
                rustls::pki_types::PrivateKeyDer::from_pem_slice(key.as_bytes()).map_err(|e| {
                    match e {
                        rustls::pki_types::pem::Error::NoItemsFound => {
                            bad("smtp_client_key_file holds no key".into())
                        }
                        e => bad(format!("smtp_client_key_file: {e}")),
                    }
                })?;
            b.with_client_auth_cert(chain, key)
                .map_err(|e| bad(format!("smtp client certificate: {e}")))?
        }
        _ => b.with_no_client_auth(),
    };
    Ok(Arc::new(cfg))
}

/// The connection: plain TCP, or TLS over it.
enum Stream {
    Tcp(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

async fn handshake(
    cfg: &SmtpConfig,
    tcp: TcpStream,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let name = rustls::pki_types::ServerName::try_from(cfg.host.clone())
        .map_err(|e| fail(&format!("host {:?} can't be checked by TLS: {e}", cfg.host)))?;
    tokio_rustls::TlsConnector::from(tls_config(&cfg.tls)?)
        .connect(name, tcp)
        .await
        .map_err(|e| fail(&format!("TLS with {}: {e}", cfg.addr())))
}

struct Session {
    r: BufReader<Stream>,
}

impl Session {
    /// Reads one reply (all its lines); returns the code and the text.
    async fn reply(&mut self) -> Result<(u16, String)> {
        let mut text = String::new();
        loop {
            let mut line = String::new();
            let n = self.r.read_line(&mut line).await.map_err(io_err)?;
            if n == 0 {
                return Err(fail("the server closed the connection"));
            }
            text.push_str(&line);
            let b = line.as_bytes();
            if b.len() < 4 || b[3] != b'-' {
                let code = line.get(..3).and_then(|c| c.parse().ok()).unwrap_or(0);
                return Ok((code, text));
            }
        }
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let w = self.r.get_mut();
        w.write_all(bytes).await.map_err(io_err)?;
        w.flush().await.map_err(io_err)
    }

    async fn cmd(&mut self, line: &str, want: u16) -> Result<String> {
        let mut b = line.as_bytes().to_vec();
        b.extend_from_slice(b"\r\n");
        self.write(&b).await?;
        let (code, text) = self.reply().await?;
        if code / 100 != want / 100 {
            return Err(fail(&format!(
                "{} answered {}",
                line.split(' ').next().unwrap_or(line),
                text.trim()
            )));
        }
        Ok(text)
    }

    /// `STARTTLS`: anything the server sent after its `220` would be read
    /// as if it came through TLS (a command injection), so it is refused.
    async fn starttls(self, cfg: &SmtpConfig) -> Result<Self> {
        if !self.r.buffer().is_empty() {
            return Err(fail("the server sent data after its STARTTLS answer"));
        }
        let Stream::Tcp(tcp) = self.r.into_inner() else {
            return Err(fail("STARTTLS on a TLS connection"));
        };
        let tls = handshake(cfg, tcp).await?;
        Ok(Self {
            r: BufReader::new(Stream::Tls(Box::new(tls))),
        })
    }
}

/// Whether an `EHLO` answer offers `ext` (one of its `250-` lines).
fn offers(ehlo: &str, ext: &str) -> bool {
    ehlo.lines().any(|l| {
        l.get(4..)
            .and_then(|r| r.split_whitespace().next())
            .is_some_and(|w| w.eq_ignore_ascii_case(ext))
    })
}

#[allow(clippy::needless_pass_by_value)] // used as map_err(io_err)
fn io_err(e: io::Error) -> SprigError {
    fail(&e.to_string())
}

fn fail(m: &str) -> SprigError {
    SprigError::new(SprigCode::NotifyFailed, format!("email: {m}"))
}

/// Sends `m` through `c`.
///
/// # Errors
/// 39978 with the server's answer, or why TLS failed.
#[allow(clippy::many_single_char_names)] // config, mail, session, reply
pub async fn send(c: &SmtpConfig, m: &Mail, date: &str, id: &str) -> Result<()> {
    if m.to.is_empty() {
        return Ok(());
    }
    let run = async {
        let tcp = TcpStream::connect(c.addr()).await.map_err(io_err)?;
        let _ = tcp.set_nodelay(true);
        let stream = if c.security == Security::Implicit {
            Stream::Tls(Box::new(handshake(c, tcp).await?))
        } else {
            Stream::Tcp(tcp)
        };
        let mut s = Session {
            r: BufReader::new(stream),
        };
        let (code, text) = s.reply().await?;
        if code != 220 {
            return Err(fail(&format!("greeting {}", text.trim())));
        }
        let mut ehlo = s.cmd("EHLO sprigdb", 250).await?;
        if c.security == Security::StartTls {
            if !offers(&ehlo, "STARTTLS") {
                return Err(fail(
                    "the server doesn't offer STARTTLS, which smtp+starttls:// requires",
                ));
            }
            s.cmd("STARTTLS", 220).await?;
            s = s.starttls(c).await?;
            ehlo = s.cmd("EHLO sprigdb", 250).await?;
        }
        if let Some((u, p)) = &c.auth {
            if !offers(&ehlo, "AUTH") {
                return Err(fail("the server offers no AUTH"));
            }
            let token = base64(format!("\0{u}\0{p}").as_bytes());
            s.cmd(&format!("AUTH PLAIN {token}"), 235).await?;
        }
        s.cmd(&format!("MAIL FROM:<{}>", clean(&c.from)), 250)
            .await?;
        for t in &m.to {
            s.cmd(&format!("RCPT TO:<{}>", clean(t)), 250).await?;
        }
        s.cmd("DATA", 354).await?;
        let msg = render(&c.from, m, date, id);
        s.write(msg.as_bytes()).await?;
        let (code, text) = s.reply().await?;
        if code / 100 != 2 {
            return Err(fail(&format!("DATA answered {}", text.trim())));
        }
        let _ = s.cmd("QUIT", 221).await;
        Ok(())
    };
    tokio::time::timeout(TIMEOUT, run)
        .await
        .map_err(|_| fail("timed out"))?
}

#[cfg(test)]
mod tests;
