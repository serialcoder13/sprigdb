//! The webhook target (chapter 51, "Webhook target"; build step 9).
//!
//! ```text
//! deliver(batch):
//!   format json | ndjson | cloudevents and batch = on → one POST for the batch
//!   cloudevents_binary or batch = off              → one POST per record
//!   POST <url><PATH>: Content-Type, Sprig-Idempotency-Key (batch id, or the
//!     record's key), Sprig-Sink, Sprig-Source, traceparent, the signature
//!     (sprig or Standard Webhooks) and the connection's authentication
//!   2xx → ok (partial_results = on: per-record results from the body)
//!   408, 429, 5xx, 401, 403, connect/TLS/timeouts → destination-wide retry,
//!     Retry-After honoured (401 refreshes an OAuth2 token once first)
//!   413 → halve the batch; 3xx (redirects are never followed) and other
//!     4xx → permanent: the runtime halves until the poison record is alone
//! ```
//!
//! The client is hyper 1.x over the address the destination rules checked
//! (`Destinations::resolve`): the name is used for TLS (SNI and the
//! certificate) and the `Host` header only, so a second lookup can't swap
//! the address. HTTP/2 comes from ALPN (`http2 = auto`), is off, or is
//! spoken without negotiation (`prior_knowledge`, also over plain TCP:
//! h2c). Connections are pooled per target: one shared HTTP/2 connection,
//! or up to [`MAX_IDLE_H1`] idle HTTP/1.1 connections.

// Product and protocol names (CloudEvents, OAuth2) read better bare.
#![allow(clippy::doc_markdown)]

use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::client::conn::{http1, http2};
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Map, Value, json};
use sprig_common::{Result, SprigCode};
use zeroize::Zeroizing;

use super::kafka::{ce_type, json_value, rfc3339};
use super::{Conn, TargetCtx};
use crate::model::{
    Batch, BatchOutcome, ConnKind, Outcome, SinkRecord, TargetSpec, bad_option, delivery_failed,
    err, hex,
};
use crate::policy::url_host_port;
use crate::runtime::Target;

/// Idle HTTP/1.1 connections kept per target.
pub const MAX_IDLE_H1: usize = 8;

/// The largest response body read (partial results, token responses).
const MAX_RESPONSE_BYTES: usize = 4 << 20;

/// `format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// A JSON array of records.
    Json,
    /// One JSON record per line.
    Ndjson,
    /// `application/cloudevents-batch+json`.
    CloudEvents,
    /// One request per record, CloudEvents attributes in `ce-` headers.
    CloudEventsBinary,
}

impl Format {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "json" => Format::Json,
            "ndjson" => Format::Ndjson,
            "cloudevents" => Format::CloudEvents,
            "cloudevents_binary" => Format::CloudEventsBinary,
            other => {
                return Err(err(
                    SprigCode::SinkFormatUnsupported,
                    format!(
                        "webhook format '{other}': json, ndjson, cloudevents or cloudevents_binary"
                    ),
                ));
            }
        })
    }

    fn content_type(self) -> &'static str {
        match self {
            Format::Json | Format::CloudEventsBinary => "application/json",
            Format::Ndjson => "application/x-ndjson",
            Format::CloudEvents => "application/cloudevents-batch+json",
        }
    }
}

/// `signature`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signature {
    /// `Sprig-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, t "." body)>`.
    Sprig,
    /// Standard Webhooks: `webhook-id`, `webhook-timestamp`,
    /// `webhook-signature: v1,<base64 HMAC-SHA256(key, id "." ts "." body)>`.
    Standard,
    /// No signature.
    None,
}

/// `http2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum H2Mode {
    /// ALPN decides (TLS only; plain TCP speaks HTTP/1.1).
    Auto,
    /// HTTP/1.1 only.
    Off,
    /// HTTP/2 without negotiation (h2c over plain TCP).
    PriorKnowledge,
}

/// The connection's `auth`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// None (also `mtls`: the client certificate is the authentication).
    None,
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// `Authorization: Basic <username:password>`.
    Basic,
    /// A named header with the credential `header_value`.
    Header(String),
    /// OAuth2 client credentials against `token_url`.
    OAuth2 {
        /// The token endpoint.
        token_url: String,
        /// `scope`.
        scope: Option<String>,
        /// `audience`.
        audience: Option<String>,
    },
}

/// The HMAC-SHA256 `Sprig-Signature` header value for `body` at `t`.
#[must_use]
pub fn sprig_signature(secret: &[u8], t: u64, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
    let mut ctx = ring::hmac::Context::with_key(&key);
    ctx.update(t.to_string().as_bytes());
    ctx.update(b".");
    ctx.update(body);
    format!("t={t},v1={}", hex(ctx.sign().as_ref()))
}

/// The key of a Standard Webhooks secret: base64 after `whsec_` (the
/// whole text when it isn't base64).
#[must_use]
pub fn standard_key(secret: &str) -> Vec<u8> {
    let s = secret.strip_prefix("whsec_").unwrap_or(secret);
    B64.decode(s).unwrap_or_else(|_| s.as_bytes().to_vec())
}

/// The Standard Webhooks `webhook-signature` value.
#[must_use]
pub fn standard_signature(secret: &str, id: &str, ts: u64, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &standard_key(secret));
    let mut ctx = ring::hmac::Context::with_key(&key);
    ctx.update(id.as_bytes());
    ctx.update(b".");
    ctx.update(ts.to_string().as_bytes());
    ctx.update(b".");
    ctx.update(body);
    format!("v1,{}", B64.encode(ctx.sign().as_ref()))
}

/// `Retry-After` as seconds or an HTTP date, measured from `now`.
#[must_use]
pub fn retry_after(v: &str, now: SystemTime) -> Option<Duration> {
    let v = v.trim();
    if let Ok(s) = v.parse::<u64>() {
        return Some(Duration::from_secs(s));
    }
    let at = httpdate::parse_http_date(v).ok()?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

/// `application/x-www-form-urlencoded` of one value.
fn form(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for b in v.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else if b == b' ' {
            out.push('+');
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A W3C `traceparent` with random ids.
fn traceparent() -> String {
    let mut b = [0u8; 24];
    let _ = getrandom::fill(&mut b);
    if b[..16].iter().all(|x| *x == 0) {
        b[0] = 1;
    }
    if b[16..].iter().all(|x| *x == 0) {
        b[16] = 1;
    }
    format!("00-{}-{}-01", hex(&b[..16]), hex(&b[16..]))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// One pooled sender.
enum Sender {
    H1(http1::SendRequest<Full<Bytes>>),
    H2(http2::SendRequest<Full<Bytes>>),
}

#[derive(Default)]
struct Pool {
    idle: Vec<http1::SendRequest<Full<Bytes>>>,
    h2: Option<http2::SendRequest<Full<Bytes>>>,
}

/// One endpoint: where requests go and how they connect.
struct Endpoint {
    conn: Conn,
    host: String,
    port: u16,
    tls: bool,
    /// `host[:port]` for `Host` and `:authority`.
    authority: String,
    h2: H2Mode,
    connect_timeout: Duration,
    pool: StdMutex<Pool>,
}

impl Endpoint {
    fn new(conn: Conn, url: &str, h2: H2Mode, connect_timeout: Duration) -> Result<Self> {
        let (host, port, tls) = url_host_port(url)?;
        let default = if tls { 443 } else { 80 };
        let bare = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host.clone()
        };
        let authority = if port == default {
            bare
        } else {
            format!("{bare}:{port}")
        };
        Ok(Self {
            conn,
            host,
            port,
            tls,
            authority,
            h2,
            connect_timeout,
            pool: StdMutex::new(Pool::default()),
        })
    }

    fn pool(&self) -> std::sync::MutexGuard<'_, Pool> {
        self.pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A ready sender: the shared HTTP/2 one, an idle HTTP/1.1 one, or a
    /// new connection to the checked address.
    async fn sender(&self) -> Result<Sender> {
        {
            let mut p = self.pool();
            if let Some(h) = &p.h2 {
                if h.is_ready() {
                    return Ok(Sender::H2(h.clone()));
                }
                p.h2 = None;
            }
            while let Some(s) = p.idle.pop() {
                if s.is_ready() {
                    return Ok(Sender::H1(s));
                }
            }
        }
        match tokio::time::timeout(self.connect_timeout, self.connect()).await {
            Ok(r) => r,
            Err(_) => Err(delivery_failed(format!(
                "{}: no connection within sink.http_connect_timeout_ms = {} ms",
                self.authority,
                self.connect_timeout.as_millis()
            ))),
        }
    }

    async fn connect(&self) -> Result<Sender> {
        let addr = self
            .conn
            .dest
            .resolve(
                &self.host,
                self.port,
                Some(ConnKind::Webhook),
                Some(&self.conn.options),
            )
            .await?;
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| delivery_failed(format!("{}: connect failed: {e}", self.authority)))?;
        let _ = tcp.set_nodelay(true);
        if !self.tls {
            return if self.h2 == H2Mode::PriorKnowledge {
                self.h2_handshake(TokioIo::new(tcp)).await
            } else {
                self.h1_handshake(TokioIo::new(tcp)).await
            };
        }
        let alpn: &[&[u8]] = match self.h2 {
            H2Mode::Auto => &[b"h2", b"http/1.1"],
            H2Mode::Off => &[b"http/1.1"],
            H2Mode::PriorKnowledge => &[b"h2"],
        };
        let cfg = super::tls_config(&self.conn, alpn)?;
        let name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|e| bad_option(format!("url host '{}': {e}", self.host)))?;
        let tls = tokio_rustls::TlsConnector::from(cfg)
            .connect(name, tcp)
            .await
            .map_err(|e| delivery_failed(format!("{}: TLS failed: {e}", self.authority)))?;
        let h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2".as_slice())
            || self.h2 == H2Mode::PriorKnowledge;
        if h2 {
            self.h2_handshake(TokioIo::new(tls)).await
        } else {
            self.h1_handshake(TokioIo::new(tls)).await
        }
    }

    async fn h1_handshake<T>(&self, io: T) -> Result<Sender>
    where
        T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let (s, c) = http1::handshake(io)
            .await
            .map_err(|e| delivery_failed(format!("{}: HTTP/1.1 handshake: {e}", self.authority)))?;
        tokio::spawn(async move {
            let _ = c.await;
        });
        Ok(Sender::H1(s))
    }

    async fn h2_handshake<T>(&self, io: T) -> Result<Sender>
    where
        T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let (s, c) = http2::handshake(TokioExecutor::new(), io)
            .await
            .map_err(|e| delivery_failed(format!("{}: HTTP/2 handshake: {e}", self.authority)))?;
        tokio::spawn(async move {
            let _ = c.await;
        });
        self.pool().h2 = Some(s.clone());
        Ok(Sender::H2(s))
    }

    /// Sends one request; the response's status, headers and body.
    async fn send(
        &self,
        path: &str,
        headers: &[(String, String)],
        body: Bytes,
    ) -> Result<(StatusCode, hyper::HeaderMap, Bytes)> {
        self.send_method(hyper::Method::POST, path, headers, body)
            .await
    }

    /// [`Self::send`] with another method (`OPTIONS` for `TEST
    /// CONNECTION`, R9 r9tests).
    async fn send_method(
        &self,
        method: hyper::Method,
        path: &str,
        headers: &[(String, String)],
        body: Bytes,
    ) -> Result<(StatusCode, hyper::HeaderMap, Bytes)> {
        let mut sender = self.sender().await?;
        let scheme = if self.tls { "https" } else { "http" };
        let uri: Uri = match &sender {
            Sender::H2(_) => format!("{scheme}://{}{path}", self.authority),
            Sender::H1(_) => path.to_owned(),
        }
        .parse()
        .map_err(|e| bad_option(format!("webhook path '{path}': {e}")))?;
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Full::new(body))
            .map_err(|e| bad_option(format!("webhook request: {e}")))?;
        let h = req.headers_mut();
        if let Sender::H1(_) = sender {
            h.insert(
                hyper::header::HOST,
                HeaderValue::from_str(&self.authority)
                    .map_err(|e| bad_option(format!("host: {e}")))?,
            );
        }
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| bad_option(format!("header {k}: {e}")))?;
            let mut value = HeaderValue::from_str(v)
                .map_err(|e| bad_option(format!("header {k}: value not allowed: {e}")))?;
            if k.eq_ignore_ascii_case("authorization") || k.starts_with("x-") {
                value.set_sensitive(true);
            }
            h.insert(name, value);
        }
        let resp = match &mut sender {
            Sender::H1(s) => {
                s.ready()
                    .await
                    .map_err(|e| delivery_failed(format!("{}: {e}", self.authority)))?;
                s.send_request(req).await
            }
            Sender::H2(s) => {
                s.ready()
                    .await
                    .map_err(|e| delivery_failed(format!("{}: {e}", self.authority)))?;
                s.send_request(req).await
            }
        }
        .map_err(|e| delivery_failed(format!("{}: request failed: {e}", self.authority)))?;
        let (parts, body) = resp.into_parts();
        let bytes = Limited::new(body, MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|e| delivery_failed(format!("{}: response body: {e}", self.authority)))?
            .to_bytes();
        if let Sender::H1(s) = sender {
            let mut p = self.pool();
            if p.idle.len() < MAX_IDLE_H1 && !s.is_closed() {
                p.idle.push(s);
            }
        }
        Ok((parts.status, parts.headers, bytes))
    }
}

/// A cached OAuth2 access token.
struct Token {
    value: Zeroizing<String>,
    refresh_at: Instant,
}

/// The webhook target.
pub struct WebhookTarget {
    ep: Endpoint,
    /// The token endpoint (OAuth2), its own checked connection.
    token_ep: Option<Endpoint>,
    path: String,
    auth: Auth,
    format: Format,
    batch: bool,
    signature: Signature,
    partial: bool,
    timeout: Duration,
    sink_name: String,
    source_name: String,
    ce_source: String,
    token: tokio::sync::Mutex<Option<Token>>,
}

impl std::fmt::Debug for WebhookTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookTarget")
            .field("endpoint", &self.ep.authority)
            .field("path", &self.path)
            .field("auth", &self.auth)
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

/// Builds the webhook target of `ctx` (nothing connects yet).
///
/// # Errors
/// 39218 for a bad option; 39231 for an unknown format; 39216 for a
/// missing credential the options need.
pub fn build(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    Ok(Box::new(WebhookTarget::new(ctx)?))
}

impl WebhookTarget {
    /// See [`build`].
    ///
    /// # Errors
    /// See [`build`].
    #[allow(clippy::too_many_lines)] // every option, checked in turn
    pub fn new(ctx: &TargetCtx) -> Result<Self> {
        let conn = ctx.conn()?.clone();
        let url = conn.need("url")?;
        let o = &ctx.opts;
        let h2 = match o
            .get("http2")
            .map(str::to_owned)
            .or_else(|| conn.opt("http2"))
            .unwrap_or_else(|| "auto".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "auto" | "on" => H2Mode::Auto,
            "off" => H2Mode::Off,
            "prior_knowledge" => H2Mode::PriorKnowledge,
            other => {
                return Err(bad_option(format!(
                    "http2 '{other}': auto, off or prior_knowledge"
                )));
            }
        };
        let connect_timeout = Duration::from_millis(o.u64("http_connect_timeout_ms", 5000)?);
        let timeout = Duration::from_millis(o.u64("http_timeout_ms", 30_000)?);
        let format = Format::parse(&o.text("format", "json"))?;
        let batch = o.flag("batch", true) && format != Format::CloudEventsBinary;
        let signature = match o
            .get("signature")
            .map(str::to_owned)
            .or_else(|| conn.opt("signature"))
            .unwrap_or_else(|| "sprig".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "sprig" => Signature::Sprig,
            "standard" => Signature::Standard,
            "none" => Signature::None,
            other => {
                return Err(bad_option(format!(
                    "signature '{other}': sprig, standard or none"
                )));
            }
        };
        if signature != Signature::None {
            conn.secret("signing_secret")?;
        }
        let auth = match conn
            .opt("auth")
            .unwrap_or_else(|| "none".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "none" | "mtls" => Auth::None,
            "bearer" => {
                conn.secret("token")?;
                Auth::Bearer
            }
            "basic" => {
                conn.secret("username")?;
                conn.secret("password")?;
                Auth::Basic
            }
            "header" => {
                conn.secret("header_value")?;
                Auth::Header(conn.need("auth_header")?.to_ascii_lowercase())
            }
            "oauth2_client_credentials" => {
                conn.secret("client_id")?;
                conn.secret("client_secret")?;
                Auth::OAuth2 {
                    token_url: conn.need("token_url")?,
                    scope: conn.opt("scope"),
                    audience: conn.opt("audience"),
                }
            }
            other => {
                return Err(bad_option(format!(
                    "auth '{other}': none, bearer, basic, header, oauth2_client_credentials or mtls"
                )));
            }
        };
        let path = match &ctx.sink.target {
            TargetSpec::Webhook { path } => path.clone(),
            _ => None,
        };
        let base: Uri = url
            .parse()
            .map_err(|e| bad_option(format!("bad url '{url}': {e}")))?;
        let mut full = base.path().trim_end_matches('/').to_owned();
        if let Some(p) = path.as_deref() {
            if !p.starts_with('/') {
                full.push('/');
            }
            full.push_str(p);
        }
        if full.is_empty() {
            full.push('/');
        }
        if let Some(q) = base.query() {
            full.push('?');
            full.push_str(q);
        }
        let token_ep = match &auth {
            Auth::OAuth2 { token_url, .. } => Some(Endpoint::new(
                conn.clone(),
                token_url,
                H2Mode::Off,
                connect_timeout,
            )?),
            _ => None,
        };
        Ok(Self {
            ep: Endpoint::new(conn, &url, h2, connect_timeout)?,
            token_ep,
            path: full,
            auth,
            format,
            batch,
            signature,
            partial: o.flag("partial_results", false),
            timeout,
            sink_name: ctx.sink.name.clone(),
            source_name: ctx.sink.source.name(),
            ce_source: ctx.ce_source(),
            token: tokio::sync::Mutex::new(None),
        })
    }

    /// One CloudEvents structured event.
    fn cloud_event(&self, r: &SinkRecord) -> Value {
        let mut m = Map::new();
        m.insert("specversion".into(), json!("1.0"));
        m.insert("id".into(), json!(r.idem_key));
        m.insert("source".into(), json!(self.ce_source));
        m.insert("type".into(), json!(ce_type(r)));
        m.insert("time".into(), json!(rfc3339(r.ts_ms)));
        if let Some(k) = r.key_text() {
            m.insert("subject".into(), json!(k));
        }
        m.insert("datacontenttype".into(), json!("application/json"));
        m.insert("data".into(), json_value(r));
        Value::Object(m)
    }

    /// The body of a request for `recs`.
    fn body(&self, recs: &[SinkRecord]) -> Vec<u8> {
        match self.format {
            Format::Json => serde_json::to_vec(&Value::Array(
                recs.iter().map(SinkRecord::to_json).collect(),
            ))
            .unwrap_or_default(),
            Format::Ndjson => {
                let mut out = Vec::new();
                for r in recs {
                    out.extend(serde_json::to_vec(&r.to_json()).unwrap_or_default());
                    out.push(b'\n');
                }
                out
            }
            Format::CloudEvents => serde_json::to_vec(&Value::Array(
                recs.iter().map(|r| self.cloud_event(r)).collect(),
            ))
            .unwrap_or_default(),
            Format::CloudEventsBinary => recs
                .first()
                .map(|r| serde_json::to_vec(&json_value(r)).unwrap_or_default())
                .unwrap_or_default(),
        }
    }

    /// An OAuth2 access token: cached until 60 s before it expires.
    async fn oauth_token(&self, force: bool) -> Result<Zeroizing<String>> {
        let Auth::OAuth2 {
            scope, audience, ..
        } = &self.auth
        else {
            return Err(bad_option("no OAuth2 configuration"));
        };
        let mut cached = self.token.lock().await;
        if !force
            && let Some(t) = cached.as_ref()
            && Instant::now() < t.refresh_at
        {
            return Ok(t.value.clone());
        }
        let Some(ep) = &self.token_ep else {
            return Err(bad_option("no token_url"));
        };
        let conn = &self.ep.conn;
        let id = conn.secret("client_id")?;
        let secret = conn.secret("client_secret")?;
        let basic = Zeroizing::new(format!(
            "Basic {}",
            B64.encode(format!("{}:{}", form(&id), form(&secret)))
        ));
        let mut body = String::from("grant_type=client_credentials");
        if let Some(s) = scope {
            body.push_str("&scope=");
            body.push_str(&form(s));
        }
        if let Some(a) = audience {
            body.push_str("&audience=");
            body.push_str(&form(a));
        }
        let path = ep_path(&ep_url(&self.auth));
        let headers = vec![
            (
                "content-type".to_owned(),
                "application/x-www-form-urlencoded".to_owned(),
            ),
            ("accept".to_owned(), "application/json".to_owned()),
            ("authorization".to_owned(), (*basic).clone()),
        ];
        let (status, _, bytes) =
            tokio::time::timeout(self.timeout, ep.send(&path, &headers, Bytes::from(body)))
                .await
                .map_err(|_| delivery_failed("OAuth2 token endpoint: no answer in time"))??;
        if !status.is_success() {
            return Err(delivery_failed(format!(
                "OAuth2 token endpoint answered {}",
                status.as_u16()
            )));
        }
        let v: Value = serde_json::from_slice(&bytes)
            .map_err(|_| delivery_failed("OAuth2 token endpoint: the answer isn't JSON"))?;
        let token = v
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| delivery_failed("OAuth2 token endpoint: no access_token"))?;
        let expires = v.get("expires_in").and_then(Value::as_u64).unwrap_or(3600);
        let t = Token {
            value: Zeroizing::new(token.to_owned()),
            refresh_at: Instant::now() + Duration::from_secs(expires.saturating_sub(60)),
        };
        let out = t.value.clone();
        *cached = Some(t);
        Ok(out)
    }

    /// Authentication headers.
    async fn auth_headers(&self, force_refresh: bool) -> Result<Vec<(String, String)>> {
        let c = &self.ep.conn;
        Ok(match &self.auth {
            Auth::None => Vec::new(),
            Auth::Bearer => vec![(
                "authorization".into(),
                format!("Bearer {}", c.secret("token")?.as_str()),
            )],
            Auth::Basic => {
                let u = c.secret("username")?;
                let p = c.secret("password")?;
                vec![(
                    "authorization".into(),
                    format!(
                        "Basic {}",
                        B64.encode(format!("{}:{}", u.as_str(), p.as_str()))
                    ),
                )]
            }
            Auth::Header(name) => vec![(name.clone(), c.secret("header_value")?.to_string())],
            Auth::OAuth2 { .. } => vec![(
                "authorization".into(),
                format!("Bearer {}", self.oauth_token(force_refresh).await?.as_str()),
            )],
        })
    }

    /// Sends one request for `recs`; its outcome.
    async fn post(&self, recs: &[SinkRecord], idem: &str) -> Result<BatchOutcome> {
        let body = self.body(recs);
        let mut headers = vec![
            (
                "content-type".to_owned(),
                self.format.content_type().to_owned(),
            ),
            ("sprig-idempotency-key".to_owned(), idem.to_owned()),
            ("sprig-sink".to_owned(), self.sink_name.clone()),
            ("sprig-source".to_owned(), self.source_name.clone()),
            ("traceparent".to_owned(), traceparent()),
        ];
        if self.format == Format::CloudEventsBinary
            && let Some(r) = recs.first()
        {
            headers.push(("ce-specversion".into(), "1.0".into()));
            headers.push(("ce-id".into(), r.idem_key.clone()));
            headers.push(("ce-source".into(), self.ce_source.clone()));
            headers.push(("ce-type".into(), ce_type(r)));
            headers.push(("ce-time".into(), rfc3339(r.ts_ms)));
            if let Some(k) = r.key_text()
                && HeaderValue::from_str(&k).is_ok()
            {
                headers.push(("ce-subject".into(), k));
            }
        }
        match self.signature {
            Signature::None => {}
            Signature::Sprig => {
                let s = self.ep.conn.secret("signing_secret")?;
                headers.push((
                    "sprig-signature".into(),
                    sprig_signature(s.as_bytes(), unix_now(), &body),
                ));
            }
            Signature::Standard => {
                let s = self.ep.conn.secret("signing_secret")?;
                let ts = unix_now();
                headers.push(("webhook-id".into(), idem.to_owned()));
                headers.push(("webhook-timestamp".into(), ts.to_string()));
                headers.push((
                    "webhook-signature".into(),
                    standard_signature(&s, idem, ts, &body),
                ));
            }
        }
        let body = Bytes::from(body);
        let mut refreshed = false;
        loop {
            let mut h = headers.clone();
            h.extend(self.auth_headers(refreshed).await?);
            let sent =
                tokio::time::timeout(self.timeout, self.ep.send(&self.path, &h, body.clone()))
                    .await
                    .map_err(|_| {
                        delivery_failed(format!(
                            "{}: no answer within sink.http_timeout_ms = {} ms",
                            self.ep.authority,
                            self.timeout.as_millis()
                        ))
                    })??;
            let (status, rh, rbody) = sent;
            if status == StatusCode::UNAUTHORIZED
                && matches!(self.auth, Auth::OAuth2 { .. })
                && !refreshed
            {
                refreshed = true;
                continue;
            }
            return Ok(self.classify(status, &rh, &rbody, recs.len()));
        }
    }

    /// Chapter 51's classes, from the response.
    fn classify(
        &self,
        status: StatusCode,
        headers: &hyper::HeaderMap,
        body: &[u8],
        n: usize,
    ) -> BatchOutcome {
        let code = status.as_u16();
        let after = headers
            .get(hyper::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| retry_after(v, SystemTime::now()));
        if status.is_success() {
            if self.partial
                && (code == 200 || code == 207)
                && let Some(outs) = partial_results(body, n)
            {
                return BatchOutcome::Each(outs);
            }
            return BatchOutcome::All(Outcome::Ok);
        }
        let reason = format!("{}: HTTP {code}{}", self.ep.authority, snippet(body));
        match code {
            408 | 429 | 401 | 403 | 500..=599 => {
                BatchOutcome::All(Outcome::Retry { after, reason })
            }
            413 => BatchOutcome::TooLarge,
            300..=399 => BatchOutcome::All(Outcome::Fail {
                reason: format!("{reason} (redirects are not followed)"),
            }),
            _ => {
                if self.partial
                    && let Some(outs) = partial_results(body, n)
                {
                    return BatchOutcome::Each(outs);
                }
                BatchOutcome::All(Outcome::Fail { reason })
            }
        }
    }
}

/// Chapter 52's approval events through a webhook connection: one JSON
/// body per event, outside any sink. The connection keeps every rule a
/// sink's has (the destination checks, TLS and its CA, the authentication)
/// and signs the body with its `signing_secret`.
impl WebhookTarget {
    /// A target for single events on `conn`, named `name` in the
    /// `Sprig-Sink` header; `opts` are resolved `sink.*` options
    /// (`http_timeout_ms`, `http_connect_timeout_ms`, `http2`, `signature`).
    ///
    /// # Errors
    /// As [`build`].
    pub fn for_events(conn: Conn, opts: super::Opts, name: &str) -> Result<Self> {
        let sink = crate::model::SinkDef {
            id: crate::model::SinkId(0),
            db_id: 0,
            name: name.to_owned(),
            source: crate::model::SourceSpec::Stream {
                id: 0,
                name: "sprig_recommendations".into(),
                partitions: 1,
            },
            filter_sql: None,
            projection: None,
            transform: None,
            target: TargetSpec::Webhook { path: None },
            connections: Vec::new(),
            guarantee: crate::model::Guarantee::AtLeastOnce,
            dead_letter: None,
            start_at: crate::model::StartAt::Latest,
            owner_user_id: 0,
            state: crate::model::SinkState::Running,
            state_reason: None,
            definition_ver: 0,
            options: std::collections::BTreeMap::new(),
        };
        let ctx = TargetCtx {
            task: crate::runtime::TaskInfo {
                tenant: 0,
                sink: 0,
                task_no: 0,
                epoch: crate::model::TaskEpoch(0),
            },
            sink: std::sync::Arc::new(sink),
            conn: Some(conn),
            extra: Vec::new(),
            opts,
            db_name: String::new(),
        };
        Self::new(&ctx)
    }

    /// POSTs one JSON `body` with `Sprig-Event: event` and the
    /// idempotency key `idem`, signed and authenticated as the connection
    /// says; chapter 51's classes of the answer (an OAuth2 token is
    /// refreshed once on 401).
    ///
    /// # Errors
    /// A connection, TLS or destination-rule failure (39213, 39214, 39220).
    pub async fn post_event(&self, body: Bytes, idem: &str, event: &str) -> Result<Outcome> {
        let mut headers = vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("sprig-event".to_owned(), event.to_owned()),
            ("sprig-idempotency-key".to_owned(), idem.to_owned()),
            ("sprig-sink".to_owned(), self.sink_name.clone()),
            ("traceparent".to_owned(), traceparent()),
        ];
        match self.signature {
            Signature::None => {}
            Signature::Sprig => {
                let s = self.ep.conn.secret("signing_secret")?;
                headers.push((
                    "sprig-signature".into(),
                    sprig_signature(s.as_bytes(), unix_now(), &body),
                ));
            }
            Signature::Standard => {
                let s = self.ep.conn.secret("signing_secret")?;
                let ts = unix_now();
                headers.push(("webhook-id".into(), idem.to_owned()));
                headers.push(("webhook-timestamp".into(), ts.to_string()));
                headers.push((
                    "webhook-signature".into(),
                    standard_signature(&s, idem, ts, &body),
                ));
            }
        }
        let mut refreshed = false;
        loop {
            let mut h = headers.clone();
            h.extend(self.auth_headers(refreshed).await?);
            let (status, rh, rbody) =
                tokio::time::timeout(self.timeout, self.ep.send(&self.path, &h, body.clone()))
                    .await
                    .map_err(|_| {
                        delivery_failed(format!(
                            "{}: no answer within sink.http_timeout_ms = {} ms",
                            self.ep.authority,
                            self.timeout.as_millis()
                        ))
                    })??;
            if status == StatusCode::UNAUTHORIZED
                && matches!(self.auth, Auth::OAuth2 { .. })
                && !refreshed
            {
                refreshed = true;
                continue;
            }
            return Ok(match self.classify(status, &rh, &rbody, 1) {
                BatchOutcome::All(o) => o,
                BatchOutcome::Each(mut v) => v.pop().unwrap_or(Outcome::Ok),
                BatchOutcome::TooLarge => Outcome::Fail {
                    reason: format!("{}: HTTP 413", self.ep.authority),
                },
            });
        }
    }
}

impl WebhookTarget {
    /// `TEST CONNECTION` (R9, r9tests): an `OPTIONS` request to the
    /// connection's URL with its authentication, nothing posted. The
    /// answer's status; 401 and 403 after an OAuth2 refresh mean the
    /// credentials were refused.
    ///
    /// # Errors
    /// A connection, TLS or destination-rule failure (39213, 39214, 39220),
    /// or no answer within `sink.http_timeout_ms`.
    pub async fn probe(&self) -> Result<u16> {
        let mut refreshed = false;
        loop {
            let mut h = vec![("sprig-sink".to_owned(), self.sink_name.clone())];
            h.extend(self.auth_headers(refreshed).await?);
            let (status, _, _) = tokio::time::timeout(
                self.timeout,
                self.ep
                    .send_method(hyper::Method::OPTIONS, &self.path, &h, Bytes::new()),
            )
            .await
            .map_err(|_| {
                delivery_failed(format!(
                    "{}: no answer within sink.http_timeout_ms = {} ms",
                    self.ep.authority,
                    self.timeout.as_millis()
                ))
            })??;
            if status == StatusCode::UNAUTHORIZED
                && matches!(self.auth, Auth::OAuth2 { .. })
                && !refreshed
            {
                refreshed = true;
                continue;
            }
            return Ok(status.as_u16());
        }
    }
}

fn ep_url(a: &Auth) -> String {
    match a {
        Auth::OAuth2 { token_url, .. } => token_url.clone(),
        _ => String::new(),
    }
}

fn ep_path(url: &str) -> String {
    url.parse::<Uri>()
        .ok()
        .and_then(|u| u.path_and_query().map(|p| p.as_str().to_owned()))
        .unwrap_or_else(|| "/".into())
}

/// The first 200 characters of a response body, for reasons.
fn snippet(body: &[u8]) -> String {
    if body.is_empty() {
        return String::new();
    }
    let s = String::from_utf8_lossy(body);
    let t: String = s.chars().take(200).collect();
    format!(": {}", t.trim())
}

/// `{"results": [{"status": "ok" | "retry" | "fail", "reason": ...}]}`,
/// one per record in order; `None` when the body isn't that.
#[must_use]
pub fn partial_results(body: &[u8], n: usize) -> Option<Vec<Outcome>> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let items = v.get("results")?.as_array()?;
    let mut out = Vec::with_capacity(n);
    for it in items.iter().take(n) {
        let reason = it
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        out.push(match it.get("status").and_then(Value::as_str)? {
            "ok" => Outcome::Ok,
            "retry" => Outcome::Retry {
                after: None,
                reason: if reason.is_empty() {
                    "the destination asked to retry the record".into()
                } else {
                    reason
                },
            },
            "fail" => Outcome::Fail {
                reason: if reason.is_empty() {
                    "the destination refused the record".into()
                } else {
                    reason
                },
            },
            _ => return None,
        });
    }
    Some(out)
}

#[async_trait]
impl Target for WebhookTarget {
    fn kind(&self) -> &'static str {
        "webhook"
    }

    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome> {
        if self.batch {
            return self.post(&b.records, &b.id).await;
        }
        // One request per record. A destination-wide failure before any
        // record went is the batch's; after some went, the rest retry.
        let mut outs: Vec<Outcome> = Vec::with_capacity(b.records.len());
        for (i, r) in b.records.iter().enumerate() {
            let res = self.post(std::slice::from_ref(r), &r.idem_key).await;
            let wide = match res {
                Ok(BatchOutcome::All(Outcome::Ok)) => {
                    outs.push(Outcome::Ok);
                    continue;
                }
                Ok(BatchOutcome::Each(mut v)) => {
                    outs.push(v.pop().unwrap_or(Outcome::Ok));
                    continue;
                }
                Ok(BatchOutcome::All(Outcome::Fail { reason })) => {
                    outs.push(Outcome::Fail { reason });
                    continue;
                }
                Ok(BatchOutcome::TooLarge) => {
                    outs.push(Outcome::Fail {
                        reason: "ER_SPRIG_SINK_RECORD_TOO_LARGE: HTTP 413 for a single record"
                            .into(),
                    });
                    continue;
                }
                Ok(BatchOutcome::All(Outcome::Retry { after, reason })) => (after, reason),
                Err(e) => {
                    if i == 0 {
                        return Err(e);
                    }
                    (crate::runtime::hint_of(&e.message), e.message)
                }
            };
            if i == 0 {
                return Ok(BatchOutcome::All(Outcome::Retry {
                    after: wide.0,
                    reason: wide.1,
                }));
            }
            for _ in i..b.records.len() {
                outs.push(Outcome::Retry {
                    after: wide.0,
                    reason: wide.1.clone(),
                });
            }
            break;
        }
        Ok(BatchOutcome::Each(outs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_webhooks_test_vector() {
        // The Standard Webhooks specification's example (the secret is the
        // one chapter 51's examples use).
        let sig = standard_signature(
            "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            1_614_265_330,
            br#"{"test": 2432232314}"#,
        );
        assert_eq!(sig, "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=");
    }

    #[test]
    fn sprig_signatures() {
        let s = sprig_signature(b"secret", 1_700_000_000, b"{}");
        assert!(s.starts_with("t=1700000000,v1="), "{s}");
        assert_eq!(s.len(), "t=1700000000,v1=".len() + 64);
        assert_ne!(s, sprig_signature(b"other", 1_700_000_000, b"{}"));
    }

    #[test]
    fn retry_after_forms() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        assert_eq!(retry_after("7", now), Some(Duration::from_secs(7)));
        let later = httpdate::fmt_http_date(now + Duration::from_secs(30));
        assert_eq!(retry_after(&later, now), Some(Duration::from_secs(30)));
        let earlier = httpdate::fmt_http_date(now - Duration::from_secs(30));
        assert_eq!(retry_after(&earlier, now), Some(Duration::ZERO));
        assert_eq!(retry_after("soon", now), None);
    }

    #[test]
    fn partial_result_bodies() {
        let b =
            br#"{"results":[{"status":"ok"},{"status":"retry"},{"status":"fail","reason":"bad"}]}"#;
        let v = partial_results(b, 3).unwrap();
        assert_eq!(v[0], Outcome::Ok);
        assert!(matches!(v[1], Outcome::Retry { .. }));
        assert_eq!(
            v[2],
            Outcome::Fail {
                reason: "bad".into()
            }
        );
        assert!(partial_results(b"{}", 1).is_none());
        assert!(partial_results(br#"{"results":[{"status":"maybe"}]}"#, 1).is_none());
        assert_eq!(form("a b&c=d/é"), "a+b%26c%3Dd%2F%C3%A9");
        let t = traceparent();
        assert_eq!(t.len(), 55);
        assert!(t.starts_with("00-") && t.ends_with("-01"));
    }
}
