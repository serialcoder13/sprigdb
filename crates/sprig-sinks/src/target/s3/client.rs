//! A small S3 client over hyper (HTTP/1.1, TLS with rustls), signing with
//! [`super::sigv4`]: the eight calls the target needs, plus `GetObject`,
//! `DeleteObject` and `CreateBucket` for recovery and tests.
//!
//! Every connection goes to the address the destination rules checked
//! ([`crate::policy::Destinations::resolve`]); TLS verifies the name.
//! Connections are kept and reused.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::io::{AsyncRead, AsyncWrite};
use zeroize::Zeroizing;

use super::sigv4::{self, Creds, Signing, sha256_hex, uri_encode};
use crate::model::{ConnKind, bad_option, delivery_failed, err};
use crate::policy::{Destinations, url_host_port};
use crate::target::{Conn, Opts};

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// One answer.
#[derive(Debug, Clone)]
pub struct Resp {
    /// The status.
    pub status: u16,
    /// Headers, lower-case names.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Bytes,
}

impl Resp {
    /// A header's value.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The `ETag`, quotes kept.
    #[must_use]
    pub fn etag(&self) -> String {
        self.header("etag").unwrap_or_default().to_owned()
    }

    fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// An object listing entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// The key.
    pub key: String,
    /// Its size.
    pub size: u64,
}

/// A multipart upload in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    /// The key.
    pub key: String,
    /// Its id.
    pub upload_id: String,
    /// When it started, ms since the epoch (0 when unreadable).
    pub initiated_ms: i64,
}

/// What a conditional write found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Put {
    /// Written, with its `ETag`.
    Written(String),
    /// The object existed (412 under `If-None-Match: *`).
    Existed,
}

/// The client.
pub struct S3Client {
    tls: Option<Arc<rustls::ClientConfig>>,
    host: String,
    port: u16,
    region: String,
    bucket: String,
    path_style: bool,
    creds: Creds,
    dest: Arc<Destinations>,
    options: serde_json::Map<String, serde_json::Value>,
    connect_timeout: Duration,
    timeout: Duration,
    pool: Mutex<Vec<SendRequest<Full<Bytes>>>>,
}

impl std::fmt::Debug for S3Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Client")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("bucket", &self.bucket)
            .field("path_style", &self.path_style)
            .finish_non_exhaustive()
    }
}

/// An S3-level failure: destination-wide (the runtime retries and, after
/// `sink.pause_after_minutes`, pauses), with the destination's hint.
fn failed(op: &str, r: &Resp) -> SprigError {
    let body = String::from_utf8_lossy(&r.body);
    let code = xml_first(&body, "Code").unwrap_or_default();
    let msg = xml_first(&body, "Message").unwrap_or_default();
    let mut text = format!("S3 {op}: HTTP {} {code} {msg}", r.status);
    if let Some(ms) = r.header("retry-after").and_then(retry_after_ms) {
        text.push_str(&format!(" retry_after_ms={ms}"));
    } else if code == "SlowDown" || r.status == 503 {
        text.push_str(" retry_after_ms=1000");
    }
    delivery_failed(text.trim_end().to_owned())
}

/// `Retry-After` in ms: seconds or an HTTP date.
#[must_use]
pub fn retry_after_ms(v: &str) -> Option<u64> {
    if let Ok(s) = v.trim().parse::<u64>() {
        return Some(s.saturating_mul(1000));
    }
    let t = httpdate::parse_http_date(v.trim()).ok()?;
    let d = t
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    Some(u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The text of every `<tag>...</tag>`, unescaped.
#[must_use]
pub fn xml_all(body: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else {
            break;
        };
        out.push(unescape(&after[..j]));
        rest = &after[j + close.len()..];
    }
    out
}

/// The first `<tag>...</tag>`.
#[must_use]
pub fn xml_first(body: &str, tag: &str) -> Option<String> {
    xml_all(body, tag).into_iter().next()
}

fn unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#34;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// ISO 8601 UTC (`2026-11-09T10:00:00.000Z`) to ms since the epoch.
#[must_use]
pub fn iso_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hh, mi, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let ms = if b.get(19) == Some(&b'.') {
        let frac: String = s[20..].chars().take_while(char::is_ascii_digit).collect();
        let frac = format!("{frac:0<3}");
        frac[..3].parse::<i64>().ok()?
    } else {
        0
    };
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + hh * 3600 + mi * 60 + ss) * 1000) + ms)
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

impl S3Client {
    /// A client for `conn` (options `endpoint`, `region`, `bucket`,
    /// `path_style`, `credentials`; credentials `access_key_id`,
    /// `secret_access_key`, `session_token`).
    ///
    /// # Errors
    /// 39218 for bad options; 39216 for missing credentials; 39228 for a
    /// platform identity (`assume_role`, `web_identity`, `instance`), which
    /// this build doesn't open yet.
    pub fn new(conn: &Conn, opts: &Opts) -> Result<Self> {
        match conn.opt("credentials").as_deref().unwrap_or("static") {
            "static" => {}
            other @ ("assume_role" | "web_identity" | "instance") => {
                return Err(err(
                    SprigCode::SinkPlatformIdentityDenied,
                    format!(
                        "credentials = {other} needs the worker's own identity, which this build doesn't use yet (static keys only)"
                    ),
                ));
            }
            other => {
                return Err(bad_option(format!(
                    "credentials must be static, assume_role, web_identity or instance, not {other}"
                )));
            }
        }
        let region = conn.opt("region").unwrap_or_else(|| "us-east-1".into());
        let bucket = conn.need("bucket")?;
        let endpoint = conn
            .opt("endpoint")
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let (host, port, tls) = url_host_port(&endpoint)?;
        let aws = host.ends_with(".amazonaws.com");
        let path_style = conn.opt_bool("path_style", !aws);
        let creds = Creds {
            access_key_id: conn.secret("access_key_id")?.as_str().to_owned(),
            secret: conn.secret("secret_access_key")?,
            session_token: conn.secrets.get("session_token"),
        };
        let tls = if tls {
            Some(crate::target::tls_config(conn, &[b"http/1.1"])?)
        } else {
            None
        };
        Ok(Self {
            tls,
            host,
            port,
            region,
            bucket,
            path_style,
            creds,
            dest: Arc::clone(&conn.dest),
            options: conn.options.clone(),
            connect_timeout: Duration::from_millis(opts.u64("http_connect_timeout_ms", 5000)?),
            timeout: Duration::from_millis(opts.u64("http_timeout_ms", 30_000)?),
            pool: Mutex::new(Vec::new()),
        })
    }

    /// The bucket.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    fn net_host(&self) -> String {
        if self.path_style {
            self.host.clone()
        } else {
            format!("{}.{}", self.bucket, self.host)
        }
    }

    fn host_header(&self) -> String {
        let default = if self.tls.is_some() { 443 } else { 80 };
        let h = self.net_host();
        if self.port == default {
            h
        } else {
            format!("{h}:{}", self.port)
        }
    }

    fn path(&self, key: Option<&str>) -> String {
        let k = key.map(|k| uri_encode(k, true)).unwrap_or_default();
        if self.path_style {
            if key.is_some() {
                format!("/{}/{k}", uri_encode(&self.bucket, false))
            } else {
                format!("/{}", uri_encode(&self.bucket, false))
            }
        } else {
            format!("/{k}")
        }
    }

    async fn connect(&self) -> Result<SendRequest<Full<Bytes>>> {
        let host = self.net_host();
        let addr = self
            .dest
            .resolve(&host, self.port, Some(ConnKind::S3), Some(&self.options))
            .await?;
        let tcp = tokio::time::timeout(self.connect_timeout, tokio::net::TcpStream::connect(addr))
            .await
            .map_err(|_| delivery_failed(format!("connect to {host}:{} timed out", self.port)))?
            .map_err(|e| delivery_failed(format!("connect to {host}:{}: {e}", self.port)))?;
        let _ = tcp.set_nodelay(true);
        let io: Box<dyn Io> = match &self.tls {
            Some(cfg) => {
                let name = rustls::pki_types::ServerName::try_from(host.clone())
                    .map_err(|e| bad_option(format!("bad host {host}: {e}")))?;
                let s = tokio_rustls::TlsConnector::from(Arc::clone(cfg))
                    .connect(name, tcp)
                    .await
                    .map_err(|e| delivery_failed(format!("TLS with {host}: {e}")))?;
                Box::new(s)
            }
            None => Box::new(tcp),
        };
        let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .map_err(|e| delivery_failed(format!("HTTP with {host}: {e}")))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(sender)
    }

    /// One signed request.
    ///
    /// # Errors
    /// 39221 (retryable) when the connection fails or times out; the
    /// answer otherwise, whatever its status.
    #[allow(clippy::too_many_lines)] // signing, pooling, one retry
    pub async fn request(
        &self,
        method: &str,
        key: Option<&str>,
        query: &[(String, String)],
        extra: &[(String, String)],
        body: Bytes,
    ) -> Result<Resp> {
        let path = self.path(key);
        let payload = sha256_hex(&body);
        let date = sigv4::amz_date(now_ms());
        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), self.host_header()),
            ("x-amz-content-sha256".into(), payload.clone()),
            ("x-amz-date".into(), date.clone()),
        ];
        if let Some(t) = &self.creds.session_token {
            headers.push(("x-amz-security-token".into(), t.as_str().to_owned()));
        }
        if !body.is_empty() || matches!(method, "PUT" | "POST") {
            headers.push(("content-length".into(), body.len().to_string()));
        }
        headers.extend(extra.iter().cloned());
        let auth = Zeroizing::new(sigv4::authorization(
            &Signing {
                method,
                path: &path,
                query,
                headers: &headers,
                payload_hash: &payload,
                region: &self.region,
                service: "s3",
                amz_date: &date,
            },
            &self.creds,
        ));
        let q = sigv4::canonical_query(query);
        let uri = if q.is_empty() {
            path
        } else {
            format!("{path}?{q}")
        };
        let build = || {
            let mut b = hyper::Request::builder().method(method).uri(uri.as_str());
            for (k, v) in &headers {
                b = b.header(k.as_str(), v.as_str());
            }
            b.header("authorization", auth.as_str())
                .body(Full::new(body.clone()))
                .map_err(|e| SprigError::internal(format!("S3 request: {e}")))
        };
        let pooled = {
            let mut p = self
                .pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut found = None;
            while let Some(s) = p.pop() {
                if !s.is_closed() {
                    found = Some(s);
                    break;
                }
            }
            found
        };
        // A kept connection the server closed meanwhile: once more on a
        // fresh one (every call here is safe to repeat).
        let mut attempt = 0;
        let mut sender = pooled;
        loop {
            let fresh = sender.is_none();
            let mut s = match sender.take() {
                Some(s) => s,
                None => self.connect().await?,
            };
            let r = tokio::time::timeout(self.timeout, async {
                s.ready().await?;
                let resp = s.send_request(build()?).await?;
                let status = resp.status().as_u16();
                let headers: Vec<(String, String)> = resp
                    .headers()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_ascii_lowercase(),
                            String::from_utf8_lossy(v.as_bytes()).into_owned(),
                        )
                    })
                    .collect();
                let body = resp.into_body().collect().await?.to_bytes();
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Resp {
                    status,
                    headers,
                    body,
                })
            })
            .await;
            match r {
                Ok(Ok(resp)) => {
                    let close = resp
                        .header("connection")
                        .is_some_and(|c| c.eq_ignore_ascii_case("close"));
                    if !close {
                        self.pool
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(s);
                    }
                    return Ok(resp);
                }
                Ok(Err(e)) if !fresh && attempt == 0 => {
                    tracing::debug!(error = %e, "kept S3 connection failed; reconnecting");
                    attempt += 1;
                }
                Ok(Err(e)) => {
                    return Err(delivery_failed(format!(
                        "S3 {method} {}: {e}",
                        key.unwrap_or("/")
                    )));
                }
                Err(_) => {
                    return Err(delivery_failed(format!(
                        "S3 {method} {}: no answer within {} ms",
                        key.unwrap_or("/"),
                        self.timeout.as_millis()
                    )));
                }
            }
        }
    }

    /// `PutObject`, `If-None-Match: *` when `only_new`.
    ///
    /// # Errors
    /// See [`S3Client::request`]; any other status is 39221.
    pub async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
        only_new: bool,
    ) -> Result<Put> {
        let mut h = vec![("content-type".to_owned(), content_type.to_owned())];
        if only_new {
            h.push(("if-none-match".into(), "*".into()));
        }
        let r = self.request("PUT", Some(key), &[], &h, body).await?;
        if only_new && r.status == 412 {
            return Ok(Put::Existed);
        }
        if !r.ok() {
            return Err(failed("PutObject", &r));
        }
        Ok(Put::Written(r.etag()))
    }

    /// `CreateMultipartUpload`: the upload id.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn create_multipart(&self, key: &str, content_type: &str) -> Result<String> {
        let r = self
            .request(
                "POST",
                Some(key),
                &[("uploads".into(), String::new())],
                &[("content-type".into(), content_type.to_owned())],
                Bytes::new(),
            )
            .await?;
        if !r.ok() {
            return Err(failed("CreateMultipartUpload", &r));
        }
        xml_first(&String::from_utf8_lossy(&r.body), "UploadId")
            .ok_or_else(|| delivery_failed("S3 CreateMultipartUpload: no UploadId"))
    }

    /// `UploadPart`: the part's `ETag`.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part: u32,
        body: Bytes,
    ) -> Result<String> {
        let r = self
            .request(
                "PUT",
                Some(key),
                &[
                    ("partNumber".into(), part.to_string()),
                    ("uploadId".into(), upload_id.to_owned()),
                ],
                &[],
                body,
            )
            .await?;
        if !r.ok() {
            return Err(failed("UploadPart", &r));
        }
        Ok(r.etag())
    }

    /// `CompleteMultipartUpload` (`If-None-Match: *` when `only_new`).
    /// S3 can answer 200 with an `<Error>` body; that is a failure.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[String],
        only_new: bool,
    ) -> Result<Put> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (i, e) in parts.iter().enumerate() {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                i + 1,
                escape(e)
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let mut h = vec![("content-type".to_owned(), "application/xml".to_owned())];
        if only_new {
            h.push(("if-none-match".into(), "*".into()));
        }
        let r = self
            .request(
                "POST",
                Some(key),
                &[("uploadId".into(), upload_id.to_owned())],
                &h,
                Bytes::from(xml),
            )
            .await?;
        if only_new && r.status == 412 {
            return Ok(Put::Existed);
        }
        let text = String::from_utf8_lossy(&r.body);
        if !r.ok() || text.contains("<Error>") {
            let mut r = r.clone();
            if r.ok() {
                r.status = 500;
            }
            return Err(failed("CompleteMultipartUpload", &r));
        }
        Ok(Put::Written(
            xml_first(&text, "ETag").unwrap_or_else(|| r.etag()),
        ))
    }

    /// `AbortMultipartUpload`.
    ///
    /// # Errors
    /// As [`S3Client::put_object`] (404: already gone, fine).
    pub async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        let r = self
            .request(
                "DELETE",
                Some(key),
                &[("uploadId".into(), upload_id.to_owned())],
                &[],
                Bytes::new(),
            )
            .await?;
        if !r.ok() && r.status != 404 {
            return Err(failed("AbortMultipartUpload", &r));
        }
        Ok(())
    }

    /// `HeadObject`: the size, or `None` when it doesn't exist.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn head_object(&self, key: &str) -> Result<Option<u64>> {
        let r = self
            .request("HEAD", Some(key), &[], &[], Bytes::new())
            .await?;
        if r.status == 404 {
            return Ok(None);
        }
        if !r.ok() {
            return Err(failed("HeadObject", &r));
        }
        Ok(Some(
            r.header("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        ))
    }

    /// `GetObject`, or `None` when it doesn't exist.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn get_object(&self, key: &str) -> Result<Option<Bytes>> {
        let r = self
            .request("GET", Some(key), &[], &[], Bytes::new())
            .await?;
        if r.status == 404 {
            return Ok(None);
        }
        if !r.ok() {
            return Err(failed("GetObject", &r));
        }
        Ok(Some(r.body))
    }

    /// `DeleteObject`.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        let r = self
            .request("DELETE", Some(key), &[], &[], Bytes::new())
            .await?;
        if !r.ok() && r.status != 404 {
            return Err(failed("DeleteObject", &r));
        }
        Ok(())
    }

    /// `ListObjectsV2` under `prefix`, every page.
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<Listed>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type".to_owned(), "2".to_owned()),
                ("prefix".to_owned(), prefix.to_owned()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".into(), t.clone()));
            }
            let r = self.request("GET", None, &q, &[], Bytes::new()).await?;
            if !r.ok() {
                return Err(failed("ListObjectsV2", &r));
            }
            let text = String::from_utf8_lossy(&r.body);
            for c in xml_all(&text, "Contents") {
                if let Some(key) = xml_first(&c, "Key") {
                    let size = xml_first(&c, "Size")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    out.push(Listed { key, size });
                }
            }
            if xml_first(&text, "IsTruncated").as_deref() == Some("true") {
                token = xml_first(&text, "NextContinuationToken");
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(out)
    }

    /// `ListMultipartUploads` under `prefix` (first 1,000).
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn list_uploads(&self, prefix: &str) -> Result<Vec<Upload>> {
        let q = vec![
            ("uploads".to_owned(), String::new()),
            ("prefix".to_owned(), prefix.to_owned()),
        ];
        let r = self.request("GET", None, &q, &[], Bytes::new()).await?;
        if !r.ok() {
            return Err(failed("ListMultipartUploads", &r));
        }
        let text = String::from_utf8_lossy(&r.body);
        Ok(xml_all(&text, "Upload")
            .into_iter()
            .filter_map(|u| {
                Some(Upload {
                    key: xml_first(&u, "Key")?,
                    upload_id: xml_first(&u, "UploadId")?,
                    initiated_ms: xml_first(&u, "Initiated")
                        .and_then(|t| iso_ms(&t))
                        .unwrap_or(0),
                })
            })
            .collect())
    }

    /// `CreateBucket` (tests and setup; an existing bucket of ours is fine).
    ///
    /// # Errors
    /// As [`S3Client::put_object`].
    pub async fn create_bucket(&self) -> Result<()> {
        let r = self.request("PUT", None, &[], &[], Bytes::new()).await?;
        let text = String::from_utf8_lossy(&r.body);
        if r.ok() || text.contains("BucketAlreadyOwnedByYou") {
            return Ok(());
        }
        Err(failed("CreateBucket", &r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_and_times() {
        let b = "<R><Contents><Key>a&amp;b</Key><Size>3</Size></Contents><Contents><Key>c</Key></Contents><IsTruncated>false</IsTruncated></R>";
        let c = xml_all(b, "Contents");
        assert_eq!(c.len(), 2);
        assert_eq!(xml_first(&c[0], "Key").as_deref(), Some("a&b"));
        assert_eq!(xml_first(b, "Nope"), None);
        assert_eq!(iso_ms("2015-08-30T12:36:00.000Z"), Some(1_440_938_160_000));
        assert_eq!(iso_ms("1970-01-01T00:00:01Z"), Some(1000));
        assert_eq!(iso_ms("2024-02-29T23:59:59.5Z"), Some(1_709_251_199_500));
        assert_eq!(retry_after_ms("3"), Some(3000));
        assert!(retry_after_ms("Wed, 21 Oct 2015 07:28:00 GMT").is_some());
        assert_eq!(retry_after_ms("soon"), None);
    }
}
