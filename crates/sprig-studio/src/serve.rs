//! Serving one request under `studio.path`.
//!
//! ```text
//! serve(request, listener):
//!  1. not studio.enabled, or listener not in studio.listeners → 404 (39920)
//!  2. rel = path after studio.path; ".." or encoded dot segments → 404
//!  3. a = store.get(rel); none and no file extension → index.html; none → 404
//!  4. brotli if accepted, else gzip, else identity; Vary: Accept-Encoding; ETag → 304
//!  5. every response: Content-Security-Policy, X-Content-Type-Options: nosniff,
//!     Referrer-Policy: no-referrer, Cross-Origin-Opener-Policy: same-origin,
//!     Cross-Origin-Resource-Policy: same-origin, X-Frame-Options: DENY,
//!     Permissions-Policy denying camera, microphone, geolocation and payment;
//!     HSTS on 8443
//!  6. hashed assets: max-age one year, immutable; index.html, version.json: no-store
//! ```
//!
//! Also `GET <path>/version.json` (the build version and the API version
//! it needs) and `POST <path>/_/errors`, client error reports: kind,
//! message, stack and route, logged and counted, never data values. A
//! binary built without `studio/dist` answers 503 with a page saying so
//! (39932).

use bytes::Bytes;
use serde_json::{Value, json};
use sprig_common::SprigCode;

use crate::assets::{Asset, AssetStore};
use crate::settings::StudioConfig;

/// The API version this Studio build needs (`version.json`'s `min_api`).
pub const MIN_API: &str = "v1";

/// Largest client error report read.
pub const MAX_ERROR_REPORT: usize = 16 * 1024;

/// One request, as far as serving needs it.
#[derive(Debug, Clone, Default)]
pub struct StudioRequest<'a> {
    /// `GET`, `HEAD` or `POST`.
    pub method: &'a str,
    /// The path as sent (percent-encoded), without the query.
    pub path: &'a str,
    /// The listener's name (`http`, `https`).
    pub listener: &'a str,
    /// The request arrived over TLS.
    pub tls: bool,
    /// `Accept-Encoding`.
    pub accept_encoding: Option<&'a str>,
    /// `If-None-Match`.
    pub if_none_match: Option<&'a str>,
    /// The body (`POST _/errors` only).
    pub body: &'a [u8],
}

/// The answer: status, headers and body.
#[derive(Debug, Clone)]
pub struct StudioResponse {
    /// HTTP status.
    pub status: u16,
    /// Headers, in order.
    pub headers: Vec<(&'static str, String)>,
    /// The body; empty for `HEAD` and 304.
    pub body: Bytes,
}

impl StudioResponse {
    /// The first value of `name` (lower case).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Whether `path` is Studio's (`studio.path` itself or below it).
#[must_use]
pub fn is_studio_path(cfg: &StudioConfig, path: &str) -> bool {
    path == cfg.path
        || path
            .strip_prefix(cfg.path.as_str())
            .is_some_and(|r| r.starts_with('/'))
}

fn security_headers(cfg: &StudioConfig, tls: bool) -> Vec<(&'static str, String)> {
    let mut h = vec![
        ("content-security-policy", cfg.csp.clone()),
        ("x-content-type-options", "nosniff".to_owned()),
        ("referrer-policy", "no-referrer".to_owned()),
        ("cross-origin-opener-policy", "same-origin".to_owned()),
        ("cross-origin-resource-policy", "same-origin".to_owned()),
        ("x-frame-options", "DENY".to_owned()),
        (
            "permissions-policy",
            "camera=(), microphone=(), geolocation=(), payment=()".to_owned(),
        ),
    ];
    if tls {
        h.push((
            "strict-transport-security",
            "max-age=31536000; includeSubDomains".to_owned(),
        ));
    }
    h
}

fn error(cfg: &StudioConfig, tls: bool, status: u16, code: SprigCode, msg: &str) -> StudioResponse {
    let body = json!({
        "code": code.number(),
        "name": code.name(),
        "message": msg,
        "sqlstate": "HY000",
    });
    let mut headers = security_headers(cfg, tls);
    headers.push(("content-type", "application/json".to_owned()));
    headers.push(("cache-control", "no-store".to_owned()));
    StudioResponse {
        status,
        headers,
        body: Bytes::from(body.to_string()),
    }
}

/// The page a binary without a Studio build serves (39932).
const NOT_BUILT_PAGE: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<title>SprigDB Studio</title></head><body><main><h1>Studio is not in this build</h1>\
<p>This gateway was built without <code>studio/dist</code> (error 39932, \
ER_SPRIG_STUDIO_NOT_BUILT). Build it with <code>pnpm build</code> in <code>studio/</code> \
and rebuild the binary.</p></main></body></html>";

/// Decodes `%XX` escapes; `None` for a malformed escape or bytes that
/// aren't UTF-8.
fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The file path below `studio.path`, or `None` for anything that could
/// leave it: `..` or `.` segments (also percent-encoded), backslashes, NUL.
fn relative(cfg: &StudioConfig, path: &str) -> Option<String> {
    let rest = path.strip_prefix(cfg.path.as_str())?;
    let decoded = percent_decode(rest)?;
    if decoded.contains('\\') || decoded.contains('\0') {
        return None;
    }
    let mut parts = Vec::new();
    for seg in decoded.split('/') {
        match seg {
            "" => {}
            "." | ".." => return None,
            s => parts.push(s),
        }
    }
    Some(parts.join("/"))
}

fn accepts(accept_encoding: Option<&str>, coding: &str) -> bool {
    accept_encoding.is_some_and(|h| {
        h.split(',').any(|part| {
            let mut it = part.split(';');
            let name = it.next().unwrap_or("").trim();
            let q_zero = it.any(|p| {
                let p = p.trim();
                p.strip_prefix("q=")
                    .is_some_and(|q| q.trim().parse::<f32>().is_ok_and(|q| q <= 0.0))
            });
            name.eq_ignore_ascii_case(coding) && !q_zero
        })
    })
}

fn etag_matches(if_none_match: Option<&str>, etag: &str) -> bool {
    if_none_match.is_some_and(|h| {
        h.split(',').any(|t| {
            let t = t.trim();
            t == "*" || t == etag || t.strip_prefix("W/") == Some(etag)
        })
    })
}

fn asset_response(
    cfg: &StudioConfig,
    req: &StudioRequest<'_>,
    a: &Asset,
    no_store: bool,
) -> StudioResponse {
    let mut headers = security_headers(cfg, req.tls);
    headers.push(("content-type", a.content_type.to_owned()));
    headers.push(("vary", "Accept-Encoding".to_owned()));
    headers.push(("etag", a.etag.to_owned()));
    headers.push((
        "cache-control",
        if no_store {
            "no-store".to_owned()
        } else if a.immutable {
            "public, max-age=31536000, immutable".to_owned()
        } else {
            "no-cache".to_owned()
        },
    ));
    if !no_store && etag_matches(req.if_none_match, a.etag) {
        count(304, "none", 0);
        return StudioResponse {
            status: 304,
            headers,
            body: Bytes::new(),
        };
    }
    let (body, encoding) = match (a.brotli, a.gzip) {
        (Some(b), _) if accepts(req.accept_encoding, "br") => (b, "br"),
        (_, Some(g)) if accepts(req.accept_encoding, "gzip") => (g, "gzip"),
        _ => (a.identity, "identity"),
    };
    if encoding != "identity" {
        headers.push(("content-encoding", encoding.to_owned()));
    }
    headers.push(("content-length", body.len().to_string()));
    count(200, encoding, body.len());
    StudioResponse {
        status: 200,
        headers,
        body: if req.method == "HEAD" {
            Bytes::new()
        } else {
            Bytes::from_static(body)
        },
    }
}

fn count(status: u16, encoding: &'static str, bytes: usize) {
    metrics::counter!("sprig_studio_asset_requests_total",
        "status" => status.to_string(), "encoding" => encoding)
    .increment(1);
    metrics::counter!("sprig_studio_asset_bytes_total").increment(bytes as u64);
}

fn client_error(cfg: &StudioConfig, req: &StudioRequest<'_>) -> StudioResponse {
    if req.body.len() > MAX_ERROR_REPORT {
        return error(
            cfg,
            req.tls,
            413,
            SprigCode::RequestTooLarge,
            "an error report is at most 16 KiB",
        );
    }
    let v: Value = serde_json::from_slice(req.body).unwrap_or(Value::Null);
    let kind = match v.get("kind").and_then(Value::as_str) {
        Some("chunk_load") => "chunk_load",
        Some("api") => "api",
        _ => "js",
    };
    let text = |k: &str, max: usize| -> String {
        v.get(k)
            .and_then(Value::as_str)
            .unwrap_or("")
            .chars()
            .take(max)
            .collect()
    };
    metrics::counter!("sprig_studio_client_errors_total", "kind" => kind).increment(1);
    tracing::warn!(
        target: "sprig_studio",
        kind,
        message = %text("message", 500),
        route = %text("route", 200),
        trace_id = %text("trace_id", 64),
        stack = %text("stack", 4000),
        "Studio client error"
    );
    let mut headers = security_headers(cfg, req.tls);
    headers.push(("cache-control", "no-store".to_owned()));
    StudioResponse {
        status: 204,
        headers,
        body: Bytes::new(),
    }
}

/// Answers one request under `studio.path` from `store`.
#[must_use]
pub fn serve(store: &AssetStore, cfg: &StudioConfig, req: &StudioRequest<'_>) -> StudioResponse {
    // 1. Off, or not on this listener.
    if !cfg.enabled || !cfg.on_listener(req.listener) {
        return error(
            cfg,
            req.tls,
            404,
            SprigCode::StudioDisabled,
            "Studio is not served here",
        );
    }
    // `/studio` → `/studio/`, so relative URLs resolve.
    if req.path == cfg.path {
        let mut headers = security_headers(cfg, req.tls);
        headers.push(("location", format!("{}/", cfg.path)));
        headers.push(("cache-control", "no-store".to_owned()));
        return StudioResponse {
            status: 308,
            headers,
            body: Bytes::new(),
        };
    }
    // 2. The file path; anything leaving the mount is not found.
    let Some(rel) = relative(cfg, req.path) else {
        return error(
            cfg,
            req.tls,
            404,
            SprigCode::AdminRequestInvalid,
            "no such file",
        );
    };
    if rel == "_/errors" {
        return if req.method == "POST" {
            client_error(cfg, req)
        } else {
            error(
                cfg,
                req.tls,
                405,
                SprigCode::AdminRequestInvalid,
                "POST error reports",
            )
        };
    }
    if !matches!(req.method, "GET" | "HEAD") {
        return error(
            cfg,
            req.tls,
            405,
            SprigCode::AdminRequestInvalid,
            "Studio's files are read with GET or HEAD",
        );
    }
    if rel == "version.json" {
        let body = json!({
            "build_version": store.build_version(),
            "min_api": MIN_API,
            "api": "v1",
            "built": store.is_built(),
            // Studio refuses a plain-HTTP login from another host before
            // sending the password unless this is on (the gateway refuses
            // too: 39921).
            "allow_insecure_http": cfg.allow_insecure_http,
        });
        let mut headers = security_headers(cfg, req.tls);
        headers.push(("content-type", "application/json".to_owned()));
        headers.push(("cache-control", "no-store".to_owned()));
        return StudioResponse {
            status: 200,
            headers,
            body: Bytes::from(body.to_string()),
        };
    }
    if !store.is_built() {
        let mut headers = security_headers(cfg, req.tls);
        headers.push(("content-type", "text/html; charset=utf-8".to_owned()));
        headers.push(("cache-control", "no-store".to_owned()));
        let code = SprigCode::StudioNotBuilt;
        headers.push((
            "x-sprig-error",
            format!("{} {}", code.number(), code.name()),
        ));
        return StudioResponse {
            status: 503,
            headers,
            body: Bytes::from_static(NOT_BUILT_PAGE.as_bytes()),
        };
    }
    // 3. The file; a path without an extension is a route of the
    //    single-page app.
    let last = rel.rsplit('/').next().unwrap_or("");
    let is_index = rel.is_empty() || rel == "index.html" || !last.contains('.');
    let target = if is_index { "index.html" } else { rel.as_str() };
    if let Some(a) = store.get(target) {
        asset_response(cfg, req, a, is_index)
    } else {
        count(404, "none", 0);
        error(
            cfg,
            req.tls,
            404,
            SprigCode::AdminRequestInvalid,
            "no such file",
        )
    }
}

#[cfg(test)]
mod tests;
