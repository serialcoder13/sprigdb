//! A client for toxiproxy's HTTP API. Toxiproxy sits on every
//! gateway-to-MariaDB, gateway-to-etcd and inter-DC link.
//!
//! In the single-DC Compose file toxiproxy runs under the `faults`
//! profile, its API on `127.0.0.1:18474` and proxy listeners on
//! `127.0.0.1:13400-13419` (`docker compose --profile faults up -d
//! toxiproxy`). Tests create their own proxies, named `sprig-sim-*`, with
//! upstreams such as `single:3306`, connect through the listener, and add
//! toxics to them; [`Proxy`] deletes its proxy when dropped.
//!
//! The client speaks HTTP/1.1 over a plain TCP stream: toxiproxy's API is a
//! handful of JSON calls, not worth an HTTP client dependency.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_common::{Result, SprigError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::util::{host_port, unique_name};

/// The API address of the Compose service.
pub const DEFAULT_API: &str = "http://127.0.0.1:18474";

/// The first and last host port toxiproxy publishes for proxies.
pub const PROXY_PORTS: (u16, u16) = (13400, 13419);

/// A toxic: a fault on one direction of a proxy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Toxic {
    /// Unique within the proxy; set by [`Toxiproxy::add_toxic`] if empty.
    #[serde(default)]
    pub name: String,
    /// `latency`, `bandwidth`, `slow_close`, `timeout`, `reset_peer`,
    /// `slicer` or `limit_data`.
    #[serde(rename = "type")]
    pub kind: String,
    /// `downstream` (server to client) or `upstream`.
    pub stream: String,
    /// Share of connections affected, 0 to 1.
    pub toxicity: f64,
    /// Type-specific attributes.
    pub attributes: Value,
}

impl Toxic {
    fn new(kind: &str, attributes: Value) -> Self {
        Self {
            name: String::new(),
            kind: kind.to_owned(),
            stream: "downstream".to_owned(),
            toxicity: 1.0,
            attributes,
        }
    }

    /// Added latency with jitter, in milliseconds.
    #[must_use]
    pub fn latency(ms: u64, jitter_ms: u64) -> Self {
        Self::new("latency", json!({ "latency": ms, "jitter": jitter_ms }))
    }

    /// Bandwidth limit in KB/s.
    #[must_use]
    pub fn bandwidth(kb_per_s: u64) -> Self {
        Self::new("bandwidth", json!({ "rate": kb_per_s }))
    }

    /// Stops all data and closes after `timeout_ms` (0: never closes: a
    /// black hole).
    #[must_use]
    pub fn timeout(timeout_ms: u64) -> Self {
        Self::new("timeout", json!({ "timeout": timeout_ms }))
    }

    /// Resets connections (TCP RST) after `timeout_ms`.
    #[must_use]
    pub fn reset_peer(timeout_ms: u64) -> Self {
        Self::new("reset_peer", json!({ "timeout": timeout_ms }))
    }

    /// Applies to the upstream direction (client to server) instead.
    #[must_use]
    pub fn upstream(mut self) -> Self {
        "upstream".clone_into(&mut self.stream);
        self
    }

    /// Affects only this share of connections.
    #[must_use]
    pub fn toxicity(mut self, t: f64) -> Self {
        self.toxicity = t.clamp(0.0, 1.0);
        self
    }
}

/// A toxiproxy server.
#[derive(Debug, Clone)]
pub struct Toxiproxy {
    addr: String,
}

/// An HTTP response: status and body.
struct Response {
    status: u16,
    body: String,
}

impl Toxiproxy {
    /// A client for the API at `url`, such as [`DEFAULT_API`].
    ///
    /// # Errors
    /// If `url` has no `host:port`.
    pub fn new(url: &str) -> Result<Self> {
        let addr = host_port(url)
            .ok_or_else(|| SprigError::internal(format!("toxiproxy URL without host:port: {url}")))?
            .to_owned();
        Ok(Self { addr })
    }

    async fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Response> {
        let fail = |what: &str, e: &dyn std::fmt::Display| {
            SprigError::internal(format!("toxiproxy {method} {path}: {what}: {e}"))
        };
        let io = async {
            let mut s = tokio::net::TcpStream::connect(&self.addr).await?;
            let body = body.map(Value::to_string).unwrap_or_default();
            let req = format!(
                "{method} {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: sprig-testkit\r\n\
                 Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                self.addr,
                body.len()
            );
            s.write_all(req.as_bytes()).await?;
            let mut raw = Vec::new();
            s.read_to_end(&mut raw).await?;
            Ok::<Vec<u8>, std::io::Error>(raw)
        };
        let raw = tokio::time::timeout(Duration::from_secs(10), io)
            .await
            .map_err(|_| fail("request", &"timed out"))?
            .map_err(|e| fail("request", &e))?;
        parse_response(&raw).map_err(|e| fail("response", &e))
    }

    async fn ok(&self, method: &str, path: &str, body: Option<&Value>) -> Result<String> {
        let r = self.call(method, path, body).await?;
        if (200..300).contains(&r.status) {
            Ok(r.body)
        } else {
            Err(SprigError::internal(format!(
                "toxiproxy {method} {path}: HTTP {}: {}",
                r.status,
                r.body.trim()
            )))
        }
    }

    /// The server's version; a cheap liveness check.
    ///
    /// # Errors
    /// If the API doesn't answer.
    pub async fn version(&self) -> Result<String> {
        self.ok("GET", "/version", None).await
    }

    /// Creates a proxy `name` listening on `listen` (inside the
    /// toxiproxy container, such as `0.0.0.0:13400`) and forwarding to
    /// `upstream` (such as `single:3306`).
    ///
    /// # Errors
    /// If it exists or the API fails.
    pub async fn create_proxy(&self, name: &str, listen: &str, upstream: &str) -> Result<()> {
        let body = json!({ "name": name, "listen": listen, "upstream": upstream, "enabled": true });
        self.ok("POST", "/proxies", Some(&body)).await.map(drop)
    }

    /// Deletes a proxy and its toxics.
    ///
    /// # Errors
    /// If the API fails (a missing proxy is not an error).
    pub async fn delete_proxy(&self, name: &str) -> Result<()> {
        let r = self
            .call("DELETE", &format!("/proxies/{name}"), None)
            .await?;
        if (200..300).contains(&r.status) || r.status == 404 {
            Ok(())
        } else {
            Err(SprigError::internal(format!(
                "toxiproxy DELETE {name}: HTTP {}",
                r.status
            )))
        }
    }

    /// Names of every proxy.
    ///
    /// # Errors
    /// If the API fails.
    pub async fn proxies(&self) -> Result<Vec<String>> {
        let body = self.ok("GET", "/proxies", None).await?;
        let v: Value = serde_json::from_str(&body)
            .map_err(|e| SprigError::internal(format!("toxiproxy /proxies: {e}")))?;
        Ok(v.as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    /// Enables or disables a proxy; disabled, it refuses and drops
    /// connections (a partition).
    ///
    /// # Errors
    /// If the API fails.
    pub async fn set_enabled(&self, proxy: &str, enabled: bool) -> Result<()> {
        let body = json!({ "enabled": enabled });
        self.ok("POST", &format!("/proxies/{proxy}"), Some(&body))
            .await
            .map(drop)
    }

    /// Adds a toxic; returns its name.
    ///
    /// # Errors
    /// If the API fails.
    pub async fn add_toxic(&self, proxy: &str, toxic: &Toxic) -> Result<String> {
        let mut t = toxic.clone();
        if t.name.is_empty() {
            t.name = unique_name(&format!("{}-{}", t.kind, t.stream));
        }
        let body = serde_json::to_value(&t)
            .map_err(|e| SprigError::internal(format!("encoding toxic: {e}")))?;
        self.ok("POST", &format!("/proxies/{proxy}/toxics"), Some(&body))
            .await?;
        Ok(t.name)
    }

    /// Removes a toxic.
    ///
    /// # Errors
    /// If the API fails.
    pub async fn remove_toxic(&self, proxy: &str, toxic: &str) -> Result<()> {
        self.ok("DELETE", &format!("/proxies/{proxy}/toxics/{toxic}"), None)
            .await
            .map(drop)
    }

    /// Creates a proxy named `sprig-sim-<prefix>-<unique>` on the first
    /// free published port, forwarding to `upstream`.
    ///
    /// # Errors
    /// If every port is taken or the API fails.
    pub async fn own_proxy(&self, prefix: &str, upstream: &str) -> Result<Proxy> {
        let name = unique_name(&format!("{}{prefix}", super::OWN_CONTAINER_PREFIX));
        let mut last = None;
        for port in PROXY_PORTS.0..=PROXY_PORTS.1 {
            match self
                .create_proxy(&name, &format!("0.0.0.0:{port}"), upstream)
                .await
            {
                Ok(()) => {
                    return Ok(Proxy {
                        api: self.clone(),
                        name,
                        host_addr: format!("127.0.0.1:{port}"),
                    });
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| SprigError::internal("no proxy ports")))
    }
}

/// A proxy this test owns; deleted on drop.
#[derive(Debug)]
pub struct Proxy {
    api: Toxiproxy,
    name: String,
    host_addr: String,
}

impl Proxy {
    /// The proxy's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Where clients on the host connect: `127.0.0.1:<port>`.
    #[must_use]
    pub fn host_addr(&self) -> &str {
        &self.host_addr
    }

    /// The API client.
    #[must_use]
    pub fn api(&self) -> &Toxiproxy {
        &self.api
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let (api, name) = (self.api.clone(), self.name.clone());
        // Best effort, from any context: a short-lived thread with its own
        // runtime, so dropping inside or outside tokio both work.
        let _ = std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                let _ = rt.block_on(api.delete_proxy(&name));
            }
        })
        .join();
    }
}

/// Parses an HTTP/1.1 response, de-chunking if needed.
fn parse_response(raw: &[u8]) -> std::result::Result<Response, String> {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "no header end".to_owned())?;
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "bad status line".to_owned())?;
    let chunked = lines.any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let body = if chunked {
        dechunk(body)?
    } else {
        body.to_owned()
    };
    Ok(Response { status, body })
}

fn dechunk(mut s: &str) -> std::result::Result<String, String> {
    let mut out = String::new();
    loop {
        let (size, rest) = s.split_once("\r\n").ok_or_else(|| "bad chunk".to_owned())?;
        let n = usize::from_str_radix(size.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|e| format!("chunk size: {e}"))?;
        if n == 0 {
            return Ok(out);
        }
        let chunk = rest.get(..n).ok_or_else(|| "short chunk".to_owned())?;
        out.push_str(chunk);
        s = rest.get(n + 2..).ok_or_else(|| "short chunk".to_owned())?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_chunked_responses() {
        let r = parse_response(b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\n{}").unwrap();
        assert_eq!((r.status, r.body.as_str()), (201, "{}"));
        let r = parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{\"a\"\r\n3\r\n:1}\r\n0\r\n\r\n",
        )
        .unwrap();
        assert_eq!(r.body, "{\"a\":1}");
        assert!(parse_response(b"garbage").is_err());
    }

    #[test]
    fn toxics_serialize_as_the_api_expects() {
        let t = Toxic::latency(30, 5).upstream().toxicity(0.5);
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v["type"], "latency");
        assert_eq!(v["stream"], "upstream");
        assert_eq!(v["attributes"]["latency"], 30);
        assert_eq!(v["toxicity"], 0.5);
    }
}
