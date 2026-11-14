//! The PROXY protocol, versions 1 (text) and 2 (binary), from load
//! balancers in trusted CIDRs (`gateway.proxy_protocol`,
//! `gateway.proxy_trusted_cidrs`).
//!
//! A header is looked for only when the mode is not `off` and the peer is in
//! a trusted CIDR; from anyone else the bytes are left to the front end.
//! `required` closes a trusted peer's connection that has no valid header.
//! `optional` peeks: a header is consumed if one starts the stream. On a
//! server-speaks-first listener (MySQL family) a client without a header
//! sends nothing, so `optional` waits at most [`OPTIONAL_WAIT`] for one.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// Version 2 signature.
const V2_SIG: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
/// Version 1 prefix.
const V1_PREFIX: &[u8; 6] = b"PROXY ";
/// Longest version 1 line, CRLF included (the specification's limit).
const V1_MAX: usize = 107;
/// Longest version 2 address block accepted; addresses plus a few TLVs.
const V2_MAX_BODY: usize = 1024;

/// How long `optional` waits for a header on a server-speaks-first
/// listener before greeting the client.
pub const OPTIONAL_WAIT: Duration = Duration::from_millis(100);

/// `gateway.proxy_protocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyMode {
    /// Never read a header.
    Off,
    /// Read a header from trusted peers if one is sent.
    Optional,
    /// Trusted peers must send a header.
    Required,
}

impl FromStr for ProxyMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "off" => Ok(ProxyMode::Off),
            "optional" => Ok(ProxyMode::Optional),
            "required" => Ok(ProxyMode::Required),
            _ => Err(format!("PROXY mode {s:?} is not off, optional or required")),
        }
    }
}

/// An IPv4 or IPv6 network, `10.0.0.0/8` or `fd00::/8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    net: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// Whether `ip` is inside. An IPv4-mapped IPv6 address matches IPv4
    /// networks.
    #[must_use]
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = match ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
            v4 @ IpAddr::V4(_) => v4,
        };
        match (self.net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                masked(u128::from(u32::from(n)), self.prefix, 32)
                    == masked(u128::from(u32::from(a)), self.prefix, 32)
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                masked(u128::from(n), self.prefix, 128) == masked(u128::from(a), self.prefix, 128)
            }
            _ => false,
        }
    }
}

fn masked(x: u128, prefix: u8, bits: u8) -> u128 {
    if prefix == 0 { 0 } else { x >> (bits - prefix) }
}

impl FromStr for Cidr {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("{s:?} is not a CIDR such as 10.0.0.0/8");
        let (ip, prefix) = match s.split_once('/') {
            Some((ip, p)) => (ip, Some(p)),
            None => (s, None),
        };
        let net: IpAddr = ip.trim().parse().map_err(|_| bad())?;
        let max = if net.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) => p.trim().parse::<u8>().map_err(|_| bad())?,
            None => max,
        };
        if prefix > max {
            return Err(bad());
        }
        Ok(Cidr { net, prefix })
    }
}

/// The PROXY settings a listener applies, snapshot in its spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyPolicy {
    /// `gateway.proxy_protocol`.
    pub mode: ProxyMode,
    /// `gateway.proxy_trusted_cidrs`.
    pub trusted: Vec<Cidr>,
}

impl ProxyPolicy {
    /// No PROXY headers.
    pub const OFF: ProxyPolicy = ProxyPolicy {
        mode: ProxyMode::Off,
        trusted: Vec::new(),
    };

    /// Parses a comma-separated CIDR list.
    ///
    /// # Errors
    /// The first entry that doesn't parse.
    pub fn parse_cidrs(list: &str) -> Result<Vec<Cidr>, String> {
        list.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::parse)
            .collect()
    }

    fn trusts(&self, peer: IpAddr) -> bool {
        self.trusted.iter().any(|c| c.contains(peer))
    }
}

/// A parsed header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Header {
    /// A proxied connection from `source`.
    Proxied(SocketAddr),
    /// A connection the proxy made itself (v2 `LOCAL`, v1 `UNKNOWN`, or a
    /// non-IP family): keep the socket's peer address.
    Local,
}

/// Why a header was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProxyError {
    /// No header where one was required.
    Missing,
    /// A header that doesn't parse.
    Malformed(&'static str),
    /// Reading failed.
    Io(String),
}

/// Applies `policy` to a new connection: returns the client address to
/// use, having consumed exactly the header bytes, if any.
pub(crate) async fn accept(
    stream: &mut TcpStream,
    peer: SocketAddr,
    policy: &ProxyPolicy,
    client_speaks_first: bool,
) -> Result<SocketAddr, ProxyError> {
    if policy.mode == ProxyMode::Off || !policy.trusts(peer.ip()) {
        return Ok(peer);
    }
    if policy.mode == ProxyMode::Optional {
        let sniffed = if client_speaks_first {
            Some(sniff_stream(stream).await?)
        } else {
            tokio::time::timeout(OPTIONAL_WAIT, sniff_stream(stream))
                .await
                .ok()
                .transpose()?
        };
        if !matches!(sniffed, Some(Sniff::V1 | Sniff::V2)) {
            return Ok(peer);
        }
    }
    Ok(match read_header(stream).await? {
        Header::Proxied(addr) => addr,
        Header::Local => peer,
    })
}

/// What the first bytes of a stream are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sniff {
    /// A version 1 header.
    V1,
    /// A version 2 header.
    V2,
    /// Not a PROXY header.
    Other,
    /// Too few bytes to tell.
    NeedMore,
}

/// Classifies a stream prefix.
pub(crate) fn sniff(prefix: &[u8]) -> Sniff {
    let check = |sig: &[u8]| {
        let n = prefix.len().min(sig.len());
        if prefix[..n] != sig[..n] {
            Some(false)
        } else if n == sig.len() {
            Some(true)
        } else {
            None
        }
    };
    match (check(V1_PREFIX), check(V2_SIG)) {
        (Some(true), _) => Sniff::V1,
        (_, Some(true)) => Sniff::V2,
        (Some(false), Some(false)) => Sniff::Other,
        _ => Sniff::NeedMore,
    }
}

/// Peeks until the prefix is classified. Waits for data; the caller bounds
/// the wait.
async fn sniff_stream(stream: &TcpStream) -> Result<Sniff, ProxyError> {
    let mut buf = [0u8; V2_SIG.len()];
    loop {
        let n = stream
            .peek(&mut buf)
            .await
            .map_err(|e| ProxyError::Io(e.to_string()))?;
        if n == 0 {
            return Err(ProxyError::Io("closed before sending data".into()));
        }
        match sniff(&buf[..n]) {
            // Part of a signature has arrived; peek returns at once while
            // data is buffered, so wait a moment for the rest.
            Sniff::NeedMore => tokio::time::sleep(Duration::from_millis(1)).await,
            s => return Ok(s),
        }
    }
}

/// Reads exactly one header from `stream`.
async fn read_header(stream: &mut TcpStream) -> Result<Header, ProxyError> {
    let io = |e: std::io::Error| ProxyError::Io(e.to_string());
    let mut head = [0u8; 16];
    stream.read_exact(&mut head[..6]).await.map_err(io)?;
    if &head[..6] == V1_PREFIX {
        let mut line = head[..6].to_vec();
        while !line.ends_with(b"\r\n") {
            if line.len() >= V1_MAX {
                return Err(ProxyError::Malformed("v1 line too long"));
            }
            line.push(stream.read_u8().await.map_err(io)?);
        }
        return parse_v1(&line);
    }
    if head[..6] != V2_SIG[..6] {
        return Err(ProxyError::Missing);
    }
    stream.read_exact(&mut head[6..]).await.map_err(io)?;
    if head[..12] != V2_SIG[..] {
        return Err(ProxyError::Missing);
    }
    let len = usize::from(u16::from_be_bytes([head[14], head[15]]));
    if len > V2_MAX_BODY {
        return Err(ProxyError::Malformed("v2 address block too long"));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.map_err(io)?;
    parse_v2(&head, &body)
}

/// Parses a version 1 line, CRLF included.
pub(crate) fn parse_v1(line: &[u8]) -> Result<Header, ProxyError> {
    let bad = ProxyError::Malformed;
    let text = std::str::from_utf8(line).map_err(|_| bad("v1 is not ASCII"))?;
    let text = text.strip_suffix("\r\n").ok_or(bad("v1 has no CRLF"))?;
    let mut parts = text.split(' ');
    if parts.next() != Some("PROXY") {
        return Err(bad("v1 has no PROXY"));
    }
    match parts.next() {
        Some("UNKNOWN") => return Ok(Header::Local),
        Some("TCP4" | "TCP6") => {}
        _ => return Err(bad("v1 family is not TCP4, TCP6 or UNKNOWN")),
    }
    let (Some(src), Some(_dst), Some(sport), Some(_dport), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(bad("v1 needs four address fields"));
    };
    let ip: IpAddr = src.parse().map_err(|_| bad("v1 source address"))?;
    let port: u16 = sport.parse().map_err(|_| bad("v1 source port"))?;
    Ok(Header::Proxied(SocketAddr::new(ip, port)))
}

/// Parses a version 2 header: the 16 fixed bytes and the address block.
pub(crate) fn parse_v2(head: &[u8; 16], body: &[u8]) -> Result<Header, ProxyError> {
    let bad = ProxyError::Malformed;
    if head[12] >> 4 != 2 {
        return Err(bad("v2 version is not 2"));
    }
    match head[12] & 0x0f {
        0 => return Ok(Header::Local),
        1 => {}
        _ => return Err(bad("v2 command is not LOCAL or PROXY")),
    }
    let port = |b: &[u8]| u16::from_be_bytes([b[0], b[1]]);
    match head[13] >> 4 {
        1 => {
            let a = body.get(..12).ok_or(bad("v2 IPv4 block too short"))?;
            let ip = Ipv4Addr::new(a[0], a[1], a[2], a[3]);
            Ok(Header::Proxied(SocketAddr::new(ip.into(), port(&a[8..10]))))
        }
        2 => {
            let a = body.get(..36).ok_or(bad("v2 IPv6 block too short"))?;
            let mut ip = [0u8; 16];
            ip.copy_from_slice(&a[..16]);
            let ip = Ipv6Addr::from(ip);
            Ok(Header::Proxied(SocketAddr::new(
                ip.into(),
                port(&a[32..34]),
            )))
        }
        // UNSPEC or UNIX: no IP source to report.
        0 | 3 => Ok(Header::Local),
        _ => Err(bad("v2 address family")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidrs() {
        let c: Cidr = "10.0.0.0/8".parse().unwrap();
        assert!(c.contains("10.200.1.2".parse().unwrap()));
        assert!(c.contains("::ffff:10.1.1.1".parse().unwrap()));
        assert!(!c.contains("11.0.0.1".parse().unwrap()));
        assert!(!c.contains("::1".parse().unwrap()));
        let all: Cidr = "0.0.0.0/0".parse().unwrap();
        assert!(all.contains("8.8.8.8".parse().unwrap()));
        let host: Cidr = "fd00::1".parse().unwrap();
        assert!(host.contains("fd00::1".parse().unwrap()));
        assert!(!host.contains("fd00::2".parse().unwrap()));
        assert!("10.0.0.0/33".parse::<Cidr>().is_err());
        assert!("nonsense".parse::<Cidr>().is_err());
        assert_eq!(
            ProxyPolicy::parse_cidrs(" 10.0.0.0/8, ,127.0.0.1/32")
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn sniffing() {
        assert_eq!(sniff(b"PRO"), Sniff::NeedMore);
        assert_eq!(sniff(b"PROXY TCP4"), Sniff::V1);
        assert_eq!(sniff(b"POST /"), Sniff::Other);
        assert_eq!(sniff(b"\r\n\r\n"), Sniff::NeedMore);
        assert_eq!(sniff(V2_SIG), Sniff::V2);
        assert_eq!(sniff(&[0x16, 0x03, 0x01]), Sniff::Other);
        assert_eq!(sniff(b"\r\nX"), Sniff::Other);
    }

    #[test]
    fn v1() {
        assert_eq!(
            parse_v1(b"PROXY TCP4 192.0.2.1 198.51.100.2 51000 3306\r\n"),
            Ok(Header::Proxied("192.0.2.1:51000".parse().unwrap()))
        );
        assert_eq!(
            parse_v1(b"PROXY TCP6 2001:db8::1 2001:db8::2 4000 443\r\n"),
            Ok(Header::Proxied("[2001:db8::1]:4000".parse().unwrap()))
        );
        assert_eq!(parse_v1(b"PROXY UNKNOWN\r\n"), Ok(Header::Local));
        assert!(parse_v1(b"PROXY TCP4 192.0.2.1 198.51.100.2 51000\r\n").is_err());
        assert!(parse_v1(b"PROXY UDP4 a b 1 2\r\n").is_err());
        assert!(parse_v1(b"PROXY TCP4 x 198.51.100.2 1 2\r\n").is_err());
    }

    fn v2_head(cmd: u8, fam: u8, len: u16) -> [u8; 16] {
        let mut h = [0u8; 16];
        h[..12].copy_from_slice(V2_SIG);
        h[12] = 0x20 | cmd;
        h[13] = fam;
        h[14..].copy_from_slice(&len.to_be_bytes());
        h
    }

    #[test]
    fn v2() {
        let body = [192, 0, 2, 1, 198, 51, 100, 2, 0xc7, 0x38, 0x0c, 0xea];
        assert_eq!(
            parse_v2(&v2_head(1, 0x11, 12), &body),
            Ok(Header::Proxied("192.0.2.1:51000".parse().unwrap()))
        );
        let mut body6 = [0u8; 36];
        body6[..16].copy_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
        body6[32..34].copy_from_slice(&4000u16.to_be_bytes());
        assert_eq!(
            parse_v2(&v2_head(1, 0x21, 36), &body6),
            Ok(Header::Proxied("[2001:db8::1]:4000".parse().unwrap()))
        );
        assert_eq!(parse_v2(&v2_head(0, 0, 0), &[]), Ok(Header::Local));
        assert!(parse_v2(&v2_head(1, 0x11, 4), &body[..4]).is_err());
        let mut v3 = v2_head(1, 0x11, 12);
        v3[12] = 0x31;
        assert!(parse_v2(&v3, &body).is_err());
    }
}
