//! Host patterns and account matching order (chapter 14, "Account
//! matching").
//!
//! A login name's accounts are sorted once at cache load and the first
//! whose host matches the client address wins:
//!
//! 1. literal IP addresses;
//! 2. CIDR (`10.0.0.0/8`) and netmask (`10.0.0.0/255.0.0.0`) entries, the
//!    longest mask first;
//! 3. literal host names;
//! 4. wildcard patterns (`%` any run, `_` one character), the longest
//!    literal prefix first;
//! 5. `'%'`.
//!
//! Ties keep the stored host text's order, so the order is deterministic.
//! Host names match only with `auth.resolve_hostnames`, which isn't
//! implemented yet: until then a host-name account never matches, except
//! `localhost`, which matches loopback addresses (what MySQL gives a TCP
//! client from 127.0.0.1 when it resolves names, its default). Client
//! addresses that are IPv4-mapped IPv6 are matched as IPv4. Patterns
//! compare case-insensitively, like the `ascii_general_ci` column.

use std::cmp::Ordering;
use std::net::IpAddr;

/// A parsed `users.host`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostPattern {
    /// A literal address.
    Ip(IpAddr),
    /// A network: address and mask (CIDR or netmask form), with the number
    /// of mask bits for ordering.
    Net {
        /// The network address, already masked.
        net: IpAddr,
        /// The mask.
        mask: IpAddr,
        /// Set bits in the mask.
        bits: u32,
    },
    /// A literal host name (lower case).
    Name(String),
    /// A pattern with `%` or `_` (lower case), and its literal prefix
    /// length.
    Wildcard {
        /// The pattern.
        pattern: String,
        /// Characters before the first wildcard.
        prefix: usize,
    },
    /// `'%'`: any host.
    Any,
}

impl HostPattern {
    /// Parses a stored host. Anything that isn't an address, a network or
    /// a pattern is a host name.
    #[must_use]
    pub fn parse(host: &str) -> Self {
        let h = host.trim().to_ascii_lowercase();
        if h == "%" || h.is_empty() {
            return HostPattern::Any;
        }
        if let Ok(ip) = h.parse::<IpAddr>() {
            return HostPattern::Ip(canonical(ip));
        }
        if let Some((addr, m)) = h.split_once('/')
            && let Ok(addr) = addr.parse::<IpAddr>()
        {
            let addr = canonical(addr);
            let mask = match (m.parse::<u32>(), m.parse::<IpAddr>()) {
                (Ok(bits), _) => mask_of(addr, bits),
                (_, Ok(mask)) => Some(canonical(mask)),
                _ => None,
            };
            if let Some(mask) = mask.filter(|m| m.is_ipv4() == addr.is_ipv4()) {
                return HostPattern::Net {
                    net: and(addr, mask),
                    mask,
                    bits: ones(mask),
                };
            }
            return HostPattern::Name(h);
        }
        if let Some(prefix) = h.find(['%', '_']) {
            return HostPattern::Wildcard { pattern: h, prefix };
        }
        HostPattern::Name(h)
    }

    /// Whether a client at `ip` matches.
    #[must_use]
    pub fn matches(&self, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        match self {
            HostPattern::Any => true,
            HostPattern::Ip(a) => *a == ip,
            HostPattern::Net { net, mask, .. } => {
                ip.is_ipv4() == net.is_ipv4() && and(ip, *mask) == *net
            }
            HostPattern::Name(n) => n == "localhost" && ip.is_loopback(),
            HostPattern::Wildcard { pattern, .. } => {
                like(pattern.as_bytes(), ip.to_string().as_bytes())
            }
        }
    }

    /// The matching order: lower ranks first.
    fn rank(&self) -> (u8, u32) {
        match self {
            HostPattern::Ip(_) => (0, 0),
            HostPattern::Net { bits, .. } => (1, u32::MAX - bits),
            HostPattern::Name(_) => (2, 0),
            HostPattern::Wildcard { prefix, .. } => {
                (3, u32::MAX - u32::try_from(*prefix).unwrap_or(u32::MAX))
            }
            HostPattern::Any => (4, 0),
        }
    }

    /// Orders two accounts' hosts for matching (chapter 14): `Less` means
    /// `a` is tried first. `a_text` and `b_text` break ties.
    #[must_use]
    pub fn match_order(a: &HostPattern, a_text: &str, b: &HostPattern, b_text: &str) -> Ordering {
        a.rank().cmp(&b.rank()).then_with(|| a_text.cmp(b_text))
    }
}

/// IPv4-mapped IPv6 as IPv4.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 @ IpAddr::V4(_) => v4,
    }
}

fn mask_of(addr: IpAddr, bits: u32) -> Option<IpAddr> {
    match addr {
        IpAddr::V4(_) if bits <= 32 => {
            let m = if bits == 0 {
                0
            } else {
                u32::MAX << (32 - bits)
            };
            Some(IpAddr::V4(m.into()))
        }
        IpAddr::V6(_) if bits <= 128 => {
            let m = if bits == 0 {
                0
            } else {
                u128::MAX << (128 - bits)
            };
            Some(IpAddr::V6(m.into()))
        }
        _ => None,
    }
}

fn and(a: IpAddr, m: IpAddr) -> IpAddr {
    match (a, m) {
        (IpAddr::V4(a), IpAddr::V4(m)) => IpAddr::V4((u32::from(a) & u32::from(m)).into()),
        (IpAddr::V6(a), IpAddr::V6(m)) => IpAddr::V6((u128::from(a) & u128::from(m)).into()),
        (a, _) => a,
    }
}

fn ones(m: IpAddr) -> u32 {
    match m {
        IpAddr::V4(m) => u32::from(m).count_ones(),
        IpAddr::V6(m) => u128::from(m).count_ones(),
    }
}

/// SQL `LIKE` with `%` and `_` (no escapes; hosts can't contain them
/// otherwise), iterative with backtracking to the last `%`.
fn like(p: &[u8], s: &[u8]) -> bool {
    let (mut pi, mut si) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while si < s.len() {
        if pi < p.len() && (p[pi] == b'_' || p[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < p.len() && p[pi] == b'%' {
            star = Some((pi, si));
            pi += 1;
        } else if let Some((sp, ss)) = star {
            pi = sp + 1;
            si = ss + 1;
            star = Some((sp, ss + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == b'%')
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn parses_every_form() {
        assert_eq!(HostPattern::parse("%"), HostPattern::Any);
        assert_eq!(
            HostPattern::parse("10.0.0.7"),
            HostPattern::Ip(ip("10.0.0.7"))
        );
        assert!(matches!(
            HostPattern::parse("10.1.2.3/8"),
            HostPattern::Net { bits: 8, net, .. } if net == ip("10.0.0.0")
        ));
        assert!(matches!(
            HostPattern::parse("10.0.0.0/255.255.0.0"),
            HostPattern::Net { bits: 16, .. }
        ));
        assert!(matches!(
            HostPattern::parse("10.%"),
            HostPattern::Wildcard { prefix: 3, .. }
        ));
        assert_eq!(
            HostPattern::parse("DB.Example.com"),
            HostPattern::Name("db.example.com".into())
        );
        assert!(matches!(
            HostPattern::parse("10.0.0.0/40"),
            HostPattern::Name(_)
        ));
    }

    #[test]
    fn matches_addresses() {
        assert!(HostPattern::parse("%").matches(ip("1.2.3.4")));
        assert!(HostPattern::parse("10.%").matches(ip("10.9.8.7")));
        assert!(!HostPattern::parse("10.%").matches(ip("110.9.8.7")));
        assert!(HostPattern::parse("10.0.0._").matches(ip("10.0.0.5")));
        assert!(!HostPattern::parse("10.0.0._").matches(ip("10.0.0.55")));
        assert!(HostPattern::parse("10.0.0.0/8").matches(ip("10.200.1.1")));
        assert!(!HostPattern::parse("10.0.0.0/8").matches(ip("11.0.0.1")));
        assert!(HostPattern::parse("192.168.1.0/255.255.255.0").matches(ip("192.168.1.9")));
        assert!(HostPattern::parse("127.0.0.1").matches(ip("::ffff:127.0.0.1")));
        assert!(HostPattern::parse("fd00::/8").matches(ip("fd12::1")));
        assert!(HostPattern::parse("localhost").matches(ip("127.0.0.1")));
        assert!(HostPattern::parse("localhost").matches(ip("::1")));
        assert!(!HostPattern::parse("localhost").matches(ip("10.0.0.1")));
        assert!(!HostPattern::parse("app.example.com").matches(ip("10.0.0.1")));
        assert!(HostPattern::parse("%.%.%.%").matches(ip("1.2.3.4")));
    }

    #[test]
    fn orders_most_specific_first() {
        let mut hosts = vec![
            "%",
            "10.%",
            "10.0.%",
            "app.local",
            "10.0.0.0/8",
            "10.0.0.0/16",
            "10.0.0.1",
        ];
        hosts.sort_by(|a, b| {
            HostPattern::match_order(&HostPattern::parse(a), a, &HostPattern::parse(b), b)
        });
        assert_eq!(
            hosts,
            [
                "10.0.0.1",
                "10.0.0.0/16",
                "10.0.0.0/8",
                "app.local",
                "10.0.%",
                "10.%",
                "%"
            ]
        );
    }
}
