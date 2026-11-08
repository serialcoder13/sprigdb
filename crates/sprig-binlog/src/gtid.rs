//! MariaDB GTIDs and positions (chapter 10, "Positions").
//!
//! A MariaDB GTID is `domain-server-seq`. A position ([`GtidPos`]) holds one
//! GTID per replication domain: the last transaction applied in that domain.
//! It is what `@@gtid_binlog_pos` returns and what `@slave_connect_state`
//! takes.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use sprig_common::{Result, SprigCode, SprigError};

/// One MariaDB global transaction id, `domain-server-seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Gtid {
    /// Replication domain (`gtid_domain_id`).
    pub domain: u32,
    /// The `server_id` of the server that first logged the transaction.
    pub server: u32,
    /// Sequence number within the domain.
    pub seq: u64,
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.domain, self.server, self.seq)
    }
}

impl FromStr for Gtid {
    type Err = SprigError;

    fn from_str(s: &str) -> Result<Self> {
        let bad = || {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("bad GTID {s:?}: want domain-server-seq"),
            )
        };
        let mut it = s.trim().splitn(3, '-');
        let mut part = || it.next().ok_or_else(bad);
        let domain = part()?.parse().map_err(|_| bad())?;
        let server = part()?.parse().map_err(|_| bad())?;
        let seq = part()?.parse().map_err(|_| bad())?;
        Ok(Gtid {
            domain,
            server,
            seq,
        })
    }
}

/// A binlog position: at most one GTID per domain, the last transaction
/// applied in it. The empty position means "from the start of the binlog".
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct GtidPos {
    domains: BTreeMap<u32, Gtid>,
}

impl GtidPos {
    /// The empty position.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A position from GTIDs; a later GTID for a domain replaces an earlier one.
    pub fn from_gtids(gtids: impl IntoIterator<Item = Gtid>) -> Self {
        let mut pos = Self::new();
        for g in gtids {
            pos.domains.insert(g.domain, g);
        }
        pos
    }

    /// Records `gtid` as the last transaction of its domain.
    pub fn update(&mut self, gtid: Gtid) {
        self.domains.insert(gtid.domain, gtid);
    }

    /// The GTID held for `domain`.
    #[must_use]
    pub fn get(&self, domain: u32) -> Option<&Gtid> {
        self.domains.get(&domain)
    }

    /// Every GTID, by domain.
    pub fn iter(&self) -> impl Iterator<Item = &Gtid> {
        self.domains.values()
    }

    /// Number of domains.
    #[must_use]
    pub fn len(&self) -> usize {
        self.domains.len()
    }

    /// Whether the position names no domain.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// Keeps only the domains `keep` accepts. On Galera, positions keep only
    /// the cluster's `wsrep_gtid_domain_id` (chapter 10).
    pub fn retain_domains(&mut self, mut keep: impl FnMut(u32) -> bool) {
        self.domains.retain(|d, _| keep(*d));
    }

    /// Whether this position has reached `other`: every domain of `other`
    /// has a sequence number here at least as large. Meaningful only with
    /// `gtid_strict_mode`, where sequence numbers rise within a domain.
    #[must_use]
    pub fn reached(&self, other: &GtidPos) -> bool {
        other
            .domains
            .values()
            .all(|g| self.domains.get(&g.domain).is_some_and(|h| h.seq >= g.seq))
    }
}

impl fmt::Display for GtidPos {
    /// The form `@@gtid_binlog_pos` uses: `0-11-90812,7-21-1550`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, g) in self.domains.values().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{g}")?;
        }
        Ok(())
    }
}

impl FromStr for GtidPos {
    type Err = SprigError;

    /// Parses `@@gtid_binlog_pos` text; whitespace and empty items are
    /// ignored, so `""` is the empty position.
    fn from_str(s: &str) -> Result<Self> {
        let mut pos = GtidPos::new();
        for item in s.split(',').map(str::trim).filter(|i| !i.is_empty()) {
            let g: Gtid = item.parse()?;
            if pos.domains.insert(g.domain, g).is_some() {
                return Err(SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("GTID position {s:?} names domain {} twice", g.domain),
                ));
            }
        }
        Ok(pos)
    }
}

impl FromIterator<Gtid> for GtidPos {
    fn from_iter<T: IntoIterator<Item = Gtid>>(iter: T) -> Self {
        Self::from_gtids(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_prints_positions() {
        let p: GtidPos = "0-11-90812, 7-21-1550".parse().unwrap();
        assert_eq!(p.to_string(), "0-11-90812,7-21-1550");
        assert_eq!(p.get(7).unwrap().seq, 1550);
        assert!("".parse::<GtidPos>().unwrap().is_empty());
        assert!("1-2".parse::<GtidPos>().is_err());
        assert!("1-2-x".parse::<GtidPos>().is_err());
        assert!("1-2-3,1-4-5".parse::<GtidPos>().is_err());
    }

    #[test]
    fn reached_compares_every_domain_of_the_target() {
        let a: GtidPos = "0-1-10,7-2-5".parse().unwrap();
        assert!(a.reached(&"0-1-10".parse().unwrap()));
        assert!(a.reached(&"0-9-3,7-2-5".parse().unwrap()));
        assert!(!a.reached(&"0-1-11".parse().unwrap()));
        assert!(!a.reached(&"8-1-1".parse().unwrap()));
        assert!(a.reached(&GtidPos::new()));
    }

    #[test]
    fn update_and_retain() {
        let mut p = GtidPos::new();
        p.update(Gtid {
            domain: 3,
            server: 301,
            seq: 4,
        });
        p.update(Gtid {
            domain: 301,
            server: 301,
            seq: 9,
        });
        p.update(Gtid {
            domain: 3,
            server: 302,
            seq: 5,
        });
        assert_eq!(p.to_string(), "3-302-5,301-301-9");
        p.retain_domains(|d| d == 3);
        assert_eq!(p.to_string(), "3-302-5");
    }
}
