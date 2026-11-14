//! [`GtidSet`]: a MariaDB GTID position, one sequence number per
//! replication domain, for read routing and read-your-writes.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// A MariaDB position: for each replication domain, the server id and
/// sequence number of the last transaction. Union keeps the higher
/// sequence number per domain.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct GtidSet {
    domains: BTreeMap<u32, (u32, u64)>,
}

/// Why a GTID list didn't parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GtidParseError(pub String);

impl fmt::Display for GtidParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bad GTID list {:?}: want domain-server-seq[,...]",
            self.0
        )
    }
}

impl std::error::Error for GtidParseError {}

impl GtidSet {
    /// The empty set: no commit to wait for.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// Adds one GTID, keeping the higher sequence number of its domain.
    pub fn add(&mut self, domain: u32, server: u32, seq: u64) {
        let slot = self.domains.entry(domain).or_insert((server, seq));
        if seq >= slot.1 {
            *slot = (server, seq);
        }
    }

    /// Adds every GTID of `other`.
    pub fn union(&mut self, other: &GtidSet) {
        for (&d, &(s, n)) in &other.domains {
            self.add(d, s, n);
        }
    }

    /// Whether this position has applied everything in `target`: for every
    /// domain of `target`, a sequence number at least as high.
    #[must_use]
    pub fn covers(&self, target: &GtidSet) -> bool {
        target
            .domains
            .iter()
            .all(|(d, &(_, n))| self.domains.get(d).is_some_and(|&(_, m)| m >= n))
    }

    /// The sequence number of `domain`, if present.
    #[must_use]
    pub fn seq(&self, domain: u32) -> Option<u64> {
        self.domains.get(&domain).map(|&(_, n)| n)
    }

    /// The part of this position in one replication domain (a Galera
    /// cluster's `wsrep_gtid_domain_id`).
    #[must_use]
    pub fn only(&self, domain: u32) -> GtidSet {
        GtidSet {
            domains: self
                .domains
                .get(&domain)
                .map(|&v| (domain, v))
                .into_iter()
                .collect(),
        }
    }

    /// Parses `@@gtid_slave_pos`-style text; empty text is the empty set.
    ///
    /// # Errors
    /// [`GtidParseError`] for anything but `domain-server-seq` items.
    pub fn parse(s: &str) -> Result<Self, GtidParseError> {
        let mut set = GtidSet::new();
        for item in s.split(',').map(str::trim).filter(|i| !i.is_empty()) {
            let bad = || GtidParseError(s.to_owned());
            let mut it = item.splitn(3, '-');
            let mut part = || it.next().ok_or_else(bad);
            let d = part()?.parse().map_err(|_| bad())?;
            let sv = part()?.parse().map_err(|_| bad())?;
            let n = part()?.parse().map_err(|_| bad())?;
            set.add(d, sv, n);
        }
        Ok(set)
    }
}

impl FromStr for GtidSet {
    type Err = GtidParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (d, (s, n)) in &self.domains {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            write!(f, "{d}-{s}-{n}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_union_cover_and_display() {
        let a = GtidSet::parse("0-11-5402, 1-12-77").unwrap();
        assert_eq!(a.to_string(), "0-11-5402,1-12-77");
        let b = GtidSet::parse("0-11-5399").unwrap();
        assert!(a.covers(&b));
        assert!(!b.covers(&a));
        assert!(b.covers(&GtidSet::new()));
        let mut c = b.clone();
        c.union(&a);
        assert_eq!(c, a);
        c.add(0, 13, 5500);
        assert_eq!(c.to_string(), "0-13-5500,1-12-77");
        c.add(0, 11, 1);
        assert_eq!(c.seq(0), Some(5500));
        assert!(GtidSet::parse("").unwrap().is_empty());
        assert!(GtidSet::parse("0-1").is_err());
        assert!(GtidSet::parse("x-1-2").is_err());
        assert_eq!(a.only(1).to_string(), "1-12-77");
        assert!(a.only(9).is_empty());
    }
}
