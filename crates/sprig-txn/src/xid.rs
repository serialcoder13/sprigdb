//! Transaction ids and XA xids.
//!
//! The **dtid** is a [`TxnId`]: the gateway incarnation (a
//! random `u64` drawn at start) in the high half and a counter in the low
//! half, so it needs no coordination. An XA xid has three parts:
//!
//! - the **gtrid**, 47 binary bytes: version, tenant, dtid, start time and
//!   the home anchor, so the resolver can find the decision from the xid
//!   alone;
//! - the **bqual**, ASCII `c<cluster>.s<shard>`;
//! - the **formatID** [`FORMAT_ID`] (`SPRG`), so the resolver ignores xids
//!   it didn't create.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId, TxnId};

/// A distributed transaction's id (a `TxnId`).
pub type Dtid = TxnId;

/// The XA formatID of coordinator xids: `SPRG` in ASCII.
pub const FORMAT_ID: u32 = 1_397_772_871;
/// The gtrid layout version.
pub const GTRID_VERSION: u8 = 1;
/// Bytes in an encoded gtrid.
pub const GTRID_LEN: usize = 47;

/// Hands out dtids for one gateway incarnation.
#[derive(Debug)]
pub struct DtidGen {
    incarnation: u64,
    counter: AtomicU64,
}

impl DtidGen {
    /// A generator for `incarnation` (the gateway's registration stores
    /// it).
    #[must_use]
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            counter: AtomicU64::new(0),
        }
    }

    /// The incarnation.
    #[must_use]
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// The next dtid.
    pub fn next(&self) -> Dtid {
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        TxnId((u128::from(self.incarnation) << 64) | u128::from(n))
    }
}

/// The gateway incarnation of a dtid.
#[must_use]
pub fn incarnation_of(dtid: Dtid) -> u64 {
    // The high half, by construction.
    u64::try_from(dtid.0 >> 64).unwrap_or(u64::MAX)
}

/// Where a decision row lives, independent of shard ids: a hash point
/// always falls in exactly one shard of the group, which holds the copied
/// decision rows after splits, merges and moves. 16 bytes on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AnchorRef {
    /// Logical database.
    pub db: DbId,
    /// Table group.
    pub group: GroupId,
    /// Start of the anchor shard's hash range.
    pub hash_point: u64,
}

impl AnchorRef {
    /// The 16-byte form (database, group, hash point; big-endian).
    #[must_use]
    pub fn to_bytes(self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&self.db.0.to_be_bytes());
        b[4..8].copy_from_slice(&self.group.0.to_be_bytes());
        b[8..16].copy_from_slice(&self.hash_point.to_be_bytes());
        b
    }

    /// From the 16-byte form.
    #[must_use]
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 16 {
            return None;
        }
        Some(Self {
            db: DbId(u32::from_be_bytes(b[0..4].try_into().ok()?)),
            group: GroupId(u32::from_be_bytes(b[4..8].try_into().ok()?)),
            hash_point: u64::from_be_bytes(b[8..16].try_into().ok()?),
        })
    }
}

impl fmt::Display for AnchorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "d{}.g{}@{:016x}", self.db, self.group, self.hash_point)
    }
}

/// The global transaction id of an XA xid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Gtrid {
    /// The tenant, for filtering `XA RECOVER` and drains.
    pub tenant: TenantId,
    /// The transaction.
    pub dtid: Dtid,
    /// Start time in ms since the epoch, 48 bits (diagnostics only).
    pub start_ms: u64,
    /// The home anchor: where the decision can always be found.
    pub home: AnchorRef,
}

impl Gtrid {
    /// The 47-byte form: version, tenant (8), dtid (16), start ms (6),
    /// home anchor (16).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; GTRID_LEN] {
        let mut b = [0u8; GTRID_LEN];
        b[0] = GTRID_VERSION;
        b[1..9].copy_from_slice(&self.tenant.0.to_be_bytes());
        b[9..25].copy_from_slice(&self.dtid.to_bytes());
        b[25..31].copy_from_slice(&self.start_ms.to_be_bytes()[2..8]);
        b[31..47].copy_from_slice(&self.home.to_bytes());
        b
    }

    /// From the 47-byte form; `None` for another version or length.
    #[must_use]
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != GTRID_LEN || b[0] != GTRID_VERSION {
            return None;
        }
        let mut ms = [0u8; 8];
        ms[2..8].copy_from_slice(&b[25..31]);
        Some(Self {
            tenant: TenantId(u64::from_be_bytes(b[1..9].try_into().ok()?)),
            dtid: TxnId::from_bytes(b[9..25].try_into().ok()?),
            start_ms: u64::from_be_bytes(ms),
            home: AnchorRef::from_bytes(&b[31..47])?,
        })
    }
}

/// The branch qualifier: `c<cluster>.s<shard>`, the cluster and the first
/// shard the branch touched.
#[must_use]
pub fn bqual(cluster: &ClusterId, shard: ShardId) -> String {
    format!("c{cluster}.s{shard}")
}

/// The cluster named by a bqual.
#[must_use]
pub fn bqual_cluster(bqual: &str) -> Option<ClusterId> {
    let rest = bqual.strip_prefix('c')?;
    let (c, _) = rest.rsplit_once(".s")?;
    ClusterId::new(c).ok()
}

/// A coordinator xid.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Xid {
    /// Global part.
    pub gtrid: Gtrid,
    /// Branch part.
    pub bqual: String,
}

impl Xid {
    /// The xid as MariaDB writes it in `XA RECOVER FORMAT='SQL'` and as
    /// XA statements take it: `X'<gtrid>',X'<bqual>',1397772871`.
    #[must_use]
    pub fn sql(&self) -> String {
        format!(
            "X'{}',X'{}',{FORMAT_ID}",
            hex(&self.gtrid.to_bytes()),
            hex(self.bqual.as_bytes())
        )
    }

    /// Parses [`Xid::sql`]'s form (any hex case). `None` for another
    /// formatID or a gtrid this version doesn't know.
    #[must_use]
    pub fn parse_sql(s: &str) -> Option<Self> {
        let mut parts = s.trim().splitn(3, ',');
        let g = unhex_lit(parts.next()?)?;
        let b = unhex_lit(parts.next()?)?;
        let fmt: u32 = parts.next()?.trim().parse().ok()?;
        if fmt != FORMAT_ID {
            return None;
        }
        Some(Self {
            gtrid: Gtrid::from_bytes(&g)?,
            bqual: String::from_utf8(b).ok()?,
        })
    }

    /// The cluster the bqual names.
    #[must_use]
    pub fn cluster(&self) -> Option<ClusterId> {
        bqual_cluster(&self.bqual)
    }
}

impl fmt::Display for Xid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.gtrid.dtid, self.bqual)
    }
}

/// Lowercase hex.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// Parses hex digits (any case).
#[must_use]
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Parses `X'..'` (or `x'..'`); an empty literal is `X''`.
fn unhex_lit(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    let inner = s
        .strip_prefix("X'")
        .or_else(|| s.strip_prefix("x'"))?
        .strip_suffix('\'')?;
    unhex(inner)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn sample() -> Xid {
        Xid {
            gtrid: Gtrid {
                tenant: TenantId(42),
                dtid: DtidGen::new(0xdead_beef).next(),
                start_ms: 1_762_000_000_123,
                home: AnchorRef {
                    db: DbId(1),
                    group: GroupId(3),
                    hash_point: 0x8000_0000_0000_0000,
                },
            },
            bqual: bqual(&ClusterId::new("mum1-c7").unwrap(), ShardId(17)),
        }
    }

    #[test]
    fn xid_round_trips_through_sql_form() {
        let x = sample();
        let s = x.sql();
        assert!(s.starts_with("X'01000000000000002a"), "{s}");
        assert!(s.ends_with(",1397772871"));
        assert_eq!(Xid::parse_sql(&s).unwrap(), x);
        assert_eq!(
            Xid::parse_sql(&s.to_uppercase().replace("X'", "x'")).unwrap(),
            x
        );
        assert_eq!(x.cluster().unwrap().as_str(), "mum1-c7");
        assert_eq!(x.gtrid.to_bytes().len(), 47);
        assert!(s.len() < 200);
    }

    #[test]
    fn foreign_xids_are_ignored() {
        assert!(Xid::parse_sql("X'0100',X'6337',1").is_none());
        // The spike's xids: 16-byte gtrid, right formatID, old layout.
        assert!(
            Xid::parse_sql("X'01000000000000000000000000000029',X'63372e733137',1397772871")
                .is_none()
        );
        assert!(Xid::parse_sql("garbage").is_none());
    }

    #[test]
    fn dtids_carry_the_incarnation() {
        let g = DtidGen::new(7);
        let a = g.next();
        let b = g.next();
        assert_ne!(a, b);
        assert_eq!(incarnation_of(a), 7);
        assert_eq!(a.0 & 0xffff, 1);
    }

    #[test]
    fn start_time_keeps_48_bits() {
        let mut x = sample();
        x.gtrid.start_ms = (1 << 48) - 1;
        let g = Gtrid::from_bytes(&x.gtrid.to_bytes()).unwrap();
        assert_eq!(g.start_ms, (1 << 48) - 1);
    }
}
