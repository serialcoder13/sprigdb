//! Resume tokens (chapter 38, "Resume tokens").
//!
//! A token names a point in a tenant's feed: the cluster and the binlog
//! position to read from, the transaction and row already delivered, the
//! clamp there, and the ownership segment it lies in. It never holds
//! values. Tokens are not signed: a forged token only names a position.
//!
//! ```text
//! token v1 (binary; hex in text):
//!   version u8 = 1 | flags u8 (bit 1 progress-only, bit 2 has until)
//!   tenant u64 | clamp u64 | segment u64
//!   cluster: length u8, then the cluster id's bytes
//!   [until: domain u32 | server u32 | seq u64 | row u32]
//!   position: count u8, then count x (domain u32 | server u32 | seq u64)
//! ```
//!
//! **As built, against the book's layout:** the book's token carries one
//! GTID and finds the other GTID domains through cluster checkpoints. This
//! one carries the whole position to restart the dump from, so a resume
//! needs no checkpoint lookup; `until` names the last delivered row, and
//! everything up to it is skipped on the re-read. The position can lie
//! before `until` while an XA transaction is prepared but not committed,
//! so the re-read sees the prepare again.

use std::fmt::Write as _;

use sprig_binlog::{Gtid, GtidPos};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

/// `until.row` for "the whole transaction was delivered".
pub const AFTER_TXN: u32 = u32::MAX;

const VERSION: u8 = 1;
const FLAG_PROGRESS: u8 = 0b10;
const FLAG_UNTIL: u8 = 0b100;

/// A point in one tenant's feed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResumeToken {
    /// The tenant.
    pub tenant: TenantId,
    /// The clamp of the `until` transaction (the last clamp given, for a
    /// progress token).
    pub clamp: u64,
    /// The cluster the position belongs to.
    pub cluster: ClusterId,
    /// The ownership segment: the map version of the handoff that made
    /// `cluster` the owner, or 0 before the tenant's first recorded move.
    pub segment: u64,
    /// Where to start reading `cluster`'s binlog on resume.
    pub pos: GtidPos,
    /// The last delivered transaction and row: on resume, everything up to
    /// and including it is skipped.
    pub until: Option<(Gtid, u32)>,
    /// A progress token: no event at this point.
    pub progress: bool,
}

fn bad(why: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::FeedBadResumeToken, why)
}

struct Reader<'a> {
    b: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.b.len() < n {
            return Err(bad("resume token is truncated"));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        let mut a = [0u8; 4];
        a.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(a))
    }
    fn u64(&mut self) -> Result<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(a))
    }
    fn gtid(&mut self) -> Result<Gtid> {
        Ok(Gtid {
            domain: self.u32()?,
            server: self.u32()?,
            seq: self.u64()?,
        })
    }
}

fn put_gtid(out: &mut Vec<u8>, g: &Gtid) {
    out.extend_from_slice(&g.domain.to_be_bytes());
    out.extend_from_slice(&g.server.to_be_bytes());
    out.extend_from_slice(&g.seq.to_be_bytes());
}

impl ResumeToken {
    /// The binary form.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + 16 * self.pos.len());
        let mut flags = 0;
        if self.progress {
            flags |= FLAG_PROGRESS;
        }
        if self.until.is_some() {
            flags |= FLAG_UNTIL;
        }
        out.push(VERSION);
        out.push(flags);
        out.extend_from_slice(&self.tenant.0.to_be_bytes());
        out.extend_from_slice(&self.clamp.to_be_bytes());
        out.extend_from_slice(&self.segment.to_be_bytes());
        let c = self.cluster.as_str().as_bytes();
        out.push(u8::try_from(c.len()).unwrap_or(u8::MAX));
        out.extend_from_slice(&c[..c.len().min(255)]);
        if let Some((g, row)) = &self.until {
            put_gtid(&mut out, g);
            out.extend_from_slice(&row.to_be_bytes());
        }
        out.push(u8::try_from(self.pos.len()).unwrap_or(u8::MAX));
        for g in self.pos.iter().take(255) {
            put_gtid(&mut out, g);
        }
        out
    }

    /// Parses the binary form.
    ///
    /// # Errors
    /// 39801 for anything malformed.
    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = Reader { b };
        if r.u8()? != VERSION {
            return Err(bad("unknown resume token version"));
        }
        let flags = r.u8()?;
        if flags & !(FLAG_PROGRESS | FLAG_UNTIL) != 0 {
            return Err(bad("unknown resume token flags"));
        }
        let tenant = TenantId(r.u64()?);
        let clamp = r.u64()?;
        let segment = r.u64()?;
        let n = usize::from(r.u8()?);
        let cluster = std::str::from_utf8(r.take(n)?)
            .ok()
            .and_then(|s| ClusterId::new(s).ok())
            .ok_or_else(|| bad("resume token names no valid cluster"))?;
        let until = if flags & FLAG_UNTIL != 0 {
            Some((r.gtid()?, r.u32()?))
        } else {
            None
        };
        let n = r.u8()?;
        let mut pos = GtidPos::new();
        for _ in 0..n {
            let g = r.gtid()?;
            if pos.get(g.domain).is_some() {
                return Err(bad("resume token names a domain twice"));
            }
            pos.update(g);
        }
        if !r.b.is_empty() {
            return Err(bad("resume token has trailing bytes"));
        }
        Ok(Self {
            tenant,
            clamp,
            cluster,
            segment,
            pos,
            until,
            progress: flags & FLAG_PROGRESS != 0,
        })
    }

    /// The hex text form.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::new();
        for b in self.encode() {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Parses the hex text form.
    ///
    /// # Errors
    /// 39801 for anything malformed.
    pub fn from_hex(s: &str) -> Result<Self> {
        if !s.len().is_multiple_of(2) || s.len() > 4096 {
            return Err(bad("resume token isn't hex"));
        }
        let bytes = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(s.get(i..i + 2).unwrap_or("x"), 16))
            .collect::<std::result::Result<Vec<u8>, _>>()
            .map_err(|_| bad("resume token isn't hex"))?;
        Self::decode(&bytes)
    }

    /// Checks the token belongs to `tenant`.
    ///
    /// # Errors
    /// 39801 if it is another tenant's.
    pub fn check_tenant(&self, tenant: TenantId) -> Result<()> {
        if self.tenant == tenant {
            Ok(())
        } else {
            Err(bad("resume token belongs to another tenant"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(until: Option<(Gtid, u32)>, progress: bool) -> ResumeToken {
        ResumeToken {
            tenant: TenantId(42),
            clamp: 0x0000_6800_0000_0003,
            cluster: ClusterId::new("dc-a-c2").unwrap(),
            segment: 7,
            pos: "0-301-28,3-301-41".parse().unwrap(),
            until,
            progress,
        }
    }

    #[test]
    fn round_trips() {
        let g = Gtid {
            domain: 3,
            server: 301,
            seq: 42,
        };
        for t in [tok(Some((g, 5)), false), tok(None, true), tok(None, false)] {
            assert_eq!(ResumeToken::decode(&t.encode()).unwrap(), t);
            assert_eq!(ResumeToken::from_hex(&t.to_hex()).unwrap(), t);
        }
        assert!(tok(None, false).encode().len() < 96);
    }

    #[test]
    fn refuses_garbage_and_other_tenants() {
        let t = tok(None, false);
        let b = t.encode();
        for n in 0..b.len() {
            assert!(ResumeToken::decode(&b[..n]).is_err(), "prefix {n}");
        }
        let mut extra = b.clone();
        extra.push(0);
        assert!(ResumeToken::decode(&extra).is_err());
        let mut v2 = b;
        v2[0] = 2;
        assert!(ResumeToken::decode(&v2).is_err());
        assert!(ResumeToken::from_hex("zz").is_err());
        assert!(t.check_tenant(TenantId(42)).is_ok());
        let e = t.check_tenant(TenantId(43)).unwrap_err();
        assert!(matches!(
            e.kind,
            sprig_common::ErrorKind::Sprig(SprigCode::FeedBadResumeToken)
        ));
    }
}
