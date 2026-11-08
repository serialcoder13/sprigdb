//! HLC stamps and the clamp (chapter 38, "The cluster reader").
//!
//! An HLC stamp has MongoDB's `Timestamp` layout: seconds since the Unix
//! epoch in the high 32 bits, a counter in the low 32. Every transaction
//! that yields events gets a **clamp**:
//!
//! ```text
//! clamp := max(previous clamp + 1, max(stamps of its statements) or header_ts << 32)
//! ```
//!
//! so clamps rise by at least one per transaction, in binlog order, and a
//! re-read from a token that stores the clamp yields the same clamps.

/// An HLC stamp: `seconds << 32 | counter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hlc(pub u64);

impl Hlc {
    /// The stamp at the start of second `secs`.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs << 32)
    }

    /// The seconds part.
    #[must_use]
    pub const fn secs(self) -> u64 {
        self.0 >> 32
    }
}

/// The clamp of the next transaction after one clamped `prev`, given the
/// largest statement stamp in it (if its statements carried comments) and
/// the event header's timestamp in seconds.
#[must_use]
pub fn next_clamp(prev: u64, stamps: Option<u64>, header_ts: u32) -> u64 {
    let base = stamps.unwrap_or(u64::from(header_ts) << 32);
    prev.saturating_add(1).max(base)
}

/// A floor above every clamp a source could have given by wall-clock time
/// `now_ms` (milliseconds since the Unix epoch), when stamps come from
/// event headers: the start of the next second. A move's handoff record
/// stores it ([`crate::ownership::Handoff::hlc_floor`]).
#[must_use]
pub fn floor_at(now_ms: u64) -> u64 {
    (now_ms / 1000 + 1) << 32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_rise_by_at_least_one() {
        assert_eq!(next_clamp(0, None, 5), 5 << 32);
        assert_eq!(next_clamp(5 << 32, None, 5), (5 << 32) + 1);
        assert_eq!(next_clamp((5 << 32) + 1, Some(7 << 32), 5), 7 << 32);
        assert_eq!(next_clamp(9 << 32, Some(7 << 32), 5), (9 << 32) + 1);
        assert_eq!(Hlc::from_secs(3).secs(), 3);
        assert_eq!(floor_at(4_999), 5 << 32);
    }
}
