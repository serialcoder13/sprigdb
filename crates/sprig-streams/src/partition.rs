//! Keys and partitions (chapter 45, "Keys and partitions").
//!
//! ```text
//! key present: (murmur2(key) & 0x7fffffff) mod partitions
//!              -- Kafka's default partitioner, so SQL, logic and Java
//!                 producers put one key on one partition
//! key absent:  (murmur2(event id) & 0x7fffffff) mod partitions
//! explicit:    a producer that names a partition is obeyed
//! ```
//!
//! **As built:** keyless records are spread by their event id, not round
//! robin per producer session. The partition is then fixed by the outbox
//! row itself, so a sequencer that re-reads a row after a crash gives it
//! the same partition (chapter 45 allows Kafka's sticky behaviour; this
//! is the same spread without session state).

use crate::model::PartitionNo;

/// Kafka's `Utils.murmur2` (seed `0x9747b28c`), bit for bit.
#[must_use]
#[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let len = data.len();
    let mut h: u32 = 0x9747_b28c ^ (len as u32);
    let mut chunks = data.chunks_exact(4);
    for c in &mut chunks {
        let mut k = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let rest = chunks.remainder();
    if rest.len() >= 3 {
        h ^= u32::from(rest[2]) << 16;
    }
    if rest.len() >= 2 {
        h ^= u32::from(rest[1]) << 8;
    }
    if !rest.is_empty() {
        h ^= u32::from(rest[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// Kafka's `toPositive(murmur2(key)) % partitions`.
#[must_use]
#[allow(clippy::cast_sign_loss)]
pub fn for_key(key: &[u8], partitions: u32) -> PartitionNo {
    let h = (murmur2(key) & 0x7fff_ffff) as u32;
    PartitionNo(h % partitions.max(1))
}

/// The partition of a record: `explicit` (a producer's own choice, taken
/// modulo `partitions` in case partitions shrank, which they can't), else
/// by key, else by the record's event id.
#[must_use]
pub fn of_record(
    explicit: Option<u32>,
    key: Option<&[u8]>,
    event_id: &[u8],
    partitions: u32,
) -> PartitionNo {
    if let Some(p) = explicit {
        return PartitionNo(p % partitions.max(1));
    }
    match key {
        Some(k) => for_key(k, partitions),
        None => for_key(event_id, partitions),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kafka's own vectors (`UtilsTest.testMurmur2`).
    #[test]
    fn murmur2_matches_kafka() {
        let cases: &[(&[u8], i32)] = &[
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ];
        for (input, want) in cases {
            assert_eq!(
                murmur2(input),
                *want,
                "{}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn partitions() {
        let p = for_key(b"7001", 12);
        assert!(p.0 < 12);
        assert_eq!(for_key(b"7001", 12), p, "stable");
        assert_eq!(of_record(Some(14), Some(b"x"), b"id", 12), PartitionNo(2));
        assert_eq!(of_record(None, None, b"id", 12), for_key(b"id", 12));
        assert_eq!(for_key(b"x", 0), PartitionNo(0));
    }
}
