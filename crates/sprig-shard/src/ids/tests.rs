#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use super::*;

/// An in-memory `sequences` row.
#[derive(Default)]
struct MemSource {
    next: StdMutex<i128>,
    strict: bool,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl SequenceSource for Arc<MemSource> {
    async fn fetch(&self, _seq: SeqId, size: u128) -> Result<(i128, i128)> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(SprigError::new(SprigCode::IdBlockUnavailable, "down"));
        }
        tokio::task::yield_now().await;
        let mut n = self.next.lock().unwrap();
        let start = (*n).max(1);
        let end = start + i128::try_from(size).unwrap();
        *n = end;
        Ok((start, end))
    }
    async fn raise(&self, _seq: SeqId, v: i128) -> Result<()> {
        let mut n = self.next.lock().unwrap();
        *n = (*n).max(v + 1);
        Ok(())
    }
    async fn ordering(&self, _seq: SeqId) -> Result<Ordering> {
        Ok(if self.strict {
            Ordering::Strict
        } else {
            Ordering::Blocks
        })
    }
}

#[test]
fn series() {
    let s = AutoIncSession {
        increment: 10,
        offset: 3,
    };
    assert_eq!(s.first_at_or_after(1), 3);
    assert_eq!(s.first_at_or_after(3), 3);
    assert_eq!(s.first_at_or_after(4), 13);
    assert_eq!(s.first_at_or_after(24), 33);
    // An offset above the increment is ignored, as in MySQL.
    let s = AutoIncSession {
        increment: 2,
        offset: 5,
    };
    assert_eq!(s.first_at_or_after(2), 3);
}

#[tokio::test]
async fn blocks_are_consecutive_and_unique_across_gateways() {
    let src = Arc::new(MemSource::default());
    let cfg = BlockConfig {
        min: 16,
        max: 1024,
        target: Duration::from_secs(1),
    };
    let gws: Vec<_> = (0..4)
        .map(|_| BlockAllocator::new(Arc::clone(&src), cfg))
        .collect();
    let mut tasks = Vec::new();
    for (g, a) in gws.iter().enumerate() {
        for t in 0..8 {
            let a = a.clone();
            tasks.push(tokio::spawn(async move {
                let s = AutoIncSession::default();
                let mut got = Vec::new();
                for i in 0..200u64 {
                    let count = 1 + (i + t + g as u64) % 5;
                    let run = a.take(SeqId(1), count, &s).await.unwrap();
                    assert_eq!(run.count, count);
                    let v: Vec<i128> = run.values().collect();
                    // Consecutive within a statement.
                    assert!(v.windows(2).all(|w| w[1] == w[0] + 1));
                    got.extend(v);
                }
                got
            }));
        }
    }
    let mut all = HashSet::new();
    let mut n = 0;
    for t in tasks {
        for v in t.await.unwrap() {
            n += 1;
            assert!(all.insert(v), "duplicate {v}");
        }
    }
    assert_eq!(all.len(), n);
    // Increasing per gateway.
    let s = AutoIncSession::default();
    let a = &gws[0];
    let x = a.take(SeqId(1), 1, &s).await.unwrap().first;
    let y = a.take(SeqId(1), 1, &s).await.unwrap().first;
    assert!(y > x);
}

#[tokio::test]
async fn increment_offset_strict_raise_and_outage() {
    let src = Arc::new(MemSource::default());
    let a = BlockAllocator::new(Arc::clone(&src), BlockConfig::default());
    let s = AutoIncSession {
        increment: 5,
        offset: 2,
    };
    let run = a.take(SeqId(1), 3, &s).await.unwrap();
    assert_eq!(run.values().collect::<Vec<_>>(), [2, 7, 12]);
    assert!(run.values().all(|v| (v - 2) % 5 == 0));
    // ALTER TABLE ... AUTO_INCREMENT = 1000 drops cached values below it.
    a.invalidate_below(SeqId(1), 1000).await;
    let r = a
        .take(SeqId(1), 1, &AutoIncSession::default())
        .await
        .unwrap();
    assert!(r.first >= 1000);
    a.raise(SeqId(1), 5000).await.unwrap();
    assert!(*src.next.lock().unwrap() >= 5001);
    // Cached and prefetched blocks keep working while the source is down.
    src.fail.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut served = 0;
    let err = loop {
        match a.take(SeqId(1), 1, &AutoIncSession::default()).await {
            Ok(_) => served += 1,
            Err(e) => break e,
        }
    };
    assert!(served > 0);
    assert_eq!(err.code(), Some(SprigCode::IdBlockUnavailable));
    // Strict: every statement goes to the source, values increase in
    // allocation order across allocators.
    let strict = Arc::new(MemSource {
        strict: true,
        ..MemSource::default()
    });
    let g1 = BlockAllocator::new(Arc::clone(&strict), BlockConfig::default());
    let g2 = BlockAllocator::new(Arc::clone(&strict), BlockConfig::default());
    let s = AutoIncSession::default();
    let mut last = 0;
    for i in 0..20 {
        let g = if i % 2 == 0 { &g1 } else { &g2 };
        let v = g.take(SeqId(1), 2, &s).await.unwrap();
        assert!(v.first > last);
        last = v.last();
    }
}

#[tokio::test]
async fn explicit_values_keep_later_values_above_them() {
    let src = Arc::new(MemSource::default());
    let a = BlockAllocator::new(Arc::clone(&src), BlockConfig::default());
    let s = AutoIncSession::default();
    let first = a.take(SeqId(1), 1, &s).await.unwrap().first;
    assert_eq!(first, 1);
    let held_end = *src.next.lock().unwrap();
    // Inside the block this gateway holds: no catalog raise, but the next
    // value is above it, as MariaDB's counter would be.
    a.explicit(SeqId(1), 5).await.unwrap();
    assert_eq!(*src.next.lock().unwrap(), held_end);
    assert_eq!(a.take(SeqId(1), 1, &s).await.unwrap().first, 6);
    // Past every block: the catalog is raised, once per new maximum.
    a.explicit(SeqId(1), 1_000_000).await.unwrap();
    assert_eq!(*src.next.lock().unwrap(), 1_000_001);
    *src.next.lock().unwrap() = 1_000_001; // unchanged by a lower value
    a.explicit(SeqId(1), 999_999).await.unwrap();
    assert_eq!(*src.next.lock().unwrap(), 1_000_001);
    let run = a.take(SeqId(1), 2, &s).await.unwrap();
    assert_eq!(run.first, 1_000_001);
    assert_eq!(run.last(), 1_000_002);
    // Another gateway's later block starts past the raise too.
    let b = BlockAllocator::new(Arc::clone(&src), BlockConfig::default());
    assert!(b.take(SeqId(1), 1, &s).await.unwrap().first > 1_000_002);
}

/// A recorded block: `[start, end)`, its holder and its lease's end.
type HeldBlock = (i128, i128, &'static str, std::time::Instant);

/// A gateway's view of a shared [`MemSource`]: its blocks are recorded
/// under its name and a lease, as `CatalogSequences::held_by` records them.
struct Held {
    src: Arc<MemSource>,
    name: &'static str,
    lease: Duration,
    blocks: Arc<StdMutex<Vec<HeldBlock>>>,
}

#[async_trait::async_trait]
impl SequenceSource for Held {
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let r = self.src.fetch(seq, size).await?;
        self.blocks.lock().unwrap().push((
            r.0,
            r.1,
            self.name,
            std::time::Instant::now() + self.lease,
        ));
        Ok(r)
    }
    async fn raise(&self, seq: SeqId, v: i128) -> Result<()> {
        self.src.raise(seq, v).await
    }
    async fn ordering(&self, seq: SeqId) -> Result<Ordering> {
        self.src.ordering(seq).await
    }
    fn lease(&self) -> Option<Duration> {
        Some(self.lease)
    }
    async fn explicit(&self, seq: SeqId, v: i128) -> Result<Explicit> {
        let next = *self.src.next.lock().unwrap();
        if v >= next {
            self.src.raise(seq, v).await?;
            return Ok(Explicit::Free);
        }
        let now = std::time::Instant::now();
        Ok(self
            .blocks
            .lock()
            .unwrap()
            .iter()
            .find(|(a, z, h, until)| *a <= v && v < *z && *h != self.name && now < *until)
            .map_or(Explicit::Free, |(_, z, _, _)| Explicit::Reserved {
                end: *z,
            }))
    }
}

#[tokio::test]
async fn explicit_values_in_another_gateways_block_are_refused_until_its_lease_ends() {
    let src = Arc::new(MemSource::default());
    let blocks = Arc::new(StdMutex::new(Vec::new()));
    let lease = Duration::from_millis(300);
    let gw = |name| {
        BlockAllocator::new(
            Held {
                src: Arc::clone(&src),
                name,
                lease,
                blocks: Arc::clone(&blocks),
            },
            BlockConfig::default(),
        )
    };
    let (a, b) = (gw("a"), gw("b"));
    let s = AutoIncSession::default();
    // B holds [1, 17) and has used 1.
    assert_eq!(b.take(SeqId(1), 1, &s).await.unwrap().first, 1);
    // A value inside B's block: B could still generate it, so A refuses.
    let e = a.explicit(SeqId(1), 10).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::IdValueReserved));
    // A value in A's own block is A's to skip; one past every block raises.
    let own = a.take(SeqId(1), 1, &s).await.unwrap().first;
    a.explicit(SeqId(1), own + 3).await.unwrap();
    assert!(a.take(SeqId(1), 1, &s).await.unwrap().first > own + 3);
    a.explicit(SeqId(1), 5000).await.unwrap();
    // B never hands out a value A wrote: its block ends below 5000, its next
    // starts above it.
    let mut seen = Vec::new();
    for _ in 0..40 {
        seen.push(b.take(SeqId(1), 1, &s).await.unwrap().first);
    }
    assert!(seen.iter().all(|v| *v != 5000 && *v != own + 3));
    assert!(seen.windows(2).all(|w| w[0] < w[1]));
    // After B's lease ends, the rest of its block is a gap, and a value in
    // it may be written.
    let last = *seen.last().unwrap();
    tokio::time::sleep(lease + Duration::from_millis(50)).await;
    a.explicit(SeqId(1), last + 1).await.unwrap();
    assert!(b.take(SeqId(1), 1, &s).await.unwrap().first > last + 1);
}
