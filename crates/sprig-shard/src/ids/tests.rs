#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;

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
