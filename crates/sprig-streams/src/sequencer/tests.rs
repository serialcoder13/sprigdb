#![allow(clippy::unwrap_used, clippy::cast_possible_truncation)]

use std::sync::atomic::AtomicUsize;

use sprig_binlog::{Gtid, Value};
use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_feed::{EventId, Image, ShardRef};

use super::*;
use crate::memlog::{Fault, MemLog};

const T: TenantId = TenantId(7);

fn c1() -> ClusterId {
    ClusterId::new("dc-a-c1").unwrap()
}

fn token(seq: u64) -> ResumeToken {
    ResumeToken {
        tenant: T,
        clamp: seq,
        cluster: c1(),
        segment: 0,
        pos: GtidPos::from_gtids([Gtid {
            domain: 1,
            server: 1,
            seq,
        }]),
        until: None,
        progress: false,
    }
}

/// An outbox insert of event `n` with key `key`.
fn outbox_event(seq: u64, row: u32, n: u8, key: &str) -> ChangeEvent {
    let mut id = [0u8; 16];
    id[0] = n;
    id[1] = (seq % 256) as u8;
    id[2] = row as u8;
    ChangeEvent {
        id: EventId {
            cluster: c1(),
            gtid: Gtid {
                domain: 1,
                server: 1,
                seq,
            },
            row,
        },
        token: token(seq),
        tenant: T,
        db: DbId(1),
        db_name: "shop".into(),
        object: 0,
        table: "__sprig_eo_5".into(),
        shard: ShardRef {
            group: GroupId(1),
            shard: ShardId(1),
        },
        key: Image(vec![("event_id".into(), Value::Bytes(id.to_vec()))]),
        op: Op::Insert,
        before: None,
        after: Some(Image(vec![
            ("event_id".into(), Value::Bytes(id.to_vec())),
            ("partition_no".into(), Value::Null),
            ("k".into(), Value::Bytes(key.as_bytes().to_vec())),
            (
                "v".into(),
                Value::Bytes(format!("v{seq}.{row}").into_bytes()),
            ),
            ("headers".into(), Value::Null),
        ])),
        delta: None,
        hlc: seq,
        txn: Gtid {
            domain: 1,
            server: 1,
            seq,
        },
        ddl: None,
    }
}

/// A binlog of transactions; seq is the index + 1.
#[derive(Clone)]
struct Script(Arc<Vec<Vec<ChangeEvent>>>);

struct ScriptFeed {
    txns: Arc<Vec<Vec<ChangeEvent>>>,
    next: usize,
    out: std::collections::VecDeque<FeedItem>,
}

#[async_trait::async_trait]
impl FeedSource for ScriptFeed {
    async fn next(&mut self) -> Result<FeedItem> {
        loop {
            if let Some(i) = self.out.pop_front() {
                return Ok(i);
            }
            if let Some(t) = self.txns.get(self.next) {
                self.next += 1;
                let seq = self.next as u64;
                for e in t {
                    self.out.push_back(FeedItem::Event(Box::new(e.clone())));
                }
                self.out.push_back(FeedItem::TxnEnd { token: token(seq) });
                continue;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(FeedItem::Progress {
                token: token(self.txns.len() as u64),
            });
        }
    }
}

#[async_trait::async_trait]
impl FeedOpener for Script {
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>> {
        let next = match start {
            Start::After(t) => t.pos.get(1).map_or(0, |g| g.seq as usize),
            Start::At { .. } => 0,
        };
        Ok(Box::new(ScriptFeed {
            txns: Arc::clone(&self.0),
            next,
            out: std::collections::VecDeque::new(),
        }))
    }
}

fn script(n: u64) -> Script {
    let keys = ["a", "b", "c", "d", "e"];
    let txns = (1..=n)
        .map(|seq| {
            (0..(seq % 3 + 1) as u32)
                .map(|row| outbox_event(seq, row, 9, keys[((seq + u64::from(row)) % 5) as usize]))
                .collect()
        })
        .collect();
    Script(Arc::new(txns))
}

fn sequencer(log: &MemLog, feed: &Script) -> Sequencer {
    Sequencer {
        filter: Filter::Outbox {
            db: DbId(1),
            table: "__sprig_eo_5".into(),
            partitions: 4,
        },
        owned: Vec::new(),
        store: Arc::new(log.clone()),
        feed: Arc::new(feed.clone()),
        marks: Arc::new(NoWatermarks),
        opts: SeqOpts::default(),
        stats: Arc::default(),
    }
}

fn start() -> Start {
    Start::At {
        cluster: c1(),
        pos: GtidPos::new(),
    }
}

/// Every record of the script once, in feed order per partition, with
/// offsets from 0 without gaps.
fn check(log: &MemLog, feed: &Script) {
    let mut want: BTreeMap<PartitionNo, Vec<Source>> = BTreeMap::new();
    for t in feed.0.iter() {
        for e in t {
            let (r, _) = outbox_record(e, 4).unwrap();
            want.entry(r.partition).or_default().push(r.source);
        }
    }
    for (p, sources) in want {
        let got = log.records(p);
        let offsets: Vec<u64> = got.iter().map(|s| s.offset.0).collect();
        assert_eq!(offsets, (0..sources.len() as u64).collect::<Vec<_>>());
        let got: Vec<Source> = got.into_iter().map(|s| s.record.source).collect();
        assert_eq!(got, sources, "partition {p:?} in feed order");
    }
}

async fn run_for(seq: &Sequencer, d: Duration) -> RunEnd {
    let stop = CancellationToken::new();
    let s2 = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(d).await;
        s2.cancel();
    });
    seq.run(&stop).await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn appends_in_feed_order_and_resumes() {
    let feed = script(40);
    let log = MemLog::new(&start());
    let s = sequencer(&log, &feed);
    assert_eq!(run_for(&s, Duration::from_secs(5)).await, RunEnd::Stopped);
    check(&log, &feed);
    let n = s.stats.appended.load(Ordering::Relaxed);
    assert_eq!(n, feed.0.iter().map(|t| t.len() as u64).sum::<u64>());
    // A restart resumes after the last batch: nothing appended twice.
    let s2 = sequencer(&log, &feed);
    run_for(&s2, Duration::from_secs(5)).await;
    assert_eq!(s2.stats.appended.load(Ordering::Relaxed), 0);
    assert_eq!(s2.stats.duplicates.load(Ordering::Relaxed), 0);
    check(&log, &feed);
    assert_eq!(log.lock().epoch, LeaderEpoch(2));
}

#[tokio::test(start_paused = true)]
async fn lost_answers_are_replayed_without_duplicates() {
    let feed = script(60);
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = Arc::clone(&n);
    let log = MemLog::new(&start()).with_faults(Arc::new(move || {
        match n2.fetch_add(1, Ordering::Relaxed) % 4 {
            1 => Fault::After,
            2 => Fault::Before,
            _ => Fault::None,
        }
    }));
    let mut s = sequencer(&log, &feed);
    s.opts.max_batch = 4;
    run_for(&s, Duration::from_secs(20)).await;
    check(&log, &feed);
    assert!(
        s.stats.duplicates.load(Ordering::Relaxed) > 0,
        "a replay happened"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stale_sequencer_is_fenced() {
    let feed = script(10);
    let log = MemLog::new(&start());
    let old = sequencer(&log, &feed);
    let stop = CancellationToken::new();
    let old_stop = stop.clone();
    // The old sequencer acquires, then a new one takes over before it
    // writes anything.
    let old_run = tokio::spawn(async move { old.run(&old_stop).await });
    tokio::task::yield_now().await;
    let new = sequencer(&log, &feed);
    let new_end = run_for(&new, Duration::from_secs(3)).await;
    assert_eq!(new_end, RunEnd::Stopped);
    let end = tokio::time::timeout(Duration::from_secs(5), old_run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end, RunEnd::Fenced);
    stop.cancel();
    check(&log, &feed);
}

#[tokio::test(start_paused = true)]
async fn owned_partitions_only() {
    let feed = script(30);
    let log = MemLog::new(&start());
    let mut s = sequencer(&log, &feed);
    s.owned = vec![PartitionNo(1)];
    run_for(&s, Duration::from_secs(5)).await;
    let st = log.lock();
    assert!(st.parts.keys().all(|p| *p == PartitionNo(1)));
}

#[test]
fn starts_round_trip() {
    let a = start();
    assert_eq!(Start::decode(&a.encode()).unwrap(), a);
    let b = Start::After(token(9));
    assert_eq!(Start::decode(&b.encode()).unwrap(), b);
    assert!(Start::decode(b"").is_err());
}
