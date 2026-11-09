//! The copy loop's acknowledgement protocol against a model feed and a
//! model maintainer: writers commit changes to random keys of three
//! tenants, the worker is killed at random points (its future dropped, so
//! nothing is acknowledged on the way out) and started again with a higher
//! epoch, and at the end every key's copy equals its source. Deterministic
//! per seed (tokio's paused clock, a seeded generator).

use std::collections::BTreeMap;

use sprig_binlog::GtidPos;
use sprig_common::ids::ClusterId;
use sprig_common::ids::{DbId, GroupId, ShardId};
use sprig_feed::event::{ChangeEvent, EventId, Image, ShardRef};

use super::*;

/// One tenant's committed changes: (key, version).
#[derive(Default)]
struct Log {
    events: Vec<(i64, u64)>,
}

#[derive(Default)]
struct World {
    logs: BTreeMap<TenantId, Log>,
    /// The newest version of every key (the source).
    source: BTreeMap<(TenantId, i64), u64>,
    /// Acknowledged positions: next index to read.
    acks: BTreeMap<TenantId, usize>,
    /// What the copy holds: (version, epoch that wrote it).
    copy: BTreeMap<(TenantId, i64), (u64, u64)>,
    /// Deliveries of each (tenant, index).
    applied: BTreeMap<(TenantId, usize), u32>,
}

type Shared = Arc<Mutex<World>>;

fn token(t: TenantId, next: usize) -> ResumeToken {
    ResumeToken {
        tenant: t,
        clamp: next as u64,
        cluster: ClusterId::new("sim-c1").unwrap(),
        segment: 0,
        pos: GtidPos::default(),
        until: None,
        progress: true,
    }
}

struct ModelTail {
    w: Shared,
    t: TenantId,
    next: usize,
}

#[async_trait]
impl Tail for ModelTail {
    async fn next(&mut self) -> Result<FeedItem> {
        {
            let ev = {
                let mut w = self.w.lock().unwrap();
                let ev = w.logs[&self.t].events.get(self.next).copied();
                if ev.is_some() {
                    *w.applied.entry((self.t, self.next)).or_default() += 1;
                }
                ev
            };
            if let Some((k, _)) = ev {
                {
                    self.next += 1;
                    let mut tok = token(self.t, self.next);
                    tok.progress = false;
                    return Ok(FeedItem::Event(Box::new(ChangeEvent {
                        id: EventId {
                            cluster: tok.cluster.clone(),
                            gtid: sprig_binlog::Gtid {
                                domain: 0,
                                server: 1,
                                seq: self.next as u64,
                            },
                            row: 0,
                        },
                        token: tok,
                        tenant: self.t,
                        db: DbId(1),
                        db_name: "shop".into(),
                        object: 1,
                        table: "products".into(),
                        shard: ShardRef {
                            group: GroupId(1),
                            shard: ShardId(1),
                        },
                        key: Image(vec![("id".into(), sprig_binlog::Value::Int(k))]),
                        op: Op::Update,
                        before: None,
                        after: None,
                        delta: None,
                        hlc: self.next as u64,
                        txn: sprig_binlog::Gtid {
                            domain: 0,
                            server: 1,
                            seq: self.next as u64,
                        },
                        ddl: None,
                    })));
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(FeedItem::Progress {
                token: token(self.t, self.next),
            })
        }
    }
}

struct ModelFeeds(Shared);

#[async_trait]
impl Feeds for ModelFeeds {
    async fn open(&self, _root: TenantId, tenant: TenantId) -> Result<Box<dyn Tail>> {
        let next = self
            .0
            .lock()
            .unwrap()
            .acks
            .get(&tenant)
            .copied()
            .unwrap_or(0);
        Ok(Box::new(ModelTail {
            w: Arc::clone(&self.0),
            t: tenant,
            next,
        }))
    }

    async fn ack(
        &self,
        _root: TenantId,
        tenant: TenantId,
        token: &ResumeToken,
        _n: u64,
    ) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        let mut w = self.0.lock().unwrap();
        // What was delivered before this position was applied.
        let _ = &w.copy;
        let next = usize::try_from(token.clamp).unwrap();
        let cur = w.acks.entry(tenant).or_default();
        assert!(next >= *cur, "acknowledged positions never go back");
        *cur = next;
        Ok(())
    }
}

struct ModelApply {
    w: Shared,
    epoch: u64,
    tenants: Vec<TenantId>,
}

#[async_trait]
impl Apply for ModelApply {
    async fn refresh(&mut self) -> Result<Vec<TenantId>> {
        Ok(self.tenants.clone())
    }

    fn key_of(&self, _t: TenantId, _db: &str, _table: &str) -> Option<Vec<String>> {
        Some(vec!["id".into()])
    }

    async fn apply(&mut self, t: TenantId, _db: &str, _table: &str, keys: &[KeyLit]) -> Result<()> {
        for k in keys {
            // A read, then a write a little later (a kill can fall between).
            let key: i64 = k.0[0].trim_matches('\'').parse().unwrap();
            let read = self
                .w
                .lock()
                .unwrap()
                .source
                .get(&(t, key))
                .copied()
                .unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(1)).await;
            let mut w = self.w.lock().unwrap();
            let e = w.copy.entry((t, key)).or_insert((0, 0));
            // The guard: a lower epoch never overwrites a higher one.
            if self.epoch >= e.1 {
                *e = (read, self.epoch);
            }
        }
        Ok(())
    }

    async fn caught_up(&mut self, _at_ms: u64) -> Result<()> {
        Ok(())
    }
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

async fn one_seed(seed: u64) -> (u64, u32) {
    let tenants = vec![TenantId(1), TenantId(2), TenantId(3)];
    let w: Shared = Arc::default();
    for t in &tenants {
        w.lock().unwrap().logs.insert(*t, Log::default());
    }
    let mut g = Lcg(seed);
    // The writers: 300 changes over about 6 s.
    let writer = {
        let w = Arc::clone(&w);
        let tenants = tenants.clone();
        let mut g = Lcg(seed ^ 0xabc);
        tokio::spawn(async move {
            for v in 1..=300u64 {
                tokio::time::sleep(Duration::from_millis(g.below(40))).await;
                let t = tenants[usize::try_from(g.below(3)).unwrap()];
                let k = i64::try_from(g.below(20)).unwrap();
                let mut w = w.lock().unwrap();
                w.source.insert((t, k), v);
                w.logs.get_mut(&t).unwrap().events.push((k, v));
            }
        })
    };
    let opts = ServeOpts {
        ack_every: Duration::from_millis(100),
        ..ServeOpts::default()
    };
    let mut epoch = 0;
    let mut kills = 0u32;
    loop {
        epoch += 1;
        let feeds: Arc<dyn Feeds> = Arc::new(ModelFeeds(Arc::clone(&w)));
        let mut apply = ModelApply {
            w: Arc::clone(&w),
            epoch,
            tenants: tenants.clone(),
        };
        let stop = CancellationToken::new();
        let life = Duration::from_millis(200 + g.below(1500));
        let done = writer.is_finished();
        let mut noop = |_: ServeStats| {};
        let run = serve(TenantId(1), feeds, &mut apply, &opts, &stop, &mut noop);
        if done {
            // The last run: drain, then stop cleanly.
            let s2 = stop.clone();
            let w2 = Arc::clone(&w);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let w = w2.lock().unwrap();
                    if w.source
                        .iter()
                        .all(|(k, v)| w.copy.get(k).map(|c| c.0) == Some(*v))
                    {
                        s2.cancel();
                        return;
                    }
                }
            });
            run.await.unwrap();
            break;
        }
        // A kill: the future is dropped wherever it is.
        tokio::select! {
            r = run => { r.unwrap(); }
            () = tokio::time::sleep(life) => { kills += 1; }
        }
    }
    let w = w.lock().unwrap();
    for (k, v) in &w.source {
        assert_eq!(
            w.copy.get(k).map(|c| c.0),
            Some(*v),
            "seed {seed}: key {k:?} missed"
        );
    }
    for (t, log) in &w.logs {
        for i in 0..log.events.len() {
            assert!(
                w.applied.contains_key(&(*t, i)),
                "seed {seed}: event {t}/{i} never delivered"
            );
        }
    }
    let replays: u64 = w
        .applied
        .values()
        .map(|n| u64::from(n.saturating_sub(1)))
        .sum();
    (replays, kills)
}

#[tokio::test(start_paused = true)]
async fn worker_kills_lose_nothing() {
    let (mut total_kills, mut total_replays) = (0, 0);
    for seed in 0..40 {
        let (replays, kills) = one_seed(seed).await;
        total_kills += kills;
        total_replays += replays;
    }
    eprintln!("40 seeds: {total_kills} kills, {total_replays} events delivered again after a kill");
    assert!(total_kills > 40, "the runs were killed {total_kills} times");
}
