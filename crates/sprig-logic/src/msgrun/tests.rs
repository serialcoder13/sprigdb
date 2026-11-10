//! The runners over an in-memory stream, group offsets and gateway that
//! commit a run's effect and its offset together, as the gateway's
//! transaction does.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use sprig_common::SprigError;

use super::*;
use crate::declare::SourceKind;
use crate::messages::SourceRow;

/// The world: a stream's records, every group's offsets, every trigger's
/// committed effects, dead letters.
#[derive(Default)]
struct World {
    /// Partition → records (offset order).
    log: BTreeMap<u32, Vec<RecordIn>>,
    /// (group, partition) → next offset.
    offsets: HashMap<(u64, u32), u64>,
    /// (binding, partition, offset) in commit order.
    effects: Vec<(u64, u32, u64)>,
    /// (binding, partition, offset).
    dead: Vec<(u64, u32, u64)>,
    /// Records whose code always fails.
    poison: HashSet<(u32, u64)>,
    /// Runs counted, to fail some transiently.
    runs: u64,
    /// Every n-th run fails transiently (0: never).
    transient_every: u64,
    /// Every n-th committed run loses its answer (0: never).
    lose_every: u64,
}

struct Fake(Arc<Mutex<World>>);

fn w(f: &Arc<Mutex<World>>) -> std::sync::MutexGuard<'_, World> {
    f.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[async_trait]
impl Gateway for Fake {
    async fn run(&self, req: &RunRequest) -> Result<RunReply> {
        tokio::task::yield_now().await;
        let b = req.source.binding_id;
        let r = req.record.clone().expect("a record");
        let mut g = w(&self.0);
        g.runs += 1;
        let key = (b, r.partition);
        if g.offsets.get(&key).copied().unwrap_or(0) != r.offset {
            return Ok(RunReply::of(RunOutcome::Moved));
        }
        if g.transient_every > 0 && g.runs.is_multiple_of(g.transient_every) {
            return Ok(RunReply::failed(&SprigError::backend(
                1213,
                *b"40001",
                "Deadlock found",
            )));
        }
        if g.poison.contains(&(r.partition, r.offset)) {
            return Ok(RunReply::failed(&SprigError::new(
                sprig_common::SprigCode::LogicUserError,
                "boom",
            )));
        }
        // The transaction: the effect and the offset commit together.
        g.effects.push((b, r.partition, r.offset));
        g.offsets.insert(key, r.offset + 1);
        if g.lose_every > 0 && g.runs.is_multiple_of(g.lose_every) {
            return Err(SprigError::new(
                sprig_common::SprigCode::ForwardFailed,
                "the answer was lost",
            ));
        }
        Ok(RunReply::of(RunOutcome::Ok))
    }
}

struct Side {
    w: Arc<Mutex<World>>,
    binding: u64,
}

#[async_trait]
impl StreamSide for Side {
    async fn positions(&self) -> Result<HashMap<u32, u64>> {
        let g = w(&self.w);
        Ok(g.offsets
            .iter()
            .filter(|((b, _), _)| *b == self.binding)
            .map(|((_, p), o)| (*p, *o))
            .collect())
    }
    async fn bounds(&self) -> Result<HashMap<u32, (u64, u64)>> {
        let g = w(&self.w);
        Ok(g.log
            .iter()
            .map(|(p, v)| (*p, (0, v.last().map_or(0, |r| r.offset + 1))))
            .collect())
    }
    async fn read(&self, p: u32, from: u64, max: u32) -> Result<Vec<RecordIn>> {
        let g = w(&self.w);
        Ok(g.log
            .get(&p)
            .map(|v| {
                v.iter()
                    .filter(|r| r.offset >= from)
                    .take(max as usize)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
    async fn init(&self, p: u32, at: u64) -> Result<()> {
        w(&self.w).offsets.entry((self.binding, p)).or_insert(at);
        Ok(())
    }
    async fn advance(&self, p: u32, expect: u64, to: u64) -> Result<bool> {
        let mut g = w(&self.w);
        let k = (self.binding, p);
        if g.offsets.get(&k).copied().unwrap_or(0) == expect {
            g.offsets.insert(k, to);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    async fn dead_letter(&self, r: &RecordIn, _attempts: u32, _last: &RunReply) -> Result<()> {
        w(&self.w).dead.push((self.binding, r.partition, r.offset));
        Ok(())
    }
}

fn template(binding: u64) -> Template {
    Template {
        req: RunRequest {
            tenant: 1,
            source: SourceRow {
                binding_id: binding,
                module_id: 10,
                kind: SourceKind::Stream,
                db_id: 1,
                db_name: "security".into(),
                name: "user_events".into(),
                object_id: 41,
                definer_user_id: 7,
                tenant_name: "system".into(),
            },
            version: 1,
            record: None,
            max_attempts: 3,
            lease_ms: 30_000,
            budget_ms: 30_000,
            instance_tenant: 0,
            instance_stream: 0,
            replay: None,
        },
        version: Arc::new(AtomicU64::new(1)),
    }
}

fn opts() -> RunOpts {
    RunOpts {
        lanes: 4,
        max_attempts: 3,
        retry_initial: Duration::from_millis(1),
        retry_max: Duration::from_millis(4),
        ..RunOpts::default()
    }
}

fn fill(g: &mut World, partitions: u32, per: u64) {
    for p in 0..partitions {
        g.log.insert(
            p,
            (0..per)
                .map(|o| RecordIn {
                    partition: p,
                    offset: o,
                    ts_ms: 0,
                    key: Some(format!("k{p}").into_bytes()),
                    value: Some(format!("v{p}:{o}").into_bytes()),
                    headers: Vec::new(),
                })
                .collect(),
        );
    }
}

async fn run_for(world: &Arc<Mutex<World>>, binding: u64, cancel: CancellationToken) {
    let side: Arc<dyn StreamSide> = Arc::new(Side {
        w: Arc::clone(world),
        binding,
    });
    let n = u32::try_from(w(world).log.len()).unwrap();
    for p in 0..n {
        side.init(p, 0).await.unwrap();
    }
    let _ = run_stream(
        side,
        Arc::new(Fake(Arc::clone(world))),
        template(binding),
        opts(),
        Arc::new(MsgStats::default()),
        n,
        Duration::from_millis(5),
        cancel,
    )
    .await;
}

fn done(world: &Arc<Mutex<World>>, binding: u64, total: u64) -> bool {
    let g = w(world);
    let n = g.effects.iter().filter(|e| e.0 == binding).count() as u64
        + g.dead.iter().filter(|e| e.0 == binding).count() as u64;
    n >= total
}

/// Each binding's effects: every record once, in each partition's order.
fn check_once_in_order(world: &Arc<Mutex<World>>, binding: u64, partitions: u32, per: u64) {
    let g = w(world);
    for p in 0..partitions {
        let mine: Vec<u64> = g
            .effects
            .iter()
            .filter(|e| e.0 == binding && e.1 == p)
            .map(|e| e.2)
            .collect();
        let dead: Vec<u64> = g
            .dead
            .iter()
            .filter(|e| e.0 == binding && e.1 == p)
            .map(|e| e.2)
            .collect();
        let mut all: Vec<u64> = mine.iter().chain(&dead).copied().collect();
        all.sort_unstable();
        assert_eq!(
            all,
            (0..per).collect::<Vec<_>>(),
            "binding {binding} partition {p}: every record once"
        );
        let mut sorted = mine.clone();
        sorted.sort_unstable();
        assert_eq!(mine, sorted, "binding {binding} partition {p}: in order");
    }
}

#[tokio::test]
async fn two_triggers_each_get_every_record_once_across_kills() {
    let world = Arc::new(Mutex::new(World::default()));
    {
        let mut g = w(&world);
        fill(&mut g, 4, 60);
        g.transient_every = 7;
        g.lose_every = 11;
    }
    let total = 4 * 60;
    // Two triggers (bindings 1 and 2), each killed and restarted until
    // done; sometimes a second runner of the same binding runs at once
    // (a worker that hasn't noticed it lost its job).
    let mut kills = 0;
    for round in 0..200u64 {
        if done(&world, 1, total) && done(&world, 2, total) {
            break;
        }
        let mut hs = Vec::new();
        for b in [1u64, 2] {
            let c = CancellationToken::new();
            let w2 = Arc::clone(&world);
            let c2 = c.clone();
            hs.push((c, tokio::spawn(async move { run_for(&w2, b, c2).await })));
            if round % 3 == 0 {
                let c = CancellationToken::new();
                let w2 = Arc::clone(&world);
                let c2 = c.clone();
                hs.push((c, tokio::spawn(async move { run_for(&w2, b, c2).await })));
            }
        }
        tokio::time::sleep(Duration::from_millis(2 + round % 5)).await;
        for (c, h) in hs {
            c.cancel();
            let _ = h.await;
            kills += 1;
        }
    }
    assert!(kills > 10, "runners were killed {kills} times");
    check_once_in_order(&world, 1, 4, 60);
    check_once_in_order(&world, 2, 4, 60);
    assert!(w(&world).dead.is_empty());
}

#[tokio::test]
async fn a_failing_record_becomes_a_dead_letter_and_the_partition_moves_on() {
    let world = Arc::new(Mutex::new(World::default()));
    {
        let mut g = w(&world);
        fill(&mut g, 2, 20);
        g.poison.insert((1, 5));
        g.transient_every = 5;
    }
    let c = CancellationToken::new();
    let w2 = Arc::clone(&world);
    let c2 = c.clone();
    let h = tokio::spawn(async move { run_for(&w2, 9, c2).await });
    for _ in 0..500 {
        if done(&world, 9, 40) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    c.cancel();
    let _ = h.await;
    check_once_in_order(&world, 9, 2, 20);
    let g = w(&world);
    assert_eq!(g.dead, vec![(9, 1, 5)]);
    // The poisoned record ran max_attempts times, the others once (plus
    // transient retries, which don't count).
    assert_eq!(g.offsets.get(&(9, 1)), Some(&20));
}

/// A queue whose messages a fake gateway hands out one at a time.
struct QueueGw {
    q: Arc<Mutex<QueueWorld>>,
}

#[derive(Default)]
struct QueueWorld {
    /// (id, failed deliveries) ready.
    ready: Vec<(u64, u32)>,
    done: Vec<u64>,
    dead: Vec<(u64, u32)>,
    poison: HashSet<u64>,
}

#[async_trait]
impl Gateway for QueueGw {
    async fn run(&self, req: &RunRequest) -> Result<RunReply> {
        tokio::task::yield_now().await;
        let mut q = self.q.lock().unwrap();
        if q.ready.is_empty() {
            return Ok(RunReply::of(RunOutcome::Empty));
        }
        let (id, failed) = q.ready.remove(0);
        if q.poison.contains(&id) {
            if failed + 1 >= req.max_attempts {
                q.dead.push((id, failed + 1));
                return Ok(RunReply {
                    message_id: Some(id.to_string()),
                    attempts: failed,
                    ..RunReply::of(RunOutcome::Dead)
                });
            }
            q.ready.push((id, failed + 1));
            let mut r = RunReply::failed(&SprigError::new(
                sprig_common::SprigCode::LogicUserError,
                "boom",
            ));
            r.message_id = Some(id.to_string());
            r.attempts = failed;
            return Ok(r);
        }
        q.done.push(id);
        Ok(RunReply::of(RunOutcome::Ok))
    }
}

#[tokio::test]
async fn a_queue_trigger_dead_letters_after_its_attempts() {
    let q = Arc::new(Mutex::new(QueueWorld {
        ready: (0..30).map(|i| (i, 0)).collect(),
        poison: HashSet::from([4, 17]),
        ..QueueWorld::default()
    }));
    let c = CancellationToken::new();
    let mut t = template(3);
    t.req.source.kind = SourceKind::Queue;
    let stats = Arc::new(MsgStats::default());
    let h = tokio::spawn(run_queue(
        Arc::new(QueueGw { q: Arc::clone(&q) }),
        t,
        opts(),
        Arc::clone(&stats),
        c.clone(),
    ));
    for _ in 0..500 {
        let finished = {
            let g = q.lock().unwrap();
            g.done.len() + g.dead.len() == 30
        };
        if finished {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    c.cancel();
    h.await.unwrap();
    let g = q.lock().unwrap();
    let mut dead: Vec<_> = g.dead.clone();
    dead.sort_unstable();
    assert_eq!(dead, vec![(4, 3), (17, 3)]);
    assert_eq!(g.done.len(), 28);
    assert_eq!(stats.dead.load(Ordering::Relaxed), 2);
    assert_eq!(stats.failed.load(Ordering::Relaxed), 4);
}
