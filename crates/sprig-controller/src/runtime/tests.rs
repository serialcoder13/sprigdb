//! Runtime tests: on `MemStore` with tokio's clock paused, so leases,
//! backoffs and resyncs are deterministic and instant (chapter 23,
//! "Testing": deterministic simulation).

// Test suites: failures are panics with a message.
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::unchecked_time_subtraction,
    clippy::single_match_else
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sprig_common::ids::DcId;
use sprig_meta::session::TokioClock;
use sprig_meta::{ListOpts, MemStore, Tier};

use super::*;

type Log = Arc<Mutex<Vec<(String, Duration)>>>;

pub(crate) fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}

/// A DC store, its global view, and the clock both follow.
pub(crate) fn mem(dc: &str) -> (MemStore, Stores, Arc<dyn Clock>) {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
    let s = MemStore::with_clock(Tier::Dc(DcId::new(dc).unwrap()), clock.clone());
    let stores = Stores {
        dc: Arc::new(s.clone()),
        global: Arc::new(s.with_tier(Tier::Global)),
    };
    (s, stores, clock)
}

pub(crate) fn cx_for(
    stores: &Stores,
    clock: &Arc<dyn Clock>,
    leadership: Option<Leadership>,
    settings: ControlSettings,
) -> (LoopCx, CancellationToken) {
    let stop = CancellationToken::new();
    let cx = LoopCx::new(
        stores.dc.clone(),
        stores.global.clone(),
        leadership,
        clock.clone(),
        stop.clone(),
        watch::channel(Arc::new(settings)).1,
    );
    (cx, stop)
}

pub(crate) async fn put(store: &dyn MetaStore, rel: &str, value: &str) -> Revision {
    store
        .txn(sprig_meta::Txn {
            when: vec![],
            then: vec![sprig_meta::Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: value.as_bytes().to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap()
        .revision
}

/// Reconciles `items/<name>`; scripted failures and blocks per key; logs
/// every call with the (paused) time since the start.
struct Items {
    start: Instant,
    log: Log,
    fail_first: HashMap<String, u32>,
    block: Option<String>,
    calls: Mutex<HashMap<String, u32>>,
    busy: Arc<Mutex<HashMap<String, u32>>>,
    max_busy_per_key: Arc<Mutex<u32>>,
    max_busy: Arc<Mutex<u32>>,
    hold: Duration,
    workers: usize,
}

impl Items {
    fn new(log: Log) -> Self {
        Self {
            start: Instant::now(),
            log,
            fail_first: HashMap::new(),
            block: None,
            calls: Mutex::new(HashMap::new()),
            busy: Arc::default(),
            max_busy_per_key: Arc::default(),
            max_busy: Arc::default(),
            hold: Duration::ZERO,
            workers: 1,
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for Items {
    type Key = String;
    fn name(&self) -> &'static str {
        "items"
    }
    fn tier(&self) -> LoopTier {
        LoopTier::DcAll
    }
    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::dc("items/")]
    }
    fn keys_for(&self, ev: &MetaEvent) -> Vec<String> {
        ev.path()
            .strip_prefix("items/")
            .map(|k| vec![k.to_owned()])
            .unwrap_or_default()
    }
    async fn all_keys(&self, cx: &LoopCx) -> Result<Vec<String>> {
        let prefix = cx.dc.tier().key("items/")?;
        let page = cx.dc.list(&prefix, ListOpts::default()).await?;
        Ok(page
            .kvs
            .iter()
            .map(|kv| relative(kv.key.as_str())["items/".len()..].to_owned())
            .collect())
    }
    async fn reconcile(&self, _cx: &LoopCx, key: &String) -> Result<Outcome> {
        self.log
            .lock()
            .unwrap()
            .push((key.clone(), self.start.elapsed()));
        {
            let mut busy = self.busy.lock().unwrap();
            let n = busy.entry(key.clone()).or_default();
            *n += 1;
            let mut m = self.max_busy_per_key.lock().unwrap();
            *m = (*m).max(*n);
            let total: u32 = busy.values().sum();
            let mut mt = self.max_busy.lock().unwrap();
            *mt = (*mt).max(total);
        }
        tokio::time::sleep(self.hold).await;
        *self.busy.lock().unwrap().get_mut(key).unwrap() -= 1;
        let n = {
            let mut calls = self.calls.lock().unwrap();
            let n = calls.entry(key.clone()).or_default();
            *n += 1;
            *n
        };
        if self.fail_first.get(key).is_some_and(|&f| n <= f) {
            return Err(SprigError::internal("scripted failure"));
        }
        if self.block.as_deref() == Some(key.as_str()) && n == 1 {
            return Ok(Outcome::Blocked { reason: "test" });
        }
        Ok(Outcome::Done)
    }
    fn workers(&self) -> usize {
        self.workers
    }
}

fn calls_of(log: &Log, key: &str) -> Vec<Duration> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, t)| *t)
        .collect()
}

#[test]
fn relative_paths() {
    assert_eq!(relative("/sprig/g/dcs/mum1"), "dcs/mum1");
    assert_eq!(relative("/sprig/dc/mum1/jobs/x"), "jobs/x");
    assert_eq!(relative("/other"), "/other");
}

#[test]
fn loop_follows_listing_events_and_resync() {
    paused().block_on(async {
        let (s, stores, clock) = mem("dc-a");
        put(&s, "items/a", "1").await;
        let log: Log = Arc::default();
        let settings = ControlSettings {
            resync: Duration::from_secs(60),
            ..ControlSettings::default()
        };
        let (cx, stop) = cx_for(&stores, &clock, None, settings);
        let task = tokio::spawn({
            let l = Loop::new(Items::new(log.clone()));
            async move { l.run(cx).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(calls_of(&log, "a").len(), 1, "listed at start");
        put(&s, "items/b", "1").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(calls_of(&log, "b").len(), 1, "event");
        // A resync relists everything.
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(calls_of(&log, "a").len(), 2);
        assert_eq!(calls_of(&log, "b").len(), 2);
        stop.cancel();
        task.await.unwrap();
    });
}

#[test]
fn errors_back_off_and_blocked_keys_wait() {
    paused().block_on(async {
        let (s, stores, clock) = mem("dc-a");
        put(&s, "items/flaky", "1").await;
        put(&s, "items/stuck", "1").await;
        let log: Log = Arc::default();
        let mut items = Items::new(log.clone());
        items.fail_first.insert("flaky".to_owned(), 4);
        items.block = Some("stuck".to_owned());
        let (cx, stop) = cx_for(&stores, &clock, None, ControlSettings::default());
        let task = tokio::spawn({
            let l = Loop::new(items);
            async move { l.run(cx).await }
        });
        tokio::time::sleep(Duration::from_secs(40)).await;
        let flaky = calls_of(&log, "flaky");
        assert_eq!(flaky.len(), 5, "4 failures then success: {flaky:?}");
        let gaps: Vec<Duration> = flaky.windows(2).map(|w| w[1] - w[0]).collect();
        for (i, g) in gaps.iter().enumerate() {
            let base = backoff(u32::try_from(i).unwrap() + 1);
            assert!(
                *g >= base.mul_f64(0.79) && *g <= base.mul_f64(1.21),
                "gap {i}: {g:?} vs {base:?}"
            );
        }
        let stuck = calls_of(&log, "stuck");
        assert_eq!(stuck.len(), 2, "{stuck:?}");
        assert!(stuck[1] - stuck[0] >= BLOCKED_RETRY);
        stop.cancel();
        task.await.unwrap();
    });
}

#[test]
fn a_key_never_runs_twice_at_once() {
    paused().block_on(async {
        let (s, stores, clock) = mem("dc-a");
        let log: Log = Arc::default();
        let mut items = Items::new(log.clone());
        items.hold = Duration::from_secs(1);
        items.workers = 4;
        let (per_key, total) = (items.max_busy_per_key.clone(), items.max_busy.clone());
        let (cx, stop) = cx_for(&stores, &clock, None, ControlSettings::default());
        let task = tokio::spawn({
            let l = Loop::new(items);
            async move { l.run(cx).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        for round in 0..5 {
            for k in ["x", "y", "z"] {
                put(&s, &format!("items/{k}"), &round.to_string()).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(*per_key.lock().unwrap(), 1);
        assert!(*total.lock().unwrap() > 1, "different keys run in parallel");
        // Updates during a reconcile are not lost: the last one runs after.
        let x = calls_of(&log, "x");
        assert!(x.len() >= 2 && x.len() < 5, "deduplicated: {x:?}");
        stop.cancel();
        task.await.unwrap();
    });
}

/// A DC-leader loop that writes `writes/<holder>-<n>` with its token,
/// guarded, once per tick.
struct Writer {
    holder: String,
    n: Mutex<u32>,
}

#[async_trait::async_trait]
impl Reconciler for Writer {
    type Key = &'static str;
    fn name(&self) -> &'static str {
        "writer"
    }
    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }
    fn keys_for(&self, _ev: &MetaEvent) -> Vec<&'static str> {
        vec![]
    }
    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<&'static str>> {
        Ok(vec!["tick"])
    }
    async fn reconcile(&self, cx: &LoopCx, _key: &&'static str) -> Result<Outcome> {
        let l = cx.leader()?;
        let n = {
            let mut n = self.n.lock().unwrap();
            *n += 1;
            *n
        };
        let key = cx.dc.tier().key(&format!("writes/{}-{n}", self.holder))?;
        l.commit(vec![sprig_meta::Op::Put {
            key,
            value: l.token.to_string().into_bytes(),
            lease: None,
        }])
        .await?;
        Ok(Outcome::RequeueAfter(Duration::from_millis(200)))
    }
}

fn controller(stores: &Stores, clock: &Arc<dyn Clock>, holder: &str) -> Runtime {
    Runtime::new(
        stores.clone(),
        clock.clone(),
        LeaderRecord {
            holder: holder.to_owned(),
            dc: Some("dc-a".to_owned()),
            build: "test".to_owned(),
        },
    )
    .with_loop(Loop::new(Writer {
        holder: holder.to_owned(),
        n: Mutex::new(0),
    }))
}

async fn writes(store: &dyn MetaStore) -> Vec<(Revision, Revision)> {
    let prefix = store.tier().key("writes/").unwrap();
    let mut w: Vec<(Revision, Revision)> = store
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await
        .unwrap()
        .kvs
        .iter()
        .map(|kv| {
            let token = std::str::from_utf8(&kv.value).unwrap().parse().unwrap();
            (kv.mod_revision, token)
        })
        .collect();
    w.sort_unstable();
    w
}

async fn wait_leader(rx: &mut watch::Receiver<Status>) -> Revision {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Some(t) = rx.borrow_and_update().dc_leader {
                return t;
            }
            rx.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[test]
fn leader_loops_run_on_one_controller_and_move_on_step_down() {
    paused().block_on(async {
        let (s, stores, clock) = mem("dc-a");
        let a = controller(&stores, &clock, "a");
        let b = controller(&stores, &clock, "b");
        let (mut sa, mut sb) = (a.status(), b.status());
        let (stop_a, stop_b) = (CancellationToken::new(), CancellationToken::new());
        let ta = tokio::spawn(a.run(stop_a.clone()));
        let first = wait_leader(&mut sa).await;
        let tb = tokio::spawn(b.run(stop_b.clone()));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(sb.borrow().dc_leader.is_none(), "one leader at a time");
        let before = writes(&s).await;
        assert!(!before.is_empty());
        assert!(before.iter().all(|&(_, t)| t == first));

        // An operator deletes the leader key: a's next guarded write fails,
        // it steps down and stops its loops; b wins with a higher token.
        let key = s.tier().key("leader").unwrap();
        s.txn(sprig_meta::Txn {
            when: vec![],
            then: vec![sprig_meta::Op::Delete(key)],
            otherwise: vec![],
        })
        .await
        .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let a_now = sa.borrow().dc_leader;
                let b_now = sb.borrow().dc_leader;
                match (a_now, b_now) {
                    (_, Some(t)) | (Some(t), None) if t != first => return t,
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await
        .unwrap();
        assert!(second > first);

        // Shutdown of the leader resigns: the other takes over at once,
        // well before a lease could expire.
        let (leader_stop, other_rx) = if sa.borrow().dc_leader == Some(second) {
            (stop_a.clone(), &mut sb)
        } else {
            (stop_b.clone(), &mut sa)
        };
        leader_stop.cancel();
        let t0 = Instant::now();
        let third = wait_leader(other_rx).await;
        assert!(third > second);
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop_a.cancel();
        stop_b.cancel();
        ta.await.unwrap();
        tb.await.unwrap();

        // Only the token holder's writes committed: in revision order the
        // tokens never go back.
        let all = writes(&s).await;
        assert!(all.windows(2).all(|w| w[0].1 <= w[1].1), "{all:?}");
        assert!(all.iter().all(|&(rev, token)| rev > token));
        assert_eq!(
            all.iter()
                .map(|w| w.1)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
    });
}

#[test]
fn losing_the_store_stops_leader_loops_until_it_returns() {
    paused().block_on(async {
        let (s, stores, clock) = mem("dc-a");
        let a = controller(&stores, &clock, "a");
        let mut sa = a.status();
        let stop = CancellationToken::new();
        let task = tokio::spawn(a.run(stop.clone()));
        let first = wait_leader(&mut sa).await;
        s.set_unavailable(true);
        // The session counts as lost a margin before its 5 s lease could
        // expire in etcd; the step-down follows within one keep-alive period.
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert!(
            sa.borrow().dc_leader.is_none(),
            "stepped down while etcd is gone"
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(sa.borrow().dc_leader.is_none());
        s.set_unavailable(false);
        let again = wait_leader(&mut sa).await;
        assert!(again > first);
        stop.cancel();
        task.await.unwrap();
    });
}

/// Deterministic simulation: three controllers, a seeded schedule of leader
/// key deletions, store outages and restarts. Property: only the token
/// holder's writes commit (tokens never go back in revision order).
#[test]
fn simulation_only_the_token_holder_writes() {
    for seed in 1..=5u64 {
        paused().block_on(async move {
            let (s, stores, clock) = mem("dc-a");
            let mut rng = queue::Jitter::new(seed);
            let mut stops: Vec<(CancellationToken, tokio::task::JoinHandle<()>)> = Vec::new();
            let mut started = 0u32;
            let start = |n: u32| {
                let stop = CancellationToken::new();
                let rt = controller(&stores, &clock, &format!("c{n}"));
                (stop.clone(), tokio::spawn(rt.run(stop)))
            };
            for _ in 0..3 {
                started += 1;
                stops.push(start(started));
            }
            for _ in 0..30 {
                let pick = rng.apply(Duration::from_secs(10)).as_millis() % 4;
                match pick {
                    0 => {
                        let key = s.tier().key("leader").unwrap();
                        let _ = s
                            .txn(sprig_meta::Txn {
                                when: vec![],
                                then: vec![sprig_meta::Op::Delete(key)],
                                otherwise: vec![],
                            })
                            .await;
                    }
                    1 => {
                        s.set_unavailable(true);
                        tokio::time::sleep(rng.apply(Duration::from_secs(3))).await;
                        s.set_unavailable(false);
                    }
                    2 => {
                        let (stop, h) = stops.remove(0);
                        stop.cancel();
                        h.await.unwrap();
                        started += 1;
                        stops.push(start(started));
                    }
                    _ => {}
                }
                tokio::time::sleep(rng.apply(Duration::from_secs(2))).await;
            }
            for (stop, h) in stops {
                stop.cancel();
                h.await.unwrap();
            }
            let all = writes(&s).await;
            assert!(all.len() > 10, "seed {seed}: {}", all.len());
            assert!(
                all.windows(2).all(|w| w[0].1 <= w[1].1),
                "seed {seed}: tokens went back: {all:?}"
            );
        });
    }
}
