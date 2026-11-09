//! A deterministic simulation of the sink runtime (chapter 51, "Testing":
//! per-key order under random outcomes, poison splitting, dead letters,
//! pausing and probing, exactly-once under worker kills).
//!
//! Each seed runs a task over an in-memory stream of three partitions
//! against an in-memory destination that fails at random: destination-wide
//! errors (some with `Retry-After`), one-record retries, poison records
//! (refused for the whole batch, so the runtime must split them out),
//! lost answers (applied, then an error), and kills of the worker at random
//! moments (no final settle). After each kill a new task starts from the
//! committed offsets, as a worker claiming the task does.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::missing_panics_doc,
    clippy::fn_params_excessive_bools,
    clippy::similar_names
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::{Result, SprigCode};
use sprig_sinks::model::{
    Batch, BatchOutcome, DestPosition, Outcome, Position, SinkRecord, TaskEpoch, err, stream_key,
};
use sprig_sinks::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, PassAll, Rng, RunSettings,
    SourceReader, Target, TaskEnd, TaskInfo, TaskParts, run_task,
};

const PARTS: u32 = 3;

#[derive(Clone)]
struct Rec {
    key: String,
    partition: u32,
    offset: u64,
}

fn records(n: usize, keys: usize) -> Vec<Rec> {
    let mut next = [0u64; PARTS as usize];
    (0..n)
        .map(|i| {
            let k = i % keys;
            let partition = u32::try_from(k).unwrap_or(0) % PARTS;
            let offset = next[partition as usize];
            next[partition as usize] += 1;
            Rec {
                key: format!("k{k}"),
                partition,
                offset,
            }
        })
        .collect()
}

fn sink_record(r: &Rec) -> SinkRecord {
    SinkRecord {
        key: Some(r.key.as_bytes().to_vec()),
        value: Some(format!("{{\"o\":{}}}", r.offset).into_bytes()),
        headers: Vec::new(),
        op: None,
        before: None,
        after: None,
        ts_ms: 0,
        idem_key: stream_key(1, r.partition, r.offset, 0),
        pos: Position::Stream {
            stream: 1,
            partition: r.partition,
            offset: r.offset,
            epoch: 0,
        },
    }
}

/// The stream: records per partition, and the group's committed offsets.
struct Stream {
    parts: Vec<Vec<Rec>>,
    committed: Mutex<HashMap<u32, u64>>,
}

struct Reader {
    s: Arc<Stream>,
    at: HashMap<u32, u64>,
    turn: u32,
}

impl Reader {
    fn new(s: Arc<Stream>) -> Self {
        let at = s.committed.lock().unwrap().clone();
        Self { s, at, turn: 0 }
    }
}

#[async_trait]
impl SourceReader for Reader {
    async fn next_batch(
        &mut self,
        max_records: usize,
        _max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let mut out = Vec::new();
        for _ in 0..PARTS {
            let p = self.turn % PARTS;
            self.turn += 1;
            let at = self.at.entry(p).or_insert(0);
            let part = &self.s.parts[p as usize];
            while out.len() < max_records && (*at as usize) < part.len() {
                out.push(sink_record(&part[*at as usize]));
                *at += 1;
            }
        }
        if out.is_empty() {
            tokio::time::sleep(wait).await;
        }
        Ok(out)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let mut c = self.s.committed.lock().unwrap();
        for p in done {
            if let Position::Stream {
                partition, offset, ..
            } = p
            {
                let e = c.entry(*partition).or_insert(0);
                *e = (*e).max(offset + 1);
            }
        }
        Ok(())
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        if let Position::Stream {
            partition, offset, ..
        } = pos
        {
            self.at.insert(*partition, offset + 1);
        }
        Ok(())
    }
}

/// The destination: what it applied, in order, and its fault plan.
#[derive(Default)]
struct DestState {
    applied: Vec<(String, u32, u64)>,
    /// Exactly-once: the offsets rows, per partition.
    offsets: HashMap<u32, u64>,
    down_until: Option<tokio::time::Instant>,
    batches: u64,
    splits_seen: u64,
}

struct Dest {
    st: Arc<Mutex<DestState>>,
    rng: Arc<Rng>,
    poison: Arc<HashSet<String>>,
    exactly_once: bool,
    faults: bool,
}

fn key_of(r: &SinkRecord) -> (String, u32, u64) {
    let Position::Stream {
        partition, offset, ..
    } = r.pos
    else {
        unreachable!()
    };
    (
        String::from_utf8(r.key.clone().unwrap()).unwrap(),
        partition,
        offset,
    )
}

#[async_trait]
impl Target for Dest {
    fn kind(&self) -> &'static str {
        "sim"
    }

    async fn open(&mut self, _task: &TaskInfo) -> Result<Vec<DestPosition>> {
        if !self.exactly_once {
            return Ok(Vec::new());
        }
        Ok(self
            .st
            .lock()
            .unwrap()
            .offsets
            .iter()
            .map(|(p, o)| DestPosition {
                pos: Position::Stream {
                    stream: 1,
                    partition: *p,
                    offset: *o,
                    epoch: 0,
                },
                epoch: TaskEpoch(0),
            })
            .collect())
    }

    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome> {
        // A round trip.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut st = self.st.lock().unwrap();
        st.batches += 1;
        if let Some(t) = st.down_until {
            if tokio::time::Instant::now() < t {
                return Err(err(SprigCode::SinkDeliveryFailed, "connection refused"));
            }
            st.down_until = None;
        }
        let roll = if self.faults {
            self.rng.next_u64() % 100
        } else {
            99
        };
        if roll < 8 {
            return Err(err(SprigCode::SinkDeliveryFailed, "HTTP 503"));
        }
        if roll < 10 {
            return Ok(BatchOutcome::All(Outcome::Retry {
                after: Some(Duration::from_millis(1500)),
                reason: "HTTP 429".into(),
            }));
        }
        let has_poison = b.records.iter().any(|r| self.poison.contains(&r.idem_key));
        if has_poison && (self.exactly_once || roll % 2 == 0) {
            // Whole-batch refusal without per-record results: split.
            if b.records.len() > 1 {
                st.splits_seen += 1;
            }
            return Ok(BatchOutcome::All(Outcome::Fail {
                reason: "HTTP 400".into(),
            }));
        }
        if self.exactly_once {
            // Records and the offsets in one transaction; records at or
            // below the stored offset of their partition were applied by
            // an earlier attempt whose answer was lost.
            for r in &b.records {
                let (k, p, o) = key_of(r);
                if st.offsets.get(&p).is_some_and(|done| o <= *done) {
                    continue;
                }
                st.applied.push((k, p, o));
            }
            for e in &b.ends {
                if let Position::Stream {
                    partition, offset, ..
                } = e
                {
                    st.offsets.insert(*partition, *offset);
                }
            }
            if roll < 15 {
                return Err(err(SprigCode::SinkDeliveryFailed, "lost answer"));
            }
            return Ok(BatchOutcome::All(Outcome::Ok));
        }
        let mut outs = Vec::new();
        // A destination that keeps per-key order: once it refuses a
        // record for now, it takes no later record of that key in the
        // batch.
        let mut held: HashSet<Vec<u8>> = HashSet::new();
        for r in &b.records {
            if self.poison.contains(&r.idem_key) {
                outs.push(Outcome::Fail {
                    reason: "HTTP 422".into(),
                });
                continue;
            }
            let rr = if self.faults {
                self.rng.next_u64() % 100
            } else {
                99
            };
            let k = r.key.clone().unwrap_or_default();
            if rr < 5 || held.contains(&k) {
                held.insert(k);
                outs.push(Outcome::Retry {
                    after: None,
                    reason: "partial 503".into(),
                });
            } else {
                st.applied.push(key_of(r));
                outs.push(Outcome::Ok);
            }
        }
        if roll < 15 {
            // Applied, but the answer was lost.
            return Err(err(SprigCode::SinkDeliveryFailed, "lost answer"));
        }
        Ok(BatchOutcome::Each(outs))
    }

    fn exactly_once(&self) -> bool {
        self.exactly_once
    }
}

#[derive(Default)]
struct Dead(Mutex<Vec<(String, String)>>);

#[async_trait]
impl DeadLetters for Dead {
    async fn write(&self, rec: &SinkRecord, info: &DeadInfo) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .push((rec.idem_key.clone(), info.class.to_owned()));
        Ok(())
    }
}

#[derive(Default)]
struct Ctl(Mutex<Vec<Event>>);

#[async_trait]
impl Control for Ctl {
    async fn poll(&self) -> Result<Directive> {
        Ok(Directive::Run)
    }
    async fn report(&self, ev: Event) -> Result<()> {
        if !matches!(ev, Event::Delivered(_)) {
            self.0.lock().unwrap().push(ev);
        }
        Ok(())
    }
    async fn check_epoch(&self) -> Result<()> {
        Ok(())
    }
}

fn settings(lanes: u16) -> RunSettings {
    RunSettings {
        lanes,
        batch_max_records: 16,
        batch_max_wait: Duration::from_millis(50),
        retry_initial: Duration::from_millis(100),
        retry_max: Duration::from_millis(5000),
        record_max_attempts: 10,
        pause_after: Duration::from_secs(15 * 60),
        probe_interval: Duration::from_secs(60),
        ..RunSettings::default()
    }
}

struct Outcome1 {
    applied: Vec<(String, u32, u64)>,
    dead: Vec<(String, String)>,
    kills: u32,
    events: Vec<Event>,
    splits: u64,
}

#[allow(clippy::too_many_arguments)]
async fn run_seed(
    seed: u64,
    n: usize,
    exactly_once: bool,
    poison_every: usize,
    kills: bool,
    faults: bool,
    down_for: Option<Duration>,
    s: RunSettings,
) -> Outcome1 {
    let rng = Arc::new(Rng::new(seed));
    let recs = records(n, 20);
    let mut parts = vec![Vec::new(); PARTS as usize];
    for r in &recs {
        parts[r.partition as usize].push(r.clone());
    }
    let poison: HashSet<String> = recs
        .iter()
        .enumerate()
        .filter(|(i, _)| poison_every > 0 && i % poison_every == poison_every / 2)
        .map(|(_, r)| stream_key(1, r.partition, r.offset, 0))
        .collect();
    let stream = Arc::new(Stream {
        parts,
        committed: Mutex::new(HashMap::new()),
    });
    let st = Arc::new(Mutex::new(DestState::default()));
    if let Some(d) = down_for {
        st.lock().unwrap().down_until = Some(tokio::time::Instant::now() + d);
    }
    let poison = Arc::new(poison);
    let dead = Arc::new(Dead::default());
    let ctl = Arc::new(Ctl::default());
    let mut kill_count = 0;
    let total: HashMap<u32, u64> = (0..PARTS)
        .map(|p| (p, stream.parts[p as usize].len() as u64))
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3 * 3600);
    let mut life = 0u64;
    loop {
        life += 1;
        let parts = TaskParts {
            info: TaskInfo {
                tenant: 1,
                sink: 1,
                task_no: 0,
                epoch: TaskEpoch(life),
            },
            source: Box::new(Reader::new(Arc::clone(&stream))),
            target: Box::new(Dest {
                st: Arc::clone(&st),
                rng: Arc::clone(&rng),
                poison: Arc::clone(&poison),
                exactly_once,
                faults,
            }),
            dead: Arc::clone(&dead) as Arc<dyn DeadLetters>,
            control: Arc::clone(&ctl) as Arc<dyn Control>,
            prepare: Arc::new(PassAll),
            settings: s.clone(),
            seed: Some(seed.wrapping_mul(31).wrapping_add(life)),
            reject_via_source: false,
        };
        let cancel = Cancel::new();
        let h = tokio::spawn(run_task(parts, cancel.clone()));
        let kill_at =
            tokio::time::Instant::now() + Duration::from_millis(200 + rng.next_u64() % 3000);
        let mut h = h;
        let ended = loop {
            let done = {
                let c = stream.committed.lock().unwrap();
                total
                    .iter()
                    .all(|(p, n)| c.get(p).copied().unwrap_or(0) >= *n)
            };
            if done {
                cancel.cancel();
                let r = (&mut h).await.unwrap().unwrap();
                assert_eq!(r, TaskEnd::Cancelled);
                break true;
            }
            tokio::select! {
                () = tokio::time::sleep_until(kill_at), if kills && kill_count < 12 => {
                    h.abort();
                    let _ = (&mut h).await;
                    kill_count += 1;
                    break false;
                }
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "seed {seed}: no progress: committed {:?} of {total:?}, dest offsets {:?}, events {:?}, kills {kill_count}",
                stream.committed.lock().unwrap(),
                st.lock().unwrap().offsets,
                ctl.0.lock().unwrap()
            );
        };
        if ended {
            break;
        }
    }
    let applied = st.lock().unwrap().applied.clone();
    let splits = st.lock().unwrap().splits_seen;
    let dead = dead.0.lock().unwrap().clone();
    let events = ctl.0.lock().unwrap().clone();
    // Every record is accounted for.
    for r in &recs {
        let key = stream_key(1, r.partition, r.offset, 0);
        let was_applied = applied
            .iter()
            .any(|a| a.1 == r.partition && a.2 == r.offset);
        if poison.contains(&key) {
            assert!(!was_applied, "seed {seed}: poison {key} applied");
            assert!(
                dead.iter().any(|d| d.0 == key),
                "seed {seed}: poison {key} not dead-lettered"
            );
        } else {
            assert!(
                was_applied || dead.iter().any(|d| d.0 == key),
                "seed {seed}: {key} lost"
            );
        }
    }
    // Per-key order of first deliveries.
    let mut seen: HashSet<(u32, u64)> = HashSet::new();
    let mut last: BTreeMap<String, u64> = BTreeMap::new();
    for (k, p, o) in &applied {
        if !seen.insert((*p, *o)) {
            continue;
        }
        if let Some(prev) = last.get(k) {
            assert!(
                o > prev,
                "seed {seed}: key {k} out of order: {o} after {prev}"
            );
        }
        last.insert(k.clone(), *o);
    }
    Outcome1 {
        applied,
        dead,
        kills: kill_count,
        events,
        splits,
    }
}

#[tokio::test(start_paused = true)]
async fn at_least_once_keeps_key_order_through_faults_and_kills() {
    let (mut applied, mut dups, mut kills, mut dead, mut splits) = (0, 0, 0, 0, 0);
    for seed in 0..60 {
        let o = run_seed(seed, 400, false, 37, true, true, None, settings(4)).await;
        let distinct: HashSet<_> = o.applied.iter().map(|a| (a.1, a.2)).collect();
        applied += o.applied.len();
        dups += o.applied.len() - distinct.len();
        kills += o.kills;
        dead += o.dead.len();
        splits += o.splits;
    }
    eprintln!(
        "at least once: 60 seeds, {applied} applied ({dups} duplicates), {kills} kills, {dead} dead letters, {splits} split batches"
    );
    assert!(kills > 50);
    assert!(splits > 10);
}

#[tokio::test(start_paused = true)]
async fn exactly_once_applies_each_record_once() {
    let (mut applied, mut kills) = (0, 0);
    for seed in 0..60 {
        let o = run_seed(seed, 300, true, 41, true, true, None, settings(4)).await;
        let distinct: HashSet<_> = o.applied.iter().map(|a| (a.1, a.2)).collect();
        assert_eq!(
            distinct.len(),
            o.applied.len(),
            "seed {seed}: a record applied twice"
        );
        applied += o.applied.len();
        kills += o.kills;
    }
    eprintln!("exactly once: 60 seeds, {applied} applied once each, {kills} kills");
    assert!(kills > 50);
}

#[tokio::test(start_paused = true)]
async fn an_outage_pauses_then_probes_and_resumes() {
    let o = run_seed(
        7,
        100,
        false,
        0,
        false,
        false,
        Some(Duration::from_secs(20 * 60)),
        settings(4),
    )
    .await;
    assert_eq!(o.applied.len(), 100, "every record once without faults");
    assert!(
        matches!(o.events.first(), Some(Event::Failing(_))),
        "{:?}",
        o.events
    );
    assert!(
        o.events
            .iter()
            .any(|e| matches!(e, Event::PausedByFailure(_))),
        "{:?}",
        o.events
    );
    assert!(
        matches!(o.events.last(), Some(Event::Recovered)),
        "{:?}",
        o.events
    );
}

#[tokio::test(start_paused = true)]
async fn one_lane_keeps_strict_source_order() {
    let o = run_seed(3, 200, false, 0, false, false, None, settings(1)).await;
    // Without faults and kills: once each, partition order kept.
    let mut last: HashMap<u32, u64> = HashMap::new();
    for (_, p, off) in &o.applied {
        if let Some(prev) = last.get(p) {
            assert!(off > prev);
        }
        last.insert(*p, *off);
    }
    assert_eq!(o.applied.len(), 200);
}
