//! The Kafka target against a real broker:
//! `r9sinks-kafka`, Apache Kafka 3.9.0 in `KRaft` mode, PLAINTEXT on
//! 127.0.0.1:29092 and SASL (SCRAM-SHA-256/512, PLAIN) on 127.0.0.1:29093.
//!
//! Skipped unless `SPRIG_SINKS_KAFKA_LIVE=1`. Run with `--features
//! wip-kafka --test-threads=1` (one test restarts the broker).
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    missing_docs
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Map, Value};
use sprig_common::Result;
use sprig_sinks::model::TaskEpoch;
use sprig_sinks::model::{
    Batch, BatchOutcome, ConnKind, Guarantee, Outcome, Position, SinkDef, SinkId, SinkRecord,
    SinkState, SourceSpec, StartAt, TargetSpec, stream_key,
};
use sprig_sinks::policy::Destinations;
use sprig_sinks::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, PassAll, RunSettings, SourceReader,
    Target, TaskInfo, TaskParts, run_task,
};
use sprig_sinks::target::kafka::KafkaTarget;
use sprig_sinks::target::{Conn, MemSecrets, Opts, TargetCtx};

fn live() -> bool {
    std::env::var("SPRIG_SINKS_KAFKA_LIVE").is_ok_and(|v| v == "1")
}

fn unique(prefix: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{n}")
}

struct Cfg<'a> {
    topic: &'a str,
    guarantee: Guarantee,
    opts: &'a [(&'a str, &'a str)],
    conn: &'a [(&'a str, &'a str)],
    secrets: &'a [(&'a str, &'a str)],
    sink: u64,
    task: u32,
}

fn ctx(c: &Cfg<'_>) -> TargetCtx {
    let mut options = Map::new();
    options.insert("bootstrap".into(), Value::from("127.0.0.1:29092"));
    for (k, v) in c.conn {
        options.insert((*k).into(), Value::from(*v));
    }
    let sink = SinkDef {
        id: SinkId(c.sink),
        db_id: 1,
        name: "orders_out".into(),
        source: SourceSpec::Stream {
            id: 1,
            name: "order_events".into(),
            partitions: 3,
        },
        filter_sql: None,
        projection: None,
        transform: None,
        target: TargetSpec::Kafka {
            topic: c.topic.into(),
            key: None,
        },
        connections: Vec::new(),
        guarantee: c.guarantee,
        dead_letter: None,
        start_at: StartAt::Earliest,
        owner_user_id: 1,
        state: SinkState::Running,
        state_reason: None,
        definition_ver: 1,
        options: BTreeMap::default(),
    };
    TargetCtx {
        task: TaskInfo {
            tenant: 77,
            sink: c.sink,
            task_no: c.task,
            epoch: TaskEpoch(1),
        },
        sink: Arc::new(sink),
        conn: Some(Conn {
            name: "kafka_local".into(),
            kind: ConnKind::Kafka,
            options,
            secrets: Arc::new(MemSecrets::of(c.secrets)),
            dest: Arc::new(Destinations::allow_list(&[
                "127.0.0.1:29092",
                "127.0.0.1:29093",
            ])),
        }),
        extra: Vec::new(),
        opts: Opts(
            c.opts
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        ),
        db_name: "shop".into(),
    }
}

fn rec(partition: u32, offset: u64, key: &str, value: &str) -> SinkRecord {
    SinkRecord {
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: vec![("type".into(), b"order.paid".to_vec())],
        op: None,
        before: None,
        after: None,
        ts_ms: 1_762_682_400_000 + i64::try_from(offset).unwrap(),
        idem_key: stream_key(1, partition, offset, 0),
        pos: Position::Stream {
            stream: 1,
            partition,
            offset,
            epoch: 0,
        },
    }
}

/// Delivers until the target answers (a new topic needs a moment).
async fn deliver(t: &dyn Target, b: &Batch) -> BatchOutcome {
    let start = Instant::now();
    loop {
        match t.deliver(b).await {
            Ok(o) => return o,
            Err(e) => {
                assert!(start.elapsed() < Duration::from_secs(30), "{e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

#[tokio::test]
async fn every_format_and_compression_reads_back() {
    if !live() {
        return;
    }
    for fmt in ["json", "raw", "cloudevents"] {
        for comp in ["none", "gzip", "zstd", "snappy", "lz4"] {
            let topic = unique(&format!("r9sinks_fmt_{fmt}_{comp}"));
            let c = ctx(&Cfg {
                topic: &topic,
                guarantee: Guarantee::AtLeastOnce,
                opts: &[("value_format", fmt), ("compression", comp)],
                conn: &[],
                secrets: &[],
                sink: 1,
                task: 0,
            });
            let t = KafkaTarget::new(&c).unwrap();
            let recs: Vec<SinkRecord> = (0..10)
                .map(|i| {
                    rec(
                        i % 3,
                        u64::from(i),
                        &format!("k{i}"),
                        &format!("{{\"n\":{i}}}"),
                    )
                })
                .collect();
            let o = deliver(&t, &Batch::new(recs.clone(), 0)).await;
            assert_eq!(o, BatchOutcome::All(Outcome::Ok), "{fmt}/{comp}");
            let back = t.read_committed(&topic).await.unwrap();
            let all: Vec<_> = back.values().flatten().collect();
            assert_eq!(all.len(), 10, "{fmt}/{comp}");
            for r in &all {
                let key =
                    String::from_utf8(r.header("sprig.idempotency.key").unwrap().to_vec()).unwrap();
                let src = recs.iter().find(|x| x.idem_key == key).unwrap();
                assert_eq!(r.key.as_deref(), src.key.as_deref());
                assert_eq!(r.header("sprig.source").unwrap(), b"order_events");
                let v: Value = serde_json::from_slice(r.value.as_deref().unwrap()).unwrap();
                assert_eq!(
                    v,
                    serde_json::from_slice::<Value>(src.value.as_deref().unwrap()).unwrap()
                );
                if fmt == "cloudevents" {
                    assert_eq!(r.header("ce_id").unwrap(), key.as_bytes());
                    assert_eq!(r.header("ce_type").unwrap(), b"order.paid");
                    assert_eq!(r.header("ce_specversion").unwrap(), b"1.0");
                    assert_eq!(
                        r.header("ce_source").unwrap(),
                        b"/sprig/77/shop/order_events"
                    );
                }
                // murmur2 on the key: the partition Java producers pick.
            }
            for (p, rs) in &back {
                for r in rs {
                    let k = r.key.as_deref().unwrap();
                    assert_eq!(sprig_sinks::target::kafka::partition_for_key(k, 3), *p);
                }
            }
        }
    }
}

#[tokio::test]
async fn sasl_logins() {
    if !live() {
        return;
    }
    let topic = unique("r9sinks_sasl");
    for (mech, user, pass) in [
        ("SCRAM-SHA-256", "sprigscram", "scram-secret-1"),
        ("SCRAM-SHA-512", "sprigscram", "scram-secret-1"),
        ("PLAIN", "sprigplain", "plain-secret-1"),
    ] {
        let c = ctx(&Cfg {
            topic: &topic,
            guarantee: Guarantee::AtLeastOnce,
            opts: &[],
            conn: &[
                ("bootstrap", "127.0.0.1:29093"),
                ("security", "sasl_plaintext"),
                ("sasl_mechanism", mech),
            ],
            secrets: &[("username", user), ("password", pass)],
            sink: 2,
            task: 0,
        });
        let t = KafkaTarget::new(&c).unwrap();
        let o = deliver(&t, &Batch::new(vec![rec(0, 0, "k", "{}")], 0)).await;
        assert_eq!(o, BatchOutcome::All(Outcome::Ok), "{mech}");
        let bad = ctx(&Cfg {
            topic: &topic,
            guarantee: Guarantee::AtLeastOnce,
            opts: &[],
            conn: &[
                ("bootstrap", "127.0.0.1:29093"),
                ("security", "sasl_plaintext"),
                ("sasl_mechanism", mech),
            ],
            secrets: &[("username", user), ("password", "wrong")],
            sink: 2,
            task: 0,
        });
        let e = KafkaTarget::new(&bad)
            .unwrap()
            .deliver(&Batch::new(vec![rec(0, 1, "k", "{}")], 0))
            .await
            .unwrap_err();
        assert!(e.message.contains("login refused"), "{mech}: {e}");
    }
    // Credentials never in the target's Debug output.
    let c = ctx(&Cfg {
        topic: &topic,
        guarantee: Guarantee::AtLeastOnce,
        opts: &[],
        conn: &[("security", "sasl_plaintext")],
        secrets: &[("username", "u"), ("password", "zzsecretzz")],
        sink: 2,
        task: 0,
    });
    let t = KafkaTarget::new(&c).unwrap();
    assert!(!format!("{t:?} {:?}", c.conn).contains("zzsecretzz"));
    // A broker address off the allowlist is refused before connecting.
    let mut c = ctx(&Cfg {
        topic: &topic,
        guarantee: Guarantee::AtLeastOnce,
        opts: &[],
        conn: &[("bootstrap", "127.0.0.1:29094")],
        secrets: &[],
        sink: 2,
        task: 0,
    });
    c.conn.as_mut().unwrap().dest = Arc::new(Destinations::allow_list(&["127.0.0.1:29092"]));
    let e = KafkaTarget::new(&c)
        .unwrap()
        .deliver(&Batch::new(vec![rec(0, 1, "k", "{}")], 0))
        .await
        .unwrap_err();
    assert!(e.message.contains("destination_allowlist"), "{e}");
}

// ----- the runtime over an in-memory stream -----

struct Stream {
    parts: Vec<Vec<SinkRecord>>,
    committed: Mutex<HashMap<u32, u64>>,
}

struct Reader {
    s: Arc<Stream>,
    at: HashMap<u32, u64>,
    turn: u32,
}

#[async_trait]
impl SourceReader for Reader {
    async fn next_batch(&mut self, max: usize, _b: u64, wait: Duration) -> Result<Vec<SinkRecord>> {
        let mut out = Vec::new();
        let n = u32::try_from(self.s.parts.len()).unwrap();
        for _ in 0..n {
            let p = self.turn % n;
            self.turn += 1;
            let at = self.at.entry(p).or_insert(0);
            let part = &self.s.parts[p as usize];
            while out.len() < max && usize::try_from(*at).unwrap() < part.len() {
                out.push(part[usize::try_from(*at).unwrap()].clone());
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

struct NoDead;
#[async_trait]
impl DeadLetters for NoDead {
    async fn write(&self, rec: &SinkRecord, info: &DeadInfo) -> Result<()> {
        panic!("unexpected dead letter {} {info:?}", rec.idem_key);
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

fn stream(n: u64) -> Arc<Stream> {
    let mut parts = vec![Vec::new(); 3];
    for i in 0..n {
        let p = u32::try_from(i % 3).unwrap();
        let off = u64::try_from(parts[p as usize].len()).unwrap();
        parts[p as usize].push(rec(
            p,
            off,
            &format!("k{}", i % 50),
            &format!("{{\"i\":{i}}}"),
        ));
    }
    Arc::new(Stream {
        parts,
        committed: Mutex::new(HashMap::new()),
    })
}

fn done(s: &Stream) -> bool {
    let c = s.committed.lock().unwrap();
    s.parts
        .iter()
        .enumerate()
        .all(|(p, r)| c.get(&u32::try_from(p).unwrap()).copied().unwrap_or(0) >= r.len() as u64)
}

fn parts_of(s: &Arc<Stream>, c: &TargetCtx, ctl: &Arc<Ctl>, lanes: u16) -> TaskParts {
    TaskParts {
        info: c.task.clone(),
        source: Box::new(Reader {
            s: Arc::clone(s),
            at: s.committed.lock().unwrap().clone(),
            turn: 0,
        }),
        target: Box::new(KafkaTarget::new(c).unwrap()),
        dead: Arc::new(NoDead),
        control: Arc::clone(ctl) as Arc<dyn Control>,
        prepare: Arc::new(PassAll),
        settings: RunSettings {
            lanes,
            batch_max_records: 200,
            batch_max_wait: Duration::from_millis(20),
            retry_initial: Duration::from_millis(200),
            retry_max: Duration::from_millis(3000),
            ..RunSettings::default()
        },
        seed: Some(1),
        reject_via_source: false,
    }
}

async fn run_to_end(s: &Arc<Stream>, c: &TargetCtx, ctl: &Arc<Ctl>, lanes: u16, limit: Duration) {
    let cancel = Cancel::new();
    let h = tokio::spawn(run_task(parts_of(s, c, ctl, lanes), cancel.clone()));
    let start = Instant::now();
    while !done(s) {
        assert!(
            start.elapsed() < limit,
            "no progress; events {:?}",
            ctl.0.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throughput_and_a_broker_restart() {
    if !live() {
        return;
    }
    // Throughput: 30,000 records, 8 lanes, zstd.
    let topic = unique("r9sinks_tput");
    let c = ctx(&Cfg {
        topic: &topic,
        guarantee: Guarantee::AtLeastOnce,
        opts: &[],
        conn: &[],
        secrets: &[],
        sink: 3,
        task: 0,
    });
    // Create the topic first (auto-creation on the first metadata call).
    let t = KafkaTarget::new(&c).unwrap();
    deliver(&t, &Batch::new(vec![rec(0, 999_999, "warm", "{}")], 0)).await;
    let s = stream(30_000);
    let ctl = Arc::new(Ctl::default());
    let start = Instant::now();
    run_to_end(&s, &c, &ctl, 8, Duration::from_secs(120)).await;
    let secs = start.elapsed().as_secs_f64();
    let back = t.read_committed(&topic).await.unwrap();
    let n: usize = back.values().map(Vec::len).sum();
    assert!(n > 30_000);
    eprintln!(
        "throughput: 30000 records in {secs:.2} s = {:.0} records/s",
        30_000.0 / secs
    );

    // A broker restart mid-run: retries, then every record arrives.
    let topic = unique("r9sinks_restart");
    let c = ctx(&Cfg {
        topic: &topic,
        guarantee: Guarantee::AtLeastOnce,
        opts: &[],
        conn: &[],
        secrets: &[],
        sink: 4,
        task: 0,
    });
    let t = KafkaTarget::new(&c).unwrap();
    deliver(&t, &Batch::new(vec![rec(0, 999_999, "warm", "{}")], 0)).await;
    let s = stream(6_000);
    let ctl = Arc::new(Ctl::default());
    let cancel = Cancel::new();
    let mut parts = parts_of(&s, &c, &ctl, 4);
    parts.settings.batch_max_records = 20;
    let h = tokio::spawn(run_task(parts, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let restart = std::thread::spawn(|| {
        std::process::Command::new("docker")
            .args(["restart", "-t", "2", "r9sinks-kafka"])
            .output()
            .unwrap()
    });
    let start = Instant::now();
    while !done(&s) {
        assert!(
            start.elapsed() < Duration::from_secs(180),
            "no progress after restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(restart.join().unwrap().status.success());
    // The reader's old connections died with the broker: the first read
    // fails, the next one reconnects.
    let mut back = None;
    for _ in 0..50 {
        if let Ok(b) = t.read_committed(&topic).await {
            back = Some(b);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let back = back.unwrap();
    let keys: Vec<String> = back
        .values()
        .flatten()
        .map(|r| String::from_utf8(r.header("sprig.idempotency.key").unwrap().to_vec()).unwrap())
        .collect();
    let distinct: HashSet<&String> = keys.iter().collect();
    for p in &s.parts {
        for r in p {
            assert!(distinct.contains(&r.idem_key), "{} lost", r.idem_key);
        }
    }
    let evs = ctl.0.lock().unwrap().clone();
    assert!(
        evs.iter().any(|e| matches!(e, Event::Failing(_))),
        "{evs:?}"
    );
    eprintln!(
        "restart: 6000 records, {} delivered ({} duplicates), {:.1} s, events {:?}",
        keys.len() - 1,
        keys.len() - distinct.len(),
        start.elapsed().as_secs_f64(),
        evs.len()
    );
}

#[tokio::test]
async fn an_uncommitted_transaction_is_aborted_by_the_next_producer() {
    if !live() {
        return;
    }
    let topic = unique("r9sinks_eo_abort");
    let offsets = unique("r9sinks_offsets");
    let eo_opts = [
        ("kafka.offsets_topic", offsets.as_str()),
        ("partitioner", "source"),
    ];
    let cfg = |task| Cfg {
        topic: &topic,
        guarantee: Guarantee::ExactlyOnce,
        opts: &eo_opts,
        conn: &[],
        secrets: &[],
        sink: 5,
        task,
    };
    // A committed batch.
    let c = ctx(&cfg(0));
    let mut a = KafkaTarget::new(&c).unwrap();
    let start = Instant::now();
    loop {
        match a.open(&c.task).await {
            Ok(p) => {
                assert!(p.is_empty());
                break;
            }
            Err(e) => {
                assert!(start.elapsed() < Duration::from_secs(30), "{e}");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    }
    let first: Vec<SinkRecord> = (0..5).map(|i| rec(0, i, "k", "{}")).collect();
    let mut b = Batch::new(first, 0);
    b.ends = sprig_sinks::model::unit_ends(&b.records);
    assert_eq!(deliver(&a, &b).await, BatchOutcome::All(Outcome::Ok));
    // A transaction left open, as a crash before EndTxn leaves it.
    let second: Vec<SinkRecord> = (5..10).map(|i| rec(0, i, "k", "{}")).collect();
    let mut b2 = Batch::new(second, 0);
    b2.ends = sprig_sinks::model::unit_ends(&b2.records);
    a.write_uncommitted(&b2).await.unwrap();
    drop(a);
    // The next producer of the task fences it and aborts its transaction.
    let mut n = KafkaTarget::new(&c).unwrap();
    let pos = n.open(&c.task).await.unwrap();
    assert_eq!(pos.len(), 1);
    assert!(
        matches!(pos[0].pos, Position::Stream { offset: 4, .. }),
        "{pos:?}"
    );
    let back = n.read_committed(&topic).await.unwrap();
    let offs: Vec<String> = back
        .values()
        .flatten()
        .map(|r| String::from_utf8(r.header("sprig.idempotency.key").unwrap().to_vec()).unwrap())
        .collect();
    assert_eq!(offs.len(), 5, "{offs:?}");
    assert!(
        offs.iter()
            .all(|k| k.ends_with("/0") && !k.contains("/0/5/"))
    );
    // The fenced producer's next write fails as fenced (39227).
    let c2 = ctx(&cfg(0));
    let mut old = KafkaTarget::new(&c2).unwrap();
    old.open(&c2.task).await.unwrap();
    n.open(&c.task).await.unwrap(); // a newer epoch again
    let e = old.deliver(&b2).await.unwrap_err();
    assert!(e.message.contains("SINK_FENCED"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_once_through_task_kills() {
    if !live() {
        return;
    }
    let topic = unique("r9sinks_eo_kills");
    let offsets = unique("r9sinks_offsets");
    let opts = [
        ("kafka.offsets_topic", offsets.as_str()),
        ("partitioner", "source"),
    ];
    let c = ctx(&Cfg {
        topic: &topic,
        guarantee: Guarantee::ExactlyOnce,
        opts: &opts,
        conn: &[],
        secrets: &[],
        sink: 6,
        task: 0,
    });
    // Create the data topic with three partitions first.
    {
        let warm = ctx(&Cfg {
            topic: &topic,
            guarantee: Guarantee::AtLeastOnce,
            opts: &[],
            conn: &[],
            secrets: &[],
            sink: 99,
            task: 0,
        });
        let t = KafkaTarget::new(&warm).unwrap();
        let start = Instant::now();
        // Metadata until the topic exists (no record written).
        while t
            .read_committed(&topic)
            .await
            .map_or(true, |m| m.is_empty())
        {
            assert!(start.elapsed() < Duration::from_secs(30));
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let s = stream(5_000);
    let ctl = Arc::new(Ctl::default());
    let mut kills = 0;
    let mut seed = 12_345u64;
    let start = Instant::now();
    while kills < 10 && !done(&s) {
        let mut parts = parts_of(&s, &c, &ctl, 1);
        parts.settings.batch_max_records = 50;
        let h = tokio::spawn(run_task(parts, Cancel::new()));
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        tokio::time::sleep(Duration::from_millis(300 + (seed >> 33) % 1200)).await;
        h.abort();
        let _ = h.await;
        kills += 1;
    }
    run_to_end(&s, &c, &ctl, 1, Duration::from_secs(120)).await;
    let secs = start.elapsed().as_secs_f64();
    let t = KafkaTarget::new(&c).unwrap();
    let back = t.read_committed(&topic).await.unwrap();
    let mut seen: HashSet<String> = HashSet::new();
    let mut total = 0;
    let mut per_part: BTreeMap<i32, u64> = BTreeMap::new();
    for (p, rs) in &back {
        let mut last: Option<u64> = None;
        for r in rs {
            let k = String::from_utf8(r.header("sprig.idempotency.key").unwrap().to_vec()).unwrap();
            assert!(seen.insert(k.clone()), "{k} twice");
            let off: u64 = k.split('/').nth(3).unwrap().parse().unwrap();
            if let Some(l) = last {
                assert!(off > l, "partition {p}: {off} after {l}");
            }
            last = Some(off);
            total += 1;
        }
        per_part.insert(*p, last.unwrap_or(0));
    }
    assert_eq!(total, 5_000, "each record exactly once");
    eprintln!(
        "exactly once: 5000 records, {kills} task kills, each read_committed once in partition order, {secs:.1} s"
    );
}
