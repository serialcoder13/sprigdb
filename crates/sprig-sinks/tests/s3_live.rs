//! The S3 target against MinIO (chapter 51, "Testing"): `r9sinks-minio`
//! (quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z) on 127.0.0.1:29000.
//! Skipped unless `SPRIG_SINKS_S3_LIVE=1`. Every test uses its own prefix
//! in the bucket `r9sinks`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::doc_markdown,
    clippy::case_sensitive_file_extension_comparisons
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{Map, Value, json};
use sprig_common::{ErrorKind, Result, SprigCode};
use sprig_sinks::model::{
    Batch, BatchOutcome, ConnKind, Guarantee, Outcome, Position, SinkDef, SinkId, SinkRecord,
    SinkState, SourceSpec, StartAt, TargetSpec, TaskEpoch, stream_key,
};
use sprig_sinks::policy::Destinations;
use sprig_sinks::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, PassAll, RunSettings, SourceReader,
    Target, TaskInfo, TaskParts, run_task,
};
use sprig_sinks::target::s3::client::{Put, S3Client};
use sprig_sinks::target::s3::{S3Target, run_settings_override};
use sprig_sinks::target::{Conn, MemSecrets, Opts, TargetCtx};

const BUCKET: &str = "r9sinks";

fn live() -> bool {
    std::env::var("SPRIG_SINKS_S3_LIVE").is_ok_and(|v| v == "1")
}

fn conn(allow: &[&str]) -> Conn {
    let mut options = Map::new();
    options.insert("endpoint".into(), json!("http://127.0.0.1:29000"));
    options.insert("region".into(), json!("us-east-1"));
    options.insert("bucket".into(), json!(BUCKET));
    Conn {
        name: "lake".into(),
        kind: ConnKind::S3,
        options,
        secrets: Arc::new(MemSecrets::of(&[
            ("access_key_id", "r9sinksadmin"),
            ("secret_access_key", "r9sinks-secret-key-01"),
        ])),
        dest: Arc::new(Destinations::allow_list(allow)),
    }
}

fn ctx(prefix: &str, guarantee: Guarantee, opts: &[(&str, &str)], task: u32) -> TargetCtx {
    let sink = SinkDef {
        id: SinkId(77),
        db_id: 1,
        name: "lake_sink".into(),
        source: SourceSpec::Stream {
            id: 9,
            name: "order_events".into(),
            partitions: 3,
        },
        filter_sql: None,
        projection: None,
        transform: None,
        target: TargetSpec::S3 {
            prefix: prefix.into(),
        },
        connections: Vec::new(),
        guarantee,
        dead_letter: None,
        start_at: StartAt::Latest,
        owner_user_id: 1,
        state: SinkState::Running,
        state_reason: None,
        definition_ver: 1,
        options: BTreeMap::new(),
    };
    TargetCtx {
        task: TaskInfo {
            tenant: 1,
            sink: 77,
            task_no: task,
            epoch: TaskEpoch(1),
        },
        sink: Arc::new(sink),
        conn: Some(conn(&["127.0.0.1:29000"])),
        extra: Vec::new(),
        opts: Opts(
            opts.iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        ),
        db_name: "shop".into(),
    }
}

fn client() -> S3Client {
    S3Client::new(&conn(&["127.0.0.1:29000"]), &Opts::default()).unwrap()
}

fn unique(tag: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("t{n}-{tag}/")
}

fn rec(p: u32, o: u64, value: String) -> SinkRecord {
    SinkRecord {
        key: Some(format!("k{}", o % 17).into_bytes()),
        value: Some(value.into_bytes()),
        headers: Vec::new(),
        op: None,
        before: None,
        after: None,
        ts_ms: 1_762_682_700_000 + i64::try_from(o).unwrap() * 1000,
        idem_key: stream_key(9, p, o, 0),
        pos: Position::Stream {
            stream: 9,
            partition: p,
            offset: o,
            epoch: 0,
        },
    }
}

fn simple(p: u32, o: u64) -> SinkRecord {
    rec(
        p,
        o,
        format!(
            r#"{{"id":{o},"name":"n{o}","amount":{}.5,"ok":true}}"#,
            o % 9
        ),
    )
}

async fn setup() -> S3Client {
    let c = client();
    c.create_bucket().await.unwrap();
    c
}

/// Every object's records (`_sprig.key` / `_sprig_key`) under `prefix`,
/// manifests and pending files left out.
async fn read_keys(c: &S3Client, prefix: &str) -> (Vec<String>, usize) {
    let mut keys = Vec::new();
    let mut objects = 0;
    for l in c.list_objects(prefix).await.unwrap() {
        if l.key.contains("/_sprig/")
            || l.key.contains("_sprig/") && l.key.starts_with(&format!("{prefix}_sprig"))
        {
            continue;
        }
        objects += 1;
        let body = c.get_object(&l.key).await.unwrap().unwrap();
        keys.extend(decode_keys(&l.key, &body));
    }
    (keys, objects)
}

fn decode_keys(name: &str, body: &Bytes) -> Vec<String> {
    let raw: Vec<u8> = if name.ends_with(".gz") {
        let mut d = flate2_read(body);
        let mut v = Vec::new();
        d.read_to_end(&mut v).unwrap();
        v
    } else if name.ends_with(".zst") {
        zstd_decode(body)
    } else {
        body.to_vec()
    };
    if name.ends_with(".parquet") {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let rd = SerializedFileReader::new(Bytes::from(raw)).unwrap();
        return rd
            .get_row_iter(None)
            .unwrap()
            .map(|r| {
                let r = r.unwrap();
                r.get_column_iter()
                    .find(|(n, _)| n.as_str() == "_sprig_key")
                    .map(|(_, f)| f.to_string().trim_matches('"').to_owned())
                    .unwrap()
            })
            .collect();
    }
    let text = String::from_utf8(raw).unwrap();
    if name.contains(".csv") {
        return text
            .split("\r\n")
            .skip(1)
            .filter(|l| !l.is_empty())
            .map(|l| l.split(',').next().unwrap().to_owned())
            .collect();
    }
    text.lines()
        .map(|l| {
            let v: Value = serde_json::from_str(l).unwrap();
            v["_sprig"]["key"].as_str().unwrap().to_owned()
        })
        .collect()
}

fn flate2_read(b: &Bytes) -> impl std::io::Read + '_ {
    flate2::read::GzDecoder::new(&b[..])
}

fn zstd_decode(b: &Bytes) -> Vec<u8> {
    zstd::decode_all(&b[..]).unwrap()
}

#[tokio::test]
async fn every_format_and_compression_and_conditional_writes() {
    if !live() {
        return;
    }
    let c = setup().await;
    // Conditional writes on MinIO (open question 7).
    let k = format!("{}probe.txt", unique("cond"));
    assert!(matches!(
        c.put_object(&k, Bytes::from_static(b"a"), "text/plain", true)
            .await
            .unwrap(),
        Put::Written(_)
    ));
    assert_eq!(
        c.put_object(&k, Bytes::from_static(b"b"), "text/plain", true)
            .await
            .unwrap(),
        Put::Existed
    );
    assert_eq!(
        c.get_object(&k).await.unwrap().unwrap(),
        Bytes::from_static(b"a")
    );
    eprintln!("MinIO: PutObject with If-None-Match: * on an existing key answers 412");

    let recs: Vec<SinkRecord> = (0..50).map(|o| simple(o32(o % 3), o)).collect();
    for (format, comp, cols) in [
        ("jsonl", "none", None),
        ("jsonl", "gzip", None),
        ("jsonl", "zstd", None),
        ("csv", "none", Some("id:int64,name,amount:double,ok:bool")),
        ("csv", "gzip", Some("id:int64,name")),
        ("csv", "zstd", Some("id:int64,name")),
        (
            "parquet",
            "none",
            Some("id:int64,name,amount:double,ok:bool"),
        ),
    ] {
        let prefix = unique(&format!("{format}-{comp}"));
        let mut o = vec![("format", format), ("compression", comp)];
        if let Some(c) = cols {
            o.push(("columns", c));
        }
        let mut t = S3Target::new(&ctx(
            &format!("{prefix}{{yyyy}}/{{MM}}/"),
            Guarantee::AtLeastOnce,
            &o,
            0,
        ))
        .unwrap();
        t.open(&TaskInfo {
            tenant: 1,
            sink: 77,
            task_no: 0,
            epoch: TaskEpoch(1),
        })
        .await
        .unwrap();
        let b = Batch::new(recs.clone(), 0);
        assert_eq!(t.deliver(&b).await.unwrap(), BatchOutcome::All(Outcome::Ok));
        // A resend makes the same names: nothing new.
        assert_eq!(t.deliver(&b).await.unwrap(), BatchOutcome::All(Outcome::Ok));
        let (keys, objects) = read_keys(&c, &prefix).await;
        assert_eq!(objects, 3, "{format}/{comp}: one object per partition");
        let set: HashSet<_> = keys.iter().collect();
        assert_eq!((keys.len(), set.len()), (50, 50), "{format}/{comp}");
        let names = c.list_objects(&prefix).await.unwrap();
        assert!(
            names[0].key.starts_with(&format!("{prefix}2025/11/77-0-p")),
            "{}",
            names[0].key
        );
        eprintln!(
            "{format}/{comp}: 50 records in {objects} objects, e.g. {}",
            names[0].key
        );
    }
}

fn o32(x: u64) -> u32 {
    u32::try_from(x).unwrap()
}

#[tokio::test]
async fn multipart_and_failed_completion_and_sweep() {
    if !live() {
        return;
    }
    let c = setup().await;
    let prefix = unique("multipart");
    // ~12 MiB of JSON Lines, no compression, 5 MiB parts: 3 parts.
    let big = "x".repeat(60_000);
    let recs: Vec<SinkRecord> = (0..200)
        .map(|o| rec(0, o, format!(r#"{{"id":{o},"pad":"{big}"}}"#)))
        .collect();
    let opts = [
        ("format", "jsonl"),
        ("compression", "none"),
        ("s3.part_bytes", "5242880"),
    ];
    let mut t = S3Target::new(&ctx(&prefix, Guarantee::ExactlyOnce, &opts, 0)).unwrap();
    t.open(&TaskInfo {
        tenant: 1,
        sink: 77,
        task_no: 0,
        epoch: TaskEpoch(1),
    })
    .await
    .unwrap();
    let started = Instant::now();
    assert_eq!(
        t.deliver(&Batch::new(recs.clone(), 0)).await.unwrap(),
        BatchOutcome::All(Outcome::Ok)
    );
    let took = started.elapsed();
    let listed = c.list_objects(&prefix).await.unwrap();
    let data: Vec<_> = listed
        .iter()
        .filter(|l| !l.key.contains("_sprig/"))
        .collect();
    assert_eq!(data.len(), 1);
    assert!(data[0].size > 10 << 20, "{}", data[0].size);
    let (keys, _) = read_keys(&c, &prefix).await;
    assert_eq!(keys.len(), 200);
    eprintln!("multipart: {} bytes in 3 parts in {took:?}", data[0].size);
    // The resend under exactly once writes nothing.
    assert_eq!(
        t.deliver(&Batch::new(recs, 0)).await.unwrap(),
        BatchOutcome::All(Outcome::Ok)
    );
    assert_eq!(c.list_objects(&prefix).await.unwrap().len(), listed.len());

    // A failed completion (a wrong part ETag) is a destination-wide 39221,
    // and the upload can be aborted.
    let key = format!("{prefix}77-9-broken.jsonl");
    let id = c
        .create_multipart(&key, "application/x-ndjson")
        .await
        .unwrap();
    let e1 = c
        .upload_part(&key, &id, 1, Bytes::from(vec![b'a'; 5 << 20]))
        .await
        .unwrap();
    let wrong = format!("{}0\"", e1.trim_end_matches('"'));
    let e = c
        .complete_multipart(&key, &id, &[wrong], true)
        .await
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Sprig(SprigCode::SinkDeliveryFailed));
    assert!(e.retryable);
    eprintln!("failed completion: {}", e.message);
    assert!(c.head_object(&key).await.unwrap().is_none());
    // MinIO lists multipart uploads only for an exact object name (open
    // question 11): by prefix it finds none.
    assert!(c.list_uploads(&prefix).await.unwrap().is_empty());
    assert_eq!(c.list_uploads(&key).await.unwrap().len(), 1);
    eprintln!("MinIO: ListMultipartUploads by prefix finds nothing; by exact key finds the upload");
    // A crashed task's pending.json names the object; the sweep finds its
    // upload through it.
    c.put_object(
        &format!("{prefix}_sprig/77/0/pending.json"),
        Bytes::from(
            serde_json::to_vec(&json!({"objects": [key.clone()], "manifests": []})).unwrap(),
        ),
        "application/json",
        false,
    )
    .await
    .unwrap();
    let n = t.sweep_orphans(Duration::ZERO).await.unwrap();
    assert!(n >= 1, "{n}");
    assert!(c.list_uploads(&key).await.unwrap().is_empty());
}

#[tokio::test]
async fn refused_destinations_and_bad_credentials() {
    if !live() {
        return;
    }
    // Off the allowlist: 39213 at the first connection.
    let mut cx = ctx(&unique("refused"), Guarantee::AtLeastOnce, &[], 0);
    cx.conn = Some(conn(&["127.0.0.1:29001"]));
    let t = S3Target::new(&cx).unwrap();
    let e = t
        .deliver(&Batch::new(vec![simple(0, 1)], 0))
        .await
        .unwrap_err();
    assert_eq!(
        e.kind,
        ErrorKind::Sprig(SprigCode::SinkDestinationNotAllowed)
    );
    // Wrong secret: MinIO refuses the signature, a destination-wide failure.
    let mut cx = ctx(&unique("badkey"), Guarantee::AtLeastOnce, &[], 0);
    let mut c = conn(&["127.0.0.1:29000"]);
    c.secrets = Arc::new(MemSecrets::of(&[
        ("access_key_id", "r9sinksadmin"),
        ("secret_access_key", "wrong-secret-0000"),
    ]));
    cx.conn = Some(c);
    let t = S3Target::new(&cx).unwrap();
    let e = t
        .deliver(&Batch::new(vec![simple(0, 1)], 0))
        .await
        .unwrap_err();
    assert!(e.message.contains("403"), "{}", e.message);
    // CSV from a stream without columns: 39231.
    let e = S3Target::new(&ctx(
        "x/",
        Guarantee::AtLeastOnce,
        &[("format", "parquet")],
        0,
    ))
    .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Sprig(SprigCode::SinkFormatUnsupported));
}

// ---- the runtime over an in-memory stream ----

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
            while out.len() < max && (*at as usize) < part.len() {
                out.push(part[*at as usize].clone());
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

#[derive(Default)]
struct Dead(Mutex<Vec<String>>);
#[async_trait]
impl DeadLetters for Dead {
    async fn write(&self, rec: &SinkRecord, _i: &DeadInfo) -> Result<()> {
        self.0.lock().unwrap().push(rec.idem_key.clone());
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

struct Run {
    took: Duration,
    kills: u32,
    events: Vec<Event>,
}

/// Runs tasks until every record is committed, aborting the task at the
/// given moments (ms after each start), and optionally stopping MinIO.
async fn run(
    prefix: &str,
    stream: &Arc<Stream>,
    guarantee: Guarantee,
    opts: &[(&str, &str)],
    kills_at: &[u64],
    outage: Option<(u64, u64)>,
) -> Run {
    let total: HashMap<u32, u64> = (0..stream.parts.len())
        .map(|p| (u32::try_from(p).unwrap(), stream.parts[p].len() as u64))
        .collect();
    let ctl = Arc::new(Ctl::default());
    let started = Instant::now();
    let mut kills = 0u32;
    let mut life = 0u64;
    if let Some((after, down)) = outage {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(after)).await;
            docker(&["stop", "-t", "1", "r9sinks-minio"]);
            tokio::time::sleep(Duration::from_millis(down)).await;
            docker(&["start", "r9sinks-minio"]);
        });
    }
    loop {
        life += 1;
        let cx = ctx(prefix, guarantee, opts, 0);
        let target = S3Target::new(&cx).unwrap();
        let mut settings = RunSettings {
            lanes: 4,
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_millis(1000),
            settle_every: Duration::from_millis(50),
            ..RunSettings::default()
        };
        run_settings_override(&cx.opts, &mut settings).unwrap();
        settings.batch_max_wait = Duration::from_millis(100);
        let parts = TaskParts {
            info: TaskInfo {
                tenant: 1,
                sink: 77,
                task_no: 0,
                epoch: TaskEpoch(life),
            },
            source: Box::new(Reader {
                s: Arc::clone(stream),
                at: stream.committed.lock().unwrap().clone(),
                turn: 0,
            }),
            target: Box::new(target),
            dead: Arc::new(Dead::default()),
            control: Arc::clone(&ctl) as Arc<dyn Control>,
            prepare: Arc::new(PassAll),
            settings,
            seed: Some(life),
            reject_via_source: false,
        };
        let cancel = Cancel::new();
        let mut h = tokio::spawn(run_task(parts, cancel.clone()));
        let kill_at = kills_at
            .get(kills as usize)
            .map(|ms| Instant::now() + Duration::from_millis(*ms));
        let finished = loop {
            let done = {
                let c = stream.committed.lock().unwrap();
                total
                    .iter()
                    .all(|(p, n)| c.get(p).copied().unwrap_or(0) >= *n)
            };
            if done {
                cancel.cancel();
                let _ = (&mut h).await;
                break true;
            }
            if kill_at.is_some_and(|k| Instant::now() >= k) {
                h.abort();
                let _ = (&mut h).await;
                kills += 1;
                break false;
            }
            assert!(started.elapsed() < Duration::from_secs(240), "no progress");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        if finished {
            break;
        }
    }
    let events = ctl.0.lock().unwrap().clone();
    Run {
        took: started.elapsed(),
        kills,
        events,
    }
}

fn docker(args: &[&str]) {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn stream(n: u64, parts: u32) -> Arc<Stream> {
    let mut ps = vec![Vec::new(); parts as usize];
    for i in 0..n {
        let p = u32::try_from(i % u64::from(parts)).unwrap();
        let o = ps[p as usize].len() as u64;
        ps[p as usize].push(simple(p, o));
    }
    Arc::new(Stream {
        parts: ps,
        committed: Mutex::new(HashMap::new()),
    })
}

#[tokio::test]
async fn exactly_once_through_task_kills() {
    if !live() {
        return;
    }
    let c = setup().await;
    for (format, cols) in [
        ("jsonl", None),
        ("parquet", Some("id:int64,name,amount:double,ok:bool")),
    ] {
        let prefix = unique(&format!("eo-{format}"));
        let s = stream(6000, 3);
        let mut o = vec![("format", format), ("s3.file_max_records", "150")];
        if let Some(c) = cols {
            o.push(("columns", c));
        }
        let r = run(
            &format!("{prefix}{{yyyy}}/{{MM}}/{{dd}}/"),
            &s,
            Guarantee::ExactlyOnce,
            &o,
            &[400, 900, 300, 1200, 700, 500, 1000, 600],
            None,
        )
        .await;
        let (keys, objects) = read_keys(&c, &prefix).await;
        let set: HashSet<_> = keys.iter().collect();
        assert_eq!(set.len(), 6000, "{format}: every record");
        assert_eq!(keys.len(), 6000, "{format}: no record twice in the objects");
        let manifests: Vec<_> = c
            .list_objects(&format!("{prefix}_sprig/77/0/"))
            .await
            .unwrap()
            .into_iter()
            .filter(|l| l.key.ends_with(".json") && !l.key.ends_with("pending.json"))
            .collect();
        // Manifests name every object once.
        let mut named = Vec::new();
        for m in &manifests {
            let b = c.get_object(&m.key).await.unwrap().unwrap();
            let v: Value = serde_json::from_slice(&b).unwrap();
            for o in v["objects"].as_array().unwrap() {
                named.push(o["name"].as_str().unwrap().to_owned());
            }
        }
        let named_set: HashSet<_> = named.iter().collect();
        assert_eq!(named.len(), named_set.len());
        assert_eq!(named.len(), objects, "{format}: every object in a manifest");
        eprintln!(
            "exactly once ({format}): 6000 records once each in {objects} objects and {} manifests, {} kills, {:.1} s ({:.0} records/s)",
            manifests.len(),
            r.kills,
            r.took.as_secs_f64(),
            6000.0 / r.took.as_secs_f64()
        );
    }
}

#[tokio::test]
async fn minio_down_then_up() {
    if !live() {
        return;
    }
    let c = setup().await;
    let prefix = unique("outage");
    let s = stream(4000, 3);
    let r = run(
        &prefix,
        &s,
        Guarantee::AtLeastOnce,
        &[
            ("format", "jsonl"),
            ("compression", "gzip"),
            ("s3.file_max_records", "100"),
        ],
        &[],
        Some((300, 4000)),
    )
    .await;
    // Wait for MinIO to answer again before reading back.
    for _ in 0..50 {
        if c.list_objects(&prefix).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (keys, objects) = read_keys(&c, &prefix).await;
    let set: HashSet<_> = keys.iter().collect();
    assert_eq!(set.len(), 4000, "every record arrived");
    assert!(
        r.events.iter().any(|e| matches!(e, Event::Failing(_))),
        "{:?}",
        r.events
    );
    assert!(
        matches!(r.events.last(), Some(Event::Recovered)),
        "{:?}",
        r.events
    );
    eprintln!(
        "outage: 4000 records ({} copies, {} duplicates with equal keys) in {objects} objects, {:.1} s, events {:?}",
        keys.len(),
        keys.len() - set.len(),
        r.took.as_secs_f64(),
        r.events.first()
    );
}

#[tokio::test]
async fn at_least_once_throughput() {
    if !live() {
        return;
    }
    let c = setup().await;
    let prefix = unique("tput");
    let s = stream(50_000, 4);
    let r = run(
        &prefix,
        &s,
        Guarantee::AtLeastOnce,
        &[
            ("format", "jsonl"),
            ("compression", "zstd"),
            ("s3.file_max_records", "5000"),
        ],
        &[],
        None,
    )
    .await;
    let (keys, objects) = read_keys(&c, &prefix).await;
    assert_eq!(keys.len(), 50_000);
    eprintln!(
        "throughput: 50000 records in {objects} objects, {:.1} s ({:.0} records/s)",
        r.took.as_secs_f64(),
        50_000.0 / r.took.as_secs_f64()
    );
}

#[tokio::test]
async fn pending_is_finished_or_undone_at_open() {
    if !live() {
        return;
    }
    let c = setup().await;
    let prefix = unique("pending");
    let info = TaskInfo {
        tenant: 1,
        sink: 77,
        task_no: 0,
        epoch: TaskEpoch(1),
    };
    let opts = [("format", "jsonl")];
    // A batch whose objects were written but whose manifest wasn't: the
    // next open writes the manifest and resumes after it.
    let mut t = S3Target::new(&ctx(&prefix, Guarantee::ExactlyOnce, &opts, 0)).unwrap();
    assert!(t.open(&info).await.unwrap().is_empty());
    let recs: Vec<SinkRecord> = (0..10).map(|o| simple(0, o)).collect();
    let mut b = Batch::new(recs, 0);
    b.ends = vec![b.records[9].pos.clone()];
    t.deliver(&b).await.unwrap();
    let manifest = format!("{prefix}_sprig/77/0/u0/{:020}.json", 9);
    let pending = format!("{prefix}_sprig/77/0/pending.json");
    assert!(c.head_object(&manifest).await.unwrap().is_some());
    c.delete_object(&manifest).await.unwrap();
    let mut t = S3Target::new(&ctx(&prefix, Guarantee::ExactlyOnce, &opts, 0)).unwrap();
    let ps = t.open(&info).await.unwrap();
    assert_eq!(ps.len(), 1);
    assert_eq!(ps[0].pos, b.records[9].pos);
    assert!(c.head_object(&manifest).await.unwrap().is_some());
    assert!(c.head_object(&pending).await.unwrap().is_none());
    // A batch cut short (one of its objects never written): its other
    // objects are removed, so listing readers never see that range twice.
    let p1: Vec<SinkRecord> = (0..5).map(|o| simple(1, o)).collect();
    let mut b1 = Batch::new(p1, 0);
    b1.ends = vec![b1.records[4].pos.clone()];
    t.deliver(&b1).await.unwrap();
    let written = c
        .list_objects(&prefix)
        .await
        .unwrap()
        .into_iter()
        .find(|l| l.key.contains("77-0-p1-"))
        .unwrap()
        .key;
    let m1 = format!("{prefix}_sprig/77/0/u1/{:020}.json", 4);
    c.delete_object(&m1).await.unwrap();
    let fake = json!({
        "objects": [written.clone(), format!("{prefix}77-0-p2-never.jsonl.gz")],
        "manifests": [{"key": m1, "body": {}}],
    });
    c.put_object(
        &pending,
        Bytes::from(serde_json::to_vec(&fake).unwrap()),
        "application/json",
        false,
    )
    .await
    .unwrap();
    let mut t = S3Target::new(&ctx(&prefix, Guarantee::ExactlyOnce, &opts, 0)).unwrap();
    let ps = t.open(&info).await.unwrap();
    assert_eq!(
        ps.len(),
        1,
        "partition 1 has no position: its object was undone"
    );
    assert!(c.head_object(&written).await.unwrap().is_none());
    assert!(c.head_object(&m1).await.unwrap().is_none());
}
