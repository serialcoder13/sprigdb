//! The S3 target.
//!
//! ```text
//! deliver(batch b):
//!   exactly once: drop records at or below their unit's last manifest
//!     (a resend after a lost answer writes nothing twice)
//!   group records by (rendered PREFIX, partition or shard); each group is
//!     one object <prefix><sink>-<task>-p<unit>-<first, 20 digits>-<last, 20 digits>.<ext>
//!     (queue records: <sink>-<task>-q<batch id of the group>)
//!   exactly once: write _sprig/<sink>/<task>/pending.json (the objects and
//!     the manifests they will get)
//!   upload each object: PutObject, or multipart above s3.part_bytes, both
//!     with If-None-Match: * (412: it exists, which counts as written)
//!   exactly once: per unit, the manifest
//!     <root>_sprig/<sink>/<task>/u<unit>/<last, 20 digits>.json
//!     {unit, first, last, end, epoch, objects: [{name, first, last, records, bytes, etag}]}
//! open (exactly once):
//!   pending.json whose manifests are missing: all its objects exist →
//!     write the manifests; otherwise delete the objects that exist (a
//!     crash during the uploads), so listing readers never see a range twice
//!   each unit's newest manifest is its position (it wins over the
//!     stream's committed offsets)
//! ```
//!
//! Object names depend only on the records, so a resent batch makes the
//! same names, and the conditional write turns it into a no-op. Names use
//! the record's own time for `{yyyy}`...`{mm}`, never the wall clock.
//! The per-unit manifests replace the chapter's `sink_tasks.pending`: the
//! destination keeps the pending range itself, so a restart needs no
//! catalog write per file.

pub mod client;
pub mod format;
pub mod sigv4;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode};

use self::client::{Put, S3Client};
use self::format::{Column, Compress, Format, encode, infer_columns, parse_columns};
use crate::model::{
    Batch, BatchOutcome, DestPosition, Guarantee, Outcome, Position, SinkRecord, SourceKind,
    TargetSpec, TaskEpoch, bad_option, batch_id, err,
};
use crate::runtime::{RunSettings, Target, TaskInfo};
use crate::target::{Opts, TargetCtx};

/// One `PREFIX` template piece.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seg {
    /// Literal text.
    Lit(String),
    /// A variable.
    Var(Var),
}

/// A `PREFIX` variable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Var {
    /// `{yyyy}`.
    Year,
    /// `{MM}`.
    Month,
    /// `{dd}`.
    Day,
    /// `{HH}`.
    Hour,
    /// `{mm}`.
    Minute,
    /// `{db}`.
    Db,
    /// `{source}`.
    Source,
    /// `{table}`.
    Table,
    /// `{op}`.
    Op,
    /// `{partition}`.
    Partition,
    /// `{key_hash}`.
    KeyHash,
    /// `{tenant}`.
    Tenant,
}

/// Parses a `PREFIX` template.
///
/// # Errors
/// 39218 for an unknown variable or an unclosed brace.
pub fn parse_template(t: &str) -> Result<Vec<Seg>> {
    let mut out = Vec::new();
    let mut rest = t;
    while let Some(i) = rest.find('{') {
        if i > 0 {
            out.push(Seg::Lit(rest[..i].to_owned()));
        }
        let after = &rest[i + 1..];
        let j = after
            .find('}')
            .ok_or_else(|| bad_option(format!("PREFIX '{t}': an unclosed {{")))?;
        let v = match &after[..j] {
            "yyyy" => Var::Year,
            "MM" => Var::Month,
            "dd" => Var::Day,
            "HH" => Var::Hour,
            "mm" => Var::Minute,
            "db" => Var::Db,
            "source" => Var::Source,
            "table" => Var::Table,
            "op" => Var::Op,
            "partition" => Var::Partition,
            "key_hash" => Var::KeyHash,
            "tenant" => Var::Tenant,
            other => {
                return Err(bad_option(format!(
                    "PREFIX '{t}': unknown variable {{{other}}}"
                )));
            }
        };
        out.push(Seg::Var(v));
        rest = &after[j + 1..];
    }
    if rest.contains('}') {
        return Err(bad_option(format!("PREFIX '{t}': a stray }}")));
    }
    if !rest.is_empty() {
        out.push(Seg::Lit(rest.to_owned()));
    }
    Ok(out)
}

/// The template's fixed root: its literal text up to the last `/` before
/// the first variable. Manifests live under `<root>_sprig/`.
#[must_use]
pub fn template_root(segs: &[Seg]) -> String {
    let mut lit = String::new();
    for s in segs {
        match s {
            Seg::Lit(l) => lit.push_str(l),
            Seg::Var(_) => break,
        }
    }
    match lit.rfind('/') {
        Some(i) => lit[..=i].to_owned(),
        None => String::new(),
    }
}

/// What rendering reads besides the record.
#[derive(Debug, Clone)]
pub struct RenderCtx {
    /// `{db}`.
    pub db: String,
    /// `{source}`.
    pub source: String,
    /// `{tenant}`.
    pub tenant: u64,
    /// `key_buckets`.
    pub key_buckets: u64,
}

/// Renders `segs` for `r`.
#[must_use]
pub fn render(segs: &[Seg], r: &SinkRecord, cx: &RenderCtx) -> String {
    let t = crate::target::kafka::rfc3339(r.ts_ms);
    let mut out = String::new();
    for s in segs {
        match s {
            Seg::Lit(l) => out.push_str(l),
            Seg::Var(v) => match v {
                Var::Year => out.push_str(&t[0..4]),
                Var::Month => out.push_str(&t[5..7]),
                Var::Day => out.push_str(&t[8..10]),
                Var::Hour => out.push_str(&t[11..13]),
                Var::Minute => out.push_str(&t[14..16]),
                Var::Db => out.push_str(&cx.db),
                Var::Source => out.push_str(&cx.source),
                Var::Table => {
                    let table = r
                        .headers
                        .iter()
                        .find(|h| h.0 == "sprig.table")
                        .and_then(|h| std::str::from_utf8(&h.1).ok())
                        .unwrap_or(&cx.source);
                    out.push_str(table);
                }
                Var::Op => out.push_str(r.op.map_or("event", crate::model::ChangeOp::letter)),
                Var::Partition => {
                    out.push_str(&r.pos.unit_seq().map_or(0, |(u, _)| u).to_string());
                }
                Var::KeyHash => {
                    let k = r.key.as_deref().unwrap_or(r.idem_key.as_bytes());
                    let h = twox_hash::XxHash3_64::oneshot(k) % cx.key_buckets.max(1);
                    out.push_str(&h.to_string());
                }
                // A fan-in sink's record: its member.
                Var::Tenant => out.push_str(
                    &crate::source::record_member(r)
                        .unwrap_or(cx.tenant)
                        .to_string(),
                ),
            },
        }
    }
    out
}

/// An object's name (see the module docs).
#[must_use]
pub fn object_name(prefix: &str, sink: u64, task: u32, recs: &[&SinkRecord], ext: &str) -> String {
    let first = recs.first().and_then(|r| r.pos.unit_seq());
    let last = recs.last().and_then(|r| r.pos.unit_seq());
    if let (Some((u, a)), Some((_, b))) = (first, last) {
        format!("{prefix}{sink}-{task}-p{u}-{a:020}-{b:020}.{ext}")
    } else {
        let owned: Vec<SinkRecord> = recs.iter().map(|r| (*r).clone()).collect();
        format!("{prefix}{sink}-{task}-q{}.{ext}", batch_id(&owned))
    }
}

/// The manifest of `unit` ending at `last`.
#[must_use]
pub fn manifest_key(root: &str, sink: u64, task: u32, unit: u64, last: u64) -> String {
    format!("{root}_sprig/{sink}/{task}/u{unit}/{last:020}.json")
}

/// The byte ranges of a multipart upload's parts: `part` bytes each, the
/// last one shorter.
#[must_use]
pub fn part_ranges(len: usize, part: usize) -> Vec<std::ops::Range<usize>> {
    let part = part.max(1);
    (0..len.div_ceil(part))
        .map(|i| i * part..((i + 1) * part).min(len))
        .collect()
}

/// The runtime's batches follow the file limits: `s3.file_max_records`
/// (1,000,000), `s3.file_max_bytes` (128 MiB), `s3.file_max_age_seconds`
/// (300); read-ahead is at least two files.
///
/// # Errors
/// 39218 for options that aren't numbers.
pub fn run_settings_override(opts: &Opts, s: &mut RunSettings) -> Result<()> {
    let records = opts.u64("s3.file_max_records", 1_000_000)?;
    let bytes = opts.u64("s3.file_max_bytes", 128 << 20)?;
    let age = opts.u64("s3.file_max_age_seconds", 300)?;
    s.batch_max_records = usize::try_from(records).unwrap_or(usize::MAX).max(1);
    s.batch_max_bytes = bytes.max(1);
    s.batch_max_wait = Duration::from_secs(age.max(1));
    s.max_inflight_bytes = s.max_inflight_bytes.max(bytes.saturating_mul(2));
    // A large file takes longer than one webhook request.
    s.delivery_timeout = s.delivery_timeout.max(Duration::from_secs(600));
    Ok(())
}

/// The S3 target.
pub struct S3Target {
    client: Arc<S3Client>,
    prefix: Vec<Seg>,
    root: String,
    format: Format,
    compress: Compress,
    columns: Option<Vec<Column>>,
    part_bytes: usize,
    render: RenderCtx,
    exactly_once: bool,
    sink: u64,
    task: u32,
    epoch: Mutex<TaskEpoch>,
    /// Exactly once: each unit's last position in a manifest.
    done: Mutex<HashMap<u64, u64>>,
    /// The hourly orphan sweep (task 0 only), stopped with the target.
    sweep: Option<SweepGuard>,
}

/// Stops the orphan sweep when the target goes.
struct SweepGuard(sprig_common::CancellationToken);

impl Drop for SweepGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// How often the orphan sweep runs, and how old an upload must be.
const SWEEP_EVERY: Duration = Duration::from_secs(3600);

impl std::fmt::Debug for S3Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Target")
            .field("client", &self.client)
            .field("root", &self.root)
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

/// Builds the S3 target of `ctx`.
///
/// # Errors
/// As [`S3Target::new`].
pub fn build(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    Ok(Box::new(S3Target::new(ctx)?))
}

impl S3Target {
    /// The target of `ctx` (sink options `format`, `compression`,
    /// `columns`, `key_buckets`, `s3.part_bytes`).
    ///
    /// # Errors
    /// 39218 for bad options; 39231 for CSV or Parquet without `columns`
    /// from a stream or queue; 39217 for `EXACTLY ONCE` from a queue; the
    /// client's.
    pub fn new(ctx: &TargetCtx) -> Result<Self> {
        let TargetSpec::S3 { prefix } = &ctx.sink.target else {
            return Err(bad_option("not an S3 sink"));
        };
        let conn = ctx.conn()?;
        let client = S3Client::new(conn, &ctx.opts)?;
        let segs = parse_template(prefix)?;
        let format = Format::parse(&ctx.opts.text("format", "jsonl"))?;
        let compress = Compress::parse(&ctx.opts.text("compression", "gzip"))?;
        let columns = ctx.opts.get("columns").map(parse_columns).transpose()?;
        let source = ctx.sink.source.kind();
        if format != Format::Jsonl && columns.is_none() && source != SourceKind::Changes {
            return Err(err(
                SprigCode::SinkFormatUnsupported,
                "csv and parquet from a stream or queue need the columns option",
            ));
        }
        let exactly_once = ctx.sink.guarantee == Guarantee::ExactlyOnce;
        if exactly_once && source == SourceKind::Queue {
            return Err(err(
                SprigCode::SinkExactlyOnceUnsupported,
                "a queue has no position to keep with S3 objects",
            ));
        }
        let part = ctx.opts.u64("s3.part_bytes", 16 << 20)?;
        if part < 5 << 20 {
            return Err(bad_option(
                "s3.part_bytes must be at least 5 MiB (S3's smallest part)",
            ));
        }
        Ok(Self {
            client: Arc::new(client),
            root: template_root(&segs),
            prefix: segs,
            format,
            compress,
            columns,
            part_bytes: usize::try_from(part).unwrap_or(usize::MAX),
            render: RenderCtx {
                db: ctx.db_name.clone(),
                source: ctx.sink.source.name(),
                tenant: ctx.task.tenant,
                key_buckets: ctx.opts.u64("key_buckets", 16)?.max(1),
            },
            exactly_once,
            sink: ctx.sink.id.0,
            task: ctx.task.task_no,
            epoch: Mutex::new(ctx.task.epoch),
            done: Mutex::new(HashMap::new()),
            sweep: None,
        })
    }

    /// Starts the hourly orphan sweep in the background: every hour it aborts
    /// this sink's multipart uploads older than an hour. Only task 0 sweeps;
    /// the sweep covers every task's uploads.
    fn start_sweep(&mut self) {
        if self.task != 0 || self.sweep.is_some() {
            return;
        }
        let stop = sprig_common::CancellationToken::new();
        let me = Sweeper {
            client: Arc::clone(&self.client),
            root: self.root.clone(),
            sink: self.sink,
            base: self.base(),
        };
        let st = stop.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = st.cancelled() => return,
                    () = tokio::time::sleep(SWEEP_EVERY) => {}
                }
                match me.sweep(SWEEP_EVERY).await {
                    Ok(n) => {
                        metrics::counter!("sprig_sink_s3_orphans_aborted_total")
                            .increment(n as u64);
                        if n > 0 {
                            tracing::info!(sink = me.sink, aborted = n, "S3 orphan sweep");
                        }
                    }
                    Err(e) => tracing::warn!(sink = me.sink, error = %e, "S3 orphan sweep failed"),
                }
            }
        });
        self.sweep = Some(SweepGuard(stop));
    }

    /// The client (recovery tools and tests).
    #[must_use]
    pub fn client(&self) -> &Arc<S3Client> {
        &self.client
    }

    fn base(&self) -> String {
        format!("{}_sprig/{}/{}/", self.root, self.sink, self.task)
    }

    /// Writes one object: `PutObject`, or a multipart upload above
    /// `s3.part_bytes` (aborted when it fails).
    async fn upload(&self, name: &str, body: Vec<u8>) -> Result<String> {
        let ct = self.format.content_type(self.compress);
        if body.len() <= self.part_bytes {
            return Ok(
                match self
                    .client
                    .put_object(name, Bytes::from(body), ct, true)
                    .await?
                {
                    Put::Written(e) => e,
                    Put::Existed => String::new(),
                },
            );
        }
        // Conditional completion can't stop the parts from uploading, so
        // look first: an object of this name holds exactly these records.
        if self.client.head_object(name).await?.is_some() {
            return Ok(String::new());
        }
        let id = self.client.create_multipart(name, ct).await?;
        let body = Bytes::from(body);
        let r = async {
            let mut etags = Vec::new();
            for (n, r) in (1u32..).zip(part_ranges(body.len(), self.part_bytes)) {
                etags.push(self.client.upload_part(name, &id, n, body.slice(r)).await?);
            }
            self.client
                .complete_multipart(name, &id, &etags, true)
                .await
        }
        .await;
        match r {
            Ok(Put::Written(e)) => Ok(e),
            Ok(Put::Existed) => Ok(String::new()),
            Err(e) => {
                if let Err(a) = self.client.abort_multipart(name, &id).await {
                    tracing::warn!(error = %a, %name, "multipart upload not aborted; the sweep will");
                }
                Err(e)
            }
        }
    }

    /// Aborts this sink's multipart uploads older than `older_than` (left
    /// by crashed tasks): the hourly sweep. Returns how many were aborted.
    /// Uploads are listed under the prefix root (AWS) and, because `MinIO`
    /// lists multipart uploads only for an exact object name, for each
    /// object the task's `pending.json` names.
    ///
    /// # Errors
    /// The client's.
    pub async fn sweep_orphans(&self, older_than: Duration) -> Result<usize> {
        Sweeper {
            client: Arc::clone(&self.client),
            root: self.root.clone(),
            sink: self.sink,
            base: self.base(),
        }
        .sweep(older_than)
        .await
    }
}

/// What the orphan sweep needs of a target.
struct Sweeper {
    client: Arc<S3Client>,
    root: String,
    sink: u64,
    base: String,
}

impl Sweeper {
    async fn sweep(&self, older_than: Duration) -> Result<usize> {
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(0);
        let limit = i64::try_from(older_than.as_millis()).unwrap_or(i64::MAX);
        let mine = format!("{}-", self.sink);
        let mut n = 0;
        let mut uploads = self.client.list_uploads(&self.root).await?;
        if let Some(b) = self
            .client
            .get_object(&format!("{}pending.json", self.base))
            .await?
        {
            let p: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
            for o in p["objects"].as_array().into_iter().flatten() {
                if let Some(k) = o.as_str() {
                    for u in self.client.list_uploads(k).await? {
                        if !uploads.contains(&u) {
                            uploads.push(u);
                        }
                    }
                }
            }
        }
        for u in uploads {
            let file = u.key.rsplit('/').next().unwrap_or(&u.key);
            if file.starts_with(&mine) && now - u.initiated_ms >= limit {
                self.client.abort_multipart(&u.key, &u.upload_id).await?;
                n += 1;
            }
        }
        Ok(n)
    }
}

impl S3Target {
    /// Finishes or undoes the batch a crashed task left in `pending.json`.
    async fn recover_pending(&self) -> Result<()> {
        let key = format!("{}pending.json", self.base());
        let Some(body) = self.client.get_object(&key).await? else {
            return Ok(());
        };
        let p: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let manifests = p["manifests"].as_array().cloned().unwrap_or_default();
        let objects: Vec<String> = p["objects"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let mut missing = Vec::new();
        for m in &manifests {
            let k = m["key"].as_str().unwrap_or_default();
            if self.client.head_object(k).await?.is_none() {
                missing.push(m.clone());
            }
        }
        if !missing.is_empty() {
            let mut present = Vec::new();
            for o in &objects {
                if self.client.head_object(o).await?.is_some() {
                    present.push(o.clone());
                }
            }
            if present.len() == objects.len() {
                for m in missing {
                    let k = m["key"].as_str().unwrap_or_default().to_owned();
                    let b = serde_json::to_vec(&m["body"]).unwrap_or_default();
                    self.client
                        .put_object(&k, Bytes::from(b), "application/json", true)
                        .await?;
                }
                tracing::info!(
                    sink = self.sink,
                    task = self.task,
                    "S3 manifests of a crashed batch written"
                );
            } else {
                for o in present {
                    self.client.delete_object(&o).await?;
                }
                tracing::info!(
                    sink = self.sink,
                    task = self.task,
                    "objects of an unfinished S3 batch removed"
                );
            }
        }
        self.client.delete_object(&key).await
    }
}

#[async_trait]
impl Target for S3Target {
    fn kind(&self) -> &'static str {
        "s3"
    }

    async fn open(&mut self, task: &TaskInfo) -> Result<Vec<DestPosition>> {
        *self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = task.epoch;
        self.start_sweep();
        if !self.exactly_once {
            return Ok(Vec::new());
        }
        self.recover_pending().await?;
        let base = self.base();
        let mut newest: HashMap<u64, (u64, String)> = HashMap::new();
        for l in self.client.list_objects(&base).await? {
            let Some(rest) = l.key.strip_prefix(&base) else {
                continue;
            };
            let Some((u, file)) = rest.split_once('/') else {
                continue;
            };
            let (Some(unit), Some(seq)) = (
                u.strip_prefix('u').and_then(|u| u.parse::<u64>().ok()),
                file.strip_suffix(".json")
                    .and_then(|s| s.parse::<u64>().ok()),
            ) else {
                continue;
            };
            let e = newest.entry(unit).or_insert((seq, l.key.clone()));
            if seq >= e.0 {
                *e = (seq, l.key.clone());
            }
        }
        let mut out = Vec::new();
        let mut done = HashMap::new();
        for (unit, (seq, key)) in newest {
            let Some(b) = self.client.get_object(&key).await? else {
                continue;
            };
            let m: Value = serde_json::from_slice(&b)
                .map_err(|e| crate::model::delivery_failed(format!("manifest {key}: {e}")))?;
            let pos: Position = serde_json::from_value(m["end"].clone())
                .map_err(|e| crate::model::delivery_failed(format!("manifest {key}: {e}")))?;
            done.insert(unit, seq);
            out.push(DestPosition {
                pos,
                epoch: TaskEpoch(m["epoch"].as_u64().unwrap_or(0)),
            });
        }
        *self
            .done
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = done;
        Ok(out)
    }

    #[allow(clippy::too_many_lines)] // the module docs' steps
    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome> {
        let done = self
            .done
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // Records to write, by index into the batch.
        let todo: Vec<usize> = (0..b.records.len())
            .filter(|i| {
                !self.exactly_once
                    || b.records[*i]
                        .pos
                        .unit_seq()
                        .is_none_or(|(u, s)| done.get(&u).is_none_or(|d| s > *d))
            })
            .collect();
        if todo.is_empty() {
            return Ok(BatchOutcome::All(Outcome::Ok));
        }
        // Group by (rendered prefix, unit), in first-seen order.
        let mut groups: Vec<(String, Option<u64>, Vec<usize>)> = Vec::new();
        for i in todo {
            let r = &b.records[i];
            let p = render(&self.prefix, r, &self.render);
            let u = r.pos.unit_seq().map(|(u, _)| u);
            match groups.iter_mut().find(|g| g.0 == p && g.1 == u) {
                Some(g) => g.2.push(i),
                None => groups.push((p, u, vec![i])),
            }
        }
        let ext = self.format.ext(self.compress);
        let mut failed: HashMap<usize, String> = HashMap::new();
        // (name, bytes, unit, written indexes)
        let mut objects: Vec<(String, Vec<u8>, Option<u64>, Vec<usize>)> = Vec::new();
        for (prefix, unit, idx) in groups {
            let recs: Vec<&SinkRecord> = idx.iter().map(|i| &b.records[*i]).collect();
            let inferred;
            let cols = match (&self.columns, self.format) {
                (Some(c), _) => Some(c.as_slice()),
                (None, Format::Jsonl) => None,
                (None, _) => {
                    inferred = infer_columns(&recs);
                    Some(inferred.as_slice())
                }
            };
            let enc = encode(self.format, self.compress, cols, &recs)?;
            let bad: Vec<usize> = enc.failed.iter().map(|(k, _)| idx[*k]).collect();
            for (k, why) in enc.failed {
                failed.insert(idx[k], why);
            }
            let written: Vec<usize> = idx.into_iter().filter(|i| !bad.contains(i)).collect();
            if written.is_empty() {
                continue;
            }
            let wrecs: Vec<&SinkRecord> = written.iter().map(|i| &b.records[*i]).collect();
            let name = object_name(&prefix, self.sink, self.task, &wrecs, ext);
            objects.push((name, enc.bytes, unit, written));
        }
        // Exactly once: the manifests each unit will get, kept first in
        // pending.json so a crash mid-way is finished or undone at open.
        let epoch = self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0;
        let mut manifests: Vec<(u64, u64, String, Value)> = Vec::new();
        let multipart = objects.iter().any(|o| o.1.len() > self.part_bytes);
        if self.exactly_once || multipart {
            let mut units: Vec<u64> = objects.iter().filter_map(|o| o.2).collect();
            units.sort_unstable();
            units.dedup();
            for u in units {
                let mine: Vec<&(String, Vec<u8>, Option<u64>, Vec<usize>)> =
                    objects.iter().filter(|o| o.2 == Some(u)).collect();
                let seqs: Vec<(u64, &Position)> = mine
                    .iter()
                    .flat_map(|o| o.3.iter())
                    .filter_map(|i| {
                        let p = &b.records[*i].pos;
                        p.unit_seq().map(|(_, s)| (s, p))
                    })
                    .collect();
                let Some(first) = seqs.iter().map(|s| s.0).min() else {
                    continue;
                };
                let Some((last, end)) = seqs.iter().max_by_key(|s| s.0).copied() else {
                    continue;
                };
                let body = json!({
                    "unit": u,
                    "first": first,
                    "last": last,
                    "end": end,
                    "epoch": epoch,
                    "objects": mine.iter().map(|o| {
                        let s: Vec<u64> = o.3.iter().filter_map(|i| b.records[*i].pos.unit_seq().map(|x| x.1)).collect();
                        json!({
                            "name": o.0,
                            "first": s.iter().min(),
                            "last": s.iter().max(),
                            "records": o.3.len(),
                            "bytes": o.1.len(),
                        })
                    }).collect::<Vec<_>>(),
                });
                let key = manifest_key(&self.root, self.sink, self.task, u, last);
                manifests.push((u, last, key, body));
            }
            let pending = json!({
                "objects": objects.iter().map(|o| o.0.clone()).collect::<Vec<_>>(),
                "manifests": manifests.iter().map(|m| json!({"key": m.2, "body": m.3})).collect::<Vec<_>>(),
            });
            self.client
                .put_object(
                    &format!("{}pending.json", self.base()),
                    Bytes::from(serde_json::to_vec(&pending).unwrap_or_default()),
                    "application/json",
                    false,
                )
                .await?;
        }
        let mut etags: HashMap<String, String> = HashMap::new();
        for (name, bytes, _, _) in &objects {
            let e = self.upload(name, bytes.clone()).await?;
            etags.insert(name.clone(), e);
        }
        for (u, last, key, mut body) in manifests {
            if let Some(objs) = body["objects"].as_array_mut() {
                for o in objs {
                    let n = o["name"].as_str().unwrap_or_default().to_owned();
                    o["etag"] = json!(etags.get(&n).cloned().unwrap_or_default());
                }
            }
            self.client
                .put_object(
                    &key,
                    Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                    "application/json",
                    true,
                )
                .await?;
            self.done
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(u, last);
        }
        metrics::counter!("sprig_sink_s3_objects_total").increment(objects.len() as u64);
        if failed.is_empty() {
            return Ok(BatchOutcome::All(Outcome::Ok));
        }
        Ok(BatchOutcome::Each(
            (0..b.records.len())
                .map(|i| match failed.remove(&i) {
                    Some(reason) => Outcome::Fail { reason },
                    None => Outcome::Ok,
                })
                .collect(),
        ))
    }

    fn exactly_once(&self) -> bool {
        self.exactly_once
    }
}
