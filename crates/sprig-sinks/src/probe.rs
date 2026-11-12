//! `TEST CONNECTION` and `TEST SINK` (chapter 51, "Testing connections and
//! sinks"; R9, r9tests): dry runs a gateway answers as statements, so
//! Studio reaches them through `Sql.Query` like any other.
//!
//! ```text
//! TEST [PLATFORM] CONNECTION c
//!   credentials   open the sealed credentials (the master key file)
//!   per type      Kafka: options, connect (TCP, TLS, ApiVersions, SASL), metadata
//!                 S3: options, HeadBucket
//!                 MySQL, MariaDB, PostgreSQL: options, log in and SELECT 1
//!                 webhook: options, OPTIONS on the URL with its authentication
//!   every address is resolved through the destination allowlist and the
//!   tenant's residency first, as a sink's would be; nothing is written
//! TEST SINK s [WITH SAMPLE n]
//!   the last n records of the source (stream: the newest of each
//!   partition; queue: browsed, not taken), through WHERE, columns and
//!   TRANSFORM, shown as they would be delivered; nothing is delivered and
//!   no position moves
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::filter::SinkPrepare;
use crate::job::{SinkEnv, open_conn, over_global, resolve_opts};
use crate::model::{
    ConnKind, ConnRef, Guarantee, SinkDef, SinkId, SinkRecord, SinkState, SourceSpec, StartAt,
    TargetSpec, TaskEpoch,
};
use crate::runtime::{Prepare, TaskInfo};
use crate::store::SinksStore;
use crate::target::{Conn, Opts, TargetCtx};

/// One step of a connection test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// `credentials`, `options`, `connect`, `metadata`, `head_bucket`,
    /// `query`, `table`, `request`.
    pub step: String,
    /// Whether it passed.
    pub ok: bool,
    /// What it found, or why it failed.
    pub detail: String,
    /// Milliseconds from the start of the test when it ended.
    pub elapsed_ms: u64,
}

/// One record of a sink test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DryRow {
    /// Where the record is in the source.
    pub position: String,
    /// `deliver`, `filtered` (by `WHERE`), `dropped` (by the transform)
    /// or `failed` (it would be dead-lettered).
    pub outcome: &'static str,
    /// The key as text (hex when it isn't UTF-8).
    pub key: Option<String>,
    /// The value as it would be delivered: the record's JSON form (a
    /// change record's envelope), or its text.
    pub value: Option<String>,
    /// Headers, as a JSON object of text values.
    pub headers: String,
    /// Why it failed.
    pub detail: Option<String>,
    /// What the target would send for it (R9, r9last): its own encoding
    /// ([`crate::target::preview`]); for `deliver` rows only.
    pub payload: Option<String>,
}

/// The catalog's pool, schema and a connection.
async fn catalog(env: &dyn SinkEnv, tenant: TenantId) -> Result<(mysql_async::Conn, String)> {
    let (pool, schema) = env.catalog(tenant).await?;
    let c = pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })?;
    Ok((c, schema))
}

/// A sink definition standing in for "any sink of database `db_id`", for
/// options and target contexts outside a sink.
#[must_use]
pub fn stand_in(db_id: u32, name: &str, target: TargetSpec) -> SinkDef {
    SinkDef {
        id: SinkId(0),
        db_id,
        name: name.to_owned(),
        source: SourceSpec::Stream {
            id: 0,
            name: String::new(),
            partitions: 1,
        },
        filter_sql: None,
        projection: None,
        transform: None,
        target,
        connections: Vec::new(),
        guarantee: Guarantee::AtLeastOnce,
        dead_letter: None,
        start_at: StartAt::Latest,
        owner_user_id: 0,
        state: SinkState::Running,
        state_reason: None,
        definition_ver: 0,
        options: BTreeMap::new(),
    }
}

/// A sink's options as its tasks resolve them: catalog rows (tenant,
/// database, the sink's own), the sink's `WITH`, then the live settings.
///
/// # Errors
/// The catalog's.
pub async fn options(env: &dyn SinkEnv, tenant: TenantId, def: &SinkDef) -> Result<Opts> {
    let (mut c, schema) = catalog(env, tenant).await?;
    let opts = resolve_opts(&mut c, &schema, def).await?;
    Ok(match env.settings() {
        Some((etcd, dc)) => over_global(opts, &etcd, &dc),
        None => opts,
    })
}

type Steps = Vec<(&'static str, std::result::Result<String, String>)>;

/// `TEST CONNECTION` (see the module docs): the connection `r` of
/// database `db_id`, opened as a sink task would open it.
///
/// # Errors
/// The catalog's (a failing destination is a failed step, not an error).
pub async fn test_connection(
    env: &dyn SinkEnv,
    tenant: TenantId,
    r: &ConnRef,
    db_id: u32,
    db_name: &str,
) -> Result<Vec<Step>> {
    test_connection_with(env, tenant, r, db_id, db_name, None).await
}

/// [`test_connection`], and with `write` (`TEST CONNECTION c WRITE [TO
/// 'target']`, R9 r9last) a marked test record written once the checks
/// pass ([`write_steps`]).
///
/// # Errors
/// As [`test_connection`].
pub async fn test_connection_with(
    env: &dyn SinkEnv,
    tenant: TenantId,
    r: &ConnRef,
    db_id: u32,
    db_name: &str,
    write: Option<Option<&str>>,
) -> Result<Vec<Step>> {
    let t0 = Instant::now();
    let ms = || u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut out = Vec::new();
    let push = |steps: Steps, out: &mut Vec<Step>| {
        let at = ms();
        for (step, res) in steps {
            out.push(Step {
                step: step.to_owned(),
                ok: res.is_ok(),
                detail: res.unwrap_or_else(|e| e),
                elapsed_ms: at,
            });
        }
    };
    let def = stand_in(db_id, "test_connection", TargetSpec::Webhook { path: None });
    let mut opts = options(env, tenant, &def).await?;
    opts.0.insert("__db_name".into(), db_name.to_owned());
    let (mut c, schema) = catalog(env, tenant).await?;
    let store = SinksStore::new(schema);
    let conn = match open_conn(env, tenant, &mut c, &store, r, &opts).await {
        Ok(conn) => {
            push(vec![("credentials", Ok("opened".to_owned()))], &mut out);
            conn
        }
        Err(e) => {
            push(vec![("credentials", Err(e.to_string()))], &mut out);
            return Ok(out);
        }
    };
    drop(c);
    let steps = probe(conn.clone(), &opts, db_id).await;
    let passed = steps.iter().all(|(_, r)| r.is_ok());
    push(steps, &mut out);
    if let Some(to) = write {
        let steps = if passed {
            write_steps(conn, &opts, db_id, to).await
        } else {
            vec![("write", Err("not tried: a check above failed".to_owned()))]
        };
        push(steps, &mut out);
    }
    Ok(out)
}

/// The id of a test record: `sprig-test-<ms>-<random>`.
fn test_id() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let mut b = [0u8; 4];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b);
    format!("sprig-test-{ms}-{:08x}", u32::from_be_bytes(b))
}

/// The marked test record `TEST CONNECTION ... WRITE` sends: header
/// `x-sprig-test: 1`, value `{"sprig_test": true, "id": ...}`.
fn test_record(id: &str) -> SinkRecord {
    SinkRecord {
        key: Some(id.as_bytes().to_vec()),
        value: Some(
            serde_json::json!({"sprig_test": true, "id": id})
                .to_string()
                .into_bytes(),
        ),
        headers: vec![("x-sprig-test".to_owned(), b"1".to_vec())],
        op: None,
        before: None,
        after: None,
        ts_ms: i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0),
        idem_key: id.to_owned(),
        pos: crate::model::Position::Queue {
            queue: 0,
            msg_id: id.to_owned(),
            delivery: String::new(),
        },
    }
}

/// `TEST CONNECTION c WRITE [TO 'target']` (R9, r9last): one marked test
/// record, written as a sink would write it and removed again where the
/// target allows:
///
/// - webhook: one request with the record (the connection's format), the
///   receiver keeps it;
/// - Kafka: one record into topic `target` (required), kept until the
///   topic's retention;
/// - S3: an object `<target, default _sprig_test/><id>.json`, then
///   deleted;
/// - MySQL, MariaDB, PostgreSQL: `INSERT INTO target SELECT * FROM target
///   WHERE 1 = 0` in a rolled-back transaction: the `INSERT` right is
///   checked, no row is written.
async fn write_steps(conn: Conn, opts: &Opts, db_id: u32, to: Option<&str>) -> Steps {
    let id = test_id();
    let rec = test_record(&id);
    let batch = |r: SinkRecord| crate::model::Batch {
        id: id.clone(),
        records: vec![r],
        attempt: 1,
        lane: 0,
        ends: Vec::new(),
    };
    let outcome = |o: Result<crate::model::BatchOutcome>| match o {
        Ok(crate::model::BatchOutcome::All(crate::model::Outcome::Ok)) => Ok(()),
        Ok(crate::model::BatchOutcome::Each(v))
            if v.iter().all(|o| matches!(o, crate::model::Outcome::Ok)) =>
        {
            Ok(())
        }
        Ok(other) => Err(format!("{other:?}")),
        Err(e) => Err(e.to_string()),
    };
    match conn.kind {
        ConnKind::Webhook => {
            let def = stand_in(db_id, "test_connection", TargetSpec::Webhook { path: None });
            let mut o = opts.clone();
            o.0.entry("signature".into())
                .or_insert_with(|| "none".into());
            let ctx = context(def, Some(conn), o);
            match crate::target::webhook::build(&ctx) {
                Err(e) => vec![("write", Err(e.to_string()))],
                Ok(t) => vec![
                    (
                        "write",
                        outcome(t.deliver(&batch(rec)).await).map(|()| {
                            format!("one request with test record {id} (header x-sprig-test: 1)")
                        }),
                    ),
                    (
                        "clean_up",
                        Ok("not possible: the receiver keeps the request".into()),
                    ),
                ],
            }
        }
        ConnKind::Kafka => {
            let Some(topic) = to.filter(|t| !t.is_empty()) else {
                return vec![(
                    "write",
                    Err("name the topic: TEST CONNECTION c WRITE TO 'topic'".into()),
                )];
            };
            let def = stand_in(
                db_id,
                "test_connection",
                TargetSpec::Kafka {
                    topic: topic.to_owned(),
                    key: None,
                },
            );
            let ctx = context(def, Some(conn), opts.clone());
            match crate::target::kafka::build(&ctx) {
                Err(e) => vec![("write", Err(e.to_string()))],
                Ok(t) => vec![
                    (
                        "write",
                        outcome(t.deliver(&batch(rec)).await).map(|()| {
                            format!("one record into {topic}, key {id}, header x-sprig-test: 1")
                        }),
                    ),
                    (
                        "clean_up",
                        Ok("not possible: the topic keeps it until its retention".into()),
                    ),
                ],
            }
        }
        ConnKind::S3 => {
            let client = match crate::target::s3::client::S3Client::new(&conn, opts) {
                Ok(c) => c,
                Err(e) => return vec![("write", Err(e.to_string()))],
            };
            let key = format!("{}{id}.json", to.unwrap_or("_sprig_test/"));
            let body = bytes::Bytes::from(rec.value.clone().unwrap_or_default());
            match client
                .put_object(&key, body, "application/json", true)
                .await
            {
                Err(e) => vec![("write", Err(e.to_string()))],
                Ok(_) => vec![
                    ("write", Ok(format!("PutObject {key}"))),
                    (
                        "clean_up",
                        client
                            .delete_object(&key)
                            .await
                            .map(|()| format!("DeleteObject {key}"))
                            .map_err(|e| format!("{key} stays: {e}")),
                    ),
                ],
            }
        }
        ConnKind::Mysql | ConnKind::Postgresql => {
            let Some(table) = to
                .filter(|t| !t.is_empty())
                .or_else(|| opts.get("test_table"))
            else {
                return vec![(
                    "write",
                    Err("name the table: TEST CONNECTION c WRITE TO 'table'".into()),
                )];
            };
            let def = stand_in(
                db_id,
                "test_connection",
                TargetSpec::Database {
                    table: table.to_owned(),
                },
            );
            let ctx = context(def, Some(conn), opts.clone());
            crate::target::db::write_probe(&ctx, table).await
        }
    }
}

/// The type's checks on an opened connection.
async fn probe(conn: Conn, opts: &Opts, db_id: u32) -> Steps {
    match conn.kind {
        ConnKind::Kafka => crate::target::kafka::probe(&conn).await,
        ConnKind::S3 => {
            let client = match crate::target::s3::client::S3Client::new(&conn, opts) {
                Ok(c) => c,
                Err(e) => return vec![("options", Err(e.to_string()))],
            };
            let bucket = client.bucket().to_owned();
            vec![
                ("options", Ok(format!("bucket {bucket}"))),
                (
                    "head_bucket",
                    match client.head_bucket().await {
                        Ok(s) if (200..300).contains(&s) => Ok(format!("HTTP {s}")),
                        Ok(403) => {
                            Err("HTTP 403: the credentials were refused for this bucket".into())
                        }
                        Ok(404) => Err(format!("HTTP 404: no bucket {bucket}")),
                        Ok(s) => Err(format!("HTTP {s}")),
                        Err(e) => Err(e.to_string()),
                    },
                ),
            ]
        }
        ConnKind::Mysql | ConnKind::Postgresql => {
            let def = stand_in(
                db_id,
                "test_connection",
                TargetSpec::Database {
                    table: String::new(),
                },
            );
            let ctx = context(def, Some(conn), opts.clone());
            let table = opts.get("test_table").map(str::to_owned);
            crate::target::db::probe(&ctx, table.as_deref()).await
        }
        ConnKind::Webhook => {
            // An OPTIONS request carries no body to sign: a connection
            // without a signing secret (for `signature = none` sinks)
            // tests as well.
            let mut o = opts.clone();
            o.0.insert("signature".into(), "none".into());
            match crate::target::webhook::WebhookTarget::for_events(conn, o, "test_connection") {
                Err(e) => vec![("options", Err(e.to_string()))],
                Ok(w) => vec![
                    ("options", Ok("webhook".into())),
                    (
                        "request",
                        match w.probe().await {
                            Ok(s @ (401 | 403)) => {
                                Err(format!("HTTP {s}: the credentials were refused"))
                            }
                            Ok(s) => Ok(format!("OPTIONS answered HTTP {s}")),
                            Err(e) => Err(e.to_string()),
                        },
                    ),
                ],
            }
        }
    }
}

/// A target context for a stand-in sink.
fn context(def: SinkDef, conn: Option<Conn>, opts: Opts) -> TargetCtx {
    TargetCtx {
        task: TaskInfo {
            tenant: 0,
            sink: 0,
            task_no: 0,
            epoch: TaskEpoch(0),
        },
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
        sink: Arc::new(def),
        conn,
        extra: Vec::new(),
        opts,
    }
}

/// Text of bytes: UTF-8, else `0x` and hex.
fn text(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            let mut s = String::with_capacity(2 + b.len() * 2);
            s.push_str("0x");
            for x in b {
                s.push_str(&format!("{x:02x}"));
            }
            s
        }
    }
}

fn position(r: &SinkRecord) -> String {
    use crate::model::Position;
    match &r.pos {
        Position::Stream {
            partition, offset, ..
        } => format!("partition {partition} offset {offset}"),
        Position::Queue { msg_id, .. } => format!("message {msg_id}"),
        other @ Position::Change { .. } => format!("{other:?}"),
    }
}

/// How a record would be delivered (see [`DryRow::value`]).
fn shown(r: &SinkRecord, outcome: &'static str, detail: Option<String>) -> DryRow {
    let headers: serde_json::Map<String, Value> = r
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(text(v))))
        .collect();
    let value = if r.op.is_some() {
        Some(crate::target::kafka::json_value(r).to_string())
    } else {
        r.value.as_deref().map(text)
    };
    DryRow {
        position: position(r),
        outcome,
        key: r.key.as_deref().map(text),
        value,
        headers: Value::Object(headers).to_string(),
        detail,
        payload: None,
    }
}

/// The target context a sink's payload preview is built in (R9,
/// r9last): its first connection opened as a task opens it (sealed
/// credentials, allowlist, residency); nothing is sent.
async fn preview_ctx(
    env: &dyn SinkEnv,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
) -> std::result::Result<TargetCtx, String> {
    let conn = match def.connections.first() {
        Some(r) => {
            let (mut c, schema) = catalog(env, tenant).await.map_err(|e| e.to_string())?;
            let store = SinksStore::new(schema);
            Some(
                open_conn(env, tenant, &mut c, &store, r, opts)
                    .await
                    .map_err(|e| format!("the connection: {e}"))?,
            )
        }
        None => None,
    };
    Ok(context(def.clone(), conn, opts.clone()))
}

/// `TEST SINK`'s dry run over `sample` (see the module docs): `WHERE`,
/// columns and `TRANSFORM` as the sink's tasks run them, nothing
/// delivered.
///
/// # Errors
/// The catalog's; the transform's loading (39219).
pub async fn dry_run(
    env: &dyn SinkEnv,
    tenant: TenantId,
    def: &SinkDef,
    sample: Vec<SinkRecord>,
) -> Result<Vec<DryRow>> {
    let opts = options(env, tenant, def).await?;
    let (pool, schema) = env.catalog(tenant).await?;
    let transform = crate::job::load_transform(&pool, &schema, def, &opts).await?;
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform,
    };
    let ctx = preview_ctx(env, tenant, def, &opts).await;
    let payload = |x: &SinkRecord| {
        Some(
            match &ctx {
                Ok(c) => crate::target::preview(c, x),
                Err(e) => Err(e.clone()),
            }
            .unwrap_or_else(|e| format!("not shown: {e}")),
        )
    };
    let mut out = Vec::new();
    for r in sample {
        let shaped = match prepare.prepare(r.clone()) {
            Ok(v) => v,
            Err(e) => {
                out.push(shown(&r, "failed", Some(e)));
                continue;
            }
        };
        if shaped.is_empty() {
            out.push(shown(&r, "filtered", None));
            continue;
        }
        match prepare.transform(shaped).await {
            Ok(v) if v.is_empty() => out.push(shown(&r, "dropped", None)),
            Ok(v) => out.extend(v.iter().map(|x| DryRow {
                payload: payload(x),
                ..shown(x, "deliver", None)
            })),
            Err(e) => out.push(shown(&r, "failed", Some(e))),
        }
    }
    Ok(out)
}

/// The newest `n` records of stream `stream` (at most `n` in all, the
/// newest of each partition), read from its log shard as a sink task
/// reads it; positions untouched.
///
/// # Errors
/// The catalog's or the shard's; 39221 when the stream is gone.
pub async fn sample_stream(
    env: &dyn SinkEnv,
    tenant: TenantId,
    stream: u64,
    partitions: u32,
    n: usize,
) -> Result<Vec<SinkRecord>> {
    use sprig_streams::read::{LogReader, ReadLimits};
    use sprig_streams::{Offset, PartitionNo, StreamId};
    let (pool, schema) = env.catalog(tenant).await?;
    let (log_schema, _) = crate::job::stream_log_schema(&pool, &schema, tenant, stream).await?;
    let shard = env.shard_pool(tenant).await?;
    let mut c = shard.get_conn().await.map_err(|e| {
        let mut x = SprigError::new(
            SprigCode::CatalogUnavailable,
            "the stream's shard is unavailable",
        )
        .with_source(e);
        x.retryable = true;
        x
    })?;
    let log = LogReader { schema: log_schema };
    let parts = partitions.max(1);
    let per = (n / parts as usize).max(1);
    let mut out = Vec::new();
    for p in 0..parts {
        let b = log.bounds(&mut c, StreamId(stream), PartitionNo(p)).await?;
        let hw = b.high_watermark.0;
        let from = hw.saturating_sub(per as u64).max(b.log_start.0);
        if from >= hw {
            continue;
        }
        let batch = log
            .read(
                &mut c,
                StreamId(stream),
                PartitionNo(p),
                Offset(from),
                ReadLimits {
                    max_records: u32::try_from(per).unwrap_or(u32::MAX),
                    max_bytes: 16 << 20,
                },
            )
            .await?;
        for rec in batch.records {
            out.push(crate::source::stream_record(stream, p, rec));
        }
    }
    out.sort_by_key(|r| std::cmp::Reverse(r.ts_ms));
    out.truncate(n);
    out.reverse();
    Ok(out)
}
