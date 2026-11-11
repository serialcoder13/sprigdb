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
}

/// The catalog's pool, schema and a connection.
async fn catalog(
    env: &dyn SinkEnv,
    tenant: TenantId,
) -> Result<(mysql_async::Conn, String)> {
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
    let steps = probe(conn, &opts, db_id).await;
    push(steps, &mut out);
    Ok(out)
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
                        Ok(403) => Err("HTTP 403: the credentials were refused for this bucket".into()),
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
            match crate::target::webhook::WebhookTarget::for_events(
                conn,
                opts.clone(),
                "test_connection",
            ) {
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
        other => format!("{other:?}"),
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
    }
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
            Ok(v) => out.extend(v.iter().map(|x| shown(x, "deliver", None))),
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
        let mut x = SprigError::new(SprigCode::CatalogUnavailable, "the stream's shard is unavailable")
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
