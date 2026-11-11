//! `queue_forwarder` (chapter 50, "Publishing inside a database
//! transaction", steps 7–8; chapter 42, "Inside a database transaction";
//! R9, r9fwd): one long-running job per tenant (`spec: {"tenant": <id>}`)
//! that forwards the tenant's transactional publishes from the change
//! feed, in commit order across transactions.
//!
//! ```text
//! serve(tenant):
//!   start after the subscription's checkpoint (feed/checkpoints/<tenant>/
//!     queue_forwarder), or at the tenant's position now
//!   tail the tenant's feed (chapter 38) keeping __sprig_qo and __sprig_xo
//!   per committed transaction, in commit order: its outbox inserts
//!     (__sprig_qo rows of kind 'publish'; every __sprig_xo row)
//!   take what the feed has (at most 1,000 rows), then per run of rows of
//!     one outbox table in one shard schema, in order: a gateway forwards
//!     the run (LogicPeer.SinkSql, op outbox_forward) and deletes it; the
//!     next run is sent only after this one's destinations confirmed
//!   acknowledge the position after the last transaction forwarded (at
//!     most once a second, and when stopping)
//! ```
//!
//! **Exactly once.** A run's rows are deleted only after every destination
//! confirmed, and each enqueue writes the forward ledger `__sprig_qf` in
//! the queue owner's batch: a run forwarded again after a crash (the
//! checkpoint is behind) finds its rows gone, or finds them in the ledger.
//! Cross-tenant rows land in their destination's outbox under their out
//! id, which the destination dedups.
//!
//! **The fallback.** The gateways' polling forwarder takes transactional
//! publishes older than `queue.forward_fallback_ms` (2 s): if this job is
//! down or behind, rows still go, in insert order. A run that keeps
//! failing is given up after [`RUN_GIVE_UP`] and left to the fallback, so
//! one unreachable destination doesn't hold the tenant's other queues.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_common::Result;
use sprig_common::ids::TenantId;
use sprig_controller::catalogs::ShardMapLocator;
use sprig_controller::dispatch::JobClass;
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::subs::Subscriptions;
use sprig_feed::{
    ChangeEvent, Clusters, FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions,
    TenantTail,
};
use sprig_meta::MetaStore;
use sprig_sinks::peer::{SinkPeer, SqlOp, SqlRequest, SqlResult};
use tokio::sync::mpsc;

use crate::runtime::{JobCtx, JobHandler};

pub use sprig_controller::queue_forwarder_loop::{JOB_KIND, SUB_NAME};

/// The queue outbox.
pub const QO: &str = "__sprig_qo";
/// The cross-tenant outbox.
pub const XO: &str = "__sprig_xo";

/// Rows taken from the feed per round at most.
pub const MAX_ROUND_ROWS: usize = 1_000;

/// A run that keeps failing this long is left to the polling fallback.
pub const RUN_GIVE_UP: Duration = Duration::from_secs(10);

/// What the job needs.
#[derive(Clone)]
pub struct ForwarderEnv {
    /// This DC's tier (subscriptions, placement).
    pub dc: Arc<dyn MetaStore>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// Binlog sources.
    pub clusters: Arc<dyn Clusters>,
    /// The gateways (`LogicPeer`).
    pub peer: Arc<dyn SinkPeer>,
}

impl std::fmt::Debug for ForwarderEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwarderEnv").finish_non_exhaustive()
    }
}

/// `queue_forwarder` (see the module docs).
#[derive(Debug, Clone)]
pub struct QueueForwarder(pub ForwarderEnv);

#[async_trait]
impl JobHandler for QueueForwarder {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(
            cx.spec["tenant"]
                .as_u64()
                .ok_or_else(|| "spec.tenant is missing".to_owned())?,
        );
        Forwarder::new(self.0.clone(), tenant).serve(cx).await
    }
}

/// One outbox row the feed showed committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Its shard schema.
    pub schema: String,
    /// [`QO`] or [`XO`].
    pub table: &'static str,
    /// Its out id.
    pub out_id: [u8; 16],
}

/// The outbox row an event inserted, if it is one the forwarder takes:
/// `__sprig_qo` rows of kind `publish` (dead letters and due delayed
/// messages are the owners' and the polling forwarder's), every
/// `__sprig_xo` row.
#[must_use]
pub fn committed(tenant: TenantId, ev: &ChangeEvent) -> Option<Committed> {
    if ev.op != Op::Insert {
        return None;
    }
    let table = match ev.table.as_str() {
        QO => QO,
        XO => XO,
        _ => return None,
    };
    let img = ev.after.as_ref()?;
    let out_id = match img.get("out_id").or_else(|| ev.key.get("out_id"))? {
        sprig_binlog::Value::Bytes(b) | sprig_binlog::Value::String(b) => {
            <[u8; 16]>::try_from(b.as_slice()).ok()?
        }
        _ => return None,
    };
    if table == QO {
        // ENUM('publish', ...): 'publish' is member 1. A row whose kind
        // can't be read is forwarded (a gone row costs a lookup only).
        match img.get("kind") {
            Some(sprig_binlog::Value::Enum(1) | sprig_binlog::Value::Null) | None => {}
            Some(sprig_binlog::Value::String(s)) if s.as_slice() == b"publish" => {}
            Some(_) => return None,
        }
    }
    Some(Committed {
        schema: sprig_shard::shard_schema(tenant, ev.db, ev.shard.group, ev.shard.shard),
        table,
        out_id,
    })
}

/// Consecutive rows of one outbox table in one schema, in commit order:
/// each run is one request, sent after the one before it confirmed.
#[must_use]
pub fn runs(rows: &[Committed]) -> Vec<(String, &'static str, Vec<[u8; 16]>)> {
    let mut out: Vec<(String, &'static str, Vec<[u8; 16]>)> = Vec::new();
    for r in rows {
        match out.last_mut() {
            Some((s, t, ids)) if *s == r.schema && *t == r.table => ids.push(r.out_id),
            _ => out.push((r.schema.clone(), r.table, vec![r.out_id])),
        }
    }
    out
}

fn hex(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    id.iter().fold(String::with_capacity(32), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The tenant's catalogs for the feed's name resolution.
struct Catalogs(Arc<ShardMapLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<mysql_async::Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

/// What the feed task sends: an item, or the error that ended the tail.
type Item = std::result::Result<FeedItem, String>;

#[derive(Debug, Default)]
struct Stats {
    forwarded: u64,
    gone: u64,
    left: u64,
    given_up: u64,
    runs: u64,
}

struct Forwarder {
    env: ForwarderEnv,
    tenant: TenantId,
    subs: Subscriptions,
    stats: Stats,
}

impl Forwarder {
    fn new(env: ForwarderEnv, tenant: TenantId) -> Self {
        let subs = Subscriptions::new(Arc::clone(&env.dc));
        Self {
            env,
            tenant,
            subs,
            stats: Stats::default(),
        }
    }

    /// Starts the tail in its own task (a tail's `next` isn't cancel-safe,
    /// so the job never drops one half-read).
    async fn open(
        &self,
        start: StartAt,
        stop: sprig_common::CancellationToken,
    ) -> Result<mpsc::Receiver<Item>> {
        let mut tail = TenantTail::start(
            self.tenant,
            start,
            Arc::clone(&self.env.clusters),
            Arc::new(MetaPlacement::new(Arc::clone(&self.env.dc))),
            Arc::new(CatalogResolver::new(Arc::new(Catalogs(Arc::clone(
                &self.env.locator,
            ))))),
            TailOptions {
                keep_internal: vec![QO.to_owned(), XO.to_owned()],
                ..TailOptions::default()
            },
        )
        .await?;
        let (tx, rx) = mpsc::channel::<Item>(4096);
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    () = stop.cancelled() => return,
                    i = tail.next() => i,
                };
                let end = item.is_err();
                if tx.send(item.map_err(|e| e.to_string())).await.is_err() || end {
                    return;
                }
            }
        });
        Ok(rx)
    }

    async fn start_at(&self) -> StartAt {
        match self.subs.checkpoint(self.tenant, SUB_NAME).await {
            Ok(Some(cp)) => match cp.resume_token() {
                Ok(t) => StartAt::After(t),
                Err(e) => {
                    tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder checkpoint unreadable; starting now");
                    StartAt::Now
                }
            },
            Ok(None) => StartAt::Now,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder checkpoint not read; starting now");
                StartAt::Now
            }
        }
    }

    #[allow(clippy::too_many_lines)] // the loop of the module docs
    async fn serve(&mut self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let mut acked: Option<ResumeToken> = None;
        let mut last_ack = Instant::now();
        let mut last_progress = Instant::now();
        let mut pending_events = 0u64;
        let mut retry = Duration::from_millis(200);
        'outer: loop {
            if cx.cancel.is_cancelled() {
                break;
            }
            // Gone tenants: the subscription is deleted with them.
            if matches!(self.subs.get(self.tenant, SUB_NAME).await, Err(e) if e.code() == Some(sprig_common::SprigCode::FeedSubscriptionNotFound))
            {
                tracing::info!(tenant = %self.tenant, "queue forwarder subscription gone; stopping");
                return Ok(json!({"end": "gone"}));
            }
            let start = match &acked {
                Some(t) => StartAt::After(t.clone()),
                None => self.start_at().await,
            };
            let stop = cx.cancel.child_token();
            let mut rx = match self.open(start, stop.clone()).await {
                Ok(rx) => rx,
                Err(e) => {
                    tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder: feed not opened; retrying");
                    tokio::select! {
                        () = cx.cancel.cancelled() => break,
                        () = tokio::time::sleep(retry) => {}
                    }
                    retry = (retry * 2).min(Duration::from_secs(5));
                    continue;
                }
            };
            retry = Duration::from_millis(200);
            tracing::info!(tenant = %self.tenant, "queue forwarder reading the feed");
            // Rows of the transaction being read, and of whole ones.
            let mut open_txn: Vec<Committed> = Vec::new();
            loop {
                let first = tokio::select! {
                    () = cx.cancel.cancelled() => { stop.cancel(); break 'outer; }
                    i = rx.recv() => i,
                };
                let Some(first) = first else {
                    stop.cancel();
                    continue 'outer;
                };
                let mut batch: Vec<Committed> = Vec::new();
                let mut token: Option<ResumeToken> = None;
                let mut next = Some(first);
                let mut ended = false;
                while let Some(item) = next.take() {
                    match item {
                        Ok(FeedItem::Event(ev)) => {
                            pending_events += 1;
                            if let Some(c) = committed(self.tenant, &ev) {
                                open_txn.push(c);
                            }
                        }
                        Ok(FeedItem::TxnEnd { token: t }) => {
                            batch.append(&mut open_txn);
                            token = Some(t);
                        }
                        Ok(FeedItem::Progress { token: t }) => {
                            if open_txn.is_empty() {
                                token = Some(t);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder: feed broke; reopening");
                            ended = true;
                            break;
                        }
                    }
                    if batch.len() >= MAX_ROUND_ROWS {
                        break;
                    }
                    next = rx.try_recv().ok();
                }
                if !batch.is_empty() && !self.forward(&batch, &cx.cancel).await {
                    stop.cancel();
                    break 'outer;
                }
                if let Some(t) = token {
                    acked = Some(t);
                }
                if last_ack.elapsed() >= Duration::from_secs(1)
                    && let Some(t) = &acked
                {
                    match self
                        .subs
                        .ack(self.tenant, SUB_NAME, t, pending_events, now_ms())
                        .await
                    {
                        Ok(()) => pending_events = 0,
                        Err(e) => {
                            tracing::debug!(tenant = %self.tenant, error = %e, "queue forwarder checkpoint not written");
                        }
                    }
                    last_ack = Instant::now();
                }
                if last_progress.elapsed() >= Duration::from_secs(2) {
                    let held = cx.progress(self.progress()).await.unwrap_or(true);
                    last_progress = Instant::now();
                    if !held {
                        stop.cancel();
                        break 'outer;
                    }
                }
                if ended {
                    stop.cancel();
                    open_txn.clear();
                    tokio::select! {
                        () = cx.cancel.cancelled() => break 'outer,
                        () = tokio::time::sleep(Duration::from_millis(200)) => {}
                    }
                    continue 'outer;
                }
            }
        }
        if let Some(t) = &acked {
            let _ = self
                .subs
                .ack(self.tenant, SUB_NAME, t, pending_events, now_ms())
                .await;
        }
        Err("cancelled".to_owned())
    }

    fn progress(&self) -> Value {
        json!({
            "tenant": self.tenant.0,
            "forwarded": self.stats.forwarded,
            "gone": self.stats.gone,
            "left": self.stats.left,
            "given_up": self.stats.given_up,
            "runs": self.stats.runs,
        })
    }

    /// Forwards `rows` run by run; `false` when cancelled.
    async fn forward(
        &mut self,
        rows: &[Committed],
        cancel: &sprig_common::CancellationToken,
    ) -> bool {
        for (schema, table, ids) in runs(rows) {
            let req = SqlRequest {
                tenant: self.tenant.0,
                tenant_name: String::new(),
                user_id: 0,
                database: String::new(),
                op: SqlOp::OutboxForward {
                    schema: schema.clone(),
                    table: table.to_owned(),
                    ids: ids.iter().map(hex).collect(),
                },
                max_rows: 0,
                timeout_ms: 0,
            };
            let started = Instant::now();
            let mut wait = Duration::from_millis(50);
            loop {
                if cancel.is_cancelled() {
                    return false;
                }
                match self
                    .env
                    .peer
                    .sql(self.tenant.0, &req)
                    .await
                    .and_then(sprig_sinks::peer::SqlReply::ok)
                {
                    Ok(reply) => {
                        let (f, g, l) = counts(reply.results.first());
                        self.stats.forwarded += f;
                        self.stats.gone += g;
                        self.stats.left += l;
                        self.stats.runs += 1;
                        metrics::counter!("sprig_queue_forward_feed_rows_total", "result" => "forwarded")
                            .increment(f);
                        metrics::counter!("sprig_queue_forward_feed_rows_total", "result" => "gone")
                            .increment(g);
                        break;
                    }
                    Err(e) if started.elapsed() >= RUN_GIVE_UP => {
                        self.stats.given_up += ids.len() as u64;
                        metrics::counter!("sprig_queue_forward_feed_rows_total", "result" => "given_up")
                            .increment(ids.len() as u64);
                        tracing::warn!(tenant = %self.tenant, %schema, table, rows = ids.len(), error = %e, "queue forwarder: run given up; the polling fallback forwards it");
                        break;
                    }
                    Err(e) => {
                        tracing::debug!(tenant = %self.tenant, %schema, error = %e, "queue forwarder: run failed; retrying");
                        tokio::select! {
                            () = cancel.cancelled() => return false,
                            () = tokio::time::sleep(wait) => {}
                        }
                        wait = (wait * 2).min(Duration::from_secs(1));
                    }
                }
            }
        }
        true
    }
}

/// (forwarded, gone, left) from an `outbox_forward` result.
fn counts(r: Option<&SqlResult>) -> (u64, u64, u64) {
    let Some(r) = r else { return (0, 0, 0) };
    let Some(row) = r.rows.first() else {
        return (0, 0, 0);
    };
    let n = |i| {
        SqlResult::text(row, i)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (n(0), n(1), n(2))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_binlog::Value as V;
    use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId};
    use sprig_feed::{EventId, Image, ShardRef};

    use super::*;

    fn ev(table: &str, op: Op, id: u8, kind: Option<V>) -> ChangeEvent {
        let mut after = vec![("out_id".to_owned(), V::Bytes(vec![id; 16]))];
        if let Some(k) = kind {
            after.push(("kind".to_owned(), k));
        }
        let cluster = ClusterId::new("dc-a-c1").unwrap();
        ChangeEvent {
            id: EventId {
                cluster: cluster.clone(),
                gtid: sprig_binlog::Gtid {
                    domain: 0,
                    server: 1,
                    seq: 1,
                },
                row: 0,
            },
            token: ResumeToken {
                tenant: TenantId(7),
                clamp: 0,
                cluster,
                segment: 0,
                pos: sprig_binlog::GtidPos::new(),
                until: None,
                progress: false,
            },
            tenant: TenantId(7),
            db: DbId(3),
            db_name: "shop".into(),
            object: 0,
            table: table.into(),
            shard: ShardRef {
                group: GroupId(1),
                shard: ShardId(2),
            },
            key: Image(vec![("out_id".to_owned(), V::Bytes(vec![id; 16]))]),
            op,
            before: None,
            after: Some(Image(after)),
            delta: None,
            hlc: 0,
            txn: sprig_binlog::Gtid {
                domain: 0,
                server: 1,
                seq: 1,
            },
            ddl: None,
            hint: None,
        }
    }

    #[test]
    fn takes_publishes_and_cross_tenant_rows_only() {
        let t = TenantId(7);
        let c = committed(t, &ev(QO, Op::Insert, 1, Some(V::Enum(1)))).unwrap();
        assert_eq!(c.table, QO);
        assert_eq!(c.out_id, [1; 16]);
        assert_eq!(
            c.schema,
            sprig_shard::shard_schema(t, DbId(3), GroupId(1), ShardId(2))
        );
        // Dead letters (2) and due delayed messages (3) are the owners'.
        assert!(committed(t, &ev(QO, Op::Insert, 1, Some(V::Enum(2)))).is_none());
        assert!(committed(t, &ev(QO, Op::Insert, 1, Some(V::Enum(3)))).is_none());
        // The forwarder's own delete, and other tables.
        assert!(committed(t, &ev(QO, Op::Delete, 1, Some(V::Enum(1)))).is_none());
        assert!(committed(t, &ev("orders", Op::Insert, 1, None)).is_none());
        let x = committed(t, &ev(XO, Op::Insert, 2, Some(V::Enum(2)))).unwrap();
        assert_eq!((x.table, x.out_id), (XO, [2; 16]));
    }

    #[test]
    fn runs_keep_commit_order() {
        let r = |s: &str, t: &'static str, i: u8| Committed {
            schema: s.into(),
            table: t,
            out_id: [i; 16],
        };
        let rows = vec![
            r("a", QO, 1),
            r("a", QO, 2),
            r("a", XO, 3),
            r("b", QO, 4),
            r("a", QO, 5),
        ];
        let got = runs(&rows);
        let shape: Vec<(String, &str, Vec<u8>)> = got
            .into_iter()
            .map(|(s, t, ids)| (s, t, ids.iter().map(|i| i[0]).collect()))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("a".to_owned(), QO, vec![1, 2]),
                ("a".to_owned(), XO, vec![3]),
                ("b".to_owned(), QO, vec![4]),
                ("a".to_owned(), QO, vec![5]),
            ]
        );
        assert_eq!(hex(&[0xab; 16]).len(), 32);
        let res = SqlResult {
            columns: vec!["forwarded".into(), "gone".into(), "left".into()],
            rows: vec![vec![
                SqlResult::cell(Some(b"5")),
                SqlResult::cell(Some(b"2")),
                SqlResult::cell(Some(b"0")),
            ]],
            ..SqlResult::default()
        };
        assert_eq!(counts(Some(&res)), (5, 2, 0));
    }
}
