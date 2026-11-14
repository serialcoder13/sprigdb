//! Change exchanges (a table's change feed published to an exchange):
//! the `change_exchange` job, one per table
//! with `queue.change_exchange` set, submitted by the controller's
//! `change_exchange` loop for the gateway's feed subscription `cx:o<object>`.
//!
//! ```text
//! the subscription's owner_ref: {db, table, object, exchange, user}
//! tail the tenant's feed from the subscription's checkpoint
//! each change of the table (insert, update, delete; DDL and truncate too):
//!   the user who set the exchange (`user`) may see it?
//!     its policies don't bind the user: SELECT on the table decides
//!     they bind: each image checked on a gateway in a session of the user
//!       (SinkSql Images): an insert goes out when its new row shows, a
//!       delete when its old row does; an update as an update (both), a
//!       delete (only the old row) or an insert (only the new row)
//!   body: the change envelope (as the change topic's record), its
//!     op and images as the user sees them; routing key <db>.<table>.<op>
//!     (c, u, d, ddl, t)
//!   published as the user on a gateway (SinkSql Publish: CALL
//!     sprig.mq_publish(exchange, key, body, properties, 0, db)), so the
//!     user's rights on the exchange apply
//! a failed publish is retried with backoff (the change isn't skipped);
//! the checkpoint moves after what was published, at most every second:
//! at least once, in commit order
//! the tail is read by a task of its own (sprig_feed::pump), so the loop's
//!   1 s housekeeping wait never cancels a read half-way
//! the job ends when the subscription is dropped (checked every 5 s)
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::dispatch::JobClass;
use sprig_feed::names::CatalogResolver;
use sprig_feed::pump::Pump;
use sprig_feed::subs::Subscriptions;
use sprig_feed::{
    ChangeEvent, Clusters, FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions,
    TenantTail,
};
use sprig_meta::MetaStore;
use sprig_sinks::peer::{ImageTable, RunAs, SinkPeer, SqlOp, images_answer};

use crate::logic::LogicEnv;
use crate::runtime::{JobCtx, JobHandler};

/// The job kind (the controller's `change_exchange_loop::JOB_KIND`).
pub const JOB_KIND: &str = "change_exchange";

/// `[before, after]` of feed event `e` visible to user `user` of `tenant`:
/// each image checked on a gateway in a session of the user (`SinkSql`
/// `Images`). An absent image is `false`; for DDL
/// and truncate, `(SELECT on the table, false)`.
///
/// # Errors
/// The gateway's, or the directory's (retry).
pub(crate) async fn visible_to(
    peer: &dyn SinkPeer,
    global: &dyn MetaStore,
    tenant: TenantId,
    user: u32,
    seed: u64,
    e: &ChangeEvent,
) -> Result<(bool, bool)> {
    let img = |i: Option<&sprig_feed::Image>| match i.map(sprig_streams::envelope::image_json) {
        Some(Value::Object(m)) => Some(m),
        _ => None,
    };
    let mut images = Vec::new();
    let (bi, ai) = if matches!(e.op, Op::Ddl(_) | Op::Truncate) {
        images.push(None);
        (Some(0), None)
    } else {
        let mut push = |m: Option<serde_json::Map<String, Value>>| {
            m.map(|m| {
                images.push(Some(m));
                images.len() - 1
            })
        };
        let b = push(img(e.before.as_ref()));
        let a = push(img(e.after.as_ref()));
        (b, a)
    };
    let tables = vec![ImageTable {
        db: e.db_name.clone(),
        table: e.table.clone(),
        images,
    }];
    let run_as = RunAs {
        tenant: tenant.0,
        tenant_name: sprig_sinks::peer::tenant_name(global, tenant.0).await?,
        user_id: user,
        database: String::new(),
    };
    let reply = peer
        .sql(
            seed,
            &run_as.request(SqlOp::Images {
                tables: tables.clone(),
            }),
        )
        .await?
        .ok()?;
    let answer = images_answer(&tables, &reply);
    let v = |i: Option<usize>| {
        i.is_some_and(|i| {
            answer
                .first()
                .and_then(|t| t.get(i))
                .copied()
                .unwrap_or(false)
        })
    };
    Ok((v(bi), v(ai)))
}

/// The op letter change event `e` goes out as for a user who sees
/// `[before, after]` as given; `None`: not at all.
#[must_use]
pub fn shown_op(e: &Op, before: bool, after: bool) -> Option<&'static str> {
    Some(match (e, before, after) {
        (Op::Insert, _, true) | (Op::Update, false, true) => "c",
        (Op::Delete, true, _) | (Op::Update, true, false) => "d",
        (Op::Update, true, true) => "u",
        (Op::Ddl(_), true, _) => "ddl",
        (Op::Truncate, true, _) => "t",
        _ => return None,
    })
}

/// The envelope `body` (the change topic's record) as op `op`: an insert
/// without its before image, a delete without its after image.
#[must_use]
pub fn reshaped(mut body: Value, op: &str) -> Value {
    if let Some(m) = body.as_object_mut() {
        m.insert("op".into(), Value::String(op.to_owned()));
        match op {
            "c" => {
                m.insert("before".into(), Value::Null);
            }
            "d" => {
                m.insert("after".into(), Value::Null);
            }
            _ => {}
        }
    }
    body
}

/// A SQL string literal (default `sql_mode`).
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// The subscription's `owner_ref`.
#[derive(Debug, Clone)]
struct Spec {
    db: String,
    table: String,
    object: u64,
    exchange: String,
    user: u32,
}

impl Spec {
    fn of(owner_ref: Option<&str>) -> std::result::Result<Self, String> {
        let v: Value = serde_json::from_str(owner_ref.unwrap_or(""))
            .map_err(|e| format!("the subscription's owner_ref: {e}"))?;
        let s = |k: &str| {
            v[k].as_str()
                .map(str::to_owned)
                .ok_or(format!("owner_ref.{k} is missing"))
        };
        Ok(Self {
            db: s("db")?,
            table: s("table")?,
            object: v["object"].as_u64().ok_or("owner_ref.object is missing")?,
            exchange: s("exchange")?,
            user: v["user"]
                .as_u64()
                .and_then(|u| u32::try_from(u).ok())
                .ok_or("owner_ref.user is missing")?,
        })
    }
}

/// `change_exchange` (see the module docs).
#[derive(Debug, Clone)]
pub struct ChangeExchange(pub LogicEnv);

#[async_trait::async_trait]
impl JobHandler for ChangeExchange {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(cx.spec["tenant"].as_u64().ok_or("spec.tenant is missing")?);
        let name = cx.spec["name"]
            .as_str()
            .ok_or("spec.name is missing")?
            .to_owned();
        Box::pin(serve(&self.0, cx, tenant, &name))
            .await
            .map_err(|e| e.to_string())
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// What the forwarder's loop reads next.
#[derive(Debug)]
enum Next {
    /// A feed item.
    Item(FeedItem),
    /// Nothing within the wait: the loop's housekeeping runs.
    Quiet,
    /// The job was cancelled.
    Stop,
}

/// The tail as the loop reads it: in a task of its own.
/// `TenantTail::next` isn't cancel-safe, and the loop's 1 s wait would
/// otherwise drop a transaction whose catalog lookup outlasts it (a new
/// table's first row while a move holds the catalog, say).
type Feed = Pump<FeedItem>;

fn feed_of(tail: TenantTail) -> Feed {
    Pump::start(tail, Pump::<FeedItem>::AHEAD, "change exchange forwarder")
}

/// The next feed item, or [`Next::Quiet`] after `wait` without one.
/// Cancel-safe: an item is taken only when it is returned.
async fn read(
    feed: &mut Feed,
    cancel: &sprig_common::CancellationToken,
    wait: Duration,
) -> Result<Next> {
    tokio::select! {
        // Cancel first: unbiased, a ready item could win after a cancel.
        biased;
        () = cancel.cancelled() => Ok(Next::Stop),
        r = tokio::time::timeout(wait, feed.next()) => match r {
            Ok(i) => Ok(Next::Item(i?)),
            Err(_) => Ok(Next::Quiet),
        },
    }
}

#[allow(clippy::too_many_lines)] // the loop of the module docs
async fn serve(env: &LogicEnv, cx: &JobCtx, tenant: TenantId, name: &str) -> Result<Value> {
    let subs = Subscriptions::new(Arc::clone(&env.dc));
    let sub = match subs.get(tenant, name).await {
        Ok(s) => s,
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionNotFound) => {
            return Ok(json!({"stopped": "the subscription is gone"}));
        }
        Err(e) => return Err(e),
    };
    let spec = Spec::of(sub.owner_ref.as_deref()).map_err(SprigError::internal)?;
    let (peer, global) = env
        .gateways
        .clone()
        .ok_or_else(|| SprigError::internal("this worker reaches no gateway"))?;
    let rls = crate::sinks::RlsCatalogs::new(Arc::clone(&env.locator))
        .ok_or_else(|| SprigError::internal("the worker can't load row-level security catalogs"))?;
    let tenant_name = sprig_sinks::peer::tenant_name(global.as_ref(), tenant.0).await?;
    let start = match subs.checkpoint(tenant, name).await? {
        Some(cp) => StartAt::After(cp.resume_token()?),
        None => StartAt::Now,
    };
    let tail = TenantTail::start(
        tenant,
        start,
        Arc::clone(&env.clusters) as Arc<dyn Clusters>,
        Arc::new(MetaPlacement::new(Arc::clone(&env.dc))),
        Arc::new(CatalogResolver::new(Arc::new(crate::logic::Catalogs(
            Arc::clone(&env.locator),
        )))),
        TailOptions::default(),
    )
    .await?;
    let mut feed = feed_of(tail);
    tracing::info!(%tenant, table = %spec.table, exchange = %spec.exchange, job = %cx.job_id, "change exchange forwarder started");
    let run_as = RunAs {
        tenant: tenant.0,
        tenant_name: tenant_name.clone(),
        user_id: spec.user,
        database: spec.db.clone(),
    };
    let ctx = sprig_streams::envelope::ChangeCtx {
        partitions: 1,
        tenant_name: Some(tenant_name),
    };
    let (mut published, mut filtered, mut pending) = (0u64, 0u64, 0u64);
    let mut last: Option<ResumeToken> = None;
    let mut acked = Instant::now();
    let mut checked = Instant::now();
    let mut select_ok: Option<(Instant, bool)> = None;
    loop {
        if cx.cancel.is_cancelled() {
            break;
        }
        if checked.elapsed() > Duration::from_secs(5) {
            checked = Instant::now();
            if let Err(e) = subs.get(tenant, name).await
                && e.code() == Some(SprigCode::FeedSubscriptionNotFound)
            {
                return Ok(json!({"stopped": "the exchange was reset", "published": published}));
            }
        }
        if acked.elapsed() > Duration::from_secs(1)
            && let Some(t) = last.take()
        {
            subs.ack(tenant, name, &t, pending, now_ms()).await?;
            pending = 0;
            acked = Instant::now();
            let _ = cx
                .progress(json!({"published": published, "filtered": filtered}))
                .await;
        }
        let item = match read(&mut feed, &cx.cancel, Duration::from_secs(1)).await? {
            Next::Item(i) => i,
            Next::Quiet => continue,
            Next::Stop => break,
        };
        let e = match item {
            FeedItem::Event(e) => e,
            FeedItem::TxnEnd { token } | FeedItem::Progress { token } => {
                last = Some(token);
                continue;
            }
        };
        let token = e.token.clone();
        if e.object != spec.object {
            last = Some(token);
            continue;
        }
        // The user's view, retried until it can be read.
        let mut attempt = 0u32;
        let op = loop {
            let r = async {
                let bound = rls.binds(tenant, spec.user, &[spec.object]).await?;
                let ddl = matches!(e.op, Op::Ddl(_) | Op::Truncate);
                let (b, a) = if bound || ddl {
                    visible_to(
                        peer.as_ref(),
                        global.as_ref(),
                        tenant,
                        spec.user,
                        spec.object,
                        &e,
                    )
                    .await?
                } else {
                    // Unbound: SELECT on the table decides (cached 2 s).
                    let p = match select_ok {
                        Some((at, p)) if at.elapsed() < Duration::from_secs(2) => p,
                        _ => {
                            let probe = ChangeEvent {
                                op: Op::Truncate,
                                ..(*e).clone()
                            };
                            let (p, _) = visible_to(
                                peer.as_ref(),
                                global.as_ref(),
                                tenant,
                                spec.user,
                                spec.object,
                                &probe,
                            )
                            .await?;
                            select_ok = Some((Instant::now(), p));
                            p
                        }
                    };
                    (p && e.before.is_some(), p && e.after.is_some())
                };
                Ok::<_, SprigError>(shown_op(&e.op, b, a))
            }
            .await;
            match r {
                Ok(op) => break op,
                Err(err) => {
                    attempt += 1;
                    tracing::warn!(%tenant, error = %err, "change exchange: the change can't be checked yet; retrying");
                    let wait = Duration::from_millis(200 * u64::from(attempt.min(25)));
                    tokio::select! {
                        () = cx.cancel.cancelled() => return Ok(json!({"stopped": "cancelled"})),
                        () = tokio::time::sleep(wait) => {}
                    }
                }
            }
        };
        let Some(op) = op else {
            filtered += 1;
            metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "amqp")
                .increment(1);
            last = Some(token);
            continue;
        };
        let Some(rec) = sprig_streams::envelope::record_of(&e, &ctx) else {
            last = Some(token);
            continue;
        };
        let body: Value = rec
            .value
            .as_deref()
            .and_then(|v| serde_json::from_slice(v).ok())
            .unwrap_or(Value::Null);
        let body = reshaped(body, op).to_string();
        let call = format!(
            "CALL sprig.mq_publish({}, {}, {}, JSON_OBJECT('content_type', 'application/json', \
             'delivery_mode', 2), 0, {})",
            lit(&spec.exchange),
            lit(&format!("{}.{}.{op}", spec.db, spec.table)),
            lit(&body),
            lit(&spec.db),
        );
        let mut attempt = 0u32;
        loop {
            let r = peer
                .sql(
                    spec.object,
                    &run_as.request(SqlOp::Publish {
                        statements: vec![call.clone()],
                    }),
                )
                .await
                .and_then(sprig_sinks::peer::SqlReply::ok)
                .and_then(|r| match r.results.first().and_then(|x| x.error.clone()) {
                    Some(pe) => Err(pe.error()),
                    None => Ok(()),
                });
            match r {
                Ok(()) => break,
                Err(err) => {
                    attempt += 1;
                    if attempt == 1 || attempt.is_multiple_of(50) {
                        tracing::warn!(%tenant, exchange = %spec.exchange, error = %err, "change exchange: publish failed; retrying");
                    }
                    let wait = Duration::from_millis(200 * u64::from(attempt.min(25)));
                    tokio::select! {
                        () = cx.cancel.cancelled() => return Ok(json!({"stopped": "cancelled"})),
                        () = tokio::time::sleep(wait) => {}
                    }
                }
            }
        }
        published += 1;
        pending += 1;
        last = Some(token);
        metrics::counter!("sprig_queue_change_exchange_published_total").increment(1);
    }
    if let Some(t) = last {
        let _ = subs.ack(tenant, name, &t, pending, now_ms()).await;
    }
    Ok(json!({"stopped": "cancelled", "published": published}))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: the 1 s housekeeping wait fires while the tail
    /// waits on a new table's catalog lookup (1.5 s here). Every change
    /// must still come once, in order, and a restart from the last token
    /// taken must repeat none.
    #[tokio::test(start_paused = true)]
    async fn the_housekeeping_wait_never_drops_a_change() {
        use sprig_feed::testing::{SlowFeed, keys_once};
        let feed = SlowFeed::new(12, Duration::from_millis(1500));
        let cancel = sprig_common::CancellationToken::new();
        let mut items = Vec::new();
        let mut last = None;
        let mut f = feed_of(feed.tail().await);
        // The first half, then a restart from the last token handled.
        let end = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < end && keys_once(&items).len() < 6 {
            if let Next::Item(i) = read(&mut f, &cancel, Duration::from_secs(1)).await.unwrap() {
                if !matches!(i, FeedItem::Event(_)) {
                    last = Some(i.token().clone());
                }
                items.push(i);
            }
        }
        drop(f);
        // Events after the last boundary token weren't checkpointed.
        let token = last.unwrap();
        let mut kept: Vec<FeedItem> = Vec::new();
        for i in items {
            let boundary = !matches!(i, FeedItem::Event(_)) && i.token() == &token;
            kept.push(i);
            if boundary {
                break;
            }
        }
        let mut f = feed_of(feed.tail_from(StartAt::After(token)).await);
        let end = tokio::time::Instant::now() + Duration::from_secs(120);
        while tokio::time::Instant::now() < end && keys_once(&kept).len() < 12 {
            match read(&mut f, &cancel, Duration::from_secs(1)).await.unwrap() {
                Next::Item(i) => kept.push(i),
                Next::Quiet => {}
                Next::Stop => unreachable!(),
            }
        }
        assert_eq!(keys_once(&kept), (1..=12).collect::<Vec<_>>());
        cancel.cancel();
        assert!(matches!(
            read(&mut f, &cancel, Duration::from_secs(1)).await.unwrap(),
            Next::Stop
        ));
    }

    #[test]
    fn ops_and_bodies() {
        assert_eq!(shown_op(&Op::Update, false, true), Some("c"));
        assert_eq!(shown_op(&Op::Update, true, false), Some("d"));
        assert_eq!(shown_op(&Op::Update, true, true), Some("u"));
        assert_eq!(shown_op(&Op::Update, false, false), None);
        assert_eq!(shown_op(&Op::Insert, false, false), None);
        let b = reshaped(
            json!({"op": "u", "before": {"a": 1}, "after": {"a": 2}}),
            "c",
        );
        assert_eq!(b, json!({"op": "c", "before": null, "after": {"a": 2}}));
        assert_eq!(lit("it's \\"), "'it''s \\\\'");
    }
}
