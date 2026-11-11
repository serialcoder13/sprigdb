//! The gateway's side of the feed-driven outbox forwarder (chapter 50,
//! "Publishing inside a database transaction"; chapter 42, "Inside a
//! database transaction"; R9, r9fwd).
//!
//! A worker job per tenant (`queue_forwarder`, `sprig_worker::queue_forwarder`)
//! reads the tenant's change feed for inserts into the queue outbox
//! (`__sprig_qo`) and the cross-tenant outbox (`__sprig_xo`) in commit
//! order, and sends each run of rows here (`LogicPeer.SinkSql`, op
//! `outbox_forward`), one run at a time:
//!
//! ```text
//! __sprig_qo: the vhost of the schema's database (as an internal
//!   principal); SqlVhost::forward_outbox(ids): the rows still there, in
//!   the order given, to their owners; wait for every confirm (each
//!   enqueue also writes the forward ledger __sprig_qf in the owner's
//!   batch); delete them
//! __sprig_xo: outbox::forward_ordered(ids): lock the rows (FOR UPDATE,
//!   waiting for a commit hook or sweeper holding them), deliver each
//!   destination's rows in the order given, delete them, commit
//! ```
//!
//! Rows already gone (forwarded by the fallback, or by an earlier attempt
//! of the same run) are counted as `gone`, never forwarded again.
//!
//! [`subscribe`] registers the tenant's `queue_forwarder` feed
//! subscription the first time this gateway opens a queue store or makes
//! a cross-tenant outbox for the tenant, with the tenant's binlog position
//! then as its first checkpoint, and the caller waits for it; the
//! controller's loop gives each tenant's cluster a forwarder job, which
//! starts the tenant's task within 5 s at that checkpoint. So a new
//! tenant's first publishes are forwarded from the feed, in commit order,
//! not by the 2 s fallback (decision 88; r9fanin).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use sprig_common::ids::{DbId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Tier, TierView};
use sprig_sinks::peer::{SqlReply, SqlResult};

use crate::tenant::TenantServices;

/// The subscription's name and kind (`feed/subs/<tenant>/queue_forwarder`).
pub const SUB_NAME: &str = "queue_forwarder";

/// The queue outbox.
pub const QO: &str = "__sprig_qo";

/// The cross-tenant outbox.
pub const XO: &str = crate::inherited::outbox::TABLE;

/// How long a caller waits for the subscription before it goes on (the
/// polling fallback forwards whatever the feed misses).
pub const SUBSCRIBE_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

type Once = Arc<tokio::sync::OnceCell<bool>>;

fn subscribed() -> &'static Mutex<HashMap<u64, Once>> {
    static S: OnceLock<Mutex<HashMap<u64, Once>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Registers `tenant`'s forwarder subscription, once per process, and
/// waits for it (at most [`SUBSCRIBE_WAIT`]): the callers open a queue
/// store or make a cross-tenant outbox before the tenant's first outbox
/// row, so the subscription, with its start position, exists before that
/// row commits (decision 88, r9fanin). Concurrent callers wait for the
/// same registration. A failed one is tried again by the next caller.
pub async fn subscribe(svc: &TenantServices, tenant: TenantId) {
    let cell = Arc::clone(
        subscribed()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(tenant.0)
            .or_default(),
    );
    if cell.initialized() {
        return;
    }
    let locator = Arc::clone(&svc.locator);
    let dc = svc.dc.clone();
    let run = cell.get_or_init(|| async move {
        match register(&locator, &dc, tenant).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(%tenant, error = %e, "queue forwarder subscription not registered; the polling fallback forwards");
                false
            }
        }
    });
    let ok = match tokio::time::timeout(SUBSCRIBE_WAIT, run).await {
        Ok(ok) => *ok,
        Err(_) => {
            tracing::warn!(%tenant, "queue forwarder subscription still registering; the polling fallback forwards meanwhile");
            return;
        }
    };
    if !ok {
        let mut m = subscribed().lock().unwrap_or_else(PoisonError::into_inner);
        if m.get(&tenant.0).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
            m.remove(&tenant.0);
        }
    }
}

/// The tenant's feed position now, as a progress token: where its
/// forwarder starts when the subscription is new.
async fn start_token(
    locator: &crate::tenant::locator::GatewayLocator,
    store: &Arc<dyn sprig_meta::MetaStore>,
    tenant: TenantId,
) -> Result<sprig_feed::ResumeToken> {
    use mysql_async::prelude::Queryable;
    use sprig_feed::PlacementView;
    let placement = sprig_feed::MetaPlacement::new(Arc::clone(store));
    let (cluster, _) = placement.current(tenant).await?;
    let segment = placement
        .history(tenant)
        .await?
        .last()
        .map_or(0, |h| h.map_version);
    let pool = locator.ctl_pool(&cluster).await?;
    let unavailable = |e: mysql_async::Error| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("read @@gtid_binlog_pos of {cluster}: {e}"),
        )
    };
    let mut c = pool.get_conn().await.map_err(unavailable)?;
    let pos: Option<String> = c
        .query_first("SELECT @@gtid_binlog_pos")
        .await
        .map_err(unavailable)?;
    drop(c);
    let pos: sprig_binlog::GtidPos = pos.unwrap_or_default().parse()?;
    Ok(sprig_feed::ResumeToken {
        tenant,
        clamp: 0,
        cluster,
        segment,
        pos,
        until: None,
        progress: true,
    })
}

async fn register(
    locator: &crate::tenant::locator::GatewayLocator,
    me: &sprig_common::ids::DcId,
    tenant: TenantId,
) -> Result<()> {
    let home = locator.home_of(tenant);
    let dc = locator.dc_store();
    let store = if &home == me {
        dc
    } else {
        TierView::over(&dc, Tier::Dc(home))
    };
    let subs = sprig_feed::subs::Subscriptions::new(Arc::clone(&store));
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0);
    let sub = sprig_feed::subs::Subscription {
        tenant,
        name: SUB_NAME.into(),
        kind: SUB_NAME.into(),
        ordering: "merged".into(),
        owner_ref: None,
        state: "active".into(),
        created_at_ms: now,
    };
    // Registered already (another gateway, an earlier process): its job
    // reads from its own checkpoint.
    if subs.get(tenant, SUB_NAME).await.is_ok() {
        return Ok(());
    }
    // With the position now, so the job reads the first publishes from the
    // feed in commit order, however late it starts; without it (the
    // cluster unreadable), from the job's start, as before.
    let r = match start_token(locator, &store, tenant).await {
        Ok(tok) => subs.register_at(&sub, &tok, now).await,
        Err(e) => {
            tracing::debug!(%tenant, error = %e, "queue forwarder start position not read; the job starts at its own start");
            subs.register(&sub).await
        }
    };
    match r {
        Ok(()) => {
            tracing::info!(%tenant, "queue forwarder subscription registered");
            Ok(())
        }
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionExists) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Whether the tenant's transactional publishes in database `db` are
/// forwarded from the feed (`queue.forwarder = feed`, the default).
#[must_use]
pub fn feed_on(svc: &TenantServices, tenant: TenantId, db: Option<DbId>) -> bool {
    let Some(cat) = svc.cache.cached(tenant) else {
        return true;
    };
    let mut ctx = cat.setting_ctx(&svc.dc);
    if let Some(d) = db {
        ctx = ctx.with_db(d);
    }
    svc.settings.get(
        &sprig_queues::settings::FORWARDER,
        &ctx,
        Some(&cat.settings),
    ) == "feed"
}

fn bad(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn id16(h: &str) -> Result<[u8; 16]> {
    let h = h.trim();
    if h.len() != 32 || !h.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad(format!("bad out id {h:?}")));
    }
    let mut a = [0u8; 16];
    for (i, b) in a.iter_mut().enumerate() {
        *b = u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).map_err(|e| bad(e.to_string()))?;
    }
    Ok(a)
}

/// Serves one `outbox_forward` request of tenant `tenant`.
///
/// # Errors
/// A malformed request; the queue engine's or the clusters' (nothing was
/// deleted then: the worker tries the run again).
pub async fn serve(
    svc: &Arc<TenantServices>,
    tenant: u64,
    schema: &str,
    table: &str,
    ids: &[String],
) -> Result<SqlReply> {
    let tenant = TenantId(tenant);
    let parsed: SchemaName = schema
        .parse()
        .map_err(|_| bad(format!("{schema} is not a shard schema")))?;
    let SchemaName::Shard { db, .. } = parsed else {
        return Err(bad(format!("{schema} is not a shard schema")));
    };
    if parsed.tenant() != tenant {
        return Err(bad(format!("{schema} is not tenant {tenant}'s")));
    }
    let ids: Vec<[u8; 16]> = ids.iter().map(|h| id16(h)).collect::<Result<_>>()?;
    // `queue.forwarder = poll` for the database: the gateways' forwarders
    // take the rows at once; the feed leaves them.
    let (forwarded, gone, left) = if feed_on(svc, tenant, Some(db)) {
        match table {
            QO => forward_qo(svc, tenant, db, &ids).await?,
            XO => crate::inherited::outbox::forward_ordered(svc, tenant, schema, &ids).await?,
            t => return Err(bad(format!("{t} is not an outbox"))),
        }
    } else {
        (0, 0, ids.len())
    };
    let n = |v: usize| SqlResult::cell(Some(v.to_string().as_bytes()));
    Ok(SqlReply {
        results: vec![SqlResult {
            columns: vec!["forwarded".into(), "gone".into(), "left".into()],
            rows: vec![vec![n(forwarded), n(gone), n(left)]],
            ..SqlResult::default()
        }],
        ..SqlReply::default()
    })
}

async fn forward_qo(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: DbId,
    ids: &[[u8; 16]],
) -> Result<(usize, usize, usize)> {
    let engine = svc.mq.get().cloned().ok_or_else(|| {
        let mut e = SprigError::new(
            SprigCode::ForwardFailed,
            "the queue engine isn't running on this gateway (AMQP is off)",
        );
        e.retryable = true;
        e
    })?;
    let cat = svc
        .cache
        .get(tenant, sprig_catalog::Freshness::Latest)
        .await?;
    let Some(row) = cat.databases.get(db) else {
        // The database is gone, its outbox with it.
        return Ok((0, ids.len(), 0));
    };
    let name = crate::inherited::tenant_name(svc, tenant).await?;
    let v = engine
        .sql_vhost(
            Arc::new(crate::inherited::mq::internal_principal(tenant, &name)),
            &row.name,
            0,
        )
        .await
        .map_err(|e| mq_err(&e))?;
    let (f, g) = v.forward_outbox(ids).await.map_err(|e| mq_err(&e))?;
    Ok((f, g, 0))
}

fn mq_err(e: &sprig_proto_amqp::error::MqError) -> SprigError {
    let mut s = SprigError::new(
        SprigCode::ForwardFailed,
        format!("{} ({})", e.text, e.code.number()),
    );
    s.retryable = true;
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_ids_parse() {
        let h = "0192f0a1b2c3d4e5f60718293a4b5c6d";
        let a = id16(h).unwrap_or_else(|_| unreachable!());
        assert_eq!(a[0], 0x01);
        assert_eq!(a[15], 0x6d);
        assert!(id16("xyz").is_err());
        assert!(id16(&"g".repeat(32)).is_err());
    }
}
