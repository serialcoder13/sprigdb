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
//! a cross-tenant outbox for the tenant; the controller's loop gives each
//! subscribed tenant one forwarder job.

use std::collections::HashSet;
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

fn subscribed() -> &'static Mutex<HashSet<u64>> {
    static S: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Registers `tenant`'s forwarder subscription, once per process, in the
/// background (see the module docs).
pub fn subscribe(svc: &TenantServices, tenant: TenantId) {
    if !subscribed()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(tenant.0)
    {
        return;
    }
    let locator = Arc::clone(&svc.locator);
    let dc = svc.dc.clone();
    tokio::spawn(async move {
        if let Err(e) = register(&locator, &dc, tenant).await {
            tracing::warn!(%tenant, error = %e, "queue forwarder subscription not registered; the polling fallback forwards");
            subscribed()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&tenant.0);
        }
    });
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
    let subs = sprig_feed::subs::Subscriptions::new(store);
    let sub = sprig_feed::subs::Subscription {
        tenant,
        name: SUB_NAME.into(),
        kind: SUB_NAME.into(),
        ordering: "merged".into(),
        owner_ref: None,
        state: "active".into(),
        created_at_ms: u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0),
    };
    match subs.register(&sub).await {
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
