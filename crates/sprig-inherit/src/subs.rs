//! The copy worker's feed subscriptions (chapter 42, "Materialized
//! copies"; chapter 38, "Subscriptions and delivery").
//!
//! One worker job keeps every copy of a tenant tree and orphans across its
//! tenants. It reads the feed of each tenant of the tree through a
//! subscription named `inherit:f<root tenant>` of kind `inherit`. The
//! gateway registers it on the sharing tenant and its ancestors when a
//! share needs the worker, from the cluster's binlog position at that
//! moment (before the share commits), so the worker, whenever it starts,
//! sees every change after it; the worker registers tenants of the tree
//! that join later the same way. The controller's `inherit` loop keeps one
//! `inherit_copy` job per tree that has subscriptions.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode};
use sprig_feed::ResumeToken;
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_meta::MetaStore;

/// The subscription kind.
pub const KIND: &str = "inherit";

/// The worker job's kind.
pub const JOB_KIND: &str = "inherit_copy";

/// The subscription of `root`'s tree.
#[must_use]
pub fn name(root: TenantId) -> String {
    format!("inherit:f{}", root.0)
}

/// The tree root a subscription name stands for.
#[must_use]
pub fn root_of(name: &str) -> Option<TenantId> {
    name.strip_prefix("inherit:f")?.parse().ok().map(TenantId)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Registers `tenant`'s subscription for `root`'s tree with its first
/// checkpoint at `pos` on `cluster`, unless it exists. Returns whether it
/// was new.
///
/// # Errors
/// The store's errors; 39801 for a bad position.
pub async fn register(
    dc: Arc<dyn MetaStore>,
    tenant: TenantId,
    root: TenantId,
    cluster: ClusterId,
    pos: &str,
) -> Result<bool> {
    let subs = Subscriptions::new(Arc::clone(&dc));
    let n = name(root);
    if subs.get(tenant, &n).await.is_ok() {
        return Ok(false);
    }
    let history = sprig_feed::ownership::history(&dc, tenant, 0).await?;
    let segment = history.last().map_or(0, |h| h.map_version);
    let token = ResumeToken {
        tenant,
        clamp: 0,
        cluster,
        segment,
        pos: pos.parse()?,
        until: None,
        progress: true,
    };
    match subs
        .register(&Subscription {
            tenant,
            name: n.clone(),
            kind: KIND.into(),
            ordering: "merged".into(),
            owner_ref: Some(format!("tree:{root}")),
            state: "active".into(),
            created_at_ms: now_ms(),
        })
        .await
    {
        Ok(()) => {}
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionExists) => return Ok(false),
        Err(e) => return Err(e),
    }
    subs.ack(tenant, &n, &token, 0, now_ms()).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        assert_eq!(name(TenantId(42)), "inherit:f42");
        assert_eq!(root_of("inherit:f42"), Some(TenantId(42)));
        assert_eq!(root_of("logic:b1"), None);
    }
}
