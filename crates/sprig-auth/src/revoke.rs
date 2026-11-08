//! The auth change feed: revocation (chapter 14, "Caching and
//! revocation"; build step 8).
//!
//! A drop, a lock, a password change or any other auth change commits with
//! a new auth version in the tenant's directory entry. Every gateway's
//! directory watch sees the entry move and its catalog cache reloads the
//! tenant: at once when sessions here pin it, otherwise at the next login,
//! which asks for the directory's version (`Freshness::Latest`). That is
//! the path a setting change takes to every gateway (M0.3 measured 73 ms to
//! a second gateway).
//!
//! [`AuthChanges`] follows the cache's catalog load feed
//! ([`TenantCache::loads`](sprig_catalog::TenantCache::loads)) and turns
//! each load whose auth version moved into one [`AuthChange`]: the new
//! snapshot (with every auth module's state, so chapter 29's privilege
//! cache can follow it too) and the users it revokes, found by comparing
//! the old and new [`TenantAuth`]:
//!
//! | Change | [`RevokeReason`] | Sessions close |
//! |---|---|---|
//! | user gone (dropped) | `Dropped` | yes |
//! | `ACCOUNT LOCK` set | `Locked` | yes |
//! | password method removed | `PasswordLoginRemoved` | yes |
//! | temporary lock started | `TempLocked` | no: an attacker's failures would end the victim's sessions |
//! | primary or secondary verifier replaced or removed | `PasswordChanged` | no, as in MySQL |
//! | `PASSWORD EXPIRE` set | `PasswordExpired` | no, as in MySQL |
//!
//! New logins need nothing from this feed: they read the reloaded snapshot.
//! Sessions do: the gateway subscribes and asks the sessions of revoked
//! users to close at their next safe point (`auth.close_sessions_on_revoke`,
//! 39306).

use std::collections::HashSet;
use std::sync::Arc;

use sprig_catalog::{CatalogLoaded, TenantCatalog};
use sprig_common::ids::{TenantId, UserId};
use tokio::sync::broadcast;

use crate::cache::{AuthUser, TenantAuth};
use crate::store::AuthMethods;

/// Why a user's credentials or sessions are no longer good.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RevokeReason {
    /// `DROP USER`.
    Dropped,
    /// `ACCOUNT LOCK`.
    Locked,
    /// A temporary lock after failed logins.
    TempLocked,
    /// A new password, `DISCARD OLD PASSWORD`, or a verifier removed.
    PasswordChanged,
    /// `PASSWORD EXPIRE`.
    PasswordExpired,
    /// The account no longer accepts passwords (`auth_methods`).
    PasswordLoginRemoved,
}

impl RevokeReason {
    /// Whether open sessions of the user close (with
    /// `auth.close_sessions_on_revoke` on).
    #[must_use]
    pub fn closes_sessions(self) -> bool {
        matches!(
            self,
            RevokeReason::Dropped | RevokeReason::Locked | RevokeReason::PasswordLoginRemoved
        )
    }

    /// A label for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RevokeReason::Dropped => "dropped",
            RevokeReason::Locked => "locked",
            RevokeReason::TempLocked => "temp_locked",
            RevokeReason::PasswordChanged => "password_changed",
            RevokeReason::PasswordExpired => "password_expired",
            RevokeReason::PasswordLoginRemoved => "password_login_removed",
        }
    }
}

/// One revoked user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Revoked {
    /// The user.
    pub user_id: UserId,
    /// Why.
    pub reason: RevokeReason,
}

/// A load whose auth version moved.
#[derive(Debug, Clone)]
pub struct AuthChange {
    /// The tenant.
    pub tenant: TenantId,
    /// The replaced snapshot's auth version (0 on a first load).
    pub from_version: u64,
    /// The new auth version.
    pub auth_version: u64,
    /// The new snapshot.
    pub catalog: Arc<TenantCatalog>,
    /// Users it revokes; empty when nothing was revoked (a new user, a
    /// setting).
    pub revoked: Vec<Revoked>,
}

/// What changed for one user between two auth states.
fn compare(old: &AuthUser, new: Option<&AuthUser>, out: &mut Vec<Revoked>) {
    let id = old.row.user_id;
    let mut push = |reason| out.push(Revoked { user_id: id, reason });
    let Some(new) = new else {
        push(RevokeReason::Dropped);
        return;
    };
    if new.row.account_locked && !old.row.account_locked {
        push(RevokeReason::Locked);
    }
    if old.row.auth_methods.contains(AuthMethods::PASSWORD)
        && !new.row.auth_methods.contains(AuthMethods::PASSWORD)
    {
        push(RevokeReason::PasswordLoginRemoved);
    }
    if new.row.password_expired && !old.row.password_expired {
        push(RevokeReason::PasswordExpired);
    }
    let ids = |u: &AuthUser| -> HashSet<u64> {
        u.caching_sha2
            .iter()
            .map(|v| v.id)
            .chain(u.native.iter().map(|v| v.id))
            .collect()
    };
    let (o, n) = (ids(old), ids(new));
    if !o.is_empty() && !o.is_subset(&n) {
        push(RevokeReason::PasswordChanged);
    }
}

/// The users `new` revokes compared with `old`.
#[must_use]
pub fn revoked_between(old: &TenantAuth, new: &TenantAuth, now: std::time::SystemTime) -> Vec<Revoked> {
    let mut out = Vec::new();
    for u in old.users() {
        compare(u, new.user(u.row.user_id).map(|a| &**a), &mut out);
    }
    for u in new.users() {
        let id = u.row.user_id;
        let was = old.temp_lock(id).is_some_and(|l| l.locked_until > now);
        let is = new.temp_lock(id).is_some_and(|l| l.locked_until > now);
        if is && !was {
            out.push(Revoked {
                user_id: id,
                reason: RevokeReason::TempLocked,
            });
        }
    }
    out.sort_by_key(|r| (r.user_id, r.reason as u8));
    out
}

/// The feed's buffer per subscriber.
const CAPACITY: usize = 1024;

/// The auth change feed (see the module docs). The task stops when the
/// catalog load feed closes (the cache was dropped).
#[derive(Debug)]
pub struct AuthChanges {
    tx: broadcast::Sender<Arc<AuthChange>>,
}

impl AuthChanges {
    /// Follows `loads` (from
    /// [`TenantCache::loads`](sprig_catalog::TenantCache::loads)) on a
    /// task of its own.
    #[must_use]
    pub fn start(mut loads: broadcast::Receiver<CatalogLoaded>) -> Arc<Self> {
        let (tx, _) = broadcast::channel(CAPACITY);
        let me = Arc::new(Self { tx: tx.clone() });
        tokio::spawn(async move {
            loop {
                match loads.recv().await {
                    Ok(l) => {
                        if let Some(c) = change_of(&l) {
                            for r in &c.revoked {
                                metrics::counter!("sprig_auth_revocations_total", "reason" => r.reason.as_str())
                                    .increment(1);
                            }
                            tracing::debug!(tenant = %c.tenant, from = c.from_version, to = c.auth_version, revoked = c.revoked.len(), "auth change");
                            let _ = tx.send(Arc::new(c));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "auth change feed lagged behind the catalog loads");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        me
    }

    /// Every later auth change.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<AuthChange>> {
        self.tx.subscribe()
    }

    /// Publishes `change` as if a load had produced it: tests, and callers
    /// that learn of changes another way.
    pub fn publish(&self, change: AuthChange) {
        let _ = self.tx.send(Arc::new(change));
    }
}

/// The [`AuthChange`] of one load, if its auth version moved.
#[must_use]
pub fn change_of(l: &CatalogLoaded) -> Option<AuthChange> {
    let from = l.prev.as_ref().map_or(0, |p| p.auth_version);
    if l.prev.is_some() && l.current.auth_version <= from {
        return None;
    }
    let revoked = match (
        l.prev.as_deref().and_then(TenantAuth::of),
        TenantAuth::of(&l.current),
    ) {
        (Some(old), Some(new)) => revoked_between(old, new, std::time::SystemTime::now()),
        _ => Vec::new(),
    };
    Some(AuthChange {
        tenant: l.tenant,
        from_version: from,
        auth_version: l.current.auth_version,
        catalog: Arc::clone(&l.current),
        revoked,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::time::Duration;

    use super::*;
    use crate::cache::tests::{at, row, sha2};
    use crate::mysql::tests::snapshot;
    use crate::store::{Slot, TempLock};

    fn base() -> TenantAuth {
        let users = (1..=6).map(|i| row(i, &format!("u{i}"), "%", 1)).collect();
        let vs: Vec<_> = (1..=6)
            .map(|i| sha2(u64::from(i) * 10, i, Slot::Primary, b"pw"))
            .collect();
        TenantAuth::build(users, &vs, Vec::new())
    }

    #[test]
    fn each_kind_of_revocation_is_found() {
        let old = base();
        let now = at();
        let mut locked = row(2, "u2", "%", 2);
        locked.account_locked = true;
        let mut expired = row(3, "u3", "%", 2);
        expired.password_expired = true;
        let mut nopw = row(4, "u4", "%", 2);
        nopw.auth_methods = AuthMethods::API_KEY;
        let mut dropped = row(1, "u1", "%", 2);
        dropped.state = crate::store::UserState::Dropped;
        let new = old.apply(
            vec![dropped, locked, expired, nopw, row(5, "u5", "%", 2)],
            &[
                sha2(20, 2, Slot::Primary, b"pw"),
                sha2(30, 3, Slot::Primary, b"pw"),
                sha2(40, 4, Slot::Primary, b"pw"),
                // u5: a new password replaces verifier 50.
                sha2(51, 5, Slot::Primary, b"new"),
            ],
            vec![TempLock {
                user_id: UserId(6),
                locked_until: now + Duration::from_secs(60),
                consecutive_failures: 3,
            }],
        );
        let got = revoked_between(&old, &new, now);
        let want = [
            (1, RevokeReason::Dropped),
            (2, RevokeReason::Locked),
            (3, RevokeReason::PasswordExpired),
            (4, RevokeReason::PasswordLoginRemoved),
            (5, RevokeReason::PasswordChanged),
            (6, RevokeReason::TempLocked),
        ];
        assert_eq!(
            got.iter()
                .map(|r| (r.user_id.0, r.reason))
                .collect::<Vec<_>>(),
            want
        );
        let closing: Vec<u32> = got
            .iter()
            .filter(|r| r.reason.closes_sessions())
            .map(|r| r.user_id.0)
            .collect();
        assert_eq!(closing, vec![1, 2, 4]);
    }

    #[test]
    fn retaining_the_old_password_revokes_nothing() {
        let old = base();
        let new = old.apply(
            vec![row(1, "u1", "%", 2)],
            &[
                sha2(11, 1, Slot::Primary, b"new"),
                sha2(10, 1, Slot::Secondary, b"pw"),
            ],
            Vec::new(),
        );
        assert!(revoked_between(&old, &new, at()).is_empty());
    }

    #[tokio::test]
    async fn the_feed_publishes_only_auth_moves() {
        let (tx, rx) = broadcast::channel(8);
        let feed = AuthChanges::start(rx);
        let mut sub = feed.subscribe();
        let t = TenantId(9);
        let v1 = snapshot(t, base(), 1);
        let mut dropped = row(1, "u1", "%", 2);
        dropped.state = crate::store::UserState::Dropped;
        let v2 = snapshot(t, base().apply(vec![dropped], &[], Vec::new()), 2);
        tx.send(CatalogLoaded {
            tenant: t,
            prev: None,
            current: Arc::clone(&v1),
        })
        .unwrap();
        // Same auth version (a catalog-only change): nothing.
        tx.send(CatalogLoaded {
            tenant: t,
            prev: Some(Arc::clone(&v1)),
            current: Arc::clone(&v1),
        })
        .unwrap();
        tx.send(CatalogLoaded {
            tenant: t,
            prev: Some(v1),
            current: v2,
        })
        .unwrap();
        let first = sub.recv().await.unwrap();
        assert_eq!((first.from_version, first.auth_version), (0, 1));
        assert!(first.revoked.is_empty());
        let second = sub.recv().await.unwrap();
        assert_eq!((second.from_version, second.auth_version), (1, 2));
        assert_eq!(
            second.revoked,
            vec![Revoked {
                user_id: UserId(1),
                reason: RevokeReason::Dropped
            }]
        );
        drop(tx);
        drop(feed);
        assert!(matches!(
            sub.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
    }
}
