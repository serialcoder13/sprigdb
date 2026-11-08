//! Subscriptions and consumer positions (chapter 38, "Subscriptions and
//! delivery").
//!
//! An internal consumer registers a named subscription, reads the tenant's
//! feed, makes a batch's effects durable, then acks the batch's last token.
//! After a crash it starts after its last ack: delivery is at least once,
//! and exactly once for a consumer that acks only what it made durable.
//!
//! **As built, against the book:** subscriptions and checkpoints live in
//! the DC tier of etcd (`feed/subs/<tenant>/<name>`,
//! `feed/checkpoints/<tenant>/<name>`), not in the tenant catalog's
//! `feed_subscriptions` and `feed_checkpoints`: acks then keep working
//! through a move's hold (whose fence renames the catalog), and the tenant
//! needs no catalog migration. R1 subscriptions are merged, one partition
//! per tenant.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Op, Record, Txn, Versioned};

use crate::token::ResumeToken;

/// A registered subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    /// The tenant.
    pub tenant: TenantId,
    /// Unique per tenant, e.g. `search:41:a`.
    pub name: String,
    /// `ref_follow`, `mover`, `search`, `logic`, `inherit`, `redis`, `sink`.
    pub kind: String,
    /// `merged` (R1 has no per-shard subscriptions yet).
    pub ordering: String,
    /// The job, index or function that owns it.
    #[serde(default)]
    pub owner_ref: Option<String>,
    /// `active` or `paused`.
    pub state: String,
    /// When registered (ms since the Unix epoch).
    pub created_at_ms: u64,
}

impl Versioned for Subscription {
    const V: u32 = 1;
}

/// A subscription's last acknowledged position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The token, hex.
    pub token: String,
    /// The token's cluster (for `lag`).
    pub cluster: String,
    /// The token's position (for people).
    pub gtid_pos: String,
    /// The token's clamp.
    pub clamp_hlc: u64,
    /// Events acknowledged so far.
    pub acked_events: u64,
    /// When written (ms since the Unix epoch).
    pub updated_at_ms: u64,
}

impl Versioned for Checkpoint {
    const V: u32 = 1;
}

impl Checkpoint {
    /// The token.
    ///
    /// # Errors
    /// 39801 for a damaged token.
    pub fn resume_token(&self) -> Result<ResumeToken> {
        ResumeToken::from_hex(&self.token)
    }
}

/// Subscriptions in one DC tier.
#[derive(Clone)]
pub struct Subscriptions {
    store: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriptions").finish_non_exhaustive()
    }
}

fn valid_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("bad subscription name {name:?}"),
        ))
    }
}

impl Subscriptions {
    /// Subscriptions in `store`, the tenant's home DC tier.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self { store }
    }

    fn sub_key(&self, tenant: TenantId, name: &str) -> Result<Key> {
        valid_name(name)?;
        self.store.tier().key(&format!("feed/subs/{tenant}/{name}"))
    }

    fn cp_key(&self, tenant: TenantId, name: &str) -> Result<Key> {
        valid_name(name)?;
        self.store
            .tier()
            .key(&format!("feed/checkpoints/{tenant}/{name}"))
    }

    /// Registers a subscription.
    ///
    /// # Errors
    /// 39804 if the name is taken; the store's errors.
    pub async fn register(&self, sub: &Subscription) -> Result<()> {
        let key = self.sub_key(sub.tenant, &sub.name)?;
        let r = self
            .store
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value: Record::new(sub.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            Ok(())
        } else {
            Err(SprigError::new(
                SprigCode::FeedSubscriptionExists,
                format!("subscription {} exists", sub.name),
            ))
        }
    }

    /// A subscription.
    ///
    /// # Errors
    /// 39805 if there is none by that name.
    pub async fn get(&self, tenant: TenantId, name: &str) -> Result<Subscription> {
        let key = self.sub_key(tenant, name)?;
        let kv = self
            .store
            .get(&key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::FeedSubscriptionNotFound,
                    format!("no subscription {name}"),
                )
            })?;
        Ok(Record::<Subscription>::decode(&kv.value)?.body)
    }

    /// The tenant's subscriptions.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn list(&self, tenant: TenantId) -> Result<Vec<Subscription>> {
        let p = self.store.tier().key(&format!("feed/subs/{tenant}/"))?;
        let page = self.store.list(&p, ListOpts::default()).await?;
        page.kvs
            .iter()
            .map(|kv| Ok(Record::<Subscription>::decode(&kv.value)?.body))
            .collect()
    }

    /// The last acknowledged position, if any.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn checkpoint(&self, tenant: TenantId, name: &str) -> Result<Option<Checkpoint>> {
        let key = self.cp_key(tenant, name)?;
        match self.store.get(&key, Consistency::Linearizable).await? {
            Some(kv) => Ok(Some(Record::<Checkpoint>::decode(&kv.value)?.body)),
            None => Ok(None),
        }
    }

    /// Acknowledges everything up to `token`, `events` more of them. Call
    /// it at most about once a second per subscription.
    ///
    /// # Errors
    /// 39805 for an unknown subscription; 39801 for another tenant's
    /// token; the store's errors.
    pub async fn ack(
        &self,
        tenant: TenantId,
        name: &str,
        token: &ResumeToken,
        events: u64,
        now_ms: u64,
    ) -> Result<()> {
        token.check_tenant(tenant)?;
        let sub = self.sub_key(tenant, name)?;
        let key = self.cp_key(tenant, name)?;
        for _ in 0..8 {
            let cur = self.store.get(&key, Consistency::Linearizable).await?;
            let acked = match &cur {
                Some(kv) => Record::<Checkpoint>::decode(&kv.value)?.body.acked_events,
                None => 0,
            };
            let cp = Checkpoint {
                token: token.to_hex(),
                cluster: token.cluster.to_string(),
                gtid_pos: token.pos.to_string(),
                clamp_hlc: token.clamp,
                acked_events: acked + events,
                updated_at_ms: now_ms,
            };
            let guard = match &cur {
                Some(kv) => Cmp::ModRev(key.clone(), kv.mod_revision),
                None => Cmp::Missing(key.clone()),
            };
            let sub_kv = self.store.get(&sub, Consistency::Linearizable).await?;
            let Some(sub_kv) = sub_kv else {
                return Err(SprigError::new(
                    SprigCode::FeedSubscriptionNotFound,
                    format!("no subscription {name}"),
                ));
            };
            let r = self
                .store
                .txn(Txn {
                    when: vec![guard, Cmp::ModRev(sub.clone(), sub_kv.mod_revision)],
                    then: vec![Op::Put {
                        key: key.clone(),
                        value: Record::new(cp).encode()?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                return Ok(());
            }
        }
        Err(SprigError::new(
            SprigCode::CatalogConflict,
            format!("subscription {name}'s checkpoint kept changing"),
        ))
    }

    /// Drops a subscription and its checkpoint.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn drop_subscription(&self, tenant: TenantId, name: &str) -> Result<()> {
        let ops = vec![
            Op::Delete(self.sub_key(tenant, name)?),
            Op::Delete(self.cp_key(tenant, name)?),
        ];
        self.store
            .txn(Txn {
                when: Vec::new(),
                then: ops,
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sprig_binlog::GtidPos;
    use sprig_common::ids::{ClusterId, DcId};
    use sprig_meta::{MemStore, Tier};

    use super::*;

    #[tokio::test]
    async fn register_ack_resume_drop() {
        let store: Arc<dyn MetaStore> =
            Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-a").unwrap())));
        let subs = Subscriptions::new(store);
        let t = TenantId(9);
        let sub = Subscription {
            tenant: t,
            name: "search:1:a".into(),
            kind: "search".into(),
            ordering: "merged".into(),
            owner_ref: None,
            state: "active".into(),
            created_at_ms: 1,
        };
        subs.register(&sub).await.unwrap();
        let e = subs.register(&sub).await.unwrap_err();
        assert!(matches!(
            e.kind,
            sprig_common::ErrorKind::Sprig(SprigCode::FeedSubscriptionExists)
        ));
        assert_eq!(subs.get(t, "search:1:a").await.unwrap(), sub);
        assert!(subs.checkpoint(t, "search:1:a").await.unwrap().is_none());
        let tok = ResumeToken {
            tenant: t,
            clamp: 5,
            cluster: ClusterId::new("dc-a-c1").unwrap(),
            segment: 0,
            pos: GtidPos::new(),
            until: None,
            progress: true,
        };
        subs.ack(t, "search:1:a", &tok, 3, 10).await.unwrap();
        subs.ack(t, "search:1:a", &tok, 2, 11).await.unwrap();
        let cp = subs.checkpoint(t, "search:1:a").await.unwrap().unwrap();
        assert_eq!(cp.acked_events, 5);
        assert_eq!(cp.resume_token().unwrap(), tok);
        let other = ResumeToken {
            tenant: TenantId(8),
            ..tok.clone()
        };
        assert!(subs.ack(t, "search:1:a", &other, 1, 12).await.is_err());
        assert_eq!(subs.list(t).await.unwrap().len(), 1);
        subs.drop_subscription(t, "search:1:a").await.unwrap();
        assert!(subs.get(t, "search:1:a").await.is_err());
        assert!(subs.ack(t, "search:1:a", &tok, 1, 13).await.is_err());
    }
}
