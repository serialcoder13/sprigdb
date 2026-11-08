//! Operator pins (chapter 11, "Manual moves and pins"): `sprigctl tenant
//! pin <tenant> [--cluster <id>]`, `sprigctl tenant unpin <tenant>` and
//! `sprigctl shard move <tenant> --to <cluster> --pin`.
//!
//! A pin keeps a tenant where an operator put it: the balancer never moves
//! it, and drains and residency changes report it as blocked instead of
//! moving it until the operator unpins it. Only an operator's own move
//! (`sprigctl shard move`) moves a pinned tenant.
//!
//! Pins live in the tenant's home DC tier next to its placement, at
//! `/sprig/dc/<dc_id>/pins/<tenant_id>` (the shard maps are at
//! `shardmaps/<tenant_id>/<group>`), as JSON: the cluster, who pinned it,
//! when, and why. R0 pins a whole tenant, the unit R0 moves.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_meta::{Consistency, Key, ListOpts, MetaStore, Op, Record, Txn, Versioned};

use crate::model::{FleetView, Timestamp};

/// One pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantPin {
    /// The tenant.
    pub tenant_id: TenantId,
    /// The cluster it is pinned to.
    pub cluster: ClusterId,
    /// The operator principal.
    pub pinned_by: String,
    /// When, seconds since the epoch.
    pub pinned_at: Timestamp,
    /// Why, as the operator said.
    #[serde(default)]
    pub reason: String,
}

impl Versioned for TenantPin {
    const V: u32 = 1;
}

impl TenantPin {
    /// "pinned by operator alice to mum1-c7".
    #[must_use]
    pub fn describe(&self) -> String {
        let s = format!("pinned by operator {} to {}", self.pinned_by, self.cluster);
        if self.reason.is_empty() {
            s
        } else {
            format!("{s} ({})", self.reason)
        }
    }
}

/// Pins in one DC tier.
pub struct PinStore {
    store: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for PinStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinStore").finish_non_exhaustive()
    }
}

impl PinStore {
    /// Pins in `store`'s tier (the DC tier).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self { store }
    }

    fn key(&self, t: TenantId) -> Result<Key> {
        self.store.tier().key(&format!("pins/{t}"))
    }

    /// Pins (or re-pins) a tenant.
    ///
    /// # Errors
    /// The store's.
    pub async fn pin(&self, p: &TenantPin) -> Result<()> {
        self.store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key: self.key(p.tenant_id)?,
                    value: Record::new(p.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// Unpins a tenant; returns the pin it had.
    ///
    /// # Errors
    /// The store's.
    pub async fn unpin(&self, t: TenantId) -> Result<Option<TenantPin>> {
        let had = self.get(t).await?;
        if had.is_some() {
            self.store
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Delete(self.key(t)?)],
                    otherwise: Vec::new(),
                })
                .await?;
        }
        Ok(had)
    }

    /// A tenant's pin.
    ///
    /// # Errors
    /// The store's; a record that doesn't decode.
    pub async fn get(&self, t: TenantId) -> Result<Option<TenantPin>> {
        self.store
            .get(&self.key(t)?, Consistency::Linearizable)
            .await?
            .map(|kv| Record::<TenantPin>::decode(&kv.value).map(|r| r.body))
            .transpose()
    }

    /// Every pin in the DC.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<BTreeMap<TenantId, TenantPin>> {
        let prefix = self.store.tier().key("pins/")?;
        let mut out = BTreeMap::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if let Ok(r) = Record::<TenantPin>::decode(&kv.value) {
                    out.insert(r.body.tenant_id, r.body);
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
}

/// Marks pinned tenants in the view (the controller calls this after
/// building it).
pub fn apply_pins(view: &mut FleetView, pins: &BTreeMap<TenantId, TenantPin>) {
    for (t, tv) in &mut view.tenants {
        tv.pin = pins.get(t).cloned();
        tv.pinned = tv.pin.is_some();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};

    use super::*;
    use crate::model::{ShardKey, ShardView};
    use crate::testutil::{cid, fleet, load};

    #[tokio::test]
    async fn pins_round_trip_and_mark_the_view() {
        let dc = DcId::new("mum1").unwrap();
        let st = PinStore::new(Arc::new(MemStore::new(Tier::Dc(dc))));
        let p = TenantPin {
            tenant_id: TenantId(42),
            cluster: cid(1),
            pinned_by: "alice".to_owned(),
            pinned_at: 100,
            reason: "latency tests".to_owned(),
        };
        st.pin(&p).await.unwrap();
        assert_eq!(st.get(TenantId(42)).await.unwrap(), Some(p.clone()));
        let all = st.list().await.unwrap();
        assert_eq!(all.len(), 1);
        let mut v = fleet(2);
        v.add_shard(ShardView::new(
            ShardKey::new(42, 0, 0),
            cid(1),
            load(1.0, 0.0),
        ));
        v.add_shard(ShardView::new(
            ShardKey::new(43, 0, 0),
            cid(1),
            load(1.0, 0.0),
        ));
        apply_pins(&mut v, &all);
        assert!(v.tenants[&TenantId(42)].pinned);
        assert!(!v.tenants[&TenantId(43)].pinned);
        assert_eq!(
            p.describe(),
            "pinned by operator alice to mum1-c1 (latency tests)"
        );
        assert_eq!(st.unpin(TenantId(42)).await.unwrap(), Some(p));
        assert!(st.get(TenantId(42)).await.unwrap().is_none());
        apply_pins(&mut v, &st.list().await.unwrap());
        assert!(!v.tenants[&TenantId(42)].pinned);
    }
}
