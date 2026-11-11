//! A tenant's residency for destinations that aren't DCs (chapter 51,
//! "Destinations, allowlist and residency"; chapter 52's webhooks): the
//! policies of the tenant and its ancestors, read from their directory
//! entries, each evaluated against a destination's tags as if they were a
//! DC's. The one copy that sink workers, gateways (`CREATE CONNECTION`)
//! and the controller's approval webhooks share (`sprig-sinks::residency`
//! wraps it in its `Residency` trait).
//!
//! A tenant with a residency block but no readable policy anywhere on its
//! path admits nothing (fail closed). No block on the path: unpinned.

use serde_json::{Map, Value};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::{Consistency, MetaStore};

use crate::Policy;

/// The policies on a tenant's path.
#[derive(Debug, Clone)]
pub struct TenantResidency {
    policies: Vec<Policy>,
    closed: bool,
}

impl TenantResidency {
    /// From policy texts (own and ancestors'); `closed` when a block
    /// exists without any policy to read.
    ///
    /// # Errors
    /// 39506 for a policy that doesn't parse.
    pub fn new(texts: &[String], closed: bool) -> Result<Self> {
        let mut policies = Vec::new();
        for t in texts {
            policies.push(Policy::parse(t)?);
        }
        Ok(Self { policies, closed })
    }

    /// Whether a destination with `tags` satisfies every policy.
    #[must_use]
    pub fn admits_tags(&self, tags: &Map<String, Value>) -> bool {
        if self.closed && self.policies.is_empty() {
            return false;
        }
        let Ok(id) = DcId::new("sink-destination") else {
            return false;
        };
        let dc = DcRecord {
            dc_id: id,
            kind: "full".into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: tags.clone(),
            global_voter: false,
        };
        self.policies.iter().all(|p| p.admits(&dc))
    }

    /// How many policies apply.
    #[must_use]
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    /// Whether no policy applies.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// Whether a block without any policy closed it.
    #[must_use]
    pub fn closed(&self) -> bool {
        self.closed
    }
}

/// The residency of `tenant` from the global tier's directory entries;
/// `None` when nothing on its path has a residency block (unpinned).
///
/// # Errors
/// The store's; 39506 for a bad policy.
pub async fn of_tenant(
    global: &dyn MetaStore,
    tenant: TenantId,
) -> Result<Option<TenantResidency>> {
    let entry = |id: u64| async move {
        let key = global.tier().key(&format!("tenants/{id}"))?;
        Ok::<_, SprigError>(
            global
                .get(&key, Consistency::Serializable)
                .await?
                .and_then(|kv| serde_json::from_slice::<Value>(&kv.value).ok()),
        )
    };
    let Some(me) = entry(tenant.0).await? else {
        return Ok(None);
    };
    let path: Vec<u64> = me
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("")
        .split('/')
        .filter_map(|x| x.parse().ok())
        .collect();
    let mut texts = Vec::new();
    let mut block = false;
    for id in path {
        let e = if id == tenant.0 {
            Some(me.clone())
        } else {
            entry(id).await?
        };
        let Some(r) = e
            .as_ref()
            .and_then(|e| e.get("residency"))
            .filter(|r| !r.is_null())
        else {
            continue;
        };
        block = true;
        if let Some(p) = r.get("policy").and_then(Value::as_str)
            && !p.trim().is_empty()
        {
            texts.push(p.to_owned());
        }
    }
    if !block {
        return Ok(None);
    }
    Ok(Some(TenantResidency::new(&texts, true)?))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sprig_meta::mem::MemStore;
    use sprig_meta::{Op, Tier, Txn};

    #[allow(clippy::needless_pass_by_value)]
    fn tags(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn destinations_against_policies() {
        let r = TenantResidency::new(&["jurisdiction = \"EU\"".to_owned()], true).unwrap();
        assert!(r.admits_tags(&tags(serde_json::json!({"jurisdiction": "EU"}))));
        assert!(!r.admits_tags(&tags(serde_json::json!({"jurisdiction": "US"}))));
        assert!(!r.admits_tags(&tags(serde_json::json!({}))));
        let r = TenantResidency::new(&[], true).unwrap();
        assert!(!r.admits_tags(&tags(serde_json::json!({"jurisdiction": "EU"}))));
        assert!(TenantResidency::new(&["nonsense ==".to_owned()], true).is_err());
    }

    #[tokio::test]
    async fn read_through_the_tree() {
        let g = MemStore::new(Tier::Global);
        let put = |id: u64, v: Value| Op::Put {
            key: Tier::Global.key(&format!("tenants/{id}")).unwrap(),
            value: serde_json::to_vec(&v).unwrap(),
            lease: None,
        };
        g.txn(Txn {
            when: vec![],
            then: vec![
                put(1, serde_json::json!({"path": "/1/", "residency": {"policy": "jurisdiction = \"EU\""}})),
                put(2, serde_json::json!({"path": "/1/2/", "residency": null})),
                put(3, serde_json::json!({"path": "/3/"})),
                put(4, serde_json::json!({"path": "/4/", "residency": {"policy": ""}})),
            ],
            otherwise: vec![],
        })
        .await
        .unwrap();
        let child = of_tenant(&g, TenantId(2)).await.unwrap().unwrap();
        assert_eq!(child.len(), 1, "the ancestor's policy");
        assert!(child.admits_tags(&tags(serde_json::json!({"jurisdiction": "EU"}))));
        assert!(of_tenant(&g, TenantId(3)).await.unwrap().is_none());
        assert!(of_tenant(&g, TenantId(99)).await.unwrap().is_none());
        let closed = of_tenant(&g, TenantId(4)).await.unwrap().unwrap();
        assert!(closed.closed() && closed.is_empty());
        assert!(!closed.admits_tags(&tags(serde_json::json!({"jurisdiction": "EU"}))));
    }
}
