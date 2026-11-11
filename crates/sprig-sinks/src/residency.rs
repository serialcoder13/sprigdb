//! A tenant's residency policies for sink destinations (chapter 51,
//! "Destinations, allowlist and residency"; chapter 26), read from the
//! directory entries of the tenant and its ancestors. Workers check every
//! connection a task opens; gateways check at `CREATE` and `ALTER
//! CONNECTION` (R9, r9more).

use serde_json::Value;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_meta::MetaStore;

/// A tenant's residency for sink destinations (chapter 51, "Destinations,
/// allowlist and residency"; chapter 26): the policies of the tenant and
/// its ancestors from their directory entries (`residency.policy`), each
/// evaluated against a destination's tags as if they were a DC's. A
/// tenant with a residency block but no readable policy anywhere on its
/// path admits nothing (fail closed). No block on the path: unpinned.
pub struct PolicyResidency {
    policies: Vec<sprig_residency::Policy>,
    closed: bool,
}

impl std::fmt::Debug for PolicyResidency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyResidency")
            .field("policies", &self.policies.len())
            .field("closed", &self.closed)
            .finish()
    }
}

impl PolicyResidency {
    /// From policy texts (own and ancestors'); `closed` when a block
    /// exists without any policy to read.
    ///
    /// # Errors
    /// 39506 for a policy that doesn't parse.
    pub fn new(texts: &[String], closed: bool) -> Result<Self> {
        let mut policies = Vec::new();
        for t in texts {
            policies.push(sprig_residency::Policy::parse(t)?);
        }
        Ok(Self { policies, closed })
    }
}

impl crate::policy::Residency for PolicyResidency {
    fn pinned(&self) -> bool {
        true
    }

    fn admits(&self, tags: &serde_json::Map<String, Value>) -> bool {
        if self.closed && self.policies.is_empty() {
            return false;
        }
        let Ok(id) = sprig_common::ids::DcId::new("sink-destination") else {
            return false;
        };
        let dc = sprig_meta::bootstrap::DcRecord {
            dc_id: id,
            kind: "full".into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: tags.clone(),
            global_voter: false,
        };
        self.policies.iter().all(|p| p.admits(&dc))
    }
}

/// [`PolicyResidency`] of `tenant` from the global tier.
///
/// # Errors
/// The store's; 39506 for a bad policy.
pub async fn residency_of(
    global: &dyn MetaStore,
    tenant: TenantId,
) -> Result<Box<dyn crate::policy::Residency>> {
    let entry = |id: u64| async move {
        let key = global.tier().key(&format!("tenants/{id}"))?;
        Ok::<_, SprigError>(
            global
                .get(&key, sprig_meta::Consistency::Serializable)
                .await?
                .and_then(|kv| serde_json::from_slice::<Value>(&kv.value).ok()),
        )
    };
    let Some(me) = entry(tenant.0).await? else {
        return Ok(Box::new(crate::policy::Unpinned));
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
        return Ok(Box::new(crate::policy::Unpinned));
    }
    Ok(Box::new(PolicyResidency::new(&texts, true)?))
}

#[cfg(test)]
mod residency_tests {
    use super::*;
    use crate::policy::Residency as _;

    #[allow(clippy::needless_pass_by_value)]
    fn tags(v: Value) -> serde_json::Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn destinations_against_policies() {
        let r = PolicyResidency::new(&["jurisdiction = \"EU\"".to_owned()], true).unwrap();
        assert!(r.pinned());
        assert!(r.admits(&tags(
            serde_json::json!({"jurisdiction": "EU", "country": "DE"})
        )));
        assert!(!r.admits(&tags(serde_json::json!({"jurisdiction": "US"}))));
        assert!(!r.admits(&tags(serde_json::json!({}))));
        // An ancestor's policy narrows a member's.
        let r = PolicyResidency::new(
            &[
                "jurisdiction = \"EU\"".to_owned(),
                "country in (\"DE\", \"FR\")".to_owned(),
            ],
            true,
        )
        .unwrap();
        assert!(r.admits(&tags(
            serde_json::json!({"jurisdiction": "EU", "country": "FR"})
        )));
        assert!(!r.admits(&tags(
            serde_json::json!({"jurisdiction": "EU", "country": "NL"})
        )));
        // A block without any policy on the path admits nothing.
        let r = PolicyResidency::new(&[], true).unwrap();
        assert!(!r.admits(&tags(serde_json::json!({"jurisdiction": "EU"}))));
    }
}

/// A residency loaded once and handed out to each check (a gateway loads
/// the tenant's per statement).
#[derive(Clone)]
pub struct Shared(pub std::sync::Arc<dyn crate::policy::Residency>);

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("pinned", &self.0.pinned())
            .finish()
    }
}

impl crate::policy::Residency for Shared {
    fn pinned(&self) -> bool {
        self.0.pinned()
    }

    fn admits(&self, tags: &serde_json::Map<String, Value>) -> bool {
        self.0.admits(tags)
    }
}
