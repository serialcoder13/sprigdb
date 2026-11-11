//! A tenant's residency policies for sink destinations (chapter 51,
//! "Destinations, allowlist and residency"; chapter 26), read from the
//! directory entries of the tenant and its ancestors. Workers check every
//! connection a task opens; gateways check at `CREATE` and `ALTER
//! CONNECTION` (R9, r9more).

use serde_json::Value;
use sprig_common::Result;
use sprig_common::ids::TenantId;
use sprig_meta::MetaStore;

/// A tenant's residency for sink destinations (chapter 51, "Destinations,
/// allowlist and residency"; chapter 26): [`sprig_residency::destinations`]
/// behind this crate's [`crate::policy::Residency`] trait. A tenant with a
/// residency block but no readable policy anywhere on its path admits
/// nothing (fail closed). No block on the path: unpinned.
pub struct PolicyResidency(sprig_residency::destinations::TenantResidency);

impl std::fmt::Debug for PolicyResidency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyResidency")
            .field("policies", &self.0.len())
            .field("closed", &self.0.closed())
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
        Ok(Self(sprig_residency::destinations::TenantResidency::new(
            texts, closed,
        )?))
    }
}

impl crate::policy::Residency for PolicyResidency {
    fn pinned(&self) -> bool {
        true
    }

    fn admits(&self, tags: &serde_json::Map<String, Value>) -> bool {
        self.0.admits_tags(tags)
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
    Ok(
        match sprig_residency::destinations::of_tenant(global, tenant).await? {
            Some(r) => Box::new(PolicyResidency(r)),
            None => Box::new(crate::policy::Unpinned),
        },
    )
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
