//! Where a tenant's catalog lives (chapter 08, "Where the catalog lives").
//!
//! The catalog is a shard of reserved table group 0:
//! `/sprig/dc/<dc_id>/shardmaps/<tenant_id>/0` in the tenant's home DC
//! tier maps the whole range to `t<tenant>_sprig` on one cluster. The
//! record is chapter 15's shard map in chapter 23's JSON form: one range at
//! `lo = "0"` with `hi: null` (2^64), so every gateway uses one code path
//! for the catalog and for data groups.
//!
//! Tenant bootstrap (step 9) writes it once, guarded by `Missing`, and the
//! mover rewrites it when it moves the catalog (chapter 10).

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_meta::{Key, Tier, Versioned};

/// The catalog's reserved table group.
pub const CATALOG_GROUP: u32 = 0;

/// A shard map's role in its DC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MapRole {
    /// The tenant's home DC.
    Home,
    /// A standby DC (chapter 25).
    Standby,
    /// Kept after a failover until reconciled (chapter 25).
    Quarantined,
}

/// A range's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeState {
    /// Serving reads and writes.
    Serving,
    /// Held by a mover cutover.
    Held,
}

/// One range of a shard map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapRange {
    /// First hash value, a decimal string.
    pub lo: String,
    /// One past the last, a decimal string; `None` means 2^64.
    #[serde(default)]
    pub hi: Option<String>,
    /// The shard.
    pub shard_id: u32,
    /// Its cluster.
    pub cluster_id: ClusterId,
    /// Raised by every move, split and merge.
    pub epoch: u64,
    /// Serving or held.
    pub state: RangeState,
    /// The mover job holding the range (while `Held`) or that last moved
    /// it (after its cutover committed): the job id's canonical text
    /// (chapter 10, "Cutover").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// While `Held`: gateways release queued statements by then, in
    /// milliseconds since the Unix epoch, even if the hold isn't cleared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_until_ms: Option<u64>,
}

/// A shard map record (chapter 23, "DC tier"; chapter 15).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMap {
    /// The tenant.
    pub tenant_id: TenantId,
    /// The table group; 0 for the catalog.
    pub group_id: u32,
    /// Home, standby or quarantined.
    pub role: MapRole,
    /// Raised on every change.
    pub map_version: u64,
    /// The directory entry's `epoch` when written.
    #[serde(default)]
    pub tenant_epoch: u64,
    /// `xxh3_64`.
    #[serde(default)]
    pub hash: String,
    /// Digest of the group's key spec; 0 for the catalog.
    #[serde(default)]
    pub key_spec_digest: u64,
    /// The next shard id.
    #[serde(default)]
    pub next_shard_id: u32,
    /// Ranges sorted by `lo`; the first `lo` is 0.
    pub ranges: Vec<MapRange>,
}

impl Versioned for ShardMap {
    const V: u32 = 1;
}

impl ShardMap {
    /// The catalog's map: the whole range on `cluster`.
    #[must_use]
    pub fn catalog(tenant: TenantId, cluster: ClusterId, tenant_epoch: u64) -> Self {
        Self {
            tenant_id: tenant,
            group_id: CATALOG_GROUP,
            role: MapRole::Home,
            map_version: 1,
            tenant_epoch,
            hash: "xxh3_64".to_owned(),
            key_spec_digest: 0,
            next_shard_id: 1,
            ranges: vec![MapRange {
                lo: "0".to_owned(),
                hi: None,
                shard_id: 0,
                cluster_id: cluster,
                epoch: 1,
                state: RangeState::Serving,
                job_id: None,
                hold_until_ms: None,
            }],
        }
    }

    /// The cluster holding the catalog: the first range's.
    #[must_use]
    pub fn catalog_cluster(&self) -> Option<&ClusterId> {
        self.ranges.first().map(|r| &r.cluster_id)
    }

    /// The key of `tenant`'s catalog map in `tier` (the tenant's home DC
    /// tier): `shardmaps/<tenant>/0`.
    ///
    /// # Errors
    /// `Internal` never in practice (the path is always valid).
    pub fn catalog_key(tier: &Tier, tenant: TenantId) -> Result<Key> {
        tier.key(&format!("shardmaps/{tenant}/{CATALOG_GROUP}"))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::DcId;
    use sprig_meta::Record;

    use super::*;

    #[test]
    fn catalog_map_is_one_serving_range() {
        let c = ClusterId::new("mum1-c7").unwrap();
        let m = ShardMap::catalog(TenantId(42), c.clone(), 3);
        assert_eq!(m.catalog_cluster(), Some(&c));
        let bytes = Record::new(m.clone()).encode().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["v"], 1);
        assert_eq!(json["group_id"], 0);
        assert_eq!(json["role"], "home");
        assert_eq!(json["ranges"][0]["lo"], "0");
        assert!(json["ranges"][0]["hi"].is_null());
        assert_eq!(json["ranges"][0]["state"], "serving");
        assert_eq!(Record::<ShardMap>::decode(&bytes).unwrap().body, m);
        let tier = Tier::Dc(DcId::new("mum1").unwrap());
        assert_eq!(
            ShardMap::catalog_key(&tier, TenantId(42)).unwrap().as_str(),
            "/sprig/dc/mum1/shardmaps/42/0"
        );
    }
}
