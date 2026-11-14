//! The whole-tenant map: `shardmaps/<tenant>/0`.
//!
//! While a tenant isn't sharded, its catalog and every database live on
//! one cluster, and the catalog's map (one range, `lo = 0`) is what
//! gateways route the whole tenant by. The mover changes it three ways, each a
//! compare-and-put on the key's `mod_revision`:
//!
//! - [`hold`]: every range `Held` by the job until `hold_until_ms`; gateways
//!   queue new statements and end open transactions;
//! - [`flip`]: the commit point of a cutover: the cluster becomes the
//!   target, the epoch rises by one, the range serves again and records the
//!   job id, so a successor worker can tell the cutover committed;
//! - [`release`]: an aborted cutover clears the hold without a change.
//!
//! Gateways never depend on epochs for safety: the mover fences the source
//! inside MariaDB before it flips the map.

use std::sync::Arc;

use sprig_catalog::placement::{RangeState, ShardMap};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, MetaStore, Op, Record, Revision, Txn};

/// A hold on a map, as gateways see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// The job holding the map.
    pub job: String,
    /// Gateways release queued statements by then (ms since the Unix epoch).
    pub until_ms: u64,
}

/// Where a tenant's map points, and whether it is held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The tenant's cluster.
    pub cluster: ClusterId,
    /// The range's epoch.
    pub epoch: u64,
    /// The map's version.
    pub map_version: u64,
    /// The hold, while held.
    pub hold: Option<Hold>,
    /// The job that last moved the range, once its cutover committed.
    pub moved_by: Option<String>,
}

impl Placement {
    /// The placement a map describes (its first range: whole-tenant maps
    /// have one).
    ///
    /// # Errors
    /// `Internal` for a map without ranges.
    pub fn of(map: &ShardMap) -> Result<Self> {
        let r = map
            .ranges
            .first()
            .ok_or_else(|| SprigError::internal("shard map without ranges"))?;
        let hold = match r.state {
            RangeState::Held => Some(Hold {
                job: r.job_id.clone().unwrap_or_default(),
                until_ms: r.hold_until_ms.unwrap_or(0),
            }),
            RangeState::Serving => None,
        };
        Ok(Self {
            cluster: r.cluster_id.clone(),
            epoch: r.epoch,
            map_version: map.map_version,
            moved_by: if hold.is_none() {
                r.job_id.clone()
            } else {
                None
            },
            hold,
        })
    }

    /// Whether a hold is in force at `now_ms`.
    #[must_use]
    pub fn held_at(&self, now_ms: u64) -> bool {
        self.hold.as_ref().is_some_and(|h| now_ms < h.until_ms)
    }
}

/// Checks the whole-tenant map's invariants: at least one range, the first at `lo = 0`,
/// starts strictly increasing, shard ids unique, a held range names its
/// job and deadline.
///
/// # Errors
/// `Internal` naming the broken rule.
pub fn validate(map: &ShardMap) -> Result<()> {
    let bad = |m: &str| Err(SprigError::internal(format!("invalid shard map: {m}")));
    let Some(first) = map.ranges.first() else {
        return bad("no ranges");
    };
    if first.lo != "0" {
        return bad("the first range doesn't start at 0");
    }
    let mut prev: Option<u128> = None;
    let mut shards = std::collections::BTreeSet::new();
    for r in &map.ranges {
        let Ok(lo) = r.lo.parse::<u128>() else {
            return bad("a range start isn't a number");
        };
        if prev.is_some_and(|p| lo <= p) {
            return bad("range starts don't increase");
        }
        prev = Some(lo);
        if !shards.insert(r.shard_id) {
            return bad("a shard id appears twice");
        }
        if r.state == RangeState::Held && (r.job_id.is_none() || r.hold_until_ms.is_none()) {
            return bad("a held range without its job or deadline");
        }
    }
    Ok(())
}

/// Holds every range for `job` until `until_ms`. Holding an already held
/// map fails unless the hold is `job`'s own (then the deadline moves).
///
/// # Errors
/// 39131 if another job holds it.
pub fn hold(map: &mut ShardMap, job: &str, until_ms: u64) -> Result<()> {
    for r in &map.ranges {
        if r.state == RangeState::Held && r.job_id.as_deref() != Some(job) {
            return Err(SprigError::new(
                SprigCode::ShardLocked,
                format!(
                    "tenant {} is held by job {}",
                    map.tenant_id,
                    r.job_id.as_deref().unwrap_or("?")
                ),
            ));
        }
    }
    for r in &mut map.ranges {
        r.state = RangeState::Held;
        r.job_id = Some(job.to_owned());
        r.hold_until_ms = Some(until_ms);
    }
    map.map_version += 1;
    Ok(())
}

/// Clears `job`'s hold, changing nothing else. Returns whether there was
/// one.
pub fn release(map: &mut ShardMap, job: &str) -> bool {
    let mut changed = false;
    for r in &mut map.ranges {
        if r.state == RangeState::Held && r.job_id.as_deref() == Some(job) {
            r.state = RangeState::Serving;
            r.hold_until_ms = None;
            r.job_id = None;
            changed = true;
        }
    }
    if changed {
        map.map_version += 1;
    }
    changed
}

/// The commit point: every range moves to `to` with its epoch raised by
/// one, serving, recording `job`. Returns the new epoch of the first range.
///
/// # Errors
/// 39135 if the map isn't held by `job` (the hold expired and something
/// else changed it, or another job holds it).
pub fn flip(map: &mut ShardMap, job: &str, to: &ClusterId) -> Result<u64> {
    if map
        .ranges
        .iter()
        .any(|r| r.state != RangeState::Held || r.job_id.as_deref() != Some(job))
    {
        return Err(SprigError::new(
            SprigCode::MoveCutoverAborted,
            format!("tenant {}'s map isn't held by job {job}", map.tenant_id),
        ));
    }
    for r in &mut map.ranges {
        r.cluster_id = to.clone();
        r.epoch += 1;
        r.state = RangeState::Serving;
        r.hold_until_ms = None;
        r.job_id = Some(job.to_owned());
    }
    map.map_version += 1;
    Ok(map.ranges.first().map_or(0, |r| r.epoch))
}

/// Reads and writes tenants' whole-tenant maps in one DC tier.
#[derive(Clone)]
pub struct TenantMaps {
    dc: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for TenantMaps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantMaps")
            .field("tier", self.dc.tier())
            .finish()
    }
}

impl TenantMaps {
    /// Maps in `dc`, the tenant's home DC tier.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// The store.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn MetaStore> {
        &self.dc
    }

    /// The tenant's map and its `mod_revision`, linearizably.
    ///
    /// # Errors
    /// 39072 if the tenant has no map here; the store's errors; `Internal`
    /// for a map that fails [`validate`].
    pub async fn load(&self, tenant: TenantId) -> Result<(ShardMap, Revision)> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant)?;
        let kv = self
            .dc
            .get(&key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::TenantNotReady,
                    format!("tenant {tenant} has no shard map in this data center"),
                )
            })?;
        let map = Record::<ShardMap>::decode(&kv.value)?.body;
        validate(&map)?;
        Ok((map, kv.mod_revision))
    }

    /// Writes `map` if the key is still at `expected`. Returns the new
    /// revision, or `None` if the key changed meanwhile.
    ///
    /// # Errors
    /// The store's errors; `Internal` for a map that fails [`validate`].
    pub async fn compare_and_put(
        &self,
        expected: Revision,
        map: &ShardMap,
    ) -> Result<Option<Revision>> {
        validate(map)?;
        let key = ShardMap::catalog_key(self.dc.tier(), map.tenant_id)?;
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), expected)],
                then: vec![Op::Put {
                    key,
                    value: Record::new(map.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(r.succeeded.then_some(r.revision))
    }

    /// Applies `f` to the tenant's map and writes it, retrying lost races a
    /// few times. `f` returns `Ok(false)` to write nothing.
    ///
    /// # Errors
    /// What `f` returns; the store's errors; 39071 if the races kept
    /// being lost.
    pub async fn update<F>(&self, tenant: TenantId, mut f: F) -> Result<ShardMap>
    where
        F: FnMut(&mut ShardMap) -> Result<bool> + Send,
    {
        for _ in 0..8 {
            let (mut map, rev) = self.load(tenant).await?;
            if !f(&mut map)? {
                return Ok(map);
            }
            if self.compare_and_put(rev, &map).await?.is_some() {
                return Ok(map);
            }
        }
        Err(SprigError::new(
            SprigCode::CatalogConflict,
            format!("tenant {tenant}'s shard map kept changing"),
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};

    use super::*;

    fn c(n: &str) -> ClusterId {
        ClusterId::new(n).unwrap()
    }

    #[test]
    fn hold_flip_release() {
        let mut m = ShardMap::catalog(TenantId(42), c("a-c1"), 1);
        validate(&m).unwrap();
        hold(&mut m, "j1", 5000).unwrap();
        validate(&m).unwrap();
        let p = Placement::of(&m).unwrap();
        assert!(p.held_at(4999) && !p.held_at(5000));
        assert!(hold(&mut m, "j2", 6000).is_err());
        assert!(flip(&mut m, "j2", &c("a-c2")).is_err());
        let v = m.map_version;
        assert_eq!(flip(&mut m, "j1", &c("a-c2")).unwrap(), 2);
        assert_eq!(m.map_version, v + 1);
        let p = Placement::of(&m).unwrap();
        assert_eq!(p.cluster, c("a-c2"));
        assert_eq!(p.moved_by.as_deref(), Some("j1"));
        assert!(p.hold.is_none());
        // A released hold changes nothing else.
        hold(&mut m, "j3", 9000).unwrap();
        assert!(release(&mut m, "j3"));
        assert!(!release(&mut m, "j3"));
        let p = Placement::of(&m).unwrap();
        assert_eq!((p.cluster, p.epoch), (c("a-c2"), 2));
    }

    #[test]
    fn validate_refuses_bad_maps() {
        let mut m = ShardMap::catalog(TenantId(1), c("a-c1"), 1);
        m.ranges[0].lo = "5".into();
        assert!(validate(&m).is_err());
        let mut m = ShardMap::catalog(TenantId(1), c("a-c1"), 1);
        m.ranges[0].state = RangeState::Held;
        assert!(validate(&m).is_err());
        let mut m = ShardMap::catalog(TenantId(1), c("a-c1"), 1);
        m.ranges.clear();
        assert!(validate(&m).is_err());
    }

    #[tokio::test]
    async fn compare_and_put_loses_races() {
        let tier = Tier::Dc(DcId::new("dc-a").unwrap());
        let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier.clone()));
        let maps = TenantMaps::new(s.clone());
        let m = ShardMap::catalog(TenantId(7), c("a-c1"), 1);
        let key = ShardMap::catalog_key(&tier, TenantId(7)).unwrap();
        s.txn(Txn {
            when: vec![Cmp::Missing(key.clone())],
            then: vec![Op::Put {
                key,
                value: Record::new(m).encode().unwrap(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
        let (mut a, rev) = maps.load(TenantId(7)).await.unwrap();
        hold(&mut a, "j", 10).unwrap();
        assert!(maps.compare_and_put(rev, &a).await.unwrap().is_some());
        assert!(maps.compare_and_put(rev, &a).await.unwrap().is_none());
        let m = maps
            .update(TenantId(7), |m| {
                flip(m, "j", &c("a-c3"))?;
                Ok(true)
            })
            .await
            .unwrap();
        assert_eq!(m.ranges[0].cluster_id, c("a-c3"));
        assert_eq!(maps.load(TenantId(7)).await.unwrap().0, m);
    }
}
