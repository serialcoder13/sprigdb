//! Data-group shard maps and the routing lookup (chapter 15, "Shard maps
//! in etcd", "Hashing and routing", "Pre-splitting").
//!
//! The record in etcd is `sprig_catalog::placement::ShardMap` (chapter
//! 23's JSON form, range starts as decimal strings) at
//! `/sprig/dc/<dc>/shardmaps/<tenant>/<group>`. [`ShardMap`] is its parsed
//! form: starts as `u64`, so [`ShardMap::route`] is a binary search.
//! [`ShardMapStore`] reads and writes maps with compare-and-put on the
//! key's `mod_revision`; [`ShardMapCache`] keeps a watched copy of every
//! map of a DC tier, parsed, so routing takes no lock and no round trip.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arc_swap::ArcSwap;
pub use sprig_catalog::placement::RangeState;
use sprig_catalog::placement::{self, MapRange, MapRole};
use sprig_common::ids::{ClusterId, GroupId, ShardId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Mirror, Op, Record, Revision, Txn};
use tokio::sync::watch;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::hash::routing_hash;
use crate::key::{KeySpec, KeyValue, RoutingHash};
use crate::normalize::{Normalizer, StdNormalizer};

/// One range `[lo, next lo)` of a map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeEntry {
    /// First hash of the range.
    pub lo: u64,
    /// The shard.
    pub shard: ShardId,
    /// Its cluster.
    pub cluster: ClusterId,
    /// Raised by every split, merge and move.
    pub epoch: u64,
    /// Serving or held by the mover.
    pub state: RangeState,
    /// The mover job holding the range, or that last moved it.
    pub job_id: Option<String>,
    /// While held: gateways release queued work by then (ms since epoch).
    pub hold_until_ms: Option<u64>,
}

/// A group's shard map, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    /// The tenant.
    pub tenant: TenantId,
    /// The group.
    pub group: GroupId,
    /// `map_version`: raised on every change.
    pub version: u64,
    /// The directory entry's epoch when written.
    pub tenant_epoch: u64,
    /// The group's `KeySpec` digest (0 for keyless groups).
    pub key_spec_digest: u64,
    /// The next shard id the group hands out.
    pub next_shard_id: u32,
    /// Ranges sorted by `lo`; the first `lo` is 0.
    pub ranges: Vec<RangeEntry>,
}

/// A routed key: its hash, the range it falls in, and whether a value was
/// off-class.
#[derive(Debug, Clone, Copy)]
pub struct Routed<'m> {
    /// The routing hash (what `_sprig_rk` holds).
    pub hash: RoutingHash,
    /// The range, with its shard and cluster.
    pub range: &'m RangeEntry,
    /// A value was hashed under its own tag.
    pub off_class: bool,
}

fn invalid(m: &str) -> SprigError {
    SprigError::internal(format!("invalid shard map: {m}"))
}

impl ShardMap {
    /// The range holding `h`.
    #[must_use]
    pub fn route(&self, h: RoutingHash) -> &RangeEntry {
        &self.ranges[self.range_of(h)]
    }

    /// The index of the range holding `h`.
    #[must_use]
    pub fn range_of(&self, h: RoutingHash) -> usize {
        self.ranges
            .partition_point(|r| r.lo <= h.0)
            .saturating_sub(1)
    }

    /// One past the last hash of range `i`; `None` is 2^64.
    #[must_use]
    pub fn hi(&self, i: usize) -> Option<u64> {
        self.ranges.get(i + 1).map(|r| r.lo)
    }

    /// The range of shard `s`.
    #[must_use]
    pub fn shard(&self, s: ShardId) -> Option<&RangeEntry> {
        self.ranges.iter().find(|r| r.shard == s)
    }

    /// Checks the invariants: at least one range, the first at 0, starts
    /// strictly increasing, shard ids unique and below `next_shard_id`, a
    /// held range names its job and deadline.
    ///
    /// # Errors
    /// `Internal` naming the broken rule.
    pub fn validate(&self) -> Result<()> {
        let Some(first) = self.ranges.first() else {
            return Err(invalid("no ranges"));
        };
        if first.lo != 0 {
            return Err(invalid("the first range doesn't start at 0"));
        }
        let mut shards = std::collections::BTreeSet::new();
        for (i, r) in self.ranges.iter().enumerate() {
            if i > 0 && r.lo <= self.ranges[i - 1].lo {
                return Err(invalid("range starts don't increase"));
            }
            if !shards.insert(r.shard) {
                return Err(invalid("a shard id appears twice"));
            }
            if r.shard.0 >= self.next_shard_id && self.group.0 != 0 {
                return Err(invalid("a shard id at or above next_shard_id"));
            }
            if r.state == RangeState::Held && (r.job_id.is_none() || r.hold_until_ms.is_none()) {
                return Err(invalid("a held range without its job or deadline"));
            }
        }
        Ok(())
    }

    /// A new group's map with `n` ranges of equal width,
    /// `lo[i] = floor(i · 2^64 / n)`, range `i` on `clusters[i mod len]`,
    /// shards `1..=n`, epochs 1 (chapter 15, "Pre-splitting").
    ///
    /// # Errors
    /// `Internal` for `n = 0` or no clusters.
    pub fn presplit(
        tenant: TenantId,
        group: GroupId,
        key_spec_digest: u64,
        n: u32,
        clusters: &[ClusterId],
    ) -> Result<Self> {
        if n == 0 || clusters.is_empty() {
            return Err(SprigError::internal("presplit needs n > 0 and a cluster"));
        }
        let ranges = (0..n)
            .map(|i| {
                let lo = (u128::from(i) << 64) / u128::from(n);
                RangeEntry {
                    lo: u64::try_from(lo).unwrap_or(u64::MAX),
                    shard: ShardId(i + 1),
                    cluster: clusters[i as usize % clusters.len()].clone(),
                    epoch: 1,
                    state: RangeState::Serving,
                    job_id: None,
                    hold_until_ms: None,
                }
            })
            .collect();
        let m = Self {
            tenant,
            group,
            version: 1,
            tenant_epoch: 0,
            key_spec_digest,
            next_shard_id: n + 1,
            ranges,
        };
        m.validate()?;
        Ok(m)
    }

    /// Parses the etcd record.
    ///
    /// # Errors
    /// `Internal` for a start that isn't a `u64`, or a map that fails
    /// [`validate`](Self::validate).
    pub fn from_record(r: &placement::ShardMap) -> Result<Self> {
        let ranges = r
            .ranges
            .iter()
            .map(|x| {
                Ok(RangeEntry {
                    lo: x
                        .lo
                        .parse::<u64>()
                        .map_err(|_| invalid("a range start isn't a u64"))?,
                    shard: ShardId(x.shard_id),
                    cluster: x.cluster_id.clone(),
                    epoch: x.epoch,
                    state: x.state,
                    job_id: x.job_id.clone(),
                    hold_until_ms: x.hold_until_ms,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let m = Self {
            tenant: r.tenant_id,
            group: GroupId(r.group_id),
            version: r.map_version,
            tenant_epoch: r.tenant_epoch,
            key_spec_digest: r.key_spec_digest,
            next_shard_id: r.next_shard_id,
            ranges,
        };
        m.validate()?;
        Ok(m)
    }

    /// The etcd record (role `home`, hash `xxh3_64`).
    #[must_use]
    pub fn to_record(&self) -> placement::ShardMap {
        placement::ShardMap {
            tenant_id: self.tenant,
            group_id: self.group.0,
            role: MapRole::Home,
            map_version: self.version,
            tenant_epoch: self.tenant_epoch,
            hash: "xxh3_64".to_owned(),
            key_spec_digest: self.key_spec_digest,
            next_shard_id: self.next_shard_id,
            ranges: self
                .ranges
                .iter()
                .enumerate()
                .map(|(i, r)| MapRange {
                    lo: r.lo.to_string(),
                    hi: self.hi(i).map(|h| h.to_string()),
                    shard_id: r.shard.0,
                    cluster_id: r.cluster.clone(),
                    epoch: r.epoch,
                    state: r.state,
                    job_id: r.job_id.clone(),
                    hold_until_ms: r.hold_until_ms,
                })
                .collect(),
        }
    }
}

/// Routes one key: normalize, hash, binary search (chapter 15, `route`).
///
/// # Errors
/// What normalization returns. A map whose digest isn't `spec`'s is
/// `Internal` (the caller holds a stale spec or map).
pub fn route_key<'m>(m: &'m ShardMap, spec: &KeySpec, v: &[KeyValue<'_>]) -> Result<Routed<'m>> {
    if m.key_spec_digest != spec.digest {
        return Err(SprigError::internal(format!(
            "group {} of tenant {}: map key digest {:#x} isn't the spec's {:#x}",
            m.group, m.tenant, m.key_spec_digest, spec.digest
        )));
    }
    let mut buf = Vec::with_capacity(32);
    let outcome = StdNormalizer.normalize(spec, v, &mut buf)?;
    let hash = routing_hash(&buf);
    Ok(Routed {
        hash,
        range: m.route(hash),
        off_class: outcome.off_class,
    })
}

/// Routes each row of an `IN` list and groups row indexes by shard.
///
/// # Errors
/// As [`route_key`].
pub fn route_in_list(
    m: &ShardMap,
    spec: &KeySpec,
    rows: &[Vec<KeyValue<'_>>],
) -> Result<BTreeMap<ShardId, Vec<usize>>> {
    let mut out: BTreeMap<ShardId, Vec<usize>> = BTreeMap::new();
    for (i, row) in rows.iter().enumerate() {
        let r = route_key(m, spec, row)?;
        out.entry(r.range.shard).or_default().push(i);
    }
    Ok(out)
}

/// The key of a group's map in `tier`: `shardmaps/<tenant>/<group>`.
///
/// # Errors
/// `Internal` never in practice.
pub fn map_key(tier: &sprig_meta::Tier, t: TenantId, g: GroupId) -> Result<Key> {
    tier.key(&format!("shardmaps/{t}/{g}"))
}

/// Reads and writes data-group maps in one DC tier (the tenant's home).
#[derive(Clone)]
pub struct ShardMapStore {
    dc: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for ShardMapStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardMapStore")
            .field("tier", self.dc.tier())
            .finish()
    }
}

impl ShardMapStore {
    /// Maps in `dc`.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// A group's map and its `mod_revision`, linearizably.
    ///
    /// # Errors
    /// 39403 if the group has no map; the store's errors; `Internal` for a
    /// map that fails validation.
    pub async fn load(&self, t: TenantId, g: GroupId) -> Result<(ShardMap, Revision)> {
        let key = map_key(self.dc.tier(), t, g)?;
        let kv = self
            .dc
            .get(&key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| unavailable(t, g))?;
        let rec = Record::<placement::ShardMap>::decode(&kv.value)?.body;
        Ok((ShardMap::from_record(&rec)?, kv.mod_revision))
    }

    /// Writes a new group's map; fails if one exists.
    ///
    /// # Errors
    /// 39071 if the key exists; the store's errors.
    pub async fn create(&self, m: &ShardMap) -> Result<Revision> {
        m.validate()?;
        let key = map_key(self.dc.tier(), m.tenant, m.group)?;
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value: Record::new(m.to_record()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            Ok(r.revision)
        } else {
            Err(SprigError::new(
                SprigCode::CatalogConflict,
                format!("group {} of tenant {} already has a map", m.group, m.tenant),
            ))
        }
    }

    /// Writes `m` if the key is still at `expected`; `None` if it changed.
    ///
    /// # Errors
    /// The store's errors; `Internal` for a map that fails validation.
    pub async fn compare_and_put(
        &self,
        expected: Revision,
        m: &ShardMap,
    ) -> Result<Option<Revision>> {
        m.validate()?;
        let key = map_key(self.dc.tier(), m.tenant, m.group)?;
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), expected)],
                then: vec![Op::Put {
                    key,
                    value: Record::new(m.to_record()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(r.succeeded.then_some(r.revision))
    }

    /// Deletes a group's map (dropping a group, tests).
    ///
    /// # Errors
    /// The store's errors.
    pub async fn delete(&self, t: TenantId, g: GroupId) -> Result<()> {
        let key = map_key(self.dc.tier(), t, g)?;
        self.dc
            .txn(Txn {
                when: vec![],
                then: vec![Op::Delete(key)],
                otherwise: vec![],
            })
            .await?;
        Ok(())
    }

    /// Every map of tenant `t` (the catalog's group 0 included).
    ///
    /// # Errors
    /// The store's errors; `Internal` for an invalid map.
    pub async fn list(&self, t: TenantId) -> Result<Vec<ShardMap>> {
        let prefix = self.dc.tier().key(&format!("shardmaps/{t}/"))?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                let rec = Record::<placement::ShardMap>::decode(&kv.value)?.body;
                out.push(ShardMap::from_record(&rec)?);
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        Ok(out)
    }
}

fn unavailable(t: TenantId, g: GroupId) -> SprigError {
    SprigError::new(
        SprigCode::ShardMapUnavailable,
        format!("no shard map for group {g} of tenant {t}"),
    )
}

type Parsed = HashMap<(TenantId, GroupId), (Revision, Arc<ShardMap>)>;

/// A watched, parsed copy of every shard map of one DC tier. Routing reads
/// an `ArcSwap` snapshot: no lock, no round trip. Cloning shares it.
#[derive(Clone)]
pub struct ShardMapCache {
    mirror: Mirror,
    maps: Arc<ArcSwap<Parsed>>,
    changes: watch::Receiver<u64>,
    _stop: Arc<DropGuard>,
}

impl std::fmt::Debug for ShardMapCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardMapCache")
            .field("maps", &self.maps.load().len())
            .finish_non_exhaustive()
    }
}

impl ShardMapCache {
    /// Mirrors `shardmaps/` of `dc` and keeps the parsed maps current. An
    /// invalid map is skipped (the previous one is kept) and logged.
    ///
    /// # Errors
    /// `Internal` never in practice (the prefix is valid).
    pub fn spawn(dc: Arc<dyn MetaStore>) -> Result<Self> {
        let prefix = dc.tier().key("shardmaps/")?;
        let mirror = Mirror::spawn(dc, prefix);
        let maps: Arc<ArcSwap<Parsed>> = Arc::new(ArcSwap::from_pointee(HashMap::new()));
        let (tx, changes) = watch::channel(0u64);
        let mut rx = mirror.changes();
        let m2 = mirror.clone();
        let maps2 = Arc::clone(&maps);
        let stop = CancellationToken::new();
        let guard = Arc::new(stop.clone().drop_guard());
        tokio::spawn(async move {
            loop {
                let gen_now = *rx.borrow_and_update();
                let next = reparse(&maps2.load(), &m2);
                maps2.store(Arc::new(next));
                let _ = tx.send(gen_now);
                tokio::select! {
                    () = stop.cancelled() => break,
                    r = rx.changed() => if r.is_err() { break },
                }
            }
        });
        Ok(Self {
            mirror,
            maps,
            changes,
            _stop: guard,
        })
    }

    /// Waits for the first listing.
    pub async fn ready(&self) {
        self.mirror.ready().await;
        let mut rx = self.changes.clone();
        let _ = rx.wait_for(|g| *g > 0).await;
    }

    /// The current map of group `g` of tenant `t`.
    ///
    /// # Errors
    /// 39403 `ER_SPRIG_SHARD_MAP_UNAVAILABLE` if none is cached.
    pub fn get(&self, t: TenantId, g: GroupId) -> Result<Arc<ShardMap>> {
        self.maps
            .load()
            .get(&(t, g))
            .map(|(_, m)| Arc::clone(m))
            .ok_or_else(|| unavailable(t, g))
    }

    /// Bumped after every applied change.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.changes.clone()
    }

    /// Waits until the cached map of `(t, g)` has at least `version`.
    pub async fn wait_version(&self, t: TenantId, g: GroupId, version: u64) {
        let mut rx = self.changes.clone();
        loop {
            if self.get(t, g).is_ok_and(|m| m.version >= version) {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Re-parses the entries whose revision changed; keeps the old map when a
/// new one is invalid.
fn reparse(old: &Parsed, mirror: &Mirror) -> Parsed {
    let mut next = Parsed::new();
    for kv in mirror.entries() {
        let Some((t, g)) = parse_key(kv.key.as_str()) else {
            continue;
        };
        if let Some((rev, m)) = old.get(&(t, g))
            && *rev == kv.mod_revision
        {
            next.insert((t, g), (*rev, Arc::clone(m)));
            continue;
        }
        match Record::<placement::ShardMap>::decode(&kv.value)
            .and_then(|r| ShardMap::from_record(&r.body))
        {
            Ok(m) => {
                next.insert((t, g), (kv.mod_revision, Arc::new(m)));
            }
            Err(e) => {
                tracing::error!(key = kv.key.as_str(), error = %e, "invalid shard map; keeping the previous one");
                if let Some(prev) = old.get(&(t, g)) {
                    next.insert((t, g), prev.clone());
                }
            }
        }
    }
    next
}

/// `.../shardmaps/<tenant>/<group>` → ids.
fn parse_key(key: &str) -> Option<(TenantId, GroupId)> {
    let mut it = key.rsplit('/');
    let g = it.next()?.parse().ok()?;
    let t = it.next()?.parse().ok()?;
    (it.next()? == "shardmaps").then_some((t, g))
}

#[cfg(test)]
mod tests;
