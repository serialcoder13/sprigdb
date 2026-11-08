//! Capacity, utilization and pressure (chapter 11, "Capacity, utilization
//! and pressure"; build step 3), over the in-memory fleet view.
//!
//! ```text
//! u[c][d]    = Σ load[s][d] for shards s on c  /  cap[c][d]
//! pressure   = max( threads_running_avg / cap.threads,
//!                   buffer_pool_miss_rate / 0.05,
//!                   galera_flow_control_ratio / 0.10,
//!                   replica_lag_s / cluster.max_replica_lag_seconds )
//! full[c]    = max( max_d u[c][d], pressure[c] )
//! ```
//!
//! The `schemas` dimension is the fuller of tables over
//! `placement.max_tables_per_cluster` and shard schemas over
//! `placement.max_shard_schemas_per_cluster` (build step 9). A capacity
//! dimension that is 0 (not declared) is left out of `full`.
//!
//! Times are whole seconds since the Unix epoch ([`Timestamp`]), so the
//! fleet simulator can run months of simulated time.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DcId, GroupId, ShardId, TenantId};

use crate::settings::{ClusterLimits, TenantKnobs};

/// Seconds since the Unix epoch.
pub type Timestamp = u64;

/// A load or capacity dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dim {
    /// Bytes on disk.
    Storage,
    /// Statement time the shards put on the cluster.
    Busy,
    /// Rows written per second.
    Writes,
    /// Backend connections.
    Connections,
    /// Tables and shard schemas against the caps.
    Schemas,
}

impl Dim {
    /// Every dimension.
    pub const ALL: [Dim; 5] = [
        Dim::Storage,
        Dim::Busy,
        Dim::Writes,
        Dim::Connections,
        Dim::Schemas,
    ];

    /// The metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Dim::Storage => "storage",
            Dim::Busy => "busy",
            Dim::Writes => "writes",
            Dim::Connections => "connections",
            Dim::Schemas => "schemas",
        }
    }
}

impl fmt::Display for Dim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What makes a cluster as full as it is: a dimension, or pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bottleneck {
    /// A load dimension.
    Dim(Dim),
    /// Server pressure (threads, buffer pool, flow control, lag).
    Pressure,
}

impl fmt::Display for Bottleneck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bottleneck::Dim(d) => d.fmt(f),
            Bottleneck::Pressure => f.write_str("pressure"),
        }
    }
}

/// A shard's (or a cluster's) load.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadVector {
    /// Bytes on disk.
    pub storage_bytes: f64,
    /// Statement time the shard puts on its cluster, ms per second.
    pub busy_ms_per_sec: f64,
    /// Rows written per second.
    pub write_rows_per_sec: f64,
    /// Backend connections.
    pub connections: f64,
    /// Physical shard schemas: one per logical database in the group.
    pub schemas: f64,
    /// Tables, side tables included.
    pub tables: f64,
}

impl LoadVector {
    /// The vector times `k`.
    #[must_use]
    pub fn scale(self, k: f64) -> Self {
        Self {
            storage_bytes: self.storage_bytes * k,
            busy_ms_per_sec: self.busy_ms_per_sec * k,
            write_rows_per_sec: self.write_rows_per_sec * k,
            connections: self.connections * k,
            schemas: self.schemas * k,
            tables: self.tables * k,
        }
    }

    /// Negative components clamped to 0 (float noise after subtraction).
    #[must_use]
    pub fn clamp0(self) -> Self {
        Self {
            storage_bytes: self.storage_bytes.max(0.0),
            busy_ms_per_sec: self.busy_ms_per_sec.max(0.0),
            write_rows_per_sec: self.write_rows_per_sec.max(0.0),
            connections: self.connections.max(0.0),
            schemas: self.schemas.max(0.0),
            tables: self.tables.max(0.0),
        }
    }
}

impl Add for LoadVector {
    type Output = Self;
    fn add(self, o: Self) -> Self {
        Self {
            storage_bytes: self.storage_bytes + o.storage_bytes,
            busy_ms_per_sec: self.busy_ms_per_sec + o.busy_ms_per_sec,
            write_rows_per_sec: self.write_rows_per_sec + o.write_rows_per_sec,
            connections: self.connections + o.connections,
            schemas: self.schemas + o.schemas,
            tables: self.tables + o.tables,
        }
    }
}

impl Sub for LoadVector {
    type Output = Self;
    fn sub(self, o: Self) -> Self {
        Self {
            storage_bytes: self.storage_bytes - o.storage_bytes,
            busy_ms_per_sec: self.busy_ms_per_sec - o.busy_ms_per_sec,
            write_rows_per_sec: self.write_rows_per_sec - o.write_rows_per_sec,
            connections: self.connections - o.connections,
            schemas: self.schemas - o.schemas,
            tables: self.tables - o.tables,
        }
    }
}

impl AddAssign for LoadVector {
    fn add_assign(&mut self, o: Self) {
        *self = *self + o;
    }
}

impl SubAssign for LoadVector {
    fn sub_assign(&mut self, o: Self) {
        *self = *self - o;
    }
}

/// A cluster's capacity vector; a 0 dimension is not declared and is left
/// out of `full`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Capacity {
    /// Bytes.
    pub storage_bytes: f64,
    /// Statement time, ms per second.
    pub busy_ms_per_sec: f64,
    /// Rows written per second.
    pub write_rows_per_sec: f64,
    /// Connections.
    pub connections: f64,
    /// `Threads_running` the cluster sustains (pressure).
    pub threads: f64,
}

/// What per-shard loads miss, from chapter 09's probes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Pressure {
    /// Mean `Threads_running`.
    pub threads_running_avg: f64,
    /// Buffer pool misses per read request.
    pub buffer_pool_miss_rate: f64,
    /// Share of time Galera flow control paused writes.
    pub flow_control_ratio: f64,
    /// Worst replica lag, seconds; counted only when write load caused it.
    pub replica_lag_s: f64,
    /// Whether the lag comes from write load.
    pub lag_from_writes: bool,
    /// `cluster.max_replica_lag_seconds`.
    pub max_replica_lag_s: f64,
}

impl Pressure {
    /// `pressure[c]`: 0 is idle, 1 is saturated.
    #[must_use]
    pub fn value(&self, cap_threads: f64) -> f64 {
        let mut p: f64 = 0.0;
        if cap_threads > 0.0 {
            p = p.max(self.threads_running_avg / cap_threads);
        }
        p = p.max(self.buffer_pool_miss_rate / 0.05);
        p = p.max(self.flow_control_ratio / 0.10);
        if self.lag_from_writes && self.max_replica_lag_s > 0.0 {
            p = p.max(self.replica_lag_s / self.max_replica_lag_s);
        }
        p
    }
}

/// Where a cluster stands for placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterState {
    /// `active` and `healthy`.
    Placeable,
    /// A writer but no redundancy: takes no new shards.
    Degraded,
    /// `draining`: shards move off, none move on.
    Draining,
    /// Anything else (probing, rejected, no writer, unknown, retired).
    Unavailable,
}

/// Capability flags the constraints read (chapter 09's `Derived`).
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterCaps {
    /// The mover can read from it.
    pub mover_source: bool,
    /// The mover can write to it.
    pub mover_target: bool,
    /// XA works.
    pub xa: bool,
    /// Failover loses nothing.
    pub zero_loss: bool,
    /// Feature flags set (`json_schema_valid`, `vector`, ...).
    pub features: BTreeSet<String>,
}

/// One cluster in the fleet view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterView {
    /// The cluster.
    pub id: ClusterId,
    /// Its DC.
    pub dc: DcId,
    /// Placement state.
    pub state: ClusterState,
    /// When it last returned from an outage or failover; `None` if never.
    pub recovered_at: Option<Timestamp>,
    /// Capacity.
    pub cap: Capacity,
    /// Probe pressure.
    pub pressure: Pressure,
    /// Load on it that belongs to no shard in the view (other DCs' maps,
    /// system schemas).
    pub background: LoadVector,
    /// DCs of its replicas, and the declared `replica_dcs` of external
    /// clusters (residency).
    pub replica_dcs: Vec<DcId>,
    /// Capability flags.
    pub caps: ClusterCaps,
    /// Chapter 09's tuning reports "over table budget".
    pub over_table_budget: bool,
    /// `sprig_cluster_table_cache_misses_total` keeps rising after tuning.
    pub cache_misses_rising: bool,
    /// Probe data is stale: neither source nor target.
    pub probe_stale: bool,
    /// The per-cluster keys.
    pub limits: ClusterLimits,
    /// When it last had a shard moved off.
    pub last_move_off: Option<Timestamp>,
}

impl ClusterView {
    /// A placeable cluster with `cap` and default limits, for tests and
    /// the simulator.
    #[must_use]
    pub fn new(id: ClusterId, dc: DcId, cap: Capacity) -> Self {
        Self {
            id,
            dc,
            state: ClusterState::Placeable,
            recovered_at: None,
            cap,
            pressure: Pressure::default(),
            background: LoadVector::default(),
            replica_dcs: Vec::new(),
            caps: ClusterCaps {
                mover_source: true,
                mover_target: true,
                xa: true,
                zero_loss: false,
                features: BTreeSet::new(),
            },
            over_table_budget: false,
            cache_misses_rising: false,
            probe_stale: false,
            limits: ClusterLimits::default(),
            last_move_off: None,
        }
    }

    /// Utilization of `load` on this cluster, per dimension; undeclared
    /// dimensions are absent.
    #[must_use]
    pub fn utilization(&self, load: &LoadVector) -> Vec<(Dim, f64)> {
        let mut u = Vec::with_capacity(5);
        let mut put = |d: Dim, v: f64, cap: f64| {
            if cap > 0.0 {
                u.push((d, v / cap));
            }
        };
        put(Dim::Storage, load.storage_bytes, self.cap.storage_bytes);
        put(Dim::Busy, load.busy_ms_per_sec, self.cap.busy_ms_per_sec);
        put(
            Dim::Writes,
            load.write_rows_per_sec,
            self.cap.write_rows_per_sec,
        );
        put(Dim::Connections, load.connections, self.cap.connections);
        #[allow(clippy::cast_precision_loss)]
        let schemas = (load.tables / self.limits.max_tables.max(1) as f64)
            .max(load.schemas / self.limits.max_shard_schemas_or_1());
        u.push((Dim::Schemas, schemas));
        u
    }

    /// `full` of this cluster carrying `load`, and what bounds it.
    #[must_use]
    pub fn full_with(&self, load: &LoadVector) -> Fullness {
        let mut best = Fullness {
            value: f64::NEG_INFINITY,
            bottleneck: Bottleneck::Dim(Dim::Schemas),
        };
        for (d, v) in self.utilization(load) {
            if v > best.value {
                best = Fullness {
                    value: v,
                    bottleneck: Bottleneck::Dim(d),
                };
            }
        }
        let p = self.pressure.value(self.cap.threads);
        if p > best.value {
            best = Fullness {
                value: p,
                bottleneck: Bottleneck::Pressure,
            };
        }
        best
    }

    /// Utilization of one dimension (0 when undeclared).
    #[must_use]
    pub fn util_of(&self, load: &LoadVector, d: Dim) -> f64 {
        self.utilization(load)
            .into_iter()
            .find(|(x, _)| *x == d)
            .map_or(0.0, |(_, v)| v)
    }
}

impl ClusterLimits {
    #[allow(clippy::cast_precision_loss)]
    fn max_shard_schemas_or_1(&self) -> f64 {
        self.max_schemas.max(1) as f64
    }
}

/// `full` with its bottleneck.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Fullness {
    /// 0 is empty, 1 is at capacity.
    pub value: f64,
    /// What makes it so.
    pub bottleneck: Bottleneck,
}

/// A shard, by tenant, table group and shard id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    /// The tenant.
    pub tenant: TenantId,
    /// The table group.
    pub group: GroupId,
    /// The shard.
    pub shard: ShardId,
}

impl ShardKey {
    /// A key.
    #[must_use]
    pub const fn new(tenant: u64, group: u32, shard: u32) -> Self {
        Self {
            tenant: TenantId(tenant),
            group: GroupId(group),
            shard: ShardId(shard),
        }
    }
}

impl fmt::Display for ShardKey {
    /// `t42/g1/s3`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{}/g{}/s{}", self.tenant, self.group, self.shard)
    }
}

/// A cluster a shard left, and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Visit {
    /// The cluster.
    pub cluster: ClusterId,
    /// When the shard left it.
    pub left_at: Timestamp,
}

/// One shard in the fleet view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardView {
    /// The shard.
    pub key: ShardKey,
    /// Where it is.
    pub cluster: ClusterId,
    /// Its EWMA load, storage, schemas and tables included.
    pub load: LoadVector,
    /// Pinned by an operator (`sprigctl shard move --pin`, `tenant pin`).
    pub pinned: bool,
    /// Locked by an active job (a move, an index build).
    pub locked: bool,
    /// No move before this time.
    pub cooldown_until: Timestamp,
    /// Its last clusters (at most 4), newest last.
    pub history: Vec<Visit>,
    /// Targets excluded until a time (a move there failed).
    pub excluded: Vec<(ClusterId, Timestamp)>,
}

impl ShardView {
    /// A shard on `cluster` with `load` and no history.
    #[must_use]
    pub fn new(key: ShardKey, cluster: ClusterId, load: LoadVector) -> Self {
        Self {
            key,
            cluster,
            load,
            pinned: false,
            locked: false,
            cooldown_until: 0,
            history: Vec::new(),
            excluded: Vec::new(),
        }
    }

    /// Size in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> f64 {
        self.load.storage_bytes
    }

    /// Whether `c` is excluded as a target at `now`.
    #[must_use]
    pub fn excluded_from(&self, c: &ClusterId, now: Timestamp) -> Option<Timestamp> {
        self.excluded
            .iter()
            .find(|(x, until)| x == c && *until > now)
            .map(|(_, u)| *u)
    }
}

/// Per-tenant policy the constraints read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantView {
    /// The tenant.
    pub id: TenantId,
    /// DCs its data may live in; `None` means anywhere (chapter 26).
    pub allowed_dcs: Option<BTreeSet<DcId>>,
    /// Features its objects use.
    pub needs: BTreeSet<String>,
    /// RPO 0 required.
    pub rpo_zero: bool,
    /// `txn.non_xa_policy = reject`.
    pub non_xa_reject: bool,
    /// Pinned as a whole (`sprigctl tenant pin`).
    pub pinned: bool,
    /// Its keys.
    pub knobs: TenantKnobs,
    /// Moves started in the last 24 hours.
    pub moves_last_day: u32,
}

impl TenantView {
    /// A tenant with no constraints and default keys.
    #[must_use]
    pub fn new(id: TenantId) -> Self {
        Self {
            id,
            allowed_dcs: None,
            needs: BTreeSet::new(),
            rpo_zero: false,
            non_xa_reject: false,
            pinned: false,
            knobs: TenantKnobs::default(),
            moves_last_day: 0,
        }
    }
}

/// Co-writing transactions between two groups of one tenant (chapter 12's
/// `txn` fingerprints).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CoWrite {
    /// The tenant.
    pub tenant: TenantId,
    /// One group.
    pub a: GroupId,
    /// The other.
    pub b: GroupId,
    /// Transactions writing both, per hour.
    pub per_hour: f64,
}

/// A move in flight: the unit is still on `from` and reserved on `to`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveMove {
    /// The shards moving together.
    pub shards: Vec<ShardKey>,
    /// Source.
    pub from: ClusterId,
    /// Target.
    pub to: ClusterId,
    /// Their load, reserved on the target.
    pub load: LoadVector,
}

/// The fleet of one DC, as the controller sees it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FleetView {
    /// The clusters.
    pub clusters: BTreeMap<ClusterId, ClusterView>,
    /// The shards.
    pub shards: BTreeMap<ShardKey, ShardView>,
    /// The tenants with shards here.
    pub tenants: BTreeMap<TenantId, TenantView>,
    /// Co-writes between groups.
    pub cowrites: Vec<CoWrite>,
    /// Moves in flight.
    pub active: Vec<ActiveMove>,
}

impl FleetView {
    /// Adds a cluster.
    pub fn add_cluster(&mut self, c: ClusterView) {
        self.clusters.insert(c.id.clone(), c);
    }

    /// Adds a shard, and a default tenant entry if the tenant is new.
    pub fn add_shard(&mut self, s: ShardView) {
        self.tenants
            .entry(s.key.tenant)
            .or_insert_with(|| TenantView::new(s.key.tenant));
        self.shards.insert(s.key, s);
    }

    /// Everything on `c`: background, its shards, and loads reserved by
    /// moves into it.
    #[must_use]
    pub fn load_on(&self, c: &ClusterId) -> LoadVector {
        let mut l = self
            .clusters
            .get(c)
            .map(|x| x.background)
            .unwrap_or_default();
        for s in self.shards.values().filter(|s| &s.cluster == c) {
            l += s.load;
        }
        for m in self.active.iter().filter(|m| &m.to == c) {
            l += m.load;
        }
        l
    }

    /// Every cluster's load, in one pass.
    #[must_use]
    pub fn loads(&self) -> BTreeMap<ClusterId, LoadVector> {
        let mut out: BTreeMap<ClusterId, LoadVector> = self
            .clusters
            .iter()
            .map(|(id, c)| (id.clone(), c.background))
            .collect();
        for s in self.shards.values() {
            if let Some(l) = out.get_mut(&s.cluster) {
                *l += s.load;
            }
        }
        for m in &self.active {
            if let Some(l) = out.get_mut(&m.to) {
                *l += m.load;
            }
        }
        out
    }

    /// `full` of every cluster.
    #[must_use]
    pub fn fullness(&self) -> BTreeMap<ClusterId, Fullness> {
        let loads = self.loads();
        self.clusters
            .iter()
            .map(|(id, c)| {
                let l = loads.get(id).copied().unwrap_or_default();
                (id.clone(), c.full_with(&l))
            })
            .collect()
    }

    /// `full` of `c`.
    #[must_use]
    pub fn full(&self, c: &ClusterId) -> Option<Fullness> {
        let cv = self.clusters.get(c)?;
        Some(cv.full_with(&self.load_on(c)))
    }

    /// Moves `shards` to `to` in the view (a planned move's effect, or a
    /// finished move).
    pub fn apply_move(&mut self, shards: &[ShardKey], to: &ClusterId) {
        for k in shards {
            if let Some(s) = self.shards.get_mut(k) {
                s.cluster = to.clone();
            }
        }
    }

    /// The fullest minus the emptiest `full` among clusters that can take
    /// or give shards (placeable, not stale).
    #[must_use]
    pub fn spread(&self) -> f64 {
        let f = self.fullness();
        let vals: Vec<f64> = self
            .clusters
            .values()
            .filter(|c| c.state == ClusterState::Placeable && !c.probe_stale)
            .filter_map(|c| f.get(&c.id).map(|x| x.value))
            .collect();
        let max = vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let min = vals.iter().copied().fold(f64::INFINITY, f64::min);
        if vals.is_empty() { 0.0 } else { max - min }
    }

    /// Moves in flight touching `c` (as source or target).
    #[must_use]
    pub fn active_on(&self, c: &ClusterId) -> usize {
        self.active
            .iter()
            .filter(|m| &m.from == c || &m.to == c)
            .count()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use super::*;

    fn cid(s: &str) -> ClusterId {
        ClusterId::new(s).unwrap()
    }

    fn dc() -> DcId {
        DcId::new("mum1").unwrap()
    }

    fn cap() -> Capacity {
        Capacity {
            storage_bytes: 1000.0,
            busy_ms_per_sec: 1000.0,
            write_rows_per_sec: 1000.0,
            connections: 100.0,
            threads: 32.0,
        }
    }

    #[test]
    fn full_is_the_fullest_dimension_or_pressure() {
        let c = ClusterView::new(cid("mum1-c1"), dc(), cap());
        let l = LoadVector {
            storage_bytes: 300.0,
            busy_ms_per_sec: 600.0,
            connections: 20.0,
            ..LoadVector::default()
        };
        let f = c.full_with(&l);
        assert_eq!(f.value, 0.6);
        assert_eq!(f.bottleneck, Bottleneck::Dim(Dim::Busy));

        let mut hot = c.clone();
        hot.pressure.buffer_pool_miss_rate = 0.04; // 0.8 of 0.05
        let f = hot.full_with(&l);
        assert!((f.value - 0.8).abs() < 1e-9);
        assert_eq!(f.bottleneck, Bottleneck::Pressure);

        // Lag counts only when write load caused it.
        let mut lag = c.clone();
        lag.pressure.replica_lag_s = 9.0;
        lag.pressure.max_replica_lag_s = 10.0;
        assert_eq!(lag.full_with(&l).value, 0.6);
        lag.pressure.lag_from_writes = true;
        assert_eq!(lag.full_with(&l).value, 0.9);

        // Threads over the declared thread capacity.
        let mut th = c;
        th.pressure.threads_running_avg = 24.0;
        assert_eq!(th.full_with(&l).value, 0.75);
    }

    #[test]
    fn schemas_dimension_is_the_fuller_cap() {
        let mut c = ClusterView::new(cid("mum1-c1"), dc(), Capacity::default());
        c.limits.max_tables = 250;
        c.limits.max_schemas = 50;
        let l = LoadVector {
            schemas: 40.0, // 0.8 of 50
            tables: 225.0, // 0.9 of 250
            ..LoadVector::default()
        };
        let f = c.full_with(&l);
        assert_eq!(f.value, 0.9);
        assert_eq!(f.bottleneck, Bottleneck::Dim(Dim::Schemas));
        // Undeclared capacity dimensions are left out.
        assert_eq!(c.utilization(&l).len(), 1);
    }

    #[test]
    fn view_loads_count_shards_background_and_reservations() {
        let mut v = FleetView::default();
        let (a, b) = (cid("mum1-c1"), cid("mum1-c2"));
        let mut ca = ClusterView::new(a.clone(), dc(), cap());
        ca.background.storage_bytes = 100.0;
        v.add_cluster(ca);
        v.add_cluster(ClusterView::new(b.clone(), dc(), cap()));
        let s1 = ShardKey::new(1, 1, 1);
        let load = LoadVector {
            storage_bytes: 200.0,
            ..LoadVector::default()
        };
        v.add_shard(ShardView::new(s1, a.clone(), load));
        assert_eq!(v.load_on(&a).storage_bytes, 300.0);
        assert_eq!(v.full(&a).unwrap().value, 0.3);
        assert!((v.spread() - 0.3).abs() < 1e-9);
        v.active.push(ActiveMove {
            shards: vec![s1],
            from: a.clone(),
            to: b.clone(),
            load,
        });
        assert_eq!(v.load_on(&b).storage_bytes, 200.0);
        assert_eq!(v.loads()[&b].storage_bytes, 200.0);
        assert_eq!(v.active_on(&a), 1);
        v.active.clear();
        v.apply_move(&[s1], &b);
        assert_eq!(v.load_on(&a).storage_bytes, 100.0);
        assert_eq!(v.load_on(&b).storage_bytes, 200.0);
        assert_eq!(s1.to_string(), "t1/g1/s1");
    }
}
