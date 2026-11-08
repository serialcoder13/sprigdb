//! Table-driven fleets for the unit tests.

#![allow(clippy::unwrap_used, dead_code)]

use sprig_common::ids::{ClusterId, DcId};

use crate::model::{Capacity, ClusterView, FleetView, LoadVector, ShardKey, ShardView};
use crate::settings::GB;

pub(crate) fn cid(n: u32) -> ClusterId {
    ClusterId::new(format!("mum1-c{n}")).unwrap()
}

pub(crate) fn dc(s: &str) -> DcId {
    DcId::new(s).unwrap()
}

/// 100 GB, 1000 ms/s busy, 1000 writes/s, 100 connections, 32 threads.
pub(crate) fn cap() -> Capacity {
    Capacity {
        storage_bytes: 100.0 * GB,
        busy_ms_per_sec: 1000.0,
        write_rows_per_sec: 1000.0,
        connections: 100.0,
        threads: 32.0,
    }
}

/// A shard's load: `gb` of storage, `busy` ms/s, 1 schema of 5 tables.
pub(crate) fn load(gb: f64, busy: f64) -> LoadVector {
    LoadVector {
        storage_bytes: gb * GB,
        busy_ms_per_sec: busy,
        write_rows_per_sec: 0.0,
        connections: 0.0,
        schemas: 1.0,
        tables: 5.0,
    }
}

/// `n` placeable clusters `mum1-c1` ... in DC `mum1`.
pub(crate) fn fleet(n: u32) -> FleetView {
    let mut v = FleetView::default();
    for i in 1..=n {
        v.add_cluster(ClusterView::new(cid(i), dc("mum1"), cap()));
    }
    v
}

/// Adds shard `t<tenant>/g1/s<shard>` on cluster `c` with `l`.
pub(crate) fn put(v: &mut FleetView, tenant: u64, shard: u32, c: u32, l: LoadVector) -> ShardKey {
    let k = ShardKey::new(tenant, 1, shard);
    v.add_shard(ShardView::new(k, cid(c), l));
    k
}
