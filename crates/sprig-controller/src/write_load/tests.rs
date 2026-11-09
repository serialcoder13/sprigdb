#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::many_single_char_names)]

use std::time::SystemTime;

use sprig_balancer::load::{LoadAccumulator, report_key};
use sprig_balancer::shards::sketch::minute_of;
use sprig_balancer::shards::{
    self, GroupStat, Reason, ShardStat, ShardingSettings, Weight, choose_split,
};
use sprig_common::ids::DcId;
use sprig_common::time::ManualClock;
use sprig_meta::{MemStore, Op, Tier, Txn};

use super::*;

struct Hot(BTreeSet<ClusterId>);

#[async_trait::async_trait]
impl HotClusters for Hot {
    async fn above_high(&self) -> Option<BTreeSet<ClusterId>> {
        Some(self.0.clone())
    }
}

fn cid(s: &str) -> ClusterId {
    ClusterId::new(s).unwrap()
}

fn wall_ms(c: &ManualClock) -> u64 {
    u64::try_from(
        c.wall()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn advance_to(c: &ManualClock, ms: u64) {
    let now = wall_ms(c);
    assert!(ms >= now);
    c.advance(Duration::from_millis(ms - now));
}

const HOT_KEY: u64 = 0x313c_a5e2_98b7_eb08;
const HALF: u64 = 1 << 63;

/// One gateway's writes in minute `m`: 300 on shard 1 (three quarters on
/// one key), 80 on shard 2, 20 unsharded, all on cluster c1.
fn write_minute(acc: &LoadAccumulator, m: u64, seed: &mut u64) {
    let c = cid("mum1-c1");
    let r1 = HashRange::new(0, Some(HALF));
    let r2 = HashRange::new(HALF, None);
    let at = m * 60_000 + 1_000;
    let mut next = || {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    };
    for i in 0..300 {
        let h = if i % 4 == 0 { next() >> 1 } else { HOT_KEY };
        acc.sketches()
            .record(at, [(ShardKey::new(42, 3, 1), &c, Some((r1, h)))]);
    }
    for _ in 0..80 {
        let h = HALF | next();
        acc.sketches()
            .record(at, [(ShardKey::new(42, 3, 2), &c, Some((r2, h)))]);
    }
    acc.sketches().record_cluster(at, &c, 20);
}

async fn put(dc: &Arc<dyn MetaStore>, r: &LoadReport) {
    let key = dc.tier().key(&report_key(&r.gateway)).unwrap();
    dc.txn(Txn {
        when: vec![],
        then: vec![Op::Put {
            key,
            value: Record::new(r.clone()).encode().unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    })
    .await
    .unwrap();
}

fn stat(w: &GatewayWriteLoad, shard: u32, lo: u64, hi: Option<u64>) -> ShardStat {
    let c = cid("mum1-c1");
    ShardStat {
        shard,
        lo,
        hi,
        cluster: c.clone(),
        size_bytes: 1e6,
        write_shares: w.write_shares(TenantId(42), GroupId(3), ShardId(shard), None),
        cluster_hot: w.cluster_hot(&c),
        born_ms: None,
        busy: false,
    }
}

#[tokio::test]
async fn gateway_reports_in_etcd_drive_a_hot_key_split() {
    let clock = Arc::new(ManualClock::new());
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("mum1").unwrap())));
    let mut w = GatewayWriteLoad::new(Arc::clone(&dc), Arc::clone(&clock) as Arc<dyn Clock>);
    w.refresh_every = Duration::ZERO;
    w.set_placement(Arc::new(Hot([cid("mum1-c1")].into())));
    let gws = [LoadAccumulator::new(), LoadAccumulator::new()];
    let first = minute_of(wall_ms(&clock)) + 1;
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for m in first..first + 35 {
        for g in &gws {
            write_minute(g, m, &mut seed);
        }
        // Each gateway writes its report 5 s after the minute ends.
        let at = (m + 1) * 60_000 + 5_000;
        advance_to(&clock, at);
        for (i, g) in gws.iter().enumerate() {
            put(&dc, &g.report(&format!("gw-{i}"), at)).await;
        }
        w.refresh().await.unwrap();
    }
    advance_to(&clock, (first + 35) * 60_000 + 50_000);
    w.refresh().await.unwrap();
    let s = ShardingSettings::default();
    let g = GroupStat {
        tenant: TenantId(42),
        group: 3,
        min_shards: None,
        shards: vec![stat(&w, 1, 0, Some(HALF)), stat(&w, 2, HALF, None)],
    };
    assert_eq!(g.shards[0].write_shares.len(), 35);
    assert!(
        g.shards[0]
            .write_shares
            .iter()
            .all(|x| (x - 0.75).abs() < 1e-9),
        "{:?}",
        g.shards[0].write_shares
    );
    assert!(g.shards[0].cluster_hot);
    let p = shards::evaluate(&g, &s, wall_ms(&clock));
    assert_eq!(p.len(), 1, "{p:?}");
    assert_eq!((p[0].reason, p[0].job.sources()), (Reason::Load, vec![1]));
    // The loop's next step: points from the write histogram.
    let h = w
        .write_histogram(
            TenantId(42),
            GroupId(3),
            ShardId(1),
            0,
            Some(HALF),
            s.sustain,
        )
        .unwrap();
    assert_eq!(h.total(Weight::Writes), 2.0 * 30.0 * 300.0);
    let plan = choose_split(&h, Weight::Writes, 2).unwrap();
    assert_eq!(plan.hot_key, Some(HOT_KEY));
    assert_eq!(plan.points, vec![HOT_KEY, HOT_KEY + 1]);
    // Another cluster isn't hot; a shard nobody wrote has no shares.
    assert!(!w.cluster_hot(&cid("mum1-c2")));
    assert!(
        w.write_shares(TenantId(42), GroupId(3), ShardId(9), None)
            .is_empty()
    );
    // After a cutover in minute 25, minutes 26 to 34 count (34 settled
    // 50 s after it ended).
    let since = (first + 25) * 60_000 + 10_000;
    assert_eq!(
        w.write_shares(TenantId(42), GroupId(3), ShardId(1), Some(since))
            .len(),
        9
    );
}

#[tokio::test]
async fn refreshes_are_rate_limited_and_need_placement_for_hot_clusters() {
    let clock = Arc::new(ManualClock::new());
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("mum1").unwrap())));
    let w = GatewayWriteLoad::new(Arc::clone(&dc), Arc::clone(&clock) as Arc<dyn Clock>);
    let acc = LoadAccumulator::new();
    let m = minute_of(wall_ms(&clock)) + 1;
    let mut seed = 7;
    write_minute(&acc, m, &mut seed);
    advance_to(&clock, (m + 1) * 60_000 + 50_000);
    w.refresh().await.unwrap(); // nothing written yet
    put(&dc, &acc.report("gw-0", wall_ms(&clock))).await;
    w.refresh().await.unwrap(); // within 30 s: not read
    assert!(
        w.write_shares(TenantId(42), GroupId(3), ShardId(1), None)
            .is_empty()
    );
    clock.advance(Duration::from_secs(31));
    w.refresh().await.unwrap();
    assert_eq!(
        w.write_shares(TenantId(42), GroupId(3), ShardId(1), None),
        vec![0.75]
    );
    // No placement: nothing is hot, so no load split.
    assert!(!w.cluster_hot(&cid("mum1-c1")));
}
