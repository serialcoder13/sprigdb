#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::cast_precision_loss)]

use std::collections::HashMap;

use super::*;
use crate::shards::settings::ShardingSettings;
use crate::shards::split::{Weight, choose_split};
use crate::shards::triggers::{GroupStat, Reason, ShardStat, evaluate};
use sprig_common::ids::TenantId;

fn cid(s: &str) -> ClusterId {
    ClusterId::new(s).unwrap()
}

/// xorshift64*: deterministic streams without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// A skewed pick from `0..n`: small values far more often.
    fn skewed(&mut self, n: u64) -> u64 {
        let a = self.next() % n;
        let b = self.next() % (a + 1);
        self.next() % (b + 1)
    }
}

#[test]
fn buckets_cover_the_range_exactly_at_its_edges() {
    // The whole hash space: 64 buckets of 2^58, the last ending at 2^64.
    let all = HashRange::new(0, None);
    assert_eq!(all.buckets(), 64);
    assert_eq!(all.bucket_of(0), Some(0));
    assert_eq!(all.bucket_of((1 << 58) - 1), Some(0));
    assert_eq!(all.bucket_of(1 << 58), Some(1));
    assert_eq!(all.bucket_of(u64::MAX), Some(63));
    assert_eq!(all.bucket_lo(1), 1 << 58);
    assert_eq!(all.bucket_lo(63), 63 << 58);
    // The upper half, hi = 2^64.
    let top = HashRange::new(1 << 63, None);
    assert_eq!(top.bucket_of((1 << 63) - 1), None);
    assert_eq!(top.bucket_of(1 << 63), Some(0));
    assert_eq!(top.bucket_of(u64::MAX), Some(63));
    // A width that doesn't divide by 64: the last bucket takes the rest.
    let r = HashRange::new(10, Some(10 + 64 * 3 + 5));
    assert_eq!(r.buckets(), 64);
    assert_eq!(r.bucket_of(9), None);
    assert_eq!(r.bucket_of(10), Some(0));
    assert_eq!(r.bucket_of(12), Some(0));
    assert_eq!(r.bucket_of(13), Some(1));
    assert_eq!(r.bucket_of(10 + 64 * 3 + 4), Some(63));
    assert_eq!(r.bucket_of(10 + 64 * 3 + 5), None);
    // Fewer hashes than buckets: one bucket each; empty: none.
    let tiny = HashRange::new(u64::MAX - 4, None);
    assert_eq!(tiny.buckets(), 5);
    assert_eq!(tiny.bucket_of(u64::MAX), Some(4));
    assert_eq!(tiny.bucket_lo(4), u64::MAX);
    let one = HashRange::new(7, Some(8));
    assert_eq!(one.buckets(), 1);
    assert_eq!(one.bucket_of(7), Some(0));
    assert_eq!(HashRange::new(5, Some(5)).buckets(), 0);
    assert_eq!(HashRange::new(5, Some(5)).bucket_of(5), None);
    // The histogram built from a sketch puts every hash in the same
    // bucket the sketch counted it in.
    for range in [all, top, r, tiny] {
        let mut s = WriteSketch::new(ShardKey::new(1, 1, 1), cid("c1"), 0);
        let mut rng = Rng(7);
        let mut hs = vec![range.lo, range.hi.map_or(u64::MAX, |h| h - 1)];
        for _ in 0..200 {
            let w = range.hi.map_or(u64::MAX - range.lo, |h| h - range.lo);
            hs.push(range.lo + rng.next() % w.max(1));
        }
        for h in &hs {
            s.record(Some((range, *h)));
        }
        let hist = s.histogram().unwrap();
        assert_eq!(hist.buckets.len(), range.buckets());
        for h in &hs {
            assert_eq!(hist.bucket_of(*h), range.bucket_of(*h).unwrap(), "{h}");
        }
        assert_eq!(hist.total(Weight::Writes), hs.len() as f64);
    }
}

fn check_bounds(ss: &SpaceSaving, truth: &HashMap<u64, u64>, n: u64) {
    let listed: HashMap<u64, (u64, u64)> = ss.entries().iter().map(|e| (e.0, (e.1, e.2))).collect();
    assert!(listed.len() <= TOP_K);
    for (h, (c, e)) in &listed {
        let t = truth.get(h).copied().unwrap_or(0);
        assert!(
            c - e <= t && t <= *c,
            "hash {h}: count {c} error {e} true {t}"
        );
        assert!(
            *e <= n / TOP_K as u64,
            "error {e} above N/k = {}",
            n / TOP_K as u64
        );
    }
    for (h, t) in truth {
        if *t > n / TOP_K as u64 {
            assert!(
                listed.contains_key(h),
                "frequent hash {h} ({t} of {n}) missing"
            );
        } else if !listed.contains_key(h) {
            assert!(*t <= ss.floor(), "unlisted {h} has {t} above the floor");
        }
    }
}

#[test]
fn space_saving_keeps_its_error_bounds() {
    for seed in 1..=20u64 {
        let mut rng = Rng(seed * 7919);
        let mut ss = SpaceSaving::default();
        let mut truth: HashMap<u64, u64> = HashMap::new();
        let n = 5_000u64;
        for _ in 0..n {
            let h = rng.skewed(300).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            ss.offer(h, 1);
            *truth.entry(h).or_default() += 1;
        }
        check_bounds(&ss, &truth, n);
    }
}

#[test]
fn space_saving_merges_within_the_bounds_of_the_union() {
    for seed in 1..=20u64 {
        let mut rng = Rng(seed * 104_729);
        let mut parts = Vec::new();
        let mut truth: HashMap<u64, u64> = HashMap::new();
        let mut n = 0;
        // Gateways see different mixes: each favours its own hashes.
        for g in 0..4u64 {
            let mut ss = SpaceSaving::default();
            let len = 500 + rng.next() % 3_000;
            for _ in 0..len {
                let h = if rng.next().is_multiple_of(3) {
                    g * 1_000 + rng.skewed(50)
                } else {
                    rng.skewed(400)
                };
                ss.offer(h, 1);
                *truth.entry(h).or_default() += 1;
            }
            n += len;
            parts.push(ss);
        }
        let mut merged = SpaceSaving::default();
        for p in &parts {
            merged.merge(p);
        }
        check_bounds(&merged, &truth, n);
        // Merging is order-independent in its bounds, and a summary that
        // isn't full merges exactly.
        let mut rev = SpaceSaving::default();
        for p in parts.iter().rev() {
            rev.merge(p);
        }
        check_bounds(&rev, &truth, n);
    }
    let mut a = SpaceSaving::default();
    let mut b = SpaceSaving::default();
    a.offer(1, 3);
    b.offer(1, 2);
    b.offer(2, 1);
    a.merge(&b);
    let mut e = a.entries().to_vec();
    e.sort_unstable();
    assert_eq!(e, vec![(1, 5, 0), (2, 1, 0)]);
}

#[test]
fn sketches_merge_and_serialize_compactly() {
    let k = ShardKey::new(42, 3, 7);
    let range = HashRange::new(1 << 62, Some(1 << 63));
    let mut a = WriteSketch::new(k, cid("c1"), 100);
    let mut b = WriteSketch::new(k, cid("c1"), 101);
    a.record(Some((range, (1 << 62) + 5)));
    a.record(None);
    b.record(Some((range, (1 << 62) + 5)));
    b.record(Some((range, (1 << 63) - 1)));
    b.record(Some((range, 3))); // outside: counted, not hashed
    a.merge(&b);
    assert_eq!(a.writes, 5);
    assert_eq!(a.hashed(), 3);
    assert_eq!(a.minute, 100);
    assert_eq!(a.buckets[0], 2);
    assert_eq!(a.buckets[63], 1);
    let h = a.histogram().unwrap();
    assert_eq!(h.heavy[0], ((1 << 62) + 5, 2.0));
    // An unhashed sketch merges into a hashed one and back.
    let mut c = WriteSketch::new(k, cid("c1"), 99);
    c.record(None);
    c.merge(&a);
    assert_eq!((c.writes, c.hashed(), c.minute), (6, 3, 99));
    // JSON round trip; an old report without sketches still reads.
    let rep = LoadReport {
        gateway: "gw-1".into(),
        at_ms: 1,
        shards: Vec::new(),
        sketches: vec![c.clone()],
        cluster_writes: vec![ClusterWrites {
            cluster: cid("c1"),
            minute: 99,
            writes: 9,
        }],
    };
    let js = serde_json::to_string(&rep).unwrap();
    assert_eq!(serde_json::from_str::<LoadReport>(&js).unwrap(), rep);
    let old: LoadReport =
        serde_json::from_str(r#"{"gateway":"gw-0","at_ms":5,"shards":[]}"#).unwrap();
    assert!(old.sketches.is_empty() && old.cluster_writes.is_empty());
    let plain = serde_json::to_string(&LoadReport {
        sketches: Vec::new(),
        cluster_writes: Vec::new(),
        ..rep
    })
    .unwrap();
    assert!(!plain.contains("sketches"), "{plain}");
}

#[test]
fn gateway_reports_completed_minutes_bounded() {
    let acc = WriteSketches::new();
    let c1 = cid("c1");
    let range = HashRange::new(0, None);
    let t0 = 1_000 * 60_000; // minute 1000
    // Minute 1000: 200 shards, shard i written i + 1 times, all hashed.
    let mut rng = Rng(3);
    for i in 0..200u32 {
        let k = ShardKey::new(9, 1, i);
        let ws: Vec<_> = (0..=i)
            .map(|_| (k, &c1, Some((range, rng.next()))))
            .collect();
        acc.record(t0 + 5_000, ws);
    }
    acc.record_cluster(t0 + 6_000, &cid("c2"), 4);
    // Minute 1001, still running: not reported yet.
    acc.record(t0 + 61_000, [(ShardKey::new(9, 1, 0), &c1, None)]);
    let (sk, cl) = acc.report(t0 + 62_000);
    assert_eq!(sk.len(), MAX_SKETCHES_PER_MINUTE);
    assert!(sk.iter().all(|s| s.minute == 1000));
    assert_eq!(sk[0].shard, ShardKey::new(9, 1, 199), "most written first");
    assert_eq!(sk[0].writes, 200);
    let total: u64 = (1..=200).sum();
    assert_eq!(
        cl,
        vec![
            ClusterWrites {
                cluster: c1.clone(),
                minute: 1000,
                writes: total
            },
            ClusterWrites {
                cluster: cid("c2"),
                minute: 1000,
                writes: 4
            },
        ]
    );
    assert_eq!(acc.newest_reported(t0 + 62_000), Some(1000));
    // A full report stays well under etcd's value limit.
    let rep = LoadReport {
        gateway: "gw-1".into(),
        at_ms: t0 + 62_000,
        shards: Vec::new(),
        sketches: sk.iter().chain(&sk).chain(&sk).cloned().collect(),
        cluster_writes: cl,
    };
    let bytes = serde_json::to_vec(&rep).unwrap().len();
    assert!(bytes < 400_000, "{bytes} bytes");
    // Minutes older than the report window are dropped.
    let (sk, cl) = acc.report(t0 + (REPORT_MINUTES + 2) * 60_000);
    assert!(sk.is_empty() && cl.is_empty());
    assert_eq!(
        acc.newest_reported(t0 + (REPORT_MINUTES + 2) * 60_000),
        None
    );
}

/// A gateway's report of minute `m`: `hot` writes to shard 1 (a share
/// `skew` of them on hash `key`, the rest spread over its range), `cold`
/// to shard 2, and `other` unsharded writes on the cluster.
fn minute_report(
    gw: &str,
    m: u64,
    hot: u64,
    cold: u64,
    other: u64,
    key: Option<u64>,
) -> LoadReport {
    let acc = WriteSketches::new();
    let c = cid("c1");
    let r1 = HashRange::new(0, Some(1 << 63));
    let r2 = HashRange::new(1 << 63, None);
    let mut rng = Rng(m * 31 + gw.len() as u64);
    let at = m * 60_000 + 1_000;
    for i in 0..hot {
        let h = match key {
            Some(k) if i % 4 != 0 => k,
            _ => rng.next() >> 1,
        };
        acc.record(at, [(ShardKey::new(42, 3, 1), &c, Some((r1, h)))]);
    }
    for _ in 0..cold {
        acc.record(
            at,
            [(
                ShardKey::new(42, 3, 2),
                &c,
                Some((r2, (1 << 63) | rng.next())),
            )],
        );
    }
    acc.record_cluster(at, &c, other);
    let mut rep = LoadReport {
        gateway: gw.to_owned(),
        at_ms: (m + 1) * 60_000 + 5_000,
        shards: Vec::new(),
        sketches: Vec::new(),
        cluster_writes: Vec::new(),
    };
    (rep.sketches, rep.cluster_writes) = acc.report(rep.at_ms);
    rep
}

#[test]
fn the_collector_takes_each_minute_once_and_computes_shares() {
    let mut col = SketchCollector::new();
    let k1 = ShardKey::new(42, 3, 1);
    for m in 500..510 {
        for gw in ["gw-a", "gw-b"] {
            let r = minute_report(gw, m, 60, 30, 10, None);
            col.ingest(&r);
            col.ingest(&r); // a report read twice counts once
        }
    }
    let now = 510 * 60_000 + 50_000; // minute 509 settled
    let shares = col.write_shares(&k1, None, now);
    assert_eq!(shares.len(), 10);
    assert!(shares.iter().all(|s| (s - 0.6).abs() < 1e-9), "{shares:?}");
    // 20 s into minute 510, minute 509 isn't complete yet.
    assert_eq!(col.write_shares(&k1, None, 510 * 60_000 + 20_000).len(), 9);
    // After a cutover at minute 505, only later minutes count.
    let since = 505 * 60_000 + 30_000;
    assert_eq!(col.write_shares(&k1, Some(since), now).len(), 4);
    // A minute with no data is 0, never skipped.
    let gap = col.write_shares(&k1, None, 512 * 60_000 + 50_000);
    assert_eq!(gap.len(), 12);
    assert_eq!(&gap[10..], &[0.0, 0.0]);
    // Histogram over the last 5 minutes: both gateways, all hashed.
    let h = col
        .write_histogram(
            &k1,
            HashRange::new(0, Some(1 << 63)),
            Duration::from_secs(300),
            now,
        )
        .unwrap();
    assert_eq!(h.total(Weight::Writes), 5.0 * 2.0 * 60.0);
    assert_eq!(h.buckets.len(), 64);
    // Another range (a stale question) has no histogram.
    assert!(
        col.write_histogram(&k1, HashRange::new(0, None), Duration::from_secs(300), now)
            .is_none()
    );
    // Writes per second on the cluster: 200 a minute.
    let r = col.cluster_writes_per_sec(&cid("c1"), Duration::from_secs(600), now);
    assert!((r - 200.0 / 60.0).abs() < 1e-9, "{r}");
    // Retention.
    col.prune(now + (TOTALS_RETAIN_MINUTES + 20) * 60_000);
    assert!(col.write_shares(&k1, None, now).is_empty());
}

fn stat(shard: u32, lo: u64, hi: Option<u64>, shares: Vec<f64>) -> ShardStat {
    ShardStat {
        shard,
        lo,
        hi,
        cluster: cid("c1"),
        size_bytes: 1e6,
        write_shares: shares,
        cluster_hot: true,
        born_ms: None,
        busy: false,
    }
}

/// Drives the triggers from sketches: 35 minutes from two gateways, shard 1
/// taking 75% of its cluster's writes.
fn drive(key: Option<u64>) -> (Vec<crate::shards::Proposal>, HashHistogram) {
    let mut col = SketchCollector::new();
    for m in 1_000..1_035 {
        for gw in ["gw-a", "gw-b"] {
            col.ingest(&minute_report(gw, m, 300, 80, 20, key));
        }
    }
    let now = 1_035 * 60_000 + 50_000;
    let s = ShardingSettings::default();
    let k1 = ShardKey::new(42, 3, 1);
    let k2 = ShardKey::new(42, 3, 2);
    let g = GroupStat {
        tenant: TenantId(42),
        group: 3,
        min_shards: None,
        shards: vec![
            stat(1, 0, Some(1 << 63), col.write_shares(&k1, None, now)),
            stat(2, 1 << 63, None, col.write_shares(&k2, None, now)),
        ],
    };
    assert!((g.shards[0].write_shares[0] - 0.75).abs() < 1e-9);
    let p = evaluate(&g, &s, now);
    let h = col
        .write_histogram(&k1, HashRange::new(0, Some(1 << 63)), s.sustain, now)
        .unwrap();
    (p, h)
}

#[test]
fn sketches_drive_a_load_split_at_the_weighted_median() {
    let (p, h) = drive(None);
    assert_eq!(p.len(), 1, "{p:?}");
    assert_eq!(p[0].reason, Reason::Load);
    assert_eq!(p[0].job.sources(), vec![1]);
    // Writes spread evenly: two children of about half the writes each.
    let plan = choose_split(&h, Weight::Writes, 2).unwrap();
    assert_eq!(plan.hot_key, None);
    assert_eq!(plan.points.len(), 1);
    let mid = 1u64 << 62;
    assert!(plan.points[0].abs_diff(mid) < 1 << 59, "{plan:?}");
    assert!(
        plan.shares.iter().all(|x| (x - 0.5).abs() < 0.1),
        "{plan:?}"
    );
}

#[test]
fn sketches_isolate_a_dominant_hot_key() {
    let key = 0x313c_a5e2_98b7_eb08;
    let (p, h) = drive(Some(key));
    assert_eq!(p[0].reason, Reason::Load);
    // Three quarters of the writes are one hash: it gets a shard of its
    // own (the loop then records the reason as hot_key).
    assert_eq!(h.heavy[0].0, key);
    let plan = choose_split(&h, Weight::Writes, 2).unwrap();
    assert_eq!(plan.hot_key, Some(key));
    assert_eq!(plan.points, vec![key, key + 1]);
    assert!(plan.shares[1] > 0.7, "{plan:?}");
}

#[test]
fn a_shard_under_the_share_or_on_a_cool_cluster_does_not_split() {
    let mut col = SketchCollector::new();
    for m in 2_000..2_035 {
        col.ingest(&minute_report("gw-a", m, 40, 40, 20, None));
    }
    let now = 2_035 * 60_000 + 50_000;
    let shares = col.write_shares(&ShardKey::new(42, 3, 1), None, now);
    assert!(shares.iter().all(|s| (s - 0.4).abs() < 1e-9));
    let s = ShardingSettings::default();
    let g = GroupStat {
        tenant: TenantId(42),
        group: 3,
        min_shards: None,
        shards: vec![stat(1, 0, Some(1 << 63), shares)],
    };
    assert!(evaluate(&g, &s, now).is_empty());
    let mut hot = g.clone();
    hot.shards[0].write_shares = vec![0.9; 35];
    hot.shards[0].cluster_hot = false;
    assert!(evaluate(&hot, &s, now).is_empty());
}
