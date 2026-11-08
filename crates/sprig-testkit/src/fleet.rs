//! The fleet simulator (chapter 11, build step 8; chapter 46): months of
//! synthetic growth and noise on a deliberately skewed fleet, with the
//! real planner ([`sprig_balancer::Planner`]) and scheduler
//! ([`sprig_balancer::Scheduler`]) and a fake mover.
//!
//! One run ([`run`]) is deterministic for its seed:
//!
//! 1. Clusters of a few sizes in one DC; one of them has a replica in
//!    another DC, so tenants pinned to the home DC must never land there.
//! 2. Tenants of one to three groups (one shard each), with log-spread
//!    sizes and busy times. Most start on the first two clusters (the
//!    skew); the rest spread at random.
//! 3. Every minute (the balancer's interval): storage grows, busy time is
//!    observed with `±noise` and averaged over `balancer.window_minutes`,
//!    finished moves cut over, the scheduler polls, the planner plans,
//!    the scheduler schedules. New tenants arrive every day through
//!    `place()`.
//! 4. The fake mover copies at `copy_gb_per_min` and fails a fraction of
//!    moves; a failed move leaves the shard where it was.
//!
//! The report has the spread (fullest minus emptiest `full`) before and
//! after, the moves, every hard-constraint violation the run saw (target
//! not placeable, residency, storage over `balancer.storage_hard_pct`),
//! and ping-pong two ways: the balancer's own reverted-move counter
//! (back within 7 days) and any shard that ever returned to a cluster it
//! had left, at any distance in time.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use sprig_balancer::decisions::MemDecisionLog;
use sprig_balancer::model::{Capacity, ClusterState, ClusterView, TenantView};
use sprig_balancer::plan::{PlaceReason, PlaceRequest};
use sprig_balancer::settings::{BalancerMode, BalancerSettings, GB};
use sprig_balancer::{
    FleetView, LoadVector, MoveExecutor, MoveOrder, MoveStatus, MoveTicket, PlacementPolicy,
    Planner, Scheduler, ShardKey, ShardView,
};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId, TenantId};

use crate::sim::SimRng;

/// What to simulate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetConfig {
    /// The seed.
    pub seed: u64,
    /// Clusters.
    pub clusters: u32,
    /// Tenants at the start.
    pub tenants: u32,
    /// Simulated days.
    pub days: u32,
    /// Observation noise on busy time, as a fraction (0.10 is ±10%).
    pub noise: f64,
    /// Share of the starting shards put on the first two clusters.
    pub skew: f64,
    /// Storage growth per day, as a fraction.
    pub growth_per_day: f64,
    /// New tenants per day.
    pub new_tenants_per_day: u32,
    /// The balancer's mode for every tenant.
    pub mode: BalancerMode,
    /// The planner refuses moves back within 7 days.
    pub avoid_move_back: bool,
    /// Copy speed of the fake mover.
    pub copy_gb_per_min: f64,
    /// Share of moves that fail.
    pub fail_rate: f64,
    /// A cluster down from one day to another (index, from day, to day):
    /// shards on it stay, it takes nothing, then it recovers.
    pub outage: Option<(u32, u32, u32)>,
}

impl FleetConfig {
    /// A 30-day run of 8 clusters and 240 tenants, 70% on two clusters.
    #[must_use]
    pub fn skewed(seed: u64) -> Self {
        Self {
            seed,
            clusters: 8,
            tenants: 240,
            days: 30,
            noise: 0.10,
            skew: 0.7,
            growth_per_day: 0.003,
            new_tenants_per_day: 4,
            mode: BalancerMode::Auto,
            avoid_move_back: true,
            copy_gb_per_min: 2.0,
            fail_rate: 0.05,
            outage: None,
        }
    }
}

/// What a run saw.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FleetReport {
    /// The seed.
    pub seed: u64,
    /// Fullest minus emptiest `full` at the start.
    pub spread_before: f64,
    /// The highest `full` at the start.
    pub max_full_before: f64,
    /// Fullest minus emptiest `full` at the end.
    pub spread_after: f64,
    /// The highest `full` at the end.
    pub max_full_after: f64,
    /// The largest spread over the last day.
    pub spread_last_day_max: f64,
    /// Hours until the spread first fell to `balancer.spread_pct`.
    pub hours_to_even: Option<f64>,
    /// Moves that cut over.
    pub moves: u64,
    /// Moves that failed.
    pub moves_failed: u64,
    /// Moves in the last week (steady state).
    pub moves_last_week: u64,
    /// Most moves of one shard.
    pub max_moves_per_shard: u64,
    /// `sprig_balancer_moves_reverted_total`.
    pub reverted: u64,
    /// Shards that ever returned to a cluster they had left.
    pub returned_shards: u64,
    /// Hard-constraint violations seen.
    pub violations: Vec<String>,
    /// New shards placed.
    pub placements: u64,
    /// Placements refused (39120).
    pub placement_failures: u64,
    /// Decisions written.
    pub decisions: u64,
    /// Moves by reason.
    pub moves_by_reason: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
struct SimShard {
    cluster: ClusterId,
    storage_gb: f64,
    busy: f64,
    observed_busy: f64,
    path: Vec<ClusterId>,
}

#[derive(Debug)]
struct Job {
    order: MoveOrder,
    finish_at: u64,
    fail: bool,
    done: bool,
}

#[derive(Debug, Default)]
struct World {
    jobs: BTreeMap<String, Job>,
    next: u64,
}

/// The fake mover: copies at a fixed speed, fails some moves.
struct FakeMover {
    world: Arc<Mutex<World>>,
    now: Arc<AtomicU64>,
    rng: SimRng,
    gb_per_min: f64,
    fail_rate: f64,
}

#[async_trait::async_trait]
impl MoveExecutor for FakeMover {
    async fn submit(&self, order: &MoveOrder) -> Result<MoveTicket> {
        let mut w = self.world.lock().unwrap_or_else(PoisonError::into_inner);
        w.next += 1;
        let id = format!("job-{}", w.next);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let secs = ((order.size_bytes / GB / self.gb_per_min) * 60.0).ceil() as u64 + 120;
        let fail = self.rng.chance(self.fail_rate);
        w.jobs.insert(
            id.clone(),
            Job {
                order: order.clone(),
                finish_at: self.now.load(Ordering::Relaxed) + secs,
                fail,
                done: false,
            },
        );
        Ok(MoveTicket(id))
    }

    async fn status(&self, t: &MoveTicket) -> Result<MoveStatus> {
        let w = self.world.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(match w.jobs.get(&t.0) {
            Some(j) if j.done && j.fail => MoveStatus::Failed {
                reason: "simulated failure".to_owned(),
            },
            Some(j) if j.done => MoveStatus::Succeeded,
            Some(_) => MoveStatus::Running,
            None => MoveStatus::Cancelled,
        })
    }

    async fn cancel(&self, _t: &MoveTicket, _reason: &str) -> Result<()> {
        Ok(())
    }
}

fn cluster_id(i: u32) -> ClusterId {
    ClusterId::new(format!("mum1-c{}", i + 1)).unwrap_or_else(|_| unreachable!())
}

fn dc(s: &str) -> DcId {
    DcId::new(s).unwrap_or_else(|_| unreachable!())
}

/// A size spread over two orders of magnitude.
fn log_spread(rng: &SimRng, lo: f64, hi: f64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let u = rng.below(1_000_000) as f64 / 1_000_000.0;
    (lo.ln() + u * (hi.ln() - lo.ln())).exp()
}

/// Runs one simulation.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn run(cfg: &FleetConfig) -> FleetReport {
    futures::executor::block_on(run_async(cfg))
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn run_async(cfg: &FleetConfig) -> FleetReport {
    let rng = SimRng::new(cfg.seed);
    let settings = BalancerSettings::default();
    let mut planner = Planner::new(settings.clone());
    planner.avoid_move_back = cfg.avoid_move_back;
    let now = Arc::new(AtomicU64::new(1_700_000_000));
    let world = Arc::new(Mutex::new(World::default()));
    let mover = FakeMover {
        world: Arc::clone(&world),
        now: Arc::clone(&now),
        rng: rng.fork(),
        gb_per_min: cfg.copy_gb_per_min,
        fail_rate: cfg.fail_rate,
    };
    let log = MemDecisionLog::new();
    let mut sched = Scheduler::new("mum1", "sim");

    // Clusters: sizes 1x, 1.5x, 2x; the last has a replica in fra1.
    let mut base = FleetView::default();
    for i in 0..cfg.clusters {
        let k = [1.0, 1.5, 2.0][(i % 3) as usize];
        let mut c = ClusterView::new(
            cluster_id(i),
            dc("mum1"),
            Capacity {
                storage_bytes: 500.0 * GB * k,
                busy_ms_per_sec: 4000.0 * k,
                write_rows_per_sec: 0.0,
                connections: 0.0,
                threads: 0.0,
            },
        );
        if i + 1 == cfg.clusters {
            c.replica_dcs = vec![dc("fra1")];
        }
        base.add_cluster(c);
    }
    let ids: Vec<ClusterId> = base.clusters.keys().cloned().collect();
    let mut tenants: BTreeMap<TenantId, TenantView> = BTreeMap::new();
    let mut shards: BTreeMap<ShardKey, SimShard> = BTreeMap::new();
    let total_storage: f64 = base.clusters.values().map(|c| c.cap.storage_bytes / GB).sum();
    // Aim the starting fleet at about 45% of total storage.
    let per_tenant_gb = total_storage * 0.45 / f64::from(cfg.tenants.max(1));
    let new_tenant = |t: u64, rng: &SimRng, tenants: &mut BTreeMap<TenantId, TenantView>| {
        let mut tv = TenantView::new(TenantId(t));
        // A third of the tenants are pinned to the home DC.
        if t % 3 == 0 {
            tv.allowed_dcs = Some(BTreeSet::from([dc("mum1")]));
        }
        tenants.insert(TenantId(t), tv);
        let groups = 1 + rng.below(3);
        (0..groups)
            .map(|g| {
                let gb = log_spread(rng, per_tenant_gb / 10.0, per_tenant_gb * 2.5) / groups as f64;
                let busy = gb * log_spread(rng, 1.0, 12.0);
                (ShardKey::new(t, u32::try_from(g + 1).unwrap_or(1), u32::try_from(g + 1).unwrap_or(1)), gb, busy)
            })
            .collect::<Vec<_>>()
    };
    let homes: Vec<ClusterId> = ids[..ids.len().min(2)].to_vec();
    // The skew fills the two home clusters to at most 95% of storage (a
    // real cluster can't hold more); the rest spreads at random.
    let mut used: BTreeMap<ClusterId, f64> = BTreeMap::new();
    for t in 1..=u64::from(cfg.tenants) {
        let on_skew = rng.chance(cfg.skew);
        for (k, gb, busy) in new_tenant(t, &rng, &mut tenants) {
            let mut c = if on_skew {
                rng.pick(&homes).clone()
            } else {
                rng.pick(&ids).clone()
            };
            let pinned = tenants[&k.tenant].allowed_dcs.is_some();
            let room = |c: &ClusterId, used: &BTreeMap<ClusterId, f64>| {
                used.get(c).copied().unwrap_or(0.0) + gb <= 0.95 * base.clusters[c].cap.storage_bytes / GB
                    && !(pinned && base.clusters[c].replica_dcs.contains(&dc("fra1")))
            };
            if !room(&c, &used) {
                c = ids
                    .iter()
                    .filter(|x| room(x, &used))
                    .min_by(|a, b| used.get(*a).copied().unwrap_or(0.0).total_cmp(&used.get(*b).copied().unwrap_or(0.0)))
                    .cloned()
                    .unwrap_or(c);
            }
            *used.entry(c.clone()).or_default() += gb;
            shards.insert(
                k,
                SimShard {
                    cluster: c.clone(),
                    storage_gb: gb,
                    busy,
                    observed_busy: busy,
                    path: vec![c],
                },
            );
        }
    }

    let truth = |shards: &BTreeMap<ShardKey, SimShard>, tenants: &BTreeMap<TenantId, TenantView>, noisy: bool| {
        let mut v = base.clone();
        v.tenants.clone_from(tenants);
        for (k, s) in shards {
            v.shards.insert(
                *k,
                ShardView::new(
                    *k,
                    s.cluster.clone(),
                    LoadVector {
                        storage_bytes: s.storage_gb * GB,
                        busy_ms_per_sec: if noisy { s.observed_busy } else { s.busy },
                        schemas: 1.0,
                        tables: 6.0,
                        ..LoadVector::default()
                    },
                ),
            );
        }
        v
    };
    let mut report = FleetReport {
        seed: cfg.seed,
        ..FleetReport::default()
    };
    let start = truth(&shards, &tenants, false);
    report.spread_before = start.spread();
    report.max_full_before = start
        .fullness()
        .values()
        .map(|f| f.value)
        .fold(0.0, f64::max);

    let t0 = now.load(Ordering::Relaxed);
    let steps = u64::from(cfg.days) * 1440;
    let alpha = 1.0 - (-60.0 / settings.window.as_secs_f64()).exp();
    let mut next_tenant = u64::from(cfg.tenants) + 1;
    let mut moves_per_shard: BTreeMap<ShardKey, u64> = BTreeMap::new();
    let mode = cfg.mode;
    let mode_of = move |_: TenantId| mode;
    for step in 0..steps {
        let t = t0 + step * 60;
        now.store(t, Ordering::Relaxed);
        let day = u32::try_from(step / 1440).unwrap_or(u32::MAX);
        // Load: growth and noise.
        for s in shards.values_mut() {
            s.storage_gb *= 1.0 + cfg.growth_per_day / 1440.0;
            s.busy *= 1.0 + cfg.growth_per_day / 1440.0;
            let u = (rng.below(2_000_001) as f64 / 1_000_000.0) - 1.0;
            let sample = s.busy * (1.0 + cfg.noise * u);
            s.observed_busy += alpha * (sample - s.observed_busy);
        }
        // Cutovers.
        {
            let mut w = world.lock().unwrap_or_else(PoisonError::into_inner);
            for j in w.jobs.values_mut().filter(|j| !j.done && j.finish_at <= t) {
                j.done = true;
                if j.fail {
                    continue;
                }
                for k in &j.order.shards {
                    if let Some(s) = shards.get_mut(k) {
                        s.cluster = j.order.to.clone();
                        s.path.push(j.order.to.clone());
                        *moves_per_shard.entry(*k).or_default() += 1;
                    }
                }
            }
        }
        // New tenants once a day, through place().
        if step % 1440 == 720 {
            for _ in 0..cfg.new_tenants_per_day {
                let t_id = next_tenant;
                next_tenant += 1;
                let parts = new_tenant(t_id, &rng, &mut tenants);
                let mut view = truth(&shards, &tenants, true);
                set_outage(&mut view, cfg, day, t0, &ids);
                for (k, gb, busy) in parts {
                    let req = PlaceRequest {
                        tenant: k.tenant,
                        units: vec![k],
                        predicted: LoadVector {
                            storage_bytes: gb * GB,
                            schemas: 1.0,
                            tables: 6.0,
                            ..LoadVector::default()
                        },
                        reason: PlaceReason::NewTenant,
                    };
                    match planner.place(&view, &req, t) {
                        Ok(d) => {
                            report.placements += 1;
                            check_target(&view, k.tenant, &d.cluster, gb * GB, &mut report.violations, "place");
                            view.add_shard(ShardView::new(k, d.cluster.clone(), req.predicted));
                            shards.insert(
                                k,
                                SimShard {
                                    cluster: d.cluster.clone(),
                                    storage_gb: gb,
                                    busy,
                                    observed_busy: busy,
                                    path: vec![d.cluster],
                                },
                            );
                        }
                        Err(_) => report.placement_failures += 1,
                    }
                }
            }
        }
        // Plan and schedule.
        let mut view = truth(&shards, &tenants, true);
        set_outage(&mut view, cfg, day, t0, &ids);
        sched.poll(&view, &mover, &log, t).await;
        sched.annotate(&mut view, t);
        let plan = planner.rebalance(&view, t);
        if std::env::var("SPRIG_FLEET_DEBUG").is_ok() && step % 240 == 0 {
            eprintln!("t+{}h trigger {:?} moves {} notes {:?} full {:?}", step / 60, plan.trigger, plan.moves.len(),
                plan.notes.iter().map(|n| n.text.clone()).collect::<Vec<_>>(),
                plan.fullness.iter().map(|(c, f)| (c.to_string(), (f.value * 100.0).round() / 100.0, f.bottleneck.to_string())).collect::<Vec<_>>());
        }
        for m in &plan.moves {
            check_target(&view, m.tenant, &m.to, m.size_bytes, &mut report.violations, "move");
        }
        sched
            .schedule(&plan, &view, &mode_of, settings.max_concurrent_moves_per_dc as usize, &mover, &log, t)
            .await;
        // Spread on the true loads.
        let real = truth(&shards, &tenants, false);
        let spread = real.spread();
        if report.hours_to_even.is_none() && spread <= settings.spread {
            report.hours_to_even = Some(step as f64 / 60.0);
        }
        if step + 1440 >= steps {
            report.spread_last_day_max = report.spread_last_day_max.max(spread);
        }
    }
    let end = truth(&shards, &tenants, false);
    report.spread_after = end.spread();
    report.max_full_after = end.fullness().values().map(|f| f.value).fold(0.0, f64::max);
    for ((reason, outcome), n) in &sched.moves_total {
        match outcome.as_str() {
            "succeeded" => {
                report.moves += n;
                *report.moves_by_reason.entry(reason.clone()).or_default() += n;
            }
            "failed" => report.moves_failed += n,
            _ => {}
        }
    }
    let week_start = t0 + steps.saturating_sub(7 * 1440) * 60;
    report.moves_last_week = {
        let w = world.lock().unwrap_or_else(PoisonError::into_inner);
        w.jobs
            .values()
            .filter(|j| j.done && !j.fail && j.finish_at >= week_start)
            .count() as u64
    };
    report.max_moves_per_shard = moves_per_shard.values().copied().max().unwrap_or(0);
    report.reverted = sched.thrash.reverted;
    report.returned_shards = shards
        .values()
        .filter(|s| {
            let mut seen = BTreeSet::new();
            let mut back = false;
            let mut prev: Option<&ClusterId> = None;
            for c in &s.path {
                if prev != Some(c) && !seen.insert(c.clone()) {
                    back = true;
                }
                prev = Some(c);
            }
            back
        })
        .count() as u64;
    report.decisions = log.all().len() as u64;
    report
}

fn set_outage(view: &mut FleetView, cfg: &FleetConfig, day: u32, t0: u64, ids: &[ClusterId]) {
    let Some((i, from, to)) = cfg.outage else {
        return;
    };
    let Some(id) = ids.get(i as usize) else {
        return;
    };
    let Some(c) = view.clusters.get_mut(id) else {
        return;
    };
    if day >= from && day < to {
        c.state = ClusterState::Unavailable;
    } else if day >= to {
        c.recovered_at = Some(t0 + u64::from(to) * 86_400);
    }
}

fn check_target(
    view: &FleetView,
    tenant: TenantId,
    to: &ClusterId,
    bytes: f64,
    out: &mut Vec<String>,
    what: &str,
) {
    let Some(c) = view.clusters.get(to) else {
        out.push(format!("{what} to unknown cluster {to}"));
        return;
    };
    if c.state != ClusterState::Placeable {
        out.push(format!("{what} of tenant {tenant} to {to}, which is not placeable"));
    }
    if let Some(allowed) = view.tenants.get(&tenant).and_then(|t| t.allowed_dcs.as_ref())
        && (!allowed.contains(&c.dc) || c.replica_dcs.iter().any(|d| !allowed.contains(d)))
    {
        out.push(format!("{what} of tenant {tenant} to {to} breaks residency"));
    }
    let after = view.load_on(to).storage_bytes + bytes;
    if after / c.cap.storage_bytes > c.limits.storage_hard + 1e-9 {
        out.push(format!("{what} of tenant {tenant} to {to} passes storage_hard_pct"));
    }
}

/// Runs `seeds` and summarizes.
#[must_use]
pub fn run_many(base: &FleetConfig, seeds: std::ops::Range<u64>) -> Vec<FleetReport> {
    seeds
        .map(|s| {
            run(&FleetConfig {
                seed: s,
                ..base.clone()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
