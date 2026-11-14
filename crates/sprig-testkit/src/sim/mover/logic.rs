//! After-commit triggers under simulation: killed workers and duplicate
//! deliveries still give exactly-once compiled triggers.
//!
//! The real trigger runner (`sprig_logic::triggers::run`) reads the real
//! tenant feed over the two clusters' binlog models while the real mover
//! moves the tenant. Its shard is a model of `__sprig_ldone` and the
//! trigger's effect, committed together, which the mover copies with the
//! shard (one store for both clusters). The worker running it is killed at
//! random and starts again from its last acknowledged checkpoint; applies
//! fail at random (retryable, as a lost connection or a held shard), and
//! some events fail every time and go to dead letters, which are then
//! replayed. At the end every client row change the verified feed
//! consumer saw must have run exactly once.

use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use sprig_binlog::GtidPos;
use sprig_common::ids::DbId;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_feed::names::StaticResolver;
use sprig_feed::{Clusters, MetaPlacement, ResumeToken, StartAt, TailOptions, TenantTail};
use sprig_logic::triggers::{self, Applied, Apply, Filter, Outcomes, RunOpts, Stats, TriggerEvent};
use sprig_meta::Tier;

use super::TENANT;
use super::feed::{Goal, SimClusters, reached};
use crate::faults::Node;
use crate::sim::World;

/// What the trigger's shard and dead letters hold.
#[derive(Debug, Default)]
pub(super) struct TriggerLog {
    /// `__sprig_ldone`: event keys done.
    pub markers: BTreeSet<String>,
    /// The trigger's effects, one per run: (cluster, seq, row).
    pub applied: Vec<(String, u64, u32)>,
    /// Dead letters.
    pub dead: Vec<TriggerEvent>,
    /// The subscription's checkpoint.
    pub token: Option<ResumeToken>,
    /// Worker kills.
    pub restarts: u64,
    /// Deliveries a marker stopped.
    pub already: u64,
    /// The feed's error, if it failed.
    pub error: Option<String>,
}

fn id_of(key: &str) -> Option<(String, u64, u32)> {
    // `<cluster>:<domain>-<server>-<seq>:<row>`
    let mut parts = key.rsplitn(3, ':');
    let row = parts.next()?.parse().ok()?;
    let g = parts.next()?;
    let cluster = parts.next()?.to_owned();
    let seq = g.rsplit('-').next()?.parse().ok()?;
    Some((cluster, seq, row))
}

struct SimShard {
    log: Arc<Mutex<TriggerLog>>,
    world: World,
    faults: bool,
}

fn poisoned(key: &str) -> bool {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    h.finish().is_multiple_of(40)
}

#[async_trait]
impl Apply for SimShard {
    async fn apply(&self, ev: &TriggerEvent) -> Result<Applied> {
        self.world.chaos_yield().await;
        if self.faults && self.world.rng().chance(0.05) {
            let mut e =
                SprigError::new(SprigCode::BackendUnavailable, "injected: shard unreachable");
            e.retryable = true;
            return Err(e);
        }
        if self.faults && poisoned(&ev.key) {
            return Err(SprigError::new(
                SprigCode::LogicUserError,
                "injected: the code fails",
            ));
        }
        let mut l = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        // Marker and effect commit together.
        if !l.markers.insert(ev.key.clone()) {
            l.already += 1;
            return Ok(Applied::AlreadyDone);
        }
        if let Some(id) = id_of(&ev.key) {
            l.applied.push(id);
        }
        Ok(Applied::Ran(1))
    }

    fn version(&self) -> u64 {
        1
    }
}

/// A shard that never fails, for replays.
struct Healthy(Arc<Mutex<TriggerLog>>);

#[async_trait]
impl Apply for Healthy {
    async fn apply(&self, ev: &TriggerEvent) -> Result<Applied> {
        let mut l = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !l.markers.insert(ev.key.clone()) {
            return Ok(Applied::AlreadyDone);
        }
        if let Some(id) = id_of(&ev.key) {
            l.applied.push(id);
        }
        Ok(Applied::Ran(1))
    }

    fn version(&self) -> u64 {
        1
    }
}

struct SimOut(Arc<Mutex<TriggerLog>>);

#[async_trait]
impl Outcomes for SimOut {
    async fn dead_letter(
        &self,
        ev: &TriggerEvent,
        _v: u64,
        _attempts: u32,
        _e: &SprigError,
    ) -> Result<()> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .dead
            .push(ev.clone());
        Ok(())
    }

    async fn ack(&self, token: &ResumeToken, _events: u64) -> Result<()> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).token = Some(token.clone());
        Ok(())
    }

    async fn log(&self, _level: &str, _message: &str) {}
}

/// The trigger's worker: runs the binding over the feed, killed at random,
/// started again from its checkpoint, until the checkpoint reaches the
/// goal. With `shared` the feed reads through a shared reader (R9,
/// r9feed2), a new one for each worker that starts after a kill.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(super) async fn trigger_worker(
    world: World,
    clusters: Arc<SimClusters>,
    shared: bool,
    tier: Tier,
    faults: bool,
    goal: Goal,
    log: Arc<Mutex<TriggerLog>>,
    stop: CancellationToken,
) {
    let node = Node::from("logic-worker");
    let placement = Arc::new(MetaPlacement::new(world.etcd().client(&node, tier)));
    let names = Arc::new(
        StaticResolver::new()
            .database(TENANT, DbId(1), "shop")
            .object(TENANT, DbId(1), "a", 11)
            .object(TENANT, DbId(1), "b", 12)
            .object(TENANT, DbId(1), "c", 13),
    );
    let rng = world.rng().fork();
    let opts = RunOpts {
        lanes: 4,
        max_attempts: 3,
        retry_initial: Duration::from_millis(50),
        retry_max: Duration::from_millis(400),
        ack_every: Duration::from_millis(200),
        max_in_flight: 2_000,
        live: None,
    };
    // Every table of the database, every operation.
    let filter = Filter {
        db: 1,
        object: 0,
        table: String::new(),
        events: vec!["insert".into(), "update".into(), "delete".into()],
        when_fields: Vec::new(),
    };
    let tail_opts = TailOptions {
        progress_every: Duration::from_millis(500),
        handoff_poll: Duration::from_millis(300),
        ..TailOptions::default()
    };
    loop {
        if stop.is_cancelled() {
            return;
        }
        let start = log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .token
            .clone()
            .map_or_else(
                || StartAt::At {
                    cluster: clusters.src.clone(),
                    pos: GtidPos::new(),
                },
                StartAt::After,
            );
        let source: Arc<dyn Clusters> = if shared {
            super::streams::shared_view(&clusters)
        } else {
            clusters.clone()
        };
        let tail = match TenantTail::start(
            TENANT,
            start,
            source,
            placement.clone(),
            names.clone(),
            tail_opts.clone(),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                world.record(format!("trigger worker: feed start failed: {e}"));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let cancel = CancellationToken::new();
        let kill_in = (faults && rng.chance(0.85))
            .then(|| rng.duration(Duration::from_millis(50), Duration::from_secs(3)));
        let apply = Arc::new(SimShard {
            log: log.clone(),
            world: world.clone(),
            faults,
        });
        let run = triggers::run(
            Box::new(tail),
            &filter,
            apply,
            Arc::new(SimOut(log.clone())),
            &opts,
            Arc::new(Stats::default()),
            &cancel,
        );
        tokio::pin!(run);
        let kill = async {
            match kill_in {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(kill);
        let mut check = tokio::time::interval(Duration::from_millis(100));
        let ended = loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => {
                    cancel.cancel();
                    let _ = (&mut run).await;
                    return;
                }
                () = &mut kill => {
                    cancel.cancel();
                    let _ = (&mut run).await;
                    let mut l = log.lock().unwrap_or_else(PoisonError::into_inner);
                    l.restarts += 1;
                    world.record("trigger worker killed");
                    break None;
                }
                r = &mut run => break Some(r),
                _ = check.tick() => {
                    let g = goal.lock().unwrap_or_else(PoisonError::into_inner).clone();
                    let t = log.lock().unwrap_or_else(PoisonError::into_inner).token.clone();
                    if let Some(g) = g
                        && reached(t.as_ref(), &g, &clusters.tgt)
                    {
                        cancel.cancel();
                        let _ = (&mut run).await;
                        return;
                    }
                }
            }
        };
        if let Some(Err(e)) = ended {
            // The feed broke: a new worker takes over from the checkpoint.
            world.record(format!("trigger worker: feed error {e}; restarting"));
            log.lock().unwrap_or_else(PoisonError::into_inner).restarts += 1;
        }
    }
}

/// Invariant 9: every client row change the feed consumer verified ran
/// once, after dead letters are replayed.
pub(super) async fn check_triggers(
    feed_seen: &[super::feed::Seen],
    log: &Arc<Mutex<TriggerLog>>,
    done: bool,
) -> std::result::Result<(), String> {
    if !done {
        let l = log.lock().unwrap_or_else(PoisonError::into_inner);
        return Err(format!(
            "invariant 9: the trigger's checkpoint didn't reach the end (token {:?}, error {:?})",
            l.token, l.error
        ));
    }
    let dead: Vec<TriggerEvent> = log
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .dead
        .clone();
    let healthy = Healthy(log.clone());
    for ev in &dead {
        healthy.apply(ev).await.map_err(|e| e.to_string())?;
    }
    let l = log.lock().unwrap_or_else(PoisonError::into_inner);
    let mut counts: HashMap<&(String, u64, u32), u32> = HashMap::new();
    for a in &l.applied {
        *counts.entry(a).or_default() += 1;
    }
    if let Some((id, n)) = counts.iter().find(|(_, n)| **n > 1) {
        return Err(format!("invariant 9: event {id:?} ran {n} times"));
    }
    let expected: BTreeSet<(String, u64, u32)> = feed_seen
        .iter()
        .filter(|s| !s.what.starts_with("ddl "))
        .map(|s| s.id.clone())
        .collect();
    let got: BTreeSet<(String, u64, u32)> = l.applied.iter().cloned().collect();
    if let Some(missing) = expected.difference(&got).next() {
        return Err(format!(
            "invariant 9: event {missing:?} never ran ({} expected, {} ran, {} dead letters, {} restarts)",
            expected.len(),
            got.len(),
            dead.len(),
            l.restarts
        ));
    }
    if let Some(extra) = got.difference(&expected).next() {
        return Err(format!(
            "invariant 9: event {extra:?} ran but isn't a client change"
        ));
    }
    Ok(())
}

/// What a run of the trigger worker did, for the record.
pub(super) fn summary(log: &Arc<Mutex<TriggerLog>>) -> String {
    let l = log.lock().unwrap_or_else(PoisonError::into_inner);
    format!(
        "trigger: {} ran, {} stopped by markers, {} dead letters, {} worker kills",
        l.applied.len(),
        l.already,
        l.dead.len(),
        l.restarts
    )
}
