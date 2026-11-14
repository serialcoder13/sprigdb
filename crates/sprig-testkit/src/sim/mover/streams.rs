//! A stream sequencer under simulation: crash the sequencer at every step;
//! no record lost, duplicated in the log, or out of order per key.
//!
//! The real sequencer (`sprig_streams::Sequencer`) keeps a change topic of
//! table `a` with four partitions, reading the real tenant feed over the two
//! clusters' binlog models while the real mover moves the tenant. Its log
//! is `sprig_streams::memlog::MemLog`, with the stream log's rules (epoch
//! fence, `uk_source`, offsets and position committed together); it is one
//! store for both clusters, as the mover copies the log with the shard.
//! Faults: the sequencer's worker is killed at random and starts again
//! from the log's position; a second sequencer starts while the first still
//! runs (a lost lease: the old one must be fenced); appends fail before
//! they commit or lose their answer after it (an ambiguous commit, then a
//! replay); the feed's streams drop. At the end each partition's records
//! must be exactly the verified feed consumer's changes of `a` with that
//! partition's keys, in commit order, at offsets 0, 1, 2, ... with leader
//! epochs that never go down.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use sprig_binlog::GtidPos;
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{CancellationToken, Result};
use sprig_feed::names::StaticResolver;
use sprig_feed::shared::{Kept, SharedFeed, SharedOpts};
use sprig_feed::{Clusters, MetaPlacement, StartAt, TailOptions, TenantTail};
use sprig_meta::Tier;
use sprig_streams::envelope::ChangeCtx;
use sprig_streams::memlog::{Fault, MemLog};
use sprig_streams::sequencer::{
    FeedOpener, FeedSource, Filter, NoWatermarks, RunEnd, SeqOpts, Sequencer, Start,
};
use sprig_streams::{PartitionNo, Source, partition};

use super::TENANT;
use super::feed::{Goal, SimClusters, reached};
use crate::faults::Node;
use crate::sim::World;

/// Partitions of the topic.
const PARTITIONS: u32 = 4;

/// What the sequencer did.
#[derive(Debug, Default)]
pub(super) struct StreamRun {
    /// Worker kills.
    pub kills: u64,
    /// Overlapping sequencers started (lost leases).
    pub overlaps: u64,
    /// Sequencers that ended fenced.
    pub fenced: u64,
    /// Feed errors that restarted a sequencer.
    pub feed_errors: u64,
}

struct SimFeed {
    clusters: Arc<dyn Clusters>,
    placement: Arc<MetaPlacement>,
    names: Arc<StaticResolver>,
    opts: TailOptions,
    /// A slow consumer: stalls drawn from this (the shared variant), so
    /// its subscription's buffer fills while the hub reads on.
    slow: Option<crate::sim::rng::SimRng>,
}

/// A tail whose reader stalls now and then.
struct Slow {
    tail: TenantTail,
    rng: crate::sim::rng::SimRng,
}

#[async_trait]
impl FeedSource for Slow {
    async fn next(&mut self) -> Result<sprig_feed::FeedItem> {
        if self.rng.chance(0.03) {
            tokio::time::sleep(
                self.rng
                    .duration(Duration::from_millis(100), Duration::from_secs(2)),
            )
            .await;
        }
        self.tail.next().await
    }
}

#[async_trait]
impl FeedOpener for SimFeed {
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>> {
        let at = match start {
            Start::After(t) => StartAt::After(t.clone()),
            Start::At { cluster, pos } => StartAt::At {
                cluster: cluster.clone(),
                pos: pos.clone(),
            },
        };
        let tail = TenantTail::start(
            TENANT,
            at,
            self.clusters.clone(),
            self.placement.clone(),
            self.names.clone(),
            self.opts.clone(),
        )
        .await?;
        match &self.slow {
            Some(r) => Ok(Box::new(Slow {
                tail,
                rng: r.fork(),
            })),
            None => Ok(Box::new(tail)),
        }
    }
}

/// The topic's log, starting where the consumer starts.
pub(super) fn new_log(world: &World, clusters: &SimClusters, faults: bool) -> MemLog {
    let log = MemLog::new(&Start::At {
        cluster: clusters.src.clone(),
        pos: GtidPos::new(),
    });
    if !faults {
        return log;
    }
    let rng = world.rng().fork();
    log.with_faults(Arc::new(move || {
        // One draw: 4% fail before the commit, 4% lose the answer after it.
        match rng.below(100) {
            0..4 => Fault::Before,
            4..8 => Fault::After,
            _ => Fault::None,
        }
    }))
}

fn position(log: &MemLog) -> Option<sprig_feed::ResumeToken> {
    let resume = log.lock().resume.clone();
    match Start::decode(&resume).ok()? {
        Start::After(t) => Some(t),
        Start::At { .. } => None,
    }
}

fn lock(r: &Arc<Mutex<StreamRun>>) -> std::sync::MutexGuard<'_, StreamRun> {
    r.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A worker process's shared reader over the clusters' binlog models, with
/// buffers and a ring small enough that the sequencer's subscription is
/// detached, returns through the ring, and needs catch-up dumps.
fn shared_view(clusters: &Arc<SimClusters>) -> Arc<dyn Clusters> {
    let feed = SharedFeed::new(
        Arc::clone(clusters) as Arc<dyn Clusters>,
        SharedOpts {
            sub_channel: 2,
            ring_groups: 6,
            ring_bytes: 1 << 20,
            max_catchup: 1,
            gather: Duration::from_millis(200),
            max_backlog: 4,
        },
    );
    feed.view(Kept::All)
}

/// The sequencers: one at a time, killed at random, sometimes overlapped
/// by a new one, until the log's position reaches the goal. With `shared`
/// they read through a shared reader, a new one for each worker that
/// starts after a kill (a process's hubs die with it).
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(super) async fn sequencer_worker(
    world: World,
    clusters: Arc<SimClusters>,
    shared: bool,
    tier: Tier,
    faults: bool,
    goal: Goal,
    log: MemLog,
    run: Arc<Mutex<StreamRun>>,
    stop: CancellationToken,
) {
    let node = Node::from("stream-worker");
    let make_feed = |c: Arc<dyn Clusters>| SimFeed {
        placement: Arc::new(MetaPlacement::new(world.etcd().client(&node, tier.clone()))),
        names: Arc::new(
            StaticResolver::new()
                .database(TENANT, DbId(1), "shop")
                .object(TENANT, DbId(1), "a", 11)
                .object(TENANT, DbId(1), "b", 12)
                .object(TENANT, DbId(1), "c", 13),
        ),
        clusters: c,
        slow: shared.then(|| world.rng().fork()),
        opts: TailOptions {
            progress_every: Duration::from_millis(500),
            handoff_poll: Duration::from_millis(300),
            ..TailOptions::default()
        },
    };
    let fresh = || -> Arc<dyn Clusters> {
        if shared {
            shared_view(&clusters)
        } else {
            clusters.clone()
        }
    };
    let mut feed = Arc::new(make_feed(fresh()));
    let rng = world.rng().fork();
    let make = |feed: &Arc<SimFeed>| Sequencer {
        filter: Filter::Changes {
            db: DbId(1),
            object: ObjectId(11),
            table: "a".into(),
            cx: ChangeCtx {
                partitions: PARTITIONS,
                tenant_name: Some("sim".into()),
            },
        },
        owned: Vec::new(),
        store: Arc::new(log.clone()),
        feed: feed.clone(),
        marks: Arc::new(NoWatermarks),
        opts: SeqOpts {
            batch_window: Duration::from_millis(20),
            max_batch: 7,
            idle_checkpoint: Duration::from_millis(300),
            retry: Duration::from_millis(50),
            cleanup: false,
        },
        stats: Arc::default(),
    };
    // An overlapped sequencer still running, and its stop.
    let mut old: Option<(tokio::task::JoinHandle<Result<RunEnd>>, CancellationToken)> = None;
    loop {
        if stop.is_cancelled() {
            break;
        }
        let cancel = CancellationToken::new();
        let seq = make(&feed);
        let c2 = cancel.clone();
        let mut task = tokio::spawn(async move { seq.run(&c2).await });
        let kill_in = (faults && rng.chance(0.8))
            .then(|| rng.duration(Duration::from_millis(30), Duration::from_secs(3)));
        let overlap = faults && rng.chance(0.3);
        let kill = async {
            match kill_in {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(kill);
        let mut check = tokio::time::interval(Duration::from_millis(100));
        let restart = loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => {
                    cancel.cancel();
                    let _ = (&mut task).await;
                    break false;
                }
                () = &mut kill => {
                    if overlap {
                        // A new sequencer takes over while this one still
                        // runs: it must be fenced, not append.
                        lock(&run).overlaps += 1;
                        world.record("stream sequencer overlapped by a new one");
                        if let Some((t, c)) = old.take() {
                            c.cancel();
                            let _ = t.await;
                        }
                        old = Some((task, cancel));
                    } else {
                        cancel.cancel();
                        let _ = (&mut task).await;
                        lock(&run).kills += 1;
                        world.record("stream sequencer killed");
                        // The worker process died: its shared reader too.
                        feed = Arc::new(make_feed(fresh()));
                    }
                    break true;
                }
                r = &mut task => {
                    match r {
                        Ok(Ok(RunEnd::Fenced)) => lock(&run).fenced += 1,
                        Ok(Err(e)) => {
                            lock(&run).feed_errors += 1;
                            world.record(format!("stream sequencer error {e}; restarting"));
                        }
                        _ => {}
                    }
                    break true;
                }
                _ = check.tick() => {
                    let g = goal.lock().unwrap_or_else(PoisonError::into_inner).clone();
                    if let Some(g) = g
                        && reached(position(&log).as_ref(), &g, &clusters.tgt)
                    {
                        cancel.cancel();
                        let _ = (&mut task).await;
                        break false;
                    }
                }
            }
        };
        if !restart {
            break;
        }
    }
    if let Some((t, c)) = old.take() {
        c.cancel();
        if let Ok(Ok(RunEnd::Fenced)) = t.await {
            lock(&run).fenced += 1;
        }
    }
}

/// The feed event id of a `source_event`.
fn id_of(s: &Source) -> Option<(String, u64, u32)> {
    let Source::Change(b) = s else {
        return None;
    };
    let n = usize::from(*b.first()?);
    let cluster = String::from_utf8_lossy(b.get(1..=n)?).into_owned();
    let rest = b.get(n + 1..)?;
    let seq = u64::from_be_bytes(rest.get(8..16)?.try_into().ok()?);
    let row = u32::from_be_bytes(rest.get(16..20)?.try_into().ok()?);
    Some((cluster, seq, row))
}

/// The key of a row text `a Some((k, "v")) -> ...`.
fn key_of(what: &str) -> Option<i64> {
    let i = what.find("Some((")? + 6;
    what[i..].split(',').next()?.trim().parse().ok()
}

/// The stream invariant: per partition, the records are the verified
/// changes of `a` with that partition's keys, in commit order, at offsets
/// from 0 without gaps or duplicates, with epochs that never go down.
pub(super) fn check_stream(
    feed_seen: &[super::feed::Seen],
    log: &MemLog,
    done: bool,
) -> std::result::Result<(), String> {
    if !done {
        return Err(format!(
            "stream invariant: the sequencer didn't reach the end (position {:?})",
            position(log)
        ));
    }
    let mut want: BTreeMap<PartitionNo, Vec<(String, u64, u32)>> = BTreeMap::new();
    for s in feed_seen.iter().filter(|s| s.what.starts_with("a ")) {
        let Some(k) = key_of(&s.what) else {
            return Err(format!("stream invariant: no key in {}", s.what));
        };
        let p = partition::for_key(format!("{{\"k\":{k}}}").as_bytes(), PARTITIONS);
        want.entry(p).or_default().push(s.id.clone());
    }
    let st = log.lock();
    for (p, stored) in &st.parts {
        let offsets: Vec<u64> = stored.iter().map(|r| r.offset.0).collect();
        if offsets != (0..stored.len() as u64).collect::<Vec<_>>() {
            return Err(format!(
                "stream invariant: partition {} offsets {offsets:?}",
                p.0
            ));
        }
        if stored.windows(2).any(|w| w[1].epoch < w[0].epoch) {
            return Err(format!(
                "stream invariant: partition {}: a fenced sequencer appended after a newer one",
                p.0
            ));
        }
    }
    for p in 0..PARTITIONS {
        let p = PartitionNo(p);
        let got: Vec<(String, u64, u32)> = st
            .parts
            .get(&p)
            .map(|v| {
                v.iter()
                    .filter(|r| {
                        // DDL and truncate records aren't row changes.
                        r.record.key.is_some()
                    })
                    .filter_map(|r| id_of(&r.record.source))
                    .collect()
            })
            .unwrap_or_default();
        let exp = want.remove(&p).unwrap_or_default();
        if got != exp {
            let first = got
                .iter()
                .zip(&exp)
                .position(|(a, b)| a != b)
                .unwrap_or(got.len().min(exp.len()));
            return Err(format!(
                "stream invariant: partition {}: {} records, {} expected; first difference at {first}: got {:?}, expected {:?}",
                p.0,
                got.len(),
                exp.len(),
                got.get(first),
                exp.get(first)
            ));
        }
    }
    Ok(())
}

/// What a run did, for the record.
pub(super) fn summary(log: &MemLog, run: &Arc<Mutex<StreamRun>>) -> String {
    let r = run.lock().unwrap_or_else(PoisonError::into_inner);
    let st = log.lock();
    let n: usize = st.parts.values().map(Vec::len).sum();
    format!(
        "stream: {n} records in {} appends, epoch {}, {} kills, {} overlaps, {} fenced, {} feed errors",
        st.commits, st.epoch.0, r.kills, r.overlaps, r.fenced, r.feed_errors
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_keys() {
        let id = sprig_feed::EventId {
            cluster: sprig_common::ids::ClusterId::new("dc-a-c2").unwrap(),
            gtid: sprig_binlog::Gtid {
                domain: 2,
                server: 9,
                seq: 77,
            },
            row: 3,
        };
        assert_eq!(
            id_of(&Source::of_event(&id)),
            Some(("dc-a-c2".into(), 77, 3))
        );
        assert_eq!(key_of("a Some((5, \"x\")) -> None"), Some(5));
        assert_eq!(key_of("a None -> Some((-7, \"x\"))"), Some(-7));
    }
}
