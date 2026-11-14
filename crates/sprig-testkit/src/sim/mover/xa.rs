//! Prepared distributed-transaction branches on the source while the
//! tenant moves (the cutover drains them at steps 2b and 3b).
//!
//! A coordinator client prepares single-row branches on the source (only
//! while the map points there, and never on a fenced source), then
//! commits or aborts them at once, crashes and leaves them in doubt (half
//! of those after recording a commit; 25% of transactions), or stalls
//! with its live connection owning the branch for 0.5 to 4 s (3%: a
//! stalled gateway, which no drain can finish). The controller's resolver finishes
//! branches older than [`RESOLVER_MIN_AGE`]. Decisions are an insert race:
//! the first one recorded wins.
//!
//! The model is adversarial about the commit: an `XA COMMIT` lands on the
//! source whatever the fence, as MariaDB's does (a prepared branch's
//! changes are already in `InnoDB`; renames and revokes don't stop it). A
//! prepared branch also holds its table's metadata lock, so the rename
//! fence fails while one exists (measured on 11.8: `RENAME TABLE` waits
//! for a disconnected prepared branch until `lock_wait_timeout`); a kill
//! fence (`kill_fence`) succeeds and leaves the branch prepared.
//!
//! A commit that lands after the cutover read `G` is lost to the target:
//! invariant 1 (and 3) catch it. With the drain (steps 2b and 3b) no
//! branch is left prepared when `G` is final.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_common::ids::ClusterId;
use sprig_mover::Drained;
use sprig_shard::{Placement, TenantMaps};
use tokio::sync::Notify;

use super::{Db, Entry, TENANT, World, lock};

/// Branches the resolver leaves to their coordinator.
const RESOLVER_MIN_AGE: Duration = Duration::from_secs(5);
/// Keys of the coordinator's rows start here (clients write below).
const KEY_BASE: i64 = 100_000;

/// Totals over every seed of a run, for the test's summary.
static PREPARED: AtomicU64 = AtomicU64::new(0);
static DRAINED_COMMIT: AtomicU64 = AtomicU64::new(0);
static DRAINED_ROLLBACK: AtomicU64 = AtomicU64::new(0);
static DRAINS_SHORT: AtomicU64 = AtomicU64::new(0);
static FENCES_REFUSED: AtomicU64 = AtomicU64::new(0);

/// What the XA variant did over every seed so far: branches prepared,
/// committed and rolled back by cutover drains, drains that ran out of
/// time with branches left (the attempt aborted), and rename fences
/// refused by a prepared branch's lock.
#[must_use]
pub fn totals() -> (u64, u64, u64, u64, u64) {
    (
        PREPARED.load(Ordering::Relaxed),
        DRAINED_COMMIT.load(Ordering::Relaxed),
        DRAINED_ROLLBACK.load(Ordering::Relaxed),
        DRAINS_SHORT.load(Ordering::Relaxed),
        FENCES_REFUSED.load(Ordering::Relaxed),
    )
}

/// How the source treats prepared branches in one seed.
#[derive(Debug, Clone, Copy)]
pub(super) struct XaMode {
    /// The fence succeeds with branches prepared (revoke and kill); else
    /// it is the rename fence, which their locks refuse.
    pub kill_fence: bool,
    /// The source drains through a resolver at cutover; `false` is the
    /// behaviour before the drain (the default `SourceIo` method).
    pub drain: bool,
}

/// One prepared branch.
#[derive(Debug, Clone)]
pub(super) struct Branch {
    id: u64,
    table: String,
    k: i64,
    v: String,
    prepared_at: Duration,
    /// A live connection owns it until then: `XA COMMIT` from elsewhere
    /// fails (1397).
    owned_until: Duration,
}

/// The source's prepared branches and the decisions.
#[derive(Debug, Default)]
pub(super) struct XaState {
    prepared: Vec<Branch>,
    decisions: BTreeMap<u64, bool>,
}

impl XaState {
    /// Whether a prepared branch holds a metadata lock on the shard's
    /// tables (every table of the model moves).
    pub(super) fn locked(&self) -> bool {
        !self.prepared.is_empty()
    }

    /// Branches still prepared.
    pub(super) fn left(&self) -> usize {
        self.prepared.len()
    }
}

/// Counts a rename fence refused by a prepared branch.
pub(super) fn fence_refused() {
    FENCES_REFUSED.fetch_add(1, Ordering::Relaxed);
}

/// The insert race: the first decision recorded wins.
fn decide(d: &mut Db, id: u64, want: bool) -> bool {
    *d.xa.decisions.entry(id).or_insert(want)
}

/// `XA COMMIT` or `XA ROLLBACK` of branch `id` by its decision (abort if
/// none is recorded yet); `None` if it isn't prepared any more.
fn finish(d: &mut Db, id: u64, at: Duration) -> Option<bool> {
    let i = d.xa.prepared.iter().position(|b| b.id == id)?;
    let b = d.xa.prepared.remove(i);
    let commit = decide(d, id, false);
    if commit {
        let before = d.src.get(&b.table).and_then(|r| r.get(&b.k)).cloned();
        d.src
            .entry(b.table.clone())
            .or_default()
            .insert(b.k, b.v.clone());
        d.versions.insert((b.table.clone(), b.k, b.v.clone()));
        d.log(Entry::Rows(vec![(
            b.table.clone(),
            before.map(|x| (b.k, x)),
            Some((b.k, b.v.clone())),
        )]));
        d.acked.insert((b.table, b.k), Some(b.v));
        d.last_src_ack = Some(at);
    }
    Some(commit)
}

/// The resolver's drain (`drain_prepared`): every branch
/// not owned by a live connection is finished now; owned ones are retried
/// until `within` passes, then reported as remaining.
pub(super) async fn drain(
    db: &Arc<Mutex<Db>>,
    notify: &Notify,
    world: &World,
    within: Duration,
) -> Drained {
    let end = world.elapsed() + within;
    let mut out = Drained::default();
    loop {
        let now = world.elapsed();
        {
            let mut d = lock(db);
            let ids: Vec<u64> =
                d.xa.prepared
                    .iter()
                    .filter(|b| b.owned_until <= now)
                    .map(|b| b.id)
                    .collect();
            for id in ids {
                match finish(&mut d, id, now) {
                    Some(true) => {
                        out.committed += 1;
                        DRAINED_COMMIT.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(false) => {
                        out.rolled_back += 1;
                        DRAINED_ROLLBACK.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {}
                }
            }
            out.remaining = d.xa.left();
        }
        if out.finished_any() {
            notify.notify_waiters();
        }
        if out.remaining == 0 || world.elapsed() >= end {
            if out.remaining > 0 {
                DRAINS_SHORT.fetch_add(1, Ordering::Relaxed);
            }
            world.record(format!(
                "drain: {} committed, {} rolled back, {} left",
                out.committed, out.rolled_back, out.remaining
            ));
            return out;
        }
        tokio::time::sleep(Duration::from_millis(100).min(within)).await;
    }
}

/// A coordinator client on the source (module docs).
#[allow(clippy::too_many_arguments)]
pub(super) async fn coordinator(
    world: World,
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    maps: TenantMaps,
    tables: Vec<String>,
    stop: CancellationToken,
    src: ClusterId,
) {
    let mut n = 0u64;
    while !stop.is_cancelled() {
        tokio::time::sleep(
            world
                .rng()
                .duration(Duration::from_millis(50), Duration::from_millis(400)),
        )
        .await;
        let Ok((m, _)) = maps.load(TENANT).await else {
            continue;
        };
        if Placement::of(&m).ok().is_none_or(|p| p.cluster != src) {
            continue;
        }
        n += 1;
        let id = n;
        let fate = world.rng().below(100);
        let now = world.elapsed();
        let owned_until = if fate >= 97 {
            now + world
                .rng()
                .duration(Duration::from_millis(500), Duration::from_secs(4))
        } else {
            Duration::ZERO
        };
        let b = Branch {
            id,
            table: world.rng().pick(&tables).clone(),
            k: KEY_BASE + i64::try_from(id).unwrap_or(0),
            v: format!("xa-{id}"),
            prepared_at: now,
            owned_until,
        };
        {
            let mut d = lock(&db);
            // A fenced source has no table to write (rename) or no session
            // (kill): nothing new prepares.
            if d.fenced || d.dropped {
                continue;
            }
            d.xa.prepared.push(b);
        }
        PREPARED.fetch_add(1, Ordering::Relaxed);
        world.record(format!("xa {id} prepared"));
        if fate < 72 {
            // Commits (or aborts) at once.
            let want = world.rng().chance(0.85);
            let pause = world
                .rng()
                .duration(Duration::from_millis(1), Duration::from_millis(30));
            let (w, db, nt) = (world.clone(), db.clone(), notify.clone());
            tokio::spawn(async move {
                tokio::time::sleep(pause).await;
                let mut d = lock(&db);
                decide(&mut d, id, want);
                if finish(&mut d, id, w.elapsed()).is_some() {
                    nt.notify_waiters();
                }
            });
        } else if fate < 97 {
            // Crashes after the prepare: in doubt, half after recording a
            // commit.
            if world.rng().chance(0.5) {
                decide(&mut lock(&db), id, true);
            }
            world.record(format!("xa {id} coordinator crashed"));
        } else {
            // Decided commit, then stalls holding the branch.
            decide(&mut lock(&db), id, true);
            let (w, db, nt) = (world.clone(), db.clone(), notify.clone());
            tokio::spawn(async move {
                tokio::time::sleep(owned_until.saturating_sub(w.elapsed())).await;
                if finish(&mut lock(&db), id, w.elapsed()).is_some() {
                    nt.notify_waiters();
                }
            });
        }
    }
}

/// The controller's resolver loop: branches older than
/// [`RESOLVER_MIN_AGE`] that no live connection owns are finished.
pub(super) async fn resolver(
    world: World,
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    stop: CancellationToken,
) {
    while !stop.is_cancelled() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = world.elapsed();
        let mut d = lock(&db);
        let ids: Vec<u64> =
            d.xa.prepared
                .iter()
                .filter(|b| b.prepared_at + RESOLVER_MIN_AGE <= now && b.owned_until <= now)
                .map(|b| b.id)
                .collect();
        let mut any = false;
        for id in ids {
            any |= finish(&mut d, id, now).is_some();
        }
        drop(d);
        if any {
            notify.notify_waiters();
        }
    }
}
