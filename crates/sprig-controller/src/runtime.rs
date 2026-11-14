//! The reconciler runtime: sessions, leader election and the loops.
//!
//! Every controller loop is a [`Reconciler`]: level-triggered, keyed, and
//! driven by watch events plus a periodic full resync.
//!
//! ```text
//! run(loop):
//! 1. a deduplicated key queue; at most one reconcile per key at a time
//! 2. feed: watch events -> keys_for(ev); every control.resync_seconds -> all_keys()
//! 3. for k = queue.pop(): Done -> reset backoff; RequeueAfter(d) -> add_after(k, d);
//!    Blocked -> gauge, add_after(k, 30 s); Err -> add_after(k, backoff(k))
//!    backoff: 100 ms doubling to 5 min, ±20% jitter
//! ```
//!
//! [`Runtime`] hosts the loops of one controller by [`LoopTier`]:
//! - `DcAll` loops run on every controller from start to shutdown;
//! - `DcLeader` loops run only while this controller holds the DC's
//!   `leader` key, and `GlobalLeader` loops only while it holds the global
//!   `leader` key (when `[controller] global_candidate` is set). On session
//!   loss, or a guarded transaction failing its token compare, the step-down
//!   token fires and every loop of that term stops before the controller
//!   campaigns again.
//!
//! Each step of a loop takes one action recorded with a guarded
//! transaction ([`Leadership::commit`] or [`Leadership::commit_if`]), so a
//! deposed leader's late writes never commit.

pub(crate) mod queue;
#[cfg(test)]
pub(crate) mod tests;

use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::elect::{Election, Elector, LeaderRecord, Leadership};
use sprig_meta::session::SessionOpts;
use sprig_meta::{Key, KeyValue, MetaStore, Revision, WatchEvent, WatchStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

pub use queue::{BACKOFF_BASE, BACKOFF_MAX, BLOCKED_RETRY, backoff};

use crate::settings::ControlSettings;

/// Queue length between a loop's watch feeders and its queue.
const FEED_QUEUE: usize = 256;
/// Wait before re-opening a failed watch or retrying a campaign.
const RETRY_AFTER: Duration = Duration::from_millis(500);

/// Where a loop runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoopTier {
    /// On every controller.
    DcAll,
    /// On the DC leader only.
    DcLeader,
    /// On the global leader only.
    GlobalLeader,
}

/// What a reconcile asks for next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The key is as it should be; reset its backoff.
    Done,
    /// Look again after this long.
    RequeueAfter(Duration),
    /// Something outside the loop prevents progress; counted in
    /// `sprig_controller_loop_blocked` and retried after 30 s.
    Blocked {
        /// Why, for logs.
        reason: &'static str,
    },
}

/// Which tier an event or a watch belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    /// The global tier, `/sprig/g/`.
    Global,
    /// This controller's DC tier, `/sprig/dc/<dc_id>/`.
    Dc,
}

/// A change seen by a loop's watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaEvent {
    /// The tier.
    pub source: Source,
    /// The key that changed.
    pub key: Key,
    /// The new value; `None` for a deletion.
    pub kv: Option<KeyValue>,
    /// The revision of the change.
    pub mod_revision: Revision,
}

impl MetaEvent {
    /// Whether the key was deleted.
    #[must_use]
    pub fn deleted(&self) -> bool {
        self.kv.is_none()
    }

    /// The key relative to its tier's root, such as `jobs/0192…`.
    #[must_use]
    pub fn path(&self) -> &str {
        relative(self.key.as_str())
    }
}

/// Strips `/sprig/g/` or `/sprig/dc/<dc_id>/` from a key.
pub(crate) fn relative(key: &str) -> &str {
    if let Some(rest) = key.strip_prefix("/sprig/g/") {
        return rest;
    }
    key.strip_prefix("/sprig/dc/")
        .and_then(|rest| rest.split_once('/'))
        .map_or(key, |(_, rest)| rest)
}

/// A prefix a loop watches, relative to the tier's root (`jobs/`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchSpec {
    /// The tier.
    pub source: Source,
    /// The prefix under the tier's root.
    pub prefix: &'static str,
}

impl WatchSpec {
    /// A prefix of this DC's tier.
    #[must_use]
    pub const fn dc(prefix: &'static str) -> Self {
        Self {
            source: Source::Dc,
            prefix,
        }
    }

    /// A prefix of the global tier.
    #[must_use]
    pub const fn global(prefix: &'static str) -> Self {
        Self {
            source: Source::Global,
            prefix,
        }
    }
}

/// What a loop gets: the stores, the leadership it acts under, the clock,
/// the step-down token and the settings.
///
/// The balancer's decision-log writer joins this once `sprig-balancer`
/// exists.
#[derive(Clone)]
pub struct LoopCx {
    /// This DC's tier.
    pub dc: Arc<dyn MetaStore>,
    /// The global tier (a view over the DC's etcd in single-etcd mode).
    pub global: Arc<dyn MetaStore>,
    /// The leadership of this term; `None` for `DcAll` loops.
    pub leadership: Option<Leadership>,
    /// The clock every loop reads time through.
    pub clock: Arc<dyn Clock>,
    /// Cancelled when the loop must stop: step-down or shutdown.
    pub step_down: CancellationToken,
    settings: watch::Receiver<Arc<ControlSettings>>,
}

impl std::fmt::Debug for LoopCx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopCx")
            .field("leadership", &self.leadership)
            .finish_non_exhaustive()
    }
}

impl LoopCx {
    /// A context over `dc` and `global`.
    #[must_use]
    pub fn new(
        dc: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        leadership: Option<Leadership>,
        clock: Arc<dyn Clock>,
        step_down: CancellationToken,
        settings: watch::Receiver<Arc<ControlSettings>>,
    ) -> Self {
        Self {
            dc,
            global,
            leadership,
            clock,
            step_down,
            settings,
        }
    }

    /// The current settings snapshot.
    #[must_use]
    pub fn settings(&self) -> Arc<ControlSettings> {
        Arc::clone(&self.settings.borrow())
    }

    /// The leadership this loop acts under.
    ///
    /// # Errors
    /// 39504 `ER_SPRIG_NOT_LEADER` for a `DcAll` loop, or once leadership
    /// is lost.
    pub fn leader(&self) -> Result<&Leadership> {
        match &self.leadership {
            Some(l) if l.is_leading() => Ok(l),
            _ => Err(SprigError::new(SprigCode::NotLeader, "not the leader")),
        }
    }

    /// The store of `source`.
    #[must_use]
    pub fn store(&self, source: Source) -> &Arc<dyn MetaStore> {
        match source {
            Source::Dc => &self.dc,
            Source::Global => &self.global,
        }
    }
}

/// One controller loop.
#[async_trait::async_trait]
pub trait Reconciler: Send + Sync + 'static {
    /// What the loop reconciles: a cluster id, a job id, or a unit key for
    /// loops that look at everything at once.
    type Key: Clone + Eq + Hash + Display + Send + Sync + 'static;
    /// The metric label `loop`.
    fn name(&self) -> &'static str;
    /// Where the loop runs.
    fn tier(&self) -> LoopTier;
    /// The prefixes whose events feed [`Self::keys_for`]; the runtime
    /// needs them to open the watches.
    fn watches(&self) -> Vec<WatchSpec> {
        Vec::new()
    }
    /// The keys an event touches.
    fn keys_for(&self, ev: &MetaEvent) -> Vec<Self::Key>;
    /// Every key, for the start of a term and each resync.
    async fn all_keys(&self, cx: &LoopCx) -> Result<Vec<Self::Key>>;
    /// Brings one key to its desired state, one guarded action at a time.
    async fn reconcile(&self, cx: &LoopCx, key: &Self::Key) -> Result<Outcome>;
    /// How many keys may be reconciled at once (each key still at most
    /// once). One unless a loop says otherwise.
    fn workers(&self) -> usize {
        1
    }
}

/// A [`Reconciler`] with its key type erased, so one runtime hosts many.
#[derive(Clone)]
pub struct Loop {
    inner: Arc<dyn ErasedLoop>,
}

impl std::fmt::Debug for Loop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Loop")
            .field("name", &self.name())
            .field("tier", &self.tier())
            .finish()
    }
}

#[async_trait::async_trait]
trait ErasedLoop: Send + Sync {
    fn name(&self) -> &'static str;
    fn tier(&self) -> LoopTier;
    async fn run(&self, cx: LoopCx);
}

struct Typed<R>(Arc<R>);

#[async_trait::async_trait]
impl<R: Reconciler> ErasedLoop for Typed<R> {
    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn tier(&self) -> LoopTier {
        self.0.tier()
    }
    async fn run(&self, cx: LoopCx) {
        run_loop(Arc::clone(&self.0), cx).await;
    }
}

impl Loop {
    /// Wraps a reconciler.
    #[must_use]
    pub fn new<R: Reconciler>(r: R) -> Self {
        Self::from_arc(Arc::new(r))
    }

    /// Wraps a shared reconciler.
    #[must_use]
    pub fn from_arc<R: Reconciler>(r: Arc<R>) -> Self {
        Self {
            inner: Arc::new(Typed(r)),
        }
    }

    /// The loop's name.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.inner.name()
    }

    /// Where the loop runs.
    #[must_use]
    pub fn tier(&self) -> LoopTier {
        self.inner.tier()
    }

    /// Runs the loop until `cx.step_down` is cancelled. In-flight
    /// reconciles are aborted then: each step is one atomic transaction.
    pub async fn run(&self, cx: LoopCx) {
        self.inner.run(cx).await;
    }
}

/// What the watch feeders send a loop.
enum Feed {
    Event(MetaEvent),
    /// Events may have been missed (compaction, or a stream that ended
    /// before its first event): list everything again.
    Resync,
}

/// A seed per loop name, so jitter replays exactly.
fn seed(name: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    h.finish()
}

async fn run_loop<R: Reconciler>(r: Arc<R>, cx: LoopCx) {
    let name = r.name();
    let (tx, mut rx) = mpsc::channel::<Feed>(FEED_QUEUE);
    // Watches open before the first listing, so a write in between is seen.
    if !start_feeds(name, &r.watches(), &cx, &tx).await {
        return;
    }
    let depth = metrics::gauge!("sprig_controller_queue_depth", "loop" => name);
    let blocked = metrics::gauge!("sprig_controller_loop_blocked", "loop" => name);
    let took = metrics::histogram!("sprig_controller_reconcile_duration_seconds", "loop" => name);

    let mut q = queue::WorkQueue::new(seed(name));
    let mut resync_due = Instant::now();
    let mut resync_failures = 0u32;
    let mut inflight: JoinSet<(R::Key, Result<Outcome>, Duration)> = JoinSet::new();
    tracing::debug!(%name, "loop started");
    loop {
        if Instant::now() >= resync_due {
            let listed = tokio::select! {
                () = cx.step_down.cancelled() => break,
                r = r.all_keys(&cx) => r,
            };
            match listed {
                Ok(keys) => {
                    resync_failures = 0;
                    for k in keys {
                        q.add(k);
                    }
                    resync_due = Instant::now() + cx.settings().resync;
                }
                Err(e) => {
                    resync_failures = resync_failures.saturating_add(1);
                    tracing::warn!(%name, error = %e, "listing keys failed");
                    resync_due = Instant::now() + backoff(resync_failures);
                }
            }
        }
        let workers = r.workers().max(1);
        while inflight.len() < workers {
            let Some(k) = q.pop() else { break };
            let (r, cx) = (Arc::clone(&r), cx.clone());
            inflight.spawn(async move {
                let start = Instant::now();
                let out = AssertUnwindSafe(r.reconcile(&cx, &k))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(SprigError::internal("reconcile panicked")));
                (k, out, start.elapsed())
            });
        }
        #[allow(clippy::cast_precision_loss)] // queue lengths are small
        {
            depth.set(q.depth() as f64);
            blocked.set(q.blocked() as f64);
        }
        let due = q.next_due();
        tokio::select! {
            () = cx.step_down.cancelled() => break,
            Some(feed) = rx.recv() => match feed {
                Feed::Event(ev) => {
                    for k in r.keys_for(&ev) {
                        q.add(k);
                    }
                }
                Feed::Resync => resync_due = Instant::now(),
            },
            () = tokio::time::sleep_until(resync_due) => {}
            () = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                q.promote_due(Instant::now());
            }
            Some(done) = inflight.join_next(), if !inflight.is_empty() => {
                let Ok((k, out, elapsed)) = done else { continue };
                took.record(elapsed.as_secs_f64());
                log_outcome(name, &k, &out);
                q.done(k, out.as_ref().map_err(|_| ()), Instant::now());
            }
        }
    }
    inflight.abort_all();
    while inflight.join_next().await.is_some() {}
    depth.set(0.0);
    blocked.set(0.0);
    tracing::debug!(%name, "loop stopped");
}

/// Opens a feeder per watched prefix; `false` if `cx.step_down` fired first.
async fn start_feeds(
    name: &str,
    specs: &[WatchSpec],
    cx: &LoopCx,
    tx: &mpsc::Sender<Feed>,
) -> bool {
    for spec in specs {
        let store = Arc::clone(cx.store(spec.source));
        let prefix = match store.tier().key(spec.prefix) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(%name, prefix = spec.prefix, error = %e, "bad watch prefix");
                continue;
            }
        };
        let Some(first) = open_watch(&*store, &prefix, 0, &cx.step_down).await else {
            return false;
        };
        tokio::spawn(feed(
            store,
            spec.source,
            prefix,
            first,
            tx.clone(),
            cx.step_down.clone(),
        ));
    }
    true
}

fn log_outcome(name: &str, k: &dyn Display, out: &Result<Outcome>) {
    match out {
        Ok(Outcome::Blocked { reason }) => {
            tracing::info!(%name, key = %k, reason, "blocked");
        }
        Err(e) if e.code() == Some(SprigCode::NotLeader) => {
            tracing::info!(%name, key = %k, "leadership lost during reconcile");
        }
        Err(e) => tracing::warn!(%name, key = %k, error = %e, "reconcile failed"),
        Ok(_) => {}
    }
}

/// Opens a watch, retrying until it opens or `stop` fires.
async fn open_watch(
    store: &dyn MetaStore,
    prefix: &Key,
    from: Revision,
    stop: &CancellationToken,
) -> Option<WatchStream> {
    loop {
        let opened = tokio::select! {
            () = stop.cancelled() => return None,
            w = store.watch(prefix, from) => w,
        };
        match opened {
            Ok(w) => return Some(w),
            Err(e) => {
                tracing::debug!(%prefix, error = %e, "watch failed to open; retrying");
                if !sleep_or_stop(RETRY_AFTER, stop).await {
                    return None;
                }
            }
        }
    }
}

/// Forwards one prefix's events to a loop, resuming after errors.
async fn feed(
    store: Arc<dyn MetaStore>,
    source: Source,
    prefix: Key,
    mut w: WatchStream,
    tx: mpsc::Sender<Feed>,
    stop: CancellationToken,
) {
    let mut last: Revision = 0;
    loop {
        let compacted = loop {
            let ev = tokio::select! {
                () = stop.cancelled() => return,
                ev = w.next() => ev,
            };
            let out = match ev {
                Some(Ok(WatchEvent::Put(kv))) => {
                    last = kv.mod_revision;
                    MetaEvent {
                        source,
                        key: kv.key.clone(),
                        mod_revision: kv.mod_revision,
                        kv: Some(kv),
                    }
                }
                Some(Ok(WatchEvent::Delete { key, mod_revision })) => {
                    last = mod_revision;
                    MetaEvent {
                        source,
                        key,
                        kv: None,
                        mod_revision,
                    }
                }
                Some(Ok(WatchEvent::Progress(rev))) => {
                    last = last.max(rev);
                    continue;
                }
                Some(Ok(WatchEvent::Compacted { .. })) => break true,
                Some(Err(_)) | None => break false,
            };
            tokio::select! {
                () = stop.cancelled() => return,
                sent = tx.send(Feed::Event(out)) => if sent.is_err() { return },
            }
        };
        let from = if compacted || last == 0 { 0 } else { last + 1 };
        if from == 0 {
            tokio::select! {
                () = stop.cancelled() => return,
                sent = tx.send(Feed::Resync) => if sent.is_err() { return },
            }
        }
        match open_watch(&*store, &prefix, from, &stop).await {
            Some(next) => w = next,
            None => return,
        }
    }
}

/// Sleeps for `d`; `false` if `stop` fired first.
async fn sleep_or_stop(d: Duration, stop: &CancellationToken) -> bool {
    tokio::select! {
        () = stop.cancelled() => false,
        () = tokio::time::sleep(d) => true,
    }
}

/// The stores a controller uses.
#[derive(Clone)]
pub struct Stores {
    /// This DC's tier.
    pub dc: Arc<dyn MetaStore>,
    /// The global tier.
    pub global: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for Stores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stores")
            .field("dc", self.dc.tier())
            .field("global", self.global.tier())
            .finish()
    }
}

/// Which leadership a controller holds, by fencing token.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Status {
    /// The DC leader token, while leading the DC.
    pub dc_leader: Option<Revision>,
    /// The global leader token, while leading the global tier.
    pub global_leader: Option<Revision>,
}

/// Which leader key a term contends for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Term {
    Dc,
    Global,
}

impl Term {
    fn label(self) -> &'static str {
        match self {
            Term::Dc => "dc",
            Term::Global => "global",
        }
    }
}

/// Hosts one controller's loops: `DcAll` loops all the time, leader loops
/// only during leadership terms.
pub struct Runtime {
    stores: Stores,
    clock: Arc<dyn Clock>,
    me: LeaderRecord,
    loops: Vec<Loop>,
    settings: watch::Receiver<Arc<ControlSettings>>,
    global_candidate: bool,
    status: watch::Sender<Status>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("me", &self.me)
            .field("loops", &self.loops)
            .field("global_candidate", &self.global_candidate)
            .finish_non_exhaustive()
    }
}

impl Runtime {
    /// A runtime with no loops yet, on the built-in settings.
    #[must_use]
    pub fn new(stores: Stores, clock: Arc<dyn Clock>, me: LeaderRecord) -> Self {
        let (_, settings) = watch::channel(Arc::new(ControlSettings::default()));
        Self {
            stores,
            clock,
            me,
            loops: Vec::new(),
            settings,
            global_candidate: false,
            status: watch::channel(Status::default()).0,
        }
    }

    /// Adds a loop.
    #[must_use]
    pub fn with_loop(mut self, l: Loop) -> Self {
        self.loops.push(l);
        self
    }

    /// Follows `settings` for resync periods, lease TTLs and limits; a new
    /// lease TTL applies at the next campaign.
    #[must_use]
    pub fn with_settings(mut self, settings: watch::Receiver<Arc<ControlSettings>>) -> Self {
        self.settings = settings;
        self
    }

    /// Whether to campaign for the global leader too.
    #[must_use]
    pub fn global_candidate(mut self, yes: bool) -> Self {
        self.global_candidate = yes;
        self
    }

    /// Follows which leadership this controller holds.
    #[must_use]
    pub fn status(&self) -> watch::Receiver<Status> {
        self.status.subscribe()
    }

    fn cx(&self, leadership: Option<Leadership>, step_down: CancellationToken) -> LoopCx {
        LoopCx::new(
            Arc::clone(&self.stores.dc),
            Arc::clone(&self.stores.global),
            leadership,
            Arc::clone(&self.clock),
            step_down,
            self.settings.clone(),
        )
    }

    fn loops_of(&self, tier: LoopTier) -> Vec<Loop> {
        self.loops
            .iter()
            .filter(|l| l.tier() == tier)
            .cloned()
            .collect()
    }

    /// Runs until `shutdown` is cancelled; a leader resigns on the way out
    /// so a successor needn't wait for the lease to expire.
    pub async fn run(self, shutdown: CancellationToken) {
        let all = self.loops_of(LoopTier::DcAll);
        let mut always = JoinSet::new();
        for l in all {
            let cx = self.cx(None, shutdown.child_token());
            always.spawn(async move { l.run(cx).await });
        }
        let dc = self.lead(Term::Dc, self.loops_of(LoopTier::DcLeader), &shutdown);
        let global = async {
            if self.global_candidate {
                self.lead(
                    Term::Global,
                    self.loops_of(LoopTier::GlobalLeader),
                    &shutdown,
                )
                .await;
            }
        };
        tokio::join!(dc, global);
        while always.join_next().await.is_some() {}
    }

    /// Campaigns, runs `loops` while leading, and campaigns again after a
    /// step-down, until shutdown.
    async fn lead(&self, term: Term, loops: Vec<Loop>, shutdown: &CancellationToken) {
        let store = match term {
            Term::Dc => Arc::clone(&self.stores.dc),
            Term::Global => Arc::clone(&self.stores.global),
        };
        let key = match store.tier().key("leader") {
            Ok(k) => k,
            Err(e) => {
                tracing::error!(error = %e, "can't build the leader key");
                return;
            }
        };
        let tier = term.label();
        loop {
            let ttl = match term {
                Term::Dc => self.settings.borrow().leader_lease,
                Term::Global => self.settings.borrow().global_leader_lease,
            };
            let election = Election::new(
                Arc::clone(&store),
                Arc::clone(&self.clock),
                SessionOpts::new(ttl),
            );
            let won = tokio::select! {
                () = shutdown.cancelled() => return,
                r = election.campaign(&key, &self.me) => r,
            };
            let l = match won {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!(tier, error = %e, "campaign failed; retrying");
                    if !sleep_or_stop(RETRY_AFTER, shutdown).await {
                        return;
                    }
                    continue;
                }
            };
            metrics::counter!("sprig_controller_leader_changes_total", "tier" => tier).increment(1);
            tracing::info!(tier, token = l.token, holder = %self.me.holder, "leading");
            self.status.send_modify(|s| match term {
                Term::Dc => s.dc_leader = Some(l.token),
                Term::Global => s.global_leader = Some(l.token),
            });
            let step_down = shutdown.child_token();
            let mut running = JoinSet::new();
            for lp in &loops {
                let (lp, cx) = (lp.clone(), self.cx(Some(l.clone()), step_down.clone()));
                running.spawn(async move { lp.run(cx).await });
            }
            let stopping = tokio::select! {
                () = l.stepped_down() => false,
                () = shutdown.cancelled() => true,
            };
            step_down.cancel();
            while running.join_next().await.is_some() {}
            self.status.send_modify(|s| match term {
                Term::Dc => s.dc_leader = None,
                Term::Global => s.global_leader = None,
            });
            if stopping {
                if let Err(e) = l.resign().await {
                    tracing::warn!(tier, error = %e, "resign failed; the lease ends on its TTL");
                }
                tracing::info!(tier, "resigned");
                return;
            }
            tracing::warn!(
                tier,
                token = l.token,
                "lost leadership; stopped its loops, campaigning again"
            );
        }
    }
}
