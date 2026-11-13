//! Large requests off the runtime's worker threads (chapter 04, "Large
//! statements"; decision gwhard-1).
//!
//! Lexing, name virtualization, the translator's rewrites, router planning
//! and workload fingerprinting all scan a statement's whole text, more than
//! once. For a 16 MiB statement that held a tokio worker for over 2 s in a
//! debug build, and chapter 25's watchdog, which takes a runtime that
//! stops answering for that long for a stuck process, aborted the gateway.
//! One client must never be able to do that.
//!
//! So a request at least `gateway.large_statement_bytes` long (1 MiB) runs
//! on a blocking thread: [`run`] calls `tokio::task::block_in_place`,
//! which hands the worker's queue to another thread, and drives the
//! request's future to its end on the session's own thread. Everything the
//! request does (its CPU work, its backend I/O, `KILL QUERY` and the client
//! hanging up, which the front end watches in the same future) stays in
//! order, and other sessions keep the worker threads. At most
//! [`MAX_CONCURRENT`] large requests run at once; more wait their turn
//! asynchronously, which also bounds the memory large statements hold.
//!
//! Outside a multi-threaded runtime (unit tests) the future runs as is.
//!
//! **Heavy statements of any size** (decision bugfix-1, after G40). A
//! short statement can be heavy too: a cross-shard sort, an analytic read,
//! a large result's encoding. Every request through [`run_for`] is timed
//! poll by poll ([`Guarded`]): once its own polls used [`SHED_AFTER`] of a
//! worker's time, the rest of it runs as a large request does
//! (`block_in_place`), at most [`shed_slots`] at once; a poll of
//! [`LONG_POLL`] or more is logged with the session and counted. While a
//! poll runs, its thread's slot names the session, so when the watchdog
//! sees the runtime stuck ([`on_stall`]) it logs the statements holding a
//! worker and stops them as `KILL QUERY` would. The gateway never aborts
//! for a stall.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use sprig_settings::{Apply, HardBounds, Scopes, Setting};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Semaphore;

/// `gateway.large_statement_bytes`.
pub static LARGE_STATEMENT_BYTES: Setting<u64> = Setting {
    key: "gateway.large_statement_bytes",
    default: 1_048_576,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(16_384, 1 << 30),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 4,
    doc: "Requests this big or bigger run on a blocking thread",
};
sprig_settings::register!(LARGE_STATEMENT_BYTES);

/// Large requests that run at once in one gateway; more wait.
pub const MAX_CONCURRENT: usize = 4;

fn permits() -> &'static Semaphore {
    static P: OnceLock<Semaphore> = OnceLock::new();
    P.get_or_init(|| Semaphore::new(MAX_CONCURRENT))
}

/// Whether a request of `size` bytes runs on a blocking thread with the
/// threshold `threshold`.
#[must_use]
pub fn is_large(size: usize, threshold: usize) -> bool {
    size >= threshold
}

/// The threshold the last [`run`] was given, for [`cpu`].
static THRESHOLD: AtomicUsize = AtomicUsize::new(1_048_576);

fn multi_thread() -> bool {
    Handle::try_current().is_ok_and(|h| h.runtime_flavor() == RuntimeFlavor::MultiThread)
}

/// Runs `fut`: guarded ([`Guarded`]) when `size` is under `threshold`,
/// else on a blocking thread (see the module notes).
pub async fn run<F: Future>(size: usize, threshold: usize, fut: F) -> F::Output {
    run_inner(size, threshold, None, fut).await
}

/// [`run`] for a session's request: a stalled runtime names the session,
/// and [`on_stall`] stops its statement.
pub async fn run_for<F: Future>(
    who: &Arc<crate::session::SessionHandle>,
    size: usize,
    threshold: usize,
    fut: F,
) -> F::Output {
    let who: Arc<dyn Stoppable> = Arc::clone(who) as Arc<dyn Stoppable>;
    run_inner(size, threshold, Some(who), fut).await
}

async fn run_inner<F: Future>(
    size: usize,
    threshold: usize,
    who: Option<Arc<dyn Stoppable>>,
    fut: F,
) -> F::Output {
    THRESHOLD.store(threshold, Ordering::Relaxed);
    if !multi_thread() {
        return fut.await;
    }
    if !is_large(size, threshold) {
        return Guarded::new(who, fut).await;
    }
    // A closed semaphore can't happen (it is never closed); run anyway.
    let _permit = permits().acquire().await.ok();
    metrics::counter!("sprig_gateway_large_requests_total").increment(1);
    let started = std::time::Instant::now();
    let out = tokio::task::block_in_place(|| Handle::current().block_on(fut));
    metrics::histogram!("sprig_gateway_large_request_seconds")
        .record(started.elapsed().as_secs_f64());
    out
}

/// Runs the CPU-bound `f` over `size` bytes of one large item that isn't a
/// request (a result row the name rewrite must decode): on a blocking
/// thread when `size` reaches the threshold, else in place.
pub fn cpu<R>(size: usize, f: impl FnOnce() -> R) -> R {
    if is_large(size, THRESHOLD.load(Ordering::Relaxed)) && multi_thread() {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

// ----- graph statements (R8 round 10, decision G80) -------------------------

/// How long a graph statement waits for one of [`graph_slots`] before it
/// runs without one.
pub const GRAPH_SLOT_WAIT: Duration = Duration::from_secs(1);

/// Graph statements on blocking threads at once: the CPUs, at least 4.
/// More wait up to [`GRAPH_SLOT_WAIT`] for one, then run anyway: a slot
/// can be held by a statement waiting on a lock another one holds.
pub fn graph_slots() -> &'static Arc<Semaphore> {
    static S: OnceLock<Arc<Semaphore>> = OnceLock::new();
    S.get_or_init(|| {
        let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
        Arc::new(Semaphore::new(cpus.max(4)))
    })
}

/// Runs a graph statement (Bolt, R8 round 10, decision G80) on a
/// blocking thread from its first poll: matching, sorting and
/// aggregating a large graph result are CPU work between awaits that
/// can last seconds, and on a runtime worker they held every other
/// session on it (the official LDBC run's IC9: polls of 0.9-2.9 s, then
/// the watchdog). `block_in_place` hands the worker's queue to another
/// thread; the statement's I/O, timers and `KILL QUERY` still work, in
/// order, on this one. Waits asynchronously for one of [`graph_slots`]
/// (at most [`GRAPH_SLOT_WAIT`]) first. Outside a multi-threaded runtime
/// the future runs as is.
pub async fn run_graph<F: Future>(fut: F) -> F::Output {
    if !multi_thread() {
        return fut.await;
    }
    let permit = tokio::time::timeout(GRAPH_SLOT_WAIT, Arc::clone(graph_slots()).acquire_owned())
        .await
        .ok()
        .and_then(std::result::Result::ok);
    if permit.is_none() {
        metrics::counter!("sprig_gateway_graph_unslotted_total").increment(1);
    }
    metrics::counter!("sprig_gateway_graph_offloaded_total").increment(1);
    let out = tokio::task::block_in_place(|| Handle::current().block_on(fut));
    drop(permit);
    out
}

/// [`run_graph`] for a streamed result's producer, which waits for the
/// client to pull between rows: on a blocking thread too, but without a
/// slot, so a client that reads slowly never holds one.
pub async fn run_graph_stream<F: Future>(fut: F) -> F::Output {
    if !multi_thread() {
        return fut.await;
    }
    metrics::counter!("sprig_gateway_graph_offloaded_total").increment(1);
    tokio::task::block_in_place(|| Handle::current().block_on(fut))
}

// ----- the stall guard (decision bugfix-1) ---------------------------------

/// A request whose own polls used this much of a worker's time runs the
/// rest of its way on a blocking thread.
pub const SHED_AFTER: Duration = Duration::from_millis(100);

/// One poll this long is logged and counted
/// (`sprig_gateway_long_polls_total`).
pub const LONG_POLL: Duration = Duration::from_millis(250);

/// Requests moved to blocking threads that run at once: the CPUs, at
/// least 4. More keep polling on the workers.
pub fn shed_slots() -> &'static Arc<Semaphore> {
    static S: OnceLock<Arc<Semaphore>> = OnceLock::new();
    S.get_or_init(|| {
        let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
        Arc::new(Semaphore::new(cpus.max(4)))
    })
}

/// What a stalled runtime can name and stop: a session's statement.
pub trait Stoppable: Send + Sync {
    /// Stops the running statement (`KILL QUERY`).
    fn stop(&self);
    /// Who it is, for the log.
    fn describe(&self) -> String;
}

impl Stoppable for crate::session::SessionHandle {
    fn stop(&self) {
        self.flags().kill_query();
    }

    fn describe(&self) -> String {
        format!("{} session {}", self.protocol().as_str(), self.id().0)
    }
}

/// Microseconds since the process's first guarded poll (0 is "not polling").
fn now_us() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = *START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX) + 1
}

/// One thread's poll in progress.
#[derive(Default)]
struct Slot {
    since_us: AtomicU64,
    who: Mutex<Option<Arc<dyn Stoppable>>>,
}

fn slots() -> &'static Mutex<Vec<Weak<Slot>>> {
    static S: OnceLock<Mutex<Vec<Weak<Slot>>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(Vec::new()))
}

thread_local! {
    static SLOT: Arc<Slot> = {
        let s = Arc::new(Slot::default());
        let mut all = slots().lock().unwrap_or_else(PoisonError::into_inner);
        all.retain(|w| w.strong_count() > 0);
        all.push(Arc::downgrade(&s));
        s
    };
}

/// A request's future, timed poll by poll (see the module notes).
pub struct Guarded<F> {
    fut: Pin<Box<F>>,
    who: Option<Arc<dyn Stoppable>>,
    busy: Duration,
}

impl<F: Future> Guarded<F> {
    /// `fut`, guarded; `who` is named and stopped on a stall.
    pub fn new(who: Option<Arc<dyn Stoppable>>, fut: F) -> Self {
        Self {
            fut: Box::pin(fut),
            who,
            busy: Duration::ZERO,
        }
    }
}

impl<F: Future> Future for Guarded<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let me = self.get_mut();
        if me.busy >= SHED_AFTER
            && multi_thread()
            && let Ok(_slot) = Arc::clone(shed_slots()).try_acquire_owned()
        {
            metrics::counter!("sprig_gateway_shed_requests_total").increment(1);
            let fut = me.fut.as_mut();
            return Poll::Ready(tokio::task::block_in_place(|| {
                Handle::current().block_on(fut)
            }));
        }
        let started = Instant::now();
        let prev = SLOT.with(|s| {
            let prev_who = std::mem::replace(
                &mut *s.who.lock().unwrap_or_else(PoisonError::into_inner),
                me.who.clone(),
            );
            (s.since_us.swap(now_us(), Ordering::SeqCst), prev_who)
        });
        let r = me.fut.as_mut().poll(cx);
        SLOT.with(|s| {
            *s.who.lock().unwrap_or_else(PoisonError::into_inner) = prev.1;
            s.since_us.store(prev.0, Ordering::SeqCst);
        });
        let took = started.elapsed();
        me.busy += took;
        if took >= LONG_POLL {
            metrics::counter!("sprig_gateway_long_polls_total").increment(1);
            tracing::warn!(
                who = %me.who.as_ref().map_or_else(|| "a request".to_owned(), |w| w.describe()),
                poll_ms = took.as_millis(),
                "one poll held a runtime worker (a CPU-heavy step that doesn't yield)"
            );
        }
        r
    }
}

/// The watchdog saw the runtime stuck for `stuck`: logs and counts it, and
/// stops every statement whose current poll has run for `min_poll` or more
/// (decision bugfix-1). Returns how many it stopped.
pub fn on_stall(stuck: Duration, min_poll: Duration) -> usize {
    metrics::counter!("sprig_gateway_runtime_stalls_total").increment(1);
    let now = now_us();
    let min = u64::try_from(min_poll.as_micros()).unwrap_or(u64::MAX);
    let all: Vec<Arc<Slot>> = slots()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    let mut stopped = 0;
    for s in all {
        let since = s.since_us.load(Ordering::SeqCst);
        if since == 0 || now.saturating_sub(since) < min {
            continue;
        }
        let who = s.who.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let poll = Duration::from_micros(now.saturating_sub(since));
        if let Some(w) = who {
            tracing::error!(
                who = %w.describe(),
                ?poll,
                ?stuck,
                "a statement held a runtime worker while the runtime was stuck: stopping it"
            );
            w.stop();
            stopped += 1;
        } else {
            tracing::error!(
                ?poll,
                ?stuck,
                "an unnamed task held a runtime worker while the runtime was stuck"
            );
        }
    }
    if stopped == 0 {
        tracing::error!(
            ?stuck,
            "the gateway's runtime was stuck (the stall watchdog fired); no statement to stop"
        );
    }
    metrics::counter!("sprig_gateway_stall_kills_total").increment(stopped as u64);
    stopped
}

/// Bytes a client stream reads before it yields to the runtime.
pub const PACE_BYTES: usize = 256 * 1024;

/// What one read of a [`Paced`] stream asks for when the caller's buffer
/// is larger: one TLS record.
const READ_CHUNK: usize = 16 * 1024;

/// A client stream that yields to the runtime after every [`PACE_BYTES`]
/// it reads. Reading a large packet (and decrypting it, under TLS) would
/// otherwise run on one worker thread for as long as the bytes keep
/// arriving: about a second for 64 MiB in a debug build, every other task
/// on that thread waiting. A read into a large buffer takes at most one
/// TLS record's worth (through a scratch buffer allocated on first use).
pub struct Paced<S> {
    inner: S,
    since_yield: usize,
    scratch: Vec<u8>,
    #[cfg(test)]
    yields: usize,
}

impl<S> Paced<S> {
    /// `inner`, paced.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            since_yield: 0,
            scratch: Vec::new(),
            #[cfg(test)]
            yields: 0,
        }
    }

    /// The stream.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// The stream, mutably.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Paced<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if me.since_yield >= PACE_BYTES {
            me.since_yield = 0;
            #[cfg(test)]
            {
                me.yields += 1;
            }
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let r = if buf.remaining() <= READ_CHUNK {
            let before = buf.filled().len();
            let r = Pin::new(&mut me.inner).poll_read(cx, buf);
            me.since_yield += buf.filled().len() - before;
            r
        } else {
            if me.scratch.is_empty() {
                me.scratch = vec![0; READ_CHUNK];
            }
            let mut rb = ReadBuf::new(&mut me.scratch);
            let r = Pin::new(&mut me.inner).poll_read(cx, &mut rb);
            if r.is_ready() {
                buf.put_slice(rb.filled());
                me.since_yield += rb.filled().len();
            }
            r
        };
        if r.is_pending() {
            me.since_yield = 0;
        }
        r
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Paced<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn the_threshold_is_inclusive() {
        assert!(!is_large(1023, 1024));
        assert!(is_large(1024, 1024));
    }

    /// A CPU-bound large request on one of two workers doesn't stop a
    /// ticker on the runtime, and the request's own awaits still work.
    #[test]
    fn a_large_request_leaves_the_workers_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let s2 = std::sync::Arc::clone(&stop);
            // The longest gap between ticks of a 5 ms ticker.
            let ticker = tokio::spawn(async move {
                let mut worst = Duration::ZERO;
                let mut last = Instant::now();
                while !s2.load(std::sync::atomic::Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    worst = worst.max(last.elapsed());
                    last = Instant::now();
                }
                worst
            });
            // Two busy requests on the session tasks of both workers.
            let busy = |n: u64| {
                tokio::spawn(run(10, 1, async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let until = Instant::now() + Duration::from_millis(400);
                    let mut x = n;
                    while Instant::now() < until {
                        x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    }
                    x
                }))
            };
            let (a, b) = (busy(1), busy(2));
            a.await.unwrap();
            b.await.unwrap();
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let worst = ticker.await.unwrap();
            assert!(
                worst < Duration::from_millis(200),
                "ticker stalled {worst:?}"
            );
        });
    }

    fn spin(d: Duration) {
        let until = Instant::now() + d;
        let mut x = 1u64;
        while Instant::now() < until {
            x = std::hint::black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1));
        }
    }

    /// G40: short but heavy statements (each poll 400 ms of CPU, like a
    /// sort that doesn't yield) on both workers of a two-worker runtime.
    /// A second client's simple request, sent from another thread, still
    /// answers within 150 ms once the heavy ones have had their first
    /// poll: they run on blocking threads from then on. Before, every
    /// simple request waited for a heavy poll to end (400-800 ms).
    #[test]
    fn heavy_short_statements_leave_room_for_a_second_client() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let h = rt.handle().clone();
        let started = Instant::now();
        let heavy: Vec<_> = (0..3)
            .map(|_| {
                h.spawn(run(10, 1 << 20, async {
                    for _ in 0..6 {
                        spin(Duration::from_millis(400));
                        tokio::task::yield_now().await;
                    }
                }))
            })
            .collect();
        // The second client: a simple request every 20 ms for 2.4 s.
        let mut late = Vec::new();
        while started.elapsed() < Duration::from_millis(2400) {
            let sent = Instant::now();
            let (tx, rx) = std::sync::mpsc::channel();
            h.spawn(async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let _ = tx.send(());
            });
            rx.recv().unwrap();
            let took = sent.elapsed();
            if sent.duration_since(started) > Duration::from_millis(1000)
                && took > Duration::from_millis(150)
            {
                late.push(took);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        rt.block_on(async {
            for t in heavy {
                t.await.unwrap();
            }
        });
        assert!(late.is_empty(), "simple requests waited {late:?}");
    }

    struct Victim(std::sync::atomic::AtomicBool);

    impl Stoppable for Victim {
        fn stop(&self) {
            self.0.store(true, Ordering::SeqCst);
        }

        fn describe(&self) -> String {
            "a test statement".into()
        }
    }

    /// R8 round 10: graph statements on more threads than the runtime has
    /// workers, each one long poll that doesn't yield, leave a ticker on
    /// the runtime free; run through the stall guard instead, the same
    /// statements held both workers for their whole first poll.
    #[test]
    fn graph_statements_leave_the_workers_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let worst = |graph: bool| {
            rt.block_on(async {
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let s2 = Arc::clone(&stop);
                let ticker = tokio::spawn(async move {
                    let mut worst = Duration::ZERO;
                    let mut last = Instant::now();
                    while !s2.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        worst = worst.max(last.elapsed());
                        last = Instant::now();
                    }
                    worst
                });
                let heavy: Vec<_> = (0..4)
                    .map(|_| {
                        let work = async {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            spin(Duration::from_millis(400));
                        };
                        if graph {
                            tokio::spawn(async move { run_graph(work).await })
                        } else {
                            tokio::spawn(async move { Guarded::new(None, work).await })
                        }
                    })
                    .collect();
                for h in heavy {
                    h.await.unwrap();
                }
                stop.store(true, Ordering::Relaxed);
                ticker.await.unwrap()
            })
        };
        let guarded = worst(false);
        let graph = worst(true);
        assert!(
            guarded >= Duration::from_millis(300),
            "the stall guard alone: {guarded:?}"
        );
        assert!(
            graph < Duration::from_millis(150),
            "graph statements: {graph:?}"
        );
    }

    /// The watchdog's stall handler names the statement in a long poll and
    /// stops it; one in a short poll is left alone.
    #[test]
    fn a_stall_stops_the_statement_holding_a_worker() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let hog = Arc::new(Victim(std::sync::atomic::AtomicBool::new(false)));
        let who: Arc<dyn Stoppable> = Arc::clone(&hog) as Arc<dyn Stoppable>;
        let t = rt.spawn(Guarded::new(Some(who), async {
            spin(Duration::from_millis(600));
        }));
        std::thread::sleep(Duration::from_millis(300));
        assert!(on_stall(Duration::from_secs(2), Duration::from_millis(200)) >= 1);
        assert!(hog.0.load(Ordering::SeqCst), "the hog wasn't stopped");
        rt.block_on(t).unwrap();
        // Polls done: nothing left to stop for this test's victim.
        let idle = Arc::new(Victim(std::sync::atomic::AtomicBool::new(false)));
        let who: Arc<dyn Stoppable> = Arc::clone(&idle) as Arc<dyn Stoppable>;
        let t = rt.spawn(Guarded::new(Some(who), async {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }));
        std::thread::sleep(Duration::from_millis(100));
        on_stall(Duration::from_secs(2), Duration::from_millis(50));
        rt.block_on(t).unwrap();
        assert!(
            !idle.0.load(Ordering::SeqCst),
            "a waiting statement was stopped"
        );
    }

    /// A large read arrives whole, yielding every `PACE_BYTES`.
    #[tokio::test]
    async fn a_paced_stream_reads_everything_and_yields() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut a, b) = tokio::io::duplex(8 << 20);
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        a.write_all(&data).await.unwrap();
        a.shutdown().await.unwrap();
        let mut p = Paced::new(b);
        let mut all = Vec::with_capacity(4 << 20);
        p.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, data);
        assert!(p.yields >= data.len() / PACE_BYTES, "{} yields", p.yields);
    }

    #[tokio::test]
    async fn a_current_thread_runtime_runs_it_inline() {
        assert_eq!(run(10, 1, async { 7 }).await, 7);
    }
}
