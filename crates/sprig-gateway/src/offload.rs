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

use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use sprig_settings::{Apply, HardBounds, Scopes, Setting};
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

/// Runs `fut`: as is when `size` is under `threshold`, else on a blocking
/// thread (see the module notes).
pub async fn run<F: Future>(size: usize, threshold: usize, fut: F) -> F::Output {
    THRESHOLD.store(threshold, Ordering::Relaxed);
    if !is_large(size, threshold) || !multi_thread() {
        return fut.await;
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
            assert!(worst < Duration::from_millis(200), "ticker stalled {worst:?}");
        });
    }

    #[tokio::test]
    async fn a_current_thread_runtime_runs_it_inline() {
        assert_eq!(run(10, 1, async { 7 }).await, 7);
    }
}
