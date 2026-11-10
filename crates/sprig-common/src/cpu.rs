//! CPU-heavy work off the runtime's worker threads (chapter 04, "Heavy
//! statements"; decision bugfix-1).
//!
//! A tokio worker thread that runs one long synchronous step (a sort of a
//! million rows, a group-by fold, a JSON encode of a large result) holds
//! every task queued behind it, and chapter 25's watchdog takes a runtime
//! that stops answering for its stall time for a stuck process. So a step
//! whose input is large runs through [`heavy`]: on a multi-threaded runtime
//! `tokio::task::block_in_place` hands the worker's queue to another thread
//! first; elsewhere (a current-thread runtime, a blocking thread, no
//! runtime) the step runs as is.

use tokio::runtime::{Handle, RuntimeFlavor};

/// Inputs of at least this many items run off the worker: rows, documents
/// or groups. About 10 ms of work in a release build for most steps.
pub const HEAVY_ITEMS: usize = 20_000;

/// Whether the current thread may call `block_in_place`: inside a
/// multi-threaded runtime.
#[must_use]
pub fn can_block_in_place() -> bool {
    Handle::try_current().is_ok_and(|h| h.runtime_flavor() == RuntimeFlavor::MultiThread)
}

/// Runs `f` off the worker thread (see the module docs).
pub fn off_worker<R>(f: impl FnOnce() -> R) -> R {
    if can_block_in_place() {
        metrics::counter!("sprig_gateway_offloaded_steps_total").increment(1);
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

/// Runs `f` off the worker thread when `items` reaches [`HEAVY_ITEMS`],
/// else in place.
pub fn heavy<R>(items: usize, f: impl FnOnce() -> R) -> R {
    if items >= HEAVY_ITEMS {
        off_worker(f)
    } else {
        f()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    #[tokio::test]
    async fn a_current_thread_runtime_runs_it_in_place() {
        assert_eq!(heavy(HEAVY_ITEMS * 2, || 7), 7);
        assert_eq!(off_worker(|| 8), 8);
    }

    #[test]
    fn no_runtime_runs_it_in_place() {
        assert!(!can_block_in_place());
        assert_eq!(heavy(HEAVY_ITEMS, || 9), 9);
    }

    /// A long synchronous step on each of two workers leaves a ticker on
    /// the runtime running: before, both workers were held for 400 ms.
    #[test]
    fn heavy_steps_leave_the_workers_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stop = Arc::new(AtomicBool::new(false));
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
            let busy = |n: u64| {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    heavy(HEAVY_ITEMS, || {
                        let until = Instant::now() + Duration::from_millis(400);
                        let mut x = n;
                        while Instant::now() < until {
                            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                        }
                        x
                    })
                })
            };
            let (a, b) = (busy(1), busy(2));
            a.await.unwrap();
            b.await.unwrap();
            stop.store(true, Ordering::Relaxed);
            let worst = ticker.await.unwrap();
            assert!(
                worst < Duration::from_millis(200),
                "ticker stalled {worst:?}"
            );
        });
    }
}
