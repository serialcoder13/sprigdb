//! Large in-gateway steps off the runtime's worker threads (decision
//! bugfix-1, G40; R5 round 4).
//!
//! Sorting, grouping, unwinding and joining a large set of documents is
//! synchronous work that doesn't yield: on a tokio worker it holds every
//! other connection on that thread until it ends, and a long one stalls
//! the gateway's watchdog heartbeat. [`run`] runs such a step through
//! `tokio::task::block_in_place` once it covers [`HEAVY_DOCS`] documents
//! or more, so the worker's queue moves to another thread for the
//! duration; smaller steps run in place, where a hand-off would cost more
//! than the step. Outside a multi-threaded runtime (unit tests, tools) the
//! step always runs in place.

use tokio::runtime::{Handle, RuntimeFlavor};

/// Documents from which a step leaves the worker thread.
pub const HEAVY_DOCS: usize = 10_000;

/// Runs `f`, a synchronous step over `n` documents: off the worker when
/// `n` reaches [`HEAVY_DOCS`] on a multi-threaded runtime, else in place.
pub fn run<R>(n: usize, f: impl FnOnce() -> R) -> R {
    if n >= HEAVY_DOCS
        && Handle::try_current().is_ok_and(|h| h.runtime_flavor() == RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bson::{Document, doc};

    use super::*;
    use crate::agg::{AggLimits, run_stage};

    fn docs(n: i64) -> Vec<Document> {
        (0..n)
            .map(
                |i| doc! {"_id": i, "k": (i * 7_919) % 1_000, "g": i % 50, "tags": ["a", "b", "c"]},
            )
            .collect()
    }

    #[test]
    fn small_steps_run_in_place_outside_a_runtime() {
        assert_eq!(run(HEAVY_DOCS * 2, || 7), 7);
    }

    /// A heavy `$sort` + `$group` on one of two workers: a 5 ms ticker
    /// on the same runtime keeps ticking (the step left the worker).
    #[test]
    fn a_heavy_stage_leaves_the_workers_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let s2 = std::sync::Arc::clone(&stop);
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
            let lim = AggLimits {
                stage_bytes: usize::MAX,
                now: bson::DateTime::now(),
            };
            let busy = |seed: i64| {
                tokio::spawn(async move {
                    let mut total = 0;
                    for _ in 0..3 {
                        // Building the input is heavy too: off the worker.
                        let d = run(HEAVY_DOCS, || docs(40_000 + seed));
                        let sorted = run(d.len(), || {
                            run_stage(d, &doc! {"$sort": {"k": -1, "_id": 1}}, &lim)
                        })
                        .unwrap();
                        let g = run(sorted.len(), || {
                            run_stage(
                                sorted,
                                &doc! {"$group": {"_id": "$g", "s": {"$addToSet": "$k"}, "n": {"$sum": 1}}},
                                &lim,
                            )
                        })
                        .unwrap();
                        total += g.len();
                        tokio::task::yield_now().await;
                    }
                    total
                })
            };
            let (a, b) = (busy(0), busy(1));
            assert_eq!(a.await.unwrap(), 150);
            assert_eq!(b.await.unwrap(), 150);
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let worst = ticker.await.unwrap();
            assert!(worst < Duration::from_millis(150), "ticker stalled {worst:?}");
        });
    }
}
