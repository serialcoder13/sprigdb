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
use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

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
