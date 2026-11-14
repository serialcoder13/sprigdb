//! The event trace of a simulation run.
//!
//! Every simulated network delivery, fault, store call and model step adds
//! a line, stamped with virtual time. Two runs of one seed must produce the
//! same trace; the harness compares them
//! and reports the first difference as a determinism bug.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// Lines kept in memory; later lines only update the hash and count.
pub const MAX_LINES: usize = 200_000;

#[derive(Debug, Default)]
struct Inner {
    lines: Vec<String>,
    count: u64,
    hash: u64,
}

/// A shared, append-only trace. Clones share one trace.
#[derive(Debug, Clone)]
pub struct Trace {
    origin: tokio::time::Instant,
    inner: Arc<Mutex<Inner>>,
}

impl Trace {
    /// An empty trace whose timestamps count from now (tokio time).
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            inner: Arc::new(Mutex::new(Inner {
                hash: 0xcbf2_9ce4_8422_2325,
                ..Inner::default()
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Virtual time since the run started.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        tokio::time::Instant::now() - self.origin
    }

    /// Appends an event.
    pub fn record(&self, event: impl AsRef<str>) {
        let at = self.elapsed();
        let mut line = String::new();
        let _ = write!(line, "{:>10.6} {}", at.as_secs_f64(), event.as_ref());
        let mut t = self.lock();
        // FNV-1a over the line and a separator.
        for b in line.bytes().chain(std::iter::once(b'\n')) {
            t.hash ^= u64::from(b);
            t.hash = t.hash.wrapping_mul(0x0100_0000_01b3);
        }
        t.count += 1;
        if t.lines.len() < MAX_LINES {
            t.lines.push(line);
        }
    }

    /// A digest of every event so far.
    #[must_use]
    pub fn digest(&self) -> (u64, u64) {
        let t = self.lock();
        (t.count, t.hash)
    }

    /// The kept lines.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        self.lock().lines.clone()
    }

    /// The last `n` kept lines, joined, for failure messages.
    #[must_use]
    pub fn tail(&self, n: usize) -> String {
        let t = self.lock();
        let from = t.lines.len().saturating_sub(n);
        t.lines[from..].join("\n")
    }
}

impl Default for Trace {
    fn default() -> Self {
        Self::new()
    }
}

/// Where two traces first differ: the index and both lines (`None` past the
/// end of one), or `None` when they are equal.
#[must_use]
pub fn first_difference(
    a: &[String],
    b: &[String],
) -> Option<(usize, Option<String>, Option<String>)> {
    let len = a.len().max(b.len());
    (0..len).find_map(|i| {
        let (left, right) = (a.get(i), b.get(i));
        (left != right).then(|| (i, left.cloned(), right.cloned()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn records_with_virtual_time() {
        let t = Trace::new();
        t.record("a");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        t.record("b");
        let lines = t.lines();
        assert!(lines[0].ends_with(" a") && lines[0].contains("0.000000"));
        assert!(lines[1].contains("1.500000"), "{}", lines[1]);
        assert_eq!(t.digest().0, 2);
        assert!(t.tail(1).ends_with(" b"));
    }

    #[test]
    fn differences() {
        let a = vec!["x".to_owned(), "y".to_owned()];
        let mut b = a.clone();
        assert_eq!(first_difference(&a, &b), None);
        b.push("z".to_owned());
        assert_eq!(
            first_difference(&a, &b),
            Some((2, None, Some("z".to_owned())))
        );
    }
}
