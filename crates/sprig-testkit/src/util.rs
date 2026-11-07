//! Small utilities for tests: unique names, temporary directories, retry
//! with a timeout and waiting for a TCP port.

use std::fmt::Display;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sprig_common::{Result, SprigError};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A suffix unique within this process and very likely across processes:
/// a hash of the process id, the time and a counter, then the counter.
fn unique_suffix() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    // Truncation is fine: only the low bits feed the hash.
    #[allow(clippy::cast_possible_truncation)]
    let seed = (nanos as u64) ^ (u64::from(std::process::id()) << 40) ^ n;
    let h = splitmix64(seed);
    format!("{:08x}{n:x}", h >> 32)
}

/// splitmix64's output function: spreads close seeds far apart.
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Keeps `[a-z0-9]` from `prefix`, lowercased, and maps everything else to
/// `sep`.
fn sanitize(prefix: &str, sep: char) -> String {
    prefix
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                sep
            }
        })
        .collect()
}

/// A name unique to this test run, in the id alphabet `[a-z0-9-]`: usable as
/// a Compose project, cluster id, etcd key prefix or container name, for
/// example `orders-3fa29c1b0`.
#[must_use]
pub fn unique_name(prefix: &str) -> String {
    let p = sanitize(prefix, '-');
    let p = p.trim_matches('-');
    if p.is_empty() {
        format!("t-{}", unique_suffix())
    } else {
        format!("{p}-{}", unique_suffix())
    }
}

/// A unique SQL identifier, `[a-z0-9_]` and starting with a letter, for
/// schemas, tables and users, for example `orders_3fa29c1b0`. Prefixes are
/// cut so the result stays within MariaDB's 64-character limit.
#[must_use]
pub fn unique_ident(prefix: &str) -> String {
    let mut p = sanitize(prefix, '_').trim_matches('_').to_owned();
    if !p.starts_with(|c: char| c.is_ascii_lowercase()) {
        p.insert_str(0, "t_");
    }
    p.truncate(40);
    format!("{}_{}", p.trim_end_matches('_'), unique_suffix())
}

/// A temporary directory, deleted when dropped unless `SPRIG_TEST_KEEP=1`.
#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
    keep: bool,
}

impl TempDir {
    /// Creates `<system temp>/sprig-testkit/<unique name>`.
    ///
    /// # Errors
    /// If the directory can't be created.
    pub fn new(prefix: &str) -> Result<Self> {
        let path = std::env::temp_dir()
            .join("sprig-testkit")
            .join(unique_name(prefix));
        std::fs::create_dir_all(&path).map_err(|e| {
            SprigError::internal(format!("creating {}", path.display())).with_source(e)
        })?;
        Ok(Self {
            path,
            keep: crate::skip::flag(crate::skip::KEEP),
        })
    }

    /// The directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `path()` joined with `rel`.
    #[must_use]
    pub fn join(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.path.join(rel)
    }

    /// Keeps the directory after drop and returns its path.
    #[must_use]
    pub fn keep(mut self) -> PathBuf {
        self.keep = true;
        self.path.clone()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if self.keep {
            eprintln!("keeping {}", self.path.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Retry with a deadline: calls an operation until it succeeds or the
/// timeout passes, sleeping between attempts with exponential backoff.
///
/// ```no_run
/// # async fn example() -> sprig_common::Result<()> {
/// use std::time::Duration;
/// use sprig_testkit::Retry;
///
/// let n = Retry::new(Duration::from_secs(30))
///     .run("etcd answers", || async { Ok::<_, String>(42) })
///     .await?;
/// # let _ = n; Ok(()) }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Retry {
    timeout: Duration,
    interval: Duration,
    max_interval: Duration,
}

impl Retry {
    /// Gives up after `timeout`. The first pause is 100 ms, doubling up to 2 s.
    #[must_use]
    pub const fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(2),
        }
    }

    /// Sets the first pause between attempts.
    #[must_use]
    pub const fn interval(mut self, d: Duration) -> Self {
        self.interval = d;
        self
    }

    /// Caps the pause between attempts; equal to `interval` for a fixed pause.
    #[must_use]
    pub const fn max_interval(mut self, d: Duration) -> Self {
        self.max_interval = d;
        self
    }

    fn next_pause(&self, pause: Duration, left: Duration) -> Duration {
        pause.min(self.max_interval).min(left)
    }

    fn gave_up(&self, what: &str, attempts: u32, last: &str) -> SprigError {
        SprigError::internal(format!(
            "{what}: gave up after {:?} and {attempts} attempts; last error: {last}",
            self.timeout
        ))
    }

    /// Runs `op` until it returns `Ok`. Each attempt is also cut off at the
    /// deadline. `what` names the operation in the error.
    ///
    /// # Errors
    /// When the timeout passes; the error carries the last failure.
    pub async fn run<T, E, F, Fut>(&self, what: &str, mut op: F) -> Result<T>
    where
        E: Display,
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let deadline = Instant::now() + self.timeout;
        let mut pause = self.interval;
        let mut attempts = 0;
        loop {
            attempts += 1;
            let left = deadline.saturating_duration_since(Instant::now());
            let last = match tokio::time::timeout(left, op()).await {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) => e.to_string(),
                Err(_) => "attempt timed out".to_owned(),
            };
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(self.gave_up(what, attempts, &last));
            }
            tokio::time::sleep(self.next_pause(pause, left)).await;
            pause = pause.saturating_mul(2);
        }
    }

    /// Runs `op` until it returns `Ok`, blocking the thread between
    /// attempts. Doesn't interrupt a slow attempt.
    ///
    /// # Errors
    /// When the timeout passes; the error carries the last failure.
    pub fn run_blocking<T, E, F>(&self, what: &str, mut op: F) -> Result<T>
    where
        E: Display,
        F: FnMut() -> std::result::Result<T, E>,
    {
        let deadline = Instant::now() + self.timeout;
        let mut pause = self.interval;
        let mut attempts = 0;
        loop {
            attempts += 1;
            let last = match op() {
                Ok(v) => return Ok(v),
                Err(e) => e.to_string(),
            };
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(self.gave_up(what, attempts, &last));
            }
            std::thread::sleep(self.next_pause(pause, left));
            pause = pause.saturating_mul(2);
        }
    }
}

/// The `host:port` of a URL such as `mysql://user:pw@127.0.0.1:13301/db` or
/// `http://127.0.0.1:2379`, or of a bare `host:port`.
#[must_use]
pub fn host_port(url: &str) -> Option<&str> {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let rest = rest.rsplit_once('@').map_or(rest, |(_, r)| r);
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let hp = &rest[..end];
    let (_, port) = hp.rsplit_once(':')?;
    port.parse::<u16>().ok().map(|_| hp)
}

/// Waits until a TCP connection to `url`'s `host:port` succeeds.
///
/// # Errors
/// If `url` has no port, or nothing accepts before the timeout.
pub async fn wait_for_tcp(url: &str, timeout: Duration) -> Result<()> {
    let addr = host_port(url)
        .ok_or_else(|| SprigError::internal(format!("no host:port in {url:?}")))?
        .to_owned();
    Retry::new(timeout)
        .run(&format!("connecting to {addr}"), || {
            let addr = addr.clone();
            async move { tokio::net::TcpStream::connect(addr).await.map(drop) }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn unique_names_are_unique_and_valid() {
        let names: HashSet<_> = (0..1000).map(|_| unique_name("My Test_1")).collect();
        assert_eq!(names.len(), 1000);
        for n in &names {
            assert!(n.starts_with("my-test-1-"), "{n}");
            assert!(
                n.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            );
            // Valid as a cluster id (chapter 03).
            sprig_common::ids::ClusterId::new(n.clone()).unwrap();
        }
        assert!(unique_name("--").starts_with("t-"));
    }

    #[test]
    fn unique_idents_are_sql_safe() {
        let i = unique_ident("1-orders");
        assert!(i.starts_with("t_1_orders_"), "{i}");
        assert!(
            i.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        );
        assert!(unique_ident(&"x".repeat(100)).len() <= 64);
    }

    #[test]
    fn temp_dir_is_removed_on_drop() {
        let d = TempDir::new("unit").unwrap();
        let p = d.path().to_owned();
        std::fs::write(d.join("f"), b"x").unwrap();
        assert!(p.is_dir());
        drop(d);
        if !crate::skip::flag(crate::skip::KEEP) {
            assert!(!p.exists());
        }
    }

    #[test]
    fn host_port_parses_urls() {
        assert_eq!(
            host_port("mysql://root:p@ss@127.0.0.1:13301/db?x=1"),
            Some("127.0.0.1:13301")
        );
        assert_eq!(host_port("http://etcd:2379"), Some("etcd:2379"));
        assert_eq!(host_port("10.0.0.1:3306"), Some("10.0.0.1:3306"));
        assert_eq!(host_port("http://etcd"), None);
        assert_eq!(host_port("mysql://h:notaport"), None);
    }

    #[test]
    fn retry_blocking_succeeds_after_failures() {
        let mut n = 0;
        let v = Retry::new(Duration::from_secs(5))
            .interval(Duration::from_millis(1))
            .run_blocking("count", || {
                n += 1;
                if n < 3 { Err("not yet") } else { Ok(n) }
            })
            .unwrap();
        assert_eq!(v, 3);
    }

    #[test]
    fn retry_blocking_gives_up_with_last_error() {
        let e = Retry::new(Duration::from_millis(30))
            .interval(Duration::from_millis(5))
            .run_blocking("never", || Err::<(), _>("still down"))
            .unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("never") && msg.contains("still down"), "{msg}");
    }

    #[tokio::test]
    async fn retry_async_cuts_off_slow_attempts() {
        let started = Instant::now();
        let e = Retry::new(Duration::from_millis(50))
            .run("slow", || async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<(), String>(())
            })
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(e.to_string().contains("timed out"), "{e}");
    }

    #[tokio::test]
    async fn wait_for_tcp_sees_a_listener() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        wait_for_tcp(&url, Duration::from_secs(5)).await.unwrap();
    }
}
