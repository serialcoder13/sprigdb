//! A feed read by a task of its own, its items through a bounded channel
//! (R9, feedcancel; decision r9-131).
//!
//! [`TenantTail::next`] isn't cancel-safe. It takes a transaction from
//! its source and then waits for catalog lookups (a new table's first row
//! needs one) before it emits the transaction's items. A consumer that
//! races `next()` against a timer (`tokio::time::timeout`, or `select!`
//! against a heartbeat, a batch window or a second feed) and goes on
//! reading after the timer won drops that transaction for good: the
//! source has moved past it. A binlog read isn't safe to cancel either, in
//! the middle of a packet.
//!
//! A [`Pump`] reads its feed in a task of its own, up to `ahead` items
//! ahead, and hands the items over a channel, whose `recv` is cancel-safe.
//! Only dropping the pump stops the read (the task is aborted), so a
//! consumer stops reading only when it stops altogether. Items read ahead
//! and not taken are lost with the pump; a consumer that checkpoints only
//! past items it took and handled loses nothing across a restart.
//!
//! Racing `next()` against a signal that ends the consumer (its stop
//! token, its client gone) is safe without a pump: the tail is dropped
//! with the consumer and the next one resumes from the checkpoint.

use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::event::FeedItem;
use crate::tail::TenantTail;

/// Something read one item at a time, whose read isn't cancel-safe.
#[async_trait::async_trait]
pub trait Source: Send + 'static {
    /// What it yields.
    type Item: Send + 'static;

    /// The next item. An error ends the pump after it is delivered.
    ///
    /// # Errors
    /// The source's.
    async fn next(&mut self) -> Result<Self::Item>;
}

#[async_trait::async_trait]
impl Source for TenantTail {
    type Item = FeedItem;

    async fn next(&mut self) -> Result<FeedItem> {
        TenantTail::next(self).await
    }
}

/// A feed read by a task of its own (see the module docs). Dropping it
/// stops the task.
#[derive(Debug)]
pub struct Pump<T> {
    rx: mpsc::Receiver<Result<T>>,
    task: JoinHandle<()>,
    what: &'static str,
}

impl<T: Send + 'static> Pump<T> {
    /// Items read ahead at most, by default.
    pub const AHEAD: usize = 64;

    /// Starts reading `src`, at most `ahead` items ahead (at least 1).
    /// `what` names the consumer in the error after the reader ended.
    #[must_use]
    pub fn start<S: Source<Item = T>>(mut src: S, ahead: usize, what: &'static str) -> Self {
        let (tx, rx) = mpsc::channel(ahead.max(1));
        let task = tokio::spawn(async move {
            loop {
                let item = src.next().await;
                let end = item.is_err();
                if tx.send(item).await.is_err() || end {
                    return;
                }
            }
        });
        Self { rx, task, what }
    }

    /// The next item. Cancel-safe: an item is taken only when this
    /// returns it.
    ///
    /// # Errors
    /// The source's error (once); 39113 (binlog disconnected, retryable) after it, or
    /// if the reader task ended otherwise.
    pub async fn next(&mut self) -> Result<T> {
        self.rx.recv().await.unwrap_or_else(|| Err(self.ended()))
    }

    /// An item already read, without waiting.
    pub fn try_next(&mut self) -> Option<Result<T>> {
        match self.rx.try_recv() {
            Ok(i) => Some(i),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => Some(Err(self.ended())),
        }
    }

    fn ended(&self) -> SprigError {
        SprigError::new(
            SprigCode::BinlogDisconnected,
            format!("the {}'s feed reader ended", self.what),
        )
    }
}

impl<T> Drop for Pump<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::time::Duration;

    use super::*;
    use crate::testing::{SlowFeed, keys_once};

    /// The bug the pump fixes (decision r9-131): a tail read raced
    /// against a timer that fires during its catalog lookup loses the
    /// transaction. Shown here on the tail itself, then fixed with a pump.
    #[tokio::test(start_paused = true)]
    async fn a_timer_mid_read_loses_a_change_without_a_pump_and_none_with_one() {
        let feed = SlowFeed::new(20, Duration::from_millis(300));
        let mut tail = feed.tail().await;
        let mut seen = Vec::new();
        let end = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < end {
            if let Ok(i) = tokio::time::timeout(Duration::from_millis(200), tail.next()).await {
                seen.push(i.unwrap());
            }
        }
        assert!(
            keys_once(&seen).len() < 20,
            "without a pump the timer should cost changes"
        );

        let mut pump = Pump::start(feed.tail().await, Pump::<FeedItem>::AHEAD, "test");
        let mut seen = Vec::new();
        let end = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < end {
            if let Ok(i) = tokio::time::timeout(Duration::from_millis(200), pump.next()).await {
                seen.push(i.unwrap());
            }
        }
        assert_eq!(keys_once(&seen), (1..=20).collect::<Vec<_>>());
    }

    struct Failing(u32);

    #[async_trait::async_trait]
    impl Source for Failing {
        type Item = u32;
        async fn next(&mut self) -> Result<u32> {
            self.0 += 1;
            if self.0 > 2 {
                return Err(SprigError::internal("broken"));
            }
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn the_error_comes_once_then_the_end() {
        let mut p = Pump::start(Failing(0), 1, "test");
        assert_eq!(p.next().await.unwrap(), 1);
        assert_eq!(p.next().await.unwrap(), 2);
        assert!(p.next().await.unwrap_err().message.contains("broken"));
        let e = p.next().await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::BinlogDisconnected));
        assert!(matches!(p.try_next(), Some(Err(_))));
    }
}
