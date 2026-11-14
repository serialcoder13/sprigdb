//! Test helpers for the feed's consumers (R9, feedcancel): a real
//! [`TenantTail`] over a fixed binlog whose catalog lookups are slow, to
//! show that a consumer's timers never cost a change (decision r9-131).
//!
//! Transaction `k` (from 1) inserts one row, `k = k`, into a table of its
//! own (`n<k>`), so every transaction needs a catalog lookup before the
//! tail can emit it, as a new table's first row does. The lookup takes
//! `lookup` (tokio time: tests run with paused time and stay fast).

use std::sync::Arc;
use std::time::Duration;

use sprig_binlog::{Gtid, GtidPos, RowImage, Value};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DbId, TenantId};

use crate::event::FeedItem;
use crate::names::Resolver;
use crate::ownership::Handoff;
use crate::placement::PlacementView;
use crate::source::{Clusters, RawRow, RawTxn, RowKind, SourceItem, TableShape, TxnSource};
use crate::tail::{StartAt, TailOptions, TenantTail};
use crate::token::ResumeToken;

/// The tenant of the fixed binlog.
pub const TENANT: TenantId = TenantId(7);

/// The cluster of the fixed binlog.
///
/// # Panics
/// Never (the name is valid).
#[must_use]
pub fn cluster() -> ClusterId {
    #[allow(clippy::unwrap_used)] // a valid constant name
    ClusterId::new("dc-a-c1").unwrap()
}

fn g(seq: u64) -> Gtid {
    Gtid {
        domain: 1,
        server: 1,
        seq,
    }
}

/// A fixed binlog of `n` transactions with slow catalog lookups.
#[derive(Debug, Clone)]
pub struct SlowFeed {
    txns: Arc<Vec<RawTxn>>,
    lookup: Duration,
    opts: TailOptions,
}

impl SlowFeed {
    /// `n` transactions; each one's catalog lookup takes `lookup`.
    #[must_use]
    pub fn new(n: u64, lookup: Duration) -> Self {
        let txns = (1..=n)
            .map(|seq| {
                let before = if seq == 1 {
                    GtidPos::new()
                } else {
                    GtidPos::from_gtids([g(seq - 1)])
                };
                let mut t = RawTxn::new(g(seq), before, 100);
                t.rows = vec![RawRow {
                    table: Arc::new(TableShape {
                        schema: format!("t{}_d1_g1_s1", TENANT.0),
                        table: format!("n{seq}"),
                        columns: vec!["k".into()],
                        key: vec![0],
                    }),
                    kind: RowKind::Insert,
                    before: None,
                    after: Some(RowImage(vec![Some(Value::Int(
                        i64::try_from(seq).unwrap_or(i64::MAX),
                    ))])),
                    hint: None,
                }];
                t
            })
            .collect();
        Self {
            txns: Arc::new(txns),
            lookup,
            opts: TailOptions {
                progress_every: Duration::from_millis(100),
                ..TailOptions::default()
            },
        }
    }

    /// With these tail options instead.
    #[must_use]
    pub fn with_opts(mut self, opts: TailOptions) -> Self {
        self.opts = opts;
        self
    }

    /// The database's name in events.
    #[must_use]
    pub fn database(&self) -> &'static str {
        "shop"
    }

    /// A tail from the start of the binlog.
    ///
    /// # Panics
    /// If the tail can't start (it always can).
    pub async fn tail(&self) -> TenantTail {
        self.tail_from(StartAt::At {
            cluster: cluster(),
            pos: GtidPos::new(),
        })
        .await
    }

    /// A tail from `start` (a consumer's checkpoint, or now).
    ///
    /// # Panics
    /// If the tail can't start (a token of another tenant).
    pub async fn tail_from(&self, start: StartAt) -> TenantTail {
        #[allow(clippy::expect_used)] // a test helper
        TenantTail::start(
            TENANT,
            start,
            Arc::new(Fixed(Arc::clone(&self.txns))),
            Arc::new(Here),
            Arc::new(Slow(self.lookup)),
            self.opts.clone(),
        )
        .await
        .expect("the test tail starts")
    }
}

/// A progress token of the fixed binlog at transaction `seq` (0: its
/// start).
#[must_use]
pub fn token(seq: u64) -> ResumeToken {
    ResumeToken {
        tenant: TENANT,
        clamp: seq,
        cluster: cluster(),
        segment: 0,
        pos: if seq == 0 {
            GtidPos::new()
        } else {
            GtidPos::from_gtids([g(seq)])
        },
        until: None,
        progress: true,
    }
}

/// The keys of `items`' events, in order.
///
/// # Panics
/// If a key comes twice (a duplicate change).
#[must_use]
pub fn keys_once<'a>(items: impl IntoIterator<Item = &'a FeedItem>) -> Vec<i64> {
    let mut out: Vec<i64> = Vec::new();
    for i in items {
        if let FeedItem::Event(e) = i
            && let Some(Value::Int(k)) = e.after.as_ref().and_then(|a| a.get("k"))
        {
            assert!(!out.contains(k), "change {k} delivered twice");
            out.push(*k);
        }
    }
    out
}

/// One cluster whose binlog is the fixed list; idle at its end.
struct Fixed(Arc<Vec<RawTxn>>);

struct FixedSrc {
    txns: Arc<Vec<RawTxn>>,
    next: usize,
}

#[async_trait::async_trait]
impl TxnSource for FixedSrc {
    async fn next(&mut self) -> Result<SourceItem> {
        if let Some(t) = self.txns.get(self.next) {
            self.next += 1;
            return Ok(SourceItem::Txn(t.clone()));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(SourceItem::Idle {
            pos: GtidPos::from_gtids([g(self.txns.len() as u64)]),
        })
    }
}

#[async_trait::async_trait]
impl Clusters for Fixed {
    async fn open(&self, _: &ClusterId, _: TenantId, from: &GtidPos) -> Result<Box<dyn TxnSource>> {
        let n = from.get(1).map_or(0, |x| x.seq);
        Ok(Box::new(FixedSrc {
            txns: Arc::clone(&self.0),
            next: usize::try_from(n).unwrap_or(usize::MAX),
        }))
    }

    async fn position(&self, _: &ClusterId) -> Result<GtidPos> {
        Ok(GtidPos::from_gtids([g(self.0.len() as u64)]))
    }
}

struct Here;

#[async_trait::async_trait]
impl PlacementView for Here {
    async fn current(&self, _: TenantId) -> Result<(ClusterId, u64)> {
        Ok((cluster(), 1))
    }

    async fn history(&self, _: TenantId) -> Result<Vec<Handoff>> {
        Ok(Vec::new())
    }
}

/// Every lookup takes the given time.
struct Slow(Duration);

#[async_trait::async_trait]
impl Resolver for Slow {
    async fn database(&self, _: TenantId, _: DbId) -> Result<Option<String>> {
        tokio::time::sleep(self.0).await;
        Ok(Some("shop".into()))
    }

    async fn object(&self, _: TenantId, _: DbId, table: &str) -> Result<Option<u64>> {
        tokio::time::sleep(self.0).await;
        Ok(table.trim_start_matches('n').parse::<u64>().ok())
    }
}
