//! RL14 (r2harden): a mover's fence never reaches a client as 1146 or
//! 1049 (chapter 10, "Sessions during cutover").
//!
//! A move's cutover holds the tenant's map, waits `mover.txn_grace_ms`,
//! then renames the source's tables away (the rename fence); after the
//! commit a source kept for 0 hours is dropped. A statement that passed
//! the gateway's hold check just before the gateway saw the hold (its
//! watch of the maps lags, more so for a remote home), or one of a
//! transaction that runs on through the hold, reaches the source after
//! the rename and gets 1146 (or 1049 once the schemas are gone). Drivers
//! don't retry those.
//!
//! ```text
//! before a tenant request:  token := the tenant's maps' revisions (and holds)
//! the request's first answer is ERR 1146 or 1049 (nothing sent before it):
//!   held back from the client
//!   fence := the database it names is one the tenant has, and the
//!            token moved or a range is held: in the watched copy, else in
//!            etcd itself (one listing, R34), else (etcd didn't answer)
//!            after waiting up to 1 s for the watch to show it
//!   not a fence                         -> the ERR goes out as it was
//!   fence, inside a transaction         -> 1213 (the transaction was on the
//!                                          fenced copy; it is rolled back)
//!   fence, autocommit, attempts left    -> wait out the hold, follow the
//!                                          map, run the request again
//!   fence, attempts used (3, or 10 s)   -> 1205
//! ```
//!
//! A DC failover fences the old home by revoking the tenant's write
//! privileges there (RL13); a write a lagging gateway still sent there gets
//! 1142, handled the same way (the database check is skipped and the wait
//! for the watch is 300 ms).
//!
//! **What a genuine error costs (R34, bugfix5).** A table that really
//! doesn't exist used to cost up to [`SETTLE`]: the gateway waited for its
//! watch in case it lagged behind a hold. Now it lists the tenant's maps in
//! etcd once (a linearizable read, about a millisecond): a mover writes its
//! hold `mover.txn_grace_ms` before it renames, so a listing made after the
//! 1146 arrived shows the hold, or the cutover after it, whatever the
//! watch's lag. Unchanged maps there mean a real missing table (or a real
//! missing privilege for 1142), answered at once. Only when etcd doesn't
//! answer within the wait does the old wait for the watch decide. When etcd
//! does show a fence the watch hasn't, the gateway still waits (up to the
//! same bound) for the watch to catch up, so the retry waits out the hold.
//!
//! Running an autocommit statement again is safe here: 1146 and 1049 are
//! raised when the statement opens its tables, before it changes
//! anything, and the request is retried only when its first answer was
//! that error (in a multi-statement text, the first statement's). A later
//! statement's 1146 in a multi-statement text goes out unchanged.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sql::names::{MemoryNameMap, NameMap as _};

use super::PassSession;
use crate::backend::StmtInfo;
use crate::pipeline::{
    EndOfRows, ExecCtx, ExecSummary, Request, ResultSink, RowFormat, RowValues, Wire,
};
use crate::tenant::locator::GatewayLocator;

/// How many times one request runs again after meeting a fence.
pub(super) const MAX_ATTEMPTS: u32 = 3;
/// How long one request may spend meeting fences, all attempts together.
pub(super) const MAX_TOTAL: Duration = Duration::from_secs(10);
/// How long the gateway waits for its watch to show the move that a 1146
/// suggests, when etcd itself can't be asked (R34), before it treats the
/// error as the table's own; also the bound on etcd's answer.
const SETTLE: Duration = Duration::from_secs(1);
/// The same for a 1142 (the old home fenced by a failover, RL13): the
/// maps were quarantined before the fence, so the watch rarely lags.
const REVOKED_SETTLE: Duration = Duration::from_millis(300);

/// The tenant's maps as seen before a request: the DC they are followed
/// in, the sum of their revisions, and whether a range was held.
pub(super) type Token = (DcId, u64, bool);

/// A copy of a backend error (`SprigError` isn't `Clone`).
fn copy(e: &SprigError) -> SprigError {
    let p = e.protocol();
    SprigError::backend(p.mysql_errno, p.sqlstate, e.message.clone())
}

/// Whether `e` is an error a fence can cause: the mover's rename fence
/// (1146, and 1049 once a source is dropped) or a failover's fence of the
/// old home, which revokes the tenant's write privileges there (1142).
pub(super) fn fence_code(e: &SprigError) -> bool {
    matches!(e.protocol().mysql_errno, 1146 | 1049 | 1142)
}

/// The database a 1146 (`Table 'db.t' doesn't exist`) or 1049 (`Unknown
/// database 'db'`) names.
fn named_db(e: &SprigError) -> Option<String> {
    let m = &e.message;
    let start = m.find('\'')? + 1;
    let end = start + m[start..].find('\'')?;
    let quoted = &m[start..end];
    Some(match e.protocol().mysql_errno {
        1146 => quoted
            .split_once('.')
            .map_or(quoted, |(db, _)| db)
            .to_owned(),
        _ => quoted.to_owned(),
    })
}

/// Whether the tenant has the database `e` names (by its logical name, or
/// a physical schema of the tenant's).
fn known_db(e: &SprigError, map: &MemoryNameMap) -> bool {
    named_db(e).is_some_and(|db| map.database(&db).is_some() || map.logical(&db).is_some())
}

/// Whether the request that failed with `e` met a mover's fence: the
/// database is the tenant's and its maps moved (or are held) since
/// `before`, as the watched copy shows it, else as etcd itself does (R34),
/// else (etcd didn't answer) waiting up to [`SETTLE`] for the watch to
/// show it.
pub(super) async fn met_fence(
    locator: &GatewayLocator,
    tenant: TenantId,
    map: &MemoryNameMap,
    before: &Token,
    e: &SprigError,
) -> bool {
    // 1142 names an account and a table, not a database; a privilege a
    // tenant really lacks is answered after a look at its maps.
    let revoked = e.protocol().mysql_errno == 1142;
    if !revoked && !known_db(e, map) {
        return false;
    }
    if before.2 {
        return true; // it ran under a hold (a transaction running on)
    }
    let settle = if revoked { REVOKED_SETTLE } else { SETTLE };
    // Follows the tenant to another DC when its map there was left.
    let _ = locator.placement_followed(tenant).await;
    let now = locator.maps_token(tenant);
    if now.2 || now != *before {
        return true;
    }
    // R34: etcd itself, whatever the watch's lag (module docs). Asked only
    // once the watched copy has loaded: `before` is from that copy.
    let mut confirmed = false;
    if locator.maps_loaded(tenant) {
        match tokio::time::timeout(settle, locator.maps_token_fresh(tenant)).await {
            Ok(Ok(fresh)) if !fresh.2 && fresh == *before => return false,
            Ok(Ok(_)) => confirmed = true,
            Ok(Err(e)) => tracing::debug!(%tenant, error = %e, "can't read the tenant's maps; waiting for the watch"),
            Err(_) => tracing::debug!(%tenant, "etcd didn't list the tenant's maps in time; waiting for the watch"),
        }
    }
    // A fence etcd showed: wait (bounded) until the watch shows it too, so
    // the retry waits out the hold. Else the old wait for the watch.
    let t0 = Instant::now();
    loop {
        let _ = locator.placement_followed(tenant).await;
        let now = locator.maps_token(tenant);
        if now.2 || now != *before {
            return true;
        }
        if t0.elapsed() >= settle {
            return confirmed;
        }
        let mut changes = locator.placement_changes_for(tenant);
        let _ = tokio::time::timeout(Duration::from_millis(100), changes.changed()).await;
    }
}

/// 1213 for a transaction whose copy was fenced under it.
pub(super) fn txn_lost() -> SprigError {
    SprigError::standard(
        1213,
        *b"40001",
        "Deadlock found when trying to get lock; try restarting transaction (the tenant's data \
         moved while the transaction ran; it was rolled back)",
    )
}

/// 1205 for a request that kept meeting fences.
pub(super) fn fenced_too_long() -> SprigError {
    SprigError::standard(
        1205,
        *b"HY000",
        "Lock wait timeout exceeded; try restarting transaction (the tenant's data kept moving \
         while the statement waited)",
    )
}

/// A sink that holds back a request's first answer when it is a 1146 or
/// 1049 (see the module docs); everything else passes through.
pub(super) struct FenceSink<'a> {
    inner: &'a mut dyn ResultSink,
    armed: bool,
    sent: bool,
    held: Option<SprigError>,
}

impl<'a> FenceSink<'a> {
    /// Wraps `inner`; holds nothing back unless `armed`.
    pub(super) fn new(inner: &'a mut dyn ResultSink, armed: bool) -> Self {
        Self {
            inner,
            armed,
            sent: false,
            held: None,
        }
    }

    /// The error held back, if any.
    pub(super) fn take_held(&mut self) -> Option<SprigError> {
        self.held.take()
    }

    /// Whether anything reached the client.
    pub(super) fn sent(&self) -> bool {
        self.sent
    }
}

#[async_trait]
impl ResultSink for FenceSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.sent = true;
        self.inner.begin_rows(columns, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.sent = true;
        self.inner.begin_rows_raw(defs, format).await
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.sent = true;
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.sent = true;
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.sent = true;
        self.inner.raw_row(payload).await
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.sent = true;
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.sent = true;
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        if self.armed && !self.sent && self.held.is_none() && fence_code(err) {
            self.held = Some(copy(err));
            return Ok(());
        }
        self.sent = true;
        self.inner.error(err).await
    }

    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.sent = true;
        self.inner.column_list(columns, end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.sent = true;
        self.inner.local_infile_request(file_name).await
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    fn finished(&self) -> bool {
        self.held.is_some() || self.inner.finished()
    }
}

impl PassSession {
    /// Waits out holds and promotions, checks the DC's write gate, and
    /// runs `req`, again when it met a mover's fence (see the module docs).
    /// The outer error is a refusal before anything ran (the caller
    /// returns it at once); the inner result is the request's.
    pub(super) async fn run_followed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<Result<ExecSummary>> {
        let t0 = Instant::now();
        let mut attempts = 0u32;
        let mut counted = false;
        loop {
            // Tenant sessions wait out a mover's hold, a promotion (RL13),
            // and follow a move.
            self.follow_tenant().await?;
            // Chapter 25 (R2): writes of tenants homed here need the DC's
            // permit, and so do their acknowledgements.
            let gate = self.dc_write_gate(&req).await?;
            if !counted {
                self.count_traffic(cx, &req);
                counted = true;
            }
            let watch = self.tenant.as_deref().map(|ts| {
                let locator = std::sync::Arc::clone(&ts.services().locator);
                let token = locator.maps_token(ts.tenant);
                (locator, ts.tenant, ts.map(), token)
            });
            let in_trx =
                self.router.in_txn() || self.bs.as_ref().is_some_and(|b| b.pins().in_transaction());
            let mut fs = FenceSink::new(out, watch.is_some());
            let r = match gate {
                Some(f) => {
                    let mut gated = crate::failover::GatedSink::new(&mut fs, f);
                    self.run_request(cx, req.clone(), &mut gated).await
                }
                None => self.run_request(cx, req.clone(), &mut fs).await,
            };
            let (sent, held) = (fs.sent(), fs.take_held());
            drop(fs);
            let (err, via_sink) = match (held, &r) {
                (Some(e), _) => (e, true),
                (None, Err(e)) if watch.is_some() && !sent && fence_code(e) => (copy(e), false),
                _ => return Ok(r),
            };
            let Some((locator, tenant, map, token)) = watch else {
                return Ok(r);
            };
            let reply = if met_fence(&locator, tenant, &map, &token, &err).await {
                if in_trx {
                    metrics::counter!("sprig_gateway_fence_errors_total", "outcome" => "txn_lost")
                        .increment(1);
                    tracing::info!(%tenant, errno = err.protocol().mysql_errno, "a transaction met a mover's fence: 1213");
                    self.fence_rollback().await;
                    txn_lost()
                } else if attempts < MAX_ATTEMPTS && t0.elapsed() < MAX_TOTAL {
                    attempts += 1;
                    metrics::counter!("sprig_gateway_fence_errors_total", "outcome" => "retried")
                        .increment(1);
                    tracing::debug!(%tenant, errno = err.protocol().mysql_errno, attempts, "a statement met a mover's fence: running it again");
                    continue;
                } else {
                    metrics::counter!("sprig_gateway_fence_errors_total", "outcome" => "timeout")
                        .increment(1);
                    fenced_too_long()
                }
            } else {
                err
            };
            if via_sink {
                out.error(&reply).await?;
                return Ok(r);
            }
            return Ok(Err(reply));
        }
    }

    /// Rolls the backend connection's open transaction back, ignoring the
    /// answer (a transaction whose copy was fenced, reported as 1213 as
    /// MariaDB does for a deadlock).
    async fn fence_rollback(&mut self) {
        let Some(bs) = self.bs.as_mut() else {
            return;
        };
        let info = StmtInfo::classify(b"ROLLBACK");
        if bs.query(&info, b"ROLLBACK").await.is_ok() {
            while let Ok(Some(_)) = bs.next().await {}
            let _ = bs.finish().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_named_database_is_read_from_the_message() {
        let e = SprigError::backend(1146, *b"42S02", "Table 'shop.orders' doesn't exist");
        assert_eq!(named_db(&e).as_deref(), Some("shop"));
        let e = SprigError::backend(1049, *b"42000", "Unknown database 't7_d1'");
        assert_eq!(named_db(&e).as_deref(), Some("t7_d1"));
        assert!(fence_code(&e));
        let e = SprigError::backend(1142, *b"42000", "INSERT command denied");
        assert!(fence_code(&e));
        let e = SprigError::backend(1062, *b"23000", "Duplicate entry");
        assert!(!fence_code(&e));
    }

    #[test]
    fn only_the_tenants_databases_count() {
        let map = MemoryNameMap::new(TenantId(7)).with_db(1, "shop", 0, 0);
        let shop = SprigError::backend(1146, *b"42S02", "Table 'shop.orders' doesn't exist");
        assert!(known_db(&shop, &map));
        let phys = SprigError::backend(1049, *b"42000", "Unknown database 't7_d1_g0_s0'");
        assert!(known_db(&phys, &map));
        let other = SprigError::backend(1146, *b"42S02", "Table 'nope.orders' doesn't exist");
        assert!(!known_db(&other, &map));
    }

    mod fresh {
        //! R34 (bugfix5): a genuine 1146 is answered at once; a fence the
        //! watch hasn't shown is still met.
        #![allow(clippy::unwrap_used)]

        use std::sync::Arc;
        use std::time::Duration;

        use futures::StreamExt as _;
        use sprig_catalog::placement::{RangeState, ShardMap};
        use sprig_cluster::registry::ClusterRegistry;
        use sprig_common::ids::ClusterId;
        use sprig_meta::{
            Consistency, Key, KeyValue, LeaseId, ListOpts, ListPage, MemStore, MetaStore, Op,
            Record, Revision, Tier, TierView, Txn, TxnResult, WatchStream,
        };

        use super::*;
        use crate::backend::Backends;
        use crate::backend::cluster::StaticClusters;
        use crate::backend::settings::{BackendSettings, FixedSettings};

        /// A store whose watches never deliver anything: a gateway whose
        /// watch lags behind every write.
        struct NoWatch(Arc<dyn MetaStore>);

        #[async_trait::async_trait]
        impl MetaStore for NoWatch {
            fn tier(&self) -> &Tier {
                self.0.tier()
            }
            async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>> {
                self.0.get(key, c).await
            }
            async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
                self.0.list(prefix, opts).await
            }
            async fn txn(&self, txn: Txn) -> Result<TxnResult> {
                self.0.txn(txn).await
            }
            async fn watch(&self, _prefix: &Key, _from: Revision) -> Result<WatchStream> {
                Ok(futures::stream::pending().boxed())
            }
            async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
                self.0.grant_lease(ttl).await
            }
            async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
                self.0.keep_alive(lease).await
            }
            async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
                self.0.revoke_lease(lease).await
            }
        }

        const T: TenantId = TenantId(7);

        fn dc() -> DcId {
            DcId::new("dc-a").unwrap()
        }

        async fn put_map(store: &Arc<dyn MetaStore>, held: bool) {
            let mut m = ShardMap::catalog(T, ClusterId::new("dc-a-c1").unwrap(), 1);
            if held {
                m.ranges[0].state = RangeState::Held;
            }
            store
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key: ShardMap::catalog_key(store.tier(), T).unwrap(),
                        value: Record::new(m).encode().unwrap(),
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await
                .unwrap();
        }

        /// A locator over `seen` (what its watch shows) whose tenant's
        /// map is in `store`, loaded.
        async fn locator(store: &Arc<dyn MetaStore>, seen: Arc<dyn MetaStore>) -> GatewayLocator {
            put_map(store, false).await;
            let global = TierView::over(store, Tier::Global);
            let backends = Backends::new(
                dc(),
                StaticClusters::new(),
                FixedSettings::new(BackendSettings::default()),
            );
            let registry = ClusterRegistry::new(global, Arc::clone(&seen)).unwrap();
            let l = GatewayLocator::new(seen, registry, backends);
            for _ in 0..200 {
                if l.maps_loaded(T) && l.maps_token(T).1 > 0 {
                    return l;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("the maps never loaded");
        }

        fn missing() -> SprigError {
            SprigError::backend(1146, *b"42S02", "Table 'shop.nope' doesn't exist")
        }

        #[tokio::test]
        async fn a_missing_table_is_answered_at_once() {
            let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc())));
            let l = locator(&store, Arc::clone(&store)).await;
            let map = MemoryNameMap::new(T).with_db(1, "shop", 0, 0);
            let before = l.maps_token(T);
            let t0 = Instant::now();
            assert!(!met_fence(&l, T, &map, &before, &missing()).await);
            let took = t0.elapsed();
            // Before R34's fix: SETTLE (1 s).
            assert!(took < Duration::from_millis(200), "took {took:?}");
            // A privilege really missing: before, REVOKED_SETTLE (300 ms).
            let denied = SprigError::backend(1142, *b"42000", "INSERT command denied");
            let t0 = Instant::now();
            assert!(!met_fence(&l, T, &map, &before, &denied).await);
            let took = t0.elapsed();
            assert!(took < Duration::from_millis(100), "took {took:?}");
        }

        #[tokio::test]
        async fn a_hold_the_watch_hasnt_shown_is_still_a_fence() {
            let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc())));
            let seen: Arc<dyn MetaStore> = Arc::new(NoWatch(Arc::clone(&store)));
            let l = locator(&store, seen).await;
            let map = MemoryNameMap::new(T).with_db(1, "shop", 0, 0);
            let before = l.maps_token(T);
            // The mover holds the map, then renames: the watch shows neither.
            put_map(&store, true).await;
            assert_eq!(l.maps_token(T), before, "the watch must lag here");
            // Before R34's fix: not a fence after 1 s (the 1146 reached
            // the client).
            assert!(met_fence(&l, T, &map, &before, &missing()).await);
            // The hold released (the cutover committed): still a fence for
            // a statement routed before it.
            put_map(&store, false).await;
            assert!(met_fence(&l, T, &map, &before, &missing()).await);
            assert_ne!(l.maps_token_fresh(T).await.unwrap(), before);
        }
    }
}
