//! One tenant's feed for internal consumers: decoded, filtered, ordered,
//! resumable, and following the tenant across moves (chapter 38,
//! "Filtering", "Ordering and transactions", "Positioning and resume").
//!
//! R0 and R1 keep a tenant's catalog and every shard on one cluster at a
//! time and move them together, so the tenant's feed is one cluster's
//! binlog at a time: commit order there is the merged order of all the
//! tenant's shards, transactions whole. The tail reads one **segment**
//! per owner: from a move's `H` on the target until the next move's `G`.
//! When the map shows a committed move, the handoff record
//! ([`crate::ownership`]) says where to stop and where to start: the tail
//! emits the source's changes up to `G`, then the target's after `H`, so a
//! consumer sees no gap and no duplicate, and none of the mover's copies.
//!
//! Filters (chapter 38's rules): only the tenant's shard and reference
//! schemas; no internal tables or DDL naming them; no transaction that
//! writes an applier position (rule 2); a segment's window (rule 3);
//! internal columns removed and no-op updates dropped (rule 4).
//!
//! Every item carries a [`ResumeToken`]; [`StartAt::After`] resumes after
//! it, also across moves that committed in between.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use sprig_binlog::{Gtid, GtidPos};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::names::{SchemaName, is_internal_object};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use tokio::time::Instant;

use crate::binlog::schemas_in;
use crate::decode::{ddl_op, ddl_table, decode_row};
use crate::event::{ChangeEvent, EventId, FeedItem, Image, ShardRef};
use crate::hlc::next_clamp;
use crate::names::{Resolver, logical_sql};
use crate::ownership::Handoff;
use crate::placement::PlacementView;
use crate::source::{Clusters, RawRow, RawTxn, SourceItem, TxnSource, XaMark};
use crate::token::{AFTER_TXN, ResumeToken};

/// Where a tail starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartAt {
    /// The tenant's cluster now, at its binlog position now.
    Now,
    /// Just after a token.
    After(ResumeToken),
    /// A cluster at a position (tests, and a snapshot's hand-over): the
    /// empty position reads the cluster's oldest binlog.
    At {
        /// The cluster.
        cluster: ClusterId,
        /// The position.
        pos: GtidPos,
    },
}

/// A tail's timing.
#[derive(Debug, Clone)]
pub struct TailOptions {
    /// Idle progress items at most this often.
    pub progress_every: Duration,
    /// Check the map for a move at most this often.
    pub handoff_poll: Duration,
    /// Warn when the map moved but no handoff record says how
    /// (`ER_SPRIG_FEED_HANDOFF_PENDING`) for this long.
    pub handoff_alert: Duration,
    /// First retry delay after a broken stream; doubles up to 5 s.
    pub retry: Duration,
    /// Internal tables whose rows are delivered anyway, by name prefix:
    /// chapter 45's sequencers read their outboxes (`__sprig_eo_`).
    /// Empty: filter rule 1 drops every internal table.
    pub keep_internal: Vec<String>,
}

impl Default for TailOptions {
    fn default() -> Self {
        Self {
            progress_every: Duration::from_secs(1),
            handoff_poll: Duration::from_millis(500),
            handoff_alert: Duration::from_secs(300),
            retry: Duration::from_millis(100),
            keep_internal: Vec::new(),
        }
    }
}

/// An XA branch prepared but not committed yet (file binlogs).
#[derive(Debug)]
struct Held {
    xid: String,
    before: GtidPos,
    rows: Vec<RawRow>,
    stamps: Vec<u64>,
    read: u64,
}

/// Skipping what a resume token already delivered.
#[derive(Debug, Clone, Copy)]
struct Until {
    gtid: Gtid,
    row: u32,
    clamp: u64,
}

/// A schema, resolved.
#[derive(Debug, Clone)]
struct SchemaInfo {
    db: DbId,
    db_name: String,
    shard: ShardRef,
}

/// The feed of one tenant.
pub struct TenantTail {
    tenant: TenantId,
    opts: TailOptions,
    clusters: Arc<dyn Clusters>,
    placement: Arc<dyn PlacementView>,
    names: Arc<dyn Resolver>,
    cluster: ClusterId,
    segment: u64,
    src: Option<Box<dyn TxnSource>>,
    /// The position after the last group read.
    pos: GtidPos,
    clamp: u64,
    until: Option<Until>,
    /// The last transaction whose items were all delivered, and its read
    /// number.
    delivered: Option<(Gtid, u64)>,
    reads: u64,
    held: Vec<Held>,
    next: Option<Handoff>,
    seen_version: Option<u64>,
    last_check: Option<Instant>,
    moved_since: Option<Instant>,
    last_progress: Instant,
    failures: u32,
    out: VecDeque<FeedItem>,
    schemas: HashMap<String, SchemaInfo>,
    objects: HashMap<(DbId, String), u64>,
}

impl std::fmt::Debug for TenantTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantTail")
            .field("tenant", &self.tenant)
            .field("cluster", &self.cluster)
            .field("segment", &self.segment)
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

fn retryable(e: &SprigError) -> bool {
    e.retryable
        || matches!(
            e.kind,
            ErrorKind::Sprig(
                SprigCode::BinlogDisconnected
                    | SprigCode::BackendUnavailable
                    | SprigCode::CatalogUnavailable
                    | SprigCode::MetaUnavailable
            )
        )
}

/// Whether `g` lies after position `f` in its domain.
fn beyond(f: &GtidPos, g: &Gtid) -> bool {
    f.get(g.domain).is_some_and(|x| g.seq > x.seq)
}

/// Runs a catalog lookup, retrying while the catalog is unavailable (a
/// move's fence renames it for up to the hold).
async fn lookup<T, F, Fut>(
    names: &Arc<dyn Resolver>,
    tenant: TenantId,
    first_delay: Duration,
    f: F,
) -> Result<T>
where
    F: Fn(Arc<dyn Resolver>, TenantId) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = first_delay;
    for _ in 0..60 {
        match f(names.clone(), tenant).await {
            Ok(v) => return Ok(v),
            Err(e) if retryable(&e) => {
                tracing::debug!(error = %e, "catalog lookup failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
            Err(e) => return Err(e),
        }
    }
    Err(SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("tenant {tenant}'s catalog stayed unreadable"),
    ))
}

impl TenantTail {
    /// Starts `tenant`'s tail at `start`.
    ///
    /// # Errors
    /// 39801 for another tenant's token or one the ownership history
    /// doesn't explain; the store's and clusters' errors.
    pub async fn start(
        tenant: TenantId,
        start: StartAt,
        clusters: Arc<dyn Clusters>,
        placement: Arc<dyn PlacementView>,
        names: Arc<dyn Resolver>,
        opts: TailOptions,
    ) -> Result<Self> {
        let history = placement.history(tenant).await?;
        let (cluster, segment, pos, clamp, until) = match start {
            StartAt::Now => {
                let (c, _) = placement.current(tenant).await?;
                let pos = clusters.position(&c).await?;
                let seg = history.last().map_or(0, |h| h.map_version);
                (c, seg, pos, 0, None)
            }
            StartAt::At { cluster, pos } => {
                let seg = history
                    .iter()
                    .rev()
                    .find(|h| h.to_cluster == cluster)
                    .map_or(0, |h| h.map_version);
                (cluster, seg, pos, 0, None)
            }
            StartAt::After(t) => {
                t.check_tenant(tenant)?;
                let known = if t.segment == 0 {
                    history.first().is_none_or(|h| h.from_cluster == t.cluster)
                } else {
                    history
                        .iter()
                        .any(|h| h.map_version == t.segment && h.to_cluster == t.cluster)
                };
                if !known {
                    return Err(SprigError::new(
                        SprigCode::FeedBadResumeToken,
                        "the resume token names a placement the tenant never had",
                    ));
                }
                let until = t.until.map(|(gtid, row)| Until {
                    gtid,
                    row,
                    clamp: t.clamp,
                });
                (t.cluster, t.segment, t.pos, t.clamp, until)
            }
        };
        let mut tail = Self {
            tenant,
            opts,
            clusters,
            placement,
            names,
            cluster,
            segment,
            src: None,
            pos,
            clamp,
            delivered: until.map(|u| (u.gtid, 0)),
            until,
            reads: 0,
            held: Vec::new(),
            next: None,
            seen_version: None,
            last_check: None,
            moved_since: None,
            last_progress: Instant::now(),
            failures: 0,
            out: VecDeque::new(),
            schemas: HashMap::new(),
            objects: HashMap::new(),
        };
        tail.drop_passed_until();
        tail.find_next(&history);
        Ok(tail)
    }

    /// The cluster being read.
    #[must_use]
    pub fn cluster(&self) -> &ClusterId {
        &self.cluster
    }

    /// The position after the last group read.
    #[must_use]
    pub fn position(&self) -> &GtidPos {
        &self.pos
    }

    /// The handoff out of the current segment, once known.
    fn find_next(&mut self, history: &[Handoff]) {
        if self.next.is_some() {
            return;
        }
        if let Some(h) = history.iter().find(|h| h.map_version > self.segment) {
            if h.from_cluster == self.cluster {
                self.next = Some(h.clone());
            } else {
                tracing::error!(
                    tenant = %self.tenant,
                    cluster = %self.cluster,
                    from = %h.from_cluster,
                    "a handoff leaves a cluster this feed isn't reading"
                );
            }
        }
    }

    /// Looks for a committed move out of the segment (at most every
    /// `handoff_poll` unless `force`).
    async fn check_handoff(&mut self, force: bool) -> Result<()> {
        if self.next.is_some() {
            return Ok(());
        }
        if !force
            && self
                .last_check
                .is_some_and(|t| t.elapsed() < self.opts.handoff_poll)
        {
            return Ok(());
        }
        self.last_check = Some(Instant::now());
        let (cur, version) = self.placement.current(self.tenant).await?;
        if self.seen_version == Some(version) && cur == self.cluster {
            return Ok(());
        }
        let history = self.placement.history(self.tenant).await?;
        self.find_next(&history);
        if self.next.is_some() || cur == self.cluster {
            self.seen_version = Some(version);
            self.moved_since = None;
            return Ok(());
        }
        // The map points elsewhere but no record says how: keep reading.
        let since = *self.moved_since.get_or_insert_with(Instant::now);
        if since.elapsed() >= self.opts.handoff_alert {
            let e = SprigError::new(
                SprigCode::FeedHandoffPending,
                format!(
                    "tenant {} moved from {} to {cur} but no handoff record exists",
                    self.tenant, self.cluster
                ),
            );
            tracing::error!(error = %e, "feed waits for a handoff");
            self.moved_since = Some(Instant::now());
        }
        Ok(())
    }

    /// Moves to the next segment if the current one has reached its fence.
    async fn maybe_switch(&mut self) -> Result<bool> {
        let Some(h) = self.next.as_ref() else {
            return Ok(false);
        };
        let fence = h.fence()?;
        if !self.pos.reached(&fence) {
            return Ok(false);
        }
        let h = h.clone();
        let mut start = h.start()?;
        // `H` keeps only a Galera cluster's own domain; other domains hold
        // node-local writes, never the tenant's.
        let now = self.clusters.position(&h.to_cluster).await?;
        for g in now.iter() {
            if start.get(g.domain).is_none() {
                start.update(*g);
            }
        }
        if !self.held.is_empty() {
            tracing::warn!(
                tenant = %self.tenant,
                held = self.held.len(),
                "XA branches still prepared at a move's fence are dropped"
            );
            self.held.clear();
        }
        tracing::info!(
            tenant = %self.tenant,
            from = %self.cluster,
            to = %h.to_cluster,
            fence = %h.fence_pos,
            start = %start,
            "feed follows a move"
        );
        metrics::counter!("sprig_feed_handoffs_total").increment(1);
        self.cluster = h.to_cluster.clone();
        self.segment = h.map_version;
        self.pos = start;
        self.src = None;
        self.clamp = self.clamp.max(h.hlc_floor);
        self.next = None;
        self.seen_version = None;
        let history = self.placement.history(self.tenant).await?;
        self.find_next(&history);
        Ok(true)
    }

    fn token_pos(&self, fallback: &GtidPos) -> GtidPos {
        self.held
            .first()
            .map_or_else(|| fallback.clone(), |h| h.before.clone())
    }

    fn token(
        &self,
        pos: GtidPos,
        until: Option<(Gtid, u32)>,
        clamp: u64,
        progress: bool,
    ) -> ResumeToken {
        ResumeToken {
            tenant: self.tenant,
            clamp,
            cluster: self.cluster.clone(),
            segment: self.segment,
            pos,
            until,
            progress,
        }
    }

    /// A progress token for "everything read so far".
    fn progress_token(&self) -> ResumeToken {
        let pos = self.token_pos(&self.pos);
        // With a prepared XA branch the position lies back at its prepare;
        // skip again what was delivered after it.
        let until = match (self.held.first(), self.delivered) {
            (Some(h), Some((g, n))) if n >= h.read => Some((g, AFTER_TXN)),
            _ => None,
        };
        self.token(pos, until, self.clamp, true)
    }

    async fn schema(&mut self, name: &str) -> Result<Option<SchemaInfo>> {
        if let Some(s) = self.schemas.get(name) {
            return Ok(Some(s.clone()));
        }
        let Ok(parsed) = name.parse::<SchemaName>() else {
            return Ok(None);
        };
        if parsed.tenant() != self.tenant {
            return Ok(None);
        }
        let (db, shard) = match parsed {
            SchemaName::Shard {
                db, group, shard, ..
            } => (db, ShardRef { group, shard }),
            SchemaName::Reference { db, .. } => (
                db,
                ShardRef {
                    group: GroupId(0),
                    shard: ShardId(0),
                },
            ),
            SchemaName::Catalog { .. } => return Ok(None),
        };
        let db_name = lookup(
            &self.names,
            self.tenant,
            self.opts.retry,
            |names, tenant| async move { names.database(tenant, db).await },
        )
        .await?
        .unwrap_or_else(|| format!("db{}", db.0));
        let info = SchemaInfo { db, db_name, shard };
        self.schemas.insert(name.to_owned(), info.clone());
        Ok(Some(info))
    }

    async fn object(&mut self, db: DbId, table: &str) -> Result<u64> {
        if let Some(id) = self.objects.get(&(db, table.to_owned())) {
            return Ok(*id);
        }
        let t = table.to_owned();
        let id = lookup(
            &self.names,
            self.tenant,
            self.opts.retry,
            |names, tenant| {
                let t = t.clone();
                async move { names.object(tenant, db, &t).await }
            },
        )
        .await?
        .unwrap_or(0);
        self.objects.insert((db, table.to_owned()), id);
        Ok(id)
    }

    /// Handles one group: XA bookkeeping, skipping, filters, decoding.
    #[allow(clippy::too_many_lines)] // the chapter's steps, in order
    async fn on_txn(&mut self, t: RawTxn) -> Result<()> {
        self.reads += 1;
        let read = self.reads;
        // Skipping what a resume token already delivered.
        let mut resume_at: Option<Until> = None;
        if let Some(u) = self.until {
            if t.gtid == u.gtid {
                self.until = None;
                self.clamp = u.clamp;
                resume_at = Some(u);
            } else if t.gtid.domain == u.gtid.domain && t.gtid.seq > u.gtid.seq {
                // Passed it (the token's transaction was before the
                // restart position): nothing here was delivered.
                self.until = None;
                self.clamp = u.clamp;
            } else {
                self.xa_bookkeeping(&t, read);
                return Ok(());
            }
        }
        // Rows, with XA branches held until their commit.
        let mut pos_for_events = t.before.clone();
        let (rows, stamps) = match &t.xa {
            Some(XaMark::Prepare(_)) => {
                self.xa_bookkeeping(&t, read);
                return Ok(());
            }
            Some(XaMark::Rollback(x)) => {
                self.held.retain(|h| &h.xid != x);
                return Ok(());
            }
            Some(XaMark::Commit(x)) => {
                if let Some(h) = self.held.first() {
                    pos_for_events = h.before.clone();
                }
                let i = self.held.iter().position(|h| &h.xid == x);
                let (mut rows, mut stamps) = if let Some(i) = i {
                    let h = self.held.remove(i);
                    (h.rows, h.stamps)
                } else {
                    if t.rows.is_empty() {
                        tracing::warn!(
                            tenant = %self.tenant,
                            xid = %x,
                            "XA COMMIT of a branch whose prepare this feed never read"
                        );
                    }
                    (Vec::new(), Vec::new())
                };
                rows.extend(t.rows.iter().cloned());
                stamps.extend(t.stamps.iter().copied());
                (rows, stamps)
            }
            None => {
                if let Some(h) = self.held.first() {
                    pos_for_events = h.before.clone();
                }
                (t.rows.clone(), t.stamps.clone())
            }
        };
        if let Some(db) = &t.statement_dml {
            return Err(SprigError::new(
                SprigCode::FeedStatementEvent,
                format!(
                    "statement-format DML in a shard schema of tenant {} (database {})",
                    self.tenant,
                    db.parse::<SchemaName>()
                        .ok()
                        .and_then(|s| s.db())
                        .map_or(0, |d| d.0)
                ),
            ));
        }
        if t.apply {
            metrics::counter!("sprig_feed_filtered_total", "reason" => "apply").increment(1);
            return Ok(());
        }
        // Decode.
        let mut events: Vec<(u32, ChangeEvent)> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            let idx = u32::try_from(i).unwrap_or(u32::MAX - 1);
            if is_internal_object(&r.table.table)
                && !self
                    .opts
                    .keep_internal
                    .iter()
                    .any(|p| r.table.table.starts_with(p.as_str()))
            {
                metrics::counter!("sprig_feed_filtered_total", "reason" => "internal").increment(1);
                continue;
            }
            let Some(info) = self.schema(&r.table.schema).await? else {
                continue;
            };
            let Some(d) = decode_row(r) else {
                metrics::counter!("sprig_feed_filtered_total", "reason" => "noop").increment(1);
                continue;
            };
            let object = self.object(info.db, &r.table.table).await?;
            events.push((
                idx,
                ChangeEvent {
                    id: EventId {
                        cluster: self.cluster.clone(),
                        gtid: t.gtid,
                        row: idx,
                    },
                    token: self.token(GtidPos::new(), None, 0, false),
                    tenant: self.tenant,
                    db: info.db,
                    db_name: info.db_name.clone(),
                    object,
                    table: r.table.table.clone(),
                    shard: info.shard,
                    key: d.key,
                    op: d.op,
                    before: d.before,
                    after: d.after,
                    delta: d.delta,
                    hlc: 0,
                    txn: t.gtid,
                    ddl: None,
                },
            ));
        }
        if let Some(ddl) = &t.ddl
            && !ddl.sql.to_ascii_lowercase().contains("__sprig_")
        {
            let mut named = schemas_in(&ddl.sql, self.tenant);
            if named.is_empty() {
                named.push(ddl.db.clone());
            }
            let mut logical = HashMap::new();
            let mut first: Option<(String, SchemaInfo)> = None;
            for s in &named {
                if let Some(info) = self.schema(s).await? {
                    logical.insert(s.clone(), info.db_name.clone());
                    first.get_or_insert((s.clone(), info));
                }
            }
            if let Some((schema, info)) = first {
                let table = ddl_table(&ddl.sql, &schema, &ddl.db).unwrap_or_default();
                let object = if table.is_empty() {
                    0
                } else {
                    self.object(info.db, &table).await?
                };
                let text = logical_sql(&ddl.sql, self.tenant, &logical);
                events.push((
                    0,
                    ChangeEvent {
                        id: EventId {
                            cluster: self.cluster.clone(),
                            gtid: t.gtid,
                            row: 0,
                        },
                        token: self.token(GtidPos::new(), None, 0, false),
                        tenant: self.tenant,
                        db: info.db,
                        db_name: info.db_name,
                        object,
                        table,
                        shard: info.shard,
                        key: Image::default(),
                        op: ddl_op(crate::binlog::strip_comments(&ddl.sql)),
                        before: None,
                        after: None,
                        delta: None,
                        hlc: 0,
                        txn: t.gtid,
                        ddl: text,
                    },
                ));
            }
        }
        if events.is_empty() {
            return Ok(());
        }
        let clamp = match resume_at {
            Some(u) => u.clamp,
            None => next_clamp(self.clamp, stamps.iter().copied().max(), t.ts),
        };
        self.clamp = clamp;
        let skip_to = resume_at.map(|u| u.row);
        if skip_to == Some(AFTER_TXN) {
            self.delivered = Some((t.gtid, read));
            return Ok(());
        }
        for (idx, mut e) in events {
            if skip_to.is_some_and(|r| idx <= r) {
                continue;
            }
            e.hlc = clamp;
            e.token = self.token(pos_for_events.clone(), Some((t.gtid, idx)), clamp, false);
            metrics::counter!("sprig_feed_events_total", "cluster" => self.cluster.to_string())
                .increment(1);
            self.out.push_back(FeedItem::Event(Box::new(e)));
        }
        let after = t.after();
        let end_pos = self.token_pos(&after);
        let until = Some((t.gtid, AFTER_TXN));
        self.out.push_back(FeedItem::TxnEnd {
            token: self.token(end_pos, until, clamp, false),
        });
        self.delivered = Some((t.gtid, read));
        Ok(())
    }

    /// Ends skipping once the stream is past the token's transaction
    /// without having read it again (it lay before the restart position):
    /// nothing more of it can come.
    fn drop_passed_until(&mut self) {
        if let Some(u) = self.until
            && self
                .pos
                .get(u.gtid.domain)
                .is_some_and(|g| g.seq >= u.gtid.seq)
            && self.held.is_empty()
        {
            self.until = None;
            self.clamp = u.clamp;
        }
    }

    /// While skipping: keep XA state as if the group had been read.
    fn xa_bookkeeping(&mut self, t: &RawTxn, read: u64) {
        match &t.xa {
            Some(XaMark::Prepare(x)) => self.held.push(Held {
                xid: x.clone(),
                before: t.before.clone(),
                rows: t.rows.clone(),
                stamps: t.stamps.clone(),
                read,
            }),
            Some(XaMark::Commit(x) | XaMark::Rollback(x)) => self.held.retain(|h| &h.xid != x),
            None => {}
        }
    }

    /// The next item: events of a transaction in order, then its
    /// `TxnEnd`; `Progress` while there is nothing for the tenant.
    ///
    /// # Errors
    /// 39800 when a cluster no longer has the position (an internal
    /// consumer then starts from a snapshot); 39810 on statement-format
    /// DML; the placement store's errors. Broken streams and unreadable
    /// catalogs are retried.
    pub async fn next(&mut self) -> Result<FeedItem> {
        loop {
            if let Some(i) = self.out.pop_front() {
                return Ok(i);
            }
            match self.maybe_switch().await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) if retryable(&e) => {
                    self.backoff(&e).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
            if let Err(e) = self.check_handoff(false).await {
                if !retryable(&e) {
                    return Err(e);
                }
                tracing::debug!(error = %e, "feed can't read the map now");
            }
            if self.src.is_none() {
                match self
                    .clusters
                    .open(&self.cluster, self.tenant, &self.pos)
                    .await
                {
                    Ok(s) => self.src = Some(s),
                    Err(e) if retryable(&e) => {
                        self.backoff(&e).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            let Some(src) = self.src.as_mut() else {
                continue;
            };
            let item = match src.next().await {
                Ok(i) => i,
                Err(e) if retryable(&e) => {
                    self.src = None;
                    self.backoff(&e).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            self.failures = 0;
            match item {
                SourceItem::Txn(t) => {
                    if let Some(h) = &self.next
                        && beyond(&h.fence()?, &t.gtid)
                    {
                        // Past the fence: the segment is over (the switch
                        // happens at the top of the loop).
                        self.pos = t.before.clone();
                        self.pos.update(t.gtid);
                        continue;
                    }
                    let after = t.after();
                    self.on_txn(t).await?;
                    self.pos = after;
                }
                SourceItem::Idle { pos } => {
                    if pos.reached(&self.pos) {
                        self.pos = pos;
                    }
                    self.drop_passed_until();
                    self.check_handoff(false)
                        .await
                        .or_else(|e| if retryable(&e) { Ok(()) } else { Err(e) })?;
                    if self.until.is_none()
                        && self.last_progress.elapsed() >= self.opts.progress_every
                    {
                        self.last_progress = Instant::now();
                        return Ok(FeedItem::Progress {
                            token: self.progress_token(),
                        });
                    }
                }
            }
        }
    }

    async fn backoff(&mut self, e: &SprigError) {
        self.failures = self.failures.saturating_add(1);
        let exp = self.failures.min(6);
        let delay = (self.opts.retry * (1u32 << exp)).min(Duration::from_secs(5));
        tracing::warn!(
            tenant = %self.tenant,
            cluster = %self.cluster,
            error = %e,
            ?delay,
            "feed stream broke; reopening"
        );
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sprig_binlog::{RowImage, Value};

    use super::*;
    use crate::names::StaticResolver;
    use crate::source::{RowKind, TableShape};

    const T: TenantId = TenantId(7);

    fn c1() -> ClusterId {
        ClusterId::new("dc-a-c1").unwrap()
    }

    fn g(seq: u64) -> Gtid {
        Gtid {
            domain: 1,
            server: 1,
            seq,
        }
    }

    fn row(k: i64) -> RawRow {
        RawRow {
            table: Arc::new(TableShape {
                schema: "t7_d1_g1_s1".into(),
                table: "a".into(),
                columns: vec!["k".into()],
                key: vec![0],
            }),
            kind: RowKind::Insert,
            before: None,
            after: Some(RowImage(vec![Some(Value::Int(k))])),
        }
    }

    fn txn(seq: u64, rows: Vec<RawRow>, xa: Option<XaMark>) -> RawTxn {
        let before = if seq == 1 {
            GtidPos::new()
        } else {
            GtidPos::from_gtids([g(seq - 1)])
        };
        let mut t = RawTxn::new(g(seq), before, 100);
        t.rows = rows;
        t.xa = xa;
        t
    }

    /// One cluster whose binlog is a fixed list; idle at its end.
    struct Fixed(Vec<RawTxn>);
    struct FixedSrc {
        txns: Vec<RawTxn>,
        next: usize,
        idle: Arc<Mutex<u32>>,
    }

    #[async_trait::async_trait]
    impl TxnSource for FixedSrc {
        async fn next(&mut self) -> Result<SourceItem> {
            if let Some(t) = self.txns.get(self.next) {
                self.next += 1;
                return Ok(SourceItem::Txn(t.clone()));
            }
            *self.idle.lock().unwrap() += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(SourceItem::Idle {
                pos: GtidPos::from_gtids([g(self.txns.len() as u64)]),
            })
        }
    }

    #[async_trait::async_trait]
    impl Clusters for Fixed {
        async fn open(
            &self,
            _: &ClusterId,
            _: TenantId,
            from: &GtidPos,
        ) -> Result<Box<dyn TxnSource>> {
            let n = from.get(1).map_or(0, |x| x.seq);
            Ok(Box::new(FixedSrc {
                txns: self.0.clone(),
                next: usize::try_from(n).unwrap(),
                idle: Arc::default(),
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
            Ok((c1(), 1))
        }
        async fn history(&self, _: TenantId) -> Result<Vec<Handoff>> {
            Ok(Vec::new())
        }
    }

    async fn tail(txns: Vec<RawTxn>, start: StartAt) -> TenantTail {
        TenantTail::start(
            T,
            start,
            Arc::new(Fixed(txns)),
            Arc::new(Here),
            Arc::new(StaticResolver::new().database(T, DbId(1), "shop")),
            TailOptions {
                progress_every: Duration::from_millis(1),
                ..TailOptions::default()
            },
        )
        .await
        .unwrap()
    }

    /// Items up to the first progress item.
    async fn drain(t: &mut TenantTail) -> Vec<FeedItem> {
        let mut out = Vec::new();
        loop {
            let i = t.next().await.unwrap();
            if matches!(i, FeedItem::Progress { .. }) {
                out.push(i);
                return out;
            }
            out.push(i);
        }
    }

    fn keys(items: &[FeedItem]) -> Vec<i64> {
        items
            .iter()
            .filter_map(|i| match i {
                FeedItem::Event(e) => match e.after.as_ref()?.get("k")? {
                    Value::Int(k) => Some(*k),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn xa_branches_publish_at_commit_and_resume_without_duplicates() {
        let binlog = vec![
            txn(1, vec![row(1)], None),
            txn(2, vec![row(20), row(21)], Some(XaMark::Prepare("x".into()))),
            txn(3, vec![row(3)], None),
            txn(4, vec![row(40)], Some(XaMark::Prepare("y".into()))),
            txn(5, vec![], Some(XaMark::Rollback("y".into()))),
            txn(6, vec![], Some(XaMark::Commit("x".into()))),
            txn(7, vec![row(7)], None),
        ];
        let start = StartAt::At {
            cluster: c1(),
            pos: GtidPos::new(),
        };
        let mut t = tail(binlog.clone(), start).await;
        let items = drain(&mut t).await;
        assert_eq!(keys(&items), vec![1, 3, 20, 21, 7]);
        // While "x" was prepared, tokens point back at its prepare.
        let after3 = items
            .iter()
            .find_map(|i| match i {
                FeedItem::TxnEnd { token } if token.until.map(|u| u.0) == Some(g(3)) => {
                    Some(token.clone())
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(after3.pos, GtidPos::from_gtids([g(1)]));
        // Resuming there skips 3 but publishes "x" at its commit.
        let mut r = tail(binlog.clone(), StartAt::After(after3)).await;
        assert_eq!(keys(&drain(&mut r).await), vec![20, 21, 7]);
        // Resuming after the first row of the XA commit gives the second.
        let mid = items
            .iter()
            .find_map(|i| match i {
                FeedItem::Event(e) if keys(std::slice::from_ref(i)) == [20] => {
                    Some(e.token.clone())
                }
                _ => None,
            })
            .unwrap();
        let mut r = tail(binlog.clone(), StartAt::After(mid)).await;
        assert_eq!(keys(&drain(&mut r).await), vec![21, 7]);
        // Clamps rise per transaction and repeat on a re-read.
        let clamps: Vec<u64> = items
            .iter()
            .filter_map(|i| match i {
                FeedItem::Event(e) => Some(e.hlc),
                _ => None,
            })
            .collect();
        assert!(clamps.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(clamps[2], clamps[3]);
        // The last token resumes at the end.
        let last = items.last().unwrap().token().clone();
        let mut r = tail(binlog, StartAt::After(last)).await;
        assert!(keys(&drain(&mut r).await).is_empty());
    }

    #[tokio::test]
    async fn statement_dml_and_apply_traffic() {
        let mut apply = txn(1, vec![row(1)], None);
        apply.apply = true;
        let mut stmt = txn(2, vec![], None);
        stmt.statement_dml = Some("t7_d1_g1_s1".into());
        let start = StartAt::At {
            cluster: c1(),
            pos: GtidPos::new(),
        };
        let mut t = tail(vec![apply, stmt], start).await;
        let e = t.next().await.unwrap_err();
        assert!(matches!(
            e.kind,
            ErrorKind::Sprig(SprigCode::FeedStatementEvent)
        ));
    }
}
