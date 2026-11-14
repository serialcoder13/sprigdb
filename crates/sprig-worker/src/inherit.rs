//! `inherit_copy` (chapter 42, "Materialized copies"; build step 8): one
//! long-running job per tenant tree, named by its root tenant
//! (`spec: {"root": <tenant id>}`), keeping the tree's materialized copies
//! and orphaning overrides across its tenants
//! ([`sprig_inherit::copy::Maintainer`]).
//!
//! ```text
//! serve(root):
//!   maintainer.refresh()                    # catalogs, copies' states; seeds
//!   for each tenant of the tree with inheritable objects:
//!     open its feed after its `inherit:f<root>` checkpoint (registering the
//!     subscription at the cluster's position now if it has none)
//!   loop:
//!     take what the feeds have (at most 256 items or 50 ms)
//!     group the row events of inheritable objects by (tenant, db, table)
//!     maintainer.on_keys(...) for each group; on a retryable error wait
//!       and apply the same batch again (nothing is acknowledged before)
//!     each tenant's position moves to its last item's token
//!     every 500 ms: acknowledge moved positions
//!     every 2 s: refresh (new shares seed; dropped ones are cleaned);
//!       open feeds of tenants that joined
//!     every 1 s, when every feed reported progress with nothing pending:
//!       record that the copies are caught up
//!     every hour: purge tombstones older than inherit.copy_tombstone_hours
//! ```
//!
//! Delivery is at least once: after a crash, a new owner resumes after the
//! last acknowledgement and applies the events after it again, which is
//! harmless because each application recomputes the current state. The
//! maintainer's epoch is the job's attempt, so a worker that lost the job
//! can't overwrite what its successor wrote.
//!
//! The loop ([`serve`]) is written against [`Feeds`] and [`Apply`], so the
//! acknowledgement protocol is tested with a model feed and a model
//! maintainer (worker kills at random points, no event missed).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_catalog::cache::{CatalogSource, MariaSource, Node};
use sprig_catalog::{CatalogExtension, CatalogLocator, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_controller::tree::Directory;
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::subs::Subscriptions;
use sprig_feed::{
    Clusters, FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions, TenantTail,
};
use sprig_inherit::copy::{CopyEnv, KeyLit, Maintainer, key_text};
use sprig_meta::MetaStore;
use tokio::sync::mpsc;

use crate::logic::RegistryClusters;
use crate::runtime::{JobCtx, JobHandler};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// One tenant's feed, as the loop reads it.
#[async_trait]
pub trait Tail: Send {
    /// The next item.
    ///
    /// # Errors
    /// The feed's.
    async fn next(&mut self) -> Result<FeedItem>;
}

#[async_trait]
impl Tail for TenantTail {
    async fn next(&mut self) -> Result<FeedItem> {
        TenantTail::next(self).await
    }
}

/// Opening feeds and acknowledging positions.
#[async_trait]
pub trait Feeds: Send + Sync {
    /// Opens `tenant`'s feed after its checkpoint for `root`'s tree.
    ///
    /// # Errors
    /// The feed's.
    async fn open(&self, root: TenantId, tenant: TenantId) -> Result<Box<dyn Tail>>;

    /// Acknowledges `tenant`'s feed up to `token`, `events` more.
    ///
    /// # Errors
    /// The store's.
    async fn ack(
        &self,
        root: TenantId,
        tenant: TenantId,
        token: &ResumeToken,
        events: u64,
    ) -> Result<()>;
}

/// What the loop does with the feeds' rows.
#[async_trait]
pub trait Apply: Send {
    /// Re-reads the tree; returns the tenants whose feeds to read.
    ///
    /// # Errors
    /// The catalogs' and backends'.
    async fn refresh(&mut self) -> Result<Vec<TenantId>>;

    /// Whether a change of `db.table` in `tenant` matters, and the names
    /// of its key columns.
    fn key_of(&self, tenant: TenantId, db: &str, table: &str) -> Option<Vec<String>>;

    /// Applies changed keys.
    ///
    /// # Errors
    /// The backends'.
    async fn apply(
        &mut self,
        tenant: TenantId,
        db: &str,
        table: &str,
        keys: &[KeyLit],
    ) -> Result<()>;

    /// Records that nothing was left to apply at `at_ms`.
    ///
    /// # Errors
    /// The backends'.
    async fn caught_up(&mut self, at_ms: u64) -> Result<()>;

    /// Purges old tombstones.
    ///
    /// # Errors
    /// The backends'.
    async fn purge(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Apply for Maintainer {
    async fn refresh(&mut self) -> Result<Vec<TenantId>> {
        Maintainer::refresh(self).await?;
        let f = self.family();
        if !f.active() {
            return Ok(Vec::new());
        }
        Ok(f.tenants
            .iter()
            .filter(|t| {
                f.cats
                    .get(t)
                    .and_then(|c| sprig_inherit::plan::state_of(c))
                    .is_some_and(|s| s.objects().next().is_some())
            })
            .copied()
            .collect())
    }

    fn key_of(&self, tenant: TenantId, db: &str, table: &str) -> Option<Vec<String>> {
        let cat = self.family().cats.get(&tenant)?;
        let st = sprig_inherit::plan::state_of(cat)?;
        st.objects()
            .find(|o| {
                o.table.eq_ignore_ascii_case(table)
                    && cat
                        .databases
                        .get(o.db)
                        .is_some_and(|d| d.name.eq_ignore_ascii_case(db))
            })
            .map(|o| o.key.clone())
    }

    async fn apply(
        &mut self,
        tenant: TenantId,
        db: &str,
        table: &str,
        keys: &[KeyLit],
    ) -> Result<()> {
        self.on_keys(tenant, db, table, keys).await
    }

    async fn caught_up(&mut self, at_ms: u64) -> Result<()> {
        Maintainer::caught_up(self, at_ms).await
    }

    async fn purge(&mut self) -> Result<()> {
        let n = Maintainer::purge(self, 24).await?;
        if n > 0 {
            tracing::info!(tombstones = n, "copy tombstones purged");
        }
        Ok(())
    }
}

/// The loop's timing.
#[derive(Debug, Clone)]
pub struct ServeOpts {
    /// Acknowledge at most this often.
    pub ack_every: Duration,
    /// Refresh the tree this often.
    pub refresh_every: Duration,
    /// Wait this long before applying a failed batch again.
    pub retry: Duration,
    /// Items per batch.
    pub batch: usize,
    /// Purge tombstones this often.
    pub purge_every: Duration,
}

impl Default for ServeOpts {
    fn default() -> Self {
        Self {
            ack_every: Duration::from_millis(500),
            refresh_every: Duration::from_secs(2),
            retry: Duration::from_millis(500),
            batch: 256,
            purge_every: Duration::from_secs(3600),
        }
    }
}

/// What a run did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServeStats {
    /// Row events applied (a key changed in a transaction counts once per
    /// event).
    pub events: u64,
    /// Batches applied.
    pub batches: u64,
    /// Batches applied again after an error.
    pub retries: u64,
    /// Acknowledgements written.
    pub acks: u64,
}

type Item = (TenantId, Result<FeedItem>);

fn spawn_tail(
    feeds: Arc<dyn Feeds>,
    root: TenantId,
    tenant: TenantId,
    tx: mpsc::Sender<Item>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tail = tokio::select! {
            () = stop.cancelled() => return,
            t = feeds.open(root, tenant) => match t {
                Ok(t) => t,
                Err(e) => {
                    let _ = tx.send((tenant, Err(e))).await;
                    return;
                }
            },
        };
        // Read in a task of its own; the only race is with the stop, which
        // drops the tail for good: the next run resumes from the last
        // acknowledged token (cancel-safe as it is; decision r9-131).
        loop {
            let item = tokio::select! {
                () = stop.cancelled() => return,
                i = tail.next() => i,
            };
            let failed = item.is_err();
            if tx.send((tenant, item)).await.is_err() || failed {
                return;
            }
        }
    });
}

/// Runs the loop of `root`'s tree until `stop` (see the module docs).
/// `progress` is called about once a second with the stats so far.
///
/// # Errors
/// A non-retryable error of the feeds or the maintainer; the job then
/// fails and the controller starts it again.
#[allow(clippy::too_many_lines)]
pub async fn serve(
    root: TenantId,
    feeds: Arc<dyn Feeds>,
    apply: &mut dyn Apply,
    opts: &ServeOpts,
    stop: &CancellationToken,
    progress: &mut (dyn FnMut(ServeStats) + Send),
) -> Result<ServeStats> {
    let mut stats = ServeStats::default();
    let (tx, mut rx) = mpsc::channel::<Item>(1024);
    let tails_stop = stop.child_token();
    let _guard = tails_stop.clone().drop_guard();
    let mut open: BTreeSet<TenantId> = BTreeSet::new();
    // Per tenant: the last applied token, events since the last ack.
    let mut applied: BTreeMap<TenantId, (ResumeToken, u64)> = BTreeMap::new();
    let mut acked: HashMap<TenantId, ResumeToken> = HashMap::new();
    let mut idle: BTreeMap<TenantId, bool> = BTreeMap::new();
    let mut last_ack = tokio::time::Instant::now();
    let mut last_refresh = tokio::time::Instant::now();
    let mut last_caught = tokio::time::Instant::now();
    let mut last_purge = tokio::time::Instant::now();
    let mut last_progress = tokio::time::Instant::now();
    let mut need_refresh = true;
    loop {
        if need_refresh || last_refresh.elapsed() >= opts.refresh_every {
            let tenants = loop {
                match apply.refresh().await {
                    Ok(t) => break t,
                    Err(e) if e.retryable => {
                        tracing::warn!(error = %e, "copy worker refresh failed; retrying");
                        tokio::select! {
                            () = stop.cancelled() => return Ok(stats),
                            () = tokio::time::sleep(opts.retry) => {}
                        }
                    }
                    Err(e) => return Err(e),
                }
            };
            for t in tenants {
                if open.insert(t) {
                    idle.insert(t, false);
                    spawn_tail(Arc::clone(&feeds), root, t, tx.clone(), tails_stop.clone());
                }
            }
            last_refresh = tokio::time::Instant::now();
            need_refresh = false;
        }
        // A batch: wait for the first item, then take what is there.
        let mut batch: Vec<Item> = Vec::new();
        let wait = opts
            .ack_every
            .min(opts.refresh_every)
            .min(Duration::from_millis(250));
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            i = rx.recv() => if let Some(i) = i { batch.push(i) },
            () = tokio::time::sleep(wait) => {}
        }
        while batch.len() < opts.batch {
            match rx.try_recv() {
                Ok(i) => batch.push(i),
                Err(_) => break,
            }
        }
        // Group the row events.
        let mut groups: BTreeMap<(TenantId, String, String), BTreeSet<KeyLit>> = BTreeMap::new();
        let mut events = 0u64;
        let mut failed_tail = None;
        for (t, item) in &batch {
            let item = match item {
                Ok(i) => i,
                Err(e) => {
                    let mut x = SprigError::new(
                        e.code().unwrap_or(SprigCode::BackendUnavailable),
                        e.message.clone(),
                    );
                    x.retryable = e.retryable;
                    failed_tail = Some((*t, x));
                    continue;
                }
            };
            match item {
                FeedItem::Event(e) if matches!(e.op, Op::Insert | Op::Update | Op::Delete) => {
                    idle.insert(*t, false);
                    let Some(key_cols) = apply.key_of(*t, &e.db_name, &e.table) else {
                        // An object made inheritable since: read the tree again.
                        need_refresh = true;
                        continue;
                    };
                    let key = KeyLit(
                        key_cols
                            .iter()
                            .map(|k| {
                                let v = e.key.get(k).or_else(|| {
                                    e.key
                                        .0
                                        .iter()
                                        .find(|(n, _)| n.eq_ignore_ascii_case(k))
                                        .map(|(_, v)| v)
                                });
                                key_text(v.and_then(text_of).as_deref())
                            })
                            .collect(),
                    );
                    groups
                        .entry((*t, e.db_name.clone(), e.table.clone()))
                        .or_default()
                        .insert(key);
                    events += 1;
                }
                FeedItem::Event(e) if matches!(e.op, Op::Ddl(_) | Op::Truncate) => {
                    idle.insert(*t, false);
                    need_refresh = true;
                }
                FeedItem::Progress { .. } => {
                    idle.insert(*t, true);
                }
                _ => {
                    idle.insert(*t, false);
                }
            }
        }
        // Apply, retrying the same batch while the error is retryable.
        if !groups.is_empty() {
            let mut attempt = 0u32;
            loop {
                let mut result = Ok(());
                for ((t, db, table), keys) in &groups {
                    let keys: Vec<KeyLit> = keys.iter().cloned().collect();
                    if let Err(e) = apply.apply(*t, db, table, &keys).await {
                        result = Err(e);
                        break;
                    }
                }
                match result {
                    Ok(()) => break,
                    Err(e) if e.retryable && attempt < 1000 => {
                        attempt += 1;
                        stats.retries += 1;
                        tracing::warn!(error = %e, attempt, "copy batch failed; applying it again");
                        tokio::select! {
                            () = stop.cancelled() => return Ok(stats),
                            () = tokio::time::sleep(opts.retry) => {}
                        }
                        // The tree may have changed (a dropped share).
                        let _ = apply.refresh().await;
                    }
                    Err(e) => return Err(e),
                }
            }
            stats.batches += 1;
            stats.events += events;
        }
        // Positions move only past applied items.
        for (t, item) in &batch {
            if let Ok(i) = item {
                let e = applied.entry(*t).or_insert_with(|| (i.token().clone(), 0));
                e.0 = i.token().clone();
                if matches!(i, FeedItem::Event(_)) {
                    e.1 += 1;
                }
            }
        }
        if let Some((t, e)) = failed_tail {
            // The feed broke (it retries what it can itself): open it again
            // after the last applied position.
            tracing::warn!(tenant = %t, error = %e, "copy worker feed failed; reopening");
            ack_all(root, &feeds, &mut applied, &mut acked, &mut stats).await?;
            if !e.retryable && e.code() != Some(SprigCode::FeedHistoryLost) {
                return Err(e);
            }
            open.remove(&t);
            need_refresh = true;
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(opts.retry) => {}
            }
        }
        if last_ack.elapsed() >= opts.ack_every {
            ack_all(root, &feeds, &mut applied, &mut acked, &mut stats).await?;
            last_ack = tokio::time::Instant::now();
        }
        if last_caught.elapsed() >= Duration::from_secs(1)
            && batch.is_empty()
            && !idle.is_empty()
            && idle.values().all(|v| *v)
        {
            if let Err(e) = apply.caught_up(now_ms()).await {
                tracing::warn!(error = %e, "copy lag not recorded");
            }
            last_caught = tokio::time::Instant::now();
        }
        if last_purge.elapsed() >= opts.purge_every {
            if let Err(e) = apply.purge().await {
                tracing::warn!(error = %e, "copy tombstones not purged");
            }
            last_purge = tokio::time::Instant::now();
        }
        if last_progress.elapsed() >= Duration::from_secs(1) {
            progress(stats);
            last_progress = tokio::time::Instant::now();
        }
    }
    ack_all(root, &feeds, &mut applied, &mut acked, &mut stats).await?;
    Ok(stats)
}

async fn ack_all(
    root: TenantId,
    feeds: &Arc<dyn Feeds>,
    applied: &mut BTreeMap<TenantId, (ResumeToken, u64)>,
    acked: &mut HashMap<TenantId, ResumeToken>,
    stats: &mut ServeStats,
) -> Result<()> {
    for (t, (token, n)) in applied.iter_mut() {
        if acked.get(t) == Some(token) {
            continue;
        }
        feeds.ack(root, *t, token, *n).await?;
        acked.insert(*t, token.clone());
        *n = 0;
        stats.acks += 1;
    }
    Ok(())
}

/// The text of a binlog value, as a result set would show it.
fn text_of(v: &sprig_binlog::Value) -> Option<String> {
    use sprig_binlog::Value as B;
    Some(match v {
        B::Null => return None,
        B::Int(i) => i.to_string(),
        B::UInt(u) | B::Bit(u) | B::Set(u) => u.to_string(),
        B::Float(f) => f.to_string(),
        B::Double(d) => d.to_string(),
        B::Decimal(s) => s.clone(),
        B::Year(y) => y.to_string(),
        B::Enum(n) => n.to_string(),
        B::Date(d) => d.to_string(),
        B::Time(t) => t.to_string(),
        B::DateTime(t) => t.to_string(),
        B::Timestamp(t) => t.secs.to_string(),
        B::String(b) | B::Bytes(b) | B::Geometry(b) => String::from_utf8_lossy(b).into_owned(),
    })
}

// ------------------------------------------------------------ the live env

/// Tenant catalogs, clusters and the directory, as the worker reaches them.
pub struct LiveEnv {
    /// The DC tier (subscriptions, shard maps).
    pub dc: Arc<dyn MetaStore>,
    /// The global tier (the tenant directory).
    pub global: Arc<dyn MetaStore>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// Binlog sources.
    pub clusters: Arc<RegistryClusters>,
    source: MariaSource,
}

impl std::fmt::Debug for LiveEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveEnv").finish_non_exhaustive()
    }
}

impl LiveEnv {
    /// An env over the worker's stores and pools.
    ///
    /// # Errors
    /// `Internal` if the catalog modules are malformed.
    pub fn new(
        dc: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        locator: Arc<ShardMapLocator>,
        pools: Arc<dyn ClusterPools>,
        clusters: Arc<RegistryClusters>,
    ) -> Result<Self> {
        // The types too: child tenants' record hierarchies (decision T28).
        let ext: Vec<Arc<dyn CatalogExtension>> = vec![
            Arc::new(sprig_inherit::InheritExtension),
            Arc::new(sprig_inherit::TypesExtension),
        ];
        let source = MariaSource::new(Arc::clone(&locator) as Arc<dyn CatalogLocator>, ext)?;
        Ok(Self {
            dc,
            global,
            locator,
            pools,
            clusters,
            source,
        })
    }
}

#[async_trait]
impl CopyEnv for LiveEnv {
    async fn family(&self, root: TenantId) -> Result<Vec<TenantId>> {
        let entries = Directory::new(Arc::clone(&self.global)).list().await?;
        let mark = format!("/{}/", root.0);
        let mut out: Vec<(usize, TenantId)> = entries
            .iter()
            .map(sprig_controller::tree::StoredEntry::body)
            .filter(|e| {
                e.state == sprig_controller::tree::TenantState::Active
                    && (e.tenant_id == root || e.path.contains(&mark))
            })
            .map(|e| (e.path.matches('/').count(), e.tenant_id))
            .collect();
        out.sort();
        Ok(out.into_iter().map(|(_, t)| t).collect())
    }

    async fn catalog(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>> {
        let loaded = self.source.load(tenant, None, Node::Writer).await?;
        let reg = sprig_settings::Registry::global()?;
        Ok(Arc::new(TenantCatalog::build(reg, tenant, loaded)))
    }

    async fn conn(&self, tenant: TenantId) -> Result<mysql_async::Conn> {
        let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
            let mut e = SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} has no shard map here"),
            );
            e.retryable = true;
            e
        })?;
        let pool = self.pools.writer(&cluster).await?;
        pool.get_conn().await.map_err(|e| {
            let mut x = SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"));
            x.retryable = true;
            x
        })
    }

    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

/// The tenant catalog's pool, for the feed's name resolver.
struct Catalogs(Arc<ShardMapLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<mysql_async::Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

#[async_trait]
impl Feeds for LiveEnv {
    async fn open(&self, root: TenantId, tenant: TenantId) -> Result<Box<dyn Tail>> {
        let subs = Subscriptions::new(Arc::clone(&self.dc));
        let name = sprig_inherit::subs::name(root);
        if subs.checkpoint(tenant, &name).await?.is_none() {
            // A tenant that joined the tree: from now on.
            let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
                SprigError::new(
                    SprigCode::TenantNotReady,
                    format!("tenant {tenant} has no shard map here"),
                )
            })?;
            let pos = self.clusters.position(&cluster).await?;
            sprig_inherit::subs::register(
                Arc::clone(&self.dc),
                tenant,
                root,
                cluster,
                &pos.to_string(),
            )
            .await?;
        }
        let cp = subs
            .checkpoint(tenant, &name)
            .await?
            .ok_or_else(|| SprigError::internal("the subscription has no checkpoint"))?;
        let tail = TenantTail::start(
            tenant,
            StartAt::After(cp.resume_token()?),
            Arc::clone(&self.clusters) as Arc<dyn Clusters>,
            Arc::new(MetaPlacement::new(Arc::clone(&self.dc))),
            Arc::new(CatalogResolver::new(Arc::new(Catalogs(Arc::clone(
                &self.locator,
            ))))),
            TailOptions::default(),
        )
        .await?;
        Ok(Box::new(tail))
    }

    async fn ack(
        &self,
        root: TenantId,
        tenant: TenantId,
        token: &ResumeToken,
        events: u64,
    ) -> Result<()> {
        Subscriptions::new(Arc::clone(&self.dc))
            .ack(
                tenant,
                &sprig_inherit::subs::name(root),
                token,
                events,
                now_ms(),
            )
            .await
    }
}

/// `inherit_copy` (see the module docs).
pub struct InheritCopy(pub Arc<LiveEnv>);

impl std::fmt::Debug for InheritCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InheritCopy").finish_non_exhaustive()
    }
}

/// The last stats of each running copy job, for progress reports.
fn shared_stats() -> &'static Mutex<HashMap<u64, ServeStats>> {
    static S: std::sync::OnceLock<Mutex<HashMap<u64, ServeStats>>> = std::sync::OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

#[async_trait]
impl JobHandler for InheritCopy {
    fn kind(&self) -> &'static str {
        sprig_inherit::subs::JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Other
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let root = TenantId(cx.spec["root"].as_u64().ok_or("spec.root is missing")?);
        let epoch = u64::from(cx.attempt().await);
        let env = Arc::clone(&self.0);
        let mut m = Maintainer::new(Arc::clone(&env) as Arc<dyn CopyEnv>, root, epoch);
        tracing::info!(%root, epoch, job = %cx.job_id, "copy worker started");
        let stop = cx.cancel.clone();
        let feeds: Arc<dyn Feeds> = env;
        let id = root.0;
        let mut report = move |s: ServeStats| {
            shared_stats()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(id, s);
        };
        let opts = ServeOpts::default();
        let run = serve(root, feeds, &mut m, &opts, &stop, &mut report);
        tokio::pin!(run);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let result = loop {
            tokio::select! {
                r = &mut run => break r,
                _ = tick.tick() => {
                    let s = shared_stats()
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .get(&id)
                        .copied()
                        .unwrap_or_default();
                    let p = json!({
                        "root": root.0,
                        "events": s.events,
                        "batches": s.batches,
                        "retries": s.retries,
                        "acks": s.acks,
                    });
                    // Ok(false): the job was lost and the cancel fired.
                    if let Err(e) = cx.progress(p).await {
                        tracing::warn!(error = %e, "copy worker progress not written");
                    }
                }
            }
        };
        match result {
            Ok(s) => Ok(json!({"stopped": "cancelled", "events": s.events})),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests;
