//! After-commit triggers (chapter 41, "After-commit triggers"; R7 order
//! 5), and the gateway's mapping of validation `CHECK` failures.
//!
//! Each binding owns a feed subscription (`logic:b<binding>`, chapter 38)
//! and one long-running worker job (`logic_trigger`, chapter 23) that
//! reads the tenant's feed from the subscription's checkpoint:
//!
//! ```text
//! run(feed, binding):
//!   every item gets a sequence number; items the binding doesn't want
//!   (another table, another op, an UPDATE that changed none of the
//!   FOR UPDATE OF fields, transaction ends, progress) are done at once
//!   a wanted event goes to lane hash(row key) % logic.trigger_lanes; a
//!   lane runs its events in order:
//!     apply(e): on the event's shard, as sprig_ctl, one transaction:
//!       INSERT IGNORE __sprig_ldone (binding, event key); nothing inserted
//!       → already done, ROLLBACK; else CALL the active version's entry
//!       procedure (SQL SECURITY DEFINER: the binding's definer's rights);
//!       COMMIT
//!     failure → retry after min(initial · 2^(n−1), max) with jitter; later
//!       events of the lane wait; after logic.trigger_max_attempts failures
//!       that aren't retryable (a lost cluster, a held shard retry for ever)
//!       → logic_dead_letters, and the lane moves on
//!   checkpoint := the token of the last item before the first unfinished
//!   one, acknowledged at most every second
//! ```
//!
//! **Exactly once.** The marker commits with the procedure's writes, so a
//! re-delivered event (a worker died before acknowledging it; a resume
//! token from before a move) finds the marker and does nothing. The feed
//! itself loses and duplicates nothing across moves (chapter 38), and the
//! mover copies `__sprig_ldone` with the shard, so the marker follows
//! the row. Events whose first attempt fails and that later go to dead
//! letters have no marker; a replay runs them once.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_binlog::Value as BValue;
use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{CancellationToken, ErrorKind, Result, SprigCode, SprigError};
use sprig_feed::{ChangeEvent, FeedItem, Op, ResumeToken, TenantTail};
use tokio::sync::mpsc;

/// The feed subscription of a binding.
#[must_use]
pub fn subscription_name(binding: u64) -> String {
    format!("logic:b{binding}")
}

/// The binding of a subscription name, if it is a logic one.
#[must_use]
pub fn binding_of(subscription: &str) -> Option<u64> {
    subscription.strip_prefix("logic:b")?.parse().ok()
}

// ---- CHECK failures (validations) ------------------------------------------

static CHECKS: LazyLock<Mutex<HashMap<String, (String, String)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static LOADED: LazyLock<Mutex<std::collections::HashSet<u64>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));

/// Whether this process still has to load `tenant`'s validation messages
/// ([`load_checks`]); marks it loaded.
#[must_use]
pub fn checks_wanted(tenant: u64) -> bool {
    LOADED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(tenant)
}

/// Remembers a validation's `CHECK` constraint: its validation and
/// message, for [`map_check_error`].
pub fn remember_check(constraint: &str, validation: &str, message: &str) {
    CHECKS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(
            constraint.to_owned(),
            (validation.to_owned(), message.to_owned()),
        );
}

/// The `sprig_lc<b>_v<v>` constraint a 4025 message names.
#[must_use]
pub fn check_constraint_of(message: &str) -> Option<String> {
    let at = message.find("sprig_lc")?;
    let name: String = message[at..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let rest = name.strip_prefix("sprig_lc")?;
    let (b, v) = rest.split_once("_v")?;
    (b.parse::<u64>().is_ok() && v.parse::<u64>().is_ok()).then_some(name)
}

/// A validation's `CHECK` failure (4025 or 3819 on `sprig_lc<b>_v<v>`) as 39863
/// with the validation's message; any other error unchanged. A
/// constraint this process hasn't seen deployed gets a message naming it.
#[must_use]
pub fn map_check_error(e: SprigError) -> SprigError {
    // 4025 (`CONSTRAINT x failed`) or, on 11.8, 3819 (`Check constraint
    // 'x' is violated`).
    if !matches!(
        e.kind,
        ErrorKind::Backend {
            errno: 4025 | 3819,
            ..
        }
    ) {
        return e;
    }
    let Some(name) = check_constraint_of(&e.message) else {
        return e;
    };
    let known = CHECKS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&name)
        .cloned();
    let msg = if let Some((_, m)) = known {
        m
    } else {
        // Deployed through another gateway: every tenant reloads its
        // messages at its next statement.
        LOADED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        format!("rejected by a validation (constraint {name})")
    };
    SprigError::new(SprigCode::LogicValidationFailed, msg)
}

/// Loads every `CHECK` validation of a tenant catalog into the registry
/// [`map_check_error`] reads.
///
/// # Errors
/// 39070 or a backend error.
pub async fn load_checks(c: &mut mysql_async::Conn, catalog_schema: &str) -> Result<usize> {
    use mysql_async::prelude::Queryable;
    let q = sprig_common::names::quote(catalog_schema);
    let rows: Vec<(u64, u64, String, Option<String>)> = c
        .query(format!(
            "SELECT b.binding_id, m.active_version, m.name, \
             JSON_UNQUOTE(JSON_EXTRACT(v.placement, '$.check_message')) \
             FROM {q}.logic_bindings b JOIN {q}.logic_modules m ON m.module_id = b.module_id \
             JOIN {q}.logic_versions v ON v.module_id = m.module_id AND v.version = m.active_version \
             WHERE b.enforcement = 'check' AND m.state = 'active'"
        ))
        .await
        .map_err(crate::deploy::conn_err)?;
    let n = rows.len();
    for (b, v, name, msg) in rows {
        remember_check(
            &crate::enforce::check_name(b, v),
            &name,
            msg.as_deref().unwrap_or("rejected by a validation"),
        );
    }
    Ok(n)
}

// ---- events -----------------------------------------------------------------

/// A SQL literal for a binlog value (default `sql_mode`).
#[must_use]
pub fn literal(v: &BValue) -> String {
    match v {
        BValue::Null => "NULL".to_owned(),
        BValue::Int(i) => i.to_string(),
        BValue::UInt(u) | BValue::Bit(u) | BValue::Set(u) => u.to_string(),
        BValue::Float(f) => format!("{:e}", f64::from(*f)),
        BValue::Double(d) => format!("{d:e}"),
        BValue::Decimal(s) => s.clone(),
        BValue::Year(y) => y.to_string(),
        BValue::Enum(n) => n.to_string(),
        BValue::Date(d) => format!("'{d}'"),
        BValue::Time(t) => format!("'{t}'"),
        BValue::DateTime(t) => format!("'{t}'"),
        BValue::Timestamp(t) => format!("FROM_UNIXTIME({})", t.secs),
        BValue::String(b) => match std::str::from_utf8(b) {
            Ok(s) => sprig_logic_compile::pure::str_lit(s),
            Err(_) => hex_lit(b),
        },
        BValue::Bytes(b) | BValue::Geometry(b) => hex_lit(b),
    }
}

fn hex_lit(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::from("X'");
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s.push('\'');
    s
}

fn display(v: &BValue) -> Value {
    match v {
        BValue::Null => Value::Null,
        BValue::Int(i) => json!(i),
        BValue::UInt(u) => json!(u),
        BValue::Double(d) => json!(d),
        BValue::Float(f) => json!(f),
        BValue::String(_) | BValue::Bytes(_) => match v.as_str() {
            Some(s) => json!(s),
            None => json!(literal(v)),
        },
        other => json!(literal(other).trim_matches('\'')),
    }
}

/// One change, as a trigger runs it and a dead letter keeps it.
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerEvent {
    /// `<cluster>:<domain>-<server>-<seq>:<row>`, unique per change.
    pub key: String,
    /// `insert`, `update` or `delete`.
    pub op: String,
    /// The database.
    pub db: u32,
    /// The table group and shard.
    pub group: u32,
    /// The shard.
    pub shard: u32,
    /// The table's logical name.
    pub table: String,
    /// Its primary key, for lanes.
    pub row_key: String,
    /// The row before, as SQL literals by column (lower case).
    pub before: Vec<(String, String)>,
    /// The row after.
    pub after: Vec<(String, String)>,
    /// The row for people (dead letters).
    pub row: Value,
    /// Resume after this event.
    pub token: Option<ResumeToken>,
}

fn image(i: Option<&sprig_feed::Image>) -> Vec<(String, String)> {
    i.map(|i| {
        i.0.iter()
            .map(|(n, v)| (n.to_ascii_lowercase(), literal(v)))
            .collect()
    })
    .unwrap_or_default()
}

impl TriggerEvent {
    /// A row change of the feed; `None` for DDL and truncation.
    #[must_use]
    pub fn from_change(e: &ChangeEvent) -> Option<Self> {
        let op = match e.op {
            Op::Insert => "insert",
            Op::Update => "update",
            Op::Delete => "delete",
            Op::Ddl(_) | Op::Truncate => return None,
        };
        let g = e.id.gtid;
        let shown = e.after.as_ref().or(e.before.as_ref());
        Some(Self {
            key: format!(
                "{}:{}-{}-{}:{}",
                e.id.cluster, g.domain, g.server, g.seq, e.id.row
            ),
            op: op.to_owned(),
            db: e.db.0,
            group: e.shard.group.0,
            shard: e.shard.shard.0,
            table: e.table.clone(),
            row_key: e
                .key
                .0
                .iter()
                .map(|(n, v)| format!("{n}={}", literal(v)))
                .collect::<Vec<_>>()
                .join(","),
            before: image(e.before.as_ref()),
            after: image(e.after.as_ref()),
            row: shown.map_or(Value::Null, |i| {
                Value::Object(i.0.iter().map(|(n, v)| (n.clone(), display(v))).collect())
            }),
            token: Some(e.token.clone()),
        })
    }

    /// The physical shard schema of the event's row.
    #[must_use]
    pub fn schema(&self, tenant: TenantId) -> String {
        SchemaName::shard(
            tenant,
            DbId(self.db),
            GroupId(self.group),
            ShardId(self.shard),
        )
        .to_string()
    }

    /// The JSON a dead letter keeps (no resume token: that has a column).
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "key": self.key, "op": self.op, "db": self.db, "group": self.group,
            "shard": self.shard, "table": self.table, "row_key": self.row_key,
            "before": self.before, "after": self.after, "row": self.row,
        })
    }

    /// Back from [`to_json`](Self::to_json).
    ///
    /// # Errors
    /// 39866 for JSON that isn't an event.
    pub fn from_json(v: &Value) -> Result<Self> {
        let bad = || {
            SprigError::new(
                SprigCode::LogicRuntimeError,
                "a dead letter's event is damaged",
            )
        };
        let s = |k: &str| v[k].as_str().map(str::to_owned).ok_or_else(bad);
        let n = |k: &str| {
            v[k].as_u64()
                .and_then(|x| u32::try_from(x).ok())
                .ok_or_else(bad)
        };
        let pairs = |k: &str| -> Result<Vec<(String, String)>> {
            serde_json::from_value(v[k].clone()).map_err(|_| bad())
        };
        Ok(Self {
            key: s("key")?,
            op: s("op")?,
            db: n("db")?,
            group: n("group")?,
            shard: n("shard")?,
            table: s("table")?,
            row_key: s("row_key")?,
            before: pairs("before")?,
            after: pairs("after")?,
            row: v["row"].clone(),
            token: None,
        })
    }

    /// The entry procedure's arguments for parameters `params` (bound by
    /// name, as [`crate::enforce::bind_row`] checked at deploy).
    #[must_use]
    pub fn args(&self, params: &[String]) -> Vec<String> {
        let find = |img: &[(String, String)], c: &str| {
            img.iter()
                .find(|(n, _)| n == c)
                .map_or_else(|| "NULL".to_owned(), |(_, v)| v.clone())
        };
        params
            .iter()
            .map(|p| {
                let p = p.to_ascii_lowercase();
                let new_img = if self.op == "delete" {
                    &self.before
                } else {
                    &self.after
                };
                if new_img.iter().any(|(n, _)| *n == p) {
                    find(new_img, &p)
                } else if let Some(c) = p.strip_prefix("old_")
                    && self.before.iter().chain(&self.after).any(|(n, _)| n == c)
                {
                    find(&self.before, c)
                } else if p == "op" {
                    format!("'{}'", self.op)
                } else {
                    "NULL".to_owned()
                }
            })
            .collect()
    }
}

/// What a binding wants from the feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    /// The database.
    pub db: u32,
    /// The table's object id (0: match by name).
    pub object: u64,
    /// The table's logical name.
    pub table: String,
    /// `insert`, `update`, `delete`.
    pub events: Vec<String>,
    /// `FOR UPDATE OF`: an update fires only when one of these changed.
    pub when_fields: Vec<String>,
}

impl Filter {
    /// Whether `e` fires the binding.
    #[must_use]
    pub fn wants(&self, e: &ChangeEvent) -> bool {
        if e.db.0 != self.db {
            return false;
        }
        let same = if self.object == 0 && self.table.is_empty() {
            // Every table of the database (the simulation's binding).
            true
        } else if self.object != 0 && e.object != 0 {
            e.object == self.object
        } else {
            e.table.eq_ignore_ascii_case(&self.table)
        };
        if !same {
            return false;
        }
        let op = match e.op {
            Op::Insert => "insert",
            Op::Update => "update",
            Op::Delete => "delete",
            _ => return false,
        };
        if !self.events.iter().any(|x| x == op) {
            return false;
        }
        if op == "update" && !self.when_fields.is_empty() {
            return e.delta.as_ref().is_none_or(|d| {
                d.iter()
                    .any(|c| self.when_fields.iter().any(|w| w.eq_ignore_ascii_case(c)))
            });
        }
        true
    }
}

// ---- the runner -------------------------------------------------------------

/// What an attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The version ran and committed.
    Ran(u64),
    /// The marker was there: an earlier delivery ran it.
    AlreadyDone,
}

/// Runs a binding's active version on one event, exactly once.
#[async_trait]
pub trait Apply: Send + Sync {
    /// See [`Applied`].
    ///
    /// # Errors
    /// The version's error, or the shard's; the attempt left nothing.
    async fn apply(&self, ev: &TriggerEvent) -> Result<Applied>;
    /// The active version (for dead letters).
    fn version(&self) -> u64;
}

/// Where the runner's results go.
#[async_trait]
pub trait Outcomes: Send + Sync {
    /// Records a dead letter.
    ///
    /// # Errors
    /// The catalog's.
    async fn dead_letter(
        &self,
        ev: &TriggerEvent,
        version: u64,
        attempts: u32,
        err: &SprigError,
    ) -> Result<()>;
    /// Acknowledges the feed up to `token`, `events` more of them.
    ///
    /// # Errors
    /// The store's.
    async fn ack(&self, token: &ResumeToken, events: u64) -> Result<()>;
    /// A line for `sprigctl logic logs` (best effort).
    async fn log(&self, level: &str, message: &str);
}

/// The feed the runner reads.
#[async_trait]
pub trait Feed: Send {
    /// The next item.
    ///
    /// # Errors
    /// The feed's.
    async fn next(&mut self) -> Result<FeedItem>;
}

#[async_trait]
impl Feed for TenantTail {
    async fn next(&mut self) -> Result<FeedItem> {
        TenantTail::next(self).await
    }
}

/// The runner's settings (chapter 41's `logic.trigger_*`).
#[derive(Debug, Clone)]
pub struct RunOpts {
    /// `logic.trigger_lanes`.
    pub lanes: usize,
    /// `logic.trigger_max_attempts`.
    pub max_attempts: u32,
    /// `logic.trigger_retry_initial_ms`.
    pub retry_initial: Duration,
    /// `logic.trigger_retry_max_ms`.
    pub retry_max: Duration,
    /// How often the checkpoint is acknowledged.
    pub ack_every: Duration,
    /// Items read ahead of the checkpoint at most.
    pub max_in_flight: usize,
}

impl Default for RunOpts {
    fn default() -> Self {
        Self {
            lanes: 8,
            max_attempts: 10,
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(300),
            ack_every: Duration::from_secs(1),
            max_in_flight: 10_000,
        }
    }
}

/// Counters of one runner.
#[derive(Debug, Default)]
pub struct Stats {
    /// Events the binding wanted.
    pub wanted: AtomicU64,
    /// Versions run.
    pub ran: AtomicU64,
    /// Deliveries a marker stopped.
    pub already: AtomicU64,
    /// Failed attempts.
    pub retries: AtomicU64,
    /// Dead letters written.
    pub dead: AtomicU64,
    /// Checkpoints acknowledged.
    pub acks: AtomicU64,
}

/// The delay before attempt `n + 1` (`n` failures so far): doubling from
/// `initial`, capped at `max`, with up to 25% jitter drawn from `seed`
/// (the event's key: deterministic, and different per event).
#[must_use]
pub fn backoff(n: u32, initial: Duration, max: Duration, seed: &str) -> Duration {
    let base = initial
        .saturating_mul(1u32 << n.saturating_sub(1).min(20))
        .min(max);
    let jitter = {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        (n, seed).hash(&mut h);
        h.finish() % 250
    };
    base + base.mul_f64(f64::from(u32::try_from(jitter).unwrap_or(0)) / 1000.0)
}

/// Runs one event through `apply` until it commits, a marker says it
/// already did, or it goes to dead letters.
async fn deliver(
    ev: &TriggerEvent,
    apply: &dyn Apply,
    out: &dyn Outcomes,
    opts: &RunOpts,
    stats: &Stats,
    cancel: &CancellationToken,
) {
    let mut failures = 0u32;
    let mut tries = 0u32;
    loop {
        tries += 1;
        match apply.apply(ev).await {
            Ok(Applied::Ran(_)) => {
                stats.ran.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Ok(Applied::AlreadyDone) => {
                stats.already.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                stats.retries.fetch_add(1, Ordering::Relaxed);
                if !e.retryable {
                    failures += 1;
                }
                if failures >= opts.max_attempts {
                    loop {
                        match out.dead_letter(ev, apply.version(), tries, &e).await {
                            Ok(()) => break,
                            Err(de) => {
                                tracing::warn!(error = %de, "can't record a dead letter; retrying");
                                tokio::select! {
                                    () = cancel.cancelled() => return,
                                    () = tokio::time::sleep(opts.retry_initial) => {}
                                }
                            }
                        }
                    }
                    stats.dead.fetch_add(1, Ordering::Relaxed);
                    out.log(
                        "error",
                        &format!(
                            "event {} on {} went to dead letters after {tries} attempts: {e}",
                            ev.key, ev.table
                        ),
                    )
                    .await;
                    return;
                }
                if tries == 1 {
                    out.log(
                        "warn",
                        &format!("event {} on {} failed, retrying: {e}", ev.key, ev.table),
                    )
                    .await;
                }
                let wait = backoff(tries, opts.retry_initial, opts.retry_max, &ev.key);
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
            }
        }
    }
}

fn lane_of(ev: &TriggerEvent, lanes: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (ev.db, ev.group, &ev.table, &ev.row_key).hash(&mut h);
    usize::try_from(h.finish() % (lanes.max(1) as u64)).unwrap_or(0)
}

/// Runs a binding over `feed` until `cancel` (see the module docs).
///
/// The feed is read by its own task into a bounded channel: a tail's
/// `next` is never dropped half way (it isn't cancel-safe), only when the
/// run ends.
///
/// # Errors
/// The feed's errors (the job then fails and is started again from the
/// checkpoint).
pub async fn run(
    feed: Box<dyn Feed>,
    filter: &Filter,
    apply: Arc<dyn Apply>,
    out: Arc<dyn Outcomes>,
    opts: &RunOpts,
    stats: Arc<Stats>,
    cancel: &CancellationToken,
) -> Result<()> {
    let lanes = opts.lanes.max(1);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<u64>();
    let mut senders = Vec::with_capacity(lanes);
    let mut tasks = Vec::with_capacity(lanes);
    let stop = cancel.child_token();
    for _ in 0..lanes {
        let (tx, mut rx) = mpsc::unbounded_channel::<(u64, TriggerEvent)>();
        senders.push(tx);
        let apply = Arc::clone(&apply);
        let out = Arc::clone(&out);
        let stats = Arc::clone(&stats);
        let opts = opts.clone();
        let done = done_tx.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            while let Some((seq, ev)) = rx.recv().await {
                deliver(&ev, apply.as_ref(), out.as_ref(), &opts, &stats, &stop).await;
                if stop.is_cancelled() {
                    return;
                }
                let _ = done.send(seq);
            }
        }));
    }
    drop(done_tx);
    let (item_tx, mut items) = mpsc::channel::<Result<FeedItem>>(256);
    let reader = tokio::spawn(async move {
        let mut feed = feed;
        loop {
            let r = feed.next().await;
            let failed = r.is_err();
            if item_tx.send(r).await.is_err() || failed {
                return;
            }
        }
    });
    let mut pending: BTreeMap<u64, (ResumeToken, bool)> = BTreeMap::new();
    let mut seq = 0u64;
    let mut done_events = 0u64;
    let mut acked: Option<ResumeToken> = None;
    let mut tick = tokio::time::interval(opts.ack_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let result = loop {
        let room = pending.len() < opts.max_in_flight;
        tokio::select! {
            biased;
            () = cancel.cancelled() => break Ok(()),
            Some(s) = done_rx.recv() => {
                if let Some(e) = pending.get_mut(&s) {
                    e.1 = true;
                    done_events += 1;
                }
            }
            _ = tick.tick() => {
                let mut last = None;
                while let Some(e) = pending.first_entry() {
                    if !e.get().1 {
                        break;
                    }
                    last = Some(e.remove().0);
                }
                if let Some(t) = last
                    && acked.as_ref() != Some(&t)
                {
                    match out.ack(&t, done_events).await {
                        Ok(()) => {
                            stats.acks.fetch_add(1, Ordering::Relaxed);
                            done_events = 0;
                            acked = Some(t);
                        }
                        Err(e) => {
                            // Not acknowledged: put it back so the next
                            // tick tries again.
                            tracing::warn!(error = %e, "trigger checkpoint not acknowledged");
                            pending.insert(0, (t, true));
                        }
                    }
                }
            }
            item = items.recv(), if room => {
                let item = match item {
                    Some(Ok(i)) => i,
                    Some(Err(e)) => break Err(e),
                    None => break Err(SprigError::internal("the feed reader stopped")),
                };
                seq += 1;
                let token = item.token().clone();
                match &item {
                    FeedItem::Event(e) if filter.wants(e) => {
                        if let Some(ev) = TriggerEvent::from_change(e) {
                            stats.wanted.fetch_add(1, Ordering::Relaxed);
                            pending.insert(seq, (token, false));
                            let lane = lane_of(&ev, lanes);
                            let _ = senders[lane].send((seq, ev));
                        } else {
                            pending.insert(seq, (token, true));
                        }
                    }
                    _ => {
                        pending.insert(seq, (token, true));
                    }
                }
            }
        }
    };
    stop.cancel();
    reader.abort();
    drop(senders);
    for t in tasks {
        let _ = t.await;
    }
    result
}

pub mod maria;

#[cfg(test)]
mod tests;
