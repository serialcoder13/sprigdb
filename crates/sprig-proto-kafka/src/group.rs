//! The consumer group coordinator: Kafka's classic group protocol.
//!
//! A gateway coordinates a group while it holds the group's lease
//! ([`Leases`]); FindCoordinator sends members there. Membership,
//! assignments and heartbeats live in this coordinator's memory; the
//! generation and the committed offsets are durable ([`GroupStore`],
//! `stream_groups` and `stream_offsets`). When the lease moves, members
//! get `NOT_COORDINATOR`, find the new coordinator and rejoin.
//!
//! States, as in Kafka: `Empty → PreparingRebalance → CompletingRebalance
//! → Stable`, back to `PreparingRebalance` on a join, leave or session
//! timeout; `Dead` after deletion.
//!
//! - **Join.** A member without an id gets one and, from JoinGroup v4, is
//!   told `MEMBER_ID_REQUIRED` to join again with it. A join starts a
//!   rebalance (or answers at once when it changes nothing). The rebalance
//!   completes when every known member has rejoined, or at the largest
//!   rebalance timeout, which removes those that didn't; a group that was
//!   empty first waits `initial_rebalance_delay` for more members, like
//!   Kafka's `group.initial.rebalance.delay.ms`. Completion bumps the
//!   generation, picks the protocol most members prefer among those all
//!   support, keeps or picks the leader, and answers every waiting join;
//!   only the leader gets the member list.
//! - **Sync.** The leader's SyncGroup carries every assignment and makes
//!   the group stable; followers wait for it.
//! - **Heartbeats** keep a member alive for its session timeout and tell
//!   it about a rebalance (`REBALANCE_IN_PROGRESS`).
//! - **Static members** (`group.instance.id`): a join with a known
//!   instance id and no member id replaces the old member, which is then
//!   fenced (`FENCED_INSTANCE_ID`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, oneshot};
use tokio::time::Instant;

use crate::protocol::err;

/// A group: its tenant and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupKey {
    /// The tenant.
    pub tenant: u64,
    /// The group id.
    pub group: String,
}

/// A group's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// No members.
    Empty,
    /// Waiting for members to (re)join.
    PreparingRebalance,
    /// Waiting for the leader's assignments.
    CompletingRebalance,
    /// Members have their assignments.
    Stable,
    /// Deleted.
    Dead,
}

impl GroupState {
    /// Kafka's name, as DescribeGroups and ListGroups show it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
        }
    }

    /// The `stream_groups.state` value.
    #[must_use]
    pub const fn stored(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Dead => "dead",
            _ => "stable",
        }
    }
}

/// A group's durable row (`stream_groups`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredGroup {
    /// The group id.
    pub group: String,
    /// `consumer`, or empty for groups that only commit offsets.
    pub protocol_type: String,
    /// The last generation.
    pub generation: i32,
}

/// One committed offset (`stream_offsets`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    /// The stream (`streams.stream_id`).
    pub stream_id: u64,
    /// The partition.
    pub partition: i32,
    /// The next record to read.
    pub offset: i64,
    /// The leader epoch of the last record read, if the client sent one.
    pub leader_epoch: Option<i32>,
    /// The client's metadata string.
    pub metadata: Option<String>,
}

/// A storage error of a [`GroupStore`] or [`Leases`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StoreError(pub String);

/// The durable part of consumer groups: `stream_groups` and
/// `stream_offsets` in the tenant catalog.
#[async_trait]
pub trait GroupStore: Send + Sync + 'static {
    /// Writes offsets, replacing earlier ones of the same partitions.
    async fn commit(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), StoreError>;
    /// Reads a group's offsets: all of them, or those of `only`
    /// (stream, partition) pairs.
    async fn fetch(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> Result<Vec<CommittedOffset>, StoreError>;
    /// Writes the group's row.
    async fn save_group(
        &self,
        tenant: u64,
        g: &StoredGroup,
        state: GroupState,
    ) -> Result<(), StoreError>;
    /// Reads the group's row.
    async fn load_group(&self, tenant: u64, group: &str)
    -> Result<Option<StoredGroup>, StoreError>;
    /// Every group row of a tenant.
    async fn list_groups(&self, tenant: u64) -> Result<Vec<StoredGroup>, StoreError>;
    /// Deletes a group's row and offsets.
    async fn delete_group(&self, tenant: u64, group: &str) -> Result<(), StoreError>;
}

/// The coordinator leases of groups (etcd key
/// `/sprig/dc/<dc_id>/stream-groups/<tenant_id>/<group>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// This gateway holds the lease. The epoch changes whenever the lease
    /// was lost and taken again, so state from before is stale.
    Held {
        /// The holding epoch.
        epoch: u64,
    },
    /// Another gateway holds it: this broker id.
    Elsewhere(i32),
}

/// Coordinator leases.
#[async_trait]
pub trait Leases: Send + Sync + 'static {
    /// Takes the lease if it is free, or confirms this gateway holds it.
    async fn acquire(&self, key: &GroupKey) -> Result<LeaseState, StoreError>;
    /// Who holds the lease, if anyone (a broker id).
    async fn holder(&self, key: &GroupKey) -> Result<Option<i32>, StoreError>;
    /// Whether this gateway still holds the lease at `epoch` (checked
    /// before timers change a group, so a gateway that lost the lease never
    /// writes the group's row).
    async fn confirm(&self, key: &GroupKey, epoch: u64) -> bool;
    /// Gives the lease up (the group became empty, or was deleted).
    async fn release(&self, key: &GroupKey);
}

/// Coordinator settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupConfig {
    /// The smallest session timeout a member may ask for (Kafka's
    /// `group.min.session.timeout.ms`, 6000).
    pub min_session: Duration,
    /// The largest (`kafka.group_session_timeout_max_ms`).
    pub max_session: Duration,
    /// How long a rebalance of an empty group waits for more members
    /// (`kafka.group_initial_rebalance_delay_ms`).
    pub initial_rebalance_delay: Duration,
    /// How often sessions and rebalance deadlines are checked.
    pub tick: Duration,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            min_session: Duration::from_millis(6_000),
            max_session: Duration::from_millis(300_000),
            initial_rebalance_delay: Duration::from_millis(3_000),
            tick: Duration::from_millis(100),
        }
    }
}

/// A JoinGroup, decoded.
#[derive(Debug, Clone, Default)]
pub struct JoinArgs {
    /// Empty for a new member.
    pub member_id: String,
    /// `group.instance.id` of a static member.
    pub group_instance_id: Option<String>,
    /// The client id (for new member ids and DescribeGroups).
    pub client_id: String,
    /// The client address, as DescribeGroups shows it.
    pub client_host: String,
    /// The session timeout.
    pub session_timeout_ms: i32,
    /// The rebalance timeout (v0: the session timeout).
    pub rebalance_timeout_ms: i32,
    /// `consumer`, or another embedded protocol.
    pub protocol_type: String,
    /// Protocols in preference order, with their metadata.
    pub protocols: Vec<(String, Vec<u8>)>,
    /// JoinGroup v4+: a new member must rejoin with its assigned id.
    pub require_known_member_id: bool,
}

/// A member in a JoinGroup response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinMember {
    /// The member id.
    pub member_id: String,
    /// Its instance id.
    pub group_instance_id: Option<String>,
    /// Its metadata for the chosen protocol.
    pub metadata: Vec<u8>,
}

/// A JoinGroup answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOutcome {
    /// The Kafka error code.
    pub error: i16,
    /// The generation.
    pub generation: i32,
    /// The protocol type.
    pub protocol_type: Option<String>,
    /// The chosen protocol.
    pub protocol_name: Option<String>,
    /// The leader's member id.
    pub leader: String,
    /// This member's id.
    pub member_id: String,
    /// Every member, for the leader; empty for the others.
    pub members: Vec<JoinMember>,
}

impl JoinOutcome {
    fn error(error: i16, member_id: &str) -> Self {
        Self {
            error,
            generation: -1,
            protocol_type: None,
            protocol_name: None,
            leader: String::new(),
            member_id: member_id.to_owned(),
            members: Vec::new(),
        }
    }
}

/// A SyncGroup, decoded.
#[derive(Debug, Clone, Default)]
pub struct SyncArgs {
    /// The member.
    pub member_id: String,
    /// Its generation.
    pub generation: i32,
    /// Its instance id.
    pub group_instance_id: Option<String>,
    /// v5: the protocol type it expects.
    pub protocol_type: Option<String>,
    /// v5: the protocol it expects.
    pub protocol_name: Option<String>,
    /// From the leader: every member's assignment.
    pub assignments: Vec<(String, Vec<u8>)>,
}

/// A SyncGroup answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncOutcome {
    /// The Kafka error code.
    pub error: i16,
    /// The protocol type.
    pub protocol_type: Option<String>,
    /// The protocol.
    pub protocol_name: Option<String>,
    /// This member's assignment.
    pub assignment: Vec<u8>,
}

impl SyncOutcome {
    fn error(error: i16) -> Self {
        Self {
            error,
            protocol_type: None,
            protocol_name: None,
            assignment: Vec::new(),
        }
    }
}

/// A member as DescribeGroups shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDescription {
    /// The member id.
    pub member_id: String,
    /// Its instance id.
    pub group_instance_id: Option<String>,
    /// Its client id.
    pub client_id: String,
    /// Its address.
    pub client_host: String,
    /// Its metadata for the chosen protocol.
    pub metadata: Vec<u8>,
    /// Its assignment.
    pub assignment: Vec<u8>,
}

/// A group as DescribeGroups and ListGroups show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    /// The Kafka error code (`NOT_COORDINATOR`, ...).
    pub error: i16,
    /// The group id.
    pub group: String,
    /// The state.
    pub state: GroupState,
    /// The protocol type.
    pub protocol_type: String,
    /// The chosen protocol.
    pub protocol: String,
    /// Members (only in `Stable` and `CompletingRebalance` with metadata).
    pub members: Vec<MemberDescription>,
}

struct Member {
    instance: Option<String>,
    client_id: String,
    client_host: String,
    session: Duration,
    rebalance: Duration,
    protocols: Vec<(String, Vec<u8>)>,
    assignment: Vec<u8>,
    join: Option<oneshot::Sender<JoinOutcome>>,
    sync: Option<oneshot::Sender<SyncOutcome>>,
    deadline: Instant,
    seq: u64,
}

struct Group {
    key: GroupKey,
    epoch: u64,
    state: GroupState,
    generation: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    leader: Option<String>,
    members: HashMap<String, Member>,
    /// Members told `MEMBER_ID_REQUIRED`, until their session timeout.
    pending: HashMap<String, Instant>,
    rebalance_deadline: Option<Instant>,
    not_before: Option<Instant>,
    seq: u64,
    dirty: bool,
    /// The number of the last snapshot handed to the group's writer.
    saved_seq: u64,
}

impl Group {
    fn new(key: GroupKey, epoch: u64, stored: Option<StoredGroup>) -> Self {
        let (generation, protocol_type) = stored.map_or((0, None), |s| {
            (
                s.generation,
                (!s.protocol_type.is_empty()).then_some(s.protocol_type),
            )
        });
        Self {
            key,
            epoch,
            state: GroupState::Empty,
            generation,
            protocol_type,
            protocol: None,
            leader: None,
            members: HashMap::new(),
            pending: HashMap::new(),
            rebalance_deadline: None,
            not_before: None,
            seq: 0,
            dirty: false,
            saved_seq: 0,
        }
    }

    fn ordered(&self) -> Vec<&String> {
        let mut ids: Vec<(&u64, &String)> = self.members.iter().map(|(k, m)| (&m.seq, k)).collect();
        ids.sort();
        ids.into_iter().map(|(_, k)| k).collect()
    }

    /// Answers every waiting join and sync with `error` (the lease moved,
    /// the group was deleted).
    fn fail_waiters(&mut self, error: i16) {
        for (id, m) in &mut self.members {
            if let Some(tx) = m.join.take() {
                let _ = tx.send(JoinOutcome::error(error, id));
            }
            if let Some(tx) = m.sync.take() {
                let _ = tx.send(SyncOutcome::error(error));
            }
        }
    }

    fn remove(&mut self, id: &str, error: i16) {
        if let Some(mut m) = self.members.remove(id) {
            if let Some(tx) = m.join.take() {
                let _ = tx.send(JoinOutcome::error(error, id));
            }
            if let Some(tx) = m.sync.take() {
                let _ = tx.send(SyncOutcome::error(error));
            }
        }
        if self.leader.as_deref() == Some(id) {
            self.leader = None;
        }
    }

    fn prepare_rebalance(&mut self, now: Instant, cfg: &GroupConfig) {
        if self.state == GroupState::CompletingRebalance || self.state == GroupState::Stable {
            for m in self.members.values_mut() {
                if let Some(tx) = m.sync.take() {
                    let _ = tx.send(SyncOutcome::error(err::REBALANCE_IN_PROGRESS));
                }
            }
        }
        if self.state != GroupState::PreparingRebalance {
            let was_empty = self.state == GroupState::Empty;
            self.state = GroupState::PreparingRebalance;
            let longest = self
                .members
                .values()
                .map(|m| m.rebalance)
                .max()
                .unwrap_or(Duration::ZERO);
            self.rebalance_deadline = Some(now + longest);
            self.not_before = was_empty.then(|| now + cfg.initial_rebalance_delay.min(longest));
        }
    }

    /// Completes the join phase if it can: every member has rejoined (and
    /// an empty group's initial delay is over), or the deadline passed.
    fn try_complete_join(&mut self, now: Instant) {
        if self.state != GroupState::PreparingRebalance {
            return;
        }
        let deadline_passed = self.rebalance_deadline.is_some_and(|d| now >= d);
        let all_joined = self.members.values().all(|m| m.join.is_some());
        let delay_over = self.not_before.is_none_or(|t| now >= t);
        if !(deadline_passed || (all_joined && delay_over)) {
            return;
        }
        if deadline_passed {
            let late: Vec<String> = self
                .members
                .iter()
                .filter(|(_, m)| m.join.is_none())
                .map(|(k, _)| k.clone())
                .collect();
            for id in late {
                self.remove(&id, err::UNKNOWN_MEMBER_ID);
            }
        }
        self.rebalance_deadline = None;
        self.not_before = None;
        self.generation += 1;
        self.dirty = true;
        metrics::counter!("sprig_kafka_group_rebalances_total").increment(1);
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.protocol = None;
            self.leader = None;
            return;
        }
        let order: Vec<String> = self.ordered().into_iter().cloned().collect();
        if self
            .leader
            .as_ref()
            .is_none_or(|l| !self.members.contains_key(l))
        {
            self.leader = order.first().cloned();
        }
        let leader = self.leader.clone().unwrap_or_default();
        self.protocol = self.choose_protocol(&leader);
        let protocol = self.protocol.clone().unwrap_or_default();
        self.state = GroupState::CompletingRebalance;
        let members: Vec<JoinMember> = order
            .iter()
            .filter_map(|id| {
                let m = self.members.get(id)?;
                Some(JoinMember {
                    member_id: id.clone(),
                    group_instance_id: m.instance.clone(),
                    metadata: m
                        .protocols
                        .iter()
                        .find(|(n, _)| *n == protocol)
                        .map(|(_, md)| md.clone())
                        .unwrap_or_default(),
                })
            })
            .collect();
        for (id, m) in &mut self.members {
            m.deadline = now + m.session;
            m.assignment.clear();
            if let Some(tx) = m.join.take() {
                let _ = tx.send(JoinOutcome {
                    error: err::NONE,
                    generation: self.generation,
                    protocol_type: self.protocol_type.clone(),
                    protocol_name: Some(protocol.clone()),
                    leader: leader.clone(),
                    member_id: id.clone(),
                    members: if *id == leader {
                        members.clone()
                    } else {
                        Vec::new()
                    },
                });
            }
        }
    }

    /// The protocol every member supports that most members list first
    /// among those; ties go to the leader's preference.
    fn choose_protocol(&self, leader: &str) -> Option<String> {
        let l = self.members.get(leader)?;
        let common: Vec<&String> = l
            .protocols
            .iter()
            .map(|(n, _)| n)
            .filter(|n| {
                self.members
                    .values()
                    .all(|m| m.protocols.iter().any(|(p, _)| p == *n))
            })
            .collect();
        let mut votes: BTreeMap<usize, usize> = BTreeMap::new();
        for m in self.members.values() {
            if let Some(i) = m
                .protocols
                .iter()
                .find_map(|(p, _)| common.iter().position(|c| *c == p))
            {
                *votes.entry(i).or_default() += 1;
            }
        }
        let best = votes
            .iter()
            .max_by(|a, b| a.1.cmp(b.1).then(b.0.cmp(a.0)))?;
        common.get(*best.0).map(|s| (*s).clone())
    }

    fn tick(&mut self, now: Instant, cfg: &GroupConfig) {
        self.pending.retain(|_, until| *until > now);
        let expired: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| m.join.is_none() && m.deadline <= now)
            .map(|(k, _)| k.clone())
            .collect();
        if !expired.is_empty() {
            for id in &expired {
                tracing::debug!(group = %self.key.group, member = %id, "kafka group member session expired");
                self.remove(id, err::UNKNOWN_MEMBER_ID);
            }
            match self.state {
                GroupState::Stable | GroupState::CompletingRebalance => {
                    if self.members.is_empty() {
                        self.become_empty();
                    } else {
                        self.prepare_rebalance(now, cfg);
                    }
                }
                _ => {}
            }
        }
        self.try_complete_join(now);
    }

    fn become_empty(&mut self) {
        self.state = GroupState::Empty;
        self.generation += 1;
        self.protocol = None;
        self.leader = None;
        self.dirty = true;
    }

    fn as_stored(&self) -> StoredGroup {
        StoredGroup {
            group: self.key.group.clone(),
            protocol_type: self.protocol_type.clone().unwrap_or_default(),
            generation: self.generation,
        }
    }
}

/// The coordinator of every group this gateway holds the lease of.
pub struct Coordinator {
    groups: std::sync::Mutex<HashMap<GroupKey, Arc<Mutex<Group>>>>,
    /// Each group's writer of its durable row: rows are written
    /// off the group's lock and off the timer loop, one group's writes in
    /// order, so a slow catalog write never holds up a rebalance.
    writers: std::sync::Mutex<HashMap<GroupKey, Arc<Writer>>>,
    store: Arc<dyn GroupStore>,
    leases: Arc<dyn Leases>,
    cfg: std::sync::RwLock<GroupConfig>,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator").finish_non_exhaustive()
    }
}

type Held = Result<Arc<Mutex<Group>>, i16>;

/// One group's row writer: the number of the last snapshot written (or
/// skipped as stale), and how many snapshots are on their way.
#[derive(Default)]
struct Writer {
    last: Mutex<u64>,
    pending: std::sync::atomic::AtomicU64,
}

impl Writer {
    fn busy(&self) -> bool {
        self.pending.load(std::sync::atomic::Ordering::Acquire) > 0
    }
}

impl Coordinator {
    /// A coordinator over `store` and `leases`.
    #[must_use]
    pub fn new(store: Arc<dyn GroupStore>, leases: Arc<dyn Leases>, cfg: GroupConfig) -> Self {
        Self {
            groups: std::sync::Mutex::new(HashMap::new()),
            writers: std::sync::Mutex::new(HashMap::new()),
            store,
            leases,
            cfg: std::sync::RwLock::new(cfg),
        }
    }

    /// The durable store.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn GroupStore> {
        &self.store
    }

    /// The leases.
    #[must_use]
    pub fn leases(&self) -> &Arc<dyn Leases> {
        &self.leases
    }

    /// The current settings.
    #[must_use]
    pub fn config(&self) -> GroupConfig {
        *self
            .cfg
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Replaces the settings (live settings).
    pub fn set_config(&self, cfg: GroupConfig) {
        *self
            .cfg
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = cfg;
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<GroupKey, Arc<Mutex<Group>>>> {
        self.groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Runs session and rebalance timers until `stop`.
    pub async fn run(self: Arc<Self>, stop: sprig_common::CancellationToken) {
        loop {
            let tick = self.config().tick;
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(tick) => {}
            }
            self.tick().await;
        }
    }

    /// One timer pass over every group.
    pub async fn tick(&self) {
        let now = Instant::now();
        let cfg = self.config();
        let groups: Vec<(GroupKey, Arc<Mutex<Group>>)> = self
            .map()
            .iter()
            .map(|(k, g)| (k.clone(), Arc::clone(g)))
            .collect();
        for (key, g) in groups {
            // A group busy elsewhere (a describe or delete reading the
            // catalog) is checked at the next tick, so it can't hold up the
            // others' timers.
            let Ok(mut gg) = g.try_lock() else {
                continue;
            };
            if !self.leases.confirm(&key, gg.epoch).await {
                // The lease moved: members find the new coordinator.
                gg.fail_waiters(err::NOT_COORDINATOR);
                drop(gg);
                self.forget(&key, &g);
                continue;
            }
            gg.tick(now, &cfg);
            self.persist(&mut gg);
            let idle =
                gg.state == GroupState::Empty && gg.members.is_empty() && gg.pending.is_empty();
            drop(gg);
            if idle && !self.writer(&key).busy() {
                // Nothing in memory worth keeping: give the lease back,
                // without holding up the other groups' timers on etcd.
                self.forget(&key, &g);
                self.drop_writer(&key);
                let leases = Arc::clone(&self.leases);
                tokio::spawn(async move { leases.release(&key).await });
            }
        }
    }

    /// Drops `g` from the map, unless another task replaced it already.
    fn forget(&self, key: &GroupKey, g: &Arc<Mutex<Group>>) {
        let mut map = self.map();
        if map.get(key).is_some_and(|cur| Arc::ptr_eq(cur, g)) {
            map.remove(key);
        }
    }

    fn drop_writer(&self, key: &GroupKey) {
        let mut w = self
            .writers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if w.get(key).is_some_and(|x| !x.busy()) {
            w.remove(key);
        }
    }

    fn writer(&self, key: &GroupKey) -> Arc<Writer> {
        let mut w = self
            .writers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(w.entry(key.clone()).or_default())
    }

    /// Hands the group's row to its writer when it changed. The write runs
    /// in its own task: the caller never waits on the catalog (a slow write
    /// once held the timer loop, and every group's rebalance with it, for
    /// up to the backend's lock wait). Writes of one group apply in order;
    /// a snapshot older than one already written is skipped.
    fn persist(&self, g: &mut Group) {
        if !g.dirty {
            return;
        }
        g.dirty = false;
        g.saved_seq += 1;
        let seq = g.saved_seq;
        let (tenant, row, state) = (g.key.tenant, g.as_stored(), g.state);
        let w = self.writer(&g.key);
        w.pending.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            {
                let mut last = w.last.lock().await;
                if *last < seq {
                    if let Err(e) = store.save_group(tenant, &row, state).await {
                        tracing::warn!(group = %row.group, error = %e, "kafka group: saving the generation failed");
                    }
                    *last = seq;
                }
            }
            w.pending.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
    }

    /// Waits until every row write handed out for `key` is done (tests,
    /// and before a group's row is deleted).
    pub async fn flush(&self, key: &GroupKey) {
        let w = self.writer(key);
        while w.busy() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The group, if this gateway coordinates it; `NOT_COORDINATOR` if
    /// another does, `COORDINATOR_NOT_AVAILABLE` if leases can't be read.
    async fn held(&self, key: &GroupKey) -> Held {
        let epoch = match self.leases.acquire(key).await {
            Ok(LeaseState::Held { epoch }) => epoch,
            Ok(LeaseState::Elsewhere(_)) => {
                let removed = self.map().remove(key);
                if let Some(g) = removed
                    && let Ok(mut g) = g.try_lock()
                {
                    g.fail_waiters(err::NOT_COORDINATOR);
                }
                return Err(err::NOT_COORDINATOR);
            }
            Err(e) => {
                tracing::warn!(group = %key.group, error = %e, "kafka group lease unavailable");
                return Err(err::COORDINATOR_NOT_AVAILABLE);
            }
        };
        let existing = self.map().get(key).cloned();
        if let Some(g) = existing {
            let mut gg = g.lock().await;
            if gg.epoch == epoch && gg.state != GroupState::Dead {
                drop(gg);
                return Ok(g);
            }
            // The lease was lost and taken again: what we had is stale.
            gg.fail_waiters(err::NOT_COORDINATOR);
        }
        // A row write of an earlier incarnation lands before the row is read.
        self.flush(key).await;
        let stored = self
            .store
            .load_group(key.tenant, &key.group)
            .await
            .map_err(|_| err::COORDINATOR_NOT_AVAILABLE)?;
        let g = Arc::new(Mutex::new(Group::new(key.clone(), epoch, stored)));
        self.map().insert(key.clone(), Arc::clone(&g));
        Ok(g)
    }

    /// JoinGroup. Waits for the rebalance to complete.
    pub async fn join(&self, key: &GroupKey, a: JoinArgs) -> JoinOutcome {
        let cfg = self.config();
        if key.group.is_empty() {
            return JoinOutcome::error(err::INVALID_GROUP_ID, &a.member_id);
        }
        let session = Duration::from_millis(u64::try_from(a.session_timeout_ms).unwrap_or(0));
        if session < cfg.min_session || session > cfg.max_session {
            return JoinOutcome::error(err::INVALID_SESSION_TIMEOUT, &a.member_id);
        }
        if a.protocol_type.is_empty() || a.protocols.is_empty() {
            return JoinOutcome::error(err::INCONSISTENT_GROUP_PROTOCOL, &a.member_id);
        }
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => return JoinOutcome::error(e, &a.member_id),
        };
        let rx = {
            let mut g = g.lock().await;
            let now = Instant::now();
            let rx = Self::join_locked(&mut g, a, session, now, &cfg);
            g.try_complete_join(now);
            self.persist(&mut g);
            rx
        };
        match rx {
            Ok(rx) => rx
                .await
                .unwrap_or_else(|_| JoinOutcome::error(err::UNKNOWN_MEMBER_ID, "")),
            Err(o) => o,
        }
    }

    #[allow(clippy::too_many_lines, clippy::result_large_err)] // the join cases, in Kafka's order; the error is the answer
    fn join_locked(
        g: &mut Group,
        a: JoinArgs,
        session: Duration,
        now: Instant,
        cfg: &GroupConfig,
    ) -> Result<oneshot::Receiver<JoinOutcome>, JoinOutcome> {
        if g.state == GroupState::Dead {
            return Err(JoinOutcome::error(
                err::COORDINATOR_NOT_AVAILABLE,
                &a.member_id,
            ));
        }
        // The protocol type must match, and some protocol must be common.
        if g.state != GroupState::Empty || !g.members.is_empty() {
            if g.protocol_type
                .as_deref()
                .is_some_and(|t| t != a.protocol_type)
            {
                return Err(JoinOutcome::error(
                    err::INCONSISTENT_GROUP_PROTOCOL,
                    &a.member_id,
                ));
            }
            let others_share =
                g.members
                    .iter()
                    .filter(|(id, _)| **id != a.member_id)
                    .all(|(_, m)| {
                        a.protocols
                            .iter()
                            .any(|(n, _)| m.protocols.iter().any(|(p, _)| p == n))
                    });
            if !others_share {
                return Err(JoinOutcome::error(
                    err::INCONSISTENT_GROUP_PROTOCOL,
                    &a.member_id,
                ));
            }
        }
        let rebalance = Duration::from_millis(
            u64::try_from(if a.rebalance_timeout_ms > 0 {
                a.rebalance_timeout_ms
            } else {
                a.session_timeout_ms
            })
            .unwrap_or(0),
        );
        let mut member_id = a.member_id.clone();
        if member_id.is_empty() {
            // A static member coming back replaces its old incarnation.
            let old = a.group_instance_id.as_ref().and_then(|i| {
                g.members
                    .iter()
                    .find(|(_, m)| m.instance.as_ref() == Some(i))
                    .map(|(k, _)| k.clone())
            });
            let base = if a.client_id.is_empty() {
                "member"
            } else {
                &a.client_id
            };
            member_id = format!("{base}-{}", uuid::Uuid::now_v7());
            if let Some(old) = old {
                g.remove(&old, err::FENCED_INSTANCE_ID);
            } else if a.require_known_member_id && a.group_instance_id.is_none() {
                g.pending.insert(member_id.clone(), now + session);
                return Err(JoinOutcome::error(err::MEMBER_ID_REQUIRED, &member_id));
            }
        } else if g.pending.remove(&member_id).is_some() {
            // Rejoining with the id it was given.
        } else if let Some(m) = g.members.get(&member_id) {
            if a.group_instance_id.is_some() && m.instance != a.group_instance_id {
                return Err(JoinOutcome::error(err::FENCED_INSTANCE_ID, &member_id));
            }
        } else {
            return Err(JoinOutcome::error(err::UNKNOWN_MEMBER_ID, &member_id));
        }
        let (tx, rx) = oneshot::channel();
        let known = g.members.contains_key(&member_id);
        if known {
            let leader = g.leader.as_deref() == Some(member_id.as_str());
            let m = g
                .members
                .get_mut(&member_id)
                .ok_or_else(|| JoinOutcome::error(err::UNKNOWN_MEMBER_ID, &a.member_id))?;
            let same = m.protocols == a.protocols;
            m.session = session;
            m.rebalance = rebalance;
            m.deadline = now + session;
            if same && !leader && g.state == GroupState::Stable {
                // Nothing changed: the current generation, at once.
                return Err(JoinOutcome {
                    error: err::NONE,
                    generation: g.generation,
                    protocol_type: g.protocol_type.clone(),
                    protocol_name: g.protocol.clone(),
                    leader: g.leader.clone().unwrap_or_default(),
                    member_id,
                    members: Vec::new(),
                });
            }
            let m = g
                .members
                .get_mut(&member_id)
                .ok_or_else(|| JoinOutcome::error(err::UNKNOWN_MEMBER_ID, &a.member_id))?;
            m.protocols = a.protocols;
            m.client_id = a.client_id;
            m.client_host = a.client_host;
            if let Some(old) = m.join.replace(tx) {
                let _ = old.send(JoinOutcome::error(err::UNKNOWN_MEMBER_ID, &member_id));
            }
        } else {
            g.seq += 1;
            let seq = g.seq;
            g.members.insert(
                member_id.clone(),
                Member {
                    instance: a.group_instance_id,
                    client_id: a.client_id,
                    client_host: a.client_host,
                    session,
                    rebalance,
                    protocols: a.protocols,
                    assignment: Vec::new(),
                    join: Some(tx),
                    sync: None,
                    deadline: now + session,
                    seq,
                },
            );
            if g.protocol_type.is_none() {
                g.protocol_type = Some(a.protocol_type);
            }
        }
        g.prepare_rebalance(now, cfg);
        // A member that joins during the rebalance gets its own rebalance
        // timeout to join in.
        if let Some(d) = g.rebalance_deadline {
            g.rebalance_deadline = Some(d.max(now + rebalance));
        }
        Ok(rx)
    }

    /// SyncGroup. A follower waits for the leader's assignments.
    pub async fn sync(&self, key: &GroupKey, a: SyncArgs) -> SyncOutcome {
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => return SyncOutcome::error(e),
        };
        let rx = {
            let mut g = g.lock().await;
            let now = Instant::now();
            let Some(m) = g.members.get(&a.member_id) else {
                return SyncOutcome::error(err::UNKNOWN_MEMBER_ID);
            };
            if a.group_instance_id.is_some() && m.instance != a.group_instance_id {
                return SyncOutcome::error(err::FENCED_INSTANCE_ID);
            }
            if a.generation != g.generation {
                return SyncOutcome::error(err::ILLEGAL_GENERATION);
            }
            if a.protocol_type
                .as_ref()
                .is_some_and(|t| Some(t) != g.protocol_type.as_ref())
                || a.protocol_name
                    .as_ref()
                    .is_some_and(|p| Some(p) != g.protocol.as_ref())
            {
                return SyncOutcome::error(err::INCONSISTENT_GROUP_PROTOCOL);
            }
            let pt = g.protocol_type.clone();
            let pn = g.protocol.clone();
            match g.state {
                GroupState::PreparingRebalance => {
                    return SyncOutcome::error(err::REBALANCE_IN_PROGRESS);
                }
                GroupState::Empty | GroupState::Dead => {
                    return SyncOutcome::error(err::UNKNOWN_MEMBER_ID);
                }
                GroupState::Stable => {
                    let m = &g.members[&a.member_id];
                    return SyncOutcome {
                        error: err::NONE,
                        protocol_type: pt,
                        protocol_name: pn,
                        assignment: m.assignment.clone(),
                    };
                }
                GroupState::CompletingRebalance => {}
            }
            let (tx, rx) = oneshot::channel();
            if let Some(m) = g.members.get_mut(&a.member_id) {
                m.deadline = now + m.session;
                m.sync = Some(tx);
            }
            if g.leader.as_deref() == Some(a.member_id.as_str()) {
                let assigned: HashMap<String, Vec<u8>> = a.assignments.into_iter().collect();
                for (id, m) in &mut g.members {
                    m.assignment = assigned.get(id).cloned().unwrap_or_default();
                }
                g.state = GroupState::Stable;
                g.dirty = true;
                for m in g.members.values_mut() {
                    if let Some(tx) = m.sync.take() {
                        let _ = tx.send(SyncOutcome {
                            error: err::NONE,
                            protocol_type: pt.clone(),
                            protocol_name: pn.clone(),
                            assignment: m.assignment.clone(),
                        });
                    }
                }
            }
            self.persist(&mut g);
            rx
        };
        rx.await
            .unwrap_or_else(|_| SyncOutcome::error(err::REBALANCE_IN_PROGRESS))
    }

    /// Heartbeat.
    pub async fn heartbeat(
        &self,
        key: &GroupKey,
        member_id: &str,
        generation: i32,
        instance: Option<&str>,
    ) -> i16 {
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => return e,
        };
        let mut g = g.lock().await;
        let state = g.state;
        let gen_now = g.generation;
        let Some(m) = g.members.get_mut(member_id) else {
            return err::UNKNOWN_MEMBER_ID;
        };
        if instance.is_some() && m.instance.as_deref() != instance {
            return err::FENCED_INSTANCE_ID;
        }
        m.deadline = Instant::now() + m.session;
        match state {
            GroupState::Empty | GroupState::Dead => err::UNKNOWN_MEMBER_ID,
            GroupState::PreparingRebalance => err::REBALANCE_IN_PROGRESS,
            _ if generation != gen_now => err::ILLEGAL_GENERATION,
            _ => err::NONE,
        }
    }

    /// LeaveGroup for one or more members (`(member id, instance id)`);
    /// returns the group-level error and one error per member.
    pub async fn leave(
        &self,
        key: &GroupKey,
        members: &[(String, Option<String>)],
    ) -> (i16, Vec<i16>) {
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => return (e, vec![e; members.len()]),
        };
        let mut g = g.lock().await;
        let cfg = self.config();
        let now = Instant::now();
        let mut out = Vec::with_capacity(members.len());
        let mut removed = false;
        for (id, instance) in members {
            let target = if id.is_empty() {
                instance.as_ref().and_then(|i| {
                    g.members
                        .iter()
                        .find(|(_, m)| m.instance.as_ref() == Some(i))
                        .map(|(k, _)| k.clone())
                })
            } else {
                Some(id.clone())
            };
            match target {
                Some(t) if g.members.contains_key(&t) => {
                    if instance.is_some() && g.members[&t].instance != *instance {
                        out.push(err::FENCED_INSTANCE_ID);
                        continue;
                    }
                    g.remove(&t, err::UNKNOWN_MEMBER_ID);
                    removed = true;
                    out.push(err::NONE);
                }
                _ => out.push(err::UNKNOWN_MEMBER_ID),
            }
        }
        if removed {
            match g.state {
                GroupState::Stable | GroupState::CompletingRebalance => {
                    if g.members.is_empty() {
                        g.become_empty();
                    } else {
                        g.prepare_rebalance(now, &cfg);
                    }
                }
                _ => {}
            }
            g.try_complete_join(now);
            self.persist(&mut g);
        }
        let top = if members.len() == 1 {
            out[0]
        } else {
            err::NONE
        };
        (top, out)
    }

    /// Checks an OffsetCommit's member and generation. A commit with
    /// generation -1 and no member id is a standalone commit, allowed
    /// while the group has no members.
    pub async fn check_commit(
        &self,
        key: &GroupKey,
        member_id: &str,
        generation: i32,
        instance: Option<&str>,
    ) -> i16 {
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => return e,
        };
        let mut g = g.lock().await;
        if generation < 0 && member_id.is_empty() {
            if g.members.is_empty() {
                if g.protocol_type.is_none() {
                    // A group that only commits offsets: keep its row.
                    g.dirty = true;
                    self.persist(&mut g);
                }
                return err::NONE;
            }
            return err::ILLEGAL_GENERATION;
        }
        let state = g.state;
        let gen_now = g.generation;
        let Some(m) = g.members.get_mut(member_id) else {
            return err::UNKNOWN_MEMBER_ID;
        };
        if instance.is_some() && m.instance.as_deref() != instance {
            return err::FENCED_INSTANCE_ID;
        }
        if generation != gen_now {
            return err::ILLEGAL_GENERATION;
        }
        m.deadline = Instant::now() + m.session;
        if state == GroupState::PreparingRebalance {
            return err::REBALANCE_IN_PROGRESS;
        }
        err::NONE
    }

    /// DescribeGroups for one group.
    pub async fn describe(&self, key: &GroupKey) -> GroupDescription {
        let mut d = GroupDescription {
            error: err::NONE,
            group: key.group.clone(),
            state: GroupState::Dead,
            protocol_type: String::new(),
            protocol: String::new(),
            members: Vec::new(),
        };
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => {
                d.error = e;
                return d;
            }
        };
        let g = g.lock().await;
        let known = !g.members.is_empty() || g.generation > 0 || g.protocol_type.is_some();
        if !known {
            // Never seen: Kafka answers Dead without an error.
            if let Ok(None) = self.store.load_group(key.tenant, &key.group).await {
                return d;
            }
        }
        d.state = g.state;
        d.protocol_type = g.protocol_type.clone().unwrap_or_default();
        let protocol = g.protocol.clone().unwrap_or_default();
        if matches!(
            g.state,
            GroupState::Stable | GroupState::CompletingRebalance
        ) {
            d.protocol.clone_from(&protocol);
        }
        let with_data = matches!(g.state, GroupState::Stable);
        for id in g.ordered() {
            let m = &g.members[id];
            d.members.push(MemberDescription {
                member_id: id.clone(),
                group_instance_id: m.instance.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                metadata: if with_data {
                    m.protocols
                        .iter()
                        .find(|(n, _)| *n == protocol)
                        .map(|(_, md)| md.clone())
                        .unwrap_or_default()
                } else {
                    Vec::new()
                },
                assignment: if with_data {
                    m.assignment.clone()
                } else {
                    Vec::new()
                },
            });
        }
        d
    }

    /// The state of a group this coordinator has in memory, if any.
    pub async fn local_state(&self, key: &GroupKey) -> Option<(GroupState, String)> {
        let g = self.map().get(key).cloned()?;
        let g = g.lock().await;
        Some((g.state, g.protocol_type.clone().unwrap_or_default()))
    }

    /// DeleteGroups for one group: `NON_EMPTY_GROUP` while it has members,
    /// `GROUP_ID_NOT_FOUND` if it doesn't exist.
    pub async fn delete(&self, key: &GroupKey) -> i16 {
        let g = match self.held(key).await {
            Ok(g) => g,
            Err(e) => return e,
        };
        let mut g = g.lock().await;
        if !g.members.is_empty() {
            return err::NON_EMPTY_GROUP;
        }
        // Every write handed out lands before the row is deleted.
        self.flush(key).await;
        match self.store.load_group(key.tenant, &key.group).await {
            Ok(None) if g.generation == 0 && g.protocol_type.is_none() => {
                return err::GROUP_ID_NOT_FOUND;
            }
            Err(_) => return err::COORDINATOR_NOT_AVAILABLE,
            _ => {}
        }
        if self
            .store
            .delete_group(key.tenant, &key.group)
            .await
            .is_err()
        {
            return err::COORDINATOR_NOT_AVAILABLE;
        }
        g.state = GroupState::Dead;
        g.fail_waiters(err::COORDINATOR_NOT_AVAILABLE);
        drop(g);
        self.map().remove(key);
        self.drop_writer(key);
        self.leases.release(key).await;
        err::NONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::{MemGroupStore, MemLeases};

    /// A store whose row writes of one group hang until released.
    struct SlowStore {
        inner: MemGroupStore,
        slow_group: String,
        gate: tokio::sync::Semaphore,
    }

    #[async_trait]
    impl GroupStore for SlowStore {
        async fn commit(&self, t: u64, g: &str, o: &[CommittedOffset]) -> Result<(), StoreError> {
            self.inner.commit(t, g, o).await
        }
        async fn fetch(
            &self,
            t: u64,
            g: &str,
            only: Option<&[(u64, i32)]>,
        ) -> Result<Vec<CommittedOffset>, StoreError> {
            self.inner.fetch(t, g, only).await
        }
        async fn save_group(
            &self,
            t: u64,
            g: &StoredGroup,
            s: GroupState,
        ) -> Result<(), StoreError> {
            if g.group == self.slow_group {
                let _p = self.gate.acquire().await;
            }
            self.inner.save_group(t, g, s).await
        }
        async fn load_group(&self, t: u64, g: &str) -> Result<Option<StoredGroup>, StoreError> {
            self.inner.load_group(t, g).await
        }
        async fn list_groups(&self, t: u64) -> Result<Vec<StoredGroup>, StoreError> {
            self.inner.list_groups(t).await
        }
        async fn delete_group(&self, t: u64, g: &str) -> Result<(), StoreError> {
            self.inner.delete_group(t, g).await
        }
    }

    fn join_args() -> JoinArgs {
        JoinArgs {
            client_id: "c".into(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 300_000,
            protocol_type: "consumer".into(),
            protocols: vec![("range".into(), vec![1])],
            ..JoinArgs::default()
        }
    }

    /// Regression: a group's row write that hangs (a catalog row lock, a slow
    /// backend) held the shared timer loop, so a new group's first join
    /// waited for it (36.6 s measured with kafka-python against a held
    /// row lock). Now the other group's join completes after its own
    /// initial delay, and the slow write still lands, in order.
    #[tokio::test]
    async fn a_slow_row_write_holds_up_no_other_group() {
        let store = Arc::new(SlowStore {
            inner: MemGroupStore::default(),
            slow_group: "slow".into(),
            gate: tokio::sync::Semaphore::new(0),
        });
        let cfg = GroupConfig {
            initial_rebalance_delay: Duration::from_millis(200),
            tick: Duration::from_millis(20),
            ..GroupConfig::default()
        };
        let c = Arc::new(Coordinator::new(
            Arc::clone(&store) as Arc<dyn GroupStore>,
            Arc::new(MemLeases::single(1)),
            cfg,
        ));
        let stop = sprig_common::CancellationToken::new();
        tokio::spawn(Arc::clone(&c).run(stop.clone()));
        let key = |g: &str| GroupKey {
            tenant: 7,
            group: g.into(),
        };
        // The slow group's join completes on a tick, whose row write hangs.
        let slow = c.join(&key("slow"), join_args()).await;
        assert_eq!(slow.error, err::NONE);
        let t = Instant::now();
        let fast = c.join(&key("fast"), join_args()).await;
        assert_eq!(fast.error, err::NONE);
        assert_eq!(fast.generation, 1);
        let took = t.elapsed();
        assert!(
            took < Duration::from_secs(2),
            "the other group waited {took:?}"
        );
        let s = c
            .sync(
                &key("fast"),
                SyncArgs {
                    member_id: fast.member_id.clone(),
                    generation: 1,
                    assignments: vec![(fast.member_id.clone(), vec![9])],
                    ..SyncArgs::default()
                },
            )
            .await;
        assert_eq!((s.error, s.assignment), (err::NONE, vec![9]));
        c.flush(&key("fast")).await;
        assert_eq!(
            store
                .load_group(7, "fast")
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        // The slow write lands once released.
        assert!(store.load_group(7, "slow").await.unwrap().is_none());
        store.gate.add_permits(100);
        c.flush(&key("slow")).await;
        assert_eq!(
            store
                .load_group(7, "slow")
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        stop.cancel();
    }

    /// Writes of one group apply in order: the last snapshot wins.
    #[tokio::test]
    async fn row_writes_of_a_group_land_in_order() {
        let store = Arc::new(MemGroupStore::default());
        let c = Coordinator::new(
            Arc::clone(&store) as Arc<dyn GroupStore>,
            Arc::new(MemLeases::single(1)),
            GroupConfig::default(),
        );
        let key = GroupKey {
            tenant: 1,
            group: "g".into(),
        };
        let g = c.held(&key).await.unwrap();
        for _ in 0..50 {
            let mut g = g.lock().await;
            g.generation += 1;
            g.dirty = true;
            c.persist(&mut g);
        }
        c.flush(&key).await;
        assert_eq!(
            store.load_group(1, "g").await.unwrap().unwrap().generation,
            50
        );
    }
}
