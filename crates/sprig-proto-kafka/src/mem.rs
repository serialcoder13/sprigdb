//! In-memory implementations of the broker's traits: for tests, and as the
//! gateway's **interim adapter** for topics until `sprig-streams`' catalog
//! API (chapter 45 build step 2) lands.
//!
//! [`MemTopics`] keeps topics per tenant in this process only: they are not
//! streams yet (nothing is created in MariaDB), other gateways don't see
//! them, and they are gone after a restart. [`MemGroupStore`] keeps groups
//! and offsets the same way; the gateway writes the real `stream_groups`
//! and `stream_offsets` tables instead. [`MemLeases`] grants coordinator
//! leases from a table shared by the brokers of one test.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use uuid::Uuid;

use crate::group::{
    CommittedOffset, GroupKey, GroupState, GroupStore, LeaseState, Leases, StoreError, StoredGroup,
};
use crate::protocol::err;
use crate::topics::{
    NewTopic, Principal, TopicConfigs, TopicDefaults, TopicError, TopicInfo, TopicKind, Topics,
};

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Topics in memory, per tenant (interim adapter; see the module docs).
#[derive(Debug)]
pub struct MemTopics {
    topics: std::sync::Mutex<HashMap<u64, BTreeMap<String, TopicInfo>>>,
    next_id: AtomicU64,
    defaults: TopicDefaults,
}

impl Default for MemTopics {
    fn default() -> Self {
        Self::new(TopicDefaults::default())
    }
}

impl MemTopics {
    /// An empty registry whose new topics inherit `defaults`.
    #[must_use]
    pub fn new(defaults: TopicDefaults) -> Self {
        Self {
            topics: std::sync::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            defaults,
        }
    }

    /// Adds a change topic (tests: change topics are made by `ALTER TABLE
    /// ... SPRIG SET (stream.change_topic = on)`, not by Kafka clients).
    pub fn add_change_topic(&self, tenant: u64, name: &str, partitions: i32) -> TopicInfo {
        let t = self.info(
            name,
            partitions,
            TopicKind::Changes,
            TopicConfigs::default(),
        );
        lock(&self.topics)
            .entry(tenant)
            .or_default()
            .insert(name.to_owned(), t.clone());
        t
    }

    fn info(
        &self,
        name: &str,
        partitions: i32,
        kind: TopicKind,
        configs: TopicConfigs,
    ) -> TopicInfo {
        TopicInfo {
            name: name.to_owned(),
            stream_id: self.next_id.fetch_add(1, Ordering::Relaxed),
            topic_id: Uuid::now_v7(),
            partitions,
            kind,
            configs,
            defaults: self.defaults,
        }
    }

    fn unknown(name: &str) -> TopicError {
        TopicError::new(
            err::UNKNOWN_TOPIC_OR_PARTITION,
            format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {name}"),
        )
    }
}

/// A topic name must name a database: `<database>.<stream>`.
///
/// # Errors
/// `INVALID_TOPIC_EXCEPTION` without a database part.
pub fn split_topic(name: &str) -> Result<(&str, &str), TopicError> {
    match name.split_once('.') {
        Some((db, s)) if !db.is_empty() && !s.is_empty() => Ok((db, s)),
        _ => Err(TopicError::new(
            err::INVALID_TOPIC_EXCEPTION,
            format!("topic {name}: a topic is named <database>.<stream>"),
        )),
    }
}

#[async_trait]
impl Topics for MemTopics {
    async fn list(&self, p: &Principal) -> Result<Vec<TopicInfo>, TopicError> {
        Ok(lock(&self.topics)
            .get(&p.tenant_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn get(&self, p: &Principal, name: &str) -> Result<Option<TopicInfo>, TopicError> {
        Ok(lock(&self.topics)
            .get(&p.tenant_id)
            .and_then(|m| m.get(name).cloned()))
    }

    async fn defaults(&self, _p: &Principal, _name: &str) -> Result<TopicDefaults, TopicError> {
        Ok(self.defaults)
    }

    async fn create(
        &self,
        p: &Principal,
        t: NewTopic,
        validate_only: bool,
    ) -> Result<TopicInfo, TopicError> {
        split_topic(&t.name)?;
        let mut all = lock(&self.topics);
        let m = all.entry(p.tenant_id).or_default();
        if m.contains_key(&t.name) {
            return Err(TopicError::new(
                err::TOPIC_ALREADY_EXISTS,
                format!("39942 ER_SPRIG_STREAM_EXISTS: {} already exists", t.name),
            ));
        }
        let info = self.info(&t.name, t.partitions, TopicKind::Events, t.configs);
        if !validate_only {
            m.insert(t.name, info.clone());
        }
        Ok(info)
    }

    async fn delete(&self, p: &Principal, name: &str) -> Result<TopicInfo, TopicError> {
        let mut all = lock(&self.topics);
        let m = all.entry(p.tenant_id).or_default();
        match m.get(name) {
            None => Err(Self::unknown(name)),
            Some(t) if t.kind == TopicKind::Changes => Err(TopicError::new(
                err::POLICY_VIOLATION,
                format!("{name} is a change topic: turn stream.change_topic off on its object"),
            )),
            Some(_) => m.remove(name).ok_or_else(|| Self::unknown(name)),
        }
    }

    async fn grow(
        &self,
        p: &Principal,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        let mut all = lock(&self.topics);
        let t = all
            .entry(p.tenant_id)
            .or_default()
            .get_mut(name)
            .ok_or_else(|| Self::unknown(name))?;
        if partitions < t.partitions {
            return Err(TopicError::new(
                err::INVALID_PARTITIONS,
                format!(
                    "39943 ER_SPRIG_STREAM_PARTITIONS_DECREASE: {name} has {} partitions; partitions can only grow",
                    t.partitions
                ),
            ));
        }
        if partitions == t.partitions {
            return Err(TopicError::new(
                err::INVALID_PARTITIONS,
                format!("{name} already has {partitions} partitions"),
            ));
        }
        if !validate_only {
            t.partitions = partitions;
        }
        Ok(())
    }

    async fn set_configs(
        &self,
        p: &Principal,
        name: &str,
        c: TopicConfigs,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        let mut all = lock(&self.topics);
        let t = all
            .entry(p.tenant_id)
            .or_default()
            .get_mut(name)
            .ok_or_else(|| Self::unknown(name))?;
        if !validate_only {
            t.configs = c;
        }
        Ok(())
    }
}

/// Groups and offsets in memory.
#[derive(Debug, Default)]
pub struct MemGroupStore {
    groups: std::sync::Mutex<HashMap<(u64, String), (StoredGroup, GroupState)>>,
    offsets: std::sync::Mutex<HashMap<(u64, String), Offsets>>,
}

/// One group's offsets by (stream, partition).
type Offsets = BTreeMap<(u64, i32), CommittedOffset>;

#[async_trait]
impl GroupStore for MemGroupStore {
    async fn commit(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), StoreError> {
        let mut all = lock(&self.offsets);
        let m = all.entry((tenant, group.to_owned())).or_default();
        for o in offsets {
            m.insert((o.stream_id, o.partition), o.clone());
        }
        Ok(())
    }

    async fn fetch(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> Result<Vec<CommittedOffset>, StoreError> {
        let all = lock(&self.offsets);
        let Some(m) = all.get(&(tenant, group.to_owned())) else {
            return Ok(Vec::new());
        };
        Ok(match only {
            None => m.values().cloned().collect(),
            Some(keys) => keys.iter().filter_map(|k| m.get(k).cloned()).collect(),
        })
    }

    async fn save_group(
        &self,
        tenant: u64,
        g: &StoredGroup,
        state: GroupState,
    ) -> Result<(), StoreError> {
        lock(&self.groups).insert((tenant, g.group.clone()), (g.clone(), state));
        Ok(())
    }

    async fn load_group(
        &self,
        tenant: u64,
        group: &str,
    ) -> Result<Option<StoredGroup>, StoreError> {
        Ok(lock(&self.groups)
            .get(&(tenant, group.to_owned()))
            .map(|(g, _)| g.clone()))
    }

    async fn list_groups(&self, tenant: u64) -> Result<Vec<StoredGroup>, StoreError> {
        let mut v: Vec<StoredGroup> = lock(&self.groups)
            .iter()
            .filter(|((t, _), _)| *t == tenant)
            .map(|(_, (g, _))| g.clone())
            .collect();
        v.sort_by(|a, b| a.group.cmp(&b.group));
        Ok(v)
    }

    async fn delete_group(&self, tenant: u64, group: &str) -> Result<(), StoreError> {
        lock(&self.groups).remove(&(tenant, group.to_owned()));
        lock(&self.offsets).remove(&(tenant, group.to_owned()));
        Ok(())
    }
}

/// A lease table shared by the brokers of one process (tests).
#[derive(Debug, Default)]
pub struct MemLeaseTable {
    held: std::sync::Mutex<HashMap<GroupKey, (i32, u64)>>,
    epoch: AtomicU64,
}

impl MemLeaseTable {
    /// Drops every lease `node` holds, as an expired etcd lease would.
    pub fn expire(&self, node: i32) {
        lock(&self.held).retain(|_, (n, _)| *n != node);
    }
}

/// Coordinator leases of one broker over a [`MemLeaseTable`].
#[derive(Debug, Clone)]
pub struct MemLeases {
    /// The shared table.
    pub table: Arc<MemLeaseTable>,
    /// This broker's id.
    pub node: i32,
}

impl MemLeases {
    /// A broker's leases over a table of its own (a single broker).
    #[must_use]
    pub fn single(node: i32) -> Self {
        Self {
            table: Arc::new(MemLeaseTable::default()),
            node,
        }
    }
}

#[async_trait]
impl Leases for MemLeases {
    async fn acquire(&self, key: &GroupKey) -> Result<LeaseState, StoreError> {
        let mut held = lock(&self.table.held);
        match held.get(key) {
            Some((n, e)) if *n == self.node => Ok(LeaseState::Held { epoch: *e }),
            Some((n, _)) => Ok(LeaseState::Elsewhere(*n)),
            None => {
                let e = self.table.epoch.fetch_add(1, Ordering::Relaxed) + 1;
                held.insert(key.clone(), (self.node, e));
                Ok(LeaseState::Held { epoch: e })
            }
        }
    }

    async fn holder(&self, key: &GroupKey) -> Result<Option<i32>, StoreError> {
        Ok(lock(&self.table.held).get(key).map(|(n, _)| *n))
    }

    async fn confirm(&self, key: &GroupKey, epoch: u64) -> bool {
        lock(&self.table.held).get(key) == Some(&(self.node, epoch))
    }

    async fn release(&self, key: &GroupKey) {
        let mut held = lock(&self.table.held);
        if held.get(key).is_some_and(|(n, _)| *n == self.node) {
            held.remove(key);
        }
    }
}
