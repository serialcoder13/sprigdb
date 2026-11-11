//! The Kafka data plane over `sprig-streams` (chapter 45 build steps 10 and
//! 11): [`GatewayData`] implements `sprig-proto-kafka`'s
//! [`StreamData`] for Produce, Fetch, ListOffsets and OffsetForLeaderEpoch.
//!
//! - **Topics** come from the tenant's catalog snapshot. Producing needs
//!   `INSERT` on the stream (by name, or on its database), reading
//!   `SELECT`; a change topic is read with `SELECT` on its table, and is
//!   refused (`TOPIC_AUTHORIZATION_FAILED`, 39366) when the authorization
//!   engine filters that table's rows, as SQL reads of it are. Privileges
//!   are chapter 29's, compiled for the user with the roles active at
//!   login, and cached per catalog and auth version.
//! - **Produce** writes the outbox directly on the stream's shard, as
//!   `sprig_ctl`, through `sprig-streams`' [`publish::append`]: one
//!   transaction per partition with the idempotent producer's check. The
//!   offsets are then looked up by the batch's first event id
//!   ([`publish::locate`]) until the sequencer has appended it, waking on
//!   the high-watermark signal, at most `stream.produce_wait_ms`.
//! - **Fetch** reads the log with [`LogReader`]; a long poll waits on the
//!   **high watermarks the sequencers publish to etcd**
//!   (`streams/<tenant>/<stream>/<shard>` in the DC tier), which one watch
//!   per gateway follows ([`Marks`]), and reads the checkpoint again at
//!   least every 250 ms in case a mark was skipped.

// The trait's names (p: principal, t: topic) and Kafka API names.
#![allow(clippy::many_single_char_names, clippy::doc_markdown)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use mysql_async::Pool;
use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{ObjectId, TenantId, UserId};
use sprig_common::{SprigCode, SprigError};
use sprig_meta::{MetaStore, Record, WatchEvent};
use sprig_proto_kafka::broker::data::{
    Access, AppendTicket, Bounds, DataTopic, Limits, ProduceRecord, ProducerBatch, StoredRecord,
    StreamData,
};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::records::Header;
use sprig_proto_kafka::topics::{Principal, TopicError, TopicKind};
use sprig_streams::publish::{self, AppendOutcome, Located, SequenceVerdict};
use sprig_streams::read::{LogReader, ReadLimits};
use sprig_streams::runtime::WatermarkRecord;
use sprig_streams::{
    LeaderEpoch, Offset, PartitionNo, StreamDef, StreamId, StreamKind, StreamState, StreamsState,
};
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

use crate::tenant::TenantServices;

// Chapter 42 (R9): inherited streams over Kafka.
mod inherit;

pub(crate) use inherit::{FANIN_PREFIX, fanin_cached, fanin_id, fanin_parts, is_fanin};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// High watermarks from etcd: `(tenant, stream, partition)` to the next
/// offset, and a counter that moves on every change.
#[derive(Debug)]
pub struct Marks {
    map: Mutex<HashMap<(u64, u64, u32), u64>>,
    tick: watch::Sender<u64>,
}

impl Marks {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            tick: watch::channel(0).0,
        }
    }

    /// Follows `streams/` of the DC tier until `stop`.
    async fn follow(
        self: Arc<Self>,
        dc: Arc<dyn MetaStore>,
        stop: sprig_common::CancellationToken,
    ) {
        let Ok(prefix) = dc.tier().key("streams/") else {
            return;
        };
        loop {
            match dc.watch(&prefix, 0).await {
                Ok(mut w) => loop {
                    let ev = tokio::select! {
                        () = stop.cancelled() => return,
                        ev = w.next() => ev,
                    };
                    match ev {
                        Some(Ok(WatchEvent::Put(kv))) => self.apply(kv.key.as_str(), &kv.value),
                        Some(Ok(WatchEvent::Compacted { .. }) | Err(_)) | None => break,
                        Some(Ok(_)) => {}
                    }
                },
                Err(e) => tracing::debug!(error = %e, "kafka: watermark watch failed"),
            }
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    fn apply(&self, key: &str, value: &[u8]) {
        // .../streams/<tenant>/<stream>/<shard>
        let mut parts = key.rsplit('/');
        let (_, Some(stream), Some(tenant)) = (parts.next(), parts.next(), parts.next()) else {
            return;
        };
        let (Ok(tenant), Ok(stream)) = (tenant.parse::<u64>(), stream.parse::<u64>()) else {
            return;
        };
        let Ok(r) = Record::<WatermarkRecord>::decode(value) else {
            return;
        };
        {
            let mut m = lock(&self.map);
            for (p, o) in &r.body.marks {
                let e = m.entry((tenant, stream, *p)).or_insert(0);
                *e = (*e).max(*o);
            }
        }
        self.tick.send_modify(|t| *t = t.wrapping_add(1));
    }

    fn beyond(&self, tenant: u64, wants: &[(u64, i32, i64)]) -> bool {
        let m = lock(&self.map);
        wants.iter().any(|(s, p, o)| {
            // A fan-in topic's marks are its member's.
            let (tenant, s) = fanin_cached(tenant, *s).unwrap_or((tenant, *s));
            u32::try_from(*p)
                .ok()
                .and_then(|p| m.get(&(tenant, s, p)))
                .is_some_and(|hw| i64::try_from(*hw).is_ok_and(|hw| hw > *o))
        })
    }

    /// Waits for the next change, at most until `until`.
    async fn changed(&self, until: Instant) {
        let mut rx = self.tick.subscribe();
        let _ = tokio::time::timeout_at(until, rx.changed()).await;
    }
}

/// `kafka.fetch_max_wait_ms` and `stream.produce_wait_ms` for a tenant.
fn limits_of(svc: &TenantServices, cat: &TenantCatalog) -> Limits {
    let ctx = cat.setting_ctx(&svc.dc);
    Limits {
        produce_wait: Duration::from_millis(svc.settings.get(
            &sprig_streams::settings::PRODUCE_WAIT_MS,
            &ctx,
            Some(&cat.settings),
        )),
        fetch_max_wait: Duration::from_millis(svc.settings.get(
            &super::FETCH_MAX_WAIT_MS,
            &ctx,
            Some(&cat.settings),
        )),
    }
}

/// Compiled privileges by (tenant, user), with the catalog and auth
/// versions they were compiled at.
type GrantCache = HashMap<(u64, Option<u64>), (u64, u64, Arc<dyn SessionGrants>)>;
/// Leader epochs by (tenant, stream), with when they were read.
type EpochCache = HashMap<(u64, u64), (Instant, HashMap<u32, u32>)>;

/// The data plane over the tenant's streams.
pub struct GatewayData {
    svc: Arc<TenantServices>,
    marks: Arc<Marks>,
    grants: Mutex<GrantCache>,
    epochs: Mutex<EpochCache>,
    expired: Mutex<HashMap<(u64, u64), Instant>>,
}

impl std::fmt::Debug for GatewayData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayData").finish_non_exhaustive()
    }
}

/// A topic resolved to its stream and where its tables are.
struct Resolved {
    cat: Arc<TenantCatalog>,
    def: Arc<StreamDef>,
    topic: DataTopic,
}

fn unknown(name: &str) -> TopicError {
    TopicError::new(
        err::UNKNOWN_TOPIC_OR_PARTITION,
        format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {name}"),
    )
}

/// A SprigDB error as a Kafka one.
fn kafka_err(e: &SprigError) -> TopicError {
    let code = match e.code() {
        Some(SprigCode::StreamOffsetOutOfRange) => err::OFFSET_OUT_OF_RANGE,
        Some(SprigCode::StreamNotFound) => err::UNKNOWN_TOPIC_OR_PARTITION,
        Some(SprigCode::StreamAppendOnly) => err::INVALID_TOPIC_EXCEPTION,
        Some(SprigCode::StreamRecordTooLarge) => err::MESSAGE_TOO_LARGE,
        Some(SprigCode::StreamCompactNeedsKey) => err::INVALID_RECORD,
        _ if e.retryable => err::KAFKA_STORAGE_ERROR,
        _ => err::UNKNOWN_SERVER_ERROR,
    };
    let p = e.protocol();
    TopicError::new(code, format!("{} {}", p.mysql_errno, e.message))
}

impl GatewayData {
    /// The data plane of `svc`'s tenants; follows the DC's watermarks until
    /// `stop`.
    #[must_use]
    pub fn start(svc: Arc<TenantServices>, stop: sprig_common::CancellationToken) -> Self {
        let marks = Arc::new(Marks::new());
        tokio::spawn(Arc::clone(&marks).follow(svc.locator.dc_store(), stop));
        Self {
            svc,
            marks,
            grants: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            expired: Mutex::new(HashMap::new()),
        }
    }

    async fn catalog(&self, p: &Principal) -> Result<Arc<TenantCatalog>, TopicError> {
        self.svc
            .cache
            .get(TenantId(p.tenant_id), Freshness::Latest)
            .await
            .map_err(|e| kafka_err(&e))
    }

    fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    fn grants(&self, p: &Principal, cat: &TenantCatalog) -> Arc<dyn SessionGrants> {
        let key = (p.tenant_id, p.user_id);
        if let Some((v, a, g)) = lock(&self.grants).get(&key)
            && *v == cat.version
            && *a == cat.auth_version
        {
            return Arc::clone(g);
        }
        let user = p.user_id.and_then(|u| u32::try_from(u).ok()).map(UserId);
        let g = crate::tenant::authz::grants_of(&self.svc, cat, user);
        lock(&self.grants).insert(key, (cat.version, cat.auth_version, Arc::clone(&g)));
        g
    }

    /// Whether `p` may `action` stream `def` (by name or on its database);
    /// for a change topic read, `SELECT` on its table.
    fn allowed(&self, p: &Principal, cat: &TenantCatalog, def: &StreamDef, action: Action) -> bool {
        let g = self.grants(p, cat);
        if g.unrestricted() {
            return true;
        }
        let Some(db) = cat.databases.get(def.db) else {
            return false;
        };
        let lctn = self.lctn(cat);
        let db_key = database_key(&db.name, lctn);
        if let (StreamKind::Changes, Some(o), Action::Select) =
            (def.kind, def.source_object, action)
        {
            return g.allows(
                action,
                Target::Object {
                    db_key: &db_key,
                    db: def.db,
                    object: o,
                },
            );
        }
        let name_key = object_key(ObjectKind::Table, &def.name, lctn);
        g.allows(
            action,
            Target::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || g.allows(action, Target::Database { db_key: &db_key })
    }

    /// Whether `p` may see stream `def` at all (Metadata, DescribeConfigs):
    /// read or write it.
    pub(crate) fn visible(&self, p: &Principal, cat: &TenantCatalog, def: &StreamDef) -> bool {
        self.allowed(p, cat, def, Action::Select) || self.allowed(p, cat, def, Action::Insert)
    }

    fn row_filtered(&self, p: &Principal, cat: &TenantCatalog, def: &StreamDef) -> bool {
        let (Some(o), Some(db)) = (def.source_object, cat.databases.get(def.db)) else {
            return false;
        };
        let db_key = database_key(&db.name, self.lctn(cat));
        self.grants(p, cat)
            .row_filter(
                Target::Object {
                    db_key: &db_key,
                    db: def.db,
                    object: ObjectId(o.0),
                },
                Action::Select,
            )
            .is_some()
    }

    fn data_topic(&self, cat: &TenantCatalog, def: &StreamDef, name: String) -> DataTopic {
        let ctx = cat
            .object_ctx(
                &self.svc.dc,
                ObjectId(def.source_object.map_or(def.id.0, |o| o.0)),
            )
            .with_db(def.db);
        let max = self.svc.settings.get(
            &sprig_streams::settings::MAX_MESSAGE_BYTES,
            &ctx,
            Some(&cat.settings),
        );
        DataTopic {
            name,
            stream_id: def.id.0,
            topic_id: Uuid::from_u64_pair(cat.tenant.0, def.id.0),
            partitions: i32::try_from(def.partitions).unwrap_or(i32::MAX),
            kind: if def.kind == StreamKind::Changes {
                TopicKind::Changes
            } else {
                TopicKind::Events
            },
            compact: def.cleanup == sprig_streams::Cleanup::Compact,
            max_message_bytes: usize::try_from(max).unwrap_or(usize::MAX),
        }
    }

    fn check(
        &self,
        p: &Principal,
        cat: &TenantCatalog,
        def: &StreamDef,
        name: &str,
        access: Access,
    ) -> Result<(), TopicError> {
        let action = match access {
            Access::Read => Action::Select,
            Access::Write => Action::Insert,
        };
        if !self.allowed(p, cat, def, action) {
            // Hide topics the user can't see at all.
            if !self.visible(p, cat, def) {
                return Err(unknown(name));
            }
            return Err(TopicError::new(
                err::TOPIC_AUTHORIZATION_FAILED,
                format!(
                    "1142 {} command denied to user '{}' for topic '{name}'",
                    if access == Access::Read {
                        "SELECT"
                    } else {
                        "INSERT"
                    },
                    p.user
                ),
            ));
        }
        match access {
            Access::Write if def.kind == StreamKind::Changes => Err(TopicError::new(
                err::INVALID_TOPIC_EXCEPTION,
                format!(
                    "39941 ER_SPRIG_STREAM_APPEND_ONLY: {name} is a change topic: only SprigDB \
                     writes it"
                ),
            )),
            Access::Read if def.kind == StreamKind::Changes && self.row_filtered(p, cat, def) => {
                Err(TopicError::new(
                    err::TOPIC_AUTHORIZATION_FAILED,
                    format!(
                        "39366 ER_SPRIG_RLS_UNSUPPORTED: the authorization engine filters rows of \
                         '{}', which change topic '{name}' can't apply yet",
                        def.source_name.as_deref().unwrap_or("")
                    ),
                ))
            }
            _ => Ok(()),
        }
    }

    async fn resolve(
        &self,
        p: &Principal,
        name: &str,
        access: Access,
    ) -> Result<Resolved, TopicError> {
        let (db, stream) = sprig_proto_kafka::mem::split_topic(name)?;
        let cat = self.catalog(p).await?;
        let state = cat
            .ext
            .get::<StreamsState>(sprig_streams::MODULE)
            .cloned()
            .unwrap_or_default();
        let def = state
            .all()
            .filter(|d| d.state == StreamState::Active)
            .find(|d| d.name == stream && cat.databases.get(d.db).is_some_and(|r| r.name == db))
            .cloned()
            .ok_or_else(|| unknown(name))?;
        self.check(p, &cat, &def, name, access)?;
        let topic = self.data_topic(&cat, &def, name.to_owned());
        Ok(Resolved { cat, def, topic })
    }

    async fn resolve_t(&self, p: &Principal, t: &DataTopic) -> Result<Resolved, TopicError> {
        // Chapter 42 (R9): a fan-in topic reads its member's instance.
        if is_fanin(t.stream_id) {
            return self.resolve_fanin(p, t).await;
        }
        // The caller resolved it with its access already; again for the
        // tables' places (cached snapshot).
        let cat = self.catalog(p).await?;
        let state = cat
            .ext
            .get::<StreamsState>(sprig_streams::MODULE)
            .cloned()
            .unwrap_or_default();
        let def = state
            .all()
            .find(|d| d.id.0 == t.stream_id && d.state == StreamState::Active)
            .cloned()
            .ok_or_else(|| unknown(&t.name))?;
        Ok(Resolved {
            cat,
            def,
            topic: t.clone(),
        })
    }

    async fn pool(&self, tenant: u64) -> Result<Pool, TopicError> {
        let cluster = self
            .svc
            .locator
            .cluster(TenantId(tenant))
            .await
            .map_err(|e| kafka_err(&e))?;
        self.svc
            .locator
            .ctl_pool(&cluster)
            .await
            .map_err(|e| kafka_err(&e))
    }

    async fn conn(&self, tenant: u64) -> Result<mysql_async::Conn, TopicError> {
        self.pool(tenant)
            .await?
            .get_conn()
            .await
            .map_err(|e| kafka_err(&sprig_streams::maria::map_err(e)))
    }

    fn outbox_schema(r: &Resolved) -> Result<String, TopicError> {
        let slot = r
            .def
            .outbox
            .first()
            .ok_or_else(|| TopicError::new(err::INVALID_TOPIC_EXCEPTION, "no outbox"))?;
        Ok(slot.schema(r.cat.tenant, r.def.db).to_string())
    }

    fn log_reader(r: &Resolved, partition: i32) -> Result<LogReader, TopicError> {
        let p = u32::try_from(partition)
            .map_err(|_| TopicError::new(err::UNKNOWN_TOPIC_OR_PARTITION, "no such partition"))?;
        let shard = r
            .def
            .log_shard_of(PartitionNo(p))
            .ok_or_else(|| TopicError::new(err::UNKNOWN_TOPIC_OR_PARTITION, "no log shard"))?;
        Ok(LogReader {
            schema: shard.slot.schema(r.cat.tenant, r.def.db).to_string(),
        })
    }

    fn bounds_of(b: &sprig_streams::read::Bounds) -> Bounds {
        Bounds {
            log_start: i64::try_from(b.log_start.0).unwrap_or(i64::MAX),
            high_watermark: i64::try_from(b.high_watermark.0).unwrap_or(i64::MAX),
            leader_epoch: i32::try_from(b.leader_epoch.0).unwrap_or(i32::MAX),
        }
    }

    fn part(partition: i32) -> Result<PartitionNo, TopicError> {
        u32::try_from(partition)
            .map(PartitionNo)
            .map_err(|_| TopicError::new(err::UNKNOWN_TOPIC_OR_PARTITION, "no such partition"))
    }

    /// Deletes expired producer rows of a stream, at most every 10 minutes
    /// per gateway.
    fn maybe_expire(&self, tenant: u64, schema: String, id: StreamId) {
        {
            let mut m = lock(&self.expired);
            let due = m
                .get(&(tenant, id.0))
                .is_none_or(|t| t.elapsed() > Duration::from_secs(600));
            if !due {
                return;
            }
            m.insert((tenant, id.0), Instant::now());
        }
        let svc = Arc::clone(&self.svc);
        tokio::spawn(async move {
            if let Ok(cluster) = svc.locator.cluster(TenantId(tenant)).await
                && let Ok(pool) = svc.locator.ctl_pool(&cluster).await
                && let Ok(mut c) = pool.get_conn().await
            {
                let _ = publish::expire_producers(&mut c, &schema, id).await;
            }
        });
    }
}

fn headers_to_json(h: &[Header]) -> Option<String> {
    let v: Vec<(String, Option<Vec<u8>>)> =
        h.iter().map(|h| (h.key.clone(), h.value.clone())).collect();
    publish::headers_json(&v)
}

#[async_trait]
impl StreamData for GatewayData {
    async fn topic(
        &self,
        p: &Principal,
        name: &str,
        access: Access,
    ) -> Result<DataTopic, TopicError> {
        if name.starts_with(FANIN_PREFIX) {
            return self.fanin_topic(p, name, access == Access::Write).await;
        }
        match self.resolve(p, name, access).await {
            // Chapter 42 (R9): an inherited stream's topic.
            Err(e) if e.code == err::UNKNOWN_TOPIC_OR_PARTITION => {
                match self
                    .inherited_topic(p, name, access == Access::Write)
                    .await?
                {
                    inherit::Inh::Retry => self.resolve(p, name, access).await.map(|r| r.topic),
                    inherit::Inh::Def(t) => Ok(t),
                    inherit::Inh::Unknown => Err(e),
                }
            }
            r => r.map(|r| r.topic),
        }
    }

    async fn topic_by_id(
        &self,
        p: &Principal,
        id: Uuid,
        access: Access,
    ) -> Result<DataTopic, TopicError> {
        let (tenant, stream) = id.as_u64_pair();
        let no = || TopicError::new(err::UNKNOWN_TOPIC_ID, format!("no topic with id {id}"));
        if tenant != p.tenant_id {
            return Err(no());
        }
        if is_fanin(stream) {
            return self
                .fanin_topic_by_id(p, stream, access == Access::Write)
                .await
                .map_err(|_| no());
        }
        let cat = self.catalog(p).await?;
        let state = cat
            .ext
            .get::<StreamsState>(sprig_streams::MODULE)
            .cloned()
            .unwrap_or_default();
        let def = state
            .all()
            .find(|d| d.id.0 == stream && d.state == StreamState::Active)
            .cloned()
            .ok_or_else(no)?;
        let db = cat.databases.get(def.db).ok_or_else(no)?;
        let name = format!("{}.{}", db.name, def.name);
        self.check(p, &cat, &def, &name, access).map_err(|e| {
            if e.code == err::UNKNOWN_TOPIC_OR_PARTITION {
                no()
            } else {
                e
            }
        })?;
        Ok(self.data_topic(&cat, &def, name))
    }

    async fn append(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        records: Vec<ProduceRecord>,
        producer: Option<ProducerBatch>,
    ) -> Result<AppendTicket, TopicError> {
        // Chapter 42 (R9): a definition's topic, by `x-sprig-tenant`.
        if t.stream_id & inherit::DEF_FLAG != 0 {
            return self.append_by_tenant(p, t, partition, records).await;
        }
        let mut records = records;
        let r = self.resolve_t(p, t).await?;
        // ...and a member's own instance: its headers stamped.
        if crate::inherited::state_of(&r.cat)
            .instance_by_id(sprig_inherit::msg::DefKind::Stream, r.def.id.0)
            .is_some()
        {
            Self::stamp_member(p, &mut records)?;
        }
        let schema = Self::outbox_schema(&r)?;
        let pn = Self::part(partition)?;
        let count = records.len();
        let mut publishes = Vec::with_capacity(count);
        for rec in records {
            let headers = headers_to_json(&rec.headers);
            publish::check_record(
                &r.def,
                rec.key.as_deref(),
                rec.value.as_ref().map_or(0, Vec::len),
                0,
                u64::try_from(t.max_message_bytes).unwrap_or(u64::MAX),
            )
            .map_err(|e| kafka_err(&e))?;
            publishes.push(publish::Publish {
                key: rec.key,
                value: rec.value,
                headers,
                partition: Some(pn.0),
                ts_ms: Some(rec.timestamp_ms),
            });
        }
        let producer = producer.map(|b| publish::ProducerBatch {
            producer_id: b.producer_id,
            epoch: b.epoch,
            base_sequence: b.base_sequence,
            count: b.count,
        });
        let mut c = self.conn(p.tenant_id).await?;
        let out = publish::append(&mut c, &schema, &r.def, pn.0, &publishes, producer)
            .await
            .map_err(|e| kafka_err(&e))?;
        drop(c);
        if producer.is_some() {
            self.maybe_expire(p.tenant_id, schema, r.def.id);
        }
        match out {
            AppendOutcome::Appended(id) => Ok(AppendTicket {
                partition,
                first_event: id,
                count,
                base_offset: None,
                duplicate: false,
            }),
            AppendOutcome::Duplicate(id) => {
                tracing::debug!(topic = %t.name, partition, "kafka produce: a retried batch, not appended again");
                metrics::counter!("sprig_kafka_produce_duplicates_total").increment(1);
                Ok(AppendTicket {
                    partition,
                    first_event: id,
                    count,
                    base_offset: None,
                    duplicate: true,
                })
            }
            AppendOutcome::Refused(SequenceVerdict::StaleEpoch { current }) => {
                Err(TopicError::new(
                    err::INVALID_PRODUCER_EPOCH,
                    format!("the producer's epoch is older than its current epoch {current}"),
                ))
            }
            AppendOutcome::Refused(SequenceVerdict::OutOfOrder { expected }) => {
                Err(TopicError::new(
                    err::OUT_OF_ORDER_SEQUENCE_NUMBER,
                    format!("out of order: the next sequence is {expected}"),
                ))
            }
            AppendOutcome::Refused(other) => Err(TopicError::new(
                err::UNKNOWN_SERVER_ERROR,
                format!("{other:?}"),
            )),
        }
    }

    async fn sequenced(
        &self,
        p: &Principal,
        t: &DataTopic,
        ticket: &AppendTicket,
        deadline: Instant,
    ) -> Result<i64, TopicError> {
        if let Some(o) = ticket.base_offset {
            return Ok(o);
        }
        let r = self.resolve_t(p, t).await?;
        let outbox = Self::outbox_schema(&r)?;
        let reader = Self::log_reader(&r, ticket.partition)?;
        let pn = Self::part(ticket.partition)?;
        let mut c = self.conn(p.tenant_id).await?;
        let mut checked_outbox = !ticket.duplicate;
        loop {
            let at = publish::locate(
                &mut c,
                &reader.schema,
                &outbox,
                r.def.id,
                pn.0,
                ticket.first_event,
                !checked_outbox,
            )
            .await
            .map_err(|e| kafka_err(&e))?;
            checked_outbox = true;
            match at {
                Located::Offset(o) => return Ok(i64::try_from(o).unwrap_or(i64::MAX)),
                // A retried batch whose records retention removed.
                Located::Gone => return Ok(-1),
                Located::Waiting => {}
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(TopicError::new(
                    err::REQUEST_TIMED_OUT,
                    "39946 ER_SPRIG_STREAM_SEQUENCING_TIMEOUT: the records are committed but have \
                     no offset yet (stream.produce_wait_ms); the retry returns their offsets",
                ));
            }
            self.marks
                .changed(deadline.min(now + Duration::from_millis(25)))
                .await;
        }
    }

    async fn bounds(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
    ) -> Result<Bounds, TopicError> {
        let r = self.resolve_t(p, t).await?;
        let reader = Self::log_reader(&r, partition)?;
        let mut c = self.conn(r.cat.tenant.0).await?;
        let b = reader
            .bounds(&mut c, r.def.id, Self::part(partition)?)
            .await
            .map_err(|e| kafka_err(&e))?;
        Ok(Self::bounds_of(&b))
    }

    async fn read(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Bounds, Vec<StoredRecord>), TopicError> {
        let r = self.resolve_t(p, t).await?;
        let reader = Self::log_reader(&r, partition)?;
        let from = u64::try_from(offset).map_err(|_| {
            TopicError::new(
                err::OFFSET_OUT_OF_RANGE,
                format!("39947 ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE: offset {offset}"),
            )
        })?;
        let mut c = self.conn(r.cat.tenant.0).await?;
        let lim = ReadLimits {
            max_records: u32::try_from((max_bytes / 32).clamp(1, 10_000)).unwrap_or(10_000),
            max_bytes: u64::try_from(max_bytes).unwrap_or(u64::MAX),
        };
        let batch = reader
            .read(&mut c, r.def.id, Self::part(partition)?, Offset(from), lim)
            .await
            .map_err(|e| kafka_err(&e))?;
        let records = batch
            .records
            .into_iter()
            .map(|x| StoredRecord {
                offset: i64::try_from(x.offset.0).unwrap_or(i64::MAX),
                timestamp_ms: x.ts_ms,
                key: x.key,
                value: x.value,
                headers: x
                    .headers
                    .into_iter()
                    .map(|(key, value)| Header { key, value })
                    .collect(),
                leader_epoch: i32::try_from(x.leader_epoch.0).unwrap_or(i32::MAX),
            })
            .collect();
        Ok((Self::bounds_of(&batch.bounds), records))
    }

    async fn wait(&self, p: &Principal, beyond: &[(u64, i32, i64)], deadline: Instant) {
        // Wake on a watermark beyond, or every 250 ms to read the
        // checkpoints again (a mark the sequencer skipped).
        let until = deadline.min(Instant::now() + Duration::from_millis(250));
        loop {
            if self.marks.beyond(p.tenant_id, beyond) || Instant::now() >= until {
                return;
            }
            self.marks.changed(until).await;
        }
    }

    async fn offset_for_time(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        ts: i64,
    ) -> Result<Option<(i64, i64)>, TopicError> {
        let r = self.resolve_t(p, t).await?;
        let reader = Self::log_reader(&r, partition)?;
        let mut c = self.conn(r.cat.tenant.0).await?;
        let found = reader
            .offset_for_time(
                &mut c,
                r.def.id,
                Self::part(partition)?,
                (ts >= 0).then_some(ts),
            )
            .await
            .map_err(|e| kafka_err(&e))?;
        Ok(found.map(|(o, at)| (i64::try_from(o.0).unwrap_or(i64::MAX), at)))
    }

    async fn epoch_end(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        epoch: i32,
    ) -> Result<(i32, i64), TopicError> {
        if epoch < 0 {
            return Ok((-1, -1));
        }
        let r = self.resolve_t(p, t).await?;
        let reader = Self::log_reader(&r, partition)?;
        let mut c = self.conn(r.cat.tenant.0).await?;
        let (e, o) = reader
            .epoch_end(
                &mut c,
                r.def.id,
                Self::part(partition)?,
                LeaderEpoch(u32::try_from(epoch).unwrap_or(0)),
            )
            .await
            .map_err(|e| kafka_err(&e))?;
        Ok((
            i32::try_from(e.0).unwrap_or(i32::MAX),
            i64::try_from(o.0).unwrap_or(i64::MAX),
        ))
    }

    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32 {
        let key = (p.tenant_id, stream_id);
        let pn = u32::try_from(partition).unwrap_or(0);
        if let Some((at, m)) = lock(&self.epochs).get(&key)
            && at.elapsed() < Duration::from_secs(1)
        {
            return m.get(&pn).map_or(0, |e| i32::try_from(*e).unwrap_or(0));
        }
        let fetched: Result<HashMap<u32, u32>, TopicError> = async {
            let cat = self.catalog(p).await?;
            let state = cat
                .ext
                .get::<StreamsState>(sprig_streams::MODULE)
                .cloned()
                .unwrap_or_default();
            let def = state
                .all()
                .find(|d| d.id.0 == stream_id)
                .cloned()
                .ok_or_else(|| unknown(""))?;
            let mut c = self.conn(p.tenant_id).await?;
            let mut m = HashMap::new();
            for l in &def.log {
                let reader = LogReader {
                    schema: l.slot.schema(cat.tenant, def.db).to_string(),
                };
                for (p, e) in reader
                    .epochs(&mut c, def.id)
                    .await
                    .map_err(|e| kafka_err(&e))?
                {
                    m.insert(p.0, e.0);
                }
            }
            Ok(m)
        }
        .await;
        let Ok(m) = fetched else {
            return 0;
        };
        let e = m.get(&pn).map_or(0, |e| i32::try_from(*e).unwrap_or(0));
        lock(&self.epochs).insert(key, (Instant::now(), m));
        e
    }

    async fn limits(&self, p: &Principal) -> Limits {
        match self.catalog(p).await {
            Ok(cat) => limits_of(&self.svc, &cat),
            Err(_) => Limits::default(),
        }
    }
}

/// Whether `p` may see stream `def` of `cat` (topic listings).
#[must_use]
pub fn visible(data: &GatewayData, p: &Principal, cat: &TenantCatalog, def: &StreamDef) -> bool {
    data.visible(p, cat, def)
}
