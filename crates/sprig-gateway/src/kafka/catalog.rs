//! Topics and consumer groups in the tenant catalog (chapter 45 build
//! steps 12 and 13).
//!
//! **Topics** ([`StreamTopics`]) are the tenant's streams, named
//! `<database>.<stream>` (a change topic `shop.orders.changes`). Reads come
//! from the tenant's catalog snapshot (`sprig-streams`' `StreamsState`).
//! Changes are stream DDL run through the gateway's SQL engine as the
//! logged-in user, so chapter 29's privileges, the catalog change and the
//! physical tables are exactly what `CREATE STREAM` does in SQL:
//!
//! | Kafka | SQL |
//! |---|---|
//! | CreateTopics | ``CREATE STREAM `db`.`s` PARTITIONS n [RETENTION 'h hours'] [CLEANUP c]`` |
//! | DeleteTopics | ``DROP STREAM `db`.`s` `` |
//! | CreatePartitions | ``ALTER STREAM `db`.`s` PARTITIONS n`` |
//! | IncrementalAlterConfigs | ``ALTER STREAM ... RETENTION / CLEANUP`` |
//!
//! `retention.bytes` and `max.message.bytes` can't be set per topic yet:
//! SQL has no statement that sets a setting on one stream, so they follow
//! the database's and tenant's `stream.retention_bytes` and
//! `stream.max_message_bytes`, and setting them is refused with
//! `INVALID_CONFIG`.
//!
//! **Groups** ([`CatalogGroups`]) are `stream_groups` and `stream_offsets`
//! in `t<tenant>_sprig`. Offsets are written with `sprig-streams`'
//! `StreamsStore::commit_offset`; the group rows, the listing and the
//! deletion are this module's own statements on the same tables (marked
//! "adapter": `sprig-streams` has no API for them yet).

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::SprigError;
use sprig_common::ids::{ObjectId, TenantId};
use sprig_proto_kafka::group::{CommittedOffset, GroupState, GroupStore, StoreError, StoredGroup};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::topics::{
    Cleanup, NewTopic, Principal, TopicConfigs, TopicDefaults, TopicError, TopicInfo, TopicKind,
    Topics,
};
use sprig_streams::{StreamDef, StreamsState};
use uuid::Uuid;

use crate::http::sql::{Caller, SqlCall, SqlEngine, collect};
use crate::tenant::TenantServices;

fn state_of(cat: &TenantCatalog) -> StreamsState {
    cat.ext
        .get::<StreamsState>(sprig_streams::MODULE)
        .cloned()
        .unwrap_or_default()
}

fn quote(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

/// Topics over the tenant's streams.
pub struct StreamTopics {
    svc: Arc<TenantServices>,
    engine: Arc<SqlEngine>,
}

impl std::fmt::Debug for StreamTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamTopics").finish_non_exhaustive()
    }
}

/// Splits `<database>.<stream>`.
fn split(name: &str) -> Result<(&str, &str), TopicError> {
    sprig_proto_kafka::mem::split_topic(name)
}

/// SQL has no way yet to set a setting on one stream (`ALTER TABLE ...
/// SPRIG SET` doesn't resolve streams), so these two are set at the
/// database or tenant.
fn per_stream_bytes() -> TopicError {
    TopicError::new(
        err::INVALID_CONFIG,
        "retention.bytes and max.message.bytes can't be set on one stream yet: set \
         stream.retention_bytes or stream.max_message_bytes on the database or tenant",
    )
}

/// A SprigDB error as a Kafka one.
fn topic_error(e: &SprigError) -> TopicError {
    use sprig_common::SprigCode as C;
    let p = e.protocol();
    let code = match e.code() {
        Some(C::StreamNotFound) => err::UNKNOWN_TOPIC_OR_PARTITION,
        Some(C::StreamExists) => err::TOPIC_ALREADY_EXISTS,
        Some(C::StreamPartitionsDecrease) => err::INVALID_PARTITIONS,
        Some(C::StreamAppendOnly) => err::POLICY_VIOLATION,
        _ => match p.mysql_errno {
            1044 | 1142 | 1227 | 1143 => err::TOPIC_AUTHORIZATION_FAILED,
            1049 => err::UNKNOWN_TOPIC_OR_PARTITION,
            1050 => err::TOPIC_ALREADY_EXISTS,
            1064 => err::INVALID_REQUEST,
            _ => err::UNKNOWN_SERVER_ERROR,
        },
    };
    TopicError::new(code, format!("{} {}", p.mysql_errno, e))
}

impl StreamTopics {
    /// Topics of `svc`'s tenants, changed through `engine`.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>, engine: Arc<SqlEngine>) -> Self {
        Self { svc, engine }
    }

    async fn catalog(&self, p: &Principal) -> Result<Arc<TenantCatalog>, TopicError> {
        self.svc
            .cache
            .get(TenantId(p.tenant_id), Freshness::Latest)
            .await
            .map_err(|e| topic_error(&e))
    }

    fn info(&self, p: &Principal, cat: &TenantCatalog, def: &StreamDef) -> Option<TopicInfo> {
        use sprig_streams::settings as s;
        let db = cat.databases.get(def.db)?;
        let dc = &self.svc.dc;
        let obj = cat.object_ctx(dc, ObjectId(def.id.0));
        let dbc = cat.setting_ctx(dc).with_db(def.db);
        let get = |k: &'static sprig_settings::Setting<u64>, c| {
            self.svc.settings.get(k, c, Some(&cat.settings))
        };
        let hours = get(&s::RETENTION_HOURS, &obj);
        let bytes = get(&s::RETENTION_BYTES, &obj);
        let max = get(&s::MAX_MESSAGE_BYTES, &obj);
        let hours_db = get(&s::RETENTION_HOURS, &dbc);
        let bytes_db = get(&s::RETENTION_BYTES, &dbc);
        let max_db = get(&s::MAX_MESSAGE_BYTES, &dbc);
        let ms = |h: u64| i64::try_from(h.saturating_mul(3_600_000)).unwrap_or(i64::MAX);
        let kbytes = |b: u64| {
            if b == 0 {
                -1
            } else {
                i64::try_from(b).unwrap_or(i64::MAX)
            }
        };
        let kmax = |m: u64| i32::try_from(m).unwrap_or(i32::MAX);
        let cleanup = match def.cleanup {
            sprig_streams::model::Cleanup::Compact => Cleanup::Compact,
            sprig_streams::model::Cleanup::Delete => Cleanup::Delete,
        };
        Some(TopicInfo {
            name: format!("{}.{}", db.name, def.name),
            stream_id: def.id.0,
            topic_id: Uuid::from_u64_pair(p.tenant_id, def.id.0),
            partitions: i32::try_from(def.partitions).unwrap_or(i32::MAX),
            kind: if def.kind == sprig_streams::model::StreamKind::Changes {
                TopicKind::Changes
            } else {
                TopicKind::Events
            },
            configs: TopicConfigs {
                retention_ms: (hours != hours_db).then(|| ms(hours)),
                retention_bytes: (bytes != bytes_db).then(|| kbytes(bytes)),
                cleanup: (cleanup == Cleanup::Compact).then_some(cleanup),
                max_message_bytes: (max != max_db).then(|| kmax(max)),
            },
            defaults: TopicDefaults {
                retention_ms: ms(hours_db),
                retention_bytes: kbytes(bytes_db),
                cleanup: Cleanup::Delete,
                max_message_bytes: kmax(max_db),
                partitions: i32::try_from(get(&s::PARTITIONS_DEFAULT, &dbc)).unwrap_or(12),
            },
        })
    }

    fn find(&self, p: &Principal, cat: &TenantCatalog, name: &str) -> Option<TopicInfo> {
        let (db, stream) = name.split_once('.')?;
        let st = state_of(cat);
        let def = st
            .all()
            .filter(|d| d.state == sprig_streams::model::StreamState::Active)
            .find(|d| d.name == stream && cat.databases.get(d.db).is_some_and(|r| r.name == db))?;
        self.info(p, cat, def)
    }

    /// Runs `sql` as the principal.
    async fn run(&self, p: &Principal, sql: String) -> Result<(), TopicError> {
        let auth = p
            .handle
            .as_ref()
            .and_then(|h| h.downcast_ref::<sprig_auth::Principal>())
            .cloned()
            .ok_or_else(|| TopicError::new(err::CLUSTER_AUTHORIZATION_FAILED, "no SQL identity"))?;
        let caller = Caller {
            peer: std::net::SocketAddr::new(auth.client_ip, 0),
            principal: auth,
            tls: None,
            listener: "kafka",
            generation: 0,
            authorization: None,
            client_dc: None,
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let call = SqlCall {
            database: String::new(),
            sql: sql.clone(),
            multi: false,
        };
        let engine = Arc::clone(&self.engine);
        let run = tokio::spawn(async move { engine.run(&caller, call, None, tx).await });
        let r = collect(rx, 1 << 20, None).await;
        let _ = run.await;
        tracing::debug!(sql = %sql, ok = r.is_ok(), "kafka topic DDL");
        r.map(|_| ()).map_err(|e| topic_error(&e))
    }

    /// Waits until the catalog cache shows the change (`present`: whether
    /// the topic should exist), at most 5 s.
    async fn settle(&self, p: &Principal, name: &str, check: impl Fn(Option<&TopicInfo>) -> bool) {
        for _ in 0..50 {
            if let Ok(cat) = self.catalog(p).await
                && check(self.find(p, &cat, name).as_ref())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn apply_configs(
        &self,
        p: &Principal,
        name: &str,
        old: TopicConfigs,
        new: TopicConfigs,
    ) -> Result<(), TopicError> {
        if new.retention_bytes != old.retention_bytes
            || new.max_message_bytes != old.max_message_bytes
        {
            return Err(per_stream_bytes());
        }
        let (db, s) = split(name)?;
        let target = format!("{}.{}", quote(db), quote(s));
        let mut alter = Vec::new();
        if new.retention_ms != old.retention_ms {
            match new.retention_ms {
                Some(ms) if ms >= 0 => alter.push(format!(
                    "RETENTION '{} hours'",
                    sprig_proto_kafka::topics::retention_hours(ms)
                )),
                _ => {
                    return Err(TopicError::new(
                        err::INVALID_CONFIG,
                        "retention.ms: no time limit (-1) and resetting to the inherited value are not supported yet",
                    ));
                }
            }
        }
        if new.cleanup != old.cleanup {
            alter.push(format!(
                "CLEANUP {}",
                new.cleanup.unwrap_or(Cleanup::Delete).name()
            ));
        }
        if !alter.is_empty() {
            self.run(p, format!("ALTER STREAM {target} {}", alter.join(" ")))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Topics for StreamTopics {
    async fn list(&self, p: &Principal) -> Result<Vec<TopicInfo>, TopicError> {
        let cat = self.catalog(p).await?;
        let st = state_of(&cat);
        let mut out: Vec<TopicInfo> = st
            .all()
            .filter(|d| d.state == sprig_streams::model::StreamState::Active)
            .filter_map(|d| self.info(p, &cat, d))
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn get(&self, p: &Principal, name: &str) -> Result<Option<TopicInfo>, TopicError> {
        let cat = self.catalog(p).await?;
        Ok(self.find(p, &cat, name))
    }

    async fn defaults(&self, p: &Principal, name: &str) -> Result<TopicDefaults, TopicError> {
        use sprig_streams::settings as s;
        let cat = self.catalog(p).await?;
        let (db, _) = split(name)?;
        let dc = &self.svc.dc;
        let mut ctx = cat.setting_ctx(dc);
        if let Some(d) = cat.databases.iter().find(|d| d.name == db) {
            ctx = ctx.with_db(d.db_id);
        }
        let get = |k: &'static sprig_settings::Setting<u64>| {
            self.svc.settings.get(k, &ctx, Some(&cat.settings))
        };
        Ok(TopicDefaults {
            partitions: i32::try_from(get(&s::PARTITIONS_DEFAULT)).unwrap_or(12),
            ..TopicDefaults::default()
        })
    }

    async fn create(
        &self,
        p: &Principal,
        t: NewTopic,
        validate_only: bool,
    ) -> Result<TopicInfo, TopicError> {
        let (db, s) = split(&t.name)?;
        let cat = self.catalog(p).await?;
        if !cat.databases.iter().any(|d| d.name == db) {
            return Err(TopicError::new(
                err::UNKNOWN_TOPIC_OR_PARTITION,
                format!("1049 Unknown database '{db}': a topic is named <database>.<stream>"),
            ));
        }
        if self.find(p, &cat, &t.name).is_some() {
            return Err(TopicError::new(
                err::TOPIC_ALREADY_EXISTS,
                format!("39942 ER_SPRIG_STREAM_EXISTS: {} already exists", t.name),
            ));
        }
        if t.configs.retention_bytes.is_some() || t.configs.max_message_bytes.is_some() {
            return Err(per_stream_bytes());
        }
        if t.configs.retention_ms.is_some_and(|ms| ms < 0) {
            return Err(TopicError::new(
                err::INVALID_CONFIG,
                "retention.ms: no time limit (-1) is not supported yet",
            ));
        }
        if validate_only {
            return Ok(TopicInfo {
                name: t.name.clone(),
                stream_id: 0,
                topic_id: Uuid::nil(),
                partitions: t.partitions,
                kind: TopicKind::Events,
                configs: t.configs,
                defaults: self.defaults(p, &format!("{db}.{s}")).await?,
            });
        }
        let mut sql = format!(
            "CREATE STREAM {}.{} PARTITIONS {}",
            quote(db),
            quote(s),
            t.partitions
        );
        if let Some(ms) = t.configs.retention_ms {
            let _ = write!(
                sql,
                " RETENTION '{} hours'",
                sprig_proto_kafka::topics::retention_hours(ms)
            );
        }
        if let Some(c) = t.configs.cleanup {
            let _ = write!(sql, " CLEANUP {}", c.name());
        }
        self.run(p, sql).await?;
        self.settle(p, &t.name, |i| i.is_some()).await;
        let base = TopicConfigs {
            retention_ms: t.configs.retention_ms,
            cleanup: t.configs.cleanup,
            ..TopicConfigs::default()
        };
        if base != t.configs {
            self.apply_configs(p, &t.name, base, t.configs).await?;
        }
        let cat = self.catalog(p).await?;
        self.find(p, &cat, &t.name).ok_or_else(|| {
            TopicError::new(
                err::UNKNOWN_SERVER_ERROR,
                "the stream was created but isn't visible yet",
            )
        })
    }

    async fn delete(&self, p: &Principal, name: &str) -> Result<TopicInfo, TopicError> {
        let cat = self.catalog(p).await?;
        let info = self.find(p, &cat, name).ok_or_else(|| {
            TopicError::new(
                err::UNKNOWN_TOPIC_OR_PARTITION,
                format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {name}"),
            )
        })?;
        if info.kind == TopicKind::Changes {
            return Err(TopicError::new(
                err::POLICY_VIOLATION,
                format!("{name} is a change topic: turn stream.change_topic off on its object"),
            ));
        }
        let (db, s) = split(name)?;
        self.run(p, format!("DROP STREAM {}.{}", quote(db), quote(s)))
            .await?;
        self.settle(p, name, |i| i.is_none()).await;
        Ok(info)
    }

    async fn grow(
        &self,
        p: &Principal,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        let cat = self.catalog(p).await?;
        let info = self.find(p, &cat, name).ok_or_else(|| {
            TopicError::new(
                err::UNKNOWN_TOPIC_OR_PARTITION,
                format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {name}"),
            )
        })?;
        if partitions < info.partitions {
            return Err(TopicError::new(
                err::INVALID_PARTITIONS,
                format!(
                    "39943 ER_SPRIG_STREAM_PARTITIONS_DECREASE: {name} has {} partitions; partitions can only grow",
                    info.partitions
                ),
            ));
        }
        if partitions == info.partitions {
            return Err(TopicError::new(
                err::INVALID_PARTITIONS,
                format!("{name} already has {partitions} partitions"),
            ));
        }
        if validate_only {
            return Ok(());
        }
        let (db, s) = split(name)?;
        self.run(
            p,
            format!(
                "ALTER STREAM {}.{} PARTITIONS {partitions}",
                quote(db),
                quote(s)
            ),
        )
        .await?;
        self.settle(p, name, |i| i.is_some_and(|i| i.partitions == partitions))
            .await;
        Ok(())
    }

    async fn set_configs(
        &self,
        p: &Principal,
        name: &str,
        c: TopicConfigs,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        let cat = self.catalog(p).await?;
        let info = self.find(p, &cat, name).ok_or_else(|| {
            TopicError::new(
                err::UNKNOWN_TOPIC_OR_PARTITION,
                format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {name}"),
            )
        })?;
        if validate_only {
            return Ok(());
        }
        self.apply_configs(p, name, info.configs, c).await
    }
}

/// `stream_groups` and `stream_offsets` in the tenant catalog.
pub struct CatalogGroups {
    svc: Arc<TenantServices>,
    migrated: Mutex<HashSet<u64>>,
}

impl std::fmt::Debug for CatalogGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogGroups").finish_non_exhaustive()
    }
}

fn store_err(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

impl CatalogGroups {
    /// Groups in `svc`'s tenant catalogs.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            migrated: Mutex::new(HashSet::new()),
        }
    }

    /// A connection to the tenant catalog's writer, with the streams
    /// module's tables in place, and the catalog schema.
    async fn conn(&self, tenant: u64) -> Result<(mysql_async::Conn, String), StoreError> {
        let ep = self
            .svc
            .locator
            .writer(TenantId(tenant))
            .await
            .map_err(store_err)?;
        let mut conn = ep.pool.get_conn().await.map_err(store_err)?;
        let schema = ep.store.schema().to_owned();
        let done = self
            .migrated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&tenant);
        if !done {
            sprig_streams::store::migrate(&mut conn, &schema)
                .await
                .map_err(store_err)?;
            self.migrated
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(tenant);
        }
        Ok((conn, schema))
    }
}

#[async_trait]
impl GroupStore for CatalogGroups {
    async fn commit(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), StoreError> {
        let (mut conn, schema) = self.conn(tenant).await?;
        let store = sprig_streams::StreamsStore::new(schema);
        let mut tx = conn
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(store_err)?;
        for o in offsets {
            store
                .commit_offset(
                    &mut tx,
                    &sprig_streams::store::CommittedOffset {
                        group: group.to_owned(),
                        stream: sprig_streams::StreamId(o.stream_id),
                        partition: sprig_streams::PartitionNo(
                            u32::try_from(o.partition).unwrap_or(0),
                        ),
                        offset: u64::try_from(o.offset).unwrap_or(0),
                        leader_epoch: o.leader_epoch.and_then(|e| u32::try_from(e).ok()),
                        metadata: o.metadata.clone(),
                    },
                )
                .await
                .map_err(store_err)?;
        }
        tx.commit().await.map_err(store_err)
    }

    async fn fetch(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> Result<Vec<CommittedOffset>, StoreError> {
        // Adapter: all of a group's offsets in one read.
        let (mut conn, schema) = self.conn(tenant).await?;
        let rows: Vec<(u64, u32, u64, Option<u32>, Option<String>)> = conn
            .exec(
                format!(
                    "SELECT stream_id, partition_no, offset_no, leader_epoch, metadata FROM {}.stream_offsets \
                     WHERE group_name = ? ORDER BY stream_id, partition_no",
                    quote(&schema)
                ),
                (group,),
            )
            .await
            .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|(s, p, o, e, m)| CommittedOffset {
                stream_id: s,
                partition: i32::try_from(p).unwrap_or(i32::MAX),
                offset: i64::try_from(o).unwrap_or(i64::MAX),
                leader_epoch: e.and_then(|e| i32::try_from(e).ok()),
                metadata: m,
            })
            .filter(|c| only.is_none_or(|k| k.contains(&(c.stream_id, c.partition))))
            .collect())
    }

    async fn save_group(
        &self,
        tenant: u64,
        g: &StoredGroup,
        state: GroupState,
    ) -> Result<(), StoreError> {
        // Adapter: the group row.
        let (mut conn, schema) = self.conn(tenant).await?;
        conn.exec_drop(
            format!(
                "INSERT INTO {}.stream_groups (group_name, protocol_type, generation, state, updated_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE \
                 protocol_type = VALUES(protocol_type), generation = VALUES(generation), \
                 state = VALUES(state), updated_at = VALUES(updated_at)",
                quote(&schema)
            ),
            (
                g.group.as_str(),
                g.protocol_type.as_str(),
                u32::try_from(g.generation).unwrap_or(0),
                state.stored(),
            ),
        )
        .await
        .map_err(store_err)
    }

    async fn load_group(
        &self,
        tenant: u64,
        group: &str,
    ) -> Result<Option<StoredGroup>, StoreError> {
        let (mut conn, schema) = self.conn(tenant).await?;
        let row: Option<(String, u32)> = conn
            .exec_first(
                format!(
                    "SELECT protocol_type, generation FROM {}.stream_groups WHERE group_name = ? AND state <> 'dead'",
                    quote(&schema)
                ),
                (group,),
            )
            .await
            .map_err(store_err)?;
        Ok(row.map(|(pt, g)| StoredGroup {
            group: group.to_owned(),
            protocol_type: pt,
            generation: i32::try_from(g).unwrap_or(i32::MAX),
        }))
    }

    async fn list_groups(&self, tenant: u64) -> Result<Vec<StoredGroup>, StoreError> {
        let (mut conn, schema) = self.conn(tenant).await?;
        let rows: Vec<(String, String, u32)> = conn
            .query(format!(
                "SELECT group_name, protocol_type, generation FROM {}.stream_groups \
                 WHERE state <> 'dead' ORDER BY group_name",
                quote(&schema)
            ))
            .await
            .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|(g, pt, gen_)| StoredGroup {
                group: g,
                protocol_type: pt,
                generation: i32::try_from(gen_).unwrap_or(i32::MAX),
            })
            .collect())
    }

    async fn delete_group(&self, tenant: u64, group: &str) -> Result<(), StoreError> {
        let (mut conn, schema) = self.conn(tenant).await?;
        let mut tx = conn
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(store_err)?;
        for t in ["stream_offsets", "stream_groups"] {
            tx.exec_drop(
                format!("DELETE FROM {}.{t} WHERE group_name = ?", quote(&schema)),
                (group,),
            )
            .await
            .map_err(store_err)?;
        }
        tx.commit().await.map_err(store_err)
    }
}
