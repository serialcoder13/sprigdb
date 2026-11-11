//! Inherited streams over Kafka (chapter 45, "Inherited streams", "The
//! Kafka view"; chapter 42; R9, r9fet):
//!
//! - **A member** produces to `<db>.<stream>` of an inherited stream as if
//!   it were its own: the first produce creates its instance (an ordinary
//!   stream of the member, which every read then finds). Its records get
//!   `x-sprig-tenant` and `x-sprig-origin-tenant` set to its own name; a
//!   record whose `x-sprig-tenant` names another tenant is refused
//!   (`INVALID_RECORD`, 39896). Reading a topic whose instance doesn't
//!   exist yet answers "unknown topic" until the first publish.
//! - **The declaring tenant (or an intermediate member)** produces to the
//!   definition's topic with the record header `x-sprig-tenant: <member>`:
//!   each record goes to that member's instance (created if needed), down
//!   the tree only (39896 otherwise). The response carries no offsets (-1):
//!   the records went to several instances.

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_inherit::msg::{Cause, DefKind, Definition, StreamSpec};
use sprig_proto_kafka::broker::data::{AppendTicket, DataTopic, ProduceRecord};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::records::Header;
use sprig_proto_kafka::topics::{Principal, TopicError, TopicKind};
use sprig_streams::publish;
use uuid::Uuid;

use super::{GatewayData, kafka_err, unknown};
use crate::inherited;

/// Marks a definition's topic in `DataTopic::stream_id`.
pub(super) const DEF_FLAG: u64 = 1 << 63;

/// The header naming a record's member.
const TENANT: &str = "x-sprig-tenant";
/// The header naming the publisher.
const ORIGIN: &str = "x-sprig-origin-tenant";

fn bad_record(m: impl std::fmt::Display) -> TopicError {
    TopicError::new(
        err::INVALID_RECORD,
        format!("39896 ER_SPRIG_INHERIT_TARGET_TENANT: {m}"),
    )
}

/// The record's `x-sprig-tenant`, if any.
fn target_of(r: &ProduceRecord) -> Option<String> {
    r.headers
        .iter()
        .rev()
        .find(|h| h.key == TENANT)
        .and_then(|h| h.value.as_ref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
}

/// `r`'s headers with the server's two set.
fn stamp(r: &mut ProduceRecord, tenant: &str, origin: &str) {
    r.headers.retain(|h| h.key != TENANT && h.key != ORIGIN);
    r.headers.push(Header {
        key: TENANT.to_owned(),
        value: Some(tenant.as_bytes().to_vec()),
    });
    r.headers.push(Header {
        key: ORIGIN.to_owned(),
        value: Some(origin.as_bytes().to_vec()),
    });
}

/// What an unknown topic name is.
pub(super) enum Inh {
    /// Not an inherited stream (or nothing to read yet).
    Unknown,
    /// The member's instance was just created: resolve again.
    Retry,
    /// A definition's topic.
    Def(DataTopic),
}

impl GatewayData {
    /// A topic that isn't one of the tenant's streams: an inherited
    /// stream's (see the module docs), or unknown.
    pub(super) async fn inherited_topic(
        &self,
        p: &Principal,
        name: &str,
        write: bool,
    ) -> Result<Inh, TopicError> {
        let (db, stream) = sprig_proto_kafka::mem::split_topic(name)?;
        let cat = self.catalog(p).await?;
        let chain = inherited::chain(&self.svc, &cat)
            .await
            .map_err(|e| kafka_err(&e))?;
        let own = inherited::state_of(&cat);
        let def = own
            .def(db, DefKind::Stream, stream)
            .cloned()
            .or_else(|| chain.resolve(db, DefKind::Stream, stream));
        let Some(def) = def else {
            return Ok(Inh::Unknown);
        };
        let g = self.grants(p, &cat);
        let lctn = self.lctn(&cat);
        let db_key = sprig_catalog::ddl::database_key(db, lctn);
        let name_key =
            sprig_catalog::ddl::object_key(sprig_catalog::store::ObjectKind::Table, stream, lctn);
        let action = if write {
            sprig_auth::authz::Action::Insert
        } else {
            sprig_auth::authz::Action::Select
        };
        let ok = g.unrestricted()
            || g.allows(
                action,
                sprig_auth::authz::Target::Named {
                    db_key: &db_key,
                    name_key: &name_key,
                },
            )
            || g.allows(
                action,
                sprig_auth::authz::Target::Database { db_key: &db_key },
            );
        if !ok {
            return Err(unknown(name));
        }
        if chain.reaches_level(&def, 0) {
            if !write {
                // No instance yet: nothing to read (it appears with the
                // first publish).
                return Ok(Inh::Unknown);
            }
            inherited::streams::ensure(&self.svc, &chain, &def, Cause::Publish)
                .await
                .map_err(|e| kafka_err(&e))?;
            return Ok(Inh::Retry);
        }
        if !write {
            return Ok(Inh::Unknown);
        }
        Ok(Inh::Def(self.definition_topic(&cat, &def, name)))
    }

    fn definition_topic(
        &self,
        cat: &sprig_catalog::TenantCatalog,
        def: &Definition,
        name: &str,
    ) -> DataTopic {
        let spec: StreamSpec = serde_json::from_value(def.definition.clone()).unwrap_or_default();
        let max = self.svc.settings.get(
            &sprig_streams::settings::MAX_MESSAGE_BYTES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        );
        DataTopic {
            name: name.to_owned(),
            stream_id: DEF_FLAG | def.object_id,
            topic_id: Uuid::from_u64_pair(cat.tenant.0, DEF_FLAG | def.object_id),
            partitions: i32::try_from(spec.partitions.max(1)).unwrap_or(1),
            kind: TopicKind::Events,
            compact: spec.cleanup == "compact",
            max_message_bytes: usize::try_from(max).unwrap_or(usize::MAX),
        }
    }

    /// A member's produce into its own instance: stamped, and refused for
    /// another tenant's name.
    pub(super) fn stamp_member(
        p: &Principal,
        records: &mut [ProduceRecord],
    ) -> Result<(), TopicError> {
        for r in records.iter_mut() {
            if let Some(t) = target_of(r)
                && t != p.tenant
            {
                return Err(bad_record(format!(
                    "a record for tenant {t} on tenant {}'s own instance",
                    p.tenant
                )));
            }
            stamp(r, &p.tenant, &p.tenant);
        }
        Ok(())
    }

    /// A produce to a definition's topic: each record into the instance
    /// its `x-sprig-tenant` names (see the module docs).
    #[allow(clippy::too_many_lines)]
    pub(super) async fn append_by_tenant(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        records: Vec<ProduceRecord>,
    ) -> Result<AppendTicket, TopicError> {
        let me = sprig_common::ids::TenantId(p.tenant_id);
        let cat = self.catalog(p).await?;
        let own = inherited::state_of(&cat);
        let def = own
            .def_by_id(t.stream_id & !DEF_FLAG)
            .cloned()
            .ok_or_else(|| unknown(&t.name))?;
        let count = records.len();
        let mut groups: BTreeMap<String, Vec<ProduceRecord>> = BTreeMap::new();
        let svc = Arc::clone(&self.svc);
        for mut r in records {
            let target = target_of(&r).ok_or_else(|| {
                bad_record(format!(
                    "a record to {} names no member: set the header {TENANT}",
                    t.name
                ))
            })?;
            if target == "*" {
                // A broadcast (chapter 42): logged now, delivered to every
                // member by the fan-out.
                r.headers.retain(|h| h.key != TENANT && h.key != ORIGIN);
                let rec = publish::Publish {
                    key: r.key.take(),
                    value: r.value.take(),
                    headers: super::headers_to_json(&r.headers),
                    partition: None,
                    ts_ms: None,
                };
                inherited::broadcast::publish_now(
                    &svc,
                    &def,
                    me,
                    inherited::broadcast::Form::default(),
                    &rec,
                )
                .await
                .map_err(|e| kafka_err(&e))?;
                continue;
            }
            groups.entry(target).or_default().push(r);
        }
        let mut first = None;
        for (target, mut recs) in groups {
            let no = || {
                bad_record(format!(
                    "'{target}' isn't a member of {} below this tenant",
                    t.name
                ))
            };
            let tid = inherited::tenant_by_name(&svc, &target)
                .await
                .map_err(|e| kafka_err(&e))?
                .ok_or_else(no)?;
            let tcat = inherited::catalog(&svc, tid)
                .await
                .map_err(|e| kafka_err(&e))?;
            let tchain = inherited::chain(&svc, &tcat)
                .await
                .map_err(|e| kafka_err(&e))?;
            let tdef = tchain
                .resolve(&def.db_name, DefKind::Stream, &def.name)
                .filter(|d| d.key() == def.key())
                .ok_or_else(no)?;
            if !tchain.is_proper_ancestor(me) {
                return Err(no());
            }
            let si = inherited::streams::ensure(&svc, &tchain, &tdef, Cause::Publish)
                .await
                .map_err(|e| kafka_err(&e))?;
            let schema = si.outbox_schema().map_err(|e| kafka_err(&e))?;
            let mut pubs = Vec::with_capacity(recs.len());
            for r in &mut recs {
                stamp(r, &target, &p.tenant);
                let headers = super::headers_to_json(&r.headers);
                publish::check_record(
                    &si.stream,
                    r.key.as_deref(),
                    r.value.as_ref().map_or(0, Vec::len),
                    0,
                    u64::try_from(t.max_message_bytes).unwrap_or(u64::MAX),
                )
                .map_err(|e| kafka_err(&e))?;
                pubs.push(publish::Publish {
                    key: r.key.take(),
                    value: r.value.take(),
                    headers,
                    partition: Some(u32::try_from(partition).unwrap_or(0)),
                    ts_ms: Some(r.timestamp_ms),
                });
            }
            let mut c = self.conn(tid.0).await?;
            let out = publish::append(
                &mut c,
                &schema,
                &si.stream,
                u32::try_from(partition).unwrap_or(0),
                &pubs,
                None,
            )
            .await
            .map_err(|e| kafka_err(&e))?;
            if let publish::AppendOutcome::Appended(id) | publish::AppendOutcome::Duplicate(id) =
                out
            {
                first.get_or_insert(id);
            }
            metrics::counter!("sprig_inherit_publishes_total", "kind" => "stream", "path" => "direct")
                .increment(pubs.len() as u64);
        }
        Ok(AppendTicket {
            partition,
            first_event: first.unwrap_or_default(),
            count,
            base_offset: Some(-1),
            duplicate: false,
        })
    }
}

/// What Metadata shows for an inherited stream's topic.
pub(crate) struct InheritedTopic {
    /// The id it is shown with (a definition's carries [`DEF_FLAG`]).
    pub stream_id: u64,
    /// Partitions.
    pub partitions: i32,
}

impl GatewayData {
    /// Metadata of a topic that isn't one of the tenant's streams: a
    /// member's view of an inherited stream (before or after its instance
    /// exists), or the declaring tenant's definition topic; `None` for
    /// anything else, or one the principal may neither read nor write.
    pub(crate) async fn inherited_info(&self, p: &Principal, name: &str) -> Option<InheritedTopic> {
        if name.starts_with(FANIN_PREFIX) {
            let t = self.fanin_topic(p, name, false).await.ok()?;
            return Some(InheritedTopic {
                stream_id: t.stream_id,
                partitions: t.partitions,
            });
        }
        let (db, stream) = sprig_proto_kafka::mem::split_topic(name).ok()?;
        let cat = self.catalog(p).await.ok()?;
        let chain = inherited::chain(&self.svc, &cat).await.ok()?;
        let own = inherited::state_of(&cat);
        let def = own
            .def(db, DefKind::Stream, stream)
            .cloned()
            .or_else(|| chain.resolve(db, DefKind::Stream, stream))?;
        let g = self.grants(p, &cat);
        let lctn = self.lctn(&cat);
        let db_key = sprig_catalog::ddl::database_key(db, lctn);
        let name_key =
            sprig_catalog::ddl::object_key(sprig_catalog::store::ObjectKind::Table, stream, lctn);
        let visible = g.unrestricted()
            || [
                sprig_auth::authz::Action::Select,
                sprig_auth::authz::Action::Insert,
            ]
            .into_iter()
            .any(|a| {
                g.allows(
                    a,
                    sprig_auth::authz::Target::Named {
                        db_key: &db_key,
                        name_key: &name_key,
                    },
                ) || g.allows(a, sprig_auth::authz::Target::Database { db_key: &db_key })
            });
        if !visible {
            return None;
        }
        let spec: StreamSpec = serde_json::from_value(def.definition.clone()).unwrap_or_default();
        let member = chain.reaches_level(&def, 0);
        Some(InheritedTopic {
            stream_id: if member { 0 } else { DEF_FLAG | def.object_id },
            partitions: i32::try_from(spec.partitions.max(1)).unwrap_or(1),
        })
    }
}

impl GatewayData {
    /// The inherited streams' topic names the tenant has no stream of:
    /// its own definitions, and those reaching it without an instance yet.
    pub(crate) async fn inherited_names(&self, p: &Principal) -> Vec<String> {
        let Ok(cat) = self.catalog(p).await else {
            return Vec::new();
        };
        let Ok(chain) = inherited::chain(&self.svc, &cat).await else {
            return Vec::new();
        };
        let own = inherited::state_of(&cat);
        let mut out: Vec<String> = own
            .defs
            .values()
            .filter(|d| d.kind == DefKind::Stream)
            .map(|d| format!("{}.{}", d.db_name, d.name))
            .collect();
        for d in chain.reaching(DefKind::Stream) {
            if !own.instances.contains_key(&d.key()) {
                out.push(format!("{}.{}", d.db_name, d.name));
            }
        }
        // Fan-in topics of the members below (SPRIG_FAN_IN).
        out.extend(self.fanin_names(p).await);
        out.sort();
        out.dedup();
        out
    }
}

// ---------------------------------------------------------------------------
// Fan-in topics (chapter 45, "The Kafka view"; chapter 42, "Fan-in"; R9,
// r9next): in an ancestor P holding SPRIG_FAN_IN and SELECT on an inherited
// stream's name, each existing instance of a descendant member T is also
// the read-only topic `__t.<T>.<db>.<stream>`. A consumer group subscribing
// to the pattern `__t\..*\.<db>\.<stream>` reads every member; its offsets
// go to P's `stream_offsets` with `instance_tenant_id = T`. The topic's
// stream id is [`FANIN_FLAG`] | an id P's catalog assigns to (T, the
// instance's stream id) in `stream_fanin_topics` (R9, r9queue: any 64-bit
// tenant id works; r9next packed T into the id, which capped tenant ids at
// 2^38), so the broker's group code (keyed by stream id) needs no change.
// Ids are cached per gateway; a row never changes once written. Members
// whose residency sets `deny_remote_access` are left out.
// ---------------------------------------------------------------------------

/// Marks a fan-in topic's stream id.
pub(crate) const FANIN_FLAG: u64 = 1 << 62;
/// A fan-in topic's name starts with this.
pub(crate) const FANIN_PREFIX: &str = "__t.";

/// Whether stream id `id` is a fan-in topic's.
#[must_use]
pub(crate) const fn is_fanin(id: u64) -> bool {
    id & FANIN_FLAG != 0 && id & DEF_FLAG == 0
}

/// Fan-in topic ids this gateway has seen: `(reader, topic stream id)` to
/// `(member, instance stream id)` and back.
#[derive(Default)]
struct FanInIds {
    by_id: std::collections::HashMap<(u64, u64), (u64, u64)>,
    by_instance: std::collections::HashMap<(u64, u64, u64), u64>,
    migrated: std::collections::HashSet<String>,
}

fn fanin_ids() -> std::sync::MutexGuard<'static, FanInIds> {
    static M: std::sync::OnceLock<std::sync::Mutex<FanInIds>> = std::sync::OnceLock::new();
    M.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn remember(reader: u64, id: u64, member: u64, stream: u64) {
    let mut m = fanin_ids();
    m.by_id.insert((reader, id), (member, stream));
    m.by_instance.insert((reader, member, stream), id);
}

/// `(member, stream)` of reader `reader`'s fan-in topic `id`, if this
/// gateway has seen it (the topic was resolved before it is fetched).
#[must_use]
pub(crate) fn fanin_cached(reader: u64, id: u64) -> Option<(u64, u64)> {
    if !is_fanin(id) {
        return None;
    }
    fanin_ids().by_id.get(&(reader, id)).copied()
}

/// The reader's catalog connection, its streams module migrated.
async fn reader_catalog(
    svc: &crate::tenant::TenantServices,
    reader: u64,
) -> sprig_common::Result<(mysql_async::Conn, String)> {
    use sprig_catalog::CatalogLocator as _;
    let ep = svc
        .locator
        .writer(sprig_common::ids::TenantId(reader))
        .await?;
    let mut c = ep
        .pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let schema = ep.store.schema().to_owned();
    if !fanin_ids().migrated.contains(&schema) {
        sprig_streams::store::migrate(&mut c, &schema).await?;
        fanin_ids().migrated.insert(schema.clone());
    }
    Ok((c, schema))
}

/// The stream id of member `member`'s instance stream `stream` as reader
/// `reader`'s fan-in topic, assigned in the reader's catalog on first use.
///
/// # Errors
/// The reader's catalog's.
pub(crate) async fn fanin_id(
    svc: &crate::tenant::TenantServices,
    reader: u64,
    member: u64,
    stream: u64,
) -> sprig_common::Result<u64> {
    if let Some(id) = fanin_ids()
        .by_instance
        .get(&(reader, member, stream))
        .copied()
    {
        return Ok(id);
    }
    let (mut c, schema) = reader_catalog(svc, reader).await?;
    let n = sprig_streams::StreamsStore::new(schema)
        .fanin_topic_id(&mut c, member, stream)
        .await?;
    if n >= FANIN_FLAG {
        return Err(sprig_common::SprigError::internal(
            "fan-in topic ids are exhausted",
        ));
    }
    let id = FANIN_FLAG | n;
    remember(reader, id, member, stream);
    Ok(id)
}

/// `(member, stream)` of reader `reader`'s fan-in topic `id`; `None` for
/// an id that isn't a fan-in topic's or was never assigned.
///
/// # Errors
/// The reader's catalog's.
pub(crate) async fn fanin_parts(
    svc: &crate::tenant::TenantServices,
    reader: u64,
    id: u64,
) -> sprig_common::Result<Option<(u64, u64)>> {
    if !is_fanin(id) {
        return Ok(None);
    }
    if let Some(x) = fanin_cached(reader, id) {
        return Ok(Some(x));
    }
    let (mut c, schema) = reader_catalog(svc, reader).await?;
    let got = sprig_streams::StreamsStore::new(schema)
        .fanin_topic(&mut c, id & !FANIN_FLAG)
        .await?;
    if let Some((member, stream)) = got {
        remember(reader, id, member, stream);
    }
    Ok(got)
}

/// `__t.<tenant>.<db>.<stream>`.
#[must_use]
pub(crate) fn fanin_name(tenant: &str, db: &str, stream: &str) -> String {
    format!("{FANIN_PREFIX}{tenant}.{db}.{stream}")
}

/// `(tenant, db, stream)` of a fan-in topic name.
#[must_use]
pub(crate) fn parse_fanin_name(name: &str) -> Option<(&str, &str, &str)> {
    let rest = name.strip_prefix(FANIN_PREFIX)?;
    let (tenant, rest) = rest.split_once('.')?;
    let (db, stream) = rest.split_once('.')?;
    (!tenant.is_empty() && !db.is_empty() && !stream.is_empty()).then_some((tenant, db, stream))
}

fn fanin_denied(name: &str) -> TopicError {
    TopicError::new(
        err::TOPIC_AUTHORIZATION_FAILED,
        format!(
            "1227 Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for \
             topic '{name}'"
        ),
    )
}

impl GatewayData {
    /// Whether `p` holds `SPRIG_FAN_IN`.
    fn fanin_allowed(&self, p: &Principal, cat: &sprig_catalog::TenantCatalog) -> bool {
        let g = self.grants(p, cat);
        g.unrestricted()
            || g.allows(
                sprig_auth::authz::Action::FanIn,
                sprig_auth::authz::Target::Tenant,
            )
    }

    /// Whether `p` may `SELECT` the name `db.stream` in its tenant.
    fn may_select(
        &self,
        p: &Principal,
        cat: &sprig_catalog::TenantCatalog,
        db: &str,
        stream: &str,
    ) -> bool {
        let g = self.grants(p, cat);
        let lctn = self.lctn(cat);
        let db_key = sprig_catalog::ddl::database_key(db, lctn);
        let name_key =
            sprig_catalog::ddl::object_key(sprig_catalog::store::ObjectKind::Table, stream, lctn);
        g.unrestricted()
            || g.allows(
                sprig_auth::authz::Action::Select,
                sprig_auth::authz::Target::Named {
                    db_key: &db_key,
                    name_key: &name_key,
                },
            )
            || g.allows(
                sprig_auth::authz::Action::Select,
                sprig_auth::authz::Target::Database { db_key: &db_key },
            )
    }

    /// The stream definitions `p` may fan in: its tenant's own, and those
    /// reaching it as an intermediate member, with `SELECT` on the name.
    async fn fanin_defs(&self, p: &Principal) -> Vec<Arc<Definition>> {
        let Ok(cat) = self.catalog(p).await else {
            return Vec::new();
        };
        if !self.fanin_allowed(p, &cat) {
            return Vec::new();
        }
        let Ok(chain) = inherited::chain(&self.svc, &cat).await else {
            return Vec::new();
        };
        let own = inherited::state_of(&cat);
        own.defs
            .values()
            .filter(|d| d.kind == DefKind::Stream)
            .cloned()
            .chain(chain.reaching(DefKind::Stream))
            .filter(|d| self.may_select(p, &cat, &d.db_name, &d.name))
            .collect()
    }

    /// Member `t` of `def` below `p`'s tenant that fan-in may read: its
    /// name, or `None` (not below, residency, no directory entry).
    async fn fanin_member(&self, p: &Principal, t: sprig_common::ids::TenantId) -> Option<String> {
        if t.0 == p.tenant_id {
            return None;
        }
        let d = inherited::dir_entry(&self.svc, t).await.ok()??;
        if !d.path_ids().contains(&p.tenant_id) {
            return None;
        }
        if sprig_auth::TenantResidency::of(&d.residency).is_some_and(|r| r.deny_remote_access) {
            metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
                .increment(1);
            return None;
        }
        Some(d.name)
    }

    /// Every fan-in topic name `p` may read (Metadata), sorted.
    pub(crate) async fn fanin_names(&self, p: &Principal) -> Vec<String> {
        let mut out = Vec::new();
        for d in self.fanin_defs(p).await {
            let Ok(list) = inherited::instances_of(&self.svc, d.key()).await else {
                continue;
            };
            for e in list {
                if e.kind != DefKind::Stream {
                    continue;
                }
                if let Some(name) = self.fanin_member(p, e.tenant).await {
                    out.push(fanin_name(&name, &d.db_name, &d.name));
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// A fan-in topic by name (see above). Read only.
    ///
    /// # Errors
    /// Unknown for a name `p` can't see; 1227 without `SPRIG_FAN_IN`;
    /// `TOPIC_AUTHORIZATION_FAILED` for a write.
    pub(super) async fn fanin_topic(
        &self,
        p: &Principal,
        name: &str,
        write: bool,
    ) -> Result<DataTopic, TopicError> {
        let (tname, db, stream) = parse_fanin_name(name).ok_or_else(|| unknown(name))?;
        let cat = self.catalog(p).await?;
        if !self.fanin_allowed(p, &cat) {
            return Err(fanin_denied(name));
        }
        if write {
            return Err(TopicError::new(
                err::TOPIC_AUTHORIZATION_FAILED,
                format!("39897 ER_SPRIG_INHERIT_NOT_OVERRIDABLE: fan-in topic {name} is read only"),
            ));
        }
        let def = self
            .fanin_defs(p)
            .await
            .into_iter()
            .find(|d| d.db_name == db && d.name == stream)
            .ok_or_else(|| unknown(name))?;
        let t = inherited::tenant_by_name(&self.svc, tname)
            .await
            .map_err(|e| kafka_err(&e))?
            .ok_or_else(|| unknown(name))?;
        if self.fanin_member(p, t).await.is_none() {
            return Err(unknown(name));
        }
        let tcat = inherited::catalog(&self.svc, t)
            .await
            .map_err(|e| kafka_err(&e))?;
        let inst = inherited::state_of(&tcat)
            .instances
            .get(&def.key())
            .filter(|i| i.state == sprig_inherit::msg::InstanceState::Active)
            .cloned()
            .ok_or_else(|| unknown(name))?;
        let s = tcat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            .and_then(|st| st.all().find(|s| s.id.0 == inst.instance_id).cloned())
            .ok_or_else(|| unknown(name))?;
        let id = fanin_id(&self.svc, p.tenant_id, t.0, s.id.0)
            .await
            .map_err(|e| kafka_err(&e))?;
        let max = self.svc.settings.get(
            &sprig_streams::settings::MAX_MESSAGE_BYTES,
            &tcat.setting_ctx(&self.svc.dc),
            Some(&tcat.settings),
        );
        Ok(DataTopic {
            name: name.to_owned(),
            stream_id: id,
            topic_id: Uuid::from_u64_pair(p.tenant_id, id),
            partitions: i32::try_from(s.partitions).unwrap_or(i32::MAX),
            kind: TopicKind::Events,
            compact: s.cleanup == sprig_streams::Cleanup::Compact,
            max_message_bytes: usize::try_from(max).unwrap_or(usize::MAX),
        })
    }

    /// The fan-in topic whose stream id is `id`, by id (Fetch v13+).
    ///
    /// # Errors
    /// As [`Self::fanin_topic`].
    pub(super) async fn fanin_topic_by_id(
        &self,
        p: &Principal,
        id: u64,
        write: bool,
    ) -> Result<DataTopic, TopicError> {
        let no = || unknown("");
        let (member, stream) = fanin_parts(&self.svc, p.tenant_id, id)
            .await
            .map_err(|e| kafka_err(&e))?
            .ok_or_else(no)?;
        let t = sprig_common::ids::TenantId(member);
        let tname = self.fanin_member(p, t).await.ok_or_else(no)?;
        let tcat = inherited::catalog(&self.svc, t)
            .await
            .map_err(|e| kafka_err(&e))?;
        let s = tcat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            .and_then(|st| st.all().find(|s| s.id.0 == stream).cloned())
            .ok_or_else(no)?;
        let db = tcat.databases.get(s.db).ok_or_else(no)?;
        let name = fanin_name(&tname, &db.name, &s.name);
        self.fanin_topic(p, &name, write).await
    }

    /// The member's catalog and stream of a fan-in topic, for reads.
    pub(super) async fn resolve_fanin(
        &self,
        p: &Principal,
        t: &DataTopic,
    ) -> Result<super::Resolved, TopicError> {
        let (member, stream) = fanin_parts(&self.svc, p.tenant_id, t.stream_id)
            .await
            .map_err(|e| kafka_err(&e))?
            .ok_or_else(|| unknown(&t.name))?;
        let cat = inherited::catalog(&self.svc, sprig_common::ids::TenantId(member))
            .await
            .map_err(|e| kafka_err(&e))?;
        let def = cat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            .and_then(|st| {
                st.all()
                    .find(|d| d.id.0 == stream && d.state == sprig_streams::StreamState::Active)
                    .cloned()
            })
            .ok_or_else(|| unknown(&t.name))?;
        Ok(super::Resolved {
            cat,
            def,
            topic: t.clone(),
        })
    }
}

#[cfg(test)]
mod fanin_tests {
    use super::*;

    #[test]
    fn fan_in_ids_and_names() {
        // Any 64-bit member id: the id is a lookup, not a packing.
        remember(26_306_000_001, FANIN_FLAG | 3, (1 << 40) + 7, 1 << 30);
        assert_eq!(
            fanin_cached(26_306_000_001, FANIN_FLAG | 3),
            Some(((1 << 40) + 7, 1 << 30))
        );
        assert_eq!(fanin_cached(26_306_000_002, FANIN_FLAG | 3), None);
        assert!(is_fanin(FANIN_FLAG | 3));
        assert!(!is_fanin(41));
        assert!(!is_fanin(DEF_FLAG | FANIN_FLAG | 0x29), "a definition's topic");
        assert_eq!(fanin_cached(26_306_000_001, 3), None);
        let n = fanin_name("client_x", "security", "user_events");
        assert_eq!(n, "__t.client_x.security.user_events");
        assert_eq!(
            parse_fanin_name(&n),
            Some(("client_x", "security", "user_events"))
        );
        assert_eq!(parse_fanin_name("security.user_events"), None);
        assert_eq!(parse_fanin_name("__t.x.y"), None);
    }
}
