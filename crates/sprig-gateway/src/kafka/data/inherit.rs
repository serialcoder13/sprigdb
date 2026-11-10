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
        out.sort();
        out.dedup();
        out
    }
}
