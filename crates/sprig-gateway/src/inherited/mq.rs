//! Inherited exchanges, queues and bindings (chapter 50, "Inherited
//! exchanges, queues and bindings"; chapter 42):
//!
//! ```text
//! ensure_vhost(T, database db):        on T's first use of the vhost (an AMQP
//!                                      connection opening it, a SQL mq_* call,
//!                                      a publish an ancestor targets at T)
//!   the exchange, queue and binding definitions of db reaching T
//!   T's database db, created empty if T has none (r9fet decision 2)
//!   each exchange or queue without an instance row: an instance row (id from
//!     T's object counter, 'active'); then, in one versioned catalog change of
//!     T, the ordinary mq_exchanges / mq_queues / mq_bindings rows: the
//!     instance IS an ordinary exchange or queue of T (r9fet decision 1, as for
//!     streams), so the queue engine, AMQP, SQL and triggers work unchanged
//!   a binding disabled for T (ALTER INHERITED BINDING ... DISABLE, or an AMQP
//!     queue.unbind of it in T) is left out, or removed
//!   queue arguments: the definition's, overlaid by the nearest overrides
//!   index entries; T's inherited triggers on the queues
//! publish in an ancestor P (AMQP, SQL):
//!   exchange e (or the default exchange and queue q) named by a definition,
//!   target from the header x-sprig-tenant or the routing key @t:key
//!   (prefix removed): t must be a member P is a proper ancestor of (39896);
//!   ensure_vhost(t), then publish in t's instance with x-sprig-tenant = t,
//!   x-sprig-origin-tenant = P; inside a SQL transaction through P's
//!   __sprig_xo (Dest::Exchange), forwarded into t's __sprig_qo (its out id
//!   is the message id there: a second forward enqueues nothing new)
//! publish in a member T into its own instance: the two headers stamped T, T
//! ```

#![allow(clippy::single_match_else)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, shard_schemas};
use sprig_catalog::version::VersionKind;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::model::{QUEUE_OVERRIDABLE, db_key};
use sprig_inherit::msg::store::MsgStore;
use sprig_inherit::msg::{
    BindingSpec, Cause, Chain, DefKey, DefKind, DefState, Definition, ExchangeSpec, Instance,
    InstanceState, QueueSpec, What,
};
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::backend::{
    BindingSpec as AmqpBinding, ConnKey, ConsumeSpec, ConsumerId, DestKind, ExchangeSpec as AmqpEx,
    OpCtx, Published, QueueDeclared, QueueSpec as AmqpQueue, Settlement, TxBatch, Vhost,
};
use sprig_proto_amqp::error::MqError;
use sprig_proto_amqp::message::{Delivery, DeliveryId, Message, MsgFormat};
use sprig_proto_amqp::v091::codec::FieldValue;
use sprig_queues::catalog::{MqState, MqStore, json_table, table_json};
use sprig_queues::engine::TopoChange;
use sprig_queues::topology::{BindingDef, ExKind, ExchangeDef, QType, QueueDef};

use super::{InstanceIndex, catalog, target_err};
use crate::tenant::TenantServices;

/// The header naming the target tenant (and, stamped by the server, the
/// instance's tenant).
pub const TENANT_HEADER: &str = "x-sprig-tenant";
/// The header naming the publishing tenant (stamped by the server).
pub const ORIGIN_HEADER: &str = "x-sprig-origin-tenant";

fn cat_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

/// The exchange, queue and binding definitions of database `db` reaching
/// the chain's member (active ones).
#[must_use]
pub fn defs_in(chain: &Chain, db: &str) -> Vec<Arc<Definition>> {
    let k = db_key(db);
    [DefKind::Exchange, DefKind::Queue, DefKind::Binding]
        .into_iter()
        .flat_map(|kind| chain.reaching(kind))
        .filter(|d| db_key(&d.db_name) == k && d.state == DefState::Active)
        .collect()
}

/// Every exchange and queue definition of database `db` declared at some
/// level of the chain (the member's own included): the names a publish in
/// the member may target at a descendant.
#[must_use]
pub fn declared_in(chain: &Chain, db: &str) -> Vec<Arc<Definition>> {
    let k = db_key(db);
    chain
        .levels
        .iter()
        .filter_map(|l| l.state.as_ref())
        .flat_map(|s| s.defs.values())
        .filter(|d| {
            matches!(d.kind, DefKind::Exchange | DefKind::Queue)
                && db_key(&d.db_name) == k
                && d.state == DefState::Active
        })
        .cloned()
        .collect()
}

/// A queue instance's arguments: the definition's, with each overridable
/// argument's nearest override (chapter 42, "Overrides").
#[must_use]
pub fn queue_arguments(
    chain: &Chain,
    def: &Definition,
) -> std::collections::BTreeMap<String, Value> {
    let spec: QueueSpec = serde_json::from_value(def.definition.clone()).unwrap_or_default();
    let mut args = spec.arguments;
    for k in QUEUE_OVERRIDABLE {
        if let Some((v, _)) = chain.effective(def.key(), What::Settings, Some(k)) {
            if v.is_null() {
                args.remove(*k);
            } else {
                args.insert((*k).to_owned(), v);
            }
        }
    }
    args
}

fn json_args(m: &std::collections::BTreeMap<String, Value>) -> Value {
    Value::Object(m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// What a vhost's derived state depends on (see the streams module's).
fn signature(chain: &Chain, defs: &[Arc<Definition>]) -> String {
    use std::fmt::Write as _;
    let keys: BTreeSet<DefKey> = defs.iter().map(|d| d.key()).collect();
    let mut s = String::new();
    for d in defs {
        let _ = write!(s, "{}:{}|", d.key(), d.def_version);
    }
    for l in &chain.levels {
        let Some(st) = &l.state else { continue };
        for o in st.overrides.iter().filter(|o| keys.contains(&o.def)) {
            let _ = write!(s, "o{}:{}:{}|", o.def, o.set_at_ms, o.value);
        }
        for g in st.defs.values().filter(|d| d.kind == DefKind::Trigger) {
            let _ = write!(s, "t{}:{}:{}|", g.key(), g.def_version, g.state.as_str());
        }
        for o in &st.overrides {
            if st
                .defs
                .values()
                .any(|g| g.kind == DefKind::Trigger && g.key() == o.def)
            {
                let _ = write!(s, "g{}:{}|", o.def, o.set_at_ms);
            }
        }
    }
    s
}

type Applied = HashMap<(TenantId, String), String>;

fn applied_map() -> &'static Mutex<Applied> {
    static M: OnceLock<Mutex<Applied>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Forgets what this process applied for `t`'s vhost database `db` (all
/// of `t`'s when `db` is `None`), so the next use applies it again.
pub fn forget_applied(t: TenantId, db: Option<&str>) {
    let mut m = applied_map().lock().unwrap_or_else(PoisonError::into_inner);
    match db {
        Some(d) => {
            m.remove(&(t, db_key(d)));
        }
        None => m.retain(|(x, _), _| *x != t),
    }
}

fn vhost_lock(t: TenantId, db: &str) -> Arc<tokio::sync::Mutex<()>> {
    type Locks = HashMap<(TenantId, String), Arc<tokio::sync::Mutex<()>>>;
    static M: OnceLock<Mutex<Locks>> = OnceLock::new();
    let mut m = M
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(m.entry((t, db_key(db))).or_default())
}

/// A member's instances of the exchange, queue and binding definitions of
/// database `db`, made if needed (see the module docs). `false` when no
/// definition of `db` reaches the member.
///
/// # Errors
/// 39240, 39899, the catalog's.
#[allow(clippy::too_many_lines)]
pub async fn ensure_vhost(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    db: &str,
    cause: Cause,
) -> Result<bool> {
    let defs = defs_in(chain, db);
    if defs.is_empty() {
        return Ok(false);
    }
    let t = chain.member();
    let sig = signature(chain, &defs);
    let done = |s: &str| {
        applied_map()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(t, db_key(db)))
            .is_some_and(|x| x == s)
    };
    if done(&sig) {
        return Ok(true);
    }
    let lock = vhost_lock(t, db);
    let _g = lock.lock().await;
    if done(&sig) {
        return Ok(true);
    }
    let db_name = defs[0].db_name.clone();
    let started = std::time::Instant::now();
    let ep = svc.locator.writer(t).await?;
    // R9 (r9scale): the member's database and the catalog's `inherit_msg`
    // and `mq` modules at once, in one migration run (they were three
    // steps one after another on a member's first publish).
    let modules = async {
        let mut c = ep.pool.get_conn().await.map_err(cat_err)?;
        sprig_catalog::Migrator::new(&[
            sprig_inherit::msg::store::MSG_MODULE,
            sprig_queues::catalog::MQ_MODULE,
        ])?
        .run(
            &mut c,
            ep.store.schema(),
            &sprig_catalog::migrate::MigrateOpts::default(),
        )
        .await?;
        Ok::<_, SprigError>(())
    };
    let (cat, ()) = tokio::try_join!(
        super::streams::ensure_database(svc, t, &db_name),
        modules
    )?;
    let prepared_ms = started.elapsed().as_millis();
    let lc = super::streams::lctn(svc, &cat);
    let row = cat
        .database(&database_key(&db_name, lc))
        .cloned()
        .ok_or_else(|| super::unavailable(format!("tenant {t}'s database {db_name}")))?;
    // The queue store's shard tables and the forwarder's subscription,
    // which the queue owner's first load would otherwise make after the
    // catalog change, while the instances are claimed and recorded.
    let store_warm = defs
        .iter()
        .any(|d| d.kind == DefKind::Queue)
        .then(|| tokio::spawn(warm_store(Arc::clone(svc), t, Arc::clone(&row))));
    let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
    let mstore = MsgStore::new(ep.store.schema());
    let st = super::state_of(&cat);
    let mq = cat
        .ext
        .get::<MqState>(sprig_queues::catalog::MODULE)
        .cloned()
        .unwrap_or_default();
    let topo = mq.topology(row.db_id.0, cat.version);
    let max = svc.settings.get(
        &sprig_inherit::msg::settings::MAX_INSTANCES_PER_TENANT,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    );
    let mut have = st.instances.len() as u64;
    let mut changes: Vec<(TopoChange, u64)> = Vec::new();
    let mut instances: Vec<(Arc<Definition>, u64)> = Vec::new();
    let mut arg_updates: Vec<(u64, Value)> = Vec::new();
    let mut fresh: Vec<Arc<Definition>> = Vec::new();
    for d in defs
        .iter()
        .filter(|d| matches!(d.kind, DefKind::Exchange | DefKind::Queue))
    {
        let key = d.key();
        let row_now = match st.instances.get(&key) {
            Some(i) => Some((**i).clone()),
            None => mstore.instance(&mut conn, key).await?,
        };
        let present = match d.kind {
            DefKind::Exchange => topo.exchanges.get(&d.name).map(|x| x.id),
            _ => topo.queues.get(&d.name).map(|q| q.id),
        };
        if let (Some(id), None) = (present, &row_now) {
            // The member's own object of that name shadows the definition
            // for it (chapter 42, "Failure handling").
            tracing::warn!(tenant = %t, def = %key, name = %d.name, object = id, "a member's own object shadows an inherited one");
            continue;
        }
        if let Some(i) = &row_now
            && i.state != InstanceState::Active
            && i.state != InstanceState::Creating
        {
            continue;
        }
        let id = if let Some(i) = &row_now {
            i.instance_id
        } else {
            if have >= max {
                return Err(SprigError::new(
                    SprigCode::InheritInstanceQuota,
                    format!(
                        "ER_SPRIG_INHERIT_INSTANCE_QUOTA: tenant {t} holds {have} inherited \
                         instances (inherit.max_instances_per_tenant = {max})"
                    ),
                ));
            }
            let id = ep.store.next_object_id(&mut conn).await?.0;
            let claimed = mstore
                .claim(
                    &mut conn,
                    &Instance {
                        def: key,
                        instance_id: id,
                        kind: d.kind,
                        def_version: d.def_version,
                        definition: d.definition.clone(),
                        state: InstanceState::Active,
                        cause,
                        physical: Some(json!({"db": d.db_name, "name": d.name})),
                        created_at_ms: 0,
                    },
                )
                .await?;
            have += 1;
            if claimed {
                fresh.push(Arc::clone(d));
                metrics::counter!("sprig_inherit_instance_creations_total", "kind" => d.kind.as_str(), "cause" => cause.as_str())
                    .increment(1);
                id
            } else {
                // Another gateway claimed it first: its id.
                mstore
                    .instance(&mut conn, key)
                    .await?
                    .map_or(id, |i| i.instance_id)
            }
        };
        instances.push((Arc::clone(d), id));
        match d.kind {
            DefKind::Exchange => {
                if present.is_none() {
                    let spec: ExchangeSpec =
                        serde_json::from_value(d.definition.clone()).unwrap_or_default();
                    let kind = ExKind::parse(&spec.kind).unwrap_or(ExKind::Direct);
                    changes.push((
                        TopoChange::AddExchange(ExchangeDef {
                            id,
                            name: d.name.clone(),
                            kind,
                            durable: true,
                            auto_delete: false,
                            internal: spec.internal,
                            arguments: json_table(&json_args(&spec.arguments)),
                            builtin: false,
                        }),
                        id,
                    ));
                }
            }
            _ => {
                let args = json_args(&queue_arguments(chain, d));
                match topo.queues.get(&d.name) {
                    None => {
                        let table = json_table(&args);
                        let qtype = table
                            .get("x-queue-type")
                            .and_then(|v| v.as_str().and_then(|s| QType::parse(&s)))
                            .unwrap_or(QType::Classic);
                        let lanes = table
                            .get("x-sprig-lanes")
                            .and_then(FieldValue::as_i64)
                            .and_then(|n| u16::try_from(n.clamp(1, 1024)).ok())
                            .unwrap_or(1);
                        changes.push((
                            TopoChange::AddQueue(QueueDef {
                                id,
                                name: d.name.clone(),
                                qtype,
                                durable: true,
                                exclusive: None,
                                auto_delete: false,
                                arguments: table,
                                lanes,
                                temp_on: None,
                            }),
                            id,
                        ));
                    }
                    Some(q) if table_json(&q.arguments) != args => {
                        arg_updates.push((q.id, args));
                    }
                    Some(_) => {}
                }
            }
        }
    }
    let exists = |kind: &str, name: &str| -> bool {
        let added = changes.iter().any(|(c, _)| match c {
            TopoChange::AddExchange(x) => kind == "exchange" && x.name == name,
            TopoChange::AddQueue(q) => kind == "queue" && q.name == name,
            _ => false,
        });
        added
            || match kind {
                "exchange" => topo.exchanges.contains_key(name),
                _ => topo.queues.contains_key(name),
            }
    };
    let mut binds = Vec::new();
    for d in defs.iter().filter(|d| d.kind == DefKind::Binding) {
        let spec: BindingSpec = serde_json::from_value(d.definition.clone()).unwrap_or_default();
        let b = BindingDef {
            source: spec.source.clone(),
            dest: spec.dest.clone(),
            dest_kind: if spec.dest_kind == "exchange" {
                DestKind::Exchange
            } else {
                DestKind::Queue
            },
            key: spec.key.clone(),
            arguments: json_table(&json_args(&spec.arguments)),
        };
        let present = topo.bindings.iter().any(|x| x.same(&b));
        if chain.disabled(d.key()) {
            if present {
                binds.push(TopoChange::Unbind(b));
            }
        } else if !present
            && exists("exchange", &spec.source)
            && exists(&spec.dest_kind, &spec.dest)
        {
            binds.push(TopoChange::Bind(b));
        }
    }
    let mut all: Vec<(TopoChange, u64)> = changes;
    all.extend(binds.into_iter().map(|b| (b, 0)));
    // The catalog writer connection goes back before the catalog change:
    // anything it still holds (a migration's lock) would make the change's
    // begin wait for it.
    drop(conn);
    if !all.is_empty() || !arg_updates.is_empty() {
        let mut c = svc.catalog.change(t, VersionKind::Catalog).await?;
        let r = {
            let (store, tx) = c.tx().parts();
            let ms = MqStore::new(store.schema());
            let schema = store.schema().to_owned();
            async {
                for (ch, id) in &all {
                    ms.apply(tx, row.db_id.0, ch, *id).await?;
                }
                for (id, args) in &arg_updates {
                    tx.exec_drop(
                        format!(
                            "UPDATE {}.mq_queues SET arguments = ? WHERE object_id = ?",
                            quote(&schema)
                        ),
                        (args.to_string(), id),
                    )
                    .await
                    .map_err(cat_err)?;
                }
                Ok::<_, SprigError>(())
            }
            .await
        };
        if let Err(e) = r {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        let _ = svc
            .cache
            .get(
                t,
                sprig_catalog::cache::Freshness::Cached { min_version: v },
            )
            .await?;
        tracing::info!(tenant = %t, db = %db_name, changes = all.len(), arguments = arg_updates.len(), ms = started.elapsed().as_millis(), prepared_ms, "inherited exchanges, queues and bindings applied");
    }
    for (d, id) in &instances {
        super::put_index(
            svc,
            &InstanceIndex {
                tenant: t,
                def: d.key(),
                kind: d.kind,
                instance_id: *id,
                db: d.db_name.clone(),
                name: d.name.clone(),
            },
        )
        .await?;
        if d.kind == DefKind::Queue {
            super::triggers::ensure_for_queue(svc, chain, d, (row.db_id.0, &row.name), *id).await?;
            // R9 (r9fanin): its FOR EACH TENANT sinks, as for streams.
            super::sinks::ensure_for_queue(svc, chain, d, t).await;
        }
    }
    if let Some(h) = store_warm {
        let _ = h.await;
    }
    applied_map()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert((t, db_key(db)), sig);
    // Chapter 42: inherit.broadcast_replay, for instances made just now.
    for d in fresh {
        tokio::spawn(super::broadcast::replay(Arc::clone(svc), d, t));
    }
    Ok(true)
}

/// Makes the queue store's tables in the shard schema of database `row`
/// of tenant `t` and registers the tenant's forwarder subscription
/// (both idempotent; the queue owner's first load does them again and
/// finds them done). Failures are left to that load.
async fn warm_store(
    svc: Arc<TenantServices>,
    t: TenantId,
    row: Arc<sprig_catalog::store::DatabaseRow>,
) {
    let Some(schema) = shard_schemas(t, &row).first().map(ToString::to_string) else {
        return;
    };
    let tables = async {
        let cluster = svc.locator.cluster(t).await?;
        let pool = svc.locator.ctl_pool(&cluster).await?;
        sprig_queues::maria::MariaStore::new(pool, schema)
            .ensure()
            .await
            .map_err(|e| super::unavailable(e.to_string()))
    };
    let (r, ()) = tokio::join!(tables, crate::outbox_feed::subscribe(&svc, t));
    if let Err(e) = r {
        tracing::debug!(tenant = %t, error = %e, "queue store tables not made ahead; the owner makes them");
    }
}

/// Removes a member's instance of exchange or queue definition `def` now
/// (`DROP ... IMMEDIATE`): the exchange or queue with its bindings, its
/// row, its index entry.
///
/// # Errors
/// The catalog's.
pub async fn remove(svc: &Arc<TenantServices>, t: TenantId, def: &Definition) -> Result<()> {
    let cat = catalog(svc, t).await?;
    let lc = super::streams::lctn(svc, &cat);
    if let Some(row) = cat.database(&database_key(&def.db_name, lc)) {
        let mq = cat
            .ext
            .get::<MqState>(sprig_queues::catalog::MODULE)
            .cloned()
            .unwrap_or_default();
        let topo = mq.topology(row.db_id.0, cat.version);
        let st = super::state_of(&cat);
        let mine = st.instances.get(&def.key()).map(|i| i.instance_id);
        let ch = match def.kind {
            DefKind::Exchange => topo
                .exchanges
                .get(&def.name)
                .filter(|x| Some(x.id) == mine)
                .map(|_| TopoChange::DelExchange(def.name.clone())),
            _ => topo
                .queues
                .get(&def.name)
                .filter(|q| Some(q.id) == mine)
                .map(|_| TopoChange::DelQueue(def.name.clone())),
        };
        if let Some(ch) = ch {
            let mut c = svc.catalog.change(t, VersionKind::Catalog).await?;
            let r = {
                let (store, tx) = c.tx().parts();
                MqStore::new(store.schema())
                    .apply(tx, row.db_id.0, &ch, 0)
                    .await
            };
            if let Err(e) = r {
                let _ = c.rollback().await;
                return Err(e);
            }
            c.commit().await?;
        }
    }
    let ep = svc.locator.writer(t).await?;
    let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
    MsgStore::new(ep.store.schema())
        .delete_instance(&mut conn, def.key())
        .await?;
    super::delete_index(svc, t, def.key()).await?;
    if def.kind == DefKind::Queue {
        super::fanin_mq::forget_ready(svc, def.key(), t).await;
    }
    forget_applied(t, Some(&def.db_name));
    Ok(())
}

/// Removes binding `b` from member `t`'s vhost database `db` (its
/// definition was dropped).
///
/// # Errors
/// The catalog's.
pub async fn remove_binding(
    svc: &Arc<TenantServices>,
    t: TenantId,
    db: &str,
    b: &BindingSpec,
) -> Result<()> {
    let cat = catalog(svc, t).await?;
    let lc = super::streams::lctn(svc, &cat);
    let Some(row) = cat.database(&database_key(db, lc)) else {
        return Ok(());
    };
    let def = BindingDef {
        source: b.source.clone(),
        dest: b.dest.clone(),
        dest_kind: if b.dest_kind == "exchange" {
            DestKind::Exchange
        } else {
            DestKind::Queue
        },
        key: b.key.clone(),
        arguments: json_table(&json_args(&b.arguments)),
    };
    let mut c = svc.catalog.change(t, VersionKind::Catalog).await?;
    let r = {
        let (store, tx) = c.tx().parts();
        MqStore::new(store.schema())
            .apply(tx, row.db_id.0, &TopoChange::Unbind(def), 0)
            .await
    };
    if let Err(e) = r {
        let _ = c.rollback().await;
        return Err(e);
    }
    c.commit().await?;
    Ok(())
}

// ----------------------------------------------------------------------
// Targets and headers.
// ----------------------------------------------------------------------

/// The target a publish names: the header `x-sprig-tenant`, else a
/// routing key `@t:key`; with the routing key the client meant (prefix
/// removed). `None` when it names none.
#[must_use]
pub fn target_of(m: &Message) -> Option<(String, String)> {
    if let Some(v) = m.info.headers.get(TENANT_HEADER)
        && let Some(s) = v.as_str()
        && !s.is_empty()
    {
        let key = strip_key(&m.routing_key).map_or_else(|| m.routing_key.clone(), |(_, k)| k);
        return Some((s.into_owned(), key));
    }
    strip_key(&m.routing_key)
}

/// `@t:key` → `(t, key)`.
#[must_use]
pub fn strip_key(key: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix('@')?;
    let (t, k) = rest.split_once(':')?;
    (!t.is_empty()).then(|| (t.to_owned(), k.to_owned()))
}

/// The message with routing key `key` and the two tenant headers set to
/// `tenant` and `origin` (over anything the client sent).
///
/// # Errors
/// Properties that don't parse.
pub fn stamp(
    m: &Message,
    key: &str,
    tenant: &str,
    origin: &str,
) -> std::result::Result<Message, MqError> {
    use sprig_proto_amqp::v10::message::{
        Sections, application_properties_section, encode_sections,
    };
    use sprig_proto_amqp::v10::types::Value as V10;
    use sprig_proto_amqp::v091::props::BasicProperties;
    let mut out = m.clone();
    key.clone_into(&mut out.routing_key);
    match m.format {
        MsgFormat::Amqp091 => {
            let mut p = BasicProperties::decode(&m.props)?;
            let mut h = p.headers.take().unwrap_or_default();
            h.insert(TENANT_HEADER.to_owned(), FieldValue::str(tenant));
            h.insert(ORIGIN_HEADER.to_owned(), FieldValue::str(origin));
            p.headers = Some(h);
            out.props = p.to_bytes();
            out.info.size = (out.props.len() + out.body.len()) as u64;
        }
        MsgFormat::Amqp10 => {
            let s = Sections::parse(&m.body).map_err(|e| MqError::precondition(e.0))?;
            let mut app: Vec<(String, V10)> = s
                .application_properties
                .iter()
                .filter(|(k, _)| k != TENANT_HEADER && k != ORIGIN_HEADER)
                .cloned()
                .collect();
            app.push((TENANT_HEADER.to_owned(), V10::String(tenant.to_owned())));
            app.push((ORIGIN_HEADER.to_owned(), V10::String(origin.to_owned())));
            let mut b =
                encode_sections(&[s.properties.to_value(), application_properties_section(app)])
                    .to_vec();
            b.extend_from_slice(&s.body_raw);
            out.body = Bytes::from(b);
            out.info.size = (out.props.len() + out.body.len()) as u64;
        }
    }
    out.info
        .headers
        .insert(TENANT_HEADER.to_owned(), FieldValue::str(tenant));
    out.info
        .headers
        .insert(ORIGIN_HEADER.to_owned(), FieldValue::str(origin));
    Ok(out)
}

fn mq_err(e: &SprigError) -> MqError {
    match e.code() {
        Some(SprigCode::InheritInstanceUnavailable | SprigCode::CatalogUnavailable) => {
            MqError::new(
                sprig_proto_amqp::error::MqCode::OwnerMoved,
                e.message.clone(),
            )
        }
        _ => MqError::access_refused(e.message.clone()),
    }
}

/// An internal principal of tenant `t` (no user: the engine checks no
/// privileges for it).
#[must_use]
pub fn internal_principal(t: TenantId, name: &str) -> Principal {
    Principal {
        tenant_id: t.0,
        tenant: name.to_owned(),
        user: super::RUN_AS_USER.to_owned(),
        user_id: None,
        handle: None,
    }
}

/// Resolves and checks the target of a publish from `publisher` (whose
/// chain is `pchain`) into the exchange or queue named `name` of `db`:
/// the target's id, name and chain.
///
/// # Errors
/// 39896 for a tenant that isn't a member below the publisher.
pub async fn resolve_target(
    svc: &Arc<TenantServices>,
    publisher: TenantId,
    target: &str,
    db: &str,
    kind: DefKind,
    name: &str,
) -> Result<(TenantId, String, Chain)> {
    let tid = super::tenant_by_name(svc, target)
        .await?
        .or_else(|| target.parse::<u64>().ok().map(TenantId))
        .ok_or_else(|| target_err(format!("no tenant '{target}' below this one")))?;
    let tcat = catalog(svc, tid)
        .await
        .map_err(|_| target_err(format!("no tenant '{target}' below this one")))?;
    let tchain = super::chain(svc, &tcat).await?;
    if tid != publisher && !tchain.is_proper_ancestor(publisher) {
        return Err(target_err(format!(
            "tenant '{target}' isn't below this one (publishing goes down the tree only)"
        )));
    }
    let d = tchain.resolve(db, kind, name).ok_or_else(|| {
        target_err(format!(
            "tenant '{target}' isn't a member of {} {db}.{name}",
            kind.as_str()
        ))
    })?;
    if d.state != DefState::Active {
        return Err(super::dropping(format!(
            "{} {db}.{name} is being dropped",
            kind.as_str()
        )));
    }
    let tname = super::tenant_name(svc, tid).await?;
    Ok((tid, tname, tchain))
}

/// Which definition a publish to `exchange` with `key` names in `chain`'s
/// database `db`: an exchange definition of that name, or (the default
/// exchange) a queue definition named by the key.
#[must_use]
pub fn named_def(chain: &Chain, db: &str, exchange: &str, key: &str) -> Option<(DefKind, String)> {
    let defs = declared_in(chain, db);
    if exchange.is_empty() {
        let k = strip_key(key).map_or_else(|| key.to_owned(), |(_, k)| k);
        return defs
            .iter()
            .find(|d| d.kind == DefKind::Queue && d.name == k)
            .map(|d| (DefKind::Queue, d.name.clone()));
    }
    defs.iter()
        .find(|d| d.kind == DefKind::Exchange && d.name == exchange)
        .map(|d| (DefKind::Exchange, d.name.clone()))
}

/// Publishes `m` (already stamped, routing key without its prefix) into
/// member `t`'s instance now, after making it: the queues reached, once
/// they committed.
///
/// # Errors
/// The engine's (as SQL errors) and the catalog's.
pub async fn publish_into(
    svc: &Arc<TenantServices>,
    tchain: &Chain,
    db: &str,
    m: Message,
    mandatory: bool,
) -> Result<u32> {
    let t = tchain.member();
    Box::pin(ensure_vhost(svc, tchain, db, Cause::Publish)).await?;
    let engine = svc
        .mq
        .get()
        .cloned()
        .ok_or_else(|| super::unavailable("the queue engine isn't running on this gateway"))?;
    let name = super::tenant_name(svc, t).await?;
    let v = engine
        .sql_vhost(Arc::new(internal_principal(t, &name)), db, 0)
        .await
        .map_err(|e| sql_mq(&e))?;
    let n = v.publish_now(m, mandatory).await.map_err(|e| sql_mq(&e))?;
    metrics::counter!("sprig_inherit_publishes_total", "kind" => "exchange", "path" => "direct")
        .increment(1);
    Ok(n)
}

fn sql_mq(e: &MqError) -> SprigError {
    let state = if e.code == sprig_proto_amqp::error::MqCode::OwnerMoved {
        *b"40001"
    } else {
        *b"HY000"
    };
    SprigError::standard(
        u16::try_from(e.code.number()).unwrap_or(39250),
        state,
        format!("{}: {}", e.code.name(), e.text),
    )
}

// ----------------------------------------------------------------------
// The cross-tenant outbox's queue side.
// ----------------------------------------------------------------------

/// An outbox row as [`deliver_outbox`] takes it: id, routing key, body,
/// [`xo_headers`].
pub type OutboxRow = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>, Option<String>);

/// What `__sprig_xo` keeps of a message for an exchange: `k` is the
/// routing key, `v` the body, `headers` this.
#[must_use]
pub fn xo_headers(m: &Message) -> String {
    json!({
        "fmt": sprig_queues::msg::fmt_code(m.format),
        "props": base64::engine::general_purpose::STANDARD.encode(&m.props),
    })
    .to_string()
}

/// Writes outbox rows (`id`, routing key, body, [`xo_headers`]) into
/// member `t`'s queue outbox `__sprig_qo` for exchange `exchange` of `db`,
/// with the row's id as the out id there: the member's forwarder routes
/// them with its own bindings, and a second write of a row adds nothing.
///
/// # Errors
/// The engine's and the cluster's.
pub async fn deliver_outbox(
    svc: &Arc<TenantServices>,
    tchain: &Chain,
    db: &str,
    exchange: &str,
    rows: &[OutboxRow],
) -> Result<()> {
    let t = tchain.member();
    Box::pin(ensure_vhost(svc, tchain, db, Cause::Publish)).await?;
    let engine = svc
        .mq
        .get()
        .cloned()
        .ok_or_else(|| super::unavailable("the queue engine isn't running on this gateway"))?;
    let name = super::tenant_name(svc, t).await?;
    let v = engine
        .sql_vhost(Arc::new(internal_principal(t, &name)), db, 0)
        .await
        .map_err(|e| sql_mq(&e))?;
    v.ensure_store().await.map_err(|e| sql_mq(&e))?;
    let cat = catalog(svc, t).await?;
    let lc = super::streams::lctn(svc, &cat);
    let row = cat
        .database(&database_key(db, lc))
        .ok_or_else(|| super::unavailable(format!("tenant {t}'s database {db}")))?;
    let schema = shard_schemas(t, row)
        .first()
        .map(ToString::to_string)
        .ok_or_else(|| SprigError::internal("a database without a shard"))?;
    let values: Vec<String> = rows
        .iter()
        .map(|(id, key, body, h)| {
            let h: Value = h
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(Value::Null);
            let fmt = h.get("fmt").and_then(Value::as_u64).unwrap_or(0);
            let props = h
                .get("props")
                .and_then(Value::as_str)
                .and_then(|p| base64::engine::general_purpose::STANDARD.decode(p).ok())
                .unwrap_or_default();
            let key = key.clone().unwrap_or_default();
            let dest =
                json!({"Exchange": {"name": exchange, "key": String::from_utf8_lossy(&key)}})
                    .to_string();
            format!(
                "({}, 'publish', {}, {}, {fmt}, {}, {}, {}, {}, NULL)",
                sprig_queues::sql::hex_literal(id),
                sprig_queues::sql::hex_literal(dest.as_bytes()),
                sprig_queues::sql::hex_literal(id),
                sprig_queues::sql::hex_literal(exchange.as_bytes()),
                sprig_queues::sql::hex_literal(&key),
                sprig_queues::sql::hex_literal(&props),
                sprig_queues::sql::hex_literal(body.as_deref().unwrap_or_default()),
            )
        })
        .collect();
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    c.query_drop(format!(
        "INSERT IGNORE INTO {}.`__sprig_qo` (out_id, kind, dest, msg_id, fmt, exchange, \
         routing_key, props, body, deaths) VALUES {}",
        quote(&schema),
        values.join(", ")
    ))
    .await
    .map_err(sprig_streams::maria::map_err)?;
    v.committed();
    engine.wake_forwarder(t.0, row.db_id.0);
    Ok(())
}

// ----------------------------------------------------------------------
// The AMQP side: a vhost of a tenant with inherited objects.
// ----------------------------------------------------------------------

/// What a connection's vhost knows of the inherited objects in reach.
#[derive(Default)]
struct Names {
    /// Exchanges and queues of the member's instances (stamped on
    /// publish; deletes refused).
    instances: BTreeSet<(DefKind, String)>,
    /// Binding definitions reaching the member: (source, dest kind, dest,
    /// key) → the definition.
    bindings: HashMap<(String, String, String, String), DefKey>,
    /// The chain, for targeted publishes.
    chain: Option<Chain>,
}

/// The engine's vhost of a tenant that inherited exchanges, queues or
/// bindings, or declares them: targeted publishes, stamped headers, and
/// the inherited names protected (chapter 50, "Inherited exchanges, queues
/// and bindings").
pub struct InheritVhost {
    inner: Arc<dyn Vhost>,
    svc: Arc<TenantServices>,
    tenant: TenantId,
    tenant_name: String,
    db: String,
    vhost: String,
    names: Mutex<Names>,
    /// The end of the previous targeted publish's hand-off: targeted
    /// publishes of a connection reach their instances in publish order.
    order: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    /// The connection's events (fan-in consumers deliver into them).
    events: Option<sprig_proto_amqp::backend::EventSender>,
    /// Fan-in consumers of the connection (chapter 50, "Fan-in
    /// consumers"), ended or not: their deliveries stay settleable.
    fanins: Mutex<HashMap<ConsumerId, Arc<super::fanin_mq::FanIn>>>,
}

impl InheritVhost {
    /// Wraps `inner`, the vhost of database `db` of the chain's member;
    /// `events` are the connection's (for fan-in consumers).
    #[must_use]
    pub fn new(
        inner: Arc<dyn Vhost>,
        svc: Arc<TenantServices>,
        chain: Chain,
        tenant_name: String,
        db: String,
        events: Option<sprig_proto_amqp::backend::EventSender>,
    ) -> Self {
        let mut names = Names::default();
        for d in defs_in(&chain, &db) {
            match d.kind {
                DefKind::Binding => {
                    if let Ok(s) = serde_json::from_value::<BindingSpec>(d.definition.clone()) {
                        names
                            .bindings
                            .insert((s.source, s.dest_kind, s.dest, s.key), d.key());
                    }
                }
                k => {
                    names.instances.insert((k, d.name.clone()));
                }
            }
        }
        let vhost = inner.name().to_owned();
        let tenant = chain.member();
        names.chain = Some(chain);
        Self {
            inner,
            svc,
            tenant,
            tenant_name,
            db,
            vhost,
            names: Mutex::new(names),
            order: Mutex::new(None),
            events,
            fanins: Mutex::new(HashMap::new()),
        }
    }

    fn fanin(&self, c: ConsumerId) -> Option<Arc<super::fanin_mq::FanIn>> {
        self.fanins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&c)
            .cloned()
    }

    /// `basic.consume` with `x-sprig-fan-in` on a queue definition's name
    /// (chapter 50, "Fan-in consumers"); `None` when the consume isn't
    /// one.
    async fn fan_in_consume(
        &self,
        cx: &OpCtx,
        spec: &ConsumeSpec,
    ) -> Option<std::result::Result<(), MqError>> {
        use super::fanin_mq as f;
        let def = {
            let n = self.names();
            n.chain
                .as_ref()
                .and_then(|c| f::queue_definition(c, &self.db, &spec.queue))
        };
        let wants = f::wants_fan_in(&spec.arguments);
        let Some(def) = def else {
            return wants.then(|| {
                Err(MqError::precondition(format!(
                    "x-sprig-fan-in: '{}' isn't a FOR EACH TENANT queue in reach",
                    spec.queue
                )))
            });
        };
        if !wants {
            // Consuming a definition's name without the argument, in a
            // tenant without an instance: refused, so an application
            // can't drain other tenants' queues by mistake.
            return (!self.inherited(DefKind::Queue, &spec.queue)).then(|| {
                Err(MqError::precondition(format!(
                    "queue '{}' is a FOR EACH TENANT definition; consume it with \
                     x-sprig-fan-in = true to read every member's instance",
                    spec.queue
                )))
            });
        }
        Some(
            async {
                f::refuse_unsupported(spec)?;
                let cat = catalog(&self.svc, self.tenant)
                    .await
                    .map_err(|e| mq_err(&e))?;
                if !f::allowed(&self.svc, &cat, &cx.principal, &self.db, &spec.queue) {
                    return Err(MqError::access_refused(format!(
                        "1227 Access denied; you need (at least one of) the SPRIG_FAN_IN \
                         privilege(s), and SELECT and DELETE on '{}', for a fan-in consumer",
                        spec.queue
                    )));
                }
                let events = self
                    .events
                    .clone()
                    .ok_or_else(|| MqError::not_implemented("fan-in consumers need the engine"))?;
                let fan = f::start(
                    &self.svc,
                    self.tenant,
                    &def,
                    &self.db,
                    spec,
                    cx.channel,
                    events,
                )?;
                self.fanins
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(spec.consumer, fan);
                Ok(())
            }
            .await,
        )
    }

    fn names(&self) -> std::sync::MutexGuard<'_, Names> {
        self.names.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn inherited(&self, kind: DefKind, name: &str) -> bool {
        self.names().instances.contains(&(kind, name.to_owned()))
    }

    /// A targeted publish: resolved, the member's instance made, then
    /// published there; in this connection's publish order.
    #[allow(clippy::too_many_arguments)]
    fn targeted(
        &self,
        cx: &OpCtx,
        m: Message,
        target: String,
        key: String,
        kind: DefKind,
        name: String,
        mandatory: bool,
    ) -> Published {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let prev = self
            .order
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(rx);
        let svc = Arc::clone(&self.svc);
        let me = self.tenant;
        let origin = self.tenant_name.clone();
        let db = self.db.clone();
        let channel = cx.channel;
        let task = tokio::spawn(async move {
            if let Some(p) = prev {
                let _ = p.await;
            }
            let r = async {
                let (tid, tname, tchain) =
                    resolve_target(&svc, me, &target, &db, kind, &name).await?;
                let m = stamp(&m, &key, &tname, &origin).map_err(|e| sql_mq(&e))?;
                Box::pin(ensure_vhost(&svc, &tchain, &db, Cause::Publish)).await?;
                let engine = svc.mq.get().cloned().ok_or_else(|| {
                    super::unavailable("the queue engine isn't running on this gateway")
                })?;
                let v = engine
                    .sql_vhost(Arc::new(internal_principal(tid, &tname)), &db, 0)
                    .await
                    .map_err(|e| sql_mq(&e))?;
                let cx = OpCtx {
                    principal: Arc::new(internal_principal(tid, &tname)),
                    conn: ConnKey(sprig_queues::sql::SQL_CONN_BIT),
                    channel,
                };
                let p = v
                    .conn_vhost()
                    .publish(&cx, m, mandatory)
                    .map_err(|e| sql_mq(&e))?;
                Ok::<_, SprigError>(p)
            }
            .await;
            let _ = tx.send(());
            match r {
                Ok(p) => {
                    metrics::counter!("sprig_inherit_publishes_total", "kind" => "exchange", "path" => "direct")
                        .increment(1);
                    p.confirm.await
                }
                Err(e) => {
                    tracing::debug!(target = %target, error = %e, "targeted AMQP publish refused");
                    metrics::counter!("sprig_inherit_refused_total", "reason" => "target")
                        .increment(1);
                    Err(mq_err(&e))
                }
            }
        });
        Published {
            routed: 1,
            confirm: Box::pin(async move {
                task.await
                    .unwrap_or_else(|e| Err(MqError::not_implemented(e.to_string())))
            }),
        }
    }
}

impl InheritVhost {
    /// `x-sprig-tenant: *` (or `@*:key`): a broadcast of the definition
    /// (chapter 42, "Broadcast"), confirmed once its log has it; in this
    /// connection's publish order.
    fn broadcast(&self, m: Message, key: String, kind: DefKind, name: String) -> Published {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let prev = self
            .order
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(rx);
        let svc = Arc::clone(&self.svc);
        let me = self.tenant;
        let db = self.db.clone();
        let def = self.names().chain.as_ref().and_then(|c| {
            declared_in(c, &db)
                .into_iter()
                .find(|d| d.kind == kind && d.name == name)
        });
        let task = tokio::spawn(async move {
            if let Some(p) = prev {
                let _ = p.await;
            }
            let r = async {
                let def = def.ok_or_else(|| target_err(format!("no definition {db}.{name}")))?;
                let rec = sprig_streams::publish::Publish {
                    key: Some(key.into_bytes()),
                    value: Some(m.body.to_vec()),
                    headers: Some(xo_headers(&m)),
                    partition: None,
                    ts_ms: None,
                };
                super::broadcast::publish_now(
                    &svc,
                    &def,
                    me,
                    super::broadcast::Form::default(),
                    &rec,
                )
                .await
            }
            .await;
            let _ = tx.send(());
            r.map(|_| ()).map_err(|e| {
                tracing::debug!(error = %e, "AMQP broadcast refused");
                mq_err(&e)
            })
        });
        Published {
            routed: 1,
            confirm: Box::pin(async move {
                task.await
                    .unwrap_or_else(|e| Err(MqError::not_implemented(e.to_string())))
            }),
        }
    }
}

#[async_trait]
impl Vhost for InheritVhost {
    fn name(&self) -> &str {
        &self.vhost
    }

    async fn declare_exchange(
        &self,
        cx: &OpCtx,
        spec: AmqpEx,
        passive: bool,
    ) -> std::result::Result<(), MqError> {
        self.inner.declare_exchange(cx, spec, passive).await
    }

    async fn delete_exchange(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
    ) -> std::result::Result<(), MqError> {
        if self.inherited(DefKind::Exchange, name) {
            return Err(MqError::access_refused(format!(
                "ER_SPRIG_INHERIT_NOT_OVERRIDABLE: exchange '{name}' is inherited; it can't be \
                 deleted in a member (39897)"
            )));
        }
        self.inner.delete_exchange(cx, name, if_unused).await
    }

    async fn declare_queue(
        &self,
        cx: &OpCtx,
        spec: AmqpQueue,
        passive: bool,
    ) -> std::result::Result<QueueDeclared, MqError> {
        self.inner.declare_queue(cx, spec, passive).await
    }

    async fn delete_queue(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> std::result::Result<u32, MqError> {
        if self.inherited(DefKind::Queue, name) {
            return Err(MqError::access_refused(format!(
                "ER_SPRIG_INHERIT_NOT_OVERRIDABLE: queue '{name}' is inherited; it can't be \
                 deleted in a member (39897); queue.purge is allowed"
            )));
        }
        self.inner.delete_queue(cx, name, if_unused, if_empty).await
    }

    async fn purge_queue(&self, cx: &OpCtx, name: &str) -> std::result::Result<u32, MqError> {
        self.inner.purge_queue(cx, name).await
    }

    async fn bind(&self, cx: &OpCtx, b: AmqpBinding) -> std::result::Result<(), MqError> {
        self.inner.bind(cx, b).await
    }

    async fn unbind(&self, cx: &OpCtx, b: AmqpBinding) -> std::result::Result<(), MqError> {
        let dest_kind = match b.dest_kind {
            DestKind::Queue => "queue",
            DestKind::Exchange => "exchange",
        };
        let def = self
            .names()
            .bindings
            .get(&(
                b.source.clone(),
                dest_kind.to_owned(),
                b.destination.clone(),
                b.key.clone(),
            ))
            .copied();
        if let Some(def) = def {
            // An inherited binding: disabled for this member (an
            // override), then removed here.
            let user = cx
                .principal
                .user_id
                .map_or(0, |u| u32::try_from(u).unwrap_or(0));
            disable_binding(&self.svc, self.tenant, def, user, true)
                .await
                .map_err(|e| mq_err(&e))?;
        }
        self.inner.unbind(cx, b).await
    }

    fn publish(
        &self,
        cx: &OpCtx,
        m: Message,
        mandatory: bool,
    ) -> std::result::Result<Published, MqError> {
        let named = {
            let n = self.names();
            n.chain
                .as_ref()
                .and_then(|c| named_def(c, &self.db, &m.exchange, &m.routing_key))
        };
        let Some((kind, name)) = named else {
            return self.inner.publish(cx, m, mandatory);
        };
        let mine = self.inherited(kind, &name);
        match target_of(&m) {
            Some((t, key)) if t == "*" => Ok(self.broadcast(m, key, kind, name)),
            Some((t, key)) if t != self.tenant_name && t != self.tenant.0.to_string() => {
                Ok(self.targeted(cx, m, t, key, kind, name, mandatory))
            }
            Some((_, key)) if mine => {
                let m = stamp(&m, &key, &self.tenant_name, &self.tenant_name)?;
                self.inner.publish(cx, m, mandatory)
            }
            None if mine => {
                let key = m.routing_key.clone();
                let m = stamp(&m, &key, &self.tenant_name, &self.tenant_name)?;
                self.inner.publish(cx, m, mandatory)
            }
            _ => {
                // The declarer without an instance and no target: unroutable
                // (returned when mandatory), counted.
                metrics::counter!("sprig_inherit_refused_total", "reason" => "no_target")
                    .increment(1);
                tracing::debug!(tenant = %self.tenant, exchange = %m.exchange, "publish into a definition without a target (39896): unroutable");
                Ok(Published::done(0))
            }
        }
    }

    async fn consume(&self, cx: &OpCtx, spec: ConsumeSpec) -> std::result::Result<(), MqError> {
        if let Some(r) = self.fan_in_consume(cx, &spec).await {
            return r;
        }
        self.inner.consume(cx, spec).await
    }

    async fn cancel(&self, cx: &OpCtx, consumer: ConsumerId) {
        if let Some(f) = self.fanin(consumer) {
            f.cancel();
            return;
        }
        self.inner.cancel(cx, consumer).await;
    }

    fn credit(&self, consumer: ConsumerId, credit: u32, drain: bool) {
        if let Some(f) = self.fanin(consumer) {
            f.credit(credit, drain);
            return;
        }
        self.inner.credit(consumer, credit, drain);
    }

    async fn get(
        &self,
        cx: &OpCtx,
        queue: &str,
        no_ack: bool,
    ) -> std::result::Result<Option<(Delivery, u32)>, MqError> {
        self.inner.get(cx, queue, no_ack).await
    }

    fn settle(&self, cx: &OpCtx, s: Vec<(DeliveryId, Settlement)>) {
        let mut s = s;
        if s.iter()
            .any(|(id, _)| id.queue & super::fanin_mq::FANIN_ID_BIT != 0 && id.queue >> 63 == 0)
        {
            let fans: Vec<_> = self
                .fanins
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .values()
                .cloned()
                .collect();
            for f in fans {
                s = f.settle(s);
                if s.is_empty() {
                    return;
                }
            }
        }
        self.inner.settle(cx, s);
    }

    async fn commit_tx(&self, cx: &OpCtx, tx: TxBatch) -> std::result::Result<Vec<u32>, MqError> {
        // Transactions publish into the member's own objects only; a
        // stamped copy of each inherited one.
        let mut tx = tx;
        for (m, _) in &mut tx.publishes {
            let named = {
                let n = self.names();
                n.chain
                    .as_ref()
                    .and_then(|c| named_def(c, &self.db, &m.exchange, &m.routing_key))
            };
            if let Some((kind, name)) = named {
                if target_of(m).is_some_and(|(t, _)| t != self.tenant_name) {
                    return Err(MqError::not_implemented(
                        "a publish into another tenant inside an AMQP transaction (use SQL's \
                         transactional publish)",
                    ));
                }
                if self.inherited(kind, &name) {
                    let key = target_of(m).map_or_else(|| m.routing_key.clone(), |(_, k)| k);
                    *m = stamp(m, &key, &self.tenant_name, &self.tenant_name)?;
                }
            }
        }
        self.inner.commit_tx(cx, tx).await
    }

    async fn connection_closed(&self, conn: ConnKey) {
        let fans: Vec<_> = self
            .fanins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, f)| f)
            .collect();
        for f in fans {
            f.close().await;
        }
        self.inner.connection_closed(conn).await;
    }
}

/// Records an override disabling (or enabling) binding definition `def`
/// for member `t` (made in `t`): `ALTER INHERITED BINDING ... DISABLE`, or
/// an AMQP `queue.unbind` of it.
///
/// # Errors
/// The catalog's.
pub async fn disable_binding(
    svc: &Arc<TenantServices>,
    t: TenantId,
    def: DefKey,
    user: u32,
    disable: bool,
) -> Result<()> {
    let o = sprig_inherit::msg::Override {
        made_in: t,
        def,
        target: t,
        applies_to: sprig_inherit::msg::AppliesTo::Own,
        what: What::State,
        value: json!(if disable { "disabled" } else { "enabled" }),
        set_by: user,
        set_at_ms: sprig_inherit::msg::store::now_ms(),
    };
    super::commit(svc, t, &[super::MsgWrite::Override(&o)]).await?;
    forget_applied(t, None);
    Ok(())
}

/// Wraps `inner` (the engine's vhost of database `db` for `principal`)
/// when inherited exchanges, queues or bindings reach the tenant or it
/// declares some, making the member's instances first; else `inner`.
///
/// # Errors
/// The catalog's (as an AMQP error).
pub async fn wrap_vhost(
    svc: &Arc<TenantServices>,
    principal: &Principal,
    db: &str,
    inner: Arc<dyn Vhost>,
    events: Option<sprig_proto_amqp::backend::EventSender>,
) -> std::result::Result<Arc<dyn Vhost>, MqError> {
    let t = TenantId(principal.tenant_id);
    let Ok(cat) = catalog(svc, t).await else {
        return Ok(inner);
    };
    let own = super::state_of(&cat);
    if own.is_empty() && cat.ancestry.is_empty() {
        return Ok(inner);
    }
    let chain = super::chain(svc, &cat).await.map_err(|e| mq_err(&e))?;
    if defs_in(&chain, db).is_empty() && declared_in(&chain, db).is_empty() {
        return Ok(inner);
    }
    Box::pin(ensure_vhost(svc, &chain, db, Cause::Declare))
        .await
        .map_err(|e| mq_err(&e))?;
    Ok(Arc::new(InheritVhost::new(
        inner,
        Arc::clone(svc),
        chain,
        principal.tenant.clone(),
        db.to_owned(),
        events,
    )))
}

/// Before the engine opens vhost database `db` for tenant `t`: the
/// member's instances, so a member's first connection finds its inherited
/// exchanges and queues (and the database, when it had none).
pub async fn before_open(svc: &Arc<TenantServices>, t: TenantId, db: &str) {
    let r = async {
        let cat = catalog(svc, t).await?;
        if cat.ancestry.is_empty() {
            return Ok(());
        }
        let chain = super::chain(svc, &cat).await?;
        Box::pin(ensure_vhost(svc, &chain, db, Cause::Declare))
            .await
            .map(|_| ())
    }
    .await;
    if let Err(e) = r {
        tracing::warn!(tenant = %t, %db, error = %e, "inherited exchanges and queues not made at open");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_proto_amqp::message::MessageInfo;
    use sprig_proto_amqp::v091::codec::FieldTable;
    use sprig_proto_amqp::v091::props::BasicProperties;

    fn msg(key: &str, headers: FieldTable) -> Message {
        let p = BasicProperties {
            headers: Some(headers.clone()),
            ..BasicProperties::default()
        };
        Message {
            format: MsgFormat::Amqp091,
            exchange: "jobs".into(),
            routing_key: key.into(),
            props: p.to_bytes(),
            body: Bytes::from_static(b"hi"),
            info: MessageInfo {
                headers,
                ..MessageInfo::default()
            },
        }
    }

    #[test]
    fn targets_from_header_or_key() {
        assert_eq!(
            strip_key("@client_x:mail.welcome"),
            Some(("client_x".into(), "mail.welcome".into()))
        );
        assert_eq!(strip_key("mail.welcome"), None);
        assert_eq!(strip_key("@:x"), None);
        let m = msg("@cx:a.b", FieldTable::new());
        assert_eq!(target_of(&m), Some(("cx".into(), "a.b".into())));
        let mut h = FieldTable::new();
        h.insert(TENANT_HEADER.to_owned(), FieldValue::str("cy"));
        let m = msg("a.b", h.clone());
        assert_eq!(target_of(&m), Some(("cy".into(), "a.b".into())));
        // The header wins; a key prefix is still removed.
        let m = msg("@cx:a.b", h);
        assert_eq!(target_of(&m), Some(("cy".into(), "a.b".into())));
        assert_eq!(target_of(&msg("a.b", FieldTable::new())), None);
    }

    #[test]
    fn stamping_overwrites_the_clients_headers() {
        let mut h = FieldTable::new();
        h.insert(ORIGIN_HEADER.to_owned(), FieldValue::str("forged"));
        h.insert("app".to_owned(), FieldValue::str("kept"));
        let m = stamp(&msg("@cx:a.b", h), "a.b", "cx", "sys").unwrap();
        assert_eq!(m.routing_key, "a.b");
        let p = BasicProperties::decode(&m.props).unwrap();
        let hs = p.headers.unwrap();
        let get = |k: &str| {
            hs.get(k)
                .and_then(FieldValue::as_str)
                .map(std::borrow::Cow::into_owned)
        };
        assert_eq!(get(TENANT_HEADER).as_deref(), Some("cx"));
        assert_eq!(get(ORIGIN_HEADER).as_deref(), Some("sys"));
        assert_eq!(get("app").as_deref(), Some("kept"));
        let origin = m
            .info
            .headers
            .get(ORIGIN_HEADER)
            .and_then(FieldValue::as_str)
            .map(std::borrow::Cow::into_owned);
        assert_eq!(origin.as_deref(), Some("sys"));
    }
}
