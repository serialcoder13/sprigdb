//! Fan-in queue sinks' reads (chapter 51, "Fan-in sinks"; chapter 42,
//! "Fan-in"; R9, r9queue): `CREATE SINK ... FROM QUEUE q FAN IN` in an
//! ancestor P reads every member's instance of the queue definition `q`.
//! The sink's task (in a worker) lists the instances from the DC's index
//! and asks a gateway, as the sink's owner, through `LogicPeer.SinkSql`:
//!
//! ```text
//! FanInGet {def, member, db, queue, max, lease_ms}
//!   the owner holds SPRIG_FAN_IN, SELECT and DELETE on db.queue in P, the
//!   definition reaches P (P declared it or is an intermediate member),
//!   member is below P and doesn't deny remote access (residency)
//!   → a get on the member's instance (an internal principal of the member,
//!     as the AMQP fan-in consumer's), each message held lease_ms:
//!     mq_get's columns (delivery_token first)
//!   (R9, r9last: under the task's lease tag when it gives one)
//! FanInRelease {def, member, db, queue, tag}
//!   the same checks → the member's instance gives back what was held
//!   under tag (a dead task's leases, as mq_release)
//! FanInSettle {tokens, ack}
//!   each token checked (its MAC, its tenant below P) and settled with its
//!   owner: acknowledged, or rejected without requeue (the queue's
//!   dead-lettering applies)
//! ```

use std::sync::Arc;

use sprig_common::ids::{DbId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::DefKey;
use sprig_proto_amqp::error::MqError;
use sprig_sinks::peer::{PeerError, SqlReply, SqlResult};

use super::mq::internal_principal;
use crate::http::sql::Caller;
use crate::tenant::TenantServices;

fn mq_err(e: &MqError) -> SprigError {
    let mut x = SprigError::standard(
        u16::try_from(e.code.number()).unwrap_or(39250),
        *b"HY000",
        format!("{}: {}", e.code.name(), e.text),
    );
    x.retryable = e.code == sprig_proto_amqp::error::MqCode::OwnerMoved;
    x
}

fn denied(what: impl std::fmt::Display) -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        format!(
            "Access denied; you need (at least one of) the {what} privilege(s) for this operation"
        ),
    )
}

fn engine(svc: &TenantServices) -> Result<Arc<sprig_queues::QueueEngine>> {
    svc.mq.get().cloned().ok_or_else(|| {
        let mut e = SprigError::new(
            SprigCode::LogicEngineUnavailable,
            "the queue engine isn't running on this gateway (AMQP is off)",
        );
        e.retryable = true;
        e
    })
}

/// The reader's tenant and its member `member`'s name, when the reader
/// may fan in from it.
async fn member_of(svc: &TenantServices, reader: TenantId, member: TenantId) -> Result<String> {
    let d = super::dir_entry(svc, member)
        .await?
        .ok_or_else(|| super::target_err(format!("tenant {member} doesn't exist")))?;
    if member == reader || !d.path_ids().contains(&reader.0) {
        return Err(super::target_err(format!(
            "tenant {member} isn't a member below tenant {reader}"
        )));
    }
    if sprig_auth::TenantResidency::of(&d.residency).is_some_and(|r| r.deny_remote_access) {
        metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
            .increment(1);
        return Err(SprigError::new(
            SprigCode::InheritResidency,
            format!("tenant {} denies remote access (residency)", d.name),
        ));
    }
    Ok(d.name)
}

/// See the module docs.
///
/// # Errors
/// 1227 without the privileges; 39896 for a definition or member out of
/// reach; 39243 for a member denying remote access; the engine's.
#[allow(clippy::too_many_arguments)]
pub async fn get(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    queue: &str,
    max: u32,
    lease_ms: u64,
    tag: Option<&str>,
) -> Result<SqlReply> {
    let v = member_vhost(svc, caller, def, member, db, queue).await?;
    let ds = v
        .get_tagged(queue, max.clamp(1, 1000), lease_ms.max(1), tag)
        .await
        .map_err(|x| mq_err(&x))?;
    let rows = ds
        .iter()
        .map(|d| {
            let token = d
                .annotations
                .get("x-sprig-delivery")
                .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned));
            let body = sprig_queues::sql::body_bytes(d);
            vec![
                SqlResult::cell(token.as_deref().map(str::as_bytes)),
                SqlResult::cell(Some(&body)),
                SqlResult::cell(Some(sprig_queues::sql::properties_json(d).as_bytes())),
                SqlResult::cell(Some(d.message.routing_key.as_bytes())),
                SqlResult::cell(Some(d.message.exchange.as_bytes())),
                SqlResult::cell(Some(if d.redelivered { b"1" } else { b"0" })),
                SqlResult::cell(Some(d.delivery_count.to_string().as_bytes())),
            ]
        })
        .collect();
    metrics::counter!("sprig_sink_fanin_queue_gets_total").increment(1);
    Ok(SqlReply {
        results: vec![SqlResult {
            columns: [
                "delivery_token",
                "body",
                "properties",
                "routing_key",
                "exchange",
                "redelivered",
                "delivery_count",
            ]
            .map(str::to_owned)
            .to_vec(),
            types: Vec::new(),
            rows,
            affected: 0,
            error: None,
        }],
        ..SqlReply::default()
    })
}

/// See the module docs: a dead task's leases in member `member`'s
/// instance go back to the queue (R9, r9last).
///
/// # Errors
/// As [`get`].
pub async fn release(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    queue: &str,
    tag: &str,
) -> Result<SqlReply> {
    let v = member_vhost(svc, caller, def, member, db, queue).await?;
    v.release_tag(queue, tag).await.map_err(|x| mq_err(&x))?;
    Ok(SqlReply {
        results: vec![SqlResult::default()],
        ..SqlReply::default()
    })
}

/// Member `member`'s instance vhost of definition `def`, for the caller
/// as a fan-in reader (see the module docs for the checks).
async fn member_vhost(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    queue: &str,
) -> Result<sprig_queues::sql::SqlVhost> {
    let reader = caller
        .principal
        .tenant
        .ok_or_else(|| SprigError::internal("a sink owner without a tenant"))?;
    let cat = super::catalog(svc, reader).await?;
    let chain = super::chain(svc, &cat).await?;
    let want = DefKey {
        tenant: TenantId(def.0),
        object: def.1,
    };
    super::fanin_mq::queue_definition(&chain, db, queue)
        .filter(|d| d.key() == want)
        .ok_or_else(|| {
            super::target_err(format!(
                "no FOR EACH TENANT queue {db}.{queue} in reach for FAN IN"
            ))
        })?;
    let p = sprig_proto_amqp::auth::Principal {
        tenant_id: reader.0,
        tenant: caller.principal.tenant_name.clone().unwrap_or_default(),
        user: caller.principal.user.clone(),
        user_id: caller.principal.user_id.map(|u| u64::from(u.0)),
        handle: None,
    };
    if !super::fanin_mq::allowed(svc, &cat, &p, db, queue) {
        return Err(denied("SPRIG_FAN_IN, SELECT and DELETE"));
    }
    let m = TenantId(member);
    let name = member_of(svc, reader, m).await?;
    let e = engine(svc)?;
    e.sql_vhost(Arc::new(internal_principal(m, &name)), db, 0)
        .await
        .map_err(|x| mq_err(&x))
}

/// See the module docs: one result per token, with its error if it
/// couldn't be settled.
///
/// # Errors
/// The reader's catalog's.
pub async fn settle(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    tokens: &[String],
    ack: bool,
) -> Result<SqlReply> {
    let reader = caller
        .principal
        .tenant
        .ok_or_else(|| SprigError::internal("a sink owner without a tenant"))?;
    let e = engine(svc)?;
    let mut results = Vec::with_capacity(tokens.len());
    for t in tokens {
        let r = async {
            let td = e.verify_token(t).map_err(|x| mq_err(&x))?;
            let m = TenantId(td.tenant);
            let name = member_of(svc, reader, m).await?;
            let mcat = super::catalog(svc, m).await?;
            let db = mcat
                .databases
                .get(DbId(td.db))
                .map(|d| d.name.clone())
                .ok_or_else(|| super::target_err("the token's vhost no longer exists"))?;
            let v = e
                .sql_vhost(Arc::new(internal_principal(m, &name)), &db, 0)
                .await
                .map_err(|x| mq_err(&x))?;
            if ack {
                v.ack(&td);
            } else {
                v.nack(&td, false);
            }
            Ok::<_, SprigError>(())
        }
        .await;
        results.push(SqlResult {
            error: r.err().map(|x| PeerError::of(&x)),
            ..SqlResult::default()
        });
    }
    Ok(SqlReply {
        results,
        ..SqlReply::default()
    })
}
