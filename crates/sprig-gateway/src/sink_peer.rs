//! The gateway's side of `LogicPeer.SinkSql` (chapter 51, R9, r9queue): a
//! sink task's statements in a session of the sink's owner, opened as the
//! trigger runs are ([`crate::triggers::principal_of`]), through the
//! gateway's SQL engine, so everything a session of the owner checks
//! applies (privileges, column privileges, row-level security, name
//! virtualization).
//!
//! ```text
//! Begin                 SqlEngine::begin(read_only): a transaction handle
//! Read {handle, sql}    one SELECT or WITH (39235 otherwise) in it, capped
//! End {handle}          ROLLBACK
//! Queue {statements}    each CALL sprig.mq_get | mq_ack | mq_nack (39235
//!                       otherwise), autocommit, in order; each statement's
//!                       error is its own result's
//! FanInGet / FanInSettle  a fan-in queue sink's reads of member instances
//! FanInRelease            (R9, r9last) a dead fan-in task's leases back
//!                       (crate::inherited::fanin_sink)
//! Images {tables}       (RL50) which row images the user may see: rows
//!                       (t, i, v) of crate::exec::rls_images
//! Publish {statements}  (RL52) a change exchange's CALL sprig.mq_publish
//!                       calls, autocommit, in order
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use sprig_common::{Result, SprigError};
use sprig_sinks::peer::{PeerError, SqlOp, SqlReply, SqlRequest, SqlResult};
use tokio::sync::mpsc;

use crate::http::sql::{Caller, Caps, SqlCall, SqlEngine, collect};

/// Replies above this are refused.
const MAX_REPLY: usize = 64 << 20;

/// Serves one request (see the module docs).
pub async fn serve(
    login: &Arc<sprig_auth::LoginService>,
    engine: &Arc<SqlEngine>,
    req: SqlRequest,
) -> SqlReply {
    match Box::pin(serve_inner(login, engine, req)).await {
        Ok(r) => r,
        Err(e) => SqlReply {
            error: Some(PeerError::of(&e)),
            ..SqlReply::default()
        },
    }
}

#[allow(clippy::too_many_lines)] // one arm per operation
async fn serve_inner(
    login: &Arc<sprig_auth::LoginService>,
    engine: &Arc<SqlEngine>,
    req: SqlRequest,
) -> Result<SqlReply> {
    // R9 (r9fwd): the feed-driven outbox forwarder's step, internal (no
    // session, no principal).
    if let SqlOp::OutboxForward { schema, table, ids } = &req.op {
        let svc = engine
            .tenants
            .clone()
            .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
        return Box::pin(crate::outbox_feed::serve(
            &svc, req.tenant, schema, table, ids,
        ))
        .await;
    }
    let principal =
        crate::triggers::principal_of(login, req.tenant, &req.tenant_name, req.user_id).await?;
    let caller = Caller {
        principal,
        peer: SocketAddr::from(([127, 0, 0, 1], 0)),
        tls: None,
        listener: "sink",
        generation: 0,
        authorization: None,
        client_dc: None,
    };
    let caps = Caps {
        max_rows: req.max_rows,
        max_bytes: 0,
        timeout: (req.timeout_ms > 0).then(|| std::time::Duration::from_millis(req.timeout_ms)),
    };
    match req.op {
        SqlOp::Begin => {
            let t = engine.begin(&caller, &req.database, true).await?;
            Ok(SqlReply {
                handle: Some(t.name),
                ..SqlReply::default()
            })
        }
        SqlOp::Read { handle, sql } => {
            sprig_sinks::target::script::check_read_only(&sql)?;
            let results = run(engine, &caller, &req.database, sql, Some(&handle), caps).await?;
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        SqlOp::End { handle } => {
            engine.end(&caller, &handle, false).await?;
            Ok(SqlReply::default())
        }
        SqlOp::Queue { statements } => {
            sprig_sinks::peer::check_queue_calls(&statements)?;
            let mut results = Vec::with_capacity(statements.len());
            // Every queue call names its vhost (an argument, or the
            // delivery token's), so the session selects no database: a
            // user granted only the queue (an inherited queue sink's
            // sprig_inherited, R9 r9tests) may not `USE` the database.
            for sql in statements {
                match run(engine, &caller, "", sql, None, caps).await {
                    Ok(mut r) => results.push(r.pop().unwrap_or_default()),
                    Err(e) => results.push(SqlResult {
                        error: Some(PeerError::of(&e)),
                        ..SqlResult::default()
                    }),
                }
            }
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        SqlOp::FanInGet {
            def_tenant,
            def_object,
            member,
            db,
            queue,
            max,
            lease_ms,
            tag,
        } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            crate::inherited::fanin_sink::get(
                &svc,
                &caller,
                (def_tenant, def_object),
                member,
                &db,
                &queue,
                max,
                lease_ms,
                tag.as_deref(),
            )
            .await
        }
        // R9 (r9last): a dead fan-in task's leases in one member.
        SqlOp::FanInRelease {
            def_tenant,
            def_object,
            member,
            db,
            queue,
            tag,
        } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            crate::inherited::fanin_sink::release(
                &svc,
                &caller,
                (def_tenant, def_object),
                member,
                &db,
                &queue,
                &tag,
            )
            .await
        }
        SqlOp::OutboxForward { .. } => Err(SprigError::internal("served above")),
        // Chapter 29 (RLS round 5, RL50): an after-commit trigger's images
        // as its definer may see them, through the marked image check
        // (crate::exec::rls_images), whose nonce only this process knows.
        SqlOp::Images { tables } => {
            let req = crate::exec::rls_images::ImagesRequest {
                tables: tables
                    .into_iter()
                    .map(|t| crate::exec::rls_images::TableImages {
                        db: t.db,
                        table: t.table,
                        images: t.images,
                    })
                    .collect(),
                plain_binary: false,
            };
            let sql = crate::exec::rls_images::statement(&req);
            let results = run(engine, &caller, "", sql, None, caps).await?;
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        // Chapter 50 (RL52): a change exchange's publishes as the user who
        // set it (its privileges on the exchange apply).
        SqlOp::Publish { statements } => {
            sprig_sinks::peer::check_publish_calls(&statements)?;
            let mut results = Vec::with_capacity(statements.len());
            for sql in statements {
                match run(engine, &caller, "", sql, None, caps).await {
                    Ok(mut r) => results.push(r.pop().unwrap_or_default()),
                    Err(e) => results.push(SqlResult {
                        error: Some(PeerError::of(&e)),
                        ..SqlResult::default()
                    }),
                }
            }
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        SqlOp::FanInSettle { tokens, ack } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            crate::inherited::fanin_sink::settle(&svc, &caller, &tokens, ack).await
        }
    }
}

/// Runs one statement: its results, cells as bytes.
async fn run(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    database: &str,
    sql: String,
    txn: Option<&str>,
    caps: Caps,
) -> Result<Vec<SqlResult>> {
    let (tx, rx) = mpsc::channel(16);
    let call = SqlCall {
        database: if database.is_empty() {
            "-".to_owned()
        } else {
            database.to_owned()
        },
        sql,
        multi: false,
    };
    let resp = {
        let engine = Arc::clone(engine);
        let caller = caller.clone();
        let txn = txn.map(str::to_owned);
        let task = tokio::spawn(async move {
            engine
                .run_capped(&caller, call, txn.as_deref(), caps, tx)
                .await;
        });
        let resp = collect(rx, MAX_REPLY, None).await;
        let _ = task.await;
        resp?
    };
    Ok(resp.results.iter().map(result_of).collect())
}

/// An API result as a peer result: each cell's bytes (binary columns
/// decoded from base64).
fn result_of(r: &sprig_api::proto::v1::Result) -> SqlResult {
    let binary: Vec<bool> = r
        .columns
        .iter()
        .map(|c| sprig_api::results::is_binary(c.mysql_type, c.charset))
        .collect();
    SqlResult {
        columns: r.columns.iter().map(|c| c.name.clone()).collect(),
        types: r.columns.iter().map(|c| c.mysql_type).collect(),
        rows: r
            .rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let b = sprig_api::results::cell_bytes(
                            v,
                            binary.get(i).copied().unwrap_or(false),
                        );
                        SqlResult::cell(b.as_deref())
                    })
                    .collect()
            })
            .collect(),
        affected: r.affected_rows,
        error: None,
    }
}
