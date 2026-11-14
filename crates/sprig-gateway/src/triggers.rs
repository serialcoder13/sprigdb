//! The gateway's side of `sprig.internal.logic.v1.LogicPeer`: a worker's
//! run of a trigger on a stream or queue.
//!
//! ```text
//! RunTrigger(RunRequest):
//!   the principal of the trigger's definer: the tenant by name (its id must
//!     match), the user by id from the tenant's auth cache, the user's
//!     backend account, as an API-key login builds it, without a password:
//!     the peer service is mutual TLS between SprigDB's own processes
//!   a fresh session for it (as HTTP's Sql.Execute opens one), and in it the
//!     marked statement of exec::msg_trigger, which runs the transaction
//!   → RunReply
//! ```
//!
//! A definer that no longer exists, or a tenant renamed or gone, fails the
//! run with 39860 (not transient: it counts as an attempt).

use std::net::SocketAddr;
use std::sync::Arc;

use sprig_api::proto::logic_peer::logic_peer_server::{LogicPeer, LogicPeerServer};
use sprig_api::proto::logic_peer::{
    RunTriggerReply, RunTriggerRequest, SinkSqlReply, SinkSqlRequest,
};
use sprig_auth::{LoginSource, Principal};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::messages::{RunReply, RunRequest};
use tokio::sync::mpsc;

use crate::http::sql::{Caller, SqlCall, SqlEngine, collect};

/// Replies above this are refused (a reply is one small JSON row).
const MAX_REPLY: usize = 4 << 20;

struct Inner {
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
}

/// The service (cheap to clone).
#[derive(Clone)]
pub struct LogicPeerService(Arc<Inner>);

impl std::fmt::Debug for LogicPeerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicPeerService").finish_non_exhaustive()
    }
}

fn not_found(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicNotFound, m)
}

impl LogicPeerService {
    /// A service opening sessions through `engine` for principals `login`
    /// knows.
    #[must_use]
    pub fn new(login: Arc<sprig_auth::LoginService>, engine: Arc<SqlEngine>) -> Self {
        Self(Arc::new(Inner { login, engine }))
    }

    /// The tonic service.
    #[must_use]
    pub fn server(&self) -> LogicPeerServer<Self> {
        LogicPeerServer::new(self.clone())
    }

    /// The definer's principal.
    async fn principal(&self, req: &RunRequest) -> Result<Principal> {
        principal_of(
            &self.0.login,
            req.tenant,
            &req.source.tenant_name,
            req.source.definer_user_id,
        )
        .await
    }

    /// `SinkSql`: a sink's statements as its owner
    /// ([`crate::sink_peer`]).
    pub async fn sink_sql(
        &self,
        req: sprig_sinks::peer::SqlRequest,
    ) -> sprig_sinks::peer::SqlReply {
        Box::pin(crate::sink_peer::serve(&self.0.login, &self.0.engine, req)).await
    }
}

/// The principal of user `user_id` of tenant `tenant_name` (whose id must
/// be `tenant`), as an API-key login builds it, without a password (the
/// peer service is mutual TLS between SprigDB's own processes).
///
/// # Errors
/// 39860 for a tenant or user that is gone or renamed; 39xxx for a
/// suspended tenant; the catalog's.
pub(crate) async fn principal_of(
    login: &sprig_auth::LoginService,
    tenant_id: u64,
    tenant_name: &str,
    user_id: u32,
) -> Result<Principal> {
    let LoginSource::Catalog(cl) = login.source() else {
        return Err(SprigError::new(
            SprigCode::LogicEngineUnavailable,
            "this gateway has no tenant catalogs (accounts-file logins)",
        ));
    };
    let tenant = cl
        .tenants()
        .lookup(tenant_name)
        .await?
        .filter(|t| t.id == TenantId(tenant_id))
        .ok_or_else(|| {
            not_found(format!(
                "tenant {tenant_name} ({tenant_id}) is gone or renamed"
            ))
        })?;
    if tenant.suspended {
        return Err(SprigError::new(
            SprigCode::TenantSuspended,
            format!("tenant {} is suspended", tenant.name),
        ));
    }
    let catalog = cl.catalogs().catalog(tenant.id).await?;
    let auth = sprig_auth::cache::TenantAuth::of(&catalog)
        .ok_or_else(|| not_found("the tenant's catalog has no accounts"))?;
    let user = auth
        .user(UserId(user_id))
        .cloned()
        .ok_or_else(|| not_found(format!("user {user_id} no longer exists")))?;
    let backend = cl.backend_of(&tenant, &user)?;
    Ok(Principal {
        tenant: Some(tenant.id),
        tenant_name: Some(tenant.name.clone()),
        user_id: Some(user.row.user_id),
        user: user.row.name.clone(),
        host: user.row.host.clone(),
        client_ip: std::net::IpAddr::from([127, 0, 0, 1]),
        must_change_password: false,
        limits: user.row.limits,
        auth_version: catalog.auth_version,
        backend,
    })
}

impl LogicPeerService {
    /// One run (see the module docs).
    pub async fn run(&self, req: &RunRequest) -> RunReply {
        match self.run_inner(req).await {
            Ok(r) => r,
            Err(e) => RunReply::failed(&e),
        }
    }

    async fn run_inner(&self, req: &RunRequest) -> Result<RunReply> {
        let principal = self.principal(req).await?;
        let caller = Caller {
            principal,
            peer: SocketAddr::from(([127, 0, 0, 1], 0)),
            tls: None,
            listener: "logic",
            generation: 0,
            authorization: None,
            client_dc: None,
        };
        let sql = crate::exec::msg_trigger::statement(req)?;
        let (tx, rx) = mpsc::channel(4);
        let engine = Arc::clone(&self.0.engine);
        let stmt = SqlCall {
            database: "-".to_owned(),
            sql,
            multi: false,
        };
        let task = tokio::spawn(async move { engine.run(&caller, stmt, None, tx).await });
        let resp = collect(rx, MAX_REPLY, None).await;
        let _ = task.await;
        let resp = resp?;
        let reply_text = resp
            .results
            .first()
            .and_then(|r| r.rows.first())
            .and_then(|row| row.values.first())
            .and_then(|v| match &v.kind {
                Some(prost_types::value::Kind::StringValue(s)) => Some(s.clone()),
                _ => None,
            })
            .ok_or_else(|| SprigError::internal("a trigger run answered no reply"))?;
        serde_json::from_str(&reply_text)
            .map_err(|e| SprigError::internal(format!("a trigger run's reply: {e}")))
    }
}

#[tonic::async_trait]
impl LogicPeer for LogicPeerService {
    async fn run_trigger(
        &self,
        request: tonic::Request<RunTriggerRequest>,
    ) -> std::result::Result<tonic::Response<RunTriggerReply>, tonic::Status> {
        let req: RunRequest = serde_json::from_str(&request.get_ref().request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("request_json: {e}")))?;
        let reply = self.run(&req).await;
        Ok(tonic::Response::new(RunTriggerReply {
            reply_json: serde_json::to_string(&reply).unwrap_or_default(),
        }))
    }

    async fn sink_sql(
        &self,
        request: tonic::Request<SinkSqlRequest>,
    ) -> std::result::Result<tonic::Response<SinkSqlReply>, tonic::Status> {
        let req: sprig_sinks::peer::SqlRequest =
            serde_json::from_str(&request.get_ref().request_json)
                .map_err(|e| tonic::Status::invalid_argument(format!("request_json: {e}")))?;
        let reply = Box::pin(LogicPeerService::sink_sql(self, req)).await;
        Ok(tonic::Response::new(SinkSqlReply {
            reply_json: serde_json::to_string(&reply).unwrap_or_default(),
        }))
    }
}
