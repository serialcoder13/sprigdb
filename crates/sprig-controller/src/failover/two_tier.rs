//! DC failover across two etcd tiers (chapter 25, "Promotion and epochs";
//! chapter 23 step 10's `Global` service): what lifts decision W12.
//!
//! With one etcd (R0's single-etcd mode) the global leader writes every
//! step of a promotion itself. With two tiers it can't reach the standby
//! DC's tier or nodes, so the promotion is split where the book splits it:
//!
//! ```text
//! global leader:  1. the commit point in tenants/T (home Y, epoch + 1,
//!                    failing_over); the record says `committed`
//! Y's leader, tenant_promote loop (every 500 ms):
//!                 for each open record with T committed to Y, T's entry still
//!                 at that epoch and failing_over:
//!                 2. stop T's standby; positions in promotions/<T> (Y's tier)
//!                 3. Y's maps for T: home, epoch + 1
//!                 4. write access
//!                 -> Global.PromotionDone to the global leader
//! global leader:  5. the entry `active`, auth_version + 1 (under its token);
//!                 the record takes the positions: `promoted`
//! ```
//!
//! The global leader publishes where its `Global` service listens in the
//! global key `global-service` (holder, URL), so DC leaders reach it
//! without knowing which controller leads. The service is JSON over HTTP
//! on `[controller] internal_addr`, like the other internal services
//! (decision F2), and has one call so far, `PromotionDone`; the book's
//! other `Global` calls (`CreateTenant`, `SetDcState`, ...) are still direct
//! writes, which the etcd roles below the global leader don't allow yet.
//!
//! The old home's maps can't be quarantined from here (its tier is its own
//! etcd, out of reach while it is lost): its gateways are fenced by their
//! permits, and the returning DC quarantines and fences the moved tenants
//! itself before it takes writes again (chapter 25, "A returning server or
//! DC stays fenced").

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use hyper::Method;
use serde::{Deserialize, Serialize};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::permit::http::JsonHttp;
use sprig_meta::{
    Consistency, Key, Leadership, ListOpts, MetaStore, Op, Record, Tier, Txn, Versioned,
};
use sprig_shard::home::read_home;

use super::global::GlobalFailover;
use super::promote::{PromoteHooks, promote_maps};
use super::record::{FailoverRecord, FailoverState};

/// `/sprig/g/global-service`.
///
/// # Errors
/// Internal never in practice.
pub fn global_service_key() -> Result<Key> {
    Tier::Global.key("global-service")
}

/// Where the global leader's `Global` service listens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalService {
    /// The leading controller.
    pub holder: String,
    /// Its internal URL.
    pub url: String,
}

impl Versioned for GlobalService {
    const V: u32 = 1;
}

/// `Global.PromotionDone`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionDone {
    /// The failover record.
    pub failover_id: String,
    /// The tenant.
    pub tenant_id: u64,
    /// The DC that finished steps 2-4.
    pub to: DcId,
    /// The epoch of the commit point.
    pub epoch: u64,
    /// `applied_source_gtid` per source.
    #[serde(default)]
    pub applied: BTreeMap<String, String>,
}

/// Its reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionDoneReply {
    /// The entry is `active` at that epoch now (`false`: the directory
    /// moved on, nothing to do).
    pub activated: bool,
}

/// `promotions/<tenant>` in the target DC's tier: a promotion's progress
/// there, so a resumed pass reports the positions of the first stop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionProgress {
    /// The failover record.
    pub failover_id: String,
    /// The epoch of the commit point.
    pub epoch: u64,
    /// `stopped`, `writable` or `done`.
    pub state: String,
    /// Positions when the standby stopped.
    #[serde(default)]
    pub applied: BTreeMap<String, String>,
}

impl Versioned for PromotionProgress {
    const V: u32 = 1;
}

fn progress_key(tier: &Tier, tenant: TenantId) -> Result<Key> {
    tier.key(&format!("promotions/{tenant}"))
}

async fn put(store: &dyn MetaStore, key: Key, value: Vec<u8>) -> Result<()> {
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Publishes the leader's `Global` service URL under its token.
///
/// # Errors
/// 39504 when leadership was lost; the store's errors.
pub async fn publish(l: &Leadership, holder: &str, url: &str) -> Result<()> {
    let value = Record::new(GlobalService {
        holder: holder.to_owned(),
        url: url.to_owned(),
    })
    .encode()?;
    l.commit_if(
        Vec::new(),
        vec![Op::Put {
            key: global_service_key()?,
            value,
            lease: None,
        }],
    )
    .await?;
    Ok(())
}

/// Calls the global leader's `Global` service.
#[derive(Clone)]
pub struct GlobalClient {
    global: Arc<dyn MetaStore>,
    http: JsonHttp,
}

impl std::fmt::Debug for GlobalClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalClient").finish_non_exhaustive()
    }
}

impl GlobalClient {
    /// A client finding the leader through `global-service`.
    #[must_use]
    pub fn new(global: Arc<dyn MetaStore>, timeout: std::time::Duration) -> Self {
        Self {
            global,
            http: JsonHttp::new(timeout),
        }
    }

    /// `Global.PromotionDone`.
    ///
    /// # Errors
    /// 39504 when no leader publishes the service or it can't be reached.
    pub async fn promotion_done(&self, req: &PromotionDone) -> Result<PromotionDoneReply> {
        let kv = self
            .global
            .get(&global_service_key()?, Consistency::Linearizable)
            .await?
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::NotLeader,
                    "no global leader publishes its service",
                )
            })?;
        let svc = Record::<GlobalService>::decode(&kv.value)?.body;
        let url = format!(
            "{}/internal/v1/global/promotion-done",
            svc.url.trim_end_matches('/')
        );
        self.http.call(Method::POST, &url, Some(req)).await
    }
}

/// The current global failover driver, while this controller leads.
pub type GlobalHandle = Arc<dyn Fn() -> Option<Arc<GlobalFailover>> + Send + Sync>;

fn reply(status: StatusCode, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap_or_default()
}

async fn promotion_done(State(h): State<GlobalHandle>, body: Bytes) -> Response {
    let req: PromotionDone = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return reply(StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };
    let Some(g) = h() else {
        return reply(
            StatusCode::SERVICE_UNAVAILABLE,
            b"39504 not the global leader".to_vec(),
        );
    };
    match g
        .promotion_done(
            &req.failover_id,
            TenantId(req.tenant_id),
            &req.to,
            req.epoch,
            req.applied,
        )
        .await
    {
        Ok(activated) => reply(
            StatusCode::OK,
            serde_json::to_vec(&PromotionDoneReply { activated }).unwrap_or_default(),
        ),
        Err(e) if e.code() == Some(SprigCode::NotLeader) => {
            reply(StatusCode::SERVICE_UNAVAILABLE, e.to_string().into_bytes())
        }
        Err(e) => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string().into_bytes(),
        ),
    }
}

/// The `Global` routes.
pub fn router(h: GlobalHandle) -> Router {
    Router::new()
        .route("/internal/v1/global/promotion-done", post(promotion_done))
        .with_state(h)
}

/// How the target DC reaches the global leader (a seam for tests).
#[async_trait::async_trait]
pub trait PromotionReporter: Send + Sync {
    /// `Global.PromotionDone`.
    async fn done(&self, req: &PromotionDone) -> Result<PromotionDoneReply>;
}

#[async_trait::async_trait]
impl PromotionReporter for GlobalClient {
    async fn done(&self, req: &PromotionDone) -> Result<PromotionDoneReply> {
        self.promotion_done(req).await
    }
}

/// One pass of the `tenant_promote` loop in DC `me` (see the module docs).
/// Returns how many promotions it finished.
///
/// # Errors
/// The stores', the hooks' and the global leader's errors; the pass is
/// resumable.
pub async fn tenant_promote(
    global: &dyn MetaStore,
    dc: &dyn MetaStore,
    me: &DcId,
    hooks: &dyn PromoteHooks,
    reporter: &dyn PromotionReporter,
) -> Result<usize> {
    let page = global
        .list(&Tier::Global.key("failovers/")?, ListOpts::default())
        .await?;
    let mut done = 0;
    for kv in page.kvs {
        let Ok(f) = Record::<FailoverRecord>::decode(&kv.value) else {
            continue;
        };
        let f = f.body;
        if f.state != FailoverState::Promoting {
            continue;
        }
        for t in f
            .tenants
            .iter()
            .filter(|t| t.outcome == "committed" && t.to.as_ref() == Some(me))
        {
            let tenant = TenantId(t.tenant_id);
            let Some(epoch) = t.epoch else { continue };
            let Some(e) = read_home(global, tenant).await? else {
                continue;
            };
            if &e.home_dc != me || e.epoch != epoch {
                continue;
            }
            let pkey = progress_key(dc.tier(), tenant)?;
            let progress = dc
                .get(&pkey, Consistency::Linearizable)
                .await?
                .and_then(|kv| Record::<PromotionProgress>::decode(&kv.value).ok())
                .map(|r| r.body)
                .filter(|p| p.failover_id == f.failover_id && p.epoch == epoch);
            let mut p = if e.state == "failing_over" {
                // 2. The standby stops once; later passes reuse its
                // positions.
                let mut p = if let Some(p) = progress {
                    p
                } else {
                    let applied = hooks.stop_standby(tenant, me).await?;
                    let p = PromotionProgress {
                        failover_id: f.failover_id.clone(),
                        epoch,
                        state: "stopped".into(),
                        applied,
                    };
                    put(dc, pkey.clone(), Record::new(p.clone()).encode()?).await?;
                    p
                };
                // 3-4.
                promote_maps(dc, tenant, epoch).await?;
                hooks.make_writable(tenant, me).await?;
                if p.state == "stopped" {
                    p.state = "writable".into();
                    put(dc, pkey.clone(), Record::new(p.clone()).encode()?).await?;
                }
                p
            } else {
                // Active already (a reply lost on the way back): report again so
                // the record takes the positions.
                match progress {
                    Some(p) if p.state != "done" => p,
                    _ => continue,
                }
            };
            // 5. Through the global leader.
            let r = reporter
                .done(&PromotionDone {
                    failover_id: f.failover_id.clone(),
                    tenant_id: tenant.0,
                    to: me.clone(),
                    epoch,
                    applied: p.applied.clone(),
                })
                .await?;
            if r.activated {
                p.state = "done".into();
                put(dc, pkey, Record::new(p).encode()?).await?;
                tracing::warn!(%tenant, dc = %me, epoch, failover = %f.failover_id, "tenant_promote: promoted here");
                done += 1;
            }
        }
    }
    Ok(done)
}

#[cfg(test)]
#[allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::items_after_statements
)]
mod tests;
