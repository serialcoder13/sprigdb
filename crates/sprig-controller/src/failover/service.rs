//! The controller's internal failover services: `Permits.Renew`,
//! `Peer.Ping` and `Observer.ObserveDc` (chapter 25, "Interfaces"; chapter
//! 23, "Internal services").
//!
//! **Where it differs from the book.** The book serves these as
//! `sprig.internal.v1` gRPC on port 7400 over mTLS. Until that port exists
//! they are JSON over HTTP/1.1 on the controller's `[controller]
//! internal_addr`, the same messages:
//!
//! | Route | Message |
//! |---|---|
//! | `POST /internal/v1/permits/renew` | `PermitRequest` -> `Permit` |
//! | `GET /internal/v1/peer/ping` | -> `PeerPing` |
//! | `POST /internal/v1/observer/observe` | `ObserveRequest` -> `Verdict` |

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_meta::dclease::DcLease;
use sprig_meta::permit::{Permit, PermitRequest};
use tokio_util_shim::CancelOnDrop;

use super::observe::{DcProbe, PeerPing, observe};
use super::record::{StandbyLag, Verdict};
use super::settings::FailoverSettings;

/// `Observer.ObserveDc`'s request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveRequest {
    /// The DC to observe.
    pub dc_id: DcId,
    /// The failover it is for.
    pub failover_id: String,
}

/// What the routes answer from.
pub struct Internal {
    /// This controller's view of its DC's lease.
    pub lease: Arc<DcLease>,
    /// Probes another DC.
    pub probe: Arc<dyn DcProbe>,
    /// The observation settings.
    pub settings: FailoverSettings,
    /// Standby lags this DC holds (the `standby` loop's reports).
    pub lags: Arc<dyn Fn() -> Vec<StandbyLag> + Send + Sync>,
    /// The admission rate a returning DC's gateways get (0: no limit).
    pub admit_per_second: Arc<dyn Fn() -> u32 + Send + Sync>,
    /// Writers this controller fenced itself.
    pub fenced_writers: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
}

impl std::fmt::Debug for Internal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Internal")
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

impl Internal {
    /// `Permits.Renew`.
    #[must_use]
    pub fn renew(&self, _req: &PermitRequest) -> Permit {
        let mut p = self.lease.permit();
        if !p.fenced {
            p.admit_per_second = (self.admit_per_second)();
        }
        p
    }

    /// `Peer.Ping`.
    #[must_use]
    pub fn ping(&self) -> PeerPing {
        let p = self.lease.permit();
        PeerPing {
            dc_id: self.lease.dc().clone(),
            dc_state: self.lease.dc_state(),
            dc_epoch: p.dc_epoch,
            permits_valid: !p.fenced,
            fenced_writers: (self.fenced_writers)(),
        }
    }

    /// `Observer.ObserveDc`.
    pub async fn observe(&self, req: &ObserveRequest) -> Verdict {
        let lags = (self.lags)();
        observe(
            self.lease.dc(),
            self.lease.lease_valid(),
            &req.dc_id,
            &*self.probe,
            self.settings.observer_probes,
            self.settings.observer_probe_interval,
            lags,
        )
        .await
    }
}

fn json<T: Serialize>(v: &T) -> Response {
    match serde_json::to_vec(v) {
        Ok(b) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b))
            .unwrap_or_default(),
        Err(_) => bad("encode"),
    }
}

fn bad(why: &str) -> Response {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(why.to_owned()))
        .unwrap_or_default()
}

async fn renew(State(s): State<Arc<Internal>>, body: Bytes) -> Response {
    match serde_json::from_slice::<PermitRequest>(&body) {
        Ok(r) => json(&s.renew(&r)),
        Err(e) => bad(&e.to_string()),
    }
}

async fn ping(State(s): State<Arc<Internal>>) -> Response {
    json(&s.ping())
}

async fn observe_dc(State(s): State<Arc<Internal>>, body: Bytes) -> Response {
    match serde_json::from_slice::<ObserveRequest>(&body) {
        Ok(r) => json(&s.observe(&r).await),
        Err(e) => bad(&e.to_string()),
    }
}

/// The routes.
pub fn router(state: Arc<Internal>) -> Router {
    Router::new()
        .route("/internal/v1/permits/renew", post(renew))
        .route("/internal/v1/peer/ping", get(ping))
        .route("/internal/v1/observer/observe", post(observe_dc))
        .with_state(state)
}

/// Serves the routes on `listener` until the returned guard drops.
#[must_use]
pub fn serve(listener: tokio::net::TcpListener, state: Arc<Internal>) -> CancelOnDrop {
    let app = router(state);
    let task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::warn!(error = %e, "internal failover service stopped");
        }
    });
    CancelOnDrop(task)
}

/// A small stand-in for a cancellation token: aborts the task on drop.
mod tokio_util_shim {
    /// Aborts the task when dropped.
    #[derive(Debug)]
    pub struct CancelOnDrop(pub tokio::task::JoinHandle<()>);

    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
}
