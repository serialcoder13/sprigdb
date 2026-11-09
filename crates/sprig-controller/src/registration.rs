//! A controller's registration, `/sprig/dc/<dc>/controllers/<node>`
//! (chapter 23, "DC tier"): where its Admin API listens, bound to a lease
//! so it disappears with the process. Gateways forward the Admin API to
//! the DC leader's address (chapter 37 step 4).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result};
use sprig_meta::session::SessionOpts;
use sprig_meta::{MetaStore, Op, Record, Session, Txn, Versioned};

/// The registration lease.
pub const TTL: Duration = Duration::from_secs(10);

/// `controllers/<node>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerRecord {
    /// `host:port` of the Admin API.
    pub admin_addr: String,
    /// The build.
    #[serde(default)]
    pub version: String,
    /// `host:port` of the internal services (chapter 25's permits, peer
    /// pings and observations; chapter 23's relay): where gateways and
    /// agents renew their permits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_addr: Option<String>,
}

impl Versioned for ControllerRecord {
    const V: u32 = 1;
}

/// The address to publish for a listener bound to `bound`: an unspecified
/// IP becomes the hostname.
#[must_use]
pub fn advertised(bound: SocketAddr) -> String {
    if bound.ip().is_unspecified() {
        let host = gethostname::gethostname().to_string_lossy().into_owned();
        format!("{host}:{}", bound.port())
    } else {
        bound.to_string()
    }
}

async fn register_once(
    store: &Arc<dyn MetaStore>,
    clock: &Arc<dyn Clock>,
    node: &str,
    rec: &ControllerRecord,
) -> Result<Session> {
    let session =
        Session::open(Arc::clone(store), Arc::clone(clock), SessionOpts::new(TTL)).await?;
    let key = store.tier().key(&format!("controllers/{node}"))?;
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: Record::new(rec.clone()).encode()?,
                lease: Some(session.lease()),
            }],
            otherwise: vec![],
        })
        .await?;
    Ok(session)
}

/// Keeps `controllers/<node>` registered until `stop`, registering again
/// after a lost lease.
pub async fn keep(
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    node: String,
    rec: ControllerRecord,
    stop: CancellationToken,
) {
    loop {
        let session = tokio::select! {
            () = stop.cancelled() => return,
            r = register_once(&store, &clock, &node, &rec) => r,
        };
        match session {
            Ok(s) => {
                tracing::info!(admin = %rec.admin_addr, "controller registered");
                tokio::select! {
                    () = stop.cancelled() => {
                        let _ = s.close().await;
                        return;
                    }
                    () = s.lost() => tracing::warn!("controller registration lease lost; registering again"),
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "controller registration failed; retrying");
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::DcId;
    use sprig_common::time::SystemClock;
    use sprig_meta::{Consistency, MemStore, Tier};

    use super::*;

    #[tokio::test]
    async fn registers_and_deregisters() {
        let store: Arc<dyn MetaStore> =
            Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-t").unwrap())));
        let stop = CancellationToken::new();
        let rec = ControllerRecord {
            admin_addr: "127.0.0.1:7480".into(),
            version: "t".into(),
            internal_addr: None,
        };
        let task = tokio::spawn(keep(
            Arc::clone(&store),
            Arc::new(SystemClock),
            "c1".into(),
            rec.clone(),
            stop.clone(),
        ));
        let key = store.tier().key("controllers/c1").unwrap();
        let mut seen = None;
        for _ in 0..50 {
            if let Some(kv) = store.get(&key, Consistency::Linearizable).await.unwrap() {
                seen = Some(Record::<ControllerRecord>::decode(&kv.value).unwrap().body);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(seen, Some(rec));
        stop.cancel();
        task.await.unwrap();
        assert!(
            store
                .get(&key, Consistency::Linearizable)
                .await
                .unwrap()
                .is_none()
        );
    }
}
