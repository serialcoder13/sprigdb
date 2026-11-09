//! Brokers and coordinator leases (chapter 45, "Brokers and metadata",
//! "Consumer groups").
//!
//! - **Brokers** are the DC's ready gateways that run the `kafka` listener,
//!   from their registry records (chapter 04), read at most once a second.
//!   The broker id is the gateway's slot: unique among live gateways of a
//!   DC, and a small integer as Kafka needs. The host is
//!   `kafka.advertised_host` when set, else the gateway's advertised
//!   address.
//! - **Coordinator leases** are etcd keys
//!   `/sprig/dc/<dc_id>/stream-groups/<tenant_id>/<group>` bound to one
//!   lease of this gateway (10 s, kept alive every 3 s). A key is created
//!   only if missing, so one gateway coordinates a group at a time. When
//!   the lease is lost, every group of this gateway is dropped (its members
//!   get `NOT_COORDINATOR`) and a later request takes a new lease.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_common::CancellationToken;
use sprig_common::ids::DcId;
use sprig_meta::{Cmp, Consistency, Key, LeaseId, MetaStore, Op, OpResult, Txn};
use sprig_proto_kafka::broker::{BrokerNode, Cluster};
use sprig_proto_kafka::group::{GroupKey, LeaseState, Leases, StoreError};
use sprig_proto_kafka::topics::Principal;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::registry::{self, GatewayState, Registrar};

/// The coordinator lease's time to live.
const LEASE_TTL: Duration = Duration::from_secs(10);
/// How often it is kept alive.
const KEEPALIVE: Duration = Duration::from_secs(3);
/// How long a broker list is reused.
const BROKERS_TTL: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize)]
struct LeaseValue {
    v: u32,
    gateway_id: String,
    node: i32,
}

#[derive(Default)]
struct Held {
    lease: Option<LeaseId>,
    epoch: u64,
    keys: HashSet<GroupKey>,
}

/// This gateway as a Kafka broker, and its coordinator leases.
pub struct GatewayCluster {
    store: Arc<dyn MetaStore>,
    registrar: Registrar,
    live: LiveSettings,
    dc: DcId,
    held: tokio::sync::Mutex<Held>,
    brokers: Mutex<Option<(Instant, Vec<BrokerNode>)>>,
}

impl std::fmt::Debug for GatewayCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayCluster")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// Escapes a group id into one etcd key segment: bytes outside
/// `[A-Za-z0-9._-]` become `%XX`.
#[must_use]
pub fn key_segment(group: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(group.len());
    for b in group.bytes() {
        if b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-' {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn store_err(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

impl GatewayCluster {
    /// The cluster of the gateway `registrar` registers, over the DC tier.
    #[must_use]
    pub fn new(
        store: Arc<dyn MetaStore>,
        registrar: Registrar,
        live: LiveSettings,
        dc: DcId,
    ) -> Self {
        Self {
            store,
            registrar,
            live,
            dc,
            held: tokio::sync::Mutex::new(Held::default()),
            brokers: Mutex::new(None),
        }
    }

    fn key(&self, k: &GroupKey) -> Result<Key, StoreError> {
        self.store
            .tier()
            .key(&format!(
                "stream-groups/{}/{}",
                k.tenant,
                key_segment(&k.group)
            ))
            .map_err(store_err)
    }

    fn value(&self) -> Vec<u8> {
        let r = self.registrar.record();
        serde_json::to_vec(&LeaseValue {
            v: 1,
            gateway_id: r.gateway_id,
            node: i32::from(r.slot),
        })
        .unwrap_or_default()
    }

    fn node_of(raw: &[u8]) -> Option<i32> {
        serde_json::from_slice::<LeaseValue>(raw)
            .ok()
            .map(|v| v.node)
    }

    /// Keeps the coordinator lease alive until `stop`; a lost lease drops
    /// every group key this gateway held.
    pub async fn keep_leases(self: Arc<Self>, stop: CancellationToken) {
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(KEEPALIVE) => {}
            }
            let lease = self.held.lock().await.lease;
            let Some(lease) = lease else { continue };
            // An error counts as a loss: the lease may expire before etcd
            // answers again, and another gateway may then coordinate.
            if !matches!(self.store.keep_alive(lease).await, Ok(Some(_))) {
                let mut h = self.held.lock().await;
                if h.lease == Some(lease) {
                    tracing::warn!("kafka coordinator lease lost: groups move to other gateways");
                    h.lease = None;
                    h.epoch += 1;
                    h.keys.clear();
                }
            }
        }
        let lease = self.held.lock().await.lease.take();
        if let Some(l) = lease {
            let _ = self.store.revoke_lease(l).await;
        }
    }
}

#[async_trait]
impl Cluster for GatewayCluster {
    fn node_id(&self) -> i32 {
        i32::from(self.registrar.record().slot)
    }

    fn cluster_id(&self) -> String {
        format!("sprig-{}", self.dc)
    }

    async fn brokers(&self, _p: &Principal) -> Vec<BrokerNode> {
        if let Some((at, b)) = self
            .brokers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && at.elapsed() < BROKERS_TTL
        {
            return b.clone();
        }
        let advertised = self
            .live
            .get(&super::ADVERTISED_HOST, &SettingCtx::process(&self.dc));
        let records = match registry::list(self.store.as_ref()).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "kafka: gateway records unreadable");
                Vec::new()
            }
        };
        let me = self.registrar.record();
        let mut out: Vec<BrokerNode> = records
            .into_iter()
            .filter(|r| r.state == GatewayState::Ready)
            .filter_map(|r| {
                let port = *r.listeners.get("kafka")?;
                let host = if r.gateway_id == me.gateway_id && !advertised.is_empty() {
                    advertised.to_string()
                } else {
                    r.addr.clone()
                };
                Some(BrokerNode {
                    node_id: i32::from(r.slot),
                    host,
                    port: i32::from(port),
                    rack: Some(self.dc.to_string()),
                })
            })
            .collect();
        out.sort_by_key(|b| b.node_id);
        *self.brokers.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Instant::now(), out.clone()));
        out
    }
}

#[async_trait]
impl Leases for GatewayCluster {
    async fn acquire(&self, k: &GroupKey) -> Result<LeaseState, StoreError> {
        let key = self.key(k)?;
        let mut h = self.held.lock().await;
        if h.keys.contains(k) {
            return Ok(LeaseState::Held { epoch: h.epoch });
        }
        let lease = if let Some(l) = h.lease {
            l
        } else {
            let l = self.store.grant_lease(LEASE_TTL).await.map_err(store_err)?;
            h.lease = Some(l);
            h.epoch += 1;
            l
        };
        let r = self
            .store
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: self.value(),
                    lease: Some(lease),
                }],
                otherwise: vec![Op::Get(key)],
            })
            .await
            .map_err(store_err)?;
        if r.succeeded {
            h.keys.insert(k.clone());
            return Ok(LeaseState::Held { epoch: h.epoch });
        }
        let node = match r.results.first() {
            Some(OpResult::Get(Some(kv))) => Self::node_of(&kv.value).unwrap_or(-1),
            _ => -1,
        };
        Ok(LeaseState::Elsewhere(node))
    }

    async fn holder(&self, k: &GroupKey) -> Result<Option<i32>, StoreError> {
        let key = self.key(k)?;
        let kv = self
            .store
            .get(&key, Consistency::Serializable)
            .await
            .map_err(store_err)?;
        Ok(kv.and_then(|kv| Self::node_of(&kv.value)))
    }

    async fn confirm(&self, k: &GroupKey, epoch: u64) -> bool {
        let h = self.held.lock().await;
        h.epoch == epoch && h.keys.contains(k)
    }

    async fn release(&self, k: &GroupKey) {
        let Ok(key) = self.key(k) else { return };
        let mut h = self.held.lock().await;
        if h.keys.remove(k) {
            let _ = self
                .store
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Delete(key)],
                    otherwise: Vec::new(),
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_ids_become_key_segments() {
        assert_eq!(key_segment("billing"), "billing");
        assert_eq!(key_segment("a/b c"), "a%2Fb%20c");
        assert_eq!(key_segment("connect-sink.1_x"), "connect-sink.1_x");
    }
}
