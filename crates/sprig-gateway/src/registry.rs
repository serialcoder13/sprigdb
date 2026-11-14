//! Registration in the DC tier: registration records, slots for `KILL`,
//! and write holds.
//!
//! ```text
//! register():
//!   lease = meta.dc.grant_lease(gateway.lease_ttl_seconds)
//!   claim a free slot with create_if_absent("gateway-slots/" + slot, id, lease)
//!   put("gateways/" + id, record, lease); keepalive every ttl / 3
//! on lease loss: register() again
//! ```
//!
//! The slot is the top 12 bits of every MySQL connection id this gateway
//! hands out ([`SessionTable::set_slot`]), so `KILL` of an id on another
//! gateway finds its owner through `gateway-slots/<slot>`. The record
//! carries the listeners' ports, the state (`starting`, `ready`,
//! `draining`), the authorization engines the build knows and a load
//! report, rewritten every [`LOAD_PERIOD`] and on
//! every state change. A re-registration after a lost lease tries the same
//! slot first; sessions opened under the old slot keep their ids.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_meta::session::SessionOpts;
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Session, Txn, Versioned};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};
use tokio::sync::watch;

use crate::session::{MAX_SLOT, SessionTable};

/// `gateway.lease_ttl_seconds`.
pub static LEASE_TTL_SECONDS: Setting<u64> = Setting {
    key: "gateway.lease_ttl_seconds",
    default: 10,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(2, 300),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 4,
    doc: "Registration lease; used at the next registration",
};
sprig_settings::register!(LEASE_TTL_SECONDS);

/// `gateway-processes/<token>` in the DC tier: a running gateway
/// process (its [`process_token`](sprig_catalog::ddl::process_token)),
/// under the registration lease; the value is the gateway's id. A DDL job
/// whose owner's key is gone was abandoned.
pub const PROCESS_PREFIX: &str = sprig_catalog::ddl::GATEWAY_PROCESS_PREFIX;

/// How often the load report is rewritten.
pub const LOAD_PERIOD: Duration = Duration::from_secs(5);
/// Wait before registering again after a failure.
const RETRY: Duration = Duration::from_secs(1);

/// A gateway's state in its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayState {
    /// Started, not yet serving.
    Starting,
    /// Serving.
    Ready,
    /// Draining: load balancers should send new connections elsewhere.
    Draining,
}

/// The load a gateway reports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Load {
    /// Open client sessions.
    pub connections: u64,
    /// Sessions running a statement now.
    pub active: u64,
    /// Process CPU in percent, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_pct: Option<u32>,
}

/// `/sprig/dc/<dc_id>/gateways/<gateway_id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayRecord {
    /// The gateway's id (`[node] id`).
    pub gateway_id: String,
    /// Its connection-id slot.
    pub slot: u16,
    /// Its build.
    pub version: String,
    /// The host other gateways and load balancers reach it at.
    pub addr: String,
    /// Port per running listener (`mysql`, `http`, `peer`, ...).
    #[serde(default)]
    pub listeners: BTreeMap<String, u16>,
    /// Its state.
    pub state: GatewayState,
    /// Catalog formats this build reads (for the catalog's migration gate).
    #[serde(default)]
    pub catalog_formats: Vec<u32>,
    /// Authorization engines this build knows (`auth.authorizer` values,
    /// [`sprig_auth::authz::authorizer::KNOWN`]): a change of
    /// `auth.authorizer` is refused unless every live gateway lists the
    /// engine (`sprig_auth::authz::engine_guard`).
    #[serde(default)]
    pub authorizers: Vec<String>,
    /// Its load.
    #[serde(default)]
    pub load: Load,
    /// The host Kafka clients reach it at (`[gateway]
    /// kafka_advertise_host`), given for it in Kafka's
    /// Metadata. Unset: `addr`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_host: Option<String>,
}

impl Versioned for GatewayRecord {
    const V: u32 = 1;
}

/// `gateway-slots/<slot>`: the gateway holding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotRecord {
    /// The holder.
    pub gateway_id: String,
}

impl Versioned for SlotRecord {
    const V: u32 = 1;
}

/// Reads one gateway's record.
///
/// # Errors
/// The store's errors.
pub async fn get(store: &dyn MetaStore, id: &str) -> Result<Option<GatewayRecord>> {
    let key = store.tier().key(&format!("gateways/{id}"))?;
    match store.get(&key, Consistency::Linearizable).await? {
        Some(kv) => Ok(Some(Record::<GatewayRecord>::decode(&kv.value)?.body)),
        None => Ok(None),
    }
}

/// The gateway holding `slot`.
///
/// # Errors
/// The store's errors.
pub async fn slot_holder(store: &dyn MetaStore, slot: u16) -> Result<Option<GatewayRecord>> {
    let key = store.tier().key(&format!("gateway-slots/{slot}"))?;
    let Some(kv) = store.get(&key, Consistency::Linearizable).await? else {
        return Ok(None);
    };
    let holder = Record::<SlotRecord>::decode(&kv.value)?.body.gateway_id;
    get(store, &holder).await
}

/// Every registered gateway of the DC, by id.
///
/// # Errors
/// The store's errors.
pub async fn list(store: &dyn MetaStore) -> Result<Vec<GatewayRecord>> {
    let prefix = store.tier().key("gateways/")?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            match Record::<GatewayRecord>::decode(&kv.value) {
                Ok(r) => out.push(r.body),
                Err(e) => tracing::warn!(key = %kv.key, error = %e, "unreadable gateway record"),
            }
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    Ok(out)
}

struct Shared {
    record: Mutex<GatewayRecord>,
    changed: watch::Sender<u64>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, GatewayRecord> {
        self.record.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn bump(&self) {
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
}

/// This gateway's registration: the record it publishes and the task that
/// keeps it.
#[derive(Clone)]
pub struct Registrar {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Registrar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registrar")
            .field("record", &*self.shared.lock())
            .finish()
    }
}

impl Registrar {
    /// A registrar for a gateway described by `record` (state `starting`).
    #[must_use]
    pub fn new(record: GatewayRecord) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            shared: Arc::new(Shared {
                record: Mutex::new(record),
                changed,
            }),
        }
    }

    /// The record as it is now.
    #[must_use]
    pub fn record(&self) -> GatewayRecord {
        self.shared.lock().clone()
    }

    /// Changes the state; the record is rewritten at once.
    pub fn set_state(&self, state: GatewayState) {
        self.shared.lock().state = state;
        self.shared.bump();
    }

    /// Changes the listener ports; the record is rewritten at once.
    pub fn set_listeners(&self, listeners: BTreeMap<String, u16>) {
        self.shared.lock().listeners = listeners;
        self.shared.bump();
    }

    /// Updates the load report (written with the next periodic write).
    pub fn set_load(&self, load: Load) {
        self.shared.lock().load = load;
    }

    /// Registers in `store`, then keeps the registration until `stop`,
    /// registering again whenever the lease is lost. `ttl` is read at each
    /// registration. Returns once the first registration succeeded (or
    /// `stop` fired), with the task that keeps it.
    ///
    /// # Errors
    /// None: failures are logged and retried.
    pub async fn start(
        &self,
        store: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        sessions: Arc<SessionTable>,
        ttl: impl Fn() -> Duration + Send + Sync + 'static,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let this = self.clone();
        let task = tokio::spawn(async move {
            let mut first = Some(first_tx);
            loop {
                let session = tokio::select! {
                    () = stop.cancelled() => break,
                    r = this.register_once(&store, &clock, &sessions, ttl()) => r,
                };
                let session = match session {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "gateway registration failed; retrying");
                        tokio::select! {
                            () = stop.cancelled() => break,
                            () = tokio::time::sleep(RETRY) => continue,
                        }
                    }
                };
                if let Some(tx) = first.take() {
                    let _ = tx.send(());
                }
                let lost = this.keep(&session, &stop).await;
                if !lost {
                    // Stopping: give the slot and the record back now.
                    let _ = session.close().await;
                    break;
                }
                metrics::counter!("sprig_gateway_lease_lost_total").increment(1);
                tracing::warn!(
                    event = "gateway.lease_lost",
                    "registration lease lost; registering again"
                );
            }
        });
        let _ = first_rx.await;
        task
    }

    /// One registration: a lease, a slot, the record.
    async fn register_once(
        &self,
        store: &Arc<dyn MetaStore>,
        clock: &Arc<dyn Clock>,
        sessions: &SessionTable,
        ttl: Duration,
    ) -> Result<Session> {
        let session =
            Session::open(Arc::clone(store), Arc::clone(clock), SessionOpts::new(ttl)).await?;
        let id = self.record().gateway_id;
        let preferred = sessions.slot();
        let slot = match claim_slot(store.as_ref(), &session, &id, preferred).await {
            Ok(s) => s,
            Err(e) => {
                let _ = session.close().await;
                return Err(e);
            }
        };
        sessions.set_slot(slot)?;
        self.shared.lock().slot = slot;
        self.write(&session).await?;
        tracing::info!(gateway = %id, slot, "registered");
        Ok(session)
    }

    /// Keeps the record current until the session is lost (`true`) or
    /// `stop` fires (`false`).
    async fn keep(&self, session: &Session, stop: &CancellationToken) -> bool {
        let mut changed = self.shared.changed.subscribe();
        let mut tick = tokio::time::interval(LOAD_PERIOD);
        tick.tick().await;
        loop {
            tokio::select! {
                () = stop.cancelled() => return false,
                () = session.lost() => return true,
                _ = tick.tick() => {}
                r = changed.changed() => if r.is_err() { return false },
            }
            if let Err(e) = self.write(session).await {
                tracing::warn!(error = %e, "can't update the gateway record");
            }
        }
    }

    async fn write(&self, session: &Session) -> Result<()> {
        let store = session.store();
        let rec = self.record();
        let key = store.tier().key(&format!("gateways/{}", rec.gateway_id))?;
        // The write sketches, when a minute completed.
        let load = crate::write_load::report_op(store.as_ref(), &rec.gateway_id, session.lease())?;
        let id = rec.gateway_id.clone();
        let mut then = vec![Op::Put {
            key,
            value: Record::new(rec).encode()?,
            lease: Some(session.lease()),
        }];
        then.extend(load.as_ref().map(|(op, _)| op.clone()));
        // This process's token under the same lease, so another
        // gateway can tell its DDL jobs were abandoned once it is gone.
        then.push(Op::Put {
            key: store.tier().key(&format!(
                "{PROCESS_PREFIX}{}",
                sprig_catalog::ddl::process_token()
            ))?,
            value: id.clone().into_bytes(),
            lease: Some(session.lease()),
        });
        store
            .txn(Txn {
                when: vec![],
                then,
                otherwise: vec![],
            })
            .await?;
        if let Some((_, mark)) = load {
            crate::write_load::written(&id, mark);
        }
        Ok(())
    }
}

/// Claims a free slot for `id` under the session's lease: `preferred`
/// first when it is not 0, then the lowest free one.
async fn claim_slot(
    store: &dyn MetaStore,
    session: &Session,
    id: &str,
    preferred: u16,
) -> Result<u16> {
    let prefix = store.tier().key("gateway-slots/")?;
    let mut taken = std::collections::HashSet::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            if let Some(n) = kv
                .key
                .as_str()
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<u16>().ok())
            {
                // A slot this gateway still holds from an older lease can be
                // taken over.
                let mine =
                    Record::<SlotRecord>::decode(&kv.value).is_ok_and(|r| r.body.gateway_id == id);
                if !mine {
                    taken.insert(n);
                }
            }
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    let candidates = std::iter::once(preferred)
        .filter(|p| *p != 0)
        .chain(1..=MAX_SLOT)
        .filter(|s| !taken.contains(s));
    for slot in candidates {
        let key = store.tier().key(&format!("gateway-slots/{slot}"))?;
        let value = Record::new(SlotRecord {
            gateway_id: id.to_owned(),
        })
        .encode()?;
        let current = store.get(&key, Consistency::Linearizable).await?;
        let when = match &current {
            None => vec![Cmp::Missing(key.clone())],
            Some(kv) => {
                let mine =
                    Record::<SlotRecord>::decode(&kv.value).is_ok_and(|r| r.body.gateway_id == id);
                if !mine {
                    continue;
                }
                vec![Cmp::ModRev(key.clone(), kv.mod_revision)]
            }
        };
        let r = store
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key,
                    value,
                    lease: Some(session.lease()),
                }],
                otherwise: vec![],
            })
            .await?;
        if r.succeeded {
            return Ok(slot);
        }
    }
    Err(SprigError::internal(format!(
        "no free gateway slot in the DC ({MAX_SLOT} in use)"
    )))
}

#[cfg(test)]
mod tests;
