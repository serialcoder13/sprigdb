//! etcd endpoint discovery, so endpoints change without restarts.
//!
//! ```text
//! discover_members(tier), every control.etcd_member_refresh_seconds and on any
//! connection error:
//! 1. MemberList on any reachable endpoint (seeds first, then the last known list)
//! 2. endpoints = client URLs of started, non-learner members
//! 3. hand the new list to the etcd client; keep it in memory as the next fallback
//! ```
//!
//! So adding or removing an etcd member changes no `sprigdb.toml`, as long
//! as one seed stays reachable at start-up.
//!
//! **As built.** The client's balanced channel takes endpoints one by one:
//! a member's URL is added once it answers a status call from this
//! process (a member can advertise a URL this host can't reach, as
//! Compose's `http://etcd:2379` is from the host), and an endpoint added
//! by discovery is removed when its member leaves the list. Seeds are never
//! removed: a seed may name a member under another URL than it advertises.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use etcd_client::{Client, ConnectOptions};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};

use crate::etcd::EtcdStore;

/// What one refresh did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Refresh {
    /// The client URLs of started, non-learner members.
    pub members: Vec<String>,
    /// Endpoints handed to the client now.
    pub added: Vec<String>,
    /// Endpoints taken from the client now.
    pub removed: Vec<String>,
}

/// Follows an etcd cluster's membership for one [`EtcdStore`].
pub struct Discovery {
    store: EtcdStore,
    seeds: BTreeSet<String>,
    /// Endpoints discovery added (never seeds).
    added: Mutex<BTreeSet<String>>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("seeds", &self.seeds)
            .field(
                "added",
                &*self.added.lock().unwrap_or_else(PoisonError::into_inner),
            )
            .finish_non_exhaustive()
    }
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::new(SprigCode::MetaUnavailable, format!("etcd {what}: {e}"))
}

impl Discovery {
    /// Discovery for `store`, whose seeds are its configured endpoints.
    #[must_use]
    pub fn new(store: &EtcdStore) -> Self {
        Self {
            seeds: store.config().endpoints.iter().cloned().collect(),
            store: store.clone(),
            added: Mutex::new(BTreeSet::new()),
        }
    }

    /// Whether `url` answers a status call from here.
    async fn reachable(&self, url: &str) -> bool {
        let cfg = self.store.config();
        let mut opts = ConnectOptions::new().with_connect_timeout(cfg.connect_timeout);
        if let Some(tls) = &cfg.tls {
            opts = opts.with_tls(tls.options());
        }
        let probe = async {
            let mut c = Client::connect([url], Some(opts)).await.ok()?;
            c.status().await.ok()
        };
        matches!(
            tokio::time::timeout(cfg.request_timeout, probe).await,
            Ok(Some(_))
        )
    }

    /// One refresh: list the members, add reachable new ones, remove
    /// discovered ones that left.
    ///
    /// # Errors
    /// 39500 when no endpoint answers `MemberList`.
    pub async fn refresh(&self) -> Result<Refresh> {
        let mut client = self.store.client().clone();
        let timeout = self.store.config().request_timeout;
        let list = tokio::time::timeout(timeout, client.member_list())
            .await
            .map_err(|_| unavailable("member list", "timed out"))?
            .map_err(|e| unavailable("member list", e))?;
        let members: BTreeSet<String> = list
            .members()
            .iter()
            .filter(|m| !m.name().is_empty() && !m.is_learner())
            .flat_map(|m| m.client_urls().iter().cloned())
            .filter(|u| {
                // TLS clients only use https:// members, plain ones http://.
                let tls = self.store.config().tls.is_some();
                u.starts_with(if tls { "https://" } else { "http://" })
            })
            .collect();
        let known: BTreeSet<String> = {
            let added = self.added.lock().unwrap_or_else(PoisonError::into_inner);
            self.seeds.union(&added).cloned().collect()
        };
        let mut out = Refresh {
            members: members.iter().cloned().collect(),
            ..Refresh::default()
        };
        for url in members.difference(&known) {
            if self.reachable(url).await {
                self.store
                    .client()
                    .add_endpoint(url)
                    .await
                    .map_err(|e| unavailable("add endpoint", e))?;
                self.added
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(url.clone());
                out.added.push(url.clone());
            }
        }
        let gone: Vec<String> = self
            .added
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|u| !members.contains(*u))
            .cloned()
            .collect();
        for url in gone {
            self.store
                .client()
                .remove_endpoint(&url)
                .await
                .map_err(|e| unavailable("remove endpoint", e))?;
            self.added
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&url);
            out.removed.push(url);
        }
        for u in &out.added {
            metrics::counter!("sprig_meta_reconfigurations_total", "what" => "etcd_member", "result" => "added").increment(1);
            tracing::info!(endpoint = %u, "etcd member added to the client");
        }
        for u in &out.removed {
            metrics::counter!("sprig_meta_reconfigurations_total", "what" => "etcd_member", "result" => "removed").increment(1);
            tracing::info!(endpoint = %u, "etcd member removed from the client");
        }
        #[allow(clippy::cast_precision_loss)] // a handful of members
        metrics::gauge!("sprig_meta_members", "tier" => self.store_tier_label())
            .set(members.len() as f64);
        Ok(out)
    }

    fn store_tier_label(&self) -> &'static str {
        match crate::MetaStore::tier(&self.store) {
            crate::Tier::Global => "g",
            crate::Tier::Dc(_) => "dc",
        }
    }

    /// Refreshes every `period` (`control.etcd_member_refresh_seconds`,
    /// read through `period` each round) until `stop`.
    pub fn spawn(
        self: Arc<Self>,
        period: impl Fn() -> Duration + Send + 'static,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(period()) => {}
                }
                if let Err(e) = self.refresh().await {
                    tracing::warn!(error = %e, "etcd member discovery failed; keeping the current endpoints");
                }
            }
        })
    }
}
