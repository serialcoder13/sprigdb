//! The sweep for orphaned member sinks (chapter 51, "Inherited sinks and
//! connections"; bugfix-47, r9-streams-queues item 111's "instead").
//!
//! A member's instance of a `FOR EACH TENANT` sink is an ordinary sink of
//! the member, owned by its `sprig_inherited` account. `DROP SINK` on the
//! definition drops each member's instance, but an instance it misses
//! stays running for ever: before r9last's fix, every member of a
//! queue-sourced definition was missed; a member whose drop failed (it is
//! logged and the definition still goes), and a member whose source
//! instance had no index entry any more, still are. Such a sink reads
//! nothing it should, holds leases and, over a queue made again under the
//! same name, takes its messages (item 111).
//!
//! ```text
//! every inherit.orphan_sink_sweep_seconds (600; 0: off), on each gateway:
//!   the tenants in this DC's sink index (sinks/<tenant>/<sink>)
//!   per tenant T with a sprig_inherited account owning sinks:
//!     T's chain (T and its ancestors' definitions); a sink db.name is an
//!     orphan when no sink definition db.name reaches T
//!   an orphan seen in two passes in a row (at least a minute apart) is
//!   dropped as T's sprig_inherited, with its dead letters
//! ```
//!
//! The two passes keep a sink whose definition a snapshot a moment behind
//! doesn't show yet. Every gateway sweeps; a drop another gateway made
//! first is a no-op (`DROP SINK IF EXISTS`).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sprig_catalog::TenantCatalog;
use sprig_common::Result;
use sprig_common::ids::{DbId, TenantId, UserId};
use sprig_inherit::msg::DefKind;
use sprig_sinks::store::SinksState;

use super::{RUN_AS_USER, catalog};
use crate::tenant::TenantServices;

/// The least time between the two sightings that drop an orphan.
pub const CONFIRM_MIN: Duration = Duration::from_secs(60);

/// A member's sink: (tenant, database, name).
pub type SinkKey = (u64, String, String);

/// Orphans seen, and when first.
#[derive(Debug, Default)]
pub struct Sightings(HashMap<SinkKey, Instant>);

impl Sightings {
    /// Records this pass's orphans (`seen`, at `now`); forgets the ones not
    /// seen again. The orphans first seen at least `confirm` ago: drop them.
    pub fn observe(
        &mut self,
        seen: &HashSet<SinkKey>,
        now: Instant,
        confirm: Duration,
    ) -> Vec<SinkKey> {
        self.0.retain(|k, _| seen.contains(k));
        let mut due = Vec::new();
        for k in seen {
            let first = *self.0.entry(k.clone()).or_insert(now);
            if now.saturating_duration_since(first) >= confirm {
                due.push(k.clone());
            }
        }
        due.sort();
        due
    }

    /// Forgets `k` (dropped).
    pub fn forget(&mut self, k: &SinkKey) {
        self.0.remove(k);
    }
}

fn sightings() -> &'static Mutex<Sightings> {
    static S: OnceLock<Mutex<Sightings>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(Sightings::default()))
}

/// `cat`'s `sprig_inherited` account, if it has one.
fn run_as(cat: &TenantCatalog) -> Option<UserId> {
    sprig_auth::cache::TenantAuth::of(cat)
        .and_then(|a| a.accounts(RUN_AS_USER).first().map(|u| u.row.user_id))
}

/// The sinks of `cat` owned by `user`: (database, name).
fn owned_by(cat: &TenantCatalog, user: UserId) -> Vec<(String, String)> {
    let Some(st) = cat.ext.get::<SinksState>(sprig_sinks::store::MODULE) else {
        return Vec::new();
    };
    st.sinks
        .values()
        .filter(|s| s.owner_user_id == user.0)
        .filter_map(|s| {
            cat.databases
                .get(DbId(s.db_id))
                .map(|d| (d.name.clone(), s.name.clone()))
        })
        .collect()
}

/// This pass's orphans: member sinks no definition reaches.
async fn find(svc: &Arc<TenantServices>) -> Result<HashSet<SinkKey>> {
    let index = sprig_sinks::meta::index_list(&svc.locator.dc_store()).await?;
    let tenants: BTreeSet<u64> = index.iter().map(|i| i.tenant).collect();
    let mut out = HashSet::new();
    for t in tenants {
        let Ok(cat) = catalog(svc, TenantId(t)).await else {
            continue;
        };
        let Some(user) = run_as(&cat) else {
            continue;
        };
        let owned = owned_by(&cat, user);
        if owned.is_empty() {
            continue;
        }
        // Every ancestor's catalog, or no verdict for this tenant.
        let Ok(chain) = super::chain(svc, &cat).await else {
            continue;
        };
        for (db, name) in owned {
            if chain.resolve(&db, DefKind::Sink, &name).is_none() {
                out.insert((t, db, name));
            }
        }
    }
    Ok(out)
}

/// One pass (see the module docs); `confirm` is the least time between
/// the two sightings. The orphans dropped.
///
/// # Errors
/// The DC tier's (listing the sink index).
pub async fn sweep(svc: &Arc<TenantServices>, confirm: Duration) -> Result<Vec<SinkKey>> {
    let seen = find(svc).await?;
    let due = sightings()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .observe(&seen, Instant::now(), confirm.max(CONFIRM_MIN));
    let mut dropped = Vec::new();
    for k in due {
        let (t, db, name) = &k;
        let tenant = TenantId(*t);
        let r = async {
            let cat = catalog(svc, tenant).await?;
            let Some(user) = run_as(&cat) else {
                return Ok(false);
            };
            super::sinks::drop_member_sink(svc, tenant, user, db, name).await
        }
        .await;
        match r {
            Ok(true) => {
                tracing::warn!(%tenant, db = %db, sink = %name, "orphaned inherited sink dropped: no FOR EACH TENANT definition reaches the member any more");
                metrics::counter!("sprig_inherit_drops_total", "how" => "orphan_sink").increment(1);
                dropped.push(k.clone());
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(%tenant, db = %db, sink = %name, error = %e, "orphaned inherited sink not dropped; tried again next pass");
                continue;
            }
        }
        sightings()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .forget(&k);
    }
    Ok(dropped)
}

/// Starts this gateway's sweep: every `inherit.orphan_sink_sweep_seconds`
/// (read each time; 0 pauses it). It ends with the services.
pub fn start(svc: &Arc<TenantServices>) {
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        loop {
            let every = {
                let Some(svc) = weak.upgrade() else { return };
                svc.settings.get(
                    &sprig_inherit::msg::settings::ORPHAN_SINK_SWEEP_SECONDS,
                    &sprig_settings::SettingCtx::process(&svc.dc),
                    None,
                )
            };
            if every == 0 {
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
            tokio::time::sleep(Duration::from_secs(every)).await;
            let Some(svc) = weak.upgrade() else { return };
            match Box::pin(sweep(&svc, Duration::from_secs(every))).await {
                Ok(d) if !d.is_empty() => {
                    tracing::info!(dropped = d.len(), "orphaned inherited sinks swept");
                }
                Ok(_) => {}
                Err(e) => tracing::debug!(error = %e, "orphaned inherited sink sweep"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(t: u64, n: &str) -> SinkKey {
        (t, "db".to_owned(), n.to_owned())
    }

    /// An orphan is dropped only when seen in two passes at least
    /// `confirm` apart; one not seen again is forgotten.
    #[test]
    fn an_orphan_is_dropped_on_its_second_sighting() {
        let mut s = Sightings::default();
        let t0 = Instant::now();
        let c = Duration::from_secs(600);
        let pass1: HashSet<SinkKey> = [k(1, "a"), k(1, "b")].into_iter().collect();
        assert!(s.observe(&pass1, t0, c).is_empty(), "first sighting");
        // b's definition shows up (a snapshot was behind): b isn't seen.
        let pass2: HashSet<SinkKey> = [k(1, "a")].into_iter().collect();
        assert!(
            s.observe(&pass2, t0 + Duration::from_secs(300), c)
                .is_empty(),
            "too soon"
        );
        assert_eq!(s.observe(&pass2, t0 + c, c), vec![k(1, "a")]);
        s.forget(&k(1, "a"));
        // b again: a new first sighting.
        let pass3: HashSet<SinkKey> = [k(1, "b")].into_iter().collect();
        assert!(s.observe(&pass3, t0 + c + c, c).is_empty());
        assert_eq!(s.observe(&pass3, t0 + c + c + c, c), vec![k(1, "b")]);
    }
}
