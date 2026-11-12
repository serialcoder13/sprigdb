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
//!
//! **Instance rows** (bugfix-56). Dropping an orphan also deletes its
//! `inherited_instances` row. Rows whose definition is gone but whose
//! sink went earlier (dropped by this sweep before bugfix-56, or by a
//! member's own `DROP SINK`) are found the same way, seen twice and
//! deleted: in the tenants of the sink index on every pass, and in every
//! child tenant of the directory on a gateway's first two passes (the rows
//! older sweeps left; new ones aren't left any more).

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

/// Stale instance rows seen: (member, `def_tenant.def_object`, "row").
fn row_sightings() -> &'static Mutex<Sightings> {
    static S: OnceLock<Mutex<Sightings>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(Sightings::default()))
}

/// Passes that still scan every child tenant for stale rows.
static FULL_ROW_PASSES: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(2);

/// Every child tenant in the directory (`tenants/<id>` with an ancestor).
async fn child_tenants(svc: &Arc<TenantServices>) -> Result<BTreeSet<u64>> {
    use sprig_meta::{ListOpts, Record};
    let prefix = svc.global.tier().key("tenants/")?;
    let mut out = BTreeSet::new();
    let mut opts = ListOpts::default();
    loop {
        let page = svc.global.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            if let Ok(r) = Record::<super::DirEntry>::decode(&kv.value)
                && let [.., _, me] = r.body.path_ids().as_slice()
            {
                out.insert(*me);
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

/// This pass's stale instance rows (module docs) in `tenants`, and the
/// tenants that couldn't be checked.
async fn find_rows(
    svc: &Arc<TenantServices>,
    tenants: &BTreeSet<u64>,
) -> (HashSet<SinkKey>, BTreeSet<u64>) {
    let mut out = HashSet::new();
    let mut failed = BTreeSet::new();
    for t in tenants {
        match super::sinks::stale_instance_rows(svc, TenantId(*t)).await {
            Ok(keys) => {
                for k in keys {
                    out.insert((*t, format!("{}.{}", k.tenant.0, k.object), "row".to_owned()));
                }
            }
            Err(e) => {
                tracing::debug!(tenant = t, error = %e, "instance rows not checked");
                failed.insert(*t);
            }
        }
    }
    (out, failed)
}

/// Deletes the stale instance rows seen twice; returns how many.
async fn sweep_rows(svc: &Arc<TenantServices>, confirm: Duration) -> Result<usize> {
    let mut tenants: BTreeSet<u64> = sprig_sinks::meta::index_list(&svc.locator.dc_store())
        .await?
        .iter()
        .map(|i| i.tenant)
        .collect();
    let full = FULL_ROW_PASSES
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |n| n.checked_sub(1),
        )
        .is_ok();
    if full {
        tenants.extend(child_tenants(svc).await?);
    }
    // Tenants with a row seen before are looked at again, so a row is
    // confirmed by a second look at its own tenant.
    tenants.extend(
        row_sightings()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .keys()
            .map(|k| k.0),
    );
    let (mut seen, failed) = find_rows(svc, &tenants).await;
    let due = {
        let mut s = row_sightings()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // A tenant that couldn't be checked keeps its sightings, and
        // nothing of it is deleted this pass.
        let kept: Vec<SinkKey> =
            s.0.keys()
                .filter(|k| failed.contains(&k.0))
                .cloned()
                .collect();
        seen.extend(kept);
        s.observe(&seen, Instant::now(), confirm.max(CONFIRM_MIN))
            .into_iter()
            .filter(|k| !failed.contains(&k.0))
            .collect::<Vec<_>>()
    };
    let mut n = 0;
    for k in due {
        let (t, def, _) = &k;
        let Some((dt, dobj)) = def
            .split_once('.')
            .and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))
        else {
            continue;
        };
        let key = sprig_inherit::msg::DefKey {
            tenant: TenantId(dt),
            object: dobj,
        };
        match super::sinks::delete_instance_row(svc, TenantId(*t), key).await {
            Ok(()) => {
                tracing::info!(tenant = t, def = %key, "stale inherited sink instance row deleted: its definition is gone");
                n += 1;
                row_sightings()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .forget(&k);
            }
            Err(e) => {
                tracing::warn!(tenant = t, def = %key, error = %e, "stale inherited sink instance row not deleted; tried again next pass");
            }
        }
    }
    Ok(n)
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
    match sweep_rows(svc, confirm).await {
        Ok(n) if n > 0 => {
            metrics::counter!("sprig_inherit_drops_total", "how" => "stale_instance_row")
                .increment(n as u64);
        }
        Ok(_) => {}
        Err(e) => tracing::debug!(error = %e, "stale inherited sink instance rows not swept"),
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
