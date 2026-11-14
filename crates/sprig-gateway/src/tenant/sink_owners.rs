//! Sinks whose owner lost access to their source, checked when a grant
//! change commits (39229).
//!
//! ```text
//! after an account statement commits (GRANT, REVOKE, roles, DROP USER,
//! ACCOUNT LOCK, ...), on the snapshot at its auth version:
//!   each running or failing sink whose owner can no longer read its source
//!     → paused, reason "39229 ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST: <why>"
//!   each sink paused that way whose owner can again → running
//!   (one versioned catalog change; the tasks follow at their next poll)
//! ```
//!
//! Workers check the same before each batch, so a change made another way
//! (a role granted to a role) is caught there too.

use std::sync::Arc;

use sprig_auth::authz::owner::{ReadSource, lost_read};
use sprig_catalog::TenantCatalog;
use sprig_catalog::version::VersionKind;
use sprig_common::ids::UserId;
use sprig_sinks::job::{OWNER_PAUSE, OwnerRead, owner_pause_reason, owner_reads};
use sprig_sinks::model::{SinkId, SinkState};
use sprig_sinks::sql::Change;
use sprig_sinks::store::SinksState;

use super::TenantServices;

/// The state changes the grants in `cat` call for (see the module docs).
#[must_use]
pub fn changes(cat: &TenantCatalog, lctn: bool) -> Vec<(SinkId, SinkState, Option<String>)> {
    let Some(st) = cat.ext.get::<SinksState>(sprig_sinks::store::MODULE) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for def in st.sinks.values() {
        let paused = def.state == SinkState::Paused
            && def
                .state_reason
                .as_deref()
                .is_some_and(|r| r.starts_with(OWNER_PAUSE));
        let active = matches!(def.state, SinkState::Running | SinkState::Failing);
        if !paused && !active {
            continue;
        }
        let reads: Vec<ReadSource> = owner_reads(def)
            .into_iter()
            .map(|r| match r {
                OwnerRead::Named { db, name } => ReadSource::Named { db, name },
                OwnerRead::Object(id) => ReadSource::Object(id),
                OwnerRead::InDatabase { db, name } => ReadSource::InDatabase { db, name },
                OwnerRead::FanIn => ReadSource::FanIn,
            })
            .collect();
        if reads.is_empty() {
            continue;
        }
        match (
            paused,
            lost_read(cat, UserId(def.owner_user_id), &reads, lctn),
        ) {
            (false, Some(why)) => {
                out.push((def.id, SinkState::Paused, Some(owner_pause_reason(&why))));
            }
            (true, None) => out.push((def.id, SinkState::Running, None)),
            _ => {}
        }
    }
    out
}

/// Applies [`changes`] for `cat` in one catalog change. Failures are
/// logged: the workers' own checks still pause the sinks.
pub async fn after_grants(svc: &Arc<TenantServices>, cat: &TenantCatalog, lctn: bool) {
    let todo = changes(cat, lctn);
    if todo.is_empty() {
        return;
    }
    let tenant = cat.tenant;
    let r: sprig_common::Result<u64> = async {
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let applied = {
            let (store, tx) = c.tx().parts();
            let mut r = Ok(());
            for (id, state, reason) in &todo {
                let ch = Change::State(*id, *state, reason.clone());
                r = sprig_sinks::sql::apply(tx, store.schema(), &ch, None, tenant, 0).await;
                if r.is_err() {
                    break;
                }
            }
            r
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        c.commit().await
    }
    .await;
    match r {
        Ok(_) => {
            for (id, state, reason) in &todo {
                if *state == SinkState::Paused {
                    tracing::warn!(tenant = %tenant, sink = id.0, reason = reason.as_deref().unwrap_or_default(), code = 39229, "SprigSinkPaused: the sink's owner lost access to its source");
                } else {
                    tracing::info!(tenant = %tenant, sink = id.0, "sink resumed: its owner can read its source again");
                }
            }
        }
        Err(e) => {
            tracing::warn!(tenant = %tenant, error = %e, "sink owner check after a grant change failed; the workers check before each batch");
        }
    }
}
