//! The end of a definition and of an instance (chapter 42, "Dropping a
//! definition"):
//!
//! ```text
//! remove_all(D):   every member's instance of D goes: those the DC indexes,
//!                  then any row the index never had (an instance stuck in
//!                  'creating' after a crash), found by asking each member
//!                  of D in the directory
//! grace end:       DROP without IMMEDIATE lists D under inherit-dropping/<D>
//!                  with its drop_after; every gateway looks every 30 s, and
//!                  the first past drop_after removes every instance, then the
//!                  definition row, then the entry
//! detach(T, D):    ALTER INHERITED kind s DETACH in member T: T's instance
//!                  row becomes 'detached' and leaves the index; the stream
//!                  (exchange, queue) stays as T's own object with its data;
//!                  T's inherited triggers on it stop; T's catalog version is
//!                  raised so every gateway reads the row
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::CatalogLocator;
use sprig_catalog::version::VersionKind;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::store::MsgStore;
use sprig_inherit::msg::{DefKey, DefKind, Definition, InstanceState};
use sprig_meta::{ListOpts, Record, Versioned};

use super::catalog;
use crate::tenant::TenantServices;

fn cat_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

/// Removes member `t`'s instance of `def` (its physical part, row and index
/// entry), whatever its state but `detached`.
async fn remove_one(svc: &Arc<TenantServices>, t: TenantId, def: &Definition) -> Result<()> {
    match def.kind {
        DefKind::Stream => super::streams::remove(svc, t, def.key()).await,
        DefKind::Exchange | DefKind::Queue => super::mq::remove(svc, t, def).await,
        _ => Ok(()),
    }
}

/// Every member's instance of `def` (see the module docs): the number
/// removed.
///
/// # Errors
/// The catalogs'.
pub async fn remove_all(svc: &Arc<TenantServices>, def: &Definition) -> Result<usize> {
    let mut done = BTreeSet::new();
    for e in super::instances_of(svc, def.key()).await? {
        // A member deleted since: only its index entry is left.
        if super::dir_entry(svc, e.tenant).await?.is_none() {
            super::delete_index(svc, e.tenant, def.key()).await?;
            continue;
        }
        remove_one(svc, e.tenant, def).await?;
        done.insert(e.tenant);
    }
    // Rows the index never had: instances stuck in 'creating'.
    let members = super::broadcast::members(
        svc,
        def,
        def.declarer,
        super::broadcast::Form::default(),
        u64::MAX,
    )
    .await?;
    for t in members {
        if done.contains(&t) {
            continue;
        }
        let ep = svc.locator.writer(t).await?;
        let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
        let st = MsgStore::new(ep.store.schema());
        if !st.migrated(&mut conn).await? {
            continue;
        }
        if let Some(i) = st.instance(&mut conn, def.key()).await?
            && i.state != InstanceState::Detached
        {
            tracing::info!(tenant = %t, def = %def.key(), state = i.state.as_str(), "removing an instance the index didn't list");
            remove_one(svc, t, def).await?;
            done.insert(t);
        }
    }
    Ok(done.len())
}

/// `inherit-dropping/<def>`: a definition in its drop grace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dropping {
    /// The definition.
    pub def: DefKey,
    /// The end of the grace, ms since the epoch.
    pub drop_after_ms: u64,
}

impl Versioned for Dropping {
    const V: u32 = 1;
}

/// Lists `def` as dropping until `drop_after_ms`.
///
/// # Errors
/// The DC tier's.
pub async fn list_dropping(svc: &TenantServices, def: DefKey, drop_after_ms: u64) -> Result<()> {
    let store = svc.locator.dc_store();
    let key = store.tier().key(&format!("inherit-dropping/{def}"))?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key,
                value: Record::new(Dropping { def, drop_after_ms }).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

async fn unlist(svc: &TenantServices, def: DefKey) -> Result<()> {
    let store = svc.locator.dc_store();
    let key = store.tier().key(&format!("inherit-dropping/{def}"))?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Delete(key)],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// One pass over the dropping definitions: those past their grace go.
/// The number removed.
///
/// # Errors
/// The DC tier's.
pub async fn sweep(svc: &Arc<TenantServices>) -> Result<usize> {
    let store = svc.locator.dc_store();
    let prefix = store.tier().key("inherit-dropping/")?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    let now = sprig_inherit::msg::store::now_ms();
    let mut n = 0;
    for kv in page.kvs {
        let Ok(r) = Record::<Dropping>::decode(&kv.value) else {
            continue;
        };
        let d = r.body;
        if d.drop_after_ms > now {
            continue;
        }
        let r = async {
            let cat = catalog(svc, d.def.tenant).await?;
            let def = super::state_of(&cat).def_by_id(d.def.object).cloned();
            if let Some(def) = def {
                // Claim it: the definition goes from the catalog first,
                // so another gateway's pass finds nothing to do.
                let removed = remove_all(svc, &def).await?;
                let v = super::commit(
                    svc,
                    d.def.tenant,
                    &[super::MsgWrite::DeleteDef(d.def.object)],
                )
                .await?;
                let _ = v;
                metrics::counter!("sprig_inherit_drops_total", "how" => "grace").increment(1);
                tracing::info!(def = %d.def, removed, "drop grace ended: instances and definition removed");
            }
            unlist(svc, d.def).await
        }
        .await;
        match r {
            Ok(()) => n += 1,
            Err(e) => {
                tracing::warn!(def = %d.def, error = %e, "drop grace end not finished; retried");
            }
        }
    }
    Ok(n)
}

/// Starts this gateway's drop-grace pass: every 30 s.
pub fn start_sweeper(svc: &Arc<TenantServices>) {
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let Some(svc) = weak.upgrade() else { return };
            if let Err(e) = sweep(&svc).await {
                tracing::debug!(error = %e, "drop grace pass");
            }
        }
    });
}

/// `ALTER INHERITED kind name DETACH` in member `t` (see the module docs).
///
/// # Errors
/// 39897 for a kind that can't be detached; the catalog's.
pub async fn detach(svc: &Arc<TenantServices>, t: TenantId, def: &Definition) -> Result<usize> {
    if !matches!(
        def.kind,
        DefKind::Stream | DefKind::Exchange | DefKind::Queue
    ) {
        return Err(super::not_overridable(
            "only a stream, exchange or queue instance can be detached",
        ));
    }
    let ep = svc.locator.writer(t).await?;
    let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
    let st = MsgStore::new(ep.store.schema());
    let Some(i) = st.instance(&mut conn, def.key()).await? else {
        return Err(super::target_err(format!(
            "this tenant has no instance of {} {}.{} to detach",
            def.kind.as_str(),
            def.db_name,
            def.name
        )));
    };
    st.set_instance(
        &mut conn,
        def.key(),
        InstanceState::Detached,
        i.def_version,
        &i.definition,
    )
    .await?;
    super::delete_index(svc, t, def.key()).await?;
    // The member's inherited triggers reading it stop.
    let cat = catalog(svc, t).await?;
    let chain = super::chain(svc, &cat).await?;
    let triggers = match def.kind {
        DefKind::Stream => super::triggers::on_stream(&chain, def),
        DefKind::Queue => super::triggers::on_queue(&chain, def),
        _ => Vec::new(),
    };
    let mut stopped = 0;
    for g in triggers {
        if let Some(gi) = st.instance(&mut conn, g.key()).await? {
            if let Some(b) = gi
                .physical
                .as_ref()
                .and_then(|p| p.get("binding"))
                .and_then(Value::as_u64)
            {
                sprig_logic::messages::delete_index(svc.locator.dc_store().as_ref(), t, b).await?;
            }
            st.delete_instance(&mut conn, g.key()).await?;
            stopped += 1;
        }
    }
    // Every gateway reads the row at the member's next version.
    let c = svc.catalog.change(t, VersionKind::Catalog).await?;
    c.commit().await?;
    super::streams::forget_applied(t, def.key());
    super::mq::forget_applied(t, Some(&def.db_name));
    metrics::counter!("sprig_inherit_drops_total", "how" => "detached").increment(1);
    Ok(stopped)
}
