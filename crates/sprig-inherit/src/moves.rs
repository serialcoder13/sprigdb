//! Moving a tenant subtree while shares reach it (chapter 42, "Tenants
//! are spaces": moves). A tenant's root space receives shares only from
//! its parent tenant, so a move is refused with 39361 while the moved
//! root receives a share from a parent that would stop being its
//! ancestor; the administrator drops those shares first. Shares inside the
//! moved subtree are unaffected.
//!
//! Chapter 23's `MoveTenantTree` is not built yet; it calls [`check`]
//! before its step 2, with the snapshots it already reads.

use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::plan::{state_of, tenant_edge};

/// The parent's shares that reach `moved` (one per object it inherits).
#[must_use]
pub fn received(parent: &TenantCatalog, moved: &TenantCatalog) -> Vec<String> {
    let Some(st) = state_of(moved) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for o in st.objects() {
        let Some(db) = moved.databases.get(o.db).map(|d| d.name.clone()) else {
            continue;
        };
        if let Ok(Some(up)) = tenant_edge(parent, moved, &db, &o.table, false)
            && !out.contains(&up.share.name)
        {
            out.push(up.share.name.clone());
        }
    }
    out
}

/// 39361 when `moved`, under `parent` today, would leave `parent` out of
/// its new ancestors (`new_ancestors`, the new parent included) while it
/// receives shares from it.
///
/// # Errors
/// 39361, naming the shares.
pub fn check(
    parent: &TenantCatalog,
    moved: &TenantCatalog,
    new_ancestors: &[TenantId],
) -> Result<()> {
    if new_ancestors.contains(&parent.tenant) {
        return Ok(());
    }
    let names = received(parent, moved);
    if names.is_empty() {
        return Ok(());
    }
    Err(SprigError::new(
        SprigCode::TenantMoveBlocked,
        format!(
            "tenant {} receives shares from tenant {}, which would stop being its ancestor: \
             drop {} first",
            moved.tenant,
            parent.tenant,
            names.join(", ")
        ),
    ))
}
