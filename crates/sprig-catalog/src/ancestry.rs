//! Ancestry and ancestors' settings in a catalog (chapter 08, "Ancestry and
//! ancestors' settings"; build step 6, the catalog side).
//!
//! A catalog knows its ancestry from `sprig.ancestry` (ancestors' ids and
//! names, root first, parent last) and holds a read-only copy of every
//! ancestor's tenant-scope rows (`scope_kind = 'ancestor'`), so a tenant's
//! settings resolve from its own snapshot even when an ancestor's home DC
//! is unreachable. `sprig.ancestor_versions` records, per ancestor, the
//! `settings_version` its copy is at.
//!
//! [`apply`] is the commit of chapter 08's `tree_settings` reconcile:
//!
//! ```text
//! change(d, Catalog):                          // one version for the batch
//!   replace d's ancestor rows of each such x; delete rows of tenants no longer in want
//!   sprig.ancestry = want; sprig.ancestor_versions[x] = loaded versions
//! ```
//!
//! "Delete" is a reset: a removed row stays as an empty row carrying the
//! change's version, so incremental cache loads see it go (purge removes
//! it later), as for every other reset. Rows whose value and bounds didn't
//! change are left alone, so a copy rewrites only what moved.
//!
//! The directory side (walking parent links, watching `settings_version`,
//! reading ancestors' catalogs) is `sprig-controller::tree_settings`.
//!
//! Ancestry modes (`tenant.ancestry_mode`, decided 7 November 2026): only
//! `copy` is implemented, and `auto` behaves as `copy`. TODO(chapter 08
//! step 18): `shared` (one tree-settings record per ancestor per DC),
//! `reference` (gateways read ancestors' rows from their home DC; 39085
//! when unreachable with a cold cache), `auto`'s size and residency rules
//! (`tenant.copy_max_descendants`), the residency check (39086) and mode
//! switches.

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use serde_json::Value as Json;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};

use crate::cache::snapshot::{ANCESTRY_KEY, parse_ancestry};
use crate::cache::TenantRef;
use crate::rows::SettingsTx;
use crate::store::{SettingRow, SettingScope};

/// The internal key holding, per ancestor, the `settings_version` its
/// copied rows are at: `{"<tenant id>": <version>, ...}`.
pub const ANCESTOR_VERSIONS_KEY: &str = "sprig.ancestor_versions";

/// `set_by` of copied rows that clear a removed ancestor row.
const SYSTEM: &str = "system";

/// What a catalog holds about its ancestors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AncestryState {
    /// `sprig.ancestry`, root first; `None` if the key is missing (a
    /// catalog the tree loop hasn't reached yet).
    pub ancestry: Option<Vec<TenantRef>>,
    /// `sprig.ancestor_versions`.
    pub versions: BTreeMap<TenantId, u64>,
}

impl AncestryState {
    /// From the two internal keys' values.
    #[must_use]
    pub fn from_values(tenant: TenantId, ancestry: Option<&Json>, versions: Option<&Json>) -> Self {
        Self {
            ancestry: ancestry.map(|v| parse_ancestry(tenant, Some(v)).to_vec()),
            versions: versions.map(parse_versions).unwrap_or_default(),
        }
    }

    /// Reads the two internal keys inside a change.
    ///
    /// # Errors
    /// The transaction's errors.
    pub async fn read<T: SettingsTx + ?Sized>(tx: &mut T, tenant: TenantId) -> Result<Self> {
        let a = tx.setting(SettingScope::Tenant, ANCESTRY_KEY).await?;
        let v = tx.setting(SettingScope::Tenant, ANCESTOR_VERSIONS_KEY).await?;
        Ok(Self::from_values(
            tenant,
            a.as_ref().and_then(|r| r.value.as_ref()),
            v.as_ref().and_then(|r| r.value.as_ref()),
        ))
    }

    /// The ancestors' ids, root first (empty without the key).
    #[must_use]
    pub fn ids(&self) -> Vec<TenantId> {
        self.ancestry
            .iter()
            .flatten()
            .map(|r| r.id)
            .collect()
    }

    /// Whether the copy is current for `want` (root first) given each
    /// ancestor's directory `settings_version`: the same ancestry, every
    /// ancestor copied at its current version, and no other tenant's rows
    /// recorded.
    #[must_use]
    pub fn is_current(&self, want: &[(TenantRef, u64)]) -> bool {
        let same_tree = self.ancestry.as_deref().is_some_and(|a| {
            a.len() == want.len() && a.iter().zip(want).all(|(x, (w, _))| x == w)
        });
        same_tree
            && self.versions.len() == want.len()
            && want
                .iter()
                .all(|(w, sv)| self.versions.get(&w.id).is_some_and(|v| v >= sv))
    }

    /// The ancestors in `want` whose copy is older than their directory
    /// `settings_version`, or missing.
    #[must_use]
    pub fn stale(&self, want: &[(TenantRef, u64)]) -> Vec<TenantId> {
        want.iter()
            .filter(|(w, sv)| self.versions.get(&w.id).is_none_or(|v| v < sv))
            .map(|(w, _)| w.id)
            .collect()
    }
}

fn parse_versions(v: &Json) -> BTreeMap<TenantId, u64> {
    v.as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((TenantId(k.parse().ok()?), v.as_u64()?)))
                .collect()
        })
        .unwrap_or_default()
}

fn versions_json(v: &BTreeMap<TenantId, u64>) -> Json {
    Json::Object(
        v.iter()
            .map(|(t, n)| (t.0.to_string(), Json::from(*n)))
            .collect(),
    )
}

fn ancestry_json(a: &[TenantRef]) -> Result<Json> {
    serde_json::to_value(a).map_err(|e| SprigError::internal(format!("encoding ancestry: {e}")))
}

/// One ancestor's tenant-scope rows, as read from its catalog for a copy.
#[derive(Debug, Clone, PartialEq)]
pub struct AncestorRows {
    /// The ancestor.
    pub tenant: TenantId,
    /// Its directory `settings_version` when the rows were read; the rows
    /// were read at a catalog version at or above it.
    pub settings_version: u64,
    /// Its settings rows with a value or bounds (no internal keys, no
    /// empty rows), at tenant scope as stored in its catalog.
    pub rows: Vec<SettingRow>,
}

/// What [`apply`] changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AncestryReport {
    /// Ancestors whose rows were (re)copied.
    pub copied: Vec<TenantId>,
    /// Former ancestors whose rows were cleared.
    pub removed: Vec<TenantId>,
    /// Rows inserted or replaced.
    pub rows_written: usize,
    /// Rows cleared.
    pub rows_cleared: usize,
    /// Whether `sprig.ancestry` or `sprig.ancestor_versions` changed.
    pub keys_written: bool,
}

impl AncestryReport {
    /// Whether anything was written.
    #[must_use]
    pub fn changed(&self) -> bool {
        self.rows_written > 0 || self.rows_cleared > 0 || self.keys_written
    }
}

/// Inside a change at `version`: makes the catalog's ancestry `want` (root
/// first, parent last), replaces the ancestor rows of every tenant in
/// `fresh` with its rows there, and clears the ancestor rows of tenants no
/// longer in `want`. `sprig.ancestry` and `sprig.ancestor_versions` are
/// written when they change, and always when missing, so every catalog the
/// loop has reached has both.
///
/// # Errors
/// `Internal` if `fresh` names a tenant that isn't in `want`; the
/// transaction's errors.
pub async fn apply<T: SettingsTx + ?Sized>(
    tx: &mut T,
    tenant: TenantId,
    version: u64,
    at: SystemTime,
    want: &[TenantRef],
    fresh: &[AncestorRows],
) -> Result<AncestryReport> {
    let wanted: BTreeSet<TenantId> = want.iter().map(|r| r.id).collect();
    if let Some(x) = fresh.iter().find(|f| !wanted.contains(&f.tenant)) {
        return Err(SprigError::internal(format!(
            "tenant {tenant}: rows of {} copied, but it isn't an ancestor",
            x.tenant
        )));
    }
    if wanted.contains(&tenant) {
        return Err(SprigError::internal(format!(
            "tenant {tenant} is listed among its own ancestors"
        )));
    }
    let current = AncestryState::read(tx, tenant).await?;
    let mut report = AncestryReport::default();

    for f in fresh {
        copy_one(tx, f, version, at, &mut report).await?;
    }

    // Former ancestors: those in the old ancestry or recorded versions.
    let former: BTreeSet<TenantId> = current
        .ids()
        .into_iter()
        .chain(current.versions.keys().copied())
        .filter(|t| !wanted.contains(t))
        .collect();
    for x in former {
        for e in tx.scope_rows(SettingScope::Ancestor(x)).await? {
            if !e.is_empty() {
                clear(tx, &e, version, at).await?;
                report.rows_cleared += 1;
            }
        }
        report.removed.push(x);
    }

    let mut versions: BTreeMap<TenantId, u64> = current
        .versions
        .iter()
        .filter(|(t, _)| wanted.contains(t))
        .map(|(t, v)| (*t, *v))
        .collect();
    for f in fresh {
        versions.insert(f.tenant, f.settings_version);
    }
    if current.ancestry.as_deref() != Some(want) {
        put_internal(tx, ANCESTRY_KEY, ancestry_json(want)?, version, at).await?;
        report.keys_written = true;
    }
    let versions_missing = tx
        .setting(SettingScope::Tenant, ANCESTOR_VERSIONS_KEY)
        .await?
        .is_none();
    if versions_missing || versions != current.versions {
        put_internal(
            tx,
            ANCESTOR_VERSIONS_KEY,
            versions_json(&versions),
            version,
            at,
        )
        .await?;
        report.keys_written = true;
    }
    Ok(report)
}

/// Replaces the copy of one ancestor's rows: writes rows that are new or
/// changed, clears rows the ancestor no longer has.
async fn copy_one<T: SettingsTx + ?Sized>(
    tx: &mut T,
    f: &AncestorRows,
    version: u64,
    at: SystemTime,
    report: &mut AncestryReport,
) -> Result<()> {
    let scope = SettingScope::Ancestor(f.tenant);
    let existing: BTreeMap<String, SettingRow> = tx
        .scope_rows(scope)
        .await?
        .into_iter()
        .map(|r| (r.key.clone(), r))
        .collect();
    let mut keep = BTreeSet::new();
    for r in f.rows.iter().filter(|r| !r.is_internal() && !r.is_empty()) {
        keep.insert(r.key.as_str());
        let same = existing
            .get(&r.key)
            .is_some_and(|e| e.value == r.value && e.bounds == r.bounds);
        if same {
            continue;
        }
        tx.put_setting(&SettingRow {
            scope,
            key: r.key.clone(),
            value: r.value.clone(),
            bounds: r.bounds.clone(),
            set_by: r.set_by.clone(),
            set_at: r.set_at,
            version,
        })
        .await?;
        report.rows_written += 1;
    }
    for e in existing.values() {
        if !keep.contains(e.key.as_str()) && !e.is_empty() {
            clear(tx, e, version, at).await?;
            report.rows_cleared += 1;
        }
    }
    report.copied.push(f.tenant);
    Ok(())
}

/// Leaves `row` empty with `version`: a tombstone incremental loads see.
async fn clear<T: SettingsTx + ?Sized>(
    tx: &mut T,
    row: &SettingRow,
    version: u64,
    at: SystemTime,
) -> Result<()> {
    tx.reset_setting(row.scope, &row.key, true, true, SYSTEM, at, version)
        .await
        .map(|_| ())
}

async fn put_internal<T: SettingsTx + ?Sized>(
    tx: &mut T,
    key: &str,
    value: Json,
    version: u64,
    at: SystemTime,
) -> Result<()> {
    tx.put_setting(&SettingRow {
        scope: SettingScope::Tenant,
        key: key.to_owned(),
        value: Some(value),
        bounds: None,
        set_by: SYSTEM.to_owned(),
        set_at: at,
        version,
    })
    .await
}

#[cfg(test)]
mod tests;
