//! Inherited messaging objects (`FOR EACH TENANT`) in the gateway: what is
//! shared by the statements of [`crate::exec`] and the protocol front ends.
//!
//! ```text
//! chain(T):  T's snapshot, then each ancestor's (root last), each with its
//!            inherit_msg rows: definitions, overrides, instances
//! resolve:   the nearest definition of kind db.name whose scope reaches T
//! instance:  streams::ensure (lazy, on first publish, with its race rule),
//!            then the member's inherited triggers (triggers::ensure_for_stream)
//! index:     inherit-instances/<tenant>/<def tenant>.<def object> in the DC
//!            tier: every instance of a definition, for SHOW, IMMEDIATE
//!            drops and (later) broadcasts and fan-in
//! ```
//!
//! **As built.** Definitions have no `objects` row (as streams have none). A
//! stream instance is an ordinary `streams` row of the member, created in a
//! versioned catalog change of the member (its `inherited_instances` row is
//! written before, unversioned), so every stream reader finds it as before.

pub mod broadcast;
pub mod fanin;
pub mod fanin_mq;
pub mod fanin_sink;
pub mod fanin_triggers;
pub mod lifecycle;
pub mod mq;
pub mod orphan_sinks;
pub mod outbox;
pub mod sinks;
pub mod streams;
pub mod triggers;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::cache::Freshness;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::store::MsgStore;
use sprig_inherit::msg::{
    AppliesTo, Chain, DefKey, DefKind, Definition, Level, MsgState, Override, What,
};
use sprig_meta::{ListOpts, Record, Versioned};

use crate::tenant::TenantServices;

/// The built-in principal inherited triggers and sinks run as in each
/// member.
pub const RUN_AS_USER: &str = "sprig_inherited";

/// `tenant`'s rows of the module (empty when it has none).
#[must_use]
pub fn state_of(cat: &TenantCatalog) -> Arc<MsgState> {
    Arc::new(
        cat.ext
            .get::<MsgState>(sprig_inherit::msg::MODULE)
            .cloned()
            .unwrap_or_default(),
    )
}

/// A tenant's snapshot, fresh as the directory watch has it.
///
/// # Errors
/// The cache's.
pub async fn catalog(svc: &TenantServices, t: TenantId) -> Result<Arc<TenantCatalog>> {
    svc.cache.get(t, Freshness::Latest).await
}

/// `cat`'s tenant and its ancestors, nearest first, with their rows.
///
/// # Errors
/// The cache's (an ancestor's catalog that can't be loaded).
pub async fn chain(svc: &TenantServices, cat: &Arc<TenantCatalog>) -> Result<Chain> {
    let mut levels = vec![Level {
        tenant: cat.tenant,
        state: Some(state_of(cat)),
    }];
    for a in cat.ancestry.iter().rev() {
        let st = if let Some(c) = svc.cache.cached(a.id) {
            state_of(&c)
        } else {
            let c = catalog(svc, a.id).await?;
            state_of(&c)
        };
        levels.push(Level {
            tenant: a.id,
            state: Some(st),
        });
    }
    Ok(Chain { levels })
}

/// Whether any level of `chain` declares anything (a cheap test before
/// work on a member's statement).
#[must_use]
pub fn has_defs(chain: &Chain) -> bool {
    chain
        .levels
        .iter()
        .any(|l| l.state.as_ref().is_some_and(|s| !s.defs.is_empty()))
}

/// The fields of a directory entry (`tenants/<id>`) this module reads.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DirEntry {
    /// The id.
    #[serde(default)]
    pub tenant_id: u64,
    /// The name.
    #[serde(default)]
    pub name: String,
    /// `active`, `creating`, `deleting`, ...
    #[serde(default)]
    pub state: String,
    /// Ids root first, self included: `/1/42/77/`.
    #[serde(default)]
    pub path: String,
    /// RFC 3339 UTC.
    #[serde(default)]
    pub created_at: String,
    /// The home data center.
    #[serde(default)]
    pub home_dc: String,
    /// The residency block.
    #[serde(default)]
    pub residency: Value,
}

impl Versioned for DirEntry {
    const V: u32 = 1;
}

impl DirEntry {
    /// The path's ids, root first, self last.
    #[must_use]
    pub fn path_ids(&self) -> Vec<u64> {
        self.path
            .split('/')
            .filter_map(|s| s.parse().ok())
            .collect()
    }
}

/// `tenants/<id>` from the global tier.
///
/// # Errors
/// The store's.
pub async fn dir_entry(svc: &TenantServices, id: TenantId) -> Result<Option<DirEntry>> {
    let key = svc.global.tier().key(&format!("tenants/{id}"))?;
    let Some(kv) = svc
        .global
        .get(&key, sprig_meta::Consistency::Serializable)
        .await?
    else {
        return Ok(None);
    };
    Ok(Record::<DirEntry>::decode(&kv.value).ok().map(|r| {
        let mut e = r.body;
        e.tenant_id = id.0;
        e
    }))
}

#[derive(Debug, Deserialize, Serialize)]
struct NameRecord {
    tenant_id: u64,
}

impl Versioned for NameRecord {
    const V: u32 = 1;
}

/// The tenant named `name` (`tenant-names/<name>`).
///
/// # Errors
/// The store's.
pub async fn tenant_by_name(svc: &TenantServices, name: &str) -> Result<Option<TenantId>> {
    let key = svc.global.tier().key(&format!("tenant-names/{name}"))?;
    let Some(kv) = svc
        .global
        .get(&key, sprig_meta::Consistency::Serializable)
        .await?
    else {
        return Ok(None);
    };
    if let Ok(r) = Record::<NameRecord>::decode(&kv.value) {
        return Ok(Some(TenantId(r.body.tenant_id)));
    }
    Ok(std::str::from_utf8(&kv.value)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .map(TenantId))
}

/// A tenant's name (from its directory entry).
///
/// # Errors
/// 39896 for a tenant without an entry; the store's.
pub async fn tenant_name(svc: &TenantServices, id: TenantId) -> Result<String> {
    dir_entry(svc, id)
        .await?
        .map(|e| e.name)
        .ok_or_else(|| target_err(format!("no tenant {id}")))
}

/// 39896 with `what`.
#[must_use]
pub fn target_err(what: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::InheritTargetTenant,
        format!("ER_SPRIG_INHERIT_TARGET_TENANT: {what}"),
    )
}

/// 39897 with `what`.
#[must_use]
pub fn not_overridable(what: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::InheritNotOverridable,
        format!("ER_SPRIG_INHERIT_NOT_OVERRIDABLE: {what}"),
    )
}

/// 39898 with `what`.
#[must_use]
pub fn dropping(what: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::InheritDefinitionDropping,
        format!("ER_SPRIG_INHERIT_DEFINITION_DROPPING: {what}"),
    )
}

/// 39899 with `what` (retryable).
#[must_use]
pub fn unavailable(what: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::InheritInstanceUnavailable,
        format!("ER_SPRIG_INHERIT_INSTANCE_UNAVAILABLE: {what}"),
    )
}

/// Applies the module's migration to `tenant`'s catalog.
///
/// # Errors
/// The catalog's.
pub async fn migrate(svc: &TenantServices, tenant: TenantId) -> Result<()> {
    let ep = svc.locator.writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    sprig_inherit::msg::store::migrate(&mut conn, ep.store.schema()).await
}

/// One versioned change of `tenant`'s catalog writing module rows.
pub enum MsgWrite<'a> {
    /// A definition (new or changed).
    Def(&'a Definition),
    /// A definition gone.
    DeleteDef(u64),
    /// An override made in this tenant.
    Override(&'a Override),
    /// Overrides made in this tenant removed: of `what` (all when
    /// `None`), for a target.
    DeleteOverrides {
        /// The definition.
        def: DefKey,
        /// For whom.
        target: TenantId,
        /// Alone or subtree.
        applies_to: AppliesTo,
        /// What.
        what: Option<What>,
    },
}

/// Commits `writes` in one versioned catalog change of `tenant`
/// (migrating the module first); the new catalog version.
///
/// # Errors
/// The catalog's.
pub async fn commit(
    svc: &TenantServices,
    tenant: TenantId,
    writes: &[MsgWrite<'_>],
) -> Result<u64> {
    migrate(svc, tenant).await?;
    let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
    let version = c.version();
    let applied = {
        let (store, tx) = c.tx().parts();
        let st = MsgStore::new(store.schema());
        async {
            for w in writes {
                match w {
                    MsgWrite::Def(d) => st.put_def(tx, d, version).await?,
                    MsgWrite::DeleteDef(o) => st.delete_def(tx, *o).await?,
                    MsgWrite::Override(o) => st.put_override(tx, o, version).await?,
                    MsgWrite::DeleteOverrides {
                        def,
                        target,
                        applies_to,
                        what,
                    } => {
                        st.delete_override(tx, *def, *target, *applies_to, *what)
                            .await?;
                    }
                }
            }
            Ok::<_, SprigError>(())
        }
        .await
    };
    if let Err(e) = applied {
        let _ = c.rollback().await;
        return Err(e);
    }
    c.commit().await
}

/// `inherit-instances/<tenant>/<def tenant>.<def object>` in the DC tier:
/// one instance of a definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceIndex {
    /// The member.
    pub tenant: TenantId,
    /// The definition.
    pub def: DefKey,
    /// Its kind.
    pub kind: DefKind,
    /// The instance's id in the member.
    pub instance_id: u64,
    /// The database name.
    pub db: String,
    /// The name.
    pub name: String,
}

impl Versioned for InstanceIndex {
    const V: u32 = 1;
}

fn index_key(svc: &TenantServices, tenant: TenantId, def: DefKey) -> Result<sprig_meta::Key> {
    svc.locator
        .dc_store()
        .tier()
        .key(&format!("inherit-instances/{tenant}/{def}"))
}

/// Writes an instance's index entry.
///
/// # Errors
/// The store's.
pub async fn put_index(svc: &TenantServices, e: &InstanceIndex) -> Result<()> {
    let store = svc.locator.dc_store();
    let key = index_key(svc, e.tenant, e.def)?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key,
                value: Record::new(e.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Removes an instance's index entry.
///
/// # Errors
/// The store's.
pub async fn delete_index(svc: &TenantServices, tenant: TenantId, def: DefKey) -> Result<()> {
    let store = svc.locator.dc_store();
    let key = index_key(svc, tenant, def)?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Delete(key)],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Every instance of `def` this DC indexes.
///
/// # Errors
/// The store's.
pub async fn instances_of(svc: &TenantServices, def: DefKey) -> Result<Vec<InstanceIndex>> {
    let store = svc.locator.dc_store();
    let prefix = store.tier().key("inherit-instances/")?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 1_000_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| {
            Record::<InstanceIndex>::decode(&kv.value)
                .ok()
                .map(|r| r.body)
        })
        .filter(|e| e.def == def)
        .collect())
}

/// Under `residency.cross_tenant_messaging_strict`: refuses (`code`) a
/// message of `publisher`'s stored in `owner`'s home data center when
/// `publisher`'s residency doesn't allow that data center.
///
/// # Errors
/// `code`; the store's.
pub async fn check_residency(
    svc: &TenantServices,
    publisher: TenantId,
    owner: TenantId,
    code: SprigCode,
) -> Result<()> {
    let p = dir_entry(svc, publisher).await?.unwrap_or_default();
    let Some(r) = sprig_auth::TenantResidency::of(&p.residency) else {
        return Ok(());
    };
    let home = dir_entry(svc, owner).await?.unwrap_or_default().home_dc;
    if r.allowed_dcs.iter().any(|d| d.as_str() == home) {
        return Ok(());
    }
    Err(SprigError::new(
        code,
        format!(
            "{}: tenant {publisher}'s residency doesn't allow its messages in data center {home} \
             (residency.cross_tenant_messaging_strict = on)",
            code.name()
        ),
    ))
}

/// A JSON string of `s`.
#[must_use]
pub fn js(s: &str) -> Value {
    Value::String(s.to_owned())
}
