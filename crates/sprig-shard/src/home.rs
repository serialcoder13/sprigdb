//! A tenant's home DC and the commit that changes it (chapter 24,
//! "Orchestrating a move", cutover steps 6 and 7).
//!
//! The home DC is the directory entry's `home_dc` (`/sprig/g/tenants/<id>`,
//! chapter 23); its tier holds the tenant's map `shardmaps/<tenant>/0` with
//! role `home`. A move between DCs ends in one etcd transaction, the
//! commit point, which in R0's single-etcd mode can span both DC tiers and
//! the global tier:
//!
//! ```text
//! when [ModRev(X map), ModRev(directory entry), Y map missing or ModRev(Y map)]
//! then X map: held by the job -> flipped to the target cluster, epoch + 1,
//!             role quarantined (a forwarding record: sessions still
//!             following it follow the move; new lookups read the directory)
//!      Y map: role home, the same ranges, tenant_epoch = e + 1
//!      directory: home_dc = Y, epoch = e + 1, state active, migration
//!                 cleared, last_move {at_ms, from, to, job, moves_back}
//! ```
//!
//! This stands in for `Global.CommitHomeChange` (R2): the global leader
//! writes the directory there, and the DC tiers' maps are written before
//! it. With one etcd, one transaction makes the three writes atomic, so no
//! reader ever sees a directory that names Y without Y's map.
//!
//! Directory fields this crate doesn't model are kept: the entry is edited
//! as JSON.

use std::sync::Arc;

use serde_json::{Map, Value, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, MetaStore, Op, Record, Revision, Tier, TierView, Txn};

use crate::map::{self, Placement, TenantMaps};

/// `tenants/<id>` in the global tier.
///
/// # Errors
/// Internal never in practice.
pub fn directory_key(tenant: TenantId) -> Result<Key> {
    Tier::Global.key(&format!("tenants/{tenant}"))
}

/// The tenant maps of DC `dc`, over `store` (any tier of the same etcd).
#[must_use]
pub fn maps_in(store: &Arc<dyn MetaStore>, dc: &DcId) -> TenantMaps {
    TenantMaps::new(TierView::over(store, Tier::Dc(dc.clone())))
}

/// What the routing and the move need from a directory entry.
#[derive(Debug, Clone, PartialEq)]
pub struct HomeEntry {
    /// The home DC.
    pub home_dc: DcId,
    /// Raised on every home change.
    pub epoch: u64,
    /// `active`, `migrating`, ...
    pub state: String,
    /// Set by `sprigctl tenant pin`: no move unless unpinned.
    pub pinned_dc: Option<DcId>,
    /// The move in progress, if any.
    pub migration: Option<Value>,
    /// The last finished move, if any.
    pub last_move: Option<Value>,
    /// The whole entry, unknown fields included.
    pub raw: Map<String, Value>,
    /// Its mod revision.
    pub rev: Revision,
}

fn bad(why: &str) -> SprigError {
    SprigError::internal(format!("bad directory entry: {why}"))
}

impl HomeEntry {
    /// Decodes an entry.
    ///
    /// # Errors
    /// Internal for an entry without a home DC or schema version.
    pub fn decode(bytes: &[u8], rev: Revision) -> Result<Self> {
        let raw: Map<String, Value> =
            serde_json::from_slice(bytes).map_err(|e| bad(&e.to_string()))?;
        if raw.get("v").and_then(Value::as_u64) != Some(1) {
            return Err(bad("unknown schema version"));
        }
        let dc = |k: &str| {
            raw.get(k)
                .and_then(Value::as_str)
                .and_then(|s| DcId::new(s).ok())
        };
        let home_dc = dc("home_dc").ok_or_else(|| bad("no home_dc"))?;
        let nonnull = |k: &str| raw.get(k).filter(|v| !v.is_null()).cloned();
        Ok(Self {
            home_dc,
            epoch: raw.get("epoch").and_then(Value::as_u64).unwrap_or(0),
            state: raw
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            pinned_dc: dc("pinned_dc"),
            migration: nonnull("migration"),
            last_move: nonnull("last_move"),
            rev,
            raw,
        })
    }

    /// The entry encoded with `raw`'s changes.
    ///
    /// # Errors
    /// Internal if it can't be encoded.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.raw).map_err(|e| bad(&e.to_string()))
    }

    /// The tenant's allowed DCs (chapter 26): the directory entry's
    /// `residency.allowed_dcs`, resolved from its policy by the
    /// controllers. `None` when the tenant has no residency (it may be
    /// anywhere).
    #[must_use]
    pub fn allowed_dcs(&self) -> Option<std::collections::BTreeSet<DcId>> {
        allowed_dcs(&self.raw)
    }

    /// Whether residency lets the tenant's data be in `dc`.
    #[must_use]
    pub fn residency_allows(&self, dc: &DcId) -> bool {
        self.allowed_dcs().is_none_or(|a| a.contains(dc))
    }

    /// The target DC of the move in progress.
    #[must_use]
    pub fn migration_to(&self) -> Option<DcId> {
        self.migration
            .as_ref()?
            .get("to")?
            .as_str()
            .and_then(|s| DcId::new(s).ok())
    }
}

/// The allowed DCs of a directory entry given as JSON (see
/// [`HomeEntry::allowed_dcs`]). A `residency` block whose `allowed_dcs`
/// is missing or malformed allows nothing: residency fails closed.
#[must_use]
pub fn allowed_dcs(raw: &Map<String, Value>) -> Option<std::collections::BTreeSet<DcId>> {
    let block = raw.get("residency").filter(|v| !v.is_null())?;
    let Some(list) = block.get("allowed_dcs").and_then(Value::as_array) else {
        return Some(std::collections::BTreeSet::new());
    };
    Some(
        list.iter()
            .filter_map(Value::as_str)
            .filter_map(|d| DcId::new(d).ok())
            .collect(),
    )
}

/// Reads `tenant`'s directory entry.
///
/// # Errors
/// The store's errors; internal for a bad entry.
pub async fn read_home(global: &dyn MetaStore, tenant: TenantId) -> Result<Option<HomeEntry>> {
    match global
        .get(&directory_key(tenant)?, Consistency::Linearizable)
        .await?
    {
        Some(kv) => HomeEntry::decode(&kv.value, kv.mod_revision).map(Some),
        None => Ok(None),
    }
}

/// Changes a directory entry with `f` (through [`HomeEntry::raw`]),
/// compare-and-swap on its revision, retrying a few times when it changed
/// meanwhile; returns the entry as written. `f` returns `false` to leave
/// the entry as it is.
///
/// # Errors
/// 39072 if the tenant has no entry; whatever `f` returns; 39070 if the
/// entry kept changing.
pub async fn update_home<F>(global: &dyn MetaStore, tenant: TenantId, mut f: F) -> Result<HomeEntry>
where
    F: FnMut(&mut HomeEntry) -> Result<bool> + Send,
{
    let key = directory_key(tenant)?;
    for _ in 0..16 {
        let mut e = read_home(global, tenant).await?.ok_or_else(|| {
            SprigError::new(SprigCode::TenantNotReady, format!("no tenant {tenant}"))
        })?;
        if !f(&mut e)? {
            return Ok(e);
        }
        let value = e.encode()?;
        let r = global
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), e.rev)],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: value.clone(),
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            // The typed fields again, from what was written.
            return HomeEntry::decode(&value, r.revision);
        }
    }
    Err(SprigError::new(
        SprigCode::CatalogConflict,
        format!("tenant {tenant}'s directory entry kept changing"),
    ))
}

/// One home change, as the mover's commit names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeChange {
    /// The tenant.
    pub tenant: TenantId,
    /// The source DC, whose map the job holds.
    pub from: DcId,
    /// The new home.
    pub to: DcId,
    /// The target cluster, in `to`.
    pub cluster: ClusterId,
    /// The hold's holder text (`<job>#<token>`, as the mover writes it).
    pub holder: String,
    /// Wall-clock milliseconds, for `last_move.at_ms`.
    pub now_ms: u64,
}

/// What [`commit`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeCommit {
    /// The three writes committed (now or by an earlier call whose reply
    /// was lost); the new epoch.
    Committed(u64),
    /// Nothing changed: the map isn't held by this holder, or the
    /// directory isn't migrating to `to`. The reason, for the log.
    Rejected(String),
}

/// The job part of a holder text.
fn job_of(holder: &str) -> &str {
    holder.split_once('#').map_or(holder, |(j, _)| j)
}

/// The map already shows the job's flip.
fn moved_by_job(m: &ShardMap, holder: &str) -> Result<bool> {
    let p = Placement::of(m)?;
    Ok(p.hold.is_none() && p.moved_by.as_deref().map(job_of) == Some(job_of(holder)))
}

/// The commit point of a move between DCs (see the module docs). `store`
/// is any tier of the one etcd.
///
/// # Errors
/// The store's errors (the outcome is then unknown: read the maps again);
/// internal for bad records.
#[allow(clippy::too_many_lines)] // the checks, then one transaction
pub async fn commit(store: &Arc<dyn MetaStore>, c: &HomeChange) -> Result<HomeCommit> {
    let src = maps_in(store, &c.from);
    let dst = maps_in(store, &c.to);
    let global = TierView::over(store, Tier::Global);
    for _ in 0..8 {
        let (x, xrev) = src.load(c.tenant).await?;
        let dir = read_home(&*global, c.tenant)
            .await?
            .ok_or_else(|| SprigError::new(SprigCode::TenantNotReady, "no directory entry"))?;
        if moved_by_job(&x, &c.holder)? {
            return Ok(if dir.home_dc == c.to {
                HomeCommit::Committed(dir.epoch)
            } else {
                HomeCommit::Rejected(format!(
                    "the source map shows the flip but the home is {}",
                    dir.home_dc
                ))
            });
        }
        if dir.home_dc != c.from {
            return Ok(HomeCommit::Rejected(format!(
                "home is {}, not {}",
                dir.home_dc, c.from
            )));
        }
        // Residency (chapter 26), checked at the commit point against the
        // entry this transaction compares: a policy tightened during the
        // move stops it here.
        if !dir.residency_allows(&c.to) {
            return Ok(HomeCommit::Rejected(format!(
                "residency doesn't allow {} for the tenant",
                c.to
            )));
        }
        if dir.state != "migrating" || dir.migration_to().as_ref() != Some(&c.to) {
            return Ok(HomeCommit::Rejected(format!(
                "the directory isn't migrating to {} (state {})",
                c.to, dir.state
            )));
        }
        let existing = match dst.load(c.tenant).await {
            Ok((m, rev)) => Some((m, rev)),
            Err(e) if e.code() == Some(SprigCode::TenantNotReady) => None,
            Err(e) => return Err(e),
        };
        if let Some((m, _)) = &existing
            && m.role == MapRole::Home
        {
            return Ok(HomeCommit::Rejected(format!(
                "{} already holds a home map for the tenant",
                c.to
            )));
        }
        let epoch = dir.epoch + 1;
        let mut flipped = x.clone();
        if let Err(e) = map::flip(&mut flipped, &c.holder, &c.cluster) {
            return Ok(HomeCommit::Rejected(e.message));
        }
        flipped.tenant_epoch = epoch;
        let mut home = flipped.clone();
        home.role = MapRole::Home;
        home.map_version = existing
            .as_ref()
            .map_or(1, |(m, _)| m.map_version.max(flipped.map_version) + 1);
        flipped.role = MapRole::Quarantined;
        let mut entry = dir.clone();
        let moves_back = match &dir.last_move {
            Some(lm) if lm.get("from").and_then(Value::as_str) == Some(c.to.as_str()) => {
                lm.get("moves_back").and_then(Value::as_u64).unwrap_or(0) + 1
            }
            _ => 0,
        };
        entry.raw.insert("home_dc".into(), json!(c.to.as_str()));
        entry.raw.insert("epoch".into(), json!(epoch));
        entry.raw.insert("state".into(), json!("active"));
        entry.raw.insert("migration".into(), Value::Null);
        entry.raw.insert(
            "last_move".into(),
            json!({"at_ms": c.now_ms, "from": c.from.as_str(), "to": c.to.as_str(),
                   "job": job_of(&c.holder), "moves_back": moves_back,
                   "migration": dir.migration.clone().unwrap_or(Value::Null)}),
        );
        let xkey = ShardMap::catalog_key(src.store().tier(), c.tenant)?;
        let ykey = ShardMap::catalog_key(dst.store().tier(), c.tenant)?;
        let dkey = directory_key(c.tenant)?;
        let mut when = vec![
            Cmp::ModRev(xkey.clone(), xrev),
            Cmp::ModRev(dkey.clone(), dir.rev),
        ];
        when.push(match &existing {
            Some((_, rev)) => Cmp::ModRev(ykey.clone(), *rev),
            None => Cmp::Missing(ykey.clone()),
        });
        map::validate(&flipped)?;
        map::validate(&home)?;
        let r = store
            .txn(Txn {
                when,
                then: vec![
                    Op::Put {
                        key: xkey,
                        value: Record::new(flipped).encode()?,
                        lease: None,
                    },
                    Op::Put {
                        key: ykey,
                        value: Record::new(home).encode()?,
                        lease: None,
                    },
                    Op::Put {
                        key: dkey,
                        value: entry.encode()?,
                        lease: None,
                    },
                ],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            return Ok(HomeCommit::Committed(epoch));
        }
        // Something changed in between (a hold renewal, a version bump on
        // the directory): read again.
    }
    Ok(HomeCommit::Rejected(
        "the maps or the directory kept changing".to_owned(),
    ))
}

#[cfg(test)]
mod tests;
