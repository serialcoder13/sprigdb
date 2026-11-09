//! The tenant tree in the directory (chapter 23, "The tenant tree" and
//! "Global tier"; build step 16, the parts tenant bootstrap needs).
//!
//! A directory entry `/sprig/g/tenants/<tenant_id>` holds the tenant's
//! name, state, home DC, versions and tree fields; `tenant-names/<name>`
//! maps the name to the id and `ids/tenant` hands out ids, which are never
//! reused.
//!
//! ```text
//! create_tenant(name, parent p):                       # Global.CreateTenant
//! 1. P = get tenants/<p>; P.state = active, P.tree_lock = null, not suspended
//! 2. P.depth + 1 <= tenant.max_depth, else 39508 ER_SPRIG_TENANT_DEPTH_EXCEEDED
//! 3. txn when [Missing(tenant-names/<name>), ModRev(tenants/<p>) = P.rev,
//!              ModRev(ids/tenant)]
//!      then [put tenant-names/<name>; put tenants/<id> {parent_tenant_id: p,
//!            path: P.path + id + "/", depth: P.depth + 1, state: creating, ...};
//!            put tenants/<p> with child_count + 1; put ids/tenant + 1]
//!    then chapter 08's bootstrap in the home DC
//!
//! delete_tenant(t):    child_count = 0 and state active, else 39362 (chapter 29); the
//!    tombstone and the parent's child_count - 1 commit in one txn guarded by both ModRevs
//! ```
//!
//! Where this differs from the book, for now:
//! - Chapter 23 makes the global leader the directory's only writer
//!   (`Global.CreateTenant`, R2). In R0's single-etcd mode the DC leader
//!   writes the entries directly, as chapter 08's catalog writers already
//!   compare-and-swap the version fields; every write is a transaction
//!   guarded by the mod revisions it read, so concurrent writers can't
//!   corrupt the tree.
//! - The tombstone of a deleted tenant is the entry in state `deleting`;
//!   the lifecycle loop removes the entry and its name once the catalog is
//!   dropped (`sprig-controller::tenants`), so a name is reusable only
//!   after cleanup.
//! - An entry may carry `placement_hint`, the `--cluster` asked for at
//!   create, until bootstrap records the catalog's shard map (interim
//!   placement until chapter 11's, M0.6).
//!
//! Not yet: `MoveTenantTree` and `tree-moves/` (with the rest of step 16),
//! `suspended` handling beyond refusing children, and the path index in
//! the controllers' mirrors (the `tree_settings` loop scans its mirror).

use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sprig_catalog::TenantRef;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::{TenantIdCounter, rfc3339};
use sprig_meta::{
    Cmp, Consistency, Key, KeyValue, ListOpts, MetaStore, Op, Record, Revision, Txn, Versioned,
};

use crate::admin::invalid;

/// Attempts at a directory write whose compare failed.
const WRITE_ATTEMPTS: usize = 20;

/// A tenant's state in the directory (chapter 23; `failed` is chapter 08's
/// end of a bootstrap past its deadline).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantState {
    /// Bootstrap is running.
    Creating,
    /// Serving.
    Active,
    /// Moving to another home DC (chapter 24).
    Migrating,
    /// Its home DC is failing over (chapter 25).
    FailingOver,
    /// Fenced (chapter 25).
    Fenced,
    /// Deleted; cleanup is running.
    Deleting,
    /// Bootstrap gave up after `catalog.bootstrap_timeout_minutes`.
    Failed,
}

impl TenantState {
    /// The state's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TenantState::Creating => "creating",
            TenantState::Active => "active",
            TenantState::Migrating => "migrating",
            TenantState::FailingOver => "failing_over",
            TenantState::Fenced => "fenced",
            TenantState::Deleting => "deleting",
            TenantState::Failed => "failed",
        }
    }
}

impl std::fmt::Display for TenantState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A directory entry, `/sprig/g/tenants/<tenant_id>` (chapter 23). Fields
/// this build doesn't know (`pinned_dc`, `residency`, ...) are kept by the
/// record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    /// The id.
    pub tenant_id: TenantId,
    /// The name, a lowercase DNS label.
    pub name: String,
    /// The state.
    pub state: TenantState,
    /// The home DC.
    pub home_dc: DcId,
    /// Raised on every home change.
    #[serde(default)]
    pub epoch: u64,
    /// The last allocated catalog version (chapter 08).
    #[serde(default)]
    pub catalog_version: u64,
    /// The last allocated auth version (chapter 14).
    #[serde(default)]
    pub auth_version: u64,
    /// The catalog version of the last tenant-scope settings change, which
    /// descendants copy.
    #[serde(default)]
    pub settings_version: u64,
    /// The parent; 0 for a root. Authoritative.
    #[serde(default)]
    pub parent_tenant_id: u64,
    /// Ids root first, self included: `"/1/42/77/"`. A cache of the parent
    /// links.
    #[serde(default)]
    pub path: String,
    /// Root 0.
    #[serde(default)]
    pub depth: u32,
    /// Children.
    #[serde(default)]
    pub child_count: u64,
    /// Set by an administrator: logins fail for the whole subtree.
    #[serde(default)]
    pub suspended: bool,
    /// The `move_id` of an open subtree move.
    #[serde(default)]
    pub tree_lock: Option<String>,
    /// Interim: the cluster `--cluster` asked for, until bootstrap writes
    /// the catalog's shard map.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_hint: Option<ClusterId>,
    /// When the tenant was created, RFC 3339 UTC (for people).
    #[serde(default)]
    pub created_at: String,
    /// Unique to this creation of the tenant (a UUIDv7), so its catalog
    /// schema can be told from one an earlier tenant with the same id left
    /// behind. Empty for entries made before it existed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub incarnation: String,
}

impl Versioned for DirectoryEntry {
    const V: u32 = 1;
}

impl DirectoryEntry {
    /// The parent, if any.
    #[must_use]
    pub fn parent(&self) -> Option<TenantId> {
        (self.parent_tenant_id != 0).then_some(TenantId(self.parent_tenant_id))
    }

    /// Whether `x` is on this entry's path (an ancestor or itself), by the
    /// cached path.
    #[must_use]
    pub fn path_contains(&self, x: TenantId) -> bool {
        self.path.contains(&format!("/{x}/"))
    }

    /// The entry as an ancestry item.
    #[must_use]
    pub fn as_ref(&self) -> TenantRef {
        TenantRef {
            id: self.tenant_id,
            name: self.name.clone(),
        }
    }
}

/// `tenant-names/<name>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameEntry {
    /// The tenant.
    pub tenant_id: TenantId,
}

impl Versioned for NameEntry {
    const V: u32 = 1;
}

/// A directory entry with its mod revision.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredEntry {
    /// The record (unknown fields kept).
    pub record: Record<DirectoryEntry>,
    /// Its mod revision.
    pub mod_revision: Revision,
}

impl StoredEntry {
    /// The entry.
    #[must_use]
    pub fn body(&self) -> &DirectoryEntry {
        &self.record.body
    }

    /// Decodes a stored value.
    ///
    /// # Errors
    /// `Internal` for a bad record.
    pub fn decode(kv: &KeyValue) -> Result<Self> {
        Ok(Self {
            record: Record::decode(&kv.value)?,
            mod_revision: kv.mod_revision,
        })
    }
}

/// Checks a tenant name: a lowercase DNS label (it appears in TLS SNI),
/// starting with a letter.
///
/// # Errors
/// 39792 for a name that isn't one.
pub fn validate_name(name: &str) -> Result<()> {
    let ok = (1..=63).contains(&name.len())
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && !name.ends_with('-')
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(invalid(format!(
            "tenant name {name:?}: a lowercase DNS label of 1-63 letters, digits and hyphens, \
             starting with a letter"
        )))
    }
}

/// The tenant ids a deployment may hand out, `tenant.id_min` to
/// `tenant.id_max` inclusive. Deployments that share MariaDB servers (the
/// development stack's private controllers) each take their own range, so
/// the counter's jump past existing `t<id>_` schemas never lands in
/// another deployment's ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdRange {
    /// The lowest id.
    pub min: u64,
    /// The highest id.
    pub max: u64,
}

impl IdRange {
    /// Every id: the defaults of `tenant.id_min` and `tenant.id_max`.
    #[allow(clippy::cast_sign_loss)] // i64::MAX is positive
    pub const ALL: IdRange = IdRange {
        min: 1,
        max: i64::MAX as u64,
    };

    /// 39080 if the range is empty, or if `next`, the id the counter
    /// would hand out, is past it.
    fn check(&self, next: Option<u64>) -> Result<()> {
        if self.min > self.max {
            return Err(SprigError::new(
                SprigCode::SettingOutOfBounds,
                format!(
                    "tenant.id_min ({}) is above tenant.id_max ({}): no tenant id can be allocated",
                    self.min, self.max
                ),
            ));
        }
        match next {
            Some(n) if n > self.max => Err(SprigError::new(
                SprigCode::SettingOutOfBounds,
                format!(
                    "no tenant id left in this deployment's range {self} \
                     (tenant.id_min..=tenant.id_max): the next id would be {n}; raise \
                     tenant.id_max or give the deployment a new range"
                ),
            )),
            _ => Ok(()),
        }
    }

    /// Whether `id` is in the range.
    #[must_use]
    pub fn contains(&self, id: u64) -> bool {
        (self.min..=self.max).contains(&id)
    }
}

impl Default for IdRange {
    fn default() -> Self {
        Self::ALL
    }
}

impl std::fmt::Display for IdRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..={}", self.min, self.max)
    }
}

/// What [`Directory::create`] writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTenant {
    /// The name.
    pub name: String,
    /// The parent, `None` for a root.
    pub parent: Option<TenantId>,
    /// The home DC.
    pub home_dc: DcId,
    /// The cluster asked for, if any.
    pub placement_hint: Option<ClusterId>,
}

/// A new entry for `req` as tenant `id` under `parent`: state
/// `creating`, versions 0, path and depth from the parent.
fn new_entry(
    req: &NewTenant,
    id: TenantId,
    parent: Option<&StoredEntry>,
    now: SystemTime,
) -> DirectoryEntry {
    let (path, depth) = match parent {
        Some(p) => (format!("{}{id}/", p.body().path), p.body().depth + 1),
        None => (format!("/{id}/"), 0),
    };
    DirectoryEntry {
        tenant_id: id,
        name: req.name.clone(),
        state: TenantState::Creating,
        home_dc: req.home_dc.clone(),
        epoch: 1,
        catalog_version: 0,
        auth_version: 0,
        settings_version: 0,
        parent_tenant_id: req.parent.map_or(0, |p| p.0),
        path,
        depth,
        child_count: 0,
        suspended: false,
        tree_lock: None,
        placement_hint: req.placement_hint.clone(),
        created_at: rfc3339(now),
        incarnation: uuid::Uuid::now_v7().simple().to_string(),
    }
}

/// The tenant directory in the global tier.
#[derive(Clone)]
pub struct Directory {
    global: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for Directory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Directory")
            .field("tier", self.global.tier())
            .finish()
    }
}

fn unknown(what: impl std::fmt::Display) -> SprigError {
    SprigError::new(SprigCode::TenantNotReady, format!("no tenant {what}"))
}

impl Directory {
    /// The directory in `global` (keys under its tier, so a test can stand
    /// a private tier in for `/sprig/g/`).
    #[must_use]
    pub fn new(global: Arc<dyn MetaStore>) -> Self {
        Self { global }
    }

    /// The store.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn MetaStore> {
        &self.global
    }

    /// `tenants/<id>`.
    ///
    /// # Errors
    /// `Internal` never in practice.
    pub fn key(&self, id: TenantId) -> Result<Key> {
        self.global.tier().key(&format!("tenants/{id}"))
    }

    /// `tenants/`.
    ///
    /// # Errors
    /// `Internal` never in practice.
    pub fn prefix(&self) -> Result<Key> {
        self.global.tier().key("tenants/")
    }

    fn name_key(&self, name: &str) -> Result<Key> {
        self.global.tier().key(&format!("tenant-names/{name}"))
    }

    /// The tenant whose entry is at `key`, if it is one.
    #[must_use]
    pub fn tenant_of(&self, key: &Key) -> Option<TenantId> {
        let prefix = self.prefix().ok()?;
        key.as_str()
            .strip_prefix(prefix.as_str())?
            .parse()
            .ok()
            .map(TenantId)
    }

    /// The entry of `id`, linearizable.
    ///
    /// # Errors
    /// The store's errors; `Internal` for a bad record.
    pub async fn get(&self, id: TenantId) -> Result<Option<StoredEntry>> {
        self.global
            .get(&self.key(id)?, Consistency::Linearizable)
            .await?
            .as_ref()
            .map(StoredEntry::decode)
            .transpose()
    }

    /// The entry named `name`.
    ///
    /// # Errors
    /// As [`get`](Self::get).
    pub async fn by_name(&self, name: &str) -> Result<Option<StoredEntry>> {
        if validate_name(name).is_err() {
            return Ok(None);
        }
        let Some(kv) = self
            .global
            .get(&self.name_key(name)?, Consistency::Linearizable)
            .await?
        else {
            return Ok(None);
        };
        let id = Record::<NameEntry>::decode(&kv.value)?.body.tenant_id;
        Ok(self.get(id).await?.filter(|e| e.body().name == name))
    }

    /// The entry named `name`, or 39072.
    ///
    /// # Errors
    /// 39072 for an unknown name; as [`get`](Self::get).
    pub async fn require(&self, name: &str) -> Result<StoredEntry> {
        self.by_name(name)
            .await?
            .ok_or_else(|| unknown(format!("named {name:?}")))
    }

    /// Every entry, by id.
    ///
    /// # Errors
    /// The store's errors. Bad records are skipped with a warning.
    pub async fn list(&self) -> Result<Vec<StoredEntry>> {
        let prefix = self.prefix()?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.global.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match StoredEntry::decode(kv) {
                    Ok(e) => out.push(e),
                    Err(e) => tracing::warn!(key = %kv.key, error = %e, "bad directory entry"),
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        out.sort_by_key(|e| e.body().tenant_id);
        Ok(out)
    }

    /// The ancestry of `entry` from the parent links, root first, parent
    /// last, each with its `settings_version`. Parent links are the truth
    /// (chapter 23: paths may lag during a move).
    ///
    /// # Errors
    /// 39072 if an ancestor's entry is missing; `Internal` for a cycle.
    pub async fn ancestry(&self, entry: &DirectoryEntry) -> Result<Vec<(TenantRef, u64)>> {
        let mut out: Vec<(TenantRef, u64)> = Vec::new();
        let mut next = entry.parent();
        while let Some(p) = next {
            if p == entry.tenant_id || out.iter().any(|(r, _)| r.id == p) || out.len() > 4096 {
                return Err(SprigError::internal(format!(
                    "tenant {}: the parent links form a cycle at {p}",
                    entry.tenant_id
                )));
            }
            let e = self
                .get(p)
                .await?
                .ok_or_else(|| unknown(format!("{p} (an ancestor of {})", entry.tenant_id)))?;
            out.push((e.body().as_ref(), e.body().settings_version));
            next = e.body().parent();
        }
        out.reverse();
        Ok(out)
    }

    /// Chapter 23's `create_tenant`: allocates the id and writes the name,
    /// the entry (state `creating`) and the parent's child count in one
    /// transaction.
    ///
    /// # Errors
    /// 39792 for a bad name; 39360 if the name is taken; 39072 for an
    /// unknown or inactive parent; 39303 for a suspended one; 39509 for a
    /// parent under a move; 39508 past `max_depth`; the store's errors.
    pub async fn create(
        &self,
        req: &NewTenant,
        max_depth: u32,
        now: SystemTime,
    ) -> Result<StoredEntry> {
        self.create_in(req, max_depth, IdRange::ALL, now).await
    }

    /// [`create`](Self::create) with an id from `ids` only: a counter
    /// below the range starts at its lowest id.
    ///
    /// # Errors
    /// As [`create`](Self::create); 39080 when the range is empty or used
    /// up (the next id would be past `tenant.id_max`).
    pub async fn create_in(
        &self,
        req: &NewTenant,
        max_depth: u32,
        ids_allowed: IdRange,
        now: SystemTime,
    ) -> Result<StoredEntry> {
        validate_name(&req.name)?;
        ids_allowed.check(None)?;
        let ids_key = self.global.tier().key("ids/tenant")?;
        let name_key = self.name_key(&req.name)?;
        for _ in 0..WRITE_ATTEMPTS {
            if self
                .global
                .get(&name_key, Consistency::Linearizable)
                .await?
                .is_some()
            {
                return Err(SprigError::new(
                    SprigCode::TenantExists,
                    format!("a tenant named {:?} exists", req.name),
                ));
            }
            let parent = match req.parent {
                Some(p) => Some(self.parent_for_child(p, max_depth).await?),
                None => None,
            };
            let ids = self.global.get(&ids_key, Consistency::Linearizable).await?;
            let (next, ids_cmp) = match &ids {
                Some(kv) => (
                    Record::<TenantIdCounter>::decode(&kv.value)?
                        .body
                        .next
                        .max(ids_allowed.min),
                    Cmp::ModRev(ids_key.clone(), kv.mod_revision),
                ),
                None => (ids_allowed.min, Cmp::Missing(ids_key.clone())),
            };
            ids_allowed.check(Some(next))?;
            let id = TenantId(next);
            let entry = new_entry(req, id, parent.as_ref(), now);
            let mut when = vec![Cmp::Missing(name_key.clone()), ids_cmp];
            let mut then = vec![
                Op::Put {
                    key: name_key.clone(),
                    value: Record::new(NameEntry { tenant_id: id }).encode()?,
                    lease: None,
                },
                Op::Put {
                    key: self.key(id)?,
                    value: Record::new(entry).encode()?,
                    lease: None,
                },
                Op::Put {
                    key: ids_key.clone(),
                    value: Record::new(TenantIdCounter { next: next + 1 }).encode()?,
                    lease: None,
                },
            ];
            if let Some(mut p) = parent {
                let pk = self.key(p.body().tenant_id)?;
                when.push(Cmp::ModRev(pk.clone(), p.mod_revision));
                p.record.body.child_count += 1;
                then.push(Op::Put {
                    key: pk,
                    value: p.record.encode()?,
                    lease: None,
                });
            }
            let r = self
                .global
                .txn(Txn {
                    when,
                    then,
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                tracing::info!(tenant = %id, name = %req.name, parent = ?req.parent, "tenant created in the directory");
                return self
                    .get(id)
                    .await?
                    .ok_or_else(|| SprigError::internal("a created entry vanished"));
            }
        }
        Err(SprigError::new(
            SprigCode::MetaUnavailable,
            "the directory kept changing; try again",
        ))
    }

    /// Raises `ids/tenant` so the next id is above `used`, the highest id
    /// found in use elsewhere (a catalog schema on a cluster). Never lowers
    /// it. Ids are never reused (chapter 23), even when the counter was
    /// lost while an earlier tenant's schemas stayed on a cluster.
    ///
    /// # Errors
    /// 39500 if the counter kept changing; the store's errors.
    pub async fn reserve_ids_above(&self, used: u64) -> Result<()> {
        let ids_key = self.global.tier().key("ids/tenant")?;
        for _ in 0..WRITE_ATTEMPTS {
            let ids = self.global.get(&ids_key, Consistency::Linearizable).await?;
            let (next, cmp) = match &ids {
                Some(kv) => (
                    Record::<TenantIdCounter>::decode(&kv.value)?.body.next,
                    Cmp::ModRev(ids_key.clone(), kv.mod_revision),
                ),
                None => (1, Cmp::Missing(ids_key.clone())),
            };
            if next > used {
                return Ok(());
            }
            let r = self
                .global
                .txn(Txn {
                    when: vec![cmp],
                    then: vec![Op::Put {
                        key: ids_key.clone(),
                        value: Record::new(TenantIdCounter { next: used + 1 }).encode()?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                tracing::warn!(
                    from = next,
                    to = used + 1,
                    "tenant ids raised past an id already in use on a cluster (an earlier tenant's schemas)"
                );
                return Ok(());
            }
        }
        Err(SprigError::new(
            SprigCode::MetaUnavailable,
            "the tenant id counter kept changing; try again",
        ))
    }

    /// Chapter 23's step 1 and 2 checks on a parent.
    async fn parent_for_child(&self, p: TenantId, max_depth: u32) -> Result<StoredEntry> {
        let parent = self.get(p).await?.ok_or_else(|| unknown(p))?;
        let pb = parent.body();
        if pb.state != TenantState::Active {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("parent tenant {} is {}, not active", pb.name, pb.state),
            ));
        }
        if pb.suspended {
            return Err(SprigError::new(
                SprigCode::TenantSuspended,
                format!("parent tenant {} is suspended", pb.name),
            ));
        }
        if let Some(m) = &pb.tree_lock {
            return Err(SprigError::new(
                SprigCode::TenantTreeBusy,
                format!("parent tenant {} is being moved ({m}); try again", pb.name),
            ));
        }
        if pb.depth + 1 > max_depth {
            return Err(SprigError::new(
                SprigCode::TenantDepthExceeded,
                format!(
                    "a tenant under {} would be at depth {}, past tenant.max_depth {max_depth}",
                    pb.name,
                    pb.depth + 1
                ),
            ));
        }
        Ok(parent)
    }

    /// Changes `id`'s entry with `f` in a compare-and-swap loop; `f`
    /// returns `false` to leave it unchanged. Returns the entry after.
    ///
    /// # Errors
    /// 39072 for a missing entry; `f`'s errors; the store's errors.
    pub async fn update<F>(&self, id: TenantId, mut f: F) -> Result<StoredEntry>
    where
        F: FnMut(&mut DirectoryEntry) -> Result<bool> + Send,
    {
        let key = self.key(id)?;
        for _ in 0..WRITE_ATTEMPTS {
            let mut cur = self.get(id).await?.ok_or_else(|| unknown(id))?;
            if !f(&mut cur.record.body)? {
                return Ok(cur);
            }
            let r = self
                .global
                .txn(Txn {
                    when: vec![Cmp::ModRev(key.clone(), cur.mod_revision)],
                    then: vec![Op::Put {
                        key: key.clone(),
                        value: cur.record.encode()?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                cur.mod_revision = r.revision;
                return Ok(cur);
            }
        }
        Err(SprigError::new(
            SprigCode::MetaUnavailable,
            format!("tenant {id}'s directory entry kept changing; try again"),
        ))
    }

    /// Chapter 23's `delete_tenant`: the entry becomes `deleting` and the
    /// parent's child count drops, in one transaction guarded by both mod
    /// revisions. A tenant already `deleting` is returned as is.
    ///
    /// # Errors
    /// 39362 while it has children; 39072 unless `active` or `failed`; the
    /// store's errors.
    pub async fn mark_deleting(&self, id: TenantId) -> Result<StoredEntry> {
        for _ in 0..WRITE_ATTEMPTS {
            let mut cur = self.get(id).await?.ok_or_else(|| unknown(id))?;
            let b = cur.body();
            if b.state == TenantState::Deleting {
                return Ok(cur);
            }
            if b.child_count > 0 {
                return Err(SprigError::new(
                    SprigCode::TenantHasChildren,
                    format!(
                        "tenant {} has {} child tenants; delete them first",
                        b.name, b.child_count
                    ),
                ));
            }
            if !matches!(b.state, TenantState::Active | TenantState::Failed) {
                return Err(SprigError::new(
                    SprigCode::TenantNotReady,
                    format!(
                        "tenant {} is {}; only an active or failed tenant can be deleted",
                        b.name, b.state
                    ),
                ));
            }
            let key = self.key(id)?;
            let mut when = vec![Cmp::ModRev(key.clone(), cur.mod_revision)];
            let mut then = Vec::new();
            if let Some(p) = b.parent() {
                let mut parent = self.get(p).await?.ok_or_else(|| unknown(p))?;
                let pk = self.key(p)?;
                when.push(Cmp::ModRev(pk.clone(), parent.mod_revision));
                parent.record.body.child_count = parent.record.body.child_count.saturating_sub(1);
                then.push(Op::Put {
                    key: pk,
                    value: parent.record.encode()?,
                    lease: None,
                });
            }
            cur.record.body.state = TenantState::Deleting;
            then.push(Op::Put {
                key,
                value: cur.record.encode()?,
                lease: None,
            });
            let r = self
                .global
                .txn(Txn {
                    when,
                    then,
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                cur.mod_revision = r.revision;
                tracing::info!(tenant = %id, name = %cur.body().name, "tenant marked deleting");
                return Ok(cur);
            }
        }
        Err(SprigError::new(
            SprigCode::MetaUnavailable,
            format!("tenant {id}'s directory entry kept changing; try again"),
        ))
    }

    /// Removes a `deleting` tenant's entry and its name: the end of
    /// cleanup. The id is never reused.
    ///
    /// # Errors
    /// 39072 unless the tenant is `deleting`; the store's errors.
    pub async fn remove(&self, entry: &StoredEntry) -> Result<()> {
        let b = entry.body();
        if b.state != TenantState::Deleting {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {} is {}, not deleting", b.name, b.state),
            ));
        }
        let key = self.key(b.tenant_id)?;
        let name_key = self.name_key(&b.name)?;
        let mut then = vec![Op::Delete(key.clone())];
        // The name only if it still names this tenant.
        if let Some(kv) = self
            .global
            .get(&name_key, Consistency::Linearizable)
            .await?
            && Record::<NameEntry>::decode(&kv.value)?.body.tenant_id == b.tenant_id
        {
            then.push(Op::Delete(name_key));
        }
        let r = self
            .global
            .txn(Txn {
                when: vec![Cmp::ModRev(key, entry.mod_revision)],
                then,
                otherwise: Vec::new(),
            })
            .await?;
        if !r.succeeded {
            return Err(SprigError::new(
                SprigCode::CatalogConflict,
                format!("tenant {}'s entry changed during cleanup", b.name),
            ));
        }
        tracing::info!(tenant = %b.tenant_id, name = %b.name, "tenant removed from the directory");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
