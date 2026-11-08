//! The `authz` module in the gateway's catalog cache: [`AuthzState`], the
//! tenant's roles, role edges and privileges at one auth version, loaded
//! into every snapshot by [`AuthzExtension`] in the snapshot's own
//! transaction.
//!
//! The tables are small (a tenant has users and roles, not rows), so every
//! load reads them whole; the cache reloads a tenant when its auth version
//! moves, which every gateway learns from the directory watch (chapter 08,
//! the same path settings changes take). A catalog whose module isn't
//! migrated yet loads as [`AuthzState::unmigrated`]: every user then holds
//! everything, as before M0.5, until the gateway migrates it (see
//! [`super::migrate`]).

use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use sprig_catalog::{CatalogExtension, ExtState, Migration, SnapshotTx, TenantCatalog, VersionKind};
use sprig_common::Result;
use sprig_common::ids::{RoleId, UserId};

use super::store::{AUTHZ_MIGRATIONS, AuthzRows, AuthzStore, EdgeRow, Grantee, MODULE, PrivRow, RoleRow};

/// One tenant's authorization rows at one auth version, indexed.
#[derive(Debug, Clone, Default)]
pub struct AuthzState {
    migrated: bool,
    rows: Arc<AuthzRows>,
    privs: HashMap<Grantee, Vec<usize>>,
    edges: HashMap<Grantee, Vec<EdgeRow>>,
    roles: HashMap<RoleId, usize>,
    role_names: HashMap<(String, String), RoleId>,
}

impl ExtState for AuthzState {
    fn bytes(&self) -> usize {
        256 + self.rows.privileges.len() * 128
            + self.rows.edges.len() * 48
            + self.rows.roles.len() * 96
    }
}

impl AuthzState {
    /// The state of a catalog without the module: every user holds
    /// everything.
    #[must_use]
    pub fn unmigrated() -> Self {
        Self::default()
    }

    /// The state of `rows`.
    #[must_use]
    pub fn new(rows: AuthzRows) -> Self {
        let mut privs: HashMap<Grantee, Vec<usize>> = HashMap::new();
        for (i, p) in rows.privileges.iter().enumerate() {
            privs.entry(p.grantee).or_default().push(i);
        }
        let mut edges: HashMap<Grantee, Vec<EdgeRow>> = HashMap::new();
        for e in &rows.edges {
            edges.entry(e.grantee).or_default().push(*e);
        }
        let roles = rows
            .roles
            .iter()
            .enumerate()
            .map(|(i, r)| (r.role_id, i))
            .collect();
        let role_names = rows
            .roles
            .iter()
            .map(|r| ((r.name.clone(), r.host.to_ascii_lowercase()), r.role_id))
            .collect();
        Self {
            migrated: true,
            rows: Arc::new(rows),
            privs,
            edges,
            roles,
            role_names,
        }
    }

    /// The tenant's state in `catalog`; `None` if the module isn't
    /// registered with the cache.
    #[must_use]
    pub fn of(catalog: &TenantCatalog) -> Option<&AuthzState> {
        catalog.ext.get::<AuthzState>(MODULE)
    }

    /// Whether the catalog has the module's tables.
    #[must_use]
    pub fn is_migrated(&self) -> bool {
        self.migrated
    }

    /// All rows.
    #[must_use]
    pub fn rows(&self) -> &AuthzRows {
        &self.rows
    }

    /// The privilege rows `grantee` holds directly.
    pub fn privileges_of(&self, grantee: Grantee) -> impl Iterator<Item = &PrivRow> {
        self.privs
            .get(&grantee)
            .into_iter()
            .flatten()
            .map(|i| &self.rows.privileges[*i])
    }

    /// The roles `grantee` holds directly.
    #[must_use]
    pub fn edges_of(&self, grantee: Grantee) -> &[EdgeRow] {
        self.edges.get(&grantee).map_or(&[], Vec::as_slice)
    }

    /// The role `id`.
    #[must_use]
    pub fn role(&self, id: RoleId) -> Option<&RoleRow> {
        self.roles.get(&id).map(|i| &self.rows.roles[*i])
    }

    /// The role `'name'@'host'` (host compared case-insensitively).
    #[must_use]
    pub fn role_named(&self, name: &str, host: &str) -> Option<RoleId> {
        self.role_names
            .get(&(name.to_owned(), host.to_ascii_lowercase()))
            .copied()
    }

    /// `roles` and every role they contain, breadth first, each once.
    #[must_use]
    pub fn closure(&self, roles: &[RoleId]) -> Vec<RoleId> {
        let mut seen: HashSet<RoleId> = HashSet::new();
        let mut out = Vec::new();
        let mut q: VecDeque<RoleId> = roles.iter().copied().collect();
        while let Some(r) = q.pop_front() {
            if !seen.insert(r) || self.role(r).is_none() {
                continue;
            }
            out.push(r);
            for e in self.edges_of(Grantee::Role(r)) {
                q.push_back(e.role);
            }
        }
        out
    }

    /// The roles granted to `user` directly.
    #[must_use]
    pub fn granted_roles(&self, user: UserId) -> Vec<RoleId> {
        self.edges_of(Grantee::User(user))
            .iter()
            .filter(|e| self.role(e.role).is_some())
            .map(|e| e.role)
            .collect()
    }

    /// `user`'s default roles.
    #[must_use]
    pub fn default_roles(&self, user: UserId) -> Vec<RoleId> {
        self.edges_of(Grantee::User(user))
            .iter()
            .filter(|e| e.is_default && self.role(e.role).is_some())
            .map(|e| e.role)
            .collect()
    }

    /// Whether granting `role` to role `to` would close a cycle (`to` is
    /// `role` or contained in it).
    #[must_use]
    pub fn would_cycle(&self, role: RoleId, to: RoleId) -> bool {
        role == to || self.closure(&[role]).contains(&to)
    }
}

/// `sprig-auth`'s chapter 29 catalog module: the tables' migrations, and
/// [`AuthzState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthzExtension;

#[async_trait::async_trait]
impl CatalogExtension for AuthzExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        AUTHZ_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Auth)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = AuthzStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(AuthzState::unmigrated()));
        }
        let rows = store.load(conn).await?;
        Ok(Arc::new(AuthzState::new(rows)))
    }
}

/// `prev` as an [`AuthzState`], for tests.
#[must_use]
pub fn downcast(prev: &Arc<dyn ExtState>) -> Option<&AuthzState> {
    let any: &dyn Any = &**prev;
    any.downcast_ref::<AuthzState>()
}
