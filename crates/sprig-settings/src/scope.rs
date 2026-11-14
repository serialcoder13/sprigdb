//! Scopes and the context a setting is resolved in.
//!
//! Six scopes in two chains: the data chain object > database > tenant
//! (> parent tenant > ... > root tenant), and the operational chain
//! cluster > dc. Both end at global, then the built-in default.

use std::fmt;

use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};

/// Where a resolved value or bound came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    /// A table, collection, namespace, search index or function.
    Object,
    /// A database (schema).
    Database,
    /// The tenant itself.
    Tenant,
    /// An ancestor tenant's tenant-scope row.
    Ancestor(TenantId),
    /// One cluster.
    Cluster,
    /// One data center.
    Dc,
    /// Every data center ("system" level).
    Global,
    /// The registry's built-in default.
    Default,
}

impl fmt::Display for ScopeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopeKind::Object => f.write_str("object"),
            ScopeKind::Database => f.write_str("database"),
            ScopeKind::Tenant => f.write_str("tenant"),
            ScopeKind::Ancestor(t) => write!(f, "ancestor {t}"),
            ScopeKind::Cluster => f.write_str("cluster"),
            ScopeKind::Dc => f.write_str("dc"),
            ScopeKind::Global => f.write_str("global"),
            ScopeKind::Default => f.write_str("default"),
        }
    }
}

/// The address of one stored row of values and bounds.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    /// `/sprig/g/settings/`.
    Global,
    /// `/sprig/dc/<dc>/settings/`.
    Dc(DcId),
    /// `/sprig/dc/<dc>/cluster-settings/<cluster>/`.
    Cluster(ClusterId),
    /// A tenant's catalog, tenant scope. Ancestors' rows are tenant rows of
    /// the ancestor.
    Tenant(TenantId),
    /// A tenant's catalog, database scope.
    Database(TenantId, DbId),
    /// A tenant's catalog, object scope. Object ids are unique per tenant.
    Object(TenantId, ObjectId),
}

/// A scope level, relative to a [`SettingCtx`]: which of the context's
/// scopes a write targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Global.
    Global,
    /// The context's DC.
    Dc,
    /// The context's cluster.
    Cluster,
    /// The context's tenant.
    Tenant,
    /// The context's database.
    Database,
    /// The context's object.
    Object,
}

impl Level {
    pub(crate) const fn kind(self) -> ScopeKind {
        match self {
            Level::Global => ScopeKind::Global,
            Level::Dc => ScopeKind::Dc,
            Level::Cluster => ScopeKind::Cluster,
            Level::Tenant => ScopeKind::Tenant,
            Level::Database => ScopeKind::Database,
            Level::Object => ScopeKind::Object,
        }
    }
}

/// The context a key is resolved in.
#[derive(Debug, Clone, Copy)]
pub struct SettingCtx<'a> {
    /// The evaluating process's DC; the home DC for tenant-wide controller
    /// decisions.
    pub dc: &'a DcId,
    /// Set only for work on one cluster.
    pub cluster: Option<&'a ClusterId>,
    /// The tenant, for data-chain keys.
    pub tenant: Option<TenantId>,
    /// The tenant's ancestors, root first, not including the tenant.
    pub ancestry: &'a [TenantId],
    /// The database, within the tenant.
    pub db: Option<DbId>,
    /// The object, within the tenant.
    pub object: Option<ObjectId>,
}

impl<'a> SettingCtx<'a> {
    /// A process-level context: dc and global only.
    #[must_use]
    pub const fn process(dc: &'a DcId) -> Self {
        Self {
            dc,
            cluster: None,
            tenant: None,
            ancestry: &[],
            db: None,
            object: None,
        }
    }

    /// The same context for `tenant` with `ancestry` (root first).
    #[must_use]
    pub const fn for_tenant(self, tenant: TenantId, ancestry: &'a [TenantId]) -> Self {
        Self {
            tenant: Some(tenant),
            ancestry,
            db: None,
            object: None,
            ..self
        }
    }

    /// The same context for one cluster.
    #[must_use]
    pub const fn with_cluster(self, cluster: &'a ClusterId) -> Self {
        Self {
            cluster: Some(cluster),
            ..self
        }
    }

    /// The same context for database `db` of its tenant.
    #[must_use]
    pub const fn with_db(self, db: DbId) -> Self {
        Self {
            db: Some(db),
            ..self
        }
    }

    /// The same context for object `object` of its tenant. Set the
    /// object's database too ([`with_db`](Self::with_db)), so the
    /// database's rows apply.
    #[must_use]
    pub const fn with_object(self, object: ObjectId) -> Self {
        Self {
            object: Some(object),
            ..self
        }
    }

    /// The scopes of this context, widest first, as resolution walks them:
    /// global, dc, cluster, ancestors (root first), tenant, database, object.
    pub(crate) fn chain(&self) -> Vec<(ScopeKind, Scope)> {
        let mut chain = vec![
            (ScopeKind::Global, Scope::Global),
            (ScopeKind::Dc, Scope::Dc(self.dc.clone())),
        ];
        if let Some(c) = self.cluster {
            chain.push((ScopeKind::Cluster, Scope::Cluster(c.clone())));
        }
        if let Some(t) = self.tenant {
            chain.extend(
                self.ancestry
                    .iter()
                    .map(|&a| (ScopeKind::Ancestor(a), Scope::Tenant(a))),
            );
            chain.push((ScopeKind::Tenant, Scope::Tenant(t)));
            if let Some(db) = self.db {
                chain.push((ScopeKind::Database, Scope::Database(t, db)));
            }
            if let Some(o) = self.object {
                chain.push((ScopeKind::Object, Scope::Object(t, o)));
            }
        }
        chain
    }
}
