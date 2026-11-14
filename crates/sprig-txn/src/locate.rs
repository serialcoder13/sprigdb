//! [`MapLocator`]: where an anchor's decision rows live now, from the
//! tenant's shard maps.
//!
//! An anchor names a database, a table group and a hash point (the start
//! of the anchor shard's range when the transaction ran):
//!
//! ```text
//! locate(tenant, d<db>.g<group>@hash_point):
//!   group 0: refused (the catalog group holds no decision rows)
//!   the group has a map shardmaps/<tenant>/<group> (a sharded group):
//!     range = the range holding hash_point now
//!     → (range.cluster, t<tenant>_d<db>_g<group>_s<range.shard>)
//!   no map (a database's default group, which lives with the catalog):
//!     cluster = the cluster of the tenant map shardmaps/<tenant>/0
//!     → (cluster, the one schema t<tenant>_d<db>_g<group>_s<n> on it)
//! ```
//!
//! A split, merge or move copies `__sprig_dtx` whole into every child, so
//! the shard whose range holds the hash point now always has the rows. Maps
//! are read linearizably: racing an `abort` row into a stale location could
//! record a false abort. A default group with no schema, or with more than
//! one, is an error rather than a guess, for the same reason.

use std::sync::Arc;

use sprig_common::ids::{ClusterId, DbId, GroupId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::{Placement, RoutingHash, ShardMapStore, TenantMaps, shard_schema};

use crate::backend::AnchorLocator;
use crate::xid::AnchorRef;

/// The schemas of a database's default group on a cluster.
#[async_trait::async_trait]
pub trait GroupSchemas: Send + Sync {
    /// Every schema `t<tenant>_d<db>_g<group>_s<n>` on `cluster`.
    ///
    /// # Errors
    /// When the cluster can't be asked.
    async fn group_schemas(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        db: DbId,
        group: GroupId,
    ) -> Result<Vec<String>>;
}

#[async_trait::async_trait]
impl GroupSchemas for crate::mysql::MyAdmin {
    async fn group_schemas(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        db: DbId,
        group: GroupId,
    ) -> Result<Vec<String>> {
        crate::mysql::MyAdmin::group_schemas(self, cluster, tenant, db, group)
            .await
            .map_err(|e| {
                SprigError::new(
                    SprigCode::ClusterUnavailable,
                    format!("list the schemas of group {group} on {cluster}: {e}"),
                )
            })
    }
}

/// The names among `names` that are exactly shard schemas of `group` of
/// database `db` of `tenant`, sorted.
#[must_use]
pub fn group_schemas_of(
    tenant: TenantId,
    db: DbId,
    group: GroupId,
    names: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut out: Vec<String> = names
        .into_iter()
        .filter(|n| {
            matches!(n.parse::<SchemaName>(), Ok(SchemaName::Shard { tenant: t, db: d, group: g, .. })
                if t == tenant && d == db && g == group)
        })
        .collect();
    out.sort();
    out
}

/// An [`AnchorLocator`] over a DC tier's shard maps (module docs).
#[derive(Clone)]
pub struct MapLocator {
    groups: ShardMapStore,
    tenants: TenantMaps,
    schemas: Arc<dyn GroupSchemas>,
}

impl std::fmt::Debug for MapLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapLocator")
            .field("groups", &self.groups)
            .finish_non_exhaustive()
    }
}

impl MapLocator {
    /// Group maps from `groups`, tenant maps from `tenants` (both over the
    /// tenants' home DC tier), default-group schemas from `schemas`.
    #[must_use]
    pub fn new(groups: ShardMapStore, tenants: TenantMaps, schemas: Arc<dyn GroupSchemas>) -> Self {
        Self {
            groups,
            tenants,
            schemas,
        }
    }
}

fn not_found(tenant: TenantId, anchor: AnchorRef, why: &str) -> SprigError {
    SprigError::new(
        SprigCode::ShardMapUnavailable,
        format!("can't locate anchor {anchor} of tenant {tenant}: {why}"),
    )
}

#[async_trait::async_trait]
impl AnchorLocator for MapLocator {
    async fn locate(&self, tenant: TenantId, anchor: AnchorRef) -> Result<(ClusterId, String)> {
        if anchor.group.0 == 0 {
            return Err(not_found(tenant, anchor, "group 0 holds no decision rows"));
        }
        match self.groups.load(tenant, anchor.group).await {
            Ok((map, _)) => {
                let r = map.route(RoutingHash(anchor.hash_point));
                return Ok((
                    r.cluster.clone(),
                    shard_schema(tenant, anchor.db, anchor.group, r.shard),
                ));
            }
            Err(e) if e.code() == Some(SprigCode::ShardMapUnavailable) => {}
            Err(e) => return Err(e),
        }
        let (tmap, _) = self.tenants.load(tenant).await?;
        let cluster = Placement::of(&tmap)?.cluster;
        let found = self
            .schemas
            .group_schemas(&cluster, tenant, anchor.db, anchor.group)
            .await?;
        match found.as_slice() {
            [one] => Ok((cluster, one.clone())),
            [] => Err(not_found(
                tenant,
                anchor,
                &format!("no schema of group {} on {cluster}", anchor.group),
            )),
            many => Err(not_found(
                tenant,
                anchor,
                &format!(
                    "group {} has no map and {} schemas on {cluster}",
                    anchor.group,
                    many.len()
                ),
            )),
        }
    }
}

#[cfg(test)]
mod tests;
