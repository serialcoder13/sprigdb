//! Physical schema names (chapter 15, "Physical schemas").
//!
//! The patterns live in `sprig_common::names`; these are the chapter's
//! named helpers, plus picking a tenant's schemas out of a cluster's
//! database list.

use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_common::names::SchemaName;

/// `t<tenant>_d<db>_g<group>_s<shard>`.
#[must_use]
pub fn shard_schema(t: TenantId, db: DbId, g: GroupId, s: ShardId) -> String {
    SchemaName::shard(t, db, g, s).to_string()
}

/// `t<tenant>_d<db>_ref` (R1).
#[must_use]
pub fn reference_schema(t: TenantId, db: DbId) -> String {
    SchemaName::reference(t, db).to_string()
}

/// `t<tenant>_sprig`.
#[must_use]
pub fn catalog_schema(t: TenantId) -> String {
    SchemaName::catalog(t).to_string()
}

/// The schemas among `databases` (a cluster's `SHOW DATABASES`) that
/// belong to `tenant`: its catalog, shard and reference schemas, sorted
/// with the catalog first. Names that don't parse as SprigDB patterns are
/// never returned, so a caller can't touch another tenant's or a system
/// schema by accident.
#[must_use]
pub fn tenant_schemas<'a>(
    tenant: TenantId,
    databases: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let mut out: Vec<SchemaName> = databases
        .into_iter()
        .filter_map(|d| d.parse::<SchemaName>().ok())
        .filter(|s| s.tenant() == tenant)
        .collect();
    out.sort_by_key(|s| (!matches!(s, SchemaName::Catalog { .. }), *s));
    out.dedup();
    out.into_iter().map(|s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_patterns() {
        assert_eq!(
            shard_schema(TenantId(42), DbId(1), GroupId(3), ShardId(17)),
            "t42_d1_g3_s17"
        );
        assert_eq!(reference_schema(TenantId(42), DbId(1)), "t42_d1_ref");
        assert_eq!(catalog_schema(TenantId(42)), "t42_sprig");
    }

    #[test]
    fn tenant_schemas_picks_only_the_tenants_own() {
        let dbs = [
            "mysql",
            "t42_d2_g2_s2",
            "t42_sprig",
            "t420_sprig",
            "t42_d1_g1_s1",
            "t42_d1_g1_s1x",
            "t7_d1_g1_s1",
            "__sprig_sys",
            "t042_sprig",
        ];
        assert_eq!(
            tenant_schemas(TenantId(42), dbs),
            vec!["t42_sprig", "t42_d1_g1_s1", "t42_d2_g2_s2"]
        );
    }
}
