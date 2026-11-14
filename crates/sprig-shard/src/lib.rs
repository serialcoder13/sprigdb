//! `sprig-shard`: table groups, key normalization and hashing, and shard
//! maps.
//!
//! Layer: 2 operations.
//!
//! The pieces the mover needs:
//!
//! - [`names`]: physical schema names of shards, reference schemas and
//!   catalogs (`shard_schema`, `reference_schema`, `catalog_schema`), and
//!   listing a tenant's schemas among a cluster's databases;
//! - [`map`]: the whole-tenant map. While a tenant isn't sharded, every
//!   database of the tenant lives on its catalog's cluster (one default group per database, its schema
//!   `t<tenant>_d<db>_g<group>_s<shard>` with ids from the catalog's
//!   counters, recorded in `databases.options.shards`), so the one map a
//!   gateway routes by is the catalog's `shardmaps/<tenant>/0`. The mover
//!   holds it, flips it to the target cluster with the epoch raised, and
//!   releases it; [`map::TenantMaps`] reads and writes it with
//!   compare-and-put on the key's `mod_revision`.
//!
//! - [`home`]: a tenant's home DC in the directory, and the commit point
//!   of a move between DCs: both DCs' maps and the directory
//!   entry in one transaction (single-etcd mode).
//!
//! Sharding:
//!
//! - [`key`], [`normalize`], [`collation`], [`hash`]: key specs and
//!   values, canonical bytes, collation weights computed in Rust, and
//!   `routing_hash` (`xxh3_64`);
//! - [`shardmap`]: data-group maps with many ranges, the routing lookup,
//!   the etcd store and the gateways' watched cache;
//! - [`groups`]: table groups and members in the catalog (the `sharding`
//!   catalog module), `colocation_safe`;
//! - [`rk`]: the `_sprig_rk` column's statements;
//! - [`reference`](mod@reference): reference-table copy states and read decisions;
//! - [`ids`]: ID blocks for `AUTO_INCREMENT`;
//! - [`lookup`]: unique and automatic lookup indexes, the consistent-lookup
//!   claim and release, routing through lookups, build and orphan scan.
//! - [`fk`]: foreign keys across groups, the `foreign_keys` rows the
//!   router enforces.
//! - [`pins`]: sharding pins (`SPRIG PIN`): the `group_pins`
//!   table, the conflict rules (39410) and the split-group rule (39411).
//! - [`colocate`]: joins across data models, the SQL part: the type map from SQL types to
//!   key classes, MySQL's comparison type for `a = b`, and `colocated`
//!   under SQL's or MongoDB's rules.

pub mod collation;
pub mod colocate;
pub mod fk;
pub mod groups;
pub mod hash;
pub mod home;
pub mod ids;
pub mod key;
pub mod lookup;
pub mod map;
pub mod names;
pub mod normalize;
pub mod pins;
pub mod reference;
pub mod rk;
pub mod shardmap;

pub use collation::{Charset, Collation, CollationId};
pub use hash::{hash_key, routing_hash};
pub use key::{
    DateTimeParts, DecimalRef, GroupKind, KeyClass, KeySpec, KeyValue, KeyValueOwned,
    MemberRouting, RoutingHash, pad_binary,
};
pub use map::{Hold, Placement, TenantMaps};
pub use names::{catalog_schema, reference_schema, shard_schema, tenant_schemas};
pub use normalize::{NormOutcome, Normalizer, StdNormalizer};
pub use shardmap::{
    RangeEntry, Routed, ShardMap, ShardMapCache, ShardMapStore, route_in_list, route_key,
};
