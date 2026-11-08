//! `sprig-shard`: table groups, key normalization and hashing, and shard
//! maps.
//!
//! Layer: 2 operations. Design: [chapter 15](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/15-sharding.md) (Sharding model) of the SprigDB low-level design.
//!
//! What exists is build step 1, the R0 pieces the mover (chapter 10) needs:
//!
//! - [`names`]: physical schema names of shards, reference schemas and
//!   catalogs (`shard_schema`, `reference_schema`, `catalog_schema`), and
//!   listing a tenant's schemas among a cluster's databases;
//! - [`map`]: the R0 tenant map. In R0 every database of a tenant lives on
//!   its catalog's cluster (one default group per database, its schema
//!   `t<tenant>_d<db>_g<group>_s<shard>` with ids from the catalog's
//!   counters, recorded in `databases.options.shards`), so the one map a
//!   gateway routes by is the catalog's `shardmaps/<tenant>/0`. The mover
//!   holds it, flips it to the target cluster with the epoch raised, and
//!   releases it; [`map::TenantMaps`] reads and writes it with
//!   compare-and-put on the key's `mod_revision`.
//!
//! - [`home`]: a tenant's home DC in the directory, and the commit point
//!   of a move between DCs (chapter 24): both DCs' maps and the directory
//!   entry in one transaction (single-etcd mode).
//!
//! Not yet (R1): table groups in the catalog, normalization, collation
//! weights, `routing_hash`, data-group maps with many ranges, `_sprig_rk`.

pub mod home;
pub mod map;
pub mod names;

pub use map::{Hold, Placement, TenantMaps};
pub use names::{catalog_schema, reference_schema, shard_schema, tenant_schemas};
