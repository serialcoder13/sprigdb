//! `sprig-geo`: geodesic functions, axis order and (later) S2 coverings,
//! GeoJSON and WKB.
//!
//! Layer: 2 data layer. Design: [chapter 36](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/36-geo.md) (Geospatial) of the SprigDB low-level design.
//!
//! Built so far (R4, the MySQL persona's part, chapter 36 steps 1 to 3 in
//! part): [`mysql`], MySQL 8.4's geographic numbers for SRID 4326 (the
//! Andoyer distance Boost.Geometry computes, `ST_Distance_Sphere`'s
//! haversine), WKT axis swaps and the bounding box a distance predicate
//! may use. No third-party crates: the georust stack isn't vendored yet,
//! and these formulas are closed-form. S2, H3 and the other models' parts
//! follow in R6.

pub mod mysql;
