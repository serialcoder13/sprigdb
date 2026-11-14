//! `sprig-geo`: geodesic functions, axis order, S2 cells and coverings.
//!
//! Layer: 2 data layer.
//!
//! - [`mysql`]: MySQL 8.4's geographic numbers for SRID 4326 (the
//!   Andoyer distance Boost.Geometry computes, `ST_Distance_Sphere`'s
//!   haversine), WKT axis swaps and the bounding box a distance predicate
//!   may use.
//! - [`s2`]: S2 cell ids, leaf cells of locations and coverings.
//! - [`region`]: the regions of SprigDB's own APIs (circle, box,
//!   polygon) with their bounding caps, S2 ranges and exact tests: WGS 84
//!   distances by Karney's method (`geographiclib-rs`), great-circle edges
//!   for polygons.

// Cube-face math is written as S2 writes it: f, i, j, u, v, s, t.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::many_single_char_names
)]

pub mod mysql;
pub mod region;
pub mod s2;
