//! Regions of SprigDB's own APIs (chapter 36, "Semantics per front end":
//! key-value, vectors, analytics, REST): WGS 84 distances by Karney's
//! method, great-circle polygon edges, S2 coverings for the `_sprig_s2`
//! index.
//!
//! ```text
//! query(region):
//!  1. ranges = cover(region's bounding cap) — a circle's cap uses radius /
//!     6,335,000 m, below the ellipsoid's smallest radius of curvature, so it
//!     is never too small
//!  2. candidates: _sprig_s2 BETWEEN each range
//!  3. exact test: Karney distance <= r; box by latitude and longitude;
//!     polygon by the winding of its great-circle edges
//! ```

use geographiclib_rs::{Geodesic, InverseGeodesic};

use crate::s2::{self, Cap, Point};

/// Metres per radian used for caps around ellipsoidal circles (chapter 36).
pub const CAP_EARTH_RADIUS_M: f64 = 6_335_000.0;
/// `geo.s2_index_min_level`.
pub const QUERY_MIN_LEVEL: u8 = 4;
/// `geo.s2_index_max_level`.
pub const QUERY_MAX_LEVEL: u8 = 20;
/// `geo.s2_query_max_cells`.
pub const QUERY_MAX_CELLS: usize = 20;

fn wgs84() -> &'static Geodesic {
    static G: std::sync::OnceLock<Geodesic> = std::sync::OnceLock::new();
    G.get_or_init(Geodesic::wgs84)
}

/// The WGS 84 geodesic distance in metres (Karney).
#[must_use]
pub fn distance_m(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    wgs84().inverse(lat1, lng1, lat2, lng2)
}

/// A region.
#[derive(Debug, Clone, PartialEq)]
pub enum Region {
    /// Points within `radius_m` metres of a center.
    Circle {
        /// Latitude, degrees.
        lat: f64,
        /// Longitude, degrees.
        lng: f64,
        /// Metres.
        radius_m: f64,
    },
    /// A latitude-longitude box; `west > east` crosses the antimeridian.
    Box {
        /// Southern latitude.
        south: f64,
        /// Western longitude.
        west: f64,
        /// Northern latitude.
        north: f64,
        /// Eastern longitude.
        east: f64,
    },
    /// A polygon with great-circle edges, `(lat, lng)` vertices, one ring
    /// (the closing vertex may repeat the first).
    Polygon(Vec<(f64, f64)>),
}

/// Why a region is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid(pub String);

fn check(lat: f64, lng: f64) -> Result<(), Invalid> {
    if !lat.is_finite()
        || !lng.is_finite()
        || !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lng)
    {
        return Err(Invalid(format!(
            "latitude {lat} or longitude {lng} out of range"
        )));
    }
    Ok(())
}

impl Region {
    /// Checks coordinates, radius and the polygon's ring.
    ///
    /// # Errors
    /// The reason.
    pub fn validate(&self, max_vertices: usize) -> Result<(), Invalid> {
        match self {
            Self::Circle { lat, lng, radius_m } => {
                check(*lat, *lng)?;
                if !radius_m.is_finite() || *radius_m < 0.0 {
                    return Err(Invalid(format!(
                        "radius {radius_m} must be 0 or more metres"
                    )));
                }
            }
            Self::Box {
                south,
                west,
                north,
                east,
            } => {
                check(*south, *west)?;
                check(*north, *east)?;
                if south > north {
                    return Err(Invalid("a box's south is above its north".into()));
                }
            }
            Self::Polygon(v) => {
                if v.len() > max_vertices {
                    return Err(Invalid(format!(
                        "{} vertices; at most {max_vertices}",
                        v.len()
                    )));
                }
                let ring = ring(v);
                if ring.len() < 3 {
                    return Err(Invalid(
                        "a polygon needs at least 3 distinct vertices".into(),
                    ));
                }
                for (la, lo) in v {
                    check(*la, *lo)?;
                }
            }
        }
        Ok(())
    }

    /// A cap containing the region.
    #[must_use]
    pub fn cap(&self) -> Cap {
        match self {
            Self::Circle { lat, lng, radius_m } => {
                let r = radius_m / CAP_EARTH_RADIUS_M;
                if r >= std::f64::consts::FRAC_PI_2 {
                    return Cap::whole();
                }
                Cap::around(*lat, *lng, r * (1.0 + 1e-9) + 1e-9)
            }
            Self::Box {
                south,
                west,
                north,
                east,
            } => {
                let width = if west <= east {
                    east - west
                } else {
                    360.0 - (west - east)
                };
                if width >= 180.0 || north - south >= 180.0 {
                    return Cap::whole();
                }
                // The boundary, densely sampled: parallels curve away from
                // great circles, so corners alone don't bound the box.
                let mut pts = Vec::new();
                for k in 0..=32 {
                    let f = f64::from(k) / 32.0;
                    let lng = west + width * f;
                    pts.push(s2::point(*south, lng));
                    pts.push(s2::point(*north, lng));
                    let lat = south + (north - south) * f;
                    pts.push(s2::point(lat, *west));
                    pts.push(s2::point(lat, west + width));
                }
                let mut c = Cap::bounding(&pts);
                c.radius *= 1.0 + 1e-6;
                c
            }
            Self::Polygon(v) => {
                let pts: Vec<Point> = ring(v).iter().map(|(a, b)| s2::point(*a, *b)).collect();
                Cap::bounding(&pts)
            }
        }
    }

    /// Leaf-id ranges covering the region.
    #[must_use]
    pub fn ranges(&self) -> Vec<(u64, u64)> {
        let cap = self.cap();
        if cap.radius >= std::f64::consts::PI {
            return vec![(0, u64::MAX)];
        }
        s2::cover(&cap, QUERY_MIN_LEVEL, QUERY_MAX_LEVEL, QUERY_MAX_CELLS)
    }

    /// The exact test.
    #[must_use]
    pub fn contains(&self, lat: f64, lng: f64) -> bool {
        match self {
            Self::Circle {
                lat: clat,
                lng: clng,
                radius_m,
            } => distance_m(*clat, *clng, lat, lng) <= *radius_m,
            Self::Box {
                south,
                west,
                north,
                east,
            } => {
                lat >= *south
                    && lat <= *north
                    && if west <= east {
                        lng >= *west && lng <= *east
                    } else {
                        lng >= *west || lng <= *east
                    }
            }
            Self::Polygon(v) => {
                let p = s2::point(lat, lng);
                let pts: Vec<Point> = ring(v).iter().map(|(a, b)| s2::point(*a, *b)).collect();
                // On a vertex or an edge counts as inside (MongoDB's
                // $geoWithin includes the boundary).
                if pts.iter().any(|q| s2::angle(*q, p) < 1e-12) {
                    return true;
                }
                winding(&pts, p).abs() > std::f64::consts::PI
            }
        }
    }

    /// The distance from the region's center (circles) or 0.
    #[must_use]
    pub fn center(&self) -> Option<(f64, f64)> {
        match self {
            Self::Circle { lat, lng, .. } => Some((*lat, *lng)),
            _ => None,
        }
    }
}

/// The ring without a repeated closing vertex.
fn ring(v: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let mut r = v.to_vec();
    if r.len() > 1 && r.first() == r.last() {
        r.pop();
    }
    r
}

/// The total turning angle at `p` of the ring's great-circle edges:
/// about ±2π inside, 0 outside.
fn winding(pts: &[Point], p: Point) -> f64 {
    let dot = |a: Point, b: Point| a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    let cross = |a: Point, b: Point| {
        [
            a[1] * b[2] - a[2] * b[1],
            a[2] * b[0] - a[0] * b[2],
            a[0] * b[1] - a[1] * b[0],
        ]
    };
    let mut total = 0.0;
    for i in 0..pts.len() {
        let a = pts[i];
        let b = pts[(i + 1) % pts.len()];
        let y = dot(p, cross(a, b));
        let x = dot(a, b) - dot(p, a) * dot(p, b);
        total += y.atan2(x);
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn karney_known_answers() {
        // GeographicLib's documented example (GeodSolve -i): JFK (40.6,
        // -73.8) to LHR (51.6, -0.5) is 5,551,759.400319 m on WGS 84.
        let d = distance_m(40.6, -73.8, 51.6, -0.5);
        assert!((d - 5_551_759.400).abs() < 0.01, "{d}");
        // MySQL's documented ST_Distance example in exact form (chapter 36):
        // POINT(1 1) to POINT(2 2) is 156,876.149 m by Karney.
        let d = distance_m(1.0, 1.0, 2.0, 2.0);
        assert!((d - 156_876.149).abs() < 0.01, "{d}");
        assert!(distance_m(10.0, 20.0, 10.0, 20.0).abs() < 1e-9);
    }

    #[test]
    fn regions() {
        let c = Region::Circle {
            lat: 19.07,
            lng: 72.87,
            radius_m: 5000.0,
        };
        assert!(c.contains(19.07, 72.87));
        assert!(c.contains(19.10, 72.87)); // about 3.3 km north
        assert!(!c.contains(19.12, 72.87)); // about 5.5 km
        let b = Region::Box {
            south: -10.0,
            west: 170.0,
            north: 10.0,
            east: -170.0,
        };
        assert!(b.contains(0.0, 179.5) && b.contains(0.0, -175.0) && !b.contains(0.0, 0.0));
        let tri = Region::Polygon(vec![(0.0, 0.0), (0.0, 10.0), (10.0, 0.0), (0.0, 0.0)]);
        assert!(tri.contains(2.0, 2.0));
        assert!(!tri.contains(8.0, 8.0));
        assert!(tri.contains(0.0, 0.0));
        assert!(tri.validate(10).is_ok());
        assert!(
            Region::Polygon(vec![(0.0, 0.0), (1.0, 1.0)])
                .validate(10)
                .is_err()
        );
        assert!(
            Region::Circle {
                lat: 91.0,
                lng: 0.0,
                radius_m: 1.0
            }
            .validate(10)
            .is_err()
        );
        // Every region's cap holds the points it contains.
        for r in [&c, &b, &tri] {
            let cap = r.cap();
            for (la, lo) in [
                (19.07, 72.87),
                (19.1, 72.87),
                (0.0, 179.5),
                (0.0, -175.0),
                (2.0, 2.0),
            ] {
                if r.contains(la, lo) {
                    assert!(cap.contains(s2::point(la, lo)), "{r:?} {la} {lo}");
                    let id = s2::leaf(la, lo);
                    assert!(
                        r.ranges().iter().any(|(a, z)| *a <= id && id <= *z),
                        "{r:?}"
                    );
                }
            }
        }
    }
}
