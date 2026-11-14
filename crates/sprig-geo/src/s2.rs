//! S2 cells: cell ids on the Hilbert curve over the cube's six faces,
//! written from the S2 geometry library's published definitions (the
//! quadratic projection, the 30-level Hilbert numbering with its four
//! orientations), and a simple covering.
//!
//! - [`leaf`]: the level-30 cell of a location, the value a `_sprig_s2`
//!   column stores. Nearby points share prefixes; a cell at any level is one
//!   contiguous range of leaf ids ([`CellId::range`]).
//! - [`cover`]: cells covering a region given by its bounding cap, chosen
//!   level by level from the faces down: every cell whose bounding cap
//!   meets the region's is kept, until the next level would pass
//!   `max_cells`. It covers a bounding cap and relies on the exact test:
//!   correct, scanning somewhat more than S2's
//!   own coverer.

use std::sync::OnceLock;

/// Levels: 0 (a face) to 30 (about 1 cm).
pub const MAX_LEVEL: u8 = 30;
const POS_BITS: u32 = 2 * MAX_LEVEL as u32 + 1;
const LOOKUP_BITS: u32 = 4;
const SWAP: u32 = 1;
const INVERT: u32 = 2;
const POS_TO_IJ: [[u32; 4]; 4] = [[0, 1, 3, 2], [0, 2, 3, 1], [3, 2, 0, 1], [3, 1, 0, 2]];
const POS_TO_ORIENTATION: [u32; 4] = [SWAP, 0, 0, INVERT | SWAP];

struct Lookup {
    pos: Vec<u32>,
    ij: Vec<u32>,
}

fn init_cell(t: &mut Lookup, level: u32, i: u32, j: u32, orig: u32, pos: u32, orientation: u32) {
    if level == LOOKUP_BITS {
        let ij = (i << LOOKUP_BITS) + j;
        t.pos[((ij << 2) + orig) as usize] = (pos << 2) + orientation;
        t.ij[((pos << 2) + orig) as usize] = (ij << 2) + orientation;
        return;
    }
    let (level, i, j, pos) = (level + 1, i << 1, j << 1, pos << 2);
    let r = POS_TO_IJ[orientation as usize];
    for (k, rk) in r.iter().enumerate() {
        init_cell(
            t,
            level,
            i + (rk >> 1),
            j + (rk & 1),
            orig,
            pos + k as u32,
            orientation ^ POS_TO_ORIENTATION[k],
        );
    }
}

fn lookup() -> &'static Lookup {
    static L: OnceLock<Lookup> = OnceLock::new();
    L.get_or_init(|| {
        let n = 1usize << (2 * LOOKUP_BITS + 2);
        let mut t = Lookup {
            pos: vec![0; n],
            ij: vec![0; n],
        };
        for o in [0, SWAP, INVERT, SWAP | INVERT] {
            init_cell(&mut t, 0, 0, 0, o, 0, o);
        }
        t
    })
}

/// A unit vector.
pub type Point = [f64; 3];

/// The point of a location in degrees.
#[must_use]
pub fn point(lat: f64, lng: f64) -> Point {
    let (la, lo) = (lat.to_radians(), lng.to_radians());
    [la.cos() * lo.cos(), la.cos() * lo.sin(), la.sin()]
}

/// A point's location in degrees.
#[must_use]
pub fn lat_lng(p: Point) -> (f64, f64) {
    let lat = p[2].atan2(p[0].hypot(p[1]));
    let lng = p[1].atan2(p[0]);
    (lat.to_degrees(), lng.to_degrees())
}

fn norm(p: Point) -> Point {
    let n = (p[0] * p[0] + p[1] * p[1] + p[2] * p[2]).sqrt();
    [p[0] / n, p[1] / n, p[2] / n]
}

/// The angle between two unit vectors, radians.
#[must_use]
pub fn angle(a: Point, b: Point) -> f64 {
    let cross = [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ];
    let c = (cross[0] * cross[0] + cross[1] * cross[1] + cross[2] * cross[2]).sqrt();
    c.atan2(a[0] * b[0] + a[1] * b[1] + a[2] * b[2])
}

fn face_uv(p: Point) -> (u8, f64, f64) {
    let (ax, ay, az) = (p[0].abs(), p[1].abs(), p[2].abs());
    let mut f = if ax > ay {
        if ax > az { 0 } else { 2 }
    } else if ay > az {
        1
    } else {
        2
    };
    if p[f as usize] < 0.0 {
        f += 3;
    }
    let (u, v) = match f {
        0 => (p[1] / p[0], p[2] / p[0]),
        1 => (-p[0] / p[1], p[2] / p[1]),
        2 => (-p[0] / p[2], -p[1] / p[2]),
        3 => (p[2] / p[0], p[1] / p[0]),
        4 => (p[2] / p[1], -p[0] / p[1]),
        _ => (-p[1] / p[2], -p[0] / p[2]),
    };
    (f, u, v)
}

fn uv_point(f: u8, u: f64, v: f64) -> Point {
    norm(match f {
        0 => [1.0, u, v],
        1 => [-u, 1.0, v],
        2 => [-u, -v, 1.0],
        3 => [-1.0, -v, -u],
        4 => [v, -1.0, -u],
        _ => [v, u, -1.0],
    })
}

fn uv_to_st(u: f64) -> f64 {
    if u >= 0.0 {
        0.5 * (1.0 + 3.0 * u).sqrt()
    } else {
        1.0 - 0.5 * (1.0 - 3.0 * u).sqrt()
    }
}

fn st_to_uv(s: f64) -> f64 {
    if s >= 0.5 {
        (4.0 * s * s - 1.0) / 3.0
    } else {
        (1.0 - 4.0 * (1.0 - s) * (1.0 - s)) / 3.0
    }
}

fn st_to_ij(s: f64) -> u32 {
    let max = (1u32 << MAX_LEVEL) - 1;
    ((s * f64::from(1u32 << MAX_LEVEL)).floor().max(0.0) as u32).min(max)
}

/// An S2 cell id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellId(pub u64);

impl CellId {
    /// The leaf cell of `(face, i, j)`.
    #[must_use]
    pub fn from_face_ij(f: u8, i: u32, j: u32) -> Self {
        let t = lookup();
        let mut n = u64::from(f) << (POS_BITS - 1);
        let mut bits = u32::from(f) & SWAP;
        let mask = (1u32 << LOOKUP_BITS) - 1;
        for k in (0..8).rev() {
            bits += ((i >> (k * LOOKUP_BITS)) & mask) << (LOOKUP_BITS + 2);
            bits += ((j >> (k * LOOKUP_BITS)) & mask) << 2;
            bits = t.pos[bits as usize];
            n |= u64::from(bits >> 2) << (k * 2 * LOOKUP_BITS);
            bits &= SWAP | INVERT;
        }
        Self(n * 2 + 1)
    }

    /// The face (0 to 5).
    #[must_use]
    pub const fn face(self) -> u8 {
        (self.0 >> POS_BITS) as u8
    }

    /// The lowest set bit.
    #[must_use]
    pub const fn lsb(self) -> u64 {
        self.0 & self.0.wrapping_neg()
    }

    /// The level (0 to 30).
    #[must_use]
    pub const fn level(self) -> u8 {
        MAX_LEVEL - (self.0.trailing_zeros() / 2) as u8
    }

    /// The ancestor at `level` (itself at its own level or finer).
    #[must_use]
    pub const fn parent(self, level: u8) -> Self {
        if level >= self.level() {
            return self;
        }
        let lsb = 1u64 << (2 * (MAX_LEVEL - level) as u32);
        Self((self.0 & lsb.wrapping_neg()) | lsb)
    }

    /// The face cell.
    #[must_use]
    pub const fn from_face(f: u8) -> Self {
        Self(((f as u64) << POS_BITS) + (1u64 << (2 * MAX_LEVEL as u32)))
    }

    /// The four children, in Hilbert order.
    #[must_use]
    pub fn children(self) -> [Self; 4] {
        let new_lsb = self.lsb() >> 2;
        let first = self.0 - self.lsb() + new_lsb;
        [
            Self(first),
            Self(first + 2 * new_lsb),
            Self(first + 4 * new_lsb),
            Self(first + 6 * new_lsb),
        ]
    }

    /// The leaf ids it holds, `[min, max]` inclusive.
    #[must_use]
    pub const fn range(self) -> (u64, u64) {
        let l = self.lsb();
        (self.0 - (l - 1), self.0 + (l - 1))
    }

    /// `(face, i, j)` of a leaf inside it (the encoding's).
    #[must_use]
    pub fn face_ij(self) -> (u8, u32, u32) {
        let t = lookup();
        let f = self.face();
        let mut i = 0u32;
        let mut j = 0u32;
        let mut bits = u32::from(f) & SWAP;
        let mut nbits = u32::from(MAX_LEVEL) - 7 * LOOKUP_BITS;
        for k in (0..8).rev() {
            let shift = k * 2 * LOOKUP_BITS + 1;
            bits += ((self.0 >> shift) as u32 & ((1 << (2 * nbits)) - 1)) << 2;
            bits = t.ij[bits as usize];
            i += (bits >> (LOOKUP_BITS + 2)) << (k * LOOKUP_BITS);
            j += ((bits >> 2) & ((1 << LOOKUP_BITS) - 1)) << (k * LOOKUP_BITS);
            bits &= SWAP | INVERT;
            nbits = LOOKUP_BITS;
        }
        (f, i, j)
    }

    /// The cell's corners and center as points.
    #[must_use]
    pub fn vertices_and_center(self) -> ([Point; 4], Point) {
        let (f, i, j) = self.face_ij();
        let size = 1u32 << (MAX_LEVEL - self.level());
        let (ilo, jlo) = (i & size.wrapping_neg(), j & size.wrapping_neg());
        let scale = f64::from(1u32 << MAX_LEVEL);
        let s0 = f64::from(ilo) / scale;
        let s1 = (f64::from(ilo) + f64::from(size)) / scale;
        let t0 = f64::from(jlo) / scale;
        let t1 = (f64::from(jlo) + f64::from(size)) / scale;
        let (u0, u1, v0, v1) = (st_to_uv(s0), st_to_uv(s1), st_to_uv(t0), st_to_uv(t1));
        let c = uv_point(
            f,
            st_to_uv(f64::midpoint(s0, s1)),
            st_to_uv(f64::midpoint(t0, t1)),
        );
        (
            [
                uv_point(f, u0, v0),
                uv_point(f, u1, v0),
                uv_point(f, u1, v1),
                uv_point(f, u0, v1),
            ],
            c,
        )
    }

    /// A cap containing the cell: its center and the angle to its
    /// farthest corner (cells are convex, so the corners bound them).
    #[must_use]
    pub fn cap(self) -> Cap {
        let (vs, c) = self.vertices_and_center();
        let r = vs.iter().map(|v| angle(c, *v)).fold(0.0, f64::max);
        Cap {
            center: c,
            radius: r * (1.0 + 1e-9) + 1e-12,
        }
    }
}

/// The leaf cell id of a location in degrees.
#[must_use]
pub fn leaf(lat: f64, lng: f64) -> u64 {
    let (f, u, v) = face_uv(point(lat, lng));
    CellId::from_face_ij(f, st_to_ij(uv_to_st(u)), st_to_ij(uv_to_st(v))).0
}

/// A spherical cap: points within `radius` radians of `center`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cap {
    /// The center.
    pub center: Point,
    /// The angle, radians; π or more is the whole sphere.
    pub radius: f64,
}

impl Cap {
    /// The cap of points within `radius` radians of a location.
    #[must_use]
    pub fn around(lat: f64, lng: f64, radius: f64) -> Self {
        Self {
            center: point(lat, lng),
            radius,
        }
    }

    /// The smallest cap found around `points` (their normalized mean as
    /// center); the whole sphere when they spread past a hemisphere.
    #[must_use]
    pub fn bounding(points: &[Point]) -> Self {
        let mut s = [0.0; 3];
        for p in points {
            s[0] += p[0];
            s[1] += p[1];
            s[2] += p[2];
        }
        let n = (s[0] * s[0] + s[1] * s[1] + s[2] * s[2]).sqrt();
        if points.is_empty() || n < 1e-9 {
            return Self::whole();
        }
        let c = [s[0] / n, s[1] / n, s[2] / n];
        let r = points.iter().map(|p| angle(c, *p)).fold(0.0, f64::max);
        if r >= std::f64::consts::FRAC_PI_2 {
            return Self::whole();
        }
        Self {
            center: c,
            radius: r * (1.0 + 1e-9) + 1e-12,
        }
    }

    /// Every point.
    #[must_use]
    pub const fn whole() -> Self {
        Self {
            center: [0.0, 0.0, 1.0],
            radius: std::f64::consts::PI,
        }
    }

    /// Whether the two caps share a point.
    #[must_use]
    pub fn intersects(&self, o: &Self) -> bool {
        self.radius >= std::f64::consts::PI
            || o.radius >= std::f64::consts::PI
            || angle(self.center, o.center) <= self.radius + o.radius
    }

    /// Whether a point is inside.
    #[must_use]
    pub fn contains(&self, p: Point) -> bool {
        self.radius >= std::f64::consts::PI || angle(self.center, p) <= self.radius
    }
}

/// Cells covering `region`: level by level from the faces, every cell
/// whose cap meets the region's is kept, stopping at `max_level`, or
/// before the level whose cells would pass `max_cells` (but not above
/// `min_level`). Returned as merged leaf-id ranges.
#[must_use]
pub fn cover(region: &Cap, min_level: u8, max_level: u8, max_cells: usize) -> Vec<(u64, u64)> {
    let mut cells: Vec<CellId> = (0..6)
        .map(CellId::from_face)
        .filter(|c| c.cap().intersects(region))
        .collect();
    let mut level = 0;
    while level < max_level.min(MAX_LEVEL) {
        let next: Vec<CellId> = cells
            .iter()
            .flat_map(|c| c.children())
            .filter(|c| c.cap().intersects(region))
            .collect();
        if level >= min_level && next.len() > max_cells.max(1) {
            break;
        }
        cells = next;
        level += 1;
        if cells.is_empty() {
            break;
        }
    }
    let mut ranges: Vec<(u64, u64)> = cells.iter().map(|c| c.range()).collect();
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (a, b) in ranges {
        match out.last_mut() {
            Some(last) if a <= last.1.saturating_add(1) => last.1 = last.1.max(b),
            _ => out.push((a, b)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_cells() {
        // The S2 library's documented values: the point (lat 0, lng 0) is
        // on face 0, and face cells are 0x1000.., 0x3000.., ... 0xb000..
        assert_eq!(CellId::from_face(0).0, 0x1000_0000_0000_0000);
        assert_eq!(CellId::from_face(5).0, 0xb000_0000_0000_0000);
        assert_eq!(CellId(leaf(0.0, 0.0)).face(), 0);
        assert_eq!(CellId(leaf(0.0, 90.0)).face(), 1);
        assert_eq!(CellId(leaf(90.0, 0.0)).face(), 2);
        assert_eq!(CellId(leaf(0.0, 180.0)).face(), 3);
        assert_eq!(CellId(leaf(0.0, -90.0)).face(), 4);
        assert_eq!(CellId(leaf(-90.0, 0.0)).face(), 5);
        // Manhattan's cells have the token prefix 89c2 (face 4).
        let nyc = CellId(leaf(40.7128, -74.0060));
        assert_eq!(nyc.face(), 4);
        assert_eq!(nyc.0 >> 48, 0x89c2, "{:x}", nyc.0);
        assert_eq!(nyc.level(), 30);
    }

    #[test]
    fn leaf_round_trips_through_ij() {
        for (lat, lng) in [
            (19.07, 72.87),
            (-33.86, 151.21),
            (51.5, -0.12),
            (0.0, 179.999),
            (89.9, 10.0),
        ] {
            let c = CellId(leaf(lat, lng));
            let (f, i, j) = c.face_ij();
            assert_eq!(CellId::from_face_ij(f, i, j), c);
            let (_, center) = c.vertices_and_center();
            // A leaf is about a centimetre: its center is within 1e-8 rad.
            assert!(angle(center, point(lat, lng)) < 1e-8);
            for lvl in [0, 4, 12, 20, 29] {
                let p = c.parent(lvl);
                assert_eq!(p.level(), lvl);
                let (a, b) = p.range();
                assert!(a <= c.0 && c.0 <= b);
                assert!(p.cap().contains(point(lat, lng)));
                assert!(p.children().iter().any(|ch| {
                    let (x, y) = ch.range();
                    x <= c.0 && c.0 <= y
                }));
            }
        }
    }

    #[test]
    fn coverings_contain_their_region() {
        // Every point sampled inside a cap lies in one of its ranges.
        let mut seed = 7u64;
        let mut rnd = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 11) as f64 / (1u64 << 53) as f64
        };
        for _ in 0..40 {
            let lat = rnd() * 170.0 - 85.0;
            let lng = rnd() * 360.0 - 180.0;
            let r = rnd() * 0.05;
            let cap = Cap::around(lat, lng, r);
            let ranges = cover(&cap, 4, 20, 20);
            assert!(!ranges.is_empty());
            for _ in 0..200 {
                // A point within the cap: offset the center a little.
                let p = point(
                    lat + (rnd() - 0.5) * r.to_degrees(),
                    lng + (rnd() - 0.5) * r.to_degrees(),
                );
                if !cap.contains(p) {
                    continue;
                }
                let (pl, pg) = lat_lng(p);
                let id = leaf(pl, pg);
                assert!(
                    ranges.iter().any(|(a, b)| *a <= id && id <= *b),
                    "{lat} {lng} {r}"
                );
            }
        }
    }
}
