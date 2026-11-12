//! MongoDB's geospatial query operators on documents (chapter 36,
//! "MongoDB operators"; R6 round 3): `$geoWithin`, `$geoIntersects`,
//! `$near`, `$nearSphere` and the `$geoNear` stage's geometry, on the
//! sphere MongoDB uses (radius 6,378,100 m, great-circle edges).
//!
//! - Stored locations are GeoJSON (`Point`, `MultiPoint`, `LineString`,
//!   `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`)
//!   or legacy coordinate pairs (`[lng, lat]`, or an embedded document of
//!   two numbers), alone or in an array.
//! - The exact test runs in the gateway on every candidate (the recheck);
//!   a `2dsphere` index on the path gives MariaDB a latitude and longitude
//!   box to prefilter points with ([`prefilter`]).
//! - Legacy flat shapes (`$box`, `$center`, `$polygon`) test points on the
//!   plane, as MongoDB does without a `2d` index; `$near` with a legacy
//!   pair needs a `2d` index, which isn't offered (39662).

use bson::{Bson, Document};
use sprig_geo::region::Region;
use sprig_geo::s2::{self, Cap, Point};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::meta::{CollMeta, slug};
use crate::sql::{f64_lit, ident, json_path, str_lit};

/// MongoDB's earth radius for metre distances.
pub const EARTH_RADIUS_M: f64 = 6_378_100.0;

/// `(lat, lng)` in degrees.
pub type LatLng = (f64, f64);

/// A GeoJSON geometry (or a legacy point), vertices as `(lat, lng)`.
#[derive(Debug, Clone, PartialEq)]
pub enum Geom {
    /// A point.
    Point(LatLng),
    /// Points.
    MultiPoint(Vec<LatLng>),
    /// A line.
    Line(Vec<LatLng>),
    /// Lines.
    MultiLine(Vec<Vec<LatLng>>),
    /// Rings: the outer one, then holes (closed: the last vertex repeats
    /// the first).
    Polygon(Vec<Vec<LatLng>>),
    /// Polygons.
    MultiPolygon(Vec<Vec<Vec<LatLng>>>),
    /// Geometries.
    Collection(Vec<Geom>),
}

/// `$geoWithin`'s shape.
#[derive(Debug, Clone, PartialEq)]
pub enum Within {
    /// `$geometry`: a `Polygon` or `MultiPolygon` on the sphere.
    Geometry(Geom),
    /// `$centerSphere: [[lng, lat], radians]`.
    CenterSphere(LatLng, f64),
    /// `$box: [[x1, y1], [x2, y2]]`, flat.
    Box((f64, f64), (f64, f64)),
    /// `$center: [[x, y], r]`, flat.
    Center((f64, f64), f64),
    /// `$polygon: [[x, y], ...]`, flat.
    Polygon(Vec<(f64, f64)>),
}

/// A latitude range and longitude ranges, degrees.
pub type Boxes = ((f64, f64), Vec<(f64, f64)>);

/// `$near` and `$nearSphere` (and the `$geoNear` stage).
#[derive(Debug, Clone, PartialEq)]
pub struct Near {
    /// The center.
    pub at: LatLng,
    /// `$minDistance`.
    pub min: Option<f64>,
    /// `$maxDistance`.
    pub max: Option<f64>,
    /// The center was GeoJSON: distances in metres; else radians.
    pub metres: bool,
}

impl Near {
    /// The distance of `p` in this query's unit.
    #[must_use]
    pub fn distance(&self, p: LatLng) -> f64 {
        let a = s2::angle(s2::point(self.at.0, self.at.1), s2::point(p.0, p.1));
        if self.metres { a * EARTH_RADIUS_M } else { a }
    }

    /// Whether distance `d` is within the bounds.
    #[must_use]
    pub fn in_bounds(&self, d: f64) -> bool {
        self.min.is_none_or(|m| d >= m) && self.max.is_none_or(|m| d <= m)
    }

    /// The distance of geometry `g` (its nearest point; 0 inside a
    /// polygon).
    #[must_use]
    pub fn distance_to(&self, g: &Geom) -> f64 {
        let a = angle_to(self.at, g);
        if self.metres { a * EARTH_RADIUS_M } else { a }
    }

    /// The nearest distance of a stored value, when it holds a location.
    #[must_use]
    pub fn nearest(&self, v: &Bson) -> Option<f64> {
        stored_all(v)
            .iter()
            .map(|g| self.distance_to(g))
            .fold(None, |b: Option<f64>, d| Some(b.map_or(d, |b| b.min(d))))
    }
}

/// A geospatial condition on a path.
#[derive(Debug, Clone, PartialEq)]
pub enum GeoOp {
    /// `$geoWithin`.
    Within(Within),
    /// `$geoIntersects`.
    Intersects(Geom),
    /// `$near` / `$nearSphere`.
    Near(Near),
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

fn num(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(f) => Some(*f),
        Bson::Int32(i) => Some(f64::from(*i)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => Some(*i as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// A `[lng, lat]` position.
fn position(v: &Bson) -> Result<LatLng, MongoError> {
    let Bson::Array(a) = v else {
        return Err(bad("Point must be an array or object"));
    };
    if a.len() < 2 {
        return Err(bad("Point must only contain numeric elements"));
    }
    let (Some(lng), Some(lat)) = (num(&a[0]), num(&a[1])) else {
        return Err(bad("Point must only contain numeric elements"));
    };
    if !(-180.0..=180.0).contains(&lng) || !(-90.0..=90.0).contains(&lat) {
        return Err(bad(format!(
            "longitude/latitude is out of bounds, lng: {lng} lat: {lat}"
        )));
    }
    Ok((lat, lng))
}

fn positions(v: &Bson) -> Result<Vec<LatLng>, MongoError> {
    let Bson::Array(a) = v else {
        return Err(bad("coordinates must be an array"));
    };
    a.iter().map(position).collect()
}

fn ring(v: &Bson) -> Result<Vec<LatLng>, MongoError> {
    let r = positions(v)?;
    if r.len() < 4 {
        return Err(bad("Loop must have at least 3 different vertices"));
    }
    if r.first() != r.last() {
        return Err(bad("Loop is not closed"));
    }
    Ok(r)
}

fn rings(v: &Bson) -> Result<Vec<Vec<LatLng>>, MongoError> {
    let Bson::Array(a) = v else {
        return Err(bad("Polygon coordinates must be an array"));
    };
    if a.is_empty() {
        return Err(bad("Polygon has no loops."));
    }
    a.iter().map(ring).collect()
}

/// Parses a GeoJSON object.
///
/// # Errors
/// 2 `BadValue` for a malformed geometry.
pub fn geojson(d: &Document) -> Result<Geom, MongoError> {
    let t = d.get_str("type").map_err(|_| bad("unknown GeoJSON type"))?;
    if t == "GeometryCollection" {
        let Ok(gs) = d.get_array("geometries") else {
            return Err(bad("GeometryCollection needs geometries"));
        };
        return gs
            .iter()
            .map(|g| match g {
                Bson::Document(x) => geojson(x),
                _ => Err(bad("geometries must be objects")),
            })
            .collect::<Result<_, _>>()
            .map(Geom::Collection);
    }
    let c = d
        .get("coordinates")
        .ok_or_else(|| bad("GeoJSON coordinates must be an array"))?;
    Ok(match t {
        "Point" => Geom::Point(position(c)?),
        "MultiPoint" => Geom::MultiPoint(positions(c)?),
        "LineString" => {
            let l = positions(c)?;
            if l.len() < 2 {
                return Err(bad("GeoJSON LineString must have at least 2 vertices"));
            }
            Geom::Line(l)
        }
        "MultiLineString" => {
            let Bson::Array(a) = c else {
                return Err(bad("MultiLineString coordinates must be an array"));
            };
            Geom::MultiLine(a.iter().map(positions).collect::<Result<_, _>>()?)
        }
        "Polygon" => Geom::Polygon(rings(c)?),
        "MultiPolygon" => {
            let Bson::Array(a) = c else {
                return Err(bad("MultiPolygon coordinates must be an array"));
            };
            Geom::MultiPolygon(a.iter().map(rings).collect::<Result<_, _>>()?)
        }
        other => return Err(bad(format!("unknown GeoJSON type: {other}"))),
    })
}

/// A legacy point: `[x, y]` or `{a: x, b: y}` (numbers).
fn legacy(v: &Bson) -> Option<LatLng> {
    let (x, y) = match v {
        Bson::Array(a) if a.len() == 2 => (num(&a[0])?, num(&a[1])?),
        Bson::Document(d) if d.len() == 2 && !d.contains_key("type") => {
            let mut it = d.values();
            (num(it.next()?)?, num(it.next()?)?)
        }
        _ => return None,
    };
    ((-180.0..=180.0).contains(&x) && (-90.0..=90.0).contains(&y)).then_some((y, x))
}

/// A stored location (GeoJSON or a legacy pair).
#[must_use]
pub fn stored(v: &Bson) -> Option<Geom> {
    match v {
        Bson::Document(d) if d.contains_key("type") => geojson(d).ok(),
        other => legacy(other).map(Geom::Point),
    }
}

/// Every location in a stored value: itself, or each element of an array
/// of locations.
#[must_use]
pub fn stored_all(v: &Bson) -> Vec<Geom> {
    if let Some(g) = stored(v) {
        return vec![g];
    }
    match v {
        Bson::Array(a) => a.iter().filter_map(stored).collect(),
        _ => Vec::new(),
    }
}

/// The vertices of `g`.
#[must_use]
pub fn points_of(g: &Geom) -> Vec<LatLng> {
    match g {
        Geom::Point(p) => vec![*p],
        Geom::MultiPoint(v) | Geom::Line(v) => v.clone(),
        Geom::MultiLine(ls) | Geom::Polygon(ls) => ls.iter().flatten().copied().collect(),
        Geom::MultiPolygon(ps) => ps.iter().flatten().flatten().copied().collect(),
        Geom::Collection(gs) => gs.iter().flat_map(points_of).collect(),
    }
}

/// The edges of `g` (lines and rings).
fn edges_of(g: &Geom) -> Vec<(LatLng, LatLng)> {
    let pairs = |v: &Vec<LatLng>| v.windows(2).map(|w| (w[0], w[1])).collect::<Vec<_>>();
    match g {
        Geom::Point(_) | Geom::MultiPoint(_) => Vec::new(),
        Geom::Line(v) => pairs(v),
        Geom::MultiLine(ls) | Geom::Polygon(ls) => ls.iter().flat_map(pairs).collect(),
        Geom::MultiPolygon(ps) => ps.iter().flatten().flat_map(pairs).collect(),
        Geom::Collection(gs) => gs.iter().flat_map(edges_of).collect(),
    }
}

/// The polygons of `g`.
fn polygons_of(g: &Geom) -> Vec<Vec<Vec<LatLng>>> {
    match g {
        Geom::Polygon(r) => vec![r.clone()],
        Geom::MultiPolygon(ps) => ps.clone(),
        Geom::Collection(gs) => gs.iter().flat_map(polygons_of).collect(),
        _ => Vec::new(),
    }
}

/// Whether `p` lies in polygon `rings` (in the outer ring, in no hole;
/// the boundary counts as inside).
fn in_polygon(rings: &[Vec<LatLng>], p: LatLng) -> bool {
    let Some(outer) = rings.first() else {
        return false;
    };
    if !Region::Polygon(outer.clone()).contains(p.0, p.1) {
        return false;
    }
    !rings[1..]
        .iter()
        .any(|h| Region::Polygon(h.clone()).contains(p.0, p.1) && !on_ring(h, p))
}

fn on_ring(r: &[LatLng], p: LatLng) -> bool {
    r.windows(2).any(|w| on_edge(w[0], w[1], p))
}

fn v3(p: LatLng) -> Point {
    s2::point(p.0, p.1)
}

fn cross(a: Point, b: Point) -> Point {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn dot(a: Point, b: Point) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn neg(a: Point) -> Point {
    [-a[0], -a[1], -a[2]]
}

fn unit(a: Point) -> Option<Point> {
    let n = dot(a, a).sqrt();
    (n > 1e-300).then(|| [a[0] / n, a[1] / n, a[2] / n])
}

/// The angle from `p` to the nearest point of arc `a`–`b`.
fn angle_to_arc(p: Point, a: Point, b: Point) -> f64 {
    let ends = s2::angle(p, a).min(s2::angle(p, b));
    let Some(n) = unit(cross(a, b)) else {
        return ends;
    };
    // The foot of the perpendicular from `p` onto the great circle.
    let d = dot(p, n);
    let Some(f) = unit([p[0] - d * n[0], p[1] - d * n[1], p[2] - d * n[2]]) else {
        return ends;
    };
    if on_arc(a, b, f) {
        d.abs().clamp(0.0, 1.0).asin().min(ends)
    } else {
        ends
    }
}

/// The angle from `p` to geometry `g`: its nearest vertex or edge, 0
/// inside a polygon.
fn angle_to(p: LatLng, g: &Geom) -> f64 {
    if polygons_of(g).iter().any(|r| in_polygon(r, p)) {
        return 0.0;
    }
    let pv = v3(p);
    let mut best = f64::INFINITY;
    for q in points_of(g) {
        best = best.min(s2::angle(pv, v3(q)));
    }
    for (a, b) in edges_of(g) {
        best = best.min(angle_to_arc(pv, v3(a), v3(b)));
    }
    best
}

/// Whether `p` lies on the great-circle arc `a`–`b`.
fn on_arc(a: Point, b: Point, p: Point) -> bool {
    (s2::angle(a, p) + s2::angle(p, b) - s2::angle(a, b)).abs() < 1e-12
}

fn on_edge(a: LatLng, b: LatLng, p: LatLng) -> bool {
    on_arc(v3(a), v3(b), v3(p))
}

/// Whether arcs `a`–`b` and `c`–`d` share a point.
fn arcs_cross(a: LatLng, b: LatLng, c: LatLng, d: LatLng) -> bool {
    let (a, b, c, d) = (v3(a), v3(b), v3(c), v3(d));
    let Some(t) = unit(cross(cross(a, b), cross(c, d))) else {
        // Same great circle: overlapping when an end lies on the other.
        return on_arc(a, b, c) || on_arc(a, b, d) || on_arc(c, d, a) || on_arc(c, d, b);
    };
    [t, neg(t)]
        .iter()
        .any(|x| on_arc(a, b, *x) && on_arc(c, d, *x))
}

/// Whether `g` lies within query polygons `polys` (every vertex inside,
/// no edge leaving through a boundary).
fn within_polygons(g: &Geom, polys: &[Vec<Vec<LatLng>>]) -> bool {
    let pts = points_of(g);
    if pts.is_empty() {
        return false;
    }
    polys.iter().any(|poly| {
        pts.iter().all(|p| in_polygon(poly, *p))
            && edges_of(g).iter().all(|(a, b)| {
                // An edge may touch the boundary at a vertex but not cross it.
                !poly.iter().flat_map(|r| r.windows(2)).any(|w| {
                    arcs_cross(*a, *b, w[0], w[1])
                        && !on_edge(w[0], w[1], *a)
                        && !on_edge(w[0], w[1], *b)
                })
            })
    })
}

/// Whether geometries `x` and `y` share a point.
#[must_use]
pub fn intersects(x: &Geom, y: &Geom) -> bool {
    let (px, py) = (points_of(x), points_of(y));
    let (ex, ey) = (edges_of(x), edges_of(y));
    let (gx, gy) = (polygons_of(x), polygons_of(y));
    if px.iter().any(|p| py.contains(p)) {
        return true;
    }
    if px.iter().any(|p| {
        ey.iter().any(|(a, b)| on_edge(*a, *b, *p)) || gy.iter().any(|g| in_polygon(g, *p))
    }) {
        return true;
    }
    if py.iter().any(|p| {
        ex.iter().any(|(a, b)| on_edge(*a, *b, *p)) || gx.iter().any(|g| in_polygon(g, *p))
    }) {
        return true;
    }
    ex.iter()
        .any(|(a, b)| ey.iter().any(|(c, d)| arcs_cross(*a, *b, *c, *d)))
}

fn flat_point(g: &Geom) -> Option<(f64, f64)> {
    match g {
        Geom::Point((lat, lng)) => Some((*lng, *lat)),
        _ => None,
    }
}

impl Within {
    /// Whether stored `g` lies within.
    #[must_use]
    pub fn holds(&self, g: &Geom) -> bool {
        match self {
            Self::Geometry(q) => within_polygons(g, &polygons_of(q)),
            Self::CenterSphere(c, r) => {
                let c = v3(*c);
                let pts = points_of(g);
                !pts.is_empty() && pts.iter().all(|p| s2::angle(c, v3(*p)) <= *r)
            }
            Self::Box(lo, hi) => flat_point(g).is_some_and(|(x, y)| {
                x >= lo.0.min(hi.0)
                    && x <= lo.0.max(hi.0)
                    && y >= lo.1.min(hi.1)
                    && y <= lo.1.max(hi.1)
            }),
            Self::Center(c, r) => {
                flat_point(g).is_some_and(|(x, y)| (x - c.0).hypot(y - c.1) <= *r)
            }
            Self::Polygon(v) => flat_point(g).is_some_and(|p| flat_in_polygon(v, p)),
        }
    }
}

/// Planar point in polygon (boundary inside), for `$polygon`.
fn flat_in_polygon(v: &[(f64, f64)], p: (f64, f64)) -> bool {
    let n = v.len();
    let mut inside = false;
    for i in 0..n {
        let (a, b) = (v[i], v[(i + 1) % n]);
        // On an edge.
        let cr = (b.0 - a.0) * (p.1 - a.1) - (b.1 - a.1) * (p.0 - a.0);
        if cr.abs() < 1e-12
            && p.0 >= a.0.min(b.0)
            && p.0 <= a.0.max(b.0)
            && p.1 >= a.1.min(b.1)
            && p.1 <= a.1.max(b.1)
        {
            return true;
        }
        if (a.1 > p.1) != (b.1 > p.1) && p.0 < (b.0 - a.0) * (p.1 - a.1) / (b.1 - a.1) + a.0 {
            inside = !inside;
        }
    }
    inside
}

impl GeoOp {
    /// Whether the values at the path satisfy the condition (`leaves`:
    /// the path's values, arrays not expanded).
    #[must_use]
    pub fn eval(&self, leaves: &[&Bson]) -> bool {
        leaves.iter().any(|v| {
            stored_all(v).iter().any(|g| match self {
                Self::Within(w) => w.holds(g),
                Self::Intersects(q) => intersects(g, q),
                Self::Near(n) => n.in_bounds(n.distance_to(g)),
            })
        })
    }

    /// The bounding cap of the query region, when it has one.
    fn cap(&self) -> Option<Cap> {
        let bounding = |pts: &[LatLng]| -> Cap {
            let v: Vec<Point> = pts.iter().map(|p| v3(*p)).collect();
            let mut c = Cap::bounding(&v);
            c.radius = c.radius * (1.0 + 1e-9) + 1e-9;
            c
        };
        match self {
            Self::Within(Within::Geometry(g)) | Self::Intersects(g) => {
                let pts = points_of(g);
                (!pts.is_empty()).then(|| bounding(&pts))
            }
            Self::Within(Within::CenterSphere(c, r)) => {
                Some(Cap::around(c.0, c.1, r * (1.0 + 1e-9) + 1e-9))
            }
            Self::Near(n) => n.max.map(|m| {
                let r = if n.metres { m / EARTH_RADIUS_M } else { m };
                Cap::around(n.at.0, n.at.1, r * (1.0 + 1e-9) + 1e-9)
            }),
            Self::Within(_) => None,
        }
    }

    /// A latitude range and longitude ranges holding every point of the
    /// region; `None` when the region is too large to help.
    #[must_use]
    pub fn boxes(&self) -> Option<Boxes> {
        if let Self::Within(Within::Box(lo, hi)) = self {
            return Some((
                (lo.1.min(hi.1), lo.1.max(hi.1)),
                vec![(lo.0.min(hi.0), lo.0.max(hi.0))],
            ));
        }
        if let Self::Within(Within::Center(c, r)) = self {
            return Some(((c.1 - r, c.1 + r), vec![(c.0 - r, c.0 + r)]));
        }
        if let Self::Within(Within::Polygon(v)) = self {
            let (mut x0, mut x1, mut y0, mut y1) = (f64::MAX, f64::MIN, f64::MAX, f64::MIN);
            for (x, y) in v {
                x0 = x0.min(*x);
                x1 = x1.max(*x);
                y0 = y0.min(*y);
                y1 = y1.max(*y);
            }
            return Some(((y0, y1), vec![(x0, x1)]));
        }
        let cap = self.cap()?;
        if cap.radius >= std::f64::consts::FRAC_PI_2 {
            return None;
        }
        let (lat, lng) = s2::lat_lng(cap.center);
        let r = cap.radius.to_degrees();
        let (lo, hi) = (lat - r, lat + r);
        if lo <= -90.0 || hi >= 90.0 {
            return Some(((lo.max(-90.0), hi.min(90.0)), vec![(-180.0, 180.0)]));
        }
        let d = (cap.radius.sin() / lat.to_radians().cos())
            .clamp(-1.0, 1.0)
            .asin()
            .to_degrees()
            + 1e-9;
        let (w, e) = (lng - d, lng + d);
        let lngs = if w < -180.0 {
            vec![(w + 360.0, 180.0), (-180.0, e)]
        } else if e > 180.0 {
            vec![(w, 180.0), (-180.0, e - 360.0)]
        } else {
            vec![(w, e)]
        };
        Some(((lo, hi), lngs))
    }
}

fn coords_pair(v: &Bson) -> Result<(f64, f64), MongoError> {
    match v {
        Bson::Array(a) if a.len() >= 2 => match (num(&a[0]), num(&a[1])) {
            (Some(x), Some(y)) => Ok((x, y)),
            _ => Err(bad("Point must only contain numeric elements")),
        },
        Bson::Document(d) if d.len() >= 2 => {
            let mut it = d.values();
            match (it.next().and_then(num), it.next().and_then(num)) {
                (Some(x), Some(y)) => Ok((x, y)),
                _ => Err(bad("Point must only contain numeric elements")),
            }
        }
        _ => Err(bad("Point must be an array or object")),
    }
}

/// The `$geometry` (or a GeoJSON point) of an operator argument.
fn geometry_arg(d: &Document) -> Result<Geom, MongoError> {
    match d.get("$geometry") {
        Some(Bson::Document(g)) => geojson(g),
        Some(_) => Err(bad("$geometry must be an object")),
        None => Err(bad("$geometry is required")),
    }
}

fn unsupported(what: &str) -> MongoError {
    MongoError::new(
        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
        format!("{what} is not supported in this release"),
    )
}

/// Parses `$geoWithin`, `$geoIntersects`, `$near` or `$nearSphere`
/// (`op` with its `$`) and, for the near operators, the sibling
/// `$minDistance` and `$maxDistance` of `siblings`.
///
/// # Errors
/// 2 `BadValue` for malformed arguments; 39662 for the forms not offered
/// (big polygons with a custom CRS, `$near` with a legacy pair).
pub fn parse(op: &str, v: &Bson, siblings: &Document) -> Result<GeoOp, MongoError> {
    match op {
        "$geoWithin" | "$within" => {
            let Bson::Document(d) = v else {
                return Err(bad("$geoWithin must be an object"));
            };
            let Some((k, a)) = d.iter().next() else {
                return Err(bad("$geoWithin needs a shape"));
            };
            Ok(GeoOp::Within(match k.as_str() {
                "$geometry" => {
                    if let Bson::Document(g) = a
                        && g.contains_key("crs")
                    {
                        return Err(unsupported("a $geometry with a custom crs"));
                    }
                    let g = geometry_arg(d)?;
                    if polygons_of(&g).is_empty() {
                        return Err(bad(
                            "$geoWithin $geometry must be a Polygon or MultiPolygon",
                        ));
                    }
                    Within::Geometry(g)
                }
                "$centerSphere" => {
                    let Bson::Array(x) = a else {
                        return Err(bad("$centerSphere must be an array"));
                    };
                    let (lng, lat) = coords_pair(x.first().unwrap_or(&Bson::Null))?;
                    let r = x
                        .get(1)
                        .and_then(num)
                        .filter(|r| *r >= 0.0)
                        .ok_or_else(|| bad("radius must be a non-negative number"))?;
                    Within::CenterSphere((lat, lng), r)
                }
                "$box" => {
                    let Bson::Array(x) = a else {
                        return Err(bad("$box must be an array"));
                    };
                    if x.len() != 2 {
                        return Err(bad("$box needs two points"));
                    }
                    Within::Box(coords_pair(&x[0])?, coords_pair(&x[1])?)
                }
                "$center" => {
                    let Bson::Array(x) = a else {
                        return Err(bad("$center must be an array"));
                    };
                    let c = coords_pair(x.first().unwrap_or(&Bson::Null))?;
                    let r = x
                        .get(1)
                        .and_then(num)
                        .filter(|r| *r >= 0.0)
                        .ok_or_else(|| bad("radius must be a non-negative number"))?;
                    Within::Center(c, r)
                }
                "$polygon" => {
                    let Bson::Array(x) = a else {
                        return Err(bad("$polygon must be an array"));
                    };
                    let v: Vec<(f64, f64)> = x.iter().map(coords_pair).collect::<Result<_, _>>()?;
                    if v.len() < 3 {
                        return Err(bad("Polygon must have at least 3 points"));
                    }
                    Within::Polygon(v)
                }
                other => return Err(bad(format!("unknown geo specifier: {other}"))),
            }))
        }
        "$geoIntersects" => {
            let Bson::Document(d) = v else {
                return Err(bad("$geoIntersects must be an object"));
            };
            Ok(GeoOp::Intersects(geometry_arg(d)?))
        }
        "$near" | "$nearSphere" => {
            let (at, metres, mut max, mut min) = match v {
                Bson::Document(d) if d.contains_key("$geometry") => {
                    let Geom::Point(p) = geometry_arg(d)? else {
                        return Err(bad("$near requires a point, given a non-point"));
                    };
                    (
                        p,
                        true,
                        d.get("$maxDistance").and_then(num),
                        d.get("$minDistance").and_then(num),
                    )
                }
                other => {
                    // A legacy pair with `$near` needs a `2d` index, which
                    // SprigDB never has: MongoDB's answer without one.
                    if op == "$near" {
                        return Err(MongoError::new(
                            291,
                            "unable to find index for $geoNear query",
                        ));
                    }
                    let (x, y) = coords_pair(other)?;
                    ((y, x), false, None, None)
                }
            };
            if let Some(m) = siblings.get("$maxDistance").and_then(num) {
                max = Some(m);
            }
            if let Some(m) = siblings.get("$minDistance").and_then(num) {
                min = Some(m);
            }
            if max.is_some_and(|m| m < 0.0) || min.is_some_and(|m| m < 0.0) {
                return Err(bad("$maxDistance and $minDistance must be non-negative"));
            }
            Ok(GeoOp::Near(Near {
                at,
                min,
                max,
                metres,
            }))
        }
        other => Err(bad(format!("unknown geo operator: {other}"))),
    }
}

/// The columns a `2dsphere` index on `path` adds: `(latitude, longitude)`
/// of a stored point, `NULL` for anything else.
#[must_use]
pub fn columns(path: &str) -> (String, String) {
    let s = slug(path);
    (format!("_sprig_g_{s}_lat"), format!("_sprig_g_{s}_lng"))
}

/// `ALTER TABLE` adding a `2dsphere` path's columns (virtual, instant).
#[must_use]
pub fn add_columns_sql(db: &str, table: &str, path: &str) -> String {
    let parts: Vec<&str> = path.split('.').collect();
    let p = json_path(&parts, &[]);
    let at = |suffix: &str| str_lit(&format!("{p}{suffix}"));
    let (lat, lng) = columns(path);
    let coord = |i: u8| {
        format!(
            "CASE WHEN JSON_VALUE(doc, {t}) = 'Point' AND JSON_TYPE(JSON_EXTRACT(doc, {c})) IN ('INTEGER','DOUBLE') \
             THEN CAST(JSON_VALUE(doc, {c}) AS DOUBLE) \
             WHEN JSON_TYPE(JSON_EXTRACT(doc, {w})) = 'ARRAY' AND JSON_LENGTH(JSON_EXTRACT(doc, {w})) = 2 \
             AND JSON_TYPE(JSON_EXTRACT(doc, {l})) IN ('INTEGER','DOUBLE') THEN CAST(JSON_VALUE(doc, {l}) AS DOUBLE) END",
            t = at(".type"),
            c = at(&format!(".coordinates[{i}]")),
            w = str_lit(&p),
            l = at(&format!("[{i}]")),
        )
    };
    format!(
        "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} DOUBLE AS ({}) VIRTUAL INVISIBLE, \
         ADD COLUMN IF NOT EXISTS {} DOUBLE AS ({}) VIRTUAL INVISIBLE",
        ident(db),
        ident(table),
        ident(&lat),
        coord(1),
        ident(&lng),
        coord(0),
    )
}

/// `CREATE INDEX` of a `2dsphere` index on `path`.
#[must_use]
pub fn create_index_sql(db: &str, table: &str, index: &str, path: &str) -> String {
    let (lat, lng) = columns(path);
    format!(
        "CREATE INDEX {} ON {}.{} ({}, {})",
        ident(index),
        ident(db),
        ident(table),
        ident(&lat),
        ident(&lng)
    )
}

/// `ALTER TABLE` dropping a `2dsphere` path's columns.
#[must_use]
pub fn drop_columns_sql(db: &str, table: &str, path: &str) -> String {
    let (lat, lng) = columns(path);
    format!(
        "ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}, DROP COLUMN IF EXISTS {}",
        ident(db),
        ident(table),
        ident(&lat),
        ident(&lng)
    )
}

/// The `2dsphere` index on `path`, when the collection has one.
#[must_use]
pub fn index_on<'a>(m: &'a CollMeta, path: &str) -> Option<&'a crate::meta::IndexDef> {
    m.indexes
        .iter()
        .find(|i| is_2dsphere(i) && i.key.first().is_some_and(|(p, _)| p == path))
}

/// Whether `i` is a `2dsphere` index.
#[must_use]
pub fn is_2dsphere(i: &crate::meta::IndexDef) -> bool {
    i.extra.contains_key("2dsphereIndexVersion")
}

/// The side table of a `2dsphere` path's lines and polygons (R5 round 6,
/// R33's other half): one row per document whose value at the path isn't
/// a point, with a latitude and longitude box holding it on the sphere.
#[must_use]
pub fn side_table(table: &str, path: &str) -> String {
    let mut k = table.as_bytes().to_vec();
    k.push(0);
    k.extend_from_slice(path.as_bytes());
    format!("__sprig_doc_gx_{:016x}", crate::meta::fnv64(&k))
}

/// `CREATE TABLE` of a side table (`db.side`, quoted).
#[must_use]
pub fn side_table_sql(st: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {st} (_id VARBINARY(3000) NOT NULL, lat_lo DOUBLE NOT NULL, \
         lat_hi DOUBLE NOT NULL, lng_lo DOUBLE NOT NULL, lng_hi DOUBLE NOT NULL, \
         _v BIGINT UNSIGNED NOT NULL, PRIMARY KEY (_id), KEY by_lat (lat_lo, lat_hi)) ENGINE=InnoDB"
    )
}

/// A box `(lat_lo, lat_hi, lng_lo, lng_hi)` holding every location in a
/// stored value on the sphere (its vertices' bounding cap, which holds
/// the great-circle edges and polygon interiors between them); the whole
/// earth when the cap is a hemisphere or more; `None` when the value holds
/// no location.
#[must_use]
pub fn side_bounds(v: &Bson) -> Option<(f64, f64, f64, f64)> {
    let geoms = stored_all(v);
    let pts: Vec<LatLng> = geoms.iter().flat_map(points_of).collect();
    if pts.is_empty() {
        return None;
    }
    let world = (-90.0, 90.0, -180.0, 180.0);
    let v3s: Vec<Point> = pts.iter().map(|p| v3(*p)).collect();
    let mut cap = Cap::bounding(&v3s);
    if cap.radius >= std::f64::consts::FRAC_PI_2 {
        return Some(world);
    }
    cap.radius = cap.radius * (1.0 + 1e-9) + 1e-9;
    let Some(((lo, hi), lngs)) =
        GeoOp::Within(Within::CenterSphere(s2::lat_lng(cap.center), cap.radius)).boxes()
    else {
        return Some(world);
    };
    if lngs.len() != 1 {
        return Some((lo, hi, -180.0, 180.0));
    }
    Some((lo, hi, lngs[0].0, lngs[0].1))
}

/// A path's side table and the `_v` past which documents aren't in it
/// yet, as the engine passes them to [`prefilter`] in the collection's
/// options (a copy used for one translation only).
pub const SIDE_OPTION: &str = "__sprig_geo_side";

/// The SQL prefilter of a geo condition on `path`: with a `2dsphere`
/// index, points outside the region's box are skipped; lines and polygons
/// are skipped by their side table's boxes when the engine synced it
/// (documents written since stay candidates), else they stay candidates;
/// without an index, every row.
#[must_use]
pub fn prefilter(path: &str, op: &GeoOp, m: &CollMeta) -> String {
    if index_on(m, path).is_none() {
        return "TRUE".into();
    }
    let Some(((lo, hi), lngs)) = op.boxes() else {
        return "TRUE".into();
    };
    let side = m
        .options
        .get_document(SIDE_OPTION)
        .ok()
        .and_then(|d| d.get_document(path).ok())
        .and_then(|d| Some((d.get_str("t").ok()?.to_owned(), d.get_i64("v").ok()?)));
    let (lat, lng) = columns(path);
    if let Some((st, min_v)) = side {
        let points = lngs
            .iter()
            .map(|(w, e)| {
                format!(
                    "{} BETWEEN {} AND {}",
                    ident(&lng),
                    f64_lit(*w - 1e-9),
                    f64_lit(*e + 1e-9)
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let shapes = lngs
            .iter()
            .map(|(w, e)| {
                format!(
                    "(lng_lo <= {} AND lng_hi >= {})",
                    f64_lit(*e + 1e-9),
                    f64_lit(*w - 1e-9)
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        return format!(
            "(({lat} IS NOT NULL AND {lat} BETWEEN {lo} AND {hi} AND ({points})) OR ({lat} IS NULL AND \
             (_v > {min_v} OR LENGTH(_id) > 3000 OR _id IN (SELECT _id FROM {st} WHERE lat_hi >= {lo} \
             AND lat_lo <= {hi} AND ({shapes})))))",
            lat = ident(&lat),
            lo = f64_lit(lo - 1e-9),
            hi = f64_lit(hi + 1e-9),
        );
    }
    let lngs = lngs
        .iter()
        .map(|(w, e)| {
            format!(
                "{} BETWEEN {} AND {}",
                ident(&lng),
                f64_lit(*w - 1e-9),
                f64_lit(*e + 1e-9)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "({lat} IS NULL OR ({lat} BETWEEN {} AND {} AND ({lngs})))",
        f64_lit(lo - 1e-9),
        f64_lit(hi + 1e-9),
        lat = ident(&lat),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn square(c: (f64, f64), h: f64) -> Bson {
        let (x, y) = c;
        bson::bson!({"type": "Polygon", "coordinates": [[[x - h, y - h], [x + h, y - h], [x + h, y + h], [x - h, y + h], [x - h, y - h]]]})
    }

    #[test]
    fn within_and_intersects() {
        let poly = square((10.0, 20.0), 1.0);
        let op = parse(
            "$geoWithin",
            &Bson::Document(doc! {"$geometry": poly.clone()}),
            &doc! {},
        )
        .unwrap();
        let pt = |x: f64, y: f64| bson::bson!({"type": "Point", "coordinates": [x, y]});
        assert!(op.eval(&[&pt(10.2, 20.3)]));
        assert!(!op.eval(&[&pt(12.0, 20.0)]));
        assert!(op.eval(&[&bson::bson!([10.5, 19.5])]));
        let line = bson::bson!({"type": "LineString", "coordinates": [[9.5, 20.0], [12.0, 20.0]]});
        assert!(!op.eval(&[&line]));
        let i = parse(
            "$geoIntersects",
            &Bson::Document(doc! {"$geometry": poly}),
            &doc! {},
        )
        .unwrap();
        assert!(i.eval(&[&line]));
        assert!(!i.eval(&[&pt(30.0, 0.0)]));
        // An array of locations: any of them.
        assert!(op.eval(&[&bson::bson!([pt(0.0, 0.0), pt(10.0, 20.0)])]));
    }

    #[test]
    fn near_in_metres() {
        let op = parse(
            "$near",
            &Bson::Document(doc! {"$geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "$maxDistance": 120_000.0}),
            &doc! {},
        )
        .unwrap();
        let GeoOp::Near(n) = &op else { panic!() };
        let d = n.distance((0.0, 1.0));
        assert!((d - EARTH_RADIUS_M * 1f64.to_radians()).abs() < 1e-6, "{d}");
        assert!(op.eval(&[&bson::bson!([1.0, 0.0])]));
        assert!(!op.eval(&[&bson::bson!([1.2, 0.0])]));
        let ((lo, hi), lngs) = op.boxes().unwrap();
        assert!(lo < -1.07 && hi > 1.07 && lngs.len() == 1);
        assert!(parse("$near", &bson::bson!([1.0, 2.0]), &doc! {}).is_err());
    }

    #[test]
    fn boxes_wrap_the_antimeridian() {
        let op = GeoOp::Within(Within::CenterSphere((0.0, 179.5), 0.02));
        let (_, lngs) = op.boxes().unwrap();
        assert_eq!(lngs.len(), 2);
        assert!(op.eval(&[&bson::bson!([-179.8, 0.0])]));
    }

    #[test]
    fn side_bounds_hold_lines_and_polygons() {
        let line = bson::bson!({"type": "LineString", "coordinates": [[10.0, 20.0], [12.0, 21.0]]});
        let (lo, hi, w, e) = side_bounds(&line).unwrap();
        assert!(
            lo <= 20.0 && hi >= 21.0 && w <= 10.0 && e >= 12.0,
            "{lo} {hi} {w} {e}"
        );
        // A great-circle edge bulges toward the pole: the box holds it.
        let arc = bson::bson!({"type": "LineString", "coordinates": [[-60.0, 60.0], [60.0, 60.0]]});
        let (_, hi, _, _) = side_bounds(&arc).unwrap();
        assert!(hi >= 70.0, "{hi}");
        let poly = square((100.0, -5.0), 2.0);
        let (lo, hi, w, e) = side_bounds(&poly).unwrap();
        assert!(lo <= -7.0 && hi >= -3.0 && w <= 98.0 && e >= 102.0);
        assert_eq!(side_bounds(&Bson::String("x".into())), None);
        // Across the antimeridian: every longitude.
        let wrap =
            bson::bson!({"type": "LineString", "coordinates": [[179.0, 0.0], [-179.0, 0.0]]});
        let (_, _, w, e) = side_bounds(&wrap).unwrap();
        assert!((w, e) == (-180.0, 180.0), "{w} {e}");
    }

    #[test]
    fn prefilter_uses_the_side_table_once_synced() {
        let mut m = CollMeta {
            name: "c".into(),
            table: "c".into(),
            uuid: [0; 16],
            array_paths: std::collections::BTreeSet::new(),
            arrays_full: false,
            pending_paths: std::collections::BTreeSet::new(),
            indexes: vec![crate::meta::IndexDef {
                id: 1,
                name: "loc_2dsphere".into(),
                key: vec![("loc".into(), 1)],
                unique: false,
                sparse: false,
                expire_after: None,
                origin: "user".into(),
                extra: doc! {"2dsphereIndexVersion": 3},
                mariadb: None,
                auto_index_id: None,
            }],
            options: Document::new(),
            next_index: 2,
        };
        let op = parse(
            "$geoIntersects",
            &Bson::Document(doc! {"$geometry": square((10.0, 20.0), 1.0)}),
            &doc! {},
        )
        .unwrap();
        // Not synced: every non-point value is a candidate.
        let before = prefilter("loc", &op, &m);
        assert!(before.contains("IS NULL OR"), "{before}");
        assert!(!before.contains("__sprig_doc_gx_"), "{before}");
        m.options.insert(
            SIDE_OPTION,
            doc! {"loc": {"t": "`d`.`__sprig_doc_gx_00`", "v": 77_i64}},
        );
        let after = prefilter("loc", &op, &m);
        assert!(
            after.contains("FROM `d`.`__sprig_doc_gx_00` WHERE lat_hi >="),
            "{after}"
        );
        assert!(after.contains("_v > 77"), "{after}");
    }
}
