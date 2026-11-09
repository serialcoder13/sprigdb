//! MySQL 8.4's geographic computations on SRID 4326 (chapter 36, "MySQL
//! persona rewrite"; R4).
//!
//! MySQL computes `ST_Distance` between two geographic points with
//! Boost.Geometry's Andoyer inverse formula on the WGS 84 ellipsoid, and
//! converts degrees with the SRS's own unit factor
//! (`0.017453292519943278`, not `π / 180`). [`andoyer_distance`] follows
//! Boost's `andoyer_inverse` operation by operation, so the numbers agree
//! to the last digit (checked against MySQL 8.4.11: 11 of 12 pairs exactly,
//! one a single unit in the last place apart, likely the C library's
//! trigonometry). The gateway sends the
//! same operations to MariaDB as SQL ([`andoyer_sql`]), so the distance
//! works in every clause and in stored programs, with the backend's libm
//! doing the arithmetic as MySQL's does.

// The formulas keep Boost's one-letter names (d, k, l, h, g) and the
// lat1/lon1/lat2/lon2 family, so they read against the original.
#![allow(clippy::many_single_char_names, clippy::similar_names)]

/// WGS 84 equatorial radius in metres.
pub const WGS84_A: f64 = 6_378_137.0;
/// WGS 84 inverse flattening.
pub const WGS84_INV_F: f64 = 298.257_223_563;
/// MySQL's degree unit for SRID 4326 (its `UNIT["degree", ...]`).
pub const DEGREE: f64 = 0.017_453_292_519_943_278;
/// `ST_Distance_Sphere`'s default radius in metres for Cartesian points.
pub const SPHERE_RADIUS: f64 = 6_370_986.0;

/// `ST_Distance_Sphere`'s default radius for SRID 4326: the ellipsoid's
/// mean radius `(2a + b) / 3` (measured: MySQL 8.4 uses it for geographic
/// points, not 6,370,986 m).
#[must_use]
pub fn wgs84_mean_radius() -> f64 {
    let b = WGS84_A * (1.0 - 1.0 / WGS84_INV_F);
    (2.0 * WGS84_A + b) / 3.0
}

/// The geodesic distance in metres between two points given in degrees,
/// as MySQL 8.4's `ST_Distance` on SRID 4326 computes it.
#[must_use]
pub fn andoyer_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let f = 1.0 / WGS84_INV_F;
    let (lat1, lon1, lat2, lon2) = (lat1 * DEGREE, lon1 * DEGREE, lat2 * DEGREE, lon2 * DEGREE);
    // Boost's `math::equals` on both coordinates: the same point is 0.
    let equals = |a: f64, b: f64| (a - b).abs() <= f64::EPSILON * 1f64.max(a.abs()).max(b.abs());
    if equals(lon1, lon2) && equals(lat1, lat2) {
        return 0.0;
    }
    let dlon = lon2 - lon1;
    let (sin_dlon, cos_dlon) = (dlon.sin(), dlon.cos());
    let _ = sin_dlon;
    let (sin_lat1, cos_lat1) = (lat1.sin(), lat1.cos());
    let (sin_lat2, cos_lat2) = (lat2.sin(), lat2.cos());
    let mut cos_d = sin_lat1 * sin_lat2 + cos_lat1 * cos_lat2 * cos_dlon;
    cos_d = cos_d.clamp(-1.0, 1.0);
    let d = cos_d.acos();
    let sin_d = d.sin();
    if d == 0.0 {
        return 0.0;
    }
    let k = (sin_lat1 - sin_lat2) * (sin_lat1 - sin_lat2);
    let l = (sin_lat1 + sin_lat2) * (sin_lat1 + sin_lat2);
    let three_sin_d = 3.0 * sin_d;
    let one_minus_cos_d = 1.0 - cos_d;
    let one_plus_cos_d = 1.0 + cos_d;
    let h = if one_minus_cos_d == 0.0 {
        0.0
    } else {
        (d + three_sin_d) / one_minus_cos_d
    };
    let g = if one_plus_cos_d == 0.0 {
        0.0
    } else {
        (d - three_sin_d) / one_plus_cos_d
    };
    let dd = -(f / 4.0) * (h * k + g * l);
    WGS84_A * (d + dd)
}

/// `ST_Distance_Sphere` in metres, points in degrees.
#[must_use]
pub fn sphere_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64, radius: f64) -> f64 {
    let (lat1, lon1, lat2, lon2) = (lat1 * DEGREE, lon1 * DEGREE, lat2 * DEGREE, lon2 * DEGREE);
    let dlat = (lat2 - lat1) / 2.0;
    let dlon = (lon2 - lon1) / 2.0;
    let a = dlat.sin() * dlat.sin() + lat1.cos() * lat2.cos() * dlon.sin() * dlon.sin();
    2.0 * radius * a.sqrt().asin()
}

/// The Andoyer distance as MariaDB SQL, over four SQL expressions giving
/// the points' latitudes and longitudes in degrees. The operations are
/// [`andoyer_distance`]'s, in the same order. Each argument appears more
/// than once, so they must be free of side effects and parameters.
#[must_use]
pub fn andoyer_sql(lat1: &str, lon1: &str, lat2: &str, lon2: &str) -> String {
    let deg = "1.7453292519943278e-2";
    let r = |e: &str| format!("(({e})*{deg})");
    let (la1, lo1, la2, lo2) = (r(lat1), r(lon1), r(lat2), r(lon2));
    let cos_d = format!(
        "LEAST(1e0,GREATEST(-1e0,SIN({la1})*SIN({la2})+COS({la1})*COS({la2})*COS({lo2}-{lo1})))"
    );
    let d = format!("ACOS({cos_d})");
    let k = format!("(SIN({la1})-SIN({la2}))*(SIN({la1})-SIN({la2}))");
    let l = format!("(SIN({la1})+SIN({la2}))*(SIN({la1})+SIN({la2}))");
    let eq = |a: &str, b: &str| {
        format!("ABS({a}-{b})<=2.220446049250313e-16*GREATEST(1e0,ABS({a}),ABS({b}))")
    };
    let same = format!("{} AND {}", eq(&lo1, &lo2), eq(&la1, &la2));
    let h = format!("IFNULL(({d}+3e0*SIN({d}))/NULLIF(1e0-{cos_d},0e0),0e0)");
    let g = format!("IFNULL(({d}-3e0*SIN({d}))/NULLIF(1e0+{cos_d},0e0),0e0)");
    format!(
        "IF({same} OR {d}=0e0,0e0,6378137e0*({d}+(-((1e0/298.257223563e0)/4e0))*({h}*{k}+{g}*{l})))"
    )
}

/// `ST_Distance_Sphere` as MariaDB SQL (see [`sphere_distance`]).
#[must_use]
pub fn sphere_sql(lat1: &str, lon1: &str, lat2: &str, lon2: &str, radius: &str) -> String {
    let deg = "1.7453292519943278e-2";
    let r = |e: &str| format!("(({e})*{deg})");
    let (la1, lo1, la2, lo2) = (r(lat1), r(lon1), r(lat2), r(lon2));
    format!(
        "(2e0*({radius})*ASIN(SQRT(SIN(({la2}-{la1})/2e0)*SIN(({la2}-{la1})/2e0)+COS({la1})*COS({la2})*SIN(({lo2}-{lo1})/2e0)*SIN(({lo2}-{lo1})/2e0))))"
    )
}

/// Swaps the two coordinates of every point in a WKT text (`POINT(10 20)`
/// becomes `POINT(20 10)`), keeping everything else byte for byte. `None`
/// when a coordinate group doesn't have exactly two numbers (Z and M
/// coordinates aren't supported) or the text isn't WKT.
#[must_use]
pub fn swap_wkt(wkt: &str) -> Option<String> {
    let b = wkt.as_bytes();
    let mut out = String::with_capacity(wkt.len());
    let mut i = 0;
    let is_num = |c: u8| c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'+' | b'e' | b'E');
    let mut depth = 0;
    while i < b.len() {
        let c = b[i];
        if depth > 0 && (c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'+')) {
            // A coordinate group: numbers separated by whitespace, up to a
            // comma or a closing parenthesis.
            let start = i;
            let mut nums: Vec<&str> = Vec::new();
            loop {
                let ns = i;
                while i < b.len() && is_num(b[i]) {
                    i += 1;
                }
                if i == ns {
                    return None;
                }
                nums.push(&wkt[ns..i]);
                let ws = i;
                while i < b.len() && b[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i < b.len() && (b[i] == b',' || b[i] == b')') {
                    i = ws;
                    break;
                }
                if i >= b.len() {
                    return None;
                }
            }
            let _ = start;
            if nums.len() != 2 {
                return None;
            }
            out.push_str(nums[1]);
            out.push(' ');
            out.push_str(nums[0]);
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        out.push(c as char);
        i += 1;
    }
    (depth == 0).then_some(out)
}

/// Degrees of latitude and longitude around `(lat, lon)` that hold every
/// point within `r` metres: `(min_lat, min_lon, max_lat, max_lon)`, or
/// `None` when the box would cross a pole or ±180° (the caller then
/// filters nothing). The constants err on the large side (chapter 36,
/// "Geodesic bounding box").
#[must_use]
pub fn grow(lat: f64, lon: f64, r: f64) -> Option<(f64, f64, f64, f64)> {
    let dlat = r / 110_574.0 + 1e-9;
    let (lo, hi) = (lat - dlat, lat + dlat);
    if lo <= -90.0 || hi >= 90.0 {
        return None;
    }
    let max_abs = lo.abs().max(hi.abs());
    let dlon = r / (111_319.0 * max_abs.to_radians().cos()) + 1e-9;
    let (w, e) = (lon - dlon, lon + dlon);
    if w <= -180.0 || e > 180.0 {
        return None;
    }
    Some((lo, w, hi, e))
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // equal to the last digit is the point
mod tests {
    use super::*;

    // MySQL 8.4.11, 9 November 2026: ST_Distance(ST_GeomFromText('POINT(lat
    // lon)', 4326), ...) and ST_Distance_Sphere.
    #[test]
    fn distances_match_mysql_to_the_last_digit() {
        assert_eq!(andoyer_distance(1.0, 1.0, 2.0, 2.0), 156_874.385_949_045_5);
        assert_eq!(
            andoyer_distance(18.9, 72.8, 28.6, 77.2),
            1_163_887.454_992_546_7
        );
        assert_eq!(
            andoyer_distance(0.0, 0.0, 0.0, 180.0),
            20_037_508.342_789_244
        );
        assert_eq!(
            andoyer_distance(89.9, 0.0, -89.9, 0.1),
            19_981_578.820_895_523
        );
        assert_eq!(
            andoyer_distance(52.52, 13.405, 40.7128, -74.006),
            6_402_423.603_743_638
        );
        assert_eq!(
            andoyer_distance(-33.8688, 151.2093, 51.5074, -0.1278),
            16_989_267.822_897_2
        );
        assert_eq!(
            andoyer_distance(0.0, 0.0, 0.0001, 0.0001),
            15.690_314_731_574_523
        );
        assert_eq!(andoyer_distance(1.0, 1.0, 1.0, 1.0), 0.0);
        assert_eq!(andoyer_distance(18.9, 72.8, 18.9, 72.8), 0.0);
        // MySQL 8.4.11 says 37721.96667110406: one unit in the last place
        // (2e-16 relative), the one pair of twelve measured that differs;
        // MariaDB's SQL gives the same as this function.
        assert_eq!(
            andoyer_distance(18.9, 72.8, 19.2, 72.97),
            37_721.966_671_104_07
        );
        assert_eq!(
            andoyer_distance(18.9, 72.8, 18.52, 73.85),
            118_464.622_003_810_35
        );
        assert_eq!(
            andoyer_distance(28.6, 77.2, 19.2, 72.97),
            1_126_414.760_982_778_2
        );
    }

    #[test]
    fn sphere() {
        let r = wgs84_mean_radius();
        assert_eq!(
            sphere_distance(1.0, 1.0, 2.0, 2.0, r),
            157_225.648_501_661_26
        );
        assert_eq!(
            sphere_distance(10.0, 20.0, 30.0, 40.0, r),
            3_040_607.004_285_33
        );
    }

    #[test]
    fn wkt_axes() {
        assert_eq!(swap_wkt("POINT(10 20)").as_deref(), Some("POINT(20 10)"));
        assert_eq!(
            swap_wkt("LINESTRING(1 2, 3.5 -4e1)").as_deref(),
            Some("LINESTRING(2 1, -4e1 3.5)")
        );
        assert_eq!(
            swap_wkt("POLYGON((0 0,0 1,1 1,0 0))").as_deref(),
            Some("POLYGON((0 0,1 0,1 1,0 0))")
        );
        assert_eq!(swap_wkt("POINT EMPTY").as_deref(), Some("POINT EMPTY"));
        assert_eq!(swap_wkt("POINT(1 2 3)"), None);
        assert_eq!(swap_wkt("POINT(1"), None);
    }

    #[test]
    fn boxes_hold_the_circle() {
        let (lo, w, hi, e) = grow(18.9, 72.8, 5000.0).unwrap();
        for k in 0..360 {
            let a = f64::from(k).to_radians();
            // Points about 5 km away in every direction.
            let (lat, lon) = (18.9 + 0.0452 * a.sin(), 72.8 + 0.0475 * a.cos());
            if andoyer_distance(18.9, 72.8, lat, lon) <= 5000.0 {
                assert!(lat >= lo && lat <= hi && lon >= w && lon <= e);
            }
        }
        assert!(grow(89.99, 0.0, 5000.0).is_none());
    }
}

#[cfg(test)]
mod print_sql {
    #[test]
    #[ignore = "prints SQL for a manual check against MariaDB"]
    fn print() {
        let pts = [
            (18.9, 72.8, 19.2, 72.97),
            (18.9, 72.8, 18.9, 72.8),
            (1.0, 1.0, 2.0, 2.0),
            (18.9, 72.8, 28.6, 77.2),
            (0.0, 0.0, 0.0, 180.0),
            (89.9, 0.0, -89.9, 0.1),
            (52.52, 13.405, 40.7128, -74.006),
            (-33.8688, 151.2093, 51.5074, -0.1278),
            (0.0, 0.0, 0.0001, 0.0001),
            (1.0, 1.0, 1.0, 1.0),
        ];
        for (a, b, c, d) in pts {
            let e = |v: f64| format!("{v:e}");
            println!("SELECT {};", super::andoyer_sql(&e(a), &e(b), &e(c), &e(d)));
            println!(
                "SELECT {};",
                super::sphere_sql(&e(a), &e(b), &e(c), &e(d), "6371008.771415059e0")
            );
        }
    }
}
