//! Locations of keys and `GeoSearch` (chapter 33, "Interfaces"; chapter
//! 36, "Other models"; R6 round 3).
//!
//! A namespace gets two columns on its key table the first time a key is
//! given a location: `loc POINT NULL` (longitude in X, latitude in Y, as
//! SQL users read it) and the hidden `_sprig_s2`, the location's level-30
//! S2 leaf cell, with an index. A search covers its region with S2 ranges
//! (`_sprig_s2 BETWEEN ...`), keeps the candidates inside the region by
//! the exact WGS 84 test (Karney), and orders by distance when asked.
//! A nearest-k search without a region grows a circle from
//! `geo.knn_initial_radius_m` (1 km) by four until it holds k keys, as
//! chapter 36's `nearest()` does. The statements run through the caller's
//! session, so on a sharded namespace each shard is read and the router
//! merges.

use sprig_common::{SprigCode, SprigError};
use sprig_geo::region::{Region, distance_m};

use crate::api::Entry;
use crate::layout::{KeyType, Namespace, live};
use crate::sql::{SqlError, SqlRunner, cell_f64, cell_i64, cell_u64};
use crate::time::ms_sql;

/// `geo.knn_initial_radius_m`.
pub const KNN_INITIAL_RADIUS_M: f64 = 1000.0;
/// Half the earth's circumference, roughly: a circle this wide covers it.
const EARTH_COVERED_M: f64 = 20_100_000.0;

fn sql(e: &SqlError) -> SprigError {
    SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", &e.message)
}

/// The statement adding the location columns (instant in MariaDB 11.8).
#[must_use]
pub fn ddl(ns: &Namespace) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS loc POINT NULL, \
         ADD COLUMN IF NOT EXISTS _sprig_s2 BIGINT UNSIGNED NULL INVISIBLE, \
         ADD INDEX IF NOT EXISTS _sprig_s2 (_sprig_s2)",
        ns.keys()
    )
}

/// Checks a location.
///
/// # Errors
/// 39765 out of range.
pub fn check(lat: f64, lng: f64) -> Result<(), SprigError> {
    if !lat.is_finite()
        || !lng.is_finite()
        || !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lng)
    {
        return Err(SprigError::new(
            SprigCode::GeoInvalid,
            format!("latitude {lat} or longitude {lng} out of range"),
        ));
    }
    Ok(())
}

/// `SET` items writing a location (or clearing it).
#[must_use]
pub fn set_sql(loc: Option<(f64, f64)>) -> String {
    match loc {
        Some((lat, lng)) => format!(
            "loc = POINT({lng:e}, {lat:e}), _sprig_s2 = {}",
            sprig_geo::s2::leaf(lat, lng)
        ),
        None => "loc = NULL, _sprig_s2 = NULL".into(),
    }
}

/// One key found by a search.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    /// The key (its value for strings).
    pub entry: Entry,
    /// Latitude.
    pub lat: f64,
    /// Longitude.
    pub lng: f64,
    /// Metres from the search's center, when it has one.
    pub distance_m: Option<f64>,
}

/// The keys of `ns` inside `region`, by S2 ranges and the exact test.
async fn candidates(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    region: &Region,
) -> Result<Vec<Hit>, SprigError> {
    let ranges = region
        .ranges()
        .iter()
        .map(|(a, b)| format!("_sprig_s2 BETWEEN {a} AND {b}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let q = format!(
        "SELECT k, type, v, _v, {}, ST_Y(loc), ST_X(loc) FROM {} WHERE ({ranges}) AND {}",
        ms_sql("expires_at"),
        ns.keys(),
        live("")
    );
    let rows = match s.query(&q).await {
        Ok(r) => r,
        // No key of the namespace has had a location yet.
        Err(e) if e.errno == Some(1054) => return Ok(Vec::new()),
        Err(e) => return Err(sql(&e)),
    };
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let lat = r
                .get(5)
                .and_then(Option::as_ref)
                .map(|c| cell_f64(Some(c)))?;
            let lng = r
                .get(6)
                .and_then(Option::as_ref)
                .map(|c| cell_f64(Some(c)))?;
            if !region.contains(lat, lng) {
                return None;
            }
            Some(Hit {
                entry: Entry {
                    key: r.first().cloned().flatten().unwrap_or_default(),
                    kind: r
                        .get(1)
                        .and_then(|c| c.as_deref())
                        .and_then(KeyType::parse)
                        .unwrap_or(KeyType::String),
                    value: r.get(2).cloned().flatten().unwrap_or_default(),
                    version: cell_u64(r.get(3).and_then(Option::as_ref)),
                    expires_at_ms: r.get(4).and_then(Option::as_ref).map(|c| cell_i64(Some(c))),
                },
                lat,
                lng,
                distance_m: None,
            })
        })
        .collect())
}

/// Orders `hits` by distance from `from` (then by key) and keeps `limit`.
fn rank(hits: &mut Vec<Hit>, from: Option<(f64, f64)>, limit: usize) {
    if let Some((la, lo)) = from {
        for h in hits.iter_mut() {
            h.distance_m = Some(distance_m(la, lo, h.lat, h.lng));
        }
        hits.sort_by(|a, b| {
            a.distance_m
                .unwrap_or(0.0)
                .total_cmp(&b.distance_m.unwrap_or(0.0))
                .then_with(|| a.entry.key.cmp(&b.entry.key))
        });
    } else {
        hits.sort_by(|a, b| a.entry.key.cmp(&b.entry.key));
    }
    hits.truncate(limit);
}

/// `GeoSearch`: the keys inside `region` (a circle's center orders them
/// by distance unless `order_from` says otherwise), at most `limit`.
///
/// # Errors
/// Backend errors.
pub async fn search(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    region: &Region,
    order_from: Option<(f64, f64)>,
    limit: usize,
) -> Result<Vec<Hit>, SprigError> {
    let mut hits = candidates(s, ns, region).await?;
    rank(&mut hits, order_from.or_else(|| region.center()), limit);
    Ok(hits)
}

/// The `k` keys nearest to `(lat, lng)`, within `max_m` metres when given.
///
/// # Errors
/// 39765 for a bad center; backend errors.
pub async fn nearest(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    lat: f64,
    lng: f64,
    k: usize,
    max_m: Option<f64>,
) -> Result<Vec<Hit>, SprigError> {
    check(lat, lng)?;
    let cap = max_m.unwrap_or(EARTH_COVERED_M).min(EARTH_COVERED_M);
    let mut r = KNN_INITIAL_RADIUS_M.min(cap);
    loop {
        let region = Region::Circle {
            lat,
            lng,
            radius_m: r,
        };
        let mut hits = candidates(s, ns, &region).await?;
        if hits.len() >= k || r >= cap {
            rank(&mut hits, Some((lat, lng)), k);
            return Ok(hits);
        }
        r = (r * 4.0).min(cap);
    }
}

/// Sets or clears the location of live key `k`; `false` when there is no
/// such key.
///
/// # Errors
/// 39765; backend errors.
pub async fn set_location(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
    loc: Option<(f64, f64)>,
) -> Result<bool, SprigError> {
    if let Some((la, lo)) = loc {
        check(la, lo)?;
    }
    let n = s
        .exec(&format!(
            "UPDATE {} SET {} WHERE {} AND {}",
            ns.keys(),
            set_sql(loc),
            crate::api::key_pred(ns, k, ""),
            live("")
        ))
        .await
        .map_err(|e| sql(&e))?;
    if n > 0 {
        return Ok(true);
    }
    // The same location again changes nothing: check the key exists.
    let rows = s
        .query(&format!(
            "SELECT 1 FROM {} WHERE {} AND {}",
            ns.keys(),
            crate::api::key_pred(ns, k, ""),
            live("")
        ))
        .await
        .map_err(|e| sql(&e))?;
    Ok(!rows.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements() {
        let ns = Namespace::redis("redis", 0);
        assert!(ddl(&ns).contains("ADD INDEX IF NOT EXISTS _sprig_s2"));
        let s = set_sql(Some((19.07, 72.87)));
        assert!(
            s.starts_with("loc = POINT(7.287e1, 1.907e1), _sprig_s2 = "),
            "{s}"
        );
        assert!(check(91.0, 0.0).is_err());
    }
}
