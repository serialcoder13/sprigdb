//! Geo commands on sorted sets: a sorted set scored by Redis's
//! 52-bit geohash, with Redis's haversine rather than the geo layer's
//! geodesics, so results match Redis.
//!
//! Searches read every member of the key and keep those
//! inside the shape, the same set Redis returns after its own exact
//! filter; reading only the covering geohash ranges is a later
//! optimization.

use bytes::Bytes;
use sprig_kv::layout::KeyType;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_f64;

use crate::cmd::zsets;
use crate::ctx::{Ctx, R, fail, int_arg, upper};
use crate::reply;

const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;
const LAT_MIN: f64 = -85.051_128_78;
const LAT_MAX: f64 = 85.051_128_78;
const STEP: u32 = 26;
/// Redis's `EARTH_RADIUS_IN_METERS`.
const EARTH_RADIUS: f64 = 6_372_797.560_856;

fn spread(x: u32) -> u64 {
    let mut x = u64::from(x);
    x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
    x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    x = (x | (x << 2)) & 0x3333_3333_3333_3333;
    (x | (x << 1)) & 0x5555_5555_5555_5555
}

fn squash(x: u64) -> u32 {
    let mut x = x & 0x5555_5555_5555_5555;
    x = (x | (x >> 1)) & 0x3333_3333_3333_3333;
    x = (x | (x >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    x = (x | (x >> 4)) & 0x00FF_00FF_00FF_00FF;
    x = (x | (x >> 8)) & 0x0000_FFFF_0000_FFFF;
    x = (x | (x >> 16)) & 0x0000_0000_FFFF_FFFF;
    u32::try_from(x).unwrap_or(u32::MAX)
}

/// Redis's `geohashEncode` over the given ranges: lat bits even, lon odd.
fn encode(lon: f64, lat: f64, lat_min: f64, lat_max: f64) -> u64 {
    let scale = f64::from(1u32 << STEP);
    let lat_off = (lat - lat_min) / (lat_max - lat_min) * scale;
    let lon_off = (lon - LON_MIN) / (LON_MAX - LON_MIN) * scale;
    spread(lat_off as u32) | (spread(lon_off as u32) << 1)
}

/// The cell centre of a 52-bit hash (`geohashDecodeToLongLatWGS84`).
fn decode(bits: u64) -> (f64, f64) {
    let ilat = f64::from(squash(bits));
    let ilon = f64::from(squash(bits >> 1));
    let scale = f64::from(1u32 << STEP);
    let lat_s = LAT_MAX - LAT_MIN;
    let lon_s = LON_MAX - LON_MIN;
    let lat_min = LAT_MIN + (ilat / scale) * lat_s;
    let lat_max = LAT_MIN + ((ilat + 1.0) / scale) * lat_s;
    let lon_min = LON_MIN + (ilon / scale) * lon_s;
    let lon_max = LON_MIN + ((ilon + 1.0) / scale) * lon_s;
    let lon = f64::midpoint(lon_min, lon_max).clamp(LON_MIN, LON_MAX);
    let lat = f64::midpoint(lat_min, lat_max).clamp(LAT_MIN, LAT_MAX);
    (lon, lat)
}

fn rad(d: f64) -> f64 {
    d * (std::f64::consts::PI / 180.0)
}

/// Redis's haversine (`geohashGetDistance`).
fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (la1, lo1, la2, lo2) = (rad(lat1), rad(lon1), rad(lat2), rad(lon2));
    let u = ((la2 - la1) / 2.0).sin();
    let v = ((lo2 - lo1) / 2.0).sin();
    2.0 * EARTH_RADIUS * (u * u + la1.cos() * la2.cos() * v * v).sqrt().asin()
}

/// `%.17Lf` with trailing zeros dropped (`addReplyHumanLongDouble`).
fn human(d: f64) -> Frame {
    let mut s = format!("{d:.17}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    Frame::DoubleText(Bytes::from(s))
}

fn unit(a: &[u8]) -> R<f64> {
    match a.to_ascii_lowercase().as_slice() {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => fail(Frame::err(
            "unsupported unit provided. please use M, KM, FT, MI",
        )),
    }
}

fn dist_frame(d: f64) -> Frame {
    Frame::bulk(format!("{d:.4}"))
}

fn lonlat(lon: &[u8], lat: &[u8]) -> R<(f64, f64)> {
    match (parse_f64(lon), parse_f64(lat)) {
        (Some(x), Some(y)) => Ok((x, y)),
        _ => fail(reply::not_float()),
    }
}

fn check_pair(x: f64, y: f64) -> R<()> {
    if !(LON_MIN..=LON_MAX).contains(&x) || !(LAT_MIN..=LAT_MAX).contains(&y) {
        return fail(Frame::err(&format!(
            "invalid longitude,latitude pair {x:.6},{y:.6}"
        )));
    }
    Ok(())
}

/// `GEOADD k [NX|XX] [CH] lon lat m ...`.
pub async fn geoadd(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (mut nx, mut xx, mut ch) = (false, false, false);
    let mut i = 2;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "CH" => ch = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &a[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(3) {
        return fail(Frame::err(
            "syntax error. Try GEOADD key [x1] [y1] [name1] [x2] [y2] [name2] ... ",
        ));
    }
    if nx && xx {
        return fail(Frame::err(
            "XX and NX options at the same time are not compatible",
        ));
    }
    let mut items = Vec::with_capacity(rest.len() / 3);
    for c in rest.chunks(3) {
        let (x, y) = lonlat(&c[0], &c[1])?;
        check_pair(x, y)?;
        Ctx::check_elem(&c[2])?;
        items.push((c[2].to_vec(), encode(x, y, LAT_MIN, LAT_MAX) as f64));
    }
    // A repeated member keeps its last position.
    let mut dedup: Vec<(Vec<u8>, f64)> = Vec::new();
    for it in items.into_iter().rev() {
        if !dedup.iter().any(|(m, _)| *m == it.0) {
            dedup.push(it);
        }
    }
    dedup.reverse();
    let row = cx.lock_typed(&a[1], KeyType::ZSet).await?;
    let mut row = match row {
        Some(r) => r,
        None if xx => return Ok(Frame::Integer(0)),
        None => Ctx::new_row(&a[1], KeyType::ZSet),
    };
    let n = zsets::add_members(cx, &mut row, &dedup, nx, xx, ch).await?;
    Ok(Frame::Integer(n))
}

/// `GEOPOS k m...`.
pub async fn geopos(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let items: Vec<&[u8]> = a[2..].iter().map(|m| &m[..]).collect();
    let m = zsets::scores_of(cx, &a[1], &items)
        .await?
        .unwrap_or_default();
    Ok(Frame::Array(
        items
            .iter()
            .map(|i| match m.get(*i) {
                None => Frame::null_array(),
                Some(s) => {
                    let (x, y) = decode(*s as u64);
                    Frame::Array(vec![human(x), human(y)])
                }
            })
            .collect(),
    ))
}

/// `GEODIST k m1 m2 [unit]`.
pub async fn geodist(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let conv = match a.len() {
        4 => 1.0,
        5 => unit(&a[4])?,
        _ => return fail(reply::syntax()),
    };
    let m = zsets::scores_of(cx, &a[1], &[&a[2], &a[3]])
        .await?
        .unwrap_or_default();
    let (Some(s1), Some(s2)) = (m.get(&a[2][..]), m.get(&a[3][..])) else {
        return Ok(Frame::null());
    };
    let (x1, y1) = decode(*s1 as u64);
    let (x2, y2) = decode(*s2 as u64);
    Ok(dist_frame(distance(x1, y1, x2, y2) / conv))
}

/// `GEOHASH k m...`: 11 base-32 characters over latitude ±90.
pub async fn geohash(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    const ALPHA: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let items: Vec<&[u8]> = a[2..].iter().map(|m| &m[..]).collect();
    let m = zsets::scores_of(cx, &a[1], &items)
        .await?
        .unwrap_or_default();
    Ok(Frame::Array(
        items
            .iter()
            .map(|i| match m.get(*i) {
                None => Frame::null(),
                Some(s) => {
                    let (x, y) = decode(*s as u64);
                    let bits = encode(x, y, -90.0, 90.0);
                    let mut buf = Vec::with_capacity(11);
                    for j in 0..11u32 {
                        let idx = if j == 10 {
                            0
                        } else {
                            usize::try_from((bits >> (52 - (j + 1) * 5)) & 0x1f).unwrap_or(0)
                        };
                        buf.push(ALPHA[idx]);
                    }
                    Frame::Bulk(buf.into())
                }
            })
            .collect(),
    ))
}

/// A search shape.
#[derive(Debug, Clone, Copy)]
enum Shape {
    Radius(f64),
    Box(f64, f64),
}

/// A parsed search.
struct Search {
    center: Option<(f64, f64)>,
    member: Option<Bytes>,
    shape: Option<Shape>,
    conv: f64,
    sort: Option<bool>,
    count: Option<(i64, bool)>,
    withcoord: bool,
    withdist: bool,
    withhash: bool,
    store: Option<(Bytes, bool)>,
}

#[allow(clippy::too_many_lines)]
fn parse_opts(a: &[Bytes], mut i: usize, s: &mut Search, geosearch: bool, ro: bool) -> R<()> {
    while i < a.len() {
        let left = a.len() - i - 1;
        match upper(&a[i]).as_str() {
            "WITHCOORD" => s.withcoord = true,
            "WITHDIST" => s.withdist = true,
            "WITHHASH" => s.withhash = true,
            "ASC" => s.sort = Some(true),
            "DESC" => s.sort = Some(false),
            "ANY" => match &mut s.count {
                Some((_, any)) => *any = true,
                None => {
                    return fail(Frame::err("the ANY argument requires COUNT argument"));
                }
            },
            "COUNT" if left >= 1 => {
                let n = int_arg(&a[i + 1])?;
                if n <= 0 {
                    return fail(Frame::err("COUNT must be > 0"));
                }
                let any = i + 2 < a.len() && upper(&a[i + 2]) == "ANY";
                s.count = Some((n, any));
                i += if any { 2 } else { 1 };
            }
            "STORE" | "STOREDIST" if !geosearch && !ro && left >= 1 => {
                s.store = Some((a[i + 1].clone(), upper(&a[i]) == "STOREDIST"));
                i += 1;
            }
            "STOREDIST" if geosearch && !ro => {
                if s.store.is_none() {
                    return fail(reply::syntax());
                }
                if let Some(st) = &mut s.store {
                    st.1 = true;
                }
            }
            "FROMMEMBER" if geosearch && left >= 1 => {
                if s.center.is_some() || s.member.is_some() {
                    return fail(Frame::err(
                        "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
                    ));
                }
                s.member = Some(a[i + 1].clone());
                i += 1;
            }
            "FROMLONLAT" if geosearch && left >= 2 => {
                if s.center.is_some() || s.member.is_some() {
                    return fail(Frame::err(
                        "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
                    ));
                }
                let (x, y) = lonlat(&a[i + 1], &a[i + 2])?;
                check_pair(x, y)?;
                s.center = Some((x, y));
                i += 2;
            }
            "BYRADIUS" if geosearch && left >= 2 => {
                if s.shape.is_some() {
                    return fail(Frame::err(
                        "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
                    ));
                }
                let Some(r) = parse_f64(&a[i + 1]).filter(|r| *r >= 0.0) else {
                    return fail(Frame::err("radius cannot be negative"));
                };
                s.conv = unit(&a[i + 2])?;
                s.shape = Some(Shape::Radius(r));
                i += 2;
            }
            "BYBOX" if geosearch && left >= 3 => {
                if s.shape.is_some() {
                    return fail(Frame::err(
                        "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
                    ));
                }
                let (Some(w), Some(h)) = (parse_f64(&a[i + 1]), parse_f64(&a[i + 2])) else {
                    return fail(reply::not_float());
                };
                if w < 0.0 || h < 0.0 {
                    return fail(Frame::err("height or width cannot be negative"));
                }
                s.conv = unit(&a[i + 3])?;
                s.shape = Some(Shape::Box(w, h));
                i += 3;
            }
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    if geosearch {
        if s.center.is_none() && s.member.is_none() {
            return fail(Frame::err(
                "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
            ));
        }
        if s.shape.is_none() {
            return fail(Frame::err(
                "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
            ));
        }
    }
    if s.store.is_some() && (s.withcoord || s.withdist || s.withhash) {
        return fail(Frame::err(if geosearch {
            "WITH* options are not allowed in GEOSEARCHSTORE"
        } else {
            "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        }));
    }
    Ok(())
}

/// Runs a parsed search on `src`.
async fn run_search(cx: &mut Ctx<'_>, src: &[u8], mut s: Search) -> R<Frame> {
    let Some(row) = zsets::row_of(cx, src).await? else {
        if let Some((dst, _)) = &s.store {
            let old = cx.lock(dst).await?;
            if let Some(o) = old {
                cx.delete(&o).await?;
            }
            return Ok(Frame::Integer(0));
        }
        return Ok(Frame::Array(Vec::new()));
    };
    let all = zsets::all_members(cx, &row).await?;
    if let Some(m) = &s.member {
        match all.iter().find(|(x, _)| x.as_slice() == &m[..]) {
            Some((_, sc)) => s.center = Some(decode(*sc as u64)),
            None => {
                return fail(Frame::err("could not decode requested zset member"));
            }
        }
    }
    let (cx0, cy0) = s.center.unwrap_or((0.0, 0.0));
    let shape = s.shape.unwrap_or(Shape::Radius(0.0));
    if s.count.is_some_and(|(_, any)| !any) && s.sort.is_none() {
        s.sort = Some(true);
    }
    // Testing and sorting a large set's members runs off the runtime's worker threads.
    let n = all.len();
    let (conv, sort) = (s.conv, s.sort);
    let mut hits = sprig_common::cpu::heavy(n, move || {
        let mut hits = Vec::new();
        for (m, sc) in all {
            let (x, y) = decode(sc as u64);
            let d = match shape {
                Shape::Radius(r) => {
                    let d = distance(cx0, cy0, x, y);
                    if d > r * conv {
                        continue;
                    }
                    d
                }
                Shape::Box(w, h) => {
                    let lat_d = EARTH_RADIUS * (rad(cy0) - rad(y)).abs();
                    if lat_d > h * conv / 2.0 {
                        continue;
                    }
                    if distance(x, y, cx0, y) > w * conv / 2.0 {
                        continue;
                    }
                    distance(cx0, cy0, x, y)
                }
            };
            hits.push((m, sc, d, x, y));
        }
        match sort {
            Some(true) => hits.sort_by(|a, b| a.2.total_cmp(&b.2)),
            Some(false) => hits.sort_by(|a, b| b.2.total_cmp(&a.2)),
            None => {}
        }
        hits
    });
    if let Some((n, _)) = s.count {
        hits.truncate(usize::try_from(n).unwrap_or(usize::MAX));
    }
    if let Some((dst, storedist)) = &s.store {
        let items: Vec<(Vec<u8>, f64)> = hits
            .iter()
            .map(|(m, sc, d, _, _)| (m.clone(), if *storedist { d / s.conv } else { *sc }))
            .collect();
        zsets::store(cx, dst, &items).await?;
        return Ok(Frame::Integer(items.len() as i64));
    }
    let plain = !(s.withcoord || s.withdist || s.withhash);
    Ok(Frame::Array(
        hits.into_iter()
            .map(|(m, sc, d, x, y)| {
                if plain {
                    return Frame::Bulk(m.into());
                }
                let mut item = vec![Frame::Bulk(m.into())];
                if s.withdist {
                    item.push(dist_frame(d / s.conv));
                }
                if s.withhash {
                    item.push(Frame::Integer(sc as i64));
                }
                if s.withcoord {
                    item.push(Frame::Array(vec![human(x), human(y)]));
                }
                Frame::Array(item)
            })
            .collect(),
    ))
}

fn empty_search() -> Search {
    Search {
        center: None,
        member: None,
        shape: None,
        conv: 1.0,
        sort: None,
        count: None,
        withcoord: false,
        withdist: false,
        withhash: false,
        store: None,
    }
}

/// `GEOSEARCH k ...` and `GEOSEARCHSTORE dst src ...`.
pub async fn geosearch(cx: &mut Ctx<'_>, a: &[Bytes], store: bool) -> R<Frame> {
    let mut s = empty_search();
    let (src, start) = if store {
        s.store = Some((a[1].clone(), false));
        (a[2].clone(), 3)
    } else {
        (a[1].clone(), 2)
    };
    parse_opts(a, start, &mut s, true, false)?;
    run_search(cx, &src, s).await
}

/// `GEORADIUS k lon lat r unit ...`, `GEORADIUSBYMEMBER k m r unit ...`
/// (and their `_RO` forms).
pub async fn georadius(cx: &mut Ctx<'_>, a: &[Bytes], by_member: bool, ro: bool) -> R<Frame> {
    let mut s = empty_search();
    let at = if by_member {
        s.member = Some(a[2].clone());
        3
    } else {
        let (x, y) = lonlat(&a[2], &a[3])?;
        check_pair(x, y)?;
        s.center = Some((x, y));
        4
    };
    let Some(r) = parse_f64(&a[at]).filter(|r| *r >= 0.0) else {
        return fail(Frame::err("radius cannot be negative"));
    };
    s.conv = unit(&a[at + 1])?;
    s.shape = Some(Shape::Radius(r));
    parse_opts(a, at + 2, &mut s, false, ro)?;
    run_search(cx, &a[1], s).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geohash_round_trip_matches_redis() {
        // Redis: GEOADD Sicily 13.361389 38.115556 "Palermo" stores score
        // 3479099956230698; GEOPOS gives 13.36138933897018433 38.11555639549629859.
        let h = encode(13.361_389, 38.115_556, LAT_MIN, LAT_MAX);
        assert_eq!(h, 3_479_099_956_230_698);
        let (x, y) = decode(h);
        assert_eq!(format!("{x:.17}"), "13.36138933897018433");
        assert_eq!(format!("{y:.17}"), "38.11555639549629859");
        let c = encode(15.087_269, 37.502_669, LAT_MIN, LAT_MAX);
        let (x2, y2) = decode(c);
        // GEODIST Sicily Palermo Catania = 166274.1516 m.
        assert_eq!(format!("{:.4}", distance(x, y, x2, y2)), "166274.1516");
    }
}
