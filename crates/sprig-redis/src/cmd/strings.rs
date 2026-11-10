//! Strings (chapter 40, "Strings").

use bytes::Bytes;
use sprig_kv::layout::{KeyType, live};
use sprig_kv::sql::lit;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{ld2string_human, parse_f64, parse_i64};

use crate::ctx::{Ctx, KeyRow, R, fail, int_arg, upper};
use crate::reply;

/// Expiry options of `SET` and `GETEX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// None given.
    Unset,
    /// Relative, ms.
    In(i64),
    /// Absolute, ms since the epoch.
    At(i64),
    /// `KEEPTTL`.
    Keep,
    /// `PERSIST`.
    Persist,
}

/// Longest string Redis allows (512 MB); SprigDB's limit is the
/// namespace's, checked separately.
const PROTO_MAX: i64 = 512 * 1024 * 1024;

/// Parses an expiry amount; `unit_ms` is 1000 for seconds.
fn ttl_amount(a: &[u8], unit_ms: i64, cmd: &str) -> R<i64> {
    let v = int_arg(a)?;
    if v <= 0 {
        return fail(Frame::err(&format!(
            "invalid expire time in '{cmd}' command"
        )));
    }
    v.checked_mul(unit_ms)
        .filter(|ms| *ms <= i64::MAX / 2)
        .map_or_else(
            || {
                fail(Frame::err(&format!(
                    "invalid expire time in '{cmd}' command"
                )))
            },
            Ok,
        )
}

/// The instant `t` gives, from MariaDB's `now`.
pub(crate) fn resolve(t: Ttl, now: i64, old: Option<i64>) -> Option<i64> {
    match t {
        Ttl::Unset | Ttl::Persist => None,
        Ttl::In(ms) => Some(now.saturating_add(ms)),
        Ttl::At(ms) => Some(ms),
        Ttl::Keep => old,
    }
}

/// `GET k`.
pub async fn get(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let r = cx.get_typed(&a[1], KeyType::String).await?;
    Ok(Frame::opt_bulk(r.and_then(|r| r.v.or(Some(Vec::new())))))
}

/// Writes string `v` to the locked `row` (any type), keeping `exp`.
pub(crate) async fn put(
    cx: &mut Ctx<'_>,
    k: &[u8],
    row: Option<KeyRow>,
    v: Vec<u8>,
    exp: Option<i64>,
) -> R<()> {
    let mut row = match row {
        Some(r) if r.t == KeyType::String => r,
        Some(r) => {
            cx.delete(&r).await?;
            Ctx::new_row(k, KeyType::String)
        }
        None => Ctx::new_row(k, KeyType::String),
    };
    row.v = Some(v);
    row.n = 0;
    row.lh = 0;
    row.lt = 0;
    row.exp = exp;
    cx.save(&mut row).await?;
    Ok(())
}

/// `SET k v [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]`.
pub async fn set(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (mut nx, mut xx, mut getf) = (false, false, false);
    let mut ttl = Ttl::Unset;
    let mut i = 3;
    while i < a.len() {
        let o = upper(&a[i]);
        let next = a.get(i + 1);
        match o.as_str() {
            "NX" if !xx => nx = true,
            "XX" if !nx => xx = true,
            "GET" => getf = true,
            "KEEPTTL" if ttl == Ttl::Unset => ttl = Ttl::Keep,
            "EX" | "PX" | "EXAT" | "PXAT" if ttl == Ttl::Unset && next.is_some() => {
                let n = next.map_or(&[][..], |b| &b[..]);
                ttl = match o.as_str() {
                    "EX" => Ttl::In(ttl_amount(n, 1000, "set")?),
                    "PX" => Ttl::In(ttl_amount(n, 1, "set")?),
                    "EXAT" => Ttl::At(ttl_amount(n, 1000, "set")?),
                    _ => Ttl::At(ttl_amount(n, 1, "set")?),
                };
                i += 1;
            }
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    cx.check_value(&a[2])?;
    let row = cx.lock(&a[1]).await?;
    if getf
        && let Some(r) = &row
        && r.t != KeyType::String
    {
        return fail(reply::wrongtype());
    }
    let old = row.as_ref().and_then(|r| r.v.clone());
    let go = if nx {
        row.is_none()
    } else if xx {
        row.is_some()
    } else {
        true
    };
    if go {
        let exp = resolve(ttl, cx.now, row.as_ref().and_then(|r| r.exp));
        put(cx, &a[1], row, a[2].to_vec(), exp).await?;
    }
    Ok(if getf {
        Frame::opt_bulk(old)
    } else if go {
        Frame::ok()
    } else {
        Frame::null()
    })
}

/// `SETNX k v`.
pub async fn setnx(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    cx.check_value(&a[2])?;
    let row = cx.lock(&a[1]).await?;
    if row.is_some() {
        return Ok(Frame::Integer(0));
    }
    put(cx, &a[1], None, a[2].to_vec(), None).await?;
    Ok(Frame::Integer(1))
}

/// `SETEX k s v`, `PSETEX k ms v`.
pub async fn setex(cx: &mut Ctx<'_>, a: &[Bytes], unit: i64, name: &str) -> R<Frame> {
    let ms = ttl_amount(&a[2], unit, name)?;
    cx.check_value(&a[3])?;
    let row = cx.lock(&a[1]).await?;
    let exp = Some(cx.now.saturating_add(ms));
    put(cx, &a[1], row, a[3].to_vec(), exp).await?;
    Ok(Frame::ok())
}

/// `GETSET k v`.
pub async fn getset(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    cx.check_value(&a[2])?;
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let old = row.as_ref().and_then(|r| r.v.clone());
    put(cx, &a[1], row, a[2].to_vec(), None).await?;
    Ok(Frame::opt_bulk(old))
}

/// `GETDEL k`.
pub async fn getdel(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let Some(row) = row else {
        return Ok(Frame::null());
    };
    cx.delete(&row).await?;
    Ok(Frame::opt_bulk(row.v))
}

/// `GETEX k [EX|PX|EXAT|PXAT|PERSIST]`.
pub async fn getex(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let mut ttl = Ttl::Unset;
    let mut i = 2;
    while i < a.len() {
        let o = upper(&a[i]);
        let next = a.get(i + 1);
        match o.as_str() {
            "PERSIST" if ttl == Ttl::Unset => ttl = Ttl::Persist,
            "EX" | "PX" | "EXAT" | "PXAT" if ttl == Ttl::Unset && next.is_some() => {
                let n = next.map_or(&[][..], |b| &b[..]);
                ttl = match o.as_str() {
                    "EX" => Ttl::In(ttl_amount(n, 1000, "getex")?),
                    "PX" => Ttl::In(ttl_amount(n, 1, "getex")?),
                    "EXAT" => Ttl::At(ttl_amount(n, 1000, "getex")?),
                    _ => Ttl::At(ttl_amount(n, 1, "getex")?),
                };
                i += 1;
            }
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let Some(mut row) = row else {
        return Ok(Frame::null());
    };
    let v = row.v.clone();
    match ttl {
        Ttl::Unset => {}
        Ttl::Persist => {
            if row.exp.is_some() {
                row.exp = None;
                cx.save(&mut row).await?;
            }
        }
        t => {
            let exp = resolve(t, cx.now, row.exp);
            if exp.is_some_and(|e| e <= cx.now) {
                cx.delete(&row).await?;
            } else {
                row.exp = exp;
                cx.save(&mut row).await?;
            }
        }
    }
    Ok(Frame::opt_bulk(v))
}

/// `MGET k...`: one statement, so one snapshot.
pub async fn mget(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let keys: Vec<&Bytes> = a[1..].iter().collect();
    let list = keys
        .iter()
        .filter(|k| k.len() <= 1024)
        .map(|k| lit(k))
        .collect::<Vec<_>>();
    let mut found: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>> =
        std::collections::HashMap::new();
    if !list.is_empty() {
        let rows = cx
            .q(&format!(
                "SELECT k, v FROM {} WHERE k IN ({}) AND type = 'string' AND {}",
                cx.ns.keys(),
                list.join(", "),
                live("")
            ))
            .await?;
        for r in rows {
            if let Some(Some(k)) = r.first() {
                found.insert(
                    k.clone(),
                    Some(r.get(1).cloned().flatten().unwrap_or_default()),
                );
            }
        }
    }
    Ok(Frame::Array(
        keys.iter()
            .map(|k| Frame::opt_bulk(found.get(k.as_ref()).cloned().flatten()))
            .collect(),
    ))
}

/// `MSET k v ...` and `MSETNX`.
pub async fn mset(cx: &mut Ctx<'_>, a: &[Bytes], nx: bool) -> R<Frame> {
    if a.len().is_multiple_of(2) {
        return fail(reply::arity(if nx { "msetnx" } else { "mset" }));
    }
    for v in a[2..].iter().step_by(2) {
        cx.check_value(v)?;
    }
    let keys: Vec<&[u8]> = a[1..].iter().step_by(2).map(|k| &k[..]).collect();
    let rows = cx.lock_many(&keys).await?;
    if nx && rows.iter().any(Option::is_some) {
        return Ok(Frame::Integer(0));
    }
    // The last value of a repeated key wins.
    let mut done: Vec<&[u8]> = Vec::new();
    for i in (1..a.len()).step_by(2).rev() {
        let k = &a[i][..];
        if done.contains(&k) {
            continue;
        }
        done.push(k);
        let idx = (i - 1) / 2;
        put(cx, k, rows[idx].clone(), a[i + 1].to_vec(), None).await?;
    }
    Ok(if nx { Frame::Integer(1) } else { Frame::ok() })
}

/// `APPEND k v`.
pub async fn append(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let mut v = row.as_ref().and_then(|r| r.v.clone()).unwrap_or_default();
    v.extend_from_slice(&a[2]);
    cx.check_value(&v)?;
    let len = v.len();
    let exp = row.as_ref().and_then(|r| r.exp);
    put(cx, &a[1], row, v, exp).await?;
    Ok(Frame::Integer(i64::try_from(len).unwrap_or(i64::MAX)))
}

/// `STRLEN k`.
pub async fn strlen(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let r = cx.get_typed(&a[1], KeyType::String).await?;
    let n = r.and_then(|r| r.v).map_or(0, |v| v.len());
    Ok(Frame::Integer(i64::try_from(n).unwrap_or(i64::MAX)))
}

/// `INCR`, `DECR`, `INCRBY`, `DECRBY`.
pub async fn incrby(cx: &mut Ctx<'_>, k: &[u8], by: i64) -> R<Frame> {
    let row = cx.lock_typed(k, KeyType::String).await?;
    let cur = match row.as_ref().and_then(|r| r.v.as_deref()) {
        None => 0,
        Some(v) => match parse_i64(v) {
            Some(n) => n,
            None => return fail(reply::not_int()),
        },
    };
    let Some(n) = cur.checked_add(by) else {
        return fail(Frame::err("increment or decrement would overflow"));
    };
    let exp = row.as_ref().and_then(|r| r.exp);
    put(cx, k, row, n.to_string().into_bytes(), exp).await?;
    Ok(Frame::Integer(n))
}

/// `DECRBY k n`: `i64::MIN` can't be negated.
pub async fn decrby(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let by = int_arg(&a[2])?;
    if by == i64::MIN {
        return fail(Frame::err("decrement would overflow"));
    }
    incrby(cx, &a[1], -by).await
}

/// `INCRBYFLOAT k f`.
pub async fn incrbyfloat(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(by) = parse_f64(&a[2]) else {
        return fail(reply::not_float());
    };
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let cur = match row.as_ref().and_then(|r| r.v.as_deref()) {
        None => 0.0,
        Some(v) => match parse_f64(v) {
            Some(n) => n,
            None => return fail(reply::not_float()),
        },
    };
    let n = cur + by;
    if n.is_nan() || n.is_infinite() {
        return fail(Frame::err("increment would produce NaN or Infinity"));
    }
    let text = ld2string_human(n);
    let exp = row.as_ref().and_then(|r| r.exp);
    put(cx, &a[1], row, text.clone().into_bytes(), exp).await?;
    Ok(Frame::bulk(text))
}

/// Redis's range rule for `GETRANGE` (`start`, `end` inclusive).
fn byte_range(len: i64, mut start: i64, mut end: i64) -> Option<(usize, usize)> {
    if start < 0 && end < 0 && start > end {
        return None;
    }
    if start < 0 {
        start += len;
    }
    if end < 0 {
        end += len;
    }
    start = start.max(0);
    end = end.max(0);
    if end >= len {
        end = len - 1;
    }
    if start > end || len == 0 {
        return None;
    }
    Some((
        usize::try_from(start).unwrap_or(0),
        usize::try_from(end).unwrap_or(0),
    ))
}

/// `GETRANGE k start end` (and `SUBSTR`).
pub async fn getrange(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (s, e) = (int_arg(&a[2])?, int_arg(&a[3])?);
    let r = cx.get_typed(&a[1], KeyType::String).await?;
    let v = r.and_then(|r| r.v).unwrap_or_default();
    let len = i64::try_from(v.len()).unwrap_or(i64::MAX);
    Ok(match byte_range(len, s, e) {
        Some((s, e)) => Frame::bulk_from(&v[s..=e]),
        None => Frame::bulk_from(b""),
    })
}

/// `SETRANGE k offset v`.
pub async fn setrange(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let off = int_arg(&a[2])?;
    if off < 0 {
        return fail(Frame::err("offset is out of range"));
    }
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let mut v = row.as_ref().and_then(|r| r.v.clone()).unwrap_or_default();
    if a[3].is_empty() {
        return Ok(Frame::Integer(i64::try_from(v.len()).unwrap_or(0)));
    }
    let len = i64::try_from(a[3].len()).unwrap_or(i64::MAX);
    if off.saturating_add(len) > PROTO_MAX {
        return fail(Frame::err(
            "string exceeds maximum allowed size (proto-max-bulk-len)",
        ));
    }
    let off = usize::try_from(off).unwrap_or(0);
    let end = off + a[3].len();
    if end as u64 > cx.ns.max_value_bytes as u64 {
        return fail(reply::value_too_big(cx.ns.max_value_bytes));
    }
    if v.len() < end {
        v.resize(end, 0);
    }
    v[off..end].copy_from_slice(&a[3]);
    let n = v.len();
    let exp = row.as_ref().and_then(|r| r.exp);
    put(cx, &a[1], row, v, exp).await?;
    Ok(Frame::Integer(i64::try_from(n).unwrap_or(0)))
}

/// A bit offset: 0 to 2^32 - 1.
fn bit_offset(a: &[u8]) -> R<u64> {
    match parse_i64(a) {
        Some(n) if (0..(1i64 << 32)).contains(&n) => Ok(n.unsigned_abs()),
        _ => fail(Frame::err("bit offset is not an integer or out of range")),
    }
}

/// `SETBIT k offset 0|1`.
pub async fn setbit(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let off = bit_offset(&a[2])?;
    let bit = match &a[3][..] {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return fail(Frame::err("bit is not an integer or out of range")),
    };
    let byte = usize::try_from(off >> 3).unwrap_or(usize::MAX);
    if byte >= cx.ns.max_value_bytes {
        return fail(reply::value_too_big(cx.ns.max_value_bytes));
    }
    let row = cx.lock_typed(&a[1], KeyType::String).await?;
    let mut v = row.as_ref().and_then(|r| r.v.clone()).unwrap_or_default();
    if v.len() <= byte {
        v.resize(byte + 1, 0);
    }
    let shift = 7 - (off & 7) as u8;
    let old = (v[byte] >> shift) & 1;
    v[byte] = (v[byte] & !(1 << shift)) | (bit << shift);
    let exp = row.as_ref().and_then(|r| r.exp);
    put(cx, &a[1], row, v, exp).await?;
    Ok(Frame::Integer(i64::from(old)))
}

/// `GETBIT k offset`.
pub async fn getbit(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let off = bit_offset(&a[2])?;
    let r = cx.get_typed(&a[1], KeyType::String).await?;
    let v = r.and_then(|r| r.v).unwrap_or_default();
    let byte = usize::try_from(off >> 3).unwrap_or(usize::MAX);
    let bit = v.get(byte).map_or(0, |b| (b >> (7 - (off & 7))) & 1);
    Ok(Frame::Integer(i64::from(bit)))
}

/// Redis's start/end handling for `BITCOUNT` and `BITPOS` over `len` units.
fn unit_range(len: i64, mut s: i64, mut e: i64) -> Option<(i64, i64)> {
    if s < 0 {
        s += len;
    }
    if e < 0 {
        e += len;
    }
    s = s.max(0);
    e = e.max(0);
    if e >= len {
        e = len - 1;
    }
    if s > e || len == 0 {
        None
    } else {
        Some((s, e))
    }
}

fn bit_at(v: &[u8], i: i64) -> u8 {
    let i = usize::try_from(i).unwrap_or(0);
    (v[i >> 3] >> (7 - (i & 7))) & 1
}

/// `BITCOUNT k [start end [BYTE|BIT]]`.
pub async fn bitcount(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let mut range = None;
    let mut bits = false;
    match a.len() {
        2 => {}
        4 | 5 => {
            range = Some((int_arg(&a[2])?, int_arg(&a[3])?));
            if a.len() == 5 {
                match upper(&a[4]).as_str() {
                    "BIT" => bits = true,
                    "BYTE" => {}
                    _ => return fail(reply::syntax()),
                }
            }
        }
        _ => return fail(reply::syntax()),
    }
    let r = cx.get_typed(&a[1], KeyType::String).await?;
    let v = r.and_then(|r| r.v).unwrap_or_default();
    let len = i64::try_from(v.len()).unwrap_or(0);
    let count = match range {
        None => v.iter().map(|b| i64::from(b.count_ones())).sum(),
        Some((s, e)) if bits => unit_range(len * 8, s, e)
            .map_or(0, |(s, e)| (s..=e).map(|i| i64::from(bit_at(&v, i))).sum()),
        Some((s, e)) => unit_range(len, s, e).map_or(0, |(s, e)| {
            v[usize::try_from(s).unwrap_or(0)..=usize::try_from(e).unwrap_or(0)]
                .iter()
                .map(|b| i64::from(b.count_ones()))
                .sum()
        }),
    };
    Ok(Frame::Integer(count))
}

/// `BITPOS k bit [start [end [BYTE|BIT]]]`.
pub async fn bitpos(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let bit = match &a[2][..] {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return fail(Frame::err("The bit argument must be 1 or 0.")),
    };
    if a.len() > 6 {
        return fail(reply::syntax());
    }
    let start = a.get(3).map(|x| int_arg(x)).transpose()?;
    let end = a.get(4).map(|x| int_arg(x)).transpose()?;
    let bits = match a.get(5).map(upper).as_deref() {
        None | Some("BYTE") => false,
        Some("BIT") => true,
        _ => return fail(reply::syntax()),
    };
    let r = cx.get_typed(&a[1], KeyType::String).await?;
    let Some(v) = r.and_then(|r| r.v) else {
        return Ok(Frame::Integer(if bit == 1 { -1 } else { 0 }));
    };
    let blen = i64::try_from(v.len()).unwrap_or(0);
    let units = if bits { blen * 8 } else { blen };
    let end_given = end.is_some();
    let Some((s, e)) = unit_range(units, start.unwrap_or(0), end.unwrap_or(-1)) else {
        return Ok(Frame::Integer(-1));
    };
    let (bs, be) = if bits { (s, e) } else { (s * 8, e * 8 + 7) };
    for i in bs..=be {
        if bit_at(&v, i) == bit {
            return Ok(Frame::Integer(i));
        }
    }
    // Looking for a clear bit past the value without an end: Redis
    // answers the first bit after it.
    if bit == 0 && !end_given {
        return Ok(Frame::Integer(be + 1));
    }
    Ok(Frame::Integer(-1))
}

/// `BITOP AND|OR|XOR|NOT dest k...`.
pub async fn bitop(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let op = upper(&a[1]);
    if !matches!(op.as_str(), "AND" | "OR" | "XOR" | "NOT") {
        return fail(reply::syntax());
    }
    if op == "NOT" && a.len() != 4 {
        return fail(Frame::err(
            "BITOP NOT must be called with a single source key.",
        ));
    }
    let mut keys: Vec<&[u8]> = vec![&a[2][..]];
    keys.extend(a[3..].iter().map(|k| &k[..]));
    let rows = cx.lock_many(&keys).await?;
    let mut srcs = Vec::new();
    for r in &rows[1..] {
        match r {
            Some(r) if r.t != KeyType::String => return fail(reply::wrongtype()),
            Some(r) => srcs.push(r.v.clone().unwrap_or_default()),
            None => srcs.push(Vec::new()),
        }
    }
    let max = srcs.iter().map(Vec::len).max().unwrap_or(0);
    let mut out = vec![0u8; max];
    for (i, o) in out.iter_mut().enumerate() {
        let at = |s: &Vec<u8>| s.get(i).copied().unwrap_or(0);
        *o = match op.as_str() {
            "NOT" => !at(&srcs[0]),
            "AND" => srcs.iter().fold(0xFF, |acc, s| acc & at(s)),
            "OR" => srcs.iter().fold(0, |acc, s| acc | at(s)),
            _ => srcs.iter().fold(0, |acc, s| acc ^ at(s)),
        };
    }
    let dest = rows[0].clone();
    if out.is_empty() {
        if let Some(d) = dest {
            cx.delete(&d).await?;
        }
        return Ok(Frame::Integer(0));
    }
    let n = out.len();
    put(cx, &a[2], dest, out, None).await?;
    Ok(Frame::Integer(i64::try_from(n).unwrap_or(0)))
}

/// `LCS k1 k2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]`.
pub async fn lcs(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (mut len_only, mut idx, mut withlen) = (false, false, false);
    let mut minlen = 0i64;
    let mut i = 3;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "LEN" => len_only = true,
            "IDX" => idx = true,
            "WITHMATCHLEN" => withlen = true,
            "MINMATCHLEN" if i + 1 < a.len() => {
                minlen = int_arg(&a[i + 1])?.max(0);
                i += 1;
            }
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    if len_only && idx {
        return fail(Frame::err(
            "If you want both the length and indexes, please just use IDX.",
        ));
    }
    let r1 = cx.get(&a[1]).await?;
    let r2 = cx.get(&a[2]).await?;
    let mut vals = Vec::new();
    for r in [r1, r2] {
        match r {
            Some(r) if r.t != KeyType::String => {
                return fail(Frame::err("The specified keys must contain string values"));
            }
            Some(r) => vals.push(r.v.unwrap_or_default()),
            None => vals.push(Vec::new()),
        }
    }
    let (x, y) = (&vals[0], &vals[1]);
    let (n, m) = (x.len(), y.len());
    let mut dp = vec![0u32; (n + 1) * (m + 1)];
    let at = |i: usize, j: usize| i * (m + 1) + j;
    for i in 1..=n {
        for j in 1..=m {
            dp[at(i, j)] = if x[i - 1] == y[j - 1] {
                dp[at(i - 1, j - 1)] + 1
            } else {
                dp[at(i - 1, j)].max(dp[at(i, j - 1)])
            };
        }
    }
    let total = dp[at(n, m)];
    if len_only {
        return Ok(Frame::Integer(i64::from(total)));
    }
    // Walk back, as Redis does, collecting the string and the ranges.
    let mut out = vec![0u8; total as usize];
    let mut matches = Vec::new();
    let (mut i, mut j, mut k) = (n, m, total as usize);
    let mut cur: Option<(usize, usize, usize, usize)> = None; // a_start,a_end,b_start,b_end
    let push = |c: Option<(usize, usize, usize, usize)>, ms: &mut Vec<_>| {
        if let Some((as_, ae, bs, be)) = c {
            let l = ae - as_ + 1;
            if i64::try_from(l).unwrap_or(0) >= minlen {
                ms.push((as_, ae, bs, be, l));
            }
        }
    };
    while i > 0 && j > 0 {
        if x[i - 1] == y[j - 1] {
            k -= 1;
            out[k] = x[i - 1];
            let (ai, bj) = (i - 1, j - 1);
            match cur {
                Some((as_, ae, bs, be)) if as_ == ai + 1 && bs == bj + 1 => {
                    cur = Some((ai, ae, bj, be));
                }
                c => {
                    push(c, &mut matches);
                    cur = Some((ai, ai, bj, bj));
                }
            }
            i -= 1;
            j -= 1;
        } else if dp[at(i - 1, j)] > dp[at(i, j - 1)] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    push(cur, &mut matches);
    if !idx {
        return Ok(Frame::Bulk(Bytes::from(out)));
    }
    let to_i = |u: usize| Frame::Integer(i64::try_from(u).unwrap_or(0));
    let list = matches
        .into_iter()
        .map(|(as_, ae, bs, be, l)| {
            let mut item = vec![
                Frame::Array(vec![to_i(as_), to_i(ae)]),
                Frame::Array(vec![to_i(bs), to_i(be)]),
            ];
            if withlen {
                item.push(to_i(l));
            }
            Frame::Array(item)
        })
        .collect();
    Ok(Frame::Map(vec![
        (Frame::bulk_from(b"matches"), Frame::Array(list)),
        (Frame::bulk_from(b"len"), Frame::Integer(i64::from(total))),
    ]))
}

/// One `BITFIELD` field type.
#[derive(Debug, Clone, Copy)]
struct Bf {
    signed: bool,
    bits: u32,
}

fn bf_type(a: &[u8]) -> R<Bf> {
    let bad = || {
        fail(Frame::err(
            "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
        ))
    };
    let Some((&first, rest)) = a.split_first() else {
        return bad();
    };
    let signed = match first {
        b'i' | b'I' => true,
        b'u' | b'U' => false,
        _ => return bad(),
    };
    let Some(bits) = parse_i64(rest).and_then(|b| u32::try_from(b).ok()) else {
        return bad();
    };
    if bits == 0 || (signed && bits > 64) || (!signed && bits > 63) {
        return bad();
    }
    Ok(Bf { signed, bits })
}

fn bf_offset(a: &[u8], t: Bf) -> R<u64> {
    let (mult, body) = match a.first() {
        Some(b'#') => (u64::from(t.bits), &a[1..]),
        _ => (1, a),
    };
    match parse_i64(body) {
        Some(n) if n >= 0 => {
            let off = n.unsigned_abs().saturating_mul(mult);
            if off + u64::from(t.bits) > (512 * 1024 * 1024 * 8) {
                return fail(Frame::err("bit offset is not an integer or out of range"));
            }
            Ok(off)
        }
        _ => fail(Frame::err("bit offset is not an integer or out of range")),
    }
}

fn bf_get(v: &[u8], off: u64, t: Bf) -> i64 {
    let mut u: u64 = 0;
    for i in 0..u64::from(t.bits) {
        let p = off + i;
        let byte = usize::try_from(p >> 3).unwrap_or(usize::MAX);
        let b = v.get(byte).map_or(0, |b| (b >> (7 - (p & 7))) & 1);
        u = (u << 1) | u64::from(b);
    }
    if t.signed && t.bits < 64 && (u >> (t.bits - 1)) & 1 == 1 {
        (u | (u64::MAX << t.bits)) as i64
    } else {
        u as i64
    }
}

fn bf_set(v: &mut Vec<u8>, off: u64, t: Bf, val: i64) {
    let need = usize::try_from((off + u64::from(t.bits)).div_ceil(8)).unwrap_or(0);
    if v.len() < need {
        v.resize(need, 0);
    }
    let u = val as u64;
    for i in 0..u64::from(t.bits) {
        let p = off + i;
        let bit = (u >> (u64::from(t.bits) - 1 - i)) & 1;
        let byte = usize::try_from(p >> 3).unwrap_or(0);
        let shift = 7 - (p & 7);
        v[byte] = (v[byte] & !(1 << shift)) | ((bit as u8) << shift);
    }
}

/// Overflow handling of `BITFIELD ... INCRBY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

fn bf_incr(old: i64, by: i64, t: Bf, ov: Overflow) -> Option<i64> {
    let (min, max): (i128, i128) = if t.signed {
        (-(1i128 << (t.bits - 1)), (1i128 << (t.bits - 1)) - 1)
    } else {
        (0, (1i128 << t.bits) - 1)
    };
    let sum = i128::from(old) + i128::from(by);
    if sum >= min && sum <= max {
        return i64::try_from(sum).ok();
    }
    match ov {
        Overflow::Fail => None,
        Overflow::Sat => i64::try_from(if sum > max { max } else { min }).ok(),
        Overflow::Wrap => {
            let m = 1i128 << t.bits;
            let mut w = (sum - min).rem_euclid(m) + min;
            if !t.signed {
                w = sum.rem_euclid(m);
            }
            i64::try_from(w).ok()
        }
    }
}

/// `BITFIELD k [GET t o] [SET t o v] [INCRBY t o n] [OVERFLOW ...]` (and
/// `BITFIELD_RO` with `GET` only).
pub async fn bitfield(cx: &mut Ctx<'_>, a: &[Bytes], ro: bool) -> R<Frame> {
    enum Op {
        Get(Bf, u64),
        Set(Bf, u64, i64, Overflow),
        Incr(Bf, u64, i64, Overflow),
    }
    let mut ops = Vec::new();
    let mut ov = Overflow::Wrap;
    let mut i = 2;
    let mut writes = false;
    while i < a.len() {
        let o = upper(&a[i]);
        let left = a.len() - i - 1;
        match o.as_str() {
            "GET" if left >= 2 => {
                let t = bf_type(&a[i + 1])?;
                ops.push(Op::Get(t, bf_offset(&a[i + 2], t)?));
                i += 3;
            }
            "SET" | "INCRBY" if left >= 3 => {
                if ro {
                    return fail(Frame::err("BITFIELD_RO only supports the GET subcommand"));
                }
                let t = bf_type(&a[i + 1])?;
                let off = bf_offset(&a[i + 2], t)?;
                let v = int_arg(&a[i + 3])?;
                writes = true;
                ops.push(if o == "SET" {
                    Op::Set(t, off, v, ov)
                } else {
                    Op::Incr(t, off, v, ov)
                });
                i += 4;
            }
            "OVERFLOW" if left >= 1 => {
                if ro {
                    return fail(Frame::err("BITFIELD_RO only supports the GET subcommand"));
                }
                ov = match upper(&a[i + 1]).as_str() {
                    "WRAP" => Overflow::Wrap,
                    "SAT" => Overflow::Sat,
                    "FAIL" => Overflow::Fail,
                    _ => return fail(Frame::err("Invalid OVERFLOW type specified")),
                };
                i += 2;
            }
            _ => return fail(reply::syntax()),
        }
    }
    let row = if writes {
        cx.lock_typed(&a[1], KeyType::String).await?
    } else {
        cx.get_typed(&a[1], KeyType::String).await?
    };
    let mut v = row.as_ref().and_then(|r| r.v.clone()).unwrap_or_default();
    let mut out = Vec::new();
    let mut changed = false;
    for op in ops {
        match op {
            Op::Get(t, off) => out.push(Frame::Integer(bf_get(&v, off, t))),
            Op::Set(t, off, val, ov) => {
                let old = bf_get(&v, off, t);
                // SET truncates as WRAP does; FAIL and SAT apply too.
                let nv = bf_incr(0, val, t, ov);
                match nv {
                    Some(nv) => {
                        bf_set(&mut v, off, t, nv);
                        changed = true;
                        out.push(Frame::Integer(old));
                    }
                    None => out.push(Frame::null()),
                }
            }
            Op::Incr(t, off, by, ov) => {
                let old = bf_get(&v, off, t);
                match bf_incr(old, by, t, ov) {
                    Some(nv) => {
                        bf_set(&mut v, off, t, nv);
                        changed = true;
                        out.push(Frame::Integer(nv));
                    }
                    None => out.push(Frame::null()),
                }
            }
        }
    }
    if changed {
        cx.check_value(&v)?;
        let exp = row.as_ref().and_then(|r| r.exp);
        put(cx, &a[1], row, v, exp).await?;
    }
    Ok(Frame::Array(out))
}
