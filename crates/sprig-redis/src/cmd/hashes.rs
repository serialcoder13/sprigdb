//! Hashes and sets: one element row per
//! field or member, `o = CRC32(e)`.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use sprig_kv::layout::{KeyType, live};
use sprig_kv::sql::{cell_u64, lit};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{ld2string_human, parse_f64, parse_i64};

use crate::cmd::keys::scan_opts;
use crate::ctx::{Ctx, KeyRow, R, crc, fail, int_arg, upper};
use crate::glob;
use crate::reply;

/// `(o = crc AND e = x) OR ...` for `items`.
fn elem_pred(items: &[&[u8]]) -> String {
    elem_pred_on("", items)
}

/// [`elem_pred`] on columns of `alias`.
fn elem_pred_on(alias: &str, items: &[&[u8]]) -> String {
    let p = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    items
        .iter()
        .map(|e| format!("({p}o = {} AND {p}e = {})", crc(e), lit(e)))
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// The stored values of `items` of `row` (fields or members).
pub(crate) async fn lookup(
    cx: &mut Ctx<'_>,
    row: &KeyRow,
    items: &[&[u8]],
) -> R<HashMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut out = HashMap::new();
    if items.is_empty() || !row.stored {
        return Ok(out);
    }
    for chunk in items.chunks(500) {
        let rows = cx
            .q(&format!(
                "SELECT e, v FROM {} WHERE {} AND ({})",
                cx.ns.elems(),
                cx.kg(row),
                elem_pred(chunk)
            ))
            .await?;
        for r in rows {
            let mut it = r.into_iter();
            if let (Some(Some(e)), Some(v)) = (it.next(), it.next()) {
                out.insert(e, v);
            }
        }
    }
    Ok(out)
}

/// Every `(e, v)` of `row`, read with the live predicate in one statement.
async fn all_live(
    cx: &mut Ctx<'_>,
    k: &[u8],
    want: KeyType,
) -> R<Option<Vec<(Vec<u8>, Option<Vec<u8>>)>>> {
    let rows = cx
        .q(&format!(
            "SELECT t.type, e.e, e.v FROM {} AS t LEFT JOIN {} AS e ON e.k = t.k AND e.g = t.g \
             AND e._sprig_dk = t._sprig_dk WHERE {} AND {} ORDER BY e.o, e.e",
            cx.ns.keys(),
            cx.ns.elems(),
            cx.kp(k, "t."),
            live("t")
        ))
        .await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    if first[0].as_deref().and_then(KeyType::parse) != Some(want) {
        return fail(reply::wrongtype());
    }
    Ok(Some(
        rows.into_iter()
            .filter_map(|r| {
                let mut it = r.into_iter().skip(1);
                match (it.next(), it.next()) {
                    (Some(Some(e)), Some(v)) => Some((e, v)),
                    _ => None,
                }
            })
            .collect(),
    ))
}

/// Upserts elements of `row` (`v` `None` for set members).
pub(crate) async fn upsert(
    cx: &mut Ctx<'_>,
    row: &KeyRow,
    items: &[(&[u8], Option<&[u8]>)],
) -> R<()> {
    let route = cx.route_vals(&row.k);
    for chunk in items.chunks(200) {
        let vals = chunk
            .iter()
            .map(|(e, v)| {
                format!(
                    "({}, {}, {}, {}, {}, {route})",
                    lit(&row.k),
                    row.g,
                    crc(e),
                    lit(e),
                    v.map_or_else(|| "NULL".to_owned(), lit)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        cx.x(&format!(
            "INSERT INTO {} (k, g, o, e, v, _sprig_dk, _sprig_rk) VALUES {vals} \
             ON DUPLICATE KEY UPDATE v = VALUES(v)",
            cx.ns.elems()
        ))
        .await?;
    }
    Ok(())
}

/// Deletes elements of `row`.
pub(crate) async fn remove(cx: &mut Ctx<'_>, row: &KeyRow, items: &[&[u8]]) -> R<()> {
    for chunk in items.chunks(500) {
        cx.x(&format!(
            "DELETE FROM {} WHERE {} AND ({})",
            cx.ns.elems(),
            cx.kg(row),
            elem_pred(chunk)
        ))
        .await?;
    }
    Ok(())
}

/// Every element of a locked or read `row`, in `o` order.
pub(crate) async fn elems_of(cx: &mut Ctx<'_>, row: &KeyRow) -> R<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    if !row.stored {
        return Ok(Vec::new());
    }
    let rows = cx
        .q(&format!(
            "SELECT e, v FROM {} WHERE {} ORDER BY o, e",
            cx.ns.elems(),
            cx.kg(row)
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let mut it = r.into_iter();
            match (it.next(), it.next()) {
                (Some(Some(e)), Some(v)) => Some((e, v)),
                _ => None,
            }
        })
        .collect())
}

fn dedup_last<'a>(pairs: &[(&'a [u8], &'a [u8])]) -> Vec<(&'a [u8], &'a [u8])> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for p in pairs.iter().rev() {
        if seen.insert(p.0) {
            out.push(*p);
        }
    }
    out.reverse();
    out
}

/// `HSET k f v ...` and `HMSET`.
pub async fn hset(cx: &mut Ctx<'_>, a: &[Bytes], hmset: bool) -> R<Frame> {
    if !a.len().is_multiple_of(2) {
        return fail(reply::arity(if hmset { "hmset" } else { "hset" }));
    }
    let pairs: Vec<(&[u8], &[u8])> = a[2..].chunks(2).map(|c| (&c[0][..], &c[1][..])).collect();
    for (f, v) in &pairs {
        Ctx::check_elem(f)?;
        cx.check_value(v)?;
    }
    let pairs = dedup_last(&pairs);
    let row = cx.lock_typed(&a[1], KeyType::Hash).await?;
    let mut row = row.unwrap_or_else(|| Ctx::new_row(&a[1], KeyType::Hash));
    let fields: Vec<&[u8]> = pairs.iter().map(|p| p.0).collect();
    let have = lookup(cx, &row, &fields).await?;
    let added = fields.iter().filter(|f| !have.contains_key(**f)).count() as i64;
    let items: Vec<(&[u8], Option<&[u8]>)> = pairs.iter().map(|(f, v)| (*f, Some(*v))).collect();
    upsert(cx, &row, &items).await?;
    row.n += added;
    cx.save(&mut row).await?;
    Ok(if hmset {
        Frame::ok()
    } else {
        Frame::Integer(added)
    })
}

/// `HSETNX k f v`.
pub async fn hsetnx(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    Ctx::check_elem(&a[2])?;
    cx.check_value(&a[3])?;
    let row = cx.lock_typed(&a[1], KeyType::Hash).await?;
    let mut row = row.unwrap_or_else(|| Ctx::new_row(&a[1], KeyType::Hash));
    if !lookup(cx, &row, &[&a[2]]).await?.is_empty() {
        return Ok(Frame::Integer(0));
    }
    upsert(cx, &row, &[(&a[2], Some(&a[3]))]).await?;
    row.n += 1;
    cx.save(&mut row).await?;
    Ok(Frame::Integer(1))
}

/// Reads fields of a live hash in one statement: `None` when the key is
/// absent.
async fn read_fields(
    cx: &mut Ctx<'_>,
    k: &[u8],
    want: KeyType,
    items: &[&[u8]],
) -> R<Option<HashMap<Vec<u8>, Option<Vec<u8>>>>> {
    let items: Vec<&[u8]> = items.iter().copied().filter(|e| e.len() <= 2024).collect();
    let cond = if items.is_empty() {
        "FALSE".to_owned()
    } else {
        elem_pred_on("e", &items)
    };
    let rows = cx
        .q(&format!(
            "SELECT t.type, e.e, e.v FROM {} AS t LEFT JOIN {} AS e ON e.k = t.k AND e.g = t.g \
             AND e._sprig_dk = t._sprig_dk AND ({}) WHERE {} AND {}",
            cx.ns.keys(),
            cx.ns.elems(),
            cond,
            cx.kp(k, "t."),
            live("t")
        ))
        .await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    if first[0].as_deref().and_then(KeyType::parse) != Some(want) {
        return fail(reply::wrongtype());
    }
    let mut out = HashMap::new();
    for r in rows {
        let mut it = r.into_iter().skip(1);
        if let (Some(Some(e)), Some(v)) = (it.next(), it.next()) {
            out.insert(e, v);
        }
    }
    Ok(Some(out))
}

/// `HGET k f`.
pub async fn hget(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let m = read_fields(cx, &a[1], KeyType::Hash, &[&a[2]]).await?;
    Ok(Frame::opt_bulk(
        m.and_then(|mut m| m.remove(&a[2][..]).flatten()),
    ))
}

/// `HMGET k f...`.
pub async fn hmget(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let fields: Vec<&[u8]> = a[2..].iter().map(|f| &f[..]).collect();
    let m = read_fields(cx, &a[1], KeyType::Hash, &fields)
        .await?
        .unwrap_or_default();
    Ok(Frame::Array(
        fields
            .iter()
            .map(|f| Frame::opt_bulk(m.get(*f).cloned().flatten()))
            .collect(),
    ))
}

/// `HGETALL`, `HKEYS`, `HVALS`.
pub async fn hgetall(cx: &mut Ctx<'_>, a: &[Bytes], what: u8) -> R<Frame> {
    let items = all_live(cx, &a[1], KeyType::Hash)
        .await?
        .unwrap_or_default();
    Ok(match what {
        b'k' => Frame::Array(
            items
                .into_iter()
                .map(|(e, _)| Frame::Bulk(e.into()))
                .collect(),
        ),
        b'v' => Frame::Array(
            items
                .into_iter()
                .map(|(_, v)| Frame::Bulk(v.unwrap_or_default().into()))
                .collect(),
        ),
        _ => Frame::Map(
            items
                .into_iter()
                .map(|(e, v)| {
                    (
                        Frame::Bulk(e.into()),
                        Frame::Bulk(v.unwrap_or_default().into()),
                    )
                })
                .collect(),
        ),
    })
}

/// `HLEN`, `SCARD`.
pub async fn card(cx: &mut Ctx<'_>, a: &[Bytes], t: KeyType) -> R<Frame> {
    let r = cx.get_typed(&a[1], t).await?;
    Ok(Frame::Integer(r.map_or(0, |r| r.n)))
}

/// `HEXISTS k f`, `SISMEMBER k m`.
pub async fn exists(cx: &mut Ctx<'_>, a: &[Bytes], t: KeyType) -> R<Frame> {
    let m = read_fields(cx, &a[1], t, &[&a[2]]).await?;
    Ok(Frame::Integer(i64::from(
        m.is_some_and(|m| m.contains_key(&a[2][..])),
    )))
}

/// `HDEL k f...`, `SREM k m...`.
pub async fn hdel(cx: &mut Ctx<'_>, a: &[Bytes], t: KeyType) -> R<Frame> {
    let Some(mut row) = cx.lock_typed(&a[1], t).await? else {
        return Ok(Frame::Integer(0));
    };
    let mut items: Vec<&[u8]> = a[2..].iter().map(|f| &f[..]).collect();
    items.sort_unstable();
    items.dedup();
    let have = lookup(cx, &row, &items).await?;
    let gone: Vec<&[u8]> = items
        .into_iter()
        .filter(|f| have.contains_key(*f))
        .collect();
    if gone.is_empty() {
        return Ok(Frame::Integer(0));
    }
    remove(cx, &row, &gone).await?;
    row.n -= gone.len() as i64;
    cx.save_or_delete(&mut row).await?;
    Ok(Frame::Integer(gone.len() as i64))
}

/// `HINCRBY k f n`.
pub async fn hincrby(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let by = int_arg(&a[3])?;
    Ctx::check_elem(&a[2])?;
    let row = cx.lock_typed(&a[1], KeyType::Hash).await?;
    let mut row = row.unwrap_or_else(|| Ctx::new_row(&a[1], KeyType::Hash));
    let have = lookup(cx, &row, &[&a[2]]).await?;
    let cur = match have.get(&a[2][..]) {
        None => 0,
        Some(v) => match v.as_deref().and_then(parse_i64) {
            Some(n) => n,
            None => return fail(Frame::err("hash value is not an integer")),
        },
    };
    let Some(n) = cur.checked_add(by) else {
        return fail(Frame::err("increment or decrement would overflow"));
    };
    let s = n.to_string();
    upsert(cx, &row, &[(&a[2], Some(s.as_bytes()))]).await?;
    if have.is_empty() {
        row.n += 1;
    }
    cx.save(&mut row).await?;
    Ok(Frame::Integer(n))
}

/// `HINCRBYFLOAT k f x`.
pub async fn hincrbyfloat(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(by) = parse_f64(&a[3]) else {
        return fail(reply::not_float());
    };
    Ctx::check_elem(&a[2])?;
    let row = cx.lock_typed(&a[1], KeyType::Hash).await?;
    let mut row = row.unwrap_or_else(|| Ctx::new_row(&a[1], KeyType::Hash));
    let have = lookup(cx, &row, &[&a[2]]).await?;
    let cur = match have.get(&a[2][..]) {
        None => 0.0,
        Some(v) => match v.as_deref().and_then(parse_f64) {
            Some(n) => n,
            None => return fail(Frame::err("hash value is not a float")),
        },
    };
    let n = cur + by;
    if n.is_nan() || n.is_infinite() {
        return fail(Frame::err("increment would produce NaN or Infinity"));
    }
    let s = ld2string_human(n);
    upsert(cx, &row, &[(&a[2], Some(s.as_bytes()))]).await?;
    if have.is_empty() {
        row.n += 1;
    }
    cx.save(&mut row).await?;
    Ok(Frame::bulk(s))
}

/// `HSTRLEN k f`.
pub async fn hstrlen(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let m = read_fields(cx, &a[1], KeyType::Hash, &[&a[2]]).await?;
    let n = m
        .and_then(|m| m.get(&a[2][..]).cloned().flatten())
        .map_or(0, |v| v.len());
    Ok(Frame::Integer(n as i64))
}

/// A random `u64`.
pub(crate) fn rand_u64() -> u64 {
    crate::ctx::new_version()
}

/// Picks for `HRANDFIELD`, `SRANDMEMBER`, `ZRANDMEMBER` and `SPOP`:
/// distinct when `count >= 0`, with repeats (exactly `|count|`) when
/// negative.
pub(crate) fn pick<T: Clone>(items: &[T], count: i64) -> Vec<T> {
    if items.is_empty() {
        return Vec::new();
    }
    let len = items.len() as u64;
    if count < 0 {
        let n = count.unsigned_abs();
        return (0..n)
            .map(|_| items[usize::try_from(rand_u64() % len).unwrap_or(0)].clone())
            .collect();
    }
    let n = usize::try_from(count)
        .unwrap_or(usize::MAX)
        .min(items.len());
    let mut idx: Vec<usize> = (0..items.len()).collect();
    // Partial Fisher-Yates.
    for i in 0..n {
        let j = i + usize::try_from(rand_u64() % (len - i as u64)).unwrap_or(0);
        idx.swap(i, j);
    }
    idx[..n].iter().map(|i| items[*i].clone()).collect()
}

/// `HRANDFIELD k [count [WITHVALUES]]`.
pub async fn hrandfield(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let count = a.get(2).map(|c| int_arg(c)).transpose()?;
    let withvalues = match a.get(3) {
        None => false,
        Some(o) if a.len() == 4 && upper(o) == "WITHVALUES" => true,
        Some(_) => return fail(reply::syntax()),
    };
    if let Some(c) = count
        && c.unsigned_abs() > (i64::MAX as u64) / 2
    {
        return fail(Frame::err("value is out of range"));
    }
    let items = all_live(cx, &a[1], KeyType::Hash).await?;
    let Some(count) = count else {
        return Ok(match items {
            None => Frame::null(),
            Some(items) => pick(&items, 1)
                .into_iter()
                .next()
                .map_or(Frame::null(), |(e, _)| Frame::Bulk(e.into())),
        });
    };
    let items = items.unwrap_or_default();
    let picked = pick(&items, count);
    Ok(Frame::Array(if withvalues {
        if cx.resp3 {
            picked
                .into_iter()
                .map(|(e, v)| {
                    Frame::Array(vec![
                        Frame::Bulk(e.into()),
                        Frame::Bulk(v.unwrap_or_default().into()),
                    ])
                })
                .collect()
        } else {
            picked
                .into_iter()
                .flat_map(|(e, v)| {
                    [
                        Frame::Bulk(e.into()),
                        Frame::Bulk(v.unwrap_or_default().into()),
                    ]
                })
                .collect()
        }
    } else {
        picked
            .into_iter()
            .map(|(e, _)| Frame::Bulk(e.into()))
            .collect()
    }))
}

/// `HSCAN` and `SSCAN`: the cursor is the next `o` (CRC32); keys of up to
/// `redis.scan_small_key_elements` elements come whole.
pub async fn escan(cx: &mut Ctx<'_>, a: &[Bytes], t: KeyType) -> R<Frame> {
    let Some(cursor) = std::str::from_utf8(&a[2])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return fail(Frame::err("invalid cursor"));
    };
    // NOVALUES (Redis 7.4) isn't 7.2's.
    let (pattern, count, _) = scan_opts(&a[3..], false)?;
    let Some(row) = cx.get_typed(&a[1], t).await? else {
        return Ok(Frame::Array(vec![
            Frame::bulk("0"),
            Frame::Array(Vec::new()),
        ]));
    };
    let small =
        usize::try_from(row.n).unwrap_or(usize::MAX) <= cx.env.config().scan_small_key_elements;
    let (items, next) = if small {
        (elems_of(cx, &row).await?, 0u64)
    } else {
        let rows = cx
            .q(&format!(
                "SELECT e, v, o FROM {} WHERE {} AND o >= {cursor} ORDER BY o, e LIMIT {count}",
                cx.ns.elems(),
                cx.kg(&row)
            ))
            .await?;
        let full = rows.len() == count;
        let mut items = Vec::new();
        let mut last = None;
        for r in rows {
            let o = cell_u64(r.get(2).and_then(Option::as_ref));
            let e = r.first().cloned().flatten().unwrap_or_default();
            last = Some((o, e.clone()));
            items.push((e, r.get(1).cloned().flatten()));
        }
        let next = match (last, full) {
            (Some((o, e)), true) => {
                let more = cx
                    .q(&format!(
                        "SELECT e, v FROM {} WHERE {} AND o = {o} AND e > {} ORDER BY e",
                        cx.ns.elems(),
                        cx.kg(&row),
                        lit(&e)
                    ))
                    .await?;
                for r in more {
                    let mut it = r.into_iter();
                    if let (Some(Some(e)), Some(v)) = (it.next(), it.next()) {
                        items.push((e, v));
                    }
                }
                o + 1
            }
            _ => 0,
        };
        (items, next)
    };
    let mut out = Vec::new();
    for (e, v) in items {
        if pattern.as_ref().is_some_and(|p| !glob::matches(p, &e)) {
            continue;
        }
        out.push(Frame::Bulk(e.into()));
        if t == KeyType::Hash {
            out.push(Frame::Bulk(v.unwrap_or_default().into()));
        }
    }
    Ok(Frame::Array(vec![
        Frame::bulk(next.to_string()),
        Frame::Array(out),
    ]))
}

// ---- sets ----

/// `SADD k m...`.
pub async fn sadd(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let mut items: Vec<&[u8]> = a[2..].iter().map(|m| &m[..]).collect();
    for m in &items {
        Ctx::check_elem(m)?;
    }
    items.sort_unstable();
    items.dedup();
    let row = cx.lock_typed(&a[1], KeyType::Set).await?;
    let mut row = row.unwrap_or_else(|| Ctx::new_row(&a[1], KeyType::Set));
    let have = lookup(cx, &row, &items).await?;
    let new: Vec<(&[u8], Option<&[u8]>)> = items
        .iter()
        .filter(|m| !have.contains_key(**m))
        .map(|m| (*m, None))
        .collect();
    if new.is_empty() {
        return Ok(Frame::Integer(0));
    }
    upsert(cx, &row, &new).await?;
    row.n += new.len() as i64;
    cx.save(&mut row).await?;
    Ok(Frame::Integer(new.len() as i64))
}

/// Members of a live set, read in one statement (`None`: absent).
pub(crate) async fn members(cx: &mut Ctx<'_>, k: &[u8]) -> R<Option<Vec<Vec<u8>>>> {
    Ok(all_live(cx, k, KeyType::Set)
        .await?
        .map(|v| v.into_iter().map(|(e, _)| e).collect()))
}

/// `SMEMBERS k`.
pub async fn smembers(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let m = members(cx, &a[1]).await?.unwrap_or_default();
    Ok(Frame::Set(
        m.into_iter().map(|e| Frame::Bulk(e.into())).collect(),
    ))
}

/// `SMISMEMBER k m...`.
pub async fn smismember(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let items: Vec<&[u8]> = a[2..].iter().map(|m| &m[..]).collect();
    let m = read_fields(cx, &a[1], KeyType::Set, &items)
        .await?
        .unwrap_or_default();
    Ok(Frame::Array(
        items
            .iter()
            .map(|e| Frame::Integer(i64::from(m.contains_key(*e))))
            .collect(),
    ))
}

/// `SPOP k [count]`.
pub async fn spop(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    if a.len() > 3 {
        return fail(reply::syntax());
    }
    let count = match a.get(2) {
        None => None,
        Some(c) => {
            let n = int_arg(c)?;
            if n < 0 {
                return fail(Frame::err("value is out of range, must be positive"));
            }
            Some(n)
        }
    };
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::Set).await? else {
        return Ok(if count.is_some() {
            Frame::Set(Vec::new())
        } else {
            Frame::null()
        });
    };
    let all: Vec<Vec<u8>> = elems_of(cx, &row)
        .await?
        .into_iter()
        .map(|(e, _)| e)
        .collect();
    let picked = pick(&all, count.unwrap_or(1));
    let gone: Vec<&[u8]> = picked.iter().map(Vec::as_slice).collect();
    if !gone.is_empty() {
        remove(cx, &row, &gone).await?;
        row.n -= gone.len() as i64;
        cx.save_or_delete(&mut row).await?;
    }
    Ok(match count {
        None => picked
            .into_iter()
            .next()
            .map_or(Frame::null(), |e| Frame::Bulk(e.into())),
        Some(_) => Frame::Set(picked.into_iter().map(|e| Frame::Bulk(e.into())).collect()),
    })
}

/// `SRANDMEMBER k [count]`.
pub async fn srandmember(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    if a.len() > 3 {
        return fail(reply::syntax());
    }
    let count = a.get(2).map(|c| int_arg(c)).transpose()?;
    let m = members(cx, &a[1]).await?;
    Ok(match count {
        None => m
            .and_then(|m| pick(&m, 1).into_iter().next())
            .map_or(Frame::null(), |e| Frame::Bulk(e.into())),
        Some(c) => Frame::Array(
            pick(&m.unwrap_or_default(), c)
                .into_iter()
                .map(|e| Frame::Bulk(e.into()))
                .collect(),
        ),
    })
}

/// `SMOVE src dst m`.
pub async fn smove(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    Ctx::check_elem(&a[3])?;
    let rows = cx.lock_many(&[&a[1][..], &a[2][..]]).await?;
    // Redis: a missing source answers 0 before any type check.
    let Some(mut src) = rows[0].clone() else {
        return Ok(Frame::Integer(0));
    };
    for r in rows.iter().flatten() {
        if r.t != KeyType::Set {
            return fail(reply::wrongtype());
        }
    }
    if lookup(cx, &src, &[&a[3]]).await?.is_empty() {
        return Ok(Frame::Integer(0));
    }
    if a[1] == a[2] {
        return Ok(Frame::Integer(1));
    }
    remove(cx, &src, &[&a[3]]).await?;
    src.n -= 1;
    cx.save_or_delete(&mut src).await?;
    let mut dst = rows[1]
        .clone()
        .unwrap_or_else(|| Ctx::new_row(&a[2], KeyType::Set));
    if lookup(cx, &dst, &[&a[3]]).await?.is_empty() {
        upsert(cx, &dst, &[(&a[3], None)]).await?;
        dst.n += 1;
    }
    cx.save(&mut dst).await?;
    Ok(Frame::Integer(1))
}

/// The set operation of `SINTER`, `SUNION`, `SDIFF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    /// `SINTER`.
    Inter,
    /// `SUNION`.
    Union,
    /// `SDIFF`.
    Diff,
}

/// Computes `op` over `keys` in Redis's key order (a missing key ends
/// `SINTER` at once; any other type is `WRONGTYPE`).
pub(crate) async fn set_algebra(
    cx: &mut Ctx<'_>,
    keys: &[Bytes],
    op: SetOp,
    lock: bool,
) -> R<Vec<Vec<u8>>> {
    let mut acc: Option<Vec<Vec<u8>>> = None;
    let mut empty = false;
    for (i, k) in keys.iter().enumerate() {
        let m = if lock {
            match cx.lock(k).await? {
                None => None,
                Some(r) if r.t != KeyType::Set => return fail(reply::wrongtype()),
                Some(r) => Some(
                    elems_of(cx, &r)
                        .await?
                        .into_iter()
                        .map(|(e, _)| e)
                        .collect(),
                ),
            }
        } else {
            members(cx, k).await?
        };
        match (op, m) {
            (SetOp::Inter, None) => {
                // Redis type-checks every key first, then answers empty.
                empty = true;
            }
            (SetOp::Inter, Some(m)) => {
                acc = Some(match acc {
                    None => m,
                    Some(a) => {
                        let s: HashSet<Vec<u8>> = m.into_iter().collect();
                        a.into_iter().filter(|e| s.contains(e)).collect()
                    }
                });
            }
            (SetOp::Union, m) => {
                let mut a = acc.unwrap_or_default();
                a.extend(m.unwrap_or_default());
                acc = Some(a);
            }
            (SetOp::Diff, m) => {
                if i == 0 {
                    acc = Some(m.unwrap_or_default());
                } else if let (Some(a), Some(m)) = (acc.take(), m) {
                    let s: HashSet<Vec<u8>> = m.into_iter().collect();
                    acc = Some(a.into_iter().filter(|e| !s.contains(e)).collect());
                } else {
                    acc = acc.or_else(|| Some(Vec::new()));
                }
            }
        }
    }
    if empty {
        return Ok(Vec::new());
    }
    let mut out = acc.unwrap_or_default();
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// `SINTER`, `SUNION`, `SDIFF`.
pub async fn salgebra(cx: &mut Ctx<'_>, a: &[Bytes], op: SetOp) -> R<Frame> {
    let out = set_algebra(cx, &a[1..], op, false).await?;
    Ok(Frame::Set(
        out.into_iter().map(|e| Frame::Bulk(e.into())).collect(),
    ))
}

/// Replaces `dst` with a set of `members` (deleting it when empty).
pub(crate) async fn store_set(
    cx: &mut Ctx<'_>,
    dst: &[u8],
    old: Option<KeyRow>,
    members: &[Vec<u8>],
) -> R<()> {
    if let Some(o) = old {
        cx.delete(&o).await?;
    }
    if members.is_empty() {
        return Ok(());
    }
    let mut row = Ctx::new_row(dst, KeyType::Set);
    let items: Vec<(&[u8], Option<&[u8]>)> = members.iter().map(|m| (m.as_slice(), None)).collect();
    upsert(cx, &row, &items).await?;
    row.n = members.len() as i64;
    cx.save(&mut row).await?;
    Ok(())
}

/// `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`.
pub async fn sstore(cx: &mut Ctx<'_>, a: &[Bytes], op: SetOp) -> R<Frame> {
    let out = set_algebra(cx, &a[2..], op, true).await?;
    let old = cx.lock(&a[1]).await?;
    store_set(cx, &a[1], old, &out).await?;
    Ok(Frame::Integer(out.len() as i64))
}

/// `SINTERCARD numkeys k... [LIMIT n]`.
pub async fn sintercard(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (keys, limit) = numkeys_limit(a)?;
    let out = set_algebra(cx, keys, SetOp::Inter, false).await?;
    let n = out.len() as i64;
    Ok(Frame::Integer(if limit > 0 { n.min(limit) } else { n }))
}

/// `numkeys k... [LIMIT n]` of `SINTERCARD` and `ZINTERCARD`.
pub(crate) fn numkeys_limit(a: &[Bytes]) -> R<(&[Bytes], i64)> {
    let n = match parse_i64(&a[1]) {
        Some(n) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
        _ => return fail(Frame::err("numkeys should be greater than 0")),
    };
    if n > a.len() - 2 {
        return fail(Frame::err(
            "Number of keys can't be greater than number of args",
        ));
    }
    let keys = &a[2..2 + n];
    let rest = &a[2 + n..];
    let mut limit = 0;
    let mut i = 0;
    while i < rest.len() {
        if upper(&rest[i]) == "LIMIT" && i + 1 < rest.len() {
            limit = match parse_i64(&rest[i + 1]) {
                Some(l) if l >= 0 => l,
                _ => return fail(Frame::err("LIMIT can't be negative")),
            };
            i += 2;
        } else {
            return fail(reply::syntax());
        }
    }
    Ok((keys, limit))
}
