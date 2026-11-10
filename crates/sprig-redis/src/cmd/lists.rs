//! Lists (chapter 40, "Lists: positions"): items fill `[lh, lt)` without
//! gaps, so pushes and pops at either end are O(1) and never renumber.

use bytes::Bytes;
use sprig_kv::layout::{KeyType, live};
use sprig_kv::sql::{cell_i64, lit};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_i64;

use crate::ctx::{Ctx, KeyRow, R, fail, int_arg, upper};
use crate::reply;

/// Which end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// Head.
    Left,
    /// Tail.
    Right,
}

impl End {
    fn parse(a: &[u8]) -> Option<Self> {
        match a.to_ascii_uppercase().as_slice() {
            b"LEFT" => Some(Self::Left),
            b"RIGHT" => Some(Self::Right),
            _ => None,
        }
    }
}

/// Inserts `items` at `end` of `row` in order (LPUSH a b puts b first).
pub(crate) async fn push(cx: &mut Ctx<'_>, row: &mut KeyRow, items: &[&[u8]], end: End) -> R<()> {
    let route = cx.route_vals(&row.k);
    let mut vals = Vec::with_capacity(items.len());
    for it in items {
        let o = match end {
            End::Left => {
                row.lh -= 1;
                row.lh
            }
            End::Right => {
                row.lt += 1;
                row.lt - 1
            }
        };
        vals.push(format!(
            "({}, {}, {o}, '', {}, {route})",
            lit(&row.k),
            row.g,
            lit(it)
        ));
    }
    for chunk in vals.chunks(200) {
        cx.x(&format!(
            "INSERT INTO {} (k, g, o, e, v, _sprig_dk, _sprig_rk) VALUES {}",
            cx.ns.elems(),
            chunk.join(", ")
        ))
        .await?;
    }
    row.n += items.len() as i64;
    cx.pushed_to(&row.k.clone());
    Ok(())
}

/// Removes up to `count` items from `end` of `row`, returning them.
pub(crate) async fn pop(
    cx: &mut Ctx<'_>,
    row: &mut KeyRow,
    count: i64,
    end: End,
) -> R<Vec<Vec<u8>>> {
    let c = count.min(row.n).max(0);
    if c == 0 {
        return Ok(Vec::new());
    }
    let (lo, hi, order) = match end {
        End::Left => (row.lh, row.lh + c, "ASC"),
        End::Right => (row.lt - c, row.lt, "DESC"),
    };
    let rows = cx
        .q(&format!(
            "SELECT v FROM {} WHERE {} AND o >= {lo} AND o < {hi} ORDER BY o {order}",
            cx.ns.elems(),
            cx.kg(row)
        ))
        .await?;
    cx.x(&format!(
        "DELETE FROM {} WHERE {} AND o >= {lo} AND o < {hi}",
        cx.ns.elems(),
        cx.kg(row)
    ))
    .await?;
    match end {
        End::Left => row.lh += c,
        End::Right => row.lt -= c,
    }
    row.n -= c;
    Ok(rows
        .into_iter()
        .map(|r| r.into_iter().next().flatten().unwrap_or_default())
        .collect())
}

/// `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`.
pub async fn lpush(cx: &mut Ctx<'_>, a: &[Bytes], end: End, only_existing: bool) -> R<Frame> {
    for v in &a[2..] {
        cx.check_value(v)?;
    }
    let row = cx.lock_typed(&a[1], KeyType::List).await?;
    let mut row = match row {
        Some(r) => r,
        None if only_existing => return Ok(Frame::Integer(0)),
        None => Ctx::new_row(&a[1], KeyType::List),
    };
    let items: Vec<&[u8]> = a[2..].iter().map(|v| &v[..]).collect();
    push(cx, &mut row, &items, end).await?;
    cx.save(&mut row).await?;
    Ok(Frame::Integer(row.n))
}

/// `LPOP k [count]`, `RPOP k [count]`.
pub async fn lpop(cx: &mut Ctx<'_>, a: &[Bytes], end: End) -> R<Frame> {
    if a.len() > 3 {
        return fail(reply::syntax());
    }
    let count = match a.get(2) {
        None => None,
        Some(c) => match parse_i64(c) {
            Some(n) if n >= 0 => Some(n),
            _ => return fail(Frame::err("value is out of range, must be positive")),
        },
    };
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::List).await? else {
        return Ok(if count.is_some() {
            Frame::null_array()
        } else {
            Frame::null()
        });
    };
    let items = pop(cx, &mut row, count.unwrap_or(1), end).await?;
    if !items.is_empty() {
        cx.save_or_delete(&mut row).await?;
    }
    Ok(match count {
        None => items
            .into_iter()
            .next()
            .map_or(Frame::null(), |v| Frame::Bulk(v.into())),
        Some(_) => Frame::Array(items.into_iter().map(|v| Frame::Bulk(v.into())).collect()),
    })
}

/// `LLEN k`.
pub async fn llen(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let r = cx.get_typed(&a[1], KeyType::List).await?;
    Ok(Frame::Integer(r.map_or(0, |r| r.n)))
}

/// Redis's `LRANGE` index rule over `n` items: `None` for an empty range.
pub(crate) fn index_range(n: i64, mut s: i64, mut e: i64) -> Option<(i64, i64)> {
    if s < 0 {
        s += n;
    }
    if e < 0 {
        e += n;
    }
    if s < 0 {
        s = 0;
    }
    if s > e || s >= n {
        return None;
    }
    if e >= n {
        e = n - 1;
    }
    Some((s, e))
}

/// The items of a live list at indexes `[s, e]`, with its key row, in one
/// statement.
async fn range_live(cx: &mut Ctx<'_>, k: &[u8], s: i64, e: i64) -> R<Vec<Vec<u8>>> {
    // Negative indexes need n: read the key row in the same statement.
    let rows = cx
        .q(&format!(
            "SELECT t.type, t.n, x.v FROM {keys} AS t LEFT JOIN {el} AS x ON x.k = t.k AND x.g = t.g \
             AND x._sprig_dk = t._sprig_dk \
             AND x.o >= t.lh + (CASE WHEN {s} < 0 THEN GREATEST(CAST(t.n AS SIGNED) + {s}, 0) ELSE {s} END) \
             AND x.o <= t.lh + (CASE WHEN {e} < 0 THEN CAST(t.n AS SIGNED) + {e} ELSE {e} END) \
             WHERE {k} AND {live} ORDER BY x.o",
            keys = cx.ns.keys(),
            el = cx.ns.elems(),
            k = cx.kp(k, "t."),
            live = live("t")
        ))
        .await?;
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    if first[0].as_deref().and_then(KeyType::parse) != Some(KeyType::List) {
        return fail(reply::wrongtype());
    }
    Ok(rows
        .into_iter()
        .filter_map(|r| r.into_iter().nth(2).flatten())
        .collect())
}

/// `LRANGE k start stop`.
pub async fn lrange(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (s, e) = (int_arg(&a[2])?, int_arg(&a[3])?);
    // Clamp before SQL so the arithmetic can't overflow.
    let clamp = |x: i64| x.clamp(-(1i64 << 62), 1i64 << 62);
    let items = range_live(cx, &a[1], clamp(s), clamp(e)).await?;
    Ok(Frame::Array(
        items.into_iter().map(|v| Frame::Bulk(v.into())).collect(),
    ))
}

/// `LINDEX k i`.
pub async fn lindex(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let i = int_arg(&a[2])?;
    let Some(row) = cx.get_typed(&a[1], KeyType::List).await? else {
        return Ok(Frame::null());
    };
    let i = if i < 0 { i + row.n } else { i };
    if i < 0 || i >= row.n {
        return Ok(Frame::null());
    }
    let rows = cx
        .q(&format!(
            "SELECT v FROM {} WHERE {} AND o = {}",
            cx.ns.elems(),
            cx.kg(&row),
            row.lh + i
        ))
        .await?;
    Ok(Frame::opt_bulk(
        rows.into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten()),
    ))
}

/// `LSET k i v`.
pub async fn lset(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let i = int_arg(&a[2])?;
    cx.check_value(&a[3])?;
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::List).await? else {
        return fail(reply::no_such_key());
    };
    let i = if i < 0 { i + row.n } else { i };
    if i < 0 || i >= row.n {
        return fail(Frame::err("index out of range"));
    }
    cx.x(&format!(
        "UPDATE {} SET v = {} WHERE {} AND o = {}",
        cx.ns.elems(),
        lit(&a[3]),
        cx.kg(&row),
        row.lh + i
    ))
    .await?;
    cx.save(&mut row).await?;
    Ok(Frame::ok())
}

/// `LINSERT k BEFORE|AFTER pivot v`: shifts the shorter side by one.
pub async fn linsert(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let before = match upper(&a[2]).as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return fail(reply::syntax()),
    };
    cx.check_value(&a[4])?;
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::List).await? else {
        return Ok(Frame::Integer(0));
    };
    let rows = cx
        .q(&format!(
            "SELECT o FROM {} WHERE {} AND v = {} ORDER BY o LIMIT 1",
            cx.ns.elems(),
            cx.kg(&row),
            lit(&a[3])
        ))
        .await?;
    let Some(p) = rows.first().map(|r| cell_i64(r[0].as_ref())) else {
        return Ok(Frame::Integer(-1));
    };
    // The new item goes between `p - 1` and `p` (BEFORE) or `p` and `p + 1`.
    let split = if before { p } else { p + 1 };
    let left = split - row.lh;
    let right = row.lt - split;
    let at = if left < right {
        cx.x(&format!(
            "UPDATE {} SET o = o - 1 WHERE {} AND o < {split} ORDER BY o ASC",
            cx.ns.elems(),
            cx.kg(&row)
        ))
        .await?;
        row.lh -= 1;
        split - 1
    } else {
        cx.x(&format!(
            "UPDATE {} SET o = o + 1 WHERE {} AND o >= {split} ORDER BY o DESC",
            cx.ns.elems(),
            cx.kg(&row)
        ))
        .await?;
        row.lt += 1;
        split
    };
    cx.x(&format!(
        "INSERT INTO {} (k, g, o, e, v, _sprig_dk, _sprig_rk) VALUES ({}, {}, {at}, '', {}, {})",
        cx.ns.elems(),
        lit(&row.k),
        row.g,
        lit(&a[4]),
        cx.route_vals(&row.k)
    ))
    .await?;
    row.n += 1;
    cx.save(&mut row).await?;
    Ok(Frame::Integer(row.n))
}

/// Rewrites positions `>= from` so the kept items `(o, v)` (in order)
/// fill `[from, from + kept)`.
async fn rewrite_tail(cx: &mut Ctx<'_>, row: &mut KeyRow, from: i64, kept: &[Vec<u8>]) -> R<()> {
    cx.x(&format!(
        "DELETE FROM {} WHERE {} AND o >= {from}",
        cx.ns.elems(),
        cx.kg(row)
    ))
    .await?;
    let route = cx.route_vals(&row.k);
    let vals: Vec<String> = kept
        .iter()
        .enumerate()
        .map(|(i, v)| {
            format!(
                "({}, {}, {}, '', {}, {route})",
                lit(&row.k),
                row.g,
                from + i as i64,
                lit(v)
            )
        })
        .collect();
    for chunk in vals.chunks(200) {
        cx.x(&format!(
            "INSERT INTO {} (k, g, o, e, v, _sprig_dk, _sprig_rk) VALUES {}",
            cx.ns.elems(),
            chunk.join(", ")
        ))
        .await?;
    }
    row.lt = from + kept.len() as i64;
    Ok(())
}

/// `LREM k count v`.
pub async fn lrem(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let count = int_arg(&a[2])?;
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::List).await? else {
        return Ok(Frame::Integer(0));
    };
    let (order, limit) = match count {
        0 => ("ASC", String::new()),
        c if c > 0 => ("ASC", format!(" LIMIT {c}")),
        c => ("DESC", format!(" LIMIT {}", c.unsigned_abs())),
    };
    let rows = cx
        .q(&format!(
            "SELECT o FROM {} WHERE {} AND v = {} ORDER BY o {order}{limit}",
            cx.ns.elems(),
            cx.kg(&row),
            lit(&a[3])
        ))
        .await?;
    let mut gone: Vec<i64> = rows.iter().map(|r| cell_i64(r[0].as_ref())).collect();
    if gone.is_empty() {
        return Ok(Frame::Integer(0));
    }
    gone.sort_unstable();
    let first = gone[0];
    let tail = cx
        .q(&format!(
            "SELECT o, v FROM {} WHERE {} AND o >= {first} ORDER BY o",
            cx.ns.elems(),
            cx.kg(&row)
        ))
        .await?;
    let kept: Vec<Vec<u8>> = tail
        .into_iter()
        .filter(|r| gone.binary_search(&cell_i64(r[0].as_ref())).is_err())
        .map(|r| r.into_iter().nth(1).flatten().unwrap_or_default())
        .collect();
    rewrite_tail(cx, &mut row, first, &kept).await?;
    row.n -= gone.len() as i64;
    cx.save_or_delete(&mut row).await?;
    Ok(Frame::Integer(gone.len() as i64))
}

/// `LTRIM k start stop`.
pub async fn ltrim(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (s, e) = (int_arg(&a[2])?, int_arg(&a[3])?);
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::List).await? else {
        return Ok(Frame::ok());
    };
    match index_range(row.n, s, e) {
        None => {
            cx.delete(&row).await?;
        }
        Some((s, e)) => {
            let (lo, hi) = (row.lh + s, row.lh + e);
            cx.x(&format!(
                "DELETE FROM {} WHERE {} AND (o < {lo} OR o > {hi})",
                cx.ns.elems(),
                cx.kg(&row)
            ))
            .await?;
            row.lh = lo;
            row.lt = hi + 1;
            row.n = hi - lo + 1;
            cx.save(&mut row).await?;
        }
    }
    Ok(Frame::ok())
}

/// `LPOS k v [RANK r] [COUNT n] [MAXLEN m]`.
pub async fn lpos(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (mut rank, mut count, mut maxlen) = (1i64, None, 0i64);
    let mut i = 3;
    while i < a.len() {
        let o = upper(&a[i]);
        let Some(v) = a.get(i + 1) else {
            return fail(reply::syntax());
        };
        match o.as_str() {
            "RANK" => {
                rank = int_arg(v)?;
                if rank == 0 {
                    return fail(Frame::err(
                        "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                    ));
                }
                if rank == i64::MIN {
                    return fail(Frame::err("value is out of range"));
                }
            }
            "COUNT" => {
                let c = int_arg(v)?;
                if c < 0 {
                    return fail(Frame::err("COUNT can't be negative"));
                }
                count = Some(c);
            }
            "MAXLEN" => {
                maxlen = int_arg(v)?;
                if maxlen < 0 {
                    return fail(Frame::err("MAXLEN can't be negative"));
                }
            }
            _ => return fail(reply::syntax()),
        }
        i += 2;
    }
    let Some(row) = cx.get_typed(&a[1], KeyType::List).await? else {
        return Ok(if count.is_some() {
            Frame::Array(Vec::new())
        } else {
            Frame::null()
        });
    };
    let (order, window) = if rank > 0 {
        (
            "ASC",
            if maxlen > 0 {
                format!(" AND o < {}", row.lh + maxlen)
            } else {
                String::new()
            },
        )
    } else {
        (
            "DESC",
            if maxlen > 0 {
                format!(" AND o >= {}", row.lt - maxlen)
            } else {
                String::new()
            },
        )
    };
    let skip = rank.unsigned_abs() - 1;
    let limit = match count {
        Some(0) => String::new(),
        Some(c) => format!(" LIMIT {skip}, {c}"),
        None => format!(" LIMIT {skip}, 1"),
    };
    let rows = cx
        .q(&format!(
            "SELECT o FROM {} WHERE {} AND v = {}{window} ORDER BY o {order}{limit}",
            cx.ns.elems(),
            cx.kg(&row),
            lit(&a[2])
        ))
        .await?;
    let mut idx: Vec<i64> = rows
        .iter()
        .map(|r| cell_i64(r[0].as_ref()) - row.lh)
        .collect();
    if count == Some(0) {
        idx = idx
            .into_iter()
            .skip(usize::try_from(skip).unwrap_or(usize::MAX))
            .collect();
    }
    Ok(match count {
        None => idx.first().map_or(Frame::null(), |i| Frame::Integer(*i)),
        Some(_) => Frame::Array(idx.into_iter().map(Frame::Integer).collect()),
    })
}

/// One `LMOVE` step on locked rows; `None` when the source is empty.
async fn lmove_rows(
    cx: &mut Ctx<'_>,
    a1: &[u8],
    a2: &[u8],
    from: End,
    to: End,
) -> R<Option<Vec<u8>>> {
    let rows = cx.lock_many(&[a1, a2]).await?;
    // Redis: a missing source answers nil before the destination's check.
    let Some(mut src) = rows[0].clone() else {
        return Ok(None);
    };
    for r in rows.iter().flatten() {
        if r.t != KeyType::List {
            return fail(reply::wrongtype());
        }
    }
    let item = pop(cx, &mut src, 1, from)
        .await?
        .into_iter()
        .next()
        .unwrap_or_default();
    if a1 == a2 {
        push(cx, &mut src, &[&item], to).await?;
        cx.save(&mut src).await?;
        return Ok(Some(item));
    }
    cx.save_or_delete(&mut src).await?;
    let mut dst = rows[1]
        .clone()
        .unwrap_or_else(|| Ctx::new_row(a2, KeyType::List));
    push(cx, &mut dst, &[&item], to).await?;
    cx.save(&mut dst).await?;
    Ok(Some(item))
}

/// `LMOVE src dst LEFT|RIGHT LEFT|RIGHT` and `RPOPLPUSH`.
pub async fn lmove(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (Some(from), Some(to)) = (End::parse(&a[3]), End::parse(&a[4])) else {
        return fail(reply::syntax());
    };
    Ok(Frame::opt_bulk(
        lmove_rows(cx, &a[1], &a[2], from, to).await?,
    ))
}

/// `RPOPLPUSH src dst`.
pub async fn rpoplpush(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    Ok(Frame::opt_bulk(
        lmove_rows(cx, &a[1], &a[2], End::Right, End::Left).await?,
    ))
}

/// Tries `BLMOVE` once: `None` when the source is empty.
pub async fn try_blmove(cx: &mut Ctx<'_>, a: &[Bytes], from: End, to: End) -> R<Option<Frame>> {
    Ok(lmove_rows(cx, &a[1], &a[2], from, to)
        .await?
        .map(|v| Frame::Bulk(v.into())))
}

/// `numkeys k... LEFT|RIGHT [COUNT n]` of `LMPOP` (`at` = index of numkeys).
pub fn mpop_args(a: &[Bytes], at: usize) -> R<(Vec<Bytes>, End, i64)> {
    let n = match parse_i64(&a[at]) {
        Some(n) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
        _ => return fail(Frame::err("numkeys should be greater than 0")),
    };
    if n >= a.len() - at {
        return fail(reply::syntax());
    }
    let keys = a[at + 1..at + 1 + n].to_vec();
    let rest = &a[at + 1 + n..];
    let Some(end) = rest.first().and_then(|e| End::parse(e)) else {
        return fail(reply::syntax());
    };
    let mut count = 1;
    match rest.len() {
        1 => {}
        3 if upper(&rest[1]) == "COUNT" => {
            count = match parse_i64(&rest[2]) {
                Some(c) if c > 0 => c,
                _ => return fail(Frame::err("count should be greater than 0")),
            };
        }
        _ => return fail(reply::syntax()),
    }
    Ok((keys, end, count))
}

/// Pops from the first non-empty list of `keys`: `[key, [items]]` (or, for
/// `BLPOP`, `[key, item]` when `single`).
pub async fn try_mpop(
    cx: &mut Ctx<'_>,
    keys: &[Bytes],
    end: End,
    count: i64,
    single: bool,
) -> R<Option<Frame>> {
    for k in keys {
        let Some(mut row) = cx.lock_typed(k, KeyType::List).await? else {
            continue;
        };
        let items = pop(cx, &mut row, count, end).await?;
        cx.save_or_delete(&mut row).await?;
        let key = Frame::Bulk(k.clone());
        return Ok(Some(if single {
            Frame::Array(vec![
                key,
                Frame::Bulk(items.into_iter().next().unwrap_or_default().into()),
            ])
        } else {
            Frame::Array(vec![
                key,
                Frame::Array(items.into_iter().map(|v| Frame::Bulk(v.into())).collect()),
            ])
        }));
    }
    Ok(None)
}

/// `LMPOP numkeys k... LEFT|RIGHT [COUNT n]`.
pub async fn lmpop(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (keys, end, count) = mpop_args(a, 1)?;
    Ok(try_mpop(cx, &keys, end, count, false)
        .await?
        .unwrap_or_else(Frame::null_array))
}

/// Parses `End` for the session's blocking commands.
#[must_use]
pub fn end_of(a: &[u8]) -> Option<End> {
    End::parse(a)
}

/// Every item of a live list (for `SORT`).
pub(crate) async fn all_items(cx: &mut Ctx<'_>, row: &KeyRow) -> R<Vec<Vec<u8>>> {
    let rows = cx
        .q(&format!(
            "SELECT v FROM {} WHERE {} ORDER BY o",
            cx.ns.elems(),
            cx.kg(row)
        ))
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_iter().next().flatten().unwrap_or_default())
        .collect())
}

/// Replaces `dst` with a list of `items`.
pub(crate) async fn store_list(
    cx: &mut Ctx<'_>,
    dst: &[u8],
    old: Option<KeyRow>,
    items: &[Vec<u8>],
) -> R<()> {
    if let Some(o) = old {
        cx.delete(&o).await?;
    }
    if items.is_empty() {
        return Ok(());
    }
    let mut row = Ctx::new_row(dst, KeyType::List);
    let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
    push(cx, &mut row, &refs, End::Right).await?;
    cx.save(&mut row).await?;
    Ok(())
}
