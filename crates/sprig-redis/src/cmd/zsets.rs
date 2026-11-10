//! Sorted sets (chapter 40, "Sorted sets"): a score is stored as
//! `(c, score)`, `c` being -1, 0 or 1 for `-inf`, finite and `+inf`, so
//! `ORDER BY c, score, m` is Redis's order.

use std::collections::HashMap;

use bytes::Bytes;
use sprig_kv::layout::KeyType;
use sprig_kv::sql::{cell_f64, cell_i64, cell_u64, lit};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{d2string, parse_f64, parse_i64};

use crate::cmd::hashes::{members, pick};
use crate::cmd::keys::scan_opts;
use crate::cmd::lists::index_range;
use crate::ctx::{Ctx, KeyRow, R, crc, fail, int_arg, upper};
use crate::glob;
use crate::reply;

/// Members with their scores.
type Members = Vec<(Vec<u8>, f64)>;

/// `(c, score)` of a score.
fn cs(s: f64) -> (i8, String) {
    if s == f64::NEG_INFINITY {
        (-1, "0".into())
    } else if s == f64::INFINITY {
        (1, "0".into())
    } else {
        (0, format!("{s:e}"))
    }
}

/// The score of stored `(c, score)`.
fn score_of(c: i64, s: f64) -> f64 {
    match c {
        -1 => f64::NEG_INFINITY,
        1 => f64::INFINITY,
        _ => s,
    }
}

/// A score reply: a double in RESP3, a bulk string in RESP2.
fn score_frame(s: f64) -> Frame {
    Frame::Double(s)
}

/// `(o = crc AND m = x) OR ...`.
fn mem_pred(items: &[&[u8]]) -> String {
    items
        .iter()
        .map(|m| format!("(o = {} AND m = {})", crc(m), lit(m)))
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Scores of `items` in `row`.
async fn zlookup(cx: &mut Ctx<'_>, row: &KeyRow, items: &[&[u8]]) -> R<HashMap<Vec<u8>, f64>> {
    let mut out = HashMap::new();
    if !row.stored || items.is_empty() {
        return Ok(out);
    }
    let items: Vec<&[u8]> = items.iter().copied().filter(|m| m.len() <= 2024).collect();
    for chunk in items.chunks(500) {
        let rows = cx
            .q(&format!(
                "SELECT m, c, score FROM {} WHERE {} AND ({})",
                cx.ns.zset(),
                Ctx::kg(row),
                mem_pred(chunk)
            ))
            .await?;
        for r in rows {
            let s = score_of(cell_i64(r[1].as_ref()), cell_f64(r[2].as_ref()));
            if let Some(Some(m)) = r.into_iter().next() {
                out.insert(m, s);
            }
        }
    }
    Ok(out)
}

/// Upserts `(member, score)` pairs.
async fn zupsert(cx: &mut Ctx<'_>, row: &KeyRow, items: &[(Vec<u8>, f64)]) -> R<()> {
    let route = cx.route_vals(&row.k);
    for chunk in items.chunks(200) {
        let vals = chunk
            .iter()
            .map(|(m, s)| {
                let (c, sv) = cs(*s);
                format!(
                    "({}, {}, {}, {}, {c}, {sv}, {route})",
                    lit(&row.k),
                    row.g,
                    crc(m),
                    lit(m)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        cx.x(&format!(
            "INSERT INTO {} (k, g, o, m, c, score, _sprig_dk, _sprig_rk) VALUES {vals} \
             ON DUPLICATE KEY UPDATE c = VALUES(c), score = VALUES(score)",
            cx.ns.zset()
        ))
        .await?;
    }
    Ok(())
}

/// Deletes members.
async fn zremove(cx: &mut Ctx<'_>, row: &KeyRow, items: &[&[u8]]) -> R<()> {
    for chunk in items.chunks(500) {
        cx.x(&format!(
            "DELETE FROM {} WHERE {} AND ({})",
            cx.ns.zset(),
            Ctx::kg(row),
            mem_pred(chunk)
        ))
        .await?;
    }
    Ok(())
}

/// Members and scores of `row` where `cond`, in order (`rev` reverses),
/// with SQL `LIMIT`.
async fn zselect(
    cx: &mut Ctx<'_>,
    row: &KeyRow,
    cond: &str,
    rev: bool,
    limit: &str,
) -> R<Vec<(Vec<u8>, f64)>> {
    let order = if rev {
        "c DESC, score DESC, m DESC"
    } else {
        "c, score, m"
    };
    let rows = cx
        .q(&format!(
            "SELECT m, c, score FROM {} WHERE {} AND ({cond}) ORDER BY {order}{limit}",
            cx.ns.zset(),
            Ctx::kg(row)
        ))
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let s = score_of(cell_i64(r[1].as_ref()), cell_f64(r[2].as_ref()));
            (r.into_iter().next().flatten().unwrap_or_default(), s)
        })
        .collect())
}

/// One end of a score range.
#[derive(Debug, Clone, Copy)]
struct ScoreBound {
    v: f64,
    excl: bool,
}

fn parse_score_bound(a: &[u8]) -> Option<ScoreBound> {
    let (excl, body) = match a.first() {
        Some(b'(') => (true, &a[1..]),
        _ => (false, a),
    };
    parse_f64(body).map(|v| ScoreBound { v, excl })
}

/// `(c, score) > / >= bound` (or `<`, `<=` for an upper bound).
fn score_cmp(b: ScoreBound, lower: bool) -> String {
    let (c, s) = cs(b.v);
    let op = match (lower, b.excl) {
        (true, false) => ">=",
        (true, true) => ">",
        (false, false) => "<=",
        (false, true) => "<",
    };
    let strict = &op[..1];
    format!("(c {strict} {c} OR (c = {c} AND score {op} {s}))")
}

fn score_range(min: ScoreBound, max: ScoreBound) -> String {
    format!("{} AND {}", score_cmp(min, true), score_cmp(max, false))
}

fn parse_score_range(min: &[u8], max: &[u8]) -> R<(ScoreBound, ScoreBound)> {
    match (parse_score_bound(min), parse_score_bound(max)) {
        (Some(a), Some(b)) => Ok((a, b)),
        _ => fail(Frame::err("min or max is not a float")),
    }
}

/// One end of a lex range.
#[derive(Debug, Clone)]
enum LexBound {
    Min,
    Max,
    Incl(Vec<u8>),
    Excl(Vec<u8>),
}

fn parse_lex_bound(a: &[u8]) -> Option<LexBound> {
    match a.first() {
        Some(b'-') if a.len() == 1 => Some(LexBound::Min),
        Some(b'+') if a.len() == 1 => Some(LexBound::Max),
        Some(b'[') => Some(LexBound::Incl(a[1..].to_vec())),
        Some(b'(') => Some(LexBound::Excl(a[1..].to_vec())),
        _ => None,
    }
}

fn lex_cmp(b: &LexBound, lower: bool) -> String {
    match (b, lower) {
        (LexBound::Min, true) | (LexBound::Max, false) => "TRUE".into(),
        (LexBound::Max, true) | (LexBound::Min, false) => "FALSE".into(),
        (LexBound::Incl(v), true) => format!("m >= {}", lit(v)),
        (LexBound::Excl(v), true) => format!("m > {}", lit(v)),
        (LexBound::Incl(v), false) => format!("m <= {}", lit(v)),
        (LexBound::Excl(v), false) => format!("m < {}", lit(v)),
    }
}

fn parse_lex_range(min: &[u8], max: &[u8]) -> R<String> {
    match (parse_lex_bound(min), parse_lex_bound(max)) {
        (Some(a), Some(b)) => Ok(format!("{} AND {}", lex_cmp(&a, true), lex_cmp(&b, false))),
        _ => fail(Frame::err("min or max not valid string range item")),
    }
}

/// Members (and scores) as a reply.
fn reply_members(cx: &Ctx<'_>, items: Vec<(Vec<u8>, f64)>, withscores: bool) -> Frame {
    if !withscores {
        return Frame::Array(
            items
                .into_iter()
                .map(|(m, _)| Frame::Bulk(m.into()))
                .collect(),
        );
    }
    if cx.resp3 {
        Frame::Array(
            items
                .into_iter()
                .map(|(m, s)| Frame::Array(vec![Frame::Bulk(m.into()), score_frame(s)]))
                .collect(),
        )
    } else {
        Frame::Array(
            items
                .into_iter()
                .flat_map(|(m, s)| [Frame::Bulk(m.into()), score_frame(s)])
                .collect(),
        )
    }
}

/// `ZADD k [NX|XX] [GT|LT] [CH] [INCR] s m ...`.
pub async fn zadd(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, false);
    let mut i = 2;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            "CH" => ch = true,
            "INCR" => incr = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &a[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return fail(reply::syntax());
    }
    if nx && xx {
        return fail(Frame::err(
            "XX and NX options at the same time are not compatible",
        ));
    }
    if (nx && (gt || lt)) || (gt && lt) {
        return fail(Frame::err(
            "GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if incr && rest.len() > 2 {
        return fail(Frame::err(
            "INCR option supports a single increment-element pair",
        ));
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    for c in rest.chunks(2) {
        let Some(s) = parse_f64(&c[0]) else {
            return fail(reply::not_float());
        };
        Ctx::check_elem(&c[1])?;
        pairs.push((s, &c[1][..]));
    }
    let row = cx.lock_typed(&a[1], KeyType::ZSet).await?;
    let mut row = match row {
        Some(r) => r,
        None if xx => {
            return Ok(if incr {
                Frame::null()
            } else {
                Frame::Integer(0)
            });
        }
        None => Ctx::new_row(&a[1], KeyType::ZSet),
    };
    let names: Vec<&[u8]> = pairs.iter().map(|p| p.1).collect();
    let mut cur = zlookup(cx, &row, &names).await?;
    let (mut added, mut changed) = (0i64, 0i64);
    let mut writes: Vec<(Vec<u8>, f64)> = Vec::new();
    let mut incr_result = None;
    for (s, m) in pairs {
        let old = cur.get(m).copied();
        let mut new = s;
        if incr && let Some(o) = old {
            new = o + s;
            if new.is_nan() {
                return fail(Frame::err("resulting score is not a number (NaN)"));
            }
        }
        match old {
            None => {
                if xx {
                    continue;
                }
                added += 1;
            }
            Some(o) => {
                if nx || (gt && new <= o) || (lt && new >= o) {
                    continue;
                }
                if new == o {
                    if incr {
                        incr_result = Some(new);
                    }
                    continue;
                }
                changed += 1;
            }
        }
        cur.insert(m.to_vec(), new);
        writes.retain(|(wm, _)| wm.as_slice() != m);
        writes.push((m.to_vec(), new));
        incr_result = Some(new);
    }
    if !writes.is_empty() {
        zupsert(cx, &row, &writes).await?;
        row.n += added;
        cx.save(&mut row).await?;
        if added > 0 {
            // BZPOPMIN and friends waiting on the key.
            cx.pushed_to(&row.k.clone());
        }
    }
    if incr {
        return Ok(incr_result.map_or(Frame::null(), score_frame));
    }
    Ok(Frame::Integer(if ch { added + changed } else { added }))
}

/// `ZINCRBY k by m`.
pub async fn zincrby(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let args: Vec<Bytes> = vec![
        a[0].clone(),
        a[1].clone(),
        Bytes::from_static(b"INCR"),
        a[2].clone(),
        a[3].clone(),
    ];
    zadd(cx, &args).await
}

/// `ZREM k m...`.
pub async fn zrem(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Integer(0));
    };
    let mut items: Vec<&[u8]> = a[2..].iter().map(|m| &m[..]).collect();
    items.sort_unstable();
    items.dedup();
    let have = zlookup(cx, &row, &items).await?;
    let gone: Vec<&[u8]> = items
        .into_iter()
        .filter(|m| have.contains_key(*m))
        .collect();
    if gone.is_empty() {
        return Ok(Frame::Integer(0));
    }
    zremove(cx, &row, &gone).await?;
    row.n -= gone.len() as i64;
    cx.save_or_delete(&mut row).await?;
    Ok(Frame::Integer(gone.len() as i64))
}

/// `ZSCORE k m`.
pub async fn zscore(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(row) = cx.get_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::null());
    };
    let m = zlookup(cx, &row, &[&a[2]]).await?;
    Ok(m.get(&a[2][..]).map_or(Frame::null(), |s| score_frame(*s)))
}

/// `ZMSCORE k m...`.
pub async fn zmscore(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let items: Vec<&[u8]> = a[2..].iter().map(|m| &m[..]).collect();
    let m = match cx.get_typed(&a[1], KeyType::ZSet).await? {
        Some(row) => zlookup(cx, &row, &items).await?,
        None => HashMap::new(),
    };
    Ok(Frame::Array(
        items
            .iter()
            .map(|i| m.get(*i).map_or(Frame::null(), |s| score_frame(*s)))
            .collect(),
    ))
}

/// `ZCARD k`.
pub async fn zcard(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let r = cx.get_typed(&a[1], KeyType::ZSet).await?;
    Ok(Frame::Integer(r.map_or(0, |r| r.n)))
}

async fn count_where(cx: &mut Ctx<'_>, row: &KeyRow, cond: &str) -> R<i64> {
    let rows = cx
        .q(&format!(
            "SELECT COUNT(*) FROM {} WHERE {} AND ({cond})",
            cx.ns.zset(),
            Ctx::kg(row)
        ))
        .await?;
    Ok(rows.first().map_or(0, |r| cell_i64(r[0].as_ref())))
}

/// `ZCOUNT k min max`.
pub async fn zcount(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (min, max) = parse_score_range(&a[2], &a[3])?;
    let Some(row) = cx.get_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Integer(0));
    };
    Ok(Frame::Integer(
        count_where(cx, &row, &score_range(min, max)).await?,
    ))
}

/// `ZLEXCOUNT k min max`.
pub async fn zlexcount(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let cond = parse_lex_range(&a[2], &a[3])?;
    let Some(row) = cx.get_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Integer(0));
    };
    Ok(Frame::Integer(count_where(cx, &row, &cond).await?))
}

/// What a range asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum By {
    Rank,
    Score,
    Lex,
}

/// A parsed range request.
struct Range {
    by: By,
    rev: bool,
    min: Bytes,
    max: Bytes,
    limit: Option<(i64, i64)>,
    withscores: bool,
}

/// Runs a range on a row (locked or read).
async fn run_range(cx: &mut Ctx<'_>, row: &KeyRow, r: &Range) -> R<Vec<(Vec<u8>, f64)>> {
    let limit_sql = |l: Option<(i64, i64)>| match l {
        None => String::new(),
        Some((o, c)) if c < 0 => format!(" LIMIT {o}, 18446744073709551615"),
        Some((o, c)) => format!(" LIMIT {o}, {c}"),
    };
    if let Some((o, _)) = r.limit
        && o < 0
    {
        return Ok(Vec::new());
    }
    match r.by {
        By::Rank => {
            let (s, e) = (int_arg(&r.min)?, int_arg(&r.max)?);
            let Some((s, e)) = index_range(row.n, s, e) else {
                return Ok(Vec::new());
            };
            zselect(
                cx,
                row,
                "TRUE",
                r.rev,
                &format!(" LIMIT {s}, {}", e - s + 1),
            )
            .await
        }
        By::Score => {
            let (lo, hi) = if r.rev {
                (&r.max, &r.min)
            } else {
                (&r.min, &r.max)
            };
            let (min, max) = parse_score_range(lo, hi)?;
            zselect(cx, row, &score_range(min, max), r.rev, &limit_sql(r.limit)).await
        }
        By::Lex => {
            let (lo, hi) = if r.rev {
                (&r.max, &r.min)
            } else {
                (&r.min, &r.max)
            };
            let cond = parse_lex_range(lo, hi)?;
            zselect(cx, row, &cond, r.rev, &limit_sql(r.limit)).await
        }
    }
}

/// Parses `ZRANGE`-family options after `min max` at `a[start..]`.
fn parse_range(
    a: &[Bytes],
    min: usize,
    start: usize,
    by: By,
    rev: bool,
    allow_by: bool,
) -> R<Range> {
    let mut r = Range {
        by,
        rev,
        min: a[min].clone(),
        max: a[min + 1].clone(),
        limit: None,
        withscores: false,
    };
    let mut i = start;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "WITHSCORES" => r.withscores = true,
            "LIMIT" if i + 2 < a.len() => {
                r.limit = Some((int_arg(&a[i + 1])?, int_arg(&a[i + 2])?));
                i += 2;
            }
            "BYSCORE" if allow_by && r.by == By::Rank => r.by = By::Score,
            "BYLEX" if allow_by && r.by == By::Rank => r.by = By::Lex,
            "REV" if allow_by => r.rev = true,
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    if r.limit.is_some() && r.by == By::Rank {
        return fail(Frame::err(
            "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if r.withscores && r.by == By::Lex {
        return fail(Frame::err(
            "syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }
    Ok(r)
}

/// Validates bounds before reading, so errors come even for missing keys.
fn check_bounds(r: &Range) -> R<()> {
    match r.by {
        By::Rank => {
            int_arg(&r.min)?;
            int_arg(&r.max)?;
        }
        By::Score => {
            parse_score_range(&r.min, &r.max)?;
        }
        By::Lex => {
            parse_lex_range(&r.min, &r.max)?;
        }
    }
    Ok(())
}

/// `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`,
/// `ZRANGEBYLEX`, `ZREVRANGEBYLEX`.
pub async fn zrange(cx: &mut Ctx<'_>, a: &[Bytes], by: By0, rev: bool) -> R<Frame> {
    let r = match by {
        By0::Generic => parse_range(a, 2, 4, By::Rank, false, true)?,
        By0::Rank => parse_range(a, 2, 4, By::Rank, rev, false)?,
        By0::Score => parse_range(a, 2, 4, By::Score, rev, false)?,
        By0::Lex => parse_range(a, 2, 4, By::Lex, rev, false)?,
    };
    check_bounds(&r)?;
    let Some(row) = cx.get_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Array(Vec::new()));
    };
    let items = run_range(cx, &row, &r).await?;
    Ok(reply_members(cx, items, r.withscores))
}

/// Which `ZRANGE` form was called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum By0 {
    /// `ZRANGE` with its own `BYSCORE`/`BYLEX`/`REV`.
    Generic,
    /// `ZREVRANGE`.
    Rank,
    /// `Z[REV]RANGEBYSCORE`.
    Score,
    /// `Z[REV]RANGEBYLEX`.
    Lex,
}

/// `ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT o c]`.
pub async fn zrangestore(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let r = parse_range(a, 3, 5, By::Rank, false, true)?;
    if r.withscores {
        return fail(reply::syntax());
    }
    check_bounds(&r)?;
    let rows = cx.lock_many(&[&a[1][..], &a[2][..]]).await?;
    let items = match &rows[1] {
        Some(src) if src.t != KeyType::ZSet => return fail(reply::wrongtype()),
        Some(src) => run_range(cx, src, &r).await?,
        None => Vec::new(),
    };
    let dst_old = if a[1] == a[2] {
        rows[1].clone()
    } else {
        rows[0].clone()
    };
    store_zset(cx, &a[1], dst_old, &items).await?;
    Ok(Frame::Integer(items.len() as i64))
}

/// Replaces `dst` with a sorted set of `items`.
async fn store_zset(
    cx: &mut Ctx<'_>,
    dst: &[u8],
    old: Option<KeyRow>,
    items: &[(Vec<u8>, f64)],
) -> R<()> {
    if let Some(o) = old {
        cx.delete(&o).await?;
    }
    if items.is_empty() {
        return Ok(());
    }
    let mut row = Ctx::new_row(dst, KeyType::ZSet);
    zupsert(cx, &row, items).await?;
    row.n = items.len() as i64;
    cx.save(&mut row).await?;
    Ok(())
}

/// The rank of `m` in `row` (0-based) and its score.
async fn rank_of(cx: &mut Ctx<'_>, row: &KeyRow, m: &[u8], rev: bool) -> R<Option<(i64, f64)>> {
    let have = zlookup(cx, row, &[m]).await?;
    let Some(s) = have.get(m).copied() else {
        return Ok(None);
    };
    let (c, sv) = cs(s);
    let ml = lit(m);
    let below = if rev {
        format!("(c > {c} OR (c = {c} AND (score > {sv} OR (score = {sv} AND m > {ml}))))")
    } else {
        format!("(c < {c} OR (c = {c} AND (score < {sv} OR (score = {sv} AND m < {ml}))))")
    };
    Ok(Some((count_where(cx, row, &below).await?, s)))
}

/// `ZRANK k m [WITHSCORE]`, `ZREVRANK`.
pub async fn zrank(cx: &mut Ctx<'_>, a: &[Bytes], rev: bool) -> R<Frame> {
    let withscore = match a.get(3) {
        None => false,
        Some(o) if a.len() == 4 && upper(o) == "WITHSCORE" => true,
        Some(_) => return fail(reply::syntax()),
    };
    let Some(row) = cx.get_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(if withscore {
            Frame::null_array()
        } else {
            Frame::null()
        });
    };
    Ok(match rank_of(cx, &row, &a[2], rev).await? {
        None if withscore => Frame::null_array(),
        None => Frame::null(),
        Some((r, s)) if withscore => Frame::Array(vec![Frame::Integer(r), score_frame(s)]),
        Some((r, _)) => Frame::Integer(r),
    })
}

/// `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`.
pub async fn zremrange(cx: &mut Ctx<'_>, a: &[Bytes], by: u8) -> R<Frame> {
    let r = Range {
        by: match by {
            b'r' => By::Rank,
            b's' => By::Score,
            _ => By::Lex,
        },
        rev: false,
        min: a[2].clone(),
        max: a[3].clone(),
        limit: None,
        withscores: false,
    };
    check_bounds(&r)?;
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Integer(0));
    };
    let items = run_range(cx, &row, &r).await?;
    if items.is_empty() {
        return Ok(Frame::Integer(0));
    }
    let gone: Vec<&[u8]> = items.iter().map(|(m, _)| m.as_slice()).collect();
    zremove(cx, &row, &gone).await?;
    row.n -= gone.len() as i64;
    cx.save_or_delete(&mut row).await?;
    Ok(Frame::Integer(gone.len() as i64))
}

/// Pops `count` members from the min or max end of a locked `row`.
async fn zpop_row(
    cx: &mut Ctx<'_>,
    row: &mut KeyRow,
    count: i64,
    max: bool,
) -> R<Vec<(Vec<u8>, f64)>> {
    let items = zselect(cx, row, "TRUE", max, &format!(" LIMIT {count}")).await?;
    if items.is_empty() {
        return Ok(items);
    }
    let gone: Vec<&[u8]> = items.iter().map(|(m, _)| m.as_slice()).collect();
    zremove(cx, row, &gone).await?;
    row.n -= gone.len() as i64;
    cx.save_or_delete(row).await?;
    Ok(items)
}

/// `ZPOPMIN k [count]`, `ZPOPMAX`.
pub async fn zpop(cx: &mut Ctx<'_>, a: &[Bytes], max: bool) -> R<Frame> {
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
    let Some(mut row) = cx.lock_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Array(Vec::new()));
    };
    let items = zpop_row(cx, &mut row, count.unwrap_or(1), max).await?;
    if count.is_none() {
        // A single pair, flat in both protocols.
        return Ok(Frame::Array(
            items
                .into_iter()
                .flat_map(|(m, s)| [Frame::Bulk(m.into()), score_frame(s)])
                .collect(),
        ));
    }
    Ok(reply_members(cx, items, true))
}

/// Tries `BZPOPMIN`/`BZPOPMAX` once over `keys`.
pub async fn try_bzpop(cx: &mut Ctx<'_>, keys: &[Bytes], max: bool) -> R<Option<Frame>> {
    for k in keys {
        let Some(mut row) = cx.lock_typed(k, KeyType::ZSet).await? else {
            continue;
        };
        let items = zpop_row(cx, &mut row, 1, max).await?;
        if let Some((m, s)) = items.into_iter().next() {
            return Ok(Some(Frame::Array(vec![
                Frame::Bulk(k.clone()),
                Frame::Bulk(m.into()),
                score_frame(s),
            ])));
        }
    }
    Ok(None)
}

/// `numkeys k... MIN|MAX [COUNT n]` of `ZMPOP` (`at` = index of numkeys).
pub fn zmpop_args(a: &[Bytes], at: usize) -> R<(Vec<Bytes>, bool, i64)> {
    let n = match parse_i64(&a[at]) {
        Some(n) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
        _ => return fail(Frame::err("numkeys should be greater than 0")),
    };
    if n >= a.len() - at {
        return fail(reply::syntax());
    }
    let keys = a[at + 1..at + 1 + n].to_vec();
    let rest = &a[at + 1 + n..];
    let max = match rest.first().map(upper).as_deref() {
        Some("MIN") => false,
        Some("MAX") => true,
        _ => return fail(reply::syntax()),
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
    Ok((keys, max, count))
}

/// Tries `ZMPOP` once: `[key, [[m, s], ...]]`.
pub async fn try_zmpop(
    cx: &mut Ctx<'_>,
    keys: &[Bytes],
    max: bool,
    count: i64,
) -> R<Option<Frame>> {
    for k in keys {
        let Some(mut row) = cx.lock_typed(k, KeyType::ZSet).await? else {
            continue;
        };
        let items = zpop_row(cx, &mut row, count, max).await?;
        if !items.is_empty() {
            return Ok(Some(Frame::Array(vec![
                Frame::Bulk(k.clone()),
                Frame::Array(
                    items
                        .into_iter()
                        .map(|(m, s)| Frame::Array(vec![Frame::Bulk(m.into()), score_frame(s)]))
                        .collect(),
                ),
            ])));
        }
    }
    Ok(None)
}

/// `ZMPOP numkeys k... MIN|MAX [COUNT n]`.
pub async fn zmpop(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (keys, max, count) = zmpop_args(a, 1)?;
    Ok(try_zmpop(cx, &keys, max, count)
        .await?
        .unwrap_or_else(Frame::null_array))
}

/// Every member of a live sorted set, ordered (`None`: absent).
async fn all_of(cx: &mut Ctx<'_>, k: &[u8]) -> R<Option<Vec<(Vec<u8>, f64)>>> {
    let Some(row) = cx.get_typed(k, KeyType::ZSet).await? else {
        return Ok(None);
    };
    Ok(Some(zselect(cx, &row, "TRUE", false, "").await?))
}

/// `ZRANDMEMBER k [count [WITHSCORES]]`.
pub async fn zrandmember(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let count = a.get(2).map(|c| int_arg(c)).transpose()?;
    let withscores = match a.get(3) {
        None => false,
        Some(o) if a.len() == 4 && upper(o) == "WITHSCORES" => true,
        Some(_) => return fail(reply::syntax()),
    };
    if let Some(c) = count
        && c.unsigned_abs() > (i64::MAX as u64) / 2
    {
        return fail(Frame::err("value is out of range"));
    }
    let items = all_of(cx, &a[1]).await?;
    let Some(count) = count else {
        return Ok(items
            .and_then(|i| pick(&i, 1).into_iter().next())
            .map_or(Frame::null(), |(m, _)| Frame::Bulk(m.into())));
    };
    let picked = pick(&items.unwrap_or_default(), count);
    Ok(reply_members(cx, picked, withscores))
}

/// How `ZUNION`/`ZINTER` combine scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Agg {
    Sum,
    Min,
    Max,
}

/// The set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZOp {
    /// `ZUNION`.
    Union,
    /// `ZINTER`.
    Inter,
    /// `ZDIFF`.
    Diff,
}

/// Computes a `ZUNION`/`ZINTER`/`ZDIFF` over `a[at]` = numkeys.
async fn zalgebra(
    cx: &mut Ctx<'_>,
    a: &[Bytes],
    at: usize,
    op: ZOp,
    store: bool,
    name: &str,
) -> R<(Vec<(Vec<u8>, f64)>, bool)> {
    let n = match parse_i64(&a[at]) {
        Some(n) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
        Some(_) => {
            return fail(Frame::err(&format!(
                "at least 1 input key is needed for '{name}' command"
            )));
        }
        None => return fail(reply::not_int()),
    };
    if n > a.len() - at - 1 {
        return fail(reply::syntax());
    }
    let keys = &a[at + 1..at + 1 + n];
    let mut weights = vec![1.0f64; n];
    let mut agg = Agg::Sum;
    let mut withscores = false;
    let mut i = at + 1 + n;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "WEIGHTS" if op != ZOp::Diff && i + n < a.len() => {
                for (j, w) in weights.iter_mut().enumerate() {
                    *w = match parse_f64(&a[i + 1 + j]) {
                        Some(w) => w,
                        None => return fail(Frame::err("weight value is not a float")),
                    };
                }
                i += n;
            }
            "AGGREGATE" if op != ZOp::Diff && i + 1 < a.len() => {
                agg = match upper(&a[i + 1]).as_str() {
                    "SUM" => Agg::Sum,
                    "MIN" => Agg::Min,
                    "MAX" => Agg::Max,
                    _ => return fail(reply::syntax()),
                };
                i += 1;
            }
            "WITHSCORES" if !store => withscores = true,
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    // Read each source: sorted sets with their scores, sets with 1.
    let mut srcs: Vec<Option<Members>> = Vec::with_capacity(n);
    for k in keys {
        let r = if store {
            cx.lock(k).await?
        } else {
            cx.get(k).await?
        };
        srcs.push(match r {
            None => None,
            Some(r) if r.t == KeyType::ZSet => Some(zselect(cx, &r, "TRUE", false, "").await?),
            Some(r) if r.t == KeyType::Set => Some(
                members(cx, k)
                    .await?
                    .unwrap_or_default()
                    .into_iter()
                    .map(|m| (m, 1.0))
                    .collect(),
            ),
            Some(_) => return fail(reply::wrongtype()),
        });
    }
    let fix = |x: f64| if x.is_nan() { 0.0 } else { x };
    let combine = |acc: f64, x: f64| match agg {
        Agg::Sum => fix(acc + x),
        Agg::Min => acc.min(x),
        Agg::Max => acc.max(x),
    };
    let mut out: Vec<(Vec<u8>, f64)> = Vec::new();
    match op {
        ZOp::Union => {
            let mut map: HashMap<Vec<u8>, f64> = HashMap::new();
            for (j, s) in srcs.into_iter().enumerate() {
                for (m, sc) in s.unwrap_or_default() {
                    let v = fix(sc * weights[j]);
                    map.entry(m)
                        .and_modify(|acc| *acc = combine(*acc, v))
                        .or_insert(v);
                }
            }
            out.extend(map);
        }
        ZOp::Inter => {
            if srcs.iter().any(Option::is_none) {
                return Ok((Vec::new(), withscores));
            }
            let srcs: Vec<Vec<(Vec<u8>, f64)>> =
                srcs.into_iter().map(Option::unwrap_or_default).collect();
            let maps: Vec<HashMap<&[u8], f64>> = srcs
                .iter()
                .map(|s| s.iter().map(|(m, sc)| (m.as_slice(), *sc)).collect())
                .collect();
            for (m, sc) in &srcs[0] {
                let mut acc = fix(sc * weights[0]);
                let mut all = true;
                for (j, mp) in maps.iter().enumerate().skip(1) {
                    let Some(x) = mp.get(m.as_slice()) else {
                        all = false;
                        break;
                    };
                    acc = combine(acc, fix(x * weights[j]));
                }
                if all {
                    out.push((m.clone(), acc));
                }
            }
        }
        ZOp::Diff => {
            let mut it = srcs.into_iter();
            let first = it.next().flatten().unwrap_or_default();
            let others: Vec<std::collections::HashSet<Vec<u8>>> = it
                .map(|s| s.unwrap_or_default().into_iter().map(|(m, _)| m).collect())
                .collect();
            out = first
                .into_iter()
                .filter(|(m, _)| !others.iter().any(|o| o.contains(m)))
                .collect();
        }
    }
    out.sort_by(|x, y| x.1.total_cmp(&y.1).then_with(|| x.0.cmp(&y.0)));
    Ok((out, withscores))
}

/// `ZUNION`, `ZINTER`, `ZDIFF`.
pub async fn zalg(cx: &mut Ctx<'_>, a: &[Bytes], op: ZOp) -> R<Frame> {
    let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
    let (items, ws) = zalgebra(cx, a, 1, op, false, &name).await?;
    Ok(reply_members(cx, items, ws))
}

/// `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`.
pub async fn zalgstore(cx: &mut Ctx<'_>, a: &[Bytes], op: ZOp) -> R<Frame> {
    let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
    let (items, _) = zalgebra(cx, a, 2, op, true, &name).await?;
    let old = cx.lock(&a[1]).await?;
    store_zset(cx, &a[1], old, &items).await?;
    Ok(Frame::Integer(items.len() as i64))
}

/// `ZINTERCARD numkeys k... [LIMIT n]`.
pub async fn zintercard(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let (keys, limit) = crate::cmd::hashes::numkeys_limit(a)?;
    let mut args: Vec<Bytes> = vec![a[0].clone(), Bytes::from(keys.len().to_string())];
    args.extend(keys.iter().cloned());
    let (items, _) = zalgebra(cx, &args, 1, ZOp::Inter, false, "zintercard").await?;
    let n = items.len() as i64;
    Ok(Frame::Integer(if limit > 0 { n.min(limit) } else { n }))
}

/// `ZSCAN k cursor [MATCH p] [COUNT n]`.
pub async fn zscan(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(cursor) = std::str::from_utf8(&a[2])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return fail(Frame::err("invalid cursor"));
    };
    let (pattern, count, _) = scan_opts(&a[3..], false)?;
    let Some(row) = cx.get_typed(&a[1], KeyType::ZSet).await? else {
        return Ok(Frame::Array(vec![
            Frame::bulk("0"),
            Frame::Array(Vec::new()),
        ]));
    };
    let small =
        usize::try_from(row.n).unwrap_or(usize::MAX) <= cx.env.config().scan_small_key_elements;
    let (items, next) = if small {
        (zselect(cx, &row, "TRUE", false, "").await?, 0u64)
    } else {
        let rows = cx
            .q(&format!(
                "SELECT m, c, score, o FROM {} WHERE {} AND o >= {cursor} ORDER BY o, m LIMIT {count}",
                cx.ns.zset(),
                Ctx::kg(&row)
            ))
            .await?;
        let full = rows.len() == count;
        let mut items = Vec::new();
        let mut last = None;
        for r in rows {
            let o = cell_u64(r.get(3).and_then(Option::as_ref));
            let s = score_of(cell_i64(r[1].as_ref()), cell_f64(r[2].as_ref()));
            let m = r.into_iter().next().flatten().unwrap_or_default();
            last = Some((o, m.clone()));
            items.push((m, s));
        }
        let next = match (last, full) {
            (Some((o, m)), true) => {
                let more = cx
                    .q(&format!(
                        "SELECT m, c, score FROM {} WHERE {} AND o = {o} AND m > {} ORDER BY m",
                        cx.ns.zset(),
                        Ctx::kg(&row),
                        lit(&m)
                    ))
                    .await?;
                for r in more {
                    let s = score_of(cell_i64(r[1].as_ref()), cell_f64(r[2].as_ref()));
                    items.push((r.into_iter().next().flatten().unwrap_or_default(), s));
                }
                o + 1
            }
            _ => 0,
        };
        (items, next)
    };
    let mut out = Vec::new();
    for (m, s) in items {
        if pattern.as_ref().is_some_and(|p| !glob::matches(p, &m)) {
            continue;
        }
        out.push(Frame::Bulk(m.into()));
        out.push(Frame::bulk(d2string(s)));
    }
    Ok(Frame::Array(vec![
        Frame::bulk(next.to_string()),
        Frame::Array(out),
    ]))
}

/// Every member and score of a locked or read row (for `SORT`, geo).
pub(crate) async fn all_members(cx: &mut Ctx<'_>, row: &KeyRow) -> R<Vec<(Vec<u8>, f64)>> {
    zselect(cx, row, "TRUE", false, "").await
}

/// Adds or updates members of a locked zset `row` (geo).
pub(crate) async fn add_members(
    cx: &mut Ctx<'_>,
    row: &mut KeyRow,
    items: &[(Vec<u8>, f64)],
    nx: bool,
    xx: bool,
    ch: bool,
) -> R<i64> {
    let names: Vec<&[u8]> = items.iter().map(|(m, _)| m.as_slice()).collect();
    let cur = zlookup(cx, row, &names).await?;
    let (mut added, mut changed) = (0, 0);
    let mut writes = Vec::new();
    for (m, s) in items {
        match cur.get(m) {
            None if xx => {}
            None => {
                added += 1;
                writes.push((m.clone(), *s));
            }
            Some(_) if nx => {}
            Some(o) if o == s => {}
            Some(_) => {
                changed += 1;
                writes.push((m.clone(), *s));
            }
        }
    }
    if !writes.is_empty() {
        zupsert(cx, row, &writes).await?;
        row.n += added;
        cx.save(row).await?;
    }
    Ok(if ch { added + changed } else { added })
}

/// Stores a sorted set (geo's `STORE`).
pub(crate) async fn store(cx: &mut Ctx<'_>, dst: &[u8], items: &[(Vec<u8>, f64)]) -> R<()> {
    let old = cx.lock(dst).await?;
    store_zset(cx, dst, old, items).await
}

/// Scores of `members` in live zset `k` (geo).
pub(crate) async fn scores_of(
    cx: &mut Ctx<'_>,
    k: &[u8],
    items: &[&[u8]],
) -> R<Option<HashMap<Vec<u8>, f64>>> {
    let Some(row) = cx.get_typed(k, KeyType::ZSet).await? else {
        return Ok(None);
    };
    Ok(Some(zlookup(cx, &row, items).await?))
}

/// The live `row` of zset `k` (geo).
pub(crate) async fn row_of(cx: &mut Ctx<'_>, k: &[u8]) -> R<Option<KeyRow>> {
    cx.get_typed(k, KeyType::ZSet).await
}
