//! Generic key commands (chapter 40, "Expiry", "SCAN cursors",
//! "Multi-key commands").

use bytes::Bytes;
use sprig_kv::layout::{KeyType, Namespace, live};
use sprig_kv::sql::{cell_u64, lit};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_i64;

use crate::ctx::{Ctx, KeyRow, R, fail, int_arg, new_version, upper};
use crate::glob;
use crate::reply;

/// `DEL k...` and `UNLINK`.
pub async fn del(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let keys: Vec<&[u8]> = a[1..].iter().map(|k| &k[..]).collect();
    let rows = cx.lock_many(&keys).await?;
    let mut n = 0;
    let mut seen: Vec<&[u8]> = Vec::new();
    for (k, r) in keys.iter().zip(rows) {
        if seen.contains(k) {
            continue;
        }
        seen.push(k);
        if let Some(r) = r {
            cx.delete(&r).await?;
            n += 1;
        }
    }
    Ok(Frame::Integer(n))
}

/// `EXISTS k...`: repeated keys count again, as in Redis.
pub async fn exists(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let found = live_keys(cx, &a[1..]).await?;
    Ok(Frame::Integer(
        a[1..]
            .iter()
            .filter(|k| found.contains(&k.to_vec()))
            .count() as i64,
    ))
}

/// The live keys among `keys`, in one statement.
async fn live_keys(cx: &mut Ctx<'_>, keys: &[Bytes]) -> R<Vec<Vec<u8>>> {
    let list: Vec<&[u8]> = keys
        .iter()
        .filter(|k| k.len() <= 1024)
        .map(|k| &k[..])
        .collect();
    if list.is_empty() {
        return Ok(Vec::new());
    }
    let rows = cx
        .q(&format!(
            "SELECT k FROM {} WHERE {} AND {}",
            cx.ns.keys(),
            cx.keys_in(&list),
            live("")
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect())
}

/// `TOUCH k...`: like `EXISTS` without repeats counting twice.
pub async fn touch(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let found = live_keys(cx, &a[1..]).await?;
    Ok(Frame::Integer(
        a[1..]
            .iter()
            .filter(|k| found.contains(&k.to_vec()))
            .count() as i64,
    ))
}

/// `TYPE k`.
pub async fn type_(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let r = cx.get(&a[1]).await?;
    Ok(Frame::simple(r.map_or("none", |r| r.t.as_str())))
}

/// `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` with `NX|XX|GT|LT`.
pub async fn expire(cx: &mut Ctx<'_>, a: &[Bytes], unit: i64, absolute: bool) -> R<Frame> {
    let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
    let mut when = int_arg(&a[2])?;
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for o in &a[3..] {
        match upper(o).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => {
                return fail(Frame::err(&format!(
                    "Unsupported option {}",
                    String::from_utf8_lossy(o)
                )));
            }
        }
    }
    if nx && (xx || gt || lt) {
        return fail(Frame::err(
            "NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return fail(Frame::err(
            "GT and LT options at the same time are not compatible",
        ));
    }
    let invalid = || {
        fail(Frame::err(&format!(
            "invalid expire time in '{name}' command"
        )))
    };
    if unit == 1000 {
        if !(i64::MIN / 1000..=i64::MAX / 1000).contains(&when) {
            return invalid();
        }
        when *= 1000;
    }
    let row = cx.lock(&a[1]).await?;
    let Some(mut row) = row else {
        return Ok(Frame::Integer(0));
    };
    let at = if absolute {
        when
    } else {
        match when.checked_add(cx.now) {
            Some(x) => x,
            None => return invalid(),
        }
    };
    // No TTL counts as infinite for GT and LT.
    let cur = row.exp;
    let ok = if nx {
        cur.is_none()
    } else if xx && cur.is_none() {
        false
    } else if gt {
        cur.is_some_and(|c| at > c)
    } else if lt {
        cur.is_none_or(|c| at < c)
    } else {
        true
    };
    if !ok {
        return Ok(Frame::Integer(0));
    }
    if at <= cx.now {
        cx.delete(&row).await?;
    } else {
        row.exp = Some(at);
        cx.save(&mut row).await?;
    }
    Ok(Frame::Integer(1))
}

/// `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`.
pub async fn ttl(cx: &mut Ctx<'_>, a: &[Bytes], ms: bool, absolute: bool) -> R<Frame> {
    let r = cx.get(&a[1]).await?;
    let Some(r) = r else {
        return Ok(Frame::Integer(-2));
    };
    let Some(exp) = r.exp else {
        return Ok(Frame::Integer(-1));
    };
    if absolute {
        return Ok(Frame::Integer(if ms { exp } else { exp / 1000 }));
    }
    let left = (exp - cx.now).max(0);
    Ok(Frame::Integer(if ms { left } else { (left + 500) / 1000 }))
}

/// `PERSIST k`.
pub async fn persist(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(mut row) = cx.lock(&a[1]).await? else {
        return Ok(Frame::Integer(0));
    };
    if row.exp.is_none() {
        return Ok(Frame::Integer(0));
    }
    row.exp = None;
    cx.save(&mut row).await?;
    Ok(Frame::Integer(1))
}

/// Copies `row`'s elements to key `to` in `dst` under generation `g`.
async fn copy_elems(
    cx: &mut Ctx<'_>,
    src: &Namespace,
    row: &KeyRow,
    dst: &Namespace,
    to: &[u8],
    g: u64,
) -> R<()> {
    let route = format!("{}, {}", lit(dst.dist_key(to)), dst.routing_hash(to));
    let sql = match row.t {
        KeyType::String => return Ok(()),
        KeyType::ZSet => format!(
            "INSERT INTO {} (k, g, o, m, c, score, _sprig_dk, _sprig_rk) \
             SELECT {}, {g}, o, m, c, score, {route} FROM {} WHERE {}",
            dst.zset(),
            lit(to),
            src.zset(),
            cx.kg(row)
        ),
        _ => format!(
            "INSERT INTO {} (k, g, o, e, v, _sprig_dk, _sprig_rk) \
             SELECT {}, {g}, o, e, v, {route} FROM {} WHERE {}",
            dst.elems(),
            lit(to),
            src.elems(),
            cx.kg(row)
        ),
    };
    cx.x(&sql).await?;
    Ok(())
}

/// Moves or copies `row` (in `src`) to key `to` in `dst`, whose old row
/// `old` is replaced. `keep` keeps the source.
async fn relocate(
    cx: &mut Ctx<'_>,
    src: &Namespace,
    row: &KeyRow,
    dst: &Namespace,
    to: &[u8],
    old: Option<KeyRow>,
    keep: bool,
) -> R<()> {
    if let Some(o) = old {
        cx.delete_in(dst, &o).await?;
    }
    let g = new_version();
    copy_elems(cx, src, row, dst, to, g).await?;
    let mut nr = row.clone();
    nr.k = to.to_vec();
    nr.g = g;
    nr.stored = false;
    cx.save_in(dst, &mut nr).await?;
    if !keep {
        cx.delete_in(src, row).await?;
    }
    Ok(())
}

/// `RENAME k newk` and `RENAMENX`.
pub async fn rename(cx: &mut Ctx<'_>, a: &[Bytes], nx: bool) -> R<Frame> {
    let rows = cx.lock_many(&[&a[1][..], &a[2][..]]).await?;
    let Some(src) = rows[0].clone() else {
        return fail(reply::no_such_key());
    };
    if a[1] == a[2] {
        return Ok(if nx { Frame::Integer(0) } else { Frame::ok() });
    }
    if nx && rows[1].is_some() {
        return Ok(Frame::Integer(0));
    }
    let ns = cx.ns.clone();
    relocate(cx, &ns, &src, &ns, &a[2], rows[1].clone(), false).await?;
    Ok(if nx { Frame::Integer(1) } else { Frame::ok() })
}

/// `COPY src dst [DB n] [REPLACE]`.
pub async fn copy(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let mut db = cx.db;
    let mut replace = false;
    let mut i = 3;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "REPLACE" => replace = true,
            "DB" if i + 1 < a.len() => {
                db = db_index(cx, &a[i + 1])?;
                i += 1;
            }
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    if db == cx.db && a[1] == a[2] {
        return fail(Frame::err("source and destination objects are the same"));
    }
    let src_ns = cx.ns.clone();
    let dst_ns = cx.ns_of(db);
    let Some(src) = cx.lock_in(&src_ns, &a[1]).await? else {
        return Ok(Frame::Integer(0));
    };
    let old = cx.lock_in(&dst_ns, &a[2]).await?;
    if old.is_some() && !replace {
        return Ok(Frame::Integer(0));
    }
    relocate(cx, &src_ns, &src, &dst_ns, &a[2], old, true).await?;
    Ok(Frame::Integer(1))
}

/// A `SELECT`-style database index.
///
/// # Errors
/// Redis's texts.
pub fn db_index(cx: &Ctx<'_>, a: &[u8]) -> R<u32> {
    let Some(n) = parse_i64(a) else {
        return fail(reply::not_int());
    };
    let max = i64::from(cx.env.config().databases);
    if n < 0 || n >= max {
        return fail(Frame::err("DB index is out of range"));
    }
    Ok(u32::try_from(n).unwrap_or(0))
}

/// `MOVE k db`.
pub async fn move_(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let db = db_index(cx, &a[2])?;
    if db == cx.db {
        return fail(Frame::err("source and destination objects are the same"));
    }
    let src_ns = cx.ns.clone();
    let dst_ns = cx.ns_of(db);
    let Some(src) = cx.lock_in(&src_ns, &a[1]).await? else {
        return Ok(Frame::Integer(0));
    };
    if cx.lock_in(&dst_ns, &a[1]).await?.is_some() {
        return Ok(Frame::Integer(0));
    }
    relocate(cx, &src_ns, &src, &dst_ns, &a[1], None, false).await?;
    Ok(Frame::Integer(1))
}

/// The smallest byte string greater than every string starting with `p`
/// (`None` when there is none, `p` being all `0xFF`).
#[must_use]
pub fn prefix_end(p: &[u8]) -> Option<Vec<u8>> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last < 0xFF {
            e.push(last + 1);
            return Some(e);
        }
    }
    None
}

/// A key-range predicate for a glob's literal prefix.
fn prefix_sql(pattern: &[u8]) -> String {
    let p = glob::literal_prefix(pattern);
    if p.is_empty() {
        return String::new();
    }
    match prefix_end(p) {
        Some(e) => format!(" AND k >= {} AND k < {}", lit(p), lit(&e)),
        None => format!(" AND k >= {}", lit(p)),
    }
}

/// `KEYS pattern`.
pub async fn keys(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let rows = cx
        .q(&format!(
            "SELECT k FROM {} WHERE {}{} ORDER BY k",
            cx.ns.keys(),
            live(""),
            prefix_sql(&a[1])
        ))
        .await?;
    Ok(Frame::Array(
        rows.into_iter()
            .filter_map(|r| r.into_iter().next().flatten())
            .filter(|k| glob::matches(&a[1], k))
            .map(|k| Frame::Bulk(Bytes::from(k)))
            .collect(),
    ))
}

/// `SCAN cursor [MATCH p] [COUNT n] [TYPE t]`: the cursor is the next
/// routing hash; a page ends after every key of its last hash.
pub async fn scan(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let Some(cursor) = std::str::from_utf8(&a[1])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return fail(Frame::err("invalid cursor"));
    };
    let (pattern, count, ty) = scan_opts(&a[2..], true)?;
    let ty_sql = match &ty {
        Some(t) => format!(" AND type = {}", lit(t.to_ascii_lowercase().as_bytes())),
        None => String::new(),
    };
    let rows = cx
        .q(&format!(
            "SELECT k, _sprig_rk FROM {} WHERE _sprig_rk >= {cursor} AND {}{ty_sql} \
             ORDER BY _sprig_rk, k LIMIT {count}",
            cx.ns.keys(),
            live("")
        ))
        .await?;
    let full = rows.len() == count;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
    let mut last: Option<(u64, Vec<u8>)> = None;
    for r in rows {
        let k = r.first().cloned().flatten().unwrap_or_default();
        let h = cell_u64(r.get(1).and_then(Option::as_ref));
        last = Some((h, k.clone()));
        keys.push(k);
    }
    let next = match (&last, full) {
        (Some((h, k)), true) => {
            // The rest of the last hash's keys.
            let more = cx
                .q(&format!(
                    "SELECT k FROM {} WHERE _sprig_rk = {h} AND k > {} AND {}{ty_sql} ORDER BY k",
                    cx.ns.keys(),
                    lit(k),
                    live("")
                ))
                .await?;
            keys.extend(
                more.into_iter()
                    .filter_map(|r| r.into_iter().next().flatten()),
            );
            h.checked_add(1).unwrap_or(0)
        }
        _ => 0,
    };
    let items = keys
        .into_iter()
        .filter(|k| pattern.as_ref().is_none_or(|p| glob::matches(p, k)))
        .map(|k| Frame::Bulk(Bytes::from(k)))
        .collect();
    Ok(Frame::Array(vec![
        Frame::bulk(next.to_string()),
        Frame::Array(items),
    ]))
}

/// `MATCH`, `COUNT` and (for `SCAN`) `TYPE` options.
///
/// # Errors
/// `-ERR syntax error`, or a bad count.
#[allow(clippy::type_complexity)]
pub fn scan_opts(a: &[Bytes], allow_type: bool) -> R<(Option<Vec<u8>>, usize, Option<String>)> {
    let mut pattern = None;
    let mut count = 10usize;
    let mut ty = None;
    let mut i = 0;
    while i < a.len() {
        if i + 1 >= a.len() {
            return fail(reply::syntax());
        }
        match upper(&a[i]).as_str() {
            "MATCH" => {
                let p = a[i + 1].to_vec();
                pattern = if p == b"*" { None } else { Some(p) };
            }
            "COUNT" => {
                let n = int_arg(&a[i + 1])?;
                if n < 1 {
                    return fail(reply::syntax());
                }
                count = usize::try_from(n).unwrap_or(usize::MAX).min(100_000);
            }
            "TYPE" if allow_type => ty = Some(String::from_utf8_lossy(&a[i + 1]).into_owned()),
            _ => return fail(reply::syntax()),
        }
        i += 2;
    }
    Ok((pattern, count, ty))
}

/// `RANDOMKEY`.
pub async fn randomkey(cx: &mut Ctx<'_>) -> R<Frame> {
    let start = new_version() << 1;
    for cond in [format!("_sprig_rk >= {start}"), "TRUE".to_owned()] {
        let rows = cx
            .q(&format!(
                "SELECT k FROM {} WHERE {cond} AND {} ORDER BY _sprig_rk LIMIT 1",
                cx.ns.keys(),
                live("")
            ))
            .await?;
        if let Some(k) = rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten())
        {
            return Ok(Frame::Bulk(Bytes::from(k)));
        }
    }
    Ok(Frame::null())
}

/// `DBSIZE`: live keys.
pub async fn dbsize(cx: &mut Ctx<'_>) -> R<Frame> {
    let rows = cx
        .q(&format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            cx.ns.keys(),
            live("")
        ))
        .await?;
    let n = rows
        .first()
        .and_then(|r| r.first())
        .map_or(0, |c| sprig_kv::sql::cell_i64(c.as_ref()));
    Ok(Frame::Integer(n))
}

/// Empties `ns` (its tables may not exist).
pub async fn flush_ns(cx: &mut Ctx<'_>, ns: &Namespace) -> R<()> {
    for t in [ns.zset(), ns.elems(), ns.keys()] {
        match cx.x(&format!("DELETE FROM {t}")).await {
            Ok(_) => {}
            Err(e) if matches!(e.errno, Some(1146 | 1049)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn flush_opts(a: &[Bytes]) -> R<()> {
    if a.len() > 2 {
        return fail(reply::syntax());
    }
    if let Some(o) = a.get(1)
        && !matches!(upper(o).as_str(), "ASYNC" | "SYNC")
    {
        return fail(reply::syntax());
    }
    Ok(())
}

/// `FLUSHDB [ASYNC|SYNC]`.
pub async fn flushdb(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    flush_opts(a)?;
    let ns = cx.ns.clone();
    flush_ns(cx, &ns).await?;
    Ok(Frame::ok())
}

/// `FLUSHALL [ASYNC|SYNC]`.
pub async fn flushall(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    flush_opts(a)?;
    // Only the databases that exist: a missing table's 1146 costs the
    // gateway's fence check up to a second each (RL14).
    // When that list can't be read, every database is flushed.
    // A quoted name (not a hex literal), which the gateway's
    // `information_schema` rewrite maps to the database's schemas.
    let schema = cx.ns.database.replace('\\', "\\\\").replace('\'', "''");
    let rows = cx
        .q(&format!(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{schema}' \
             AND TABLE_NAME REGEXP '^db[0-9]+$'"
        ))
        .await;
    let have: Option<Vec<u32>> = rows.ok().map(|rows| {
        rows.iter()
            .filter_map(|r| r.first().cloned().flatten())
            .filter_map(|t| {
                std::str::from_utf8(&t)
                    .ok()?
                    .strip_prefix("db")?
                    .parse()
                    .ok()
            })
            .collect()
    });
    for db in 0..cx.env.config().databases {
        if have.as_ref().is_some_and(|h| !h.contains(&db)) {
            continue;
        }
        let ns = cx.ns_of(db);
        flush_ns(cx, &ns).await?;
    }
    Ok(Frame::ok())
}

/// `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ|HELP k`.
pub async fn object(cx: &mut Ctx<'_>, a: &[Bytes]) -> R<Frame> {
    let sub = upper(&a[1]);
    if sub == "HELP" {
        return Ok(Frame::Array(
            [
                "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "ENCODING <key>",
                "FREQ <key>",
                "IDLETIME <key>",
                "REFCOUNT <key>",
                "HELP",
            ]
            .iter()
            .map(|s| Frame::simple(s))
            .collect(),
        ));
    }
    if a.len() != 3 || !matches!(sub.as_str(), "ENCODING" | "REFCOUNT" | "IDLETIME" | "FREQ") {
        return fail(reply::unknown_sub("object", &a[1]));
    }
    let Some(r) = cx.get(&a[2]).await? else {
        return Ok(Frame::null());
    };
    Ok(match sub.as_str() {
        "REFCOUNT" => Frame::Integer(1),
        "IDLETIME" => Frame::Integer(0),
        "FREQ" => {
            return fail(Frame::err(
                "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
            ));
        }
        _ => Frame::bulk(encoding(cx, &r).await?),
    })
}

/// The encoding Redis 7.2 would report for `r` with default settings.
async fn encoding(cx: &mut Ctx<'_>, r: &KeyRow) -> R<&'static str> {
    Ok(match r.t {
        KeyType::String => {
            let v = r.v.as_deref().unwrap_or_default();
            if v.len() <= 20 && parse_i64(v).is_some() {
                "int"
            } else if v.len() <= 44 {
                "embstr"
            } else {
                "raw"
            }
        }
        KeyType::List => {
            if r.n <= 128 && max_len(cx, r, "v").await? <= 64 {
                "listpack"
            } else {
                "quicklist"
            }
        }
        KeyType::Hash => {
            if r.n <= 128 && max_len(cx, r, "e").await?.max(max_len(cx, r, "v").await?) <= 64 {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KeyType::Set => {
            let rows = cx
                .q(&format!(
                    "SELECT e FROM {} WHERE {} LIMIT 513",
                    cx.ns.elems(),
                    cx.kg(r)
                ))
                .await?;
            let ints = rows
                .iter()
                .all(|x| x[0].as_deref().and_then(parse_i64).is_some());
            if ints && r.n <= 512 {
                "intset"
            } else if r.n <= 128 && max_len(cx, r, "e").await? <= 64 {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KeyType::ZSet => {
            let rows = cx
                .q(&format!(
                    "SELECT COALESCE(MAX(LENGTH(m)), 0) FROM {} WHERE {}",
                    cx.ns.zset(),
                    cx.kg(r)
                ))
                .await?;
            let ml = rows
                .first()
                .map_or(0, |x| sprig_kv::sql::cell_i64(x[0].as_ref()));
            if r.n <= 128 && ml <= 64 {
                "listpack"
            } else {
                "skiplist"
            }
        }
    })
}

async fn max_len(cx: &mut Ctx<'_>, r: &KeyRow, col: &str) -> R<i64> {
    let rows = cx
        .q(&format!(
            "SELECT COALESCE(MAX(LENGTH({col})), 0) FROM {} WHERE {}",
            cx.ns.elems(),
            cx.kg(r)
        ))
        .await?;
    Ok(rows
        .first()
        .map_or(0, |x| sprig_kv::sql::cell_i64(x[0].as_ref())))
}
