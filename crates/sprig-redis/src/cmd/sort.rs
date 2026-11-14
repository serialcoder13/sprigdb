//! `SORT` and `SORT_RO` without external `BY` or `GET` patterns.

use bytes::Bytes;
use sprig_kv::layout::KeyType;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_f64;

use crate::cmd::{hashes, lists, zsets};
use crate::ctx::{Ctx, R, fail, int_arg, upper};
use crate::reply;

/// `SORT k [BY nosort] [LIMIT o c] [GET #] [ASC|DESC] [ALPHA] [STORE d]`.
pub async fn sort(cx: &mut Ctx<'_>, a: &[Bytes], ro: bool) -> R<Frame> {
    let (mut desc, mut alpha, mut nosort) = (false, false, false);
    let mut limit: Option<(i64, i64)> = None;
    let mut store: Option<Bytes> = None;
    let mut i = 2;
    while i < a.len() {
        let left = a.len() - i - 1;
        match upper(&a[i]).as_str() {
            "ASC" => desc = false,
            "DESC" => desc = true,
            "ALPHA" => alpha = true,
            "LIMIT" if left >= 2 => {
                limit = Some((int_arg(&a[i + 1])?, int_arg(&a[i + 2])?));
                i += 2;
            }
            "STORE" if left >= 1 && !ro => {
                store = Some(a[i + 1].clone());
                i += 1;
            }
            "BY" if left >= 1 => {
                if a[i + 1].contains(&b'*') {
                    return fail(reply::unsupported(
                        "sort by",
                        "not offered: BY and GET patterns read other keys",
                    ));
                }
                nosort = true;
                i += 1;
            }
            "GET" if left >= 1 => {
                if &a[i + 1][..] != b"#" {
                    return fail(reply::unsupported(
                        "sort get",
                        "not offered: BY and GET patterns read other keys",
                    ));
                }
                i += 1;
            }
            _ => return fail(reply::syntax()),
        }
        i += 1;
    }
    let row = if store.is_some() {
        cx.lock(&a[1]).await?
    } else {
        cx.get(&a[1]).await?
    };
    let mut items: Vec<Vec<u8>> = match &row {
        None => Vec::new(),
        Some(r) => match r.t {
            KeyType::List => lists::all_items(cx, r).await?,
            KeyType::Set => hashes::elems_of(cx, r)
                .await?
                .into_iter()
                .map(|(e, _)| e)
                .collect(),
            KeyType::ZSet => {
                let mut m = zsets::all_members(cx, r).await?;
                if nosort && desc {
                    m.reverse();
                }
                m.into_iter().map(|(e, _)| e).collect()
            }
            _ => return fail(reply::wrongtype()),
        },
    };
    let is_set = row.as_ref().is_some_and(|r| r.t == KeyType::Set);
    // Redis sorts a set even with BY nosort, so the output is stable.
    if !nosort || is_set {
        // A large sort runs off the runtime's worker threads.
        let n = items.len();
        items = match sprig_common::cpu::heavy(n, || sorted(items, alpha, desc)) {
            Some(v) => v,
            None => {
                return fail(Frame::err(
                    "One or more scores can't be converted into double",
                ));
            }
        };
    }
    if let Some((o, c)) = limit {
        let len = items.len() as i64;
        let start = o.clamp(0, len);
        let end = if c < 0 { len } else { (start + c).min(len) };
        items =
            items[usize::try_from(start).unwrap_or(0)..usize::try_from(end).unwrap_or(0)].to_vec();
    }
    if let Some(dst) = store {
        let old = cx.lock(&dst).await?;
        lists::store_list(cx, &dst, old, &items).await?;
        return Ok(Frame::Integer(items.len() as i64));
    }
    Ok(Frame::Array(
        items.into_iter().map(|v| Frame::Bulk(v.into())).collect(),
    ))
}

/// `items` sorted as `SORT` does: as numbers (`None` when one isn't),
/// equal numbers by their bytes, or as bytes with `ALPHA`; reversed for
/// `DESC`.
fn sorted(mut items: Vec<Vec<u8>>, alpha: bool, desc: bool) -> Option<Vec<Vec<u8>>> {
    if alpha {
        items.sort();
    } else {
        let mut keyed = Vec::with_capacity(items.len());
        for it in items {
            keyed.push((parse_f64(&it)?, it));
        }
        keyed.sort_by(|x, y| x.0.total_cmp(&y.0).then_with(|| x.1.cmp(&y.1)));
        items = keyed.into_iter().map(|(_, it)| it).collect();
    }
    if desc {
        items.reverse();
    }
    Some(items)
}

#[cfg(test)]
mod tests {
    use super::sorted;

    #[test]
    fn sorts_as_redis_does() {
        let v = |s: &[&str]| s.iter().map(|x| x.as_bytes().to_vec()).collect::<Vec<_>>();
        assert_eq!(
            sorted(v(&["10", "9", "2.5", "-1"]), false, false),
            Some(v(&["-1", "2.5", "9", "10"]))
        );
        assert_eq!(
            sorted(v(&["10", "9", "a"]), true, true),
            Some(v(&["a", "9", "10"]))
        );
        assert_eq!(sorted(v(&["1", "x"]), false, false), None);
    }
}
