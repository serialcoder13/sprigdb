//! The `KeyValue` operations over string keys, for the REST and gRPC API.
//! Structures are reached through `Exec`, which runs a Redis command on
//! the namespace.
//!
//! Each operation is one MariaDB transaction through the caller's
//! [`SqlRunner`]: lock the key row, check the precondition and the type,
//! write with a fresh version, commit.

use sprig_common::{SprigCode, SprigError};

use crate::layout::{KeyType, MAX_KEY_BYTES, Namespace, live};
use crate::sql::{SqlError, SqlRunner, cell_i64, cell_u64, lit};
use crate::time::{NOW_MS_SQL, datetime_lit, ms_sql};

/// A string key's value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The key.
    pub key: Vec<u8>,
    /// Its type (`Scan` lists every type; `Get` only strings).
    pub kind: KeyType,
    /// The value (strings only).
    pub value: Vec<u8>,
    /// The version (`_v`).
    pub version: u64,
    /// Expiry, ms since the epoch.
    pub expires_at_ms: Option<i64>,
}

/// A write's precondition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pre {
    /// None.
    None,
    /// The key's version must be this.
    Version(u64),
    /// The key must be absent.
    Absent,
    /// The key must exist.
    Exists,
}

fn sql(e: &SqlError) -> SprigError {
    SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", &e.message)
}

fn check_key(k: &[u8]) -> Result<(), SprigError> {
    if k.is_empty() || k.len() > MAX_KEY_BYTES {
        return Err(SprigError::new(
            SprigCode::KvInvalidKey,
            "a key is 1 to 1024 bytes",
        ));
    }
    Ok(())
}

/// A fresh version: random, 63 bits, never 0. Versions only need to
/// differ from the key's earlier ones.
fn version() -> u64 {
    let r = getrandom::u64().unwrap_or_else(|_| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64)
    });
    (r & (u64::MAX >> 1)).max(1)
}

/// `k = <key> AND _sprig_dk = <its distribution key>` on alias `p`
/// (`t.` or empty): one shard's rows on a sharded namespace.
#[must_use]
pub fn key_pred(ns: &Namespace, k: &[u8], p: &str) -> String {
    format!(
        "{p}k = {} AND {p}_sprig_dk = {}",
        lit(k),
        lit(ns.dist_key(k))
    )
}

/// The key row: `(type, value, version, g, expiry, now)`.
type KeyRow = (KeyType, Vec<u8>, u64, u64, Option<i64>);

async fn read_row(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
    lock: bool,
) -> Result<(i64, Option<KeyRow>), SprigError> {
    // One table, the key and its distribution key in the condition, so a
    // sharded namespace's router sends it to one shard; an
    // absent key returns no row, and expiries are computed in SQL.
    let rows = s
        .query(&format!(
            "SELECT {NOW_MS_SQL}, t.type, t.v, t._v, t.g, {} FROM {} AS t WHERE {} AND {}{}",
            ms_sql("t.expires_at"),
            ns.keys(),
            key_pred(ns, k, "t."),
            live("t"),
            if lock { " FOR UPDATE" } else { "" }
        ))
        .await
        .map_err(|e| sql(&e))?;
    let Some(r) = rows.into_iter().next() else {
        return Ok((0, None));
    };
    let now = cell_i64(r.first().and_then(Option::as_ref));
    let Some(t) = r.get(1).and_then(|c| c.as_deref()).and_then(KeyType::parse) else {
        return Ok((now, None));
    };
    Ok((
        now,
        Some((
            t,
            r.get(2).cloned().flatten().unwrap_or_default(),
            cell_u64(r.get(3).and_then(Option::as_ref)),
            cell_u64(r.get(4).and_then(Option::as_ref)),
            r.get(5).and_then(Option::as_ref).map(|c| cell_i64(Some(c))),
        )),
    ))
}

/// Reads a live string key.
///
/// # Errors
/// 39723, 39730 for another type, backend errors.
pub async fn get(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
) -> Result<Option<Entry>, SprigError> {
    check_key(k)?;
    let (_, row) = read_row(s, ns, k, false).await?;
    match row {
        None => Ok(None),
        Some((KeyType::String, v, ver, _, exp)) => Ok(Some(Entry {
            key: k.to_vec(),
            kind: KeyType::String,
            value: v,
            version: ver,
            expires_at_ms: exp,
        })),
        Some(_) => Err(SprigError::new(
            SprigCode::KvWrongType,
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

async fn txn<T>(
    s: &mut dyn SqlRunner,
    f: impl for<'a> FnOnce(
        &'a mut dyn SqlRunner,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<T, SprigError>> + Send + 'a>,
    >,
) -> Result<T, SprigError> {
    s.exec("START TRANSACTION").await.map_err(|e| sql(&e))?;
    match f(s).await {
        Ok(v) => {
            s.exec("COMMIT").await.map_err(|e| sql(&e))?;
            Ok(v)
        }
        Err(e) => {
            let _ = s.exec("ROLLBACK").await;
            Err(e)
        }
    }
}

async fn drop_elements(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
    t: KeyType,
    g: u64,
) -> Result<(), SprigError> {
    let table = match t {
        KeyType::String => return Ok(()),
        KeyType::ZSet => ns.zset(),
        _ => ns.elems(),
    };
    s.exec(&format!(
        "DELETE FROM {table} WHERE k = {} AND g = {g}",
        lit(k)
    ))
    .await
    .map_err(|e| sql(&e))?;
    Ok(())
}

/// Writes a string key; returns its new version.
///
/// # Errors
/// 39721 (with the current version), 39722, 39723, 39724, 39730, 404 for
/// `Exists` on a missing key (39720 is the namespace's; a missing key is
/// `KvVersionMismatch` with version 0).
pub async fn put(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
    value: &[u8],
    pre: Pre,
    ttl_ms: Option<i64>,
) -> Result<u64, SprigError> {
    put_at(s, ns, k, value, pre, ttl_ms, None).await
}

/// [`put`] that also sets the key's location `(lat, lng)` in the same
/// transaction (the namespace's location columns must exist,
/// [`crate::geo::ddl`]).
///
/// # Errors
/// As [`put`]; 39765 for a location out of range.
#[allow(clippy::too_many_lines)] // the write's cases, in order
pub async fn put_at(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
    value: &[u8],
    pre: Pre,
    ttl_ms: Option<i64>,
    loc: Option<(f64, f64)>,
) -> Result<u64, SprigError> {
    check_key(k)?;
    if let Some((la, lo)) = loc {
        crate::geo::check(la, lo)?;
    }
    if value.len() > ns.max_value_bytes {
        return Err(SprigError::new(
            SprigCode::KvValueInvalid,
            format!(
                "values are at most {} bytes in this namespace",
                ns.max_value_bytes
            ),
        ));
    }
    let (ns, k, value) = (ns.clone(), k.to_vec(), value.to_vec());
    txn(s, move |s| {
        Box::pin(async move {
            let (now, row) = read_row(s, &ns, &k, true).await?;
            match (pre, &row) {
                (Pre::Absent, Some(_)) => {
                    return Err(SprigError::new(SprigCode::KvKeyExists, "the key exists"));
                }
                (Pre::Exists, None) => {
                    return Err(SprigError::new(SprigCode::KvVersionMismatch, "the key doesn't exist; current version 0"));
                }
                (Pre::Version(v), r) if r.as_ref().map_or(0, |r| r.2) != v => {
                    let cur = r.as_ref().map_or(0, |r| r.2);
                    return Err(SprigError::new(
                        SprigCode::KvVersionMismatch,
                        format!("the key's version is {cur}, not {v}"),
                    ));
                }
                _ => {}
            }
            if let Some((t, ..)) = &row
                && *t != KeyType::String
                && pre != Pre::None
            {
                return Err(SprigError::new(
                    SprigCode::KvWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            let ver = version();
            let exp = ttl_ms.map_or_else(
                || "NULL".to_owned(),
                |t| match row {
                    Some(_) => datetime_lit(now.saturating_add(t)),
                    // No row read, so no clock read: MariaDB's, in SQL.
                    None => format!(
                        "TIMESTAMPADD(MICROSECOND, {} * 1000, UTC_TIMESTAMP(3))",
                        t.clamp(0, crate::time::MAX_MS / 2)
                    ),
                },
            );
            if let Some((t, _, _, g, _)) = row {
                // A collection is replaced: its elements go now.
                let g = if t == KeyType::String {
                    g
                } else {
                    drop_elements(s, &ns, &k, t, g).await?;
                    ver
                };
                s.exec(&format!(
                    "UPDATE {} SET type = 'string', v = {}, n = 0, lh = 0, lt = 0, g = {g}, _v = {ver}, expires_at = {exp} WHERE {}",
                    ns.keys(),
                    lit(&value),
                    key_pred(&ns, &k, "")
                ))
                .await
                .map_err(|e| sql(&e))?;
            } else {
                // An expired row may still be there: replace it.
                s.exec(&format!(
                    "DELETE FROM {} WHERE {}",
                    ns.keys(),
                    key_pred(&ns, &k, "")
                ))
                .await
                .map_err(|e| sql(&e))?;
                s.exec(&format!(
                    "INSERT INTO {} (k, type, v, n, lh, lt, g, _v, expires_at, _sprig_dk, _sprig_rk) \
                     VALUES ({}, 'string', {}, 0, 0, 0, {ver}, {ver}, {exp}, {}, {})",
                    ns.keys(),
                    lit(&k),
                    lit(&value),
                    lit(ns.dist_key(&k)),
                    ns.routing_hash(&k)
                ))
                .await
                .map_err(|e| sql(&e))?;
            }
            if loc.is_some() {
                s.exec(&format!(
                    "UPDATE {} SET {} WHERE {}",
                    ns.keys(),
                    crate::geo::set_sql(loc),
                    key_pred(&ns, &k, "")
                ))
                .await
                .map_err(|e| sql(&e))?;
            }
            Ok(ver)
        })
    })
    .await
}

/// Deletes a key of any type; `false` when it was absent.
///
/// # Errors
/// 39721 when `if_version` doesn't match; backend errors.
pub async fn delete(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    k: &[u8],
    if_version: Option<u64>,
) -> Result<bool, SprigError> {
    check_key(k)?;
    let (ns, k) = (ns.clone(), k.to_vec());
    txn(s, move |s| {
        Box::pin(async move {
            let (_, row) = read_row(s, &ns, &k, true).await?;
            if let Some(v) = if_version {
                let cur = row.as_ref().map_or(0, |r| r.2);
                if cur != v {
                    return Err(SprigError::new(
                        SprigCode::KvVersionMismatch,
                        format!("the key's version is {cur}, not {v}"),
                    ));
                }
            }
            let Some((t, _, _, g, _)) = row else {
                return Ok(false);
            };
            s.exec(&format!(
                "DELETE FROM {} WHERE {}",
                ns.keys(),
                key_pred(&ns, &k, "")
            ))
            .await
            .map_err(|e| sql(&e))?;
            drop_elements(s, &ns, &k, t, g).await?;
            Ok(true)
        })
    })
    .await
}

/// A page of keys in key order: those starting with `prefix`, after
/// `after`, at most `limit`; the next page's `after`, if any.
///
/// # Errors
/// Backend errors.
pub async fn scan(
    s: &mut dyn SqlRunner,
    ns: &Namespace,
    prefix: &[u8],
    after: &[u8],
    limit: usize,
) -> Result<(Vec<Entry>, Option<Vec<u8>>), SprigError> {
    let mut cond = vec![live("")];
    if !prefix.is_empty() {
        cond.push(format!("k >= {}", lit(prefix)));
        let mut end = prefix.to_vec();
        while let Some(b) = end.pop() {
            if b < 0xFF {
                end.push(b + 1);
                cond.push(format!("k < {}", lit(&end)));
                break;
            }
        }
    }
    if !after.is_empty() {
        cond.push(format!("k > {}", lit(after)));
    }
    let limit = limit.clamp(1, 1000);
    let rows = s
        .query(&format!(
            "SELECT k, type, v, _v, {} FROM {} WHERE {} ORDER BY k LIMIT {}",
            ms_sql("expires_at"),
            ns.keys(),
            cond.join(" AND "),
            limit + 1
        ))
        .await
        .map_err(|e| sql(&e))?;
    let more = rows.len() > limit;
    let entries: Vec<Entry> = rows
        .into_iter()
        .take(limit)
        .map(|r| Entry {
            key: r.first().cloned().flatten().unwrap_or_default(),
            kind: r
                .get(1)
                .and_then(|c| c.as_deref())
                .and_then(KeyType::parse)
                .unwrap_or(KeyType::String),
            value: r.get(2).cloned().flatten().unwrap_or_default(),
            version: cell_u64(r.get(3).and_then(Option::as_ref)),
            expires_at_ms: r.get(4).and_then(Option::as_ref).map(|c| cell_i64(Some(c))),
        })
        .collect();
    let next = if more {
        entries.last().map(|e| e.key.clone())
    } else {
        None
    };
    Ok((entries, next))
}

/// Read-only SQL relations over a namespace's structures:
/// `<ns>__hash`, `<ns>__list`, `<ns>__set`
/// and `<ns>__zset`, joined to live key rows.
#[must_use]
pub fn view_ddl(ns: &Namespace) -> Vec<String> {
    let db = crate::sql::ident(&ns.database);
    let v = |suffix: &str| {
        format!(
            "{db}.{}",
            crate::sql::ident(&format!("{}__{suffix}", ns.table))
        )
    };
    let k = ns.keys();
    let lv = live("t");
    vec![
        format!(
            "CREATE OR REPLACE SQL SECURITY INVOKER VIEW {} AS SELECT t.k AS k, e.e AS field, e.v AS value \
             FROM {k} AS t JOIN {} AS e ON e.k = t.k AND e.g = t.g WHERE t.type = 'hash' AND {lv}",
            v("hash"),
            ns.elems()
        ),
        format!(
            "CREATE OR REPLACE SQL SECURITY INVOKER VIEW {} AS SELECT t.k AS k, e.o - t.lh AS idx, e.v AS item \
             FROM {k} AS t JOIN {} AS e ON e.k = t.k AND e.g = t.g WHERE t.type = 'list' AND {lv}",
            v("list"),
            ns.elems()
        ),
        format!(
            "CREATE OR REPLACE SQL SECURITY INVOKER VIEW {} AS SELECT t.k AS k, e.e AS member \
             FROM {k} AS t JOIN {} AS e ON e.k = t.k AND e.g = t.g WHERE t.type = 'set' AND {lv}",
            v("set"),
            ns.elems()
        ),
        format!(
            "CREATE OR REPLACE SQL SECURITY INVOKER VIEW {} AS SELECT t.k AS k, z.m AS member, \
             IF(z.c = 0, z.score, NULL) AS score, z.c AS score_class \
             FROM {k} AS t JOIN {} AS z ON z.k = t.k AND z.g = t.g WHERE t.type = 'zset' AND {lv}",
            v("zset"),
            ns.zset()
        ),
    ]
}
