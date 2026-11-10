//! One-packet guarded batches (chapter 40, "Guarded batches"; R6 round 3,
//! decision R23).
//!
//! The hottest write commands run as one compound statement
//! (`BEGIN NOT ATOMIC ... END`): one round trip to MariaDB that starts a
//! transaction, locks the key row, clears an expired key, checks the type,
//! writes and commits, and returns the reply's source as one result row.
//! Everything else keeps the statement-by-statement path in [`crate::cmd`],
//! and so do these commands when an argument needs that path's error
//! reply (an odd `HSET`, a bad `SET` option, a repeated `ZADD` member,
//! `ZADD INCR`, a value over the namespace's limit...).
//!
//! The prologue every batch shares:
//!
//! ```sql
//! DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RESIGNAL; END;
//! START TRANSACTION;
//! SELECT now_ms, t.type, t.v, IFNULL(t.n,0), IFNULL(t.lh,0), IFNULL(t.lt,0), t.g,
//!        t.expires_at, (t.expires_at IS NOT NULL AND t.expires_at <= UTC_TIMESTAMP(3))
//!   INTO @now, @t, @sv, @n, @lh, @lt, @g, @e, @x
//!   FROM (SELECT 1 AS one) AS d LEFT JOIN <keys> AS t
//!     ON t.k = :k AND t._sprig_dk = :dk FOR UPDATE;
//! IF @x THEN <delete the key row and its elements>; <reset to absent>; END IF;
//! ```
//!
//! The left join always returns one row, so every variable is assigned
//! (`NULL` for an absent key) and MariaDB raises no "no data" warning.
//! Later statements are guarded by `@rc` (0 = go, 1 = nothing to do,
//! 2 = `WRONGTYPE`, 3 = not an integer, 4 = overflow), so a refused
//! command changes nothing. A new key is a plain `INSERT`: two clients
//! creating one key meet on its primary key (1062) and the loser reruns
//! the whole batch, as on the statement path. On any error the exit
//! handler rolls back and re-raises the error (the caller still sends
//! `ROLLBACK`, a no-op). On a sharded database the gateway routes the
//! statement by its `_sprig_dk` literal (`sprig_router::compound`).

// The batches are SQL text built piece by piece.
#![allow(clippy::format_push_string)]

use bytes::Bytes;
use sprig_kv::layout::{KeyType, MAX_ELEMENT_BYTES, MAX_KEY_BYTES, Namespace};
use sprig_kv::sql::{cell_i64, lit};
use sprig_kv::time::{MAX_MS, NOW_MS_SQL, datetime_lit};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{parse_f64, parse_i64};

use crate::ctx::{crc, new_version, upper};
use crate::reply;

/// Most items one batch carries; more take the statement path.
const MAX_ITEMS: usize = 200;
/// Most argument bytes one batch carries (the packet stays small).
const MAX_BYTES: usize = 1 << 20;

/// Which end of a list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// Head.
    Left,
    /// Tail.
    Right,
}

/// `SET`'s expiry option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ttl {
    /// None: the key loses any expiry.
    Unset,
    /// Relative to MariaDB's clock, ms.
    In(i64),
    /// Absolute, ms since the epoch.
    At(i64),
    /// `KEEPTTL`.
    Keep,
}

/// A command the batch path runs.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// `SET k v [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]`.
    Set {
        /// The key.
        k: Bytes,
        /// The value.
        v: Bytes,
        /// `NX`.
        nx: bool,
        /// `XX`.
        xx: bool,
        /// `GET`.
        get: bool,
        /// Expiry.
        ttl: TtlOpt,
    },
    /// `INCR`, `DECR`, `INCRBY`, `DECRBY`.
    Incr {
        /// The key.
        k: Bytes,
        /// The increment.
        by: i64,
    },
    /// `HSET` (`hmset`: reply `OK`).
    Hset {
        /// The key.
        k: Bytes,
        /// Distinct fields, last value wins.
        pairs: Vec<(Bytes, Bytes)>,
        /// `HMSET`.
        hmset: bool,
    },
    /// `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`.
    Push {
        /// The key.
        k: Bytes,
        /// Items in the order sent.
        items: Vec<Bytes>,
        /// Which end.
        end: End,
        /// Only an existing list (`*PUSHX`).
        only_existing: bool,
    },
    /// `LPOP k [count]`, `RPOP k [count]`.
    Pop {
        /// The key.
        k: Bytes,
        /// `count`, when given.
        count: Option<i64>,
        /// Which end.
        end: End,
    },
    /// `ZADD k [NX|XX] [GT|LT] [CH] s m ...` (not `INCR`).
    Zadd {
        /// The key.
        k: Bytes,
        /// Distinct members with their scores.
        pairs: Vec<(f64, Bytes)>,
        /// `NX`.
        nx: bool,
        /// `XX`.
        xx: bool,
        /// `GT`.
        gt: bool,
        /// `LT`.
        lt: bool,
        /// `CH`.
        ch: bool,
    },
}

/// `SET`'s expiry, public for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlOpt(Ttl);

/// An expiry amount as the statement path accepts it; `None` sends the
/// command there (for its error reply).
fn ttl_ms(a: &[u8], unit: i64) -> Option<i64> {
    let v = parse_i64(a)?;
    if v <= 0 {
        return None;
    }
    v.checked_mul(unit).filter(|ms| *ms <= i64::MAX / 2)
}

fn key_ok(k: &[u8]) -> bool {
    k.len() <= MAX_KEY_BYTES
}

/// The batch for `name` (lower case) with arguments `a` (`a[0]` the name),
/// when the batch path runs it; `None` sends it to the statement path.
#[must_use]
pub fn plan(ns: &Namespace, name: &str, a: &[Bytes]) -> Option<Plan> {
    let k = a.get(1)?;
    if !key_ok(k) || a.iter().map(Bytes::len).sum::<usize>() > MAX_BYTES {
        return None;
    }
    let max_v = ns.max_value_bytes;
    match name {
        "set" => {
            let v = a.get(2)?;
            if v.len() > max_v {
                return None;
            }
            let (mut nx, mut xx, mut get) = (false, false, false);
            let mut ttl = Ttl::Unset;
            let mut i = 3;
            while i < a.len() {
                let o = upper(&a[i]);
                let next = a.get(i + 1);
                match o.as_str() {
                    "NX" if !xx => nx = true,
                    "XX" if !nx => xx = true,
                    "GET" => get = true,
                    "KEEPTTL" if ttl == Ttl::Unset => ttl = Ttl::Keep,
                    "EX" | "PX" | "EXAT" | "PXAT" if ttl == Ttl::Unset => {
                        let n = next?;
                        ttl = match o.as_str() {
                            "EX" => Ttl::In(ttl_ms(n, 1000)?),
                            "PX" => Ttl::In(ttl_ms(n, 1)?),
                            "EXAT" => Ttl::At(ttl_ms(n, 1000)?),
                            _ => Ttl::At(ttl_ms(n, 1)?),
                        };
                        i += 1;
                    }
                    _ => return None,
                }
                i += 1;
            }
            Some(Plan::Set {
                k: k.clone(),
                v: v.clone(),
                nx,
                xx,
                get,
                ttl: TtlOpt(ttl),
            })
        }
        "incr" | "decr" if a.len() == 2 => Some(Plan::Incr {
            k: k.clone(),
            by: if name == "incr" { 1 } else { -1 },
        }),
        "incrby" | "decrby" if a.len() == 3 => {
            let by = parse_i64(&a[2])?;
            let by = if name == "incrby" {
                by
            } else {
                by.checked_neg()?
            };
            Some(Plan::Incr { k: k.clone(), by })
        }
        "hset" | "hmset" => {
            let rest = &a[2..];
            if rest.is_empty() || !rest.len().is_multiple_of(2) || rest.len() / 2 > MAX_ITEMS {
                return None;
            }
            let mut pairs: Vec<(Bytes, Bytes)> = Vec::with_capacity(rest.len() / 2);
            // Last value wins, in first-seen order (the statement path's
            // `dedup_last` keeps last-seen order; the order of distinct
            // fields doesn't change the result).
            for c in rest.chunks(2) {
                if c[0].len() > MAX_ELEMENT_BYTES || c[1].len() > max_v {
                    return None;
                }
                if let Some(p) = pairs.iter_mut().find(|p| p.0 == c[0]) {
                    p.1 = c[1].clone();
                } else {
                    pairs.push((c[0].clone(), c[1].clone()));
                }
            }
            Some(Plan::Hset {
                k: k.clone(),
                pairs,
                hmset: name == "hmset",
            })
        }
        "lpush" | "rpush" | "lpushx" | "rpushx" => {
            let items = &a[2..];
            if items.is_empty() || items.len() > MAX_ITEMS || items.iter().any(|v| v.len() > max_v)
            {
                return None;
            }
            Some(Plan::Push {
                k: k.clone(),
                items: items.to_vec(),
                end: if name.starts_with('l') {
                    End::Left
                } else {
                    End::Right
                },
                only_existing: name.ends_with('x'),
            })
        }
        "lpop" | "rpop" => {
            if a.len() > 3 {
                return None;
            }
            let count = match a.get(2) {
                None => None,
                Some(c) => match parse_i64(c) {
                    Some(n) if n >= 0 => Some(n),
                    _ => return None,
                },
            };
            Some(Plan::Pop {
                k: k.clone(),
                count,
                end: if name == "lpop" {
                    End::Left
                } else {
                    End::Right
                },
            })
        }
        "zadd" => {
            let (mut nx, mut xx, mut gt, mut lt, mut ch) = (false, false, false, false, false);
            let mut i = 2;
            while i < a.len() {
                match upper(&a[i]).as_str() {
                    "NX" => nx = true,
                    "XX" => xx = true,
                    "GT" => gt = true,
                    "LT" => lt = true,
                    "CH" => ch = true,
                    "INCR" => return None,
                    _ => break,
                }
                i += 1;
            }
            let rest = &a[i..];
            if rest.is_empty()
                || !rest.len().is_multiple_of(2)
                || rest.len() / 2 > MAX_ITEMS
                || (nx && xx)
                || (nx && (gt || lt))
                || (gt && lt)
            {
                return None;
            }
            let mut pairs: Vec<(f64, Bytes)> = Vec::with_capacity(rest.len() / 2);
            for c in rest.chunks(2) {
                let s = parse_f64(&c[0])?;
                if c[1].len() > MAX_ELEMENT_BYTES || pairs.iter().any(|p| p.1 == c[1]) {
                    return None;
                }
                pairs.push((s, c[1].clone()));
            }
            Some(Plan::Zadd {
                k: k.clone(),
                pairs,
                nx,
                xx,
                gt,
                lt,
                ch,
            })
        }
        _ => None,
    }
}

/// The prologue: lock the key row, clear an expired key. `sv`: read the
/// string value into `@sv`.
fn prologue(ns: &Namespace, key: &[u8], sv: bool) -> String {
    let keys = ns.keys();
    let k = lit(key);
    // `_sprig_dk` names the key's shard on a sharded database: the gateway
    // routes the whole compound statement by it (R6 round 4).
    let dk = lit(ns.dist_key(key));
    format!(
        "START TRANSACTION; \
         SELECT {NOW_MS_SQL}, t.type, {v}, IFNULL(t.n, 0), IFNULL(t.lh, 0), IFNULL(t.lt, 0), t.g, t.expires_at, \
         (t.expires_at IS NOT NULL AND t.expires_at <= UTC_TIMESTAMP(3)) \
         INTO @now, @t, @sv, @n, @lh, @lt, @g, @e, @x \
         FROM (SELECT 1 AS one) AS d LEFT JOIN {keys} AS t ON t.k = {k} AND t._sprig_dk = {dk} FOR UPDATE; \
         IF @x THEN DELETE FROM {keys} WHERE k = {k}; \
         IF @t = 'zset' THEN DELETE FROM {zs} WHERE k = {k} AND g = @g; \
         ELSEIF @t <> 'string' THEN DELETE FROM {el} WHERE k = {k} AND g = @g; END IF; \
         SET @t = NULL, @sv = NULL, @n = 0, @lh = 0, @lt = 0, @e = NULL; END IF; \
         SET @rc = 0; ",
        v = if sv { "t.v" } else { "NULL" },
        zs = ns.zset(),
        el = ns.elems(),
    )
}

/// Deletes the locked key row and its elements (any type).
fn drop_key(ns: &Namespace, k: &str) -> String {
    format!(
        "DELETE FROM {keys} WHERE k = {k}; \
         IF @t = 'zset' THEN DELETE FROM {zs} WHERE k = {k} AND g = @g; \
         ELSEIF @t <> 'string' THEN DELETE FROM {el} WHERE k = {k} AND g = @g; END IF; ",
        keys = ns.keys(),
        zs = ns.zset(),
        el = ns.elems(),
    )
}

/// A new key row of type `t`; `g` from `@g`, `_v` = `ver`.
fn insert_key(
    ns: &Namespace,
    key: &[u8],
    t: KeyType,
    v: &str,
    n: &str,
    exp: &str,
    ver: u64,
) -> String {
    format!(
        "INSERT INTO {} (k, type, v, n, lh, lt, g, _v, expires_at, _sprig_dk, _sprig_rk) \
         VALUES ({}, '{}', {v}, {n}, @lh, @lt, @g, {ver}, {exp}, {}, {}); ",
        ns.keys(),
        lit(key),
        t.as_str(),
        lit(ns.dist_key(key)),
        ns.routing_hash(key)
    )
}

impl Plan {
    /// The key.
    #[must_use]
    pub fn key(&self) -> &Bytes {
        match self {
            Self::Set { k, .. }
            | Self::Incr { k, .. }
            | Self::Hset { k, .. }
            | Self::Push { k, .. }
            | Self::Pop { k, .. }
            | Self::Zadd { k, .. } => k,
        }
    }

    /// The compound statement, with a fresh version (build it again for
    /// each attempt).
    #[must_use]
    pub fn sql(&self, ns: &Namespace) -> String {
        let ver = new_version();
        let key = self.key();
        let k = lit(key);
        let keys = ns.keys();
        let mut s = String::with_capacity(1024);
        // An error rolls the transaction back inside the statement, so no
        // connection is left with it open (the shard's connection may not
        // be the session's own, R6 round 4); RESIGNAL keeps the error.
        s.push_str(
            "BEGIN NOT ATOMIC DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RESIGNAL; END; ",
        );
        match self {
            Self::Set {
                v,
                nx,
                xx,
                get,
                ttl,
                ..
            } => {
                s.push_str(&prologue(ns, key, *get));
                let go = if *nx {
                    "@t IS NULL"
                } else if *xx {
                    "@t IS NOT NULL"
                } else {
                    "TRUE"
                };
                let exp = match ttl.0 {
                    Ttl::Unset => "NULL".to_owned(),
                    Ttl::Keep => "@e".to_owned(),
                    Ttl::At(ms) => datetime_lit(ms),
                    Ttl::In(ms) => format!(
                        "TIMESTAMPADD(MICROSECOND, LEAST(@now + {ms}, {MAX_MS}) * 1000, '1970-01-01 00:00:00')"
                    ),
                };
                let vl = lit(v);
                s.push_str(&format!(
                    "IF {get} AND @t IS NOT NULL AND @t <> 'string' THEN SET @rc = 2; \
                     ELSEIF NOT ({go}) THEN SET @rc = 1; END IF; \
                     IF @rc = 0 THEN \
                     IF @t = 'string' THEN UPDATE {keys} SET v = {vl}, n = 0, lh = 0, lt = 0, _v = {ver}, expires_at = {exp} WHERE k = {k}; \
                     ELSE IF @t IS NOT NULL THEN {drop} END IF; \
                     SET @g = {ver}, @lh = 0, @lt = 0; {ins}END IF; END IF; \
                     COMMIT; SELECT @rc, IF(@t = 'string', @sv, NULL); END",
                    get = if *get { "TRUE" } else { "FALSE" },
                    drop = drop_key(ns, &k),
                    ins = insert_key(ns, key, KeyType::String, &vl, "0", &exp, ver),
                ));
            }
            Self::Incr { by, .. } => {
                s.push_str(&prologue(ns, key, true));
                s.push_str(&format!(
                    "SET @r = NULL; \
                     IF @t IS NOT NULL AND @t <> 'string' THEN SET @rc = 2; \
                     ELSEIF @sv IS NOT NULL AND NOT (@sv = '0' OR (LENGTH(@sv) <= 20 AND LOCATE(X'0A', @sv) = 0 \
                     AND @sv REGEXP '^-?[1-9][0-9]*$' \
                     AND CAST(@sv AS DECIMAL(25,0)) BETWEEN -9223372036854775808 AND 9223372036854775807)) THEN SET @rc = 3; \
                     ELSE SET @r = CAST(IFNULL(@sv, '0') AS DECIMAL(25,0)) + {by}; \
                     IF @r < -9223372036854775808 OR @r > 9223372036854775807 THEN SET @rc = 4; END IF; END IF; \
                     IF @rc = 0 THEN \
                     IF @t = 'string' THEN UPDATE {keys} SET v = CAST(CAST(@r AS SIGNED) AS CHAR), n = 0, lh = 0, lt = 0, _v = {ver} WHERE k = {k}; \
                     ELSE SET @g = {ver}, @lh = 0, @lt = 0; {ins}END IF; END IF; \
                     COMMIT; SELECT @rc, CAST(@r AS SIGNED); END",
                    ins = insert_key(
                        ns,
                        key,
                        KeyType::String,
                        "CAST(CAST(@r AS SIGNED) AS CHAR)",
                        "0",
                        "NULL",
                        ver
                    ),
                ));
            }
            Self::Hset { pairs, .. } => {
                s.push_str(&prologue(ns, key, false));
                let pred = pairs
                    .iter()
                    .map(|(f, _)| format!("(o = {} AND e = {})", crc(f), lit(f)))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let route = format!("{}, {}", lit(ns.dist_key(key)), ns.routing_hash(key));
                let vals = pairs
                    .iter()
                    .map(|(f, v)| format!("({k}, @g, {}, {}, {}, {route})", crc(f), lit(f), lit(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                s.push_str(&format!(
                    "SET @add = 0; \
                     IF @t IS NOT NULL AND @t <> 'hash' THEN SET @rc = 2; \
                     ELSE \
                     IF @t IS NULL THEN SET @g = {ver}, @lh = 0, @lt = 0, @have = 0; \
                     ELSE SELECT COUNT(*) INTO @have FROM {el} WHERE k = {k} AND g = @g AND ({pred}); END IF; \
                     INSERT INTO {el} (k, g, o, e, v, _sprig_dk, _sprig_rk) VALUES {vals} ON DUPLICATE KEY UPDATE v = VALUES(v); \
                     SET @add = {nf} - @have; \
                     IF @t IS NULL THEN {ins}\
                     ELSE UPDATE {keys} SET n = n + @add, _v = {ver} WHERE k = {k}; END IF; \
                     END IF; \
                     COMMIT; SELECT @rc, @add; END",
                    el = ns.elems(),
                    nf = pairs.len(),
                    ins = insert_key(ns, key, KeyType::Hash, "NULL", "@add", "NULL", ver),
                ));
            }
            Self::Push {
                items,
                end,
                only_existing,
                ..
            } => {
                s.push_str(&prologue(ns, key, false));
                let route = format!("{}, {}", lit(ns.dist_key(key)), ns.routing_hash(key));
                let m = items.len();
                let vals = items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let o = match end {
                            End::Left => format!("@lh - {}", i + 1),
                            End::Right => format!("@lt + {i}"),
                        };
                        format!("({k}, @g, {o}, '', {}, {route})", lit(v))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let bounds = match end {
                    End::Left => format!("@lh = @lh - {m}"),
                    End::Right => format!("@lt = @lt + {m}"),
                };
                s.push_str(&format!(
                    "IF @t IS NOT NULL AND @t <> 'list' THEN SET @rc = 2; \
                     ELSEIF @t IS NULL AND {ox} THEN SET @rc = 1; \
                     ELSE \
                     IF @t IS NULL THEN SET @g = {ver}, @lh = 0, @lt = 0, @n = 0; END IF; \
                     INSERT INTO {el} (k, g, o, e, v, _sprig_dk, _sprig_rk) VALUES {vals}; \
                     SET {bounds}, @n = @n + {m}; \
                     IF @t IS NULL THEN {ins}\
                     ELSE UPDATE {keys} SET n = @n, lh = @lh, lt = @lt, _v = {ver} WHERE k = {k}; END IF; \
                     END IF; \
                     COMMIT; SELECT @rc, @n; END",
                    ox = if *only_existing { "TRUE" } else { "FALSE" },
                    el = ns.elems(),
                    ins = insert_key(ns, key, KeyType::List, "NULL", "@n", "NULL", ver),
                ));
            }
            Self::Pop { count, end, .. } => {
                s.push_str(&prologue(ns, key, false));
                let c = count.unwrap_or(1);
                let (range, order, bounds) = match end {
                    End::Left => ("@lo = @lh, @hi = @lh + @c", "ASC", "@lh = @lh + @c"),
                    End::Right => ("@lo = @lt - @c, @hi = @lt", "DESC", "@lt = @lt - @c"),
                };
                let el = ns.elems();
                s.push_str(&format!(
                    "SET @c = 0, @lo = 0, @hi = 0; \
                     IF @t IS NOT NULL AND @t <> 'list' THEN SET @rc = 2; \
                     ELSEIF @t IS NULL THEN SET @rc = 1; \
                     ELSE SET @c = GREATEST(LEAST({c}, @n), 0); SET {range}; END IF; \
                     SELECT @rc, x.v FROM (SELECT 1 AS one) AS d LEFT JOIN {el} AS x \
                     ON x.k = {k} AND x.g = @g AND x.o >= @lo AND x.o < @hi ORDER BY x.o {order}; \
                     IF @c > 0 THEN \
                     DELETE FROM {el} WHERE k = {k} AND g = @g AND o >= @lo AND o < @hi; \
                     SET @n = @n - @c, {bounds}; \
                     IF @n <= 0 THEN DELETE FROM {keys} WHERE k = {k}; DELETE FROM {el} WHERE k = {k} AND g = @g; \
                     ELSE UPDATE {keys} SET n = @n, lh = @lh, lt = @lt, _v = {ver} WHERE k = {k}; END IF; \
                     END IF; \
                     COMMIT; END",
                ));
            }
            Self::Zadd {
                pairs,
                nx,
                xx,
                gt,
                lt,
                ..
            } => {
                s.push_str(&prologue(ns, key, false));
                let route = format!("{}, {}", lit(ns.dist_key(key)), ns.routing_hash(key));
                let derived = pairs
                    .iter()
                    .map(|(sc, m)| {
                        let (c, sv) = if *sc == f64::NEG_INFINITY {
                            (-1, "0e0".to_owned())
                        } else if *sc == f64::INFINITY {
                            (1, "0e0".to_owned())
                        } else {
                            (0, format!("{sc:e}"))
                        };
                        format!(
                            "SELECT {} AS o, {} AS m, {c} AS c, {sv} AS s",
                            crc(m),
                            lit(m)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" UNION ALL ");
                let chg = if *nx {
                    "FALSE"
                } else if *gt {
                    "(x.c, x.s) > (z.c, z.score)"
                } else if *lt {
                    "(x.c, x.s) < (z.c, z.score)"
                } else {
                    "(x.c, x.s) <> (z.c, z.score)"
                };
                let zs = ns.zset();
                let join = format!(
                    "FROM ({derived}) AS x LEFT JOIN {zs} AS z ON z.k = {k} AND z.g = @g AND z.o = x.o AND z.m = x.m"
                );
                s.push_str(&format!(
                    "SET @add = 0, @chg = 0; \
                     IF @t IS NOT NULL AND @t <> 'zset' THEN SET @rc = 2; \
                     ELSEIF @t IS NULL AND {xxs} THEN SET @rc = 0; \
                     ELSE \
                     IF @t IS NULL THEN SET @g = {ver}, @lh = 0, @lt = 0; END IF; \
                     SELECT COUNT(z.m), IFNULL(SUM(z.m IS NOT NULL AND {chg}), 0) INTO @have, @chg {join}; \
                     SET @add = IF({xxs}, 0, {np} - @have); \
                     IF @add + @chg > 0 THEN \
                     INSERT INTO {zs} (k, g, o, m, c, score, _sprig_dk, _sprig_rk) \
                     SELECT {k}, @g, x.o, x.m, x.c, x.s, {route} {join} \
                     WHERE (z.m IS NULL AND NOT {xxs}) OR (z.m IS NOT NULL AND {chg}) \
                     ON DUPLICATE KEY UPDATE c = VALUES(c), score = VALUES(score); \
                     IF @t IS NULL THEN {ins}\
                     ELSE UPDATE {keys} SET n = n + @add, _v = {ver} WHERE k = {k}; END IF; \
                     END IF; \
                     END IF; \
                     COMMIT; SELECT @rc, @add, @chg; END",
                    xxs = if *xx { "TRUE" } else { "FALSE" },
                    np = pairs.len(),
                    ins = insert_key(ns, key, KeyType::ZSet, "NULL", "@add", "NULL", ver),
                ));
            }
        }
        s
    }

    /// The reply from the batch's rows, and whether the command pushed to
    /// the key (wakes blocked clients). `None`: the rows aren't what the
    /// batch returns (a backend surprise; the caller reports it).
    #[must_use]
    pub fn reply(&self, rows: &[Row]) -> Option<(Frame, bool)> {
        let first = rows.first()?;
        let rc = cell_i64(first.first()?.as_ref());
        let col = |i: usize| first.get(i).and_then(Clone::clone);
        match rc {
            2 => return Some((reply::wrongtype(), false)),
            3 => return Some((reply::not_int(), false)),
            4 => {
                return Some((Frame::err("increment or decrement would overflow"), false));
            }
            _ => {}
        }
        Some(match self {
            Self::Set { get, .. } => {
                let f = if *get {
                    Frame::opt_bulk(col(1))
                } else if rc == 0 {
                    Frame::ok()
                } else {
                    Frame::null()
                };
                (f, false)
            }
            Self::Incr { .. } => (Frame::Integer(parse_i64(&col(1)?)?), false),
            Self::Hset { hmset, .. } => {
                if *hmset {
                    (Frame::ok(), false)
                } else {
                    (Frame::Integer(cell_i64(col(1).as_ref())), false)
                }
            }
            Self::Push { .. } => {
                if rc == 1 {
                    (Frame::Integer(0), false)
                } else {
                    (Frame::Integer(cell_i64(col(1).as_ref())), true)
                }
            }
            Self::Pop { count, .. } => {
                if rc == 1 {
                    let f = if count.is_some() {
                        Frame::null_array()
                    } else {
                        Frame::null()
                    };
                    return Some((f, false));
                }
                let items: Vec<Vec<u8>> = rows
                    .iter()
                    .filter_map(|r| r.get(1).and_then(Clone::clone))
                    .collect();
                let f = match count {
                    None => items
                        .into_iter()
                        .next()
                        .map_or(Frame::null(), |v| Frame::Bulk(v.into())),
                    Some(_) => {
                        Frame::Array(items.into_iter().map(|v| Frame::Bulk(v.into())).collect())
                    }
                };
                (f, false)
            }
            Self::Zadd { ch, .. } => {
                let add = cell_i64(col(1).as_ref());
                let chg = cell_i64(col(2).as_ref());
                (Frame::Integer(if *ch { add + chg } else { add }), add > 0)
            }
        })
    }
}

/// Runs `plan` once on `sql` (the caller retries): the reply and whether
/// it pushed. On an error the transaction is rolled back.
///
/// # Errors
/// The backend's; an unexpected result is reported as an error without a
/// number (not retried).
pub async fn run_once(
    sql: &mut dyn SqlRunner,
    ns: &Namespace,
    plan: &Plan,
) -> Result<(Frame, bool), SqlError> {
    let text = plan.sql(ns);
    match sql.query(&text).await {
        Ok(rows) => plan.reply(&rows).ok_or_else(|| SqlError {
            errno: None,
            message: "the guarded batch returned an unexpected result".into(),
        }),
        Err(e) => {
            let _ = sql.exec("ROLLBACK").await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<Bytes> {
        v.iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    #[test]
    fn plans_only_well_formed_commands() {
        let ns = Namespace::redis("redis", 0);
        assert!(plan(&ns, "set", &args(&["set", "k", "v", "EX", "10"])).is_some());
        assert!(plan(&ns, "set", &args(&["set", "k", "v", "EX", "0"])).is_none());
        assert!(plan(&ns, "set", &args(&["set", "k", "v", "NX", "XX"])).is_none());
        assert!(plan(&ns, "hset", &args(&["hset", "k", "f"])).is_none());
        assert!(plan(&ns, "zadd", &args(&["zadd", "k", "1", "a", "2", "a"])).is_none());
        assert!(plan(&ns, "zadd", &args(&["zadd", "k", "INCR", "1", "a"])).is_none());
        assert!(
            plan(
                &ns,
                "decrby",
                &args(&["decrby", "k", "-9223372036854775808"])
            )
            .is_none()
        );
        assert!(plan(&ns, "lpop", &args(&["lpop", "k", "-1"])).is_none());
        assert_eq!(
            plan(
                &ns,
                "hset",
                &args(&["hset", "k", "f", "1", "g", "2", "f", "3"])
            ),
            Some(Plan::Hset {
                k: Bytes::from_static(b"k"),
                pairs: vec![
                    (Bytes::from_static(b"f"), Bytes::from_static(b"3")),
                    (Bytes::from_static(b"g"), Bytes::from_static(b"2"))
                ],
                hmset: false
            })
        );
    }

    #[test]
    fn one_compound_statement() {
        let ns = Namespace::redis("redis", 0);
        for a in [
            args(&["set", "k", "v", "GET", "PX", "100"]),
            args(&["incrby", "k", "5"]),
            args(&["hset", "k", "f", "1"]),
            args(&["rpush", "k", "a", "b"]),
            args(&["lpop", "k", "2"]),
            args(&["zadd", "k", "CH", "1", "a", "+inf", "b"]),
        ] {
            let name = String::from_utf8_lossy(&a[0]).into_owned();
            let p = plan(&ns, &name, &a).unwrap();
            let s = p.sql(&ns);
            assert!(
                s.starts_with(
                    "BEGIN NOT ATOMIC DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RESIGNAL; END; START TRANSACTION;"
                ),
                "{s}"
            );
            assert!(s.ends_with(" END"), "{s}");
            assert!(s.contains("COMMIT;"), "{s}");
            assert!(s.is_ascii());
        }
    }

    #[test]
    fn replies() {
        let ns = Namespace::redis("redis", 0);
        let p = plan(&ns, "incr", &args(&["incr", "k"])).unwrap();
        let row = |v: &[Option<&str>]| -> Row {
            v.iter().map(|c| c.map(|s| s.as_bytes().to_vec())).collect()
        };
        assert_eq!(
            p.reply(&[row(&[Some("0"), Some("7")])]),
            Some((Frame::Integer(7), false))
        );
        assert_eq!(
            p.reply(&[row(&[Some("2"), None])]).map(|r| r.0),
            Some(reply::wrongtype())
        );
        let p = plan(&ns, "rpop", &args(&["rpop", "k", "3"])).unwrap();
        assert_eq!(
            p.reply(&[row(&[Some("0"), Some("c")]), row(&[Some("0"), Some("b")])])
                .map(|r| r.0),
            Some(Frame::Array(vec![Frame::bulk("c"), Frame::bulk("b")]))
        );
        assert_eq!(
            p.reply(&[row(&[Some("1"), None])]).map(|r| r.0),
            Some(Frame::null_array())
        );
        assert_eq!(
            p.reply(&[row(&[Some("0"), None])]).map(|r| r.0),
            Some(Frame::Array(vec![]))
        );
    }
}
