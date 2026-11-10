//! A command's view of one Redis database (chapter 40, "Guarded batches";
//! chapter 33, "Type checks").
//!
//! As built in R6, a write command runs in one MariaDB transaction:
//!
//! 1. lock the key row (`SELECT ... FOR UPDATE`, which also reads MariaDB's
//!    clock); an expired row is deleted with its elements, so the key is
//!    absent;
//! 2. check the type (`WRONGTYPE` changes nothing);
//! 3. read and write elements, then write the key row with a new version
//!    `_v`, or delete it when the collection is empty.
//!
//! The dispatcher opens and commits the transaction (or the `EXEC` or the
//! script does). Reads take no lock and carry the live predicate, so an
//! expired key is gone the instant it expires by MariaDB's clock.

use bytes::Bytes;
use sprig_kv::layout::{KeyType, MAX_ELEMENT_BYTES, MAX_KEY_BYTES, Namespace, live};
use sprig_kv::sql::{cell_i64, cell_u64, lit};
use sprig_kv::time::{NOW_MS_SQL, datetime_lit, ms_sql};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::Frame;

use crate::env::Env;
use crate::reply;

/// Why a command stopped.
#[derive(Debug)]
pub enum Fail {
    /// Reply with this (an error frame); nothing was written.
    Reply(Frame),
    /// A backend error: roll back (and maybe retry).
    Sql(SqlError),
}

impl From<SqlError> for Fail {
    fn from(e: SqlError) -> Self {
        Self::Sql(e)
    }
}

/// A command's result.
pub type R<T> = Result<T, Fail>;

/// Stops the command with an error reply.
pub fn fail<T>(f: Frame) -> R<T> {
    Err(Fail::Reply(f))
}

/// A key row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRow {
    /// The key.
    pub k: Vec<u8>,
    /// Its type.
    pub t: KeyType,
    /// A string's value.
    pub v: Option<Vec<u8>>,
    /// Element count (not for strings).
    pub n: i64,
    /// List: first position.
    pub lh: i64,
    /// List: position after the last item.
    pub lt: i64,
    /// Generation: the version that created the key.
    pub g: u64,
    /// Version: changes on every write.
    pub ver: u64,
    /// Expiry, in ms since the epoch.
    pub exp: Option<i64>,
    /// Whether the row exists in the table (else it is new).
    pub stored: bool,
}

/// A fresh version (`_v`): random, 63 bits, never 0. Versions only need to
/// differ from the key's earlier ones (`WATCH`, compare-and-set).
#[must_use]
pub fn new_version() -> u64 {
    let r = getrandom::u64().unwrap_or_else(|_| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64)
    });
    (r & (u64::MAX >> 1)).max(1)
}

/// CRC32 of an element (`o` for hashes, sets and sorted sets).
#[must_use]
pub fn crc(e: &[u8]) -> i64 {
    i64::from(crc32fast::hash(e))
}

/// The columns [`Ctx::lock`] and [`Ctx::get`] select, after "now".
const KEY_COLS: &str = "t.type, t.v, t.n, t.lh, t.lt, t.g, t._v";

/// One command's context.
pub struct Ctx<'a> {
    /// The caller's SQL.
    pub sql: &'a mut dyn SqlRunner,
    /// The selected database's namespace.
    pub ns: Namespace,
    /// The selected database's index.
    pub db: u32,
    /// The connection speaks RESP3.
    pub resp3: bool,
    /// Inside `EXEC` or a script: the caller owns the transaction.
    pub in_txn: bool,
    /// The gateway's shared state.
    pub env: &'a Env,
    /// The tenant, scoping the hub and the wait table.
    pub scope: &'a str,
    /// MariaDB's clock at the last key read, ms since the epoch; 0 after a
    /// read of an absent key ([`Ctx::clock`] reads it then).
    pub now: i64,
    /// Keys this command pushed to (wakes blocked clients after commit).
    pub pushed: Vec<(u32, Vec<u8>)>,
    /// Statements run (for `INFO` and tests).
    pub statements: u64,
}

impl std::fmt::Debug for Ctx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("db", &self.db)
            .field("in_txn", &self.in_txn)
            .finish_non_exhaustive()
    }
}

fn parse_row(k: &[u8], r: &Row) -> Option<(i64, Option<KeyRow>, bool)> {
    let now = cell_i64(r.first()?.as_ref());
    let Some(t) = r.get(1)?.as_deref().and_then(KeyType::parse) else {
        return Some((now, None, false));
    };
    let exp = r.get(8)?.as_ref().map(|_| cell_i64(r[8].as_ref()));
    let expired = cell_i64(r.get(9)?.as_ref()) != 0;
    Some((
        now,
        Some(KeyRow {
            k: k.to_vec(),
            t,
            v: r.get(2)?.clone(),
            n: cell_i64(r.get(3)?.as_ref()),
            lh: cell_i64(r.get(4)?.as_ref()),
            lt: cell_i64(r.get(5)?.as_ref()),
            g: cell_u64(r.get(6)?.as_ref()),
            ver: cell_u64(r.get(7)?.as_ref()),
            exp,
            stored: true,
        }),
        expired,
    ))
}

impl<'a> Ctx<'a> {
    /// A context on database `db`.
    pub fn new(
        sql: &'a mut dyn SqlRunner,
        env: &'a Env,
        scope: &'a str,
        db: u32,
        resp3: bool,
    ) -> Self {
        let database = env.config().logical_database;
        Self {
            sql,
            ns: Namespace::redis(&database, db),
            db,
            resp3,
            in_txn: false,
            env,
            scope,
            now: 0,
            pushed: Vec::new(),
            statements: 0,
        }
    }

    /// The namespace of database `db`.
    #[must_use]
    pub fn ns_of(&self, db: u32) -> Namespace {
        Namespace::redis(&self.ns.database, db)
    }

    /// A statement with rows.
    ///
    /// # Errors
    /// The backend's.
    pub async fn q(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        self.statements += 1;
        self.sql.query(sql).await
    }

    /// A statement with an affected-row count.
    ///
    /// # Errors
    /// The backend's.
    pub async fn x(&mut self, sql: &str) -> Result<u64, SqlError> {
        self.statements += 1;
        self.sql.exec(sql).await
    }

    /// The cache key of `ns`'s tables in [`Env`].
    #[must_use]
    pub fn ensure_key(&self, ns: &Namespace) -> String {
        format!("{}/{}/{}", self.scope, ns.database, ns.table)
    }

    /// Creates `ns`'s tables when this gateway hasn't seen them yet. DDL
    /// commits, so this runs outside transactions only.
    ///
    /// # Errors
    /// The backend's.
    pub async fn ensure(&mut self, ns: &Namespace) -> Result<(), SqlError> {
        let key = self.ensure_key(ns);
        if self.env.is_ensured(&key) {
            return Ok(());
        }
        for s in ns.ddl() {
            self.x(&s).await?;
        }
        // SQL's read-only relations over the structures (chapter 33).
        for s in sprig_kv::api::view_ddl(ns) {
            if let Err(e) = self.x(&s).await {
                tracing::debug!(error = %e, "a key-value view was not created");
            }
        }
        self.env.set_ensured(&key);
        Ok(())
    }

    /// Checks a key's length.
    ///
    /// # Errors
    /// 39723 past 1024 bytes.
    pub fn check_key(k: &[u8]) -> R<()> {
        if k.len() > MAX_KEY_BYTES {
            return fail(reply::key_too_long());
        }
        Ok(())
    }

    /// Checks an element's length.
    ///
    /// # Errors
    /// 39731 past 2024 bytes.
    pub fn check_elem(e: &[u8]) -> R<()> {
        if e.len() > MAX_ELEMENT_BYTES {
            return fail(reply::element_too_long());
        }
        Ok(())
    }

    /// Checks a value's length.
    ///
    /// # Errors
    /// 39724 past the namespace's limit.
    pub fn check_value(&self, v: &[u8]) -> R<()> {
        if v.len() > self.ns.max_value_bytes {
            return fail(reply::value_too_big(self.ns.max_value_bytes));
        }
        Ok(())
    }

    /// The key row's read: one table, the key and its distribution key in
    /// the condition, so a sharded database's router sends it to the key's
    /// shard (R6 round 4). An absent key returns no row.
    fn select_sql(ns: &Namespace, k: &[u8], lock: bool) -> String {
        let and_live = if lock {
            String::new()
        } else {
            format!(" AND {}", live("t"))
        };
        format!(
            "SELECT {NOW_MS_SQL}, {KEY_COLS}, {}, (t.expires_at IS NOT NULL AND t.expires_at <= UTC_TIMESTAMP(3)) \
             FROM {} AS t WHERE {}{and_live}{}",
            ms_sql("t.expires_at"),
            ns.keys(),
            sprig_kv::api::key_pred(ns, k, "t."),
            if lock { " FOR UPDATE" } else { "" }
        )
    }

    /// MariaDB's clock (ms since the epoch): the last key read's, or, after
    /// a read of an absent key, read on `k`'s shard (one aggregate over the
    /// key table, so it routes like the key's statements).
    ///
    /// # Errors
    /// The backend's.
    pub async fn clock(&mut self, k: &[u8]) -> R<i64> {
        if self.now == 0 {
            let ns = self.ns.clone();
            let rows = self
                .q(&format!(
                    "SELECT {NOW_MS_SQL}, COUNT(*) FROM {} WHERE {}",
                    ns.keys(),
                    sprig_kv::api::key_pred(&ns, k, "")
                ))
                .await?;
            self.now = cell_i64(rows.first().and_then(|r| r.first()).and_then(Option::as_ref));
        }
        Ok(self.now)
    }

    /// Locks `k`'s row in `ns`; an expired key is deleted and reads as absent.
    ///
    /// # Errors
    /// The backend's; 39723 for a long key.
    pub async fn lock_in(&mut self, ns: &Namespace, k: &[u8]) -> R<Option<KeyRow>> {
        Self::check_key(k)?;
        let sql = Self::select_sql(ns, k, true);
        let rows = self.q(&sql).await?;
        let Some((now, row, expired)) = rows.first().and_then(|r| parse_row(k, r)) else {
            self.now = 0;
            return Ok(None);
        };
        self.now = now;
        match row {
            Some(r) if expired => {
                self.delete_in(ns, &r).await?;
                Ok(None)
            }
            r => Ok(r),
        }
    }

    /// Locks `k` in the selected database.
    ///
    /// # Errors
    /// See [`Ctx::lock_in`].
    pub async fn lock(&mut self, k: &[u8]) -> R<Option<KeyRow>> {
        let ns = self.ns.clone();
        self.lock_in(&ns, k).await
    }

    /// Locks `k` and checks its type.
    ///
    /// # Errors
    /// `WRONGTYPE`; see [`Ctx::lock`].
    pub async fn lock_typed(&mut self, k: &[u8], want: KeyType) -> R<Option<KeyRow>> {
        let r = self.lock(k).await?;
        if let Some(row) = &r
            && row.t != want
        {
            return fail(reply::wrongtype());
        }
        Ok(r)
    }

    /// Locks several keys in byte order (so concurrent commands don't
    /// deadlock), returning them in the order asked.
    ///
    /// # Errors
    /// See [`Ctx::lock`].
    pub async fn lock_many(&mut self, keys: &[&[u8]]) -> R<Vec<Option<KeyRow>>> {
        let mut sorted: Vec<&[u8]> = keys.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let mut got: Vec<(Vec<u8>, Option<KeyRow>)> = Vec::with_capacity(sorted.len());
        for k in sorted {
            let r = self.lock(k).await?;
            got.push((k.to_vec(), r));
        }
        Ok(keys
            .iter()
            .map(|k| {
                got.iter()
                    .find(|(gk, _)| gk.as_slice() == *k)
                    .and_then(|(_, r)| r.clone())
            })
            .collect())
    }

    /// Reads `k`'s live row in `ns` without locking.
    ///
    /// # Errors
    /// The backend's.
    pub async fn get_in(&mut self, ns: &Namespace, k: &[u8]) -> R<Option<KeyRow>> {
        if k.len() > MAX_KEY_BYTES {
            return Ok(None);
        }
        let sql = Self::select_sql(ns, k, false);
        let rows = self.q(&sql).await?;
        let Some((now, row, _)) = rows.first().and_then(|r| parse_row(k, r)) else {
            self.now = 0;
            return Ok(None);
        };
        self.now = now;
        Ok(row)
    }

    /// Reads `k`'s live row without locking.
    ///
    /// # Errors
    /// The backend's.
    pub async fn get(&mut self, k: &[u8]) -> R<Option<KeyRow>> {
        let ns = self.ns.clone();
        self.get_in(&ns, k).await
    }

    /// Reads `k` and checks its type.
    ///
    /// # Errors
    /// `WRONGTYPE`.
    pub async fn get_typed(&mut self, k: &[u8], want: KeyType) -> R<Option<KeyRow>> {
        let r = self.get(k).await?;
        if let Some(row) = &r
            && row.t != want
        {
            return fail(reply::wrongtype());
        }
        Ok(r)
    }

    /// A new key row of type `t` (not yet stored).
    #[must_use]
    pub fn new_row(k: &[u8], t: KeyType) -> KeyRow {
        let v = new_version();
        KeyRow {
            k: k.to_vec(),
            t,
            v: None,
            n: 0,
            lh: 0,
            lt: 0,
            g: v,
            ver: v,
            exp: None,
            stored: false,
        }
    }

    /// Writes `row` to `ns` with a new version.
    ///
    /// # Errors
    /// The backend's.
    pub async fn save_in(&mut self, ns: &Namespace, row: &mut KeyRow) -> Result<(), SqlError> {
        row.ver = new_version();
        let exp = row.exp.map_or_else(|| "NULL".to_owned(), datetime_lit);
        let v = row.v.as_deref().map_or_else(|| "NULL".to_owned(), lit);
        let sql = if row.stored {
            format!(
                "UPDATE {} SET type = '{}', v = {v}, n = {}, lh = {}, lt = {}, g = {}, _v = {}, expires_at = {exp} WHERE {}",
                ns.keys(),
                row.t.as_str(),
                row.n,
                row.lh,
                row.lt,
                row.g,
                row.ver,
                sprig_kv::api::key_pred(ns, &row.k, "")
            )
        } else {
            format!(
                "INSERT INTO {} (k, type, v, n, lh, lt, g, _v, expires_at, _sprig_dk, _sprig_rk) \
                 VALUES ({}, '{}', {v}, {}, {}, {}, {}, {}, {exp}, {}, {})",
                ns.keys(),
                lit(&row.k),
                row.t.as_str(),
                row.n,
                row.lh,
                row.lt,
                row.g,
                row.ver,
                lit(ns.dist_key(&row.k)),
                ns.routing_hash(&row.k)
            )
        };
        self.x(&sql).await?;
        row.stored = true;
        Ok(())
    }

    /// Writes `row` in the selected database.
    ///
    /// # Errors
    /// The backend's.
    pub async fn save(&mut self, row: &mut KeyRow) -> Result<(), SqlError> {
        let ns = self.ns.clone();
        self.save_in(&ns, row).await
    }

    /// Writes `row`, or deletes it when it is an empty collection.
    ///
    /// # Errors
    /// The backend's.
    pub async fn save_or_delete(&mut self, row: &mut KeyRow) -> Result<(), SqlError> {
        if row.t != KeyType::String && row.n <= 0 {
            if row.stored {
                let ns = self.ns.clone();
                self.delete_in(&ns, row).await?;
                row.stored = false;
            }
            Ok(())
        } else {
            self.save(row).await
        }
    }

    /// Deletes `row` from `ns` with its elements.
    ///
    /// # Errors
    /// The backend's.
    pub async fn delete_in(&mut self, ns: &Namespace, row: &KeyRow) -> Result<(), SqlError> {
        let kp = sprig_kv::api::key_pred(ns, &row.k, "");
        self.x(&format!("DELETE FROM {} WHERE {kp}", ns.keys()))
            .await?;
        match row.t {
            KeyType::String => {}
            KeyType::ZSet => {
                self.x(&format!(
                    "DELETE FROM {} WHERE {kp} AND g = {}",
                    ns.zset(),
                    row.g
                ))
                .await?;
            }
            _ => {
                self.x(&format!(
                    "DELETE FROM {} WHERE {kp} AND g = {}",
                    ns.elems(),
                    row.g
                ))
                .await?;
            }
        }
        Ok(())
    }

    /// Deletes `row` from the selected database.
    ///
    /// # Errors
    /// The backend's.
    pub async fn delete(&mut self, row: &KeyRow) -> Result<(), SqlError> {
        let ns = self.ns.clone();
        self.delete_in(&ns, row).await
    }

    /// `k = <key> AND _sprig_dk = <its distribution key>` on alias `p`
    /// (`t.` or empty): on a sharded database the router sends the
    /// statement to the key's shard (R6 round 4).
    #[must_use]
    pub fn kp(&self, k: &[u8], p: &str) -> String {
        sprig_kv::api::key_pred(&self.ns, k, p)
    }

    /// `k IN (...) AND _sprig_dk IN (...)` for `keys` (each at most 1024
    /// bytes), or `FALSE` when there are none.
    #[must_use]
    pub fn keys_in(&self, keys: &[&[u8]]) -> String {
        if keys.is_empty() {
            return "FALSE".into();
        }
        let ks: Vec<String> = keys.iter().map(|k| lit(k)).collect();
        let mut dks: Vec<&[u8]> = keys.iter().map(|k| self.ns.dist_key(k)).collect();
        dks.sort_unstable();
        dks.dedup();
        let dks: Vec<String> = dks.into_iter().map(lit).collect();
        format!(
            "k IN ({}) AND _sprig_dk IN ({})",
            ks.join(", "),
            dks.join(", ")
        )
    }

    /// `(k, g)` of `row` as a SQL condition, with the distribution key
    /// (see [`Ctx::kp`]).
    #[must_use]
    pub fn kg(&self, row: &KeyRow) -> String {
        format!("{} AND g = {}", self.kp(&row.k, ""), row.g)
    }

    /// The routing columns' values for an element row of `k`.
    #[must_use]
    pub fn route_vals(&self, k: &[u8]) -> String {
        format!("{}, {}", lit(self.ns.dist_key(k)), self.ns.routing_hash(k))
    }

    /// Records a push to `k` (wakes blocked clients after commit).
    pub fn pushed_to(&mut self, k: &[u8]) {
        self.pushed.push((self.db, k.to_vec()));
    }
}

/// An argument as UTF-8 upper case, for options.
#[must_use]
pub fn upper(a: &Bytes) -> String {
    String::from_utf8_lossy(a).to_ascii_uppercase()
}

/// An argument as `i64` by Redis's rules.
///
/// # Errors
/// `-ERR value is not an integer or out of range`.
pub fn int_arg(a: &[u8]) -> R<i64> {
    sprig_proto_redis::num::parse_i64(a).map_or_else(|| fail(reply::not_int()), Ok)
}

/// An argument as `f64` by Redis's rules.
///
/// # Errors
/// `-ERR value is not a valid float`.
pub fn float_arg(a: &[u8]) -> R<f64> {
    sprig_proto_redis::num::parse_f64(a).map_or_else(|| fail(reply::not_float()), Ok)
}

/// A bulk reply of an element cell.
#[must_use]
pub fn bulk(c: Option<&Vec<u8>>) -> Frame {
    c.map_or_else(Frame::null, |v| Frame::bulk_from(v))
}
