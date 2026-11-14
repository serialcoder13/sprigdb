//! Capped collections and tailable cursors.
//!
//! - **Storage.** A capped collection's table has two more columns:
//!   `_seq`, an `AUTO_INCREMENT` insertion sequence with a unique key (the
//!   collection's natural order), and `_sz`, the document's BSON size.
//! - **Totals.** The document count and the bytes held are kept in one
//!   reserved row of the database's records table
//!   (`__sprig_doc_sessions`, whose grants every writer of the database
//!   already holds): `lsid` = `"\0capped"` + the collection's UUID, `uid`
//!   all zeros, `results` = `"<count>:<bytes>"`, `updated_at` far in the
//!   future so the records purge never takes it. An update that changes a
//!   document's size, or a delete, writes `stale` there; the next insert
//!   counts the table again.
//! - **Inserts** lock that row first (`FOR UPDATE`), so a collection's
//!   inserts are serialized and `_seq` order is commit order, which
//!   tailable cursors rely on. After the rows go in, the oldest are
//!   deleted while the collection holds more than `max` documents or more
//!   than `size` bytes, never the newest: MongoDB 8.0 keeps a document
//!   larger than `size` alone. All in one MariaDB transaction (the
//!   caller's, when a retryable write already opened one).
//! - **Reads** without a sort, or with `{$natural: ±1}`, walk `_seq`.
//! - **Tailable cursors** keep their `_seq` position and read on from it at
//!   each `getMore`; the position is lost (136) when its row is gone.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{
    CollMeta, CursorPlan, Engine, Ns, PlanKind, Rendered, SqlError, SqlRunner, dup_error, hlc_now,
    mongo_error, text,
};
use crate::json;
use crate::sql::{bin_lit, ident, str_lit};

/// The largest `size` (MongoDB 8.0: 1 PiB).
pub const MAX_SIZE: i64 = 1 << 50;
/// `max` must be below this.
pub const MAX_MAX: i64 = 1 << 31;
/// What `max` of 0 or less is stored as.
pub const NO_MAX: i32 = i32::MAX;
/// MongoDB's `CappedPositionLost`.
pub const CAPPED_POSITION_LOST: i32 = 136;

/// The column a plan walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOrder {
    /// `_id`, every plain collection's order.
    Id,
    /// A capped collection's insertion order.
    Seq,
}

impl KeyOrder {
    /// The natural order of collection `m`.
    #[must_use]
    pub fn of(m: &CollMeta) -> Self {
        if Cap::of(&m.options).is_some() {
            Self::Seq
        } else {
            Self::Id
        }
    }

    pub(crate) fn column(self) -> &'static str {
        match self {
            Self::Id => "_id",
            Self::Seq => "_seq",
        }
    }

    /// A position as SQL: `_id`'s key bytes, or `_seq`'s decimal text.
    pub(crate) fn literal(self, k: &[u8]) -> String {
        match self {
            Self::Id => bin_lit(k),
            Self::Seq => std::str::from_utf8(k)
                .ok()
                .and_then(|t| t.parse::<u64>().ok())
                .unwrap_or(0)
                .to_string(),
        }
    }
}

/// A capped collection's limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cap {
    /// Bytes of BSON held at most.
    pub size: u64,
    /// Documents held at most.
    pub max: u64,
}

impl Cap {
    /// The limits of a collection with `options`; `None` when it isn't
    /// capped.
    #[must_use]
    pub fn of(options: &Document) -> Option<Self> {
        if !matches!(options.get("capped"), Some(Bson::Boolean(true))) {
            return None;
        }
        let size = options.get("size").and_then(num).map(trunc)?;
        let max = options
            .get("max")
            .and_then(num)
            .map(trunc)
            .filter(|m| *m > 0)
            .unwrap_or(i64::MAX);
        Some(Self {
            size: u64::try_from(size.max(1)).unwrap_or(1),
            max: u64::try_from(max).unwrap_or(u64::MAX),
        })
    }

    fn over(self, n: u64, bytes: u64) -> bool {
        n > self.max || bytes > self.size
    }
}

fn num(v: &Bson) -> Option<f64> {
    #[allow(clippy::cast_precision_loss)]
    match v {
        Bson::Int32(i) => Some(f64::from(*i)),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn trunc(f: f64) -> i64 {
    if f.is_nan() {
        0
    } else {
        f.trunc().clamp(-9.0e18, 9.0e18) as i64
    }
}

/// An integer as MongoDB stores it: `int` when it fits, else `long`.
fn int_bson(n: i64) -> Bson {
    i32::try_from(n).map_or(Bson::Int64(n), Bson::Int32)
}

fn wrong_type(field: &str, v: &Bson, expected: &str) -> MongoError {
    MongoError::new(
        code::TYPE_MISMATCH,
        format!(
            "BSON field '{field}' is the wrong type '{}', expected types '[{expected}']",
            crate::update::type_name(v)
        ),
    )
}

fn number_of(cmd: &str, field: &str, v: &Bson) -> Result<i64, MongoError> {
    num(v)
        .map(trunc)
        .ok_or_else(|| wrong_type(&format!("{cmd}.{field}"), v, "long, double, int, decimal"))
}

fn check_size(field: &str, n: i64) -> Result<(), MongoError> {
    if n < 1 {
        return Err(MongoError::new(
            code::BAD_VALUE,
            format!("BSON field '{field}' value must be >= 1, actual value '{n}'"),
        ));
    }
    if n > MAX_SIZE {
        return Err(MongoError::new(
            code::BAD_VALUE,
            format!("BSON field '{field}' value must be <= {MAX_SIZE}, actual value '{n}'"),
        ));
    }
    Ok(())
}

fn check_max(field: &str, n: i64) -> Result<i32, MongoError> {
    if n >= MAX_MAX {
        return Err(MongoError::new(
            code::BAD_VALUE,
            format!("BSON field '{field}' value must be < {MAX_MAX}, actual value '{n}'"),
        ));
    }
    Ok(i32::try_from(n).ok().filter(|m| *m > 0).unwrap_or(NO_MAX))
}

/// `create`'s `capped`, `size` and `max`, checked and normalized as
/// MongoDB 8.0 stores them (`capped: true`; `size` an integer, `int` when
/// it fits; `max` 0 or less as 2147483647). `capped: false` is dropped.
///
/// # Errors
/// 14 for a wrong type; 2 for a value out of range; 72 for `capped`
/// without `size`, or `size`/`max` without `capped`.
pub fn normalize_create(options: Document) -> Result<Document, MongoError> {
    if !["capped", "size", "max"]
        .iter()
        .any(|k| options.contains_key(*k))
    {
        return Ok(options);
    }
    let capped = match options.get("capped") {
        None | Some(Bson::Null) => false,
        Some(Bson::Boolean(b)) => *b,
        Some(v) => match num(v) {
            Some(f) => f.classify() != std::num::FpCategory::Zero,
            None => {
                return Err(wrong_type(
                    "create.capped",
                    v,
                    "bool, long, double, int, decimal",
                ));
            }
        },
    };
    let size = match options.get("size") {
        None | Some(Bson::Null) => None,
        Some(v) => {
            let n = number_of("create", "size", v)?;
            check_size("size", n)?;
            Some(n)
        }
    };
    let max = match options.get("max") {
        None | Some(Bson::Null) => None,
        Some(v) => Some(check_max("max", number_of("create", "max", v)?)?),
    };
    if capped && size.is_none() {
        return Err(MongoError::new(
            code::INVALID_OPTIONS,
            "the 'size' field is required when 'capped' is true",
        ));
    }
    if !capped && (size.is_some() || max.is_some()) {
        return Err(MongoError::new(
            code::INVALID_OPTIONS,
            "the 'capped' field needs to be true when either the 'size' or 'max' fields are present",
        ));
    }
    let mut out = Document::new();
    for (k, v) in options {
        match k.as_str() {
            "capped" if capped => {
                out.insert(k, true);
            }
            "capped" => {}
            "size" => {
                if let Some(n) = size {
                    out.insert(k, int_bson(n));
                }
            }
            "max" => {
                if let Some(n) = max {
                    out.insert(k, n);
                }
            }
            _ => {
                out.insert(k, v);
            }
        }
    }
    Ok(out)
}

/// `collMod`'s `cappedSize` and `cappedMax` (MongoDB 6.0+): the
/// collection's new options, or `None` when the command has neither.
/// Like MongoDB, nothing is deleted until the next insert.
///
/// # Errors
/// 14, 2 as [`normalize_create`]; 72 for a collection that isn't capped.
pub fn coll_mod(options: &Document, body: &Document) -> Result<Option<Document>, MongoError> {
    let size = body.get("cappedSize");
    let max = body.get("cappedMax");
    if size.is_none() && max.is_none() {
        return Ok(None);
    }
    let size = size
        .map(|v| {
            let n = number_of("collMod", "cappedSize", v)?;
            check_size("cappedSize", n).map(|()| n)
        })
        .transpose()?;
    let max = max
        .map(|v| check_max("cappedMax", number_of("collMod", "cappedMax", v)?))
        .transpose()?;
    if Cap::of(options).is_none() {
        return Err(MongoError::new(
            code::INVALID_OPTIONS,
            "Collection must be capped.",
        ));
    }
    let mut o = options.clone();
    if let Some(n) = size {
        o.insert("size", int_bson(n));
    }
    if let Some(n) = max {
        o.insert("max", n);
    }
    Ok(Some(o))
}

/// `size` of `convertToCapped` and `cloneCollectionAsCapped`.
///
/// # Errors
/// 72 for a missing size or one of 0 or less; 2 past 1 PiB.
pub fn command_size(cmd: &str, v: Option<&Bson>) -> Result<i64, MongoError> {
    let n = match v {
        None | Some(Bson::Null) => 0,
        Some(v) => number_of(cmd, "size", v)?,
    };
    if n < 1 {
        return Err(MongoError::new(
            code::INVALID_OPTIONS,
            "Capped collection size must be greater than zero",
        ));
    }
    check_size("size", n)?;
    Ok(n)
}

/// `$natural` in a sort: `Some(1)` or `Some(-1)`.
///
/// # Errors
/// 2 for `$natural` other than 1 or -1, or next to other keys.
pub fn natural_of(sort: Option<&Document>) -> Result<Option<i32>, MongoError> {
    let Some(s) = sort else { return Ok(None) };
    let Some(v) = s.get("$natural") else {
        return Ok(None);
    };
    let bad = || {
        MongoError::new(
            code::BAD_VALUE,
            "$natural sort cannot be set to a value other than -1 or 1.",
        )
    };
    if s.len() != 1 {
        return Err(bad());
    }
    match crate::filter::as_i64(v) {
        Some(1) => Ok(Some(1)),
        Some(-1) => Ok(Some(-1)),
        _ => Err(bad()),
    }
}

/// `CREATE TABLE` of a capped collection.
#[must_use]
pub fn table_sql(db: &str, table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  _id    VARBINARY(1024) NOT NULL,
  _t     VARCHAR(255) NULL,
  doc    JSON NOT NULL,
  _types JSON NULL,
  _v     BIGINT UNSIGNED NOT NULL,
  _seq   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  _sz    INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (_id),
  UNIQUE KEY _sprig_seq (_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        ident(db),
        ident(table)
    )
}

const ZERO_UID: [u8; 32] = [0; 32];
const FAR: &str = "'9999-12-31 23:59:59'";

fn totals_key(uuid: &[u8; 16]) -> Vec<u8> {
    let mut k = b"\0capped".to_vec();
    k.extend_from_slice(uuid);
    k
}

/// The records table, and the condition naming the totals row.
fn totals_at(db: &str, uuid: &[u8; 16]) -> (String, String) {
    (
        format!("{}.{}", ident(db), ident(crate::sessions::SESSIONS_TABLE)),
        format!(
            "lsid = {} AND uid = {}",
            bin_lit(&totals_key(uuid)),
            bin_lit(&ZERO_UID)
        ),
    )
}

fn totals_where(db: &str, uuid: &[u8; 16]) -> String {
    let (t, w) = totals_at(db, uuid);
    format!("{t} WHERE {w}")
}

fn totals_set(db: &str, uuid: &[u8; 16], set: &str) -> String {
    let (t, w) = totals_at(db, uuid);
    format!("UPDATE {t} SET {set} WHERE {w}")
}

/// Marks the collection's totals for a recount (after an update that
/// changed a document's size, or a delete).
///
/// # Errors
/// SQL errors other than a missing records table.
pub(crate) async fn mark_stale(
    sql: &mut dyn SqlRunner,
    db: &str,
    uuid: &[u8; 16],
) -> Result<(), MongoError> {
    match sql.exec(&totals_set(db, uuid, "results = 'stale'")).await {
        Ok(_) => Ok(()),
        Err(e) if e.is(1146) => Ok(()),
        Err(e) => Err(mongo_error(&e)),
    }
}

/// Deletes the collection's totals row (the collection was dropped).
pub(crate) async fn forget(sql: &mut dyn SqlRunner, db: &str, uuid: &[u8; 16]) {
    let _ = sql
        .exec(&format!("DELETE FROM {}", totals_where(db, uuid)))
        .await;
}

fn first_u64(rows: &super::Rows, col: usize) -> u64 {
    rows.first()
        .and_then(|r| r.get(col))
        .map(|c| text(c.as_ref()))
        .and_then(|t| t.parse().ok())
        .unwrap_or(0)
}

/// The count and bytes held, from the table itself.
async fn count_table(
    sql: &mut dyn SqlRunner,
    db: &str,
    table: &str,
) -> Result<(u64, u64), SqlError> {
    let rows = sql
        .query(&format!(
            "SELECT COUNT(*), COALESCE(SUM(_sz), 0) FROM {}.{}",
            ident(db),
            ident(table)
        ))
        .await?;
    Ok((first_u64(&rows, 0), first_u64(&rows, 1)))
}

/// Locks the totals row and reads it; counts the table when the row is
/// stale or missing.
async fn lock_totals(
    sql: &mut dyn SqlRunner,
    db: &str,
    table: &str,
    uuid: &[u8; 16],
) -> Result<(u64, u64), SqlError> {
    for _ in 0..2 {
        let rows = sql
            .query(&format!(
                "SELECT results FROM {} FOR UPDATE",
                totals_where(db, uuid)
            ))
            .await?;
        if let Some(r) = rows.first() {
            let t = text(r.first().and_then(Option::as_ref));
            if let Some((n, b)) = t.split_once(':')
                && let (Ok(n), Ok(b)) = (n.parse(), b.parse())
            {
                return Ok((n, b));
            }
            return count_table(sql, db, table).await;
        }
        sql.exec(&format!(
            "INSERT IGNORE INTO {}.{} (lsid, uid, txn_number, results, txn_state, updated_at) \
             VALUES ({}, {}, 0, 'stale', 'retryable', {FAR})",
            ident(db),
            ident(crate::sessions::SESSIONS_TABLE),
            bin_lit(&totals_key(uuid)),
            bin_lit(&ZERO_UID)
        ))
        .await?;
    }
    count_table(sql, db, table).await
}

/// Tailable cursors waiting for inserts on this gateway, by namespace.
type Waiters = HashMap<(u64, String, String), Arc<tokio::sync::Notify>>;
static WAITERS: LazyLock<Mutex<Waiters>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn waiter(tenant: u64, ns: &Ns) -> Arc<tokio::sync::Notify> {
    let mut m = WAITERS.lock().unwrap_or_else(PoisonError::into_inner);
    // Entries nobody waits on any more go.
    m.retain(|_, n| Arc::strong_count(n) > 1);
    Arc::clone(
        m.entry((tenant, ns.db.clone(), ns.coll.clone()))
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new())),
    )
}

fn notify(tenant: u64, ns: &Ns) {
    let m = WAITERS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(n) = m.get(&(tenant, ns.db.clone(), ns.coll.clone())) {
        n.notify_waiters();
    }
}

/// Waits up to `d` for an insert into `ns` through this gateway. Inserts
/// through other gateways aren't signalled: the caller polls as well.
pub async fn wait_insert(tenant: u64, ns: &Ns, d: Duration) {
    let n = waiter(tenant, ns);
    let _ = tokio::time::timeout(d, n.notified()).await;
}

impl CursorPlan {
    /// Makes a `find` plan tailable: reaching the end of the
    /// collection leaves it open. Only a plan in a capped collection's
    /// insertion order can be.
    pub fn make_tailable(&mut self) -> bool {
        match &mut self.kind {
            PlanKind::Keyset {
                order: KeyOrder::Seq,
                desc: false,
                tail,
                ..
            } => {
                *tail = true;
                true
            }
            _ => false,
        }
    }

    /// Whether this is a tailable plan.
    #[must_use]
    pub fn is_tailable(&self) -> bool {
        matches!(self.kind, PlanKind::Keyset { tail: true, .. })
    }

    /// Whether a tailable plan has read any row: one that hasn't is closed
    /// after its first batch, as MongoDB closes a tailable cursor over an
    /// empty collection.
    #[must_use]
    pub fn tail_positioned(&self) -> bool {
        matches!(&self.kind, PlanKind::Keyset { last: Some(_), .. })
    }

    /// A tailable plan's position check, taken before an `await` (the
    /// plan itself isn't `Sync`): the statement finding its row, and the
    /// position. See [`Engine::check_tail_position`].
    #[must_use]
    pub fn tail_probe(&self) -> Option<(String, String)> {
        match &self.kind {
            PlanKind::Keyset {
                tail: true,
                last: Some(k),
                ..
            } => {
                let seq = KeyOrder::Seq.literal(k);
                Some((
                    format!(
                        "SELECT 1 FROM {}.{} WHERE _seq = {seq}",
                        ident(&self.ns.db),
                        ident(&self.table)
                    ),
                    seq,
                ))
            }
            _ => None,
        }
    }
}

impl Engine {
    /// Inserts rendered documents into a capped collection, then deletes
    /// the oldest past its limits (see the module docs).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn insert_capped(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        cap: Cap,
        rows: Vec<Rendered>,
        sizes: &[u64],
        ordered: bool,
    ) -> Result<(u64, Vec<(usize, MongoError)>), MongoError> {
        let in_txn = sql
            .query("SELECT @@in_transaction")
            .await
            .map_err(|e| mongo_error(&e))?;
        let own = first_u64(&in_txn, 0) == 0;
        let mut attempt = 0u32;
        let mut created = false;
        loop {
            attempt += 1;
            if own {
                sql.exec("START TRANSACTION")
                    .await
                    .map_err(|e| mongo_error(&e))?;
            }
            let r = self
                .insert_capped_once(sql, ns, m, cap, &rows, sizes, ordered)
                .await;
            match r {
                Ok(done) => {
                    if own {
                        sql.exec("COMMIT").await.map_err(|e| mongo_error(&e))?;
                    }
                    if done.0 > 0 {
                        notify(sql.tenant(), ns);
                    }
                    return Ok(done);
                }
                Err(e) => {
                    if own {
                        let _ = sql.exec("ROLLBACK").await;
                    }
                    match e {
                        // The records table went with an old move or
                        // drop: created outside the transaction.
                        e if own && e.is(1146) && !created => {
                            created = true;
                            sql.exec(&crate::sessions::table_sql(&ns.db))
                                .await
                                .map_err(|e| mongo_error(&e))?;
                        }
                        e if own && (e.is(1213) || e.is(1205)) && attempt < 5 => {
                            tokio::time::sleep(Duration::from_millis(u64::from(attempt) * 5)).await;
                        }
                        e => return Err(mongo_error(&e)),
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_capped_once(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        cap: Cap,
        rows: &[Rendered],
        sizes: &[u64],
        ordered: bool,
    ) -> Result<(u64, Vec<(usize, MongoError)>), SqlError> {
        let (mut n, mut bytes) = lock_totals(sql, &ns.db, &m.table, &m.uuid).await?;
        let size_of = |i: usize| sizes.get(i).copied().unwrap_or(0);
        let value = |s: &json::Stored, i: usize| {
            format!(
                "({}, {}, {}, {}, {})",
                bin_lit(&s.id_key),
                str_lit(&s.doc),
                s.types
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), str_lit),
                hlc_now(),
                size_of(i)
            )
        };
        let head = format!(
            "INSERT INTO {}.{} (_id, doc, _types, _v, _sz) VALUES ",
            ident(&ns.db),
            ident(&m.table)
        );
        let mut errors = Vec::new();
        let mut inserted = 0u64;
        let mut added = 0u64;
        let all_good = rows.iter().all(|(_, r)| r.is_ok());
        let mut one_by_one = true;
        if all_good && rows.len() > 1 {
            let values: Vec<String> = rows
                .iter()
                .filter_map(|(i, r)| r.as_ref().ok().map(|(s, _)| value(s, *i)))
                .collect();
            match sql.exec(&format!("{head}{}", values.join(", "))).await {
                Ok(_) => {
                    inserted = rows.len() as u64;
                    added = rows.iter().map(|(i, _)| size_of(*i)).sum();
                    one_by_one = false;
                }
                Err(e) if e.is(1062) => {}
                Err(e) => return Err(e),
            }
        }
        if one_by_one {
            for (i, r) in rows {
                match r {
                    Err(e) => {
                        errors.push((*i, e.clone()));
                        if ordered {
                            break;
                        }
                    }
                    Ok((s, id)) => match sql.exec(&format!("{head}{}", value(s, *i))).await {
                        Ok(_) => {
                            inserted += 1;
                            added += size_of(*i);
                        }
                        Err(e) if e.is(1062) => {
                            errors.push((*i, dup_error(&e, id)));
                            if ordered {
                                break;
                            }
                        }
                        Err(e) => return Err(e),
                    },
                }
            }
        }
        n += inserted;
        bytes += added;
        if inserted > 0 && cap.over(n, bytes) {
            (n, bytes) = self.evict(sql, ns, m, cap, n, bytes).await?;
        }
        sql.exec(&totals_set(
            &ns.db,
            &m.uuid,
            &format!("results = '{n}:{bytes}', updated_at = {FAR}"),
        ))
        .await?;
        Ok((inserted, errors))
    }

    /// Deletes the oldest documents while the collection is past `cap`,
    /// never the newest. Returns the new totals.
    async fn evict(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        cap: Cap,
        mut n: u64,
        mut bytes: u64,
    ) -> Result<(u64, u64), SqlError> {
        let t = format!("{}.{}", ident(&ns.db), ident(&m.table));
        let newest = first_u64(&sql.query(&format!("SELECT MAX(_seq) FROM {t}")).await?, 0);
        let mut after = 0u64;
        let mut cutoff = None;
        'scan: loop {
            let rows = sql
                .query(&format!(
                    "SELECT _seq, _sz FROM {t} WHERE _seq > {after} ORDER BY _seq LIMIT 1000"
                ))
                .await?;
            let k = rows.len();
            for r in &rows {
                let seq: u64 = text(r.first().and_then(Option::as_ref))
                    .parse()
                    .unwrap_or(u64::MAX);
                let sz: u64 = text(r.get(1).and_then(Option::as_ref)).parse().unwrap_or(0);
                if !cap.over(n, bytes) || seq >= newest {
                    break 'scan;
                }
                n = n.saturating_sub(1);
                bytes = bytes.saturating_sub(sz);
                cutoff = Some(seq);
                after = seq;
            }
            if k < 1000 {
                break;
            }
        }
        if let Some(c) = cutoff {
            sql.exec(&format!("DELETE FROM {t} WHERE _seq <= {c}"))
                .await?;
        }
        Ok((n, bytes))
    }

    /// A capped collection's limits, document count and bytes (for
    /// `collStats`); `None` when `ns` isn't a capped collection.
    ///
    /// # Errors
    /// SQL errors.
    pub async fn capped_stats(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Option<(Cap, u64, u64)>, MongoError> {
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(None);
        };
        let Some(cap) = Cap::of(&m.options) else {
            return Ok(None);
        };
        let (n, b) = count_table(sql, &ns.db, &m.table)
            .await
            .map_err(|e| mongo_error(&e))?;
        Ok(Some((cap, n, b)))
    }

    /// Whether `find` with `tailable` may run on `ns`: `false` when the
    /// collection doesn't exist (the cursor is closed at once, as in
    /// MongoDB).
    ///
    /// # Errors
    /// 2 for a collection that isn't capped.
    pub async fn check_tailable(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<bool, MongoError> {
        match self.meta(sql, ns).await? {
            None => Ok(false),
            Some(m) if Cap::of(&m.options).is_some() => Ok(true),
            Some(_) => Err(MongoError::new(
                code::BAD_VALUE,
                format!(
                    "error processing query: ns={ns} tailable cursor requested on non capped collection"
                ),
            )),
        }
    }

    /// Whether a tailable plan's position is gone: the row it last read
    /// was deleted (evicted, or by a delete), as MongoDB's 136. `probe` is
    /// [`CursorPlan::tail_probe`]'s.
    ///
    /// # Errors
    /// 136 `CappedPositionLost`; SQL errors.
    pub async fn check_tail_position(
        &self,
        sql: &mut dyn SqlRunner,
        probe: Option<(String, String)>,
    ) -> Result<(), MongoError> {
        let Some((q, seq)) = probe else {
            return Ok(());
        };
        let rows = sql.query(&q).await.map_err(|e| mongo_error(&e))?;
        if rows.is_empty() {
            return Err(MongoError::new(
                CAPPED_POSITION_LOST,
                format!(
                    "CollectionScan died due to failure to restore tailable cursor position. Last seen record id: RecordId({seq})"
                ),
            ));
        }
        Ok(())
    }

    /// `cloneCollectionAsCapped`: a new capped collection `to` of `size`
    /// bytes, filled with `from`'s documents in its natural order (the
    /// oldest deleted past `size`, as each insert does).
    ///
    /// # Errors
    /// 26 without `from`; 48 when `to` exists; 72 and 2 for the size.
    pub async fn clone_as_capped(
        &self,
        sql: &mut dyn SqlRunner,
        from: &Ns,
        to: &Ns,
        size: Option<&Bson>,
    ) -> Result<(), MongoError> {
        let Some(src) = self.meta(sql, from).await? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("source collection {from} does not exist"),
            ));
        };
        if self.meta(sql, to).await?.is_some() {
            return Err(MongoError::new(
                code::NAMESPACE_EXISTS,
                format!(
                    "cloneCollectionAsCapped failed - destination collection {to} already exists. source collection: {from}"
                ),
            ));
        }
        let size = command_size("cloneCollectionAsCapped", size)?;
        self.copy_capped(sql, &src, from, to, size).await
    }

    async fn copy_capped(
        &self,
        sql: &mut dyn SqlRunner,
        src: &CollMeta,
        from: &Ns,
        to: &Ns,
        size: i64,
    ) -> Result<(), MongoError> {
        self.create(
            sql,
            to,
            bson::doc! {"capped": true, "size": int_bson(size)},
            true,
        )
        .await?;
        let order = KeyOrder::of(src);
        let all = crate::translate::translate(&crate::filter::Pred::True, src);
        let mut last: Option<Vec<u8>> = None;
        loop {
            let rows = self
                .read_chunk(
                    sql,
                    from,
                    &src.table,
                    &all,
                    last.as_deref(),
                    false,
                    self.cfg.chunk_rows,
                    order,
                )
                .await?;
            let k = rows.len();
            let mut docs = Vec::with_capacity(k);
            for (key, d) in rows {
                last = Some(key);
                docs.push(d);
            }
            if !docs.is_empty() {
                let (_, errs) = Box::pin(self.insert(sql, to, docs, true)).await?;
                if let Some((_, e)) = errs.into_iter().next() {
                    return Err(e);
                }
            }
            if k < self.cfg.chunk_rows {
                return Ok(());
            }
        }
    }

    /// `convertToCapped`: `ns` becomes a capped collection of `size` bytes
    /// holding its newest documents; its secondary indexes and other
    /// options go, as in MongoDB. Built as a copy into a temporary
    /// collection, a drop and a rename: not atomic.
    ///
    /// # Errors
    /// 26 without `ns`; 72 and 2 for the size.
    pub async fn convert_to_capped(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        size: Option<&Bson>,
    ) -> Result<(), MongoError> {
        let Some(src) = self.meta(sql, ns).await? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("source collection {ns} does not exist"),
            ));
        };
        let size = command_size("convertToCapped", size)?;
        let tmp = Ns {
            db: ns.db.clone(),
            coll: format!("tmp.convertToCapped.{}", ns.coll),
        };
        self.drop(sql, &tmp).await?;
        if let Err(e) = self.copy_capped(sql, &src, ns, &tmp, size).await {
            let _ = self.drop(sql, &tmp).await;
            return Err(e);
        }
        self.rename(sql, &tmp, ns, true).await
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    #[test]
    fn create_options_as_mongodb_stores_them() {
        let n = |d: Document| normalize_create(d);
        assert_eq!(
            n(doc! {"capped": true, "size": 4097.5, "max": -1}).unwrap(),
            doc! {"capped": true, "size": 4097, "max": i32::MAX}
        );
        assert_eq!(
            n(doc! {"capped": 1, "size": 1e15}).unwrap(),
            doc! {"capped": true, "size": 1_000_000_000_000_000_i64}
        );
        assert_eq!(
            n(doc! {"capped": true, "size": 5000, "max": 5.7}).unwrap(),
            doc! {"capped": true, "size": 5000, "max": 5}
        );
        assert_eq!(n(doc! {"capped": false}).unwrap(), doc! {});
        assert_eq!(n(doc! {"capped": true}).unwrap_err().code, 72);
        assert_eq!(n(doc! {"capped": false, "max": 3}).unwrap_err().code, 72);
        assert_eq!(n(doc! {"size": 3000}).unwrap_err().code, 72);
        assert_eq!(n(doc! {"capped": true, "size": 0}).unwrap_err().code, 2);
        assert_eq!(
            n(doc! {"capped": true, "size": (1_i64 << 50) + 1})
                .unwrap_err()
                .code,
            2
        );
        assert_eq!(
            n(doc! {"capped": true, "size": 10, "max": 1_i64 << 32})
                .unwrap_err()
                .code,
            2
        );
        assert_eq!(n(doc! {"capped": "yes", "size": 1}).unwrap_err().code, 14);
        assert_eq!(n(doc! {"capped": true, "size": "x"}).unwrap_err().code, 14);
    }

    #[test]
    fn caps_and_coll_mod() {
        let c = Cap::of(&doc! {"capped": true, "size": 1000}).unwrap();
        assert_eq!((c.size, c.max), (1000, u64::try_from(i64::MAX).unwrap()));
        assert!(c.over(1, 1001) && !c.over(9, 1000));
        let c = Cap::of(&doc! {"capped": true, "size": 1000, "max": 3}).unwrap();
        assert!(c.over(4, 10));
        assert!(Cap::of(&doc! {"size": 1000}).is_none());
        let o = doc! {"capped": true, "size": 100_000};
        assert_eq!(
            coll_mod(&o, &doc! {"cappedSize": 8192, "cappedMax": 0}).unwrap(),
            Some(doc! {"capped": true, "size": 8192, "max": i32::MAX})
        );
        assert_eq!(coll_mod(&o, &doc! {"validator": {}}).unwrap(), None);
        assert_eq!(
            coll_mod(&doc! {}, &doc! {"cappedSize": 10})
                .unwrap_err()
                .code,
            72
        );
        assert_eq!(coll_mod(&o, &doc! {"cappedSize": 0}).unwrap_err().code, 2);
    }

    #[test]
    fn natural_sorts() {
        assert_eq!(natural_of(Some(&doc! {"$natural": -1})).unwrap(), Some(-1));
        assert_eq!(natural_of(Some(&doc! {"a": 1})).unwrap(), None);
        assert_eq!(natural_of(Some(&doc! {"$natural": 2})).unwrap_err().code, 2);
        assert_eq!(
            natural_of(Some(&doc! {"$natural": 1, "a": 1}))
                .unwrap_err()
                .code,
            2
        );
        assert_eq!(KeyOrder::Seq.literal(b"42"), "42");
        assert_eq!(command_size("c", None).unwrap_err().code, 72);
    }
}
