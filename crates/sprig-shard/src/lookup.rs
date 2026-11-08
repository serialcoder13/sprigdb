//! Lookup indexes: unique keys across shards and automatic global lookup
//! indexes (chapter 19, "Unique lookups", "Routing through lookups",
//! "Building, back-filling and dropping", "Automatic global lookup
//! indexes").
//!
//! A lookup table maps a value to the routing hash of the row that owns
//! it. It is a member of the tenant's lookup group, sharded by the hash of
//! its own value. Writes follow the consistent-lookup order: new lookup
//! rows in **Pre** sessions (committed first), the data in **Main**, and
//! lookup deletes buffered for **Post** (run after Main commits). Every
//! committed owner row then has its lookup row; a crash in between leaves
//! only orphans, which route to nothing. No step needs cross-cluster
//! atomicity, so it works on Galera and without XA.
//!
//! The mechanism lives here so the router (chapter 17) and distributed
//! transactions (chapter 18) share one implementation: the caller supplies
//! the sessions through [`LookupSessions`] (its `DistTxn`), and the
//! placement of lookup and owner shards through [`LookupPlacement`].
//! [`direct::DirectTxn`] is a plain implementation over `mysql_async`
//! pools, used by the tests and by jobs.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use mysql_async::{Params, Row, Value};
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::names::quote;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};

use crate::collation::{self, Charset};
use crate::hash::routing_hash;
use crate::key::{KeyClass, KeySpec, KeyValueOwned, RoutingHash};
use crate::names::shard_schema;
use crate::normalize::{Normalizer, StdNormalizer};
use crate::shardmap::ShardMap;

pub mod direct;

/// `lookup_indexes` (chapter 19), in `t<tenant>_sprig`.
pub const DDL_LOOKUP_INDEXES: &str = "CREATE TABLE IF NOT EXISTS lookup_indexes (
  lookup_id     BIGINT UNSIGNED NOT NULL,
  object_id     BIGINT UNSIGNED NOT NULL,
  index_name    VARCHAR(64) NOT NULL,
  columns       JSON NOT NULL,
  is_unique     TINYINT(1) NOT NULL,
  origin        ENUM('unique_key','primary_key','auto','fk_child') NOT NULL,
  physical_name VARCHAR(64) NOT NULL,
  group_id      INT UNSIGNED NOT NULL,
  state         ENUM('building','write_only','backfilling','public','dropping') NOT NULL,
  auto_index_id BIGINT UNSIGNED NULL,
  conflicts     BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_scan_at  DATETIME(6) NULL,
  updated_at    DATETIME(6) NOT NULL,
  PRIMARY KEY (lookup_id),
  UNIQUE KEY uk_name (object_id, index_name)
) ENGINE=InnoDB";

/// `foreign_keys` (chapter 19). Created with the module; enforcement
/// (`sprig-router::fk`) is not built yet.
pub const DDL_FOREIGN_KEYS: &str = "CREATE TABLE IF NOT EXISTS foreign_keys (
  fk_id            BIGINT UNSIGNED NOT NULL,
  name             VARCHAR(64) NOT NULL,
  child_object_id  BIGINT UNSIGNED NOT NULL,
  child_columns    JSON NOT NULL,
  parent_object_id BIGINT UNSIGNED NOT NULL,
  parent_columns   JSON NOT NULL,
  on_delete        ENUM('restrict','no_action','cascade','set_null') NOT NULL,
  on_update        ENUM('restrict','no_action','cascade','set_null') NOT NULL,
  enforcement      ENUM('native','gateway','reference') NOT NULL,
  child_lookup_id  BIGINT UNSIGNED NULL,
  orphanage        ENUM('none','present') NOT NULL DEFAULT 'none',
  state            ENUM('validating','enforced','dropping') NOT NULL,
  updated_at       DATETIME(6) NOT NULL,
  PRIMARY KEY (fk_id),
  UNIQUE KEY uk_child (child_object_id, name),
  KEY by_parent (parent_object_id)
) ENGINE=InnoDB";

/// A `lookup_indexes` row id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LookupId(pub u64);

/// A lookup's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupState {
    /// Tables being created on every lookup shard.
    Building,
    /// Writers claim and release; conflicts are counted, not raised.
    WriteOnly,
    /// Existing owners are being copied in.
    Backfilling,
    /// Routing allowed; claims raise 1062.
    Public,
    /// Being dropped.
    Dropping,
}

impl LookupState {
    /// The catalog's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::WriteOnly => "write_only",
            Self::Backfilling => "backfilling",
            Self::Public => "public",
            Self::Dropping => "dropping",
        }
    }

    /// Whether writers maintain it (claims and releases).
    #[must_use]
    pub fn maintained(self) -> bool {
        matches!(self, Self::WriteOnly | Self::Backfilling | Self::Public)
    }
}

/// Why a lookup exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOrigin {
    /// A user's `UNIQUE` key without the distribution key.
    UniqueKey,
    /// A primary key without the distribution key.
    PrimaryKey,
    /// Automatic indexing proposed it for a scatter query (chapter 13).
    Auto,
    /// The child columns of a cross-group foreign key.
    FkChild,
}

impl LookupOrigin {
    /// The catalog's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UniqueKey => "unique_key",
            Self::PrimaryKey => "primary_key",
            Self::Auto => "auto",
            Self::FkChild => "fk_child",
        }
    }
}

/// A column of a lookup table: a value column (copying the owner's
/// definition) or an owner primary-key column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupColumn {
    /// The owner's column name.
    pub name: String,
    /// Its full SQL type as in the owner, collation included:
    /// `VARCHAR(255) COLLATE utf8mb4_uca1400_ai_ci`.
    pub sql_type: String,
    /// Its key class (value columns: the lookup member's key).
    pub class: KeyClass,
}

/// A lookup index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupIndex {
    /// Its id.
    pub id: LookupId,
    /// The owner table or collection.
    pub owner: ObjectId,
    /// The owner's logical table name.
    pub owner_table: String,
    /// The user's key name, or `sprig_auto_<8 hex>`.
    pub index_name: String,
    /// The value columns, in key order.
    pub columns: Vec<LookupColumn>,
    /// The owner's primary-key columns.
    pub owner_pk: Vec<LookupColumn>,
    /// Unique values (a unique key) or not (automatic lookups).
    pub unique: bool,
    /// Why it exists.
    pub origin: LookupOrigin,
    /// The tenant's lookup group.
    pub group: GroupId,
    /// Its state.
    pub state: LookupState,
    /// The automatic index (chapter 13) behind an `auto` lookup.
    pub auto_index_id: Option<u64>,
}

/// `__sprig_lk_<owner>__<index>`, or `__sprig_lk_<lookup_id>` when that
/// would exceed 64 characters.
#[must_use]
pub fn lookup_table_name(owner: &str, index: &str, id: LookupId) -> String {
    let n = format!("__sprig_lk_{owner}__{index}");
    if n.chars().count() <= 64 {
        n
    } else {
        format!("__sprig_lk_{}", id.0)
    }
}

/// The hidden name of an automatic lookup: `sprig_auto_<8 hex>` of the
/// owner and columns, so the same proposal gets the same name.
#[must_use]
pub fn auto_lookup_name(owner: ObjectId, columns: &[String]) -> String {
    let h = twox_hash::XxHash3_64::oneshot(format!("{}:{}", owner.0, columns.join(",")).as_bytes());
    format!("sprig_auto_{:08x}", h >> 32)
}

impl LookupIndex {
    /// The physical table name.
    #[must_use]
    pub fn physical(&self) -> String {
        lookup_table_name(&self.owner_table, &self.index_name, self.id)
    }

    /// The lookup member's key: one part per value column.
    ///
    /// # Errors
    /// 39400 or 39401 for a column that can't be a key.
    pub fn spec(&self) -> Result<KeySpec> {
        KeySpec::new(self.columns.iter().map(|c| c.class).collect())
    }

    fn owner_col(c: &LookupColumn) -> String {
        format!("owner_{}", c.name)
    }

    /// `CREATE TABLE IF NOT EXISTS` for one lookup shard schema.
    #[must_use]
    pub fn create_table_sql(&self, schema: &str) -> String {
        let mut s = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (\n",
            quote(schema),
            quote(&self.physical())
        );
        for c in &self.columns {
            let _ = writeln!(s, "  {} {} NOT NULL,", quote(&c.name), c.sql_type);
        }
        for c in &self.owner_pk {
            let _ = writeln!(
                s,
                "  {} {} NOT NULL,",
                quote(&Self::owner_col(c)),
                c.sql_type
            );
        }
        s.push_str("  owner_rk BIGINT UNSIGNED NOT NULL,\n");
        s.push_str("  prev_rk BIGINT UNSIGNED NULL,\n");
        s.push_str("  _sprig_rk BIGINT UNSIGNED NOT NULL DEFAULT 0 INVISIBLE,\n");
        let mut pk: Vec<String> = self.columns.iter().map(|c| quote(&c.name)).collect();
        if !self.unique {
            pk.extend(self.owner_pk.iter().map(|c| quote(&Self::owner_col(c))));
        }
        let _ = writeln!(s, "  PRIMARY KEY ({}),", pk.join(", "));
        s.push_str("  KEY _sprig_rk (_sprig_rk)\n) ENGINE=InnoDB");
        s
    }

    fn value_where(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!("{} = ?", quote(&c.name)))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn owner_match(&self) -> String {
        self.owner_pk
            .iter()
            .map(|c| format!("{} <=> ?", quote(&Self::owner_col(c))))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn insert_sql(&self, schema: &str, ignore: bool) -> String {
        let mut cols: Vec<String> = self.columns.iter().map(|c| quote(&c.name)).collect();
        cols.extend(self.owner_pk.iter().map(|c| quote(&Self::owner_col(c))));
        cols.push("owner_rk".into());
        cols.push("_sprig_rk".into());
        let qs = vec!["?"; cols.len()].join(", ");
        format!(
            "INSERT {}INTO {}.{} ({}) VALUES ({qs})",
            if ignore { "IGNORE " } else { "" },
            quote(schema),
            quote(&self.physical()),
            cols.join(", ")
        )
    }
}

/// The owner of a lookup value: its primary key and routing hash.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnerRef {
    /// The owner's primary key.
    pub pk: Vec<KeyValueOwned>,
    /// The owner's routing hash.
    pub rk: RoutingHash,
}

/// Where a statement runs: a cluster and a schema.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Target {
    /// The cluster.
    pub cluster: ClusterId,
    /// The schema.
    pub schema: String,
}

/// Where lookup and owner rows live.
pub trait LookupPlacement: Send + Sync {
    /// The lookup shard holding hash `h` of the lookup group.
    ///
    /// # Errors
    /// 39403 without a map.
    fn lookup_target(&self, h: RoutingHash) -> Result<Target>;
    /// The owner shard holding routing hash `rk`.
    ///
    /// # Errors
    /// 39403 without a map.
    fn owner_target(&self, rk: RoutingHash) -> Result<Target>;
    /// Every lookup shard (building, dropping, scans).
    fn lookup_shards(&self) -> Vec<Target>;
    /// Every owner shard (back-fill).
    fn owner_shards(&self) -> Vec<Target>;
}

/// [`LookupPlacement`] from two shard maps: the lookup group's and the
/// owner group's, for one database.
#[derive(Debug, Clone)]
pub struct MapPlacement {
    /// The tenant.
    pub tenant: TenantId,
    /// The database the owner and the lookup tables are in.
    pub db: DbId,
    /// The lookup group's map.
    pub lookup_map: std::sync::Arc<ShardMap>,
    /// The owner group's map.
    pub owner_map: std::sync::Arc<ShardMap>,
}

impl MapPlacement {
    fn target(&self, m: &ShardMap, h: RoutingHash) -> Target {
        let r = m.route(h);
        Target {
            cluster: r.cluster.clone(),
            schema: shard_schema(self.tenant, self.db, m.group, r.shard),
        }
    }
    fn all(&self, m: &ShardMap) -> Vec<Target> {
        m.ranges
            .iter()
            .map(|r| Target {
                cluster: r.cluster.clone(),
                schema: shard_schema(self.tenant, self.db, m.group, r.shard),
            })
            .collect()
    }
}

impl LookupPlacement for MapPlacement {
    fn lookup_target(&self, h: RoutingHash) -> Result<Target> {
        Ok(self.target(&self.lookup_map, h))
    }
    fn owner_target(&self, rk: RoutingHash) -> Result<Target> {
        Ok(self.target(&self.owner_map, rk))
    }
    fn lookup_shards(&self) -> Vec<Target> {
        self.all(&self.lookup_map)
    }
    fn owner_shards(&self) -> Vec<Target> {
        self.all(&self.owner_map)
    }
}

/// The sessions of one transaction, as the lookup algorithm needs them.
/// The router's `DistTxn` (chapter 18) implements it; so does
/// [`direct::DirectTxn`].
#[async_trait::async_trait]
pub trait LookupSessions: Send {
    /// Runs `sql` in the **Pre** session on `t` (a transaction opened on
    /// first use, with `lookup.lock_wait_seconds`; committed before Main).
    async fn pre(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>>;
    /// Runs `sql` in the **Main** session on `t` (the transaction's own
    /// participant there).
    async fn main(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>>;
    /// Buffers a statement for **Post**: run in autocommit on `t` after
    /// Main commits.
    fn post(&mut self, t: &Target, sql: String, params: Params);
}

/// Outcome of a claim (`sprig_lookup_claims_total{result}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimResult {
    /// A new lookup row (or a value with a `NULL` part: nothing stored).
    Inserted,
    /// The value was already ours (an earlier claim in this transaction).
    Own,
    /// The lookup row was an orphan and now points at us.
    Reclaimed,
    /// A `write_only` or `backfilling` lookup met a real duplicate; it was
    /// counted, not raised.
    ConflictCounted,
}

/// The value hashed under the lookup member's key.
///
/// # Errors
/// Normalization errors.
pub fn value_hash(l: &LookupIndex, v: &[KeyValueOwned]) -> Result<RoutingHash> {
    let spec = l.spec()?;
    let refs: Vec<_> = v.iter().map(KeyValueOwned::as_ref).collect();
    let mut buf = Vec::with_capacity(32);
    StdNormalizer.normalize(&spec, &refs, &mut buf)?;
    Ok(routing_hash(&buf))
}

/// A key value as a MariaDB parameter. Strings travel as UTF-8 (the
/// connection's character set); MariaDB converts them to the column's.
///
/// # Errors
/// 39405 for a string that isn't valid in its character set.
pub fn to_param(v: &KeyValueOwned) -> Result<Value> {
    Ok(match v {
        KeyValueOwned::Null => Value::NULL,
        KeyValueOwned::Int(i) => match (i64::try_from(*i), u64::try_from(*i)) {
            (Ok(x), _) => Value::Int(x),
            (_, Ok(x)) => Value::UInt(x),
            _ => Value::Bytes(i.to_string().into_bytes()),
        },
        KeyValueOwned::Decimal(d) => Value::Bytes(d.clone().into_bytes()),
        KeyValueOwned::Double(d) => Value::Double(*d),
        KeyValueOwned::Bool(b) => Value::Int(i64::from(*b)),
        KeyValueOwned::Str { bytes, charset } => {
            Value::Bytes(collation::to_utf8(bytes, *charset)?.into_owned())
        }
        KeyValueOwned::Bytes(b) => Value::Bytes(b.clone()),
        KeyValueOwned::ObjectId(o) => Value::Bytes(o.to_vec()),
        KeyValueOwned::Uuid(u) => Value::Bytes(uuid_text(u).into_bytes()),
        KeyValueOwned::DateTime(d) => {
            Value::Date(d.year, d.month, d.day, d.hour, d.minute, d.second, d.micros)
        }
        KeyValueOwned::Timestamp { secs, micros } => {
            Value::Bytes(format!("{secs}.{micros:06}").into_bytes())
        }
    })
}

fn uuid_text(u: &[u8; 16]) -> String {
    let h = u.iter().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    });
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// `schema`.`table`.
#[must_use]
pub fn fq(t: &Target, table: &str) -> String {
    format!("{}.{}", quote(&t.schema), quote(table))
}

fn params(vals: Vec<Value>) -> Params {
    Params::Positional(vals)
}

fn values(v: &[KeyValueOwned]) -> Result<Vec<Value>> {
    v.iter().map(to_param).collect()
}

/// The errno of a MariaDB (or standard) error.
#[must_use]
pub fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => Some(errno),
        ErrorKind::Sprig(_) => None,
    }
}

fn duplicate(l: &LookupIndex, v: &[KeyValueOwned]) -> SprigError {
    let shown = v
        .iter()
        .map(|x| match x {
            KeyValueOwned::Str { bytes, charset } => collation::to_utf8(bytes, *charset)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default(),
            KeyValueOwned::Int(i) => i.to_string(),
            KeyValueOwned::Decimal(d) => d.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("-");
    SprigError::standard(
        1062,
        *b"23000",
        format!("Duplicate entry '{shown}' for key '{}'", l.index_name),
    )
}

fn lookup_unavailable(e: SprigError) -> SprigError {
    if errno(&e).is_some() || e.code() != Some(SprigCode::BackendUnavailable) {
        return e;
    }
    SprigError::new(
        SprigCode::LookupUnavailable,
        format!("a lookup shard is unreachable: {}", e.message),
    )
}

#[allow(clippy::many_single_char_names)] // the chapter's names: t, p, l, v
/// Claims value `v` of lookup `l` for `owner` inside transaction `t`
/// (chapter 19, `claim`). Call it before Main writes the owner row; claims
/// of one statement go in `lookup_id` order.
///
/// # Errors
/// 1062 when another committed owner holds `v` (public lookups only);
/// 39461 when the lookup shard is unreachable; lock waits and Galera
/// conflicts as MariaDB returns them (1205, 1213: retryable).
pub async fn claim(
    t: &mut dyn LookupSessions,
    p: &dyn LookupPlacement,
    l: &LookupIndex,
    v: &[KeyValueOwned],
    owner: &OwnerRef,
) -> Result<ClaimResult> {
    if v.iter().any(|x| matches!(x, KeyValueOwned::Null)) || !l.state.maintained() {
        return Ok(ClaimResult::Inserted);
    }
    let h = value_hash(l, v)?;
    let s = p.lookup_target(h)?;
    let mut ins = values(v)?;
    ins.extend(values(&owner.pk)?);
    ins.push(Value::UInt(owner.rk.0));
    ins.push(Value::UInt(h.0));
    // Non-unique lookups: the row key includes the owner, so a duplicate
    // is our own earlier claim.
    let insert_sql = l.insert_sql(&s.schema, !l.unique);
    match t.pre(&s, &insert_sql, params(ins)).await {
        Ok(_) => return Ok(ClaimResult::Inserted),
        Err(e) if errno(&e) == Some(1062) => {}
        Err(e) => return Err(lookup_unavailable(e)),
    }
    // Someone holds v: ours, an orphan, or a real owner.
    let mut sel = values(&owner.pk)?;
    sel.extend(values(v)?);
    let rows = t
        .pre(
            &s,
            &format!(
                "SELECT owner_rk, ({}) AS mine FROM {} WHERE {} FOR UPDATE",
                l.owner_match(),
                fq(&s, &l.physical()),
                l.value_where()
            ),
            params(sel),
        )
        .await
        .map_err(lookup_unavailable)?;
    let Some(row) = rows.into_iter().next() else {
        // Deleted meanwhile (a Post ran): insert again.
        let mut ins = values(v)?;
        ins.extend(values(&owner.pk)?);
        ins.push(Value::UInt(owner.rk.0));
        ins.push(Value::UInt(h.0));
        t.pre(&s, &l.insert_sql(&s.schema, false), params(ins))
            .await?;
        return Ok(ClaimResult::Inserted);
    };
    let rk0: u64 = row.get(0).unwrap_or_default();
    let mine: Option<i64> = row.get(1).unwrap_or_default();
    if mine == Some(1) && rk0 == owner.rk.0 {
        return Ok(ClaimResult::Own);
    }
    // The owner check: a locking read in Main on the holder's shard waits
    // for an in-flight insert or delete of it.
    let o = p.owner_target(RoutingHash(rk0))?;
    let found = t
        .main(
            &o,
            &format!(
                "SELECT 1 FROM {} WHERE {} LIMIT 1 LOCK IN SHARE MODE",
                fq(&o, &l.owner_table),
                l.value_where()
            ),
            params(values(v)?),
        )
        .await?;
    if !found.is_empty() {
        if l.state == LookupState::Public {
            return Err(duplicate(l, v));
        }
        return Ok(ClaimResult::ConflictCounted);
    }
    let mut upd = values(&owner.pk)?;
    upd.push(Value::UInt(owner.rk.0));
    upd.extend(values(v)?);
    let sets = l
        .owner_pk
        .iter()
        .map(|c| format!("{} = ?", quote(&LookupIndex::owner_col(c))))
        .chain(["owner_rk = ?".to_owned(), "prev_rk = NULL".to_owned()])
        .collect::<Vec<_>>()
        .join(", ");
    t.pre(
        &s,
        &format!(
            "UPDATE {} SET {sets} WHERE {}",
            fq(&s, &l.physical()),
            l.value_where()
        ),
        params(upd),
    )
    .await
    .map_err(lookup_unavailable)?;
    Ok(ClaimResult::Reclaimed)
}

#[allow(clippy::many_single_char_names)]
/// Buffers the release of `v` by `owner` for Post: `DELETE ... WHERE v = ?
/// AND owner_pk = ?`, a no-op if another transaction reclaimed `v`.
///
/// # Errors
/// Normalization errors.
pub fn release(
    t: &mut dyn LookupSessions,
    p: &dyn LookupPlacement,
    l: &LookupIndex,
    v: &[KeyValueOwned],
    owner: &OwnerRef,
) -> Result<()> {
    if v.iter().any(|x| matches!(x, KeyValueOwned::Null)) || !l.state.maintained() {
        return Ok(());
    }
    let s = p.lookup_target(value_hash(l, v)?)?;
    let mut prm = values(v)?;
    prm.extend(values(&owner.pk)?);
    t.post(
        &s,
        format!(
            "DELETE FROM {} WHERE {} AND {}",
            fq(&s, &l.physical()),
            l.value_where(),
            l.owner_match()
        ),
        params(prm),
    );
    Ok(())
}

/// Reads lookup rows outside any transaction.
#[async_trait::async_trait]
pub trait LookupReader: Send + Sync {
    /// Runs a read on `t` (autocommit, consistent read).
    async fn read(&self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>>;
}

/// The routing hashes of the owners of `values` (chapter 19, "Routing
/// through lookups"): `owner_rk` and, while an owner moves, `prev_rk`.
/// The router runs the original predicate on the shards of these hashes.
///
/// # Errors
/// 39461 if a lookup shard is unreachable: the router scatters instead.
pub async fn route(
    r: &dyn LookupReader,
    p: &dyn LookupPlacement,
    l: &LookupIndex,
    vals: &[Vec<KeyValueOwned>],
) -> Result<Vec<RoutingHash>> {
    let mut by_shard: BTreeMap<Target, Vec<&Vec<KeyValueOwned>>> = BTreeMap::new();
    for v in vals {
        if v.iter().any(|x| matches!(x, KeyValueOwned::Null)) {
            continue;
        }
        by_shard
            .entry(p.lookup_target(value_hash(l, v)?)?)
            .or_default()
            .push(v);
    }
    let mut out = Vec::new();
    for (t, vs) in by_shard {
        let cond = vec![format!("({})", l.value_where()); vs.len()].join(" OR ");
        let mut prm = Vec::new();
        for v in &vs {
            prm.extend(values(v)?);
        }
        let rows = r
            .read(
                &t,
                &format!(
                    "SELECT owner_rk, prev_rk FROM {} WHERE {cond}",
                    fq(&t, &l.physical())
                ),
                params(prm),
            )
            .await
            .map_err(lookup_unavailable)?;
        for row in rows {
            let rk: u64 = row.get(0).unwrap_or_default();
            out.push(RoutingHash(rk));
            if let Some(Some(prev)) = row.get::<Option<u64>, _>(1) {
                out.push(RoutingHash(prev));
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// A string value in a lookup, for tests and tools.
#[must_use]
pub fn text(s: &str) -> KeyValueOwned {
    KeyValueOwned::Str {
        bytes: s.as_bytes().to_vec(),
        charset: Charset::Utf8mb4,
    }
}

/// The catalog rows of lookups: create, change state, count conflicts.
#[derive(Debug, Clone, Copy, Default)]
pub struct LookupCatalog;

impl LookupCatalog {
    /// Inserts `l` (state as given, normally `building`).
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn insert(&self, c: &mut mysql_async::Conn, l: &LookupIndex) -> Result<()> {
        use mysql_async::prelude::Queryable;
        let cols: Vec<&str> = l.columns.iter().map(|c| c.name.as_str()).collect();
        c.exec_drop(
            "INSERT INTO lookup_indexes (lookup_id, object_id, index_name, columns, is_unique,
               origin, physical_name, group_id, state, auto_index_id, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))",
            (
                l.id.0,
                l.owner.0,
                &l.index_name,
                serde_json::to_string(&cols).unwrap_or_default(),
                l.unique,
                l.origin.as_str(),
                l.physical(),
                l.group.0,
                l.state.as_str(),
                l.auto_index_id,
            ),
        )
        .await
        .map_err(crate::groups::db_err)
    }

    /// Moves `l` to `state` if it is in `from`. Returns whether it moved.
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn set_state(
        &self,
        c: &mut mysql_async::Conn,
        l: LookupId,
        from: LookupState,
        state: LookupState,
    ) -> Result<bool> {
        use mysql_async::prelude::Queryable;
        c.exec_drop(
            "UPDATE lookup_indexes SET state = ?, updated_at = UTC_TIMESTAMP(6)
             WHERE lookup_id = ? AND state = ?",
            (state.as_str(), l.0, from.as_str()),
        )
        .await
        .map_err(crate::groups::db_err)?;
        Ok(c.affected_rows() == 1)
    }

    /// Adds `n` to `conflicts`.
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn count_conflicts(
        &self,
        c: &mut mysql_async::Conn,
        l: LookupId,
        n: u64,
    ) -> Result<()> {
        use mysql_async::prelude::Queryable;
        c.exec_drop(
            "UPDATE lookup_indexes SET conflicts = conflicts + ? WHERE lookup_id = ?",
            (n, l.0),
        )
        .await
        .map_err(crate::groups::db_err)
    }
}

/// The hook automatic indexing (chapter 13) calls when it proposes a global
/// lookup for a scatter query: an `auto`, non-unique lookup named
/// `sprig_auto_<8 hex>`, in state `building`. The build itself
/// ([`direct::build`]) is the same as for unique lookups; chapter 13 then
/// watches it for `auto_index.observe_days` and drops it ([`direct::drop`])
/// unless p95 improved by `auto_index.min_improvement_pct`.
#[must_use]
pub fn propose_auto_lookup(
    id: LookupId,
    owner: ObjectId,
    owner_table: &str,
    columns: Vec<LookupColumn>,
    owner_pk: Vec<LookupColumn>,
    lookup_group: GroupId,
    auto_index_id: u64,
) -> LookupIndex {
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    LookupIndex {
        id,
        owner,
        owner_table: owner_table.to_owned(),
        index_name: auto_lookup_name(owner, &names),
        columns,
        owner_pk,
        unique: false,
        origin: LookupOrigin::Auto,
        group: lookup_group,
        state: LookupState::Building,
        auto_index_id: Some(auto_index_id),
    }
}

#[cfg(test)]
mod tests;
