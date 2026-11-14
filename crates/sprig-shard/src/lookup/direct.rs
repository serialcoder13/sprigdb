//! A plain [`LookupSessions`] over `mysql_async` pools, and the lookup
//! jobs built on it: build with back-fill and verification, drop, and the
//! orphan scan.
//!
//! [`DirectTxn`] keeps one Pre and one Main connection per cluster and
//! commits in the fixed order: every Pre, then Main, then the buffered
//! Post statements in autocommit. Its Main commit is local per cluster; a
//! Main spanning clusters needs the router's `DistTxn` (XA), which
//! implements the same trait.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Pool, Row, Value};
use sprig_common::ids::ClusterId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use super::{
    LookupCatalog, LookupIndex, LookupPlacement, LookupReader, LookupSessions, LookupState, Target,
    errno, fq,
};
use crate::groups::db_err;
use crate::hash::routing_hash;
use crate::normalize::{Normalizer, StdNormalizer};

/// Pools by cluster.
pub type Pools = Arc<HashMap<ClusterId, Pool>>;

/// A pool that keeps idle connections (a pool with
/// `min = 0` needs an idle TTL, or it closes every returned connection).
#[must_use]
pub fn pool(url: &str, max: usize) -> Option<Pool> {
    let opts = mysql_async::Opts::from_url(url).ok()?;
    let po = mysql_async::PoolOpts::default()
        .with_constraints(mysql_async::PoolConstraints::new(0, max.max(1))?)
        .with_inactive_connection_ttl(Duration::from_secs(60));
    Some(Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(po),
    ))
}

fn get_pool<'a>(pools: &'a Pools, c: &ClusterId) -> Result<&'a Pool> {
    pools.get(c).ok_or_else(|| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("no connection pool for cluster {c}"),
        )
    })
}

async fn conn(pools: &Pools, c: &ClusterId) -> Result<Conn> {
    get_pool(pools, c)?.get_conn().await.map_err(db_err)
}

/// One transaction's Pre, Main and Post, directly on pooled connections.
pub struct DirectTxn {
    pools: Pools,
    lock_wait_seconds: u64,
    pre: BTreeMap<ClusterId, Conn>,
    main: BTreeMap<ClusterId, Conn>,
    post: Vec<(Target, String, Params)>,
    /// Post statements that failed in the last commit (orphans left for
    /// the scan; `sprig_lookup_post_failures_total`).
    pub post_failures: u64,
}

impl std::fmt::Debug for DirectTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectTxn")
            .field("pre", &self.pre.keys().collect::<Vec<_>>())
            .field("main", &self.main.keys().collect::<Vec<_>>())
            .field("post", &self.post.len())
            .field("post_failures", &self.post_failures)
            .finish_non_exhaustive()
    }
}

impl DirectTxn {
    /// A new transaction; `lock_wait_seconds` is `lookup.lock_wait_seconds`.
    #[must_use]
    pub fn new(pools: Pools, lock_wait_seconds: u64) -> Self {
        Self {
            pools,
            lock_wait_seconds,
            pre: BTreeMap::new(),
            main: BTreeMap::new(),
            post: Vec::new(),
            post_failures: 0,
        }
    }

    async fn open(&self, c: &ClusterId) -> Result<Conn> {
        let mut k = conn(&self.pools, c).await?;
        k.query_drop(format!(
            "SET SESSION innodb_lock_wait_timeout = {}",
            self.lock_wait_seconds.max(1)
        ))
        .await
        .map_err(db_err)?;
        k.query_drop("BEGIN").await.map_err(db_err)?;
        Ok(k)
    }

    /// Runs `sql` in Main on `t` (data writes go here).
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn exec_main(&mut self, t: &Target, sql: &str, p: Params) -> Result<Vec<Row>> {
        LookupSessions::main(self, t, sql, p).await
    }

    /// Commits: every Pre, then Main, then Post (failures there only leave
    /// orphans and are counted).
    ///
    /// # Errors
    /// The first failing Pre or Main commit; everything not yet committed
    /// is rolled back.
    pub async fn commit(mut self) -> Result<u64> {
        let pre = std::mem::take(&mut self.pre);
        let mut pre_iter = pre.into_iter();
        while let Some((_, mut c)) = pre_iter.next() {
            if let Err(e) = c.query_drop("COMMIT").await {
                for (_, mut r) in pre_iter.by_ref() {
                    let _ = r.query_drop("ROLLBACK").await;
                }
                self.rollback_main().await;
                return Err(db_err(e));
            }
        }
        let main = std::mem::take(&mut self.main);
        let mut main_iter = main.into_iter();
        while let Some((_, mut c)) = main_iter.next() {
            if let Err(e) = c.query_drop("COMMIT").await {
                for (_, mut r) in main_iter.by_ref() {
                    let _ = r.query_drop("ROLLBACK").await;
                }
                return Err(db_err(e));
            }
        }
        let mut failures = 0;
        for (t, sql, p) in std::mem::take(&mut self.post) {
            let r = async {
                let mut c = conn(&self.pools, &t.cluster).await?;
                c.exec_drop(sql, p).await.map_err(db_err)
            }
            .await;
            if let Err(e) = r {
                tracing::warn!(error = %e, "lookup Post delete failed; the orphan scan will remove it");
                failures += 1;
            }
        }
        Ok(failures)
    }

    /// A crash between the Pre and Main commits, for tests and simulation:
    /// commits every Pre session and rolls Main back, leaving orphans.
    pub async fn abort_after_pre(mut self) {
        for (_, mut c) in std::mem::take(&mut self.pre) {
            let _ = c.query_drop("COMMIT").await;
        }
        self.rollback_main().await;
    }

    async fn rollback_main(&mut self) {
        for (_, mut c) in std::mem::take(&mut self.main) {
            let _ = c.query_drop("ROLLBACK").await;
        }
    }

    /// Rolls everything back.
    pub async fn rollback(mut self) {
        for (_, mut c) in std::mem::take(&mut self.pre) {
            let _ = c.query_drop("ROLLBACK").await;
        }
        self.rollback_main().await;
    }
}

#[async_trait::async_trait]
impl LookupSessions for DirectTxn {
    async fn pre(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        if !self.pre.contains_key(&t.cluster) {
            let c = self.open(&t.cluster).await?;
            self.pre.insert(t.cluster.clone(), c);
        }
        let c = self
            .pre
            .get_mut(&t.cluster)
            .ok_or_else(|| SprigError::internal("pre session vanished"))?;
        c.exec(sql, params).await.map_err(db_err)
    }

    async fn main(&mut self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        if !self.main.contains_key(&t.cluster) {
            let c = self.open(&t.cluster).await?;
            self.main.insert(t.cluster.clone(), c);
        }
        let c = self
            .main
            .get_mut(&t.cluster)
            .ok_or_else(|| SprigError::internal("main session vanished"))?;
        c.exec(sql, params).await.map_err(db_err)
    }

    fn post(&mut self, t: &Target, sql: String, params: Params) {
        self.post.push((t.clone(), sql, params));
    }
}

/// [`LookupReader`] on pooled connections.
#[derive(Debug, Clone)]
pub struct DirectReader(pub Pools);

#[async_trait::async_trait]
impl LookupReader for DirectReader {
    async fn read(&self, t: &Target, sql: &str, params: Params) -> Result<Vec<Row>> {
        let mut c = conn(&self.0, &t.cluster).await?;
        c.exec(sql, params).await.map_err(db_err)
    }
}

/// What a build did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BuildReport {
    /// Owner rows read.
    pub rows: u64,
    /// Lookup rows inserted by the back-fill.
    pub inserted: u64,
    /// Values already present (claimed by writers meanwhile, or shared by
    /// owners of a non-unique lookup).
    pub skipped: u64,
    /// Owner rows the verification found without their lookup row, or
    /// with a stale routing hash, and repaired (rows a writer committed
    /// after the back-fill read their range, or a rebuild's old hashes).
    pub repaired: u64,
}

/// `innodb_lock_wait_timeout` of the back-fill's locking reads.
const BACKFILL_LOCK_WAIT_SECS: u64 = 10;
/// Attempts of one back-fill chunk on 1205 or 1213.
const BACKFILL_RETRIES: u32 = 20;

async fn move_state(
    catalog: &mut Conn,
    l: &mut LookupIndex,
    from: LookupState,
    to: LookupState,
) -> Result<()> {
    if !LookupCatalog.set_state(catalog, l.id, from, to).await? {
        return Err(SprigError::new(
            SprigCode::LookupUnavailable,
            format!(
                "lookup index {} left state {} while it was being built",
                l.index_name,
                from.as_str()
            ),
        ));
    }
    l.state = to;
    Ok(())
}

/// Creates `l`'s table (and the shard schema, with its decision table) on
/// every lookup shard.
///
/// # Errors
/// MariaDB's errors.
pub async fn create_tables(pools: &Pools, p: &dyn LookupPlacement, l: &LookupIndex) -> Result<()> {
    for t in p.lookup_shards() {
        let mut c = conn(pools, &t.cluster).await?;
        c.query_drop(format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote(&t.schema)
        ))
        .await
        .map_err(db_err)?;
        c.query_drop(sprig_catalog::ddl::dtx_table_sql(&t.schema))
            .await
            .map_err(db_err)?;
        c.query_drop(l.create_table_sql(&t.schema))
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

#[allow(clippy::many_single_char_names)] // the build's steps, in order
/// Builds `l` from whatever state it is in, so a build a crash stopped
/// resumes where it was:
///
/// ```text
/// building     create the table on every lookup shard → write_only, publish
/// write_only   → backfilling (writers already claim and release)
/// backfilling  back-fill every owner shard (primary-key chunks of `chunk`
///              rows, read LOCK IN SHARE MODE so writes in flight finish
///              first, INSERT IGNORE), then verify: every owner row's value
///              must name that owner; a missing row or a stale hash is
///              repaired with a claim, a value another live owner holds is a
///              real duplicate (drop the lookup, 1062)
///              → public, publish
/// ```
///
/// `catalog` is a connection to the tenant catalog (its default
/// database). `publish` runs after each state change writers must see: it
/// raises the catalog version and returns once every gateway can have
/// loaded it. Each chunk touches `updated_at`, so another job can tell a
/// running build from a dead one ([`super::admin::BUILD_LEASE`]).
///
/// # Errors
/// 1062 on a real duplicate (the lookup is dropped); MariaDB's errors.
pub async fn build<W, F>(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &mut LookupIndex,
    chunk: usize,
    mut publish: W,
) -> Result<BuildReport>
where
    W: FnMut() -> F + Send,
    F: std::future::Future<Output = Result<()>> + Send,
{
    if l.state == LookupState::Building {
        create_tables(pools, p, l).await?;
        move_state(catalog, l, LookupState::Building, LookupState::WriteOnly).await?;
        publish().await?;
    }
    if l.state == LookupState::WriteOnly {
        move_state(catalog, l, LookupState::WriteOnly, LookupState::Backfilling).await?;
    }
    if l.state != LookupState::Backfilling {
        return Err(SprigError::internal(format!(
            "lookup index {} is {}, not being built",
            l.index_name,
            l.state.as_str()
        )));
    }
    let mut report = backfill(pools, catalog, p, l, chunk).await?;
    match verify(pools, catalog, p, l, chunk).await {
        Ok(n) => report.repaired = n,
        Err(e) if errno(&e) == Some(1062) => {
            drop_with(pools, catalog, p, l, &mut publish).await?;
            return Err(e);
        }
        Err(e) => return Err(e),
    }
    move_state(catalog, l, LookupState::Backfilling, LookupState::Public).await?;
    publish().await?;
    Ok(report)
}

/// The owner columns a build reads: the values, the primary key, the
/// routing hash.
fn owner_select(l: &LookupIndex, o: &Target, cond: &str, chunk: usize, lock: bool) -> String {
    let vcols: Vec<String> = l.columns.iter().map(|c| quote(&c.name)).collect();
    let pk_cols: Vec<String> = l.owner_pk.iter().map(|c| quote(&c.name)).collect();
    format!(
        "SELECT {}, {}, _sprig_rk FROM {} {cond} ORDER BY {} LIMIT {chunk}{}",
        vcols.join(", "),
        pk_cols.join(", "),
        fq(o, &l.owner_table),
        pk_cols.join(", "),
        if lock { " LOCK IN SHARE MODE" } else { "" }
    )
}

fn after_cond(l: &LookupIndex, after: Option<&Vec<Value>>) -> (String, Vec<Value>) {
    match after {
        None => (String::new(), Vec::new()),
        Some(k) => {
            let pk_cols: Vec<String> = l.owner_pk.iter().map(|c| quote(&c.name)).collect();
            (
                format!(
                    "WHERE ({}) > ({})",
                    pk_cols.join(", "),
                    vec!["?"; pk_cols.len()].join(", ")
                ),
                k.clone(),
            )
        }
    }
}

/// One chunk of owner rows, read in a short transaction with share locks:
/// a write in flight on those rows finishes (and commits or rolls back)
/// before the chunk is read, so the back-fill sees it.
async fn locked_chunk(oc: &mut Conn, sql: &str, prm: Vec<Value>) -> Result<Vec<Row>> {
    let mut tries = 0;
    loop {
        oc.query_drop("BEGIN").await.map_err(db_err)?;
        let r: std::result::Result<Vec<Row>, _> = oc.exec(sql, Params::Positional(prm.clone())).await;
        match r {
            Ok(rows) => {
                oc.query_drop("COMMIT").await.map_err(db_err)?;
                return Ok(rows);
            }
            Err(e) => {
                let _ = oc.query_drop("ROLLBACK").await;
                let e = db_err(e);
                tries += 1;
                if matches!(errno(&e), Some(1205 | 1213)) && tries < BACKFILL_RETRIES {
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(tries))).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

#[allow(clippy::many_single_char_names)]
async fn backfill(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &LookupIndex,
    chunk: usize,
) -> Result<BuildReport> {
    let mut report = BuildReport::default();
    let spec = l.spec()?;
    let nv = l.columns.len();
    let np = l.owner_pk.len();
    for o in p.owner_shards() {
        let mut oc = conn(pools, &o.cluster).await?;
        oc.query_drop(format!(
            "SET SESSION innodb_lock_wait_timeout = {BACKFILL_LOCK_WAIT_SECS}"
        ))
        .await
        .map_err(db_err)?;
        let mut after: Option<Vec<Value>> = None;
        loop {
            let (cond, prm) = after_cond(l, after.as_ref());
            let rows = locked_chunk(&mut oc, &owner_select(l, &o, &cond, chunk, true), prm).await?;
            if rows.is_empty() {
                break;
            }
            report.rows += rows.len() as u64;
            let mut batches: BTreeMap<Target, Vec<Vec<Value>>> = BTreeMap::new();
            for r in &rows {
                let vals: Vec<Value> = (0..nv).map(|i| r[i].clone()).collect();
                if vals.contains(&Value::NULL) {
                    continue;
                }
                if r[nv + np] == Value::NULL {
                    return Err(rk_missing(l));
                }
                let h = hash_values(&spec, l, &vals)?;
                let mut ins = vals;
                ins.extend((nv..nv + np).map(|i| r[i].clone()));
                ins.push(r[nv + np].clone());
                ins.push(Value::UInt(h.0));
                batches.entry(p.lookup_target(h)?).or_default().push(ins);
            }
            after = rows
                .last()
                .map(|r| (nv..nv + np).map(|i| r[i].clone()).collect());
            for (t, batch) in batches {
                let mut c = conn(pools, &t.cluster).await?;
                let sql = l.insert_sql(&t.schema, true);
                for ins in batch {
                    c.exec_drop(&sql, Params::Positional(ins))
                        .await
                        .map_err(db_err)?;
                    if c.affected_rows() == 1 {
                        report.inserted += 1;
                    } else {
                        report.skipped += 1;
                    }
                }
            }
            LookupCatalog.touch(catalog, l.id).await?;
        }
    }
    Ok(report)
}

fn rk_missing(l: &LookupIndex) -> SprigError {
    SprigError::new(
        SprigCode::LookupUnavailable,
        format!(
            "{} has rows without their routing hash (_sprig_rk) yet; build lookup index {} \
             once its group's back-fill is done",
            l.owner_table, l.index_name
        ),
    )
}

/// A value read back from MariaDB as the key value a client would send:
/// strings by their bytes (UTF-8 on the wire), numbers exactly.
fn owned_of(class: crate::key::KeyClass, v: &Value) -> crate::key::KeyValueOwned {
    use crate::key::{KeyClass, KeyValueOwned};
    match (class, v) {
        (_, Value::NULL) => KeyValueOwned::Null,
        (_, Value::Int(i)) => KeyValueOwned::Int(i128::from(*i)),
        (_, Value::UInt(u)) => KeyValueOwned::Int(i128::from(*u)),
        (KeyClass::Numeric, Value::Bytes(b)) => {
            let t = String::from_utf8_lossy(b).into_owned();
            match t.parse::<i128>() {
                Ok(i) => KeyValueOwned::Int(i),
                Err(_) => KeyValueOwned::Decimal(t),
            }
        }
        (KeyClass::String { .. }, Value::Bytes(b)) => KeyValueOwned::Str {
            bytes: b.clone(),
            charset: crate::collation::Charset::Utf8mb4,
        },
        (_, Value::Bytes(b)) => KeyValueOwned::Bytes(b.clone()),
        (_, Value::Double(d)) => KeyValueOwned::Double(*d),
        (_, other) => KeyValueOwned::Str {
            bytes: other.as_sql(true).into_bytes(),
            charset: crate::collation::Charset::Utf8mb4,
        },
    }
}

/// Hashes values read back from MariaDB, as the gateway would hash the
/// same values from a client: strings by the column's collation, numbers
/// exactly. Supports the classes lookups use: numeric, string,
/// binary.
fn hash_values(
    spec: &crate::key::KeySpec,
    l: &LookupIndex,
    vals: &[Value],
) -> Result<crate::key::RoutingHash> {
    use crate::key::KeyValueOwned;
    let owned: Vec<KeyValueOwned> = l
        .columns
        .iter()
        .zip(vals)
        .map(|(c, v)| match (c.class, v) {
            // An integer read as text by the text protocol hashes as the
            // integer, as before.
            (crate::key::KeyClass::Numeric, Value::Bytes(b)) => {
                KeyValueOwned::Decimal(String::from_utf8_lossy(b).into_owned())
            }
            _ => owned_of(c.class, v),
        })
        .collect();
    let refs: Vec<_> = owned.iter().map(KeyValueOwned::as_ref).collect();
    let mut buf = Vec::new();
    StdNormalizer.normalize(spec, &refs, &mut buf)?;
    Ok(routing_hash(&buf))
}

#[allow(clippy::many_single_char_names, clippy::too_many_lines)]
/// Step 4, in chunks: every owner row's value names that owner under its
/// current routing hash. Rows that don't are claimed now, as a writer
/// would: a missing lookup row is inserted, an orphan or our own row under
/// an old hash is taken over, and a value another live owner holds (the
/// claim's locking owner check) is a real duplicate: 1062. Returns the rows
/// repaired.
async fn verify(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &LookupIndex,
    chunk: usize,
) -> Result<u64> {
    use crate::key::{KeyValueOwned, RoutingHash};
    let spec = l.spec()?;
    let nv = l.columns.len();
    let np = l.owner_pk.len();
    let mut repaired = 0;
    let pk_cols: Vec<String> = l
        .owner_pk
        .iter()
        .map(|c| quote(&format!("owner_{}", c.name)))
        .collect();
    let vcols: Vec<String> = l.columns.iter().map(|c| quote(&c.name)).collect();
    for o in p.owner_shards() {
        let mut oc = conn(pools, &o.cluster).await?;
        let mut after: Option<Vec<Value>> = None;
        loop {
            let (cond, prm) = after_cond(l, after.as_ref());
            let rows: Vec<Row> = oc
                .exec(owner_select(l, &o, &cond, chunk, false), Params::Positional(prm))
                .await
                .map_err(db_err)?;
            if rows.is_empty() {
                break;
            }
            after = rows
                .last()
                .map(|r| (nv..nv + np).map(|i| r[i].clone()).collect());
            // Owner rows by lookup shard: (values, primary key, hash).
            let mut by_shard: BTreeMap<Target, Vec<(Vec<Value>, Vec<Value>, Value)>> =
                BTreeMap::new();
            for r in &rows {
                let vals: Vec<Value> = (0..nv).map(|i| r[i].clone()).collect();
                if vals.contains(&Value::NULL) {
                    continue;
                }
                if r[nv + np] == Value::NULL {
                    return Err(rk_missing(l));
                }
                let pk: Vec<Value> = (nv..nv + np).map(|i| r[i].clone()).collect();
                let t = p.lookup_target(hash_values(&spec, l, &vals)?)?;
                by_shard
                    .entry(t)
                    .or_default()
                    .push((vals, pk, r[nv + np].clone()));
            }
            for (t, owners) in by_shard {
                // The lookup rows that name these owners exactly.
                let tuple = vec!["?"; nv + np + 1].join(", ");
                let mut prm = Vec::new();
                for (v, pk, rk) in &owners {
                    prm.extend(v.iter().cloned());
                    prm.extend(pk.iter().cloned());
                    prm.push(rk.clone());
                }
                let mut c = conn(pools, &t.cluster).await?;
                let found: Vec<Row> = c
                    .exec(
                        format!(
                            "SELECT {}, owner_rk FROM {} WHERE ({}, {}, owner_rk) IN ({})",
                            pk_cols.join(", "),
                            fq(&t, &l.physical()),
                            vcols.join(", "),
                            pk_cols.join(", "),
                            vec![format!("({tuple})"); owners.len()].join(", ")
                        ),
                        Params::Positional(prm),
                    )
                    .await
                    .map_err(db_err)?;
                let have: std::collections::HashSet<String> = found
                    .into_iter()
                    .map(|r| {
                        let pk: Vec<Value> = (0..np).map(|i| r[i].clone()).collect();
                        let rk: u64 = r.get(np).unwrap_or_default();
                        owner_key(&pk, rk)
                    })
                    .collect();
                for (vals, pk, rk) in owners {
                    let rk: u64 = mysql_async::from_value_opt(rk).unwrap_or_default();
                    if have.contains(&owner_key(&pk, rk)) {
                        continue;
                    }
                    let value: Vec<KeyValueOwned> = l
                        .columns
                        .iter()
                        .zip(&vals)
                        .map(|(col, v)| owned_of(col.class, v))
                        .collect();
                    let owner = super::OwnerRef {
                        pk: l
                            .owner_pk
                            .iter()
                            .zip(&pk)
                            .map(|(col, v)| owned_of(col.class, v))
                            .collect(),
                        rk: RoutingHash(rk),
                    };
                    if l.unique {
                        let mut txn = DirectTxn::new(Arc::clone(pools), BACKFILL_LOCK_WAIT_SECS);
                        let r = super::claim(&mut txn, p, l, &value, &owner).await;
                        match r {
                            Ok(super::ClaimResult::ConflictCounted) => {
                                txn.rollback().await;
                                let shown = vals
                                    .iter()
                                    .map(|v| v.as_sql(true))
                                    .collect::<Vec<_>>()
                                    .join("-");
                                return Err(SprigError::standard(
                                    1062,
                                    *b"23000",
                                    format!("Duplicate entry {shown} for key '{}'", l.index_name),
                                ));
                            }
                            Ok(_) => {
                                txn.commit().await?;
                            }
                            Err(e) => {
                                txn.rollback().await;
                                return Err(e);
                            }
                        }
                    } else {
                        let h = hash_values(&spec, l, &vals)?;
                        let mut ins = vals.clone();
                        ins.extend(pk.iter().cloned());
                        ins.push(Value::UInt(rk));
                        ins.push(Value::UInt(h.0));
                        let sql = format!(
                            "{} ON DUPLICATE KEY UPDATE owner_rk = VALUES(owner_rk), prev_rk = NULL",
                            l.insert_sql(&t.schema, false)
                        );
                        c.exec_drop(sql, Params::Positional(ins))
                            .await
                            .map_err(db_err)?;
                    }
                    repaired += 1;
                }
            }
            LookupCatalog.touch(catalog, l.id).await?;
        }
    }
    Ok(repaired)
}

/// An owner's primary key and hash as one comparable text.
fn owner_key(pk: &[Value], rk: u64) -> String {
    let mut k = pk
        .iter()
        .map(|v| v.as_sql(true))
        .collect::<Vec<_>>()
        .join("\u{1}");
    k.push('\u{2}');
    k.push_str(&rk.to_string());
    k
}

/// Drops `l` while gateways may still maintain it: `dropping` (writers stop
/// claiming), `publish`, then the table on every lookup shard and the
/// catalog row, `publish` again.
///
/// # Errors
/// MariaDB's errors; `publish`'s.
pub async fn drop_with<W, F>(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &mut LookupIndex,
    publish: &mut W,
) -> Result<()>
where
    W: FnMut() -> F + Send,
    F: std::future::Future<Output = Result<()>> + Send,
{
    catalog
        .exec_drop(
            "UPDATE lookup_indexes SET state = 'dropping', updated_at = UTC_TIMESTAMP(6)
             WHERE lookup_id = ?",
            (l.id.0,),
        )
        .await
        .map_err(db_err)?;
    l.state = LookupState::Dropping;
    publish().await?;
    for t in p.lookup_shards() {
        let mut c = conn(pools, &t.cluster).await?;
        c.query_drop(format!("DROP TABLE IF EXISTS {}", fq(&t, &l.physical())))
            .await
            .map_err(db_err)?;
    }
    catalog
        .exec_drop("DELETE FROM lookup_indexes WHERE lookup_id = ?", (l.id.0,))
        .await
        .map_err(db_err)?;
    publish().await
}

/// Drops `l`: `dropping`, then the table on every lookup shard, then the
/// catalog row (no gateway is told; [`drop_with`] tells them).
///
/// # Errors
/// MariaDB's errors.
pub async fn drop_lookup(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &mut LookupIndex,
) -> Result<()> {
    drop_with(pools, catalog, p, l, &mut || async { Ok(()) }).await
}

#[allow(clippy::many_single_char_names, clippy::too_many_lines)] // one pass, in order
/// The orphan scan: reads each lookup shard in chunks of 100
/// rows; per row, locks it `FOR UPDATE`, checks its owner `LOCK IN SHARE
/// MODE`, and deletes it with the full guard if the owner is gone.
/// Returns the number of orphans removed.
///
/// # Errors
/// MariaDB's errors.
pub async fn orphan_scan(
    pools: &Pools,
    p: &dyn LookupPlacement,
    l: &LookupIndex,
    lock_wait_seconds: u64,
) -> Result<u64> {
    let vcols: Vec<String> = l.columns.iter().map(|c| quote(&c.name)).collect();
    let pk_cols: Vec<String> = l
        .owner_pk
        .iter()
        .map(|c| quote(&format!("owner_{}", c.name)))
        .collect();
    let mut key_cols = vcols.clone();
    if !l.unique {
        key_cols.extend(pk_cols.iter().cloned());
    }
    let nv = vcols.len();
    let np = pk_cols.len();
    let mut removed = 0;
    for s in p.lookup_shards() {
        let mut after: Option<Vec<Value>> = None;
        loop {
            let (cond, prm) = match &after {
                None => (String::new(), Vec::new()),
                Some(k) => (
                    format!(
                        "WHERE ({}) > ({})",
                        key_cols.join(", "),
                        vec!["?"; key_cols.len()].join(", ")
                    ),
                    k.clone(),
                ),
            };
            let rows: Vec<Row> = {
                let mut c = conn(pools, &s.cluster).await?;
                c.exec(
                    format!(
                        "SELECT {}, {}, owner_rk FROM {} {cond} ORDER BY {} LIMIT 100",
                        vcols.join(", "),
                        pk_cols.join(", "),
                        fq(&s, &l.physical()),
                        key_cols.join(", ")
                    ),
                    Params::Positional(prm),
                )
                .await
                .map_err(db_err)?
            };
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|r| {
                let mut k: Vec<Value> = (0..nv).map(|i| r[i].clone()).collect();
                if !l.unique {
                    k.extend((nv..nv + np).map(|i| r[i].clone()));
                }
                k
            });
            for r in rows {
                let vals: Vec<Value> = (0..nv).map(|i| r[i].clone()).collect();
                let pk: Vec<Value> = (nv..nv + np).map(|i| r[i].clone()).collect();
                let rk = r[nv + np].clone();
                let mut guard = vals.clone();
                guard.extend(pk.iter().cloned());
                guard.push(rk.clone());
                let full = format!(
                    "{} AND {} AND owner_rk = ?",
                    l.value_where(),
                    l.owner_match()
                );
                let mut t = DirectTxn::new(Arc::clone(pools), lock_wait_seconds);
                let locked = t
                    .pre(
                        &s,
                        &format!(
                            "SELECT 1 FROM {} WHERE {full} FOR UPDATE",
                            fq(&s, &l.physical())
                        ),
                        Params::Positional(guard.clone()),
                    )
                    .await?;
                if locked.is_empty() {
                    t.rollback().await;
                    continue;
                }
                let owner_rk: u64 = mysql_async::from_value_opt(rk).unwrap_or_default();
                let o = p.owner_target(crate::key::RoutingHash(owner_rk))?;
                let mut owner_prm = vals.clone();
                let pk_eq = l
                    .owner_pk
                    .iter()
                    .map(|c| format!("{} <=> ?", quote(&c.name)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                owner_prm.extend(pk);
                let found = t
                    .main(
                        &o,
                        &format!(
                            "SELECT 1 FROM {} WHERE {} AND {pk_eq} LOCK IN SHARE MODE",
                            fq(&o, &l.owner_table),
                            l.value_where()
                        ),
                        Params::Positional(owner_prm),
                    )
                    .await;
                match found {
                    Ok(f) if f.is_empty() => {
                        t.pre(
                            &s,
                            &format!("DELETE FROM {} WHERE {full}", fq(&s, &l.physical())),
                            Params::Positional(guard),
                        )
                        .await?;
                        t.commit().await?;
                        removed += 1;
                    }
                    Ok(_) => t.rollback().await,
                    Err(e) if errno(&e) == Some(1146) => {
                        // The owner shard's table is gone (a moved range): skip.
                        t.rollback().await;
                    }
                    Err(e) => {
                        t.rollback().await;
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(removed)
}
