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
}

#[allow(clippy::many_single_char_names, clippy::too_many_lines)] // the build's steps, in order
/// Builds `l`: creates the table on every lookup
/// shard, `write_only`, back-fills every owner shard in primary-key
/// chunks of `chunk` rows, checks that every owner's value points at it
/// (else a real duplicate: drops `l` and fails with 1062), then `public`.
/// `catalog` is a connection to the tenant catalog; `wait_gateways` is
/// called after `write_only` and must return once every gateway has the
/// catalog version that shows it.
///
/// # Errors
/// 1062 on a real duplicate; MariaDB's errors.
pub async fn build<W, F>(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &mut LookupIndex,
    chunk: usize,
    wait_gateways: W,
) -> Result<BuildReport>
where
    W: FnOnce() -> F + Send,
    F: std::future::Future<Output = ()> + Send,
{
    let cat = LookupCatalog;
    for t in p.lookup_shards() {
        let mut c = conn(pools, &t.cluster).await?;
        c.query_drop(format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote(&t.schema)
        ))
        .await
        .map_err(db_err)?;
        c.query_drop(l.create_table_sql(&t.schema))
            .await
            .map_err(db_err)?;
    }
    cat.set_state(catalog, l.id, LookupState::Building, LookupState::WriteOnly)
        .await?;
    l.state = LookupState::WriteOnly;
    wait_gateways().await;
    cat.set_state(
        catalog,
        l.id,
        LookupState::WriteOnly,
        LookupState::Backfilling,
    )
    .await?;
    l.state = LookupState::Backfilling;
    let mut report = BuildReport::default();
    let spec = l.spec()?;
    let vcols: Vec<String> = l.columns.iter().map(|c| quote(&c.name)).collect();
    let pk_cols: Vec<String> = l.owner_pk.iter().map(|c| quote(&c.name)).collect();
    let nv = vcols.len();
    let np = pk_cols.len();
    for o in p.owner_shards() {
        let mut oc = conn(pools, &o.cluster).await?;
        let mut after: Option<Vec<Value>> = None;
        loop {
            let (cond, prm) = match &after {
                None => (String::new(), Vec::new()),
                Some(k) => (
                    format!(
                        "WHERE ({}) > ({})",
                        pk_cols.join(", "),
                        vec!["?"; np].join(", ")
                    ),
                    k.clone(),
                ),
            };
            let rows: Vec<Row> = oc
                .exec(
                    format!(
                        "SELECT {}, {}, _sprig_rk FROM {} {cond} ORDER BY {} LIMIT {chunk}",
                        vcols.join(", "),
                        pk_cols.join(", "),
                        fq(&o, &l.owner_table),
                        pk_cols.join(", ")
                    ),
                    Params::Positional(prm),
                )
                .await
                .map_err(db_err)?;
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
        }
    }
    if l.unique {
        verify(pools, catalog, p, l).await?;
    }
    cat.set_state(catalog, l.id, LookupState::Backfilling, LookupState::Public)
        .await?;
    l.state = LookupState::Public;
    Ok(report)
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
    use crate::key::{KeyClass, KeyValueOwned};
    let owned: Vec<KeyValueOwned> = l
        .columns
        .iter()
        .zip(vals)
        .map(|(c, v)| match (c.class, v) {
            (_, Value::Int(i)) => KeyValueOwned::Int(i128::from(*i)),
            (_, Value::UInt(u)) => KeyValueOwned::Int(i128::from(*u)),
            (KeyClass::Numeric, Value::Bytes(b)) => {
                KeyValueOwned::Decimal(String::from_utf8_lossy(b).into_owned())
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
        })
        .collect();
    let refs: Vec<_> = owned.iter().map(KeyValueOwned::as_ref).collect();
    let mut buf = Vec::new();
    StdNormalizer.normalize(spec, &refs, &mut buf)?;
    Ok(routing_hash(&buf))
}

#[allow(clippy::many_single_char_names)]
/// Step 4: every owner's value points at that owner; otherwise the value
/// belongs to another live owner, a real duplicate: drop and fail.
async fn verify(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &mut LookupIndex,
) -> Result<()> {
    let spec = l.spec()?;
    let vcols: Vec<String> = l.columns.iter().map(|c| quote(&c.name)).collect();
    let pk_cols: Vec<String> = l.owner_pk.iter().map(|c| quote(&c.name)).collect();
    let nv = vcols.len();
    for o in p.owner_shards() {
        let mut oc = conn(pools, &o.cluster).await?;
        let rows: Vec<Row> = oc
            .query(format!(
                "SELECT {}, {} FROM {}",
                vcols.join(", "),
                pk_cols.join(", "),
                fq(&o, &l.owner_table)
            ))
            .await
            .map_err(db_err)?;
        for r in rows {
            let vals: Vec<Value> = (0..nv).map(|i| r[i].clone()).collect();
            if vals.contains(&Value::NULL) {
                continue;
            }
            let pk: Vec<Value> = (nv..nv + pk_cols.len()).map(|i| r[i].clone()).collect();
            let t = p.lookup_target(hash_values(&spec, l, &vals)?)?;
            let mut prm = vals.clone();
            prm.extend(pk);
            let mut c = conn(pools, &t.cluster).await?;
            let n: Option<u64> = c
                .exec_first(
                    format!(
                        "SELECT COUNT(*) FROM {} WHERE {} AND {}",
                        fq(&t, &l.physical()),
                        l.value_where(),
                        l.owner_match()
                    ),
                    Params::Positional(prm),
                )
                .await
                .map_err(db_err)?;
            if n != Some(1) {
                let shown = vals
                    .iter()
                    .map(|v| v.as_sql(true))
                    .collect::<Vec<_>>()
                    .join("-");
                drop_lookup(pools, catalog, p, l).await?;
                return Err(SprigError::standard(
                    1062,
                    *b"23000",
                    format!("Duplicate entry {shown} for key '{}'", l.index_name),
                ));
            }
        }
    }
    Ok(())
}

/// Drops `l`: `dropping`, then the table on every lookup shard, then the
/// catalog row.
///
/// # Errors
/// MariaDB's errors.
pub async fn drop_lookup(
    pools: &Pools,
    catalog: &mut Conn,
    p: &dyn LookupPlacement,
    l: &mut LookupIndex,
) -> Result<()> {
    catalog
        .exec_drop(
            "UPDATE lookup_indexes SET state = 'dropping', updated_at = UTC_TIMESTAMP(6)
             WHERE lookup_id = ?",
            (l.id.0,),
        )
        .await
        .map_err(db_err)?;
    l.state = LookupState::Dropping;
    for t in p.lookup_shards() {
        let mut c = conn(pools, &t.cluster).await?;
        c.query_drop(format!("DROP TABLE IF EXISTS {}", fq(&t, &l.physical())))
            .await
            .map_err(db_err)?;
    }
    catalog
        .exec_drop("DELETE FROM lookup_indexes WHERE lookup_id = ?", (l.id.0,))
        .await
        .map_err(db_err)
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
