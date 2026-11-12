//! Asynchronous mirrors (chapter 43, "Mirrors and freshness"; build step
//! 14; R8 round 5).
//!
//! Under `graph.mirror = async` a grouped graph's mirror rows
//! (`__sprig_gm_<graph>`, on each edge's destination shard) are kept by the
//! worker's applier rather than by the writer's transaction, so an edge to
//! a node on another cluster is a one-cluster write.
//!
//! - **Applying** ([`apply_batch`]). The applier reads every cluster's
//!   binlog for the tenant and hands the edge-table row changes of a
//!   committed group here. Changes are coalesced per edge (the last one
//!   wins; an edge's ends and type never change). A deleted edge's mirror
//!   is deleted. An inserted edge is checked on its source's shard with a
//!   shared lock (`LOCK IN SHARE MODE`) held until its mirror is written:
//!   present, the mirror is upserted; gone, the mirror is deleted. So the
//!   mirrors converge on the edges whatever order the clusters' streams
//!   are applied in (an edge whose shard moved mid-life has its insert in
//!   one cluster's binlog and its delete in another's), and a delete that
//!   races the upsert waits for the lock and is applied after it. Mirror
//!   writes commit before the locks are released. The caller re-runs a
//!   batch whose shards changed epoch meanwhile (a split, merge or move
//!   cut over), with the new map; every step is idempotent.
//! - **Positions** (`graph_mirror_pos` in the tenant catalog): one row per
//!   shard of the graph's map, naming the shard's cluster and epoch, the
//!   binlog position of that cluster up to which the applier has applied
//!   every change, and when it last wrote the row.
//! - **Freshness** ([`fresh`]). A read may use the mirrors when every shard
//!   of its map has a row with the same cluster and epoch, written within
//!   [`LIVE_MS`], whose position reaches the session's causal target on
//!   that cluster (its own writes and the bookmarks it was given). The
//!   gateway's [`MirrorGate`] waits up to `graph.mirror_max_wait_ms` for
//!   that; otherwise incoming hops scatter the `rev` key, which is exact.
//!   Write statements never read asynchronous mirrors.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

use async_trait::async_trait;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

use crate::catalog::backend_error;
use crate::maria::GraphShards;

/// The tenant's feed subscription (name and kind) the controller keeps a
/// `graph_mirror` applier job for; registered by the gateway when a graph
/// of the tenant wants `async` mirrors.
pub const SUB_NAME: &str = "graph_mirror";

/// The applier's job kind.
pub const JOB_KIND: &str = "graph_mirror";

/// The positions table in `t<tenant>_sprig`.
pub const POS_TABLE: &str = "graph_mirror_pos";

/// A row older than this (milliseconds, by the catalog server's clock)
/// means the applier isn't running: readers scatter at once.
pub const LIVE_MS: u64 = 10_000;

/// `CREATE TABLE` of the positions table (catalog migration 3).
pub const DDL_POS: &str = "CREATE TABLE IF NOT EXISTS graph_mirror_pos (
  graph_id    BIGINT UNSIGNED NOT NULL,
  shard_id    INT UNSIGNED NOT NULL,
  cluster     VARCHAR(64) NOT NULL,
  epoch       BIGINT UNSIGNED NOT NULL,
  gtid_pos    VARCHAR(4096) NOT NULL,
  applied_at  DATETIME(6) NOT NULL,
  PRIMARY KEY (graph_id, shard_id)
) ENGINE=InnoDB";

/// Whether a read statement may use asynchronous mirrors now.
#[async_trait]
pub trait MirrorGate: Send + Sync {
    /// `true` once the applier has applied what the reader must see,
    /// waiting up to the gate's bound; `false` to scatter instead.
    async fn fresh(&self, layout: &GraphShards) -> bool;
}

/// One shard's applied position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PosRow {
    /// The shard.
    pub shard: u32,
    /// Its cluster when the row was written.
    pub cluster: String,
    /// Its epoch then.
    pub epoch: u64,
    /// The cluster's binlog position applied through (`d-s-n,...`).
    pub pos: String,
    /// Milliseconds since the row was written (catalog server's clock).
    pub age_ms: u64,
}

/// MariaDB GTID positions as domain → sequence number.
fn domains(pos: &str) -> Option<BTreeMap<u32, u64>> {
    let mut m = BTreeMap::new();
    for g in pos.split(',').map(str::trim).filter(|g| !g.is_empty()) {
        let mut it = g.split('-');
        let (Some(d), Some(_server), Some(n), None) = (it.next(), it.next(), it.next(), it.next())
        else {
            return None;
        };
        let (d, n) = (d.parse::<u32>().ok()?, n.parse::<u64>().ok()?);
        let e = m.entry(d).or_insert(n);
        *e = (*e).max(n);
    }
    Some(m)
}

/// Whether binlog position `pos` includes every transaction of `target`
/// (each of `target`'s domains at least as far); unparsable text never
/// does.
#[must_use]
pub fn reached(pos: &str, target: &str) -> bool {
    let (Some(p), Some(t)) = (domains(pos), domains(target)) else {
        return false;
    };
    t.iter()
        .all(|(d, n)| p.get(d).is_some_and(|have| have >= n))
}

/// Whether `rows` let a reader with causal `targets` (cluster → GTID
/// position) use `layout`'s mirrors: a live row for every shard, on the
/// shard's cluster and epoch, reaching the target of that cluster.
#[must_use]
pub fn fresh(rows: &[PosRow], layout: &GraphShards, targets: &BTreeMap<String, String>) -> bool {
    layout.shards.iter().all(|s| {
        rows.iter().any(|r| {
            r.shard == s.shard
                && r.cluster == s.cluster
                && r.epoch == s.epoch
                && r.age_ms <= LIVE_MS
                && targets.get(&s.cluster).is_none_or(|t| reached(&r.pos, t))
        })
    })
}

/// The positions of `graph` in catalog schema `catalog`.
///
/// # Errors
/// A backend error (1146 before the catalog's migration 3).
pub async fn read_pos<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    graph: u64,
) -> Result<Vec<PosRow>> {
    let rows: Vec<(u32, String, u64, String, i64)> = q
        .query(format!(
            "SELECT shard_id, cluster, epoch, gtid_pos, \
             TIMESTAMPDIFF(MICROSECOND, applied_at, UTC_TIMESTAMP(6)) DIV 1000 \
             FROM {}.{POS_TABLE} WHERE graph_id = {graph}",
            quote(catalog)
        ))
        .await
        .map_err(backend_error)?;
    Ok(pos_rows(rows))
}

fn pos_rows(rows: Vec<(u32, String, u64, String, i64)>) -> Vec<PosRow> {
    rows.into_iter()
        .map(|(shard, cluster, epoch, pos, age)| PosRow {
            shard,
            cluster,
            epoch,
            pos,
            age_ms: u64::try_from(age).unwrap_or(0),
        })
        .collect()
}

/// The positions of `graph` written after its `graphs` row last changed
/// (its mirror mode): what an applier that serves the graph under that
/// mode wrote.
///
/// # Errors
/// A backend error.
pub async fn read_pos_since_change<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    graph: u64,
) -> Result<Vec<PosRow>> {
    let c = quote(catalog);
    let rows: Vec<(u32, String, u64, String, i64)> = q
        .query(format!(
            "SELECT p.shard_id, p.cluster, p.epoch, p.gtid_pos, \
             TIMESTAMPDIFF(MICROSECOND, p.applied_at, UTC_TIMESTAMP(6)) DIV 1000 \
             FROM {c}.{POS_TABLE} p JOIN {c}.graphs g ON g.graph_id = p.graph_id \
             WHERE p.graph_id = {graph} AND p.applied_at > g.updated_at"
        ))
        .await
        .map_err(backend_error)?;
    Ok(pos_rows(rows))
}

/// Records positions of `graph`: `(shard, cluster, epoch, pos)`, and
/// deletes the rows of shards no longer in the map.
///
/// # Errors
/// A backend error.
pub async fn write_pos<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    graph: u64,
    rows: &[(u32, String, u64, String)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let t = format!("{}.{POS_TABLE}", quote(catalog));
    let mut sql = format!(
        "INSERT INTO {t} (graph_id, shard_id, cluster, epoch, gtid_pos, applied_at) VALUES "
    );
    let mut params: Vec<mysql_async::Value> = Vec::new();
    for (k, (shard, cluster, epoch, pos)) in rows.iter().enumerate() {
        if k > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "({graph}, {shard}, ?, {epoch}, ?, UTC_TIMESTAMP(6))");
        params.push(cluster.as_str().into());
        params.push(pos.as_str().into());
    }
    sql.push_str(
        " ON DUPLICATE KEY UPDATE cluster = VALUES(cluster), epoch = VALUES(epoch), \
         gtid_pos = VALUES(gtid_pos), applied_at = VALUES(applied_at)",
    );
    q.exec_drop(sql, params).await.map_err(backend_error)?;
    let keep: Vec<String> = rows.iter().map(|r| r.0.to_string()).collect();
    q.query_drop(format!(
        "DELETE FROM {t} WHERE graph_id = {graph} AND shard_id NOT IN ({})",
        keep.join(", ")
    ))
    .await
    .map_err(backend_error)?;
    Ok(())
}

/// Deletes every position row of `graph` (it was dropped, or its mirrors
/// went back to `sync`).
///
/// # Errors
/// A backend error.
pub async fn clear_pos<Q: Queryable + Send>(q: &mut Q, catalog: &str, graph: u64) -> Result<()> {
    q.query_drop(format!(
        "DELETE FROM {}.{POS_TABLE} WHERE graph_id = {graph}",
        quote(catalog)
    ))
    .await
    .map_err(backend_error)
}

/// One edge row change seen in a shard's binlog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeChange {
    /// The edge id.
    pub id: u64,
    /// Its source.
    pub src: u64,
    /// Its destination.
    pub dst: u64,
    /// Its type id.
    pub rtype: u32,
    /// The source's routing hash.
    pub src_rk: u64,
    /// The destination's routing hash.
    pub dst_rk: u64,
    /// Inserted (`true`) or deleted.
    pub live: bool,
}

/// The last change of each edge, in first-seen order.
#[must_use]
pub fn coalesce(changes: &[EdgeChange]) -> Vec<EdgeChange> {
    let mut at: HashMap<u64, usize> = HashMap::new();
    let mut out: Vec<EdgeChange> = Vec::new();
    for c in changes {
        if let Some(&i) = at.get(&c.id) {
            out[i] = *c;
        } else {
            at.insert(c.id, out.len());
            out.push(*c);
        }
    }
    out
}

/// What a connection of a batch is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Shared locks on edges, held until the mirrors are written.
    Check,
    /// Mirror writes.
    Write,
}

/// The applier's connections: a fresh `sprig_ctl` connection to a
/// cluster's writer, outside any other transaction.
#[async_trait]
pub trait ApplyConns: Send {
    /// A connection to `cluster`.
    ///
    /// # Errors
    /// When none can be had.
    async fn connect(&mut self, cluster: &str) -> Result<Conn>;
}

/// What one batch did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// Mirrors written (upserted).
    pub upserts: u64,
    /// Mirrors deleted (edges gone).
    pub deletes: u64,
    /// Shards read or written: `(shard, epoch, cluster)`.
    pub touched: Vec<(u32, u64, String)>,
}

fn ids(v: &[u64]) -> String {
    let s: Vec<String> = v.iter().map(u64::to_string).collect();
    s.join(", ")
}

/// Applies `changes` (one graph's, in binlog order) to its mirrors under
/// `layout` (see the module docs). Every write is idempotent, so a batch
/// may be applied again, with a newer map, after a failure or an epoch
/// change of a shard in [`Applied::touched`].
///
/// # Errors
/// A backend error (all of the batch's transactions are rolled back):
/// 1146 or 1049 when a shard moved, 1205 or 1213 when a lock wait failed.
pub async fn apply_batch(
    conns: &mut dyn ApplyConns,
    layout: &GraphShards,
    changes: &[EdgeChange],
    now_us: u64,
) -> Result<Applied> {
    let changes = coalesce(changes);
    let mut open: HashMap<(String, Role), Conn> = HashMap::new();
    let r = apply_in(conns, layout, &changes, now_us, &mut open).await;
    // Mirror writes commit before the locks on their edges go.
    let mut order: Vec<(String, Role)> = open.keys().cloned().collect();
    order.sort_by_key(|(c, r)| (*r == Role::Check, c.clone()));
    let mut out = r;
    for k in order {
        let Some(mut c) = open.remove(&k) else {
            continue;
        };
        let end = if out.is_ok() { "COMMIT" } else { "ROLLBACK" };
        if let Err(e) = c.query_drop(end).await {
            if out.is_ok() {
                out = Err(backend_error(e));
            }
            let _ = c.disconnect().await;
        }
    }
    out
}

async fn begin(
    conns: &mut dyn ApplyConns,
    open: &mut HashMap<(String, Role), Conn>,
    cluster: &str,
    role: Role,
) -> Result<()> {
    let k = (cluster.to_owned(), role);
    if open.contains_key(&k) {
        return Ok(());
    }
    let mut c = conns.connect(cluster).await?;
    c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .map_err(backend_error)?;
    c.query_drop("SET SESSION innodb_lock_wait_timeout = 5")
        .await
        .map_err(backend_error)?;
    c.query_drop("START TRANSACTION")
        .await
        .map_err(backend_error)?;
    open.insert(k, c);
    Ok(())
}

fn conn<'a>(
    open: &'a mut HashMap<(String, Role), Conn>,
    cluster: &str,
    role: Role,
) -> Result<&'a mut Conn> {
    open.get_mut(&(cluster.to_owned(), role))
        .ok_or_else(|| SprigError::internal("no applier connection"))
}

async fn apply_in(
    conns: &mut dyn ApplyConns,
    layout: &GraphShards,
    changes: &[EdgeChange],
    now_us: u64,
    open: &mut HashMap<(String, Role), Conn>,
) -> Result<Applied> {
    let mut out = Applied::default();
    let touch = |i: usize, out: &mut Applied| {
        let s = &layout.shards[i];
        let t = (s.shard, s.epoch, s.cluster.clone());
        if !out.touched.contains(&t) {
            out.touched.push(t);
        }
    };
    // Inserted edges: still there? Locked until their mirrors commit.
    let mut by_src: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for c in changes.iter().filter(|c| c.live) {
        by_src
            .entry(layout.index_of(c.src_rk))
            .or_default()
            .push(c.id);
    }
    let mut present: HashSet<u64> = HashSet::new();
    for (i, edge_ids) in by_src {
        touch(i, &mut out);
        let s = &layout.shards[i];
        begin(conns, open, &s.cluster, Role::Check).await?;
        let c = conn(open, &s.cluster, Role::Check)?;
        for batch in edge_ids.chunks(1000) {
            let found: Vec<u64> = c
                .query(format!(
                    "SELECT id FROM {} WHERE id IN ({}) LOCK IN SHARE MODE",
                    s.tables.edges,
                    ids(batch)
                ))
                .await
                .map_err(backend_error)?;
            present.extend(found);
        }
    }
    // Mirrors: upserts for present edges, deletes for the rest.
    let mut put: BTreeMap<usize, Vec<&EdgeChange>> = BTreeMap::new();
    let mut gone: BTreeMap<usize, Vec<&EdgeChange>> = BTreeMap::new();
    for c in changes {
        let j = layout.index_of(c.dst_rk);
        if c.live && present.contains(&c.id) {
            put.entry(j).or_default().push(c);
        } else {
            gone.entry(j).or_default().push(c);
        }
    }
    for (j, rows) in put {
        touch(j, &mut out);
        let s = &layout.shards[j];
        begin(conns, open, &s.cluster, Role::Write).await?;
        let c = conn(open, &s.cluster, Role::Write)?;
        for batch in rows.chunks(500) {
            let mut sql = format!(
                "INSERT INTO {} (dst, type, src, id, src_rk, _v, _sprig_rk) VALUES ",
                s.tables.mirrors
            );
            for (k, e) in batch.iter().enumerate() {
                if k > 0 {
                    sql.push_str(", ");
                }
                let _ = write!(
                    sql,
                    "({}, {}, {}, {}, {}, {now_us}, {})",
                    e.dst, e.rtype, e.src, e.id, e.src_rk, e.dst_rk
                );
            }
            sql.push_str(" ON DUPLICATE KEY UPDATE src_rk = VALUES(src_rk)");
            c.query_drop(sql).await.map_err(backend_error)?;
            out.upserts += batch.len() as u64;
        }
    }
    for (j, rows) in gone {
        touch(j, &mut out);
        let s = &layout.shards[j];
        begin(conns, open, &s.cluster, Role::Write).await?;
        let c = conn(open, &s.cluster, Role::Write)?;
        for batch in rows.chunks(1000) {
            let dsts: Vec<u64> = batch.iter().map(|e| e.dst).collect();
            let eids: Vec<u64> = batch.iter().map(|e| e.id).collect();
            c.query_drop(format!(
                "DELETE FROM {} WHERE dst IN ({}) AND id IN ({})",
                s.tables.mirrors,
                ids(&dsts),
                ids(&eids)
            ))
            .await
            .map_err(backend_error)?;
            out.deletes += c.affected_rows();
        }
    }
    Ok(out)
}

/// Whether the shards `touched` still have the cluster and epoch they
/// were applied at in `now`.
#[must_use]
pub fn still_current(touched: &[(u32, u64, String)], now: &GraphShards) -> bool {
    touched.iter().all(|(shard, epoch, cluster)| {
        now.shards
            .iter()
            .any(|s| s.shard == *shard && s.epoch == *epoch && &s.cluster == cluster)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Tables;
    use crate::maria::ShardLoc;

    fn layout(shards: &[(u32, u64, &str, u64)]) -> GraphShards {
        GraphShards {
            graph: 9,
            db: 1,
            group: 3,
            version: 1,
            shards: shards
                .iter()
                .map(|&(shard, lo, cluster, epoch)| ShardLoc {
                    shard,
                    lo,
                    epoch,
                    cluster: cluster.to_owned(),
                    schema: format!("s{shard}"),
                    tables: Tables::of(&format!("s{shard}"), 9),
                })
                .collect(),
            seq: String::new(),
            mirrors: true,
            mirror_async: true,
            mirror_building: false,
            route: None,
        }
    }

    #[test]
    fn positions_compare_by_domain() {
        assert!(reached("0-1-10,1-2-5", "0-1-10"));
        assert!(reached("0-1-11,1-2-5", "0-3-10,1-2-5"));
        assert!(!reached("0-1-9,1-2-5", "0-1-10"));
        assert!(!reached("0-1-10", "1-2-1"));
        assert!(reached("0-1-10", ""));
        assert!(!reached("junk", "0-1-1"));
    }

    #[test]
    fn freshness_needs_every_shard_live_current_and_far_enough() {
        let l = layout(&[(1, 0, "c1", 4), (2, 1 << 62, "c2", 7)]);
        let row = |shard, cluster: &str, epoch, pos: &str, age_ms| PosRow {
            shard,
            cluster: cluster.into(),
            epoch,
            pos: pos.into(),
            age_ms,
        };
        let ok = vec![
            row(1, "c1", 4, "0-1-50", 100),
            row(2, "c2", 7, "0-2-9", 100),
        ];
        let mut t = BTreeMap::new();
        assert!(fresh(&ok, &l, &t));
        t.insert("c1".to_owned(), "0-1-50".to_owned());
        assert!(fresh(&ok, &l, &t));
        t.insert("c2".to_owned(), "0-2-10".to_owned());
        assert!(!fresh(&ok, &l, &t), "c2 not reached");
        t.remove("c2");
        assert!(!fresh(&ok[..1], &l, &t), "shard 2 has no row");
        let moved = vec![
            row(1, "c1", 4, "0-1-50", 100),
            row(2, "c1", 7, "0-1-50", 100),
        ];
        assert!(!fresh(&moved, &l, &t), "row of the old cluster");
        let split = vec![
            row(1, "c1", 3, "0-1-50", 100),
            row(2, "c2", 7, "0-2-9", 100),
        ];
        assert!(!fresh(&split, &l, &t), "row of the old epoch");
        let stale = vec![
            row(1, "c1", 4, "0-1-50", LIVE_MS + 1),
            row(2, "c2", 7, "0-2-9", 1),
        ];
        assert!(!fresh(&stale, &l, &t), "applier stopped");
    }

    #[test]
    fn the_last_change_of_an_edge_wins() {
        let e = |id, live| EdgeChange {
            id,
            src: 1,
            dst: 2,
            rtype: 3,
            src_rk: 4,
            dst_rk: 5,
            live,
        };
        let c = coalesce(&[e(1, true), e(2, true), e(1, false), e(3, false)]);
        assert_eq!(c, vec![e(1, false), e(2, true), e(3, false)]);
    }

    #[test]
    fn epochs_must_hold() {
        let l = layout(&[(1, 0, "c1", 4), (2, 1 << 62, "c2", 7)]);
        assert!(still_current(&[(1, 4, "c1".into())], &l));
        assert!(!still_current(&[(2, 6, "c2".into())], &l));
        assert!(!still_current(&[(3, 1, "c1".into())], &l));
    }
}
