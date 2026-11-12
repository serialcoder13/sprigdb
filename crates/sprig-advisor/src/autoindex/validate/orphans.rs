//! Leftovers of validations that were cut off (bugfix-61; chapter 13,
//! "Validation harness").
//!
//! A replica or Galera RSU validation builds its index (and a field-column
//! candidate's columns) on one node with the binary log off. If the
//! controller stops in the middle (a restart, a lost leadership, a pass
//! over its time limit drops the validation's future), MariaDB finishes
//! the `ALTER` on its own, and nothing drops it: the node keeps an index
//! the writer doesn't have, every later validation of the candidate
//! aborts with `index_exists`, and the drift reconciler, which writes only
//! on the writer, never sees it.
//!
//! ```text
//! a validation in this process registers (node, schema, table) while it runs
//! leftovers(node, writer, t), unless a validation here runs on (node, t):
//!   writer: the sprig_auto_* indexes and _sprig_p_ columns of t, and its
//!           GTID position (replicas: the node waits for it, at most 10 s)
//!   node:   the same; leftovers = the node's minus the writer's, minus keep
//!           (Galera: the rows rolling out; field columns only on replicas)
//!   none → done
//!   KILL QUERY every client ALTER on t naming a leftover (the build MariaDB
//!     still runs for a client that is gone), wait until they end (30 s)
//!   on a session with sql_log_bin = 0 (Galera: each statement in RSU, then
//!     wait until the node is Synced again):
//!     DROP INDEX IF EXISTS each leftover index; DROP COLUMN IF EXISTS the
//!     leftover field columns
//! ```
//!
//! Who calls it: the replica and RSU validators when the index they are
//! about to build is already on their node but not on the writer (the
//! controller restarted during the last validation of the same row), and
//! the drift reconciler on every replica and every Galera node but the
//! writer, so a leftover goes even when its row left `validating` (a
//! covering retry's variant, a row rejected since). A replica never holds
//! a `sprig_auto_*` index its primary lacks for any other reason: builds
//! run on the primary and replicate.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sprig_cluster::sql::{NodeEndpoint, ProbeError, literal};
use sprig_common::ids::NodeId;
use sprig_common::names::parse_auto_index;

use super::IndexTarget;
use super::node::{NodeSql, col, drop_index_sql};
use super::replica::{drop_with_retries, quiet_session};

/// The prefix of key-value field columns (R6 round 6).
const FIELD_PREFIX: &str = "_sprig_p_";

type Key = (String, String, String);

fn running_map() -> &'static Mutex<HashMap<Key, usize>> {
    static M: OnceLock<Mutex<HashMap<Key, usize>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

fn key(node: &NodeId, t: &IndexTarget) -> Key {
    (
        node.to_string(),
        t.schema.to_ascii_lowercase(),
        t.table.to_ascii_lowercase(),
    )
}

/// A validation running in this process on one node's table; dropped when
/// it ends (or its future is dropped).
#[derive(Debug)]
pub struct Running(Key);

impl Drop for Running {
    fn drop(&mut self) {
        let mut m = running_map().lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = m.get_mut(&self.0) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.0);
            }
        }
    }
}

/// Registers a validation on `node`'s table `t` until the guard drops.
#[must_use]
pub fn running(node: &NodeId, t: &IndexTarget) -> Running {
    let k = key(node, t);
    *running_map()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(k.clone())
        .or_default() += 1;
    Running(k)
}

/// Whether a validation of this process runs on `node`'s table `t`.
#[must_use]
pub fn is_running(node: &NodeId, t: &IndexTarget) -> bool {
    running_map()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(&key(node, t))
}

/// What [`drop_leftovers`] found and dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Leftovers {
    /// Leftover indexes dropped.
    pub indexes: Vec<String>,
    /// Leftover field columns dropped.
    pub fields: Vec<String>,
    /// Builds still running that were killed (thread ids).
    pub killed: Vec<u64>,
}

impl Leftovers {
    /// Nothing was found.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty() && self.fields.is_empty()
    }
}

/// The node's kind, for how to drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    /// A replica: binary log off.
    Replica,
    /// A Galera node that isn't the writer: RSU and binary log off.
    Galera,
}

/// `sprig_auto_*` indexes and `_sprig_p_` columns of `t` on a node.
async fn automatic(
    n: &mut NodeSql,
    t: &IndexTarget,
) -> Result<(BTreeSet<String>, BTreeSet<String>), ProbeError> {
    let rows = n
        .rows(&format!(
            "SELECT 'I' AS k, INDEX_NAME AS n FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {t} \
             UNION SELECT 'C', COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {t}",
            s = literal(&t.schema),
            t = literal(&t.table)
        ))
        .await?;
    let mut idx = BTreeSet::new();
    let mut cols = BTreeSet::new();
    for r in &rows {
        let (Some(k), Some(name)) = (col(r, "k"), col(r, "n")) else {
            continue;
        };
        if k == "I" && parse_auto_index(name).is_some() {
            idx.insert(name.to_owned());
        } else if k == "C" && name.to_ascii_lowercase().starts_with(FIELD_PREFIX) {
            cols.insert(name.to_ascii_lowercase());
        }
    }
    Ok((idx, cols))
}

/// Client `ALTER`s on `t` naming one of `names` (the builds MariaDB still
/// runs for a validation whose client is gone), never the replication
/// threads or this session.
async fn builds(
    n: &mut NodeSql,
    t: &IndexTarget,
    names: &[String],
) -> Result<Vec<u64>, ProbeError> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let rows = n
        .rows(
            "SELECT ID, INFO FROM information_schema.PROCESSLIST WHERE COMMAND = 'Query' \
             AND INFO LIKE 'ALTER TABLE%' AND ID <> CONNECTION_ID()",
        )
        .await?;
    Ok(rows
        .iter()
        .filter(|r| {
            col(r, "INFO").is_some_and(|i| {
                i.contains(t.table.as_str()) && names.iter().any(|x| i.contains(x.as_str()))
            })
        })
        .filter_map(|r| col(r, "ID").and_then(|v| v.parse().ok()))
        .collect())
}

/// Waits (at most `limit`) until the node is `Synced` (Galera, after an
/// RSU statement).
async fn wait_synced(n: &mut NodeSql, limit: Duration) -> Result<(), ProbeError> {
    let until = Instant::now() + limit;
    loop {
        let m = n.status(&["wsrep_local_state_comment"]).await?;
        if m.get("wsrep_local_state_comment").map(String::as_str) == Some("Synced") {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(ProbeError {
                errno: None,
                message: format!(
                    "not Synced within {} s after dropping a leftover",
                    limit.as_secs()
                ),
                timeout: true,
            });
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Drops the leftovers of cut-off validations on `node`'s tables
/// `tables`: the `sprig_auto_*` indexes the writer lacks (except `keep`)
/// and, on a replica, the `_sprig_p_` field columns the writer lacks
/// (module docs), with one connection to each node. Tables a validation
/// of this process runs on are skipped. The answer has one entry per
/// table, in order.
///
/// # Errors
/// Either node unreachable, or a statement failed; what was dropped before
/// the failure stays dropped.
pub async fn drop_leftovers(
    node: &NodeEndpoint,
    writer: &NodeEndpoint,
    tables: &[IndexTarget],
    kind: NodeKind,
    keep: &BTreeSet<String>,
) -> Result<Vec<Leftovers>, ProbeError> {
    let todo: Vec<IndexTarget> = tables
        .iter()
        .filter(|t| !is_running(&node.node, t))
        .cloned()
        .collect();
    let mut done = sweep(node, writer, &todo, kind, keep, None)
        .await?
        .into_iter();
    Ok(tables
        .iter()
        .map(|t| {
            if is_running(&node.node, t) && !todo.contains(t) {
                Leftovers::default()
            } else {
                done.next().unwrap_or_default()
            }
        })
        .collect())
}

/// [`drop_leftovers`] of index `name` only (and, on a replica, the field
/// columns the writer lacks), for the validation about to build `name` on
/// `node`'s table `t`: it has registered itself, so the running check
/// would stop it, and on Galera another row's rolling build may hold other
/// names on the node.
pub(crate) async fn sweep_from_validator(
    node: &NodeEndpoint,
    writer: &NodeEndpoint,
    t: &IndexTarget,
    kind: NodeKind,
    name: &str,
) -> Result<Leftovers, ProbeError> {
    let mut v = sweep(
        node,
        writer,
        std::slice::from_ref(t),
        kind,
        &BTreeSet::new(),
        Some(name),
    )
    .await?;
    Ok(v.pop().unwrap_or_default())
}

async fn sweep(
    node: &NodeEndpoint,
    writer: &NodeEndpoint,
    tables: &[IndexTarget],
    kind: NodeKind,
    keep: &BTreeSet<String>,
    only: Option<&str>,
) -> Result<Vec<Leftovers>, ProbeError> {
    let mut all: Vec<Leftovers> = tables.iter().map(|_| Leftovers::default()).collect();
    if node.node == writer.node || tables.is_empty() {
        return Ok(all);
    }
    let timeout = Duration::from_secs(10);
    let mut w = NodeSql::connect(writer, timeout).await?;
    let read = async {
        let mut have = Vec::with_capacity(tables.len());
        for t in tables {
            have.push(automatic(&mut w, t).await?);
        }
        let pos = if kind == NodeKind::Replica {
            w.scalar("SELECT @@gtid_binlog_pos").await?
        } else {
            None
        };
        Ok::<_, ProbeError>((have, pos))
    }
    .await;
    w.s.close().await;
    let (have, pos) = read?;
    let mut n = NodeSql::connect(node, timeout).await?;
    let res = async {
        // What the writer had is applied here first, so an index the
        // writer just built (or dropped) isn't taken for a leftover.
        if let Some(p) = pos.filter(|p| !p.is_empty()) {
            let _ = n
                .scalar(&format!("SELECT MASTER_GTID_WAIT({}, 10)", literal(&p)))
                .await?;
        }
        for ((t, (w_idx, w_cols)), out) in tables.iter().zip(&have).zip(all.iter_mut()) {
            sweep_one(&mut n, node, t, (w_idx, w_cols), kind, keep, only, out).await?;
        }
        Ok::<_, ProbeError>(())
    }
    .await;
    n.s.close().await;
    res?;
    Ok(all)
}

/// One table of [`sweep`] on the node's session `n`.
#[allow(clippy::too_many_arguments)]
async fn sweep_one(
    n: &mut NodeSql,
    node: &NodeEndpoint,
    t: &IndexTarget,
    (w_idx, w_cols): (&BTreeSet<String>, &BTreeSet<String>),
    kind: NodeKind,
    keep: &BTreeSet<String>,
    only: Option<&str>,
    out: &mut Leftovers,
) -> Result<(), ProbeError> {
    let (n_idx, n_cols) = automatic(n, t).await?;
    let indexes: Vec<String> = n_idx
        .difference(w_idx)
        .filter(|x| !keep.contains(*x) && only.is_none_or(|o| o == x.as_str()))
        .cloned()
        .collect();
    let fields: Vec<String> = if kind == NodeKind::Replica {
        n_cols.difference(w_cols).cloned().collect()
    } else {
        Vec::new()
    };
    if indexes.is_empty() && fields.is_empty() {
        return Ok(());
    }
    tracing::warn!(node = %node.node, schema = %t.schema, table = %t.table,
        ?indexes, ?fields, "leftovers of a cut-off validation found; dropping them");
    // The build MariaDB may still be running for a client that's gone.
    let mut names = indexes.clone();
    names.extend(fields.iter().cloned());
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let ids = builds(n, t, &names).await?;
        if ids.is_empty() {
            break;
        }
        for id in &ids {
            if !out.killed.contains(id) {
                let _ = n.exec(&format!("KILL QUERY {id}")).await;
                out.killed.push(*id);
            }
        }
        if Instant::now() >= until {
            return Err(ProbeError {
                errno: None,
                message: format!("leftover builds still running on {}: {ids:?}", node.node),
                timeout: true,
            });
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let mut ddl = quiet_session(node, Duration::from_secs(120), Duration::from_secs(5)).await?;
    let dropped = async {
        for x in &indexes {
            if kind == NodeKind::Galera {
                ddl.exec("SET SESSION wsrep_OSU_method = 'RSU'").await?;
            }
            let r = drop_with_retries(&mut ddl, &drop_index_sql(t, x)).await;
            if kind == NodeKind::Galera {
                let _ = ddl.exec("SET SESSION wsrep_OSU_method = 'TOI'").await;
                wait_synced(n, Duration::from_secs(60)).await?;
            }
            r?;
            out.indexes.push(x.clone());
        }
        if let Some(sql) = crate::autoindex::kv::drop_fields_sql(&t.schema, &t.table, &fields) {
            drop_with_retries(&mut ddl, &sql).await?;
            out.fields.clone_from(&fields);
        }
        Ok::<_, ProbeError>(())
    }
    .await;
    ddl.s.close().await;
    dropped?;
    metrics::counter!("sprig_advisor_autoindex_validation_leftovers_total")
        .increment((out.indexes.len() + out.fields.len()) as u64);
    tracing::info!(node = %node.node, schema = %t.schema, table = %t.table,
        indexes = ?out.indexes, fields = ?out.fields, killed = ?out.killed,
        "leftovers of a cut-off validation dropped (binary log off)");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn running_validations_are_counted_per_node_and_table() {
        let n1 = NodeId::new("orph-n1").unwrap();
        let n2 = NodeId::new("orph-n2").unwrap();
        let t = IndexTarget {
            schema: "t1_d1_g1_s1".into(),
            table: "Orders".into(),
            pk: vec!["id".into()],
        };
        let lower = IndexTarget {
            table: "orders".into(),
            ..t.clone()
        };
        assert!(!is_running(&n1, &t));
        let a = running(&n1, &t);
        let b = running(&n1, &lower);
        assert!(is_running(&n1, &t) && is_running(&n1, &lower));
        assert!(!is_running(&n2, &t));
        drop(a);
        assert!(is_running(&n1, &t), "the second validation still runs");
        drop(b);
        assert!(!is_running(&n1, &t));
    }
}
