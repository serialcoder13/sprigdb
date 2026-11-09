//! Foreign keys across groups (chapter 19, "Foreign keys across groups";
//! R3): the gateway keeps the keys MariaDB can't, because the two ends sit
//! on different shards.
//!
//! ```text
//! child write (INSERT, UPDATE of the key's columns), before the rows:
//!   per parent shard: (SELECT i FROM parent WHERE cols = tuple_i LIMIT 1
//!                      LOCK IN SHARE MODE) UNION ALL …        a missing tuple → 1452
//! parent write (DELETE, UPDATE of the referenced columns), before the rows:
//!   1. SELECT referenced columns [, new values] FROM parent WHERE … FOR UPDATE
//!   2. RESTRICT / NO ACTION: per child shard, the same probe   a hit → 1451
//!   3. CASCADE / SET NULL: DELETE or UPDATE of the children, planned by the
//!      router and run in the same statement, at most 15 levels deep
//! ```
//!
//! Everything runs in the write's own transaction (the implicit one, or
//! the statement savepoint), so the share locks last until it commits: a
//! parent delete waits for a child insert that passed its check, then sees
//! the child. `foreign_key_checks = 0` skips the checks and the actions, as
//! MariaDB does.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};
pub use sprig_shard::fk::FkAction;

use crate::exec::{Budget, Output, ShardBackend, ShardOutcome};
use crate::plan::{Plan, ShardAt, Target};
use crate::planner::{IdMode, PlanCtx};
use crate::topology::{KeyClass, KeyValue, TableEntry, TableKind, Topology};

/// MariaDB's limit on cascading actions (15 levels).
pub const MAX_DEPTH: usize = 15;

/// How a key value is read back from MariaDB and written into SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValClass {
    /// Integers and decimals: the text as is.
    Num,
    /// Character strings: read as hex of utf8mb4, written as `_utf8mb4
    /// X'…'` (coercible, so the column's collation compares).
    Str,
    /// Binary strings: hex both ways.
    Bin,
    /// Anything else (dates, times): a quoted string.
    Text,
}

impl ValClass {
    /// The select-list expression reading `expr` (a column, or a new
    /// value) so [`Self::value`] can write it back.
    #[must_use]
    pub fn read_expr(self, expr: &str) -> String {
        match self {
            Self::Num | Self::Text => expr.to_owned(),
            Self::Str => format!("HEX(CONVERT({expr} USING utf8mb4))"),
            Self::Bin => format!("HEX({expr})"),
        }
    }

    /// A value read with [`Self::read_expr`]; `None` for `NULL`.
    #[must_use]
    pub fn value(self, v: Option<&[u8]>) -> Option<FkVal> {
        let v = v?;
        let text = std::str::from_utf8(v).ok()?;
        Some(match self {
            Self::Num => FkVal {
                sql: text.to_owned(),
                key: text.parse::<i128>().ok().map(KeyValue::Int),
            },
            Self::Str => FkVal {
                sql: format!("_utf8mb4 X'{text}'"),
                key: unhex(text).map(KeyValue::Bytes),
            },
            Self::Bin => FkVal {
                sql: format!("X'{text}'"),
                key: unhex(text).map(KeyValue::Bytes),
            },
            Self::Text => FkVal {
                sql: format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''")),
                key: None,
            },
        })
    }
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// One value of a foreign-key tuple: its SQL literal and, when known, the
/// key value it routes by.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FkVal {
    /// The literal.
    pub sql: String,
    /// The value as a routing key.
    pub key: Option<KeyValue>,
}

/// A foreign key the gateway keeps, between two tables of one logical
/// database ([`FkGraph::tables`] indexes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkRef {
    /// The constraint's name.
    pub name: String,
    /// The child table.
    pub child: usize,
    /// The child's columns, lowercase.
    pub child_columns: Vec<String>,
    /// Their positions among the child's visible columns.
    pub child_ordinals: Vec<Option<usize>>,
    /// Per child column: an `INSERT` that leaves it out writes `NULL`.
    pub child_null_default: Vec<bool>,
    /// The parent table.
    pub parent: usize,
    /// The referenced columns, lowercase.
    pub parent_columns: Vec<String>,
    /// How each column's values read and write.
    pub classes: Vec<ValClass>,
    /// `ON DELETE`.
    pub on_delete: FkAction,
    /// `ON UPDATE`.
    pub on_update: FkAction,
}

impl FkRef {
    /// The constraint as MariaDB's messages show it.
    #[must_use]
    pub fn describe(&self, g: &FkGraph) -> String {
        let q = |v: &[String]| {
            v.iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut s = format!(
            "`{}`.`{}`, CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES `{}` ({})",
            g.default_schema,
            g.tables[self.child].name,
            self.name,
            q(&self.child_columns),
            g.tables[self.parent].name,
            q(&self.parent_columns)
        );
        if self.on_delete != FkAction::Restrict {
            let _ = write!(s, " ON DELETE {}", self.on_delete.sql());
        }
        if self.on_update != FkAction::Restrict {
            let _ = write!(s, " ON UPDATE {}", self.on_update.sql());
        }
        s
    }
}

/// The foreign keys of one logical database the gateway keeps, and the
/// tables at their ends (copies of the topology's entries, so cascades can
/// be planned from it alone).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkGraph {
    /// The database's default schema (chapter 07's rewrite names it).
    pub default_schema: String,
    /// The tables at either end of a key.
    pub tables: Vec<TableEntry>,
    /// The keys.
    pub fks: Vec<FkRef>,
}

impl FkGraph {
    /// Table `name`'s index.
    #[must_use]
    pub fn index(&self, name: &str) -> Option<usize> {
        self.tables
            .iter()
            .position(|t| t.name.eq_ignore_ascii_case(name))
    }

    /// The keys whose child is table `t`.
    pub fn as_child(&self, t: usize) -> impl Iterator<Item = (usize, &FkRef)> {
        self.fks
            .iter()
            .enumerate()
            .filter(move |(_, f)| f.child == t)
    }

    /// The keys whose parent is table `t`.
    pub fn as_parent(&self, t: usize) -> impl Iterator<Item = (usize, &FkRef)> {
        self.fks
            .iter()
            .enumerate()
            .filter(move |(_, f)| f.parent == t)
    }
}

/// A topology over a graph's tables, for planning cascaded statements.
#[derive(Debug, Clone)]
pub struct GraphTopo(pub Arc<FkGraph>);

impl Topology for GraphTopo {
    fn table(&self, schema: &str, table: &str) -> Option<&TableEntry> {
        if !schema.eq_ignore_ascii_case(&self.0.default_schema) {
            return None;
        }
        self.0.index(table).map(|i| &self.0.tables[i])
    }

    fn mentions(&self, _sql: &str) -> bool {
        true
    }

    fn fk_graph(&self, schema: &str) -> Option<Arc<FkGraph>> {
        schema
            .eq_ignore_ascii_case(&self.0.default_schema)
            .then(|| Arc::clone(&self.0))
    }
}

/// Values of a child write whose parents must exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkCheck {
    /// The key ([`FkGraph::fks`] index).
    pub fk: usize,
    /// The tuples, none with a `NULL` part.
    pub tuples: Vec<Vec<FkVal>>,
    /// For a reference parent: the cluster whose copy is read (the child
    /// rows' cluster).
    pub near: Option<String>,
}

/// What a parent write does to its rows' referenced values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentKind {
    /// `DELETE`.
    Delete,
    /// `UPDATE`: per assigned referenced column, its position among the
    /// read's columns of its new value.
    Update(Vec<(String, usize)>),
}

/// The parent side of a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkParent {
    /// The parent table.
    pub table: usize,
    /// `SELECT <referenced columns> [, <new values>] FROM … FOR UPDATE`,
    /// planned: the rows the write changes, locked.
    pub read: Box<Plan>,
    /// The read's referenced columns, in order (new values follow).
    pub cols: Vec<String>,
    /// Per read column, how its value reads.
    pub classes: Vec<ValClass>,
    /// Delete or update.
    pub kind: ParentKind,
    /// The keys whose referenced values change.
    pub fks: Vec<usize>,
}

/// A write's foreign-key work, run before its rows (in its transaction).
#[derive(Debug, Clone)]
pub struct FkWork {
    /// The database's keys.
    pub graph: Arc<FkGraph>,
    /// Child-side checks.
    pub checks: Vec<FkCheck>,
    /// The parent side.
    pub parent: Option<FkParent>,
    /// Where cascaded statements' unsharded parts would run.
    pub session_cluster: String,
    /// `router.max_route_values`, for cascaded statements.
    pub max_route_values: usize,
    /// Tuples per probe statement (`router.lookup_batch_size`).
    pub batch: usize,
}

impl PartialEq for FkWork {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.graph, &o.graph)
            && self.checks == o.checks
            && self.parent == o.parent
            && self.session_cluster == o.session_cluster
    }
}

impl Eq for FkWork {}

impl FkWork {
    /// Whether there is nothing to do.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checks.iter().all(|c| c.tuples.is_empty()) && self.parent.is_none()
    }
}

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// `c1 = v1 AND c2 = v2`.
fn tuple_cond(cols: &[String], t: &[FkVal]) -> String {
    cols.iter()
        .zip(t)
        .map(|(c, v)| format!("{} = {}", q(c), v.sql))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The probe of `tuples` (indexes into the caller's list) in `schema`.`table`:
/// one row per tuple found, its index.
#[must_use]
pub fn probe_sql(
    schema: &str,
    table: &str,
    cols: &[String],
    tuples: &[(usize, &[FkVal])],
) -> String {
    let mut out = String::new();
    for (i, t) in tuples {
        if !out.is_empty() {
            out.push_str(" UNION ALL ");
        }
        let _ = write!(
            out,
            "(SELECT {i} FROM {}.{} WHERE {} LIMIT 1 LOCK IN SHARE MODE)",
            q(schema),
            q(table),
            tuple_cond(cols, t)
        );
    }
    out
}

/// The shards of `entry` that may hold `tuple` in `cols`: its key's
/// shard when the key is among them, else every shard (a reference table:
/// the copy on `near`, else its first).
fn shards_for(
    entry: &TableEntry,
    cols: &[String],
    tuple: &[FkVal],
    near: Option<&str>,
) -> Vec<usize> {
    let g = &entry.group;
    if let TableKind::Reference { .. } = entry.kind {
        let i = near
            .and_then(|c| g.shards.iter().position(|s| s.cluster == c))
            .unwrap_or(0);
        return vec![i];
    }
    if let Some(k) = entry.key()
        && let Some(j) = cols.iter().position(|c| c == k)
        && let Some(key) = tuple.get(j).and_then(|v| v.key.as_ref())
    {
        let kv = match (g.key_class, key) {
            (KeyClass::Int, KeyValue::Int(_))
            | (KeyClass::Str { .. } | KeyClass::Binary, KeyValue::Bytes(_)) => Some(key),
            _ => None,
        };
        if let Some(s) = kv.and_then(|kv| g.shard_of(kv)) {
            return vec![s];
        }
    }
    (0..g.shards.len()).collect()
}

fn target_of(entry: &TableEntry, i: usize, sql: String) -> Target {
    let g = &entry.group;
    let s = &g.shards[i];
    let at = match &entry.kind {
        TableKind::Reference { schema } => ShardAt {
            db: g.db,
            schema: schema.clone(),
            ..ShardAt::default()
        },
        TableKind::Sharded { .. } => g.at(i),
    };
    Target {
        cluster: s.cluster.clone(),
        shard: s.shard,
        sql,
        at,
    }
}

/// Which of `tuples` exist in `entry`'s `cols` (each probed on the shards
/// that may hold it), locking what it finds until the transaction ends.
async fn present(
    entry: &TableEntry,
    cols: &[String],
    tuples: &[Vec<FkVal>],
    near: Option<&str>,
    batch: usize,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<Vec<bool>> {
    let mut found = vec![false; tuples.len()];
    let mut by_shard: std::collections::BTreeMap<usize, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, t) in tuples.iter().enumerate() {
        for s in shards_for(entry, cols, t, near) {
            by_shard.entry(s).or_default().push(i);
        }
    }
    for (s, idx) in by_shard {
        let schema = match &entry.kind {
            TableKind::Reference { schema } => schema.clone(),
            TableKind::Sharded { .. } => entry.group.shards[s].schema.clone(),
        };
        for chunk in idx.chunks(batch.max(1)) {
            let want: Vec<(usize, &[FkVal])> = chunk
                .iter()
                .filter(|&&i| !found[i])
                .map(|&i| (i, tuples[i].as_slice()))
                .collect();
            if want.is_empty() {
                continue;
            }
            let t = target_of(entry, s, probe_sql(&schema, &entry.name, cols, &want));
            let ShardOutcome::Rows(rows) = be.run(&t, &t.sql, budget).await? else {
                return Err(SprigError::internal("a foreign-key probe answered no rows"));
            };
            for r in rows.rows {
                if let Some(i) = r
                    .first()
                    .and_then(Option::as_ref)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    && let Some(f) = found.get_mut(i)
                {
                    *f = true;
                }
            }
        }
    }
    Ok(found)
}

fn dedup(tuples: &[Vec<FkVal>]) -> Vec<Vec<FkVal>> {
    let mut seen = std::collections::HashSet::new();
    tuples
        .iter()
        .filter(|t| seen.insert((*t).clone()))
        .cloned()
        .collect()
}

/// 1452 for key `f`.
fn no_parent(g: &FkGraph, f: &FkRef) -> SprigError {
    SprigError::standard(
        1452,
        *b"23000",
        format!(
            "Cannot add or update a child row: a foreign key constraint fails ({})",
            f.describe(g)
        ),
    )
}

/// 1451 for key `f`.
fn has_children(g: &FkGraph, f: &FkRef) -> SprigError {
    SprigError::standard(
        1451,
        *b"23000",
        format!(
            "Cannot delete or update a parent row: a foreign key constraint fails ({})",
            f.describe(g)
        ),
    )
}

fn not_distributed(detail: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::FkActionNotDistributed, detail)
}

/// Runs `work` before its write's rows (chapter 19's child and parent
/// checks and R3's cascading actions); `depth` counts cascade levels.
///
/// # Errors
/// 1452, 1451, 39462 (an action past 15 levels, or one the router can't
/// apply), and the shards' errors.
pub async fn before_write(
    work: &FkWork,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
    depth: usize,
) -> Result<()> {
    if !be.foreign_key_checks() || work.is_empty() {
        return Ok(());
    }
    if depth > MAX_DEPTH {
        return Err(not_distributed(
            "a cascading foreign-key action more than 15 levels deep",
        ));
    }
    let g = &*work.graph;
    for c in &work.checks {
        let f = &g.fks[c.fk];
        let tuples = dedup(&c.tuples);
        if tuples.is_empty() {
            continue;
        }
        let parent = &g.tables[f.parent];
        let found = present(
            parent,
            &f.parent_columns,
            &tuples,
            c.near.as_deref(),
            work.batch,
            be,
            budget,
        )
        .await?;
        metrics::counter!("sprig_fk_checks_total", "side" => "child", "result" => if found.iter().all(|x| *x) { "ok" } else { "violation" })
            .increment(1);
        if found.iter().any(|x| !x) {
            return Err(no_parent(g, f));
        }
    }
    if let Some(p) = &work.parent {
        Box::pin(parent_side(work, p, be, budget, depth)).await?;
    }
    Ok(())
}

/// One changed parent row: its referenced values and, for an update, the
/// new values of the assigned columns.
struct Changed {
    old: Vec<Option<FkVal>>,
    new: Vec<Option<FkVal>>,
}

async fn parent_side(
    work: &FkWork,
    p: &FkParent,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
    depth: usize,
) -> Result<()> {
    let g = &*work.graph;
    let Output::Rows { rows, .. } = crate::exec::execute(&p.read, be, budget).await? else {
        return Err(SprigError::internal("a parent read answered no rows"));
    };
    let n_old = p.cols.len();
    let changed: Vec<Changed> = rows
        .iter()
        .map(|r| {
            let val = |i: usize| {
                p.classes
                    .get(i)
                    .and_then(|c| c.value(r.get(i).and_then(|v| v.as_deref())))
            };
            Changed {
                old: (0..n_old).map(val).collect(),
                new: (n_old..p.classes.len()).map(val).collect(),
            }
        })
        .collect();
    if changed.is_empty() {
        return Ok(());
    }
    let delete = matches!(p.kind, ParentKind::Delete);
    // Per key: the distinct old tuples without a NULL part.
    let mut per_fk: Vec<(usize, Vec<Vec<FkVal>>)> = Vec::new();
    for &fi in &p.fks {
        let f = &g.fks[fi];
        let pos: Vec<usize> = f
            .parent_columns
            .iter()
            .filter_map(|c| p.cols.iter().position(|x| x == c))
            .collect();
        if pos.len() != f.parent_columns.len() {
            return Err(SprigError::internal(
                "a parent read without a referenced column",
            ));
        }
        let tuples: Vec<Vec<FkVal>> = changed
            .iter()
            .filter_map(|ch| pos.iter().map(|&i| ch.old[i].clone()).collect())
            .collect();
        per_fk.push((fi, dedup(&tuples)));
    }
    // RESTRICT and NO ACTION first: one refusal rolls the statement back.
    for (fi, tuples) in &per_fk {
        let f = &g.fks[*fi];
        let action = if delete { f.on_delete } else { f.on_update };
        if !action.restricts() || tuples.is_empty() {
            continue;
        }
        let child = &g.tables[f.child];
        let found = present(
            child,
            &f.child_columns,
            tuples,
            None,
            work.batch,
            be,
            budget,
        )
        .await?;
        let hit = found.iter().any(|x| *x);
        metrics::counter!("sprig_fk_checks_total", "side" => "parent", "result" => if hit { "violation" } else { "ok" })
            .increment(1);
        if hit {
            return Err(has_children(g, f));
        }
    }
    for (fi, tuples) in &per_fk {
        let f = &g.fks[*fi];
        let action = if delete { f.on_delete } else { f.on_update };
        if action.restricts() || tuples.is_empty() {
            continue;
        }
        let sql = action_sql(g, f, action, delete, p, &changed, tuples)?;
        metrics::counter!("sprig_fk_actions_total", "action" => action.as_str()).increment(1);
        let cx = PlanCtx {
            topo: &GraphTopo(Arc::clone(&work.graph)),
            default_schema: Some(&g.default_schema),
            session_cluster: &work.session_cluster,
            max_route_values: work.max_route_values,
            lookup_batch: work.batch,
            ids: IdMode {
                filled: true,
                ..IdMode::default()
            },
        };
        let plan = crate::planner::plan(&sql, &cx);
        if let Plan::Reject(r) = &plan {
            return Err(not_distributed(format!(
                "ON {} {} of {}: {}",
                if delete { "DELETE" } else { "UPDATE" },
                action.sql(),
                f.name,
                r.detail
            )));
        }
        Box::pin(crate::exec::run_nested(&plan, be, budget, depth + 1)).await?;
    }
    Ok(())
}

/// The child statement of a cascading action.
fn action_sql(
    g: &FkGraph,
    f: &FkRef,
    action: FkAction,
    delete: bool,
    p: &FkParent,
    changed: &[Changed],
    tuples: &[Vec<FkVal>],
) -> Result<String> {
    let child = q(&g.tables[f.child].name);
    let any = tuples
        .iter()
        .map(|t| format!("({})", tuple_cond(&f.child_columns, t)))
        .collect::<Vec<_>>()
        .join(" OR ");
    match (action, delete) {
        (FkAction::Cascade, true) => Ok(format!("DELETE FROM {child} WHERE {any}")),
        (FkAction::SetNull, _) => {
            let set = f
                .child_columns
                .iter()
                .map(|c| format!("{} = NULL", q(c)))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!("UPDATE {child} SET {set} WHERE {any}"))
        }
        (FkAction::Cascade, false) => {
            // Each child column gets its parent's new value (or keeps its
            // own when that column isn't assigned), one CASE per column
            // over the old tuples, evaluated on the children's old values.
            let ParentKind::Update(assigned) = &p.kind else {
                return Err(SprigError::internal("ON UPDATE without an update"));
            };
            let old_pos: Vec<usize> = f
                .parent_columns
                .iter()
                .filter_map(|c| p.cols.iter().position(|x| x == c))
                .collect();
            let mut sets = Vec::new();
            for (j, (cc, pc)) in f.child_columns.iter().zip(&f.parent_columns).enumerate() {
                let Some(&(_, at)) = assigned.iter().find(|(c, _)| c == pc) else {
                    continue;
                };
                let mut case = format!("{} = CASE", q(cc));
                for ch in changed {
                    let old: Option<Vec<FkVal>> =
                        old_pos.iter().map(|&i| ch.old[i].clone()).collect();
                    let Some(old) = old else { continue };
                    let new = ch
                        .new
                        .get(at - p.cols.len())
                        .cloned()
                        .flatten()
                        .map_or_else(|| "NULL".to_owned(), |v| v.sql);
                    let _ = write!(
                        case,
                        " WHEN {} THEN {new}",
                        tuple_cond(&f.child_columns, &old)
                    );
                }
                let _ = write!(case, " ELSE {} END", q(cc));
                let _ = j;
                sets.push(case);
            }
            if sets.is_empty() {
                return Ok(String::new());
            }
            Ok(format!(
                "UPDATE {child} SET {} WHERE {any}",
                sets.join(", ")
            ))
        }
        _ => Err(not_distributed(format!(
            "ON {} {} of {}",
            if delete { "DELETE" } else { "UPDATE" },
            action.sql(),
            f.name
        ))),
    }
}

/// The distinct non-`NULL` parent tables a write's checks name, for
/// `EXPLAIN`.
#[must_use]
pub fn explain_lines(w: &FkWork) -> Vec<String> {
    let g = &*w.graph;
    let mut out: BTreeSet<String> = BTreeSet::new();
    for c in &w.checks {
        let f = &g.fks[c.fk];
        out.insert(format!(
            "fk_check {} -> {} ({} tuples)",
            f.name,
            g.tables[f.parent].name,
            c.tuples.len()
        ));
    }
    if let Some(p) = &w.parent {
        for &fi in &p.fks {
            let f = &g.fks[fi];
            let action = match p.kind {
                ParentKind::Delete => f.on_delete,
                ParentKind::Update(_) => f.on_update,
            };
            out.insert(format!(
                "fk_parent {} <- {} ({})",
                f.name,
                g.tables[f.child].name,
                action.as_str()
            ));
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod plan_tests;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn values_read_back_as_literals() {
        let v = ValClass::Str.value(Some(b"616263")).unwrap();
        assert_eq!(v.sql, "_utf8mb4 X'616263'");
        assert_eq!(v.key, Some(KeyValue::Bytes(b"abc".to_vec())));
        let v = ValClass::Num.value(Some(b"-17")).unwrap();
        assert_eq!((v.sql.as_str(), v.key), ("-17", Some(KeyValue::Int(-17))));
        assert_eq!(ValClass::Text.value(Some(b"it's")).unwrap().sql, "'it''s'");
        assert!(ValClass::Num.value(None).is_none());
        assert_eq!(
            ValClass::Str.read_expr("`c`"),
            "HEX(CONVERT(`c` USING utf8mb4))"
        );
    }

    #[test]
    fn probes_lock_each_tuple() {
        let a = [FkVal {
            sql: "1".into(),
            key: None,
        }];
        let b = [FkVal {
            sql: "_utf8mb4 X'41'".into(),
            key: None,
        }];
        let s = probe_sql(
            "t1_d1_g2_s1",
            "p",
            &["id".into()],
            &[(0, &a[..]), (3, &b[..])],
        );
        assert_eq!(
            s,
            "(SELECT 0 FROM `t1_d1_g2_s1`.`p` WHERE `id` = 1 LIMIT 1 LOCK IN SHARE MODE) \
             UNION ALL (SELECT 3 FROM `t1_d1_g2_s1`.`p` WHERE `id` = _utf8mb4 X'41' LIMIT 1 \
             LOCK IN SHARE MODE)"
        );
    }
}
