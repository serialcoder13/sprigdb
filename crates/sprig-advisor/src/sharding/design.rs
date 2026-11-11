//! The advisor's designs: the structural seed, scoring, the search and the
//! verdict (chapter 16, "Structural design", "Scoring", "Key quality
//! filters", "Search", "Confidence", "Verdicts and guardrails").
//!
//! Everything here is pure and deterministic: the same [`Input`] gives the
//! same design, whatever order its parts came in.
//!
//! **R1 as built.** Statements carry no parameter values here (chapter 12
//! samples are sealed, and the resolver of chapter 16 needs them), so a
//! statement's targets are judged by which columns it fixes, not by the
//! hashes of their values: an equality (or an insert's column list) on a
//! member's routing columns pins it to one shard; an equality on a unique
//! key that doesn't hold them pins it through a unique lookup (one lookup's
//! penalty); a parent member is pinned by an insert (the gateway reads its
//! parent's hash) or by an equality on its foreign key (the resolver, one
//! lookup); and members joined by a co-location-safe equality share their
//! pin. Two pinned components of one group count as two shards, as the
//! book counts distinct hashes. Moves are `Attach` (join-frequency
//! clustering) and `Detach`; reference tables, `Rekey` and `Reparent` wait
//! for R3.
//!
//! **Pins** ([`Input::pins`], chapter 16 step 8) bind the seed and the
//! search: a table pinned `UNSHARDED` or `REFERENCE` stays out of sharded
//! groups (a `REFERENCE` pin too, since reference copies aren't built in
//! R1); `SHARD KEY (cols)` makes the table route by exactly `cols`, as a
//! column member of its parent's group when the foreign key reaches the
//! group's key through `cols`, else as the root of its own group (its key
//! passes no NDV or skew filter, only the type check); `COLOCATE WITH p ON
//! (cols)` makes it a column member of `p`'s group on `cols`, in place of
//! any foreign-key parent. The search never moves a pinned table.
//!
//! **Joins across models** (chapter 22, "Co-location through the advisor";
//! added by r1close). A join edge carries the query language whose
//! equality it uses ([`Rules`]): SQL's (MySQL's comparison of the two
//! columns' types) or MongoDB's (`$lookup`, from R5). An edge co-locates
//! only when `sprig_shard::colocate::edge_colocated` holds: under SQL's
//! rules both columns must fit the group's key and MySQL's comparison of
//! their types must imply equal normalized bytes (an `INT` against a
//! `VARCHAR` compares in double precision and doesn't; a `utf8mb4_bin`
//! column against a `utf8mb4_general_ci` key does). A parent edge (a
//! member joined to its foreign-key parent) needs the same of the two
//! foreign-key columns. Edges that can never co-locate don't propose
//! moves. Members of a first design have no flags yet, so every member is
//! taken as clean.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_shard::colocate::{MemberFlags, SqlType, edge_colocated};
use sprig_shard::pins::{Pin, PinKind};
use sprig_shard::{KeyClass, KeySpec};

use super::settings::{AdvisorMode, AdvisorSettings};

/// An object id (chapter 08's `objects.object_id`).
pub type Obj = u64;

/// One column of a table.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// The name, lower case.
    pub name: String,
    /// Its key class; `None` when it can't be a key part.
    pub class: Option<KeyClass>,
    /// Distinct values (`InnoDB`'s estimate), if known.
    pub ndv: Option<f64>,
    /// The share of rows holding its most common value, if known.
    pub top_share: Option<f64>,
    /// Its SQL type (chapter 22's type map), for the comparison a join on
    /// it uses; `None` when unknown (its joins then never co-locate under
    /// SQL's rules).
    pub sql: Option<SqlType>,
}

/// One table.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The object.
    pub object: Obj,
    /// Its database.
    pub db: u32,
    /// Its name.
    pub name: String,
    /// Columns in table order.
    pub columns: Vec<Column>,
    /// Primary-key columns.
    pub pk: Vec<String>,
    /// Other unique keys.
    pub uniques: Vec<Vec<String>>,
    /// Data and index bytes.
    pub size_bytes: f64,
}

impl Table {
    fn column(&self, n: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == n)
    }
}

/// A declared foreign key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    /// The constraint name.
    pub name: String,
    /// The child table.
    pub child: Obj,
    /// Its columns, in order.
    pub child_cols: Vec<String>,
    /// The parent table.
    pub parent: Obj,
    /// The referenced columns, in order.
    pub parent_cols: Vec<String>,
}

/// What one statement does to one table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Touch {
    /// The table.
    pub object: Obj,
    /// Columns fixed by `=` (top-level conjuncts).
    pub eq: Vec<String>,
    /// An insert's columns (every column for an insert without a list).
    pub insert: Vec<String>,
    /// The statement writes the table.
    pub write: bool,
}

/// Which query language's equality a join edge uses (chapter 22,
/// "Comparison rules per query language").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Rules {
    /// SQL, and what compiles to it (the Analytics API, REST, gRPC):
    /// MySQL's comparison of the two columns' types.
    #[default]
    Sql,
    /// MongoDB's `$lookup` (R5): numbers by value across types, a string
    /// never equals a number.
    Mongo,
}

/// A join `a.x = b.y`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEdge {
    /// One side.
    pub a: (Obj, String),
    /// The other.
    pub b: (Obj, String),
    /// The equality's rules.
    pub rules: Rules,
}

impl JoinEdge {
    /// Whether this edge could co-locate its two sides in some group: under
    /// MongoDB's rules always; under SQL's, when both columns' types are
    /// known and their comparison implies equal normalized bytes under the
    /// first side's key class. Edges that can't never propose a move.
    #[must_use]
    pub fn could_colocate(&self, input: &Input) -> bool {
        if self.rules == Rules::Mongo {
            return true;
        }
        let (Some(a), Some(b)) = (input.sql_type(&self.a), input.sql_type(&self.b)) else {
            return false;
        };
        a.key_class()
            .is_some_and(|k| edge_colocated(true, &a, &b, k, CLEAN, CLEAN))
    }
}

/// A member of a first design: no flag set yet.
const CLEAN: MemberFlags = MemberFlags {
    off_class_seen: false,
    double_inexact_seen: false,
};

/// One statement fingerprint over the window.
#[derive(Debug, Clone, PartialEq)]
pub struct Stmt {
    /// Executions.
    pub weight: f64,
    /// The fingerprint, for the evidence.
    pub fingerprint: u64,
    /// Tables touched.
    pub touches: Vec<Touch>,
    /// Equality joins.
    pub joins: Vec<JoinEdge>,
}

/// One transaction footprint over the window.
#[derive(Debug, Clone, PartialEq)]
pub struct Unit {
    /// Commits.
    pub weight: f64,
    /// Tables written.
    pub writes: Vec<Obj>,
}

/// Everything a run reads.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Input {
    /// The tenant's tables.
    pub tables: Vec<Table>,
    /// Declared foreign keys.
    pub fks: Vec<ForeignKey>,
    /// Statements.
    pub stmts: Vec<Stmt>,
    /// Write transactions.
    pub units: Vec<Unit>,
    /// `sharding.split_size_gb` in bytes (for expected shard counts).
    pub split_bytes: f64,
    /// The tenant's pins (`SPRIG PIN`, chapter 15's `group_pins`).
    pub pins: Vec<Pin>,
    /// Chapter 33's key-value units (a key table with its element
    /// tables), placed by [`super::kv::imply`] (decision R28, R6 round 5).
    pub kv: Vec<super::kv::KvUnit>,
    /// `sharding.split_write_share_pct` as a ratio, for the key-value load
    /// rule (0: the default, 0.5).
    pub split_write_share: f64,
    /// Lines the key-value rule left for the run's evidence.
    pub notes: Vec<String>,
    /// Objects the key-value rule placed (their implied pins aren't
    /// reported as client pins).
    pub kv_decided: Vec<Obj>,
}

impl Input {
    fn table(&self, o: Obj) -> Option<&Table> {
        self.tables.iter().find(|t| t.object == o)
    }

    /// The SQL type of column `side.1` of table `side.0`.
    fn sql_type(&self, side: &(Obj, String)) -> Option<SqlType> {
        self.table(side.0)?.column(&side.1)?.sql
    }

    /// `o`'s pin.
    #[must_use]
    pub fn pin(&self, o: Obj) -> Option<&PinKind> {
        self.pins.iter().find(|p| p.object.0 == o).map(|p| &p.kind)
    }

    /// Sorts every list, so equal inputs in any order give equal designs.
    pub fn normalize(&mut self) {
        self.tables.sort_by_key(|t| t.object);
        self.fks
            .sort_by(|a, b| (a.child, &a.name).cmp(&(b.child, &b.name)));
        for s in &mut self.stmts {
            s.touches.sort_by_key(|t| t.object);
            for t in &mut s.touches {
                t.eq.sort();
                t.eq.dedup();
                t.insert.sort();
                t.insert.dedup();
            }
            s.joins.sort_by(|x, y| (&x.a, &x.b).cmp(&(&y.a, &y.b)));
        }
        self.stmts.sort_by_key(|s| s.fingerprint);
        for u in &mut self.units {
            u.writes.sort_unstable();
            u.writes.dedup();
        }
        self.units
            .sort_by(|a, b| a.writes.cmp(&b.writes).then(a.weight.total_cmp(&b.weight)));
        self.pins.sort_by_key(|p| p.object);
        self.pins.dedup_by_key(|p| p.object);
        self.kv.sort_by_key(|u| u.key);
    }

    /// Total weight of the statements and units, as `units_sampled`.
    #[must_use]
    pub fn units_sampled(&self) -> u64 {
        let s: f64 = self.stmts.iter().map(|s| s.weight).sum::<f64>()
            + self.units.iter().map(|u| u.weight).sum::<f64>();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let n = s as u64;
        n
    }
}

/// How a member supplies its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Routing {
    /// Its own columns hold the key.
    Columns {
        /// The key columns, in key order.
        columns: Vec<String>,
    },
    /// Its parent's hash, through a declared foreign key.
    Parent {
        /// The parent.
        parent: Obj,
        /// The constraint.
        fk: String,
    },
}

/// One member of a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// The table.
    pub object: Obj,
    /// How it routes.
    pub routing: Routing,
}

/// One sharded group: its key and members (the first is the root).
#[derive(Debug, Clone, PartialEq)]
pub struct GroupDesign {
    /// The display name: the root table's.
    pub name: String,
    /// The key's classes.
    pub key: Vec<KeyClass>,
    /// Members, root first.
    pub members: Vec<Member>,
}

impl GroupDesign {
    /// The group's `KeySpec`.
    ///
    /// # Errors
    /// What `KeySpec::new` returns.
    pub fn key_spec(&self) -> sprig_common::Result<KeySpec> {
        KeySpec::new(self.key.clone())
    }
}

/// A design: groups, and what stays unsharded.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Design {
    /// Sharded groups.
    pub groups: Vec<GroupDesign>,
    /// Objects in their database's default group.
    pub unsharded: Vec<Obj>,
}

impl Design {
    fn place(&self) -> BTreeMap<Obj, (usize, &Routing)> {
        let mut m = BTreeMap::new();
        for (g, gd) in self.groups.iter().enumerate() {
            for mem in &gd.members {
                m.insert(mem.object, (g, &mem.routing));
            }
        }
        m
    }

    fn canonical(mut self) -> Self {
        for g in &mut self.groups {
            let root = g.members.remove(0);
            g.members.sort_by_key(|m| m.object);
            g.members.insert(0, root);
        }
        self.groups.sort_by_key(|g| g.members[0].object);
        self.unsharded.sort_unstable();
        self.unsharded.dedup();
        self
    }

    /// The design as `advisor_runs.design` stores it.
    #[must_use]
    pub fn to_json(&self, input: &Input) -> serde_json::Value {
        let name = |o: Obj| {
            input
                .table(o)
                .map_or_else(|| o.to_string(), |t| t.name.clone())
        };
        json!({
            "groups": self.groups.iter().map(|g| json!({
                "name": g.name,
                "key": g.key_spec().map(|k| k.to_json()).unwrap_or(serde_json::Value::Null),
                "members": g.members.iter().map(|m| json!({
                    "object": m.object,
                    "table": name(m.object),
                    "routing": m.routing,
                    "pin": input.pin(m.object).map(PinKind::as_str),
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "unsharded": self.unsharded.iter().map(|o| json!({
                "object": o,
                "table": name(*o),
                "pin": input.pin(*o).map(PinKind::as_str),
            })).collect::<Vec<_>>(),
        })
    }
}

/// A design's score (chapter 16, `Score`).
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct Score {
    /// The weighted score in [0, 1].
    pub total: f64,
    /// The share of statement weight that is local.
    pub stmt_local: f64,
    /// The share of transaction weight that is local.
    pub txn_local: f64,
    /// Lookups per execution, weighted.
    pub lookups: f64,
}

/// The expected shard count of a group of `bytes`.
#[must_use]
pub fn expected_shards(bytes: f64, split_bytes: f64) -> f64 {
    if split_bytes <= 0.0 {
        return 2.0;
    }
    (bytes / (split_bytes / 2.0)).ceil().max(2.0)
}

/// Why `cols` of `t` can't be a key, or its classes.
///
/// # Errors
/// The reason, for the evidence.
pub fn key_quality(
    t: &Table,
    cols: &[String],
    group_bytes: f64,
    input: &Input,
    s: &AdvisorSettings,
) -> Result<Vec<KeyClass>, String> {
    if cols.is_empty() {
        return Err(format!("{}: no key columns", t.name));
    }
    let mut classes = Vec::new();
    for c in cols {
        let col = t
            .column(c)
            .ok_or_else(|| format!("{}.{c}: no such column", t.name))?;
        let class = col
            .class
            .ok_or_else(|| format!("{}.{c}: type or collation can't be a key", t.name))?;
        classes.push(class);
    }
    if cols.len() == 1
        && let Some(col) = t.column(&cols[0])
    {
        #[allow(clippy::cast_precision_loss)]
        let need = s.min_ndv_per_shard as f64 * expected_shards(group_bytes, input.split_bytes);
        if let Some(ndv) = col.ndv
            && ndv < need
        {
            return Err(format!(
                "{}.{}: {ndv:.0} distinct values, {need:.0} needed",
                t.name, col.name
            ));
        }
        if let Some(top) = col.top_share
            && top > s.max_top_value
        {
            return Err(format!(
                "{}.{}: one value holds {:.0}% of rows",
                t.name,
                col.name,
                top * 100.0
            ));
        }
    }
    Ok(classes)
}

fn same_cols(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

/// The weight of statements joining `fk`'s ends along its columns, or
/// writing both.
fn fk_weight(input: &Input, fk: &ForeignKey) -> f64 {
    let mut w = 0.0;
    for s in &input.stmts {
        let joined = s.joins.iter().any(|j| {
            let (a, b) = (&j.a, &j.b);
            [(a, b), (b, a)].iter().any(|(x, y)| {
                x.0 == fk.child
                    && y.0 == fk.parent
                    && fk.child_cols.contains(&x.1)
                    && fk.parent_cols.contains(&y.1)
            })
        });
        let writes_both = s
            .touches
            .iter()
            .filter(|t| t.write && (t.object == fk.child || t.object == fk.parent))
            .count()
            == 2;
        if joined || writes_both {
            w += s.weight;
        }
    }
    for u in &input.units {
        if u.writes.contains(&fk.child) && u.writes.contains(&fk.parent) {
            w += u.weight;
        }
    }
    w
}

/// How a table reaches its parent in the seed's forest.
#[derive(Debug, Clone, Copy)]
enum Edge<'a> {
    /// A declared foreign key.
    Fk(&'a ForeignKey),
    /// A `COLOCATE WITH parent ON (columns)` pin.
    Pin { parent: Obj, columns: &'a [String] },
}

impl Edge<'_> {
    fn parent(&self) -> Obj {
        match self {
            Edge::Fk(f) => f.parent,
            Edge::Pin { parent, .. } => *parent,
        }
    }
}

/// The classes of `cols` of `t`, when every one can be a key part.
fn classes_of(t: &Table, cols: &[String]) -> Option<Vec<KeyClass>> {
    if cols.is_empty() {
        return None;
    }
    cols.iter()
        .map(|c| t.column(c).and_then(|x| x.class))
        .collect()
}

/// A child's routing in a group keyed by `key`, given its parent's; `None`
/// when a `COLOCATE` pin's columns don't hold the key.
fn derive(
    child: &Table,
    edge: Edge<'_>,
    parent: Option<&Routing>,
    key: &[KeyClass],
) -> Option<Routing> {
    match edge {
        Edge::Fk(f) => {
            let via_columns = match parent {
                Some(Routing::Columns { columns }) if same_cols(&f.parent_cols, columns) => {
                    // The child's columns, in key order, with the key's classes.
                    let cols = f.child_cols.clone();
                    let ok = cols.len() == key.len()
                        && cols
                            .iter()
                            .zip(key)
                            .all(|(c, k)| child.column(c).and_then(|x| x.class) == Some(*k));
                    ok.then_some(Routing::Columns { columns: cols })
                }
                _ => None,
            };
            Some(via_columns.unwrap_or_else(|| Routing::Parent {
                parent: f.parent,
                fk: f.name.clone(),
            }))
        }
        Edge::Pin { columns, .. } => {
            (classes_of(child, columns).as_deref() == Some(key)).then(|| Routing::Columns {
                columns: columns.to_vec(),
            })
        }
    }
}

/// Chapter 16's `structural_design`, with the evidence of what it rejected.
#[must_use]
#[allow(clippy::too_many_lines)] // the book's five steps, pins first
pub fn structural(input: &Input, s: &AdvisorSettings) -> (Design, Vec<String>) {
    // Key-value units first (decision R28): their pins are implied.
    let owned;
    let input = if input.kv.is_empty() {
        input
    } else {
        let mut i = input.clone();
        super::kv::imply(&mut i, s);
        owned = i;
        &owned
    };
    let mut evidence = input.notes.clone();
    let mut design = Design::default();
    let name = |o: Obj| {
        input
            .table(o)
            .map_or_else(|| o.to_string(), |t| t.name.clone())
    };
    // 1. Pins: tables pinned out of sharded groups.
    let mut out: BTreeSet<Obj> = BTreeSet::new();
    for t in &input.tables {
        match input.pin(t.object) {
            Some(PinKind::Unsharded) => {
                if !input.kv_decided.contains(&t.object) {
                    evidence.push(format!("{}: pinned UNSHARDED", t.name));
                }
                out.insert(t.object);
            }
            Some(PinKind::Reference) => {
                evidence.push(format!(
                    "{}: pinned REFERENCE; unsharded in R1 (reference copies are not built)",
                    t.name
                ));
                out.insert(t.object);
            }
            _ => {}
        }
    }
    design.unsharded.extend(out.iter().copied());
    // 2. The forest: a COLOCATE pin is the table's parent edge; otherwise
    // each child keeps its heaviest foreign key.
    let mut parent_of: BTreeMap<Obj, Edge<'_>> = BTreeMap::new();
    for t in &input.tables {
        if out.contains(&t.object) {
            continue;
        }
        let mut fks: Vec<&ForeignKey> = input
            .fks
            .iter()
            .filter(|f| f.child == t.object && f.parent != t.object)
            .filter(|f| input.table(f.parent).is_some() && !out.contains(&f.parent))
            .collect();
        fks.sort_by(|a, b| {
            fk_weight(input, b)
                .total_cmp(&fk_weight(input, a))
                .then(a.name.cmp(&b.name))
        });
        let pinned = match input.pin(t.object) {
            Some(PinKind::Colocate { parent, columns }) => {
                let p = parent.0;
                if p != t.object && input.table(p).is_some() && !out.contains(&p) {
                    Some(Edge::Pin { parent: p, columns })
                } else {
                    evidence.push(format!(
                        "{}: pin COLOCATE WITH {} not honoured (no such sharded table)",
                        t.name,
                        name(p)
                    ));
                    None
                }
            }
            _ => None,
        };
        if let Some(e) = pinned {
            parent_of.insert(t.object, e);
            for other in &fks {
                evidence.push(format!(
                    "{}: cross-group foreign key {} (pinned COLOCATE WITH {})",
                    t.name,
                    other.name,
                    name(e.parent())
                ));
            }
        } else if let Some(f) = fks.first() {
            parent_of.insert(t.object, Edge::Fk(f));
            for other in &fks[1..] {
                evidence.push(format!(
                    "{}: cross-group foreign key {} (kept {})",
                    t.name, other.name, f.name
                ));
            }
        }
    }
    // Break cycles: a table whose parent chain returns to it is a root.
    // Foreign keys give way first; pins only on a cycle of pins.
    for pins_too in [false, true] {
        for t in &input.tables {
            let mut seen = BTreeSet::new();
            let mut cur = t.object;
            let mut cyclic = false;
            while let Some(e) = parent_of.get(&cur) {
                if !seen.insert(cur) {
                    cyclic = true;
                    break;
                }
                cur = e.parent();
            }
            let removable = pins_too || matches!(parent_of.get(&t.object), Some(Edge::Fk(_)));
            if cyclic && removable {
                parent_of.remove(&t.object);
            }
        }
    }
    let children = |p: Obj| -> Vec<(Obj, Edge<'_>)> {
        let mut v: Vec<(Obj, Edge<'_>)> = parent_of
            .iter()
            .filter(|(_, e)| e.parent() == p)
            .map(|(c, e)| (*c, *e))
            .collect();
        v.sort_by_key(|(c, _)| *c);
        v
    };
    let mut work: std::collections::VecDeque<Obj> = input
        .tables
        .iter()
        .map(|t| t.object)
        .filter(|o| !out.contains(o) && !parent_of.contains_key(o))
        .collect();
    let mut done: BTreeSet<Obj> = out.clone();
    while let Some(r) = work.pop_front() {
        if !done.insert(r) {
            continue;
        }
        let Some(root) = input.table(r) else { continue };
        // The tree's bytes, before any pinned member leaves it.
        let mut tree = vec![r];
        let mut i = 0;
        while i < tree.len() {
            for (c, _) in children(tree[i]) {
                if !done.contains(&c) {
                    tree.push(c);
                }
            }
            i += 1;
        }
        let bytes: f64 = tree
            .iter()
            .filter_map(|o| input.table(*o))
            .map(|t| t.size_bytes)
            .sum();
        // 3. The root's key: a SHARD KEY pin's (type checks only), else
        // the primary key's, through the quality filters.
        let (key_cols, key) = match input.pin(r) {
            Some(PinKind::ShardKey { columns }) => (
                columns.clone(),
                classes_of(root, columns).ok_or_else(|| {
                    format!(
                        "{}: pinned SHARD KEY ({}) can't be a key",
                        root.name,
                        columns.join(", ")
                    )
                }),
            ),
            _ => (
                root.pk.clone(),
                key_quality(root, &root.pk, bytes, input, s),
            ),
        };
        let key = match key {
            Ok(k) => Some(k),
            Err(why) => {
                evidence.push(format!("{}: unsharded ({why})", root.name));
                design.unsharded.push(r);
                None
            }
        };
        let mut members = vec![Member {
            object: r,
            routing: Routing::Columns {
                columns: key_cols.clone(),
            },
        }];
        let mut routing: BTreeMap<Obj, Routing> = BTreeMap::new();
        routing.insert(r, Routing::Columns { columns: key_cols });
        // Descendants, nearest first. A pinned one that can't route as its
        // pin says leaves with its subtree for a group of its own.
        let mut queue = std::collections::VecDeque::from([r]);
        while let Some(p) = queue.pop_front() {
            for (c, edge) in children(p) {
                if done.contains(&c) {
                    continue;
                }
                let Some(child) = input.table(c) else {
                    continue;
                };
                let derived = key
                    .as_deref()
                    .and_then(|k| derive(child, edge, routing.get(&p), k));
                if let Some(PinKind::ShardKey { columns }) = input.pin(c)
                    && derived.as_ref()
                        != Some(&Routing::Columns {
                            columns: columns.clone(),
                        })
                {
                    evidence.push(format!(
                        "{}: pinned SHARD KEY ({}): a group of its own",
                        child.name,
                        columns.join(", ")
                    ));
                    work.push_back(c);
                    continue;
                }
                if key.is_none() {
                    if matches!(edge, Edge::Pin { .. }) {
                        evidence.push(format!(
                            "{}: pin COLOCATE WITH {} not honoured ({} is unsharded)",
                            child.name,
                            name(edge.parent()),
                            name(edge.parent())
                        ));
                    }
                    design.unsharded.push(c);
                    done.insert(c);
                    queue.push_back(c);
                    continue;
                }
                let Some(rt) = derived else {
                    evidence.push(format!(
                        "{}: pin COLOCATE WITH {} not honoured (its columns don't hold the \
                         group's key)",
                        child.name,
                        name(edge.parent())
                    ));
                    work.push_back(c);
                    continue;
                };
                routing.insert(c, rt.clone());
                members.push(Member {
                    object: c,
                    routing: rt,
                });
                done.insert(c);
                queue.push_back(c);
            }
        }
        if let Some(key) = key {
            design.groups.push(GroupDesign {
                name: root.name.clone(),
                key,
                members,
            });
        }
    }
    (design.canonical(), evidence)
}

/// A statement's targets under a design.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Place {
    /// The database's default group.
    Default(u32),
    /// One shard of a group: a pinned component.
    Point(usize, Obj),
    /// Every shard of a group.
    Scatter(usize),
}

struct Placed<'a> {
    at: BTreeMap<Obj, (usize, &'a Routing)>,
    /// Each group's key, by group index.
    keys: Vec<&'a [KeyClass]>,
}

impl Placed<'_> {
    /// Whether `a.x = b.y` keeps rows of both on one shard.
    fn safe(&self, input: &Input, j: &JoinEdge) -> bool {
        let (Some((ga, ra)), Some((gb, rb))) = (self.at.get(&j.a.0), self.at.get(&j.b.0)) else {
            return false;
        };
        if ga != gb {
            return false;
        }
        let single = |r: &Routing, c: &str| matches!(r, Routing::Columns { columns } if columns.len() == 1 && columns[0] == c);
        // Chapter 22: the equality itself must imply equal normalized bytes
        // under `key`, by the edge's rules and the two columns' types.
        let types_ok = |x: &(Obj, String), y: &(Obj, String), key: Option<KeyClass>| {
            if j.rules == Rules::Mongo {
                return true;
            }
            match (input.sql_type(x), input.sql_type(y), key) {
                (Some(tx), Some(ty), Some(k)) => edge_colocated(true, &tx, &ty, k, CLEAN, CLEAN),
                _ => false,
            }
        };
        if single(ra, &j.a.1) && single(rb, &j.b.1) {
            let key = self.keys.get(*ga).and_then(|k| k.first().copied());
            return types_ok(&j.a, &j.b, key);
        }
        let parent_edge = |child: &(Obj, String), cr: &Routing, parent: &(Obj, String)| {
            if let Routing::Parent { parent: p, fk } = cr
                && *p == parent.0
                && let Some(f) = input
                    .fks
                    .iter()
                    .find(|f| &f.name == fk && f.child == child.0)
            {
                // The foreign key's columns, under the parent column's own
                // class.
                let key = input.sql_type(parent).and_then(|t| t.key_class());
                return f.child_cols.len() == 1
                    && f.child_cols[0] == child.1
                    && f.parent_cols[0] == parent.1
                    && types_ok(child, parent, key);
            }
            false
        };
        parent_edge(&j.a, ra, &j.b) || parent_edge(&j.b, rb, &j.a)
    }
}

/// Whether `t` pins its table to one shard, and the lookups it costs.
fn pinned(input: &Input, t: &Touch, r: &Routing) -> Option<u32> {
    let has =
        |cols: &[String], set: &[String]| !cols.is_empty() && cols.iter().all(|c| set.contains(c));
    match r {
        Routing::Columns { columns } => {
            if has(columns, &t.eq) || has(columns, &t.insert) {
                return Some(0);
            }
        }
        Routing::Parent { fk, .. } => {
            if !t.insert.is_empty() {
                return Some(0);
            }
            if let Some(f) = input
                .fks
                .iter()
                .find(|f| &f.name == fk && f.child == t.object)
                && has(&f.child_cols, &t.eq)
            {
                return Some(1);
            }
        }
    }
    let tbl = input.table(t.object)?;
    let unique = std::iter::once(&tbl.pk).chain(&tbl.uniques);
    for u in unique {
        if has(u, &t.eq) {
            return Some(1);
        }
    }
    None
}

/// Chapter 16's `score(D)`.
#[must_use]
pub fn score(input: &Input, d: &Design, s: &AdvisorSettings) -> Score {
    score_weighted(input, d, s, None)
}

/// The root of `x` in a union-find over objects.
fn find(c: &mut BTreeMap<Obj, Obj>, x: Obj) -> Obj {
    let p = c.get(&x).copied().unwrap_or(x);
    if p == x {
        return x;
    }
    let r = find(c, p);
    c.insert(x, r);
    r
}

fn score_weighted(input: &Input, d: &Design, s: &AdvisorSettings, w: Option<&[f64]>) -> Score {
    let placed = Placed {
        at: d.place(),
        keys: d.groups.iter().map(|g| g.key.as_slice()).collect(),
    };
    let db_of = |o: Obj| input.table(o).map_or(0, |t| t.db);
    let (mut num, mut den, mut stmt_num, mut stmt_den, mut look) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for (i, st) in input.stmts.iter().enumerate() {
        let weight = w.map_or(st.weight, |w| w[i]);
        if weight == 0.0 {
            continue;
        }
        let mut places = BTreeSet::new();
        // Union-find over touched objects joined safely.
        let objs: Vec<Obj> = st.touches.iter().map(|t| t.object).collect();
        let mut comp: BTreeMap<Obj, Obj> = objs.iter().map(|o| (*o, *o)).collect();
        for j in &st.joins {
            if comp.contains_key(&j.a.0) && comp.contains_key(&j.b.0) && placed.safe(input, j) {
                let (a, b) = (find(&mut comp, j.a.0), find(&mut comp, j.b.0));
                if a != b {
                    let (lo, hi) = (a.min(b), a.max(b));
                    comp.insert(hi, lo);
                }
            }
        }
        let mut pins: BTreeMap<Obj, u32> = BTreeMap::new();
        let mut lookups = 0u32;
        for t in &st.touches {
            match placed.at.get(&t.object) {
                None => {
                    places.insert(Place::Default(db_of(t.object)));
                }
                Some((_, r)) => {
                    if let Some(l) = pinned(input, t, r) {
                        let c = find(&mut comp, t.object);
                        let e = pins.entry(c).or_insert(l);
                        *e = (*e).min(l);
                    }
                }
            }
        }
        for t in &st.touches {
            if let Some((g, _)) = placed.at.get(&t.object) {
                let c = find(&mut comp, t.object);
                match pins.get(&c) {
                    Some(_) => {
                        places.insert(Place::Point(*g, c));
                    }
                    None => {
                        places.insert(Place::Scatter(*g));
                    }
                }
            }
        }
        lookups += pins.values().sum::<u32>();
        let local = places.len() == 1 && !matches!(places.first(), Some(Place::Scatter(_)));
        let credit = if local {
            (1.0 - s.lookup_penalty * f64::from(lookups)).max(0.0)
        } else {
            0.0
        };
        num += weight * credit;
        den += weight;
        stmt_num += weight * f64::from(u8::from(local));
        stmt_den += weight;
        look += weight * f64::from(lookups);
    }
    let (mut txn_num, mut txn_den) = (0.0, 0.0);
    for u in &input.units {
        let mut where_: BTreeSet<Place> = BTreeSet::new();
        for o in &u.writes {
            match placed.at.get(o) {
                Some((g, _)) => where_.insert(Place::Scatter(*g)),
                None => where_.insert(Place::Default(db_of(*o))),
            };
        }
        let local = where_.len() <= 1;
        num += u.weight * s.txn_weight * f64::from(u8::from(local));
        den += u.weight * s.txn_weight;
        txn_num += u.weight * f64::from(u8::from(local));
        txn_den += u.weight;
    }
    let ratio = |a: f64, b: f64| if b > 0.0 { a / b } else { 1.0 };
    Score {
        total: ratio(num, den),
        stmt_local: ratio(stmt_num, stmt_den),
        txn_local: ratio(txn_num, txn_den),
        lookups: ratio(look, stmt_den),
    }
}

/// A move of the search.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "move")]
pub enum Move {
    /// `o` joins group `g` as a column member on `column`.
    Attach {
        /// The table.
        object: Obj,
        /// The group, by its root.
        group_root: Obj,
        /// The column holding the key.
        column: String,
    },
    /// `o` leaves its group for one of its own on its primary key.
    Detach {
        /// The table.
        object: Obj,
    },
}

fn apply_move(input: &Input, d: &Design, m: &Move, s: &AdvisorSettings) -> Option<Design> {
    // The search never moves a pinned table.
    let (Move::Attach { object, .. } | Move::Detach { object }) = m;
    if input.pin(*object).is_some() {
        return None;
    }
    let mut d = d.clone();
    match m {
        Move::Attach {
            object,
            group_root,
            column,
        } => {
            // Only a group of its own (or an unsharded table) moves.
            let mine = d
                .groups
                .iter()
                .position(|g| g.members.len() == 1 && g.members[0].object == *object);
            let was_unsharded = d.unsharded.contains(object);
            if mine.is_none() && !was_unsharded {
                return None;
            }
            let gi = d
                .groups
                .iter()
                .position(|g| g.members[0].object == *group_root)?;
            let t = input.table(*object)?;
            if d.groups[gi].key.len() != 1
                || t.column(column).and_then(|c| c.class) != Some(d.groups[gi].key[0])
            {
                return None;
            }
            d.groups[gi].members.push(Member {
                object: *object,
                routing: Routing::Columns {
                    columns: vec![column.clone()],
                },
            });
            if let Some(i) = mine {
                d.groups.remove(i);
            }
            d.unsharded.retain(|o| o != object);
        }
        Move::Detach { object } => {
            let gi = d
                .groups
                .iter()
                .position(|g| g.members[1..].iter().any(|m| m.object == *object))?;
            let dependents = d.groups[gi]
                .members
                .iter()
                .any(|m| matches!(m.routing, Routing::Parent { parent, .. } if parent == *object));
            if dependents {
                return None;
            }
            let t = input.table(*object)?;
            let key = key_quality(t, &t.pk, t.size_bytes, input, s).ok()?;
            d.groups[gi].members.retain(|m| m.object != *object);
            d.groups.push(GroupDesign {
                name: t.name.clone(),
                key,
                members: vec![Member {
                    object: *object,
                    routing: Routing::Columns {
                        columns: t.pk.clone(),
                    },
                }],
            });
        }
    }
    Some(d.canonical())
}

/// Candidate moves from the 50 heaviest join edges, in a fixed order.
fn moves(input: &Input, d: &Design) -> Vec<Move> {
    let placed = d.place();
    let mut edges: BTreeMap<(Obj, String, Obj, String), f64> = BTreeMap::new();
    for s in &input.stmts {
        for j in s.joins.iter().filter(|j| j.could_colocate(input)) {
            for (x, y) in [(&j.a, &j.b), (&j.b, &j.a)] {
                *edges
                    .entry((x.0, x.1.clone(), y.0, y.1.clone()))
                    .or_default() += s.weight;
            }
        }
    }
    let mut heavy: Vec<((Obj, String, Obj, String), f64)> = edges.into_iter().collect();
    heavy.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    heavy.truncate(100);
    let mut out = Vec::new();
    for ((o, c, other, oc), _) in heavy {
        // Attach o to other's group when other holds the key in oc.
        if let Some((g, Routing::Columns { columns })) = placed.get(&other).map(|(g, r)| (*g, *r))
            && columns.len() == 1
            && columns[0] == oc
            && placed.get(&o).is_none_or(|(og, _)| *og != g)
        {
            out.push(Move::Attach {
                object: o,
                group_root: d.groups[g].members[0].object,
                column: c.clone(),
            });
        }
    }
    for g in &d.groups {
        for m in &g.members[1..] {
            if matches!(m.routing, Routing::Columns { .. }) {
                out.push(Move::Detach { object: m.object });
            }
        }
    }
    out.dedup();
    out
}

/// What a search found.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// The best design.
    pub best: Design,
    /// Its score.
    pub score: Score,
    /// The moves that led there.
    pub path: Vec<Move>,
    /// Up to 3 other designs seen, best first, with scores.
    pub alternatives: Vec<(Design, Score)>,
    /// Why objects stayed unsharded or kept other foreign keys.
    pub evidence: Vec<String>,
}

fn groups_count(d: &Design) -> usize {
    d.groups.len()
}

/// Chapter 16's `search`.
#[must_use]
pub fn search(input: &Input, s: &AdvisorSettings) -> SearchResult {
    let mut input = input.clone();
    input.normalize();
    super::kv::imply(&mut input, s);
    let (mut d, evidence) = structural(&input, s);
    let mut sc = score(&input, &d, s);
    let mut seen: Vec<(Design, Score)> = vec![(d.clone(), sc)];
    let mut path = Vec::new();
    for _ in 0..s.max_moves {
        let mut best: Option<(Design, Score, Move)> = None;
        for m in moves(&input, &d) {
            let Some(nd) = apply_move(&input, &d, &m, s) else {
                continue;
            };
            if seen.iter().any(|(x, _)| *x == nd) {
                continue;
            }
            let ns = score(&input, &nd, s);
            seen.push((nd.clone(), ns));
            let better = match &best {
                None => true,
                Some((bd, bs, _)) => {
                    ns.total > bs.total + 0.001
                        || ((ns.total - bs.total).abs() <= 0.001
                            && (ns.lookups, groups_count(&nd)) < (bs.lookups, groups_count(bd)))
                }
            };
            if better {
                best = Some((nd, ns, m));
            }
        }
        match best {
            Some((nd, ns, m)) if ns.total - sc.total >= 0.001 => {
                d = nd;
                sc = ns;
                path.push(m);
            }
            _ => break,
        }
    }
    let mut alternatives: Vec<(Design, Score)> =
        seen.into_iter().filter(|(x, _)| *x != d).collect();
    alternatives.sort_by(|a, b| b.1.total.total_cmp(&a.1.total));
    alternatives.truncate(3);
    SearchResult {
        best: d,
        score: sc,
        path,
        alternatives,
        evidence,
    }
}

/// Chapter 16's confidence before a first split: over 200 bootstrap
/// resamples, the share where the best design beats the runner-up (1.0
/// without a runner-up). Statements are aggregated per fingerprint, so a
/// resample draws each fingerprint's executions from a Poisson
/// distribution with its count as the mean (the Poisson bootstrap of the
/// executions), not the fingerprints themselves.
#[must_use]
pub fn confidence(
    input: &Input,
    best: &Design,
    runner_up: Option<&Design>,
    s: &AdvisorSettings,
) -> f64 {
    let Some(other) = runner_up else {
        return 1.0;
    };
    let mut input = input.clone();
    input.normalize();
    if input.stmts.is_empty() {
        return 0.0;
    }
    // A fixed generator: the same input gives the same confidence.
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut uniform = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        #[allow(clippy::cast_precision_loss)]
        let u = (x >> 11) as f64 / (1u64 << 53) as f64;
        u.max(f64::MIN_POSITIVE)
    };
    let mut poisson = |mean: f64| -> f64 {
        if mean > 30.0 {
            let z = (-2.0 * uniform().ln()).sqrt() * (std::f64::consts::TAU * uniform()).cos();
            (mean + mean.sqrt() * z).max(0.0)
        } else {
            let l = (-mean).exp();
            let (mut k, mut p) = (0.0, 1.0);
            loop {
                p *= uniform();
                if p <= l {
                    break k;
                }
                k += 1.0;
            }
        }
    };
    let mut wins = 0u32;
    for _ in 0..200 {
        let w: Vec<f64> = input.stmts.iter().map(|st| poisson(st.weight)).collect();
        let a = score_weighted(&input, best, s, Some(&w)).total;
        let b = score_weighted(&input, other, s, Some(&w)).total;
        if a > b {
            wins += 1;
        }
    }
    f64::from(wins) / 200.0
}

/// Why a run was started (chapter 16, `Cause`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    /// The tenant's first split is due.
    FirstSplit,
    /// `min_shards` asks for groups.
    MinShards,
    /// The periodic run (R3).
    Periodic,
    /// An operator.
    Manual,
    /// A new object (R3).
    NewObject,
}

impl Cause {
    /// The `advisor_runs.cause` text.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FirstSplit => "first_split",
            Self::MinShards => "min_shards",
            Self::Periodic => "periodic",
            Self::Manual => "manual",
            Self::NewObject => "new_object",
        }
    }
}

/// A run's verdict (chapter 16, `Verdict`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Apply the design.
    Apply,
    /// Apply, though confidence or history fell short after the longest wait.
    ApplyLowConfidence,
    /// Log the design for an operator.
    Recommend,
    /// Not enough evidence yet.
    Wait,
    /// Nothing to change.
    NoChange,
}

impl Verdict {
    /// The `advisor_runs.verdict` text.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Apply | Self::ApplyLowConfidence => "apply",
            Self::Recommend => "recommend",
            Self::Wait => "wait",
            Self::NoChange => "no_change",
        }
    }
}

/// What a decision is based on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Evidence {
    /// How much history the window holds.
    pub observed: std::time::Duration,
    /// Units sampled.
    pub units: u64,
    /// The confidence.
    pub confidence: f64,
    /// How long the tenant has waited for a first design.
    pub waited: std::time::Duration,
}

/// Chapter 16's `decide` for causes before a first split.
#[must_use]
pub fn decide(cause: Cause, e: &Evidence, s: &AdvisorSettings) -> Verdict {
    let v = match cause {
        Cause::FirstSplit | Cause::MinShards | Cause::Manual => {
            // With the advisor off, the structural design applies as is.
            if s.mode == AdvisorMode::Off
                || (e.observed >= s.min_observe
                    && e.units >= s.min_units
                    && e.confidence >= s.min_confidence)
            {
                Verdict::Apply
            } else if e.waited >= s.max_wait {
                Verdict::ApplyLowConfidence
            } else {
                Verdict::Wait
            }
        }
        Cause::Periodic | Cause::NewObject => Verdict::NoChange,
    };
    match (s.mode, v) {
        (
            AdvisorMode::Recommend | AdvisorMode::Approve,
            Verdict::Apply | Verdict::ApplyLowConfidence,
        ) => Verdict::Recommend,
        _ => v,
    }
}

#[cfg(test)]
mod tests;
