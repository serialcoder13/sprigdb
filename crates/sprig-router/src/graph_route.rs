//! Graph hops as router routes (chapter 43's `Step::Shard(RouteSpec)`,
//! chapter 17's `RouteSpec`; R8 round 8, decision G69).
//!
//! The native graph store ([`sprig-graph`'s `MariaStore`]) no longer
//! decides which shards a hop reads or writes the statements itself: each
//! read step is a [`RouteSpec`] (the graph's table group from the router's
//! map cache, what the step reads, and how its targets are chosen), and
//! [`plan_route`] turns it into the router's own [`Plan`]:
//!
//! ```text
//! RouteSpec { group, graph, op, targets }
//!   targets ByKey(node ids)    → each id hashed with the group's key spec (part = id),
//!                                routed to its shard; each shard's statement names only
//!                                its own ids (narrowing)
//!   targets ByHash(hash, id)   → ids whose shard a hash already names (an edge's source)
//!   targets All(ids)           → every shard, each with every id (a scatter: incoming
//!                                hops without fresh mirrors, edges by id)
//!   targets Shard(i)           → one shard (a recursive CTE burst on the frontier's shard)
//! plan_route(spec) → Route { plan: Pass(target) | Read(Scatter, targets), held_until_ms }
//!   each Target: cluster, shard id, the statement with that shard's physical names,
//!   ShardAt (group, db, lo, schema) for chapter 18's branches
//! ```
//!
//! `GRAPH_TABLE` statements and Bolt statements therefore share one
//! planner: the calls' hops are routes, and the outer statement is planned
//! by [`crate::planner::plan_graph`]. A route over a range a mover cutover
//! holds reports the hold's end ([`Route::held_until_ms`]); the gateway
//! waits for it before an auto-commit read, as it does for SQL. Values are
//! written into the text: integers as digits, strings as `_utf8mb4 X'…'`
//! (no escaping, whatever the session's `sql_mode`).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use crate::plan::{Plan, ReadKind, ReadPlan, Target};
use crate::topology::{Group, KeyValue};

/// The node columns every node read returns, in the store's order.
pub const NODE_COLS: &str = "n.id, n._sprig_rk, n.labels, n.props, n._types";
/// The edge columns every edge read returns, in the store's order.
pub const EDGE_COLS: &str = "e.src, e.type, e.dst, e.id, e.dst_rk, e._sprig_rk, e.props, e._types";
/// Edge ids per statement after a mirror read: larger lists split into
/// several targets on the same shard.
pub const IDS_PER_STATEMENT: usize = 1000;

/// One of a native graph's tables in a shard schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphTable {
    /// `__sprig_gn_<graph>`.
    Nodes,
    /// `__sprig_ge_<graph>`.
    Edges,
    /// `__sprig_gl_<graph>`.
    Labels,
    /// `__sprig_gm_<graph>`.
    Mirrors,
}

/// The quoted, qualified name of `t` of graph `graph` in `schema` (the
/// names `sprig-graph`'s `Tables::of` gives).
#[must_use]
pub fn table(schema: &str, graph: u64, t: GraphTable) -> String {
    let p = match t {
        GraphTable::Nodes => "gn",
        GraphTable::Edges => "ge",
        GraphTable::Labels => "gl",
        GraphTable::Mirrors => "gm",
    };
    format!("`{}`.`__sprig_{p}_{graph}`", schema.replace('`', "``"))
}

/// A value written into a route's statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lit {
    /// An integer.
    Int(i64),
    /// A string: `_utf8mb4 X'…'`.
    Str(String),
}

impl Lit {
    /// The SQL literal.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Self::Int(i) => i.to_string(),
            Self::Str(s) => {
                let mut o = String::with_capacity(s.len() * 2 + 12);
                o.push_str("_utf8mb4 X'");
                for b in s.as_bytes() {
                    let _ = write!(o, "{b:02X}");
                }
                o.push('\'');
                o
            }
        }
    }
}

/// How a route's targets are chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Targets {
    /// Node ids, each to the shard of its routing hash (`part` = `id`).
    ByKey(Vec<u64>),
    /// Ids to the shard a known routing hash names: `(hash, id)`.
    ByHash(Vec<(u64, u64)>),
    /// Every shard, each with every id (none: a scan of every shard).
    All(Vec<u64>),
    /// One shard, by its index in the group.
    Shard(usize),
}

/// A node scan's conditions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanOp {
    /// Only nodes with this label (the label table).
    pub label: Option<u32>,
    /// An online index on the label and a pushed-down property: its column
    /// and the values' index texts (it replaces the label join).
    pub index: Option<(String, Vec<String>)>,
    /// `JSON_VALUE(n.props, '$."k"') IN (...)`: a superset, rechecked.
    pub props: Vec<(String, Vec<Lit>)>,
    /// Paging: ids above this one, in id order.
    pub after: Option<u64>,
    /// At most this many rows per shard.
    pub limit: Option<usize>,
}

/// What a route reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HopOp {
    /// Nodes by id.
    Nodes,
    /// Nodes by id, locked (`FOR UPDATE` or `LOCK IN SHARE MODE`).
    LockNodes {
        /// `FOR UPDATE`.
        exclusive: bool,
    },
    /// A node scan (the start of a pattern).
    Scan(ScanOp),
    /// One hop: edges whose near end is one of the ids, the far node
    /// left-joined when it is on the same shard. Outgoing reads the
    /// sources' shards by key; incoming without mirrors scatters the `rev`
    /// key.
    Expand {
        /// Outgoing (`src` is near).
        out: bool,
        /// Type ids (empty: any).
        types: Vec<u32>,
    },
    /// Mirror rows `(id, src_rk)` of edges into the ids, on the
    /// destinations' shards.
    Mirrors {
        /// Type ids (empty: any).
        types: Vec<u32>,
    },
    /// Edges by id with their source node, on the sources' shards.
    EdgesWithSrc,
    /// Edges by id.
    Edges,
    /// Edges touching the ids: outgoing by key or incoming by scatter, at
    /// most `limit` per statement.
    Touching {
        /// Outgoing.
        out: bool,
        /// The `LIMIT`.
        limit: usize,
    },
    /// One shard's recursive CTE from `seeds` (origin, node).
    Reach {
        /// The seeds.
        seeds: Vec<(u64, u64)>,
        /// Type ids (empty: any).
        types: Vec<u32>,
        /// Outgoing.
        out: bool,
        /// Up to this many hops.
        hops: u32,
    },
}

impl HopOp {
    /// Snake-case name for metrics and EXPLAIN.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::LockNodes { .. } => "lock_nodes",
            Self::Scan(_) => "scan",
            Self::Expand { out: true, .. } => "expand_out",
            Self::Expand { out: false, .. } => "expand_in",
            Self::Mirrors { .. } => "mirrors",
            Self::EdgesWithSrc => "edges_with_src",
            Self::Edges => "edges",
            Self::Touching { .. } => "touching",
            Self::Reach { .. } => "reach",
        }
    }
}

/// A graph step for the router to plan (chapter 17's `RouteSpec`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSpec {
    /// The graph's table group as the router's map cache has it.
    pub group: Arc<Group>,
    /// The graph id (its tables' names).
    pub graph: u64,
    /// What it reads.
    pub op: HopOp,
    /// How the targets are chosen.
    pub targets: Targets,
}

/// A planned route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The step, for metrics and EXPLAIN.
    pub op: &'static str,
    /// `Pass` (one target), or a `Scatter` read (none or several).
    pub plan: Plan,
    /// While a mover cutover holds a range a target reads: the hold's end
    /// (ms since the epoch).
    pub held_until_ms: Option<u64>,
}

impl Route {
    /// The statements, in order.
    #[must_use]
    pub fn targets(&self) -> &[Target] {
        match &self.plan {
            Plan::Pass(t) => std::slice::from_ref(t),
            Plan::Read(r) => &r.targets,
            _ => &[],
        }
    }

    /// One EXPLAIN line: the step, the plan kind and its shards.
    #[must_use]
    pub fn line(&self) -> String {
        let mut shards: Vec<String> = Vec::new();
        for t in self.targets() {
            let s = format!("s{}@{}", t.shard, t.cluster);
            if !shards.contains(&s) {
                shards.push(s);
            }
        }
        format!(
            "GraphRoute {}: {} over {} statement(s) on {}",
            self.op,
            self.plan.kind(),
            self.targets().len(),
            if shards.is_empty() {
                "no shard".to_owned()
            } else {
                shards.join(", ")
            }
        )
    }
}

fn ids_list(ids: &[u64]) -> String {
    let mut s = String::with_capacity(ids.len() * 8);
    for (i, x) in ids.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{x}");
    }
    s
}

fn types_filter(alias: &str, types: &[u32]) -> String {
    if types.is_empty() {
        return String::new();
    }
    let t: Vec<u64> = types.iter().map(|x| u64::from(*x)).collect();
    format!(" AND {alias}.type IN ({})", ids_list(&t))
}

/// The routing hash of node id `id` in `group` (chapter 15's numeric
/// class; `part` = `id`).
#[must_use]
pub fn node_hash(group: &Group, id: u64) -> Option<u64> {
    group.hash_of(&KeyValue::Int(i128::from(id)))
}

/// Shard `i`'s statement for `op` over `ids`; `None` when it can match
/// nothing.
fn statement(spec: &RouteSpec, i: usize, ids: &[u64]) -> Option<String> {
    let g = &spec.group;
    let schema = &g.shards.get(i)?.schema;
    let t = |x| table(schema, spec.graph, x);
    let nodes = t(GraphTable::Nodes);
    let edges = t(GraphTable::Edges);
    if ids.is_empty() && !matches!(spec.op, HopOp::Scan(_) | HopOp::Reach { .. }) {
        return None;
    }
    let id_list = ids_list(ids);
    Some(match &spec.op {
        HopOp::Nodes => format!("SELECT {NODE_COLS} FROM {nodes} n WHERE n.id IN ({id_list})"),
        HopOp::LockNodes { exclusive } => format!(
            "SELECT {NODE_COLS} FROM {nodes} n WHERE n.id IN ({id_list}) {}",
            if *exclusive {
                "FOR UPDATE"
            } else {
                "LOCK IN SHARE MODE"
            }
        ),
        HopOp::Scan(s) => {
            let mut sql = format!("SELECT {NODE_COLS} FROM {nodes} n");
            if let Some((col, texts)) = &s.index {
                if texts.is_empty() {
                    return None;
                }
                let vals: Vec<String> = texts.iter().map(|x| Lit::Str(x.clone()).sql()).collect();
                let _ = write!(
                    sql,
                    " WHERE 1 = 1 AND n.`{}` IN ({})",
                    col.replace('`', "``"),
                    vals.join(", ")
                );
            } else if let Some(l) = s.label {
                let _ = write!(
                    sql,
                    " JOIN {} l ON l.id = n.id AND l.label = {l} WHERE 1 = 1",
                    t(GraphTable::Labels)
                );
            } else {
                sql.push_str(" WHERE 1 = 1");
            }
            if matches!(spec.targets, Targets::ByKey(_) | Targets::ByHash(_)) || !ids.is_empty() {
                if ids.is_empty() {
                    return None;
                }
                let _ = write!(sql, " AND n.id IN ({id_list})");
            }
            for (k, vals) in &s.props {
                if vals.is_empty() || k.contains(['"', '\\', '\'']) {
                    continue;
                }
                let v: Vec<String> = vals.iter().map(Lit::sql).collect();
                let _ = write!(
                    sql,
                    " AND JSON_VALUE(n.props, '$.\"{k}\"') IN ({})",
                    v.join(", ")
                );
            }
            if let Some(a) = s.after {
                let _ = write!(sql, " AND n.id > {a} ORDER BY n.id");
            }
            if let Some(l) = s.limit {
                let _ = write!(sql, " LIMIT {l}");
            }
            sql
        }
        HopOp::Expand { out, types } => {
            let (near, far) = if *out { ("src", "dst") } else { ("dst", "src") };
            format!(
                "SELECT {EDGE_COLS}, {NODE_COLS} FROM {edges} e LEFT JOIN {nodes} n ON n.id = e.{far} \
                 WHERE e.{near} IN ({id_list}){}",
                types_filter("e", types)
            )
        }
        HopOp::Mirrors { types } => format!(
            "SELECT m.id, m.src_rk FROM {} m WHERE m.dst IN ({id_list}){}",
            t(GraphTable::Mirrors),
            types_filter("m", types)
        ),
        HopOp::EdgesWithSrc => format!(
            "SELECT {EDGE_COLS}, {NODE_COLS} FROM {edges} e LEFT JOIN {nodes} n ON n.id = e.src \
             WHERE e.id IN ({id_list})"
        ),
        HopOp::Edges => format!("SELECT {EDGE_COLS} FROM {edges} e WHERE e.id IN ({id_list})"),
        HopOp::Touching { out, limit } => format!(
            "SELECT {EDGE_COLS} FROM {edges} e WHERE e.{} IN ({id_list}) LIMIT {limit}",
            if *out { "src" } else { "dst" }
        ),
        HopOp::Reach {
            seeds,
            types,
            out,
            hops,
        } => {
            if seeds.is_empty() || *hops == 0 {
                return None;
            }
            let many = g.shards.len() > 1;
            let (tbl, near, far, far_rk) = match (out, many) {
                (true, _) => (edges.clone(), "src", "dst", "dst_rk"),
                (false, true) => (t(GraphTable::Mirrors), "dst", "src", "src_rk"),
                (false, false) => (edges.clone(), "dst", "src", "_sprig_rk"),
            };
            let mut seed_sql = String::new();
            for (k, (o, n)) in seeds.iter().enumerate() {
                if k > 0 {
                    seed_sql.push_str(" UNION ALL ");
                }
                let _ = write!(seed_sql, "SELECT {o} AS o, {n} AS n");
            }
            let tf = types_filter("e", types);
            // Only nodes inside this shard's range continue: the others
            // are boundary nodes for the gateway's frontier.
            let mut range = String::new();
            if many {
                let _ = write!(range, " AND r.rk >= {}", g.shards[i].lo);
                if let Some(next) = g.shards.get(i + 1) {
                    let _ = write!(range, " AND r.rk < {}", next.lo);
                }
            }
            format!(
                "SET STATEMENT max_recursive_iterations = {iters} FOR \
                 WITH RECURSIVE r (o, id, rk, d) AS ( \
                   SELECT s.o, e.{far}, e.{far_rk}, 1 FROM ({seed_sql}) s \
                     JOIN {tbl} e ON e.{near} = s.n{tf} \
                   UNION \
                   SELECT r.o, e.{far}, e.{far_rk}, r.d + 1 FROM r \
                     JOIN {tbl} e ON e.{near} = r.id{tf} \
                    WHERE r.d < {hops}{range} \
                 ) CYCLE o, id RESTRICT \
                 SELECT o, id, rk, MIN(d) FROM r GROUP BY o, id, rk",
                iters = hops + 1,
            )
        }
    })
}

/// Plans `spec`: the router's targets for the step, narrowed to the shards
/// that can hold its rows.
#[must_use]
pub fn plan_route(spec: &RouteSpec) -> Route {
    let g = &spec.group;
    // Per shard index, the ids its statement names.
    let mut per: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    match &spec.targets {
        Targets::ByKey(ids) => {
            for &id in ids {
                // A graph's key is an integer: every id hashes.
                let i = node_hash(g, id).map_or(0, |h| g.route(h));
                per.entry(i).or_default().push(id);
            }
        }
        Targets::ByHash(v) => {
            for &(h, id) in v {
                per.entry(g.route(h)).or_default().push(id);
            }
        }
        Targets::All(ids) => {
            for i in 0..g.shards.len() {
                per.insert(i, ids.clone());
            }
        }
        Targets::Shard(i) => {
            if *i < g.shards.len() {
                per.insert(*i, Vec::new());
            }
        }
    }
    // Edges by id after the mirrors come in batches (the mirrors of a
    // whole frontier); every other step takes the executor's batch as is.
    let chunked = matches!(spec.op, HopOp::EdgesWithSrc);
    let mut targets: Vec<Target> = Vec::new();
    let mut held: Option<u64> = None;
    for (i, ids) in per {
        let Some(loc) = g.shards.get(i) else { continue };
        let chunks: Vec<&[u64]> = if chunked && ids.len() > IDS_PER_STATEMENT {
            ids.chunks(IDS_PER_STATEMENT).collect()
        } else {
            vec![ids.as_slice()]
        };
        let mut any = false;
        for c in chunks {
            let Some(sql) = statement(spec, i, c) else {
                continue;
            };
            any = true;
            targets.push(Target {
                cluster: loc.cluster.clone(),
                shard: loc.shard,
                sql,
                at: g.at(i),
            });
        }
        if any && let Some(u) = loc.held_until_ms {
            held = Some(held.map_or(u, |h| h.max(u)));
        }
    }
    let plan = if targets.len() == 1 {
        Plan::Pass(targets.remove(0))
    } else {
        Plan::Read(ReadPlan {
            kind: ReadKind::Scatter,
            targets,
            hidden: 0,
            order: Vec::new(),
            agg: None,
            offset: 0,
            limit: None,
            pushed_limit: None,
            group: g.id,
        })
    };
    let op = spec.op.name();
    metrics::counter!("sprig_router_graph_routes_total", "op" => op, "plan" => plan.kind())
        .increment(1);
    Route {
        op,
        plan,
        held_until_ms: held,
    }
}

/// EXPLAIN's lines for a graph's routes: its group and shards as the
/// router's map cache has them, and how each kind of step picks its
/// shards.
#[must_use]
pub fn explain_lines(group: &Group, async_mirrors: bool) -> Vec<String> {
    let mut out = vec![format!(
        "GraphRoutes: group {} (map version {}), {} shard(s), each step planned by the router",
        group.id,
        group.version,
        group.shards.len()
    )];
    for s in &group.shards {
        out.push(format!(
            "  GraphShard: s{} {} {} lo {} epoch {}{}",
            s.shard,
            s.cluster,
            s.schema,
            s.lo,
            s.epoch,
            if s.held_until_ms.is_some() {
                " (held)"
            } else {
                ""
            }
        ));
    }
    if group.shards.len() > 1 {
        out.push("  Route scan: every shard (scatter); by id: the ids' shards (by key)".into());
        out.push("  Route expand_out: the frontier's shards (by key), far node joined".into());
        out.push(format!(
            "  Route expand_in: mirrors on the frontier's shards (by key), then edges on their sources' shards (by hash){}; without them every shard (scatter)",
            if async_mirrors {
                " when the mirrors are fresh"
            } else {
                ""
            }
        ));
        out.push("  Route reach: a recursive CTE on each frontier shard (pass)".into());
    } else {
        out.push("  Route: every step on the one shard (pass)".into());
    }
    out
}

#[cfg(test)]
mod tests;
