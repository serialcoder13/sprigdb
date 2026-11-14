//! Plans: what the executor runs on which shards and how it combines the
//! answers.

use std::fmt::Write as _;
use std::sync::Arc;

use sprig_shard::KeyValueOwned;

use crate::reject::Rejection;
use crate::topology::{Group, LookupRef};

/// One statement for one shard (or one cluster).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The cluster that runs it.
    pub cluster: String,
    /// The shard (0 for reference-only and unsharded statements).
    pub shard: u32,
    /// The SQL, with physical names for that shard.
    pub sql: String,
    /// Where the shard is (a transaction branch's anchor); default for reference-only statements.
    pub at: ShardAt,
}

/// A shard's place in its group, for transaction branches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardAt {
    /// Table group (0: none).
    pub group: u32,
    /// Logical database.
    pub db: u32,
    /// Start of the shard's hash range.
    pub lo: u64,
    /// The shard schema.
    pub schema: String,
}

/// A column of a shard result: one of the statement's own (visible)
/// columns, or one the router appended (hidden, counted from the first
/// hidden column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColRef {
    /// The statement's column at this position.
    Visible(usize),
    /// The router's hidden column at this position among the hidden ones.
    Hidden(usize),
}

impl ColRef {
    /// The index in a row of `ncols` columns of which `hidden` are hidden.
    #[must_use]
    pub fn index(self, ncols: usize, hidden: usize) -> usize {
        match self {
            Self::Visible(i) => i,
            Self::Hidden(j) => ncols - hidden + j,
        }
    }
}

/// A value and, for strings, its weights at levels 1 to 3 and its
/// collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyCols {
    /// The value.
    pub value: ColRef,
    /// `WEIGHT_STRING(e LEVEL 1..3)`, then `COLLATION(e)`: column
    /// definitions carry the connection's collation, not the expression's.
    pub weights: [ColRef; 4],
}

/// One `ORDER BY` item of an ordered merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    /// Value and weights.
    pub cols: KeyCols,
    /// `DESC`.
    pub desc: bool,
}

/// How a multi-shard read combines its shards' rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadKind {
    /// Concatenate.
    Scatter,
    /// Merge on sort keys.
    Merge,
    /// Combine partial aggregates by group.
    Aggregate,
}

impl ReadKind {
    /// Snake-case name for metrics and EXPLAIN.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scatter => "scatter",
            Self::Merge => "merge",
            Self::Aggregate => "aggregate",
        }
    }
}

/// An aggregate function the gateway finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    /// `COUNT(*)`, `COUNT(e)`: sum of counts.
    Count,
    /// `SUM(e)`.
    Sum,
    /// `MIN(e)`.
    Min,
    /// `MAX(e)`.
    Max,
    /// `AVG(e)`: from hidden `SUM(e)` and `COUNT(e)`.
    Avg,
}

/// One aggregate call: where its partials are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggCall {
    /// The function.
    pub func: AggFunc,
    /// The partial (COUNT, SUM, MIN, MAX: the call itself; AVG: hidden
    /// `SUM`).
    pub value: ColRef,
    /// AVG's hidden `COUNT`.
    pub count: Option<ColRef>,
    /// MIN and MAX: weights and collation of the partial, for strings.
    pub weights: Option<[ColRef; 4]>,
    /// AVG: the visible column whose definition gives the result scale.
    pub scale_from: Option<ColRef>,
}

/// What a visible column of an aggregate plan holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutCol {
    /// The value of the group's first row (group expressions, constants).
    First,
    /// The final value of aggregate `i`.
    Agg(usize),
}

/// An operand of the gateway's HAVING evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operand {
    /// Aggregate `i`'s final value.
    Agg(usize),
    /// A column of the group's first row.
    Col(ColRef),
    /// A numeric literal.
    Num(String),
    /// `NULL`.
    Null,
}

/// The gateway's HAVING expressions (numbers only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pred {
    /// `a AND b`.
    And(Box<Pred>, Box<Pred>),
    /// `a OR b`.
    Or(Box<Pred>, Box<Pred>),
    /// `NOT a`.
    Not(Box<Pred>),
    /// A comparison: `=`, `<>`, `<`, `<=`, `>`, `>=`.
    Cmp(CmpOp, Operand, Operand),
    /// `IS [NOT] NULL`.
    IsNull(Operand, bool),
}

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `=`.
    Eq,
    /// `<>`.
    Ne,
    /// `<`.
    Lt,
    /// `<=`.
    Le,
    /// `>`.
    Gt,
    /// `>=`.
    Ge,
}

/// Where a final sort key of an aggregate plan comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalSort {
    /// A group key (by its position in [`AggPlan::groups`]).
    Group(usize, bool),
    /// An aggregate's final value.
    Agg(usize, bool),
}

/// Split aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggPlan {
    /// Group keys; empty for a scalar aggregate.
    pub groups: Vec<KeyCols>,
    /// Aggregate calls.
    pub aggs: Vec<AggCall>,
    /// Per visible column.
    pub outputs: Vec<OutCol>,
    /// HAVING.
    pub having: Option<Pred>,
    /// ORDER BY over final values; empty: by group keys, as MariaDB's
    /// `GROUP BY` sorts.
    pub order: Vec<FinalSort>,
}

/// A read over several shards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlan {
    /// How shards' rows combine.
    pub kind: ReadKind,
    /// One statement per shard.
    pub targets: Vec<Target>,
    /// Hidden columns appended to each statement's select list.
    pub hidden: usize,
    /// Merge order.
    pub order: Vec<SortKey>,
    /// Aggregation.
    pub agg: Option<AggPlan>,
    /// Rows skipped in the gateway.
    pub offset: u64,
    /// Rows returned, after the offset.
    pub limit: Option<u64>,
    /// The `LIMIT` each shard got, for EXPLAIN.
    pub pushed_limit: Option<u64>,
    /// The group, for EXPLAIN.
    pub group: u32,
}

/// Where an output column of a lookup join comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinCol {
    /// The outer side's visible column.
    Outer(usize),
    /// The inner side's visible column.
    Inner(usize),
}

/// A batched lookup join between two groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupJoinPlan {
    /// The outer side, a read with its own order.
    pub outer: ReadPlan,
    /// The outer join column `x` and its weights.
    pub outer_key: KeyCols,
    /// The inner table's group.
    pub inner_group: Arc<Group>,
    /// The inner join column is the inner table's key: keys go to their
    /// shards; else every key to every shard.
    pub inner_by_key: bool,
    /// The inner statement, with `\u{1}S` for the shard schema and
    /// `\u{1}K` for the `IN` list.
    pub inner_template: String,
    /// Hidden columns of the inner statement (`y` and its weights).
    pub inner_hidden: usize,
    /// The inner join column `y` and its weights.
    pub inner_key: KeyCols,
    /// `LEFT JOIN`: outer rows without a match get NULLs.
    pub left: bool,
    /// The statement's columns.
    pub outputs: Vec<JoinCol>,
    /// Rows skipped after the join.
    pub offset: u64,
    /// Rows returned after the join.
    pub limit: Option<u64>,
    /// Keys per `IN` list.
    pub batch: usize,
}

/// What a write's OK packet sums.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    /// `INSERT`: `Records: r  Duplicates: d  Warnings: w`.
    Insert,
    /// `UPDATE`: `Rows matched: m  Changed: c  Warnings: w`.
    Update,
    /// `DELETE`: no info.
    Delete,
}

/// A write over several shards of one cluster, in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePlan {
    /// Statement kind.
    pub kind: WriteKind,
    /// One statement per shard, in the order rows were written.
    pub targets: Vec<Target>,
    /// Rows the client's statement listed (multi-row `INSERT` info).
    pub rows: u64,
    /// Unique lookup values the inserted rows claim, in `lookup_id`
    /// order, before the rows are written (`claim`).
    pub claims: Vec<Claim>,
    /// Lookups whose values the deleted rows release (`release`), read
    /// before the delete.
    pub releases: Vec<Release>,
    /// Foreign keys across groups: parents locked before
    /// child rows are written, children probed (or their actions applied)
    /// before parent rows change.
    pub fk: Option<Box<crate::fk::FkWork>>,
    /// Limited DML (`ORDER BY … LIMIT n` over several shards): the
    /// rows are read in order and locked first, then `targets` (one per
    /// shard, with [`LIMITED_ROWS`] where the rows' condition goes) run on
    /// the shards that hold them.
    pub limited: Option<Box<Limited>>,
}

/// Where a limited write's per-shard statement takes the condition naming
/// its rows.
pub const LIMITED_ROWS: &str = "__sprig_limited_rows__";

/// The read of a limited write: `SELECT <key>, <primary key> FROM … WHERE
/// … ORDER BY … LIMIT n FOR UPDATE`, planned as a merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limited {
    /// The read.
    pub read: Box<Plan>,
    /// The primary-key columns, lowercase.
    pub pk: Vec<String>,
    /// How the key and then each primary-key column read back.
    pub classes: Vec<crate::fk::ValClass>,
    /// The table's group (rows go to their key's shard).
    pub group: Arc<Group>,
}

/// One value an inserted row claims in a unique lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    /// The lookup.
    pub lookup: Arc<LookupRef>,
    /// The value (one part per value column).
    pub value: Vec<KeyValueOwned>,
    /// The row's primary key.
    pub owner_pk: Vec<KeyValueOwned>,
    /// The row's routing hash.
    pub owner_rk: u64,
}

// Key values hold no NaN: literals only.
impl Eq for Claim {}

/// A lookup whose values a `DELETE` releases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    /// The lookup.
    pub lookup: Arc<LookupRef>,
    /// Per target shard, `SELECT <value>, <primary key> ... FOR UPDATE`
    /// of the rows the delete removes; strings and binary values come
    /// hex-encoded (`HEX(CONVERT(c USING utf8mb4))`), so the session's
    /// character set doesn't matter.
    pub reads: Vec<Target>,
}

/// The values a statement's equality on a lookup column names.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupProbe {
    /// The lookup.
    pub lookup: Arc<LookupRef>,
    /// The values (`c = v`, `c IN (v, ...)`), one part each.
    pub values: Vec<Vec<KeyValueOwned>>,
}

impl Eq for LookupProbe {}

/// A plan over every shard that a lookup narrows: the executor reads the owners' routing hashes and
/// keeps the targets on their shards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViaLookup {
    /// What to look up.
    pub probe: LookupProbe,
    /// The table's group, to route the hashes.
    pub group: Arc<Group>,
    /// The plan over every shard: a read or a write.
    pub plan: Plan,
}

impl ViaLookup {
    /// The plan narrowed to the shards owning `rks`. With no owner (no row
    /// has the value) one shard still runs, so the answer has its
    /// columns and counts.
    #[must_use]
    pub fn narrow(&self, rks: &[u64]) -> Plan {
        let shards: std::collections::BTreeSet<u32> = rks
            .iter()
            .filter_map(|h| self.group.shards.get(self.group.route(*h)))
            .map(|s| s.shard)
            .collect();
        let keep = |ts: &[Target]| -> Vec<Target> {
            let mut out: Vec<Target> = ts
                .iter()
                .filter(|t| shards.contains(&t.shard))
                .cloned()
                .collect();
            if out.is_empty() {
                out.extend(ts.first().cloned());
            }
            out
        };
        let mut p = self.plan.clone();
        match &mut p {
            Plan::Read(r) => r.targets = keep(&r.targets),
            Plan::Write(w) => {
                w.targets = keep(&w.targets);
                for r in &mut w.releases {
                    r.reads = keep(&r.reads);
                }
            }
            _ => {}
        }
        p
    }
}

/// An `INSERT` into a parent member over several shards (`rk_for_insert`):
/// the executor finds
/// each row's parent (through the parent's unique lookup, or by asking
/// every shard), then [`ViaParent::finish`] sends each row to its
/// parent's shard with the parent's routing hash in `_sprig_rk`.
#[derive(Debug, Clone, PartialEq)]
pub struct ViaParent {
    /// The `INSERT`, its column list filled in when `_sprig_rk` is added.
    pub stmt: sqlparser::ast::Statement,
    /// The child table, lowercase.
    pub table: String,
    /// The default schema its name resolved under.
    pub default_schema: String,
    /// The parent table, lowercase.
    pub parent: String,
    /// The group both live in.
    pub group: Arc<Group>,
    /// One per distinct parent key: the condition on the parent's
    /// referenced columns (`` `iid` = 7 ``).
    pub probes: Vec<String>,
    /// One per distinct parent key, when the parent has a public unique
    /// lookup on its one referenced column: the value to look up.
    pub lookup: Option<(Arc<LookupRef>, Vec<KeyValueOwned>)>,
    /// Per row of the statement, in order: its probe.
    pub row_probe: Vec<usize>,
    /// Rows get `_sprig_rk` (the member is `filling` or later).
    pub fill_rk: bool,
    /// Probes per lookup statement (`router.lookup_batch_size`).
    pub batch: usize,
}

// Key values hold no NaN: literals only.
impl Eq for ViaParent {}

/// Where one parent key was found: per shard index, the routing hash its
/// row holds (`None`: `_sprig_rk` wasn't read or is `NULL`).
pub type ParentHits = Vec<(usize, Option<u64>)>;

impl ViaParent {
    /// The statement that reads probes `probes` from the parent's copy in
    /// shard schema `schema`: one row per hit, `(probe, _sprig_rk)`.
    #[must_use]
    pub fn lookup_sql(&self, schema: &str, probes: std::ops::Range<usize>) -> String {
        let q = |s: &str| format!("`{}`", s.replace('`', "``"));
        let rk = if self.fill_rk {
            q(sprig_shard::rk::RK_COLUMN)
        } else {
            "NULL".to_owned()
        };
        let mut out = String::new();
        for i in probes {
            if !out.is_empty() {
                out.push_str(" UNION ALL ");
            }
            let _ = write!(
                out,
                "SELECT {i}, {rk} FROM {}.{} WHERE {}",
                q(schema),
                q(&self.parent),
                self.probes.get(i).map_or("FALSE", String::as_str)
            );
        }
        out
    }
}

/// An `INSERT` into a table whose `AUTO_INCREMENT` column takes the
/// gateway's ID blocks: the gateway takes `rows.len()` values from its block for
/// the sequence (and raises it past `explicit_max`), then
/// [`IdFill::finish`] writes them into the rows and plans the statement.
#[derive(Debug, Clone, PartialEq)]
pub struct IdFill {
    /// The `INSERT`, its column list (or `SET` list) holding the column,
    /// added with `NULL` values when the client left it out.
    pub stmt: sqlparser::ast::Statement,
    /// The `sequences` row (`seq_id`, the table's object id).
    pub seq: u64,
    /// The table, as the statement names it.
    pub table: String,
    /// The `AUTO_INCREMENT` column, lowercase.
    pub column: String,
    /// The table's group: where `MAX(column)` is read when the sequence
    /// is created.
    pub group: Arc<Group>,
    /// The rows that take a generated value, in statement order (`[0]`
    /// for `INSERT … SET`).
    pub rows: Vec<usize>,
    /// The column's position in each row of `VALUES` (`None`: `SET`).
    pub pos: Option<usize>,
    /// The highest explicit value the statement writes, if positive.
    pub explicit_max: Option<i128>,
}

// Expressions hold no NaN: the statement as parsed.
impl Eq for IdFill {}

/// A plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// The statement touches no table the router knows: the caller runs
    /// it as before.
    NotRouted,
    /// One statement on one cluster, relayed.
    Pass(Target),
    /// A read over several shards.
    Read(ReadPlan),
    /// A lookup join.
    LookupJoin(Box<LookupJoinPlan>),
    /// A write over several shards.
    Write(WritePlan),
    /// A read or write narrowed by a unique lookup.
    ViaLookup(Box<ViaLookup>),
    /// An `INSERT` into a parent member over several shards.
    ViaParent(Box<ViaParent>),
    /// An `INSERT` that needs `AUTO_INCREMENT` values from the gateway's
    /// ID blocks before it can be planned.
    NeedIds(Box<IdFill>),
    /// `EXPLAIN FORMAT=SPRIG`: the plan of the statement.
    Explain(Box<Plan>),
    /// A gathered read: the rows copied to the session's cluster and
    /// the statement run there over the copies.
    Gather(Box<crate::gather::GatherPlan>),
    /// A statement with `GRAPH_TABLE` (SQL/PGQ): the graph
    /// calls run first in the gateway, then the statement with their rows
    /// is planned again ([`crate::graph`]).
    Graph(Box<crate::graph::GraphPlan>),
    /// No exact plan.
    Reject(Rejection),
}

impl Plan {
    /// Snake-case kind for metrics (`sprig_router_statements_total{plan}`).
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NotRouted => "not_routed",
            Self::Pass(_) => "pass",
            Self::Read(r) => r.kind.as_str(),
            Self::LookupJoin(_) => "lookup_join",
            Self::Write(_) => "write",
            Self::ViaLookup(_) => "via_lookup",
            Self::ViaParent(_) => "via_parent",
            Self::NeedIds(_) => "need_ids",
            Self::Explain(_) => "explain",
            Self::Gather(_) => "gather",
            Self::Graph(_) => "graph",
            Self::Reject(_) => "reject",
        }
    }
}
