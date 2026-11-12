//! The Cypher syntax tree (chapter 43, "Language surface"; build step 6).
//! The executor runs it after [`crate::bind`] checks names and marks what
//! the planner needs, so it doubles as the graph IR of this build.

use crate::value::Value;

/// A statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// `EXPLAIN` or `PROFILE` in front.
    pub explain: Explain,
    /// The single queries joined by `UNION`.
    pub parts: Vec<SingleQuery>,
    /// For each `UNION` between parts: `true` for `UNION ALL`.
    pub union_all: Vec<bool>,
}

/// `EXPLAIN` / `PROFILE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Explain {
    /// Neither.
    None,
    /// `EXPLAIN`: the plan, nothing run.
    Explain,
    /// `PROFILE`: run, and the plan with row counts.
    Profile,
}

/// Clauses up to `RETURN` (or the last update).
#[derive(Debug, Clone, PartialEq)]
pub struct SingleQuery {
    /// The clauses in order.
    pub clauses: Vec<Clause>,
}

/// A clause.
#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    /// `[OPTIONAL] MATCH patterns [WHERE pred]`.
    Match {
        /// `OPTIONAL MATCH`.
        optional: bool,
        /// The comma-separated path patterns.
        patterns: Vec<PathPattern>,
        /// `WHERE`.
        where_: Option<Expr>,
    },
    /// `UNWIND expr AS var`.
    Unwind {
        /// The list.
        expr: Expr,
        /// The variable.
        alias: String,
    },
    /// `WITH ...`.
    With(Projection),
    /// `RETURN ...`.
    Return(Projection),
    /// `CREATE patterns`.
    Create(Vec<PathPattern>),
    /// `MERGE pattern [ON CREATE SET ...] [ON MATCH SET ...]`.
    Merge {
        /// The pattern.
        pattern: PathPattern,
        /// `ON CREATE SET`.
        on_create: Vec<SetItem>,
        /// `ON MATCH SET`.
        on_match: Vec<SetItem>,
    },
    /// `SET items`.
    Set(Vec<SetItem>),
    /// `REMOVE items`.
    Remove(Vec<RemoveItem>),
    /// `[DETACH] DELETE exprs`.
    Delete {
        /// `DETACH`.
        detach: bool,
        /// What to delete.
        exprs: Vec<Expr>,
    },
    /// `CALL { subquery }`, or `CALL (a, b) { ... }` with a scope clause:
    /// run once per incoming row.
    Subquery {
        /// The variables imported by a scope clause (`None`: the
        /// subquery's leading `WITH` imports).
        imports: Option<Vec<String>>,
        /// The subquery.
        query: Box<Query>,
        /// `IN TRANSACTIONS ...`: the incoming rows in batches, each batch's
        /// subquery committed on its own (R8 round 8).
        batch: Option<Box<TxBatch>>,
    },
    /// `FOREACH (var IN list | updates)`.
    Foreach {
        /// The variable.
        var: String,
        /// The list.
        list: Expr,
        /// The update clauses run for each element.
        body: Vec<Clause>,
    },
    /// Index and constraint commands.
    Schema(Schema),
    /// `CALL proc(args) [YIELD ...] [WHERE ...]`.
    Call {
        /// Dotted procedure name.
        name: String,
        /// Arguments; `None` for the implicit form without parentheses.
        args: Option<Vec<Expr>>,
        /// `YIELD` items (column, alias); `None` for all columns.
        yields: Option<Vec<(String, String)>>,
        /// `WHERE` after `YIELD`.
        where_: Option<Expr>,
    },
}

/// Whether an index or constraint is on nodes or relationships.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entity {
    /// `FOR (n:Label)`.
    Node,
    /// `FOR ()-[r:TYPE]-()`.
    Relationship,
}

/// Index kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// `RANGE` (the default).
    Range,
    /// `TEXT`: string values only.
    Text,
}

/// Index and constraint commands (chapter 43, "Indexes and
/// constraints").
#[derive(Debug, Clone, PartialEq)]
pub enum Schema {
    /// `CREATE INDEX` or `CREATE CONSTRAINT ... IS UNIQUE`.
    Create {
        /// The name (`None`: generated).
        name: Option<String>,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// A uniqueness constraint (with its backing index).
        unique: bool,
        /// The index kind.
        kind: IndexKind,
        /// Nodes or relationships.
        entity: Entity,
        /// The label or relationship type.
        token: String,
        /// The properties.
        props: Vec<String>,
    },
    /// `DROP INDEX name` or `DROP CONSTRAINT name`.
    Drop {
        /// `CONSTRAINT`.
        constraint: bool,
        /// The name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
}

/// The items of `WITH` or `RETURN`.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    /// `DISTINCT`.
    pub distinct: bool,
    /// `*`: every variable in scope, before `items`.
    pub star: bool,
    /// Expressions and their names.
    pub items: Vec<(Expr, String)>,
    /// `ORDER BY`: expression, ascending.
    pub order: Vec<(Expr, bool)>,
    /// `SKIP`.
    pub skip: Option<Expr>,
    /// `LIMIT`.
    pub limit: Option<Expr>,
    /// `WITH ... WHERE`.
    pub where_: Option<Expr>,
}

/// `shortestPath` and friends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    /// An ordinary pattern.
    Normal,
    /// `shortestPath(...)` or `ANY SHORTEST`.
    Shortest,
    /// `allShortestPaths(...)` or `ALL SHORTEST`.
    AllShortest,
}

/// A path pattern: `[p =] (a)-[r]->(b)...`.
#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    /// The path variable.
    pub var: Option<String>,
    /// Shortest-path kind.
    pub kind: PathKind,
    /// The first node.
    pub start: NodePat,
    /// Then each relationship and node.
    pub steps: Vec<(RelPat, NodePat)>,
}

/// A label expression (Neo4j 5): `:A:B` is `And([A, B])`.
#[derive(Debug, Clone, PartialEq)]
pub enum LabelExpr {
    /// One label.
    Name(String),
    /// `%`: any label.
    Any,
    /// `A&B`, or `:A:B`.
    And(Vec<LabelExpr>),
    /// `A|B`.
    Or(Vec<LabelExpr>),
    /// `!A`.
    Not(Box<LabelExpr>),
}

impl LabelExpr {
    /// Whether a node with `labels` matches.
    #[must_use]
    pub fn matches(&self, labels: &[String]) -> bool {
        match self {
            LabelExpr::Name(n) => labels.iter().any(|l| l == n),
            LabelExpr::Any => !labels.is_empty(),
            LabelExpr::And(v) => v.iter().all(|e| e.matches(labels)),
            LabelExpr::Or(v) => v.iter().any(|e| e.matches(labels)),
            LabelExpr::Not(e) => !e.matches(labels),
        }
    }

    /// A label every match must carry, for a label scan.
    #[must_use]
    pub fn required(&self) -> Option<&str> {
        match self {
            LabelExpr::Name(n) => Some(n),
            LabelExpr::And(v) => v.iter().find_map(LabelExpr::required),
            _ => None,
        }
    }

    /// The plain labels of `:A:B` (for `CREATE`); `None` for anything else.
    #[must_use]
    pub fn plain(&self) -> Option<Vec<String>> {
        match self {
            LabelExpr::Name(n) => Some(vec![n.clone()]),
            LabelExpr::And(v) => {
                let mut out = Vec::new();
                for e in v {
                    out.extend(e.plain()?);
                }
                Some(out)
            }
            _ => None,
        }
    }
}

/// A node pattern `(var:Labels {props})`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NodePat {
    /// The variable.
    pub var: Option<String>,
    /// Labels.
    pub labels: Option<LabelExpr>,
    /// Property map.
    pub props: Option<Expr>,
}

/// Relationship direction in a pattern, left to right.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dir {
    /// `-->`.
    Out,
    /// `<--`.
    In,
    /// `--`.
    Both,
}

impl Dir {
    /// The direction read right to left.
    #[must_use]
    pub const fn reverse(self) -> Dir {
        match self {
            Dir::Out => Dir::In,
            Dir::In => Dir::Out,
            Dir::Both => Dir::Both,
        }
    }
}

/// A relationship pattern `-[var:T1|T2 *m..n {props}]->`.
#[derive(Debug, Clone, PartialEq)]
pub struct RelPat {
    /// The variable.
    pub var: Option<String>,
    /// Type alternatives; empty for any.
    pub types: Vec<String>,
    /// Direction.
    pub dir: Dir,
    /// Property map.
    pub props: Option<Expr>,
    /// Variable length: (min, max); `None` for one hop.
    pub range: Option<(Option<u32>, Option<u32>)>,
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    /// `OR`.
    Or,
    /// `XOR`.
    Xor,
    /// `AND`.
    And,
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
    /// `=~`.
    Regex,
    /// `IN`.
    In,
    /// `STARTS WITH`.
    StartsWith,
    /// `ENDS WITH`.
    EndsWith,
    /// `CONTAINS`.
    Contains,
    /// `+`.
    Add,
    /// `-`.
    Sub,
    /// `*`.
    Mul,
    /// `/`.
    Div,
    /// `%`.
    Mod,
    /// `^`.
    Pow,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    /// `NOT`.
    Not,
    /// `-`.
    Neg,
    /// `+`.
    Plus,
}

/// An expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal.
    Lit(Value),
    /// `$name`.
    Param(String),
    /// A variable.
    Var(String),
    /// `e.key`.
    Prop(Box<Expr>, String),
    /// `e[i]`.
    Index(Box<Expr>, Box<Expr>),
    /// `e[a..b]`.
    Slice(Box<Expr>, Option<Box<Expr>>, Option<Box<Expr>>),
    /// `[a, b]`.
    List(Vec<Expr>),
    /// `{k: v}`.
    Map(Vec<(String, Expr)>),
    /// Unary.
    Unary(UnOp, Box<Expr>),
    /// Binary.
    Binary(BinOp, Box<Expr>, Box<Expr>),
    /// `e IS [NOT] NULL`: (expr, negated).
    IsNull(Box<Expr>, bool),
    /// `n:A:B` in an expression.
    HasLabels(Box<Expr>, LabelExpr),
    /// A function call.
    Func {
        /// Lower-cased dotted name.
        name: String,
        /// `DISTINCT` (aggregates).
        distinct: bool,
        /// Arguments.
        args: Vec<Expr>,
    },
    /// `count(*)`.
    CountStar,
    /// `CASE [subject] WHEN .. THEN .. [ELSE ..] END`.
    Case {
        /// The simple form's subject.
        subject: Option<Box<Expr>>,
        /// `WHEN`, `THEN` pairs.
        whens: Vec<(Expr, Expr)>,
        /// `ELSE`.
        else_: Option<Box<Expr>>,
    },
    /// `[x IN list WHERE pred | map]`.
    ListComp {
        /// The variable.
        var: String,
        /// The list.
        list: Box<Expr>,
        /// `WHERE`.
        pred: Option<Box<Expr>>,
        /// `| expr`.
        map: Option<Box<Expr>>,
    },
    /// `all|any|none|single(x IN list WHERE pred)`.
    Quantifier {
        /// `all`, `any`, `none` or `single`.
        kind: String,
        /// The variable.
        var: String,
        /// The list.
        list: Box<Expr>,
        /// The predicate.
        pred: Box<Expr>,
    },
    /// A pattern used as a predicate (`(a)-[:R]->(b)`, `exists(...)`,
    /// `EXISTS { pattern }`).
    Pattern(Box<PathPattern>),
    /// A slot the binder filled: the pattern predicate's result
    /// variable.
    Slot(String),
    /// `n {.a, .*, k: e, v}`.
    MapProjection(Box<Expr>, Vec<MapItem>),
    /// `EXISTS { ... }`, `COUNT { ... }`, `COLLECT { ... }`.
    Subquery(SubqueryKind, Box<Query>),
}

/// An entry of a map projection.
#[derive(Debug, Clone, PartialEq)]
pub enum MapItem {
    /// `.key`.
    Prop(String),
    /// `.*`.
    All,
    /// `key: expr`.
    Lit(String, Expr),
    /// `var`: `var: var`.
    Var(String),
}

/// An expression subquery's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryKind {
    /// `EXISTS { }`: whether it has a row.
    Exists,
    /// `COUNT { }`: its row count.
    Count,
    /// `COLLECT { }`: its one column as a list.
    Collect,
}

impl Expr {
    /// Visits this expression and every sub-expression.
    pub fn walk(&self, f: &mut dyn FnMut(&Expr)) {
        f(self);
        match self {
            Expr::Prop(e, _) | Expr::Unary(_, e) | Expr::IsNull(e, _) | Expr::HasLabels(e, _) => {
                e.walk(f);
            }
            Expr::Index(a, b) | Expr::Binary(_, a, b) => {
                a.walk(f);
                b.walk(f);
            }
            Expr::Slice(a, b, c) => {
                a.walk(f);
                if let Some(b) = b {
                    b.walk(f);
                }
                if let Some(c) = c {
                    c.walk(f);
                }
            }
            Expr::List(l) | Expr::Func { args: l, .. } => l.iter().for_each(|e| e.walk(f)),
            Expr::Map(m) => m.iter().for_each(|(_, e)| e.walk(f)),
            Expr::Case {
                subject,
                whens,
                else_,
            } => {
                if let Some(s) = subject {
                    s.walk(f);
                }
                for (a, b) in whens {
                    a.walk(f);
                    b.walk(f);
                }
                if let Some(e) = else_ {
                    e.walk(f);
                }
            }
            Expr::ListComp {
                list, pred, map, ..
            } => {
                list.walk(f);
                if let Some(p) = pred {
                    p.walk(f);
                }
                if let Some(m) = map {
                    m.walk(f);
                }
            }
            Expr::Quantifier { list, pred, .. } => {
                list.walk(f);
                pred.walk(f);
            }
            Expr::MapProjection(b, items) => {
                b.walk(f);
                for i in items {
                    if let MapItem::Lit(_, e) = i {
                        e.walk(f);
                    }
                }
            }
            Expr::Subquery(..)
            | Expr::Lit(_)
            | Expr::Param(_)
            | Expr::Var(_)
            | Expr::CountStar
            | Expr::Pattern(_)
            | Expr::Slot(_) => {}
        }
    }

    /// Whether an aggregate function appears (not inside a list
    /// comprehension's own scope; good enough for projections).
    #[must_use]
    pub fn has_aggregate(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| {
            if matches!(e, Expr::CountStar)
                || matches!(e, Expr::Func { name, .. } if is_aggregate(name))
            {
                found = true;
            }
        });
        found
    }
}

/// Whether `name` (lower case) is an aggregate function.
#[must_use]
pub fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect"
            | "stdev"
            | "stdevp"
            | "percentiledisc"
            | "percentilecont"
    )
}

/// A `SET` item.
#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    /// `e.key = value`.
    Prop(Expr, String, Expr),
    /// `v = map` (replace all properties).
    Replace(String, Expr),
    /// `v += map`.
    Merge(String, Expr),
    /// `v:A:B`.
    Labels(String, Vec<String>),
}

/// A `REMOVE` item.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    /// `e.key`.
    Prop(Expr, String),
    /// `v:A:B`.
    Labels(String, Vec<String>),
}

/// What `CALL { } IN TRANSACTIONS` does with a batch that fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnError {
    /// The statement fails (batches committed before stay).
    #[default]
    Fail,
    /// The batch rolls back, its rows get `null`s, the next batch runs.
    Continue,
    /// The batch rolls back, it and every later batch's rows get `null`s.
    Break,
}

/// `IN [n CONCURRENT] TRANSACTIONS [OF n ROWS] [ON ERROR ...] [REPORT
/// STATUS AS s]`.
#[derive(Debug, Clone, PartialEq)]
pub struct TxBatch {
    /// `OF n ROWS` (default 1,000).
    pub rows: Option<Expr>,
    /// `ON ERROR`.
    pub on_error: OnError,
    /// `REPORT STATUS AS s`.
    pub report: Option<String>,
    /// `IN [n] CONCURRENT`: accepted; batches run one after another.
    pub concurrent: bool,
}
