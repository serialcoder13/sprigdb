//! The built-in tools.
//!
//! | Tool | Roles | Effect | Shares |
//! |---|---|---|---|
//! | `search_docs` | admin, dba, data | read | schema |
//! | `catalog`, `describe` | dba, data | read | schema |
//! | `workload`, `fingerprint` | dba | read | stats |
//! | `explain` | dba, data | read | stats |
//! | `indexes`, `auto_indexes` | dba | read | schema |
//! | `settings` | dba, admin | read | schema |
//! | `shards`, `advisor_runs` | dba | read | stats |
//! | `approvals` | admin, dba, data | read | schema |
//! | `column_stats` | dba, data | read | stats |
//! | `run_query` | data | read | rows |
//! | `test_policy` | dba | read | stats |
//! | `compile_logic` | dba | read | schema |
//! | `decision_log` | admin, dba | read | stats |
//! | `metrics_trend` | admin | read | stats |
//! | `propose_sql` | dba, data | propose | |
//! | `propose_recommendation` | dba, admin | propose | |
//! | `propose_tile` | data, dba | propose | |
//! | `propose_glossary` | data, dba | propose | |
//! | `generate_rows` | dba | propose | |
//!
//! The triage role has no tools.

use serde_json::{Map, Value, json};

use super::{Effect, ProposalKind, Shares, ToolSpec};
use crate::role::Role;
use crate::sql_guard;

const ALL: &[Role] = &[Role::Admin, Role::Dba, Role::Data];
const DBA_DATA: &[Role] = &[Role::Dba, Role::Data];
const DBA: &[Role] = &[Role::Dba];
const DATA: &[Role] = &[Role::Data];
const DBA_ADMIN: &[Role] = &[Role::Dba, Role::Admin];
const ADMIN: &[Role] = &[Role::Admin];

/// The longest SQL or source text a tool takes.
pub const MAX_TEXT: usize = 64 * 1024;

/// The chart marks a tile may use.
pub const MARKS: &[&str] = &[
    "bar",
    "line",
    "area",
    "point",
    "pie",
    "donut",
    "waterfall",
    "histogram",
    "box",
    "heatmap",
    "highlight-table",
    "treemap",
    "funnel",
    "radar",
    "sankey",
    "kpi",
    "gauge",
    "bullet",
    "gantt",
    "text-table",
    "pivot",
    "map-point",
    "map-filled",
];

fn obj(props: &[(&str, Value)], required: &[&str]) -> Value {
    let mut p = Map::new();
    for (k, v) in props {
        p.insert((*k).to_owned(), v.clone());
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": Value::Object(p),
        "required": required,
    })
}

fn s(d: &str) -> Value {
    json!({"type": "string", "description": d})
}

fn int(d: &str) -> Value {
    json!({"type": "integer", "description": d})
}

fn one_of(vals: &[&str], d: &str) -> Value {
    json!({"type": "string", "enum": vals, "description": d})
}

fn strs(d: &str) -> Value {
    json!({"type": "array", "items": {"type": "string"}, "description": d})
}

fn db() -> Value {
    s("The database (schema) name")
}

fn spec(
    name: &'static str,
    description: &'static str,
    input: Value,
    effect: Effect,
    roles: &'static [Role],
    shares: Shares,
) -> ToolSpec {
    ToolSpec {
        name,
        description,
        input,
        effect,
        roles,
        shares,
    }
}

/// Every built-in tool.
#[must_use]
#[allow(clippy::too_many_lines)] // one table
pub fn specs() -> Vec<ToolSpec> {
    use Effect::{Propose, Read};
    vec![
        spec(
            "search_docs",
            "Search SprigDB's own documentation for this release. Call it before answering any question about how SprigDB works, its settings, errors, SQL extensions or operations, and cite the pages you use.",
            obj(
                &[
                    ("query", s("What to look for, in words")),
                    (
                        "audience",
                        one_of(&["user", "dba", "operator"], "Only pages for this audience"),
                    ),
                ],
                &["query"],
            ),
            Read,
            ALL,
            Shares::Schema,
        ),
        spec(
            "catalog",
            "List the databases the user can see or, with db, that database's tables, views and collections with their kinds and approximate sizes. Call it to find what exists before writing a query.",
            obj(&[("db", db())], &[]),
            Read,
            DBA_DATA,
            Shares::Schema,
        ),
        spec(
            "describe",
            "Describe one table: columns, types, keys, indexes, distribution and comments. Call it before writing SQL that uses the table.",
            obj(&[("db", db()), ("table", s("The table"))], &["db", "table"]),
            Read,
            DBA_DATA,
            Shares::Schema,
        ),
        spec(
            "workload",
            "The tenant's heaviest statement fingerprints over the last hours (1 to 168): calls, latency percentiles, rows examined. Call it when asked what is slow or busy.",
            obj(
                &[("db", db()), ("hours", int("Hours to look back, 1 to 168"))],
                &["hours"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "fingerprint",
            "One statement fingerprint's normalized text, plans seen and latency history. Call it after workload to look closer at one statement.",
            obj(
                &[("id", s("The fingerprint id, as workload returns it"))],
                &["id"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "explain",
            "The plan of a statement (EXPLAIN FORMAT=JSON), run as the user without executing it. Call it to judge a query's cost or an index's use.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One SELECT, INSERT, UPDATE or DELETE")),
                ],
                &["db", "sql"],
            ),
            Read,
            DBA_DATA,
            Shares::Stats,
        ),
        spec(
            "indexes",
            "A table's indexes, with their columns, uniqueness and whether automatic indexing made them.",
            obj(&[("db", db()), ("table", s("The table"))], &["db", "table"]),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "auto_indexes",
            "Automatic indexing's candidates, active indexes and recent drops, with the reasons.",
            obj(&[("db", db())], &[]),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "settings",
            "SprigDB settings with their values, where each value comes from, and what they mean. Call it before suggesting a setting change.",
            obj(
                &[
                    (
                        "key",
                        s("A key or a prefix such as ai. or studio.; all when absent"),
                    ),
                    (
                        "scope",
                        one_of(
                            &["global", "dc", "tenant", "database"],
                            "Resolve at this scope",
                        ),
                    ),
                ],
                &[],
            ),
            Read,
            DBA_ADMIN,
            Shares::Schema,
        ),
        spec(
            "shards",
            "How the tenant's sharded tables are split and placed, with shard sizes.",
            obj(&[("db", db())], &[]),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "advisor_runs",
            "Recent runs of the shard and index advisors, with their verdicts.",
            obj(&[], &[]),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "approvals",
            "Recommendations waiting in the approvals inbox, or decided ones, that the user may see.",
            obj(
                &[
                    (
                        "state",
                        one_of(&["open", "decided", "all"], "Which items; open when absent"),
                    ),
                    ("kind", s("Only this recommendation kind")),
                ],
                &[],
            ),
            Read,
            ALL,
            Shares::Schema,
        ),
        spec(
            "column_stats",
            "Statistics of up to 50 columns: counts, null fractions and distinct estimates; minimum, maximum and common values only when data sharing is on.",
            obj(
                &[
                    ("db", db()),
                    ("table", s("The table")),
                    ("cols", strs("The columns, 1 to 50")),
                ],
                &["db", "table", "cols"],
            ),
            Read,
            DBA_DATA,
            Shares::Stats,
        ),
        spec(
            "run_query",
            "Run one read-only SELECT as the user, in a read-only transaction, returning at most limit rows (1 to 1000). Rows reach you only when data sharing is on; otherwise you get the columns and the row count, and should offer the query to the user with propose_sql.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One SELECT")),
                    ("limit", int("Rows to return, 1 to 1000")),
                ],
                &["db", "sql"],
            ),
            Read,
            DATA,
            Shares::Rows,
        ),
        spec(
            "test_policy",
            "Test the row-level security policies: which rows of a SELECT another user would see (counts, not values).",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One SELECT")),
                    ("as_user", s("The user to test as")),
                ],
                &["db", "sql", "as_user"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "compile_logic",
            "Compile KIRun logic without deploying it, returning its errors and warnings.",
            obj(
                &[("db", db()), ("source", s("The logic's source"))],
                &["db", "source"],
            ),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "decision_log",
            "SprigDB's own decisions about a subject (placement, moves, failovers, approvals history), with their reasons. Call it to explain why SprigDB did something.",
            obj(
                &[(
                    "subject",
                    s("What the decisions are about, such as a table or cluster"),
                )],
                &["subject"],
            ),
            Read,
            DBA_ADMIN,
            Shares::Stats,
        ),
        spec(
            "metrics_trend",
            "A metric's history from the controller, by day, for up to 30 days.",
            obj(
                &[
                    ("name", s("The metric, such as sprig_shard_bytes")),
                    ("labels", strs("label=value filters")),
                    ("days", int("Days to look back, 1 to 30")),
                ],
                &["name", "days"],
            ),
            Read,
            ADMIN,
            Shares::Stats,
        ),
        spec(
            "propose_sql",
            "Propose a statement for the user to review and run. Use it for anything that changes data or schema, and for queries the user should run. It is shown to the user and never run by you.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("The statement")),
                    ("why", s("Why, in a sentence or two")),
                    ("estimate", s("What it will do: rows touched, time, locks")),
                ],
                &["db", "sql", "why"],
            ),
            Propose,
            DBA_DATA,
            Shares::Schema,
        ),
        spec(
            "propose_recommendation",
            "Propose a recommendation for the approvals inbox, for changes people decide on later.",
            obj(
                &[
                    ("kind", s("The recommendation kind")),
                    ("subject", s("What it is about")),
                    ("body", s("What to do and why")),
                ],
                &["kind", "subject", "body"],
            ),
            Propose,
            DBA_ADMIN,
            Shares::Schema,
        ),
        spec(
            "propose_tile",
            "Propose a dashboard tile: a query and a chart spec (JSON with a mark and shelves). The user previews it and decides whether to add it.",
            obj(
                &[
                    ("dashboard", s("The dashboard; a new one when absent")),
                    ("title", s("The tile's title")),
                    ("db", db()),
                    ("sql", s("The tile's query, returning rows")),
                    (
                        "chart",
                        s("The chart spec as JSON: mark, columns, rows, color, ..."),
                    ),
                ],
                &["title", "sql", "chart"],
            ),
            Propose,
            &[Role::Data, Role::Dba],
            Shares::Schema,
        ),
        spec(
            "propose_glossary",
            "Propose a glossary entry: a business term, its definition and optionally its SQL. A tenant admin accepts it.",
            obj(
                &[
                    ("term", s("The term")),
                    ("definition", s("What it means here")),
                    ("sql", s("An SQL expression for it")),
                ],
                &["term", "definition"],
            ),
            Propose,
            &[Role::Data, Role::Dba],
            Shares::Schema,
        ),
        spec(
            "generate_rows",
            "Propose test rows for a table, generated from its schema by SprigDB when the user accepts (1 to 100000 rows).",
            obj(
                &[
                    ("db", db()),
                    ("table", s("The table")),
                    ("n", int("Rows, 1 to 100000")),
                    ("rules", s("Rules for the values, in words")),
                ],
                &["db", "table", "n"],
            ),
            Propose,
            DBA,
            Shares::Schema,
        ),
    ]
}

/// The proposal kind of a proposal tool.
#[must_use]
pub fn proposal_kind(name: &str) -> Option<ProposalKind> {
    Some(match name {
        "propose_sql" => ProposalKind::Sql,
        "propose_recommendation" => ProposalKind::Recommendation,
        "propose_tile" => ProposalKind::Tile,
        "propose_glossary" => ProposalKind::Glossary,
        "generate_rows" => ProposalKind::GenerateRows,
        _ => return None,
    })
}

fn range(input: &Value, k: &str, lo: i64, hi: i64) -> Result<(), String> {
    match input.get(k).and_then(Value::as_i64) {
        Some(n) if !(lo..=hi).contains(&n) => Err(format!("{k} must be {lo} to {hi}")),
        _ => Ok(()),
    }
}

/// The checks a schema can't say: ranges, lengths, SQL that must be a
/// read, a chart spec.
///
/// # Errors
/// What is wrong, for the model.
pub fn check_input(name: &str, input: &Value) -> Result<(), String> {
    let text = |k: &str| input.get(k).and_then(Value::as_str).unwrap_or_default();
    for k in ["sql", "source", "body", "definition", "rules", "chart"] {
        if text(k).len() > MAX_TEXT {
            return Err(format!("{k} is longer than {MAX_TEXT} bytes"));
        }
    }
    match name {
        "workload" => range(input, "hours", 1, 168),
        "metrics_trend" => range(input, "days", 1, 30),
        "generate_rows" => range(input, "n", 1, 100_000),
        "run_query" => {
            range(input, "limit", 1, 1000)?;
            sql_guard::read_only(text("sql"))
        }
        "test_policy" => sql_guard::read_only(text("sql")),
        "explain" => sql_guard::explainable(text("sql")),
        "column_stats" => {
            let n = input
                .get("cols")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            if (1..=50).contains(&n) {
                Ok(())
            } else {
                Err("cols must name 1 to 50 columns".to_owned())
            }
        }
        "propose_tile" => {
            let chart: Value = serde_json::from_str(text("chart"))
                .map_err(|e| format!("chart isn't JSON: {e}"))?;
            match chart.get("mark").and_then(Value::as_str) {
                Some(m) if MARKS.contains(&m) => Ok(()),
                Some(m) => Err(format!("unknown mark {m}; use one of {}", MARKS.join(", "))),
                None => Err("chart needs a mark".to_owned()),
            }
        }
        _ => Ok(()),
    }
}
