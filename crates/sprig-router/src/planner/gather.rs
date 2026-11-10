//! Planning gathered reads (chapter 17, "Gathered reads (R3)"): a `SELECT`
//! the planner rejected for a reason R3 lifts becomes a
//! [`GatherPlan`](crate::gather::GatherPlan) when every function it calls
//! is a MariaDB built-in that answers the same on another connection.
//!
//! ```text
//! gather(stmt, why):
//!   only a SELECT, without INTO, FOR UPDATE / FOR SHARE, MATCH … AGAINST,
//!     stored or session functions                       → else why (or the guard's reason)
//!   each sharded or reference table named              → one GatherTable
//!   a table named once, in the top SELECT, not on the null side of an outer join:
//!     shards: its key equalities (the planner's own targets)
//!     filter: its WHERE conjuncts that read only it and print exactly
//! ```

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, ObjectNamePart, SetExpr, Statement, UnaryOperator, Value,
    visit_expressions, visit_relations,
};

use super::{PlanCtx, R, Tgt, analyse, collect_queries_stmt, ident_parts, refs, resolve_name};
use crate::gather::{GatherPlan, GatherTable};
use crate::plan::{Plan, ShardAt, Target};
use crate::reject::{Reason, Rejection};
use crate::topology::{TableEntry, TableKind};

/// Whether a gathered read lifts rejection `r` of a `SELECT`.
pub(super) fn lifts(r: &Rejection) -> bool {
    matches!(
        r.reason,
        Reason::JoinCondition
            | Reason::JoinTypes
            | Reason::JoinDepth
            | Reason::CorrelatedSubquery
            | Reason::DerivedTable
            | Reason::RecursiveCte
            | Reason::WindowFunction
            | Reason::SetOperation
            | Reason::Aggregate
            | Reason::Ordering
            | Reason::Subquery
            | Reason::CrossCluster
            | Reason::Unsupported
    )
}

/// The gathered read of `stmt`, or why there is none.
pub(super) fn plan(stmt: &Statement, sql: &str, cx: &PlanCtx<'_>, why: &Rejection) -> Plan {
    match build(stmt, sql, cx, why) {
        Ok(g) => Plan::Gather(Box::new(g)),
        Err(r) => Plan::Reject(r),
    }
}

/// MariaDB built-ins whose answer depends on the connection that runs
/// them, or that change state: refused.
const SESSION_FUNCTIONS: &[&str] = &[
    "connection_id",
    "found_rows",
    "row_count",
    "last_insert_id",
    "get_lock",
    "release_lock",
    "release_all_locks",
    "is_free_lock",
    "is_used_lock",
    "master_pos_wait",
    "master_gtid_wait",
    "nextval",
    "lastval",
    "setval",
    "wsrep_last_seen_gtid",
    "wsrep_last_written_gtid",
    "wsrep_sync_wait_upto_gtid",
    "spider_bg_direct_sql",
    "spider_copy_tables",
    "spider_direct_sql",
    "spider_flush_table_mon_cache",
];

/// MariaDB 11.8's built-in functions (its help tables' function topics,
/// plus the ones they leave out). Anything else is a stored function or a
/// UDF, which could read the base tables the copies hide.
const BUILTINS: &[&str] = &[
    "abs",
    "acos",
    "adddate",
    "addtime",
    "add_months",
    "aes_decrypt",
    "aes_encrypt",
    "area",
    "asbinary",
    "ascii",
    "asin",
    "astext",
    "aswkb",
    "aswkt",
    "atan",
    "atan2",
    "avg",
    "benchmark",
    "bin",
    "binlog_gtid_pos",
    "bit_and",
    "bit_count",
    "bit_length",
    "bit_or",
    "bit_xor",
    "boundary",
    "buffer",
    "cast",
    "ceil",
    "ceiling",
    "centroid",
    "char",
    "charset",
    "char_length",
    "character_length",
    "chr",
    "coalesce",
    "coercibility",
    "collation",
    "column_add",
    "column_check",
    "column_create",
    "column_delete",
    "column_exists",
    "column_get",
    "column_json",
    "column_list",
    "compress",
    "concat",
    "concat_ws",
    "contains",
    "conv",
    "convert",
    "convert_tz",
    "convexhull",
    "cos",
    "cot",
    "count",
    "crc32",
    "crc32c",
    "crosses",
    "cume_dist",
    "curdate",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "curtime",
    "database",
    "date",
    "datediff",
    "date_add",
    "date_format",
    "date_sub",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "decode",
    "decode_histogram",
    "decode_oracle",
    "default",
    "degrees",
    "dense_rank",
    "des_decrypt",
    "des_encrypt",
    "dimension",
    "disjoint",
    "elt",
    "encode",
    "encrypt",
    "endpoint",
    "envelope",
    "equals",
    "exp",
    "export_set",
    "exteriorring",
    "extract",
    "extractvalue",
    "field",
    "find_in_set",
    "first_value",
    "floor",
    "format",
    "format_bytes",
    "format_pico_time",
    "from_base64",
    "from_days",
    "from_unixtime",
    "geomcollfromtext",
    "geomcollfromwkb",
    "geometrycollection",
    "geometrycollectionfromtext",
    "geometrycollectionfromwkb",
    "geometryfromtext",
    "geometryfromwkb",
    "geometryn",
    "geometrytype",
    "geomfromtext",
    "geomfromwkb",
    "get_format",
    "glength",
    "greatest",
    "group_concat",
    "hex",
    "hour",
    "if",
    "ifnull",
    "inet6_aton",
    "inet6_ntoa",
    "inet_aton",
    "inet_ntoa",
    "insert",
    "instr",
    "interiorringn",
    "intersects",
    "interval",
    "isclosed",
    "isempty",
    "isnull",
    "isring",
    "issimple",
    "is_ipv4",
    "is_ipv4_compat",
    "is_ipv4_mapped",
    "is_ipv6",
    "json_array",
    "json_arrayagg",
    "json_array_append",
    "json_array_insert",
    "json_array_intersect",
    "json_compact",
    "json_contains",
    "json_contains_path",
    "json_depth",
    "json_detailed",
    "json_equals",
    "json_exists",
    "json_extract",
    "json_insert",
    "json_keys",
    "json_key_value",
    "json_length",
    "json_loose",
    "json_merge",
    "json_merge_patch",
    "json_merge_preserve",
    "json_normalize",
    "json_object",
    "json_objectagg",
    "json_object_filter_keys",
    "json_object_to_array",
    "json_overlaps",
    "json_pretty",
    "json_query",
    "json_quote",
    "json_remove",
    "json_replace",
    "json_schema_valid",
    "json_search",
    "json_set",
    "json_type",
    "json_unquote",
    "json_valid",
    "json_value",
    "kdf",
    "lag",
    "last_day",
    "last_value",
    "lcase",
    "lead",
    "least",
    "left",
    "length",
    "lengthb",
    "linefromtext",
    "linefromwkb",
    "linestring",
    "linestringfromtext",
    "linestringfromwkb",
    "ln",
    "load_file",
    "localtime",
    "localtimestamp",
    "locate",
    "log",
    "log10",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "makedate",
    "maketime",
    "make_set",
    "max",
    "mbrcontains",
    "mbrcoveredby",
    "mbrdisjoint",
    "mbrequal",
    "mbrequals",
    "mbrintersects",
    "mbroverlaps",
    "mbrtouches",
    "mbrwithin",
    "md5",
    "median",
    "microsecond",
    "mid",
    "min",
    "minute",
    "mlinefromtext",
    "mlinefromwkb",
    "mod",
    "month",
    "monthname",
    "mpointfromtext",
    "mpointfromwkb",
    "mpolyfromtext",
    "mpolyfromwkb",
    "multilinestring",
    "multilinestringfromtext",
    "multilinestringfromwkb",
    "multipoint",
    "multipointfromtext",
    "multipointfromwkb",
    "multipolygon",
    "multipolygonfromtext",
    "multipolygonfromwkb",
    "name_const",
    "natural_sort_key",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "numgeometries",
    "numinteriorrings",
    "numpoints",
    "nvl",
    "nvl2",
    "oct",
    "octet_length",
    "old_password",
    "ord",
    "overlaps",
    "password",
    "percentile_cont",
    "percentile_disc",
    "percent_rank",
    "period_add",
    "period_diff",
    "pi",
    "point",
    "pointfromtext",
    "pointfromwkb",
    "pointn",
    "pointonsurface",
    "polyfromtext",
    "polyfromwkb",
    "polygon",
    "polygonfromtext",
    "polygonfromwkb",
    "position",
    "pow",
    "power",
    "quarter",
    "quote",
    "radians",
    "rand",
    "random_bytes",
    "rank",
    "regexp_instr",
    "regexp_replace",
    "regexp_substr",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "rownum",
    "row_number",
    "rpad",
    "rtrim",
    "schema",
    "second",
    "sec_to_time",
    "session_user",
    "sformat",
    "sha",
    "sha1",
    "sha2",
    "sign",
    "sin",
    "sleep",
    "soundex",
    "space",
    "sqrt",
    "srid",
    "startpoint",
    "std",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "strcmp",
    "str_to_date",
    "st_area",
    "st_asbinary",
    "st_asgeojson",
    "st_astext",
    "st_aswkb",
    "st_aswkt",
    "st_boundary",
    "st_buffer",
    "st_centroid",
    "st_collect",
    "st_contains",
    "st_convexhull",
    "st_crosses",
    "st_difference",
    "st_dimension",
    "st_disjoint",
    "st_distance",
    "st_distance_sphere",
    "st_endpoint",
    "st_envelope",
    "st_equals",
    "st_exteriorring",
    "st_geohash",
    "st_geomcollfromtext",
    "st_geomcollfromwkb",
    "st_geometrycollectionfromtext",
    "st_geometrycollectionfromwkb",
    "st_geometryfromtext",
    "st_geometryfromwkb",
    "st_geometryn",
    "st_geometrytype",
    "st_geomfromgeojson",
    "st_geomfromtext",
    "st_geomfromwkb",
    "st_interiorringn",
    "st_intersection",
    "st_intersects",
    "st_isclosed",
    "st_isempty",
    "st_isring",
    "st_issimple",
    "st_isvalid",
    "st_latfromgeohash",
    "st_length",
    "st_linefromtext",
    "st_linefromwkb",
    "st_linestringfromtext",
    "st_linestringfromwkb",
    "st_longfromgeohash",
    "st_numgeometries",
    "st_numinteriorrings",
    "st_numpoints",
    "st_overlaps",
    "st_pointfromgeohash",
    "st_pointfromtext",
    "st_pointfromwkb",
    "st_pointn",
    "st_pointonsurface",
    "st_polyfromtext",
    "st_polyfromwkb",
    "st_polygonfromtext",
    "st_polygonfromwkb",
    "st_relate",
    "st_simplify",
    "st_srid",
    "st_startpoint",
    "st_symdifference",
    "st_touches",
    "st_union",
    "st_validate",
    "st_within",
    "st_x",
    "st_y",
    "subdate",
    "substr",
    "substring",
    "substring_index",
    "subtime",
    "sum",
    "sysdate",
    "system_user",
    "sys_guid",
    "tan",
    "time",
    "timediff",
    "timestamp",
    "timestampadd",
    "timestampdiff",
    "time_format",
    "time_to_sec",
    "touches",
    "to_base64",
    "to_char",
    "to_days",
    "to_seconds",
    "trim",
    "truncate",
    "ucase",
    "uncompress",
    "uncompressed_length",
    "unhex",
    "unix_timestamp",
    "updatexml",
    "upper",
    "user",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    "uuid",
    "uuid_short",
    "uuid_v4",
    "uuid_v7",
    "values",
    "variance",
    "var_pop",
    "var_samp",
    "vec_distance",
    "vec_distance_cosine",
    "vec_distance_euclidean",
    "vec_fromtext",
    "vec_totext",
    "version",
    "week",
    "weekday",
    "weekofyear",
    "weight_string",
    "within",
    "x",
    "y",
    "year",
    "yearweek",
];

fn builtins() -> &'static HashSet<&'static str> {
    static SET: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    SET.get_or_init(|| BUILTINS.iter().copied().collect())
}

/// The guard: what the copies can't answer the way the session would.
fn guard(stmt: &Statement, why: &Rejection) -> R<()> {
    let mut queries = Vec::new();
    collect_queries_stmt(stmt, &mut queries);
    for q in &queries {
        if !q.locks.is_empty() || q.for_clause.is_some() {
            return Err(why.clone());
        }
        let mut sels = Vec::new();
        super::blocks(q, &mut sels, false);
        if sels.iter().any(|(s, _)| s.into.is_some()) {
            return Err(Rejection::new(
                Reason::Variables,
                "SELECT … INTO across shards",
            ));
        }
    }
    let mut bad: Option<Rejection> = None;
    let _ = visit_expressions(stmt, |e| {
        match e {
            Expr::Function(f) => {
                let parts: Vec<String> = f
                    .name
                    .0
                    .iter()
                    .map(|p| match p {
                        ObjectNamePart::Identifier(i) => i.value.to_ascii_lowercase(),
                        ObjectNamePart::Function(_) => String::from("?"),
                    })
                    .collect();
                let name = parts.join(".");
                if SESSION_FUNCTIONS.contains(&name.as_str()) {
                    bad = Some(Rejection::new(
                        Reason::Gather,
                        format!("{name}() answers for the session's own connection"),
                    ));
                } else if parts.len() != 1 || !builtins().contains(name.as_str()) {
                    bad = Some(Rejection::new(
                        Reason::Routine,
                        format!("{name}() is not a MariaDB built-in"),
                    ));
                }
            }
            Expr::MatchAgainst { .. } => {
                bad = Some(Rejection::new(
                    Reason::FulltextRelevance,
                    "MATCH … AGAINST across shards",
                ));
            }
            _ => {}
        }
        if bad.is_some() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    bad.map_or(Ok(()), Err)
}

/// A conjunct the shard can evaluate as printed: columns, plain literals,
/// comparisons, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `IN (…)`, `BETWEEN`
/// and `LIKE`. Strings with backslashes or quotes, user variables and
/// anything else stay in the statement only.
pub(crate) fn pushable(e: &Expr) -> bool {
    let ident_ok = |i: &sqlparser::ast::Ident| {
        matches!(i.quote_style, None | Some('`')) && !i.value.starts_with('@')
    };
    match e {
        Expr::Identifier(i) => ident_ok(i),
        Expr::CompoundIdentifier(p) => p.len() == 2 && p.iter().all(ident_ok),
        Expr::Value(v) => match &v.value {
            Value::Number(..) | Value::Null | Value::Boolean(_) | Value::HexStringLiteral(_) => {
                true
            }
            Value::SingleQuotedString(s) => !s.contains('\\') && !s.contains('\''),
            _ => false,
        },
        Expr::Nested(x) | Expr::IsNull(x) | Expr::IsNotNull(x) => pushable(x),
        Expr::UnaryOp { op, expr } => {
            matches!(
                op,
                UnaryOperator::Not | UnaryOperator::Minus | UnaryOperator::Plus
            ) && pushable(expr)
        }
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Spaceship
                    | BinaryOperator::And
                    | BinaryOperator::Or
                    | BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
            ) && pushable(left)
                && pushable(right)
        }
        Expr::InList { expr, list, .. } => pushable(expr) && list.iter().all(pushable),
        Expr::Between {
            expr, low, high, ..
        } => pushable(expr) && pushable(low) && pushable(high),
        Expr::Like {
            any: false,
            expr,
            pattern,
            escape_char: None,
            ..
        } => pushable(expr) && pushable(pattern),
        _ => false,
    }
}

/// One table to copy, as planning collects it.
struct Found<'a> {
    entry: &'a TableEntry,
    schema: String,
    name: String,
    count: usize,
    alias: Option<String>,
    shards: Option<BTreeSet<usize>>,
    filter: Option<String>,
}

fn build(stmt: &Statement, sql: &str, cx: &PlanCtx<'_>, why: &Rejection) -> R<GatherPlan> {
    let Statement::Query(q) = stmt else {
        return Err(why.clone());
    };
    guard(stmt, why)?;
    // CTE names: a relation that names one isn't a table.
    let mut queries = Vec::new();
    collect_queries_stmt(stmt, &mut queries);
    let mut ctes: HashSet<String> = HashSet::new();
    for qq in &queries {
        if let Some(w) = &qq.with {
            for c in &w.cte_tables {
                ctes.insert(c.alias.name.value.to_ascii_lowercase());
            }
        }
    }
    let mut found: Vec<Found<'_>> = Vec::new();
    let mut index: HashMap<(String, String), usize> = HashMap::new();
    let mut reads_unsharded = false;
    let _ = visit_relations(stmt, |n| {
        let one_part_cte = matches!(ident_parts(n).as_deref(), Some([t]) if ctes.contains(&t.value.to_ascii_lowercase()));
        match resolve_name(n, cx) {
            Some((entry, schema)) => {
                let name = ident_parts(n)
                    .and_then(|p| p.last().map(|i| i.value.clone()))
                    .unwrap_or_else(|| entry.name.clone());
                let key = (schema.clone(), name.to_ascii_lowercase());
                // (A name a CTE also has turns filters off: `ctes` below.)
                if let Some(&i) = index.get(&key) {
                    found[i].count += 1;
                } else {
                    index.insert(key, found.len());
                    found.push(Found {
                        entry,
                        schema,
                        name,
                        count: 1,
                        alias: None,
                        shards: None,
                        filter: None,
                    });
                }
            }
            None if one_part_cte => {}
            None => reads_unsharded = true,
        }
        ControlFlow::<()>::Continue(())
    });
    if found.is_empty() {
        return Err(why.clone());
    }
    // Shard targets and filters for tables named once in the top SELECT.
    if ctes.is_empty()
        && !sql.contains("||")
        && let SetExpr::Select(select) = &*q.body
        && let Ok(b) = analyse(select, cx)
        && !b.complex
    {
        for (i, r) in b.rels.iter().enumerate() {
            let Some(entry) = r.entry else { continue };
            let key = (r.default_schema.clone(), r.table.clone());
            let Some(&fi) = index.get(&key) else { continue };
            let f = &mut found[fi];
            if f.count != 1 || r.nullable || !std::ptr::eq(f.entry, entry) {
                continue;
            }
            if !entry.is_reference()
                && let Tgt::Set(s) = &b.tgt[i]
            {
                f.shards = Some(s.clone());
            }
            let mine: Vec<String> = b
                .where_conj
                .iter()
                .filter(|c| {
                    refs(&b.rels, c).is_some_and(|s| s.len() == 1 && s.contains(&i)) && pushable(c)
                })
                .map(|c| format!("({c})"))
                .collect();
            if !mine.is_empty() {
                f.alias = Some(r.alias.clone().unwrap_or_else(|| f.name.clone()));
                f.filter = Some(mine.join(" AND "));
            }
        }
    }
    let tables = found
        .into_iter()
        .map(|f| {
            let g = &f.entry.group;
            let sources: Vec<Target> = match &f.entry.kind {
                TableKind::Reference { schema } => {
                    let i = g
                        .shards
                        .iter()
                        .position(|s| s.cluster == cx.session_cluster)
                        .unwrap_or(0);
                    g.shards
                        .get(i)
                        .map(|s| Target {
                            cluster: s.cluster.clone(),
                            shard: s.shard,
                            sql: String::new(),
                            at: ShardAt {
                                group: g.id,
                                db: g.db,
                                lo: s.lo,
                                schema: schema.clone(),
                            },
                        })
                        .into_iter()
                        .collect()
                }
                TableKind::Sharded { .. } => (0..g.shards.len())
                    .filter(|i| f.shards.as_ref().is_none_or(|s| s.contains(i)))
                    .map(|i| Target {
                        cluster: g.shards[i].cluster.clone(),
                        shard: g.shards[i].shard,
                        sql: String::new(),
                        at: g.at(i),
                    })
                    .collect(),
            };
            GatherTable {
                alias: f.alias.unwrap_or_else(|| f.name.clone()),
                schema: f.schema,
                name: f.name,
                shards_total: g.shards.len(),
                sources,
                filter: f.filter,
            }
        })
        .collect();
    Ok(GatherPlan {
        tables,
        sql: sql.to_owned(),
        reads_unsharded,
        fallback: why.clone(),
    })
}

#[cfg(test)]
mod tests;
