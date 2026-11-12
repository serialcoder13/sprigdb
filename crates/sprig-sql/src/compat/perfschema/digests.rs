//! Statement digests on port 3306 (chapter 28 step 13 with chapter 12's
//! workload capture; R4 round 5): `events_statements_summary_by_digest`
//! and the `sys` views over it, `statement_analysis` and
//! `statements_with_errors_or_warnings` (with their `x$` forms).
//!
//! The gateway passes the tenant's digests ([`PsDigest`]): chapter 12's
//! recorder keeps, beside each fingerprint's hourly statistics, its totals
//! since this gateway first counted it or since the tenant's last
//! `TRUNCATE` of the summary, MySQL's lifetime for the table. Each digest
//! becomes one row with MySQL 8.4's columns:
//!
//! - `DIGEST` is the fingerprint's id in 16 hex digits (MySQL's is a
//!   SHA-256 in 64), `DIGEST_TEXT` its normalized text (literals as `?`,
//!   `IN` lists folded, names qualified with the database).
//! - Times are in picoseconds, from the gateway's measured latency (send
//!   to end of result), so they include the network to MariaDB.
//! - `SUM_ROWS_EXAMINED` adds up only the executions chapter 12 probed
//!   (watched fingerprints, a sample); `QUANTILE_*` are the upper bounds
//!   of the recorder's histogram buckets.
//! - Counters MariaDB can't give per statement (lock time, temporary
//!   tables, sorts, index use, memory, warnings) are 0;
//!   `QUERY_SAMPLE_TEXT` is `NULL`: chapter 12 keeps no plain statement
//!   text (parameter samples are sealed).

use super::{format_pico_time, sql_string};

/// One digest row, as the gateway hands it over.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PsDigest {
    /// The logical database (`SCHEMA_NAME`), if any.
    pub schema: Option<String>,
    /// The fingerprint id, 16 hex digits.
    pub digest: String,
    /// The normalized text.
    pub text: String,
    /// Executions.
    pub count: u64,
    /// Executions that failed.
    pub errors: u64,
    /// Sum, shortest and longest latency, in microseconds.
    pub sum_us: u64,
    /// Shortest latency.
    pub min_us: u64,
    /// Longest latency.
    pub max_us: u64,
    /// Rows changed.
    pub rows_affected: u64,
    /// Rows sent.
    pub rows_sent: u64,
    /// Rows examined by the probed executions.
    pub rows_examined: u64,
    /// First and last execution, microseconds since the epoch.
    pub first_seen_us: u64,
    /// Last execution.
    pub last_seen_us: u64,
    /// Latency quantiles 0.95, 0.99 and 0.999, in microseconds.
    pub p95_us: u64,
    /// 0.99.
    pub p99_us: u64,
    /// 0.999.
    pub p999_us: u64,
}

fn ps(us: u64) -> u64 {
    us.saturating_mul(1_000_000)
}

fn time(us: u64) -> String {
    format!("FROM_UNIXTIME({}.{:06})", us / 1_000_000, us % 1_000_000)
}

/// One row: each column of `cols` (`NAME:CAST;...`) from `values` by name,
/// else 0 for a number and `NULL` for anything else.
fn row(cols: &str, values: &[(&str, String)]) -> String {
    let items: Vec<String> = cols
        .split(';')
        .filter_map(|c| c.split_once(':'))
        .map(|(n, ty)| {
            let v = values
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(n))
                .map_or_else(
                    || {
                        if ty == "UNSIGNED" || ty == "SIGNED" || ty.starts_with("DECIMAL") {
                            "0".to_owned()
                        } else {
                            "NULL".to_owned()
                        }
                    },
                    |(_, v)| v.clone(),
                );
            format!("CAST({v} AS {ty}) AS `{n}`")
        })
        .collect();
    format!("SELECT {}", items.join(", "))
}

#[allow(clippy::needless_pass_by_value)] // takes the collected rows
fn union(rows: Vec<String>) -> Option<String> {
    (!rows.is_empty()).then(|| rows.join(" UNION ALL "))
}

/// The computed rows of `obj` (lower case) in `schema` (`performance_schema`
/// or `sys`), whose columns are `cols`; `None` when it isn't a digest
/// table or there are no digests (the caller then gives the empty table).
#[allow(clippy::too_many_lines)] // one arm per table
pub(super) fn object(
    schema: &str,
    obj: &str,
    cols: &str,
    digests: &[PsDigest],
    nbe: bool,
) -> Option<String> {
    let lit = |v: &str| sql_string(v, nbe);
    let opt = |v: Option<&str>| v.map_or_else(|| "NULL".to_owned(), lit);
    let avg = |sum: u64, n: u64| sum.checked_div(n).unwrap_or(0);
    let pct = |part: u64, n: u64| {
        if n == 0 {
            "0".to_owned()
        } else {
            format!("ROUND(100 * {part} / {n}, 4)")
        }
    };
    match (schema, obj) {
        ("performance_schema", "events_statements_summary_by_digest") => union(
            digests
                .iter()
                .map(|d| {
                    row(
                        cols,
                        &[
                            ("SCHEMA_NAME", opt(d.schema.as_deref())),
                            ("DIGEST", lit(&d.digest)),
                            ("DIGEST_TEXT", lit(&d.text)),
                            ("COUNT_STAR", d.count.to_string()),
                            ("SUM_TIMER_WAIT", ps(d.sum_us).to_string()),
                            ("MIN_TIMER_WAIT", ps(d.min_us).to_string()),
                            ("AVG_TIMER_WAIT", ps(avg(d.sum_us, d.count)).to_string()),
                            ("MAX_TIMER_WAIT", ps(d.max_us).to_string()),
                            ("SUM_ERRORS", d.errors.to_string()),
                            ("SUM_ROWS_AFFECTED", d.rows_affected.to_string()),
                            ("SUM_ROWS_SENT", d.rows_sent.to_string()),
                            ("SUM_ROWS_EXAMINED", d.rows_examined.to_string()),
                            ("FIRST_SEEN", time(d.first_seen_us)),
                            ("LAST_SEEN", time(d.last_seen_us)),
                            ("QUANTILE_95", ps(d.p95_us).to_string()),
                            ("QUANTILE_99", ps(d.p99_us).to_string()),
                            ("QUANTILE_999", ps(d.p999_us).to_string()),
                        ],
                    )
                })
                .collect(),
        ),
        ("sys", "statement_analysis" | "x$statement_analysis") => {
            let x = obj.starts_with("x$");
            let lat = |us: u64| {
                if x {
                    ps(us).to_string()
                } else {
                    format_pico_time(&ps(us).to_string())
                }
            };
            let query = |t: &str| {
                if x {
                    lit(t)
                } else {
                    let v = lit(t);
                    format!(
                        "(CASE WHEN CHAR_LENGTH({v}) > 64 THEN CONCAT(LEFT({v}, 30), ' ... ', RIGHT({v}, 30)) ELSE {v} END)"
                    )
                }
            };
            let zero_lat = lat(0);
            let mem = if x {
                "0".to_owned()
            } else {
                "'   0 bytes'".to_owned()
            };
            union(
                digests
                    .iter()
                    .map(|d| {
                        row(
                            cols,
                            &[
                                ("query", query(&d.text)),
                                ("db", opt(d.schema.as_deref())),
                                ("full_scan", "''".to_owned()),
                                ("exec_count", d.count.to_string()),
                                ("err_count", d.errors.to_string()),
                                ("total_latency", lat(d.sum_us)),
                                ("max_latency", lat(d.max_us)),
                                ("avg_latency", lat(avg(d.sum_us, d.count))),
                                ("lock_latency", zero_lat.clone()),
                                ("cpu_latency", zero_lat.clone()),
                                ("rows_sent", d.rows_sent.to_string()),
                                (
                                    "rows_sent_avg",
                                    format!("ROUND({} / {})", d.rows_sent, d.count.max(1)),
                                ),
                                ("rows_examined", d.rows_examined.to_string()),
                                (
                                    "rows_examined_avg",
                                    format!("ROUND({} / {})", d.rows_examined, d.count.max(1)),
                                ),
                                ("rows_affected", d.rows_affected.to_string()),
                                (
                                    "rows_affected_avg",
                                    format!("ROUND({} / {})", d.rows_affected, d.count.max(1)),
                                ),
                                ("max_controlled_memory", mem.clone()),
                                ("max_total_memory", mem.clone()),
                                ("digest", lit(&d.digest)),
                                ("first_seen", time(d.first_seen_us)),
                                ("last_seen", time(d.last_seen_us)),
                            ],
                        )
                    })
                    .collect(),
            )
        }
        ("sys", "statements_with_errors_or_warnings" | "x$statements_with_errors_or_warnings") => {
            let x = obj.starts_with("x$");
            union(
                digests
                    .iter()
                    .filter(|d| d.errors > 0)
                    .map(|d| {
                        let q = lit(&d.text);
                        let query = if x {
                            q
                        } else {
                            format!(
                                "(CASE WHEN CHAR_LENGTH({q}) > 64 THEN CONCAT(LEFT({q}, 30), ' ... ', RIGHT({q}, 30)) ELSE {q} END)"
                            )
                        };
                        row(
                            cols,
                            &[
                                ("query", query),
                                ("db", opt(d.schema.as_deref())),
                                ("exec_count", d.count.to_string()),
                                ("errors", d.errors.to_string()),
                                ("error_pct", pct(d.errors, d.count)),
                                ("digest", lit(&d.digest)),
                                ("first_seen", time(d.first_seen_us)),
                                ("last_seen", time(d.last_seen_us)),
                            ],
                        )
                    })
                    .collect(),
            )
        }
        _ => None,
    }
}

/// Whether `obj` (lower case) in `schema` is computed from digests.
#[must_use]
pub(super) fn is_digest_object(schema: &str, obj: &str) -> bool {
    matches!(
        (schema, obj),
        ("performance_schema", "events_statements_summary_by_digest")
            | (
                "sys",
                "statement_analysis"
                    | "x$statement_analysis"
                    | "statements_with_errors_or_warnings"
                    | "x$statements_with_errors_or_warnings"
            )
    )
}

/// The text of a statement that empties the digest summary: `TRUNCATE
/// [TABLE] performance_schema.events_statements_summary_by_digest` (or
/// unqualified with `performance_schema` current).
#[must_use]
pub fn is_truncate_digests(sql: &[u8], current_db: Option<&str>) -> bool {
    let text = String::from_utf8_lossy(sql);
    let words: Vec<String> = text
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|w| w.replace('`', "").to_ascii_lowercase())
        .collect();
    let rest = match words.as_slice() {
        [t, tb, r] if t == "truncate" && tb == "table" => r,
        [t, r] if t == "truncate" => r,
        _ => return false,
    };
    match rest.split_once('.') {
        Some((db, t)) => db == "performance_schema" && t == "events_statements_summary_by_digest",
        None => {
            rest == "events_statements_summary_by_digest"
                && current_db.is_some_and(|d| d.eq_ignore_ascii_case("performance_schema"))
        }
    }
}
