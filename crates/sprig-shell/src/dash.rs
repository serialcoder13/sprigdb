//! Live dashboards (chapter 53, "Live dashboards"): what each view reads,
//! as tables, for the TUI's Dashboard tab and `sprig top --once`.
//!
//! Every view reads only what the user may: tenant users get sessions,
//! fingerprints and messaging for their own tenant; the cluster, shard and
//! DC views need a profile with an Admin API (`admin_url`).

use std::collections::BTreeMap;
use std::process::ExitCode;

use clap::ArgMatches;
use serde_json::{Map, Value, json};

use crate::admin::AdminApi;
use crate::api::{Caps, DataClient};
use crate::cli::Ctx;
use crate::error::{Result, ShellError};
use crate::model::{Collect, ResultSet};
use crate::native::Protocol;
use crate::output::cell_text;

/// The views, in tab order.
pub const VIEW_NAMES: &[&str] = &[
    "sessions",
    "fingerprints",
    "clusters",
    "shards",
    "dcs",
    "messaging",
];

/// One view's table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Panel {
    /// The view's name.
    pub view: String,
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of text.
    pub rows: Vec<Vec<String>>,
    /// A note ("needs an admin profile", an error).
    pub note: Option<String>,
}

impl Panel {
    fn note(view: &str, note: impl Into<String>) -> Self {
        Self {
            view: view.to_owned(),
            note: Some(note.into()),
            ..Self::default()
        }
    }

    /// As JSON for `sprig top --json`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let rows: Vec<Value> = self
            .rows
            .iter()
            .map(|r| {
                let m: Map<String, Value> = self
                    .columns
                    .iter()
                    .zip(r)
                    .map(|(c, v)| (c.clone(), Value::String(v.clone())))
                    .collect();
                Value::Object(m)
            })
            .collect();
        let mut o = json!({ "rows": rows });
        if let Some(n) = &self.note {
            o["note"] = Value::String(n.clone());
        }
        o
    }
}

/// Where the dashboards read from.
#[derive(Clone)]
pub struct Source {
    /// The data API, as the user.
    pub client: DataClient,
    /// The current database (messaging views).
    pub database: Option<String>,
    /// The Admin API, for operators.
    pub admin: Option<AdminApi>,
    /// The tenant, for the shards view.
    pub tenant: Option<String>,
}

fn flatten(prefix: &str, v: &Value, depth: usize, out: &mut Vec<(String, String)>) {
    match v {
        Value::Object(m) if depth < 3 => {
            for (k, c) in m {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten(&key, c, depth + 1, out);
            }
        }
        Value::Array(a) => out.push((prefix.to_owned(), format!("[{}]", a.len()))),
        Value::String(s) => out.push((prefix.to_owned(), s.clone())),
        Value::Null => out.push((prefix.to_owned(), String::new())),
        other => out.push((prefix.to_owned(), other.to_string())),
    }
}

/// A JSON list as a table: the first array found (the value itself or a
/// field), objects flattened to dotted keys, preferred columns first, at
/// most `max_cols` columns.
#[must_use]
pub fn json_table(
    v: &Value,
    list_key: Option<&str>,
    prefer: &[&str],
    max_cols: usize,
) -> (Vec<String>, Vec<Vec<String>>) {
    let arr = match (v, list_key) {
        (_, Some(k)) => v.get(k).and_then(Value::as_array),
        (Value::Array(a), None) => Some(a),
        (Value::Object(m), None) => m.values().find_map(Value::as_array),
        _ => None,
    };
    let Some(arr) = arr else {
        return (Vec::new(), Vec::new());
    };
    let flat: Vec<Vec<(String, String)>> = arr
        .iter()
        .map(|o| {
            let mut f = Vec::new();
            flatten("", o, 0, &mut f);
            f
        })
        .collect();
    let mut cols: Vec<String> = Vec::new();
    for p in prefer {
        if flat.iter().any(|r| r.iter().any(|(k, _)| k == p)) {
            cols.push((*p).to_owned());
        }
    }
    for r in &flat {
        for (k, _) in r {
            if cols.len() >= max_cols {
                break;
            }
            if !cols.contains(k) {
                cols.push(k.clone());
            }
        }
    }
    cols.truncate(max_cols);
    let rows = flat
        .iter()
        .map(|r| {
            cols.iter()
                .map(|c| {
                    r.iter()
                        .find(|(k, _)| k == c)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect();
    (cols, rows)
}

async fn sql_panel(src: &Source, view: &str, sql: &str) -> Panel {
    let mut c = Collect::default();
    match src
        .client
        .sql_query(
            src.database.as_deref(),
            sql,
            None,
            Caps {
                max_rows: 500,
                ..Caps::default()
            },
            &mut c,
        )
        .await
    {
        Ok(()) => {
            let sets = ResultSet::from_events(&c.0);
            let Some(set) = sets.into_iter().find(|s| !s.columns.is_empty()) else {
                return Panel::note(view, "no rows");
            };
            Panel {
                view: view.to_owned(),
                columns: set.columns.iter().map(|c| c.name.clone()).collect(),
                rows: set
                    .rows
                    .iter()
                    .map(|r| {
                        set.columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| cell_text(&r.get(i).cloned().flatten(), c))
                            .collect()
                    })
                    .collect(),
                note: None,
            }
        }
        Err(e) => Panel::note(view, e.to_string()),
    }
}

async fn admin_panel(src: &Source, view: &str, paths: &[(&str, Option<&str>, &[&str])]) -> Panel {
    let Some(a) = &src.admin else {
        return Panel::note(
            view,
            "operators only: add admin_url (and admin_token_file) to the profile",
        );
    };
    let mut panel = Panel {
        view: view.to_owned(),
        ..Panel::default()
    };
    let mut notes = Vec::new();
    for (path, key, prefer) in paths {
        match a.get(path).await {
            Ok(v) => {
                let (cols, rows) = json_table(&v, *key, prefer, 9);
                if panel.columns.is_empty() {
                    panel.columns = cols;
                    panel.rows = rows;
                } else if !rows.is_empty() {
                    // A second list: a separator row, then its rows under
                    // the first list's columns, position by position.
                    panel.rows.push(vec![format!("── {path}")]);
                    panel.rows.push(cols);
                    panel.rows.extend(rows);
                }
            }
            Err(e) => notes.push(format!("{path}: {e}")),
        }
    }
    if !notes.is_empty() {
        panel.note = Some(notes.join("; "));
    }
    panel
}

/// Reads one view.
#[allow(clippy::too_many_lines)]
pub async fn fetch(src: &Source, view: &str) -> Panel {
    match view {
        "sessions" => sql_panel(src, view, "SHOW FULL PROCESSLIST").await,
        "fingerprints" => match src.client.fingerprints(1, 50).await {
            Ok(v) => {
                let (columns, rows) = json_table(
                    &v,
                    Some("fingerprints"),
                    &[
                        "id", "runs", "errors", "total_ms", "p50_ms", "p95_ms", "max_ms", "text",
                    ],
                    9,
                );
                Panel {
                    view: view.to_owned(),
                    columns,
                    rows,
                    note: None,
                }
            }
            Err(e) => Panel::note(view, e.to_string()),
        },
        "clusters" => {
            admin_panel(
                src,
                view,
                &[(
                    "/admin/v1/clusters",
                    Some("clusters"),
                    &[
                        "record.id",
                        "record.state",
                        "record.topology",
                        "health_report.writer",
                        "health_report.max_lag_ms",
                    ],
                )],
            )
            .await
        }
        "shards" => {
            let tenant_path = src
                .tenant
                .as_ref()
                .map(|t| format!("/admin/v1/tenants/{}/shards", crate::api::encode(t)));
            let mut paths: Vec<(&str, Option<&str>, &[&str])> = Vec::new();
            if let Some(p) = &tenant_path {
                paths.push((
                    p.as_str(),
                    None,
                    &["group", "shard", "cluster", "size_bytes", "rows"],
                ));
            }
            paths.push((
                "/admin/v1/mover/jobs",
                None,
                &["id", "tenant", "state", "phase", "progress"],
            ));
            admin_panel(src, view, &paths).await
        }
        "dcs" => {
            admin_panel(
                src,
                view,
                &[
                    (
                        "/admin/v1/dcs",
                        None,
                        &["id", "name", "state", "lease", "leader"],
                    ),
                    (
                        "/admin/v1/failovers:all",
                        None,
                        &["tenant", "state", "from", "to"],
                    ),
                ],
            )
            .await
        }
        "messaging" => {
            if src.database.is_none() {
                return Panel::note(
                    view,
                    "choose a database (USE db) to see its streams and sinks",
                );
            }
            let mut p = sql_panel(src, view, "SHOW STREAMS").await;
            let s = sql_panel(src, view, "SHOW SINK STATUS").await;
            if !s.rows.is_empty() {
                p.rows.push(vec!["── sinks".to_owned()]);
                p.rows.push(s.columns.clone());
                p.rows.extend(s.rows);
            }
            if p.note.is_some() && s.note.is_none() && p.rows.is_empty() {
                p.note = None;
            }
            p
        }
        other => Panel::note(other, "unknown view"),
    }
}

/// Keeps the previous poll to show rates: per row key (the first column),
/// the change of `counter` per second.
#[derive(Debug, Default)]
pub struct Rates {
    prev: BTreeMap<String, (f64, std::time::Instant)>,
}

impl Rates {
    /// Adds a `<counter>/s` column after `counter`.
    pub fn apply(&mut self, p: &mut Panel, counter: &str) {
        let Some(ci) = p.columns.iter().position(|c| c == counter) else {
            return;
        };
        let now = std::time::Instant::now();
        p.columns.insert(ci + 1, format!("{counter}/s"));
        for r in &mut p.rows {
            let key = r.first().cloned().unwrap_or_default();
            let val: Option<f64> = r.get(ci).and_then(|v| v.parse().ok());
            let rate = match (val, self.prev.get(&key)) {
                (Some(v), Some((pv, pt))) => {
                    let dt = now.duration_since(*pt).as_secs_f64();
                    if dt > 0.0 {
                        format!("{:.1}", (v - pv).max(0.0) / dt)
                    } else {
                        String::new()
                    }
                }
                _ => String::new(),
            };
            if let Some(v) = val {
                self.prev.insert(key, (v, now));
            }
            if r.len() > ci {
                r.insert(ci + 1, rate);
            }
        }
    }
}

/// The source for a profile.
///
/// # Errors
/// Sign-in errors.
pub async fn source(ctx: &Ctx) -> Result<Source> {
    let runner = ctx.connect(Protocol::Http, Caps::default()).await?;
    Ok(Source {
        client: runner.client,
        database: ctx.profile.database.clone(),
        admin: AdminApi::of(&ctx.profile)?,
        tenant: ctx.profile.tenant.clone(),
    })
}

fn print_panel(p: &Panel) {
    println!("== {}", p.view);
    if let Some(n) = &p.note {
        println!("({n})");
    }
    if p.columns.is_empty() {
        return;
    }
    let mut w: Vec<usize> = p.columns.iter().map(String::len).collect();
    for r in &p.rows {
        for (i, c) in r.iter().enumerate() {
            if i < w.len() {
                w[i] = w[i].max(c.chars().count().min(40));
            }
        }
    }
    let line = |r: &[String]| {
        r.iter()
            .enumerate()
            .map(|(i, c)| {
                format!(
                    "{:<w$}",
                    crate::tui::clip(c, 40),
                    w = w.get(i).copied().unwrap_or(0)
                )
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    println!("{}", line(&p.columns));
    for r in &p.rows {
        println!("{}", line(r));
    }
}

/// `sprig top`.
pub async fn cli(ctx: Ctx, m: &ArgMatches) -> ExitCode {
    if !m.get_flag("once") {
        return crate::tui::run_dashboard(ctx).await;
    }
    let src = match source(&ctx).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("sprig: {e}");
            return ExitCode::from(e.class().code());
        }
    };
    let views: Vec<&str> = match m.get_one::<String>("view") {
        Some(v) => vec![v.as_str()],
        None => VIEW_NAMES.to_vec(),
    };
    let mut out = Map::new();
    for v in views {
        let p = fetch(&src, v).await;
        if m.get_flag("json") {
            out.insert(v.to_owned(), p.to_json());
        } else {
            print_panel(&p);
        }
    }
    if m.get_flag("json") {
        println!(
            "{}",
            serde_json::to_string_pretty(&Value::Object(out)).unwrap_or_default()
        );
    }
    ExitCode::SUCCESS
}

/// `KILL [QUERY] id` for the sessions view.
///
/// # Errors
/// The server's error.
pub async fn kill(src: &Source, id: &str, query_only: bool) -> Result<()> {
    if !id.chars().all(|c| c.is_ascii_digit()) || id.is_empty() {
        return Err(ShellError::server(
            0,
            "",
            format!("not a session id: {id:?}"),
        ));
    }
    let sql = if query_only {
        format!("KILL QUERY {id}")
    } else {
        format!("KILL {id}")
    };
    src.client
        .sql_query(
            src.database.as_deref(),
            &sql,
            None,
            Caps::default(),
            &mut Collect::default(),
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_from_json() {
        let v = json!({"clusters": [
            {"record": {"id": "c1", "state": "active"}, "health_report": {"writer": "n1"}, "nodes": [1, 2]},
            {"record": {"id": "c2", "state": "draining"}}
        ]});
        let (c, r) = json_table(
            &v,
            Some("clusters"),
            &["record.id", "health_report.writer"],
            4,
        );
        assert_eq!(
            c,
            ["record.id", "health_report.writer", "record.state", "nodes"]
        );
        assert_eq!(r[0], ["c1", "n1", "active", "[2]"]);
        assert_eq!(r[1], ["c2", "", "draining", ""]);
        let p = Panel {
            view: "x".into(),
            columns: vec!["a".into()],
            rows: vec![vec!["1".into()]],
            note: None,
        };
        assert_eq!(p.to_json(), json!({"rows": [{"a": "1"}]}));
    }

    #[test]
    fn rates() {
        let mut r = Rates::default();
        let mut p = Panel {
            view: "f".into(),
            columns: vec!["id".into(), "runs".into()],
            rows: vec![vec!["a".into(), "10".into()]],
            note: None,
        };
        r.apply(&mut p, "runs");
        assert_eq!(p.columns, ["id", "runs", "runs/s"]);
        assert_eq!(p.rows[0], ["a", "10", ""]);
    }
}
