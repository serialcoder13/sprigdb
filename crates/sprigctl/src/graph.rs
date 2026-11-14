//! `sprigctl graph list|show|partition`, over
//! the Admin API's `/admin/v1/tenants/{name}/graphs`.
//!
//! ```text
//! sprigctl graph list acme
//! sprigctl graph show acme social.friends
//! sprigctl graph partition acme social.friends
//! ```

use clap::{Arg, ArgMatches, Command};
use serde_json::Value;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl graph`.
pub(crate) fn group() -> Command {
    let tenant = || Arg::new("tenant").required(true).help("The tenant");
    let graph = || {
        Arg::new("graph")
            .required(true)
            .help("The graph, as database.graph")
    };
    Command::new("graph")
        .about("Graphs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("A tenant's graphs")
                .arg(tenant()),
        )
        .subcommand(
            Command::new("show")
                .about("One graph: labels, types, indexes, shards and row counts")
                .arg(tenant())
                .arg(graph()),
        )
        .subcommand(
            Command::new("partition")
                .about("How a graph is partitioned, and the share of relationships crossing shards")
                .arg(tenant())
                .arg(graph()),
        )
}

fn s(v: &Value, k: &str) -> String {
    match &v[k] {
        Value::String(x) => x.clone(),
        Value::Null => "-".into(),
        x => x.to_string(),
    }
}

fn list_text(v: &Value) -> String {
    let rows: Vec<[String; 7]> = v
        .as_array()
        .map(|a| {
            a.iter()
                .map(|g| {
                    [
                        s(g, "database"),
                        s(g, "name"),
                        s(g, "graph_id"),
                        s(g, "kind"),
                        s(g, "state"),
                        s(g, "group"),
                        s(g, "shards"),
                    ]
                })
                .collect()
        })
        .unwrap_or_default();
    table(
        [
            "DATABASE", "GRAPH", "ID", "KIND", "STATE", "GROUP", "SHARDS",
        ],
        &rows,
    )
}

fn show_text(v: &Value) -> String {
    let g = &v["graph"];
    let names = |k: &str| {
        v[k].as_array()
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    };
    let mut out = format!(
        "Graph {}.{} (id {}, {}, {}), group {}\nLabels: {}\nRelationship types: {}\n",
        s(g, "database"),
        s(g, "name"),
        s(g, "graph_id"),
        s(g, "kind"),
        s(g, "state"),
        s(g, "group"),
        names("labels"),
        names("types"),
    );
    if let Some(m) = v["mirror"].as_str() {
        out.push_str("Mirrors: ");
        out.push_str(m);
        out.push('\n');
    }
    let idx: Vec<[String; 6]> = v["indexes"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|i| {
                    [
                        s(i, "name"),
                        if i["unique"] == Value::Bool(true) {
                            "constraint".into()
                        } else {
                            s(i, "kind")
                        },
                        s(i, "entity"),
                        s(i, "token"),
                        i["properties"]
                            .as_array()
                            .map(|p| {
                                p.iter()
                                    .filter_map(Value::as_str)
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            })
                            .unwrap_or_default(),
                        s(i, "state"),
                    ]
                })
                .collect()
        })
        .unwrap_or_default();
    if !idx.is_empty() {
        out.push_str(&table(
            [
                "INDEX",
                "KIND",
                "ON",
                "LABEL OR TYPE",
                "PROPERTIES",
                "STATE",
            ],
            &idx,
        ));
    }
    let shards: Vec<[String; 6]> = v["shards"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|x| {
                    [
                        s(x, "shard"),
                        s(x, "cluster"),
                        s(x, "nodes"),
                        s(x, "relationships"),
                        s(x, "mirrors"),
                        s(x, "claims"),
                    ]
                })
                .collect()
        })
        .unwrap_or_default();
    if !shards.is_empty() {
        out.push_str(&table(
            [
                "SHARD",
                "CLUSTER",
                "NODES",
                "RELATIONSHIPS",
                "MIRRORS",
                "CLAIMS",
            ],
            &shards,
        ));
    }
    out
}

fn partition_text(v: &Value) -> String {
    let g = &v["graph"];
    format!(
        "Graph {}.{}: {} shard(s), partitioned by {}\n{} of {} relationships cross shards ({}%)\n{}\n",
        s(g, "database"),
        s(g, "name"),
        s(g, "shards"),
        s(v, "partition_by"),
        s(v, "cross_shard"),
        s(v, "relationships"),
        s(v, "cross_shard_pct"),
        s(v, "advice"),
    )
}

/// Runs `sprigctl graph <cmd>`.
pub(crate) async fn run(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Err(Failed("graph: a subcommand is needed".into()));
    };
    let tenant = m.get_one::<String>("tenant").cloned().unwrap_or_default();
    let base = format!("/admin/v1/tenants/{}/graphs", escape(&tenant));
    let graph = || escape(&m.get_one::<String>("graph").cloned().unwrap_or_default());
    match cmd {
        "list" => {
            let v: Value = c.get(&base).await?;
            print(out, &v, list_text);
        }
        "show" => {
            let v: Value = c.get(&format!("{base}/{}", graph())).await?;
            print(out, &v, show_text);
        }
        "partition" => {
            let v: Value = c.get(&format!("{base}/{}/partition", graph())).await?;
            print(out, &v, partition_text);
        }
        other => return Err(Failed(format!("graph {other}: unknown subcommand"))),
    }
    Ok(())
}
