//! `sprigctl gateway list|show|drain` (the Admin API's
//! `Gateways` service): the DC's registered gateways, and draining one.
//! Any gateway answers; a drain reaches the named gateway through the peer
//! service, which then finishes its work, closes its listeners and exits.

use clap::{Arg, ArgMatches, Command};
use serde_json::Value;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl gateway`.
pub(crate) fn group() -> Command {
    let id = || Arg::new("id").required(true).help("Gateway id ([node] id)");
    Command::new("gateway")
        .about("Gateways of the DC")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("List the registered gateways"))
        .subcommand(
            Command::new("show")
                .about("Show one gateway's record")
                .arg(id()),
        )
        .subcommand(
            Command::new("drain")
                .about("Drain a gateway: readiness off, listeners closed, work finished, exit")
                .arg(id()),
        )
}

fn row(g: &Value) -> [String; 6] {
    let s = |k: &str| {
        g.get(k).map_or_else(String::new, |v| {
            v.as_str().map_or_else(|| v.to_string(), str::to_owned)
        })
    };
    let listeners = g["listeners"].as_object().map_or_else(String::new, |m| {
        m.iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join(" ")
    });
    [
        s("gateway_id"),
        s("slot"),
        s("state"),
        s("addr"),
        format!("{}/{}", g["load"]["connections"], g["load"]["active"]),
        listeners,
    ]
}

/// Runs `sprigctl gateway <cmd>`.
pub(crate) async fn run(c: &AdminClient, group: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("expected a subcommand".into()));
    };
    let id = || sub.get_one::<String>("id").cloned().unwrap_or_default();
    match cmd {
        "list" => {
            let v: Value = c.get("/admin/v1/gateways").await?;
            print(out, &v, |v| {
                let rows: Vec<[String; 6]> = v["gateways"]
                    .as_array()
                    .map(|a| a.iter().map(row).collect())
                    .unwrap_or_default();
                table(
                    [
                        "GATEWAY",
                        "SLOT",
                        "STATE",
                        "ADDR",
                        "CONNS/ACTIVE",
                        "LISTENERS",
                    ],
                    &rows,
                )
            });
        }
        "show" => {
            let v: Value = c
                .get(&format!("/admin/v1/gateways/{}", escape(&id())))
                .await?;
            print(out, &v, |v| {
                table(
                    [
                        "GATEWAY",
                        "SLOT",
                        "STATE",
                        "ADDR",
                        "CONNS/ACTIVE",
                        "LISTENERS",
                    ],
                    &[row(v)],
                )
            });
        }
        "drain" => {
            let v: Value = c
                .post(
                    &format!("/admin/v1/gateways/{}:drain", escape(&id())),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &v, |v| {
                if v["started"].as_bool().unwrap_or(false) {
                    format!("gateway {} is draining\n", id())
                } else {
                    format!("gateway {} was already draining\n", id())
                }
            });
        }
        other => return Err(Failed(format!("unknown subcommand {other}"))),
    }
    Ok(())
}
