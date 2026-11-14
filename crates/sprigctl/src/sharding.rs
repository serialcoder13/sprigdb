//! `sprigctl shard list|split|merge|move` for table groups and
//! `sprigctl advisor run|show|apply`, over the Admin API's `/admin/v1/tenants/{name}/shards` and
//! `/admin/v1/tenants/{name}/advisor`.
//!
//! ```text
//! sprigctl shard list acme
//! sprigctl shard split acme 3 4 [--at 0x9000000000000000] [--ways 3]
//! sprigctl shard merge acme 3 6 7
//! sprigctl shard move acme 3 4 --to dc-a-c2      # one shard of group 3
//! sprigctl shard move acme --to dc-a-c2          # the whole tenant
//! sprigctl shard rekey acme 3 --key customer_id [--member events=cust] [--shards 4] [--to dc-a-c2]
//! sprigctl advisor run acme
//! sprigctl advisor show acme [run]
//! sprigctl advisor apply acme <run>
//! ```

use clap::{Arg, ArgMatches, Command};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_mover::JobStatus;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// One range, as the Admin API returns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardRow {
    shard: u32,
    lo: String,
    hi: Option<String>,
    cluster: String,
    state: String,
    epoch: u64,
    job: Option<String>,
}

/// One group, as the Admin API returns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupView {
    group: u32,
    name: String,
    state: String,
    map_version: u64,
    shards: Vec<ShardRow>,
}

/// `sprigctl shard split|merge` (the subcommands `shard_group` adds).
pub(crate) fn shard_subcommands(cmd: Command) -> Command {
    let tenant = || Arg::new("tenant").required(true).help("The tenant");
    let group = || Arg::new("group").required(true).help("The table group id");
    cmd.subcommand(
        Command::new("list")
            .about("List a tenant's table groups and their shards")
            .arg(tenant()),
    )
    .subcommand(
        Command::new("split")
            .about("Split a shard of a table group")
            .arg(tenant())
            .arg(group())
            .arg(Arg::new("shard").required(true).help("The shard id"))
            .arg(
                Arg::new("at")
                    .long("at")
                    .help("The split point, a hash such as 0x9000000000000000"),
            )
            .arg(
                Arg::new("ways")
                    .long("ways")
                    .help("Children at the size histogram's quantiles (default 2)"),
            ),
    )
    .subcommand(
        Command::new("merge")
            .about("Merge two adjacent shards of a table group")
            .arg(tenant())
            .arg(group())
            .arg(Arg::new("left").required(true).help("The lower shard"))
            .arg(Arg::new("right").required(true).help("The upper shard")),
    )
    .subcommand(
        Command::new("rekey")
            .about(
                "Re-key a table group online: move its rows to new shards routed by another column",
            )
            .arg(tenant())
            .arg(group())
            .arg(
                Arg::new("key")
                    .long("key")
                    .required(true)
                    .help("The new key's column, in every member"),
            )
            .arg(
                Arg::new("member")
                    .long("member")
                    .action(clap::ArgAction::Append)
                    .help("table=column: a member's own column for the new key (repeatable)"),
            )
            .arg(
                Arg::new("shards")
                    .long("shards")
                    .help("How many new shards (default: as many as now, at least 2)"),
            )
            .arg(
                Arg::new("to")
                    .long("to")
                    .help("The cluster for the new shards (default: the group's)"),
            ),
    )
}

/// `sprigctl advisor`.
pub(crate) fn advisor_group() -> Command {
    let tenant = || Arg::new("tenant").required(true).help("The tenant");
    Command::new("advisor")
        .about("The sharding advisor")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("run")
                .about("Run the advisor now for a tenant without table groups")
                .arg(tenant()),
        )
        .subcommand(
            Command::new("show")
                .about("Show the newest advisor run, or one run")
                .arg(tenant())
                .arg(Arg::new("run").help("The run id")),
        )
        .subcommand(
            Command::new("apply")
                .about("Apply a run's design (a recommend or wait verdict)")
                .arg(tenant())
                .arg(Arg::new("run").required(true).help("The run id")),
        )
}

fn num<T: std::str::FromStr>(m: &ArgMatches, name: &str) -> Result<Option<T>, Failed> {
    m.get_one::<String>(name)
        .map(|v| {
            v.parse::<T>()
                .map_err(|_| Failed(format!("--{name} {v}: not a number")))
        })
        .transpose()
}

fn hash(v: &str) -> Result<u64, Failed> {
    let t = v.trim();
    let r = match t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        Some(h) => u64::from_str_radix(h, 16),
        None => t.parse::<u64>(),
    };
    r.map_err(|_| Failed(format!("{v}: not a 64-bit hash")))
}

fn list_text(gs: &Vec<GroupView>) -> String {
    let mut rows = Vec::new();
    for g in gs {
        for s in &g.shards {
            rows.push([
                g.group.to_string(),
                g.name.clone(),
                g.state.clone(),
                s.shard.to_string(),
                s.lo.clone(),
                s.hi.clone().unwrap_or_else(|| "2^64".to_owned()),
                s.cluster.clone(),
                s.state.clone(),
                s.epoch.to_string(),
            ]);
        }
        if g.shards.is_empty() {
            rows.push([
                g.group.to_string(),
                g.name.clone(),
                g.state.clone(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "no map".into(),
                "-".into(),
            ]);
        }
    }
    table(
        [
            "GROUP",
            "NAME",
            "GROUP STATE",
            "SHARD",
            "FROM",
            "TO",
            "CLUSTER",
            "STATE",
            "EPOCH",
        ],
        &rows,
    )
}

fn job_text(j: &JobStatus) -> String {
    format!(
        "job {} ({}) of tenant {}: {}\n",
        j.id.0,
        j.kind.as_str(),
        j.tenant,
        j.state
    )
}

fn pretty(v: &Value) -> String {
    format!("{}\n", serde_json::to_string_pretty(v).unwrap_or_default())
}

/// `sprigctl shard list|split|merge` and `shard move <t> <group> <shard>`.
/// `Ok(false)` when the command isn't one of these (the whole-tenant
/// `shard move`).
pub(crate) async fn run_shard(
    c: &AdminClient,
    sub: &ArgMatches,
    out: Output,
) -> Result<bool, Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Ok(false);
    };
    let tenant = || m.get_one::<String>("tenant").cloned().unwrap_or_default();
    let path = || format!("/admin/v1/tenants/{}/shards", escape(&tenant()));
    let body = match cmd {
        "list" => {
            let gs: Vec<GroupView> = c.get(&path()).await?;
            print(out, &gs, list_text);
            return Ok(true);
        }
        "split" => {
            let at = m.get_one::<String>("at").map(|v| hash(v)).transpose()?;
            json!({
                "group": num::<u32>(m, "group")?, "kind": "split",
                "shard": num::<u32>(m, "shard")?, "at": at, "ways": num::<u32>(m, "ways")?,
            })
        }
        "merge" => json!({
            "group": num::<u32>(m, "group")?, "kind": "merge",
            "left": num::<u32>(m, "left")?, "right": num::<u32>(m, "right")?,
        }),
        "rekey" => {
            let mut members = serde_json::Map::new();
            for v in m.get_many::<String>("member").into_iter().flatten() {
                let Some((t, col)) = v.split_once('=') else {
                    return Err(Failed(format!("--member {v}: expected table=column")));
                };
                members.insert(t.trim().to_owned(), json!(col.trim()));
            }
            json!({
                "group": num::<u32>(m, "group")?, "kind": "rekey",
                "column": m.get_one::<String>("key").cloned().unwrap_or_default(),
                "members": members, "shards": num::<u32>(m, "shards")?,
                "to": m.get_one::<String>("to"),
            })
        }
        "move" => {
            let (Some(g), Some(s)) = (num::<u32>(m, "group")?, num::<u32>(m, "shard")?) else {
                return Ok(false);
            };
            json!({
                "group": g, "kind": "move", "shard": s,
                "to": m.get_one::<String>("to").cloned().unwrap_or_default(),
            })
        }
        _ => return Ok(false),
    };
    let j: JobStatus = c.post(&path(), &body).await?;
    print(out, &j, job_text);
    Ok(true)
}

/// `sprigctl advisor`.
pub(crate) async fn run_advisor(
    c: &AdminClient,
    sub: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Err(Failed("missing subcommand".into()));
    };
    let t = escape(&m.get_one::<String>("tenant").cloned().unwrap_or_default());
    let v: Value = match cmd {
        "run" => {
            c.post(&format!("/admin/v1/tenants/{t}/advisor:run"), &json!({}))
                .await?
        }
        "show" => {
            let q = num::<u64>(m, "run")?.map_or(String::new(), |r| format!("?run={r}"));
            c.get(&format!("/admin/v1/tenants/{t}/advisor{q}")).await?
        }
        "apply" => {
            c.post(
                &format!("/admin/v1/tenants/{t}/advisor:apply"),
                &json!({ "run": num::<u64>(m, "run")? }),
            )
            .await?
        }
        other => return Err(Failed(format!("advisor {other}: unknown"))),
    };
    print(out, &v, pretty);
    Ok(())
}
