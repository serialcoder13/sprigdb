//! `sprigctl`: the operator command-line tool.
//!
//! Layer 4 (binaries). Design: the writing guide's 5.10 (command groups),
//! chapter 47 (Operations, "sprigctl reference") and chapter 09 (Clusters,
//! "Interfaces") of the SprigDB low-level design.
//!
//! Implemented, over the Admin API (chapter 37), never etcd:
//! - `sprigctl dc register|list|show` (chapter 23 step 7);
//! - `sprigctl cluster register|list|show|probe|drain` (chapter 09 step 9,
//!   chapter 27 step 1).
//!
//! Every other command group parses and prints "not implemented yet",
//! exiting with status 1.
//!
//! The Admin API is served by the DC's controllers on
//! `[controller] admin_addr` (default `http://127.0.0.1:7480`) until the
//! gateway's 8443 listener serves it; `--endpoint` or `SPRIGCTL_ENDPOINT`
//! names it, and `--token-file` or `SPRIGCTL_TOKEN_FILE` its bearer token.

mod client;
mod cluster;
mod render;

use std::io::{BufRead, IsTerminal, Write};
use std::process::ExitCode;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde::Serialize;
use sprig_controller::admin::types::{
    ClusterView, GetDcResponse, ListClustersResponse, ListDcsResponse, ProbeClusterRequest,
    ProbeClusterResponse, RegisterClusterResponse, RegisterDcRequest, RegisterDcResponse,
};
use sprig_controller::dc;

use crate::client::{AdminClient, CallError, DEFAULT_ENDPOINT};
use crate::cluster::RegisterFlags;

/// Command groups not implemented yet, and their subcommands (writing
/// guide, 5.10).
const GROUPS: &[(&str, &str, &[&str])] = &[
    (
        "tenant",
        "Tenants",
        &[
            "create", "list", "show", "move", "pin", "unpin", "failover", "settings",
        ],
    ),
    ("shard", "Shards", &["list", "split", "merge", "move"]),
    (
        "index",
        "Automatic indexes",
        &["list", "show", "drop", "freeze"],
    ),
    ("job", "Jobs", &["list", "show", "cancel"]),
    (
        "explain",
        "Explain a query fingerprint, a decision or a tenant's placement",
        &["query", "decision", "placement"],
    ),
    (
        "config",
        "Policy settings, with --scope global|dc:<id>|cluster:<id>|tenant:<name>|db:<tenant>.<db>|object:<tenant>.<db>.<object>",
        &["get", "set", "unset", "explain"],
    ),
    ("feed", "Change feeds", &["list", "show", "lag"]),
    ("search", "Search indexes", &["list", "show", "rebuild"]),
    (
        "logic",
        "Logic functions",
        &["deploy", "list", "show", "logs", "rollback", "dead-letters"],
    ),
    (
        "space",
        "Spaces and shares",
        &["create", "list", "show", "share", "unshare"],
    ),
    (
        "import",
        "Copy data and index definitions from a live source",
        &["mongodb", "mysql"],
    ),
];

/// `sprigctl dc`.
fn dc_group() -> Command {
    let dc_arg = || {
        Arg::new("id")
            .required(true)
            .help("Data center id, such as ams1")
    };
    Command::new("dc")
        .about("Data centers")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("register")
                .about("Register a DC as joining (the global leader activates it)")
                .arg(dc_arg())
                .arg(
                    Arg::new("tags")
                        .long("tags")
                        .value_delimiter(',')
                        .action(ArgAction::Append)
                        .help("Tags as key=value, comma-separated or repeated"),
                )
                .arg(
                    Arg::new("kind")
                        .long("kind")
                        .value_parser(["full", "witness"])
                        .default_value("full")
                        .help("full or witness"),
                )
                .arg(
                    Arg::new("global-voter")
                        .long("global-voter")
                        .action(ArgAction::SetTrue)
                        .help("A global etcd voter runs in this DC"),
                ),
        )
        .subcommand(Command::new("list").about("List DCs"))
        .subcommand(
            Command::new("show")
                .about("Show a DC and its current leader")
                .arg(dc_arg()),
        )
}

/// `sprigctl cluster`.
fn cluster_group() -> Command {
    let id = || {
        Arg::new("id")
            .required(true)
            .help("Cluster id, such as mum1-c7")
    };
    Command::new("cluster")
        .about("MariaDB clusters")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("register")
                .about("Precheck and register an external cluster (chapter 27)")
                .arg(Arg::new("name").help("A name unique in the DC"))
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .help("A TOML file with dc, name, topology, endpoints or nodes, credentials_ref, capacity, declared"),
                )
                .arg(
                    Arg::new("topology")
                        .long("topology")
                        .value_parser(["galera", "primary_replica", "replication", "single", "single_node", "external"])
                        .help("The cluster's topology"),
                )
                .arg(
                    Arg::new("node")
                        .long("node")
                        .action(ArgAction::Append)
                        .help("A node as [id=]host:port; repeat for each, the writer first"),
                )
                .arg(
                    Arg::new("credentials")
                        .long("credentials")
                        .help("Credentials reference with [sprig_ctl] and [sprig_repl] tables: file:<path> or env:<name>"),
                )
                .arg(Arg::new("ctl-secret").long("ctl-secret").help("sprig_ctl's credential reference"))
                .arg(Arg::new("repl-secret").long("repl-secret").help("sprig_repl's credential reference"))
                .arg(Arg::new("declare").long("declare").help("A TOML file of [declared] facts"))
                .arg(
                    Arg::new("memory-gb")
                        .long("memory-gb")
                        .value_parser(clap::value_parser!(u64))
                        .help("Server memory, GB (for tuning)"),
                )
                .arg(
                    Arg::new("storage-gb")
                        .long("storage-gb")
                        .value_parser(clap::value_parser!(u64))
                        .help("Storage, GB (for placement)"),
                ),
        )
        .subcommand(Command::new("list").about("List the DC's clusters"))
        .subcommand(
            Command::new("show")
                .about("Show a cluster: nodes, health, capability profile, gaps")
                .arg(id()),
        )
        .subcommand(
            Command::new("probe")
                .about("Probe a cluster now and publish what changed")
                .arg(id())
                .arg(
                    Arg::new("full")
                        .long("full")
                        .action(ArgAction::SetTrue)
                        .help("Also run the XA, feature and routine probes"),
                ),
        )
        .subcommand(
            Command::new("drain")
                .about("Stop placing on a cluster and move its shards away")
                .arg(id()),
        )
}

fn cli() -> Command {
    let mut cmd = Command::new("sprigctl")
        .about("SprigDB operator CLI")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("endpoint")
                .long("endpoint")
                .env("SPRIGCTL_ENDPOINT")
                .global(true)
                .help("Admin API endpoint [default: http://127.0.0.1:7480]"),
        )
        .arg(
            Arg::new("token-file")
                .long("token-file")
                .env("SPRIGCTL_TOKEN_FILE")
                .global(true)
                .help("A file holding the Admin API bearer token"),
        )
        .arg(
            Arg::new("dc")
                .long("dc")
                .global(true)
                .help("Data center to act in"),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .global(true)
                .value_parser(["table", "json", "yaml"])
                .default_value("table")
                .help("Output format"),
        )
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .global(true)
                .action(ArgAction::SetTrue)
                .help("Show what would change"),
        )
        .arg(
            Arg::new("yes")
                .long("yes")
                .global(true)
                .action(ArgAction::SetTrue)
                .help("Don't ask for confirmation"),
        )
        .subcommand(dc_group())
        .subcommand(cluster_group());
    for &(group, about, subs) in GROUPS {
        let mut g = Command::new(group)
            .about(about)
            .subcommand_required(true)
            .arg_required_else_help(true);
        for &sub in subs {
            let mut s = Command::new(sub).arg(
                Arg::new("args")
                    .num_args(0..)
                    .trailing_var_arg(true)
                    .allow_hyphen_values(true)
                    .help("Arguments (not parsed yet)"),
            );
            if group == "config" {
                s = s.arg(
                    Arg::new("scope")
                        .long("scope")
                        .help("global, dc:<id>, cluster:<id>, tenant:<name>, db:<tenant>.<db> or object:<tenant>.<db>.<object>"),
                );
            }
            g = g.subcommand(s);
        }
        cmd = cmd.subcommand(g);
    }
    cmd
}

/// A failed command: what to print.
#[derive(Debug)]
struct Failed(String);

impl From<CallError> for Failed {
    fn from(e: CallError) -> Self {
        Failed(e.to_string())
    }
}

impl From<String> for Failed {
    fn from(e: String) -> Self {
        Failed(e)
    }
}

fn main() -> ExitCode {
    let matches = cli().get_matches();
    let Some((group, sub)) = matches.subcommand() else {
        return ExitCode::FAILURE;
    };
    let name = sub.subcommand_name().unwrap_or_default().to_owned();
    if group != "dc" && group != "cluster" {
        eprintln!("sprigctl {group} {name}: not implemented yet");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("sprigctl: can't start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let r = runtime.block_on(async {
        let client = client(&matches)?;
        if group == "dc" {
            run_dc(&client, &matches, sub).await
        } else {
            run_cluster(&client, &matches, sub).await
        }
    });
    match r {
        Ok(code) => code,
        Err(Failed(e)) => {
            eprintln!("sprigctl {group} {name}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn client(m: &ArgMatches) -> Result<AdminClient, Failed> {
    let endpoint = m
        .get_one::<String>("endpoint")
        .map_or(DEFAULT_ENDPOINT, String::as_str);
    let token = match m.get_one::<String>("token-file") {
        Some(p) => Some(
            std::fs::read_to_string(p)
                .map_err(|e| format!("--token-file {p}: {e}"))?
                .trim()
                .to_owned(),
        ),
        None => None,
    };
    Ok(AdminClient::new(endpoint, token)?)
}

/// Output formats. YAML is printed as JSON, which every YAML 1.2 parser
/// reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Output {
    Table,
    Json,
}

fn output(m: &ArgMatches) -> Output {
    match m.get_one::<String>("output").map(String::as_str) {
        Some("json" | "yaml") => Output::Json,
        _ => Output::Table,
    }
}

/// Prints `v` as JSON, or `text(v)`.
fn print<T: Serialize>(out: Output, v: &T, text: impl FnOnce(&T) -> String) {
    match out {
        Output::Json => println!("{}", serde_json::to_string_pretty(v).unwrap_or_default()),
        Output::Table => print!("{}", text(v)),
    }
}

async fn run_dc(c: &AdminClient, m: &ArgMatches, group: &ArgMatches) -> Result<ExitCode, Failed> {
    let out = output(m);
    let Some((cmd, sub)) = group.subcommand() else {
        return Ok(ExitCode::FAILURE);
    };
    let id = || sub.get_one::<String>("id").cloned().unwrap_or_default();
    match cmd {
        "register" => {
            let tags = dc::parse_tags(
                sub.get_many::<String>("tags")
                    .into_iter()
                    .flatten()
                    .map(String::as_str),
            )
            .map_err(|e| e.message)?;
            let req = RegisterDcRequest {
                dc: sprig_common::ids::DcId::new(id().as_str()).map_err(|e| e.to_string())?,
                kind: sub
                    .get_one::<String>("kind")
                    .cloned()
                    .unwrap_or_else(|| "full".into()),
                tags,
                global_voter: sub.get_flag("global-voter"),
            };
            if m.get_flag("dry-run") {
                println!(
                    "would register {} ({}) as joining, tags {}",
                    req.dc,
                    req.kind,
                    serde_json::Value::Object(req.tags.clone())
                );
                return Ok(ExitCode::SUCCESS);
            }
            let r: RegisterDcResponse = c.post("/admin/v1/dcs", &req).await?;
            if !r.created {
                eprintln!(
                    "dc {} is already registered ({})",
                    r.record.dc_id, r.record.state
                );
            }
            print(out, &[r.record], |r| render::dc_list(r));
            Ok(if r.created {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        "list" => {
            let r: ListDcsResponse = c.get("/admin/v1/dcs").await?;
            print(out, &r.dcs, |d| render::dc_list(d));
            Ok(ExitCode::SUCCESS)
        }
        "show" => {
            let r: GetDcResponse = c.get(&format!("/admin/v1/dcs/{}", id())).await?;
            print(out, &r, render::dc_show);
            Ok(ExitCode::SUCCESS)
        }
        _ => Ok(ExitCode::FAILURE),
    }
}

fn read(path: &str) -> Result<String, Failed> {
    std::fs::read_to_string(path).map_err(|e| Failed(format!("{path}: {e}")))
}

fn register_flags(m: &ArgMatches, sub: &ArgMatches) -> Result<RegisterFlags, Failed> {
    let s = |n: &str| sub.get_one::<String>(n).cloned();
    Ok(RegisterFlags {
        file: s("file").map(|p| read(&p)).transpose()?,
        declare: s("declare").map(|p| read(&p)).transpose()?,
        name: s("name"),
        dc: m.get_one::<String>("dc").cloned(),
        topology: s("topology"),
        nodes: sub
            .get_many::<String>("node")
            .into_iter()
            .flatten()
            .cloned()
            .collect(),
        credentials: s("credentials"),
        ctl_secret: s("ctl-secret"),
        repl_secret: s("repl-secret"),
        memory_gb: sub.get_one::<u64>("memory-gb").copied(),
        storage_gb: sub.get_one::<u64>("storage-gb").copied(),
        dry_run: m.get_flag("dry-run"),
    })
}

/// Asks for the cluster id unless `--yes`.
fn confirm(m: &ArgMatches, what: &str, id: &str) -> Result<(), Failed> {
    if m.get_flag("yes") {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(Failed(format!(
            "{what} {id} needs --yes when not run interactively"
        )));
    }
    print!("{what} {id}? Type the cluster id to confirm: ");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .map_err(|e| e.to_string())?;
    if line.trim() == id {
        Ok(())
    } else {
        Err(Failed("not confirmed".into()))
    }
}

async fn run_cluster(
    c: &AdminClient,
    m: &ArgMatches,
    group: &ArgMatches,
) -> Result<ExitCode, Failed> {
    let out = output(m);
    let Some((cmd, sub)) = group.subcommand() else {
        return Ok(ExitCode::FAILURE);
    };
    let id = || sub.get_one::<String>("id").cloned().unwrap_or_default();
    match cmd {
        "register" => {
            let req = cluster::build(&register_flags(m, sub)?)?;
            let dry = req.dry_run;
            let r: RegisterClusterResponse = c.post("/admin/v1/clusters", &req).await?;
            let passed = r.precheck.passed();
            print(out, &r, |r| {
                let mut s = render::precheck(&r.precheck);
                if let Some(v) = &r.cluster {
                    s.push_str(&render::cluster_show(v));
                } else if dry {
                    s.push_str("dry run: nothing registered\n");
                }
                s
            });
            Ok(if passed {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        "list" => {
            let r: ListClustersResponse = c.get("/admin/v1/clusters").await?;
            print(out, &r, |r| render::cluster_list(&r.clusters));
            Ok(ExitCode::SUCCESS)
        }
        "show" => {
            let r: ClusterView = c.get(&format!("/admin/v1/clusters/{}", id())).await?;
            print(out, &r, render::cluster_show);
            Ok(ExitCode::SUCCESS)
        }
        "probe" => {
            let req = ProbeClusterRequest {
                full: sub.get_flag("full"),
            };
            let r: ProbeClusterResponse = c
                .post(&format!("/admin/v1/clusters/{}:probe", id()), &req)
                .await?;
            print(out, &r, render::probe);
            Ok(ExitCode::SUCCESS)
        }
        "drain" => {
            let id = id();
            if m.get_flag("dry-run") {
                println!("would drain {id}: no new placements; its shards move away");
                return Ok(ExitCode::SUCCESS);
            }
            confirm(m, "drain", &id)?;
            let r: ClusterView = c
                .post(
                    &format!("/admin/v1/clusters/{id}:drain"),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &r, render::cluster_show);
            Ok(ExitCode::SUCCESS)
        }
        _ => Ok(ExitCode::FAILURE),
    }
}

#[cfg(test)]
mod tests;
