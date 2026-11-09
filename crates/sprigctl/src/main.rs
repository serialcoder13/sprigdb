//! `sprigctl`: the operator command-line tool.
//!
//! Layer 4 (binaries). Design: the writing guide's 5.10 (command groups),
//! chapter 47 (Operations, "sprigctl reference") and chapter 09 (Clusters,
//! "Interfaces") of the SprigDB low-level design.
//!
//! Implemented, over the Admin API (chapter 37), never etcd:
//! - `sprigctl dc register|list|show` (chapter 23 step 7);
//! - `sprigctl cluster register|list|show|probe|drain` (chapter 09 step 9,
//!   chapter 27 step 1);
//! - `sprigctl tenant create|list|show|delete` (chapter 08 step 9, chapter
//!   23 step 16): `create <name> [--under <parent>] [--cluster <id>]`
//!   bootstraps the tenant's catalog and returns once it is active;
//!   `delete` removes a leaf tenant (it asks for the name unless `--yes`);
//!   `settings <name> [--all]` lists the tenant's setting rows;
//!   `export <name> --to <dir>` and `import <dir> --into <tenant>` copy a
//!   tenant (schemas, rows, users and grants, settings) and verify it
//!   (chapter 47 step 8, see [`export`]);
//!   `user add <tenant> <user> --password ...` creates a user in the
//!   tenant's catalog (interim, until chapter 29's `CREATE USER`);
//! - `sprigctl settings list|describe|get|set|reset|bounds|lock|unlock`
//!   (alias `config`; chapter 08 steps 7-8, chapter 23 step 8, chapter 47
//!   step 5): see where a value comes from and change it at any of the six
//!   scopes (see [`settings`]);
//! - `sprigctl tenant move <tenant> --to <dc>` and `sprigctl tenant
//!   traffic <tenant>` (chapter 24 steps 3 and 4): see [`dcmove`];
//! - `sprigctl shard move <tenant> --to <cluster>` and `sprigctl job
//!   list|show|cancel|cutover` (chapter 10 step 11): see [`mover`];
//! - `sprigctl index list|show|recommendations|approve|reject|drop|pin|unpin`
//!   and `sprigctl explain query <fingerprint>` (chapter 13 step 13): see
//!   [`index`]; `sprigctl explain decision|placement` (chapter 11 step
//!   7): see [`placement`].
//!
//! - `sprigctl logic compile|deploy|list|show` (chapter 41, R7): see
//!   [`logic`].
//! - `sprigctl check no-restart` (chapter 47 step 6): settings, listener
//!   and certificate changes under load with no restart; see [`check`].
//! - `sprigctl txn list|show|resolve` (chapter 18): in-doubt distributed
//!   transactions; see [`txn`].
//!
//! Every other command group parses and prints "not implemented yet",
//! exiting with status 1.
//!
//! The Admin API is served by the DC's controllers on
//! `[controller] admin_addr` (default `http://127.0.0.1:7480`) until the
//! gateway's 8443 listener serves it; `--endpoint` or `SPRIGCTL_ENDPOINT`
//! names it, and `--token-file` or `SPRIGCTL_TOKEN_FILE` its bearer token.

mod check;
mod client;
mod cluster;
mod dcmove;
mod export;
mod failover;
mod gateway;
mod index;
mod logic;
mod mover;
mod placement;
mod render;
mod residency;
mod settings;
mod sharding;
mod space;
mod txn;

use std::io::{BufRead, IsTerminal, Write};
use std::process::ExitCode;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde::Serialize;
use sprig_controller::admin::types::{
    ClusterView, CreateTenantRequest, CreateTenantResponse, GetDcResponse, ListClustersResponse,
    ListDcsResponse, ListTenantsResponse, ProbeClusterRequest, ProbeClusterResponse,
    RegisterClusterResponse, RegisterDcRequest, RegisterDcResponse, TenantView,
};
use sprig_controller::dc;
use sprig_controller::users::{CreateUserRequest, CreateUserResponse};

use crate::client::{AdminClient, CallError, DEFAULT_ENDPOINT};
use crate::cluster::RegisterFlags;

/// Command groups not implemented yet, and their subcommands (writing
/// guide, 5.10).
const GROUPS: &[(&str, &str, &[&str])] = &[
    ("feed", "Change feeds", &["list", "show", "lag"]),
    ("search", "Search indexes", &["list", "show", "rebuild"]),
    (
        "import",
        "Copy data and index definitions from a live source",
        &["mongodb", "mysql"],
    ),
];

/// `sprigctl tenant` subcommands not implemented yet.
const TENANT_LATER: &[&str] = &[];

/// `sprigctl tenant`.
#[allow(clippy::too_many_lines)]
fn tenant_group() -> Command {
    let name = || {
        Arg::new("name")
            .required(true)
            .help("Tenant name, a lowercase DNS label")
    };
    let mut g = Command::new("tenant")
        .about("Tenants")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create a tenant and bootstrap its catalog (returns once active)")
                .arg(name())
                .arg(
                    Arg::new("under")
                        .long("under")
                        .help("The parent tenant; a root tenant without it"),
                )
                .arg(
                    Arg::new("cluster")
                        .long("cluster")
                        .help("The cluster for the catalog, such as mum1-c7 (default: the active cluster with the fewest tenants)"),
                ),
        )
        .subcommand(Command::new("list").about("List tenants"))
        .subcommand(
            Command::new("show")
                .about("Show a tenant: state, ancestry, catalog cluster and versions")
                .arg(name()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a leaf tenant and drop its catalog")
                .arg(name()),
        )
        .subcommand(
            Command::new("user")
                .about("Users of a tenant (interim, until CREATE USER: chapter 29)")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("add")
                        .about("Create a user with a password in the tenant's catalog")
                        .arg(name())
                        .arg(
                            Arg::new("user")
                                .required(true)
                                .help("The user name, without the tenant prefix"),
                        )
                        .arg(
                            Arg::new("password")
                                .long("password")
                                .required(true)
                                .help("The password"),
                        )
                        .arg(
                            Arg::new("host")
                                .long("host")
                                .help("The host pattern (default %)"),
                        )
                        .arg(
                            Arg::new("if-not-exists")
                                .long("if-not-exists")
                                .action(ArgAction::SetTrue)
                                .help("Succeed without a change if the user exists (1396)"),
                        )
                        .arg(
                            Arg::new("native")
                                .long("native")
                                .action(ArgAction::SetTrue)
                                .help("Also store a mysql_native_password verifier (used where the tenant's auth.mysql_native_password is on)"),
                        ),
                ),
        )
        .subcommand(crate::export::export_command())
        .subcommand(crate::export::import_command())
        .subcommand(
            Command::new("settings")
                .about("The tenant's own setting rows (tenant, database and object scope)")
                .arg(name())
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .help("Also its copies of its ancestors' rows"),
                ),
        );
    g = placement::add_pin_cmds(g);
    // Chapter 24's moves between DCs and traffic (crate::dcmove).
    g = g
        .subcommand(dcmove::move_command())
        .subcommand(dcmove::traffic_command())
        // Chapter 25's manual failover and standbys (crate::failover).
        .subcommand(failover::failover_command())
        .subcommand(failover::standby_command())
        // Chapter 26's residency (crate::residency).
        .subcommand(residency::residency_command());
    for &sub in TENANT_LATER {
        g = g.subcommand(
            Command::new(sub).arg(
                Arg::new("args")
                    .num_args(0..)
                    .trailing_var_arg(true)
                    .allow_hyphen_values(true)
                    .help("Arguments (not parsed yet)"),
            ),
        );
    }
    g
}

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
        // Chapter 23 step 13: the DC lifecycle.
        .subcommand(
            Command::new("drain")
                .about("Take no new tenants; the DC is retired once migration empties it")
                .arg(dc_arg()),
        )
        .subcommand(
            Command::new("undrain")
                .about("Make a draining DC active again")
                .arg(dc_arg()),
        )
        .subcommand(
            Command::new("latency").about("The measured latency matrix between DCs (p50, p90, p99)"),
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
        .subcommand(cluster_group())
        .subcommand(tenant_group())
        .subcommand(settings::group())
        .subcommand(index::group())
        .subcommand(index::explain_group())
        .subcommand(mover::shard_group())
        .subcommand(mover::job_group())
        .subcommand(sharding::advisor_group())
        .subcommand(gateway::group())
        .subcommand(check::group())
        .subcommand(logic::group())
        .subcommand(space::group())
        .subcommand(txn::group());
    for &(group, about, subs) in GROUPS {
        let mut g = Command::new(group)
            .about(about)
            .subcommand_required(true)
            .arg_required_else_help(true);
        for &sub in subs {
            g = g.subcommand(
                Command::new(sub).arg(
                    Arg::new("args")
                        .num_args(0..)
                        .trailing_var_arg(true)
                        .allow_hyphen_values(true)
                        .help("Arguments (not parsed yet)"),
                ),
            );
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
    let implemented = match group {
        "dc" | "cluster" | "settings" | "index" | "job" | "gateway" | "check" | "advisor"
        | "shard" | "space" | "txn" => true,
        "explain" => matches!(name.as_str(), "query" | "decision" | "placement"),
        "tenant" => !TENANT_LATER.contains(&name.as_str()),
        "logic" => logic::implemented(&name),
        _ => false,
    };
    if !implemented {
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
        match group {
            "dc" => run_dc(&client, &matches, sub).await,
            "tenant" => run_tenant(&client, &matches, sub).await,
            "settings" => settings::run(&client, &matches, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "index" => index::run(&client, &matches, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "explain" => index::explain(&client, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "check" => check::no_restart(&client, sub, output(&matches))
                .await
                .map(|passed| {
                    if passed {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::FAILURE
                    }
                }),
            "gateway" => gateway::run(&client, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "logic" => logic::run(sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "space" => space::run(sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "txn" => txn::run(&client, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "shard" if sharding::run_shard(&client, sub, output(&matches)).await? => {
                Ok(ExitCode::SUCCESS)
            }
            "advisor" => sharding::run_advisor(&client, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            "shard" | "job" => mover::run(&client, group, sub, output(&matches))
                .await
                .map(|()| ExitCode::SUCCESS),
            _ => run_cluster(&client, &matches, sub).await,
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
        "drain" | "undrain" => {
            if m.get_flag("dry-run") {
                println!("would {cmd} dc {}", id());
                return Ok(ExitCode::SUCCESS);
            }
            let r: sprig_meta::bootstrap::DcRecord = c
                .post(
                    &format!("/admin/v1/dcs/{}:{cmd}", id()),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &[r], |r| render::dc_list(r));
            Ok(ExitCode::SUCCESS)
        }
        "latency" => {
            let r: serde_json::Value = c.get("/admin/v1/dcs:latency").await?;
            print(out, &r, |v| {
                let rows: Vec<[String; 6]> = v["rows"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|x| {
                        let f = |k: &str| {
                            x[k].as_f64()
                                .map_or_else(|| "-".to_owned(), |n| format!("{n:.2} ms"))
                        };
                        [
                            x["from"].as_str().unwrap_or_default().to_owned(),
                            x["to"].as_str().unwrap_or_default().to_owned(),
                            f("p50_ms"),
                            f("p90_ms"),
                            f("p99_ms"),
                            x["reachable"].to_string(),
                        ]
                    })
                    .collect();
                render::table(["FROM", "TO", "P50", "P90", "P99", "REACHABLE"], &rows)
            });
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

/// Asks for the cluster id (or tenant name) unless `--yes`.
fn confirm(m: &ArgMatches, what: &str, id: &str) -> Result<(), Failed> {
    if m.get_flag("yes") {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(Failed(format!(
            "{what} {id} needs --yes when not run interactively"
        )));
    }
    print!("{what} {id}? Type {id} to confirm: ");
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

#[allow(clippy::too_many_lines)] // one arm per subcommand
async fn run_tenant(
    c: &AdminClient,
    m: &ArgMatches,
    group: &ArgMatches,
) -> Result<ExitCode, Failed> {
    let out = output(m);
    let Some((cmd, sub)) = group.subcommand() else {
        return Ok(ExitCode::FAILURE);
    };
    let name = || sub.get_one::<String>("name").cloned().unwrap_or_default();
    match cmd {
        "pin" | "unpin" => {
            placement::run_pin(c, cmd, sub, out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "failover" | "standby" => {
            failover::run(c, cmd, sub, m.get_flag("dry-run"), out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "move" | "traffic" => {
            dcmove::run(c, cmd, sub, m.get_flag("dry-run"), out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "residency" => {
            residency::run(c, sub, out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "create" => {
            let cluster = sub
                .get_one::<String>("cluster")
                .map(|c| sprig_common::ids::ClusterId::new(c.as_str()))
                .transpose()
                .map_err(|e| format!("--cluster: {e}"))?;
            let req = CreateTenantRequest {
                name: name(),
                parent: sub.get_one::<String>("under").cloned(),
                cluster,
            };
            if m.get_flag("dry-run") {
                println!(
                    "would create tenant {}{}{} and bootstrap its catalog",
                    req.name,
                    req.parent
                        .as_ref()
                        .map_or(String::new(), |p| format!(" under {p}")),
                    req.cluster
                        .as_ref()
                        .map_or(String::new(), |c| format!(" on {c}")),
                );
                return Ok(ExitCode::SUCCESS);
            }
            let r: CreateTenantResponse = c.post("/admin/v1/tenants", &req).await?;
            if !r.created {
                eprintln!(
                    "tenant {} already existed; its bootstrap was continued",
                    r.tenant.name
                );
            }
            print(out, &r, |r| {
                let mut s = render::tenant_show(&r.tenant);
                if let Some(p) = &r.placement {
                    s.push_str("placement:         ");
                    s.push_str(p);
                    s.push('\n');
                }
                s
            });
            Ok(ExitCode::SUCCESS)
        }
        "list" => {
            let r: ListTenantsResponse = c.get("/admin/v1/tenants").await?;
            print(out, &r, |r| render::tenant_list(&r.tenants));
            Ok(ExitCode::SUCCESS)
        }
        "show" => {
            let r: TenantView = c.get(&format!("/admin/v1/tenants/{}", name())).await?;
            print(out, &r, render::tenant_show);
            Ok(ExitCode::SUCCESS)
        }
        "delete" => {
            let name = name();
            if m.get_flag("dry-run") {
                println!("would delete tenant {name} and drop its catalog");
                return Ok(ExitCode::SUCCESS);
            }
            confirm(m, "delete tenant", &name)?;
            let r: TenantView = c.delete(&format!("/admin/v1/tenants/{name}")).await?;
            print(out, &r, render::tenant_show);
            Ok(ExitCode::SUCCESS)
        }
        "settings" => {
            settings::tenant(c, sub, out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "export" => {
            export::export(c, sub, out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "import" => {
            export::import(c, sub, out).await?;
            Ok(ExitCode::SUCCESS)
        }
        "user" => {
            let Some(("add", add)) = sub.subcommand() else {
                return Ok(ExitCode::FAILURE);
            };
            let tenant = add.get_one::<String>("name").cloned().unwrap_or_default();
            let req = CreateUserRequest {
                user: add.get_one::<String>("user").cloned().unwrap_or_default(),
                host: add.get_one::<String>("host").cloned(),
                password: add
                    .get_one::<String>("password")
                    .cloned()
                    .unwrap_or_default(),
                native: add.get_flag("native"),
            };
            if m.get_flag("dry-run") {
                println!(
                    "would create user {}@{} in tenant {tenant}",
                    req.user,
                    req.host.as_deref().unwrap_or("%")
                );
                return Ok(ExitCode::SUCCESS);
            }
            let r: Result<CreateUserResponse, client::CallError> = c
                .post(&format!("/admin/v1/tenants/{tenant}/users"), &req)
                .await;
            let r = match r {
                Err(client::CallError::Api(e))
                    if add.get_flag("if-not-exists") && e.code == 1396 =>
                {
                    eprintln!("user {} already exists in tenant {tenant}", req.user);
                    return Ok(ExitCode::SUCCESS);
                }
                other => other?,
            };
            print(out, &r, |r| {
                format!(
                    "created user '{}'@'{}' (id {}) in tenant {} ({})\nlog in as:        {}\n",
                    r.user, r.host, r.user_id, r.tenant, r.tenant_id, r.login
                )
            });
            Ok(ExitCode::SUCCESS)
        }
        _ => Ok(ExitCode::FAILURE),
    }
}

#[cfg(test)]
mod tests;
