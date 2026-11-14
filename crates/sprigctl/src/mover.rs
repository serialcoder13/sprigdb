//! `sprigctl shard move` and `sprigctl job list|show|cancel|cutover`
//! over the Admin API's
//! `/admin/v1/mover/jobs`.
//!
//! A move takes a tenant's whole placement unit (its catalog and every
//! database's shard schema, kept on one cluster):
//!
//! ```text
//! sprigctl shard move acme --to dc-a-c2 [--manual] [--verify off] [--wait]
//! sprigctl job list [--tenant acme]
//! sprigctl job show <id>
//! sprigctl job cancel <id>
//! sprigctl job cutover <id>        # a --manual move waiting in ready
//! ```

use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde::Serialize;
use sprig_mover::JobStatus;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `POST /admin/v1/mover/jobs`.
#[derive(Debug, Serialize)]
struct MoveRequest {
    tenant: String,
    to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cutover: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verify: Option<String>,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct MoveJobs {
    jobs: Vec<JobStatus>,
}

/// `sprigctl shard`.
pub(crate) fn shard_group() -> Command {
    crate::sharding::shard_subcommands(Command::new("shard"))
        .about("Shards")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("move")
                .about(
                    "Move one shard of a table group (<tenant> <group> <shard>), or a tenant \
                     without table groups whole (<tenant>), to another cluster, online",
                )
                .arg(Arg::new("tenant").required(true).help("The tenant"))
                .arg(Arg::new("group").help("The table group id (a shard move)"))
                .arg(Arg::new("shard").help("The shard id (a shard move)"))
                .arg(
                    Arg::new("to")
                        .long("to")
                        .required(true)
                        .help("The target cluster, such as dc-a-c2"),
                )
                .arg(
                    Arg::new("manual")
                        .long("manual")
                        .action(ArgAction::SetTrue)
                        .help("Wait in ready for `sprigctl job cutover`"),
                )
                .arg(
                    Arg::new("verify")
                        .long("verify")
                        .help("full (default), off, or sample:<pct>"),
                )
                .arg(
                    Arg::new("wait")
                        .long("wait")
                        .action(ArgAction::SetTrue)
                        .help("Return once the cutover committed or the job ended"),
                )
                .arg(
                    Arg::new("pin")
                        .long("pin")
                        .action(ArgAction::SetTrue)
                        .help("Then pin the tenant to the target"),
                ),
        )
}

/// `sprigctl job`.
pub(crate) fn job_group() -> Command {
    let id = || Arg::new("id").required(true).help("The job id");
    Command::new("job")
        .about("Mover jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List mover jobs, newest first")
                .arg(Arg::new("tenant").long("tenant").help("Only this tenant's")),
        )
        .subcommand(Command::new("show").about("Show a job").arg(id()))
        .subcommand(
            Command::new("cancel")
                .about("Cancel a job before its cutover commits")
                .arg(id()),
        )
        .subcommand(
            Command::new("cutover")
                .about("Let a --manual move cut over")
                .arg(id()),
        )
}

fn ms(v: Option<u64>) -> String {
    v.map_or_else(|| "-".to_owned(), |m| format!("{m} ms"))
}

fn show_text(j: &JobStatus) -> String {
    let p = &j.progress;
    let mut s = String::new();
    let mut line = |k: &str, v: String| {
        use std::fmt::Write as _;
        let _ = writeln!(s, "{k:<18}{v}");
    };
    line("id", j.id.0.to_string());
    line("kind", j.kind.as_str().to_owned());
    line("tenant", j.tenant.to_string());
    line("state", j.state.to_string());
    line(
        "clusters",
        format!(
            "{} -> {}",
            j.source.as_ref().map_or("-".into(), ToString::to_string),
            j.target.as_ref().map_or("-".into(), ToString::to_string)
        ),
    );
    line("tables", format!("{}/{} copied", p.tables_done, p.tables));
    line(
        "rows",
        format!("{} copied, {} applied", p.rows_copied, p.rows_applied),
    );
    line("lag", ms(p.lag_ms));
    line(
        "cutovers",
        format!("{} attempts, holds {:?} ms", p.cutover_attempts, p.holds_ms),
    );
    line("verify mismatches", p.verify_mismatches.to_string());
    if let Some(e) = j.epoch {
        line("epoch", e.to_string());
    }
    if let Some(e) = &j.error {
        line("last error", e.clone());
    }
    if j.cancel_requested {
        line("cancel", "requested".to_owned());
    }
    s
}

fn list_text(r: &MoveJobs) -> String {
    let rows: Vec<[String; 6]> = r
        .jobs
        .iter()
        .map(|j| {
            [
                j.id.0.to_string(),
                j.tenant.to_string(),
                j.state.to_string(),
                format!(
                    "{} -> {}",
                    j.source.as_ref().map_or("-".into(), ToString::to_string),
                    j.target.as_ref().map_or("-".into(), ToString::to_string)
                ),
                ms(j.progress.lag_ms),
                format!("{:?}", j.progress.holds_ms),
            ]
        })
        .collect();
    table(["JOB", "TENANT", "STATE", "MOVE", "LAG", "HOLDS"], &rows)
}

/// Runs `shard move` or a `job` command.
pub(crate) async fn run(
    c: &AdminClient,
    group: &str,
    sub: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Err(Failed("missing subcommand".into()));
    };
    let id = || m.get_one::<String>("id").cloned().unwrap_or_default();
    match (group, cmd) {
        ("shard", "move") => {
            let req = MoveRequest {
                tenant: m.get_one::<String>("tenant").cloned().unwrap_or_default(),
                to: m.get_one::<String>("to").cloned().unwrap_or_default(),
                cutover: m.get_flag("manual").then(|| "manual".to_owned()),
                verify: m.get_one::<String>("verify").cloned(),
            };
            let mut j: JobStatus = c.post("/admin/v1/mover/jobs", &req).await?;
            if m.get_flag("pin") {
                // Pinned to the target at once, so the balancer leaves the
                // tenant alone while it moves and after.
                let p = crate::placement::pin(c, &req.tenant, Some(req.to.clone()), None).await?;
                eprintln!("tenant {} {}", req.tenant, p.describe());
            }
            if m.get_flag("wait") {
                while !j.state.is_terminal() && !j.state.is_committed() {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    j = c.get(&format!("/admin/v1/mover/jobs/{}", j.id.0)).await?;
                }
            }
            print(out, &j, show_text);
        }
        ("job", "list") => {
            let path = match m.get_one::<String>("tenant") {
                Some(t) => format!("/admin/v1/mover/jobs?tenant={}", escape(t)),
                None => "/admin/v1/mover/jobs".to_owned(),
            };
            let r: MoveJobs = c.get(&path).await?;
            print(out, &r, list_text);
        }
        ("job", "show") => {
            let j: JobStatus = c
                .get(&format!("/admin/v1/mover/jobs/{}", escape(&id())))
                .await?;
            print(out, &j, show_text);
        }
        ("job", "cancel" | "cutover") => {
            let j: JobStatus = c
                .post(
                    &format!("/admin/v1/mover/jobs/{}:{cmd}", escape(&id())),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &j, show_text);
        }
        _ => {
            return Err(Failed(format!(
                "{group} {cmd}: not available in this version"
            )));
        }
    }
    Ok(())
}
