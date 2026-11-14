//! `sprigctl tenant failover` and `sprigctl tenant standby`
//! (manual failover), over
//! `/admin/v1/tenants/{name}:failover`, `/admin/v1/failovers:all`,
//! `/admin/v1/tenants/{name}/failovers` and `/admin/v1/tenants/{name}:standby`.
//!
//! ```text
//! sprigctl tenant failover acme --to ams1 [--accept-data-loss] [--old-home-fenced]
//! sprigctl --dry-run tenant failover acme --to ams1   # the checks and the lag
//! sprigctl tenant failover --all-from fra1 --to ams1  # every tenant of fra1
//! sprigctl tenant failover acme --errant              # diverged-transaction reports
//! sprigctl tenant standby acme --dc ams1 --cluster ams1-c1
//! ```
//!
//! Point `--endpoint` at a controller of the new home: the old one may be
//! gone. The failover is refused (39505) while the old home holds its DC
//! lease, unless `--old-home-fenced` vouches for it (audited); any
//! possible loss needs `--accept-data-loss`.

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::failover::manual::{
    ErrantView, FailoverAllRequest, FailoverRequest, FailoverView, StandbyRequest,
};

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl tenant failover`.
pub(crate) fn failover_command() -> Command {
    Command::new("failover")
        .about("Fail a tenant over to its standby in another data center")
        .arg(
            Arg::new("name")
                .required_unless_present("all-from")
                .help("The tenant"),
        )
        .arg(
            Arg::new("to")
                .long("to")
                .required_unless_present("errant")
                .help("The new home, a data center holding the tenant's standby"),
        )
        .arg(
            Arg::new("all-from")
                .long("all-from")
                .conflicts_with_all(["name", "errant"])
                .help("Every tenant homed in this data center"),
        )
        .arg(
            Arg::new("accept-data-loss")
                .long("accept-data-loss")
                .action(ArgAction::SetTrue)
                .help("Accept losing what the standby hadn't applied"),
        )
        .arg(
            Arg::new("old-home-fenced")
                .long("old-home-fenced")
                .action(ArgAction::SetTrue)
                .help("Vouch that the old home is fenced although it holds its DC lease (audited)"),
        )
        .arg(
            Arg::new("errant")
                .long("errant")
                .action(ArgAction::SetTrue)
                .conflicts_with("to")
                .help("Show the tenant's failovers and their diverged-transaction reports"),
        )
}

/// `sprigctl tenant standby`.
pub(crate) fn standby_command() -> Command {
    Command::new("standby")
        .about("Record a standby of a tenant: a cluster in another data center that replicates its home cluster")
        .arg(Arg::new("name").required(true).help("The tenant"))
        .arg(Arg::new("dc").long("dc").required(true).help("The standby data center"))
        .arg(
            Arg::new("cluster")
                .long("cluster")
                .required(true)
                .help("Its cluster, such as ams1-c1"),
        )
}

fn show(v: &[FailoverView]) -> String {
    let rows: Vec<[String; 7]> = v
        .iter()
        .map(|f| {
            [
                f.tenant.clone(),
                format!("{} -> {}", f.from, f.to),
                f.lag_ms
                    .map_or_else(|| "unknown".to_owned(), |l| format!("{l} ms")),
                f.old_home_fenced_by.clone(),
                f.outcome.clone(),
                f.epoch.map_or_else(|| "-".to_owned(), |e| e.to_string()),
                f.reason.clone().unwrap_or_default(),
            ]
        })
        .collect();
    table(
        [
            "TENANT",
            "MOVE",
            "STANDBY LAG",
            "OLD HOME FENCED BY",
            "OUTCOME",
            "EPOCH",
            "REASON",
        ],
        &rows,
    )
}

fn show_errant(v: &ErrantView) -> String {
    let mut s = format!("tenant {}: {} failover(s)\n", v.tenant, v.failovers.len());
    for f in &v.failovers {
        let _ = writeln!(
            s,
            "{} from {} ({:?}): {}",
            f.failover_id, f.dc_id, f.state, f.outcome
        );
        match &f.errant {
            None => {
                let _ = writeln!(
                    s,
                    "  diverged transactions: not reported yet (the old home hasn't returned)"
                );
            }
            Some(r) => {
                let _ = writeln!(
                    s,
                    "  diverged transactions: {} (reported, never merged); tables: {}",
                    r.count,
                    if r.tables.is_empty() {
                        "-".to_owned()
                    } else {
                        r.tables.join(", ")
                    }
                );
                for g in &r.gtids {
                    let _ = writeln!(s, "    {g}");
                }
                for st in r.statements.iter().take(20) {
                    let _ = writeln!(s, "    > {st}");
                }
                if r.statements.len() > 20 {
                    let _ = writeln!(s, "    ... {} more statements", r.statements.len() - 20);
                }
            }
        }
    }
    s
}

/// Runs `tenant failover` or `tenant standby`.
pub(crate) async fn run(
    c: &AdminClient,
    cmd: &str,
    m: &ArgMatches,
    dry_run: bool,
    out: Output,
) -> Result<(), Failed> {
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if cmd == "standby" {
        let req = StandbyRequest {
            dc: m
                .get_one::<String>("dc")
                .map(|d| sprig_common::ids::DcId::new(d.as_str()))
                .transpose()
                .map_err(|e| Failed(format!("--dc: {e}")))?
                .ok_or_else(|| Failed("--dc is required".into()))?,
            cluster: m
                .get_one::<String>("cluster")
                .map(|c| sprig_common::ids::ClusterId::new(c.as_str()))
                .transpose()
                .map_err(|e| Failed(format!("--cluster: {e}")))?
                .ok_or_else(|| Failed("--cluster is required".into()))?,
        };
        if dry_run {
            println!(
                "would record {} as a standby of tenant {name} in {}",
                req.cluster, req.dc
            );
            return Ok(());
        }
        let v: serde_json::Value = c
            .post(
                &format!("/admin/v1/tenants/{}:standby", escape(&name)),
                &req,
            )
            .await?;
        print(out, &v, |v| {
            format!(
                "standby of tenant {name} recorded: {} maps on {} in {}\n",
                v["maps"], req.cluster, req.dc
            )
        });
        return Ok(());
    }
    if m.get_flag("errant") {
        let v: ErrantView = c
            .get(&format!("/admin/v1/tenants/{}/failovers", escape(&name)))
            .await?;
        print(out, &v, show_errant);
        return Ok(());
    }
    let to = m
        .get_one::<String>("to")
        .map(|d| sprig_common::ids::DcId::new(d.as_str()))
        .transpose()
        .map_err(|e| Failed(format!("--to: {e}")))?
        .ok_or_else(|| Failed("--to is required".into()))?;
    let accept = m.get_flag("accept-data-loss");
    let fenced = m.get_flag("old-home-fenced");
    if let Some(from) = m.get_one::<String>("all-from") {
        let req = FailoverAllRequest {
            from: sprig_common::ids::DcId::new(from.as_str())
                .map_err(|e| Failed(format!("--all-from: {e}")))?,
            to,
            accept_data_loss: accept,
            old_home_fenced: fenced,
            dry_run,
        };
        let v: Vec<FailoverView> = c.post("/admin/v1/failovers:all", &req).await?;
        print(out, &v, |v| show(v));
        return Ok(());
    }
    let req = FailoverRequest {
        to,
        accept_data_loss: accept,
        old_home_fenced: fenced,
        dry_run,
    };
    let v: FailoverView = c
        .post(
            &format!("/admin/v1/tenants/{}:failover", escape(&name)),
            &req,
        )
        .await?;
    print(out, &v, |v| show(std::slice::from_ref(v)));
    Ok(())
}
