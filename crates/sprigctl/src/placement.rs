//! `sprigctl explain placement <tenant>` and `sprigctl explain decision
//! <id> --tenant <tenant>` (chapter 11, build step 7), over the Admin
//! API's `GET /admin/v1/tenants/{name}/placement` and
//! `GET /admin/v1/tenants/{name}/decisions/{id}`.

use clap::{Arg, ArgMatches, Command};
use sprig_balancer::decisions::DecisionRow;
use sprig_balancer::explain::{PlacementExplanation, decision_text, placement_text};
use sprig_balancer::pins::TenantPin;
use sprig_controller::admin::placement_admin::{PinRequest, UnpinResponse};

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `explain placement`.
pub(crate) fn placement_cmd() -> Command {
    Command::new("placement")
        .about("Explain where a tenant's shards are and what the balancer plans")
        .arg(Arg::new("tenant").required(true).help("The tenant"))
}

/// `explain decision`.
pub(crate) fn decision_cmd() -> Command {
    Command::new("decision")
        .about("Show one decision from a tenant's decision log")
        .arg(Arg::new("id").required(true).help("The decision id"))
        .arg(
            Arg::new("tenant")
                .long("tenant")
                .env("SPRIGCTL_TENANT")
                .required(true)
                .help("The tenant"),
        )
}

/// Runs `explain placement` or `explain decision`.
pub(crate) async fn run(
    c: &AdminClient,
    cmd: &str,
    sub: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let tenant = sub.get_one::<String>("tenant").cloned().unwrap_or_default();
    if cmd == "placement" {
        let r: PlacementExplanation = c
            .get(&format!("/admin/v1/tenants/{}/placement", escape(&tenant)))
            .await?;
        print(out, &r, placement_text);
    } else {
        {
            let id = sub.get_one::<String>("id").cloned().unwrap_or_default();
            if id.parse::<u64>().is_err() {
                return Err(Failed(format!("decision id {id:?} is not a number")));
            }
            let r: DecisionRow = c
                .get(&format!(
                    "/admin/v1/tenants/{}/decisions/{}",
                    escape(&tenant),
                    escape(&id)
                ))
                .await?;
            print(out, &r, decision_text);
        }
    }
    Ok(())
}

/// Adds chapter 11's `tenant pin` and `tenant unpin` to the tenant group.
pub(crate) fn add_pin_cmds(g: Command) -> Command {
    g.subcommand(pin_cmd()).subcommand(unpin_cmd())
}

/// `tenant pin`.
fn pin_cmd() -> Command {
    Command::new("pin")
        .about(
            "Pin a tenant to its cluster and DC: the balancer won't move it; drains and \
             residency changes report it as blocked",
        )
        .arg(Arg::new("name").required(true).help("The tenant"))
        .arg(
            Arg::new("cluster")
                .long("cluster")
                .help("The cluster; the tenant's current cluster when absent"),
        )
        .arg(
            Arg::new("reason")
                .long("reason")
                .help("Why, for the decision log"),
        )
}

/// `tenant unpin`.
fn unpin_cmd() -> Command {
    Command::new("unpin")
        .about("Let the balancer manage a pinned tenant again")
        .arg(Arg::new("name").required(true).help("The tenant"))
}

fn operator() -> Option<String> {
    std::env::var("USER").ok().filter(|u| !u.is_empty())
}

/// Pins `tenant` to `cluster` (also used by `shard move --pin`).
pub(crate) async fn pin(
    c: &AdminClient,
    tenant: &str,
    cluster: Option<String>,
    reason: Option<String>,
) -> Result<TenantPin, Failed> {
    let req = PinRequest {
        cluster,
        reason,
        by: operator(),
    };
    Ok(
        c.post(&format!("/admin/v1/tenants/{}/pin", escape(tenant)), &req)
            .await?,
    )
}

/// Chapter 24's DC pin (`POST /admin/v1/tenants/{name}:pin|:unpin`): a
/// tenant pinned to a cluster is pinned to its DC too. Best effort: a
/// controller without DC moves says so and the cluster pin still holds.
async fn dc_pin(c: &AdminClient, name: &str, op: &str) {
    let r: Result<serde_json::Value, _> = c
        .post(
            &format!("/admin/v1/tenants/{}:{op}", escape(name)),
            &serde_json::json!({}),
        )
        .await;
    if let Err(e) = r {
        eprintln!("note: the DC {op} didn't apply: {e}");
    }
}

/// Runs `tenant pin` or `tenant unpin`.
pub(crate) async fn run_pin(
    c: &AdminClient,
    cmd: &str,
    sub: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let name = sub.get_one::<String>("name").cloned().unwrap_or_default();
    if cmd == "pin" {
        let p = pin(
            c,
            &name,
            sub.get_one::<String>("cluster").cloned(),
            sub.get_one::<String>("reason").cloned(),
        )
        .await?;
        print(out, &p, |p| format!("tenant {name} {}\n", p.describe()));
        dc_pin(c, &name, "pin").await;
    } else {
        let r: UnpinResponse = c
            .delete(&format!("/admin/v1/tenants/{}/pin", escape(&name)))
            .await?;
        dc_pin(c, &name, "unpin").await;
        print(out, &r, |r| match &r.was {
            Some(p) => format!("tenant {} unpinned from {}\n", r.tenant, p.cluster),
            None => format!("tenant {} was not pinned\n", r.tenant),
        });
    }
    Ok(())
}
