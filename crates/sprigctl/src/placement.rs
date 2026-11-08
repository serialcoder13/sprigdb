//! `sprigctl explain placement <tenant>` and `sprigctl explain decision
//! <id> --tenant <tenant>` (chapter 11, build step 7), over the Admin
//! API's `GET /admin/v1/tenants/{name}/placement` and
//! `GET /admin/v1/tenants/{name}/decisions/{id}`.

use clap::{Arg, ArgMatches, Command};
use sprig_balancer::decisions::DecisionRow;
use sprig_balancer::explain::{PlacementExplanation, decision_text, placement_text};

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
