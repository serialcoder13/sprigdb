//! `sprigctl tenant residency`, over
//! `/admin/v1/tenants/{name}:residency` and
//! `/admin/v1/tenants/{name}/residency`.
//!
//! ```text
//! sprigctl tenant residency acme                                   # show
//! sprigctl tenant residency acme --policy 'jurisdiction = "EU"'    # the plan only
//! sprigctl tenant residency acme --policy 'jurisdiction = "EU"' --apply
//! sprigctl tenant residency acme --deny-remote-access true --apply
//! sprigctl tenant residency acme --policy '' --apply               # remove it
//! ```
//!
//! This is not `sprigctl tenant settings acme set
//! residency.policy ...`: tenant settings live in the tenant catalog, while
//! residency must be readable by every DC from the directory, so it has its
//! own call. Without `--apply` nothing changes: the plan for the tenant and
//! every descendant is printed.

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::residency::{ResidencyRequest, ResidencyView};

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl tenant residency`.
pub(crate) fn residency_command() -> Command {
    Command::new("residency")
        .about("Show or set a tenant's data residency")
        .arg(Arg::new("name").required(true).help("The tenant"))
        .arg(
            Arg::new("policy")
                .long("policy")
                .help("A condition over DC tags, such as 'jurisdiction = \"EU\"'; '' removes it"),
        )
        .arg(
            Arg::new("deny-remote-access")
                .long("deny-remote-access")
                .value_parser(clap::value_parser!(bool))
                .help("Refuse logins at gateways outside the allowed DCs (true or false)"),
        )
        .arg(
            Arg::new("apply")
                .long("apply")
                .action(ArgAction::SetTrue)
                .help("Write it; without this only the plan is printed"),
        )
}

fn show(v: &ResidencyView) -> String {
    let mut s = String::new();
    let allowed = v.allowed_dcs.as_ref().map_or_else(
        || "any (no residency)".to_owned(),
        |a| {
            if a.is_empty() {
                "none".to_owned()
            } else {
                a.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        },
    );
    let _ = writeln!(s, "tenant:              {} ({})", v.tenant, v.tenant_id);
    let _ = writeln!(
        s,
        "policy:              {}",
        if v.policy.is_empty() {
            "(none of its own)"
        } else {
            &v.policy
        }
    );
    let _ = writeln!(s, "allowed DCs:         {allowed}");
    let _ = writeln!(s, "deny remote access:  {}", v.deny_remote_access);
    let _ = writeln!(s, "state:               {}", v.state);
    for p in &v.pending {
        let _ = writeln!(s, "  pending: {p}");
    }
    for w in &v.warnings {
        let _ = writeln!(s, "warning: {w}");
    }
    for t in &v.plans {
        let _ = writeln!(
            s,
            "plan for {} (home {}): {}",
            t.tenant,
            t.home_dc,
            if t.plan.is_empty() {
                "nothing to do"
            } else {
                ""
            }
        );
        for l in &t.plan {
            let _ = writeln!(s, "  - {l}");
        }
    }
    if !v.plans.is_empty() {
        let _ = writeln!(
            s,
            "{}",
            if v.applied {
                "applied"
            } else {
                "dry run: nothing changed (add --apply)"
            }
        );
    }
    s
}

pub(crate) async fn run(c: &AdminClient, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    let policy = m.get_one::<String>("policy").cloned();
    let deny = m.get_one::<bool>("deny-remote-access").copied();
    let apply = m.get_flag("apply");
    let v: ResidencyView = if policy.is_none() && deny.is_none() && !apply {
        c.get(&format!("/admin/v1/tenants/{}/residency", escape(&name)))
            .await?
    } else {
        c.post(
            &format!("/admin/v1/tenants/{}:residency", escape(&name)),
            &ResidencyRequest {
                policy,
                deny_remote_access: deny,
                apply,
            },
        )
        .await?
    };
    print(out, &v, show);
    Ok(())
}
