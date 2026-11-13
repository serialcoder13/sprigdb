//! `sprigctl tenant suspend <name> --reason …` and `sprigctl tenant resume
//! <name>` (chapter 14, "Suspension"): `POST
//! /admin/v1/tenants/{name}:suspend` and `:resume`.

use std::fmt::Write as _;

use clap::{Arg, ArgMatches, Command};
use sprig_controller::users::{SuspendTenantRequest, SuspendTenantResponse};

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// Adds `suspend` and `resume` to `sprigctl tenant`.
pub(crate) fn add_cmds(g: Command) -> Command {
    g.subcommand(
        Command::new("suspend")
            .about(
                "Suspend a tenant and its subtree: logins fail with 39303, open sessions close \
                 (auth.close_sessions_on_revoke), triggers, sinks and schedules wait",
            )
            .arg(Arg::new("name").required(true).help("The tenant"))
            .arg(
                Arg::new("reason")
                    .long("reason")
                    .required(true)
                    .help("Why; shown by tenant show and kept in the audit log"),
            ),
    )
    .subcommand(
        Command::new("resume")
            .about("Resume a suspended tenant: logins and background work start again")
            .arg(Arg::new("name").required(true).help("The tenant")),
    )
}

/// The text of a call's answer.
pub(crate) fn render(r: &SuspendTenantResponse) -> String {
    let mut s = if !r.changed {
        format!(
            "tenant {} was already {}\n",
            r.tenant,
            if r.suspended { "suspended" } else { "active" }
        )
    } else if r.suspended {
        format!(
            "tenant {} suspended ({})\n",
            r.tenant,
            r.reason.as_deref().unwrap_or("")
        )
    } else {
        format!("tenant {} resumed\n", r.tenant)
    };
    if !r.subtree.is_empty() {
        let _ = writeln!(
            s,
            "{} below it: {}",
            if r.suspended {
                "also suspended"
            } else {
                "tenants"
            },
            r.subtree.join(", ")
        );
    }
    if !r.sessions_not_closed.is_empty() {
        let _ = writeln!(
            s,
            "open sessions not closed in: {} (they end at reconnect)",
            r.sessions_not_closed.join(", ")
        );
    }
    s
}

/// Runs `suspend` or `resume`.
///
/// # Errors
/// The API's.
pub(crate) async fn run(
    c: &AdminClient,
    cmd: &str,
    sub: &ArgMatches,
    dry_run: bool,
    out: Output,
) -> Result<(), Failed> {
    let name = sub.get_one::<String>("name").cloned().unwrap_or_default();
    let reason = if cmd == "suspend" {
        sub.get_one::<String>("reason").cloned().unwrap_or_default()
    } else {
        String::new()
    };
    if dry_run {
        if cmd == "suspend" {
            println!("would suspend tenant {name} and its subtree ({reason})");
        } else {
            println!("would resume tenant {name}");
        }
        return Ok(());
    }
    let path = format!("/admin/v1/tenants/{}:{cmd}", escape(&name));
    let r: SuspendTenantResponse = if cmd == "suspend" {
        c.post(&path, &SuspendTenantRequest { reason }).await?
    } else {
        c.post(&path, &serde_json::json!({})).await?
    };
    print(out, &r, render);
    Ok(())
}
