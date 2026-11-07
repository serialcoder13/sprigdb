//! `sprigctl`: the operator command-line tool.
//!
//! Layer 4 (binaries). Design: the writing guide's 5.10 (command groups) and
//! chapter 47 (Operations, global flags) of the SprigDB low-level design.
//!
//! **Skeleton.** Every command group and subcommand parses, and every one
//! prints "not implemented yet" and exits with status 1. The commands call
//! the admin API on 8443 once it exists (chapter 37).

use std::process::ExitCode;

use clap::{Arg, ArgAction, Command};

/// Command groups and their subcommands (writing guide, 5.10).
const GROUPS: &[(&str, &str, &[&str])] = &[
    ("dc", "Data centers", &["register", "list", "show"]),
    (
        "cluster",
        "MariaDB clusters",
        &["register", "list", "show", "probe", "drain"],
    ),
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

fn cli() -> Command {
    let mut cmd = Command::new("sprigctl")
        .about("SprigDB operator CLI")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("endpoint")
                .long("endpoint")
                .global(true)
                .help("Admin API endpoint (port 8443)"),
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
        );
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

fn main() -> ExitCode {
    let matches = cli().get_matches();
    if let Some((group, sub)) = matches.subcommand() {
        let name = sub.subcommand_name().unwrap_or_default();
        eprintln!("sprigctl {group} {name}: not implemented yet");
    }
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_parses() {
        cli().debug_assert();
        for &(group, _, subs) in GROUPS {
            for &sub in subs {
                let m = cli()
                    .try_get_matches_from(["sprigctl", "-o", "json", group, sub, "x", "--flag"])
                    .unwrap();
                let (g, s) = m.subcommand().unwrap();
                assert_eq!((g, s.subcommand_name()), (group, Some(sub)));
            }
        }
        let scoped =
            cli().try_get_matches_from(["sprigctl", "config", "get", "--scope", "dc:mum1", "x"]);
        assert!(scoped.is_ok());
        assert!(cli().try_get_matches_from(["sprigctl", "nope"]).is_err());
    }
}
