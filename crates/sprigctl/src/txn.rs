//! `sprigctl txn list|show|resolve`, over the Admin API's
//! `/admin/v1/txns`.
//!
//! ```text
//! sprigctl txn list [--tenant acme]     # in-doubt transactions, oldest first
//! sprigctl txn show <dtid>              # one, with its branches and decision
//! sprigctl txn resolve <dtid>           # finish it now, by its decision
//! ```
//!
//! A transaction is in doubt while one of its XA branches is prepared. The
//! controller's resolver finishes such branches once they are
//! `txn.resolver_min_age_seconds` old; `resolve` does it at once, the same
//! way: the decision recorded at the transaction's home commits it, and
//! with none recorded the insert race records an abort. An operator never
//! picks the outcome.

use std::fmt::Write as _;

use clap::{Arg, ArgMatches, Command};
use serde::{Deserialize, Serialize};

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// One prepared branch, as the Admin API returns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BranchView {
    cluster: String,
    bqual: String,
}

/// One in-doubt transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxnView {
    dtid: String,
    tenant: u64,
    started_ms: u64,
    home: String,
    home_at: Option<String>,
    decision: String,
    decided_by: Option<String>,
    decision_age_seconds: Option<u64>,
    note: Option<String>,
    branches: Vec<BranchView>,
}

/// `GET /admin/v1/txns`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxnList {
    transactions: Vec<TxnView>,
    unreachable: Vec<String>,
}

/// `POST /admin/v1/txns/{dtid}:resolve`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResolveView {
    dtid: String,
    committed: Vec<BranchView>,
    rolled_back: Vec<BranchView>,
    unresolved: Vec<BranchView>,
    unreachable: Vec<String>,
}

/// `sprigctl txn`.
pub(crate) fn group() -> Command {
    let dtid = || {
        Arg::new("dtid")
            .required(true)
            .help("The transaction's dtid (32 hex digits, as `txn list` prints it)")
    };
    Command::new("txn")
        .about("In-doubt distributed transactions")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List in-doubt transactions (prepared branches) on this DC's clusters")
                .arg(
                    Arg::new("tenant")
                        .long("tenant")
                        .help("Only this tenant's transactions"),
                ),
        )
        .subcommand(
            Command::new("show")
                .about("Show one in-doubt transaction: its branches and its decision")
                .arg(dtid()),
        )
        .subcommand(
            Command::new("resolve")
                .about(
                    "Finish an in-doubt transaction now, by its recorded decision (abort when \
                     none is recorded), as the resolver would",
                )
                .arg(dtid()),
        )
}

fn branches(v: &[BranchView]) -> String {
    if v.is_empty() {
        return "-".to_owned();
    }
    v.iter()
        .map(|b| format!("{}:{}", b.cluster, b.bqual))
        .collect::<Vec<_>>()
        .join(", ")
}

fn list_text(l: &TxnList) -> String {
    let rows: Vec<[String; 6]> = l
        .transactions
        .iter()
        .map(|t| {
            [
                t.dtid.clone(),
                t.tenant.to_string(),
                t.decision.clone(),
                t.branches.len().to_string(),
                t.home_at.clone().unwrap_or_else(|| "-".into()),
                t.started_ms.to_string(),
            ]
        })
        .collect();
    let mut s = table(
        [
            "DTID",
            "TENANT",
            "DECISION",
            "BRANCHES",
            "HOME",
            "STARTED_MS",
        ],
        &rows,
    );
    if l.transactions.is_empty() {
        s.push_str("no in-doubt transactions\n");
    }
    for u in &l.unreachable {
        let _ = writeln!(s, "not reachable: {u}");
    }
    s
}

fn show_text(t: &TxnView) -> String {
    let mut s = String::new();
    let mut line = |k: &str, v: String| {
        let _ = writeln!(s, "{k:<14}{v}");
    };
    line("dtid", t.dtid.clone());
    line("tenant", t.tenant.to_string());
    line("started_ms", t.started_ms.to_string());
    line("home", t.home.clone());
    line("home at", t.home_at.clone().unwrap_or_else(|| "-".into()));
    line("decision", t.decision.clone());
    if let Some(b) = &t.decided_by {
        line("decided by", b.clone());
    }
    if let Some(a) = t.decision_age_seconds {
        line("decided", format!("{a} s ago"));
    }
    if let Some(n) = &t.note {
        line("note", n.clone());
    }
    line("branches", branches(&t.branches));
    s
}

fn resolve_text(r: &ResolveView) -> String {
    let mut s = format!(
        "{}: committed {}; rolled back {}; unresolved {}\n",
        r.dtid,
        branches(&r.committed),
        branches(&r.rolled_back),
        branches(&r.unresolved)
    );
    for u in &r.unreachable {
        let _ = writeln!(s, "not reachable: {u}");
    }
    s
}

/// Runs a `txn` command.
pub(crate) async fn run(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Err(Failed("missing subcommand".into()));
    };
    let dtid = || m.get_one::<String>("dtid").cloned().unwrap_or_default();
    match cmd {
        "list" => {
            let path = match m.get_one::<String>("tenant") {
                Some(t) => format!("/admin/v1/txns?tenant={}", escape(t)),
                None => "/admin/v1/txns".to_owned(),
            };
            let l: TxnList = c.get(&path).await?;
            print(out, &l, list_text);
        }
        "show" => {
            let t: TxnView = c
                .get(&format!("/admin/v1/txns/{}", escape(&dtid())))
                .await?;
            print(out, &t, show_text);
        }
        "resolve" => {
            let r: ResolveView = c
                .post(
                    &format!("/admin/v1/txns/{}:resolve", escape(&dtid())),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &r, resolve_text);
            if !r.unresolved.is_empty() {
                return Err(Failed(format!(
                    "{} branch(es) still prepared: a live connection owns them or the decision \
                     couldn't be made; retry later",
                    r.unresolved.len()
                )));
            }
        }
        other => {
            return Err(Failed(format!(
                "txn {other}: not available in this version"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn the_group_parses_its_commands() {
        let m = group()
            .try_get_matches_from(["txn", "list", "--tenant", "acme"])
            .unwrap();
        let (cmd, sub) = m.subcommand().unwrap();
        assert_eq!(cmd, "list");
        assert_eq!(sub.get_one::<String>("tenant").unwrap(), "acme");
        assert!(group().try_get_matches_from(["txn", "show"]).is_err());
        assert!(
            group()
                .try_get_matches_from(["txn", "resolve", "0000000000000001000000000000002a"])
                .is_ok()
        );
    }

    #[test]
    fn views_render() {
        let l: TxnList = serde_json::from_value(serde_json::json!({
            "transactions": [{
                "dtid": "0000000000000001000000000000002a", "tenant": 7, "started_ms": 5,
                "home": "d1.g3@0000000000000000", "home_at": "dc-a-c1/t7_d1_g3_s1",
                "decision": "none", "decided_by": null, "decision_age_seconds": null,
                "note": null,
                "branches": [{"cluster": "dc-a-c1", "bqual": "cdc-a-c1.s1"}]
            }],
            "unreachable": ["dc-a-c3: down"]
        }))
        .unwrap();
        let s = list_text(&l);
        assert!(s.contains("0000000000000001000000000000002a"), "{s}");
        assert!(s.contains("not reachable: dc-a-c3"), "{s}");
        let t = show_text(&l.transactions[0]);
        assert!(t.contains("dc-a-c1:cdc-a-c1.s1"), "{t}");
        let r = ResolveView {
            dtid: "x".into(),
            committed: Vec::new(),
            rolled_back: l.transactions[0].branches.clone(),
            unresolved: Vec::new(),
            unreachable: Vec::new(),
        };
        assert!(resolve_text(&r).contains("rolled back dc-a-c1"));
    }
}
