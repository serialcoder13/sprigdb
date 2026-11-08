//! `sprigctl tenant move` and `sprigctl tenant traffic` (chapter 24, build
//! steps 3 and 4), over `/admin/v1/tenants/{name}:move`, `.../move` and
//! `.../traffic`.
//!
//! ```text
//! sprigctl tenant move acme --to fra1 [--max-pause-ms 5000] [--cluster fra1-c2] [--wait]
//! sprigctl --dry-run tenant move acme --to fra1    # check and show the plan
//! sprigctl tenant move acme --status               # the move in progress, or the last one
//! sprigctl tenant traffic acme                     # the traffic window and the decision
//! ```
//!
//! The move is answered by the leader of the tenant's home DC: point
//! `--endpoint` at a controller (or gateway) of that DC. `--wait` returns
//! once the move committed or rolled back, and shows the write pause and
//! the time from start to commit.

use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::migrate::{MoveTenantRequest, MoveView};
use sprig_controller::traffic::TrafficView;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl tenant move`.
pub(crate) fn move_command() -> Command {
    Command::new("move")
        .about("Move a tenant to another data center, online (chapter 24)")
        .arg(Arg::new("name").required(true).help("The tenant"))
        .arg(
            Arg::new("to")
                .long("to")
                .required_unless_present("status")
                .help("The target data center, such as fra1"),
        )
        .arg(
            Arg::new("max-pause-ms")
                .long("max-pause-ms")
                .value_parser(clap::value_parser!(u64))
                .help("Write-pause budget (default: migration.write_pause_target_ms)"),
        )
        .arg(
            Arg::new("cluster")
                .long("cluster")
                .help("The target cluster (default: the target DC's first active cluster)"),
        )
        .arg(
            Arg::new("wait")
                .long("wait")
                .action(ArgAction::SetTrue)
                .help("Return once the move committed or rolled back"),
        )
        .arg(
            Arg::new("status")
                .long("status")
                .action(ArgAction::SetTrue)
                .conflicts_with("to")
                .help("Show the move in progress, or the last one"),
        )
}

/// `sprigctl tenant traffic`.
pub(crate) fn traffic_command() -> Command {
    Command::new("traffic")
        .about("A tenant's traffic by client data center, and the move it suggests (chapter 24)")
        .arg(Arg::new("name").required(true).help("The tenant"))
}

fn ms(v: Option<u64>) -> String {
    v.map_or_else(|| "-".to_owned(), |m| format!("{m} ms"))
}

fn show_move(v: &MoveView) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let mut line = |k: &str, val: String| {
        let _ = writeln!(s, "{k:<18}{val}");
    };
    line("tenant", format!("{} ({})", v.tenant, v.tenant_id));
    line("state", v.state.clone());
    line("home dc", format!("{} (epoch {})", v.home_dc, v.epoch));
    let m = v
        .migration
        .as_ref()
        .or_else(|| v.last_move.as_ref().and_then(|l| l.get("migration")));
    if let Some(m) = m {
        let g = |k: &str| m.get(k).and_then(|x| x.as_str()).unwrap_or("-").to_owned();
        let label = if v.dry_run {
            "would move"
        } else if v.migration.is_some() {
            "moving"
        } else {
            "last move"
        };
        line(
            label,
            format!(
                "{} ({}) -> {} ({})",
                g("from"),
                g("source_cluster"),
                g("to"),
                g("target_cluster")
            ),
        );
        line(
            "pause budget",
            ms(m.get("max_pause_ms").and_then(serde_json::Value::as_u64)),
        );
    }
    if let Some(j) = &v.job {
        line("job", format!("{} ({})", j.id.0, j.state));
        line(
            "copy",
            format!(
                "{}/{} tables, {} rows copied, {} applied, lag {}",
                j.progress.tables_done,
                j.progress.tables,
                j.progress.rows_copied,
                j.progress.rows_applied,
                ms(j.progress.lag_ms)
            ),
        );
        line(
            "cutovers",
            format!(
                "{} attempts, holds {:?} ms",
                j.progress.cutover_attempts, j.progress.holds_ms
            ),
        );
        if let Some(e) = &j.error {
            line("job error", e.clone());
        }
    }
    line("write pause", ms(v.pause_ms));
    line("start to commit", ms(v.duration_ms));
    if let Some(e) = &v.last_error {
        line(
            "last rollback",
            e.get("error")
                .and_then(|x| x.as_str())
                .unwrap_or("-")
                .to_owned(),
        );
    }
    s
}

fn show_traffic(v: &TrafficView) -> String {
    use std::fmt::Write as _;
    let rows: Vec<[String; 4]> = v
        .clients
        .iter()
        .map(|c| {
            let dc = match c.client_dc.as_str() {
                "" => "(none)".to_owned(),
                "?" => "(unknown)".to_owned(),
                d => d.to_owned(),
            };
            [
                dc,
                c.reads.to_string(),
                c.writes.to_string(),
                c.weighted.to_string(),
            ]
        })
        .collect();
    let mut s = format!(
        "tenant {} homed in {}; window {} min ({} reported)\n",
        v.tenant, v.home_dc, v.window_minutes, v.minutes_reported
    );
    s.push_str(&table(["CLIENT DC", "READS", "WRITES", "WEIGHTED"], &rows));
    let _ = writeln!(
        s,
        "weighted with DC ids: {} ({})",
        v.weighted_total,
        if v.signal {
            "enough signal"
        } else {
            "not enough signal"
        }
    );
    for (dc, pct) in &v.cross_dc_pct {
        let _ = writeln!(
            s,
            "  homed in {dc}: {pct:.1}% of weighted traffic crosses DCs"
        );
    }
    let _ = writeln!(
        s,
        "decision: {}{}",
        v.decision,
        v.candidate.as_ref().map_or(String::new(), |c| format!(
            " (move to {c}: sprigctl tenant move {} --to {c})",
            v.tenant
        ))
    );
    s
}

/// Runs `tenant move` or `tenant traffic`.
pub(crate) async fn run(
    c: &AdminClient,
    cmd: &str,
    m: &ArgMatches,
    dry_run: bool,
    out: Output,
) -> Result<(), Failed> {
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if cmd == "traffic" {
        let v: TrafficView = c
            .get(&format!("/admin/v1/tenants/{}/traffic", escape(&name)))
            .await?;
        print(out, &v, show_traffic);
        return Ok(());
    }
    let path = format!("/admin/v1/tenants/{}/move", escape(&name));
    if m.get_flag("status") {
        let v: MoveView = c.get(&path).await?;
        print(out, &v, show_move);
        return Ok(());
    }
    let req = MoveTenantRequest {
        to: m.get_one::<String>("to").cloned().unwrap_or_default(),
        dry_run,
        max_pause_ms: m.get_one::<u64>("max-pause-ms").copied(),
        cluster: m.get_one::<String>("cluster").cloned(),
    };
    let mut v: MoveView = c
        .post(&format!("/admin/v1/tenants/{}:move", escape(&name)), &req)
        .await?;
    if m.get_flag("wait") && !dry_run {
        let id = v
            .migration
            .as_ref()
            .and_then(|x| x.get("id"))
            .and_then(|x| x.as_str())
            .map(str::to_owned);
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            // The new home answers once the move committed: ask any
            // controller through the same endpoint (reads are answered
            // everywhere).
            v = c.get(&path).await?;
            let still = v
                .migration
                .as_ref()
                .and_then(|x| x.get("id"))
                .and_then(|x| x.as_str());
            if v.state != "migrating" || still != id.as_deref() {
                break;
            }
        }
    }
    print(out, &v, show_move);
    let failed = !dry_run && m.get_flag("wait") && v.home_dc.as_str() != req.to;
    if failed {
        return Err(Failed(format!("the move to {} rolled back", req.to)));
    }
    Ok(())
}
