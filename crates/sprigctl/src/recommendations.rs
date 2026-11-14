//! `sprigctl recommendations`: the approvals inbox for
//! operators, over the controller's `Recommendations` service.
//!
//! - `list [--tenant T] [--level L] [--kind K] [--state S] [--all]`
//! - `show ID`
//! - `approve ID [--reason R] [--window START/END]`
//! - `ignore ID --reason R [--days N | --forever]`
//! - `snooze ID --hours N`
//! - `comment ID TEXT`
//! - `policy [--tenant T] [--db D] [--object O]`

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde_json::{Value, json};
use sprig_approvals::events::{parse_rfc3339, rfc3339};
use sprig_controller::approvals::service::{ListResponse, PolicyResponse, RecView};

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

fn id_arg() -> Arg {
    Arg::new("id")
        .required(true)
        .help("A recommendation id: t<tenant>.<n>, dc.<dc>.<n> or g.<n>")
}

fn actor() -> Arg {
    Arg::new("actor")
        .long("actor")
        .env("SPRIGCTL_ACTOR")
        .help("Who, recorded as op:<actor> [default: $USER]")
}

fn reason() -> Arg {
    Arg::new("reason")
        .long("reason")
        .help("Why (logged with the decision)")
}

/// `sprigctl recommendations`.
pub(crate) fn group() -> Command {
    Command::new("recommendations")
        .about("Recommendations held for approval, and their policies")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("Waiting recommendations (--all adds closed ones)")
                .arg(Arg::new("tenant").long("tenant").env("SPRIGCTL_TENANT").help("Only this tenant"))
                .arg(Arg::new("level").long("level").help("table, database, tenant, dc or platform"))
                .arg(Arg::new("kind").long("kind").help("A kind (index.create) or family (index)"))
                .arg(Arg::new("state").long("state").help("open, approved, applying, applied, failed, ignored, snoozed, superseded or expired"))
                .arg(Arg::new("db").long("db").help("Only this database"))
                .arg(Arg::new("table").long("table").help("Only this table"))
                .arg(Arg::new("all").long("all").action(ArgAction::SetTrue).help("Closed items too")),
        )
        .subcommand(Command::new("show").about("One recommendation with its evidence and history").arg(id_arg()))
        .subcommand(
            Command::new("approve")
                .about("Approve; it is applied on the deciding component's next cycle")
                .arg(id_arg())
                .arg(reason())
                .arg(actor())
                .arg(Arg::new("window").long("window").help("Apply only between START/END (RFC 3339, UTC)")),
        )
        .subcommand(
            Command::new("ignore")
                .about("Ignore: once, for N days or forever")
                .arg(id_arg())
                .arg(reason().required(true))
                .arg(actor())
                .arg(Arg::new("days").long("days").value_parser(clap::value_parser!(u32)).conflicts_with("forever"))
                .arg(Arg::new("forever").long("forever").action(ArgAction::SetTrue)),
        )
        .subcommand(
            Command::new("snooze")
                .about("Out of the inbox for N hours")
                .arg(id_arg())
                .arg(actor())
                .arg(Arg::new("hours").long("hours").required(true).value_parser(clap::value_parser!(u32))),
        )
        .subcommand(
            Command::new("reopen")
                .about("Back to open (from ignored, expired, failed or snoozed)")
                .arg(id_arg())
                .arg(reason())
                .arg(actor()),
        )
        .subcommand(
            Command::new("comment")
                .about("Add a comment")
                .arg(id_arg())
                .arg(Arg::new("text").required(true))
                .arg(actor()),
        )
        .subcommand(
            Command::new("policy")
                .about("Every kind's mode, level and timers at a scope")
                .arg(Arg::new("tenant").long("tenant").env("SPRIGCTL_TENANT"))
                .arg(Arg::new("db").long("db"))
                .arg(Arg::new("object").long("object")),
        )
}

fn s(m: &ArgMatches, k: &str) -> Option<String> {
    m.try_get_one::<String>(k).ok().flatten().cloned()
}

fn list_path(m: &ArgMatches) -> String {
    let mut q = Vec::new();
    for k in ["tenant", "level", "kind", "state", "db", "table"] {
        if let Some(v) = s(m, k) {
            q.push(format!("{k}={}", escape(&v)));
        }
    }
    if m.get_flag("all") {
        q.push("all=true".into());
    }
    if q.is_empty() {
        "/admin/v1/recommendations".into()
    } else {
        format!("/admin/v1/recommendations?{}", q.join("&"))
    }
}

/// The body of an action, from the subcommand's arguments.
pub(crate) fn body(cmd: &str, m: &ArgMatches) -> Result<Value, Failed> {
    let actor = s(m, "actor").or_else(|| std::env::var("USER").ok());
    let mut b = match cmd {
        "approve" => {
            let window = match s(m, "window") {
                None => Value::Null,
                Some(w) => {
                    let (a, z) = w
                        .split_once('/')
                        .ok_or_else(|| Failed("--window is START/END".into()))?;
                    let (Some(a), Some(z)) = (parse_rfc3339(a), parse_rfc3339(z)) else {
                        return Err(Failed(
                            "--window times are RFC 3339, such as 2026-11-11T20:00:00Z".into(),
                        ));
                    };
                    json!({"start_us": a, "end_us": z})
                }
            };
            json!({"reason": s(m, "reason").unwrap_or_default(), "window": window})
        }
        "ignore" => json!({
            "reason": s(m, "reason").unwrap_or_default(),
            "for_days": m.try_get_one::<u32>("days").ok().flatten(),
            "forever": m.get_flag("forever"),
        }),
        "snooze" => {
            json!({"hours": m.try_get_one::<u32>("hours").ok().flatten().copied().unwrap_or(0)})
        }
        "reopen" => json!({"reason": s(m, "reason").unwrap_or_default()}),
        "comment" => json!({"text": s(m, "text").unwrap_or_default()}),
        _ => return Err(Failed(format!("unknown action {cmd}"))),
    };
    if let (Value::Object(o), Some(a)) = (&mut b, actor) {
        o.insert("actor".into(), Value::String(a));
    }
    Ok(b)
}

/// `sprigctl recommendations ...`.
pub(crate) async fn run(
    c: &AdminClient,
    m: &ArgMatches,
    group: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("a subcommand is required".into()));
    };
    match cmd {
        "list" => {
            let r: ListResponse = c.get(&list_path(sub)).await?;
            print(out, &r, list_text);
        }
        "show" => {
            let id = s(sub, "id").unwrap_or_default();
            let r: RecView = c
                .get(&format!("/admin/v1/recommendations/{}", escape(&id)))
                .await?;
            print(out, &r, show_text);
        }
        "policy" => {
            let mut q = Vec::new();
            for k in ["tenant", "db", "object"] {
                if let Some(v) = s(sub, k) {
                    q.push(format!("{k}={}", escape(&v)));
                }
            }
            let path = format!("/admin/v1/recommendations:policy?{}", q.join("&"));
            let r: PolicyResponse = c.get(&path).await?;
            print(out, &r, policy_text);
        }
        _ => {
            let id = s(sub, "id").unwrap_or_default();
            let b = body(cmd, sub)?;
            if m.get_flag("dry-run") {
                println!("would {cmd} recommendation {id}: {b}");
                return Ok(());
            }
            let r: RecView = c
                .post(
                    &format!("/admin/v1/recommendations/{}:{cmd}", escape(&id)),
                    &b,
                )
                .await?;
            print(out, &r, |r| format!("{} is {}\n", r.rec.id, r.rec.state));
        }
    }
    Ok(())
}

/// The table of `list`.
pub(crate) fn list_text(r: &ListResponse) -> String {
    let rows: Vec<[String; 7]> = r
        .items
        .iter()
        .map(|i| {
            [
                i.rec.id.to_string(),
                i.rec.kind.to_string(),
                i.rec.level.to_string(),
                i.rec.state.to_string(),
                i.rec.subject.display(),
                format!("{:.1}", i.age_hours),
                i.rec.summary.clone(),
            ]
        })
        .collect();
    let mut t = table(
        ["ID", "KIND", "LEVEL", "STATE", "TARGET", "AGE_H", "SUMMARY"],
        &rows,
    );
    let _ = writeln!(t, "{} open", r.open);
    t
}

/// The text of `show`.
pub(crate) fn show_text(v: &RecView) -> String {
    let r = &v.rec;
    let mut o = String::new();
    let at = |u: Option<i64>| u.map_or_else(|| "-".to_owned(), rfc3339);
    let _ = writeln!(
        o,
        "{}  {}  {}  ({} level, notifies {})",
        r.id, r.kind, r.state, r.level, r.notify_level
    );
    let _ = writeln!(o, "target:      {}", r.subject.display());
    let _ = writeln!(o, "summary:     {}", r.summary);
    let _ = writeln!(o, "action:      {}", r.action.text);
    let _ = writeln!(
        o,
        "mode from:   {}{}",
        r.mode_source,
        if r.advisory { " (advice)" } else { "" }
    );
    let _ = writeln!(
        o,
        "raised:      {} ({:.1} h ago, proposed {} more times)",
        rfc3339(r.created_at_us),
        v.age_hours,
        r.refreshes
    );
    let _ = writeln!(
        o,
        "escalates:   {}\nauto-apply:  {}",
        at(r.escalate_at_us),
        at(r.auto_apply_at_us)
    );
    if let Some(w) = r.window {
        let _ = writeln!(
            o,
            "window:      {} to {}",
            rfc3339(w.start_us),
            rfc3339(w.end_us)
        );
    }
    if let Some(d) = &r.decided_by {
        let _ = writeln!(
            o,
            "decided by:  {d} ({})",
            r.reason.clone().unwrap_or_default()
        );
    }
    if let Some(x) = &r.result {
        let _ = writeln!(o, "result:      {x}");
    }
    let _ = writeln!(o, "evidence:    {}", r.evidence);
    for h in &r.history {
        let _ = writeln!(
            o,
            "  {}  {} -> {}  by {}  {}",
            rfc3339(h.at_us),
            h.from,
            h.to,
            h.by,
            h.note
        );
    }
    for c in &r.comments {
        let _ = writeln!(o, "  # {} {}: {}", rfc3339(c.at_us), c.author, c.body);
    }
    o
}

/// The table of `policy`.
pub(crate) fn policy_text(p: &PolicyResponse) -> String {
    let rows: Vec<[String; 6]> = p
        .kinds
        .iter()
        .map(|k| {
            [
                k.kind.clone(),
                k.mode.clone(),
                k.mode_source.clone(),
                k.level.to_string(),
                k.escalate_after.clone().unwrap_or_else(|| "off".into()),
                k.auto_apply_after.clone().unwrap_or_else(|| "off".into()),
            ]
        })
        .collect();
    table(
        ["KIND", "MODE", "FROM", "LEVEL", "ESCALATE", "AUTO_APPLY"],
        &rows,
    )
}
