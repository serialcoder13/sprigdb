//! `sprigctl index` and `sprigctl explain query` (chapter 13, "`recommend`
//! mode" and the interfaces for people; chapter 47's reference), over the
//! Admin API's automatic-indexing calls.
//!
//! ```text
//! sprigctl index list --tenant acme [--db shop] [--table orders] [--state active] [--recommended] [--all]
//! sprigctl index show <index> --tenant acme
//! sprigctl index recommendations --tenant acme [--db shop] [--table orders]
//! sprigctl index approve <index> --tenant acme [--reason ...]     (alias apply)
//! sprigctl index reject <index> --tenant acme [--reason ...]      (alias dismiss)
//! sprigctl index drop <index> --tenant acme [--reason ...] --yes
//! sprigctl index pin <index> --tenant acme [--off]                 (alias freeze)
//! sprigctl index unpin <index> --tenant acme
//! sprigctl index restore <index> --tenant acme                     (decision O1)
//! sprigctl explain query <fingerprint> --tenant acme
//! ```
//!
//! `<index>` is an id from `index list`, or `[<db>.][<table>.]sprig_auto_<hex>`;
//! a user or migrated index that automatic indexing retired, recommended for
//! removal or pinned is named `[<db>.][<table>.]<name>`. `restore` makes a
//! retiring (`IGNORED`) index visible again, or recreates a user index
//! dropped for disuse from its saved definition, and pins it.
//! `--tenant` may come from `SPRIGCTL_TENANT`. Writes record
//! `op:<actor>` (`--actor`, `SPRIGCTL_ACTOR`, then `USER`).

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::autoindex_admin::view::{
    CandidateExplain, CandidateView, IndexActionRequest, IndexActionResponse, IndexDetail,
    ListIndexesResponse, QueryExplanation, ValidationView,
};

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, confirm, print};

fn tenant() -> Arg {
    Arg::new("tenant")
        .long("tenant")
        .env("SPRIGCTL_TENANT")
        .required(true)
        .help("The tenant")
}

fn index_arg() -> Arg {
    Arg::new("index").required(true).help(
        "An index id from `index list`, [<db>.][<table>.]sprig_auto_<hex>, or a user \
             index as [<db>.][<table>.]<name>",
    )
}

fn filters(c: Command) -> Command {
    c.arg(tenant())
        .arg(Arg::new("db").long("db").help("Only this database"))
        .arg(Arg::new("table").long("table").help("Only this table"))
}

fn write_cmd(name: &'static str, about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .arg(index_arg())
        .arg(tenant())
        .arg(
            Arg::new("reason")
                .long("reason")
                .help("Why (logged with the decision)"),
        )
        .arg(
            Arg::new("actor")
                .long("actor")
                .env("SPRIGCTL_ACTOR")
                .help("Who, recorded as op:<actor> [default: $USER]"),
        )
}

/// `sprigctl index`.
pub(crate) fn group() -> Command {
    Command::new("index")
        .about("Automatic indexes and recommendations")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            filters(
                Command::new("list")
                    .about("List automatic indexes (live ones; --all adds dropped and rejected)"),
            )
            .arg(Arg::new("state").long("state").help(
                "Only this state: validating, recommended, rolling_out, observing, active, \
                     retiring, dropping, dropped, rejected, failed, frozen",
            ))
            .arg(
                Arg::new("recommended")
                    .long("recommended")
                    .action(ArgAction::SetTrue)
                    .conflicts_with("state")
                    .help("Only recommendations (as `index recommendations`)"),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .action(ArgAction::SetTrue)
                    .help("Also dropped and rejected indexes (history)"),
            ),
        )
        .subcommand(
            Command::new("show")
                .about("One index: definition, evidence, validations, history")
                .arg(index_arg())
                .arg(tenant()),
        )
        .subcommand(filters(Command::new("recommendations").about(
            "Recommendations waiting for approval (recommend mode, advice)",
        )))
        .subcommand(
            write_cmd("approve", "Roll a recommendation out as an automatic index")
                .visible_alias("apply"),
        )
        .subcommand(
            write_cmd(
                "reject",
                "Turn a recommendation down (not recommended again for a while)",
            )
            .visible_alias("dismiss"),
        )
        .subcommand(write_cmd(
            "drop",
            "Drop an automatic index from every shard",
        ))
        .subcommand(
            write_cmd("pin", "Freeze an automatic index in its state")
                .visible_alias("freeze")
                .arg(
                    Arg::new("off")
                        .long("off")
                        .action(ArgAction::SetTrue)
                        .help("Unpin instead (chapter 13's `freeze --off`)"),
                ),
        )
        .subcommand(write_cmd(
            "unpin",
            "Let automatic indexing manage a pinned index again",
        ))
        .subcommand(write_cmd(
            "restore",
            "Make a retiring (IGNORED) index visible again, or recreate a user index dropped \
             for disuse, and pin it",
        ))
}

/// `sprigctl explain`: `query` here; `decision` and `placement` in
/// [`crate::placement`].
pub(crate) fn explain_group() -> Command {
    Command::new("explain")
        .about("Explain a query fingerprint, a decision or a tenant's placement")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("query")
                .about("Why an index was or wasn't built for a query shape")
                .arg(
                    Arg::new("fingerprint")
                        .required(true)
                        .help("The fingerprint, 16 hex digits"),
                )
                .arg(tenant()),
        )
        .subcommand(crate::placement::decision_cmd())
        .subcommand(crate::placement::placement_cmd())
}

fn tenant_of(sub: &ArgMatches) -> String {
    sub.get_one::<String>("tenant").cloned().unwrap_or_default()
}

/// The list path with its filters.
pub(crate) fn list_path(sub: &ArgMatches, recommendations: bool) -> String {
    let tenant = escape(&tenant_of(sub));
    let recommended = !recommendations
        && sub
            .try_get_one::<bool>("recommended")
            .ok()
            .flatten()
            .copied()
            .unwrap_or(false);
    let mut q = Vec::new();
    for k in ["db", "table", "state"] {
        if let Some(v) = sub.try_get_one::<String>(k).ok().flatten() {
            q.push(format!("{k}={}", escape(v)));
        }
    }
    if sub.try_get_one::<bool>("all").ok().flatten().copied() == Some(true) {
        q.push("all=true".into());
    }
    let what = if recommendations || recommended {
        "recommendations"
    } else {
        "indexes"
    };
    let mut p = format!("/admin/v1/tenants/{tenant}/{what}");
    if !q.is_empty() {
        p.push('?');
        p.push_str(&q.join("&"));
    }
    p
}

/// The path of one index, with an optional custom method.
pub(crate) fn index_path(tenant: &str, index: &str, method: &str) -> String {
    format!(
        "/admin/v1/tenants/{}/indexes/{}{method}",
        escape(tenant),
        escape(index)
    )
}

/// The action a write subcommand names (`pin --off` is `unpin`).
pub(crate) fn action_of(cmd: &str, sub: &ArgMatches) -> &'static str {
    match cmd {
        "approve" | "apply" => "approve",
        "reject" | "dismiss" => "reject",
        "drop" => "drop",
        "pin" | "freeze" if sub.get_flag("off") => "unpin",
        "pin" | "freeze" => "pin",
        "restore" => "restore",
        _ => "unpin",
    }
}

/// `sprigctl index ...`.
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
        "list" | "recommendations" => {
            let recs = cmd == "recommendations";
            let r: ListIndexesResponse = c.get(&list_path(sub, recs)).await?;
            let recs = recs || sub.try_get_one::<bool>("recommended").ok().flatten() == Some(&true);
            print(out, &r, |r| index_list(r, recs));
            Ok(())
        }
        "show" => {
            let index = sub.get_one::<String>("index").cloned().unwrap_or_default();
            let r: IndexDetail = c.get(&index_path(&tenant_of(sub), &index, "")).await?;
            print(out, &r, index_show);
            Ok(())
        }
        _ => {
            let tenant = tenant_of(sub);
            let index = sub.get_one::<String>("index").cloned().unwrap_or_default();
            let action = action_of(cmd, sub);
            if m.get_flag("dry-run") {
                println!("would {action} index {index} of tenant {tenant}");
                return Ok(());
            }
            if action == "drop" {
                confirm(m, "drop automatic index", &index)?;
            }
            let req = IndexActionRequest {
                reason: sub.get_one::<String>("reason").cloned().unwrap_or_default(),
                actor: sub
                    .get_one::<String>("actor")
                    .cloned()
                    .or_else(|| std::env::var("USER").ok())
                    .filter(|a| !a.trim().is_empty()),
            };
            let r: IndexActionResponse = c
                .post(&index_path(&tenant, &index, &format!(":{action}")), &req)
                .await?;
            print(out, &r, action_text);
            Ok(())
        }
    }
}

/// `sprigctl explain query`.
pub(crate) async fn explain(
    c: &AdminClient,
    group: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let sub = match group.subcommand() {
        Some(("query", sub)) => sub,
        Some((cmd @ ("decision" | "placement"), sub)) => {
            return crate::placement::run(c, cmd, sub, out).await;
        }
        _ => return Err(Failed("not implemented yet".into())),
    };
    let fp = sub
        .get_one::<String>("fingerprint")
        .cloned()
        .unwrap_or_default();
    let r: QueryExplanation = c
        .get(&format!(
            "/admin/v1/tenants/{}/queries/{}:explain",
            escape(&tenant_of(sub)),
            escape(&fp)
        ))
        .await?;
    print(out, &r, explain_text);
    Ok(())
}

// ---- text output ----

/// Bytes as B, KiB, MiB, GiB, TiB with one decimal.
#[allow(clippy::cast_precision_loss)] // display only
pub(crate) fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < UNITS.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

fn ms(v: f64) -> String {
    if v.abs() >= 100.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
    }
}

/// `sprigctl index list` and `recommendations`.
pub(crate) fn index_list(r: &ListIndexesResponse, recommendations: bool) -> String {
    if r.indexes.is_empty() {
        return if recommendations {
            format!("no recommendations for tenant {}\n", r.tenant)
        } else {
            format!("no automatic indexes in tenant {}\n", r.tenant)
        };
    }
    let rows: Vec<[String; 7]> = r
        .indexes
        .iter()
        .map(|i| {
            [
                i.index_id.to_string(),
                format!("{}.{}", i.db, i.table),
                i.name.clone(),
                if i.columns.is_empty() {
                    i.kind.clone()
                } else {
                    i.columns.join(", ")
                },
                match (&i.drop_reason, &i.origin) {
                    (Some(d), Some(o)) => format!("{} ({d}; {o} index)", i.state),
                    (None, Some(o)) => format!("{} ({o} index)", i.state),
                    (Some(d), None) => format!("{} ({d})", i.state),
                    (None, None) => i.state.clone(),
                },
                ms(i.est_net_ms_h),
                bytes(i.actual_bytes.unwrap_or(i.est_bytes)),
            ]
        })
        .collect();
    table(
        [
            "ID", "TABLE", "NAME", "COLUMNS", "STATE", "NET MS/H", "SIZE",
        ],
        &rows,
    )
}

fn validation_line(v: &ValidationView) -> String {
    let mut s = format!(
        "{} {} on {}/{}: {}",
        v.validation_id, v.method, v.cluster, v.node, v.verdict
    );
    if let (Some(b), Some(a)) = (v.target_p95_before_ms, v.target_p95_after_ms) {
        let _ = write!(s, ", target p95 {} -> {} ms", ms(b), ms(a));
        if let Some(p) = v.improvement_pct {
            let _ = write!(s, " ({p:.1}% better)");
        }
    }
    if let Some(w) = v.worst_regression_pct {
        let _ = write!(s, ", worst regression {w:.1}%");
        if let Some(f) = &v.worst_fingerprint {
            let _ = write!(s, " ({f})");
        }
    }
    let _ = write!(s, ", {} statements", v.statements);
    if v.noisy > 0 {
        let _ = write!(s, " ({} noisy)", v.noisy);
    }
    if let Some(r) = &v.reason {
        let _ = write!(s, ": {r}");
    }
    s
}

fn evidence_lines(c: &CandidateView, indent: &str) -> String {
    let mut s = String::new();
    if !c.selectivity.is_empty() {
        let sel: Vec<String> = c
            .selectivity
            .iter()
            .map(|(col, v)| {
                let src = c
                    .selectivity_source
                    .get(col)
                    .map(|x| format!(" ({x})"))
                    .unwrap_or_default();
                format!("{col} {v:.2e}{src}")
            })
            .collect();
        let _ = writeln!(s, "{indent}selectivity:   {}", sel.join(", "));
    }
    if !c.part_sources.is_empty() {
        let _ = writeln!(s, "{indent}parts from:    {}", c.part_sources.join(", "));
    }
    if let Some(e) = c.entries_per_run {
        let _ = writeln!(s, "{indent}entries/run:   {}", ms(e));
    }
    if let (Some(b), Some(w)) = (c.benefit_ms_h, c.write_ms_h) {
        let _ = write!(
            s,
            "{indent}cost:          B {} ms/h, Wc {} ms/h, net {} ms/h",
            ms(b),
            ms(w),
            ms(b - w)
        );
        if let Some(n) = c.est_bytes {
            let _ = write!(s, ", {}", bytes(n));
        }
        if let Some(t) = c.build_seconds {
            let _ = write!(s, ", build {t} s");
        }
        s.push('\n');
    }
    for n in &c.notes {
        let _ = writeln!(s, "{indent}note:          {n}");
    }
    s
}

/// `sprigctl index show`.
pub(crate) fn index_show(d: &IndexDetail) -> String {
    let i = &d.index;
    let mut s = String::new();
    let line = |s: &mut String, k: &str, v: &str| {
        let _ = writeln!(s, "{k:<15}{v}");
    };
    line(&mut s, "index:", &format!("{} ({})", i.name, i.index_id));
    line(&mut s, "tenant:", &d.tenant);
    line(&mut s, "table:", &format!("{}.{}", i.db, i.table));
    line(&mut s, "kind:", &format!("{} ({})", i.kind, i.model));
    if !i.columns.is_empty() {
        line(&mut s, "columns:", &i.columns.join(", "));
    }
    let state = match (&i.drop_reason, &i.prior_state) {
        (Some(r), _) => format!("{} ({r})", i.state),
        (None, Some(p)) => format!("{} (was {p})", i.state),
        _ => i.state.clone(),
    };
    line(
        &mut s,
        "state:",
        &format!("{state} since {}", i.state_changed_at),
    );
    line(
        &mut s,
        "estimate:",
        &format!(
            "B {} ms/h, Wc {} ms/h, net {} ms/h, {}",
            ms(i.est_benefit_ms_h),
            ms(i.est_write_ms_h),
            ms(i.est_net_ms_h),
            bytes(i.est_bytes)
        ),
    );
    if let Some(a) = i.actual_bytes {
        line(&mut s, "size:", &bytes(a));
    }
    if !i.shards.is_empty() {
        let sh: Vec<String> = i.shards.iter().map(|(k, v)| format!("{k} {v}")).collect();
        line(&mut s, "shards:", &sh.join(", "));
    }
    if let Some(u) = &i.last_used_at {
        line(&mut s, "last used:", u);
    }
    if let Some(ddl) = &i.ddl {
        line(&mut s, "ddl:", ddl);
    }
    if !i.fingerprints.is_empty() {
        s.push_str("fingerprints:\n");
        let rows: Vec<[String; 5]> = i
            .fingerprints
            .iter()
            .map(|f| {
                [
                    format!("  {}", f.id),
                    f.role.clone().unwrap_or_else(|| "-".into()),
                    f.runs.map_or_else(|| "-".into(), |r| r.to_string()),
                    f.p95_ms.map_or_else(|| "-".into(), ms),
                    f.mean_ms.map_or_else(|| "-".into(), ms),
                ]
            })
            .collect();
        s.push_str(&table(
            ["  FINGERPRINT", "ROLE", "RUNS", "P95 MS", "MEAN MS"],
            &rows,
        ));
    }
    if let Some(c) = &d.candidate {
        let _ = writeln!(
            s,
            "candidate:     {} (run {}): {}{}",
            c.candidate_id,
            c.run_id,
            c.status,
            c.reason_text
                .as_deref()
                .map(|r| format!(": {r}"))
                .unwrap_or_default()
        );
        s.push_str(&evidence_lines(c, "  "));
    }
    for v in &d.validations {
        let _ = writeln!(s, "validation:    {}", validation_line(v));
    }
    if !d.history.is_empty() {
        s.push_str("history:\n");
        for e in &d.history {
            let _ = writeln!(s, "  {}  {:<10}  {}", e.at, e.event, e.detail);
        }
    }
    s
}

/// The output of `approve`, `reject`, `drop`, `pin`, `unpin`.
pub(crate) fn action_text(r: &IndexActionResponse) -> String {
    let i = &r.index;
    if r.changed {
        format!(
            "{} {} on {}.{} (tenant {}): {} -> {}\n{}\n",
            r.action, i.name, i.db, i.table, r.tenant, r.from, r.to, r.message
        )
    } else {
        format!("nothing to do: {}\n", r.message)
    }
}

fn candidate_block(s: &mut String, c: &CandidateExplain) {
    let v = &c.candidate;
    if v.columns.is_empty() {
        let _ = writeln!(
            s,
            "\n{}.{} (run {}): no candidate: {}",
            v.db,
            v.table,
            v.run_id,
            v.reason_text.as_deref().unwrap_or("no reason recorded")
        );
        for n in &v.notes {
            let _ = writeln!(s, "  note:          {n}");
        }
        return;
    }
    let _ = writeln!(
        s,
        "\ncandidate {} on {}.{} ({}){}, {}, run {}: {}{}",
        v.candidate_id,
        v.db,
        v.table,
        v.columns.join(", "),
        if v.covering { ", covering" } else { "" },
        c.role,
        v.run_id,
        v.status,
        v.reason_text
            .as_deref()
            .map(|r| format!(": {r}"))
            .unwrap_or_default()
    );
    s.push_str(&evidence_lines(v, "  "));
    for g in &c.gates {
        let verdict = match g.passed {
            Some(true) => "pass",
            Some(false) => "FAIL",
            None => "-",
        };
        let _ = writeln!(s, "  gate {:<14} {verdict:<4}  {}", g.gate, g.detail);
    }
    for val in &c.validations {
        let _ = writeln!(s, "  validation:    {}", validation_line(val));
    }
    if let Some(i) = &c.index {
        let _ = writeln!(
            s,
            "  index:         {} {} ({}{})",
            i.index_id,
            i.name,
            i.state,
            i.drop_reason
                .as_deref()
                .map(|d| format!(", {d}"))
                .unwrap_or_default()
        );
    }
}

/// `sprigctl explain query`.
pub(crate) fn explain_text(e: &QueryExplanation) -> String {
    let mut s = format!(
        "fingerprint {} in tenant {} (evidence since {})\n",
        e.fingerprint, e.tenant, e.since
    );
    for l in &e.summary {
        let _ = writeln!(s, "  {l}");
    }
    for c in &e.candidates {
        candidate_block(&mut s, c);
    }
    for p in &e.policies {
        let _ = writeln!(s, "\npolicy of {}.{}: {}", p.db, p.table, p.route);
        for (k, v) in &p.settings {
            let _ = writeln!(s, "  {k:<38} {v}");
        }
    }
    s
}
