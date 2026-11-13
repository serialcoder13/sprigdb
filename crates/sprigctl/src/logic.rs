//! `sprigctl logic` (chapter 41, "Declaring code"; R7 order 1):
//!
//! - `compile <file>`: compiles a KIRun definition offline and prints its
//!   IR, placement and PL/SQL (what `EXPLAIN FUNCTION ... FORMAT=SQL`
//!   will show); `--table name:col=type,...` declares the tables its SQL
//!   reads (types `int`, `float`, `str`, `json`, `bool`);
//! - `deploy <file> --db <db> --gateway <url>`: sends `CREATE OR REPLACE
//!   PROCEDURE ... LANGUAGE kirun AS '...'` through a gateway as the user
//!   in the URL, and prints the notes the deploy left;
//! - `list` and `show <name>` read the tenant's logic catalog directly,
//!   `--catalog mysql://...@host:port/t<tenant>_sprig` (an operator's
//!   connection), until the Admin API serves them.
//!
//! - `logs [name] [--limit n] [--follow]`: what after-commit triggers and
//!   schedules did (`logic_log`), oldest first;
//! - `dead-letters list [--binding b] [--state s]`, `dead-letters replay
//!   <id>... | --all [--binding b]` (back to the trigger's worker, which
//!   runs them in order with the active version) and `dead-letters
//!   discard <id>... | --all [--binding b]`.
//!
//! - `rollback <name> --db <db> [--to-version n] [--procedure] --gateway
//!   <url>`: sends `ALTER FUNCTION | PROCEDURE ... ROLLBACK [TO VERSION n]`
//!   through a gateway (R7 close-out) and prints the notes.

use clap::{Arg, ArgAction, ArgMatches, Command};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use sprig_logic::LogicStore;
use sprig_logic::declare::TierPref;
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, Runtime, StaticColumns, compile};

use crate::{Failed, Output};

/// `sprigctl logic`.
#[allow(clippy::too_many_lines)] // one builder chain
pub(crate) fn group() -> Command {
    let file = || {
        Arg::new("file")
            .required(true)
            .help("A KIRun definition (.json)")
    };
    let catalog = || {
        Arg::new("catalog")
            .long("catalog")
            .required(true)
            .help("The tenant's catalog: mysql://user:pass@host:port/t<tenant>_sprig")
    };
    Command::new("logic")
        .about("Logic functions")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("compile")
                .about("Compile a KIRun definition and print its IR, placement and PL/SQL")
                .arg(file())
                .arg(
                    Arg::new("table")
                        .long("table")
                        .action(ArgAction::Append)
                        .help("A table its SQL reads: name:col=type,col=type"),
                ),
        )
        .subcommand(
            Command::new("deploy")
                .about("Deploy a KIRun definition through a gateway (CREATE OR REPLACE PROCEDURE ... LANGUAGE kirun)")
                .arg(file())
                .arg(Arg::new("db").long("db").required(true).help("The database"))
                .arg(Arg::new("name").long("name").help("The routine's name (default: the file's stem)"))
                .arg(
                    Arg::new("gateway")
                        .long("gateway")
                        .required(true)
                        .help("mysql://tenant.user:password@host:port"),
                )
                .arg(
                    Arg::new("tier")
                        .long("tier")
                        .value_parser(["auto", "mariadb", "engine"])
                        .default_value("auto"),
                )
                .arg(
                    Arg::new("definer")
                        .long("definer")
                        .action(ArgAction::SetTrue)
                        .help("SQL SECURITY DEFINER"),
                ),
        )
        .subcommand(Command::new("list").about("List logic modules").arg(catalog()))
        .subcommand(
            Command::new("show")
                .about("Show a module's versions and placement")
                .arg(Arg::new("name").required(true))
                .arg(catalog()),
        )
        .subcommand(
            Command::new("logs")
                .about("What triggers and schedules did (logic_log), oldest first")
                .arg(Arg::new("name").help("A module's name (default: every module)"))
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_parser(clap::value_parser!(u32))
                        .default_value("50"),
                )
                .arg(
                    Arg::new("follow")
                        .long("follow")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Keep printing new lines"),
                )
                .arg(catalog()),
        )
        .subcommand(
            Command::new("rollback")
                .about("Make an earlier version active again (ALTER FUNCTION f ROLLBACK [TO VERSION n])")
                .arg(Arg::new("name").required(true).help("The function or procedure"))
                .arg(Arg::new("db").long("db").required(true).help("The database"))
                .arg(
                    Arg::new("to-version")
                        .long("to-version")
                        .value_parser(clap::value_parser!(u64))
                        .help("The version (default: the newest earlier one that deployed)"),
                )
                .arg(
                    Arg::new("procedure")
                        .long("procedure")
                        .action(ArgAction::SetTrue)
                        .help("It is a procedure"),
                )
                .arg(
                    Arg::new("gateway")
                        .long("gateway")
                        .required(true)
                        .help("mysql://tenant.user:password@host:port"),
                ),
        )
        .subcommand(
            Command::new("dead-letters")
                .about("Trigger events that kept failing (after-commit, stream and queue triggers)")
                .subcommand_required(true)
                .subcommand(
                    Command::new("list")
                        .about("List dead letters, oldest first")
                        .arg(Arg::new("binding").long("binding").value_parser(clap::value_parser!(u64)))
                        .arg(
                            Arg::new("state")
                                .long("state")
                                .value_parser(["dead", "replaying", "replayed", "discarded"]),
                        )
                        .arg(
                            Arg::new("limit")
                                .long("limit")
                                .value_parser(clap::value_parser!(u32))
                                .default_value("100"),
                        )
                        .arg(catalog()),
                )
                .subcommand(dl_change("replay", "Replay dead letters with the active version, in order"))
                .subcommand(dl_change("discard", "Discard dead letters"))
                .arg_required_else_help(true),
        )
}

fn dl_change(name: &'static str, about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .arg(
            Arg::new("ids")
                .num_args(0..)
                .value_parser(clap::value_parser!(u64))
                .help("Dead letter ids"),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .action(ArgAction::SetTrue)
                .help("Every dead letter (of --binding)"),
        )
        .arg(
            Arg::new("binding")
                .long("binding")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("catalog")
                .long("catalog")
                .required(true)
                .help("The tenant's catalog: mysql://user:pass@host:port/t<tenant>_sprig"),
        )
}

/// Whether the subcommand is implemented.
pub(crate) fn implemented(name: &str) -> bool {
    matches!(
        name,
        "compile" | "deploy" | "list" | "show" | "logs" | "dead-letters" | "rollback"
    )
}

fn read(m: &ArgMatches) -> Result<(String, String), Failed> {
    let path = m.get_one::<String>("file").cloned().unwrap_or_default();
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    Ok((path, text))
}

fn tables(m: &ArgMatches) -> Result<StaticColumns, Failed> {
    let mut cols = StaticColumns::new();
    for spec in m.get_many::<String>("table").into_iter().flatten() {
        let (t, rest) = spec
            .split_once(':')
            .ok_or_else(|| format!("--table {spec}: expected name:col=type,..."))?;
        for c in rest.split(',').filter(|c| !c.is_empty()) {
            let (col, ty) = c
                .split_once('=')
                .ok_or_else(|| format!("--table {spec}: expected col=type"))?;
            let ty = match ty {
                "int" => Ty::Int,
                "float" => Ty::Float,
                "str" => Ty::Str,
                "json" => Ty::Json,
                "bool" => Ty::Bool,
                other => return Err(format!("--table {spec}: unknown type {other}").into()),
            };
            cols.add(t, col, ty);
        }
    }
    Ok(cols)
}

async fn conn(url: &str) -> Result<Conn, Failed> {
    let opts = Opts::from_url(url).map_err(|e| format!("bad URL: {e}"))?;
    Conn::new(opts).await.map_err(|e| Failed(e.to_string()))
}

/// Runs `sprigctl logic <sub>`.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run(sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    match sub.subcommand() {
        Some(("compile", m)) => {
            let (_, text) = read(m)?;
            let c = compile(Runtime::Kirun, &text, &tables(m)?, &Options::default())
                .map_err(|e| e.to_string())?;
            let placed = sprig_logic::placement::place(&c, TierPref::Auto, true)
                .map_err(|e| e.to_string())?;
            if out == Output::Json {
                let v = serde_json::json!({
                    "placement": placed,
                    "ir": c.ir.as_ref().map(ToString::to_string),
                    "plsql": c.plsql.as_ref().map(|p| p.statements.clone()),
                });
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                return Ok(());
            }
            if let Some(f) = &c.ir {
                println!("{f}");
            }
            println!("{}\n", sprig_logic::placement::explain(&placed));
            if let Some(p) = &c.plsql {
                println!("{};", p.statements.join(";\n\n"));
            }
            Ok(())
        }
        Some(("deploy", m)) => {
            let (path, text) = read(m)?;
            let name = m.get_one::<String>("name").cloned().unwrap_or_else(|| {
                std::path::Path::new(&path)
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
            let db = m.get_one::<String>("db").cloned().unwrap_or_default();
            let tier = m
                .get_one::<String>("tier")
                .cloned()
                .unwrap_or_else(|| "auto".into());
            let security = if m.get_flag("definer") {
                "DEFINER"
            } else {
                "INVOKER"
            };
            let body = text.replace('\\', "\\\\").replace('\'', "''");
            let sql = format!(
                "CREATE OR REPLACE PROCEDURE `{}`.`{}` LANGUAGE kirun SQL SECURITY {security} \
                 WITH (SPRIG_TIER = '{tier}') AS '{body}'",
                db.replace('`', "``"),
                name.replace('`', "``")
            );
            let url = m.get_one::<String>("gateway").cloned().unwrap_or_default();
            let mut c = conn(&url).await?;
            c.query_drop(&sql)
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let notes: Vec<(String, u32, String)> = c
                .query("SHOW WARNINGS")
                .await
                .map_err(|e| Failed(e.to_string()))?;
            for (_, _, msg) in notes {
                println!("{msg}");
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("rollback", m)) => {
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            let db = m.get_one::<String>("db").cloned().unwrap_or_default();
            let kind = if m.get_flag("procedure") {
                "PROCEDURE"
            } else {
                "FUNCTION"
            };
            let to = m
                .get_one::<u64>("to-version")
                .map(|v| format!(" TO VERSION {v}"))
                .unwrap_or_default();
            let sql = format!(
                "ALTER {kind} `{}`.`{}` ROLLBACK{to}",
                db.replace('`', "``"),
                name.replace('`', "``")
            );
            let url = m.get_one::<String>("gateway").cloned().unwrap_or_default();
            let mut c = conn(&url).await?;
            c.query_drop(&sql)
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let notes: Vec<(String, u32, String)> = c
                .query("SHOW WARNINGS")
                .await
                .map_err(|e| Failed(e.to_string()))?;
            for (_, _, msg) in notes {
                println!("{msg}");
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("list", m)) => {
            let (mut c, store) = catalog(m).await?;
            let rows = store
                .modules(&mut c, None)
                .await
                .map_err(|e| e.to_string())?;
            if out == Output::Json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&rows).unwrap_or_default()
                );
            } else {
                println!(
                    "{:<8} {:<6} {:<24} {:<10} {:<10} {:<8} ACTIVE",
                    "MODULE", "DB", "NAME", "KIND", "RUNTIME", "TIER"
                );
                for r in rows {
                    println!(
                        "{:<8} {:<6} {:<24} {:<10} {:<10} {:<8} {}",
                        r.module_id,
                        r.db_id,
                        r.name,
                        r.kind,
                        r.runtime,
                        r.tier_pref,
                        r.active_version
                            .map_or_else(|| "-".to_owned(), |v| v.to_string())
                    );
                }
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("show", m)) => {
            let (mut c, store) = catalog(m).await?;
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            let modules = store
                .modules(&mut c, None)
                .await
                .map_err(|e| e.to_string())?;
            let Some(module) = modules.into_iter().find(|r| r.name == name) else {
                return Err(format!("no logic module {name}").into());
            };
            let versions = store
                .versions(&mut c, module.module_id)
                .await
                .map_err(|e| e.to_string())?;
            if out == Output::Json {
                let v = serde_json::json!({"module": module, "versions": versions});
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            } else {
                println!(
                    "{} {} (module {}, {}, active version {})",
                    module.kind,
                    module.name,
                    module.module_id,
                    module.runtime,
                    module
                        .active_version
                        .map_or_else(|| "-".to_owned(), |v| v.to_string())
                );
                for v in versions {
                    println!(
                        "  v{} {} tier {} ({})",
                        v.version, v.state, v.tier, v.compiler_version
                    );
                    for r in v.placement["reasons"].as_array().into_iter().flatten() {
                        println!("    reason {} at {}: {}", r["code"], r["at"], r["detail"]);
                    }
                }
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("logs", m)) => logs(m, out).await,
        Some(("dead-letters", m)) => dead_letters(m, out).await,
        _ => Err("not available in this version".to_owned().into()),
    }
}

async fn logs(m: &ArgMatches, out: Output) -> Result<(), Failed> {
    let (mut c, store) = catalog(m).await?;
    let module = match m.get_one::<String>("name") {
        Some(n) => {
            let all = store
                .modules(&mut c, None)
                .await
                .map_err(|e| e.to_string())?;
            Some(
                all.into_iter()
                    .find(|r| r.name == *n)
                    .ok_or_else(|| format!("no logic module {n}"))?
                    .module_id,
            )
        }
        None => None,
    };
    let limit = m.get_one::<u32>("limit").copied().unwrap_or(50);
    let follow = m.get_flag("follow");
    let mut after = 0u64;
    loop {
        let rows = store
            .logs(&mut c, module, after, limit)
            .await
            .map_err(|e| e.to_string())?;
        for r in &rows {
            after = after.max(r.log_id);
            if out == Output::Json {
                println!("{}", serde_json::to_string(r).unwrap_or_default());
            } else {
                println!(
                    "{} {:<5} {:<8} module {} {}{}",
                    r.at,
                    r.level,
                    r.source,
                    r.module_id,
                    r.version.map_or_else(String::new, |v| format!("v{v} ")),
                    r.message
                );
            }
        }
        if !follow {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let _ = c.disconnect().await;
    Ok(())
}

async fn dead_letters(m: &ArgMatches, out: Output) -> Result<(), Failed> {
    match m.subcommand() {
        Some(("list", m)) => {
            let (mut c, store) = catalog(m).await?;
            let rows = store
                .dead_letters(
                    &mut c,
                    m.get_one::<u64>("binding").copied(),
                    m.get_one::<String>("state").map(String::as_str),
                    m.get_one::<u32>("limit").copied().unwrap_or(100),
                )
                .await
                .map_err(|e| e.to_string())?;
            if out == Output::Json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&rows).unwrap_or_default()
                );
            } else {
                println!(
                    "{:<6} {:<8} {:<4} {:<10} {:<8} {:<20} {:<28} ERROR",
                    "ID", "BINDING", "VER", "STATE", "TRIES", "FAILED", "EVENT"
                );
                for r in rows {
                    println!(
                        "{:<6} {:<8} {:<4} {:<10} {:<8} {:<20} {:<28} {} {}",
                        r.dl_id,
                        r.binding_id,
                        r.version,
                        r.state,
                        r.attempts,
                        r.last_failed_at,
                        format!("{} {}", r.event["op"].as_str().unwrap_or(""), r.event_key),
                        r.error_code,
                        r.error_message
                    );
                    println!("       row {}", r.event["row"]);
                }
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some((verb @ ("replay" | "discard"), m)) => {
            let ids: Vec<u64> = m
                .get_many::<u64>("ids")
                .into_iter()
                .flatten()
                .copied()
                .collect();
            if ids.is_empty() && !m.get_flag("all") {
                return Err("name dead letter ids, or --all".to_owned().into());
            }
            let (mut c, store) = catalog(m).await?;
            let binding = m.get_one::<u64>("binding").copied();
            if verb == "replay" {
                // After-commit triggers' dead letters, and those of
                // triggers on streams and queues (RC11), go back to their
                // jobs, which run them in order with the active version.
                let (n, left) = store
                    .replay_dead_letters(&mut c, binding, &ids)
                    .await
                    .map_err(|e| e.to_string())?;
                println!(
                    "{n} dead letters queued for replay (the trigger's worker runs them in order)"
                );
                if left > 0 {
                    println!(
                        "{left} dead letters of other kinds of triggers stay dead: they can't \
                         be replayed; discard them"
                    );
                }
                let _ = c.disconnect().await;
                return Ok(());
            }
            let n = store
                .set_dead_letters(&mut c, binding, &ids, "dead", "discarded")
                .await
                .map_err(|e| e.to_string())?;
            println!("{n} dead letters discarded");
            let _ = c.disconnect().await;
            Ok(())
        }
        _ => Err("dead-letters takes list, replay or discard"
            .to_owned()
            .into()),
    }
}

async fn catalog(m: &ArgMatches) -> Result<(Conn, LogicStore), Failed> {
    let url = m.get_one::<String>("catalog").cloned().unwrap_or_default();
    let opts = Opts::from_url(&url).map_err(|e| format!("bad URL: {e}"))?;
    let schema = opts
        .db_name()
        .map(str::to_owned)
        .ok_or_else(|| "--catalog needs the catalog schema as its database".to_owned())?;
    let c = Conn::new(opts).await.map_err(|e| Failed(e.to_string()))?;
    Ok((c, LogicStore::new(&schema)))
}
