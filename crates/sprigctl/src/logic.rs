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
//! `logs`, `rollback` and `dead-letters` are not implemented yet.

use clap::{Arg, ArgAction, ArgMatches, Command};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use sprig_logic::LogicStore;
use sprig_logic::declare::TierPref;
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, Runtime, StaticColumns, compile};

use crate::{Failed, Output};

/// `sprigctl logic`.
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
    let later = |name: &'static str| {
        Command::new(name).arg(
            Arg::new("args")
                .num_args(0..)
                .trailing_var_arg(true)
                .allow_hyphen_values(true)
                .help("Arguments (not parsed yet)"),
        )
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
        .subcommand(later("logs"))
        .subcommand(later("rollback"))
        .subcommand(later("dead-letters"))
}

/// Whether the subcommand is implemented.
pub(crate) fn implemented(name: &str) -> bool {
    matches!(name, "compile" | "deploy" | "list" | "show")
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
        _ => Err("not implemented yet".to_owned().into()),
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
