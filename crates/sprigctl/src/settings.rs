//! `sprigctl settings` (alias `config`): see and change
//! settings at every scope over the Admin API's `Settings` service.
//!
//! ```text
//! sprigctl settings list [--prefix catalog.] [--operator-only | --tenant-settable] [--accepts tenant]
//! sprigctl settings describe <key>
//! sprigctl settings get <key> [--tenant t] [--db d] [--object o] [--cluster c] [--dc d]
//! sprigctl settings set <key> <value> --scope global|dc|cluster|tenant|database|object [targets]
//! sprigctl settings reset <key> --scope ... [--bounds | --all]
//! sprigctl settings bounds <key> --scope ... [--min n] [--max n] [--allowed a,b] [--clear min|max|allowed]
//! sprigctl settings lock|unlock <key> --scope ...
//! ```
//!
//! Targets are `--tenant`, `--db`, `--object`, `--cluster` and the global
//! `--dc`; `--scope` also takes a one-word form
//! (`--scope object:acme.shop.orders`, `--scope dc:mum1`). `get` is also
//! `explain`, `reset` also `unset`. Writes record `op:<actor>`, the actor
//! being `--actor`, `SPRIGCTL_ACTOR` or `USER`, with `--reason`.

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::admin::types::{
    ListSettingKeysResponse, LockSettingRequest, ResetSettingRequest, ScopeRef, SetBoundsRequest,
    SetSettingRequest, SettingExplanation, SettingKeyView, SettingWriteResponse,
    TenantSettingsResponse,
};

use crate::client::AdminClient;
use crate::{Failed, Output, print, render};

const SCOPE_HELP: &str = "global, dc, cluster, tenant, database or object (with --tenant, --db, \
                          --object, --cluster, --dc), or one context string: dc:<id>, \
                          cluster:<id>, tenant:<name>, db:<tenant>.<db>, object:<tenant>.<db>.<object>";

fn key() -> Arg {
    Arg::new("key")
        .required(true)
        .help("The setting, such as auto_index.max_per_table")
}

fn targets(c: Command) -> Command {
    c.arg(Arg::new("tenant").long("tenant").help("A tenant's name"))
        .arg(
            Arg::new("db")
                .long("db")
                .visible_alias("database")
                .help("A database of the tenant"),
        )
        .arg(
            Arg::new("object")
                .long("object")
                .help("An object (table, collection, ...) of the database, or <db>.<object>"),
        )
        .arg(
            Arg::new("cluster")
                .long("cluster")
                .help("A cluster id, such as mum1-c7"),
        )
}

fn write_args(c: Command) -> Command {
    targets(c)
        .arg(
            Arg::new("scope")
                .long("scope")
                .required(true)
                .help(SCOPE_HELP),
        )
        .arg(Arg::new("reason").long("reason").help("Why (audited)"))
        .arg(
            Arg::new("actor")
                .long("actor")
                .env("SPRIGCTL_ACTOR")
                .help("Who, recorded as op:<actor> [default: $USER]"),
        )
}

/// `sprigctl settings`.
#[allow(clippy::too_many_lines)] // one command tree, declared in one place
pub(crate) fn group() -> Command {
    Command::new("settings")
        .visible_alias("config")
        .about("See and change settings at every scope")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List the keys this deployment knows")
                .arg(
                    Arg::new("prefix")
                        .long("prefix")
                        .help("Keys starting with this, such as catalog."),
                )
                .arg(
                    Arg::new("operator-only")
                        .long("operator-only")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("tenant-settable")
                        .help("Only keys tenants may not set"),
                )
                .arg(
                    Arg::new("tenant-settable")
                        .long("tenant-settable")
                        .action(ArgAction::SetTrue)
                        .help("Only keys tenants may set"),
                )
                .arg(
                    Arg::new("accepts")
                        .long("accepts")
                        .value_parser(["global", "dc", "cluster", "tenant", "database", "object"])
                        .help("Only keys that accept this scope"),
                ),
        )
        .subcommand(
            Command::new("describe")
                .about("A key's type, default, scopes, hard bounds and documentation")
                .arg(key()),
        )
        .subcommand(targets(
            Command::new("get")
                .visible_alias("explain")
                .about("The effective value, where it comes from, and the bounds of every scope")
                .arg(key())
                .arg(
                    Arg::new("scope")
                        .long("scope")
                        .help("The context as one string, such as object:acme.shop.orders"),
                ),
        ))
        .subcommand(write_args(
            Command::new("set")
                .about("Set a value at one scope")
                .arg(key())
                .arg(
                    Arg::new("value")
                        .required(true)
                        .allow_hyphen_values(true)
                        .help("The value: a number, true/false, or text"),
                ),
        ))
        .subcommand(write_args(
            Command::new("reset")
                .visible_alias("unset")
                .about("Remove a value (or bounds) at one scope; narrower scopes then inherit")
                .arg(key())
                .arg(
                    Arg::new("bounds")
                        .long("bounds")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("all")
                        .help("Reset the bounds instead of the value"),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .help("Reset the value and the bounds"),
                ),
        ))
        .subcommand(write_args(
            Command::new("bounds")
                .about("Limit what narrower scopes may set; parts not named are kept")
                .arg(key())
                .arg(
                    Arg::new("min")
                        .long("min")
                        .allow_hyphen_values(true)
                        .value_parser(clap::value_parser!(f64))
                        .help("The smallest value narrower scopes may set"),
                )
                .arg(
                    Arg::new("max")
                        .long("max")
                        .allow_hyphen_values(true)
                        .value_parser(clap::value_parser!(f64))
                        .help("The largest"),
                )
                .arg(
                    Arg::new("allowed")
                        .long("allowed")
                        .value_delimiter(',')
                        .action(ArgAction::Append)
                        .help("The only values narrower scopes may set, comma-separated"),
                )
                .arg(
                    Arg::new("clear")
                        .long("clear")
                        .value_delimiter(',')
                        .action(ArgAction::Append)
                        .value_parser(["min", "max", "allowed"])
                        .help("Remove these parts of the bounds"),
                ),
        ))
        .subcommand(write_args(
            Command::new("lock")
                .about("Stop narrower scopes from setting the key")
                .arg(key()),
        ))
        .subcommand(write_args(
            Command::new("unlock")
                .about("Let narrower scopes set the key again")
                .arg(key()),
        ))
}

/// The scope a write names: `--scope` with its targets.
pub(crate) fn scope_ref(m: &ArgMatches, sub: &ArgMatches) -> Result<ScopeRef, Failed> {
    let s = |n: &str| sub.get_one::<String>(n).map(String::as_str);
    let scope = s("scope").unwrap_or("global");
    if scope.contains(':') {
        return scope.parse().map_err(Failed);
    }
    let need = |n: &str| {
        s(n).map(str::to_owned)
            .ok_or_else(|| Failed(format!("--scope {scope} needs --{n}")))
    };
    let joined = match scope {
        "global" => "global".to_owned(),
        "dc" => match m.get_one::<String>("dc") {
            Some(d) => format!("dc:{d}"),
            None => "dc".to_owned(),
        },
        "cluster" => format!("cluster:{}", need("cluster")?),
        "tenant" => format!("tenant:{}", need("tenant")?),
        "database" | "db" => format!("db:{}.{}", need("tenant")?, need("db")?),
        "object" => {
            let object = need("object")?;
            match s("db") {
                Some(db) => format!("object:{}.{db}.{object}", need("tenant")?),
                None if object.contains('.') => format!("object:{}.{object}", need("tenant")?),
                None => {
                    return Err(Failed(
                        "--scope object needs --db (or --object <db>.<object>)".into(),
                    ));
                }
            }
        }
        other => {
            return Err(Failed(format!(
                "--scope {other}: expected global, dc, cluster, tenant, database or object"
            )));
        }
    };
    joined.parse().map_err(Failed)
}

/// `name=value` pairs of a query string, escaped.
fn query(pairs: &[(&str, Option<String>)]) -> String {
    let parts: Vec<String> = pairs
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| format!("{k}={}", escape(v))))
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

/// Percent-escapes everything but unreserved characters.
pub(crate) fn escape(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// The query of `settings get`: the targets, or a one-word `--scope`.
pub(crate) fn get_query(m: &ArgMatches, sub: &ArgMatches) -> Result<String, Failed> {
    let s = |n: &str| sub.get_one::<String>(n).cloned();
    let (mut tenant, mut db, mut object, mut cluster) =
        (s("tenant"), s("db"), s("object"), s("cluster"));
    let mut dc = m.get_one::<String>("dc").cloned();
    if let Some(scope) = s("scope") {
        match scope.parse::<ScopeRef>().map_err(Failed)? {
            ScopeRef::Global => {}
            ScopeRef::Dc(d) => dc = d.map(|d| d.to_string()).or(dc),
            ScopeRef::Cluster(c) => cluster = Some(c.to_string()),
            ScopeRef::Tenant(t) => tenant = Some(t),
            ScopeRef::Database { tenant: tn, db: dn } => (tenant, db) = (Some(tn), Some(dn)),
            ScopeRef::Object {
                tenant: tn,
                db: dn,
                object: on,
            } => (tenant, db, object) = (Some(tn), Some(dn), Some(on)),
        }
    }
    Ok(query(&[
        ("tenant", tenant),
        ("db", db),
        ("object", object),
        ("cluster", cluster),
        ("dc", dc),
    ]))
}

fn actor(sub: &ArgMatches) -> Option<String> {
    sub.get_one::<String>("actor")
        .cloned()
        .or_else(|| std::env::var("USER").ok())
        .filter(|a| !a.trim().is_empty())
}

fn reason(sub: &ArgMatches) -> String {
    sub.get_one::<String>("reason").cloned().unwrap_or_default()
}

/// The path of a key, escaped.
fn path(key: &str, method: &str) -> String {
    format!("/admin/v1/settings/{}{method}", escape(key))
}

/// Runs `sprigctl settings <cmd>`.
pub(crate) async fn run(
    c: &AdminClient,
    m: &ArgMatches,
    group: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("a subcommand is required".into()));
    };
    let key = sub
        .try_get_one::<String>("key")
        .ok()
        .flatten()
        .cloned()
        .unwrap_or_default();
    match cmd {
        "list" => {
            let op = if sub.get_flag("operator-only") {
                Some("true".to_owned())
            } else if sub.get_flag("tenant-settable") {
                Some("false".to_owned())
            } else {
                None
            };
            let q = query(&[
                ("prefix", sub.get_one::<String>("prefix").cloned()),
                ("operator_only", op),
                ("scope", sub.get_one::<String>("accepts").cloned()),
            ]);
            let r: ListSettingKeysResponse = c.get(&format!("/admin/v1/settings{q}")).await?;
            print(out, &r, |r| render::settings_list(&r.keys));
        }
        "describe" => {
            let r: SettingExplanation = c.get(&path(&key, "")).await?;
            print(out, &r.declaration, |d: &SettingKeyView| {
                render::settings_describe(d)
            });
        }
        "get" => {
            let r: SettingExplanation = c
                .get(&format!("{}{}", path(&key, ""), get_query(m, sub)?))
                .await?;
            print(out, &r, render::settings_get);
        }
        _ => {
            let scope = scope_ref(m, sub)?;
            if m.get_flag("dry-run") {
                println!("would {cmd} {key} at {scope}");
                return Ok(());
            }
            let r = write(c, cmd, &key, scope, sub).await?;
            print(out, &r, render::settings_write);
        }
    }
    Ok(())
}

async fn write(
    c: &AdminClient,
    cmd: &str,
    key: &str,
    scope: ScopeRef,
    sub: &ArgMatches,
) -> Result<SettingWriteResponse, Failed> {
    let (actor, reason) = (actor(sub), reason(sub));
    let scope = Some(scope);
    Ok(match cmd {
        "set" => {
            let req = SetSettingRequest {
                scope,
                value: None,
                value_text: sub.get_one::<String>("value").cloned(),
                reason,
                actor,
            };
            c.put(&path(key, ""), &req).await?
        }
        "reset" => {
            let req = ResetSettingRequest {
                scope,
                bounds: sub.get_flag("bounds"),
                all: sub.get_flag("all"),
                reason,
                actor,
            };
            c.post(&path(key, ":reset"), &req).await?
        }
        "bounds" => {
            let allowed: Option<Vec<String>> = sub
                .get_many::<String>("allowed")
                .map(|v| v.cloned().collect());
            let req = SetBoundsRequest {
                scope,
                min: sub.get_one::<f64>("min").copied(),
                max: sub.get_one::<f64>("max").copied(),
                allowed: None,
                allowed_text: allowed,
                clear: sub
                    .get_many::<String>("clear")
                    .map(|v| v.cloned().collect())
                    .unwrap_or_default(),
                reason,
                actor,
            };
            c.post(&path(key, ":bounds"), &req).await?
        }
        "lock" | "unlock" => {
            let req = LockSettingRequest {
                scope,
                reason,
                actor,
            };
            c.post(&path(key, &format!(":{cmd}")), &req).await?
        }
        other => return Err(Failed(format!("unknown command {other}"))),
    })
}

/// `sprigctl tenant settings <name> [--all]`.
pub(crate) async fn tenant(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let name = sub.get_one::<String>("name").cloned().unwrap_or_default();
    let mut r: TenantSettingsResponse = c
        .get(&format!("/admin/v1/tenants/{}/settings", escape(&name)))
        .await?;
    if !sub.get_flag("all") {
        r.rows.retain(|row| row.scope != "ancestor");
    }
    print(out, &r, render::tenant_settings);
    Ok(())
}
