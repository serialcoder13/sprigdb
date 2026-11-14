//! `sprigctl space`:
//!
//! - `list` and `show [space]` read the tenant's catalog directly,
//!   `--catalog mysql://...@host:port/t<tenant>_sprig` (an operator's
//!   connection, as `sprigctl logic list` does), until the Admin API serves
//!   them. `show` prints the space tree, every share with its filter,
//!   merge mode and copy state (`serving` or `seeding`, and how far behind
//!   the copy worker may be), and per inheritable object the count of each
//!   layer state;
//! - `create`, `share` and `unshare` send `CREATE SPACE`, `CREATE SHARE`
//!   and `DROP SHARE` through a gateway as the user in `--gateway
//!   mysql://tenant.user:password@host:port`, so the user's privileges
//!   and every check of the SQL path apply.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Row};
use sprig_inherit::model::ShareTarget;
use sprig_inherit::{InheritStore, ROOT_SPACE, SpaceId};

use crate::{Failed, Output};

/// `sprigctl space`.
pub(crate) fn group() -> Command {
    let catalog = || {
        Arg::new("catalog")
            .long("catalog")
            .required(true)
            .help("The tenant's catalog: mysql://user:pass@host:port/t<tenant>_sprig")
    };
    let gateway = || {
        Arg::new("gateway")
            .long("gateway")
            .required(true)
            .help("mysql://tenant.user:password@host:port")
    };
    Command::new("space")
        .about("Spaces and shares")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List the tenant's spaces")
                .arg(catalog()),
        )
        .subcommand(
            Command::new("show")
                .about("Show the space tree, shares, copies and layer counts")
                .arg(Arg::new("space").help("Only this space and the shares into and out of it"))
                .arg(catalog()),
        )
        .subcommand(
            Command::new("create")
                .about("Create a space (CREATE SPACE ... UNDER ...)")
                .arg(Arg::new("name").required(true))
                .arg(
                    Arg::new("under")
                        .long("under")
                        .help("The parent space (default: the root)"),
                )
                .arg(gateway()),
        )
        .subcommand(
            Command::new("share")
                .about("Share an inheritable table (CREATE SHARE)")
                .arg(Arg::new("name").required(true).help("The share's name"))
                .arg(
                    Arg::new("table")
                        .long("table")
                        .required(true)
                        .help("db.table"),
                )
                .arg(
                    Arg::new("to")
                        .long("to")
                        .required(true)
                        .help("A child space or child tenant, or 'all' for ALL CHILDREN"),
                )
                .arg(
                    Arg::new("from")
                        .long("from")
                        .help("The sharing space (default: the user's home)"),
                )
                .arg(Arg::new("where").long("where").help("The row filter"))
                .arg(
                    Arg::new("columns")
                        .long("columns")
                        .help("Shared columns: a,b,c"),
                )
                .arg(
                    Arg::new("reshare")
                        .long("reshare")
                        .action(ArgAction::SetTrue)
                        .help("WITH RESHARE"),
                )
                .arg(gateway()),
        )
        .subcommand(
            Command::new("unshare")
                .about("Drop a share (DROP SHARE)")
                .arg(Arg::new("name").required(true))
                .arg(
                    Arg::new("from")
                        .long("from")
                        .help("The sharing space (default: the user's home)"),
                )
                .arg(gateway()),
        )
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

async fn conn(url: &str) -> Result<Conn, Failed> {
    let opts = Opts::from_url(url).map_err(|e| format!("bad URL: {e}"))?;
    Conn::new(opts).await.map_err(|e| Failed(e.to_string()))
}

async fn send(m: &ArgMatches, from: Option<&String>, sql: &str) -> Result<(), Failed> {
    let url = m.get_one::<String>("gateway").cloned().unwrap_or_default();
    let mut c = conn(&url).await?;
    if let Some(f) = from {
        c.query_drop(format!("USE SPACE {}", ident(f)))
            .await
            .map_err(|e| Failed(e.to_string()))?;
    }
    c.query_drop(sql).await.map_err(|e| Failed(e.to_string()))?;
    println!("{sql}: OK");
    let _ = c.disconnect().await;
    Ok(())
}

/// The `CREATE SHARE` statement of `sprigctl space share`.
pub(crate) fn share_sql(m: &ArgMatches) -> Result<String, Failed> {
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    let table = m.get_one::<String>("table").cloned().unwrap_or_default();
    let (db, t) = table
        .split_once('.')
        .ok_or_else(|| "--table takes db.table".to_owned())?;
    let to = m.get_one::<String>("to").cloned().unwrap_or_default();
    let to = if to.eq_ignore_ascii_case("all") {
        "ALL CHILDREN".to_owned()
    } else {
        format!("SPACE {}", ident(&to))
    };
    let mut sql = format!(
        "CREATE SHARE {} ON {}.{} TO {to}",
        ident(&name),
        ident(db),
        ident(t)
    );
    if let Some(w) = m.get_one::<String>("where") {
        let _ = write!(sql, " WHERE {w}");
    }
    if let Some(cs) = m.get_one::<String>("columns") {
        let cols: Vec<String> = cs.split(',').map(|c| ident(c.trim())).collect();
        let _ = write!(sql, " COLUMNS ({})", cols.join(", "));
    }
    if m.get_flag("reshare") {
        sql.push_str(" WITH RESHARE");
    }
    Ok(sql)
}

async fn catalog(m: &ArgMatches) -> Result<(Conn, String), Failed> {
    let url = m.get_one::<String>("catalog").cloned().unwrap_or_default();
    let opts = Opts::from_url(&url).map_err(|e| format!("bad URL: {e}"))?;
    let schema = opts
        .db_name()
        .map(str::to_owned)
        .ok_or_else(|| "--catalog needs the catalog schema as its database".to_owned())?;
    let c = Conn::new(opts).await.map_err(|e| Failed(e.to_string()))?;
    Ok((c, schema))
}

/// The tenant id of catalog schema `t<id>_sprig`.
fn tenant_of(schema: &str) -> Option<u64> {
    schema
        .strip_prefix('t')?
        .strip_suffix("_sprig")?
        .parse()
        .ok()
}

fn ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn tree(
    st: &sprig_inherit::InheritState,
    at: SpaceId,
    depth: usize,
    name: &dyn Fn(SpaceId) -> String,
) {
    println!("  {}{} ({})", "  ".repeat(depth), name(at), at.0);
    for c in st.children(at) {
        tree(st, c, depth + 1, name);
    }
}

/// Runs `sprigctl space <sub>`.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run(sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    match sub.subcommand() {
        Some(("create", m)) => {
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            let under = m
                .get_one::<String>("under")
                .map(|u| format!(" UNDER {}", ident(u)))
                .unwrap_or_default();
            send(m, None, &format!("CREATE SPACE {}{under}", ident(&name))).await
        }
        Some(("share", m)) => send(m, m.get_one::<String>("from"), &share_sql(m)?).await,
        Some(("unshare", m)) => {
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            send(
                m,
                m.get_one::<String>("from"),
                &format!("DROP SHARE {}", ident(&name)),
            )
            .await
        }
        Some((cmd @ ("list" | "show"), m)) => {
            let (mut c, schema) = catalog(m).await?;
            let st = InheritStore::new(&schema)
                .load(&mut c)
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let name = |id: SpaceId| {
                st.space(id, "(root)")
                    .map_or_else(|| id.to_string(), |s| s.name)
            };
            if cmd == "list" {
                if out == Output::Json {
                    let v: Vec<_> = st
                        .spaces()
                        .map(|s| {
                            serde_json::json!({
                                "space_id": s.id.0, "name": s.name,
                                "parent": s.parent.map(|p| p.0), "depth": s.depth,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                    return Ok(());
                }
                println!("{:<8} {:<24} {:<24} DEPTH", "ID", "NAME", "PARENT");
                for s in st.spaces() {
                    println!(
                        "{:<8} {:<24} {:<24} {}",
                        s.id.0,
                        s.name,
                        s.parent.map_or_else(|| "-".into(), name),
                        s.depth
                    );
                }
                return Ok(());
            }
            // show: copies' states, from the tenant's shard schemas.
            let tenant = tenant_of(&schema).unwrap_or(0);
            let state_schemas: Vec<String> = c
                .exec(
                    "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = \
                     '__sprig_ih_state' AND TABLE_SCHEMA LIKE ?",
                    (format!("t{tenant}\\_d%"),),
                )
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let mut copies: BTreeMap<(u64, u64), (String, u64)> = BTreeMap::new();
            for s in &state_schemas {
                let rows: Vec<(u64, u64, String, u64)> = c
                    .query(format!(
                        "SELECT object_id, share_key, CAST(state AS CHAR), caught_up_ms FROM \
                         {}.`__sprig_ih_state`",
                        ident(s)
                    ))
                    .await
                    .map_err(|e| Failed(e.to_string()))?;
                for (o, k, state, at) in rows {
                    copies.insert((o, k), (state, ms_now().saturating_sub(at)));
                }
            }
            // Layer counts per object.
            let mut layers: BTreeMap<u64, BTreeMap<String, u64>> = BTreeMap::new();
            let objs: Vec<(u64, String, String)> = c
                .query(format!(
                    "SELECT i.object_id, d.name, o.name FROM {s}.inheritable_objects i JOIN \
                     {s}.objects o ON o.object_id = i.object_id JOIN {s}.`databases` d ON \
                     d.db_id = o.db_id WHERE o.state <> 'dropped'",
                    s = ident(&schema)
                ))
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let mut onames = BTreeMap::new();
            for (oid, db, table) in &objs {
                onames.insert(*oid, format!("{db}.{table}"));
                let db_id: u64 = c
                    .exec_first(
                        format!(
                            "SELECT db_id FROM {}.`databases` WHERE name = ? AND state <> \
                             'dropped'",
                            ident(&schema)
                        ),
                        (db.as_str(),),
                    )
                    .await
                    .map_err(|e| Failed(e.to_string()))?
                    .unwrap_or(0);
                let phys: Option<String> = c
                    .exec_first(
                        "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ? \
                         AND TABLE_SCHEMA LIKE ? LIMIT 1",
                        (table.as_str(), format!("t{tenant}\\_d{db_id}\\_%")),
                    )
                    .await
                    .map_err(|e| Failed(e.to_string()))?;
                if let Some(p) = phys {
                    let rows: Vec<Row> = c
                        .query(format!(
                            "SELECT CAST(`_sprig_state` AS CHAR), COUNT(*) FROM {}.{} GROUP BY 1",
                            ident(&p),
                            ident(table)
                        ))
                        .await
                        .map_err(|e| Failed(e.to_string()))?;
                    for r in rows {
                        let (k, n): (String, u64) = mysql_async::from_row(r);
                        layers.entry(*oid).or_default().insert(k, n);
                    }
                }
            }
            let only = m.get_one::<String>("space").cloned();
            let shown = |id: SpaceId| {
                only.as_ref()
                    .is_none_or(|o| name(id).eq_ignore_ascii_case(o))
            };
            if out == Output::Json {
                let shares: Vec<_> = st
                    .shares()
                    .map(|s| {
                        let copy = copies.get(&(s.object_id.0, s.id));
                        serde_json::json!({
                            "share": s.name, "object": onames.get(&s.object_id.0),
                            "from": name(s.from),
                            "to": match s.to {
                                ShareTarget::Space(t) => name(t),
                                ShareTarget::Tenant(t) => format!("tenant {t}"),
                                ShareTarget::AllChildren => "ALL CHILDREN".into(),
                            },
                            "filter": s.filter, "mode": s.mode.as_str(),
                            "copy": copy.map(|c| c.0.clone()), "lag_ms": copy.map(|c| c.1),
                        })
                    })
                    .collect();
                let v = serde_json::json!({
                    "spaces": st.spaces().map(|s| serde_json::json!({
                        "space_id": s.id.0, "name": s.name, "parent": s.parent.map(|p| p.0),
                    })).collect::<Vec<_>>(),
                    "shares": shares,
                    "copies_from_parent_tenant": copies.iter().filter(|((_, k), _)| *k == 0)
                        .map(|((o, _), (st, lag))| serde_json::json!({
                            "object": onames.get(o), "copy": st, "lag_ms": lag,
                        })).collect::<Vec<_>>(),
                    "layers": layers.iter().map(|(o, m)| (onames.get(o).cloned().unwrap_or_default(), m))
                        .collect::<BTreeMap<_, _>>(),
                });
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                return Ok(());
            }
            println!("Spaces:");
            tree(&st, ROOT_SPACE, 0, &name);
            println!("\nShares:");
            println!(
                "  {:<16} {:<28} {:<14} {:<16} {:<13} {:<9} {:<8} FILTER",
                "NAME", "OBJECT", "FROM", "TO", "MODE", "COPY", "LAG_MS"
            );
            for s in st
                .shares()
                .filter(|s| shown(s.from) || matches!(s.to, ShareTarget::Space(t) if shown(t)))
            {
                let copy = copies.get(&(s.object_id.0, s.id));
                println!(
                    "  {:<16} {:<28} {:<14} {:<16} {:<13} {:<9} {:<8} {}",
                    s.name,
                    onames.get(&s.object_id.0).cloned().unwrap_or_default(),
                    name(s.from),
                    match s.to {
                        ShareTarget::Space(t) => name(t),
                        ShareTarget::Tenant(t) => format!("tenant {t}"),
                        ShareTarget::AllChildren => "ALL CHILDREN".into(),
                    },
                    s.mode.as_str(),
                    copy.map_or("-", |c| c.0.as_str()),
                    copy.map_or_else(|| "-".into(), |c| c.1.to_string()),
                    s.filter.as_deref().unwrap_or("")
                );
            }
            for ((o, k), (state, lag)) in &copies {
                if *k == 0 {
                    println!(
                        "  {:<16} {:<28} {:<14} {:<16} {:<13} {:<9} {:<8}",
                        "(parent tenant)",
                        onames.get(o).cloned().unwrap_or_default(),
                        "parent",
                        "root",
                        "materialized",
                        state,
                        lag
                    );
                }
            }
            println!("\nLayer rows:");
            for (o, counts) in &layers {
                let parts: Vec<String> = ["own", "ovr", "hide", "orphan"]
                    .iter()
                    .map(|k| format!("{k} {}", counts.get(*k).copied().unwrap_or(0)))
                    .collect();
                println!(
                    "  {:<28} {}",
                    onames.get(o).cloned().unwrap_or_default(),
                    parts.join(", ")
                );
            }
            Ok(())
        }
        _ => Err("space takes list, show, create, share or unshare"
            .to_owned()
            .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_statement() {
        let m = group().get_matches_from([
            "space",
            "share",
            "in_products",
            "--table",
            "shop.products",
            "--to",
            "acme",
            "--where",
            "region = 'IN'",
            "--columns",
            "name, price",
            "--reshare",
            "--gateway",
            "mysql://x",
        ]);
        let (_, s) = m.subcommand().unwrap();
        assert_eq!(
            share_sql(s).unwrap(),
            "CREATE SHARE `in_products` ON `shop`.`products` TO SPACE `acme` WHERE region = \
             'IN' COLUMNS (`name`, `price`) WITH RESHARE"
        );
        assert_eq!(tenant_of("t42_sprig"), Some(42));
        assert_eq!(tenant_of("t42_d1"), None);
    }
}
