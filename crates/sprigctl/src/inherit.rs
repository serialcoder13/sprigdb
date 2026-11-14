//! `sprigctl inherit`: what a tenant inherits and what holds
//! its deletes back, through a gateway as the user in `--gateway
//! mysql://tenant.user:password@host:port`, so the user's privileges
//! decide what is shown:
//!
//! - `check TABLE`: ids of `TABLE` that more than one tenant of its tree
//!   holds as its own (`SPRIG CHECK IDS`);
//! - `tables`: each inheritable table's origin tenant, how many levels up
//!   it is, the columns this tenant added, its foreign-key group and its
//!   shares (`SHOW INHERITABLE TABLES`);
//! - `share NAME`: the share as created and the group it carries (`SHOW
//!   CREATE SHARE`);
//! - `blockers TABLE [--where COND]`: what would refuse a delete of those
//!   rows (`SPRIG CHECK REFERENCES`); tenant administrators see the
//!   descendant tenants' names and rows.

use clap::{Arg, ArgMatches, Command};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Row, Value};

use crate::{Failed, Output};

/// `sprigctl inherit`.
pub(crate) fn group() -> Command {
    let gateway = || {
        Arg::new("gateway")
            .long("gateway")
            .required(true)
            .help("mysql://tenant.user:password@host:port")
    };
    let database = || {
        Arg::new("database")
            .long("database")
            .short('d')
            .help("The database to use first")
    };
    let space = || {
        Arg::new("space")
            .long("space")
            .help("The space to look from (the user's home space by default)")
    };
    Command::new("inherit")
        .about("Inherited tables, shares, tree-wide ids and the references that refuse deletes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("check")
                .about("Ids held by more than one tenant of the table's tree (SPRIG CHECK IDS)")
                .arg(Arg::new("table").required(true))
                .args([gateway(), database(), space()]),
        )
        .subcommand(
            Command::new("tables")
                .about("Each inheritable table's origin, own columns, group and shares")
                .args([gateway(), database(), space()]),
        )
        .subcommand(
            Command::new("share")
                .about("A share as created and the foreign-key group it carries")
                .arg(Arg::new("name").required(true))
                .args([gateway(), database(), space()]),
        )
        .subcommand(
            Command::new("blockers")
                .about("What refuses a delete of the table's rows (SPRIG CHECK REFERENCES)")
                .arg(Arg::new("table").required(true))
                .arg(
                    Arg::new("where")
                        .long("where")
                        .help("Which rows, as an SQL condition (all rows of the view by default)"),
                )
                .args([gateway(), database(), space()]),
        )
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// A table name as `a` or `db.a`, each part quoted.
fn table_name(s: &str) -> String {
    s.split('.').map(ident).collect::<Vec<_>>().join(".")
}

/// The statement a subcommand sends.
pub(crate) fn statement(name: &str, m: &ArgMatches) -> Result<String, Failed> {
    let arg = |k: &str| m.get_one::<String>(k).cloned().unwrap_or_default();
    Ok(match name {
        "check" => format!("SPRIG CHECK IDS {}", table_name(&arg("table"))),
        "tables" => "SHOW INHERITABLE TABLES".to_owned(),
        "share" => format!("SHOW CREATE SHARE {}", ident(&arg("name"))),
        "blockers" => {
            let mut s = format!("SPRIG CHECK REFERENCES {}", table_name(&arg("table")));
            if let Some(w) = m.get_one::<String>("where") {
                s.push_str(" WHERE ");
                s.push_str(w);
            }
            s
        }
        other => return Err(Failed(format!("unknown subcommand {other}"))),
    })
}

fn text(v: &Value) -> String {
    match v {
        Value::NULL => String::new(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => other.as_sql(true).trim_matches('\'').to_owned(),
    }
}

/// Runs `sprigctl inherit ...`.
///
/// # Errors
/// The gateway's error, or a bad URL.
pub(crate) async fn run(sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((name, m)) = sub.subcommand() else {
        return Err(Failed("a subcommand is required".into()));
    };
    let sql = statement(name, m)?;
    let url = m.get_one::<String>("gateway").cloned().unwrap_or_default();
    let opts = Opts::from_url(&url).map_err(|e| Failed(format!("bad URL: {e}")))?;
    let mut c = Conn::new(opts).await.map_err(|e| Failed(e.to_string()))?;
    let result = async {
        if let Some(s) = m.get_one::<String>("space") {
            c.query_drop(format!("USE SPACE {}", ident(s))).await?;
        }
        if let Some(d) = m.get_one::<String>("database") {
            c.query_drop(format!("USE {}", ident(d))).await?;
        }
        c.query::<Row, _>(sql.as_str()).await
    }
    .await;
    let _ = c.disconnect().await;
    let rows = result.map_err(|e| Failed(e.to_string()))?;
    let header: Vec<String> = rows
        .first()
        .map(|r| {
            r.columns_ref()
                .iter()
                .map(|c| c.name_str().into_owned())
                .collect()
        })
        .unwrap_or_default();
    let body: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            (0..r.len())
                .map(|i| r.as_ref(i).map_or_else(String::new, text))
                .collect()
        })
        .collect();
    match out {
        Output::Json => {
            let items: Vec<serde_json::Value> = body
                .iter()
                .map(|r| {
                    serde_json::Value::Object(
                        header
                            .iter()
                            .cloned()
                            .zip(r.iter().cloned().map(serde_json::Value::String))
                            .collect(),
                    )
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&items).map_err(|e| Failed(e.to_string()))?
            );
        }
        Output::Table => {
            if body.is_empty() {
                println!("{sql}: nothing to show");
            } else {
                print!("{}", render(&header, &body));
            }
        }
    }
    Ok(())
}

/// A plain aligned table.
fn render(header: &[String], body: &[Vec<String>]) -> String {
    let mut w: Vec<usize> = header.iter().map(String::len).collect();
    for r in body {
        for (i, v) in r.iter().enumerate() {
            if let Some(x) = w.get_mut(i) {
                *x = (*x).max(v.chars().count());
            }
        }
    }
    let line = |cells: &[String]| {
        let mut s = cells
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{v:<width$}", width = w.get(i).copied().unwrap_or(0)))
            .collect::<Vec<_>>()
            .join("  ");
        s.truncate(s.trim_end().len());
        s.push('\n');
        s
    };
    let mut out = line(header);
    for r in body {
        out.push_str(&line(r));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> ArgMatches {
        let mut a = vec!["inherit"];
        a.extend_from_slice(args);
        group().try_get_matches_from(a).unwrap()
    }

    #[test]
    fn statements() {
        let gw = "--gateway=mysql://a.app:x@127.0.0.1:1/";
        let s = |args: &[&str]| {
            let m = parse(args);
            let (n, sub) = m.subcommand().unwrap();
            statement(n, sub).unwrap()
        };
        assert_eq!(s(&["check", "shop.t9", gw]), "SPRIG CHECK IDS `shop`.`t9`");
        assert_eq!(s(&["tables", gw]), "SHOW INHERITABLE TABLES");
        assert_eq!(s(&["share", "s1", gw]), "SHOW CREATE SHARE `s1`");
        assert_eq!(
            s(&["blockers", "t2", "--where", "id = 5", gw]),
            "SPRIG CHECK REFERENCES `t2` WHERE id = 5"
        );
        assert_eq!(
            render(
                &["A".into(), "Bee".into()],
                &[vec!["xyz".into(), "1".into()]]
            ),
            "A    Bee\nxyz  1\n"
        );
    }
}
