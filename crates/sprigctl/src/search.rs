//! `sprigctl search list|show|rebuild` (chapter 39 build step 14; R5
//! rounds 6 and 8), over the Admin API's `/admin/v1/tenants/{name}/search`:
//! the indexes placed on search workers and where their copies are, and a
//! new generation of an index, built from MariaDB (as `ALTER SEARCH INDEX
//! ... REBUILD`).
//!
//! ```text
//! sprigctl search list acme
//! sprigctl search show acme shop.product_text
//! sprigctl search rebuild acme shop.product_text
//! ```

use clap::{Arg, ArgMatches, Command};
use serde_json::Value;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl search`.
pub(crate) fn group() -> Command {
    let tenant = || Arg::new("tenant").required(true).help("The tenant");
    Command::new("search")
        .about("Search indexes and their copies on workers (chapter 39)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("A tenant's search indexes placed on workers, and their copies")
                .arg(tenant()),
        )
        .subcommand(
            Command::new("show")
                .about("One index's record and copies")
                .arg(tenant())
                .arg(
                    Arg::new("index")
                        .required(true)
                        .help("The index, as database.index (or index)"),
                ),
        )
        .subcommand(
            Command::new("rebuild")
                .about(
                    "Build a new generation of an index from MariaDB; queries wait for its copies",
                )
                .arg(tenant())
                .arg(
                    Arg::new("index")
                        .required(true)
                        .help("The index, as database.index"),
                ),
        )
}

fn s(v: &Value) -> String {
    match v {
        Value::String(x) => x.clone(),
        Value::Null => "-".into(),
        x => x.to_string(),
    }
}

fn list_text(v: &Value) -> String {
    let rows: Vec<[String; 6]> = v
        .as_array()
        .map(|a| {
            a.iter()
                .map(|i| {
                    let copies: Vec<String> = i["copies"]
                        .as_array()
                        .map(|c| {
                            c.iter()
                                .map(|c| {
                                    format!(
                                        "{}:{}@{}",
                                        s(&c["copy"]),
                                        s(&c["state"]),
                                        s(&c["worker"])
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    [
                        format!("{}.{}", s(&i["database"]), s(&i["name"])),
                        s(&i["object"]),
                        s(&i["model"]),
                        s(&i["generation"]),
                        s(&i["copies_wanted"]),
                        copies.join(" "),
                    ]
                })
                .collect()
        })
        .unwrap_or_default();
    table(
        ["INDEX", "OBJECT", "MODEL", "GENERATION", "WANTED", "COPIES"],
        &rows,
    )
}

fn show_text(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

/// Runs `sprigctl search`.
pub(crate) async fn run(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Err(Failed("search: a subcommand is needed".into()));
    };
    let tenant = m.get_one::<String>("tenant").cloned().unwrap_or_default();
    let base = format!("/admin/v1/tenants/{}/search", escape(&tenant));
    match cmd {
        "list" => {
            let v: Value = c.get(&base).await?;
            print(out, &v, list_text);
        }
        "show" => {
            let index = m.get_one::<String>("index").cloned().unwrap_or_default();
            let v: Value = c.get(&format!("{base}/{}", escape(&index))).await?;
            print(out, &v, show_text);
        }
        "rebuild" => {
            let index = m.get_one::<String>("index").cloned().unwrap_or_default();
            let v: Value = c
                .post(
                    &format!("{base}/{}:rebuild", escape(&index)),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &v, |v| {
                format!(
                    "{}.{}: generation {} is being built (catalog version {})\n",
                    s(&v["database"]),
                    s(&v["name"]),
                    s(&v["generation"]),
                    s(&v["catalog_version"])
                )
            });
        }
        other => return Err(Failed(format!("search {other}: unknown subcommand"))),
    }
    Ok(())
}
