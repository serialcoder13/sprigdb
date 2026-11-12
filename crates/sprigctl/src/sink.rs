//! `sprigctl sink` (chapter 51, "`sprigctl` and the admin API"; R9):
//!
//! - `list [--db d]`, `show <db.sink>`, `pause | resume <db.sink>`,
//!   `reset <db.sink> --to latest|earliest|snapshot|<time>`, and
//!   `connections list [--db d] | show <db.c>`: the SQL statements, sent
//!   through a gateway as the user in `--gateway mysql://user:pw@host:port/db`
//!   (the Admin API's `Sinks` service isn't built yet);
//! - `dead-letters list | replay | discard <sink> [--id n]... [--all]`: the
//!   tenant catalog's `sink_dead_letters`, over `--catalog
//!   mysql://...@host:port/t<tenant>_sprig` (an operator's connection), as
//!   `sprigctl logic dead-letters` does; a replay goes back to the sink's
//!   task, which delivers it again with its original idempotency key;
//! - `platform-connections register | grant | revoke | list | show`: the
//!   global tier in etcd (`--etcd http://host:port`), operator only. Secret
//!   values never pass through here: only `file:<path>` references.
//!
//! - `test <db.sink> [--sample n]` and `connections test <db.c>
//!   [--platform] [--write [--to target]]` (R9, r9last): `TEST SINK` and
//!   `TEST [PLATFORM] CONNECTION ... [WRITE [TO 'target']]` through the
//!   gateway, as Studio runs them; the rows printed as for `list`.

use std::collections::BTreeMap;
use std::sync::Arc;

use clap::{Arg, ArgAction, ArgMatches, Command};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Row};
use serde_json::{Map, Value};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_sinks::meta::{PlatformConnection, PlatformGrant, Visibility};
use sprig_sinks::model::ConnKind;

use crate::{Failed, Output};

/// `sprigctl sink`.
#[allow(clippy::too_many_lines)] // one builder chain
pub(crate) fn group() -> Command {
    let sink = || Arg::new("sink").required(true).help("db.sink");
    let gw = || {
        Arg::new("gateway")
            .long("gateway")
            .env("SPRIGCTL_GATEWAY")
            .required(true)
            .help("mysql://user:password@host:port/db of a gateway")
    };
    let cat = || {
        Arg::new("catalog")
            .long("catalog")
            .required(true)
            .help("mysql://...@host:port/t<tenant>_sprig")
    };
    let etcd = || {
        Arg::new("etcd")
            .long("etcd")
            .env("SPRIGCTL_ETCD")
            .default_value("http://127.0.0.1:2379")
            .help("etcd endpoint of the global tier")
    };
    let db = || Arg::new("db").long("db").help("Logical database");
    let ids = || {
        Arg::new("id")
            .long("id")
            .action(ArgAction::Append)
            .value_parser(clap::value_parser!(u64))
    };
    let all = || Arg::new("all").long("all").action(ArgAction::SetTrue);
    Command::new("sink")
        .about("Sinks: list, show, pause, resume, reset, dead letters, connections")
        .subcommand_required(true)
        .subcommand(Command::new("list").arg(gw()).arg(db()))
        .subcommand(Command::new("show").arg(sink()).arg(gw()))
        .subcommand(Command::new("pause").arg(sink()).arg(gw()))
        .subcommand(Command::new("resume").arg(sink()).arg(gw()))
        .subcommand(
            Command::new("reset")
                .arg(sink())
                .arg(gw())
                .arg(Arg::new("to").long("to").required(true)),
        )
        .subcommand(
            Command::new("test")
                .about("TEST SINK: the latest source records as they would be delivered")
                .arg(sink())
                .arg(gw())
                .arg(
                    Arg::new("sample")
                        .long("sample")
                        .value_parser(clap::value_parser!(u32).range(1..=1000)),
                ),
        )
        .subcommand(
            Command::new("dead-letters")
                .subcommand_required(true)
                .subcommand(Command::new("list").arg(sink()).arg(cat()))
                .subcommand(
                    Command::new("replay")
                        .arg(sink())
                        .arg(cat())
                        .arg(ids())
                        .arg(all()),
                )
                .subcommand(
                    Command::new("discard")
                        .arg(sink())
                        .arg(cat())
                        .arg(ids())
                        .arg(all()),
                ),
        )
        .subcommand(
            Command::new("connections")
                .subcommand_required(true)
                .subcommand(Command::new("list").arg(gw()).arg(db()))
                .subcommand(
                    Command::new("show")
                        .arg(Arg::new("name").required(true))
                        .arg(gw()),
                )
                .subcommand(
                    Command::new("test")
                        .about("TEST CONNECTION: open and check it; --write sends a test record")
                        .arg(Arg::new("name").required(true))
                        .arg(gw())
                        .arg(
                            Arg::new("platform")
                                .long("platform")
                                .action(ArgAction::SetTrue),
                        )
                        .arg(Arg::new("write").long("write").action(ArgAction::SetTrue))
                        .arg(Arg::new("to").long("to").requires("write")),
                ),
        )
        .subcommand(
            Command::new("platform-connections")
                .subcommand_required(true)
                .subcommand(
                    Command::new("register")
                        .arg(Arg::new("name").required(true))
                        .arg(Arg::new("type").long("type").required(true))
                        .arg(Arg::new("option").long("option").action(ArgAction::Append))
                        .arg(Arg::new("tags").long("tags").action(ArgAction::Append))
                        .arg(Arg::new("description").long("description"))
                        .arg(
                            Arg::new("hide-option")
                                .long("hide-option")
                                .action(ArgAction::Append),
                        )
                        .arg(Arg::new("secret").long("secret").action(ArgAction::Append))
                        .arg(
                            Arg::new("secret-previous")
                                .long("secret-previous")
                                .action(ArgAction::Append),
                        )
                        .arg(Arg::new("host").long("host").action(ArgAction::Append))
                        .arg(Arg::new("visibility").long("visibility"))
                        .arg(etcd()),
                )
                .subcommand(
                    Command::new("grant")
                        .arg(Arg::new("name").required(true))
                        .arg(
                            Arg::new("tenant")
                                .long("tenant")
                                .required(true)
                                .value_parser(clap::value_parser!(u64)),
                        )
                        .arg(
                            Arg::new("subtree")
                                .long("subtree")
                                .action(ArgAction::SetTrue),
                        )
                        .arg(etcd()),
                )
                .subcommand(
                    Command::new("revoke")
                        .arg(Arg::new("name").required(true))
                        .arg(
                            Arg::new("tenant")
                                .long("tenant")
                                .required(true)
                                .value_parser(clap::value_parser!(u64)),
                        )
                        .arg(etcd()),
                )
                .subcommand(Command::new("list").arg(etcd()))
                .subcommand(
                    Command::new("show")
                        .arg(Arg::new("name").required(true))
                        .arg(etcd()),
                ),
        )
}

async fn connect(url: &str) -> Result<Conn, Failed> {
    let opts = Opts::from_url(url).map_err(|e| format!("bad URL: {e}"))?;
    Conn::new(opts)
        .await
        .map_err(|e| Failed(format!("can't connect: {e}")))
}

fn cell(r: &Row, i: usize) -> String {
    r.get_opt::<Option<String>, usize>(i)
        .and_then(Result::ok)
        .flatten()
        .unwrap_or_default()
}

async fn rows(gw: &str, sql: &str, out: Output) -> Result<(), Failed> {
    let mut c = connect(gw).await?;
    let res: Vec<Row> = c.query(sql).await.map_err(|e| Failed(e.to_string()))?;
    let cols: Vec<String> = res
        .first()
        .map(|r| {
            r.columns_ref()
                .iter()
                .map(|c| c.name_str().into_owned())
                .collect()
        })
        .unwrap_or_default();
    if out == Output::Json {
        let v: Vec<Value> = res
            .iter()
            .map(|r| {
                Value::Object(
                    cols.iter()
                        .enumerate()
                        .map(|(i, c)| (c.clone(), Value::from(cell(r, i))))
                        .collect(),
                )
            })
            .collect();
        println!("{}", Value::Array(v));
    } else {
        for r in &res {
            let line: Vec<String> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{c}={}", cell(r, i)))
                .collect();
            println!("{}", line.join("  "));
        }
    }
    let _ = c.disconnect().await;
    Ok(())
}

async fn exec(gw: &str, sql: &str) -> Result<(), Failed> {
    let mut c = connect(gw).await?;
    c.query_drop(sql).await.map_err(|e| Failed(e.to_string()))?;
    let _ = c.disconnect().await;
    println!("ok");
    Ok(())
}

/// `sprigctl sink test`'s statement (R9, r9last).
fn test_sink_sql(sink: &str, sample: Option<u32>) -> String {
    match sample {
        Some(n) => format!("TEST SINK {} WITH SAMPLE {n}", ident(sink)),
        None => format!("TEST SINK {}", ident(sink)),
    }
}

/// `sprigctl sink connections test`'s statement (R9, r9last).
fn test_connection_sql(name: &str, platform: bool, write: bool, to: Option<&str>) -> String {
    let mut sql = format!(
        "TEST {}CONNECTION {}",
        if platform { "PLATFORM " } else { "" },
        ident(name)
    );
    if write {
        sql.push_str(" WRITE");
        if let Some(t) = to {
            sql.push_str(" TO '");
            sql.push_str(&t.replace('\\', "\\\\").replace('\'', "''"));
            sql.push('\'');
        }
    }
    sql
}

fn ident(s: &str) -> String {
    s.split('.')
        .map(|p| format!("`{}`", p.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

async fn global(m: &ArgMatches) -> Result<Arc<dyn MetaStore>, Failed> {
    let ep = m
        .get_one::<String>("etcd")
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:2379".into());
    let s = EtcdStore::connect(Tier::Global, &EtcdConfig::new(vec![ep]))
        .await
        .map_err(|e| Failed(e.to_string()))?;
    Ok(Arc::new(s))
}

fn pairs(m: &ArgMatches, name: &str) -> Result<BTreeMap<String, String>, Failed> {
    let mut out = BTreeMap::new();
    for v in m.get_many::<String>(name).into_iter().flatten() {
        let (k, val) = v
            .split_once('=')
            .ok_or_else(|| Failed(format!("--{name} {v}: expected key=value")))?;
        out.insert(k.trim().to_owned(), val.trim().to_owned());
    }
    Ok(out)
}

async fn sink_id(c: &mut Conn, sink: &str) -> Result<u64, Failed> {
    let name = sink.rsplit('.').next().unwrap_or(sink);
    let ids: Vec<u64> = c
        .exec("SELECT sink_id FROM sinks WHERE name = ?", (name,))
        .await
        .map_err(|e| Failed(e.to_string()))?;
    match ids.as_slice() {
        [id] => Ok(*id),
        [] => Err(Failed(format!("no sink {sink}"))),
        _ => Err(Failed(format!("several sinks are named {name}"))),
    }
}

/// Runs `sprigctl sink ...`.
#[allow(clippy::too_many_lines)] // one match over the commands
pub(crate) async fn run(sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let gw = |m: &ArgMatches| m.get_one::<String>("gateway").cloned().unwrap_or_default();
    let sink = |m: &ArgMatches| m.get_one::<String>("sink").cloned().unwrap_or_default();
    match sub.subcommand() {
        Some(("list", m)) => {
            let sql = match m.get_one::<String>("db") {
                Some(d) => format!("SHOW SINKS FROM {}", ident(d)),
                None => "SHOW SINKS".into(),
            };
            rows(&gw(m), &sql, out).await
        }
        Some(("show", m)) => {
            rows(
                &gw(m),
                &format!("SHOW CREATE SINK {}", ident(&sink(m))),
                out,
            )
            .await?;
            rows(
                &gw(m),
                &format!("SHOW SINK STATUS {}", ident(&sink(m))),
                out,
            )
            .await
        }
        Some(("test", m)) => {
            let sql = test_sink_sql(&sink(m), m.get_one::<u32>("sample").copied());
            rows(&gw(m), &sql, out).await
        }
        Some(("pause", m)) => exec(&gw(m), &format!("ALTER SINK {} PAUSE", ident(&sink(m)))).await,
        Some(("resume", m)) => {
            exec(&gw(m), &format!("ALTER SINK {} RESUME", ident(&sink(m)))).await
        }
        Some(("reset", m)) => {
            let to = m.get_one::<String>("to").cloned().unwrap_or_default();
            let at = match to.to_ascii_lowercase().as_str() {
                "latest" => "LATEST".to_owned(),
                "earliest" => "EARLIEST".to_owned(),
                "snapshot" => "SNAPSHOT".to_owned(),
                _ => format!("TIMESTAMP '{}'", to.replace('\'', "")),
            };
            exec(
                &gw(m),
                &format!("ALTER SINK {} RESET TO {at}", ident(&sink(m))),
            )
            .await
        }
        Some(("dead-letters", d)) => {
            let Some((cmd, m)) = d.subcommand() else {
                return Err(Failed("dead-letters needs list, replay or discard".into()));
            };
            let cat = m.get_one::<String>("catalog").cloned().unwrap_or_default();
            let mut c = connect(&cat).await?;
            let schema: Option<String> = c
                .query_first("SELECT DATABASE()")
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let store = sprig_sinks::store::SinksStore::new(schema.unwrap_or_default());
            let id = sprig_sinks::model::SinkId(sink_id(&mut c, &sink(m)).await?);
            match cmd {
                "list" => {
                    let v = store
                        .dead_letters(&mut c, id, 1000)
                        .await
                        .map_err(|e| Failed(e.to_string()))?;
                    for r in v {
                        if out == Output::Json {
                            println!(
                                "{}",
                                serde_json::json!({"id": r.id, "key": r.idem_key, "reason": r.reason,
                                    "class": r.class, "attempts": r.attempts, "state": r.state, "at": r.created_at})
                            );
                        } else {
                            println!(
                                "{}  {}  {}  {}  attempts={}  {}  {}",
                                r.id,
                                r.state,
                                r.created_at,
                                r.idem_key,
                                r.attempts,
                                r.class,
                                r.reason
                            );
                        }
                    }
                }
                "replay" | "discard" => {
                    let ids: Vec<u64> = m
                        .get_many::<u64>("id")
                        .into_iter()
                        .flatten()
                        .copied()
                        .collect();
                    if ids.is_empty() && !m.get_flag("all") {
                        return Err(Failed("name --id n ... or --all".into()));
                    }
                    let state = if cmd == "replay" {
                        "replaying"
                    } else {
                        "discarded"
                    };
                    let n = store
                        .mark_dead(&mut c, id, &ids, state)
                        .await
                        .map_err(|e| Failed(e.to_string()))?;
                    println!("{n} dead letters marked {state}");
                }
                _ => {}
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("connections", d)) => match d.subcommand() {
            Some(("list", m)) => {
                let sql = match m.get_one::<String>("db") {
                    Some(db) => format!("SHOW CONNECTIONS FROM {}", ident(db)),
                    None => "SHOW CONNECTIONS".into(),
                };
                rows(&gw(m), &sql, out).await
            }
            Some(("show", m)) => {
                let n = m.get_one::<String>("name").cloned().unwrap_or_default();
                rows(
                    &gw(m),
                    &format!("SHOW CREATE CONNECTION {}", ident(&n)),
                    out,
                )
                .await
            }
            Some(("test", m)) => {
                let n = m.get_one::<String>("name").cloned().unwrap_or_default();
                let sql = test_connection_sql(
                    &n,
                    m.get_flag("platform"),
                    m.get_flag("write"),
                    m.get_one::<String>("to").map(String::as_str),
                );
                rows(&gw(m), &sql, out).await
            }
            _ => Err(Failed("connections needs list, show or test".into())),
        },
        Some(("platform-connections", d)) => {
            let Some((cmd, m)) = d.subcommand() else {
                return Err(Failed("platform-connections needs a command".into()));
            };
            let g = global(m).await?;
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            match cmd {
                "register" => {
                    let kind =
                        ConnKind::parse(m.get_one::<String>("type").map_or("", String::as_str))
                            .ok_or_else(|| {
                                Failed("--type webhook|kafka|s3|mysql|postgresql".into())
                            })?;
                    let options: Map<String, Value> = pairs(m, "option")?
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect();
                    let tags: Map<String, Value> = pairs(m, "tags")?
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect();
                    let mut hosts: Vec<String> = m
                        .get_many::<String>("host")
                        .into_iter()
                        .flatten()
                        .cloned()
                        .collect();
                    if hosts.is_empty() {
                        hosts = sprig_sinks::sql::hosts_of(kind, &options)
                            .map_err(|e| Failed(e.to_string()))?
                            .into_iter()
                            .map(|(h, p)| format!("{h}:{p}"))
                            .collect();
                    }
                    let c = PlatformConnection {
                        name: name.clone(),
                        kind,
                        options,
                        hidden: m
                            .get_many::<String>("hide-option")
                            .into_iter()
                            .flatten()
                            .cloned()
                            .collect(),
                        tags,
                        description: m
                            .get_one::<String>("description")
                            .cloned()
                            .unwrap_or_default(),
                        visibility: match m.get_one::<String>("visibility").map(String::as_str) {
                            Some("name_only") => Visibility::NameOnly,
                            _ => Visibility::Full,
                        },
                        secrets: pairs(m, "secret")?,
                        previous: pairs(m, "secret-previous")?,
                        hosts,
                    };
                    sprig_sinks::meta::platform_put(&g, &c)
                        .await
                        .map_err(|e| Failed(e.to_string()))?;
                    println!("registered {name}");
                }
                "grant" | "revoke" => {
                    let tenant = m.get_one::<u64>("tenant").copied().unwrap_or(0);
                    let grant = (cmd == "grant").then(|| PlatformGrant {
                        tenant,
                        subtree: m.get_flag("subtree"),
                        granted_by: std::env::var("USER").unwrap_or_else(|_| "operator".into()),
                        at_ms: u64::try_from(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis(),
                        )
                        .unwrap_or(0),
                    });
                    sprig_sinks::meta::platform_grant(&g, &name, grant, tenant)
                        .await
                        .map_err(|e| Failed(e.to_string()))?;
                    println!("{cmd}ed {name} for tenant {tenant}");
                }
                "list" | "show" => {
                    for (c, grants) in sprig_sinks::meta::platform_list(&g)
                        .await
                        .map_err(|e| Failed(e.to_string()))?
                    {
                        if cmd == "show" && c.name != name {
                            continue;
                        }
                        let v = serde_json::json!({
                            "name": c.name, "kind": c.kind.as_str(), "description": c.description,
                            "options": c.options, "hidden": c.hidden, "tags": c.tags,
                            "hosts": c.hosts, "secrets": c.secrets.keys().collect::<Vec<_>>(),
                            "grants": grants.iter().map(|g| serde_json::json!({"tenant": g.tenant, "subtree": g.subtree})).collect::<Vec<_>>(),
                        });
                        println!("{v}");
                    }
                }
                _ => return Err(Failed(format!("platform-connections {cmd}: unknown"))),
            }
            Ok(())
        }
        _ => Err(Failed("sink needs a command".into())),
    }
}

#[cfg(test)]
mod test_statements {
    use super::*;

    /// R9 (r9last): the wrappers send the statements Studio runs.
    #[test]
    fn sink_and_connection_tests() {
        assert_eq!(test_sink_sql("shop.out", None), "TEST SINK `shop`.`out`");
        assert_eq!(
            test_sink_sql("shop.out", Some(25)),
            "TEST SINK `shop`.`out` WITH SAMPLE 25"
        );
        assert_eq!(
            test_connection_sql("shop.hook", false, false, None),
            "TEST CONNECTION `shop`.`hook`"
        );
        assert_eq!(
            test_connection_sql("kafka_main", true, true, Some("audit's")),
            "TEST PLATFORM CONNECTION `kafka_main` WRITE TO 'audit''s'"
        );
        let m = group()
            .try_get_matches_from([
                "sink",
                "connections",
                "test",
                "shop.k",
                "--gateway",
                "mysql://a:b@127.0.0.1:1/x",
                "--to",
                "t",
            ])
            .unwrap_err();
        assert!(m.to_string().contains("--write"), "{m}");
    }
}
