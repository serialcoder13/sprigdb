//! Running statements (chapter 53, "Running statements"): one connection
//! state per tab or CLI run, over the gateway's REST API or a native
//! protocol.

use std::fmt::Write as _;

use serde_json::Value;

use crate::api::{Caps, DataClient};
use crate::error::{Result, ShellCode, ShellError};
use crate::lang::{Lang, redis_argv, top_words};
use crate::model::{EndInfo, ResultEvent, Sink};
use crate::native::Native;

/// The Redis front end's default home of auto-created namespaces
/// (`redis.logical_database`, chapter 40).
pub const REDIS_DATABASE: &str = "redis";

/// A connection's state.
pub struct Runner {
    /// The REST client (always there: the catalog and dashboards use it).
    pub client: DataClient,
    /// A native connection, with `--protocol`.
    pub native: Option<Native>,
    /// The current database (`USE`).
    pub database: Option<String>,
    /// The open transaction handle over REST (`BEGIN` ... `COMMIT`).
    pub txn: Option<String>,
    /// Caps sent with each statement.
    pub caps: Caps,
    /// The logical database of Redis commands over REST.
    pub redis_database: String,
    /// The namespace of Redis commands over REST (`db<n>` after `SELECT n`).
    pub redis_namespace: String,
}

#[allow(clippy::needless_pass_by_value)]
fn out(e: std::io::Error) -> ShellError {
    ShellError::shell(ShellCode::Cancelled, format!("output closed: {e}"))
}

fn local(sink: &mut dyn Sink, info: &str) -> Result<()> {
    sink.event(ResultEvent::End(EndInfo {
        info: info.to_owned(),
        ..EndInfo::default()
    }))
    .map_err(out)
}

impl Runner {
    /// A runner over REST.
    #[must_use]
    pub fn new(client: DataClient, database: Option<String>, caps: Caps) -> Self {
        Self {
            client,
            native: None,
            database,
            txn: None,
            caps,
            redis_database: REDIS_DATABASE.to_owned(),
            redis_namespace: "db0".to_owned(),
        }
    }

    /// A short description of the state, for the status bar.
    #[must_use]
    pub fn describe(&self) -> String {
        let mut s = match &self.native {
            Some(n) => n.describe(),
            None => self.client.http().endpoint().authority(),
        };
        if let Some(t) = &self.client.tenant {
            let _ = write!(s, " tenant {t}");
        }
        if let Some(d) = &self.database {
            let _ = write!(s, " db {d}");
        }
        if self.txn.is_some() || self.native.as_ref().is_some_and(Native::in_transaction) {
            s.push_str(" [txn]");
        }
        s
    }

    /// Runs one statement of `lang`, writing its results to `sink`.
    ///
    /// # Errors
    /// The statement's error; 39983 for a language the transport can't
    /// carry.
    pub async fn run(&mut self, stmt: &str, lang: Lang, sink: &mut dyn Sink) -> Result<()> {
        if let Some(n) = self.native.as_mut() {
            return n.run(stmt, lang, sink).await;
        }
        match lang {
            Lang::Sql => self.run_sql(stmt, sink).await,
            Lang::Redis => self.run_redis(stmt, sink).await,
            Lang::Mongo | Lang::Cypher => Err(ShellError::shell(
                ShellCode::LanguageUnsupported,
                format!(
                    "{} over the REST API needs chapter 37's Documents service or the \
                     statement endpoint's language field, which aren't built yet; use \
                     --protocol {}",
                    lang.name(),
                    if lang == Lang::Mongo { "mongo" } else { "bolt" }
                ),
            )),
        }
    }

    async fn run_sql(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        let w = top_words(stmt, false);
        let word = |i: usize| w.get(i).map_or("", String::as_str);
        match (word(0), word(1)) {
            ("USE", "TENANT") if w.len() == 3 => {
                let t = stmt.split_whitespace().nth(2).unwrap_or_default();
                self.client.tenant = Some(t.trim_matches('`').to_owned());
                return local(sink, &format!("Tenant changed to {}", t.trim_matches('`')));
            }
            ("USE", db) if w.len() == 2 && !db.is_empty() => {
                let name = stmt.split_whitespace().nth(1).unwrap_or_default();
                let name = name.trim_end_matches(';').trim_matches('`').to_owned();
                // Let the gateway check it (1049 for an unknown database).
                let mut sink2 = crate::model::Collect::default();
                self.client
                    .sql_query(
                        Some(&name),
                        "DO 0",
                        self.txn.as_deref(),
                        self.caps,
                        &mut sink2,
                    )
                    .await?;
                self.database = Some(name);
                return local(sink, "Database changed");
            }
            ("BEGIN" | "START", _) if self.txn.is_none() => {
                if word(0) == "START" && word(1) != "TRANSACTION" {
                    return self.query(stmt, sink).await;
                }
                let ro = w.windows(2).any(|p| p[0] == "READ" && p[1] == "ONLY");
                let h = self.client.begin(self.database.as_deref(), ro).await?;
                self.txn = Some(h);
                return local(sink, "Transaction started");
            }
            ("COMMIT" | "ROLLBACK", second) if self.txn.is_some() && !matches!(second, "TO") => {
                let h = self.txn.take().unwrap_or_default();
                let r = self.client.end(&h, word(0) == "COMMIT").await;
                r?;
                return local(
                    sink,
                    if word(0) == "COMMIT" {
                        "Committed"
                    } else {
                        "Rolled back"
                    },
                );
            }
            _ => {}
        }
        let r = self.query(stmt, sink).await;
        if let Err(e) = &r
            && matches!(e.code, 39781..=39783)
        {
            // The handle is gone; later statements autocommit.
            self.txn = None;
        }
        r
    }

    async fn query(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        self.client
            .sql_query(
                self.database.as_deref(),
                stmt,
                self.txn.as_deref(),
                self.caps,
                sink,
            )
            .await
    }

    async fn run_redis(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        let argv = redis_argv(stmt)?;
        let Some(cmd) = argv.first() else {
            return Ok(());
        };
        if cmd.eq_ignore_ascii_case("SELECT") && argv.len() == 2 {
            let n: u32 = argv[1].parse().map_err(|_| {
                ShellError::server(0, "", "ERR value is not an integer or out of range")
            })?;
            self.redis_namespace = format!("db{n}");
            sink.event(ResultEvent::Doc(Value::String("OK".into())))
                .map_err(out)?;
            return local(sink, "");
        }
        let db = self.redis_database.clone();
        let ns = self.redis_namespace.clone();
        let reply = self.client.kv_exec(&db, &ns, &argv).await?;
        if let Some(e) = reply.get("error").and_then(Value::as_str) {
            let code = e
                .split_whitespace()
                .find_map(|w| w.parse::<u32>().ok().filter(|n| (39000..40000).contains(n)))
                .unwrap_or(0);
            return Err(ShellError::server(code, "", e));
        }
        sink.event(ResultEvent::Doc(reply)).map_err(out)?;
        local(sink, "")
    }
}
