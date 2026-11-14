//! Elle tooling: run `elle-cli` on
//! a recorded history, and a list-append workload over MariaDB that
//! produces one.
//!
//! `elle-cli` (Clojure, EPL-2.0) runs as a separate process, never linked:
//! by default in the `sprigdb-elle:dev` image (`docker compose --profile
//! elle build elle`), or from a standalone jar with a local `java` (21 or
//! later). `SPRIG_TEST_ELLE` picks: a path ending in `.jar`, an image name,
//! or `1` for the default image.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_common::{Result, SprigError};

use super::history::{Checker, History, OpKind, OpRecord, Verdict, to_edn};
use crate::sim::SimRng;
use crate::skip;
use crate::util::TempDir;

/// `SPRIG_TEST_ELLE`: how to run elle-cli.
pub const ELLE_VAR: &str = skip::ELLE;

/// The image built from `deploy/compose/elle`.
pub const DEFAULT_IMAGE: &str = "sprigdb-elle:dev";

/// An Elle model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElleModel {
    /// Lists with appends and reads.
    ListAppend,
    /// Registers with writes and reads.
    RwRegister,
    /// Bank transfers (Jepsen's bank checker).
    Bank,
}

impl ElleModel {
    fn name(self) -> &'static str {
        match self {
            ElleModel::ListAppend => "list-append",
            ElleModel::RwRegister => "rw-register",
            ElleModel::Bank => "bank",
        }
    }
}

#[derive(Debug, Clone)]
enum Runner {
    Docker(String),
    Java(PathBuf),
}

/// The `elle-cli` command.
#[derive(Debug, Clone)]
pub struct ElleCli {
    runner: Runner,
}

impl ElleCli {
    /// Runs elle-cli in `image`.
    #[must_use]
    pub fn docker(image: &str) -> Self {
        Self {
            runner: Runner::Docker(image.to_owned()),
        }
    }

    /// Runs a standalone jar with the local `java`.
    #[must_use]
    pub fn jar(path: impl Into<PathBuf>) -> Self {
        Self {
            runner: Runner::Java(path.into()),
        }
    }

    /// As `SPRIG_TEST_ELLE` says, if set.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let v = skip::var(ELLE_VAR)?;
        Some(if Path::new(&v).extension().is_some_and(|e| e == "jar") {
            Self::jar(v)
        } else if matches!(v.as_str(), "1" | "true" | "yes" | "on" | "docker") {
            Self::docker(DEFAULT_IMAGE)
        } else {
            Self::docker(&v)
        })
    }

    /// Checks an EDN history file against `model`, expecting the
    /// `consistency` models (such as `serializable`).
    ///
    /// # Errors
    /// If elle-cli can't run or its output doesn't parse.
    pub fn check_file(
        &self,
        edn: &Path,
        model: ElleModel,
        consistency: &[&str],
    ) -> Result<Verdict> {
        let dir = edn
            .parent()
            .ok_or_else(|| SprigError::internal("history path has no directory"))?;
        let file = edn
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| SprigError::internal("history path has no file name"))?;
        let mut args: Vec<String> =
            vec!["-m".into(), model.name().into(), "-v".into(), "json".into()];
        if !consistency.is_empty() {
            args.push("-c".into());
            args.push(consistency.join(","));
        }
        let mut cmd = match &self.runner {
            Runner::Docker(image) => {
                let mut c = Command::new("docker");
                c.args(["run", "--rm", "--network", "none", "-v"])
                    .arg(format!("{}:/work:ro", dir.display()))
                    .arg(image)
                    .args(&args)
                    .arg(format!("/work/{file}"));
                c
            }
            Runner::Java(jar) => {
                let mut c = Command::new("java");
                c.arg("-jar").arg(jar).args(&args).arg(edn);
                c
            }
        };
        let out = cmd
            .output()
            .map_err(|e| SprigError::internal("running elle-cli").with_source(e))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        parse_output(&stdout).ok_or_else(|| {
            SprigError::internal(format!(
                "elle-cli output didn't parse (exit {:?}): {}{}",
                out.status.code(),
                stdout.chars().take(2000).collect::<String>(),
                String::from_utf8_lossy(&out.stderr)
                    .chars()
                    .take(2000)
                    .collect::<String>()
            ))
        })
    }

    /// Writes `records` as EDN (and JSON lines, for the record) to a
    /// temporary directory and checks them.
    ///
    /// # Errors
    /// As [`Self::check_file`].
    pub fn check(
        &self,
        records: &[OpRecord],
        model: ElleModel,
        consistency: &[&str],
    ) -> Result<Verdict> {
        let dir = TempDir::new("elle")?;
        let edn = dir.join("history.edn");
        std::fs::write(&edn, to_edn(records))
            .map_err(|e| SprigError::internal("writing history").with_source(e))?;
        super::history::write_jsonl(&dir.join("history.jsonl"), records)?;
        let v = self.check_file(&edn, model, consistency)?;
        if v.valid != Some(true) {
            // Keep the evidence.
            let kept = dir.keep();
            eprintln!("elle: history kept in {}", kept.display());
        }
        Ok(v)
    }
}

/// Parses elle-cli's `-v json` output: one JSON map, `valid?` true, false
/// or `"unknown"`.
fn parse_output(stdout: &str) -> Option<Verdict> {
    let start = stdout.find('{')?;
    let v: Value = serde_json::from_str(stdout[start..].trim()).ok()?;
    let valid = match v.get("valid?")? {
        Value::Bool(b) => Some(*b),
        _ => None,
    };
    let anomalies = v
        .get("anomaly-types")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    Some(Verdict {
        valid,
        anomalies,
        detail: stdout[start..].to_owned(),
    })
}

/// A [`Checker`] running elle-cli.
#[derive(Debug, Clone)]
pub struct ElleChecker {
    /// The command.
    pub cli: ElleCli,
    /// The model.
    pub model: ElleModel,
    /// Expected consistency models.
    pub consistency: Vec<String>,
}

impl Checker for ElleChecker {
    fn check(&self, history: &[OpRecord]) -> Verdict {
        let c: Vec<&str> = self.consistency.iter().map(String::as_str).collect();
        self.cli
            .check(history, self.model, &c)
            .unwrap_or_else(|e| Verdict {
                valid: None,
                anomalies: Vec::new(),
                detail: e.to_string(),
            })
    }
}

/// Settings of [`list_append`].
#[derive(Debug, Clone)]
pub struct ListAppendOpts {
    /// Concurrent clients.
    pub processes: u32,
    /// Transactions per client.
    pub txns: u32,
    /// Keys (rows).
    pub keys: i64,
    /// Micro-operations per transaction, at most.
    pub max_ops: u64,
    /// `SET SESSION TRANSACTION ISOLATION LEVEL ...`.
    pub isolation: &'static str,
    /// Seed of the operation mix.
    pub seed: u64,
}

impl Default for ListAppendOpts {
    fn default() -> Self {
        Self {
            processes: 4,
            txns: 50,
            keys: 5,
            max_ops: 4,
            isolation: "SERIALIZABLE",
            seed: 1,
        }
    }
}

/// Runs Elle's list-append workload against MariaDB at `url` (directly
/// or through a toxiproxy route), in table `<schema>.la`, which it creates.
/// Appends are `INSERT ... ON DUPLICATE KEY UPDATE v = CONCAT(...)`;
/// reads parse the comma-separated list.
///
/// # Errors
/// If the table can't be created; failed transactions are recorded, not
/// returned.
pub async fn list_append(url: &str, schema: &str, o: &ListAppendOpts) -> Result<Arc<History>> {
    let opts = mysql_async::Opts::from_url(url)
        .map_err(|e| SprigError::internal(format!("bad URL: {e}")))?;
    let opts: mysql_async::Opts = mysql_async::OptsBuilder::from_opts(opts)
        .prefer_socket(false)
        .into();
    let sql_err = |e: mysql_async::Error| SprigError::internal(format!("list-append setup: {e}"));
    let mut c = mysql_async::Conn::new(opts.clone())
        .await
        .map_err(sql_err)?;
    c.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS `{schema}`.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB"
    ))
    .await
    .map_err(sql_err)?;
    let _ = c.disconnect().await;

    let history = Arc::new(History::new());
    let next_value = Arc::new(AtomicU64::new(1));
    let rng = SimRng::new(o.seed);
    let mut tasks = Vec::new();
    for p in 0..o.processes {
        let (history, next_value, opts) =
            (Arc::clone(&history), Arc::clone(&next_value), opts.clone());
        let (rng, o, schema) = (rng.fork(), o.clone(), schema.to_owned());
        tasks.push(tokio::spawn(async move {
            let mut conn: Option<mysql_async::Conn> = None;
            for _ in 0..o.txns {
                let n = 1 + rng.below(o.max_ops);
                let mops: Vec<(bool, i64, u64)> = (0..n)
                    .map(|_| {
                        let k = i64::try_from(rng.below(u64::try_from(o.keys).unwrap_or(1)))
                            .unwrap_or(0);
                        if rng.chance(0.5) {
                            (true, k, next_value.fetch_add(1, Ordering::Relaxed))
                        } else {
                            (false, k, 0)
                        }
                    })
                    .collect();
                let invoke: Vec<Value> = mops
                    .iter()
                    .map(|(append, k, v)| {
                        if *append {
                            json!(["append", k, v])
                        } else {
                            json!(["r", k, null])
                        }
                    })
                    .collect();
                history.record(p, OpKind::Invoke, "txn", Value::Array(invoke.clone()));
                if conn.is_none() {
                    conn = mysql_async::Conn::new(opts.clone()).await.ok();
                    if let Some(c) = conn.as_mut() {
                        let _ = c
                            .query_drop(format!(
                                "SET SESSION TRANSACTION ISOLATION LEVEL {}",
                                o.isolation
                            ))
                            .await;
                    }
                }
                let Some(c) = conn.as_mut() else {
                    history.record(p, OpKind::Fail, "txn", Value::Array(invoke));
                    continue;
                };
                match run_txn(c, &schema, &mops).await {
                    Ok(done) => history.record(p, OpKind::Ok, "txn", Value::Array(done)),
                    Err(mysql_async::Error::Server(e)) if matches!(e.code, 1205 | 1213) => {
                        let _ = c.query_drop("ROLLBACK").await;
                        history.record(p, OpKind::Fail, "txn", Value::Array(invoke));
                    }
                    Err(_) => {
                        // The commit may or may not have happened.
                        history.record(p, OpKind::Info, "txn", Value::Array(invoke));
                        conn = None;
                    }
                }
            }
            if let Some(c) = conn {
                let _ = c.disconnect().await;
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    Ok(history)
}

async fn run_txn(
    c: &mut mysql_async::Conn,
    schema: &str,
    mops: &[(bool, i64, u64)],
) -> std::result::Result<Vec<Value>, mysql_async::Error> {
    c.query_drop("BEGIN").await?;
    let mut done = Vec::with_capacity(mops.len());
    for (append, k, v) in mops {
        if *append {
            c.exec_drop(
                format!(
                    "INSERT INTO `{schema}`.la (k, v) VALUES (?, ?) \
                     ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))"
                ),
                (k, v.to_string()),
            )
            .await?;
            done.push(json!(["append", k, v]));
        } else {
            let cur: Option<String> = c
                .exec_first(format!("SELECT v FROM `{schema}`.la WHERE k = ?"), (k,))
                .await?;
            let list: Vec<u64> = cur
                .unwrap_or_default()
                .split(',')
                .filter_map(|x| x.parse().ok())
                .collect();
            done.push(json!(["r", k, list]));
        }
    }
    c.query_drop("COMMIT").await?;
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_elle_json() {
        let v = parse_output("{\"valid?\":false,\n \"anomaly-types\":[\"G2-item\"]}").unwrap();
        assert_eq!(v.valid, Some(false));
        assert_eq!(v.anomalies, vec!["G2-item"]);
        let v = parse_output("noise\n{\"valid?\":\"unknown\"}").unwrap();
        assert_eq!(v.valid, None);
        assert!(parse_output("Exception in thread main").is_none());
    }
}
