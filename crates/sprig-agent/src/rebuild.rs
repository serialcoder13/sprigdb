//! The agent's half of `node_rebuild` (recovery after an outage): an
//! internal service on `[agent] internal_addr` that
//! streams a backup of its node to another agent, and rebuilds its own
//! node from another agent's stream.
//!
//! | Route | What |
//! |---|---|
//! | `GET /internal/v1/backup?job=&token=` | `mariadb-backup --backup --stream=mbstream --slave-info --safe-slave-backup` of this node, capped at the ticket's rate |
//! | `POST /internal/v1/rebuild` | `StartRebuild` -> `RebuildStatus`: rebuild this node (idempotent per job) |
//! | `GET /internal/v1/rebuild?job=` | `RebuildStatus` |
//! | `POST /internal/v1/rebuild/cancel?job=` | cancels a rebuild that hasn't touched the data directory |
//!
//! ```text
//! rebuild(job), on the node rebuilt (it stays fenced throughout):
//! 1. streaming: GET the source agent's backup, piped into `mbstream -x`
//!    in <staging>/<job> (mysqld still runs, read_only)
//! 2. preparing: mariadb-backup --prepare; the start position from
//!    mariadb_backup_slave_info (else mariadb_backup_binlog_info)
//! 3. stopping: [agent.rebuild] stop_command; mysqld must refuse connections
//! 4. restoring: the data directory's contents go to <datadir>.pre-rebuild-<job>
//!    (once per job; a retry empties a half-restored directory instead),
//!    mariadb-backup --move-back, chown; the old copy is removed unless the
//!    ticket keeps it
//! 5. starting: start_command; wait until mysqld answers (it boots read_only)
//! 6. started: the worker attaches it to the writer at the reported position
//! ```
//!
//! While a rebuild runs, and after it until the node replicates, the
//! agent's fence never takes the writer lease (the hold flag), so the
//! restored node can't become writable before it follows the writer.
//!
//! Every command runs after `[agent.rebuild] exec_prefix`, so the same
//! code drives a VM (no prefix) and a node in a container
//! (`docker exec -i <name>`). The backup account is the agent's own
//! fencing account; it needs `RELOAD`, `PROCESS`, `LOCK TABLES`,
//! `BINLOG MONITOR` and `SLAVE MONITOR` on the source. Its password goes
//! to `mariadb-backup` in a defaults file created with mode 0600 and
//! removed afterwards, never on a command line.

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::Uri;
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use http_body_util::BodyExt;
use sprig_cluster::failover::internal_tls::{HANDSHAKE_TIMEOUT, InternalTls, split_url};
use sprig_cluster::failover::rebuild::{
    BACKUP_ROUTE, CANCEL_ROUTE, REBUILD_ROUTE, RebuildPhase, RebuildStatus, RebuildTicket,
    StartRebuild, backup_gtid, ticket_key,
};
use sprig_cluster::sql::{NodeEndpoint, ProbeSession};
use sprig_common::config::AgentRebuildConfig;
use sprig_common::ids::{ClusterId, NodeId};
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_meta::{Consistency, MetaStore, Record};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

/// How long mysqld may take to answer after a start.
const START_WAIT: Duration = Duration::from_secs(600);
/// How long mysqld may take to refuse connections after a stop.
const STOP_WAIT: Duration = Duration::from_secs(120);
/// One read of the backup stream.
const CHUNK: usize = 256 * 1024;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The node's shell: every command after `exec_prefix`.
#[derive(Debug, Clone)]
pub struct NodeShell {
    prefix: Vec<String>,
}

impl NodeShell {
    /// A shell running commands after `prefix`.
    #[must_use]
    pub fn new(prefix: Vec<String>) -> Self {
        Self { prefix }
    }

    fn command(&self, args: &[String]) -> Result<Command> {
        let all: Vec<&String> = self.prefix.iter().chain(args.iter()).collect();
        let (first, rest) = all
            .split_first()
            .ok_or_else(|| SprigError::internal("an empty command"))?;
        let mut c = Command::new(first.as_str());
        c.args(rest.iter().map(|s| s.as_str()));
        c.kill_on_drop(true);
        Ok(c)
    }

    /// Runs `args` to its end, feeding `stdin`; its stdout, or an error
    /// with the tail of its stderr.
    ///
    /// # Errors
    /// The command couldn't start or failed.
    pub async fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<String> {
        let mut c = self.command(args)?;
        c.stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
        let mut child = c
            .spawn()
            .map_err(|e| SprigError::internal(format!("{}: {e}", args.join(" "))))?;
        if let (Some(input), Some(mut w)) = (stdin, child.stdin.take()) {
            w.write_all(input)
                .await
                .map_err(|e| SprigError::internal(format!("{}: stdin: {e}", args[0])))?;
            drop(w);
        }
        let out = child
            .wait_with_output()
            .await
            .map_err(|e| SprigError::internal(format!("{}: {e}", args[0])))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(SprigError::internal(format!(
                "{} failed ({}): {}",
                args.first().map_or("", String::as_str),
                out.status,
                tail(&out.stderr)
            )))
        }
    }

    /// Runs a `sh -c` script.
    ///
    /// # Errors
    /// As [`NodeShell::run`].
    pub async fn sh(&self, script: &str) -> Result<String> {
        self.run(&["sh".into(), "-c".into(), script.into()], None)
            .await
    }
}

/// The last lines of a command's stderr, for errors.
fn tail(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(b);
    let lines: Vec<&str> = s.lines().rev().take(4).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

/// A shell-quoted word.
fn q(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// The defaults file with the backup account (the password never goes on
/// a command line).
fn defaults_file(ep: &NodeEndpoint) -> String {
    let user = ep.opts.user().unwrap_or_default().replace('"', "");
    let pass = ep.opts.pass().unwrap_or_default().replace('"', "");
    format!("[client]\nuser=\"{user}\"\npassword=\"{pass}\"\n")
}

/// A short, path-safe form of a job id.
fn short(job: &str) -> String {
    job.chars()
        .filter(char::is_ascii_alphanumeric)
        .take(12)
        .collect()
}

struct Running {
    job_id: String,
    status: Arc<Mutex<RebuildStatus>>,
    cancel: CancellationToken,
    /// The data directory has been touched: no cancel from here on.
    committed: Arc<AtomicBool>,
}

/// The agent's internal rebuild service (see the module docs).
pub struct RebuildService {
    cfg: AgentRebuildConfig,
    store: Arc<dyn MetaStore>,
    cluster: ClusterId,
    node: NodeId,
    endpoint: NodeEndpoint,
    shell: NodeShell,
    hold: Arc<AtomicBool>,
    current: Mutex<Option<Running>>,
    http: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        http_body_util::Full<Bytes>,
    >,
    /// `[tls]`: the service is mutual TLS, and backups are pulled from
    /// `https://` sources with this identity.
    tls: Option<InternalTls>,
}

impl std::fmt::Debug for RebuildService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildService")
            .field("cluster", &self.cluster)
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl RebuildService {
    /// The service for the agent's node. `hold` is the fence's flag: set,
    /// the agent never takes the writer lease.
    #[must_use]
    pub fn new(
        cfg: AgentRebuildConfig,
        store: Arc<dyn MetaStore>,
        cluster: ClusterId,
        node: NodeId,
        endpoint: NodeEndpoint,
        hold: Arc<AtomicBool>,
    ) -> Self {
        let shell = NodeShell::new(cfg.exec_prefix.clone());
        let http =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http();
        Self {
            cfg,
            store,
            cluster,
            node,
            endpoint,
            shell,
            hold,
            current: Mutex::new(None),
            http,
            tls: None,
        }
    }

    /// Serves and pulls backups over mutual TLS with `tls` (`[tls]`).
    #[must_use]
    pub fn with_tls(mut self, tls: InternalTls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// GET `url`: over mutual TLS for `https://`, plain HTTP otherwise.
    async fn get(&self, url: &str) -> Result<hyper::Response<hyper::body::Incoming>> {
        let (https, host, port) = split_url(url)?;
        if !https {
            let uri: hyper::Uri = url
                .parse()
                .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
            return self
                .http
                .get(uri)
                .await
                .map_err(|e| SprigError::internal(format!("{url}: {e}")));
        }
        let tls = self.tls.as_ref().ok_or_else(|| {
            SprigError::internal(format!("{url} needs TLS, and this agent has no [tls]"))
        })?;
        let stream = tls.connect(&host, port).await?;
        let (mut send, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "backup connection ended");
            }
        });
        let path = url
            .splitn(4, '/')
            .nth(3)
            .map_or_else(|| "/".to_owned(), |p| format!("/{p}"));
        let req = hyper::Request::get(path)
            .header(header::HOST, format!("{host}:{port}"))
            .body(http_body_util::Full::new(Bytes::new()))
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
        send.send_request(req)
            .await
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))
    }

    async fn ticket(&self, job: &str) -> Result<RebuildTicket> {
        let key = ticket_key(self.store.tier(), job)?;
        let kv = self
            .store
            .get(&key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| SprigError::internal(format!("no rebuild ticket for job {job}")))?;
        Ok(Record::<RebuildTicket>::decode(&kv.value)?.body)
    }

    fn status(&self, job: &str) -> Option<RebuildStatus> {
        let cur = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        cur.as_ref().filter(|r| r.job_id == job).map(|r| {
            r.status
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        })
    }

    /// Starts rebuilding this node for `req`'s job, or reports the rebuild
    /// already under way for it.
    ///
    /// # Errors
    /// The ticket is missing, isn't for this node, or doesn't admit the
    /// token; another job's rebuild runs.
    pub async fn start(self: &Arc<Self>, req: StartRebuild) -> Result<RebuildStatus> {
        let t = self.ticket(&req.job_id).await?;
        if t.cluster != self.cluster.as_str() || t.target_node != self.node.as_str() {
            return Err(SprigError::internal(format!(
                "job {} rebuilds {}/{}, not this node",
                req.job_id, t.cluster, t.target_node
            )));
        }
        if !t.admits(&req.token, now_ms()) {
            return Err(SprigError::internal("the rebuild token isn't the ticket's"));
        }
        let mut cur = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(r) = cur.as_ref() {
            let st = r
                .status
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            if r.job_id == req.job_id && !matches!(st.phase, RebuildPhase::Failed) {
                return Ok(st);
            }
            if !st.phase.is_final() {
                return Err(SprigError::internal(format!(
                    "job {} is rebuilding this node",
                    r.job_id
                )));
            }
        }
        let status = Arc::new(Mutex::new(RebuildStatus {
            job_id: req.job_id.clone(),
            phase: RebuildPhase::Queued,
            bytes: 0,
            elapsed_ms: 0,
            downtime_ms: None,
            gtid: None,
            old_datadir: None,
            error: None,
        }));
        let running = Running {
            job_id: req.job_id.clone(),
            status: Arc::clone(&status),
            cancel: CancellationToken::new(),
            committed: Arc::new(AtomicBool::new(false)),
        };
        let (cancel, committed) = (running.cancel.clone(), Arc::clone(&running.committed));
        *cur = Some(running);
        drop(cur);
        self.hold.store(true, Ordering::SeqCst);
        let me = Arc::clone(self);
        let st = status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            let r = me
                .rebuild(&t, &req.token, &status, &cancel, &committed, t0)
                .await;
            let mut s = status.lock().unwrap_or_else(PoisonError::into_inner);
            s.elapsed_ms = ms(t0.elapsed());
            match r {
                Ok(()) => {
                    s.phase = RebuildPhase::Started;
                    tracing::warn!(job = %t.job_id, gtid = ?s.gtid, bytes = s.bytes, elapsed_ms = s.elapsed_ms, downtime_ms = ?s.downtime_ms, "node rebuilt: mysqld runs on the restored data, read_only; the worker attaches it");
                }
                Err(e) if cancel.is_cancelled() && !committed.load(Ordering::SeqCst) => {
                    s.phase = RebuildPhase::Cancelled;
                    s.error = Some(e.to_string());
                    me.hold.store(false, Ordering::SeqCst);
                    tracing::warn!(job = %t.job_id, "rebuild cancelled; the node is as it was");
                }
                Err(e) => {
                    s.phase = RebuildPhase::Failed;
                    s.error = Some(e.to_string());
                    // Before the data directory was touched the node is as
                    // it was; after, it stays held (fenced) for an operator.
                    if !committed.load(Ordering::SeqCst) {
                        me.hold.store(false, Ordering::SeqCst);
                    }
                    tracing::error!(job = %t.job_id, error = %e, "rebuild failed");
                }
            }
        });
        Ok(st)
    }

    /// Cancels the rebuild of `job` if the data directory is untouched.
    #[must_use]
    pub fn cancel(&self, job: &str) -> Option<RebuildStatus> {
        let cur = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        let r = cur.as_ref().filter(|r| r.job_id == job)?;
        if !r.committed.load(Ordering::SeqCst) {
            r.cancel.cancel();
        }
        Some(
            r.status
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
    }

    /// Called by the fence each round: a node that replicates again after a
    /// finished rebuild releases the hold.
    pub fn observe_replicating(&self) {
        let cur = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        let done = cur.as_ref().is_none_or(|r| {
            matches!(
                r.status
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .phase,
                RebuildPhase::Started | RebuildPhase::Cancelled
            )
        });
        if done && self.hold.swap(false, Ordering::SeqCst) {
            tracing::info!("the rebuilt node replicates: hold released");
        }
    }

    fn set(status: &Mutex<RebuildStatus>, phase: RebuildPhase) {
        status.lock().unwrap_or_else(PoisonError::into_inner).phase = phase;
    }

    #[allow(clippy::too_many_lines)] // the steps in order
    async fn rebuild(
        &self,
        t: &RebuildTicket,
        token: &str,
        status: &Mutex<RebuildStatus>,
        cancel: &CancellationToken,
        committed: &AtomicBool,
        t0: Instant,
    ) -> Result<()> {
        let cfg = &self.cfg;
        let job = short(&t.job_id);
        let staging = format!("{}/{job}", cfg.staging_dir.trim_end_matches('/'));
        let datadir = cfg.datadir.trim_end_matches('/').to_owned();
        let aside = format!("{datadir}.pre-rebuild-{job}");
        // 1. Stream into a fresh staging directory.
        Self::set(status, RebuildPhase::Streaming);
        self.shell
            .sh(&format!("rm -rf {s} && mkdir -p {s}", s = q(&staging)))
            .await?;
        let mut child = self
            .shell
            .command(&[
                cfg.mbstream_bin.clone(),
                "-x".into(),
                "-C".into(),
                staging.clone(),
            ])?
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| SprigError::internal(format!("mbstream: {e}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| SprigError::internal("mbstream: no stdin"))?;
        let url = format!(
            "{}{BACKUP_ROUTE}?job={}&token={token}",
            t.source_url.trim_end_matches('/'),
            t.job_id
        );
        let resp = self
            .get(&url)
            .await
            .map_err(|e| SprigError::internal(format!("backup from {}: {e}", t.source_node)))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp
                .into_body()
                .collect()
                .await
                .map(|b| String::from_utf8_lossy(&b.to_bytes()).into_owned())
                .unwrap_or_default();
            return Err(SprigError::internal(format!(
                "backup from {}: {code}: {body}",
                t.source_node
            )));
        }
        let mut body = resp.into_body();
        loop {
            let frame = tokio::select! {
                () = cancel.cancelled() => return Err(SprigError::internal("cancelled while streaming")),
                f = body.frame() => f,
            };
            let Some(frame) = frame else { break };
            let frame = frame.map_err(|e| {
                SprigError::internal(format!(
                    "the backup stream from {} broke: {e}",
                    t.source_node
                ))
            })?;
            if let Ok(data) = frame.into_data() {
                stdin
                    .write_all(&data)
                    .await
                    .map_err(|e| SprigError::internal(format!("mbstream: {e}")))?;
                status.lock().unwrap_or_else(PoisonError::into_inner).bytes += data.len() as u64;
            }
        }
        drop(stdin);
        let out = child
            .wait_with_output()
            .await
            .map_err(|e| SprigError::internal(format!("mbstream: {e}")))?;
        if !out.status.success() {
            return Err(SprigError::internal(format!(
                "mbstream failed ({}): {}",
                out.status,
                tail(&out.stderr)
            )));
        }
        tracing::info!(job = %t.job_id, bytes = status.lock().unwrap_or_else(PoisonError::into_inner).bytes, elapsed_ms = ms(t0.elapsed()), "backup received");
        if cancel.is_cancelled() {
            return Err(SprigError::internal("cancelled"));
        }
        // 2. Prepare; read the start position.
        Self::set(status, RebuildPhase::Preparing);
        self.shell
            .run(
                &[
                    cfg.backup_bin.clone(),
                    "--prepare".into(),
                    format!("--target-dir={staging}"),
                ],
                None,
            )
            .await?;
        let slave = self
            .shell
            .sh(&format!(
                "cat {}/mariadb_backup_slave_info 2>/dev/null || true",
                q(&staging)
            ))
            .await?;
        let binlog = self
            .shell
            .sh(&format!(
                "cat {}/mariadb_backup_binlog_info 2>/dev/null || true",
                q(&staging)
            ))
            .await?;
        let gtid = backup_gtid(
            Some(slave.as_str()).filter(|s| !s.is_empty()),
            Some(binlog.as_str()).filter(|s| !s.is_empty()),
        )
        .ok_or_else(|| SprigError::internal("the backup names no GTID position"))?;
        status.lock().unwrap_or_else(PoisonError::into_inner).gtid = Some(gtid.clone());
        if cancel.is_cancelled() {
            return Err(SprigError::internal("cancelled"));
        }
        // 3. Stop mysqld. From here on the rebuild runs to its end.
        committed.store(true, Ordering::SeqCst);
        Self::set(status, RebuildPhase::Stopping);
        let down_at = Instant::now();
        self.shell.run(&cfg.stop_command, None).await?;
        let t_stop = Instant::now();
        while ProbeSession::connect(&self.endpoint, Duration::from_secs(2))
            .await
            .is_ok()
        {
            if t_stop.elapsed() > STOP_WAIT {
                return Err(SprigError::internal(
                    "mysqld still answers after the stop command",
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        // 4. Set the old data aside (once per job), move the backup in.
        Self::set(status, RebuildPhase::Restoring);
        let restore = format!(
            "set -e; d={d}; a={a}; \
             if [ ! -e \"$a\" ]; then mkdir -p \"$a\"; find \"$d\" -mindepth 1 -maxdepth 1 -exec mv {{}} \"$a\"/ \\; ; \
             else find \"$d\" -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + ; fi",
            d = q(&datadir),
            a = q(&aside)
        );
        self.shell.sh(&restore).await?;
        self.shell
            .run(
                &[
                    cfg.backup_bin.clone(),
                    "--move-back".into(),
                    format!("--target-dir={staging}"),
                    format!("--datadir={datadir}"),
                ],
                None,
            )
            .await?;
        self.shell
            .sh(&format!(
                "chown -R {o} {d} && rm -rf {s}",
                o = q(&cfg.owner),
                d = q(&datadir),
                s = q(&staging)
            ))
            .await?;
        if t.keep_old {
            status
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .old_datadir = Some(aside.clone());
        } else {
            self.shell.sh(&format!("rm -rf {}", q(&aside))).await?;
        }
        // 5. Start; wait for it to answer.
        Self::set(status, RebuildPhase::Starting);
        self.shell.run(&cfg.start_command, None).await?;
        let t_start = Instant::now();
        loop {
            if let Ok(s) = ProbeSession::connect(&self.endpoint, Duration::from_secs(2)).await {
                s.close().await;
                break;
            }
            if t_start.elapsed() > START_WAIT {
                return Err(SprigError::internal(
                    "mysqld didn't answer after the restore",
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .downtime_ms = Some(ms(down_at.elapsed()));
        Ok(())
    }

    /// Streams a backup of this node for the ticket of `job`, if `token`
    /// is its token and this node is its source.
    ///
    /// # Errors
    /// The ticket doesn't admit the request; the backup couldn't start.
    #[allow(clippy::too_many_lines)] // the stream, its pacing and its end
    pub async fn backup(&self, job: &str, token: &str) -> Result<Body> {
        let t = self.ticket(job).await?;
        if t.cluster != self.cluster.as_str() || t.source_node != self.node.as_str() {
            return Err(SprigError::internal(format!(
                "job {job} streams from {}/{}, not this node",
                t.cluster, t.source_node
            )));
        }
        if !t.admits(token, now_ms()) {
            return Err(SprigError::internal("the backup token isn't the ticket's"));
        }
        let cfg = &self.cfg;
        let dir = format!(
            "{}/source-{}",
            cfg.staging_dir.trim_end_matches('/'),
            short(job)
        );
        let cnf = format!("{dir}.cnf");
        self.shell
            .run(
                &[
                    "sh".into(),
                    "-c".into(),
                    format!(
                        "umask 077; rm -rf {d}; mkdir -p {d} && cat > {c}",
                        d = q(&dir),
                        c = q(&cnf)
                    ),
                ],
                Some(defaults_file(&self.endpoint).as_bytes()),
            )
            .await?;
        let mut child = self
            .shell
            .command(&[
                cfg.backup_bin.clone(),
                format!("--defaults-extra-file={cnf}"),
                "--backup".into(),
                "--stream=mbstream".into(),
                "--slave-info".into(),
                "--safe-slave-backup".into(),
                format!("--target-dir={dir}"),
            ])?
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| SprigError::internal(format!("mariadb-backup: {e}")))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| SprigError::internal("mariadb-backup: no stdout"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| SprigError::internal("mariadb-backup: no stderr"))?;
        let rate = t.rate_mb_per_second.max(1) * 1024 * 1024;
        let shell = self.shell.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(4);
        let job = job.to_owned();
        tracing::info!(%job, to = %t.target_node, rate_mb_per_second = t.rate_mb_per_second, "streaming a backup of this node");
        tokio::spawn(async move {
            let err_task = tokio::spawn(async move {
                let mut b = Vec::new();
                let _ = stderr.read_to_end(&mut b).await;
                b
            });
            let t0 = Instant::now();
            let mut sent: u64 = 0;
            let mut buf = vec![0u8; CHUNK];
            let mut broken = None;
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        sent += n as u64;
                        if tx
                            .send(Ok(Bytes::copy_from_slice(&buf[..n])))
                            .await
                            .is_err()
                        {
                            broken = Some("the receiver went away".to_owned());
                            break;
                        }
                        // The rate cap: never ahead of `rate` bytes per second.
                        #[allow(clippy::cast_precision_loss)] // pacing
                        let due = Duration::from_secs_f64(sent as f64 / rate as f64);
                        if let Some(wait) = due.checked_sub(t0.elapsed()) {
                            tokio::time::sleep(wait).await;
                        }
                    }
                    Err(e) => {
                        broken = Some(e.to_string());
                        break;
                    }
                }
            }
            if broken.is_some() {
                let _ = child.kill().await;
            }
            let status = child.wait().await;
            let err = err_task.await.unwrap_or_default();
            let _ = shell.sh(&format!("rm -rf {} {}", q(&dir), q(&cnf))).await;
            match (broken, status) {
                (None, Ok(s)) if s.success() => {
                    tracing::info!(%job, bytes = sent, elapsed_ms = ms(t0.elapsed()), "backup streamed");
                }
                (b, s) => {
                    let why = b.unwrap_or_else(|| {
                        format!(
                            "mariadb-backup failed ({}): {}",
                            s.map(|s| s.to_string()).unwrap_or_default(),
                            tail(&err)
                        )
                    });
                    tracing::error!(%job, error = %why, "backup stream failed");
                    // An error frame aborts the response, so the receiver
                    // sees a broken stream, never a short one.
                    let _ = tx.send(Err(std::io::Error::other(why))).await;
                }
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Body::from_stream(stream))
    }
}

fn json<T: serde::Serialize>(v: &T) -> Response {
    match serde_json::to_vec(v) {
        Ok(b) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b))
            .unwrap_or_default(),
        Err(e) => refuse(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn refuse(code: StatusCode, why: &str) -> Response {
    Response::builder()
        .status(code)
        .body(Body::from(why.to_owned()))
        .unwrap_or_default()
}

/// `?job=<id>&token=<hex>`: both are URL-safe as written (UUIDs, hex), so
/// no percent-decoding is needed.
#[derive(Debug, Default)]
struct JobQuery {
    job: String,
    token: String,
}

impl JobQuery {
    fn of(uri: &Uri) -> Self {
        let mut q = Self::default();
        for pair in uri.query().unwrap_or_default().split('&') {
            match pair.split_once('=') {
                Some(("job", v)) => v.clone_into(&mut q.job),
                Some(("token", v)) => v.clone_into(&mut q.token),
                _ => {}
            }
        }
        q
    }
}

async fn backup(State(s): State<Arc<RebuildService>>, uri: Uri) -> Response {
    let q = JobQuery::of(&uri);
    match s.backup(&q.job, &q.token).await {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .unwrap_or_default(),
        Err(e) => {
            tracing::warn!(job = %q.job, error = %e, "backup refused");
            refuse(StatusCode::FORBIDDEN, &e.to_string())
        }
    }
}

async fn start(State(s): State<Arc<RebuildService>>, body: Bytes) -> Response {
    match serde_json::from_slice::<StartRebuild>(&body) {
        Ok(r) => match s.start(r).await {
            Ok(st) => json(&st),
            Err(e) => refuse(StatusCode::CONFLICT, &e.to_string()),
        },
        Err(e) => refuse(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

async fn status(State(s): State<Arc<RebuildService>>, uri: Uri) -> Response {
    let q = JobQuery::of(&uri);
    match s.status(&q.job) {
        Some(st) => json(&st),
        None => refuse(StatusCode::NOT_FOUND, "no rebuild of that job here"),
    }
}

async fn cancel(State(s): State<Arc<RebuildService>>, uri: Uri) -> Response {
    let q = JobQuery::of(&uri);
    match s.cancel(&q.job) {
        Some(st) => json(&st),
        None => refuse(StatusCode::NOT_FOUND, "no rebuild of that job here"),
    }
}

/// The routes.
pub fn router(state: Arc<RebuildService>) -> Router {
    Router::new()
        .route(BACKUP_ROUTE, get(backup))
        .route(REBUILD_ROUTE, post(start).get(status))
        .route(CANCEL_ROUTE, post(cancel))
        .with_state(state)
}

/// Serves the routes on `listener` until `stop`: over mutual TLS when the
/// service has `[tls]` ([`RebuildService::with_tls`]), else plain HTTP.
pub fn serve(
    listener: tokio::net::TcpListener,
    state: Arc<RebuildService>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let tls = state.tls.clone();
    let app = router(state);
    tokio::spawn(async move {
        let stopped = {
            let stop = stop.clone();
            async move { stop.cancelled().await }
        };
        let r = match tls {
            Some(tls) => match TlsListener::start(listener, &tls, stop) {
                Ok(l) => axum::serve(l, app).with_graceful_shutdown(stopped).await,
                Err(e) => Err(e),
            },
            None => {
                axum::serve(listener, app)
                    .with_graceful_shutdown(stopped)
                    .await
            }
        };
        if let Err(e) = r {
            tracing::warn!(error = %e, "agent internal service stopped");
        }
    })
}

/// Mutual-TLS connections for `axum::serve`: TCP accepts here, each
/// handshake in its own task (at most [`HANDSHAKE_TIMEOUT`]), so a slow or
/// foreign client never holds up the others; a failed handshake (no
/// certificate, or one the internal CA didn't sign) is dropped.
struct TlsListener {
    rx: tokio::sync::mpsc::Receiver<(
        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        std::net::SocketAddr,
    )>,
    local: std::net::SocketAddr,
}

impl TlsListener {
    fn start(
        listener: tokio::net::TcpListener,
        tls: &InternalTls,
        stop: CancellationToken,
    ) -> std::io::Result<Self> {
        let local = listener.local_addr()?;
        let acceptor = tls.acceptor();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let (tcp, peer) = tokio::select! {
                    () = stop.cancelled() => return,
                    r = listener.accept() => match r {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::debug!(error = %e, "internal service accept failed");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    },
                };
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(s)) => {
                            let _ = tx.send((s, peer)).await;
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(%peer, error = %e, "internal service: TLS handshake refused");
                        }
                        Err(_) => {
                            tracing::warn!(%peer, "internal service: TLS handshake timed out");
                        }
                    }
                });
            }
        });
        Ok(Self { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(c) => c,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_quoted_for_the_shell() {
        assert_eq!(q("/var/lib/mysql"), "'/var/lib/mysql'");
        assert_eq!(q("it's"), "'it'\\''s'");
        assert_eq!(short("0192e0a1-7c3b-7e21-9a4f"), "0192e0a17c3b");
    }

    #[tokio::test]
    async fn the_shell_runs_after_its_prefix_and_reports_failures() {
        let sh = NodeShell::new(vec!["env".into()]);
        let out = sh.sh("echo hi").await.expect("runs");
        assert_eq!(out.trim(), "hi");
        let err = sh.sh("echo oops >&2; exit 3").await.expect_err("fails");
        assert!(err.to_string().contains("oops"), "{err}");
        let fed = NodeShell::new(Vec::new())
            .run(&["cat".into()], Some(b"fed"))
            .await
            .expect("cat");
        assert_eq!(fed, "fed");
    }
}
