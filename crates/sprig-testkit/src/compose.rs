//! Docker Compose fixtures (chapter 46, "Integration with Docker Compose";
//! chapter 27, "Development setup").
//!
//! [`ComposeProject`] describes what to start: Compose files, a project
//! name, profiles and variables. [`ComposeProject::up`] starts it, waits
//! until every container is healthy and returns a [`ComposeStack`]. Dropping
//! the stack tears it down (`down -v`); if the test is panicking it first
//! saves the project's logs and prints where they are. Set
//! `SPRIG_TEST_KEEP=1` to leave the project running for debugging.
//!
//! The commands are blocking: start a stack before entering async code, or
//! from `spawn_blocking`. Gate tests that start stacks on
//! `SPRIG_TEST_DOCKER=1` (see [`crate::skip`]).
//!
//! ```no_run
//! use sprig_testkit::{ComposeProject, require_env, skip};
//!
//! #[test]
//! fn three_dcs_come_up() {
//!     require_env!(skip::DOCKER);
//!     let stack = ComposeProject::three_dc().up().unwrap();
//!     let addr = stack.port("mum1-db-1", 3306).unwrap();
//!     assert!(addr.ends_with(":34106"));
//! }   // `down -v` here
//! ```
//!
//! Compose files with fixed host ports (both files in `deploy/compose/`)
//! can run once per machine at a time, so give such a project a fixed name
//! and don't run two tests on it in parallel.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use sprig_common::{Result, SprigError};

use crate::env::workspace_root;
use crate::skip;
use crate::util::{Retry, unique_name};

/// The project name of the three-DC layout.
pub const THREE_DC_PROJECT: &str = "sprigdb-3dc";

/// `deploy/compose/` in this workspace.
#[must_use]
pub fn compose_dir() -> PathBuf {
    workspace_root().join("deploy").join("compose")
}

/// Whether `docker compose` can run here.
#[must_use]
pub fn docker_available() -> bool {
    Command::new("docker")
        .args(["compose", "version"])
        .output()
        .is_ok_and(|o| o.status.success())
}

/// A Compose project to start: files, name, profiles and variables.
#[derive(Debug, Clone)]
pub struct ComposeProject {
    files: Vec<PathBuf>,
    name: String,
    profiles: Vec<String>,
    vars: Vec<(String, String)>,
    wait_timeout: Duration,
}

impl ComposeProject {
    /// A project for `file` (absolute, or relative to the workspace root),
    /// named `sprig-test-<unique>` so parallel tests don't collide.
    #[must_use]
    pub fn new(file: impl AsRef<Path>) -> Self {
        let file = file.as_ref();
        let file = if file.is_absolute() {
            file.to_path_buf()
        } else {
            workspace_root().join(file)
        };
        Self {
            files: vec![file],
            name: unique_name("sprig-test"),
            profiles: Vec::new(),
            vars: Vec::new(),
            wait_timeout: Duration::from_secs(300),
        }
    }

    /// The three-DC layout, `deploy/compose/compose.3dc.yaml`, under its
    /// fixed project name [`THREE_DC_PROJECT`].
    #[must_use]
    pub fn three_dc() -> Self {
        Self::new(compose_dir().join("compose.3dc.yaml")).name(THREE_DC_PROJECT)
    }

    /// Adds another Compose file, merged over the earlier ones.
    #[must_use]
    pub fn file(mut self, file: impl AsRef<Path>) -> Self {
        let f = file.as_ref();
        self.files.push(if f.is_absolute() {
            f.to_path_buf()
        } else {
            workspace_root().join(f)
        });
        self
    }

    /// Sets the project name (`-p`). It overrides `COMPOSE_PROJECT_NAME`
    /// from an `.env` file.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Enables a profile.
    #[must_use]
    pub fn profile(mut self, profile: impl Into<String>) -> Self {
        self.profiles.push(profile.into());
        self
    }

    /// Sets a variable for interpolation in the Compose files.
    #[must_use]
    pub fn var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.vars.push((key.into(), value.into()));
        self
    }

    /// How long `up` waits for every container to be healthy (default 300 s).
    #[must_use]
    pub fn wait_timeout(mut self, d: Duration) -> Self {
        self.wait_timeout = d;
        self
    }

    /// The project name.
    #[must_use]
    pub fn project_name(&self) -> &str {
        &self.name
    }

    /// A `docker compose` command for this project, without a subcommand.
    #[must_use]
    pub fn command(&self) -> Command {
        let mut c = Command::new("docker");
        c.arg("compose").arg("-p").arg(&self.name);
        for f in &self.files {
            c.arg("-f").arg(f);
        }
        for p in &self.profiles {
            c.arg("--profile").arg(p);
        }
        c.envs(self.vars.iter().map(|(k, v)| (k, v)));
        c
    }

    fn run<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
        let shown = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(" ");
        let out = self.command().args(&args).output().map_err(|e| {
            SprigError::internal(format!("running docker compose {shown}")).with_source(e)
        })?;
        if out.status.success() {
            Ok(out)
        } else {
            Err(SprigError::internal(format!(
                "docker compose -p {} {shown} failed ({}): {}",
                self.name,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }

    /// Validates the files (`config --quiet`).
    ///
    /// # Errors
    /// If Compose rejects them.
    pub fn validate(&self) -> Result<()> {
        self.run(["config", "--quiet"]).map(drop)
    }

    /// Starts the project and waits until every container is healthy (or
    /// running, if it has no health check). On failure the logs are saved,
    /// the project is torn down (unless `SPRIG_TEST_KEEP=1`), and the error
    /// names the log file and ends with the logs' last lines.
    ///
    /// # Errors
    /// If `up` fails or a container isn't healthy within the timeout.
    pub fn up(self) -> Result<ComposeStack> {
        let secs = self.wait_timeout.as_secs().max(1).to_string();
        let started = self.run(["up", "-d", "--wait", "--wait-timeout", secs.as_str()]);
        let stack = ComposeStack {
            project: self,
            owned: true,
        };
        match started {
            Ok(_) => Ok(stack),
            Err(e) => {
                let (path, tail) = stack.save_logs("up-failed");
                Err(SprigError::internal(format!(
                    "{}\nlogs: {path}\n{tail}",
                    e.message
                )))
                // `stack` drops here and tears the project down.
            }
        }
    }

    /// A stack for a project something else started; dropping it leaves
    /// the project running.
    #[must_use]
    pub fn attach(self) -> ComposeStack {
        ComposeStack {
            project: self,
            owned: false,
        }
    }
}

/// One container's state, from `docker compose ps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerState {
    /// The Compose service.
    pub service: String,
    /// The container name.
    pub name: String,
    /// `running`, `exited`, ...
    pub state: String,
    /// `healthy`, `starting`, `unhealthy`, or empty without a health check.
    pub health: String,
    /// The exit code, for an exited container.
    pub exit_code: i64,
}

impl ContainerState {
    /// Running and healthy (or without a health check), or a one-shot
    /// container that exited with 0.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        match self.state.as_str() {
            "running" => self.health.is_empty() || self.health == "healthy",
            "exited" => self.exit_code == 0,
            _ => false,
        }
    }
}

/// Parses `docker compose ps --format json`: one object per line (Compose
/// 2.21 and later) or one array (earlier).
fn parse_ps(text: &str) -> Result<Vec<ContainerState>> {
    let bad =
        |e: serde_json::Error| SprigError::internal("parsing docker compose ps").with_source(e);
    let trimmed = text.trim();
    let values: Vec<serde_json::Value> = if trimmed.starts_with('[') {
        serde_json::from_str(trimmed).map_err(bad)?
    } else {
        trimmed
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()
            .map_err(bad)?
    };
    let s = |v: &serde_json::Value, k: &str| v[k].as_str().unwrap_or_default().to_owned();
    Ok(values
        .iter()
        .map(|v| ContainerState {
            service: s(v, "Service"),
            name: s(v, "Name"),
            state: s(v, "State"),
            health: s(v, "Health"),
            exit_code: v["ExitCode"].as_i64().unwrap_or_default(),
        })
        .collect())
}

/// A started Compose project; torn down on drop when this fixture started it.
#[derive(Debug)]
pub struct ComposeStack {
    project: ComposeProject,
    owned: bool,
}

impl ComposeStack {
    /// The project.
    #[must_use]
    pub fn project(&self) -> &ComposeProject {
        &self.project
    }

    /// Every container of the project, stopped ones included.
    ///
    /// # Errors
    /// If `docker compose ps` fails.
    pub fn ps(&self) -> Result<Vec<ContainerState>> {
        let out = self.project.run(["ps", "--all", "--format", "json"])?;
        parse_ps(&String::from_utf8_lossy(&out.stdout))
    }

    /// Waits until every container is ready ([`ContainerState::is_ready`]),
    /// for example after restarting one.
    ///
    /// # Errors
    /// On timeout, naming the containers that weren't ready.
    pub fn wait_healthy(&self, timeout: Duration) -> Result<()> {
        Retry::new(timeout)
            .interval(Duration::from_millis(500))
            .run_blocking(
                &format!("waiting for {} to be healthy", self.project.name),
                || {
                    let ps = self.ps().map_err(|e| e.message)?;
                    if ps.is_empty() {
                        return Err("no containers".to_owned());
                    }
                    let waiting: Vec<String> = ps
                        .iter()
                        .filter(|c| !c.is_ready())
                        .map(|c| format!("{} ({} {})", c.service, c.state, c.health))
                        .collect();
                    if waiting.is_empty() {
                        Ok(())
                    } else {
                        Err(format!("not ready: {}", waiting.join(", ")))
                    }
                },
            )
    }

    /// The logs of every container, with timestamps.
    ///
    /// # Errors
    /// If `docker compose logs` fails.
    pub fn logs(&self) -> Result<String> {
        let out = self.project.run(["logs", "--no-color", "--timestamps"])?;
        let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
        s.push_str(&String::from_utf8_lossy(&out.stderr));
        Ok(s)
    }

    /// Runs a command in a service's container and returns its stdout.
    ///
    /// # Errors
    /// If the command fails; the error carries its stderr.
    pub fn exec(&self, service: &str, args: &[&str]) -> Result<String> {
        let mut all = vec!["exec", "-T", service];
        all.extend_from_slice(args);
        let out = self.project.run(all)?;
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// The host address (`127.0.0.1:13301`) published for a service's
    /// container port.
    ///
    /// # Errors
    /// If the port isn't published.
    pub fn port(&self, service: &str, container_port: u16) -> Result<String> {
        let p = container_port.to_string();
        let out = self.project.run(["port", service, p.as_str()])?;
        let addr = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        if addr.is_empty() {
            Err(SprigError::internal(format!(
                "{service}:{container_port} is not published"
            )))
        } else {
            Ok(addr)
        }
    }

    /// Saves the logs to `$SPRIG_TEST_LOG_DIR` (default: the system temp
    /// directory's `sprig-testkit/logs`) and returns the file's path and the
    /// last 60 lines. Never fails: problems are described in the result.
    fn save_logs(&self, why: &str) -> (String, String) {
        let logs = self
            .logs()
            .unwrap_or_else(|e| format!("(collecting logs failed: {e})"));
        let dir = skip::var(skip::LOG_DIR).map_or_else(
            || std::env::temp_dir().join("sprig-testkit").join("logs"),
            PathBuf::from,
        );
        let file = dir.join(format!("{}-{why}.log", self.project.name));
        let path = match std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&file, &logs)) {
            Ok(()) => file.display().to_string(),
            Err(e) => format!("(not saved: {e})"),
        };
        let lines: Vec<&str> = logs.lines().collect();
        let mut tail = String::new();
        for l in &lines[lines.len().saturating_sub(60)..] {
            let _ = writeln!(tail, "  {l}");
        }
        (path, tail)
    }

    /// Stops the project and deletes its volumes now, instead of on drop.
    ///
    /// # Errors
    /// If `docker compose down` fails.
    pub fn down(mut self) -> Result<()> {
        self.owned = false;
        self.project
            .run(["down", "-v", "--remove-orphans", "--timeout", "10"])
            .map(drop)
    }
}

impl Drop for ComposeStack {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let (path, tail) = self.save_logs("failed");
            eprintln!("compose project {} logs: {path}\n{tail}", self.project.name);
        }
        if !self.owned {
            return;
        }
        if skip::flag(skip::KEEP) {
            eprintln!(
                "SPRIG_TEST_KEEP is set: leaving compose project {} running",
                self.project.name
            );
            return;
        }
        if let Err(e) = self
            .project
            .run(["down", "-v", "--remove-orphans", "--timeout", "10"])
        {
            eprintln!("tearing down {}: {e}", self.project.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ps_lines_and_arrays() {
        let lines = r#"{"Service":"db","Name":"p-db-1","State":"running","Health":"healthy","ExitCode":0}
{"Service":"init","Name":"p-init-1","State":"exited","Health":"","ExitCode":0}
{"Service":"web","Name":"p-web-1","State":"running","Health":"starting","ExitCode":0}"#;
        let ps = parse_ps(lines).unwrap();
        assert_eq!(ps.len(), 3);
        assert!(ps[0].is_ready() && ps[1].is_ready() && !ps[2].is_ready());

        let array =
            r#"[{"Service":"db","Name":"p-db-1","State":"exited","Health":"","ExitCode":1}]"#;
        let ps = parse_ps(array).unwrap();
        assert_eq!(ps[0].service, "db");
        assert!(!ps[0].is_ready());
        assert!(parse_ps("").unwrap().is_empty());
    }

    #[test]
    fn command_carries_project_files_and_profiles() {
        let p = ComposeProject::new("deploy/compose/compose.yaml")
            .name("p1")
            .profile("single")
            .var("MARIADB_IMAGE", "mariadb:12.3");
        let c = p.command();
        let args: Vec<String> = c
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(&args[..3], ["compose", "-p", "p1"]);
        assert!(args[4].ends_with("deploy/compose/compose.yaml"));
        assert_eq!(&args[5..], ["--profile", "single"]);
        assert!(c.get_envs().any(|(k, _)| k == "MARIADB_IMAGE"));
    }

    #[test]
    fn default_names_are_unique() {
        let a = ComposeProject::new("x.yaml");
        let b = ComposeProject::new("x.yaml");
        assert_ne!(a.project_name(), b.project_name());
        assert_eq!(ComposeProject::three_dc().project_name(), THREE_DC_PROJECT);
    }
}
