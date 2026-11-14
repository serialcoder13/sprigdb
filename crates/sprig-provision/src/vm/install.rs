//! The agent's install and configure steps on a VM, as data: a list of [`Step`]s the agent's plan
//! engine runs in order, and that tests (and `--dry-run`) print.
//!
//! ```text
//! install:   pin MariaDB's repository at provision.mariadb_version; keep the
//!            package from starting mariadb.service (the agent starts it);
//!            install mariadb-server, mariadb-backup (galera-4 for Galera)
//! configure: the baseline settings file, TLS files, the mariadb.service
//!            drop-ins, daemon-reload, mariadb.service disabled at boot
//! ```

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::units::{self, DROPIN_DIR, StartRole};
use crate::mycnf::{self, NodeRole, NodeSettings};
use crate::{MariaDbVersion, Topology};

/// The package manager of a host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distro {
    /// Debian and Ubuntu: apt.
    Apt,
    /// RHEL-compatible: dnf.
    Dnf,
}

impl Distro {
    /// Where the baseline settings file goes.
    #[must_use]
    pub const fn cnf_path(self) -> &'static str {
        match self {
            Self::Apt => "/etc/mysql/mariadb.conf.d/60-sprigdb.cnf",
            Self::Dnf => "/etc/my.cnf.d/60-sprigdb.cnf",
        }
    }

    /// The Galera provider library.
    #[must_use]
    pub const fn galera_provider(self) -> &'static str {
        match self {
            Self::Apt => "/usr/lib/galera/libgalera_smm.so",
            Self::Dnf => "/usr/lib64/galera-4/libgalera_smm.so",
        }
    }
}

/// A host's operating system, as its agent reports it (`/etc/os-release`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HostOs {
    /// The package manager.
    pub distro: Distro,
    /// `ID`: `debian`, `ubuntu`, `rhel`, `rocky`, ...
    pub id: String,
    /// `VERSION_CODENAME` (apt hosts): `bookworm`, `noble`, ...
    #[serde(default)]
    pub codename: String,
}

/// One step of a node plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum Step {
    /// Writes a file (parents created).
    WriteFile {
        /// The path.
        path: String,
        /// Octal mode, such as `0644`.
        mode: String,
        /// The contents.
        content: String,
    },
    /// Removes a file if it exists.
    RemoveFile {
        /// The path.
        path: String,
    },
    /// Runs a command; a non-zero exit fails the step.
    Run {
        /// argv.
        argv: Vec<String>,
    },
}

fn run(args: &[&str]) -> Step {
    Step::Run {
        argv: args.iter().map(|s| (*s).to_owned()).collect(),
    }
}

fn file(path: &str, mode: &str, content: String) -> Step {
    Step::WriteFile {
        path: path.to_owned(),
        mode: mode.to_owned(),
        content,
    }
}

/// MariaDB's repository for `v`, in the layout of MariaDB's own
/// repository setup (`dlm.mariadb.com/repo/mariadb-server/<v>/...`).
#[must_use]
pub fn repository(os: &HostOs, v: MariaDbVersion) -> Step {
    let (id, codename) = (&os.id, &os.codename);
    match os.distro {
        Distro::Apt => file(
            "/etc/apt/sources.list.d/mariadb.sources",
            "0644",
            format!(
                "# MariaDB {v}, pinned by SprigDB (provision.mariadb_version).\n\
                 X-Repolib-Name: MariaDB\n\
                 Types: deb\n\
                 URIs: https://dlm.mariadb.com/repo/mariadb-server/{v}/repo/{id}\n\
                 Suites: {codename}\n\
                 Components: main\n\
                 Signed-By: /etc/apt/keyrings/mariadb-keyring.pgp\n"
            ),
        ),
        Distro::Dnf => file(
            "/etc/yum.repos.d/MariaDB.repo",
            "0644",
            format!(
                "# MariaDB {v}, pinned by SprigDB (provision.mariadb_version).\n\
                 [mariadb-main]\n\
                 name = MariaDB Server\n\
                 baseurl = https://dlm.mariadb.com/repo/mariadb-server/{v}/yum/rhel/$releasever/$basearch\n\
                 gpgkey = https://supplychain.mariadb.com/MariaDB-Server-GPG-KEY\n\
                 gpgcheck = 1\n\
                 enabled = 1\n\
                 module_hotfixes = 1\n"
            ),
        ),
    }
}

/// The packages for `topology`.
#[must_use]
pub fn packages(distro: Distro, topology: Topology) -> Vec<&'static str> {
    let mut p = match distro {
        Distro::Apt => vec!["mariadb-server", "mariadb-backup"],
        Distro::Dnf => vec!["MariaDB-server", "MariaDB-backup"],
    };
    if topology == Topology::Galera {
        p.push("galera-4");
    }
    p
}

/// Duty 1: install MariaDB `v` without starting it.
#[must_use]
pub fn install_steps(os: &HostOs, v: MariaDbVersion, topology: Topology) -> Vec<Step> {
    let distro = os.distro;
    let mut steps = vec![repository(os, v)];
    let pkgs = packages(distro, topology);
    match distro {
        Distro::Apt => {
            // Debian's maintainer scripts start a service they install; a
            // policy-rc.d that answers 101 stops that, so MariaDB never
            // runs unfenced or with the package's settings.
            steps.push(file(
                "/usr/sbin/policy-rc.d",
                "0755",
                "#!/bin/sh\n# SprigDB: services start through the agent only.\nexit 101\n".into(),
            ));
            steps.push(run(&["apt-get", "update"]));
            let mut argv = vec!["apt-get", "install", "-y", "--no-install-recommends"];
            argv.extend(pkgs);
            let mut s = run(&argv);
            if let Step::Run { argv } = &mut s {
                argv.insert(0, "DEBIAN_FRONTEND=noninteractive".into());
                argv.insert(0, "env".into());
            }
            steps.push(s);
            steps.push(Step::RemoveFile {
                path: "/usr/sbin/policy-rc.d".into(),
            });
        }
        Distro::Dnf => {
            let mut argv = vec!["dnf", "install", "-y"];
            argv.extend(pkgs);
            steps.push(run(&argv));
        }
    }
    steps.push(run(&["systemctl", "stop", "mariadb.service"]));
    steps.push(run(&["systemctl", "disable", "mariadb.service"]));
    steps
}

/// Duty 2: the node's settings, TLS files and drop-ins.
#[must_use]
pub fn configure_steps(distro: Distro, node: &NodeSettings, tls_pem: Option<&TlsPem>) -> Vec<Step> {
    let mut steps = Vec::new();
    if let (Some(t), Some(files)) = (tls_pem, &node.tls) {
        steps.push(file(&files.ca, "0644", t.ca.clone()));
        steps.push(file(&files.cert, "0644", t.cert.clone()));
        steps.push(file(&files.key, "0600", t.key.clone()));
        steps.push(run(&["chown", "mysql:mysql", &files.key]));
    }
    steps.push(file(distro.cnf_path(), "0644", mycnf::render(node)));
    steps.push(file(
        &format!("{DROPIN_DIR}/10-sprigdb-limits.conf"),
        "0644",
        units::mariadb_limits_dropin(node.open_files_limit),
    ));
    let role = if node.role == NodeRole::Replica {
        StartRole::Slave
    } else {
        StartRole::Master
    };
    steps.push(file(
        &format!("{DROPIN_DIR}/20-sprigdb-role.conf"),
        "0644",
        units::mariadb_role_dropin(role),
    ));
    steps.push(run(&["systemctl", "daemon-reload"]));
    steps
}

/// TLS material in PEM, from the deployment's CA.
#[derive(Clone, PartialEq, Eq)]
pub struct TlsPem {
    /// The CA bundle.
    pub ca: String,
    /// The server certificate.
    pub cert: String,
    /// Its key.
    pub key: String,
}

impl std::fmt::Debug for TlsPem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsPem").finish_non_exhaustive()
    }
}

/// A shell script running `steps`, for `--dry-run` output and for running
/// a plan by hand.
#[must_use]
pub fn script(steps: &[Step]) -> String {
    let mut s = String::from("#!/bin/sh\n# Generated by SprigDB.\nset -eu\n");
    for st in steps {
        match st {
            Step::WriteFile {
                path,
                mode,
                content,
            } => {
                let dir = path.rsplit_once('/').map_or("/", |(d, _)| d);
                let _ = writeln!(s, "mkdir -p '{dir}'");
                let _ = write!(s, "cat > '{path}' <<'SPRIGDB_EOF'\n{content}");
                if !content.ends_with('\n') {
                    s.push('\n');
                }
                s.push_str("SPRIGDB_EOF\n");
                let _ = writeln!(s, "chmod {mode} '{path}'");
            }
            Step::RemoveFile { path } => {
                let _ = writeln!(s, "rm -f '{path}'");
            }
            Step::Run { argv } => {
                let q: Vec<String> = argv.iter().map(|a| format!("'{a}'")).collect();
                s.push_str(&q.join(" "));
                s.push('\n');
            }
        }
    }
    s
}
