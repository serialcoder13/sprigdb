//! `faultd`, the privileged fault sidecar (chapter 46: "partitions by
//! `iptables` from a privileged `faultd` sidecar").
//!
//! The Compose service `faultd` (profile `faults`, image built from
//! `deploy/compose/faultd`) shares the host's PID namespace, so it can
//! enter any container's network namespace with `nsenter` and run
//! `iptables` and `tc` there; the target image needs neither tool. This
//! module drives it with `docker exec` and only ever names this testkit's
//! own containers ([`super::OWN_CONTAINER_PREFIX`]).

use sprig_common::Result;

use super::compose::{docker, ensure_own};
use crate::skip;

/// `SPRIG_TEST_FAULTD`: the faultd container's name, such as
/// `sprigdb-dev-faultd-1`.
pub const FAULTD_VAR: &str = skip::FAULTD;

/// The faultd sidecar.
#[derive(Debug, Clone)]
pub struct Faultd {
    container: String,
}

impl Faultd {
    /// faultd running in `container`.
    #[must_use]
    pub fn new(container: &str) -> Self {
        Self {
            container: container.to_owned(),
        }
    }

    /// faultd named by `SPRIG_TEST_FAULTD`, if set.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        skip::var(FAULTD_VAR).map(|c| Self::new(&c))
    }

    fn pid(target: &str) -> Result<String> {
        ensure_own(target)?;
        docker(&["inspect", "-f", "{{.State.Pid}}", target])
    }

    fn ips(target: &str) -> Result<Vec<String>> {
        ensure_own(target)?;
        let out = docker(&[
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
            target,
        ])?;
        Ok(out.split_whitespace().map(str::to_owned).collect())
    }

    fn run(&self, args: &[&str]) -> Result<String> {
        let mut a = vec![
            "exec",
            self.container.as_str(),
            "/bin/sh",
            "/usr/local/bin/faultd.sh",
        ];
        a.extend_from_slice(args);
        docker(&a)
    }

    /// Drops every packet between `target` and `peer`, in `target`'s
    /// network namespace (call it for both ends for a clean cut).
    ///
    /// # Errors
    /// If either isn't an own container or a command fails.
    pub fn partition(&self, target: &str, peer: &str) -> Result<()> {
        let pid = Self::pid(target)?;
        for ip in Self::ips(peer)? {
            self.run(&["partition", &pid, &ip])?;
        }
        Ok(())
    }

    /// Adds netem delay, jitter and loss to `target`'s `eth0`.
    ///
    /// # Errors
    /// If `target` isn't an own container or `tc` fails.
    pub fn delay(&self, target: &str, ms: u64, jitter_ms: u64, loss_pct: f64) -> Result<()> {
        let pid = Self::pid(target)?;
        self.run(&[
            "delay",
            &pid,
            &ms.to_string(),
            &jitter_ms.to_string(),
            &format!("{loss_pct}"),
        ])
        .map(drop)
    }

    /// Removes faultd's rules and netem from `target`.
    ///
    /// # Errors
    /// If `target` isn't an own container or a command fails.
    pub fn heal(&self, target: &str) -> Result<()> {
        let pid = Self::pid(target)?;
        self.run(&["heal", &pid]).map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_shared_containers() {
        let f = Faultd::new("sprigdb-dev-faultd-1");
        assert!(f.partition("sprigdb-dev-single-1", "sprig-sim-x").is_err());
        assert!(f.heal("sprigdb-dev-primary-1").is_err());
        assert!(f.delay("sprigdb-dev-etcd-1", 10, 0, 0.0).is_err());
    }
}
