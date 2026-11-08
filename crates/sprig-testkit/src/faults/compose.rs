//! Faults against Docker Compose: [`ComposeFaults`] implements [`Faults`]
//! with toxiproxy (link faults on proxied routes), `faultd` (partitions
//! between containers), the Docker CLI (kill, pause) and libfaketime (clock
//! skew in throwaway containers started for it, [`super::faketime`]), and
//! [`OwnContainer`] runs the throwaway `sprig-sim-*` containers those
//! faults may touch.
//!
//! Faults never reach a shared server directly: container faults check
//! the [`OWN_CONTAINER_PREFIX`] and link faults act on toxiproxy routes the
//! caller registered with [`ComposeFaults::route`].

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sprig_common::{Result, SprigError};

use super::faultd::Faultd;
use super::toxiproxy::{Toxic, Toxiproxy};
use super::{FaultId, Faults, LinkFault, Node, OWN_CONTAINER_PREFIX};
use crate::skip;
use crate::util::{Retry, unique_name};

/// Runs `docker` with `args`; returns stdout, trimmed.
///
/// # Errors
/// If docker can't run or exits non-zero; the error carries stderr.
pub fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| SprigError::internal("running docker").with_source(e))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        Err(SprigError::internal(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

/// Refuses any container that isn't this testkit's own.
///
/// # Errors
/// When `name` doesn't start with [`OWN_CONTAINER_PREFIX`].
pub fn ensure_own(name: &str) -> Result<()> {
    if name.starts_with(OWN_CONTAINER_PREFIX) {
        Ok(())
    } else {
        Err(SprigError::internal(format!(
            "refusing to inject a fault into {name}: only {OWN_CONTAINER_PREFIX}* containers"
        )))
    }
}

/// A container this test started, named `sprig-sim-<prefix>-<unique>`.
/// Dropping it removes it, unless the test is panicking and
/// `SPRIG_TEST_KEEP=1` asks to keep it for debugging.
#[derive(Debug)]
pub struct OwnContainer {
    name: String,
}

impl OwnContainer {
    /// `docker run -d --name <name> <args>`; `args` holds options, the
    /// image and its command.
    ///
    /// # Errors
    /// If docker fails.
    pub fn run(prefix: &str, args: &[&str]) -> Result<Self> {
        let name = unique_name(&format!("{OWN_CONTAINER_PREFIX}{prefix}"));
        let mut a = vec!["run", "-d", "--name", &name, "--label", "sprig-sim=1"];
        a.extend_from_slice(args);
        docker(&a)?;
        Ok(Self { name })
    }

    /// The container's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The host `ip:port` published for `port`.
    ///
    /// # Errors
    /// If the port isn't published.
    pub fn port(&self, port: u16) -> Result<String> {
        let out = docker(&["port", &self.name, &format!("{port}/tcp")])?;
        out.lines()
            .find(|l| l.starts_with("127.0.0.1:") || l.starts_with("0.0.0.0:"))
            .map(|l| l.replace("0.0.0.0:", "127.0.0.1:"))
            .ok_or_else(|| {
                SprigError::internal(format!("{}: port {port} not published", self.name))
            })
    }

    /// The container's IP address on its first network.
    ///
    /// # Errors
    /// If docker fails.
    pub fn ip(&self) -> Result<String> {
        docker(&[
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
            &self.name,
        ])
        .map(|s| s.split_whitespace().next().unwrap_or_default().to_owned())
    }
}

impl Drop for OwnContainer {
    fn drop(&mut self) {
        if std::thread::panicking() && skip::flag(skip::KEEP) {
            eprintln!("keeping container {}", self.name);
            return;
        }
        let _ = docker(&["rm", "-f", "-v", &self.name]);
    }
}

/// Root password of throwaway MariaDB containers.
pub const OWN_MARIADB_PASSWORD: &str = "sprig-sim-root";

/// A throwaway single-node MariaDB in a container, configured like the
/// Compose nodes (binlog, GTIDs, durability).
#[derive(Debug)]
pub struct OwnMariaDb {
    /// The container.
    pub container: OwnContainer,
    /// `mysql://root:…@127.0.0.1:<port>`.
    pub url: String,
}

impl OwnMariaDb {
    /// Starts `image` (default `MARIADB_IMAGE` or `mariadb:11.8`) with
    /// `server_id` and `domain`, and waits until it accepts connections.
    ///
    /// # Errors
    /// If the container doesn't start or never accepts connections.
    pub async fn start(image: Option<&str>, server_id: u32, domain: u32) -> Result<Self> {
        Self::run(&Self::image(image), server_id, domain).await
    }

    /// Like [`start`](Self::start), from the image's
    /// [`faketime`](super::faketime) variant (built on first use): its
    /// wall clock can be skewed with
    /// [`ComposeFaults::skew_clock`](super::ComposeFaults) or
    /// [`faketime::set`](super::faketime::set).
    ///
    /// # Errors
    /// If the image can't be built, or as [`start`](Self::start).
    pub async fn start_with_faketime(
        image: Option<&str>,
        server_id: u32,
        domain: u32,
    ) -> Result<Self> {
        let image = super::faketime::ensure_image(&Self::image(image))?;
        Self::run(&image, server_id, domain).await
    }

    fn image(image: Option<&str>) -> String {
        image
            .map(str::to_owned)
            .or_else(|| skip::var("MARIADB_IMAGE"))
            .unwrap_or_else(|| "mariadb:11.8".to_owned())
    }

    async fn run(image: &str, server_id: u32, domain: u32) -> Result<Self> {
        let cnf = crate::compose::compose_dir()
            .join("mariadb")
            .join("binlog.cnf");
        let mount = format!("{}:/etc/mysql/conf.d/10-binlog.cnf:ro", cnf.display());
        let env = format!("MARIADB_ROOT_PASSWORD={OWN_MARIADB_PASSWORD}");
        let sid = format!("--server-id={server_id}");
        let dom = format!("--gtid-domain-id={domain}");
        // A fixed host port: Docker picks a new ephemeral one on every
        // restart, and crash tests restart the container.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| SprigError::internal("finding a free port").with_source(e))?
            .port();
        let publish = format!("127.0.0.1:{port}:3306");
        let container = OwnContainer::run(
            "mariadb",
            &["-e", &env, "-p", &publish, "-v", &mount, image, &sid, &dom],
        )?;
        let addr = container.port(3306)?;
        let url = format!("mysql://root:{OWN_MARIADB_PASSWORD}@{addr}");
        let probe = url.clone();
        Retry::new(Duration::from_secs(120))
            .run("throwaway MariaDB accepts connections", || {
                let url = probe.clone();
                async move {
                    let opts = mysql_async::Opts::from_url(&url).map_err(|e| e.to_string())?;
                    let opts = mysql_async::OptsBuilder::from_opts(opts).prefer_socket(false);
                    let c = mysql_async::Conn::new(opts)
                        .await
                        .map_err(|e| e.to_string())?;
                    let _ = c.disconnect().await;
                    Ok::<(), String>(())
                }
            })
            .await?;
        Ok(Self { container, url })
    }
}

#[derive(Debug, Clone)]
enum Held {
    /// A toxic on a proxy.
    Toxic { proxy: String, toxic: String },
    /// A disabled proxy.
    Disabled { proxy: String },
    /// faultd rules in a container.
    Faultd { container: String },
    /// A paused container.
    Paused { container: String },
    /// A skewed clock in a faketime container.
    Skew { container: String },
}

/// [`Faults`] against Docker Compose.
#[derive(Debug)]
pub struct ComposeFaults {
    toxiproxy: Option<Toxiproxy>,
    faultd: Option<Faultd>,
    routes: Mutex<BTreeMap<(Node, Node), String>>,
    held: Mutex<BTreeMap<FaultId, Held>>,
    next: AtomicU64,
}

impl ComposeFaults {
    /// Faults through `toxiproxy` and `faultd`, either optional: a fault
    /// needing a missing one fails.
    #[must_use]
    pub fn new(toxiproxy: Option<Toxiproxy>, faultd: Option<Faultd>) -> Self {
        Self {
            toxiproxy,
            faultd,
            routes: Mutex::new(BTreeMap::new()),
            held: Mutex::new(BTreeMap::new()),
            next: AtomicU64::new(0),
        }
    }

    /// Declares that traffic between `a` and `b` goes through `proxy`, a
    /// toxiproxy route this test created.
    pub fn route(&self, a: &Node, b: &Node, proxy: &str) {
        let mut r = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        r.insert((a.clone(), b.clone()), proxy.to_owned());
        r.insert((b.clone(), a.clone()), proxy.to_owned());
    }

    fn proxy(&self, a: &Node, b: &Node) -> Option<String> {
        self.routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(a.clone(), b.clone()))
            .cloned()
    }

    fn hold(&self, h: Held) -> FaultId {
        let id = FaultId(self.next.fetch_add(1, Ordering::Relaxed) + 1);
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, h);
        id
    }

    fn toxiproxy(&self) -> Result<&Toxiproxy> {
        self.toxiproxy
            .as_ref()
            .ok_or_else(|| SprigError::internal("no toxiproxy configured (SPRIG_TEST_TOXIPROXY)"))
    }

    fn faultd(&self) -> Result<&Faultd> {
        self.faultd
            .as_ref()
            .ok_or_else(|| SprigError::internal("no faultd configured (SPRIG_TEST_FAULTD)"))
    }
}

#[async_trait::async_trait]
impl Faults for ComposeFaults {
    async fn partition(&self, a: &[Node], b: &[Node]) -> Result<FaultId> {
        // Routed pairs: disable their proxies. Container pairs: iptables.
        let mut first = None;
        for x in a {
            for y in b {
                let id = if let Some(p) = self.proxy(x, y) {
                    self.toxiproxy()?.set_enabled(&p, false).await?;
                    self.hold(Held::Disabled { proxy: p })
                } else {
                    let fd = self.faultd()?;
                    ensure_own(x.as_str())?;
                    ensure_own(y.as_str())?;
                    fd.partition(x.as_str(), y.as_str())?;
                    fd.partition(y.as_str(), x.as_str())?;
                    self.hold(Held::Faultd {
                        container: x.0.clone(),
                    });
                    self.hold(Held::Faultd {
                        container: y.0.clone(),
                    })
                };
                first.get_or_insert(id);
            }
        }
        first.ok_or_else(|| SprigError::internal("empty partition"))
    }

    async fn link(&self, a: &Node, b: &Node, fault: LinkFault) -> Result<FaultId> {
        let p = self.proxy(a, b).ok_or_else(|| {
            SprigError::internal(format!("no toxiproxy route between {a} and {b}"))
        })?;
        let tp = self.toxiproxy()?;
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let toxic = Toxic::latency(ms(fault.latency), ms(fault.jitter));
        let name = tp.add_toxic(&p, &toxic).await?;
        let id = self.hold(Held::Toxic {
            proxy: p.clone(),
            toxic: name,
        });
        if fault.loss > 0.0 {
            let name = tp
                .add_toxic(&p, &Toxic::reset_peer(0).toxicity(fault.loss))
                .await?;
            self.hold(Held::Toxic {
                proxy: p,
                toxic: name,
            });
        }
        Ok(id)
    }

    async fn kill(&self, n: &Node) -> Result<()> {
        ensure_own(n.as_str())?;
        docker(&["kill", "--signal", "KILL", n.as_str()])?;
        docker(&["start", n.as_str()])?;
        Ok(())
    }

    async fn pause(&self, n: &Node) -> Result<FaultId> {
        ensure_own(n.as_str())?;
        docker(&["pause", n.as_str()])?;
        Ok(self.hold(Held::Paused {
            container: n.0.clone(),
        }))
    }

    /// Only for containers started with libfaketime
    /// ([`OwnMariaDb::start_with_faketime`]); see [`super::faketime`] for
    /// why faultd can't skew a container's clock.
    async fn skew_clock(&self, n: &Node, offset_ms: i64, rate_ppm: i64) -> Result<FaultId> {
        super::faketime::set(n.as_str(), offset_ms, rate_ppm).await?;
        Ok(self.hold(Held::Skew {
            container: n.0.clone(),
        }))
    }

    async fn heal(&self, id: FaultId) -> Result<()> {
        let h = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        match h {
            None => Ok(()),
            Some(Held::Toxic { proxy, toxic }) => {
                self.toxiproxy()?.remove_toxic(&proxy, &toxic).await
            }
            Some(Held::Disabled { proxy }) => self.toxiproxy()?.set_enabled(&proxy, true).await,
            Some(Held::Faultd { container }) => self.faultd()?.heal(&container),
            Some(Held::Paused { container }) => docker(&["unpause", &container]).map(drop),
            Some(Held::Skew { container }) => super::faketime::set(&container, 0, 0).await,
        }
    }

    async fn heal_all(&self) -> Result<()> {
        let ids: Vec<FaultId> = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        let mut first_err = None;
        for id in ids {
            if let Err(e) = self.heal(id).await {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_own_containers() {
        assert!(ensure_own("sprig-sim-mariadb-1").is_ok());
        assert!(ensure_own("sprigdb-dev-single-1").is_err());
    }

    #[tokio::test]
    async fn container_faults_refuse_shared_servers() {
        let f = ComposeFaults::new(None, None);
        let shared = Node::from("sprigdb-dev-primary-1");
        assert!(f.kill(&shared).await.is_err());
        assert!(f.pause(&shared).await.is_err());
        assert!(f.skew_clock(&shared, 1000, 0).await.is_err());
        assert!(
            f.partition(std::slice::from_ref(&shared), &[Node::from("sprig-sim-x")])
                .await
                .is_err()
        );
        assert!(
            f.link(
                &shared,
                &Node::from("x"),
                LinkFault::latency(Duration::from_millis(5))
            )
            .await
            .is_err()
        );
        f.heal_all().await.unwrap();
    }
}
