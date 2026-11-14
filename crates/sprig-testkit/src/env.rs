//! Test environments: which data centers, MariaDB clusters and etcd tiers a
//! test can use, and where they listen.
//!
//! An [`Env`] comes from one of two places:
//!
//! - a TOML file, named by `SPRIG_TEST_ENV_FILE` or loaded with
//!   [`Env::load`]; `deploy/compose/env.3dc.toml` describes the three-DC
//!   Compose layout and is built in as [`Env::three_dc`];
//! - the `SPRIG_TEST_*` variables of [`crate::skip`], which describe one data
//!   center (the single-DC Compose file) with up to three clusters.
//!
//! ```toml
//! name = "3dc"
//! global_etcd = ["http://127.0.0.1:34180"]
//!
//! [[dcs]]
//! id = "mum1"
//! etcd = ["http://127.0.0.1:34179"]
//!
//! [[dcs.clusters]]
//! id = "mum1-c1"
//! topology = { kind = "primary_replicas", replicas = 2, semisync = true }
//! writer = "mysql://root:sprig-dev-root@127.0.0.1:34106"
//! readers = ["mysql://root:sprig-dev-root@127.0.0.1:34107"]
//!
//! [[links]]
//! a = "mum1"
//! b = "fra1"
//! one_way_ms = 30
//! ```
//!
//! An `Env` trait (gateway URLs per protocol, metrics, faults, test
//! tenants) grows on top of this once the gateway serves clients.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::skip;

/// The data center the `SPRIG_TEST_*` variables describe, unless
/// `SPRIG_TEST_DC` names another.
pub const DEFAULT_DC: &str = "dc-a";

/// A MariaDB cluster's topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Topology {
    /// Three-node Galera. Every node takes writes; SprigDB picks one writer.
    Galera3,
    /// A primary with asynchronous or semi-synchronous replicas.
    PrimaryReplicas {
        /// Number of replicas.
        replicas: u8,
        /// Semi-synchronous, waiting `AFTER_SYNC`.
        #[serde(default)]
        semisync: bool,
    },
    /// One node.
    Single,
    /// Registered, not managed: its shape is the operator's.
    External,
}

impl Topology {
    /// Number of nodes, or `None` for an external cluster.
    #[must_use]
    pub fn nodes(self) -> Option<usize> {
        match self {
            Topology::Galera3 => Some(3),
            Topology::PrimaryReplicas { replicas, .. } => Some(1 + usize::from(replicas)),
            Topology::Single => Some(1),
            Topology::External => None,
        }
    }

    /// The name used in TOML, such as `primary_replicas`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Topology::Galera3 => "galera3",
            Topology::PrimaryReplicas { .. } => "primary_replicas",
            Topology::Single => "single",
            Topology::External => "external",
        }
    }
}

/// One MariaDB cluster and its endpoints, as URLs such as
/// `mysql://root:pw@127.0.0.1:13301`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterEnv {
    /// The cluster id, such as `mum1-c1`.
    pub id: ClusterId,
    /// Its topology.
    pub topology: Topology,
    /// Where writes go: the primary, the single node, or the first Galera node.
    pub writer: String,
    /// The other nodes: replicas, or the other Galera nodes.
    #[serde(default)]
    pub readers: Vec<String>,
}

impl ClusterEnv {
    /// Every endpoint, writer first.
    pub fn endpoints(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.writer.as_str()).chain(self.readers.iter().map(String::as_str))
    }
}

/// One data center.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DcEnv {
    /// The DC id, such as `mum1`.
    pub id: DcId,
    /// Client URLs of the DC's etcd tier.
    #[serde(default)]
    pub etcd: Vec<String>,
    /// The DC's subnet inside the Compose network, for fault injection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subnet: Option<String>,
    /// MariaDB clusters in the DC.
    #[serde(default)]
    pub clusters: Vec<ClusterEnv>,
}

/// Simulated network latency between two DCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    /// One end.
    pub a: DcId,
    /// The other end.
    pub b: DcId,
    /// Delay added in each direction, in milliseconds.
    pub one_way_ms: u32,
}

impl Link {
    /// The added round-trip time: twice the one-way delay.
    #[must_use]
    pub fn rtt_ms(&self) -> u32 {
        self.one_way_ms * 2
    }
}

/// A test environment: data centers, their clusters and etcd tiers, and the
/// global etcd tier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Env {
    /// A label for messages, such as `3dc`.
    #[serde(default)]
    pub name: String,
    /// Client URLs of the global etcd tier; empty in single-etcd mode.
    #[serde(default)]
    pub global_etcd: Vec<String>,
    /// The data centers.
    #[serde(default)]
    pub dcs: Vec<DcEnv>,
    /// Latency between pairs of DCs.
    #[serde(default)]
    pub links: Vec<Link>,
}

fn invalid(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, msg)
}

impl Env {
    /// Parses and validates a TOML description.
    ///
    /// # Errors
    /// `ER_SPRIG_CONFIG_INVALID` if it doesn't parse or doesn't validate.
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let env: Env = toml::from_str(s)
            .map_err(|e| invalid(format!("test environment: {e}")).with_source(e))?;
        env.validate()?;
        Ok(env)
    }

    /// Loads a TOML file. A relative path that doesn't exist from the
    /// current directory is tried from the workspace root, because
    /// `cargo test` runs each crate's tests in that crate's directory.
    ///
    /// # Errors
    /// If the file can't be read, parsed or validated.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = resolve(path.as_ref());
        let text = std::fs::read_to_string(&path).map_err(|e| {
            invalid(format!("reading test environment {}", path.display())).with_source(e)
        })?;
        Self::from_toml_str(&text)
            .map_err(|e| invalid(format!("{}: {}", path.display(), e.message)))
    }

    /// The three-DC Compose layout (`deploy/compose/compose.3dc.yaml`), as
    /// described by `deploy/compose/env.3dc.toml`.
    ///
    /// # Panics
    /// Never in practice: a unit test checks the built-in file.
    #[must_use]
    pub fn three_dc() -> Self {
        Self::from_toml_str(THREE_DC_TOML).expect("deploy/compose/env.3dc.toml is valid")
    }

    /// The environment of this test run: the file named by
    /// `SPRIG_TEST_ENV_FILE` if set, else the `SPRIG_TEST_*` variables.
    /// With neither, the result is empty ([`Env::is_empty`]).
    ///
    /// # Errors
    /// If the file or a variable is invalid.
    pub fn from_env() -> Result<Self> {
        match skip::var(skip::ENV_FILE) {
            Some(file) => Self::load(file),
            None => Self::from_lookup(skip::var),
        }
    }

    /// Builds one data center from `SPRIG_TEST_*`-style variables, read
    /// through `get` (so tests needn't touch the process environment):
    /// `SPRIG_TEST_DC` (default `dc-a`), [`skip::ETCD`] and one cluster per
    /// `SPRIG_TEST_MARIADB_*` variable, named `<dc>-single`, `<dc>-repl`
    /// and `<dc>-galera`.
    ///
    /// # Errors
    /// If a value is invalid, such as a bad DC id.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let dc_name = get("SPRIG_TEST_DC").unwrap_or_else(|| DEFAULT_DC.to_owned());
        let dc_id =
            DcId::new(dc_name.clone()).map_err(|e| invalid(format!("SPRIG_TEST_DC: {e}")))?;
        let mut dc = DcEnv {
            id: dc_id,
            etcd: get(skip::ETCD).map(|v| skip::urls(&v)).unwrap_or_default(),
            subnet: None,
            clusters: Vec::new(),
        };
        let shapes = [
            (skip::MARIADB_SINGLE, "single"),
            (skip::MARIADB_REPLICATION, "repl"),
            (skip::MARIADB_GALERA, "galera"),
        ];
        for (var, suffix) in shapes {
            let Some(value) = get(var) else { continue };
            let mut urls = skip::urls(&value).into_iter();
            let writer = urls
                .next()
                .ok_or_else(|| invalid(format!("{var} holds no URL")))?;
            let readers: Vec<String> = urls.collect();
            let topology = match suffix {
                "single" => Topology::Single,
                "repl" => Topology::PrimaryReplicas {
                    replicas: u8::try_from(readers.len())
                        .map_err(|_| invalid(format!("{var}: too many URLs")))?,
                    semisync: true,
                },
                _ => Topology::Galera3,
            };
            let id = ClusterId::new(format!("{dc_name}-{suffix}"))
                .map_err(|e| invalid(format!("{var}: {e}")))?;
            dc.clusters.push(ClusterEnv {
                id,
                topology,
                writer,
                readers,
            });
        }
        let env = if dc.etcd.is_empty() && dc.clusters.is_empty() {
            Env::default()
        } else {
            Env {
                name: "vars".to_owned(),
                dcs: vec![dc],
                ..Env::default()
            }
        };
        env.validate()?;
        Ok(env)
    }

    /// Whether the environment describes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dcs.is_empty() && self.global_etcd.is_empty()
    }

    /// The data center `id`.
    #[must_use]
    pub fn dc(&self, id: &str) -> Option<&DcEnv> {
        self.dcs.iter().find(|d| d.id.as_str() == id)
    }

    /// Every cluster, with its data center.
    pub fn clusters(&self) -> impl Iterator<Item = (&DcEnv, &ClusterEnv)> {
        self.dcs
            .iter()
            .flat_map(|d| d.clusters.iter().map(move |c| (d, c)))
    }

    /// The cluster `id`.
    #[must_use]
    pub fn cluster(&self, id: &str) -> Option<&ClusterEnv> {
        self.clusters()
            .map(|(_, c)| c)
            .find(|c| c.id.as_str() == id)
    }

    /// The first cluster whose topology satisfies `f`, such as
    /// `env.cluster_where(|t| matches!(t, Topology::Galera3))`.
    #[must_use]
    pub fn cluster_where(&self, f: impl Fn(Topology) -> bool) -> Option<&ClusterEnv> {
        self.clusters().map(|(_, c)| c).find(|c| f(c.topology))
    }

    /// The link between `a` and `b`, in either order.
    #[must_use]
    pub fn link(&self, a: &str, b: &str) -> Option<&Link> {
        self.links.iter().find(|l| {
            (l.a.as_str() == a && l.b.as_str() == b) || (l.a.as_str() == b && l.b.as_str() == a)
        })
    }

    /// Checks ids are unique, endpoints fit the topology and links name
    /// known DCs.
    ///
    /// # Errors
    /// `ER_SPRIG_CONFIG_INVALID`, naming the first problem.
    pub fn validate(&self) -> Result<()> {
        let mut dcs = HashSet::new();
        let mut clusters = HashSet::new();
        for dc in &self.dcs {
            if !dcs.insert(dc.id.as_str()) {
                return Err(invalid(format!("data center {} appears twice", dc.id)));
            }
            for c in &dc.clusters {
                if !clusters.insert(c.id.as_str()) {
                    return Err(invalid(format!("cluster {} appears twice", c.id)));
                }
                if c.writer.trim().is_empty() {
                    return Err(invalid(format!("cluster {} has no writer", c.id)));
                }
                let n = 1 + c.readers.len();
                if let Some(max) = c.topology.nodes()
                    && n > max
                {
                    return Err(invalid(format!(
                        "cluster {} lists {n} endpoints but a {} cluster has {max} nodes",
                        c.id,
                        c.topology.name()
                    )));
                }
            }
        }
        for l in &self.links {
            for end in [&l.a, &l.b] {
                if !dcs.contains(end.as_str()) {
                    return Err(invalid(format!("link names unknown data center {end}")));
                }
            }
            if l.a == l.b {
                return Err(invalid(format!("link from {} to itself", l.a)));
            }
        }
        Ok(())
    }
}

/// The built-in description of the three-DC Compose layout.
const THREE_DC_TOML: &str = include_str!("../../../deploy/compose/env.3dc.toml");

/// The workspace root, from this crate's manifest directory.
#[must_use]
pub fn workspace_root() -> PathBuf {
    let here = Path::new(env!("CARGO_MANIFEST_DIR"));
    here.parent()
        .and_then(Path::parent)
        .unwrap_or(here)
        .to_path_buf()
}

fn resolve(p: &Path) -> PathBuf {
    if p.is_absolute() || p.exists() {
        p.to_path_buf()
    } else {
        workspace_root().join(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn three_dc_file_is_valid_and_complete() {
        let env = Env::three_dc();
        assert_eq!(env.dcs.len(), 3);
        assert_eq!(env.global_etcd.len(), 3);
        for dc in &env.dcs {
            assert_eq!(dc.etcd.len(), 1, "{}", dc.id);
            assert_eq!(dc.clusters.len(), 1, "{}", dc.id);
        }
        let mum1 = env.cluster("mum1-c1").unwrap();
        assert!(matches!(
            mum1.topology,
            Topology::PrimaryReplicas {
                replicas: 2,
                semisync: true
            }
        ));
        assert_eq!(mum1.endpoints().count(), 3);
        assert_eq!(env.cluster("fra1-c1").unwrap().topology, Topology::Galera3);
        assert_eq!(env.cluster("iad1-c1").unwrap().topology, Topology::Single);
        assert_eq!(env.links.len(), 3);
        assert_eq!(env.link("iad1", "mum1").unwrap().one_way_ms, 80);
    }

    #[test]
    fn from_lookup_builds_one_dc() {
        let env = Env::from_lookup(lookup(&[
            ("SPRIG_TEST_ETCD", "http://127.0.0.1:2379"),
            (
                "SPRIG_TEST_MARIADB_SINGLE",
                "mysql://root:x@127.0.0.1:13301",
            ),
            (
                "SPRIG_TEST_MARIADB_REPLICATION",
                "mysql://r@h:13311, mysql://r@h:13312,mysql://r@h:13313",
            ),
            ("SPRIG_TEST_MARIADB_GALERA", "mysql://r@h:13321"),
        ]))
        .unwrap();
        let dc = env.dc(DEFAULT_DC).unwrap();
        assert_eq!(dc.etcd, vec!["http://127.0.0.1:2379"]);
        assert_eq!(dc.clusters.len(), 3);
        let repl = env
            .cluster_where(|t| matches!(t, Topology::PrimaryReplicas { .. }))
            .unwrap();
        assert_eq!(repl.id.as_str(), "dc-a-repl");
        assert_eq!(repl.writer, "mysql://r@h:13311");
        assert_eq!(
            repl.topology,
            Topology::PrimaryReplicas {
                replicas: 2,
                semisync: true
            }
        );
        assert_eq!(env.cluster("dc-a-galera").unwrap().readers.len(), 0);
    }

    #[test]
    fn from_lookup_without_vars_is_empty() {
        assert!(Env::from_lookup(lookup(&[])).unwrap().is_empty());
    }

    #[test]
    fn from_lookup_rejects_a_bad_dc() {
        let e = Env::from_lookup(lookup(&[
            ("SPRIG_TEST_DC", "Bad DC"),
            ("SPRIG_TEST_ETCD", "http://e:2379"),
        ]))
        .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
    }

    #[test]
    fn validation_catches_mistakes() {
        let too_many = r#"
            [[dcs]]
            id = "dc-a"
            [[dcs.clusters]]
            id = "c1"
            topology = { kind = "single" }
            writer = "mysql://h:1"
            readers = ["mysql://h:2"]
        "#;
        let e = Env::from_toml_str(too_many).unwrap_err();
        assert!(e.message.contains("2 endpoints"), "{}", e.message);

        let bad_link = r#"
            [[dcs]]
            id = "dc-a"
            [[links]]
            a = "dc-a"
            b = "dc-z"
            one_way_ms = 5
        "#;
        assert!(Env::from_toml_str(bad_link).is_err());

        let dup = r#"
            [[dcs]]
            id = "dc-a"
            [[dcs]]
            id = "dc-a"
        "#;
        assert!(Env::from_toml_str(dup).is_err());

        let unknown_field = "nmae = \"x\"";
        assert!(Env::from_toml_str(unknown_field).is_err());
    }

    #[test]
    fn round_trips_through_toml() {
        let env = Env::three_dc();
        let text = toml::to_string(&env).unwrap();
        assert_eq!(Env::from_toml_str(&text).unwrap(), env);
    }

    #[test]
    fn load_resolves_from_the_workspace_root() {
        let env = Env::load("deploy/compose/env.3dc.toml").unwrap();
        assert_eq!(env, Env::three_dc());
        assert!(Env::load("no/such/file.toml").is_err());
    }
}
