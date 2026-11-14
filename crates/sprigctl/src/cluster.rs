//! Building a `RegisterCluster` request from `sprigctl cluster register`'s
//! flags and its `-f` file (also for external
//! registration).
//!
//! The file is TOML, with these fields:
//!
//! ```toml
//! dc = "fra1"
//! name = "fra1-ext1"
//! topology = "primary_replica"   # galera | primary_replica | single | external
//! credentials_ref = "file:/etc/sprigdb/creds/fra1-ext1.toml"  # [sprig_ctl], [sprig_repl]
//!
//! [endpoints]                    # or [[nodes]] with node, addr, zone
//! writer = "db-primary.fra1.internal:3306"
//! readers = ["db-r1.fra1.internal:3306", "db-r2.fra1.internal:3306"]
//!
//! [capacity]
//! memory_gb = 256
//!
//! [declared]
//! failover = "provider"
//! rpo_seconds = 0
//! ```
//!
//! Flags override the file. It is TOML rather than YAML, like
//! `sprigdb.toml` and `--declare`.

use serde::Deserialize;
use sprig_cluster::record::{Capacity, Declared, NodeSpec, SecretRefs, TopologyKind};
use sprig_common::ids::{DcId, NodeId};
use sprig_controller::admin::types::RegisterClusterRequest;
use sprig_provision::credentials::refs_for;

/// The `-f` file.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterFile {
    dc: Option<DcId>,
    name: Option<String>,
    topology: Option<String>,
    credentials_ref: Option<String>,
    secrets: Option<SecretRefs>,
    endpoints: Option<Endpoints>,
    nodes: Option<Vec<NodeSpec>>,
    capacity: Option<Capacity>,
    declared: Option<Declared>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoints {
    writer: String,
    #[serde(default)]
    readers: Vec<String>,
}

/// The flags of `register`.
#[derive(Debug, Default, Clone)]
pub(crate) struct RegisterFlags {
    /// The `-f` file's text.
    pub(crate) file: Option<String>,
    /// `--declare`'s text.
    pub(crate) declare: Option<String>,
    /// The positional name.
    pub(crate) name: Option<String>,
    /// `--dc`.
    pub(crate) dc: Option<String>,
    /// `--topology`.
    pub(crate) topology: Option<String>,
    /// `--node`, repeated: `[id=]host:port`.
    pub(crate) nodes: Vec<String>,
    /// `--credentials`: one reference for both accounts.
    pub(crate) credentials: Option<String>,
    /// `--ctl-secret`.
    pub(crate) ctl_secret: Option<String>,
    /// `--repl-secret`.
    pub(crate) repl_secret: Option<String>,
    /// `--memory-gb`.
    pub(crate) memory_gb: Option<u64>,
    /// `--storage-gb`.
    pub(crate) storage_gb: Option<u64>,
    /// `--dry-run`.
    pub(crate) dry_run: bool,
}

/// A topology name: the file's spellings and the record's.
///
/// # Errors
/// For anything else.
pub(crate) fn topology(s: &str) -> Result<TopologyKind, String> {
    match s {
        "galera" => Ok(TopologyKind::Galera),
        "primary_replica" | "replication" => Ok(TopologyKind::PrimaryReplica),
        "single" | "single_node" => Ok(TopologyKind::SingleNode),
        "external" => Ok(TopologyKind::External),
        other => Err(format!(
            "topology {other:?}: expected galera, primary_replica, single or external"
        )),
    }
}

fn node(i: usize, spec: &str) -> Result<NodeSpec, String> {
    let (id, addr) = match spec.split_once('=') {
        Some((id, addr)) => (id.to_owned(), addr),
        None => (format!("n{}", i + 1), spec),
    };
    Ok(NodeSpec {
        node: NodeId::new(id.as_str()).map_err(|e| format!("node {spec:?}: {e}"))?,
        addr: addr.to_owned(),
        zone: None,
    })
}

/// `sprigctl cluster set-nodes`: the record's nodes after the
/// changes `specs` asks for (`[id=]host:port`, as `register --node`): a
/// node named there gets the new address (its zone kept), a new id is
/// added at the end, and with `only` the list is exactly `specs`, in that
/// order. `remove` drops nodes by id. The answer is `:setNodes`' body.
///
/// # Errors
/// A message for a malformed spec, an unknown node to remove, or an empty
/// result.
pub(crate) fn set_nodes(
    current: &[NodeSpec],
    specs: &[String],
    remove: &[String],
    only: bool,
) -> Result<Vec<NodeSpec>, String> {
    let given: Vec<NodeSpec> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| node(i, s))
        .collect::<Result<_, _>>()?;
    let mut out: Vec<NodeSpec> = if only {
        given
            .iter()
            .map(|g| NodeSpec {
                zone: current
                    .iter()
                    .find(|c| c.node == g.node)
                    .and_then(|c| c.zone.clone()),
                ..g.clone()
            })
            .collect()
    } else {
        let mut v = current.to_vec();
        for g in given {
            match v.iter_mut().find(|c| c.node == g.node) {
                Some(c) => c.addr = g.addr,
                None => v.push(g),
            }
        }
        v
    };
    for r in remove {
        let before = out.len();
        out.retain(|n| n.node.as_str() != r);
        if out.len() == before {
            return Err(format!("no node {r:?} to remove"));
        }
    }
    if out.is_empty() {
        return Err("a cluster needs at least one node".to_owned());
    }
    Ok(out)
}

/// Merges the file and the flags into a request.
///
/// # Errors
/// A message for a missing or malformed field.
pub(crate) fn build(flags: &RegisterFlags) -> Result<RegisterClusterRequest, String> {
    let file: ClusterFile = match &flags.file {
        Some(text) => toml::from_str(text).map_err(|e| format!("cluster file: {}", e.message()))?,
        None => ClusterFile::default(),
    };
    let name = flags
        .name
        .clone()
        .or(file.name)
        .ok_or("a cluster name is required")?;
    let dc = match flags.dc.as_deref() {
        Some(d) => Some(DcId::new(d).map_err(|e| format!("--dc {d:?}: {e}"))?),
        None => file.dc,
    };
    let topology = topology(
        flags
            .topology
            .as_deref()
            .or(file.topology.as_deref())
            .ok_or("a topology is required (--topology)")?,
    )?;
    let nodes = if flags.nodes.is_empty() {
        match (file.nodes, file.endpoints) {
            (Some(n), None) => n,
            (None, Some(e)) => std::iter::once(e.writer)
                .chain(e.readers)
                .enumerate()
                .map(|(i, a)| node(i, &a))
                .collect::<Result<_, _>>()?,
            (Some(_), Some(_)) => return Err("give [endpoints] or [[nodes]], not both".into()),
            (None, None) => return Err("nodes are required (--node host:port)".into()),
        }
    } else {
        flags
            .nodes
            .iter()
            .enumerate()
            .map(|(i, s)| node(i, s))
            .collect::<Result<_, _>>()?
    };
    let shared = flags.credentials.as_ref().or(file.credentials_ref.as_ref());
    let mut secrets = match (shared, file.secrets) {
        (Some(r), _) => Some(refs_for(r).map_err(|e| e.message)?),
        (None, s) => s,
    };
    if flags.ctl_secret.is_some() || flags.repl_secret.is_some() {
        let base = secrets.clone();
        let pick = |flag: &Option<String>, from: Option<String>, what: &str| {
            flag.clone().or(from).ok_or(format!(
                "--{what}-secret is required with the other secret flag"
            ))
        };
        secrets = Some(SecretRefs {
            ctl: pick(
                &flags.ctl_secret,
                base.as_ref().map(|b| b.ctl.clone()),
                "ctl",
            )?,
            repl: pick(&flags.repl_secret, base.map(|b| b.repl), "repl")?,
        });
    }
    let secrets = secrets.ok_or("credentials are required (--credentials file:<path>)")?;
    let mut capacity = file.capacity.unwrap_or_default();
    capacity.memory_gb = flags.memory_gb.or(capacity.memory_gb);
    capacity.storage_gb = flags.storage_gb.or(capacity.storage_gb);
    let declared = match &flags.declare {
        Some(text) => Some(declared(text)?),
        None => file.declared,
    };
    Ok(RegisterClusterRequest {
        dc,
        name,
        topology,
        provisioner: sprig_cluster::record::ProvisionerKind::External,
        nodes,
        secrets,
        capacity,
        declared,
        dry_run: flags.dry_run,
    })
}

/// `--declare`: a `[declared]` table, or its fields at the top level.
fn declared(text: &str) -> Result<Declared, String> {
    let mut t: toml::Table = text
        .parse()
        .map_err(|e: toml::de::Error| format!("--declare: {}", e.message()))?;
    let v = t.remove("declared").unwrap_or(toml::Value::Table(t));
    v.try_into()
        .map_err(|e: toml::de::Error| format!("--declare: {}", e.message()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_alone() {
        let r = build(&RegisterFlags {
            name: Some("dc-a-galera".into()),
            dc: Some("dc-a".into()),
            topology: Some("galera".into()),
            nodes: vec![
                "127.0.0.1:13321".into(),
                "g2=127.0.0.1:13322".into(),
                "127.0.0.1:13323".into(),
            ],
            credentials: Some("file:/c.toml".into()),
            memory_gb: Some(16),
            ..RegisterFlags::default()
        })
        .unwrap();
        assert_eq!(r.topology, TopologyKind::Galera);
        let ids: Vec<&str> = r.nodes.iter().map(|n| n.node.as_str()).collect();
        assert_eq!(ids, ["n1", "g2", "n3"]);
        assert_eq!(r.secrets.ctl, "file:/c.toml#sprig_ctl");
        assert_eq!(r.capacity.memory_gb, Some(16));
        assert!(!r.dry_run);
    }

    #[test]
    fn the_chapter_27_file_with_overrides() {
        let file = r#"
            dc = "fra1"
            name = "fra1-ext1"
            topology = "primary_replica"
            credentials_ref = "file:/etc/sprigdb/creds/fra1-ext1.toml"
            [endpoints]
            writer = "db-primary.fra1.internal:3306"
            readers = ["db-r1.fra1.internal:3306", "db-r2.fra1.internal:3306"]
            [declared]
            failover = "provider"
            rpo_seconds = 0
            replica_dcs = ["fra1"]
        "#;
        let r = build(&RegisterFlags {
            file: Some(file.into()),
            repl_secret: Some("env:REPL".into()),
            dry_run: true,
            ..RegisterFlags::default()
        })
        .unwrap();
        assert_eq!(r.dc.unwrap().as_str(), "fra1");
        assert_eq!(r.nodes.len(), 3);
        assert_eq!(r.nodes[2].addr, "db-r2.fra1.internal:3306");
        assert_eq!(
            r.secrets.ctl,
            "file:/etc/sprigdb/creds/fra1-ext1.toml#sprig_ctl"
        );
        assert_eq!(r.secrets.repl, "env:REPL");
        assert_eq!(r.declared.unwrap().rpo_seconds, Some(0));
        assert!(r.dry_run);
        let d = build(&RegisterFlags {
            file: Some(file.into()),
            declare: Some("rpo_seconds = 5\ntrust = \"asserted\"\n".into()),
            ..RegisterFlags::default()
        })
        .unwrap()
        .declared
        .unwrap();
        assert_eq!(d.rpo_seconds, Some(5));
        assert!(d.failover.is_none(), "--declare replaces the file's table");
    }

    #[test]
    fn missing_and_malformed_fields() {
        let base = RegisterFlags {
            name: Some("x".into()),
            topology: Some("single".into()),
            nodes: vec!["db:3306".into()],
            credentials: Some("file:/c.toml".into()),
            ..RegisterFlags::default()
        };
        assert!(build(&base).is_ok());
        let cases: Vec<(RegisterFlags, &str)> = vec![
            (
                RegisterFlags {
                    name: None,
                    ..base.clone()
                },
                "a cluster name is required",
            ),
            (
                RegisterFlags {
                    topology: Some("mesh".into()),
                    ..base.clone()
                },
                "topology \"mesh\": expected galera, primary_replica, single or external",
            ),
            (
                RegisterFlags {
                    nodes: vec![],
                    ..base.clone()
                },
                "nodes are required (--node host:port)",
            ),
            (
                RegisterFlags {
                    credentials: None,
                    ..base.clone()
                },
                "credentials are required (--credentials file:<path>)",
            ),
            (
                RegisterFlags {
                    credentials: None,
                    ctl_secret: Some("env:A".into()),
                    ..base.clone()
                },
                "--repl-secret is required with the other secret flag",
            ),
            (
                RegisterFlags {
                    file: Some("nme = 1".into()),
                    ..base.clone()
                },
                "cluster file: unknown field `nme`, expected one of `dc`, `name`, `topology`, `credentials_ref`, `secrets`, `endpoints`, `nodes`, `capacity`, `declared`",
            ),
        ];
        for (flags, want) in cases {
            assert_eq!(build(&flags).unwrap_err(), want);
        }
    }

    /// A bundle's `mariadb-1:3306` moves to VM 1's private
    /// address; the replica keeps its address and zone.
    #[test]
    fn set_nodes_changes_named_addresses_only() {
        let cur = vec![
            NodeSpec {
                node: NodeId::new("n1").unwrap(),
                addr: "mariadb-1:3306".into(),
                zone: Some("z1".into()),
            },
            NodeSpec {
                node: NodeId::new("n2").unwrap(),
                addr: "10.0.0.6:3306".into(),
                zone: Some("z2".into()),
            },
        ];
        let got = set_nodes(&cur, &["n1=10.0.0.5:3306".into()], &[], false).unwrap();
        assert_eq!(got[0].addr, "10.0.0.5:3306");
        assert_eq!(got[0].zone.as_deref(), Some("z1"));
        assert_eq!(got[1], cur[1]);
        // A new id is added; --remove drops one.
        let got = set_nodes(&cur, &["n3=10.0.0.7:3306".into()], &["n2".into()], false).unwrap();
        let ids: Vec<&str> = got.iter().map(|n| n.node.as_str()).collect();
        assert_eq!(ids, ["n1", "n3"]);
        // --only: exactly the list, in order, zones kept by id.
        let got = set_nodes(
            &cur,
            &["n2=10.0.0.6:3306".into(), "n1=10.0.0.5:3306".into()],
            &[],
            true,
        )
        .unwrap();
        let ids: Vec<&str> = got.iter().map(|n| n.node.as_str()).collect();
        assert_eq!(ids, ["n2", "n1"]);
        assert_eq!(got[1].zone.as_deref(), Some("z1"));
        assert_eq!(
            set_nodes(&cur, &[], &["n9".into()], false).unwrap_err(),
            "no node \"n9\" to remove"
        );
        assert!(set_nodes(&cur, &[], &["n1".into(), "n2".into()], false).is_err());
        assert!(set_nodes(&cur, &["bad id=x:1".into()], &[], false).is_err());
    }
}
