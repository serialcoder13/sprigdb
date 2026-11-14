//! What sinks keep in etcd:
//!
//! - `/sprig/dc/<dc>/sinks/<tenant>/<sink>`: the sink index the
//!   controller's `sinks` loop reads to start one `sink_task` job per task
//!   (the job runtime's lease replaces the chapter's per-task lease key, as
//!   for stream sequencers; the task epoch in `sink_tasks` fences);
//! - `/sprig/g/platform-connections/<name>`: a platform connection's kind,
//!   options, tags and secret references (never values);
//! - `/sprig/g/platform-connections/<name>/grants/<tenant>`: a grant.
//!
//! Platform secrets are files under `sink.platform_secret_dir`, read only
//! by sink workers ([`read_secret_file`]).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sprig_common::{Result, SprigCode};
use sprig_meta::{ListOpts, MetaStore, Op, Record, Txn, Versioned};
use zeroize::Zeroizing;

use crate::model::{ConnKind, err};

/// The job kind of sink tasks.
pub const JOB_KIND: &str = "sink_task";

/// One sink in the DC's index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkIndex {
    /// The tenant.
    pub tenant: u64,
    /// The sink.
    pub sink: u64,
    /// Its tasks, numbered 0..n.
    pub tasks: u32,
    /// Bumps with the definition, so the loop can tell a change.
    pub definition_ver: u64,
    /// A fan-in sink's grouping: the controller's `sinks`
    /// loop counts the definition's member instances and sets `tasks`
    /// from it ([`fanin_tasks`]); each task reads the members
    /// [`fanin_task_of`] gives it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_in: Option<FanInGroups>,
}

impl Versioned for SinkIndex {
    const V: u32 = 1;
}

/// How a fan-in sink's member instances are split into tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanInGroups {
    /// The definition's declaring tenant.
    pub def_tenant: u64,
    /// The definition's object id there.
    pub def_object: u64,
    /// `stream` or `queue` (the `inherit-instances/` entries' kind).
    pub kind: String,
    /// Partitions of each instance (1 for a queue).
    pub partitions: u32,
    /// `sink.fanin_instances_per_task`.
    pub per_task: u32,
}

/// The tasks a fan-in sink needs for `instances` member instances of
/// `partitions` partitions each, at most `per_task` instance partitions
/// per task: at least 1.
#[must_use]
pub fn fanin_tasks(instances: u64, partitions: u32, per_task: u32) -> u32 {
    let total = instances.saturating_mul(u64::from(partitions.max(1)));
    u32::try_from(total.div_ceil(u64::from(per_task.max(1))))
        .unwrap_or(u32::MAX)
        .max(1)
}

/// The task (of `tasks`) that reads member `member`'s instance: a mix of
/// the member's id, so consecutive ids spread evenly and a member stays
/// with its task while the count holds.
#[must_use]
pub fn fanin_task_of(member: u64, tasks: u32) -> u32 {
    if tasks <= 1 {
        return 0;
    }
    // splitmix64's finalizer.
    let mut z = member.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    u32::try_from(z % u64::from(tasks)).unwrap_or(0)
}

/// Whether a regrouping from `current` to `wanted` tasks is worth the
/// restart it costs: growth always; shrinking only to half or less, so a
/// member dropped near a boundary doesn't restart every task.
#[must_use]
pub fn fanin_regroup(current: u32, wanted: u32) -> bool {
    wanted > current || wanted.saturating_mul(2) <= current
}

fn put(key: sprig_meta::Key, value: Vec<u8>) -> Txn {
    Txn {
        when: Vec::new(),
        then: vec![Op::Put {
            key,
            value,
            lease: None,
        }],
        otherwise: Vec::new(),
    }
}

/// Writes the index entry of a sink.
///
/// # Errors
/// The DC tier's.
pub async fn index_put(dc: &Arc<dyn MetaStore>, ix: &SinkIndex) -> Result<()> {
    let key = dc.tier().key(&format!("sinks/{}/{}", ix.tenant, ix.sink))?;
    dc.txn(put(key, Record::new(ix.clone()).encode()?)).await?;
    Ok(())
}

/// The index entry of a sink, if any.
///
/// # Errors
/// The DC tier's.
pub async fn index_get(
    dc: &Arc<dyn MetaStore>,
    tenant: u64,
    sink: u64,
) -> Result<Option<SinkIndex>> {
    let key = dc.tier().key(&format!("sinks/{tenant}/{sink}"))?;
    Ok(dc
        .get(&key, sprig_meta::Consistency::Serializable)
        .await?
        .and_then(|kv| Record::<SinkIndex>::decode(&kv.value).ok().map(|r| r.body)))
}

/// Sets a sink's task count in its index entry (a fan-in sink regrouped
/// by the controller), if the entry still has definition version
/// `definition_ver` and nothing wrote it in between; `false` otherwise.
///
/// # Errors
/// The DC tier's.
pub async fn index_set_tasks(
    dc: &Arc<dyn MetaStore>,
    tenant: u64,
    sink: u64,
    definition_ver: u64,
    tasks: u32,
) -> Result<bool> {
    let key = dc.tier().key(&format!("sinks/{tenant}/{sink}"))?;
    let Some(kv) = dc.get(&key, sprig_meta::Consistency::Linearizable).await? else {
        return Ok(false);
    };
    let Ok(mut rec) = Record::<SinkIndex>::decode(&kv.value) else {
        return Ok(false);
    };
    if rec.body.definition_ver != definition_ver || !rec.writable() {
        return Ok(false);
    }
    rec.body.tasks = tasks;
    let r = dc
        .txn(Txn {
            when: vec![sprig_meta::Cmp::ModRev(key.clone(), kv.mod_revision)],
            then: vec![Op::Put {
                key,
                value: rec.encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(r.succeeded)
}

/// Removes the index entry of a sink (its tasks stop).
///
/// # Errors
/// The DC tier's.
pub async fn index_delete(dc: &Arc<dyn MetaStore>, tenant: u64, sink: u64) -> Result<()> {
    let key = dc.tier().key(&format!("sinks/{tenant}/{sink}"))?;
    dc.txn(Txn {
        when: Vec::new(),
        then: vec![Op::Delete(key)],
        otherwise: Vec::new(),
    })
    .await?;
    Ok(())
}

/// Every sink in the DC's index.
///
/// # Errors
/// The DC tier's.
pub async fn index_list(dc: &Arc<dyn MetaStore>) -> Result<Vec<SinkIndex>> {
    let prefix = dc.tier().key("sinks/")?;
    let page = dc
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<SinkIndex>::decode(&kv.value).ok().map(|r| r.body))
        .collect())
}

/// Who may see what of a platform connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// Name, kind, description, tags and options not hidden.
    #[default]
    Full,
    /// Name and kind only.
    NameOnly,
}

/// A platform connection's registration (no secret values).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlatformConnection {
    /// Its name.
    pub name: String,
    /// Its type.
    pub kind: ConnKind,
    /// Options (endpoints, TLS, auth).
    pub options: Map<String, Value>,
    /// Options tenants never see.
    pub hidden: Vec<String>,
    /// Residency tags.
    pub tags: Map<String, Value>,
    /// The operator's description.
    pub description: String,
    /// What tenants see.
    pub visibility: Visibility,
    /// Secret references: name → `file:<path under sink.platform_secret_dir>`.
    pub secrets: BTreeMap<String, String>,
    /// Previous references, usable during a switch.
    pub previous: BTreeMap<String, String>,
    /// The hosts the operator vetted (`host:port`).
    pub hosts: Vec<String>,
}

impl Versioned for PlatformConnection {
    const V: u32 = 1;
}

/// A grant of a platform connection to a tenant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformGrant {
    /// The tenant.
    pub tenant: u64,
    /// Its descendants too.
    pub subtree: bool,
    /// The operator who granted it.
    pub granted_by: String,
    /// When (ms since the epoch).
    pub at_ms: u64,
}

impl Versioned for PlatformGrant {
    const V: u32 = 1;
}

impl PlatformConnection {
    /// What a tenant administrator sees: options minus hidden ones (none
    /// for `name_only`), never secrets or their paths.
    #[must_use]
    pub fn tenant_view(&self) -> Value {
        let mut m = Map::new();
        m.insert("name".into(), Value::from(self.name.clone()));
        m.insert("kind".into(), Value::from(self.kind.as_str()));
        if self.visibility == Visibility::Full {
            m.insert("description".into(), Value::from(self.description.clone()));
            m.insert("tags".into(), Value::Object(self.tags.clone()));
            let opts: Map<String, Value> = self
                .options
                .iter()
                .filter(|(k, _)| !self.hidden.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            m.insert("options".into(), Value::Object(opts));
        }
        Value::Object(m)
    }
}

/// Registers or replaces a platform connection.
///
/// # Errors
/// The global tier's; 39218 for a bad secret reference.
pub async fn platform_put(global: &Arc<dyn MetaStore>, c: &PlatformConnection) -> Result<()> {
    for r in c.secrets.values().chain(c.previous.values()) {
        secret_path(Path::new("/"), r)?;
    }
    let key = global
        .tier()
        .key(&format!("platform-connections/{}", c.name))?;
    global
        .txn(put(key, Record::new(c.clone()).encode()?))
        .await?;
    Ok(())
}

/// A platform connection.
///
/// # Errors
/// The global tier's.
pub async fn platform_get(
    global: &Arc<dyn MetaStore>,
    name: &str,
) -> Result<Option<PlatformConnection>> {
    let key = global.tier().key(&format!("platform-connections/{name}"))?;
    let kv = global
        .get(&key, sprig_meta::Consistency::Linearizable)
        .await?;
    Ok(kv.and_then(|kv| {
        Record::<PlatformConnection>::decode(&kv.value)
            .ok()
            .map(|r| r.body)
    }))
}

/// Every platform connection, with its grants.
///
/// # Errors
/// The global tier's.
pub async fn platform_list(
    global: &Arc<dyn MetaStore>,
) -> Result<Vec<(PlatformConnection, Vec<PlatformGrant>)>> {
    let prefix = global.tier().key("platform-connections/")?;
    let page = global
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    let mut conns: BTreeMap<String, (Option<PlatformConnection>, Vec<PlatformGrant>)> =
        BTreeMap::new();
    let root = prefix.as_str().to_owned();
    for kv in &page.kvs {
        let rel = kv.key.as_str().strip_prefix(&root).unwrap_or("");
        let mut parts = rel.split('/');
        let name = parts.next().unwrap_or("").to_owned();
        match (parts.next(), parts.next()) {
            (None, _) => {
                if let Ok(r) = Record::<PlatformConnection>::decode(&kv.value) {
                    conns.entry(name).or_default().0 = Some(r.body);
                }
            }
            (Some("grants"), Some(_)) => {
                if let Ok(r) = Record::<PlatformGrant>::decode(&kv.value) {
                    conns.entry(name).or_default().1.push(r.body);
                }
            }
            _ => {}
        }
    }
    Ok(conns
        .into_values()
        .filter_map(|(c, g)| c.map(|c| (c, g)))
        .collect())
}

/// Grants or revokes a platform connection for a tenant.
///
/// # Errors
/// The global tier's; 39210 for an unknown connection.
pub async fn platform_grant(
    global: &Arc<dyn MetaStore>,
    name: &str,
    grant: Option<PlatformGrant>,
    tenant: u64,
) -> Result<()> {
    if platform_get(global, name).await?.is_none() {
        return Err(err(
            SprigCode::SinkNotFound,
            format!("no platform connection {name}"),
        ));
    }
    let key = global
        .tier()
        .key(&format!("platform-connections/{name}/grants/{tenant}"))?;
    let op = match grant {
        Some(g) => Op::Put {
            key,
            value: Record::new(g).encode()?,
            lease: None,
        },
        None => Op::Delete(key),
    };
    global
        .txn(Txn {
            when: Vec::new(),
            then: vec![op],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Whether `tenant` (with its ancestors, nearest first) holds a grant of
/// `name`: its own, or an ancestor's with `subtree`.
///
/// # Errors
/// The global tier's.
pub async fn platform_granted(
    global: &Arc<dyn MetaStore>,
    name: &str,
    tenant: u64,
    ancestors: &[u64],
) -> Result<bool> {
    let all = platform_list(global).await?;
    let Some((_, grants)) = all.into_iter().find(|(c, _)| c.name == name) else {
        return Ok(false);
    };
    Ok(grants
        .iter()
        .any(|g| g.tenant == tenant || (g.subtree && ancestors.contains(&g.tenant))))
}

/// The path a `file:<rel>` reference names under `dir`; refuses `..`,
/// absolute paths and other schemes.
///
/// # Errors
/// 39218.
pub fn secret_path(dir: &Path, reference: &str) -> Result<PathBuf> {
    let rel = reference.strip_prefix("file:").ok_or_else(|| {
        crate::model::bad_option(format!(
            "a secret reference must be file:<path>, not {reference}"
        ))
    })?;
    let p = Path::new(rel);
    if rel.is_empty()
        || p.is_absolute()
        || p.components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(crate::model::bad_option(format!(
            "secret reference {reference} must stay inside sink.platform_secret_dir"
        )));
    }
    Ok(dir.join(p))
}

/// Reads a platform secret file (trailing newline trimmed), refusing links
/// that leave `dir`.
///
/// # Errors
/// 39234 when it can't be read.
pub fn read_secret_file(dir: &Path, reference: &str) -> Result<Zeroizing<String>> {
    let p = secret_path(dir, reference)?;
    let unavailable = |why: String| {
        let mut e = err(SprigCode::SinkPlatformSecretUnavailable, why);
        e.retryable = true;
        e
    };
    let real = std::fs::canonicalize(&p).map_err(|e| unavailable(format!("{reference}: {e}")))?;
    let root =
        std::fs::canonicalize(dir).map_err(|e| unavailable(format!("{}: {e}", dir.display())))?;
    if !real.starts_with(&root) {
        return Err(unavailable(format!(
            "{reference} leaves the secret directory"
        )));
    }
    let s = Zeroizing::new(
        std::fs::read_to_string(&real).map_err(|e| unavailable(format!("{reference}: {e}")))?,
    );
    Ok(Zeroizing::new(s.trim_end_matches(['\n', '\r']).to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_paths_stay_inside() {
        let d = Path::new("/etc/sprigdb/platform-secrets");
        assert_eq!(
            secret_path(d, "file:kafka_main/password").unwrap(),
            d.join("kafka_main/password")
        );
        for bad in [
            "kafka/password",
            "file:../x",
            "file:/etc/passwd",
            "file:",
            "file:a/../../b",
        ] {
            assert!(secret_path(d, bad).is_err(), "{bad}");
        }
        let tmp = std::env::temp_dir().join(format!("r9sinks-sec-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("k")).unwrap();
        std::fs::write(tmp.join("k/password"), "s3cr3t\n").unwrap();
        assert_eq!(
            read_secret_file(&tmp, "file:k/password").unwrap().as_str(),
            "s3cr3t"
        );
        let e = read_secret_file(&tmp, "file:k/missing").unwrap_err();
        assert_eq!(
            e.kind,
            sprig_common::ErrorKind::Sprig(SprigCode::SinkPlatformSecretUnavailable)
        );
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn tenants_see_what_the_operator_allows() {
        let mut c = PlatformConnection {
            name: "kafka_main".into(),
            kind: ConnKind::Kafka,
            options: [
                ("bootstrap".to_owned(), Value::from("k1:9093")),
                ("security".to_owned(), Value::from("sasl_ssl")),
            ]
            .into_iter()
            .collect(),
            hidden: vec!["bootstrap".into()],
            tags: [("jurisdiction".to_owned(), Value::from("EU"))]
                .into_iter()
                .collect(),
            description: "Platform Kafka, EU".into(),
            visibility: Visibility::Full,
            secrets: [("password".to_owned(), "file:kafka_main/password".to_owned())]
                .into_iter()
                .collect(),
            previous: BTreeMap::new(),
            hosts: vec!["k1:9093".into()],
        };
        let v = c.tenant_view().to_string();
        assert!(v.contains("Platform Kafka") && v.contains("sasl_ssl"));
        assert!(!v.contains("k1:9093") && !v.contains("file:") && !v.contains("password"));
        c.visibility = Visibility::NameOnly;
        assert_eq!(c.tenant_view().as_object().unwrap().len(), 2);
    }

    /// Fan-in tasks by instance partitions, members spread
    /// evenly, and an index entry written before grouping still reads.
    #[test]
    fn fan_in_groups() {
        assert_eq!(fanin_tasks(0, 4, 256), 1);
        assert_eq!(fanin_tasks(64, 4, 256), 1);
        assert_eq!(fanin_tasks(65, 4, 256), 2);
        assert_eq!(fanin_tasks(10_000, 1, 256), 40);
        assert_eq!(fanin_tasks(3, 1, 1), 3);
        assert_eq!(fanin_tasks(5, 0, 0), 5);
        let mut per = [0u32; 8];
        for m in 43_306_000_000..43_306_008_000u64 {
            per[fanin_task_of(m, 8) as usize] += 1;
        }
        assert!(per.iter().all(|n| (900..=1100).contains(n)), "{per:?}");
        assert!((0..100).all(|m| fanin_task_of(m, 1) == 0));
        assert!(fanin_regroup(1, 2) && !fanin_regroup(4, 3) && fanin_regroup(4, 2));
        let old = br#"{"v":1,"tenant":1,"sink":2,"tasks":1,"definition_ver":3}"#;
        let ix = Record::<SinkIndex>::decode(old).unwrap().body;
        assert!(ix.fan_in.is_none() && ix.tasks == 1);
        let ix = SinkIndex {
            tenant: 1,
            sink: 2,
            tasks: 1,
            definition_ver: 3,
            fan_in: None,
        };
        let back = Record::<SinkIndex>::decode(&Record::new(ix.clone()).encode().unwrap())
            .unwrap()
            .body;
        assert_eq!(back, ix);
    }
}
