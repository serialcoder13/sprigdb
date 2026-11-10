//! Chapter 25's recovery jobs (class `recovery`): `errant_extract` and
//! `node_rebuild` ("Recovery after an outage", steps 2 and 3).
//!
//! ```text
//! errant_extract {cluster, node, range, failover_at_unix_ms}:
//!   dump the node's binlog (sprig_repl) over the diverged GTIDs; row images
//!   and SQL per change (column names from the node); one file per tenant,
//!   sealed under the tenant's `errant extract` key; written to
//!   [recovery] extract_dir; read back, opened and recounted per table
//!   -> progress = ExtractSummary { verified: true, ... }
//! node_rebuild {cluster, node, source?, require_extract}:
//!   refuse the writer, Galera, a node without an agent service; a diverged
//!   former writer needs its verified extract (or require_extract = false,
//!   which keeps its old data directory)
//!   source = the named replica, else the replica with an agent that
//!   replicates with the least lag (never the writer)
//!   ticket rebuilds/<job>, then the target agent rebuilds (see sprig-agent)
//!   attach the restored node to the writer at the backup's position;
//!   done once it replicates and has caught up
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use serde_json::{Value, json};
use sprig_cluster::failover::extract::{
    self, ErrantExtractSpec, ExtractFile, ExtractHeader, ExtractStore, ExtractSummary,
    LocalDirStore,
};
use sprig_cluster::failover::rebuild::{
    self, CANCEL_ROUTE, NodeRebuildSpec, REBUILD_ROUTE, RebuildPhase, RebuildStatus, RebuildTicket,
    StartRebuild,
};
use sprig_cluster::failover::writer::{AgentRecord, MysqldState, WriterLease, writer_lease_key};
use sprig_cluster::record::{ClusterRecord, TopologyKind};
use sprig_cluster::registry::ClusterRegistry;
use sprig_cluster::sql::{NodeEndpoint, ProbeSession, literal};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_controller::dispatch::JobClass;
use sprig_controller::failover::cluster::{
    ClusterFailoverRecord, FormerWriter, record_key, repl_source,
};
use sprig_controller::failover::nodes::{ctl_endpoints, repl_endpoints};
use sprig_meta::{Consistency, ListOpts, MetaStore, Op, Record, Txn};
use sprig_workload::keyring::MasterKeyring;

use crate::runtime::{JobCtx, JobHandler};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn fail(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// What both jobs read.
#[derive(Clone)]
pub struct RecoveryEnv {
    /// The DC tier.
    pub dc: Arc<dyn MetaStore>,
    /// Cluster records.
    pub registry: ClusterRegistry,
}

impl std::fmt::Debug for RecoveryEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryEnv").finish_non_exhaustive()
    }
}

impl RecoveryEnv {
    async fn cluster(&self, id: &sprig_common::ids::ClusterId) -> Result<ClusterRecord> {
        self.registry
            .get(id)
            .await?
            .map(|s| s.body().clone())
            .ok_or_else(|| SprigError::internal(format!("cluster {id} isn't registered")))
    }

    async fn failover_record(
        &self,
        id: &sprig_common::ids::ClusterId,
    ) -> Result<Option<ClusterFailoverRecord>> {
        let key = record_key(self.dc.tier(), id)?;
        Ok(match self.dc.get(&key, Consistency::Linearizable).await? {
            Some(kv) => Some(Record::<ClusterFailoverRecord>::decode(&kv.value)?.body),
            None => None,
        })
    }

    /// The writer: the lease's node, else the record's.
    async fn writer(&self, rec: &ClusterRecord) -> Result<Option<String>> {
        let key = writer_lease_key(self.dc.tier(), &rec.cluster_id)?;
        if let Some(kv) = self.dc.get(&key, Consistency::Linearizable).await? {
            return Ok(Some(Record::<WriterLease>::decode(&kv.value)?.body.node_id));
        }
        Ok(self
            .failover_record(&rec.cluster_id)
            .await?
            .and_then(|r| r.writer))
    }

    /// The agents of the cluster's nodes.
    async fn agents(&self, rec: &ClusterRecord) -> Result<BTreeMap<String, AgentRecord>> {
        let prefix = self.dc.tier().key("agents/")?;
        let page = self.dc.list(&prefix, ListOpts::default()).await?;
        let mut out = BTreeMap::new();
        for kv in page.kvs {
            if let Ok(r) = Record::<AgentRecord>::decode(&kv.value)
                && r.body.cluster == rec.cluster_id.as_str()
            {
                out.insert(r.body.node.clone(), r.body);
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// errant_extract
// ---------------------------------------------------------------------------

/// `errant_extract` (see the module docs).
pub struct ErrantExtract {
    env: RecoveryEnv,
    store: Option<Arc<dyn ExtractStore>>,
    keys: Option<Arc<MasterKeyring>>,
}

impl std::fmt::Debug for ErrantExtract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrantExtract")
            .field("store", &self.store.is_some())
            .field("keys", &self.keys.is_some())
            .finish_non_exhaustive()
    }
}

impl ErrantExtract {
    /// The handler; without a store or keys every job fails with the reason.
    #[must_use]
    pub fn new(
        env: RecoveryEnv,
        store: Option<Arc<dyn ExtractStore>>,
        keys: Option<Arc<MasterKeyring>>,
    ) -> Self {
        Self { env, store, keys }
    }

    /// From `[recovery]`: a local directory and a key file.
    ///
    /// # Errors
    /// The key file can't be read.
    pub fn from_config(
        env: RecoveryEnv,
        cfg: &sprig_common::config::RecoveryConfig,
    ) -> Result<Self> {
        let store = cfg
            .extract_dir
            .as_ref()
            .map(|d| Arc::new(LocalDirStore::new(d)) as Arc<dyn ExtractStore>);
        let keys = cfg
            .extract_key_file
            .as_ref()
            .map(|p| MasterKeyring::from_file(p).map(Arc::new))
            .transpose()?;
        Ok(Self::new(env, store, keys))
    }
}

/// Column names in ordinal order for the tables `tables` names
/// (`schema.table`), read on the node.
async fn column_names(ep: &NodeEndpoint, tables: &[String]) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if tables.is_empty() {
        return out;
    }
    let Ok(mut s) = ProbeSession::connect(ep, Duration::from_secs(5)).await else {
        return out;
    };
    let schemas: Vec<String> = tables
        .iter()
        .filter_map(|t| t.split_once('.').map(|(s, _)| literal(s)))
        .collect();
    let sql = format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA IN ({}) ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
        schemas.join(", ")
    );
    if let Ok(rows) = s.rows(&sql).await {
        for r in rows {
            let get = |i: usize| r.get(i).and_then(|(_, v)| v.clone()).unwrap_or_default();
            let key = format!("{}.{}", get(0), get(1));
            if tables.contains(&key) {
                out.entry(key).or_default().push(get(2));
            }
        }
    }
    s.close().await;
    out
}

#[async_trait]
impl JobHandler for ErrantExtract {
    fn kind(&self) -> &'static str {
        extract::KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Recovery
    }

    #[allow(clippy::too_many_lines)] // the steps in order
    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let spec: ErrantExtractSpec = serde_json::from_value(cx.spec.clone()).map_err(fail)?;
        let store = self.store.as_ref().ok_or(
            "no extract store: set [recovery] extract_dir and extract_key_file on the workers",
        )?;
        let keys = self
            .keys
            .as_ref()
            .ok_or("no extract keys: set [recovery] extract_key_file on the workers")?;
        let rec = self.env.cluster(&spec.cluster).await.map_err(fail)?;
        let node = rec
            .nodes
            .iter()
            .position(|n| n.node.as_str() == spec.node)
            .ok_or_else(|| format!("{} isn't a node of {}", spec.node, spec.cluster))?;
        let repl = repl_endpoints(&rec).map_err(fail)?.swap_remove(node);
        let ctl = ctl_endpoints(&rec).map_err(fail)?.swap_remove(node);
        let applied = self
            .env
            .failover_record(&spec.cluster)
            .await
            .map_err(fail)?
            .and_then(|r| {
                r.history
                    .iter()
                    .rev()
                    .find(|e| e.from == spec.node)
                    .map(|e| e.applied.clone())
            })
            .unwrap_or_default();
        let start = extract::start_position(&spec.range, &applied);
        let t0 = Instant::now();
        // A dump id of its own, in the probes' unallocated range (chapter 10).
        let server_id = 4_200_000_000
            + u32::try_from(
                cx.job_id
                    .to_string()
                    .bytes()
                    .fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)))
                    % 90_000_000,
            )
            .unwrap_or(0);
        let txns = extract::read_diverged(
            &repl.binlog_source().await,
            &spec.range,
            &start,
            server_id,
            Duration::from_secs(600),
        )
        .await
        .map_err(|e| format!("reading {}'s binlog from {start}: {e}", spec.node))?;
        let mut tables: Vec<String> = txns
            .iter()
            .flat_map(|t| {
                t.changes
                    .iter()
                    .map(|c| format!("{}.{}", c.schema, c.table))
            })
            .collect();
        tables.sort();
        tables.dedup();
        let names = column_names(&ctl, &tables).await;
        let rows = extract::rows_of(&txns, &names);
        // One file per tenant.
        let mut by_tenant: BTreeMap<u64, Vec<&extract::ExtractRow>> = BTreeMap::new();
        for r in &rows {
            by_tenant
                .entry(extract::tenant_of(&r.schema))
                .or_default()
                .push(r);
        }
        let dir = format!(
            "{}/{}/{}",
            spec.cluster, spec.node, spec.failover_at_unix_ms
        );
        let mut files = Vec::new();
        let mut expected: BTreeMap<String, u64> = BTreeMap::new();
        for (tenant, rs) in &by_tenant {
            let (version, key) = keys
                .current_for(extract::PURPOSE, TenantId(*tenant))
                .ok_or("the extract keyring holds no key")?;
            let mut plain = Vec::new();
            let mut gtids: Vec<&str> = Vec::new();
            for r in rs {
                plain.extend(serde_json::to_vec(r).map_err(fail)?);
                plain.push(b'\n');
                if r.kind != "statement" {
                    *expected
                        .entry(format!("{}.{}", r.schema, r.table))
                        .or_default() += 1;
                }
                if gtids.last() != Some(&r.gtid.as_str()) {
                    gtids.push(&r.gtid);
                }
            }
            gtids.dedup();
            let header = ExtractHeader {
                tenant: *tenant,
                key_version: version,
                cluster: spec.cluster.to_string(),
                node: spec.node.clone(),
                job_id: cx.job_id.to_string(),
            };
            let sealed = extract::seal(key.bytes(), &header, &plain).map_err(fail)?;
            let location = store
                .put(&format!("{dir}/t{tenant}.extract"), &sealed)
                .await
                .map_err(fail)?;
            files.push(ExtractFile {
                tenant: *tenant,
                location,
                key_version: version,
                transactions: gtids.len() as u64,
                rows: rs.iter().filter(|r| r.kind != "statement").count() as u64,
                bytes: sealed.len() as u64,
                sha256: extract::sha256_hex(&sealed),
            });
        }
        // Verify: read back, open, recount.
        let mut counted: BTreeMap<String, u64> = BTreeMap::new();
        for f in &files {
            let back = store.get(&f.location).await.map_err(fail)?;
            if extract::sha256_hex(&back) != f.sha256 {
                return Err(format!("{} changed after it was written", f.location));
            }
            let key = keys
                .key_for(extract::PURPOSE, TenantId(f.tenant), f.key_version)
                .ok_or("the extract key vanished")?;
            let (_, plain) = extract::open(key.bytes(), &back).map_err(fail)?;
            for (t, n) in extract::count_rows(&plain).map_err(fail)? {
                *counted.entry(t).or_default() += n;
            }
        }
        if counted != expected {
            return Err(format!(
                "the extract doesn't verify: read back {counted:?}, wrote {expected:?}"
            ));
        }
        let summary = ExtractSummary {
            job_id: cx.job_id.to_string(),
            location: store.location(&dir),
            transactions: txns.len() as u64,
            rows: files.iter().map(|f| f.rows).sum(),
            files,
            tables: counted,
            first_commit_unix: txns.iter().map(|t| t.at).min(),
            last_commit_unix: txns.iter().map(|t| t.at).max(),
            verified: true,
        };
        if txns.len() as u64 != spec.range.count() {
            tracing::warn!(job = %cx.job_id, read = txns.len(), range = spec.range.count(), "the binlog held fewer diverged transactions than the range");
        }
        tracing::warn!(job = %cx.job_id, cluster = %spec.cluster, node = %spec.node, transactions = summary.transactions, rows = summary.rows, location = %summary.location, elapsed_ms = t0.elapsed().as_millis(), "errant extract written and verified");
        serde_json::to_value(summary).map_err(fail)
    }
}

// ---------------------------------------------------------------------------
// node_rebuild
// ---------------------------------------------------------------------------

/// `node_rebuild` (see the module docs).
pub struct NodeRebuild {
    env: RecoveryEnv,
    http: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        Full<Bytes>,
    >,
    /// `[tls]`: agents' `https://` services are called over mutual TLS
    /// (decision RC2).
    tls: Option<sprig_cluster::failover::internal_tls::InternalTls>,
}

impl std::fmt::Debug for NodeRebuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRebuild").finish_non_exhaustive()
    }
}

impl NodeRebuild {
    /// The handler.
    #[must_use]
    pub fn new(env: RecoveryEnv) -> Self {
        let http =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http();
        Self {
            env,
            http,
            tls: None,
        }
    }

    /// Calls agents' `https://` services with `tls` (`[tls]`).
    #[must_use]
    pub fn with_tls(mut self, tls: sprig_cluster::failover::internal_tls::InternalTls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sends `req` to `url`: over mutual TLS for `https://`.
    async fn send(
        &self,
        url: &str,
        req: hyper::Request<Full<Bytes>>,
    ) -> Result<hyper::Response<hyper::body::Incoming>> {
        let (https, host, port) = sprig_cluster::failover::internal_tls::split_url(url)?;
        if !https {
            return self
                .http
                .request(req)
                .await
                .map_err(|e| SprigError::internal(format!("{url}: {e}")));
        }
        let tls = self.tls.as_ref().ok_or_else(|| {
            SprigError::internal(format!("{url} needs TLS, and this worker has no [tls]"))
        })?;
        let stream = tls.connect(&host, port).await?;
        let (mut tx, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let (mut parts, body) = req.into_parts();
        parts.uri = parts
            .uri
            .path_and_query()
            .map_or_else(|| "/".to_owned(), ToString::to_string)
            .parse()
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
        parts.headers.insert(
            hyper::header::HOST,
            hyper::header::HeaderValue::from_str(&format!("{host}:{port}"))
                .map_err(|e| SprigError::internal(format!("{url}: {e}")))?,
        );
        tx.send_request(hyper::Request::from_parts(parts, body))
            .await
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))
    }

    async fn call(
        &self,
        method: hyper::Method,
        url: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let req = hyper::Request::builder()
            .method(method)
            .uri(url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.unwrap_or_default())))
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
        let resp = tokio::time::timeout(Duration::from_secs(10), self.send(url, req))
            .await
            .map_err(|_| SprigError::internal(format!("{url}: timed out")))??;
        let code = resp.status();
        let b = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))?
            .to_bytes()
            .to_vec();
        if code.is_success() {
            Ok(b)
        } else {
            Err(SprigError::internal(format!(
                "{url}: {code}: {}",
                String::from_utf8_lossy(&b)
            )))
        }
    }

    /// Replicas fit to stream from: agents with a service, both threads
    /// running; least lag first.
    async fn source(
        &self,
        rec: &ClusterRecord,
        eps: &[NodeEndpoint],
        agents: &BTreeMap<String, AgentRecord>,
        writer: Option<&str>,
        target: &str,
        named: Option<&str>,
    ) -> std::result::Result<(String, String), String> {
        let mut best: Option<(u64, String, String)> = None;
        let mut why = Vec::new();
        for (n, ep) in rec.nodes.iter().zip(eps) {
            let node = n.node.as_str();
            if node == target || Some(node) == writer || named.is_some_and(|x| x != node) {
                continue;
            }
            let Some(url) = agents.get(node).and_then(|a| {
                (a.mysqld != MysqldState::Down)
                    .then(|| a.internal_url.clone())
                    .flatten()
            }) else {
                why.push(format!("{node}: no agent service"));
                continue;
            };
            let Ok(mut s) = ProbeSession::connect(ep, Duration::from_secs(3)).await else {
                why.push(format!("{node}: unreachable"));
                continue;
            };
            let rows = s.rows("SHOW SLAVE STATUS").await.unwrap_or_default();
            s.close().await;
            let Some(r) = rows.first() else {
                why.push(format!("{node}: doesn't replicate"));
                continue;
            };
            let get = |k: &str| {
                r.iter()
                    .find(|(c, _)| c.eq_ignore_ascii_case(k))
                    .and_then(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            if get("Slave_IO_Running") != "Yes" || get("Slave_SQL_Running") != "Yes" {
                why.push(format!("{node}: replication threads not running"));
                continue;
            }
            let lag = get("Seconds_Behind_Master").parse().unwrap_or(u64::MAX);
            if best
                .as_ref()
                .is_none_or(|(l, b, _)| (lag, node) < (*l, b.as_str()))
            {
                best = Some((lag, node.to_owned(), url));
            }
        }
        best.map(|(_, n, u)| (n, u)).ok_or_else(|| {
            format!(
                "no replica to stream from (never the writer): {}",
                why.join("; ")
            )
        })
    }
}

#[async_trait]
impl JobHandler for NodeRebuild {
    fn kind(&self) -> &'static str {
        rebuild::KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Recovery
    }

    #[allow(clippy::too_many_lines)] // the steps in order
    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let spec: NodeRebuildSpec = serde_json::from_value(cx.spec.clone()).map_err(fail)?;
        let rate = cx
            .spec
            .get("rate_mb_per_second")
            .and_then(Value::as_u64)
            .unwrap_or(200);
        let rec = self.env.cluster(&spec.cluster).await.map_err(fail)?;
        if rec.topology != TopologyKind::PrimaryReplica {
            return Err(format!(
                "{} is {}: only primary + replicas nodes are rebuilt (Galera nodes rejoin by SST)",
                spec.cluster,
                rec.topology.as_str()
            ));
        }
        let idx = rec
            .nodes
            .iter()
            .position(|n| n.node.as_str() == spec.node)
            .ok_or_else(|| format!("{} isn't a node of {}", spec.node, spec.cluster))?;
        let eps = ctl_endpoints(&rec).map_err(fail)?;
        let writer = self.env.writer(&rec).await.map_err(fail)?;
        if writer.as_deref() == Some(spec.node.as_str()) {
            return Err(format!("{} is the writer: never rebuilt", spec.node));
        }
        let writer = writer.ok_or("the cluster has no writer to attach to")?;
        let writer_ep = rec
            .nodes
            .iter()
            .zip(&eps)
            .find(|(n, _)| n.node.as_str() == writer)
            .map(|(_, e)| e.clone())
            .ok_or_else(|| format!("the writer {writer} isn't in the record"))?;
        // A diverged former writer is rebuilt only after its extract is
        // verified, unless the operator keeps the old data instead.
        let fo = self
            .env
            .failover_record(&spec.cluster)
            .await
            .map_err(fail)?;
        let diverged = fo.as_ref().and_then(|r| {
            r.history.iter().rev().find(|e| {
                e.from == spec.node
                    && matches!(
                        e.former,
                        Some(FormerWriter::Diverged | FormerWriter::Rebuilding)
                    )
            })
        });
        let verified = diverged
            .and_then(|e| e.errant.as_ref())
            .and_then(|r| r.extract.as_ref())
            .is_some_and(|x| x.verified);
        if diverged.is_some() && spec.require_extract && !verified {
            return Err(format!(
                "{} has diverged transactions without a verified errant extract; \
                 run `sprigctl cluster extract {} {}` first, or rebuild with --no-extract \
                 (its old data directory is kept)",
                spec.node, spec.cluster, spec.node
            ));
        }
        let keep_old = diverged.is_some() && !verified;
        let agents = self.env.agents(&rec).await.map_err(fail)?;
        let target_url = agents
            .get(&spec.node)
            .and_then(|a| a.internal_url.clone())
            .ok_or_else(|| {
                format!(
                    "{}'s agent has no internal service ([agent] internal_addr)",
                    spec.node
                )
            })?;
        let (source, source_url) = self
            .source(
                &rec,
                &eps,
                &agents,
                Some(writer.as_str()),
                &spec.node,
                spec.source.as_deref(),
            )
            .await?;
        // The ticket and its token (a new pair per attempt).
        let token = rebuild::new_token().map_err(fail)?;
        let ticket = RebuildTicket {
            job_id: cx.job_id.to_string(),
            attempt: cx.attempt().await,
            cluster: spec.cluster.to_string(),
            target_node: spec.node.clone(),
            source_node: source.clone(),
            source_url,
            token_sha256: rebuild::sha256_hex(&token),
            rate_mb_per_second: rate,
            keep_old,
            expires_unix_ms: now_ms() + 12 * 3600 * 1000,
        };
        let tkey = rebuild::ticket_key(self.env.dc.tier(), &ticket.job_id).map_err(fail)?;
        self.env
            .dc
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key: tkey.clone(),
                    value: Record::new(ticket.clone()).encode().map_err(fail)?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await
            .map_err(fail)?;
        tracing::warn!(job = %cx.job_id, cluster = %spec.cluster, node = %spec.node, %source, %writer, rate_mb_per_second = rate, keep_old, "node_rebuild: streaming a backup from a replica");
        let t0 = Instant::now();
        let result = self
            .drive(cx, &spec, &target_url, &token, &source, rate)
            .await;
        let st = match result {
            Ok(st) => st,
            Err(e) => {
                let _ = self
                    .env
                    .dc
                    .txn(Txn {
                        when: Vec::new(),
                        then: vec![Op::Delete(tkey)],
                        otherwise: Vec::new(),
                    })
                    .await;
                return Err(e);
            }
        };
        let _ = self
            .env
            .dc
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Delete(tkey)],
                otherwise: Vec::new(),
            })
            .await;
        let gtid = st.gtid.clone().ok_or("the agent reported no position")?;
        // Attach at the backup's position; read_only stays on.
        let target_ep = &eps[idx];
        let to = repl_source(&rec, &writer_ep).await.map_err(fail)?;
        rebuild::attach(target_ep, &to, &gtid)
            .await
            .map_err(|e| format!("attaching {} to {writer}: {e}", spec.node))?;
        let t_attach = Instant::now();
        let caught_up = loop {
            let a = rebuild::attached(target_ep).await.map_err(fail)?;
            if let Some(e) = &a.error
                && (a.io == "No" || a.sql == "No")
            {
                return Err(format!(
                    "{} doesn't replicate after the rebuild: {e}",
                    spec.node
                ));
            }
            let wpos = sprig_cluster::failover::primary_replica::binlog_pos(&writer_ep)
                .await
                .unwrap_or_default();
            if a.io == "Yes" && a.sql == "Yes" && contains(&a.applied, &wpos) {
                break a.applied;
            }
            if t_attach.elapsed() > Duration::from_secs(900) {
                return Err(format!(
                    "{} hasn't caught up 15 min after the rebuild (applied {}, writer {wpos})",
                    spec.node, a.applied
                ));
            }
            if cx.cancel.is_cancelled() {
                return Err("cancelled while catching up".into());
            }
            let _ = cx
                .progress(json!({"phase": "catching_up", "source": source, "gtid": gtid, "applied": a.applied, "bytes": st.bytes}))
                .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        let secs = t0.elapsed().as_secs_f64();
        #[allow(clippy::cast_precision_loss)] // a rate for people
        let mb_s = st.bytes as f64 / 1_048_576.0 / secs.max(0.001);
        tracing::warn!(job = %cx.job_id, node = %spec.node, %source, bytes = st.bytes, seconds = secs, downtime_ms = ?st.downtime_ms, applied = %caught_up, "node rebuilt and caught up with the writer");
        Ok(json!({
            "phase": "done",
            "source": source,
            "writer": writer,
            "gtid": gtid,
            "applied": caught_up,
            "bytes": st.bytes,
            "seconds": secs,
            "mb_per_second": mb_s,
            "downtime_ms": st.downtime_ms,
            "old_datadir": st.old_datadir,
        }))
    }
}

impl NodeRebuild {
    /// Starts the target agent's rebuild and follows it to `started`.
    async fn drive(
        &self,
        cx: &JobCtx,
        spec: &NodeRebuildSpec,
        target_url: &str,
        token: &str,
        source: &str,
        rate: u64,
    ) -> std::result::Result<RebuildStatus, String> {
        let base = target_url.trim_end_matches('/');
        let start = StartRebuild {
            job_id: cx.job_id.to_string(),
            token: token.to_owned(),
        };
        self.call(
            hyper::Method::POST,
            &format!("{base}{REBUILD_ROUTE}"),
            Some(serde_json::to_vec(&start).map_err(fail)?),
        )
        .await
        .map_err(|e| format!("{}'s agent refused the rebuild: {e}", spec.node))?;
        let status_url = format!("{base}{REBUILD_ROUTE}?job={}", cx.job_id);
        let mut misses = 0;
        loop {
            if cx.cancel.is_cancelled() {
                let _ = self
                    .call(
                        hyper::Method::POST,
                        &format!("{base}{CANCEL_ROUTE}?job={}", cx.job_id),
                        None,
                    )
                    .await;
                return Err("cancelled".into());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            let st: RebuildStatus = match self.call(hyper::Method::GET, &status_url, None).await {
                Ok(b) => serde_json::from_slice(&b).map_err(fail)?,
                // The node's agent restarts with its mysqld in some set-ups;
                // a minute without answers fails the job.
                Err(e) => {
                    misses += 1;
                    if misses > 60 {
                        return Err(format!("{}'s agent stopped answering: {e}", spec.node));
                    }
                    continue;
                }
            };
            misses = 0;
            let _ = cx
                .progress(json!({
                    "phase": st.phase,
                    "source": source,
                    "bytes": st.bytes,
                    "rate_mb_per_second": rate,
                    "elapsed_ms": st.elapsed_ms,
                }))
                .await;
            match st.phase {
                RebuildPhase::Started => return Ok(st),
                RebuildPhase::Failed | RebuildPhase::Cancelled => {
                    return Err(format!(
                        "{}'s agent: {}",
                        spec.node,
                        st.error.unwrap_or_else(|| "failed".into())
                    ));
                }
                _ => {}
            }
        }
    }
}

/// Whether `applied` reached `writer`'s position in every domain of it.
fn contains(applied: &str, writer: &str) -> bool {
    let a: sprig_binlog::GtidPos = applied.parse().unwrap_or_default();
    let w: sprig_binlog::GtidPos = writer.parse().unwrap_or_default();
    w.to_string()
        .split(',')
        .filter_map(|g| g.parse::<sprig_binlog::Gtid>().ok())
        .all(|g| a.get(g.domain).is_some_and(|x| x.seq >= g.seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caught_up_means_every_domain_of_the_writer_is_reached() {
        assert!(contains("1-102-50", "1-102-50"));
        assert!(contains("0-11-3,1-102-51,2-9-1", "0-11-3,1-102-50"));
        assert!(!contains("1-102-49", "1-102-50"));
        assert!(!contains("1-102-60", "0-11-3,1-102-50"), "every domain");
        assert!(contains("", ""));
    }
}
