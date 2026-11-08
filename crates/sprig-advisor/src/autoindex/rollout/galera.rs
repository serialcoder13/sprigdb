//! Galera builds until the mover's copy-and-swap exists (M0.6). See the
//! module docs of [`rollout`](super) and [`GaleraSwitch`](super::GaleraSwitch).
//!
//! **Rolling RSU** ([`build_rsu`]), node by node, the writer last:
//!
//! ```text
//! before each node: the cluster Primary, at least min_cluster_size (and never
//!   fewer than it started with) nodes, every node Synced; no user DDL on the
//!   table on any node
//! node already has the index: skip it
//! mark the node out of service (NodeMarker: no reads, writer weight 0)
//! on the node: SET SESSION sql_log_bin = 0, lock_wait_timeout, wsrep_OSU_method = RSU
//!   ALTER TABLE ... ADD INDEX IF NOT EXISTS x (...), ALGORITHM=INPLACE, LOCK=NONE
//!   watched: user DDL on any node, a client queue behind the lock wait, a peer
//!   not Synced, the cluster not Primary or smaller → KILL QUERY, abort
//! SET SESSION wsrep_OSU_method = TOI; wait for Synced, max(resync_floor, 2 × build)
//! unmark
//! after the last node: every node has x with the same parts
//! ```
//!
//! `sql_log_bin = 0` because an RSU statement in the binary log would be a
//! GTID of this node alone, diverging the nodes' binary logs (MariaDB
//! refuses it under `wsrep_mode = DISALLOW_LOCAL_GTID`); the cost is that
//! asynchronous replicas of a Galera node don't get the index (none in R0).
//! An index on some nodes only, after an abort, is harmless for row-based
//! write sets; the next attempt skips the nodes that have it.
//!
//! **TOI** ([`build_toi`]) for tables up to `toi_max_bytes`: one
//! `ALTER ... ALGORITHM=INPLACE, LOCK=NONE` on the writer in total order,
//! which every node applies at the same point; the cluster's commits wait
//! behind it, so it is kept to small tables. A TOI statement is never
//! killed once sent: it may already be running on the other nodes.

use std::time::{Duration, Instant};

use sprig_cluster::sql::{NodeEndpoint, ProbeError};
use sprig_common::names::quote;

use super::node::{BuildCheck, Node, Stop, galera_unfit, judge_threads, ms, watched};
use super::{
    AbortReason, BuildOutcome, BuildReport, ClusterView, NodeBuild, Nodes, RolloutSettings,
    ShardTable, add_index_clause,
};
use crate::autoindex::model::IndexDef;
use crate::autoindex::validate::NodeMarker;
use crate::autoindex::validate::galera::degraded;

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Watches every node while one builds.
struct GWatch {
    mons: Vec<Node>,
    /// The node building (index into `mons`), RSU only.
    building: Option<usize>,
    table: ShardTable,
    ours: u64,
    started: bool,
    queue_kill_after: Duration,
    min_size: u32,
    /// The writer's column signature when the build started.
    signature: String,
    writer: usize,
}

#[async_trait::async_trait]
impl BuildCheck for GWatch {
    async fn check(&mut self) -> Option<(Stop, String)> {
        for (i, m) in self.mons.iter_mut().enumerate() {
            let me = self.building == Some(i);
            match m.threads().await {
                Ok(th) => {
                    let ours = if me { self.ours } else { 0 };
                    let mut started = if me { self.started } else { true };
                    let v =
                        judge_threads(&th, ours, &self.table, &mut started, self.queue_kill_after);
                    if me {
                        self.started = started;
                    }
                    if let Some(s) = v {
                        return Some((s.0, format!("{}: {}", m.name, s.1)));
                    }
                }
                Err(e) => return Some((Stop::Degraded, format!("{} unreachable: {e}", m.name))),
            }
            match m.wsrep().await {
                Ok(s) => {
                    // The building node is desynced while its RSU statement runs.
                    let why = if me {
                        degraded(&s, self.min_size, false)
                    } else {
                        galera_unfit(&s, self.min_size)
                    };
                    if let Some(why) = why {
                        return Some((Stop::Degraded, format!("{}: {why}", m.name)));
                    }
                }
                Err(e) => return Some((Stop::Degraded, format!("{} unreachable: {e}", m.name))),
            }
        }
        None
    }
}

impl GWatch {
    async fn close(self) {
        for m in self.mons {
            m.close().await;
        }
    }

    /// Every node fit (Synced, Primary, size); no user DDL anywhere, and
    /// none ran since the build started (a total-order DDL that finished
    /// between two polls shows only in the table's columns).
    async fn all_fit(&mut self) -> Option<(Stop, String)> {
        let b = self.building.take();
        let r = self.check().await;
        self.building = b;
        if r.is_some() {
            return r;
        }
        let t = self.table.clone();
        match self.mons[self.writer].columns_signature(&t).await {
            Ok(sig) if sig == self.signature => None,
            Ok(_) => Some((
                Stop::UserDdl,
                format!(
                    "the columns of {}.{} changed during the build",
                    t.schema, t.table
                ),
            )),
            Err(e) => Some((Stop::Degraded, format!("writer: {e}"))),
        }
    }

    async fn wait_synced(&mut self, i: usize, deadline: Duration) -> Option<u64> {
        let started = Instant::now();
        while started.elapsed() <= deadline {
            if let Ok(s) = self.mons[i].wsrep().await
                && s.local.eq_ignore_ascii_case("Synced")
                && s.ready
            {
                return Some(ms(started.elapsed()));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

async fn connect_all(eps: &[NodeEndpoint]) -> Result<Vec<Node>, (String, ProbeError)> {
    let mut out = Vec::new();
    for ep in eps {
        match Node::connect(ep, PROBE_TIMEOUT).await {
            Ok(n) => out.push(n),
            Err(e) => {
                for n in out {
                    n.close().await;
                }
                return Err((ep.node.to_string(), e));
            }
        }
    }
    Ok(out)
}

fn stop_reason(s: Stop) -> AbortReason {
    match s {
        Stop::UserDdl => AbortReason::UserDdl,
        Stop::ReplicaLag => AbortReason::ReplicaLag,
        Stop::Degraded | Stop::LockQueue => AbortReason::Degraded,
    }
}

fn aborted(reason: AbortReason, detail: String) -> BuildOutcome {
    BuildOutcome::Aborted { reason, detail }
}

fn alter_sql(t: &ShardTable, def: &IndexDef) -> String {
    format!(
        "ALTER TABLE {}.{} {}, ALGORITHM=INPLACE, LOCK=NONE",
        quote(&t.schema),
        quote(&t.table),
        add_index_clause(def)
    )
}

fn report(t: &ShardTable, method: &str) -> BuildReport {
    BuildReport {
        shard: t.shard.clone(),
        method: method.into(),
        already_present: false,
        build_ms: 0,
        lock_retries: 0,
        replicas_ms: None,
        max_replica_lag_s: None,
        nodes: Vec::new(),
    }
}

/// Opens the monitors and checks the cluster before a build.
async fn prepare(
    view: &ClusterView,
    t: &ShardTable,
    s: &RolloutSettings,
) -> Result<(GWatch, Vec<NodeEndpoint>, usize), BuildOutcome> {
    let Nodes::Galera { nodes, writer } = &view.nodes else {
        return Err(BuildOutcome::Failed {
            error: "not a Galera cluster".into(),
        });
    };
    let mons = connect_all(nodes)
        .await
        .map_err(|(n, e)| aborted(AbortReason::Degraded, format!("{n} unreachable: {e}")))?;
    let size = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
    let mut w = GWatch {
        mons,
        building: None,
        table: t.clone(),
        ours: 0,
        started: false,
        queue_kill_after: s.queue_kill_after,
        min_size: s.galera.min_cluster_size.min(size).max(1),
        signature: String::new(),
        writer: 0,
    };
    let wi = nodes.iter().position(|n| &n.node == writer).unwrap_or(0);
    match w.mons[wi].table_exists(t).await {
        Ok(true) => {}
        Ok(false) => {
            w.close().await;
            return Err(aborted(
                AbortReason::Degraded,
                format!("table {}.{} not found", t.schema, t.table),
            ));
        }
        Err(e) => {
            w.close().await;
            return Err(aborted(AbortReason::Degraded, format!("writer: {e}")));
        }
    }
    w.writer = wi;
    match w.mons[wi].columns_signature(t).await {
        Ok(sig) => w.signature = sig,
        Err(e) => {
            w.close().await;
            return Err(aborted(AbortReason::Degraded, format!("writer: {e}")));
        }
    }
    if let Some((stop, why)) = w.all_fit().await {
        w.close().await;
        return Err(aborted(stop_reason(stop), why));
    }
    Ok((w, nodes.clone(), wi))
}

/// Every node has `def` with its parts: `None`, else the nodes without.
async fn missing_on(w: &mut GWatch, t: &ShardTable, def: &IndexDef) -> Vec<String> {
    let name = def.name();
    let mut out = Vec::new();
    for m in &mut w.mons {
        match m.indexes(t).await {
            Ok(have) if have.get(&name).is_some_and(|i| i.parts == def.parts) => {}
            _ => out.push(m.name.clone()),
        }
    }
    out
}

/// Builds by rolling RSU, one node at a time, the writer last.
#[allow(clippy::too_many_lines)] // one node after another, each step checked
pub async fn build_rsu(
    view: &ClusterView,
    t: &ShardTable,
    def: &IndexDef,
    s: &RolloutSettings,
    marker: &dyn NodeMarker,
) -> BuildOutcome {
    let (mut w, eps, wi) = match prepare(view, t, s).await {
        Ok(x) => x,
        Err(o) => return o,
    };
    let name = def.name();
    let mut rep = report(t, "galera_rsu");
    let order: Vec<usize> = (0..eps.len())
        .filter(|&i| i != wi)
        .chain(std::iter::once(wi))
        .collect();
    let sql = alter_sql(t, def);
    let started_all = Instant::now();
    for i in order {
        let ep = &eps[i];
        match w.mons[i].indexes(t).await {
            Ok(have) => match have.get(&name) {
                Some(x) if x.parts == def.parts => {
                    rep.nodes.push(NodeBuild {
                        node: ep.node.to_string(),
                        build_ms: 0,
                        resync_ms: None,
                        skipped: true,
                    });
                    continue;
                }
                Some(_) => {
                    w.close().await;
                    return BuildOutcome::Failed {
                        error: format!("{name} on {} has another definition (drift)", ep.node),
                    };
                }
                None => {}
            },
            Err(e) => {
                w.close().await;
                return aborted(AbortReason::Degraded, format!("{}: {e}", ep.node));
            }
        }
        let mut tries = 0;
        loop {
            tries += 1;
            if let Some((stop, why)) = w.all_fit().await {
                w.close().await;
                return aborted(stop_reason(stop), why);
            }
            if let Err(e) = marker.mark_validating(&ep.node).await {
                w.close().await;
                return aborted(
                    AbortReason::Degraded,
                    format!("could not take {} out of service: {e}", ep.node),
                );
            }
            let out = rsu_one(&mut w, i, ep, &sql, s).await;
            if let Err(e) = marker.unmark(&ep.node).await {
                tracing::error!(node = %ep.node, error = %e, "could not put the node back in service");
            }
            match out {
                RsuStep::Built(nb) => {
                    rep.nodes.push(nb);
                    break;
                }
                RsuStep::LockWait => {
                    rep.lock_retries += 1;
                    if let Some((Stop::UserDdl, why)) = w.all_fit().await {
                        w.close().await;
                        return aborted(AbortReason::UserDdl, why);
                    }
                    if tries >= s.lock_retries {
                        w.close().await;
                        return BuildOutcome::LockTimeout { tries };
                    }
                    tokio::time::sleep(s.lock_retry_pause).await;
                }
                RsuStep::Stop(o) => {
                    w.close().await;
                    return o;
                }
            }
        }
    }
    rep.build_ms = ms(started_all.elapsed());
    if let Some((Stop::UserDdl, why)) = w.all_fit().await {
        w.close().await;
        return aborted(AbortReason::UserDdl, why);
    }
    let missing = missing_on(&mut w, t, def).await;
    w.close().await;
    if !missing.is_empty() {
        return BuildOutcome::Failed {
            error: format!("{name} missing on {} after the build", missing.join(", ")),
        };
    }
    rep.already_present = rep.nodes.iter().all(|n| n.skipped);
    BuildOutcome::Built(rep)
}

enum RsuStep {
    Built(NodeBuild),
    LockWait,
    Stop(BuildOutcome),
}

async fn rsu_one(
    w: &mut GWatch,
    i: usize,
    ep: &NodeEndpoint,
    sql: &str,
    s: &RolloutSettings,
) -> RsuStep {
    let mut ddl = match Node::connect(ep, s.max_build + Duration::from_secs(60)).await {
        Ok(d) => d,
        Err(e) => {
            return RsuStep::Stop(aborted(
                AbortReason::Degraded,
                format!("{}: DDL session: {e}", ep.node),
            ));
        }
    };
    for set in [
        "SET SESSION sql_log_bin = 0".to_owned(),
        format!(
            "SET SESSION lock_wait_timeout = {}",
            s.ddl_lock_wait.as_secs().max(1)
        ),
        "SET SESSION wsrep_OSU_method = 'RSU'".to_owned(),
    ] {
        if let Err(e) = ddl.exec(&set).await {
            ddl.close().await;
            return RsuStep::Stop(BuildOutcome::Failed {
                error: format!("{}: {set}: {e}", ep.node),
            });
        }
    }
    w.building = Some(i);
    w.ours = u64::from(ddl.id());
    w.started = false;
    let (res, stopped, took) = watched(&mut ddl, ep, sql, s.poll, true, w).await;
    let back = ddl.exec("SET SESSION wsrep_OSU_method = 'TOI'").await;
    ddl.close().await;
    w.building = None;
    if let Err(e) = back {
        tracing::warn!(node = %ep.node, error = %e, "could not set TOI back (the session is closed anyway)");
    }
    // Resync whatever happened: the node was desynced.
    let deadline = s.galera.resync_floor.max(took * 2);
    let synced = w.wait_synced(i, deadline).await;
    if synced.is_none() {
        return RsuStep::Stop(aborted(
            AbortReason::Degraded,
            format!(
                "{} not Synced within {} ms after its RSU build",
                ep.node,
                deadline.as_millis()
            ),
        ));
    }
    match (res, stopped) {
        (Ok(()), _) => RsuStep::Built(NodeBuild {
            node: ep.node.to_string(),
            build_ms: ms(took),
            resync_ms: synced,
            skipped: false,
        }),
        (_, Some((Stop::LockQueue, _))) => RsuStep::LockWait,
        (_, Some((stop, why))) => RsuStep::Stop(aborted(stop_reason(stop), why)),
        (Err(e), None) if e.errno == Some(1205) => RsuStep::LockWait,
        (Err(e), None) => RsuStep::Stop(BuildOutcome::Failed {
            error: format!("{}: {e}", ep.node),
        }),
    }
}

/// Builds in total order from the writer: small tables only.
#[allow(clippy::too_many_lines)] // checks, retries, then every node
pub async fn build_toi(
    view: &ClusterView,
    t: &ShardTable,
    def: &IndexDef,
    s: &RolloutSettings,
) -> BuildOutcome {
    let (mut w, eps, wi) = match prepare(view, t, s).await {
        Ok(x) => x,
        Err(o) => return o,
    };
    let mut rep = report(t, "galera_toi");
    if missing_on(&mut w, t, def).await.is_empty() {
        w.close().await;
        rep.already_present = true;
        return BuildOutcome::Built(rep);
    }
    match w.mons[wi].table_bytes(t).await {
        Ok(b) if b <= s.galera.toi_max_bytes => {}
        Ok(b) => {
            w.close().await;
            return BuildOutcome::Failed {
                error: format!(
                    "{b} bytes: over the TOI limit of {} bytes",
                    s.galera.toi_max_bytes
                ),
            };
        }
        Err(e) => {
            w.close().await;
            return aborted(AbortReason::Degraded, format!("writer: {e}"));
        }
    }
    let ep = &eps[wi];
    let sql = alter_sql(t, def);
    let mut tries = 0;
    let took = loop {
        tries += 1;
        if let Some((stop, why)) = w.all_fit().await {
            w.close().await;
            return aborted(stop_reason(stop), why);
        }
        let mut ddl = match Node::connect(ep, s.max_build + Duration::from_secs(60)).await {
            Ok(d) => d,
            Err(e) => {
                w.close().await;
                return aborted(AbortReason::Degraded, format!("DDL session: {e}"));
            }
        };
        let set = format!(
            "SET SESSION lock_wait_timeout = {}",
            s.ddl_lock_wait.as_secs().max(1)
        );
        if let Err(e) = async {
            ddl.exec(&set).await?;
            ddl.exec("SET SESSION wsrep_OSU_method = 'TOI'").await
        }
        .await
        {
            ddl.close().await;
            w.close().await;
            return BuildOutcome::Failed {
                error: format!("session: {e}"),
            };
        }
        // Not killable: a TOI statement may already run on every node.
        let (res, _, took) = watched(&mut ddl, ep, &sql, s.poll, false, &mut w).await;
        ddl.close().await;
        match res {
            Ok(()) => break took,
            Err(e) if e.errno == Some(1205) => {
                rep.lock_retries += 1;
                if tries >= s.lock_retries {
                    w.close().await;
                    return BuildOutcome::LockTimeout { tries };
                }
                tokio::time::sleep(s.lock_retry_pause).await;
            }
            Err(e) => {
                w.close().await;
                return BuildOutcome::Failed {
                    error: e.to_string(),
                };
            }
        }
    };
    rep.build_ms = ms(took);
    rep.nodes.push(NodeBuild {
        node: ep.node.to_string(),
        build_ms: rep.build_ms,
        resync_ms: None,
        skipped: false,
    });
    // Every node applies it in order; give them a moment.
    let started = Instant::now();
    let mut missing = missing_on(&mut w, t, def).await;
    while !missing.is_empty() && started.elapsed() < s.catch_up_floor.max(took * 2) {
        tokio::time::sleep(s.poll).await;
        missing = missing_on(&mut w, t, def).await;
    }
    w.close().await;
    if !missing.is_empty() {
        return BuildOutcome::Failed {
            error: format!("TOI build missing on {}", missing.join(", ")),
        };
    }
    rep.replicas_ms = Some(ms(started.elapsed()));
    BuildOutcome::Built(rep)
}
