//! The router in the tenant path (chapter 17, R1): a statement chapter 07
//! rewrote goes to [`sprig_router::plan`] when the session's tenant has
//! sharded or reference tables, and is relayed as before otherwise.
//!
//! ```text
//! no topology for the tenant          → forward (today's path, one map lookup)
//! NotRouted                           → forward
//! Pass on the session's cluster       → forward the spliced text (relayed undecoded)
//! Reject                              → ERR 39420 with the reason
//! Read, LookupJoin, Write, Explain    → sprig_router::execute over the session's
//!                                       backend connection; one resultset or OK
//! ```
//!
//! Until chapter 15's table groups reach the catalog, the layout comes
//! from `[gateway] router_topology_file` (a development and test hook,
//! [`sprig_router::TopologyFile`]), re-read when the file changes. Without
//! it nothing here runs. Shards on another cluster than the session's
//! connection fail with reason `cross_cluster` for now.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::client::PacketKind;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::decode_text_row;
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags};
use sprig_router::{
    Budget, Output, Plan, PlanCtx, ShardBackend, ShardOk, ShardOutcome, ShardRows, StaticTopology,
};
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{NameMap, OutputCtx, ResultMapper};

use super::PassSession;
use super::relay::{backend_error, map_error, protocol_error, rename_all};
use super::tenant::Step;
use crate::backend::{BackendSession, StmtInfo};
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues};
use crate::tenant::names::Accounts;

/// The file's modification time and each tenant's layout.
type Loaded = (Option<SystemTime>, HashMap<String, Arc<StaticTopology>>);

/// The tenants' layouts from the topology file.
struct Topologies {
    path: PathBuf,
    loaded: RwLock<Loaded>,
    /// Milliseconds since the epoch of the last modification-time check.
    checked: AtomicU64,
}

static TOPOLOGIES: OnceLock<Topologies> = OnceLock::new();

/// Reads `[gateway] router_topology_file` (once per process).
pub(crate) fn init(path: Option<&Path>) {
    let Some(path) = path else { return };
    let t = Topologies {
        path: path.to_owned(),
        loaded: RwLock::new((None, HashMap::new())),
        checked: AtomicU64::new(0),
    };
    t.reload();
    tracing::info!(file = %path.display(), "router topology file (development hook)");
    let _ = TOPOLOGIES.set(t);
}

impl Topologies {
    fn reload(&self) {
        let mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        if let Ok(g) = self.loaded.read()
            && g.0 == mtime
            && mtime.is_some()
        {
            return;
        }
        let parsed = std::fs::read(&self.path)
            .map_err(|e| e.to_string())
            .and_then(|b| {
                serde_json::from_slice::<sprig_router::TopologyFile>(&b).map_err(|e| e.to_string())
            });
        let mut map = HashMap::new();
        match parsed {
            Ok(f) => {
                for (name, t) in &f.tenants {
                    match t.build() {
                        Ok(s) => {
                            map.insert(name.clone(), Arc::new(s));
                        }
                        Err(e) => {
                            tracing::warn!(tenant = %name, error = %e, "router topology: tenant skipped");
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(file = %self.path.display(), error = %e, "router topology unreadable");
            }
        }
        if let Ok(mut g) = self.loaded.write() {
            *g = (mtime, map);
        }
    }

    fn get(&self, tenant: &str) -> Option<Arc<StaticTopology>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let last = self.checked.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= 1000
            && self
                .checked
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.reload();
        }
        self.loaded.read().ok()?.1.get(tenant).cloned()
    }
}

fn topology_for(tenant: &str) -> Option<Arc<StaticTopology>> {
    TOPOLOGIES.get()?.get(tenant)
}

/// The session's backend connection as the router's [`ShardBackend`].
struct GwBackend<'a> {
    bs: &'a mut BackendSession,
    cluster: String,
}

#[async_trait::async_trait]
impl ShardBackend for GwBackend<'_> {
    async fn run(&mut self, cluster: &str, sql: &str, budget: &mut Budget) -> Result<ShardOutcome> {
        if cluster != self.cluster {
            return Err(sprig_router::Rejection::new(
                sprig_router::Reason::CrossCluster,
                format!("shards on cluster {cluster} as well as {}", self.cluster),
            )
            .error());
        }
        let info = StmtInfo::classify(sql.as_bytes());
        self.bs.query(&info, sql.as_bytes()).await?;
        let caps = self.bs.backend_caps().unwrap_or_default();
        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut want = 0usize;
        let mut rows = Vec::new();
        let mut outcome: Option<ShardOutcome> = None;
        let mut err: Option<SprigError> = None;
        let mut over: Option<SprigError> = None;
        while let Some(p) = self.bs.next().await? {
            match p.kind {
                PacketKind::ColumnCount(n) => {
                    want = usize::try_from(n).unwrap_or(0);
                    columns.clear();
                }
                PacketKind::ColumnDef => {
                    columns.push(
                        ColumnDef::decode(&p.payload, caps)
                            .map_err(|e| protocol_error(&format!("column definition: {e}")))?,
                    );
                }
                PacketKind::ColumnsEof => {}
                PacketKind::Row => {
                    if over.is_some() {
                        continue;
                    }
                    if let Err(e) = budget.charge(p.payload.len() as u64 + 16 * want as u64) {
                        over = Some(e);
                        continue;
                    }
                    let vals = decode_text_row(&p.payload, want)
                        .map_err(|e| protocol_error(&format!("text row: {e}")))?;
                    rows.push(
                        vals.into_iter()
                            .map(|v| v.map(|b| p.payload.slice_ref(b)))
                            .collect(),
                    );
                }
                PacketKind::End(t) => {
                    outcome = Some(ShardOutcome::Rows(ShardRows {
                        columns: std::mem::take(&mut columns),
                        rows: std::mem::take(&mut rows),
                        warnings: t.warnings(),
                    }));
                }
                PacketKind::Ok(ok) => {
                    outcome = Some(ShardOutcome::Ok(ShardOk {
                        affected: ok.affected_rows,
                        last_insert_id: ok.last_insert_id,
                        warnings: ok.warnings,
                        info: String::from_utf8_lossy(&ok.info).into_owned(),
                    }));
                }
                PacketKind::Err(e) => err = Some(backend_error(&e)),
                _ => return Err(protocol_error("a packet the router doesn't expect")),
            }
        }
        self.bs.finish().await?;
        if let Some(e) = err.or(over) {
            return Err(e);
        }
        outcome.ok_or_else(|| protocol_error("a response without an end"))
    }

    fn in_transaction(&self) -> bool {
        self.bs.pins().in_transaction()
    }
}

impl PassSession {
    /// A forwarded statement of a tenant session: through the router when
    /// the tenant has a topology, else as before.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn routed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Vec<u8>,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mut mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let topo = self
            .tenant
            .as_deref()
            .and_then(|ts| ts.principal.tenant_name.as_deref())
            .and_then(topology_for);
        let (Some(topo), Ok(text)) = (topo, std::str::from_utf8(&sql)) else {
            return self
                .forward(
                    cx,
                    Bytes::from(sql),
                    Some(original),
                    more,
                    format,
                    mapper,
                    map,
                    out,
                )
                .await;
        };
        let default_schema = match &self.bs {
            Some(bs) => bs.vars().schema.clone(),
            None => self
                .tenant
                .as_deref()
                .and_then(|ts| ts.schema_of(ts.current.as_deref())),
        };
        let cluster = self.bs.as_ref().map_or_else(
            || self.target.cluster.to_string(),
            |bs| bs.target().cluster.to_string(),
        );
        let plan = {
            let pcx = PlanCtx {
                topo: &*topo,
                default_schema: default_schema.as_deref(),
                session_cluster: &cluster,
                max_route_values: sprig_router::MAX_ROUTE_VALUES,
                lookup_batch: sprig_router::LOOKUP_BATCH,
            };
            sprig_router::plan(text, &pcx)
        };
        match plan {
            Plan::NotRouted => {
                self.forward(
                    cx,
                    Bytes::from(sql),
                    Some(original),
                    more,
                    format,
                    mapper,
                    map,
                    out,
                )
                .await
            }
            Plan::Pass(t) if t.cluster == cluster => {
                metrics::counter!("sprig_router_statements_total", "plan" => "pass").increment(1);
                self.forward(
                    cx,
                    Bytes::from(t.sql),
                    Some(original),
                    more,
                    format,
                    mapper,
                    map,
                    out,
                )
                .await
            }
            Plan::Reject(r) => self.fail(out, &r.error()).await,
            plan => {
                if format == RowFormat::Binary {
                    let e = sprig_router::Rejection::new(
                        sprig_router::Reason::Unsupported,
                        "a prepared statement across shards (text protocol only in R1 so far)",
                    );
                    return self.fail(out, &e.error()).await;
                }
                let _ = self.bs(cx);
                let Some(bs) = self.bs.as_mut() else {
                    return Err(SprigError::internal("no backend session"));
                };
                let mut be = GwBackend { bs, cluster };
                let mut budget = Budget::mb(sprig_router::MAX_STATEMENT_MEMORY_MB);
                let result = sprig_router::execute(&plan, &mut be, &mut budget).await;
                let accounts = Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
                let ctx = OutputCtx {
                    map,
                    accounts: Some(&accounts),
                    lex: LexOptions::mariadb(),
                };
                let mut status = self.status();
                if more {
                    status |= StatusFlags::MORE_RESULTS_EXISTS;
                }
                match result {
                    Err(e) => {
                        let e = map_error(e, &ctx);
                        self.fail(out, &e).await
                    }
                    Ok(Output::Ok(ok)) => {
                        let mut p = OkPacket::new(status);
                        p.affected_rows = ok.affected;
                        p.last_insert_id = ok.last_insert_id;
                        p.warnings = ok.warnings;
                        p.info = Bytes::from(ok.info);
                        out.ok(p).await?;
                        Ok(Step {
                            errored: false,
                            status: Some(status),
                        })
                    }
                    Ok(Output::Rows {
                        mut columns,
                        rows,
                        warnings,
                    }) => {
                        mapper.column_defs(&mut columns, &ctx);
                        let renames = std::mem::take(&mut self.subst);
                        rename_all(&mut columns, &renames);
                        if self.persona == Persona::MySql84 {
                            for c in &mut columns {
                                sprig_sql::compat::meta::mysql_column(c);
                            }
                        }
                        out.begin_rows(&columns, RowFormat::Text).await?;
                        for r in &rows {
                            let vals: Vec<Option<&[u8]>> = r.iter().map(|v| v.as_deref()).collect();
                            out.row(RowValues::Text(&vals)).await?;
                        }
                        out.end_rows(EndOfRows { status, warnings }).await?;
                        Ok(Step {
                            errored: false,
                            status: Some(status),
                        })
                    }
                }
            }
        }
    }
}
