//! Full-text search in the gateway.
//!
//! **Where copies live.** With `search.copies` above 0 (2 by default) and a
//! worker with the search role in the DC, copies run on workers
//! ([`remote`]; `sprig_search::placement`): this gateway writes the
//! index's record, the controller places `search.copies` copies on
//! different workers, and queries go to a ready copy, the other one when it
//! fails. Without search workers (or with `search.copies = 0`), every
//! gateway that serves an index holds one copy itself,
//! under `SPRIG_SEARCH_DIR` (default `<temp>/sprig-search/<gateway id>`).
//! Both run the same copy runtime (`sprig_search::copy`).
//!
//! ```text
//! query(index, ops, consistency):
//!   latest / own_writes: targets := every DC cluster's @@gtid_binlog_pos now
//!   on workers: the index's record (once per definition); a ready copy of its
//!     generation answers after its applied positions reach the targets
//!   in this gateway: the local copy (started when missing); the same wait
//!   stale_ok: no targets; a copy still seeding waits up to
//!     search.wait_timeout_ms, then 39824
//! ```

// Short names (s, t, d, k) and long arms, as in the rest of the gateway.
#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::cast_precision_loss
)]

pub mod catalog;
pub mod docs;
pub mod explain;
pub(crate) mod remote;
pub mod runtime;
pub mod seed;
pub mod visible;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use sprig_binlog::GtidPos;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_search::analyze::CustomAnalyzer;
use sprig_search::copy::FeedParts;
use sprig_search::def::IndexDef;
use sprig_search::query::SearchQuery;
use sprig_search::rpc::{Answer, Op, QueryRequest, Wait};
use sprig_settings::{LiveSettings, SettingCtx};

use crate::pipeline::SharedExecutor;
use crate::session::SessionTable;
use crate::tenant::TenantServices;

pub use runtime::Runtime;

pub use sprig_search::settings::*;

/// How fresh a search must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// The last commit.
    StaleOk,
    /// Everything committed on the tenant's cluster before the query.
    /// `own_writes` is served as this: stronger, and the
    /// wait is the same when the session's last write is the newest.
    Latest,
}

impl Consistency {
    /// From a setting or option value.
    ///
    /// # Errors
    /// 39822 for an unknown name.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "stale_ok" | "" => Ok(Self::StaleOk),
            "own_writes" | "latest" => Ok(Self::Latest),
            other => Err(SprigError::new(
                SprigCode::SearchQuerySyntax,
                format!("unknown search consistency '{other}'"),
            )),
        }
    }
}

/// Values read per query.
#[derive(Debug, Clone)]
pub struct Knobs {
    /// `search.commit_interval_ms`.
    pub commit_interval: Duration,
    /// `search.wait_timeout_ms`.
    pub wait_timeout: Duration,
    /// `search.default_consistency`.
    pub consistency: Consistency,
    /// `search.scoring`.
    pub scoring: sprig_search::Scoring,
    /// `search.max_window`.
    pub max_window: usize,
    /// `search.max_expansions`.
    pub max_expansions: usize,
    /// `search.highlight_chars`.
    pub highlight_chars: usize,
    /// `search.writer_memory_mb` in bytes.
    pub writer_bytes: usize,
    /// `search.fulltext_ddl`.
    pub fulltext_ddl: String,
    /// `search.max_indexes_per_object`.
    pub max_per_object: usize,
    /// `search.copies` (0: copies in gateways).
    pub copies: u32,
    /// `search.query_timeout_ms`.
    pub query_timeout: Duration,
    /// `search.overfetch_factor`.
    pub overfetch: usize,
}

/// What the service needs.
pub struct SearchDeps {
    /// Tenant catalogs, settings, locator.
    pub tenants: Arc<TenantServices>,
    /// This DC's tier (shard maps, handoff records).
    pub dc_store: Arc<dyn sprig_meta::MetaStore>,
    /// Logins (definers' principals).
    pub login: Arc<sprig_auth::LoginService>,
    /// The shared executor (seeding reads).
    pub executor: SharedExecutor,
    /// The gateway's sessions.
    pub sessions: Arc<SessionTable>,
    /// Live settings.
    pub live: LiveSettings,
    /// This DC.
    pub dc: DcId,
    /// This gateway's id.
    pub gateway_id: String,
    /// Mutual TLS to search workers (`[tls]`; `None`: plaintext, dev).
    pub peer_tls: Option<crate::peer::PeerTls>,
}

impl std::fmt::Debug for SearchDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchDeps")
            .field("gateway_id", &self.gateway_id)
            .finish_non_exhaustive()
    }
}

/// The gateway's search service.
pub struct SearchService {
    pub(crate) deps: SearchDeps,
    pub(crate) catalog: catalog::SearchCatalog,
    pub(crate) feed: FeedParts,
    pub(crate) dir: PathBuf,
    pub(crate) remote: remote::Remote,
    runtimes: Mutex<HashMap<(u64, u64), Arc<Runtime>>>,
    stop: CancellationToken,
}

impl std::fmt::Debug for SearchService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchService")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

static SERVICE: OnceLock<Arc<SearchService>> = OnceLock::new();

/// The gateway's search service, once started.
#[must_use]
pub fn service() -> Option<&'static Arc<SearchService>> {
    SERVICE.get()
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SearchService {
    /// Starts the service and makes it [`service`]; stops its runtimes
    /// on `stop`.
    ///
    /// # Errors
    /// The cluster registry's.
    pub fn start(deps: SearchDeps, stop: CancellationToken) -> Result<Arc<Self>> {
        let t = &deps.tenants;
        let locator: Arc<dyn sprig_catalog::CatalogLocator> = t.locator.clone();
        let feed = crate::mongo::changes::feed_parts(
            Arc::clone(&t.global),
            Arc::clone(&deps.dc_store),
            locator,
        )?;
        let dir = std::env::var_os("SPRIG_SEARCH_DIR").map_or_else(
            || {
                std::env::temp_dir()
                    .join("sprig-search")
                    .join(deps.gateway_id.replace(['/', ':'], "_"))
            },
            PathBuf::from,
        );
        let remote = remote::Remote::new(Arc::clone(&deps.dc_store), deps.peer_tls.clone());
        let svc = Arc::new(Self {
            catalog: catalog::SearchCatalog::new(Arc::clone(&deps.tenants)),
            feed: FeedParts {
                clusters: feed.0,
                placement: feed.1,
                names: feed.2,
            },
            remote,
            deps,
            dir,
            runtimes: Mutex::new(HashMap::new()),
            stop,
        });
        let _ = SERVICE.set(Arc::clone(&svc));
        tracing::info!(dir = %svc.dir.display(), "search service started");
        Ok(svc)
    }

    /// The values a query reads now.
    #[must_use]
    pub fn knobs(&self) -> Knobs {
        let live = &self.deps.live;
        let cx = SettingCtx::process(&self.deps.dc);
        let us = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
        Knobs {
            commit_interval: Duration::from_millis(live.get(&COMMIT_INTERVAL_MS, &cx)),
            wait_timeout: Duration::from_millis(live.get(&WAIT_TIMEOUT_MS, &cx)),
            consistency: Consistency::parse(&live.get(&DEFAULT_CONSISTENCY, &cx))
                .unwrap_or(Consistency::StaleOk),
            scoring: if live.get(&SCORING, &cx).eq_ignore_ascii_case("local") {
                sprig_search::Scoring::Local
            } else {
                sprig_search::Scoring::Global
            },
            max_window: us(live.get(&MAX_WINDOW, &cx)),
            max_expansions: us(live.get(&MAX_EXPANSIONS, &cx)),
            highlight_chars: us(live.get(&HIGHLIGHT_CHARS, &cx)),
            writer_bytes: us(live.get(&WRITER_MEMORY_MB, &cx)).saturating_mul(1 << 20),
            fulltext_ddl: live.get(&FULLTEXT_DDL, &cx).to_ascii_lowercase(),
            max_per_object: us(live.get(&MAX_INDEXES_PER_OBJECT, &cx)),
            copies: u32::try_from(live.get(&COPIES, &cx)).unwrap_or(2),
            query_timeout: Duration::from_millis(live.get(&QUERY_TIMEOUT_MS, &cx)),
            overfetch: us(live.get(&OVERFETCH_FACTOR, &cx)),
        }
    }

    /// The catalog module.
    #[must_use]
    pub fn catalog(&self) -> &catalog::SearchCatalog {
        &self.catalog
    }

    /// The runtime of `def` (started when missing, or when its definition
    /// changed).
    ///
    /// # Errors
    /// 39828 for analyzers that aren't defined; Tantivy's.
    pub fn runtime(
        self: &Arc<Self>,
        tenant: TenantId,
        def: &Arc<IndexDef>,
        customs: &[sprig_search::analyze::CustomAnalyzer],
    ) -> Result<Arc<Runtime>> {
        let k = (tenant.0, def.index_id);
        let mut m = lock(&self.runtimes);
        if let Some(rt) = m.get(&k) {
            if rt.def.as_ref() == def.as_ref() && !rt.is_stopped() {
                return Ok(Arc::clone(rt));
            }
            rt.stop();
        }
        let dir = self
            .dir
            .join(format!("t{}", tenant.0))
            .join(format!("i{}", def.index_id))
            .join(format!("g{}", def.generation));
        let rt = Runtime::start(
            Arc::new(runtime::GatewayHost(Arc::clone(self))),
            tenant,
            Arc::clone(def),
            customs,
            dir,
            self.stop.child_token(),
        )?;
        m.insert(k, Arc::clone(&rt));
        Ok(rt)
    }

    /// Stops and forgets the runtime of index `index` and deletes its
    /// files; removes its records from the DC tier, so its copies on
    /// workers stop and delete theirs.
    pub fn forget(self: &Arc<Self>, tenant: TenantId, index: u64) {
        if let Some(rt) = lock(&self.runtimes).remove(&(tenant.0, index)) {
            rt.stop();
        }
        let me = Arc::clone(self);
        tokio::spawn(async move { me.remote.drop_index(tenant, index).await });
        let d = self
            .dir
            .join(format!("t{}", tenant.0))
            .join(format!("i{index}"));
        if let Err(e) = std::fs::remove_dir_all(&d)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %d.display(), error = %e, "search index files not removed");
        }
    }

    /// Stops every runtime (gateway shutdown).
    pub fn stop_all(&self) {
        self.stop.cancel();
        for rt in lock(&self.runtimes).values() {
            rt.stop();
        }
    }

    /// Runtimes held (tests, `EXPLAIN`).
    #[must_use]
    pub fn runtimes(&self) -> Vec<Arc<Runtime>> {
        lock(&self.runtimes).values().cloned().collect()
    }

    /// Whether copies run on workers now: `search.copies` above 0 and a
    /// worker with the search role registered in the DC.
    pub async fn on_workers(&self) -> bool {
        if self.knobs().copies == 0 {
            return false;
        }
        match self.remote.search_workers().await {
            Ok(w) if !w.is_empty() => true,
            Ok(_) => {
                metrics::counter!("sprig_search_gateway_copies_total").increment(1);
                false
            }
            Err(e) => {
                tracing::warn!(error = %e, "search workers unknown; using this gateway's copy");
                false
            }
        }
    }

    /// Makes sure `def` has copies (a local runtime, or its record for the
    /// workers) and waits up to `wait` for one to be ready; a copy still
    /// building after that is logged, not an error.
    ///
    /// # Errors
    /// 39828 for undefined analyzers; the store's.
    pub async fn prepare(
        self: &Arc<Self>,
        tenant: TenantId,
        def: &Arc<IndexDef>,
        customs: &[CustomAnalyzer],
        wait: Duration,
    ) -> Result<()> {
        let r = if self.on_workers().await {
            let k = self.knobs();
            self.remote.ensure(tenant, def, customs, k.copies).await?;
            let req = QueryRequest {
                tenant: tenant.0,
                index: def.index_id,
                generation: def.generation,
                query: SearchQuery::All,
                ops: vec![Op::Count],
                max_expansions: k.max_expansions,
                wait: Wait {
                    timeout_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                    targets: Vec::new(),
                },
            };
            self.remote
                .query(tenant, def, &req, wait, k.query_timeout)
                .await
                .map(|_| ())
        } else {
            self.runtime(tenant, def, customs)?.wait_ready(wait).await
        };
        if let Err(e) = r {
            tracing::info!(index = def.name, error = %e, "search index still building");
        }
        Ok(())
    }

    /// Each DC cluster's position now (`latest` and `own_writes`).
    ///
    /// # Errors
    /// The clusters' (retryable).
    pub async fn targets(&self) -> Result<Vec<(ClusterId, GtidPos)>> {
        let mut out = Vec::new();
        for c in runtime::dc_clusters(self).await? {
            out.push((c.clone(), self.feed.clusters.position(&c).await?));
        }
        Ok(out)
    }

    /// Runs `ops` over `query` on a copy of `def`, after the wait
    /// `consistency` asks for; the answers and the copy's applied
    /// positions (JSON).
    ///
    /// # Errors
    /// 39822-39827 and the query's errors.
    pub async fn query(
        self: &Arc<Self>,
        tenant: TenantId,
        def: &Arc<IndexDef>,
        customs: &[CustomAnalyzer],
        query: SearchQuery,
        ops: Vec<Op>,
        consistency: Consistency,
    ) -> Result<(Vec<Answer>, String)> {
        let k = self.knobs();
        let targets = match consistency {
            Consistency::StaleOk => Vec::new(),
            Consistency::Latest => self.targets().await?,
        };
        if self.on_workers().await {
            self.remote.ensure(tenant, def, customs, k.copies).await?;
            let req = QueryRequest {
                tenant: tenant.0,
                index: def.index_id,
                generation: def.generation,
                query,
                ops,
                max_expansions: k.max_expansions,
                wait: Wait {
                    timeout_ms: u64::try_from(k.wait_timeout.as_millis()).unwrap_or(u64::MAX),
                    targets: targets
                        .iter()
                        .map(|(c, p)| (c.to_string(), p.to_string()))
                        .collect(),
                },
            };
            return self
                .remote
                .query(tenant, def, &req, k.wait_timeout, k.query_timeout)
                .await;
        }
        let rt = self.runtime(tenant, def, customs)?;
        if targets.is_empty() {
            rt.wait_ready(k.wait_timeout).await?;
        } else {
            rt.wait_for(&targets, k.wait_timeout).await?;
        }
        let ix = Arc::clone(&rt.index);
        let answers = tokio::task::spawn_blocking(move || {
            sprig_search::rpc::execute(&ix, &query, &ops, k.max_expansions)
        })
        .await
        .map_err(|e| SprigError::internal(e.to_string()))??;
        let applied = serde_json::to_string(&rt.applied().text()).unwrap_or_default();
        Ok((answers, applied))
    }

    /// Whether a copy of `def` is ready, and where (`SHOW SEARCH
    /// INDEXES`, `$listSearchIndexes`).
    pub async fn state(&self, tenant: TenantId, def: &IndexDef) -> (bool, String) {
        if let Some(rt) = lock(&self.runtimes)
            .get(&(tenant.0, def.index_id))
            .filter(|r| r.def.generation == def.generation)
        {
            let p = rt.phase();
            return (
                p == runtime::Phase::Ready,
                format!("{} in this gateway", p.name()),
            );
        }
        if self.knobs().copies > 0 {
            return self.remote.state(tenant, def).await;
        }
        (false, "not started".to_owned())
    }
}
