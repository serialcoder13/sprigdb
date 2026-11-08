//! The Admin API's automatic-indexing calls (chapter 13, "`recommend`
//! mode" and the interfaces for people; chapter 37's `Indexes` and
//! `Explain` services; build step 13), behind `sprigctl index` and
//! `sprigctl explain query`.
//!
//! | Call | Route |
//! |---|---|
//! | `Indexes.ListIndexes` | `GET /admin/v1/tenants/{name}/indexes?db=&table=&state=&all=` |
//! | `Indexes.ListRecommendations` | `GET /admin/v1/tenants/{name}/recommendations?db=&table=` |
//! | `Indexes.GetIndex` | `GET /admin/v1/tenants/{name}/indexes/{index}` |
//! | `Indexes.Approve` (`Apply`) | `POST /admin/v1/tenants/{name}/indexes/{index}:approve` |
//! | `Indexes.Reject` (`Dismiss`) | `POST .../indexes/{index}:reject` |
//! | `Indexes.Drop` | `POST .../indexes/{index}:drop` |
//! | `Indexes.Pin` (`Freeze`), `Unpin` | `POST .../indexes/{index}:pin`, `:unpin` |
//! | `Indexes.Restore` | `POST .../indexes/{index}:restore` |
//! | `Explain.ExplainQuery` | `GET /admin/v1/tenants/{name}/queries/{fingerprint}:explain` |
//!
//! `{index}` is an `auto_indexes.index_id`, or a name: `sprig_auto_<hex>`,
//! `<table>.<name>` or `<db>.<table>.<name>` (case-insensitive), which
//! must name one row.
//!
//! **Actions** move rows along chapter 13's state machine with a
//! conditional update (`set_state`), so a concurrent change by the
//! evaluation, rollout or observer loops makes the call fail with 39202
//! instead of overwriting it:
//!
//! - **approve**: `recommended` → `rolling_out`. Rollout picks up rows in
//!   `rolling_out` (its queue in the store) and builds them shard by
//!   shard. Advice (`advice_*`) can't be approved in R0.
//! - **reject**: `recommended` → `dropped` with reason `dismissed`, which
//!   blocks the same definition for `auto_index.recreate_block_days`.
//! - **drop**: `rolling_out`, `observing`, `active`, `retiring` or
//!   `failed` → `dropping` with reason `operator`; the observer's drop
//!   step removes it from every shard (also blocked from re-creation).
//! - **pin** / **unpin**: chapter 13's `freeze` and `freeze --off`: any
//!   live state → `frozen` (the prior state kept) and back.
//! - **restore** (decision O1): a `retiring` index is made visible again
//!   (`NOT IGNORED` on every shard) and pinned; a serving one is pinned. For
//!   a user or migrated index (an `index_usage` row, named by
//!   `[<db>.][<table>.]<name>`) a dropped one is recreated from its saved
//!   definition first. Pinned user indexes are never judged again until
//!   `unpin`. Restore runs the shard statements itself, so the service
//!   needs the loop's [`AutoIndexEnv`] (`with_env`).
//!
//! `list` also shows user and migrated indexes whose removal is under way
//! or recommended, or that were removed or pinned (with `origin`).
//!
//! Every action is logged as a `decision` event (chapter 11's `decisions`
//! table doesn't exist yet), with the actor (`op:<actor>`) and reason.
//! Calls are catalog work, answered by any controller, like catalog-scope
//! settings. The `autoindex` module is migrated on first use
//! (idempotent), as the user calls migrate `auth`.
//!
//! **Explain** reads the store's evidence for one fingerprint over
//! `workload.retention_days` (30): the candidates serving it (and the
//! tables that got none, recorded as [`CandidateRow::skipped`]), each
//! one's cost gates checked against today's settings, its validations
//! and the index it became, and each object's resolved policy and route.
//! What the store doesn't hold yet: why a fingerprint didn't qualify
//! (trigger evaluation, step 2, records only fingerprints that did).

pub mod view;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mysql_async::Conn;
use serde::{Deserialize, Serialize};
use sprig_advisor::autoindex::observe::{self, user};
use sprig_advisor::autoindex::policy::{Layered, ObjectPolicy};
use sprig_advisor::autoindex::rollout::ShardTable;
use sprig_advisor::autoindex::rollout::node::ignore_sql;
use sprig_advisor::autoindex::store::usage::{UsageRow, UserIndexState};
use sprig_advisor::autoindex::store::{
    AUTOINDEX_MODULE, AutoIndexRow, AutoIndexStore, CandidateRow, DecisionLog, IndexDecision,
    IndexState, LogDecisions, StateChange,
};
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::ddl::backend::DdlBackend;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{CatalogLocator, MariaCatalog, Migrator};
use sprig_common::ids::{DcId, Fingerprint, ObjectId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{LiveSettings, Registry};

use crate::admin::invalid;
use crate::autoindex_loop::AutoIndexEnv;
use crate::tree::{Directory, TenantState};
use view::{
    Action, Evidence, IndexActionRequest, IndexActionResponse, IndexDetail, ListIndexesResponse,
    ObjectNames, Plan, QueryExplanation, UserDdl, UserPlan,
};

/// How far back `explain` reads candidates: `workload.retention_days`'s
/// default, the store's retention.
pub const EXPLAIN_WINDOW: Duration = Duration::from_secs(30 * 86_400);

/// `GET .../indexes` and `.../recommendations`: which rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListIndexesQuery {
    /// Only this logical database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,
    /// Only this table (with `db`, or in any database).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Only this state (`active`, `recommended`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Also dropped and rejected rows (history). A query string carries it
    /// as text.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "bool_text"
    )]
    pub all: Option<bool>,
}

fn bool_text<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<bool>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum B {
        Bool(bool),
        Text(String),
    }
    match Option::<B>::deserialize(d)? {
        None => Ok(None),
        Some(B::Bool(b)) => Ok(Some(b)),
        Some(B::Text(t)) => match t.as_str() {
            "true" | "1" | "yes" => Ok(Some(true)),
            "false" | "0" | "no" => Ok(Some(false)),
            other => Err(serde::de::Error::custom(format!(
                "expected true or false, not {other:?}"
            ))),
        },
    }
}

/// The Admin API's automatic-indexing calls.
#[async_trait::async_trait]
pub trait AutoIndexAdmin: Send + Sync {
    /// The tenant's automatic indexes and recommendations.
    ///
    /// # Errors
    /// 39072 unknown or inactive tenant; 1049, 1146 for an unknown
    /// database or table filter; 39792 for a bad state; catalog errors.
    async fn list(&self, tenant: &str, q: &ListIndexesQuery) -> Result<ListIndexesResponse>;

    /// One index with its candidate, validations and history.
    ///
    /// # Errors
    /// 39201 for an unknown index; 39792 for an ambiguous name.
    async fn show(&self, tenant: &str, index: &str) -> Result<IndexDetail>;

    /// An operator action (module docs).
    ///
    /// # Errors
    /// 39201 unknown index; 39202 when its state doesn't allow the action
    /// or changed meanwhile.
    async fn act(
        &self,
        tenant: &str,
        index: &str,
        action: Action,
        req: &IndexActionRequest,
    ) -> Result<IndexActionResponse>;

    /// Why an index was or wasn't built for `fingerprint`.
    ///
    /// # Errors
    /// 39792 for a fingerprint that isn't 16 hex digits; catalog errors.
    async fn explain_query(&self, tenant: &str, fingerprint: &str) -> Result<QueryExplanation>;
}

/// The rows of `rows` an `{index}` reference names (module docs).
///
/// # Errors
/// 39201 when none; 39792 when a name matches several.
pub fn resolve_index<'r>(
    rows: &'r [AutoIndexRow],
    names: &ObjectNames,
    reference: &str,
) -> Result<&'r AutoIndexRow> {
    let not_found = || {
        SprigError::new(
            SprigCode::AutoIndexNotFound,
            format!("no automatic index {reference:?}: sprigctl index list --all shows them"),
        )
    };
    if let Ok(id) = reference.parse::<u64>() {
        return rows
            .iter()
            .find(|r| r.index_id.0 == id)
            .ok_or_else(not_found);
    }
    let parts: Vec<&str> = reference.rsplitn(3, '.').collect();
    let (name, table, db) = match parts.as_slice() {
        [n] => (*n, None, None),
        [n, t] => (*n, Some(*t), None),
        [n, t, d] => (*n, Some(*t), Some(*d)),
        _ => return Err(not_found()),
    };
    let found: Vec<&AutoIndexRow> = rows
        .iter()
        .filter(|r| r.name.eq_ignore_ascii_case(name))
        .filter(|r| {
            let (d, t) = names.of(r.object);
            table.is_none_or(|x| x.eq_ignore_ascii_case(&t))
                && db.is_none_or(|x| x.eq_ignore_ascii_case(&d))
        })
        .collect();
    // A name may have a history row (dropped) and a live one: prefer the
    // live one, then the newest.
    let live: Vec<&AutoIndexRow> = found
        .iter()
        .copied()
        .filter(|r| !r.state.is_terminal())
        .collect();
    match (live.as_slice(), found.as_slice()) {
        ([one], _) | ([], [one]) => Ok(one),
        ([], []) => Err(not_found()),
        ([], many) => many
            .iter()
            .max_by_key(|r| r.index_id)
            .copied()
            .ok_or_else(not_found),
        (many, _) => Err(invalid(format!(
            "{reference:?} names {} indexes ({}); use its id or <db>.<table>.<name>",
            many.len(),
            many.iter()
                .map(|r| {
                    let (d, t) = names.of(r.object);
                    format!("{} = {d}.{t}.{}", r.index_id, r.name)
                })
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// The `index_usage` row a `[<db>.][<table>.]<name>` reference names,
/// `None` when none does.
///
/// # Errors
/// 39792 when it names several (on several tables).
pub fn resolve_user_index(
    rows: &[UsageRow],
    names: &ObjectNames,
    reference: &str,
) -> Result<Option<UsageRow>> {
    let parts: Vec<&str> = reference.rsplitn(3, '.').collect();
    let (name, table, db) = match parts.as_slice() {
        [n] => (*n, None, None),
        [n, t] => (*n, Some(*t), None),
        [n, t, d] => (*n, Some(*t), Some(*d)),
        _ => return Ok(None),
    };
    let found: Vec<&UsageRow> = rows
        .iter()
        .filter(|r| r.index_name.eq_ignore_ascii_case(name))
        .filter(|r| {
            let (d, t) = names.of(r.object);
            table.is_none_or(|x| x.eq_ignore_ascii_case(&t))
                && db.is_none_or(|x| x.eq_ignore_ascii_case(&d))
        })
        .collect();
    match found.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some((*one).clone())),
        many => Err(invalid(format!(
            "{reference:?} names {} indexes ({}); use <db>.<table>.<name>",
            many.len(),
            many.iter()
                .map(|r| {
                    let (d, t) = names.of(r.object);
                    format!("{d}.{t}.{}", r.index_name)
                })
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// Filters `rows` by `q`.
///
/// # Errors
/// 39792 for an unknown state.
pub fn filter_rows<'r>(
    rows: &'r [AutoIndexRow],
    names: &ObjectNames,
    q: &ListIndexesQuery,
    recommendations: bool,
) -> Result<Vec<&'r AutoIndexRow>> {
    let state = q
        .state
        .as_deref()
        .map(sprig_advisor::autoindex::store::IndexState::parse)
        .transpose()
        .map_err(|_| {
            invalid(format!(
                "unknown state {:?}: validating, recommended, rolling_out, observing, active, \
                 retiring, dropping, dropped, rejected, failed or frozen",
                q.state.as_deref().unwrap_or_default()
            ))
        })?;
    let history = q.all.unwrap_or(false)
        || state.is_some_and(sprig_advisor::autoindex::store::IndexState::is_terminal);
    Ok(rows
        .iter()
        .filter(|r| history || !r.state.is_terminal())
        .filter(|r| state.is_none_or(|s| r.state == s))
        .filter(|r| !recommendations || view::is_recommendation(r))
        .filter(|r| {
            let (d, t) = names.of(r.object);
            q.db.as_deref().is_none_or(|x| x.eq_ignore_ascii_case(&d))
                && q.table
                    .as_deref()
                    .is_none_or(|x| x.eq_ignore_ascii_case(&t))
        })
        .collect())
}

/// [`AutoIndexAdmin`] over tenant catalogs.
pub struct AutoIndexService {
    directory: Directory,
    locator: Arc<dyn CatalogLocator>,
    catalog: MariaCatalog,
    live: LiveSettings,
    decisions: Arc<dyn DecisionLog>,
    clock: Arc<dyn Clock>,
    env: Option<Arc<dyn AutoIndexEnv>>,
}

impl std::fmt::Debug for AutoIndexService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoIndexService").finish_non_exhaustive()
    }
}

/// One tenant's catalog, read for a call.
struct TenantRead {
    id: TenantId,
    name: String,
    cat: TenantCatalog,
    names: ObjectNames,
    conn: Conn,
    store: AutoIndexStore,
}

impl AutoIndexService {
    /// The service: `directory` names tenants, `locator` reaches their
    /// catalogs, `catalog` reads snapshots, `live` is the etcd mirror the
    /// policy resolves with.
    #[must_use]
    pub fn new(
        directory: Directory,
        locator: Arc<dyn CatalogLocator>,
        catalog: MariaCatalog,
        live: LiveSettings,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            directory,
            locator,
            catalog,
            live,
            decisions: Arc::new(LogDecisions),
            clock,
            env: None,
        }
    }

    /// The same service reaching shards through `env` (for `restore`).
    #[must_use]
    pub fn with_env(mut self, env: Arc<dyn AutoIndexEnv>) -> Self {
        self.env = Some(env);
        self
    }

    /// Runs one statement per shard of `object` (`restore`).
    async fn on_shards(
        &self,
        t: &TenantRead,
        object: ObjectId,
        sql: impl Fn(&ShardTable) -> Result<String>,
    ) -> Result<()> {
        let env = self.env.as_ref().ok_or_else(|| {
            invalid("this controller can't reach the tenant's clusters; restore on the DC leader")
        })?;
        let envs = env.objects(t.id, &[object].into()).await?;
        let o = envs
            .get(&object)
            .ok_or_else(|| invalid(format!("table {} has no shards", object.0)))?;
        let w = o
            .view
            .nodes
            .writer()
            .ok_or_else(|| invalid("the cluster has no writer now; try again"))?;
        for sh in &o.shards {
            observe::run_ddl(w, &sql(sh)?, &o.rollout)
                .await
                .map_err(|e| {
                    SprigError::new(
                        SprigCode::AutoIndexStateInvalid,
                        format!("on {}: {e}", sh.shard),
                    )
                })?;
        }
        Ok(())
    }

    /// `act` on a user or migrated index (decision O1).
    #[allow(clippy::too_many_lines)] // plan, shard statement, state, decision
    async fn act_user(
        &self,
        t: &mut TenantRead,
        r: &UsageRow,
        action: Action,
        req: &IndexActionRequest,
    ) -> Result<IndexActionResponse> {
        let now = self.clock.wall();
        let (changed, message, to) = match view::plan_user_action(r, action) {
            UserPlan::Refuse(why) => {
                return Err(SprigError::new(SprigCode::AutoIndexStateInvalid, why));
            }
            UserPlan::Noop(m) => (false, m, r.state),
            UserPlan::Move {
                from,
                to,
                ddl,
                message,
            } => {
                let name = r.index_name.clone();
                match ddl {
                    UserDdl::None => {}
                    UserDdl::Visible => {
                        self.on_shards(t, r.object, |sh| user::ignore_user_sql(sh, &name, false))
                            .await?;
                    }
                    UserDdl::Recreate => {
                        let clause = r.definition.clone();
                        self.on_shards(t, r.object, |sh| user::add_user_sql(sh, &name, &clause))
                            .await?;
                    }
                }
                if to == UserIndexState::Serving {
                    let mut us: observe::UsageState = serde_json::from_value(
                        r.observed.get("usage").cloned().unwrap_or_default(),
                    )
                    .unwrap_or_default();
                    us.since_ms = sprig_advisor::autoindex::rollout::unix_ms(now);
                    us.reads = 0;
                    let mut o = r.observed.clone();
                    if let Some(m) = o.as_object_mut() {
                        m.insert(
                            "usage".into(),
                            serde_json::to_value(&us).unwrap_or_default(),
                        );
                    }
                    t.store
                        .set_usage_observed(&mut t.conn, r.object, &name, &o)
                        .await?;
                }
                let reason = (to == UserIndexState::Pinned).then_some("operator");
                if !t
                    .store
                    .set_usage_state(&mut t.conn, r.object, &name, from, to, reason, now)
                    .await?
                {
                    return Err(SprigError::new(
                        SprigCode::AutoIndexStateInvalid,
                        format!("{name} changed while the call ran (it was {from}); look again"),
                    ));
                }
                (true, message, to)
            }
        };
        let after = t
            .store
            .usage_rows(&mut t.conn, Some(r.object))
            .await?
            .into_iter()
            .find(|x| x.index_name == r.index_name)
            .unwrap_or_else(|| r.clone());
        if changed {
            let actor = format!(
                "op:{}",
                req.actor
                    .as_deref()
                    .filter(|a| !a.is_empty())
                    .unwrap_or("admin")
            );
            let (db, table) = t.names.of(r.object);
            let d = IndexDecision {
                tenant: t.id,
                kind: "index.restore",
                subject_type: "index",
                subject_id: format!("{}/{}", r.object, r.index_name),
                mode: "applied",
                outcome: "succeeded",
                reason_code: format!("operator:{}", action.as_str()),
                summary: format!(
                    "{actor} {} {} ({}) on {db}.{table}: {} -> {to}",
                    action.as_str(),
                    r.index_name,
                    r.origin.as_str(),
                    r.state
                ),
                evidence: serde_json::json!({"actor": actor, "reason": req.reason}),
                alternatives: None,
                at: now,
            };
            if let Err(e) = self.decisions.record(&d).await {
                tracing::warn!(error = %e, "an index decision wasn't recorded");
            }
        }
        Ok(IndexActionResponse {
            tenant: t.name.clone(),
            action: action.as_str().to_owned(),
            changed,
            from: r.state.to_string(),
            to: to.to_string(),
            message,
            index: view::user_index_view(&after, &t.names),
        })
    }

    /// The same service recording decisions in `log`.
    #[must_use]
    pub fn with_decisions(mut self, log: Arc<dyn DecisionLog>) -> Self {
        self.decisions = log;
        self
    }

    fn registry(&self) -> &'static Registry {
        self.live.registry()
    }

    fn dc(&self) -> &DcId {
        self.live.stores().dc_id()
    }

    async fn tenant(&self, name: &str) -> Result<TenantRead> {
        let e = self.directory.require(name).await?;
        let b = e.body();
        if b.home_dc != *self.dc() {
            return Err(invalid(format!(
                "tenant {name} is homed in dc {}; ask that DC's controller",
                b.home_dc
            )));
        }
        if b.state != TenantState::Active {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {name} is {}", b.state.as_str()),
            ));
        }
        let id = b.tenant_id;
        let snap = self.catalog.backend().full_snapshot(id).await?;
        let cat = TenantCatalog::build(self.registry(), id, Loaded::core(snap));
        let mut names = BTreeMap::new();
        for o in cat.objects.iter() {
            let db = cat
                .databases
                .get(o.db_id)
                .map_or_else(|| format!("#{}", o.db_id), |d| d.name.clone());
            names.insert(o.object_id, (db, o.name.clone()));
        }
        let ep = self.locator.writer(id).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|err| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(err)
        })?;
        Migrator::new(&[AUTOINDEX_MODULE])?
            .run(&mut conn, ep.store.schema(), &MigrateOpts::default())
            .await?;
        Ok(TenantRead {
            id,
            name: name.to_owned(),
            cat,
            names: ObjectNames(names),
            conn,
            store: AutoIndexStore::new(ep.store.schema()),
        })
    }

    fn policy(&self, t: &TenantRead, object: sprig_common::ids::ObjectId) -> Option<ObjectPolicy> {
        let row = t.cat.objects.get(object)?;
        let etcd = self.live.snapshot();
        let dc = self.dc().clone();
        let ctx = t
            .cat
            .setting_ctx(&dc)
            .with_db(row.db_id)
            .with_object(object);
        let src = Layered {
            etcd: &etcd,
            tenant: Some(&t.cat.settings),
        };
        match ObjectPolicy::resolve(&src, &ctx) {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!(error = %e, tenant = %t.id, %object, "auto_index policy unresolved");
                None
            }
        }
    }

    /// Logs an operator's action as a decision.
    async fn record(
        &self,
        t: &TenantRead,
        before: &AutoIndexRow,
        after: &AutoIndexRow,
        action: Action,
        req: &IndexActionRequest,
        at: SystemTime,
    ) {
        let actor = format!(
            "op:{}",
            req.actor
                .as_deref()
                .filter(|a| !a.is_empty())
                .unwrap_or("admin")
        );
        let (db, table) = t.names.of(before.object);
        let d = IndexDecision {
            tenant: t.id,
            kind: match action {
                Action::Approve => "index.approve",
                Action::Reject => "index.reject",
                Action::Drop => "index.drop",
                Action::Pin => "index.pin",
                Action::Unpin => "index.unpin",
                Action::Restore => "index.restore",
            },
            subject_type: "index",
            subject_id: format!("{}/{}", before.object, before.name),
            mode: "applied",
            outcome: "succeeded",
            reason_code: format!("operator:{}", action.as_str()),
            summary: format!(
                "{actor} {} {} on {db}.{table}: {} -> {}",
                action.as_str(),
                before.name,
                before.state,
                after.state
            ),
            evidence: serde_json::json!({
                "actor": actor,
                "reason": req.reason,
                "from": before.state.as_str(),
                "to": after.state.as_str(),
            }),
            alternatives: None,
            at,
        };
        if let Err(e) = self.decisions.record(&d).await {
            tracing::warn!(error = %e, "an index decision wasn't recorded");
        }
    }

    async fn candidate_of(t: &mut TenantRead, r: &AutoIndexRow) -> Result<Option<CandidateRow>> {
        t.store.candidate(&mut t.conn, r.candidate_id).await
    }
}

#[async_trait::async_trait]
#[allow(clippy::too_many_lines)] // `act`: every plan, restore included
impl AutoIndexAdmin for AutoIndexService {
    async fn list(&self, tenant: &str, q: &ListIndexesQuery) -> Result<ListIndexesResponse> {
        let mut t = self.tenant(tenant).await?;
        let rows = t.store.list_indexes(&mut t.conn, true).await?;
        let picked = filter_rows(&rows, &t.names, q, false)?;
        let mut indexes: Vec<_> = picked
            .into_iter()
            .map(|r| view::index_view(r, &t.names, None))
            .collect();
        // User and migrated indexes under decision O1: those whose removal
        // is under way, recommended, done (with `all`) or pinned.
        let all = q.all.unwrap_or(false);
        for r in t.store.usage_rows(&mut t.conn, None).await? {
            let shown = match r.state {
                UserIndexState::Serving => false,
                UserIndexState::Dropped => all || q.state.as_deref() == Some("dropped"),
                _ => true,
            };
            let (d, tb) = t.names.of(r.object);
            if shown
                && q.state.as_deref().is_none_or(|s| s == r.state.as_str())
                && q.db.as_deref().is_none_or(|x| x.eq_ignore_ascii_case(&d))
                && q.table
                    .as_deref()
                    .is_none_or(|x| x.eq_ignore_ascii_case(&tb))
            {
                indexes.push(view::user_index_view(&r, &t.names));
            }
        }
        Ok(ListIndexesResponse {
            tenant: t.name.clone(),
            indexes,
        })
    }

    async fn show(&self, tenant: &str, index: &str) -> Result<IndexDetail> {
        let mut t = self.tenant(tenant).await?;
        let rows = t.store.list_indexes(&mut t.conn, true).await?;
        let r = resolve_index(&rows, &t.names, index)?.clone();
        let cand = Self::candidate_of(&mut t, &r).await?;
        let validations = t.store.validations_of(&mut t.conn, r.candidate_id).await?;
        Ok(IndexDetail {
            tenant: t.name.clone(),
            index: view::index_view(&r, &t.names, cand.as_ref()),
            candidate: cand.as_ref().map(|c| view::candidate_view(c, &t.names)),
            validations: validations.iter().map(view::validation_view).collect(),
            history: view::history(&r, cand.as_ref(), &validations),
        })
    }

    async fn act(
        &self,
        tenant: &str,
        index: &str,
        action: Action,
        req: &IndexActionRequest,
    ) -> Result<IndexActionResponse> {
        let mut t = self.tenant(tenant).await?;
        let rows = t.store.list_indexes(&mut t.conn, true).await?;
        let r = match resolve_index(&rows, &t.names, index) {
            Ok(r) => r.clone(),
            Err(e) if e.code() == Some(SprigCode::AutoIndexNotFound) => {
                let users = t.store.usage_rows(&mut t.conn, None).await?;
                let Some(u) = resolve_user_index(&users, &t.names, index)? else {
                    return Err(e);
                };
                return self.act_user(&mut t, &u, action, req).await;
            }
            Err(e) => return Err(e),
        };
        let now = self.clock.wall();
        let conflict = || {
            SprigError::new(
                SprigCode::AutoIndexStateInvalid,
                format!(
                    "{} changed while the call ran (it was {}); look again with sprigctl index show",
                    r.name, r.state
                ),
            )
        };
        let (changed, message) = match view::plan_action(&r, action) {
            Plan::Refuse(why) => {
                return Err(SprigError::new(SprigCode::AutoIndexStateInvalid, why));
            }
            Plan::Noop(m) => (false, m),
            Plan::Move {
                from,
                to,
                drop_reason,
                message,
            } => {
                let ok = t
                    .store
                    .set_state(
                        &mut t.conn,
                        r.index_id,
                        &StateChange {
                            from,
                            to,
                            at: now,
                            drop_reason,
                        },
                    )
                    .await?;
                if !ok {
                    return Err(conflict());
                }
                (true, message)
            }
            Plan::Unfreeze => match t.store.unfreeze(&mut t.conn, r.index_id, now).await? {
                Some(s) => (true, format!("unpinned; back to {s}")),
                None => return Err(conflict()),
            },
            Plan::Restore => {
                // Out of retiring first, so the observer can't drop it
                // meanwhile; then visible; then pinned. A failed statement
                // leaves it active, and the drift reconciler makes it
                // visible.
                let change = |from, to| StateChange {
                    from,
                    to,
                    at: now,
                    drop_reason: None,
                };
                if !t
                    .store
                    .set_state(
                        &mut t.conn,
                        r.index_id,
                        &change(IndexState::Retiring, IndexState::Active),
                    )
                    .await?
                {
                    return Err(conflict());
                }
                let name = r.name.clone();
                self.on_shards(&t, r.object, |sh| ignore_sql(sh, &name, false))
                    .await?;
                if !t
                    .store
                    .set_state(
                        &mut t.conn,
                        r.index_id,
                        &change(IndexState::Active, IndexState::Frozen),
                    )
                    .await?
                {
                    return Err(conflict());
                }
                (
                    true,
                    "made visible again (NOT IGNORED) and pinned: never removed until \
                     sprigctl index unpin"
                        .to_owned(),
                )
            }
        };
        let after = t
            .store
            .index(&mut t.conn, r.index_id)
            .await?
            .ok_or_else(|| SprigError::internal("an index row vanished during an action"))?;
        if changed {
            self.record(&t, &r, &after, action, req, now).await;
        }
        Ok(IndexActionResponse {
            tenant: t.name.clone(),
            action: action.as_str().to_owned(),
            changed,
            from: r.state.to_string(),
            to: after.state.to_string(),
            message,
            index: view::index_view(&after, &t.names, None),
        })
    }

    async fn explain_query(&self, tenant: &str, fingerprint: &str) -> Result<QueryExplanation> {
        let fp: Fingerprint = fingerprint
            .parse()
            .map_err(|e: sprig_common::ids::IdParseError| invalid(e.to_string()))?;
        let mut t = self.tenant(tenant).await?;
        let since = self
            .clock
            .wall()
            .checked_sub(EXPLAIN_WINDOW)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let candidates = t.store.candidates_serving(&mut t.conn, fp, since).await?;
        let mut ev = Evidence {
            candidates: &candidates,
            ..Evidence::default()
        };
        for c in &candidates {
            if c.is_skip() {
                continue;
            }
            let v = t.store.validations_of(&mut t.conn, c.candidate_id).await?;
            if !v.is_empty() {
                ev.validations.insert(c.candidate_id, v);
            }
            let i = t
                .store
                .indexes_of_candidate(&mut t.conn, c.candidate_id)
                .await?;
            if !i.is_empty() {
                ev.indexes.insert(c.candidate_id, i);
            }
        }
        for c in &candidates {
            if !ev.policies.contains_key(&c.object)
                && let Some(p) = self.policy(&t, c.object)
            {
                ev.policies.insert(c.object, p);
            }
        }
        Ok(view::explain(&t.name, fp, since, &ev, &t.names))
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod live_tests;
