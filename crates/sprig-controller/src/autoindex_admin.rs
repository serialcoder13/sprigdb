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
use sprig_advisor::autoindex::policy::{Layered, ObjectPolicy};
use sprig_advisor::autoindex::store::{
    AUTOINDEX_MODULE, AutoIndexRow, AutoIndexStore, CandidateRow, DecisionLog, IndexDecision,
    LogDecisions, StateChange,
};
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::ddl::backend::DdlBackend;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{CatalogLocator, MariaCatalog, Migrator};
use sprig_common::ids::{DcId, Fingerprint, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{LiveSettings, Registry};

use crate::admin::invalid;
use crate::tree::{Directory, TenantState};
use view::{
    Action, Evidence, IndexActionRequest, IndexActionResponse, IndexDetail, ListIndexesResponse,
    ObjectNames, Plan, QueryExplanation,
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
        }
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
impl AutoIndexAdmin for AutoIndexService {
    async fn list(&self, tenant: &str, q: &ListIndexesQuery) -> Result<ListIndexesResponse> {
        let mut t = self.tenant(tenant).await?;
        let rows = t.store.list_indexes(&mut t.conn, true).await?;
        let picked = filter_rows(&rows, &t.names, q, false)?;
        Ok(ListIndexesResponse {
            tenant: t.name.clone(),
            indexes: picked
                .into_iter()
                .map(|r| view::index_view(r, &t.names, None))
                .collect(),
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
        let r = resolve_index(&rows, &t.names, index)?.clone();
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
