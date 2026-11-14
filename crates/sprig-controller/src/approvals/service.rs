//! The Admin API's `Recommendations` service:
//! list, get, act, bulk, policy and email subscriptions.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_approvals::catalog::{CatalogRecStore, Subscription};
use sprig_approvals::gate::ActKnobs;
use sprig_approvals::model::{Level, RecId, Recommendation, State};
use sprig_approvals::policy::level_for;
use sprig_approvals::store::{Filter, RecStore};
use sprig_approvals::{Act, Caller, Kind, may_act, may_see, settings as keys};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::ScopeKind;

use super::ControllerApprovals;
use crate::admin::invalid;
use crate::admin::types::SettingContextQuery;
use crate::settings_admin::SettingsAdmin;
use crate::tree::TenantState;

/// `GET /admin/v1/recommendations`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListQuery {
    /// One tenant (its catalog); otherwise every tenant homed here and the
    /// DC tier.
    #[serde(default)]
    pub tenant: Option<String>,
    /// `table`, `database`, `tenant`, `dc` or `platform`.
    #[serde(default)]
    pub level: Option<String>,
    /// A kind or family.
    #[serde(default)]
    pub kind: Option<String>,
    /// One state.
    #[serde(default)]
    pub state: Option<String>,
    /// Closed items too (`true`).
    #[serde(default)]
    pub all: Option<String>,
    /// A database name.
    #[serde(default)]
    pub db: Option<String>,
    /// A table name.
    #[serde(default)]
    pub table: Option<String>,
    /// Only items waiting at least this many hours.
    #[serde(default)]
    pub min_age_hours: Option<String>,
    /// At most this many (500).
    #[serde(default)]
    pub limit: Option<String>,
    /// The gateway's caller claim, as JSON (or the `x-sprig-caller`
    /// header).
    #[serde(default)]
    pub caller: Option<String>,
}

/// One item as the API shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecView {
    /// The item.
    #[serde(flatten)]
    pub rec: Recommendation,
    /// Hours since it was raised.
    pub age_hours: f64,
    /// Whether the caller may act on it.
    pub may_act: bool,
}

/// `ListRecommendations`'s answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListResponse {
    /// Newest first.
    pub items: Vec<RecView>,
    /// Open items in the list (for the inbox badge).
    pub open: usize,
}

/// An action's body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActRequest {
    /// The action.
    #[serde(flatten)]
    pub act: Act,
    /// The caller, accepted only on the gateway credential.
    #[serde(default)]
    pub caller: Option<Caller>,
    /// Who, on the admin and DC operator credentials.
    #[serde(default)]
    pub actor: Option<String>,
}

/// `BulkAct`'s body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BulkRequest {
    /// Up to 200 ids.
    pub ids: Vec<String>,
    /// The action.
    #[serde(flatten)]
    pub act: Act,
    /// As in [`ActRequest`].
    #[serde(default)]
    pub caller: Option<Caller>,
    /// As in [`ActRequest`].
    #[serde(default)]
    pub actor: Option<String>,
}

/// One id's result in a bulk action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BulkResult {
    /// The id.
    pub id: String,
    /// The new state, on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<State>,
    /// The error, on failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One kind's policy at a scope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KindPolicy {
    /// The kind.
    pub kind: String,
    /// The mode key.
    pub mode_key: String,
    /// Its value.
    pub mode: String,
    /// Where the value came from.
    pub mode_source: String,
    /// The level a new item goes to.
    pub level: Level,
    /// `approvals.escalate_after` for the kind.
    pub escalate_after: Option<String>,
    /// `approvals.auto_apply_after` for the kind.
    pub auto_apply_after: Option<String>,
}

/// `GetApprovalPolicy`'s answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyResponse {
    /// The scope it was resolved at.
    pub context: SettingContextQuery,
    /// Every kind.
    pub kinds: Vec<KindPolicy>,
    /// The `approvals.*` keys: `(value, source)`.
    pub settings: std::collections::BTreeMap<String, (serde_json::Value, String)>,
}

/// The service.
pub struct RecommendationsService {
    ctl: Arc<ControllerApprovals>,
    settings: Option<Arc<dyn SettingsAdmin>>,
}

impl std::fmt::Debug for RecommendationsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecommendationsService")
            .finish_non_exhaustive()
    }
}

fn flag(s: Option<&String>) -> bool {
    s.is_some_and(|v| matches!(v.as_str(), "true" | "1" | "yes" | "on"))
}

fn source_kind(s: &str) -> ScopeKind {
    match s.split_whitespace().next().unwrap_or_default() {
        "object" => ScopeKind::Object,
        "database" => ScopeKind::Database,
        "tenant" => ScopeKind::Tenant,
        "ancestor" => ScopeKind::Ancestor(TenantId(0)),
        "cluster" => ScopeKind::Cluster,
        "dc" => ScopeKind::Dc,
        "global" => ScopeKind::Global,
        _ => ScopeKind::Default,
    }
}

impl RecommendationsService {
    /// Over `ctl`; `settings` resolves policies.
    #[must_use]
    pub fn new(ctl: Arc<ControllerApprovals>, settings: Option<Arc<dyn SettingsAdmin>>) -> Self {
        Self { ctl, settings }
    }

    /// The approvals.
    #[must_use]
    pub fn ctl(&self) -> &Arc<ControllerApprovals> {
        &self.ctl
    }

    async fn tenant_id(&self, name: &str) -> Result<TenantId> {
        let e = self.ctl.directory().require(name).await?;
        if e.body().home_dc != *self.ctl.dc() {
            return Err(invalid(format!(
                "tenant {name} is homed in dc {}; ask that DC's controller",
                e.body().home_dc
            )));
        }
        Ok(e.body().tenant_id)
    }

    fn filter(q: &ListQuery) -> Result<Filter> {
        let mut f = Filter {
            all: flag(q.all.as_ref()),
            ..Filter::default()
        };
        if let Some(s) = &q.state {
            f.states =
                vec![State::parse(s).ok_or_else(|| invalid(format!("unknown state {s:?}")))?];
        }
        if let Some(l) = &q.level {
            f.level = Some(Level::parse(l).ok_or_else(|| invalid(format!("unknown level {l:?}")))?);
        }
        if let Some(k) = &q.kind {
            if !sprig_approvals::store::known_kind(k) {
                return Err(invalid(format!("unknown kind {k:?}")));
            }
            f.kinds = vec![k.clone()];
        }
        if let Some(n) = &q.limit {
            f.limit = n.parse().map_err(|_| invalid("limit is a number"))?;
        }
        Ok(f)
    }

    /// `ListRecommendations`.
    ///
    /// # Errors
    /// 39792 for a bad filter; 39072 for an unknown tenant; store errors.
    pub async fn list(&self, caller: &Caller, q: &ListQuery) -> Result<ListResponse> {
        let f = Self::filter(q)?;
        let now = self.ctl.hub().now();
        let mut stores: Vec<Arc<dyn RecStore>> = Vec::new();
        if let Some(name) = &q.tenant {
            stores.push(self.ctl.hub().stores().tenant(self.tenant_id(name).await?));
        } else {
            if !caller.platform && caller.dc_operator.is_none() {
                return Err(invalid("name a tenant"));
            }
            for e in self.ctl.directory().list().await? {
                let b = e.body();
                if b.home_dc == *self.ctl.dc() && b.state == TenantState::Active {
                    stores.push(self.ctl.hub().stores().tenant(b.tenant_id));
                }
            }
            if let Some(s) = self.ctl.hub().stores().dc(self.ctl.dc()) {
                stores.push(s);
            }
            if caller.platform
                && let Some(s) = self.ctl.hub().stores().global()
            {
                stores.push(s);
            }
        }
        let min_age = q
            .min_age_hours
            .as_deref()
            .map(str::parse::<f64>)
            .transpose()
            .map_err(|_| invalid("min_age_hours is a number"))?;
        let mut items = Vec::new();
        for s in stores {
            let rows = match s.list(&f).await {
                Ok(r) => r,
                Err(e) if q.tenant.is_none() => {
                    tracing::debug!(error = %e, "recommendations: a store failed to list");
                    continue;
                }
                Err(e) => return Err(e),
            };
            for r in rows {
                if q.db.as_ref().is_some_and(|d| {
                    r.subject
                        .db_name
                        .as_ref()
                        .is_none_or(|x| !x.eq_ignore_ascii_case(d))
                }) {
                    continue;
                }
                if q.table.as_ref().is_some_and(|t| {
                    r.subject
                        .object_name
                        .as_ref()
                        .is_none_or(|x| !x.eq_ignore_ascii_case(t))
                }) {
                    continue;
                }
                if min_age.is_some_and(|m| r.age_hours(now) < m) {
                    continue;
                }
                let anc = match r.subject.tenant {
                    Some(t) => self.ctl.ancestry(t).await,
                    None => Vec::new(),
                };
                if !may_see(&r, caller, &anc) {
                    continue;
                }
                let act = may_act(&r, caller, &anc);
                items.push(RecView {
                    age_hours: r.age_hours(now),
                    may_act: act,
                    rec: r,
                });
            }
        }
        items.sort_by(|a, b| b.rec.created_at_us.cmp(&a.rec.created_at_us));
        items.truncate(f.limit());
        let open = items.iter().filter(|i| i.rec.state == State::Open).count();
        Ok(ListResponse { items, open })
    }

    /// `GetRecommendation`.
    ///
    /// # Errors
    /// 39972 when missing or not visible to the caller.
    pub async fn get(&self, caller: &Caller, id: &str) -> Result<RecView> {
        let id = RecId::parse(id)?;
        let r = self.ctl.hub().get(&id).await?;
        let anc = match r.subject.tenant {
            Some(t) => self.ctl.ancestry(t).await,
            None => Vec::new(),
        };
        if !may_see(&r, caller, &anc) {
            return Err(sprig_approvals::store::not_found(&id));
        }
        Ok(RecView {
            age_hours: r.age_hours(self.ctl.hub().now()),
            may_act: may_act(&r, caller, &anc),
            rec: r,
        })
    }

    /// `ActOnRecommendation`.
    ///
    /// # Errors
    /// 39972-39977; store errors.
    pub async fn act(&self, caller: &Caller, id: &str, act: &Act) -> Result<RecView> {
        let id = RecId::parse(id)?;
        let r = self.ctl.hub().get(&id).await?;
        let knobs = self.knobs(&r).await;
        let anc = match r.subject.tenant {
            Some(t) => self.ctl.ancestry(t).await,
            None => Vec::new(),
        };
        let r = self.ctl.hub().act(&id, act, caller, &anc, knobs).await?;
        Ok(RecView {
            age_hours: r.age_hours(self.ctl.hub().now()),
            may_act: true,
            rec: r,
        })
    }

    /// `BulkAct`.
    ///
    /// # Errors
    /// 39792 for more than 200 ids.
    pub async fn bulk(
        &self,
        caller: &Caller,
        ids: &[String],
        act: &Act,
    ) -> Result<Vec<BulkResult>> {
        if ids.len() > 200 {
            return Err(invalid("at most 200 ids per bulk action"));
        }
        let mut out = Vec::new();
        for id in ids {
            out.push(match self.act(caller, id, act).await {
                Ok(v) => BulkResult {
                    id: id.clone(),
                    state: Some(v.rec.state),
                    error: None,
                },
                Err(e) => BulkResult {
                    id: id.clone(),
                    state: None,
                    error: Some(format!(
                        "{}: {}",
                        e.code().map_or(0, SprigCode::number),
                        e.message
                    )),
                },
            });
        }
        Ok(out)
    }

    async fn knobs(&self, r: &Recommendation) -> ActKnobs {
        let p = self
            .ctl
            .policy(
                r.kind,
                r.subject.tenant,
                r.subject.db,
                r.subject.object,
                r.subject.cluster.as_ref(),
            )
            .await;
        ActKnobs {
            ignore_once_days: p.ignore_once_days,
            reraise_factor: p.reraise_factor,
        }
    }

    /// `GetApprovalPolicy`: every kind's mode, level and timers at a scope.
    ///
    /// # Errors
    /// The settings service's (unknown tenant, database or object).
    pub async fn policy(&self, ctx: &SettingContextQuery) -> Result<PolicyResponse> {
        let s = self
            .settings
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't serve settings"))?;
        let mut settings = std::collections::BTreeMap::new();
        for k in [
            keys::START_LEVEL.key,
            keys::ESCALATE_AFTER.key,
            keys::AUTO_APPLY_AFTER.key,
            keys::AUTO_APPLY_FORBIDDEN.key,
            keys::ALLOW_EMERGENCY.key,
            keys::RERAISE_FACTOR.key,
            keys::IGNORE_ONCE_DAYS.key,
            keys::REMINDER.key,
            keys::DIGEST_HOUR_UTC.key,
        ] {
            if let Ok(x) = s.explain(k, ctx).await {
                settings.insert(
                    k.to_owned(),
                    (serde_json::to_value(&x.value).unwrap_or_default(), x.source),
                );
            }
        }
        let text = |k: &str| {
            settings
                .get(k)
                .and_then(|(v, _)| v.as_str().map(str::to_owned))
                .unwrap_or_default()
        };
        let start = text(keys::START_LEVEL.key);
        let esc = sprig_approvals::policy::TimerSpec::parse(&text(keys::ESCALATE_AFTER.key))
            .unwrap_or_default();
        let auto = sprig_approvals::policy::TimerSpec::parse(&text(keys::AUTO_APPLY_AFTER.key))
            .unwrap_or_default();
        let forbidden = text(keys::AUTO_APPLY_FORBIDDEN.key);
        let show = |d: Option<std::time::Duration>| d.map(|d| format!("{}m", d.as_secs() / 60));
        let mut kinds = Vec::new();
        for k in Kind::ALL {
            let (mode, source) = match s.explain(k.mode_key(), ctx).await {
                Ok(x) => (
                    match &x.value {
                        sprig_settings::Value::Str(t) => t.clone(),
                        v => sprig_settings::show_value(v),
                    },
                    x.source,
                ),
                Err(_) => ("on".into(), "default".into()),
            };
            kinds.push(KindPolicy {
                kind: k.as_str().into(),
                mode_key: k.mode_key().into(),
                level: level_for(*k, source_kind(&source), &start),
                mode,
                mode_source: source,
                escalate_after: show(esc.for_kind(*k)),
                auto_apply_after: if sprig_approvals::policy::kind_listed(&forbidden, *k) {
                    None
                } else {
                    show(auto.for_kind(*k))
                },
            });
        }
        Ok(PolicyResponse {
            context: ctx.clone(),
            kinds,
            settings,
        })
    }

    /// `ListSubscriptions` for a tenant.
    ///
    /// # Errors
    /// 39072 for an unknown tenant; catalog errors.
    pub async fn subscriptions(&self, tenant: &str) -> Result<Vec<Subscription>> {
        let t = self.tenant_id(tenant).await?;
        let st = self.catalog(t);
        let (mut c, recs) = st.open().await?;
        match recs.subscriptions(&mut c).await {
            Err(e) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1146, .. }) => {
                Ok(Vec::new())
            }
            r => r,
        }
    }

    /// `SetSubscription`.
    ///
    /// # Errors
    /// 39792 for a bad address or delivery; catalog errors.
    pub async fn set_subscription(&self, tenant: &str, s: &Subscription) -> Result<()> {
        if !s.email.contains('@') || s.email.len() > 254 || s.email.contains(['\r', '\n', ' ']) {
            return Err(invalid(format!("{:?} is not an email address", s.email)));
        }
        if !matches!(s.delivery.as_str(), "immediate" | "digest" | "off") {
            return Err(invalid("delivery is immediate, digest or off"));
        }
        let t = self.tenant_id(tenant).await?;
        let st = self.catalog(t);
        // Make sure the module is migrated (the first raise does it too).
        let _ = st
            .list(&Filter {
                limit: 1,
                ..Filter::default()
            })
            .await?;
        let (mut c, recs) = st.open().await?;
        recs.set_subscription(&mut c, s, self.ctl.hub().now()).await
    }

    fn catalog(&self, t: TenantId) -> CatalogRecStore {
        CatalogRecStore::new(self.ctl.locator(), t)
    }
}

/// `POST /admin/v1/recommendations:gate`: a gateway asks the approval
/// gate for a decision it makes itself (`vector.ann`: an automatic ANN
/// graph), or reports the action it was let through.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // one request, read once
pub enum GateRequest {
    /// Record or refresh the proposal; may it act now?
    Check {
        /// The kind (`vector.ann`).
        kind: String,
        /// The tenant's id.
        tenant: u64,
        /// The database's name.
        db: String,
        /// What changes (unique per index).
        target: String,
        /// What would be done, for people.
        action: String,
        /// Its machine form.
        #[serde(default)]
        machine: serde_json::Value,
        /// The numbers behind it.
        #[serde(default)]
        evidence: serde_json::Value,
        /// One line for people.
        summary: String,
        /// Grows with the evidence.
        #[serde(default)]
        score: f64,
    },
    /// The action a `Check` let through finished.
    Finished {
        /// The ticket `Check` returned.
        ticket: sprig_approvals::Ticket,
        /// Whether it worked.
        ok: bool,
        /// What happened.
        #[serde(default)]
        detail: String,
    },
}

/// The answer to a [`GateRequest::Check`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateReply {
    /// Act now.
    pub go: bool,
    /// Report the result with this ticket (when an item tracks it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<sprig_approvals::Ticket>,
    /// Why not, naming the item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
}

impl RecommendationsService {
    /// [`GateRequest`].
    ///
    /// # Errors
    /// 39792 for a kind gateways don't decide, or an unknown database;
    /// catalog errors.
    pub async fn gate(&self, req: GateRequest) -> Result<GateReply> {
        use sprig_approvals::{Action, Mode, Proposal, Verdict};
        let (kind, tenant, db, target, action, machine, evidence, summary, score) = match req {
            GateRequest::Finished { ticket, ok, detail } => {
                self.ctl.finished(&ticket, ok, &detail).await;
                return Ok(GateReply {
                    go: ok,
                    ticket: None,
                    why: None,
                });
            }
            GateRequest::Check {
                kind,
                tenant,
                db,
                target,
                action,
                machine,
                evidence,
                summary,
                score,
            } => (
                kind, tenant, db, target, action, machine, evidence, summary, score,
            ),
        };
        let kind = Kind::parse(&kind)
            .filter(|k| *k == Kind::VectorAnn)
            .ok_or_else(|| invalid(format!("{kind}: gateways ask the gate for vector.ann only")))?;
        let t = TenantId(tenant);
        let db_id = self.db_id(t, &db).await?;
        // A gateway asks once an index is due, at most once a minute: read
        // the tenant's rows as of now, so an `approve` set a moment ago
        // isn't answered with the old `on`.
        self.ctl.refresh_tenant(t).await;
        let policy = self
            .ctl
            .policy(kind, Some(t), Some(db_id), None, None)
            .await;
        if policy.mode == Mode::On {
            return Ok(GateReply {
                go: true,
                ticket: None,
                why: None,
            });
        }
        if policy.mode == Mode::Off {
            return Ok(GateReply {
                go: false,
                ticket: None,
                why: Some("off".into()),
            });
        }
        let mut subject = self.ctl.subject(t).await;
        subject.db = Some(db_id);
        subject.db_name = Some(db);
        let p = Proposal {
            kind,
            subject,
            target: sprig_approvals::model::clip(&format!("t{tenant}/{target}"), 255),
            action: Action {
                text: action,
                machine,
            },
            evidence,
            score,
            summary,
            emergency: false,
        };
        Ok(match self.ctl.gate(&p, &policy).await {
            Verdict::Proceed(t) => GateReply {
                go: true,
                ticket: t.id.is_some().then_some(t),
                why: None,
            },
            Verdict::Wait(id) => GateReply {
                go: false,
                ticket: None,
                why: Some(format!("held for approval as {id}")),
            },
            Verdict::Advice(id) => GateReply {
                go: false,
                ticket: None,
                why: Some(format!("recommended as {id}")),
            },
            Verdict::Suppressed(id) => GateReply {
                go: false,
                ticket: None,
                why: Some(format!("ignored ({id})")),
            },
            Verdict::Off => GateReply {
                go: false,
                ticket: None,
                why: Some("off".into()),
            },
        })
    }

    /// The id of database `name` of `t`, from its catalog.
    async fn db_id(&self, t: TenantId, name: &str) -> Result<sprig_common::ids::DbId> {
        use mysql_async::prelude::Queryable;
        let ep = self.ctl.locator().writer(t).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
        let id: Option<u32> = c
            .exec_first(
                format!(
                    "SELECT db_id FROM {}.`databases` WHERE name_key = ? AND state = 'active'",
                    sprig_common::names::quote(&schema)
                ),
                (name.to_lowercase(),),
            )
            .await
            .map_err(|e| SprigError::internal("databases").with_source(e))?;
        id.map(sprig_common::ids::DbId)
            .ok_or_else(|| invalid(format!("no database {name}")))
    }
}

/// 39974 for callers without a right to the operation.
#[must_use]
pub fn denied(what: &str) -> SprigError {
    SprigError::new(SprigCode::ApprovalDenied, format!("not allowed: {what}"))
}
