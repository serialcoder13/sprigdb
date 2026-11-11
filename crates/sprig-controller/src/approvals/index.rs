//! Automatic indexing under `auto_index.mode = approve` (chapter 52,
//! "Appliers"; chapter 13). The evaluator and the loop park a selected
//! candidate as a `recommended` row, as under `recommend`; this producer
//! raises and refreshes an item for every such row whose object's mode is
//! `approve` or `recommend`, and an approval rolls the row out through
//! the same transition as `sprigctl index approve`, so the rollout keeps
//! its build windows, slots and shard locks.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_approvals::model::{Action, Proposal, Recommendation};
use sprig_approvals::settings as keys;
use sprig_approvals::{Kind, Mode, Policy, Verdict};
use sprig_common::Result;
use sprig_common::ids::{ObjectId, TenantId};

use super::{Applier, ControllerApprovals, Producer};
use crate::autoindex_admin::view::{Action as IndexAction, IndexActionRequest};
use crate::autoindex_admin::{AutoIndexAdmin, AutoIndexService};

/// How often a tenant's `recommended` rows are re-read.
pub const SYNC_EVERY: Duration = Duration::from_secs(120);

/// The producer and applier of `index.create`.
pub struct IndexApprovals {
    svc: Arc<AutoIndexService>,
    last: Mutex<BTreeMap<TenantId, Instant>>,
}

impl std::fmt::Debug for IndexApprovals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexApprovals").finish_non_exhaustive()
    }
}

impl IndexApprovals {
    /// Over the Admin API's automatic-indexing service.
    #[must_use]
    pub fn new(svc: Arc<AutoIndexService>) -> Self {
        Self {
            svc,
            last: Mutex::new(BTreeMap::new()),
        }
    }

    fn reference(rec: &Recommendation) -> Option<(String, String)> {
        let id = rec.action.machine.get("index_id")?.as_u64()?;
        let t = rec.subject.tenant_name.clone()?;
        Some((t, id.to_string()))
    }
}

const KEYS: &[&str] = &[
    "auto_index.mode",
    "approvals.start_level",
    "approvals.escalate_after",
    "approvals.auto_apply_after",
    "approvals.auto_apply_forbidden",
    "approvals.allow_emergency",
    "approvals.reraise_factor",
    "approvals.ignore_once_days",
];

#[async_trait::async_trait]
impl Producer for IndexApprovals {
    async fn sync(&self, ctl: &ControllerApprovals, tenant: TenantId) -> Result<()> {
        {
            let mut g = self.last.lock().unwrap_or_else(PoisonError::into_inner);
            if g.get(&tenant).is_some_and(|t| t.elapsed() < SYNC_EVERY) {
                return Ok(());
            }
            g.insert(tenant, Instant::now());
        }
        let _ = keys::START_LEVEL.key; // the KEYS above are these settings
        let held = self.svc.held(tenant, KEYS).await?;
        for h in held {
            let v = &h.view;
            if v.kind.starts_with("advice") {
                continue; // advice is never applied
            }
            let lookup = |k: &str| h.settings.get(k).cloned();
            let policy = Policy::resolve(Kind::IndexCreate, &lookup);
            if !matches!(policy.mode, Mode::Approve | Mode::Recommend) {
                continue; // `on`: parked for another reason (a large table)
            }
            let mut subject = ctl.subject(tenant).await;
            subject.db = h.db_id;
            subject.db_name = Some(v.db.clone());
            subject.object = Some(ObjectId(v.object_id));
            subject.object_name = Some(v.table.clone());
            let text = v.ddl.clone().unwrap_or_else(|| {
                format!(
                    "ALTER TABLE {}.{} ADD INDEX {} ({})",
                    v.db,
                    v.table,
                    v.name,
                    v.columns.join(", ")
                )
            });
            let p = Proposal {
                kind: Kind::IndexCreate,
                subject,
                target: format!("t{tenant}/o{}/{}", v.object_id, v.name),
                action: Action {
                    text,
                    machine: serde_json::json!({
                        "index_id": v.index_id,
                        "index": v.name,
                        "model": v.model,
                        "index_kind": v.kind,
                        "columns": v.columns,
                    }),
                },
                evidence: serde_json::json!({
                    "est_benefit_ms_h": v.est_benefit_ms_h,
                    "est_write_ms_h": v.est_write_ms_h,
                    "est_net_ms_h": v.est_net_ms_h,
                    "est_bytes": v.est_bytes,
                    "fingerprints": v.fingerprints.len(),
                    "candidate_id": v.candidate_id,
                }),
                score: v.est_net_ms_h.max(0.0),
                summary: format!(
                    "{} index {} on {}.{} ({}): saves about {:.0} ms/h, costs {:.0} ms/h of writes",
                    v.kind,
                    v.name,
                    v.db,
                    v.table,
                    v.columns.join(", "),
                    v.est_benefit_ms_h,
                    v.est_write_ms_h
                ),
                emergency: false,
            };
            if let Verdict::Proceed(ticket) = ctl.gate(&p, &policy).await {
                let req = IndexActionRequest {
                    reason: "approved in the approvals inbox".into(),
                    actor: Some(format!(
                        "approvals:{}",
                        ticket
                            .id
                            .as_ref()
                            .map_or_else(String::new, ToString::to_string)
                    )),
                };
                if let Err(e) = self
                    .svc
                    .act(
                        &h.tenant_name,
                        &v.index_id.to_string(),
                        IndexAction::Approve,
                        &req,
                    )
                    .await
                {
                    ctl.finished(&ticket, false, &e.message).await;
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Applier for IndexApprovals {
    async fn revalidate(&self, rec: &Recommendation) -> Result<Option<String>> {
        let Some((t, id)) = Self::reference(rec) else {
            return Ok(Some("the item names no index".into()));
        };
        match self.svc.show(&t, &id).await {
            Ok(d) if d.index.state == "recommended" => Ok(None),
            Ok(d) => Ok(Some(format!("the index is now {}", d.index.state))),
            Err(e) if e.code() == Some(sprig_common::SprigCode::AutoIndexNotFound) => {
                Ok(Some("the index row is gone".into()))
            }
            Err(e) => Err(e),
        }
    }

    async fn apply(&self, rec: &Recommendation) -> Result<()> {
        let (t, id) = Self::reference(rec)
            .ok_or_else(|| sprig_common::SprigError::internal("the item names no index"))?;
        let req = IndexActionRequest {
            reason: rec.reason.clone().unwrap_or_default(),
            actor: Some(format!("approvals:{}", rec.id)),
        };
        self.svc.act(&t, &id, IndexAction::Approve, &req).await?;
        Ok(())
    }

    async fn progress(&self, rec: &Recommendation) -> Result<Option<(bool, String)>> {
        let Some((t, id)) = Self::reference(rec) else {
            return Ok(Some((false, "the item names no index".into())));
        };
        let d = match self.svc.show(&t, &id).await {
            Ok(d) => d,
            Err(e) if e.code() == Some(sprig_common::SprigCode::AutoIndexNotFound) => {
                return Ok(Some((false, "the index row is gone".into())));
            }
            Err(e) => return Err(e),
        };
        Ok(match d.index.state.as_str() {
            "observing" | "active" => Some((
                true,
                format!("index {} built: {}", d.index.name, d.index.state),
            )),
            "failed" | "dropped" | "dropping" | "rejected" => Some((
                false,
                format!("index {} is {}", d.index.name, d.index.state),
            )),
            _ => None,
        })
    }
}

/// Chapter 52's gate before automatic indexing removes an index of one
/// object (decision AP14): an automatic index's retirement or drop, and a
/// user or migrated index's removal.
///
/// - An automatic index is held when `auto_index.mode = approve` for the
///   object (kind `index.drop`); `recommend` keeps retiring automatically,
///   as chapter 13 does.
/// - A user or migrated index is held when
///   `auto_index.retire_user_indexes = approve`, or when it is `on` and
///   `auto_index.mode = approve`: one switch holds every index change.
///
/// The observer proposes the removal again at its next judgement, so an
/// approved one goes ahead only while the evidence still asks for it.
pub struct IndexRemovalGate {
    a: Arc<ControllerApprovals>,
    tenant: TenantId,
    object: ObjectId,
    db: Option<(sprig_common::ids::DbId, String)>,
    table: String,
}

impl std::fmt::Debug for IndexRemovalGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexRemovalGate")
            .field("tenant", &self.tenant)
            .field("object", &self.object)
            .finish_non_exhaustive()
    }
}

/// The gate for `object` of `tenant`, when the controller has approvals.
#[must_use]
pub fn removal_gate(
    tenant: TenantId,
    object: ObjectId,
    o: &crate::autoindex_loop::ObjectEnv,
) -> Option<IndexRemovalGate> {
    let a = super::installed()?;
    Some(IndexRemovalGate {
        a: Arc::clone(a),
        tenant,
        object,
        db: o.db.clone(),
        table: o
            .shards
            .first()
            .map(|t| t.table.clone())
            .unwrap_or_default(),
    })
}

impl IndexRemovalGate {
    /// The policy of `index.drop` for this removal.
    async fn policy(&self, user: bool) -> Policy {
        let db = self.db.as_ref().map(|d| d.0);
        let p = self
            .a
            .policy(
                Kind::IndexDrop,
                Some(self.tenant),
                db,
                Some(self.object),
                None,
            )
            .await;
        if !user {
            return p;
        }
        let Some(r) = self
            .a
            .resolve(
                "auto_index.retire_user_indexes",
                Some(self.tenant),
                db,
                Some(self.object),
                None,
            )
            .await
        else {
            return p;
        };
        match &r.value {
            sprig_settings::Value::Str(t) if t == "approve" => {
                self.a
                    .policy_with(
                        Kind::IndexDrop,
                        "approve",
                        r.source,
                        Some(self.tenant),
                        db,
                        Some(self.object),
                    )
                    .await
            }
            _ => p,
        }
    }
}

#[async_trait::async_trait]
impl sprig_advisor::autoindex::observe::RemovalGate for IndexRemovalGate {
    async fn removal(
        &self,
        r: &sprig_advisor::autoindex::observe::Removal<'_>,
    ) -> sprig_advisor::autoindex::observe::RemovalVerdict {
        use sprig_advisor::autoindex::observe::RemovalVerdict as V;
        let policy = self.policy(r.user).await;
        if policy.mode != Mode::Approve {
            return V::Go(None);
        }
        let mut subject = self.a.subject(self.tenant).await;
        subject.db = self.db.as_ref().map(|d| d.0);
        subject.db_name = self.db.as_ref().map(|d| d.1.clone());
        subject.object = Some(self.object);
        subject.object_name = Some(self.table.clone());
        let db_name = self.db.as_ref().map_or("?", |d| d.1.as_str());
        let what = if r.user {
            "user index"
        } else {
            "automatic index"
        };
        let text = if r.user {
            format!(
                "ALTER TABLE {db_name}.{} ALTER INDEX {} IGNORED, then DROP INDEX after \
                 auto_index.usage_sample_reads reads with no query slower",
                self.table, r.index
            )
        } else {
            format!(
                "ALTER TABLE {db_name}.{} DROP INDEX {}",
                self.table, r.index
            )
        };
        let p = Proposal {
            kind: Kind::IndexDrop,
            subject,
            target: format!("t{}/o{}/{}", self.tenant, self.object.0, r.index),
            action: Action {
                text,
                machine: serde_json::json!({
                    "index": r.index,
                    "user": r.user,
                    "reason": r.reason,
                    "columns": r.columns,
                }),
            },
            evidence: r.evidence.clone(),
            score: 1.0,
            summary: format!(
                "remove {what} {} on {db_name}.{} ({}): {}",
                r.index,
                self.table,
                r.columns.join(", "),
                r.reason
            ),
            emergency: false,
        };
        match self.a.gate(&p, &policy).await {
            Verdict::Proceed(t) => V::Go(
                t.id.is_some()
                    .then(|| serde_json::to_string(&t).unwrap_or_default()),
            ),
            Verdict::Wait(id) => V::Hold(format!("held for approval as {id}")),
            Verdict::Advice(id) => V::Hold(format!("recommended as {id}")),
            Verdict::Suppressed(id) => V::Hold(format!("ignored ({id})")),
            Verdict::Off => V::Go(None),
        }
    }

    async fn removed(&self, ticket: &str, ok: bool, detail: &str) {
        if let Ok(t) = serde_json::from_str::<sprig_approvals::Ticket>(ticket) {
            self.a.finished(&t, ok, detail).await;
        }
    }
}
