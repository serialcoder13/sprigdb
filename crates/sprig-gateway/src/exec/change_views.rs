//! Change-topic filter views (chapter 29, "Change feed consumers"; RLS
//! round 5, decision RL47): what views and stored code over a change topic
//! read, so their readers get only the records their own policies show.
//!
//! ```text
//! CREATE | ALTER VIEW (or other stored code) reading t.changes:
//!   deploy __sprig_scf_<stream> in the topic's log schema, as sprig_ctl:
//!     t's security off: a plain view of the log
//!     on: each record's before and after images through t's SELECT policies,
//!         rendered for MariaDB over the reader's context __sprig_rls_ctx()
//!         (the policy views' function), with the triggers' guard (internal
//!         work and exempt sessions see everything)
//!   the stored statement reads __sprig_scf_<stream> instead of the log
//! a policy or security change on t (deploy_checks): the view again
//! ```
//!
//! A reader's backend connection carries its own context (`@sprig_rls`),
//! so a view's definer can't widen what a reader sees; a connection
//! without a context that isn't internal sees nothing.

use std::sync::Arc;

use sprig_auth::authz::rls::RlsState;
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigError};
use sprig_sql::rls::triggers::{TriggerPolicy, TriggerSpec};
use sprig_streams::StreamDef;

use super::PassSession;

/// The filter view of stream `id`.
#[must_use]
fn view_name(id: u64) -> String {
    format!("__sprig_scf_{id}")
}

impl PassSession {
    /// Deploys change topic `def`'s filter view (see the module docs).
    ///
    /// # Errors
    /// The cluster's; 39364 for a policy that can't be rendered.
    #[allow(clippy::too_many_lines)] // one step per part, in order
    pub(super) async fn deploy_change_view(&mut self, def: &StreamDef) -> Result<()> {
        let own_cluster = self.target.cluster.clone();
        let (lex, _) = self.lex_opts();
        let Some(object) = def.source_object else {
            return Ok(());
        };
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let cat = ts.refresh().await?;
        let obj = cat
            .objects
            .get(object)
            .cloned()
            .ok_or_else(|| SprigError::internal("a change topic's table is gone"))?;
        let db = cat
            .databases
            .get(obj.db_id)
            .cloned()
            .ok_or_else(|| SprigError::internal("a change topic's database is gone"))?;
        let log_schema = def
            .log
            .first()
            .map(|l| l.slot.schema(cat.tenant, def.db).to_string())
            .ok_or_else(|| SprigError::internal("a change topic without a log shard"))?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("no user"))?;
        let svc = Arc::clone(ts.services());
        let template = ts.schema_of(Some(&db.name));
        let layout = super::router::ddl_layout(ts);
        let (table_schema, table_cluster): (String, ClusterId) = match (&layout, &template) {
            (Some(l), Some(t)) => l(t, Some(&obj.name))
                .filter(|v| !v.is_empty())
                .and_then(|v| v.into_iter().next()),
            _ => None,
        }
        .or_else(|| template.clone().map(|t| (t, own_cluster.clone())))
        .ok_or_else(|| SprigError::internal("the table has no shard this gateway can find"))?;
        let columns = crate::tenant::rls_streams::filter_columns(
            &svc,
            cat.tenant.0,
            &obj,
            &table_cluster,
            &table_schema,
        )
        .await?;
        let cols = crate::tenant::rls_streams::json_table_columns(&columns);
        let state = RlsState::of(&cat).cloned();
        let filtered = state
            .as_ref()
            .and_then(|s| s.security(object))
            .is_some_and(|s| s.enabled);
        let policies: Vec<_> = state
            .as_ref()
            .map(|s| s.policies_of(object).cloned().collect())
            .unwrap_or_default();
        // A policy reading a column the record carries in another form
        // (base64) can't be evaluated on the image: its term is FALSE.
        let opaque: Vec<String> = columns
            .iter()
            .filter(|c| c.opaque)
            .map(|c| c.name.to_lowercase())
            .collect();
        let reads_opaque = |e: &str| {
            let l = e.to_lowercase();
            opaque.iter().any(|c| {
                l.match_indices(c.as_str()).any(|(i, _)| {
                    let before = l[..i].chars().next_back();
                    let after = l[i + c.len()..].chars().next();
                    let word = |x: Option<char>| {
                        x.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
                    };
                    !word(before) && !word(after)
                })
            })
        };
        let tp: Vec<TriggerPolicy<'_>> = policies
            .iter()
            .map(|p| TriggerPolicy {
                id: p.policy_id,
                restrictive: p.restrictive,
                commands: p.commands,
                using: p
                    .using_expr
                    .as_deref()
                    .map(|e| if reads_opaque(e) { "FALSE" } else { e }),
                check: p.check_expr.as_deref(),
            })
            .collect();
        // The spec isn't Send: everything it touches ends here.
        let stmts = {
            let definer_user = crate::tenant::rls::view_definer(&cat, object, user);
            let definer_account = sprig_auth::authz::backend::account(cat.tenant, definer_user);
            let definer = format!("`{}`@`%`", definer_account.replace('`', "``"));
            let db_name = db.name.clone();
            let here = log_schema.clone();
            let schema_of = |d: &str, t: &str| -> Option<String> {
                let tmpl = ts.schema_of(Some(d))?;
                let shards = layout
                    .as_ref()
                    .and_then(|l| l(&tmpl, Some(t)))
                    .unwrap_or_default();
                if shards.iter().any(|(s, _)| *s == here) {
                    return Some(here.clone());
                }
                shards.first().map(|(s, _)| s.clone()).or(Some(tmpl))
            };
            let physical = |e: &str| -> Result<String> {
                sprig_sql::rls::expr::map_table_schemas(e, &lex, &|d, t| {
                    schema_of(d.unwrap_or(&db_name), t)
                })
            };
            let spec = TriggerSpec {
                schema: &log_schema,
                table: &obj.name,
                object: object.0,
                tenant: cat.tenant.0,
                definer: &definer,
                policies: &tp,
                lex,
                physical: &physical,
            };
            let view = spec.change_view_statement(
                &view_name(def.id.0),
                &sprig_streams::model::log_table(def.id),
                &cols,
                &definer,
                filtered,
            )?;
            let mut stmts = vec![format!("USE {}", sprig_sql::rls::quote(&log_schema))];
            if filtered {
                stmts.push(spec.view_statements(&definer, &[])?.function);
                stmts.push(format!(
                    "GRANT EXECUTE ON FUNCTION {}.{} TO '{}'@'%'",
                    sprig_sql::rls::quote(&log_schema),
                    sprig_sql::rls::quote(sprig_sql::rls::views::CTX_FUNCTION),
                    definer_account
                ));
            }
            stmts.push(view);
            stmts
        };
        let pool = svc.locator.ctl_pool(&own_cluster).await?;
        let mut conn = pool.get_conn().await.map_err(|e| {
            SprigError::new(
                sprig_common::SprigCode::BackendUnavailable,
                format!("sprig_ctl on {own_cluster}: {e}"),
            )
        })?;
        for q in &stmts {
            mysql_async::prelude::Queryable::query_drop(&mut conn, q.as_str())
                .await
                .map_err(sprig_streams::maria::map_err)?;
        }
        tracing::info!(tenant = %cat.tenant, stream = def.id.0, filtered, "change topic filter view deployed");
        Ok(())
    }

    /// After `object`'s policies or security changed: its change topic's
    /// filter view again, when it has one (a failure is logged; the view
    /// keeps the policies it had).
    pub(super) async fn redeploy_change_view(&mut self, object: sprig_common::ids::ObjectId) {
        let Some(def) = self
            .tenant
            .as_deref()
            .and_then(|ts| ts.catalog().cloned())
            .and_then(|cat| {
                cat.ext
                    .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
                    .and_then(|st| {
                        st.all()
                            .find(|s| {
                                s.kind == sprig_streams::model::StreamKind::Changes
                                    && s.source_object == Some(object)
                            })
                            .map(|s| (**s).clone())
                    })
            })
        else {
            return;
        };
        if let Err(e) = Box::pin(self.deploy_change_view(&def)).await {
            tracing::warn!(error = %e, stream = def.id.0, "change topic filter view not redeployed");
        }
    }
}
