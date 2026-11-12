//! `queue.change_exchange` (chapter 50, "Change feed to an exchange";
//! chapter 29, RLS round 5, decision RL52): after
//! `ALTER TABLE t SPRIG SET | RESET (... queue.change_exchange ...)` the
//! table's forwarder subscription matches the setting.
//!
//! ```text
//! the setting's value for t (object scope) after the statement:
//!   empty → drop the feed subscription cx:o<object>
//!   else  → (re)register cx:o<object>, kind change_exchange, at the feed's
//!           position now, its owner_ref {db, table, object, exchange, user}:
//!           the user who set it; the controller's change_exchange loop
//!           gives it a worker job (sprig_worker::change_exchange) that
//!           publishes each change as that user, filtered per change by the
//!           user's privileges and row-level security
//! ```

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_common::{Result, SprigError};
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_feed::{ResumeToken, StartAt, TailOptions, TenantTail};

use super::PassSession;

/// The subscription kind (and the controller's job kind).
pub(crate) const KIND: &str = "change_exchange";

/// The subscription of table `object`.
#[must_use]
pub(crate) fn sub_name(object: u64) -> String {
    format!("cx:o{object}")
}

impl PassSession {
    /// See the module docs. An error is the statement's (the setting stays).
    #[allow(clippy::too_many_lines)] // one step per line of the module docs
    pub(super) async fn change_exchange_after_settings(&mut self, text: &[u8]) -> Result<()> {
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(());
        };
        if !sql.to_ascii_lowercase().contains("queue.change_exchange") {
            return Ok(());
        }
        let (lex, _) = self.lex_opts();
        // The table the statement names, read as the change-topic hook
        // reads it (its parser looks for that key's name).
        let Some(target) = sprig_streams::sql::change_topic_target(
            &format!("{sql} /* stream.change_topic */"),
            &lex,
        ) else {
            return Ok(());
        };
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(());
        };
        let lctn = ts.lower_case_table_names();
        let cat = ts.refresh().await?;
        let Some(db_name) = target.db.clone().or(current) else {
            return Ok(());
        };
        let Some(db) = cat
            .database(&database_key(&db_name, lctn))
            .filter(|d| d.state == DbState::Active)
            .cloned()
        else {
            return Ok(());
        };
        let Some(obj) = cat
            .object(
                db.db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, &target.name, lctn),
            )
            .filter(|o| o.state == ObjectState::Active)
            .cloned()
        else {
            return Ok(());
        };
        let svc = Arc::clone(ts.services());
        let exchange = svc
            .settings
            .get(
                &sprig_queues::settings::CHANGE_EXCHANGE,
                &cat.object_ctx(&svc.dc, obj.object_id),
                Some(&cat.settings),
            )
            .into_owned();
        let tenant = ts.tenant;
        let user = ts.principal.user_id.map_or(0, |u| u.0);
        let subs = Subscriptions::new(svc.locator.dc_store());
        let name = sub_name(obj.object_id.0);
        match subs.drop_subscription(tenant, &name).await {
            Ok(()) => {}
            Err(e) if e.code() == Some(sprig_common::SprigCode::FeedSubscriptionNotFound) => {}
            Err(e) => return Err(e),
        }
        if exchange.is_empty() {
            tracing::info!(%tenant, table = %obj.name, "change exchange removed");
            return Ok(());
        }
        // Registered at the feed's position now, so the forwarder misses no
        // change committed after the statement.
        let (clusters, placement, names) = crate::http::changes::feed(&svc)?;
        let tail = TenantTail::start(
            tenant,
            StartAt::Now,
            clusters,
            Arc::clone(&placement),
            names,
            TailOptions {
                progress_every: Duration::from_millis(500),
                ..TailOptions::default()
            },
        )
        .await?;
        let segment = placement
            .history(tenant)
            .await?
            .last()
            .map_or(0, |h| h.map_version);
        let token = ResumeToken {
            tenant,
            clamp: 0,
            cluster: tail.cluster().clone(),
            segment,
            pos: tail.position().clone(),
            until: None,
            progress: true,
        };
        drop(tail);
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        let sub = Subscription {
            tenant,
            name,
            kind: KIND.to_owned(),
            ordering: "merged".to_owned(),
            owner_ref: Some(
                json!({
                    "db": db.name,
                    "table": obj.name,
                    "object": obj.object_id.0,
                    "exchange": exchange,
                    "user": user,
                })
                .to_string(),
            ),
            state: "active".to_owned(),
            created_at_ms: now_ms,
        };
        subs.register_at(&sub, &token, now_ms).await.map_err(|e| {
            SprigError::internal(format!("the change exchange's subscription: {e}"))
        })?;
        tracing::info!(%tenant, table = %obj.name, %exchange, "change exchange set");
        Ok(())
    }
}
