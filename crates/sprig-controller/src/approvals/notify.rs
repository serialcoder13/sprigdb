//! Approval notifications: every event to the
//! tenant's `sprig_recommendations` stream and the platform tenant's, a
//! signed webhook per event (the sinks' `sprig-signature` and backoff),
//! and email: at once for new, escalated, applied, failed and emergency
//! items, and a daily digest of waiting ones. Nothing here can fail or
//! slow a decision: channels run on spawned tasks and count their results
//! in `sprig_approvals_notifications_total`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use hyper::body::Bytes;
use mysql_async::prelude::Queryable;
use sprig_approvals::catalog::{SqlRecs, Subscription};
use sprig_approvals::events::{Outbound, rfc3339};
use sprig_approvals::gate::{Event, Hook};
use sprig_approvals::metrics::notified;
use sprig_approvals::model::{NotifyLevel, Recommendation, State};
use sprig_approvals::policy::kind_listed;
use sprig_approvals::settings as keys;
use sprig_approvals::smtp::{Mail, SmtpConfig, TlsFiles};
use sprig_approvals::store::Filter;
use sprig_catalog::CatalogLocator;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};
use sprig_settings::Value;

use super::ControllerApprovals;

/// Webhook attempts per event.
pub const WEBHOOK_ATTEMPTS: u32 = 6;

/// Secrets from files named in `[controller]`.
#[derive(Debug, Clone, Default)]
pub struct Secrets {
    /// `smtp_password_file`.
    pub smtp_password: Option<String>,
    /// `approvals_webhook_secret_file`.
    pub webhook_secret: Option<String>,
    /// `smtp_ca_file`, `smtp_client_cert_file`, `smtp_client_key_file`.
    pub smtp_tls: TlsFiles,
}

/// The notification hook.
pub struct Notifier {
    ctl: OnceLock<Arc<ControllerApprovals>>,
    locator: Arc<dyn CatalogLocator>,
    secrets: Secrets,
    webhooks: OnceLock<Arc<super::webhook::Webhooks>>,
    /// Day (since the epoch) of each tenant's last digest run.
    digested: Mutex<BTreeMap<Option<TenantId>, i64>>,
}

impl std::fmt::Debug for Notifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifier").finish_non_exhaustive()
    }
}

fn text(v: Option<sprig_settings::Resolved>) -> String {
    match v.map(|r| r.value) {
        Some(Value::Str(s)) => s,
        _ => String::new(),
    }
}

impl Notifier {
    /// A notifier reading catalogs through `locator`.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>, secrets: Secrets) -> Self {
        Self {
            ctl: OnceLock::new(),
            locator,
            secrets,
            webhooks: OnceLock::new(),
            digested: Mutex::new(BTreeMap::new()),
        }
    }

    /// Webhooks through the sinks' webhook target (the platform URL and
    /// tenants' connections).
    pub fn set_webhooks(&self, w: Arc<super::webhook::Webhooks>) {
        let _ = self.webhooks.set(w);
    }

    /// Completes the cycle hub → hook → controller approvals.
    pub fn bind(&self, ctl: Arc<ControllerApprovals>) {
        let _ = self.ctl.set(ctl);
    }

    async fn setting(&self, key: &str, tenant: Option<TenantId>) -> String {
        match self.ctl.get() {
            Some(c) => text(c.resolve(key, tenant, None, None, None).await),
            None => String::new(),
        }
    }

    async fn smtp(&self) -> Result<Option<SmtpConfig>> {
        let url = self.setting(keys::SMTP_URL.key, None).await;
        let from = self.setting(keys::SMTP_FROM.key, None).await;
        let user = self.setting(keys::SMTP_USERNAME.key, None).await;
        let auth = (!user.is_empty())
            .then(|| (user, self.secrets.smtp_password.clone().unwrap_or_default()));
        SmtpConfig::parse(
            &url,
            if from.is_empty() {
                "sprigdb@localhost"
            } else {
                &from
            },
            auth,
            self.secrets.smtp_tls.clone(),
        )
    }

    /// Appends `body` to stream `sprig_recommendations` in `tenant`'s
    /// database named by `approvals.stream_database`, if both exist.
    async fn to_stream(&self, tenant: TenantId, body: &[u8]) -> Result<bool> {
        let db_name = self.setting(keys::STREAM_DATABASE.key, Some(tenant)).await;
        if db_name.is_empty() {
            return Ok(false);
        }
        let ep = self.locator.writer(tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep
            .pool
            .get_conn()
            .await
            .map_err(|e| SprigError::internal("stream: catalog unavailable").with_source(e))?;
        let db: Option<u32> = c
            .exec_first(
                format!(
                    "SELECT db_id FROM {}.`databases` WHERE name_key = ? AND state = 'active'",
                    quote(&schema)
                ),
                (db_name.to_lowercase(),),
            )
            .await
            .map_err(|e| SprigError::internal("stream: databases").with_source(e))?;
        let Some(db) = db else {
            return Ok(false);
        };
        let st = sprig_streams::store::StreamsStore::new(schema.clone())
            .load(&mut c)
            .await?;
        let Some(def) = st.get(sprig_common::ids::DbId(db), "sprig_recommendations", true) else {
            return Ok(false);
        };
        let Some(slot) = def.outbox.first() else {
            return Ok(false);
        };
        let shard = slot.schema(tenant, def.db).to_string();
        let rec = sprig_streams::publish::Publish {
            key: None,
            value: Some(body.to_vec()),
            headers: Some(r#"{"source":"sprig_approvals"}"#.into()),
            partition: None,
            ts_ms: None,
        };
        let Some(sql) = sprig_streams::publish::outbox_insert(def, Some(&shard), &[rec]) else {
            return Ok(false);
        };
        c.query_drop(sql)
            .await
            .map_err(|e| SprigError::internal("stream: outbox insert").with_source(e))?;
        Ok(true)
    }

    async fn streams(&self, ev: &Event, body: &[u8]) {
        let mut tenants = Vec::new();
        if let Some(t) = ev.rec.subject.tenant {
            tenants.push(t);
        }
        let platform = self.setting(keys::PLATFORM_TENANT.key, None).await;
        if !platform.is_empty()
            && let Some(c) = self.ctl.get()
            && let Ok(Some(e)) = c.directory().by_name(&platform).await
            && !tenants.contains(&e.body().tenant_id)
        {
            tenants.push(e.body().tenant_id);
        }
        for t in tenants {
            match self.to_stream(t, body).await {
                Ok(true) => notified("stream", "sent"),
                Ok(false) => {}
                Err(e) => {
                    notified("stream", "failed");
                    tracing::warn!(tenant = %t, error = %e, "approvals: stream event not written");
                }
            }
        }
    }

    /// The webhook destinations of `ev`: the platform URL, the subject
    /// tenant's connection, and the connection of an ancestor the item
    /// has escalated to.
    async fn webhook_dests(&self, ev: &Event) -> Vec<super::webhook::Dest> {
        use super::webhook::Dest;
        let mut out = Vec::new();
        let url = self
            .setting(keys::WEBHOOK_URL.key, ev.rec.subject.tenant)
            .await;
        if !url.is_empty() {
            out.push(Dest::Platform(url));
        }
        let mut tenants: Vec<TenantId> = ev.rec.subject.tenant.into_iter().collect();
        if let NotifyLevel::Ancestor(a) = ev.rec.notify_level
            && !tenants.contains(&a)
        {
            tenants.push(a);
        }
        for t in tenants {
            let c = self.tenant_setting(keys::WEBHOOK_CONNECTION.key, t).await;
            if !c.is_empty() {
                out.push(Dest::Tenant(t, c));
            }
        }
        out
    }

    /// A tenant-only key at `t` (its own value; never a wider scope's).
    async fn tenant_setting(&self, key: &str, t: TenantId) -> String {
        match self.ctl.get() {
            Some(c) => match c.resolve(key, Some(t), None, None, None).await {
                Some(r) if matches!(r.source, sprig_settings::ScopeKind::Tenant) => text(Some(r)),
                _ => String::new(),
            },
            None => String::new(),
        }
    }

    async fn webhook(&self, ev: &Event, body: Bytes) {
        let dests = self.webhook_dests(ev).await;
        if dests.is_empty() {
            return;
        }
        let Some(w) = self.webhooks.get().cloned() else {
            notified("webhook", "skipped");
            tracing::warn!("approvals: webhooks aren't wired in this controller");
            return;
        };
        let event = ev.kind.name().to_owned();
        let key = format!("{}:{}:{}", ev.rec.id, event, ev.at_us);
        for d in dests {
            let (w, body, event, key) = (Arc::clone(&w), body.clone(), event.clone(), key.clone());
            tokio::spawn(async move {
                if w.deliver(&d, body, &key, &event).await {
                    notified("webhook", "sent");
                } else {
                    notified("webhook", "failed");
                }
            });
        }
    }

    /// Who gets an immediate email for `rec` now: the notify level's
    /// people (a tenant's or an ancestor's subscribers; operators'
    /// addresses for dc and platform).
    async fn recipients(&self, rec: &Recommendation, digest: bool) -> Vec<String> {
        let want = |s: &Subscription| {
            (s.delivery == "immediate" || (digest && s.delivery == "digest"))
                && (s.kinds == "*" || kind_listed(&s.kinds, rec.kind))
        };
        let tenant = match rec.notify_level {
            NotifyLevel::Ancestor(a) => Some(a),
            NotifyLevel::Dc | NotifyLevel::Platform => None,
            _ => rec.subject.tenant,
        };
        match tenant {
            Some(t) => self
                .subscriptions(t)
                .await
                .into_iter()
                .filter(want)
                .map(|s| s.email)
                .collect(),
            None => self
                .setting(keys::OPERATOR_EMAILS.key, None)
                .await
                .split(',')
                .map(str::trim)
                .filter(|s| s.contains('@'))
                .map(str::to_owned)
                .collect(),
        }
    }

    async fn subscriptions(&self, t: TenantId) -> Vec<Subscription> {
        let Ok(ep) = self.locator.writer(t).await else {
            return Vec::new();
        };
        let Ok(mut c) = ep.pool.get_conn().await else {
            return Vec::new();
        };
        SqlRecs::new(t, ep.store.schema())
            .subscriptions(&mut c)
            .await
            .unwrap_or_default()
    }

    async fn email(&self, ev: &Event) {
        let cfg = match self.smtp().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                notified("email", "skipped");
                return;
            }
            Err(e) => {
                notified("email", "failed");
                tracing::warn!(error = %e, "approvals: email misconfigured");
                return;
            }
        };
        let to = self.recipients(&ev.rec, false).await;
        if to.is_empty() {
            return;
        }
        let studio = self.setting(keys::STUDIO_URL.key, None).await;
        let o = Outbound::of(ev, &studio);
        let mail = Mail {
            to,
            subject: format!(
                "[SprigDB] {} {}: {}",
                o.kind,
                o.event.trim_start_matches("recommendation."),
                o.target
            ),
            body: format!(
                "{}\n\nKind: {}\nLevel: {} (notifies {})\nState: {}\nAction: {}\nAuto-apply: {}\n{}\nId: {}\n",
                o.summary,
                o.kind,
                o.level,
                o.notify_level,
                o.state,
                o.action,
                o.auto_apply_at.clone().unwrap_or_else(|| "off".into()),
                o.link
                    .clone()
                    .map(|l| format!("Open in Studio: {l}\n"))
                    .unwrap_or_default(),
                o.id
            ),
        };
        let id = format!("{}.{}@sprigdb", o.id, ev.at_us);
        let date = rfc2822(ev.at_us);
        tokio::spawn(async move {
            for attempt in 0..5u32 {
                match sprig_approvals::smtp::send(&cfg, &mail, &date, &id).await {
                    Ok(()) => {
                        notified("email", "sent");
                        return;
                    }
                    Err(e) if attempt == 4 => {
                        notified("email", "failed");
                        tracing::warn!(error = %e, "approvals: email not sent");
                    }
                    Err(_) => tokio::time::sleep(Duration::from_secs(15 << attempt)).await,
                }
            }
        });
    }

    /// The daily digest for `tenant` (or the operators, `None`), once a
    /// day at `approvals.digest_hour_utc`, of every waiting item.
    pub async fn digest(&self, tenant: Option<TenantId>, items: &[Recommendation], now_us: i64) {
        let hour = self
            .setting(keys::DIGEST_HOUR_UTC.key, tenant)
            .await
            .parse::<i64>()
            .ok();
        let hour = match self.ctl.get() {
            Some(c) => c
                .resolve(keys::DIGEST_HOUR_UTC.key, tenant, None, None, None)
                .await
                .and_then(|r| r.value.as_f64())
                .map_or(i64::from(keys::DIGEST_HOUR_UTC.default), |v| {
                    #[allow(clippy::cast_possible_truncation)]
                    let v = v as i64;
                    v
                }),
            None => hour.unwrap_or(8),
        };
        let reminder = self.setting(keys::REMINDER.key, tenant).await;
        if reminder == "off" {
            return;
        }
        let day = now_us.div_euclid(86_400_000_000);
        let h = now_us.rem_euclid(86_400_000_000) / 3_600_000_000;
        if h < hour {
            return;
        }
        {
            let mut g = self.digested.lock().unwrap_or_else(PoisonError::into_inner);
            if g.get(&tenant).is_some_and(|d| *d >= day) {
                return;
            }
            g.insert(tenant, day);
        }
        let waiting: Vec<&Recommendation> = items
            .iter()
            .filter(|r| matches!(r.state, State::Open | State::Approved))
            .collect();
        if waiting.is_empty() {
            return;
        }
        let Ok(Some(cfg)) = self.smtp().await else {
            notified("email", "skipped");
            return;
        };
        let mut to: Vec<String> = Vec::new();
        for r in &waiting {
            for a in self.recipients(r, true).await {
                if !to.contains(&a) {
                    to.push(a);
                }
            }
        }
        if to.is_empty() {
            return;
        }
        let mut body = format!("{} recommendations are waiting:\n\n", waiting.len());
        for r in &waiting {
            let _ = write!(
                body,
                "- {} {} ({}, {:.0} h): {}\n  {}\n",
                r.id,
                r.kind,
                r.state,
                r.age_hours(now_us),
                r.summary,
                r.action.text
            );
        }
        let mail = Mail {
            to,
            subject: format!("[SprigDB] {} recommendations waiting", waiting.len()),
            body,
        };
        let id = format!("digest.{day}.{}@sprigdb", tenant.map_or(0, |t| t.0));
        let date = rfc2822(now_us);
        tokio::spawn(async move {
            match sprig_approvals::smtp::send(&cfg, &mail, &date, &id).await {
                Ok(()) => notified("email", "sent"),
                Err(e) => {
                    notified("email", "failed");
                    tracing::warn!(error = %e, "approvals: digest not sent");
                }
            }
        });
    }
}

/// `Tue, 10 Nov 2026 14:03:11 +0000`.
#[must_use]
pub fn rfc2822(us: i64) -> String {
    let iso = rfc3339(us); // 2026-11-10T14:03:11Z
    let days = us.div_euclid(86_400_000_000);
    let wd = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"]
        [usize::try_from(days.rem_euclid(7)).unwrap_or(0)];
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let m: usize = iso[5..7].parse().unwrap_or(1);
    format!(
        "{wd}, {} {} {} {} +0000",
        iso[8..10].trim_start_matches('0'),
        months[m.saturating_sub(1).min(11)],
        &iso[0..4],
        &iso[11..19]
    )
}

#[async_trait::async_trait]
impl Hook for Notifier {
    async fn event(&self, ev: &Event) {
        let studio = self.setting(keys::STUDIO_URL.key, None).await;
        let Ok(body) = serde_json::to_vec(&Outbound::of(ev, &studio)) else {
            return;
        };
        self.streams(ev, &body).await;
        self.webhook(ev, Bytes::from(body)).await;
        if ev.kind.notify_now() && !ev.rec.advisory {
            self.email(ev).await;
        }
    }
}

/// Lists a store's waiting items for the digest.
///
/// # Errors
/// The store's.
pub async fn waiting(store: &dyn sprig_approvals::RecStore) -> Result<Vec<Recommendation>> {
    store.list(&Filter::default()).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn dates() {
        assert_eq!(
            super::rfc2822(1_794_319_391_000_000),
            "Tue, 10 Nov 2026 14:03:11 +0000"
        );
    }
}
