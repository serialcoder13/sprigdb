//! Audit events of logins and account changes (chapter 14,
//! "Observability"; build step 10).
//!
//! Every login decision ([`crate::mysql`], [`crate::http`]) and every
//! account change ([`crate::admin`], [`crate::policy`]) builds an
//! [`AuditEvent`] and hands it to the process's [`AuditSink`]. Events carry
//! the tenant, the user, the acting administrator when different (an
//! ancestor's, chapter 14 "Management from above"), the client address, the
//! gateway, the protocol and the outcome, never a secret: passwords,
//! scrambles and verifiers never reach this module.
//!
//! [`LogAudit`] writes each event as one JSON line through `tracing` with
//! target `sprig_audit`, so a log shipper can split them from the rest,
//! and hands it to the process's chapter 47 audit pipeline
//! ([`sprig_common::audit`]) when one is installed (`[audit] spool_dir`),
//! which spools it and ships it to the audit sink; [`MemoryAudit`] keeps
//! them for tests. Both sides of a
//! change from above are recorded by emitting the event once per stream
//! ([`AuditEvent::streams`]): the target tenant's and the actor's.

use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sprig_common::audit as pipeline;
use sprig_common::ids::{TenantId, UserId};

/// Who made an account change, when it wasn't the account itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Actor {
    /// The actor's tenant (the target's, or an ancestor's).
    pub tenant: TenantId,
    /// The actor's tenant name, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_name: Option<String>,
    /// The actor's user name (`operator` for the Admin API).
    pub user: String,
    /// The actor's user id; `None` for an operator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<UserId>,
}

impl Actor {
    /// A platform operator through the Admin API or a tool.
    #[must_use]
    pub fn operator(tenant: TenantId, who: impl Into<String>) -> Self {
        Self {
            tenant,
            tenant_name: None,
            user: who.into(),
            user_id: None,
        }
    }
}

/// The event names (chapter 14, "Observability").
pub mod events {
    /// A login was accepted.
    pub const LOGIN_SUCCEEDED: &str = "auth.login_succeeded";
    /// A login was refused (`reason` says why).
    pub const LOGIN_FAILED: &str = "auth.login_failed";
    /// An account was locked: temporarily after failed logins
    /// (`reason = failed_logins`) or by `ACCOUNT LOCK` (`manual`).
    pub const ACCOUNT_LOCKED: &str = "auth.account_locked";
    /// An account was unlocked: `manual` or `lock_expired`.
    pub const ACCOUNT_UNLOCKED: &str = "auth.account_unlocked";
    /// A password was set.
    pub const PASSWORD_CHANGED: &str = "auth.password_changed";
    /// A password change was refused by the password policy.
    pub const PASSWORD_REJECTED: &str = "auth.password_rejected";
    /// A user was created.
    pub const USER_CREATED: &str = "auth.user_created";
    /// A user was dropped.
    pub const USER_DROPPED: &str = "auth.user_dropped";
    /// Another account attribute changed (`detail` names it).
    pub const ACCOUNT_CHANGED: &str = "auth.account_changed";
    /// A login was handed to another DC (residency).
    pub const HANDOFF: &str = "auth.handoff";
    /// An address was refused for too many failed logins.
    pub const RATE_LIMITED: &str = "auth.rate_limited";
    /// Verifiers a password lacked were stored at a login (SCRAM backfill;
    /// the detail lists the mechanisms, never a verifier).
    pub const VERIFIERS_ADDED: &str = "auth.verifiers_added";
}

/// One audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEvent {
    /// The event name ([`events`]).
    pub event: &'static str,
    /// When, in milliseconds since the Unix epoch (UTC).
    pub at_ms: u64,
    /// The tenant the event is about.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantId>,
    /// Its name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_name: Option<String>,
    /// The user name (without the tenant prefix), or the login name as sent
    /// when no account matched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// The account's host pattern.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The user's id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<UserId>,
    /// The acting administrator, when not the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,
    /// The client's address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<IpAddr>,
    /// The gateway (node id) that decided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    /// `mysql`, `mariadb`, `http`, or `admin` for account changes.
    pub protocol: &'static str,
    /// The mechanism (`caching_sha2_password` fast or full path, `native`,
    /// `basic`, `api_key`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mechanism: Option<&'static str>,
    /// `ok` or `refused`.
    pub outcome: &'static str,
    /// Why (`bad_credentials`, `locked`, `temp_locked`, `suspended`,
    /// `expired`, `tls_required`, `rate_limited`, `failed_logins`,
    /// `lock_expired`, `manual`, ...).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// More, never a secret (the changed attribute, the lock's end).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl AuditEvent {
    /// An event named `event` at `at`, with outcome `ok` and nothing else
    /// set.
    #[must_use]
    pub fn new(event: &'static str, protocol: &'static str, at: SystemTime) -> Self {
        let at_ms = at
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        Self {
            event,
            at_ms,
            tenant: None,
            tenant_name: None,
            user: None,
            host: None,
            user_id: None,
            actor: None,
            client_ip: None,
            gateway: None,
            protocol,
            mechanism: None,
            outcome: "ok",
            reason: None,
            detail: None,
        }
    }

    /// The tenants whose streams keep the event: the target's, and the
    /// actor's when it is another tenant (an ancestor acting from above).
    #[must_use]
    pub fn streams(&self) -> Vec<TenantId> {
        let mut s: Vec<TenantId> = self.tenant.into_iter().collect();
        if let Some(a) = &self.actor
            && Some(a.tenant) != self.tenant
        {
            s.push(a.tenant);
        }
        s
    }

    /// The event as one JSON line.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| format!("{{\"event\":\"{}\"}}", self.event))
    }
}

/// Where audit events go.
pub trait AuditSink: Send + Sync + 'static {
    /// Records `event`. Never blocks for long and never fails the caller:
    /// a sink that can't deliver counts the loss.
    fn emit(&self, event: &AuditEvent);
}

/// Writes each event as one JSON line through `tracing` (target
/// `sprig_audit`, level info), once per stream (field `stream`), records it
/// in the process's audit pipeline once per stream (a tenant's stream, or
/// the platform's for an event without a tenant), and counts it in
/// `sprig_auth_audit_events_total` (`event`).
#[derive(Debug, Clone, Default)]
pub struct LogAudit {
    gateway: Option<String>,
}

impl LogAudit {
    /// Events stamped with `gateway` (the node id) when they carry none.
    #[must_use]
    pub fn new(gateway: impl Into<String>) -> Self {
        Self {
            gateway: Some(gateway.into()),
        }
    }
}

impl AuditSink for LogAudit {
    fn emit(&self, event: &AuditEvent) {
        let mut e = event.clone();
        if e.gateway.is_none() {
            e.gateway.clone_from(&self.gateway);
        }
        metrics::counter!("sprig_auth_audit_events_total", "event" => e.event).increment(1);
        let json = e.to_json();
        let streams = e.streams();
        let body = serde_json::to_value(&e).unwrap_or(serde_json::Value::Null);
        if streams.is_empty() {
            tracing::info!(target: "sprig_audit", stream = "deployment", "{json}");
            pipeline::record(pipeline::AuditStream::Platform, e.event, body.clone());
        }
        for s in streams {
            tracing::info!(target: "sprig_audit", stream = %s, "{json}");
            pipeline::record(pipeline::AuditStream::Tenant(s), e.event, body.clone());
        }
    }
}

/// Keeps every event in memory: tests.
#[derive(Debug, Default)]
pub struct MemoryAudit {
    events: Mutex<Vec<AuditEvent>>,
}

impl MemoryAudit {
    /// An empty sink.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Every event so far.
    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The names of every event so far, in order.
    #[must_use]
    pub fn names(&self) -> Vec<&'static str> {
        self.events().iter().map(|e| e.event).collect()
    }
}

impl AuditSink for MemoryAudit {
    fn emit(&self, event: &AuditEvent) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.clone());
    }
}

/// Drops every event: processes and tests that don't audit.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAudit;

impl AuditSink for NoAudit {
    fn emit(&self, _event: &AuditEvent) {}
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn json_has_no_empty_fields_and_two_streams_from_above() {
        let mut e = AuditEvent::new(events::ACCOUNT_LOCKED, "admin", UNIX_EPOCH);
        e.tenant = Some(TenantId(7));
        e.user = Some("alice".into());
        e.reason = Some("manual");
        e.actor = Some(Actor::operator(TenantId(3), "root"));
        let j: serde_json::Value = serde_json::from_str(&e.to_json()).unwrap();
        assert_eq!(j["event"], "auth.account_locked");
        assert_eq!(j["tenant"], 7);
        assert_eq!(j["actor"]["tenant"], 3);
        assert!(j.get("client_ip").is_none());
        assert_eq!(e.streams(), vec![TenantId(7), TenantId(3)]);
        e.actor = Some(Actor::operator(TenantId(7), "admin"));
        assert_eq!(e.streams(), vec![TenantId(7)]);
    }

    #[test]
    fn memory_sink_keeps_order() {
        let m = MemoryAudit::new();
        m.emit(&AuditEvent::new(events::LOGIN_FAILED, "mysql", UNIX_EPOCH));
        m.emit(&AuditEvent::new(
            events::LOGIN_SUCCEEDED,
            "mysql",
            UNIX_EPOCH,
        ));
        assert_eq!(
            m.names(),
            vec![events::LOGIN_FAILED, events::LOGIN_SUCCEEDED]
        );
    }
}
