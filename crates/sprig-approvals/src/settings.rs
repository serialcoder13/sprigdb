//! The `approvals.*` keys (chapter 52, "Configuration") and the three new
//! mode keys whose automation has no settings module of its own here:
//! `vector.auto_ann`, `failover.standby_mode` and `graph.auto_partition`.
//! The existing mode keys gain `approve` where they are declared.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

type Text = Cow<'static, str>;

/// Tenant scope only.
pub const TENANT_ONLY: Scopes = Scopes(0b00_0100);

/// The values of the new mode keys.
pub const MODES: &[&str] = &["on", "approve", "recommend", "off"];

macro_rules! keys {
    ($(
        $(#[$doc:meta])*
        $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $op:literal, $owner:literal, $text:literal;
    )*) => {
        $(
            $(#[$doc])*
            pub static $name: Setting<$t> = Setting {
                key: $key,
                default: $default,
                scopes: $scopes,
                hard: $hard,
                apply: Apply::Live,
                operator_only: $op,
                owner: $owner,
                doc: $text,
            };
            sprig_settings::register!($name);
        )*

        /// Every key this module declares.
        pub static ALL: &[&'static dyn sprig_settings::AnySetting] = &[$(&$name),*];
    };
}

keys! {
    /// `approvals.start_level`.
    START_LEVEL: Text = "approvals.start_level", Cow::Borrowed("resolved"), Scopes::ALL,
        HardBounds::one_of(&["resolved", "table", "database", "tenant", "dc", "platform"]), false, 52,
        "Level of a new recommendation: resolved (the mode's scope) or a fixed level";
    /// `approvals.escalate_after`.
    ESCALATE_AFTER: Text = "approvals.escalate_after", Cow::Borrowed("off"), Scopes::ALL,
        HardBounds::NONE, false, 52,
        "Escalation timer: off, a duration (90m, 24h, 3d) or kind=duration,...,*=duration";
    /// `approvals.auto_apply_after`.
    AUTO_APPLY_AFTER: Text = "approvals.auto_apply_after", Cow::Borrowed("off"), Scopes::ALL,
        HardBounds::NONE, false, 52,
        "Auto-apply timer, same syntax as approvals.escalate_after";
    /// `approvals.auto_apply_forbidden`.
    AUTO_APPLY_FORBIDDEN: Text = "approvals.auto_apply_forbidden", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "Kinds or families that never auto-apply (comma list)";
    /// `approvals.allow_emergency`.
    ALLOW_EMERGENCY: bool = "approvals.allow_emergency", true, Scopes::ALL,
        HardBounds::NONE, false, 52,
        "Emergencies skip the wait for approval";
    /// `approvals.reraise_factor`.
    RERAISE_FACTOR: f64 = "approvals.reraise_factor", 2.0, Scopes::ALL,
        HardBounds { min: Some(1.0), max: Some(1000.0), allowed: None }, false, 52,
        "An ignored recommendation comes back when its score grows past this factor";
    /// `approvals.ignore_once_days`.
    IGNORE_ONCE_DAYS: u32 = "approvals.ignore_once_days", 1, Scopes::ALL,
        HardBounds::range(0, 3650), false, 52,
        "Suppression after ignore once";
    /// `approvals.reminder`.
    REMINDER: Text = "approvals.reminder", Cow::Borrowed("daily"), Scopes::ALL,
        HardBounds::one_of(&["daily", "off"]), false, 52,
        "Daily digest of waiting recommendations, or off";
    /// `approvals.digest_hour_utc`.
    DIGEST_HOUR_UTC: u32 = "approvals.digest_hour_utc", 8, Scopes::ALL,
        HardBounds::range(0, 23), false, 52,
        "Hour (UTC) of the daily digest";
    /// `approvals.tick_seconds`.
    TICK_SECONDS: u32 = "approvals.tick_seconds", 30, Scopes::PROCESS,
        HardBounds::range(1, 3600), true, 52,
        "The approvals loop's period";
    /// `approvals.applying_timeout_hours`.
    APPLYING_TIMEOUT_HOURS: u32 = "approvals.applying_timeout_hours", 24, Scopes::ALL,
        HardBounds::range(1, 720), false, 52,
        "A recommendation stuck in applying becomes failed after this";
    /// `approvals.retention_days`.
    RETENTION_DAYS: u32 = "approvals.retention_days", 180, Scopes::ALL,
        HardBounds::range(1, 3650), false, 52,
        "Closed recommendations are purged after this";
    /// `approvals.smtp_url`.
    SMTP_URL: Text = "approvals.smtp_url", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "smtp://host[:25], smtp+starttls://host[:587] or smtps://host[:465]; empty disables email";
    /// `approvals.smtp_from`.
    SMTP_FROM: Text = "approvals.smtp_from", Cow::Borrowed("sprigdb@localhost"), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "Sender address of approval emails";
    /// `approvals.smtp_username`.
    SMTP_USERNAME: Text = "approvals.smtp_username", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "SMTP user; the password is the file [controller] smtp_password_file";
    /// `approvals.operator_emails`.
    OPERATOR_EMAILS: Text = "approvals.operator_emails", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "Comma list of platform (global) or DC (dc scope) operator addresses";
    /// `approvals.studio_url`.
    STUDIO_URL: Text = "approvals.studio_url", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "Base URL of Studio for links in emails and events";
    /// `approvals.webhook_url`.
    WEBHOOK_URL: Text = "approvals.webhook_url", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "An http:// or https:// URL for every event; signed with [controller] approvals_webhook_secret_file";
    /// `approvals.webhook_connection`.
    WEBHOOK_CONNECTION: Text = "approvals.webhook_connection", Cow::Borrowed(""), TENANT_ONLY,
        HardBounds::NONE, false, 52,
        "<database>.<connection> or platform:<name>: a webhook connection (chapter 51) that gets the tenant's events";
    /// `approvals.stream_database`.
    STREAM_DATABASE: Text = "approvals.stream_database", Cow::Borrowed(""), TENANT_ONLY,
        HardBounds::NONE, false, 52,
        "Database whose stream sprig_recommendations gets the tenant's events";
    /// `approvals.platform_tenant`.
    PLATFORM_TENANT: Text = "approvals.platform_tenant", Cow::Borrowed(""), Scopes::PROCESS,
        HardBounds::NONE, true, 52,
        "Tenant whose stream sprig_recommendations gets every event";
    /// `vector.auto_ann` (chapter 34).
    VECTOR_AUTO_ANN: Text = "vector.auto_ann", Cow::Borrowed("on"), Scopes::ALL,
        HardBounds::one_of(MODES), false, 34,
        "Automatic ANN (HNSW) graphs: on, approve, recommend or off";
    /// `failover.standby_mode` (chapter 25).
    STANDBY_MODE: Text = "failover.standby_mode", Cow::Borrowed("on"), Scopes(0b11_0100),
        HardBounds::one_of(MODES), true, 25,
        "Automatic standby creation and removal: on, approve, recommend or off";
    /// `graph.auto_partition` (chapter 43).
    GRAPH_AUTO_PARTITION: Text = "graph.auto_partition", Cow::Borrowed("on"), Scopes::ALL,
        HardBounds::one_of(MODES), false, 43,
        "Automatic graph partitioning (when built): on, approve, recommend or off";
}

#[cfg(test)]
mod tests {
    #[test]
    fn keys_are_valid() {
        sprig_settings::Registry::new(super::ALL.iter().copied()).unwrap();
    }
}
