//! Chapter 47's `telemetry.*` and `audit.*` keys, and [`apply_live`],
//! which keeps the process's tracer, OTLP exporter and audit pipeline on
//! their current values (every role calls it once its settings mirror is
//! ready). They live here, not in `sprig-common`, because declaring a key
//! needs this crate.
//!
//! They are resolved at dc scope for the process, and declared so (dc and
//! global): the tracer and the audit pipeline are per process, so a tenant
//! value would do nothing (bugfix-89's round narrowed `trace_sample_ratio`,
//! `trace_slow_ms` and `audit.retention_days` from tenant scope).

use std::borrow::Cow;

use sprig_common::audit;
use sprig_common::ids::DcId;
use sprig_common::telemetry::trace::{self, otlp};

use crate::registry::{Apply, HardBounds, Scopes, Setting};
use crate::scope::SettingCtx;
use crate::watch::{LiveSettings, Registration};

macro_rules! ops_key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: true,
            owner: 47,
            doc: $text,
        };
        crate::register!($name);
    };
}

ops_key!(
    /// `telemetry.otlp_endpoint`.
    OTLP_ENDPOINT: Cow<'static, str> = "telemetry.otlp_endpoint",
    Cow::Borrowed(""),
    Scopes::PROCESS,
    HardBounds::NONE,
    "OTLP/HTTP collector for traces (http://host:4318); empty: none"
);
ops_key!(
    /// `telemetry.trace_sample_ratio`.
    TRACE_SAMPLE_RATIO: f64 = "telemetry.trace_sample_ratio",
    0.01,
    Scopes::PROCESS,
    HardBounds {
        min: Some(0.0),
        max: Some(1.0),
        allowed: None
    },
    "Head sampling of root spans"
);
ops_key!(
    /// `telemetry.trace_slow_ms`.
    TRACE_SLOW_MS: u64 = "telemetry.trace_slow_ms",
    500,
    Scopes::PROCESS,
    HardBounds::range(0, 3_600_000),
    "Export every slower or failed statement"
);
ops_key!(
    /// `telemetry.tenant_series_min_rps`.
    TENANT_SERIES_MIN_RPS: u64 = "telemetry.tenant_series_min_rps",
    10,
    Scopes::PROCESS,
    HardBounds::range(0, 1_000_000),
    "Traffic needed for per-tenant series"
);
ops_key!(
    /// `audit.spool_max_mb`.
    AUDIT_SPOOL_MAX_MB: u64 = "audit.spool_max_mb",
    audit::DEFAULT_SPOOL_MAX_MB,
    Scopes::PROCESS,
    HardBounds::range(1, 1_048_576),
    "Local spool before failing closed"
);
ops_key!(
    /// `audit.retention_days`.
    AUDIT_RETENTION_DAYS: u64 = "audit.retention_days",
    365,
    Scopes::PROCESS,
    HardBounds::range(1, 36_500),
    "Audit retention"
);

/// Applies the chapter 47 keys to the process's tracer, OTLP exporter and
/// audit pipeline (those installed) now and after every change. Keep the
/// returned registration for as long as the role runs.
pub fn apply_live(live: &LiveSettings, dc: &DcId) -> Registration {
    let ctx_dc = dc.clone();
    let ctx = SettingCtx::process(&ctx_dc);
    Values {
        ratio: live.get(&TRACE_SAMPLE_RATIO, &ctx),
        slow_ms: live.get(&TRACE_SLOW_MS, &ctx),
        otlp: live.get(&OTLP_ENDPOINT, &ctx).into_owned(),
        spool_mb: live.get(&AUDIT_SPOOL_MAX_MB, &ctx),
        retention_days: live.get(&AUDIT_RETENTION_DAYS, &ctx),
        tenant_rps: live.get(&TENANT_SERIES_MIN_RPS, &ctx),
    }
    .apply();
    let dc = dc.clone();
    live.on_change("telemetry", &["telemetry.*", "audit.*"], move |m| {
        let ctx = SettingCtx::process(&dc);
        Values {
            ratio: m.get(&TRACE_SAMPLE_RATIO, &ctx),
            slow_ms: m.get(&TRACE_SLOW_MS, &ctx),
            otlp: m.get(&OTLP_ENDPOINT, &ctx).into_owned(),
            spool_mb: m.get(&AUDIT_SPOOL_MAX_MB, &ctx),
            retention_days: m.get(&AUDIT_RETENTION_DAYS, &ctx),
            tenant_rps: m.get(&TENANT_SERIES_MIN_RPS, &ctx),
        }
        .apply();
        Ok(())
    })
}

/// The values [`apply_live`] applies.
#[derive(Debug, Clone, PartialEq)]
struct Values {
    ratio: f64,
    slow_ms: u64,
    otlp: String,
    spool_mb: u64,
    /// `audit.retention_days` at global and DC scope, for the sink's
    /// retention pass.
    retention_days: u64,
    tenant_rps: u64,
}

impl Values {
    fn apply(&self) {
        trace::tracer().set_policy(self.ratio, self.slow_ms);
        sprig_common::telemetry::set_tenant_series_min_rps(self.tenant_rps);
        if let Some(o) = otlp::global() {
            o.set_endpoint(Some(self.otlp.clone()).filter(|e| !e.is_empty()));
        }
        if let Some(p) = audit::global() {
            p.set_max_spool_mb(self.spool_mb);
        }
        audit::set_retention_days(self.retention_days);
    }
}
