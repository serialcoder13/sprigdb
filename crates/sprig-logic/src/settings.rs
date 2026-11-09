//! Chapter 41's settings for the fallback tier, declared next to the code
//! (chapter 03, "Declaring a key"), and the limits one call reads.

use std::borrow::Cow;
use std::time::Duration;

use sprig_logic_js::Limits;
use sprig_logic_js::outbound::OutboundPolicy;
use sprig_settings::{Apply, HardBounds, Scopes, Setting, SettingCtx, Settings, TenantSettings};

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident = $key:literal, $scopes:expr, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: false,
            owner: 41,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key! {
    /// `logic.function_budget_ms`.
    FUNCTION_BUDGET_MS = "logic.function_budget_ms", Scopes::DATA, 5000, 1, 3_600_000,
    "Wall time of one synchronous call"
}
key! {
    /// `logic.kirun_max_steps`.
    KIRUN_MAX_STEPS = "logic.kirun_max_steps", Scopes::DATA, 1_000_000, 1, 1_000_000_000,
    "Steps per native KIRun invocation"
}
key! {
    /// `logic.max_loop_iterations`.
    MAX_LOOP_ITERATIONS = "logic.max_loop_iterations", Scopes::DATA, 100_000, 1, 1_000_000_000,
    "Guard on unbounded loops"
}
key! {
    /// `logic.memory_limit_mb`.
    MEMORY_LIMIT_MB = "logic.memory_limit_mb", TENANT_GLOBAL, 64, 1, 4096,
    "Engine heap per invocation"
}
key! {
    /// `logic.max_data_ops`.
    MAX_DATA_OPS = "logic.max_data_ops", Scopes::DATA, 1000, 1, 1_000_000,
    "db calls per invocation"
}
key! {
    /// `logic.max_rows_read`.
    MAX_ROWS_READ = "logic.max_rows_read", Scopes::DATA, 100_000, 1, 1_000_000_000,
    "Rows read per invocation"
}
key! {
    /// `logic.outbound_timeout_ms`.
    OUTBOUND_TIMEOUT_MS = "logic.outbound_timeout_ms", TENANT_GLOBAL, 2000, 1, 600_000,
    "Per outbound call"
}
key! {
    /// `logic.outbound_max_bytes`.
    OUTBOUND_MAX_BYTES = "logic.outbound_max_bytes", TENANT_GLOBAL, 1_048_576, 1, 1 << 30,
    "Response cap"
}

/// `logic.outbound_allowlist`: hosts `Sprig.Http.Fetch` may reach, as a
/// JSON list (`["api.example.com", "*.svc.example.org:8443"]`) or
/// comma-separated.
pub static OUTBOUND_ALLOWLIST: Setting<Cow<'static, str>> = Setting {
    key: "logic.outbound_allowlist",
    default: Cow::Borrowed("[]"),
    scopes: TENANT_GLOBAL,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 41,
    doc: "Hosts fetch may reach",
};
sprig_settings::register!(OUTBOUND_ALLOWLIST);

/// The allowlist's entries.
#[must_use]
pub fn allowlist(text: &str) -> Vec<String> {
    let t = text.trim();
    if t.starts_with('[')
        && let Ok(v) = serde_json::from_str::<Vec<String>>(t)
    {
        return v;
    }
    t.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The limits and outbound policy of one call, resolved at `ctx` with the
/// tenant's own values `t`.
#[must_use]
pub fn limits(
    s: &Settings,
    ctx: &SettingCtx<'_>,
    t: Option<&TenantSettings>,
) -> (Limits, OutboundPolicy) {
    let limits = Limits {
        wall: Duration::from_millis(s.get(&FUNCTION_BUDGET_MS, ctx, t)),
        max_steps: s.get(&KIRUN_MAX_STEPS, ctx, t),
        max_loop_iterations: s.get(&MAX_LOOP_ITERATIONS, ctx, t),
        memory_bytes: s.get(&MEMORY_LIMIT_MB, ctx, t) << 20,
        max_data_ops: s.get(&MAX_DATA_OPS, ctx, t),
        max_rows_read: s.get(&MAX_ROWS_READ, ctx, t),
    };
    let outbound = OutboundPolicy {
        allow: allowlist(&s.get(&OUTBOUND_ALLOWLIST, ctx, t)),
        timeout: Duration::from_millis(s.get(&OUTBOUND_TIMEOUT_MS, ctx, t)),
        max_bytes: s.get(&OUTBOUND_MAX_BYTES, ctx, t),
    };
    (limits, outbound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlists_in_both_forms() {
        assert_eq!(allowlist("[]"), Vec::<String>::new());
        assert_eq!(
            allowlist(r#"["a.example", "10.0.0.1:80"]"#),
            ["a.example", "10.0.0.1:80"]
        );
        assert_eq!(
            allowlist(" a.example , *.b.example "),
            ["a.example", "*.b.example"]
        );
    }
}
