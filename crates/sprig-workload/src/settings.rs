//! The `workload.*` keys (chapter 12, "Configuration"; Appendix B),
//! declared next to the code that reads them (chapter 03, "Declaring a
//! key"). Names, defaults and scopes are the chapter's: **data** keys take
//! global, tenant, database and object; **tenant** keys global and
//! tenant; **gw** keys global and dc.
//!
//! How M0.4 reads them *(choice)*: the gateway builds its
//! [`RecorderConfig`] from the global and dc values ([`recorder_config`])
//! when it starts and again whenever a `workload.*` key changes (it
//! follows the settings and swaps the recorder's configuration), so the
//! recorder's keys apply live, as declared; the flusher and the rollup
//! read theirs on every run. Per-object resolution of the data keys (a tenant turning
//! samples off for one table) waits for the recorder to resolve per
//! fingerprint; `workload.capture_samples` is the exception: the gateway
//! resolves it for the tenant before it captures a sample.

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use crate::recorder::RecorderConfig;

/// Global and tenant.
pub const TENANT: Scopes = Scopes(0b10_0100);
/// Global and dc: gateway and worker behaviour.
pub const GW: Scopes = Scopes(0b11_0000);

macro_rules! keys {
    ($(
        $(#[$doc:meta])*
        $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal;
    )*) => {
        $(
            $(#[$doc])*
            pub static $name: Setting<$t> = Setting {
                key: $key,
                default: $default,
                scopes: $scopes,
                hard: $hard,
                apply: Apply::Live,
                operator_only: false,
                owner: 12,
                doc: $text,
            };
            sprig_settings::register!($name);
        )*

        /// Every key this module declares, for tests and documentation.
        pub static ALL: &[&'static dyn sprig_settings::AnySetting] = &[$(&$name),*];
    };
}

const fn rate() -> HardBounds {
    HardBounds {
        min: Some(0.0),
        max: Some(1.0),
        allowed: None,
    }
}

keys! {
    /// `workload.bucket_minutes`.
    BUCKET_MINUTES: u32 = "workload.bucket_minutes", 60, TENANT,
        HardBounds::range(1, 1440), "Statistics bucket size";
    /// `workload.retention_days`.
    RETENTION_DAYS: u32 = "workload.retention_days", 30, TENANT,
        HardBounds::range(1, 3650), "Stats retention";
    /// `workload.max_fingerprints_per_tenant`.
    MAX_FINGERPRINTS_PER_TENANT: u32 = "workload.max_fingerprints_per_tenant", 10_000, TENANT,
        HardBounds::range(10, 10_000_000), "Cap; the rest count as other";
    /// `workload.flush_seconds`.
    FLUSH_SECONDS: u32 = "workload.flush_seconds", 900, GW,
        HardBounds::range(1, 86_400), "Partial flush period";
    /// `workload.rollup_delay_minutes`.
    ROLLUP_DELAY_MINUTES: u32 = "workload.rollup_delay_minutes", 10, GW,
        HardBounds::range(0, 1440), "Wait after a bucket closes";
    /// `workload.max_text_bytes`.
    MAX_TEXT_BYTES: u32 = "workload.max_text_bytes", 4096, TENANT,
        HardBounds::range(64, 1 << 20), "Longest normalized text";
    /// `workload.max_memory_mb`.
    MAX_MEMORY_MB: u32 = "workload.max_memory_mb", 128, GW,
        HardBounds::range(1, 65_536), "Recorder memory per gateway";
    /// `workload.watch_p95_ms`.
    WATCH_P95_MS: u32 = "workload.watch_p95_ms", 50, Scopes::DATA,
        HardBounds::range(0, 3_600_000), "p95 that makes a fingerprint watched";
    /// `workload.examined_sample_rate`.
    EXAMINED_SAMPLE_RATE: f64 = "workload.examined_sample_rate", 0.02, Scopes::DATA,
        rate(), "Probe rate for watched fingerprints";
    /// `workload.full_probe_ms`.
    FULL_PROBE_MS: u32 = "workload.full_probe_ms", 100, Scopes::DATA,
        HardBounds::range(0, 3_600_000), "Mean latency from which every execution is probed";
    /// `workload.capture_samples`.
    CAPTURE_SAMPLES: bool = "workload.capture_samples", true, Scopes::DATA,
        HardBounds::NONE, "Parameter capture switch";
    /// `workload.sample_rate`.
    SAMPLE_RATE: f64 = "workload.sample_rate", 0.01, Scopes::DATA,
        rate(), "Capture rate for watched fingerprints";
    /// `workload.samples_per_bucket`.
    SAMPLES_PER_BUCKET: u32 = "workload.samples_per_bucket", 3, TENANT,
        HardBounds::range(0, 100), "Per gateway per bucket";
    /// `workload.max_samples_per_fingerprint`.
    MAX_SAMPLES_PER_FINGERPRINT: u32 = "workload.max_samples_per_fingerprint", 50, TENANT,
        HardBounds::range(0, 10_000), "Stored samples kept";
    /// `workload.sample_retention_days`.
    SAMPLE_RETENTION_DAYS: u32 = "workload.sample_retention_days", 7, TENANT,
        HardBounds::range(1, 365), "Sample lifetime";
    /// `workload.max_in_list_values`.
    MAX_IN_LIST_VALUES: u32 = "workload.max_in_list_values", 1000, TENANT,
        HardBounds::range(1, 1_000_000), "IN values kept in a sample";
}

/// The recorder's settings from one resolver (`get` resolves a key at
/// the scope the caller chose, global and dc for a gateway).
#[must_use]
pub fn recorder_config(
    get_u32: impl Fn(&Setting<u32>) -> u32,
    get_f64: impl Fn(&Setting<f64>) -> f64,
    get_bool: impl Fn(&Setting<bool>) -> bool,
) -> RecorderConfig {
    let d = RecorderConfig::default();
    RecorderConfig {
        bucket_minutes: get_u32(&BUCKET_MINUTES),
        max_memory_bytes: usize::try_from(get_u32(&MAX_MEMORY_MB))
            .unwrap_or(128)
            .saturating_mul(1 << 20),
        max_fingerprints_per_tenant: get_u32(&MAX_FINGERPRINTS_PER_TENANT),
        watch_p95_ms: u64::from(get_u32(&WATCH_P95_MS)),
        examined_sample_rate: get_f64(&EXAMINED_SAMPLE_RATE),
        full_probe_ms: u64::from(get_u32(&FULL_PROBE_MS)),
        capture_samples: get_bool(&CAPTURE_SAMPLES),
        sample_rate: get_f64(&SAMPLE_RATE),
        samples_per_bucket: get_u32(&SAMPLES_PER_BUCKET),
        ..d
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_the_chapters() {
        assert_eq!(ALL.len(), 16);
        for k in ALL {
            assert!(k.key().starts_with("workload."), "{}", k.key());
        }
        let cfg = recorder_config(|s| s.default, |s| s.default, |s| s.default);
        assert_eq!(cfg, RecorderConfig::default());
    }
}
