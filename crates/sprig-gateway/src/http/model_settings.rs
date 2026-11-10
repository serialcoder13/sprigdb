//! Live `vector.*` and `analytics.*` settings for the Vectors and
//! Analytics APIs (chapters 34 and 35, "Configuration"; R6 round 3).
//!
//! Read at each call from the gateway's live settings mirror, at the
//! gateway's process scopes (dc, global), so a change applies to the next
//! call. `analytics.max_concurrent_per_gateway` is the key the router
//! already registers (chapter 17's analytic reads share it).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Apply, HardBounds, Scopes, Setting, SettingCtx};

use super::settings::ApiSettingsSource;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $owner:literal, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::PROCESS,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: $owner,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `vector.default_m`.
    VECTOR_DEFAULT_M: u64 = "vector.default_m", 16, HardBounds::range(3, 200), 34,
    "M for indexes SprigDB creates"
);
key!(
    /// `vector.default_ef_search`.
    VECTOR_DEFAULT_EF: u64 = "vector.default_ef_search", 64, HardBounds::range(1, 10_000), 34,
    "ef when the request gives none"
);
key!(
    /// `vector.prefilter_max_rows`.
    VECTOR_PREFILTER_MAX_ROWS: u64 = "vector.prefilter_max_rows", 10_000, HardBounds::range(0, 100_000_000), 34,
    "Filtered rows per shard for exact search"
);
key!(
    /// `vector.max_overfetch`.
    VECTOR_MAX_OVERFETCH: u64 = "vector.max_overfetch", 16, HardBounds::range(1, 1000), 34,
    "Largest over-fetch multiple"
);
key!(
    /// `vector.max_k`.
    VECTOR_MAX_K: u64 = "vector.max_k", 10_000, HardBounds::range(1, 100_000), 34,
    "Largest k"
);
key!(
    /// `vector.ann_min_rows`.
    VECTOR_ANN_MIN_ROWS: u64 = "vector.ann_min_rows", 20_000, HardBounds::range(0, 1_000_000_000), 34,
    "Rows before an auto index gets its graph"
);
key!(
    /// `vector.min_recall`.
    VECTOR_MIN_RECALL: f64 = "vector.min_recall", 0.95, HardBounds::NONE, 34,
    "Recall at k to accept and keep a graph"
);
key!(
    /// `analytics.max_groups`.
    ANALYTICS_MAX_GROUPS: u64 = "analytics.max_groups", 1_000_000, HardBounds::range(1, 1_000_000_000), 35,
    "Groups held by one merge"
);
key!(
    /// `analytics.query_timeout_seconds`.
    ANALYTICS_QUERY_TIMEOUT: u64 = "analytics.query_timeout_seconds", 300, HardBounds::range(1, 86_400), 35,
    "Per query"
);
key!(
    /// `analytics.max_concurrent_per_tenant`.
    ANALYTICS_PER_TENANT: u64 = "analytics.max_concurrent_per_tenant", 2, HardBounds::range(1, 1024), 35,
    "Queries per tenant per gateway"
);

/// The Vectors API's settings now.
#[must_use]
pub fn vector(src: &ApiSettingsSource) -> sprig_vector::plan::Settings {
    let d = sprig_vector::plan::Settings::default();
    let ApiSettingsSource::Live { live, dc } = src else {
        return d;
    };
    let snap = live.snapshot();
    let cx = SettingCtx::process(dc);
    let u32_of = |s: &Setting<u64>| u32::try_from(snap.get(s, &cx)).unwrap_or(u32::MAX);
    sprig_vector::plan::Settings {
        default_ef: u32_of(&VECTOR_DEFAULT_EF),
        prefilter_max_rows: snap.get(&VECTOR_PREFILTER_MAX_ROWS, &cx),
        max_overfetch: u32_of(&VECTOR_MAX_OVERFETCH),
        max_k: u32_of(&VECTOR_MAX_K),
        ann_min_rows: snap.get(&VECTOR_ANN_MIN_ROWS, &cx),
        min_recall: snap.get(&VECTOR_MIN_RECALL, &cx).clamp(0.0, 1.0),
        default_m: u8::try_from(snap.get(&VECTOR_DEFAULT_M, &cx)).unwrap_or(d.default_m),
    }
}

/// The Analytics API's limits now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Analytics {
    /// `analytics.max_groups`.
    pub max_groups: usize,
    /// `analytics.query_timeout_seconds`.
    pub timeout: Duration,
    /// `analytics.max_concurrent_per_tenant`.
    pub per_tenant: usize,
    /// `analytics.max_concurrent_per_gateway`.
    pub per_gateway: usize,
}

/// [`Analytics`] from `src`.
#[must_use]
pub fn analytics(src: &ApiSettingsSource) -> Analytics {
    let ApiSettingsSource::Live { live, dc } = src else {
        return Analytics {
            max_groups: 1_000_000,
            timeout: Duration::from_secs(300),
            per_tenant: 2,
            per_gateway: 8,
        };
    };
    let snap = live.snapshot();
    let cx = SettingCtx::process(dc);
    let us = |s: &Setting<u64>| usize::try_from(snap.get(s, &cx)).unwrap_or(usize::MAX);
    Analytics {
        max_groups: us(&ANALYTICS_MAX_GROUPS),
        timeout: Duration::from_secs(snap.get(&ANALYTICS_QUERY_TIMEOUT, &cx)),
        per_tenant: us(&ANALYTICS_PER_TENANT),
        per_gateway: us(&sprig_router::settings::ANALYTICS_MAX_CONCURRENT),
    }
}

/// One admitted analytics query: frees its slots when dropped.
#[derive(Debug)]
pub struct Admitted {
    tenant: Arc<AtomicUsize>,
    gateway: &'static AtomicUsize,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.tenant.fetch_sub(1, Ordering::AcqRel);
        self.gateway.fetch_sub(1, Ordering::AcqRel);
    }
}

fn take(c: &AtomicUsize, limit: usize) -> bool {
    c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < limit).then_some(n + 1))
        .is_ok()
}

/// Admits a query of `tenant` under the limits now, waiting up to a
/// second for a slot (chapter 35, "Admission control").
///
/// # Errors
/// 39780 (retryable) when no slot frees in time.
pub async fn admit(tenant: u64, limits: Analytics) -> Result<Admitted> {
    static GATEWAY: AtomicUsize = AtomicUsize::new(0);
    static TENANTS: OnceLock<Mutex<HashMap<u64, Arc<AtomicUsize>>>> = OnceLock::new();
    let t = Arc::clone(
        TENANTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(tenant)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0))),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if take(&t, limits.per_tenant) {
            if take(&GATEWAY, limits.per_gateway) {
                return Ok(Admitted {
                    tenant: t,
                    gateway: &GATEWAY,
                });
            }
            t.fetch_sub(1, Ordering::AcqRel);
        }
        if tokio::time::Instant::now() >= deadline {
            metrics::counter!("sprig_dim_rejected_total", "reason" => "admission").increment(1);
            let mut e = SprigError::new(
                SprigCode::RateLimited,
                "too many analytics queries running (analytics.max_concurrent_per_tenant, analytics.max_concurrent_per_gateway); retry",
            );
            e.retryable = true;
            return Err(e);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
