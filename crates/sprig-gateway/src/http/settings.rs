//! The `api.*` keys the HTTP listeners read (chapter 37, "Configuration").
//!
//! Every key is read when a request needs it, so a change applies to the
//! next request. Keys the chapter also accepts at tenant scope resolve at
//! the process scopes (dc, global) in R0: a request knows its tenant only
//! after authentication, and per-tenant API limits arrive with rate limits
//! (chapter 37 step 9).

use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

macro_rules! key {
    ($NAME:ident, $key:literal, $default:expr, $scopes:expr, $min:expr, $max:expr, $doc:literal) => {
        #[doc = concat!("`", $key, "`.")]
        pub static $NAME: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: false,
            owner: 37,
            doc: $doc,
        };
        sprig_settings::register!($NAME);
    };
}

key!(TXN_IDLE_TIMEOUT_MS, "api.txn.idle_timeout_ms", 10_000, Scopes::TREE, 100, 3_600_000,
    "Idle time before a transaction handle expires");
key!(TXN_MAX_LIFETIME_MS, "api.txn.max_lifetime_ms", 60_000, Scopes::TREE, 100, 86_400_000,
    "Longest life of a transaction handle");
key!(TXN_MAX_OPEN_PER_TENANT, "api.txn.max_open_per_tenant", 256, Scopes::TREE, 0, 1_000_000,
    "Open transaction handles per tenant per gateway");
key!(STREAM_BATCH_ROWS, "api.stream_batch_rows", 500, Scopes::PROCESS, 1, 1_000_000,
    "Rows per streamed batch");
key!(REST_MAX_BUFFERED_BYTES, "api.rest.max_buffered_bytes", 16 << 20, Scopes::PROCESS, 1024, 1 << 30,
    "Largest non-streamed REST result");
key!(MAX_REQUEST_BYTES, "api.max_request_bytes", 16 << 20, Scopes::PROCESS, 1024, 1 << 30,
    "Largest request body");
key!(MAX_CONCURRENT_STREAMS, "api.max_concurrent_streams", 100, Scopes::PROCESS, 1, 10_000,
    "HTTP/2 streams per connection");

/// The values one request uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiSettings {
    /// `api.txn.idle_timeout_ms`.
    pub txn_idle: Duration,
    /// `api.txn.max_lifetime_ms`.
    pub txn_lifetime: Duration,
    /// `api.txn.max_open_per_tenant`.
    pub txn_max_open: usize,
    /// `api.stream_batch_rows`.
    pub batch_rows: usize,
    /// `api.rest.max_buffered_bytes`.
    pub max_buffered: usize,
    /// `api.max_request_bytes`.
    pub max_request: usize,
    /// `api.max_concurrent_streams`.
    pub max_streams: u32,
}

impl Default for ApiSettings {
    fn default() -> Self {
        let us = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
        Self {
            txn_idle: Duration::from_millis(TXN_IDLE_TIMEOUT_MS.default),
            txn_lifetime: Duration::from_millis(TXN_MAX_LIFETIME_MS.default),
            txn_max_open: us(TXN_MAX_OPEN_PER_TENANT.default),
            batch_rows: us(STREAM_BATCH_ROWS.default),
            max_buffered: us(REST_MAX_BUFFERED_BYTES.default),
            max_request: us(MAX_REQUEST_BYTES.default),
            max_streams: u32::try_from(MAX_CONCURRENT_STREAMS.default).unwrap_or(100),
        }
    }
}

/// Where the values come from.
#[derive(Clone)]
pub enum ApiSettingsSource {
    /// Fixed values (tests).
    Fixed(ApiSettings),
    /// The live settings mirror at this DC's process scopes.
    Live {
        /// The mirror.
        live: LiveSettings,
        /// This gateway's DC.
        dc: DcId,
    },
}

impl std::fmt::Debug for ApiSettingsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed(s) => f.debug_tuple("Fixed").field(s).finish(),
            Self::Live { dc, .. } => f.debug_struct("Live").field("dc", dc).finish_non_exhaustive(),
        }
    }
}

impl ApiSettingsSource {
    /// The values now.
    #[must_use]
    pub fn current(&self) -> ApiSettings {
        match self {
            Self::Fixed(s) => *s,
            Self::Live { live, dc } => {
                let snap = live.snapshot();
                let ctx = SettingCtx::process(dc);
                let us = |s: &Setting<u64>| usize::try_from(snap.get(s, &ctx)).unwrap_or(usize::MAX);
                ApiSettings {
                    txn_idle: Duration::from_millis(snap.get(&TXN_IDLE_TIMEOUT_MS, &ctx)),
                    txn_lifetime: Duration::from_millis(snap.get(&TXN_MAX_LIFETIME_MS, &ctx)),
                    txn_max_open: us(&TXN_MAX_OPEN_PER_TENANT),
                    batch_rows: us(&STREAM_BATCH_ROWS),
                    max_buffered: us(&REST_MAX_BUFFERED_BYTES),
                    max_request: us(&MAX_REQUEST_BYTES),
                    max_streams: u32::try_from(snap.get(&MAX_CONCURRENT_STREAMS, &ctx))
                        .unwrap_or(100),
                }
            }
        }
    }
}
