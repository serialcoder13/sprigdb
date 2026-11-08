//! Chapter 18's settings, declared next to the code (chapter 03,
//! "Declaring a key"), and the snapshots the coordinator and the resolver
//! read.

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

/// Tenant, dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);
/// Tenant, cluster, dc and global.
const TENANT_CLUSTER: Scopes = Scopes(0b11_1100);
/// Database, tenant, dc and global.
const DB_TENANT_DC: Scopes = Scopes(0b11_0110);
/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Dc and global.
const DC_GLOBAL: Scopes = Scopes::PROCESS;
/// Global only.
const GLOBAL: Scopes = Scopes(0b10_0000);

/// MariaDB's default `innodb_lock_wait_timeout`, restored on a branch
/// after a distributed transaction lowered it.
pub const SESSION_LOCK_WAIT_DEFAULT: u32 = 50;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $scopes:expr, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: false,
            owner: 18,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

/// `txn.non_xa_policy`.
pub static NON_XA_POLICY: Setting<Cow<'static, str>> = Setting {
    key: "txn.non_xa_policy",
    default: Cow::Borrowed("reject"),
    scopes: TENANT_DC,
    hard: HardBounds::one_of(&["reject", "best_effort", "sprig_2pc"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 18,
    doc: "reject, best_effort or sprig_2pc (R3)",
};
sprig_settings::register!(NON_XA_POLICY);

key! {
    /// `txn.prepare_timeout_ms`.
    PREPARE_TIMEOUT_MS: u64 = "txn.prepare_timeout_ms", TENANT_CLUSTER, 5000, 100, 600_000,
    "Deadline for each prepare"
}
key! {
    /// `txn.resolver_interval_seconds`.
    RESOLVER_INTERVAL_SECONDS: u64 = "txn.resolver_interval_seconds", DC_GLOBAL, 10, 1, 3600,
    "Resolver scan interval"
}
key! {
    /// `txn.resolver_min_age_seconds`.
    RESOLVER_MIN_AGE_SECONDS: u64 = "txn.resolver_min_age_seconds", DC_GLOBAL, 30, 1, 86_400,
    "Prepared this long before the resolver acts"
}
key! {
    /// `txn.distributed_lock_wait_timeout_seconds`.
    DISTRIBUTED_LOCK_WAIT_TIMEOUT_SECONDS: u64 = "txn.distributed_lock_wait_timeout_seconds", DB_TENANT_DC, 10, 1, 3600,
    "Lock-wait cap with two branches"
}
key! {
    /// `txn.phase2_wait_ms`.
    PHASE2_WAIT_MS: u64 = "txn.phase2_wait_ms", TENANT_DC, 1000, 0, 600_000,
    "Longest COMMIT waits for phase 2"
}
key! {
    /// `txn.commit_resolve_timeout_ms`.
    COMMIT_RESOLVE_TIMEOUT_MS: u64 = "txn.commit_resolve_timeout_ms", TENANT_DC, 5000, 100, 600_000,
    "Longest wait for an unknown outcome"
}
key! {
    /// `txn.max_branches`.
    MAX_BRANCHES: u64 = "txn.max_branches", TENANT_GLOBAL, 32, 2, 1024,
    "Clusters per transaction"
}
key! {
    /// `txn.abort_record_retention_hours`.
    ABORT_RECORD_RETENTION_HOURS: u64 = "txn.abort_record_retention_hours", GLOBAL, 24, 9, 8760,
    "How long abort rows are kept"
}

/// `txn.non_xa_policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NonXaPolicy {
    /// A write that would add a second cluster without XA fails with 39440.
    #[default]
    Reject,
    /// XA where possible, the rest committed one at a time.
    BestEffort,
    /// R3: agents hold the extra branches. Until then it behaves as
    /// `reject`.
    Sprig2pc,
}

impl NonXaPolicy {
    /// The setting's text.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NonXaPolicy::Reject => "reject",
            NonXaPolicy::BestEffort => "best_effort",
            NonXaPolicy::Sprig2pc => "sprig_2pc",
        }
    }

    /// From the setting's text; unknown text is `reject`.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "best_effort" => NonXaPolicy::BestEffort,
            "sprig_2pc" => NonXaPolicy::Sprig2pc,
            _ => NonXaPolicy::Reject,
        }
    }
}

/// What one coordinator transaction reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnConfig {
    /// `txn.non_xa_policy`.
    pub policy: NonXaPolicy,
    /// `txn.prepare_timeout_ms`.
    pub prepare_timeout: Duration,
    /// `txn.phase2_wait_ms`.
    pub phase2_wait: Duration,
    /// `txn.commit_resolve_timeout_ms`.
    pub commit_resolve_timeout: Duration,
    /// `txn.distributed_lock_wait_timeout_seconds`.
    pub distributed_lock_wait_timeout_s: u32,
    /// The session's `innodb_lock_wait_timeout`.
    pub session_lock_wait_timeout_s: u32,
    /// `txn.max_branches`.
    pub max_branches: usize,
    /// Every cluster holding the tenant's shards supports XA: the home
    /// opens as a plain transaction.
    pub tenant_all_xa: bool,
    /// The gateway id written in decision rows.
    pub coordinator_id: String,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            policy: NonXaPolicy::Reject,
            prepare_timeout: Duration::from_millis(PREPARE_TIMEOUT_MS.default),
            phase2_wait: Duration::from_millis(PHASE2_WAIT_MS.default),
            commit_resolve_timeout: Duration::from_millis(COMMIT_RESOLVE_TIMEOUT_MS.default),
            distributed_lock_wait_timeout_s: u32_of(DISTRIBUTED_LOCK_WAIT_TIMEOUT_SECONDS.default),
            session_lock_wait_timeout_s: SESSION_LOCK_WAIT_DEFAULT,
            max_branches: usize::try_from(MAX_BRANCHES.default).unwrap_or(32),
            tenant_all_xa: false,
            coordinator_id: "gateway".to_owned(),
        }
    }
}

fn u32_of(v: u64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

impl TxnConfig {
    /// Resolves the keys for `ctx` (the session's tenant and database).
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>, coordinator_id: &str) -> Self {
        Self {
            policy: NonXaPolicy::parse(&s.get(&NON_XA_POLICY, ctx)),
            prepare_timeout: Duration::from_millis(s.get(&PREPARE_TIMEOUT_MS, ctx)),
            phase2_wait: Duration::from_millis(s.get(&PHASE2_WAIT_MS, ctx)),
            commit_resolve_timeout: Duration::from_millis(s.get(&COMMIT_RESOLVE_TIMEOUT_MS, ctx)),
            distributed_lock_wait_timeout_s: u32_of(
                s.get(&DISTRIBUTED_LOCK_WAIT_TIMEOUT_SECONDS, ctx),
            ),
            session_lock_wait_timeout_s: SESSION_LOCK_WAIT_DEFAULT,
            max_branches: usize::try_from(s.get(&MAX_BRANCHES, ctx)).unwrap_or(32),
            tenant_all_xa: false,
            coordinator_id: coordinator_id.to_owned(),
        }
    }

    /// The lock-wait timeout every branch gets once a transaction has two.
    #[must_use]
    pub fn lock_wait_cap(&self) -> u32 {
        self.session_lock_wait_timeout_s
            .min(self.distributed_lock_wait_timeout_s)
    }
}

/// What the resolver reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolverConfig {
    /// `txn.resolver_interval_seconds`.
    pub interval: Duration,
    /// `txn.resolver_min_age_seconds`.
    pub min_age: Duration,
    /// `txn.abort_record_retention_hours`.
    pub abort_retention: Duration,
    /// Written as the coordinator of rows the resolver inserts.
    pub id: String,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(RESOLVER_INTERVAL_SECONDS.default),
            min_age: Duration::from_secs(RESOLVER_MIN_AGE_SECONDS.default),
            abort_retention: Duration::from_secs(ABORT_RECORD_RETENTION_HOURS.default * 3600),
            id: "resolver".to_owned(),
        }
    }
}

impl ResolverConfig {
    /// Resolves the keys for the data center.
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self {
            interval: Duration::from_secs(s.get(&RESOLVER_INTERVAL_SECONDS, ctx)),
            min_age: Duration::from_secs(s.get(&RESOLVER_MIN_AGE_SECONDS, ctx)),
            abort_retention: Duration::from_secs(s.get(&ABORT_RECORD_RETENTION_HOURS, ctx) * 3600),
            id: "resolver".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::DcId;
    use sprig_settings::Registry;

    use super::*;

    #[test]
    fn keys_are_registered_with_the_books_defaults() {
        let reg = Registry::global().unwrap();
        for k in [
            "txn.non_xa_policy",
            "txn.prepare_timeout_ms",
            "txn.resolver_interval_seconds",
            "txn.resolver_min_age_seconds",
            "txn.distributed_lock_wait_timeout_seconds",
            "txn.phase2_wait_ms",
            "txn.commit_resolve_timeout_ms",
            "txn.max_branches",
            "txn.abort_record_retention_hours",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
        let s = MemSettings::new(reg);
        let dc = DcId::new("mum1").unwrap();
        let got = TxnConfig::resolve(&s, &SettingCtx::process(&dc), "gateway");
        assert_eq!(got, TxnConfig::default());
        assert_eq!(got.lock_wait_cap(), 10);
        let r = ResolverConfig::resolve(&s, &SettingCtx::process(&dc));
        assert_eq!(r, ResolverConfig::default());
        assert_eq!(NonXaPolicy::parse("best_effort"), NonXaPolicy::BestEffort);
    }
}
