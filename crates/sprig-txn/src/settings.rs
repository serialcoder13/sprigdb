//! Chapter 18's settings, declared next to the code (chapter 03,
//! "Declaring a key"), and the snapshots the coordinator and the resolver
//! read.

use std::borrow::Cow;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_common::{SprigCode, SprigError};
use sprig_settings::{
    Apply, GuardFuture, HardBounds, LiveSettings, MemSettings, Scopes, Setting, SettingCtx, Value,
    ValueGuard,
};

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
    doc: "A write that adds a second cluster without XA: reject (error 39440), best_effort (each cluster commits on its own) or sprig_2pc (held branches: atomic, but if a gateway dies after the commit point other writers can change those rows before the replay)",
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
    /// Sprig 2PC: a branch on a cluster without XA records its writes and
    /// is held as a prepared branch (`crate::redo`); after a crash the
    /// controller's resolver replays or drops its redo rows, so the
    /// transaction stays atomic. The gateway session holds the branch on
    /// its own connection, so a gateway that dies after the commit point
    /// releases the branch's locks before the replay runs, and another
    /// writer can change those rows in between. That is why it stays
    /// opt-in (the default is `reject`) until the relay fence runs in
    /// deployments. A backend that can't record writes
    /// (`start_redo` returns false) gets `reject`'s behaviour.
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
            commit_resolve_timeout: within_window(
                s.get(&COMMIT_RESOLVE_TIMEOUT_MS, ctx),
                s.get(&RESOLVER_MIN_AGE_SECONDS, ctx),
            ),
            distributed_lock_wait_timeout_s: u32_of(
                s.get(&DISTRIBUTED_LOCK_WAIT_TIMEOUT_SECONDS, ctx),
            ),
            session_lock_wait_timeout_s: SESSION_LOCK_WAIT_DEFAULT,
            max_branches: usize::try_from(s.get(&MAX_BRANCHES, ctx)).unwrap_or(32),
            tenant_all_xa: false,
            coordinator_id: coordinator_id.to_owned(),
        }
    }

    /// [`Self::resolve`] with the caller's resolvers (the gateway's live
    /// settings with a tenant's rows): `num` for the numeric keys, `text`
    /// for `txn.non_xa_policy`.
    pub fn resolve_with(
        ctx: &SettingCtx<'_>,
        mut num: impl FnMut(&'static Setting<u64>, &SettingCtx<'_>) -> u64,
        mut text: impl FnMut(&'static Setting<Cow<'static, str>>, &SettingCtx<'_>) -> String,
        coordinator_id: &str,
    ) -> Self {
        Self {
            policy: NonXaPolicy::parse(&text(&NON_XA_POLICY, ctx)),
            prepare_timeout: Duration::from_millis(num(&PREPARE_TIMEOUT_MS, ctx)),
            phase2_wait: Duration::from_millis(num(&PHASE2_WAIT_MS, ctx)),
            commit_resolve_timeout: within_window(
                num(&COMMIT_RESOLVE_TIMEOUT_MS, ctx),
                num(&RESOLVER_MIN_AGE_SECONDS, ctx),
            ),
            distributed_lock_wait_timeout_s: u32_of(num(
                &DISTRIBUTED_LOCK_WAIT_TIMEOUT_SECONDS,
                ctx,
            )),
            session_lock_wait_timeout_s: SESSION_LOCK_WAIT_DEFAULT,
            max_branches: usize::try_from(num(&MAX_BRANCHES, ctx)).unwrap_or(32),
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

/// Q39 (bugfix-62): a coordinator races an unknown commit only until
/// `txn.commit_resolve_timeout_ms` after the commit was sent, and that
/// must end before the resolver may sweep the commit row, which it does
/// once the row is `txn.resolver_min_age_seconds` old. So the timeout must
/// stay strictly below the minimum age.
///
/// # Errors
/// 39080 `ER_SPRIG_SETTING_OUT_OF_BOUNDS` naming both keys and values.
pub fn check_resolve_window(
    commit_resolve_timeout_ms: u64,
    min_age_seconds: u64,
) -> sprig_common::Result<()> {
    let min_age_ms = min_age_seconds.saturating_mul(1000);
    if commit_resolve_timeout_ms < min_age_ms {
        return Ok(());
    }
    Err(SprigError::new(
        SprigCode::SettingOutOfBounds,
        format!(
            "{} ({commit_resolve_timeout_ms} ms) must stay below {} ({min_age_seconds} s = \
             {min_age_ms} ms): otherwise a coordinator can record an abort for a transaction \
             whose commit row the resolver already swept, and tell the client \"rolled back\" \
             for a committed transaction",
            COMMIT_RESOLVE_TIMEOUT_MS.key, RESOLVER_MIN_AGE_SECONDS.key
        ),
    ))
}

/// The timeout a coordinator uses: the setting, kept below the minimum
/// age even where a tenant's own value predates a lower minimum age
/// (the guard checks values against this DC's, not every tenant's).
fn within_window(commit_resolve_timeout_ms: u64, min_age_seconds: u64) -> Duration {
    let cap = min_age_seconds
        .saturating_mul(1000)
        .saturating_sub(1)
        .max(1);
    if commit_resolve_timeout_ms > cap {
        tracing::warn!(
            commit_resolve_timeout_ms,
            min_age_seconds,
            "txn.commit_resolve_timeout_ms isn't below txn.resolver_min_age_seconds; using {cap} ms"
        );
    }
    Duration::from_millis(commit_resolve_timeout_ms.min(cap))
}

/// [`check_resolve_window`] on the values `live` resolves for this
/// process's DC: a controller or gateway refuses to start with an invalid
/// pair.
///
/// # Errors
/// As [`check_resolve_window`].
pub fn check_live(live: &LiveSettings, dc: &DcId) -> sprig_common::Result<()> {
    let ctx = SettingCtx::process(dc);
    check_resolve_window(
        live.get(&COMMIT_RESOLVE_TIMEOUT_MS, &ctx),
        live.get(&RESOLVER_MIN_AGE_SECONDS, &ctx),
    )
}

/// Refuses a `Set` of either key that would break
/// [`check_resolve_window`] against the other key's value at this DC
/// (the settings writers call it before writing; `sprig_settings::guard`).
#[derive(Debug, Clone)]
pub struct ResolveWindowGuard {
    live: LiveSettings,
    dc: DcId,
}

impl ResolveWindowGuard {
    /// A guard reading the other key from `live` at `dc`.
    #[must_use]
    pub fn new(live: LiveSettings, dc: DcId) -> Self {
        Self { live, dc }
    }

    /// The check for `key` set to `value` (the guard's body).
    ///
    /// # Errors
    /// As [`check_resolve_window`].
    pub fn check_value(&self, key: &str, value: &Value) -> sprig_common::Result<()> {
        let Value::Int(v) = value else {
            return Ok(());
        };
        let v = u64::try_from(*v).unwrap_or(0);
        let ctx = SettingCtx::process(&self.dc);
        if key == COMMIT_RESOLVE_TIMEOUT_MS.key {
            check_resolve_window(v, self.live.get(&RESOLVER_MIN_AGE_SECONDS, &ctx))
        } else if key == RESOLVER_MIN_AGE_SECONDS.key {
            check_resolve_window(self.live.get(&COMMIT_RESOLVE_TIMEOUT_MS, &ctx), v)
        } else {
            Ok(())
        }
    }
}

impl ValueGuard for ResolveWindowGuard {
    fn check<'a>(&'a self, key: &'a str, value: &'a Value) -> GuardFuture<'a> {
        Box::pin(async move { self.check_value(key, value) })
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
        let with = TxnConfig::resolve_with(
            &SettingCtx::process(&dc),
            |k, cx| s.get(k, cx),
            |k, cx| s.get(k, cx).into_owned(),
            "gateway",
        );
        assert_eq!(with, got);
        assert_eq!(got.lock_wait_cap(), 10);
        let r = ResolverConfig::resolve(&s, &SettingCtx::process(&dc));
        assert_eq!(r, ResolverConfig::default());
        assert_eq!(NonXaPolicy::parse("best_effort"), NonXaPolicy::BestEffort);
    }

    #[test]
    fn commit_resolve_timeout_stays_below_the_resolvers_minimum_age() {
        // Q39 (bugfix-62): the defaults pass; equal or above is refused.
        check_resolve_window(5000, 30).unwrap();
        check_resolve_window(29_999, 30).unwrap();
        for (ms, s) in [(30_000, 30), (60_000, 30), (100, 0)] {
            let e = check_resolve_window(ms, s).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::SettingOutOfBounds), "{e}");
            let m = e.to_string();
            assert!(
                m.contains("txn.commit_resolve_timeout_ms")
                    && m.contains("txn.resolver_min_age_seconds"),
                "{m}"
            );
        }
        // A coordinator never races past the minimum age, whatever the
        // tenant's value says.
        assert_eq!(within_window(60_000, 30), Duration::from_millis(29_999));
        assert_eq!(within_window(5000, 30), Duration::from_millis(5000));
        let reg = Registry::global().unwrap();
        let mut s = MemSettings::new(reg);
        let dc = DcId::new("mum1").unwrap();
        let cx = SettingCtx::process(&dc);
        s.set_value(
            sprig_settings::Level::Dc,
            &cx,
            "txn.commit_resolve_timeout_ms",
            &Value::Int(45_000),
        )
        .unwrap();
        let got = TxnConfig::resolve(&s, &cx, "gateway");
        assert_eq!(got.commit_resolve_timeout, Duration::from_millis(29_999));
    }
}
