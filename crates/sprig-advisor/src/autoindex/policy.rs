//! Per-object settings, budgets at each scope, bound tightening and the
//! churn dampers (chapter 13, "Settings scopes and budgets" and "Observing
//! and dropping"; build step 16).
//!
//! **Resolution.** Every `auto_index.*` key resolves through
//! `sprig-settings` for the object a candidate indexes (object, database,
//! tenant, ancestors, cluster, dc, global, default: nearest first), with
//! every wider scope's bounds applied and values clamped at read time
//! (chapter 08).
//! [`ObjectPolicy::resolve`] reads them all at once and keeps where each
//! came from, for the decision log.
//!
//! - **Per object**: mode, trigger thresholds, `max_per_table`,
//!   `max_storage_pct`, the improvement and regression limits, observe and
//!   drop periods. A table with `mode = off` gets nothing even when its
//!   tenant is `on`.
//! - **Shared cluster resources** ([`SharedLimits`]):
//!   `max_builds_per_cluster`, `build_window` and
//!   `abort_replica_lag_seconds` resolve along the cluster's chain; a value
//!   set at a data scope (object, database, tenant, an ancestor) can only
//!   tighten them for that tenant's objects.
//! - **`max_new_per_day`** ([`DayCaps`]) applies as a cap at every level
//!   where it is set: the tenant's resolved value caps the tenant, and a
//!   value set on a database or an object also caps that database or
//!   object, inside the tenant's cap.
//! - **Bound tightening**: nothing is dropped when a bound is lowered; the
//!   next selection ([`select`](super::cost::select)) sees the table over
//!   budget and retires one index per evaluation.
//!
//! **Churn dampers** ([`Dampers`]), against chapter 47's index-churn
//! thrash loop: a definition dropped for `no_gain`, `regression` or
//! `write_cost` can't be proposed again for `recreate_block_days`; a
//! fingerprint decided within `cooldown_hours` isn't re-decided; creations
//! per day are capped; and every creation of a definition dropped within
//! the last 30 days counts in `sprig_advisor_autoindex_churn_total`.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use sprig_common::ids::{DbId, Fingerprint, ObjectId};
use sprig_common::{Result, SprigCode};
use sprig_settings::{
    MemSettings, Resolved, ScopeKind, Setting, SettingCtx, SettingValue, Settings, TenantSettings,
    Value, resolve_layered,
};

use super::candidates::GenSettings;
use super::cost::{Budget, CostParams};
use super::model::IndexDef;
use super::settings as k;
use super::store::{AutoIndexRow, CandidateRow, DropReason, IndexState};

/// Where resolved values come from. `Send + Sync` so an evaluation
/// holding one can run inside the controller's (`Send`) reconcile futures.
pub trait SettingSource: Send + Sync {
    /// Resolves `key` in `ctx`.
    ///
    /// # Errors
    /// 39081 for an unknown key; `Internal` for a context naming another
    /// tenant.
    fn resolve(&self, key: &str, ctx: &SettingCtx<'_>) -> Result<Resolved>;
}

impl SettingSource for MemSettings<'_> {
    fn resolve(&self, key: &str, ctx: &SettingCtx<'_>) -> Result<Resolved> {
        MemSettings::resolve(self, key, ctx)
    }
}

/// The process's settings with a tenant's catalog snapshot.
#[derive(Debug, Clone, Copy)]
pub struct TenantSource<'a> {
    /// The process-wide settings (etcd mirrors).
    pub settings: &'a Settings,
    /// The tenant's rows, from its catalog snapshot.
    pub tenant: &'a TenantSettings,
}

impl SettingSource for TenantSource<'_> {
    fn resolve(&self, key: &str, ctx: &SettingCtx<'_>) -> Result<Resolved> {
        self.settings.resolve(key, ctx, Some(self.tenant))
    }
}

/// An etcd snapshot layered under a tenant's rows.
#[derive(Debug, Clone, Copy)]
pub struct Layered<'a> {
    /// Global, dc and cluster rows.
    pub etcd: &'a MemSettings<'a>,
    /// The tenant's rows.
    pub tenant: Option<&'a TenantSettings>,
}

impl SettingSource for Layered<'_> {
    fn resolve(&self, key: &str, ctx: &SettingCtx<'_>) -> Result<Resolved> {
        resolve_layered(self.etcd, self.tenant, key, ctx)
    }
}

/// `auto_index.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Create, validate, roll out and drop automatically.
    On,
    /// Run everything up to validation; stop at `recommended`.
    Recommend,
    /// No evaluation; existing automatic indexes are kept as they are.
    Off,
}

impl Mode {
    /// Parses the setting's text; unknown text is `On` (the registry only
    /// admits the three values).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "recommend" => Mode::Recommend,
            "off" => Mode::Off,
            _ => Mode::On,
        }
    }
}

/// One resolved value as decisions record it.
#[derive(Debug, Clone, PartialEq)]
pub struct Used {
    /// The value.
    pub value: Value,
    /// The scope it came from.
    pub source: ScopeKind,
    /// It was clamped into the bounds in force.
    pub clamped: bool,
}

/// The trigger thresholds of one object (consumed by trigger evaluation,
/// step 2).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TriggerThresholds {
    /// `auto_index.window_hours`.
    pub window_hours: u32,
    /// `auto_index.include_studio`.
    pub include_studio: bool,
    /// `auto_index.min_runs`.
    pub min_runs: u32,
    /// `auto_index.p95_threshold_ms`.
    pub p95_threshold_ms: f64,
    /// `auto_index.top_n`.
    pub top_n: u32,
    /// `auto_index.min_examined_per_returned`.
    pub min_examined_per_returned: f64,
    /// `auto_index.min_examined_samples`.
    pub min_examined_samples: u32,
    /// `auto_index.cooldown_hours`.
    pub cooldown_hours: u32,
    /// `auto_index.scan_min_runs`.
    pub scan_min_runs: u32,
    /// `auto_index.scan_min_examined`.
    pub scan_min_examined: u64,
    /// `auto_index.scan_max_returned_ratio`.
    pub scan_max_returned_ratio: f64,
    /// `auto_index.scan_min_sessions`.
    pub scan_min_sessions: u32,
    /// `auto_index.scan_recommend_above_gb`.
    pub scan_recommend_above_gb: f64,
}

/// The observe and drop rules of one object.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ObserveRules {
    /// `auto_index.min_improvement_pct`.
    pub min_improvement_pct: f64,
    /// `auto_index.max_regression_pct`.
    pub max_regression_pct: f64,
    /// `auto_index.observe_days`.
    pub observe_days: u32,
    /// `auto_index.unused_drop_days`.
    pub unused_drop_days: u32,
    /// `auto_index.ignore_before_drop_hours`.
    pub ignore_before_drop_hours: u32,
    /// `auto_index.recreate_block_days`.
    pub recreate_block_days: u32,
}

/// Every `auto_index.*` value for one object, resolved at the object.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectPolicy {
    /// `auto_index.mode`.
    pub mode: Mode,
    /// Trigger thresholds.
    pub trigger: TriggerThresholds,
    /// `max_per_table` and `max_storage_pct`.
    pub budget: Budget,
    /// Covering limits.
    pub generation: GenSettings,
    /// Cost model parameters (the build rate from the setting; the caller
    /// replaces it with a measured one and sets the estimate margin).
    pub cost: CostParams,
    /// Observation and dropping.
    pub observe: ObserveRules,
    /// `auto_index.estimate_margin`.
    pub estimate_margin: f64,
    /// `auto_index.recommend_validate`.
    pub recommend_validate: bool,
    /// `auto_index.recommend_ttl_days`.
    pub recommend_ttl_days: u32,
    /// Every value read, by key, with its source: the decision log's
    /// "resolved values they used".
    pub used: BTreeMap<&'static str, Used>,
}

struct Reader<'a> {
    src: &'a dyn SettingSource,
    ctx: &'a SettingCtx<'a>,
    used: BTreeMap<&'static str, Used>,
}

impl Reader<'_> {
    fn get<T: SettingValue>(&mut self, s: &'static Setting<T>) -> Result<T> {
        let r = self.src.resolve(s.key, self.ctx)?;
        let v = T::from_value(&r.value).unwrap_or_else(|| s.default.clone());
        self.used.insert(
            s.key,
            Used {
                value: r.value,
                source: r.source,
                clamped: r.clamped,
            },
        );
        Ok(v)
    }
}

fn usize_of(n: u32) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

impl ObjectPolicy {
    /// Resolves every key for the object of `ctx` (which names the tenant,
    /// the object's database and the object).
    ///
    /// # Errors
    /// 39081 if a key is missing from the registry; `Internal` for a
    /// context naming another tenant than the source's.
    pub fn resolve(src: &dyn SettingSource, ctx: &SettingCtx<'_>) -> Result<Self> {
        let mut r = Reader {
            src,
            ctx,
            used: BTreeMap::new(),
        };
        let mode = Mode::parse(&r.get(&k::MODE)?);
        let trigger = TriggerThresholds {
            window_hours: r.get(&k::WINDOW_HOURS)?,
            include_studio: r.get(&k::INCLUDE_STUDIO)?,
            min_runs: r.get(&k::MIN_RUNS)?,
            p95_threshold_ms: r.get(&k::P95_THRESHOLD_MS)?,
            top_n: r.get(&k::TOP_N)?,
            min_examined_per_returned: r.get(&k::MIN_EXAMINED_PER_RETURNED)?,
            min_examined_samples: r.get(&k::MIN_EXAMINED_SAMPLES)?,
            cooldown_hours: r.get(&k::COOLDOWN_HOURS)?,
            scan_min_runs: r.get(&k::SCAN_MIN_RUNS)?,
            scan_min_examined: r.get(&k::SCAN_MIN_EXAMINED)?,
            scan_max_returned_ratio: r.get(&k::SCAN_MAX_RETURNED_RATIO)?,
            scan_min_sessions: r.get(&k::SCAN_MIN_SESSIONS)?,
            scan_recommend_above_gb: r.get(&k::SCAN_RECOMMEND_ABOVE_GB)?,
        };
        let budget = Budget {
            max_per_table: r.get(&k::MAX_PER_TABLE)?,
            max_storage_pct: r.get(&k::MAX_STORAGE_PCT)?,
            min_storage_bytes: r.get(&k::MIN_STORAGE_BUDGET_MB)? * 1024.0 * 1024.0,
        };
        let generation = GenSettings {
            max_cover_columns: usize_of(r.get(&k::MAX_COVER_COLUMNS)?),
            max_key_bytes: u64::from(r.get(&k::MAX_KEY_BYTES)?),
        };
        let observe = ObserveRules {
            min_improvement_pct: r.get(&k::MIN_IMPROVEMENT_PCT)?,
            max_regression_pct: r.get(&k::MAX_REGRESSION_PCT)?,
            observe_days: r.get(&k::OBSERVE_DAYS)?,
            unused_drop_days: r.get(&k::UNUSED_DROP_DAYS)?,
            ignore_before_drop_hours: r.get(&k::IGNORE_BEFORE_DROP_HOURS)?,
            recreate_block_days: r.get(&k::RECREATE_BLOCK_DAYS)?,
        };
        let cost = CostParams {
            window_hours: f64::from(trigger.window_hours),
            lookup_cost_factor: r.get(&k::LOOKUP_COST_FACTOR)?,
            maintenance_cost_ms: r.get(&k::MAINTENANCE_COST_MS)?,
            min_net_ms_per_hour: r.get(&k::MIN_NET_MS_PER_HOUR)?,
            min_benefit_ratio: r.get(&k::MIN_BENEFIT_RATIO)?,
            max_build_hours: r.get(&k::MAX_BUILD_HOURS)?,
            payback_hours: r.get(&k::PAYBACK_HOURS)?,
            min_improvement_pct: observe.min_improvement_pct,
            build_rate_bytes_per_sec: r.get(&k::BUILD_RATE_MB_PER_SEC)? * 1e6,
            estimate_margin: None,
        };
        let estimate_margin = r.get(&k::ESTIMATE_MARGIN)?;
        let recommend_validate = r.get(&k::RECOMMEND_VALIDATE)?;
        let recommend_ttl_days = r.get(&k::RECOMMEND_TTL_DAYS)?;
        Ok(Self {
            mode,
            trigger,
            budget,
            generation,
            cost,
            observe,
            estimate_margin,
            recommend_validate,
            recommend_ttl_days,
            used: r.used,
        })
    }

    /// The cost parameters for a cluster: `measured` build rate when one
    /// is known, and the estimate margin when there is no replica to
    /// validate on.
    #[must_use]
    pub fn cost_for(&self, measured_rate: Option<f64>, has_replica: bool) -> CostParams {
        CostParams {
            build_rate_bytes_per_sec: measured_rate.unwrap_or(self.cost.build_rate_bytes_per_sec),
            estimate_margin: (!has_replica).then_some(self.estimate_margin),
            ..self.cost
        }
    }

    /// The resolved values as JSON for a decision's evidence:
    /// `{"auto_index.max_per_table": {"value": 5, "source": "default"}}`.
    #[must_use]
    pub fn evidence(&self) -> serde_json::Value {
        let m: serde_json::Map<String, serde_json::Value> = self
            .used
            .iter()
            .map(|(key, u)| {
                (
                    (*key).to_owned(),
                    serde_json::json!({
                        "value": u.value,
                        "source": u.source.to_string(),
                        "clamped": u.clamped,
                    }),
                )
            })
            .collect();
        serde_json::Value::Object(m)
    }
}

/// Where a selected candidate goes next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Nothing: `mode = off`.
    Skip,
    /// Validate, then roll out (`mode = on`).
    ValidateThenRollOut,
    /// Validate, then stop at `recommended`.
    ValidateThenRecommend,
    /// Stop at `recommended` without validating.
    Recommend,
}

impl Route {
    /// The first state of the index row, if any.
    #[must_use]
    pub const fn first_state(self) -> Option<IndexState> {
        match self {
            Route::Skip => None,
            Route::ValidateThenRollOut | Route::ValidateThenRecommend => {
                Some(IndexState::Validating)
            }
            Route::Recommend => Some(IndexState::Recommended),
        }
    }
}

/// Chapter 13's routing: `recommend` mode, and a fast-trigger candidate
/// on a table above `scan_recommend_above_gb` (`advise`), validate only
/// when `recommend_validate` is on and a validation node exists, then stop
/// at `recommended`.
#[must_use]
pub fn route(mode: Mode, advise: bool, recommend_validate: bool, validation_node: bool) -> Route {
    match (mode, advise) {
        (Mode::Off, _) => Route::Skip,
        (Mode::On, false) => Route::ValidateThenRollOut,
        (Mode::Recommend, _) | (Mode::On, true) => {
            if recommend_validate && validation_node {
                Route::ValidateThenRecommend
            } else {
                Route::Recommend
            }
        }
    }
}

/// A build window: `auto` or a fixed UTC range of minutes of the day
/// (`start == end` is the whole day; `start > end` wraps midnight).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildWindow {
    /// The least busy 4 hours of the past 7 days, chosen by the scheduler.
    Auto,
    /// `HH:MM-HH:MM` UTC.
    Fixed {
        /// Start, minutes after midnight.
        start: u16,
        /// End, minutes after midnight.
        end: u16,
    },
}

impl BuildWindow {
    /// Parses `auto` or `HH:MM-HH:MM`; `None` for anything else.
    #[must_use]
    #[allow(clippy::many_single_char_names)] // h and m: hours and minutes
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("auto") {
            return Some(BuildWindow::Auto);
        }
        let (a, b) = s.split_once('-')?;
        let hm = |x: &str| -> Option<u16> {
            let (h, m) = x.trim().split_once(':')?;
            let (h, m): (u16, u16) = (h.parse().ok()?, m.parse().ok()?);
            (h < 24 && m < 60).then_some(h * 60 + m)
        };
        Some(BuildWindow::Fixed {
            start: hm(a)?,
            end: hm(b)?,
        })
    }

    fn minutes(self) -> Vec<bool> {
        let mut v = vec![false; 1440];
        if let BuildWindow::Fixed { start, end } = self {
            let (s, e) = (usize::from(start), usize::from(end));
            if s == e {
                v.fill(true);
            } else {
                let mut i = s;
                while i != e {
                    v[i] = true;
                    i = (i + 1) % 1440;
                }
            }
        }
        v
    }

    /// Whether minute-of-day `m` (UTC) is inside a fixed window (always
    /// true for `auto`, which the scheduler narrows).
    #[must_use]
    pub fn contains(self, m: u16) -> bool {
        match self {
            BuildWindow::Auto => true,
            BuildWindow::Fixed { .. } => self.minutes()[usize::from(m % 1440)],
        }
    }

    /// The narrower of a cluster's window and a tenant's: a fixed tenant
    /// window inside `auto` is the tenant's (the scheduler intersects it
    /// with the automatic one); two fixed windows give their overlap, or
    /// the cluster's when they don't overlap or the overlap isn't one
    /// range.
    #[must_use]
    pub fn tighten(self, data: BuildWindow) -> BuildWindow {
        match (self, data) {
            (c, BuildWindow::Auto) => c,
            (BuildWindow::Auto, d) => d,
            (c, d) => {
                let (a, b) = (c.minutes(), d.minutes());
                let both: Vec<bool> = a.iter().zip(&b).map(|(x, y)| *x && *y).collect();
                match one_range(&both) {
                    Some((start, end)) => BuildWindow::Fixed { start, end },
                    None => c,
                }
            }
        }
    }
}

/// The single circular range of set minutes, if they form one.
fn one_range(m: &[bool]) -> Option<(u16, u16)> {
    let n = m.len();
    if m.iter().all(|x| *x) {
        return Some((0, 0));
    }
    let starts: Vec<usize> = (0..n).filter(|&i| m[i] && !m[(i + n - 1) % n]).collect();
    if starts.len() != 1 {
        return None;
    }
    let s = starts[0];
    let mut e = s;
    while m[e % n] {
        e += 1;
    }
    Some((u16::try_from(s).ok()?, u16::try_from(e % n).ok()?))
}

/// The shared cluster resources for one tenant's object on one cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedLimits {
    /// `auto_index.max_builds_per_cluster`.
    pub max_builds_per_cluster: u32,
    /// `auto_index.build_window`.
    pub build_window: BuildWindow,
    /// `auto_index.abort_replica_lag_seconds`.
    pub abort_replica_lag_seconds: u32,
}

const fn is_data(s: ScopeKind) -> bool {
    matches!(
        s,
        ScopeKind::Object | ScopeKind::Database | ScopeKind::Tenant | ScopeKind::Ancestor(_)
    )
}

impl SharedLimits {
    /// Resolves the three keys along the cluster's chain (`cluster_ctx`:
    /// dc and cluster, no tenant), then lets a value set at a data scope of
    /// `data_ctx` (the object) only tighten them.
    ///
    /// # Errors
    /// As [`ObjectPolicy::resolve`]; `max_builds_per_cluster` falls back to
    /// its default 1 where it isn't registered (it is declared in
    /// [`settings`](super::settings), next to the rollout).
    pub fn resolve(
        src: &dyn SettingSource,
        cluster_ctx: &SettingCtx<'_>,
        data_ctx: &SettingCtx<'_>,
    ) -> Result<Self> {
        let num = |key: &str, ctx: &SettingCtx<'_>| -> Result<Option<(u32, ScopeKind)>> {
            match src.resolve(key, ctx) {
                Ok(r) => Ok(r.value.as_f64().map(|v| (num_u32(v), r.source))),
                Err(e) if e.code() == Some(SprigCode::SettingUnknown) => Ok(None),
                Err(e) => Err(e),
            }
        };
        let tight = |key: &str, default: u32| -> Result<u32> {
            let c = num(key, cluster_ctx)?.map_or(default, |(v, _)| v);
            Ok(match num(key, data_ctx)? {
                Some((d, s)) if is_data(s) => c.min(d),
                _ => c,
            })
        };
        let window = |ctx: &SettingCtx<'_>| -> Result<(BuildWindow, ScopeKind)> {
            let r = src.resolve(k::BUILD_WINDOW.key, ctx)?;
            let w = match &r.value {
                Value::Str(s) => BuildWindow::parse(s).unwrap_or(BuildWindow::Auto),
                _ => BuildWindow::Auto,
            };
            Ok((w, r.source))
        };
        let (cw, _) = window(cluster_ctx)?;
        let (dw, ds) = window(data_ctx)?;
        Ok(Self {
            max_builds_per_cluster: tight(k::MAX_BUILDS_PER_CLUSTER_KEY, 1)?,
            build_window: if is_data(ds) { cw.tighten(dw) } else { cw },
            abort_replica_lag_seconds: tight(
                k::ABORT_REPLICA_LAG_SECONDS.key,
                k::ABORT_REPLICA_LAG_SECONDS.default,
            )?,
        })
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn num_u32(v: f64) -> u32 {
    v.clamp(0.0, f64::from(u32::MAX)) as u32
}

/// `max_new_per_day` at each level where it applies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DayCaps {
    /// The tenant's cap.
    pub tenant: u32,
    /// Caps set on databases.
    pub databases: BTreeMap<DbId, u32>,
    /// Caps set on objects.
    pub objects: BTreeMap<ObjectId, u32>,
}

impl DayCaps {
    /// Resolves the caps for `objects` (each with its database) under the
    /// tenant context `tenant_ctx`.
    ///
    /// # Errors
    /// As [`ObjectPolicy::resolve`].
    pub fn resolve(
        src: &dyn SettingSource,
        tenant_ctx: &SettingCtx<'_>,
        objects: &[(DbId, ObjectId)],
    ) -> Result<Self> {
        let key = k::MAX_NEW_PER_DAY.key;
        let get = |ctx: &SettingCtx<'_>| -> Result<(u32, ScopeKind)> {
            let r = src.resolve(key, ctx)?;
            Ok((
                r.value.as_f64().map_or(k::MAX_NEW_PER_DAY.default, num_u32),
                r.source,
            ))
        };
        let mut caps = DayCaps {
            tenant: get(&SettingCtx {
                db: None,
                object: None,
                ..*tenant_ctx
            })?
            .0,
            ..DayCaps::default()
        };
        for (db, obj) in objects {
            let dctx = tenant_ctx.with_db(*db);
            if let (v, ScopeKind::Database) = get(&dctx)? {
                caps.databases.insert(*db, v);
            }
            if let (v, ScopeKind::Object) = get(&dctx.with_object(*obj))? {
                caps.objects.insert(*obj, v);
            }
        }
        Ok(caps)
    }

    /// Whether one more index on `object` (in `db`) fits, given the
    /// indexes created in the last day, as (database, object) pairs.
    #[must_use]
    pub fn admits(&self, created: &[(DbId, ObjectId)], db: DbId, object: ObjectId) -> bool {
        let n = |f: &dyn Fn(&(DbId, ObjectId)) -> bool| {
            u32::try_from(created.iter().filter(|x| f(x)).count()).unwrap_or(u32::MAX)
        };
        if n(&|_| true) >= self.tenant {
            return false;
        }
        if self
            .databases
            .get(&db)
            .is_some_and(|c| n(&|x| x.0 == db) >= *c)
        {
            return false;
        }
        self.objects
            .get(&object)
            .is_none_or(|c| n(&|x| x.1 == object) < *c)
    }
}

/// What the dampers say about a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Damped {
    /// Go ahead.
    Allow,
    /// Its definition was dropped as harmful recently.
    RecreateBlocked(DropReason),
    /// Its fingerprints were decided within the cooldown.
    Cooldown,
}

impl Damped {
    /// The candidate's `reason`, if it is held back.
    #[must_use]
    pub fn reason(self) -> Option<String> {
        match self {
            Damped::Allow => None,
            Damped::RecreateBlocked(r) => Some(format!("recreate_blocked:{r}")),
            Damped::Cooldown => Some("cooldown".into()),
        }
    }
}

/// The window in which a re-creation counts as churn (chapter 13: 30
/// days).
pub const CHURN_WINDOW: Duration = Duration::from_secs(30 * 86_400);

/// The churn dampers of one evaluation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Dampers {
    /// `auto_index.recreate_block_days`.
    pub recreate_block: Duration,
    /// `auto_index.cooldown_hours`.
    pub cooldown: Duration,
}

impl Dampers {
    /// The dampers of `p`.
    #[must_use]
    pub fn of(p: &ObjectPolicy) -> Self {
        Self {
            recreate_block: Duration::from_secs(u64::from(p.observe.recreate_block_days) * 86_400),
            cooldown: Duration::from_secs(u64::from(p.trigger.cooldown_hours) * 3600),
        }
    }

    /// Checks `def` serving `fps` at `now` against the object's dropped
    /// index rows (`dropped`, from
    /// [`dropped_since`](super::store::AutoIndexStore::dropped_since)) and
    /// the tenant's recently decided candidates (`decided`).
    #[must_use]
    pub fn check(
        &self,
        def: &IndexDef,
        fps: &[Fingerprint],
        dropped: &[AutoIndexRow],
        decided: &[CandidateRow],
        now: SystemTime,
    ) -> Damped {
        let name = def.name();
        let within = |t: SystemTime, d: Duration| now.duration_since(t).map_or(true, |a| a < d);
        if let Some(r) = dropped.iter().find(|r| {
            r.object == def.object
                && r.name == name
                && r.state == IndexState::Dropped
                && r.drop_reason.is_some_and(DropReason::blocks_recreate)
                && within(r.state_changed_at, self.recreate_block)
        }) {
            return Damped::RecreateBlocked(r.drop_reason.unwrap_or(DropReason::NoGain));
        }
        if decided.iter().any(|c| {
            within(c.created_at, self.cooldown)
                && c.fingerprints.iter().any(|(f, _)| fps.contains(f))
        }) {
            return Damped::Cooldown;
        }
        Damped::Allow
    }
}

/// Whether creating `def` now re-creates a definition dropped within
/// [`CHURN_WINDOW`]; if so it is counted in
/// `sprig_advisor_autoindex_churn_total` (chapter 47's index-churn signal).
#[must_use]
pub fn record_churn(def: &IndexDef, dropped: &[AutoIndexRow], now: SystemTime) -> bool {
    let name = def.name();
    let churn = dropped.iter().any(|r| {
        r.object == def.object
            && r.name == name
            && r.state == IndexState::Dropped
            && now
                .duration_since(r.state_changed_at)
                .map_or(true, |a| a < CHURN_WINDOW)
    });
    if churn {
        metrics::counter!("sprig_advisor_autoindex_churn_total").increment(1);
    }
    churn
}

#[cfg(test)]
mod tests;
