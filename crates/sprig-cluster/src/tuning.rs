//! Server tuning: sizing `table_definition_cache`, `table_open_cache`,
//! `innodb_open_files`, `open_files_limit` and `stored_program_cache` from
//! what a cluster holds, within a memory budget, with hysteresis.
//!
//! ```text
//! plan(cluster):                       # same values on every node
//!   1. def := clamp(ceil(T × 1.1) + 400, 400, 2097152)          # table_definition_cache
//!   2. toc := clamp(ceil(Ta × 1.2) + P × 8, 2000, 1048576)        # table_open_cache
//!   3. iof := max(toc, 400)                                       # innodb_open_files
//!   4. ofl := toc + iof + C × 2 + 1000                            # open_files_limit
//!   5. spc := clamp(next_pow2(Rmax × 2), 256, 4096)               # stored_program_cache
//!   6. mem := def × c_def + toc × c_toc + C × spc × c_sp × u_sp
//!   7. while mem > M × tuning.memory_budget_pct / 100:
//!        shrink spc, then toc (not below Ta), then def (not below T);
//!        if still over: "over table budget"; the balancer stops placing here
//!   8. change a variable only if it moves by tuning.min_change_pct or is below floor
//!   9. grow at once; shrink only after the smaller value was wanted for 24 h
//! ```
//!
//! [`want`] is steps 1 to 7, pure. [`Tuner`] keeps the per-cluster memory
//! for steps 8 and 9 and the rules for applying: `table_open_cache` closes
//! every open table when set, so it changes only on growth of at least
//! `tuning.min_change_pct` (or after 24 h of wanting less) and never more
//! often than every `tuning.interval_minutes`; `innodb_open_files` and
//! `open_files_limit` are read-only (1238) on 11.8 and 12.3, so they go to
//! `pending_restart`, and `table_open_cache` is capped meanwhile by the
//! file descriptors the running server has; if the miss rate doesn't fall
//! within an hour of a growth, the next growth needs twice the margin.
//!
//! Per-entry costs were measured in the month-1 spike (about 20 KiB per
//! table for a definition and one open instance) and re-measured by this
//! step's scale test: [`Costs::default`].

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

use crate::probe::parse::TableCacheInfo;

/// `tuning.table_open_cache_mode`: `auto`, `recommend` or `off`.
pub static MODE: Setting<std::borrow::Cow<'static, str>> = Setting {
    key: "tuning.table_open_cache_mode",
    default: std::borrow::Cow::Borrowed("auto"),
    scopes: Scopes::OPERATIONAL,
    hard: HardBounds::one_of(&["auto", "recommend", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 9,
    doc: "auto, recommend or off",
};
sprig_settings::register!(MODE);

macro_rules! ops_key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::OPERATIONAL,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 9,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

ops_key! {
    /// `tuning.memory_budget_pct`.
    MEMORY_BUDGET_PCT: u32 = "tuning.memory_budget_pct", 10, 1, 90,
    "Server memory the table and routine caches may use"
}
ops_key! {
    /// `tuning.min_change_pct`.
    MIN_CHANGE_PCT: u32 = "tuning.min_change_pct", 20, 0, 1000,
    "Smallest change worth applying or recommending"
}
ops_key! {
    /// `tuning.interval_minutes`.
    INTERVAL_MINUTES: u64 = "tuning.interval_minutes", 15, 1, 10_080,
    "Tuning pass period"
}

/// `tuning.table_open_cache_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TuningMode {
    /// Managed clusters tuned live; external ones get recommendations.
    Auto,
    /// Recommendations only.
    Recommend,
    /// Nothing.
    Off,
}

/// The `tuning.*` keys, resolved for one cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuningSettings {
    /// The mode.
    pub mode: TuningMode,
    /// Share of server memory the caches may use, percent.
    pub memory_budget_pct: u32,
    /// Smallest change worth making, percent.
    pub min_change_pct: u32,
    /// Pass period.
    pub interval: Duration,
}

impl Default for TuningSettings {
    fn default() -> Self {
        Self {
            mode: TuningMode::Auto,
            memory_budget_pct: MEMORY_BUDGET_PCT.default,
            min_change_pct: MIN_CHANGE_PCT.default,
            interval: Duration::from_secs(INTERVAL_MINUTES.default * 60),
        }
    }
}

impl TuningSettings {
    /// Resolves the keys in `ctx` (a context with the cluster set).
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self {
            mode: match &*s.get(&MODE, ctx) {
                "auto" => TuningMode::Auto,
                "recommend" => TuningMode::Recommend,
                _ => TuningMode::Off,
            },
            memory_budget_pct: s.get(&MEMORY_BUDGET_PCT, ctx),
            min_change_pct: s.get(&MIN_CHANGE_PCT, ctx),
            interval: Duration::from_secs(s.get(&INTERVAL_MINUTES, ctx) * 60),
        }
    }
}

/// A server variable tuning sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerVar {
    /// `table_definition_cache`.
    TableDefinitionCache,
    /// `table_open_cache`.
    TableOpenCache,
    /// `innodb_open_files`.
    InnodbOpenFiles,
    /// `open_files_limit`.
    OpenFilesLimit,
    /// `stored_program_cache`.
    StoredProgramCache,
}

impl ServerVar {
    /// Every variable, in plan order.
    pub const ALL: [ServerVar; 5] = [
        ServerVar::TableDefinitionCache,
        ServerVar::TableOpenCache,
        ServerVar::InnodbOpenFiles,
        ServerVar::OpenFilesLimit,
        ServerVar::StoredProgramCache,
    ];

    /// The MariaDB name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::TableDefinitionCache => "table_definition_cache",
            Self::TableOpenCache => "table_open_cache",
            Self::InnodbOpenFiles => "innodb_open_files",
            Self::OpenFilesLimit => "open_files_limit",
            Self::StoredProgramCache => "stored_program_cache",
        }
    }

    /// Whether `SET GLOBAL` can change it (measured on 11.8 and 12.3:
    /// `innodb_open_files` and `open_files_limit` give 1238).
    #[must_use]
    pub const fn dynamic(self) -> bool {
        !matches!(self, Self::InnodbOpenFiles | Self::OpenFilesLimit)
    }

    /// The built-in floor below which a value is always raised.
    #[must_use]
    pub const fn floor(self) -> u64 {
        match self {
            Self::TableDefinitionCache | Self::InnodbOpenFiles => 400,
            Self::TableOpenCache => 2000,
            Self::OpenFilesLimit => 0,
            Self::StoredProgramCache => 256,
        }
    }
}

impl fmt::Display for ServerVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How a wanted value is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyMode {
    /// `SET GLOBAL` on every node, and the configuration.
    Live,
    /// The configuration, for the next restart (`pending_restart`).
    NextRestart,
    /// A recommendation only.
    RecommendOnly,
}

/// What a cluster holds and does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuningInputs {
    /// `T`: tables on the cluster.
    pub tables: u64,
    /// `R`: routines on the cluster.
    pub routines: u64,
    /// `Ta`: tables used in the last 24 h.
    pub tables_used: u64,
    /// `P`: peak `Threads_running`.
    pub peak_threads_running: u64,
    /// `C`: `max_connections`.
    pub max_connections: u64,
    /// `Rmax`: most routines in one schema.
    pub max_routines_per_schema: u64,
    /// `M`: server memory, bytes; `None` means unknown (recommend only).
    pub memory_bytes: Option<u64>,
}

/// Per-entry cache costs.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Costs {
    /// One table definition, bytes.
    pub def_bytes: f64,
    /// One open table instance, bytes.
    pub toc_bytes: f64,
    /// One cached routine per connection, bytes.
    pub sp_bytes: f64,
    /// `u_sp`: the share of connections whose routine cache is full.
    pub sp_use: f64,
}

impl Default for Costs {
    /// 12 KiB per definition and 8 KiB per open instance (20 KiB per table
    /// together, from `Memory_used` in the month-1 spike), 32 KiB per
    /// cached routine, a tenth of connections with a full routine cache.
    fn default() -> Self {
        Self {
            def_bytes: 12.0 * 1024.0,
            toc_bytes: 8.0 * 1024.0,
            sp_bytes: 32.0 * 1024.0,
            sp_use: 0.1,
        }
    }
}

/// Steps 1 to 7.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Wanted {
    /// `table_definition_cache`.
    pub def: u64,
    /// `table_open_cache`.
    pub toc: u64,
    /// `innodb_open_files`.
    pub iof: u64,
    /// `open_files_limit`.
    pub ofl: u64,
    /// `stored_program_cache`.
    pub spc: u64,
    /// Estimated cache memory, bytes.
    pub mem_bytes: f64,
    /// Over the memory budget even at the smallest values.
    pub over_budget: bool,
}

impl Wanted {
    /// The value for `v`.
    #[must_use]
    pub const fn get(&self, v: ServerVar) -> u64 {
        match v {
            ServerVar::TableDefinitionCache => self.def,
            ServerVar::TableOpenCache => self.toc,
            ServerVar::InnodbOpenFiles => self.iof,
            ServerVar::OpenFilesLimit => self.ofl,
            ServerVar::StoredProgramCache => self.spc,
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn mem(def: u64, toc: u64, c: u64, spc: u64, k: &Costs) -> f64 {
    def as f64 * k.def_bytes
        + toc as f64 * k.toc_bytes
        + c as f64 * spc as f64 * k.sp_bytes * k.sp_use
}

/// Steps 1 to 7 of the plan.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn want(i: &TuningInputs, k: &Costs, budget_pct: u32) -> Wanted {
    let t = i.tables;
    let ta = i.tables_used;
    let c = i.max_connections;
    // ceil(T × 1.1) and ceil(Ta × 1.2) in integers.
    let mut def = (t.saturating_mul(11).div_ceil(10) + 400).clamp(400, 2_097_152);
    let mut toc =
        (ta.saturating_mul(12).div_ceil(10) + i.peak_threads_running * 8).clamp(2000, 1_048_576);
    let mut spc = (i.max_routines_per_schema * 2)
        .next_power_of_two()
        .clamp(256, 4096);
    let mut over = false;
    if let Some(m) = i.memory_bytes {
        let budget = m as f64 * f64::from(budget_pct) / 100.0;
        // Shrink spc, then toc (not below Ta), then def (not below T).
        while mem(def, toc, c, spc, k) > budget && spc > 256 {
            spc /= 2;
        }
        if mem(def, toc, c, spc, k) > budget {
            let floor = ta.max(2000).min(toc);
            let room = budget - mem(def, 0, c, spc, k);
            let fit = if room > 0.0 {
                (room / k.toc_bytes) as u64
            } else {
                0
            };
            toc = fit.clamp(floor, toc);
        }
        if mem(def, toc, c, spc, k) > budget {
            let floor = t.max(400).min(def);
            let room = budget - mem(0, toc, c, spc, k);
            let fit = if room > 0.0 {
                (room / k.def_bytes) as u64
            } else {
                0
            };
            def = fit.clamp(floor, def);
        }
        over = mem(def, toc, c, spc, k) > budget;
    }
    let iof = toc.max(400);
    let ofl = toc + iof + c * 2 + 1000;
    Wanted {
        def,
        toc,
        iof,
        ofl,
        spc,
        mem_bytes: mem(def, toc, c, spc, k),
        over_budget: over,
    }
}

/// The values a server runs with, from a probe.
#[must_use]
pub fn current_of(t: &TableCacheInfo, stored_program_cache: u64) -> BTreeMap<ServerVar, u64> {
    BTreeMap::from([
        (ServerVar::TableDefinitionCache, t.table_definition_cache),
        (ServerVar::TableOpenCache, t.table_open_cache),
        (ServerVar::InnodbOpenFiles, t.innodb_open_files),
        (ServerVar::OpenFilesLimit, t.open_files_limit),
        (ServerVar::StoredProgramCache, stored_program_cache),
    ])
}

/// One change in a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    /// The variable.
    pub var: ServerVar,
    /// The running value.
    pub from: u64,
    /// The wanted value.
    pub to: u64,
    /// How it is applied.
    pub mode: ApplyMode,
}

/// A tuning plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TuningPlan {
    /// The changes worth making now.
    pub wanted: Vec<Change>,
    /// Estimated cache memory at the wanted values, bytes.
    pub mem_bytes: u64,
    /// Over the table budget: the balancer stops placing here.
    pub over_budget: bool,
    /// Why some wanted values wait (for logs and `cluster show`).
    pub held: Vec<String>,
}

impl TuningPlan {
    /// The plan as statements: `SET GLOBAL` for what applies live (and for
    /// recommendations of dynamic variables), comments for the rest.
    #[must_use]
    pub fn statements(&self) -> Vec<String> {
        self.wanted
            .iter()
            .map(|c| {
                if c.var.dynamic() {
                    format!("SET GLOBAL {} = {};", c.var, c.to)
                } else {
                    format!(
                        "-- {} = {} in the server configuration (read-only; next restart)",
                        c.var, c.to
                    )
                }
            })
            .collect()
    }
}

/// Per-cluster tuning memory: steps 8 and 9 and the rules of "Applying".
#[derive(Debug, Clone, Default)]
pub struct Tuner {
    smaller_since: BTreeMap<ServerVar, u64>,
    last_toc_change: Option<u64>,
    /// The last growth of `table_open_cache`: when, and the miss rate
    /// before it.
    last_growth: Option<(u64, f64)>,
    margin_mult: u32,
}

/// Shrinks wait this long.
pub const SHRINK_AFTER_SECS: u64 = 86_400;
/// A growth that doesn't cut misses within this long doubles the margin.
pub const GROWTH_CHECK_SECS: u64 = 3600;

impl Tuner {
    /// A tuner with no history.
    #[must_use]
    pub fn new() -> Self {
        Self {
            margin_mult: 1,
            ..Self::default()
        }
    }

    /// The current growth margin multiplier (1, 2, 4 or 8).
    #[must_use]
    pub fn margin_mult(&self) -> u32 {
        self.margin_mult.max(1)
    }

    /// Plans one pass at `now` (seconds). `managed` clusters are tuned
    /// live; others get recommendations. `miss_rate` is the current table
    /// cache misses per second.
    #[allow(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn plan(
        &mut self,
        inputs: &TuningInputs,
        current: &BTreeMap<ServerVar, u64>,
        costs: &Costs,
        s: &TuningSettings,
        managed: bool,
        miss_rate: f64,
        now: u64,
    ) -> TuningPlan {
        let w = want(inputs, costs, s.memory_budget_pct);
        let mut plan = TuningPlan {
            wanted: Vec::new(),
            mem_bytes: w.mem_bytes as u64,
            over_budget: w.over_budget,
            held: Vec::new(),
        };
        if s.mode == TuningMode::Off {
            return plan;
        }
        // The ratchet guard: a growth that didn't cut misses.
        if let Some((at, before)) = self.last_growth
            && now >= at + GROWTH_CHECK_SECS
        {
            if miss_rate >= before * 0.5 && before > 0.0 {
                self.margin_mult = (self.margin_mult.max(1) * 2).min(8);
            } else {
                self.margin_mult = 1;
            }
            self.last_growth = None;
        }
        let live = managed && s.mode == TuningMode::Auto && inputs.memory_bytes.is_some();
        let pct = f64::from(s.min_change_pct) / 100.0;
        let cur = |v: ServerVar| current.get(&v).copied().unwrap_or(0);
        // Until open_files_limit is raised, table_open_cache stays within
        // the descriptors the running server has.
        let fd_cap = cur(ServerVar::OpenFilesLimit)
            .saturating_sub(cur(ServerVar::InnodbOpenFiles))
            .saturating_sub(inputs.max_connections * 2 + 1000)
            .max(ServerVar::TableOpenCache.floor());
        for v in ServerVar::ALL {
            let from = cur(v);
            let mut to = w.get(v);
            if v == ServerVar::TableOpenCache && cur(ServerVar::OpenFilesLimit) > 0 && to > fd_cap {
                plan.held.push(format!(
                    "table_open_cache capped at {fd_cap} until open_files_limit is raised"
                ));
                to = fd_cap;
            }
            if to == from {
                self.smaller_since.remove(&v);
                continue;
            }
            let below_floor = from < v.floor();
            let margin = if v == ServerVar::TableOpenCache {
                pct * f64::from(self.margin_mult())
            } else {
                pct
            };
            let moved = (to as f64 - from as f64).abs() / (from.max(1) as f64);
            if to > from {
                self.smaller_since.remove(&v);
                if !below_floor && moved < margin {
                    continue;
                }
                if v == ServerVar::TableOpenCache
                    && let Some(t) = self.last_toc_change
                    && now < t + s.interval.as_secs()
                {
                    plan.held.push(format!(
                        "table_open_cache waits for tuning.interval_minutes (changed at {t})"
                    ));
                    continue;
                }
            } else {
                if moved < pct {
                    self.smaller_since.remove(&v);
                    continue;
                }
                let since = *self.smaller_since.entry(v).or_insert(now);
                if now < since + SHRINK_AFTER_SECS {
                    plan.held
                        .push(format!("{v} shrinks to {to} after 24 h of wanting it"));
                    continue;
                }
            }
            let mode = if !live {
                ApplyMode::RecommendOnly
            } else if v.dynamic() {
                ApplyMode::Live
            } else {
                ApplyMode::NextRestart
            };
            plan.wanted.push(Change {
                var: v,
                from,
                to,
                mode,
            });
        }
        plan
    }

    /// Records that `plan` was applied at `now` with `miss_rate` before it.
    pub fn applied(&mut self, plan: &TuningPlan, miss_rate: f64, now: u64) {
        for c in &plan.wanted {
            self.smaller_since.remove(&c.var);
            if c.var == ServerVar::TableOpenCache && c.mode == ApplyMode::Live {
                self.last_toc_change = Some(now);
                if c.to > c.from {
                    self.last_growth = Some((now, miss_rate));
                }
            }
        }
    }
}

/// The tuning interface.
#[async_trait::async_trait]
pub trait ServerTuner: Send + Sync {
    /// Plans a pass for `cluster`.
    ///
    /// # Errors
    /// Probe or input errors.
    async fn plan(&self, cluster: &ClusterId) -> Result<TuningPlan>;
    /// Applies a plan (managed clusters only).
    ///
    /// # Errors
    /// The server's.
    async fn apply(&self, plan: &TuningPlan) -> Result<()>;
}

/// Applies the `Live` changes of `plan` (and, with `recommendations`, the
/// dynamic recommendations too: the dev and scale-test path) on one node
/// through an administrative connection; returns what was set. Read-only
/// variables are never set (1238).
///
/// # Errors
/// The first `SET GLOBAL` that fails.
pub async fn apply_on_node<Q: Queryable + Send>(
    q: &mut Q,
    plan: &TuningPlan,
    recommendations: bool,
) -> Result<Vec<(ServerVar, u64)>> {
    let mut done = Vec::new();
    for c in &plan.wanted {
        let go = c.var.dynamic()
            && (c.mode == ApplyMode::Live
                || (recommendations && c.mode == ApplyMode::RecommendOnly));
        if !go {
            continue;
        }
        q.query_drop(format!("SET GLOBAL {} = {}", c.var.name(), c.to))
            .await
            .map_err(|e| {
                SprigError::new(
                    SprigCode::ClusterCapabilityRequired,
                    format!("SET GLOBAL {} failed", c.var),
                )
                .with_source(e)
            })?;
        done.push((c.var, c.to));
    }
    Ok(done)
}

/// Tables and routines in one schema, by equality on `table_schema`
/// (never an `IN` list or `LIKE`, which scan the whole server).
///
/// # Errors
/// The server's.
pub async fn count_schema<Q: Queryable + Send>(q: &mut Q, schema: &str) -> Result<(u64, u64)> {
    let err = |e| SprigError::new(SprigCode::ClusterUnavailable, "count failed").with_source(e);
    let tables: Option<u64> = q
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema = ?",
            (schema,),
        )
        .await
        .map_err(err)?;
    let routines: Option<u64> = q
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE routine_schema = ?",
            (schema,),
        )
        .await
        .map_err(err)?;
    Ok((tables.unwrap_or(0), routines.unwrap_or(0)))
}

#[cfg(test)]
mod tests;
