//! Placement (chapter 41, "Placement"; build step 9 in part): the tier a
//! version runs in, and why.
//!
//! As built: steps 1 to 3 (language, effects, bounds and cost) come from
//! the compiler's analysis. Step 4, locality, is answered only for groups
//! that are always one shard (every tenant until R1's sharding): the route
//! is the whole group. Code with data access in a sharded group goes to
//! the engine tier with `db.unrouted`. Steps 5 to 7 (gateway-only writes,
//! kinds other than functions, cluster capabilities) have nothing to
//! check yet.

use serde::Serialize;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::{Compiled, Reason};

use crate::declare::TierPref;

/// Where a version runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    /// Compiled into MariaDB.
    #[serde(rename = "mariadb")]
    MariaDb,
    /// The fallback runtime in gateways and workers.
    Engine,
}

impl Tier {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Tier::MariaDb => "mariadb",
            Tier::Engine => "engine",
        }
    }
}

/// Where a compiled call is sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    /// No data access: any shard of the database's group.
    Any,
    /// The group is one shard: that shard.
    WholeGroup,
}

/// A version's placement, stored as `logic_versions.placement`.
#[derive(Debug, Clone, Serialize)]
pub struct Placement {
    /// The tier.
    pub tier: Tier,
    /// Why not MariaDB; empty when compiled.
    pub reasons: Vec<Reason>,
    /// Warnings.
    pub lints: Vec<Reason>,
    /// The route, for the compiled tier.
    pub route: Option<Route>,
    /// Tables read.
    pub reads: Vec<String>,
    /// Tables written.
    pub writes: Vec<String>,
    /// Static cost.
    pub cost: u64,
}

/// Places a compiled version. `single_shard` says the database's group
/// is always one shard.
///
/// # Errors
/// 39868 when `SPRIG_TIER = 'mariadb'` and the code can't compile.
pub fn place(c: &Compiled, pref: TierPref, single_shard: bool) -> Result<Placement> {
    let a = &c.analysis;
    let mut reasons = a.reasons.clone();
    let touches_data = !a.reads.is_empty() || !a.writes.is_empty();
    if touches_data && !single_shard {
        reasons.push(Reason::new(
            "db.unrouted",
            "function",
            "locality over sharded groups isn't built yet",
        ));
    }
    let compilable = reasons.is_empty() && c.plsql.is_some();
    if pref == TierPref::Mariadb && !compilable {
        let why: Vec<String> = reasons.iter().map(ToString::to_string).collect();
        return Err(SprigError::new(
            SprigCode::LogicTierUnavailable,
            format!(
                "SPRIG_TIER = 'mariadb' but the code can't compile: {}",
                why.join("; ")
            ),
        ));
    }
    let tier = if compilable && pref != TierPref::Engine {
        Tier::MariaDb
    } else {
        Tier::Engine
    };
    Ok(Placement {
        tier,
        reasons,
        lints: a.lints.clone(),
        route: (tier == Tier::MariaDb).then_some(if touches_data {
            Route::WholeGroup
        } else {
            Route::Any
        }),
        reads: a.reads.clone(),
        writes: a.writes.clone(),
        cost: a.cost,
    })
}

/// `EXPLAIN FUNCTION`'s text for a placement.
#[must_use]
pub fn explain(p: &Placement) -> String {
    let mut s = format!("tier {}", p.tier.as_str());
    if let Some(r) = &p.route {
        s.push_str(&format!(
            ", route {}",
            match r {
                Route::Any => "any shard",
                Route::WholeGroup => "the group's shard",
            }
        ));
    }
    s.push_str(&format!(", cost {}", p.cost));
    if !p.reads.is_empty() {
        s.push_str(&format!(", reads {}", p.reads.join(", ")));
    }
    if !p.writes.is_empty() {
        s.push_str(&format!(", writes {}", p.writes.join(", ")));
    }
    for r in &p.reasons {
        s.push_str(&format!("\n  reason {r}"));
    }
    for r in &p.lints {
        s.push_str(&format!("\n  lint {r}"));
    }
    s
}
