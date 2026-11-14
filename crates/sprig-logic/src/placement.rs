//! Placement: the tier a version runs in, and why.
//!
//! As built: language, effects, bounds and cost come from the compiler's
//! analysis. Locality is answered only for groups that are always one
//! shard (every unsharded tenant): the route is the whole group. Code
//! with data access in a sharded group goes to the engine tier with
//! `db.unrouted`. Gateway-only writes, kinds other than functions and
//! cluster capabilities have nothing to check yet.

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

/// `EXPLAIN FUNCTION | PROCEDURE [db.]name [FORMAT = SQL]`: the stored versions' placements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainFunction {
    /// `function` or `procedure`.
    pub kind: crate::declare::Kind,
    /// The database, when named.
    pub db: Option<String>,
    /// The module's name.
    pub name: String,
    /// `FORMAT = SQL`: the generated PL/SQL too (`ALTER ROUTINE` only).
    pub sql: bool,
}

/// `EXPLAIN FUNCTION`'s columns; `PL/SQL` only with `FORMAT = SQL`.
pub const EXPLAIN_COLUMNS: [&str; 5] = ["Version", "State", "Tier", "Placement", "PL/SQL"];

/// Parses `EXPLAIN FUNCTION | PROCEDURE [db.]name [FORMAT [=] SQL |
/// TEXT]`; `None` for any other statement (`EXPLAIN tbl`, ``EXPLAIN
/// `function` ``, `EXPLAIN SELECT ...`).
#[must_use]
pub fn parse_explain(sql: &str) -> Option<Result<ExplainFunction>> {
    use crate::declare::Kind;
    let t = sql.trim_start();
    let head = t.get(..7)?;
    if !head.eq_ignore_ascii_case("explain") {
        return None;
    }
    let rest = t[7..].strip_prefix(|c: char| c.is_ascii_whitespace())?;
    let rest = rest.trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    let kind = match rest[..end].to_ascii_lowercase().as_str() {
        "function" => Kind::Function,
        "procedure" => Kind::Procedure,
        _ => return None,
    };
    let rest = &rest[end..];
    let usage = || {
        SprigError::new(
            SprigCode::LogicSyntax,
            "EXPLAIN FUNCTION | PROCEDURE [db.]name [FORMAT = SQL | TEXT]",
        )
    };
    if !(rest.trim_end_matches(';').trim().is_empty()
        || rest.starts_with(|c: char| c.is_ascii_whitespace()))
    {
        return None;
    }
    // The name (backquotes allowed), then an optional FORMAT.
    let w: Vec<String> = {
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut quoted = false;
        for ch in rest.trim().trim_end_matches(';').chars() {
            match ch {
                '`' => quoted = !quoted,
                '=' if !quoted => {
                    if !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                    }
                    out.push("=".to_owned());
                }
                c if !quoted && c.is_whitespace() => {
                    if !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                    }
                }
                c => cur.push(c),
            }
        }
        if !cur.is_empty() {
            out.push(cur);
        }
        out
    };
    let Some(target) = w.first() else {
        return Some(Err(usage()));
    };
    let (db, name) = match target.rsplit_once('.') {
        Some((d, n)) if !d.is_empty() && !n.is_empty() => (Some(d.to_owned()), n.to_owned()),
        Some(_) => return Some(Err(usage())),
        None => (None, target.clone()),
    };
    let fmt: Vec<&str> = w[1..].iter().map(String::as_str).collect();
    let format = match fmt.as_slice() {
        [] => None,
        [f, v] if f.eq_ignore_ascii_case("format") => Some(*v),
        [f, "=", v] if f.eq_ignore_ascii_case("format") => Some(*v),
        _ => return Some(Err(usage())),
    };
    let sql = match format {
        None => false,
        Some(v) if v.eq_ignore_ascii_case("sql") => true,
        Some(v) if v.eq_ignore_ascii_case("text") => false,
        Some(_) => return Some(Err(usage())),
    };
    Some(Ok(ExplainFunction {
        kind,
        db,
        name,
        sql,
    }))
}

/// `EXPLAIN FUNCTION`'s text for a stored version: its tier and its
/// stored placement (`logic_versions.placement`, a [`Placement`] as JSON),
/// as [`explain`] prints a fresh one.
#[must_use]
pub fn explain_stored(tier: &str, placement: &serde_json::Value) -> String {
    let text = |v: &serde_json::Value| v.as_str().map(str::to_owned);
    let list = |k: &str| -> Vec<String> {
        placement[k]
            .as_array()
            .map(|a| a.iter().filter_map(text).collect())
            .unwrap_or_default()
    };
    let mut s = format!("tier {tier}");
    match placement["route"].as_str() {
        Some("any") => s.push_str(", route any shard"),
        Some("whole_group") => s.push_str(", route the group's shard"),
        _ => {}
    }
    if let Some(c) = placement["cost"].as_u64() {
        s.push_str(&format!(", cost {c}"));
    }
    for (k, label) in [("reads", "reads"), ("writes", "writes")] {
        let l = list(k);
        if !l.is_empty() {
            s.push_str(&format!(", {label} {}", l.join(", ")));
        }
    }
    for (k, label) in [("reasons", "reason"), ("lints", "lint")] {
        for r in placement[k].as_array().into_iter().flatten() {
            s.push_str(&format!(
                "\n  {label} {} at {}: {}",
                r["code"].as_str().unwrap_or(""),
                r["at"].as_str().unwrap_or(""),
                r["detail"].as_str().unwrap_or("")
            ));
        }
    }
    s
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

#[cfg(test)]
mod explain_tests {
    use super::*;
    use crate::declare::Kind;

    #[test]
    fn explain_function_parses() {
        let ok = |s: &str| parse_explain(s).unwrap().unwrap();
        assert_eq!(
            ok("EXPLAIN FUNCTION shop.quote"),
            ExplainFunction {
                kind: Kind::Function,
                db: Some("shop".into()),
                name: "quote".into(),
                sql: false,
            }
        );
        let e = ok("explain  procedure `my proc` FORMAT=SQL;");
        assert_eq!(e.kind, Kind::Procedure);
        assert_eq!((e.db, e.name.as_str(), e.sql), (None, "my proc", true));
        assert!(ok("EXPLAIN FUNCTION f FORMAT = sql").sql);
        assert!(!ok("EXPLAIN FUNCTION f FORMAT TEXT").sql);
        // Other statements stay MariaDB's.
        for s in [
            "EXPLAIN SELECT 1",
            "EXPLAIN orders",
            "EXPLAIN `function`",
            "EXPLAIN FUNCTIONS",
            "EXPLAINFUNCTION f",
            "DESCRIBE FUNCTION f",
        ] {
            assert!(parse_explain(s).is_none(), "{s}");
        }
        for s in [
            "EXPLAIN FUNCTION",
            "EXPLAIN FUNCTION f FORMAT=JSON",
            "EXPLAIN FUNCTION f g",
            "EXPLAIN FUNCTION .f",
        ] {
            assert!(parse_explain(s).unwrap().is_err(), "{s}");
        }
    }

    #[test]
    fn stored_placement_reads_like_a_fresh_one() {
        let p = Placement {
            tier: Tier::Engine,
            reasons: vec![Reason::new("js.closure", "line 3", "a closure")],
            lints: Vec::new(),
            route: None,
            reads: vec!["products".into()],
            writes: Vec::new(),
            cost: 4,
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(explain_stored("engine", &v), explain(&p));
        let m = Placement {
            tier: Tier::MariaDb,
            reasons: Vec::new(),
            lints: Vec::new(),
            route: Some(Route::WholeGroup),
            reads: Vec::new(),
            writes: vec!["orders".into()],
            cost: 2,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(explain_stored("mariadb", &v), explain(&m));
    }
}
