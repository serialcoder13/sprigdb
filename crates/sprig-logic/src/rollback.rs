//! `ALTER FUNCTION | PROCEDURE [db.]f ROLLBACK [TO VERSION n]`.
//!
//! A rollback makes an earlier version the active one again, at once. Older
//! versions are recompiled and redeployed: the
//! version's own source is compiled again against today's tables, placed,
//! and its routines (entry, wrapper, `SELECT` function) re-created under
//! the same version number on every shard of the database, then that
//! version is activated and the one it replaces is `superseded`. No
//! version number is used up. Without `TO VERSION`, the target is the
//! newest earlier version that ever deployed (state `superseded` or
//! `active`, never `failed`).
//!
//! `sprigctl logic rollback <name> [--to-version n]` sends the statement
//! through a gateway, as `sprigctl logic deploy` sends declarations.

use mysql_async::Conn;
use sprig_common::{Result, SprigCode, SprigError};

use crate::catalog::LogicStore;
use crate::declare::{Declaration, Kind, Param, TierPref};
use crate::rollout::{RolloutOp, RolloutSpec};

/// A parsed rollback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rollback {
    /// Function or procedure.
    pub kind: Kind,
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// `TO VERSION n`.
    pub to: Option<u64>,
    /// `ALTER ... ROLLOUT ...` instead of `ROLLBACK`
    /// ([`crate::rollout`]): the candidate's next stage, its promotion, or
    /// the end of the rollout.
    pub rollout: Option<RolloutOp>,
}

fn words(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for ch in sql.trim().trim_end_matches(';').chars() {
        match ch {
            '`' => quoted = !quoted,
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
}

/// Whether `sql` is a rollback (cheap test for the gateway's dispatch).
#[must_use]
pub fn is_rollback(sql: &str) -> bool {
    let w = words(&sql.chars().take(512).collect::<String>());
    w.len() >= 4
        && w[0].eq_ignore_ascii_case("alter")
        && (w[1].eq_ignore_ascii_case("function") || w[1].eq_ignore_ascii_case("procedure"))
        && (w[3].eq_ignore_ascii_case("rollback") || w[3].eq_ignore_ascii_case("rollout"))
}

/// Parses a rollback; `None` when `sql` isn't one.
///
/// # Errors
/// 39861 for a malformed `TO VERSION`.
#[must_use]
pub fn parse(sql: &str) -> Option<Result<Rollback>> {
    if !is_rollback(sql) {
        return None;
    }
    let w = words(sql);
    let kind = if w[1].eq_ignore_ascii_case("function") {
        Kind::Function
    } else {
        Kind::Procedure
    };
    let (db, name) = match w[2].rsplit_once('.') {
        Some((d, n)) if !d.is_empty() && !n.is_empty() => (Some(d.to_owned()), n.to_owned()),
        _ => (None, w[2].clone()),
    };
    let rest: Vec<String> = w[4..].iter().map(|x| x.to_ascii_lowercase()).collect();
    if w[3].eq_ignore_ascii_case("rollout") {
        return Some(crate::rollout::parse_op(&rest).map(|op| Rollback {
            kind,
            db,
            name,
            to: None,
            rollout: Some(op),
        }));
    }
    let to = match rest.as_slice() {
        [] => None,
        [t, v, n] if t == "to" && v == "version" => match n.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => {
                return Some(Err(SprigError::new(
                    SprigCode::LogicSyntax,
                    format!("ROLLBACK TO VERSION takes a number, not {n}"),
                )));
            }
        },
        _ => {
            return Some(Err(SprigError::new(
                SprigCode::LogicSyntax,
                "expected ALTER FUNCTION f ROLLBACK [TO VERSION n]",
            )));
        }
    };
    Some(Ok(Rollback {
        kind,
        db,
        name,
        to,
        rollout: None,
    }))
}

impl Rollback {
    /// A declaration carrying only what the gateway needs before it has
    /// read the catalog (kind, database, name).
    #[must_use]
    pub fn placeholder(&self) -> Declaration {
        Declaration {
            kind: self.kind,
            or_replace: true,
            db: self.db.clone(),
            name: self.name.clone(),
            runtime: sprig_logic_compile::Runtime::Kirun,
            definer_security: false,
            deterministic: false,
            exposure: vec!["sql".to_owned()],
            tier: TierPref::Auto,
            body: String::new(),
            table: None,
            source: None,
            events: Vec::new(),
            when_fields: Vec::new(),
            schedule: None,
            params: Vec::new(),
            returns: None,
            rollout: None,
        }
    }
}

/// The SQL type text for an IR type name in a stored signature.
fn sql_type(ir: &str) -> &'static str {
    match ir {
        "i64" => "bigint",
        "f64" => "double",
        "bool" => "boolean",
        "json" => "json",
        _ => "longtext",
    }
}

/// The version to go back to, and its declaration rebuilt from the
/// catalog: the module's options today (security, exposure, tier), the
/// version's source and signature.
///
/// # Errors
/// 39860 when there is no such function, version, or earlier version;
/// 39862 for a version that never deployed.
pub async fn resolve(
    store: &LogicStore,
    c: &mut Conn,
    db_id: u32,
    r: &Rollback,
) -> Result<(Declaration, u64)> {
    let not_found = |m: String| SprigError::new(SprigCode::LogicNotFound, m);
    let module = store
        .module(c, db_id, r.kind.as_str(), &r.name)
        .await?
        .ok_or_else(|| not_found(format!("no {} {}", r.kind.as_str(), r.name)))?;
    let active = module.active_version.unwrap_or(0);
    let versions = store.versions(c, module.module_id).await?;
    let usable = |s: &str| matches!(s, "superseded" | "active" | "retired" | "rolling_out");
    // `ROLLOUT ...`: the candidate (a stage or its promotion) or the
    // active version (the end of the rollout).
    let mut stage: Option<RolloutSpec> = None;
    let to = match r.rollout {
        None => r.to,
        Some(op) => {
            let ro = crate::rollout::get(c, store.schema(), module.module_id)
                .await?
                .ok_or_else(|| {
                    not_found(format!(
                        "{} has no rollout: deploy a candidate WITH (SPRIG_ROLLOUT = n)",
                        r.name
                    ))
                })?;
            match op {
                RolloutOp::To(pct, by) => {
                    stage = Some(RolloutSpec {
                        pct,
                        by: by.unwrap_or(ro.by),
                    });
                    Some(ro.candidate_version)
                }
                RolloutOp::Promote => Some(ro.candidate_version),
                RolloutOp::Abort => Some(ro.active_version),
            }
        }
    };
    let target = match to {
        Some(n) => {
            let v = versions
                .iter()
                .find(|v| v.version == n)
                .ok_or_else(|| not_found(format!("{} has no version {n}", r.name)))?;
            if !usable(&v.state) {
                return Err(SprigError::new(
                    SprigCode::LogicUnsupported,
                    format!(
                        "version {n} of {} is {}: only a version that was deployed can come back",
                        r.name, v.state
                    ),
                ));
            }
            v.clone()
        }
        None => versions
            .iter()
            .find(|v| v.version < active && usable(&v.state))
            .cloned()
            .ok_or_else(|| {
                not_found(format!(
                    "{} has no earlier version to roll back to (active: {active})",
                    r.name
                ))
            })?,
    };
    let source = store.source(c, module.module_id, target.version).await?;
    let exposure = store
        .callable(c, db_id, &r.name)
        .await?
        .map_or_else(|| vec!["sql".to_owned()], |f| f.exposure);
    let runtime = sprig_logic_compile::Runtime::parse(&module.runtime)
        .ok_or_else(|| SprigError::internal(format!("runtime {}", module.runtime)))?;
    let sig = &target.signature;
    let params = if runtime == sprig_logic_compile::Runtime::Kirun {
        Vec::new()
    } else {
        sig["params"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| Param {
                name: p["name"].as_str().unwrap_or_default().to_owned(),
                ty: p["sql"].as_str().map_or_else(
                    || sql_type(p["type"].as_str().unwrap_or("")).to_owned(),
                    str::to_owned,
                ),
            })
            .collect()
    };
    let d = Declaration {
        kind: r.kind,
        or_replace: true,
        db: r.db.clone(),
        name: module.name.clone(),
        runtime,
        definer_security: module.security == "definer",
        deterministic: false,
        exposure,
        tier: match module.tier_pref.as_str() {
            "mariadb" => TierPref::Mariadb,
            "engine" => TierPref::Engine,
            _ => TierPref::Auto,
        },
        body: source,
        table: None,
        source: None,
        events: Vec::new(),
        when_fields: Vec::new(),
        schedule: None,
        params,
        returns: sig["returns"].as_str().map(str::to_owned),
        rollout: stage,
    };
    Ok((d, target.version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        assert!(parse("SELECT 1").is_none());
        assert!(parse("ALTER FUNCTION f COMMENT 'x'").is_none());
        let r = parse("alter function shop.`quote` rollback")
            .unwrap()
            .unwrap();
        assert_eq!(
            r,
            Rollback {
                kind: Kind::Function,
                db: Some("shop".into()),
                name: "quote".into(),
                to: None,
                rollout: None,
            }
        );
        let r = parse("ALTER FUNCTION shop.quote ROLLOUT TO 25 PERCENT BY TENANT")
            .unwrap()
            .unwrap();
        assert_eq!(
            r.rollout,
            Some(RolloutOp::To(25, Some(crate::rollout::RolloutBy::Tenant)))
        );
        assert!(is_rollback("alter procedure p rollout promote"));
        assert!(parse("ALTER FUNCTION f ROLLOUT").unwrap().is_err());
        let r = parse("ALTER PROCEDURE place ROLLBACK TO VERSION 3;")
            .unwrap()
            .unwrap();
        assert_eq!(
            (r.kind, r.name.as_str(), r.to),
            (Kind::Procedure, "place", Some(3))
        );
        assert!(
            parse("ALTER FUNCTION f ROLLBACK TO VERSION x")
                .unwrap()
                .is_err()
        );
        assert!(parse("ALTER FUNCTION f ROLLBACK NOW").unwrap().is_err());
        assert!(is_rollback("  ALTER  FUNCTION f ROLLBACK"));
        assert_eq!(sql_type("i64"), "bigint");
    }
}
