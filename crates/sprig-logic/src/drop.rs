//! Dropping logic modules (chapter 41, "Deployment and removal"; error
//! 39875; R7 follow-ups):
//!
//! ```text
//! DROP {FUNCTION | PROCEDURE | VALIDATION | TRIGGER | SCHEDULE} [IF EXISTS] [db.]name
//!   [RESTRICT | CASCADE]
//! ```
//!
//! A function or procedure may be in use:
//!
//! | Dependent | `RESTRICT` (default) | `CASCADE` |
//! |---|---|---|
//! | a schedule that calls it | 39875 | the schedule is dropped too |
//! | a sink's `TRANSFORM` | 39875 | 39875: alter or drop the sink first (it would dead-letter every record) |
//! | a computed column made from it | 39875 | the column stays (its expression is its own); the link is removed |
//! | other code that calls it (`db.call`, `Sprig.Db.Call`) | 39875 | dropped anyway; those calls fail with 39860 |
//!
//! "Other code" is found by reading the active source of the database's
//! other modules for a call naming the function, so it can list code that
//! only mentions the name in a call it never makes.
//!
//! Dropping removes, on every shard: the wrapper procedure, the `SELECT`
//! function, and every version's entry procedure and helper package; then
//! the module is `dropped` (its name gets `#<id>`), its versions retired,
//! and its rollout ended. Triggers and schedules also leave their index
//! entry or feed subscription, as `DROP DATABASE` does
//! ([`crate::dropdb`]); a validation's `CHECK` constraint or `BEFORE`
//! triggers go with it.
//!
//! A plain SQL routine of the same name (no logic module) is ordinary
//! DDL, not handled here.

use std::sync::Arc;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde::Serialize;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_feed::subs::Subscriptions;
use sprig_meta::MetaStore;

use crate::catalog::{LogicStore, ModuleRow};
use crate::declare::Kind;
use crate::dropdb::{Cleanup, cleanup_of};

/// A parsed `DROP`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropStmt {
    /// What it drops.
    pub kind: Kind,
    /// `IF EXISTS`.
    pub if_exists: bool,
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// `CASCADE`.
    pub cascade: bool,
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

/// Parses a `DROP` of a logic kind; `None` for any other statement
/// (including `DROP TRIGGER` and `DROP FUNCTION` of plain SQL objects,
/// which the caller recognises by finding no module).
#[must_use]
pub fn parse(sql: &str) -> Option<Result<DropStmt>> {
    let head: String = sql.chars().take(512).collect();
    let w = words(&head);
    if w.len() < 3 || !w[0].eq_ignore_ascii_case("drop") {
        return None;
    }
    let kind = match w[1].to_ascii_lowercase().as_str() {
        "function" => Kind::Function,
        "procedure" => Kind::Procedure,
        "validation" => Kind::Validation,
        "trigger" => Kind::Trigger,
        "schedule" => Kind::Schedule,
        _ => return None,
    };
    let w = words(sql);
    let mut i = 2;
    let mut if_exists = false;
    if w.len() > 3 && w[2].eq_ignore_ascii_case("if") && w[3].eq_ignore_ascii_case("exists") {
        if_exists = true;
        i = 4;
    }
    let Some(target) = w.get(i) else {
        return Some(Err(SprigError::new(
            SprigCode::LogicSyntax,
            "DROP needs a name",
        )));
    };
    let (db, name) = match target.rsplit_once('.') {
        Some((d, n)) if !d.is_empty() && !n.is_empty() => (Some(d.to_owned()), n.to_owned()),
        _ => (None, target.clone()),
    };
    let rest: Vec<String> = w[i + 1..].iter().map(|x| x.to_ascii_lowercase()).collect();
    let cascade = match rest.as_slice() {
        [] => false,
        [r] if r == "restrict" => false,
        [c] if c == "cascade" => true,
        _ => {
            // Something else (a list of names, `ON t` of MySQL triggers):
            // not a logic drop.
            return None;
        }
    };
    Some(Ok(DropStmt {
        kind,
        if_exists,
        db,
        name,
        cascade,
    }))
}

/// `SHOW DEPENDENTS OF {FUNCTION | PROCEDURE} [db.]name` (ST67): what
/// [`dependents`] finds, one row each, as `DROP ... RESTRICT` would refuse
/// it and what `CASCADE` would do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowDependents {
    /// A function or a procedure.
    pub kind: Kind,
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
}

/// Parses `SHOW DEPENDENTS OF FUNCTION | PROCEDURE [db.]name`; `None` for
/// any other statement.
#[must_use]
pub fn parse_show_dependents(sql: &str) -> Option<Result<ShowDependents>> {
    let head: String = sql.chars().take(512).collect();
    let w = words(&head);
    if w.len() < 3 || !w[0].eq_ignore_ascii_case("show") || !w[1].eq_ignore_ascii_case("dependents")
    {
        return None;
    }
    let w = words(sql);
    let usage = || {
        SprigError::new(
            SprigCode::LogicSyntax,
            "SHOW DEPENDENTS OF FUNCTION | PROCEDURE [db.]name",
        )
    };
    if w.len() != 5 || !w[2].eq_ignore_ascii_case("of") {
        return Some(Err(usage()));
    }
    let kind = match w[3].to_ascii_lowercase().as_str() {
        "function" => Kind::Function,
        "procedure" => Kind::Procedure,
        _ => return Some(Err(usage())),
    };
    let (db, name) = match w[4].rsplit_once('.') {
        Some((d, n)) if !d.is_empty() && !n.is_empty() => (Some(d.to_owned()), n.to_owned()),
        _ => (None, w[4].clone()),
    };
    Some(Ok(ShowDependents { kind, db, name }))
}

/// `SHOW DEPENDENTS`' columns.
pub const DEPENDENTS_COLUMNS: [&str; 4] = ["Kind", "Name", "Cascade", "Detail"];

/// What uses a function or procedure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Dependents {
    /// Schedules that call it: (module id, name).
    pub schedules: Vec<(u64, String)>,
    /// Sinks whose `TRANSFORM` it is.
    pub sinks: Vec<String>,
    /// Computed columns made from it: (binding id, `table.column`).
    pub columns: Vec<(u64, String)>,
    /// Other modules whose active code calls it: `kind name`.
    pub callers: Vec<String>,
}

impl Dependents {
    /// Nothing uses it.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schedules.is_empty()
            && self.sinks.is_empty()
            && self.columns.is_empty()
            && self.callers.is_empty()
    }

    /// One row per dependent for `SHOW DEPENDENTS` ([`DEPENDENTS_COLUMNS`]):
    /// its kind (`schedule`, `sink`, `column`, `caller`), its name, what
    /// `CASCADE` does to it (`dropped`, `refused`, `kept`) and why, in
    /// the order `DROP`'s refusal lists them.
    #[must_use]
    pub fn rows(&self) -> Vec<[String; 4]> {
        let mut out = Vec::new();
        for (_, n) in &self.schedules {
            out.push([
                "schedule".into(),
                n.clone(),
                "dropped".into(),
                "the schedule calls it and is dropped with it".into(),
            ]);
        }
        for n in &self.sinks {
            out.push([
                "sink".into(),
                n.clone(),
                "refused".into(),
                "its TRANSFORM: CASCADE doesn't drop sinks, so ALTER SINK or DROP SINK first"
                    .into(),
            ]);
        }
        for (_, n) in &self.columns {
            out.push([
                "column".into(),
                n.clone(),
                "kept".into(),
                "a computed column made from it: it keeps its own expression; the link is removed"
                    .into(),
            ]);
        }
        for n in &self.callers {
            out.push([
                "caller".into(),
                n.clone(),
                "kept".into(),
                "its code calls it: kept, and those calls fail with 39860".into(),
            ]);
        }
        out
    }

    fn list(&self) -> String {
        let mut parts = Vec::new();
        if !self.schedules.is_empty() {
            let n: Vec<&str> = self.schedules.iter().map(|(_, n)| n.as_str()).collect();
            parts.push(format!("schedules {}", n.join(", ")));
        }
        if !self.sinks.is_empty() {
            parts.push(format!("sinks {} (TRANSFORM)", self.sinks.join(", ")));
        }
        if !self.columns.is_empty() {
            let n: Vec<&str> = self.columns.iter().map(|(_, n)| n.as_str()).collect();
            parts.push(format!("computed columns {}", n.join(", ")));
        }
        if !self.callers.is_empty() {
            parts.push(format!("code that calls it: {}", self.callers.join(", ")));
        }
        parts.join("; ")
    }

    /// The refusal (39875) for `DROP ... name` with these dependents, or
    /// `None` when the drop may go ahead (`CASCADE` past what it may
    /// take).
    #[must_use]
    pub fn refusal(&self, kind: Kind, name: &str, cascade: bool) -> Option<SprigError> {
        if self.is_empty() {
            return None;
        }
        if !cascade {
            return Some(SprigError::new(
                SprigCode::LogicInUse,
                format!(
                    "{} {name} is in use: {}. Drop or change them first, or DROP {} {name} CASCADE",
                    kind.as_str(),
                    self.list(),
                    kind.as_str().to_ascii_uppercase()
                ),
            ));
        }
        if !self.sinks.is_empty() {
            return Some(SprigError::new(
                SprigCode::LogicInUse,
                format!(
                    "{} {name} is the TRANSFORM of sinks {}: ALTER SINK ... or DROP SINK first \
                     (CASCADE doesn't drop sinks)",
                    kind.as_str(),
                    self.sinks.join(", ")
                ),
            ));
        }
        None
    }
}

fn map_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the logic catalog is unreachable",
    )
    .with_source(e)
}

/// Whether a missing table or column means "none" (a catalog without the
/// sinks module, or an older one).
fn absent(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146 || s.code == 1054)
}

/// Whether `source` holds a call of `name` (`db` qualified or not):
/// `db.call("name"` in JavaScript or Lua, a `Sprig.Db.Call` step naming it
/// in KIRun.
#[must_use]
pub fn calls(source: &str, db: &str, name: &str) -> bool {
    let s = source.to_ascii_lowercase();
    let n = name.to_ascii_lowercase();
    let q = format!("{}.{n}", db.to_ascii_lowercase());
    let named = [
        format!("\"{n}\""),
        format!("'{n}'"),
        format!("\"{q}\""),
        format!("'{q}'"),
    ];
    let has_name = named.iter().any(|x| s.contains(x.as_str()));
    let call_site = s.contains("db.call(") || (s.contains("sprig.db") && s.contains("\"call\""));
    has_name && call_site
}

/// What uses `m` (a function or procedure of database `db_id`, named
/// `db_name`); see the module docs.
///
/// # Errors
/// The catalog's.
pub async fn dependents(
    c: &mut Conn,
    store: &LogicStore,
    db_id: u32,
    db_name: &str,
    m: &ModuleRow,
) -> Result<Dependents> {
    let s = sprig_common::names::quote(store.schema());
    let mut d = Dependents::default();
    let r: std::result::Result<Vec<(u64, String)>, _> = c
        .exec(
            format!(
                "SELECT m.module_id, m.name FROM {s}.logic_schedules sc JOIN {s}.logic_modules m \
                 ON m.module_id = sc.module_id WHERE sc.target_module_id = ? AND m.state <> 'dropped' \
                 ORDER BY m.name"
            ),
            (m.module_id,),
        )
        .await;
    d.schedules = match r {
        Err(e) if absent(&e) => Vec::new(),
        r => r.map_err(map_err)?,
    };
    let qualified = format!("{db_name}.{}", m.name);
    let r: std::result::Result<Vec<String>, _> = c
        .exec(
            format!(
                "SELECT name FROM {s}.sinks WHERE db_id = ? AND state <> 'dropping' \
                 AND (transform_name = ? OR transform_name = ?) ORDER BY name"
            ),
            (db_id, &m.name, &qualified),
        )
        .await;
    d.sinks = match r {
        Err(e) if absent(&e) => Vec::new(),
        r => r.map_err(map_err)?,
    };
    let r: std::result::Result<Vec<(u64, Option<u64>, String)>, _> = c
        .exec(
            format!(
                "SELECT binding_id, object_id, column_name FROM {s}.logic_bindings \
                 WHERE module_id = ? AND column_name IS NOT NULL AND state = 'active'"
            ),
            (m.module_id,),
        )
        .await;
    let cols = match r {
        Err(e) if absent(&e) => Vec::new(),
        r => r.map_err(map_err)?,
    };
    for (b, obj, col) in cols {
        let table: Option<String> = match obj {
            Some(o) => c
                .exec_first(
                    format!("SELECT name FROM {s}.objects WHERE object_id = ?"),
                    (o,),
                )
                .await
                .ok()
                .flatten(),
            None => None,
        };
        d.columns.push((
            b,
            format!("{}.{col}", table.unwrap_or_else(|| "?".to_owned())),
        ));
    }
    // Other live modules of the database whose active code calls it.
    let r: std::result::Result<Vec<(String, String, String)>, _> = c
        .exec(
            format!(
                "SELECT m.kind, m.name, v.source FROM {s}.logic_modules m JOIN {s}.logic_versions v \
                 ON v.module_id = m.module_id AND v.version = m.active_version \
                 WHERE m.db_id = ? AND m.state <> 'dropped' AND m.module_id <> ? ORDER BY m.name"
            ),
            (db_id, m.module_id),
        )
        .await;
    let others = match r {
        Err(e) if absent(&e) => Vec::new(),
        r => r.map_err(map_err)?,
    };
    for (kind, name, source) in others {
        if calls(&source, db_name, &m.name) {
            d.callers.push(format!("{kind} {name}"));
        }
    }
    Ok(d)
}

/// The objects a function's or procedure's versions left in a shard
/// schema: `(kind, name)` for [`crate::deploy::drop_objects`], packages
/// before their procedures, the wrapper and `SELECT` function last.
#[must_use]
pub fn routine_objects(m: &ModuleRow, versions: &[u64]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for v in versions {
        let (package, entry) = sprig_logic_compile::plsql::names(m.module_id, *v);
        out.push(("PACKAGE".to_owned(), package));
        out.push(("PROCEDURE".to_owned(), entry));
    }
    if m.kind == "function" || m.kind == "procedure" {
        out.push(("PROCEDURE".to_owned(), m.name.clone()));
    }
    if m.kind == "function" {
        out.push(("FUNCTION".to_owned(), m.name.clone()));
    }
    out
}

/// The catalog and DC-tier side of dropping one module: its rollout ends,
/// the module is `dropped`, its versions retired, its computed-column
/// links removed, and its trigger's index entry or feed subscription, or
/// its schedule's index entry, deleted (the trigger's or schedule's job
/// ends at its next check).
///
/// # Errors
/// The catalog's or the DC tier's.
pub async fn module_dropped(
    c: &mut Conn,
    store: &LogicStore,
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    m: &ModuleRow,
    catalog_version: u64,
) -> Result<()> {
    let schema = store.schema().to_owned();
    if matches!(m.kind.as_str(), "function" | "procedure") {
        crate::rollout::clear(c, &schema, m.module_id).await?;
        let s = sprig_common::names::quote(&schema);
        c.exec_drop(
            format!(
                "UPDATE {s}.logic_bindings SET state = 'dropping' WHERE module_id = ? \
                 AND column_name IS NOT NULL"
            ),
            (m.module_id,),
        )
        .await
        .map_err(map_err)?;
    }
    let binding = store.binding(c, m.module_id).await?;
    store.mark_dropped(c, m.module_id, catalog_version).await?;
    match cleanup_of(&m.kind, binding.as_ref()) {
        Cleanup::MessageIndex => {
            if let Some(b) = &binding {
                if let Some(def) = crate::messages::source(c, &schema, b.binding_id)
                    .await?
                    .and_then(|s| s.fanin_definition())
                {
                    crate::messages::delete_fanin(dc.as_ref(), tenant, b.binding_id, def).await?;
                }
                crate::messages::delete_index(dc.as_ref(), tenant, b.binding_id).await?;
            }
        }
        Cleanup::Subscription => {
            if let Some(b) = &binding {
                Subscriptions::new(Arc::clone(dc))
                    .drop_subscription(tenant, &crate::triggers::subscription_name(b.binding_id))
                    .await?;
            }
        }
        Cleanup::ScheduleIndex => {
            let key = crate::schedule::index_key(dc.as_ref(), tenant, m.module_id)?;
            dc.txn(sprig_meta::Txn {
                when: Vec::new(),
                then: vec![sprig_meta::Op::Delete(key)],
                otherwise: Vec::new(),
            })
            .await?;
        }
        Cleanup::None => {}
    }
    tracing::info!(%tenant, module = m.module_id, name = %m.name, kind = %m.kind, "logic module dropped");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parses_logic_drops_only() {
        let d = parse("DROP FUNCTION IF EXISTS shop.`quote` CASCADE;")
            .unwrap()
            .unwrap();
        assert_eq!(
            d,
            DropStmt {
                kind: Kind::Function,
                if_exists: true,
                db: Some("shop".into()),
                name: "quote".into(),
                cascade: true,
            }
        );
        let d = parse("drop procedure place restrict").unwrap().unwrap();
        assert!(!d.cascade && !d.if_exists && d.kind == Kind::Procedure);
        assert_eq!(
            parse("DROP SCHEDULE nightly").unwrap().unwrap().kind,
            Kind::Schedule
        );
        assert!(parse("DROP TABLE t").is_none());
        assert!(parse("DROP TRIGGER a, b").is_none());
        assert!(parse("SELECT 1").is_none());
        assert!(parse("DROP FUNCTION").is_none());
    }

    #[test]
    fn show_dependents_parses_and_lists() {
        let d = parse_show_dependents("SHOW DEPENDENTS OF FUNCTION shop.rate;")
            .unwrap()
            .unwrap();
        assert_eq!(
            d,
            ShowDependents {
                kind: Kind::Function,
                db: Some("shop".into()),
                name: "rate".into()
            }
        );
        let p = parse_show_dependents("show dependents of procedure `nightly`")
            .unwrap()
            .unwrap();
        assert_eq!(
            (p.kind, p.db, p.name.as_str()),
            (Kind::Procedure, None, "nightly")
        );
        assert!(
            parse_show_dependents("SHOW DEPENDENTS OF TABLE t")
                .unwrap()
                .is_err()
        );
        assert!(parse_show_dependents("SHOW DEPENDENTS f").unwrap().is_err());
        assert!(parse_show_dependents("SHOW DATABASES").is_none());
        let deps = Dependents {
            schedules: vec![(7, "nightly".into())],
            sinks: vec!["out".into()],
            columns: vec![(3, "orders.total".into())],
            callers: vec!["function other".into()],
        };
        let rows = deps.rows();
        let kinds: Vec<(&str, &str, &str)> = rows
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str(), r[2].as_str()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("schedule", "nightly", "dropped"),
                ("sink", "out", "refused"),
                ("column", "orders.total", "kept"),
                ("caller", "function other", "kept"),
            ]
        );
        assert!(Dependents::default().rows().is_empty());
    }

    #[test]
    fn dependents_decide() {
        let none = Dependents::default();
        assert!(none.refusal(Kind::Function, "f", false).is_none());
        let sched = Dependents {
            schedules: vec![(9, "nightly".into())],
            ..Dependents::default()
        };
        let e = sched.refusal(Kind::Procedure, "p", false).unwrap();
        assert_eq!(e.code(), Some(SprigCode::LogicInUse));
        assert!(e.message.contains("schedules nightly"), "{}", e.message);
        assert!(sched.refusal(Kind::Procedure, "p", true).is_none());
        let sink = Dependents {
            sinks: vec!["to_kafka".into()],
            ..Dependents::default()
        };
        assert!(sink.refusal(Kind::Function, "f", true).is_some());
        let callers = Dependents {
            callers: vec!["trigger audit".into()],
            ..Dependents::default()
        };
        assert!(callers.refusal(Kind::Function, "f", false).is_some());
        assert!(callers.refusal(Kind::Function, "f", true).is_none());
    }

    #[test]
    fn finds_calls_in_code() {
        assert!(calls("const r = db.call('quote', 7, 3);", "shop", "quote"));
        assert!(calls(
            "local r = db.call(\"shop.quote\", 1)",
            "shop",
            "quote"
        ));
        assert!(calls(
            r#"{"steps": {"c": {"namespace": "Sprig.Db", "name": "Call", "parameterMap": {"name": {"one": {"value": "quote"}}}}}}"#,
            "shop",
            "quote"
        ));
        assert!(!calls("return 'quote';", "shop", "quote"));
        assert!(!calls("db.call('quotes')", "shop", "quote"));
        let m = ModuleRow {
            module_id: 44,
            db_id: 1,
            name: "quote".into(),
            kind: "function".into(),
            runtime: "javascript".into(),
            security: "invoker".into(),
            tier_pref: "auto".into(),
            active_version: Some(2),
            state: "active".into(),
            created_by: 1,
        };
        let o = routine_objects(&m, &[1, 2]);
        assert_eq!(o[0], ("PACKAGE".into(), "__sprig_l44_v1_lib".into()));
        assert_eq!(o[3], ("PROCEDURE".into(), "__sprig_l44_v2".into()));
        assert_eq!(o[4], ("PROCEDURE".into(), "quote".into()));
        assert_eq!(o[5], ("FUNCTION".into(), "quote".into()));
    }
}
